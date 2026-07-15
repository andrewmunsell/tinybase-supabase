import type { Content, Store, Table, Tables } from 'tinybase';
import { createCustomPersister, type Persister } from 'tinybase/persisters';
import { LocalState, type PendingOperation, type SyncCursor } from './storage.js';
import type { SyncScheduler } from './sync-scheduler.js';
import {
	asError,
	cloneContent,
	createPendingOperations,
	fromRemote,
	getRows,
	operationId,
	sortOperations,
} from './standard/operations.js';
import {
	isPermanentError,
	StandardTransport,
	type StandardRealtimeChannel,
} from './standard/protocol.js';
import type {
	RejectedOperation,
	SupabasePersister,
	SupabasePersisterConfig,
	SyncPhase,
	SyncStatus,
} from './types.js';

export type StandardPersister = Omit<
	SupabasePersister,
	'closeRow' | 'destroy' | 'isRowOpen' | 'openRow' | 'startAutoPersisting'
> & {
	destroy(): Promise<StandardPersister>;
	completeSync(): Promise<void>;
	isRowBlocked(tableId: string, rowId: string): Promise<boolean>;
	reconcile(markIdle?: boolean): Promise<boolean>;
	reportSyncError(error: unknown): Promise<void>;
	startAutoPersisting(): Promise<StandardPersister>;
};

const defaultPageSize = 500;
const defaultPollIntervalMs = 60_000;
const defaultCursorLookbackMs = 5 * 60_000;
const defaultRetryBaseDelayMs = 1_000;
const defaultRetryMaxDelayMs = 30_000;

/**
 * Creates a browser-only TinyBase Store persister with durable IndexedDB state,
 * direct Supabase CRUD synchronization, and optional Realtime pull wake-ups.
 */
export const createStandardPersister = async (
	store: Store,
	config: SupabasePersisterConfig,
	scheduler: SyncScheduler,
): Promise<StandardPersister> => {
	const state = await LocalState.open(config.databaseName, config.scopeKey, config.onError);
	const pageSize = config.pageSize ?? defaultPageSize;
	const cursorLookbackMs = Math.max(0, config.cursorLookbackMs ?? defaultCursorLookbackMs);
	const transport = new StandardTransport(config.supabase, pageSize);
	const retryBaseDelayMs = config.retryBaseDelayMs ?? defaultRetryBaseDelayMs;
	const retryMaxDelayMs = config.retryMaxDelayMs ?? defaultRetryMaxDelayMs;
	const tableConfigs = config.tables;
	let lastContent = (await state.getContent()) ?? store.getContent();
	let listener: ((content?: Content) => void) | undefined;
	let isDestroyed = false;
	let hasStartedSyncing = false;
	let retryAttempt = 0;
	const channels: StandardRealtimeChannel[] = [];
	const statusListeners = new Set<(status: SyncStatus) => void>();
	let status: SyncStatus = {
		pendingCount: (await state.getOperations()).length,
		phase: 'hydrating',
		rejectedCount: (await state.getRejected()).length,
	};

	const setStatus = async (phase: SyncPhase, error?: Error): Promise<void> => {
		status = {
			lastError: error,
			lastSuccessfulSyncAt: phase === 'idle' ? Date.now() : status.lastSuccessfulSyncAt,
			pendingCount: (await state.getOperations()).length,
			phase,
			rejectedCount: (await state.getRejected()).length,
		};
		for (const statusListener of statusListeners) {
			statusListener(status);
		}
	};

	const persistContent = async (
		content: Content,
		operations: readonly PendingOperation[],
	): Promise<void> => {
		await state.persist(content, operations);
		lastContent = cloneContent(content);
	};

	const createOperations = (content: Content): PendingOperation[] =>
		createPendingOperations(store, lastContent, content, tableConfigs);

	const applyRemoteContent = async (
		content: Content,
		cursorKey: string,
		cursor?: SyncCursor,
	): Promise<void> => {
		await state.replaceContent(content, cursorKey, cursor);
		lastContent = cloneContent(content);
		listener?.(content);
	};

	const pullTable = async (tableId: string): Promise<void> => {
		const tableConfig = tableConfigs[tableId];
		if (!tableConfig) {
			return;
		}

		const cursorKey = JSON.stringify([
			tableId,
			tableConfig.table,
			tableConfig.idColumn ?? 'id',
			tableConfig.deletedAtColumn ?? 'deleted_at',
			tableConfig.updatedAtColumn ?? 'updated_at',
			tableConfig.select ?? '*',
			tableConfig.cursorVersion ?? '',
		]);
		const cursor = await state.getCursor(cursorKey);
		const cursorTime = cursor ? Date.parse(cursor.updatedAt) : Number.NaN;
		const pullCursor =
			cursor && cursorLookbackMs > 0 && Number.isFinite(cursorTime)
				? { updatedAt: new Date(cursorTime - cursorLookbackMs).toISOString() }
				: cursor;
		const { cursor: pulledCursor, rows } = await transport.fetchRows(tableConfig, pullCursor);
		const pulledCursorTime = Date.parse(pulledCursor.updatedAt);
		const nextCursor = cursor && pulledCursorTime <= cursorTime ? cursor : pulledCursor;

		const content = cloneContent((await state.getContent()) ?? lastContent);
		const table: Table = { ...getRows(content, tableId) };
		const [pending, rejected] = await Promise.all([state.getOperations(), state.getRejected()]);
		const blockedIds = new Set([...pending, ...rejected].map((operation) => operation.id));
		const seen = new Set<string>();
		const deletedAtColumn = tableConfig.deletedAtColumn ?? 'deleted_at';

		for (const remote of rows) {
			const [rowId, row] = fromRemote(tableConfig, remote);
			seen.add(rowId);
			if (blockedIds.has(operationId(tableId, rowId))) {
				continue;
			}
			if (remote[deletedAtColumn] !== null && remote[deletedAtColumn] !== undefined) {
				delete table[rowId];
			} else {
				table[rowId] = row;
			}
		}

		if (!cursor) {
			for (const rowId of Object.keys(table)) {
				if (!seen.has(rowId) && !blockedIds.has(operationId(tableId, rowId))) {
					delete table[rowId];
				}
			}
		}

		const tables: Tables = { ...content[0] };
		if (Object.keys(table).length === 0) {
			delete tables[tableId];
		} else {
			tables[tableId] = table;
		}
		await applyRemoteContent([tables, content[1]], cursorKey, nextCursor);
	};

	const flushOutbox = async (): Promise<void> => {
		const operations = sortOperations(await state.getOperations(), tableConfigs);
		for (const operation of operations) {
			const tableConfig = tableConfigs[operation.tableId];
			if (!tableConfig) {
				await state.removeOperation(operation.id);
				continue;
			}
			try {
				await transport.upsert(tableConfig, operation.payload);
				await state.removeOperation(operation.id);
			} catch (error) {
				if (isPermanentError(error as Parameters<typeof isPermanentError>[0])) {
					await state.reject(operation, asError(error).message);
					continue;
				}
				throw error;
			}
		}
	};

	const clearRetry = (): void => {
		retryAttempt = 0;
	};

	const scheduleRetry = (): void => {
		if (isDestroyed) {
			return;
		}
		const delay = Math.min(retryBaseDelayMs * 2 ** retryAttempt, retryMaxDelayMs);
		retryAttempt += 1;
		scheduler.schedule(delay);
	};

	const reportSyncError = async (error: unknown): Promise<void> => {
		const normalized = asError(error);
		config.onError?.(normalized);
		await setStatus('offline', normalized);
		scheduleRetry();
	};

	const completeSync = async (): Promise<void> => {
		clearRetry();
		await setStatus('idle');
	};

	const reconcile = async (markIdle = true): Promise<boolean> => {
		if (isDestroyed) {
			return false;
		}
		await setStatus('syncing');
		try {
			await flushOutbox();
			for (const tableId of Object.keys(tableConfigs)) {
				await pullTable(tableId);
			}
			if (markIdle) {
				await completeSync();
			}
			return true;
		} catch (error) {
			await reportSyncError(error);
			return false;
		}
	};

	const syncNow = (): Promise<void> => scheduler.runNow();

	const schedulePull = (tableId: string): void => {
		const realtime = tableConfigs[tableId]?.realtime;
		if (realtime) {
			const delay = typeof realtime === 'object' ? (realtime.debounceMs ?? 200) : 200;
			scheduler.schedule(delay);
		}
	};

	const startRealtime = (): void => {
		for (const [tableId, tableConfig] of Object.entries(tableConfigs)) {
			if (!tableConfig.realtime) {
				continue;
			}

			const realtime = tableConfig.realtime;
			const channelName =
				typeof realtime === 'object' && realtime.channelName
					? realtime.channelName
					: `tinybase-supabase:${config.scopeKey}:${tableId}`;
			const channel = transport.subscribe(channelName, tableConfig, () =>
				schedulePull(tableId),
			);
			channels.push(channel);
		}
	};

	const stopSyncing = async (): Promise<void> => {
		hasStartedSyncing = false;
		clearRetry();
		scheduler.stop();
		for (const channel of channels.splice(0)) {
			await transport.unsubscribe(channel);
		}
	};

	const startSyncing = async (): Promise<void> => {
		if (isDestroyed || hasStartedSyncing) {
			return;
		}
		hasStartedSyncing = true;
		startRealtime();
		await scheduler.start(config.pollIntervalMs ?? defaultPollIntervalMs);
	};

	const basePersister = createCustomPersister(
		store,
		async () => state.getContent(),
		async (getContent) => {
			const content = getContent();
			const operations = createOperations(content);
			await persistContent(content, operations);
			if (operations.length > 0) {
				void syncNow();
			}
		},
		(nextListener) => {
			listener = nextListener;
			return undefined;
		},
		() => {
			listener = undefined;
		},
		config.onError,
	);

	const baseDestroy = basePersister.destroy.bind(basePersister);
	const baseStartAutoPersisting = basePersister.startAutoPersisting.bind(basePersister);
	const baseMethods = Object.fromEntries(
		Object.entries(basePersister).map(([name, method]) => [
			name,
			(method as (...arguments_: never[]) => unknown).bind(basePersister),
		]),
	);
	let result: StandardPersister;
	result = Object.assign(baseMethods as unknown as Persister, {
		addSyncStatusListener(nextListener: (nextStatus: SyncStatus) => void): () => void {
			statusListeners.add(nextListener);
			nextListener(status);
			return () => statusListeners.delete(nextListener);
		},
		completeSync,
		async discardRejected(): Promise<void> {
			await state.discardRejected();
			await setStatus(status.phase, status.lastError);
		},
		async destroy(): Promise<StandardPersister> {
			isDestroyed = true;
			await stopSyncing();
			state.close();
			await baseDestroy();
			return result;
		},
		async getRejectedOperations(): Promise<readonly RejectedOperation[]> {
			return (await state.getRejected()).map(({ error, rowId, tableId }) => ({
				error,
				rowId,
				tableId,
			}));
		},
		getSyncStatus(): SyncStatus {
			return status;
		},
		async isRowBlocked(tableId: string, rowId: string): Promise<boolean> {
			const id = operationId(tableId, rowId);
			const [pending, rejected] = await Promise.all([
				state.getOperations(),
				state.getRejected(),
			]);
			return (
				pending.some((operation) => operation.id === id) ||
				rejected.some((row) => row.id === id)
			);
		},
		async retryRejected(): Promise<void> {
			await state.retryRejected();
			await syncNow();
		},
		reconcile,
		reportSyncError,
		startSyncing,
		async startAutoPersisting(): Promise<StandardPersister> {
			await baseStartAutoPersisting();
			await startSyncing();
			return result;
		},
		stopSyncing,
		syncNow,
	}) as unknown as StandardPersister;

	await setStatus('idle');
	return result;
};
