import { type Content, createStore, type Store, type Table, type Tables } from 'tinybase';
import { createCustomPersister, type Persister } from 'tinybase/persisters';
import {
	type IndexedDbConnectionClosedForUpgradeError,
	isIndexedDbConnectionClosedException,
} from './indexeddb-errors.js';
import {
	asError,
	cloneContent,
	contentEquals,
	createPendingOperations,
	fromRemote,
	getRows,
	mergeContentChanges,
	operationId,
	sortOperations,
} from './standard/operations.js';
import {
	isPermanentError,
	type StandardRealtimeChannel,
	StandardTransport,
} from './standard/protocol.js';
import { LocalState, type PendingOperation, type SyncCursor } from './storage.js';
import type { SyncScheduler } from './sync-scheduler.js';
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
	assertUsable(): void;
	destroy(): Promise<StandardPersister>;
	completeSync(): Promise<void>;
	isRowBlocked(tableId: string, rowId: string): Promise<boolean>;
	reconcile(markIdle?: boolean): Promise<boolean>;
	reportSyncError(error: unknown): Promise<void>;
	startAutoPersisting(): Promise<StandardPersister>;
	terminate(error: IndexedDbConnectionClosedForUpgradeError): void;
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
	onTerminal?: (error: IndexedDbConnectionClosedForUpgradeError) => void,
): Promise<StandardPersister> => {
	let handleConnectionClosedForUpgrade:
		| ((error: IndexedDbConnectionClosedForUpgradeError) => void)
		| undefined;
	let pendingTerminalError: IndexedDbConnectionClosedForUpgradeError | undefined;
	const state = await LocalState.open(
		config.databaseName,
		config.scopeKey,
		config.onError,
		(error) => {
			if (handleConnectionClosedForUpgrade) {
				handleConnectionClosedForUpgrade(error);
			} else {
				pendingTerminalError = error;
			}
		},
	);
	const pageSize = config.pageSize ?? defaultPageSize;
	const cursorLookbackMs = Math.max(0, config.cursorLookbackMs ?? defaultCursorLookbackMs);
	const transport = new StandardTransport(config.supabase, pageSize);
	const retryBaseDelayMs = config.retryBaseDelayMs ?? defaultRetryBaseDelayMs;
	const retryMaxDelayMs = config.retryMaxDelayMs ?? defaultRetryMaxDelayMs;
	const tableConfigs = config.tables;
	const readDuringInitialization = async <Value>(
		read: () => Promise<Value>,
		fallback: Value,
	): Promise<Value> => {
		try {
			const value = await read();
			return pendingTerminalError ? fallback : value;
		} catch (error) {
			if (pendingTerminalError && isIndexedDbConnectionClosedException(error)) {
				return fallback;
			}

			throw error;
		}
	};

	const [initialContent, initialOperations, initialRejected] = await Promise.all([
		readDuringInitialization(() => state.getContent(), undefined),
		readDuringInitialization(() => state.getOperations(), []),
		readDuringInitialization(() => state.getRejected(), []),
	]);
	let lastContent = initialContent ?? store.getContent();
	let autoLoading = false;
	let hasHydrated = false;
	const initialStoreContent = cloneContent(store.getContent());
	let localRevision = 0;
	let basePersister: Persister;
	let loadedPersistedContent = false;
	let loadFallbackBaseline = initialStoreContent;
	const loadFallback = (initialContent?: Content | (() => Content)) => () =>
		loadedPersistedContent || initialContent === undefined
			? store.getContent()
			: mergeContentChanges(
					typeof initialContent === 'function' ? initialContent() : initialContent,
					loadFallbackBaseline,
					store.getContent(),
				);
	let persistence: Promise<unknown> = Promise.resolve();
	const revisionListener = store.addDidFinishTransactionListener(() => {
		localRevision += 1;
		if (hasHydrated && basePersister?.isAutoSaving() && basePersister.getStatus() === 1) {
			void persistSerially(persistLocalChanges).catch(reportListenerError);
		}
	});

	const persistSerially = <Value>(action: () => Promise<Value>): Promise<Value> => {
		const next = persistence.then(action);
		persistence = next.catch(() => undefined);
		return next;
	};

	let isDestroyed = false;
	let hasStartedSyncing = false;
	let retryAttempt = 0;
	let stopAutoPersistence: (() => Promise<unknown>) | undefined;
	let terminalError: IndexedDbConnectionClosedForUpgradeError | undefined;
	const channels: StandardRealtimeChannel[] = [];
	const statusListeners = new Set<(status: SyncStatus) => void>();
	let status: SyncStatus = {
		pendingCount: initialOperations.length,
		phase: 'hydrating',
		rejectedCount: initialRejected.length,
	};

	const reportListenerError = (error: unknown): void => {
		try {
			config.onError?.(asError(error));
		} catch {}
	};

	const notifyStatusListener = (statusListener: (status: SyncStatus) => void): void => {
		try {
			statusListener(status);
		} catch (error) {
			reportListenerError(error);
		}
	};

	const emitStatus = (): void => {
		for (const statusListener of statusListeners) {
			notifyStatusListener(statusListener);
		}
	};

	const setStatus = async (phase: SyncPhase, error?: Error): Promise<void> => {
		if (isDestroyed || terminalError) {
			return;
		}

		const nextStatus: SyncStatus = {
			lastError: error,
			lastSuccessfulSyncAt: phase === 'idle' ? Date.now() : status.lastSuccessfulSyncAt,
			pendingCount: (await state.getOperations()).length,
			phase,
			rejectedCount: (await state.getRejected()).length,
		};

		if (!isDestroyed && !terminalError) {
			status = nextStatus;
			emitStatus();
		}
	};

	const assertUsable = (): void => {
		if (terminalError) {
			throw terminalError;
		}
	};

	const runWhileUsable = async <Value>(operation: () => Promise<Value>): Promise<Value> => {
		assertUsable();
		try {
			const value = await operation();
			assertUsable();
			return value;
		} catch (error) {
			if (terminalError) {
				throw terminalError;
			}

			throw error;
		}
	};

	const persistContent = async (
		content: Content,
		operations: readonly PendingOperation[],
	): Promise<void> => {
		await state.persist(content, operations);
		lastContent = cloneContent(content);
		if (operations.length > 0) {
			void syncNow().catch((error: unknown) => {
				if (error !== terminalError) {
					config.onError?.(asError(error));
				}
			});
		}
	};

	const createOperations = (content: Content): PendingOperation[] =>
		createPendingOperations(lastContent, content, tableConfigs);

	const persistLocalChanges = async (): Promise<void> => {
		const content = cloneContent(store.getContent());
		if (!contentEquals(content, lastContent)) {
			await persistContent(content, createOperations(content));
		}
	};

	const applyRemoteContent = async (
		content: Content,
		cursorKey: string,
		cursor: SyncCursor | undefined,
		revision: number,
	): Promise<boolean> => {
		if (revision !== localRevision || isDestroyed || terminalError) {
			return false;
		}

		const contentStore = createStore();
		if (store.hasTablesSchema()) {
			contentStore.setTablesSchema(JSON.parse(store.getTablesSchemaJson()));
		}

		if (store.hasValuesSchema()) {
			contentStore.setValuesSchema(JSON.parse(store.getValuesSchemaJson()));
		}

		content = contentStore.setContent(cloneContent(content)).getContent();

		const previousContent = lastContent;
		const previousStore = cloneContent(store.getContent());
		let applied = false;
		try {
			return await state.replaceContent(content, cursorKey, cursor, () => {
				if (revision !== localRevision || isDestroyed || terminalError) {
					return false;
				}

				// Apply synchronously; TinyBase's load listener can defer snapshots and skip saves.
				lastContent = cloneContent(content);
				applied = true;
				if (autoLoading) {
					store.setContent(cloneContent(content));
				}

				return true;
			});
		} catch (error) {
			lastContent = previousContent;
			if (applied && autoLoading) {
				store.setContent(mergeContentChanges(previousStore, content, store.getContent()));
			}

			throw error;
		}
	};

	const pullTable = async (tableId: string): Promise<void> => {
		const tableConfig = tableConfigs[tableId];
		if (!tableConfig) {
			return;
		}

		const updatedAtColumn = tableConfig.updatedAtColumn;
		const cursorKey = JSON.stringify([
			tableId,
			tableConfig.table,
			tableConfig.idColumn ?? 'id',
			tableConfig.deletedAtColumn ?? 'deleted_at',
			updatedAtColumn ?? null,
			tableConfig.select ?? '*',
			tableConfig.cursorVersion ?? '',
		]);
		const cursor = updatedAtColumn ? await state.getCursor(cursorKey) : undefined;
		const cursorTime = cursor ? Date.parse(cursor.updatedAt) : Number.NaN;
		const pullCursor =
			cursor && cursorLookbackMs > 0 && Number.isFinite(cursorTime)
				? { updatedAt: new Date(cursorTime - cursorLookbackMs).toISOString() }
				: cursor;
		const { cursor: pulledCursor, rows } = await transport.fetchRows(tableConfig, pullCursor);
		const pulledCursorTime = pulledCursor ? Date.parse(pulledCursor.updatedAt) : Number.NaN;
		const nextCursor = cursor && pulledCursorTime <= cursorTime ? cursor : pulledCursor;

		while (!isDestroyed && !terminalError) {
			const applied = await persistSerially(async () => {
				const revision = localRevision;
				if (autoLoading && hasHydrated) {
					await persistLocalChanges();
				}

				const content = cloneContent((await state.getContent()) ?? lastContent);
				const table: Table = { ...getRows(content, tableId) };
				const [pending, blocked] = await Promise.all([
					state.getOperations(),
					state.getBlockedOperations(),
				]);
				const blockedIds = new Set(
					[...pending, ...blocked].map((operation) => operation.id),
				);
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

				return applyRemoteContent([tables, content[1]], cursorKey, nextCursor, revision);
			});

			if (applied) {
				return;
			}
		}
	};

	const flushOutbox = async (): Promise<void> => {
		const operations = sortOperations(await state.getOperations(), tableConfigs);
		for (const operation of operations) {
			if (isDestroyed || terminalError) {
				return;
			}

			const tableConfig = tableConfigs[operation.tableId];
			if (!tableConfig) {
				await state.removeOperation(operation);
				continue;
			}

			try {
				await transport.upsert(tableConfig, operation.payload);
				if (isDestroyed || terminalError) {
					return;
				}

				await state.removeOperation(operation);
			} catch (error) {
				if (isDestroyed || terminalError) {
					return;
				}

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
		if (isDestroyed || terminalError) {
			return;
		}

		const delay = Math.min(retryBaseDelayMs * 2 ** retryAttempt, retryMaxDelayMs);
		retryAttempt += 1;
		scheduler.schedule(delay);
	};

	const reportSyncError = async (error: unknown): Promise<void> => {
		if (isDestroyed || terminalError) {
			return;
		}

		const normalized = asError(error);
		config.onError?.(normalized);
		await setStatus('offline', normalized);
		scheduleRetry();
	};

	const completeSync = async (): Promise<void> => {
		if (isDestroyed || terminalError) {
			return;
		}

		clearRetry();
		await setStatus('idle');
	};

	const reconcile = async (markIdle = true): Promise<boolean> => {
		if (isDestroyed || terminalError) {
			return false;
		}

		await setStatus('syncing');
		try {
			await flushOutbox();
			for (const tableId of Object.keys(tableConfigs)) {
				if (isDestroyed || terminalError) {
					return false;
				}

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

	const syncNow = (): Promise<void> => runWhileUsable(() => scheduler.runNow());

	const schedulePull = (tableId: string): void => {
		if (terminalError) {
			return;
		}

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
		await runWhileUsable(async () => {
			if (isDestroyed || hasStartedSyncing) {
				return;
			}

			hasStartedSyncing = true;
			startRealtime();
			await scheduler.start(config.pollIntervalMs ?? defaultPollIntervalMs);
		});
	};

	const terminate = (error: IndexedDbConnectionClosedForUpgradeError): void => {
		if (terminalError) {
			return;
		}

		terminalError = error;
		hasStartedSyncing = false;
		clearRetry();
		scheduler.stop();
		const terminalChannels = channels.splice(0);
		void Promise.all(terminalChannels.map((channel) => transport.unsubscribe(channel))).catch(
			(cleanupError: unknown) => config.onError?.(asError(cleanupError)),
		);
		void stopAutoPersistence?.().catch((cleanupError: unknown) =>
			config.onError?.(asError(cleanupError)),
		);
		status = {
			...status,
			lastError: error,
			phase: 'error',
		};

		onTerminal?.(error);
		emitStatus();
	};

	basePersister = createCustomPersister(
		store,
		async () => {
			await persistSerially(async () => {
				const before = hasHydrated ? cloneContent(store.getContent()) : initialStoreContent;
				const persisted = await state.getContent();
				loadedPersistedContent = persisted !== undefined;
				loadFallbackBaseline = before;
				const content = cloneContent(persisted ?? store.getContent());
				const current = store.getContent();
				const merged = mergeContentChanges(content, before, current);

				if (!isDestroyed && !terminalError) {
					lastContent = content;
					store.setContent(merged);
					hasHydrated = true;
					await persistLocalChanges();
				}
			});

			// Application occurs above; TinyBase has no deferred snapshot to apply.
			return undefined;
		},
		async () => {
			await persistSerially(async () => {
				const content = cloneContent(store.getContent());
				await persistContent(content, createOperations(content));
			});
		},
		() => {
			autoLoading = true;
			return true;
		},
		() => {
			autoLoading = false;
		},
		config.onError,
	);

	const baseDestroy = basePersister.destroy.bind(basePersister);
	const baseLoad = basePersister.load.bind(basePersister);
	const baseSave = basePersister.save.bind(basePersister);
	const baseSchedule = basePersister.schedule.bind(basePersister);
	const baseStartAutoLoad = basePersister.startAutoLoad.bind(basePersister);
	const baseStartAutoPersisting = basePersister.startAutoPersisting.bind(basePersister);
	const baseStartAutoSave = basePersister.startAutoSave.bind(basePersister);
	const baseStopAutoPersisting = basePersister.stopAutoPersisting.bind(basePersister);
	stopAutoPersistence = () => baseStopAutoPersisting(true);
	const guardBaseMethod =
		<Arguments extends unknown[], Value>(
			method: (...arguments_: Arguments) => Promise<Value>,
		): ((...arguments_: Arguments) => Promise<Value>) =>
		(...arguments_) =>
			runWhileUsable(() => method(...arguments_));
	const baseMethods = Object.fromEntries(
		Object.entries(basePersister).map(([name, method]) => [
			name,
			(method as (...arguments_: never[]) => unknown).bind(basePersister),
		]),
	);
	let result: StandardPersister;
	result = Object.assign(baseMethods as unknown as Persister, {
		assertUsable,
		addSyncStatusListener(nextListener: (nextStatus: SyncStatus) => void): () => void {
			statusListeners.add(nextListener);
			notifyStatusListener(nextListener);
			return () => statusListeners.delete(nextListener);
		},
		completeSync,
		async discardRejected(): Promise<void> {
			await runWhileUsable(async () => {
				await state.discardRejected();
				await setStatus(status.phase, status.lastError);
			});
		},
		async destroy(): Promise<StandardPersister> {
			isDestroyed = true;
			await stopSyncing();
			await baseDestroy();
			await persistence;
			store.delListener(revisionListener);
			state.close();
			return result;
		},
		async getRejectedOperations(): Promise<readonly RejectedOperation[]> {
			return runWhileUsable(async () =>
				(await state.getRejected()).map(({ error, rowId, tableId }) => ({
					error,
					rowId,
					tableId,
				})),
			);
		},
		getSyncStatus(): SyncStatus {
			return status;
		},
		async isRowBlocked(tableId: string, rowId: string): Promise<boolean> {
			return runWhileUsable(async () => {
				const id = operationId(tableId, rowId);
				const [pending, rejected] = await Promise.all([
					state.getOperations(),
					state.getRejected(),
				]);
				return (
					pending.some((operation) => operation.id === id) ||
					rejected.some((row) => row.id === id)
				);
			});
		},
		load: (initialContent?: Content | (() => Content)) =>
			runWhileUsable(() => baseLoad(loadFallback(initialContent))),
		async retryRejected(): Promise<void> {
			await runWhileUsable(async () => {
				await state.retryRejected();
				await syncNow();
			});
		},
		reconcile,
		reportSyncError,
		save: (...arguments_: Parameters<Persister['save']>) =>
			runWhileUsable(async () => {
				if (basePersister.getStatus() === 1) {
					await persistSerially(persistLocalChanges);
					return basePersister;
				}

				return baseSave(...arguments_);
			}),
		schedule: guardBaseMethod(baseSchedule),
		startAutoLoad: (initialContent?: Content | (() => Content)) =>
			runWhileUsable(() => baseStartAutoLoad(loadFallback(initialContent))),
		startAutoSave: guardBaseMethod(baseStartAutoSave),
		startSyncing,
		async startAutoPersisting(): Promise<StandardPersister> {
			try {
				return await runWhileUsable(async () => {
					await baseStartAutoPersisting();
					assertUsable();
					await startSyncing();
					return result;
				});
			} catch (error) {
				if (terminalError) {
					await baseStopAutoPersisting(true);
					throw terminalError;
				}

				throw error;
			}
		},
		stopSyncing,
		syncNow,
		terminate,
	}) as unknown as StandardPersister;

	handleConnectionClosedForUpgrade = terminate;
	if (pendingTerminalError) {
		terminate(pendingTerminalError);
	}

	await setStatus('idle');
	return result;
};
