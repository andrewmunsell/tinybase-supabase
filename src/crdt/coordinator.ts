import type { Row, Store } from 'tinybase';
import * as Y from 'yjs';
import { type BufferedCrdtUpdate, CrdtLocalState, type StoredCrdtUpdate } from '../crdt-storage.js';
import type { CrdtCellConfig, CrdtRowHandle, RejectedOperation } from '../types.js';
import { getDocumentKey, getProjection, remoteOrigin } from './codec.js';
import type { ConfiguredCrdtTable } from './config.js';
import {
	CrdtTransport,
	CrdtTransportError,
	type CrdtRealtimeChannel,
	type SupabaseError,
} from './transport.js';

interface OpenDocument {
	readonly channel?: CrdtRealtimeChannel;
	readonly document: Y.Doc;
	readonly handle: CrdtRowHandle;
	readonly pendingWrites: Set<Promise<void>>;
	readonly rowId: string;
	readonly tableId: string;
}

const permanentPostgresErrorCodes = new Set(['22P02', '23503', '23505', '23514', '42501']);
const isPermanentError = (error: SupabaseError): boolean =>
	(error.status !== undefined && error.status >= 400 && error.status < 500) ||
	(error.code !== undefined && permanentPostgresErrorCodes.has(error.code));

export interface CrdtCoordinator {
	closeRow(tableId: string, rowId: string): Promise<void>;
	destroy(): Promise<void>;
	discardRejected(): Promise<void>;
	flushOutbox(isRowBlocked: (tableId: string, rowId: string) => Promise<boolean>): Promise<void>;
	flushBufferedUpdates(): Promise<void>;
	getPendingCount(): number;
	getProjection(tableId: string, rowId: string): Row | undefined;
	getRejected(): Promise<readonly RejectedOperation[]>;
	getRejectedCount(): number;
	isRowOpen(tableId: string, rowId: string): boolean;
	openRow(tableId: string, rowId: string): Promise<CrdtRowHandle>;
	pullOpenDocuments(): Promise<void>;
	retryRejected(): Promise<void>;
	startSyncing(): Promise<void>;
	stopSyncing(): Promise<void>;
}

export const createCrdtCoordinator = async (
	store: Store,
	config: {
		readonly crdtUpdateBufferMs?: number;
		readonly databaseName: string;
		readonly onError?: (error: Error) => void;
		readonly pageSize?: number;
		readonly scopeKey: string;
		readonly supabase: unknown;
	},
	tables: Readonly<Record<string, ConfiguredCrdtTable>>,
	requestSync: () => Promise<void>,
	scheduleSync: (delayMs: number) => void,
	onStatusChange: () => void,
): Promise<CrdtCoordinator> => {
	const state = await CrdtLocalState.open(config.databaseName, config.scopeKey);
	const transport = new CrdtTransport(config.supabase, config.pageSize ?? 500, config.scopeKey);
	const documents = new Map<string, OpenDocument>();
	const openingDocuments = new Map<string, Promise<CrdtRowHandle>>();
	const pendingSubscriptions = new Map<string, AbortController>();
	const subscribingDocuments = new Map<string, Promise<OpenDocument>>();
	const rowGenerations = new Map<string, number>();
	const updateBufferMs = Math.max(0, config.crdtUpdateBufferMs ?? 500);
	let destroyed = false;
	let started = false;
	let syncGeneration = 0;
	const [initialOutbox, initialBuffer] = await Promise.all([
		state.getOutbox(),
		state.getBuffered(),
	]);
	let pendingCount = initialOutbox.length + initialBuffer.length;
	let rejectedCount = (await state.getRejected()).length;
	const reportError = (error: unknown): void =>
		config.onError?.(error instanceof Error ? error : new Error(String(error)));
	const waitForPendingWrites = async (): Promise<void> => {
		await Promise.all(
			[...documents.values()].flatMap(({ pendingWrites }) => [...pendingWrites]),
		);
	};

	const scheduleBufferedUpdates = async (): Promise<void> => {
		if (!started) {
			return;
		}
		const buffered = await state.getBuffered();
		if (buffered.length > 0) {
			const firstBufferedAt = Math.min(...buffered.map(({ bufferedAt }) => bufferedAt));
			scheduleSync(Math.max(0, firstBufferedAt + updateBufferMs - Date.now()));
		}
	};

	const compactBufferedUpdates = async (force: boolean): Promise<void> => {
		const buffered = await state.getBuffered();
		const byDocument = new Map<string, BufferedCrdtUpdate[]>();
		for (const update of buffered) {
			const updates = byDocument.get(update.documentKey) ?? [];
			updates.push(update);
			byDocument.set(update.documentKey, updates);
		}
		const now = Date.now();
		for (const updates of byDocument.values()) {
			updates.sort(
				(first, second) =>
					first.bufferedAt - second.bufferedAt || first.id.localeCompare(second.id),
			);
			const first = updates[0];
			if (!first || (!force && first.bufferedAt + updateBufferMs > now)) {
				continue;
			}
			const merged: StoredCrdtUpdate = {
				documentKey: first.documentKey,
				id: updates.length === 1 ? first.id : crypto.randomUUID(),
				rowId: first.rowId,
				tableId: first.tableId,
				update:
					updates.length === 1
						? first.update
						: Y.mergeUpdates(updates.map(({ update }) => update)),
			};
			await state.promoteBuffered(
				updates.map(({ id }) => id),
				merged,
			);
			pendingCount -= updates.length - 1;
			if (updates.length > 1) {
				onStatusChange();
			}
		}
		await scheduleBufferedUpdates();
	};

	const getOpenProjection = (open: OpenDocument): Row =>
		Object.fromEntries(
			Object.entries(tables[open.tableId]?.crdtCells ?? {}).map(([cellId, cellConfig]) => [
				cellId,
				getProjection(open.document, cellId, cellConfig),
			]),
		) as Row;

	const project = (open: OpenDocument): void => {
		if (
			documents.get(getDocumentKey(open.tableId, open.rowId))?.document === open.document &&
			store.hasRow(open.tableId, open.rowId)
		) {
			store.setPartialRow(open.tableId, open.rowId, getOpenProjection(open));
		}
	};

	const pull = async (open: OpenDocument): Promise<void> => {
		const table = tables[open.tableId];
		if (!table) {
			return;
		}
		for (const update of await transport.fetchUpdates(open.tableId, open.rowId, table)) {
			if (await state.hasUpdate(update.id)) {
				continue;
			}
			Y.applyUpdate(open.document, update.update, remoteOrigin);
			await state.persistRemoteUpdate(update);
		}
		project(open);
	};

	const subscribeInternal = async (open: OpenDocument): Promise<OpenDocument> => {
		const table = tables[open.tableId];
		if (!started || !table?.realtime || open.channel) {
			return open;
		}
		const key = getDocumentKey(open.tableId, open.rowId);
		const expectedRowGeneration = rowGenerations.get(key) ?? 0;
		const expectedSyncGeneration = syncGeneration;
		const delay = typeof table.realtime === 'object' ? (table.realtime.debounceMs ?? 200) : 200;
		const controller = new AbortController();
		pendingSubscriptions.set(key, controller);
		let channel: CrdtRealtimeChannel;
		try {
			channel = await transport.subscribe(
				open.tableId,
				open.rowId,
				table,
				() => scheduleSync(delay),
				controller.signal,
			);
		} catch (error) {
			if (
				controller.signal.aborted &&
				!destroyed &&
				(rowGenerations.get(key) ?? 0) === expectedRowGeneration &&
				documents.get(key)?.document === open.document &&
				!started
			) {
				return open;
			}
			throw error;
		} finally {
			if (pendingSubscriptions.get(key) === controller) {
				pendingSubscriptions.delete(key);
			}
		}
		if (
			destroyed ||
			(rowGenerations.get(key) ?? 0) !== expectedRowGeneration ||
			documents.get(key)?.document !== open.document
		) {
			await transport.unsubscribe(channel);
			throw new Error(`CRDT row was closed while opening: ${open.tableId}.${open.rowId}`);
		}
		if (!started || syncGeneration !== expectedSyncGeneration) {
			await transport.unsubscribe(channel);
			return open;
		}
		const subscribed: OpenDocument = {
			...open,
			channel,
		};
		documents.set(key, subscribed);
		return subscribed;
	};
	const subscribe = (open: OpenDocument): Promise<OpenDocument> => {
		const key = getDocumentKey(open.tableId, open.rowId);
		const existing = subscribingDocuments.get(key);
		if (existing) {
			return existing;
		}
		const promise = subscribeInternal(open).finally(() => {
			if (subscribingDocuments.get(key) === promise) {
				subscribingDocuments.delete(key);
			}
		});
		subscribingDocuments.set(key, promise);
		return promise;
	};

	const closeRow = async (tableId: string, rowId: string): Promise<void> => {
		const key = getDocumentKey(tableId, rowId);
		rowGenerations.set(key, (rowGenerations.get(key) ?? 0) + 1);
		pendingSubscriptions.get(key)?.abort();
		await openingDocuments.get(key)?.catch(() => undefined);
		const open = documents.get(key);
		if (open) {
			await Promise.all(open.pendingWrites);
			if (open.channel) {
				await transport.unsubscribe(open.channel);
			}
			open.document.destroy();
			if (documents.get(key)?.document === open.document) {
				documents.delete(key);
			}
		}
		const table = tables[tableId];
		if (table && store.hasRow(tableId, rowId)) {
			store.transaction(() => {
				for (const cellId of Object.keys(table.crdtCells)) {
					store.delCell(tableId, rowId, cellId);
				}
			});
		}
	};

	const openRowInternal = async (
		tableId: string,
		rowId: string,
		rowGeneration: number,
	): Promise<CrdtRowHandle> => {
		const key = getDocumentKey(tableId, rowId);
		const table = tables[tableId];
		if (!table) {
			throw new Error(`Table ${tableId} has no configured CRDT cells`);
		}
		if (!store.hasRow(tableId, rowId)) {
			throw new Error(`Cannot open missing CRDT row: ${tableId}.${rowId}`);
		}

		const document = new Y.Doc();
		for (const [cellId, cellConfig] of Object.entries(table.crdtCells)) {
			getProjection(document, cellId, cellConfig);
		}
		for (const update of await state.getDocumentUpdates(key)) {
			Y.applyUpdate(document, update.update, remoteOrigin);
		}
		if (destroyed || (rowGenerations.get(key) ?? 0) !== rowGeneration) {
			document.destroy();
			throw new Error(`CRDT row was closed while opening: ${tableId}.${rowId}`);
		}
		const pendingWrites = new Set<Promise<void>>();
		const assertType = (cellId: string, expected: CrdtCellConfig['type']): void => {
			const actual = table.crdtCells[cellId]?.type;
			if (actual !== expected) {
				throw new Error(
					`CRDT cell ${tableId}.${cellId} is ${actual ?? 'not configured'}, not ${expected}`,
				);
			}
		};
		let open: OpenDocument;
		const handle: CrdtRowHandle = {
			async destroy() {
				await closeRow(tableId, rowId);
			},
			getArray<T = unknown>(cellId: string): Y.Array<T> {
				assertType(cellId, 'array');
				return document.getArray<T>(cellId);
			},
			getMap<T = unknown>(cellId: string): Y.Map<T> {
				assertType(cellId, 'map');
				return document.getMap<T>(cellId);
			},
			getText(cellId: string): Y.Text {
				assertType(cellId, 'text');
				return document.getText(cellId);
			},
		};
		open = { document, handle, pendingWrites, rowId, tableId };
		documents.set(key, open);
		document.on('update', (bytes: Uint8Array, origin: unknown) => {
			if (origin === remoteOrigin) {
				return;
			}
			const update: BufferedCrdtUpdate = {
				bufferedAt: Date.now(),
				documentKey: key,
				id: crypto.randomUUID(),
				rowId,
				tableId,
				update: bytes,
			};
			pendingCount += 1;
			onStatusChange();
			const write = state
				.persistLocalUpdate(update)
				.then(() => {
					if (started) {
						scheduleSync(Math.max(0, update.bufferedAt + updateBufferMs - Date.now()));
					}
				})
				.catch((error: unknown) => {
					pendingCount -= 1;
					onStatusChange();
					reportError(error);
				})
				.finally(() => pendingWrites.delete(write));
			pendingWrites.add(write);
		});
		document.on('afterTransaction', () => project(open));
		try {
			let transientError: unknown;
			try {
				open = await subscribe(open);
			} catch (error) {
				transientError = error;
			}
			try {
				await pull(open);
			} catch (error) {
				if (!(error instanceof CrdtTransportError) || isPermanentError(error)) {
					throw error;
				}
				transientError ??= error;
			}
			if (
				destroyed ||
				(rowGenerations.get(key) ?? 0) !== rowGeneration ||
				documents.get(key)?.document !== document
			) {
				throw new Error(`CRDT row was closed while opening: ${tableId}.${rowId}`);
			}
			project(open);
			if (transientError) {
				if (started) {
					void requestSync().catch(reportError);
				} else {
					reportError(transientError);
				}
			}
			return handle;
		} catch (error) {
			if (documents.get(key)?.document === document) {
				documents.delete(key);
			}
			if (open.channel) {
				await transport.unsubscribe(open.channel);
			}
			document.destroy();
			throw error;
		}
	};

	const openRow = (tableId: string, rowId: string): Promise<CrdtRowHandle> => {
		if (destroyed) {
			return Promise.reject(new Error('CRDT coordinator has been destroyed'));
		}
		const key = getDocumentKey(tableId, rowId);
		const opening = openingDocuments.get(key);
		if (opening) {
			return opening;
		}
		const existing = documents.get(key);
		if (existing) {
			return Promise.resolve(existing.handle);
		}
		const rowGeneration = rowGenerations.get(key) ?? 0;
		const promise = openRowInternal(tableId, rowId, rowGeneration).finally(() =>
			openingDocuments.delete(key),
		);
		openingDocuments.set(key, promise);
		return promise;
	};

	return {
		closeRow,
		async destroy() {
			destroyed = true;
			started = false;
			syncGeneration += 1;
			for (const key of [...openingDocuments.keys()]) {
				const [tableId, rowId] = key.split('\0');
				if (tableId !== undefined && rowId !== undefined) {
					await closeRow(tableId, rowId);
				}
			}
			for (const open of [...documents.values()]) {
				await closeRow(open.tableId, open.rowId);
			}
			state.close();
		},
		async discardRejected() {
			await state.discardRejected();
			rejectedCount = 0;
			onStatusChange();
		},
		async flushOutbox(isRowBlocked) {
			await compactBufferedUpdates(false);
			for (const update of await state.getOutbox()) {
				const table = tables[update.tableId];
				if (!table) {
					await state.removeOutbox(update.id);
					pendingCount -= 1;
					onStatusChange();
					continue;
				}
				if (await isRowBlocked(update.tableId, update.rowId)) {
					continue;
				}
				try {
					await transport.insertUpdate(update, table);
					await state.removeOutbox(update.id);
					pendingCount -= 1;
					onStatusChange();
				} catch (error) {
					if (isPermanentError(error as SupabaseError)) {
						await state.reject(update, (error as SupabaseError).message);
						pendingCount -= 1;
						rejectedCount += 1;
						onStatusChange();
						continue;
					}
					throw error;
				}
			}
		},
		async flushBufferedUpdates() {
			await waitForPendingWrites();
			await compactBufferedUpdates(true);
		},
		getPendingCount: () => pendingCount,
		getProjection(tableId, rowId) {
			const open = documents.get(getDocumentKey(tableId, rowId));
			return open ? getOpenProjection(open) : undefined;
		},
		async getRejected() {
			return (await state.getRejected()).map(({ error, rowId, tableId }) => ({
				error,
				rowId,
				tableId,
			}));
		},
		getRejectedCount: () => rejectedCount,
		isRowOpen: (tableId, rowId) => documents.has(getDocumentKey(tableId, rowId)),
		openRow,
		async pullOpenDocuments() {
			for (const open of [...documents.values()]) {
				await pull(await subscribe(open));
			}
		},
		async retryRejected() {
			await state.retryRejected();
			pendingCount += rejectedCount;
			rejectedCount = 0;
			onStatusChange();
		},
		async startSyncing() {
			if (started) {
				return;
			}
			started = true;
			syncGeneration += 1;
			for (const open of [...documents.values()]) {
				await subscribe(open);
			}
			await scheduleBufferedUpdates();
		},
		async stopSyncing() {
			started = false;
			syncGeneration += 1;
			for (const controller of pendingSubscriptions.values()) {
				controller.abort();
			}
			for (const [key, open] of documents) {
				if (!open.channel) {
					continue;
				}
				await transport.unsubscribe(open.channel);
				const { channel: _channel, ...closed } = open;
				documents.set(key, closed);
			}
		},
	};
};
