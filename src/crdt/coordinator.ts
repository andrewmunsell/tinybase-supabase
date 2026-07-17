import type { Row, Store } from 'tinybase';
import * as Y from 'yjs';
import { type BufferedCrdtUpdate, CrdtLocalState, type StoredCrdtUpdate } from '../crdt-storage.js';
import {
	type IndexedDbConnectionClosedForUpgradeError,
	isIndexedDbConnectionClosedException,
} from '../indexeddb-errors.js';
import type { CrdtCellConfig, CrdtRowHandle, RejectedOperation } from '../types.js';
import { getDocumentKey, getProjection, remoteOrigin } from './codec.js';
import type { ConfiguredCrdtTable } from './config.js';
import {
	type CrdtRealtimeChannel,
	CrdtTransport,
	CrdtTransportError,
	type SupabaseError,
} from './transport.js';

interface OpenDocument {
	readonly channel?: CrdtRealtimeChannel;
	readonly document: Y.Doc;
	readonly handle: CrdtRowHandle;
	readonly lifecycle: { active: boolean };
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
	terminate(error: IndexedDbConnectionClosedForUpgradeError): void;
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
	onTerminal: (error: IndexedDbConnectionClosedForUpgradeError) => void,
): Promise<CrdtCoordinator> => {
	let handleConnectionClosedForUpgrade:
		| ((error: IndexedDbConnectionClosedForUpgradeError) => void)
		| undefined;
	let pendingTerminalError: IndexedDbConnectionClosedForUpgradeError | undefined;
	const state = await CrdtLocalState.open(
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
	let terminalError: IndexedDbConnectionClosedForUpgradeError | undefined;
	const readOnlyTableIds = new Set(
		Object.entries(tables)
			.filter(([, table]) => table.mode === 'read-only')
			.map(([tableId]) => tableId),
	);
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
	await readDuringInitialization(() => state.discardTableLocalState(readOnlyTableIds), {
		pendingCount: 0,
		rejectedCount: 0,
	});
	const [initialOutbox, initialBuffer, initialRejected] = await Promise.all([
		readDuringInitialization(() => state.getOutbox(), []),
		readDuringInitialization(() => state.getBuffered(), []),
		readDuringInitialization(() => state.getRejected(), []),
	]);
	let pendingCount = initialOutbox.length + initialBuffer.length;
	let rejectedCount = initialRejected.length;
	const reportError = (error: unknown): void =>
		config.onError?.(error instanceof Error ? error : new Error(String(error)));
	const assertUsable = (): void => {
		if (terminalError) {
			throw terminalError;
		}
	};
	const waitForPendingWrites = async (): Promise<void> => {
		await Promise.all(
			[...documents.values()].flatMap(({ pendingWrites }) => [...pendingWrites]),
		);
	};

	const scheduleBufferedUpdates = async (): Promise<void> => {
		if (!started || terminalError) {
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
			if (!first) {
				continue;
			}
			const table = tables[first.tableId];
			if (!table || table.mode === 'read-only') {
				const discarded = await state.discardDocuments([first.documentKey]);
				pendingCount -= discarded.pendingCount;
				rejectedCount -= discarded.rejectedCount;
				onStatusChange();
				continue;
			}
			if (!force && first.bufferedAt + updateBufferMs > now) {
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

	const coalesceOutboxUpdates = async (): Promise<StoredCrdtUpdate[]> => {
		const byDocument = new Map<string, StoredCrdtUpdate[]>();
		for (const update of await state.getOutbox()) {
			const updates = byDocument.get(update.documentKey) ?? [];
			updates.push(update);
			byDocument.set(update.documentKey, updates);
		}
		const coalesced: StoredCrdtUpdate[] = [];
		for (const updates of byDocument.values()) {
			const first = updates[0];
			if (!first) {
				continue;
			}
			if (updates.length === 1) {
				coalesced.push(first);
				continue;
			}
			const merged: StoredCrdtUpdate = {
				documentKey: first.documentKey,
				id: crypto.randomUUID(),
				rowId: first.rowId,
				tableId: first.tableId,
				update: Y.mergeUpdates(updates.map(({ update }) => update)),
			};
			await state.replaceOutboxUpdates(
				updates.map(({ id }) => id),
				merged,
			);
			pendingCount -= updates.length - 1;
			onStatusChange();
			coalesced.push(merged);
		}
		return coalesced;
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
			open.lifecycle.active &&
			documents.get(getDocumentKey(open.tableId, open.rowId))?.document === open.document &&
			store.hasRow(open.tableId, open.rowId)
		) {
			store.setPartialRow(open.tableId, open.rowId, getOpenProjection(open));
		}
	};

	const pull = async (open: OpenDocument): Promise<void> => {
		assertUsable();
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
		assertUsable();
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
				() => {
					if (!terminalError) {
						scheduleSync(delay);
					}
				},
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
			open.lifecycle.active = false;
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
		const lifecycle = { active: true };
		const assertWritable = (): void => {
			assertUsable();
			if (!lifecycle.active) {
				throw new Error(`CRDT row ${tableId}.${rowId} is closed`);
			}
			if (table.mode === 'read-only') {
				throw new Error(`CRDT row ${tableId}.${rowId} is read-only`);
			}
		};
		const mutatingMethods = new Set<PropertyKey>([
			'applyDelta',
			'clear',
			'delete',
			'format',
			'insert',
			'insertAfter',
			'insertEmbed',
			'push',
			'removeAttribute',
			'set',
			'setAttribute',
			'unshift',
		]);
		const callbackMethods = new Set<PropertyKey>(['forEach', 'map']);
		const observerMethods = new Set<PropertyKey>(['observe', 'observeDeep']);
		const unobserverMethods = new Set<PropertyKey>(['unobserve', 'unobserveDeep']);
		const guardedTypes = new WeakMap<object, object>();
		const guardedProxies = new WeakSet<object>();
		const guardedDocuments = new WeakMap<Y.Doc, Y.Doc>();
		const guardedDocumentProxies = new WeakSet<Y.Doc>();
		const guardedEvents = new WeakMap<object, object>();
		const guardedTransactions = new WeakMap<Y.Transaction, object>();
		const instrumentedTypes = new WeakSet<object>();
		const instrumentedValues = new WeakSet<object>();
		const observerCallbacks = new WeakMap<
			object,
			Map<PropertyKey, WeakMap<object, (...args: unknown[]) => unknown>>
		>();
		const instrumentDocument = (value: Y.Doc): Y.Doc => {
			if (guardedDocumentProxies.has(value)) {
				return value;
			}
			const existing = guardedDocuments.get(value);
			if (existing) {
				return existing;
			}
			let transaction: unknown = Reflect.get(value, '_transaction', value);
			Object.defineProperty(value, '_transaction', {
				configurable: true,
				get: () => transaction,
				set: (nextTransaction: unknown) => {
					if (nextTransaction !== null) {
						assertUsable();
						if (
							!(nextTransaction instanceof Y.Transaction) ||
							nextTransaction.origin !== remoteOrigin
						) {
							assertWritable();
						}
					}
					transaction = nextTransaction;
				},
			});
			const guarded = new Proxy(value, {
				get(target, property) {
					const item: unknown = Reflect.get(target, property, target);
					if (property === 'transact' && typeof item === 'function') {
						return (...args: unknown[]) => {
							assertWritable();
							return guardReturnedValue(Reflect.apply(item, target, args));
						};
					}
					return typeof item === 'function'
						? (...args: unknown[]) =>
								guardReturnedValue(Reflect.apply(item, target, args))
						: item;
				},
				set(target, property, nextValue) {
					if (property === '_transaction' && nextValue !== null) {
						assertWritable();
					}
					return Reflect.set(target, property, nextValue, target);
				},
			});
			guardedDocuments.set(value, guarded);
			guardedDocumentProxies.add(guarded);
			return guarded;
		};
		const instrumentValue = (value: unknown): void => {
			if (typeof value !== 'object' || value === null || guardedProxies.has(value)) {
				return;
			}
			if (value instanceof Y.AbstractType) {
				guardMutations(value);
				return;
			}
			if (value instanceof Y.Doc) {
				instrumentDocument(value);
				return;
			}
			if (instrumentedValues.has(value)) {
				return;
			}
			instrumentedValues.add(value);
			if (value instanceof Y.YEvent) {
				instrumentValue(value.target);
				instrumentValue(value.currentTarget);
				instrumentValue(value.transaction);
				return;
			}
			if (value instanceof Y.Transaction) {
				instrumentDocument(value.doc);
				for (const type of value.changed.keys()) {
					instrumentValue(type);
				}
				for (const [type, events] of value.changedParentTypes) {
					instrumentValue(type);
					instrumentValue(events);
				}
				return;
			}
			if (Array.isArray(value)) {
				for (const item of value) {
					instrumentValue(item);
				}
				return;
			}
			for (const item of Object.values(value)) {
				instrumentValue(item);
			}
		};
		const guardReturnedValue = (value: unknown): unknown => {
			instrumentValue(value);
			if (value instanceof Y.AbstractType) {
				return guardMutations(value);
			}
			if (value instanceof Y.Doc) {
				return instrumentDocument(value);
			}
			if (value instanceof Y.YEvent) {
				const existing = guardedEvents.get(value);
				if (existing) {
					return existing;
				}
				const guarded = new Proxy(value, {
					get(target, property) {
						const item: unknown = Reflect.get(target, property, target);
						return typeof item === 'function'
							? (...args: unknown[]) =>
									guardReturnedValue(Reflect.apply(item, target, args))
							: guardReturnedValue(item);
					},
				});
				guardedEvents.set(value, guarded);
				return guarded;
			}
			if (value instanceof Y.Transaction) {
				const existing = guardedTransactions.get(value);
				if (existing) {
					return existing;
				}
				const guarded = new Proxy(value, {
					get(target, property) {
						return guardReturnedValue(Reflect.get(target, property, target));
					},
				});
				guardedTransactions.set(value, guarded);
				return guarded;
			}
			if (Array.isArray(value)) {
				return value.map(guardReturnedValue);
			}
			if (
				typeof value === 'object' &&
				value !== null &&
				typeof (value as Partial<Iterator<unknown>>).next === 'function' &&
				typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function'
			) {
				const iterator = value as Iterator<unknown>;
				const guardedIterator: IterableIterator<unknown> = {
					next() {
						const result = iterator.next();
						return result.done
							? result
							: { done: false, value: guardReturnedValue(result.value) };
					},
					[Symbol.iterator]() {
						return guardedIterator;
					},
				};
				return guardedIterator;
			}
			return value;
		};
		const guardMutations = <SharedType extends object>(sharedType: SharedType): SharedType => {
			if (guardedProxies.has(sharedType)) {
				return sharedType;
			}
			const existing = guardedTypes.get(sharedType);
			if (existing) {
				return existing as SharedType;
			}
			if (sharedType instanceof Y.AbstractType && !instrumentedTypes.has(sharedType)) {
				instrumentedTypes.add(sharedType);
				if (sharedType.doc) {
					instrumentDocument(sharedType.doc);
				}
				for (const property of mutatingMethods) {
					const method: unknown = Reflect.get(sharedType, property, sharedType);
					if (typeof method !== 'function') {
						continue;
					}
					Object.defineProperty(sharedType, property, {
						configurable: true,
						value: (...args: unknown[]) => {
							assertWritable();
							for (const argument of args) {
								instrumentValue(argument);
							}
							return Reflect.apply(method, sharedType, args);
						},
					});
				}
				if (sharedType.doc) {
					const descendants: unknown[] = [];
					if (sharedType instanceof Y.Map) {
						descendants.push(...sharedType.values());
					}
					if (sharedType instanceof Y.Array || sharedType instanceof Y.XmlFragment) {
						descendants.push(...sharedType.toArray());
					}
					if (sharedType instanceof Y.Text) {
						for (const operation of sharedType.toDelta()) {
							descendants.push(operation.insert);
						}
					}
					for (const descendant of descendants) {
						instrumentValue(descendant);
					}
				}
			}
			const guarded = new Proxy(sharedType, {
				get(target, property) {
					const value: unknown = Reflect.get(target, property, target);
					if (typeof value !== 'function' || property === 'constructor') {
						return guardReturnedValue(value);
					}
					if (mutatingMethods.has(property)) {
						return (...args: unknown[]) => {
							assertWritable();
							return guardReturnedValue(Reflect.apply(value, target, args));
						};
					}
					return (...args: unknown[]) => {
						if (observerMethods.has(property) && typeof args[0] === 'function') {
							const callback = args[0];
							let byMode = observerCallbacks.get(target);
							if (!byMode) {
								byMode = new Map();
								observerCallbacks.set(target, byMode);
							}
							let byCallback = byMode.get(property);
							if (!byCallback) {
								byCallback = new WeakMap();
								byMode.set(property, byCallback);
							}
							let guardedCallback = byCallback.get(callback);
							if (!guardedCallback) {
								guardedCallback = function (
									this: unknown,
									...callbackArguments: unknown[]
								) {
									return Reflect.apply(
										callback,
										this,
										callbackArguments.map(guardReturnedValue),
									);
								};
								byCallback.set(callback, guardedCallback);
							}
							args[0] = guardedCallback;
						} else if (
							unobserverMethods.has(property) &&
							typeof args[0] === 'function'
						) {
							const observerProperty =
								property === 'unobserve' ? 'observe' : 'observeDeep';
							args[0] =
								observerCallbacks
									.get(target)
									?.get(observerProperty)
									?.get(args[0]) ?? args[0];
						} else if (callbackMethods.has(property) && typeof args[0] === 'function') {
							const callback = args[0];
							args[0] = function (this: unknown, ...callbackArguments: unknown[]) {
								return Reflect.apply(
									callback,
									this,
									callbackArguments.map(guardReturnedValue),
								);
							};
						}
						return guardReturnedValue(Reflect.apply(value, target, args));
					};
				},
			});
			guardedTypes.set(sharedType, guarded);
			guardedProxies.add(guarded);
			return guarded;
		};
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
				return guardMutations(document.getArray<T>(cellId));
			},
			getMap<T = unknown>(cellId: string): Y.Map<T> {
				assertType(cellId, 'map');
				return guardMutations(document.getMap<T>(cellId));
			},
			getText(cellId: string): Y.Text {
				assertType(cellId, 'text');
				return guardMutations(document.getText(cellId));
			},
			getXmlFragment(cellId: string): Y.XmlFragment {
				assertType(cellId, 'xml-fragment');
				return guardMutations(document.getXmlFragment(cellId));
			},
		};
		open = { document, handle, lifecycle, pendingWrites, rowId, tableId };
		documents.set(key, open);
		document.on('update', (bytes: Uint8Array, origin: unknown) => {
			if (origin === remoteOrigin) {
				return;
			}
			if (terminalError) {
				reportError(terminalError);
				return;
			}
			if (!lifecycle.active || table.mode === 'read-only') {
				reportError(
					new Error(
						`Ignored a local update for ${tableId}.${rowId} because the CRDT row is ${lifecycle.active ? 'read-only' : 'closed'}`,
					),
				);
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
			lifecycle.active = false;
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
		if (terminalError) {
			return Promise.reject(terminalError);
		}
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

	const startSyncing = async (): Promise<void> => {
		assertUsable();
		if (started) {
			return;
		}
		started = true;
		syncGeneration += 1;
		for (const open of [...documents.values()]) {
			try {
				await subscribe(open);
			} catch (error) {
				if (terminalError) {
					throw terminalError;
				}
				reportError(error);
			}
		}
		await scheduleBufferedUpdates();
		assertUsable();
	};

	const stopSyncing = async (): Promise<void> => {
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
	};

	const terminate = (error: IndexedDbConnectionClosedForUpgradeError): void => {
		if (terminalError) {
			return;
		}
		terminalError = error;
		void stopSyncing().catch(reportError);
		onTerminal(error);
	};

	handleConnectionClosedForUpgrade = terminate;
	if (pendingTerminalError) {
		terminate(pendingTerminalError);
	}

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
			assertUsable();
			const quarantined = await state.getQuarantined();
			for (const { documentKey } of quarantined) {
				const open = documents.get(documentKey);
				if (open) {
					open.lifecycle.active = false;
				}
			}
			for (const { rowId, tableId } of quarantined) {
				await closeRow(tableId, rowId);
			}
			const discarded = await state.discardDocuments(
				quarantined.map(({ documentKey }) => documentKey),
			);
			pendingCount -= discarded.pendingCount;
			rejectedCount -= discarded.rejectedCount;
			onStatusChange();
		},
		async flushOutbox(isRowBlocked) {
			assertUsable();
			await compactBufferedUpdates(false);
			const quarantined = new Map(
				(await state.getQuarantined()).map((document) => [document.documentKey, document]),
			);
			for (const update of await coalesceOutboxUpdates()) {
				const table = tables[update.tableId];
				if (!table || table.mode === 'read-only') {
					const discarded = await state.discardDocuments([update.documentKey]);
					pendingCount -= discarded.pendingCount;
					rejectedCount -= discarded.rejectedCount;
					onStatusChange();
					continue;
				}
				if (quarantined.get(update.documentKey)?.state === 'rejected') {
					continue;
				}
				if (await isRowBlocked(update.tableId, update.rowId)) {
					continue;
				}
				try {
					await transport.insertUpdate(update, table);
					await state.completeOutbox(update.id, update.documentKey);
					pendingCount -= 1;
					onStatusChange();
				} catch (error) {
					if (isPermanentError(error as SupabaseError)) {
						await state.reject(update, (error as SupabaseError).message);
						quarantined.set(update.documentKey, {
							documentKey: update.documentKey,
							error: (error as SupabaseError).message,
							failedUpdateId: update.id,
							rowId: update.rowId,
							state: 'rejected',
							tableId: update.tableId,
						});
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
			assertUsable();
			await waitForPendingWrites();
			await compactBufferedUpdates(true);
			assertUsable();
		},
		getPendingCount: () => pendingCount,
		getProjection(tableId, rowId) {
			const open = documents.get(getDocumentKey(tableId, rowId));
			return open ? getOpenProjection(open) : undefined;
		},
		async getRejected() {
			assertUsable();
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
			assertUsable();
			for (const open of [...documents.values()]) {
				await pull(await subscribe(open));
			}
		},
		async retryRejected() {
			assertUsable();
			const retriedCount = await state.retryRejected();
			pendingCount += retriedCount;
			rejectedCount -= retriedCount;
			onStatusChange();
		},
		startSyncing,
		stopSyncing,
		terminate,
	};
};
