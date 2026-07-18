import { createStore, type Store } from 'tinybase';
import type { SupabasePersister } from '../../src/index.js';
import {
	createSupabasePersister,
	IndexedDbConnectionClosedForUpgradeError,
	IndexedDbUpgradeBlockedError,
} from '../../src/index.js';

interface ReportedError {
	readonly code?: string;
	readonly currentVersion?: number;
	readonly isConnectionClosedForUpgrade: boolean;
	readonly isUpgradeBlocked: boolean;
	readonly name: string;
	readonly requestedVersion?: number | null;
}

interface IndexedDbUpgradeHarness {
	awaitPersister(): Promise<{
		readonly networkRequestCount: number;
		readonly outbox: readonly Record<string, unknown>[];
		readonly pendingCount: number;
		readonly rejected: readonly Record<string, unknown>[];
		readonly rejectedCount: number;
		readonly row: Record<string, unknown>;
	}>;
	awaitCrdtPersister(): Promise<
		Readonly<Record<'buffered' | 'outbox' | 'quarantined' | 'rejected' | 'updates', unknown[]>>
	>;
	closeHeldConnection(): void;
	destroy(): Promise<void>;
	getErrors(): readonly ReportedError[];
	getTerminalState(): Promise<{
		readonly isAutoSaving: boolean;
		readonly listenerCode?: string;
		readonly phase: string;
		readonly saveCode?: string;
		readonly saveRejectedWithStatusError: boolean;
		readonly statusCode?: string;
		readonly syncCode?: string;
		readonly syncRejectedWithStatusError: boolean;
	}>;
	hasPersisterResolved(): boolean;
	holdVersionOne(databaseName: string, scopeKey: string): Promise<void>;
	holdVersionTwoCrdt(databaseName: string, scopeKey: string): Promise<void>;
	requestFutureVersion(databaseName: string, scopeKey: string): Promise<number>;
	requestFutureCrdtVersion(databaseName: string, scopeKey: string): Promise<number>;
	startCrdtPersister(databaseName: string, scopeKey: string): void;
	startPersister(databaseName: string, scopeKey: string): void;
}

declare global {
	interface Window {
		indexedDbUpgradeTest: IndexedDbUpgradeHarness;
	}
}

let creation: Promise<void> | undefined;
let currentDatabaseName: string | undefined;
let currentScopeKey: string | undefined;
let errors: ReportedError[] = [];
let heldConnection: IDBDatabase | undefined;
let networkRequestCount = 0;
let persister: SupabasePersister | undefined;
let persisterResolved = false;
let statusListenerCode: string | undefined;
let store: Store | undefined;

const waitForTransaction = (transaction: IDBTransaction): Promise<void> =>
	new Promise((resolve, reject) => {
		transaction.oncomplete = () => resolve();
		transaction.onerror = () => reject(transaction.error);
		transaction.onabort = () => reject(transaction.error);
	});

const getRequestResult = <Value>(request: IDBRequest<Value>): Promise<Value> =>
	new Promise((resolve, reject) => {
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
	});

const readStores = async <StoreName extends string>(
	databaseName: string,
	storeNames: readonly StoreName[],
): Promise<Readonly<Record<StoreName, unknown[]>>> => {
	const request = indexedDB.open(databaseName);
	const database = await getRequestResult(request);
	const transaction = database.transaction(storeNames, 'readonly');
	const entries = await Promise.all(
		storeNames.map(async (storeName) => [
			storeName,
			await getRequestResult(transaction.objectStore(storeName).getAll()),
		]),
	);
	await waitForTransaction(transaction);
	database.close();
	return Object.fromEntries(entries) as Record<StoreName, unknown[]>;
};

const reportError = (error: Error): void => {
	const versioned = error as Error & {
		readonly code?: string;
		readonly currentVersion?: number;
		readonly requestedVersion?: number | null;
	};
	errors.push({
		code: versioned.code,
		currentVersion: versioned.currentVersion,
		isConnectionClosedForUpgrade: error instanceof IndexedDbConnectionClosedForUpgradeError,
		isUpgradeBlocked: error instanceof IndexedDbUpgradeBlockedError,
		name: error.name,
		requestedVersion: versioned.requestedVersion,
	});
};

const start = (
	databaseName: string,
	scopeKey: string,
	tables: Parameters<typeof createSupabasePersister>[1]['tables'],
): void => {
	errors = [];
	networkRequestCount = 0;
	persisterResolved = false;
	statusListenerCode = undefined;
	currentDatabaseName = databaseName;
	currentScopeKey = scopeKey;
	store = createStore();
	creation = createSupabasePersister(store, {
		databaseName,
		onError: reportError,
		pollIntervalMs: 0,
		scopeKey,
		supabase: {
			from() {
				networkRequestCount += 1;
				throw new Error('Unexpected network request');
			},
		},
		tables,
	}).then(async (createdPersister) => {
		persister = createdPersister;
		createdPersister.addSyncStatusListener((status) => {
			statusListenerCode = (status.lastError as (Error & { code?: string }) | undefined)
				?.code;
		});
		await createdPersister.load();
		persisterResolved = true;
	});
};

window.indexedDbUpgradeTest = {
	async awaitPersister() {
		await creation;
		if (!persister || !store) {
			throw new Error('Persister did not start');
		}
		if (!currentDatabaseName || !currentScopeKey) {
			throw new Error('Database scope is not configured');
		}
		const status = persister.getSyncStatus();
		const records = await readStores(`${currentDatabaseName}:${currentScopeKey}`, [
			'outbox',
			'rejected',
		]);
		return {
			networkRequestCount,
			outbox: records.outbox as Record<string, unknown>[],
			pendingCount: status.pendingCount,
			rejected: records.rejected as Record<string, unknown>[],
			rejectedCount: status.rejectedCount,
			row: store.getRow('todos', 'cached'),
		};
	},
	async awaitCrdtPersister() {
		await creation;
		if (!currentDatabaseName || !currentScopeKey) {
			throw new Error('Database scope is not configured');
		}
		const records = await readStores(`${currentDatabaseName}:${currentScopeKey}:yjs`, [
			'buffered',
			'outbox',
			'quarantined',
			'rejected',
			'updates',
		]);
		return Object.fromEntries(
			Object.entries(records).map(([storeName, values]) => [
				storeName,
				values.map((value) => {
					const record = value as Record<string, unknown>;
					return record.update instanceof Uint8Array
						? { ...record, update: [...record.update] }
						: record;
				}),
			]),
		) as Record<'buffered' | 'outbox' | 'quarantined' | 'rejected' | 'updates', unknown[]>;
	},
	closeHeldConnection() {
		heldConnection?.close();
		heldConnection = undefined;
	},
	async destroy() {
		await persister?.destroy();
		persister = undefined;
	},
	getErrors() {
		return errors;
	},
	async getTerminalState() {
		if (!persister) {
			throw new Error('Persister did not start');
		}
		const status = persister.getSyncStatus();
		const statusError = status.lastError as (Error & { code?: string }) | undefined;
		let saveError: (Error & { code?: string }) | undefined;
		let syncError: (Error & { code?: string }) | undefined;
		try {
			await persister.save();
		} catch (error) {
			saveError = error as Error & { code?: string };
		}
		try {
			await persister.syncNow();
		} catch (error) {
			syncError = error as Error & { code?: string };
		}
		return {
			isAutoSaving: persister.isAutoSaving(),
			listenerCode: statusListenerCode,
			phase: status.phase,
			saveCode: saveError?.code,
			saveRejectedWithStatusError: saveError === statusError,
			statusCode: statusError?.code,
			syncCode: syncError?.code,
			syncRejectedWithStatusError: syncError === statusError,
		};
	},
	hasPersisterResolved() {
		return persisterResolved;
	},
	async holdVersionOne(databaseName, scopeKey) {
		const request = indexedDB.open(`${databaseName}:${scopeKey}`, 1);
		request.onupgradeneeded = () => {
			request.result.createObjectStore('content');
			request.result.createObjectStore('outbox');
			request.result.createObjectStore('rejected');
		};
		heldConnection = await new Promise<IDBDatabase>((resolve, reject) => {
			request.onsuccess = () => resolve(request.result);
			request.onerror = () => reject(request.error);
		});

		const transaction = heldConnection.transaction(
			['content', 'outbox', 'rejected'],
			'readwrite',
		);
		transaction
			.objectStore('content')
			.put([{ todos: { cached: { title: 'Cached before upgrade' } } }, {}], 'store');
		transaction.objectStore('outbox').put(
			{
				id: 'todos:pending',
				kind: 'upsert',
				payload: { id: 'pending', title: 'Pending before upgrade' },
				rowId: 'pending',
				tableId: 'todos',
			},
			'todos:pending',
		);
		transaction.objectStore('rejected').put(
			{
				error: 'Rejected before upgrade',
				id: 'todos:rejected',
				kind: 'upsert',
				payload: { id: 'rejected', title: 'Rejected before upgrade' },
				rowId: 'rejected',
				tableId: 'todos',
			},
			'todos:rejected',
		);
		await waitForTransaction(transaction);
	},
	async holdVersionTwoCrdt(databaseName, scopeKey) {
		const request = indexedDB.open(`${databaseName}:${scopeKey}:yjs`, 2);
		request.onupgradeneeded = () => {
			const updates = request.result.createObjectStore('updates');
			updates.createIndex('documentKey', 'documentKey');
			request.result.createObjectStore('outbox');
			request.result.createObjectStore('rejected');
			const buffered = request.result.createObjectStore('buffered');
			buffered.createIndex('documentKey', 'documentKey');
		};
		heldConnection = await getRequestResult(request);
		const transaction = heldConnection.transaction(
			['buffered', 'outbox', 'rejected', 'updates'],
			'readwrite',
		);
		const storedUpdate = {
			documentKey: 'documents\0doc-1',
			rowId: 'doc-1',
			tableId: 'documents',
		};
		transaction
			.objectStore('updates')
			.put({ ...storedUpdate, id: 'update-1', update: Uint8Array.from([1]) }, 'update-1');
		transaction.objectStore('buffered').put(
			{
				...storedUpdate,
				bufferedAt: 123,
				id: 'buffered-1',
				update: Uint8Array.from([2]),
			},
			'buffered-1',
		);
		transaction
			.objectStore('outbox')
			.put({ ...storedUpdate, id: 'outbox-1', update: Uint8Array.from([3]) }, 'outbox-1');
		transaction.objectStore('rejected').put(
			{
				...storedUpdate,
				error: 'Rejected before CRDT upgrade',
				id: 'rejected-1',
				update: Uint8Array.from([4]),
			},
			'rejected-1',
		);
		await waitForTransaction(transaction);
	},
	async requestFutureCrdtVersion(databaseName, scopeKey) {
		const request = indexedDB.open(`${databaseName}:${scopeKey}:yjs`, 4);
		const database = await getRequestResult(request);
		const version = database.version;
		database.close();
		return version;
	},
	async requestFutureVersion(databaseName, scopeKey) {
		const request = indexedDB.open(`${databaseName}:${scopeKey}`, 3);
		const database = await new Promise<IDBDatabase>((resolve, reject) => {
			request.onsuccess = () => resolve(request.result);
			request.onerror = () => reject(request.error);
		});
		const version = database.version;
		database.close();
		return version;
	},
	startPersister(databaseName, scopeKey) {
		start(databaseName, scopeKey, { todos: { table: 'todos' } });
	},
	startCrdtPersister(databaseName, scopeKey) {
		start(databaseName, scopeKey, {
			documents: {
				crdtCells: { body: { type: 'text' } },
				crdtUpdatesTable: 'document_updates',
				table: 'documents',
			},
		});
	},
};
