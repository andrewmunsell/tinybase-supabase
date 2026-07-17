import 'fake-indexeddb/auto';
import { jest } from '@jest/globals';
import { openDB } from 'idb';
import { createStore } from 'tinybase';
import * as Y from 'yjs';
import { CrdtLocalState } from '../../src/crdt-storage.js';
import {
	createSupabasePersister,
	IndexedDbConnectionClosedForUpgradeError,
} from '../../src/index.js';

type RemoteRow = Record<string, unknown>;
type RemoteError = { message: string; status: number };
type ChannelRecord = {
	active: boolean;
	callback: () => void;
	filter: { table: string };
	name: string;
};

class Query implements PromiseLike<{ data: RemoteRow[]; error: RemoteError | null }> {
	readonly #error: RemoteError | undefined;
	readonly #filters: Array<(row: RemoteRow) => boolean> = [];
	readonly #orders: string[] = [];
	readonly #rows: RemoteRow[];
	#from = 0;
	#to = Number.POSITIVE_INFINITY;

	constructor(rows: RemoteRow[], error?: RemoteError) {
		this.#rows = rows;
		this.#error = error;
	}

	eq(column: string, value: string): Query {
		this.#filters.push((row) => String(row[column]) === value);
		return this;
	}

	gte(column: string, value: string): Query {
		this.#filters.push((row) => String(row[column]) >= value);
		return this;
	}

	gt(column: string, value: string): Query {
		this.#filters.push((row) => String(row[column]) > value);
		return this;
	}

	limit(count: number): Query {
		this.#to = this.#from + count - 1;
		return this;
	}

	order(column: string): Query {
		this.#orders.push(column);
		return this;
	}

	range(from: number, to: number): Query {
		this.#from = from;
		this.#to = to;
		return this;
	}

	// biome-ignore lint/suspicious/noThenProperty: Supabase query builders are intentionally thenable.
	then<TResult1 = { data: RemoteRow[]; error: RemoteError | null }, TResult2 = never>(
		onfulfilled?:
			| ((value: {
					data: RemoteRow[];
					error: RemoteError | null;
			  }) => TResult1 | PromiseLike<TResult1>)
			| null,
		onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
	): PromiseLike<TResult1 | TResult2> {
		const filtered = this.#rows.filter((row) => this.#filters.every((filter) => filter(row)));
		const ordered = [...filtered].sort((left, right) => {
			for (const column of this.#orders) {
				const comparison = String(left[column]).localeCompare(String(right[column]));
				if (comparison !== 0) {
					return comparison;
				}
			}
			return 0;
		});
		return Promise.resolve({
			data: ordered.slice(this.#from, this.#to + 1),
			error: this.#error ?? null,
		}).then(onfulfilled, onrejected);
	}
}

class MemorySupabase {
	readonly channels: ChannelRecord[] = [];
	readonly delayedSubscriptionTables = new Set<string>();
	readonly rows = new Map<string, RemoteRow[]>();
	readonly selectCounts = new Map<string, number>();
	readonly errors = new Map<string, RemoteError>();
	readonly selectErrors = new Map<string, RemoteError>();
	readonly subscriptionCallbacks = new Map<string, Array<(status: string) => void>>();
	readonly subscriptionStatuses = new Map<string, string>();
	#timestamp = 0;
	onSubscribe?: (record: (typeof this.channels)[number]) => void;

	nextUpdatedAt(): string {
		this.#timestamp += 1;
		return `2026-07-14T00:00:00.${String(this.#timestamp).padStart(6, '0')}+00:00`;
	}

	channel(name: string) {
		let record: ChannelRecord | undefined;
		const channel = {
			on: (_type: string, filter: { table: string }, callback: () => void) => {
				record = { active: true, callback, filter, name };
				this.channels.push(record);
				return channel;
			},
			get record() {
				return record;
			},
			subscribe: (callback?: (status: string) => void) => {
				if (record) {
					this.onSubscribe?.(record);
					if (callback && this.delayedSubscriptionTables.has(record.filter.table)) {
						const callbacks = this.subscriptionCallbacks.get(record.filter.table) ?? [];
						callbacks.push(callback);
						this.subscriptionCallbacks.set(record.filter.table, callbacks);
						return;
					}
				}
				callback?.(
					record
						? (this.subscriptionStatuses.get(record.filter.table) ?? 'SUBSCRIBED')
						: 'SUBSCRIBED',
				);
			},
		};
		return channel;
	}

	from(table: string) {
		const rows = this.rows.get(table) ?? [];
		this.rows.set(table, rows);
		return {
			insert: async (value: RemoteRow) => {
				const error = this.errors.get(table);
				if (error) {
					return { data: null, error };
				}
				if (!rows.some((row) => row.id === value.id)) {
					rows.push({ ...value, created_at: new Date().toISOString() });
				}
				return { data: null, error: null };
			},
			select: (columns: string) => {
				this.selectCounts.set(table, (this.selectCounts.get(table) ?? 0) + 1);
				if (columns === '*') {
					for (const row of rows) {
						row.deleted_at ??= null;
						row.updated_at ??= this.nextUpdatedAt();
					}
				}
				return new Query(rows, this.selectErrors.get(table));
			},
			upsert: async (value: RemoteRow, options: { onConflict: string }) => {
				const error = this.errors.get(table);
				if (error) {
					return { data: null, error };
				}
				const index = rows.findIndex(
					(row) => row[options.onConflict] === value[options.onConflict],
				);
				if (index === -1) {
					rows.push({ deleted_at: null, ...value, updated_at: this.nextUpdatedAt() });
				} else {
					rows[index] = {
						...rows[index],
						...value,
						updated_at: this.nextUpdatedAt(),
					};
				}
				return { data: null, error: null };
			},
		};
	}

	removeChannel(channel: { record?: ChannelRecord }): void {
		if (channel.record) {
			channel.record.active = false;
		}
	}

	releaseSubscriptions(table: string): void {
		for (const callback of this.subscriptionCallbacks.get(table) ?? []) {
			callback('SUBSCRIBED');
		}
		this.subscriptionCallbacks.delete(table);
	}
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const waitFor = async (condition: () => boolean, message: string): Promise<void> => {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (condition()) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 2));
	}
	throw new Error(message);
};
const waitForSyncPhase = async (
	persister: { getSyncStatus(): { phase: string } },
	phase: string,
): Promise<void> => {
	for (let attempt = 0; attempt < 50; attempt += 1) {
		if (persister.getSyncStatus().phase === phase) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 2));
	}
	throw new Error(`Timed out waiting for sync phase ${phase}`);
};

const configuration = (client: MemorySupabase, databaseName: string) => ({
	crdtUpdateBufferMs: 0,
	databaseName,
	pollIntervalMs: 0,
	scopeKey: 'user',
	supabase: client,
	tables: {
		documents: {
			crdtCells: {
				body: { type: 'text' as const },
				items: { type: 'array' as const },
				properties: { type: 'map' as const },
			},
			crdtRowIdColumn: 'document_id',
			crdtUpdatesTable: 'document_yjs_updates',
			table: 'documents',
			updatedAtColumn: 'updated_at',
		},
	},
});

describe('createSupabasePersister with CRDT cells', () => {
	it('normalizes a CRDT upgrade that closes IndexedDB during initialization', async () => {
		const client = new MemorySupabase();
		const databaseName = `initialization-crdt-upgrade-${crypto.randomUUID()}`;
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		let releaseRead = (): void => undefined;
		let reportReadStarted = (): void => undefined;
		const readStarted = new Promise<void>((resolve) => {
			reportReadStarted = resolve;
		});
		const readGate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});
		const originalDiscard = CrdtLocalState.prototype.discardTableLocalState;
		const discard = jest
			.spyOn(CrdtLocalState.prototype, 'discardTableLocalState')
			.mockImplementation(async function (
				this: CrdtLocalState,
				tableIds: ReadonlySet<string>,
			) {
				reportReadStarted();
				await readGate;
				return originalDiscard.call(this, tableIds);
			});
		let futureDatabase: Awaited<ReturnType<typeof openDB>> | undefined;
		try {
			const creation = createSupabasePersister(store, configuration(client, databaseName));
			await readStarted;
			futureDatabase = await openDB(`${databaseName}:user:yjs`, 4);
			releaseRead();
			const persister = await creation;
			const terminalStatus = persister.getSyncStatus();
			const terminalError = terminalStatus.lastError;

			expect(terminalStatus.phase).toBe('error');
			expect(terminalError).toMatchObject({
				code: 'indexeddb-connection-closed-for-upgrade',
				currentVersion: 3,
				requestedVersion: 4,
			});
			await expect(persister.openRow('documents', 'doc-1')).rejects.toBe(terminalError);
			await persister.destroy();
		} finally {
			releaseRead();
			futureDatabase?.close();
			discard.mockRestore();
		}
	});

	it('preserves an unrelated CRDT initialization error that races with an upgrade', async () => {
		const client = new MemorySupabase();
		const databaseName = `initialization-crdt-error-${crypto.randomUUID()}`;
		const sentinel = new Error('Unrelated CRDT initialization failure');
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		let releaseRead = (): void => undefined;
		let reportReadStarted = (): void => undefined;
		const readStarted = new Promise<void>((resolve) => {
			reportReadStarted = resolve;
		});
		const readGate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});
		const discard = jest
			.spyOn(CrdtLocalState.prototype, 'discardTableLocalState')
			.mockImplementation(async () => {
				reportReadStarted();
				await readGate;
				throw sentinel;
			});
		let futureDatabase: Awaited<ReturnType<typeof openDB>> | undefined;
		try {
			const creation = createSupabasePersister(store, configuration(client, databaseName));
			await readStarted;
			futureDatabase = await openDB(`${databaseName}:user:yjs`, 4);
			releaseRead();
			await expect(creation).rejects.toBe(sentinel);
		} finally {
			releaseRead();
			futureDatabase?.close();
			discard.mockRestore();
		}
	});

	it('finishes hybrid termination when a status listener throws', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const databaseName = `terminal-crdt-upgrade-${crypto.randomUUID()}`;
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, databaseName),
			tables: {
				documents: {
					...configuration(client, databaseName).tables.documents,
					crdtCells: {
						...configuration(client, databaseName).tables.documents.crdtCells,
						structuredBody: { type: 'xml-fragment' },
					},
					realtime: true,
				},
			},
		});
		await persister.startAutoPersisting();
		const row = await persister.openRow('documents', 'doc-1');
		const nestedMap = new Y.Map<string>();
		row.getMap<unknown>('properties').set('child', nestedMap);
		const guardedNestedMap = row.getMap<unknown>('properties').get('child') as Y.Map<string>;
		const retainedDocument = nestedMap.doc;
		const documentText = row.getText('body').doc?.getText('body');
		const guardedDocument = documentText?.doc;
		const retainedXmlText = new Y.XmlText();
		retainedXmlText.insert(0, 'Before termination');
		const retainedXmlElement = new Y.XmlElement('paragraph');
		retainedXmlElement.insert(0, [retainedXmlText]);
		row.getXmlFragment('structuredBody').insert(0, [retainedXmlElement]);
		const remoteDocument = new Y.Doc();
		remoteDocument.getText('body').insert(0, 'Remote update');
		const remoteUpdate = Y.encodeStateAsUpdate(remoteDocument);
		remoteDocument.destroy();
		expect(client.channels.length).toBeGreaterThan(0);
		persister.addSyncStatusListener((status) => {
			if (status.phase === 'error') {
				throw new Error('Status listener failed');
			}
		});
		const statuses: Array<ReturnType<typeof persister.getSyncStatus>> = [];
		persister.addSyncStatusListener((status) => statuses.push(status));

		const futureDatabase = await openDB(`${databaseName}:user`, 3);
		const terminalStatus = persister.getSyncStatus();
		const terminalError = terminalStatus.lastError;

		expect(terminalStatus.phase).toBe('error');
		expect(terminalError).toBeInstanceOf(IndexedDbConnectionClosedForUpgradeError);
		expect(terminalError).toMatchObject({
			code: 'indexeddb-connection-closed-for-upgrade',
			currentVersion: 2,
			requestedVersion: 3,
		});
		expect(statuses.at(-1)?.lastError).toBe(terminalError);
		expect(statuses.at(-1)?.phase).toBe('error');
		expect(persister.isAutoSaving()).toBe(false);
		await waitFor(
			() => client.channels.every(({ active }) => !active),
			'Realtime channels remained active after IndexedDB closed',
		);

		let mutationError: unknown;
		try {
			row.getText('body').insert(0, 'Must not persist');
		} catch (error) {
			mutationError = error;
		}
		expect(mutationError).toBe(terminalError);
		let nestedMutationError: unknown;
		try {
			guardedNestedMap.set('after', 'termination');
		} catch (error) {
			nestedMutationError = error;
		}
		expect(nestedMutationError).toBe(terminalError);
		let retainedMutationError: unknown;
		try {
			nestedMap.set('retained', 'after termination');
		} catch (error) {
			retainedMutationError = error;
		}
		expect(retainedMutationError).toBe(terminalError);
		let documentMutationError: unknown;
		try {
			documentText?.insert(0, 'Must not persist');
		} catch (error) {
			documentMutationError = error;
		}
		expect(documentMutationError).toBe(terminalError);
		let applyUpdateError: unknown;
		try {
			if (guardedDocument) {
				Y.applyUpdate(guardedDocument, remoteUpdate);
			}
		} catch (error) {
			applyUpdateError = error;
		}
		expect(applyUpdateError).toBe(terminalError);
		let retainedDocumentError: unknown;
		try {
			if (retainedDocument) {
				Y.applyUpdate(retainedDocument, remoteUpdate);
			}
		} catch (error) {
			retainedDocumentError = error;
		}
		expect(retainedDocumentError).toBe(terminalError);
		let retainedDescendantError: unknown;
		try {
			retainedXmlText.insert(0, 'Must not persist');
		} catch (error) {
			retainedDescendantError = error;
		}
		expect(retainedDescendantError).toBe(terminalError);
		for (const operation of [
			persister.load(),
			persister.save(),
			persister.syncNow(),
			persister.startSyncing(),
			persister.startAutoPersisting(),
			persister.openRow('documents', 'doc-1'),
			persister.retryRejected(),
			persister.discardRejected(),
			persister.getRejectedOperations(),
		]) {
			await expect(operation).rejects.toBe(terminalError);
		}

		futureDatabase.close();
		await persister.destroy();
	});

	it('uses the ordinary implementation when crdtCells is empty and needs no updates table', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			databaseName: `ordinary-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: 'user',
			supabase: client,
			tables: { todos: { crdtCells: {}, table: 'todos' } },
		});
		await persister.startAutoPersisting();
		store.setRow('todos', 'todo-1', { title: 'Ordinary' });
		await persister.save();
		await persister.syncNow();

		expect(client.rows.get('todos')).toContainEqual(
			expect.objectContaining({ id: 'todo-1', title: 'Ordinary' }),
		);
		expect(client.rows.has('document_yjs_updates')).toBe(false);
		await expect(persister.openRow('todos', 'todo-1')).rejects.toThrow(
			'no configured CRDT cells',
		);
		await persister.destroy();
	});

	it('supports ordinary and CRDT-enabled tables in one persister', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			...configuration(client, `mixed-${crypto.randomUUID()}`),
			tables: {
				audit_logs: { table: 'audit_logs' },
				...configuration(client, 'unused').tables,
			},
		});
		await persister.startAutoPersisting();
		store.setRow('documents', 'doc-1', { owner_id: 'user', status: 'draft' });
		store.setRow('audit_logs', 'log-1', { action: 'created' });
		await persister.save();
		await persister.syncNow();
		const row = await persister.openRow('documents', 'doc-1');
		row.getText('body').insert(0, 'Hybrid');
		await tick();
		await persister.syncNow();

		expect(client.rows.get('audit_logs')).toContainEqual(
			expect.objectContaining({
				action: 'created',
				id: 'log-1',
			}),
		);
		expect(store.getCell('documents', 'doc-1', 'body')).toBe('Hybrid');
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		await persister.destroy();
	});

	it('hydrates and follows CRDT updates without retaining local updates in read-only mode', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'owner' }]);
		const writerStore = createStore().setRow('documents', 'doc-1', { owner_id: 'owner' });
		const writer = await createSupabasePersister(
			writerStore,
			configuration(client, `read-only-writer-${crypto.randomUUID()}`),
		);
		const writerRow = await writer.openRow('documents', 'doc-1');
		writerRow.getText('body').insert(0, 'Remote');
		await tick();
		await writer.syncNow();

		const databaseName = `read-only-reader-${crypto.randomUUID()}`;
		const readerStore = createStore().setRow('documents', 'doc-1', { owner_id: 'owner' });
		const reader = await createSupabasePersister(readerStore, {
			...configuration(client, databaseName),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					mode: 'read-only',
					realtime: true,
				},
			},
		});
		await reader.startSyncing();
		const readerRow = await reader.openRow('documents', 'doc-1');
		expect(readerStore.getCell('documents', 'doc-1', 'body')).toBe('Remote');
		expect(readerRow.getText('body').doc).toBeDefined();

		writerRow.getText('body').insert(6, ' update');
		await tick();
		await writer.syncNow();
		client.channels
			.find(({ active, filter }) => active && filter.table === 'document_yjs_updates')
			?.callback();
		await waitFor(
			() => readerStore.getCell('documents', 'doc-1', 'body') === 'Remote update',
			'Read-only CRDT update was not reconciled',
		);
		expect(readerStore.getCell('documents', 'doc-1', 'body')).toBe('Remote update');

		expect(() => readerRow.getText('body').insert(0, 'Local ')).toThrow('is read-only');
		readerStore.setCell('documents', 'doc-1', 'owner_id', 'local-owner');
		await reader.save();
		await reader.syncNow();
		expect(reader.getSyncStatus().pendingCount).toBe(0);
		expect(client.rows.get('document_yjs_updates')).toHaveLength(2);
		expect(client.rows.get('documents')).toEqual([
			expect.objectContaining({ id: 'doc-1', owner_id: 'owner' }),
		]);

		const localState = await CrdtLocalState.open(databaseName, 'user');
		await expect(localState.getBuffered()).resolves.toHaveLength(0);
		await expect(localState.getOutbox()).resolves.toHaveLength(0);
		await expect(localState.getDocumentUpdates('documents\0doc-1')).resolves.toHaveLength(2);
		localState.close();
		await reader.closeRow('documents', 'doc-1');
		await reader.openRow('documents', 'doc-1');
		expect(readerStore.getCell('documents', 'doc-1', 'body')).toBe('Remote update');
		await reader.destroy();
		await writer.destroy();
	});

	it('preserves Yjs observer identity across targets and observer modes', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'owner' }]);
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'owner' });
		const persister = await createSupabasePersister(
			store,
			configuration(client, `observer-identity-${crypto.randomUUID()}`),
		);
		const row = await persister.openRow('documents', 'doc-1');
		const body = row.getText('body');
		const properties = row.getMap('properties');
		const observer = jest.fn();

		body.observe(observer);
		body.observe(observer);
		body.observeDeep(observer);
		properties.observe(observer);
		body.insert(0, 'first');
		expect(observer).toHaveBeenCalledTimes(3);

		body.unobserve(observer);
		body.insert(5, ' second');
		expect(observer).toHaveBeenCalledTimes(4);
		body.unobserveDeep(observer);
		body.insert(12, ' third');
		expect(observer).toHaveBeenCalledTimes(4);

		properties.set('observed', true);
		expect(observer).toHaveBeenCalledTimes(5);
		properties.unobserve(observer);
		properties.set('unobserved', true);
		expect(observer).toHaveBeenCalledTimes(5);

		await persister.destroy();
	});

	it('discards buffered CRDT writes when a table is reopened read-only', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'owner' }]);
		const databaseName = `read-only-reopen-${crypto.randomUUID()}`;
		const writerStore = createStore().setRow('documents', 'doc-1', { owner_id: 'owner' });
		const writer = await createSupabasePersister(writerStore, {
			...configuration(client, databaseName),
			crdtUpdateBufferMs: 10_000,
		});
		const writerRow = await writer.openRow('documents', 'doc-1');
		writerRow.getText('body').insert(0, 'Unsent');
		await tick();
		expect(writer.getSyncStatus().pendingCount).toBe(1);
		await writer.destroy();

		const readerStore = createStore().setRow('documents', 'doc-1', { owner_id: 'owner' });
		const reader = await createSupabasePersister(readerStore, {
			...configuration(client, databaseName),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					mode: 'read-only',
				},
			},
		});
		await reader.openRow('documents', 'doc-1');
		expect(readerStore.getCell('documents', 'doc-1', 'body')).toBe('');
		expect(reader.getSyncStatus().pendingCount).toBe(0);
		await reader.syncNow();
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);
		await reader.destroy();
	});

	it('removes rejected CRDT content before reopening the table read-only', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'owner' }]);
		const databaseName = `read-only-rejected-${crypto.randomUUID()}`;
		const writerStore = createStore().setRow('documents', 'doc-1', { owner_id: 'owner' });
		const writer = await createSupabasePersister(
			writerStore,
			configuration(client, databaseName),
		);
		const writerRow = await writer.openRow('documents', 'doc-1');
		client.errors.set('document_yjs_updates', { message: 'forbidden', status: 403 });
		writerRow.getText('body').insert(0, 'Rejected');
		await tick();
		await writer.syncNow();
		await expect(writer.getRejectedOperations()).resolves.toHaveLength(1);
		await writer.destroy();

		client.errors.delete('document_yjs_updates');
		const readerStore = createStore().setRow('documents', 'doc-1', { owner_id: 'owner' });
		const reader = await createSupabasePersister(readerStore, {
			...configuration(client, databaseName),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					mode: 'read-only',
				},
			},
		});
		await reader.openRow('documents', 'doc-1');

		expect(readerStore.getCell('documents', 'doc-1', 'body')).toBe('');
		expect(reader.getSyncStatus()).toMatchObject({ pendingCount: 0, rejectedCount: 0 });
		await expect(reader.getRejectedOperations()).resolves.toEqual([]);
		await reader.retryRejected();
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);
		const localState = await CrdtLocalState.open(databaseName, 'user');
		await expect(localState.getDocumentUpdates('documents\0doc-1')).resolves.toHaveLength(0);
		localState.close();
		await reader.destroy();
	});

	it('rejects local mutations for every CRDT cell type in read-only mode', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'owner' }]);
		const databaseName = `read-only-types-${crypto.randomUUID()}`;
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'owner' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, databaseName),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					crdtCells: {
						body: { type: 'text' },
						items: { type: 'array' },
						properties: { type: 'map' },
						structuredBody: { type: 'xml-fragment' },
					},
					mode: 'read-only',
				},
			},
		});
		const row = await persister.openRow('documents', 'doc-1');

		expect(() => row.getText('body').insert(0, 'Blocked')).toThrow('is read-only');
		expect(() => row.getArray('items').push(['blocked'])).toThrow('is read-only');
		expect(() => row.getMap('properties').set('blocked', true)).toThrow('is read-only');
		expect(() =>
			row.getXmlFragment('structuredBody').insert(0, [new Y.XmlElement('blocked')]),
		).toThrow('is read-only');
		expect(store.getRow('documents', 'doc-1')).toEqual({
			body: '',
			items: [],
			owner_id: 'owner',
			properties: {},
			structuredBody: '',
		});
		expect(persister.getSyncStatus()).toMatchObject({ pendingCount: 0, rejectedCount: 0 });
		const localState = await CrdtLocalState.open(databaseName, 'user');
		await expect(localState.getDocumentUpdates('documents\0doc-1')).resolves.toHaveLength(0);
		localState.close();

		await persister.destroy();
	});

	it('projects multiple differently typed Yjs cells beside ordinary metadata', async () => {
		const client = new MemorySupabase();
		const store = createStore().setRow('documents', 'doc-1', {
			owner_id: 'user',
			status: 'draft',
		});
		const persister = await createSupabasePersister(
			store,
			configuration(client, `crdt-types-${crypto.randomUUID()}`),
		);
		const row = await persister.openRow('documents', 'doc-1');

		row.getText('body').insert(0, 'Hello');
		row.getArray<string>('items').push(['first']);
		row.getMap<string>('properties').set('color', 'blue');
		await tick();

		expect(store.getRow('documents', 'doc-1')).toEqual({
			body: 'Hello',
			items: ['first'],
			owner_id: 'user',
			properties: { color: 'blue' },
			status: 'draft',
		});
		expect(() => row.getMap('body')).toThrow('is text, not map');
		await row.destroy();
		expect(store.hasCell('documents', 'doc-1', 'body')).toBe(false);
		await persister.destroy();
	});

	it('synchronizes Yjs XML fragment documents and projects their serialized XML', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const xmlConfiguration = (databaseName: string) => ({
			...configuration(client, databaseName),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					crdtCells: { content: { type: 'xml-fragment' as const } },
				},
			},
		});
		const senderStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const sender = await createSupabasePersister(
			senderStore,
			xmlConfiguration(`xml-sender-${crypto.randomUUID()}`),
		);
		const senderRow = await sender.openRow('documents', 'doc-1');
		const paragraph = new Y.XmlElement('p');
		paragraph.setAttribute('class', 'lead');
		paragraph.insert(0, [new Y.XmlText('Collaborative XML')]);
		senderRow.getXmlFragment('content').insert(0, [paragraph]);
		await tick();
		await sender.syncNow();

		expect(senderStore.getCell('documents', 'doc-1', 'content')).toBe(
			'<p class="lead">Collaborative XML</p>',
		);
		expect(() => senderRow.getText('content')).toThrow('is xml-fragment, not text');

		const receiverStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const receiver = await createSupabasePersister(
			receiverStore,
			xmlConfiguration(`xml-receiver-${crypto.randomUUID()}`),
		);
		const receiverRow = await receiver.openRow('documents', 'doc-1');
		expect(receiverRow.getXmlFragment('content').toString()).toBe(
			'<p class="lead">Collaborative XML</p>',
		);
		expect(receiverStore.getCell('documents', 'doc-1', 'content')).toBe(
			'<p class="lead">Collaborative XML</p>',
		);

		await receiver.destroy();
		await sender.destroy();
	});

	it('buffers and merges local updates into one automatic upload per document', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, `buffered-${crypto.randomUUID()}`),
			crdtUpdateBufferMs: 20,
		});
		await persister.startSyncing();
		const row = await persister.openRow('documents', 'doc-1');

		row.getText('body').insert(0, 'a');
		row.getText('body').insert(1, 'b');
		row.getText('body').insert(2, 'c');
		await tick();
		expect(persister.getSyncStatus().pendingCount).toBe(3);
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);
		await waitFor(
			() => client.rows.get('document_yjs_updates')?.length === 1,
			'Buffered CRDT update was not uploaded',
		);
		expect(persister.getSyncStatus().pendingCount).toBe(0);
		const receiverStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const receiver = await createSupabasePersister(
			receiverStore,
			configuration(client, `buffered-receiver-${crypto.randomUUID()}`),
		);
		await receiver.openRow('documents', 'doc-1');
		expect(receiverStore.getCell('documents', 'doc-1', 'body')).toBe('abc');
		await receiver.destroy();
		await persister.destroy();
	});

	it('uses a 500ms update buffer by default', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const { crdtUpdateBufferMs: _crdtUpdateBufferMs, ...defaultConfiguration } = configuration(
			client,
			`default-buffer-${crypto.randomUUID()}`,
		);
		const persister = await createSupabasePersister(store, defaultConfiguration);
		await persister.startSyncing();
		const row = await persister.openRow('documents', 'doc-1');
		row.getText('body').insert(0, 'default');
		await tick();

		await new Promise((resolve) => setTimeout(resolve, 100));
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);
		await new Promise((resolve) => setTimeout(resolve, 450));
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		await persister.destroy();
	});

	it('compacts each document independently within the shared buffer window', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [
			{ id: 'doc-1', owner_id: 'user' },
			{ id: 'doc-2', owner_id: 'user' },
		]);
		const store = createStore()
			.setRow('documents', 'doc-1', { owner_id: 'user' })
			.setRow('documents', 'doc-2', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, `multi-buffer-${crypto.randomUUID()}`),
			crdtUpdateBufferMs: 20,
		});
		await persister.startSyncing();
		const first = await persister.openRow('documents', 'doc-1');
		const second = await persister.openRow('documents', 'doc-2');

		first.getText('body').insert(0, 'one');
		first.getMap<string>('properties').set('source', 'first');
		second.getText('body').insert(0, 'two');
		second.getArray<string>('items').push(['second']);
		await waitFor(
			() => client.rows.get('document_yjs_updates')?.length === 2,
			'Buffered CRDT updates were not uploaded',
		);

		const updates = client.rows.get('document_yjs_updates') ?? [];
		expect(updates).toHaveLength(2);
		expect(updates.map(({ document_id }) => document_id).sort()).toEqual(['doc-1', 'doc-2']);
		await persister.destroy();
	});

	it('forces buffered updates through syncNow without waiting for the window', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, `forced-buffer-${crypto.randomUUID()}`),
			crdtUpdateBufferMs: 10_000,
		});
		const row = await persister.openRow('documents', 'doc-1');
		row.getText('body').insert(0, 'first');
		row.getText('body').insert(5, ' second');

		await persister.syncNow();
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		await persister.destroy();
	});

	it('recovers and compacts a durable buffer after recreation', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const databaseName = `durable-buffer-${crypto.randomUUID()}`;
		const firstStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const first = await createSupabasePersister(firstStore, {
			...configuration(client, databaseName),
			crdtUpdateBufferMs: 10_000,
		});
		const firstRow = await first.openRow('documents', 'doc-1');
		firstRow.getText('body').insert(0, 'durable');
		firstRow.getText('body').insert(7, ' buffer');
		await tick();
		await first.destroy();
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);

		const secondStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const second = await createSupabasePersister(secondStore, {
			...configuration(client, databaseName),
			crdtUpdateBufferMs: 10_000,
		});
		await second.openRow('documents', 'doc-1');
		expect(secondStore.getCell('documents', 'doc-1', 'body')).toBe('durable buffer');
		await second.syncNow();

		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		expect(second.getSyncStatus().pendingCount).toBe(0);
		await second.destroy();
	});

	it('deduplicates concurrent row opens and removes a failed open from lifecycle state', async () => {
		const client = new MemorySupabase();
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(
			store,
			configuration(client, `concurrent-open-${crypto.randomUUID()}`),
		);

		const [first, second] = await Promise.all([
			persister.openRow('documents', 'doc-1'),
			persister.openRow('documents', 'doc-1'),
		]);
		expect(first).toBe(second);
		expect(persister.isRowOpen('documents', 'doc-1')).toBe(true);
		await first.destroy();
		expect(persister.isRowOpen('documents', 'doc-1')).toBe(false);
		await persister.destroy();
	});

	it('does not persist or retain a malformed remote Yjs update after open fails', async () => {
		const client = new MemorySupabase();
		const updates = [
			{
				created_at: new Date().toISOString(),
				document_id: 'doc-1',
				id: 'malformed',
				update: '\\x00',
			},
		];
		client.rows.set('document_yjs_updates', updates);
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, `malformed-${crypto.randomUUID()}`),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					realtime: true,
				},
			},
		});

		await persister.startSyncing();
		await expect(persister.openRow('documents', 'doc-1')).rejects.toThrow();
		expect(persister.isRowOpen('documents', 'doc-1')).toBe(false);
		expect(
			client.channels.filter(
				({ active, filter }) => active && filter.table === 'document_yjs_updates',
			),
		).toHaveLength(0);
		updates.splice(0);
		await expect(persister.openRow('documents', 'doc-1')).resolves.toBeDefined();
		await persister.destroy();
	});

	it('converges concurrent edits within one text cell through append-only updates', async () => {
		const client = new MemorySupabase();
		const firstStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const secondStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const first = await createSupabasePersister(
			firstStore,
			configuration(client, `crdt-first-${crypto.randomUUID()}`),
		);
		const second = await createSupabasePersister(
			secondStore,
			configuration(client, `crdt-second-${crypto.randomUUID()}`),
		);
		const firstRow = await first.openRow('documents', 'doc-1');
		const secondRow = await second.openRow('documents', 'doc-1');

		firstRow.getText('body').insert(0, 'alpha');
		secondRow.getText('body').insert(0, 'beta');
		await tick();
		await first.syncNow();
		await second.syncNow();
		await first.syncNow();

		expect(firstStore.getCell('documents', 'doc-1', 'body')).toBe(
			secondStore.getCell('documents', 'doc-1', 'body'),
		);
		expect(String(firstStore.getCell('documents', 'doc-1', 'body'))).toContain('alpha');
		expect(String(firstStore.getCell('documents', 'doc-1', 'body'))).toContain('beta');
		expect(client.rows.get('document_yjs_updates')).toHaveLength(2);
		await first.destroy();
		await second.destroy();
	});

	it('converges concurrent map and array edits and multi-type transactions', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const firstStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const secondStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const first = await createSupabasePersister(
			firstStore,
			configuration(client, `typed-first-${crypto.randomUUID()}`),
		);
		const second = await createSupabasePersister(
			secondStore,
			configuration(client, `typed-second-${crypto.randomUUID()}`),
		);
		const firstRow = await first.openRow('documents', 'doc-1');
		const secondRow = await second.openRow('documents', 'doc-1');
		firstRow.getText('body').doc?.transact(() => {
			firstRow.getText('body').insert(0, 'first');
			firstRow.getMap<string>('properties').set('first', 'yes');
			firstRow.getArray<string>('items').push(['first']);
		});
		secondRow.getMap<string>('properties').set('second', 'yes');
		secondRow.getArray<string>('items').push(['second']);
		await tick();
		await first.syncNow();
		await second.syncNow();
		await first.syncNow();

		expect(firstStore.getCell('documents', 'doc-1', 'properties')).toEqual({
			first: 'yes',
			second: 'yes',
		});
		expect(secondStore.getCell('documents', 'doc-1', 'properties')).toEqual(
			firstStore.getCell('documents', 'doc-1', 'properties'),
		);
		expect(secondStore.getCell('documents', 'doc-1', 'items')).toEqual(
			firstStore.getCell('documents', 'doc-1', 'items'),
		);
		await first.destroy();
		await second.destroy();
	});

	it('rehydrates locally durable CRDT updates after an offline recreation', async () => {
		const client = new MemorySupabase();
		const databaseName = `crdt-reload-${crypto.randomUUID()}`;
		const firstStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const first = await createSupabasePersister(
			firstStore,
			configuration(client, databaseName),
		);
		const firstRow = await first.openRow('documents', 'doc-1');
		firstRow.getText('body').insert(0, 'Offline durable');
		await tick();
		await first.destroy();

		client.selectErrors.set('document_yjs_updates', {
			message: 'network unavailable',
			status: 503,
		});
		const secondStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const second = await createSupabasePersister(
			secondStore,
			configuration(client, databaseName),
		);
		await second.startSyncing();
		await expect(second.openRow('documents', 'doc-1')).resolves.toBeDefined();
		expect(secondStore.getCell('documents', 'doc-1', 'body')).toBe('Offline durable');
		await waitForSyncPhase(second, 'offline');
		expect(second.getSyncStatus().phase).toBe('offline');

		client.selectErrors.delete('document_yjs_updates');
		await second.syncNow();
		expect(second.getSyncStatus().phase).toBe('idle');
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		await second.destroy();
	});

	it('rejects opening a cached CRDT row when the update pull is unauthorized', async () => {
		const client = new MemorySupabase();
		client.selectErrors.set('document_yjs_updates', { message: 'forbidden', status: 403 });
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(
			store,
			configuration(client, `unauthorized-open-${crypto.randomUUID()}`),
		);

		await expect(persister.openRow('documents', 'doc-1')).rejects.toThrow('forbidden');
		expect(persister.isRowOpen('documents', 'doc-1')).toBe(false);
		await persister.destroy();
	});

	it('debounces parent and CRDT realtime events through one reconciliation pass', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, `debounce-${crypto.randomUUID()}`),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					realtime: { debounceMs: 20 },
				},
			},
		});
		await persister.startAutoPersisting();
		await persister.openRow('documents', 'doc-1');
		client.selectCounts.clear();

		client.channels.find(({ filter }) => filter.table === 'documents')?.callback();
		client.channels.find(({ filter }) => filter.table === 'document_yjs_updates')?.callback();
		await waitFor(
			() => client.selectCounts.get('document_yjs_updates') === 1,
			'Reconciliation did not pull the CRDT updates table',
		);

		// One parent reconciliation uses keyset queries for the page, timestamp tie, and EOF.
		expect(client.selectCounts.get('documents')).toBe(3);
		expect(client.selectCounts.get('document_yjs_updates')).toBe(1);
		await persister.destroy();
	});

	it('starts and stops parent and CRDT realtime subscriptions together', async () => {
		const client = new MemorySupabase();
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, `lifecycle-${crypto.randomUUID()}`),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					realtime: true,
				},
			},
		});
		await persister.openRow('documents', 'doc-1');
		expect(client.channels).toHaveLength(0);

		await persister.startSyncing();
		expect(
			client.channels.filter(({ active }) => active).map(({ filter }) => filter.table),
		).toEqual(expect.arrayContaining(['documents', 'document_yjs_updates']));
		await persister.stopSyncing();
		expect(client.channels.filter(({ active }) => active)).toHaveLength(0);
		await persister.destroy();
	});

	it('starts ordinary syncing and retries when CRDT realtime startup fails', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		client.subscriptionStatuses.set('document_yjs_updates', 'CHANNEL_ERROR');
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const errors: Error[] = [];
		const persister = await createSupabasePersister(store, {
			...configuration(client, `startup-retry-${crypto.randomUUID()}`),
			onError: (error) => errors.push(error),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					realtime: true,
				},
			},
		});
		await persister.openRow('documents', 'doc-1');

		await expect(persister.startSyncing()).resolves.toBeUndefined();
		expect(errors.some(({ message }) => message.includes('CHANNEL_ERROR'))).toBe(true);
		expect(
			client.channels.some(({ active, filter }) => active && filter.table === 'documents'),
		).toBe(true);

		client.subscriptionStatuses.delete('document_yjs_updates');
		await persister.syncNow();
		expect(
			client.channels.some(
				({ active, filter }) => active && filter.table === 'document_yjs_updates',
			),
		).toBe(true);
		await persister.destroy();
	});

	it('removes a CRDT channel that finishes subscribing after syncing stops', async () => {
		const client = new MemorySupabase();
		client.delayedSubscriptionTables.add('document_yjs_updates');
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, `late-subscription-${crypto.randomUUID()}`),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					realtime: true,
				},
			},
		});

		await persister.startSyncing();
		const opening = persister.openRow('documents', 'doc-1');
		await waitFor(
			() =>
				client.channels.some(
					({ active, filter }) => active && filter.table === 'document_yjs_updates',
				),
			'CRDT realtime subscription did not start',
		);

		await persister.stopSyncing();
		await expect(opening).resolves.toBeDefined();
		expect(client.channels.filter(({ active }) => active)).toHaveLength(0);
		client.releaseSubscriptions('document_yjs_updates');
		await persister.destroy();
	});

	it('activates a CRDT realtime subscription before the authoritative open pull', async () => {
		const client = new MemorySupabase();
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		let updateSelectsAtSubscription = -1;
		client.onSubscribe = ({ filter }) => {
			if (filter.table === 'document_yjs_updates') {
				updateSelectsAtSubscription = client.selectCounts.get('document_yjs_updates') ?? 0;
			}
		};
		const persister = await createSupabasePersister(store, {
			...configuration(client, `subscribe-before-pull-${crypto.randomUUID()}`),
			tables: {
				documents: {
					...configuration(client, 'unused').tables.documents,
					realtime: true,
				},
			},
		});
		await persister.startSyncing();
		client.selectCounts.clear();
		await persister.openRow('documents', 'doc-1');

		expect(updateSelectsAtSubscription).toBe(0);
		expect(client.selectCounts.get('document_yjs_updates')).toBe(1);
		await persister.destroy();
	});

	it('uses the shared safety poll to recover CRDT updates without realtime', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const firstStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const secondStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const first = await createSupabasePersister(firstStore, {
			...configuration(client, `poll-first-${crypto.randomUUID()}`),
			pollIntervalMs: 10,
		});
		const second = await createSupabasePersister(secondStore, {
			...configuration(client, `poll-second-${crypto.randomUUID()}`),
			pollIntervalMs: 10,
		});
		await first.startAutoPersisting();
		await second.startAutoPersisting();
		const firstRow = await first.openRow('documents', 'doc-1');
		await second.openRow('documents', 'doc-1');

		firstRow.getText('body').insert(0, 'Recovered by poll');
		await tick();
		await waitFor(
			() => secondStore.getCell('documents', 'doc-1', 'body') === 'Recovered by poll',
			'CRDT update was not recovered by the safety poll',
		);
		await first.destroy();
		await second.destroy();
	});

	it('does not upload CRDT updates until the ordinary parent row succeeds', async () => {
		const client = new MemorySupabase();
		client.errors.set('documents', { message: 'network unavailable', status: 503 });
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			configuration(client, `parent-order-${crypto.randomUUID()}`),
		);
		store.setRow('documents', 'doc-1', { owner_id: 'user' });
		await persister.save();
		const row = await persister.openRow('documents', 'doc-1');
		row.getText('body').insert(0, 'Wait for parent');
		await tick();
		await persister.syncNow();
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);

		client.errors.delete('documents');
		await persister.syncNow();
		expect(client.rows.get('documents')).toContainEqual(
			expect.objectContaining({ id: 'doc-1' }),
		);
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		await persister.destroy();
	});

	it('reports and retries transient CRDT transport failures', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, {
			...configuration(client, `crdt-retry-${crypto.randomUUID()}`),
			retryBaseDelayMs: 5,
			retryMaxDelayMs: 5,
		});
		const statuses: Array<{ pendingCount: number; phase: string }> = [];
		const removeStatusListener = persister.addSyncStatusListener((status) =>
			statuses.push({ pendingCount: status.pendingCount, phase: status.phase }),
		);
		await persister.startSyncing();
		const row = await persister.openRow('documents', 'doc-1');
		client.errors.set('document_yjs_updates', { message: 'network unavailable', status: 503 });
		row.getText('body').insert(0, 'Retry');
		await tick();
		await waitFor(
			() =>
				statuses.some(
					({ pendingCount, phase }) => pendingCount === 1 && phase === 'offline',
				),
			'CRDT transport failure was not reported as offline',
		);

		client.errors.delete('document_yjs_updates');
		await waitFor(() => {
			const status = persister.getSyncStatus();
			return status.pendingCount === 0 && status.phase === 'idle';
		}, 'CRDT retry did not return to idle');
		expect(persister.getSyncStatus()).toMatchObject({ pendingCount: 0, phase: 'idle' });
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		expect(statuses).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ pendingCount: 1 }),
				expect.objectContaining({ pendingCount: 1, phase: 'offline' }),
				expect.objectContaining({ pendingCount: 0, phase: 'idle' }),
			]),
		);
		removeStatusListener();
		await persister.destroy();
	});

	it('retains permanent CRDT failures for explicit retry', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const databaseName = `crdt-rejected-${crypto.randomUUID()}`;
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, configuration(client, databaseName));
		const row = await persister.openRow('documents', 'doc-1');
		client.errors.set('document_yjs_updates', {
			message: 'new row violates row-level security policy',
			status: 403,
		});
		row.getText('body').insert(0, 'Rejected');
		row.getText('body').insert(8, ' update');
		row.getMap<string>('properties').set('state', 'rejected');
		row.getArray<string>('items').push(['pending']);
		await tick();
		await persister.syncNow();
		await expect(persister.getRejectedOperations()).resolves.toEqual([
			expect.objectContaining({ rowId: 'doc-1', tableId: 'documents' }),
		]);
		const rejectedState = await CrdtLocalState.open(databaseName, 'user');
		await expect(rejectedState.getQuarantined()).resolves.toEqual([
			expect.objectContaining({
				rowId: 'doc-1',
				state: 'rejected',
				tableId: 'documents',
			}),
		]);
		rejectedState.close();

		client.errors.delete('document_yjs_updates');
		await persister.retryRejected();
		await expect(persister.getRejectedOperations()).resolves.toEqual([]);
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		const retriedState = await CrdtLocalState.open(databaseName, 'user');
		await expect(retriedState.getQuarantined()).resolves.toEqual([]);
		retriedState.close();
		await persister.destroy();
	});

	it('converges dependent updates after a rejected predecessor is retried', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		client.errors.set('document_yjs_updates', { message: 'forbidden', status: 403 });
		const senderStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const sender = await createSupabasePersister(
			senderStore,
			configuration(client, `dependent-sender-${crypto.randomUUID()}`),
		);
		const senderRow = await sender.openRow('documents', 'doc-1');
		senderRow.getText('body').insert(0, 'A');
		await sender.syncNow();
		await expect(sender.getRejectedOperations()).resolves.toHaveLength(1);

		client.errors.delete('document_yjs_updates');
		senderRow.getText('body').insert(1, 'B');
		await sender.syncNow();
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);

		const receiverStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const receiver = await createSupabasePersister(
			receiverStore,
			configuration(client, `dependent-receiver-${crypto.randomUUID()}`),
		);
		await receiver.openRow('documents', 'doc-1');
		expect(receiverStore.getCell('documents', 'doc-1', 'body')).toBe('');

		await sender.retryRejected();
		await receiver.syncNow();
		expect(receiverStore.getCell('documents', 'doc-1', 'body')).toBe('AB');
		await receiver.destroy();
		await sender.destroy();
	});

	it('keeps quarantine across restart while retaining optimistic local edits', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		client.errors.set('document_yjs_updates', { message: 'forbidden', status: 403 });
		const databaseName = `quarantine-restart-${crypto.randomUUID()}`;
		const firstStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const first = await createSupabasePersister(
			firstStore,
			configuration(client, databaseName),
		);
		const firstRow = await first.openRow('documents', 'doc-1');
		firstRow.getText('body').insert(0, 'Rejected');
		await first.syncNow();
		firstRow.getText('body').insert(8, ' successor');
		await first.syncNow();
		expect(firstStore.getCell('documents', 'doc-1', 'body')).toBe('Rejected successor');
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);
		await first.destroy();

		const secondStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const second = await createSupabasePersister(
			secondStore,
			configuration(client, databaseName),
		);
		await second.openRow('documents', 'doc-1');
		expect(secondStore.getCell('documents', 'doc-1', 'body')).toBe('Rejected successor');
		await second.syncNow();
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);

		client.errors.delete('document_yjs_updates');
		await second.retryRejected();
		expect(second.getSyncStatus()).toMatchObject({ pendingCount: 0, rejectedCount: 0 });
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		await second.destroy();
	});

	it('keeps a retrying document quarantined across transient failures', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const databaseName = `quarantine-retry-${crypto.randomUUID()}`;
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, configuration(client, databaseName));
		const row = await persister.openRow('documents', 'doc-1');
		client.errors.set('document_yjs_updates', { message: 'forbidden', status: 403 });
		row.getText('body').insert(0, 'A');
		await persister.syncNow();

		client.errors.set('document_yjs_updates', {
			message: 'network unavailable',
			status: 503,
		});
		await persister.retryRejected();
		row.getText('body').insert(1, 'B');
		await persister.syncNow();
		const retryingState = await CrdtLocalState.open(databaseName, 'user');
		await expect(retryingState.getQuarantined()).resolves.toEqual([
			expect.objectContaining({ state: 'retrying' }),
		]);
		await expect(retryingState.getOutbox()).resolves.toHaveLength(1);
		retryingState.close();
		expect(client.rows.get('document_yjs_updates')).toHaveLength(0);

		client.errors.delete('document_yjs_updates');
		await persister.syncNow();
		expect(persister.getSyncStatus()).toMatchObject({ pendingCount: 0, rejectedCount: 0 });
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
		const completedState = await CrdtLocalState.open(databaseName, 'user');
		await expect(completedState.getQuarantined()).resolves.toEqual([]);
		completedState.close();
		await persister.destroy();
	});

	it('coalesces multiple pending envelopes for one document before upload', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const databaseName = `outbox-coalescing-${crypto.randomUUID()}`;
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, configuration(client, databaseName));
		const row = await persister.openRow('documents', 'doc-1');
		client.errors.set('document_yjs_updates', {
			message: 'network unavailable',
			status: 503,
		});
		row.getText('body').insert(0, 'A');
		await persister.syncNow();
		row.getText('body').insert(1, 'B');
		await persister.syncNow();

		const pendingState = await CrdtLocalState.open(databaseName, 'user');
		await expect(pendingState.getOutbox()).resolves.toHaveLength(1);
		await expect(pendingState.getDocumentUpdates('documents\0doc-1')).resolves.toHaveLength(1);
		pendingState.close();

		client.errors.delete('document_yjs_updates');
		await persister.syncNow();
		const receiverStore = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const receiver = await createSupabasePersister(
			receiverStore,
			configuration(client, `outbox-coalescing-receiver-${crypto.randomUUID()}`),
		);
		await receiver.openRow('documents', 'doc-1');
		expect(receiverStore.getCell('documents', 'doc-1', 'body')).toBe('AB');
		await receiver.destroy();
		await persister.destroy();
	});

	it('discards the complete quarantined chain and invalidates its open handle', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [{ id: 'doc-1', owner_id: 'user' }]);
		const databaseName = `quarantine-discard-${crypto.randomUUID()}`;
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(store, configuration(client, databaseName));
		const row = await persister.openRow('documents', 'doc-1');
		row.getText('body').insert(0, 'Accepted');
		await persister.syncNow();

		client.errors.set('document_yjs_updates', { message: 'forbidden', status: 403 });
		row.getText('body').insert(8, ' rejected');
		await persister.syncNow();
		row.getText('body').insert(17, ' successor');
		await persister.syncNow();
		expect(store.getCell('documents', 'doc-1', 'body')).toBe('Accepted rejected successor');

		await persister.discardRejected();
		expect(persister.isRowOpen('documents', 'doc-1')).toBe(false);
		expect(store.hasCell('documents', 'doc-1', 'body')).toBe(false);
		expect(() => row.getText('body').insert(0, 'stale')).toThrow('is closed');
		const state = await CrdtLocalState.open(databaseName, 'user');
		await expect(state.getBuffered()).resolves.toHaveLength(0);
		await expect(state.getOutbox()).resolves.toHaveLength(0);
		await expect(state.getRejected()).resolves.toHaveLength(0);
		await expect(state.getQuarantined()).resolves.toHaveLength(0);
		await expect(state.getDocumentUpdates('documents\0doc-1')).resolves.toHaveLength(1);
		state.close();

		client.errors.delete('document_yjs_updates');
		await persister.openRow('documents', 'doc-1');
		expect(store.getCell('documents', 'doc-1', 'body')).toBe('Accepted');
		await persister.destroy();
	});

	it('quarantines one document without blocking another document', async () => {
		const client = new MemorySupabase();
		client.rows.set('documents', [
			{ id: 'blocked', owner_id: 'user' },
			{ id: 'writable', owner_id: 'user' },
		]);
		const store = createStore()
			.setRow('documents', 'blocked', { owner_id: 'user' })
			.setRow('documents', 'writable', { owner_id: 'user' });
		const persister = await createSupabasePersister(
			store,
			configuration(client, `quarantine-isolation-${crypto.randomUUID()}`),
		);
		const blocked = await persister.openRow('documents', 'blocked');
		const writable = await persister.openRow('documents', 'writable');
		client.errors.set('document_yjs_updates', { message: 'forbidden', status: 403 });
		blocked.getText('body').insert(0, 'Blocked');
		await persister.syncNow();

		client.errors.delete('document_yjs_updates');
		blocked.getText('body').insert(7, ' successor');
		writable.getText('body').insert(0, 'Uploaded');
		await persister.syncNow();
		expect(client.rows.get('document_yjs_updates')).toEqual([
			expect.objectContaining({ document_id: 'writable' }),
		]);
		await persister.destroy();
	});
});
