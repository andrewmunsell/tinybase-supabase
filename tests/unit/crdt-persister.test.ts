import 'fake-indexeddb/auto';
import { createStore } from 'tinybase';
import { createSupabasePersister } from '../../src/index.js';

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
	readonly #rows: RemoteRow[];
	#column?: string;
	#from = 0;
	#to = Number.POSITIVE_INFINITY;
	#value?: string;

	constructor(rows: RemoteRow[], error?: RemoteError) {
		this.#rows = rows;
		this.#error = error;
	}

	eq(column: string, value: string): Query {
		this.#column = column;
		this.#value = value;
		return this;
	}

	order(_column: string): Query {
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
		const filtered = this.#column
			? this.#rows.filter((row) => String(row[this.#column as string]) === this.#value)
			: this.#rows;
		return Promise.resolve({
			data: filtered.slice(this.#from, this.#to + 1),
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
	onSubscribe?: (record: (typeof this.channels)[number]) => void;

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
			select: (_columns: string) => {
				this.selectCounts.set(table, (this.selectCounts.get(table) ?? 0) + 1);
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
					rows.push(value);
				} else {
					rows[index] = { ...rows[index], ...value };
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
		},
	},
});

describe('createSupabasePersister with CRDT cells', () => {
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

		expect(client.rows.get('todos')).toContainEqual({ id: 'todo-1', title: 'Ordinary' });
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

		expect(client.rows.get('audit_logs')).toContainEqual({
			action: 'created',
			id: 'log-1',
		});
		expect(store.getCell('documents', 'doc-1', 'body')).toBe('Hybrid');
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
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
		await new Promise((resolve) => setTimeout(resolve, 30));

		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
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
		await new Promise((resolve) => setTimeout(resolve, 40));

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
		await new Promise((resolve) => setTimeout(resolve, 40));

		expect(client.selectCounts.get('documents')).toBe(1);
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
		await tick();
		expect(
			client.channels.some(
				({ active, filter }) => active && filter.table === 'document_yjs_updates',
			),
		).toBe(true);

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
		await new Promise((resolve) => setTimeout(resolve, 50));

		expect(secondStore.getCell('documents', 'doc-1', 'body')).toBe('Recovered by poll');
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
		await new Promise((resolve) => setTimeout(resolve, 5));
		expect(persister.getSyncStatus()).toMatchObject({ pendingCount: 1, phase: 'offline' });

		client.errors.delete('document_yjs_updates');
		await new Promise((resolve) => setTimeout(resolve, 20));
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
		const store = createStore().setRow('documents', 'doc-1', { owner_id: 'user' });
		const persister = await createSupabasePersister(
			store,
			configuration(client, `crdt-rejected-${crypto.randomUUID()}`),
		);
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

		client.errors.delete('document_yjs_updates');
		await persister.retryRejected();
		await expect(persister.getRejectedOperations()).resolves.toEqual([]);
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);
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
		expect(client.rows.get('document_yjs_updates')).toHaveLength(1);

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
});
