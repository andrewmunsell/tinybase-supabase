import 'fake-indexeddb/auto';
import { createStore } from 'tinybase';
import { createSupabasePersister } from '../../src/index.js';

type RemoteRow = Record<string, unknown>;

class MemoryQuery implements PromiseLike<{ data: RemoteRow[]; error: null }> {
	readonly #filters: Array<(row: RemoteRow) => boolean> = [];
	readonly #maximumRows: number;
	readonly #onFilter: (column: string, value: string) => void;
	readonly #rows: RemoteRow[];
	readonly #orders: string[] = [];
	#from = 0;
	#to = Number.POSITIVE_INFINITY;

	constructor(
		rows: RemoteRow[],
		onFilter: (column: string, value: string) => void,
		maximumRows: number,
	) {
		this.#rows = rows;
		this.#onFilter = onFilter;
		this.#maximumRows = maximumRows;
	}

	eq(column: string, value: string): MemoryQuery {
		this.#filters.push((row) => String(row[column]) === value);
		this.#onFilter(column, value);
		return this;
	}

	gt(column: string, value: string): MemoryQuery {
		this.#filters.push((row) => String(row[column]) > value);
		this.#onFilter(column, value);
		return this;
	}

	gte(column: string, value: string): MemoryQuery {
		this.#filters.push((row) => String(row[column]) >= value);
		this.#onFilter(column, value);
		return this;
	}

	limit(count: number): MemoryQuery {
		this.#to = this.#from + Math.min(count, this.#maximumRows) - 1;
		return this;
	}

	order(column: string): MemoryQuery {
		this.#orders.push(column);
		return this;
	}

	range(from: number, to: number): MemoryQuery {
		this.#from = from;
		this.#to = to;
		return this;
	}

	// biome-ignore lint/suspicious/noThenProperty: Supabase query builders are intentionally thenable.
	then<TResult1 = { data: RemoteRow[]; error: null }, TResult2 = never>(
		onfulfilled?:
			| ((value: { data: RemoteRow[]; error: null }) => TResult1 | PromiseLike<TResult1>)
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
			error: null,
		}).then(onfulfilled, onrejected);
	}
}

class MemorySupabase {
	readonly channels: Array<{ callback: () => void; table: string }> = [];
	readonly cursorQueries: Array<{ column: string; table: string; value: string }> = [];
	readonly rows = new Map<string, Map<string, RemoteRow>>();
	#timestamp = 0;
	#selectCount = 0;
	onSelect?: (table: string, count: number) => void;
	permanentError: { message: string; status: number } | undefined;
	serverRowLimit = Number.POSITIVE_INFINITY;
	transientError: { message: string; status: number } | undefined;

	nextUpdatedAt(): string {
		this.#timestamp += 1;
		return `2026-07-14T00:00:00.${String(this.#timestamp).padStart(6, '0')}+00:00`;
	}

	from(table: string) {
		const rows = this.rows.get(table) ?? new Map<string, RemoteRow>();
		this.rows.set(table, rows);

		return {
			select: () => {
				this.#selectCount += 1;
				this.onSelect?.(table, this.#selectCount);
				for (const row of rows.values()) {
					row.deleted_at ??= null;
				}
				return new MemoryQuery(
					[...rows.values()],
					(column, value) => this.cursorQueries.push({ column, table, value }),
					this.serverRowLimit,
				);
			},
			upsert: async (payload: RemoteRow) => {
				if (this.permanentError) {
					return { data: null, error: this.permanentError };
				}
				if (this.transientError) {
					return { data: null, error: this.transientError };
				}
				rows.set(String(payload.id), {
					...rows.get(String(payload.id)),
					deleted_at: null,
					...payload,
					updated_at: this.nextUpdatedAt(),
				});
				return { data: null, error: null };
			},
		};
	}

	channel(_name: string) {
		const state: { callback: () => void; table: string } = {
			callback: () => undefined,
			table: '',
		};
		const channel = {
			on: (
				_type: 'postgres_changes',
				filter: { event: '*'; schema: string; table: string },
				callback: () => void,
			) => {
				state.callback = callback;
				state.table = filter.table;
				return channel;
			},
			subscribe: () => {
				this.channels.push(state);
			},
		};
		return channel;
	}

	removeChannel(): void {}
}

const configuration = (client: MemorySupabase, databaseName: string) => ({
	databaseName,
	pageSize: 10,
	pollIntervalMs: 0,
	scopeKey: 'user-1',
	supabase: client,
	tables: {
		todos: {
			deletedAtColumn: 'deleted_at',
			realtime: true,
			table: 'todos',
			updatedAtColumn: 'updated_at',
		},
	},
});

const fullPullConfiguration = (client: MemorySupabase, databaseName: string) => ({
	...configuration(client, databaseName),
	tables: {
		todos: {
			deletedAtColumn: 'deleted_at',
			realtime: true,
			table: 'todos',
		},
	},
});

describe('createSupabasePersister', () => {
	it('persists offline rows, flushes them, and reconciles the remote snapshot', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			configuration(client, crypto.randomUUID()),
		);
		await persister.startAutoPersisting();

		store.setRow('todos', 'todo-1', { completed: false, title: 'Write tests' });
		await persister.save();
		await persister.syncNow();

		expect(client.rows.get('todos')?.get('todo-1')).toMatchObject({
			completed: false,
			id: 'todo-1',
			title: 'Write tests',
		});
		expect(persister.getSyncStatus()).toMatchObject({
			pendingCount: 0,
			phase: 'idle',
		});

		await persister.destroy();
	});

	it('converts local deletes into remote tombstones', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			configuration(client, crypto.randomUUID()),
		);
		await persister.startAutoPersisting();

		store.setRow('todos', 'todo-1', { title: 'Remove me' });
		await persister.save();
		await persister.syncNow();
		store.delRow('todos', 'todo-1');
		await persister.save();
		await persister.syncNow();

		expect(client.rows.get('todos')?.get('todo-1')?.deleted_at).toEqual(expect.any(String));
		expect(store.getRow('todos', 'todo-1')).toEqual({});

		await persister.destroy();
	});

	it('keeps discarded optimistic rows across authoritative pulls and restarts', async () => {
		const client = new MemorySupabase();
		const databaseName = crypto.randomUUID();
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			fullPullConfiguration(client, databaseName),
		);
		await persister.startAutoPersisting();
		client.permanentError = {
			message: 'new row violates row-level security policy',
			status: 403,
		};

		store.setRow('todos', 'forbidden', { title: 'Denied' });
		await persister.save();
		await persister.syncNow();

		await expect(persister.getRejectedOperations()).resolves.toEqual([
			expect.objectContaining({ rowId: 'forbidden', tableId: 'todos' }),
		]);
		await persister.discardRejected();
		await expect(persister.getRejectedOperations()).resolves.toEqual([]);
		expect(persister.getSyncStatus().rejectedCount).toBe(0);
		await persister.retryRejected();
		expect(persister.getSyncStatus()).toMatchObject({ pendingCount: 0, rejectedCount: 0 });
		expect(store.getRow('todos', 'forbidden')).toEqual({ title: 'Denied' });

		await persister.destroy();

		const restartedStore = createStore();
		const restarted = await createSupabasePersister(
			restartedStore,
			fullPullConfiguration(client, databaseName),
		);
		await restarted.startAutoPersisting();
		expect(restartedStore.getRow('todos', 'forbidden')).toEqual({ title: 'Denied' });
		client.permanentError = undefined;
		restartedStore.setCell('todos', 'forbidden', 'title', 'Accepted');
		await restarted.save();
		await restarted.syncNow();
		expect(client.rows.get('todos')?.get('forbidden')?.title).toBe('Accepted');
		client.rows.get('todos')?.set('forbidden', {
			deleted_at: null,
			id: 'forbidden',
			title: 'Remote authoritative',
		});
		await restarted.syncNow();
		expect(restartedStore.getCell('todos', 'forbidden', 'title')).toBe('Remote authoritative');

		await restarted.destroy();
	});

	it('uses realtime events as a debounced reconciliation wake-up', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			configuration(client, crypto.randomUUID()),
		);
		await persister.startAutoPersisting();
		client.rows.get('todos')?.set('remote-1', {
			completed: true,
			id: 'remote-1',
			title: 'Remote',
			updated_at: client.nextUpdatedAt(),
		});

		client.channels[0]?.callback();
		await new Promise((resolve) => setTimeout(resolve, 250));

		expect(store.getRow('todos', 'remote-1')).toEqual({ completed: true, title: 'Remote' });
		await persister.destroy();
	});

	it('uses authoritative full pulls when updatedAtColumn is omitted', async () => {
		const client = new MemorySupabase();
		client.rows.set(
			'todos',
			new Map([
				[
					'legacy',
					{
						id: 'legacy',
						title: 'Legacy',
						updated_at: 'existing application value',
					},
				],
				['without-timestamp', { id: 'without-timestamp', title: 'No timestamp' }],
			]),
		);
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			fullPullConfiguration(client, crypto.randomUUID()),
		);
		await persister.startAutoPersisting();

		expect(store.getRow('todos', 'legacy')).toEqual({
			title: 'Legacy',
			updated_at: 'existing application value',
		});
		expect(store.getCell('todos', 'without-timestamp', 'title')).toBe('No timestamp');
		client.rows.get('todos')?.delete('legacy');
		await persister.syncNow();

		expect(store.hasRow('todos', 'legacy')).toBe(false);
		expect(client.cursorQueries).not.toContainEqual(
			expect.objectContaining({ column: 'updated_at', table: 'todos' }),
		);
		await persister.destroy();
	});

	it('uses a durable updated_at cursor and preserves rows omitted from deltas', async () => {
		const client = new MemorySupabase();
		client.rows.set(
			'todos',
			new Map([
				[
					'existing',
					{
						id: 'existing',
						title: 'Existing',
						updated_at: client.nextUpdatedAt(),
					},
				],
			]),
		);
		const databaseName = crypto.randomUUID();
		const firstStore = createStore();
		const first = await createSupabasePersister(
			firstStore,
			configuration(client, databaseName),
		);
		await first.startAutoPersisting();
		expect(firstStore.getCell('todos', 'existing', 'title')).toBe('Existing');

		client.rows.get('todos')?.delete('existing');
		client.rows.get('todos')?.set('new', {
			id: 'new',
			title: 'New',
			updated_at: client.nextUpdatedAt(),
		});
		await first.syncNow();

		expect(firstStore.getCell('todos', 'existing', 'title')).toBe('Existing');
		expect(firstStore.getCell('todos', 'new', 'title')).toBe('New');
		expect(client.cursorQueries).toContainEqual(
			expect.objectContaining({ column: 'updated_at', table: 'todos' }),
		);
		await first.destroy();

		client.cursorQueries.length = 0;
		const second = await createSupabasePersister(
			createStore(),
			configuration(client, databaseName),
		);
		await second.startAutoPersisting();
		expect(client.cursorQueries).toContainEqual(
			expect.objectContaining({ column: 'updated_at', table: 'todos' }),
		);
		await second.destroy();
	});

	it('applies incremental soft-delete tombstones', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			configuration(client, crypto.randomUUID()),
		);
		await persister.startAutoPersisting();
		client.rows.get('todos')?.set('remote', {
			id: 'remote',
			title: 'Remote',
			updated_at: client.nextUpdatedAt(),
		});
		await persister.syncNow();
		expect(store.hasRow('todos', 'remote')).toBe(true);

		client.rows.get('todos')?.set('remote', {
			deleted_at: client.nextUpdatedAt(),
			id: 'remote',
			title: 'Remote',
			updated_at: client.nextUpdatedAt(),
		});
		await persister.syncNow();
		expect(store.hasRow('todos', 'remote')).toBe(false);
		await persister.destroy();
	});

	it('paginates rows that share an updated_at timestamp', async () => {
		const client = new MemorySupabase();
		const updatedAt = client.nextUpdatedAt();
		client.rows.set(
			'todos',
			new Map([
				['second', { id: 'second', title: 'Second', updated_at: updatedAt }],
				['first', { id: 'first', title: 'First', updated_at: updatedAt }],
			]),
		);
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			...configuration(client, crypto.randomUUID()),
			pageSize: 1,
		});
		await persister.startAutoPersisting();

		expect(store.getRowIds('todos')).toEqual(['first', 'second']);
		await persister.syncNow();
		expect(store.getRowIds('todos')).toEqual(['first', 'second']);
		await persister.destroy();
	});

	it('uses keyset pagination when rows move between page requests', async () => {
		const client = new MemorySupabase();
		client.rows.set(
			'todos',
			new Map([
				['first', { id: 'first', title: 'First', updated_at: client.nextUpdatedAt() }],
				['second', { id: 'second', title: 'Second', updated_at: client.nextUpdatedAt() }],
				['third', { id: 'third', title: 'Third', updated_at: client.nextUpdatedAt() }],
			]),
		);
		client.onSelect = (table, count) => {
			if (table === 'todos' && count === 2) {
				const first = client.rows.get('todos')?.get('first');
				if (first) {
					first.updated_at = client.nextUpdatedAt();
				}
			}
		};
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			...configuration(client, crypto.randomUUID()),
			pageSize: 2,
		});
		await persister.startAutoPersisting();

		expect(store.getRowIds('todos')).toEqual(['first', 'second', 'third']);
		await persister.destroy();
	});

	it('continues full-pull pagination when Supabase caps pages below pageSize', async () => {
		const client = new MemorySupabase();
		client.serverRowLimit = 2;
		client.rows.set(
			'todos',
			new Map(
				Array.from({ length: 5 }, (_, index) => [
					`row-${index}`,
					{
						id: `row-${index}`,
						title: `Row ${index}`,
					},
				]),
			),
		);
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			...fullPullConfiguration(client, crypto.randomUUID()),
			pageSize: 10,
		});
		await persister.startAutoPersisting();

		expect(store.getRowIds('todos')).toHaveLength(5);
		await persister.destroy();
	});

	it('uses the cursor lookback to recover a late commit', async () => {
		const client = new MemorySupabase();
		const lateUpdatedAt = client.nextUpdatedAt();
		client.rows.set(
			'todos',
			new Map([
				[
					'current',
					{ id: 'current', title: 'Current', updated_at: client.nextUpdatedAt() },
				],
			]),
		);
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			configuration(client, crypto.randomUUID()),
		);
		await persister.startAutoPersisting();
		client.rows.get('todos')?.set('late', {
			id: 'late',
			title: 'Late',
			updated_at: lateUpdatedAt,
		});
		await persister.syncNow();

		expect(store.getCell('todos', 'late', 'title')).toBe('Late');
		await persister.destroy();
	});

	it('starts a new cursor when the remote table mapping changes', async () => {
		const client = new MemorySupabase();
		client.rows.set(
			'todos',
			new Map([
				[
					'current',
					{ id: 'current', title: 'Current', updated_at: client.nextUpdatedAt() },
				],
			]),
		);
		const databaseName = crypto.randomUUID();
		const first = await createSupabasePersister(
			createStore(),
			configuration(client, databaseName),
		);
		await first.startAutoPersisting();
		await first.destroy();

		client.rows.set(
			'archived_todos',
			new Map([
				[
					'archived',
					{
						deleted_at: null,
						id: 'archived',
						title: 'Archived',
						updated_at: '2026-07-13T00:00:00.000000+00:00',
					},
				],
			]),
		);
		const store = createStore();
		const second = await createSupabasePersister(store, {
			...configuration(client, databaseName),
			tables: {
				todos: { table: 'archived_todos', updatedAtColumn: 'updated_at' },
			},
		});
		await second.startAutoPersisting();

		expect(store.getCell('todos', 'archived', 'title')).toBe('Archived');
		await second.destroy();
	});

	it('supports a custom updated-at column without exposing it as a TinyBase cell', async () => {
		const client = new MemorySupabase();
		client.rows.set(
			'todos',
			new Map([
				[
					'custom',
					{
						deleted_at: null,
						id: 'custom',
						modified_at: client.nextUpdatedAt(),
						title: 'Custom',
					},
				],
			]),
		);
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			...configuration(client, crypto.randomUUID()),
			tables: {
				todos: { table: 'todos', updatedAtColumn: 'modified_at' },
			},
		});
		await persister.startAutoPersisting();

		expect(store.getRow('todos', 'custom')).toEqual({ title: 'Custom' });
		await persister.syncNow();
		expect(client.cursorQueries).toContainEqual(
			expect.objectContaining({ column: 'modified_at', table: 'todos' }),
		);
		await persister.destroy();
	});

	it('reports a missing updated-at projection as a synchronization error', async () => {
		const client = new MemorySupabase();
		client.rows.set(
			'todos',
			new Map([['invalid', { deleted_at: null, id: 'invalid', title: 'Invalid' }]]),
		);
		const persister = await createSupabasePersister(createStore(), {
			...configuration(client, crypto.randomUUID()),
			retryBaseDelayMs: 60_000,
			retryMaxDelayMs: 60_000,
		});
		await persister.startAutoPersisting();

		expect(persister.getSyncStatus()).toMatchObject({
			lastError: expect.objectContaining({ message: expect.stringContaining('updated_at') }),
			phase: 'offline',
		});
		await persister.destroy();
	});

	it('retries transient failed writes with exponential backoff', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			...configuration(client, crypto.randomUUID()),
			retryBaseDelayMs: 5,
			retryMaxDelayMs: 5,
		});
		await persister.startAutoPersisting();
		client.transientError = { message: 'network unavailable', status: 503 };

		store.setRow('todos', 'retry', { title: 'Retry me' });
		await persister.save();
		await persister.syncNow();
		expect(persister.getSyncStatus().pendingCount).toBe(1);

		client.transientError = undefined;
		await new Promise((resolve) => setTimeout(resolve, 20));
		expect(client.rows.get('todos')?.get('retry')).toMatchObject({ title: 'Retry me' });

		await persister.destroy();
	});
});
