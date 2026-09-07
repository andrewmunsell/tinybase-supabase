import 'fake-indexeddb/auto';
import { jest } from '@jest/globals';
import { openDB } from 'idb';
import { createStore, type Row } from 'tinybase';
import { createCustomPersister } from 'tinybase/persisters';
import {
	createSupabasePersister,
	IndexedDbConnectionClosedForUpgradeError,
} from '../../src/index.js';
import { StandardTransport } from '../../src/standard/protocol.js';
import { LocalState } from '../../src/storage.js';

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
	removedChannelCount = 0;
	#timestamp = 0;
	#selectCount = 0;
	onSelect?: (table: string, count: number) => void;
	permanentError: { message: string; status: number } | undefined;
	serverRowLimit = Number.POSITIVE_INFINITY;
	transientError: { message: string; status: number } | undefined;

	get selectCount(): number {
		return this.#selectCount;
	}

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

	removeChannel(): void {
		this.removedChannelCount += 1;
	}
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

// TinyBase 8 uses application codecs for JSON cells; TinyBase 9 stores them directly.
const supportsJsonCells = createStore()
	.setCell('probe', 'row', 'cell', { value: true })
	.hasCell('probe', 'row', 'cell');
const localPreferences = (liveUpdates: boolean) =>
	supportsJsonCells ? { liveUpdates } : JSON.stringify({ liveUpdates });
const jsonCodec = supportsJsonCells
	? {}
	: {
			toRemote: (_rowId: string, row: Row) => ({
				...row,
				...(typeof row.preferences === 'string'
					? { preferences: JSON.parse(row.preferences) }
					: {}),
			}),
			fromRemote: (row: RemoteRow): readonly [string, Row] => {
				const {
					id,
					deleted_at: _deleted,
					updated_at: _updated,
					preferences,
					...cells
				} = row;
				return [
					String(id),
					{
						...cells,
						...(preferences === undefined
							? {}
							: { preferences: JSON.stringify(preferences) }),
					} as Row,
				];
			},
		};

const gate = () => {
	let release!: () => void;
	const promise = new Promise<void>((resolve) => {
		release = resolve;
	});

	return { promise, release };
};

describe.each([false, true])('concurrent writes (hybrid=%s)', (hybrid) => {
	const setup = async () => {
		const client = new MemorySupabase();
		client.rows.set(
			'todos',
			new Map([
				[
					'row-1',
					{
						id: 'row-1',
						preferences: { liveUpdates: true },
						title: 'initial',
						deleted_at: null,
						updated_at: client.nextUpdatedAt(),
					},
				],
			]),
		);
		const config = {
			...configuration(client, `concurrency-${crypto.randomUUID()}`),
			onError: jest.fn<(error: Error) => void>(),
			cursorLookbackMs: 0,
			tables: {
				todos: { ...configuration(client, '').tables.todos, ...jsonCodec },
				other: { table: 'other', ...jsonCodec },
				...(hybrid
					? {
							documents: {
								table: 'documents',
								crdtCells: { body: { type: 'text' as const } },
								crdtUpdatesTable: 'updates',
							},
						}
					: {}),
			},
		};

		const store = createStore();
		const persister = await createSupabasePersister(store, config);
		await persister.startAutoPersisting();
		await persister.stopAutoSave();
		const state = await LocalState.open(config.databaseName, config.scopeKey);
		return { client, config, store, persister, state };
	};

	it.each([
		'todos',
		'other',
	])('retains a local edit in %s after reading a remote content snapshot', async (tableId) => {
		const { client, config, store, persister, state } = await setup();
		const entered = gate();
		const release = gate();
		const original = LocalState.prototype.getContent;
		const spy = jest
			.spyOn(LocalState.prototype, 'getContent')
			.mockImplementationOnce(async function (this: LocalState) {
				const content = await original.call(this);
				entered.release();
				await release.promise;
				return content;
			});

		const syncing = persister.syncNow();
		await entered.promise;
		store.setRow(tableId, 'row-1', { preferences: localPreferences(false) });
		const saving = persister.save();
		client.transientError = { message: 'offline', status: 503 };
		release.release();
		await Promise.all([saving, syncing]);
		spy.mockRestore();
		try {
			expect(store.getRow(tableId, 'row-1')).toEqual({
				preferences: localPreferences(false),
			});

			expect((await state.getContent())?.[0][tableId]?.['row-1']).toEqual(
				store.getRow(tableId, 'row-1'),
			);
			expect(await state.getOperations()).toEqual([
				expect.objectContaining({ tableId, rowId: 'row-1' }),
			]);
			await persister.destroy();
			const reopenedStore = createStore();
			const reopened = await createSupabasePersister(reopenedStore, config);
			await reopened.startAutoPersisting();
			expect(reopenedStore.getRow(tableId, 'row-1')).toEqual({
				preferences: { liveUpdates: false },
			});

			client.transientError = undefined;
			await reopened.syncNow();
			expect(client.rows.get(tableId)?.get('row-1')?.preferences).toEqual({
				liveUpdates: false,
			});

			await reopened.destroy();
		} finally {
			state.close();
			await persister.destroy();
		}
	});

	it.each([
		'accepted',
		'rejected',
		'tombstone',
		'rejected-tombstone',
	])('keeps revision B when in-flight A is %s', async (outcome) => {
		const { client, config, store, persister, state } = await setup();
		const entered = gate();
		const release = gate();
		const original = StandardTransport.prototype.upsert;
		const spy = jest
			.spyOn(StandardTransport.prototype, 'upsert')
			.mockImplementationOnce(async function (this: StandardTransport, config, payload) {
				entered.release();
				await release.promise;
				if (outcome.includes('rejected')) {
					throw { message: 'denied A', code: '42501' };
				}

				await original.call(this, config, payload);
				client.transientError = { message: 'offline B', status: 503 };
			});

		store.setCell('todos', 'row-1', 'title', 'A');
		await persister.save();
		await entered.promise;
		if (outcome.includes('tombstone')) {
			store.delRow('todos', 'row-1');
		} else {
			store.setCell('todos', 'row-1', 'title', 'B');
		}

		await persister.save();
		if (outcome.includes('rejected')) {
			client.transientError = { message: 'offline B', status: 503 };
		}

		release.release();
		await persister.syncNow();
		spy.mockRestore();
		try {
			expect(await state.getOperations()).toEqual([
				expect.objectContaining(
					outcome.includes('tombstone')
						? { kind: 'tombstone' }
						: { payload: expect.objectContaining({ title: 'B' }) },
				),
			]);
			expect(await state.getRejected()).toEqual([]);
			await persister.destroy();
			const reopenedStore = createStore();
			const reopened = await createSupabasePersister(reopenedStore, config);
			await reopened.startAutoPersisting();
			expect(reopenedStore.getCell('todos', 'row-1', 'title')).toBe(
				outcome.includes('tombstone') ? undefined : 'B',
			);
			client.transientError = undefined;
			await reopened.syncNow();
			const remote = client.rows.get('todos')?.get('row-1');
			if (outcome.includes('tombstone')) {
				expect(remote?.deleted_at).toEqual(expect.any(String));
			} else {
				expect(remote?.title).toBe('B');
			}

			await reopened.destroy();
		} finally {
			state.close();
			await persister.destroy();
		}
	});

	it('aborts durable replacement without advancing its cursor or losing an unsaved edit', async () => {
		const { client, store, persister, state } = await setup();
		const oldRow = client.rows.get('todos')?.get('row-1');
		client.rows.get('todos')?.set('row-2', {
			id: 'row-2',
			title: 'unapplied',
			deleted_at: null,
			updated_at: client.nextUpdatedAt(),
		});

		const put = IDBObjectStore.prototype.put;
		let injected = false;
		const write = jest.spyOn(IDBObjectStore.prototype, 'put').mockImplementation(function (
			this: IDBObjectStore,
			...args
		) {
			const request = put.apply(this, args);
			if (this.name === 'content' && !injected) {
				injected = true;
				request.addEventListener(
					'success',
					() => store.setCell('todos', 'row-1', 'title', 'unsaved edit'),
					{ once: true },
				);
			}

			return request;
		});

		const replace = LocalState.prototype.replaceContent;
		let applied: unknown;
		const replacement = jest
			.spyOn(LocalState.prototype, 'replaceContent')
			.mockImplementationOnce(async function (this: LocalState, ...args) {
				applied = await replace.apply(this, args);
				throw new Error('Controlled interruption after the replacement attempt');
			});

		await persister.syncNow();
		write.mockRestore();
		replacement.mockRestore();
		try {
			expect(applied).toBe(false);
			expect(store.getCell('todos', 'row-1', 'title')).toBe('unsaved edit');
			expect(store.hasRow('todos', 'row-2')).toBe(false);
			const cursorKey = JSON.stringify([
				'todos',
				'todos',
				'id',
				'deleted_at',
				'updated_at',
				'*',
				'',
			]);
			expect(await state.getCursor(cursorKey)).toEqual({ updatedAt: oldRow?.updated_at });
			await persister.save();
			expect((await state.getContent())?.[0].todos?.['row-1']?.title).toBe('unsaved edit');
			expect((await state.getOperations()).map((op) => op.rowId)).toEqual(['row-1']);
			await persister.syncNow();
			expect(store.getCell('todos', 'row-2', 'title')).toBe('unapplied');
			expect(client.rows.get('todos')?.get('row-1')?.title).toBe('unsaved edit');
			store.delRow('todos', 'row-2');
			await persister.save();
			await persister.syncNow();
			expect(client.rows.get('todos')?.get('row-2')?.deleted_at).toEqual(expect.any(String));
		} finally {
			state.close();
			await persister.destroy();
		}
	});

	it.each([
		'load',
		'startAutoLoad',
	] as const)('preserves edits and saves while %s is awaiting storage', async (method) => {
		const { store, persister, state } = await setup();
		const entered = gate();
		const release = gate();
		const original = LocalState.prototype.getContent;
		const read = jest
			.spyOn(LocalState.prototype, 'getContent')
			.mockImplementationOnce(async function (this: LocalState) {
				const content = await original.call(this);
				entered.release();
				await release.promise;
				return content;
			});

		const loading = persister[method]();
		await entered.promise;
		store.setCell('todos', 'row-1', 'title', 'during loading');
		const saving = persister.save();
		release.release();
		await Promise.all([loading, saving]);
		read.mockRestore();
		try {
			expect(store.getCell('todos', 'row-1', 'title')).toBe('during loading');
			expect((await state.getContent())?.[0].todos?.['row-1']?.title).toBe('during loading');
			await persister.syncNow();
		} finally {
			state.close();
			await persister.destroy();
		}
	});

	it.each([
		'edit',
		'delete',
	])('restores the baseline and local %s after a transaction aborts after application', async (change) => {
		const { client, store, persister, state } = await setup();
		client.rows.get('todos')?.set('row-2', {
			id: 'row-2',
			title: 'remote',
			deleted_at: null,
			updated_at: client.nextUpdatedAt(),
		});

		let transaction: IDBTransaction | undefined;
		const put = IDBObjectStore.prototype.put;
		const write = jest.spyOn(IDBObjectStore.prototype, 'put').mockImplementation(function (
			this: IDBObjectStore,
			...args
		) {
			if (this.name === 'content') {
				transaction = this.transaction;
			}

			return put.apply(this, args);
		});

		const replace = LocalState.prototype.replaceContent;
		const replacement = jest
			.spyOn(LocalState.prototype, 'replaceContent')
			.mockImplementationOnce(async function (this: LocalState, content, key, cursor, apply) {
				return replace.call(this, content, key, cursor, () => {
					const applied = apply();
					if (change === 'delete') {
						store.delRow('todos', 'row-1');
					} else {
						store.setCell('todos', 'row-1', 'title', 'newer edit');
					}

					transaction?.abort();
					return applied;
				});
			});

		await persister.syncNow();
		write.mockRestore();
		replacement.mockRestore();
		try {
			expect(store.getCell('todos', 'row-1', 'title')).toBe(
				change === 'delete' ? undefined : 'newer edit',
			);
			expect(store.hasRow('todos', 'row-2')).toBe(false);
			client.transientError = { message: 'offline', status: 503 };
			await persister.save();
			expect((await state.getOperations()).map((operation) => operation.rowId)).toEqual([
				'row-1',
			]);
			client.transientError = undefined;
			await persister.syncNow();
			expect(store.getCell('todos', 'row-2', 'title')).toBe('remote');
		} finally {
			state.close();
			await persister.destroy();
		}
	});

	it('preserves startup edits and allows synchronization inside the TinyBase scheduler', async () => {
		const { client, config, store, persister, state } = await setup();
		await persister.destroy();
		const reopenedStore = createStore();
		const reopened = await createSupabasePersister(reopenedStore, config);
		const entered = gate();
		const release = gate();
		const original = LocalState.prototype.getContent;
		const read = jest
			.spyOn(LocalState.prototype, 'getContent')
			.mockImplementationOnce(async function (this: LocalState) {
				const content = await original.call(this);
				entered.release();
				await release.promise;
				return content;
			});

		const starting = reopened.startAutoPersisting();
		await entered.promise;
		reopenedStore.setRow('other', 'local', { title: 'startup' });
		const saving = reopened.save();
		release.release();
		await Promise.all([starting, saving]);
		read.mockRestore();
		try {
			expect(reopenedStore.getCell('other', 'local', 'title')).toBe('startup');
			expect(reopenedStore.getRow('todos', 'row-1')).toEqual(store.getRow('todos', 'row-1'));
			client.rows.get('todos')?.set('row-2', {
				id: 'row-2',
				title: 'scheduled remote',
				deleted_at: null,
				updated_at: client.nextUpdatedAt(),
			});

			await reopened.schedule(() => reopened.syncNow());
			expect(reopenedStore.getCell('todos', 'row-2', 'title')).toBe('scheduled remote');
			reopenedStore.setCell('todos', 'row-2', 'title', 'after scheduler');
			await reopened.save();
			await reopened.syncNow();
			expect((await state.getContent())?.[0].todos?.['row-2']?.title).toBe('after scheduler');
		} finally {
			state.close();
			await reopened.destroy();
		}
	});

	it('keeps an edit made while a pull persists an earlier unsaved edit', async () => {
		const { store, persister, state } = await setup();
		store.setCell('todos', 'row-1', 'title', 'first unsaved');
		const entered = gate();
		const release = gate();
		const original = LocalState.prototype.persist;
		const write = jest
			.spyOn(LocalState.prototype, 'persist')
			.mockImplementationOnce(async function (this: LocalState, ...args) {
				entered.release();
				await release.promise;
				return original.apply(this, args);
			});

		const syncing = persister.syncNow();
		const reachedPersistence = await Promise.race([
			entered.promise.then(() => true),
			syncing.then(() => false),
		]);
		if (!reachedPersistence) {
			write.mockRestore();
			try {
				expect(store.getCell('todos', 'row-1', 'title')).toBe('first unsaved');
			} finally {
				state.close();
				await persister.destroy();
			}

			return;
		}

		store.setCell('todos', 'row-1', 'title', 'second unsaved');
		release.release();
		await syncing;
		write.mockRestore();
		try {
			expect(store.getCell('todos', 'row-1', 'title')).toBe('second unsaved');
			expect((await state.getContent())?.[0].todos?.['row-1']?.title).toBe('second unsaved');
		} finally {
			state.close();
			await persister.destroy();
		}
	});

	it('keeps native load defaults and immediate auto-load cancellation', async () => {
		const { store, persister, state } = await setup();
		const fallback = jest.fn(() => [{}, {}] as ReturnType<typeof store.getContent>);
		await persister.load(fallback);
		expect(fallback).not.toHaveBeenCalled();
		const starting = persister.startAutoLoad();
		await persister.stopAutoLoad();
		await starting;
		const native = createCustomPersister(
			createStore(),
			async () => undefined,
			async () => undefined,
			() => true,
			() => undefined,
		);
		await native.startAutoLoad();
		const nativeStarting = native.startAutoLoad();
		await native.stopAutoLoad();
		await nativeStarting;
		expect(persister.isAutoLoading()).toBe(native.isAutoLoading());
		await native.destroy();
		state.close();
		await persister.destroy();
	});

	it('ignores a remote response after destruction', async () => {
		const { config, persister, state } = await setup();
		const entered = gate();
		const release = gate();
		const original = StandardTransport.prototype.fetchRows;
		const fetch = jest
			.spyOn(StandardTransport.prototype, 'fetchRows')
			.mockImplementationOnce(async function (this: StandardTransport, ...args) {
				const result = await original.apply(this, args);
				entered.release();
				await release.promise;
				return result;
			});

		const syncing = persister.syncNow();
		await entered.promise;
		await persister.destroy();
		release.release();
		await syncing;
		fetch.mockRestore();
		state.close();
		expect(config.onError).not.toHaveBeenCalled();
	});

	it('does not upload remote normalization or object-key ordering changes', async () => {
		const { client, store, persister, state } = await setup();
		await persister.startAutoSave();
		const remote = client.rows.get('todos')?.get('row-1') as RemoteRow;
		client.rows.get('todos')?.set('row-1', {
			...Object.fromEntries(Object.entries(remote).reverse()),
			nullable: null,
			updated_at: client.nextUpdatedAt(),
		});

		const upload = jest
			.spyOn(StandardTransport.prototype, 'upsert')
			.mockRejectedValue({ message: 'Unexpected remote echo', code: '42501' });
		await persister.syncNow();
		await persister.save();
		await persister.syncNow();
		try {
			expect(upload).not.toHaveBeenCalled();
			expect(store.getCell('todos', 'row-1', 'title')).toBe('initial');
			expect(await state.getOperations()).toEqual([]);
			expect(await state.getRejected()).toEqual([]);
		} finally {
			upload.mockRestore();
			state.close();
			await persister.destroy();
		}
	});

	it('applies the receiving schema without echoing filtered remote cells', async () => {
		const { client, store, persister, state } = await setup();
		persister.getStore().setTablesSchema({ todos: { title: { type: 'string' } } });
		await persister.save();
		await persister.syncNow();
		const remote = client.rows.get('todos')?.get('row-1') as RemoteRow;
		client.rows.get('todos')?.set('row-1', {
			...remote,
			title: 'schema remote',
			ignored: true,
			updated_at: client.nextUpdatedAt(),
		});

		const upload = jest
			.spyOn(StandardTransport.prototype, 'upsert')
			.mockRejectedValue({ message: 'Unexpected schema echo', code: '42501' });
		await persister.syncNow();
		await persister.save();
		await persister.syncNow();
		try {
			expect(upload).not.toHaveBeenCalled();
			expect(store.getRow('todos', 'row-1')).toEqual({ title: 'schema remote' });
		} finally {
			upload.mockRestore();
			state.close();
			await persister.destroy();
		}
	});

	it('waits for remote application while a persistence action is queued', async () => {
		const { client, store, persister, state } = await setup();
		const entered = gate();
		const release = gate();
		const scheduled = persister.schedule(async () => {
			entered.release();
			await release.promise;
		});

		await entered.promise;
		const saving = persister.save();
		client.rows.get('todos')?.set('row-2', {
			id: 'row-2',
			title: 'remote',
			deleted_at: null,
			updated_at: client.nextUpdatedAt(),
		});

		let complete = false;
		const syncing = persister.syncNow().then(() => {
			complete = true;
			expect(store.getCell('todos', 'row-2', 'title')).toBe('remote');
		});

		// A transport/storage gate proves the pull reached its application boundary.
		const original = LocalState.prototype.replaceContent;
		const replaced = gate();
		const spy = jest
			.spyOn(LocalState.prototype, 'replaceContent')
			.mockImplementationOnce(async function (this: LocalState, ...args) {
				const result = await original.apply(this, args);
				replaced.release();
				return result;
			});

		await replaced.promise;
		release.release();
		await Promise.all([scheduled, saving, syncing]);
		spy.mockRestore();
		try {
			expect(complete).toBe(true);
			store.setCell('todos', 'row-2', 'title', 'local after sync');
			await persister.save();
			expect((await state.getContent())?.[0].todos?.['row-2']?.title).toBe(
				'local after sync',
			);
			await persister.syncNow();
		} finally {
			state.close();
			await persister.destroy();
		}
	});
});

describe('createSupabasePersister', () => {
	it('normalizes an upgrade that closes IndexedDB during initialization', async () => {
		const databaseName = `initialization-upgrade-${crypto.randomUUID()}`;
		let releaseRead = (): void => undefined;
		let reportReadStarted = (): void => undefined;
		const readStarted = new Promise<void>((resolve) => {
			reportReadStarted = resolve;
		});

		const readGate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});

		const originalGetContent = LocalState.prototype.getContent;
		const getContent = jest
			.spyOn(LocalState.prototype, 'getContent')
			.mockImplementation(async function (this: LocalState) {
				reportReadStarted();
				await readGate;
				return originalGetContent.call(this);
			});

		let futureDatabase: Awaited<ReturnType<typeof openDB>> | undefined;
		try {
			const creation = createSupabasePersister(
				createStore(),
				configuration(new MemorySupabase(), databaseName),
			);
			await readStarted;
			futureDatabase = await openDB(`${databaseName}:user-1`, 3);
			releaseRead();
			const persister = await creation;
			const terminalStatus = persister.getSyncStatus();
			const terminalError = terminalStatus.lastError;

			expect(terminalStatus.phase).toBe('error');
			expect(terminalError).toMatchObject({
				code: 'indexeddb-connection-closed-for-upgrade',
				currentVersion: 2,
				requestedVersion: 3,
			});

			await expect(persister.save()).rejects.toBe(terminalError);
			await persister.destroy();
		} finally {
			releaseRead();
			futureDatabase?.close();
			getContent.mockRestore();
		}
	});

	it('preserves an unrelated initialization error that races with an upgrade', async () => {
		const databaseName = `initialization-error-${crypto.randomUUID()}`;
		const sentinel = new Error('Unrelated initialization failure');
		let releaseRead = (): void => undefined;
		let reportReadStarted = (): void => undefined;
		const readStarted = new Promise<void>((resolve) => {
			reportReadStarted = resolve;
		});

		const readGate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});

		const getContent = jest
			.spyOn(LocalState.prototype, 'getContent')
			.mockImplementation(async () => {
				reportReadStarted();
				await readGate;
				throw sentinel;
			});

		let futureDatabase: Awaited<ReturnType<typeof openDB>> | undefined;
		try {
			const creation = createSupabasePersister(
				createStore(),
				configuration(new MemorySupabase(), databaseName),
			);
			await readStarted;
			futureDatabase = await openDB(`${databaseName}:user-1`, 3);
			releaseRead();
			await expect(creation).rejects.toBe(sentinel);
		} finally {
			releaseRead();
			futureDatabase?.close();
			getContent.mockRestore();
		}
	});

	it('becomes terminal when its IndexedDB connection closes for a future upgrade', async () => {
		const client = new MemorySupabase();
		const databaseName = `terminal-upgrade-${crypto.randomUUID()}`;
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			...configuration(client, databaseName),
			pollIntervalMs: 5,
		});

		await persister.startAutoPersisting();
		expect(client.selectCount).toBeGreaterThan(0);
		store.setRow('todos', 'persisted', { title: 'Persisted' });
		await persister.save();
		const statuses: Array<ReturnType<typeof persister.getSyncStatus>> = [];
		persister.addSyncStatusListener((status) => statuses.push(status));

		const futureDatabase = await openDB(`${databaseName}:user-1`, 3);
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
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(client.removedChannelCount).toBe(1);
		await new Promise((resolve) => setTimeout(resolve, 10));
		const selectCountAfterTermination = client.selectCount;
		await new Promise((resolve) => setTimeout(resolve, 20));
		expect(client.selectCount).toBe(selectCountAfterTermination);

		store.setRow('todos', 'unpersisted', { title: 'Must not persist' });
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(await futureDatabase.get('content', 'store')).toEqual([
			{ todos: { persisted: { title: 'Persisted' } } },
			{},
		]);

		for (const operation of [
			persister.load(),
			persister.save(),
			persister.syncNow(),
			persister.startSyncing(),
			persister.startAutoPersisting(),
			persister.retryRejected(),
			persister.discardRejected(),
			persister.getRejectedOperations(),
		]) {
			await expect(operation).rejects.toBe(terminalError);
		}

		futureDatabase.close();
		await persister.destroy();
	});

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
