import 'fake-indexeddb/auto';
import { createStore } from 'tinybase';
import { createSupabasePersister } from '../../src/index.js';

type RemoteRow = Record<string, unknown>;

class MemorySupabase {
	readonly channels: Array<{ callback: () => void; table: string }> = [];
	readonly rows = new Map<string, Map<string, RemoteRow>>();
	permanentError: { message: string; status: number } | undefined;

	from(table: string) {
		const rows = this.rows.get(table) ?? new Map<string, RemoteRow>();
		this.rows.set(table, rows);

		const query = Object.assign(Promise.resolve({ data: [...rows.values()], error: null }), {
			order: () => query,
			range: () => query,
		});

		return {
			select: () => query,
			upsert: async (payload: RemoteRow) => {
				if (this.permanentError) {
					return { data: null, error: this.permanentError };
				}
				rows.set(String(payload.id), payload);
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

	it('keeps permanent failures for explicit retry or discard', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			configuration(client, crypto.randomUUID()),
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

		await persister.destroy();
	});

	it('uses realtime events as a debounced reconciliation wake-up', async () => {
		const client = new MemorySupabase();
		const store = createStore();
		const persister = await createSupabasePersister(
			store,
			configuration(client, crypto.randomUUID()),
		);
		await persister.startAutoPersisting();
		client.rows
			.get('todos')
			?.set('remote-1', { completed: true, id: 'remote-1', title: 'Remote' });

		client.channels[0]?.callback();
		await new Promise((resolve) => setTimeout(resolve, 250));

		expect(store.getRow('todos', 'remote-1')).toEqual({ completed: true, title: 'Remote' });
		await persister.destroy();
	});
});
