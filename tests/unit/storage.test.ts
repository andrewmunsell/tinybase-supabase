import 'fake-indexeddb/auto';
import { openDB, type DBSchema } from 'idb';
import type { Content } from 'tinybase';
import { LocalState, type PendingOperation, type RejectedRecord } from '../../src/storage.js';

interface VersionOneDatabase extends DBSchema {
	content: { key: 'store'; value: Content };
	outbox: { key: string; value: PendingOperation };
	rejected: { key: string; value: RejectedRecord };
}

describe('LocalState', () => {
	it('preserves version 1 content and operations when adding cursor storage', async () => {
		const databaseName = `storage-upgrade-${crypto.randomUUID()}`;
		const scopeKey = 'user';
		const database = await openDB<VersionOneDatabase>(`${databaseName}:${scopeKey}`, 1, {
			upgrade(upgradeDatabase) {
				upgradeDatabase.createObjectStore('content');
				upgradeDatabase.createObjectStore('outbox');
				upgradeDatabase.createObjectStore('rejected');
			},
		});
		const content: Content = [{ todos: { cached: { title: 'Cached' } } }, {}];
		const pending: PendingOperation = {
			id: 'todos:pending',
			kind: 'upsert',
			payload: { id: 'pending', title: 'Pending' },
			rowId: 'pending',
			tableId: 'todos',
		};
		const rejected: RejectedRecord = {
			...pending,
			error: 'Rejected',
			id: 'todos:rejected',
			rowId: 'rejected',
		};
		await database.put('content', content, 'store');
		await database.put('outbox', pending, pending.id);
		await database.put('rejected', rejected, rejected.id);
		database.close();

		const state = await LocalState.open(databaseName, scopeKey);
		await expect(state.getContent()).resolves.toEqual(content);
		await expect(state.getOperations()).resolves.toEqual([pending]);
		await expect(state.getRejected()).resolves.toEqual([rejected]);
		await expect(state.getCursor('todos')).resolves.toBeUndefined();
		state.close();
	});
});
