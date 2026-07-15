import { openDB, type DBSchema, type IDBPDatabase } from 'idb';
import type { Content } from 'tinybase';

export interface PendingOperation {
	readonly id: string;
	readonly kind: 'upsert' | 'tombstone';
	readonly payload: Record<string, unknown>;
	readonly rowId: string;
	readonly tableId: string;
}

export interface RejectedRecord extends PendingOperation {
	readonly error: string;
}

export interface SyncCursor {
	readonly updatedAt: string;
}

interface PersisterDatabase extends DBSchema {
	content: {
		key: 'store';
		value: Content;
	};
	cursors: {
		key: string;
		value: SyncCursor;
	};
	outbox: {
		key: string;
		value: PendingOperation;
	};
	rejected: {
		key: string;
		value: RejectedRecord;
	};
}

const databaseVersion = 2;

export class LocalState {
	readonly #database: IDBPDatabase<PersisterDatabase>;

	private constructor(database: IDBPDatabase<PersisterDatabase>) {
		this.#database = database;
	}

	static async open(
		databaseName: string,
		scopeKey: string,
		onBlocked?: (error: Error) => void,
	): Promise<LocalState> {
		const database = await openDB<PersisterDatabase>(
			`${databaseName}:${scopeKey}`,
			databaseVersion,
			{
				blocked() {
					onBlocked?.(
						new Error(
							`IndexedDB upgrade blocked for ${databaseName}:${scopeKey}; reload other tabs using this scope`,
						),
					);
				},
				upgrade(upgradeDatabase, oldVersion) {
					if (oldVersion < 1) {
						upgradeDatabase.createObjectStore('content');
						upgradeDatabase.createObjectStore('outbox');
						upgradeDatabase.createObjectStore('rejected');
					}
					if (oldVersion < 2) {
						upgradeDatabase.createObjectStore('cursors');
					}
				},
			},
		);

		return new LocalState(database);
	}

	close(): void {
		this.#database.close();
	}

	async getContent(): Promise<Content | undefined> {
		return this.#database.get('content', 'store');
	}

	async getCursor(cursorKey: string): Promise<SyncCursor | undefined> {
		return this.#database.get('cursors', cursorKey);
	}

	async getOperations(): Promise<PendingOperation[]> {
		return this.#database.getAll('outbox');
	}

	async getRejected(): Promise<RejectedRecord[]> {
		return this.#database.getAll('rejected');
	}

	async removeOperation(id: string): Promise<void> {
		await this.#database.delete('outbox', id);
	}

	async persist(content: Content, operations: readonly PendingOperation[]): Promise<void> {
		const transaction = this.#database.transaction(['content', 'outbox'], 'readwrite');
		await transaction.objectStore('content').put(content, 'store');

		for (const operation of operations) {
			await transaction.objectStore('outbox').put(operation, operation.id);
		}

		await transaction.done;
	}

	async replaceContent(content: Content, cursorKey: string, cursor?: SyncCursor): Promise<void> {
		const transaction = this.#database.transaction(['content', 'cursors'], 'readwrite');
		await transaction.objectStore('content').put(content, 'store');
		if (cursor) {
			await transaction.objectStore('cursors').put(cursor, cursorKey);
		}
		await transaction.done;
	}

	async reject(operation: PendingOperation, error: string): Promise<void> {
		const transaction = this.#database.transaction(['outbox', 'rejected'], 'readwrite');
		await transaction.objectStore('outbox').delete(operation.id);
		await transaction.objectStore('rejected').put({ ...operation, error }, operation.id);
		await transaction.done;
	}

	async retryRejected(): Promise<void> {
		const transaction = this.#database.transaction(['outbox', 'rejected'], 'readwrite');
		const rejected = await transaction.objectStore('rejected').getAll();

		for (const operation of rejected) {
			const { error: _error, ...pending } = operation;
			await transaction.objectStore('outbox').put(pending, pending.id);
			await transaction.objectStore('rejected').delete(operation.id);
		}

		await transaction.done;
	}

	async discardRejected(): Promise<void> {
		await this.#database.clear('rejected');
	}
}
