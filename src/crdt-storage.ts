import { openDB, type DBSchema, type IDBPDatabase } from 'idb';

export interface StoredCrdtUpdate {
	readonly documentKey: string;
	readonly id: string;
	readonly rowId: string;
	readonly tableId: string;
	readonly update: Uint8Array;
}

export interface BufferedCrdtUpdate extends StoredCrdtUpdate {
	readonly bufferedAt: number;
}

interface CrdtDatabase extends DBSchema {
	buffered: {
		key: string;
		value: BufferedCrdtUpdate;
		indexes: { documentKey: string };
	};
	updates: {
		key: string;
		value: StoredCrdtUpdate;
		indexes: { documentKey: string };
	};
	outbox: {
		key: string;
		value: StoredCrdtUpdate;
	};
	rejected: {
		key: string;
		value: StoredCrdtUpdate & { readonly error: string };
	};
}

export class CrdtLocalState {
	readonly #database: IDBPDatabase<CrdtDatabase>;

	private constructor(database: IDBPDatabase<CrdtDatabase>) {
		this.#database = database;
	}

	static async open(databaseName: string, scopeKey: string): Promise<CrdtLocalState> {
		const database = await openDB<CrdtDatabase>(`${databaseName}:${scopeKey}:yjs`, 2, {
			upgrade(upgradeDatabase, oldVersion) {
				if (oldVersion < 1) {
					const updates = upgradeDatabase.createObjectStore('updates');
					updates.createIndex('documentKey', 'documentKey');
					upgradeDatabase.createObjectStore('outbox');
					upgradeDatabase.createObjectStore('rejected');
				}
				if (oldVersion < 2) {
					const buffered = upgradeDatabase.createObjectStore('buffered');
					buffered.createIndex('documentKey', 'documentKey');
				}
			},
		});
		return new CrdtLocalState(database);
	}

	close(): void {
		this.#database.close();
	}

	async getDocumentUpdates(documentKey: string): Promise<StoredCrdtUpdate[]> {
		return this.#database.getAllFromIndex('updates', 'documentKey', documentKey);
	}

	async getBuffered(): Promise<BufferedCrdtUpdate[]> {
		return this.#database.getAll('buffered');
	}

	async getOutbox(): Promise<StoredCrdtUpdate[]> {
		return this.#database.getAll('outbox');
	}

	async getRejected(): Promise<Array<StoredCrdtUpdate & { readonly error: string }>> {
		return this.#database.getAll('rejected');
	}

	async persistLocalUpdate(update: BufferedCrdtUpdate): Promise<void> {
		const transaction = this.#database.transaction(['updates', 'buffered'], 'readwrite');
		await transaction.objectStore('updates').put(update, update.id);
		await transaction.objectStore('buffered').put(update, update.id);
		await transaction.done;
	}

	async promoteBuffered(sourceIds: readonly string[], merged: StoredCrdtUpdate): Promise<void> {
		const transaction = this.#database.transaction(
			['updates', 'buffered', 'outbox'],
			'readwrite',
		);
		for (const id of sourceIds) {
			await transaction.objectStore('updates').delete(id);
			await transaction.objectStore('buffered').delete(id);
		}
		await transaction.objectStore('updates').put(merged, merged.id);
		await transaction.objectStore('outbox').put(merged, merged.id);
		await transaction.done;
	}

	async hasUpdate(id: string): Promise<boolean> {
		return (await this.#database.getKey('updates', id)) !== undefined;
	}

	async persistRemoteUpdate(update: StoredCrdtUpdate): Promise<void> {
		await this.#database.put('updates', update, update.id);
	}

	async removeOutbox(id: string): Promise<void> {
		await this.#database.delete('outbox', id);
	}

	async reject(update: StoredCrdtUpdate, error: string): Promise<void> {
		const transaction = this.#database.transaction(['outbox', 'rejected'], 'readwrite');
		await transaction.objectStore('outbox').delete(update.id);
		await transaction.objectStore('rejected').put({ ...update, error }, update.id);
		await transaction.done;
	}

	async retryRejected(): Promise<void> {
		const transaction = this.#database.transaction(['outbox', 'rejected'], 'readwrite');
		for (const rejected of await transaction.objectStore('rejected').getAll()) {
			const { error: _error, ...update } = rejected;
			await transaction.objectStore('outbox').put(update, update.id);
			await transaction.objectStore('rejected').delete(update.id);
		}
		await transaction.done;
	}

	async discardRejected(): Promise<void> {
		await this.#database.clear('rejected');
	}
}
