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

export interface QuarantinedCrdtDocument {
	readonly documentKey: string;
	readonly error: string;
	readonly failedUpdateId: string;
	readonly rowId: string;
	readonly state: 'rejected' | 'retrying';
	readonly tableId: string;
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
		indexes: { documentKey: string };
	};
	quarantined: {
		key: string;
		value: QuarantinedCrdtDocument;
	};
	rejected: {
		key: string;
		value: StoredCrdtUpdate & { readonly error: string };
		indexes: { documentKey: string };
	};
}

export interface DiscardedCrdtState {
	readonly pendingCount: number;
	readonly rejectedCount: number;
}

export class CrdtLocalState {
	readonly #database: IDBPDatabase<CrdtDatabase>;

	private constructor(database: IDBPDatabase<CrdtDatabase>) {
		this.#database = database;
	}

	static async open(databaseName: string, scopeKey: string): Promise<CrdtLocalState> {
		const database = await openDB<CrdtDatabase>(`${databaseName}:${scopeKey}:yjs`, 3, {
			upgrade(upgradeDatabase, oldVersion, _newVersion, upgradeTransaction) {
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
				if (oldVersion < 3) {
					upgradeTransaction
						.objectStore('outbox')
						.createIndex('documentKey', 'documentKey');
					upgradeTransaction
						.objectStore('rejected')
						.createIndex('documentKey', 'documentKey');
					upgradeDatabase.createObjectStore('quarantined');
				}
			},
		});
		const state = new CrdtLocalState(database);
		await state.#backfillQuarantinedDocuments();
		return state;
	}

	async #backfillQuarantinedDocuments(): Promise<void> {
		const transaction = this.#database.transaction(['rejected', 'quarantined'], 'readwrite');
		for (const rejected of await transaction.objectStore('rejected').getAll()) {
			if (
				(await transaction.objectStore('quarantined').get(rejected.documentKey)) !==
				undefined
			) {
				continue;
			}
			await transaction.objectStore('quarantined').put(
				{
					documentKey: rejected.documentKey,
					error: rejected.error,
					failedUpdateId: rejected.id,
					rowId: rejected.rowId,
					state: 'rejected',
					tableId: rejected.tableId,
				},
				rejected.documentKey,
			);
		}
		await transaction.done;
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

	async getQuarantined(): Promise<QuarantinedCrdtDocument[]> {
		return this.#database.getAll('quarantined');
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

	async completeOutbox(id: string, documentKey: string): Promise<boolean> {
		const transaction = this.#database.transaction(
			['buffered', 'outbox', 'rejected', 'quarantined'],
			'readwrite',
		);
		await transaction.objectStore('outbox').delete(id);
		const quarantine = await transaction.objectStore('quarantined').get(documentKey);
		let clearedQuarantine = false;
		if (
			quarantine?.state === 'retrying' &&
			(await transaction.objectStore('buffered').index('documentKey').count(documentKey)) ===
				0 &&
			(await transaction.objectStore('outbox').index('documentKey').count(documentKey)) ===
				0 &&
			(await transaction.objectStore('rejected').index('documentKey').count(documentKey)) ===
				0
		) {
			await transaction.objectStore('quarantined').delete(documentKey);
			clearedQuarantine = true;
		}
		await transaction.done;
		return clearedQuarantine;
	}

	async reject(update: StoredCrdtUpdate, error: string): Promise<void> {
		const transaction = this.#database.transaction(
			['outbox', 'rejected', 'quarantined'],
			'readwrite',
		);
		await transaction.objectStore('outbox').delete(update.id);
		await transaction.objectStore('rejected').put({ ...update, error }, update.id);
		await transaction.objectStore('quarantined').put(
			{
				documentKey: update.documentKey,
				error,
				failedUpdateId: update.id,
				rowId: update.rowId,
				state: 'rejected',
				tableId: update.tableId,
			},
			update.documentKey,
		);
		await transaction.done;
	}

	async retryRejected(): Promise<number> {
		const transaction = this.#database.transaction(
			['outbox', 'rejected', 'quarantined'],
			'readwrite',
		);
		let retriedCount = 0;
		for (const rejected of await transaction.objectStore('rejected').getAll()) {
			const { error, ...update } = rejected;
			await transaction.objectStore('outbox').put(update, update.id);
			await transaction.objectStore('rejected').delete(update.id);
			await transaction.objectStore('quarantined').put(
				{
					documentKey: update.documentKey,
					error,
					failedUpdateId: update.id,
					rowId: update.rowId,
					state: 'retrying',
					tableId: update.tableId,
				},
				update.documentKey,
			);
			retriedCount += 1;
		}
		await transaction.done;
		return retriedCount;
	}

	async replaceOutboxUpdates(
		sourceIds: readonly string[],
		merged: StoredCrdtUpdate,
	): Promise<void> {
		const transaction = this.#database.transaction(['updates', 'outbox'], 'readwrite');
		for (const id of sourceIds) {
			await transaction.objectStore('updates').delete(id);
			await transaction.objectStore('outbox').delete(id);
		}
		await transaction.objectStore('updates').put(merged, merged.id);
		await transaction.objectStore('outbox').put(merged, merged.id);
		await transaction.done;
	}

	async discardDocuments(documentKeys: readonly string[]): Promise<DiscardedCrdtState> {
		const transaction = this.#database.transaction(
			['updates', 'buffered', 'outbox', 'rejected', 'quarantined'],
			'readwrite',
		);
		let pendingCount = 0;
		let rejectedCount = 0;
		for (const documentKey of new Set(documentKeys)) {
			const buffered = await transaction
				.objectStore('buffered')
				.index('documentKey')
				.getAll(documentKey);
			const outbox = await transaction
				.objectStore('outbox')
				.index('documentKey')
				.getAll(documentKey);
			const rejected = await transaction
				.objectStore('rejected')
				.index('documentKey')
				.getAll(documentKey);
			for (const update of [...buffered, ...outbox, ...rejected]) {
				await transaction.objectStore('updates').delete(update.id);
				await transaction.objectStore('buffered').delete(update.id);
				await transaction.objectStore('outbox').delete(update.id);
				await transaction.objectStore('rejected').delete(update.id);
			}
			await transaction.objectStore('quarantined').delete(documentKey);
			pendingCount += buffered.length + outbox.length;
			rejectedCount += rejected.length;
		}
		await transaction.done;
		return { pendingCount, rejectedCount };
	}

	async discardTableLocalState(tableIds: ReadonlySet<string>): Promise<DiscardedCrdtState> {
		const [buffered, outbox, rejected, quarantined] = await Promise.all([
			this.getBuffered(),
			this.getOutbox(),
			this.getRejected(),
			this.getQuarantined(),
		]);
		return this.discardDocuments(
			[...buffered, ...outbox, ...rejected, ...quarantined]
				.filter(({ tableId }) => tableIds.has(tableId))
				.map(({ documentKey }) => documentKey),
		);
	}
}
