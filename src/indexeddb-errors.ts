import type { DBSchema, OpenDBCallbacks } from 'idb';

export class IndexedDbUpgradeBlockedError extends Error {
	readonly code = 'indexeddb-upgrade-blocked' as const;

	constructor(
		readonly databaseName: string,
		readonly currentVersion: number,
		readonly requestedVersion: number | null,
	) {
		super(
			`IndexedDB upgrade blocked for ${databaseName}; close other tabs using this scope and initialization will resume automatically`,
		);
		this.name = 'IndexedDbUpgradeBlockedError';
	}
}

export class IndexedDbConnectionClosedForUpgradeError extends Error {
	readonly code = 'indexeddb-connection-closed-for-upgrade' as const;

	constructor(
		readonly databaseName: string,
		readonly currentVersion: number,
		readonly requestedVersion: number | null,
	) {
		super(
			`IndexedDB connection closed for an upgrade of ${databaseName}; reload before continuing`,
		);
		this.name = 'IndexedDbConnectionClosedForUpgradeError';
	}
}

export const isIndexedDbConnectionClosedException = (error: unknown): boolean =>
	typeof error === 'object' &&
	error !== null &&
	'name' in error &&
	error.name === 'InvalidStateError';

export const getIndexedDbLifecycleCallbacks = <Database extends DBSchema>(
	databaseName: string,
	onError?: (error: Error) => void,
	onConnectionClosedForUpgrade?: (error: IndexedDbConnectionClosedForUpgradeError) => void,
): Pick<OpenDBCallbacks<Database>, 'blocked' | 'blocking'> => ({
	blocked(currentVersion, requestedVersion) {
		onError?.(new IndexedDbUpgradeBlockedError(databaseName, currentVersion, requestedVersion));
	},
	blocking(currentVersion, requestedVersion, event) {
		(event.target as IDBDatabase).close();
		const error = new IndexedDbConnectionClosedForUpgradeError(
			databaseName,
			currentVersion,
			requestedVersion,
		);
		onConnectionClosedForUpgrade?.(error);
		onError?.(error);
	},
});
