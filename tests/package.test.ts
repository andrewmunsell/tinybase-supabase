import {
	createSupabasePersister,
	IndexedDbConnectionClosedForUpgradeError,
	IndexedDbUpgradeBlockedError,
} from '../src/index.js';

describe('package surface', () => {
	it('exports the persister factory', () => {
		expect(createSupabasePersister).toBeInstanceOf(Function);
	});

	it('exports discriminable IndexedDB lifecycle errors', () => {
		expect(new IndexedDbUpgradeBlockedError('app:user', 1, 2)).toMatchObject({
			code: 'indexeddb-upgrade-blocked',
			currentVersion: 1,
			name: 'IndexedDbUpgradeBlockedError',
			requestedVersion: 2,
		});
		expect(new IndexedDbConnectionClosedForUpgradeError('app:user', 2, 3)).toMatchObject({
			code: 'indexeddb-connection-closed-for-upgrade',
			currentVersion: 2,
			name: 'IndexedDbConnectionClosedForUpgradeError',
			requestedVersion: 3,
		});
	});
});
