import { type ChildProcess, spawn } from 'node:child_process';
import { jest } from '@jest/globals';
import { type Browser, type BrowserContext, chromium } from 'playwright';

const appUrl = 'http://127.0.0.1:4174/tests/e2e/indexeddb-upgrade.html';
let server: ChildProcess | undefined;
let browser: Browser | undefined;
const promptTimeoutMs = 2_000;

const withPromptTimeout = async <Value>(
	promise: Promise<Value>,
	operation: string,
): Promise<Value> => {
	let timeout: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_resolve, reject) => {
				timeout = setTimeout(
					() => reject(new Error(`${operation} did not complete promptly`)),
					promptTimeoutMs,
				);
			}),
		]);
	} finally {
		clearTimeout(timeout);
	}
};

const waitForApp = async (): Promise<void> => {
	for (let attempt = 0; attempt < 50; attempt += 1) {
		try {
			const response = await fetch(appUrl);
			if (response.ok) {
				return;
			}
		} catch {}
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
	throw new Error('Vite did not start the IndexedDB browser test app');
};

describe('IndexedDB upgrade lifecycle', () => {
	jest.setTimeout(30_000);

	beforeAll(async () => {
		server = spawn(
			process.execPath,
			[
				'node_modules/vite/bin/vite.js',
				'--host',
				'127.0.0.1',
				'--port',
				'4174',
				'--strictPort',
			],
			{ cwd: process.cwd(), stdio: 'ignore' },
		);
		await waitForApp();
		browser = await chromium.launch({ headless: true });
	});

	afterAll(async () => {
		await browser?.close();
		server?.kill('SIGTERM');
	});

	it('reports a blocked v1 to v2 upgrade, resumes without data loss, and cooperates with v3', async () => {
		if (!browser) {
			throw new Error('Browser did not start');
		}
		const context: BrowserContext = await browser.newContext();
		const [olderPage, currentPage] = await Promise.all([context.newPage(), context.newPage()]);
		await Promise.all([olderPage.goto(appUrl), currentPage.goto(appUrl)]);

		const databaseName = `upgrade-${crypto.randomUUID()}`;
		const scopeKey = 'user';
		await olderPage.evaluate(
			({ name, scope }) => window.indexedDbUpgradeTest.holdVersionOne(name, scope),
			{ name: databaseName, scope: scopeKey },
		);
		await currentPage.evaluate(
			({ name, scope }) => window.indexedDbUpgradeTest.startPersister(name, scope),
			{ name: databaseName, scope: scopeKey },
		);

		await currentPage.waitForFunction(
			() => window.indexedDbUpgradeTest.getErrors().length === 1,
			undefined,
			{ timeout: promptTimeoutMs },
		);
		expect(
			await currentPage.evaluate(() => window.indexedDbUpgradeTest.getErrors()[0]),
		).toEqual({
			code: 'indexeddb-upgrade-blocked',
			currentVersion: 1,
			isConnectionClosedForUpgrade: false,
			isUpgradeBlocked: true,
			name: 'IndexedDbUpgradeBlockedError',
			requestedVersion: 2,
		});
		expect(
			await currentPage.evaluate(() => window.indexedDbUpgradeTest.hasPersisterResolved()),
		).toBe(false);

		await olderPage.evaluate(() => window.indexedDbUpgradeTest.closeHeldConnection());
		await expect(
			currentPage.evaluate(() => window.indexedDbUpgradeTest.awaitPersister()),
		).resolves.toEqual({
			networkRequestCount: 0,
			outbox: [
				{
					id: 'todos:pending',
					kind: 'upsert',
					payload: { id: 'pending', title: 'Pending before upgrade' },
					rowId: 'pending',
					tableId: 'todos',
				},
			],
			pendingCount: 1,
			rejected: [
				{
					error: 'Rejected before upgrade',
					id: 'todos:rejected',
					kind: 'upsert',
					payload: { id: 'rejected', title: 'Rejected before upgrade' },
					rowId: 'rejected',
					tableId: 'todos',
				},
			],
			rejectedCount: 1,
			row: { title: 'Cached before upgrade' },
		});

		await expect(
			withPromptTimeout(
				olderPage.evaluate(
					({ name, scope }) =>
						window.indexedDbUpgradeTest.requestFutureVersion(name, scope),
					{ name: databaseName, scope: scopeKey },
				),
				'Future standard database upgrade',
			),
		).resolves.toBe(3);
		await currentPage.waitForFunction(
			() => window.indexedDbUpgradeTest.getErrors().length === 2,
			undefined,
			{ timeout: promptTimeoutMs },
		);
		expect(
			await currentPage.evaluate(() => window.indexedDbUpgradeTest.getErrors()[1]),
		).toEqual({
			code: 'indexeddb-connection-closed-for-upgrade',
			currentVersion: 2,
			isConnectionClosedForUpgrade: true,
			isUpgradeBlocked: false,
			name: 'IndexedDbConnectionClosedForUpgradeError',
			requestedVersion: 3,
		});
		await expect(
			currentPage.evaluate(() => window.indexedDbUpgradeTest.getTerminalState()),
		).resolves.toEqual({
			isAutoSaving: false,
			listenerCode: 'indexeddb-connection-closed-for-upgrade',
			phase: 'error',
			saveCode: 'indexeddb-connection-closed-for-upgrade',
			saveRejectedWithStatusError: true,
			statusCode: 'indexeddb-connection-closed-for-upgrade',
			syncCode: 'indexeddb-connection-closed-for-upgrade',
			syncRejectedWithStatusError: true,
		});

		await currentPage.evaluate(() => window.indexedDbUpgradeTest.destroy());
		await context.close();
	});

	it('reports and resumes a blocked CRDT upgrade while preserving every local store', async () => {
		if (!browser) {
			throw new Error('Browser did not start');
		}
		const context: BrowserContext = await browser.newContext();
		const [olderPage, currentPage] = await Promise.all([context.newPage(), context.newPage()]);
		await Promise.all([olderPage.goto(appUrl), currentPage.goto(appUrl)]);

		const databaseName = `crdt-upgrade-${crypto.randomUUID()}`;
		const scopeKey = 'user';
		await olderPage.evaluate(
			({ name, scope }) => window.indexedDbUpgradeTest.holdVersionTwoCrdt(name, scope),
			{ name: databaseName, scope: scopeKey },
		);
		await currentPage.evaluate(
			({ name, scope }) => window.indexedDbUpgradeTest.startCrdtPersister(name, scope),
			{ name: databaseName, scope: scopeKey },
		);

		await currentPage.waitForFunction(
			() => window.indexedDbUpgradeTest.getErrors().length === 1,
			undefined,
			{ timeout: promptTimeoutMs },
		);
		expect(
			await currentPage.evaluate(() => window.indexedDbUpgradeTest.getErrors()[0]),
		).toEqual({
			code: 'indexeddb-upgrade-blocked',
			currentVersion: 2,
			isConnectionClosedForUpgrade: false,
			isUpgradeBlocked: true,
			name: 'IndexedDbUpgradeBlockedError',
			requestedVersion: 3,
		});
		expect(
			await currentPage.evaluate(() => window.indexedDbUpgradeTest.hasPersisterResolved()),
		).toBe(false);

		await olderPage.evaluate(() => window.indexedDbUpgradeTest.closeHeldConnection());
		await expect(
			currentPage.evaluate(() => window.indexedDbUpgradeTest.awaitCrdtPersister()),
		).resolves.toEqual({
			buffered: [
				{
					bufferedAt: 123,
					documentKey: 'documents\0doc-1',
					id: 'buffered-1',
					rowId: 'doc-1',
					tableId: 'documents',
					update: [2],
				},
			],
			outbox: [
				{
					documentKey: 'documents\0doc-1',
					id: 'outbox-1',
					rowId: 'doc-1',
					tableId: 'documents',
					update: [3],
				},
			],
			quarantined: [
				{
					documentKey: 'documents\0doc-1',
					error: 'Rejected before CRDT upgrade',
					failedUpdateId: 'rejected-1',
					rowId: 'doc-1',
					state: 'rejected',
					tableId: 'documents',
				},
			],
			rejected: [
				{
					documentKey: 'documents\0doc-1',
					error: 'Rejected before CRDT upgrade',
					id: 'rejected-1',
					rowId: 'doc-1',
					tableId: 'documents',
					update: [4],
				},
			],
			updates: [
				{
					documentKey: 'documents\0doc-1',
					id: 'update-1',
					rowId: 'doc-1',
					tableId: 'documents',
					update: [1],
				},
			],
		});

		await expect(
			withPromptTimeout(
				olderPage.evaluate(
					({ name, scope }) =>
						window.indexedDbUpgradeTest.requestFutureCrdtVersion(name, scope),
					{ name: databaseName, scope: scopeKey },
				),
				'Future CRDT database upgrade',
			),
		).resolves.toBe(4);
		await currentPage.waitForFunction(
			() => window.indexedDbUpgradeTest.getErrors().length === 2,
			undefined,
			{ timeout: promptTimeoutMs },
		);
		expect(
			await currentPage.evaluate(() => window.indexedDbUpgradeTest.getErrors()[1]),
		).toEqual({
			code: 'indexeddb-connection-closed-for-upgrade',
			currentVersion: 3,
			isConnectionClosedForUpgrade: true,
			isUpgradeBlocked: false,
			name: 'IndexedDbConnectionClosedForUpgradeError',
			requestedVersion: 4,
		});
		await expect(
			currentPage.evaluate(() => window.indexedDbUpgradeTest.getTerminalState()),
		).resolves.toEqual({
			isAutoSaving: false,
			listenerCode: 'indexeddb-connection-closed-for-upgrade',
			phase: 'error',
			saveCode: 'indexeddb-connection-closed-for-upgrade',
			saveRejectedWithStatusError: true,
			statusCode: 'indexeddb-connection-closed-for-upgrade',
			syncCode: 'indexeddb-connection-closed-for-upgrade',
			syncRejectedWithStatusError: true,
		});

		await currentPage.evaluate(() => window.indexedDbUpgradeTest.destroy());
		await context.close();
	});
});
