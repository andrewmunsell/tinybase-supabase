import { spawn, type ChildProcess } from 'node:child_process';
import { jest } from '@jest/globals';
import { chromium, type Browser, type BrowserContext, type Page } from 'playwright';

const apiUrl = process.env.SUPABASE_TEST_URL;
const anonKey = process.env.SUPABASE_TEST_ANON_KEY;
const e2eDescribe = apiUrl && anonKey ? describe : describe.skip;

const appUrl = 'http://127.0.0.1:4173/tests/e2e/index.html';
let server: ChildProcess | undefined;
let browser: Browser | undefined;

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
	throw new Error('Vite did not start the browser test app');
};

const createPage = async (): Promise<{ context: BrowserContext; page: Page }> => {
	if (!browser) {
		throw new Error('Browser did not start');
	}
	const context = await browser.newContext();
	await context.addInitScript(
		(environment) => {
			window.__TINYBASE_SUPABASE_ENV__ = environment;
		},
		{ anonKey: anonKey as string, apiUrl: apiUrl as string },
	);
	const page = await context.newPage();
	await page.goto(appUrl);
	return { context, page };
};

const waitForTodoTitle = async (page: Page, id: string, title: string): Promise<void> => {
	for (let attempt = 0; attempt < 50; attempt += 1) {
		const currentTitle = await page.evaluate(
			(todoId) => window.tinybaseSupabaseTest.getTodo(todoId).title,
			id,
		);
		if (currentTitle === title) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
	throw new Error(`Timed out waiting for todo ${id} to have title ${title}`);
};

const waitForCrdtConvergence = async (first: Page, second: Page, id: string): Promise<string> => {
	for (let attempt = 0; attempt < 50; attempt += 1) {
		const [firstText, secondText] = await Promise.all([
			first.evaluate((documentId) => window.tinybaseSupabaseTest.getCrdtText(documentId), id),
			second.evaluate(
				(documentId) => window.tinybaseSupabaseTest.getCrdtText(documentId),
				id,
			),
		]);
		if (
			firstText === secondText &&
			firstText.includes('first') &&
			firstText.includes('second')
		) {
			return firstText;
		}
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
	throw new Error(`Timed out waiting for CRDT document ${id} to converge`);
};

const waitForCrdtText = async (page: Page, id: string, text: string): Promise<void> => {
	for (let attempt = 0; attempt < 50; attempt += 1) {
		if (
			(await page.evaluate(
				(documentId) => window.tinybaseSupabaseTest.getCrdtText(documentId),
				id,
			)) === text
		) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
	throw new Error(`Timed out waiting for CRDT document ${id} to contain ${text}`);
};

e2eDescribe('browser end-to-end synchronization', () => {
	jest.setTimeout(45_000);

	beforeAll(async () => {
		server = spawn(
			'pnpm',
			['exec', 'vite', '--host', '127.0.0.1', '--port', '4173', '--strictPort'],
			{
				cwd: process.cwd(),
				stdio: 'ignore',
			},
		);
		await waitForApp();
		browser = await chromium.launch({ headless: true });
	});

	afterAll(async () => {
		await browser?.close();
		server?.kill('SIGTERM');
	});

	it('survives offline browser reload, syncs on reconnect, and receives realtime updates', async () => {
		const email = `browser-${crypto.randomUUID()}@example.test`;
		const password = 'correct-horse-battery-staple';
		const registration = await fetch(`${apiUrl}/auth/v1/signup`, {
			body: JSON.stringify({ email, password }),
			headers: { apikey: anonKey as string, 'content-type': 'application/json' },
			method: 'POST',
		});
		expect(registration.ok).toBe(true);

		const { context, page } = await createPage();
		const userId = await page.evaluate(
			({ nextEmail, nextPassword }) =>
				window.tinybaseSupabaseTest.boot(nextEmail, nextPassword),
			{ nextEmail: email, nextPassword: password },
		);
		const offlineId = `offline-${crypto.randomUUID()}`;
		await context.setOffline(true);
		await page.evaluate(
			({ id, ownerId }) => window.tinybaseSupabaseTest.writeTodo(id, ownerId),
			{ id: offlineId, ownerId: userId },
		);
		expect(
			await page.evaluate((id) => window.tinybaseSupabaseTest.getTodo(id), offlineId),
		).toMatchObject({
			title: 'Offline todo',
		});

		await context.setOffline(false);
		await page.reload();
		await page.evaluate(
			({ nextEmail, nextPassword }) =>
				window.tinybaseSupabaseTest.boot(nextEmail, nextPassword),
			{ nextEmail: email, nextPassword: password },
		);
		expect(
			await page.evaluate((id) => window.tinybaseSupabaseTest.getTodo(id), offlineId),
		).toMatchObject({
			title: 'Offline todo',
		});

		await page.evaluate(() => window.tinybaseSupabaseTest.sync());
		expect(
			await page.evaluate((id) => window.tinybaseSupabaseTest.getRemoteTodo(id), offlineId),
		).toMatchObject({
			title: 'Offline todo',
		});

		const realtimeId = `realtime-${crypto.randomUUID()}`;
		await page.evaluate(
			({ id, ownerId }) => window.tinybaseSupabaseTest.upsertRemoteTodo(id, ownerId),
			{ id: realtimeId, ownerId: userId },
		);
		await waitForTodoTitle(page, realtimeId, 'Realtime todo');

		const conflictId = `conflict-${crypto.randomUUID()}`;
		const second = await createPage();
		await second.page.evaluate(
			({ nextEmail, nextPassword }) =>
				window.tinybaseSupabaseTest.boot(nextEmail, nextPassword),
			{ nextEmail: email, nextPassword: password },
		);
		await context.setOffline(true);
		await page.evaluate(
			({ id, ownerId }) => window.tinybaseSupabaseTest.writeTodo(id, ownerId),
			{ id: conflictId, ownerId: userId },
		);
		await second.page.evaluate(
			({ id, ownerId }) => window.tinybaseSupabaseTest.upsertRemoteTodo(id, ownerId),
			{ id: conflictId, ownerId: userId },
		);
		await context.setOffline(false);
		await page.evaluate(() => window.tinybaseSupabaseTest.sync());
		expect(
			await page.evaluate((id) => window.tinybaseSupabaseTest.getRemoteTodo(id), conflictId),
		).toMatchObject({
			title: 'Offline todo',
		});
		await second.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		expect(
			await second.page.evaluate((id) => window.tinybaseSupabaseTest.getTodo(id), conflictId),
		).toMatchObject({
			title: 'Offline todo',
		});

		await page.evaluate(() => window.tinybaseSupabaseTest.destroy());
		await context.close();
		await second.page.evaluate(() => window.tinybaseSupabaseTest.destroy());
		await second.context.close();
	});

	it('rehydrates CRDT text offline and converges concurrent browser edits', async () => {
		const email = `browser-crdt-${crypto.randomUUID()}@example.test`;
		const password = 'correct-horse-battery-staple';
		const registration = await fetch(`${apiUrl}/auth/v1/signup`, {
			body: JSON.stringify({ email, password }),
			headers: { apikey: anonKey as string, 'content-type': 'application/json' },
			method: 'POST',
		});
		expect(registration.ok).toBe(true);

		const first = await createPage();
		const userId = await first.page.evaluate(
			({ nextEmail, nextPassword }) =>
				window.tinybaseSupabaseTest.boot(nextEmail, nextPassword),
			{ nextEmail: email, nextPassword: password },
		);
		const documentId = `browser-document-${crypto.randomUUID()}`;
		await first.page.evaluate(
			({ id, ownerId }) => window.tinybaseSupabaseTest.createCrdtDocument(id, ownerId),
			{ id: documentId, ownerId: userId },
		);
		await first.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		await first.page.evaluate(
			(id) => window.tinybaseSupabaseTest.editCrdtText(id, 'initial'),
			documentId,
		);
		await first.page.evaluate(() => window.tinybaseSupabaseTest.sync());

		await first.page.reload();
		await first.page.evaluate(
			({ nextEmail, nextPassword }) =>
				window.tinybaseSupabaseTest.boot(nextEmail, nextPassword),
			{ nextEmail: email, nextPassword: password },
		);
		await first.context.setOffline(true);
		await first.page.evaluate(() => window.tinybaseSupabaseTest.restartPersister());
		await first.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		expect(
			await first.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getCrdtText(id),
				documentId,
			),
		).toBe('initial');
		await first.page.evaluate(
			(id) => window.tinybaseSupabaseTest.editCrdtText(id, ' offline'),
			documentId,
		);
		await first.context.setOffline(false);
		await first.page.evaluate(() => window.tinybaseSupabaseTest.sync());

		const second = await createPage();
		await second.page.evaluate(
			({ nextEmail, nextPassword }) =>
				window.tinybaseSupabaseTest.boot(nextEmail, nextPassword),
			{ nextEmail: email, nextPassword: password },
		);
		await second.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		expect(
			await second.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getCrdtText(id),
				documentId,
			),
		).toBe('initial offline');

		await Promise.all([first.context.setOffline(true), second.context.setOffline(true)]);
		await Promise.all([
			first.page.evaluate(
				(id) => window.tinybaseSupabaseTest.editCrdtText(id, ' first'),
				documentId,
			),
			second.page.evaluate(
				(id) => window.tinybaseSupabaseTest.editCrdtText(id, ' second'),
				documentId,
			),
		]);
		await Promise.all([first.context.setOffline(false), second.context.setOffline(false)]);
		await first.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		await second.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		await first.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		const converged = await waitForCrdtConvergence(first.page, second.page, documentId);
		expect(converged).toContain('initial offline');

		await first.page.evaluate(() => window.tinybaseSupabaseTest.destroy());
		await first.context.close();
		await second.page.evaluate(() => window.tinybaseSupabaseTest.destroy());
		await second.context.close();
	});

	it('hydrates and follows CRDT content without uploading from a read-only browser', async () => {
		const ownerEmail = `browser-crdt-owner-${crypto.randomUUID()}@example.test`;
		const readerEmail = `browser-crdt-reader-${crypto.randomUUID()}@example.test`;
		const password = 'correct-horse-battery-staple';
		for (const email of [ownerEmail, readerEmail]) {
			const registration = await fetch(`${apiUrl}/auth/v1/signup`, {
				body: JSON.stringify({ email, password }),
				headers: { apikey: anonKey as string, 'content-type': 'application/json' },
				method: 'POST',
			});
			expect(registration.ok).toBe(true);
		}

		const owner = await createPage();
		const ownerId = await owner.page.evaluate(
			({ email, nextPassword }) => window.tinybaseSupabaseTest.boot(email, nextPassword),
			{ email: ownerEmail, nextPassword: password },
		);
		const documentId = `browser-read-only-${crypto.randomUUID()}`;
		await owner.page.evaluate(
			({ id, nextOwnerId }) =>
				window.tinybaseSupabaseTest.createCrdtDocument(id, nextOwnerId),
			{ id: documentId, nextOwnerId: ownerId },
		);
		await owner.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		await owner.page.evaluate(
			(id) => window.tinybaseSupabaseTest.editCrdtText(id, 'Initial'),
			documentId,
		);
		await owner.page.evaluate(() => window.tinybaseSupabaseTest.sync());

		const reader = await createPage();
		const readerId = await reader.page.evaluate(
			({ email, nextPassword }) =>
				window.tinybaseSupabaseTest.boot(email, nextPassword, true),
			{ email: readerEmail, nextPassword: password },
		);
		await owner.page.evaluate(
			({ id, nextReaderId }) => window.tinybaseSupabaseTest.addCrdtReader(id, nextReaderId),
			{ id: documentId, nextReaderId: readerId },
		);
		await reader.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		await reader.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		expect(
			await reader.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getCrdtText(id),
				documentId,
			),
		).toBe('Initial');

		await owner.page.evaluate(
			(id) => window.tinybaseSupabaseTest.editCrdtText(id, ' remote'),
			documentId,
		);
		await owner.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		await waitForCrdtText(reader.page, documentId, 'Initial remote');
		const remoteUpdateCount = await reader.page.evaluate(
			(id) => window.tinybaseSupabaseTest.getRemoteCrdtUpdateCount(id),
			documentId,
		);

		await expect(
			reader.page.evaluate(
				(id) => window.tinybaseSupabaseTest.editCrdtText(id, ' local'),
				documentId,
			),
		).rejects.toThrow('is read-only');
		await reader.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		expect(
			await reader.page.evaluate(() => window.tinybaseSupabaseTest.getCrdtSyncStatus()),
		).toEqual({ pendingCount: 0, rejectedCount: 0 });
		expect(
			await reader.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getRemoteCrdtUpdateCount(id),
				documentId,
			),
		).toBe(remoteUpdateCount);

		await reader.page.evaluate(() => window.tinybaseSupabaseTest.restartPersister());
		await reader.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		expect(
			await reader.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getCrdtText(id),
				documentId,
			),
		).toBe('Initial remote');

		await owner.page.evaluate(() => window.tinybaseSupabaseTest.destroy());
		await owner.context.close();
		await reader.page.evaluate(() => window.tinybaseSupabaseTest.destroy());
		await reader.context.close();
	});

	it('quarantines rejected CRDT history across restart and discards it authoritatively', async () => {
		const ownerEmail = `browser-quarantine-owner-${crypto.randomUUID()}@example.test`;
		const readerEmail = `browser-quarantine-reader-${crypto.randomUUID()}@example.test`;
		const password = 'correct-horse-battery-staple';
		for (const email of [ownerEmail, readerEmail]) {
			const registration = await fetch(`${apiUrl}/auth/v1/signup`, {
				body: JSON.stringify({ email, password }),
				headers: { apikey: anonKey as string, 'content-type': 'application/json' },
				method: 'POST',
			});
			expect(registration.ok).toBe(true);
		}

		const owner = await createPage();
		const ownerId = await owner.page.evaluate(
			({ email, nextPassword }) => window.tinybaseSupabaseTest.boot(email, nextPassword),
			{ email: ownerEmail, nextPassword: password },
		);
		const documentId = `browser-quarantine-${crypto.randomUUID()}`;
		await owner.page.evaluate(
			({ id, nextOwnerId }) =>
				window.tinybaseSupabaseTest.createCrdtDocument(id, nextOwnerId),
			{ id: documentId, nextOwnerId: ownerId },
		);
		await owner.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		await owner.page.evaluate(
			(id) => window.tinybaseSupabaseTest.editCrdtText(id, 'Authoritative'),
			documentId,
		);
		await owner.page.evaluate(() => window.tinybaseSupabaseTest.sync());

		const reader = await createPage();
		const readerId = await reader.page.evaluate(
			({ email, nextPassword }) => window.tinybaseSupabaseTest.boot(email, nextPassword),
			{ email: readerEmail, nextPassword: password },
		);
		await owner.page.evaluate(
			({ id, nextReaderId }) => window.tinybaseSupabaseTest.addCrdtReader(id, nextReaderId),
			{ id: documentId, nextReaderId: readerId },
		);
		await reader.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		await reader.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		expect(
			await reader.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getCrdtText(id),
				documentId,
			),
		).toBe('Authoritative');

		await reader.page.evaluate(
			(id) => window.tinybaseSupabaseTest.editCrdtText(id, ' rejected'),
			documentId,
		);
		await reader.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		expect(
			await reader.page.evaluate(() => window.tinybaseSupabaseTest.getCrdtSyncStatus()),
		).toEqual({ pendingCount: 0, rejectedCount: 1 });
		await reader.page.evaluate(
			(id) => window.tinybaseSupabaseTest.editCrdtText(id, ' successor'),
			documentId,
		);
		await reader.page.evaluate(() => window.tinybaseSupabaseTest.sync());
		expect(
			await reader.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getRemoteCrdtUpdateCount(id),
				documentId,
			),
		).toBe(1);

		await reader.page.evaluate(() => window.tinybaseSupabaseTest.restartPersister());
		await reader.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		expect(
			await reader.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getCrdtText(id),
				documentId,
			),
		).toBe('Authoritative rejected successor');

		await reader.page.evaluate(() => window.tinybaseSupabaseTest.discardRejected());
		await reader.page.evaluate(
			(id) => window.tinybaseSupabaseTest.openCrdtDocument(id),
			documentId,
		);
		expect(
			await reader.page.evaluate(
				(id) => window.tinybaseSupabaseTest.getCrdtText(id),
				documentId,
			),
		).toBe('Authoritative');
		expect(
			await reader.page.evaluate(() => window.tinybaseSupabaseTest.getCrdtSyncStatus()),
		).toEqual({ pendingCount: 0, rejectedCount: 0 });

		await owner.page.evaluate(() => window.tinybaseSupabaseTest.destroy());
		await owner.context.close();
		await reader.page.evaluate(() => window.tinybaseSupabaseTest.destroy());
		await reader.context.close();
	});
});
