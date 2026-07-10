import 'fake-indexeddb/auto';
import { createClient } from '@supabase/supabase-js';
import { createStore } from 'tinybase';
import { createSupabasePersister } from '../../src/index.js';

const apiUrl = process.env.SUPABASE_TEST_URL;
const anonKey = process.env.SUPABASE_TEST_ANON_KEY;
const integrationDescribe = apiUrl && anonKey ? describe : describe.skip;

const createAuthenticatedClient = async () => {
	const client = createClient(apiUrl as string, anonKey as string, {
		auth: { autoRefreshToken: false, persistSession: false },
	});
	const email = `tinybase-${crypto.randomUUID()}@example.test`;
	const password = 'correct-horse-battery-staple';
	const { data, error } = await client.auth.signUp({ email, password });
	if (error || !data.user || !data.session) {
		throw error ?? new Error('Supabase did not create a user session');
	}
	return { client, userId: data.user.id };
};

integrationDescribe('local Supabase integration', () => {
	it('persists comprehensive todo rows, local-only values, dependencies, and non-realtime tables', async () => {
		const { client, userId } = await createAuthenticatedClient();
		const suffix = crypto.randomUUID();
		const projectId = `project-${suffix}`;
		const todoId = `todo-${suffix}`;
		const auditLogId = `log-${suffix}`;
		const store = createStore().setValue('view', 'list');
		const persister = await createSupabasePersister(store, {
			databaseName: `integration-${crypto.randomUUID()}`,
			pageSize: 2,
			pollIntervalMs: 0,
			scopeKey: userId,
			supabase: client,
			tables: {
				audit_logs: { table: 'audit_logs' },
				projects: { table: 'projects' },
				todos: { dependsOn: ['projects'], realtime: true, table: 'todos' },
			},
		});
		await persister.startAutoPersisting();

		store.transaction(() => {
			store.setRow('projects', projectId, {
				active: true,
				owner_id: userId,
				settings: { color: 'green', nested: { enabled: true } },
				title: 'Integration project',
			});
			store.setRow('todos', todoId, {
				attachment_path: `attachments/${todoId}.txt`,
				completed: false,
				due_at: '2026-07-10T12:00:00.000Z',
				metadata: { estimate: 3, labels: ['sync', 'offline'] },
				owner_id: userId,
				priority: 2,
				project_id: projectId,
				tags: ['sync', 'offline'],
				title: 'Persist every column kind',
			});
			store.setRow('audit_logs', auditLogId, {
				action: 'created',
				owner_id: userId,
				payload: { source: 'integration' },
			});
		});
		await persister.save();
		await persister.syncNow();
		await expect(persister.getRejectedOperations()).resolves.toEqual([]);

		const { data: todo, error } = await client
			.from('todos')
			.select('*')
			.eq('id', todoId)
			.single();
		expect(error).toBeNull();
		expect(todo).toMatchObject({
			attachment_path: `attachments/${todoId}.txt`,
			completed: false,
			metadata: { estimate: 3, labels: ['sync', 'offline'] },
			priority: 2,
			tags: ['sync', 'offline'],
		});
		expect(store.getValue('view')).toBe('list');

		await persister.destroy();
	});

	it('enforces RLS between authenticated users', async () => {
		const first = await createAuthenticatedClient();
		const second = await createAuthenticatedClient();
		const suffix = crypto.randomUUID();
		const projectId = `private-project-${suffix}`;
		const todoId = `private-${suffix}`;
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			databaseName: `rls-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: first.userId,
			supabase: first.client,
			tables: {
				projects: { table: 'projects' },
				todos: { dependsOn: ['projects'], table: 'todos' },
			},
		});
		await persister.startAutoPersisting();

		store.transaction(() => {
			store.setRow('projects', projectId, {
				active: true,
				owner_id: first.userId,
				settings: {},
				title: 'Private project',
			});
			store.setRow('todos', todoId, {
				completed: false,
				owner_id: first.userId,
				priority: 0,
				project_id: projectId,
				title: 'Private',
			});
		});
		await persister.save();
		await persister.syncNow();

		const { data, error } = await second.client.from('todos').select('*').eq('id', todoId);
		expect(error).toBeNull();
		expect(data).toEqual([]);

		const secondStore = createStore();
		const secondPersister = await createSupabasePersister(secondStore, {
			databaseName: `second-user-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: second.userId,
			supabase: second.client,
			tables: { todos: { table: 'todos' } },
		});
		await secondPersister.startAutoPersisting();
		expect(secondStore.getRow('todos', todoId)).toEqual({});
		await secondPersister.destroy();

		await persister.destroy();
	});

	it('retains an optimistic RLS-rejected write until the application discards it', async () => {
		const authorized = await createAuthenticatedClient();
		const other = await createAuthenticatedClient();
		const store = createStore();
		const invalidId = `forbidden-${crypto.randomUUID()}`;
		const persister = await createSupabasePersister(store, {
			databaseName: `rejected-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: authorized.userId,
			supabase: authorized.client,
			tables: { todos: { table: 'todos' } },
		});
		await persister.startAutoPersisting();

		store.setRow('todos', invalidId, {
			completed: false,
			owner_id: other.userId,
			priority: 0,
			project_id: 'not-readable',
			title: 'RLS should reject this',
		});
		await persister.save();
		await persister.syncNow();

		await expect(persister.getRejectedOperations()).resolves.toEqual([
			expect.objectContaining({ rowId: invalidId, tableId: 'todos' }),
		]);
		expect(persister.getSyncStatus().rejectedCount).toBe(1);
		expect(store.getRow('todos', invalidId)).toMatchObject({ title: 'RLS should reject this' });

		await persister.discardRejected();
		await persister.syncNow();
		expect(store.getRow('todos', invalidId)).toMatchObject({ title: 'RLS should reject this' });

		await persister.destroy();
	});

	it('pulls publicly readable tables with and without RLS in read-only mode', async () => {
		const client = createClient(apiUrl as string, anonKey as string, {
			auth: { autoRefreshToken: false, persistSession: false },
		});
		const store = createStore();
		const persister = await createSupabasePersister(store, {
			databaseName: `public-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: 'anonymous-public-data',
			supabase: client,
			tables: {
				public_templates: { mode: 'read-only', table: 'public_templates' },
				shared_templates: { mode: 'read-only', table: 'shared_templates' },
			},
		});
		await persister.startAutoPersisting();

		expect(store.getRow('public_templates', 'public-template')).toMatchObject({
			metadata: { source: 'no-rls' },
			title: 'Public template',
		});
		expect(store.getRow('shared_templates', 'shared-template')).toMatchObject({
			metadata: { source: 'rls' },
			title: 'Shared template',
		});

		store.setRow('public_templates', 'local-only', { title: 'Do not upload' });
		await persister.save();
		await persister.syncNow();
		const { data } = await client.from('public_templates').select('*').eq('id', 'local-only');
		expect(data).toEqual([]);

		await persister.destroy();
	});
});
