import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { createStore, type Store } from 'tinybase';
import { createSupabasePersister, type SupabasePersister } from '../../src/index.js';

interface BrowserEnvironment {
	readonly anonKey: string;
	readonly apiUrl: string;
}

interface BrowserHarness {
	boot(email: string, password: string): Promise<string>;
	destroy(): Promise<void>;
	getTodo(id: string): Record<string, unknown>;
	getRemoteTodo(id: string): Promise<Record<string, unknown> | null>;
	sync(): Promise<void>;
	upsertRemoteTodo(id: string, ownerId: string): Promise<void>;
	writeTodo(id: string, ownerId: string): Promise<void>;
}

declare global {
	interface Window {
		__TINYBASE_SUPABASE_ENV__?: BrowserEnvironment;
		tinybaseSupabaseTest: BrowserHarness;
	}
}

let client: SupabaseClient;
let persister: SupabasePersister | undefined;
let store: Store | undefined;

const getEnvironment = (): BrowserEnvironment => {
	if (!window.__TINYBASE_SUPABASE_ENV__) {
		throw new Error('Missing browser Supabase test environment');
	}
	return window.__TINYBASE_SUPABASE_ENV__;
};

window.tinybaseSupabaseTest = {
	async boot(email, password) {
		const environment = getEnvironment();
		client = createClient(environment.apiUrl, environment.anonKey, {
			auth: { autoRefreshToken: false, persistSession: true },
		});
		const signedIn = await client.auth.signInWithPassword({ email, password });
		if (signedIn.error || !signedIn.data.user) {
			throw signedIn.error ?? new Error('Could not sign in test user');
		}

		store = createStore();
		persister = await createSupabasePersister(store, {
			databaseName: 'tinybase-supabase-e2e',
			pollIntervalMs: 0,
			scopeKey: signedIn.data.user.id,
			supabase: client,
			tables: {
				projects: { table: 'projects' },
				todos: { dependsOn: ['projects'], realtime: { debounceMs: 50 }, table: 'todos' },
			},
		});
		await persister.startAutoPersisting();
		return signedIn.data.user.id;
	},
	async destroy() {
		await persister?.destroy();
		await client?.auth.signOut();
	},
	getTodo(id) {
		return store?.getRow('todos', id) ?? {};
	},
	async getRemoteTodo(id) {
		const response = await client.from('todos').select('*').eq('id', id).maybeSingle();
		if (response.error) {
			throw response.error;
		}
		return response.data;
	},
	async sync() {
		await persister?.syncNow();
	},
	async upsertRemoteTodo(id, ownerId) {
		const projectId = `${id}-project`;
		const project = await client.from('projects').upsert(
			{
				active: true,
				id: projectId,
				owner_id: ownerId,
				settings: {},
				title: 'Remote project',
			},
			{ onConflict: 'id' },
		);
		if (project.error) {
			throw project.error;
		}
		const todo = await client.from('todos').upsert(
			{
				completed: true,
				id,
				metadata: { source: 'realtime' },
				owner_id: ownerId,
				priority: 3,
				project_id: projectId,
				tags: ['realtime'],
				title: 'Realtime todo',
			},
			{ onConflict: 'id' },
		);
		if (todo.error) {
			throw todo.error;
		}
	},
	async writeTodo(id, ownerId) {
		if (!store || !persister) {
			throw new Error('Harness is not started');
		}
		const projectId = `${id}-project`;
		store.transaction(() => {
			store?.setRow('projects', projectId, {
				active: true,
				owner_id: ownerId,
				settings: { source: 'browser' },
				title: 'Offline project',
			});
			store?.setRow('todos', id, {
				completed: false,
				metadata: { source: 'browser' },
				owner_id: ownerId,
				priority: 1,
				project_id: projectId,
				tags: ['offline'],
				title: 'Offline todo',
			});
		});
		await persister.save();
	},
};
