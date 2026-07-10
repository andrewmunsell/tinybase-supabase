import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { createStore, type Store } from 'tinybase';
import {
	createSupabasePersister,
	type CrdtRowHandle,
	type SupabasePersister,
} from '../../src/index.js';

interface BrowserEnvironment {
	readonly anonKey: string;
	readonly apiUrl: string;
}

interface BrowserHarness {
	boot(email: string, password: string): Promise<string>;
	createCrdtDocument(id: string, ownerId: string): Promise<void>;
	destroy(): Promise<void>;
	editCrdtText(id: string, text: string): Promise<void>;
	getCrdtText(id: string): string;
	getTodo(id: string): Record<string, unknown>;
	getRemoteTodo(id: string): Promise<Record<string, unknown> | null>;
	openCrdtDocument(id: string): Promise<void>;
	restartPersister(): Promise<void>;
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
const crdtRows = new Map<string, CrdtRowHandle>();
let persister: SupabasePersister | undefined;
let store: Store | undefined;

const getEnvironment = (): BrowserEnvironment => {
	if (!window.__TINYBASE_SUPABASE_ENV__) {
		throw new Error('Missing browser Supabase test environment');
	}
	return window.__TINYBASE_SUPABASE_ENV__;
};

const startPersister = async (userId: string): Promise<void> => {
	store = createStore();
	persister = await createSupabasePersister(store, {
		databaseName: 'tinybase-supabase-e2e',
		pollIntervalMs: 0,
		scopeKey: userId,
		supabase: client,
		tables: {
			crdt_documents: {
				crdtCells: {
					body: { type: 'text' },
					items: { type: 'array' },
					properties: { type: 'map' },
				},
				crdtRowIdColumn: 'document_id',
				crdtUpdatesTable: 'crdt_document_updates',
				realtime: { debounceMs: 50 },
				table: 'crdt_documents',
			},
			projects: { table: 'projects' },
			todos: { dependsOn: ['projects'], realtime: { debounceMs: 50 }, table: 'todos' },
		},
	});
	await persister.startAutoPersisting();
};

window.tinybaseSupabaseTest = {
	async boot(email, password) {
		const environment = getEnvironment();
		client = createClient(environment.apiUrl, environment.anonKey, {
			auth: { autoRefreshToken: false, persistSession: true },
		});
		const session = await client.auth.getSession();
		let user = session.data.session?.user;
		if (!user) {
			const signedIn = await client.auth.signInWithPassword({ email, password });
			if (signedIn.error || !signedIn.data.user) {
				throw signedIn.error ?? new Error('Could not sign in test user');
			}
			user = signedIn.data.user;
		}

		await startPersister(user.id);
		return user.id;
	},
	async createCrdtDocument(id, ownerId) {
		if (!store || !persister) {
			throw new Error('Harness is not started');
		}
		store.setRow('crdt_documents', id, { owner_id: ownerId, status: 'draft' });
		await persister.save();
		await persister.syncNow();
	},
	async destroy() {
		await persister?.destroy();
		crdtRows.clear();
		await client?.auth.signOut();
	},
	async editCrdtText(id, text) {
		const row = crdtRows.get(id);
		if (!row) {
			throw new Error(`CRDT document is not open: ${id}`);
		}
		const body = row.getText('body');
		body.insert(body.length, text);
		await new Promise((resolve) => setTimeout(resolve, 0));
	},
	getCrdtText(id) {
		return String(store?.getCell('crdt_documents', id, 'body') ?? '');
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
	async openCrdtDocument(id) {
		if (!persister) {
			throw new Error('Harness is not started');
		}
		crdtRows.set(id, await persister.openRow('crdt_documents', id));
	},
	async restartPersister() {
		const session = await client.auth.getSession();
		const userId = session.data.session?.user.id;
		if (!userId) {
			throw new Error('Harness has no authenticated session');
		}
		await persister?.destroy();
		crdtRows.clear();
		await startPersister(userId);
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
