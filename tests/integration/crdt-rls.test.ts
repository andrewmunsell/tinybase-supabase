import 'fake-indexeddb/auto';
import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { createStore } from 'tinybase';
import { createSupabasePersister } from '../../src/index.js';

const apiUrl = process.env.SUPABASE_TEST_URL;
const anonKey = process.env.SUPABASE_TEST_ANON_KEY;
const integrationDescribe = apiUrl && anonKey ? describe : describe.skip;

const createAuthenticatedClient = async (): Promise<{
	client: SupabaseClient;
	userId: string;
}> => {
	const client = createClient(apiUrl as string, anonKey as string, {
		auth: { autoRefreshToken: false, persistSession: false },
	});
	const { data, error } = await client.auth.signUp({
		email: `crdt-${crypto.randomUUID()}@example.test`,
		password: 'correct-horse-battery-staple',
	});
	if (error || !data.user || !data.session) {
		throw error ?? new Error('Supabase did not create a user session');
	}
	return { client, userId: data.user.id };
};

const insertUpdate = (client: SupabaseClient, documentId: string) =>
	client.from('crdt_document_updates').insert({
		document_id: documentId,
		id: crypto.randomUUID(),
		update: '\\x0001',
	});

integrationDescribe('CRDT update RLS', () => {
	it('persists and hydrates mixed Yjs types through the Supabase bytea table', async () => {
		const owner = await createAuthenticatedClient();
		const collaborator = await createAuthenticatedClient();
		const documentId = `persister-document-${crypto.randomUUID()}`;
		expect(
			(
				await owner.client.from('crdt_documents').insert({
					id: documentId,
					owner_id: owner.userId,
					status: 'draft',
				})
			).error,
		).toBeNull();
		expect(
			(
				await owner.client.from('crdt_document_collaborators').insert({
					can_write: false,
					document_id: documentId,
					user_id: collaborator.userId,
				})
			).error,
		).toBeNull();

		const ownerStore = createStore().setRow('crdt_documents', documentId, {
			owner_id: owner.userId,
			status: 'draft',
		});
		const ownerPersister = await createSupabasePersister(ownerStore, {
			databaseName: `crdt-owner-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: owner.userId,
			supabase: owner.client,
			tables: {
				crdt_documents: {
					crdtCells: {
						body: { type: 'text' },
						items: { type: 'array' },
						properties: { type: 'map' },
					},
					crdtRowIdColumn: 'document_id',
					crdtUpdatesTable: 'crdt_document_updates',
					table: 'crdt_documents',
				},
			},
		});
		const ownerRow = await ownerPersister.openRow('crdt_documents', documentId);
		ownerRow.getText('body').insert(0, 'Shared');
		ownerRow.getArray<string>('items').push(['one']);
		ownerRow.getMap<string>('properties').set('color', 'blue');
		await new Promise((resolve) => setTimeout(resolve, 0));
		await ownerPersister.syncNow();
		const uploaded = await owner.client
			.from('crdt_document_updates')
			.select('id')
			.eq('document_id', documentId);
		expect(uploaded.error).toBeNull();
		expect(uploaded.data).toHaveLength(1);

		const collaboratorStore = createStore();
		const collaboratorPersister = await createSupabasePersister(collaboratorStore, {
			databaseName: `crdt-collaborator-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: collaborator.userId,
			supabase: collaborator.client,
			tables: {
				crdt_documents: {
					crdtCells: {
						body: { type: 'text' },
						items: { type: 'array' },
						properties: { type: 'map' },
					},
					crdtRowIdColumn: 'document_id',
					crdtUpdatesTable: 'crdt_document_updates',
					mode: 'read-only',
					table: 'crdt_documents',
				},
			},
		});
		await collaboratorPersister.startAutoPersisting();
		const collaboratorRow = await collaboratorPersister.openRow('crdt_documents', documentId);
		expect(collaboratorStore.getRow('crdt_documents', documentId)).toMatchObject({
			body: 'Shared',
			items: ['one'],
			properties: { color: 'blue' },
			status: 'draft',
		});
		expect(() => collaboratorRow.getText('body').insert(0, 'Not uploaded: ')).toThrow(
			'is read-only',
		);
		await collaboratorPersister.syncNow();
		await expect(collaboratorPersister.getRejectedOperations()).resolves.toEqual([]);
		expect(
			(
				await owner.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toHaveLength(1);

		await ownerPersister.destroy();
		await collaboratorPersister.destroy();
	});

	it('enforces read and write roles through a many-to-one workspace relationship', async () => {
		const owner = await createAuthenticatedClient();
		const viewer = await createAuthenticatedClient();
		const editor = await createAuthenticatedClient();
		const outsider = await createAuthenticatedClient();
		const suffix = crypto.randomUUID();
		const workspaceId = `workspace-${suffix}`;
		const documentId = `workspace-document-${suffix}`;

		expect(
			(
				await owner.client
					.from('crdt_workspaces')
					.insert({ id: workspaceId, owner_id: owner.userId })
			).error,
		).toBeNull();
		expect(
			(
				await owner.client.from('crdt_workspace_members').insert([
					{ can_write: false, user_id: viewer.userId, workspace_id: workspaceId },
					{ can_write: true, user_id: editor.userId, workspace_id: workspaceId },
				])
			).error,
		).toBeNull();
		expect(
			(
				await owner.client.from('crdt_documents').insert({
					id: documentId,
					owner_id: owner.userId,
					workspace_id: workspaceId,
				})
			).error,
		).toBeNull();
		expect((await insertUpdate(owner.client, documentId)).error).toBeNull();

		for (const authorized of [viewer.client, editor.client]) {
			const { data, error } = await authorized
				.from('crdt_document_updates')
				.select('id')
				.eq('document_id', documentId);
			expect(error).toBeNull();
			expect(data).toHaveLength(1);
		}
		expect((await insertUpdate(viewer.client, documentId)).error).not.toBeNull();
		expect((await insertUpdate(editor.client, documentId)).error).toBeNull();
		expect(
			(
				await outsider.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toEqual([]);
		expect((await insertUpdate(outsider.client, documentId)).error).not.toBeNull();
		let unauthorizedRealtimeEvents = 0;
		const unauthorizedChannel = outsider.client.channel(`unauthorized-crdt-${suffix}`).on(
			'postgres_changes',
			{
				event: 'INSERT',
				filter: `document_id=eq.${documentId}`,
				schema: 'public',
				table: 'crdt_document_updates',
			},
			() => {
				unauthorizedRealtimeEvents += 1;
			},
		);
		await new Promise<void>((resolve, reject) => {
			unauthorizedChannel.subscribe((status, error) => {
				if (status === 'SUBSCRIBED') {
					resolve();
				} else if (status === 'CHANNEL_ERROR') {
					reject(error);
				}
			});
		});
		expect((await insertUpdate(owner.client, documentId)).error).toBeNull();
		await new Promise((resolve) => setTimeout(resolve, 300));
		expect(unauthorizedRealtimeEvents).toBe(0);
		await outsider.client.removeChannel(unauthorizedChannel);

		expect(
			(
				await owner.client
					.from('crdt_workspace_members')
					.delete()
					.eq('workspace_id', workspaceId)
					.eq('user_id', editor.userId)
			).error,
		).toBeNull();
		expect((await insertUpdate(editor.client, documentId)).error).not.toBeNull();
	});

	it('enforces viewer and editor roles through a many-to-many collaborator relationship', async () => {
		const owner = await createAuthenticatedClient();
		const viewer = await createAuthenticatedClient();
		const editor = await createAuthenticatedClient();
		const documentId = `collaborative-document-${crypto.randomUUID()}`;

		expect(
			(
				await owner.client.from('crdt_documents').insert({
					id: documentId,
					owner_id: owner.userId,
				})
			).error,
		).toBeNull();
		expect(
			(
				await owner.client.from('crdt_document_collaborators').insert([
					{ can_write: false, document_id: documentId, user_id: viewer.userId },
					{ can_write: true, document_id: documentId, user_id: editor.userId },
				])
			).error,
		).toBeNull();
		expect((await insertUpdate(owner.client, documentId)).error).toBeNull();

		expect(
			(
				await viewer.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toHaveLength(1);
		expect((await insertUpdate(viewer.client, documentId)).error).not.toBeNull();
		expect((await insertUpdate(editor.client, documentId)).error).toBeNull();

		expect(
			(
				await owner.client
					.from('crdt_document_collaborators')
					.update({ can_write: false })
					.eq('document_id', documentId)
					.eq('user_id', editor.userId)
			).error,
		).toBeNull();
		expect((await insertUpdate(editor.client, documentId)).error).not.toBeNull();

		expect(
			(
				await owner.client
					.from('crdt_document_collaborators')
					.delete()
					.eq('document_id', documentId)
					.eq('user_id', viewer.userId)
			).error,
		).toBeNull();
		expect(
			(
				await viewer.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toEqual([]);
	});

	it('applies relationship grants, revocations, and parent workspace moves immediately', async () => {
		const owner = await createAuthenticatedClient();
		const member = await createAuthenticatedClient();
		const suffix = crypto.randomUUID();
		const firstWorkspace = `grant-first-${suffix}`;
		const secondWorkspace = `grant-second-${suffix}`;
		const documentId = `grant-document-${suffix}`;
		for (const workspaceId of [firstWorkspace, secondWorkspace]) {
			expect(
				(
					await owner.client
						.from('crdt_workspaces')
						.insert({ id: workspaceId, owner_id: owner.userId })
				).error,
			).toBeNull();
		}
		expect(
			(
				await owner.client.from('crdt_documents').insert({
					id: documentId,
					owner_id: owner.userId,
					workspace_id: firstWorkspace,
				})
			).error,
		).toBeNull();
		expect((await insertUpdate(owner.client, documentId)).error).toBeNull();
		expect(
			(
				await member.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toEqual([]);

		expect(
			(
				await owner.client.from('crdt_workspace_members').insert({
					can_write: true,
					user_id: member.userId,
					workspace_id: firstWorkspace,
				})
			).error,
		).toBeNull();
		expect((await insertUpdate(member.client, documentId)).error).toBeNull();

		expect(
			(
				await owner.client
					.from('crdt_documents')
					.update({ workspace_id: secondWorkspace })
					.eq('id', documentId)
			).error,
		).toBeNull();
		expect((await insertUpdate(member.client, documentId)).error).not.toBeNull();
		expect(
			(
				await owner.client.from('crdt_workspace_members').insert({
					can_write: false,
					user_id: member.userId,
					workspace_id: secondWorkspace,
				})
			).error,
		).toBeNull();
		expect(
			(
				await member.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toHaveLength(2);
		expect((await insertUpdate(member.client, documentId)).error).not.toBeNull();

		expect(
			(
				await owner.client
					.from('crdt_workspace_members')
					.delete()
					.eq('workspace_id', secondWorkspace)
					.eq('user_id', member.userId)
			).error,
		).toBeNull();
		expect(
			(
				await member.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toEqual([]);
	});

	it('quarantines dependent CRDT updates after access is revoked and retries them together', async () => {
		const owner = await createAuthenticatedClient();
		const editor = await createAuthenticatedClient();
		const documentId = `open-revocation-${crypto.randomUUID()}`;
		expect(
			(
				await owner.client.from('crdt_documents').insert({
					id: documentId,
					owner_id: owner.userId,
				})
			).error,
		).toBeNull();
		expect(
			(
				await owner.client.from('crdt_document_collaborators').insert({
					can_write: true,
					document_id: documentId,
					user_id: editor.userId,
				})
			).error,
		).toBeNull();

		const store = createStore().setRow('crdt_documents', documentId, {
			owner_id: owner.userId,
		});
		const persister = await createSupabasePersister(store, {
			databaseName: `open-revocation-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: editor.userId,
			supabase: editor.client,
			tables: {
				crdt_documents: {
					crdtCells: { body: { type: 'text' } },
					crdtRowIdColumn: 'document_id',
					crdtUpdatesTable: 'crdt_document_updates',
					table: 'crdt_documents',
				},
			},
		});
		const row = await persister.openRow('crdt_documents', documentId);
		row.getText('body').insert(0, 'Unsent after revocation');
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(
			(
				await owner.client
					.from('crdt_document_collaborators')
					.delete()
					.eq('document_id', documentId)
					.eq('user_id', editor.userId)
			).error,
		).toBeNull();

		await persister.syncNow();
		await expect(persister.getRejectedOperations()).resolves.toEqual([
			expect.objectContaining({ rowId: documentId, tableId: 'crdt_documents' }),
		]);
		expect(persister.getSyncStatus().rejectedCount).toBe(1);
		row.getText('body').insert(row.getText('body').length, ' with a successor');
		await persister.syncNow();
		expect(
			(
				await owner.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toHaveLength(0);

		expect(
			(
				await owner.client.from('crdt_document_collaborators').insert({
					can_write: true,
					document_id: documentId,
					user_id: editor.userId,
				})
			).error,
		).toBeNull();
		await persister.retryRejected();
		expect(persister.getSyncStatus()).toMatchObject({ pendingCount: 0, rejectedCount: 0 });
		expect(
			(
				await owner.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toHaveLength(1);

		const receiverStore = createStore().setRow('crdt_documents', documentId, {
			owner_id: owner.userId,
		});
		const receiver = await createSupabasePersister(receiverStore, {
			databaseName: `open-revocation-receiver-${crypto.randomUUID()}`,
			pollIntervalMs: 0,
			scopeKey: owner.userId,
			supabase: owner.client,
			tables: {
				crdt_documents: {
					crdtCells: { body: { type: 'text' } },
					crdtRowIdColumn: 'document_id',
					crdtUpdatesTable: 'crdt_document_updates',
					table: 'crdt_documents',
				},
			},
		});
		await receiver.openRow('crdt_documents', documentId);
		expect(receiverStore.getCell('crdt_documents', documentId, 'body')).toBe(
			'Unsent after revocation with a successor',
		);
		await receiver.destroy();
		await persister.destroy();
	});

	it('denies nonexistent, deleted, anonymous, update, and delete attack paths', async () => {
		const owner = await createAuthenticatedClient();
		const outsider = await createAuthenticatedClient();
		const documentId = `deleted-document-${crypto.randomUUID()}`;
		expect(
			(await insertUpdate(owner.client, `missing-${crypto.randomUUID()}`)).error,
		).not.toBeNull();
		expect(
			(
				await owner.client.from('crdt_documents').insert({
					id: documentId,
					owner_id: owner.userId,
				})
			).error,
		).toBeNull();
		const inserted = await insertUpdate(owner.client, documentId);
		expect(inserted.error).toBeNull();
		const { data: rows } = await owner.client
			.from('crdt_document_updates')
			.select('id')
			.eq('document_id', documentId);
		const updateId = rows?.[0]?.id as string;
		expect(
			(await outsider.client.from('crdt_document_updates').select('id').eq('id', updateId))
				.data,
		).toEqual([]);
		expect(
			(
				await owner.client.from('crdt_document_updates').insert({
					document_id: documentId,
					id: updateId,
					update: '\\xffff',
				})
			).error?.code,
		).toBe('23505');
		expect(
			(
				await owner.client
					.from('crdt_document_updates')
					.update({ update: '\\x01' })
					.eq('id', updateId)
			).error,
		).not.toBeNull();
		expect(
			(
				await owner.client.from('crdt_document_updates').insert({
					created_at: '2000-01-01T00:00:00.000Z',
					document_id: documentId,
					id: crypto.randomUUID(),
					update: '\\x0001',
				})
			).error,
		).not.toBeNull();
		expect(
			(await owner.client.from('crdt_document_updates').delete().eq('id', updateId)).error,
		).not.toBeNull();

		expect(
			(
				await owner.client
					.from('crdt_documents')
					.update({ deleted_at: new Date().toISOString() })
					.eq('id', documentId)
			).error,
		).toBeNull();
		expect((await insertUpdate(owner.client, documentId)).error).not.toBeNull();
		expect(
			(
				await owner.client
					.from('crdt_document_updates')
					.select('id')
					.eq('document_id', documentId)
			).data,
		).toEqual([]);

		const anonymous = createClient(apiUrl as string, anonKey as string, {
			auth: { persistSession: false },
		});
		const anonymousRead = await anonymous
			.from('crdt_document_updates')
			.select('id')
			.eq('document_id', documentId);
		expect(anonymousRead.data).toBeNull();
		expect(anonymousRead.error).not.toBeNull();
		expect((await insertUpdate(anonymous, documentId)).error).not.toBeNull();

		await owner.client.auth.signOut();
		const signedOutRead = await owner.client
			.from('crdt_document_updates')
			.select('id')
			.eq('document_id', documentId);
		expect(signedOutRead.data).toBeNull();
		expect(signedOutRead.error).not.toBeNull();
		expect((await insertUpdate(owner.client, documentId)).error).not.toBeNull();
	});
});
