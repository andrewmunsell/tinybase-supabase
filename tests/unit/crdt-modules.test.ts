import 'fake-indexeddb/auto';
import { openDB } from 'idb';
import { createStore } from 'tinybase';
import * as Y from 'yjs';
import { CrdtLocalState } from '../../src/crdt-storage.js';
import { decodeBytea, encodeBytea, getProjection } from '../../src/crdt/codec.js';
import {
	getConfiguredCrdtTables,
	getMetadataRow,
	hasConfiguredCrdtCells,
} from '../../src/crdt/config.js';
import { createShadowStoreBridge } from '../../src/crdt/shadow-store.js';
import { CrdtTransport } from '../../src/crdt/transport.js';

describe('CRDT modules', () => {
	it('upgrades existing CRDT IndexedDB state with document quarantine', async () => {
		const databaseName = `quarantine-upgrade-${crypto.randomUUID()}`;
		const database = await openDB(`${databaseName}:user:yjs`, 2, {
			upgrade(upgradeDatabase) {
				const updates = upgradeDatabase.createObjectStore('updates');
				updates.createIndex('documentKey', 'documentKey');
				upgradeDatabase.createObjectStore('outbox');
				upgradeDatabase.createObjectStore('rejected');
				const buffered = upgradeDatabase.createObjectStore('buffered');
				buffered.createIndex('documentKey', 'documentKey');
			},
		});
		await database.put(
			'rejected',
			{
				documentKey: 'documents\0doc-1',
				error: 'forbidden',
				id: 'rejected-1',
				rowId: 'doc-1',
				tableId: 'documents',
				update: Uint8Array.from([1, 2]),
			},
			'rejected-1',
		);
		database.close();

		const state = await CrdtLocalState.open(databaseName, 'user');
		await state.persistLocalUpdate({
			bufferedAt: Date.now(),
			documentKey: 'documents\0doc-1',
			id: 'update-1',
			rowId: 'doc-1',
			tableId: 'documents',
			update: Uint8Array.from([1, 2]),
		});
		await expect(state.getBuffered()).resolves.toHaveLength(1);
		await expect(state.getQuarantined()).resolves.toEqual([
			expect.objectContaining({
				documentKey: 'documents\0doc-1',
				failedUpdateId: 'rejected-1',
				state: 'rejected',
			}),
		]);
		state.close();
	});

	it('round trips Supabase bytea values', () => {
		const update = Uint8Array.from([0, 1, 127, 255]);
		expect(encodeBytea(update)).toBe('\\x00017fff');
		expect(decodeBytea(encodeBytea(update))).toEqual(update);
		expect(() => decodeBytea('not-hex')).toThrow('invalid bytea');
	});

	it('normalizes optional CRDT configuration without requiring an updates table for zero cells', () => {
		const ordinary = {
			databaseName: 'test',
			scopeKey: 'user',
			supabase: {},
			tables: { todos: { crdtCells: {}, table: 'todos' } },
		};
		expect(hasConfiguredCrdtCells(ordinary)).toBe(false);
		expect(getConfiguredCrdtTables(ordinary)).toEqual({});
		expect(() =>
			getConfiguredCrdtTables({
				...ordinary,
				tables: { todos: { crdtCells: { body: { type: 'text' } }, table: 'todos' } },
			}),
		).toThrow('no crdtUpdatesTable');
		expect(
			getConfiguredCrdtTables({
				...ordinary,
				tables: {
					todos: {
						crdtCells: { body: { type: 'text' } },
						crdtUpdatesTable: 'todo_updates',
						mode: 'read-only',
						table: 'todos',
					},
				},
			}),
		).toEqual({
			todos: expect.objectContaining({
				crdtUpdatesTable: 'todo_updates',
				mode: 'read-only',
			}),
		});
	});

	it('projects each supported Yjs type and strips only configured cells from metadata', () => {
		const document = new Y.Doc();
		document.getText('body').insert(0, 'Hello');
		document.getArray('items').push(['one']);
		document.getMap('properties').set('color', 'blue');
		const paragraph = new Y.XmlElement('p');
		paragraph.insert(0, [new Y.XmlText('Structured')]);
		document.getXmlFragment('content').insert(0, [paragraph]);
		expect(getProjection(document, 'body', { type: 'text' })).toBe('Hello');
		expect(getProjection(document, 'items', { type: 'array' })).toEqual(['one']);
		expect(getProjection(document, 'properties', { type: 'map' })).toEqual({ color: 'blue' });
		expect(getProjection(document, 'content', { type: 'xml-fragment' })).toBe(
			'<p>Structured</p>',
		);
		expect(
			getMetadataRow(
				{ body: 'projection', owner_id: 'user', status: 'draft' },
				{ crdtCells: { body: { type: 'text' } }, table: 'documents' },
			),
		).toEqual({ owner_id: 'user', status: 'draft' });
	});

	it('keeps ordinary shadow fields writable while restoring CRDT projections', async () => {
		const errors: Error[] = [];
		const store = createStore().setRow('documents', 'doc-1', {
			body: 'Rendered',
			owner_id: 'user',
		});
		const bridge = createShadowStoreBridge(
			store,
			{
				databaseName: 'test',
				scopeKey: 'user',
				supabase: {},
				tables: {
					documents: { crdtCells: { body: { type: 'text' } }, table: 'documents' },
				},
			},
			() => ({ body: 'Rendered' }),
			(error) => errors.push(error),
		);
		store.setCell('documents', 'doc-1', 'body', 'Rendered');
		await Promise.resolve();
		errors.length = 0;

		store.setCell('documents', 'doc-1', 'owner_id', 'next-user');
		expect(bridge.shadowStore.getRow('documents', 'doc-1')).toEqual({ owner_id: 'next-user' });
		store.setCell('documents', 'doc-1', 'body', 'Invalid replacement');
		await Promise.resolve();
		expect(store.getCell('documents', 'doc-1', 'body')).toBe('Rendered');
		expect(errors).toHaveLength(1);
		bridge.destroy();
	});

	it('accepts an idempotent duplicate update but rejects a colliding UUID', async () => {
		const stored: Record<string, unknown> = {
			document_id: 'doc-1',
			id: 'update-1',
			update: '\\x0102',
		};
		const client = {
			from: () => ({
				insert: async () => ({
					data: null,
					error: { code: '23505', message: 'duplicate key value' },
				}),
				select: () => {
					const query = Object.assign(Promise.resolve({ data: [stored], error: null }), {
						eq: () => query,
						order: () => query,
						range: () => query,
					});
					return query;
				},
			}),
		};
		const transport = new CrdtTransport(client, 10, 'user');
		const config = {
			crdtCells: { body: { type: 'text' as const } },
			crdtRowIdColumn: 'document_id',
			crdtUpdatesTable: 'document_updates',
			table: 'documents',
		};
		const update = {
			documentKey: 'documents\0doc-1',
			id: 'update-1',
			rowId: 'doc-1',
			tableId: 'documents',
			update: Uint8Array.from([1, 2]),
		};
		await expect(transport.insertUpdate(update, config)).resolves.toBeUndefined();
		stored.document_id = 42;
		update.rowId = '42';
		await expect(transport.insertUpdate(update, config)).resolves.toBeUndefined();
		stored.update = '\\x0304';
		await expect(transport.insertUpdate(update, config)).rejects.toMatchObject({
			code: '23505',
		});
	});
});
