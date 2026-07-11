import type { Cell } from 'tinybase';
import type * as Y from 'yjs';
import type { CrdtCellConfig } from '../types.js';

export const remoteOrigin = Symbol('remote');
export const getDocumentKey = (tableId: string, rowId: string): string => `${tableId}\0${rowId}`;

export const encodeBytea = (update: Uint8Array): string =>
	`\\x${Array.from(update, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;

export const decodeBytea = (value: unknown): Uint8Array => {
	if (value instanceof Uint8Array) {
		return value;
	}
	if (typeof value !== 'string') {
		throw new Error('A CRDT update must be returned from Supabase as bytea');
	}
	const hexadecimal = value.startsWith('\\x') ? value.slice(2) : value;
	if (!/^(?:[0-9a-f]{2})+$/i.test(hexadecimal)) {
		throw new Error('A CRDT update contains invalid bytea data');
	}
	return Uint8Array.from(hexadecimal.match(/.{2}/g) ?? [], (byte) => Number.parseInt(byte, 16));
};

export const getProjection = (document: Y.Doc, cellId: string, config: CrdtCellConfig): Cell => {
	switch (config.type) {
		case 'array':
			return document.getArray(cellId).toJSON();
		case 'map':
			return document.getMap(cellId).toJSON();
		case 'text':
			return document.getText(cellId).toString();
		case 'xml-fragment':
			return document.getXmlFragment(cellId).toString();
	}
};
