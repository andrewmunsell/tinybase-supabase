import { createStore, type Row, type Store, type Table, type Tables } from 'tinybase';
import type { SupabasePersisterConfig } from '../types.js';
import { getMetadataRow } from './config.js';

export interface ShadowStoreBridge {
	readonly shadowStore: Store;
	destroy(): void;
}

export const createShadowStoreBridge = (
	store: Store,
	config: SupabasePersisterConfig,
	getProjection: (tableId: string, rowId: string) => Row | undefined,
	reportError: (error: Error) => void,
): ShadowStoreBridge => {
	const shadowStore = createStore();
	const initialTables: Tables = {};
	for (const [tableId, tableConfig] of Object.entries(config.tables)) {
		const table: Table = {};
		for (const [rowId, row] of Object.entries(store.getTable(tableId))) {
			table[rowId] = getMetadataRow(row, tableConfig);
		}
		initialTables[tableId] = table;
	}
	shadowStore.setTables(initialTables);
	store.transaction(() => {
		for (const [tableId, tableConfig] of Object.entries(config.tables)) {
			for (const rowId of store.getRowIds(tableId)) {
				for (const cellId of Object.keys(tableConfig.crdtCells ?? {})) {
					store.delCell(tableId, rowId, cellId);
				}
			}
		}
	});

	const storeListenerIds: string[] = [];
	const shadowListenerIds: string[] = [];
	let applyingProjection = false;
	let applyingShadow = false;

	for (const [tableId, tableConfig] of Object.entries(config.tables)) {
		storeListenerIds.push(
			store.addTableListener(tableId, () => {
				if (applyingProjection || applyingShadow) {
					return;
				}
				const nextTable: Table = {};
				for (const [rowId, row] of Object.entries(store.getTable(tableId))) {
					nextTable[rowId] = getMetadataRow(row, tableConfig);
					const projection = getProjection(tableId, rowId);
					const closedCrdtCells = projection
						? []
						: Object.keys(tableConfig.crdtCells ?? {}).filter(
								(cellId) => cellId in row,
							);
					if (closedCrdtCells.length > 0) {
						reportError(
							new Error(
								`CRDT cells in ${tableId}.${rowId} are unavailable until the row is opened`,
							),
						);
						queueMicrotask(() => {
							applyingProjection = true;
							store.transaction(() => {
								for (const cellId of closedCrdtCells) {
									store.delCell(tableId, rowId, cellId);
								}
							});
							applyingProjection = false;
						});
					}
					if (
						projection &&
						Object.entries(projection).some(
							([cellId, value]) =>
								JSON.stringify(row[cellId]) !== JSON.stringify(value),
						)
					) {
						reportError(
							new Error(
								`CRDT cells in ${tableId}.${rowId} are read-only in TinyBase; use the Yjs row handle`,
							),
						);
						queueMicrotask(() => {
							applyingProjection = true;
							store.setPartialRow(tableId, rowId, projection);
							applyingProjection = false;
						});
					}
				}
				shadowStore.setTable(tableId, nextTable);
			}),
		);

		shadowListenerIds.push(
			shadowStore.addTableListener(tableId, () => {
				if (applyingShadow) {
					return;
				}
				applyingShadow = true;
				const shadowTable = shadowStore.getTable(tableId);
				for (const rowId of store.getRowIds(tableId)) {
					if (!shadowTable[rowId]) {
						store.delRow(tableId, rowId);
					}
				}
				for (const [rowId, row] of Object.entries(shadowTable)) {
					store.setRow(tableId, rowId, { ...row, ...getProjection(tableId, rowId) });
				}
				applyingShadow = false;
			}),
		);
	}

	return {
		destroy() {
			for (const listenerId of storeListenerIds) {
				store.delListener(listenerId);
			}
			for (const listenerId of shadowListenerIds) {
				shadowStore.delListener(listenerId);
			}
		},
		shadowStore,
	};
};
