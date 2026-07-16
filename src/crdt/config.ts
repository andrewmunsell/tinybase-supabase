import type { Row } from 'tinybase';
import type {
	CrdtCellConfig,
	SupabasePersisterConfig,
	SupabaseRow,
	SupabaseTableConfig,
} from '../types.js';

export interface ConfiguredCrdtTable extends SupabaseTableConfig {
	readonly crdtCells: Readonly<Record<string, CrdtCellConfig>>;
	readonly crdtUpdatesTable: string;
}

export const getConfiguredCrdtTables = (
	config: SupabasePersisterConfig,
): Readonly<Record<string, ConfiguredCrdtTable>> => {
	const configured: Record<string, ConfiguredCrdtTable> = {};
	for (const [tableId, table] of Object.entries(config.tables)) {
		if (Object.keys(table.crdtCells ?? {}).length === 0) {
			continue;
		}
		if (!table.crdtUpdatesTable) {
			throw new Error(`Table ${tableId} configures CRDT cells but no crdtUpdatesTable`);
		}
		configured[tableId] = table as ConfiguredCrdtTable;
	}
	return configured;
};

export const hasConfiguredCrdtCells = (config: SupabasePersisterConfig): boolean =>
	Object.values(config.tables).some((table) => Object.keys(table.crdtCells ?? {}).length > 0);

export const getMetadataRow = (row: Row, config: SupabaseTableConfig): Row => {
	const metadata: Row = {};
	for (const [cellId, cell] of Object.entries(row)) {
		if (!config.crdtCells?.[cellId]) {
			metadata[cellId] = cell;
		}
	}
	return metadata;
};

export const getStandardConfig = (config: SupabasePersisterConfig): SupabasePersisterConfig => ({
	...config,
	tables: Object.fromEntries(
		Object.entries(config.tables).map(([tableId, table]) => {
			const {
				crdtCells: _cells,
				crdtRowIdColumn: _rowId,
				crdtUpdatesTable: _updates,
				...standard
			} = table;
			return [
				tableId,
				{
					...standard,
					fromRemote: (remote: SupabaseRow) => {
						const decoded =
							table.fromRemote?.(remote) ??
							([
								String(remote[table.idColumn ?? 'id']),
								Object.fromEntries(
									Object.entries(remote).filter(
										([column]) =>
											column !== (table.idColumn ?? 'id') &&
											column !== (table.deletedAtColumn ?? 'deleted_at') &&
											column !== table.updatedAtColumn,
									),
								) as Row,
							] as const);
						return [decoded[0], getMetadataRow(decoded[1], table)] as const;
					},
					toRemote: (rowId: string, row: Row) =>
						table.toRemote?.(rowId, getMetadataRow(row, table)) ??
						getMetadataRow(row, table),
				},
			];
		}),
	),
});
