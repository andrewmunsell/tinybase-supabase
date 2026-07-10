import type { Content, Row, Store, Table } from 'tinybase';
import type { PendingOperation } from '../storage.js';
import type { SupabaseRow, SupabaseTableConfig } from '../types.js';

export const cloneContent = (content: Content): Content => structuredClone(content);
export const asError = (error: unknown): Error =>
	error instanceof Error
		? error
		: new Error(
				typeof error === 'string'
					? error
					: typeof error === 'object' && error && 'message' in error
						? String(error.message)
						: 'Unknown error',
			);
export const operationId = (tableId: string, rowId: string): string => `${tableId}:${rowId}`;
export const getRows = (content: Content, tableId: string): Table => content[0][tableId] ?? {};

export const fromRemote = (
	config: SupabaseTableConfig,
	remote: SupabaseRow,
): readonly [string, Row] => {
	if (config.fromRemote) {
		return config.fromRemote(remote);
	}
	const idColumn = config.idColumn ?? 'id';
	const deletedAtColumn = config.deletedAtColumn ?? 'deleted_at';
	const row: Row = {};
	for (const [column, value] of Object.entries(remote)) {
		if (column !== idColumn && column !== deletedAtColumn) {
			row[column] = value as Row[string];
		}
	}
	return [String(remote[idColumn]), row];
};

const toRemote = (config: SupabaseTableConfig, rowId: string, row: Row): SupabaseRow => ({
	...(config.toRemote?.(rowId, row) ?? row),
	[config.idColumn ?? 'id']: rowId,
});

export const createPendingOperations = (
	store: Store,
	lastContent: Content,
	content: Content,
	tables: Readonly<Record<string, SupabaseTableConfig>>,
): PendingOperation[] => {
	const operations: PendingOperation[] = [];
	for (const [tableId, config] of Object.entries(tables)) {
		if (config.mode === 'read-only') {
			continue;
		}
		const previousRows = getRows(lastContent, tableId);
		const nextRows = getRows(content, tableId);
		for (const rowId of new Set([...Object.keys(previousRows), ...Object.keys(nextRows)])) {
			const previous = previousRows[rowId];
			const next = nextRows[rowId];
			if (JSON.stringify(previous) === JSON.stringify(next)) {
				continue;
			}
			operations.push(
				next
					? {
							id: operationId(tableId, rowId),
							kind: 'upsert',
							payload: toRemote(config, rowId, store.getRow(tableId, rowId)),
							rowId,
							tableId,
						}
					: {
							id: operationId(tableId, rowId),
							kind: 'tombstone',
							payload: {
								[config.idColumn ?? 'id']: rowId,
								[config.deletedAtColumn ?? 'deleted_at']: new Date().toISOString(),
							},
							rowId,
							tableId,
						},
			);
		}
	}
	return operations;
};

export const sortOperations = (
	operations: readonly PendingOperation[],
	tables: Readonly<Record<string, SupabaseTableConfig>>,
): PendingOperation[] => {
	const depths = new Map<string, number>();
	const getDepth = (tableId: string): number => {
		const cached = depths.get(tableId);
		if (cached !== undefined) {
			return cached;
		}
		const depth =
			(tables[tableId]?.dependsOn ?? []).reduce(
				(maximum, dependency) => Math.max(maximum, getDepth(dependency)),
				0,
			) + 1;
		depths.set(tableId, depth);
		return depth;
	};
	return [...operations].sort((left, right) => {
		const leftDepth = getDepth(left.tableId);
		const rightDepth = getDepth(right.tableId);
		return (
			(left.kind === 'tombstone' ? -leftDepth : leftDepth) -
				(right.kind === 'tombstone' ? -rightDepth : rightDepth) ||
			left.id.localeCompare(right.id)
		);
	});
};
