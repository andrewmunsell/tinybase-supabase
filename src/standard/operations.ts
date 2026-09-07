import type { Content, Row, Table } from 'tinybase';
import type { PendingOperation } from '../storage.js';
import type { SupabaseRow, SupabaseTableConfig } from '../types.js';

export const cloneContent = (content: Content): Content => structuredClone(content);
const comparableJson = (value: unknown): string | undefined =>
	JSON.stringify(value, (_key, entry: unknown) =>
		entry && typeof entry === 'object' && !Array.isArray(entry)
			? Object.fromEntries(
					Object.entries(entry).sort(([left], [right]) =>
						left < right ? -1 : left > right ? 1 : 0,
					),
				)
			: entry,
	);
export const contentEquals = (left: unknown, right: unknown): boolean =>
	comparableJson(left) === comparableJson(right);

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

/** Overlays local row and value changes, including deletions, on a content snapshot. */
export const mergeContentChanges = (
	content: Content,
	before: Content,
	current: Content,
): Content => {
	const merged = cloneContent(content);

	for (const tableId of new Set([...Object.keys(before[0]), ...Object.keys(current[0])])) {
		const previousRows = getRows(before, tableId);
		const currentRows = getRows(current, tableId);
		for (const rowId of new Set([...Object.keys(previousRows), ...Object.keys(currentRows)])) {
			if (!contentEquals(previousRows[rowId], currentRows[rowId])) {
				merged[0][tableId] ??= {};
				const table = merged[0][tableId];
				if (currentRows[rowId]) {
					table[rowId] = currentRows[rowId];
				} else {
					delete table[rowId];
				}
			}
		}
	}

	for (const valueId of new Set([...Object.keys(before[1]), ...Object.keys(current[1])])) {
		if (!contentEquals(before[1][valueId], current[1][valueId])) {
			const value = current[1][valueId];
			if (value !== undefined) {
				merged[1][valueId] = value;
			} else {
				delete merged[1][valueId];
			}
		}
	}

	for (const [tableId, table] of Object.entries(merged[0])) {
		if (Object.keys(table).length === 0) {
			delete merged[0][tableId];
		}
	}

	return merged;
};

export const fromRemote = (
	config: SupabaseTableConfig,
	remote: SupabaseRow,
): readonly [string, Row] => {
	if (config.fromRemote) {
		return config.fromRemote(remote);
	}

	const idColumn = config.idColumn ?? 'id';
	const deletedAtColumn = config.deletedAtColumn ?? 'deleted_at';
	const updatedAtColumn = config.updatedAtColumn;
	const row: Row = {};
	for (const [column, value] of Object.entries(remote)) {
		if (column !== idColumn && column !== deletedAtColumn && column !== updatedAtColumn) {
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
			if (contentEquals(previous, next)) {
				continue;
			}

			operations.push(
				next
					? {
							id: operationId(tableId, rowId),
							revision: crypto.randomUUID(),
							kind: 'upsert',
							payload: toRemote(config, rowId, next),
							rowId,
							tableId,
						}
					: {
							id: operationId(tableId, rowId),
							revision: crypto.randomUUID(),
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
