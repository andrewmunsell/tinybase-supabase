import type { Content, Row, Store, Table, Tables } from 'tinybase';
import { createCustomPersister, type Persister } from 'tinybase/persisters';
import { LocalState, type PendingOperation } from './storage.js';
import type {
    RejectedOperation,
    SupabasePersister,
    SupabasePersisterConfig,
    SupabaseRow,
    SupabaseTableConfig,
    SyncPhase,
    SyncStatus,
} from './types.js';

interface SupabaseResponse {
    readonly data: unknown;
    readonly error: SupabaseError | null;
}

interface SupabaseError {
    readonly code?: string;
    readonly message: string;
    readonly status?: number;
}

interface SupabaseTableClient {
    upsert(values: SupabaseRow, options: { onConflict: string }): Promise<SupabaseResponse>;
    select(columns: string): SupabaseSelectClient;
}

interface SupabaseSelectClient extends PromiseLike<SupabaseResponse> {
    order(column: string): SupabaseSelectClient;
    range(from: number, to: number): SupabaseSelectClient;
}

interface SupabaseRealtimeChannel {
    on(
        type: 'postgres_changes',
        filter: { event: '*'; schema: string; table: string },
        callback: () => void,
    ): SupabaseRealtimeChannel;
    subscribe(): unknown;
}

interface SupabaseClient {
    channel(name: string): SupabaseRealtimeChannel;
    from(table: string): SupabaseTableClient;
    removeChannel(channel: SupabaseRealtimeChannel): unknown;
}

const defaultPageSize = 500;
const defaultPollIntervalMs = 60_000;

const cloneContent = (content: Content): Content => structuredClone(content);

const asError = (error: unknown): Error =>
    error instanceof Error ? error : new Error(typeof error === 'string' ? error : 'Unknown error');

const operationId = (tableId: string, rowId: string): string => `${tableId}:${rowId}`;

const isPermanentError = (error: SupabaseError): boolean =>
    error.status !== undefined && error.status >= 400 && error.status < 500;

const rowsAreEqual = (left: Row | undefined, right: Row | undefined): boolean =>
    JSON.stringify(left) === JSON.stringify(right);

const getRows = (content: Content, tableId: string): Table => content[0][tableId] ?? {};

const toRemote = (config: SupabaseTableConfig, rowId: string, row: Row): SupabaseRow => {
    const payload = config.toRemote?.(rowId, row) ?? { ...row };
    return {
        ...payload,
        [config.idColumn ?? 'id']: rowId,
    };
};

const fromRemote = (config: SupabaseTableConfig, remote: SupabaseRow): readonly [string, Row] => {
    if (config.fromRemote) {
        return config.fromRemote(remote);
    }

    const idColumn = config.idColumn ?? 'id';
    const deletedAtColumn = config.deletedAtColumn ?? 'deleted_at';
    const rowId = remote[idColumn];
    const row: Row = {};

    for (const [column, value] of Object.entries(remote)) {
        if (column !== idColumn && column !== deletedAtColumn) {
            row[column] = value as Row[string];
        }
    }

    return [String(rowId), row];
};

const sortOperations = (
    operations: readonly PendingOperation[],
    tables: Readonly<Record<string, SupabaseTableConfig>>,
): PendingOperation[] => {
    const depths = new Map<string, number>();
    const getDepth = (tableId: string): number => {
        const cached = depths.get(tableId);
        if (cached !== undefined) {
            return cached;
        }

        const dependencies = tables[tableId]?.dependsOn ?? [];
        const depth =
            dependencies.reduce(
                (maximum, dependency) => Math.max(maximum, getDepth(dependency)),
                0,
            ) + 1;
        depths.set(tableId, depth);
        return depth;
    };

    return [...operations].sort((left, right) => {
        const leftDepth = getDepth(left.tableId);
        const rightDepth = getDepth(right.tableId);
        const leftOrder = left.kind === 'tombstone' ? -leftDepth : leftDepth;
        const rightOrder = right.kind === 'tombstone' ? -rightDepth : rightDepth;
        return leftOrder - rightOrder || left.id.localeCompare(right.id);
    });
};

/**
 * Creates a browser-only TinyBase Store persister with durable IndexedDB state,
 * direct Supabase CRUD synchronization, and optional Realtime pull wake-ups.
 */
export const createSupabasePersister = async (
    store: Store,
    config: SupabasePersisterConfig,
): Promise<SupabasePersister> => {
    const state = await LocalState.open(config.databaseName, config.scopeKey);
    const client = config.supabase as SupabaseClient;
    const pageSize = config.pageSize ?? defaultPageSize;
    const tableConfigs = config.tables;
    let lastContent = (await state.getContent()) ?? store.getContent();
    let listener: ((content?: Content) => void) | undefined;
    let isDestroyed = false;
    let isSyncing = false;
    let interval: ReturnType<typeof setInterval> | undefined;
    const channels: SupabaseRealtimeChannel[] = [];
    const realtimeTimers = new Map<string, ReturnType<typeof setTimeout>>();
    const statusListeners = new Set<(status: SyncStatus) => void>();
    let status: SyncStatus = {
        pendingCount: (await state.getOperations()).length,
        phase: 'hydrating',
        rejectedCount: (await state.getRejected()).length,
    };

    const setStatus = async (phase: SyncPhase, error?: Error): Promise<void> => {
        status = {
            lastError: error,
            lastSuccessfulSyncAt: phase === 'idle' ? Date.now() : status.lastSuccessfulSyncAt,
            pendingCount: (await state.getOperations()).length,
            phase,
            rejectedCount: (await state.getRejected()).length,
        };
        for (const statusListener of statusListeners) {
            statusListener(status);
        }
    };

    const persistContent = async (
        content: Content,
        operations: readonly PendingOperation[],
    ): Promise<void> => {
        await state.persist(content, operations);
        lastContent = cloneContent(content);
    };

    const createOperations = (content: Content): PendingOperation[] => {
        const operations: PendingOperation[] = [];

        for (const [tableId, tableConfig] of Object.entries(tableConfigs)) {
            const previousRows = getRows(lastContent, tableId);
            const nextRows = getRows(content, tableId);
            const rowIds = new Set([...Object.keys(previousRows), ...Object.keys(nextRows)]);

            for (const rowId of rowIds) {
                const previous = previousRows[rowId];
                const next = nextRows[rowId];
                if (rowsAreEqual(previous, next)) {
                    continue;
                }

                if (next) {
                    operations.push({
                        id: operationId(tableId, rowId),
                        kind: 'upsert',
                        payload: toRemote(tableConfig, rowId, next),
                        rowId,
                        tableId,
                    });
                } else if (previous) {
                    const deletedAtColumn = tableConfig.deletedAtColumn ?? 'deleted_at';
                    operations.push({
                        id: operationId(tableId, rowId),
                        kind: 'tombstone',
                        payload: {
                            ...toRemote(tableConfig, rowId, previous),
                            [deletedAtColumn]: new Date().toISOString(),
                        },
                        rowId,
                        tableId,
                    });
                }
            }
        }

        return operations;
    };

    const applyRemoteContent = async (content: Content): Promise<void> => {
        await state.replaceContent(content);
        lastContent = cloneContent(content);
        listener?.(content);
    };

    const pullTable = async (tableId: string): Promise<void> => {
        const tableConfig = tableConfigs[tableId];
        if (!tableConfig) {
            return;
        }

        const rows: SupabaseRow[] = [];
        const idColumn = tableConfig.idColumn ?? 'id';
        let from = 0;

        while (true) {
            const response = await client
                .from(tableConfig.table)
                .select(tableConfig.select ?? '*')
                .order(idColumn)
                .range(from, from + pageSize - 1);
            if (response.error) {
                throw new Error(response.error.message);
            }

            const page = Array.isArray(response.data) ? (response.data as SupabaseRow[]) : [];
            rows.push(...page);
            if (page.length < pageSize) {
                break;
            }
            from += pageSize;
        }

        const content = cloneContent((await state.getContent()) ?? lastContent);
        const table: Table = { ...getRows(content, tableId) };
        const pendingIds = new Set((await state.getOperations()).map((operation) => operation.id));
        const seen = new Set<string>();
        const deletedAtColumn = tableConfig.deletedAtColumn ?? 'deleted_at';

        for (const remote of rows) {
            const [rowId, row] = fromRemote(tableConfig, remote);
            seen.add(rowId);
            if (pendingIds.has(operationId(tableId, rowId))) {
                continue;
            }
            if (remote[deletedAtColumn] !== null && remote[deletedAtColumn] !== undefined) {
                delete table[rowId];
            } else {
                table[rowId] = row;
            }
        }

        for (const rowId of Object.keys(table)) {
            if (!seen.has(rowId) && !pendingIds.has(operationId(tableId, rowId))) {
                delete table[rowId];
            }
        }

        const tables: Tables = { ...content[0], [tableId]: table };
        await applyRemoteContent([tables, content[1]]);
    };

    const flushOutbox = async (): Promise<void> => {
        const operations = sortOperations(await state.getOperations(), tableConfigs);
        for (const operation of operations) {
            const tableConfig = tableConfigs[operation.tableId];
            if (!tableConfig) {
                await state.removeOperation(operation.id);
                continue;
            }

            const response = await client
                .from(tableConfig.table)
                .upsert(operation.payload, { onConflict: tableConfig.idColumn ?? 'id' });
            if (!response.error) {
                await state.removeOperation(operation.id);
                continue;
            }

            if (isPermanentError(response.error)) {
                await state.reject(operation, response.error.message);
                continue;
            }

            throw new Error(response.error.message);
        }
    };

    const syncNow = async (): Promise<void> => {
        if (isDestroyed || isSyncing) {
            return;
        }

        isSyncing = true;
        await setStatus('syncing');
        try {
            await flushOutbox();
            for (const tableId of Object.keys(tableConfigs)) {
                await pullTable(tableId);
            }
            await setStatus('idle');
        } catch (error) {
            const normalized = asError(error);
            config.onError?.(normalized);
            await setStatus('offline', normalized);
        } finally {
            isSyncing = false;
        }
    };

    const schedulePull = (tableId: string): void => {
        const tableConfig = tableConfigs[tableId];
        const realtime = tableConfig?.realtime;
        const delay = typeof realtime === 'object' ? (realtime.debounceMs ?? 200) : 200;
        const previous = realtimeTimers.get(tableId);
        if (previous) {
            clearTimeout(previous);
        }
        realtimeTimers.set(
            tableId,
            setTimeout(() => {
                void pullTable(tableId).catch((error: unknown) => config.onError?.(asError(error)));
            }, delay),
        );
    };

    const startRealtime = (): void => {
        for (const [tableId, tableConfig] of Object.entries(tableConfigs)) {
            if (!tableConfig.realtime) {
                continue;
            }

            const realtime = tableConfig.realtime;
            const channelName =
                typeof realtime === 'object' && realtime.channelName
                    ? realtime.channelName
                    : `tinybase-supabase:${config.scopeKey}:${tableId}`;
            const channel = client
                .channel(channelName)
                .on(
                    'postgres_changes',
                    { event: '*', schema: 'public', table: tableConfig.table },
                    () => schedulePull(tableId),
                );
            channel.subscribe();
            channels.push(channel);
        }
    };

    const stopSyncing = async (): Promise<void> => {
        if (interval) {
            clearInterval(interval);
            interval = undefined;
        }
        for (const timer of realtimeTimers.values()) {
            clearTimeout(timer);
        }
        realtimeTimers.clear();
        for (const channel of channels.splice(0)) {
            client.removeChannel(channel);
        }
    };

    const startSyncing = async (): Promise<void> => {
        if (isDestroyed || interval) {
            return;
        }
        startRealtime();
        const pollIntervalMs = config.pollIntervalMs ?? defaultPollIntervalMs;
        if (pollIntervalMs > 0) {
            interval = setInterval(() => void syncNow(), pollIntervalMs);
        }
        await syncNow();
    };

    const basePersister = createCustomPersister(
        store,
        async () => state.getContent(),
        async (getContent) => {
            const content = getContent();
            const operations = createOperations(content);
            await persistContent(content, operations);
            if (operations.length > 0) {
                void syncNow();
            }
        },
        (nextListener) => {
            listener = nextListener;
            return undefined;
        },
        () => {
            listener = undefined;
        },
        config.onError,
    );

    const baseDestroy = basePersister.destroy.bind(basePersister);
    const baseStartAutoPersisting = basePersister.startAutoPersisting.bind(basePersister);
    let result: SupabasePersister;
    result = Object.assign(basePersister as Persister, {
        addSyncStatusListener(nextListener: (nextStatus: SyncStatus) => void): () => void {
            statusListeners.add(nextListener);
            nextListener(status);
            return () => statusListeners.delete(nextListener);
        },
        async discardRejected(): Promise<void> {
            await state.discardRejected();
            await setStatus(status.phase, status.lastError);
        },
        async destroy(): Promise<SupabasePersister> {
            isDestroyed = true;
            await stopSyncing();
            state.close();
            await baseDestroy();
            return result;
        },
        async getRejectedOperations(): Promise<readonly RejectedOperation[]> {
            return (await state.getRejected()).map(({ error, rowId, tableId }) => ({
                error,
                rowId,
                tableId,
            }));
        },
        getSyncStatus(): SyncStatus {
            return status;
        },
        async retryRejected(): Promise<void> {
            await state.retryRejected();
            await syncNow();
        },
        startSyncing,
        async startAutoPersisting(): Promise<SupabasePersister> {
            await baseStartAutoPersisting();
            await startSyncing();
            return result;
        },
        stopSyncing,
        syncNow,
    }) as SupabasePersister;

    await setStatus('idle');
    return result;
};
