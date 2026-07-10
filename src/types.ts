import type { Persister } from 'tinybase/persisters';
import type { Row } from 'tinybase';
import type * as Y from 'yjs';

export type SupabaseRow = Record<string, unknown>;

export type SyncPhase = 'hydrating' | 'offline' | 'syncing' | 'idle' | 'error';

export interface SyncStatus {
	readonly phase: SyncPhase;
	readonly lastError?: Error;
	readonly lastSuccessfulSyncAt?: number;
	readonly pendingCount: number;
	readonly rejectedCount: number;
}

export interface SupabaseTableConfig {
	/** Read-only tables are pulled but never enqueue local write operations. */
	readonly mode?: 'read-only' | 'read-write';
	/** The TinyBase table Id. It is the key of `tables` in the main config. */
	readonly table: string;
	/** Existing remotely-generated or application-generated primary key column. */
	readonly idColumn?: string;
	/** Existing nullable timestamp column used for durable soft deletes. */
	readonly deletedAtColumn?: string;
	/** Parent TinyBase tables which must be uploaded before this table. */
	readonly dependsOn?: readonly string[];
	/** Columns to select during reconciliation. Defaults to `*`. */
	readonly select?: string;
	/** Maps a TinyBase row to a row accepted by the Supabase table. */
	readonly toRemote?: (rowId: string, row: Row) => SupabaseRow;
	/** Maps a Supabase row back to a TinyBase row. */
	readonly fromRemote?: (row: SupabaseRow) => readonly [string, Row];
	/** Enables Postgres Changes as a debounced pull wake-up for this table. */
	readonly realtime?: boolean | RealtimeTableConfig;
	/** Optional collaborative Yjs cells. Omit or use an empty object for ordinary tables. */
	readonly crdtCells?: Readonly<Record<string, CrdtCellConfig>>;
	/** Append-only Y.Doc updates table. Required when `crdtCells` is non-empty. */
	readonly crdtUpdatesTable?: string;
	/** Foreign-key column in the updates table. Defaults to `row_id`. */
	readonly crdtRowIdColumn?: string;
}

export interface RealtimeTableConfig {
	readonly channelName?: string;
	readonly debounceMs?: number;
}

export interface SupabasePersisterConfig {
	/** A browser Supabase client created with a publishable/anon key. */
	readonly supabase: unknown;
	/** A stable name for the browser IndexedDB database. */
	readonly databaseName: string;
	/** A stable authenticated user or tenant identifier for local isolation. */
	readonly scopeKey: string;
	readonly tables: Readonly<Record<string, SupabaseTableConfig>>;
	/** Safety pull interval. Set to 0 to disable interval pulls. Defaults to 60 seconds. */
	readonly pollIntervalMs?: number;
	/** Maximum rows fetched per Supabase page. Defaults to 500. */
	readonly pageSize?: number;
	/** Initial retry delay after a transient network failure. Defaults to 1 second. */
	readonly retryBaseDelayMs?: number;
	/** Maximum retry delay after repeated transient failures. Defaults to 30 seconds. */
	readonly retryMaxDelayMs?: number;
	/** Called for ignored persistence, realtime, and synchronization errors. */
	readonly onError?: (error: Error) => void;
}

export interface RejectedOperation {
	readonly error: string;
	readonly rowId: string;
	readonly tableId: string;
}

export interface SupabasePersister extends Persister {
	getSyncStatus(): SyncStatus;
	addSyncStatusListener(listener: (status: SyncStatus) => void): () => void;
	getRejectedOperations(): Promise<readonly RejectedOperation[]>;
	retryRejected(): Promise<void>;
	discardRejected(): Promise<void>;
	syncNow(): Promise<void>;
	startSyncing(): Promise<void>;
	stopSyncing(): Promise<void>;
	openRow(tableId: string, rowId: string): Promise<CrdtRowHandle>;
	closeRow(tableId: string, rowId: string): Promise<void>;
	isRowOpen(tableId: string, rowId: string): boolean;
}

export type CrdtCellConfig =
	| { readonly type: 'array' }
	| { readonly type: 'map' }
	| { readonly type: 'text' };

export interface CrdtRowHandle {
	getArray<T = unknown>(cellId: string): Y.Array<T>;
	getMap<T = unknown>(cellId: string): Y.Map<T>;
	getText(cellId: string): Y.Text;
	destroy(): Promise<void>;
}
