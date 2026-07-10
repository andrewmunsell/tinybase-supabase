import type { StoredCrdtUpdate } from '../crdt-storage.js';
import type { SupabaseRow } from '../types.js';
import { decodeBytea, encodeBytea, getDocumentKey } from './codec.js';
import type { ConfiguredCrdtTable } from './config.js';

export interface SupabaseError {
	readonly code?: string;
	readonly message: string;
	readonly status?: number;
}

export class CrdtTransportError extends Error implements SupabaseError {
	readonly code?: string;
	readonly status?: number;

	constructor(error: unknown) {
		const source = error as Partial<SupabaseError> | undefined;
		super(source?.message ?? String(error));
		this.name = 'CrdtTransportError';
		this.code = source?.code;
		this.status = source?.status;
	}
}

interface SupabaseResponse {
	readonly data: unknown;
	readonly error: SupabaseError | null;
}

interface SelectQuery extends PromiseLike<SupabaseResponse> {
	eq(column: string, value: string): SelectQuery;
	order(column: string): SelectQuery;
	range(from: number, to: number): SelectQuery;
}

interface RealtimeChannel {
	on(
		type: 'postgres_changes',
		filter: { event: '*'; filter?: string; schema: string; table: string },
		callback: () => void,
	): RealtimeChannel;
	subscribe(callback?: (status: string, error?: Error) => void): unknown;
}

interface SupabaseClient {
	channel(name: string): RealtimeChannel;
	from(table: string): {
		insert(value: SupabaseRow): Promise<SupabaseResponse>;
		select(columns: string): SelectQuery;
	};
	removeChannel(channel: RealtimeChannel): Promise<unknown> | unknown;
}

export type CrdtRealtimeChannel = RealtimeChannel;

export class CrdtTransport {
	readonly #client: SupabaseClient;
	readonly #pageSize: number;
	readonly #scopeKey: string;

	constructor(client: unknown, pageSize: number, scopeKey: string) {
		this.#client = client as SupabaseClient;
		this.#pageSize = pageSize;
		this.#scopeKey = scopeKey;
	}

	async fetchUpdates(
		tableId: string,
		rowId: string,
		config: ConfiguredCrdtTable,
	): Promise<StoredCrdtUpdate[]> {
		const updates: StoredCrdtUpdate[] = [];
		let from = 0;
		while (true) {
			let response: SupabaseResponse;
			try {
				response = await this.#client
					.from(config.crdtUpdatesTable)
					.select('id,update')
					.eq(config.crdtRowIdColumn ?? 'row_id', rowId)
					.order('created_at')
					.order('id')
					.range(from, from + this.#pageSize - 1);
			} catch (error) {
				throw new CrdtTransportError(error);
			}
			if (response.error) {
				throw new CrdtTransportError(response.error);
			}
			const rows = Array.isArray(response.data) ? (response.data as SupabaseRow[]) : [];
			updates.push(
				...rows.map((row) => ({
					documentKey: getDocumentKey(tableId, rowId),
					id: String(row.id),
					rowId,
					tableId,
					update: decodeBytea(row.update),
				})),
			);
			if (rows.length < this.#pageSize) {
				return updates;
			}
			from += this.#pageSize;
		}
	}

	async insertUpdate(update: StoredCrdtUpdate, config: ConfiguredCrdtTable): Promise<void> {
		const response = await this.#client.from(config.crdtUpdatesTable).insert({
			id: update.id,
			[config.crdtRowIdColumn ?? 'row_id']: update.rowId,
			update: encodeBytea(update.update),
		});
		if (!response.error) {
			return;
		}
		if (/duplicate|23505/i.test(`${response.error.code ?? ''} ${response.error.message}`)) {
			const rowIdColumn = config.crdtRowIdColumn ?? 'row_id';
			const existing = await this.#client
				.from(config.crdtUpdatesTable)
				.select(`id,${rowIdColumn},update`)
				.eq('id', update.id)
				.order('id')
				.range(0, 0);
			const row = Array.isArray(existing.data)
				? (existing.data[0] as SupabaseRow | undefined)
				: undefined;
			const stored = row ? decodeBytea(row.update) : undefined;
			if (
				!existing.error &&
				row !== undefined &&
				String(row[rowIdColumn]) === update.rowId &&
				stored?.length === update.update.length &&
				stored.every((byte, index) => byte === update.update[index])
			) {
				return;
			}
		}
		throw response.error;
	}

	async subscribe(
		tableId: string,
		rowId: string,
		config: ConfiguredCrdtTable,
		callback: () => void,
		signal?: AbortSignal,
	): Promise<CrdtRealtimeChannel> {
		const channel = this.#client
			.channel(`tinybase-supabase:crdt:${this.#scopeKey}:${tableId}:${rowId}`)
			.on(
				'postgres_changes',
				{
					event: '*',
					filter: `${config.crdtRowIdColumn ?? 'row_id'}=eq.${rowId}`,
					schema: 'public',
					table: config.crdtUpdatesTable,
				},
				callback,
			);
		try {
			await new Promise<void>((resolve, reject) => {
				const abort = (): void => {
					const error = new Error('CRDT Realtime subscription cancelled');
					error.name = 'AbortError';
					reject(error);
				};
				if (signal?.aborted) {
					abort();
					return;
				}
				signal?.addEventListener('abort', abort, { once: true });
				channel.subscribe((status, error) => {
					if (status === 'SUBSCRIBED') {
						signal?.removeEventListener('abort', abort);
						resolve();
					} else if (
						status === 'CHANNEL_ERROR' ||
						status === 'TIMED_OUT' ||
						status === 'CLOSED'
					) {
						signal?.removeEventListener('abort', abort);
						reject(error ?? new Error(`CRDT Realtime subscription failed: ${status}`));
					}
				});
			});
		} catch (error) {
			await this.#client.removeChannel(channel);
			throw error;
		}
		return channel;
	}

	async unsubscribe(channel: CrdtRealtimeChannel): Promise<void> {
		await this.#client.removeChannel(channel);
	}
}
