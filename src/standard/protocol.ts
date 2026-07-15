import type { SyncCursor } from '../storage.js';
import type { SupabaseRow, SupabaseTableConfig } from '../types.js';

export interface SupabaseError {
	readonly code?: string;
	readonly message: string;
	readonly status?: number;
}

interface SupabaseResponse {
	readonly data: unknown;
	readonly error: SupabaseError | null;
}

interface SelectQuery extends PromiseLike<SupabaseResponse> {
	eq(column: string, value: string): SelectQuery;
	gt(column: string, value: string): SelectQuery;
	gte(column: string, value: string): SelectQuery;
	limit(count: number): SelectQuery;
	order(column: string): SelectQuery;
}

export interface FetchRowsResult {
	readonly cursor: SyncCursor;
	readonly rows: SupabaseRow[];
}

interface RealtimeChannel {
	on(
		type: 'postgres_changes',
		filter: { event: '*'; schema: string; table: string },
		callback: () => void,
	): RealtimeChannel;
	subscribe(): unknown;
}

interface SupabaseClient {
	channel(name: string): RealtimeChannel;
	from(table: string): {
		select(columns: string): SelectQuery;
		upsert(values: SupabaseRow, options: { onConflict: string }): Promise<SupabaseResponse>;
	};
	removeChannel(channel: RealtimeChannel): Promise<unknown> | unknown;
}

export type StandardRealtimeChannel = RealtimeChannel;

const permanentPostgresErrorCodes = new Set(['22P02', '23503', '23505', '23514', '42501']);
const initialCursor: SyncCursor = { updatedAt: '1970-01-01T00:00:00.000000+00:00' };
export const isPermanentError = (error: SupabaseError): boolean =>
	(error.status !== undefined && error.status >= 400 && error.status < 500) ||
	(error.code !== undefined && permanentPostgresErrorCodes.has(error.code));

export class StandardTransport {
	readonly #client: SupabaseClient;
	readonly #pageSize: number;

	constructor(client: unknown, pageSize: number) {
		this.#client = client as SupabaseClient;
		this.#pageSize = pageSize;
	}

	async fetchRows(config: SupabaseTableConfig, cursor?: SyncCursor): Promise<FetchRowsResult> {
		const rows: SupabaseRow[] = [];
		const deletedAtColumn = config.deletedAtColumn ?? 'deleted_at';
		const idColumn = config.idColumn ?? 'id';
		const updatedAtColumn = config.updatedAtColumn ?? 'updated_at';
		const fetchPage = async (
			afterUpdatedAt?: string,
			equalUpdatedAt?: string,
			afterId?: string,
			atOrAfterUpdatedAt?: string,
		): Promise<SupabaseRow[]> => {
			let query = this.#client.from(config.table).select(config.select ?? '*');
			if (afterUpdatedAt) {
				query = query.gt(updatedAtColumn, afterUpdatedAt);
			}
			if (atOrAfterUpdatedAt) {
				query = query.gte(updatedAtColumn, atOrAfterUpdatedAt);
			}
			if (equalUpdatedAt) {
				query = query.eq(updatedAtColumn, equalUpdatedAt);
			}
			if (afterId) {
				query = query.gt(idColumn, afterId);
			}
			const response = await query
				.order(updatedAtColumn)
				.order(idColumn)
				.limit(this.#pageSize);
			if (response.error) {
				throw response.error;
			}
			const page = Array.isArray(response.data) ? (response.data as SupabaseRow[]) : [];
			for (const row of page) {
				if (
					typeof row[idColumn] !== 'string' ||
					!(deletedAtColumn in row) ||
					typeof row[updatedAtColumn] !== 'string' ||
					!Number.isFinite(Date.parse(row[updatedAtColumn]))
				) {
					throw new Error(
						`Table ${config.table} must select ${idColumn}, ${deletedAtColumn}, and ${updatedAtColumn}`,
					);
				}
			}
			return page;
		};

		const drainTimestamp = async (updatedAt: string, afterId?: string): Promise<void> => {
			let lastId = afterId;
			while (true) {
				const page = await fetchPage(undefined, updatedAt, lastId);
				rows.push(...page);
				if (page.length === 0) {
					return;
				}
				lastId = String(page.at(-1)?.[idColumn]);
			}
		};

		let page = await fetchPage(undefined, undefined, undefined, cursor?.updatedAt);
		while (page.length > 0) {
			rows.push(...page);
			const last = page.at(-1) as SupabaseRow;
			const afterUpdatedAt = String(last[updatedAtColumn]);
			await drainTimestamp(afterUpdatedAt, String(last[idColumn]));
			page = await fetchPage(afterUpdatedAt);
		}

		const last = rows.at(-1);
		return {
			cursor: last ? { updatedAt: String(last[updatedAtColumn]) } : (cursor ?? initialCursor),
			rows,
		};
	}

	async upsert(config: SupabaseTableConfig, payload: SupabaseRow): Promise<void> {
		const response = await this.#client
			.from(config.table)
			.upsert(payload, { onConflict: config.idColumn ?? 'id' });
		if (response.error) {
			throw response.error;
		}
	}

	subscribe(
		channelName: string,
		config: SupabaseTableConfig,
		callback: () => void,
	): StandardRealtimeChannel {
		const channel = this.#client
			.channel(channelName)
			.on(
				'postgres_changes',
				{ event: '*', schema: 'public', table: config.table },
				callback,
			);
		channel.subscribe();
		return channel;
	}

	async unsubscribe(channel: StandardRealtimeChannel): Promise<void> {
		await this.#client.removeChannel(channel);
	}
}
