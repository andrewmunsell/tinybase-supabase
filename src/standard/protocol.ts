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
	order(column: string): SelectQuery;
	range(from: number, to: number): SelectQuery;
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

	async fetchRows(config: SupabaseTableConfig): Promise<SupabaseRow[]> {
		const rows: SupabaseRow[] = [];
		let from = 0;
		while (true) {
			const response = await this.#client
				.from(config.table)
				.select(config.select ?? '*')
				.order(config.idColumn ?? 'id')
				.range(from, from + this.#pageSize - 1);
			if (response.error) {
				throw response.error;
			}
			const page = Array.isArray(response.data) ? (response.data as SupabaseRow[]) : [];
			rows.push(...page);
			if (page.length < this.#pageSize) {
				return rows;
			}
			from += this.#pageSize;
		}
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
