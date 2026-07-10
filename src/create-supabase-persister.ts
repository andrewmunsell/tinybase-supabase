import type { Store } from 'tinybase';
import { hasConfiguredCrdtCells } from './crdt/config.js';
import { createStandardPersister } from './create-standard-persister.js';
import { SyncScheduler } from './sync-scheduler.js';
import type { SupabasePersister, SupabasePersisterConfig } from './types.js';

/**
 * Creates the unified TinyBase/Supabase persister. Tables without configured
 * CRDT cells use only whole-row synchronization; collaborative tables add the
 * Yjs document layer and append-only update transport.
 */
export const createSupabasePersister = async (
	store: Store,
	config: SupabasePersisterConfig,
): Promise<SupabasePersister> => {
	if (hasConfiguredCrdtCells(config)) {
		const { createHybridPersister } = await import('./create-hybrid-persister.js');
		return createHybridPersister(store, config);
	}

	let standard: Awaited<ReturnType<typeof createStandardPersister>>;
	const scheduler = new SyncScheduler(async () => {
		await standard.reconcile();
	}, config.onError);
	standard = await createStandardPersister(store, config, scheduler);
	return Object.assign(standard, {
		async closeRow(): Promise<void> {},
		isRowOpen(): boolean {
			return false;
		},
		async openRow(): Promise<never> {
			throw new Error('This persister has no configured CRDT cells');
		},
	}) as unknown as SupabasePersister;
};
