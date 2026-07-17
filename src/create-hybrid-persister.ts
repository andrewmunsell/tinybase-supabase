import type { Store } from 'tinybase';
import { getConfiguredCrdtTables, getStandardConfig } from './crdt/config.js';
import { type CrdtCoordinator, createCrdtCoordinator } from './crdt/coordinator.js';
import { createShadowStoreBridge } from './crdt/shadow-store.js';
import { createStandardPersister } from './create-standard-persister.js';
import type { IndexedDbConnectionClosedForUpgradeError } from './indexeddb-errors.js';
import { SyncScheduler } from './sync-scheduler.js';
import type {
	RejectedOperation,
	SupabasePersister,
	SupabasePersisterConfig,
	SyncStatus,
} from './types.js';

/** Creates the whole-row persister with an optional Yjs coordinator layered on top. */
export const createHybridPersister = async (
	store: Store,
	config: SupabasePersisterConfig,
): Promise<SupabasePersister> => {
	const tables = getConfiguredCrdtTables(config);
	const reportError = (error: Error): void => config.onError?.(error);
	let coordinator: CrdtCoordinator | undefined;
	let standard: Awaited<ReturnType<typeof createStandardPersister>>;
	let result: SupabasePersister | undefined;
	let terminalError: IndexedDbConnectionClosedForUpgradeError | undefined;
	const statusListeners = new Set<(status: SyncStatus) => void>();
	const notifyStatusListener = (
		listener: (status: SyncStatus) => void,
		status: SyncStatus,
	): void => {
		try {
			listener(status);
		} catch (error) {
			try {
				reportError(error instanceof Error ? error : new Error(String(error)));
			} catch {}
		}
	};
	const emitStatus = (): void => {
		if (result) {
			const status = result.getSyncStatus();
			for (const listener of statusListeners) {
				notifyStatusListener(listener, status);
			}
		}
	};
	const scheduler = new SyncScheduler(async () => {
		if (!(await standard.reconcile(false))) {
			return;
		}
		try {
			await coordinator?.flushOutbox((tableId, rowId) =>
				standard.isRowBlocked(tableId, rowId),
			);
			await coordinator?.pullOpenDocuments();
			await standard.completeSync();
		} catch (error) {
			await standard.reportSyncError(error);
		}
	}, config.onError);

	const bridge = createShadowStoreBridge(
		store,
		config,
		(tableId, rowId) => coordinator?.getProjection(tableId, rowId),
		reportError,
	);
	const terminate = (error: IndexedDbConnectionClosedForUpgradeError): void => {
		if (terminalError) {
			return;
		}
		terminalError = error;
		bridge.destroy();
		coordinator?.terminate(error);
	};
	standard = await createStandardPersister(
		bridge.shadowStore,
		getStandardConfig(config),
		scheduler,
		terminate,
	);
	coordinator = await createCrdtCoordinator(
		store,
		config,
		tables,
		() => scheduler.runNow(),
		(delayMs) => scheduler.schedule(delayMs),
		emitStatus,
		(error) => {
			standard.terminate(error);
			terminate(error);
		},
	);
	if (terminalError) {
		coordinator.terminate(terminalError);
	}
	const removeStandardStatusListener = standard.addSyncStatusListener(emitStatus);
	result = Object.assign({}, standard, {
		addSyncStatusListener(listener: (status: SyncStatus) => void): () => void {
			statusListeners.add(listener);
			notifyStatusListener(listener, result?.getSyncStatus() ?? standard.getSyncStatus());
			return () => statusListeners.delete(listener);
		},
		closeRow: coordinator.closeRow,
		async destroy(): Promise<SupabasePersister> {
			removeStandardStatusListener();
			await coordinator?.destroy();
			bridge.destroy();
			await standard.destroy();
			return result as SupabasePersister;
		},
		async discardRejected(): Promise<void> {
			await Promise.all([standard.discardRejected(), coordinator?.discardRejected()]);
		},
		async getRejectedOperations(): Promise<readonly RejectedOperation[]> {
			const [ordinary, crdt] = await Promise.all([
				standard.getRejectedOperations(),
				coordinator?.getRejected() ?? [],
			]);
			return [...ordinary, ...crdt];
		},
		getSyncStatus(): SyncStatus {
			const status = standard.getSyncStatus();
			return {
				...status,
				pendingCount: status.pendingCount + (coordinator?.getPendingCount() ?? 0),
				rejectedCount: status.rejectedCount + (coordinator?.getRejectedCount() ?? 0),
			};
		},
		isRowOpen: coordinator.isRowOpen,
		openRow: coordinator.openRow,
		async retryRejected(): Promise<void> {
			await Promise.all([standard.retryRejected(), coordinator?.retryRejected()]);
			await scheduler.runNow();
		},
		async startAutoPersisting(): Promise<SupabasePersister> {
			await coordinator?.startSyncing();
			await standard.startAutoPersisting();
			return result as SupabasePersister;
		},
		async startSyncing(): Promise<void> {
			await coordinator?.startSyncing();
			await standard.startSyncing();
		},
		async stopSyncing(): Promise<void> {
			await coordinator?.stopSyncing();
			await standard.stopSyncing();
		},
		async syncNow(): Promise<void> {
			await coordinator?.flushBufferedUpdates();
			await scheduler.runNow();
		},
	}) as SupabasePersister;
	return result;
};
