import { jest } from '@jest/globals';
import { SyncScheduler } from '../../src/sync-scheduler.js';

describe('SyncScheduler', () => {
	afterEach(() => jest.useRealTimers());

	it('uses one timer and keeps the earliest requested realtime deadline', async () => {
		jest.useFakeTimers();
		const reconcile = jest.fn<() => Promise<void>>().mockResolvedValue();
		const scheduler = new SyncScheduler(reconcile);

		scheduler.schedule(200);
		scheduler.schedule(400);
		scheduler.schedule(50);
		await jest.advanceTimersByTimeAsync(49);
		expect(reconcile).not.toHaveBeenCalled();
		await jest.advanceTimersByTimeAsync(1);
		expect(reconcile).toHaveBeenCalledTimes(1);
		scheduler.stop();
	});

	it('coalesces concurrent requests and runs one follow-up pass for work arriving in flight', async () => {
		let releaseFirst: (() => void) | undefined;
		let calls = 0;
		const scheduler = new SyncScheduler(async () => {
			calls += 1;
			if (calls === 1) {
				await new Promise<void>((resolve) => {
					releaseFirst = resolve;
				});
			}
		});

		const first = scheduler.runNow();
		const second = scheduler.runNow();
		expect(first).toBe(second);
		releaseFirst?.();
		await first;
		expect(calls).toBe(2);
	});
});
