export class SyncScheduler {
	readonly #onError?: (error: Error) => void;
	readonly #reconcile: () => Promise<void>;
	#browserListenersAttached = false;
	#deadline: number | undefined;
	#interval: ReturnType<typeof setInterval> | undefined;
	#promise: Promise<void> | undefined;
	#requested = false;
	#started = false;
	#timer: ReturnType<typeof setTimeout> | undefined;

	constructor(reconcile: () => Promise<void>, onError?: (error: Error) => void) {
		this.#reconcile = reconcile;
		this.#onError = onError;
	}

	#runInBackground(): void {
		void this.runNow().catch((error: unknown) =>
			this.#onError?.(error instanceof Error ? error : new Error(String(error))),
		);
	}

	#clearTimer(): void {
		if (this.#timer) {
			clearTimeout(this.#timer);
			this.#timer = undefined;
			this.#deadline = undefined;
		}
	}

	readonly #triggerOnline = (): void => {
		this.#runInBackground();
	};

	readonly #triggerVisible = (): void => {
		if (document.visibilityState === 'visible') {
			this.#runInBackground();
		}
	};

	runNow(): Promise<void> {
		this.#clearTimer();
		this.#requested = true;
		if (this.#promise) {
			return this.#promise;
		}
		this.#promise = (async () => {
			while (this.#requested) {
				this.#requested = false;
				await this.#reconcile();
			}
		})().finally(() => {
			this.#promise = undefined;
		});
		return this.#promise;
	}

	schedule(delayMs: number): void {
		const deadline = Date.now() + Math.max(0, delayMs);
		if (this.#deadline !== undefined && this.#deadline <= deadline) {
			return;
		}
		this.#clearTimer();
		this.#deadline = deadline;
		this.#timer = setTimeout(
			() => {
				this.#timer = undefined;
				this.#deadline = undefined;
				this.#runInBackground();
			},
			Math.max(0, deadline - Date.now()),
		);
	}

	async start(pollIntervalMs: number): Promise<void> {
		if (this.#started) {
			return;
		}
		this.#started = true;
		if (typeof window !== 'undefined') {
			window.addEventListener('online', this.#triggerOnline);
			document.addEventListener('visibilitychange', this.#triggerVisible);
			this.#browserListenersAttached = true;
		}
		if (pollIntervalMs > 0) {
			this.#interval = setInterval(() => this.#runInBackground(), pollIntervalMs);
		}
		await this.runNow();
	}

	stop(): void {
		this.#started = false;
		this.#clearTimer();
		if (this.#interval) {
			clearInterval(this.#interval);
			this.#interval = undefined;
		}
		if (this.#browserListenersAttached && typeof window !== 'undefined') {
			window.removeEventListener('online', this.#triggerOnline);
			document.removeEventListener('visibilitychange', this.#triggerVisible);
			this.#browserListenersAttached = false;
		}
	}
}
