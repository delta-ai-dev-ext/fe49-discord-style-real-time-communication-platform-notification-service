import { logger as defaultLogger, type AppLogger } from "../logger.ts";

import type { DeliveryService, RetrySummary } from "./deliveryService.ts";

export type DeferredSchedulerDeps = {
  deliveryService: Pick<DeliveryService, "retryDue">;
  intervalMs?: number;
  logger?: AppLogger;
  now?: () => Date;
};

export const DEFAULT_RETRY_INTERVAL_MS = 60_000;

/**
 * Background scheduler that retries deferred work at the UTC rollover
 * (ADR-002). It is a bounded, drainable task, like the Kafka consumer: the
 * shutdown path stops it before the DB pool closes.
 */
export class DeferredScheduler {
  private readonly deliveryService: Pick<DeliveryService, "retryDue">;
  private readonly intervalMs: number;
  private readonly logger: AppLogger;
  private readonly now: () => Date;
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;

  constructor(deps: DeferredSchedulerDeps) {
    this.deliveryService = deps.deliveryService;
    this.intervalMs = deps.intervalMs ?? DEFAULT_RETRY_INTERVAL_MS;
    this.logger = deps.logger ?? defaultLogger;
    this.now = deps.now ?? (() => new Date());
  }

  start(): void {
    if (this.timer) return;
    this.running = true;
    this.timer = setInterval(() => {
      void this.tick().catch((error: unknown) => {
        this.logger.error({
          operation: "deferred_scheduler",
          status: "failed",
          error: error instanceof Error ? error.message : String(error),
        });
      });
    }, this.intervalMs);
    // Never keep the process alive just for the scheduler.
    (this.timer as unknown as { unref?: () => void }).unref?.();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  isRunning(): boolean {
    return this.running;
  }

  /** One retry pass; exported so tests can drive it with an injected clock. */
  async tick(now: Date = this.now()): Promise<RetrySummary> {
    const summary = await this.deliveryService.retryDue(now);
    if (summary.examined > 0) {
      this.logger.info({
        operation: "deferred_scheduler",
        status: "pass",
        examined: summary.examined,
        sent: summary.sent,
        deferred: summary.deferred,
        failed: summary.failed,
      });
    }
    return summary;
  }
}
