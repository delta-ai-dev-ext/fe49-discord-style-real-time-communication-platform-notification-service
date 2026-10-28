import { logger as defaultLogger, type AppLogger } from "./logger.ts";

/**
 * Process lifecycle state shared by the probes, the Kafka consumer, and the
 * deferred-send scheduler.
 *
 * - `shuttingDown` flips readiness to 503 immediately on SIGTERM/SIGINT.
 * - in-flight work is tracked so shutdown can drain it within a bounded window.
 */
export class Lifecycle {
  private inflight = 0;
  private idleResolvers: (() => void)[] = [];
  private shuttingDown = false;
  private kafkaAssigned = false;
  private draining = false;

  isShuttingDown(): boolean {
    return this.shuttingDown;
  }

  isDraining(): boolean {
    return this.draining;
  }

  setKafkaAssigned(assigned: boolean): void {
    this.kafkaAssigned = assigned;
  }

  isKafkaAssigned(): boolean {
    return this.kafkaAssigned;
  }

  inFlightCount(): number {
    return this.inflight;
  }

  /** Marks shutdown start: readiness fails, new work must not be accepted. */
  beginShutdown(): void {
    this.shuttingDown = true;
  }

  /** Register a unit of in-flight work; the returned function must be called. */
  track<T>(work: () => Promise<T>): Promise<T> {
    this.inflight += 1;
    return work().finally(() => {
      this.inflight -= 1;
      if (this.inflight === 0) {
        const resolvers = this.idleResolvers;
        this.idleResolvers = [];
        for (const resolve of resolvers) resolve();
      }
    });
  }

  /** Wait until in-flight work is empty or the drain window elapses. */
  async drain(timeoutMs: number): Promise<boolean> {
    this.draining = true;
    if (this.inflight === 0) return true;

    let timer: ReturnType<typeof setTimeout> | undefined;
    const idle = new Promise<boolean>((resolve) => {
      this.idleResolvers.push(() => resolve(true));
      timer = setTimeout(() => resolve(false), Math.max(0, timeoutMs));
    });

    const drained = await idle;
    if (timer) clearTimeout(timer);
    return drained;
  }
}

export type ShutdownDeps = {
  lifecycle: Lifecycle;
  http: { stop(): Promise<void> | void };
  consumer: { stopFetching(): Promise<void> | void; close(): Promise<void> | void };
  scheduler: { stop(): Promise<void> | void } | null;
  pool: { end(): Promise<void> | void };
  timeoutMs: number;
  logger?: AppLogger;
  exit?: (code: number) => void;
};

/**
 * Graceful shutdown sequence (architecture §7):
 * readiness fails → stop HTTP intake and Kafka fetch → drain in-flight work
 * (~20s) → commit offsets and close the consumer → close the DB pool → exit 0.
 */
export async function gracefulShutdown(deps: ShutdownDeps): Promise<number> {
  const log = deps.logger ?? defaultLogger;

  deps.lifecycle.beginShutdown();
  log.info({ operation: "shutdown", status: "started" });

  await stopStep("http", log, async () => deps.http.stop());
  await stopStep("consumer_fetch", log, async () => deps.consumer.stopFetching());

  const drained = await deps.lifecycle.drain(deps.timeoutMs);
  if (!drained) {
    log.warn({ operation: "shutdown", status: "drain_timeout", inflight: deps.lifecycle.inFlightCount() });
  }

  await stopStep("scheduler_stop", log, async () => deps.scheduler?.stop());
  await stopStep("consumer_close", log, async () => deps.consumer.close());
  await stopStep("pool_close", log, async () => deps.pool.end());

  log.info({ operation: "shutdown", status: "complete", drained });
  deps.exit?.(0);
  return 0;
}

/** Runs one teardown step; a failure is logged and never blocks the sequence. */
async function stopStep(
  step: string,
  log: AppLogger,
  action: () => Promise<void> | void,
): Promise<void> {
  try {
    await action();
  } catch (error) {
    log.warn({ operation: "shutdown", status: `${step}_failed`, error: describe(error) });
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
