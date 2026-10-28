import { describe, expect, it } from "bun:test";

import { DeferredScheduler } from "../../src/services/deferredScheduler.ts";
import type { RetrySummary } from "../../src/services/deliveryService.ts";
import { silentLogger } from "../mocks/logger.ts";

const EMPTY: RetrySummary = { examined: 0, sent: 0, deferred: 0, failed: 0, skipped: 0 };

describe("DeferredScheduler", () => {
  it("runs a retry pass with the injected clock", async () => {
    const calls: Date[] = [];
    const scheduler = new DeferredScheduler({
      deliveryService: {
        async retryDue(now: Date = new Date()) {
          calls.push(now);
          return { ...EMPTY, examined: 2, sent: 2 };
        },
      },
      logger: silentLogger,
      now: () => new Date("2025-03-16T00:00:00.000Z"),
    });

    const summary = await scheduler.tick();

    expect(summary.sent).toBe(2);
    expect(calls[0]!.toISOString()).toBe("2025-03-16T00:00:00.000Z");
  });

  it("starts and stops its interval", async () => {
    const scheduler = new DeferredScheduler({
      deliveryService: {
        async retryDue() {
          return EMPTY;
        },
      },
      intervalMs: 5,
      logger: silentLogger,
    });

    scheduler.start();
    expect(scheduler.isRunning()).toBe(true);
    scheduler.start(); // idempotent
    await scheduler.stop();
    expect(scheduler.isRunning()).toBe(false);
    await scheduler.stop(); // idempotent
  });

  it("swallows pass failures so the interval keeps running", async () => {
    const scheduler = new DeferredScheduler({
      deliveryService: {
        async retryDue() {
          throw new Error("db down");
        },
      },
      intervalMs: 5,
      logger: silentLogger,
    });

    await expect(scheduler.tick()).rejects.toThrow("db down");
    scheduler.start();
    await Bun.sleep(15);
    await scheduler.stop();
    expect(scheduler.isRunning()).toBe(false);
  });
});
