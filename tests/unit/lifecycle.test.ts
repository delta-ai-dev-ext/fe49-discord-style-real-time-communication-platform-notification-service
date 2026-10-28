import { describe, expect, it } from "bun:test";

import { Lifecycle, gracefulShutdown } from "../../src/lifecycle.ts";
import { silentLogger } from "../mocks/logger.ts";

describe("Lifecycle", () => {
  it("starts ready-ish with nothing assigned or in flight", () => {
    const lifecycle = new Lifecycle();
    expect(lifecycle.isShuttingDown()).toBe(false);
    expect(lifecycle.isKafkaAssigned()).toBe(false);
    expect(lifecycle.inFlightCount()).toBe(0);
  });

  it("tracks in-flight work and drains it", async () => {
    const lifecycle = new Lifecycle();
    let release: () => void = () => {};
    const work = new Promise<void>((resolve) => {
      release = resolve;
    });

    const tracked = lifecycle.track(async () => {
      await work;
      return "done";
    });

    expect(lifecycle.inFlightCount()).toBe(1);
    const draining = lifecycle.drain(1000);
    release();
    expect(await tracked).toBe("done");
    expect(await draining).toBe(true);
    expect(lifecycle.inFlightCount()).toBe(0);
  });

  it("returns immediately when nothing is in flight", async () => {
    const lifecycle = new Lifecycle();
    expect(await lifecycle.drain(10)).toBe(true);
  });

  it("gives up after the drain window elapses", async () => {
    const lifecycle = new Lifecycle();
    let release: () => void = () => {};
    const tracked = lifecycle.track(
      () =>
        new Promise<void>((resolve) => {
          release = resolve;
        }),
    );

    expect(await lifecycle.drain(5)).toBe(false);
    release();
    await tracked;
  });

  it("flips readiness flags on shutdown", () => {
    const lifecycle = new Lifecycle();
    lifecycle.setKafkaAssigned(true);
    lifecycle.beginShutdown();

    expect(lifecycle.isShuttingDown()).toBe(true);
    expect(lifecycle.isKafkaAssigned()).toBe(true);
  });
});

describe("gracefulShutdown", () => {
  function harness(inflightWork?: Promise<void>) {
    const order: string[] = [];
    const lifecycle = new Lifecycle();

    const result = gracefulShutdown({
      lifecycle,
      http: {
        stop: () => {
          order.push("http:stop");
          expect(lifecycle.isShuttingDown()).toBe(true);
        },
      },
      consumer: {
        stopFetching: () => {
          order.push("consumer:stopFetching");
        },
        close: () => {
          order.push("consumer:close");
          return inflightWork;
        },
      },
      scheduler: {
        stop: () => {
          order.push("scheduler:stop");
        },
      },
      pool: {
        end: () => {
          order.push("pool:end");
        },
      },
      timeoutMs: 5,
      logger: silentLogger,
      exit: (code) => order.push(`exit:${code}`),
    });

    return { result, order, lifecycle };
  }

  it("drains, closes and exits 0 in the documented order", async () => {
    const { result, order, lifecycle } = harness();

    expect(await result).toBe(0);
    expect(order).toEqual([
      "http:stop",
      "consumer:stopFetching",
      "scheduler:stop",
      "consumer:close",
      "pool:end",
      "exit:0",
    ]);
    expect(lifecycle.isShuttingDown()).toBe(true);
  });

  it("keeps going when a teardown step fails", async () => {
    const order: string[] = [];
    const code = await gracefulShutdown({
      lifecycle: new Lifecycle(),
      http: {
        stop: () => {
          throw new Error("http already stopped");
        },
      },
      consumer: {
        stopFetching: () => {},
        close: () => {
          throw new Error("consumer boom");
        },
      },
      scheduler: null,
      pool: {
        end: () => {
          order.push("pool:end");
        },
      },
      timeoutMs: 1,
      logger: silentLogger,
    });

    expect(code).toBe(0);
    expect(order).toEqual(["pool:end"]);
  });
});
