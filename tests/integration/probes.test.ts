import { beforeEach, describe, expect, it } from "bun:test";

import { createApp } from "../../src/app.ts";
import { Lifecycle } from "../../src/lifecycle.ts";
import { registry } from "../../src/metrics.ts";
import { WebhookService } from "../../src/services/webhookService.ts";
import { createWebhookDedupeStore } from "../../src/services/webhookDedupe.ts";
import {
  createFakeDb,
  createFakeState,
  createInMemoryDeliveryRepo,
  createInMemorySuppressionRepo,
} from "../mocks/inMemory.ts";
import { silentLogger } from "../mocks/logger.ts";
import type { WebhookService as WebhookServiceType } from "../../src/services/webhookService.ts";

const noopWebhookService: Pick<WebhookServiceType, "handle"> = {
  async handle() {
    return { status: 200, body: { status: "ignored" } };
  },
};

function buildApp(options: {
  databaseOk?: () => Promise<boolean>;
  assigned?: boolean;
  shuttingDown?: boolean;
  kafkaEnabled?: boolean;
} = {}) {
  const lifecycle = new Lifecycle();
  if (options.assigned) lifecycle.setKafkaAssigned(true);
  if (options.shuttingDown) lifecycle.beginShutdown();

  const app = createApp({
    lifecycle,
    webhookService: noopWebhookService,
    checkDatabase: options.databaseOk ?? (async () => true),
    ...(options.kafkaEnabled === undefined ? {} : { kafkaEnabled: options.kafkaEnabled }),
  });

  return { app, lifecycle };
}

function get(app: ReturnType<typeof buildApp>["app"], path: string) {
  return app.handle(new Request(`http://localhost${path}`, { method: "GET" }));
}

beforeEach(() => {
  registry.resetMetrics();
});

describe("GET /healthz (US-009)", () => {
  it("reports alive even when the database is down and Kafka is unassigned", async () => {
    const { app } = buildApp({ databaseOk: async () => false, assigned: false });

    const response = await get(app, "/healthz");

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: "ok" });
  });

  it("reports alive while shutting down", async () => {
    const { app } = buildApp({ shuttingDown: true });
    const response = await get(app, "/healthz");
    expect(response.status).toBe(200);
  });
});

describe("GET /readyz (US-009)", () => {
  it("is ready when the pool is usable and the consumer is assigned", async () => {
    const { app } = buildApp({ assigned: true });

    const response = await get(app, "/readyz");

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      status: "ok",
      checks: { database: "ok", kafka_consumer: "assigned" },
    });
  });

  it("is not ready when the database is unavailable", async () => {
    const { app } = buildApp({ assigned: true, databaseOk: async () => false });

    const response = await get(app, "/readyz");

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      status: "unavailable",
      checks: { database: "unavailable", kafka_consumer: "assigned" },
    });
  });

  it("is not ready before the consumer is assigned", async () => {
    const { app } = buildApp({ assigned: false });

    const response = await get(app, "/readyz");

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      status: "unavailable",
      checks: { database: "ok", kafka_consumer: "unassigned" },
    });
  });

  it("is not ready once shutdown has started", async () => {
    const { app } = buildApp({ assigned: true, shuttingDown: true });
    const response = await get(app, "/readyz");
    expect(response.status).toBe(503);
  });

  it("treats a throwing database check as unavailable", async () => {
    const { app } = buildApp({
      assigned: true,
      databaseOk: async () => {
        throw new Error("pool closed");
      },
    });

    const response = await get(app, "/readyz");
    expect(response.status).toBe(503);
  });

  it("reports kafka_consumer as disabled when no consumer is wired", async () => {
    const { app } = buildApp({ assigned: false, kafkaEnabled: false });

    const response = await get(app, "/readyz");

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      status: "ok",
      checks: { database: "ok", kafka_consumer: "disabled" },
    });
  });
});

describe("GET /metrics", () => {
  it("exposes every architecture §8 metric", async () => {
    const { app } = buildApp({ assigned: true });

    const response = await get(app, "/metrics");
    const text = await response.text();

    expect(response.status).toBe(200);
    for (const name of [
      "notification_emails_sent_total",
      "notification_emails_delivered_total",
      "notification_emails_bounced_total",
      "notification_emails_complained_total",
      "notification_emails_suppressed_total",
      "notification_resend_api_duration_seconds",
      "notification_resend_api_errors_total",
      "notification_daily_quota_used_ratio",
      "notification_emails_deferred_total",
      "kafka_consumer_lag",
    ]) {
      expect(text).toContain(name);
    }
  });
});

describe("error contract", () => {
  it("returns the {error:{code,message}} shape for a rejected webhook", async () => {
    const state = createFakeState();
    const webhookService = new WebhookService({
      db: createFakeDb(state),
      verifier: { verify: () => false },
      dedupe: createWebhookDedupeStore(),
      deliveryRepo: createInMemoryDeliveryRepo(state),
      suppressionRepo: createInMemorySuppressionRepo(state),
      logger: silentLogger,
    });

    const app = createApp({
      lifecycle: new Lifecycle(),
      webhookService,
      checkDatabase: async () => true,
    });

    const response = await app.handle(
      new Request("http://localhost/v1/webhooks/resend", {
        method: "POST",
        headers: { "content-type": "application/json", "svix-id": "msg_1", "svix-timestamp": "1", "svix-signature": "v1,bad" },
        body: JSON.stringify({ nope: true }),
      }),
    );

    expect(response.status).toBe(401);
    const payload = (await response.json()) as { error?: { code?: string; message?: string } };
    expect(payload.error?.code).toBe("webhook_signature_invalid");
    expect(typeof payload.error?.message).toBe("string");
  });
});
