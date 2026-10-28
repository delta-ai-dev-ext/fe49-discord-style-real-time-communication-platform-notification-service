import { beforeEach, describe, expect, it } from "bun:test";
import { Webhook } from "svix";

import { createApp } from "../../src/app.ts";
import { Lifecycle } from "../../src/lifecycle.ts";
import { registry } from "../../src/metrics.ts";
import { createWebhookDedupeStore } from "../../src/services/webhookDedupe.ts";
import { WebhookService } from "../../src/services/webhookService.ts";
import { createSvixVerifier } from "../../src/services/webhookVerifier.ts";
import {
  createFakeDb,
  createFakeState,
  createInMemoryDeliveryRepo,
  createInMemorySuppressionRepo,
  deliveryRow,
} from "../mocks/inMemory.ts";
import { silentLogger } from "../mocks/logger.ts";
import { webhookPayload } from "../mocks/fixtures.ts";

const SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
const NOW = new Date("2025-01-01T00:01:00.000Z");

function buildApp() {
  const state = createFakeState();
  state.deliveries.push(
    deliveryRow({ id: "delivery-1", providerMessageId: "resend-message-1", status: "sent" }),
  );

  const webhookService = new WebhookService({
    db: createFakeDb(state),
    verifier: createSvixVerifier(SECRET),
    dedupe: createWebhookDedupeStore(),
    deliveryRepo: createInMemoryDeliveryRepo(state),
    suppressionRepo: createInMemorySuppressionRepo(state),
    logger: silentLogger,
    now: () => NOW,
  });

  const app = createApp({
    lifecycle: new Lifecycle(),
    webhookService,
    checkDatabase: async () => true,
    kafkaEnabled: true,
  });

  return { app, state };
}

/** Sign the exact raw body with the official Svix SDK, as the provider would. */
function sign(body: string, messageId = "msg_1", timestamp = new Date()) {
  const signature = new Webhook(SECRET).sign(messageId, timestamp, body);
  return {
    "svix-id": messageId,
    "svix-timestamp": String(Math.floor(timestamp.getTime() / 1000)),
    "svix-signature": signature,
  };
}

function post(app: ReturnType<typeof buildApp>["app"], body: string, headers: Record<string, string>) {
  return app.handle(
    new Request("http://localhost/v1/webhooks/resend", {
      method: "POST",
      headers: { "content-type": "application/json", ...headers },
      body,
    }),
  );
}

beforeEach(() => {
  registry.resetMetrics();
});

describe("POST /v1/webhooks/resend (in-process)", () => {
  it("accepts a correctly signed delivery event and updates the record", async () => {
    const { app, state } = buildApp();
    const body = JSON.stringify(webhookPayload());

    const response = await post(app, body, sign(body));

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: "processed" });
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(state.deliveries[0]!.status).toBe("delivered");
  });

  it("returns 401 with the error contract when signature headers are missing", async () => {
    const { app, state } = buildApp();
    const body = JSON.stringify(webhookPayload());

    const response = await post(app, body, {});

    expect(response.status).toBe(401);
    const payload = (await response.json()) as { error: { code: string; message: string } };
    expect(payload.error.code).toBe("webhook_signature_invalid");
    expect(typeof payload.error.message).toBe("string");
    expect(state.deliveries[0]!.status).toBe("sent");
  });

  it("returns 401 for a forged signature", async () => {
    const { app, state } = buildApp();
    const body = JSON.stringify(webhookPayload());

    const response = await post(app, body, {
      "svix-id": "msg_forged",
      "svix-timestamp": "1735689600",
      "svix-signature": "v1,deadbeef",
    });

    expect(response.status).toBe(401);
    expect(state.deliveries[0]!.status).toBe("sent");
  });

  it("rejects a signature-invalid request before parsing the body", async () => {
    const { app } = buildApp();
    const response = await post(app, "{ definitely not json", {
      "svix-id": "msg_1",
      "svix-timestamp": "1735689600",
      "svix-signature": "v1,deadbeef",
    });

    // 401 (verification) wins over 400 (parsing) — proof of ordering.
    expect(response.status).toBe(401);
  });

  it("returns 400 for a malformed body with a valid signature", async () => {
    const { app } = buildApp();
    const body = "{ definitely not json";

    const response = await post(app, body, sign(body));

    expect(response.status).toBe(400);
    const payload = (await response.json()) as { error: { code: string } };
    expect(payload.error.code).toBe("invalid_request_body");
  });

  it("returns 422 for an unsupported event type", async () => {
    const { app } = buildApp();
    const body = JSON.stringify(webhookPayload({ type: "email.opened" }));

    const response = await post(app, body, sign(body));

    expect(response.status).toBe(422);
    const payload = (await response.json()) as { error: { code: string } };
    expect(payload.error.code).toBe("invalid_event_type");
  });

  it("accepts and ignores a duplicate Svix message id", async () => {
    const { app } = buildApp();
    const body = JSON.stringify(webhookPayload());

    const first = await post(app, body, sign(body, "msg_repeat"));
    const second = await post(app, body, sign(body, "msg_repeat"));

    expect(first.status).toBe(200);
    expect(await first.json()).toEqual({ status: "processed" });
    expect(second.status).toBe(200);
    expect(await second.json()).toEqual({ status: "ignored" });
  });

  it("accepts an unknown provider message id with 200 ignored", async () => {
    const { app } = buildApp();
    const body = JSON.stringify(webhookPayload({ data: { email_id: "resend-unknown" } }));

    const response = await post(app, body, sign(body));

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: "ignored" });
  });

  it("suppresses the address on a hard bounce and reuses it for later sends", async () => {
    const { app, state } = buildApp();
    const body = JSON.stringify(
      webhookPayload({
        type: "email.bounced",
        data: { email_id: "resend-message-1", to: ["Recipient@Example.com"], bounce: { type: "hard" } },
      }),
    );

    const response = await post(app, body, sign(body));

    expect(response.status).toBe(200);
    expect(state.suppressions).toHaveLength(1);
    expect(state.suppressions[0]).toMatchObject({ email: "recipient@example.com", reason: "hard_bounce" });
  });
});
