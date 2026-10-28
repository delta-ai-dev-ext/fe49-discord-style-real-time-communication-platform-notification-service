import { beforeEach, describe, expect, it } from "bun:test";

import { registry } from "../../src/metrics.ts";
import { WebhookService } from "../../src/services/webhookService.ts";
import { createWebhookDedupeStore } from "../../src/services/webhookDedupe.ts";
import type { WebhookVerifier } from "../../src/services/webhookVerifier.ts";
import {
  createFakeDb,
  createFakeState,
  createInMemoryDeliveryRepo,
  createInMemorySuppressionRepo,
} from "../mocks/inMemory.ts";
import { deliveryRow } from "../mocks/inMemory.ts";
import { svixHeaders, webhookPayload } from "../mocks/fixtures.ts";
import { silentLogger } from "../mocks/logger.ts";
import { metricValue } from "../mocks/metrics.ts";

const NOW = new Date("2025-01-01T00:00:10.000Z");

function setup(options: { signatureValid?: boolean } = {}) {
  const state = createFakeState();
  const verifierCalls: { rawBody: string }[] = [];
  const verifier: WebhookVerifier = {
    verify(rawBody) {
      verifierCalls.push({ rawBody });
      return options.signatureValid ?? true;
    },
  };

  state.deliveries.push(deliveryRow({ id: "delivery-1", providerMessageId: "resend-message-1", status: "sent" }));

  const service = new WebhookService({
    db: createFakeDb(state),
    verifier,
    dedupe: createWebhookDedupeStore(),
    deliveryRepo: createInMemoryDeliveryRepo(state),
    suppressionRepo: createInMemorySuppressionRepo(state),
    logger: silentLogger,
    now: () => NOW,
  });

  return { service, state, verifierCalls };
}

beforeEach(() => {
  registry.resetMetrics();
});

describe("signature verification before parsing (US-008)", () => {
  it("rejects missing signature headers with 401", async () => {
    const { service, verifierCalls } = setup();
    const response = await service.handle(JSON.stringify(webhookPayload()), {
      id: null,
      timestamp: null,
      signature: null,
    });

    expect(response.status).toBe(401);
    expect(response.body).toEqual({
      error: { code: "webhook_signature_invalid", message: expect.any(String) },
    });
    expect(verifierCalls).toHaveLength(0);
  });

  it("rejects an invalid signature with 401 before the body is parsed", async () => {
    const { service, state } = setup({ signatureValid: false });
    const response = await service.handle("{ not json at all", svixHeaders());

    // 401, not 400: verification happens before JSON parsing.
    expect(response.status).toBe(401);
    expect(state.deliveries[0]!.status).toBe("sent");
  });

  it("verifies the exact raw body bytes", async () => {
    const { service, verifierCalls } = setup();
    const raw = JSON.stringify(webhookPayload());
    await service.handle(raw, svixHeaders());
    expect(verifierCalls[0]!.rawBody).toBe(raw);
  });

  it("returns 400 for malformed JSON once the signature is valid", async () => {
    const { service } = setup();
    const response = await service.handle("{ not json at all", svixHeaders());

    expect(response.status).toBe(400);
    expect(response.body).toMatchObject({ error: { code: "invalid_request_body" } });
  });

  it("returns 422 for an unsupported event type", async () => {
    const { service } = setup();
    const response = await service.handle(
      JSON.stringify(webhookPayload({ type: "email.opened" })),
      svixHeaders(),
    );

    expect(response.status).toBe(422);
    expect(response.body).toMatchObject({ error: { code: "invalid_event_type" } });
  });

  it("returns 422 for a payload that fails schema validation", async () => {
    const { service } = setup();
    const response = await service.handle(
      JSON.stringify({ type: "email.delivered", created_at: "2025-01-01T00:00:00.000Z", data: {} }),
      svixHeaders(),
    );

    expect(response.status).toBe(422);
    expect(response.body).toMatchObject({ error: { code: "invalid_payload" } });
  });
});

describe("delivery status tracking (US-007)", () => {
  it("marks a delivery as delivered", async () => {
    const { service, state } = setup();
    const response = await service.handle(JSON.stringify(webhookPayload()), svixHeaders());

    expect(response).toEqual({ status: 200, body: { status: "processed" } });
    expect(state.deliveries[0]!.status).toBe("delivered");
    expect(state.deliveries[0]!.deliveredAt?.toISOString()).toBe("2025-01-01T00:00:05.000Z");
    expect(await metricValue("notification_emails_delivered_total", { kind: "friend_request" })).toBe(1);
  });

  it("marks a queued record as sent", async () => {
    const { service, state } = setup();
    state.deliveries[0]!.status = "queued";
    await service.handle(
      JSON.stringify(webhookPayload({ type: "email.sent" })),
      svixHeaders("msg_sent"),
    );

    expect(String(state.deliveries[0]!.status)).toBe("sent");
    expect(state.deliveries[0]!.sentAt?.toISOString()).toBe("2025-01-01T00:00:05.000Z");
  });

  it("accepts an unknown message id with 200 and ignores it", async () => {
    const { service, state } = setup();
    const response = await service.handle(
      JSON.stringify(webhookPayload({ data: { email_id: "resend-unknown" } })),
      svixHeaders(),
    );

    expect(response).toEqual({ status: 200, body: { status: "ignored" } });
    expect(state.deliveries[0]!.status).toBe("sent");
  });

  it("only applies a duplicate Svix message id once", async () => {
    const { service } = setup();
    const body = JSON.stringify(webhookPayload());

    const first = await service.handle(body, svixHeaders("msg_dup"));
    const second = await service.handle(body, svixHeaders("msg_dup"));

    expect(first.body).toEqual({ status: "processed" });
    expect(second.body).toEqual({ status: "ignored" });
    expect(await metricValue("notification_emails_delivered_total", { kind: "friend_request" })).toBe(1);
  });
});

describe("suppression on hard bounce and complaint (US-006)", () => {
  it("suppresses a hard-bounced address", async () => {
    const { service, state } = setup();
    await service.handle(
      JSON.stringify(
        webhookPayload({
          type: "email.bounced",
          data: { email_id: "resend-message-1", to: ["Bouncer@Example.com"], bounce: { type: "hard" } },
        }),
      ),
      svixHeaders(),
    );

    expect(state.deliveries[0]!.status).toBe("bounced");
    expect(state.suppressions).toHaveLength(1);
    expect(state.suppressions[0]!.email).toBe("bouncer@example.com");
    expect(state.suppressions[0]!.reason).toBe("hard_bounce");
    expect(await metricValue("notification_emails_bounced_total", { kind: "friend_request", type: "hard" })).toBe(1);
    expect(await metricValue("notification_emails_suppressed_total", { reason: "hard_bounce" })).toBe(1);
  });

  it("never suppresses a soft bounce", async () => {
    const { service, state } = setup();
    await service.handle(
      JSON.stringify(
        webhookPayload({
          type: "email.bounced",
          data: { email_id: "resend-message-1", bounce: { type: "soft", diagnostic_code: "mailbox full" } },
        }),
      ),
      svixHeaders(),
    );

    expect(state.deliveries[0]!.status).toBe("bounced");
    expect(state.suppressions).toHaveLength(0);
    expect(await metricValue("notification_emails_bounced_total", { kind: "friend_request", type: "soft" })).toBe(1);
  });

  it("suppresses a complaining address", async () => {
    const { service, state } = setup();
    await service.handle(
      JSON.stringify(webhookPayload({ type: "email.complained", data: { email_id: "resend-message-1" } })),
      svixHeaders(),
    );

    expect(state.deliveries[0]!.status).toBe("complained");
    expect(state.suppressions[0]!.reason).toBe("complaint");
    expect(await metricValue("notification_emails_complained_total", { kind: "friend_request" })).toBe(1);
    expect(await metricValue("notification_emails_suppressed_total", { reason: "complaint" })).toBe(1);
  });

  it("keeps suppression idempotent across repeated complaints", async () => {
    const { service, state } = setup();
    const body = JSON.stringify(webhookPayload({ type: "email.complained", data: { email_id: "resend-message-1" } }));

    await service.handle(body, svixHeaders("msg_a"));
    await service.handle(body, svixHeaders("msg_b"));

    expect(state.suppressions).toHaveLength(1);
    expect(await metricValue("notification_emails_suppressed_total", { reason: "complaint" })).toBe(1);
  });
});
