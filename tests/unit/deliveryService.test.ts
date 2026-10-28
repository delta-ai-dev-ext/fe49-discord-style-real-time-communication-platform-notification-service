import { beforeEach, describe, expect, it } from "bun:test";

import { registry, type SuppressionReason } from "../../src/metrics.ts";
import { DeliveryService } from "../../src/services/deliveryService.ts";
import type { SendOutcome } from "../../src/services/emailProvider.ts";
import {
  createFakeDb,
  createFakeProvider,
  createFakeState,
  createInMemoryCounterRepo,
  createInMemoryDeliveryRepo,
  createInMemorySuppressionRepo,
} from "../mocks/inMemory.ts";
import { inviteEnvelope, friendRequestEnvelope, uuid } from "../mocks/fixtures.ts";
import { silentLogger } from "../mocks/logger.ts";
import { metricValue } from "../mocks/metrics.ts";

const NOW = new Date("2025-03-15T10:00:00.000Z");
const TODAY = "2025-03-15";

function setup(options: { script?: SendOutcome[]; now?: Date; maxAttempts?: number } = {}) {
  const state = createFakeState();
  const db = createFakeDb(state);
  const provider = createFakeProvider(options.script ?? []);
  const sleepCalls: number[] = [];
  let clock = options.now ?? NOW;

  const service = new DeliveryService({
    db,
    provider,
    config: {
      dailyLimit: 100,
      monthlyLimit: 3000,
      maxAttempts: options.maxAttempts ?? 3,
      backoffBaseMs: 10,
      backoffMaxMs: 40,
    },
    deliveryRepo: createInMemoryDeliveryRepo(state),
    suppressionRepo: createInMemorySuppressionRepo(state),
    counterRepo: createInMemoryCounterRepo(state),
    now: () => clock,
    sleep: async (ms) => {
      sleepCalls.push(ms);
      clock = new Date(clock.getTime() + ms);
    },
    logger: silentLogger,
  });

  return {
    state,
    provider,
    sleepCalls,
    service,
    clock: () => clock,
    setClock: (date: Date) => {
      clock = date;
    },
  };
}

beforeEach(() => {
  registry.resetMetrics();
});

describe("happy path (US-001, US-002)", () => {
  it("sends a friend-request email and records the sent transition", async () => {
    const { service, provider, state } = setup();
    const event = friendRequestEnvelope();

    const outcome = await service.processEnvelope(event);

    expect(outcome.status).toBe("sent");
    expect(provider.calls).toHaveLength(1);
    expect(provider.calls[0]!.to).toBe("recipient@example.com");
    expect(provider.calls[0]!.subject).toContain("Ada");
    expect(state.deliveries).toHaveLength(1);

    const record = state.deliveries[0]!;
    expect(record.status).toBe("sent");
    expect(record.providerMessageId).toBe("resend-1");
    expect(record.sentAt?.toISOString()).toBe(NOW.toISOString());
    expect(record.recipientEmail).toBe("recipient@example.com");
    expect(state.counters.get(TODAY)?.sentCount).toBe(1);
    expect(await metricValue("notification_emails_sent_total", { kind: "friend_request" })).toBe(1);
  });

  it("passes the Kafka event id as the Resend idempotency key (ADR-001)", async () => {
    const { service, provider } = setup();
    const eventId = uuid();
    await service.processEnvelope(friendRequestEnvelope({ event_id: eventId }));

    expect(provider.calls[0]!.idempotencyKey).toBe(eventId);
  });

  it("renders the community invitation with every required field", async () => {
    const { service, provider } = setup();
    await service.processEnvelope(inviteEnvelope());

    const call = provider.calls[0]!;
    expect(call.subject).toContain("Rustaceans");
    expect(call.html).toContain("Grace");
    expect(call.html).toContain("https://app.example.com/invites/abc123");
    expect(call.html).toContain("2999-01-01T00:00:00.000Z");
    expect(call.text).toContain("Rustaceans");
    expect(call.text).toContain("https://app.example.com/invites/abc123");
  });

  it("increments the daily counter inside the sent transaction (ADR-002)", async () => {
    const { service, state } = setup();
    await service.processEnvelope(friendRequestEnvelope());
    expect(state.transactions).toBe(1);
    expect(state.counters.get(TODAY)?.sentCount).toBe(1);
  });
});

describe("invalid and suppressed events (US-001, US-002)", () => {
  it("marks a missing recipient email as failed/invalid_event without sending", async () => {
    const { service, provider, state } = setup();
    const event = friendRequestEnvelope({ data: { sender_username: "Ada" } });

    const outcome = await service.processEnvelope(event);

    expect(outcome).toMatchObject({ status: "failed", reason: "invalid_event" });
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries[0]!.status).toBe("failed");
    expect(state.deliveries[0]!.failureReason).toBe("invalid_event");
    expect(state.counters.size).toBe(0);
  });

  it("marks an expired invitation as failed/invite_expired without sending", async () => {
    const { service, provider, state } = setup();
    const event = inviteEnvelope({
      data: {
        community_id: "c1",
        community_name: "Rustaceans",
        inviter_username: "Grace",
        recipient_email: "invitee@example.com",
        invite_url: "https://app.example.com/invites/abc",
        expires_at: "2020-01-01T00:00:00.000Z",
      },
    });

    const outcome = await service.processEnvelope(event);

    expect(outcome).toMatchObject({ status: "failed", reason: "invite_expired" });
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries[0]!.failureReason).toBe("invite_expired");
  });

  it("marks an unresolvable invite_url as failed without sending", async () => {
    const { service, provider, state } = setup();
    const event = inviteEnvelope({
      data: {
        community_name: "Rustaceans",
        inviter_username: "Grace",
        recipient_email: "invitee@example.com",
        invite_url: "",
        expires_at: "2999-01-01T00:00:00.000Z",
      },
    });

    const outcome = await service.processEnvelope(event);

    expect(outcome).toMatchObject({ status: "failed", reason: "invalid_event" });
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries[0]!.failureReason).toBe("invalid_event");
  });

  it("suppresses a recipient on the suppression list without calling the provider", async () => {
    const { service, provider, state } = setup();
    state.suppressions.push({
      id: "s1",
      email: "recipient@example.com",
      reason: "hard_bounce" as SuppressionReason,
      providerMessageId: "resend-old",
      createdAt: NOW,
    });

    const outcome = await service.processEnvelope(friendRequestEnvelope());

    expect(outcome).toMatchObject({ status: "suppressed", reason: "hard_bounce" });
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries[0]!.status).toBe("suppressed");
    expect(await metricValue("notification_emails_suppressed_total", { reason: "hard_bounce" })).toBe(1);
  });

  it("acknowledges non-email event types without side effects", async () => {
    const { service, provider, state } = setup();
    const outcome = await service.processEnvelope(
      friendRequestEnvelope({ event_type: "friend_request_accepted" }),
    );

    expect(outcome).toMatchObject({ status: "ignored", reason: "non_email_event_type" });
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries).toHaveLength(0);
  });

  it("acknowledges an unparseable envelope without side effects", async () => {
    const { service, provider, state } = setup();
    const outcome = await service.processEnvelope({ event_type: "friend_request_sent", data: {} });

    expect(outcome).toMatchObject({ status: "ignored", reason: "invalid_envelope" });
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries).toHaveLength(0);
  });
});

describe("exactly-once effect (ADR-001, US-003)", () => {
  it("acks a redelivered event that was already processed", async () => {
    const { service, provider, state } = setup();
    const event = friendRequestEnvelope();

    await service.processEnvelope(event);
    const second = await service.processEnvelope(event);

    expect(second).toMatchObject({ status: "duplicate", currentStatus: "sent" });
    expect(provider.calls).toHaveLength(1);
    expect(state.deliveries).toHaveLength(1);
    expect(state.counters.get(TODAY)?.sentCount).toBe(1);
  });

  it("re-issues the provider call with the same key after a crash before the record update", async () => {
    const { service, provider, state } = setup({
      script: [
        { ok: true, messageId: "resend-original" },
        { ok: true, messageId: "resend-original" },
      ],
    });
    const event = friendRequestEnvelope({ event_id: uuid("11111111-1111-4111-8111") });

    // Simulate "provider accepted, record update lost": the row exists as queued.
    state.deliveries.push({
      id: "delivery-crash",
      sourceEventId: event.event_id as string,
      kind: "friend_request",
      recipientEmail: "recipient@example.com",
      senderUserId: null,
      recipientUserId: null,
      inviteCode: null,
      communityId: null,
      providerMessageId: null,
      status: "queued",
      failureReason: null,
      attempts: 0,
      nextRetryAt: null,
      sentAt: null,
      deliveredAt: null,
      createdAt: NOW,
      updatedAt: NOW,
    });

    const outcome = await service.processEnvelope(event);

    expect(outcome.status).toBe("sent");
    expect(provider.calls).toHaveLength(1);
    expect(provider.calls[0]!.idempotencyKey).toBe(event.event_id as string);
    expect(state.deliveries).toHaveLength(1);
    expect(state.deliveries[0]!.providerMessageId).toBe("resend-original");
  });

  it("sends twice for two distinct events targeting the same recipient", async () => {
    const { service, provider, state } = setup();
    await service.processEnvelope(friendRequestEnvelope());
    await service.processEnvelope(friendRequestEnvelope());

    expect(provider.calls).toHaveLength(2);
    expect(state.deliveries).toHaveLength(2);
  });

  it("does not resend when a failed record is redelivered", async () => {
    const { service, provider, state } = setup({
      script: [{ ok: false, error: { kind: "permanent", code: "invalid_from_address", message: "nope" } }],
    });
    const event = friendRequestEnvelope();

    await service.processEnvelope(event);
    const again = await service.processEnvelope(event);

    expect(again).toMatchObject({ status: "duplicate", currentStatus: "failed" });
    expect(provider.calls).toHaveLength(1);
    expect(state.deliveries).toHaveLength(1);
  });
});

describe("quota-aware deferral (ADR-002, US-004)", () => {
  it("defers at the daily cap with reason quota and no provider call", async () => {
    const { service, provider, state } = setup();
    state.counters.set(TODAY, { sentCount: 100, deferredCount: 0 });

    const outcome = await service.processEnvelope(friendRequestEnvelope());

    expect(outcome).toMatchObject({ status: "deferred", reason: "quota" });
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries[0]!.status).toBe("deferred");
    expect(state.deliveries[0]!.failureReason).toBe("quota");
    expect(state.deliveries[0]!.nextRetryAt?.toISOString()).toBe("2025-03-16T00:00:00.000Z");
    expect(state.counters.get(TODAY)?.deferredCount).toBe(1);
    expect(await metricValue("notification_emails_deferred_total", { reason: "quota" })).toBe(1);
    expect(await metricValue("notification_daily_quota_used_ratio")).toBe(1);
  });

  it("defers until the next UTC month when the monthly cap is reached", async () => {
    const { service, provider, state } = setup();
    state.counters.set("2025-03-01", { sentCount: 3000, deferredCount: 0 });

    const outcome = await service.processEnvelope(inviteEnvelope());

    expect(outcome).toMatchObject({ status: "deferred", reason: "quota" });
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries[0]!.nextRetryAt?.toISOString()).toBe("2025-04-01T00:00:00.000Z");
  });

  it("defers when the provider itself reports exhausted quota", async () => {
    const { service, state } = setup({
      script: [{ ok: false, error: { kind: "quota", code: "daily_quota_exceeded", message: "cap" } }],
    });

    const outcome = await service.processEnvelope(friendRequestEnvelope());

    expect(outcome).toMatchObject({ status: "deferred", reason: "quota" });
    expect(state.deliveries[0]!.nextRetryAt?.toISOString()).toBe("2025-03-16T00:00:00.000Z");
  });
});

describe("provider failure handling (US-005)", () => {
  it("retries a transient error with exponential backoff and then succeeds", async () => {
    const { service, provider, state, sleepCalls } = setup({
      script: [
        { ok: false, error: { kind: "transient", code: "rate_limit_exceeded", message: "429", statusCode: 429 } },
        { ok: true, messageId: "resend-after-retry" },
      ],
    });

    const outcome = await service.processEnvelope(friendRequestEnvelope());

    expect(outcome.status).toBe("sent");
    expect(provider.calls).toHaveLength(2);
    expect(sleepCalls).toEqual([10]);
    expect(state.deliveries[0]!.attempts).toBe(2);
    expect(state.deliveries[0]!.providerMessageId).toBe("resend-after-retry");
  });

  it("dead-letters the record once retries are exhausted", async () => {
    const transient: SendOutcome = {
      ok: false,
      error: { kind: "transient", code: "internal_server_error", message: "500", statusCode: 500 },
    };
    const { service, provider, state, sleepCalls } = setup({
      script: [transient, transient, transient],
      maxAttempts: 3,
    });

    const outcome = await service.processEnvelope(friendRequestEnvelope());

    expect(outcome).toMatchObject({ status: "failed", reason: "retries_exhausted" });
    expect(provider.calls).toHaveLength(3);
    expect(sleepCalls).toEqual([10, 20]);
    expect(state.deliveries[0]!.status).toBe("failed");
    expect(state.deliveries[0]!.failureReason).toBe("retries_exhausted");
    expect(await metricValue("notification_emails_failed_total", { reason: "retries_exhausted" })).toBe(1);
  });

  it("fails immediately on a permanent provider error without retrying", async () => {
    const { service, provider, state } = setup({
      script: [{ ok: false, error: { kind: "permanent", code: "invalid_from_address", message: "bad from" } }],
    });

    const outcome = await service.processEnvelope(friendRequestEnvelope());

    expect(outcome).toMatchObject({ status: "failed", reason: "permanent" });
    expect(provider.calls).toHaveLength(1);
    expect(state.deliveries[0]!.failureReason).toBe("permanent");
  });
});

describe("deferred retry at rollover (US-004)", () => {
  it("retries a deferred record after the UTC day rolls over", async () => {
    const { service, provider, state, setClock } = setup();
    state.deliveries.push({
      id: "delivery-deferred",
      sourceEventId: uuid("22222222-2222-4222-8222"),
      kind: "community_invite",
      recipientEmail: "invitee@example.com",
      senderUserId: null,
      recipientUserId: null,
      inviteCode: "abc123",
      communityId: "community-1",
      providerMessageId: null,
      status: "deferred",
      failureReason: "quota",
      attempts: 0,
      nextRetryAt: new Date("2025-03-16T00:00:00.000Z"),
      sentAt: null,
      deliveredAt: null,
      createdAt: NOW,
      updatedAt: NOW,
    });

    setClock(new Date("2025-03-16T00:00:05.000Z"));
    const summary = await service.retryDue();

    expect(summary.sent).toBe(1);
    expect(provider.calls).toHaveLength(1);
    expect(state.deliveries[0]!.status).toBe("sent");
    expect(state.counters.get("2025-03-16")?.sentCount).toBe(1);
  });

  it("keeps deferring while the new day's cap is still exhausted", async () => {
    const { service, provider, state, setClock } = setup();
    state.deliveries.push({
      id: "delivery-deferred",
      sourceEventId: uuid("33333333-3333-4333-8333"),
      kind: "friend_request",
      recipientEmail: "recipient@example.com",
      senderUserId: null,
      recipientUserId: null,
      inviteCode: null,
      communityId: null,
      providerMessageId: null,
      status: "deferred",
      failureReason: "quota",
      attempts: 0,
      nextRetryAt: new Date("2025-03-16T00:00:00.000Z"),
      sentAt: null,
      deliveredAt: null,
      createdAt: NOW,
      updatedAt: NOW,
    });
    state.counters.set("2025-03-16", { sentCount: 100, deferredCount: 0 });

    setClock(new Date("2025-03-16T00:00:05.000Z"));
    const summary = await service.retryDue();

    expect(summary.deferred).toBe(1);
    expect(provider.calls).toHaveLength(0);
    expect(state.deliveries[0]!.status).toBe("deferred");
    expect(state.deliveries[0]!.nextRetryAt?.toISOString()).toBe("2025-03-17T00:00:00.000Z");
  });

  it("leaves records that are not due yet untouched", async () => {
    const { service, provider, state } = setup();
    state.deliveries.push({
      id: "delivery-future",
      sourceEventId: uuid("44444444-4444-4444-8444"),
      kind: "friend_request",
      recipientEmail: "recipient@example.com",
      senderUserId: null,
      recipientUserId: null,
      inviteCode: null,
      communityId: null,
      providerMessageId: null,
      status: "deferred",
      failureReason: "quota",
      attempts: 0,
      nextRetryAt: new Date("2025-04-01T00:00:00.000Z"),
      sentAt: null,
      deliveredAt: null,
      createdAt: NOW,
      updatedAt: NOW,
    });

    const summary = await service.retryDue();

    expect(summary.examined).toBe(0);
    expect(provider.calls).toHaveLength(0);
  });
});
