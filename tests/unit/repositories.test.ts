import { describe, expect, it } from "bun:test";
import type { SQL } from "drizzle-orm";
import { PgDialect } from "drizzle-orm/pg-core";

import type { QueryResultLike, SqlExecutor } from "../../src/db/client.ts";
import { MIGRATION_STATEMENTS, runMigrations } from "../../src/db/migrations.ts";
import { createDailySendCounterRepository } from "../../src/repositories/dailySendCounter.repository.ts";
import { createEmailDeliveryRepository } from "../../src/repositories/emailDelivery.repository.ts";
import { createEmailSuppressionRepository } from "../../src/repositories/emailSuppression.repository.ts";

/**
 * The repositories are exercised against a recording SQL port: every statement
 * is captured as SQL text plus bound parameters, so the tests prove the queries
 * are parameterized and that results map onto the domain records.
 */

const dialect = new PgDialect();

type Captured = { sql: string; params: unknown[] };

function recorder(...responses: Record<string, unknown>[][]): { exec: SqlExecutor; captured: Captured[] } {
  const captured: Captured[] = [];
  let index = 0;
  const exec: SqlExecutor = {
    async execute(query: SQL): Promise<QueryResultLike> {
      const built = dialect.sqlToQuery(query);
      captured.push({ sql: built.sql, params: built.params as unknown[] });
      const rows = responses[index] ?? [];
      index += 1;
      return { rows };
    },
  };
  return { exec, captured };
}

const deliveryRow = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  id: "delivery-1",
  source_event_id: "11111111-1111-4111-8111-111111111111",
  kind: "friend_request",
  recipient_email: "user@example.com",
  sender_user_id: "sender-1",
  recipient_user_id: "recipient-1",
  invite_code: null,
  community_id: null,
  provider_message_id: "resend-1",
  status: "sent",
  failure_reason: null,
  attempts: 1,
  next_retry_at: null,
  sent_at: "2025-01-01T00:00:00.000Z",
  delivered_at: null,
  created_at: "2025-01-01T00:00:00.000Z",
  updated_at: "2025-01-01T00:00:01.000Z",
  ...overrides,
});

describe("emailDelivery repository", () => {
  const repo = createEmailDeliveryRepository();

  it("maps a row onto the domain record", async () => {
    const { exec } = recorder([deliveryRow()]);
    const record = await repo.findBySourceEventId(exec, "11111111-1111-4111-8111-111111111111");

    expect(record).not.toBeNull();
    expect(record!.kind).toBe("friend_request");
    expect(record!.status).toBe("sent");
    expect(record!.attempts).toBe(1);
    expect(record!.sentAt).toBeInstanceOf(Date);
    expect(record!.sentAt?.toISOString()).toBe("2025-01-01T00:00:00.000Z");
    expect(record!.nextRetryAt).toBeNull();
  });

  it("binds the event id as a parameter instead of interpolating it", async () => {
    const { exec, captured } = recorder([]);
    const evil = "' OR 1=1; DROP TABLE email_delivery; --";
    await repo.findBySourceEventId(exec, evil);

    expect(captured[0]!.params).toContain(evil);
    expect(captured[0]!.sql).not.toContain("DROP TABLE");
    expect(captured[0]!.sql).toContain("$1");
  });

  it("returns null when no row matches", async () => {
    const { exec } = recorder([]);
    expect(await repo.findBySourceEventId(exec, "missing")).toBeNull();
    expect(await repo.findById(exec, "missing")).toBeNull();
    expect(await repo.findByProviderMessageId(exec, "missing")).toBeNull();
  });

  it("returns null on a unique-constraint conflict during insert", async () => {
    const { exec, captured } = recorder([]);
    const result = await repo.insert(exec, {
      sourceEventId: "11111111-1111-4111-8111-111111111111",
      kind: "community_invite",
      recipientEmail: "user@example.com",
      status: "deferred",
      failureReason: "quota",
      nextRetryAt: new Date("2025-01-02T00:00:00.000Z"),
    });

    expect(result).toBeNull();
    expect(captured[0]!.sql).toContain("ON CONFLICT (source_event_id) DO NOTHING");
    expect(captured[0]!.params).toContain("deferred");
    expect(captured[0]!.params).toContain("quota");
  });

  it("inserts a queued record and returns it", async () => {
    const { exec } = recorder([deliveryRow({ status: "queued", provider_message_id: null, sent_at: null })]);
    const result = await repo.insert(exec, {
      sourceEventId: "11111111-1111-4111-8111-111111111111",
      kind: "friend_request",
      recipientEmail: "user@example.com",
    });
    expect(result?.status).toBe("queued");
  });

  it("applies status transitions with explicit SQL branches", async () => {
    const sent = recorder([deliveryRow({ status: "sent" })]);
    await repo.markSent(sent.exec, "delivery-1", "resend-9", new Date("2025-01-01T00:00:02.000Z"));
    expect(sent.captured[0]!.sql).toContain("status = 'sent'");
    expect(sent.captured[0]!.params).toContain("resend-9");
    expect(sent.captured[0]!.sql).toContain("attempts = attempts + 1");

    const attempt = recorder([deliveryRow({ status: "queued", attempts: 2 })]);
    const updated = await repo.markAttempt(attempt.exec, "delivery-1", new Date("2025-01-01T00:00:05.000Z"));
    expect(updated?.attempts).toBe(2);
    expect(attempt.captured[0]!.sql).toContain("attempts = attempts + 1");

    const failed = recorder([deliveryRow({ status: "failed", failure_reason: "permanent" })]);
    const failedRow = await repo.markFailed(failed.exec, "delivery-1", "permanent");
    expect(failedRow?.failureReason).toBe("permanent");
    expect(failed.captured[0]!.params).toContain("permanent");

    const suppressed = recorder([deliveryRow({ status: "suppressed" })]);
    expect((await repo.markSuppressed(suppressed.exec, "delivery-1"))?.status).toBe("suppressed");

    const deferred = recorder([deliveryRow({ status: "deferred", failure_reason: "quota" })]);
    const deferredRow = await repo.markDeferred(deferred.exec, "delivery-1", "quota", new Date());
    expect(deferredRow?.status).toBe("deferred");
    expect(deferred.captured[0]!.sql).toContain("status = 'deferred'");
  });

  it("never regresses a provider status", async () => {
    const sent = recorder([deliveryRow({ status: "sent" })]);
    await repo.applyProviderStatus(sent.exec, "delivery-1", "sent", new Date());
    expect(sent.captured[0]!.sql).toContain("CASE WHEN status IN ('delivered','bounced','complained')");

    const delivered = recorder([deliveryRow({ status: "delivered" })]);
    await repo.applyProviderStatus(delivered.exec, "delivery-1", "delivered", new Date());
    expect(delivered.captured[0]!.sql).toContain("delivered_at = COALESCE(delivered_at");

    const bounced = recorder([deliveryRow({ status: "bounced" })]);
    const bouncedRow = await repo.applyProviderStatus(bounced.exec, "delivery-1", "bounced", new Date());
    expect(bouncedRow?.status).toBe("bounced");

    const complained = recorder([deliveryRow({ status: "complained" })]);
    expect((await repo.applyProviderStatus(complained.exec, "delivery-1", "complained", new Date()))?.status).toBe(
      "complained",
    );
  });

  it("lists only due queued/deferred records", async () => {
    const { exec, captured } = recorder([
      deliveryRow({ status: "deferred", next_retry_at: "2025-01-01T00:00:00.000Z" }),
    ]);
    const rows = await repo.listDueForRetry(exec, new Date("2025-01-02T00:00:00.000Z"), 10);

    expect(rows).toHaveLength(1);
    expect(captured[0]!.sql).toContain("status IN ('queued', 'deferred')");
    expect(captured[0]!.params).toContain(10);
  });
});

describe("emailSuppression repository", () => {
  const repo = createEmailSuppressionRepository();

  it("finds a suppressed address", async () => {
    const { exec } = recorder([
      { id: "s1", email: "user@example.com", reason: "complaint", provider_message_id: "resend-1", created_at: new Date() },
    ]);
    const record = await repo.findByEmail(exec, "user@example.com");
    expect(record?.reason).toBe("complaint");
    expect(record?.providerMessageId).toBe("resend-1");
  });

  it("reports a first-time suppression as created", async () => {
    const { exec, captured } = recorder([
      { id: "s1", email: "user@example.com", reason: "hard_bounce", provider_message_id: "resend-1", created_at: new Date() },
    ]);
    const result = await repo.upsert(exec, { email: "user@example.com", reason: "hard_bounce" });

    expect(result.created).toBe(true);
    expect(captured[0]!.sql).toContain("ON CONFLICT (email) DO NOTHING");
  });

  it("keeps suppression upserts idempotent", async () => {
    const { exec } = recorder(
      [],
      [{ id: "s1", email: "user@example.com", reason: "complaint", provider_message_id: "resend-1", created_at: new Date() }],
    );
    const result = await repo.upsert(exec, { email: "user@example.com", reason: "complaint" });

    expect(result.created).toBe(false);
    expect(result.record.reason).toBe("complaint");
  });
});

describe("dailySendCounter repository", () => {
  const repo = createDailySendCounterRepository();

  it("reads a counter row", async () => {
    const { exec } = recorder([{ day: "2025-03-15", sent_count: 42, deferred_count: 7 }]);
    const record = await repo.findByDay(exec, "2025-03-15");
    expect(record).toEqual({ day: "2025-03-15", sentCount: 42, deferredCount: 7 });
  });

  it("returns null when the day has no row yet", async () => {
    const { exec } = recorder([]);
    expect(await repo.findByDay(exec, "2025-03-15")).toBeNull();
  });

  it("increments sent and deferred counts as upserts", async () => {
    const { exec, captured } = recorder([], []);
    await repo.incrementSent(exec, "2025-03-15");
    await repo.incrementDeferred(exec, "2025-03-15");

    expect(captured[0]!.sql).toContain("sent_count = daily_send_counter.sent_count + 1");
    expect(captured[0]!.params).toContain("2025-03-15");
    expect(captured[1]!.sql).toContain("deferred_count = daily_send_counter.deferred_count + 1");
  });

  it("sums the month range", async () => {
    const { exec, captured } = recorder([{ total: "1234" }]);
    const total = await repo.sumForRange(exec, "2025-03-01", "2025-03-31");

    expect(total).toBe(1234);
    expect(captured[0]!.params).toEqual(["2025-03-01", "2025-03-31"]);
  });

  it("treats a missing sum as zero", async () => {
    const { exec } = recorder([]);
    expect(await repo.sumForRange(exec, "2025-03-01", "2025-03-31")).toBe(0);
  });
});

describe("migrations", () => {
  it("applies the architecture §5 DDL idempotently", async () => {
    const { exec, captured } = recorder();
    await runMigrations(exec);

    expect(captured).toHaveLength(MIGRATION_STATEMENTS.length);
    expect(captured[0]!.sql).toContain("CREATE TABLE IF NOT EXISTS email_delivery");
    expect(captured[0]!.sql).toContain("source_event_id     UUID        NOT NULL UNIQUE");
    expect(captured.some((q) => q.sql.includes("status IN ('queued','sent','delivered'"))).toBe(true);
    expect(captured.some((q) => q.sql.includes("CREATE TABLE IF NOT EXISTS email_suppression"))).toBe(true);
    expect(captured.some((q) => q.sql.includes("CREATE TABLE IF NOT EXISTS daily_send_counter"))).toBe(true);
    expect(captured.some((q) => q.sql.includes("idx_email_delivery_status_retry"))).toBe(true);
  });
});
