import type { AppDb, QueryResultLike, SqlExecutor } from "../../src/db/client.ts";
import type { EmailStatus } from "../../src/db/schema.ts";
import type { EmailKind, SuppressionReason } from "../../src/metrics.ts";
import type {
  DeliveryRecord,
  EmailDeliveryRepository,
  InsertDeliveryInput,
} from "../../src/repositories/emailDelivery.repository.ts";
import type {
  EmailSuppressionRepository,
  SuppressionRecord,
} from "../../src/repositories/emailSuppression.repository.ts";
import type { DailySendCounterRepository } from "../../src/repositories/dailySendCounter.repository.ts";
import type { SendEmailInput, SendOutcome } from "../../src/services/emailProvider.ts";

/**
 * In-memory fakes that mirror the real database constraints (unique
 * `source_event_id`, PK on the UTC day, unique suppression address) and the
 * repository ports the services depend on. No SQL, no infrastructure.
 */

export type FakeSupplier = {
  deliveries: DeliveryRecord[];
  suppressions: SuppressionRecord[];
  counters: Map<string, { sentCount: number; deferredCount: number }>;
  transactions: number;
};

export function createFakeState(): FakeSupplier {
  return { deliveries: [], suppressions: [], counters: new Map(), transactions: 0 };
}

let sequence = 0;
function nextId(prefix = "id"): string {
  sequence += 1;
  return `${prefix}-${sequence}`;
}

/** Fake SQL port: records no SQL, supports transactions. */
export function createFakeDb(state: FakeSupplier): AppDb {
  const executor: SqlExecutor = {
    async execute(): Promise<QueryResultLike> {
      return { rows: [] };
    },
  };
  return {
    execute: (query) => executor.execute(query),
    async transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T> {
      state.transactions += 1;
      return fn(executor);
    },
  };
}

export function createInMemoryDeliveryRepo(state: FakeSupplier): EmailDeliveryRepository {
  const find = (id: string) => state.deliveries.find((row) => row.id === id) ?? null;

  return {
    async findBySourceEventId(_exec, sourceEventId) {
      return state.deliveries.find((row) => row.sourceEventId === sourceEventId) ?? null;
    },
    async findById(_exec, id) {
      return find(id);
    },
    async findByProviderMessageId(_exec, providerMessageId) {
      return state.deliveries.find((row) => row.providerMessageId === providerMessageId) ?? null;
    },
    async insert(_exec, input: InsertDeliveryInput) {
      // UNIQUE (source_event_id) — the Kafka dedupe constraint.
      if (state.deliveries.some((row) => row.sourceEventId === input.sourceEventId)) return null;
      const now = new Date();
      const row: DeliveryRecord = {
        id: nextId("delivery"),
        sourceEventId: input.sourceEventId,
        kind: input.kind,
        recipientEmail: input.recipientEmail,
        senderUserId: input.senderUserId ?? null,
        recipientUserId: input.recipientUserId ?? null,
        inviteCode: input.inviteCode ?? null,
        communityId: input.communityId ?? null,
        providerMessageId: null,
        status: input.status ?? "queued",
        failureReason: input.failureReason ?? null,
        attempts: 0,
        nextRetryAt: input.nextRetryAt ?? null,
        sentAt: null,
        deliveredAt: null,
        createdAt: now,
        updatedAt: now,
      };
      state.deliveries.push(row);
      return row;
    },
    async markSent(_exec, id, providerMessageId, sentAt) {
      const row = find(id);
      if (!row) return null;
      row.status = "sent";
      row.providerMessageId = providerMessageId;
      row.sentAt = sentAt;
      row.failureReason = null;
      row.nextRetryAt = null;
      row.attempts += 1;
      row.updatedAt = sentAt;
      return row;
    },
    async markAttempt(_exec, id, nextRetryAt) {
      const row = find(id);
      if (!row) return null;
      row.attempts += 1;
      row.nextRetryAt = nextRetryAt;
      return row;
    },
    async markFailed(_exec, id, reason) {
      const row = find(id);
      if (!row) return null;
      row.status = "failed";
      row.failureReason = reason;
      row.nextRetryAt = null;
      return row;
    },
    async markSuppressed(_exec, id) {
      const row = find(id);
      if (!row) return null;
      row.status = "suppressed";
      row.nextRetryAt = null;
      return row;
    },
    async markDeferred(_exec, id, reason, nextRetryAt) {
      const row = find(id);
      if (!row) return null;
      row.status = "deferred";
      row.failureReason = reason;
      row.nextRetryAt = nextRetryAt;
      return row;
    },
    async applyProviderStatus(_exec, id, status, at) {
      const row = find(id);
      if (!row) return null;
      const terminal = new Set(["bounced", "complained"]);
      if (status === "sent") {
        if (!row.status || !["delivered", "bounced", "complained"].includes(row.status)) row.status = "sent";
        row.sentAt = row.sentAt ?? at;
      } else if (status === "delivered") {
        if (!["bounced", "complained"].includes(row.status)) row.status = "delivered";
        row.deliveredAt = row.deliveredAt ?? at;
        row.sentAt = row.sentAt ?? at;
      } else if (terminal.has(status)) {
        row.status = status as EmailStatus;
      }
      return row;
    },
    async listDueForRetry(_exec, now, limit = 100) {
      return state.deliveries
        .filter(
          (row) =>
            (row.status === "queued" || row.status === "deferred") &&
            row.nextRetryAt !== null &&
            row.nextRetryAt.getTime() <= now.getTime(),
        )
        .sort((a, b) => (a.nextRetryAt?.getTime() ?? 0) - (b.nextRetryAt?.getTime() ?? 0))
        .slice(0, limit);
    },
  };
}

export function createInMemorySuppressionRepo(state: FakeSupplier): EmailSuppressionRepository {
  return {
    async findByEmail(_exec, email) {
      return state.suppressions.find((row) => row.email === email) ?? null;
    },
    async upsert(_exec, input) {
      const existing = state.suppressions.find((row) => row.email === input.email);
      if (existing) return { record: existing, created: false };
      const record: SuppressionRecord = {
        id: nextId("suppression"),
        email: input.email,
        reason: input.reason as SuppressionReason,
        providerMessageId: input.providerMessageId ?? null,
        createdAt: new Date(),
      };
      state.suppressions.push(record);
      return { record, created: true };
    },
  };
}

export function createInMemoryCounterRepo(state: FakeSupplier): DailySendCounterRepository {
  const entry = (day: string) => {
    const existing = state.counters.get(day);
    if (existing) return existing;
    const created = { sentCount: 0, deferredCount: 0 };
    state.counters.set(day, created);
    return created;
  };

  return {
    async findByDay(_exec, day) {
      const found = state.counters.get(day);
      if (!found) return null;
      return { day, sentCount: found.sentCount, deferredCount: found.deferredCount };
    },
    async incrementSent(_exec, day) {
      entry(day).sentCount += 1;
    },
    async incrementDeferred(_exec, day) {
      entry(day).deferredCount += 1;
    },
    async sumForRange(_exec, firstDay, lastDay) {
      let total = 0;
      for (const [day, value] of state.counters) {
        if (day >= firstDay && day <= lastDay) total += value.sentCount;
      }
      return total;
    },
  };
}

export type FakeProvider = {
  calls: SendEmailInput[];
  send(input: SendEmailInput): Promise<SendOutcome>;
};

/** Provider fake that replays a scripted list of outcomes (defaults to success). */
export function createFakeProvider(script: SendOutcome[] = []): FakeProvider {
  const calls: SendEmailInput[] = [];
  let index = 0;

  return {
    calls,
    async send(input) {
      calls.push(input);
      const outcome = script[index] ?? { ok: true, messageId: `resend-${calls.length}` };
      index += 1;
      return outcome;
    },
  };
}

export function deliveryRow(overrides: Partial<DeliveryRecord> = {}): DeliveryRecord {
  return {
    id: overrides.id ?? nextId("delivery"),
    sourceEventId: overrides.sourceEventId ?? nextId("event"),
    kind: overrides.kind ?? ("friend_request" as EmailKind),
    recipientEmail: overrides.recipientEmail ?? "recipient@example.com",
    senderUserId: overrides.senderUserId ?? null,
    recipientUserId: overrides.recipientUserId ?? null,
    inviteCode: overrides.inviteCode ?? null,
    communityId: overrides.communityId ?? null,
    providerMessageId: overrides.providerMessageId ?? null,
    status: overrides.status ?? "queued",
    failureReason: overrides.failureReason ?? null,
    attempts: overrides.attempts ?? 0,
    nextRetryAt: overrides.nextRetryAt ?? null,
    sentAt: overrides.sentAt ?? null,
    deliveredAt: overrides.deliveredAt ?? null,
    createdAt: overrides.createdAt ?? new Date(),
    updatedAt: overrides.updatedAt ?? new Date(),
  };
}
