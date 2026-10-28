import type { AppDb } from "../db/client.ts";
import type { EmailStatus } from "../db/schema.ts";
import { logger as defaultLogger, sanitize, type AppLogger } from "../logger.ts";
import {
  emailsDeferredTotal,
  emailsFailedTotal,
  emailsSentTotal,
  emailsSuppressedTotal,
  setDailyQuotaRatio,
} from "../metrics.ts";
import {
  guessRecipientEmail,
  parseEmailEvent,
  type EmailEvent,
  type EventEnvelope,
  type FriendRequestSentData,
  type InviteEmailRequestedData,
} from "../models/events.ts";
import {
  createDailySendCounterRepository,
  type DailySendCounterRepository,
} from "../repositories/dailySendCounter.repository.ts";
import {
  createEmailDeliveryRepository,
  type DeliveryRecord,
  type EmailDeliveryRepository,
} from "../repositories/emailDelivery.repository.ts";
import {
  createEmailSuppressionRepository,
  type EmailSuppressionRepository,
} from "../repositories/emailSuppression.repository.ts";

import type { EmailProvider } from "./emailProvider.ts";
import {
  renderCommunityInviteEmail,
  renderDeferredRetryEmail,
  renderFriendRequestEmail,
  type RenderedEmail,
} from "./templates.ts";
import {
  BACKOFF_BASE_MS,
  BACKOFF_MAX_MS,
  backoffDelayMs,
  nextUtcDayStart,
  nextUtcMonthStart,
  parseTimestamp,
  utcDay,
  utcMonthRange,
} from "./time.ts";

export type ProcessOutcome =
  | { status: "sent"; deliveryId: string }
  | { status: "duplicate"; deliveryId: string; currentStatus: EmailStatus }
  | { status: "suppressed"; deliveryId: string; reason: string }
  | { status: "deferred"; deliveryId: string; reason: "quota" }
  | { status: "failed"; deliveryId: string; reason: string }
  | { status: "ignored"; reason: string };

export type DeliveryServiceDeps = {
  db: AppDb;
  provider: EmailProvider;
  config: {
    dailyLimit: number;
    monthlyLimit: number;
    maxAttempts: number;
    backoffBaseMs?: number;
    backoffMaxMs?: number;
  };
  logger?: AppLogger;
  now?: () => Date;
  sleep?: (ms: number) => Promise<void>;
  renderDeferred?: (record: DeliveryRecord) => RenderedEmail;
  deliveryRepo?: EmailDeliveryRepository;
  suppressionRepo?: EmailSuppressionRepository;
  counterRepo?: DailySendCounterRepository;
};

export type RetrySummary = {
  examined: number;
  sent: number;
  deferred: number;
  failed: number;
  skipped: number;
};

const noopSleep = async (ms: number): Promise<void> => {
  await Bun.sleep(ms);
};

/**
 * The email delivery pipeline (`docs/architecture.md` §3.2/§3.3):
 * dedupe → semantic checks → suppression → quota gate → render → send →
 * record outcome. The Kafka `event_id` is the single idempotency key used for
 * the dedupe constraint and the Resend idempotency key (ADR-001).
 */
export class DeliveryService {
  private readonly db: AppDb;
  private readonly provider: EmailProvider;
  private readonly deliveryRepo: EmailDeliveryRepository;
  private readonly suppressionRepo: EmailSuppressionRepository;
  private readonly counterRepo: DailySendCounterRepository;
  private readonly logger: AppLogger;
  private readonly now: () => Date;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly renderDeferred: (record: DeliveryRecord) => RenderedEmail;
  private readonly config: DeliveryServiceDeps["config"];

  constructor(deps: DeliveryServiceDeps) {
    this.db = deps.db;
    this.provider = deps.provider;
    this.deliveryRepo = deps.deliveryRepo ?? createEmailDeliveryRepository();
    this.suppressionRepo = deps.suppressionRepo ?? createEmailSuppressionRepository();
    this.counterRepo = deps.counterRepo ?? createDailySendCounterRepository();
    this.logger = deps.logger ?? defaultLogger;
    this.now = deps.now ?? (() => new Date());
    this.sleep = deps.sleep ?? noopSleep;
    this.renderDeferred = deps.renderDeferred ?? renderDeferredRetryEmail;
    this.config = deps.config;
  }

  /** Entry point used by the Kafka consumer for every message. */
  async processEnvelope(raw: unknown): Promise<ProcessOutcome> {
    const parsed = parseEmailEvent(raw);

    if (!parsed.ok && parsed.stage === "envelope") {
      // Nothing to persist: no usable event id. Ack so the partition keeps moving.
      this.logger.error({
        ...sanitize({ operation: "consume_event", status: "ignored", error: "invalid_envelope" }),
        issues: parsed.error.issues.map((issue) => issue.path.join(".")),
      });
      return { status: "ignored", reason: "invalid_envelope" };
    }

    if (!parsed.ok) {
      if (parsed.kind === null) {
        // Not an email-producing event type: acknowledge without side effects.
        this.logger.debug({
          operation: "consume_event",
          status: "ignored",
          reason: "non_email_event_type",
          event_type: parsed.envelope.event_type,
          trace_id: parsed.envelope.trace_parent,
        });
        return { status: "ignored", reason: "non_email_event_type" };
      }

      return this.recordInvalidEvent(parsed.envelope, parsed.kind, parsed.error.issues);
    }

    return this.processEvent(parsed.event);
  }

  /** Full happy-path/failure-path pipeline for a recognised email event. */
  async processEvent(event: EmailEvent): Promise<ProcessOutcome> {
    const now = this.now();
    const traceId = event.envelope.trace_parent;
    const log = this.logger.child({
      operation: "send_email",
      kind: event.kind,
      event_id: event.envelope.event_id,
      ...(traceId ? { trace_id: traceId } : {}),
    });

    // 1. Kafka-level dedupe: the row is the ledger.
    const existing = await this.deliveryRepo.findBySourceEventId(this.db, event.envelope.event_id);
    if (existing) {
      return this.handleExisting(existing, event, log);
    }

    // 2. Semantic validation of the event payload.
    if (event.kind === "community_invite") {
      const expiresAt = parseTimestamp(event.data.expires_at);
      if (!expiresAt || expiresAt.getTime() <= now.getTime()) {
        return this.recordTerminalFailure(
          event,
          "invite_expired",
          log,
        );
      }
    }

    // 3. Recipient suppression.
    const suppression = await this.suppressionRepo.findByEmail(this.db, event.recipientEmail);
    if (suppression) {
      const inserted = await this.insertRecord(event, {
        status: "suppressed",
        failureReason: null,
      });
      if (!inserted) return this.alreadyRecorded(event.envelope.event_id);
      emailsSuppressedTotal.inc({ reason: suppression.reason });
      log.info(sanitize({ status: "suppressed", suppression_reason: suppression.reason }));
      return { status: "suppressed", deliveryId: inserted.id, reason: suppression.reason };
    }

    // 4. Quota gate — defer, never drop.
    const gate = await this.checkQuota(now);
    if (!gate.allowed) {
      const inserted = await this.insertRecord(event, {
        status: "deferred",
        failureReason: "quota",
        nextRetryAt: gate.nextRetryAt,
      });
      if (!inserted) return this.alreadyRecorded(event.envelope.event_id);
      await this.counterRepo.incrementDeferred(this.db, utcDay(now));
      emailsDeferredTotal.inc({ reason: "quota" });
      log.warn(sanitize({ status: "deferred", failure_reason: "quota", sent_today: gate.sentToday }));
      return { status: "deferred", deliveryId: inserted.id, reason: "quota" };
    }

    // 5. Render, insert the `queued` row, then send.
    const rendered = this.renderForEvent(event);
    const inserted = await this.insertRecord(event, { status: "queued" });
    if (!inserted) return this.alreadyRecorded(event.envelope.event_id);

    return this.attemptSend(inserted, rendered, log, traceId);
  }

  /**
   * Retries records whose `next_retry_at` has elapsed (deferred work at the UTC
   * rollover, plus queued work orphaned by a crash). Called by the scheduler.
   */
  async retryDue(now: Date = this.now(), limit = 100): Promise<RetrySummary> {
    const due = await this.deliveryRepo.listDueForRetry(this.db, now, limit);
    const summary: RetrySummary = { examined: due.length, sent: 0, deferred: 0, failed: 0, skipped: 0 };

    for (const record of due) {
      const log = this.logger.child({ operation: "retry_delivery", kind: record.kind, delivery_id: record.id });

      const gate = await this.checkQuota(now);
      if (!gate.allowed) {
        await this.deliveryRepo.markDeferred(this.db, record.id, "quota", gate.nextRetryAt);
        emailsDeferredTotal.inc({ reason: "quota" });
        summary.deferred += 1;
        log.warn(sanitize({ status: "deferred", failure_reason: "quota" }));
        continue;
      }

      const outcome = await this.attemptSend(record, this.renderDeferred(record), log);
      if (outcome.status === "sent") summary.sent += 1;
      else if (outcome.status === "deferred") summary.deferred += 1;
      else if (outcome.status === "failed") summary.failed += 1;
      else summary.skipped += 1;
    }

    return summary;
  }

  private async handleExisting(
    existing: DeliveryRecord,
    event: EmailEvent,
    log: AppLogger,
  ): Promise<ProcessOutcome> {
    if (existing.status !== "queued") {
      log.info(sanitize({ status: "duplicate", current_status: existing.status }));
      return { status: "duplicate", deliveryId: existing.id, currentStatus: existing.status };
    }

    // Crash between "provider accepted" and "record updated": re-issue with the
    // same idempotency key so the provider returns the original message id.
    log.warn(sanitize({ status: "redelivery_retry", attempts: existing.attempts }));
    return this.attemptSend(existing, this.renderForEvent(event), log, event.envelope.trace_parent);
  }

  private async attemptSend(
    startRecord: DeliveryRecord,
    rendered: RenderedEmail,
    log: AppLogger,
    traceId?: string,
  ): Promise<ProcessOutcome> {
    let record = startRecord;
    const baseMs = this.config.backoffBaseMs ?? BACKOFF_BASE_MS;
    const maxMs = this.config.backoffMaxMs ?? BACKOFF_MAX_MS;

    for (;;) {
      const outcome = await this.provider.send({
        to: record.recipientEmail,
        subject: rendered.subject,
        html: rendered.html,
        text: rendered.text,
        idempotencyKey: record.sourceEventId,
        traceId,
      });

      if (outcome.ok) {
        const sentAt = this.now();
        await this.db.transaction(async (tx) => {
          await this.deliveryRepo.markSent(tx, record.id, outcome.messageId, sentAt);
          await this.counterRepo.incrementSent(tx, utcDay(sentAt));
        });
        emailsSentTotal.inc({ kind: record.kind });

        const counter = await this.counterRepo.findByDay(this.db, utcDay(sentAt));
        setDailyQuotaRatio(counter?.sentCount ?? 0, this.config.dailyLimit);

        log.info(sanitize({ status: "sent", attempts: record.attempts + 1 }));
        return { status: "sent", deliveryId: record.id };
      }

      if (outcome.error.kind === "quota") {
        const nextRetryAt = nextUtcDayStart(this.now());
        await this.deliveryRepo.markDeferred(this.db, record.id, "quota", nextRetryAt);
        emailsDeferredTotal.inc({ reason: "quota" });
        log.warn(sanitize({ status: "deferred", failure_reason: "quota", provider_code: outcome.error.code }));
        return { status: "deferred", deliveryId: record.id, reason: "quota" };
      }

      if (outcome.error.kind === "permanent") {
        await this.deliveryRepo.markFailed(this.db, record.id, "permanent");
        emailsFailedTotal.inc({ reason: "permanent" });
        log.error(sanitize({ status: "failed", failure_reason: "permanent", provider_code: outcome.error.code }));
        return { status: "failed", deliveryId: record.id, reason: "permanent" };
      }

      // Transient: increment attempts, arm exponential backoff, retry until the cap.
      const delayMs = backoffDelayMs(record.attempts + 1, baseMs, maxMs);
      const nextRetryAt = new Date(this.now().getTime() + delayMs);
      const updated = await this.deliveryRepo.markAttempt(this.db, record.id, nextRetryAt);
      record = updated ?? record;
      emailsDeferredTotal.inc({ reason: "transient" });

      if (record.attempts >= this.config.maxAttempts) {
        await this.deliveryRepo.markFailed(this.db, record.id, "retries_exhausted");
        emailsFailedTotal.inc({ reason: "retries_exhausted" });
        log.error(
          sanitize({
            status: "failed",
            failure_reason: "retries_exhausted",
            attempts: record.attempts,
            provider_code: outcome.error.code,
          }),
        );
        return { status: "failed", deliveryId: record.id, reason: "retries_exhausted" };
      }

      log.warn(
        sanitize({
          status: "retry_scheduled",
          attempts: record.attempts,
          backoff_ms: delayMs,
          provider_code: outcome.error.code,
        }),
      );
      await this.sleep(delayMs);
    }
  }

  private async checkQuota(
    now: Date,
  ): Promise<{ allowed: boolean; nextRetryAt: Date; sentToday: number }> {
    const counter = await this.counterRepo.findByDay(this.db, utcDay(now));
    const sentToday = counter?.sentCount ?? 0;
    setDailyQuotaRatio(sentToday, this.config.dailyLimit);

    if (this.config.dailyLimit > 0 && sentToday >= this.config.dailyLimit) {
      return { allowed: false, nextRetryAt: nextUtcDayStart(now), sentToday };
    }

    if (this.config.monthlyLimit > 0) {
      const { firstDay, lastDay } = utcMonthRange(now);
      const monthTotal = await this.counterRepo.sumForRange(this.db, firstDay, lastDay);
      if (monthTotal >= this.config.monthlyLimit) {
        return { allowed: false, nextRetryAt: nextUtcMonthStart(now), sentToday };
      }
    }

    return { allowed: true, nextRetryAt: nextUtcDayStart(now), sentToday };
  }

  private renderForEvent(event: EmailEvent): RenderedEmail {
    if (event.kind === "friend_request") {
      return renderFriendRequestEmail(event.data as FriendRequestSentData);
    }
    return renderCommunityInviteEmail(event.data as InviteEmailRequestedData);
  }

  private insertRecord(
    event: EmailEvent,
    overrides: { status: EmailStatus; failureReason?: string | null; nextRetryAt?: Date | null },
  ): Promise<DeliveryRecord | null> {
    const base = {
      sourceEventId: event.envelope.event_id,
      kind: event.kind,
      recipientEmail: event.recipientEmail,
      status: overrides.status,
      failureReason: overrides.failureReason ?? null,
      nextRetryAt: overrides.nextRetryAt ?? null,
    };

    if (event.kind === "friend_request") {
      return this.deliveryRepo.insert(this.db, {
        ...base,
        senderUserId: event.data.sender_user_id ?? null,
        recipientUserId: event.data.recipient_user_id ?? null,
      });
    }

    return this.deliveryRepo.insert(this.db, {
      ...base,
      inviteCode: event.data.invite_code ?? null,
      communityId: event.data.community_id ?? null,
    });
  }

  private async alreadyRecorded(sourceEventId: string): Promise<ProcessOutcome> {
    const current = await this.deliveryRepo.findBySourceEventId(this.db, sourceEventId);
    return {
      status: "duplicate",
      deliveryId: current?.id ?? "",
      currentStatus: current?.status ?? "queued",
    };
  }

  private async recordTerminalFailure(
    event: EmailEvent,
    reason: string,
    log: AppLogger,
  ): Promise<ProcessOutcome> {
    const inserted = await this.insertRecord(event, { status: "failed", failureReason: reason });
    if (!inserted) return this.alreadyRecorded(event.envelope.event_id);
    emailsFailedTotal.inc({ reason });
    log.warn(sanitize({ status: "failed", failure_reason: reason }));
    return { status: "failed", deliveryId: inserted.id, reason };
  }

  /** Invalid `data` payloads still produce a `failed/invalid_event` ledger row. */
  private async recordInvalidEvent(
    envelope: EventEnvelope,
    kind: string,
    issues: { path: PropertyKey[]; message: string }[],
  ): Promise<ProcessOutcome> {
    const log = this.logger.child({
      operation: "send_email",
      kind,
      event_id: envelope.event_id,
      ...(envelope.trace_parent ? { trace_id: envelope.trace_parent } : {}),
    });

    const inserted = await this.deliveryRepo.insert(this.db, {
      sourceEventId: envelope.event_id,
      kind: kind as "friend_request" | "community_invite",
      recipientEmail: guessRecipientEmail(envelope),
      status: "failed",
      failureReason: "invalid_event",
    });

    if (!inserted) return this.alreadyRecorded(envelope.event_id);

    emailsFailedTotal.inc({ reason: "invalid_event" });
    log.error(
      sanitize({
        status: "failed",
        failure_reason: "invalid_event",
        issues: issues.map((issue) => issue.path.join(".")),
      }),
    );
    return { status: "failed", deliveryId: inserted.id, reason: "invalid_event" };
  }
}
