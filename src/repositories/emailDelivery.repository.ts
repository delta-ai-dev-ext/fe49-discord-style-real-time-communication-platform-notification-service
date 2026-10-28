import { sql } from "drizzle-orm";

import type { SqlExecutor } from "../db/client.ts";
import type { EmailStatus } from "../db/schema.ts";
import type { EmailKind } from "../metrics.ts";

/**
 * All SQL touching `email_delivery`. Parameterized drizzle `sql` templates only;
 * no business rules live here.
 */

export type DeliveryRecord = {
  id: string;
  sourceEventId: string;
  kind: EmailKind;
  recipientEmail: string;
  senderUserId: string | null;
  recipientUserId: string | null;
  inviteCode: string | null;
  communityId: string | null;
  providerMessageId: string | null;
  status: EmailStatus;
  failureReason: string | null;
  attempts: number;
  nextRetryAt: Date | null;
  sentAt: Date | null;
  deliveredAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
};

export type InsertDeliveryInput = {
  sourceEventId: string;
  kind: EmailKind;
  recipientEmail: string;
  senderUserId?: string | null;
  recipientUserId?: string | null;
  inviteCode?: string | null;
  communityId?: string | null;
  status?: EmailStatus;
  failureReason?: string | null;
  nextRetryAt?: Date | null;
};

const toDate = (value: unknown): Date | null => {
  if (value instanceof Date) return value;
  if (typeof value === "string") {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }
  return null;
};

export function mapDeliveryRow(row: Record<string, unknown>): DeliveryRecord {
  return {
    id: String(row.id),
    sourceEventId: String(row.source_event_id),
    kind: String(row.kind) as EmailKind,
    recipientEmail: String(row.recipient_email),
    senderUserId: row.sender_user_id === null || row.sender_user_id === undefined ? null : String(row.sender_user_id),
    recipientUserId:
      row.recipient_user_id === null || row.recipient_user_id === undefined ? null : String(row.recipient_user_id),
    inviteCode: row.invite_code === null || row.invite_code === undefined ? null : String(row.invite_code),
    communityId: row.community_id === null || row.community_id === undefined ? null : String(row.community_id),
    providerMessageId:
      row.provider_message_id === null || row.provider_message_id === undefined
        ? null
        : String(row.provider_message_id),
    status: String(row.status) as EmailStatus,
    failureReason: row.failure_reason === null || row.failure_reason === undefined ? null : String(row.failure_reason),
    attempts: Number(row.attempts ?? 0),
    nextRetryAt: toDate(row.next_retry_at),
    sentAt: toDate(row.sent_at),
    deliveredAt: toDate(row.delivered_at),
    createdAt: toDate(row.created_at) ?? new Date(0),
    updatedAt: toDate(row.updated_at) ?? new Date(0),
  };
}

export function createEmailDeliveryRepository() {
  return {
    async findBySourceEventId(exec: SqlExecutor, sourceEventId: string): Promise<DeliveryRecord | null> {
      const result = await exec.execute(
        sql`SELECT * FROM email_delivery WHERE source_event_id = ${sourceEventId} LIMIT 1`,
      );
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    async findById(exec: SqlExecutor, id: string): Promise<DeliveryRecord | null> {
      const result = await exec.execute(sql`SELECT * FROM email_delivery WHERE id = ${id} LIMIT 1`);
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    async findByProviderMessageId(exec: SqlExecutor, providerMessageId: string): Promise<DeliveryRecord | null> {
      const result = await exec.execute(
        sql`SELECT * FROM email_delivery WHERE provider_message_id = ${providerMessageId} LIMIT 1`,
      );
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    /** Returns `null` when `source_event_id` already exists (dedupe constraint). */
    async insert(exec: SqlExecutor, input: InsertDeliveryInput): Promise<DeliveryRecord | null> {
      const result = await exec.execute(
        sql`INSERT INTO email_delivery (
              source_event_id, kind, recipient_email, sender_user_id, recipient_user_id,
              invite_code, community_id, status, failure_reason, next_retry_at
            ) VALUES (
              ${input.sourceEventId}, ${input.kind}, ${input.recipientEmail},
              ${input.senderUserId ?? null}, ${input.recipientUserId ?? null},
              ${input.inviteCode ?? null}, ${input.communityId ?? null},
              ${input.status ?? "queued"}, ${input.failureReason ?? null}, ${input.nextRetryAt ?? null}
            )
            ON CONFLICT (source_event_id) DO NOTHING
            RETURNING *`,
      );
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    async markSent(
      exec: SqlExecutor,
      id: string,
      providerMessageId: string,
      sentAt: Date,
    ): Promise<DeliveryRecord | null> {
      const result = await exec.execute(
        sql`UPDATE email_delivery
            SET status = 'sent',
                provider_message_id = ${providerMessageId},
                sent_at = ${sentAt},
                failure_reason = NULL,
                next_retry_at = NULL,
                attempts = attempts + 1,
                updated_at = now()
            WHERE id = ${id}
            RETURNING *`,
      );
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    /** Increments `attempts` and schedules the next backoff slot. */
    async markAttempt(
      exec: SqlExecutor,
      id: string,
      nextRetryAt: Date | null,
    ): Promise<DeliveryRecord | null> {
      const result = await exec.execute(
        sql`UPDATE email_delivery
            SET attempts = attempts + 1,
                next_retry_at = ${nextRetryAt},
                updated_at = now()
            WHERE id = ${id}
            RETURNING *`,
      );
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    async markFailed(exec: SqlExecutor, id: string, reason: string): Promise<DeliveryRecord | null> {
      const result = await exec.execute(
        sql`UPDATE email_delivery
            SET status = 'failed',
                failure_reason = ${reason},
                next_retry_at = NULL,
                updated_at = now()
            WHERE id = ${id}
            RETURNING *`,
      );
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    async markSuppressed(exec: SqlExecutor, id: string): Promise<DeliveryRecord | null> {
      const result = await exec.execute(
        sql`UPDATE email_delivery
            SET status = 'suppressed',
                next_retry_at = NULL,
                updated_at = now()
            WHERE id = ${id}
            RETURNING *`,
      );
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    async markDeferred(
      exec: SqlExecutor,
      id: string,
      reason: string,
      nextRetryAt: Date,
    ): Promise<DeliveryRecord | null> {
      const result = await exec.execute(
        sql`UPDATE email_delivery
            SET status = 'deferred',
                failure_reason = ${reason},
                next_retry_at = ${nextRetryAt},
                updated_at = now()
            WHERE id = ${id}
            RETURNING *`,
      );
      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    /** Applies a provider-reported status without ever regressing the record. */
    async applyProviderStatus(
      exec: SqlExecutor,
      id: string,
      status: "sent" | "delivered" | "bounced" | "complained",
      at: Date,
    ): Promise<DeliveryRecord | null> {
      const result =
        status === "sent"
          ? await exec.execute(
              sql`UPDATE email_delivery
                  SET status = CASE WHEN status IN ('delivered','bounced','complained') THEN status ELSE 'sent' END,
                      sent_at = COALESCE(sent_at, ${at}),
                      failure_reason = NULL,
                      updated_at = now()
                  WHERE id = ${id}
                  RETURNING *`,
            )
          : status === "delivered"
            ? await exec.execute(
                sql`UPDATE email_delivery
                    SET status = CASE WHEN status IN ('bounced','complained') THEN status ELSE 'delivered' END,
                        delivered_at = COALESCE(delivered_at, ${at}),
                        sent_at = COALESCE(sent_at, ${at}),
                        updated_at = now()
                    WHERE id = ${id}
                    RETURNING *`,
              )
            : await exec.execute(
                sql`UPDATE email_delivery
                    SET status = ${status},
                        updated_at = now()
                    WHERE id = ${id}
                    RETURNING *`,
              );

      const row = result.rows[0];
      return row ? mapDeliveryRow(row) : null;
    },

    /** Deferred/queued records whose retry slot has elapsed. */
    async listDueForRetry(exec: SqlExecutor, now: Date, limit = 100): Promise<DeliveryRecord[]> {
      const result = await exec.execute(
        sql`SELECT * FROM email_delivery
            WHERE status IN ('queued', 'deferred')
              AND next_retry_at IS NOT NULL
              AND next_retry_at <= ${now}
            ORDER BY next_retry_at ASC
            LIMIT ${limit}`,
      );
      return result.rows.map(mapDeliveryRow);
    },
  };
}

export type EmailDeliveryRepository = ReturnType<typeof createEmailDeliveryRepository>;
