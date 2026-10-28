import { sql } from "drizzle-orm";
import { check, date, index, integer, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";

/**
 * PostgreSQL schema owned by this service — mirrors `docs/architecture.md` §5
 * exactly. No cross-service table sharing.
 */

export const EMAIL_KINDS = ["friend_request", "community_invite"] as const;
export const EMAIL_STATUSES = [
  "queued",
  "sent",
  "delivered",
  "bounced",
  "complained",
  "deferred",
  "suppressed",
  "failed",
] as const;
export const SUPPRESSION_REASONS = ["hard_bounce", "complaint"] as const;

export type EmailKindValue = (typeof EMAIL_KINDS)[number];
export type EmailStatus = (typeof EMAIL_STATUSES)[number];
export type SuppressionReasonValue = (typeof SUPPRESSION_REASONS)[number];

export const emailDelivery = pgTable(
  "email_delivery",
  {
    id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
    sourceEventId: uuid("source_event_id").notNull().unique(),
    kind: text("kind").notNull(),
    recipientEmail: text("recipient_email").notNull(),
    senderUserId: text("sender_user_id"),
    recipientUserId: text("recipient_user_id"),
    inviteCode: text("invite_code"),
    communityId: text("community_id"),
    providerMessageId: text("provider_message_id"),
    status: text("status").notNull().default("queued"),
    failureReason: text("failure_reason"),
    attempts: integer("attempts").notNull().default(0),
    nextRetryAt: timestamp("next_retry_at", { withTimezone: true }),
    sentAt: timestamp("sent_at", { withTimezone: true }),
    deliveredAt: timestamp("delivered_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    index("idx_email_delivery_status_retry")
      .on(table.status, table.nextRetryAt)
      .where(sql`status IN ('queued', 'deferred')`),
    index("idx_email_delivery_provider_message")
      .on(table.providerMessageId)
      .where(sql`provider_message_id IS NOT NULL`),
    check(
      "email_delivery_kind_check",
      sql`kind IN ('friend_request', 'community_invite')`,
    ),
    check(
      "email_delivery_status_check",
      sql`status IN ('queued','sent','delivered','bounced','complained','deferred','suppressed','failed')`,
    ),
    check("email_delivery_attempts_check", sql`attempts >= 0`),
  ],
);

export const emailSuppression = pgTable(
  "email_suppression",
  {
    id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
    email: text("email").notNull().unique(),
    reason: text("reason").notNull(),
    providerMessageId: text("provider_message_id"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    check("email_suppression_reason_check", sql`reason IN ('hard_bounce', 'complaint')`),
  ],
);

export const dailySendCounter = pgTable(
  "daily_send_counter",
  {
    day: date("day").primaryKey(),
    sentCount: integer("sent_count").notNull().default(0),
    deferredCount: integer("deferred_count").notNull().default(0),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [check("daily_send_counter_sent_count_check", sql`sent_count >= 0`)],
);

export const schema = { emailDelivery, emailSuppression, dailySendCounter };

export type EmailDeliveryRow = typeof emailDelivery.$inferSelect;
export type NewEmailDeliveryRow = typeof emailDelivery.$inferInsert;
export type EmailSuppressionRow = typeof emailSuppression.$inferSelect;
export type DailySendCounterRow = typeof dailySendCounter.$inferSelect;
