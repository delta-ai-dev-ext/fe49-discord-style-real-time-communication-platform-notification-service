import { sql } from "drizzle-orm";

import type { SqlExecutor } from "./client.ts";

/**
 * Schema DDL for `notification_service`, applied at startup (before the HTTP
 * probes come up). Statements are idempotent so a restart is harmless.
 * The definition matches `docs/architecture.md` §5 verbatim.
 */
export const MIGRATION_STATEMENTS = [
  `CREATE TABLE IF NOT EXISTS email_delivery (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_event_id     UUID        NOT NULL UNIQUE,
    kind                TEXT        NOT NULL
                        CHECK (kind IN ('friend_request', 'community_invite')),
    recipient_email     TEXT        NOT NULL,
    sender_user_id      TEXT        NULL,
    recipient_user_id   TEXT        NULL,
    invite_code         TEXT        NULL,
    community_id        TEXT        NULL,
    provider_message_id TEXT        NULL,
    status              TEXT        NOT NULL DEFAULT 'queued'
                        CHECK (status IN ('queued','sent','delivered',
                                          'bounced','complained',
                                          'deferred','suppressed','failed')),
    failure_reason      TEXT        NULL,
    attempts            INTEGER     NOT NULL DEFAULT 0
                        CHECK (attempts >= 0),
    next_retry_at       TIMESTAMPTZ NULL,
    sent_at             TIMESTAMPTZ NULL,
    delivered_at        TIMESTAMPTZ NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
  )`,
  `CREATE INDEX IF NOT EXISTS idx_email_delivery_status_retry
    ON email_delivery (status, next_retry_at)
    WHERE status IN ('queued', 'deferred')`,
  `CREATE INDEX IF NOT EXISTS idx_email_delivery_provider_message
    ON email_delivery (provider_message_id)
    WHERE provider_message_id IS NOT NULL`,
  `CREATE TABLE IF NOT EXISTS email_suppression (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT        NOT NULL UNIQUE,
    reason        TEXT        NOT NULL
                  CHECK (reason IN ('hard_bounce', 'complaint')),
    provider_message_id TEXT  NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
  )`,
  `CREATE TABLE IF NOT EXISTS daily_send_counter (
    day            DATE     PRIMARY KEY,
    sent_count     INTEGER  NOT NULL DEFAULT 0
                   CHECK (sent_count >= 0),
    deferred_count INTEGER  NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
  )`,
] as const;

export async function runMigrations(executor: SqlExecutor): Promise<void> {
  for (const statement of MIGRATION_STATEMENTS) {
    await executor.execute(sql.raw(statement));
  }
}
