import { sql } from "drizzle-orm";

import type { SqlExecutor } from "../db/client.ts";
import type { SuppressionReason } from "../metrics.ts";

export type SuppressionRecord = {
  id: string;
  email: string;
  reason: SuppressionReason;
  providerMessageId: string | null;
  createdAt: Date;
};

function mapRow(row: Record<string, unknown>): SuppressionRecord {
  return {
    id: String(row.id),
    email: String(row.email),
    reason: String(row.reason) as SuppressionReason,
    providerMessageId:
      row.provider_message_id === null || row.provider_message_id === undefined
        ? null
        : String(row.provider_message_id),
    createdAt: row.created_at instanceof Date ? row.created_at : new Date(String(row.created_at)),
  };
}

/** All SQL touching `email_suppression`. Unique on address keeps inserts idempotent. */
export function createEmailSuppressionRepository() {
  return {
    async findByEmail(exec: SqlExecutor, email: string): Promise<SuppressionRecord | null> {
      const result = await exec.execute(
        sql`SELECT * FROM email_suppression WHERE email = ${email} LIMIT 1`,
      );
      const row = result.rows[0];
      return row ? mapRow(row) : null;
    },

    /**
     * Idempotent upsert. Returns the row, plus `created: false` when the address
     * was already suppressed (so metrics only count first-time suppressions).
     */
    async upsert(
      exec: SqlExecutor,
      input: { email: string; reason: SuppressionReason; providerMessageId?: string | null },
    ): Promise<{ record: SuppressionRecord; created: boolean }> {
      const result = await exec.execute(
        sql`INSERT INTO email_suppression (email, reason, provider_message_id)
            VALUES (${input.email}, ${input.reason}, ${input.providerMessageId ?? null})
            ON CONFLICT (email) DO NOTHING
            RETURNING *`,
      );
      const inserted = result.rows[0];
      if (inserted) {
        return { record: mapRow(inserted), created: true };
      }

      const existing = await exec.execute(
        sql`SELECT * FROM email_suppression WHERE email = ${input.email} LIMIT 1`,
      );
      const row = existing.rows[0];
      return {
        record: mapRow(row ?? { id: input.email, email: input.email, reason: input.reason, created_at: new Date() }),
        created: false,
      };
    },
  };
}

export type EmailSuppressionRepository = ReturnType<typeof createEmailSuppressionRepository>;
