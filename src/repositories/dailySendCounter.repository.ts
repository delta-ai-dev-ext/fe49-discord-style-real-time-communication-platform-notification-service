import { sql } from "drizzle-orm";

import type { SqlExecutor } from "../db/client.ts";

export type DailySendCounterRecord = {
  day: string;
  sentCount: number;
  deferredCount: number;
};

/**
 * All SQL touching `daily_send_counter` (PK = UTC date). Increments are upserts so
 * the row exists from the first send of the day.
 */
export function createDailySendCounterRepository() {
  return {
    async findByDay(exec: SqlExecutor, day: string): Promise<DailySendCounterRecord | null> {
      const result = await exec.execute(
        sql`SELECT day, sent_count, deferred_count FROM daily_send_counter WHERE day = ${day} LIMIT 1`,
      );
      const row = result.rows[0];
      if (!row) return null;
      return {
        day: String(row.day).slice(0, 10),
        sentCount: Number(row.sent_count ?? 0),
        deferredCount: Number(row.deferred_count ?? 0),
      };
    },

    /** Called from the same transaction as the `sent` status transition. */
    async incrementSent(exec: SqlExecutor, day: string): Promise<void> {
      await exec.execute(
        sql`INSERT INTO daily_send_counter (day, sent_count, deferred_count, updated_at)
            VALUES (${day}, 1, 0, now())
            ON CONFLICT (day) DO UPDATE
              SET sent_count = daily_send_counter.sent_count + 1,
                  updated_at = now()`,
      );
    },

    async incrementDeferred(exec: SqlExecutor, day: string): Promise<void> {
      await exec.execute(
        sql`INSERT INTO daily_send_counter (day, sent_count, deferred_count, updated_at)
            VALUES (${day}, 0, 1, now())
            ON CONFLICT (day) DO UPDATE
              SET deferred_count = daily_send_counter.deferred_count + 1,
                  updated_at = now()`,
      );
    },

    /** Sum of sends for the UTC month containing `firstDay`..`lastDay`. */
    async sumForRange(exec: SqlExecutor, firstDay: string, lastDay: string): Promise<number> {
      const result = await exec.execute(
        sql`SELECT COALESCE(SUM(sent_count), 0)::bigint AS total
            FROM daily_send_counter
            WHERE day >= ${firstDay} AND day <= ${lastDay}`,
      );
      const row = result.rows[0];
      return Number(row?.total ?? 0);
    },
  };
}

export type DailySendCounterRepository = ReturnType<typeof createDailySendCounterRepository>;
