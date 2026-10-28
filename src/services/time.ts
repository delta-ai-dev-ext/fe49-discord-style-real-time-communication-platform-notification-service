import { TZDate } from "@date-fns/tz";
import { addDays, addMonths, startOfDay, startOfMonth } from "date-fns";

/**
 * UTC bucketing for quota accounting and backoff scheduling, built on date-fns
 * with an explicit UTC time zone so the pod's local time zone is irrelevant.
 */

const utc = (date: Date): TZDate => new TZDate(date, "UTC");

/** `YYYY-MM-DD` for the UTC day containing `date` (matches `daily_send_counter.day`). */
export function utcDay(date: Date): string {
  return startOfDay(utc(date)).toISOString().slice(0, 10);
}

/** First instant of the next UTC day — the daily quota reset point. */
export function nextUtcDayStart(date: Date): Date {
  return new Date(addDays(startOfDay(utc(date)), 1).getTime());
}

/** First instant of the next UTC month — the monthly quota reset point. */
export function nextUtcMonthStart(date: Date): Date {
  return new Date(addMonths(startOfMonth(utc(date)), 1).getTime());
}

/** Inclusive UTC day range (`YYYY-MM-DD`) covering the month of `date`. */
export function utcMonthRange(date: Date): { firstDay: string; lastDay: string } {
  const monthStart = startOfMonth(utc(date));
  const nextMonthStart = addMonths(monthStart, 1);
  return {
    firstDay: monthStart.toISOString().slice(0, 10),
    lastDay: new Date(nextMonthStart.getTime() - 86_400_000).toISOString().slice(0, 10),
  };
}

/** Parse an ISO-8601 timestamp, returning `null` when unparseable. */
export function parseTimestamp(value: string | null | undefined): Date | null {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export const BACKOFF_BASE_MS = 500;
export const BACKOFF_MAX_MS = 30_000;

/** Exponential backoff with a cap, e.g. 500ms, 1s, 2s, 4s ... 30s. */
export function backoffDelayMs(attempt: number, baseMs = BACKOFF_BASE_MS, maxMs = BACKOFF_MAX_MS): number {
  const safeAttempt = Math.max(1, Math.floor(attempt));
  return Math.min(baseMs * 2 ** (safeAttempt - 1), maxMs);
}
