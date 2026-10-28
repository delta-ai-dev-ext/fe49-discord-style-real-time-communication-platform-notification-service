import { Counter, Gauge, Histogram, Registry } from "prom-client";

/**
 * Every metric defined in `docs/architecture.md` §8 is registered here.
 * Labels are low-cardinality only: never user ids, community ids, event ids,
 * provider message ids, or timestamps.
 */

export const registry = new Registry();

export type EmailKind = "friend_request" | "community_invite";
export type BounceType = "hard" | "soft";
export type SuppressionReason = "hard_bounce" | "complaint";
export type DeferralReason = "quota" | "transient";
export type StatusClass = "2xx" | "4xx" | "5xx" | "network";

export const emailsSentTotal = new Counter({
  name: "notification_emails_sent_total",
  help: "Transactional emails accepted by the provider.",
  labelNames: ["kind"] as const,
  registers: [registry],
});

export const emailsDeliveredTotal = new Counter({
  name: "notification_emails_delivered_total",
  help: "Emails reported as delivered by the provider webhook.",
  labelNames: ["kind"] as const,
  registers: [registry],
});

export const emailsBouncedTotal = new Counter({
  name: "notification_emails_bounced_total",
  help: "Emails reported as bounced by the provider webhook.",
  labelNames: ["kind", "type"] as const,
  registers: [registry],
});

export const emailsComplainedTotal = new Counter({
  name: "notification_emails_complained_total",
  help: "Emails reported as complained by the provider webhook.",
  labelNames: ["kind"] as const,
  registers: [registry],
});

export const emailsSuppressedTotal = new Counter({
  name: "notification_emails_suppressed_total",
  help: "Recipient addresses added to the suppression list.",
  labelNames: ["reason"] as const,
  registers: [registry],
});

export const resendApiDurationSeconds = new Histogram({
  name: "notification_resend_api_duration_seconds",
  help: "Latency of Resend send API calls.",
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [registry],
});

export const resendApiErrorsTotal = new Counter({
  name: "notification_resend_api_errors_total",
  help: "Resend send API calls that failed, by response status class.",
  labelNames: ["status_class"] as const,
  registers: [registry],
});

export const dailyQuotaUsedRatio = new Gauge({
  name: "notification_daily_quota_used_ratio",
  help: "Share of the configured daily send cap already consumed (alert at >= 0.8).",
  registers: [registry],
});

export const emailsDeferredTotal = new Counter({
  name: "notification_emails_deferred_total",
  help: "Emails deferred instead of dropped.",
  labelNames: ["reason"] as const,
  registers: [registry],
});

export const kafkaConsumerLag = new Gauge({
  name: "kafka_consumer_lag",
  help: "Messages not yet consumed per topic for this consumer group.",
  labelNames: ["topic", "group"] as const,
  registers: [registry],
});

export const emailsFailedTotal = new Counter({
  name: "notification_emails_failed_total",
  help: "Emails dead-lettered into the failed state, by failure reason.",
  labelNames: ["reason"] as const,
  registers: [registry],
});

export function statusClassOf(statusCode: number | undefined): StatusClass {
  if (statusCode === undefined) return "network";
  if (statusCode >= 500) return "5xx";
  if (statusCode >= 400) return "4xx";
  return "2xx";
}

export async function renderMetrics(): Promise<string> {
  return registry.metrics();
}

export function metricsContentType(): string {
  return registry.contentType;
}

/** Set the daily quota gauge; a zero cap means "unlimited" and reports 0. */
export function setDailyQuotaRatio(sentToday: number, dailyLimit: number): void {
  dailyQuotaUsedRatio.set(dailyLimit > 0 ? Math.min(sentToday / dailyLimit, 1) : 0);
}
