import winston from "winston";

import type { Config } from "./config.ts";

export const SERVICE_NAME = "notification_service";

export type LogContext = {
  trace_id?: string;
  operation?: string;
  [field: string]: unknown;
};

/**
 * Winston JSON logger writing to stdout. `console.log` is never used anywhere
 * in the service; every line is a structured JSON document carrying
 * `timestamp`, `severity`, `service_name`, and (when known) `trace_id`.
 */
const severity = winston.format((info) => {
  info.severity = info.level;
  return info;
});

export function createLogger(level: string = "info"): winston.Logger {
  return winston.createLogger({
    level,
    defaultMeta: { service_name: SERVICE_NAME },
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.errors({ stack: true }),
      severity(),
      winston.format.json(),
    ),
    transports: [new winston.transports.Console({ handleExceptions: true })],
  });
}

export const logger = createLogger(process.env.LOG_LEVEL ?? "info");

export type AppLogger = winston.Logger;

/** Child logger carrying the trace id of the unit of work. */
export function withContext(base: AppLogger, context: LogContext): AppLogger {
  return base.child(context);
}

const SENSITIVE_KEYS = new Set([
  "recipient_email",
  "email",
  "to",
  "html",
  "text",
  "body",
  "subject",
  "api_key",
  "RESEND_API_KEY",
  "RESEND_WEBHOOK_SECRET",
  "DATABASE_URL",
  "password",
]);

/**
 * Drop sensitive fields (recipient addresses, message bodies, secrets) before
 * they reach a log line.
 */
export function sanitize(fields: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (SENSITIVE_KEYS.has(key)) continue;
    out[key] = value;
  }
  return out;
}

export function configureLogger(level: Config["LOG_LEVEL"]): void {
  logger.level = level;
}
