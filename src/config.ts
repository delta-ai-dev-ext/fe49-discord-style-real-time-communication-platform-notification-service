import { z } from "zod";

/**
 * 12-factor configuration. Every value is validated once at startup so the
 * process fails fast (structured error) instead of misbehaving later.
 */

const blankToUndefined = (value: unknown): unknown =>
  typeof value === "string" && value.trim() === "" ? undefined : value;

const intFromEnv = (fallback: number, min: number) =>
  z.preprocess(blankToUndefined, z.coerce.number().int().min(min).default(fallback));

const boolFromEnv = (fallback: boolean) =>
  z.preprocess((value) => {
    const raw = blankToUndefined(value);
    if (raw === undefined) return undefined;
    if (typeof raw === "boolean") return raw;
    if (typeof raw === "string") return ["1", "true", "yes", "on"].includes(raw.toLowerCase());
    return undefined;
  }, z.boolean().default(fallback));

const optionalString = z.preprocess(
  blankToUndefined,
  z.string().min(1).optional(),
);

export const logLevels = ["error", "warn", "info", "http", "debug"] as const;

export const configSchema = z.object({
  PORT: intFromEnv(3000, 1),

  DATABASE_URL: z.string().min(1, "DATABASE_URL is required"),

  KAFKA_BROKERS: z
    .string()
    .min(1, "KAFKA_BROKERS is required")
    .transform((value) => value.split(",").map((broker) => broker.trim()).filter(Boolean))
    .refine((brokers) => brokers.length > 0, "KAFKA_BROKERS must list at least one broker"),
  KAFKA_CLIENT_ID: z.preprocess(blankToUndefined, z.string().min(1).default("notification_service")),
  KAFKA_SASL_USERNAME: optionalString,
  KAFKA_SASL_PASSWORD: optionalString,
  KAFKA_SASL_MECHANISM: z.preprocess(
    blankToUndefined,
    z.enum(["plain", "scram-sha-256", "scram-sha-512"]).default("plain"),
  ),
  KAFKA_TLS: boolFromEnv(false),

  RESEND_API_KEY: z.string().min(1, "RESEND_API_KEY is required"),
  RESEND_WEBHOOK_SECRET: z.string().min(1, "RESEND_WEBHOOK_SECRET is required"),
  RESEND_FROM_ADDRESS: z.string().min(1, "RESEND_FROM_ADDRESS is required"),

  EMAIL_DAILY_LIMIT: intFromEnv(100, 0),
  EMAIL_MONTHLY_LIMIT: intFromEnv(3000, 0),
  SEND_MAX_ATTEMPTS: intFromEnv(5, 1),
  SHUTDOWN_TIMEOUT_SECONDS: intFromEnv(20, 1),

  LOG_LEVEL: z.preprocess(blankToUndefined, z.enum(logLevels).default("info")),
});

export type Config = z.infer<typeof configSchema>;

export class ConfigError extends Error {
  readonly code = "invalid_configuration";

  constructor(readonly issues: { path: string; message: string }[]) {
    super(`Invalid configuration: ${issues.map((i) => `${i.path}: ${i.message}`).join("; ")}`);
    this.name = "ConfigError";
  }
}

export function loadConfig(env: Record<string, string | undefined> = process.env): Config {
  const result = configSchema.safeParse(env);
  if (!result.success) {
    throw new ConfigError(
      result.error.issues.map((issue) => ({
        path: issue.path.join(".") || "(root)",
        message: issue.message,
      })),
    );
  }
  return result.data;
}

export const CONSUMER_GROUP = "notification_service";

export const TOPICS = {
  identityFriendEvents: "identity_friend_events",
  communityInviteEvents: "community_invite_events",
} as const;

export const CONSUMED_TOPICS = [TOPICS.identityFriendEvents, TOPICS.communityInviteEvents] as const;

export const EVENT_TYPES = {
  friendRequestSent: "friend_request_sent",
  inviteEmailRequested: "invite_email_requested",
} as const;
