import { z } from "zod";

/**
 * Resend (Svix-signed) delivery-status webhook contract — mirrors
 * `docs/openapi_spec.yaml#/components/schemas/ResendWebhookEvent`.
 * Parsed only *after* the Svix signature has been verified over the raw body.
 */

export const RESEND_WEBHOOK_EVENT_TYPES = [
  "email.sent",
  "email.delivered",
  "email.bounced",
  "email.complained",
] as const;

export type ResendWebhookEventType = (typeof RESEND_WEBHOOK_EVENT_TYPES)[number];

export const resendWebhookDataSchema = z.object({
  email_id: z.string().min(1),
  created_at: z.string().min(1).optional(),
  to: z.array(z.string()).optional(),
  from: z.string().optional(),
  subject: z.string().optional(),
  bounce: z
    .object({
      type: z.enum(["hard", "soft"]),
      diagnostic_code: z.string().optional(),
    })
    .optional(),
});

export const resendWebhookEventSchema = z.object({
  type: z.enum(RESEND_WEBHOOK_EVENT_TYPES),
  created_at: z.string().min(1),
  data: resendWebhookDataSchema,
});

export type ResendWebhookEvent = z.infer<typeof resendWebhookEventSchema>;

export const SVIX_HEADERS = {
  id: "svix-id",
  timestamp: "svix-timestamp",
  signature: "svix-signature",
} as const;

export type SvixHeaders = {
  id: string | null;
  timestamp: string | null;
  signature: string | null;
};

export function readSvixHeaders(headers: Headers): SvixHeaders {
  return {
    id: headers.get(SVIX_HEADERS.id),
    timestamp: headers.get(SVIX_HEADERS.timestamp),
    signature: headers.get(SVIX_HEADERS.signature),
  };
}

export function hasAllSvixHeaders(headers: SvixHeaders): boolean {
  return Boolean(headers.id && headers.timestamp && headers.signature);
}

/** The shape `new Webhook(secret).verify(payload, headers)` expects. */
export function toSvixVerifyHeaders(headers: SvixHeaders): Record<string, string> {
  return {
    [SVIX_HEADERS.id]: headers.id ?? "",
    [SVIX_HEADERS.timestamp]: headers.timestamp ?? "",
    [SVIX_HEADERS.signature]: headers.signature ?? "",
  };
}
