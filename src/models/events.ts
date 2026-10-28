import { z } from "zod";

import { EVENT_TYPES } from "../config.ts";

/**
 * Shared event envelope consumed from every platform topic:
 * `{ event_id, event_type, occurred_at, producer, version, trace_parent, data }`.
 *
 * Identity fields (`event_id`, `event_type`, `data`) are mandatory — without
 * them nothing can be deduped or dispatched. Decoration fields tolerate a
 * missing value so a producer-side omission cannot wedge a partition.
 */
export const envelopeSchema = z.object({
  event_id: z.uuid(),
  event_type: z.string().min(1),
  occurred_at: z.string().min(1),
  producer: z.string().min(1).optional(),
  version: z.union([z.string(), z.number()]).optional(),
  trace_parent: z.string().min(1).optional(),
  data: z.unknown(),
});

export type EventEnvelope = z.infer<typeof envelopeSchema>;

const httpUrlSchema = z
  .string()
  .min(1)
  .refine((value) => /^https?:\/\/\S+$/i.test(value.trim()), "invite_url must be an absolute http(s) URL");

/** `friend_request_sent` on `identity_friend_events`. */
export const friendRequestSentDataSchema = z.object({
  sender_user_id: z.string().min(1).optional(),
  sender_username: z.string().min(1),
  recipient_user_id: z.string().min(1).optional(),
  recipient_email: z.email(),
});

export type FriendRequestSentData = z.infer<typeof friendRequestSentDataSchema>;

/** `invite_email_requested` on `community_invite_events`. */
export const inviteEmailRequestedDataSchema = z.object({
  community_id: z.string().min(1).optional(),
  community_name: z.string().min(1),
  inviter_username: z.string().min(1),
  recipient_email: z.email(),
  invite_url: httpUrlSchema,
  invite_code: z.string().min(1).optional(),
  expires_at: z.string().min(1),
});

export type InviteEmailRequestedData = z.infer<typeof inviteEmailRequestedDataSchema>;

export const EMAIL_EVENT_TYPES = [
  EVENT_TYPES.friendRequestSent,
  EVENT_TYPES.inviteEmailRequested,
] as const;

export type EmailEventType = (typeof EMAIL_EVENT_TYPES)[number];

export type EmailEvent =
  | {
      eventType: typeof EVENT_TYPES.friendRequestSent;
      envelope: EventEnvelope;
      data: FriendRequestSentData;
      kind: "friend_request";
      recipientEmail: string;
    }
  | {
      eventType: typeof EVENT_TYPES.inviteEmailRequested;
      envelope: EventEnvelope;
      data: InviteEmailRequestedData;
      kind: "community_invite";
      recipientEmail: string;
    };

export type EmailEventParseResult =
  | { ok: true; event: EmailEvent }
  | { ok: false; stage: "envelope"; error: z.ZodError }
  | { ok: false; stage: "data"; error: z.ZodError; envelope: EventEnvelope; kind: string | null };

export function isEmailEventType(eventType: string): eventType is EmailEventType {
  return (EMAIL_EVENT_TYPES as readonly string[]).includes(eventType);
}

/**
 * Parse a raw Kafka payload into a typed email event. Envelope and `data`
 * failures are reported separately because they have different outcomes:
 * an invalid envelope cannot be persisted, an invalid `data` payload becomes a
 * `failed/invalid_event` delivery record.
 */
export function parseEmailEvent(raw: unknown): EmailEventParseResult {
  const envelopeResult = envelopeSchema.safeParse(raw);
  if (!envelopeResult.success) {
    return { ok: false, stage: "envelope", error: envelopeResult.error };
  }

  const envelope = envelopeResult.data;

  if (!isEmailEventType(envelope.event_type)) {
    return { ok: false, stage: "data", error: new z.ZodError([]), envelope, kind: null };
  }

  if (envelope.event_type === EVENT_TYPES.friendRequestSent) {
    const data = friendRequestSentDataSchema.safeParse(envelope.data);
    if (!data.success) {
      return { ok: false, stage: "data", error: data.error, envelope, kind: "friend_request" };
    }
    return {
      ok: true,
      event: {
        eventType: EVENT_TYPES.friendRequestSent,
        envelope,
        data: data.data,
        kind: "friend_request",
        recipientEmail: data.data.recipient_email.trim().toLowerCase(),
      },
    };
  }

  const data = inviteEmailRequestedDataSchema.safeParse(envelope.data);
  if (!data.success) {
    return { ok: false, stage: "data", error: data.error, envelope, kind: "community_invite" };
  }
  return {
    ok: true,
    event: {
      eventType: EVENT_TYPES.inviteEmailRequested,
      envelope,
      data: data.data,
      kind: "community_invite",
      recipientEmail: data.data.recipient_email.trim().toLowerCase(),
    },
  };
}

/** Best-effort recipient address for a record that failed validation. */
export function guessRecipientEmail(raw: unknown): string {
  if (typeof raw !== "object" || raw === null) return "";
  const data = (raw as { data?: unknown }).data;
  if (typeof data !== "object" || data === null) return "";
  const email = (data as { recipient_email?: unknown }).recipient_email;
  return typeof email === "string" ? email.trim().toLowerCase() : "";
}
