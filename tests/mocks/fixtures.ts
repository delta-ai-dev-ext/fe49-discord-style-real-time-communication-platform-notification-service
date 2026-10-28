/** Event and webhook fixtures shared by unit and integration tests. */

let counter = 0;

export function uuid(prefix = "00000000-0000-4000-8000"): string {
  counter += 1;
  return `${prefix}-${counter.toString().padStart(12, "0")}`;
}

export type EnvelopeOverrides = {
  event_id?: string;
  event_type?: string;
  occurred_at?: string;
  trace_parent?: string;
  data?: unknown;
  [key: string]: unknown;
};

export function envelope(overrides: EnvelopeOverrides = {}): Record<string, unknown> {
  const { event_id, event_type, occurred_at, trace_parent, data, ...rest } = overrides;
  return {
    event_id: event_id ?? uuid(),
    event_type: event_type ?? "friend_request_sent",
    occurred_at: occurred_at ?? "2025-01-01T00:00:00.000Z",
    producer: "identity_service",
    version: 1,
    trace_parent: trace_parent ?? "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    data: data ?? friendRequestData(),
    ...rest,
  };
}

export function friendRequestData(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    sender_user_id: "user-sender",
    sender_username: "Ada",
    recipient_user_id: "user-recipient",
    recipient_email: "Recipient@Example.com",
    ...overrides,
  };
}

export function inviteData(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    community_id: "community-1",
    community_name: "Rustaceans",
    inviter_username: "Grace",
    recipient_email: "invitee@example.com",
    invite_url: "https://app.example.com/invites/abc123",
    invite_code: "abc123",
    expires_at: "2999-01-01T00:00:00.000Z",
    ...overrides,
  };
}

export function friendRequestEnvelope(overrides: EnvelopeOverrides = {}): Record<string, unknown> {
  return envelope({
    event_type: "friend_request_sent",
    data: overrides.data ?? friendRequestData(),
    ...overrides,
    ...(overrides.data === undefined ? { data: friendRequestData() } : {}),
  });
}

export function inviteEnvelope(overrides: EnvelopeOverrides = {}): Record<string, unknown> {
  return envelope({
    event_type: "invite_email_requested",
    data: inviteData(),
    ...overrides,
  });
}

export function webhookPayload(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: "email.delivered",
    created_at: "2025-01-01T00:00:05.000Z",
    data: {
      email_id: "resend-message-1",
      created_at: "2025-01-01T00:00:05.000Z",
      to: ["Recipient@Example.com"],
      from: "noreply@example.com",
      subject: "Hello",
    },
    ...overrides,
  };
}

export function svixHeaders(id = "msg_1"): { id: string; timestamp: string; signature: string } {
  return { id, timestamp: "1735689600", signature: "v1,deadbeef" };
}

export function headersToRequest(headers: Record<string, string>, body: string): Request {
  return new Request("http://localhost/v1/webhooks/resend", {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body,
  });
}
