import { Resend } from "resend";

import { resendApiDurationSeconds, resendApiErrorsTotal, statusClassOf } from "../metrics.ts";

/**
 * Thin adapter over the official Resend SDK. The SDK resolves with
 * `{ data, error }` and does not throw for provider-level failures, so both the
 * resolved `error` field and transport-level throws are normalized here.
 */

export type ProviderErrorKind = "transient" | "permanent" | "quota";

export type ProviderError = {
  kind: ProviderErrorKind;
  /** Provider error code (or `network_error` for transport failures). */
  code: string;
  message: string;
  statusCode?: number;
};

export type SendEmailInput = {
  to: string;
  subject: string;
  html: string;
  text: string;
  /** Kafka `event_id` — the Resend idempotency key (ADR-001). */
  idempotencyKey: string;
  traceId?: string;
};

export type SendOutcome =
  | { ok: true; messageId: string }
  | { ok: false; error: ProviderError };

export interface EmailProvider {
  send(input: SendEmailInput): Promise<SendOutcome>;
}

const TRANSIENT_CODES = new Set([
  "rate_limit_exceeded",
  "internal_server_error",
  "application_error",
  "concurrent_idempotent_requests",
]);

const QUOTA_CODES = new Set(["daily_quota_exceeded", "monthly_quota_exceeded"]);

const PERMANENT_CODES = new Set([
  "validation_error",
  "missing_required_field",
  "invalid_from_address",
  "invalid_idempotency_key",
  "invalid_idempotent_request",
  "invalid_parameter",
  "invalid_attachment",
  "invalid_access",
  "invalid_region",
  "invalid_api_key",
  "missing_api_key",
  "restricted_api_key",
  "not_found",
  "method_not_allowed",
  "security_error",
]);

/**
 * 429 / 5xx / transport failures are transient (retry with backoff), exhausted
 * provider quota is a deferral, everything else is a permanent failure.
 */
export function classifyProviderError(error: {
  name?: string;
  code?: string;
  message?: string;
  statusCode?: number;
}): ProviderError {
  const code = error.name ?? error.code ?? "unknown_error";
  const message = error.message ?? "Resend request failed";
  const statusCode = error.statusCode;

  if (QUOTA_CODES.has(code)) {
    return { kind: "quota", code, message, statusCode };
  }
  if (TRANSIENT_CODES.has(code) || statusCode === 429 || (statusCode !== undefined && statusCode >= 500)) {
    return { kind: "transient", code, message, statusCode };
  }
  if (statusCode === undefined && !PERMANENT_CODES.has(code)) {
    // No HTTP response at all (DNS, socket, timeout) — always retryable.
    return { kind: "transient", code, message };
  }
  return { kind: "permanent", code, message, statusCode };
}

export type ResendProviderOptions = {
  apiKey: string;
  fromAddress: string;
  /** Injectable for tests; defaults to the official SDK client. */
  client?: { emails: { send: (payload: unknown, options?: unknown) => Promise<unknown> } };
  now?: () => number;
};

export function createResendProvider(options: ResendProviderOptions): EmailProvider {
  const client = options.client ?? (new Resend(options.apiKey) as unknown as ResendProviderOptions["client"]);
  const now = options.now ?? (() => Date.now());

  return {
    async send(input: SendEmailInput): Promise<SendOutcome> {
      const startedAt = now();
      try {
        const response = (await client!.emails.send(
          {
            from: options.fromAddress,
            to: input.to,
            subject: input.subject,
            html: input.html,
            text: input.text,
          },
          { idempotencyKey: input.idempotencyKey },
        )) as { data?: { id?: string } | null; error?: { name?: string; message?: string; statusCode?: number } | null };

        resendApiDurationSeconds.observe((now() - startedAt) / 1000);

        if (response?.error) {
          const error = classifyProviderError(response.error);
          resendApiErrorsTotal.inc({ status_class: statusClassOf(error.statusCode) });
          return { ok: false, error };
        }

        const messageId = response?.data?.id;
        if (!messageId) {
          resendApiErrorsTotal.inc({ status_class: "2xx" });
          return {
            ok: false,
            error: { kind: "permanent", code: "missing_message_id", message: "Resend response carried no message id" },
          };
        }

        return { ok: true, messageId };
      } catch (thrown) {
        resendApiDurationSeconds.observe((now() - startedAt) / 1000);
        resendApiErrorsTotal.inc({ status_class: "network" });
        const error = classifyProviderError({
          name: thrown instanceof Error ? thrown.name : "network_error",
          message: thrown instanceof Error ? thrown.message : "Resend request threw",
        });
        return { ok: false, error };
      }
    },
  };
}
