import type { AppDb } from "../db/client.ts";
import type { EmailStatus } from "../db/schema.ts";
import { logger as defaultLogger, sanitize, type AppLogger } from "../logger.ts";
import {
  emailsBouncedTotal,
  emailsComplainedTotal,
  emailsDeliveredTotal,
  emailsSuppressedTotal,
  type BounceType,
  type SuppressionReason,
} from "../metrics.ts";
import {
  RESEND_WEBHOOK_EVENT_TYPES,
  hasAllSvixHeaders,
  resendWebhookEventSchema,
  type ResendWebhookEvent,
  type SvixHeaders,
} from "../models/webhook.ts";
import {
  createEmailDeliveryRepository,
  type DeliveryRecord,
  type EmailDeliveryRepository,
} from "../repositories/emailDelivery.repository.ts";
import {
  createEmailSuppressionRepository,
  type EmailSuppressionRepository,
} from "../repositories/emailSuppression.repository.ts";

import { parseTimestamp } from "./time.ts";
import type { WebhookDedupeStore } from "./webhookDedupe.ts";
import type { WebhookVerifier } from "./webhookVerifier.ts";

export type WebhookResponse =
  | { status: 200; body: { status: "processed" | "ignored" } }
  | {
      status: 400 | 401 | 422 | 500;
      body: { error: { code: string; message: string; details?: unknown } };
    };

export type WebhookServiceDeps = {
  db: AppDb;
  verifier: WebhookVerifier;
  dedupe: WebhookDedupeStore;
  logger?: AppLogger;
  now?: () => Date;
  deliveryRepo?: EmailDeliveryRepository;
  suppressionRepo?: EmailSuppressionRepository;
};

const STATUS_BY_EVENT: Record<string, "sent" | "delivered" | "bounced" | "complained"> = {
  "email.sent": "sent",
  "email.delivered": "delivered",
  "email.bounced": "bounced",
  "email.complained": "complained",
};

/**
 * `POST /v1/webhooks/resend` business logic.
 *
 * Order is load-bearing (US-008): signature verification over the raw body
 * happens before the JSON is parsed into business logic.
 */
export class WebhookService {
  private readonly db: AppDb;
  private readonly verifier: WebhookVerifier;
  private readonly dedupe: WebhookDedupeStore;
  private readonly logger: AppLogger;
  private readonly now: () => Date;
  private readonly deliveryRepo: EmailDeliveryRepository;
  private readonly suppressionRepo: EmailSuppressionRepository;

  constructor(deps: WebhookServiceDeps) {
    this.db = deps.db;
    this.verifier = deps.verifier;
    this.dedupe = deps.dedupe;
    this.logger = deps.logger ?? defaultLogger;
    this.now = deps.now ?? (() => new Date());
    this.deliveryRepo = deps.deliveryRepo ?? createEmailDeliveryRepository();
    this.suppressionRepo = deps.suppressionRepo ?? createEmailSuppressionRepository();
  }

  async handle(rawBody: string, headers: SvixHeaders): Promise<WebhookResponse> {
    const log = this.logger.child({ operation: "resend_webhook" });

    if (!hasAllSvixHeaders(headers)) {
      log.warn({ status: "rejected", reason: "missing_signature_headers" });
      return unauthorized();
    }

    let signatureValid = false;
    try {
      signatureValid = this.verifier.verify(rawBody, headers);
    } catch {
      signatureValid = false;
    }
    if (!signatureValid) {
      log.warn({ status: "rejected", reason: "invalid_signature", svix_id: headers.id });
      return unauthorized();
    }

    let payload: unknown;
    try {
      payload = JSON.parse(rawBody);
    } catch {
      log.warn({ status: "rejected", reason: "malformed_json", svix_id: headers.id });
      return {
        status: 400,
        body: { error: { code: "invalid_request_body", message: "Request body is not valid JSON" } },
      };
    }

    const declaredType = (payload as { type?: unknown } | null)?.type;
    if (typeof declaredType !== "string" || !(RESEND_WEBHOOK_EVENT_TYPES as readonly string[]).includes(declaredType)) {
      log.warn(sanitize({ status: "rejected", reason: "unknown_event_type", event_type: declaredType }));
      return {
        status: 422,
        body: {
          error: {
            code: "invalid_event_type",
            message: "Unsupported webhook event type",
            details: { supported: [...RESEND_WEBHOOK_EVENT_TYPES] },
          },
        },
      };
    }

    const parsed = resendWebhookEventSchema.safeParse(payload);
    if (!parsed.success) {
      log.warn({
        status: "rejected",
        reason: "invalid_payload",
        issues: parsed.error.issues.map((issue) => issue.path.join(".")),
      });
      return {
        status: 422,
        body: {
          error: {
            code: "invalid_payload",
            message: "Webhook payload failed validation",
            details: parsed.error.issues.map((issue) => ({ path: issue.path.join("."), message: issue.message })),
          },
        },
      };
    }

    const event = parsed.data;
    const svixId = headers.id ?? "";

    // Webhook-level dedupe: the same Svix message id is processed once.
    if (!this.dedupe.checkAndRemember(svixId)) {
      log.info({ status: "ignored", reason: "duplicate_svix_message", svix_id: svixId });
      return { status: 200, body: { status: "ignored" } };
    }

    const record = await this.deliveryRepo.findByProviderMessageId(this.db, event.data.email_id);
    if (!record) {
      log.info({ status: "ignored", reason: "unknown_message_id", svix_id: svixId });
      return { status: 200, body: { status: "ignored" } };
    }

    await this.applyEvent(event, record, log);
    return { status: 200, body: { status: "processed" } };
  }

  private async applyEvent(event: ResendWebhookEvent, record: DeliveryRecord, log: AppLogger): Promise<void> {
    const at = parseTimestamp(event.data.created_at) ?? parseTimestamp(event.created_at) ?? this.now();
    const target = STATUS_BY_EVENT[event.type] ?? "sent";

    await this.deliveryRepo.applyProviderStatus(this.db, record.id, target, at);

    if (event.type === "email.delivered") {
      emailsDeliveredTotal.inc({ kind: record.kind });
    } else if (event.type === "email.complained") {
      emailsComplainedTotal.inc({ kind: record.kind });
      await this.suppress(event, record, "complaint");
    } else if (event.type === "email.bounced") {
      const bounceType = (event.data.bounce?.type ?? "soft") as BounceType;
      emailsBouncedTotal.inc({ kind: record.kind, type: bounceType });
      if (bounceType === "hard") {
        await this.suppress(event, record, "hard_bounce");
      }
    }

    log.info(
      sanitize({
        status: target,
        delivery_id: record.id,
        kind: record.kind,
        event_type: event.type,
        current_status: record.status as EmailStatus,
      }),
    );
  }

  private async suppress(
    event: ResendWebhookEvent,
    record: DeliveryRecord,
    reason: SuppressionReason,
  ): Promise<void> {
    const address = (event.data.to?.[0] ?? record.recipientEmail).trim().toLowerCase();
    if (!address) return;
    const { created } = await this.suppressionRepo.upsert(this.db, {
      email: address,
      reason,
      providerMessageId: event.data.email_id,
    });
    if (created) {
      emailsSuppressedTotal.inc({ reason });
    }
  }
}

function unauthorized(): WebhookResponse {
  return {
    status: 401,
    body: {
      error: {
        code: "webhook_signature_invalid",
        message: "Webhook signature verification failed",
      },
    },
  };
}
