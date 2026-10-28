import { Elysia } from "elysia";

import { createWebhookController } from "../controllers/webhook.controller.ts";
import type { WebhookService } from "../services/webhookService.ts";

/**
 * `POST /v1/webhooks/resend` — publicly routed, signature-authenticated.
 *
 * The body is read as raw text: Svix verifies the exact bytes the provider
 * signed, so the route must not let a body parser consume the stream first.
 */
export function webhookRoutes(deps: { webhookService: Pick<WebhookService, "handle"> }) {
  const controller = createWebhookController({ webhookService: deps.webhookService });

  return new Elysia({ name: "webhook" }).post("/v1/webhooks/resend", async ({ request, set }) => {
    const result = await controller.handle(request);
    set.status = result.status;
    return result.body;
  });
}
