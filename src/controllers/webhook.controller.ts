import type { WebhookService } from "../services/webhookService.ts";
import { readSvixHeaders } from "../models/webhook.ts";

export type WebhookControllerDeps = {
  webhookService: Pick<WebhookService, "handle">;
};

/**
 * Thin request handling: hands the **raw** body and the `svix-*` headers to the
 * service, which verifies the signature before parsing anything.
 */
export function createWebhookController(deps: WebhookControllerDeps) {
  return {
    async handle(request: Request): Promise<{ status: number; body: unknown }> {
      const rawBody = await request.text();
      const headers = readSvixHeaders(request.headers);
      return deps.webhookService.handle(rawBody, headers);
    },
  };
}
