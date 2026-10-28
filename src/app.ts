import { Elysia } from "elysia";

import type { Lifecycle } from "./lifecycle.ts";
import { metricsRoutes } from "./routes/metrics.ts";
import { probeRoutes } from "./routes/probes.ts";
import { webhookRoutes } from "./routes/webhook.ts";
import type { WebhookService } from "./services/webhookService.ts";

export type AppDeps = {
  lifecycle: Lifecycle;
  webhookService: Pick<WebhookService, "handle">;
  checkDatabase: () => Promise<boolean>;
  kafkaEnabled?: boolean;
};

/**
 * Elysia app factory. Routes are thin: probes evaluate readiness state, the
 * webhook route delegates to the service layer after reading the raw body.
 */
export function createApp(deps: AppDeps) {
  return new Elysia({ name: "notification_service" })
    .use(
      probeRoutes({
        lifecycle: deps.lifecycle,
        checkDatabase: deps.checkDatabase,
        ...(deps.kafkaEnabled === undefined ? {} : { kafkaEnabled: deps.kafkaEnabled }),
      }),
    )
    .use(metricsRoutes())
    .use(webhookRoutes({ webhookService: deps.webhookService }))
    .onError(({ error, set }) => {
      const known = error as { code?: string; message?: string };
      if (known.code === "PARSE") {
        set.status = 400;
        return { error: { code: "invalid_request_body", message: "Request body could not be parsed" } };
      }
      set.status = 500;
      return { error: { code: "internal_error", message: "Internal server error" } };
    });
}

export type App = ReturnType<typeof createApp>;
