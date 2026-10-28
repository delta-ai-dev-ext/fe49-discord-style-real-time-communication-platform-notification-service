import { Elysia } from "elysia";

import { metricsContentType, renderMetrics } from "../metrics.ts";

/** Standard Prometheus scrape endpoint for every metric in architecture §8. */
export function metricsRoutes() {
  return new Elysia({ name: "metrics" }).get("/metrics", async ({ set }) => {
    set.headers["content-type"] = metricsContentType();
    return renderMetrics();
  });
}
