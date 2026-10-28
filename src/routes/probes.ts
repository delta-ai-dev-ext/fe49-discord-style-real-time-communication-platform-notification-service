import { Elysia } from "elysia";

import type { Lifecycle } from "../lifecycle.ts";

export type ProbeDeps = {
  lifecycle: Lifecycle;
  checkDatabase: () => Promise<boolean>;
  /** When no Kafka consumer is wired (tests, tooling) readiness reports `disabled`. */
  kafkaEnabled?: boolean;
};

/**
 * `GET /healthz` (liveness) and `GET /readyz` (readiness).
 *
 * Liveness never touches Kafka or PostgreSQL — a dependency hiccup must not
 * restart the pod. Readiness requires a usable pool, an assigned consumer, and
 * a process that has not begun shutting down.
 */
export function probeRoutes(deps: ProbeDeps) {
  const kafkaEnabled = deps.kafkaEnabled ?? true;

  return new Elysia({ name: "probes" })
    .get("/healthz", () => ({ status: "ok" as const }))
    .get("/readyz", async ({ set }) => {
      const databaseOk = await safeCheck(deps.checkDatabase);
      const assigned = kafkaEnabled ? deps.lifecycle.isKafkaAssigned() : true;
      const kafkaState = kafkaEnabled ? (assigned ? "assigned" : "unassigned") : "disabled";

      if (deps.lifecycle.isShuttingDown() || !databaseOk || !assigned) {
        set.status = 503;
        return {
          status: "unavailable" as const,
          checks: { database: databaseOk ? ("ok" as const) : ("unavailable" as const), kafka_consumer: kafkaState },
        };
      }

      return {
        status: "ok" as const,
        checks: { database: "ok" as const, kafka_consumer: kafkaState },
      };
    });
}

async function safeCheck(check: () => Promise<boolean>): Promise<boolean> {
  try {
    return await check();
  } catch {
    return false;
  }
}
