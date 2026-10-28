import { Kafka, type KafkaConfig, type SASLOptions } from "kafkajs";

import { createApp } from "./app.ts";
import { CONSUMER_GROUP, loadConfig, type Config } from "./config.ts";
import { EventConsumer } from "./consumers/eventConsumer.ts";
import { closePool, createDb, createPool, pingDatabase } from "./db/client.ts";
import { runMigrations } from "./db/migrations.ts";
import { Lifecycle, gracefulShutdown } from "./lifecycle.ts";
import { configureLogger, logger } from "./logger.ts";
import { DeferredScheduler } from "./services/deferredScheduler.ts";
import { DeliveryService } from "./services/deliveryService.ts";
import { createResendProvider } from "./services/emailProvider.ts";
import { createWebhookDedupeStore } from "./services/webhookDedupe.ts";
import { WebhookService } from "./services/webhookService.ts";
import { createSvixVerifier } from "./services/webhookVerifier.ts";

/**
 * Startup order (engineering guide "Kubernetes Lifecycle & Probes"):
 * config → DB → migrations → HTTP probes up → Kafka consumer → ready.
 */
async function main(): Promise<void> {
  let config: Config;
  try {
    config = loadConfig();
  } catch (error) {
    logger.error({
      service_name: "notification_service",
      operation: "startup",
      status: "failed",
      error: error instanceof Error ? error.message : String(error),
      issues: (error as { issues?: unknown }).issues,
    });
    process.exit(1);
    return;
  }

  configureLogger(config.LOG_LEVEL);

  const pool = createPool(config.DATABASE_URL);
  const db = createDb(pool);
  const lifecycle = new Lifecycle();

  await runMigrations(db);
  logger.info({ service_name: "notification_service", operation: "migrations", status: "applied" });

  const provider = createResendProvider({
    apiKey: config.RESEND_API_KEY,
    fromAddress: config.RESEND_FROM_ADDRESS,
  });

  const deliveryService = new DeliveryService({
    db,
    provider,
    config: {
      dailyLimit: config.EMAIL_DAILY_LIMIT,
      monthlyLimit: config.EMAIL_MONTHLY_LIMIT,
      maxAttempts: config.SEND_MAX_ATTEMPTS,
    },
  });

  const webhookService = new WebhookService({
    db,
    verifier: createSvixVerifier(config.RESEND_WEBHOOK_SECRET),
    dedupe: createWebhookDedupeStore(),
  });

  const app = createApp({
    lifecycle,
    webhookService,
    checkDatabase: () => pingDatabase(pool),
    kafkaEnabled: true,
  });

  app.listen(config.PORT);
  logger.info({ service_name: "notification_service", operation: "http", status: "listening", port: config.PORT });

  const kafka = new Kafka(buildKafkaConfig(config));
  const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  const eventConsumer = new EventConsumer({ consumer, deliveryService, lifecycle });

  const scheduler = new DeferredScheduler({ deliveryService });
  scheduler.start();

  await eventConsumer.start();
  logger.info({ service_name: "notification_service", operation: "kafka_consumer", status: "started" });

  const shutdownTimeoutMs = config.SHUTDOWN_TIMEOUT_SECONDS * 1000;
  let shuttingDown = false;

  const shutdown = (signal: string): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ service_name: "notification_service", operation: "shutdown", status: "signalled", signal });

    void gracefulShutdown({
      lifecycle,
      http: {
        stop: async () => {
          await app.stop();
        },
      },
      consumer: { stopFetching: () => eventConsumer.stopFetching(), close: () => eventConsumer.close() },
      scheduler,
      pool: { end: () => closePool(pool) },
      timeoutMs: shutdownTimeoutMs,
      exit: (code) => process.exit(code),
    }).catch((error: unknown) => {
      logger.error({
        service_name: "notification_service",
        operation: "shutdown",
        status: "failed",
        error: error instanceof Error ? error.message : String(error),
      });
      process.exit(1);
    });
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

function buildKafkaConfig(config: Config): KafkaConfig {
  const sasl: SASLOptions | undefined =
    config.KAFKA_SASL_USERNAME && config.KAFKA_SASL_PASSWORD
      ? {
          mechanism: config.KAFKA_SASL_MECHANISM,
          username: config.KAFKA_SASL_USERNAME,
          password: config.KAFKA_SASL_PASSWORD,
        }
      : undefined;

  return {
    clientId: config.KAFKA_CLIENT_ID,
    brokers: config.KAFKA_BROKERS,
    ssl: config.KAFKA_TLS,
    ...(sasl ? { sasl } : {}),
  };
}

void main();
