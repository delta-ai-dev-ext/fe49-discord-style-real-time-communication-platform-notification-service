import { CONSUMED_TOPICS, CONSUMER_GROUP } from "../config.ts";
import type { Lifecycle } from "../lifecycle.ts";
import { logger as defaultLogger, sanitize, type AppLogger } from "../logger.ts";
import { kafkaConsumerLag } from "../metrics.ts";

/** Minimal shape of the kafkajs consumer used here (substitutable in tests). */
export interface KafkaMessageLike {
  offset: string;
  value: unknown;
}

export interface KafkaBatchLike {
  topic: string;
  partition: number;
  highWatermark: string | number;
  messages: KafkaMessageLike[];
}

export interface KafkaEachBatchPayloadLike {
  batch: KafkaBatchLike;
  isRunning: () => boolean;
  isStale: () => boolean;
  resolveOffset: (offset: string) => void;
  heartbeat: () => Promise<void>;
  commitOffsetsIfNecessary?: () => Promise<void>;
}

export interface KafkaConsumerLike {
  connect(): Promise<void>;
  subscribe(subscription: { topic: string; fromBeginning?: boolean }): Promise<void>;
  run(config: {
    eachBatch: (payload: KafkaEachBatchPayloadLike) => Promise<void>;
    eachBatchAutoResolve?: boolean;
  }): Promise<void>;
  disconnect(): Promise<void>;
  pause?(topics: { topic: string }[]): void;
  resume?(topics: { topic: string }[]): void;
  on(event: string, listener: (payload: unknown) => void): void;
  events: { GROUP_JOIN: string; DISCONNECT: string };
}

export type EventProcessor = {
  processEnvelope(raw: unknown): Promise<{ status: string }>;
};

export type EventConsumerDeps = {
  consumer: KafkaConsumerLike;
  deliveryService: EventProcessor;
  lifecycle: Lifecycle;
  logger?: AppLogger;
  groupId?: string;
  topics?: readonly string[];
  heartbeatEveryNMessages?: number;
};

/** Decode a Kafka message value into JSON, tolerating Buffer/Uint8Array/string. */
export function decodeMessageValue(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  try {
    if (typeof value === "string") return JSON.parse(value);
    if (value instanceof Uint8Array) return JSON.parse(new TextDecoder().decode(value));
    return value;
  } catch {
    return null;
  }
}

/**
 * Kafka consumer for `identity_friend_events` and `community_invite_events`
 * in the `notification_service` group. Offsets are committed manually, after
 * the delivery record has been updated (§3.2).
 */
export class EventConsumer {
  private readonly consumer: KafkaConsumerLike;
  private readonly deliveryService: EventProcessor;
  private readonly lifecycle: Lifecycle;
  private readonly logger: AppLogger;
  private readonly groupId: string;
  private readonly topics: readonly string[];
  private readonly heartbeatEvery: number;
  private stopped = false;

  constructor(deps: EventConsumerDeps) {
    this.consumer = deps.consumer;
    this.deliveryService = deps.deliveryService;
    this.lifecycle = deps.lifecycle;
    this.logger = deps.logger ?? defaultLogger;
    this.groupId = deps.groupId ?? CONSUMER_GROUP;
    this.topics = deps.topics ?? CONSUMED_TOPICS;
    this.heartbeatEvery = deps.heartbeatEveryNMessages ?? 20;
  }

  async start(): Promise<void> {
    this.consumer.on(this.consumer.events.GROUP_JOIN, () => {
      this.lifecycle.setKafkaAssigned(true);
      this.logger.info({ operation: "kafka_consumer", status: "assigned", group: this.groupId });
    });
    this.consumer.on(this.consumer.events.DISCONNECT, () => {
      this.lifecycle.setKafkaAssigned(false);
    });

    await this.consumer.connect();
    for (const topic of this.topics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
    }
    this.stopped = false;
    await this.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: (payload) => this.handleBatch(payload),
    });
  }

  /** Stop polling for new records; in-flight work keeps running (drain). */
  async stopFetching(): Promise<void> {
    this.stopped = true;
    this.consumer.pause?.(this.topics.map((topic) => ({ topic })));
  }

  async close(): Promise<void> {
    this.stopped = true;
    await this.consumer.disconnect();
  }

  /**
   * Processes one batch sequentially and commits offsets only after the
   * corresponding delivery record has been persisted. A thrown error leaves the
   * remaining messages uncommitted so kafkajs redelivers them; the idempotency
   * chain makes that safe.
   */
  async handleBatch(payload: KafkaEachBatchPayloadLike): Promise<void> {
    const { batch } = payload;
    let processed = 0;
    let lastOffset: string | null = null;

    try {
      for (const message of batch.messages) {
        if (this.stopped || this.lifecycle.isShuttingDown()) break;
        if (payload.isStale()) break;

        const decoded = decodeMessageValue(message.value);
        await this.lifecycle.track(async () => {
          await this.deliveryService.processEnvelope(decoded);
        });

        payload.resolveOffset(message.offset);
        lastOffset = message.offset;
        processed += 1;

        if (processed % this.heartbeatEvery === 0) {
          await payload.heartbeat();
        }
      }
    } catch (error) {
      this.logger.error({
        operation: "kafka_batch",
        status: "error",
        topic: batch.topic,
        partition: batch.partition,
        error: error instanceof Error ? error.message : String(error),
      });
      await payload.commitOffsetsIfNecessary?.();
      throw error;
    }

    await payload.commitOffsetsIfNecessary?.();

    if (lastOffset !== null) {
      const lag = Number(batch.highWatermark) - (Number(lastOffset) + 1);
      kafkaConsumerLag.set({ topic: batch.topic, group: this.groupId }, Number.isFinite(lag) && lag > 0 ? lag : 0);
    }

    this.logger.debug(
      sanitize({
        operation: "kafka_batch",
        status: "committed",
        topic: batch.topic,
        partition: batch.partition,
        processed,
      }),
    );
  }
}
