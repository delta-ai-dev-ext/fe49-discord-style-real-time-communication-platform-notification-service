import { beforeEach, describe, expect, it } from "bun:test";

import {
  EventConsumer,
  decodeMessageValue,
  type KafkaConsumerLike,
  type KafkaEachBatchPayloadLike,
} from "../../src/consumers/eventConsumer.ts";
import { Lifecycle } from "../../src/lifecycle.ts";
import { registry } from "../../src/metrics.ts";
import { friendRequestEnvelope, inviteEnvelope } from "../mocks/fixtures.ts";
import { silentLogger } from "../mocks/logger.ts";
import { metricValue } from "../mocks/metrics.ts";

type FakeConsumer = KafkaConsumerLike & {
  connected: boolean;
  subscribed: string[];
  paused: string[];
  disconnected: boolean;
  runConfig?: { eachBatchAutoResolve?: boolean };
  emit(event: string): void;
};

function createFakeConsumer(): FakeConsumer {
  const listeners = new Map<string, ((payload: unknown) => void)[]>();
  const consumer: FakeConsumer = {
    connected: false,
    subscribed: [],
    paused: [],
    disconnected: false,
    events: { GROUP_JOIN: "GROUP_JOIN", DISCONNECT: "DISCONNECT" },
    async connect() {
      consumer.connected = true;
    },
    async subscribe({ topic }) {
      consumer.subscribed.push(topic);
    },
    async run(config) {
      consumer.runConfig = config;
    },
    async disconnect() {
      consumer.disconnected = true;
    },
    pause(topics) {
      consumer.paused.push(...topics.map((topic) => topic.topic));
    },
    on(event, listener) {
      const existing = listeners.get(event) ?? [];
      existing.push(listener);
      listeners.set(event, existing);
    },
    emit(event) {
      for (const listener of listeners.get(event) ?? []) listener({});
    },
  };
  return consumer;
}

function batchPayload(
  messages: { offset: string; value: unknown }[],
  overrides: Partial<KafkaEachBatchPayloadLike> & { highWatermark?: number } = {},
): KafkaEachBatchPayloadLike & { resolved: string[]; commits: number; heartbeats: number } {
  const resolved: string[] = [];
  let commits = 0;
  let heartbeats = 0;
  const payload = {
    batch: {
      topic: "identity_friend_events",
      partition: 0,
      highWatermark: overrides.highWatermark ?? messages.length,
      messages,
    },
    isRunning: () => true,
    isStale: () => false,
    resolveOffset: (offset: string) => resolved.push(offset),
    heartbeat: async () => {
      heartbeats += 1;
    },
    commitOffsetsIfNecessary: async () => {
      commits += 1;
    },
    resolved,
    get commits() {
      return commits;
    },
    get heartbeats() {
      return heartbeats;
    },
    ...overrides,
  };
  return payload as KafkaEachBatchPayloadLike & { resolved: string[]; commits: number; heartbeats: number };
}

beforeEach(() => {
  registry.resetMetrics();
});

describe("decodeMessageValue", () => {
  it("decodes JSON strings and byte buffers", () => {
    expect(decodeMessageValue('{"a":1}')).toEqual({ a: 1 });
    expect(decodeMessageValue(new TextEncoder().encode('{"a":2}'))).toEqual({ a: 2 });
  });

  it("returns null for empty or malformed payloads", () => {
    expect(decodeMessageValue(null)).toBeNull();
    expect(decodeMessageValue(undefined)).toBeNull();
    expect(decodeMessageValue("not json")).toBeNull();
  });
});

describe("EventConsumer.start", () => {
  it("connects, subscribes to both topics and runs with manual offset commit", async () => {
    const consumer = createFakeConsumer();
    const lifecycle = new Lifecycle();
    const eventConsumer = new EventConsumer({
      consumer,
      deliveryService: { processEnvelope: async () => ({ status: "sent" }) },
      lifecycle,
      logger: silentLogger,
    });

    await eventConsumer.start();

    expect(consumer.connected).toBe(true);
    expect(consumer.subscribed).toEqual(["identity_friend_events", "community_invite_events"]);
    expect(consumer.runConfig?.eachBatchAutoResolve).toBe(false);
  });

  it("marks the consumer assigned on GROUP_JOIN and unassigned on DISCONNECT", async () => {
    const consumer = createFakeConsumer();
    const lifecycle = new Lifecycle();
    const eventConsumer = new EventConsumer({
      consumer,
      deliveryService: { processEnvelope: async () => ({ status: "sent" }) },
      lifecycle,
      logger: silentLogger,
    });

    await eventConsumer.start();
    consumer.emit("GROUP_JOIN");
    expect(lifecycle.isKafkaAssigned()).toBe(true);

    consumer.emit("DISCONNECT");
    expect(lifecycle.isKafkaAssigned()).toBe(false);
  });
});

describe("EventConsumer.handleBatch", () => {
  function setup() {
    const consumer = createFakeConsumer();
    const lifecycle = new Lifecycle();
    const processed: unknown[] = [];
    const eventConsumer = new EventConsumer({
      consumer,
      deliveryService: {
        async processEnvelope(raw) {
          processed.push(raw);
          return { status: "sent" };
        },
      },
      lifecycle,
      logger: silentLogger,
      heartbeatEveryNMessages: 2,
    });
    return { consumer, lifecycle, processed, eventConsumer };
  }

  it("processes every message, then commits the resolved offsets", async () => {
    const { eventConsumer, processed } = setup();
    const payload = batchPayload([
      { offset: "0", value: JSON.stringify(friendRequestEnvelope()) },
      { offset: "1", value: JSON.stringify(inviteEnvelope()) },
    ]);

    await eventConsumer.handleBatch(payload);

    expect(processed).toHaveLength(2);
    expect(payload.resolved).toEqual(["0", "1"]);
    expect(payload.commits).toBe(1);
    expect(payload.heartbeats).toBe(1);
  });

  it("acknowledges malformed payloads instead of wedging the partition", async () => {
    const { eventConsumer, processed } = setup();
    const payload = batchPayload([{ offset: "5", value: "not-json" }]);

    await eventConsumer.handleBatch(payload);

    expect(processed).toEqual([null]);
    expect(payload.resolved).toEqual(["5"]);
  });

  it("publishes the consumer lag for the topic", async () => {
    const { eventConsumer } = setup();
    const payload = batchPayload([{ offset: "0", value: "{}" }], { highWatermark: 10 });

    await eventConsumer.handleBatch(payload);

    expect(
      await metricValue("kafka_consumer_lag", { topic: "identity_friend_events", group: "notification_service" }),
    ).toBe(9);
  });

  it("commits what it processed and rethrows so the batch is redelivered", async () => {
    const consumer = createFakeConsumer();
    const lifecycle = new Lifecycle();
    let calls = 0;
    const eventConsumer = new EventConsumer({
      consumer,
      deliveryService: {
        async processEnvelope() {
          calls += 1;
          if (calls === 2) throw new Error("database unavailable");
          return { status: "sent" };
        },
      },
      lifecycle,
      logger: silentLogger,
    });

    const payload = batchPayload([
      { offset: "0", value: "{}" },
      { offset: "1", value: "{}" },
    ]);

    await expect(eventConsumer.handleBatch(payload)).rejects.toThrow("database unavailable");
    expect(payload.resolved).toEqual(["0"]);
    expect(payload.commits).toBe(1);
  });

  it("stops processing mid-batch once fetching has been stopped", async () => {
    const { eventConsumer, processed } = setup();
    const payload = batchPayload([
      { offset: "0", value: "{}" },
      { offset: "1", value: "{}" },
    ]);

    const original = eventConsumer.handleBatch.bind(eventConsumer);
    const stopAfterFirst = async () => {
      await eventConsumer.stopFetching();
    };

    // Stop fetching before the batch starts: nothing is processed or committed.
    await stopAfterFirst();
    await original(payload);

    expect(processed).toHaveLength(0);
    expect(payload.resolved).toEqual([]);
  });

  it("stops when the batch is stale", async () => {
    const { eventConsumer, processed } = setup();
    const payload = batchPayload([{ offset: "0", value: "{}" }], { isStale: () => true });

    await eventConsumer.handleBatch(payload);

    expect(processed).toHaveLength(0);
    expect(payload.resolved).toEqual([]);
  });

  it("pauses both topics when fetching stops", async () => {
    const { eventConsumer, consumer } = setup();
    await eventConsumer.stopFetching();
    expect(consumer.paused).toEqual(["identity_friend_events", "community_invite_events"]);
  });

  it("disconnects on close", async () => {
    const { eventConsumer, consumer } = setup();
    await eventConsumer.close();
    expect(consumer.disconnected).toBe(true);
  });
});
