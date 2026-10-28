import { describe, expect, it } from "bun:test";
import { Webhook } from "svix";

import { createSvixVerifier } from "../../src/services/webhookVerifier.ts";
import { createWebhookDedupeStore } from "../../src/services/webhookDedupe.ts";

const SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
const BODY = JSON.stringify({ type: "email.delivered", created_at: "2025-01-01T00:00:00.000Z", data: { email_id: "resend-1" } });

function signedHeaders(payload: string, messageId = "msg_1") {
  const timestamp = new Date();
  const signature = new Webhook(SECRET).sign(messageId, timestamp, payload);
  return {
    id: messageId,
    timestamp: String(Math.floor(timestamp.getTime() / 1000)),
    signature,
  };
}

describe("createSvixVerifier (real svix SDK)", () => {
  const verifier = createSvixVerifier(SECRET);

  it("accepts a signature produced by the official SDK", () => {
    expect(verifier.verify(BODY, signedHeaders(BODY))).toBe(true);
  });

  it("rejects a tampered body", () => {
    expect(verifier.verify(BODY.replace("resend-1", "resend-2"), signedHeaders(BODY))).toBe(false);
  });

  it("rejects a tampered signature", () => {
    expect(verifier.verify(BODY, { ...signedHeaders(BODY), signature: "v1,deadbeef" })).toBe(false);
  });

  it("rejects headers belonging to another message id", () => {
    const headers = signedHeaders(BODY, "msg_1");
    expect(verifier.verify(BODY, { ...headers, id: "msg_other" })).toBe(false);
  });

  it("rejects a stale timestamp", () => {
    const timestamp = new Date("2020-01-01T00:00:00.000Z");
    const signature = new Webhook(SECRET).sign("msg_1", timestamp, BODY);
    const headers = {
      id: "msg_1",
      timestamp: String(Math.floor(timestamp.getTime() / 1000)),
      signature,
    };
    expect(verifier.verify(BODY, headers)).toBe(false);
  });

  it("rejects an empty signature instead of throwing", () => {
    expect(verifier.verify(BODY, { id: "msg_1", timestamp: "1735689600", signature: "" })).toBe(false);
  });
});

describe("webhook dedupe store", () => {
  it("remembers message ids and reports duplicates", () => {
    const store = createWebhookDedupeStore();
    expect(store.checkAndRemember("msg_1")).toBe(true);
    expect(store.checkAndRemember("msg_1")).toBe(false);
    expect(store.checkAndRemember("msg_2")).toBe(true);
    expect(store.size()).toBe(2);
  });

  it("stays bounded", () => {
    const store = createWebhookDedupeStore(2);
    store.checkAndRemember("a");
    store.checkAndRemember("b");
    store.checkAndRemember("c");
    expect(store.size()).toBe(2);
    expect(store.checkAndRemember("a")).toBe(true);
  });
});
