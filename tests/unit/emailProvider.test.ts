import { beforeEach, describe, expect, it } from "bun:test";

import { registry } from "../../src/metrics.ts";
import { classifyProviderError, createResendProvider } from "../../src/services/emailProvider.ts";
import { histogramCount, metricValue } from "../mocks/metrics.ts";

const input = {
  to: "user@example.com",
  subject: "Hello",
  html: "<p>Hello</p>",
  text: "Hello",
  idempotencyKey: "11111111-1111-4111-8111-111111111111",
};

beforeEach(() => {
  registry.resetMetrics();
});

describe("classifyProviderError", () => {
  it("treats 429 and 5xx as transient", () => {
    expect(classifyProviderError({ name: "rate_limit_exceeded", statusCode: 429 }).kind).toBe("transient");
    expect(classifyProviderError({ name: "internal_server_error", statusCode: 500 }).kind).toBe("transient");
    expect(classifyProviderError({ name: "application_error", statusCode: 503 }).kind).toBe("transient");
    expect(classifyProviderError({ statusCode: 502 }).kind).toBe("transient");
  });

  it("treats transport failures without a status code as transient", () => {
    expect(classifyProviderError({ name: "network_error", message: "socket hang up" }).kind).toBe("transient");
    expect(classifyProviderError({ name: "TypeError", message: "fetch failed" }).kind).toBe("transient");
  });

  it("treats permanent provider rejections as permanent", () => {
    expect(classifyProviderError({ name: "invalid_from_address", statusCode: 422 }).kind).toBe("permanent");
    expect(classifyProviderError({ name: "validation_error", statusCode: 422 }).kind).toBe("permanent");
    expect(classifyProviderError({ name: "missing_required_field", statusCode: 422 }).kind).toBe("permanent");
    expect(classifyProviderError({ name: "invalid_api_key", statusCode: 401 }).kind).toBe("permanent");
  });

  it("distinguishes exhausted provider quota", () => {
    expect(classifyProviderError({ name: "daily_quota_exceeded", statusCode: 429 }).kind).toBe("quota");
    expect(classifyProviderError({ name: "monthly_quota_exceeded", statusCode: 429 }).kind).toBe("quota");
  });
});

describe("createResendProvider", () => {
  function providerWith(send: (payload: unknown, options?: unknown) => Promise<unknown>) {
    const calls: { payload: unknown; options?: unknown }[] = [];
    const provider = createResendProvider({
      apiKey: "re_test",
      fromAddress: "noreply@example.com",
      client: {
        emails: {
          async send(payload: unknown, options?: unknown) {
            calls.push({ payload, options });
            return send(payload, options);
          },
        },
      },
      now: () => 0,
    });
    return { provider, calls };
  }

  it("returns the provider message id on success", async () => {
    const { provider, calls } = providerWith(async () => ({ data: { id: "resend-1" }, error: null }));

    const outcome = await provider.send(input);

    expect(outcome).toEqual({ ok: true, messageId: "resend-1" });
    expect(calls[0]!.payload).toMatchObject({ from: "noreply@example.com", to: "user@example.com" });
    expect(calls[0]!.options).toEqual({ idempotencyKey: input.idempotencyKey });
    expect(await histogramCount("notification_resend_api_duration_seconds")).toBe(1);
  });

  it("normalizes the resolved error field instead of throwing", async () => {
    const { provider } = providerWith(async () => ({
      data: null,
      error: { name: "invalid_from_address", message: "bad from", statusCode: 422 },
    }));

    const outcome = await provider.send(input);

    expect(outcome.ok).toBe(false);
    expect(outcome).toMatchObject({ error: { kind: "permanent", code: "invalid_from_address" } });
    expect(await metricValue("notification_resend_api_errors_total", { status_class: "4xx" })).toBe(1);
  });

  it("classifies a thrown transport error as transient", async () => {
    const { provider } = providerWith(async () => {
      throw new Error("socket hang up");
    });

    const outcome = await provider.send(input);

    expect(outcome).toMatchObject({ ok: false, error: { kind: "transient" } });
    expect(await metricValue("notification_resend_api_errors_total", { status_class: "network" })).toBe(1);
  });

  it("fails permanently when the response carries no message id", async () => {
    const { provider } = providerWith(async () => ({ data: {}, error: null }));

    const outcome = await provider.send(input);

    expect(outcome).toMatchObject({ ok: false, error: { kind: "permanent", code: "missing_message_id" } });
  });

  it("records 5xx errors under the 5xx status class", async () => {
    const { provider } = providerWith(async () => ({
      data: null,
      error: { name: "internal_server_error", message: "boom", statusCode: 500 },
    }));

    await provider.send(input);

    expect(await metricValue("notification_resend_api_errors_total", { status_class: "5xx" })).toBe(1);
  });
});
