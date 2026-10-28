import { describe, expect, it } from "bun:test";

import { ConfigError, configSchema, loadConfig } from "../../src/config.ts";

const REQUIRED = {
  DATABASE_URL: "postgres://user:pass@localhost:5432/notifications",
  KAFKA_BROKERS: "broker-1:9092,broker-2:9092",
  RESEND_API_KEY: "re_test",
  RESEND_WEBHOOK_SECRET: "whsec_test",
  RESEND_FROM_ADDRESS: "noreply@example.com",
};

describe("loadConfig", () => {
  it("applies documented defaults", () => {
    const config = loadConfig(REQUIRED);

    expect(config.PORT).toBe(3000);
    expect(config.KAFKA_CLIENT_ID).toBe("notification_service");
    expect(config.EMAIL_DAILY_LIMIT).toBe(100);
    expect(config.EMAIL_MONTHLY_LIMIT).toBe(3000);
    expect(config.SEND_MAX_ATTEMPTS).toBe(5);
    expect(config.SHUTDOWN_TIMEOUT_SECONDS).toBe(20);
    expect(config.LOG_LEVEL).toBe("info");
    expect(config.KAFKA_TLS).toBe(false);
    expect(config.KAFKA_SASL_MECHANISM).toBe("plain");
  });

  it("splits and trims the broker list", () => {
    const config = loadConfig({ ...REQUIRED, KAFKA_BROKERS: " a:9092 , b:9092 ," });
    expect(config.KAFKA_BROKERS).toEqual(["a:9092", "b:9092"]);
  });

  it("coerces numeric and boolean overrides", () => {
    const config = loadConfig({
      ...REQUIRED,
      PORT: "8080",
      EMAIL_DAILY_LIMIT: "250",
      SEND_MAX_ATTEMPTS: "7",
      KAFKA_TLS: "true",
      LOG_LEVEL: "debug",
    });

    expect(config.PORT).toBe(8080);
    expect(config.EMAIL_DAILY_LIMIT).toBe(250);
    expect(config.SEND_MAX_ATTEMPTS).toBe(7);
    expect(config.KAFKA_TLS).toBe(true);
    expect(config.LOG_LEVEL).toBe("debug");
  });

  it("accepts optional SASL credentials", () => {
    const config = loadConfig({
      ...REQUIRED,
      KAFKA_SASL_USERNAME: "user",
      KAFKA_SASL_PASSWORD: "secret",
      KAFKA_SASL_MECHANISM: "scram-sha-512",
    });

    expect(config.KAFKA_SASL_USERNAME).toBe("user");
    expect(config.KAFKA_SASL_MECHANISM).toBe("scram-sha-512");
  });

  it("fails fast with a structured error listing every missing variable", () => {
    let error: unknown;
    try {
      loadConfig({});
    } catch (thrown) {
      error = thrown;
    }

    expect(error).toBeInstanceOf(ConfigError);
    const issues = (error as ConfigError).issues.map((issue) => issue.path);
    expect(issues).toContain("DATABASE_URL");
    expect(issues).toContain("KAFKA_BROKERS");
    expect(issues).toContain("RESEND_API_KEY");
    expect(issues).toContain("RESEND_WEBHOOK_SECRET");
    expect(issues).toContain("RESEND_FROM_ADDRESS");
    expect((error as ConfigError).code).toBe("invalid_configuration");
  });

  it("rejects an empty value for a required variable", () => {
    const result = configSchema.safeParse({ ...REQUIRED, DATABASE_URL: "" });
    expect(result.success).toBe(false);
  });

  it("rejects an unknown log level", () => {
    const result = configSchema.safeParse({ ...REQUIRED, LOG_LEVEL: "trace" });
    expect(result.success).toBe(false);
  });

  it("treats blank optional values as absent", () => {
    const config = loadConfig({ ...REQUIRED, KAFKA_CLIENT_ID: "   ", PORT: "" });
    expect(config.KAFKA_CLIENT_ID).toBe("notification_service");
    expect(config.PORT).toBe(3000);
  });
});
