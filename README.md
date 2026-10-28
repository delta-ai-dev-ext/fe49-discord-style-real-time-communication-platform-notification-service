# notification_service

Consumes platform events (`friend_request_sent`, `invite_email_requested`) from Kafka and sends
transactional email through Resend with **exactly-once effect**, while tracking delivery outcomes
from Svix-signed Resend webhooks. TypeScript + Bun (Elysia, zod, drizzle-orm/pg, kafkajs, resend,
svix, winston, prom-client).

Design: `docs/architecture.md` (§3 flows, §5 schema, §8 metrics), `docs/engineering_guide.md`,
`docs/openapi_spec.yaml`, and `docs/decisions/ADR-001`, `ADR-002`.

## Layout

```
src/
├── index.ts         # startup order: config → DB → migrations → HTTP → Kafka → ready
├── app.ts           # Elysia app factory (probes, /metrics, webhook)
├── config.ts        # zod-validated env (fail fast)
├── lifecycle.ts     # readiness state, in-flight drain, graceful shutdown
├── logger.ts        # winston JSON logs (trace_id, sanitized fields)
├── metrics.ts       # every architecture §8 metric (prom-client)
├── consumers/       # Kafka consumer group `notification_service` (manual offset commit)
├── routes/          # /healthz, /readyz, /metrics, /v1/webhooks/resend
├── controllers/     # thin request handling
├── services/        # pipeline, quota gate, templates, provider, webhook, scheduler
├── repositories/    # the only SQL in the codebase
├── models/          # zod event/webhook contracts
└── db/              # drizzle schema, migrations, pool
```

## Behaviour highlights

- **Exactly-once**: `email_delivery.source_event_id` is UNIQUE (Kafka dedupe) and the same
  `event_id` is the Resend idempotency key, so a crash between "provider accepted" and "record
  updated" cannot duplicate an email. A `queued` row found on redelivery is re-sent with the same key.
- **Quota**: at the daily/monthly cap the record becomes `deferred` (reason `quota`) with
  `next_retry_at` at the next UTC day/month boundary; `daily_send_counter` increments in the same
  transaction as the `sent` transition. A background scheduler retries due records.
- **Failures**: invalid payload → `failed/invalid_event`; expired invite → `failed/invite_expired`;
  429/5xx/network → exponential backoff with `attempts`; permanent provider error → `failed`
  immediately; exhausted retries → `failed` (the row is the dead-letter ledger).
- **Webhook**: Svix signature verified over the raw body + `svix-*` headers *before* JSON parsing
  (401 on failure), then `email.sent|delivered|bounced|complained` applied by `provider_message_id`;
  hard bounces and complaints upsert `email_suppression`.

## Configuration

| Variable | Required | Default |
|---|---|---|
| `DATABASE_URL` | yes | — |
| `KAFKA_BROKERS` | yes | — |
| `RESEND_API_KEY` | yes | — |
| `RESEND_WEBHOOK_SECRET` | yes | — |
| `RESEND_FROM_ADDRESS` | yes | — |
| `PORT` | no | `3000` |
| `KAFKA_CLIENT_ID` | no | `notification_service` |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` / `KAFKA_SASL_MECHANISM` | no | — / `plain` |
| `KAFKA_TLS` | no | `false` |
| `EMAIL_DAILY_LIMIT` | no | `100` |
| `EMAIL_MONTHLY_LIMIT` | no | `3000` |
| `SEND_MAX_ATTEMPTS` | no | `5` |
| `SHUTDOWN_TIMEOUT_SECONDS` | no | `20` |
| `LOG_LEVEL` | no | `info` |

## Commands

```bash
make dev        # bun --watch src/index.ts
make run        # bun run src/index.ts
make test       # bun test --coverage (unit + integration, no infrastructure needed)
make typecheck  # tsc --noEmit
make build      # docker build -t notification_service:latest .
```
