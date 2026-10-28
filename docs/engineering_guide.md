# Engineering Guide — notification_service

## Runtime & Toolchain

| Concern | Choice |
|---|---|
| Language | TypeScript (strict mode, `moduleResolution: bundler`, target ES2020+) |
| Runtime | **Bun** (`bun run`, `bun --watch`); never `node` |
| Package manager | `bun` (`bun add` / `bun add -d`); lockfile `bun.lockb` committed |
| Build/test entrypoints | `Makefile` targets: `dev`, `test` (`bun test --coverage`), `build` (`docker build -t $(SERVICE_NAME):latest .`), `run`, `lint` (`bun x eslint src/ tests/`) |
| tsconfig | `"strict": true` required; ES modules only (`type: "module"` in package.json) |

## Libraries & External Provider SDKs

| Concern | Package / Library | Source | Usage Notes |
|---|---|---|---|
| HTTP framework | `elysia` | language-skill | Bun-native; use for `/healthz`, `/readyz`, webhook route; auto-validation with zod |
| Validation | `zod` | language-skill | Runtime validation of webhook payloads, Kafka event `data`, and env config; external inputs (provider payloads, Kafka events) are untrusted — validate at boundaries |
| Database ORM | `drizzle-orm` | language-skill | Type-safe schema for `email_delivery`, `email_suppression`, `daily_send_counter`; parameterized queries only |
| Database driver | `pg` | language-skill | PostgreSQL native driver; connection pool sized for the pod; closed on shutdown |
| Kafka client | `kafkajs` | tech-lead-research | Consumer group `notification_service`; subscribe to `identity_friend_events` and `community_invite_events`; manual offset commit after record update; SASL/TLS via env if configured |
| Email provider SDK | `resend` (official SDK) | tech-lead-research | `resend.emails.send(payload)` returns `{ data?: { id }, error? }` — handle the `error` field, do not assume throw; pass the Kafka `event_id` as the idempotency key per send |
| Webhook signature verification | `svix` (official SDK) | tech-lead-research | Use the standalone `Webhook(secret)` verifier against the **raw request body** + `svix-id`, `svix-timestamp`, `svix-signature` headers; verify before parsing JSON into business logic |
| Logging | `winston` | language-skill | Structured JSON logs to stdout; levels error/warn/info/debug; never `console.log` |
| Metrics | `prom-client` | language-skill | Register all metrics from `docs/architecture.md` §8 on the standard Prometheus endpoint |
| Date utilities | `date-fns` | language-skill | UTC-day/month bucketing for quota counters and backoff scheduling |
| Testing | `bun:test` | language-skill | Only approved runner; parallel-safe, deterministic |
| Mocking | `mock()` from `bun:test` | language-skill | Mock repos, Kafka consumer, Resend SDK, and Svix verifier at module boundaries |

## Container Lifecycle & Probes

- **Statelessness**: pods own no persistent local state; durable state is PostgreSQL + Kafka offsets. Safe with `replicas > 1`.
- **`GET /healthz` (liveness)**: returns `200` while the event loop is responsive. MUST NOT probe Kafka/PostgreSQL — a transient dependency hiccup must not trigger a pod restart.
- **`GET /readyz` (readiness)**: returns `200` only when the PostgreSQL pool is usable AND the Kafka consumer has assigned partitions; returns `503` once shutdown has begun.
- **`SIGTERM`/`SIGINT` graceful shutdown**:
  1. Flip readiness to failing immediately (the orchestrator stops routing).
  2. Stop accepting new HTTP requests and pause Kafka fetch loops.
  3. Drain in-flight event handlers and HTTP requests (bounded, ~15–30 s).
  4. Commit Kafka offsets, close the consumer and DB pool, exit `0`.
- Startup order: load & validate config → connect DB → run migrations → start HTTP server (probes up) → start Kafka consumer → mark ready.

## Testing Strategy

- **Structure**: `tests/unit/`, `tests/integration/`, `tests/mocks/` (shared fakes/stubs). Test files `*.test.ts`, run with `bun test --coverage`, ≥ 80% coverage, parallel execution with no shared mutable state.
- **Determinism**: no real network, no real infrastructure. Time-dependent logic (quota day rollover, backoff) uses injected clocks.
- **PostgreSQL**: repository layer tested against mock implementations mirroring real constraints (unique `source_event_id`, CHECK constraints).
- **Kafka**: consumer logic tested with a fake consumer that replays envelopes — including redelivery scenarios to prove event-id dedupe.
- **Resend**: SDK mocked with Bun `mock()`; cover `{ data }` success, `{ error }` responses, transient (429/5xx) vs permanent errors, and idempotency-key propagation assertions.
- **Svix**: verifier mocked for `true`/`false`/missing-header cases; one integration-style test asserts 401 ordering (verification happens before any body parsing).
- **Webhook handler integration tests** use the HTTP layer in-process (Elysia app instance) with all externals mocked.

## Coding Conventions & Project Structure

```
src/
├── index.ts            # entrypoint: app factory + server start + shutdown wiring
├── config.ts           # zod-validated env config
├── logger.ts           # winston structured logger
├── routes/             # Elysia routers (webhooks, probes)
├── controllers/        # thin request handling, delegates to services
├── services/           # business logic: consumer orchestration, quota gate,
│                       #   template rendering, suppression, retry policy
├── repositories/       # drizzle-orm data access (only SQL in the codebase)
├── models/             # zod schemas + TypeScript types (event contracts, webhook payloads)
├── db/                 # schema.ts, client setup
└── metrics.ts          # prom-client registrations
```

- **Layering**: routes/controllers → services → repositories. Controllers never touch the DB; repositories never contain business rules.
- **Errors**: domain errors carry stable machine-readable codes matching `docs/openapi_spec.yaml` (`webhook_signature_invalid`, `invalid_event`, ...); HTTP errors use the `{ error: { code, message, details? } }` contract; never expose stack traces or internals.
- **Typing**: strict TypeScript; event contracts and webhook payloads defined once in `models/` and reused by both consumer and webhook paths; no `any`.
- **ES modules only**; no CommonJS; no NodeJS-specific imports (`fs`, `path`, `crypto`) — use Bun-native APIs (`Bun.file()`, `crypto.getRandomValues()`).

## Configuration & Secrets Management

All configuration is 12-factor environment variables, validated at startup with zod (fail fast with a structured error if required vars are missing). No `.env` in production (allowed locally only, `.gitignore`d).

| Variable | Required | Purpose |
|---|---|---|
| `PORT` | no (default 3000) | HTTP server port |
| `DATABASE_URL` | yes | PostgreSQL connection string (portable) |
| `KAFKA_BROKERS` | yes | Comma-separated broker list |
| `KAFKA_CLIENT_ID` | no | Client id (default `notification_service`) |
| `KAFKA_SASL_*` / `KAFKA_TLS` | no | Optional SASL/TLS for the Kafka client |
| `RESEND_API_KEY` | yes | Resend API key (platform secret) |
| `RESEND_WEBHOOK_SECRET` | yes | Svix signing secret for webhook verification |
| `RESEND_FROM_ADDRESS` | yes | Verified from address for transactional emails |
| `EMAIL_DAILY_LIMIT` | no (default 100) | Daily send cap (provider plan) |
| `EMAIL_MONTHLY_LIMIT` | no (default 3000) | Monthly send cap (provider plan) |
| `SEND_MAX_ATTEMPTS` | no (default 5) | Max attempts before dead-letter `failed` |
| `SHUTDOWN_TIMEOUT_SECONDS` | no (default 20) | Graceful drain window |
| `LOG_LEVEL` | no (default info) | winston level |

## Observability & Logging Conventions

- **Structured JSON logs** with: `timestamp`, `severity`, `service_name` (`notification_service`), `trace_id`, `operation`, `status`, plus contextual fields (e.g. `event_id`, `kind`, `attempts` — never recipient email addresses or message bodies).
- **Trace propagation**: `trace_parent` from the Kafka envelope header → `trace_id` on every log line and span for that event; inbound webhook requests start a fresh trace (Svix ids provide correlation at the provider).
- **Metrics**: register every metric defined in `docs/architecture.md` §8 at startup via `prom-client`; low-cardinality labels only; `notification_daily_quota_used_ratio` alert threshold ≥ 0.8.
- Log every external call (Resend send, webhook verification) with operation, outcome, and latency; log state transitions of delivery records.

## Pinned Decisions

| Dependency / Area | Pin / Constraint | Rationale | Added By | Date |
|---|---|---|---|---|
| Resend idempotency | Kafka `event_id` doubles as the consumer dedupe key (`email_delivery.source_event_id`) and the Resend idempotency key | Exactly-once email effect across redeliveries and crash-recovery; single mechanism covers both failure modes | tech-lead | 2025-01-01 |
| Email provider | Official `resend` SDK only; no custom HTTP wrapper | SYSTEM_DESIGN mandates official SDK; `{data, error}` response shape must be handled, not assumed to throw | tech-lead | 2025-01-01 |
| Webhook verification | Official `svix` SDK `Webhook` verifier on the raw request body, before JSON parsing | Signature verification must see the exact bytes the provider signed | tech-lead | 2025-01-01 |
| Kafka client | `kafkajs` consumer group `notification_service`, manual offset commit after record update | Approved messaging library; commit-after-persist matches the idempotency chain | tech-lead | 2025-01-01 |
| Dead-letter model | No dead-letter topic; `email_delivery.status='failed'` rows are the dead-letter ledger surfaced via metrics | Single source of truth; avoids orphan topics in the MVP topology | tech-lead | 2025-01-01 |
| Quota policy | Defer on cap (next UTC day / month), never drop; alert at ratio ≥ 0.8 | SYSTEM_DESIGN §4.6 requirement; protects sender reputation and prevents silent loss | tech-lead | 2025-01-01 |
