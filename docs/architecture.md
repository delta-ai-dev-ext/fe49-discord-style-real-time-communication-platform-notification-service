# Architecture — notification_service

## 1. Service Responsibilities

`notification_service` turns platform events into transactional emails and tracks their delivery outcomes. It owns:

1. **Event consumption** — consuming friend-request and community-invite events from Kafka.
2. **Email rendering & sending** — rendering the friend-request and community-invitation templates and sending through the transactional email provider (Resend).
3. **Exactly-once delivery effect** — ensuring no event ever causes a duplicate email.
4. **Quota governance** — tracking daily and monthly send counts against the configured provider plan limits and deferring work rather than dropping it.
5. **Delivery tracking** — recording each email's lifecycle (`queued → sent → delivered`) and applying provider-reported outcomes (`bounced`, `complained`).
6. **Recipient suppression** — hard-bounced and complained addresses are suppressed from all future sends.
7. **Webhook intake** — one publicly routed, signature-verified webhook endpoint for provider status updates.

Explicit non-responsibilities: no in-app notification delivery (owned by `realtime_gateway`), no invite creation (owned by `community_service`), no SMS/push channels, no management/CRUD API in the MVP.

## 2. Internal Component Boundaries

```
┌──────────────────────────────────────────────────────────────┐
│                     notification_service pod                  │
│                                                              │
│  ┌────────────┐   ┌──────────────┐   ┌────────────────────┐  │
│  │ HTTP server │   │ Event        │   │ Email sender       │  │
│  │ (probes +   │──▶│ consumers    │──▶│ (template render,  │  │
│  │  webhook)   │   │ (2 topics)   │   │  quota gate,       │  │
│  └────────────┘   └──────────────┘   │  send, retry)      │  │
│        │                 │            └────────┬───────────┘  │
│        ▼                 ▼                     ▼              │
│  ┌──────────────────────────────────────────────────────────┐│
│  │ Delivery repository layer (EmailDelivery,               ││
│  │ EmailSuppression, DailySendCounter)                     ││
│  └──────────────────────────────────────────────────────────┘│
│                                │                             │
│                          PostgreSQL ── Kafka consumer group ──│
└──────────────────────────────────────────────────────────────┘
```

- **HTTP server** — thin: `/healthz`, `/readyz`, and `POST /v1/webhooks/resend`. Owns nothing but signature verification, probe evaluation, and delegation.
- **Event consumers** — one consumer group (`notification_service`) across two topic subscriptions; envelope parsing, event-id dedupe, dispatch to the email sender.
- **Email sender** — quota gate → suppression check → render template → provider call with idempotency key → record outcome; retries transient failures, dead-letters persistent ones.
- **Repository layer** — all SQL; the only component touching PostgreSQL.

## 3. Asynchronous Event Flows

### 3.1 Consumed topics & envelope

All events share the envelope:

```
{ event_id: uuid, event_type, occurred_at: ISO-8601, producer, version, trace_parent, data }
```

| Topic | Partition key | Consumed event types |
|---|---|---|
| `identity_friend_events` | `recipient_user_id` | `friend_request_sent` |
| `community_invite_events` | `community_id` | `invite_email_requested` |

Only the event types listed produce emails; other event types on the same topics are acknowledged without side effects.

### 3.2 Consumption flow (happy path)

```
Kafka message
  → parse envelope; validate `data` against the event contract
  → dedupe: has event_id already been processed?  (EmailDelivery row exists) → ack, stop
  → suppression check: recipient email suppressed? → record status=suppressed → ack
  → quota gate: daily/monthly cap reached? → record status=deferred (reason=quota) → ack (no re-delivery loop)
  → render template (friend request | community invite) from event data
  → insert EmailDelivery row (status=queued, source_event_id=event_id) in the SAME step as sending
  → provider send call carrying event_id as the provider idempotency key
  → on success: update record (status=sent, provider_message_id, sent_at); ack message
```

The consumer commits the offset only after the record update succeeds, so a crash between send and update is repaired by redelivery plus idempotency (§4.1).

### 3.3 Failure paths

- **Invalid payload / missing recipient email** → record `failed` (reason=`invalid_event`), ack, log error. Malformed events must not wedge the partition.
- **Transient provider error** (429, 5xx, network) → keep the record `queued`, increment `attempts`, retry with exponential backoff capped by a max attempt count.
- **Permanent provider error** (invalid from address, unverified domain) → record `failed` immediately; no retry.
- **Retries exhausted** → record `failed` (dead-letter state). Metrics expose the dead-letter count; there is no dead-letter topic in the MVP — the database row is the dead-letter ledger.

### 3.4 Webhook flow (Resend → notification_service)

```
POST /v1/webhooks/resend
  → read raw body + Svix headers (svix-id, svix-timestamp, svix-signature)
  → verify signature against shared secret (reject 401 on failure or missing headers)
  → dedupe by Svix message id
  → map event: email.sent|email.delivered → status transition
               email.bounced → status=bounced (+ suppression if type=hard)
               email.complained → status=complained (+ suppression)
  → match delivery record by provider message id; unknown ids are accepted and ignored
  → return 200
```

Suppression inserts are idempotent (unique on address).

## 4. Resilience Patterns

### 4.1 Exactly-once effect via idempotency chain

1. **Kafka-level dedupe**: `EmailDelivery.source_event_id` is unique; a redelivered event finds the existing row and is acked without a send.
2. **Provider-level idempotency**: the event id doubles as the Resend idempotency key, so a crash after "provider accepted" but before "record updated" cannot duplicate the email — the retry call returns the original message id.
3. **Webhook-level dedupe**: Svix message ids are recorded; duplicates do not re-apply status transitions.

### 4.2 Backpressure & ordering

- Single consumer group; partition ordering per key preserves per-entity order.
- The quota gate defers work to the **next UTC day** (records with `status=deferred, reason=quota`) and a scheduler retries deferred records at day rollover before new sends; nothing is dropped during cap spikes.
- `notification_daily_quota_used_ratio` is the alert signal (warn at ≥ 0.8).

## 5. PostgreSQL Database Schema

Schema owned and defined here; no cross-service table sharing.

```sql
CREATE TABLE email_delivery (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_event_id     UUID        NOT NULL UNIQUE,          -- Kafka event_id; dedupe + Resend idempotency key
    kind                TEXT        NOT NULL
                        CHECK (kind IN ('friend_request', 'community_invite')),
    recipient_email     TEXT        NOT NULL,                 -- lowercased at write
    sender_user_id      TEXT        NULL,                     -- friend_request context
    recipient_user_id   TEXT        NULL,
    invite_code         TEXT        NULL,                     -- community_invite context
    community_id        TEXT        NULL,
    provider_message_id TEXT        NULL,                     -- Resend message id (set on successful send)
    status              TEXT        NOT NULL DEFAULT 'queued'
                        CHECK (status IN ('queued','sent','delivered',
                                          'bounced','complained',
                                          'deferred','suppressed','failed')),
    failure_reason      TEXT        NULL,                     -- e.g. 'quota','transient','invite_expired','invalid_event'
    attempts            INTEGER     NOT NULL DEFAULT 0
                        CHECK (attempts >= 0),
    next_retry_at       TIMESTAMPTZ NULL,                     -- backoff scheduling for deferred/transient
    sent_at             TIMESTAMPTZ NULL,
    delivered_at        TIMESTAMPTZ NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_email_delivery_status_retry
    ON email_delivery (status, next_retry_at)
    WHERE status IN ('queued', 'deferred');
CREATE INDEX idx_email_delivery_provider_message
    ON email_delivery (provider_message_id)
    WHERE provider_message_id IS NOT NULL;

CREATE TABLE email_suppression (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT        NOT NULL UNIQUE,              -- lowercased
    reason        TEXT        NOT NULL
                  CHECK (reason IN ('hard_bounce', 'complaint')),
    provider_message_id TEXT  NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE daily_send_counter (
    day            DATE     PRIMARY KEY,                   -- UTC date
    sent_count     INTEGER  NOT NULL DEFAULT 0
                   CHECK (sent_count >= 0),
    deferred_count INTEGER  NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Notes:
- `email_delivery.source_event_id UNIQUE` is the Kafka dedupe constraint.
- Daily quota accounting increments `daily_send_counter` in the same transaction as the `sent` status transition; the UTC day is computed server-side.
- Webhook status transitions update the row matched by `provider_message_id`.

## 6. External Provider Integration (Resend)

- **Outbound**: send email API, authenticated by API key from configuration. Every send carries the event id as the idempotency key. Payload = rendered template (from address, recipient, subject, body). Sender domain and from address come from configuration.
- **Inbound**: `POST /v1/webhooks/resend`, Svix-signed. Auth is signature-only; payload verification failure → `401`. Idempotent by Svix message id.
- **Failure handling**: transient errors retried with exponential backoff; permanent errors and exhausted retries become `failed`; quota exhaustion defers.
- **Tracing**: `trace_parent` from the Kafka envelope header is propagated into provider call spans; webhook spans derive a trace from the request.

## 7. Stateless Kubernetes Operational Model

- **Stateless pods** (`replicas >= 1`): no on-disk state; all durable state lives in PostgreSQL and Kafka offsets.
- **Liveness (`/healthz`)**: event-loop responsiveness only; must not fail on Kafka/DB hiccups.
- **Readiness (`/readyz`)**: PostgreSQL reachable AND Kafka consumer assigned; returns 503 once shutdown starts.
- **Graceful shutdown (`SIGTERM`/`SIGINT`)**:
  1. Mark readiness failing (pod removed from routing).
  2. Stop accepting new HTTP requests; stop polling for new Kafka messages.
  3. Drain in-flight event processing and HTTP requests within a bounded timeout (~15–30 s).
  4. Commit Kafka offsets, close the consumer and DB pool, exit `0`.
- **Crash safety**: uncommitted events are redelivered after restart; the idempotency chain (§4.1) prevents duplicate emails.

## 8. Observability

### Business metrics
| Metric | Type | Labels |
|---|---|---|
| `notification_emails_sent_total` | counter | `kind` (friend_request, community_invite) |
| `notification_emails_delivered_total` | counter | `kind` |
| `notification_emails_bounced_total` | counter | `kind`, `type` (hard, soft) |
| `notification_emails_complained_total` | counter | `kind` |
| `notification_emails_suppressed_total` | counter | `reason` (hard_bounce, complaint) |

### Performance / health metrics
| Metric | Type | Labels |
|---|---|---|
| `notification_resend_api_duration_seconds` | histogram | — |
| `notification_resend_api_errors_total` | counter | `status_class` |
| `notification_daily_quota_used_ratio` | gauge (alert ≥ 0.8) | — |
| `notification_emails_deferred_total` | counter | `reason` (quota, transient) |
| `kafka_consumer_lag` | gauge | `topic`, `group` |

Labels are low-cardinality only — never `user_id`, `community_id`, `event_id`, message ids, or timestamps.

### SLOs
- Event-to-send p95 < 30 s when under quota.
- Delivery success ≥ 98%.
- Zero duplicate sends (enforced by the idempotency chain, verified by tests).

### Tracing
- W3C `traceparent` propagated from the Kafka envelope (`trace_parent` header/field) into all spans, including provider calls; webhook spans start from the inbound request. Every log line carries the active `trace_id`.
