# ADR-002: Quota-aware deferral and recipient suppression

**Status:** Accepted
**Date:** 2025-01-01
**Deciders:** tech-lead

## Context

Resend's free plan caps sends at 100/day and 3,000/month. During growth spikes (e.g. a wave of community invitations) the daily cap can be reached mid-day. Silently dropping events at the cap would lose user notifications with no recovery path, and continuing to send to hard-bounced or complaining addresses would damage sender reputation and risk provider suspension.

## Decision

1. **Deferral over dropping.** When the daily (or monthly) cap is reached, the delivery record transitions to `deferred` with `failure_reason = 'quota'` and the event is acknowledged — no re-delivery loop back into Kafka. A scheduler retries deferred records at the UTC day rollover (or month boundary), before new sends, and without violating the new period's cap. If deferral persists beyond a retention window, the record transitions to `failed`.
2. **Quota accounting in the send transaction.** `daily_send_counter` is incremented in the same database transaction as the `sent` status transition, so the counter cannot drift from reality under concurrency.
3. **Suppression on hard bounce and complaint.** Provider webhooks of type `email.bounced` with `type=hard`, and `email.complained`, upsert the address into `email_suppression` (unique on address, idempotent). Suppressed addresses short-circuit before any render/send step. Soft bounces never suppress; the record stays retryable.
4. **Alerting.** `notification_daily_quota_used_ratio` (gauge, alert ≥ 0.8) is the early-warning signal; the operational upgrade path is plan upgrade, not code change.

## Consequences

- No notification is silently lost during cap spikes; they arrive the next day at worst.
- Deferred records make quota pressure visible and quantifiable in the database, not just in metrics.
- Suppression guarantees degraded (but safe) behavior for problematic addresses; un-suppression is an operational follow-up outside the MVP.
- The scheduler adds a bounded background task; it must be drained on shutdown like the consumer.

## Alternatives considered

- **Drop events at the cap** — rejected: loses user notifications irrecoverably.
- **Queue in memory without DB rows** — rejected: a pod restart would lose deferred work; stateless pods must persist deferral.
- **Suppress on any bounce** — rejected: soft bounces are transient (full mailbox, temporary greylisting) and must not permanently silence an address.
