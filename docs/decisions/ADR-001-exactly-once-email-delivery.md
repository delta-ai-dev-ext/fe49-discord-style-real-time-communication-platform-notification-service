# ADR-001: Exactly-once email delivery via event-id idempotency chain

**Status:** Accepted
**Date:** 2025-01-01
**Deciders:** tech-lead

## Context

`notification_service` consumes `identity_friend_events` and `community_invite_events` from Kafka. Kafka delivery guarantees are at-least-once, and the email send is a non-transactional external call to Resend. Three failure modes can otherwise produce duplicate emails:

1. Kafka redelivery of a message whose processing already succeeded.
2. Crash after the provider accepted the send but before the delivery record was updated — the redelivered event looks unprocessed.
3. Duplicate provider webhook deliveries distorting delivery metrics.

## Decision

The Kafka `event_id` is used as a **single idempotency key across the whole chain**:

- **Consumer dedupe:** `email_delivery.source_event_id` is `UNIQUE`; a redelivered event finds the existing row and is acknowledged without a send.
- **Provider idempotency:** the same `event_id` is passed as the Resend idempotency key on every send attempt. If a send succeeded but the record update failed, the retry returns the original provider message id instead of sending again.
- **Webhook dedupe:** Svix message ids are recorded; duplicate webhook deliveries do not re-apply status transitions.

The consumer commits Kafka offsets only after the delivery record update succeeds, making "crash before commit" recoverable by redelivery plus the idempotency chain.

## Consequences

- Duplicates are structurally impossible, not best-effort; the SLO "zero duplicate sends" is testable.
- Dedupe is per event, not per recipient: two different events legitimately targeting the same recipient each send one email.
- The Resend SDK's idempotency-key support is a hard dependency (documented in `engineering_guide.md` Pinned Decisions).
- Unit tests must assert the redelivery path for each failure mode.

## Alternatives considered

- **Deduplicate per recipient** — rejected: would swallow legitimate separate notifications (e.g. two different friend requests or invitations).
- **Transactional outbox on the consumer side** — rejected: the consumer has no state change of its own to co-publish; the delivery record + provider idempotency key already provide the same guarantee with less machinery.
