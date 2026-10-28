# User Stories — notification_service

## US-001: Friend request email notification

**As a** platform user who has been added as a friend by someone,
**I want to** receive an email when they send me a friend request,
**So that** I learn about the request even when I am not online.

### Acceptance Criteria
- Given a `friend_request_sent` event carrying the recipient's email and the sender's username, when the service processes the event, then an email is sent to the recipient and the delivery record's status ends as `sent`.
- Given an event whose recipient email is on the suppression list (hard bounce or complaint), when the service processes the event, then no email is sent and the delivery record is marked `suppressed` (counted in metrics).
- Given an event that is missing the recipient email, when the service processes the event, then the delivery record is marked `failed` without any send attempt and an error is logged.

### Out of Scope
- Live in-app notification delivery (owned by `realtime_gateway`).
- Rendering chat messages or DMs (not in this MVP).

## US-002: Community invitation email

**As a** person invited by email to join a community,
**I want to** receive an invitation email containing the community name, the inviter's username, and a working invite link,
**So that** I can preview and join the community.

### Acceptance Criteria
- Given an `invite_email_requested` event carrying `invite_url`, `community_name`, `inviter_username`, `recipient_email`, and `expires_at`, when the service processes the event, then the rendered email is sent through the provider and includes all of those fields.
- Given an event whose `expires_at` is already in the past when processed, when the service processes the event, then no email is sent and the delivery record is marked `failed` with the reason `invite_expired`.
- Given an event with an unresolvable or empty `invite_url`, when the service processes the event, then the send is rejected as invalid input and the delivery record is marked `failed`.

### Out of Scope
- Creating or revoking invites (owned by `community_service`).
- Sending SMS or push notifications.

## US-003: Exactly-once email effect on redelivery

**As a** platform operator,
**I want to** guarantee that a redelivered Kafka event never results in a second email to the same recipient,
**So that** users are never spammed by duplicates.

### Acceptance Criteria
- Given an event id that has already been recorded as processed, when the same event is consumed again (Kafka redelivery), then no provider call is made and the consumer acknowledges the message.
- Given a send that succeeded at the provider but whose record update failed, when the event is redelivered, then the provider is called again with the same idempotency key and returns the original message id without sending a second email.
- Given two different events that reference the same recipient, when both are processed, then two separate emails are sent (dedupe is per event, not per recipient).

### Out of Scope
- Batching multiple events into one email digest.

## US-004: Deferral when the daily send quota is exhausted

**As a** platform operator on a limited email plan,
**I want to** have remaining emails deferred to the next day instead of dropped when the daily cap is hit,
**So that** no user notification is silently lost.

### Acceptance Criteria
- Given the daily send counter has reached the configured daily cap, when an email event is consumed, then the delivery record is marked `deferred` with reason `quota`, no provider call is made, and a deferred metric is incremented.
- Given deferred events from the previous UTC day, when the UTC day rolls over and the counter resets, then deferred events are retried before new events are dropped and without violating the new day's cap.
- Given the monthly cap is also reached, when an email event is consumed, then the record is `deferred` until the next UTC month boundary; if deferral persists beyond the retention window the record becomes `failed`.

### Out of Scope
- Automatically upgrading the provider plan (manual operational follow-up).

## US-005: Transient provider failures are retried, then dead-lettered

**As a** platform operator,
**I want** transient provider errors to be retried with backoff and persistent failures to be surfaced,
**So that** emails are eventually delivered and stuck events are visible.

### Acceptance Criteria
- Given a transient provider error (e.g. 429/5xx), when a send fails, then the send is retried with exponential backoff and the record's `attempts` count is incremented.
- Given a send that has exhausted the maximum retry attempts, when the final attempt fails, then the delivery record is marked `failed` (dead-letter state) and the failure is counted in metrics.
- Given a permanent provider error (e.g. invalid `from` address, unverified domain), when the send fails, then the record is immediately marked `failed` without further retries.

### Out of Scope
- Human re-queue tooling for dead-lettered events (out of MVP).

## US-006: Suppress future sends after hard bounce or complaint

**As a** platform operator,
**I want to** stop emailing addresses that hard-bounced or complained,
**So that** sender reputation is protected and the recipient is not bothered.

### Acceptance Criteria
- Given a provider webhook reporting a hard bounce for an address, when the webhook is verified and accepted, then that address is added to the suppression list with reason `hard_bounce`.
- Given a provider webhook reporting a complaint for an address, when the webhook is verified and accepted, then that address is added to the suppression list with reason `complaint`.
- Given a suppressed address, when any subsequent email event targets it, then no send occurs and a suppressed metric is incremented.
- Given a webhook that reports a soft bounce, when accepted, then the address is NOT suppressed and the record is left retryable.

### Out of Scope
- Manual un-suppression UI (operational follow-up).

## US-007: Delivery status tracking through provider webhooks

**As a** platform operator,
**I want** each email's delivery record to reflect `sent`, `delivered`, `bounced`, or `complained` as reported by the provider,
**So that** delivery success can be measured against the ≥ 98% SLO.

### Acceptance Criteria
- Given a `sent`/`delivered`/`bounced`/`complained` webhook event matching a delivery record by provider message id, when the verified webhook is processed, then the record's status is updated accordingly with the event timestamp.
- Given a webhook referencing an unknown message id, when processed, then it is accepted with `200` but ignored (no error surfaced to the provider).
- Given the same webhook message id delivered twice, when the duplicate is processed, then no second status transition occurs (idempotent).

### Out of Scope
- Re-notifying users about delivery failures.

## US-008: Reject webhook payloads that fail signature verification

**As a** platform operator,
**I want to** reject any webhook request that fails provider signature verification,
**So that** attackers cannot forge delivery states or suppression entries.

### Acceptance Criteria
- Given a webhook request whose signature headers fail verification, when received, then the service responds `401` without processing the body.
- Given a request with missing signature headers, when received, then the service responds `401`.
- Given a validly signed request, when received, then it is processed and `200` is returned regardless of whether the event matched a record.

### Out of Scope
- Rate limiting the webhook endpoint (handled at ingress).

## US-009: Health and readiness probes

**As a** container orchestrator,
**I want to** know when the service is alive and ready to consume,
**So that** traffic is only routed to healthy pods.

### Acceptance Criteria
- Given the process is running, when `GET /healthz` is called, then it returns `200` with a status payload as long as the event loop is responsive — even if Kafka or the database is temporarily unavailable.
- Given the database is reachable and the Kafka consumer is assigned, when `GET /readyz` is called, then it returns `200`.
- Given the Kafka consumer is not yet assigned (or the DB is unreachable), when `GET /readyz` is called, then it returns `503`.

### Out of Scope
- Any other public endpoints (no management/CRUD API in the MVP).

## US-010: Graceful shutdown without losing or duplicating emails

**As a** platform operator,
**I want** pods to shut down cleanly on `SIGTERM`/`SIGINT`,
**So that** in-flight sends complete and no event is lost or double-processed across restarts.

### Acceptance Criteria
- Given a shutdown signal, when the pod receives it, then readiness starts failing immediately, in-flight event processing completes, the Kafka consumer commits offsets and stops, DB connections drain, and the process exits `0` within the shutdown timeout.
- Given a consumer that stops mid-batch during shutdown, when the pod restarts, then uncommitted events are reprocessed and the dedupe logic prevents duplicate emails.

### Out of Scope
- Zero-downtime in-place upgrades beyond normal rolling restarts.
