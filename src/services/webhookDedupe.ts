/**
 * Dedupe ledger for provider webhook deliveries (ADR-001 failure mode 3).
 *
 * `docs/architecture.md` §5 pins the schema to exactly three tables, so the Svix
 * message-id ledger is kept in-process and bounded rather than in a fourth
 * table. Re-applying a provider status transition is already idempotent
 * (suppression upsert is unique on address, status writes are monotonic), so a
 * restart can at worst re-apply the same transition once.
 */
export interface WebhookDedupeStore {
  /** Returns `false` when the message id was already seen. */
  checkAndRemember(messageId: string): boolean;
  size(): number;
}

export function createWebhookDedupeStore(maxEntries = 5000): WebhookDedupeStore {
  const seen = new Set<string>();

  return {
    checkAndRemember(messageId: string): boolean {
      if (seen.has(messageId)) return false;
      seen.add(messageId);
      if (seen.size > maxEntries) {
        const oldest = seen.values().next();
        if (!oldest.done) seen.delete(oldest.value);
      }
      return true;
    },
    size(): number {
      return seen.size;
    },
  };
}
