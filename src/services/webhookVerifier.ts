import { Webhook } from "svix";

import { toSvixVerifyHeaders, type SvixHeaders } from "../models/webhook.ts";

/**
 * Svix signature verification. The official SDK verifier is applied to the
 * **raw request body** plus the three `svix-*` headers, before any JSON parsing.
 */
export interface WebhookVerifier {
  verify(rawBody: string, headers: SvixHeaders): boolean;
}

export function createSvixVerifier(secret: string): WebhookVerifier {
  const webhook = new Webhook(secret);

  return {
    verify(rawBody: string, headers: SvixHeaders): boolean {
      try {
        webhook.verify(rawBody, toSvixVerifyHeaders(headers));
        return true;
      } catch {
        return false;
      }
    },
  };
}
