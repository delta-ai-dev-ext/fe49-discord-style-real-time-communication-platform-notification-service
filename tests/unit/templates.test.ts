import { describe, expect, it } from "bun:test";

import {
  escapeHtml,
  renderCommunityInviteEmail,
  renderDeferredRetryEmail,
  renderFriendRequestEmail,
} from "../../src/services/templates.ts";
import { backoffDelayMs, nextUtcDayStart, nextUtcMonthStart, parseTimestamp, utcDay, utcMonthRange } from "../../src/services/time.ts";
import { deliveryRow } from "../mocks/inMemory.ts";

describe("templates", () => {
  it("renders the friend-request email from the event", () => {
    const rendered = renderFriendRequestEmail({ sender_username: "Ada", recipient_email: "a@example.com" });

    expect(rendered.subject).toContain("Ada");
    expect(rendered.text).toContain("Ada sent you a friend request");
    expect(rendered.html).toContain("<strong>Ada</strong>");
  });

  it("renders every community invitation field", () => {
    const rendered = renderCommunityInviteEmail({
      community_name: "Rustaceans",
      inviter_username: "Grace",
      recipient_email: "invitee@example.com",
      invite_url: "https://app.example.com/invites/abc",
      expires_at: "2999-01-01T00:00:00.000Z",
    });

    expect(rendered.subject).toBe("You're invited to join Rustaceans");
    expect(rendered.html).toContain("Grace");
    expect(rendered.html).toContain("Rustaceans");
    expect(rendered.html).toContain('href="https://app.example.com/invites/abc"');
    expect(rendered.html).toContain("2999-01-01T00:00:00.000Z");
    expect(rendered.text).toContain("https://app.example.com/invites/abc");
  });

  it("escapes user-controlled fields to prevent HTML injection", () => {
    expect(escapeHtml('<script>alert("x")</script>')).toBe(
      "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;",
    );

    const rendered = renderCommunityInviteEmail({
      community_name: "<b>evil</b>",
      inviter_username: "Grace & Co",
      recipient_email: "invitee@example.com",
      invite_url: "https://example.com/\"onmouseover=\"alert(1)",
      expires_at: "2999-01-01T00:00:00.000Z",
    });

    expect(rendered.html).not.toContain("<b>evil</b>");
    expect(rendered.html).toContain("&lt;b&gt;evil&lt;/b&gt;");
    expect(rendered.html).toContain("Grace &amp; Co");
  });

  it("falls back to persisted fields for deferred retries", () => {
    const invite = renderDeferredRetryEmail(
      deliveryRow({ kind: "community_invite", communityId: "community-7" }),
    );
    expect(invite.text).toContain("community-7");

    const friend = renderDeferredRetryEmail(deliveryRow({ kind: "friend_request" }));
    expect(friend.subject).toContain("friend request");
  });
});

describe("UTC bucketing (ADR-002)", () => {
  it("computes the UTC day and the rollover instants", () => {
    const now = new Date("2025-03-15T23:30:00.000Z");

    expect(utcDay(now)).toBe("2025-03-15");
    expect(nextUtcDayStart(now).toISOString()).toBe("2025-03-16T00:00:00.000Z");
    expect(nextUtcMonthStart(now).toISOString()).toBe("2025-04-01T00:00:00.000Z");
  });

  it("computes the inclusive UTC month range", () => {
    expect(utcMonthRange(new Date("2025-03-15T10:00:00.000Z"))).toEqual({
      firstDay: "2025-03-01",
      lastDay: "2025-03-31",
    });
    expect(utcMonthRange(new Date("2024-02-10T10:00:00.000Z"))).toEqual({
      firstDay: "2024-02-01",
      lastDay: "2024-02-29",
    });
  });

  it("is independent of the process time zone", () => {
    // 23:30 local-time-free comparison: same instant, same UTC day.
    expect(utcDay(new Date("2025-01-01T00:00:00.000Z"))).toBe("2025-01-01");
    expect(utcDay(new Date("2025-01-01T23:59:59.999Z"))).toBe("2025-01-01");
  });

  it("parses timestamps safely", () => {
    expect(parseTimestamp("2025-03-15T10:00:00.000Z")?.toISOString()).toBe("2025-03-15T10:00:00.000Z");
    expect(parseTimestamp("nonsense")).toBeNull();
    expect(parseTimestamp(null)).toBeNull();
  });

  it("grows the backoff exponentially up to the cap", () => {
    expect(backoffDelayMs(1, 500, 30_000)).toBe(500);
    expect(backoffDelayMs(2, 500, 30_000)).toBe(1000);
    expect(backoffDelayMs(3, 500, 30_000)).toBe(2000);
    expect(backoffDelayMs(10, 500, 30_000)).toBe(30_000);
    expect(backoffDelayMs(0, 500, 30_000)).toBe(500);
  });
});
