import type { FriendRequestSentData, InviteEmailRequestedData } from "../models/events.ts";
import type { DeliveryRecord } from "../repositories/emailDelivery.repository.ts";

export type RenderedEmail = {
  subject: string;
  html: string;
  text: string;
};

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/**
 * Friend-request template. Reads only the sender's username from the event —
 * never any other user data.
 */
export function renderFriendRequestEmail(data: FriendRequestSentData): RenderedEmail {
  const sender = escapeHtml(data.sender_username);
  return {
    subject: `${data.sender_username} sent you a friend request`,
    text: [
      `Hi,`,
      "",
      `${data.sender_username} sent you a friend request.`,
      "Open the app to accept or decline it.",
    ].join("\n"),
    html: [
      "<p>Hi,</p>",
      `<p><strong>${sender}</strong> sent you a friend request.</p>`,
      "<p>Open the app to accept or decline it.</p>",
    ].join(""),
  };
}

/** Community-invitation template: community name, inviter, and invite link. */
export function renderCommunityInviteEmail(data: InviteEmailRequestedData): RenderedEmail {
  const community = escapeHtml(data.community_name);
  const inviter = escapeHtml(data.inviter_username);
  const inviteUrl = escapeHtml(data.invite_url);
  return {
    subject: `You're invited to join ${data.community_name}`,
    text: [
      "Hi,",
      "",
      `${data.inviter_username} invited you to join the community "${data.community_name}".`,
      `Accept the invitation: ${data.invite_url}`,
      `This invitation expires on ${data.expires_at}.`,
    ].join("\n"),
    html: [
      "<p>Hi,</p>",
      `<p><strong>${inviter}</strong> invited you to join the community <strong>${community}</strong>.</p>`,
      `<p><a href="${inviteUrl}">Accept the invitation</a></p>`,
      `<p>This invitation expires on ${escapeHtml(data.expires_at)}.</p>`,
    ].join(""),
  };
}

/**
 * Fallback template used when a persisted record (deferred at the quota gate, or
 * left `queued` by a crash) has to be retried without the original event payload.
 * The schema in `docs/architecture.md` §5 stores no rendered body, so only the
 * persisted columns are available here.
 */
export function renderDeferredRetryEmail(record: DeliveryRecord): RenderedEmail {
  if (record.kind === "community_invite") {
    const communityId = record.communityId ?? record.inviteCode ?? "a community";
    return {
      subject: "Your community invitation is ready",
      text: [
        "Hi,",
        "",
        `Your invitation to the community "${communityId}" is waiting for you.`,
        "Open the app to accept it.",
      ].join("\n"),
      html: [
        "<p>Hi,</p>",
        `<p>Your invitation to the community <strong>${escapeHtml(communityId)}</strong> is waiting for you.</p>`,
        "<p>Open the app to accept it.</p>",
      ].join(""),
    };
  }

  return {
    subject: "You have a pending friend request",
    text: ["Hi,", "", "You have a pending friend request waiting in the app.", "Open the app to respond to it."].join(
      "\n",
    ),
    html: [
      "<p>Hi,</p>",
      "<p>You have a pending friend request waiting in the app.</p>",
      "<p>Open the app to respond to it.</p>",
    ].join(""),
  };
}
