/**
 * sessions_send helper logic.
 *
 * Resolves announcement targets, channel/session routing metadata, and ping-pong guard prompt text.
 */
import {
  getChannelPlugin,
  normalizeChannelId as normalizeAnyChannelId,
} from "../../channels/plugins/index.js";
import { resolveSessionConversationRef } from "../../channels/plugins/session-conversation.js";
import { normalizeChatChannelId } from "../../channels/registry.js";
import { parseSessionDeliveryRoute } from "../../sessions/session-key-utils.js";
export { isNonDeliverableSessionsReply } from "./sessions-send-tokens.js";

export type AnnounceTarget = {
  channel: string;
  to: string;
  accountId?: string;
  threadId?: string; // Forum topic/thread ID
};

/** Resolves a session key into the channel target used for source-reply announcements. */
export function resolveAnnounceTargetFromKey(sessionKey: string): AnnounceTarget | null {
  const parsed = resolveSessionConversationRef(sessionKey);
  if (!parsed) {
    const directRoute = parseSessionDeliveryRoute(sessionKey);
    if (!directRoute || (directRoute.peerKind !== "direct" && directRoute.peerKind !== "dm")) {
      return null;
    }

    const normalizedChannel =
      normalizeAnyChannelId(directRoute.channel) ?? normalizeChatChannelId(directRoute.channel);
    const channel = normalizedChannel ?? directRoute.channel;
    const messaging = normalizedChannel
      ? getChannelPlugin(normalizedChannel)?.messaging
      : undefined;
    // Session peers are canonical; adapters restore API casing at their boundary.
    // Channel-style resolvers must not turn an explicit direct user into a room.
    const resolvedTarget =
      messaging?.directTargetStyle === "user-prefixed"
        ? undefined
        : messaging?.resolveDeliveryTarget?.({ conversationId: directRoute.peerId });
    const directTarget = `user:${directRoute.peerId}`;

    return {
      channel,
      to: resolvedTarget?.to?.trim() || messaging?.normalizeTarget?.(directTarget) || directTarget,
      ...(directRoute.accountId ? { accountId: directRoute.accountId } : {}),
      threadId: resolvedTarget?.threadId ?? directRoute.threadId,
    };
  }
  const normalizedChannel =
    normalizeAnyChannelId(parsed.channel) ?? normalizeChatChannelId(parsed.channel);
  const channel = normalizedChannel ?? parsed.channel;
  const plugin = normalizedChannel ? getChannelPlugin(normalizedChannel) : null;
  const genericTarget = parsed.kind === "channel" ? `channel:${parsed.id}` : `group:${parsed.id}`;
  // Prefer plugin-owned target normalization so channel-specific IDs and topics survive routing.
  const normalized =
    plugin?.messaging?.resolveSessionTarget?.({
      kind: parsed.kind,
      id: parsed.id,
      threadId: parsed.threadId,
    }) ?? plugin?.messaging?.normalizeTarget?.(genericTarget);
  return {
    channel,
    to: normalized ?? (normalizedChannel ? genericTarget : parsed.id),
    threadId: parsed.threadId,
  };
}

function buildAgentSessionLines(params: {
  requesterSessionKey?: string;
  requesterChannel?: string;
  targetSessionKey: string;
  targetChannel?: string;
}): string[] {
  return [
    // Session keys are high-cardinality (thread/run ids), so concrete values churn the
    // system prompt and break provider prompt-cache reuse across A2A turns. Channels are
    // low-cardinality and inform reply formatting, so they stay concrete.
    params.requesterSessionKey ? "Agent 1 (requester) session: <REQUESTER_SESSION>." : undefined,
    params.requesterChannel
      ? `Agent 1 (requester) channel: ${params.requesterChannel}.`
      : undefined,
    "Agent 2 (target) session: <TARGET_SESSION>.",
    params.targetChannel ? `Agent 2 (target) channel: ${params.targetChannel}.` : undefined,
  ].filter((line): line is string => Boolean(line));
}

/** Builds the initial prompt context for a sessions_send agent-to-agent request. */
export function buildAgentToAgentMessageContext(params: {
  requesterSessionKey?: string;
  requesterChannel?: string;
  targetSessionKey: string;
}) {
  const lines = ["Agent-to-agent message context:", ...buildAgentSessionLines(params)].filter(
    Boolean,
  );
  return lines.join("\n");
}
