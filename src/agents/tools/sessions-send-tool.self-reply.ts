/**
 * sessions_send self-send reply delivery.
 *
 * A send never wakes another session with the target's answer: a waited send
 * returns it inline and a fire-and-forget send returns at admission. Only a
 * self-send has no other session to wake, so this owner routes the caller's
 * own answer back to the caller's own channel.
 */
import crypto from "node:crypto";
import { formatErrorMessage } from "../../infra/errors.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { splitMediaFromOutput } from "../../media/parse.js";
import {
  type AgentWaitResult,
  isTerminalAgentWaitTimeout,
  waitForAgentRunReply,
} from "../run-wait.js";
import { runAgentStep } from "./agent-step.js";
import {
  callAgentToolGatewayRequest,
  type AgentToolGatewayRequestCaller,
} from "./in-process-gateway.js";
import { resolveAnnounceTarget } from "./sessions-announce-target.js";
import { type AnnounceTarget, isNonDeliverableSessionsReply } from "./sessions-send-helpers.js";

const log = createSubsystemLogger("agents/sessions-send");

function isDeliveryFailureWait(wait: AgentWaitResult): boolean {
  return (
    (wait.status === "error" && !wait.retryableTransportError) || isTerminalAgentWaitTimeout(wait)
  );
}

async function deliverReplyToChannel(params: {
  announceTarget: AnnounceTarget;
  callGateway: AgentToolGatewayRequestCaller;
  message: string;
  runContextId: string;
  targetAgentId: string;
}) {
  // Gateway sends need the selected owner for text routing and media roots;
  // carry the admitted target instead of relying on an implicit default.
  const { text: message, mediaUrls, audioAsVoice } = splitMediaFromOutput(params.message.trim());
  if (!message && !mediaUrls?.length) {
    return;
  }
  try {
    await params.callGateway({
      method: "send",
      params: {
        to: params.announceTarget.to,
        message,
        ...(mediaUrls?.length ? { mediaUrls } : {}),
        agentId: params.targetAgentId,
        ...(audioAsVoice ? { asVoice: true } : {}),
        channel: params.announceTarget.channel,
        accountId: params.announceTarget.accountId,
        threadId: params.announceTarget.threadId,
        idempotencyKey: crypto.randomUUID(),
      },
      timeoutMs: 10_000,
    });
  } catch (err) {
    log.warn("sessions_send self reply delivery failed", {
      runId: params.runContextId,
      channel: params.announceTarget.channel,
      to: params.announceTarget.to,
      error: formatErrorMessage(err),
    });
  }
}

export async function runSessionsSendSelfReply(params: {
  callGateway?: AgentToolGatewayRequestCaller;
  targetSessionKey: string;
  targetAgentId: string;
  displayKey: string;
  announceTimeoutMs: number;
  requesterSessionKey?: string;
  requesterAgentId?: string;
  requesterChannel?: string;
  sourceReplyDelivered?: true;
  roundOneReply?: string;
  waitRunId?: string;
  notifyRequesterOnWaitFailure?: boolean;
}) {
  const runContextId = params.waitRunId ?? "unknown";
  const gatewayCall = params.callGateway ?? callAgentToolGatewayRequest;
  try {
    let reply = params.roundOneReply;
    let sourceReplyDelivered = params.sourceReplyDelivered;
    if (!reply && params.waitRunId) {
      const wait = await waitForAgentRunReply({
        runId: params.waitRunId,
        timeoutMs: Math.min(params.announceTimeoutMs, 60_000),
        callGateway: gatewayCall,
        untilTerminal: true,
      });
      if (wait.status === "ok") {
        reply = wait.replyText;
        sourceReplyDelivered = wait.sourceReplyDelivered;
      } else {
        if (
          params.notifyRequesterOnWaitFailure === true &&
          params.requesterSessionKey &&
          isDeliveryFailureWait(wait)
        ) {
          const error =
            typeof wait.error === "string" && wait.error.trim() ? `: ${wait.error.trim()}` : "";
          await runAgentStep({
            agentId: params.requesterAgentId,
            sessionKey: params.requesterSessionKey,
            message: wait.sourceReplyDelivered
              ? `sessions_send target run for ${params.displayKey} failed${error}. The target's final reply was already delivered to its source conversation. Do not resend; report the run failure.`
              : `sessions_send delivery to ${params.displayKey} failed${error}. The target may not have received the message; retry or report the failure instead of assuming delivery succeeded.`,
            extraSystemPrompt: wait.sourceReplyDelivered
              ? "The target run failed after its final source reply was delivered. Preserve the run error diagnosis. Do not resend the message or the reply."
              : "A previous sessions_send delivery failed after it was accepted. Inspect the accepted operation before retrying, or report the failure. Preserve attributed session-tool delivery; do not replace it with an operator CLI request. Do not assume the target received the message.",
            timeoutMs: params.announceTimeoutMs,
            sourceSessionKey: params.targetSessionKey,
            sourceTool: "sessions_send",
            callGateway: gatewayCall,
          });
        }
        return;
      }
    }
    if (!reply || isNonDeliverableSessionsReply(reply)) {
      return;
    }
    // The run already answered its own source conversation; delivering the same
    // text again would duplicate it.
    if (sourceReplyDelivered) {
      return;
    }
    const announceTarget = await resolveAnnounceTarget({
      sessionKey: params.targetSessionKey,
      displayKey: params.displayKey,
      callGateway: gatewayCall,
      agentId: params.targetAgentId,
    });
    // Never route the caller's own answer to a conversation it did not come from.
    if (
      !announceTarget ||
      (params.requesterChannel && params.requesterChannel !== announceTarget.channel)
    ) {
      return;
    }
    await deliverReplyToChannel({
      announceTarget,
      callGateway: gatewayCall,
      message: reply,
      runContextId,
      targetAgentId: params.targetAgentId,
    });
  } catch (err) {
    log.warn("sessions_send self reply flow failed", {
      runId: runContextId,
      error: formatErrorMessage(err),
    });
  }
}
