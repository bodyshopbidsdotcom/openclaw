/**
 * sessions_send self-send reply delivery.
 *
 * A send to another session never announces the target's answer back: a waited
 * send returns it inline and a fire-and-forget send returns at admission. A
 * fire-and-forget self-send has no other session to wake, so this owner only
 * routes the caller's own answer to the conversation that requested it.
 */
import type { SessionDeliveryGeneration } from "../../config/sessions/session-delivery-generation.types.js";
import { runWithoutOwnedSessionTranscriptWrites } from "../../config/sessions/transcript-write-context.js";
import { bindInProcessSessionDeliveryGeneration } from "../../gateway/in-process-session-delivery.js";
import { formatErrorMessage } from "../../infra/errors.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { splitMediaFromOutput } from "../../media/parse.js";
import { stringifyRouteThreadId } from "../../plugin-sdk/channel-route.js";
import { runWithGatewayDetachedWorkContinuation } from "../../process/gateway-work-admission.js";
import type { DeliveryContext } from "../../utils/delivery-context.types.js";
import { isInternalMessageChannel } from "../../utils/message-channel.js";
import { runOutsidePreparedModelRuntimePluginGenerationScope } from "../prepared-model-runtime-generation-scope.js";
import { waitForAgentRunReply } from "../run-wait.js";
import {
  runWithGatewayToolContinuationContext,
  type AgentToolGatewayRequestCaller,
} from "./in-process-gateway.js";
import { resolveAnnounceTarget } from "./sessions-announce-target.js";
import type { AnnounceTarget } from "./sessions-send-helpers.js";
import { isNonDeliverableSessionsReply } from "./sessions-send-tokens.js";

const log = createSubsystemLogger("agents/sessions-send");

type SessionsSendSelfReplyParams = {
  callGateway: AgentToolGatewayRequestCaller;
  runId: string;
  sessionKey: string;
  displayKey: string;
  agentId: string;
  waitTimeoutMs: number;
  requesterChannel?: string;
  requesterOrigin?: DeliveryContext;
  requesterDeliveryGeneration?: SessionDeliveryGeneration;
};

async function resolveSelfReplyTarget(
  params: SessionsSendSelfReplyParams,
): Promise<AnnounceTarget | undefined> {
  const origin = params.requesterOrigin;
  if (origin?.channel && origin.to && !isInternalMessageChannel(origin.channel)) {
    return {
      channel: origin.channel,
      to: origin.to,
      accountId: origin.accountId,
      threadId: stringifyRouteThreadId(origin.threadId),
    };
  }
  const stored = await resolveAnnounceTarget({
    sessionKey: params.sessionKey,
    displayKey: params.displayKey,
    callGateway: params.callGateway,
    agentId: params.agentId,
  });
  // Never route the caller's own answer to a conversation it did not come from.
  return stored && (!params.requesterChannel || params.requesterChannel === stored.channel)
    ? stored
    : undefined;
}

async function runSessionsSendSelfReply(params: SessionsSendSelfReplyParams) {
  try {
    const wait = await waitForAgentRunReply({
      runId: params.runId,
      timeoutMs: Math.min(params.waitTimeoutMs, 60_000),
      callGateway: params.callGateway,
      untilTerminal: true,
    });
    // A failed run stays in the session; nothing wakes the caller to report it.
    // The run that already answered its source conversation must not repeat it.
    if (
      wait.status !== "ok" ||
      wait.sourceReplyDelivered ||
      !wait.replyText ||
      isNonDeliverableSessionsReply(wait.replyText)
    ) {
      return;
    }
    const target = await resolveSelfReplyTarget(params);
    if (!target) {
      return;
    }
    if (!params.requesterDeliveryGeneration) {
      log.warn(
        "sessions_send reply skipped because its original session generation is unavailable",
        {
          runId: params.runId,
        },
      );
      return;
    }
    const { text: message, mediaUrls, audioAsVoice } = splitMediaFromOutput(wait.replyText.trim());
    if (!message && !mediaUrls?.length) {
      return;
    }
    // The delivery owner checks the original session generation immediately before dispatch.
    await params.callGateway({
      method: "send",
      params: bindInProcessSessionDeliveryGeneration(
        {
          to: target.to,
          message,
          ...(mediaUrls?.length ? { mediaUrls } : {}),
          agentId: params.agentId,
          ...(audioAsVoice ? { asVoice: true } : {}),
          channel: target.channel,
          accountId: target.accountId,
          threadId: target.threadId,
          idempotencyKey: `sessions-send:${params.runId}`,
        },
        params.requesterDeliveryGeneration,
      ),
      timeoutMs: 10_000,
    });
  } catch (error) {
    log.warn("sessions_send self reply delivery failed", {
      runId: params.runId,
      error: formatErrorMessage(error),
    });
  }
}

/**
 * Routes a fire-and-forget self-send answer to its source conversation once.
 * The returned promise settles after that detached observer and never rejects.
 */
export function startSessionsSendSelfReply(params: SessionsSendSelfReplyParams): Promise<void> {
  const failed = (error: unknown) => {
    log.warn("sessions_send self reply admission failed", {
      runId: params.runId,
      error: formatErrorMessage(error),
    });
  };
  try {
    // No caller-owned transcript/resource scope may survive in the detached observer.
    return runWithGatewayToolContinuationContext(() =>
      runWithGatewayDetachedWorkContinuation(
        () =>
          runOutsidePreparedModelRuntimePluginGenerationScope(() =>
            runWithoutOwnedSessionTranscriptWrites(() => runSessionsSendSelfReply(params)),
          ),
        "session:self-reply",
      ),
    ).catch(failed);
  } catch (error) {
    failed(error);
    return Promise.resolve();
  }
}
