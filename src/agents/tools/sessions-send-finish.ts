import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { resolveGatewaySessionStoreTargetInWorker } from "../../gateway/session-utils-store-worker.js";
import type { GatewaySessionStoreTargetWithStore } from "../../gateway/session-utils-store.types.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { recordSessionParticipantBestEffort } from "../../sessions/session-participant-recording.js";
import { registerSessionStateWatch } from "../../sessions/session-state-events.js";
import type { DeliveryContext } from "../../utils/delivery-context.types.js";
import { isTerminalAgentWaitTimeout, waitForAgentRunReply } from "../run-wait.js";
import { prepareGatewayToolCallerAssertion } from "./gateway-caller-context.js";
import type { AgentToolGatewayRequestCaller } from "./in-process-gateway.js";
import { recordSessionToolActionFact } from "./sessions-access.js";
import { sendReplyResult } from "./sessions-send-helpers.js";
import { startSessionsSendSelfReply } from "./sessions-send-self-reply.js";
import type { SessionsSendStart } from "./sessions-send-tool.delivery.js";
import { jsonResult } from "./tool-results.js";

const log = createSubsystemLogger("agents/sessions-send");

/**
 * Settle one accepted send. The target's answer stays in the target session: a
 * waited send returns it inline, and nothing delivers it into the requester
 * later. A fire-and-forget self-send has no other session to wake, so only its
 * own answer is routed back to the conversation that asked.
 */
export async function finishSessionsSendReply(params: {
  start: SessionsSendStart;
  /** Claims a steered child's existing completion; returns an error result on failure. */
  claimSteerCompletion?: (
    steered: Extract<SessionsSendStart, { ok: true }>,
  ) => Promise<ReturnType<typeof jsonResult> | undefined>;
  /** No completion owner exists for this self-send, so its answer returns to its source. */
  selfReply: boolean;
  watch: boolean;
  expectedSessionId?: string;
  callGateway: AgentToolGatewayRequestCaller;
  cfg: OpenClawConfig;
  targetSession: GatewaySessionStoreTargetWithStore;
  targetAgentId: string;
  displayKey: string;
  requesterTarget: GatewaySessionStoreTargetWithStore;
  requesterSessionId?: string;
  requesterSessionKey?: string;
  requesterAgentId: string;
  requesterChannel?: string;
  requesterOrigin?: DeliveryContext;
  promptedAt: number;
  timeoutSeconds: number;
  timeoutMs: number;
  deliveryTimeoutMs: number;
}) {
  const { start, displayKey } = params;
  if (!start.ok) {
    return start.result;
  }
  if (params.claimSteerCompletion && start.targetDisposition === "steered") {
    const claimFailure = await params.claimSteerCompletion(start);
    if (claimFailure) {
      return claimFailure;
    }
  }
  const runId = start.runId;
  const acceptedTargetSessionKey = start.a2aSessionKey ?? params.targetSession.canonicalKey;
  // A Cron fallback is a distinct session; only the original target can be this caller.
  const deliversSelfReply =
    params.selfReply && !start.a2aSessionKey && start.targetDisposition === "queued";
  const delivery = { status: deliversSelfReply ? "pending" : "skipped" } as const;
  let watched = false;
  if (
    params.watch &&
    !params.expectedSessionId &&
    params.requesterSessionKey &&
    params.requesterSessionKey !== acceptedTargetSessionKey
  ) {
    watched = await registerSessionStateWatch(
      {
        watcherSessionKey: params.requesterSessionKey,
        targetSessionKey: acceptedTargetSessionKey,
        targetAgentId: params.targetAgentId,
      },
      { prepareCurrent: prepareGatewayToolCallerAssertion },
    );
  }
  const watchField = params.watch
    ? {
        watched,
        ...(!watched
          ? {
              watchError:
                "The send was accepted, but no proactive state watch was registered. Watches require distinct, valid agent-qualified session keys and current session authority. Use owner-scoped session_status changesSince to reconcile state.",
            }
          : {}),
      }
    : {};
  recordSessionToolActionFact({
    operation: "send",
    fact: "committed",
    targetAgentId: params.targetAgentId,
    targetSessionKey: acceptedTargetSessionKey,
  });
  try {
    const acceptedTarget = start.a2aSessionKey
      ? await resolveGatewaySessionStoreTargetInWorker({
          cfg: params.cfg,
          key: acceptedTargetSessionKey,
          agentId: params.targetAgentId,
          projection: "full",
        })
      : params.targetSession;
    if (start.a2aSessionKey && !acceptedTarget.store[acceptedTarget.canonicalKey]) {
      throw new Error("Accepted Cron parent has no stored session entry.");
    }
    recordSessionParticipantBestEffort({
      identity: { type: "agent", id: params.requesterAgentId },
      promptedAt: params.promptedAt,
      agentId: acceptedTarget.agentId,
      sessionKey: acceptedTarget.canonicalKey,
      storePath: acceptedTarget.storePath,
      onError: (error) => log.warn("failed to record session participant", { error }),
    });
  } catch (error) {
    log.warn("failed to record session participant", { error });
  }
  const accepted = () =>
    jsonResult({
      runId,
      status: "accepted",
      sessionKey: displayKey,
      targetDisposition: start.targetDisposition,
      delivery,
      ...watchField,
    });
  if (params.timeoutSeconds === 0) {
    if (deliversSelfReply) {
      const requesterEntry = params.requesterTarget.store[params.requesterTarget.canonicalKey];
      const requesterSessionId = params.requesterSessionId ?? requesterEntry?.sessionId;
      await startSessionsSendSelfReply({
        callGateway: params.callGateway,
        runId,
        sessionKey: params.targetSession.canonicalKey,
        displayKey,
        agentId: params.targetAgentId,
        waitTimeoutMs: params.deliveryTimeoutMs,
        requesterChannel: params.requesterChannel,
        requesterOrigin: params.requesterOrigin,
        requesterDeliveryGeneration:
          requesterEntry && requesterSessionId
            ? {
                agentId: params.requesterTarget.agentId,
                storePath: params.requesterTarget.storePath,
                sessionKey: params.requesterTarget.canonicalKey,
                sessionId: requesterSessionId,
                lifecycleRevision: requesterEntry.lifecycleRevision ?? null,
              }
            : undefined,
      });
    }
    return accepted();
  }
  // A reply after this wait stays in the target session; nothing observes it.
  const result = await waitForAgentRunReply({
    runId,
    timeoutMs: params.timeoutMs,
    callGateway: params.callGateway,
  });
  if (result.yielded === true || result.status === "pending") {
    return accepted();
  }
  if (result.status === "timeout") {
    if (result.pendingError === true && result.error?.trim()) {
      return jsonResult({
        runId,
        status: "timeout",
        error: result.error,
        sentBeforeError: true,
        sessionKey: displayKey,
        delivery,
        ...watchField,
      });
    }
    if (!isTerminalAgentWaitTimeout(result)) {
      return accepted();
    }
  }
  if (result.status === "timeout" || result.status === "error") {
    return jsonResult({
      runId,
      status: result.status,
      error: result.error ?? (result.status === "timeout" ? "agent run timed out" : "agent error"),
      sentBeforeError: true,
      sessionKey: displayKey,
      ...watchField,
    });
  }
  return sendReplyResult({ runId, sessionKey: displayKey, ...watchField }, result);
}
