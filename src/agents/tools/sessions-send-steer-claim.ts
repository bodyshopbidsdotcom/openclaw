import { formatErrorMessage } from "../../infra/errors.js";
import { isCronRunSessionKey } from "../../sessions/session-key-utils.js";
import { mergeAcceptedSessionSpawnsForRun } from "../accepted-session-spawn.js";
import { getLatestLiveSubagentRunByChildSessionKey } from "../subagents/registry/subagent-registry-read.js";
import {
  captureGatewayToolCallerAssertion,
  getGatewayToolCallerIdentity,
} from "./gateway-caller-context.js";
import { runWithGatewayToolCleanupContext } from "./in-process-gateway.js";
import { jsonResult } from "./tool-results.js";

/**
 * A watched nonblocking steer into a native child that owes the requester a
 * completion can claim that existing completion for the current requester turn,
 * so sessions_yield can wait for it. Nothing new is registered: the child's own
 * completion owner still delivers that result once. sessions_send never
 * reports a target's answer back by itself.
 */
export function prepareWatchedSteerCompletionClaim(params: {
  watch: boolean;
  nonblocking: boolean;
  expectedSessionId?: string;
  nativeChild: boolean;
  ownChild: boolean;
  childSessionKey: string;
  childAgentId: string;
  requesterSessionKey: string;
  requesterAgentId: string;
  requesterTurnRunId?: string;
  displayKey: string;
}) {
  const requesterTurnRunId = params.requesterTurnRunId;
  if (
    !params.watch ||
    !params.nonblocking ||
    params.expectedSessionId ||
    !params.nativeChild ||
    !requesterTurnRunId ||
    isCronRunSessionKey(params.requesterSessionKey)
  ) {
    return undefined;
  }
  const completionChild = getLatestLiveSubagentRunByChildSessionKey(
    params.childSessionKey,
    undefined,
    params.childAgentId,
  );
  if (
    !params.ownChild &&
    !(
      completionChild?.requesterSessionKey === params.requesterSessionKey &&
      completionChild.requesterAgentId === params.requesterAgentId &&
      completionChild.expectsCompletionMessage === true
    )
  ) {
    return undefined;
  }
  const assertCallerAssertion = captureGatewayToolCallerAssertion();
  const assertCallerCurrent = () => assertCallerAssertion?.();
  const instance = getGatewayToolCallerIdentity()?.operationalRunInstance;
  /** Returns an error result when admitted steering could not be claimed. */
  return async (steered: { runId: string; steeredRunId?: string }) => {
    try {
      const { adoptSubagentRunForRequesterTurn } =
        await import("../subagents/registry/subagent-registry.js");
      assertCallerCurrent();
      const expected = steered.steeredRunId
        ? getLatestLiveSubagentRunByChildSessionKey(
            params.childSessionKey,
            (entry) => entry.runId === steered.steeredRunId,
            params.childAgentId,
          )
        : undefined;
      const accepted = expected
        ? await adoptSubagentRunForRequesterTurn({
            expected,
            requesterSessionKey: params.requesterSessionKey,
            requesterAgentId: params.requesterAgentId,
            requesterTurnRunId,
            assertCurrent: assertCallerCurrent,
          })
        : undefined;
      if (!accepted) {
        throw new Error(
          "Steering was admitted, but its completion could not be claimed. Inspect the target before retrying.",
        );
      }
      assertCallerCurrent();
      if (instance) {
        mergeAcceptedSessionSpawnsForRun(instance, [accepted]);
      }
      return undefined;
    } catch (error) {
      let failure = error;
      const steeredRunId = steered.steeredRunId;
      if (steeredRunId && instance) {
        try {
          const { reconcileRequesterTurnClaimForRun } =
            await import("../subagents/registry/subagent-registry-requester-claim.js");
          await runWithGatewayToolCleanupContext(() =>
            reconcileRequesterTurnClaimForRun({
              runId: steeredRunId,
              requesterSessionKey: params.requesterSessionKey,
              requesterAgentId: params.requesterAgentId,
              requesterRunInstance: instance,
            }),
          );
        } catch (reconciliationError) {
          failure = new AggregateError(
            [error, reconciliationError],
            "Accepted child completion could not be reconciled with its requester.",
          );
        }
      }
      return jsonResult({
        runId: steered.runId,
        status: "error",
        error: formatErrorMessage(failure),
        sentBeforeError: true,
        sessionKey: params.displayKey,
      });
    }
  };
}
