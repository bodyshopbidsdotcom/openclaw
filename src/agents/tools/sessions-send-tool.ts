import crypto from "node:crypto";
import { finiteSecondsToTimerSafeMilliseconds } from "@openclaw/normalization-core/number-coercion";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { readAcpSessionMetaForEntry } from "../../acp/runtime/session-meta-readonly.js";
import { tryResolveLegacyCompatibilityAgentId } from "../../config/legacy.default-agent-owner.js";
import { createRuntimeConfigReader } from "../../config/runtime-snapshot.js";
import { resolvePersistedSessionStoreOwnerForKey } from "../../config/sessions/session-store-owner.js";
import { shouldResumeParentSubagent } from "../../gateway/session-subagent-resume.js";
import { resolveGatewaySessionStoreTargetWithStore } from "../../gateway/session-utils-store-lookup.js";
import { formatErrorMessage } from "../../infra/errors.js";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import {
  logSessionOwnershipLookupFailure,
  lookupFailedDenialMessage,
  lookupFailedOperationMessage,
  sessionOwnershipLookupFailure,
} from "../../plugin-sdk/session-visibility-internal.js";
import {
  classifySessionKeyShape,
  isUnscopedSessionKeySentinel,
  normalizeAgentId,
  normalizeAgentIdStrict,
} from "../../routing/session-key.js";
import { annotateInterSessionPromptText } from "../../sessions/input-provenance.js";
import { isCronRunSessionKey, parseAgentSessionKey } from "../../sessions/session-key-utils.js";
import { recordSessionParticipantBestEffort } from "../../sessions/session-participant-recording.js";
import { registerSessionStateWatch } from "../../sessions/session-state-events.js";
import { formatSystemTurnPrompt } from "../../sessions/system-turn-prompt.js";
import { normalizeDeliveryContext } from "../../utils/delivery-context.shared.js";
import { INTERNAL_MESSAGE_CHANNEL } from "../../utils/message-channel.js";
import { resolveSessionAgentId } from "../agent-scope.js";
import { resolveNestedAgentLaneForSession } from "../lanes.js";
import { RESTART_RECOVERY_INTERRUPTION_NOTE } from "../restart-recovery-prompt.js";
import { isTerminalAgentWaitTimeout, waitForAgentRunReply } from "../run-wait.js";
import { isSubagentSessionFromEntry } from "../subagents/spawn/subagent-depth-policy.js";
import {
  describeSessionsSendTool,
  SESSIONS_SEND_TOOL_DISPLAY_SUMMARY,
} from "../tool-description-presets.js";
import { ToolInputError } from "../tool-input-error.js";
import type { AnyAgentTool } from "./common.js";
import { jsonResult, readToolStringParam } from "./common.js";
import {
  prepareGatewayToolCallerAssertion,
  wrapGatewayPersonalToolExecution,
} from "./gateway-caller-context.js";
import { callAgentToolGatewayRequest } from "./in-process-gateway.js";
import {
  resolveSessionToolTargetAgentId,
  runWithScopedSessionAccess,
} from "./scoped-session-access.js";
import {
  createSessionVisibilityRowChecker,
  formatSessionToolAccessDenial,
  isExpectedSessionLookupMiss,
  recordSessionToolActionFact,
  resolveDisplaySessionKey,
  resolveSessionReference,
  resolveSessionToolAccess,
  resolveSessionToolContext,
  resolveVisibleSessionReference,
} from "./sessions-helpers.js";
import {
  PlacedSessionsSendSchema,
  PLACED_SESSIONS_SEND_DESCRIPTION,
} from "./sessions-placement-tool-contract.js";
import { sendFailure, sendReplyResult } from "./sessions-send-helpers.js";
import { captureSessionsSendResumeCaller, resumeSessionsSendTask } from "./sessions-send-resume.js";
import { startSessionsSendSelfReply } from "./sessions-send-self-reply.js";
import { prepareWatchedSteerCompletionClaim } from "./sessions-send-steer-claim.js";
import {
  callSessionsSendGateway,
  createConfiguredAgentMainSession,
  isConfiguredAgentMainSessionKey,
  notifySessionsSendSession,
  resolveConfiguredAgentMainSessionKey,
  startSessionsSendAgentRun,
  trySessionsSendActiveRunDelivery,
} from "./sessions-send-tool.delivery.js";
import {
  readSessionsSendMessage,
  readSessionsSendMode,
  readSessionsSendTimeout,
} from "./sessions-send-tool.input.js";
import { SessionsSendToolSchema, SessionsSendOutputSchema } from "./sessions-send-tool.schema.js";
import type { SessionsSendToolOptions } from "./sessions-send-tool.types.js";

const log = createSubsystemLogger("agents/sessions-send");

export function createSessionsSendTool(opts?: SessionsSendToolOptions): AnyAgentTool {
  const requesterOrigin = normalizeDeliveryContext(opts?.requesterOrigin);
  return {
    label: "Session Send",
    name: "sessions_send",
    displaySummary: SESSIONS_SEND_TOOL_DISPLAY_SUMMARY,
    description: opts?.workerPlacement
      ? PLACED_SESSIONS_SEND_DESCRIPTION
      : describeSessionsSendTool(),
    parameters: opts?.workerPlacement ? PlacedSessionsSendSchema : SessionsSendToolSchema,
    outputSchema: SessionsSendOutputSchema,
    execute: wrapGatewayPersonalToolExecution(async (_toolCallId, args) => {
      const params = isRecord(args) ? args : {};
      const promptedAt = Date.now();
      const gatewayCall = opts?.callGateway ?? callAgentToolGatewayRequest;
      const sendGatewayCall = opts?.callGateway ?? callSessionsSendGateway;
      const message = readSessionsSendMessage(params);
      const mode = readSessionsSendMode(params);
      const resumeCaller =
        mode === undefined || mode === "resume" ? captureSessionsSendResumeCaller() : undefined;
      if (mode === "resume" && !resumeCaller) {
        return sendFailure("forbidden", "Task resume requires an admitted parent tool caller.");
      }
      const timeoutSeconds = readSessionsSendTimeout(params, mode);
      const {
        cfg,
        mainKey,
        alias,
        effectiveRequesterKey,
        mainSessionKey,
        restrictToSpawned,
        sessionVisibility,
        a2aPolicy,
      } = resolveSessionToolContext(opts);
      const readConfig = createRuntimeConfigReader(cfg);
      let requesterAgentId: string;
      try {
        requesterAgentId = resolveSessionAgentId({
          config: cfg,
          sessionKey: effectiveRequesterKey,
          agentId: opts?.agentId,
        });
      } catch (err) {
        return sendFailure("forbidden", formatErrorMessage(err));
      }
      const readSession = (key: string, agentId: string) =>
        resolveGatewaySessionStoreTargetWithStore({
          cfg,
          key,
          agentId,
          readOnly: true,
          exactRead: true,
          clone: false,
          projection: "full",
        });

      const sessionKeyParam = readToolStringParam(params, "sessionKey");
      const labelParam = readToolStringParam(params, "label");
      const labelAgentIdInput = readToolStringParam(params, "agentId");
      const normalizedLabelAgentId =
        labelAgentIdInput === undefined ? null : normalizeAgentIdStrict(labelAgentIdInput);
      if (normalizedLabelAgentId && !normalizedLabelAgentId.ok) {
        return sendFailure(
          "error",
          `Agent "${labelAgentIdInput}" not found. Run openclaw agents list to see configured agents.`,
        );
      }
      const explicitTargetAgentId = normalizedLabelAgentId?.value;

      let sessionKey = sessionKeyParam;
      let resolvedTargetAgentId: string | undefined;
      let resolvedLabelKey: string | undefined;
      if (!sessionKey && !labelParam && explicitTargetAgentId) {
        const agentMainKey = resolveConfiguredAgentMainSessionKey({
          cfg,
          agentId: explicitTargetAgentId,
          mainKey,
        });
        if (!agentMainKey) {
          return sendFailure(
            "error",
            `Agent "${labelAgentIdInput}" not found. Run openclaw agents list to see configured agents.`,
          );
        }
        sessionKey = agentMainKey;
      }
      if (!sessionKey && labelParam) {
        const requestedAgentId = explicitTargetAgentId;

        if (restrictToSpawned && requestedAgentId && requestedAgentId !== requesterAgentId) {
          return sendFailure(
            "forbidden",
            "Sandboxed sessions_send label lookup is limited to this agent",
          );
        }

        if (requesterAgentId && requestedAgentId && requestedAgentId !== requesterAgentId) {
          if (!a2aPolicy.enabled) {
            return sendFailure(
              "forbidden",
              "Agent-to-agent messaging is disabled. Set tools.agentToAgent.enabled=true to allow cross-agent sends.",
            );
          }
          if (!a2aPolicy.isAllowed(requesterAgentId, requestedAgentId)) {
            return sendFailure(
              "forbidden",
              "Agent-to-agent messaging denied by tools.agentToAgent.allow.",
            );
          }
        }

        const resolveParams: Record<string, unknown> = {
          label: labelParam,
          ...(requestedAgentId ? { agentId: requestedAgentId } : {}),
          ...(restrictToSpawned ? { spawnedBy: effectiveRequesterKey } : {}),
        };
        let resolvedKey = "";
        try {
          const resolved = await gatewayCall<{ agentId?: string; key: string }>({
            method: "sessions.resolve",
            params: resolveParams,
            timeoutMs: 10_000,
          });
          resolvedKey = normalizeOptionalString(resolved?.key) ?? "";
          resolvedTargetAgentId = normalizeOptionalString(resolved?.agentId);
        } catch (err) {
          if (!isExpectedSessionLookupMiss(err)) {
            const failure = sessionOwnershipLookupFailure(err);
            logSessionOwnershipLookupFailure({
              requesterSessionKey: effectiveRequesterKey,
              failure,
            });
            return sendFailure(
              restrictToSpawned ? "forbidden" : "error",
              restrictToSpawned
                ? lookupFailedDenialMessage("send", failure.kind)
                : lookupFailedOperationMessage("send", failure.kind),
            );
          }
        }

        if (!resolvedKey) {
          if (restrictToSpawned) {
            return sendFailure(
              "forbidden",
              "Session not visible from this sandboxed agent session.",
            );
          }
          return sendFailure("error", `No session found with label: ${labelParam}`);
        }
        sessionKey = resolvedKey;
        resolvedLabelKey = resolvedKey;
      }

      if (!sessionKey) {
        return sendFailure("error", "Either sessionKey or label is required");
      }
      const allowMissingKey = isConfiguredAgentMainSessionKey({
        cfg,
        sessionKey,
        mainKey,
      });
      const resolvedSession = resolvedLabelKey
        ? {
            ok: true as const,
            ...(resolvedTargetAgentId ? { agentId: resolvedTargetAgentId } : {}),
            key: resolvedLabelKey,
            displayKey: resolveDisplaySessionKey({ key: resolvedLabelKey, alias, mainKey }),
            resolvedViaSessionId: false,
            requesterOwned: restrictToSpawned,
          }
        : await resolveSessionReference({
            action: "send",
            sessionKey,
            keyAgentId: requesterAgentId,
            alias,
            mainKey,
            requesterInternalKey: effectiveRequesterKey,
            restrictToSpawned,
            callGateway: gatewayCall,
          });
      if (!resolvedSession.ok) {
        return sendFailure(resolvedSession.status, resolvedSession.error);
      }
      if (
        resolvedSession.resolvedViaSessionId &&
        !resolvedSession.agentId &&
        classifySessionKeyShape(resolvedSession.key) === "legacy_or_alias"
      ) {
        return sendFailure(
          "forbidden",
          "Session ownership could not be verified. Upgrade the gateway or use an agent-prefixed session key.",
          sessionKey,
        );
      }
      const resolutionAccess = createSessionVisibilityRowChecker({
        action: "send",
        defaultAgentId: resolveSessionToolTargetAgentId({
          cfg,
          targetSessionKey: resolvedSession.key,
          resolvedAgentId: resolvedSession.agentId,
          requesterAgentId,
        }),
        requesterAgentId,
        requesterSessionKey: effectiveRequesterKey,
        mainSessionKey,
        visibility: sessionVisibility,
        a2aPolicy,
      }).check({ key: resolvedSession.key });
      const visibleSession = await resolveVisibleSessionReference({
        action: "send",
        resolvedSession,
        requesterSessionKey: effectiveRequesterKey,
        requesterAgentId,
        restrictToSpawned,
        visibilitySessionKey: sessionKey,
        allowMissingKey,
        concealResolutionError: resolutionAccess.allowed ? undefined : resolutionAccess.error,
        callGateway: gatewayCall,
      });
      const unresolvedDisplayKey = sessionKey;
      if (!visibleSession.ok) {
        return sendFailure(visibleSession.status, visibleSession.error, unresolvedDisplayKey);
      }
      const resolvedKey = visibleSession.key;
      const displayKey = visibleSession.displayKey;
      const resolvedKeyAgentId = parseAgentSessionKey(resolvedKey)?.agentId;
      const isLiteralLegacyKeyInput =
        !labelParam && sessionKeyParam !== undefined && !resolvedSession.resolvedViaSessionId;
      const isLiteralUnscopedTarget =
        isLiteralLegacyKeyInput && classifySessionKeyShape(resolvedKey) === "legacy_or_alias";
      const persistedTargetOwner = isLiteralUnscopedTarget
        ? resolvePersistedSessionStoreOwnerForKey(cfg, resolvedKey)
        : { kind: "none" as const };
      const compatibilityTargetAgentId =
        isLiteralUnscopedTarget && persistedTargetOwner.kind === "none"
          ? tryResolveLegacyCompatibilityAgentId(cfg)
          : undefined;
      const isLiteralUnscopedMainTarget =
        isLiteralUnscopedTarget &&
        (isUnscopedSessionKeySentinel(sessionKeyParam.trim()) ||
          sessionKeyParam.trim().toLowerCase() === mainKey);
      if (persistedTargetOwner.kind === "retired") {
        return sendFailure(
          "forbidden",
          "Session ownership could not be verified because its fixed-store owner retired.",
          unresolvedDisplayKey,
        );
      }
      const resolvedTargetOwner =
        visibleSession.agentId ??
        resolvedTargetAgentId ??
        (labelParam ? explicitTargetAgentId : undefined);
      if (
        persistedTargetOwner.kind === "configured" &&
        resolvedTargetOwner &&
        normalizeAgentId(resolvedTargetOwner) !== persistedTargetOwner.agentId
      ) {
        return sendFailure(
          "forbidden",
          `Session belongs to agent "${persistedTargetOwner.agentId}", not "${normalizeAgentId(resolvedTargetOwner)}".`,
          unresolvedDisplayKey,
        );
      }
      const targetAgentId =
        (persistedTargetOwner.kind === "configured" ? persistedTargetOwner.agentId : undefined) ??
        resolvedTargetOwner ??
        resolvedKeyAgentId ??
        (isLiteralUnscopedMainTarget ? requesterAgentId : undefined) ??
        compatibilityTargetAgentId;
      if (!targetAgentId) {
        return sendFailure(
          "forbidden",
          "Session ownership could not be verified. Upgrade the gateway or use an agent-prefixed session key.",
          unresolvedDisplayKey,
        );
      }
      const mayUseRequesterForLiteralSentinel =
        isLiteralUnscopedMainTarget && normalizeAgentId(targetAgentId) === requesterAgentId;
      const requesterSessionKey = opts?.agentSessionKey ? effectiveRequesterKey : undefined;
      const requesterSession = readSession(effectiveRequesterKey, requesterAgentId);
      const requesterSessionEntry = requesterSession.store[requesterSession.canonicalKey];
      const requesterSessionId = opts?.agentSessionId ?? requesterSessionEntry?.sessionId;
      const requesterDeliveryGeneration =
        requesterSessionEntry && requesterSessionId
          ? {
              agentId: requesterSession.agentId,
              storePath: requesterSession.storePath,
              sessionKey: requesterSession.canonicalKey,
              sessionId: requesterSessionId,
              lifecycleRevision: requesterSessionEntry.lifecycleRevision ?? null,
            }
          : undefined;
      const requesterIsSubagent = isSubagentSessionFromEntry(
        requesterSession.canonicalKey,
        requesterSessionEntry,
        readAcpSessionMetaForEntry({
          sessionKey: requesterSession.canonicalKey,
          agentId: requesterSession.agentId,
          cfg,
          entry: requesterSessionEntry,
        }),
      );
      const timeoutMs = finiteSecondsToTimerSafeMilliseconds(timeoutSeconds) ?? 0;
      const deliveryTimeoutMs = timeoutSeconds === 0 ? 30_000 : timeoutMs;
      const idempotencyKey = opts?.idempotencyKey ?? crypto.randomUUID();
      let runId: string = idempotencyKey;
      const sameSession = requesterSessionKey === resolvedKey && targetAgentId === requesterAgentId;
      // Fire-and-forget self-send remains a channel-delivery path. A synchronous
      // self-send would wait behind its own active session lane until timeout.
      if (timeoutSeconds !== 0 && sameSession) {
        return sendFailure(
          "error",
          "sessions_send cannot target the calling session; use your own reply instead",
          unresolvedDisplayKey,
          runId,
        );
      }
      const authorizationTargetKey = mayUseRequesterForLiteralSentinel
        ? effectiveRequesterKey
        : targetAgentId && !parseAgentSessionKey(resolvedKey)
          ? `agent:${targetAgentId}:${resolvedKey}`
          : resolvedKey;
      const access = await resolveSessionToolAccess({
        action: "send",
        watch: params.watch === true,
        readConfig,
        sandboxed: opts?.sandboxed,
        requesterAgentId,
        requesterSessionKey: effectiveRequesterKey,
        mainSessionKey,
        targetAgentId,
        targetSessionKey: resolvedKey,
        authorizationTargetSessionKey: authorizationTargetKey,
        requesterOwned: visibleSession.requesterOwned,
        visibility: sessionVisibility,
        a2aPolicy,
        callGateway: gatewayCall,
      });
      if (!access.allowed) {
        return sendFailure(
          access.status,
          formatSessionToolAccessDenial(access, {
            action: "send",
            targetSessionKey: unresolvedDisplayKey,
          }),
          unresolvedDisplayKey,
        );
      }
      const expectedSessionId = opts?.expectedTargetSessionId ?? access.expectedSessionId;
      if (mode === "notify" && expectedSessionId) {
        return sendFailure(
          "forbidden",
          "Notifications cannot outlive an exact-session access grant. Use steer or followup.",
          displayKey,
          runId,
        );
      }

      return await runWithScopedSessionAccess({
        cfg,
        storePath: opts?.expectedTargetStorePath,
        agentId: targetAgentId,
        expectedSessionId,
        ...(opts?.signal ? { signal: opts.signal } : {}),
        targetSessionKey: resolvedKey,
        run: async () => {
          if (visibleSession.missing) {
            const createdSession = await createConfiguredAgentMainSession({
              mode,
              inheritedToolPolicySource: opts?.inheritedToolPolicySource,
              callGateway: sendGatewayCall,
              agentId: targetAgentId,
              sessionKey: resolvedKey,
              requesterSessionKey,
              useTrustedInProcessCreation: opts?.callGateway === undefined,
              assertCurrent: access.assertCurrent,
            });
            if (!createdSession.ok) {
              return sendFailure(createdSession.status, createdSession.error, displayKey);
            }
          }

          const requesterChannel = opts?.agentChannel;
          const isIsolatedCronRequester = isCronRunSessionKey(requesterSessionKey);
          const targetSession = readSession(resolvedKey, targetAgentId);
          const targetSessionEntry = targetSession.store[targetSession.canonicalKey];
          const targetAcpMeta = readAcpSessionMetaForEntry({
            sessionKey: targetSession.canonicalKey,
            agentId: targetSession.agentId,
            cfg,
            entry: targetSessionEntry,
          });
          const targetIsSubagent = isSubagentSessionFromEntry(
            targetSession.canonicalKey,
            targetSessionEntry,
            targetAcpMeta,
          );
          const inputProvenance = {
            kind: "inter_session" as const,
            sourceSessionKey: requesterSessionKey,
            sourceChannel: requesterChannel,
            sourceTool: "sessions_send",
            ...(requesterIsSubagent ? { sourceRole: "subagent" as const } : {}),
          };
          if (mode === "notify") {
            return await notifySessionsSendSession({
              message,
              inputProvenance,
              sessionKey: resolvedKey,
              targetAgentId,
              idempotencyKey,
              runId,
              displayKey,
              assertCurrent: access.assertCurrent,
            });
          }
          const sendParams = {
            message: annotateInterSessionPromptText(message, inputProvenance),
            agentId: targetAgentId,
            sessionKey: resolvedKey,
            idempotencyKey,
            // Thread and channel keys carry a human-facing route. Delivery off and
            // message-tool-only replies keep this run's final reply off it; the target
            // posts there only via `message`. A fire-and-forget self-send replies to its
            // source route later through sessions-send-self-reply.
            deliver: false,
            sourceReplyDeliveryMode: "message_tool_only" as const,
            channel: INTERNAL_MESSAGE_CHANNEL,
            lane: resolveNestedAgentLaneForSession(resolvedKey),
            inputProvenance,
          };
          if (!targetAcpMeta && targetIsSubagent && targetSessionEntry?.status === "interrupted") {
            sendParams.message = `${formatSystemTurnPrompt(RESTART_RECOVERY_INTERRUPTION_NOTE)}\n\n${sendParams.message}`;
          }
          if (
            mode === "resume" ||
            (mode === undefined &&
              resumeCaller &&
              !targetAcpMeta &&
              shouldResumeParentSubagent({
                cfg,
                caller: resumeCaller,
                childSessionKey: resolvedKey,
              }))
          ) {
            if (!resumeCaller) {
              throw new ToolInputError("Task resume requires an admitted parent tool caller.");
            }
            return await resumeSessionsSendTask({
              cfg,
              caller: resumeCaller,
              assertCurrent: access.assertCurrent,
              targetAgentId,
              sessionKey: resolvedKey,
              displayKey,
              runId,
              expectedSessionId,
              sendParams,
              callGateway: sendGatewayCall,
            });
          }
          const ownChild = targetSessionEntry?.spawnedBy === effectiveRequesterKey;
          const startParams: Parameters<typeof startSessionsSendAgentRun>[0] = {
            cfg,
            callGateway: sendGatewayCall,
            runId,
            mode,
            sendParams,
            sourceOrigin: sameSession ? requesterOrigin : undefined,
            sessionKey: mode || ownChild ? resolvedKey : displayKey,
            sessionStoreTarget: targetSession,
            deliveryTimeoutMs,
            allowActiveRunQueueDelivery: timeoutSeconds === 0,
            expectedSessionId,
            assertSendCurrent: access.assertCurrent,
          };
          const watch = params.watch === true;
          const claimSteerCompletion = prepareWatchedSteerCompletionClaim({
            watch,
            nonblocking: timeoutSeconds === 0,
            expectedSessionId,
            nativeChild: !targetAcpMeta,
            ownChild,
            childSessionKey: targetSession.canonicalKey,
            childAgentId: targetSession.agentId,
            requesterSessionKey: effectiveRequesterKey,
            requesterAgentId,
            requesterTurnRunId:
              requesterSessionKey === effectiveRequesterKey ? opts?.requesterTurnRunId : undefined,
            displayKey,
          });
          const activeDelivery = await trySessionsSendActiveRunDelivery(startParams, ownChild);
          const start =
            "ok" in activeDelivery
              ? activeDelivery
              : await startSessionsSendAgentRun({ ...startParams, ...activeDelivery });
          if (!start.ok) {
            return start.result;
          }
          if (claimSteerCompletion && start.targetDisposition === "steered") {
            const claimFailure = await claimSteerCompletion(start);
            if (claimFailure) {
              return claimFailure;
            }
          }
          const acceptedTargetSessionKey = start.a2aSessionKey ?? resolvedKey;
          // The target's answer stays in its own session: a waited send returns it
          // inline, and nothing delivers it into the requester later. A
          // fire-and-forget self-send has no other session to wake, so its new turn
          // routes its own answer to the conversation that asked. Child sessions,
          // isolated Cron runs, and exact-incarnation grants keep their completion owners.
          const deliversSelfReply =
            sameSession &&
            !start.a2aSessionKey &&
            !requesterIsSubagent &&
            !isIsolatedCronRequester &&
            !expectedSessionId &&
            start.targetDisposition === "queued";
          const delivery = { status: deliversSelfReply ? "pending" : "skipped" } as const;
          recordSessionToolActionFact({
            operation: "send",
            fact: "committed",
            targetAgentId,
            targetSessionKey: acceptedTargetSessionKey,
          });
          try {
            const acceptedTarget = start.a2aSessionKey
              ? readSession(acceptedTargetSessionKey, targetAgentId)
              : targetSession;
            if (start.a2aSessionKey && !acceptedTarget.store[acceptedTarget.canonicalKey]) {
              throw new Error("Accepted Cron parent has no stored session entry.");
            }
            recordSessionParticipantBestEffort({
              identity: { type: "agent", id: requesterAgentId },
              promptedAt,
              agentId: acceptedTarget.agentId,
              sessionKey: acceptedTarget.canonicalKey,
              storePath: acceptedTarget.storePath,
              onError: (error) => log.warn("failed to record session participant", { error }),
            });
          } catch (error) {
            log.warn("failed to record session participant", { error });
          }
          runId = start.runId;
          // Watch the session that actually received the message, including a Cron parent.
          const watchedSessionKey = start.a2aSessionKey ?? targetSession.canonicalKey;
          const watched =
            watch &&
            !expectedSessionId &&
            requesterSessionKey &&
            requesterSessionKey !== watchedSessionKey
              ? await registerSessionStateWatch(
                  {
                    watcherSessionKey: requesterSessionKey,
                    targetSessionKey: watchedSessionKey,
                    targetAgentId,
                  },
                  { prepareCurrent: prepareGatewayToolCallerAssertion },
                )
              : false;
          const watchField = watch ? { watched } : {};
          const accepted = () =>
            jsonResult({
              runId,
              status: "accepted",
              sessionKey: displayKey,
              targetDisposition: start.targetDisposition,
              delivery,
              ...watchField,
            });
          if (timeoutSeconds === 0) {
            if (deliversSelfReply) {
              await startSessionsSendSelfReply({
                callGateway: gatewayCall,
                runId,
                sessionKey: resolvedKey,
                displayKey,
                agentId: targetAgentId,
                waitTimeoutMs: deliveryTimeoutMs,
                requesterChannel,
                requesterOrigin,
                requesterDeliveryGeneration,
              });
            }
            return accepted();
          }

          // A reply after this wait stays in the target session; nothing observes it.
          const result = await waitForAgentRunReply({ runId, timeoutMs, callGateway: gatewayCall });
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
              error:
                result.error ??
                (result.status === "timeout" ? "agent run timed out" : "agent error"),
              sentBeforeError: true,
              sessionKey: displayKey,
              ...watchField,
            });
          }
          return sendReplyResult({ runId, sessionKey: displayKey, ...watchField }, result);
        },
      });
    }),
  };
}
