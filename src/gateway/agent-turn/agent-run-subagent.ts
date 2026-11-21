/** Native subagent registration and paused-run adoption precede Gateway acceptance. */
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { resolveExactSubagentCompletionEvent } from "../../agents/subagents/announce/subagent-announce-handoff.js";
import { resolveSubagentChildAuthority } from "../../agents/subagents/registry/subagent-child-owner-match.js";
import { getLatestLiveSubagentRunByChildSessionKey } from "../../agents/subagents/registry/subagent-registry-read.js";
import type {
  SubagentRunRecord,
  SubagentAdmissionRejection,
} from "../../agents/subagents/registry/subagent-registry.types.js";
import { isSameSubagentRunOwner } from "../../agents/subagents/registry/subagent-run-generation.js";
import type { SessionEntry } from "../../config/sessions/types.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { runWithGatewayDetachedWorkContinuation } from "../../process/gateway-work-admission.js";
import { isAcpSessionKey } from "../../routing/session-key.js";
import type { InputProvenance } from "../../sessions/input-provenance.js";
import { readInProcessSubagentResume } from "../in-process-subagent-resume.js";
import type { AgentRunRequest } from "../server-methods/agent-request-types.js";
import { registerPluginSubagentRunFromGateway } from "../server-methods/agent-subagent-registration.js";
import { prepareParentSubagentResume } from "../session-subagent-resume.js";
import { consumeSubagentCompletionToolHandoff } from "../subagent-completion-tool-handoff.js";
import { formatForLog } from "../ws-log.js";
import type { PrepareAgentRunDispatchParams } from "./agent-run-admission-types.js";
import type { AgentTurnContext, AgentTurnPrincipal } from "./types.js";

export function resolveGatewayPluginSubagentToolPolicy(client: AgentTurnPrincipal | null) {
  // Plugin-owned additive grants stay internal to the authenticated in-process run.
  // Public agent params cannot supply them, and normal tool policy still filters them.
  const runtimePluginToolGrant =
    client?.internal?.agentRunTracking === "plugin_subagent" &&
    client.internal.pluginRuntimeOwnerId === client.internal.runtimePluginToolGrant?.pluginId
      ? client.internal.runtimePluginToolGrant
      : undefined;
  const pluginSubagentToolsAllow =
    client?.internal?.agentRunTracking === "plugin_subagent" &&
    Array.isArray(client.internal.pluginSubagentToolsAllow)
      ? [...client.internal.pluginSubagentToolsAllow]
      : undefined;
  return { runtimePluginToolGrant, pluginSubagentToolsAllow };
}

export async function prepareGatewaySubagentRun(params: {
  cfg: OpenClawConfig;
  client: AgentTurnPrincipal | null;
  resolvedSessionKey?: string;
  activeSessionAgentId: string;
  inputProvenance?: InputProvenance;
  request: Pick<AgentRunRequest, "message">;
  isOneShotModelRun: boolean;
  runId: string;
  getAdmittedSessionId: () => string;
  lifecycleStorePath: string;
  retainPluginSubagentAdmission?: (reject: SubagentAdmissionRejection) => void;
  assertResumeAdmissionCurrent: () => SessionEntry | undefined;
  onParentResumeAdopted?: (entry: SubagentRunRecord) => void;
  context: Pick<AgentTurnContext, "resolveGatewayContext"> & {
    logGateway: Pick<AgentTurnContext["logGateway"], "warn">;
  };
}): Promise<{
  pluginSubagent: boolean;
  reactivateSubagent: boolean;
  adoptParentResume?: () => Promise<string>;
  registerPluginSubagent?: () => Promise<void>;
}> {
  const resume = readInProcessSubagentResume(params.client?.internal);
  if (resume) {
    return {
      pluginSubagent: false,
      reactivateSubagent: false,
      adoptParentResume: await prepareParentSubagentResume({
        cfg: params.cfg,
        resume,
        sessionKey: params.resolvedSessionKey,
        getSessionId: params.getAdmittedSessionId,
        runId: params.runId,
        task: params.request.message,
        assertAdmissionCurrent: params.assertResumeAdmissionCurrent,
        onAdopted: params.onParentResumeAdopted,
        gatewayContextResolver: params.context.resolveGatewayContext,
      }),
    };
  }
  const sessionKey = params.resolvedSessionKey?.trim();
  const internalOwner = params.client?.internal?.agentRunTracking;
  const interSession = params.inputProvenance?.kind === "inter_session";
  const pluginSubagent = Boolean(
    !params.isOneShotModelRun &&
    sessionKey &&
    (interSession
      ? params.inputProvenance?.sourceTool === "subagent_settle" &&
        getLatestLiveSubagentRunByChildSessionKey(
          sessionKey,
          (entry) => entry.pauseReason === "sessions_yield",
          params.activeSessionAgentId,
        )
      : internalOwner === "plugin_subagent"),
  );
  params.assertResumeAdmissionCurrent();
  const registerPluginSubagent =
    pluginSubagent && sessionKey
      ? async () => {
          // Persist the actual execution owner before acknowledging a plugin dispatch.
          try {
            await registerPluginSubagentRunFromGateway({
              cfg: params.cfg,
              runId: params.runId,
              childSessionKey: sessionKey,
              childAgentId: params.activeSessionAgentId,
              task: params.request.message.trim(),
              requester: params.client?.internal?.pluginSubagentRequester,
              pluginId: normalizeOptionalString(params.client?.internal?.pluginRuntimeOwnerId),
              assertCurrent: params.assertResumeAdmissionCurrent,
              retainAdmission: params.retainPluginSubagentAdmission,
              admittedSession: {
                sessionId: params.getAdmittedSessionId(),
                storePath: params.lifecycleStorePath,
                entry: params.assertResumeAdmissionCurrent(),
              },
              gatewayContextResolver: params.context.resolveGatewayContext,
            });
          } catch (error) {
            params.assertResumeAdmissionCurrent();
            params.context.logGateway.warn(
              `failed to register plugin subagent run ${params.runId}; rejecting untracked dispatch: ${formatForLog(error)}`,
            );
            throw new Error("plugin subagent registry persistence failed; run was not started", {
              cause: error,
            });
          }
        }
      : undefined;
  // Operator follow-ups may continue a child; inter-session delivery retains its own owner.
  const reactivateSubagent = Boolean(
    sessionKey &&
    !params.isOneShotModelRun &&
    !interSession &&
    !pluginSubagent &&
    internalOwner !== "native_subagent" &&
    !isAcpSessionKey(sessionKey),
  );
  return { pluginSubagent, reactivateSubagent, registerPluginSubagent };
}

export function settleUnstartedGatewayParentResume(
  adopted: SubagentRunRecord,
  failure?: string,
): Promise<void> {
  return runWithGatewayDetachedWorkContinuation(async () => {
    const runtime = await import("../../agents/subagents/registry/subagent-registry.js");
    const ownsAdoption = () =>
      isSameSubagentRunOwner(
        getLatestLiveSubagentRunByChildSessionKey(
          adopted.childSessionKey,
          undefined,
          adopted.childAgentId,
        ),
        adopted,
      );
    if (!ownsAdoption()) {
      return;
    }
    const settled = await runtime.markSubagentRunTerminated({
      runId: adopted.runId,
      reason: failure ?? "Parent resume admission ended before acceptance.",
      suppressTaskDelivery: true,
      suppressSessionEffects: true,
      assertCurrent: () => {
        if (!ownsAdoption()) {
          throw new Error("Adopted parent resume changed before admission rejection settled");
        }
      },
    });
    if (!settled && ownsAdoption()) {
      throw new Error(
        "Adopted parent resume could not be durably settled after admission rejection",
      );
    }
  }, "subagents:unstarted-resume");
}

/** A registered subagent run passes its timeout only to the turn admitted for its own session. */
export function resolveRegisteredSubagentTimeoutSeconds(params: {
  sessionKey?: string;
  agentId?: string;
  admittedSessionId: string;
  admittedSessionEntry: SessionEntry | undefined;
}): number | undefined {
  const registeredRun = params.sessionKey
    ? getLatestLiveSubagentRunByChildSessionKey(params.sessionKey, undefined, params.agentId)
    : undefined;
  const authority = registeredRun
    ? resolveSubagentChildAuthority(registeredRun, params.admittedSessionEntry)
    : undefined;
  // Admission may adopt a replacement; retained rows must match its final identity.
  const inherits =
    registeredRun &&
    (!registeredRun.execution.suppressSessionEffects ||
      authority?.status === "legacy-unverified") &&
    params.admittedSessionEntry?.sessionId === params.admittedSessionId &&
    registeredRun.childSessionIdentity?.sessionId === params.admittedSessionId &&
    registeredRun.childSessionIdentity?.lifecycleRevision ===
      params.admittedSessionEntry?.lifecycleRevision &&
    authority?.status !== "mismatch";
  return inherits ? (registeredRun.runTimeoutSeconds ?? 0) : undefined;
}

export function consumeGatewaySubagentCompletionToolHandoff(
  params: Pick<
    PrepareAgentRunDispatchParams,
    | "request"
    | "client"
    | "providerOverride"
    | "modelOverride"
    | "restoredCronContinuation"
    | "resolvedSessionKey"
    | "getAdmittedSessionId"
  >,
  inputProvenance: InputProvenance | undefined,
  resolvedRuntime: { provider: string; model: string },
) {
  // Replay may retain another scheduling source from the same frozen cohort.
  // Bind the one-use grant only after transcript admission selects that source.
  const completionEvent = resolveExactSubagentCompletionEvent({
    inputProvenance,
    internalEvents: params.request.internalEvents,
  });
  return params.providerOverride === undefined &&
    params.modelOverride === undefined &&
    params.restoredCronContinuation === undefined
    ? consumeSubagentCompletionToolHandoff({
        handoffId: params.client?.internal?.delegatedToolPolicyHandoffId,
        sourceTool: inputProvenance?.sourceTool,
        sourceSessionKey:
          inputProvenance?.kind === "inter_session" &&
          inputProvenance.sourceTool === "subagent_settle"
            ? inputProvenance.sourceSessionKey
            : completionEvent?.childSessionKey,
        sourceSessionId: completionEvent?.childSessionId,
        targetSessionKey: params.resolvedSessionKey,
        targetSessionId: params.getAdmittedSessionId(),
        idempotencyKey: params.request.idempotencyKey,
        provider: resolvedRuntime.provider,
        model: resolvedRuntime.model,
      })
    : undefined;
}
