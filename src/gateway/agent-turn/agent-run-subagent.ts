/** Native subagent registration and paused-run adoption precede Gateway acceptance. */
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { readAcpSessionMetaForEntries } from "../../acp/runtime/session-meta-readonly.js";
import { resolveSessionStorePathForAcp } from "../../acp/runtime/session-meta-store.js";
import { getLatestLiveSubagentRunByChildSessionKey } from "../../agents/subagents/registry/subagent-registry-read.js";
import type { SubagentRunRecord } from "../../agents/subagents/registry/subagent-registry.types.js";
import type { SessionEntry } from "../../config/sessions/types.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { isAcpSessionKey } from "../../routing/session-key.js";
import type { InputProvenance } from "../../sessions/input-provenance.js";
import { readInProcessSubagentResume } from "../in-process-subagent-resume.js";
import type { AgentRunRequest } from "../server-methods/agent-request-types.js";
import { registerPluginSubagentRunFromGateway } from "../server-methods/agent-subagent-registration.js";
import { prepareParentSubagentResume } from "../session-subagent-resume.js";
import { formatForLog } from "../ws-log.js";
import type { AgentTurnContext, AgentTurnPrincipal } from "./types.js";

export async function prepareGatewaySubagentRun(params: {
  cfg: OpenClawConfig;
  client: AgentTurnPrincipal | null;
  resolvedSessionKey?: string;
  activeSessionAgentId?: string;
  inputProvenance?: InputProvenance;
  sessionEntry?: SessionEntry;
  request: Pick<AgentRunRequest, "message">;
  isOneShotModelRun: boolean;
  runId: string;
  getAdmittedSessionId: () => string;
  assertResumeAdmissionCurrent: () => SessionEntry | undefined;
  onParentResumeAdopted?: (entry: SubagentRunRecord) => void;
  context: Pick<AgentTurnContext, "resolveGatewayContext"> & {
    logGateway: Pick<AgentTurnContext["logGateway"], "warn">;
  };
}): Promise<{
  pluginSubagent: boolean;
  reactivateSubagent: boolean;
  adoptParentResume?: () => Promise<string>;
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
  if (pluginSubagent && sessionKey) {
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
  // Operator follow-ups may continue a child; inter-session delivery retains its own owner.
  const reactivateSubagent = Boolean(
    sessionKey &&
    !params.isOneShotModelRun &&
    !interSession &&
    !pluginSubagent &&
    internalOwner !== "native_subagent" &&
    !isAcpSessionKey(sessionKey),
  );
  if (!reactivateSubagent || !sessionKey) {
    return { pluginSubagent, reactivateSubagent: false };
  }
  const entry = params.assertResumeAdmissionCurrent() ?? params.sessionEntry;
  const agentId =
    params.activeSessionAgentId ??
    resolveSessionStorePathForAcp({ cfg: params.cfg, sessionKey }).agentId;
  const [acpMeta] = await readAcpSessionMetaForEntries({
    cfg: params.cfg,
    entries: [{ agentId, sessionKey, entry }],
  });
  params.assertResumeAdmissionCurrent();
  return { pluginSubagent, reactivateSubagent: acpMeta == null };
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
  const registeredSession = registeredRun?.childSessionIdentity;
  // Admission may adopt a replacement; retained rows must match its final identity.
  const inherits =
    registeredRun &&
    !registeredRun.execution.suppressSessionEffects &&
    registeredSession?.sessionId === params.admittedSessionId &&
    registeredSession.sessionId === params.admittedSessionEntry?.sessionId &&
    registeredSession.lifecycleRevision === params.admittedSessionEntry.lifecycleRevision;
  return inherits ? (registeredRun.runTimeoutSeconds ?? 0) : undefined;
}
