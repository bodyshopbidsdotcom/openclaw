import type { PreparedModelRuntimeLease } from "../../agents/prepared-model-runtime.js";
import type { SubagentRunRecord } from "../../agents/subagents/registry/subagent-registry.types.js";
import { clearAgentRunContext, getAgentRunContext } from "../../infra/agent-run-registry.js";
import type { SessionEffectRef } from "../../sessions/session-controller.lifecycle.js";
import {
  getRpcSource,
  hasRpcSource,
  type RpcSourceAdapter,
} from "../../sessions/session-controller.rpc-sources.js";
import type { registerChatAbortController } from "../chat-abort.js";
import { settleUnstartedGatewayParentResume } from "./agent-run-subagent.js";

/** Retire rejected preparation only after its adopted task and runtime resources settle. */
export async function cleanupRejectedAgentRunAdmission({
  runId,
  activeRunAbort,
  activeGatewayWorkAdmission,
  admittedRunIdentity,
  admittedRunContext,
  releaseCallerAuthority,
  lease,
  adopted,
  admissionReleased,
  failure,
}: {
  runId: string;
  activeRunAbort: ReturnType<typeof registerChatAbortController>;
  activeGatewayWorkAdmission?: SessionEffectRef;
  admittedRunIdentity:
    | (Pick<RpcSourceAdapter, "operationalRunInstance"> & { lifecycleGeneration?: string })
    | undefined;
  admittedRunContext?: ReturnType<typeof getAgentRunContext>;
  releaseCallerAuthority?: () => void;
  lease?: PreparedModelRuntimeLease;
  adopted?: SubagentRunRecord;
  admissionReleased: boolean;
  failure?: string;
}): Promise<void> {
  try {
    if (adopted) {
      await settleUnstartedGatewayParentResume(adopted, failure);
    }
  } finally {
    try {
      await lease?.[Symbol.asyncDispose]();
    } finally {
      try {
        releaseCallerAuthority?.();
      } finally {
        // A retired source may release its context, but a successor with the
        // same protocol ID owns its own registration and must remain intact.
        if (
          admittedRunContext &&
          admittedRunIdentity &&
          getAgentRunContext(runId) === admittedRunContext &&
          (!hasRpcSource(runId) || getRpcSource(runId) === activeRunAbort.entry) &&
          activeRunAbort.entry?.adapter.operationalRunInstance ===
            admittedRunIdentity.operationalRunInstance
        ) {
          clearAgentRunContext(runId, admittedRunIdentity.lifecycleGeneration);
        }
      }
      activeRunAbort.cleanup();
      if (!admissionReleased) {
        activeGatewayWorkAdmission?.release();
      }
    }
  }
}
