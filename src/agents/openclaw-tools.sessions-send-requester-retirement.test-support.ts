import { expect, it, vi } from "vitest";
import type { MockInstance } from "vitest";
import type { OpenClawConfig } from "../config/config.js";
import {
  createContext,
  createOperatorClient,
} from "../gateway/server-plugin-in-process-dispatch.test-support.js";
import { emitAgentEvent } from "../infra/agent-events.js";
import { sqliteWorkerOwnerProbe as probe } from "../infra/sqlite-worker-owner-probe.test-support.js";
import { withPluginRuntimeGatewayRequestScope } from "../plugins/runtime/gateway-request-scope.js";
import { createDeferredCore } from "../shared/deferred.js";
import * as stateWorker from "../state/openclaw-state-worker-store.js";
import { mergeAcceptedSessionSpawnsForRun } from "./accepted-session-spawn.js";
import { prepareSystemAgentRunAdmission } from "./admitted-run-context.js";
import type { EmbeddedAgentQueueHandle } from "./embedded-agent-runner/runs.js";
import {
  clearTestEmbeddedRun as clearActiveEmbeddedRun,
  createEmbeddedRunHandle,
} from "./embedded-agent-runner/runs.test-support.js";
import {
  createSessionsSendTestCaller,
  setSessionsSendTestActiveRun,
} from "./openclaw-tools.sessions-steering.test-support.js";
import { announceTesting } from "./subagents/announce/subagent-announce-overrides.test-support.js";
import { subscribeSubagentRunChanges } from "./subagents/registry/subagent-registry-publication.js";
import {
  addSubagentRunForTests,
  getSubagentRunByRunId,
  registerSubagentRun,
  resetSubagentRegistryForTests,
} from "./subagents/registry/subagent-registry.test-helpers.js";
import {
  createAdmittedGatewayToolCallerIdentity,
  withGatewayToolCallerIdentity,
} from "./tools/gateway-caller-context.js";
import type { AgentToolGatewayRequestCaller } from "./tools/in-process-gateway.js";
import { createSessionsSendTool } from "./tools/sessions-send-tool.js";

export type GatewayCall = {
  method?: string;
  params?: Record<string, unknown>;
  onAccepted?: (payload: unknown) => void;
};

function observeSubagentChange(ready: () => boolean) {
  const done = createDeferredCore();
  const stop = subscribeSubagentRunChanges("persistence", () => {
    if (ready()) {
      done.resolve();
    }
  });
  return { ...done, stop };
}

/** Gateway response for a requester turn that accepted and delivered one reply. */
export function deliveredRequesterReply(text: string) {
  return {
    status: "ok",
    inputProcessingCompleted: true,
    result: {
      payloads: [{ text }],
      deliveryStatus: { status: "sent", resultCount: 1 },
    },
  };
}

export function seedDeliveredChild(
  runId: string,
  childSessionKey: string,
  requesterSessionKey: string,
) {
  return addSubagentRunForTests({
    runId,
    childSessionKey,
    requesterSessionKey,
    requesterAgentId: "main",
    expectsCompletionMessage: true,
    createdAt: 1,
    execution: { status: "terminal", startedAt: 1, endedAt: 2, outcome: { status: "ok" } },
    completion: {
      required: true,
      resultText: "Original result",
      terminalReply: { disposition: "visible", text: "Original result" },
    },
    delivery: { status: "delivered" },
    cleanupCompletedAt: 3,
  });
}

function childDeliverySettled(runId: string) {
  const child = getSubagentRunByRunId(runId);
  return child?.delivery?.status === "delivered" && !child.requesterSettleWake;
}

function requesterScope(
  context: ReturnType<typeof createContext>,
  identity: Parameters<typeof createAdmittedGatewayToolCallerIdentity>[0],
  client?: ReturnType<typeof createOperatorClient>,
) {
  return <T>(run: () => T, receiptAuthority?: () => boolean) =>
    withPluginRuntimeGatewayRequestScope(
      { client, context, resolveGatewayContext: () => context, isWebchatConnect: () => false },
      () =>
        withGatewayToolCallerIdentity(
          createAdmittedGatewayToolCallerIdentity({ ...identity, receiptAuthority }),
          run,
        ),
    );
}

/** Reuse the coordination suite's transport and state fixture for post-commit retirement. */
export function registerSessionsSendRequesterRetirementTests({
  config,
  callGatewayMock,
  calls,
  writeFinal,
  writePair,
  sendTool,
  deliveredReply,
  requesterCalls,
  settleSessionWork,
}: {
  config: OpenClawConfig;
  callGatewayMock: AgentToolGatewayRequestCaller &
    MockInstance<(request: GatewayCall) => Promise<unknown>>;
  calls: GatewayCall[];
  writeFinal: (sessionKey: string, runId: string, text: string) => Promise<void>;
  writePair: (
    requesterKey: string,
    requesterId: string,
    childKey: string,
    childId: string,
  ) => Promise<void>;
  sendTool: (requesterKey: string, runId?: string) => ReturnType<typeof createSessionsSendTool>;
  deliveredReply: (text: string) => unknown;
  requesterCalls: (sessionKey: string) => GatewayCall[];
  settleSessionWork: () => Promise<void>;
}) {
  // A watched steer claims the child's existing completion; the completion owner, not
  // sessions_send, still delivers it once after the requester retires.
  it.each(["before publication", "after publication"] as const)(
    "delivers a watched steer once when the requester retires %s",
    async (retirement) => {
      const requesterSessionKey = "agent:main:dashboard:retiring-requester";
      const childSessionKey = "agent:main:dashboard:retiring-child";
      const requesterTurnRunId = "retiring-requester-turn";
      const childSessionId = "retiring-child";
      const runId = "retired-requester-child-run";
      await writePair(requesterSessionKey, "retiring-requester", childSessionKey, childSessionId);
      await resetSubagentRegistryForTests();
      const childPending = createDeferredCore();
      const fullReply = "Result after requester retirement";
      const terminalReply = { disposition: "visible" as const, text: fullReply };
      callGatewayMock.mockImplementation(async (request: GatewayCall) => {
        calls.push(request);
        if (request.method === "agent.wait" && request.params?.runId === runId) {
          await childPending.promise;
          await writeFinal(childSessionKey, runId, fullReply);
          return { runId, status: "ok", startedAt: 1, endedAt: Date.now(), terminalReply };
        }
        if (request.method === "agent") {
          return deliveredReply("Retired requester's child result delivered");
        }
        return {};
      });
      const queueMessage = vi.fn<EmbeddedAgentQueueHandle["queueMessage"]>(
        async (_text, options) => {
          const recorder = options?.userTurnTranscriptRecorder;
          if (!recorder || !(await recorder.persistApproved())) {
            throw new Error("Steering fixture did not commit the admitted guidance");
          }
        },
      );
      const handle = createEmbeddedRunHandle({
        runId,
        queueMessage,
        supportsTranscriptCommitWait: true,
        guarded: true,
      });
      await registerSubagentRun({
        runId,
        childSessionKey,
        childAgentId: "main",
        sessionEntry: { sessionId: childSessionId },
        requesterSessionKey,
        requesterDisplayKey: requesterSessionKey,
        requesterAgentId: "main",
        task: "Existing child work",
        cleanup: "keep",
        spawnMode: "session",
        expectsCompletionMessage: true,
      });
      await setSessionsSendTestActiveRun(
        childSessionId,
        handle,
        childSessionKey,
        requesterSessionKey,
        config,
      );
      const admission = prepareSystemAgentRunAdmission(
        config,
        requesterTurnRunId,
        "main",
        "watched-followup-retirement",
      );
      const admittedRunContext = await admission.admit("embedded");
      const context = createContext();
      // This fixture uses the mocked transport; it does not boot a hosted recovery runtime.
      context.localEmbedded = true;
      context.getRuntimeConfig = () => config;
      context.resolveGatewayContext = () => context;
      let requesterRetired = false;
      const retireRequester = () => {
        expect(mergeAcceptedSessionSpawnsForRun(admission.operationalRunInstance)).toEqual([]);
        requesterRetired = true;
        admission.close();
      };
      const retireBeforePublication = probe.command(
        stateWorker,
        async (command, executeOptions, scope) => {
          const result = await scope.execute(command, executeOptions);
          if (
            retirement === "before publication" &&
            !requesterRetired &&
            command.type === "subagents.persistChanges"
          ) {
            retireRequester();
          }
          return result;
        },
      );
      const stopRetiring = subscribeSubagentRunChanges("persistence", () => {
        if (
          retirement === "after publication" &&
          !requesterRetired &&
          getSubagentRunByRunId(runId)?.requesterTurnRunId === requesterTurnRunId
        ) {
          retireRequester();
        }
      });
      announceTesting.setDepsForTest({ callGateway: callGatewayMock });
      let stopObserving = () => {};
      try {
        const withCaller = requesterScope(context, {
          admittedRunContext,
          agentId: "main",
          sessionKey: requesterSessionKey,
          requesterOrigin: createSessionsSendTestCaller(requesterSessionKey).requesterOrigin,
        });
        const result = await withCaller(() =>
          sendTool(requesterSessionKey, requesterTurnRunId).execute("retiring-watched-send", {
            sessionKey: childSessionKey,
            mode: "steer",
            watch: true,
            timeoutSeconds: 0,
            message: "Return the follow-up result",
          }),
        );
        expect(requesterRetired).toBe(true);
        expect(result.details).toMatchObject({ status: "error", sentBeforeError: true });
        expect(getSubagentRunByRunId(runId)?.requesterTurnRunId).toBeUndefined();
        expect(queueMessage).toHaveBeenCalledOnce();
        expect(requesterCalls(requesterSessionKey)).toHaveLength(0);
        const delivered = observeSubagentChange(() => childDeliverySettled(runId));
        stopObserving = delivered.stop;
        clearActiveEmbeddedRun(childSessionId, handle, childSessionKey);
        childPending.resolve();
        await delivered.promise;
        await settleSessionWork();
        expect(requesterCalls(requesterSessionKey)).toHaveLength(1);
        expect(requesterCalls(requesterSessionKey)[0]?.params).toMatchObject({
          message: "Continue the OpenClaw runtime event.",
          internalEvents: [
            {
              type: "task_completion",
              childSessionKey,
              result: expect.stringContaining(terminalReply.text),
            },
          ],
          inputProvenance: { sourceTool: "subagent_announce" },
        });
        emitAgentEvent({
          runId,
          sessionKey: childSessionKey,
          stream: "lifecycle",
          data: { phase: "end", endedAt: Date.now(), terminalReply },
        });
        await settleSessionWork();
        expect(requesterCalls(requesterSessionKey)).toHaveLength(1);
      } finally {
        stopRetiring();
        retireBeforePublication.mockRestore();
        admission.close();
        clearActiveEmbeddedRun(childSessionId, handle, childSessionKey);
        childPending.resolve();
        stopObserving();
        await resetSubagentRegistryForTests();
        await settleSessionWork();
        announceTesting.setDepsForTest();
      }
    },
  );
}
