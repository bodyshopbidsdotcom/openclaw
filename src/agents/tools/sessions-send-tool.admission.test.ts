import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import { awaitGateBeforeSettlement } from "../../../test/helpers/promise.js";
import { createReplyTurnParticipants } from "../../auto-reply/reply/reply-run-registry.tool-authority.js";
import { setRuntimeConfigSnapshot } from "../../config/config.js";
import { replaceSessionEntry } from "../../config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import {
  captureGatewayDeviceRevocation,
  readGatewayDeviceSourceAuthority,
} from "../../gateway/device-revocation.js";
import { readInProcessSessionDeliveryGeneration } from "../../gateway/in-process-session-delivery.js";
import * as operatorCapture from "../../gateway/operator-run-authority.js";
import { withOperatorToolGatewayAuthority } from "../../gateway/server-plugin-in-process-dispatch.js";
import {
  createContext,
  createOperatorClient,
} from "../../gateway/server-plugin-in-process-dispatch.test-support.js";
import { drainSystemEvents, peekSystemEvents } from "../../infra/system-events.js";
import { setActivePluginRegistry } from "../../plugins/runtime.js";
import {
  getPluginRuntimeGatewayRequestScope,
  withPluginRuntimeGatewayRequestScope,
} from "../../plugins/runtime/gateway-request-scope.js";
import * as gatewayWorkAdmission from "../../process/gateway-work-admission.js";
import {
  getActiveGatewayRootWorkCount,
  resetGatewayWorkAdmission,
} from "../../process/gateway-work-admission.js";
import * as sessionStateEvents from "../../sessions/session-state-events.js";
import { createDeferredCore } from "../../shared/deferred.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../../test-utils/openclaw-test-state.js";
import { createSessionConversationTestRegistry } from "../../test-utils/session-conversation-registry.js";
import { normalizeSessionDeliveryState } from "../../utils/delivery-context.shared.js";
import { createOpenClawTools } from "../openclaw-tools.js";
import "../test-helpers/fast-openclaw-tools-sessions.js";
import { withGatewayToolCallerIdentity } from "./gateway-caller-context.js";
import * as inProcessGateway from "./in-process-gateway.js";
import type { AgentToolGatewayRequestCaller } from "./in-process-gateway.js";
import * as sessionsSendDelivery from "./sessions-send-tool.delivery.js";
import { createSessionsSendTool } from "./sessions-send-tool.js";

const requesterSessionKey = "agent:main:main";
const targetSessionKey = "agent:main:dashboard:admission-target";
const runId = "sessions-send-admission-run";
const config = {
  agents: { ownership: "explicit", entries: { main: {} } },
  session: { mainKey: "main", scope: "per-sender" },
  tools: { sessions: { visibility: "all" }, agentToAgent: { enabled: true } },
} satisfies OpenClawConfig;

// The tool returns once the self-reply observer is admitted; tests join the observer itself.
function trackSelfReply() {
  const observers: Promise<unknown>[] = [];
  const continueDetached = gatewayWorkAdmission.runWithGatewayDetachedWorkContinuation;
  const spy = vi
    .spyOn(gatewayWorkAdmission, "runWithGatewayDetachedWorkContinuation")
    .mockImplementation((run, origin) => {
      const observer = continueDetached(run, origin);
      if (origin === "session:self-reply") {
        observers.push(observer);
      }
      return observer;
    });
  return { observers, spy, settle: () => Promise.all(observers) };
}

describe("sessions_send dispatch admission", () => {
  let state: OpenClawTestState;
  let registerWatch: MockInstance<typeof sessionStateEvents.registerSessionStateWatch>;

  beforeEach(async () => {
    state = await createOpenClawTestState({ scenario: "minimal" });
    setRuntimeConfigSnapshot(config);
    setActivePluginRegistry(createSessionConversationTestRegistry());
    resetGatewayWorkAdmission();
    registerWatch = vi.spyOn(sessionStateEvents, "registerSessionStateWatch");
    for (const [sessionKey, sessionId] of [
      [requesterSessionKey, "requester-session"],
      [targetSessionKey, "target-session"],
    ] as const) {
      await replaceSessionEntry(
        { agentId: "main", sessionKey },
        { sessionId, updatedAt: Date.now() },
      );
    }
  });

  afterEach(async () => {
    await vi.waitFor(() => expect(getActiveGatewayRootWorkCount()).toBe(0));
    registerWatch.mockRestore();
    resetGatewayWorkAdmission();
    await state.cleanup();
  });

  it("applies peer preferences to new notifications without revoking accepted events", async () => {
    const callGateway = vi.fn();
    callGateway.mockResolvedValue({ key: targetSessionKey, agentId: "main" });
    const tool = createSessionsSendTool({
      agentSessionKey: requesterSessionKey,
      config,
      callGateway,
      idempotencyKey: runId,
    });
    const first = await tool.execute("notify-peer", {
      sessionKey: targetSessionKey,
      message: "accepted notice",
      mode: "notify",
    });
    expect(first.details).toMatchObject({
      status: "queued",
      durability: "process",
      runStarted: false,
    });
    await replaceSessionEntry(
      { agentId: "main", sessionKey: targetSessionKey },
      { sessionId: "target-session", updatedAt: 2, communication: { receive: "never" } },
    );
    expect(peekSystemEvents(targetSessionKey)).toHaveLength(1);
    const second = await tool.execute("notify-denied", {
      sessionKey: targetSessionKey,
      message: "unaccepted notice",
      mode: "notify",
    });
    expect(second.details).toMatchObject({ status: "forbidden" });
    const consumed = drainSystemEvents(targetSessionKey);
    expect(consumed).toHaveLength(1);
    expect(consumed[0]).toContain("accepted notice");
    expect(consumed[0]).not.toContain("unaccepted notice");
  });

  it.each(["send", "receive"] as const)(
    "rechecks an in-place default %s policy publication at final input admission",
    async (direction) => {
      const currentConfig: OpenClawConfig = {
        ...config,
        session: { ...config.session, communication: { send: "always", receive: "always" } },
      };
      setRuntimeConfigSnapshot(currentConfig);
      let admitted = false;
      const callGateway = vi.fn();
      callGateway.mockImplementation(
        async (request: Parameters<AgentToolGatewayRequestCaller>[0]) => {
          if (request.method === "sessions.resolve") {
            return { key: targetSessionKey, agentId: "main" };
          }
          if (request.method === "agent") {
            currentConfig.session!.communication![direction] = "never";
            setRuntimeConfigSnapshot(currentConfig);
            request.assertDispatchCurrent?.();
            request.sessionMutationCommitGuard?.();
            admitted = true;
            return { runId, status: "accepted" };
          }
          throw new Error(`Unexpected Gateway method: ${request.method}`);
        },
      );
      const result = await createSessionsSendTool({
        agentSessionKey: requesterSessionKey,
        config: currentConfig,
        callGateway,
        idempotencyKey: runId,
      }).execute("revoked-default", {
        sessionKey: targetSessionKey,
        message: "Must not enter the target after policy revocation",
        mode: "followup",
        timeoutSeconds: 0,
      });
      expect(admitted).toBe(false);
      expect(result.details).toMatchObject({
        status: "error",
        error: expect.stringContaining("communication"),
      });
    },
  );

  const callerKeys = [requesterSessionKey, "agent:main:telegram:direct:peer-1"];
  it.each(callerKeys)(
    "retains a self-send reply source before returning (%s)",
    async (sourceKey) => {
      if (sourceKey !== requesterSessionKey) {
        await replaceSessionEntry(
          { agentId: "main", sessionKey: sourceKey },
          { sessionId: "key-only-requester", updatedAt: 1 },
        );
      }
      const context = createContext();
      // The admitted Gateway owns one current snapshot, not the factory's fresh {} per read.
      context.getRuntimeConfig = () => config;
      const owner = createOperatorClient({ profileName: "send-owner", scopes: ["operator.write"] });
      const source = captureGatewayDeviceRevocation(
        context,
        { deviceId: "send-device", role: "operator" },
        () => true,
      );
      const selfReply = trackSelfReply();
      const capturing = createDeferredCore();
      const allowCapture = createDeferredCore();
      const finishRun = createDeferredCore();
      let invocationStarted = false;
      let returned = false;
      const capture = operatorCapture.captureGatewayOperatorRunAuthority;
      const heldCapture = vi
        .spyOn(operatorCapture, "captureGatewayOperatorRunAuthority")
        .mockImplementation(async (...args) => {
          const captured = await capture(...args);
          if (invocationStarted) {
            capturing.resolve();
            await allowCapture.promise;
          }
          return captured;
        });
      const callGateway = vi.fn();
      callGateway.mockImplementation(
        async (request: Parameters<AgentToolGatewayRequestCaller>[0]) => {
          if (request.method === "sessions.resolve") {
            return { key: sourceKey, agentId: "main" };
          }
          if (request.method === "sessions.list") {
            return { sessions: [{ key: sourceKey, agentId: "main", kind: "direct" }] };
          }
          if (request.method === "agent") {
            return { runId, status: "accepted" };
          }
          if (request.method === "agent.wait") {
            await finishRun.promise;
            return { status: "ok", terminalReply: { disposition: "empty" } };
          }
          throw new Error(`Unexpected Gateway method: ${request.method}`);
        },
      );
      try {
        const pending = withPluginRuntimeGatewayRequestScope(
          {
            client: owner,
            context,
            isWebchatConnect: () => false,
            hasCurrentClientAuthority: source.isCurrent,
          },
          () =>
            withOperatorToolGatewayAuthority(
              {
                authenticatedUserProfile: owner.authenticatedUserProfile,
                scopes: owner.connect.scopes ?? [],
              },
              async () => {
                const participants = createReplyTurnParticipants({
                  operatorAuthority:
                    getPluginRuntimeGatewayRequestScope()?.client?.internal?.operatorRunAuthority,
                });
                try {
                  return await withGatewayToolCallerIdentity(
                    {
                      agentId: "main",
                      sessionKey: sourceKey,
                      personalToolParticipants: participants,
                      gatewayContextResolver: () => context,
                      receiptAuthority: () => true,
                    },
                    () => {
                      invocationStarted = true;
                      return createSessionsSendTool({
                        agentSessionKey: sourceKey,
                        config,
                        callGateway,
                        idempotencyKey: runId,
                      }).execute("self-send", {
                        sessionKey: sourceKey,
                        message: "Continue the task",
                        mode: "followup",
                        timeoutSeconds: 0,
                      });
                    },
                  );
                } finally {
                  returned = true;
                  participants.close();
                }
              },
            ),
        );
        await awaitGateBeforeSettlement(
          capturing.promise,
          pending.then((result) => {
            throw new Error(
              `Send settled before reply-source capture: ${JSON.stringify(result.details)}`,
            );
          }),
          "Send completed without reaching reply-source capture",
        );
        vi.useFakeTimers();
        await vi.advanceTimersByTimeAsync(0);
        vi.useRealTimers();
        // The caller turn stays open until the observer owns its source authority.
        expect(returned).toBe(false);
        expect(selfReply.observers).toHaveLength(0);
        allowCapture.resolve();
        const result = await pending;
        expect(result.details).toMatchObject({
          status: "accepted",
          delivery: { status: "pending" },
        });
        expect(selfReply.observers).toHaveLength(1);
        // The pending observer keeps its own source hold after the caller's hold ends.
        source.release();
        expect(readGatewayDeviceSourceAuthority(source.isCurrent)?.()).toBe(true);
        finishRun.resolve();
        await selfReply.settle();
      } finally {
        vi.useRealTimers();
        allowCapture.resolve();
        finishRun.resolve();
        await selfReply.settle();
        selfReply.spy.mockRestore();
        heldCapture.mockRestore();
        source.release();
      }
    },
  );
  it("keeps an opaque self-send on its admitted source route after a later inbound turn", async () => {
    const sessionKey = "agent:main:direct:identity-linked-person";
    const originalRoute = { channel: "telegram", accountId: "default", to: "original-recipient" };
    const laterRoute = { channel: "telegram", accountId: "other", to: "later-recipient" };
    const selfReply = trackSelfReply();
    const entry = {
      sessionId: "self-session",
      updatedAt: 1,
      lifecycleRevision: "original-generation",
    };
    await replaceSessionEntry(
      { agentId: "main", sessionKey },
      { ...entry, delivery: normalizeSessionDeliveryState({ context: originalRoute }) },
    );
    const callGateway = vi.fn();
    callGateway.mockImplementation(
      async (request: Parameters<AgentToolGatewayRequestCaller>[0]) => {
        switch (request.method) {
          case "sessions.resolve":
            return { key: sessionKey, agentId: "main" };
          case "sessions.list":
            return {
              sessions: [{ key: sessionKey, agentId: "main", deliveryContext: laterRoute }],
            };
          case "agent":
            await replaceSessionEntry(
              { agentId: "main", sessionKey },
              {
                ...entry,
                updatedAt: 2,
                delivery: normalizeSessionDeliveryState({ context: laterRoute }),
              },
            );
            return { runId };
          case "agent.wait":
            return {
              status: "ok",
              terminalReply: { disposition: "visible", text: "Task complete" },
            };
          case "send":
            return { messageId: "final-reply" };
          default:
            throw new Error(`Unexpected Gateway method: ${request.method}`);
        }
      },
    );
    const gateway = vi
      .spyOn(inProcessGateway, "callAgentToolGatewayRequest")
      .mockImplementation(callGateway);
    const mutation = vi
      .spyOn(sessionsSendDelivery, "callSessionsSendGateway")
      .mockImplementation(callGateway);
    try {
      const tool = createOpenClawTools({
        agentSessionKey: sessionKey,
        sessionId: entry.sessionId,
        agentChannel: originalRoute.channel,
        agentAccountId: originalRoute.accountId,
        currentMessagingTarget: originalRoute.to,
        config,
        disableMessageTool: true,
        disablePluginTools: true,
        wrapBeforeToolCallHook: false,
      }).find((candidate) => candidate.name === "sessions_send");
      expect(tool).toBeDefined();
      const result = await tool!.execute("self-followup", {
        sessionKey,
        message: "Complete this task",
        mode: "followup",
        timeoutSeconds: 0,
      });
      expect(result.details).toMatchObject({ status: "accepted", delivery: { status: "pending" } });
      await selfReply.settle();
      const requests = callGateway.mock.calls.map(([request]) => request);
      expect.soft(requests.filter((request) => request.method === "agent")).toEqual([
        expect.objectContaining({
          params: expect.objectContaining({
            ...originalRoute,
            sessionKey,
            deliver: false,
            sourceReplyDeliveryMode: "message_tool_only",
            inputProvenance: expect.objectContaining({
              kind: "inter_session",
              sourceSessionKey: sessionKey,
            }),
          }),
        }),
      ]);
      expect.soft(requests.filter((request) => request.method === "send")).toEqual([
        expect.objectContaining({
          params: expect.objectContaining({ ...originalRoute, message: "Task complete" }),
        }),
      ]);
      const sendParams = requests.find((request) => request.method === "send")?.params;
      expect(readInProcessSessionDeliveryGeneration(sendParams)).toMatchObject({
        agentId: "main",
        sessionKey,
        sessionId: entry.sessionId,
        lifecycleRevision: entry.lifecycleRevision,
      });
      expect(sendParams).toHaveProperty("idempotencyKey", `sessions-send:${runId}`);
      expect(sendParams).not.toHaveProperty("sessionGeneration");
    } finally {
      selfReply.spy.mockRestore();
      gateway.mockRestore();
      mutation.mockRestore();
    }
  });

  it.each([
    { admission: "rejected", timeoutSeconds: 1 },
    { admission: "pending", timeoutSeconds: 1 },
  ] as const)(
    "does not install a watch when admission is $admission (wait $timeoutSeconds)",
    async ({ admission, timeoutSeconds }) => {
      const requests: Parameters<AgentToolGatewayRequestCaller>[0][] = [];
      const callGateway = vi.fn();
      callGateway.mockImplementation(
        async (request: Parameters<AgentToolGatewayRequestCaller>[0]) => {
          requests.push(request);
          if (request.method === "sessions.resolve") {
            return { key: targetSessionKey, agentId: "main" };
          }
          if (request.method === "sessions.list") {
            return { sessions: [{ key: targetSessionKey, agentId: "main", kind: "direct" }] };
          }
          if (request.method === "agent") {
            if (admission === "rejected") {
              throw new Error("Task admission failed before dispatch");
            }
            return { runId, status: "in_flight", admissionPending: true };
          }
          if (request.method === "agent.wait") {
            return { status: "timeout" };
          }
          throw new Error(`Unexpected Gateway method: ${request.method}`);
        },
      );
      const tool = createSessionsSendTool({
        agentSessionKey: requesterSessionKey,
        config,
        callGateway,
        idempotencyKey: runId,
      });

      const result = await tool.execute("send-followup", {
        sessionKey: targetSessionKey,
        message: "Continue the requested task.",
        mode: "followup",
        watch: true,
        timeoutSeconds,
      });

      expect.soft(result.details).toMatchObject({
        status: "error",
        runId,
        sessionKey: targetSessionKey,
        error:
          admission === "rejected"
            ? "Task admission failed before dispatch"
            : expect.stringMatching(/admission|unconfirmed|pending/i),
      });
      if (admission === "pending") {
        expect.soft(result.details).toMatchObject({
          sentBeforeError: true,
          error: expect.stringMatching(/(?:inspect|check).*before.*retry/i),
        });
      } else {
        expect.soft(result.details).not.toHaveProperty("sentBeforeError");
      }
      expect.soft(registerWatch).not.toHaveBeenCalled();
      expect.soft(requests.filter((request) => request.method === "agent")).toHaveLength(1);
      expect.soft(requests.some((request) => request.method === "agent.wait")).toBe(false);
    },
  );
});
