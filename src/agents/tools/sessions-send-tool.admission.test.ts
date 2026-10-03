import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import { setRuntimeConfigSnapshot } from "../../config/config.js";
import { replaceSessionEntry } from "../../config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { readInProcessSessionDeliveryGeneration } from "../../gateway/in-process-session-delivery.js";
import { setActivePluginRegistry } from "../../plugins/runtime.js";
import {
  getActiveGatewayRootWorkCount,
  resetGatewayWorkAdmission,
} from "../../process/gateway-work-admission.js";
import * as sessionStateEvents from "../../sessions/session-state-events.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../../test-utils/openclaw-test-state.js";
import { createSessionConversationTestRegistry } from "../../test-utils/session-conversation-registry.js";
import { normalizeSessionDeliveryState } from "../../utils/delivery-context.shared.js";
import { createOpenClawTools } from "../openclaw-tools.js";
import "../test-helpers/fast-openclaw-tools-sessions.js";
import * as inProcessGateway from "./in-process-gateway.js";
import type { AgentToolGatewayRequestCaller } from "./in-process-gateway.js";
import * as sessionsSendSelfReply from "./sessions-send-self-reply.js";
import { createSessionsSendTool } from "./sessions-send-tool.js";

const requesterSessionKey = "agent:main:main";
const targetSessionKey = "agent:main:dashboard:admission-target";
const runId = "sessions-send-admission-run";
const config = {
  agents: { ownership: "explicit", entries: { main: {} } },
  session: { mainKey: "main", scope: "per-sender" },
  tools: { sessions: { visibility: "all" }, agentToAgent: { enabled: true } },
} satisfies OpenClawConfig;

function trackSelfReply() {
  const settled: Promise<void>[] = [];
  const start = sessionsSendSelfReply.startSessionsSendSelfReply;
  const spy = vi
    .spyOn(sessionsSendSelfReply, "startSessionsSendSelfReply")
    .mockImplementation((params) => {
      const observed = start(params);
      settled.push(observed);
      return observed;
    });
  return { spy, settle: () => Promise.all(settled) };
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
      expect(selfReply.spy).toHaveBeenCalledOnce();
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
