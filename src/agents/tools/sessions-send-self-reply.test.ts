import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  getOwnedSessionTranscriptWriterFence,
  withOwnedSessionTranscriptWrites,
} from "../../config/sessions/transcript-write-context.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { readInProcessSessionDeliveryGeneration } from "../../gateway/in-process-session-delivery.js";
import { setActivePluginRegistry } from "../../plugins/runtime.js";
import * as gatewayWorkAdmission from "../../process/gateway-work-admission.js";
import {
  getActiveGatewayRootWorkCount,
  resetGatewayWorkAdmission,
} from "../../process/gateway-work-admission.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../../test-utils/openclaw-test-state.js";
import { createSessionConversationTestRegistry } from "../../test-utils/session-conversation-registry.js";
import "../test-helpers/fast-openclaw-tools-sessions.js";
import type { AgentToolGatewayRequestCaller } from "./in-process-gateway.js";
import type { GatewaySessionListRow } from "./sessions-helpers.js";
import { startSessionsSendSelfReply } from "./sessions-send-self-reply.js";
import { createSessionsSendTool } from "./sessions-send-tool.js";

type GatewayRequest = Parameters<AgentToolGatewayRequestCaller>[0];

const sessionKey = "agent:main:telegram:group:source";
const generation = {
  agentId: "main",
  storePath: "/tmp/openclaw-self-reply-sessions.sqlite",
  sessionKey,
  sessionId: "source-session",
  lifecycleRevision: null,
};

function receipt(sourceReplyDelivered: boolean) {
  return {
    runId: "self-run",
    sessionId: "source-session",
    turnId: "source-turn",
    requested: { provider: "provider", model: "model" },
    effective: { provider: "provider", model: "model", responseModel: "model" },
    successfulToolNames: sourceReplyDelivered ? ["message"] : [],
    sourceReplyDelivered,
    rerouted: false,
    terminalDisposition: "visible",
  };
}

function gatewayFor(
  wait: Record<string, unknown> | Array<Record<string, unknown>>,
  sessions: GatewaySessionListRow[] = [],
) {
  const requests: GatewayRequest[] = [];
  const waits = Array.isArray(wait) ? [...wait] : [wait];
  const callGateway = vi.fn(async (request: GatewayRequest) => {
    requests.push(request);
    if (request.method === "agent.wait") {
      return waits.length > 1 ? waits.shift() : waits[0];
    }
    if (request.method === "sessions.describe") {
      const params = request.params as { key: string; agentId?: string };
      return {
        session:
          sessions.find((row) => row.key === params.key && row.agentId === params.agentId) ?? null,
      };
    }
    return {};
  }) as unknown as AgentToolGatewayRequestCaller;
  return { requests, callGateway };
}

// The tool returns once the observer is admitted; tests join the detached observer itself.
const observers: Promise<unknown>[] = [];
beforeEach(() => {
  const continueDetached = gatewayWorkAdmission.runWithGatewayDetachedWorkContinuation;
  vi.spyOn(gatewayWorkAdmission, "runWithGatewayDetachedWorkContinuation").mockImplementation(
    (run, origin) => {
      const observer = continueDetached(run, origin);
      observers.push(observer);
      return observer;
    },
  );
});
afterEach(() => {
  vi.restoreAllMocks();
  observers.length = 0;
});
async function settleObservers() {
  await Promise.all(observers);
}

function sendParams(requests: GatewayRequest[]) {
  const sends = requests.filter((request) => request.method === "send");
  expect(sends).toHaveLength(1);
  return sends[0]?.params as Record<string, unknown>;
}

async function selfReply(
  callGateway: AgentToolGatewayRequestCaller,
  overrides: Partial<Parameters<typeof startSessionsSendSelfReply>[0]> = {},
) {
  const key = overrides.sessionKey ?? sessionKey;
  await startSessionsSendSelfReply({
    callGateway,
    runId: "self-run",
    sessionKey: key,
    displayKey: key,
    agentId: "main",
    waitTimeoutMs: 30_000,
    requesterDeliveryGeneration: { ...generation, sessionKey: key },
    ...overrides,
  });
  await settleObservers();
}

describe("sessions_send self-reply delivery", () => {
  beforeEach(() => {
    setActivePluginRegistry(createSessionConversationTestRegistry());
  });

  it("delivers the caller's own answer once to its captured source route", async () => {
    const { requests, callGateway } = gatewayFor({
      status: "ok",
      terminalReply: { disposition: "visible", text: "Task complete" },
    });
    await startSessionsSendSelfReply({
      callGateway,
      runId: "self-run",
      sessionKey,
      displayKey: sessionKey,
      agentId: "main",
      waitTimeoutMs: 30_000,
      requesterChannel: "telegram",
      requesterOrigin: { channel: "telegram", to: "telegram:source", accountId: "work" },
      requesterDeliveryGeneration: generation,
    });
    await settleObservers();

    const sends = requests.filter((request) => request.method === "send");
    expect(sends).toEqual([
      expect.objectContaining({
        params: expect.objectContaining({
          channel: "telegram",
          to: "telegram:source",
          accountId: "work",
          message: "Task complete",
          idempotencyKey: "sessions-send:self-run",
        }),
      }),
    ]);
    expect(readInProcessSessionDeliveryGeneration(sends[0]?.params)).toMatchObject(generation);
  });

  it("keeps the Telegram forum topic of a session without a captured route", async () => {
    const { requests, callGateway } = gatewayFor({
      status: "ok",
      terminalReply: { disposition: "visible", text: "Worker completed successfully" },
    });
    await selfReply(callGateway, { sessionKey: "agent:main:telegram:group:-100123:topic:554" });

    expect(sendParams(requests)).toMatchObject({
      channel: "telegram",
      to: "group:-100123",
      threadId: "554",
      agentId: "main",
    });
  });

  it("uses the stored direct-message route for the source account", async () => {
    const key = "agent:main:discord:direct:alice";
    const { requests, callGateway } = gatewayFor(
      { status: "ok", terminalReply: { disposition: "visible", text: "Worker completed" } },
      [
        {
          key,
          agentId: "main",
          kind: "direct",
          classification: "channel",
          channel: "discord",
          deliveryContext: { channel: "discord", to: "user:alice", accountId: "thinker" },
        },
      ],
    );
    await selfReply(callGateway, { sessionKey: key });

    expect(requests.map((request) => request.method)).toContain("sessions.describe");
    expect(sendParams(requests)).toMatchObject({
      channel: "discord",
      to: "user:alice",
      accountId: "thinker",
    });
  });

  it("projects generated voice media into the source delivery contract", async () => {
    const { requests, callGateway } = gatewayFor({
      status: "ok",
      terminalReply: {
        disposition: "visible",
        text: "Your voice note is ready.\nMEDIA:./generated.ogg\n[[audio_as_voice]]",
      },
    });
    await selfReply(callGateway, {
      requesterOrigin: { channel: "telegram", to: "telegram:source" },
    });

    const params = sendParams(requests);
    expect(params).toMatchObject({
      message: "Your voice note is ready.",
      mediaUrls: ["./generated.ogg"],
      asVoice: true,
    });
    expect(params).not.toHaveProperty("sessionKey");
  });

  it("keeps observing the run through queued and retried waits", async () => {
    const { requests, callGateway } = gatewayFor([
      { status: "timeout" },
      { status: "pending", timeoutPhase: "queue" },
      { status: "timeout", pendingError: true, error: "retrying provider" },
      { status: "ok", terminalReply: { disposition: "visible", text: "Delayed channel reply" } },
    ]);
    await selfReply(callGateway, {
      requesterOrigin: { channel: "telegram", to: "telegram:source" },
    });

    expect(sendParams(requests)).toMatchObject({ message: "Delayed channel reply" });
    expect(requests.map((request) => request.method)).not.toContain("agent");
  });

  it.each([
    {
      name: "the run already answered its source conversation",
      wait: {
        status: "ok",
        terminalReply: { disposition: "visible", text: "Task complete" },
        terminalReceipt: receipt(true),
      },
    },
    { name: "the run failed", wait: { status: "error", error: "provider failed" } },
    {
      name: "the run produced only a control token",
      wait: { status: "ok", terminalReply: { disposition: "visible", text: "NO_REPLY" } },
    },
    {
      name: "the original session generation is unavailable",
      wait: { status: "ok", terminalReply: { disposition: "visible", text: "Task complete" } },
      withoutGeneration: true,
    },
    {
      name: "the stored route belongs to another channel",
      wait: { status: "ok", terminalReply: { disposition: "visible", text: "Task complete" } },
      storedRouteOnly: true,
    },
  ])(
    "sends nothing and wakes nobody when $name",
    async ({ wait, storedRouteOnly, withoutGeneration }) => {
      const { requests, callGateway } = gatewayFor(wait);
      await startSessionsSendSelfReply({
        callGateway,
        runId: "self-run",
        sessionKey: storedRouteOnly ? "agent:main:discord:group:dev" : sessionKey,
        displayKey: storedRouteOnly ? "agent:main:discord:group:dev" : sessionKey,
        agentId: "main",
        waitTimeoutMs: 30_000,
        requesterChannel: "telegram",
        ...(storedRouteOnly
          ? {}
          : { requesterOrigin: { channel: "telegram", to: "telegram:source" } }),
        requesterDeliveryGeneration: withoutGeneration ? undefined : generation,
      });
      await settleObservers();

      expect(requests.map((request) => request.method)).not.toContain("send");
      expect(requests.map((request) => request.method)).not.toContain("agent");
    },
  );
});

describe("sessions_send fire-and-forget self-send", () => {
  let state: OpenClawTestState;

  beforeEach(async () => {
    state = await createOpenClawTestState({ scenario: "minimal" });
    setActivePluginRegistry(createSessionConversationTestRegistry());
    resetGatewayWorkAdmission();
  });

  afterEach(async () => {
    await settleObservers();
    expect(getActiveGatewayRootWorkCount()).toBe(0);
    resetGatewayWorkAdmission();
    await state.cleanup();
  });

  it("observes its own run outside the parent's transcript ownership", async () => {
    let inheritedFence: ReturnType<typeof getOwnedSessionTranscriptWriterFence> | "unobserved" =
      "unobserved";
    const callGateway = vi.fn(async (request: GatewayRequest) => {
      if (request.method === "agent") {
        return { runId: "run-self-send", acceptedAt: 123 };
      }
      if (request.method === "agent.wait") {
        inheritedFence = getOwnedSessionTranscriptWriterFence();
        return { status: "ok", terminalReply: { disposition: "empty" } };
      }
      return {};
    }) as unknown as AgentToolGatewayRequestCaller;
    let parentTranscriptWriteCalls = 0;
    const tool = createSessionsSendTool({
      agentSessionKey: "agent:main:main",
      agentChannel: "telegram",
      callGateway,
      config: {
        session: { scope: "per-sender", mainKey: "main" },
        tools: { agentToAgent: { enabled: false } },
      } satisfies OpenClawConfig,
    });

    const result = await withOwnedSessionTranscriptWrites(
      {
        sessionKey: "agent:main:main",
        sessionTarget: {
          expectedWriterRunId: "disposed-parent-run",
          sessionKey: "agent:main:main",
        },
        withTranscriptWrite: async (run) => {
          parentTranscriptWriteCalls += 1;
          return await run();
        },
      },
      () =>
        tool.execute("self-send", {
          sessionKey: "agent:main:main",
          message: "ping",
          timeoutSeconds: 0,
        }),
    );

    expect(result.details).toMatchObject({
      status: "accepted",
      delivery: { status: "pending" },
    });
    await settleObservers();
    expect(inheritedFence).toBeUndefined();
    expect(parentTranscriptWriteCalls).toBe(0);
  });

  it("recognizes an aliased requester key as the same session", async () => {
    const callGateway = vi.fn(async (request: GatewayRequest) => {
      if (request.method === "sessions.list") {
        return { sessions: [{ key: "agent:main:main", kind: "direct" }] };
      }
      if (request.method === "agent") {
        return { runId: "run-alias-self-send", acceptedAt: 123 };
      }
      return request.method === "agent.wait"
        ? { status: "ok", terminalReply: { disposition: "empty" } }
        : {};
    }) as unknown as AgentToolGatewayRequestCaller;
    const result = await createSessionsSendTool({
      agentSessionKey: "main",
      agentChannel: "telegram",
      callGateway,
      config: {
        session: { scope: "per-sender", mainKey: "agent:main:main" },
        tools: { agentToAgent: { enabled: false } },
      } satisfies OpenClawConfig,
    }).execute("aliased-self-send", {
      sessionKey: "agent:main:main",
      message: "ping",
      timeoutSeconds: 0,
    });

    // Only a recognized self-send keeps a reply observer; the alias must not hide it.
    expect(result.details).toMatchObject({
      status: "accepted",
      sessionKey: "main",
      delivery: { status: "pending" },
    });
    await settleObservers();
    expect(callGateway).toHaveBeenCalledWith(
      expect.objectContaining({
        method: "agent.wait",
        params: expect.objectContaining({ runId: "run-alias-self-send" }),
      }),
    );
  });
});
