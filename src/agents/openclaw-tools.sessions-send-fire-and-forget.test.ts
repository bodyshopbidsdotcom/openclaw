import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
// Verifies fire-and-forget (effective timeoutSeconds 0) sessions_send semantics.
import type { OpenClawConfig } from "../config/config.js";
import { resolveSessionStorePathCore } from "../config/sessions/paths.js";
import { replaceSessionEntry } from "../config/sessions/session-accessor.js";
import type { SessionEntry } from "../config/sessions/types.js";
import { parseAgentSessionKey } from "../routing/session-key.js";

const { config, callGatewayMock, runA2AFlowMock } = vi.hoisted(() => ({
  config: {
    session: { mainKey: "main", scope: "per-sender" },
    agents: { list: [{ id: "main", default: true }, { id: "peer" }] },
    tools: { sessions: { visibility: "all" }, agentToAgent: { enabled: true } },
  } as OpenClawConfig,
  callGatewayMock: vi.fn(),
  runA2AFlowMock: vi.fn(async (_params: Record<string, unknown>) => {}),
}));
vi.mock("../gateway/call.js", () => ({ callGateway: (opts: unknown) => callGatewayMock(opts) }));
vi.mock("../config/config.js", () => ({
  getRuntimeConfig: () => config,
  resolveGatewayPort: () => 18789,
}));
vi.mock("./tools/sessions-send-tool.self-reply.js", () => ({
  runSessionsSendSelfReply: (params: unknown) => runA2AFlowMock(params as Record<string, unknown>),
}));

import "./test-helpers/fast-openclaw-tools-sessions.js";
import { setActivePluginRegistry } from "../plugins/runtime.js";
import {
  getActiveGatewayRootWorkCount,
  resetGatewayWorkAdmission,
} from "../process/gateway-work-admission.js";
import { closeOpenClawStateDatabaseForTest } from "../state/openclaw-state-db.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../test-utils/openclaw-test-state.js";
import { createSessionConversationTestRegistry } from "../test-utils/session-conversation-registry.js";
import { testing as agentStepTesting } from "./tools/agent-step.test-support.js";
import { createSessionsSendTool } from "./tools/sessions-send-tool.js";

type GatewayCall = { method?: string; params?: Record<string, unknown> };
type SendDetails = {
  status?: string;
  targetDisposition?: string;
  reply?: string;
  delivery?: { status?: string; mode?: string };
};

const PEER_KEY = "agent:peer:main";
const REQUESTER_KEY = "agent:main:main";

function sendTool(agentSessionKey: string) {
  return createSessionsSendTool({ agentSessionKey, config, callGateway: callGatewayMock });
}

function details(result: { details?: unknown }): SendDetails {
  return result.details as SendDetails;
}

async function writeEntry(sessionKey: string, entry: SessionEntry) {
  const agentId = parseAgentSessionKey(sessionKey)?.agentId;
  if (!agentId) {
    throw new Error(`Expected an agent-scoped fixture key: ${sessionKey}`);
  }
  await replaceSessionEntry(
    {
      agentId,
      sessionKey,
      storePath: resolveSessionStorePathCore(config.session?.store, { agentId }),
    },
    entry,
  );
}

describe("sessions_send fire-and-forget", () => {
  let state: OpenClawTestState;
  let calls: GatewayCall[];
  let announceSteps: number;

  beforeEach(async () => {
    state = await createOpenClawTestState({ scenario: "minimal" });
    config.session = {
      mainKey: "main",
      scope: "per-sender",
      dmScope: "main",
      store: state.path("configured", "agents", "{agentId}", "sessions", "sessions.json"),
    };
    resetGatewayWorkAdmission();
    calls = [];
    announceSteps = 0;
    callGatewayMock.mockReset().mockImplementation(async (request: GatewayCall) => {
      calls.push(request);
      if (request.method === "agent") {
        return { runId: `run-${calls.length}`, status: "accepted", acceptedAt: calls.length };
      }
      if (request.method === "agent.wait") {
        return { status: "ok", terminalReply: { disposition: "visible", text: "target result" } };
      }
      return {};
    });
    runA2AFlowMock.mockClear();
    setActivePluginRegistry(createSessionConversationTestRegistry());
    await agentStepTesting.setDepsForTest({
      agentCommandFromIngress: async () => {
        announceSteps += 1;
        return { payloads: [{ text: "ANNOUNCE_SKIP", mediaUrl: null }], meta: { durationMs: 1 } };
      },
    });
    await writeEntry(PEER_KEY, { sessionId: "peer-session", updatedAt: 1 });
  });

  afterEach(async () => {
    await vi.waitFor(() => expect(getActiveGatewayRootWorkCount()).toBe(0));
    resetGatewayWorkAdmission();
    await agentStepTesting.setDepsForTest();
    closeOpenClawStateDatabaseForTest();
    await state.cleanup();
  });

  function agentCalls() {
    return calls.filter((call) => call.method === "agent");
  }

  it.each([
    { label: "peer target", targetEntry: undefined, requesterKey: REQUESTER_KEY },
    {
      label: "subagent target",
      targetEntry: { spawnedBy: REQUESTER_KEY, spawnDepth: 1 },
      requesterKey: REQUESTER_KEY,
    },
    {
      label: "isolated Cron requester",
      targetEntry: { spawnedBy: "agent:main:cron:job:run:once", spawnDepth: 1 },
      requesterKey: "agent:main:cron:job:run:once",
    },
  ])(
    "starts the target run and returns without registering the reply flow ($label)",
    async ({ targetEntry, requesterKey }) => {
      if (targetEntry) {
        await writeEntry(PEER_KEY, { sessionId: "peer-session", updatedAt: 1, ...targetEntry });
      }

      const result = await sendTool(requesterKey).execute("fire-and-forget", {
        sessionKey: PEER_KEY,
        message: "start the report",
        timeoutSeconds: 0,
      });

      expect(details(result)).toMatchObject({
        status: "accepted",
        targetDisposition: "queued",
        delivery: { status: "skipped", mode: "announce" },
      });
      // The flow is never registered, so no later scheduling decision can undo this.
      expect(runA2AFlowMock).not.toHaveBeenCalled();
      // The target run still starts exactly as a waited send starts it.
      expect(agentCalls()).toHaveLength(1);
      expect(agentCalls()[0]?.params).toMatchObject({
        sessionKey: PEER_KEY,
        agentId: "peer",
        deliver: false,
        sourceReplyDeliveryMode: "message_tool_only",
      });

      // Completing that run cannot wake the requester: nothing waits on it and
      // no announce step or channel send is scheduled.
      await new Promise((resolve) => {
        setTimeout(resolve, 25);
      });
      expect(calls.filter((call) => call.method === "agent.wait")).toHaveLength(0);
      expect(calls.filter((call) => call.method === "send")).toHaveLength(0);
      expect(agentCalls().some((call) => call.params?.sessionKey === requesterKey)).toBe(false);
      expect(announceSteps).toBe(0);
      expect(getActiveGatewayRootWorkCount()).toBe(0);
    },
  );

  it("lets the target report back with its own explicit send", async () => {
    await sendTool(REQUESTER_KEY).execute("fire-and-forget", {
      sessionKey: PEER_KEY,
      message: "start the report",
      timeoutSeconds: 0,
    });
    await writeEntry(REQUESTER_KEY, { sessionId: "requester-session", updatedAt: 1 });

    const callback = await sendTool(PEER_KEY).execute("explicit-callback", {
      sessionKey: REQUESTER_KEY,
      message: "report ready",
      timeoutSeconds: 0,
    });

    expect(details(callback).status).toBe("accepted");
    const requesterRun = agentCalls().find((call) => call.params?.sessionKey === REQUESTER_KEY);
    expect(requesterRun?.params?.message).toContain("report ready");
    expect(requesterRun?.params?.inputProvenance).toMatchObject({ sourceSessionKey: PEER_KEY });
  });

  it("returns a waited reply inline instead of announcing it back", async () => {
    const result = await sendTool(REQUESTER_KEY).execute("waited", {
      sessionKey: PEER_KEY,
      message: "start the report",
      timeoutSeconds: 1,
    });

    // The caller already holds the reply; replaying it would repeat the same
    // message into the requester and the target.
    expect(details(result)).toMatchObject({
      status: "ok",
      reply: "target result",
      delivery: { status: "skipped", mode: "announce" },
    });
    await new Promise((resolve) => {
      setTimeout(resolve, 25);
    });
    expect(runA2AFlowMock).not.toHaveBeenCalled();
    expect(announceSteps).toBe(0);
    expect(calls.filter((call) => call.method === "send")).toHaveLength(0);
    expect(agentCalls()).toHaveLength(1);
  });

  it("never announces for mode=steer or mode=followup", async () => {
    const steer = await sendTool(REQUESTER_KEY).execute("steer", {
      sessionKey: PEER_KEY,
      message: "adjust course",
      mode: "steer",
    });
    // Steering forces an effective timeout of 0 and never announces.
    expect(details(steer).status).toBe("error");

    const followup = await sendTool(REQUESTER_KEY).execute("followup", {
      sessionKey: PEER_KEY,
      message: "one more thing",
      mode: "followup",
      timeoutSeconds: 1,
    });
    expect(details(followup)).toMatchObject({
      status: "ok",
      delivery: { status: "skipped", mode: "announce" },
    });
    await new Promise((resolve) => {
      setTimeout(resolve, 25);
    });
    expect(runA2AFlowMock).not.toHaveBeenCalled();
    expect(announceSteps).toBe(0);
  });
});
