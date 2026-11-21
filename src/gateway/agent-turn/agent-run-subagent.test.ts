import { afterEach, beforeEach, expect, it, vi } from "vitest";
import * as acpMetadata from "../../acp/runtime/session-meta-readonly.js";
import { bindInProcessSubagentResume } from "../in-process-subagent-resume.js";
import { prepareGatewaySubagentRun } from "./agent-run-subagent.js";
import type { AgentTurnPrincipal } from "./types.js";

const mocks = vi.hoisted(() => ({
  registerSubagentRun: vi.fn(),
  adoptPausedSubagentRunForFollowUp: vi.fn(),
  adoptPausedSubagentRunIntoSuccessor: vi.fn(),
  getLatestLiveSubagentRunByChildSessionKey: vi.fn(),
  prepareParentSubagentResume: vi.fn(),
}));
vi.mock("../../agents/subagents/registry/subagent-registry-read.js", () => ({
  getLatestLiveSubagentRunByChildSessionKey: mocks.getLatestLiveSubagentRunByChildSessionKey,
}));
vi.mock("../../agents/subagents/registry/subagent-registry.js", () => ({
  registerSubagentRun: mocks.registerSubagentRun,
  adoptPausedSubagentRunForFollowUp: mocks.adoptPausedSubagentRunForFollowUp,
  adoptPausedSubagentRunIntoSuccessor: mocks.adoptPausedSubagentRunIntoSuccessor,
}));
vi.mock("../../config/sessions.js", () => ({
  resolveAgentIdFromSessionKey: () => "main",
  resolveAgentMainSessionKey: () => "agent:main:main",
}));
vi.mock("../session-subagent-resume.js", () => ({
  prepareParentSubagentResume: mocks.prepareParentSubagentResume,
}));

const childSessionKey = "agent:main:subagent:child";
const runId = "child-run";
const native = { pluginSubagent: false, reactivateSubagent: false };
function expectUnused(...spies: { mock: { calls: unknown[][] } }[]) {
  expect(spies.map((spy) => spy.mock.calls)).toEqual(spies.map(() => []));
}
function pluginClient(): AgentTurnPrincipal {
  return {
    connect: {
      minProtocol: 1,
      maxProtocol: 1,
      client: { id: "gateway-client", version: "test", platform: "test", mode: "backend" },
    },
    internal: { agentRunTracking: "plugin_subagent", pluginRuntimeOwnerId: "example" },
  };
}
function prepare(overrides: Partial<Parameters<typeof prepareGatewaySubagentRun>[0]> = {}) {
  return prepareGatewaySubagentRun({
    cfg: {},
    activeSessionAgentId: "main",
    client: null,
    resolvedSessionKey: childSessionKey,
    request: { message: "Continue the child" },
    isOneShotModelRun: false,
    runId,
    lifecycleStorePath: "/fixture/agents/main/sessions/sessions.json",
    getAdmittedSessionId: () => "child-session",
    assertResumeAdmissionCurrent: vi.fn(),
    context: { logGateway: { warn: vi.fn() } },
    ...overrides,
  });
}

afterEach(() => vi.restoreAllMocks());

beforeEach(() => {
  vi.resetAllMocks();
  mocks.adoptPausedSubagentRunForFollowUp.mockResolvedValue(false);
  mocks.adoptPausedSubagentRunIntoSuccessor.mockResolvedValue(false);
});

it.each([true, false])(
  "registers plugin work only while admission is current: %s",
  async (current) => {
    const sessionEntry = {
      sessionId: "admitted-child",
      lifecycleRevision: "admitted",
      updatedAt: 1,
    };
    let admitted = true;
    const assertCurrent = () => {
      if (!admitted) {
        throw new Error("admission retired");
      }
      return sessionEntry;
    };
    const prepared = await prepare({
      client: pluginClient(),
      assertResumeAdmissionCurrent: assertCurrent,
    });
    expect(mocks.registerSubagentRun).not.toHaveBeenCalled();
    expect(prepared.registerPluginSubagent).toBeTypeOf("function");
    const preparation = prepared.registerPluginSubagent!();
    admitted = current;
    if (current) {
      await expect(preparation).resolves.toBeUndefined();
      expect(mocks.registerSubagentRun).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          runId,
          childSessionKey,
          sessionEntry,
          task: "Continue the child",
          requesterSessionKey: "agent:main:main",
        }),
        { assertCurrent },
      );
    } else {
      await expect(preparation).rejects.toThrow("admission retired");
      expectUnused(mocks.registerSubagentRun, mocks.adoptPausedSubagentRunForFollowUp);
    }
  },
);

it("preserves an explicit parent resume without creating a sibling owner", async () => {
  const client = pluginClient();
  const resume = {
    caller: { agentId: "main", sessionKey: "agent:main:main" },
    childSessionKey,
    childSessionId: "child-session",
    previousRunId: "previous-run",
    taskRunId: "canonical-run",
    generation: 1,
    createdAt: 1,
  };
  client.internal = bindInProcessSubagentResume({}, resume);
  const adoptParentResume = vi.fn(async () => "previous-run");
  mocks.prepareParentSubagentResume.mockResolvedValue(adoptParentResume);
  await expect(prepare({ client })).resolves.toEqual({ ...native, adoptParentResume });
  expectUnused(adoptParentResume, mocks.registerSubagentRun);
});

it("does not let inter-session delivery reactivate a completed child", async () => {
  const readMetadata = vi.spyOn(acpMetadata, "readAcpSessionMetaForEntries");
  const result = await prepare({
    inputProvenance: {
      kind: "inter_session",
      sourceSessionKey: "agent:main:main",
      sourceTool: "sessions_send",
    },
  });
  expect(result).toEqual(native);
  expect(mocks.registerSubagentRun).not.toHaveBeenCalled();
  await expect(prepare()).resolves.toEqual({ pluginSubagent: false, reactivateSubagent: true });
  expect(readMetadata).not.toHaveBeenCalled();
});
