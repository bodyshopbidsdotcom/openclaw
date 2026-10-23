import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { buildAcpDatabaseSessionKey } from "../acp/runtime/session-meta-keys.js";
import type { OpenClawConfig } from "../config/config.js";
import { resolveSessionStorePathCore } from "../config/sessions/paths.js";
import {
  listSessionParticipantsReadOnly,
  loadSessionEntryReadOnly,
  replaceSessionEntry,
} from "../config/sessions/session-accessor.js";
import {
  resolveSqliteScope,
  toDatabaseOptions,
} from "../config/sessions/session-accessor.sqlite-scope.js";
import type { SessionEntry } from "../config/sessions/types.js";
import { resolveGatewaySessionStoreTargetWithStore } from "../gateway/session-utils-store-lookup.js";
import { parseAgentSessionKey } from "../routing/session-key.js";

const { config, callGatewayMock, readAcpSessionMetaMock, readAcpSessionMetaForEntryMock } =
  vi.hoisted(() => ({
    config: {
      session: { mainKey: "main", scope: "per-sender" },
      agents: {
        entries: { main: {}, peer: {} },
      },
      tools: { sessions: { visibility: "all" }, agentToAgent: { enabled: true } },
    } as OpenClawConfig,
    callGatewayMock: vi.fn(),
    readAcpSessionMetaMock: vi.fn(),
    readAcpSessionMetaForEntryMock: vi.fn(),
  }));
// mock-isolation: Keep ACP runtime access mocked while coordinating fixture session rows.
vi.mock("../acp/runtime/session-meta.js", () => ({
  readAcpSessionEntryAsync: async (params: {
    cfg?: OpenClawConfig;
    sessionKey: string;
    agentId?: string;
    assertCurrent?: () => void;
  }) => {
    params.assertCurrent?.();
    const cfg = params.cfg ?? config;
    const target = resolveGatewaySessionStoreTargetWithStore({
      cfg,
      key: params.sessionKey,
      agentId: params.agentId,
      readOnly: true,
      exactRead: true,
    });
    const entry = target.store[target.canonicalKey];
    const acp = await readAcpSessionMetaForEntryMock({
      ...params,
      cfg,
      sessionKey: target.canonicalKey,
      agentId: target.agentId,
      entry,
    });
    params.assertCurrent?.();
    return {
      cfg,
      agentId: target.agentId,
      storePath: target.storePath,
      sessionKey: params.sessionKey,
      storeSessionKey: target.canonicalKey,
      entry,
      acp,
    };
  },
}));
vi.mock("../acp/runtime/session-meta-readonly.js", async () => {
  const { rowToAcpSessionMeta } = await vi.importActual<
    typeof import("../acp/runtime/session-meta-readonly.js")
  >("../acp/runtime/session-meta-readonly.js");
  return {
    rowToAcpSessionMeta,
    readAcpSessionMetaForEntry: (params: unknown) => readAcpSessionMetaForEntryMock(params),
  };
});
vi.mock("../gateway/call.js", () => ({ callGateway: (opts: unknown) => callGatewayMock(opts) }));
vi.mock("../commands/agent.js", () => ({ agentCommandFromIngress: vi.fn() }));
vi.mock("../config/config.js", () => ({
  getRuntimeConfig: () => config,
  resolveGatewayPort: () => 18789,
}));

import "./test-helpers/fast-openclaw-tools-sessions.js";
import { setActivePluginRegistry } from "../plugins/runtime.js";
import {
  getActiveGatewayRootWorkCount,
  getActiveGatewayRootWorkHolders,
  resetGatewayWorkAdmission,
} from "../process/gateway-work-admission.js";
import { openOpenClawAgentDatabase } from "../state/openclaw-agent-db.js";
import { runOpenClawAgentWriteAdmission } from "../state/openclaw-agent-write-admission.js";
import { closeOpenClawStateDatabaseForTest } from "../state/openclaw-state-db.js";
import {
  createOpenClawTestState,
  type OpenClawTestState,
} from "../test-utils/openclaw-test-state.js";
import { createSessionConversationTestRegistry } from "../test-utils/session-conversation-registry.js";
import { clearActiveEmbeddedRun, setActiveEmbeddedRun } from "./embedded-agent-runner/runs.js";
import { createEmbeddedRunHandle } from "./embedded-agent-runner/runs.test-support.js";
import { createRequesterYieldCallback } from "./openclaw-tools.requester-yield.js";
import {
  registerSessionsSendRequesterRetirementTests,
  type GatewayCall,
} from "./openclaw-tools.sessions-send-requester-retirement.test-support.js";
import { mutateSubagentRuns } from "./subagents/registry/subagent-registry-persistence.js";
import { observeRootWork } from "./subagents/registry/subagent-registry.browser-cleanup.test-support.js";
import {
  addSubagentRunForTests,
  getSubagentRunByRunId,
  resetSubagentRegistryForTests,
} from "./subagents/registry/subagent-registry.test-helpers.js";
import * as sendDelivery from "./tools/sessions-send-tool.delivery.js";
import { createSessionsSendTool } from "./tools/sessions-send-tool.js";
import { createSessionsYieldTool } from "./tools/sessions-yield-tool.js";

const tempDirs = useAutoCleanupTempDirTracker(afterEach);
let settleRootWork: ReturnType<typeof observeRootWork>;

async function settleSessionWork() {
  await settleRootWork(true);
  expect(getActiveGatewayRootWorkCount(), getActiveGatewayRootWorkHolders().join(", ")).toBe(0);
}

type AgentCallParams = {
  extraSystemPrompt?: string;
  inputProvenance?: { sourceSessionKey?: string; sourceRole?: string };
};
const calls: GatewayCall[] = [];
function mockGatewayReply(
  waitResult: Record<string, unknown> = {
    status: "ok",
    terminalReply: { disposition: "visible", text: "Requested result" },
  },
) {
  callGatewayMock.mockImplementation(async (request: GatewayCall) => {
    calls.push(request);
    if (request.method === "agent") {
      return { runId: "coordination-run", status: "accepted" };
    }
    return request.method === "agent.wait" ? waitResult : {};
  });
}
function agentParams(call: GatewayCall): AgentCallParams {
  return (call.params ?? {}) as AgentCallParams;
}
function send(requesterKey: string, targetKey: string, timeoutSeconds = 1) {
  return createSessionsSendTool({
    agentSessionKey: requesterKey,
    config,
    callGateway: callGatewayMock,
  }).execute("coordination", {
    sessionKey: targetKey,
    message: "Return the requested result",
    timeoutSeconds,
  });
}
function expectCoordination(
  result: Awaited<ReturnType<typeof send>>,
  requesterChild: boolean,
  nonblocking = false,
) {
  // Children and peers alike answer inline or keep the answer in their own
  // session; neither side gets a reply-back turn.
  expect.soft(result.details).toMatchObject(
    nonblocking
      ? { status: "accepted", delivery: { status: "skipped" } }
      : {
          status: "ok",
          reply: "Requested result",
          delivery: { status: "skipped" },
        },
  );
  const agentCalls = calls.filter((call) => call.method === "agent");
  expect.soft(agentCalls).toHaveLength(1);
  expect.soft(calls.some((call) => call.method === "send")).toBe(false);
  if (nonblocking) {
    expect.soft(calls.some((call) => call.method === "agent.wait")).toBe(false);
  }
  expect
    .soft(agentParams(agentCalls[0] ?? {}).inputProvenance?.sourceRole)
    .toBe(requesterChild ? "subagent" : undefined);
  return agentCalls;
}
async function writeEntry(sessionKey: string, entry: SessionEntry, storePath?: string) {
  const agentId = parseAgentSessionKey(sessionKey)?.agentId;
  if (!agentId) {
    throw new Error(`Expected an agent-scoped fixture key: ${sessionKey}`);
  }
  await replaceSessionEntry(
    {
      agentId,
      sessionKey,
      storePath: storePath ?? resolveSessionStorePathCore(config.session?.store, { agentId }),
    },
    entry,
  );
}

describe("sessions_send child coordination", () => {
  let state: OpenClawTestState;
  beforeEach(async () => {
    settleRootWork = observeRootWork();
    state = await createOpenClawTestState({ scenario: "minimal" });
    config.session = {
      mainKey: "main",
      scope: "per-sender",
      dmScope: "main",
      store: state.path("configured", "agents", "{agentId}", "sessions", "sessions.json"),
    };
    resetGatewayWorkAdmission();
    callGatewayMock.mockReset();
    calls.length = 0;
    mockGatewayReply();
    readAcpSessionMetaMock.mockReset().mockReturnValue(undefined);
    readAcpSessionMetaForEntryMock
      .mockReset()
      .mockImplementation((params: unknown) => readAcpSessionMetaMock(params));
    setActivePluginRegistry(createSessionConversationTestRegistry());
  });
  afterEach(async () => {
    await settleSessionWork();
    resetGatewayWorkAdmission();
    closeOpenClawStateDatabaseForTest();
    await state.cleanup();
  });

  it.each(["before admission", "before receipt", "after completion"] as const)(
    "settles watched steering against its admitted run (%s)",
    async (timing) => {
      const requesterSessionKey = "agent:main:dashboard:steer-requester";
      const childSessionKey = "agent:main:dashboard:steer-child";
      const sessionId = "steer-child";
      const requesterTurnRunId = "steer-requester-turn";
      await writeEntry(requesterSessionKey, { sessionId: "steer-requester", updatedAt: 1 });
      await writeEntry(childSessionKey, {
        sessionId,
        updatedAt: 1,
        spawnedBy: requesterSessionKey,
        spawnDepth: 1,
      });
      await resetSubagentRegistryForTests();
      const seed = (runId: string, generation: number) =>
        addSubagentRunForTests({
          runId,
          generation,
          childSessionKey,
          requesterSessionKey,
          requesterAgentId: "main",
          expectsCompletionMessage: true,
          createdAt: Date.now(),
          execution: { status: "running", startedAt: Date.now() },
          completion: { required: true },
          delivery: { status: "pending" },
        });
      await seed("steer-A", 1);
      const previous = () => getSubagentRunByRunId("steer-A")!;
      const queueB = vi.fn(async () => {});
      const handleB = createEmbeddedRunHandle({ runId: "steer-B", queueMessage: queueB });
      const completePrevious = async (delivered = false) => {
        await mutateSubagentRuns(["steer-A"], (rows) => {
          const current = rows.get("steer-A");
          if (!current) {
            throw new Error("Expected the admitted steering target");
          }
          return {
            value: undefined,
            postimages: new Map([
              [
                current.runId,
                {
                  ...current,
                  execution: {
                    status: "terminal" as const,
                    endedAt: Date.now(),
                    outcome: { status: "ok" as const },
                  },
                  ...(delivered
                    ? {
                        delivery: { status: "delivered" as const },
                        cleanupCompletedAt: Date.now(),
                      }
                    : {}),
                },
              ],
            ]),
          };
        });
      };
      const replace = async () => {
        await completePrevious();
        await seed("steer-B", 2);
        setActiveEmbeddedRun(sessionId, handleB, childSessionKey);
      };
      const queueA = vi.fn(async () => {
        if (timing === "after completion") {
          await completePrevious(true);
        } else {
          await replace();
        }
      });
      const handleA = createEmbeddedRunHandle({ runId: "steer-A", queueMessage: queueA });
      setActiveEmbeddedRun(sessionId, handleA, childSessionKey);
      const deliver = sendDelivery.trySessionsSendActiveRunDelivery;
      const admission = vi
        .spyOn(sendDelivery, "trySessionsSendActiveRunDelivery")
        .mockImplementationOnce(async (...args) => {
          if (timing === "before admission") {
            await replace();
          }
          return deliver(...args);
        });
      try {
        const sent = await createSessionsSendTool({
          agentSessionKey: requesterSessionKey,
          requesterTurnRunId,
          config,
          callGateway: callGatewayMock,
        }).execute("watched-steer", {
          sessionKey: childSessionKey,
          message: "Continue this task",
          mode: "steer",
          watch: true,
        });
        if (timing !== "before admission") {
          expect(sent.details).toMatchObject({
            status: "error",
            sentBeforeError: true,
            error: expect.stringContaining("completion could not be claimed"),
          });
          expect(queueA).toHaveBeenCalledOnce();
          expect(previous().requesterTurnRunId).toBeUndefined();
          expect(queueB).not.toHaveBeenCalled();
          expect(getSubagentRunByRunId("steer-B")?.requesterTurnRunId).toBeUndefined();
          expect(previous().delivery?.status).toBe(
            timing === "after completion" ? "delivered" : "pending",
          );
          return;
        }
        // The claimed completion is the child's existing report, not a sessions_send reply.
        expect(sent.details).toMatchObject({
          status: "accepted",
          targetDisposition: "steered",
          delivery: { status: "skipped" },
        });
        expect(queueB).toHaveBeenCalledOnce();
        expect(queueA).not.toHaveBeenCalled();
        expect(getSubagentRunByRunId("steer-B")?.requesterTurnRunId).toBe(requesterTurnRunId);
        expect(previous().requesterTurnRunId).toBeUndefined();
        const yielded = await createSessionsYieldTool({
          sessionId: "steer-requester",
          onYield: () => {},
          claimYield: createRequesterYieldCallback({
            requesterSessionKey,
            requesterAgentId: "main",
            requesterTurnRunId,
          }),
        }).execute("yield-steered", {});
        expect(yielded.details).toEqual({ status: "yielded" });
      } finally {
        admission.mockRestore();
        clearActiveEmbeddedRun(sessionId, handleB, childSessionKey);
        clearActiveEmbeddedRun(sessionId, handleA, childSessionKey);
        await resetSubagentRegistryForTests();
      }
    },
  );

  it.each([undefined, true] as const)(
    "leaves a queued child follow-up result in the child session (watch=%s)",
    async (watch) => {
      const requesterSessionKey = "agent:main:dashboard:requester";
      const childSessionKey = "agent:main:dashboard:existing-child";
      const runId = "coordination-followup";
      await writeEntry(requesterSessionKey, { sessionId: "requester", updatedAt: 1 });
      await writeEntry(childSessionKey, {
        sessionId: "existing-child",
        updatedAt: 1,
        spawnedBy: requesterSessionKey,
        spawnDepth: 1,
      });
      await resetSubagentRegistryForTests();
      callGatewayMock.mockImplementation(async (request: GatewayCall) => {
        calls.push(request);
        return request.method === "agent"
          ? { runId, status: "accepted", targetDisposition: "queued" }
          : {};
      });
      try {
        const result = await createSessionsSendTool({
          agentSessionKey: requesterSessionKey,
          requesterTurnRunId: "followup-requester-turn",
          config,
          callGateway: callGatewayMock,
        }).execute("followup", {
          sessionKey: childSessionKey,
          mode: "followup",
          ...(watch === undefined ? {} : { watch }),
          timeoutSeconds: 0,
          message: "Return the follow-up result",
        });
        expect(result.details).toMatchObject({
          status: "accepted",
          runId,
          targetDisposition: "queued",
          delivery: { status: "skipped" },
          ...(watch ? { watched: true } : {}),
        });
        await settleSessionWork();
        // No completion claim exists for the follow-up, so nothing reports it back.
        expect(getSubagentRunByRunId(runId)).toBeUndefined();
        expect(calls.filter((call) => call.method === "agent")).toEqual([
          expect.objectContaining({
            params: expect.objectContaining({ sessionKey: childSessionKey }),
          }),
        ]);
        expect(calls.some((call) => call.method === "agent.wait")).toBe(false);
      } finally {
        await resetSubagentRegistryForTests();
      }
    },
  );

  registerSessionsSendRequesterRetirementTests({
    config,
    callGatewayMock,
    calls,
    writeEntry,
    settleSessionWork,
  });

  it.each([
    { direction: "requester", child: true },
    { direction: "target", child: false },
  ])(
    "uses the registered alternate store for $direction with child=$child",
    async ({ direction, child }) => {
      const peerKey = "agent:peer:main";
      const alternateKey = child ? "agent:main:dashboard:alternate" : "agent:main:subagent:root";
      const alternate = openOpenClawAgentDatabase({ agentId: "main" });
      const configuredStorePath = resolveSessionStorePathCore(config.session?.store, {
        agentId: "main",
      });
      const alternateScope = {
        agentId: "main",
        sessionKey: alternateKey,
        storePath: alternate.path,
      };
      await writeEntry(peerKey, { sessionId: "peer-session", updatedAt: 1 });
      await writeEntry(
        alternateKey,
        {
          sessionId: "alternate-session",
          updatedAt: 1,
          parentSessionKey: peerKey,
          ...(child ? { spawnedBy: peerKey, spawnDepth: 1 } : { spawnDepth: 0 }),
        },
        alternate.path,
      );
      expect(
        loadSessionEntryReadOnly({ ...alternateScope, storePath: configuredStorePath }),
      ).toBeUndefined();
      const requesterKey = direction === "requester" ? alternateKey : peerKey;
      const targetKey = direction === "target" ? alternateKey : peerKey;
      const result = await send(requesterKey, targetKey, direction === "target" ? 0 : 1);
      await settleSessionWork();
      const agentCalls = expectCoordination(
        result,
        direction === "requester" && child,
        direction === "target",
      );
      if (child) {
        expect(agentParams(agentCalls[0] ?? {}).extraSystemPrompt).toBeUndefined();
      }
      if (direction === "target") {
        await runOpenClawAgentWriteAdmission(
          toDatabaseOptions(resolveSqliteScope(alternateScope)),
          () => undefined,
        );
        expect
          .soft(listSessionParticipantsReadOnly(alternateScope).get(alternateKey) ?? [])
          .toEqual([
            expect.objectContaining({
              identity: { type: "agent", id: "peer" },
              contributionCount: 1,
            }),
          ]);
        expect
          .soft(
            listSessionParticipantsReadOnly({
              ...alternateScope,
              storePath: configuredStorePath,
            }).get(alternateKey) ?? [],
          )
          .toEqual([]);
      }
    },
  );

  it.each(["requester", "target"])(
    "binds %s ACP coordination to its loaded lifecycle entry",
    async (direction) => {
      const expectedChild = direction === "target";
      const lifecycleRevision = expectedChild ? "current" : "retired";
      const sessionId = "same-session";
      const sessionStartedAt = 50;
      const metadata = await vi.importActual<typeof import("../acp/runtime/session-meta.js")>(
        "../acp/runtime/session-meta.js",
      );
      const metadataRead = await vi.importActual<
        typeof import("../acp/runtime/session-meta-readonly.js")
      >("../acp/runtime/session-meta-readonly.js");
      const databasePath = path.join(
        tempDirs.make("sessions-send-acp-binding-"),
        "openclaw.sqlite",
      );
      const peerKey = "agent:main:main";
      const reusedKey = "agent:main:dashboard:reused-acp";
      const requesterKey = direction === "requester" ? reusedKey : peerKey;
      const targetKey = direction === "target" ? reusedKey : peerKey;
      const currentEntry: SessionEntry = {
        sessionId: "same-session",
        lifecycleRevision: "current",
        sessionStartedAt,
        updatedAt: 200,
        parentSessionKey: peerKey,
      };
      metadata.writeAcpSessionMetaForMigration({
        databasePath,
        sessionKey: buildAcpDatabaseSessionKey(reusedKey, parseAgentSessionKey(reusedKey)?.agentId),
        sessionId,
        lifecycleRevision,
        now: () => 100,
        meta: {
          backend: "acpx",
          agent: "main",
          runtimeSessionName: "previous-acp",
          mode: "persistent",
          state: "idle",
          lastActivityAt: 100,
        },
      });
      // The separate lookup can observe another entry snapshot; classification must
      // join its metadata against the entry already selected by sessions_send.
      readAcpSessionMetaMock.mockImplementation(
        (params: Parameters<typeof metadata.readAcpSessionEntry>[0]) =>
          metadataRead.readAcpSessionMetaForEntry({
            ...params,
            databasePath,
            entry: { sessionId, lifecycleRevision, sessionStartedAt: 50 },
          }),
      );
      readAcpSessionMetaForEntryMock.mockImplementation(
        (params: Parameters<typeof metadataRead.readAcpSessionMetaForEntry>[0]) =>
          metadataRead.readAcpSessionMetaForEntry({ ...params, databasePath }),
      );
      await writeEntry(reusedKey, currentEntry);
      const result = await send(requesterKey, targetKey, direction === "target" ? 0 : 1);
      await settleSessionWork();
      if (direction === "target") {
        expect(result.details).toMatchObject({
          status: "accepted",
          delivery: { status: "skipped" },
        });
        const agentCalls = calls.filter((call) => call.method === "agent");
        expect(agentCalls).toEqual([
          expect.objectContaining({ params: expect.objectContaining({ sessionKey: targetKey }) }),
        ]);
        expect(agentParams(agentCalls[0] ?? {}).inputProvenance?.sourceRole).toBeUndefined();
        expect(
          calls.filter((call) => call.method === "agent.wait" || call.method === "send"),
        ).toHaveLength(0);
      } else {
        expectCoordination(result, expectedChild);
      }
    },
  );

  it.each([
    {
      name: "hidden child with opaque direct token under main DM scope",
      requesterKey: "agent:main:subagent:direct:peer-1",
      targetKey: "agent:peer:main",
      entry: {},
      timeoutSeconds: 0,
    },
    {
      name: "restored child with cyclic lineage",
      requesterKey: "agent:main:dashboard:cycle",
      entry: { spawnedBy: "agent:main:dashboard:cycle" },
      timeoutSeconds: 1,
    },
  ])(
    "sessions_send does not start reply turns for $name after timeoutSeconds=$timeoutSeconds",
    async ({ requesterKey, entry, timeoutSeconds, targetKey = "agent:main:main" }) => {
      await writeEntry(requesterKey, { sessionId: "child", updatedAt: 1, ...entry });
      mockGatewayReply({ status: "timeout" });
      const result = await send(requesterKey, targetKey, timeoutSeconds);
      expect(result.details).toMatchObject({
        status: "accepted",
        delivery: { status: "skipped" },
      });
      expect(getActiveGatewayRootWorkCount()).toBe(0);
      const agentCalls = calls.filter((call) => call.method === "agent");
      expect(agentCalls).toHaveLength(1);
      expect(agentCalls[0]?.params).toMatchObject({
        sessionKey: targetKey,
        inputProvenance: {
          kind: "inter_session",
          sourceSessionKey: requesterKey,
          sourceTool: "sessions_send",
        },
      });
      expect(agentParams(agentCalls[0] ?? {}).inputProvenance?.sourceRole).toBe("subagent");
      expect(calls.filter((call) => call.method === "agent.wait")).toHaveLength(
        timeoutSeconds === 0 ? 0 : 1,
      );
    },
  );
});
