import { AsyncLocalStorage } from "node:async_hooks";
import { expect, it, vi, type Mock } from "vitest";
import { runQaGatewayFixture } from "../../test/helpers/qa-gateway-cleanup.js";
import { upsertSessionEntryCore } from "../config/sessions/session-accessor.sqlite-entry.js";
import * as gatewayWorkAdmission from "../process/gateway-work-admission.js";
import { runWithGatewayRootWorkAdmissionForTest } from "../process/gateway-work-admission.test-helpers.js";
import * as asyncWork from "../shared/async-work-scope.js";
import {
  createToolSearchCatalogRef,
  registerHeadlessToolSearchCatalog,
} from "./tool-search-catalog.js";
import { resolveToolSearchConfig } from "./tool-search-config.js";
import { ToolSearchRuntime } from "./tool-search-runtime.js";
import type { AnyAgentTool } from "./tools/common.js";

type SessionsSendTimeoutFixtures = {
  getSessionTool: (
    name: "sessions_send",
    options: { agentSessionKey: string; agentChannel: string },
  ) => AnyAgentTool;
  callGatewayMock: Mock;
};
type GatewayRequest = {
  method?: string;
  params?: {
    sessionKey?: string;
    runId?: string;
    extraSystemPrompt?: string;
    message?: string;
    inputProvenance?: { sourceSessionKey?: string; sourceRole?: string };
  };
};
const requesterKey = "agent:main:main";
const sendInput = (sessionKey: string) => ({ sessionKey, message: "ping", timeoutSeconds: 1 });
const accepted = (runId: string, acceptedAt: number) => ({ runId, status: "accepted", acceptedAt });
const sendTool = ({ getSessionTool }: SessionsSendTimeoutFixtures) =>
  getSessionTool("sessions_send", { agentSessionKey: requesterKey, agentChannel: "discord" });

export function observeSessionSendContinuations(options: { trackAllWork?: boolean } = {}) {
  const completions = new Set<Promise<unknown>>();
  const continuationWork = new AsyncLocalStorage<boolean>();
  const originalTrack = asyncWork.trackAsyncWork;
  const workSpy = vi.spyOn(asyncWork, "trackAsyncWork").mockImplementation(function observeWork<T>(
    run: () => T | Promise<T>,
  ): Promise<T> {
    const completion = originalTrack(run);
    if (options.trackAllWork || continuationWork.getStore()) {
      completions.add(completion);
    }
    return completion;
  });
  const original = gatewayWorkAdmission.runWithGatewayDetachedWorkContinuation;
  const spy = vi
    .spyOn(gatewayWorkAdmission, "runWithGatewayDetachedWorkContinuation")
    .mockImplementation(function observe<T>(run: () => Promise<T>, origin?: string): Promise<T> {
      const completion = original(
        origin === "session:self-reply" ? () => continuationWork.run(true, run) : run,
        origin,
      );
      if (options.trackAllWork || origin === "session:self-reply") {
        completions.add(completion);
      }
      return completion;
    });
  let joining: Promise<void> | undefined;

  return {
    settle(): Promise<void> {
      return (joining ??= (async () => {
        const failures: unknown[] = [];
        while (completions.size > 0) {
          const batch = [...completions];
          const results = await Promise.allSettled(batch);
          batch.forEach((completion) => completions.delete(completion));
          for (const result of results) {
            if (result.status === "rejected") {
              failures.push(result.reason);
            }
          }
        }
        if (failures.length === 1 && failures[0] instanceof Error) {
          throw failures[0];
        }
        if (failures.length > 0) {
          throw new AggregateError(failures, "sessions_send continuation cleanup failed");
        }
      })().finally(() => {
        joining = undefined;
      }));
    },
    restore() {
      spy.mockRestore();
      workSpy.mockRestore();
      continuationWork.disable();
    },
  };
}

export function registerSessionsSendTimeoutTests(fixtures: SessionsSendTimeoutFixtures) {
  const { callGatewayMock } = fixtures;
  it("sessions_send preserves a provider-attributed terminal timeout without a diagnostic through Tool Search without a reply-back", async () => {
    const calls: GatewayRequest[] = [];
    const targetKey = "agent:director1:main";
    await upsertSessionEntryCore(
      { agentId: "director1", sessionKey: targetKey },
      { sessionId: "timeout-target", updatedAt: 1 },
    );
    callGatewayMock.mockImplementation(async (request: GatewayRequest) => {
      calls.push(request);
      if (request.method === "agent") {
        return accepted("run-terminal", 2000);
      }
      if (request.method === "agent.wait") {
        return {
          runId: "run-terminal",
          status: "ok",
          endedAt: 3000,
          timeoutPhase: "provider",
          providerStarted: true,
        };
      }
      return {};
    });

    const catalogRef = createToolSearchCatalogRef();
    registerHeadlessToolSearchCatalog({ catalogRef, tools: [sendTool(fixtures)] });
    const runtime = new ToolSearchRuntime(
      { catalogRef },
      resolveToolSearchConfig({ tools: { toolSearch: { enabled: true, mode: "tools" } } }),
      { validateInput: true },
    );

    const details = await runtime.callValue("sessions_send", sendInput(targetKey));
    expect(details).toEqual({
      runId: "run-terminal",
      status: "timeout",
      error: "agent run timed out",
      sentBeforeError: true,
      sessionKey: targetKey,
    });
    expect(gatewayWorkAdmission.getActiveGatewayRootWorkCount()).toBe(0);
    expect(calls.filter((call) => call.method === "agent")).toHaveLength(1);
    expect(calls.filter((call) => call.method === "agent.wait")).toHaveLength(1);
  });
}

export function registerSessionsSendLateReplyTests(fixtures: SessionsSendTimeoutFixtures) {
  const { callGatewayMock, getSessionTool } = fixtures;
  it("lets the target report back to the requester with its own explicit send", async () => {
    const targetKey = "agent:director1:main";
    const calls: GatewayRequest[] = [];
    await upsertSessionEntryCore(
      { agentId: "director1", sessionKey: targetKey },
      { sessionId: "director-session", updatedAt: 1 },
    );
    callGatewayMock.mockImplementation(async (request: GatewayRequest) => {
      calls.push(request);
      return request.method === "agent" ? accepted(`run-${calls.length}`, 2000) : {};
    });
    const send = (from: string, to: string, message: string) =>
      getSessionTool("sessions_send", { agentSessionKey: from, agentChannel: "discord" }).execute(
        "explicit",
        { sessionKey: to, message, timeoutSeconds: 0 },
      );
    for (const [from, to, message] of [
      [requesterKey, targetKey, "start the report"],
      [targetKey, requesterKey, "report ready"],
    ] as const) {
      expect((await send(from, to, message)).details).toMatchObject({
        status: "accepted",
        delivery: { status: "skipped" },
      });
    }
    // Each direction is one explicit turn; neither send starts another one.
    const agentCalls = calls.filter((call) => call.method === "agent").map((call) => call.params);
    expect(agentCalls.map((params) => params?.sessionKey)).toEqual([targetKey, requesterKey]);
    expect(agentCalls[1]?.message).toContain("report ready");
    expect(agentCalls[1]?.inputProvenance).toMatchObject({ sourceSessionKey: targetKey });
    expect(calls.some((call) => call.method === "agent.wait")).toBe(false);
    expect(gatewayWorkAdmission.getActiveGatewayRootWorkCount()).toBe(0);
  });

  it.each<{
    targetKind: string;
    targetKey: string;
    spawned: boolean;
    timeoutSeconds?: number;
    pendingError?: boolean;
    cronRequester?: boolean;
  }>([
    { targetKind: "peer", targetKey: "agent:director1:main", spawned: false },
    {
      targetKind: "nonblocking peer",
      targetKey: "agent:director1:main",
      spawned: false,
      timeoutSeconds: 0,
    },
    {
      targetKind: "retrying child",
      targetKey: "agent:director1:subagent:child",
      spawned: true,
      pendingError: true,
    },
    { targetKind: "waited child", targetKey: "agent:director1:subagent:child", spawned: true },
    {
      targetKind: "nonblocking child of Cron",
      targetKey: "agent:director1:dashboard:child",
      spawned: true,
      cronRequester: true,
      timeoutSeconds: 0,
    },
  ])(
    "sessions_send leaves the late reply from a $targetKind in its session without waking the caller",
    async ({ targetKey, spawned, timeoutSeconds = 1, pendingError, cronRequester = false }) => {
      const calls: GatewayRequest[] = [];
      const requester = cronRequester ? "agent:main:cron:job:run:once" : requesterKey;
      if (cronRequester) {
        await upsertSessionEntryCore(
          { agentId: "main", sessionKey: requester },
          { sessionId: "cron-run-session", updatedAt: 1 },
        );
      }
      await upsertSessionEntryCore(
        { agentId: "director1", sessionKey: targetKey },
        {
          sessionId: "child-session",
          updatedAt: 1,
          ...(spawned ? { spawnedBy: requester, spawnDepth: 1 } : {}),
        },
      );
      callGatewayMock.mockImplementation(async (request: GatewayRequest) => {
        calls.push(request);
        if (request.method === "agent") {
          return accepted("run-target", 2000);
        }
        if (request.method === "agent.wait") {
          // The caller's own wait expires before the target finishes.
          return {
            runId: "run-target",
            status: "timeout",
            ...(pendingError ? { pendingError: true, error: "retrying provider" } : {}),
          };
        }
        return {};
      });
      const tool = getSessionTool("sessions_send", {
        agentSessionKey: requester,
        agentChannel: "discord",
      });

      await runQaGatewayFixture(async () => {
        const parentWork = new asyncWork.AsyncWorkScope();
        const result = await runWithGatewayRootWorkAdmissionForTest(async () => {
          try {
            return await parentWork.track(() =>
              tool.execute("call-delayed", {
                sessionKey: targetKey,
                message: "ping",
                timeoutSeconds,
              }),
            );
          } finally {
            await asyncWork.AsyncWorkScope.runWhenAllIdle(
              () => [parentWork],
              () => parentWork.drain(),
            );
          }
        });
        expect(result.details).toMatchObject({
          status: pendingError ? "timeout" : "accepted",
          sessionKey: targetKey,
          ...(!pendingError ? { targetDisposition: "queued" } : {}),
          delivery: { status: "skipped" },
        });
        // Nothing keeps observing the accepted run once the tool returns, so the
        // target's late reply has no path back into the caller or a channel.
        expect(gatewayWorkAdmission.getActiveGatewayRootWorkCount()).toBe(0);
        expect(calls.filter((call) => call.method === "agent.wait")).toHaveLength(
          timeoutSeconds === 0 ? 0 : 1,
        );
        expect(
          calls.filter((call) => call.method === "agent").map((call) => call.params?.sessionKey),
        ).toEqual([targetKey]);
        expect(calls.find((call) => call.method === "send")).toBeUndefined();
      });
    },
  );
}
