import { AsyncLocalStorage } from "node:async_hooks";
import { expect, it, vi, type Mock } from "vitest";
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
import { testing as agentStepTesting } from "./tools/agent-step.test-support.js";
import type { AnyAgentTool } from "./tools/common.js";

type SessionsSendTimeoutFixtures = {
  getSessionTool: (
    name: "sessions_send",
    options: { agentSessionKey: string; agentChannel: string },
  ) => AnyAgentTool;
  callGatewayMock: Mock;
};

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
        origin === "session:a2a-send" ? () => continuationWork.run(true, run) : run,
        origin,
      );
      if (origin === "session:a2a-send") {
        completions.add(completion);
      }
      return completion;
    });
  let joining: Promise<void> | undefined;

  return {
    settle(): Promise<void> {
      if (joining) {
        return joining;
      }
      joining = (async () => {
        const failures: unknown[] = [];
        while (completions.size > 0) {
          const batch = [...completions];
          const results = await Promise.allSettled(batch);
          for (const completion of batch) {
            completions.delete(completion);
          }
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
      });
      return joining;
    },
    restore() {
      spy.mockRestore();
      workSpy.mockRestore();
      continuationWork.disable();
    },
  };
}

export function registerSessionsSendPendingErrorTest({
  getSessionTool,
  callGatewayMock,
}: SessionsSendTimeoutFixtures) {
  it("sessions_send returns pending agent error diagnostics on timeout", async () => {
    const calls: Array<{ method?: string; params?: unknown }> = [];
    callGatewayMock.mockImplementation(async (opts: unknown) => {
      const request = opts as { method?: string; params?: unknown };
      calls.push(request);
      if (request.method === "agent") {
        return {
          runId: "run-pending-model-error",
          status: "accepted",
          acceptedAt: 1234,
        };
      }
      if (request.method === "agent.wait") {
        return {
          runId: "run-pending-model-error",
          status: "timeout",
          error: "429 RESOURCE_EXHAUSTED",
          pendingError: true,
        };
      }
      return {};
    });

    const tool = getSessionTool("sessions_send", {
      agentSessionKey: "discord:group:req",
      agentChannel: "discord",
    });
    const result = await tool.execute("call-pending-error", {
      sessionKey: "main",
      message: "check status",
      timeoutSeconds: 1,
    });
    // The retrying run keeps going in its own session; no announcement follows it.
    expect(result.details).toMatchObject({
      status: "timeout",
      error: "429 RESOURCE_EXHAUSTED",
      runId: "run-pending-model-error",
      sentBeforeError: true,
      delivery: { status: "skipped" },
    });
    expect(gatewayWorkAdmission.getActiveGatewayRootWorkCount()).toBe(0);
    expect(calls.filter((call) => call.method === "agent")).toHaveLength(1);
    expect(calls.filter((call) => call.method === "agent.wait")).toHaveLength(1);
  });
}

export function registerSessionsSendTimeoutTests({
  getSessionTool,
  callGatewayMock,
}: SessionsSendTimeoutFixtures) {
  it.each([
    {
      name: "terminal timeout with an explicit diagnostic",
      waitResult: {
        status: "timeout",
        endedAt: 3000,
        stopReason: "timeout",
        error: "agent run timed out",
      },
      expectedError: "agent run timed out",
    },
    {
      name: "terminal timeout with a provider-specific diagnostic",
      waitResult: {
        status: "timeout",
        endedAt: 3000,
        stopReason: "timeout",
        error: "provider request exceeded its deadline",
      },
      expectedError: "provider request exceeded its deadline",
    },
    {
      name: "provider-attributed terminal timeout without a diagnostic",
      waitResult: {
        status: "ok",
        endedAt: 3000,
        timeoutPhase: "provider",
        providerStarted: true,
      },
      expectedError: "agent run timed out",
    },
  ] as const)(
    "sessions_send preserves a $name through Tool Search without starting A2A",
    async ({ waitResult, expectedError }) => {
      const calls: Array<{ method?: string; params?: unknown }> = [];
      const requesterKey = "agent:main:main";
      const targetKey = "agent:director1:main";
      callGatewayMock.mockImplementation(async (opts: unknown) => {
        const request = opts as { method?: string; params?: unknown };
        calls.push(request);
        if (request.method === "agent") {
          return { runId: "run-terminal", status: "accepted", acceptedAt: 2000 };
        }
        if (request.method === "agent.wait") {
          return { runId: "run-terminal", ...waitResult };
        }
        return {};
      });

      const tool = getSessionTool("sessions_send", {
        agentSessionKey: requesterKey,
        agentChannel: "discord",
      });
      const catalogRef = createToolSearchCatalogRef();
      registerHeadlessToolSearchCatalog({ catalogRef, tools: [tool] });
      const runtime = new ToolSearchRuntime(
        { catalogRef },
        resolveToolSearchConfig({ tools: { toolSearch: { enabled: true, mode: "tools" } } }),
        { validateInput: true },
      );

      const details = await runtime.callValue("sessions_send", {
        sessionKey: targetKey,
        message: "ping",
        timeoutSeconds: 1,
      });
      expect(details).toEqual({
        runId: "run-terminal",
        status: "timeout",
        error: expectedError,
        sentBeforeError: true,
        sessionKey: targetKey,
      });
      expect(gatewayWorkAdmission.getActiveGatewayRootWorkCount()).toBe(0);
      expect(calls.filter((call) => call.method === "agent")).toHaveLength(1);
      expect(calls.filter((call) => call.method === "agent.wait")).toHaveLength(1);
    },
  );
}

export function registerSessionsSendLateReplyTests({
  getSessionTool,
  callGatewayMock,
}: SessionsSendTimeoutFixtures) {
  it.each<{
    targetKind: string;
    targetKey: string;
    spawned: boolean;
    pendingError?: boolean;
    cronRequester?: boolean;
  }>([
    { targetKind: "peer", targetKey: "agent:director1:main", spawned: false },
    { targetKind: "visible child", targetKey: "agent:director1:dashboard:child", spawned: true },
    { targetKind: "hidden child", targetKey: "agent:director1:subagent:child", spawned: true },
    {
      targetKind: "retrying child",
      targetKey: "agent:director1:subagent:child",
      spawned: true,
      pendingError: true,
    },
    {
      targetKind: "child of Cron",
      targetKey: "agent:director1:subagent:child",
      spawned: true,
      cronRequester: true,
    },
  ])(
    "sessions_send leaves a late reply from a $targetKind with its own session",
    async ({ targetKey, spawned, pendingError, cronRequester = false }) => {
      const calls: Array<{ method?: string; params?: unknown }> = [];
      const requesterKey = cronRequester ? "agent:main:cron:job:run:once" : "agent:main:main";
      if (spawned) {
        await upsertSessionEntryCore(
          { agentId: "director1", sessionKey: targetKey },
          { sessionId: "child-session", updatedAt: 1, spawnedBy: requesterKey, spawnDepth: 1 },
        );
      }
      let targetWaitCount = 0;
      let announceProviderStarts = 0;
      callGatewayMock.mockImplementation(async (opts: unknown) => {
        const request = opts as { method?: string; params?: unknown };
        calls.push(request);
        if (request.method === "agent") {
          return { runId: "run-target", status: "accepted", acceptedAt: 2000 };
        }
        if (request.method === "agent.wait") {
          targetWaitCount += 1;
          return {
            runId: "run-target",
            status: "timeout",
            ...(pendingError ? { pendingError: true, error: "retrying provider" } : {}),
          };
        }
        return {};
      });
      await agentStepTesting.setDepsForTest({
        agentCommandFromIngress: async () => {
          announceProviderStarts += 1;
          return {
            payloads: [{ text: "ANNOUNCE_SKIP", mediaUrl: null }],
            meta: { durationMs: 1 },
          };
        },
      });

      const tool = getSessionTool("sessions_send", {
        agentSessionKey: requesterKey,
        agentChannel: "discord",
      });

      const result = await runWithGatewayRootWorkAdmissionForTest(() =>
        tool.execute("call-delayed", {
          sessionKey: targetKey,
          message: "ping",
          timeoutSeconds: 1,
        }),
      );
      expect(result.details).toMatchObject({
        status: pendingError ? "timeout" : "accepted",
        sessionKey: targetKey,
        ...(!pendingError ? { targetDisposition: "queued" } : {}),
        delivery: { status: "skipped", mode: "announce" },
      });
      // A waited send owns no detached reply work: once participant recording
      // settles, the caller's root is free and the late reply stays with the target.
      await vi.waitFor(() => {
        expect(gatewayWorkAdmission.getActiveGatewayRootWorkCount()).toBe(0);
      });
      expect(targetWaitCount).toBe(1);
      expect(announceProviderStarts).toBe(0);
      expect(calls.filter((call) => call.method === "agent")).toHaveLength(1);
      expect(calls.find((call) => call.method === "send")).toBeUndefined();
    },
  );
}
