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
      if (origin === "session:self-reply") {
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

export function registerSessionsSendTimeoutTests({
  getSessionTool,
  callGatewayMock,
}: SessionsSendTimeoutFixtures) {
  it.each([
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
    "sessions_send preserves a $name through Tool Search without a reply-back",
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
      const calls: Array<{ method?: string; params?: unknown }> = [];
      const requesterKey = cronRequester ? "agent:main:cron:job:run:once" : "agent:main:main";
      if (spawned) {
        await upsertSessionEntryCore(
          { agentId: "director1", sessionKey: targetKey },
          { sessionId: "child-session", updatedAt: 1, spawnedBy: requesterKey, spawnDepth: 1 },
        );
      }
      callGatewayMock.mockImplementation(async (opts: unknown) => {
        const request = opts as { method?: string; params?: unknown };
        calls.push(request);
        if (request.method === "agent") {
          return { runId: "run-target", status: "accepted", acceptedAt: 2000 };
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
        agentSessionKey: requesterKey,
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
          delivery: { status: "skipped", mode: "announce" },
        });
        // Nothing keeps observing the accepted run once the tool returns, so the
        // target's late reply has no path back into the caller or a channel.
        expect(gatewayWorkAdmission.getActiveGatewayRootWorkCount()).toBe(0);
        expect(calls.filter((call) => call.method === "agent.wait")).toHaveLength(
          timeoutSeconds === 0 ? 0 : 1,
        );
        expect(calls.filter((call) => call.method === "agent")).toEqual([
          expect.objectContaining({ params: expect.objectContaining({ sessionKey: targetKey }) }),
        ]);
        expect(calls.find((call) => call.method === "send")).toBeUndefined();
      });
    },
  );
}
