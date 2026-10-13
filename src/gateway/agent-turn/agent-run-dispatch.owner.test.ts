import { beforeEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import type { AgentCommandDeliveryResult } from "../../agents/command/delivery-result.js";
import type { AgentCommandOpts } from "../../agents/command/types.js";
import { createChatAbortOps } from "../chat-abort-ops.js";
import { abortChatRunById, type ChatAbortControllerEntry } from "../chat-abort.js";
import { setGatewayDedupeEntries } from "./agent-dedupe.js";
import { dispatchAgentRunFromGateway } from "./agent-run-dispatch.js";
import { createTrackedDispatch } from "./agent-run-dispatch.test-support.js";

const mocks = vi.hoisted(() => ({
  agentCommand: vi.fn(
    async (
      _options: AgentCommandOpts,
    ): Promise<
      Pick<AgentCommandDeliveryResult, "payloads"> & {
        meta: Partial<AgentCommandDeliveryResult["meta"]>;
      }
    > => ({ payloads: [], meta: {} }),
  ),
  clearAgentRunContext: vi.fn(),
}));
vi.mock("../../commands/agent.js", () => ({ agentCommandFromGatewayIngress: mocks.agentCommand }));
vi.mock("../../runtime.js", () => ({ defaultRuntime: {} }));
vi.mock(import("../../infra/agent-run-registry.js"), async (importOriginal) => ({
  ...(await importOriginal()),
  clearAgentRunContext: mocks.clearAgentRunContext,
  validateAgentRunDelegatedAuthority: () => true,
}));
vi.mock("./agent-dedupe.js", () => ({ setGatewayDedupeEntries: vi.fn() }));

describe("Gateway dispatch run ownership", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.agentCommand.mockImplementation(async () => ({ payloads: [], meta: {} }));
  });

  function createDispatch(terminalProducer = false) {
    const f = createTrackedDispatch();
    const params = {
      admittedRunEntry: f.entry,
      ingressOpts: {
        message: "continue",
        sessionKey: f.sessionKey,
        allowModelOverride: false,
        ...(terminalProducer ? { abortSignal: f.entry.controller.signal } : {}),
      },
      runId: f.runId,
      dedupeKeys: [],
      abortController: f.entry.controller,
      cleanupAbortController: vi.fn(),
      io: { emitAcceptance: vi.fn(), emitFinal: vi.fn() },
      context: f.context,
    };
    return { ...f, params };
  }

  it.each(["command", "commentary-media"] as const)(
    "joins a captured terminal save when %s startup fails before its delivery hook",
    async (startup) => {
      const { entry, params } = createDispatch(true);
      const finishCommand = createDeferred();
      const saving = createDeferred();
      const finishSave = createDeferred();
      const failStartup = async () => {
        await finishCommand.promise;
        throw new Error("Synthetic startup failure");
      };
      if (startup === "command") {
        mocks.agentCommand.mockImplementationOnce(failStartup);
      }
      const { emitFinal } = params.io;
      const completion = dispatchAgentRunFromGateway({
        ...params,
        loadCommentaryMedia: startup === "commentary-media" ? failStartup : undefined,
      });
      try {
        const producer = entry.resolveTerminalProducer?.();
        expect(
          producer?.handoff(async (producerCompleted) => {
            await producerCompleted;
            saving.resolve();
            await finishSave.promise;
          }),
        ).toBe(true);
        entry.controller.abort();
        finishCommand.resolve();
        await saving.promise;
        expect(emitFinal).not.toHaveBeenCalled();
        finishSave.resolve();
        await completion;
        expect(emitFinal).toHaveBeenCalledOnce();
        expect(params.cleanupAbortController).toHaveBeenCalledOnce();
        expect(mocks.agentCommand).toHaveBeenCalledTimes(startup === "command" ? 1 : 0);
        expect(entry.resolveTerminalProducer?.()).toBeUndefined();
      } finally {
        finishCommand.resolve();
        finishSave.resolve();
        await completion;
      }
    },
  );

  it.each(["registration", "controller", "session", "instance"] as const)(
    "rejects captured transcript custody after %s replacement",
    async (replacement) => {
      const { runId, context, entry, params } = createDispatch(true);
      const finish = createDeferred();
      mocks.agentCommand.mockImplementationOnce(async () => {
        await finish.promise;
        return { payloads: [], meta: {} };
      });
      const completion = dispatchAgentRunFromGateway(params);
      try {
        const producer = entry.resolveTerminalProducer?.();
        expect(producer).toBeDefined();
        if (replacement === "registration") {
          context.chatAbortControllers.set(runId, { ...entry });
        } else if (replacement === "controller") {
          entry.controller = new AbortController();
        } else if (replacement === "session") {
          entry.sessionId = "successor-session";
        } else {
          entry.operationalRunInstance = { runId, instanceId: "successor-instance" };
        }
        const save = vi.fn(async () => {});
        expect(producer?.handoff(save)).toBe(false);
        expect(save).not.toHaveBeenCalled();
      } finally {
        finish.resolve();
        await completion;
      }
    },
  );

  it("keeps rejected pre-dispatch results with their admitted registration", async () => {
    const { runId, sessionKey, context, entry, params } = createDispatch();
    const successor: ChatAbortControllerEntry = {
      ...entry,
      controller: new AbortController(),
      sessionId: "successor-session",
      sessionKey: "agent:main:successor-session",
      operationalRunInstance: { runId, instanceId: "successor-instance" },
    };
    context.chatAbortControllers.set(runId, successor);
    await dispatchAgentRunFromGateway({
      ...params,
      assertCurrent() {
        if (context.chatAbortControllers.get(runId) !== entry) {
          throw new Error("Gateway run owner replaced");
        }
      },
      dedupeKeys: [`agent:${runId}`],
    });
    expect(mocks.agentCommand).not.toHaveBeenCalled();
    expect(mocks.clearAgentRunContext).not.toHaveBeenCalled();
    expect(context.chatAbortControllers.get(runId)).toBe(successor);
    expect(setGatewayDedupeEntries).toHaveBeenCalledWith(
      expect.objectContaining({
        session: {
          sessionKey,
          sessionId: entry.sessionId,
          agentId: entry.agentId,
          lifecycleGeneration: entry.lifecycleGeneration,
        },
        entry: expect.objectContaining({ ok: false }),
      }),
    );
    expect(params.io.emitFinal).toHaveBeenCalledOnce();
  });

  it.each(["success", "failure", "cancelled"] as const)(
    "joins continuation and input settlement before publishing final or replay for %s",
    async (outcome) => {
      const { runId, entry, params } = createDispatch();
      const entered = createDeferred();
      const resume = createDeferred();
      const cleanupEntered = createDeferred();
      const finishCleanup = createDeferred();
      let recover: (() => void) | undefined;
      mocks.agentCommand.mockImplementationOnce(async () => {
        if (outcome === "failure") {
          throw new Error("Synthetic active run failure");
        }
        if (outcome === "cancelled") {
          entry.controller.abort();
          throw entry.controller.signal.reason;
        }
        return { payloads: [], meta: {} };
      });
      const { emitFinal } = params.io;
      const { cleanupAbortController } = params;
      cleanupAbortController.mockImplementation(async () => {
        cleanupEntered.resolve();
        await finishCleanup.promise;
      });
      const onSettled = vi.fn(async ({ onRecovered }: { onRecovered?: () => void }) => {
        recover = onRecovered;
        entered.resolve();
        await resume.promise;
        return true;
      });
      const completion = dispatchAgentRunFromGateway({
        ...params,
        onSettled,
      });
      try {
        await entered.promise;
        expect(emitFinal).not.toHaveBeenCalled();
        expect(cleanupAbortController).not.toHaveBeenCalled();
        resume.resolve();
        await cleanupEntered.promise;
        expect(emitFinal).not.toHaveBeenCalled();
        expect(setGatewayDedupeEntries).not.toHaveBeenCalled();
        recover?.();
        expect(setGatewayDedupeEntries).not.toHaveBeenCalled();
        finishCleanup.resolve();
        await completion;
        expect(setGatewayDedupeEntries).toHaveBeenCalledOnce();
        expect(cleanupAbortController).toHaveBeenCalledOnce();
        expect(emitFinal).toHaveBeenCalledOnce();
        expect(emitFinal).toHaveBeenCalledWith(
          [
            outcome !== "failure",
            expect.objectContaining({
              status: outcome === "success" ? "ok" : outcome === "failure" ? "error" : "timeout",
            }),
            outcome === "failure" ? expect.any(Object) : undefined,
          ],
          expect.objectContaining({ runId }),
        );
        expect(cleanupAbortController.mock.invocationCallOrder[0]).toBeLessThan(
          emitFinal.mock.invocationCallOrder[0] ?? Infinity,
        );
      } finally {
        resume.resolve();
        finishCleanup.resolve();
        await completion;
      }
    },
  );

  it.each(["same-session", "different-session", "removed-by-abort", "mutated-entry"] as const)(
    "settles the original run without releasing a %s registration",
    async (replacement) => {
      const { runId, sessionKey, context, entry, params } = createDispatch(true);
      const successor = {
        ...entry,
        controller: new AbortController(),
        operationalRunInstance: { runId, instanceId: "successor" },
        sessionKey: replacement === "same-session" ? sessionKey : "agent:main:other",
      };
      const finishCommand = createDeferred();
      const saving = createDeferred();
      const finishSave = createDeferred();
      mocks.agentCommand.mockImplementationOnce(async () => {
        await finishCommand.promise;
        return {
          payloads: [],
          meta: {
            ...(entry.controller.signal.aborted ? { aborted: true, stopReason: "rpc" } : {}),
            terminalReply: { disposition: "visible", text: "Original result" },
          },
        };
      });
      const { emitFinal } = params.io;
      const completion = dispatchAgentRunFromGateway(params);
      try {
        const producer = entry.resolveTerminalProducer?.();
        expect(
          producer?.handoff(async (producerCompleted) => {
            await producerCompleted;
            saving.resolve();
            await finishSave.promise;
          }),
        ).toBe(true);
        if (replacement === "removed-by-abort") {
          expect(
            abortChatRunById(createChatAbortOps(context), { runId, sessionKey, stopReason: "rpc" }),
          ).toEqual({ aborted: true });
          expect(context.chatAbortControllers.has(runId)).toBe(false);
        } else if (replacement === "mutated-entry") {
          entry.sessionKey = successor.sessionKey;
        } else {
          context.chatAbortControllers.set(runId, successor);
        }
        finishCommand.resolve();
        await saving.promise;
        expect(emitFinal).not.toHaveBeenCalled();
        finishSave.resolve();
        await completion;
        expect(emitFinal).toHaveBeenCalledOnce();
        if (replacement !== "removed-by-abort") {
          expect(mocks.clearAgentRunContext).not.toHaveBeenCalled();
          expect(context.chatAbortControllers.get(runId)).toBe(
            replacement === "mutated-entry" ? entry : successor,
          );
        }
        expect(setGatewayDedupeEntries).toHaveBeenCalledWith(
          expect.objectContaining({
            session: {
              sessionKey: replacement === "mutated-entry" ? entry.sessionKey : sessionKey,
              sessionId: entry.sessionId,
              agentId: entry.agentId,
              lifecycleGeneration: entry.lifecycleGeneration,
            },
          }),
        );
      } finally {
        finishCommand.resolve();
        finishSave.resolve();
        await completion;
      }
    },
  );

  it.each(["Primitive command failure", 42])(
    "retains the rendered message and original cause for synchronous throw %s",
    async (failure) => {
      const { params } = createDispatch();
      mocks.agentCommand.mockImplementationOnce(() => {
        // oxlint-disable-next-line typescript/only-throw-error -- Exercise JavaScript primitive throws at the dispatch boundary.
        throw failure;
      });
      await dispatchAgentRunFromGateway(params);
      expect(params.io.emitFinal).toHaveBeenCalledWith(
        [
          false,
          expect.objectContaining({ status: "error", summary: String(failure) }),
          expect.objectContaining({
            message: String(failure),
            cause: expect.objectContaining({ cause: failure }),
          }),
        ],
        expect.objectContaining({ error: String(failure) }),
      );
    },
  );
});
