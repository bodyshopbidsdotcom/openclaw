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

  it("joins a captured terminal save when command startup fails before its delivery hook", async () => {
    const { runId, sessionKey, context, entry } = createTrackedDispatch();
    const finishCommand = createDeferred();
    const saving = createDeferred();
    const finishSave = createDeferred();
    mocks.agentCommand.mockImplementationOnce(async () => {
      await finishCommand.promise;
      throw new Error("Synthetic startup failure");
    });
    const emitFinal = vi.fn();
    const completion = dispatchAgentRunFromGateway({
      admittedRunEntry: entry,
      ingressOpts: {
        message: "Synthetic startup",
        sessionKey,
        allowModelOverride: false,
        abortSignal: entry.controller.signal,
      },
      runId,
      dedupeKeys: [],
      abortController: entry.controller,
      cleanupAbortController: vi.fn(),
      io: { emitAcceptance: vi.fn(), emitFinal },
      context,
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
      expect(entry.resolveTerminalProducer?.()).toBeUndefined();
    } finally {
      finishCommand.resolve();
      finishSave.resolve();
      await completion;
    }
  });

  it.each(["registration", "controller", "session", "instance"] as const)(
    "rejects captured transcript custody after %s replacement",
    async (replacement) => {
      const { runId, sessionKey, context, entry } = createTrackedDispatch();
      const finish = createDeferred();
      mocks.agentCommand.mockImplementationOnce(async () => {
        await finish.promise;
        return { payloads: [], meta: {} };
      });
      const completion = dispatchAgentRunFromGateway({
        admittedRunEntry: entry,
        ingressOpts: {
          message: "Synthetic stale producer",
          sessionKey,
          allowModelOverride: false,
          abortSignal: entry.controller.signal,
        },
        runId,
        dedupeKeys: [],
        abortController: entry.controller,
        cleanupAbortController: vi.fn(),
        io: { emitAcceptance: vi.fn(), emitFinal: vi.fn() },
        context,
      });
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
    const { runId, sessionKey, context, entry } = createTrackedDispatch();
    const successor: ChatAbortControllerEntry = {
      ...entry,
      controller: new AbortController(),
      sessionId: "successor-session",
      sessionKey: "agent:main:successor-session",
      operationalRunInstance: { runId, instanceId: "successor-instance" },
    };
    context.chatAbortControllers.set(runId, successor);
    const emitFinal = vi.fn();
    await dispatchAgentRunFromGateway({
      assertCurrent() {
        if (context.chatAbortControllers.get(runId) !== entry) {
          throw new Error("Gateway run owner replaced");
        }
      },
      admittedRunEntry: entry,
      ingressOpts: {
        message: "run only for the admitted owner",
        sessionKey,
        allowModelOverride: false,
      },
      runId,
      dedupeKeys: [`agent:${runId}`],
      abortController: entry.controller,
      cleanupAbortController: vi.fn(),
      io: { emitAcceptance: vi.fn(), emitFinal },
      context,
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
    expect(emitFinal).toHaveBeenCalledOnce();
  });

  it.each(["success", "failure", "cancelled"] as const)(
    "awaits continuation settlement before releasing the run and reporting %s",
    async (outcome) => {
      const { runId, sessionKey, context, entry } = createTrackedDispatch();
      const entered = createDeferred();
      const resume = createDeferred();
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
      const emitFinal = vi.fn();
      const cleanupAbortController = vi.fn();
      const onSettled = vi.fn(async () => {
        entered.resolve();
        await resume.promise;
        return true;
      });
      const completion = dispatchAgentRunFromGateway({
        admittedRunEntry: entry,
        ingressOpts: { message: "continue", sessionKey, allowModelOverride: false },
        runId,
        dedupeKeys: [],
        abortController: entry.controller,
        cleanupAbortController,
        io: { emitAcceptance: vi.fn(), emitFinal },
        context,
        onSettled,
      });
      try {
        await entered.promise;
        expect(emitFinal).not.toHaveBeenCalled();
        expect(cleanupAbortController).not.toHaveBeenCalled();
        resume.resolve();
        await completion;
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
        await completion;
      }
    },
  );

  it.each(["same-session", "different-session", "removed-by-abort", "mutated-entry"] as const)(
    "settles the original run without releasing a %s registration",
    async (replacement) => {
      const { runId, sessionKey, context, entry } = createTrackedDispatch();
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
      const emitFinal = vi.fn();
      const completion = dispatchAgentRunFromGateway({
        admittedRunEntry: entry,
        ingressOpts: {
          message: "original",
          sessionKey,
          allowModelOverride: false,
          abortSignal: entry.controller.signal,
        },
        runId,
        dedupeKeys: [],
        abortController: entry.controller,
        cleanupAbortController: vi.fn(),
        io: { emitAcceptance: vi.fn(), emitFinal },
        context,
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
      const { runId, sessionKey, context, entry } = createTrackedDispatch();
      mocks.agentCommand.mockImplementationOnce(() => {
        // oxlint-disable-next-line typescript/only-throw-error -- Exercise JavaScript primitive throws at the dispatch boundary.
        throw failure;
      });
      const emitFinal = vi.fn();
      await dispatchAgentRunFromGateway({
        admittedRunEntry: entry,
        ingressOpts: { message: "continue", sessionKey, allowModelOverride: false },
        runId,
        dedupeKeys: [],
        abortController: entry.controller,
        cleanupAbortController: vi.fn(),
        io: { emitAcceptance: vi.fn(), emitFinal },
        context,
      });
      expect(emitFinal).toHaveBeenCalledWith(
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
