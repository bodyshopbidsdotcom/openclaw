import { beforeEach, describe, expect, it, onTestFinished, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import type { AgentCommandDeliveryResult } from "../../agents/command/delivery-result.js";
import type { AgentCommandOpts } from "../../agents/command/types.js";
import { ReplaySafeRunAbortError } from "../../agents/embedded-agent-runner/transcript-conflict.js";
import { createAgentRunRestartAbortError } from "../../agents/run-termination.js";
import {
  getRpcSourceIdentity,
  getRpcSourceLifecycleGeneration,
} from "../../sessions/session-controller.rpc-sources.js";
import { rpcSourceTesting } from "../../sessions/session-lifecycle-admission.test-support.js";
import * as transcript from "../../sessions/user-turn-transcript-admission.js";
import { setGatewayDedupeEntries } from "./agent-dedupe.js";
import { createGate, expectCalls } from "./agent-owner.test-support.js";
import { dispatchAgentRunFromGateway } from "./agent-run-dispatch.js";
import { createTrackedDispatch } from "./agent-run-dispatch.test-support.js";
import {
  createTestRpcSource,
  setTestRpcSourceIdentity,
  testRpcSourceController,
} from "./rpc-source.test-support.js";

type CommandResult = Pick<AgentCommandDeliveryResult, "payloads"> & {
  meta: Partial<AgentCommandDeliveryResult["meta"]>;
};
type Entry = ReturnType<typeof createTrackedDispatch>["entry"];
const commandResult = (
  meta: CommandResult["meta"] = {},
  payloads: CommandResult["payloads"] = [],
) => ({ payloads, meta });

const mocks = vi.hoisted(() => ({
  agentCommand: vi.fn<(options: AgentCommandOpts) => Promise<CommandResult>>(),
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
    mocks.agentCommand.mockImplementation(async () => commandResult());
  });

  function createDispatch(terminalProducer = false) {
    const f = createTrackedDispatch();
    const params = {
      admittedRunEntry: f.entry,
      ingressOpts: {
        message: "continue",
        sessionKey: f.sessionKey,
        allowModelOverride: false,
        ...(terminalProducer ? { abortSignal: f.entry.input.abortSignal } : {}),
      },
      runId: f.runId,
      dedupeKeys: [],
      abortController: testRpcSourceController(f.entry),
      cleanupAbortController: vi.fn(),
      io: { emitAcceptance: vi.fn(), emitFinal: vi.fn() },
      context: f.context,
    };
    return { ...f, params };
  }

  function createSuccessor(
    entry: Entry,
    runId: string,
    identity: Partial<Parameters<typeof createTestRpcSource>[0]> = {},
    sameStore = false,
  ) {
    return createTestRpcSource(
      {
        ...entry.adapter,
        ...getRpcSourceIdentity(entry),
        ...identity,
      },
      runId,
      sameStore ? entry.input.target?.storeScope : undefined,
    );
  }

  function expectReplaySession(
    entry: Entry,
    sessionKey: string,
    extra: Record<string, unknown> = {},
  ) {
    expect(setGatewayDedupeEntries).toHaveBeenCalledWith(
      expect.objectContaining({
        session: {
          sessionKey,
          sessionId: getRpcSourceIdentity(entry).sessionId,
          agentId: getRpcSourceIdentity(entry).agentId,
          lifecycleGeneration: getRpcSourceLifecycleGeneration(entry),
        },
        ...extra,
      }),
    );
  }

  function releaseAfterTest(completion: Promise<unknown>, ...gates: { resolve: () => void }[]) {
    onTestFinished(async () => {
      for (const gate of gates) {
        gate.resolve();
      }
      await completion;
    });
  }

  function holdCommand(result: () => CommandResult = commandResult) {
    const finish = createDeferred();
    mocks.agentCommand.mockImplementationOnce(async () => {
      await finish.promise;
      return result();
    });
    return finish;
  }

  it.each(["command", "command-cleanup", "media"] as const)(
    "joins a captured terminal save with the %s producer failure",
    async (startup) => {
      const { entry, params } = createDispatch(true);
      const finishCommand = createDeferred();
      const save = createGate();
      const failure = new Error("Synthetic startup failure");
      const failStartup = async (options?: AgentCommandOpts) => {
        await finishCommand.promise;
        if (startup === "command-cleanup") {
          await options?.beforeTerminalDelivery?.(undefined, failure);
        }
        throw failure;
      };
      if (startup !== "media") {
        mocks.agentCommand.mockImplementationOnce(failStartup);
      }
      const { emitFinal } = params.io;
      const completion = dispatchAgentRunFromGateway({
        ...params,
        loadMedia: startup === "media" ? failStartup : undefined,
      });
      releaseAfterTest(completion, finishCommand, save);
      const producer = entry.adapter.resolveTerminalProducer?.();
      expect(
        producer?.handoff(async (producerCompleted) => {
          expect(await producerCompleted).toBe(failure);
          await save.wait();
        }),
      ).toBe(true);
      testRpcSourceController(entry).abort();
      finishCommand.resolve();
      await save.entered;
      expect(emitFinal).not.toHaveBeenCalled();
      save.resolve();
      await completion;
      expectCalls(1, emitFinal, params.cleanupAbortController);
      expect(entry.adapter.resolveTerminalProducer?.()).toBeUndefined();
    },
  );

  it("preserves an authentication failure after cleanup cancels its unconsumed input", async () => {
    const { params } = createDispatch(true);
    const failure = new Error('No credentials found for profile "synthetic:default".');
    const withdrawal = vi.spyOn(transcript, "readWithdrawnUserTurnInputId");
    withdrawal.mockReturnValue(undefined);
    onTestFinished(() => withdrawal.mockRestore());
    mocks.agentCommand.mockRejectedValueOnce(failure);
    params.cleanupAbortController.mockImplementation(() => {
      withdrawal.mockReturnValue("pending-input-1");
    });

    await dispatchAgentRunFromGateway(params);

    const payload = { runId: params.runId, status: "error", summary: failure.message };
    expect(params.io.emitFinal).toHaveBeenCalledWith(
      [false, payload, expect.objectContaining({ message: failure.message, cause: failure })],
      expect.objectContaining({ error: failure.message }),
    );
    expect(setGatewayDedupeEntries).toHaveBeenCalledWith(
      expect.objectContaining({
        entry: expect.objectContaining({ ok: false, payload }),
      }),
    );
  });

  it.each(["rpc", "timeout", "late-timeout"] as const)(
    "publishes confirmed withdrawal after cleanup (%s)",
    async (reason) => {
      const late = reason === "late-timeout";
      const stopReason = reason === "rpc" ? "rpc" : "timeout";
      const { params, entry } = createDispatch(true);
      const withdrawal = vi.spyOn(transcript, "readWithdrawnUserTurnInputId");
      withdrawal.mockReturnValue(undefined);
      onTestFinished(() => withdrawal.mockRestore());
      const timeout = new Error("deadline elapsed");
      timeout.name = "TimeoutError";
      const abort = () => {
        entry.adapter.abortStopReason = stopReason;
        testRpcSourceController(entry).abort(stopReason === "rpc" ? "rpc" : timeout);
      };
      mocks.agentCommand.mockImplementation(async () => {
        if (!late) {
          abort();
          throw timeout;
        }
        throw new Error("startup failed");
      });
      params.cleanupAbortController.mockImplementation(() => {
        expectCalls(0, params.io.emitFinal, setGatewayDedupeEntries);
        withdrawal.mockReturnValue("pending-input-1");
      });

      await dispatchAgentRunFromGateway({
        ...params,
        onSettled: async () => {
          if (late) {
            abort();
          }
          return true;
        },
      });

      const payload = expect.objectContaining({
        status: late ? "error" : "timeout",
        stopReason,
        reason: "input_withdrawn_before_turn",
        pendingInputId: "pending-input-1",
        summary: expect.stringContaining(
          stopReason === "timeout" ? "Raise --timeout and retry" : "Resend it when ready",
        ),
      });
      expect(params.io.emitFinal).toHaveBeenCalledWith(
        [!late, payload, late ? expect.objectContaining({ message: "startup failed" }) : undefined],
        expect.any(Object),
      );
      expect(setGatewayDedupeEntries).toHaveBeenCalledWith(
        expect.objectContaining({ entry: expect.objectContaining({ ok: !late, payload }) }),
      );
    },
  );

  it.each(["registration", "controller", "session", "instance"] as const)(
    "rejects captured transcript custody after %s replacement",
    async (replacement) => {
      const { runId, entry, params } = createDispatch(true);
      const finish = holdCommand();
      const completion = dispatchAgentRunFromGateway(params);
      releaseAfterTest(completion, finish);
      const producer = entry.adapter.resolveTerminalProducer?.();
      expect(producer).toBeDefined();
      if (replacement === "registration") {
        rpcSourceTesting.set(runId, { ...entry });
      } else if (replacement === "controller") {
        const successor = createSuccessor(entry, runId);
        rpcSourceTesting.set(runId, successor);
      } else if (replacement === "session") {
        setTestRpcSourceIdentity(entry, { sessionId: "successor-session" });
      } else {
        entry.adapter.operationalRunInstance = { runId, instanceId: "successor-instance" };
      }
      const save = vi.fn(async () => {});
      expect(producer?.handoff(save)).toBe(false);
      expect(save).not.toHaveBeenCalled();
    },
  );

  it("keeps rejected pre-dispatch results with their admitted registration", async () => {
    const { runId, sessionKey, entry, params } = createDispatch();
    const successor = createSuccessor(entry, runId, {
      sessionId: "successor-session",
      sessionKey: "agent:main:successor-session",
      operationalRunInstance: { runId, instanceId: "successor-instance" },
    });
    rpcSourceTesting.set(runId, successor);
    const emitFinal = vi.fn();

    await dispatchAgentRunFromGateway({
      ...params,
      assertCurrent() {
        if (rpcSourceTesting.get(runId) !== entry) {
          throw new Error("Gateway run owner replaced");
        }
      },
      dedupeKeys: [`agent:${runId}`],
      io: { ...params.io, emitFinal },
    });
    expectCalls(0, mocks.agentCommand, mocks.clearAgentRunContext);
    expect(rpcSourceTesting.get(runId)).toBe(successor);
    expectReplaySession(entry, sessionKey, { entry: expect.objectContaining({ ok: false }) });
    expect(emitFinal).toHaveBeenCalledOnce();
  });

  it.each(["success", "failure", "cancelled", "safe-cancelled", "restart"] as const)(
    "joins continuation and input settlement before publishing final or replay for %s",
    async (outcome) => {
      const { runId, entry, params } = createDispatch();
      const settlement = createGate();
      const cleanup = createGate();
      let recover: (() => void) | undefined;
      mocks.agentCommand.mockImplementationOnce(async () => {
        if (outcome === "failure") {
          throw new Error("Synthetic active run failure");
        }
        if (outcome === "restart") {
          const reason = createAgentRunRestartAbortError();
          testRpcSourceController(entry).abort(reason);
          throw reason;
        }
        if (outcome === "cancelled" || outcome === "safe-cancelled") {
          testRpcSourceController(entry).abort();
          const reason = entry.input.abortSignal.reason;
          throw outcome === "safe-cancelled" ? new ReplaySafeRunAbortError(reason) : reason;
        }
        return commandResult();
      });
      const { emitFinal } = params.io;
      const { cleanupAbortController } = params;
      cleanupAbortController.mockImplementation(cleanup.wait);
      const onSettled = vi.fn(async ({ onRecovered }: { onRecovered?: () => void }) => {
        recover = onRecovered;
        await settlement.wait();
        return true;
      });
      const completion = dispatchAgentRunFromGateway({ ...params, onSettled });
      releaseAfterTest(completion, settlement, cleanup);
      await settlement.entered;
      expectCalls(0, emitFinal, cleanupAbortController);
      settlement.resolve();
      await cleanup.entered;
      expectCalls(0, emitFinal, setGatewayDedupeEntries);
      recover?.();
      expect(setGatewayDedupeEntries).not.toHaveBeenCalled();
      cleanup.resolve();
      await completion;
      expectCalls(1, setGatewayDedupeEntries, cleanupAbortController);
      expect(emitFinal.mock.calls[0]?.[0][1]?.replaySafe).toBe(
        outcome === "safe-cancelled" ? true : undefined,
      );
      expect(emitFinal).toHaveBeenCalledExactlyOnceWith(
        [
          outcome !== "failure",
          expect.objectContaining({
            status: outcome === "success" ? "ok" : outcome === "failure" ? "error" : "timeout",
            ...(outcome === "restart" ? { stopReason: "restart" } : {}),
          }),
          outcome === "failure" ? expect.any(Object) : undefined,
        ],
        expect.objectContaining({ runId }),
      );
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
