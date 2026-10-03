import { beforeEach, describe, expect, it, vi } from "vitest";
import { createDeferred } from "../../../test/helpers/promise.js";
import { resolveAgentRunContext } from "../../agents/command/run-context.js";
import {
  getPreparedModelRuntimeBorrowedSnapshot,
  getPreparedModelRuntimePluginGeneration,
} from "../../agents/prepared-model-runtime-generation-scope.js";
import type { SessionEntry } from "../../config/sessions.js";
import { isWebchatClient } from "../../utils/message-channel.js";
import { readGatewayAccessRevision } from "../gateway-access-revision.js";
import * as sessionChange from "../server-methods/session-change-event.js";
import { replayAgentTurnIfCached } from "./agent-dedupe.js";
import { resolveAgentDeliveryPhase } from "./agent-delivery-phase.js";
import { startAgentRunExecution } from "./agent-run-execution-phase.js";
import type { AgentTurnPrincipal } from "./types.js";

const dispatchAgentRunFromGateway = vi.hoisted(() => vi.fn());

vi.mock("./agent-run-dispatch.js", () => ({
  dispatchAgentRunFromGateway,
  resolveAbortedAgentStopReason: () => "rpc",
}));

function createExecution(options: { aborted?: boolean; assertContextCurrent?: () => void } = {}) {
  const abortCleanup = vi.fn();
  const gatewayRelease = vi.fn();
  const callerRelease = vi.fn();
  const { promise: runtimeReleased, resolve: resolveRuntimeReleased } = createDeferred();
  const runtimeRelease = vi.fn(async () => resolveRuntimeReleased());
  const controller = new AbortController();
  if (options.aborted) {
    controller.abort();
  }
  return {
    abortCleanup,
    gatewayRelease,
    callerRelease,
    runtimeRelease,
    runtimeReleased,
    params: {
      assertContextCurrent: options.assertContextCurrent,
      prepared: {
        releaseCallerAuthority: callerRelease,
        activeGatewayWorkAdmission: {
          release: gatewayRelease,
          run: async (run: () => Promise<void>) => await run(),
        },
        activeRunAbort: {
          cleanup: abortCleanup,
          controller,
          registered: false,
        },
        effectiveAllowModelOverride: false,
        lifecycleStorePath: "",
        operationalRunInstance: {},
        preparedModelRuntimeLease: { [Symbol.asyncDispose]: runtimeRelease, snapshot: {} },
        replyDispatchRuntime: {
          config: { runtime: "A" },
          pluginGeneration: "generation-A",
        },
        unpersistedOffloadedRefs: [],
        userTurn: {
          execApprovalFollowupHandoffClaimId: "claim",
          message: "continue",
          senderIsOwner: false,
          suppressPromptPersistence: false,
        },
        workspaceOverride: "/workspace/A",
      },
      request: {},
      cfg: {},
      activeSessionAgentId: "main",
      delivery: {},
      isNewSession: false,
      isRawModelRun: true,
      isOneShotModelRun: true,
      isRestartRecoveryResumeRun: false,
      suppressVisibleSessionEffects: true,
      images: [],
      imageOrder: [],
      media: [],
      runId: "owner-test",
      agentDedupeKeys: [],
      bestEffortDeliver: false,
      lifecycleGeneration: "test",
      preserveUserFacingSessionModelState: false,
      skipAgentInitialSessionTouch: true,
      canUseInternalRuntimeHandoff: false,
      client: null,
      context: {
        dedupe: new Map(),
        deps: {},
        logGateway: { error: vi.fn(), warn: vi.fn() },
      },
      io: {
        emitAcceptance: vi.fn(),
        emitFinal: vi.fn(),
      },
      releaseCronContinuationClaimWithRecovery: async () => true,
    } as unknown as Parameters<typeof startAgentRunExecution>[0],
  };
}

function createVisibleExecution() {
  const execution = createExecution();
  const sessionKey = "agent:main:task-access-liveness";
  Object.assign(execution.params, {
    suppressVisibleSessionEffects: false,
    requestedSessionKey: sessionKey,
    resolvedSessionKey: sessionKey,
  });
  Object.assign(execution.params.context, {
    getRuntimeConfig: () => ({}),
    getSessionEventSubscriberConnIds: () => new Set(),
  });
  execution.params.prepared.activeRunAbort.markExecutionStarted = vi.fn(() => true);
  execution.params.prepared.userTurn.recorder = {
    finishPendingInput: vi.fn(),
  } as unknown as NonNullable<typeof execution.params.prepared.userTurn.recorder>;
  return execution;
}

describe("startAgentRunExecution Gateway ownership", () => {
  beforeEach(() => {
    dispatchAgentRunFromGateway.mockReset();
  });

  it.each([false, true])(
    "preserves access across liveness and invalidates creation (new session: %s)",
    async (isNewSession) => {
      const execution = createVisibleExecution();
      execution.params.isNewSession = isNewSession;
      const publish = sessionChange.emitSessionsChanged;
      const notices: Array<{ reason: string; accessChanges: number }> = [];
      const publisher = vi
        .spyOn(sessionChange, "emitSessionsChanged")
        .mockImplementation((...args) => {
          const before = readGatewayAccessRevision();
          publish(...args);
          notices.push({
            reason: args[1].reason,
            accessChanges: readGatewayAccessRevision() - before,
          });
        });
      dispatchAgentRunFromGateway.mockImplementationOnce(async (dispatch) => {
        await dispatch.ingressOpts.onExecutionStarted();
        dispatch.cleanupAbortController();
      });

      try {
        await startAgentRunExecution(execution.params);

        expect(dispatchAgentRunFromGateway).toHaveBeenCalledOnce();
        expect(notices).toEqual([
          ...(isNewSession ? [{ reason: "create", accessChanges: expect.any(Number) }] : []),
          { reason: "send", accessChanges: 0 },
          { reason: "agent.run.started", accessChanges: 0 },
          { reason: "agent.input.settled", accessChanges: 0 },
        ]);
        if (isNewSession) {
          expect(notices[0]?.accessChanges).toBeGreaterThan(0);
        }
      } finally {
        publisher.mockRestore();
      }
    },
  );

  it.each<{
    name: string;
    webchat?: boolean;
    sourceChannel?: string;
    replyChannel?: string;
    sessionDelivery?: SessionEntry["delivery"];
    expectedChannel?: string;
  }>([
    { name: "unbound CLI" },
    { name: "CLI with internal delivery history", sessionDelivery: { kind: "internal" } },
    { name: "WebChat client", webchat: true, expectedChannel: "webchat" },
    { name: "WebChat continuation", sourceChannel: "webchat", expectedChannel: "webchat" },
    {
      name: "channel continuation with an internal reply override",
      sourceChannel: "discord",
      replyChannel: "webchat",
      expectedChannel: "discord",
    },
    {
      name: "remembered provider without a target",
      sessionDelivery: {
        kind: "external",
        route: { channel: "discord" },
        context: { channel: "discord" },
        origin: { provider: "discord" },
      },
      expectedChannel: "discord",
    },
    { name: "explicit internal channel", replyChannel: "webchat", expectedChannel: "webchat" },
  ])("preserves $name provider context through command resolution", async (testCase) => {
    const execution = createExecution();
    const client: AgentTurnPrincipal = {
      connect: {
        minProtocol: 1,
        maxProtocol: 1,
        client: {
          id: testCase.webchat ? "webchat-ui" : "cli",
          mode: testCase.webchat ? "webchat" : "cli",
          version: "test",
          platform: "test",
        },
      },
    };
    const delivery = await resolveAgentDeliveryPhase({
      request: {
        message: "continue",
        idempotencyKey: execution.params.runId,
        replyChannel: testCase.replyChannel,
      },
      cfg: {},
      sessionEntry: testCase.sessionDelivery
        ? { sessionId: "source-session", updatedAt: 1, delivery: testCase.sessionDelivery }
        : undefined,
      agentId: "main",
      recipientChannel: testCase.sourceChannel,
      replyTo: "",
      to: "",
      bestEffortDeliver: false,
      runId: execution.params.runId,
      client,
      context: execution.params.context,
      respond: vi.fn(),
      isWebchatConnect: (connect) => isWebchatClient(connect?.client),
    });
    expect(delivery).toBeDefined();
    if (!delivery) {
      throw new Error("delivery planning failed");
    }
    execution.params.delivery = delivery;
    execution.params.client = client;
    dispatchAgentRunFromGateway.mockResolvedValueOnce(undefined);

    await startAgentRunExecution(execution.params);

    expect(dispatchAgentRunFromGateway).toHaveBeenCalledOnce();
    const dispatch = dispatchAgentRunFromGateway.mock.calls[0]?.[0];
    const runContext = resolveAgentRunContext(dispatch.ingressOpts);
    expect(runContext.messageChannel).toBe(testCase.expectedChannel);
    expect(runContext.currentChannelId).toBeUndefined();
  });

  it.each([
    { sourceIngress: "control-ui" as const, sourceChannel: "webchat", deliveryContext: undefined },
    {
      sourceIngress: "channel" as const,
      sourceChannel: "discord",
      deliveryContext: { channel: "discord" },
    },
  ])(
    "preserves targetless $sourceChannel policy context at recovery dispatch",
    async ({ sourceIngress, sourceChannel, deliveryContext }) => {
      const execution = createExecution();
      Object.assign(execution.params, {
        canUseInternalRuntimeHandoff: true,
        isRestartRecoveryResumeRun: true,
        resolvedSessionId: "recovery-session",
        sessionEntry: {
          sessionId: "recovery-session",
          updatedAt: 1,
          restartRecoveryDeliveryRunId: execution.params.runId,
          restartRecoveryDeliverySourceRunId: "source-run",
          restartRecoveryDeliveryContext: deliveryContext,
          restartRecoverySourceIngress: sourceIngress,
        },
      });
      execution.params.request.expectedExistingSessionId = "recovery-session";
      execution.params.delivery.originMessageChannel = "slack";
      dispatchAgentRunFromGateway.mockResolvedValueOnce(undefined);

      await startAgentRunExecution(execution.params);

      expect(dispatchAgentRunFromGateway).toHaveBeenCalledOnce();
      const dispatch = dispatchAgentRunFromGateway.mock.calls[0]?.[0];
      expect(dispatch?.ingressOpts.runContext.messageChannel).toBe(sourceChannel);
      expect(dispatch?.ingressOpts.runContext.currentChannelId).toBeUndefined();
    },
  );

  it("dispatches with the runtime generation frozen at admission", async () => {
    const execution = createExecution();
    const { promise: dispatched, resolve: resolveDispatched } = createDeferred();
    const { promise: cleanupObserved, resolve: resolveCleanupObserved } = createDeferred();
    let borrowedAfterCleanup: Promise<unknown> | undefined;
    let dispatchedGeneration: unknown;
    let dispatchedSnapshot: unknown;
    dispatchAgentRunFromGateway.mockImplementationOnce(() => {
      const generation = execution.params.prepared.replyDispatchRuntime.pluginGeneration;
      dispatchedGeneration = getPreparedModelRuntimePluginGeneration();
      dispatchedSnapshot = getPreparedModelRuntimeBorrowedSnapshot(generation);
      borrowedAfterCleanup = (async () => {
        await cleanupObserved;
        return getPreparedModelRuntimeBorrowedSnapshot(generation);
      })();
      resolveDispatched();
      return cleanupObserved;
    });

    const completion = startAgentRunExecution(execution.params);

    await dispatched;
    expect(dispatchedGeneration).toBe(
      execution.params.prepared.replyDispatchRuntime.pluginGeneration,
    );
    expect(dispatchedSnapshot).toBe(execution.params.prepared.preparedModelRuntimeLease.snapshot);
    const dispatch = dispatchAgentRunFromGateway.mock.calls[0]?.[0];
    expect(dispatch?.commandRuntimeContext).toEqual({
      config: { runtime: "A" },
      pluginGeneration: "generation-A",
    });
    expect(dispatch?.ingressOpts.workspaceDir).toBe("/workspace/A");
    expect(execution.runtimeRelease).not.toHaveBeenCalled();

    dispatch?.cleanupAbortController();
    dispatch?.cleanupAbortController();
    expect(execution.callerRelease).not.toHaveBeenCalled();
    resolveCleanupObserved();
    await expect(borrowedAfterCleanup).resolves.toBeUndefined();
    await completion;
    expect(execution.runtimeRelease).toHaveBeenCalledOnce();
    expect(execution.callerRelease).toHaveBeenCalledOnce();
  });

  it.each(["aborted", "failed"] as const)(
    "releases an undispatched %s run only after cleanup",
    async (ending) => {
      const execution = createExecution({
        aborted: ending === "aborted",
        ...(ending === "failed"
          ? {
              assertContextCurrent: () => {
                throw new Error("Gateway owner retired");
              },
            }
          : {}),
      });
      const recoveryEntered = createDeferred();
      const releaseRecovery = createDeferred();
      const disposalEntered = createDeferred();
      const finishDisposal = createDeferred();
      execution.params.releaseCronContinuationClaimWithRecovery = async () => {
        recoveryEntered.resolve();
        await releaseRecovery.promise;
        return true;
      };
      execution.runtimeRelease.mockImplementation(async () => {
        disposalEntered.resolve();
        await finishDisposal.promise;
      });
      const finished = vi.fn();
      const completion = startAgentRunExecution(execution.params).then(finished);
      try {
        await Promise.race([recoveryEntered.promise, completion]);
        expect(dispatchAgentRunFromGateway).not.toHaveBeenCalled();
        expect(execution.params.io.emitFinal).toHaveBeenCalledOnce();
        expect(execution.abortCleanup).not.toHaveBeenCalled();
        releaseRecovery.resolve();
        await Promise.race([disposalEntered.promise, completion]);
        expect(execution.abortCleanup).toHaveBeenCalledOnce();
        expect(execution.gatewayRelease).toHaveBeenCalledOnce();
        expect(execution.runtimeRelease).toHaveBeenCalledOnce();
        expect(execution.callerRelease).not.toHaveBeenCalled();
        expect(finished).not.toHaveBeenCalled();
        finishDisposal.resolve();
        await completion;
        expect(finished).toHaveBeenCalledOnce();
        expect(execution.callerRelease).toHaveBeenCalledOnce();
      } finally {
        releaseRecovery.resolve();
        finishDisposal.resolve();
        await completion.catch(() => {});
      }
    },
  );

  it.each([false, true])(
    "releases the admitted runtime and preserves private failure replay before dispatch (Incognito: %s)",
    async (incognito) => {
      const privateMessage = "synthetic-private-pre-dispatch-error";
      const execution = createVisibleExecution();
      const fail = () => {
        throw new Error(privateMessage);
      };
      execution.params.assertContextCurrent = fail;
      execution.params.prepared.userTurn.releaseProcessingAbortObserver = fail;
      Object.assign(execution.params.prepared.userTurn.recorder ?? {}, {
        completeProcessing: fail,
      });
      execution.params.resolvedSessionKey = "agent:main:dashboard:private-owner";
      execution.params.sessionEntry = {
        sessionId: "private-owner",
        updatedAt: Date.now(),
        ...(incognito ? { incognito: true } : {}),
      };
      execution.params.agentDedupeKeys = [`agent:${execution.params.runId}`];

      await startAgentRunExecution(execution.params);
      expect(dispatchAgentRunFromGateway).not.toHaveBeenCalled();
      expect(execution.abortCleanup).toHaveBeenCalledOnce();
      expect(execution.gatewayRelease).toHaveBeenCalledOnce();
      expect(execution.runtimeRelease).toHaveBeenCalledOnce();
      const warnings = vi.mocked(execution.params.context.logGateway.warn).mock.calls;
      expect(warnings).toHaveLength(2);
      if (incognito) {
        expect.soft(JSON.stringify(warnings)).not.toContain(privateMessage);
      } else {
        expect(JSON.stringify(warnings)).toContain(privateMessage);
      }
      const [frame, metadata] = vi.mocked(execution.params.io.emitFinal).mock.calls[0] ?? [];
      expect(frame?.[2]?.message).toBe(privateMessage);
      const diagnostics = { errorMessage: frame?.[2]?.message, ...metadata };
      if (incognito) {
        expect.soft(JSON.stringify(diagnostics)).not.toContain(privateMessage);
      } else {
        expect(diagnostics).toMatchObject({ error: privateMessage, errorMessage: privateMessage });
      }

      const emitAcceptance = vi.fn();
      expect(
        replayAgentTurnIfCached({
          preflight: {
            runId: execution.params.runId,
            agentDedupeKeys: execution.params.agentDedupeKeys,
          },
          context: execution.params.context,
          io: { emitAcceptance, emitFinal: vi.fn() },
        }),
      ).toBe(true);
      const [replayFrame, replayMetadata] = emitAcceptance.mock.calls[0] ?? [];
      expect(replayFrame).toEqual(frame);
      const replayDiagnostics = { errorMessage: replayFrame?.[2]?.message, ...replayMetadata };
      if (incognito) {
        expect(JSON.stringify(replayDiagnostics)).not.toContain(privateMessage);
      } else {
        expect(replayDiagnostics).toMatchObject({ cached: true, errorMessage: privateMessage });
      }
    },
  );
});
