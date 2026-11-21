import { randomUUID } from "node:crypto";
import { expectDefined } from "@openclaw/normalization-core/expect";
import { describe, expect, it, vi } from "vitest";
import {
  awaitGateBeforeSettlement,
  createDeferred,
  withinTest,
} from "../../test/helpers/promise.js";
import {
  createOperationalRunInstanceRef,
  getAdmittedRunDelegatedAuthority,
  prepareAgentRunAdmission,
} from "../agents/admitted-run-context.js";
import { prepareAgentRunTerminalReply } from "../agents/agent-run-terminal-reply.js";
import { captureAgentToolSourceExecutionGuard } from "../agents/agent-tool-source-execution-guard.js";
import type { AgentCommandGatewayIngressOpts } from "../agents/command/types.js";
import {
  createAdmittedGatewayToolCallerIdentity,
  withGatewayToolCallerIdentity,
} from "../agents/tools/gateway-caller-context.js";
import { createSessionsSendTool } from "../agents/tools/sessions-send-tool.js";
import { setRuntimeConfigSnapshot } from "../config/runtime-snapshot.js";
import * as sessionAccessor from "../config/sessions/session-accessor.js";
import * as pendingInputs from "../config/sessions/session-accessor.pending-inputs.js";
import { listSessionPendingInputs } from "../config/sessions/session-accessor.pending-inputs.js";
import {
  resolveSqliteStoreScope,
  runExclusiveSqliteSessionWrite,
} from "../config/sessions/session-accessor.sqlite-scope.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { registerInternalHook, unregisterInternalHook } from "../hooks/internal-hooks.js";
import { emitAgentEvent } from "../infra/agent-events.js";
import { withPluginRuntimeGatewayRequestScope } from "../plugins/runtime/gateway-request-scope.js";
import * as gatewayWork from "../process/gateway-work-admission.js";
import { createReplyTurnParticipants } from "../sessions/session-controller.tool-authority.js";
import { captureGatewayOperatorRunAuthority } from "./operator-run-authority.js";
import * as gatewayDispatch from "./server-plugin-in-process-dispatch.js";
import { createOperatorClient } from "./server-plugin-in-process-dispatch.test-support.js";
import { dispatchGatewayMethodInProcess } from "./server-plugins.js";
import { runIndependentCreationInputCase } from "./server.agent-input-authority.create.test-helper.js";
import {
  holdExecution,
  installAuthorityProofGateway,
} from "./server.agent-runtime-authority-proof.test-support.js";
import { loadSessionEntryAsync } from "./session-entry-load.test-support.js";
import { getSessionRowProjection } from "./session-row-projection-access.js";
import { withPreparedSessionResolve } from "./sessions-resolve.js";
import { agentCommandMock, prepareGatewayReplyRuntimeForTest } from "./test-helpers.js";

describe("spawn input ownership transfer", () => {
  const getContext = installAuthorityProofGateway();
  const prepareAdmission = (
    cfg: OpenClawConfig,
    runId: string,
    boundary: string,
    operatorAuthority?: Parameters<typeof prepareAgentRunAdmission>[0]["operatorAuthority"],
  ) =>
    prepareAgentRunAdmission({
      cfg,
      operationalRunInstance: createOperationalRunInstanceRef(runId),
      operatorAuthority,
      facts: {
        runId,
        agentId: "main",
        ingress: {
          kind: operatorAuthority ? "gateway-client" : "system",
          boundary,
          state: "present",
        },
      },
    });
  const send = (
    identity: () => Parameters<typeof withGatewayToolCallerIdentity>[0],
    options: Parameters<typeof createSessionsSendTool>[0],
    call: Parameters<ReturnType<typeof createSessionsSendTool>["execute"]>,
    client?: ReturnType<typeof createOperatorClient>,
  ) => {
    const context = getContext();
    return withPluginRuntimeGatewayRequestScope(
      { client, context, resolveGatewayContext: () => context, isWebchatConnect: () => false },
      () =>
        withGatewayToolCallerIdentity(identity(), () =>
          createSessionsSendTool(options).execute(...call),
        ),
    );
  };
  const caller = (
    admittedRunContext: Parameters<
      typeof createAdmittedGatewayToolCallerIdentity
    >[0]["admittedRunContext"],
    sessionKey: string,
  ) => createAdmittedGatewayToolCallerIdentity({ admittedRunContext, agentId: "main", sessionKey });
  const inputScope = async (sessionKey: string, sessionId: string, agentId = "main") => ({
    agentId,
    sessionKey,
    sessionId,
    storePath: (await loadSessionEntryAsync(sessionKey, { agentId })).storePath,
  });
  const holdInput = async (
    scope: { storePath: string; agentId: string },
    beforeStaging: boolean,
    signal: AbortSignal,
  ) => {
    const staged = createDeferred();
    const releaseWriter = createDeferred();
    let writer: Promise<unknown> | undefined;
    const execution = await holdExecution(signal);
    const stage = pendingInputs.stageSessionPendingInput;
    const stageSpy = vi
      .spyOn(pendingInputs, "stageSessionPendingInput")
      .mockImplementationOnce(async (...args) => {
        if (beforeStaging) {
          const locked = createDeferred();
          writer = runExclusiveSqliteSessionWrite(
            resolveSqliteStoreScope(scope.storePath, { agentId: scope.agentId }),
            async () => {
              locked.resolve();
              await releaseWriter.promise;
            },
            "session.transcript.batch",
          );
          await locked.promise;
        }
        const pending = stage(...args);
        staged.resolve();
        return await pending;
      });
    return {
      ...execution,
      staged,
      releaseWriter,
      release(this: void) {
        releaseWriter.resolve();
      },
      async close(pending?: Promise<unknown>) {
        this.release();
        await Promise.allSettled([writer, pending, execution.resume()]);
        // A pending dispatch can enter execution after resume; drain it before restoring the spy.
        await execution.cleanup().catch(() => {});
        stageSpy.mockRestore();
      },
    };
  };

  it.for([
    "slow acceptance",
    "slow kickoff failure",
    "before acceptance",
    "after acceptance",
    "guest after acceptance",
    "participant after creation",
    "participant after acceptance",
    "target abort",
    "target replacement",
    "operator revocation",
  ] as const)("transfers independently created input at %s", async (boundary, { signal }) => {
    await runIndependentCreationInputCase(getContext(), boundary, signal);
  });

  it.for(["native", "peer"] as const)(
    "leaves a %s target reply in its session without waking the requester",
    async (kind, { signal }) => {
      await prepareGatewayReplyRuntimeForTest();
      const context = getContext();
      const cfg = context.getRuntimeConfig();
      const runId = randomUUID();
      const parentKey = `agent:main:dashboard:reply-parent-${runId}`;
      const childKey = `agent:main:dashboard:reply-child-${runId}`;
      const client = createOperatorClient({
        profileName: `reply-custody-${runId}`,
        scopes: ["operator.admin"],
      });
      for (const sessionKey of [parentKey, childKey]) {
        await sessionAccessor.upsertSessionEntryCore(
          { agentId: "main", sessionKey },
          {
            sessionId: sessionKey,
            updatedAt: 1,
            ...(kind === "native" && sessionKey === childKey
              ? { spawnedBy: parentKey, spawnDepth: 1 }
              : {}),
          },
        );
      }
      await withPreparedSessionResolve(
        {
          projection: expectDefined(getSessionRowProjection(context), "Gateway session projection"),
          client,
          p: { key: childKey, agentId: "main" },
        },
        (resolved) => expect(resolved).toMatchObject({ ok: true, key: childKey }),
      );
      const operator = await captureGatewayOperatorRunAuthority({ client, context });
      if (!operator) {
        throw new Error("Expected original operator authority");
      }
      const admission = prepareAdmission(cfg, `requester-${runId}`, "agent", operator.authority);
      const admitted = await admission.admit("embedded");
      const participants = createReplyTurnParticipants({ operatorAuthority: operator.authority });
      const childEntered = createDeferred();
      const releaseChild = createDeferred();
      const executions: Promise<void>[] = [];
      const commandSessions: string[] = [];
      const detachedHolders: string[] = [];
      const continueWork = gatewayWork.runWithGatewayDetachedWorkContinuation;
      const detachedObserver = vi
        .spyOn(gatewayWork, "runWithGatewayDetachedWorkContinuation")
        .mockImplementation((work, holder) => {
          if (holder?.startsWith("session:")) {
            detachedHolders.push(holder);
          }
          return continueWork(work, holder);
        });
      const executionModule = await import("./agent-turn/agent-run-execution-phase.js");
      const execute = executionModule.startAgentRunExecution;
      const executionSpy = vi
        .spyOn(executionModule, "startAgentRunExecution")
        .mockImplementation((params) => {
          const pending = execute(params);
          executions.push(pending);
          return pending;
        });
      agentCommandMock.mockImplementation(async (input) => {
        const opts = input as AgentCommandGatewayIngressOpts;
        await opts.userTurnTranscriptRecorder?.persistApproved();
        commandSessions.push(opts.sessionKey ?? "");
        if (opts.sessionKey === childKey) {
          childEntered.resolve();
          await releaseChild.promise;
        }
        const text = "delayed child result";
        const { terminalReply } = prepareAgentRunTerminalReply({ rawText: text });
        const loaded = await loadSessionEntryAsync(opts.sessionKey!, { agentId: "main" });
        await sessionAccessor.persistSessionTranscriptTurn(
          {
            agentId: "main",
            sessionKey: opts.sessionKey!,
            sessionId: loaded.entry!.sessionId,
            storePath: loaded.storePath,
          },
          {
            cwd: "/tmp",
            runId: opts.runId,
            expectedSessionId: loaded.entry!.sessionId,
            touchSessionEntry: false,
            updateMode: "none",
            messages: [
              {
                message: {
                  role: "assistant",
                  phase: "final_answer",
                  stopReason: "stop",
                  content: [{ type: "text", text }],
                },
                now: Date.now(),
              },
            ],
          },
        );
        emitAgentEvent({
          runId: opts.runId!,
          stream: "lifecycle",
          data: { phase: "end", startedAt: 1, endedAt: Date.now(), terminalReply },
        });
        return { payloads: [{ text, mediaUrl: null }], meta: { durationMs: 1, terminalReply } };
      });
      const unblock = () => releaseChild.resolve();
      signal.addEventListener("abort", unblock, { once: true });
      try {
        const result = await send(
          () => ({
            ...caller(admitted, parentKey),
            agentId: "main",
            sessionKey: parentKey,
            personalToolParticipants: participants,
          }),
          {
            agentSessionKey: parentKey,
            agentChannel: "webchat",
            config: cfg,
            idempotencyKey: runId,
          },
          [
            "detached-reply",
            { sessionKey: childKey, message: "finish the accepted task", timeoutSeconds: 0 },
          ],
          client,
        );
        expect(result.details, JSON.stringify(result.details)).toMatchObject({
          status: "accepted",
          targetDisposition: "queued",
          delivery: { status: "skipped" },
        });
        await withinTest(childEntered.promise, signal);
        participants.close();
        admission.close();
        operator.release();
        unblock();
        await withinTest(Promise.all(executions), signal);
        // The target run finished, but nothing observed it to report back.
        expect(commandSessions).toEqual([childKey]);
        expect(detachedHolders).toEqual([]);
        // The answer stays with the target's own run for an explicit later read.
        const childRunId = (result.details as { runId?: string }).runId;
        await expect(
          dispatchGatewayMethodInProcess(
            "agent.wait",
            { runId: childRunId, timeoutMs: 1_000 },
            {
              forceSyntheticClient: true,
              syntheticScopes: ["operator.admin"],
              resolveGatewayContext: () => context,
            },
          ),
        ).resolves.toMatchObject({
          status: "ok",
          terminalReply: expect.objectContaining({ text: "delayed child result" }),
        });
      } finally {
        unblock();
        await Promise.allSettled(executions);
        participants.close();
        admission.close();
        operator.release();
        detachedObserver.mockRestore();
        executionSpy.mockRestore();
        signal.removeEventListener("abort", unblock);
      }
    },
  );

  it.for(["before staging", "after acceptance", "before acknowledgement"] as const)(
    "rechecks directed send policy at Gateway input admission (%s)",
    async (boundary, { signal }) => {
      const context = getContext();
      const previousConfig = context.getRuntimeConfig();
      const config: OpenClawConfig = {
        ...previousConfig,
        agents: {
          ...previousConfig.agents,
          ownership: "explicit",
          entries: {
            ...previousConfig.agents?.entries,
            main: { tools: { agentToAgent: { send: ["worker"] } } },
            worker: {},
          },
        },
        tools: {
          ...previousConfig.tools,
          sessions: { visibility: "self" },
          agentToAgent: { enabled: true, allow: ["main", "worker"] },
        },
      };
      const runId = randomUUID();
      // A subagent sender has no peer reply loop; the unrelated target still needs its send edge.
      const requesterKey = `agent:main:subagent:send-policy-${runId}`;
      const targetKey = `agent:worker:dashboard:send-policy-${runId}`;
      const sessionId = `send-policy-target-${runId}`;
      const message = "synthetic cross-agent input governed by current send policy";
      let sending: Promise<unknown> | undefined;
      let admission: ReturnType<typeof prepareAgentRunAdmission> | undefined;
      let held: Awaited<ReturnType<typeof holdInput>> | undefined;
      let restoreDispatch: (() => void) | undefined;
      const withdraw = () =>
        setRuntimeConfigSnapshot({
          ...config,
          agents: {
            ...config.agents,
            entries: { ...config.agents?.entries, main: { tools: { agentToAgent: { send: [] } } } },
          },
        });
      try {
        setRuntimeConfigSnapshot(config);
        await prepareGatewayReplyRuntimeForTest({ force: true, config });
        await sessionAccessor.upsertSessionEntryCore(
          { agentId: "main", sessionKey: requesterKey },
          { sessionId: requesterKey, updatedAt: 1, spawnDepth: 1 },
        );
        await sessionAccessor.upsertSessionEntryCore(
          { agentId: "worker", sessionKey: targetKey },
          { sessionId, updatedAt: 1 },
        );
        const scope = await inputScope(targetKey, sessionId, "worker");
        const transcript = sessionAccessor.loadTranscriptEventsSync(scope);
        admission = prepareAdmission(config, `send-policy-source-${runId}`, "send-policy-proof");
        const admitted = await admission.admit("embedded");
        held = await holdInput(scope, boundary === "before staging", signal);
        signal.addEventListener("abort", held.release, { once: true });
        if (boundary === "before acknowledgement") {
          const dispatch = gatewayDispatch.dispatchGatewayMethodInProcess;
          const spy = vi
            .spyOn(gatewayDispatch, "dispatchGatewayMethodInProcess")
            .mockImplementation(async <T>(...args: Parameters<typeof dispatch>): Promise<T> => {
              const response = await dispatch<T>(...args);
              if (args[0] === "agent") {
                withdraw();
              }
              return response;
            });
          restoreDispatch = () => spy.mockRestore();
        }
        const pending = send(
          () => caller(admitted, requesterKey),
          { agentSessionKey: requesterKey, config, idempotencyKey: runId },
          ["send-policy", { sessionKey: targetKey, mode: "followup", message, timeoutSeconds: 0 }],
        );
        sending = pending;
        await withinTest(
          awaitGateBeforeSettlement(
            held.staged.promise,
            pending,
            "Send ended before input staging",
          ),
          signal,
        );
        if (boundary !== "before staging") {
          expect((await withinTest(pending, signal)).details).toMatchObject({ status: "accepted" });
          await withinTest(held.entered, signal);
          expect((await listSessionPendingInputs(scope)).total).toBe(1);
        }
        withdraw();
        // Only the destination permission changed; the admitting caller is still live.
        expect(getAdmittedRunDelegatedAuthority(admitted)).toBeDefined();
        held.releaseWriter.resolve();
        if (boundary === "before staging") {
          const result = await withinTest(pending, signal);
          expect(result.details).toMatchObject({
            status: "error",
            error: expect.stringContaining("tools.agentToAgent.send"),
          });
          expect(result.details).not.toHaveProperty("sentBeforeError");
          expect(held.observer).not.toHaveBeenCalled();
          expect(agentCommandMock).not.toHaveBeenCalled();
          expect((await listSessionPendingInputs(scope)).total).toBe(0);
          expect(sessionAccessor.loadTranscriptEventsSync(scope)).toEqual(transcript);
        } else {
          const prepared = await held.entered;
          const recorder = expectDefined(prepared.userTurn.recorder, "accepted input recorder");
          const persisted = await recorder.withPendingInput!(() => recorder.persistApproved());
          expect(persisted?.appended).toBe(true);
          expect(persisted?.message.content).toContain(message);
          expect((await listSessionPendingInputs(scope)).total).toBe(0);
          await withinTest(held.resume(), signal);
          expect(agentCommandMock).toHaveBeenCalledOnce();
          expect(agentCommandMock.mock.calls[0]?.[0]).toMatchObject({ sessionKey: targetKey });
        }
      } finally {
        await held?.close(sending);
        restoreDispatch?.();
        admission?.close();
        setRuntimeConfigSnapshot(previousConfig);
        if (held) {
          signal.removeEventListener("abort", held.release);
        }
      }
    },
  );

  it.for([
    "before staging",
    "after acceptance",
    "child abort",
    "before reset",
    "live reset",
  ] as const)("keeps input authority at its current owner: %s", async (boundary, { signal }) => {
    await prepareGatewayReplyRuntimeForTest();
    const context = getContext();
    const runId = randomUUID();
    const parentKey = `agent:main:parent:${runId}`;
    const childKey = `agent:main:subagent:${runId}`;
    const sessionId = `child-${runId}`;
    await sessionAccessor.upsertSessionEntryCore(
      { agentId: "main", sessionKey: childKey },
      { sessionId, updatedAt: Date.now() },
    );
    const scope = await inputScope(childKey, sessionId);
    const admission = prepareAdmission(
      context.getRuntimeConfig(),
      `parent-${runId}`,
      "spawn-input-proof",
    );
    const admitted = await admission.admit("embedded");
    const guard = await withGatewayToolCallerIdentity(caller(admitted, parentKey), () =>
      captureAgentToolSourceExecutionGuard(),
    );
    if (boundary === "before reset" || boundary === "live reset") {
      const before = (await loadSessionEntryAsync(childKey, { agentId: "main" })).entry;
      let hookCalls = 0;
      const onReset = (event: import("../hooks/internal-hooks.js").InternalHookEvent) => {
        if (event.sessionKey !== childKey) {
          return;
        }
        hookCalls++;
        if (boundary === "before reset") {
          admission.close();
        }
      };
      registerInternalHook("command:new", onReset);
      try {
        const reset = dispatchGatewayMethodInProcess(
          "agent",
          { message: "/new", sessionKey: childKey, idempotencyKey: runId },
          {
            forceSyntheticClient: true,
            syntheticScopes: ["operator.admin"],
            resolveGatewayContext: () => context,
            sessionMutationCommitGuard: guard,
          },
        );
        if (boundary === "before reset") {
          await expect(reset).rejects.toThrow("tool invocation authority is no longer active");
          expect((await loadSessionEntryAsync(childKey, { agentId: "main" })).entry).toEqual(
            before,
          );
        } else {
          await expect(reset).resolves.toMatchObject({ status: "ok", summary: "completed" });
          const after = (await loadSessionEntryAsync(childKey, { agentId: "main" })).entry;
          expect(after?.sessionId).toBe(sessionId);
          expect(after?.lifecycleRevision).not.toBe(before?.lifecycleRevision);
        }
        expect(hookCalls).toBe(1);
      } finally {
        unregisterInternalHook("command:new", onReset);
        admission.close();
      }
      return;
    }
    const held = await holdInput(scope, boundary === "before staging", signal);
    signal.addEventListener("abort", held.release, { once: true });
    let dispatch: Promise<unknown> | undefined;
    try {
      dispatch = dispatchGatewayMethodInProcess(
        "agent",
        { message: "synthetic staged child input", sessionKey: childKey, idempotencyKey: runId },
        {
          forceSyntheticClient: true,
          resolveGatewayContext: () => context,
          sessionMutationCommitGuard: guard,
        },
      );
      const outcome = dispatch.then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      );
      await awaitGateBeforeSettlement(
        held.staged.promise,
        dispatch,
        "Dispatch ended before staging",
      );
      if (boundary === "before staging") {
        admission.close();
        held.releaseWriter.resolve();
        expect(await outcome).toHaveProperty(
          "error.message",
          "tool invocation authority is no longer active",
        );
        expect(held.observer).not.toHaveBeenCalled();
        expect((await listSessionPendingInputs(scope)).total).toBe(0);
      } else {
        expect(await outcome).toHaveProperty("value.status", "accepted");
        const prepared = await held.entered;
        const recorder = prepared.userTurn.recorder!;
        expect(recorder.getPendingInputMessage?.()).toBeDefined();
        admission.close();
        expect(() => guard()).toThrow("tool invocation authority is no longer active");
        if (boundary === "child abort") {
          prepared.activeRunAbort.controller.abort(new Error("child stopped"));
          expect(() => recorder.withPendingInput!(() => undefined)).toThrow(
            "agent input admission no longer owns this run",
          );
        } else {
          const persisted = await recorder.withPendingInput!(() => recorder.persistApproved());
          expect(persisted?.appended).toBe(true);
          expect(persisted?.message.content).toBe("synthetic staged child input");
          expect((await listSessionPendingInputs(scope)).total).toBe(0);
        }
      }
    } finally {
      await held.close(dispatch);
      admission.close();
      signal.removeEventListener("abort", held.release);
    }
  });
});
