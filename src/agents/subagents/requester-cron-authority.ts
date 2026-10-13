import { AsyncLocalStorage } from "node:async_hooks";
import { getRuntimeConfig } from "../../config/config.js";
import type { PreparedSessionMutationFacts } from "../../gateway/session-sharing-policy.js";
import {
  prepareSessionMutationFacts,
  type SessionFactsRead,
} from "../../gateway/session-sharing-preparation.js";
import { getAgentRunLifecycleGeneration } from "../../infra/agent-run-registry.js";
import type { InputProvenance } from "../../sessions/input-provenance.js";
import { resolveGlobalSingleton } from "../../shared/global-singleton.js";
import {
  captureActiveCronManagementAuthority,
  type CronCreatorAuthorityCapability,
} from "../cron-creator-authority-context.js";
import type { SubagentRunRecord } from "./registry/subagent-registry.types.js";
import {
  sameRequesterSettleBatch,
  isRequesterYieldCohortMember,
  resolveCurrentRequesterSettleBatch,
} from "./registry/subagent-requester-settle-identity.js";
import {
  getSubagentRunRuntimeKey,
  isSameSubagentRunOwner,
} from "./registry/subagent-run-generation.js";

type RequesterCronAuthority = {
  managementEntitlement?: NonNullable<CronCreatorAuthorityCapability["managementEntitlement"]>;
  requesterOwner?: CronCreatorAuthorityCapability["requesterOwner"];
  requesterSessionKey: string;
  requesterSessionId: string;
  requesterAgentId: string;
  requesterTurnRunId: string;
  lifecycleGeneration: string;
  sessionLifecycleRevision?: string;
  admittedRunId?: string;
  runScopeBound?: true;
  active: boolean;
  sessionFacts: SessionFactsRead<PreparedSessionMutationFacts>;
  runs: ReadonlyMap<string, SubagentRunRecord>;
  batch: readonly SubagentRunRecord[];
  rearmGeneration?: number;
};

type RequesterCronAuthorityState = {
  byEntry: WeakMap<object, RequesterCronAuthority>;
  bySession: Map<string, Set<RequesterCronAuthority>>;
};

const state = resolveGlobalSingleton<RequesterCronAuthorityState>(
  Symbol.for("openclaw.subagents.requesterCronAuthority"),
  () => ({ byEntry: new WeakMap(), bySession: new Map() }),
  (value) => {
    for (const entries of value.bySession.values()) {
      for (const entry of entries) {
        entry.active = false;
        entry.sessionFacts.release();
      }
    }
    value.byEntry = new WeakMap();
    value.bySession.clear();
  },
);

function discard(authority: RequesterCronAuthority): void {
  authority.active = false;
  authority.sessionFacts.release();
  for (const entry of authority.batch) {
    if (state.byEntry.get(getSubagentRunRuntimeKey(entry)) === authority) {
      state.byEntry.delete(getSubagentRunRuntimeKey(entry));
    }
  }
  const session = state.bySession.get(authority.requesterSessionKey);
  session?.delete(authority);
  if (session?.size === 0) {
    state.bySession.delete(authority.requesterSessionKey);
  }
}

function isCurrent(authority: RequesterCronAuthority): boolean {
  if (
    !authority.active ||
    (authority.managementEntitlement?.source === "channel-owner" &&
      !authority.managementEntitlement.isCurrent()) ||
    authority.lifecycleGeneration !== getAgentRunLifecycleGeneration() ||
    !state.bySession.get(authority.requesterSessionKey)?.has(authority)
  ) {
    return false;
  }
  let session: PreparedSessionMutationFacts["target"];
  try {
    session = authority.sessionFacts.readCurrent(getRuntimeConfig()).target;
  } catch {
    return false;
  }
  if (
    session?.entry.sessionId !== authority.requesterSessionId ||
    session.entry.lifecycleRevision !== authority.sessionLifecycleRevision ||
    session.entry.archivedAt !== undefined
  ) {
    return false;
  }
  if (authority.runScopeBound) {
    return true;
  }
  const batch = resolveCurrentRequesterSettleBatch(authority.batch, authority.runs);
  if (
    !batch ||
    batch.some(
      (entry) =>
        entry.killIntent?.suppressTaskDelivery === true ||
        entry.killReconciliation?.suppressTaskDelivery === true,
    ) ||
    batch.every((entry) => entry.suppressCompletionDelivery === true)
  ) {
    return false;
  }
  const batchRunIds = authority.batch.map((entry) => entry.runId).toSorted();
  return batch.every(
    (entry) =>
      state.byEntry.get(getSubagentRunRuntimeKey(entry)) === authority &&
      (authority.rearmGeneration === undefined ||
        isRequesterYieldCohortMember(entry, batchRunIds, authority.rearmGeneration)),
  );
}

export type PreparedRequesterCronAuthority = {
  assertCurrent(): void;
  validate(): Promise<void>;
  bind(params: {
    batch: readonly SubagentRunRecord[];
    runs: ReadonlyMap<string, SubagentRunRecord>;
  }): Promise<{ commit(): void; revoke(): void } | undefined>;
  release(): void | Promise<void>;
};

/** Capture automation management before registry hydration can yield. */
export function prepareRequesterCronAuthority(params: {
  requesterSessionKey: string;
  requesterAgentId?: string;
  requesterTurnRunId: string;
}): PreparedRequesterCronAuthority | undefined {
  const requesterAgentId = params.requesterAgentId;
  if (!requesterAgentId) {
    return undefined;
  }
  const capture = captureActiveCronManagementAuthority({
    runId: params.requesterTurnRunId,
    sessionKey: params.requesterSessionKey,
    agentId: requesterAgentId,
  });
  if (!capture) {
    return undefined;
  }
  const preparedFacts = prepareSessionMutationFacts({
    cfg: getRuntimeConfig(),
    sessionKey: params.requesterSessionKey,
    agentId: requesterAgentId,
    allowMissing: true,
  });
  // Hydration can fail before this accepted preparation is consumed.
  let readyFacts: SessionFactsRead<PreparedSessionMutationFacts> | undefined;
  void preparedFacts.then(
    (facts) => {
      readyFacts = facts;
    },
    () => {},
  );
  let consumed = false;
  let released = false;
  let boundAuthority: RequesterCronAuthority | undefined;
  const assertCurrent = () => {
    if (!capture.isActive()) {
      throw new Error("Requester authority retired during session preparation");
    }
    if ((released && !boundAuthority) || boundAuthority?.active === false) {
      throw new Error("Requester authority retired before yield handoff");
    }
    if (readyFacts) {
      const current = readyFacts.readCurrent(getRuntimeConfig()).target;
      if (
        current?.entry.sessionId !== capture.sessionId ||
        current.entry.archivedAt !== undefined ||
        (boundAuthority &&
          current.entry.lifecycleRevision !== boundAuthority.sessionLifecycleRevision)
      ) {
        throw new Error("Requester session authority changed before yield handoff");
      }
    }
  };
  const releaseFacts = (sessionFacts?: SessionFactsRead<PreparedSessionMutationFacts>) => {
    if (!boundAuthority) {
      sessionFacts?.release();
    }
  };
  return {
    assertCurrent,
    async validate() {
      await preparedFacts;
      assertCurrent();
    },
    async bind({ batch, runs }) {
      if (consumed || released) {
        throw new Error("Requester authority preparation was already consumed");
      }
      consumed = true;
      const sessionFacts = await preparedFacts;
      assertCurrent();
      const session = sessionFacts.readCurrent(getRuntimeConfig()).target;
      if (
        batch.length === 0 ||
        session?.entry.sessionId !== capture.sessionId ||
        session.entry.archivedAt !== undefined
      ) {
        return undefined;
      }
      const authority: RequesterCronAuthority = {
        ...params,
        requesterAgentId,
        requesterSessionId: capture.sessionId,
        managementEntitlement: capture.managementEntitlement,
        requesterOwner: capture.requesterOwner,
        lifecycleGeneration: capture.lifecycleGeneration,
        sessionLifecycleRevision: session.entry.lifecycleRevision,
        sessionFacts,
        runs,
        batch: [...batch],
        active: true,
      };
      boundAuthority = authority;
      const sessionAuthorities = state.bySession.get(authority.requesterSessionKey) ?? new Set();
      sessionAuthorities.add(authority);
      state.bySession.set(authority.requesterSessionKey, sessionAuthorities);
      return {
        commit: () => {
          assertCurrent();
          const committedBatch = resolveCurrentRequesterSettleBatch(authority.batch, runs);
          if (!committedBatch) {
            throw new Error("Requester automation authority lost its committed child owner");
          }
          authority.batch = committedBatch;
          for (const entry of authority.batch) {
            const previous = state.byEntry.get(getSubagentRunRuntimeKey(entry));
            if (previous && previous !== authority) {
              discard(previous);
            }
            state.byEntry.set(getSubagentRunRuntimeKey(entry), authority);
          }
        },
        revoke: () => discard(authority),
      };
    },
    release() {
      if (released) {
        return undefined;
      }
      released = true;
      if (readyFacts) {
        releaseFacts(readyFacts);
        return undefined;
      }
      return preparedFacts.then(releaseFacts, () => releaseFacts());
    },
  };
}

/** The committed complete cohort, rather than a child result, owns continuation authority. */
export function promoteRequesterCronAuthority(params: {
  requesterTurnRunId: string;
  batch: readonly SubagentRunRecord[];
  rearmGeneration?: number;
}): void {
  const authority = params.batch[0] && state.byEntry.get(getSubagentRunRuntimeKey(params.batch[0]));
  if (!authority) {
    return;
  }
  if (
    params.rearmGeneration === undefined ||
    authority.requesterTurnRunId !== params.requesterTurnRunId ||
    !sameRequesterSettleBatch(authority.batch, params.batch) ||
    !isCurrent(authority)
  ) {
    discard(authority);
    return;
  }
  authority.rearmGeneration = params.rearmGeneration;
  if (!isCurrent(authority)) {
    discard(authority);
  }
}

/** Preserve only the registry's committed same-task replacement and its remapped cohort. */
export function replaceRequesterCronAuthorityEntry(params: {
  previous: SubagentRunRecord;
  next: SubagentRunRecord;
  preserve: boolean;
}): void {
  const authority = state.byEntry.get(getSubagentRunRuntimeKey(params.previous));
  if (!authority) {
    return;
  }
  if (!params.preserve) {
    discard(authority);
    return;
  }
  authority.batch = authority.batch.map((entry) =>
    isSameSubagentRunOwner(entry, params.previous) ? params.next : entry,
  );
  state.byEntry.delete(getSubagentRunRuntimeKey(params.previous));
  state.byEntry.set(getSubagentRunRuntimeKey(params.next), authority);
  if (!isCurrent(authority)) {
    discard(authority);
  }
}

/** A new direct user turn cannot lend its identity to an older pending batch. */
export function revokeRequesterCronAuthority(sessionKey: string): void {
  for (const authority of state.bySession.get(sessionKey) ?? []) {
    discard(authority);
  }
}

/** Committed outbox cleanup releases only its exact generation, including cancelled batches. */
export function revokeRequesterCronAuthorityBatch(
  batch: readonly SubagentRunRecord[],
  rearmGeneration: number | undefined,
): void {
  if (rearmGeneration === undefined) {
    return;
  }
  for (const entry of batch) {
    const authority = state.byEntry.get(getSubagentRunRuntimeKey(entry));
    if (authority && authority.rearmGeneration === rearmGeneration) {
      if (
        isSameSubagentRunOwner(authority.runs.get(entry.runId), entry) &&
        entry.pauseReason === "sessions_yield" &&
        entry.requesterSettleWake?.rearmGeneration === rearmGeneration &&
        isCurrent(authority)
      ) {
        continue;
      }
      discard(authority);
    }
  }
}

type RequesterCronAuthorityDispatch = {
  authority: RequesterCronAuthority;
  runId: string;
  isCurrent: () => boolean;
  consumed: boolean;
  pause?: { entry: SubagentRunRecord; scope?: CronCreatorAuthorityCapability; released?: true };
};
const activeDispatch = new AsyncLocalStorage<RequesterCronAuthorityDispatch>();

export async function withRequesterCronAuthority<T>(
  params: {
    requesterSessionKey: string;
    requesterSessionId: string;
    requesterAgentId?: string;
    batch: readonly SubagentRunRecord[];
    rearmGeneration: number | undefined;
    runId: string;
    isCurrent: () => boolean;
  },
  run: () => Promise<T>,
): Promise<T> {
  const authority = params.batch[0] && state.byEntry.get(getSubagentRunRuntimeKey(params.batch[0]));
  const child = params.batch.length === 1 ? params.batch[0] : undefined;
  const pause: RequesterCronAuthorityDispatch["pause"] =
    child?.pauseReason === "sessions_yield" && child.requesterSettleWake?.pauseNotice
      ? { entry: child }
      : undefined;
  if (
    !authority ||
    authority.requesterSessionKey !== params.requesterSessionKey ||
    authority.requesterSessionId !== params.requesterSessionId ||
    authority.requesterAgentId !== params.requesterAgentId ||
    authority.rearmGeneration === undefined ||
    authority.rearmGeneration !== params.rearmGeneration ||
    !(pause
      ? authority.batch.some((entry) => isSameSubagentRunOwner(entry, pause.entry))
      : sameRequesterSettleBatch(authority.batch, params.batch))
  ) {
    return await run();
  }
  const current = () => {
    const pausedEntry = pause && authority.runs.get(pause.entry.runId);
    return (
      isCurrent(authority) &&
      (pause
        ? !pause.released &&
          (pause.scope
            ? pause.scope.active && !pause.scope.signal.aborted
            : isSameSubagentRunOwner(pausedEntry, pause.entry) &&
              pausedEntry?.pauseReason === "sessions_yield" &&
              Boolean(pausedEntry.requesterSettleWake?.pauseNotice) &&
              params.isCurrent())
        : authority.runScopeBound === true || params.isCurrent())
    );
  };
  if (!current()) {
    discard(authority);
    return await run();
  }
  const dispatch: RequesterCronAuthorityDispatch = {
    authority,
    runId: params.runId,
    isCurrent: current,
    consumed: false,
    pause,
  };
  try {
    return await activeDispatch.run(dispatch, run);
  } finally {
    // The committed settlement owner retires this cohort. A returned delivery
    // failure can still need a retry, just like a thrown transport error.
    if (!isCurrent(authority)) {
      discard(authority);
    }
  }
}

type RequesterAdmissionTarget = {
  runId: string;
  sessionKey: string | undefined;
  sessionId: string | undefined;
  inputProvenance: InputProvenance | undefined;
};

function matchesAdmissionTarget(
  dispatch: RequesterCronAuthorityDispatch,
  params: RequesterAdmissionTarget,
): boolean {
  const { authority } = dispatch;
  return (
    dispatch.runId === params.runId &&
    authority.requesterSessionKey === params.sessionKey &&
    authority.requesterSessionId === params.sessionId &&
    params.inputProvenance?.kind === "inter_session" &&
    params.inputProvenance.sourceTool === "subagent_settle" &&
    authority.batch.some(
      (entry) => entry.childSessionKey === params.inputProvenance?.sourceSessionKey,
    )
  );
}

export function captureRequesterCronAuthorityAdmissionAssertion(params: RequesterAdmissionTarget) {
  const dispatch = activeDispatch.getStore();
  if (!dispatch || dispatch.consumed) {
    return undefined;
  }
  if (!matchesAdmissionTarget(dispatch, params)) {
    throw new Error("Requester authority does not own this continuation");
  }
  // Storage can invoke the pre-commit guard outside this dispatch's async context.
  return () => {
    if (!dispatch.consumed && !dispatch.isCurrent()) {
      throw new Error("Requester authority is no longer current");
    }
  };
}

export function consumeRequesterCronAuthorityAdmission(params: RequesterAdmissionTarget):
  | {
      runId: string;
      callerOrigin: { kind: "unknown" };
      managementEntitlement: NonNullable<CronCreatorAuthorityCapability["managementEntitlement"]>;
      requesterOwner?: CronCreatorAuthorityCapability["requesterOwner"];
      isCurrent: () => boolean;
      bindRunScope: (scope: CronCreatorAuthorityCapability) => void;
      release?: () => void;
    }
  | undefined {
  const dispatch = activeDispatch.getStore();
  if (
    !dispatch ||
    dispatch.consumed ||
    dispatch.authority.admittedRunId !== undefined ||
    !matchesAdmissionTarget(dispatch, params) ||
    !dispatch.isCurrent()
  ) {
    return undefined;
  }
  dispatch.consumed = true;
  if (!dispatch.authority.managementEntitlement) {
    return undefined;
  }
  if (!dispatch.pause) {
    dispatch.authority.admittedRunId = params.runId;
  }
  return {
    runId: params.runId,
    callerOrigin: { kind: "unknown" },
    managementEntitlement: dispatch.authority.managementEntitlement,
    requesterOwner: dispatch.authority.requesterOwner,
    isCurrent: dispatch.isCurrent,
    ...(dispatch.pause
      ? {
          release: () => {
            if (dispatch.pause) {
              dispatch.pause.released = true;
            }
          },
        }
      : {}),
    bindRunScope: (scope) => {
      if (
        dispatch.authority.runScopeBound ||
        dispatch.pause?.scope ||
        !dispatch.isCurrent() ||
        scope.runId !== params.runId ||
        scope.isCurrent !== dispatch.isCurrent ||
        scope.managementEntitlement !== dispatch.authority.managementEntitlement ||
        scope.requesterOwner !== dispatch.authority.requesterOwner ||
        scope.callerOrigin.kind !== "unknown" ||
        !scope.active ||
        scope.signal.aborted
      ) {
        throw new Error("Requester automation authority no longer owns this run scope");
      }
      if (dispatch.pause) {
        // Admission owns this turn even after notice consumption; final custody stays with the cohort.
        dispatch.pause.scope = scope;
        return;
      }
      // Queue acceptance can retire the child outbox before the parent finishes.
      // Its fresh run scope now owns the entitlement and all per-operation grants.
      dispatch.authority.runScopeBound = true;
      for (const entry of dispatch.authority.batch) {
        if (state.byEntry.get(getSubagentRunRuntimeKey(entry)) === dispatch.authority) {
          state.byEntry.delete(getSubagentRunRuntimeKey(entry));
        }
      }
      dispatch.authority.batch = [];
      scope.signal.addEventListener("abort", () => discard(dispatch.authority), { once: true });
    },
  };
}
