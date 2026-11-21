import type { InputProvenance } from "../../sessions/input-provenance.js";

export type RequesterAdmissionTarget = {
  runId: string;
  sessionKey: string | undefined;
  sessionId: string | undefined;
  inputProvenance: InputProvenance | undefined;
};

/** Match an admitted continuation to its exact requester authority source. */
export function matchesRequesterCronAuthorityAdmission(
  dispatch: {
    runId: string;
    authority: {
      requesterSessionKey: string;
      requesterSessionId: string;
      batch: readonly { childSessionKey: string }[];
    };
  },
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
