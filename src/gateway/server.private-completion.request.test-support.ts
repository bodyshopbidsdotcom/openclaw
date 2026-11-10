import { expectDefined } from "@openclaw/normalization-core/expect";
import { expect } from "vitest";
import type { AgentCommandOpts } from "../agents/command/types.js";
import type { UserTurnTranscriptRecorder } from "../sessions/user-turn-transcript.types.js";

export function createPrivateCompletionRequest({
  sessionKey,
  sessionId,
  runId,
  message = "Synthetic private child marker",
}: {
  sessionKey: string;
  sessionId: string;
  runId: string;
  message?: string;
}) {
  return {
    sessionKey,
    expectedExistingSessionId: sessionId,
    idempotencyKey: runId,
    message,
    deliver: false,
    sourceReplyDeliveryMode: "automatic",
    inputProvenance: {
      kind: "inter_session",
      sourceTool: "subagent_announce",
      sourceSessionKey: "agent:main:subagent:synthetic-child",
    },
  };
}

export function readPrivateCompletionRecorder(
  input: unknown,
  sessionId: string,
): UserTurnTranscriptRecorder {
  const command = input as AgentCommandOpts;
  expect(command.deliver).toBe(false);
  expect(command.privateCompletion).toBe(true);
  expect(command.sessionId).toBe(sessionId);
  return expectDefined(command.userTurnTranscriptRecorder, "Expected real private input recorder");
}
