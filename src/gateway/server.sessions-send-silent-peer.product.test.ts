import { expect, it } from "vitest";
import { readRequestText } from "../../test/helpers/gateway-integration-fixtures.js";
import { startOpenAiResponsesModelServer } from "../../test/helpers/openai-responses-model-fixture.js";
import { writeOpenAiResponsesText } from "../../test/helpers/openai-responses-sse.js";
import { createOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { writeToolCall } from "./gateway-provider-fixture.test-support.js";
import { createSubagentModelConfig } from "./server.subagent-stop-settlement.product.test-support.js";
import { disconnectGatewayClient, startGatewayWithClient } from "./test-helpers.e2e.js";

it("lets a sessions_send peer turn end with NO_REPLY through the real provider", async () => {
  const sourcePrompt = "Ask the main session to file the report.";
  const peerTask = "PEER_TASK_file_the_report";
  const peerRequests: string[] = [];
  const requests: string[] = [];
  let sourceToolOutput = "";
  let sequence = 0;
  const model = await startOpenAiResponsesModelServer({
    modelId: "silent-peer",
    bindError: "Expected silent peer model server address",
    onRequest: async (request, response) => {
      const body = await readRequestText(request);
      requests.push(body);
      const reply = (text: string) => {
        const id = `peer_${++sequence}`;
        writeOpenAiResponsesText(response, { text, messageId: id, responseId: `${id}_response` });
      };
      if (body.includes(sourcePrompt)) {
        if (!body.includes("function_call_output")) {
          writeToolCall(response, "sessions_send", {
            sessionKey: "main",
            message: peerTask,
            timeoutSeconds: 30,
          });
        } else {
          sourceToolOutput = body;
          reply("Source done.");
        }
      } else if (body.includes(peerTask)) {
        peerRequests.push(body);
        // The peer already delivered its result elsewhere; bound a forced answer on base.
        reply(peerRequests.length === 1 ? "NO_REPLY" : "Forced peer answer.");
      } else {
        reply("Unexpected request.");
      }
    },
  });
  const state = await createOpenClawTestState({ label: "silent-sessions-send-peer" });
  const cfg = createSubagentModelConfig(state.workspaceDir, {
    baseUrl: `${model.baseUrl}/v1`,
    modelId: "silent-peer",
    runtime: true,
  });
  const gateway = await startGatewayWithClient({
    cfg: { ...cfg, tools: { ...cfg.tools, sessions: { visibility: "all" } } },
    configPath: state.configPath,
    token: "silent-peer-token",
  });
  try {
    await gateway.server.startupSettled;
    await gateway.client.request(
      "agent",
      {
        sessionKey: "agent:main:peer-source",
        message: sourcePrompt,
        idempotencyKey: "silent-peer-source",
        deliver: false,
      },
      { expectFinal: true },
    );
    expect(peerRequests).toHaveLength(1);
    expect(requests.some((body) => body.includes("NO_REPLY is not allowed"))).toBe(false);
    // The requester sees a completed send, not a failed or forced peer turn.
    expect(sourceToolOutput).toContain(peerTask);
    expect(sourceToolOutput).not.toContain("incomplete_turn");
    expect(sourceToolOutput).not.toContain("Forced peer answer.");
  } finally {
    await disconnectGatewayClient(gateway.client);
    await gateway.server.close({ reason: "silent peer proof complete" });
    await model.close();
    await state.cleanup();
  }
});
