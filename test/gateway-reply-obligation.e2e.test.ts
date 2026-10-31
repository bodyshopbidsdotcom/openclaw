// Real Gateway admission, in-process sessions_send, and HTTP provider completion.
import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import path from "node:path";
import { asOptionalRecord } from "@openclaw/normalization-core/record-coerce";
import { afterEach, describe, expect, it } from "vitest";
import { isInternalRuntimeContextCarrierText } from "../extensions/qa-lab/api.js";
import type { OpenClawConfig } from "../src/config/types.openclaw.js";
import { connectGatewayClient, disconnectGatewayClient } from "../src/gateway/test-helpers.e2e.js";
import { loadOrCreateDeviceIdentity } from "../src/infra/device-identity.js";
import { reserveTestPortListener } from "../src/test-utils/port-claims.js";
import {
  writeOpenAiResponsesSse,
  writeOpenAiResponsesText,
} from "./helpers/openai-responses-sse.js";
import {
  createOpenClawTestInstance,
  type OpenClawTestInstance,
} from "./helpers/openclaw-test-instance.js";
import { createDeferred, withinTest } from "./helpers/promise.js";
import { runQaGatewayFixture } from "./helpers/qa-gateway-cleanup.js";

const MODEL_REF = "reply-obligation/reply-obligation";
const REQUIRED_REQUEST = "REQUIRED_EMPTY_CONTROL";
const REQUIRED_RECOVERED = "REQUIRED_RECOVERED";
type ObservedEvent = { event?: string; payload?: unknown };
let instance: OpenClawTestInstance | undefined;
let fixture: Awaited<ReturnType<typeof startFixture>> | undefined;

function createScenario() {
  return {
    parentKey: "agent:main:reply-obligation",
    peerKey: "agent:main:reply-obligation-peer",
    parentRunId: randomUUID(),
    parentPrompt: "START_PARENT",
    parentFinal: "PARENT_FINAL",
    peerPrompt: "PEER_WORK",
    peerResult: "PEER_RESULT",
    peerStarted: createDeferred(),
    releasePeer: createDeferred(),
    peerRunId: undefined as string | undefined,
    parentRequests: 0,
    peerRequests: 0,
    callbackRequests: 0,
  };
}
type Scenario = ReturnType<typeof createScenario>;

afterEach(async () => {
  fixture?.releaseAll();
  await runQaGatewayFixture(
    async () => {},
    () => instance?.cleanup(),
    () => fixture?.close(),
  );
  instance = undefined;
  fixture = undefined;
});

describe("Gateway trusted reply obligations", () => {
  it(
    "keeps a late peer result in the peer session, and recovers a required empty reply",
    { timeout: 180_000 },
    async ({ signal }) => {
      const scenario = createScenario();
      const activeFixture = await startFixture(scenario);
      fixture = activeFixture;
      const profile = "reply-obligation-" + randomUUID();
      const gateway = await createOpenClawTestInstance({
        name: profile,
        entrypoint: ["scripts/run-node.mjs", "--profile", profile],
        config: createConfig(activeFixture.url),
        env: {
          OPENCLAW_PROFILE: profile,
          OPENCLAW_SKIP_PROVIDERS: undefined,
          OPENCLAW_TEST_MINIMAL_GATEWAY: undefined,
          OPENCLAW_AGENT_RUNTIME: undefined,
        },
        signal,
      });
      instance = gateway;
      // Keep the named service identity and logging inside the same disposable state.
      await gateway.state.writeConfig({
        ...createConfig(activeFixture.url),
        gateway: {
          port: gateway.port,
          auth: { mode: "token", token: gateway.gatewayToken },
          controlUi: { enabled: false },
        },
        logging: { file: path.join(gateway.stateDir, "gateway.log") },
      });
      const prepared = await gateway.cli(["--help"], { timeoutMs: 120_000 });
      expect(prepared.code, prepared.stderr).toBe(0);
      await gateway.startGateway();
      const events: ObservedEvent[] = [];
      const client = await connectGatewayClient({
        url: gateway.url,
        token: gateway.gatewayToken,
        role: "operator",
        scopes: ["operator.admin", "operator.read", "operator.write"],
        deviceIdentity: loadOrCreateDeviceIdentity({
          path: gateway.state.path("reply-obligation-client.sqlite"),
        }),
        signal,
        onEvent: (event) => {
          events.push(event);
        },
      });
      try {
        await client.request("sessions.subscribe", {});
        await client.request("sessions.create", { key: scenario.peerKey, agentId: "main" });
        await client.request("sessions.create", { key: scenario.parentKey, agentId: "main" });
        await client.request("sessions.messages.subscribe", {
          key: scenario.parentKey,
          subscriptionId: "reply-obligation",
        });
        const parent = await client.request(
          "agent",
          {
            sessionKey: scenario.parentKey,
            message: scenario.parentPrompt,
            channel: "webchat",
            deliver: false,
            idempotencyKey: scenario.parentRunId,
          },
          { expectFinal: true, timeoutMs: 60_000 },
        );
        expect(parent, gateway.logs()).toMatchObject({ status: "ok" });
        await withinTest(scenario.peerStarted.promise, signal);
        const before = await client.request<{ messages: unknown[] }>("chat.history", {
          sessionKey: scenario.parentKey,
          limit: 100,
        });
        expect(modelReplyTexts(before.messages)).toEqual([scenario.parentFinal]);

        // Release only after the original final is committed and publicly readable.
        scenario.releasePeer.resolve();
        expect(scenario.peerRunId).toBeTruthy();
        const settled = await client.request(
          "agent.wait",
          { runId: scenario.peerRunId, timeoutMs: 30_000 },
          { timeoutMs: 35_000 },
        );
        expect(settled, gateway.logs()).toMatchObject({ status: "ok" });
        const peerHistory = await client.request<{ messages: unknown[] }>("chat.history", {
          sessionKey: scenario.peerKey,
          limit: 100,
        });
        // The target records its own answer; nothing reports it back to the parent.
        expect(modelReplyTexts(peerHistory.messages)).toEqual([scenario.peerResult]);
        const after = await client.request<{ messages: unknown[] }>("chat.history", {
          sessionKey: scenario.parentKey,
          limit: 100,
        });
        expect(modelReplyTexts(after.messages)).toEqual([scenario.parentFinal]);
        expect(interSessionInputs(after.messages)).toEqual([]);
        expect(scenario.parentRequests).toBe(2);
        expect(scenario.peerRequests).toBe(1);
        expect(
          events.filter((event) => {
            const payload = asOptionalRecord(event.payload);
            const data = asOptionalRecord(payload?.data);
            return (
              payload?.sessionKey === scenario.parentKey &&
              ((event.event === "chat" && payload.state === "error") ||
                (event.event === "agent" && data?.phase === "error"))
            );
          }),
        ).toEqual([]);

        const requiredKey = "agent:main:reply-obligation-required";
        const required = await client.request(
          "agent",
          {
            sessionKey: requiredKey,
            message: REQUIRED_REQUEST,
            channel: "webchat",
            deliver: false,
            idempotencyKey: randomUUID(),
            // Public provenance is not the trusted owner's optional-reply decision.
            inputProvenance: { kind: "inter_session", sourceTool: "sessions_send" },
          },
          { expectFinal: true, timeoutMs: 60_000 },
        );
        expect(required, gateway.logs()).toMatchObject({ status: "ok" });
        expect(activeFixture.requiredRequests()).toBe(2);
        const requiredHistory = await client.request<{ messages: unknown[] }>("chat.history", {
          sessionKey: requiredKey,
          limit: 100,
        });
        expect(modelReplyTexts(requiredHistory.messages)).toEqual([REQUIRED_RECOVERED]);
        const requiredInputs = interSessionInputs(requiredHistory.messages);
        expect(requiredInputs).toHaveLength(1);
        expect(contentText(requiredInputs[0]?.content).trim()).toBe(REQUIRED_REQUEST);
        expect(requiredInputs[0]).toMatchObject({
          role: "assistant",
          provenance: { kind: "inter_session", sourceTool: "sessions_send" },
        });
        // Checked last so a late callback to the parent has had time to surface.
        expect(scenario.callbackRequests).toBe(0);
        expect(activeFixture.failures).toEqual([]);
        console.log(
          "[reply-obligation-proof] A late peer result stayed in the peer session; a required empty request recovered",
        );
      } finally {
        activeFixture.releaseAll();
        await disconnectGatewayClient(client);
      }
    },
  );
});

function contentText(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  return Array.isArray(content)
    ? content
        .map((part) => asOptionalRecord(part)?.text)
        .filter((text) => typeof text === "string")
        .join("\n")
    : "";
}

function interSessionInputs(messages: unknown[]): Record<string, unknown>[] {
  return messages.flatMap((message) => {
    const record = asOptionalRecord(message);
    return record && asOptionalRecord(record.provenance)?.kind === "inter_session" ? [record] : [];
  });
}

function modelReplyTexts(messages: unknown[]): string[] {
  return messages.flatMap((message) => {
    const record = asOptionalRecord(message);
    const text = contentText(record?.content).trim();
    // History presents forwarded peer input as an attributed assistant message.
    return record?.role === "assistant" &&
      asOptionalRecord(record.provenance)?.kind !== "inter_session" &&
      text
      ? [text]
      : [];
  });
}

function createConfig(url: string): OpenClawConfig {
  return {
    plugins: { enabled: false },
    discovery: { mdns: { mode: "off" } },
    agents: {
      defaults: {
        heartbeat: { every: "0m" },
        model: { primary: MODEL_REF },
        models: { [MODEL_REF]: { agentRuntime: { id: "openclaw" } } },
        skipBootstrap: true,
        skills: [],
        sandbox: { mode: "off" },
      },
    },
    tools: {
      profile: "coding",
      codeMode: false,
      toolSearch: false,
      allow: ["sessions_send"],
      sessions: { visibility: "all" },
      agentToAgent: { enabled: true },
    },
    models: {
      mode: "replace",
      providers: {
        "reply-obligation": {
          baseUrl: url + "/v1",
          apiKey: "test-placeholder",
          api: "openai-responses",
          request: { allowPrivateNetwork: true },
          models: [
            {
              id: "reply-obligation",
              name: "reply-obligation",
              api: "openai-responses",
              reasoning: false,
              input: ["text"],
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
              contextWindow: 128_000,
              maxTokens: 4096,
            },
          ],
        },
      },
    },
  };
}

async function startFixture(scenario: Scenario) {
  const failures: string[] = [];
  let sequence = 0;
  let requiredRequests = 0;
  const reserved = await reserveTestPortListener({
    offsets: [0],
    createListener: () =>
      createServer((request, response) => {
        void handle(request, response).catch((error: unknown) => {
          failures.push(String(error));
          response.writeHead(400, { "content-type": "application/json" });
          response.end(JSON.stringify({ error: { message: String(error) } }));
        });
      }),
  });
  async function handle(request: IncomingMessage, response: ServerResponse) {
    if (request.method !== "POST" || request.url !== "/v1/responses") {
      response.writeHead(404).end();
      return;
    }
    const chunks: Buffer[] = [];
    for await (const chunk of request) {
      chunks.push(Buffer.from(chunk));
    }
    const body = asOptionalRecord(JSON.parse(Buffer.concat(chunks).toString("utf8")));
    const input = Array.isArray(body?.input) ? body.input.map(asOptionalRecord) : [];
    const userTexts = input
      .filter((item) => item?.role === "user")
      .map((item) => contentText(item?.content))
      .filter((text) => !isInternalRuntimeContextCarrierText(text));
    const userText = userTexts.at(-1) ?? "";
    const id = String(++sequence);
    const final = (text: string) =>
      writeOpenAiResponsesText(response, {
        text,
        messageId: "msg_" + id,
        responseId: "resp_" + id,
      });
    // Search retained user turns too: an erroneous finalizer must count as a retry,
    // not become an unrelated fixture error merely because it appended a prompt.
    if (userTexts.some((text) => text.includes(REQUIRED_REQUEST))) {
      if (++requiredRequests > 2) {
        throw new Error("Required reply exceeded one empty response and one recovery");
      }
      final(requiredRequests === 1 ? "" : REQUIRED_RECOVERED);
      return;
    }
    if (userTexts.some((text) => text.includes(scenario.peerResult))) {
      scenario.callbackRequests += 1;
      throw new Error("A late peer result was reported back to its requester");
    }
    if (userText.includes(scenario.peerPrompt)) {
      if (++scenario.peerRequests !== 1) {
        throw new Error("Peer work was repeated");
      }
      scenario.peerStarted.resolve();
      await scenario.releasePeer.promise;
      final(scenario.peerResult);
      return;
    }
    const parent = userText.includes(scenario.parentPrompt) ? scenario : undefined;
    if (!parent || ++parent.parentRequests > 2) {
      throw new Error("Unexpected provider turn outside the finite reply-obligation scenario");
    }
    if (parent.parentRequests === 2) {
      const output = input.findLast((item) => item?.type === "function_call_output");
      const serialized = JSON.stringify(output?.output);
      if (!serialized?.includes("accepted") || !serialized.includes("skipped")) {
        throw new Error("Real sessions_send did not accept the peer send without later delivery");
      }
      parent.peerRunId = serialized.match(/\\?"runId\\?":\s*\\?"([^"\\]+)/)?.[1];
      final(parent.parentFinal);
      return;
    }
    const item = {
      type: "function_call",
      id: "fc_" + id,
      call_id: "call_" + id,
      name: "sessions_send",
      arguments: JSON.stringify({
        sessionKey: parent.peerKey,
        message: parent.peerPrompt,
        timeoutSeconds: 0,
      }),
      status: "completed",
    };
    writeOpenAiResponsesSse(response, [
      {
        type: "response.output_item.added",
        output_index: 0,
        item: { ...item, status: "in_progress", arguments: "" },
      },
      {
        type: "response.function_call_arguments.done",
        item_id: item.id,
        output_index: 0,
        arguments: item.arguments,
      },
      { type: "response.output_item.done", output_index: 0, item },
      {
        type: "response.completed",
        response: {
          id: "resp_" + id,
          status: "completed",
          output: [item],
          usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
        },
      },
    ]);
  }
  return {
    url: "http://127.0.0.1:" + reserved.claim.port,
    failures,
    requiredRequests: () => requiredRequests,
    releaseAll: () => scenario.releasePeer.resolve(),
    close: () =>
      runQaGatewayFixture(
        async () => {
          reserved.listener.closeAllConnections();
          await reserved.releaseListener();
        },
        () => reserved.claim.release(),
      ),
  };
}
