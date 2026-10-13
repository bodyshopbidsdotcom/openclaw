import path from "node:path";
import { expect, vi } from "vitest";
import {
  bindAgentToolGatewayRequest,
  type AgentToolGatewayRequestCaller,
} from "../agents/tools/in-process-gateway.js";
import { startSessionsSendSelfReply } from "../agents/tools/sessions-send-self-reply.js";
import { loadSessionEntry } from "../config/sessions/session-accessor.js";
import * as gatewayWork from "../process/gateway-work-admission.js";
import { createOutboundTestPlugin, createTestRegistry } from "../test-utils/channel-plugins.js";
import type { GatewayContextResolver } from "./server-methods/types.js";
import { setTestPluginRegistry, testState, writeSessionStore } from "./test-helpers.js";

export async function runDirectSessionReplyScenario(params: {
  dir: string;
  sessionKey: string;
  expectedAccountId: string | undefined;
  resolveGatewayContext: GatewayContextResolver;
}): Promise<void> {
  const { dir, sessionKey, expectedAccountId } = params;
  const sendCalls: Array<{
    to?: string;
    text?: string;
    accountId?: string | null;
  }> = [];
  const feishuPlugin = createOutboundTestPlugin({
    id: "feishu",
    label: "Feishu",
    outbound: {
      deliveryMode: "direct",
      resolveTarget: ({ to }) =>
        to?.startsWith("user:")
          ? { ok: true, to }
          : { ok: false, error: new Error("expected a direct user target") },
      sendText: async (ctx) => {
        sendCalls.push({ to: ctx.to, text: ctx.text, accountId: ctx.accountId });
        return { channel: "feishu", messageId: "direct-reply-proof" };
      },
    },
    messaging: {
      normalizeTarget: (raw) => raw,
      resolveDeliveryTarget: ({ conversationId }) => ({ to: `user:${conversationId}` }),
    },
  });
  setTestPluginRegistry(
    createTestRegistry([
      {
        pluginId: "feishu",
        source: "test",
        plugin: {
          ...feishuPlugin,
          config: {
            ...feishuPlugin.config,
            listAccountIds: () => ["default", "work"],
          },
        },
      },
    ]),
  );

  const storePath = path.join(dir, "sessions.json");
  const sessionId = `direct-reply-${expectedAccountId ?? "default"}-${sessionKey.includes(":dm:") ? "dm" : "direct"}`;
  testState.sessionStorePath = storePath;
  await writeSessionStore({
    entries: {
      [sessionKey]: { sessionId, updatedAt: Date.now() },
    },
  });
  const entry = loadSessionEntry({ sessionKey, storePath });
  expect(entry?.sessionId).toBe(sessionId);

  const gateway = bindAgentToolGatewayRequest({
    resolveGatewayContext: params.resolveGatewayContext,
  });
  // The self-send run is not executed here: its settled reply comes from the wait,
  // and the source delivery goes through the real Gateway send path.
  const callGateway = (async (request) =>
    request.method === "agent.wait"
      ? {
          status: "ok",
          terminalReply: { disposition: "visible", text: "same-session reply delivered" },
        }
      : await gateway(request)) as AgentToolGatewayRequestCaller;
  // The tool returns at observer admission; join the detached observer itself.
  const observers: Promise<unknown>[] = [];
  const continueDetached = gatewayWork.runWithGatewayDetachedWorkContinuation;
  const observe = vi
    .spyOn(gatewayWork, "runWithGatewayDetachedWorkContinuation")
    .mockImplementation((run, origin) => {
      const observer = continueDetached(run, origin);
      observers.push(observer);
      return observer;
    });
  try {
    await startSessionsSendSelfReply({
      callGateway,
      runId: "direct-reply-proof",
      agentId: "main",
      sessionKey,
      displayKey: sessionKey,
      requesterChannel: "feishu",
      requesterDeliveryGeneration: {
        agentId: "main",
        storePath,
        sessionKey,
        sessionId,
        lifecycleRevision: entry?.lifecycleRevision ?? null,
      },
      waitTimeoutMs: 5_000,
    });
    await Promise.all(observers);
  } finally {
    observe.mockRestore();
  }

  expect(sendCalls).toHaveLength(1);
  expect(sendCalls[0]).toMatchObject({
    to: "user:ou_reply_recipient",
    text: "same-session reply delivered",
    ...(expectedAccountId ? { accountId: expectedAccountId } : {}),
  });
}
