import path from "node:path";
import { expect, vi } from "vitest";
import { runSessionsSendSelfReply } from "../agents/tools/sessions-send-tool.self-reply.js";
import { createOutboundTestPlugin, createTestRegistry } from "../test-utils/channel-plugins.js";
import { setTestPluginRegistry, testState, writeSessionStore } from "./test-helpers.js";

export async function runDirectSessionAnnounceScenario(params: {
  dir: string;
  sessionKey: string;
  expectedAccountId: string | undefined;
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
        return { channel: "feishu", messageId: "direct-announce-proof" };
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

  testState.sessionStorePath = path.join(dir, "sessions.json");
  await writeSessionStore({
    entries: {
      [sessionKey]: {
        sessionId: `direct-announce-${expectedAccountId ?? "default"}-${sessionKey.includes(":dm:") ? "dm" : "direct"}`,
        updatedAt: Date.now(),
      },
    },
  });
  await runSessionsSendSelfReply({
    targetAgentId: "main",
    targetSessionKey: sessionKey,
    displayKey: sessionKey,
    announceTimeoutMs: 5_000,
    roundOneReply: "direct announcement delivered",
  });

  await vi.waitFor(
    () => {
      expect(sendCalls).toHaveLength(1);
      expect(sendCalls[0]).toMatchObject({
        to: "user:ou_announce_recipient",
        text: "direct announcement delivered",
        ...(expectedAccountId ? { accountId: expectedAccountId } : {}),
      });
    },
    { timeout: 5_000 },
  );
}
