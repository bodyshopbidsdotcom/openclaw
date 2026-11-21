import type { OpenClawConfig } from "../config/config.js";
import { setActivePluginRegistry } from "../plugins/runtime.js";
import { callGatewayMock } from "./openclaw-tools.sessions.mocks.test-support.js";
import { createSessionsChannelTestRegistry } from "./tools/sessions-channel-fixture.test-support.js";
import { createSessionsHistoryTool } from "./tools/sessions-history-tool.js";
import { createSessionsListTool } from "./tools/sessions-list-tool.js";
import { createSessionsSendTool } from "./tools/sessions-send-tool.js";

export const TEST_CONFIG = {
  session: { mainKey: "main", scope: "per-sender" },
  tools: { sessions: { visibility: "all" }, agentToAgent: { enabled: true } },
} as OpenClawConfig;

export function installMessagingTestRegistry() {
  setActivePluginRegistry(
    createSessionsChannelTestRegistry([
      { id: "discord", label: "Discord", chatTypes: ["direct", "channel", "thread"] },
      {
        id: "whatsapp",
        label: "WhatsApp",
        chatTypes: ["direct", "group"],
        preferSessionLookupForAnnounceTarget: true,
      },
    ]),
  );
}

export function getSessionTool(
  name: "sessions_list" | "sessions_history" | "sessions_send",
  options?: {
    agentSessionKey?: string;
    agentChannel?: string;
    sandboxed?: boolean;
    config?: OpenClawConfig;
  },
) {
  return {
    sessions_list: createSessionsListTool,
    sessions_history: createSessionsHistoryTool,
    sessions_send: createSessionsSendTool,
  }[name]({
    ...options,
    agentChannel: options?.agentChannel as never,
    config: options?.config ?? TEST_CONFIG,
    callGateway: callGatewayMock,
  });
}

export type GatewayCall = { method?: string; params?: Record<string, unknown> };
