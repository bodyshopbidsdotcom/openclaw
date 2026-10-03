// Final-reply routing for a thread session reached through sessions_send dispatch facts.
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { CliDeps } from "../../cli/outbound-send-deps.js";
import type { OpenClawConfig } from "../../config/config.js";
import { setActivePluginRegistry } from "../../plugins/runtime.js";
import { createOutboundTestPlugin, createTestRegistry } from "../../test-utils/channel-plugins.js";
import { normalizeSessionDeliveryState } from "../../utils/delivery-context.shared.js";
import { deliverAgentCommandResult } from "./delivery.js";
import type { AgentCommandOpts } from "./types.js";

const deliverOutboundPayloadsMock = vi.hoisted(() =>
  vi.fn(async (..._args: unknown[]) => [{ channel: "slack", messageId: "msg-1" }] as unknown[]),
);
vi.mock("../../infra/outbound/deliver.js", () => ({
  deliverOutboundPayloads: deliverOutboundPayloadsMock,
  deliverOutboundPayloadsInternal: deliverOutboundPayloadsMock,
}));

const THREAD_SESSION_KEY = "agent:tester:slack:channel:C123:thread:1710000000.000100";

beforeEach(() => {
  deliverOutboundPayloadsMock.mockClear();
  setActivePluginRegistry(
    createTestRegistry([
      {
        pluginId: "slack",
        source: "test",
        plugin: createOutboundTestPlugin({
          id: "slack",
          outbound: {
            deliveryMode: "direct",
            sendText: async ({ to }) => ({ channel: "slack", messageId: to }),
          },
        }),
      },
    ]),
  );
});

afterEach(() => {
  setActivePluginRegistry(createTestRegistry([]));
});

it.each([
  { name: "a delivered thread turn", opts: {}, delivered: true },
  {
    name: "a sessions_send turn",
    opts: {
      deliver: false,
      channel: "webchat",
      messageChannel: "webchat",
      sourceReplyDeliveryMode: "message_tool_only",
    },
    delivered: false,
  },
] as const)("routes the final reply of $name on a Slack thread session", async (testCase) => {
  await deliverAgentCommandResult({
    cfg: {} as OpenClawConfig,
    deps: {} as CliDeps,
    runtime: { log: vi.fn(), error: vi.fn() } as never,
    opts: {
      message: "context from a peer",
      deliver: true,
      sessionKey: THREAD_SESSION_KEY,
      ...testCase.opts,
    } as AgentCommandOpts,
    outboundSession: { key: THREAD_SESSION_KEY, agentId: "tester" } as never,
    sessionEntry: {
      sessionId: "thread-session",
      updatedAt: 1,
      delivery: normalizeSessionDeliveryState({
        context: { channel: "slack", to: "channel:C123", threadId: "1710000000.000100" },
      }),
    },
    payloads: [{ text: "final answer" }],
    result: { meta: { durationMs: 1 } },
  });

  // The stored route points at the thread; sessions_send dispatch facts keep the
  // final reply off it so only an explicit `message` call can post there.
  expect(deliverOutboundPayloadsMock).toHaveBeenCalledTimes(testCase.delivered ? 1 : 0);
  if (testCase.delivered) {
    expect(deliverOutboundPayloadsMock.mock.calls[0]?.[0]).toMatchObject({
      channel: "slack",
      to: "channel:C123",
      threadId: "1710000000.000100",
    });
  }
});
