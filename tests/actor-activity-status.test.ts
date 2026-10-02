import "./fixtures/conversation-host.js";
import type { Theme } from "@earendil-works/pi-coding-agent";
import type { TUI } from "@earendil-works/pi-tui";
import { describe, expect, it, vi } from "vitest";
import { isActiveStatus } from "../src/ui/types.js";
import { matchesFilter } from "../src/ui/dashboard-model.js";
import { FabricConversationState, FabricConversationView } from "../src/ui/conversation.js";
import { nativeTranscript } from "./fixtures/native-conversation.js";

const theme = { fg: (_color: string, text: string) => text, bg: (_color: string, text: string) => text,
  bold: (text: string) => text, italic: (text: string) => text, underline: (text: string) => text,
  strikethrough: (text: string) => text } as unknown as Theme;

describe("accepted workerless actor activity (#3167)", () => {
  it.each(["preparing", "waiting"])("classifies %s as active and retains it in the dashboard Active filter/count", (status) => {
    expect(isActiveStatus(status)).toBe(true);
    expect(matchesFilter(status, "active")).toBe(true);
    expect(matchesFilter(status, "completed")).toBe(false);
    expect(matchesFilter(status, "failed")).toBe(false);
    expect([status, "idle", "stopped"].filter(isActiveStatus)).toHaveLength(1);
  });

  it.each(["preparing", "waiting"])("shows the conversation Working indicator for %s without a worker", (status) => {
    const target = { id: "actor", name: "accepted actor", kind: "actor" as const, status, canSteer: true, canFollowUp: true, canStop: true };
    const view = new FabricConversationView({ terminal: { rows: 22, columns: 120 }, requestRender: vi.fn() } as unknown as TUI,
      theme, { state: new FabricConversationState(), targets: () => [target], initialTargetId: target.id,
        transcript: () => nativeTranscript(), loadOlder: () => false, loadNewer: () => false, loadLatest: () => false,
        send: vi.fn(), stop: vi.fn(), close: vi.fn() });
    try {
      expect(view.render(120).join("\n")).toContain("Working");
      target.status = "idle";
      view.invalidate();
      expect(view.render(120).join("\n")).not.toContain("Working");
    } finally { view.dispose(); }
  });
});
