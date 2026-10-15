import type { Theme } from "@earendil-works/pi-coding-agent";
import { visibleWidth } from "@earendil-works/pi-tui";
import { beforeAll, describe, expect, it } from "vitest";
import { projectAgentTranscript, type FabricAgentTranscript } from "../src/ui/transcript.js";
import type { FabricDashboardSnapshot, FabricUiAgent } from "../src/ui/types.js";
import { FabricWidget, loadWidgetInteraction, type FabricWidgetRow } from "../src/ui/widget.js";

beforeAll(async () => { await loadWidgetInteraction(); });

const theme = {
  fg: (_color: string, text: string) => text,
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text,
  italic: (text: string) => text,
  underline: (text: string) => text,
  inverse: (text: string) => text,
} as unknown as Theme;

const agent = (id: string, name: string, status = "running"): FabricUiAgent => ({
  id,
  name,
  status,
  transport: "process",
  cwd: "/tmp",
  startedAt: Date.now() - 5_000,
  toolCalls: 1,
});

const snapshot = (): FabricDashboardSnapshot => {
  const now = Date.now();
  return {
    now,
    main: { ...agent("main", "main"), status: "idle" },
    peers: [],
    runs: [],
    agents: [agent("agent-1", "security-reviewer")],
    actors: [
      {
        id: "actor-1",
        scope: "project",
        name: "advisor",
        status: "running",
        worker: agent("worker-1", "advisor-worker"),
        instructions: "",
        recentMessages: [],
      },
    ],
    componentGraph: { nodes: [], edges: [] },
    globalActors: [],
    state: [],
    events: [],
  } as unknown as FabricDashboardSnapshot;
};

const transcript = (): FabricAgentTranscript =>
  projectAgentTranscript([
    { type: "message_update", assistantMessageEvent: { type: "thinking_delta", delta: "Checking the " } },
    { type: "message_update", assistantMessageEvent: { type: "thinking_delta", delta: "new branch state" } },
    { type: "tool_execution_start", toolCallId: "t1", toolName: "read", args: { path: "src/a.ts" } },
  ]);

const setup = (options: { current?: FabricDashboardSnapshot; terminalRows?: number; hideThinking?: boolean; activity?: () => FabricAgentTranscript } = {}) => {
  const current = options.current ?? snapshot();
  const reads: FabricWidgetRow[] = [];
  const forwarded: string[] = [];
  let released = 0;
  const widget = new FabricWidget(theme, () => current, 6, () => options.terminalRows ?? 40, undefined, undefined, {
    activity: (row) => {
      reads.push(row);
      return (options.activity ?? transcript)();
    },
    release: (forward) => {
      released++;
      if (forward !== undefined) forwarded.push(forward);
      widget.focused = false;
    },
    requestRender: () => {},
    ...(options.hideThinking === undefined ? {} : { hideThinking: () => options.hideThinking! }),
  });
  return { widget, reads, forwarded, released: () => released };
};

const longTranscript = (): FabricAgentTranscript =>
  projectAgentTranscript(Array.from({ length: 40 }, (_, index) => ({
    type: "tool_execution_start",
    toolCallId: `t${index}`,
    toolName: "read",
    args: { path: `src/file-${index}.ts` },
  })));

const manyAgents = (count: number): FabricDashboardSnapshot => {
  const current = snapshot() as FabricDashboardSnapshot & { agents: FabricUiAgent[]; actors: unknown[] };
  current.agents = Array.from({ length: count }, (_, index) => agent(`agent-${index}`, `agent-${index}`));
  current.actors = [];
  return current;
};

describe("Fabric widget activity expansion", () => {
  it("keeps thinking text from streamed worker deltas", () => {
    const entries = transcript().entries;
    const thinking = entries.find((entry) => entry.kind === "thinking");
    expect(thinking?.text).toBe("Checking the new branch state");
    expect(entries.some((entry) => entry.kind === "tool" && entry.toolName === "read")).toBe(true);
  });

  it("expands the selected participant inline and collapses it again from the keyboard", () => {
    const { widget, reads } = setup();
    const collapsed = widget.render(100);
    expect(collapsed.join("\n")).toContain("ctrl+shift+e expand");

    widget.focused = true;
    widget.selectDefault();
    widget.handleInput("\x1b[B"); // down: agent row → advisor actor row
    widget.handleInput("\r");
    expect(widget.expandedId).toBe("actor-1");
    const expanded = widget.render(100).join("\n");
    expect(reads.at(-1)).toMatchObject({ id: "actor-1", kind: "actor" });
    expect(expanded).toContain("advisor activity");
    expect(expanded).toContain("thinking");
    expect(expanded).toContain("new branch state");
    expect(expanded).toContain("read");

    widget.handleInput("\r");
    expect(widget.expandedId).toBeUndefined();
    expect(widget.render(100).join("\n")).not.toContain("advisor activity");
  });

  it("Esc collapses first, then releases focus to the editor", () => {
    const { widget, released } = setup();
    widget.focused = true;
    widget.selectDefault();
    widget.handleInput("\r");
    expect(widget.expandedId).toBe("agent-1");
    widget.handleInput("\x1b");
    expect(widget.expandedId).toBeUndefined();
    expect(released()).toBe(0);
    widget.handleInput("\x1b");
    expect(released()).toBe(1);
  });

  it("a click on a participant row focuses the widget and toggles its pane", () => {
    const { widget } = setup();
    const lines = widget.render(100);
    const advisorLine = lines.findIndex((line) => line.includes("advisor"));
    expect(advisorLine).toBeGreaterThan(0);
    const click = { type: "click", button: "left", x: 4, y: advisorLine, screenX: 4, screenY: advisorLine, width: 100, height: lines.length, shift: false, alt: false, ctrl: false } as const;
    expect(widget.handleMouse(click)).toMatchObject({ focus: true });
    expect(widget.expandedId).toBe("actor-1");
    widget.render(100);
    widget.handleMouse(click);
    expect(widget.expandedId).toBeUndefined();
  });

  it("keeps every expanded line inside the width", () => {
    const { widget } = setup();
    widget.focused = true;
    widget.selectDefault();
    widget.handleInput("\r");
    for (const width of [30, 60, 120]) {
      expect(widget.render(width).every((line) => visibleWidth(line) <= width)).toBe(true);
    }
  });

  it("hands a key it does not use back to the editor with focus", () => {
    const { widget, forwarded, released } = setup();
    widget.focused = true;
    widget.selectDefault();
    widget.handleInput("x");
    expect(released()).toBe(1);
    expect(forwarded).toEqual(["x"]);
    // Ctrl+C lives in Pi's editor, so it must reach it too.
    widget.focused = true;
    widget.handleInput("\x03");
    expect(forwarded).toEqual(["x", "\x03"]);
  });

  it("a click outside the participant rows does not take focus", () => {
    const { widget } = setup();
    const lines = widget.render(100);
    const header = { type: "click", button: "left", x: 1, y: 0, screenX: 1, screenY: 0, width: 100, height: lines.length, shift: false, alt: false, ctrl: false } as const;
    expect(widget.handleMouse(header)).toBeUndefined();
    expect(widget.expandedId).toBeUndefined();
  });

  it("keeps an expanded widget inside its terminal share", () => {
    for (const terminalRows of [12, 24, 50]) {
      const { widget } = setup({ terminalRows, activity: longTranscript });
      widget.focused = true;
      widget.selectDefault();
      widget.handleInput("\r");
      const lines = widget.render(100);
      expect(lines.length).toBeLessThanOrEqual(Math.max(7, Math.floor(terminalRows * 0.5)));
      expect(lines.join("\n")).toContain("activity");
    }
  });

  it("selects only the rows the widget shows", () => {
    const { widget } = setup({ current: manyAgents(12) });
    const shown = widget.render(100);
    widget.focused = true;
    widget.selectDefault();
    for (let index = 0; index < 20; index++) widget.handleInput("\x1b[B");
    widget.handleInput("\r");
    const lastShown = shown.map((line) => /agent-(\d+)/.exec(line)?.[1]).filter(Boolean).at(-1);
    expect(widget.expandedId).toBe(`agent-${lastShown}`);
  });

  it("keeps the thinking marker and drops its text when Pi hides thinking", () => {
    const { widget } = setup({ hideThinking: true });
    widget.focused = true;
    widget.selectDefault();
    widget.handleInput("\r");
    const expanded = widget.render(100).join("\n");
    expect(expanded).toContain("thinking");
    expect(expanded).not.toContain("new branch state");
  });

  it("finishes the thinking of a reply that did not stream and keeps it for retained sessions", () => {
    const streamed = projectAgentTranscript([
      { type: "message_end", message: { role: "assistant", content: [{ type: "thinking", thinking: "plan" }, { type: "text", text: "done" }] } },
    ]).entries;
    expect(streamed.find((entry) => entry.kind === "thinking")).toMatchObject({ text: "plan", status: "completed" });
    const retained = projectAgentTranscript([
      { type: "message", message: { role: "assistant", content: [{ type: "thinking", thinking: "first" }, { type: "text", text: "a" }] } },
      { type: "message", message: { role: "assistant", content: [{ type: "thinking", thinking: "second" }, { type: "text", text: "b" }] } },
    ]).entries.filter((entry) => entry.kind === "thinking");
    expect(retained.map((entry) => [entry.text, entry.status])).toEqual([["first", "completed"], ["second", "completed"]]);
  });

  it("stays passive without interaction wiring", () => {
    const current = snapshot();
    const widget = new FabricWidget(theme, () => current, 6);
    expect(widget.render(100).join("\n")).not.toContain("expand");
    expect(widget.handleMouse({ type: "click", button: "left", x: 0, y: 1, screenX: 0, screenY: 1, width: 100, height: 3, shift: false, alt: false, ctrl: false })).toBeUndefined();
  });
});
