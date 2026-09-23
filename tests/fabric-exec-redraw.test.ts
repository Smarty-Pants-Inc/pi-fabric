import { initTheme, ToolExecutionComponent } from "@earendil-works/pi-coding-agent";
import { TuiMainScreen, stripTerminalSequences, type Terminal } from "@earendil-works/pi-tui";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createFabricExecTool } from "../src/fabric-exec-tool.js";
import type { FabricState } from "../src/fabric-state.js";
import { defaultCodePreviewSettings } from "../src/ui/code-preview.js";
import type { FabricRenderAudit } from "../src/ui/fabric-render.js";
import { configureHighlighting } from "../src/ui/highlight.js";

let terminalRows = 30;
const originalRows = Object.getOwnPropertyDescriptor(process.stdout, "rows");

class ProbeTerminal implements Terminal {
  columns = 120;
  get rows(): number { return terminalRows; }
  kittyProtocolActive = false;
  output = "";
  start(): void {}
  stop(): void {}
  async drainInput(): Promise<void> {}
  write(data: string): void { this.output += data; }
  moveBy(): void {}
  hideCursor(): void {}
  showCursor(): void {}
  clearLine(): void {}
  clearFromCursor(): void {}
  clearScreen(): void {}
  setTitle(): void {}
  setProgress(): void {}
}

interface ProbeOptions {
  shell?: "bash" | "powershell";
  display?: "compact" | "full";
  expanded?: boolean;
  background?: "on" | "border" | "off";
  timing?: boolean;
  commandLines?: number;
  audits?: FabricRenderAudit[];
}

const createProbe = (options: ProbeOptions = {}) => {
  const terminal = new ProbeTerminal();
  const tui = new TuiMainScreen(terminal);
  // Exercise the real host's invalidation without its paint throttle hiding
  // which timer caused a redraw. No subprocess or actual terminal is needed.
  vi.spyOn(tui, "requestRender").mockImplementation(() => tui.renderNow());
  const shell = options.shell ?? "bash";
  const command = ["echo probe", ...Array.from(
    { length: (options.commandLines ?? 40) - 1 }, (_, index) => `# command line ${index + 2}`,
  )].join("\n");
  const audits = options.audits ?? [{
    ref: `pi.${shell}`, provider: "pi", tool: shell, args: { command },
  }];
  const state = {
    bootstrapped: true,
    initialized: true,
    config: { ui: { showAgentToolPreview: true, toolDisplay: options.display ?? "compact" } },
  } as unknown as FabricState;
  const tool = createFabricExecTool(state, {
    ...defaultCodePreviewSettings(),
    toolCallBackground: options.background ?? "on",
    toolCallTiming: options.timing ?? true,
    syntaxHighlighting: false,
  }, new Map());
  const args = { code: `return await pi.${shell}({ cmd: ${JSON.stringify(command)} });` };
  const card = new ToolExecutionComponent("fabric_exec", "redraw-probe", args,
    { showImages: false }, tool, tui, process.cwd());
  const history = Array.from({ length: 2_000 }, (_, index) => `history-sentinel-${index} ${"prior text ".repeat(7)}`);
  tui.addChild({ render: () => history, invalidate() {} });
  tui.addChild(card);
  tui.addChild({ render: () => Array.from({ length: 8 }, () => "editor/footer"), invalidate() {} });
  card.setArgsComplete();
  card.markExecutionStarted();
  card.setExpanded(options.expanded ?? false);
  card.updateResult({ content: [], details: { audits, phases: [] }, isError: false }, true);
  tui.renderNow();
  const lines = () => card.render(terminal.columns).map(stripTerminalSequences);
  const headings = () => lines().filter(line => /[◐◓◑◒] (?:bash|powershell)/.test(line));
  const finish = (success = true) => {
    card.updateResult({
      content: [{ type: "text", text: success ? "done" : "failed" }],
      details: { success, audits: audits.map(audit => ({ ...audit, success })), phases: [] },
      isError: !success,
    });
  };
  return { terminal, tui, card, audits, lines, headings, finish };
};

beforeEach(() => {
  terminalRows = 30;
  Object.defineProperty(process.stdout, "rows", { configurable: true, get: () => terminalRows });
  vi.useFakeTimers({ now: new Date("2026-01-01T00:00:00Z") });
  initTheme("dark", false);
  configureHighlighting("github-dark", false);
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  if (originalRows) Object.defineProperty(process.stdout, "rows", originalRows);
  else Reflect.deleteProperty(process.stdout, "rows");
});

const shellCases = (["bash", "powershell"] as const).flatMap(shell =>
  (["compact", "full"] as const).flatMap(display =>
    [false, true].flatMap(expanded =>
      (["on", "border", "off"] as const).map(background => ({ shell, display, expanded, background })),
    ),
  ),
);

const expectPausedTicks = async (probe: ReturnType<typeof createProbe>) => {
  await vi.advanceTimersByTimeAsync(1_000);
  const redraws = probe.tui.fullRedraws;
  const headings = probe.headings();
  expect(headings.length).toBeGreaterThan(0);
  for (let tick = 0; tick < 3; tick++) {
    probe.terminal.output = "";
    await vi.advanceTimersByTimeAsync(250);
    expect(probe.headings()).toEqual(headings);
    expect(probe.tui.fullRedraws).toBe(redraws);
    expect(probe.terminal.output).not.toContain("\x1b[3J");
    expect(probe.terminal.output).not.toContain("history-sentinel");
    expect(Buffer.byteLength(probe.terminal.output)).toBeLessThan(4_096);
    expect(probe.lines().join("\n")).not.toContain("Running…");
  }
};

const expectAnimatedTick = async (probe: ReturnType<typeof createProbe>) => {
  const headings = probe.headings();
  const redraws = probe.tui.fullRedraws;
  await vi.advanceTimersByTimeAsync(250);
  expect(probe.headings()).not.toEqual(headings);
  expect(probe.tui.fullRedraws).toBe(redraws);
  expect(probe.lines().join("\n")).not.toContain("Running…");
};

describe("Fabric height-guarded animation through Pi's regular-screen renderer", () => {
  it.each(shellCases)(
    "freezes tall $shell headings without changing the layout ($display, expanded=$expanded, shell=$background)",
    async options => {
      const probe = createProbe(options);
      expect(probe.lines().join("\n")).toContain("# command line 40");
      expect(probe.lines().length).toBeGreaterThan(probe.terminal.rows);
      await expectPausedTicks(probe);
      probe.finish();
      expect(probe.lines().join("\n")).toContain("Took");
      expect(probe.headings()).toHaveLength(0);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  it.each(["on", "border", "off"] as const)("keeps short headings animated (%s)", async background => {
    const probe = createProbe({ commandLines: 1, background });
    await vi.advanceTimersByTimeAsync(1_000);
    await expectAnimatedTick(probe);
    if (background === "border") {
      const lines = probe.lines();
      expect(lines.find(line => line.includes("╭"))).toContain("Elapsed");
      expect(lines.find(line => line.includes("╰"))).not.toContain("Elapsed");
    }
    probe.finish();
  });

  it.each(["on", "border", "off"] as const)("resumes after growing the terminal and freezes again after shrinking (%s)", async background => {
    const probe = createProbe({ background });
    await expectPausedTicks(probe);
    terminalRows = 200;
    probe.tui.renderNow();
    await vi.advanceTimersByTimeAsync(1_000);
    await expectAnimatedTick(probe);
    terminalRows = 30;
    probe.tui.renderNow();
    await expectPausedTicks(probe);
    probe.finish();
  });

  it.each([true, false])("finishes a paused command without elapsed timing (success=%s)", async success => {
    const probe = createProbe({ timing: false });
    await expectPausedTicks(probe);
    probe.finish(success);
    expect(probe.headings()).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("allows room for the editor and footer before the command reaches terminal height", async () => {
    const probe = createProbe({ commandLines: 22 });
    await expectPausedTicks(probe);
    probe.finish();
  });

  it("freezes an offscreen border timer while a short result heading stays animated", async () => {
    const probe = createProbe({ commandLines: 1, background: "border", display: "full", expanded: true });
    await vi.advanceTimersByTimeAsync(1_000);
    probe.card.updateArgs({ code: Array.from({ length: 60 }, () => "// outer program").join("\n") });
    probe.tui.renderNow();
    const border = probe.lines().find(line => line.includes("╭"));
    expect(border).toContain("Elapsed");
    await expectAnimatedTick(probe);
    expect(probe.lines().find(line => line.includes("╭"))).toBe(border);
    probe.finish();
  });

  it("keeps output updates live while the heading is paused", async () => {
    const probe = createProbe();
    await expectPausedTicks(probe);
    const headings = probe.headings();
    probe.card.updateResult({ content: [], isError: false, details: {
      audits: probe.audits.map(audit => ({ ...audit, result: { output: "fresh output" } })), phases: [],
    } }, true);
    probe.tui.renderNow();
    expect(probe.lines().join("\n")).toContain("fresh output");
    expect(probe.headings()).toEqual(headings);
    probe.finish();
  });

  it("resumes multicall headings after collapsing a tall expanded card", async () => {
    const audits = Array.from({ length: 40 }, (_, index) => ({
      ref: "pi.bash", provider: "pi", tool: "bash", args: { command: `echo call-${index}` },
    }));
    const probe = createProbe({ audits, expanded: true, timing: false });
    expect(probe.lines().join("\n")).toContain("echo call-29");
    await expectPausedTicks(probe);
    probe.card.setExpanded(false);
    probe.tui.renderNow();
    await vi.advanceTimersByTimeAsync(1_000);
    await expectAnimatedTick(probe);
    probe.finish();
    expect(vi.getTimerCount()).toBe(0);
  });
});
