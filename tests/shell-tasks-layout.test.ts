import type { Theme } from "@earendil-works/pi-coding-agent";
import { visibleWidth, type KeybindingsManager } from "@earendil-works/pi-tui";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { ShellTasksView } from "../src/ui/shell-tasks.js";

const theme = { fg: (_: string, text: string) => text, bg: (_: string, text: string) => text } as Theme;
const cleanup: Array<() => void | Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); vi.useRealTimers(); });
const fixture = () => {
  // ScratchScope release yields with setImmediate; only fake the timers under test.
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] }); vi.setSystemTime(10000);
  const jobs = new FabricShellJobStore();
  cleanup.push(() => jobs.close());
  let rows = 40;
  const requestRender = vi.fn(), done = vi.fn();
  const open = (id?: string, customTheme = theme, keys?: KeybindingsManager) => {
    const view = new ShellTasksView({ jobs, theme: customTheme, rows: () => rows, requestRender, done, ...(id ? { id } : {}), ...(keys ? { keys } : {}) });
    cleanup.push(() => view.dispose());
    return view;
  };
  const add = (description: string) => {
    const job = jobs.begin("bash", `echo ${description}`, { description, cwd: "/work" }); job.spill(); return job;
  };
  return { jobs, add, open, requestRender, done, resize: (height: number) => { rows = height; } };
};

describe("shell task inspector layout", () => {
  it("frames and groups purpose-first tasks with semantic status and selection styling", async () => {
    const h = fixture();
    const done = h.add("Bundle package"); await done.finish(0);
    const failed = h.add("Run tests"); await failed.finish(1);
    h.add("Watch CI 界");
    const fg = vi.fn(theme.fg), bg = vi.fn(theme.bg);
    const view = h.open(undefined, { ...theme, fg, bg } as unknown as Theme);
    const lines = view.render(100), text = lines.join("\n");
    expect(lines[0]).toMatch(/^╭─.*Fabric · shell tasks.*╮$/);
    expect(lines.at(-1)).toMatch(/^╰─+╯$/);
    expect(lines.slice(1, -1).every(line => /^(│|├).*(│|┤)$/.test(line))).toBe(true);
    expect(lines.every(line => visibleWidth(line) === 100)).toBe(true);
    expect(text).toContain("1 active · 1 needs attention · 1 finished · 1/3");
    expect(text.indexOf("Active · 1")).toBeLessThan(text.indexOf("Needs attention · 1"));
    expect(text.indexOf("Needs attention · 1")).toBeLessThan(text.indexOf("Finished · 1"));
    expect(text).toContain("Watch CI 界"); expect(text).toContain("exit 1");
    expect(fg).toHaveBeenCalledWith("error", "✗ failed");
    expect(fg).toHaveBeenCalledWith("success", "✓ exited");
    expect(bg).toHaveBeenCalledWith("selectedBg", expect.stringContaining("Watch CI 界"));
  });

  it("keeps every selected card visible through paging, group boundaries, resizing and completion", async () => {
    const h = fixture();
    const jobs = Array.from({ length: 18 }, (_, i) => h.add(`Task ${i}`));
    for (const job of jobs.slice(10)) await job.finish(job === jobs[10] ? 1 : 0);
    h.resize(16);
    const view = h.open();
    const seen = new Set<string>();
    for (let i = 0; i < 18; i++) {
      const lines = view.render(80);
      expect(lines).toHaveLength(12);
      const selected = lines.find(line => line.includes("›"));
      expect(selected).toBeDefined(); seen.add(selected!);
      view.handleInput("\u001b[B");
    }
    expect(seen.size).toBe(18);
    view.handleInput("\u001b[6~");
    expect(view.render(80).join("\n")).not.toContain("· 1/18");
    view.handleInput("\u001b[5~");
    expect(view.render(80).join("\n")).toContain("· 1/18");
    view.handleInput("\u001b[B");
    const selectedName = view.render(80).find(line => line.includes("›"))!.match(/Task \d+/)![0];
    const selectedJob = jobs.find(job => job.info().description === selectedName)!;
    await selectedJob.finish(0);
    h.resize(12);
    expect(view.render(40).find(line => line.includes("›"))).toContain(selectedName);
    view.handleInput("\r");
    expect(view.render(80)[0]).toContain(selectedJob.id.slice(0, 8));
    view.handleInput("\u001b");
    expect(view.render(80).find(line => line.includes("›"))).toContain(selectedName);
  });

  it("animates only live views, never reads logs per frame, and releases timers/subscriptions", async () => {
    // ScratchScope release yields with setImmediate; only fake the timers under test.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] }); vi.setSystemTime(10000);
    const h = fixture(); const job = h.add("Quiet task");
    const read = vi.spyOn(job, "outputText");
    const view = h.open(); await Promise.resolve();
    expect(view.render(80).join("\n")).toContain("◐ spilled");
    read.mockClear(); h.requestRender.mockClear();
    await vi.advanceTimersByTimeAsync(250);
    expect(view.render(80).join("\n")).toContain("◓ spilled");
    expect(h.requestRender).toHaveBeenCalled(); expect(read).not.toHaveBeenCalled();
    await job.finish(0); await Promise.resolve();
    expect(view.render(80).join("\n")).toContain("✓ exited");
    h.requestRender.mockClear(); read.mockClear();
    await vi.advanceTimersByTimeAsync(1000);
    expect(h.requestRender).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled();
    view.dispose(); view.dispose();
    h.add("After close");
    expect(h.requestRender).not.toHaveBeenCalled();
  });

  it("uses injected keybindings and closes without cancelling work", () => {
    const h = fixture(); const job = h.add("First"); h.add("Second");
    const keys = { matches: (data: string, action: string) => ({ next: "tui.select.down", open: "tui.select.confirm", back: "tui.select.cancel" })[data] === action } as KeybindingsManager;
    const view = h.open(undefined, theme, keys);
    view.handleInput("next"); view.handleInput("open");
    expect(view.render(80)[0]).toContain(h.jobs.list()[1]!.id.slice(0, 8));
    view.handleInput("back"); view.handleInput("back");
    expect(h.done).toHaveBeenCalledOnce(); expect(job.abort.signal.aborted).toBe(false);
  });

  it("keeps borders, footer and selected rows bounded for tiny, narrow and wide terminals", () => {
    const h = fixture(); h.add("Watch 界 👩‍💻 é\u001b[2J"); h.add("Build");
    const view = h.open();
    for (const rows of [1, 2, 8, 9, 12, 24, 50]) {
      h.resize(rows);
      for (const width of [0, 1, 7, 8, 12, 30, 40, 80, 140]) {
        for (const detail of [false, true]) {
          if (detail) view.handleInput("\r");
          const lines = view.render(width);
          expect(lines.length).toBeLessThanOrEqual(Math.max(1, Math.floor(rows * 0.8)));
          expect(lines.every(line => visibleWidth(line) <= width)).toBe(true);
          expect(lines.join("\n")).not.toContain("\u001b[2J");
          if (width >= 8 && rows >= 9) expect(lines.at(-1)).toMatch(/^╰.*╯$/);
          if (detail) view.handleInput("\u001b");
        }
      }
    }
  });

  it("wraps safe output without losing indentation, scrolls and follows the tail", async () => {
    const h = fixture(); h.resize(24);
    const job = h.add("Output check");
    job.append(Buffer.from(Array.from({ length: 30 }, (_, i) => `  line ${i} 界\u001b[2J`).join("\n")));
    const view = h.open(); await Promise.resolve();
    let text = view.render(80).join("\n");
    expect(text).toContain("  line 29 界"); expect(text).not.toContain("\u001b");
    expect(text).toContain("Output · tail · bounded"); expect(text).toContain("Command");
    view.handleInput("\u001b[5~");
    text = view.render(80).join("\n");
    expect(text).not.toContain("line 29"); expect(text).toMatch(/Output · \d+–\d+\/30/);
    view.handleInput("G"); expect(view.render(80).join("\n")).toContain("line 29");
    job.append(Buffer.from("\n  new output")); await Promise.resolve();
    expect(view.render(80).join("\n")).toContain("  new output");
  });

  it("renders monitor delivery and status changes, expires stop confirmation, and stays theme-aware", async () => {
    const h = fixture();
    const job = h.jobs.begin("bash", "watch-ci", { description: "Watch CI", monitor: { delivery: "ui", timeoutMs: 300000, intervalMs: 5000 } }); job.spill();
    h.add("Other task");
    let color = 36;
    const ansiTheme = { fg: (_: string, text: string) => `\u001b[${color}m${text}\u001b[39m`, bg: (_: string, text: string) => `\u001b[44m${text}\u001b[49m` } as Theme;
    const view = h.open(job.id, ansiTheme);
    expect(view.render(100).join("\n")).toContain("Monitor: UI only · deadline 5m00s · 0 events");
    view.handleInput("x");
    expect(view.render(100).join("\n")).toContain("Press x again");
    await vi.advanceTimersByTimeAsync(3000);
    expect(view.render(100).join("\n")).not.toContain("Press x again");
    view.handleInput("x"); expect(job.abort.signal.aborted).toBe(false);
    view.handleInput("x"); expect(job.abort.signal.aborted).toBe(true);
    expect(view.render(100).join("\n")).toContain("stopping");
    await job.finish(null);
    expect(view.render(100).join("\n")).toContain("■ killed");
    color = 35; view.invalidate();
    expect(view.render(100).join("\n")).not.toContain("\u001b[36m");
    view.handleInput("\u001b");
    expect(view.render(100).join("\n")).toContain("monitor:ui");
    for (const width of [8, 20, 40, 80]) expect(view.render(width).every(line => visibleWidth(line) <= width)).toBe(true);
  });

  it("keeps empty and finished-only views timer-free, shows read errors, and ignores late reads on disposal", async () => {
    const h = fixture(); const empty = h.open();
    expect(vi.getTimerCount()).toBe(0);
    expect(empty.render(80).join("\n")).toContain("No background shell tasks.");
    empty.dispose();
    const job = h.add("Completed"); await job.finish(0);
    vi.spyOn(job, "outputText").mockRejectedValueOnce(new Error("missing log"));
    const view = h.open(); await Promise.resolve();
    expect(view.render(80).join("\n")).toContain("Output unavailable: missing log");
    expect(vi.getTimerCount()).toBe(0);
    view.dispose();
    let finishRead!: (output: string) => void;
    vi.spyOn(job, "outputText").mockImplementationOnce(() => new Promise(resolve => { finishRead = resolve; }));
    const pending = h.open(); pending.dispose();
    h.requestRender.mockClear(); finishRead("late result"); await Promise.resolve();
    expect(h.requestRender).not.toHaveBeenCalled();
  });

  it("reads the newly selected task after an old asynchronous read settles", async () => {
    const h = fixture(); const first = h.add("First"); const second = h.add("Second");
    await first.finish(0); await second.finish(0);
    let finishRead!: (output: string) => void;
    vi.spyOn(first, "outputText").mockImplementationOnce(() => new Promise(resolve => { finishRead = resolve; }));
    vi.spyOn(second, "outputText").mockResolvedValue("second task output");
    const view = h.open(first.id);
    view.handleInput("\u001b"); view.handleInput("\u001b[B"); view.handleInput("\r");
    finishRead("stale first output"); await Promise.resolve(); await Promise.resolve();
    const text = view.render(100).join("\n");
    expect(text).toContain("second task output"); expect(text).not.toContain("stale first output");
  });
});
