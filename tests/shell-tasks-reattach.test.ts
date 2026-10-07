import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";
import type { FabricState } from "../src/fabric-state.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { FabricUiController } from "../src/ui/controller.js";
import type { ShellTasksView } from "../src/ui/shell-tasks.js";

const cleanup: Array<() => Promise<void> | void> = [];
afterEach(async () => { for (const close of cleanup.splice(0).reverse()) await close(); });
const fixture = (mode = "rpc") => {
  const jobs = new FabricShellJobStore(); cleanup.push(() => jobs.close());
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  jobs.durable = { resume: vi.fn(async () => {
    await gate;
    const job = jobs.begin("bash", "retained command", { id: "retained-1234", description: "Retained build" });
    job.spill();
  }) } as unknown as NonNullable<typeof jobs.durable>;
  const state = { shellJobs: jobs, config: { ui: { enabled: true, widget: "hidden", refreshMs: 1000 }, mesh: { enabled: false } },
    activity: { subscribe: () => () => {}, runs: () => [] }, agents: { list: () => [], subscribeUi: () => () => {} },
    actors: { list: () => [], subscribe: () => () => {} }, globalActors: { list: () => [] }, mainAgentInfo: () => ({ status: "idle" }),
  } as unknown as FabricState;
  const controller = new FabricUiController(state); cleanup.push(() => controller.stop());
  const notify = vi.fn();
  let rendered = "";
  const custom = vi.fn(async (factory: (...args: any[]) => ShellTasksView) => {
    const theme = { fg: (_: string, s: string) => s, bg: (_: string, s: string) => s, bold: (s: string) => s } as unknown as Theme;
    const view = factory({ requestRender() {}, terminal: { rows: 24 } }, theme, {}, () => {});
    rendered = view.render(100).join("\n");
  });
  const context = { mode, ui: { notify, custom, setWidget: vi.fn() } } as unknown as ExtensionContext;
  return { jobs, controller, context, notify, custom, release, rendered: () => rendered };
};

it.each([undefined, "retained"])("awaits first retained-task reattachment before non-TUI query %s", async query => {
  const h = fixture(); const pending = h.controller.openTasks(h.context, query);
  try {
    expect(h.controller.ownsInput).toBe(true);
    expect(h.notify).not.toHaveBeenCalled();
    h.release(); await pending;
    expect(h.notify).toHaveBeenCalledOnce();
    expect(h.notify.mock.calls[0]![0]).toContain("retained-1234");
    expect(h.notify.mock.calls[0]![1]).toBe("info");
    expect(h.controller.ownsInput).toBe(false);
  } finally { h.release(); await pending; }
});

it("opens retained detail on the first TUI inspection and keeps synchronous input ownership", async () => {
  const h = fixture("tui"); const pending = h.controller.openTasks(h.context, "retained");
  try {
    expect(h.controller.ownsInput).toBe(true);
    expect(h.custom).not.toHaveBeenCalled();
    await h.controller.openTasks(h.context); // a second command cannot race setup
    expect(h.jobs.durable!.resume).toHaveBeenCalledOnce();
    h.release(); await pending;
    expect(h.notify).not.toHaveBeenCalled();
    expect(h.custom).toHaveBeenCalledOnce();
    expect(h.rendered()).toContain("retained command");
    expect(h.controller.ownsInput).toBe(false);
  } finally { h.release(); await pending; }
});

it("does not validate or mount an old command after stop during reattachment", async () => {
  const h = fixture("tui"); const pending = h.controller.openTasks(h.context, "retained");
  try {
    h.controller.stop(); h.release(); await pending;
    expect(h.custom).not.toHaveBeenCalled();
    expect(h.notify).not.toHaveBeenCalled();
    expect(h.controller.ownsInput).toBe(false);
  } finally { h.release(); await pending; }
});
