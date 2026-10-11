import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import {
  createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager,
  type AgentSession, type ExtensionUIContext, type Theme,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import type { TUI } from "@earendil-works/pi-tui";
import { FABRIC_PROVIDER_REGISTER_EVENT } from "../src/protocol.js";

const entry = path.resolve("dist/index.js");

function stackIncludesDirectory(stack: string, directory: string): boolean {
  // Native Windows paths use backslashes, but Node ESM stack frames use file:///D:/... URLs.
  // Normalize both spellings before attributing timers; an empty owned list is not evidence
  // that a headless Windows runner never queued a visible dashboard event refresh.
  return stack.replaceAll("\\", "/").includes(`${directory.replaceAll("\\", "/")}/`);
}

describe("reload timer stack attribution", () => {
  it.each([
    ["D:\\a\\pi-fabric\\dist", "at poll (file:///D:/a/pi-fabric/dist/index.js:1:2)"],
    ["D:\\a\\pi-fabric\\dist", "at poll (D:\\a\\pi-fabric\\dist\\index.js:1:2)"],
    ["/repo/dist", "at poll (file:///repo/dist/chunks/poll.js:1:2)"],
    ["/repo/dist", "at poll (/repo/dist/index.js:1:2)"],
  ])("attributes timers in %s from %s", (directory, stack) => {
    expect(stackIncludesDirectory(stack, directory)).toBe(true);
  });

  it("does not attribute a sibling directory or a host timer to Fabric", () => {
    expect(stackIncludesDirectory("at poll (file:///D:/a/pi-fabric/dist-other/index.js:1:2)", "D:\\a\\pi-fabric\\dist")).toBe(false);
    expect(stackIncludesDirectory("at poll (node:internal/timers:1:2)", "/repo/dist")).toBe(false);
  });
});

async function waitUntil(condition: () => boolean, description: string): Promise<void> {
  const deadline = performance.now() + 5_000;
  while (!condition()) {
    if (performance.now() >= deadline) {
      throw new Error(`Timed out after 5 s waiting for ${description}`);
    }
    await delay(10);
  }
}

describe.skipIf(!fs.existsSync(entry))("real Pi reload event-refresh guard (smarty-dev#4383, #7791)", () => {
  it.each(["native", "win32"] as const)("clears the old generation's timers across ten reload windows with asynchronous disposal (%s paths)", async (pathStyle) => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-reload-poll-")));
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir);
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      mesh: { enabled: true, announce: true },
      ui: { enabled: true, refreshMs: 100, widget: "hidden" },
    }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", path.join(root, "mesh"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
    const faults: unknown[] = [];
    const extensionErrors: unknown[] = [];
    const onFault = (error: unknown) => { faults.push(error); };
    process.on("uncaughtException", onFault);
    process.on("unhandledRejection", onFault);
    const owned: Array<{ timer: NodeJS.Timeout; stack: string }> = [];
    // Exercise Windows-native directory spelling against real ESM stack frames on every OS.
    const directory = pathStyle === "win32" ? path.dirname(entry).replaceAll("/", "\\") : path.dirname(entry);
    const track = (timer: NodeJS.Timeout) => {
      const stack = new Error().stack ?? "";
      if (stackIncludesDirectory(stack, directory)) owned.push({ timer, stack });
      return timer;
    };
    const timeout = globalThis.setTimeout;
    const interval = globalThis.setInterval;
    vi.spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof timeout>) =>
      track(timeout(...args))) as typeof timeout);
    vi.spyOn(globalThis, "setInterval").mockImplementation(((...args: Parameters<typeof interval>) =>
      track(interval(...args))) as typeof interval);
    const destroyed = (timer: NodeJS.Timeout) => (timer as NodeJS.Timeout & { _destroyed: boolean })._destroyed;
    const doneCallbacks: Array<() => void> = [];
    const tui = { requestRender: vi.fn() } as unknown as TUI;
    const theme = { fg: (_color: string, text: string) => text, bg: (_color: string, text: string) => text,
      bold: (text: string) => text } as unknown as Theme;
    const ui = new Proxy({
      custom: (factory: (tui: TUI, theme: Theme, keys: unknown, done: () => void) => unknown) =>
        new Promise<void>(resolve => { doneCallbacks.push(resolve); factory(tui, theme, {}, resolve); }),
      notify: vi.fn(), setWidget: vi.fn(), setStatus: vi.fn(),
    }, { get: (target, key) => Reflect.get(target, key) ?? (() => {}) }) as unknown as ExtensionUIContext;
    let closeCount = 0;
    let session: AgentSession | undefined;
    let modal: Promise<void> | undefined;
    try {
      const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
      const loader = new DefaultResourceLoader({
        cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
        additionalExtensionPaths: [entry],
        extensionFactories: [{ name: "slow-disposal-window", factory: pi => {
          pi.events.emit(FABRIC_PROVIDER_REGISTER_EVENT, { version: 1, provider: {
            name: "poll_guard_probe", description: "Hold the real asynchronous runtime disposal window",
            list: async () => [], describe: async () => undefined, invoke: async () => undefined,
            close: async () => { closeCount++; await delay(250); },
          } });
        } }],
      });
      await loader.reload();
      ({ session } = await createAgentSession({ cwd: root, agentDir, modelRuntime, resourceLoader: loader,
        sessionManager: SessionManager.inMemory(root) }));
      await session.bindExtensions({ mode: "tui", uiContext: ui, onError: error => { extensionErrors.push(error); } });
      const open = async () => {
        const runner = session!.extensionRunner!;
        const result = await runner.getToolDefinition("fabric_exec")!.execute("activate", { code: "return 1" } as never,
          undefined, undefined, runner.createContext());
        expect(result.content).toEqual(expect.arrayContaining([expect.objectContaining({ type: "text" })]));
        modal = runner.getCommand("fabric")!.handler("dashboard", runner.createCommandContext());
        await waitUntil(() => doneCallbacks.length > 0, "the dashboard TUI to attach");
        // A visible dashboard is silent until an actual activity event. Queue
        // a second real tool execution, then reload through its pending coalescer.
        expect(owned.filter(({ timer, stack }) => !destroyed(timer) && stack.includes("FabricUiController.#schedulePoll"))).toEqual([]);
        await runner.getToolDefinition("fabric_exec")!.execute("event", { code: "return 2" } as never,
          undefined, undefined, runner.createContext());
        await waitUntil(
          () => owned.some(({ timer, stack }) => !destroyed(timer) && stack.includes("FabricUiController.#scheduleRefresh")),
          "a real dashboard event refresh to be queued before opening a reload window",
        );
      };
      await open();
      for (const pause of [0, 17, 49, 99, 101, 149, 199, 249, 499, 999]) {
        await delay(pause);
        const old = owned.filter(({ timer }) => !destroyed(timer));
        const closes = closeCount;
        await session.reload();
        for (const done of doneCallbacks.splice(0)) done();
        await modal;
        expect(closeCount).toBe(closes + 1);
        expect(old.filter(({ timer }) => !destroyed(timer)).map(({ stack }) => stack)).toEqual([]);
        expect(faults).toEqual([]);
        expect(extensionErrors).toEqual([]);
        await open();
      }
      for (const done of doneCallbacks.splice(0)) done();
      await modal;
      await session.extensionRunner!.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose(); session = undefined;
      await delay(25);
      expect(owned.filter(({ timer }) => !destroyed(timer)).map(({ stack }) => stack)).toEqual([]);
      expect(faults).toEqual([]);
    } finally {
      for (const done of doneCallbacks.splice(0)) done();
      await modal;
      if (session) { await session.extensionRunner!.emit({ type: "session_shutdown", reason: "quit" }); session.dispose(); }
      process.off("uncaughtException", onFault); process.off("unhandledRejection", onFault);
      vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);
});
