import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import {
  createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager,
  type AgentSession, type ExtensionUIContext,
} from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { FABRIC_PROVIDER_REGISTER_EVENT } from "../src/protocol.js";

const entry = path.resolve("dist/index.js");

describe.skipIf(!fs.existsSync(entry))("real Pi reload poll guard (smarty-dev#4383)", () => {
  it("clears the old generation's timers across ten reload windows with asynchronous disposal", async () => {
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
    const track = (timer: NodeJS.Timeout) => {
      const stack = new Error().stack ?? "";
      if (stack.includes(path.dirname(entry))) owned.push({ timer, stack });
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
    const ui = new Proxy({
      custom: () => new Promise<void>(resolve => { doneCallbacks.push(resolve); }),
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
        await delay(150);
        expect(owned.some(({ timer, stack }) => !destroyed(timer) && stack.includes("#schedulePoll"))).toBe(true);
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
