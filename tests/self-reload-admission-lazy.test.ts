import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { expect, it, vi } from "vitest";

const slotLoads = vi.hoisted(() => vi.fn());
vi.mock("../src/lifecycle/reload-slots.js", async original => {
  slotLoads();
  return original<typeof import("../src/lifecycle/reload-slots.js")>();
});

it("keeps slots off cold import, registration, idle, busy, unlimited and manual paths; loads once at automatic first use", async () => {
  for (const name of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_NO_AUTO_RELOAD"]) vi.stubEnv(name, undefined);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "reload-admission-lazy-"));
  const stops: Array<() => void> = [];
  try {
    const { installSelfReload, SELF_RELOAD_COMMAND } = await import("../src/lifecycle/self-reload.js");
    expect(slotLoads).not.toHaveBeenCalled();
    const settingsPath = path.join(root, "settings.json"), slots = path.join(root, "slots");
    let bump = 0;
    const activate = (target: string) => {
      fs.writeFileSync(settingsPath, JSON.stringify({ packages: [target] }));
      const date = new Date(Date.now() + ++bump * 1000); fs.utimesSync(settingsPath, date, date);
    };
    activate(process.cwd());
    const setup = (count: number, id: string) => {
      const handlers = new Map<string, (event: any, context: any) => unknown>();
      let command: { handler: (args: string, context: any) => Promise<void> };
      let busy = 0;
      const pi = { on: (name: string, fn: any) => handlers.set(name, fn),
        registerCommand: (name: string, value: any) => { expect(name).toBe(SELF_RELOAD_COMMAND); command = value; },
        sendUserMessage: vi.fn() };
      const context = { mode: "tui", hasUI: false, isIdle: () => true, hasPendingMessages: () => false,
        isPromptPending: () => false, isSettling: () => false, reload: vi.fn(async () => {}),
        sessionManager: { getSessionId: () => `${root}-${id}` } };
      const controller = installSelfReload(pi as never, {
        moduleUrl: pathToFileURL(path.join(process.cwd(), "src/index.ts")).href, settingsPath,
        busy: () => busy, autoReloadConfigured: () => true, selfReloadConcurrency: () => count,
        reloadSlotsDirectory: slots, reloadJitterMs: () => 0,
      });
      controller.sessionStart("startup", context as never);
      stops.push(() => handlers.get("session_shutdown")?.({}, context));
      handlers.get("turn_end")?.({}, context); handlers.get("agent_settled")?.({}, context);
      return { context, busy: (value: number) => { busy = value; },
        execute: (args = "auto") => command!.handler(args, context) };
    };
    const limited = setup(2, "limited"), unlimited = setup(0, "unlimited"), manual = setup(2, "manual");
    expect(slotLoads).not.toHaveBeenCalled(); expect(fs.existsSync(slots)).toBe(false);
    const release = (name: string) => {
      const dir = path.join(root, name); fs.mkdirSync(dir);
      fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ name: "pi-fabric" })); return dir;
    };
    activate(release("next")); limited.busy(1); await limited.execute();
    await unlimited.execute(); await manual.execute("");
    expect(slotLoads).not.toHaveBeenCalled(); expect(fs.existsSync(slots)).toBe(false);
    expect(limited.context.reload).not.toHaveBeenCalled();
    expect(unlimited.context.reload).toHaveBeenCalledOnce(); expect(manual.context.reload).toHaveBeenCalledOnce();
    limited.busy(0); await limited.execute();
    expect(slotLoads).toHaveBeenCalledOnce(); expect(limited.context.reload).toHaveBeenCalledOnce();
    activate(release("third")); await limited.execute();
    expect(slotLoads).toHaveBeenCalledOnce(); expect(limited.context.reload).toHaveBeenCalledTimes(2);
    expect(fs.readdirSync(slots)).toEqual([]);
  } finally {
    stops.forEach(stop => stop()); fs.rmSync(root, { recursive: true, force: true }); vi.unstubAllEnvs();
  }
});
