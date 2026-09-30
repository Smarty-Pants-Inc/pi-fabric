import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { expect, it, vi } from "vitest";

const profileLoads = vi.hoisted(() => vi.fn());
vi.mock("../src/lifecycle/reload-target-profile.js", async original => {
  profileLoads();
  return original<typeof import("../src/lifecycle/reload-target-profile.js")>();
});

it("keeps profile validation out of cold import, registration and idle; loads it once at the first public request", async () => {
  for (const name of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID"]) vi.stubEnv(name, undefined);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lazy-reload-target-"));
  try {
    const { installSelfReload } = await import("../src/lifecycle/self-reload.js");
    expect(profileLoads).not.toHaveBeenCalled();
    const resource = path.join(root, "code.js"); fs.writeFileSync(resource, "export default () => {};\n");
    const settingsPath = path.join(root, "settings.json");
    fs.writeFileSync(settingsPath, JSON.stringify({ packages: [process.cwd()], extensions: [resource] }));
    const handlers = new Map<string, (event: any, context: any) => unknown>();
    const listeners = new Map<string, Set<(data: any) => void>>();
    const events = {
      on: (topic: string, handler: (data: any) => void) => {
        const set = listeners.get(topic) ?? new Set(); set.add(handler); listeners.set(topic, set);
        return () => { set.delete(handler); };
      },
      emit: (topic: string, data: unknown) => { for (const handler of listeners.get(topic) ?? []) handler(data); },
    };
    const pi = { events, on: (name: string, fn: any) => handlers.set(name, fn), registerCommand: vi.fn(), sendUserMessage: vi.fn() };
    const context = { mode: "tui", hasUI: false, isIdle: () => true, hasPendingMessages: () => false,
      sessionManager: { getSessionId: () => root } };
    const reload = installSelfReload(pi as never, { moduleUrl: pathToFileURL(path.join(process.cwd(), "src/index.ts")).href,
      settingsPath, busy: () => 0, autoReloadConfigured: () => true });
    reload.sessionStart("startup", context as never);
    handlers.get("turn_end")?.({}, context); handlers.get("agent_settled")?.({}, context);
    expect(profileLoads).not.toHaveBeenCalled();
    expect(listeners.get("pi-fabric:reload-target:v1")?.size).toBeGreaterThan(0);
    const staleResult = new Promise<unknown>(resolve => {
      const off = events.on("pi-fabric:reload-target:v1:result", reply => {
        if (reply.requestId === "stale-first-use") { off(); resolve(reply); }
      });
    });
    events.emit("pi-fabric:reload-target:v1", { requestId: "stale-first-use", owner: "code", resource,
      loaded: resource, configured: resource, reason: "bind" });
    // A session switch while the optional validator imports must invalidate the original proof.
    reload.sessionStart("switch", context as never);
    expect(await staleResult).toMatchObject({ requestId: "stale-first-use", accepted: false, reason: "session-changed" });
    for (const requestId of ["initial", "repeat"]) {
      const result = new Promise<unknown>(resolve => {
        const off = events.on("pi-fabric:reload-target:v1:result", reply => {
          if (reply.requestId === requestId) { off(); resolve(reply); }
        });
      });
      events.emit("pi-fabric:reload-target:v1", { requestId, owner: "code", resource, loaded: resource, configured: resource, reason: "bind" });
      expect(await result).toMatchObject({ accepted: true, reason: "bound-unchanged", requestId });
      expect(profileLoads).toHaveBeenCalledTimes(1);
    }
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    handlers.get("session_shutdown")?.({}, context);
  } finally { fs.rmSync(root, { recursive: true, force: true }); vi.unstubAllEnvs(); }
});
