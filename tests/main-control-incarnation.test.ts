import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { CONTROL_STALE_INCARNATION, FabricControlPlane } from "../src/topology/control-plane.js";

it("real Main runtime reload rotates the published/control epoch and fences pre-reload admission", async () => {
  const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
  const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
  const { normalizeFabricConfig } = await import("../src/config.js");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-control-incarnation-"));
  const meshRoot = path.join(root, "mesh");
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root); vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  const sessionId = "7514aaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
  const ctx = () => ({ cwd: root, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
    isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
      getLeafId: () => null, getEntries: () => [] }, ui: { setStatus: () => {}, notify: () => {} } }) as unknown as ExtensionContext;
  const sendMessage = vi.fn();
  const host = { on: () => () => {}, events: { emit: () => {}, on: () => () => {} }, sendMessage,
    appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "incarnation-main" } as unknown as ExtensionAPI;
  const config = normalizeFabricConfig({ fullCodeMode: false, mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } });
  const runtimes: InstanceType<typeof FabricRuntimeState>[] = [];
  const ownerPlanes: FabricControlPlane[] = [];
  const start = FabricControlPlane.prototype.start;
  vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation(function (this: FabricControlPlane, ...args) {
    if (this.options.hostId === `session:${sessionId}`) ownerPlanes.push(this);
    return start.apply(this, args);
  });
  const createRuntime = () => {
    const value = new FabricRuntimeState(host, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(root, "unused.mjs"), skills: root,
    } }); runtimes.push(value); return value;
  };
  const mesh = new MeshStore(meshRoot, 65_536, 1_000);
  const sender = new FabricControlPlane(mesh, { id: "session:observer", name: "observer", kind: "main" },
    { enabled: true, hostId: "session:observer", pollMs: 20, acknowledgementTimeoutMs: 10_000 });
  const observer = new ParticipantDirectory(mesh, { enabled: true, hostId: "session:observer", rootId: "session:observer",
    identity: { id: "session:observer", name: "observer", kind: "main" } });
  let pending: Promise<unknown> | undefined;
  try {
    const first = createRuntime(); await first.initialize(ctx(), config);
    const old = observer.get(`session:${sessionId}`, undefined, { fresh: true })!;
    expect(old.ownerIncarnation).toBe(ownerPlanes[0]!.incarnation);
    ownerPlanes[0]!.pause(); sender.start(() => ({ accepted: false }));
    pending = sender.request(old.ownerHostId, old.id, "steer", { message: "before reload", ownerIncarnation: old.ownerIncarnation, triggerTurn: false },
      old.ownerIdentityId, { routedRemoteHost: null }).catch(error => error);
    await vi.waitFor(() => expect(mesh.read({ topic: "fabric.control.command" })).toHaveLength(1));
    await first.shutdown("reload");
    const second = createRuntime(); await second.initialize(ctx(), config);
    const fresh = observer.get(old.id, undefined, { fresh: true })!;
    expect(fresh.ownerIncarnation).toBe(ownerPlanes[1]!.incarnation);
    expect(fresh.ownerIncarnation).not.toBe(old.ownerIncarnation);
    expect(await pending).toMatchObject({ code: CONTROL_STALE_INCARNATION });
    expect(sendMessage).not.toHaveBeenCalled();
    await expect(sender.request(fresh.ownerHostId, fresh.id, "steer", { message: "after reload", ownerIncarnation: fresh.ownerIncarnation, triggerTurn: false },
      fresh.ownerIdentityId, { routedRemoteHost: null, idempotencyKey: "fresh-main" })).resolves.toMatchObject({ acknowledged: true });
    expect(sendMessage).toHaveBeenCalledOnce();
    await expect(sender.request(fresh.ownerHostId, fresh.id, "steer", { message: "after reload", ownerIncarnation: fresh.ownerIncarnation, triggerTurn: false },
      fresh.ownerIdentityId, { routedRemoteHost: null, idempotencyKey: "fresh-main" })).resolves.toMatchObject({ acknowledged: true });
    expect(sendMessage).toHaveBeenCalledOnce();
  } finally {
    await sender.close(); await pending;
    for (const runtime of runtimes.reverse()) await runtime.shutdown("exit");
    await observer.close(); vi.restoreAllMocks(); vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 20_000);
