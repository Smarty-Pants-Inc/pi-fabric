// Keep reason: data safety. Exercise real retained-receipt/directory composition with a
// broker fixture; these are not native cross-host qualification or durability evidence.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricState } from "../src/fabric-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { SessionRecoveryActivation, TransportHostLeasePredecessorSnapshot } from "../src/recovery/l7-interfaces.js";
import { kvWorld } from "./helpers/bridge-nats-kv.js";

const roots: string[] = [], directories: ParticipantDirectory[] = [], meshes: MeshStore[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const mesh of meshes.splice(0)) mesh.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-activation-")); roots.push(root);
  const stateDirectory = path.join(root, "retained"); fs.mkdirSync(stateDirectory);
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000, { stateBackend: "file" }); meshes.push(mesh);
  const world = kvWorld(), hostId = "session:retained";
  return { root, stateDirectory, mesh, world, hostId };
}

describe("portable recovery activation composition", () => {
  it("carries the B2 verifier to actual lease custody before any broker ownership", async () => {
    const f = fixture(), denied = new Error("B2 exclusion unresolved");
    const verifyPredecessor = vi.fn(async (snapshot: TransportHostLeasePredecessorSnapshot, signal: AbortSignal) => {
      expect(snapshot).toEqual({ receipt: undefined, bytes: undefined });
      expect(signal).toBeInstanceOf(AbortSignal);
      expect(f.world.writes).toEqual([]);
      throw denied;
    });
    const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: f.hostId, rootId: f.hostId,
      identity: { id: f.hostId, name: "retained", kind: "main" },
      transport: f.world.transport("machine-a", f.hostId), transportStateDir: f.stateDirectory,
      transportStartupSuccessor: { verifyPredecessor }, reapDeadHosts: false });
    directories.push(directory);
    await expect(directory.start()).rejects.toBe(denied);
    expect(verifyPredecessor).toHaveBeenCalledOnce();
    expect(f.world.writes).toEqual([]);
    expect(directory.canConsumeMesh()).toBe(false);
  });

  it("does not truncate a portable TTL+2s startup to the ordinary five-second timeout", async () => {
    const f = fixture(), timeout = vi.spyOn(AbortSignal, "timeout");
    const verifyPredecessor = vi.fn(async () => {});
    const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: f.hostId, rootId: f.hostId,
      identity: { id: f.hostId, name: "retained", kind: "main" },
      transport: f.world.transport("machine-a", f.hostId), transportStateDir: f.stateDirectory,
      transportStartupSuccessor: { verifyPredecessor }, reapDeadHosts: false });
    directories.push(directory);
    await directory.start();
    expect(timeout).toHaveBeenCalledWith(17_000);
    expect(verifyPredecessor).toHaveBeenCalledOnce();
    expect(directory.canConsumeMesh()).toBe(true);
    expect(fs.readdirSync(f.stateDirectory).some(name => /^transport-host-lease-.*\.json$/.test(name))).toBe(true);
  });

  it("leaves ordinary startup at five seconds without a portable verifier", async () => {
    const f = fixture(), timeout = vi.spyOn(AbortSignal, "timeout");
    const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: f.hostId, rootId: f.hostId,
      identity: { id: f.hostId, name: "retained", kind: "main" },
      transport: f.world.transport("machine-a", f.hostId), transportStateDir: f.stateDirectory,
      reapDeadHosts: false });
    directories.push(directory);
    await directory.start();
    expect(timeout).toHaveBeenCalledWith(5_000);
    expect(timeout).not.toHaveBeenCalledWith(17_000);
    expect(directory.canConsumeMesh()).toBe(true);
  });

  it("keeps idle bootstrap lazy and forwards the activation before first runtime construction", async () => {
    const f = fixture();
    fs.mkdirSync(path.join(f.root, ".pi"));
    fs.writeFileSync(path.join(f.root, ".pi", "fabric.json"), JSON.stringify({ mesh: { enabled: false } }));
    const activation = { binding: { stateDirectory: f.stateDirectory }, identity: {} } as SessionRecoveryActivation;
    let options: Record<string, unknown> | undefined;
    const active = vi.fn(), setSessionRecovery = vi.fn();
    class Runtime {
      initialized = false;
      constructor(_pi: unknown, _catalog: unknown, value: Record<string, unknown>) { options = value; }
      initialize = async () => { this.initialized = true; };
      shutdown = async () => { this.initialized = false; };
      registerExternal = () => {};
      assertSessionRecoveryActive = active;
      setSessionRecovery = setSessionRecovery;
    }
    const loader = vi.fn(async () => ({ FabricRuntimeState: Runtime }));
    const state = new FabricState({} as ExtensionAPI, new CapturedToolCatalog(), { runtimeLoader: loader as never });
    const context = { cwd: f.root, isProjectTrusted: () => true,
      sessionManager: { getSessionId: () => "retained" }, ui: { setStatus() {} } } as unknown as ExtensionContext;
    try {
      state.setSessionRecovery(activation);
      await state.bootstrap(context);
      expect(loader).not.toHaveBeenCalled();
      expect(() => state.assertSessionRecoveryActive()).toThrow("has not activated");
      await state.ensure(context);
      expect(options?.sessionRecovery).toBe(activation);
      state.assertSessionRecoveryActive(); expect(active).toHaveBeenCalledOnce();
      state.setSessionRecovery(undefined);
      expect(setSessionRecovery).toHaveBeenCalledWith(undefined);
    } finally { await state.shutdown(); }
  });
});
