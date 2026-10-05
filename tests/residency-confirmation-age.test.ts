import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import * as files from "../src/topology/participant-files.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

vi.mock("../src/topology/participant-files.js", async original => ({ ...await original<typeof import("../src/topology/participant-files.js")>(), writeParticipantFileIf: vi.fn() }));
const pause = (ms: number) => new Promise(r => setTimeout(r, ms));
it("file-only confirmation is stamped at probe completion, preserving delayed continuation age", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-confirmation-lock-"));
  const mesh = new MeshStore(root, 65536, 1000, { lockTimeoutMs: 2000 });
  const identity = { id: "owner", name: "owner", kind: "main" as const };
  await mesh.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, hostLeases: "files" } });
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: "owner", rootId: "root", identity, heartbeatMs: 100 });
  try {
    await directory.start();
    let receipt = 0;
    const confirm = mesh.confirmWritable.bind(mesh);
    const fault = vi.spyOn(mesh, "confirmWritable").mockImplementation(async callback => {
      await pause(340); // delayed filesystem probe, independent of the mesh lock
      await confirm(at => { receipt = at; callback?.(at); });
      await pause(320); // successful probe, but delayed promise continuation
    });
    fs.mkdirSync(path.join(root, ".lock")); fs.writeFileSync(path.join(root, ".lock", "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
    const started = Date.now(); const renewing = directory.refresh();
    await pause(320); fs.rmSync(path.join(root, ".lock"), { recursive: true });
    await renewing;
    expect(receipt).toBeGreaterThanOrEqual(started + 300); // not attempt start/pre-probe read
    expect(directory.confirmedAt()).toBe(receipt); // not continuation end
    expect(directory.canConsumeMesh()).toBe(false);
    fault.mockRestore(); await directory.refresh(); expect(directory.canConsumeMesh()).toBe(true);
  } finally { fs.rmSync(path.join(root, ".lock"), { recursive: true, force: true }); await directory.close(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
});

it("preserves shared commit age across overdue copies; consumption and sequence stay fenced until a real renewal", { timeout: 10000 }, async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-confirmation-age-"));
  const mesh = new MeshStore(root, 65536, 1000, { lockTimeoutMs: 100 });
  const identity = { id: "owner", name: "owner", kind: "main" as const };
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: "owner", rootId: "root", identity, heartbeatMs: 100 });
  directory.registerSource(() => [{ format: 1, id: "actor", kind: "actor", rootId: "root", ownerHostId: "owner", ownerIdentityId: "owner", name: "actor", status: "idle", runner: "pi", transport: "process", capabilities: [], controlProtocol: "v1", startedAt: 1, updatedAt: 1 }]);
  let copied!: () => void, release!: () => void;
  const entered = new Promise<void>(r => { copied = r; }), blocked = new Promise<void>(r => { release = r; });
  const real = await vi.importActual<typeof files>("../src/topology/participant-files.js");
  vi.mocked(files.writeParticipantFileIf).mockImplementation(async (...args) => { copied(); await blocked; return real.writeParticipantFileIf(...args); });
  const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: "owner", pollMs: 20, canConsumeMesh: () => directory.canConsumeMesh() });
  const handler = vi.fn(() => ({ accepted: true, messageId: "once" })); control.start(handler);
  const sender = new FabricControlPlane(new MeshStore(root, 65536, 1000), { ...identity, id: "sender" }, { enabled: true, hostId: "sender", pollMs: 20, acknowledgementTimeoutMs: 5000 }); sender.start(() => ({ accepted: false }));
  const starting = directory.start(); let result: Promise<unknown> | undefined;
  try {
    await entered;
    const commitAt = (mesh.listAll("topology/hosts/")[0]?.value as { updatedAt: number }).updatedAt;
    fs.mkdirSync(path.join(root, ".lock")); fs.writeFileSync(path.join(root, ".lock", "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
    await pause(320); release(); await starting;
    expect(directory.confirmedAt()).toBeLessThanOrEqual(commitAt + 20);
    // Block both shared renewal paths before canConsumeMesh schedules its recovery.
    // An unchanged heartbeat now confirms writability without rewriting shared state.
    const batch = mesh.writeBatch.bind(mesh);
    const confirm = mesh.confirmWritable.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockRejectedValue(new Error("shared renewal blocked"));
    vi.spyOn(mesh, "confirmWritable").mockRejectedValue(new Error("shared confirmation blocked"));
    expect(directory.canConsumeMesh()).toBe(false);
    // Stage control traffic after the overdue copy: no handler or sequence while the lock is unavailable.
    fs.rmSync(path.join(root, ".lock"), { recursive: true });
    result = sender.request("owner", "actor", "followUp", { message: "work" }).catch(e => e);
    await pause(180);
    expect(handler).not.toHaveBeenCalled();
    for (const dir of fs.readdirSync(path.join(root, "control-seen"))) {
      const seen = new MeshStore(path.join(root, "control-seen", dir), 65536, 1000);
      expect(seen.listAll("topology/control-seen/").every(entry => !(entry.value as { sequence?: number }).sequence)).toBe(true);
    }
    vi.mocked(mesh.writeBatch).mockImplementation(batch);
    vi.mocked(mesh.confirmWritable).mockImplementation(confirm);
    await directory.refresh();
    expect(directory.canConsumeMesh()).toBe(true);
    expect(await result).toMatchObject({ acknowledged: true }); expect(handler).toHaveBeenCalledOnce();
  } finally { release(); fs.rmSync(path.join(root, ".lock"), { recursive: true, force: true }); await starting.catch(() => undefined); await directory.close(); await control.close(); await sender.close(); await result; vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
});
