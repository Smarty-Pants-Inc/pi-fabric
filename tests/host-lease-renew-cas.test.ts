import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricHostLeaseSupersededError, hostLeasePath, readHostLeaseCurrent, renewHostLease, writeHostLease,
  type FabricHostLease } from "../src/topology/host-leases.js";
import { HostLeaseLockBusyError } from "../src/topology/host-lease-lock.js";

const stores: MeshStore[] = [], roots: string[] = [], directories: ParticipantDirectory[] = [];
afterEach(async () => {
  for (const directory of directories.splice(0)) await directory.close();
  for (const store of stores.splice(0)) store.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = (stateBackend: "file" | "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-cas-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend }); stores.push(mesh);
  expect(mesh.stateBackend).toBe(stateBackend);
  const lease: FabricHostLease = { id: "host", rootId: "root", identityId: "owner", startedAt: 1,
    updatedAt: Date.now(), expiresAt: Date.now() + 15_000 };
  const read = () => readHostLeaseCurrent(root, lease.id);
  const hold = (pid = process.pid) => {
    const lock = path.join(root, "host-lease-locks", path.basename(hostLeasePath(root, lease.id), ".json"));
    fs.mkdirSync(lock, { recursive: true });
    fs.writeFileSync(path.join(lock, "owner"), `test\n${pid}\n`);
    return lock;
  };
  return { root, mesh, lease, read, hold };
};

describe.each(["file", "sqlite"] as const)("%s host lease CAS", stateBackend => {
  it("matches identity AND startedAt, and never recreates a missing renewal", async () => {
    const s = setup(stateBackend);
    await renewHostLease(s.mesh, s.lease, { claim: true });
    for (const fields of [{ identityId: "other" }, { startedAt: 2 }]) {
      const successor = { ...s.lease, ...fields }; writeHostLease(s.root, successor);
      await expect(renewHostLease(s.mesh, s.lease)).rejects.toBeInstanceOf(FabricHostLeaseSupersededError);
      expect(s.read()).toEqual(successor);
    }
    fs.rmSync(hostLeasePath(s.root, s.lease.id));
    await expect(renewHostLease(s.mesh, s.lease)).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
    expect(s.read()).toBeUndefined();
  });
  it("does not shorten an ordinary deadline after the wall clock moves backwards", async () => {
    const s = setup(stateBackend); await renewHostLease(s.mesh, s.lease, { claim: true });
    await renewHostLease(s.mesh, { ...s.lease, updatedAt: s.lease.updatedAt - 10_000, expiresAt: s.lease.expiresAt - 10_000 });
    expect(s.read()).toEqual(s.lease);
  });
  it("uses a monotonic acquisition deadline, never age-steals a live receipt", async () => {
    const s = setup(stateBackend); const lock = s.hold();
    const ancient = new Date(0); fs.utimesSync(lock, ancient, ancient);
    let clock = Date.now(); vi.spyOn(Date, "now").mockImplementation(() => clock -= 1_000);
    const began = performance.now();
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 25 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(performance.now() - began).toBeLessThan(1_000);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(`test\n${process.pid}\n`);
    expect(s.read()).toBeUndefined();
  });
  it("cancels an acquisition without publishing or deleting the holder receipt", async () => {
    const s = setup(stateBackend); const lock = s.hold(); const controller = new AbortController();
    const pending = renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 10_000, signal: controller.signal });
    controller.abort(); await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(fs.existsSync(lock)).toBe(true); expect(s.read()).toBeUndefined();
  });
  it("recovers only a provably dead receipt under file custody", async () => {
    const s = setup(stateBackend); s.hold(999_999_999);
    await renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 1_000 });
    expect(s.read()).toEqual(s.lease);
  });
  it("close cancels a waiting directory preparation and publishes no lease", async () => {
    const s = setup(stateBackend); const lock = s.hold();
    const directory = new ParticipantDirectory(s.mesh, { enabled: true, hostId: s.lease.id, rootId: s.lease.rootId,
      identity: { id: s.lease.identityId, name: "owner", kind: "agent" }, reapDeadHosts: false });
    directories.push(directory);
    const starting = directory.start().then(() => undefined, (error: unknown) => error);
    await new Promise(resolve => setTimeout(resolve, 10));
    await directory.close();
    expect(await starting).toMatchObject({ name: "AbortError" });
    expect(s.read()).toBeUndefined(); expect(fs.existsSync(lock)).toBe(true);
    expect(directory.canConsumeMesh()).toBe(false);
  });
});
