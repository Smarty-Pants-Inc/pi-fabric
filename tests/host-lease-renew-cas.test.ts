import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricHostLeaseSupersededError, hostLeasePath, readHostLeaseCurrent, removeOwnedHostLease, renewHostLease, writeHostLease,
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
  const lease: FabricHostLease = { id: "host", rootId: "root", identityId: "owner", incarnationToken: randomUUID(), startedAt: 1,
    updatedAt: Date.now(), expiresAt: Date.now() + 15_000 };
  const read = () => readHostLeaseCurrent(root, lease.id);
  const hold = (pid = process.pid, host = os.hostname(), expiresAt?: number) => {
    const lock = path.join(root, "host-lease-locks", path.basename(hostLeasePath(root, lease.id), ".json"));
    fs.mkdirSync(lock, { recursive: true });
    fs.writeFileSync(path.join(lock, "owner"), JSON.stringify({ token: randomUUID(), pid, host,
      ...(expiresAt !== undefined ? { expiresAt, incarnationToken: lease.incarnationToken } : {}) }));
    return lock;
  };
  return { root, mesh, lease, read, hold };
};

describe.each(["file", "sqlite"] as const)("%s host lease CAS", stateBackend => {
  it("matches identity AND UUID, and never recreates a missing renewal", async () => {
    const s = setup(stateBackend);
    await renewHostLease(s.mesh, s.lease, { claim: true });
    for (const fields of [{ identityId: "other" }, { incarnationToken: randomUUID() }]) {
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
    const receipt = fs.readFileSync(path.join(lock, "owner"), "utf8");
    const ancient = new Date(0); fs.utimesSync(lock, ancient, ancient);
    let clock = Date.now(); vi.spyOn(Date, "now").mockImplementation(() => clock -= 1_000);
    const began = performance.now();
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 25 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(performance.now() - began).toBeLessThan(1_000);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(receipt);
    expect(s.read()).toBeUndefined();
  });
  it("cancels an acquisition without publishing or deleting the holder receipt", async () => {
    const s = setup(stateBackend); const lock = s.hold(); const controller = new AbortController();
    const pending = renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 10_000, signal: controller.signal });
    controller.abort(); await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(fs.existsSync(lock)).toBe(true); expect(s.read()).toBeUndefined();
  });
  it("recovers only a provably dead local receipt under file custody", async () => {
    const s = setup(stateBackend); s.hold(999_999_999);
    await renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 1_000 });
    expect(s.read()).toEqual(s.lease);
  });
  it("uses the UUID, not repeated or backwards start-time metadata, for renew/release", async () => {
    const s = setup(stateBackend); await renewHostLease(s.mesh, s.lease, { claim: true });
    await renewHostLease(s.mesh, { ...s.lease, startedAt: -1 });
    expect(s.read()?.startedAt).toBe(-1);
    const successor = { ...s.lease, incarnationToken: randomUUID() };
    await renewHostLease(s.mesh, successor, { claim: true });
    await expect(renewHostLease(s.mesh, s.lease)).rejects.toBeInstanceOf(FabricHostLeaseSupersededError);
    expect(await removeOwnedHostLease(s.mesh, s.lease)).toBe(false);
    expect(s.read()).toEqual(successor);
  });
  it.each([false, true])("rejects a stale claim snapshot after a successor writes (initially absent: %s)", async absent => {
    const s = setup(stateBackend);
    if (!absent) await renewHostLease(s.mesh, s.lease, { claim: true });
    const stale = { ...s.lease, incarnationToken: randomUUID() };
    const successor = { ...s.lease, incarnationToken: randomUUID() };
    let resume!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; });
    const waiting = new Promise<void>(resolve => { entered = resolve; });
    const custody = s.mesh.leaseCustody.bind(s.mesh);
    vi.spyOn(s.mesh, "leaseCustody").mockImplementationOnce(async (file, operation, timeout) => {
      entered(); await gate; return custody(file, operation, timeout);
    });
    const pending = renewHostLease(s.mesh, stale, { claim: true }).then(() => undefined, (error: unknown) => error);
    try {
      await waiting; await renewHostLease(s.mesh, successor, { claim: true }); resume();
      expect(await pending).toBeInstanceOf(FabricHostLeaseSupersededError);
      expect(s.read()).toEqual(successor);
    } finally { resume(); await pending; }
  });
  it("does not infer death of a foreign PID before its lease-bound deadline", async () => {
    const s = setup(stateBackend); const lock = s.hold(999_999_999, "another-host", Date.now() + 60_000);
    const receipt = fs.readFileSync(path.join(lock, "owner"), "utf8");
    const kill = vi.spyOn(process, "kill");
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 25 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(receipt);
    expect(kill).not.toHaveBeenCalled(); expect(s.read()).toBeUndefined();
  });
  it("recovers an expired foreign receipt and rejects its resumed owner's token CAS", async () => {
    const s = setup(stateBackend); await renewHostLease(s.mesh, s.lease, { claim: true });
    s.hold(999_999_999, "another-host", Date.now() - 1);
    let resume!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; });
    const waiting = new Promise<void>(resolve => { entered = resolve; });
    const custody = s.mesh.leaseCustody.bind(s.mesh);
    vi.spyOn(s.mesh, "leaseCustody").mockImplementationOnce(async (file, operation, timeout) => {
      entered(); await gate; return custody(file, operation, timeout);
    });
    const pending = renewHostLease(s.mesh, s.lease).then(() => undefined, (error: unknown) => error);
    const successor = { ...s.lease, incarnationToken: randomUUID() };
    try {
      await waiting; await renewHostLease(s.mesh, successor, { claim: true, timeoutMs: 1_000 }); resume();
      expect(await pending).toBeInstanceOf(FabricHostLeaseSupersededError);
      expect(await removeOwnedHostLease(s.mesh, s.lease)).toBe(false);
      expect(s.read()).toEqual(successor);
    } finally { resume(); await pending; }
  });
  it("wakes acquisition on lock removal instead of repeated 5 ms timer checks", async () => {
    const s = setup(stateBackend); const lock = s.hold();
    const watch = vi.spyOn(fs, "watch");
    const reads = vi.spyOn(fs, "readFileSync");
    const releasing = setTimeout(() => fs.rmSync(lock, { recursive: true, force: true }), 30);
    try {
      await renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 1_000 });
      expect(watch).toHaveBeenCalledOnce();
      expect(reads.mock.calls.filter(([file]) => file === path.join(lock, "owner")).length).toBeLessThan(8);
      expect(s.read()).toEqual(s.lease);
    } finally { clearTimeout(releasing); }
  });
  it("bounds the active-acquisition fallback when fs.watch is unsupported", async () => {
    const s = setup(stateBackend); s.hold();
    const watch = vi.spyOn(fs, "watch").mockImplementation(() => { throw new Error("watch unsupported"); });
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 45 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(watch.mock.calls.length).toBeLessThanOrEqual(5);
    expect(s.read()).toBeUndefined();
  });
  it.each(["another-host", "unknown"])("does not PID-recover a %s per-host commit gate", async host => {
    const s = setup(stateBackend);
    const file = hostLeasePath(s.root, s.lease.id);
    const domain = path.join(s.root, "host-lease-commits", createHash("sha256").update(path.basename(file)).digest("hex"));
    const lock = path.join(domain, "custody.lock"); fs.mkdirSync(lock, { recursive: true });
    const owner = host === "unknown" ? `legacy\n999999999\n1\n` : `gate\n999999999\n1\n\n${host}\n`;
    fs.writeFileSync(path.join(lock, "owner"), owner);
    const kill = vi.spyOn(process, "kill");
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 25 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(kill).not.toHaveBeenCalled(); expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
    expect(s.read()).toBeUndefined();
  });
  it("recovers a provably dead local commit gate without waiting on shared state", async () => {
    const s = setup(stateBackend); const file = hostLeasePath(s.root, s.lease.id);
    const domain = path.join(s.root, "host-lease-commits", createHash("sha256").update(path.basename(file)).digest("hex"));
    const lock = path.join(domain, "custody.lock"); fs.mkdirSync(lock, { recursive: true });
    fs.writeFileSync(path.join(lock, "owner"), `gate\n999999999\n1\n\n${os.hostname()}\n`);
    await renewHostLease(s.mesh, s.lease, { claim: true }); expect(s.read()).toEqual(s.lease);
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
