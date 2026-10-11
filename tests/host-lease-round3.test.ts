import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { readPhysicalHostIdentity } from "../src/core/atomic-write.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { HostLeaseLockBusyError } from "../src/topology/host-lease-lock.js";
import { hostLeasePath, readHostLeaseCurrent, renewHostLease, writeHostLease,
  withOwnedHostLease, removeOwnedHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { readParticipantFiles } from "../src/topology/participant-files.js";

// A deterministic physical host, independent of the OS running this unit suite.
// Production non-Linux/missing identity still fails closed; unreadable paths are tested below.
const realReadFile = fs.readFileSync;
const readWithPhysicalFixture = (...args: Parameters<typeof fs.readFileSync>) => {
  if (String(args[0]) === "/etc/machine-id") return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n";
  if (String(args[0]) === "/proc/sys/kernel/random/boot_id") return "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n";
  return realReadFile(...args);
};
beforeEach(() => { vi.spyOn(fs, "readFileSync").mockImplementation(readWithPhysicalFixture); });
const roots: string[] = [], stores: MeshStore[] = [], directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.restoreAllMocks(); vi.useRealTimers();
  for (const directory of directories.splice(0)) await directory.close();
  for (const mesh of stores.splice(0)) mesh.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = (stateBackend: "file" | "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-r3-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend }); stores.push(mesh);
  const lease: FabricHostLease = { id: "r3-host", rootId: "r3-root", identityId: "r3-owner",
    incarnationToken: randomUUID(), startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 };
  return { root, mesh, lease };
};
const makeDirectory = async (mesh: MeshStore) => {
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: "r3-host", rootId: "r3-root",
    identity: { id: "r3-owner", name: "r3-owner", kind: "agent" }, reapDeadHosts: false,
    heartbeatMs: 60_000, leaseMs: 120_000 });
  directories.push(directory); await directory.start(); return directory;
};
const candidate = { format: 1 as const, id: "r3-agent", kind: "agent" as const, rootId: "r3-root",
  ownerHostId: "r3-host", ownerIdentityId: "r3-owner", name: "r3-agent", status: "running" as const,
  runner: "pi" as const, transport: "process" as const, capabilities: [] as [], startedAt: 1, updatedAt: 1, controlProtocol: "v1" as const };

describe.each(["file", "sqlite"] as const)("%s Round 3", backend => {
  it.each([false, true])("pause after renew, successor claims only its file token, resume -> no write (files-only=%s)", async filesOnly => {
    const s = setup(backend);
    if (filesOnly) await s.mesh.put({ key: "topology/liveness", value: { version: 1, hostLeases: "files", participants: "files" },
      identity: { id: "policy", name: "policy", kind: "agent" } });
    const directory = await makeDirectory(s.mesh);
    const prior = directory.confirmedAt(), before = s.mesh.listAll("", { fresh: true });
    directory.registerSource(() => [candidate]);
    let resume!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; });
    const waiting = new Promise<void>(resolve => { entered = resolve; });
    const custody = s.mesh.leaseCustody.bind(s.mesh);
    vi.spyOn(s.mesh, "leaseCustody").mockImplementation(async (file, operation, timeout) => {
      if (operation.constructor.name === "AsyncFunction") { entered(); await gate; }
      return custody(file, operation, timeout);
    });
    const batch = vi.spyOn(s.mesh, "writeBatch");
    const pending = directory.refresh().then(() => undefined, (error: unknown) => error);
    try {
      await waiting;
      const successor = { ...readHostLeaseCurrent(s.root, s.lease.id)!, incarnationToken: randomUUID() };
      await renewHostLease(s.mesh, successor, { claim: true });
      resume();
      expect(await pending).toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED", retryable: false });
      expect(batch).not.toHaveBeenCalled();
      expect(s.mesh.listAll("", { fresh: true })).toEqual(before);
      expect(readParticipantFiles(s.root)).toEqual([]);
      expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(successor);
      expect(directory.confirmedAt()).toBe(prior); expect(directory.canConsumeMesh()).toBe(false);
    } finally { resume(); await pending; }
  });

  it("directory propagates no-source lock busy without a jittered publication-retry timer", async () => {
    const s = setup(backend), directory = await makeDirectory(s.mesh), physical = readPhysicalHostIdentity()!;
    directory.options.waitForPublicationRetry = vi.fn(async () => {});
    const file = hostLeasePath(s.root, s.lease.id);
    const domain = path.join(s.root, "host-lease-commits", createHash("sha256").update(path.basename(file)).digest("hex"));
    const lock = path.join(domain, "custody.lock"); fs.mkdirSync(lock, { recursive: true });
    fs.writeFileSync(path.join(lock, "owner"), `busy\n${process.pid}\n1\n\n${physical.machineId}\n${physical.bootId}\n`);
    const timer = vi.spyOn(globalThis, "setTimeout");
    await expect(directory.refresh()).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(timer).not.toHaveBeenCalled(); expect(directory.options.waitForPublicationRetry).not.toHaveBeenCalled();
    fs.rmSync(lock, { recursive: true, force: true });
  });

  it("holds lease custody until an asynchronous shared transaction finishes", async () => {
    const s = setup(backend); await renewHostLease(s.mesh, s.lease, { claim: true });
    let resume!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; });
    const waiting = new Promise<void>(resolve => { entered = resolve; });
    const pending = withOwnedHostLease(s.mesh, s.lease, async () => { entered(); await gate; return 42; });
    try {
      await waiting;
      await expect(renewHostLease(s.mesh, { ...s.lease, incarnationToken: randomUUID() }, { claim: true, timeoutMs: 50 }))
        .rejects.toBeInstanceOf(HostLeaseLockBusyError);
      expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(s.lease);
      resume(); expect(await pending).toBe(42);
    } finally { resume(); await pending; }
  });

  it("fresh legacy lease blocks zero-wait UUID admission", async () => {
    const s = setup(backend), legacy = { ...s.lease }; delete legacy.incarnationToken; writeHostLease(s.root, legacy);
    const timer = vi.spyOn(globalThis, "setTimeout");
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 0 }))
      .rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_LEGACY_BUSY", expiresAt: legacy.expiresAt });
    expect(timer).not.toHaveBeenCalled(); expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(legacy);
  });

  it("arms one expiry timer and never rearms if a legacy writer extends its lease", async () => {
    const s = setup(backend), legacy = { ...s.lease, expiresAt: Date.now() + 1_000 }; delete legacy.incarnationToken;
    writeHostLease(s.root, legacy);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const timer = vi.spyOn(globalThis, "setTimeout");
    const pending = renewHostLease(s.mesh, s.lease, { claim: true }).then(() => undefined, (error: unknown) => error);
    expect(timer).toHaveBeenCalledOnce();
    const extended = { ...legacy, expiresAt: legacy.expiresAt + 60_000 }; writeHostLease(s.root, extended);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(await pending).toMatchObject({ code: "FABRIC_HOST_LEASE_LEGACY_BUSY", expiresAt: extended.expiresAt });
    expect(timer).toHaveBeenCalledOnce(); expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(extended);
  });

  it("one expiry wake can claim an expired legacy predecessor", async () => {
    const s = setup(backend), legacy = { ...s.lease, expiresAt: Date.now() + 15 }; delete legacy.incarnationToken;
    writeHostLease(s.root, legacy);
    await renewHostLease(s.mesh, s.lease, { claim: true });
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(s.lease);
  });

  it("legacy overwrite after UUID claim is contested and terminal, never repaired", async () => {
    const s = setup(backend), directory = await makeDirectory(s.mesh);
    const before = s.mesh.listAll("", { fresh: true }), legacy = { ...readHostLeaseCurrent(s.root, s.lease.id)! };
    delete legacy.incarnationToken; writeHostLease(s.root, legacy);
    await expect(removeOwnedHostLease(s.mesh, s.lease)).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_CONTESTED" });
    await expect(directory.refresh()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_CONTESTED", retryable: false });
    expect(directory.canConsumeMesh()).toBe(false);
    expect(s.mesh.listAll("", { fresh: true })).toEqual(before);
    const restored = { ...legacy, incarnationToken: s.lease.incarnationToken! }; writeHostLease(s.root, restored);
    await expect(directory.refresh()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_CONTESTED" });
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(restored);
  });
});

describe("physical-host-qualified commit custody", () => {
  const gate = (root: string, machineId: string, bootId: string, pid = 999_999_999) => {
    const lock = path.join(root, "custody.lock"); fs.mkdirSync(lock, { recursive: true });
    const owner = `gate\n${pid}\n1\n\n${machineId}\n${bootId}\n`;
    fs.writeFileSync(path.join(lock, "owner"), owner); return { lock, owner };
  };
  it("two hosts with the same hostname but different machine-id are FOREIGN", async () => {
    const s = setup("file"), physical = readPhysicalHostIdentity()!;
    const foreignId = physical.machineId === "a".repeat(32) ? "b".repeat(32) : "a".repeat(32);
    const held = gate(s.root, foreignId, physical.bootId);
    const hostname = vi.spyOn(os, "hostname").mockReturnValue("same-on-both-hosts");
    const kill = vi.spyOn(process, "kill");
    await expect(acquireMeshCustodyLock(s.root, 0, { hostQualified: true })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(kill).not.toHaveBeenCalled(); expect(hostname).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(held.lock, "owner"), "utf8")).toBe(held.owner);
  });
  it("same machine-id with a previous boot is recoverable even if its PID is live now", async () => {
    const s = setup("file"), physical = readPhysicalHostIdentity()!;
    const oldBoot = physical.bootId === "00000000-0000-0000-0000-000000000000"
      ? "11111111-1111-1111-1111-111111111111" : "00000000-0000-0000-0000-000000000000";
    gate(s.root, physical.machineId, oldBoot, process.pid);
    const kill = vi.spyOn(process, "kill");
    const release = await acquireMeshCustodyLock(s.root, 0, { hostQualified: true });
    try { expect(kill).not.toHaveBeenCalled(); } finally { release(); }
  });
  it.each(["/etc/machine-id", "/proc/sys/kernel/random/boot_id"])("unreadable %s fails closed", async missing => {
    const s = setup("file"), physical = readPhysicalHostIdentity()!;
    const held = gate(s.root, physical.machineId, physical.bootId);
    vi.spyOn(fs, "readFileSync").mockImplementation((file, options) => {
      if (String(file) === missing) throw Object.assign(new Error("unreadable"), { code: "EACCES" });
      return readWithPhysicalFixture(file, options);
    });
    const kill = vi.spyOn(process, "kill");
    await expect(acquireMeshCustodyLock(s.root, 0, { hostQualified: true })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(kill).not.toHaveBeenCalled(); expect(fs.readFileSync(path.join(held.lock, "owner"), "utf8")).toBe(held.owner);
  });
  it("busy custody without an event source makes exactly one attempt and no timer", async () => {
    const s = setup("file"), physical = readPhysicalHostIdentity()!;
    const file = hostLeasePath(s.root, s.lease.id);
    const domain = path.join(s.root, "host-lease-commits", createHash("sha256").update(path.basename(file)).digest("hex"));
    gate(domain, physical.machineId, physical.bootId, process.pid);
    const timers = vi.spyOn(globalThis, "setTimeout"), reads = vi.spyOn(fs, "readFileSync");
    await expect(renewHostLease(s.mesh, s.lease, { claim: true, timeoutMs: 1_000 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
    expect(timers).not.toHaveBeenCalled();
    expect(reads.mock.calls.filter(([p]) => p === path.join(domain, "custody.lock", "owner"))).toHaveLength(2);
  });
});
