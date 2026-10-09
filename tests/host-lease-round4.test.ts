import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { hostLeasePath, readHostLeaseCurrent, renewHostLease, withOwnedHostLease, writeHostLease,
  type FabricHostLease } from "../src/topology/host-leases.js";
import { readParticipantFiles, writeParticipantFile } from "../src/topology/participant-files.js";

const roots: string[] = [], stores: MeshStore[] = [], directories: ParticipantDirectory[] = [];
const realReadFile = fs.readFileSync;
beforeEach(() => {
  vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
    if (String(args[0]) === "/etc/machine-id") return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n";
    if (String(args[0]) === "/proc/sys/kernel/random/boot_id") return "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\n";
    return realReadFile(...args);
  });
});
afterEach(async () => {
  vi.restoreAllMocks();
  for (const directory of directories.splice(0)) await directory.close();
  for (const mesh of stores.splice(0)) mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "r4-owner", name: "owner", kind: "agent" };
const hostKey = (id: string) => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
const participantKey = (id: string) => "topology/participants/" + createHash("sha256").update(id).digest("hex");
const setup = (stateBackend: "file" | "sqlite" | "shadow") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-r4-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend }); stores.push(mesh);
  const lease: FabricHostLease = { id: "r4-host", rootId: "r4-root", identityId: identity.id,
    incarnationToken: randomUUID(), startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000 };
  return { root, mesh, lease };
};

describe.each(["file", "sqlite", "shadow"] as const)("%s Round 4 commit CAS", backend => {
  it.each(["legacy", "uuid", "missing"] as const)("%s overwrite after prepare rolls back all ops and callbacks", async overwrite => {
    const s = setup(backend); await renewHostLease(s.mesh, s.lease, { claim: true });
    await s.mesh.put({ key: "r4/keep", value: "before", identity });
    const before = s.mesh.listAll("", { fresh: true }), stamp = s.mesh.stateStamp();
    const replacement = { ...s.lease };
    if (overwrite === "legacy") delete replacement.incarnationToken;
    else replacement.incarnationToken = randomUUID();
    const prepare = vi.fn(() => []), afterCommit = vi.fn(), commitOutbox = vi.fn();
    await expect(withOwnedHostLease(s.mesh, s.lease, () => s.mesh.writeBatch({ identity,
      prepare, afterCommit, commitOutbox, ops: [
        { kind: "delete", key: "r4/keep" },
        { kind: "put", key: "r4/new", value: () => {
          if (overwrite === "missing") fs.rmSync(hostLeasePath(s.root, s.lease.id));
          else writeHostLease(s.root, replacement); // Old releases bypass the gate; happens AFTER prepare.
          return "must not commit";
        } },
      ] }))).rejects.toMatchObject({ code: overwrite === "legacy" ? "FABRIC_HOST_LEASE_CONTESTED" : "FABRIC_HOST_LEASE_SUPERSEDED", retryable: false });
    expect(prepare).toHaveBeenCalledOnce(); expect(afterCommit).not.toHaveBeenCalled(); expect(commitOutbox).not.toHaveBeenCalled();
    expect(s.mesh.listAll("", { fresh: true })).toEqual(before); expect(s.mesh.stateStamp()).toBe(stamp);
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(overwrite === "missing" ? undefined : replacement);
  });

  it.skipIf(backend === "sqlite")("legacy overwrite AFTER state-file staging still aborts before rename", async () => {
    const s = setup(backend); await renewHostLease(s.mesh, s.lease, { claim: true });
    await s.mesh.put({ key: "r4/keep", value: "before", identity });
    const before = s.mesh.listAll("", { fresh: true }), stamp = s.mesh.stateStamp();
    const legacy = { ...s.lease }; delete legacy.incarnationToken;
    const write = fs.writeFileSync; let overwrote = false;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, ...args) => {
      write(file, ...args);
      if (String(file).startsWith(path.join(s.root, "state.json."))) {
        overwrote = true; writeHostLease(s.root, legacy);
      }
    });
    const commitOutbox = vi.fn();
    await expect(withOwnedHostLease(s.mesh, s.lease, () => s.mesh.writeBatch({ identity, commitOutbox,
      ops: [{ kind: "delete", key: "r4/keep" }] }))).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_CONTESTED" });
    expect(overwrote).toBe(true); expect(commitOutbox).not.toHaveBeenCalled();
    expect(s.mesh.listAll("", { fresh: true })).toEqual(before); expect(s.mesh.stateStamp()).toBe(stamp);
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(legacy);
    expect(fs.readdirSync(s.root).filter(file => file.startsWith("state.json.") && file.endsWith(".tmp"))).toEqual([]);
  });

  it.skipIf(backend === "sqlite")("legacy overwrite during a transient rename retry is rechecked", async () => {
    const s = setup(backend); await renewHostLease(s.mesh, s.lease, { claim: true });
    await s.mesh.put({ key: "r4/keep", value: "before", identity });
    const before = s.mesh.listAll("", { fresh: true }), legacy = { ...s.lease }; delete legacy.incarnationToken;
    const rename = fs.renameSync; let attempts = 0;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === path.join(s.root, "state.json")) {
        attempts++; writeHostLease(s.root, legacy);
        throw Object.assign(new Error("transient rename denial"), { code: "EPERM" });
      }
      return rename(from, to);
    });
    await expect(withOwnedHostLease(s.mesh, s.lease, () => s.mesh.writeBatch({ identity,
      ops: [{ kind: "delete", key: "r4/keep" }] }))).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_CONTESTED" });
    expect(attempts).toBe(1); expect(s.mesh.listAll("", { fresh: true })).toEqual(before);
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(legacy);
  });

  it("a write-free batch checks contested ownership before its callbacks", async () => {
    const s = setup(backend); await renewHostLease(s.mesh, s.lease, { claim: true });
    const legacy = { ...s.lease }; delete legacy.incarnationToken;
    const afterCommit = vi.fn(), commitOutbox = vi.fn();
    await expect(withOwnedHostLease(s.mesh, s.lease, () => s.mesh.writeBatch({ identity, ops: [], afterCommit, commitOutbox,
      prepare: () => { writeHostLease(s.root, legacy); return []; },
    }))).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_CONTESTED" });
    expect(afterCommit).not.toHaveBeenCalled(); expect(commitOutbox).not.toHaveBeenCalled();
  });

  it("the real directory batch fails closed on an in-flight legacy overwrite", async () => {
    const s = setup(backend);
    const directory = new ParticipantDirectory(s.mesh, { enabled: true, hostId: s.lease.id, rootId: s.lease.rootId,
      identity, reapDeadHosts: false, heartbeatMs: 60_000, leaseMs: 120_000 });
    directories.push(directory); await directory.start();
    const before = s.mesh.listAll("", { fresh: true }), confirmed = directory.confirmedAt();
    directory.registerSource(() => [{ format: 1, id: "r4-agent", kind: "agent", rootId: s.lease.rootId,
      ownerHostId: s.lease.id, ownerIdentityId: identity.id, name: "candidate", status: "running", runner: "pi",
      transport: "process", capabilities: [], startedAt: 1, updatedAt: 1, controlProtocol: "v1" }]);
    const legacy = { ...readHostLeaseCurrent(s.root, s.lease.id)! }; delete legacy.incarnationToken;
    const writeBatch = s.mesh.writeBatch.bind(s.mesh);
    vi.spyOn(s.mesh, "writeBatch").mockImplementationOnce(input => writeBatch({ ...input,
      prepare: view => [...(input.prepare?.(view) ?? []), { kind: "put", key: "r4/contested", value: () => {
        writeHostLease(s.root, legacy); return "must not commit";
      } }] }));
    await expect(directory.refresh()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_CONTESTED", retryable: false });
    expect(s.mesh.listAll("", { fresh: true })).toEqual(before);
    expect(directory.confirmedAt()).toBe(confirmed); expect(directory.canConsumeMesh()).toBe(false);
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(legacy);
  });
});

describe.each(["file", "sqlite"] as const)("%s Round 4 target reaping", backend => {
  const fixture = async () => {
    const s = setup(backend), now = Date.now(), target = { ...s.lease, id: "r4-target",
      updatedAt: now - 8 * 3_600_000, expiresAt: now - 7 * 3_600_000 };
    await renewHostLease(s.mesh, s.lease, { claim: true }); await renewHostLease(s.mesh, target, { claim: true });
    await s.mesh.put({ key: hostKey(target.id), identity, value: { id: target.id, rootId: target.rootId,
      identity, incarnationToken: target.incarnationToken, expiresAt: target.expiresAt } });
    const entry = await s.mesh.put({ key: participantKey("r4-participant"), identity,
      value: { id: "r4-participant", ownerHostId: target.id } });
    writeParticipantFile(s.root, entry);
    const before = s.mesh.listAll("", { fresh: true }), files = readParticipantFiles(s.root);
    const reap = () => reapDeadHostRecords(s.mesh, identity, { ownHostId: s.lease.id, now,
      withCommitFence: operation => withOwnedHostLease(s.mesh, s.lease, operation) });
    return { ...s, target, now, before, files, reap };
  };

  it.each([false, true])("target renews between selection and target gate (new UUID=%s): delete NOTHING", async takeover => {
    const s = await fixture();
    let resume!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; }), waiting = new Promise<void>(resolve => { entered = resolve; });
    const custody = s.mesh.leaseCustody.bind(s.mesh); let armed = true;
    vi.spyOn(s.mesh, "leaseCustody").mockImplementation(async (file, operation, timeout, options) => {
      if (armed && file === hostLeasePath(s.root, s.target.id)) { armed = false; entered(); await gate; }
      return custody(file, operation, timeout, options);
    });
    const pending = s.reap();
    const peer = new MeshStore(s.root, 65_536, 100, { stateBackend: backend }); stores.push(peer);
    try {
      await waiting;
      const renewed = { ...s.target, updatedAt: s.now, expiresAt: s.now + 60_000,
        incarnationToken: takeover ? randomUUID() : s.target.incarnationToken! };
      await renewHostLease(peer, renewed, takeover ? { claim: true } : {}); resume();
      expect(await pending).toBe(0); expect(s.mesh.listAll("", { fresh: true })).toEqual(s.before);
      expect(readParticipantFiles(s.root)).toEqual(s.files); expect(readHostLeaseCurrent(s.root, s.target.id)).toEqual(renewed);
    } finally { resume(); await pending; }
  });

  it("final CAS rejects a target renewal after prepare without deleting state or files", async () => {
    const s = await fixture(), renewed = { ...s.target, updatedAt: s.now, expiresAt: s.now + 60_000 };
    const writeBatch = s.mesh.writeBatch.bind(s.mesh);
    vi.spyOn(s.mesh, "writeBatch").mockImplementationOnce(input => writeBatch({ ...input,
      prepare: view => [...(input.prepare?.(view) ?? []), { kind: "put", key: "r4/reaper-race", value: () => {
        writeHostLease(s.root, renewed); return "must not commit";
      } }] }));
    expect(await s.reap()).toBe(0); expect(s.mesh.listAll("", { fresh: true })).toEqual(s.before);
    expect(readParticipantFiles(s.root)).toEqual(s.files); expect(readHostLeaseCurrent(s.root, s.target.id)).toEqual(renewed);
  });

  it("a busy target commit gate retains state, participant files, and the target lease", async () => {
    const s = await fixture(); let resume!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { resume = resolve; }), waiting = new Promise<void>(resolve => { entered = resolve; });
    const peer = new MeshStore(s.root, 65_536, 100, { stateBackend: backend }); stores.push(peer);
    const pending = peer.leaseCustody(hostLeasePath(s.root, s.target.id), async () => { entered(); await gate; });
    try {
      await waiting; expect(await s.reap()).toBe(0);
      expect(s.mesh.listAll("", { fresh: true })).toEqual(s.before); expect(readParticipantFiles(s.root)).toEqual(s.files);
      expect(readHostLeaseCurrent(s.root, s.target.id)).toEqual(s.target);
    } finally { resume(); await pending; }
  });

  it("holds the TARGET gate through state commit and exact lease compare-delete", async () => {
    const s = await fixture(), custody = s.mesh.leaseCustody.bind(s.mesh), held = new Set<string>();
    vi.spyOn(s.mesh, "leaseCustody").mockImplementation((file, operation, timeout, options) => custody(file, async () => {
      held.add(file); try { return await operation(); } finally { held.delete(file); }
    }, timeout, options));
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === hostLeasePath(s.root, s.target.id)) expect(held.has(hostLeasePath(s.root, s.target.id))).toBe(true);
      return rename(from, to);
    });
    expect(await s.reap()).toBe(3); expect(readHostLeaseCurrent(s.root, s.target.id)).toBeUndefined();
    expect(readParticipantFiles(s.root)).toEqual([]); expect(s.mesh.get(hostKey(s.target.id), { fresh: true })).toBeUndefined();
    expect(readHostLeaseCurrent(s.root, s.lease.id)).toEqual(s.lease);
  });
});
