import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeOwnershipError, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeaseGeneration, hostLeasePath, hostLiveness, readHostLease, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#6477 L2b (R11): the bridge mirror's host-lease effects run after its state commit from
// rows recorded in that commit, and its participant-file reads happen before the transaction.
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);
const opened: MeshStore[] = [];
const store = (stateBackend?: "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-bridge-outbox-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, stateBackend ? { stateBackend } : {});
  opened.push(mesh);
  return mesh;
};
afterEach(() => { for (const mesh of opened.splice(0)) mesh.closeState(); });
// A rename that installs a lease file (not the owner-matched removal's rename aside).
const leaseWrite = (to: string) => to.includes(`${path.sep}host-leases${path.sep}`) && to.endsWith(".json");
const bridgeIdentity: MeshIdentity = { id: "bridge:ryzen2", name: "ryzen2", kind: "main" };

// A native of the peer, as the far side reports it.
const remote = (name: string, now: number) => {
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  const host = { format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + 60_000 } as unknown as FabricHostRecord;
  const participant = {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } as unknown as FabricParticipantRecord;
  return { identity, host, participant, presence: { hosts: [{ record: host, expiresAt: now + 60_000 }], participants: [participant] } };
};

describe("bridge mirror commit outbox", () => {
  // Trust boundary: a completed-but-not-retired effect is still protected work.
  // A skipped root must remain byte-exact without blocking unrelated retirements.
  it.each(["file", "sqlite"] as const)("%s retains completed skipped-root rows after a retirement failure, including explicit flush", async backend => {
    const hub = store(backend === "sqlite" ? "sqlite" : undefined);
    const now = Date.now();
    let clock = now;
    const a = remote("retained-completed-a", now), b = remote("retained-completed-b", now);
    a.host.incarnationToken = "A";
    b.host.incarnationToken = "B";
    const input = { hosts: [...a.presence.hosts, ...b.presence.hosts], participants: [a.participant, b.participant] };
    const side = new StoreBridgeSide(hub, "ryzen2", () => clock);
    const retirement = vi.spyOn(CommitOutbox.prototype, "retire").mockRejectedValueOnce(new Error("retire failed after install"));
    await expect(side.mirror(input)).rejects.toThrow("retire failed after install");
    retirement.mockRestore();
    const outbox = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    const aKey = outbox.rowKey(`lease:${a.host.id}`), bKey = outbox.rowKey(`lease:${b.host.id}`);
    const before = { row: hub.get(aKey, { fresh: true }), host: hub.get(hostKey(a.host.id), { fresh: true }),
      participant: hub.get(participantKey(a.host.id), { fresh: true }), lease: fs.readFileSync(hostLeasePath(hub.root, a.host.id)) };
    expect(before.row).toBeDefined();
    expect(hub.get(bKey, { fresh: true })).toBeDefined();
    const { incarnationToken: _token, ...tokenless } = a.host;
    clock += 1000;
    await side.mirror({ hosts: [{ record: tokenless, observedAt: clock, expiresAt: clock }, ...b.presence.hosts],
      participants: [a.participant, b.participant] }, clock);
    await side.flushLeaseEffects();
    expect(hub.get(aKey, { fresh: true })).toEqual(before.row);
    expect(hub.get(hostKey(a.host.id), { fresh: true })).toEqual(before.host);
    expect(hub.get(participantKey(a.host.id), { fresh: true })).toEqual(before.participant);
    expect(fs.readFileSync(hostLeasePath(hub.root, a.host.id))).toEqual(before.lease);
    expect(hub.get(bKey, { fresh: true })).toBeUndefined();
    await side.mirror(input, clock);
    await side.flushLeaseEffects();
    expect(hub.listAll(outbox.prefix, { fresh: true })).toEqual([]);
  });

  // SA-03: unlike SA-01's fresh pending rows, this row still has a prior done proof.
  it.each(["file", "sqlite"].flatMap(backend => [false, true].flatMap(deferred =>
    ["EACCES", "invalid"].map(fault => ({ backend, deferred, fault })))))(
    "SA-03 keeps a completed lease row after retirement failure then strict read failure ($backend, deferred=$deferred, $fault)", async mode => {
    const hub = store(mode.backend === "sqlite" ? "sqlite" : undefined);
    const now = Date.now(), peer = remote(`dual-fault-${mode.backend}-${mode.deferred}-${mode.fault}`, now);
    peer.host.incarnationToken = "same-incarnation";
    const side = new StoreBridgeSide(hub, "ryzen2", () => now, false, mode.deferred);
    const retire = CommitOutbox.prototype.retire;
    let failures = 0;
    const retirement = vi.spyOn(CommitOutbox.prototype, "retire").mockImplementation(async function (this: CommitOutbox, eligible) {
      if (this.retiring > 0) { failures += 1; throw new Error("retire failed after successful install"); }
      return retire.call(this, eligible);
    });
    if (mode.deferred) {
      await side.mirror(peer.presence);
      await expect(side.flushLeaseEffects()).rejects.toThrow("retire failed after successful install");
    } else await expect(side.mirror(peer.presence)).rejects.toThrow("retire failed after successful install");
    retirement.mockRestore();
    expect(failures).toBeGreaterThan(0);
    const outbox = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    const rowKey = outbox.rowKey(`lease:${peer.host.id}`);
    const before = hub.get(rowKey, { fresh: true });
    const target = hostLeasePath(hub.root, peer.host.id);
    const bytes = fs.readFileSync(target);
    expect(before).toBeDefined();
    expect(readHostLease(hub.root, peer.host.id)?.incarnationToken).toBe("same-incarnation");
    if (mode.fault === "invalid") fs.writeFileSync(target, "{invalid current lease bytes");
    const currentBytes = fs.readFileSync(target);
    const read = fs.readFileSync.bind(fs);
    const fault = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === target && mode.fault === "EACCES") {
        throw Object.assign(new Error("current file unreadable after successful install"), { code: "EACCES" });
      }
      return Reflect.apply(read, fs, [file, ...args]);
    }) as typeof fs.readFileSync);
    await side.mirror(peer.presence);
    await side.flushLeaseEffects();
    expect(fault.mock.calls.some(([file]) => String(file) === target)).toBe(true);
    fault.mockRestore();
    expect(fs.readFileSync(target)).toEqual(currentBytes);
    expect(hub.get(rowKey, { fresh: true })).toEqual(before);
    // Restoring the real file permits the same row/version to be replayed and retired.
    fs.writeFileSync(target, bytes);
    await side.mirror(peer.presence);
    await side.flushLeaseEffects();
    expect(hub.listAll(outbox.prefix, { fresh: true })).toEqual([]);
    expect(fs.readFileSync(target)).toEqual(bytes);
  });

  it.each([false, true].flatMap(deferred => ["lease", "replay", "unlease"].map(effect => ({ deferred, effect }))))(
    "SA-01 unreadable current native bytes never use the cached mirror ($effect, deferred=$deferred)", async ({ deferred, effect }) => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote(`read-error-${effect}-${deferred}`, now);
    peer.host.incarnationToken = "mirror-A";
    const side = new StoreBridgeSide(hub, "ryzen2", () => now, false, deferred);
    await side.mirror(peer.presence);
    await side.flushLeaseEffects();
    const cached = readHostLease(hub.root, peer.host.id)!;
    const outbox = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    if (effect !== "lease") {
      if (effect === "unlease") {
        await hub.delete({ key: hostKey(peer.host.id) });
        await hub.delete({ key: participantKey(peer.host.id) });
      }
      await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => outbox.plan().stage([], [{
        kind: effect === "unlease" ? "unlease" : "lease", key: `lease:${peer.host.id}`,
        payload: { ...cached, key: hostKey(peer.host.id) },
      }], { durable: true }) });
    }
    const target = hostLeasePath(hub.root, peer.host.id);
    const native = { ...cached, incarnationToken: "native-B", updatedAt: now + 1,
      writer: { pid: process.pid, host: "native", releaseSha: "test", lockProtocol: 1, stateBackend: "sqlite", startedAt: now } };
    const watch = fs.watch.bind(fs);
    vi.spyOn(fs, "watch").mockImplementation(((...args: unknown[]) => {
      const index = args.findIndex(arg => typeof arg === "function");
      if (index >= 0) args[index] = () => undefined;
      return Reflect.apply(watch, fs, args);
    }) as typeof fs.watch);
    fs.writeFileSync(`${target}.native`, JSON.stringify({ format: 1, ...native }));
    fs.renameSync(`${target}.native`, target);
    const bytes = fs.readFileSync(target);
    const read = fs.readFileSync.bind(fs);
    const fault = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === target) throw Object.assign(new Error("current native file unreadable"), { code: "EACCES" });
      return Reflect.apply(read, fs, [file, ...args]);
    }) as typeof fs.readFileSync);
    if (effect === "lease") await side.mirror({ hosts: [{ record: { ...peer.host,
      identity: { ...peer.identity, name: "changed" } }, expiresAt: now + 60000 }], participants: [peer.participant] });
    else await side.mirror(effect === "unlease" ? { hosts: [], participants: [] } : peer.presence);
    await side.flushLeaseEffects();
    expect(fault.mock.calls.some(([file]) => String(file) === target)).toBe(true);
    fault.mockRestore();
    expect(fs.readFileSync(target)).toEqual(bytes);
    expect(readHostLease(hub.root, peer.host.id)?.incarnationToken).toBe("native-B");
    expect(hub.get(outbox.rowKey(`lease:${peer.host.id}`), { fresh: true })).toBeDefined();
  });

  it.each([false, true])("SA-01 ENOENT still permits legitimate lease recreation (deferred=%s)", async deferred => {
    const hub = store("sqlite"), now = Date.now(), peer = remote(`missing-current-${deferred}`, now);
    const side = new StoreBridgeSide(hub, "ryzen2", () => now, false, deferred);
    await side.mirror(peer.presence);
    await side.flushLeaseEffects();
    const file = hostLeasePath(hub.root, peer.host.id);
    const prior = readHostLease(hub.root, peer.host.id);
    expect(prior).toBeDefined();
    fs.rmSync(file);
    await side.mirror(peer.presence);
    await side.flushLeaseEffects();
    expect(readHostLease(hub.root, peer.host.id)).toEqual(prior);
    expect(side.holds(peer.host.id, peer.identity)).toBe(true);
  });

  // SA-01: model another process's atomic writes while this process's watcher delivery is
  // deliberately held. Raw installs do not bump its module-local generation counters.
  it.each([false, true].flatMap(deferred => ["lease", "participant", "legacy-lease"].map(claim => ({ deferred, claim }))))(
    "keeps native authority before its SQL row ($claim, deferred=$deferred) with delayed watchers", async ({ deferred, claim }) => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote(`native-gap-${deferred}`, now);
    peer.host.incarnationToken = "old-mirror-token";
    const watch = fs.watch.bind(fs);
    vi.spyOn(fs, "watch").mockImplementation(((...args: unknown[]) => {
      const callback = args.findIndex(arg => typeof arg === "function");
      if (callback >= 0) args[callback] = () => undefined;
      return Reflect.apply(watch, fs, args);
    }) as typeof fs.watch);
      const target = { ...peer, host: { ...peer.host, id: `${peer.host.id}-${claim}` },
        identity: { ...peer.identity, id: `${peer.identity.id}-${claim}` } };
      target.host.identity = target.identity;
      target.host.rootId = target.identity.id;
      const participant = { ...peer.participant, id: target.identity.id, rootId: target.identity.id,
        ownerHostId: target.identity.id, ownerIdentityId: target.identity.id };
      const presence = { hosts: [{ record: target.host, expiresAt: now + 60_000, observedAt: now }], participants: [participant] };
      const nativeLease = { id: target.host.id, rootId: target.host.rootId, identityId: target.identity.id,
        startedAt: now + 1, incarnationToken: "native-successor-token", updatedAt: now + 1, expiresAt: now + 60_000,
        ...(claim === "lease" ? { writer: { pid: process.pid, host: "native", releaseSha: "test", lockProtocol: 1,
          stateBackend: "sqlite", startedAt: now + 1 } } : {}) };
      const install = (file: string, value: unknown): void => {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(`${file}.native-tmp`, JSON.stringify({ format: 1, ...(value as object) }));
        fs.renameSync(`${file}.native-tmp`, file);
      };
      let injected = false;
      const inject = (): void => {
        if (injected) return;
        injected = true;
        expect(hub.get(hostKey(target.host.id), { fresh: true })?.value).toMatchObject({ remoteHost: "ryzen2" });
        const generation = hostLeaseGeneration(hub.root, target.host.id);
        if (claim === "participant") install(path.join(hub.root, "participants", `${participantKey(target.host.id).split("/").at(-1)}.json`), {
          key: participantKey(target.host.id), version: 1, updatedAt: now + 1, updatedBy: target.identity, value: participant,
        });
        else install(hostLeasePath(hub.root, target.host.id), nativeLease);
        expect(hostLeaseGeneration(hub.root, target.host.id)).toBe(generation);
      };
      const custody = hub.leaseCustody.bind(hub);
      let acquisitions = 0;
      const custodySpy = vi.spyOn(hub, "leaseCustody").mockImplementation((file, operation, timeout, options) => {
        if (deferred && ++acquisitions === 2) inject();
        return custody(file, operation, timeout, options);
      });
      const writeBatch = hub.writeBatch.bind(hub);
      const batchSpy = vi.spyOn(hub, "writeBatch").mockImplementation(input => {
        if (deferred || !input.commitOutbox) return writeBatch(input);
        const commitOutbox = input.commitOutbox;
        return writeBatch({ ...input, commitOutbox: committed => { inject(); return commitOutbox(committed); } });
      });
      const side = new StoreBridgeSide(hub, "ryzen2", () => now, false, deferred);
      try {
        await side.mirror(presence);
        await side.flushLeaseEffects();
        expect(injected).toBe(true);
        if (claim === "participant") expect(readHostLease(hub.root, target.host.id)).toBeUndefined();
        else expect(readHostLease(hub.root, target.host.id)).toEqual(nativeLease);
        expect(side.holds(target.identity.id, target.identity)).toBe(false);
      } finally { custodySpy.mockRestore(); batchSpy.mockRestore(); }
  });

  it("merges current same-incarnation history and keeps its newer sample after deferred custody reacquisition", async () => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote("same-incarnation-gap", now);
    peer.host.incarnationToken = "same-owner-token";
    const presence = { hosts: [{ record: peer.host, expiresAt: now + 60_000, observedAt: now }], participants: [peer.participant] };
    await new StoreBridgeSide(hub, "ryzen2", () => now).mirror(presence);
    // History must be locally persisted authority, never a remote's claimed history.
    const held = hub.get(hostKey(peer.host.id), { fresh: true })!;
    await hub.put({ key: held.key, identity: held.updatedBy, value: { ...(held.value as object),
      bridgeSupersededTokens: ["planned-retired"], bridgeSupersededTokenSeen: { "planned-retired": 1 } } });
    const custody = hub.leaseCustody.bind(hub);
    let acquisitions = 0;
    vi.spyOn(hub, "leaseCustody").mockImplementation((file, operation, timeout, options) => {
      if (++acquisitions === 2) {
        const current = readHostLease(hub.root, peer.host.id)!;
        writeHostLease(hub.root, { ...current, updatedAt: now + 2_000, expiresAt: now + 17_000,
          mirror: { ...current.mirror!, observedAt: now + 2_000, sourceObservedAt: now + 2_000 },
          bridgeSupersededTokens: ["current-retired"], bridgeSupersededTokenSeen: { "current-retired": 1 } });
      }
      return custody(file, operation, timeout, options);
    });
    const side = new StoreBridgeSide(hub, "ryzen2", () => now + 1_000, false, true);
    await side.mirror({ ...presence, hosts: [{ ...presence.hosts[0]!, observedAt: now + 1_000 }] });
    await side.flushLeaseEffects();
    const current = readHostLease(hub.root, peer.host.id)!;
    expect(acquisitions).toBe(2);
    expect(current.incarnationToken).toBe("same-owner-token");
    expect(current.mirror?.observedAt).toBe(now + 2_000);
    expect(current.expiresAt).toBe(now + 17_000);
    expect(current.bridgeSupersededTokens).toEqual(expect.arrayContaining(["current-retired", "planned-retired"]));
  });

  it.each([false, true])("recovery keeps a native file-first owner with deferred installs=%s", async deferred => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote(`native-recovery-${deferred}`, now);
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    const lease = { id: peer.host.id, rootId: peer.host.rootId, identityId: peer.identity.id,
      startedAt: now, incarnationToken: "mirror-token", updatedAt: now, expiresAt: now + 30_000 };
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([{
      kind: "put", key: hostKey(peer.host.id), identity: peer.identity,
      value: { ...peer.host, incarnationToken: "mirror-token", remoteHost: "ryzen2" },
    }], [{ kind: "lease", key: `lease:${peer.host.id}`, payload: lease }]) });
    const nativeLease = { ...lease, incarnationToken: "native-token", startedAt: now + 1,
      writer: { pid: process.pid, host: "native", releaseSha: "test", lockProtocol: 1, stateBackend: "sqlite", startedAt: now + 1 } };
    writeHostLease(hub.root, nativeLease);
    const side = new StoreBridgeSide(hub, "ryzen2", () => now, false, deferred);
    await side.mirror({ hosts: [], participants: [] });
    await side.flushLeaseEffects();
    expect(readHostLease(hub.root, peer.host.id)).toEqual(nativeLease);
    expect(hub.listAll(dead.prefix, { fresh: true })).toEqual([]);
  });

  it("records lease rows in a changing commit, writes the lease after it and never uses exclusive()", async () => {
    const hub = store();
    const side = new StoreBridgeSide(hub, "ryzen2");
    const exclusive = vi.spyOn(hub, "exclusive");
    const peer = remote("alpha", Date.now());
    const batches: Array<{ keys: string[] }> = [];
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      const prepare = input.prepare;
      if (!prepare) return writeBatch(input);
      return writeBatch({ ...input, prepare: (view) => {
        const ops = prepare(view);
        batches.push({ keys: ops.map((op) => op.key) });
        return ops;
      } });
    });
    await side.mirror(peer.presence);
    expect(exclusive).not.toHaveBeenCalled();
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeDefined();
    expect(readHostLease(hub.root, peer.identity.id)?.identityId).toBe(peer.identity.id);
    const rows = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    expect(batches[0]!.keys).toContain(rows.rowKey(`lease:${peer.identity.id}`));
    // A lease-only renewal commits nothing and records nothing; it still renews the lease file.
    fs.rmSync(path.join(hub.root, "host-leases"), { recursive: true, force: true });
    await side.mirror(peer.presence);
    expect(batches[1]!.keys).toEqual([]);
    expect(readHostLease(hub.root, peer.identity.id)).toBeDefined();
  });

  it("crash after the mirror commit, before the lease write: the next pass writes it once and deletes the row", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("beta", now);
    // What a bridge that died between COMMIT and its effect leaves: the mirror and its row, no lease.
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    const lease = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, startedAt: peer.host.startedAt,
      updatedAt: now, expiresAt: now + 30_000 };
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([{
      kind: "put", key: hostKey(peer.identity.id), identity: peer.identity,
      value: { ...peer.host, updatedAt: now, expiresAt: now + 30_000, remoteHost: "ryzen2" },
    }], [{ kind: "lease", key: `lease:${peer.identity.id}`, payload: lease }]) });
    expect(readHostLease(hub.root, peer.identity.id)).toBeUndefined();
    expect(hub.listAll(dead.prefix, { fresh: true })).toHaveLength(1);
    const writes = vi.spyOn(fs, "renameSync");
    const side = new StoreBridgeSide(hub, "ryzen2");
    // An empty presence: only the recovered row can write the lease; the pass then withdraws the
    // mirror (the peer no longer reports it) and removes the lease again.
    await side.mirror({ hosts: [], participants: [] });
    const leaseWrites = writes.mock.calls.filter(([, to]) => leaseWrite(String(to)));
    expect(leaseWrites).toHaveLength(1);
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(hub.root, peer.identity.id)).toBeUndefined();
    // Replayed effects and this pass's completed removal are independently retired.
    expect(hub.listAll(dead.prefix, { fresh: true })).toEqual([]);
  });

  it("a replayed row never shortens a lease that was renewed since, and a replayed removal checks the commit", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("gamma", now);
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    const old = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, updatedAt: now, expiresAt: now + 10_000 };
    const other = remote("delta", now);
    await hub.put({ key: hostKey(other.identity.id), identity: other.identity, value: other.host });
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([{
      kind: "put", key: hostKey(peer.identity.id), identity: peer.identity,
      value: { ...peer.host, remoteHost: "ryzen2" },
    }], [
      { kind: "lease", key: `lease:${peer.identity.id}`, payload: old },
      // A removal whose host is back (a native here): the replay must keep its lease.
      { kind: "unlease", key: `lease:${other.identity.id}`, payload: { key: hostKey(other.identity.id), id: other.identity.id } },
    ]) });
    writeHostLease(hub.root, { ...old, updatedAt: now + 5_000, expiresAt: now + 40_000 });
    writeHostLease(hub.root, { id: other.identity.id, rootId: other.identity.id, identityId: other.identity.id, updatedAt: now, expiresAt: now + 60_000 });
    const writes = vi.spyOn(fs, "renameSync");
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    // Neither replay wrote a lease (the renewed one is newer); the pass then withdrew the mirror.
    expect(writes.mock.calls.filter(([, to]) => leaseWrite(String(to)))).toEqual([]);
    expect(readHostLease(hub.root, peer.identity.id)).toBeUndefined();
    expect(readHostLease(hub.root, other.identity.id)).toBeDefined();
    // Replayed effects and this pass's completed removal are independently retired.
    expect(hub.listAll(dead.prefix, { fresh: true })).toEqual([]);
  });

  it("reads participant files before the transaction and again only when their directory moved", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("epsilon", now);
    const side = new StoreBridgeSide(hub, "ryzen2");
    // A native file for the same id lands between the pre-read and the commit: the stamp moved,
    // so the transaction reads again and the mirror never covers the native.
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementationOnce(async (input) => {
      writeParticipantFile(hub.root, { key: participantKey(peer.identity.id), version: 1, updatedAt: now, updatedBy: peer.identity,
        value: { ...peer.participant } });
      return writeBatch(input);
    });
    await side.mirror(peer.presence);
    expect(hub.get(participantKey(peer.identity.id), { fresh: true })).toBeUndefined();
  });

  it("does not restart a mirror plan for unrelated native lease renewals during custody acquisition", async () => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote("heartbeat-target", now);
    const other = remote("unrelated-native", now);
    const custody = hub.leaseCustody.bind(hub);
    let acquisitions = 0;
    vi.spyOn(hub, "leaseCustody").mockImplementation((file, operation, timeout, options) => {
      // The real lease gate yields before it enters; a native on another gate can renew
      // during that wait. Bound the seeded regression instead of hanging the test worker.
      if (++acquisitions > 8) throw new Error("Unrelated heartbeats starved mirror planning");
      writeHostLease(hub.root, { id: other.host.id, rootId: other.host.rootId, identityId: other.identity.id,
        startedAt: now, updatedAt: now + acquisitions, expiresAt: now + 60_000 + acquisitions });
      return custody(file, operation, timeout, options);
    });
    await new StoreBridgeSide(hub, "ryzen2").mirror(peer.presence);
    expect(hub.get(hostKey(peer.host.id), { fresh: true })?.value).toMatchObject({ id: peer.host.id, remoteHost: "ryzen2" });
    expect(hub.get(participantKey(peer.host.id), { fresh: true })?.updatedBy).toEqual(peer.identity);
    expect(readHostLease(hub.root, peer.host.id)?.rootId).toBe(peer.host.rootId);
    expect(readHostLease(hub.root, other.host.id)?.updatedAt).toBeGreaterThan(now);
  });

  it("still replans when a newly observed lease aliases into the selected root", async () => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote("alias-target", now);
    const other = remote("alias-native", now);
    const custody = hub.leaseCustody.bind(hub);
    let inserted = false;
    vi.spyOn(hub, "leaseCustody").mockImplementation((file, operation, timeout, options) => {
      if (!inserted) {
        inserted = true;
        writeHostLease(hub.root, { id: other.host.id, rootId: other.host.rootId, identityId: peer.identity.id,
          startedAt: now, updatedAt: now, expiresAt: now + 60_000 });
      }
      return custody(file, operation, timeout, options);
    });
    await new StoreBridgeSide(hub, "ryzen2").mirror(peer.presence);
    expect(inserted).toBe(true);
    expect(hub.get(hostKey(peer.host.id), { fresh: true })).toBeUndefined();
    expect(hub.get(participantKey(peer.host.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(hub.root, peer.host.id)).toBeUndefined();
    expect(readHostLease(hub.root, other.host.id)?.identityId).toBe(peer.identity.id);
  });

  it("bounds genuinely stale mirror retries without scanning topology in the writer fence", async () => {
    const hub = store("sqlite");
    const peer = remote("contended-root", Date.now());
    let proofs = 0;
    vi.spyOn(hub, "stateUnchangedSince").mockImplementation(() => { proofs += 1; return false; });
    await expect(new StoreBridgeSide(hub, "ryzen2").mirror(peer.presence))
      .rejects.toMatchObject({ code: "FABRIC_MESH_SNAPSHOT_RETRIES_EXHAUSTED" });
    expect(proofs).toBe(3);
    expect(hub.get(hostKey(peer.host.id), { fresh: true })).toBeUndefined();
  });

  it("does not move a carried plan past an external native takeover after its batch commit", async () => {
    const hub = store("sqlite");
    const now = Date.now();
    const owners = Array.from({ length: 40 }, (_, index) => remote(`carry-${index}`, now));
    const target = owners[39]!;
    const writeBatch = hub.writeBatch.bind(hub);
    let injected = false;
    vi.spyOn(hub, "writeBatch").mockImplementation(async input => {
      if (input.lockClass !== "bridge" || !input.commitOutbox || injected) return writeBatch(input);
      const { commitOutbox, ...rest } = input;
      let effect: Parameters<typeof commitOutbox>[0] | undefined;
      const result = await writeBatch({ ...rest, commitOutbox: value => { effect = value; } });
      injected = true;
      // Another writer commits before the adapter resumes its outbox callback. A
      // later-head stamp must not certify the unconsumed roots of the original plan.
      await hub.put({ key: hostKey(target.host.id), identity: target.identity, value: target.host });
      commitOutbox({ ...effect!, stamp: hub.stateRevision() });
      return result;
    });
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: owners.flatMap(owner => owner.presence.hosts),
      participants: owners.flatMap(owner => owner.presence.participants) });
    expect(injected).toBe(true);
    expect(hub.get(hostKey(target.host.id), { fresh: true })?.value).toEqual(target.host);
    expect(hub.get(participantKey(target.host.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(hub.root, target.host.id)).toBeUndefined();
    expect(hub.get(hostKey(owners[0]!.host.id), { fresh: true })?.value).toMatchObject({ remoteHost: "ryzen2" });
  });

  it("defers an unproven notification baseline without filesystem I/O or topology scans inside SQLite", async () => {
    const hub = store("sqlite");
    const side = new StoreBridgeSide(hub, "ryzen2");
    const fence = hub.withStateWriteFence.bind(hub);
    const unchanged = hub.stateUnchangedSince.bind(hub);
    const stat = fs.statSync.bind(fs);
    const list = hub.listAll.bind(hub);
    let inside = false, fileOps = 0, scans = 0, proofs = 0;
    vi.spyOn(hub, "withStateWriteFence").mockImplementation(operation => fence(() => {
      inside = true;
      try { return operation(); } finally { inside = false; }
    }));
    vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
      if (inside) fileOps += 1;
      return stat(...args);
    }) as typeof fs.statSync);
    vi.spyOn(hub, "listAll").mockImplementation((prefix, options) => {
      if (inside && prefix?.startsWith("topology/")) scans += 1;
      return list(prefix, options);
    });
    vi.spyOn(hub, "stateUnchangedSince").mockImplementation((revision, prefixes) =>
      ++proofs <= 3 ? false : unchanged(revision, prefixes));
    const changes: string[] = [], failures: Error[] = [];
    const close = side.subscribeChanges(kind => changes.push(kind), error => failures.push(error));
    try {
      expect(proofs).toBe(3);
      expect(changes).toEqual([]);
      await vi.waitFor(() => expect(changes).toEqual(["presence"]));
      expect(failures).toEqual([]);
      expect(fileOps).toBe(0);
      expect(scans).toBe(0);
    } finally { close(); }
  });

  it("does not adopt a notification snapshot when a native file lands before its fence", async () => {
    const hub = store("sqlite");
    const peer = remote("notification-native", Date.now());
    const side = new StoreBridgeSide(hub, "ryzen2");
    const fence = hub.withStateWriteFence.bind(hub);
    let fences = 0;
    vi.spyOn(hub, "withStateWriteFence").mockImplementation(operation => {
      if (++fences === 1) writeParticipantFile(hub.root, { key: participantKey(peer.host.id), version: 1,
        updatedAt: peer.host.updatedAt, updatedBy: peer.identity, value: peer.participant });
      return fence(operation);
    });
    const changes: string[] = [], failures: Error[] = [];
    const close = side.subscribeChanges(kind => changes.push(kind), error => failures.push(error));
    try {
      // The synchronous writer has not delivered fs.watch yet: post-fence stamps must
      // reject its old snapshot. The later notification then sees the true baseline.
      expect(fences).toBeGreaterThanOrEqual(2);
      await vi.waitFor(() => expect(fences).toBeGreaterThanOrEqual(3));
      expect(changes).toEqual([]);
      expect(failures).toEqual([]);
    } finally { close(); }
  });

  it("skips unchanged lease installs and lets delivery proceed while changed installs are async", async () => {
    const hub = store("sqlite");
    let now = Date.now();
    const owners = Array.from({ length: 150 }, (_, index) => remote(`bulk-${index}`, now));
    const presence = {
      hosts: owners.flatMap(owner => owner.presence.hosts),
      participants: owners.flatMap(owner => owner.presence.participants),
    };
    const side = new StoreBridgeSide(hub, "hub", () => now, false, true);
    const originalRename = fs.promises.rename.bind(fs.promises);
    let block = true, blocked = 0;
    let releaseRename!: () => void;
    const release = new Promise<void>(resolve => { releaseRename = resolve; });
    const rename = vi.spyOn(fs.promises, "rename").mockImplementation(async (source, target) => {
      if (block && leaseWrite(String(target))) { blocked += 1; await release; }
      return originalRename(source, target);
    });
    try {
      await side.mirror(presence);
      await vi.waitFor(() => expect(blocked).toBeGreaterThan(0), { timeout: 10_000 });
      const delivery = await side.publish({ topic: "fleet.work.probe", kind: "ask",
        from: { id: "session:hub", name: "main", kind: "main" }, to: "session:destination",
        text: "delivery remains live", data: { bridge: { from: "hub", id: "async-install-probe", via: "hub" } } }, [], true);
      expect(delivery.sequence).toBeGreaterThan(0);
      expect(hub.read({ after: delivery.sequence - 1, limit: 1 })[0]?.text).toBe("delivery remains live");
      block = false;
      releaseRename();
      await side.flushLeaseEffects();
      expect(readHostLeases(hub.root).size).toBe(150);
      expect(rename.mock.calls.filter(([, target]) => leaseWrite(String(target)))).toHaveLength(150);

      rename.mockClear();
      await side.mirror(presence);
      await side.flushLeaseEffects();
      expect(rename.mock.calls.filter(([, target]) => leaseWrite(String(target)))).toHaveLength(0);

      now += 100;
      const changed = {
        hosts: presence.hosts.map(host => ({ ...host, record: { ...host.record, updatedAt: now, expiresAt: now + 60_000 }, expiresAt: now + 60_000 })),
        participants: presence.participants,
      };
      await side.mirror(changed);
      await side.flushLeaseEffects();
      expect(rename.mock.calls.filter(([, target]) => leaseWrite(String(target)))).toHaveLength(150);
    } finally {
      block = false;
      releaseRename();
      await side.flushLeaseEffects();
      rename.mockRestore();
    }
  }, 60_000);
});

describe("owner-matched unlease (smarty-dev#6477 L2b owner review, P2)", () => {
  // A removal whose effect did not run (a crash between COMMIT and the effect): its row stays.
  const removeWithoutEffect = async (hub: MeshStore, side: StoreBridgeSide) => {
    const writeBatch = hub.writeBatch.bind(hub);
    // Only the mirror's own batch (recovery of earlier rows runs as usual).
    const spy = vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      if (input.lockClass !== "bridge") return writeBatch(input);
      const { afterCommit: _dropped, commitOutbox: _droppedEffects, ...rest } = input;
      return writeBatch(rest);
    });
    await side.mirror({ hosts: [], participants: [] });
    spy.mockRestore();
  };

  it("records the removed mirror's lease identity and a delayed replay removes only that lease", async () => {
    const hub = store();
    const peer = remote("zeta", Date.now());
    await new StoreBridgeSide(hub, "ryzen2").mirror(peer.presence);
    // The mirror's lease carries the origin's incarnation.
    expect(readHostLease(hub.root, peer.identity.id)?.startedAt).toBe(peer.host.startedAt);
    await removeWithoutEffect(hub, new StoreBridgeSide(hub, "ryzen2"));
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeUndefined();
    const rows = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    expect(hub.get(rows.rowKey(`lease:${peer.identity.id}`), { fresh: true })?.value).toMatchObject({ kind: "unlease", payload: {
      key: hostKey(peer.identity.id), id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, startedAt: peer.host.startedAt,
    } });
    expect(readHostLease(hub.root, peer.identity.id)).toBeDefined();
    // The replay finds the mirror's own lease and removes it.
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    expect(readHostLease(hub.root, peer.identity.id)).toBeUndefined();
  });

  it("a delayed replay keeps the lease of a replacement owner that wrote its file lease first", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("eta", now);
    await new StoreBridgeSide(hub, "ryzen2").mirror(peer.presence);
    await removeWithoutEffect(hub, new StoreBridgeSide(hub, "ryzen2"));
    // A replacement owner of the same id (a new incarnation) writes its file lease before its
    // state record: the shared-state key is absent when the removal replays.
    const replacement = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id,
      startedAt: now + 5_000, updatedAt: now + 5_000, expiresAt: now + 60_000 };
    writeHostLease(hub.root, replacement);
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })).toBeUndefined();
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(replacement);
    // The replayed row is retired all the same.
    const rows = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    expect(hub.get(rows.rowKey(`lease:${peer.identity.id}`), { fresh: true })).toBeUndefined();
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(replacement);
  });
});

describe("stale lease effect after a replacement mirror (pi-fabric#640 review round 2)", () => {
  it("overlapping SQLite bridge writers: the stale afterCommit keeps the replacement's lease and liveness", async () => {
    const hub = store("sqlite");
    expect(hub.stateBackend).toBe("sqlite");
    // A second writer for the same peer, through another connection to the same database.
    const other = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: "sqlite" });
    opened.push(other);
    const now = Date.now();
    const peer = remote("lambda", now);
    // The same id, root and identity, restarted: a new incarnation (startedAt).
    const restarted = { ...peer.host, startedAt: now + 5_000, updatedAt: now + 5_000 } as FabricHostRecord;
    const stale = new StoreBridgeSide(hub, "ryzen2", Date.now, false, true);
    const replacing = new StoreBridgeSide(other, "ryzen2");
    const custody = hub.leaseCustody.bind(hub);
    let delayEffects = false, delayed = 0;
    let release!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(hub, "leaseCustody").mockImplementation(async (file, operation, timeout, options) => {
      if (delayEffects) { delayed += 1; await blocked; }
      return custody(file, operation, timeout, options);
    });
    await stale.mirror(peer.presence);
    delayEffects = true;
    let replacementLease: ReturnType<typeof readHostLease>;
    try {
      await vi.waitFor(() => expect(delayed).toBeGreaterThan(0));
      // The real async post-commit effect has not acquired its lease gate. A replacement
      // writer commits and installs the newer incarnation before the delayed effect resumes.
      await replacing.mirror({ hosts: [{ record: restarted, expiresAt: now + 60_000 }], participants: [peer.participant] });
      replacementLease = readHostLease(hub.root, peer.identity.id);
      expect(replacementLease?.startedAt).toBe(restarted.startedAt);
    } finally { delayEffects = false; release(); await stale.flushLeaseEffects(); }
    const entry = hub.get(hostKey(peer.identity.id), { fresh: true });
    expect(entry?.value).toMatchObject({ startedAt: restarted.startedAt, remoteHost: "ryzen2" });
    // The replacement's lease survives the stale effect...
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(replacementLease);
    // ...and liveness matches it: the live mirror never looks expired.
    const held = entry!.value as FabricHostRecord;
    const liveness = hostLiveness(readHostLeases(hub.root), held);
    expect(liveness.expiresAt).toBe(Math.max(held.expiresAt, replacementLease!.expiresAt));
    expect(liveness.expiresAt).toBeGreaterThan(Date.now());
    expect(readHostLeases(hub.root).get(peer.identity.id)?.startedAt).toBe(held.startedAt);
  });
});

describe("out-of-order live lease effects of one incarnation (smarty-dev#6939)", () => {
  it("concurrent SQLite mirror batches: the older effect, run last, never shortens the newer lease", async () => {
    const hub = store("sqlite");
    expect(hub.stateBackend).toBe("sqlite");
    const other = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: "sqlite" });
    opened.push(other);
    const now = Date.now();
    const peer = remote("xi", now);
    // Two writers for the same peer, host and incarnation, through two connections to one database.
    const older = new StoreBridgeSide(hub, "ryzen2", () => now, false, true);
    const newer = new StoreBridgeSide(other, "ryzen2", () => now);
    // The newer writer has nothing to recover: only live effects run below.
    await newer.mirror({ hosts: [], participants: [] });
    const custody = hub.leaseCustody.bind(hub);
    let delayEffects = false, delayed = 0;
    let release!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(hub, "leaseCustody").mockImplementation(async (file, operation, timeout, options) => {
      if (delayEffects) { delayed += 1; await blocked; }
      return custody(file, operation, timeout, options);
    });
    const writes = vi.spyOn(fs, "renameSync");
    await older.mirror({ hosts: [{ record: peer.host, expiresAt: now + 8_000 }], participants: [peer.participant] });
    delayEffects = true;
    let newerLease: ReturnType<typeof readHostLease>;
    let writesBeforeRelease = 0;
    try {
      await vi.waitFor(() => expect(delayed).toBeGreaterThan(0));
      await newer.mirror({ hosts: [{ record: peer.host, expiresAt: now + 14_000 }], participants: [peer.participant] });
      newerLease = readHostLease(hub.root, peer.identity.id);
      expect(newerLease).toMatchObject({ startedAt: peer.host.startedAt, expiresAt: now + 14_000 });
      // Recovery may first replay the recorded older row; only the resumed late effect
      // must not install anything over the newer lease.
      writesBeforeRelease = writes.mock.calls.filter(([, to]) => leaseWrite(String(to))).length;
    } finally { delayEffects = false; release(); await older.flushLeaseEffects(); }
    expect(hub.get(hostKey(peer.identity.id), { fresh: true })?.value).toMatchObject({ startedAt: peer.host.startedAt, remoteHost: "ryzen2" });
    // The newer lease and its expiry survive; the older effect wrote no lease file.
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(newerLease);
    expect(writes.mock.calls.filter(([, to]) => leaseWrite(String(to)))).toHaveLength(writesBeforeRelease);
  });

  it("a live effect still replaces a later-expiring lease of another incarnation", async () => {
    const hub = store("sqlite");
    const now = Date.now();
    const peer = remote("omicron", now);
    const restarted = { ...peer.host, startedAt: now + 1_000, updatedAt: now + 1_000 } as FabricHostRecord;
    // The previous incarnation's lease, expiring later than the restarted mirror's.
    writeHostLease(hub.root, { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id,
      startedAt: peer.host.startedAt, updatedAt: now, expiresAt: now + 60_000 });
    await new StoreBridgeSide(hub, "ryzen2", () => now + 1_000)
      .mirror({ hosts: [{ record: restarted, expiresAt: now + 9_000 }], participants: [peer.participant] });
    expect(readHostLease(hub.root, peer.identity.id)).toMatchObject({ startedAt: restarted.startedAt, expiresAt: now + 9_000 });
  });
});

describe("legacy outbox rows without an incarnation (pi-fabric#640 review round 3)", () => {
  it("a legacy lease row replayed after a restarted mirror writes nothing; a legacy unlease row removes nothing", async () => {
    const hub = store();
    const now = Date.now();
    const peer = remote("mu", now);
    const gone = remote("nu", now);
    // The same id, root and identity, restarted: the held mirror is the new incarnation.
    const restarted = { ...peer.host, startedAt: now + 5_000, updatedAt: now + 5_000, remoteHost: "ryzen2" };
    // Rows of an earlier release: no startedAt. The lease row expires later than the replacement's.
    const dead = new CommitOutbox(hub, "bridge/ryzen2", bridgeIdentity, {});
    await hub.writeBatch({ identity: bridgeIdentity, ops: [], prepare: () => dead.plan().stage([{
      kind: "put", key: hostKey(peer.identity.id), identity: peer.identity, value: restarted,
    }], [
      { kind: "lease", key: `lease:${peer.identity.id}`, payload: {
        id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id, updatedAt: now, expiresAt: now + 50_000 } },
      { kind: "unlease", key: `lease:${gone.identity.id}`, payload: {
        key: hostKey(gone.identity.id), id: gone.identity.id, rootId: gone.identity.id, identityId: gone.identity.id } },
    ]) });
    const replacement = { id: peer.identity.id, rootId: peer.identity.id, identityId: peer.identity.id,
      startedAt: restarted.startedAt, updatedAt: now + 5_000, expiresAt: now + 20_000 };
    writeHostLease(hub.root, replacement);
    // A lease file without an incarnation, whose state record is absent.
    const unstamped = { id: gone.identity.id, rootId: gone.identity.id, identityId: gone.identity.id, updatedAt: now, expiresAt: now + 60_000 };
    writeHostLease(hub.root, unstamped);
    // Only the replay runs effects: this pass's post-commit drain is dropped.
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementation(async (input) => {
      const { afterCommit: _dropped, commitOutbox: _droppedEffects, ...rest } = input;
      return writeBatch(rest);
    });
    const writes = vi.spyOn(fs, "renameSync");
    await new StoreBridgeSide(hub, "ryzen2").mirror({ hosts: [], participants: [] });
    expect(writes.mock.calls.filter(([, to]) => leaseWrite(String(to)))).toEqual([]);
    // The replacement's lease and expiry stay as they were; the unstamped lease is not removed.
    expect(readHostLease(hub.root, peer.identity.id)).toEqual(replacement);
    expect(readHostLease(hub.root, gone.identity.id)).toEqual(unstamped);
    // Both legacy rows are retired all the same.
    expect(hub.get(dead.rowKey(`lease:${gone.identity.id}`), { fresh: true })).toBeUndefined();
    expect(hub.listAll(dead.prefix, { fresh: true }).map((entry) => (entry.value as { payload: { startedAt?: number } }).payload))
      .toEqual([expect.objectContaining({ startedAt: restarted.startedAt })]);
  });
});

describe("R20 bridge write fence on SQLite (smarty-dev#6477 L2b owner review, P1)", () => {
  const event = (id: string, from: MeshIdentity): BridgePublish => ({
    topic: "fleet.work.task", kind: "ask", from, to: "session:hub-main", data: { bridge: { from: "ryzen2", id } },
  });
  const rawDatabase = async (root: string) => {
    const { DatabaseSync } = await import("node:sqlite");
    const db = new DatabaseSync(path.join(root, "state.db"));
    db.exec("PRAGMA busy_timeout = 0");
    return db;
  };

  it("holds the SQLite write lock from holds() through the append: a takeover commits after it, then refuses the stale owner", async () => {
    const hub = store("sqlite");
    expect(hub.stateBackend).toBe("sqlite");
    const peer = remote("theta", Date.now());
    const side = new StoreBridgeSide(hub, "ryzen2");
    await side.mirror(peer.presence);
    expect(side.holds(peer.identity.id)).toBe(true);
    // A native takeover of the same id, through another connection to the same database.
    const native = new MeshStore(hub.root, 64 * 1024, 100, { stateBackend: "sqlite" });
    opened.push(native);
    const raw = await rawDatabase(hub.root);
    const holds = side.holds.bind(side);
    let probe: string | undefined;
    let takeover: Promise<unknown> | undefined;
    let sequenceAtTakeover = -1;
    vi.spyOn(side, "holds").mockImplementation((id) => {
      const held = holds(id);
      if (takeover) return held;
      // Between holds() and the append: another writer cannot take the write lock...
      try { raw.exec("BEGIN IMMEDIATE"); raw.exec("ROLLBACK"); probe = "acquired"; }
      catch (error) { probe = (error as { errcode?: number }).errcode === undefined ? String(error) : `errcode:${(error as { errcode: number }).errcode & 0xff}`; }
      // ...and a takeover's commit, attempted now, runs only after the append.
      takeover = native.writeBatch({ identity: peer.identity, ops: [], prepare: () => {
        sequenceAtTakeover = native.latestSequence();
        return [{ kind: "put", key: hostKey(peer.identity.id), value: { ...peer.host, startedAt: peer.host.startedAt + 1 }, identity: peer.identity }];
      } });
      return held;
    });
    const published = await side.publish(event("e1", peer.identity), [peer.identity.id]);
    expect(probe).toBe("errcode:5"); // SQLITE_BUSY
    await takeover;
    expect(sequenceAtTakeover).toBeGreaterThanOrEqual(published.sequence);
    raw.close();
    // The takeover committed: the stale owner's next publish is refused, nothing is appended.
    vi.mocked(side.holds).mockRestore();
    expect(side.holds(peer.identity.id)).toBe(false);
    await expect(side.publish(event("e2", peer.identity), [peer.identity.id])).rejects.toBeInstanceOf(BridgeOwnershipError);
    expect(hub.latestSequence()).toBe(published.sequence);
    // The fence wrote nothing and is released: an ordinary write goes through.
    await hub.put({ key: "probe/after", value: 1, identity: bridgeIdentity });
  });

  it("a busy write lock refuses before the event stamp; the same event publishes after it is free", async () => {
    const hub = store("sqlite");
    const peer = remote("iota", Date.now());
    const side = new StoreBridgeSide(hub, "ryzen2");
    await side.mirror(peer.presence);
    const before = hub.latestSequence();
    const raw = await rawDatabase(hub.root);
    raw.exec("BEGIN IMMEDIATE");
    const checks = vi.spyOn(side, "holds");
    const retriedEvent = event("e3", peer.identity);
    await expect(side.publish(retriedEvent, [peer.identity.id])).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    // Refused before the ownership check and the append, never published unfenced or stamped.
    expect(checks).not.toHaveBeenCalled();
    expect(hub.latestSequence()).toBe(before);
    raw.exec("ROLLBACK");
    raw.close();
    const published = await side.publish(retriedEvent, [peer.identity.id]);
    expect(published.sequence).toBeGreaterThan(before);
    // The sender's id, with the identity the event carries (checked against its record, pi-fabric#876).
    expect(checks).toHaveBeenCalledWith(peer.identity.id, expect.objectContaining({ id: peer.identity.id, name: peer.identity.name, kind: peer.identity.kind }));
  });

  it("an event with no held ids takes no state fence", async () => {
    const hub = store("sqlite");
    const peer = remote("kappa", Date.now());
    const side = new StoreBridgeSide(hub, "ryzen2");
    const fence = vi.spyOn(hub, "withStateWriteFence");
    await side.publish(event("e4", peer.identity));
    expect(fence).not.toHaveBeenCalled();
    await side.mirror(peer.presence);
    await side.publish(event("e5", peer.identity), [peer.identity.id]);
    expect(fence).toHaveBeenCalledTimes(1);
  });
});

