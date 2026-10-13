import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { observeActorOwnership, ownershipPointReads } from "../src/topology/publication-generation.js";

// fv2-11107: an idle poll's ownership map must not rebuild the fleet directory for unrelated
// commits, while every relevant ownership input of the managed actors still invalidates it.
const roots: string[] = [];
const stores: MeshStore[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map((directory) => directory.close().catch(() => undefined)));
  for (const mesh of stores.splice(0)) mesh.closeState?.();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const digest = (id: string): string => createHash("sha256").update(id).digest("hex");
const SELF: MeshIdentity = { id: "session:self", name: "main", kind: "main" };
const OTHER: MeshIdentity = { id: "session:other", name: "main", kind: "main" };
const PEER: MeshIdentity = { id: "session:peer", name: "main", kind: "main" };

const setup = async (stateBackend: "file" | "sqlite", extra: Record<string, unknown> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "participant-ownership-stamp-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend }); stores.push(mesh);
  const writer = new MeshStore(meshRoot, 64 * 1024, 100, { stateBackend }); stores.push(writer);
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: "host:observer", rootId: "session:observer",
    identity: { id: "session:observer", name: "main", kind: "main" }, heartbeatMs: 60_000, leaseMs: 120_000, ...extra,
  });
  directories.push(directory);
  const putHost = (identity: MeshIdentity, patch: Record<string, unknown> = {}) => writer.put({
    key: "topology/hosts/" + digest(`host:${identity.id}`), identity,
    value: { format: 1, id: `host:${identity.id}`, rootId: identity.id, identity, startedAt: 1, updatedAt: Date.now(),
      expiresAt: Date.now() + 120_000, ...patch },
  });
  const putActor = (id: string, owner: MeshIdentity, patch: Record<string, unknown> = {}) => writer.put({
    key: "topology/participants/" + digest(id), identity: owner,
    value: { format: 1, id, kind: "actor", rootId: owner.id, ownerHostId: `host:${owner.id}`, ownerIdentityId: owner.id,
      name: id, status: "idle", runner: "pi", transport: "host", capabilities: [], startedAt: 1, updatedAt: Date.now(),
      controlProtocol: "v1", ...patch },
  });
  await putHost(SELF); await putHost(OTHER); await putHost(PEER);
  const managed = ["actor:a", "actor:b"];
  for (const id of managed) await putActor(id, SELF);
  // A wider fleet the poll never manages.
  for (let index = 0; index < 20; index++) await putActor(`actor:peer-${index}`, PEER);
  const list = vi.spyOn(directory, "list");
  const poll = () => directory.actorOwnership(`host:${SELF.id}`, { fresh: false, actorIds: managed });
  // Poll rebuilds: non-fresh list() calls. Arming adds one fresh list() read AFTER the observation.
  const rebuilds = () => list.mock.calls.filter(([options]) => options?.fresh !== true).length;
  const actorValue = (id: string, owner: MeshIdentity, updatedAt: number) => ({ format: 1, id, kind: "actor", rootId: owner.id,
    ownerHostId: `host:${owner.id}`, ownerIdentityId: owner.id, name: id, status: "idle", runner: "pi", transport: "host",
    capabilities: [], startedAt: 1, updatedAt, controlProtocol: "v1" });
  // A native participant FILE copy of the same key (smarty-dev#2004); list() keeps the later updatedAt.
  const fileEntry = (id: string, owner: MeshIdentity, updatedAt: number, version = 1) => ({
    key: "topology/participants/" + digest(id), version, updatedAt, updatedBy: owner, value: actorValue(id, owner, updatedAt) });
  const putFile = (id: string, owner: MeshIdentity, updatedAt: number, version = 1) =>
    writeParticipantFile(meshRoot, fileEntry(id, owner, updatedAt, version) as never);
  const freshOwner = (id: string) => directory.list({ scope: "project", fresh: true }).find((entry) => entry.id === id)?.ownerHostId;
  return { meshRoot, mesh, writer, directory, list, poll, putHost, putActor, managed, rebuilds, putFile, fileEntry, freshOwner };
};

describe.each(["file", "sqlite"] as const)("fv2-11107 %s poll ownership stamp gate", (backend) => {
  it("reuses the poll map across unrelated commits and peer heartbeats; never for fresh reads", async () => {
    const { list, poll, directory, writer, putActor, putHost, mesh } = await setup(backend);
    expect(mesh.stateRevision() !== undefined).toBe(backend === "sqlite");
    const first = poll();
    expect(first.get("actor:a")).toBe(true);
    // One poll list plus the fresh arming read taken after the observation.
    expect(list).toHaveBeenCalledTimes(2);
    expect(list.mock.calls[1]?.[0]?.fresh).toBe(true);
    // Unrelated commits: traffic keys, peer participant updates, peer host heartbeat.
    for (let index = 0; index < 5; index++) {
      await writer.put({ key: `traffic/${index}`, value: { index }, identity: PEER });
      await putActor(`actor:peer-${index}`, PEER, { status: "running", updatedAt: Date.now() + index });
      await putHost(PEER, { updatedAt: Date.now() + index });
      expect(poll()).toEqual(new Map([["actor:a", true], ["actor:b", true]]));
    }
    expect(list).toHaveBeenCalledTimes(2);
    // Idle maintenance with nothing changed reuses it too.
    for (let index = 0; index < 10; index++) expect(poll().get("actor:b")).toBe(true);
    expect(list).toHaveBeenCalledTimes(2);
    // Fresh authority never hits the reuse.
    directory.actorOwnership(`host:${SELF.id}`, { fresh: true, actorIds: ["actor:a"] });
    directory.actorOwnership(`host:${SELF.id}`, { fresh: true });
    expect(list).toHaveBeenCalledTimes(4);
    expect(list.mock.calls.slice(2).every(([options]) => options?.fresh === true)).toBe(true);
    expect(poll().get("actor:a")).toBe(true);
    expect(list).toHaveBeenCalledTimes(4);
  });

  it("invalidates on a takeover of a managed actor, and a rightful restoration is seen again", async () => {
    const { poll, putActor, rebuilds } = await setup(backend);
    expect(poll().get("actor:b")).toBe(true);
    await putActor("actor:b", OTHER);
    const moved = poll();
    expect(moved.get("actor:b")).toBe(false);
    expect(rebuilds()).toBe(2);
    // Reuse answers exactly the managed ids, with the armed values.
    expect(poll()).toEqual(new Map([["actor:a", moved.get("actor:a")], ["actor:b", false]]));
    expect(rebuilds()).toBe(2);
    await putActor("actor:b", SELF);
    expect(poll().get("actor:b")).toBe(true);
    expect(rebuilds()).toBe(3);
  });

  it("invalidates on owner host record, host lease, root closure, new managed ids and deadlines", async () => {
    const { poll, putHost, writer, meshRoot, directory, putActor, rebuilds } = await setup(backend);
    const expectCalls = (count: number) => expect(rebuilds()).toBe(count);
    let calls = 1;
    poll(); expectCalls(calls);
    // Owner host replaced by a new incarnation.
    await putHost(SELF, { incarnationToken: "incarnation-2" });
    poll(); expectCalls(++calls);
    poll(); expectCalls(calls);
    // Owner host file lease written (new lease facts).
    writeHostLease(meshRoot, { id: `host:${SELF.id}`, rootId: SELF.id, identityId: SELF.id, startedAt: 1,
      updatedAt: Date.now(), expiresAt: Date.now() + 120_000 });
    poll(); expectCalls(++calls);
    // Root lineage closure.
    await writer.put({ key: "topology/lineage-closures/" + digest(SELF.id), value: { closed: true }, identity: SELF });
    poll(); expectCalls(++calls);
    poll(); expectCalls(calls);
    // A newly managed id was never observed.
    await putActor("actor:c", SELF);
    directory.actorOwnership(`host:${SELF.id}`, { fresh: false, actorIds: ["actor:a", "actor:b", "actor:c"] });
    expectCalls(++calls);
    // A subset of the armed ids reuses, but answers only the validated ids: no unscoped entry
    // of the old map can resurrect an actor whose ownership moved.
    const subset = directory.actorOwnership(`host:${SELF.id}`, { fresh: false, actorIds: ["actor:a"] });
    expectCalls(calls);
    expect([...subset.keys()]).toEqual(["actor:a"]);
    // A reloading record's deadline: reaching it invalidates without any write.
    await putActor("actor:a", SELF, { status: "reloading", reloadUntil: Date.now() + 150 });
    poll(); expectCalls(++calls);
    poll(); expectCalls(calls);
    await new Promise((resolve) => setTimeout(resolve, 250));
    expect(poll().get("actor:a")).toBeUndefined();
    expectCalls(++calls);
  });

  it("never arms reuse for an unknown (absent) managed actor or a mirrored record", async () => {
    const { list, directory, putActor } = await setup(backend);
    const host = `host:${SELF.id}`;
    directory.actorOwnership(host, { fresh: false, actorIds: ["actor:a", "actor:missing"] });
    directory.actorOwnership(host, { fresh: false, actorIds: ["actor:a", "actor:missing"] });
    expect(list).toHaveBeenCalledTimes(2);
    await putActor("actor:mirror", SELF, { remoteHost: "forge" });
    directory.actorOwnership(host, { fresh: false, actorIds: ["actor:mirror"] });
    directory.actorOwnership(host, { fresh: false, actorIds: ["actor:mirror"] });
    expect(list).toHaveBeenCalledTimes(4);
    // Without ids (legacy hook callers) it is exactly list().
    directory.actorOwnership(host, { fresh: false });
    directory.actorOwnership(host, { fresh: false });
    expect(list).toHaveBeenCalledTimes(6);
  });

  // Hardening counterexamples: fields outside the selected save facts that change the listing.
  it("invalidates when a managed record becomes malformed in a field outside the ownership facts", async () => {
    const { poll, putActor, rebuilds } = await setup(backend);
    expect(poll().get("actor:b")).toBe(true);
    // Same owner/root/token, but capabilities no longer parse: list() drops the record.
    await putActor("actor:b", SELF, { capabilities: ["not-a-capability"] });
    const after = poll();
    expect(rebuilds()).toBe(2);
    expect(after.has("actor:b")).toBe(false);
    // A legitimate renewal of the other actor (updatedAt/status) does not invalidate.
    await putActor("actor:b", SELF);
    expect(poll().get("actor:b")).toBe(true);
    const armed = rebuilds();
    await putActor("actor:a", SELF, { status: "running", updatedAt: Date.now() + 5, turns: 3 });
    expect(poll().get("actor:a")).toBe(true);
    expect(rebuilds()).toBe(armed);
  });

  it("invalidates when the owner host record becomes invalid schema outside the selected host facts", async () => {
    const { poll, putHost, rebuilds } = await setup(backend);
    expect(poll().get("actor:a")).toBe(true);
    // identity.name is not a selected host fact, but hostFromEntry rejects the record.
    await putHost(SELF, { identity: { id: SELF.id, kind: "main" } });
    const after = poll();
    expect(rebuilds()).toBe(2);
    expect(after.get("actor:a")).not.toBe(true);
  });

  it("invalidates on a shortened or removed owner lease; a legitimate renewal stays reusable", async () => {
    const { poll, putHost, rebuilds } = await setup(backend);
    expect(poll().get("actor:a")).toBe(true);
    // Legitimate renewal: a later expiry.
    await putHost(SELF, { updatedAt: Date.now() + 1, expiresAt: Date.now() + 240_000 });
    expect(poll().get("actor:a")).toBe(true);
    expect(rebuilds()).toBe(1);
    // Shortening: the observed deadline no longer bounds the decision.
    await putHost(SELF, { expiresAt: Date.now() + 60_000 });
    poll();
    expect(rebuilds()).toBe(2);
    // Expired (removed lease): relists and the actor is no longer owned live.
    await putHost(SELF, { expiresAt: Date.now() - 1 });
    expect(poll().get("actor:a")).toBeUndefined();
    expect(rebuilds()).toBe(3);
  });

  it("invalidates on a shortened owner file lease", async () => {
    const { poll, meshRoot, rebuilds } = await setup(backend);
    const lease = { id: `host:${SELF.id}`, rootId: SELF.id, identityId: SELF.id, startedAt: 1, updatedAt: Date.now(),
      incarnationToken: "lease-1" };
    writeHostLease(meshRoot, { ...lease, expiresAt: Date.now() + 600_000 });
    expect(poll().get("actor:a")).toBe(true);
    const armed = rebuilds();
    writeHostLease(meshRoot, { ...lease, updatedAt: Date.now() + 1, expiresAt: Date.now() + 900_000 });
    poll();
    expect(rebuilds()).toBe(armed);
    writeHostLease(meshRoot, { ...lease, expiresAt: Date.now() + 300_000 });
    poll();
    expect(rebuilds()).toBe(armed + 1);
  });

  it("never arms from a cached list older than the observation (reported stale-cache sequence)", async () => {
    const { directory, writer, poll, putFile, fileEntry, list, freshOwner, meshRoot } = await setup(backend, { listReadCacheMs: 60_000 });
    // actor:b lives only in its participant FILE (no shared copy, so no arbitration is involved).
    await writer.delete({ key: "topology/participants/" + digest("actor:b") });
    putFile("actor:b", SELF, Date.now());
    const owner = (listed: ReadonlyArray<{ id: string; ownerHostId?: string }>) => listed.find((entry) => entry.id === "actor:b")?.ownerHostId;
    // Warm the non-fresh list cache while actor:b is ours.
    expect(owner(directory.list({ scope: "project" }))).toBe(`host:${SELF.id}`);
    // Another PROCESS replaces the file (in-process writeParticipantFile would rescan our cache).
    const target = path.join(meshRoot, "participants", digest("actor:b") + ".json");
    execFileSync(process.execPath, ["-e", "const fs=require('node:fs');fs.writeFileSync(process.argv[1]+'.new',process.argv[2]);fs.renameSync(process.argv[1]+'.new',process.argv[1]);",
      target, JSON.stringify({ format: 1, ...fileEntry("actor:b", OTHER, Date.now() + 1, 2) })]);
    // No list in between: one list() can itself rescan the directory (a shorter-TTL inner read),
    // so the poll's own non-fresh list must be the first one after the replacement.
    list.mockClear();
    const first = poll();
    // The fixture really is stale: the poll's own non-fresh list answered the OLD owner (proved
    // from the spy, not assumed), while its observation already saw the takeover.
    const nonFresh = list.mock.calls.flatMap(([options], index) => options?.fresh === true ? [] : [list.mock.results[index]!.value]);
    expect(nonFresh.length).toBe(1);
    expect(owner(nonFresh[0])).toBe(`host:${SELF.id}`);
    // Either that poll did not arm (its exactness re-parse already disagreed with the old list) and
    // answers exactly what list() answered, the pre-existing non-fresh TTL semantics; or it armed,
    // and then only from the fresh read after its observation. Never an armed old view.
    if (list.mock.calls.length === 1) expect(first.get("actor:b")).toBe(true);
    else expect(first.get("actor:b")).toBe(false);
    // The next poll arms only from the fresh read after its observation: the takeover, then reused.
    expect(poll().get("actor:b")).toBe(false);
    const armed = list.mock.calls.length;
    expect(poll().get("actor:b")).toBe(false);
    expect(poll().get("actor:b")).toBe(false);
    expect(list.mock.calls.length).toBe(armed);
    expect(freshOwner("actor:b")).toBe(`host:${OTHER.id}`);
  });

  // Arbitration counterexample: file and shared copies of one managed key, both valid and unmirrored,
  // name different owners. list() keeps the later updatedAt, which the ownership facts ignore.
  it("never reuses while file and shared copies disagree: a losing copy's renewal flips the winner both ways", async () => {
    const { poll, putActor, putFile, rebuilds, freshOwner, mesh, directory } = await setup(backend);
    const self = `host:${SELF.id}`;
    const step = (expected: boolean) => {
      expect(freshOwner("actor:b") === self).toBe(expected);
      const before = rebuilds();
      expect(poll().get("actor:b")).toBe(expected);
      expect(poll().get("actor:b")).toBe(expected);
      // Never armed: every poll lists again.
      expect(rebuilds()).toBe(before + 2);
    };
    // Direction 1 (OTHER -> SELF): shared OTHER wins over an older SELF file.
    await putActor("actor:b", OTHER);
    putFile("actor:b", SELF, Date.now() - 60_000);
    step(false);
    // The registry-save observation (non-strict) does not see the flip: proof the facts are unchanged.
    const plain = observeActorOwnership(mesh, ["actor:b"], ownershipPointReads(mesh));
    // The LOSING file copy renews only updatedAt/version, same owner facts: it now wins.
    putFile("actor:b", SELF, Date.now() + 60_000, 2);
    expect(plain.unchanged()).toBe(true);
    step(true);
    // Direction 2 (SELF -> OTHER): shared SELF wins over an older OTHER file.
    await putActor("actor:b", SELF);
    putFile("actor:b", OTHER, Date.now() - 60_000, 3);
    step(true);
    putFile("actor:b", OTHER, Date.now() + 40, 4);
    step(false);
    // Self-restoration: SELF's ordinary shared renewal outruns the file again.
    await new Promise((resolve) => setTimeout(resolve, 80));
    await putActor("actor:b", SELF);
    step(true);
    // Fresh admission is exactly list({ fresh: true }).
    expect(directory.actorOwnership(self, { fresh: true, actorIds: ["actor:b"] }).get("actor:b")).toBe(true);
  });

  it("agreeing file and shared copies stay reusable across normal renewals and unrelated peer writes", async () => {
    const { poll, putActor, putHost, putFile, writer, rebuilds } = await setup(backend);
    putFile("actor:b", SELF, Date.now() - 1_000);
    expect(poll().get("actor:b")).toBe(true);
    const armed = rebuilds();
    // Normal renewals of either copy (arbitration facts only) and unrelated peer traffic.
    putFile("actor:b", SELF, Date.now() + 60_000, 2);
    expect(poll().get("actor:b")).toBe(true);
    await putActor("actor:b", SELF, { status: "running" });
    await writer.put({ key: "traffic/x", value: 1, identity: PEER });
    await putActor("actor:peer-1", PEER, { status: "running" });
    await putHost(PEER);
    expect(poll().get("actor:b")).toBe(true);
    expect(rebuilds()).toBe(armed);
    // A takeover through the winning file copy still invalidates.
    putFile("actor:b", OTHER, Date.now() + 120_000, 3);
    expect(poll().get("actor:b")).toBe(false);
    expect(rebuilds()).toBe(armed + 1);
  });
});
