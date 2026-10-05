import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { deadHostRecords, reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { RootInbox } from "../src/topology/root-inbox.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const HOUR = 60 * 60 * 1000;
const writer: MeshIdentity = { id: "session:writer", name: "main", kind: "main", sessionId: "writer" };
const store = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-host-reaper-"));
  roots.push(root);
  return new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
};
const hostKey = (id: string) => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
const host = (mesh: MeshStore, id: string, expiresAt: number) =>
  mesh.put({ key: hostKey(id), value: { format: 1, id, expiresAt }, identity: writer });
const participant = (mesh: MeshStore, id: string, ownerHostId: string) =>
  mesh.put({ key: `topology/participants/${id}`, value: { format: 1, id, ownerHostId }, identity: writer });
const keys = (records: Array<{ entry: { key: string } }>) => records.map((record) => record.entry.key).sort();

const participantKey = (id: string) => "topology/participants/" + createHash("sha256").update(id).digest("hex");
const inboxKey = (id: string) => "topology/inbox/" + createHash("sha256").update(id).digest("hex").slice(0, 32);

describe("stale directory bookkeeping", () => {
  // F1 (#410): a cursor can be unchanged for days while its owner is still addressable.
  // Losing that checkpoint makes #load start at latestSequence and skip seconds-old work.
  const recoveryFixture = async () => {
    const mesh = store();
    const now = Date.now();
    const recipient: MeshIdentity = { id: "session:legacy", name: "main", kind: "main", sessionId: "legacy" };
    const clock = vi.spyOn(Date, "now").mockReturnValue(now - 25 * HOUR);
    await mesh.put({ key: inboxKey(recipient.id), value: { after: mesh.latestSequence() }, identity: recipient });
    clock.mockReturnValue(now);
    const work = await mesh.publish({ topic: "fleet.work.pi-fabric.410", to: recipient.id, kind: "ack", from: writer,
      text: "recent undelivered work", data: { key: "recovery-410" } });
    const session = (status = "idle") => mesh.put({ key: "sessions/legacy", identity: recipient,
      value: { id: recipient.id, sessionId: "legacy", cwd: "/legacy", startedAt: now - 26 * HOUR, status } });
    const hostRecord = (expiresAt: number) => mesh.put({ key: hostKey("legacy-host"), identity: recipient,
      value: { format: 1, id: "legacy-host", rootId: recipient.id, identity: recipient,
        startedAt: now - 26 * HOUR, updatedAt: now, expiresAt } });
    const lease = () => writeHostLease(mesh.root, { id: "legacy-host", rootId: recipient.id,
      identityId: recipient.id, updatedAt: now, expiresAt: now + 15_000 });
    const recover = async () => {
      // A different store and inbox force a reload of the persisted checkpoint, not memory.
      const reloaded = new MeshStore(mesh.root, 64 * 1024, 100);
      const inbox = new RootInbox(reloaded, recipient, () => [recipient.id], { now: () => now, steerGraceMs: 0 });
      expect((await inbox.next({ holdsBatch: () => false, holdsSteer: () => false })).events).toEqual([work]);
    };
    return { mesh, now, recipient, clock, session, hostRecord, lease, recover };
  };

  it.each(["idle", "running"])("keeps a session-only live %s root's old checkpoint and recovers fresh work", async (status) => {
    const { mesh, now, recipient, session, recover } = await recoveryFixture();
    await session(status);
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: "own", rootId: writer.id, identity: writer });
    directories.push(directory);
    expect(directory.list({ fresh: true }).find((entry) => entry.id === recipient.id)?.stale).toBe(false);
    expect(mesh.get(participantKey(recipient.id))).toBeUndefined();
    const removed = await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now });
    await recover();
    expect(removed).toBe(0);
  });

  it.each(["state", "file", "file-only"])("keeps a root's old checkpoint under a live %s host lease without a participant", async (kind) => {
    const { mesh, now, hostRecord, lease, recover } = await recoveryFixture();
    if (kind !== "file-only") await hostRecord(kind === "state" ? now + 15_000 : now - 7 * HOUR);
    if (kind !== "state") lease();
    const removed = await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now });
    await recover();
    expect(removed).toBe(0);
  });

  it.each(["session-renewed", "session-appeared", "host-renewed", "file-renewed"])(
    "rechecks %s liveness under the commit lock and recovers fresh work after reload", async (kind) => {
      const { mesh, now, clock, session, hostRecord, lease, recover } = await recoveryFixture();
      if (kind === "session-renewed") {
        clock.mockReturnValue(now - 16_000);
        await session();
        clock.mockReturnValue(now);
      }
      if (kind === "host-renewed" || kind === "file-renewed") await hostRecord(now - 7 * HOUR);
      const original = mesh.writeBatch.bind(mesh);
      const batches = vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async (input) => {
        // Selection is complete. A different writer renews before the batch acquires its lock,
        // without touching the cursor or adding any participant record/file.
        if (kind.startsWith("session-")) await session();
        if (kind === "host-renewed") await hostRecord(now + 15_000);
        if (kind === "file-renewed") lease();
        return original(input);
      });
      const removed = await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now });
      expect(batches).toHaveBeenCalledOnce();
      await recover();
      expect(removed).toBe(0);
    },
  );

  it("keeps the exact legacy lease boundary and recovers fresh work", async () => {
    const { mesh, now, clock, session, recover } = await recoveryFixture();
    clock.mockReturnValue(now - 15_000);
    await session();
    clock.mockReturnValue(now);
    const removed = await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now });
    await recover();
    expect(removed).toBe(0);
  });

  it.each(["expired", "terminal", "wrong-key", "wrong-writer", "malformed-writer"])(
    "does not let a %s session protect an orphan cursor", async (kind) => {
      const { mesh, now, recipient, clock, session } = await recoveryFixture();
      if (kind === "expired") clock.mockReturnValue(now - 15_001);
      const entry = await session(kind === "terminal" ? "completed" : "idle");
      clock.mockReturnValue(now);
      if (["wrong-key", "wrong-writer", "malformed-writer"].includes(kind)) {
        const statePath = path.join(mesh.root, "state.json");
        const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
        if (kind === "wrong-key") state.entries[entry.key].value.sessionId = "other";
        else state.entries[entry.key].updatedBy = kind === "wrong-writer" ? writer : null;
        // Publish a replacement, not an equal-length in-place edit whose UUID
        // and filesystem timestamps can still identify the cached valid writer.
        writeJsonAtomic(statePath, state);
      }
      expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(1);
      expect(mesh.get(inboxKey(recipient.id), { fresh: true })).toBeUndefined();
      expect(mesh.get(entry.key, { fresh: true })).toBeDefined(); // fresh terminal/nonterminal record is not swept
    },
  );
  it("rejects a wrong writer after an atomic replacement even when all state timestamps repeat", async () => {
    const { mesh, now, recipient, session } = await recoveryFixture();
    const entry = await session();
    const statePath = path.join(mesh.root, "state.json");
    const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
    const before = fs.statSync(statePath);
    const precise = fs.statSync(statePath, { bigint: true });
    const stat = fs.statSync;
    vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof stat>) => {
      const current = Reflect.apply(stat, fs, args);
      if (current && String(args[0]) === statePath) {
        const frozen = typeof current.ino === "bigint" ? precise : before;
        for (const field of ["mtime", "ctime", "birthtime", "mtimeMs", "ctimeMs", "birthtimeMs", "mtimeNs", "ctimeNs", "birthtimeNs"]) {
          if (field in frozen) Reflect.set(current, field, Reflect.get(frozen, field));
        }
      }
      return current;
    }) as never);
    state.entries[entry.key].updatedBy = writer;
    // Keep the old UUID and equal payload length too: the replacement's real
    // file identity, not a clock advance or generated commit marker, must win.
    writeJsonAtomic(statePath, state);
    expect(fs.statSync(statePath).size).toBe(before.size);
    expect(fs.statSync(statePath).ino).not.toBe(before.ino);
    expect(mesh.get(entry.key, { fresh: true })?.updatedBy).toEqual(writer);
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(1);
    expect(mesh.get(inboxKey(recipient.id), { fresh: true })).toBeUndefined();
    expect(mesh.get(entry.key, { fresh: true })).toBeDefined();
  });

  it("prunes old orphan cursors and terminal sessions together with dead hosts in one commit", async () => {
    const mesh = store();
    const now = Date.now();
    const inbox = (id: string) => mesh.put({ key: inboxKey(id), value: { after: 42 }, identity: { ...writer, id } });
    await inbox("session:gone");
    await inbox("session:present");
    await inbox("session:file");
    await inbox("session:own");
    await mesh.put({ key: participantKey("session:present"), value: {}, identity: writer }); // raw presence fails closed
    writeParticipantFile(mesh.root, { key: participantKey("session:file"), value: {}, version: 1, updatedAt: now, updatedBy: writer });
    for (const status of ["completed", "failed", "stopped", "timed_out", "idle", "running", "stopping", "unknown"]) {
      await mesh.put({ key: `sessions/${status}`, value: { status }, identity: writer });
    }
    await host(mesh, "dead", now - 7 * HOUR);
    // Deterministic age, with a fresh cursor/session written inside the retention window.
    const clock = vi.spyOn(Date, "now").mockReturnValue(now + 25 * HOUR);
    await inbox("session:recent");
    await mesh.put({ key: "sessions/recent", value: { status: "completed" }, identity: writer });
    const writes = vi.spyOn(fs, "renameSync");
    const batches = vi.spyOn(mesh, "writeBatch");
    expect(await reapDeadHostRecords(mesh, { ...writer, id: "session:own" }, { ownHostId: "own", now: now + 25 * HOUR })).toBe(6);
    expect(batches).toHaveBeenCalledOnce();
    expect(writes.mock.calls.filter(([, target]) => target === path.join(mesh.root, "state.json"))).toHaveLength(1);
    expect(mesh.get(inboxKey("session:gone"))).toBeUndefined();
    for (const id of ["present", "file", "own", "recent"]) expect(mesh.get(inboxKey(`session:${id}`))).toBeDefined();
    for (const status of ["completed", "failed", "stopped", "timed_out"]) expect(mesh.get(`sessions/${status}`)).toBeUndefined();
    for (const status of ["idle", "running", "stopping", "unknown", "recent"]) expect(mesh.get(`sessions/${status}`)).toBeDefined();
    // Tombstones survive cleanup, fencing any stale writer at the removed entry's version.
    await expect(mesh.put({ key: inboxKey("session:gone"), value: { after: 0 }, identity: writer, ifVersion: 1 })).rejects.toThrow(/compare-and-swap/);
    batches.mockClear();
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now: now + 25 * HOUR })).toBe(1); // formerly protected own cursor
    batches.mockClear();
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now: now + 25 * HOUR })).toBe(0);
    expect(batches).not.toHaveBeenCalled();
    clock.mockRestore();
  });

  it("keeps cursors when a participant returns and sessions/cursors rewritten before commit", async () => {
    const mesh = store();
    const now = Date.now();
    for (const id of ["back", "file-back", "rewritten"]) {
      await mesh.put({ key: inboxKey(id), value: { after: 1 }, identity: { ...writer, id } });
    }
    await mesh.put({ key: "sessions/back", value: { status: "completed" }, identity: writer });
    const original = mesh.writeBatch.bind(mesh);
    const batches = vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async (input) => {
      await mesh.put({ key: participantKey("back"), value: {}, identity: writer });
      writeParticipantFile(mesh.root, { key: participantKey("file-back"), value: {}, version: 1, updatedAt: now, updatedBy: writer });
      await mesh.put({ key: inboxKey("rewritten"), value: { after: 2 }, identity: { ...writer, id: "rewritten" } });
      await mesh.put({ key: "sessions/back", value: { status: "running" }, identity: writer });
      return original(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now: now + 25 * HOUR })).toBe(0);
    expect(batches).toHaveBeenCalledOnce();
    for (const id of ["back", "file-back", "rewritten"]) expect(mesh.get(inboxKey(id))).toBeDefined();
    expect(mesh.get("sessions/back")?.value).toEqual({ status: "running" });
  });

  it("does not treat malformed cursor metadata as proof of staleness or abort the pass", async () => {
    const mesh = store();
    const now = Date.now();
    const entry = await mesh.put({ key: inboxKey("bad-id"), value: { after: 0 }, identity: { ...writer, id: "bad-id" } });
    const statePath = path.join(mesh.root, "state.json");
    const state = JSON.parse(fs.readFileSync(statePath, "utf8"));
    state.entries[entry.key] = { ...entry, updatedBy: null, updatedAt: now - 25 * HOUR };
    const key = inboxKey("bad-age");
    state.entries[key] = { ...entry, key, updatedBy: { ...writer, id: "bad-age" }, updatedAt: null };
    state.entries["sessions/bad-age"] = { ...entry, key: "sessions/bad-age", value: { status: "completed" }, updatedAt: null };
    fs.writeFileSync(statePath, JSON.stringify(state));
    const batches = vi.spyOn(mesh, "writeBatch");
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(0);
    expect(batches).not.toHaveBeenCalled();
    expect(mesh.get(entry.key, { fresh: true })).toBeDefined();
    expect(mesh.get(key, { fresh: true })).toBeDefined();
    expect(mesh.get("sessions/bad-age", { fresh: true })).toBeDefined();
  });

  it("keeps malformed cursor attribution and the exact 24-hour boundary", async () => {
    const mesh = store();
    const now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now);
    await mesh.put({ key: inboxKey("other"), value: { after: 0 }, identity: writer });
    await mesh.put({ key: inboxKey("boundary"), value: { after: 0 }, identity: { ...writer, id: "boundary" } });
    await mesh.put({ key: "sessions/boundary", value: { status: "completed" }, identity: writer });
    const batches = vi.spyOn(mesh, "writeBatch");
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now: now + 24 * HOUR })).toBe(0);
    expect(batches).not.toHaveBeenCalled();
  });
});

// smarty-dev#367: a host removes its own records only on a clean shutdown.
describe("records of dead hosts", () => {
  it("selects hosts expired longer than the window, their participants, and orphans past the window", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "dead", now - 7 * HOUR);
    await participant(mesh, "dead-root", "dead");
    await participant(mesh, "dead-agent", "dead");
    await host(mesh, "recent", now - HOUR);                       // expired, but within the window
    await participant(mesh, "recent-root", "recent");
    await host(mesh, "live", now + 10_000);
    await participant(mesh, "live-root", "live");
    await host(mesh, "own", now - 7 * HOUR);                      // the caller itself: never
    await participant(mesh, "own-root", "own");
    await participant(mesh, "orphan", "vanished");                // no host record, written just now
    expect(keys(deadHostRecords(mesh, { ownHostId: "own", now })))
      .toEqual([hostKey("dead"), "topology/participants/dead-agent", "topology/participants/dead-root"].sort());
    // Past the window, the orphan without a host record goes too.
    expect(keys(deadHostRecords(mesh, { ownHostId: "own", now: now + 7 * HOUR })))
      .toContain("topology/participants/orphan");
  });

  // smarty-dev#816: hosts renew a file lease outside the shared state, and under the fleet
  // owner's policy renew their shared record only every few minutes.
  it("keeps a host whose file lease is fresh, and removes a dead host's file lease with it", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "filed", now - 7 * HOUR);                    // shared lease old ...
    await participant(mesh, "filed-root", "filed");
    writeHostLease(mesh.root, { id: "filed", rootId: "filed", identityId: "filed", updatedAt: now, expiresAt: now + 10_000 });
    await host(mesh, "dead", now - 7 * HOUR);
    writeHostLease(mesh.root, { id: "dead", rootId: "dead", identityId: "dead", updatedAt: now - 8 * HOUR, expiresAt: now - 7 * HOUR });
    expect(keys(deadHostRecords(mesh, { ownHostId: "own", now }))).toEqual([hostKey("dead")]);   // ... file lease fresh
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(1);
    expect(mesh.get(hostKey("filed"))).toBeDefined();
    expect([...readHostLeases(mesh.root).keys()]).toEqual(["filed"]);
  });

  it("deletes in one batch fenced to the versions it saw, and writes nothing when none are dead", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "dead", now - 7 * HOUR);
    await participant(mesh, "dead-root", "dead");
    const original = mesh.writeBatch.bind(mesh);
    const batch = vi.spyOn(mesh, "writeBatch");
    batch.mockImplementationOnce(async (input) => {
      await participant(mesh, "dead-root", "dead");              // rewritten meanwhile: kept
      return original(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(1);
    expect(mesh.get(hostKey("dead"))).toBeUndefined();
    expect(mesh.get("topology/participants/dead-root")).toBeDefined();
    batch.mockClear();
    await mesh.delete({ key: "topology/participants/dead-root" });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(0);
    expect(batch).not.toHaveBeenCalled();
  });

  // review/astra on #63: the two scans are separate reads, and per-record version fences did not
  // protect the host's liveness, so a host renewing in between lost its participants.
  it("keeps a host and its participants when the host renews between the host and participant scans", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "back", now - 7 * HOUR);
    await participant(mesh, "back-root", "back");
    // The host's heartbeat (a live lease and its participant) lands after the host scan and before
    // the participant scan, so the participant is read at its new version.
    const listAll = mesh.listAll.bind(mesh);
    let heartbeat: Promise<unknown> | undefined;
    vi.spyOn(mesh, "listAll").mockImplementation((prefix, options) => {
      if (prefix === "topology/participants/" && !heartbeat) {
        heartbeat = mesh.writeBatch({ identity: writer, ops: [
          { kind: "put", key: hostKey("back"), value: { format: 1, id: "back", expiresAt: now + 60_000 } },
          { kind: "put", key: "topology/participants/back-root", value: { format: 1, id: "back-root", ownerHostId: "back" } },
        ] });
      }
      return listAll(prefix, options);
    });
    const writeBatch = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementation(async (input) => {
      if (input.ops.every((op) => op.kind === "delete")) await heartbeat;
      return writeBatch(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(0);
    expect(mesh.get(hostKey("back"))).toBeDefined();
    expect(mesh.get("topology/participants/back-root")).toBeDefined();
  });

  it("keeps a host's participants when only its lease renews before the delete commits", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "back", now - 7 * HOUR);
    await participant(mesh, "back-root", "back");                // unchanged: not rewritten by the renewal
    const original = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async (input) => {
      await host(mesh, "back", now + 60_000);                    // a lease-only heartbeat
      return original(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now })).toBe(0);
    expect(mesh.get(hostKey("back"))).toBeDefined();
    expect(mesh.get("topology/participants/back-root")).toBeDefined();
  });

  it("keeps an orphan participant whose host appears before the delete commits", async () => {
    const mesh = store();
    const now = Date.now();
    await participant(mesh, "orphan", "returning");
    const original = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async (input) => {
      await host(mesh, "returning", now + 7 * HOUR + 60_000);    // the host comes back with a live lease
      return original(input);
    });
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "own", now: now + 7 * HOUR })).toBe(0);
    expect(mesh.get("topology/participants/orphan")).toBeDefined();
  });

  it("is swept by a directory after its heartbeat, at most once per sweep interval, never its own records", async () => {
    const mesh = store();
    const now = Date.now();
    await host(mesh, "dead", now - 7 * HOUR);
    await participant(mesh, "dead-root", "dead");
    const identity: MeshIdentity = { id: "session:live", name: "main", kind: "main", sessionId: "live" };
    const make = (reapDeadHosts: false | { sweepMs: number }) => {
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 300, reapDeadHosts,
      });
      directory.registerSource(() => [directory.root({
        id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
        updatedAt: 1, pendingMessages: false, local: true,
      })]);
      directories.push(directory);
      return directory;
    };
    const off = make(false);
    await off.start();
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(mesh.get(hostKey("dead"))).toBeDefined();         // disabled: no sweep
    await off.close();
    directories.length = 0;
    const batches = vi.spyOn(mesh, "writeBatch");
    const on = make({ sweepMs: 600 });
    await on.start();
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(mesh.get(hostKey("dead"))).toBeDefined();         // the first sweep waits an interval too
    await vi.waitFor(() => expect(mesh.get(hostKey("dead"))).toBeUndefined(), { timeout: 3_000, interval: 20 });
    expect(mesh.get("topology/participants/dead-root")).toBeUndefined();
    expect(on.list({ scope: "project" }).map((entry) => entry.id)).toEqual([identity.id]);   // its own records stay
    const sweeps = () => batches.mock.calls.filter(([input]) => input.ops.every((op) => op.kind === "delete")).length;
    expect(sweeps()).toBe(1);
    await new Promise((resolve) => setTimeout(resolve, 700));
    expect(sweeps()).toBe(1);                                      // nothing dead since: no more writes
  });
});
