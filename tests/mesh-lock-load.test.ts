import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { hostLeasePath, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { sweepParticipantLockLeftovers } from "../src/topology/participant-files.js";

const roots: string[] = [];
const identity: MeshIdentity = { id: "writer", name: "writer", kind: "main" };
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const key = (prefix: string, id: string): string => prefix + hash(id);
const sixHours = 6 * 60 * 60 * 1000;
const now = 1_000_000_000;
const old = now - sixHours - 1_000;
const setup = () => {
  vi.spyOn(Date, "now").mockReturnValue(now);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lock-load-"));
  roots.push(root);
  return { root, mesh: new MeshStore(root, 64 * 1024, 100, { lockTimeoutMs: 100, maxStateTombstones: 2 }) };
};
const seed = (root: string, values: Array<[string, unknown, number?]>) => {
  const entries: Record<string, MeshStateEntry> = {};
  values.forEach(([k, value, at = old], index) => { entries[k] = { key: k, value, version: index + 1, updatedAt: at, updatedBy: identity }; });
  fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: 1, entries }));
  return entries;
};
const participant = (id: string, hostId = id) => ({ format: 1, id, rootId: hostId, ownerHostId: hostId, ownerIdentityId: hostId });
const host = (id: string, expiresAt = old) => ({ format: 1, id, rootId: id, identity: { ...identity, id }, startedAt: old - 1_000, updatedAt: old, expiresAt });
const deliveryKey = (id: string) => `residency/deliveries/${hash("recipient").slice(0, 32)}/${id}`;
const delivery = (id: string, extra = {}) => ({ format: 1, id, rootId: "recipient", createdAt: old, from: { id: "agent:run", kind: "agent" }, message: "durable", ...extra });
const tick = (mesh: MeshStore) => mesh.put({ key: "probe/tick", value: 1, identity });
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("#4383 bounded expiry inside the existing mesh write", () => {
  it("drops only old settled envelopes; terminal result payloads and unknown custody survive", async () => {
    const { root, mesh } = setup();
    seed(root, [
      [deliveryKey("terminal"), delivery("terminal", { status: "completed" })],
      [deliveryKey("ack"), delivery("ack", { acknowledged: true })],
      [deliveryKey("ack-at"), delivery("ack-at", { acknowledgedAt: old })],
      [deliveryKey("pending"), delivery("pending")],
      [deliveryKey("result"), delivery("result", { data: { status: "completed", id: "agent:run", startedAt: old } })],
      [deliveryKey("running"), delivery("running", { status: "running" })],
      [deliveryKey("recent-write"), delivery("recent-write", { status: "completed" }), now],
      [deliveryKey("recent-ack"), delivery("recent-ack", { acknowledgedAt: now })],
      [deliveryKey("recent-complete"), delivery("recent-complete", { status: "failed", completedAt: now })],
      [deliveryKey("recent-terminal-ack"), delivery("recent-terminal-ack", { status: "completed", completedAt: old, acknowledgedAt: now })],
      [deliveryKey("invalid-time"), delivery("invalid-time", { status: "completed", completedAt: "uncertain" })],
      [deliveryKey("boundary"), delivery("boundary", { status: "completed", createdAt: now - sixHours })],
      [deliveryKey("malformed"), { ...delivery("malformed", { status: "completed" }), createdAt: "old" }],
      ["residency/deliveries/wrong-key", delivery("wrong", { status: "completed" })],
    ]);
    await tick(mesh);
    for (const id of ["terminal", "ack", "ack-at"]) expect(mesh.get(deliveryKey(id))).toBeUndefined();
    for (const id of ["pending", "result", "running", "recent-write", "recent-ack", "recent-complete", "recent-terminal-ack", "invalid-time", "boundary", "malformed"]) expect(mesh.get(deliveryKey(id)), id).toBeDefined();
    expect(mesh.get("residency/deliveries/wrong-key")).toBeDefined();
  });

  it("requires an old valid durable completion receipt, never just a terminal completion", async () => {
    const { root, mesh } = setup();
    const ids = ["consumed", "pending", "recent", "invalid", "wrong-id"];
    seed(root, ids.map(id => [deliveryKey(id), delivery(id, { from: { id, kind: "agent" }, agentCompletionId: id })]));
    const dir = path.join(root, "agent-completions", "receipts");
    fs.mkdirSync(dir, { recursive: true });
    for (const id of ids.filter(id => id !== "pending")) fs.writeFileSync(path.join(dir, `${hash(id)}.json`), JSON.stringify({
      id: id === "wrong-id" ? "other" : id, sessionId: id === "invalid" ? "" : "session", consumedAt: id === "recent" ? now : old,
    }));
    await tick(mesh);
    expect(mesh.get(deliveryKey("consumed"))).toBeUndefined();
    for (const id of ids.slice(1)) expect(mesh.get(deliveryKey(id)), id).toBeDefined();
  });

  it("retains live leases, recent records, orphans, mismatched and unreadable ownership", async () => {
    const { root, mesh } = setup();
    const ids = ["gone", "live-state", "live-file", "recent", "orphan", "mismatch", "corrupt", "unreadable", "boundary", "takeover"];
    seed(root, ids.flatMap(id => [
      [key("topology/participants/", id), participant(id), id === "recent" ? now : old] as [string, unknown, number],
      ...(id === "orphan" ? [] : [[key("topology/hosts/", id), host(id, id === "live-state" ? now + 15_000 : id === "boundary" ? now - sixHours : old)] as [string, unknown]]),
    ]));
    for (const id of ["live-file", "mismatch", "unreadable", "takeover"]) writeHostLease(root, {
      id, rootId: id, identityId: id === "mismatch" ? "different" : id, startedAt: id === "takeover" ? now : old - 1_000,
      updatedAt: id === "live-file" ? now : old, expiresAt: id === "live-file" ? now + 15_000 : old,
    });
    readHostLeases(root); // warm expired cache evidence must not authorize an unreadable file
    fs.writeFileSync(hostLeasePath(root, "corrupt"), "{");
    const read = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === hostLeasePath(root, "unreadable")) throw Object.assign(new Error("uncertain"), { code: "EIO" });
      return (read as (...args: unknown[]) => unknown)(file, ...args);
    }) as typeof fs.readFileSync);
    await tick(mesh);
    expect(mesh.get(key("topology/participants/", "gone"))).toBeUndefined();
    for (const id of ids.slice(1)) expect(mesh.get(key("topology/participants/", id)), id).toBeDefined();
  });

  it("rechecks host files inside the locked write after an expired cached selection", async () => {
    const { root, mesh } = setup();
    const id = "renewed", p = key("topology/participants/", id);
    seed(root, [[p, participant(id)], [key("topology/hosts/", id), host(id)]]);
    writeHostLease(root, { id, rootId: id, identityId: id, startedAt: old - 1_000, updatedAt: old, expiresAt: old });
    readHostLeases(root);
    expect(mesh.get(p)).toBeDefined(); // selection before the renewal
    writeHostLease(root, { id, rootId: id, identityId: id, startedAt: old - 1_000, updatedAt: now, expiresAt: now + 15_000 });
    await tick(mesh);
    expect(mesh.get(p)).toBeDefined();
  });

  it.each(["put", "delete", "batch"])("compacts under %s and preserves CAS allocation after tombstone eviction", async operation => {
    const { root, mesh } = setup();
    const original = seed(root, Array.from({ length: 3 }, (_, n) => [deliveryKey(String(n)), delivery(String(n), { status: "completed" })]));
    if (operation === "put") await tick(mesh);
    else if (operation === "delete") await mesh.delete({ key: deliveryKey("0") });
    else await mesh.writeBatch({ identity, ops: [{ kind: "put", key: "probe/tick", value: 1 }] });
    expect(mesh.listAll("residency/deliveries/")).toEqual([]);
    for (const entry of Object.values(original)) {
      await expect(mesh.put({ key: entry.key, value: "stale", identity, ifVersion: entry.version })).rejects.toThrow("compare-and-swap");
      const recreated = await mesh.put({ key: entry.key, value: "new", identity });
      expect(recreated.version).toBeGreaterThan(entry.version);
    }
  });

  it("bounds expiry to 500 records per commit and converges without a new daemon", async () => {
    const { root, mesh } = setup();
    seed(root, Array.from({ length: 510 }, (_, n) => [deliveryKey(String(n)), delivery(String(n), { acknowledged: true })]));
    await tick(mesh);
    expect(mesh.listAll("residency/deliveries/")).toHaveLength(10);
    await tick(mesh);
    expect(mesh.listAll("residency/deliveries/")).toEqual([]);
  });
});

describe("#4383 lock-free confirmation and accurate timeout ages", () => {
  it("takes no mesh lock for an empty/recent leftover sweep, retaining the fence for real cleanup", async () => {
    const { root, mesh } = setup();
    const exclusive = vi.spyOn(mesh, "exclusive");
    await sweepParticipantLockLeftovers(mesh, 60_000, now);
    const locks = path.join(root, "participants", ".locks");
    fs.mkdirSync(locks, { recursive: true });
    await sweepParticipantLockLeftovers(mesh, 60_000, now);
    const fresh = path.join(locks, "fresh.tmp");
    fs.mkdirSync(fresh);
    fs.utimesSync(fresh, now / 1000, now / 1000);
    await sweepParticipantLockLeftovers(mesh, 60_000, now);
    expect(exclusive).not.toHaveBeenCalled();
    const stale = path.join(locks, "stale.dead");
    fs.mkdirSync(stale);
    fs.utimesSync(stale, (now - 60_001) / 1000, (now - 60_001) / 1000);
    await sweepParticipantLockLeftovers(mesh, 60_000, now);
    expect(exclusive).toHaveBeenCalledOnce();
    expect(fs.existsSync(stale)).toBe(false);
    expect(fs.existsSync(fresh)).toBe(true);
  });

  it("does not touch a held lock and cleans its private probe without changing state", async () => {
    const { root, mesh } = setup();
    await tick(mesh);
    const state = fs.readFileSync(path.join(root, "state.json"), "utf8");
    const lock = path.join(root, ".lock");
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), `holder\n${process.pid}\n${now}\n`);
    const writes = vi.spyOn(fs, "mkdirSync");
    const confirmed = vi.fn();
    await mesh.confirmWritable(confirmed);
    expect(confirmed).toHaveBeenCalledWith(now);
    expect(writes.mock.calls.some(([file]) => String(file) === lock)).toBe(false);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toContain("holder");
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(state);
    expect(fs.readdirSync(root).some(name => name.startsWith(".writable."))).toBe(false);
  });

  it("does not confirm a failed atomic rename and removes all private probe files", async () => {
    const { root, mesh } = setup();
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to).includes(".writable.")) throw Object.assign(new Error("filesystem stalled"), { code: "EIO" });
      return rename(from, to);
    });
    const confirmed = vi.fn();
    await expect(mesh.confirmWritable(confirmed)).rejects.toThrow("filesystem stalled");
    expect(confirmed).not.toHaveBeenCalled();
    expect(fs.readdirSync(root)).toEqual([]);
  });

  it.each([1, 2] as const)("stamps protocol %s at successful acquisition, excluding this holder's prior wait", async lockProtocol => {
    const { root } = setup();
    vi.restoreAllMocks();
    vi.useFakeTimers({ now });
    const mesh = new MeshStore(root, 64 * 1024, 100, { lockProtocol, lockTimeoutMs: 2_000 });
    const lock = path.join(root, ".lock");
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), `previous\n${process.pid}\n${now - 60_000}\n`);
    const entered = vi.fn(() => {
      const owner = fs.readFileSync(path.join(lock, "owner"), "utf8").trim().split("\n");
      expect(Number(owner[2])).toBeGreaterThanOrEqual(now + 1_000);
      expect(Number(owner[2])).toBe(Date.now());
    });
    const result = mesh.exclusive(entered);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(entered).not.toHaveBeenCalled();
    fs.rmSync(lock, { recursive: true });
    await vi.advanceTimersByTimeAsync(250);
    await result;
    expect(entered).toHaveBeenCalledOnce();
  });

  it("reports the observed holder's hold age separately from this waiter's duration", async () => {
    const { root, mesh } = setup();
    vi.restoreAllMocks();
    vi.useFakeTimers({ now });
    const lock = path.join(root, ".lock");
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), `holder\n${process.pid}\n${now - 60_000}\n`);
    const result = mesh.exclusive(() => {}).catch(error => error);
    await vi.advanceTimersByTimeAsync(100);
    const error = await result;
    expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", waitedMs: 100 });
    expect(error.message).toContain("for 60 s; this waiter waited 100 ms");
  });
});
