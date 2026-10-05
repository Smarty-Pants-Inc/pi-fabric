import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { hostLeasePath, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { publishTopologyFixture } from "./helpers/mesh-topology-fixture.js";
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
  values.forEach(([k, value, at = old], index) => {
    const record = value as { identity?: MeshIdentity; ownerIdentityId?: string };
    const writer = record.identity ?? (record.ownerIdentityId ? { ...identity, id: record.ownerIdentityId } : identity);
    entries[k] = { key: k, value, version: index + 1, updatedAt: at, updatedBy: writer };
  });
  fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: 1, entries }));
  return entries;
};
const topologyFixtures = async (root: string, ids: string[]) => {
  const fixtures = new Map<string, Awaited<ReturnType<typeof publishTopologyFixture>>>();
  for (const id of ids) {
    vi.mocked(Date.now).mockReturnValue(old - 1_000);
    const fixture = await publishTopologyFixture(path.join(root, "fixtures", id), id);
    fixtures.set(id, fixture);
  }
  vi.mocked(Date.now).mockReturnValue(now);
  return fixtures;
};
const settledDelivery = (root: string, id: string) => {
  const dir = path.join(root, "agent-completions", "receipts");
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${hash(id)}.json`), JSON.stringify({ id, sessionId: "session", consumedAt: old }));
  return delivery(id, { from: { id, kind: "agent" }, agentCompletionId: id });
};
const deliveryKey = (id: string) => `residency/deliveries/${hash("recipient").slice(0, 32)}/${id}`;
const delivery = (id: string, extra = {}) => ({ format: 1, id, rootId: "recipient", createdAt: old, from: { id: "agent:run", kind: "agent" }, message: "durable", ...extra });
const tick = (mesh: MeshStore) => mesh.put({ key: "probe/tick", value: 1, identity });
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("#4383 participant expiry and background receipt compaction", () => {
  it.each([true, false])("expiry advances the generation and preserves reader tokens (journal=%s)", async writeReadJournal => {
    const { root } = setup();
    const mesh = new MeshStore(root, 64 * 1024, 100, { writeReadJournal });
    const id = "expired-reader-peer", p = key("topology/participants/", id);
    const fixture = (await topologyFixtures(root, [id])).get(id)!;
    seed(root, [[p, fixture.participant.value], [key("topology/hosts/", id), fixture.host.value]]);
    vi.mocked(Date.now).mockReturnValue(old);
    await tick(mesh); // Establish a canonical generation while the records are still recent.
    const reader = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 5000 });
    const token = reader.stateToken({ fresh: true });
    const file = path.join(root, "state.json");
    const generation = JSON.parse(fs.readFileSync(file, "utf8")).readGeneration;
    expect(reader.get(p, { snapshot: token })).toBeDefined();
    vi.mocked(Date.now).mockReturnValue(now);
    await mesh.put({ key: "probe/tick", value: 2, identity });
    const canonical = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(canonical.readGeneration).not.toBe(generation);
    const reads = vi.spyOn(fs, "readFileSync");
    const next = reader.stateToken({ fresh: true });
    expect(next).toEqual(canonical);
    expect(reader.get(p, { fresh: true })).toBeUndefined();
    expect(reader.get(p, { snapshot: token })).toBeDefined();
    expect(new MeshStore(root, 64 * 1024, 100).stateToken({ fresh: true })).toBe(next);
    if (writeReadJournal) expect(reads.mock.calls.filter(([target, encoding]) =>
      String(target) === file && encoding === "utf8")).toHaveLength(0);
  });

  it.each([true, false])("receipt compaction advances the generation and preserves reader tokens (journal=%s)", async writeReadJournal => {
    const { root } = setup();
    const mesh = new MeshStore(root, 64 * 1024, 100, { writeReadJournal });
    const k = deliveryKey("generation-receipt");
    seed(root, [[k, settledDelivery(root, "generation-receipt")]]);
    await tick(mesh); // Foreground writes never confirm/remove receipts.
    const reader = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 5000 });
    const token = reader.stateToken({ fresh: true });
    const file = path.join(root, "state.json");
    const generation = JSON.parse(fs.readFileSync(file, "utf8")).readGeneration;
    expect(await mesh.compactReceipts()).toBe(1);
    const canonical = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(canonical.readGeneration).not.toBe(generation);
    const reads = vi.spyOn(fs, "readFileSync");
    const next = reader.stateToken({ fresh: true });
    expect(next).toEqual(canonical);
    expect(reader.get(k, { fresh: true })).toBeUndefined();
    expect(reader.get(k, { snapshot: token })).toBeDefined();
    expect(new MeshStore(root, 64 * 1024, 100).stateToken({ fresh: true })).toBe(next);
    if (writeReadJournal) expect(reads.mock.calls.filter(([target, encoding]) =>
      String(target) === file && encoding === "utf8")).toHaveLength(0);
  });
  it("retains status flags without a durable receipt, terminal results and unknown custody", async () => {
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
    await mesh.compactReceipts();
    for (const id of ["terminal", "ack", "ack-at"]) expect(mesh.get(deliveryKey(id))).toBeDefined();
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
    expect(mesh.get(deliveryKey("consumed"))).toBeDefined(); // foreground write never confirms receipts
    await mesh.compactReceipts();
    expect(mesh.get(deliveryKey("consumed"))).toBeUndefined();
    for (const id of ids.slice(1)) expect(mesh.get(deliveryKey(id)), id).toBeDefined();
  });

  it("retains live leases, recent records, orphans, mismatched and unreadable ownership", async () => {
    const { root, mesh } = setup();
    const ids = ["gone", "live-state", "live-file", "recent", "orphan", "mismatch", "corrupt", "unreadable", "boundary", "takeover"];
    const fixtures = await topologyFixtures(root, ids);
    seed(root, ids.flatMap(id => [
      [key("topology/participants/", id), fixtures.get(id)!.participant.value, id === "recent" ? now : old] as [string, unknown, number],
      ...(id === "orphan" ? [] : [[key("topology/hosts/", id), { ...fixtures.get(id)!.host.value as object, expiresAt: id === "live-state" ? now + 15_000 : id === "boundary" ? now - sixHours : old }] as [string, unknown]]),
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
    const fixture = (await topologyFixtures(root, [id])).get(id)!;
    seed(root, [[p, fixture.participant.value], [key("topology/hosts/", id), fixture.host.value]]);
    writeHostLease(root, { id, rootId: id, identityId: id, startedAt: old - 1_000, updatedAt: old, expiresAt: old });
    readHostLeases(root);
    expect(mesh.get(p)).toBeDefined(); // selection before the renewal
    writeHostLease(root, { id, rootId: id, identityId: id, startedAt: old - 1_000, updatedAt: now, expiresAt: now + 15_000 });
    await tick(mesh);
    expect(mesh.get(p)).toBeDefined();
  });

  it.each(["put", "delete", "batch"])("compacts after %s and preserves CAS allocation after tombstone eviction", async operation => {
    const { root, mesh } = setup();
    const original = seed(root, Array.from({ length: 3 }, (_, n) => [deliveryKey(String(n)), settledDelivery(root, String(n))]));
    if (operation === "put") await tick(mesh);
    else if (operation === "delete") await mesh.delete({ key: deliveryKey("0") });
    else await mesh.writeBatch({ identity, ops: [{ kind: "put", key: "probe/tick", value: 1 }] });
    for (let pass = 0; pass < 6 && mesh.listAll("residency/deliveries/").length > 0; pass++) await mesh.compactReceipts();
    expect(mesh.listAll("residency/deliveries/")).toEqual([]);
    for (const entry of Object.values(original)) {
      await expect(mesh.put({ key: entry.key, value: "stale", identity, ifVersion: entry.version })).rejects.toThrow("compare-and-swap");
      const recreated = await mesh.put({ key: entry.key, value: "new", identity });
      expect(recreated.version).toBeGreaterThan(entry.version);
    }
  });

  it("bounds successful AND failed receipt attempts per pass and carries the backlog forward", async () => {
    const { root, mesh } = setup();
    const open = fs.promises.open.bind(fs.promises);
    const receiptOpens: string[] = [];
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      if (String(args[0]).endsWith(".json")) receiptOpens.push(String(args[0]));
      return open(...args);
    });
    seed(root, Array.from({ length: 40 }, (_, n) => {
      const id = String(n).padStart(2, "0");
      return [deliveryKey(id), n < 20 ? delivery(id, { from: { id, kind: "agent" }, agentCompletionId: id }) : settledDelivery(root, id)];
    }));
    await tick(mesh);
    expect(receiptOpens).toHaveLength(0);
    await mesh.compactReceipts();
    expect(receiptOpens.length).toBeGreaterThan(0);
    expect(receiptOpens.length).toBeLessThanOrEqual(16); // failed opens consume the budget too
    for (let pass = 0; pass < 40 && mesh.listAll("residency/deliveries/").length > 20; pass++) {
      const before = receiptOpens.length;
      await mesh.compactReceipts();
      expect(receiptOpens.length - before).toBeLessThanOrEqual(16);
    }
    expect(mesh.listAll("residency/deliveries/")).toHaveLength(20);
  });
});

describe("Astra R1 durable receipt and canonical ownership", () => {
  it.each(["put", "delete", "batch"])("retains a visible old receipt with failed file/namespace barriers during unrelated %s", async operation => {
    const { root, mesh } = setup();
    const id = "visible-failed-rename";
    seed(root, [[deliveryKey(id), settledDelivery(root, id)], ...Array.from({ length: 4 }, (_, n) => [`unrelated/deletable-${n}`, 1] as [string, unknown])]);
    const receipt = path.join(root, "agent-completions", "receipts", `${hash(id)}.json`);
    const original = fs.readFileSync(receipt, "utf8");
    const open = fs.promises.open.bind(fs.promises);
    let failure: "file" | "namespace" | undefined = "file";
    const receiptInode = fs.statSync(receipt).ino;
    let confirmedFile = 0, confirmedDirectory = 0;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      const sync = handle.sync.bind(handle);
      handle.sync = async () => {
        const stat = await handle.stat();
        if ((failure === "file" && stat.ino === receiptInode) ||
          (failure === "namespace" && stat.isDirectory())) throw new Error("receipt durability barrier failed");
        if (stat.ino === receiptInode) confirmedFile++;
        if (stat.isDirectory()) confirmedDirectory++;
        await sync();
      };
      return handle;
    });
    let deletion = 0;
    const commit = async () => {
      if (operation === "put") await tick(mesh);
      else if (operation === "delete") await mesh.delete({ key: `unrelated/deletable-${deletion++}` });
      else await mesh.writeBatch({ identity, ops: [{ kind: "put", key: "probe/tick", value: 1 }] });
      await mesh.compactReceipts();
    };
    for (const barrier of (process.platform === "win32" ? ["file", "file"] : ["file", "namespace", "file"]) as Array<"file" | "namespace">) {
      failure = barrier;
      await commit();
      expect(mesh.get(deliveryKey(id)), barrier).toBeDefined();
      expect(fs.readFileSync(receipt, "utf8")).toBe(original);
    }
    failure = undefined;
    await commit();
    expect(mesh.get(deliveryKey(id))).toBeUndefined();
    expect(confirmedFile).toBeGreaterThan(0);
    if (process.platform !== "win32") expect(confirmedDirectory).toBeGreaterThan(0);
  });

  it("retains wrong-writer and malformed host/participant incarnations as unknown ownership", async () => {
    const { root, mesh } = setup();
    const ids = ["valid", "wrong-writer", "missing-start", "missing-name", "wrong-kind", "malformed-participant"];
    const fixtures = await topologyFixtures(root, ids);
    const entries = seed(root, ids.flatMap(id => {
      const fixture = fixtures.get(id)!;
      return [[fixture.host.key, fixture.host.value], [fixture.participant.key, fixture.participant.value]] as Array<[string, unknown]>;
    }));
    entries[fixtures.get("wrong-writer")!.host.key]!.updatedBy = identity;
    delete (entries[fixtures.get("missing-start")!.host.key]!.value as Record<string, unknown>).startedAt;
    delete ((entries[fixtures.get("missing-name")!.host.key]!.value as { identity: Record<string, unknown> }).identity).name;
    (entries[fixtures.get("wrong-kind")!.host.key]!.value as { identity: Record<string, unknown> }).identity.kind = "unknown";
    delete (entries[fixtures.get("malformed-participant")!.participant.key]!.value as Record<string, unknown>).runner;
    fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: 1, entries }));
    await tick(mesh);
    expect(mesh.get(fixtures.get("valid")!.participant.key)).toBeUndefined();
    for (const id of ids.slice(1)) expect(mesh.get(fixtures.get(id)!.participant.key), id).toBeDefined();
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
