import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BridgeOwnershipError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MESH_ARCHIVE_CONFIG, MeshArchive } from "../src/mesh/archive.js";

const roots: string[] = [];
const closes: Array<() => Promise<unknown>> = [];
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-events-batch-"));
  roots.push(root);
  return root;
};
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closes.splice(0)) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:batch", name: "main", kind: "main" };
const event = (id: string): BridgePublish => ({ topic: "fleet.work.batch", kind: "ask", from: identity, to: "session:local-0000",
  text: id, data: { bridge: { from: "ignored-untrusted-name", id } } });
const store = () => new MeshStore(scratch(), 65536, 500);
const lockCount = (root: string) => {
  const mkdir = vi.spyOn(fs, "mkdirSync");
  return () => mkdir.mock.calls.filter(([file]) => String(file) === path.join(root, ".lock")).length;
};
const addRoot = async (mesh: MeshStore, id: string) => {
  const now = Date.now();
  const from: MeshIdentity = { id, name: "main", kind: "main" };
  const hash = createHash("sha256").update(id).digest("hex");
  await mesh.put({ key: `topology/hosts/${hash}`, identity: from,
    value: { format: 1, id, rootId: id, identity: from, startedAt: now, updatedAt: now, expiresAt: now + 15000 } });
  await mesh.put({ key: `topology/participants/${hash}`, identity: from,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: id, label: id,
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return from;
};
const setup = async (wire: boolean) => {
  const hub = store(), far = store();
  const localId = await addRoot(hub, "session:local-0000"), remoteId = await addRoot(far, "session:remote-0000");
  const local = new StoreBridgeSide(hub, "forge"), agent = new StoreBridgeSide(far, "dev1");
  let remote: StoreBridgeSide | RemoteBridgeSide = agent;
  if (wire) {
    const input = new PassThrough(), output = new PassThrough();
    const serving = serveBridgeAgent(agent, input, output);
    const rpc = new RemoteBridgeSide(output, input);
    closes.push(async () => { rpc.close(); input.end(); await serving; });
    await rpc.hello();
    remote = rpc;
  }
  const cursorPath = path.join(scratch(), "cursor.json");
  const options = { localName: "dev1", remoteName: "forge", local, remote, cursorPath, presenceMs: 60000 };
  const bridge = new MeshBridge(options);
  await bridge.start();
  return { hub, far, local, agent, remote, localId, remoteId, bridge, cursorPath, options };
};

describe("bounded durable bridge event batches", () => {
  it("takes one lock for 256 events, pins the peer, and shares one durability barrier", async () => {
    const mesh = store(), side = new StoreBridgeSide(mesh, "forge");
    // Deterministic count bound independent of this machine's fsync latency.
    vi.spyOn(performance, "now").mockReturnValue(0);
    const count = lockCount(mesh.root);
    const sync = vi.spyOn(fs, "fsyncSync");
    const events = Array.from({ length: 256 }, (_, index) => ({ event: event(String(index)) }));
    const first = await side.publishBatch(events);
    expect(first).toHaveLength(256);
    expect(count()).toBe(1);
    expect(mesh.read({ limit: 500 }).map(e => e.text)).toEqual(events.map(e => e.event.text));
    expect(mesh.read({ limit: 500 }).every(e => (e.data as BridgePublish["data"]).bridge.from === "forge")).toBe(true);
    // One event-file sync and namespace confirmation, not receipts per event.
    expect(sync.mock.calls.length).toBeLessThan(8);
    expect(fs.existsSync(path.join(mesh.root, "event-receipts"))).toBe(false);
    await expect(side.publishBatch([...events, events[0]!])).rejects.toThrow("1..256");
  }, 30000);

  it("returns a durable prefix at the 50 ms work bound and rechecks ownership per event", async () => {
    const mesh = store(), side = new StoreBridgeSide(mesh, "forge");
    const now = vi.spyOn(performance, "now").mockReturnValueOnce(0).mockReturnValue(50);
    const two = [{ event: event("a"), held: [identity.id] }, { event: event("b"), held: [identity.id] }];
    const holds = vi.spyOn(side, "holds").mockReturnValue(true);
    expect(await side.publishBatch(two)).toHaveLength(1);
    expect(holds).toHaveBeenCalledOnce();
    expect(mesh.read()).toHaveLength(1);
    now.mockReturnValue(0);
    holds.mockReset().mockReturnValueOnce(true).mockReturnValue(false);
    // A fresh prefix succeeds; its refused suffix cannot erase it or run out of order.
    const next = [{ event: event("c"), held: [identity.id] }, { event: event("d"), held: [identity.id] }];
    expect(await side.publishBatch(next)).toHaveLength(1);
    await expect(side.publish(next[1]!.event, next[1]!.held)).rejects.toBeInstanceOf(BridgeOwnershipError);
    expect(mesh.read().map(e => e.text)).toEqual(["a", "c"]);
  });

  it("rejects empty or invalid batches before writing", async () => {
    const mesh = store();
    await expect(mesh.publishBatch([])).rejects.toThrow("1..256");
    await expect(mesh.publishBatch([{ topic: "bad topic", from: identity }])).rejects.toThrow("Invalid Fabric mesh topic");
    expect(mesh.read()).toEqual([]);
  });

  it.each([false, true])("forwards both directions in bounded ordered batches (wire=%s)", async (wire) => {
    const { hub, far, bridge, local, agent, cursorPath, localId, remoteId } = await setup(wire);
    const batchIn = vi.spyOn(local, "publishBatch"), batchOut = vi.spyOn(agent, "publishBatch");
    const originals = [];
    for (let index = 0; index < 12; index++) {
      originals.push(await hub.publish({ topic: "fleet.work.batch", kind: "ask", from: localId, to: remoteId.id, text: `out-${index}` }));
      originals.push(await far.publish({ topic: "fleet.work.batch", kind: "ask", from: remoteId, to: localId.id, text: `in-${index}` }));
    }
    expect(await bridge.step()).toEqual({ toRemote: 12, toLocal: 12, dropped: 0 });
    expect(batchIn).toHaveBeenCalled();
    expect(batchOut).toHaveBeenCalled();
    for (const mesh of [hub, far]) {
      const bridged = mesh.read({ limit: 500 }).filter(e => e.verification === "bridge");
      expect(bridged.map(e => e.text)).toEqual(Array.from({ length: 12 }, (_, index) => `${mesh === hub ? "in" : "out"}-${index}`));
    }
    const saved = JSON.parse(fs.readFileSync(cursorPath, "utf8"));
    expect(saved.toRemote.after).toBeGreaterThanOrEqual(originals.at(-2)!.sequence);
    expect(saved.toLocal.after).toBeGreaterThanOrEqual(originals.at(-1)!.sequence);
    expect(await bridge.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
  });

  it("checkpoints only a committed prefix before a suffix lock timeout, then retries without duplicates", async () => {
    const { hub, far, local, remoteId, localId, bridge, cursorPath } = await setup(false);
    const sent = [];
    for (const text of ["one", "two", "three"]) sent.push(await far.publish({ topic: "fleet.work.batch", kind: "ask", from: remoteId, to: localId.id, text }));
    const batch = local.publishBatch.bind(local);
    vi.spyOn(local, "publishBatch").mockImplementationOnce(input => batch(input.slice(0, 1)))
      .mockRejectedValueOnce(Object.assign(new Error("busy"), { code: "FABRIC_MESH_LOCK_TIMEOUT" }));
    await expect(bridge.step()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(JSON.parse(fs.readFileSync(cursorPath, "utf8")).toLocal.after).toBe(sent[0]!.sequence);
    expect(hub.read().map(e => e.text)).toEqual(["one"]);
    expect(await bridge.step()).toMatchObject({ toLocal: 2 });
    expect(hub.read().map(e => e.text)).toEqual(["one", "two", "three"]);
  });

  it("restart reconciles a whole committed batch after a failed cursor checkpoint", async () => {
    const { hub, far, remoteId, localId, bridge, cursorPath, options } = await setup(false);
    for (const text of ["one", "two", "three"]) await far.publish({ topic: "fleet.work.batch", kind: "ask", from: remoteId, to: localId.id, text });
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(target) === cursorPath) throw new Error("checkpoint failed");
      return rename(source, target);
    });
    await expect(bridge.step()).rejects.toThrow("checkpoint failed");
    const committed = hub.read().map(e => e.id);
    expect(committed.length).toBeGreaterThan(0);
    vi.restoreAllMocks();
    const restarted = new MeshBridge(options);
    await restarted.start();
    await restarted.step();
    expect(hub.read().map(e => e.text)).toEqual(["one", "two", "three"]);
    expect(hub.read().slice(0, committed.length).map(e => e.id)).toEqual(committed);
  });

  it("a lost batch reply stops the pass and reconnect reconciles every committed ID", async () => {
    const { hub, far, local, localId, remoteId, bridge, options } = await setup(false);
    for (const text of ["one", "two", "three"]) await far.publish({ topic: "fleet.work.batch", kind: "ask", from: remoteId, to: localId.id, text });
    const batch = local.publishBatch.bind(local);
    vi.spyOn(local, "publishBatch").mockImplementationOnce(async inputs => {
      await batch(inputs);
      throw new Error("transport reply lost");
    });
    await expect(bridge.step()).rejects.toThrow("transport reply lost");
    const before = hub.read().map(e => e.id);
    vi.restoreAllMocks();
    const restarted = new MeshBridge(options);
    await restarted.start();
    await restarted.step();
    expect(hub.read().map(e => e.text)).toEqual(["one", "two", "three"]);
    expect(hub.read().slice(0, before.length).map(e => e.id)).toEqual(before);
  });

  it("does not acknowledge or replay an uncertain post-append failure", async () => {
    const mesh = store(), side = new StoreBridgeSide(mesh, "forge");
    const append = fs.appendFileSync.bind(fs);
    vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
      append(file, data, options);
      if (String(file) === path.join(mesh.root, "events.jsonl")) throw new Error("failed after complete append");
    });
    await expect(side.publishBatch([{ event: event("one") }, { event: event("two") }])).rejects.toThrow("outcome is uncertain");
    expect(mesh.read().map(e => e.text)).toEqual(["one"]);
    expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(false);
    expect(fs.readdirSync(path.join(mesh.root, ".lock.q"))).toEqual([]);
  });

  it("does not acknowledge a failed final event-file sync", async () => {
    const mesh = store(), side = new StoreBridgeSide(mesh, "forge");
    vi.spyOn(fs, "fsyncSync").mockImplementationOnce(() => { throw new Error("sync unavailable"); });
    await expect(side.publishBatch([{ event: event("one") }, { event: event("two") }])).rejects.toThrow("sync unavailable");
    expect(mesh.read().length).toBeGreaterThan(0);
    expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(false);
  });

  it("compaction retains all IDs in each acknowledged bounded prefix", async () => {
    const mesh = new MeshStore(scratch(), 2048, 500, { maxEventLogBytes: 16000, retainedEventLogBytes: 8000 });
    const side = new StoreBridgeSide(mesh, "forge");
    vi.spyOn(performance, "now").mockReturnValue(0);
    const inputs = Array.from({ length: 50 }, (_, index) => ({ event: { ...event(String(index)), text: "x".repeat(700) } }));
    let offset = 0;
    while (offset < inputs.length) {
      const prefix = await side.publishBatch(inputs.slice(offset));
      expect(prefix.length).toBeGreaterThan(0);
      expect(prefix.length).toBeLessThan(inputs.length);
      const current = new Set(mesh.read({ limit: 500 }).map(e => e.sequence));
      expect(prefix.every(e => current.has(e.sequence))).toBe(true);
      offset += prefix.length;
    }
  });

  it("uses the same archive ordering and durable live receipts without per-event bridge receipts", async () => {
    const mesh = store(), side = new StoreBridgeSide(mesh, "forge");
    const dir = scratch();
    fs.writeFileSync(path.join(mesh.root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
    const inputs = Array.from({ length: 12 }, (_, index) => ({ event: event(String(index)) }));
    for (let index = 0; index < inputs.length;) index += (await side.publishBatch(inputs.slice(index))).length;
    const events = mesh.read({ limit: 500 });
    expect(events.map(e => e.text)).toEqual(inputs.map(input => input.event.text));
    const archive = MeshArchive.fromRoot(mesh.root)!;
    for (const e of events) expect(archive.lookup(e.sequence)).toEqual(e);
    expect(fs.existsSync(path.join(mesh.root, "event-receipts"))).toBe(false);
  });

  it("legacy hello has no batch capability and therefore keeps singleton publication", async () => {
    const input = new PassThrough(), output = new PassThrough();
    const requests: string[] = [];
    input.on("data", chunk => {
      const request = JSON.parse(String(chunk));
      requests.push(request.op);
      output.write(JSON.stringify({ id: request.id, ok: true, result: request.op === "hello" ? { version: 1 } : { sequence: 1 } }) + "\n");
    });
    const remote = new RemoteBridgeSide(output, input);
    try {
      await remote.hello();
      expect(remote.publishBatch).toBeUndefined();
      await remote.publish(event("legacy"));
      expect(requests).toEqual(["hello", "publish"]);
    } finally { remote.close(); }
  });
});
