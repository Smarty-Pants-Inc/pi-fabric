import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { watchBridgeStore } from "../src/mesh/change-notifier.js";
import { readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const roots: string[] = [];
const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const root of roots.splice(0)) fs.rmSync(root, { force: true, recursive: true });
});
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
const settle = () => sleep(40);
const until = async (check: () => boolean) => {
  const deadline = performance.now() + 3_000;
  while (!check() && performance.now() < deadline) await sleep(5);
  expect(check()).toBe(true);
};
const events = (store: MeshStore) => {
  const result = [];
  let after = 0;
  for (;;) {
    const page = store.read({ after });
    if (!page.length) return result;
    result.push(...page);
    after = page.at(-1)!.sequence;
  }
};
const fixture = (rpc = true, stateBackend?: "sqlite") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-notifications-"));
  roots.push(root);
  const options = stateBackend ? { stateBackend } : {};
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 2, options);
  const far = new MeshStore(path.join(root, "far"), 64 * 1024, 2, options);
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  const local = new StoreBridgeSide(hub, "far");
  const agent = new StoreBridgeSide(far, "hub");
  const requests: string[] = [];
  let remote: StoreBridgeSide | RemoteBridgeSide = agent;
  if (rpc) {
    const input = new PassThrough();
    const output = new PassThrough();
    input.on("data", chunk => { for (const line of String(chunk).trim().split("\n")) requests.push(JSON.parse(line).op); });
    const serving = serveBridgeAgent(agent, input, output);
    remote = new RemoteBridgeSide(output, input);
    const rpcSide = remote;
    cleanups.push(async () => { rpcSide.close(); input.end(); await serving; output.end(); });
  }
  // Cursor inside the watched mesh exercises filtering of our atomic checkpoints.
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(hub.root, "bridge-cursor.json"), stopMs: 100 });
  cleanups.push(() => bridge.stop());
  return { root, hub, far, local, agent, remote, bridge, requests };
};
const native = async (store: MeshStore, name: string, ttl = 60_000, participant = true) => {
  const now = Date.now();
  const id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  const host = { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + ttl };
  await store.put({ key: key("hosts", id), value: host, identity });
  const entry: MeshStateEntry = { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: [],
    sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } };
  if (participant) writeParticipantFile(store.root, entry);
  return { id, identity, entry, host };
};
const launch = async (bridge: MeshBridge) => {
  const steps = vi.spyOn(bridge, "step");
  let failure: unknown;
  const running = bridge.run().catch(error => { failure = error; });
  cleanups.push(async () => { await bridge.stop(); await running; });
  await until(() => steps.mock.calls.length > 0 || failure !== undefined);
  if (failure) throw failure;
  await settle();
  return { steps, running, failure: () => failure };
};

describe("notification-driven bridge (smarty-dev#7299 A5)", () => {
  it.each([false, true])("has zero timers and passes over 305 idle seconds (stdio: %s)", async rpc => {
    const f = fixture(rpc);
    const live = await launch(f.bridge); // also tests automatic hello for run() callers
    const passes = live.steps.mock.calls.length;
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const timers = vi.spyOn(globalThis, "setTimeout");
    await vi.advanceTimersByTimeAsync(305_001);
    await turn();
    expect(timers).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    expect(live.steps).toHaveBeenCalledTimes(passes);
    expect(live.failure()).toBeUndefined();
    if (rpc) expect(f.requests).toContain("hello");
  });

  it("latches an append after its direction drained, while an RPC pass is still in flight", async () => {
    const f = fixture();
    const lead = await native(f.hub, "lead");
    const lane = await native(f.far, "lane");
    const live = await launch(f.bridge);
    let release!: () => void;
    let entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const seam = new Promise<void>(resolve => { entered = resolve; });
    const read = f.agent.read.bind(f.agent);
    let intercept = true;
    f.agent.read = async after => {
      const page = await read(after);
      if (intercept) { intercept = false; entered(); await held; }
      return page;
    };
    // Unrelated log traffic still changes the cursor and wakes a pass.
    await f.hub.publish({ topic: "unrelated", kind: "probe", from: lead.identity });
    await seam;
    try {
      for (let index = 0; index < 12; index++) await f.hub.publish({ topic: "fleet.work.notify.7299", kind: "ask",
        from: lead.identity, to: lane.id, text: String(index) });
      // Agent-side notifications are unsolicited and flow even though its FIFO RPC is held.
      await f.far.publish({ topic: "fleet.work.notify.7299", kind: "reply", from: lane.identity, to: lead.id, text: "inbound" });
    } finally { release(); }
    await until(() => events(f.far).filter(event => event.text !== "inbound" && event.topic === "fleet.work.notify.7299").length === 12);
    await until(() => events(f.hub).some(event => event.text === "inbound"));
    expect(events(f.far).filter(event => event.kind === "ask").map(event => event.text))
      .toEqual(Array.from({ length: 12 }, (_, index) => String(index)));
    expect(f.requests.some(op => /wait|poll/i.test(op))).toBe(false);
    expect(live.failure()).toBeUndefined();
  });

  it("discovers mkdir and rebinds atomic participant/lease directory replacements; renews from the origin", async () => {
    const f = fixture();
    const live = await launch(f.bridge);
    const lane = await native(f.far, "new-lane", 10_000, false);
    await until(() => !!f.hub.get(key("hosts", lane.id)));
    expect(fs.existsSync(path.join(f.far.root, "participants"))).toBe(false);
    writeParticipantFile(f.far.root, lane.entry);
    await until(() => !!f.hub.get(lane.entry.key));
    const updated = { ...lane.entry, value: { ...lane.entry.value as object, status: "running", updatedAt: Date.now() + 1 } };
    const replacement = path.join(f.root, "replacement");
    fs.mkdirSync(replacement);
    writeParticipantFile(replacement, updated);
    fs.renameSync(path.join(f.far.root, "participants"), path.join(f.root, "old-participants"));
    fs.renameSync(path.join(replacement, "participants"), path.join(f.far.root, "participants"));
    await until(() => (f.hub.get(lane.entry.key)?.value as { status?: string })?.status === "running");
    const renew = (root: string, expiry: number) => writeHostLease(root, { id: lane.id, rootId: lane.id,
      identityId: lane.id, startedAt: lane.host.startedAt, updatedAt: Date.now(), expiresAt: expiry });
    const expiry = Date.now() + 12_000;
    renew(f.far.root, expiry); // host-leases mkdir after watch setup
    // Renewal-only updates are pending until the half-lease flush; actual work demands fresh authority now.
    await f.far.publish({ topic: "fleet.work.notify.7299", kind: "probe", from: lane.identity, to: "absent" });
    await until(() => (readHostLeases(f.hub.root).get(lane.id)?.expiresAt ?? 0) >= expiry);
    const nextExpiry = Date.now() + 14_000;
    renew(replacement, nextExpiry);
    fs.renameSync(path.join(f.far.root, "host-leases"), path.join(f.root, "old-leases"));
    fs.renameSync(path.join(replacement, "host-leases"), path.join(f.far.root, "host-leases"));
    await f.far.publish({ topic: "fleet.work.notify.7299", kind: "probe", from: lane.identity, to: "absent" });
    await until(() => (readHostLeases(f.hub.root).get(lane.id)?.expiresAt ?? 0) >= nextExpiry);
    // Own mirror/lease writes and cursor renames settle, rather than feeding a renewal loop.
    await settle();
    const mirrors = vi.spyOn(f.local, "mirror");
    await settle();
    expect(mirrors).not.toHaveBeenCalled();
    expect(live.failure()).toBeUndefined();
  });

  it("reconciles finite source expiry once, then becomes timer-free instead of renewing every 5 s", async () => {
    const f = fixture();
    const lane = await native(f.far, "short-lane", 200);
    const live = await launch(f.bridge);
    expect(f.hub.get(lane.entry.key)).toBeDefined();
    await until(() => f.hub.get(lane.entry.key) === undefined);
    expect(readHostLeases(f.hub.root).has(lane.id)).toBe(false);
    await settle();
    const mirrors = vi.spyOn(f.local, "mirror");
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const timers = vi.spyOn(globalThis, "setTimeout");
    await vi.advanceTimersByTimeAsync(305_001);
    expect(timers).not.toHaveBeenCalled();
    expect(mirrors).not.toHaveBeenCalled();
    expect(live.failure()).toBeUndefined();
  });

  it("keeps a 90 s silent origin's capped mirror live at 20 s and repairs external row/lease pruning", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const f = fixture(false);
    const lane = await native(f.far, "long-lane", 90_000);
    const live = await launch(f.bridge);
    await vi.advanceTimersByTimeAsync(20_000);
    await settle();
    expect(f.local.holds(lane.id)).toBe(true);
    expect(readHostLeases(f.hub.root).get(lane.id)!.expiresAt).toBeGreaterThan(Date.now());
    expect(readHostLeases(f.hub.root).get(lane.id)!.expiresAt).toBeLessThanOrEqual(lane.host.expiresAt);
    await f.hub.delete({ key: lane.entry.key });
    await f.hub.delete({ key: key("hosts", lane.id) });
    await until(() => !!f.hub.get(lane.entry.key) && f.local.holds(lane.id));
    const leaseDir = path.join(f.hub.root, "host-leases");
    for (const name of fs.readdirSync(leaseDir)) fs.rmSync(path.join(leaseDir, name));
    await until(() => readHostLeases(f.hub.root).has(lane.id));
    expect(f.local.holds(lane.id)).toBe(true);
    await settle();
    const mirrors = vi.spyOn(f.local, "mirror");
    await settle();
    expect(mirrors).not.toHaveBeenCalled();
    expect(live.failure()).toBeUndefined();
  });

  it("coalesces 20 separate lane heartbeats into one half-lease flush, with no per-heartbeat locks", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const f = fixture();
    const lanes = [];
    for (let index = 0; index < 20; index++) lanes.push(await native(f.far, `batch-${index}`, 15_000));
    const live = await launch(f.bridge);
    const hubBatch = vi.spyOn(f.hub, "writeBatch");
    const farBatch = vi.spyOn(f.far, "writeBatch");
    const mirrors = vi.spyOn(f.local, "mirror");
    await vi.advanceTimersByTimeAsync(5_000);
    for (const lane of lanes) {
      writeHostLease(f.far.root, { id: lane.id, rootId: lane.id, identityId: lane.id, startedAt: lane.host.startedAt,
        updatedAt: Date.now(), expiresAt: Date.now() + 15_000 });
      await sleep(2); // separate native fs.watch deliveries, not one synchronous write burst
    }
    await settle();
    expect(mirrors).not.toHaveBeenCalled();
    expect(hubBatch).not.toHaveBeenCalled();
    expect(farBatch).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(2_500);
    await until(() => mirrors.mock.calls.length === 1);
    await settle();
    expect(hubBatch).toHaveBeenCalledTimes(1);
    expect(farBatch).toHaveBeenCalledTimes(1);
    for (const lane of lanes) {
      expect(f.local.holds(lane.id)).toBe(true);
      expect(readHostLeases(f.hub.root).get(lane.id)?.expiresAt).toBe(lane.host.expiresAt + 5_000);
    }
    expect(live.failure()).toBeUndefined();
  });

  it("skips an equal unchanged mirror lease while retaining the committed ownership fence", async () => {
    const f = fixture(false);
    const lane = await native(f.far, "equal-lane", 8_000);
    const snapshot = await f.agent.presence();
    await f.local.mirror(snapshot);
    const renames = vi.spyOn(fs, "renameSync");
    await f.local.mirror(snapshot);
    expect(renames.mock.calls.filter(([, destination]) => String(destination).startsWith(path.join(f.hub.root, "host-leases"))))
      .toHaveLength(0);
    expect(f.local.holds(lane.id)).toBe(true);
  });

  it("installs watches before the startup snapshot and retains changes while presence is in flight", async () => {
    const f = fixture();
    const lead = await native(f.hub, "lead");
    let release!: () => void;
    let observed!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const seam = new Promise<void>(resolve => { observed = resolve; });
    const presence = f.agent.presence.bind(f.agent);
    let first = true;
    f.agent.presence = async () => {
      const snapshot = await presence();
      if (first) { first = false; observed(); await held; }
      return snapshot;
    };
    const watches = vi.spyOn(fs, "watch");
    let failed: unknown;
    const running = f.bridge.run().catch(error => { failed = error; });
    cleanups.push(async () => { await f.bridge.stop(); await running; });
    await seam;
    expect(watches.mock.calls.map(([target]) => String(target))).toEqual(expect.arrayContaining([f.hub.root, f.far.root]));
    const lane = await native(f.far, "during-snapshot");
    await f.far.publish({ topic: "fleet.work.notify.7299", kind: "reply", from: lane.identity, to: lead.id, text: "during-start" });
    release();
    await until(() => !!f.hub.get(lane.entry.key));
    await until(() => events(f.hub).some(event => event.text === "during-start"));
    expect(failed).toBeUndefined();
  });

  it("keeps 250 ms polling only for a legacy endpoint and delivers both directions without loss", async () => {
    const f = fixture(false);
    const lead = await native(f.hub, "lead");
    const lane = await native(f.far, "legacy-lane");
    const input = new PassThrough();
    const output = new PassThrough();
    const operations: string[] = [];
    let queue = Promise.resolve();
    input.on("data", chunk => {
      for (const line of String(chunk).trim().split("\n")) queue = queue.then(async () => {
        const request = JSON.parse(line);
        operations.push(request.op);
        const args = request.args;
        const result = request.op === "hello" ? { version: 1 }
          : request.op === "latestSequence" ? await f.agent.latestSequence()
          : request.op === "read" ? await f.agent.read(args.after)
          : request.op === "presence" ? await f.agent.presence()
          : request.op === "mirror" ? await f.agent.mirror(args)
          : request.op === "publish" ? await f.agent.publish(args)
          : request.op === "bridgedIds" ? await f.agent.bridgedIds(args.after) : undefined;
        output.write(JSON.stringify({ id: request.id, ok: true, result }) + "\n");
      });
    });
    const remote = new RemoteBridgeSide(output, input);
    cleanups.push(() => remote.close());
    const logs: string[] = [];
    const bridge = new MeshBridge({ ...f.bridge.options, remote, log: message => logs.push(message) });
    const live = await launch(bridge);
    await f.far.publish({ topic: "fleet.work.notify.7299", kind: "reply", from: lane.identity, to: lead.id, text: "legacy-inbound" });
    await f.hub.publish({ topic: "fleet.work.notify.7299", kind: "ask", from: lead.identity, to: lane.id, text: "legacy-outbound" });
    await until(() => events(f.hub).some(event => event.text === "legacy-inbound"));
    await until(() => events(f.far).some(event => event.text === "legacy-outbound"));
    const count = live.steps.mock.calls.length;
    await sleep(550);
    expect(live.steps.mock.calls.length).toBeGreaterThan(count);
    expect(logs.filter(message => message.includes("legacy peer"))).toHaveLength(1);
    expect(live.failure()).toBeUndefined();
    expect(operations.filter(op => op === "publish")).toHaveLength(1);
  });

  it("does not send unsolicited frames to a legacy hello client", async () => {
    const f = fixture(false);
    const input = new PassThrough();
    const output = new PassThrough();
    const frames: Array<{ notification?: unknown; id?: number }> = [];
    output.on("data", chunk => frames.push(JSON.parse(String(chunk))));
    const serving = serveBridgeAgent(f.agent, input, output);
    cleanups.push(async () => { input.end(); await serving; output.end(); });
    input.write(JSON.stringify({ id: 1, op: "hello" }) + "\n");
    await until(() => frames.length === 1);
    await f.far.publish({ topic: "unrelated", kind: "probe", from: { id: "probe", name: "probe", kind: "main" } });
    await native(f.far, "legacy-client");
    await settle();
    expect(frames).toHaveLength(1);
    expect(frames[0]).toMatchObject({ id: 1 });
  });

  it("fails rather than polling when fs.watch cannot be installed", async () => {
    const f = fixture(false);
    vi.spyOn(fs, "watch").mockImplementation(() => { throw Object.assign(new Error("unsupported"), { code: "ENOSYS" }); });
    await expect(f.bridge.run()).rejects.toThrow(/requires filesystem notifications.*unsupported/);
  });

  it("fails a suspended idle run on an asynchronous watcher error", async () => {
    const f = fixture(false);
    const original = fs.watch.bind(fs);
    const watchers: fs.FSWatcher[] = [];
    vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
      const watcher = original(...args);
      watchers.push(watcher);
      return watcher;
    });
    const live = await launch(f.bridge);
    watchers[0]!.emit("error", new Error("lost watch"));
    await live.running;
    expect(live.failure()).toMatchObject({ message: expect.stringContaining("lost watch") });
  });

  it("latches a SQLite filesystem signal arriving before COMMIT, without accepting the old read as settled", async () => {
    const f = fixture(false, "sqlite");
    const lane = await native(f.far, "sqlite-lane");
    const live = await launch(f.bridge);
    const { DatabaseSync } = await import("node:sqlite");
    const raw = new DatabaseSync(path.join(f.far.root, "state.db"));
    raw.exec("PRAGMA busy_timeout = 0");
    const fence = vi.spyOn(f.far, "withStateWriteFence");
    try {
      raw.exec("BEGIN IMMEDIATE");
      raw.prepare("UPDATE kv SET value = ? WHERE key = ?").run(
        JSON.stringify({ ...lane.host, identity: { ...lane.identity, name: "committed-name" } }), key("hosts", lane.id));
      raw.exec("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE name = 'commit_no'");
      // A pre-commit fs hint (the same race as a writer spilling WAL frames before COMMIT).
      const now = new Date();
      fs.utimesSync(path.join(f.far.root, "state.db"), now, now);
      await until(() => fence.mock.calls.length > 0);
      await sleep(120);
      expect(fence.mock.calls.length).toBeGreaterThanOrEqual(2);
      expect(live.failure()).toBeUndefined();
      expect((f.hub.get(key("hosts", lane.id))!.value as { identity: MeshIdentity }).identity.name).toBe("main");
      raw.exec("COMMIT");
      await until(() => (f.hub.get(key("hosts", lane.id))?.value as { identity?: MeshIdentity })?.identity?.name === "committed-name");
      expect(f.local.holds(lane.id)).toBe(true);
    } finally {
      try { raw.exec("ROLLBACK"); } catch { /* already committed */ }
      raw.close();
    }
    expect(live.failure()).toBeUndefined();
  });

  it("watches file and SQLite state replacements/WAL writes, but ignores mirror cursor/temporary names", async () => {
    const f = fixture(false);
    const changes: string[] = [];
    const close = watchBridgeStore(f.hub.root, kind => changes.push(kind), error => { throw error; });
    cleanups.push(close);
    for (const name of ["state.json", "state.db", "state.db-wal", "state.db-journal", "events.jsonl", "generation"]) {
      changes.length = 0;
      const staged = path.join(f.root, "staged");
      fs.writeFileSync(staged, "probe");
      fs.renameSync(staged, path.join(f.hub.root, name));
      await until(() => changes.length > 0);
      expect(changes).toContain(name === "events.jsonl" || name === "generation" ? "events" : "presence");
    }
    await settle();
    changes.length = 0;
    fs.writeFileSync(path.join(f.hub.root, "bridge-cursor.json"), "cursor");
    fs.writeFileSync(path.join(f.hub.root, "state.json.123.tmp"), "temporary");
    await settle();
    expect(changes).toEqual([]);
    close();
    // Probe files are intentionally not valid store data; remove before the bridge cleanup.
    for (const name of ["state.json", "state.db", "state.db-wal", "state.db-journal", "events.jsonl", "generation"]) fs.rmSync(path.join(f.hub.root, name));
  });
});
