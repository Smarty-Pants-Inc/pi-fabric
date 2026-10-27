import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { buildSync } from "esbuild";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_PROTOCOL_VERSION, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { watchBridgeStore } from "../src/mesh/change-notifier.js";
import { readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

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
const fixture = (rpc = true, stateBackend?: "sqlite", callTimeoutMs?: number) => {
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
    remote = new RemoteBridgeSide(output, input, callTimeoutMs);
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
// `steps` counts the hub-side page reads: run() reads one per toRemote pass (smarty-dev#9751: the direction loops
// do not go through step()).
const launch = async (bridge: MeshBridge) => {
  const steps = vi.spyOn(bridge.options.local as StoreBridgeSide, "tail");
  let failure: unknown;
  const running = bridge.run().catch(error => { failure = error; });
  cleanups.push(async () => { await bridge.stop(); await running; });
  await until(() => steps.mock.calls.length > 0 || failure !== undefined);
  if (failure) throw failure;
  await settle();
  return { steps, running, failure: () => failure };
};

// smarty-dev#9751: head-of-line blocking. Each direction runs in its own sequential loop, so nothing in the
// hub-to-remote direction (a page read that takes minutes, a publish that never resolves) holds back a remote command.
describe("independent directions (smarty-dev#9751)", () => {
  const command = (lane: { id: string; identity: MeshIdentity }, lead: { id: string }, commandId: string) => ({ topic: "fabric.control.command", kind: "steer",
    from: lane.identity, to: lead.id, data: { version: 1, commandId, replyTo: lane.id, targetId: lead.id, operation: "steer" } });
  const within = async (check: () => boolean, ms: number) => {
    const started = performance.now();
    while (!check() && performance.now() - started < ms) await sleep(10);
    return { ok: check(), ms: Math.round(performance.now() - started) };
  };

  it("carries a remote command within 1 s while one hub page read stalls for 3 s", async () => {
    const f = fixture();
    const lead = await native(f.hub, "lead");
    const lane = await native(f.far, "lane");
    const live = await launch(f.bridge);
    const tail = f.local.tail!.bind(f.local);
    let stalled = false;
    f.local.tail = async (after, offset) => { if (!stalled) { stalled = true; await sleep(3_000); } return tail(after, offset); };
    await f.hub.publish({ topic: "unrelated", kind: "noise", from: lead.identity });
    await until(() => stalled);                                // toRemote is inside the slow read
    await f.far.publish(command(lane, lead, "c-page"));
    const arrived = await within(() => events(f.hub).some(event => event.topic === "fabric.control.command"), 1_000);
    expect(arrived.ok, `after ${arrived.ms} ms`).toBe(true);
    expect(live.failure()).toBeUndefined();
  }, 20_000);

  // pi-fabric#929 round 2: the 19:35Z shape is a catch-up after a restart. run() must not drain both directions in
  // turn before the loops start: each loop catches up from its own saved cursor.
  it("carries a remote command within 1 s after a restart with a saved cursor and a slow 3 s hub backlog", async () => {
    const f = fixture(false); // stop() closes an RPC transport; a real restart opens a new one

    const lead = await native(f.hub, "lead");
    const lane = await native(f.far, "lane");
    const first = await launch(f.bridge);
    await f.bridge.stop();
    await first.running;
    expect(fs.existsSync(f.bridge.options.cursorPath)).toBe(true);
    // While the bridge is down: a hub backlog, and a remote command behind it.
    for (let index = 0; index < 20; index++) await f.hub.publish({ topic: "unrelated", kind: "noise", from: lead.identity, text: String(index) });
    await f.far.publish(command(lane, lead, "c-restart"));
    // The restarted bridge's hub reads are slow: the first takes 3 s.
    const local = new StoreBridgeSide(f.hub, "far");
    const tail = local.tail.bind(local);
    let slow = false;
    local.tail = async (after, offset) => { if (!slow) { slow = true; await sleep(3_000); } return tail(after, offset); };
    const restarted = new MeshBridge({ ...f.bridge.options, local });
    let failure: unknown;
    const running = restarted.run().catch(error => { failure = error; });
    cleanups.push(async () => { await restarted.stop(); await running; });
    await until(() => slow);
    const arrived = await within(() => events(f.hub).some(event => event.topic === "fabric.control.command"), 1_000);
    expect(arrived.ok, `after ${arrived.ms} ms`).toBe(true);
    expect(failure).toBeUndefined();
  }, 20_000);

  it("carries a remote command within 1 s while a hub-to-remote publish never resolves", async () => {
    const f = fixture();
    const lead = await native(f.hub, "lead");
    const lane = await native(f.far, "lane");
    const live = await launch(f.bridge);
    let stuck = false;
    const never = () => { stuck = true; return new Promise<never>(() => {}); };
    (f.remote as RemoteBridgeSide).publish = never;
    (f.remote as RemoteBridgeSide).publishBatch = never;
    await f.hub.publish({ topic: "fleet.work.notify.9751", kind: "ask", from: lead.identity, to: lane.id, text: "stuck" });
    await until(() => stuck);                                  // toRemote awaits a publish that never settles
    await f.far.publish(command(lane, lead, "c-publish"));
    const arrived = await within(() => events(f.hub).some(event => event.topic === "fabric.control.command"), 1_000);
    expect(arrived.ok, `after ${arrived.ms} ms`).toBe(true);
    expect(live.failure()).toBeUndefined();
  }, 20_000);

  // The two directions share ONE transport (one ssh stdio; the agent serves its RPCs in order). A remote call that
  // passes its deadline closes that transport for both, so run() ends (it does not hang) and the mesh-bridge CLI
  // exits 1 ("stopped: ..."), and the service manager restarts it (docs/mesh-bridge.md, "exits with status 1").
  it("ends run() with the timeout when a remote call passes its deadline, rather than hanging", async () => {
    const f = fixture(true, undefined, 300);
    const lead = await native(f.hub, "lead");
    const lane = await native(f.far, "lane");
    // The agent never answers this publish (its FIFO is released at cleanup, so the agent can end).
    let release!: () => void;
    const held = new Promise<never>((_resolve, reject) => { release = () => reject(new Error("released")); });
    held.catch(() => undefined);
    cleanups.push(() => release());
    f.agent.publish = () => held;
    f.agent.publishBatch = () => held;
    let failure: unknown;
    const running = f.bridge.run().catch(error => { failure = error; });
    cleanups.push(async () => { await f.bridge.stop(); await running; });
    await until(() => events(f.far).length >= 0 && f.requests.length > 0);
    await f.hub.publish({ topic: "fleet.work.notify.9751", kind: "ask", from: lead.identity, to: lane.id, text: "times out" });
    const ended = await within(() => failure !== undefined, 5_000);
    expect(ended.ok, `after ${ended.ms} ms`).toBe(true);
    expect(String(failure)).toMatch(/did not answer|transport closed/);
  }, 20_000);
});

// smarty-dev#9751: a hub sender that is no participant (a factory host) gets the ACK of a command this link carried;
// an ACK for a command the link never carried still does not cross.
describe("ACK relay to a non-participant hub sender (smarty-dev#9751)", () => {
  it("relays the ACK of a carried command to its factory-host replyTo, and nothing else to that name", async () => {
    const f = fixture();
    await native(f.hub, "lead");
    const lane = await native(f.far, "lane");
    const other = await native(f.far, "other");
    const factory: MeshIdentity = { id: "factory-host:ryzen1", name: "factory-host", kind: "agent" };
    const live = await launch(f.bridge);
    await f.hub.publish({ topic: "fabric.control.command", kind: "steer", from: factory, to: lane.id,
      data: { version: 1, commandId: "c-carried", replyTo: factory.id, targetId: lane.id, operation: "steer" } });
    await until(() => events(f.far).some(event => event.topic === "fabric.control.command"));
    const ack = (commandId: string, from = lane) => f.far.publish({ topic: "fabric.control.ack", kind: "ack", from: from.identity, to: factory.id,
      data: { version: 1, commandId, targetId: lane.id, accepted: true, result: { secret: true } } });
    await ack("c-unknown");
    await ack("c-carried", other);        // (b) not from the owner the command was addressed to
    await ack("c-carried");
    await until(() => events(f.hub).some(event => event.topic === "fabric.control.ack"));
    await settle();
    const relayed = events(f.hub).filter(event => event.topic === "fabric.control.ack");
    expect(relayed.map(event => [(event.data as { commandId: string }).commandId, event.from.id])).toEqual([["c-carried", lane.id]]);
    // (c) a relayed ACK carries the outcome only, never the command's result payload
    expect(relayed[0]!.data).toMatchObject({ commandId: "c-carried", targetId: lane.id, accepted: true });
    expect(relayed[0]!.data).not.toHaveProperty("result");
    expect(live.failure()).toBeUndefined();
  });

  // pi-fabric#928 SEC P1 (a): a replyTo that is no single session or factory host (an org, topic or broadcast
  // name) is never relayed to, even when it is the sender's own id.
  it("relays no ACK to a non-unicast replyTo", async () => {
    const f = fixture();
    await native(f.hub, "lead");
    const lane = await native(f.far, "lane");
    const broadcast: MeshIdentity = { id: "org:smarty-pants", name: "org", kind: "agent" };
    const live = await launch(f.bridge);
    await f.hub.publish({ topic: "fabric.control.command", kind: "steer", from: broadcast, to: lane.id,
      data: { version: 1, commandId: "c-org", replyTo: broadcast.id, targetId: lane.id, operation: "steer" } });
    await until(() => events(f.far).some(event => event.topic === "fabric.control.command"));
    await f.far.publish({ topic: "fabric.control.ack", kind: "ack", from: lane.identity, to: broadcast.id,
      data: { version: 1, commandId: "c-org", targetId: lane.id, accepted: true } });
    await sleep(500);
    expect(events(f.hub).filter(event => event.topic === "fabric.control.ack")).toEqual([]);
    expect(live.failure()).toBeUndefined();
  });

  // pi-fabric#928 review P1: a command whose replyTo names someone else (here another hub session, not its sender)
  // records nothing, so its ACK never reaches that address through the relay.
  it("relays no ACK to a replyTo that is not the command's own sender", async () => {
    const f = fixture();
    await native(f.hub, "lead");
    const lane = await native(f.far, "lane");
    const factory: MeshIdentity = { id: "factory-host:ryzen1", name: "factory-host", kind: "agent" };
    const victim = "session:victim-00000000";
    const live = await launch(f.bridge);
    await f.hub.publish({ topic: "fabric.control.command", kind: "steer", from: factory, to: lane.id,
      data: { version: 1, commandId: "c-crafted", replyTo: victim, targetId: lane.id, operation: "steer" } });
    await until(() => events(f.far).some(event => event.topic === "fabric.control.command"));
    for (const to of [victim, factory.id]) {
      await f.far.publish({ topic: "fabric.control.ack", kind: "ack", from: lane.identity, to,
        data: { commandId: "c-crafted", targetId: lane.id, accepted: true, result: { secret: true } } });
    }
    await sleep(500);
    expect(events(f.hub).filter(event => event.topic === "fabric.control.ack")).toEqual([]);
    expect(live.failure()).toBeUndefined();
  });
});

describe("notification-driven bridge (smarty-dev#7299 A5)", () => {
  it("exits a native process with no watches when stdin ends before a queued hello runs", () => {
    const f = fixture(false);
    const source = String.raw`
      import fs from "node:fs";
      import { PassThrough } from "node:stream";
      import { once } from "node:events";
      import { MeshStore } from ${JSON.stringify(path.resolve("src/mesh/store.ts"))};
      import { StoreBridgeSide, serveBridgeAgent } from ${JSON.stringify(path.resolve("src/mesh/bridge.ts"))};
      const store = new MeshStore(${JSON.stringify(path.join(f.root, "native"))});
      const side = new StoreBridgeSide(store, "hub");
      const input = new PassThrough(), output = new PassThrough();
      let release, entered;
      const held = new Promise(resolve => { release = resolve; });
      const started = new Promise(resolve => { entered = resolve; });
      side.presence = async () => { entered(); await held; return { hosts: [], participants: [], reserved: [] }; };
      let installed = 0, active = 0;
      const watch = fs.watch;
      fs.watch = (...args) => {
        const watcher = watch(...args);
        installed++; active++;
        const close = watcher.close.bind(watcher);
        let closed = false;
        watcher.close = () => { if (!closed) { closed = true; active--; } close(); };
        return watcher;
      };
      const serving = serveBridgeAgent(side, input, output);
      input.write(JSON.stringify({ id: 1, op: "presence" }) + "\n");
      await started;
      const ended = once(input, "end");
      input.end(JSON.stringify({ id: 2, op: "hello", args: { version: ${BRIDGE_PROTOCOL_VERSION}, changes: true } }) + "\n");
      await ended; // hello is enqueued behind the still-held presence RPC
      release();
      await serving;
      output.end(); store.closeState();
      console.log(JSON.stringify({ installed, active }));
    `;
    const program = path.join(f.root, "queued-hello.mjs");
    buildSync({ stdin: { contents: source, resolveDir: process.cwd() }, outfile: program, bundle: true, platform: "node", format: "esm", packages: "external" });
    const child = spawnSync(process.execPath, [program], { encoding: "utf8", timeout: 3_000 });
    expect({ error: child.error?.message, status: child.status, stderr: child.stderr }).toEqual({ error: undefined, status: 0, stderr: "" });
    expect(JSON.parse(child.stdout.trim())).toEqual({ installed: 0, active: 0 });
  });

  it("disposes a watcher returned after finish runs reentrantly during installation", async () => {
    const f = fixture(false);
    const input = new PassThrough(), output = new PassThrough();
    const subscribe = f.agent.subscribeChanges.bind(f.agent);
    const close = vi.fn();
    vi.spyOn(f.agent, "subscribeChanges").mockImplementation((changed, failed) => {
      const unsubscribe = subscribe(changed, failed);
      input.emit("end"); // finish runs before the handler can assign unsubscribe
      return () => { close(); unsubscribe(); };
    });
    const serving = serveBridgeAgent(f.agent, input, output);
    input.write(JSON.stringify({ id: 1, op: "hello", args: { version: BRIDGE_PROTOCOL_VERSION, changes: true } }) + "\n");
    await serving;
    expect(close).toHaveBeenCalledTimes(1);
    input.end(); output.end();
  });

  it.each([undefined, false])("refuses a current-version peer with changes=%s instead of starting legacy polling", async changes => {
    const f = fixture(false);
    const input = new PassThrough(), output = new PassThrough();
    const operations: string[] = [];
    input.on("data", chunk => {
      for (const line of String(chunk).trim().split("\n")) {
        const request = JSON.parse(line);
        operations.push(request.op);
        expect(request.args).toEqual({ version: BRIDGE_PROTOCOL_VERSION, busyTail: true, changes: true });
        output.write(JSON.stringify({ id: request.id, ok: true, result: { version: BRIDGE_PROTOCOL_VERSION, ...(changes === undefined ? {} : { changes }) } }) + "\n");
      }
    });
    const remote = new RemoteBridgeSide(output, input);
    cleanups.push(() => remote.close());
    const logs: string[] = [];
    const bridge = new MeshBridge({ ...f.bridge.options, remote, log: message => logs.push(message) });
    const start = vi.spyOn(bridge, "start");
    await expect(bridge.run()).rejects.toThrow(/requires changes:true; refusing notification downgrade/);
    expect(start).not.toHaveBeenCalled();
    expect(operations).toEqual(["hello"]);
    expect(logs).toEqual([]);
  });

  it.each([undefined, BRIDGE_PROTOCOL_VERSION])("refuses a remote side without a subscription or explicit legacy protocol (%s)", async protocolVersion => {
    const f = fixture(false);
    const remote: BridgeSide = {
      protocolVersion,
      subscribeChanges: () => undefined,
      latestSequence: () => f.agent.latestSequence(),
      read: after => f.agent.read(after),
      presence: () => f.agent.presence(),
      mirror: presence => f.agent.mirror(presence),
      publish: event => f.agent.publish(event),
      bridgedIds: after => f.agent.bridgedIds(after),
    };
    const bridge = new MeshBridge({ ...f.bridge.options, remote });
    const start = vi.spyOn(bridge, "start");
    await expect(bridge.run()).rejects.toThrow(/refusing unversioned\/current notification downgrade/);
    expect(start).not.toHaveBeenCalled();
  });

  it.each([undefined, false])("refuses a current-version peer without changes:true even in ordinary hello (%s)", async changes => {
    const input = new PassThrough(), output = new PassThrough();
    input.on("data", chunk => {
      const request = JSON.parse(String(chunk));
      output.write(JSON.stringify({ id: request.id, ok: true, result: { version: BRIDGE_PROTOCOL_VERSION, ...(changes === undefined ? {} : { changes }) } }) + "\n");
    });
    const remote = new RemoteBridgeSide(output, input);
    cleanups.push(() => remote.close());
    await expect(remote.hello()).rejects.toThrow(/requires changes:true/);
  });

  it("does not let a peer change its reported current protocol to v1 during notification setup", async () => {
    const input = new PassThrough(), output = new PassThrough();
    let count = 0;
    input.on("data", chunk => {
      const request = JSON.parse(String(chunk));
      output.write(JSON.stringify({ id: request.id, ok: true, result: ++count === 1 ? { version: BRIDGE_PROTOCOL_VERSION, changes: true } : { version: 1 } }) + "\n");
    });
    const remote = new RemoteBridgeSide(output, input);
    cleanups.push(() => remote.close());
    await remote.hello();
    await expect(remote.subscribeChanges(() => undefined, () => undefined)).rejects.toThrow(/refusing protocol downgrade/);
  });

  it.each([undefined, 0, "1", BRIDGE_PROTOCOL_VERSION + 1])("refuses an unknown protocol %s rather than treating it as legacy", async version => {
    const input = new PassThrough(), output = new PassThrough();
    input.on("data", chunk => {
      const request = JSON.parse(String(chunk));
      output.write(JSON.stringify({ id: request.id, ok: true, result: { version } }) + "\n");
    });
    const remote = new RemoteBridgeSide(output, input);
    cleanups.push(() => remote.close());
    await expect(remote.subscribeChanges(() => undefined, () => undefined)).rejects.toThrow(/unsupported protocol/);
  });

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
    await until(() => (readHostLeases(f.hub.root).get(lane.id)?.mirror?.sourceExpiresAt ?? 0) >= expiry);
    const nextExpiry = Date.now() + 14_000;
    renew(replacement, nextExpiry);
    fs.renameSync(path.join(f.far.root, "host-leases"), path.join(f.root, "old-leases"));
    fs.renameSync(path.join(replacement, "host-leases"), path.join(f.far.root, "host-leases"));
    await f.far.publish({ topic: "fleet.work.notify.7299", kind: "probe", from: lane.identity, to: "absent" });
    await until(() => (readHostLeases(f.hub.root).get(lane.id)?.mirror?.sourceExpiresAt ?? 0) >= nextExpiry);
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

  // #13645: a 15 s native lease can have only 10 s left when sampled. Coalescing
  // for half the fixed mirror cap (7.5 s), then spending 4 s in the mirror write,
  // creates a sender-visible authority gap even though the origin has renewed.
  // Keep reason: recorded remote-owner trust boundary; never relax sender admission.
  it("flushes pending renewals against the sampled remaining life before a delayed commit", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const f = fixture(true, "sqlite");
    const lane = await native(f.far, "remaining-life", 10_000);
    const live = await launch(f.bridge);
    const initial = readHostLeases(f.hub.root).get(lane.id)!;
    const mirror = f.local.mirror.bind(f.local);
    vi.spyOn(f.local, "mirror").mockImplementation(async (...args) => {
      await new Promise(resolve => setTimeout(resolve, 4_000));
      return mirror(...args);
    });
    const senderId = "session:deadline-sender-00000000";
    const directory = new ParticipantDirectory(f.hub, { enabled: true, hostId: senderId, rootId: senderId,
      identity: { id: senderId, name: "sender", kind: "main" } });
    cleanups.push(() => directory.close());
    await vi.advanceTimersByTimeAsync(5_000);
    writeHostLease(f.far.root, { id: lane.id, rootId: lane.id, identityId: lane.id,
      startedAt: lane.host.startedAt, updatedAt: Date.now(), expiresAt: Date.now() + 15_000 });
    await settle(); // Real native watch -> RPC notification -> pending half-lease flush.
    try {
      await vi.advanceTimersByTimeAsync(5_500);
      await settle();
      expect(initial.expiresAt).toBeLessThan(Date.now());
      expect(directory.get(lane.id)).toMatchObject({ remoteHost: "far", stale: false });
      expect(directory.get(lane.id)?.bridgeStale).toBeUndefined();
      expect(f.local.holds(lane.id)).toBe(true);
      expect(readHostLeases(f.hub.root).get(lane.id)!.expiresAt).toBeGreaterThan(Date.now());
      expect(live.failure()).toBeUndefined();
    } finally {
      // Settle the old head's later commit too, so the RED run has no stranded timer.
      await vi.advanceTimersByTimeAsync(5_000);
      await settle();
    }
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

  it.each([undefined, false])("keeps 250 ms polling only for an explicit v1 endpoint (changes=%s) and delivers both directions without loss", async changes => {
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
        const result = request.op === "hello" ? { version: 1, ...(changes === undefined ? {} : { changes }) }
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
    // Polling goes on: since smarty-dev#9751 each direction polls in its own loop, so count the remote reads.
    const count = operations.filter(op => op === "read").length;
    await sleep(550);
    expect(operations.filter(op => op === "read").length).toBeGreaterThan(count);
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
    expect(frames[0]).toMatchObject({ id: 1, result: { version: 1, changes: true } });
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
