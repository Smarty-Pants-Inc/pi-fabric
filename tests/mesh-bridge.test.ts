import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import {
  type BridgeSide,
  MeshBridge,
  RemoteBridgeSide,
  serveBridgeAgent,
  StoreBridgeSide,
} from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { runBridge, transportCommand } from "../src/mesh-bridge.js";
import { readHostLeases } from "../src/topology/host-leases.js";
import { RootInbox } from "../src/topology/root-inbox.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFiles, writeParticipantFile } from "../src/topology/participant-files.js";

// Two scratch meshes: "dev1" (the hub, in-process) and "forge" (reached through the agent over
// a stdio pair, as the ssh transport does).
const roots: string[] = [];
const cleanups: Array<() => void> = [];

afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-bridge-"));
  roots.push(root);
  return root;
};

const hash = (id: string): string => createHash("sha256").update(id).digest("hex");

/** A live root session on a mesh, as the participant directory writes it. */
/** A root's canonical id, as Fabric mints it (`session:<session id>`); its host id is the same. */
const sid = (name: string): string => `session:${name}-00000000`;

const addRoot = async (
  store: MeshStore, name: string, expiresIn = 15_000,
  claims: { hostId?: string; id?: string; label?: string; sessionId?: string; cwd?: string } = {},
) => {
  const now = Date.now();
  const hostId = claims.hostId ?? claims.id ?? sid(name);
  const sessionId = claims.sessionId ?? name;
  const identity: MeshIdentity = { id: claims.id ?? sid(name), name: "main", kind: "main", sessionId };
  await store.put({
    key: `topology/hosts/${hash(hostId)}`,
    identity,
    value: { format: 1, id: hostId, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + expiresIn },
  });
  await store.put({
    key: `topology/participants/${hash(identity.id)}`,
    identity,
    value: {
      format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: hostId, ownerIdentityId: identity.id,
      name, label: claims.label ?? name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
      sessionId, startedAt: now, updatedAt: now, controlProtocol: "v1", ...(claims.cwd ? { cwd: claims.cwd } : {}),
    },
  });
  return { hostId, identity };
};

interface SetupOptions {
  hub?: MeshStore;
  remoteName?: string;
  maxEventBytes?: number;
  /** Carry the agent's replies through a real OS pipe (a relay child), as ssh does. */
  realPipe?: boolean;
  callTimeoutMs?: number;
  stopMs?: number;
  /** Wrap the hub's view of the remote (to hold or fail its calls). */
  wrapRemote?: (remote: RemoteBridgeSide) => BridgeSide;
  local?: (hub: MeshStore, remoteName: string) => StoreBridgeSide;
  agent?: (far: MeshStore) => StoreBridgeSide;
  pollMs?: number;
}

const setup = (cursorPath?: string, options: SetupOptions = {}) => {
  const eventBytes = options.maxEventBytes ?? 64 * 1024;
  const hub = options.hub ?? new MeshStore(scratch(), eventBytes, 100);
  const far = new MeshStore(scratch(), eventBytes, 100);
  const remoteName = options.remoteName ?? "forge";
  const toAgent = new PassThrough();
  const fromAgent = new PassThrough();
  // A switch in the hub->agent direction: when silent, the agent never sees a request.
  const gate = { silent: false };
  const toGate = new PassThrough();
  toGate.on("data", (chunk) => {
    if (!gate.silent) toAgent.write(chunk);
  });
  void serveBridgeAgent(options.agent?.(far) ?? new StoreBridgeSide(far, "dev1"), toAgent, fromAgent);
  let replies: NodeJS.ReadableStream & import("node:stream").Readable = fromAgent;
  if (options.realPipe) {
    const relay = spawn(process.execPath, ["-e", "process.stdin.pipe(process.stdout)"], { stdio: ["pipe", "pipe", "inherit"] });
    fromAgent.pipe(relay.stdin);
    replies = relay.stdout;
    cleanups.push(() => relay.kill("SIGKILL"));
  }
  const remote = new RemoteBridgeSide(replies, toGate, options.callTimeoutMs);
  cleanups.push(() => toAgent.end());
  const logs: string[] = [];
  const bridge = new MeshBridge({
    localName: "dev1", remoteName,
    local: options.local?.(hub, remoteName) ?? new StoreBridgeSide(hub, remoteName),
    remote: options.wrapRemote?.(remote) ?? remote,
    cursorPath: cursorPath ?? path.join(scratch(), "cursor.json"),
    presenceMs: 0,
    ...(options.pollMs ? { pollMs: options.pollMs } : {}),
    ...(options.stopMs ? { stopMs: options.stopMs } : {}),
    log: (message) => logs.push(message),
  });
  return { hub, far, remote, bridge, logs, gate };
};

const command = (targetId: string, replyTo: string) => ({
  version: 1, commandId: `c-${Math.random()}`, targetId, operation: "followUp", replyTo, message: "hi",
  requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
});

const on = (store: MeshStore, topic: string): MeshEvent[] => store.read({ after: 0, limit: 100 }).filter((e) => e.topic === topic);

const lockTimeout = (): Error => Object.assign(new Error("mesh busy"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });

const waitFor = async (check: () => boolean): Promise<void> => {
  const until = Date.now() + 3_000;
  while (!check() && Date.now() < until) await new Promise((resolve) => setTimeout(resolve, 10));
  expect(check()).toBe(true);
};

describe("mesh bridge", () => {
  it.each(["local", "remote"] as const)("retries one typed %s mesh timeout at startup, presence and forward without loss or duplicates", async (where) => {
    for (const op of ["bridgedIds", "mirror", "publish"] as const) {
      let side!: StoreBridgeSide;
      const wrap = (store: MeshStore, peer: string): StoreBridgeSide => (side = new StoreBridgeSide(store, peer));
      const { hub, far, bridge, logs } = setup(undefined, {
        pollMs: 5, stopMs: 100,
        ...(where === "local" ? { local: wrap } : { agent: (store: MeshStore) => wrap(store, "dev1") }),
      });
      const lane = await addRoot(hub, "lane");
      const forge = await addRoot(far, "forge-main");
      await bridge.start();
      const original = side[op].bind(side) as (...args: unknown[]) => Promise<unknown>;
      let calls = 0;
      const failAt = op === "publish" ? 2 : 1; // A committed prefix must not replay after retry.
      const times: number[] = [];
      Object.assign(side, { [op]: async (...args: unknown[]) => {
        times.push(Date.now());
        if (++calls === failAt) throw lockTimeout();
        return original(...args);
      } });
      // The startup cursor is already durable: backlog arriving during the retry is retained.
      const source = where === "local" ? far : hub;
      const target = where === "local" ? hub : far;
      const from = where === "local" ? forge.identity : lane.identity;
      const to = where === "local" ? lane.identity.id : forge.identity.id;
      const sent = [
        await source.publish({ topic: "fleet.work.retry.1", kind: "ask", from, to, text: "one" }),
        await source.publish({ topic: "fleet.work.retry.1", kind: "ask", from, to, text: "two" }),
      ];
      let failure: unknown;
      const running = bridge.run().catch((error: unknown) => { failure = error; });
      try {
        await waitFor(() => on(target, "fleet.work.retry.1").length === 2);
        await waitFor(() => calls >= 2);
        expect(times[failAt]! - times[failAt - 1]!).toBeGreaterThanOrEqual(80);
        expect(on(target, "fleet.work.retry.1").map((event) => event.text)).toEqual(["one", "two"]);
        expect(on(target, "fleet.work.retry.1").map((event) => (event.data as { bridge: { id: string } }).bridge.id))
          .toEqual(sent.map((event) => event.id));
        expect(logs.filter((line) => line.includes("retrying"))).toHaveLength(1);
        expect(logs.some((line) => /dropped|refused/.test(line))).toBe(false);
        expect(failure).toBeUndefined();
      } finally {
        await bridge.stop();
        await running;
      }
    }
  });

  it.each(["local", "remote"] as const)("releases new seen IDs only after successful %s checkpoints", async (where) => {
    const cursorPath = path.join(scratch(), "cursor.json");
    const { hub, far, bridge } = setup(cursorPath);
    const lane = await addRoot(hub, "lane");
    const forge = await addRoot(far, "forge-main");
    await bridge.start();
    const source = where === "local" ? far : hub;
    const target = where === "local" ? hub : far;
    const from = where === "local" ? forge.identity : lane.identity;
    const to = where === "local" ? lane.identity.id : forge.identity.id;
    const sent: MeshEvent[] = [];
    for (let index = 0; index < 12; index++) {
      sent.push(await source.publish({ topic: "fleet.work.seen.1", kind: "ask", from, to, text: String(index) }));
    }
    const known = new Set(sent.map((event) => event.id));
    const observed = new Set<Set<string>>();
    const add = Set.prototype.add;
    Set.prototype.add = function (id) {
      if (known.has(id)) add.call(observed, this);
      return add.call(this, id);
    };
    try {
      expect(await bridge.step()).toMatchObject({ [where === "local" ? "toLocal" : "toRemote"]: sent.length, dropped: 0 });
      expect(observed.size).toBe(1);
      for (const seen of observed) expect(sent.filter((event) => seen.has(event.id))).toEqual([]);
      const saved = JSON.parse(fs.readFileSync(cursorPath, "utf8"));
      expect(saved[where === "local" ? "toLocal" : "toRemote"].after).toBe(sent.at(-1)!.sequence);
      expect(on(target, "fleet.work.seen.1").map((event) => (event.data as { bridge: { id: string } }).bridge.id))
        .toEqual(sent.map((event) => event.id));
    } finally {
      Set.prototype.add = add;
    }
  });

  it.each(["local", "remote"] as const)("retains an uncheckpointed ID on a failed %s save", async (where) => {
    const cursorDir = scratch();
    const cursorPath = path.join(cursorDir, "cursor.json");
    let side!: StoreBridgeSide;
    const wrap = (store: MeshStore, peer: string): StoreBridgeSide => (side = new StoreBridgeSide(store, peer));
    const { hub, far, bridge } = setup(cursorPath, where === "local"
      ? { local: wrap } : { agent: (store) => wrap(store, "dev1") });
    const lane = await addRoot(hub, "lane");
    const forge = await addRoot(far, "forge-main");
    await bridge.start();
    const source = where === "local" ? far : hub;
    const sent = await source.publish({ topic: "fleet.work.seen.1", kind: "ask",
      from: where === "local" ? forge.identity : lane.identity,
      to: where === "local" ? lane.identity.id : forge.identity.id });
    const observed = new Set<Set<string>>();
    const add = Set.prototype.add;
    Set.prototype.add = function (id) {
      if (id === sent.id) add.call(observed, this);
      return add.call(this, id);
    };
    const publish = side.publish.bind(side);
    const backup = path.join(scratch(), "saved");
    side.publish = async (...args) => {
      const result = await publish(...args);
      fs.renameSync(cursorDir, backup);
      fs.writeFileSync(cursorDir, "blocks checkpoint mkdir");
      return result;
    };
    try {
      await expect(bridge.step()).rejects.toThrow(/EEXIST|ENOTDIR|not a directory/i);
      expect(observed.size).toBe(1);
      for (const seen of observed) expect(seen.has(sent.id)).toBe(true);
      const saved = JSON.parse(fs.readFileSync(path.join(backup, "cursor.json"), "utf8"));
      expect(saved[where === "local" ? "toLocal" : "toRemote"].after).toBeLessThan(sent.sequence);
    } finally {
      Set.prototype.add = add;
      side.publish = publish;
      if (fs.existsSync(backup)) {
        fs.rmSync(cursorDir, { force: true });
        fs.renameSync(backup, cursorDir);
      }
    }
  });

  it("wakes a capped lock backoff on stop and makes no further pass", async () => {
    let calls = 0;
    const { bridge, logs } = setup(undefined, {
      stopMs: 100,
      local: (store, peer) => {
        const side = new StoreBridgeSide(store, peer);
        side.latestSequence = async () => { calls++; throw lockTimeout(); };
        return side;
      },
    });
    const running = bridge.run();
    await waitFor(() => calls >= 4); // 100, 200, 400 ms backoffs; now waiting 800 ms
    expect(logs.filter(line => line.includes("retrying"))).toHaveLength(1);
    const before = calls;
    const started = Date.now();
    await bridge.stop();
    await running;
    expect(Date.now() - started).toBeLessThan(500);
    expect(calls).toBe(before);
  });

  it.each([new Error("Timed out waiting for the Fabric mesh lock"), Object.assign(new Error("unauthorized"), { code: "DENIED" })])(
    "fails unknown/security errors rather than retrying: %s", async (error) => {
      const { bridge, logs } = setup(undefined, {
        local: (store, peer) => {
          const side = new StoreBridgeSide(store, peer);
          side.latestSequence = async () => { throw error; };
          return side;
        },
      });
      await expect(bridge.run()).rejects.toBe(error);
      await bridge.stop();
      expect(logs.some((line) => line.includes("retrying"))).toBe(false);
    },
  );

  it("fails transport death after a remote lock timeout, without retrying a closed transport", async () => {
    let side!: StoreBridgeSide;
    const { bridge, remote, logs } = setup(undefined, {
      stopMs: 100,
      agent: (store) => {
        side = new StoreBridgeSide(store, "dev1");
        side.latestSequence = async () => { throw lockTimeout(); };
        return side;
      },
    });
    const running = bridge.run();
    const failed = expect(running).rejects.toThrow("transport died");
    await waitFor(() => logs.some((line) => line.includes("retrying")));
    remote.close(new Error("transport died"));
    await failed;
    await bridge.stop();
    expect(logs.filter((line) => line.includes("retrying"))).toHaveLength(1);
  });
  it("mirrors each side's live roots into the other, marked remoteHost, and withdraws them on stop", async () => {
    const { hub, far, bridge } = setup();
    const lane = await addRoot(hub, "lane");
    const forgeRoot = await addRoot(far, "forge-main");
    await bridge.start();
    await bridge.step();

    const mirroredHost = hub.get(`topology/hosts/${hash(forgeRoot.hostId)}`)!;
    expect(mirroredHost.value).toMatchObject({ id: forgeRoot.hostId, remoteHost: "forge" });
    expect(mirroredHost.updatedBy.id).toBe(forgeRoot.identity.id);
    expect((mirroredHost.value as { expiresAt: number }).expiresAt).toBeLessThanOrEqual(Date.now() + 15_000);
    expect(hub.get(`topology/participants/${hash(forgeRoot.identity.id)}`)!.value).toMatchObject({ kind: "root", remoteHost: "forge" });
    expect(readHostLeases(hub.root).get(forgeRoot.hostId)).toMatchObject({ identityId: forgeRoot.identity.id });
    expect(far.get(`topology/participants/${hash(lane.identity.id)}`)!.value).toMatchObject({ remoteHost: "dev1" });

    // A mirror is never mirrored back (no loops).
    await bridge.step();
    expect(far.get(`topology/hosts/${hash(forgeRoot.hostId)}`)!.value).not.toHaveProperty("remoteHost");

    await bridge.stop();
    expect(hub.get(`topology/hosts/${hash(forgeRoot.hostId)}`)).toBeUndefined();
    expect(hub.get(`topology/participants/${hash(forgeRoot.identity.id)}`)).toBeUndefined();
    expect(readHostLeases(hub.root).has(forgeRoot.hostId)).toBe(false);
    expect(far.get(`topology/participants/${hash(lane.identity.id)}`)).toBeUndefined();

    // A restarted bridge mirrors again over the deleted keys' tombstones.
    const again = new MeshBridge({
      localName: "dev1", remoteName: "forge", presenceMs: 0, cursorPath: path.join(scratch(), "c.json"),
      local: new StoreBridgeSide(hub, "forge"), remote: new StoreBridgeSide(far, "dev1"),
    });
    await again.step();
    expect(hub.get(`topology/hosts/${hash(forgeRoot.hostId)}`)!.value).toMatchObject({ remoteHost: "forge" });
    expect(hub.get(`topology/participants/${hash(forgeRoot.identity.id)}`)!.value).toMatchObject({ remoteHost: "forge" });
    expect(far.get(`topology/participants/${hash(lane.identity.id)}`)!.value).toMatchObject({ remoteHost: "dev1" });
  });

  it("drops a remote root whose lease lapsed, and never replaces a native record", async () => {
    const { hub, far, bridge } = setup();
    await addRoot(hub, "same");
    await addRoot(far, "same"); // collides with the hub's ids: a spoof, never mirrored
    const gone = await addRoot(far, "gone", -1);
    await bridge.start();
    await bridge.step();
    expect(hub.get(`topology/hosts/${hash(sid("same"))}`)!.value).not.toHaveProperty("remoteHost");
    expect(hub.get(`topology/hosts/${hash(gone.hostId)}`)).toBeUndefined();
  });

  it("carries a control command to a remote host and its ack back, sender kept and stamped", async () => {
    const { hub, far, bridge } = setup();
    const lane = await addRoot(hub, "lane");
    const forgeRoot = await addRoot(far, "forge-main");
    await bridge.start();

    const sent = await hub.publish({
      topic: "fabric.control.command", kind: "followUp", from: lane.identity, to: forgeRoot.hostId,
      data: command(forgeRoot.identity.id, lane.hostId),
    });
    expect(await bridge.step()).toMatchObject({ toRemote: 1 });
    const [arrived] = on(far, "fabric.control.command");
    expect(arrived).toMatchObject({ from: lane.identity, to: forgeRoot.hostId, kind: "followUp" });
    expect(arrived!.data).toMatchObject({ targetId: forgeRoot.identity.id, bridge: { from: "dev1", id: sent.id } });

    await far.publish({
      topic: "fabric.control.ack", kind: "ack", from: forgeRoot.identity, to: lane.hostId,
      data: { version: 1, commandId: "x", targetId: forgeRoot.identity.id, accepted: true },
    });
    expect(await bridge.step()).toMatchObject({ toLocal: 1, toRemote: 0 });
    const [ack] = on(hub, "fabric.control.ack");
    expect(ack).toMatchObject({ from: forgeRoot.identity, to: lane.hostId, data: { accepted: true, bridge: { from: "forge" } } });

    // Neither bridged copy crosses back.
    expect(await bridge.step()).toMatchObject({ toRemote: 0, toLocal: 0 });
    expect(on(far, "fabric.control.ack")).toHaveLength(1);
    expect(on(hub, "fabric.control.command")).toHaveLength(1);
  });

  it("carries work events and owner wakes to a remote root, and nothing off the allow-list", async () => {
    const { hub, far, bridge } = setup();
    const factory: MeshIdentity = { id: "factory-host:owner", name: "factory-owner", kind: "main" };
    await addRoot(hub, "lane");
    await addRoot(far, "forge-main");
    await bridge.start();
    await hub.publish({ topic: "fleet.work.smarty-dev.2004", kind: "ask", from: factory, to: sid("forge-main"), text: "w" });
    await hub.publish({ topic: "ops.owner", kind: "pr.wake", from: factory, to: sid("forge-main"), data: { rootId: sid("forge-main") } });
    await hub.publish({ topic: "ops.owner", kind: "drift", from: factory, to: sid("forge-main") });
    await hub.publish({ topic: "github.pull_request", kind: "opened", from: factory, to: sid("forge-main") });
    await hub.publish({ topic: "fleet.work.smarty-dev.1", kind: "ask", from: factory, to: "someone-local", text: "stays" });
    expect(await bridge.step()).toMatchObject({ toRemote: 2 });
    expect(far.read({ after: 0, limit: 100 }).map((e) => `${e.topic}/${e.kind}`))
      .toEqual(["fleet.work.smarty-dev.2004/ask", "ops.owner/pr.wake"]);
  });

  it("reads past more than one page of events it does not carry", async () => {
    const { hub, far, bridge } = setup();
    const lane = await addRoot(hub, "lane");
    const forgeRoot = await addRoot(far, "forge-main");
    await bridge.start();
    for (let index = 0; index < 250; index++) await hub.publish({ topic: "chatter", from: lane.identity, text: String(index) });
    await hub.publish({ topic: "fleet.work.x.1", kind: "ask", from: lane.identity, to: forgeRoot.identity.id, text: "last" });
    expect(await bridge.step()).toMatchObject({ toRemote: 1 });
    expect(on(far, "fleet.work.x.1")[0]!.text).toBe("last");
  });

  it("refuses remote events that spoof a hub identity or come from no live remote participant", async () => {
    const { hub, far, bridge, logs } = setup();
    const lane = await addRoot(hub, "lane");
    const forgeRoot = await addRoot(far, "forge-main");
    await bridge.start();
    await bridge.step();
    const ack = { version: 1, commandId: "x", targetId: "t", accepted: true };
    await far.publish({ topic: "fabric.control.ack", kind: "ack", from: lane.identity, to: lane.hostId, data: ack });
    await far.publish({ topic: "fabric.control.ack", kind: "ack", from: { id: "stranger", name: "s", kind: "main" }, to: lane.hostId, data: ack });
    await far.publish({ topic: "fabric.control.command", kind: "steer", from: forgeRoot.identity, to: lane.hostId, data: { bridge: { from: "x", id: "y" } } });
    expect(await bridge.step()).toMatchObject({ toLocal: 0, dropped: 2 }); // the stamped one is filtered at its source
    expect(on(hub, "fabric.control.command")).toHaveLength(0);
    expect(on(hub, "fabric.control.ack")).toHaveLength(0);
    expect(logs.join("\n")).toMatch(/claims a hub identity/);
    expect(logs.join("\n")).toMatch(/not a live participant of the remote/);
  });

  it("resumes from its cursor file without forwarding an event twice", async () => {
    const cursorPath = path.join(scratch(), "cursor.json");
    const first = setup(cursorPath);
    const lane = await addRoot(first.hub, "lane");
    const forgeRoot = await addRoot(first.far, "forge-main");
    await first.bridge.start();
    const before = fs.readFileSync(cursorPath, "utf8");
    const recovered = await first.hub.publish({ topic: "fabric.control.command", kind: "steer", from: lane.identity, to: forgeRoot.hostId, data: command(forgeRoot.identity.id, lane.hostId) });
    expect(await first.bridge.step()).toMatchObject({ toRemote: 1 });
    // A crash after the publish, before the cursor save: the cursor file is the older one.
    fs.writeFileSync(cursorPath, before);

    const toAgent = new PassThrough();
    const fromAgent = new PassThrough();
    void serveBridgeAgent(new StoreBridgeSide(first.far, "dev1"), toAgent, fromAgent);
    cleanups.push(() => toAgent.end());
    const second = new MeshBridge({
      localName: "dev1", remoteName: "forge", presenceMs: 0, cursorPath,
      local: new StoreBridgeSide(first.hub, "forge"), remote: new RemoteBridgeSide(fromAgent, toAgent),
    });
    let recoverySeen: Set<string> | undefined;
    const add = Set.prototype.add;
    Set.prototype.add = function (id) {
      if (id === recovered.id) recoverySeen = this;
      return add.call(this, id);
    };
    try {
      await second.start();
      expect(recoverySeen?.has(recovered.id)).toBe(true);
      expect(await second.step()).toMatchObject({ toRemote: 0 });
      expect(on(first.far, "fabric.control.command")).toHaveLength(1);
      const fresh = await first.hub.publish({ topic: "fabric.control.command", kind: "steer", from: lane.identity, to: forgeRoot.hostId, data: command(forgeRoot.identity.id, lane.hostId) });
      expect(await second.step()).toMatchObject({ toRemote: 1 });
      expect(recoverySeen?.has(recovered.id)).toBe(true); // No blanket recovery-set clear.
      expect(recoverySeen?.has(fresh.id)).toBe(false);
      expect(on(first.far, "fabric.control.command").map((event) => (event.data as { bridge: { id: string } }).bridge.id))
        .toEqual([recovered.id, fresh.id]);
    } finally {
      Set.prototype.add = add;
    }
  });

  it("the agent refuses writes off the allow-list, and stamps its pinned peer name", async () => {
    const { far, remote } = setup();
    const from: MeshIdentity = { id: "a", name: "a", kind: "main" };
    await expect(remote.publish({ topic: "github.push", kind: "x", from, to: "b", data: { bridge: { from: "dev1", id: "1" } } }))
      .rejects.toThrow(/not allowed/);
    await expect(remote.publish({ topic: "fleet.work.x", kind: "x", from, to: "b", data: {} as never })).rejects.toThrow(/bridge stamp/);
    await remote.publish({ topic: "fleet.work.x", kind: "x", from, to: "b", data: { bridge: { from: "evil", id: "1" } } });
    expect(far.read({ after: 0 })[0]!.data).toEqual({ bridge: { from: "dev1", id: "1" } });
  });
  // Security review F1: a claim the hub refuses gets no routing either.
  // smarty-dev#2004: under the participants-files policy a native root is only in its own file.
  it("exports a native root that is only in its file, and never mirrors a spoof over it", async () => {
    const { hub, far, bridge } = setup();
    const keyOf = (id: string) => `topology/participants/${hash(id)}`;
    const toFile = async (id: string) => {                      // its file, then off the shared state
      writeParticipantFile(hub.root, hub.get(keyOf(id))!);
      await hub.delete({ key: keyOf(id) });
    };
    const lane = await addRoot(hub, "lane");
    const org = await addRoot(hub, "org");
    await toFile(lane.identity.id);
    await toFile(org.identity.id);
    await addRoot(far, "thief", 15_000, { id: org.identity.id, hostId: sid("thief") });   // claims org's id
    await bridge.start();
    await bridge.step();
    expect(far.get(keyOf(lane.identity.id))!.value).toMatchObject({ id: lane.identity.id, remoteHost: "dev1" });
    expect(hub.get(keyOf(org.identity.id))).toBeUndefined();     // no mirror at a native's key
    expect(hub.get(`topology/hosts/${hash(sid("thief"))}`)).toBeUndefined();
    expect(readParticipantFiles(hub.root, { maxAgeMs: 0 }).map((item) => item.key).sort())
      .toEqual([keyOf(lane.identity.id), keyOf(org.identity.id)].sort());
  });

  // Security pass S3 on #142: an unreadable or invalid native file still guards its key.
  it("never mirrors over a native participant file it cannot read", async () => {
    const { hub, far, bridge } = setup();
    const forge = await addRoot(far, "forge-main");
    const key = `topology/participants/${hash(forge.identity.id)}`;
    fs.mkdirSync(path.join(hub.root, "participants"), { recursive: true });
    fs.writeFileSync(path.join(hub.root, "participants", `${hash(forge.identity.id)}.json`), "{ not json");
    await bridge.start();
    await bridge.step();
    expect(hub.get(key)).toBeUndefined();
  });

  it("exports nothing to a remote host, root or alias that collides with a hub record", async () => {
    const { hub, far, bridge } = setup();
    const lane = await addRoot(hub, "lane");
    const org = await addRoot(hub, "org");
    // The remote claims the hub's host id, the hub root's label and session id, and a name.
    await addRoot(far, "thief", 15_000, { hostId: lane.hostId, id: sid("thief") });
    await addRoot(far, "alias", 15_000, { label: "ORG", sessionId: "org" });
    // A hub Main whose session name is "fabric-v2" is in no record: the remote claims it as label and session id.
    await addRoot(far, "fabric-v2", 15_000, { sessionId: "fabric-v2", label: "fabric-v2" });
    await bridge.start();
    await bridge.step();
    await hub.publish({ topic: "fabric.control.command", kind: "steer", from: org.identity, to: lane.hostId, data: command(lane.identity.id, org.hostId) });
    await hub.publish({ topic: "fleet.work.x.1", kind: "ask", from: lane.identity, to: "ORG", text: "local" });
    await hub.publish({ topic: "fleet.work.x.2", kind: "ask", from: lane.identity, to: "org", text: "local" });
    await hub.publish({ topic: "fleet.work.x.3", kind: "ask", from: lane.identity, to: "fabric-v2", text: "a name, not an address" });
    await hub.publish({ topic: "fleet.work.x.4", kind: "ask", from: lane.identity, to: sid("fabric-v2"), text: "crosses" });
    await bridge.step();
    expect(far.read({ after: 0, limit: 100 }).map((e) => e.topic)).toEqual(["fleet.work.x.4"]);
    expect(hub.get(`topology/participants/${hash(sid("alias"))}`)).toBeUndefined();
    expect(hub.get(`topology/participants/${hash(sid("thief"))}`)).toBeUndefined();
  });

  // Security review F2: identities are bound to the link that mirrored them first.
  it("refuses messages and acks from one remote that claim another remote's identity", async () => {
    const hub = new MeshStore(scratch(), 64 * 1024, 100);
    const lane = await addRoot(hub, "lane");
    const a = setup(undefined, { hub, remoteName: "forge" });
    const b = setup(undefined, { hub, remoteName: "ryzen2" });
    const aRoot = await addRoot(a.far, "a");
    const bRoot = await addRoot(b.far, "b");
    await a.bridge.start();
    await a.bridge.step();
    expect(hub.get(`topology/participants/${hash(aRoot.identity.id)}`)!.value).toMatchObject({ remoteHost: "forge" });
    // B advertises A's root as its own native, then speaks as it.
    await addRoot(b.far, "a");
    await b.bridge.start();
    await b.bridge.step();
    expect(hub.get(`topology/participants/${hash(aRoot.identity.id)}`)!.value).toMatchObject({ remoteHost: "forge" });
    const ack = { version: 1, commandId: "c", targetId: aRoot.identity.id, accepted: true, result: "forged" };
    await b.far.publish({ topic: "fabric.control.ack", kind: "ack", from: aRoot.identity, to: lane.hostId, data: ack });
    await b.far.publish({ topic: "fleet.work.x.1", kind: "ask", from: aRoot.identity, to: lane.identity.id, text: "as a" });
    // Its own root may not ack for A's either.
    await b.far.publish({ topic: "fabric.control.ack", kind: "ack", from: bRoot.identity, to: lane.hostId, data: ack });
    expect(await b.bridge.step()).toMatchObject({ toLocal: 0, dropped: 3 });
    expect(b.logs.join("\n")).toMatch(/ack target is not bound to this link/);
    // B's own root still works.
    await b.far.publish({ topic: "fleet.work.x.2", kind: "ask", from: bRoot.identity, to: lane.identity.id, text: "as b" });
    expect(await b.bridge.step()).toMatchObject({ toLocal: 1 });
    expect(hub.read({ after: 0, limit: 100 }).map((e) => `${e.topic}:${e.text}`)).toEqual(["fleet.work.x.2:as b"]);
  });

  // Security review F3: a silent remote cannot hold a stop or the loop.
  it("stops within a bound and withdraws local mirrors when the remote stops answering", async () => {
    const { hub, far, bridge, gate, remote } = setup(undefined, { callTimeoutMs: 60_000, stopMs: 300 });
    const forgeRoot = await addRoot(far, "forge-main");
    await bridge.start();
    await bridge.step();
    expect(hub.get(`topology/participants/${hash(forgeRoot.identity.id)}`)).toBeDefined();
    gate.silent = true;
    const pass = bridge.step().catch((error: Error) => error);
    await new Promise((resolve) => setTimeout(resolve, 50));
    const started = Date.now();
    await bridge.stop();
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(hub.get(`topology/participants/${hash(forgeRoot.identity.id)}`)).toBeUndefined();
    expect(hub.get(`topology/hosts/${hash(forgeRoot.hostId)}`)).toBeUndefined();
    await pass;
    await expect(remote.latestSequence()).rejects.toThrow(/stopped|closed/);
    // The pass in flight did not put the mirror back.
    expect(hub.get(`topology/participants/${hash(forgeRoot.identity.id)}`)).toBeUndefined();
  });

  it("closes the transport when a call passes its deadline", async () => {
    const { far, bridge, gate, remote } = setup(undefined, { callTimeoutMs: 200 });
    await addRoot(far, "forge-main");
    await bridge.start();
    gate.silent = true;
    await expect(bridge.step()).rejects.toThrow(/did not answer .* within 200 ms/);
    await expect(remote.closed).resolves.toBeInstanceOf(Error);
  });

  it("the CLI ends a silent transport child within its bounds, on a deadline and on abort", async () => {
    const hub = scratch();
    const silent = [process.execPath, "-e", "setInterval(() => {}, 1e6)"];
    const flags = (timeout: number) => new Map([
      ["mesh", hub], ["name", "dev1"], ["remote", "forge"], ["cursor", path.join(scratch(), "c.json")],
      ["call-timeout-ms", String(timeout)],
    ]);
    const alive = (pid: number): boolean => {
      try {
        process.kill(pid, 0);
        return true;
      } catch {
        return false;
      }
    };
    for (const [timeout, abortAfter, expected] of [[300, undefined, 1], [1_000, 100, 0]] as const) {
      const controller = new AbortController();
      if (abortAfter) setTimeout(() => controller.abort(), abortAfter);
      let pid: number | undefined;
      const started = Date.now();
      await expect(runBridge(flags(timeout), silent, controller.signal, (child) => (pid = child))).resolves.toBe(expected);
      expect(Date.now() - started).toBeLessThan(timeout * 3 + 3_000);
      expect(alive(pid!)).toBe(false);
    }
  }, 20_000);

  // Review F4: a page never passes one transport frame, over a real pipe.
  it("delivers, in order, a backlog larger than one transport frame over a real pipe", async () => {
    const { hub, far, bridge, logs } = setup(undefined, { maxEventBytes: 300 * 1024, realPipe: true });
    // The backlog takes a while to write on a busy host: leases outlast it.
    const lane = await addRoot(hub, "lane", 300_000);
    const forgeRoot = await addRoot(far, "forge-main", 300_000);
    await bridge.start();
    await bridge.step();
    const text = "x".repeat(240 * 1024);
    for (let index = 0; index < 80; index++) {
      await far.publish({ topic: `fleet.work.x.${index}`, kind: "ask", from: forgeRoot.identity, to: lane.identity.id, text });
    }
    expect(await bridge.step()).toMatchObject({ toLocal: 80, dropped: 0 });
    expect(hub.read({ after: 0, limit: 100 }).map((e) => e.topic)).toEqual(Array.from({ length: 80 }, (_, i) => `fleet.work.x.${i}`));
    expect(logs).toEqual([]);
  }, 60_000);

  it("skips an event too large for one frame with evidence, and moves on", async () => {
    const far = new MeshStore(scratch(), 64 * 1024, 100);
    const from: MeshIdentity = { id: "session:f", name: "main", kind: "main" };
    const big = await far.publish({ topic: "fleet.work.big", kind: "ask", from, to: "x", text: "y".repeat(4_000) });
    await far.publish({ topic: "fleet.work.small", kind: "ask", from, to: "x", text: "z" });
    const page = await new StoreBridgeSide(far, "dev1").read(0, 2_000);
    expect(page.skipped).toEqual([{ id: big.id, sequence: big.sequence, topic: "fleet.work.big", bytes: expect.any(Number) }]);
    expect(page.events.map((e) => e.topic)).toEqual(["fleet.work.small"]);
    expect(page.through).toBe(2);
  });
  // Security and review round 2, F1/F2: a claim refused or lost in this very pass is never trusted.
  it("gives a claim lost while its presence reply was held no traffic and no voice, in the same pass", async () => {
    for (const winner of ["peer", "native"] as const) {
      const hub = new MeshStore(scratch(), 64 * 1024, 100);
      const lane = await addRoot(hub, "lane");
      const a = setup(undefined, { hub, remoteName: "forge" });
      let release!: () => void;
      const held = new Promise<void>((resolve) => (release = resolve));
      let asked!: () => void;
      const presenceAsked = new Promise<void>((resolve) => (asked = resolve));
      const b = setup(undefined, {
        hub, remoteName: "ryzen2",
        wrapRemote: (remote) => ({
          latestSequence: () => remote.latestSequence(),
          read: (after) => remote.read(after),
          publish: (event) => remote.publish(event),
          mirror: (presence) => remote.mirror(presence),
          bridgedIds: (after) => remote.bridgedIds(after),
          close: (error) => remote.close(error),
          presence: async () => {
            asked();
            await held;
            return remote.presence();
          },
        }),
      });
      await b.bridge.start();
      await addRoot(b.far, "x"); // B claims X ...
      const pass = b.bridge.step();
      await presenceAsked; // ... B has read the hub; X is still free.
      if (winner === "peer") {
        await addRoot(a.far, "x");
        await a.bridge.start();
        await a.bridge.step();
      } else {
        await addRoot(hub, "x");
      }
      const x = sid("x");
      await hub.publish({ topic: "fabric.control.command", kind: "steer", from: lane.identity, to: x, data: command(x, lane.hostId) });
      await b.far.publish({ topic: "fabric.control.ack", kind: "ack", from: { id: x, name: "main", kind: "main" }, to: lane.hostId, data: { version: 1, commandId: "c", targetId: x, accepted: true } });
      await b.far.publish({ topic: "fleet.work.x.1", kind: "ask", from: { id: x, name: "main", kind: "main" }, to: lane.identity.id, text: "as x" });
      release();
      expect(await pass).toMatchObject({ toRemote: 0, toLocal: 0 });
      expect(b.far.read({ after: 0, limit: 100 }).filter((e) => e.topic === "fabric.control.command")).toHaveLength(0);
      expect(hub.read({ after: 0, limit: 100 }).filter((e) => e.topic !== "fabric.control.command")).toHaveLength(0);
      expect(hub.get(`topology/participants/${hash(x)}`)!.value).toMatchObject(winner === "peer" ? { remoteHost: "forge" } : {});
      if (winner === "native") expect(hub.get(`topology/participants/${hash(x)}`)!.value).not.toHaveProperty("remoteHost");
    }
  });

  // Security review round 2, F3: no mirror write in flight outlives the withdrawal.
  it("restores no record or lease when a suspended local mirror write resumes after stop", async () => {
    const hub = new MeshStore(scratch(), 64 * 1024, 100);
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    let entered!: () => void;
    const inPut = new Promise<void>((resolve) => (entered = resolve));
    const put = hub.put.bind(hub);
    let suspend = true;
    hub.put = async (input) => {
      if (suspend) {
        suspend = false;
        entered();
        await held;
      }
      return put(input);
    };
    const { far, bridge, remote } = setup(undefined, { hub, stopMs: 200 });
    const forgeRoot = await addRoot(far, "forge-main");
    await bridge.start();
    const pass = bridge.step().catch((error: Error) => error);
    await inPut;
    remote.close(new Error("transport failed"));
    const stopped = bridge.stop();
    await new Promise((resolve) => setTimeout(resolve, 300));
    release();
    await stopped;
    await pass;
    expect(hub.get(`topology/hosts/${hash(forgeRoot.hostId)}`)).toBeUndefined();
    expect(hub.get(`topology/participants/${hash(forgeRoot.identity.id)}`)).toBeUndefined();
    expect(readHostLeases(hub.root).has(forgeRoot.hostId)).toBe(false);
  });

  // Security review round 2, F4: presence both ways stays inside one frame, over a real pipe.
  it("keeps presence replies and mirror requests inside one frame over a real pipe", async () => {
    // Nine 1.9 MiB roots a side: about 17 MiB of presence, over the 16 MiB frame.
    const { hub, far, bridge, remote, logs } = setup(undefined, { maxEventBytes: 2 * 1024 * 1024, realPipe: true, callTimeoutMs: 240_000 });
    const cwd = "c".repeat(1_900 * 1024);
    for (let index = 0; index < 9; index++) {
      await addRoot(far, `far${index}`, 60_000, { cwd });
      await addRoot(hub, `hub${index}`, 60_000, { cwd });
    }
    await bridge.start();
    await bridge.step();
    const mirrored = (store: MeshStore) => store.listAll("topology/participants/").filter((e) => (e.value as { remoteHost?: string }).remoteHost).length;
    expect(mirrored(hub)).toBeGreaterThan(0);
    expect(mirrored(hub)).toBeLessThan(9);
    expect(mirrored(far)).toBeGreaterThan(0);
    expect(mirrored(far)).toBeLessThan(9);
    expect(logs.join("\n")).toMatch(/hosts pass the frame budget/);
    await expect(remote.latestSequence()).resolves.toBeTypeOf("number");
  }, 300_000);

  // Review round 2, F5: a transport that cannot start fails the bridge by name, within a bound.
  it("fails with a named error when the transport cannot start", async () => {
    const notExecutable = path.join(scratch(), "not-executable");
    fs.writeFileSync(notExecutable, "#!/bin/sh\n", { mode: 0o600 });
    const flags = new Map([
      ["mesh", scratch()], ["name", "dev1"], ["remote", "forge"], ["cursor", path.join(scratch(), "c.json")],
      ["call-timeout-ms", "60000"],
    ]);
    for (const command of [[path.join(scratch(), "no-such-ssh")], ...(process.platform === "win32" ? [] : [[notExecutable]])]) {
      const started = Date.now();
      await expect(runBridge(flags, command, new AbortController().signal)).resolves.toBe(1);
      expect(Date.now() - started).toBeLessThan(10_000);
    }
  }, 30_000);
  // Security round 3, F1: a hub session name never answers to a bridged id.
  it("keeps a hub Main whose session name looks like a remote id from taking that id's traffic", async () => {
    const { hub, far, bridge } = setup();
    const main = await addRoot(hub, "hubmain");
    const evil = await addRoot(far, "evil");
    await bridge.start();
    await bridge.step();
    // The hub Main's session name is the remote's canonical id.
    const inbox = new RootInbox(hub, main.identity, () => [main.identity.id, evil.identity.id], { steerGraceMs: 0 });
    const idle = { holdsBatch: () => false, holdsSteer: () => false };
    await inbox.next(idle);
    await hub.publish({ topic: "fleet.work.x.1", kind: "ask", from: main.identity, to: evil.identity.id, text: "to the remote id" });
    await hub.publish({ topic: "fleet.work.x.2", kind: "ask", from: evil.identity, to: main.identity.id, text: "to the hub id" });
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect((await inbox.next(idle)).events.map((e) => e.text)).toEqual(["to the hub id"]);
    await bridge.step();
    expect(far.read({ after: 0, limit: 100 }).map((e) => e.text)).toEqual(["to the remote id"]);
  });

  // Security round 3, F2: ownership is checked for each event at commit, not once per page.
  it("refuses an inbound event whose sender a native took over while the page was being published", async () => {
    const hub = new MeshStore(scratch(), 64 * 1024, 100);
    const lane = await addRoot(hub, "lane");
    const { far, bridge, logs } = setup(undefined, { hub });
    const y = await addRoot(far, "y");
    const x = await addRoot(far, "x");
    await bridge.start();
    await bridge.step();
    expect(hub.get(`topology/participants/${hash(x.identity.id)}`)!.value).toMatchObject({ remoteHost: "forge" });
    await far.publish({ topic: "fleet.work.x.1", kind: "ask", from: y.identity, to: lane.identity.id, text: "from y" });
    await far.publish({ topic: "fleet.work.x.2", kind: "ask", from: x.identity, to: lane.identity.id, text: "from x" });
    // Hold the first hub publish; a native X registers meanwhile.
    const publish = hub.publish.bind(hub);
    let first = true;
    hub.publish = async (input) => {
      if (first) {
        first = false;
        await addRoot(hub, "x");
      }
      return publish(input);
    };
    await bridge.step();
    expect(hub.read({ after: 0, limit: 100 }).map((e) => e.text)).toEqual(["from y"]);
    expect(hub.get(`topology/participants/${hash(x.identity.id)}`)!.value).not.toHaveProperty("remoteHost");
    expect(hub.get(`topology/hosts/${hash(x.hostId)}`)!.value).not.toHaveProperty("remoteHost");
    expect(logs.join("\n")).toMatch(/no longer bound to bridge link forge/);
  });

  // Security round 3, F5: a setup failure never leaves a transport child behind.
  it("checks its setup before it starts the transport", async () => {
    const silent = [process.execPath, "-e", "setInterval(() => {}, 1e6)"];
    const meshFile = path.join(scratch(), "a-file");
    fs.writeFileSync(meshFile, "");
    const base = { mesh: scratch(), name: "dev1", remote: "forge", cursor: path.join(scratch(), "c.json") };
    const cases: Array<[Record<string, string>, RegExp]> = [
      [{ ...base, cursor: "" }, /--cursor is required/],
      [{ ...base, remote: "dev1" }, /names must differ/],
      [{ ...base, mesh: path.join(meshFile, "mesh") }, /ENOTDIR|EEXIST|not a directory/i],
    ];
    for (const [flags, error] of cases) {
      const spawned: Array<number | undefined> = [];
      const started = Date.now();
      await expect(runBridge(new Map(Object.entries(flags)), silent, new AbortController().signal, (pid) => spawned.push(pid)))
        .rejects.toThrow(error);
      expect(spawned).toEqual([]);
      expect(Date.now() - started).toBeLessThan(5_000);
    }
  });
  // Security round 4, F2: one id names one owner, and any other owner denies at commit.
  it("refuses a remote root whose host splits its id from the root's", async () => {
    const { hub, far, bridge } = setup();
    const h = sid("h");
    await addRoot(far, "x", 15_000, { hostId: h });
    await bridge.start();
    await bridge.step();
    expect(hub.get(`topology/hosts/${hash(h)}`)).toBeUndefined();
    expect(hub.get(`topology/participants/${hash(sid("x"))}`)).toBeUndefined();
  });

  it("denies at commit an id a native Main registered, even when an old mirror of it survives", async () => {
    const hub = new MeshStore(scratch(), 64 * 1024, 1_000);
    const side = new StoreBridgeSide(hub, "forge");
    const x = sid("x");
    const h = sid("h");
    // Mirrors an older bridge wrote in the split layout: host H owns root X.
    const now = Date.now();
    const hIdentity: MeshIdentity = { id: h, name: "main", kind: "main", sessionId: "h" };
    await hub.put({ key: `topology/hosts/${hash(h)}`, identity: hIdentity, value: { format: 1, id: h, rootId: x, identity: hIdentity, startedAt: now, updatedAt: now, expiresAt: now + 60_000, remoteHost: "forge" } });
    await hub.put({ key: `topology/participants/${hash(x)}`, identity: hIdentity, value: {
      format: 1, id: x, kind: "root", rootId: x, ownerHostId: h, ownerIdentityId: h, name: "main", status: "idle", runner: "pi",
      transport: "host", capabilities: ["steer"], sessionId: "x", startedAt: now, updatedAt: now, controlProtocol: "v1", remoteHost: "forge",
    } });
    // A native Main X registers through the real directory.
    const native: MeshIdentity = { id: x, name: "main", kind: "main", sessionId: "x" };
    const directory = new ParticipantDirectory(hub, { enabled: true, hostId: x, rootId: x, identity: native, heartbeatMs: 100, leaseMs: 5_000, reapDeadHosts: false });
    await directory.start();
    cleanups.push(() => void directory.close());
    expect(hub.get(`topology/hosts/${hash(x)}`)!.value).not.toHaveProperty("remoteHost");
    const event = { topic: "fleet.work.x.1", kind: "ask", from: native, to: sid("lane"), text: "forged", data: { bridge: { from: "forge", id: "e1" } } };
    await expect(side.publish(event, [x])).rejects.toThrow(/no longer bound to bridge link forge/);
    expect(hub.read({ after: 0, limit: 100 }).filter((e) => e.topic === "fleet.work.x.1")).toEqual([]);
  });

  it("builds the ssh transport from explicit flags and refuses an option as the host", () => {
    const argv = transportCommand(new Map([["ssh", "forge"], ["ssh-key", "/k"], ["ssh-port", "2222"], ["ssh-known-hosts", "/kh"]]), []);
    expect(argv.slice(-3)).toEqual(["forge", "mesh-bridge", "agent"]);
    expect(argv).toEqual(expect.arrayContaining(["-p", "2222", "-i", "/k", "UserKnownHostsFile=/kh", "StrictHostKeyChecking=yes"]));
    expect(() => transportCommand(new Map([["ssh", "-oProxyCommand=x"]]), [])).toThrow(/a host, not an option/);
    expect(() => transportCommand(new Map([["ssh", "forge"], ["ssh-port", "22 -oX"]]), [])).toThrow(/port number/);
  });
});
