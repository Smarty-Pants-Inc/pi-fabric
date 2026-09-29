import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import {
  MeshBridge,
  RemoteBridgeSide,
  serveBridgeAgent,
  StoreBridgeSide,
} from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { runBridge } from "../src/mesh-bridge.js";
import { readHostLeases } from "../src/topology/host-leases.js";

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
const addRoot = async (
  store: MeshStore, name: string, expiresIn = 15_000,
  claims: { hostId?: string; id?: string; label?: string; sessionId?: string } = {},
) => {
  const now = Date.now();
  const hostId = claims.hostId ?? `host:${name}`;
  const sessionId = claims.sessionId ?? name;
  const identity: MeshIdentity = { id: claims.id ?? `session:${name}`, name: "main", kind: "main", sessionId };
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
      sessionId, startedAt: now, updatedAt: now, controlProtocol: "v1",
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
  void serveBridgeAgent(new StoreBridgeSide(far, "dev1"), toAgent, fromAgent);
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
    local: new StoreBridgeSide(hub, remoteName), remote,
    cursorPath: cursorPath ?? path.join(scratch(), "cursor.json"),
    presenceMs: 0,
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

describe("mesh bridge", () => {
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
    expect(hub.get(`topology/hosts/${hash("host:same")}`)!.value).not.toHaveProperty("remoteHost");
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
    await hub.publish({ topic: "fleet.work.smarty-dev.2004", kind: "ask", from: factory, to: "FORGE-MAIN", text: "w" });
    await hub.publish({ topic: "ops.owner", kind: "pr.wake", from: factory, to: "session:forge-main", data: { rootId: "session:forge-main" } });
    await hub.publish({ topic: "ops.owner", kind: "drift", from: factory, to: "session:forge-main" });
    await hub.publish({ topic: "github.pull_request", kind: "opened", from: factory, to: "session:forge-main" });
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
    await first.hub.publish({ topic: "fabric.control.command", kind: "steer", from: lane.identity, to: forgeRoot.hostId, data: command(forgeRoot.identity.id, lane.hostId) });
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
    await second.start();
    expect(await second.step()).toMatchObject({ toRemote: 0 });
    expect(on(first.far, "fabric.control.command")).toHaveLength(1);
    await first.hub.publish({ topic: "fabric.control.command", kind: "steer", from: lane.identity, to: forgeRoot.hostId, data: command(forgeRoot.identity.id, lane.hostId) });
    expect(await second.step()).toMatchObject({ toRemote: 1 });
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
  it("exports nothing to a remote host, root or alias that collides with a hub record", async () => {
    const { hub, far, bridge } = setup();
    const lane = await addRoot(hub, "lane");
    const org = await addRoot(hub, "org");
    // The remote claims the hub's host id, the hub root's label and session id, and a name.
    await addRoot(far, "thief", 15_000, { hostId: lane.hostId, id: "session:thief" });
    await addRoot(far, "alias", 15_000, { label: "ORG", sessionId: "org" });
    await addRoot(far, "fabric-v2", 15_000, { sessionId: "0f2a", label: "FV-1" });
    await bridge.start();
    await bridge.step();
    await hub.publish({ topic: "fabric.control.command", kind: "steer", from: org.identity, to: lane.hostId, data: command(lane.identity.id, org.hostId) });
    await hub.publish({ topic: "fleet.work.x.1", kind: "ask", from: lane.identity, to: "ORG", text: "local" });
    await hub.publish({ topic: "fleet.work.x.2", kind: "ask", from: lane.identity, to: "org", text: "local" });
    await hub.publish({ topic: "fleet.work.x.3", kind: "ask", from: lane.identity, to: "fabric-v2", text: "a name, not an address" });
    await hub.publish({ topic: "fleet.work.x.4", kind: "ask", from: lane.identity, to: "session:fabric-v2", text: "crosses" });
    await bridge.step();
    expect(far.read({ after: 0, limit: 100 }).map((e) => e.topic)).toEqual(["fleet.work.x.4"]);
    expect(hub.get(`topology/participants/${hash("session:alias")}`)).toBeUndefined();
    expect(hub.get(`topology/participants/${hash("session:thief")}`)).toBeUndefined();
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
    const lane = await addRoot(hub, "lane");
    const forgeRoot = await addRoot(far, "forge-main");
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
});
