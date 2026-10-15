// smarty-dev#9349: a work-host Main's durable WAIT/ETA record is published unaddressed (no `to`) on its
// parsed `fleet.work.<repo>.<n>` topic, and the hub factory (setup/factory/mesh_answers.py, KINDS
// = ('wait', 'eta')) reads it by topic. The bridge dropped it as "not addressed across", so the
// factory never saw the renewal. Only an unaddressed wait/eta on a parsed work topic, spoke to hub,
// from a live spoke participant gains the factory exception; the hub copy names `to: "factory"`.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { MeshBridge, type MeshBridgeOptions, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const TOPIC = "fleet.work.smarty-dev.1";
// Long private fixture lease: a loaded host must not expire the participants mid-test.
const LEASE_MS = 10 * 60_000;
// Registered durable record texts, as a work-host Main publishes them (not free text).
const WAIT_TEXT = "WAIT: hub review of the bridge fix (owner architecture-lead) next-check ~00:10Z";
const ETA_TEXT = "ETA ~00:10Z";
const RENEWAL_TEXT = "WAIT: hub review of the bridge fix (owner architecture-lead) next-check ~00:40Z";

const roots: string[] = [];
const cleanups: Array<() => unknown> = [];

// Owned cleanup, run at most once (an explicit early call is the same one), LIFO in afterEach: a bridge
// stops (its fenced remote withdrawal answered) before the agent pipe it owns ends, so no stop bound is hit.
const own = (cleanup: () => unknown): (() => Promise<void>) => {
  let done: Promise<void> | undefined;
  const run = () => (done ??= Promise.resolve().then(cleanup).then(() => undefined));
  cleanups.push(run);
  return run;
};

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-bridge-9349-"));
  roots.push(root);
  return root;
};

const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const sid = (name: string): string => `session:${name}-00000000`;

/** A live native root session, as the participant directory writes it (same shape as mesh-bridge.test.ts). */
const addRoot = async (store: MeshStore, name: string): Promise<{ hostId: string; identity: MeshIdentity }> => {
  const now = Date.now();
  const hostId = sid(name);
  const identity: MeshIdentity = { id: hostId, name: "main", kind: "main", sessionId: name };
  await store.put({
    key: `topology/hosts/${hash(hostId)}`,
    identity,
    value: { format: 1, id: hostId, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + LEASE_MS },
  });
  await store.put({
    key: `topology/participants/${hash(identity.id)}`,
    identity,
    value: {
      format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: hostId, ownerIdentityId: identity.id,
      ownerIncarnation: `fixture:${hostId}`, name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
      capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
    },
  });
  return { hostId, identity };
};

/** A real bridge: the hub's store side in process, the spoke's agent over a stdio pipe pair. */
type Tuning = Pick<MeshBridgeOptions, "clock" | "factoryRate">;
const link = (hub: MeshStore, far: MeshStore, cursorPath: string, tuning: Tuning = {}) => {
  const toAgent = new PassThrough();
  const fromAgent = new PassThrough();
  void serveBridgeAgent(new StoreBridgeSide(far, "dev1"), toAgent, fromAgent);
  const endAgent = own(() => toAgent.end());
  const logs: string[] = [];
  const bridge = new MeshBridge({
    localName: "dev1", remoteName: "forge", presenceMs: 0, cursorPath, ...tuning,
    local: new StoreBridgeSide(hub, "forge"), remote: new RemoteBridgeSide(fromAgent, toAgent),
    log: (message) => logs.push(message),
  });
  const stopBridge = own(() => bridge.stop()); // pushed after endAgent: LIFO stops the bridge first
  /** The owned teardown in fence order (bridge, then its agent pipe); afterEach then skips both. */
  const close = async () => { await stopBridge(); await endAgent(); };
  return { bridge, logs, close };
};

const setup = async (tuning: Tuning = {}) => {
  const hub = new MeshStore(scratch(), 64 * 1024, 100);
  const far = new MeshStore(scratch(), 64 * 1024, 100);
  const cursorPath = path.join(scratch(), "cursor.json");
  const lane = await addRoot(hub, "lane");
  const spoke = await addRoot(far, "forge-main");
  const { bridge, logs, close } = link(hub, far, cursorPath, tuning);
  await bridge.start();
  await bridge.step();
  return { hub, far, cursorPath, lane, spoke, bridge, logs, close };
};

const on = (store: MeshStore, topic: string): MeshEvent[] => store.read({ after: 0, limit: 100 }).filter((event) => event.topic === topic);
const bridgedIds = (store: MeshStore, topic: string): string[] =>
  on(store, topic).map((event) => (event.data as { bridge?: { id?: string } } | undefined)?.bridge?.id ?? "(native)");

describe("unaddressed durable WAIT/ETA records reach the hub factory (smarty-dev#9349)", () => {
  // Both real MeshProvider.publish inputs (no `to`, and the `factory` role address) walk the public
  // provider entry, the spoke store, the real bridge source and land in the hub's durable log.
  it.each([undefined, "factory"] as const)("carries a provider-published wait and eta (to=%s) on a parsed work topic exactly once, batched", async (to) => {
    const { hub, far, bridge, spoke } = await setup();
    const { hostId, identity } = spoke;
    const provider = new MeshProvider(far, identity, new ParticipantDirectory(far, { enabled: true, hostId, rootId: identity.id, identity }));
    const context = {} as FabricInvocationContext;
    const address = to === undefined ? {} : { to }; // the omitted-key form, as a caller sends it
    const wait = await provider.invoke("publish", { topic: TOPIC, kind: "wait", ...address, text: WAIT_TEXT }, context) as MeshEvent;
    const eta = await provider.invoke("publish", { topic: TOPIC, kind: "eta", ...address, text: ETA_TEXT }, context) as MeshEvent;
    expect(wait.to).toBe(to);
    expect(wait.from).toMatchObject({ id: identity.id });
    // Two contiguous admitted events: the batch path.
    expect(await bridge.step()).toMatchObject({ toLocal: 2, toRemote: 0, dropped: 0 });
    expect(on(hub, TOPIC)).toEqual([
      expect.objectContaining({ kind: "wait", to: "factory", text: WAIT_TEXT,
        from: expect.objectContaining({ id: spoke.identity.id, verified: "bridge" }), data: { bridge: { from: "forge", id: wait.id } } }),
      expect.objectContaining({ kind: "eta", to: "factory", text: ETA_TEXT,
        from: expect.objectContaining({ id: spoke.identity.id }), data: { bridge: { from: "forge", id: eta.id } } }),
    ]);
    expect(await bridge.step()).toMatchObject({ toLocal: 0 });
    expect(bridgedIds(hub, TOPIC)).toEqual([wait.id, eta.id]);
  });

  it("carries a lone unaddressed wait, then a lone eta, exactly once each (singleton path)", async () => {
    const { hub, far, bridge, spoke } = await setup();
    const wait = await far.publish({ topic: TOPIC, kind: "wait", from: spoke.identity, text: WAIT_TEXT });
    expect(await bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    const eta = await far.publish({ topic: TOPIC, kind: "eta", from: spoke.identity, text: ETA_TEXT });
    expect(await bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    expect(await bridge.step()).toMatchObject({ toLocal: 0 });
    expect(on(hub, TOPIC).map((event) => [event.kind, event.to, event.text])).toEqual([["wait", "factory", WAIT_TEXT], ["eta", "factory", ETA_TEXT]]);
    expect(bridgedIds(hub, TOPIC)).toEqual([wait.id, eta.id]);
  });

  it("keeps other unaddressed kinds, other recipients and hub-to-spoke records where they are", async () => {
    const { hub, far, bridge, lane, spoke } = await setup();
    for (const kind of ["progress", "ack", "ask"]) await far.publish({ topic: TOPIC, kind, from: spoke.identity, text: kind });
    await far.publish({ topic: TOPIC, kind: "wait", from: spoke.identity, to: "nobody", text: "explicit unrelated recipient" });
    await hub.publish({ topic: "fleet.work.hub.1", kind: "wait", from: lane.identity, text: "hub to spoke" });
    expect(await bridge.step()).toMatchObject({ toLocal: 0, toRemote: 0 });
    expect(on(hub, TOPIC)).toEqual([]);
    expect(on(far, "fleet.work.hub.1")).toEqual([]);
  });

  // Rate only the factory exception: an event bucket of one, or a one-byte byte bucket (an event bigger
  // than the burst crosses on a full bucket and leaves a debt). Either way the eta is deferred, not dropped.
  it.each([
    ["event", { perMinute: 1, burst: 1 }],
    ["byte", { perMinute: 600, burst: 100, bytesPerMinute: 60_000, byteBurst: 1 }],
  ] as const)("spends the factory %s bucket on unaddressed wait/eta, deferring the eta until refill, each id once", async (_bucket, factoryRate) => {
    let now = 5_000_000;
    const { hub, far, cursorPath, bridge, spoke, logs } = await setup({ clock: () => now, factoryRate });
    const toLocalAfter = (): number => (JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toLocal: { after: number } }).toLocal.after;
    const wait = await far.publish({ topic: TOPIC, kind: "wait", from: spoke.identity, text: WAIT_TEXT });
    const eta = await far.publish({ topic: TOPIC, kind: "eta", from: spoke.identity, text: ETA_TEXT });
    expect(await bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    expect(bridgedIds(hub, TOPIC)).toEqual([wait.id]);
    expect(logs.filter((line) => line.includes("factory rate bound"))).toEqual([
      expect.stringContaining(`1 deferrals; waiting at ${TOPIC} ${eta.id}, retry in `)]);
    expect(logs.filter((line) => line.includes("factory rate bound"))[0]).toMatch(/ ms; nothing dropped$/);
    expect(toLocalAfter()).toBeLessThan(eta.sequence);
    expect(await bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 }); // same instant: still deferred
    now += 60_000;
    expect(await bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    expect(await bridge.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(bridgedIds(hub, TOPIC)).toEqual([wait.id, eta.id]);
    expect(on(hub, TOPIC).map((event) => [event.kind, event.to])).toEqual([["wait", "factory"], ["eta", "factory"]]);
    expect(toLocalAfter()).toBeGreaterThanOrEqual(eta.sequence);
  });

  it("gives no exception to a malformed topic, an unknown or hub-claimed sender, or a bridge-stamped record", async () => {
    const { hub, far, bridge, lane, spoke } = await setup();
    const stranger: MeshIdentity = { id: sid("stranger"), name: "main", kind: "main" };
    for (const topic of ["fleet.work.smarty-dev", "fleet.work.inbox-receipts", "fleet.work.smarty-dev.01"]) {
      await far.publish({ topic, kind: "wait", from: spoke.identity, text: WAIT_TEXT });
    }
    for (const [kind, text] of [["wait", WAIT_TEXT], ["eta", ETA_TEXT]] as const) {
      await far.publish({ topic: TOPIC, kind, from: stranger, text }); // no live sender
      await far.publish({ topic: TOPIC, kind, from: lane.identity, text }); // claims a hub-native id
      // Explicitly bridge-stamped (a replayed or forged mirror): never forwarded again, even from the live spoke.
      await far.publish({ topic: TOPIC, kind, from: spoke.identity, text, data: { bridge: { from: "dev1", id: `forged-${kind}` } } });
    }
    expect(await bridge.step()).toMatchObject({ toLocal: 0 });
    expect(hub.read({ after: 0, limit: 100 }).filter((event) => event.topic.startsWith("fleet.work."))).toEqual([]);
  });

  it("replays the same ids after a crash before the cursor checkpoint without a duplicate, then carries a renewal once", async () => {
    const { hub, far, cursorPath, bridge, spoke, close } = await setup();
    const before = fs.readFileSync(cursorPath, "utf8"); // the pre-step checkpoint
    const wait = await far.publish({ topic: TOPIC, kind: "wait", from: spoke.identity, text: WAIT_TEXT });
    const eta = await far.publish({ topic: TOPIC, kind: "eta", from: spoke.identity, text: ETA_TEXT });
    expect(await bridge.step()).toMatchObject({ toLocal: 2 });
    expect(bridgedIds(hub, TOPIC)).toEqual([wait.id, eta.id]);
    // Crash after publication, before the source cursor checkpoint: the original link stops through its owned
    // teardown (bridge, then its agent pipe), so afterEach does not stop it again; the cursor is the older one.
    await close();
    fs.writeFileSync(cursorPath, before);

    // Restart the real sides; the recovered bridge rereads the same original events (same ids).
    const recovered = link(hub, far, cursorPath).bridge;
    await recovered.start();
    expect(await recovered.step()).toMatchObject({ toLocal: 0, dropped: 0 });
    expect(bridgedIds(hub, TOPIC)).toEqual([wait.id, eta.id]);

    // A fresh renewal (new id) after recovery still crosses, once.
    const renewal = await far.publish({ topic: TOPIC, kind: "wait", from: spoke.identity, text: RENEWAL_TEXT });
    expect(await recovered.step()).toMatchObject({ toLocal: 1 });
    expect(await recovered.step()).toMatchObject({ toLocal: 0 });
    expect(bridgedIds(hub, TOPIC)).toEqual([wait.id, eta.id, renewal.id]);
    expect(on(hub, TOPIC).at(-1)).toMatchObject({ kind: "wait", to: "factory", text: RENEWAL_TEXT });
  });
});
