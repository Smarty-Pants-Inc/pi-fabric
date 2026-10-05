import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { LIVENESS_POLICY_KEY, readHostLeases, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const roots: string[] = [];
const links: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of links.splice(0)) await close();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
const participantKey = (id: string) => key("topology/participants/", id);

// File-only native participants, as deployed on the fleet; projections stay in shared state.
const native = async (store: MeshStore, name: string, now: number) => {
  const identity: MeshIdentity = { id: `session:${name}-00000000`, name: "main", kind: "main", sessionId: name };
  await store.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, hostLeases: "files", participants: "files" } });
  await store.put({ key: hostKey(identity.id), identity, value: {
    format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + 60_000,
  } });
  writeHostLease(store.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
    startedAt: now, updatedAt: now, expiresAt: now + 60_000 });
  const entry: MeshStateEntry = { key: participantKey(identity.id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } };
  writeParticipantFile(store.root, entry);
  return { identity, entry, file: path.join(store.root, "participants", `${participantKey(identity.id).split("/").at(-1)}.json`) };
};

const connect = (far: MeshStore) => {
  const input = new PassThrough();
  const output = new PassThrough();
  const serving = serveBridgeAgent(new StoreBridgeSide(far, "ryzen1"), input, output);
  const remote = new RemoteBridgeSide(output, input);
  const close = async () => { remote.close(); input.end(); await serving; output.end(); };
  links.push(close);
  return { remote, close };
};

const fixture = async () => {
  let now = 1_800_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-participants-"));
  roots.push(root);
  const hub = new MeshStore(path.join(root, "hub"), 64 * 1024, 100);
  const far = new MeshStore(path.join(root, "remote"), 64 * 1024, 100);
  const lead = await native(hub, "lead", now);
  const lane = await native(far, "lane", now);
  const transport = connect(far);
  const options = { localName: "ryzen1", remoteName: "ryzen2", local: new StoreBridgeSide(hub, "ryzen2"),
    remote: transport.remote, cursorPath: path.join(root, "cursor.json") };
  const bridge = new MeshBridge(options);
  const directory = (store: MeshStore, identity: MeshIdentity) => new ParticipantDirectory(store, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity,
  });
  const leads = directory(far, lane.identity);
  const lanes = directory(hub, lead.identity);
  const visible = () => {
    expect(leads.get(lead.identity.id)).toMatchObject({ id: lead.identity.id, remoteHost: "ryzen1", stale: false });
    expect(lanes.get(lane.identity.id)).toMatchObject({ id: lane.identity.id, remoteHost: "ryzen2", stale: false });
  };
  const prune = async () => {
    for (const [store, source] of [[far, lead], [hub, lane]] as const) {
      await store.delete({ key: participantKey(source.identity.id) });
      await store.delete({ key: hostKey(source.identity.id) });
    }
  };
  return { root, hub, far, lead, lane, transport, options, bridge, leads, lanes, visible, prune,
    advance: (ms: number) => { now += ms; } };
};

describe("bridge participant reconciliation (smarty-dev#5036)", () => {
  it("projects current live file-only participants in both directions on connect, before polling", async () => {
    const f = await fixture();
    expect(f.hub.get(f.lead.entry.key)).toBeUndefined();
    expect(f.far.get(f.lane.entry.key)).toBeUndefined();
    await f.bridge.start();
    f.visible();
  });

  it.each(["expired", "pruned"] as const)("reprojects on restart with an existing cursor after projections are %s", async (state) => {
    const f = await fixture();
    await f.bridge.start();
    await f.bridge.step();
    f.visible();
    const cursor = fs.readFileSync(f.options.cursorPath, "utf8");
    // A crashed transport cannot withdraw; source leases remain live across the gap.
    await f.transport.close();
    f.advance(BRIDGE_LEASE_MS + 1);
    expect(f.leads.get(f.lead.identity.id)).toBeUndefined();
    expect(f.lanes.get(f.lane.identity.id)).toBeUndefined();
    if (state === "pruned") await f.prune();
    const restarted = new MeshBridge({ ...f.options, local: new StoreBridgeSide(f.hub, "ryzen2"), remote: connect(f.far).remote });
    await restarted.start();
    f.visible();
    expect(fs.readFileSync(f.options.cursorPath, "utf8")).toBe(cursor);
  });

  it("forces reprojection on reconnect even before the previous refresh is due, and routes lane work to the lead", async () => {
    const f = await fixture();
    await f.bridge.start();
    await f.bridge.step();
    await f.transport.close();
    f.advance(1_000);
    await f.prune();
    f.options.remote = connect(f.far).remote;
    await f.bridge.start();
    f.visible();
    await f.far.publish({ topic: "fleet.work.projection", kind: "ask", from: f.lane.identity,
      to: f.lead.identity.id, text: "lead handoff after reconnect" });
    expect(await f.bridge.step()).toMatchObject({ toLocal: 1, dropped: 0 });
    expect(f.hub.read({ after: 0 }).find(e => e.topic === "fleet.work.projection"))
      .toMatchObject({ from: { id: f.lane.identity.id }, to: f.lead.identity.id, text: "lead handoff after reconnect" });
  });

  it("repairs lost destination records on an idle periodic pass even when the source set is unchanged", async () => {
    const f = await fixture();
    await f.bridge.start();
    await f.bridge.step();
    await f.prune();
    f.advance(5_000);
    expect(await f.bridge.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
    f.visible();
  });

  it("compares content digests without rewriting an unchanged set, including reordered participant properties", async () => {
    const f = await fixture();
    await f.bridge.start();
    await f.bridge.step();
    const before = [f.hub.listAll(), f.far.listAll()];
    const writes = vi.spyOn(fs, "renameSync");
    // File replacement/property order is not a semantic participant change.
    for (const [store, source] of [[f.hub, f.lead], [f.far, f.lane]] as const) {
      writeParticipantFile(store.root, { ...source.entry,
        value: Object.fromEntries(Object.entries(source.entry.value as Record<string, unknown>).reverse()) });
    }
    for (let tick = 0; tick < 3; tick++) { f.advance(5_000); await f.bridge.step(); }
    expect(writes.mock.calls.filter(([, file]) => [path.join(f.hub.root, "state.json"), path.join(f.far.root, "state.json")].includes(String(file))))
      .toHaveLength(0);
    expect([f.hub.listAll(), f.far.listAll()]).toEqual(before);
    expect(readHostLeases(f.far.root).get(f.lead.identity.id)!.updatedAt).toBe(Date.now());
    f.visible();
  });

  it("lets a removed source participant expire remotely and removes its projection on reconciliation", async () => {
    const f = await fixture();
    await f.bridge.start();
    await f.bridge.step();
    f.visible();
    fs.rmSync(f.lead.file);
    f.advance(BRIDGE_LEASE_MS + 1);
    expect(f.leads.get(f.lead.identity.id)).toBeUndefined();
    await f.bridge.step();
    expect(f.far.get(f.lead.entry.key)).toBeUndefined();
    expect(f.leads.get(f.lead.identity.id)).toBeUndefined();
    expect(f.lanes.get(f.lane.identity.id)).toMatchObject({ stale: false });
  });
});
