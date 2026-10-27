import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { PresenceDropReasons } from "../src/mesh/presence-drop-reasons.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Keep: data safety/trust boundary. Exercise raw dual publication through the real RPC and
// mirror commit, not a mock of partial admission; a conflict must hold only its own authority.
const roots: string[] = [];
const stores: MeshStore[] = [];
const closes: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closes.splice(0)) await close();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (family: string, id: string) => `topology/${family}/${createHash("sha256").update(id).digest("hex")}`;
const record = (tag: string) => {
  const now = Date.now();
  const id = `session:conflict-test-${tag}`;
  const identity: MeshIdentity = { id, name: tag, kind: "main", sessionId: `conflict-test-${tag}` };
  const host: FabricHostRecord = { format: 1, id, rootId: id, identity, startedAt: now,
    updatedAt: now, expiresAt: now + 120_000 };
  const participant: FabricParticipantRecord = { format: 1, id, kind: "root", rootId: id,
    ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "synthetic-owner-token", name: tag,
    status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], sessionId: `conflict-test-${tag}`,
    startedAt: now, updatedAt: now, controlProtocol: "v1" };
  return { identity, host, participant };
};
const install = async (store: MeshStore, root: ReturnType<typeof record>) => {
  await store.put({ key: key("hosts", root.host.id), identity: root.identity, value: root.host });
  await store.put({ key: key("participants", root.participant.id), identity: root.identity, value: root.participant });
};
const fixture = (backend: "file" | "sqlite") => {
  const open = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "presence-copy-conflict-"));
    roots.push(root);
    const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
    stores.push(store);
    return store;
  };
  const hub = open(), far = open();
  const input = new PassThrough(), output = new PassThrough();
  const local = new StoreBridgeSide(hub, "far", Date.now, false);
  const agent = new StoreBridgeSide(far, "hub", Date.now, false);
  const serving = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input);
  const logs: string[] = [];
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
    cursorPath: path.join(hub.root, "cursor.json"), log: line => logs.push(line) });
  closes.push(async () => { await bridge.stop(); remote.close(); input.end(); await serving; output.end(); });
  return { hub, far, local, agent, bridge, logs };
};

describe.each(["file", "sqlite"] as const)("participant copy isolation (%s)", backend => {
  it.each((["name", "status"] as const).flatMap(field => ([
    ["inbound", true], ["outbound", true], ["inbound", false], ["outbound", false],
  ] as const).map(([direction, prior]) => ({ direction, prior, field }))))(
    "holds a conflicting root while admitting a new valid root ($direction, prior=$prior, field=$field)", async ({ direction, prior, field }) => {
    const f = fixture(backend);
    const source = direction === "inbound" ? f.far : f.hub;
    const target = direction === "inbound" ? f.hub : f.far;
    const side = direction === "inbound" ? f.agent : f.local;
    const held = record("held"), omitted = record("omitted"), added = record("added");
    if (prior) await install(source, held);
    await install(source, omitted);
    await f.bridge.syncPresence();
    const heldKey = key("participants", held.identity.id);
    const previous = target.get(heldKey);
    if (!prior) await install(source, held);
    await source.delete({ key: key("hosts", omitted.identity.id) });
    await source.delete({ key: key("participants", omitted.identity.id) });
    await install(source, added);
    const entry = source.get(heldKey)!;
    const difference = field === "name" ? { name: "secret-alias-never-log" } : { status: "running" };
    writeParticipantFile(source.root, { ...entry, version: entry.version + 1, updatedAt: entry.updatedAt + 1,
      value: { ...held.participant, ...difference } });

    const sampled = await side.presence();
    expect(sampled.partial).toBe(true);
    expect(sampled.participants.map(p => p.id)).toEqual([added.identity.id]);
    expect(sampled.hosts.map(h => h.record.id)).not.toContain(held.identity.id);
    await f.bridge.syncPresence();
    await f.bridge.syncPresence(); // Persistent partials must neither starve additions nor replace the conflict.
    expect(target.get(key("participants", added.identity.id))?.value).toMatchObject({ id: added.identity.id });
    expect(target.get(heldKey)).toEqual(previous);
    expect(target.get(key("participants", omitted.identity.id))).toBeDefined();
    expect(f.logs.join("\n")).toContain(`copy(id=${JSON.stringify(held.identity.id)}; fields=${field})`);
    expect(f.logs.join("\n")).not.toContain("secret-alias-never-log");
    expect(f.logs.join("\n")).not.toContain("synthetic-owner-token");

    // Consistent copies resume normal admission AND omission withdrawal, without a restart.
    await source.put({ key: heldKey, identity: held.identity, value: { ...held.participant, ...difference } });
    await f.bridge.syncPresence();
    expect(target.get(heldKey)?.value).toMatchObject(difference);
    expect(target.get(key("participants", omitted.identity.id))).toBeUndefined();
  });

  it.each(["actorQueued", "actorMessages"] as const)("keeps a child's %s conflict from suppressing its parent or another new root", async field => {
    const f = fixture(backend), root = record("parent"), added = record("independent");
    await install(f.far, root);
    await install(f.far, added);
    const child = { ...root.participant, id: "0123456789abcdef0123456789abcdef", kind: "actor" as const, [field]: 1 };
    const childKey = key("participants", child.id);
    await f.far.put({ key: childKey, identity: root.identity, value: child });
    const entry = f.far.get(childKey)!;
    writeParticipantFile(f.far.root, { ...entry, version: entry.version - 1, updatedAt: entry.updatedAt - 1,
      value: { ...child, [field]: 0 } });
    const sampled = await f.agent.presence();
    expect(sampled.partial).toBe(true);
    expect(sampled.participants.map(p => p.id)).toEqual([root.identity.id, added.identity.id]);
    await f.bridge.syncPresence();
    expect(f.hub.get(key("participants", root.identity.id))).toBeDefined();
    expect(f.hub.get(key("participants", added.identity.id))).toBeDefined();
    expect(f.logs.join("\n")).toContain(`copy(id=${JSON.stringify(child.id)}; fields=${field})`);
  });

  it.each(["root", "agent"] as const)("does not confuse dual-published activity with an authority conflict (%s)", async kind => {
    const f = fixture(backend), root = record("activity");
    await install(f.far, root);
    const participant = kind === "root" ? root.participant : { ...root.participant, id: "abcdef0123456789abcdef0123456789ab", kind };
    if (kind !== "root") await f.far.put({ key: key("participants", participant.id), identity: root.identity, value: participant });
    const entry = f.far.get(key("participants", participant.id))!;
    writeParticipantFile(f.far.root, { ...entry, version: entry.version - 1, updatedAt: entry.updatedAt - 1,
      value: { ...participant, currentTool: "secret-tool", turns: 2, toolCalls: 3,
        usage: { input: 1, output: 2, cacheRead: 3, cacheWrite: 4, cost: 0.01 } } });
    expect(await f.agent.presence()).not.toHaveProperty("partial");
    await f.bridge.syncPresence();
    expect(f.hub.get(key("participants", root.identity.id))?.value).toMatchObject({ id: root.identity.id });
    expect(f.logs).toEqual([]);
  });
});

it("bounds and sanitizes untrusted conflict details before logging them", () => {
  const reasons = new PresenceDropReasons([{ keyClass: "topology/participants", reason: "conflicting-copy", count: 100,
    copies: Array.from({ length: 100 }, (_, i) => ({ id: `session:synthetic-${i.toString().padStart(8, "0")}`,
      fields: ["name", "secret-arbitrary-field", "ownerIncarnation"] })) }], 100);
  expect(reasons.summary()).toContain('copy(id="session:synthetic-00000000"; fields=name,ownerIncarnation)');
  expect(reasons.summary().match(/copy\(/g)).toHaveLength(3);
  expect(reasons.summary()).not.toContain("secret-arbitrary-field");
  const forged = new PresenceDropReasons([{ keyClass: "topology/participants", reason: "conflicting-copy", count: 1,
    copies: [{ id: "secret\nforged log", fields: ["name"] }] }], 1);
  expect(forged.summary()).toBe("conflicting-copy:topology/participants=1");
});
