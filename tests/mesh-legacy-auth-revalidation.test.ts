// #164 Security S1: a legacy writer may copy a UUID and repeat the canonical stat.
// Neither is authority to retain an earlier owner after confirmation or a fresh read.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { writeFileAtomic } from "../src/core/atomic-write.js";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, type FabricLifecycleEvent, type FabricLifecycleSubscription } from "../src/lifecycle/types.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const scratch = fileURLToPath(new URL("../.local/check-temp/", import.meta.url));
const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
const brokers: LifecycleBroker[] = [];
const real = { statSync: fs.statSync, readFileSync: fs.readFileSync };
const oldOwner: MeshIdentity = { id: "session:old", name: "owner", kind: "main" };
const newOwner: MeshIdentity = { id: "session:new", name: "owner", kind: "main" };
const target: MeshIdentity = { id: "session:target", name: "main", kind: "main" };
const sourceId = "actor:shared";
const keyFor = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const key = keyFor("topology/participants/", sourceId);
type State = { entries: Record<string, MeshStateEntry>; readGeneration?: string };
const disk = (root: string): State => JSON.parse(String(real.readFileSync(path.join(root, "state.json"), "utf8")));
const record = (owner: MeshIdentity): FabricParticipantRecord => ({
  format: 1, id: sourceId, kind: "actor", rootId: "session:root", ownerHostId: owner.id,
  ownerIdentityId: owner.id, name: "shared", status: "idle", runner: "pi", transport: "host",
  capabilities: ["fabric"], startedAt: 1, updatedAt: 1, controlProtocol: "v1",
});
const setup = async () => {
  fs.mkdirSync(scratch, { recursive: true });
  const root = fs.mkdtempSync(path.join(scratch, "legacy-auth-"));
  roots.push(root);
  const writer = new MeshStore(root, 64 * 1024, 100);
  const reader = new MeshStore(root, 64 * 1024, 100, { readCacheMs: RUNTIME_MESH_READ_CACHE_MS });
  await writer.put({ key, value: record(oldOwner), identity: oldOwner });
  return { root, writer, reader };
};
// Only the canonical stat is mocked; the lock, atomic rename, bytes, and directory are real.
const warmAndHandoff = async (root: string, writer: MeshStore, reader: MeshStore) => {
  if (!disk(root).readGeneration) await writer.exclusive(() => writeFileAtomic(path.join(root, "state.json"),
    JSON.stringify({ readGeneration: "12345678-1234-4234-8234-123456789abc", ...disk(root) })));
  const file = path.join(root, "state.json");
  const frozen = real.statSync(file);
  vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) =>
    path.resolve(String(target)) === file ? frozen :
      (real.statSync as (...args: unknown[]) => fs.Stats)(target, ...rest)) as typeof fs.statSync);
  expect(reader.get(key)?.value).toEqual(record(oldOwner));
  reader.listAll("topology/participants/");
  const before = disk(root);
  const signalFile = path.join(root, "state.read-signal.json");
  const signal = fs.existsSync(signalFile) ? real.readFileSync(signalFile, "utf8") : undefined;
  await writer.exclusive(() => {
    const state = disk(root);
    state.entries[key]!.value = record(newOwner);
    state.entries[key]!.updatedBy = newOwner;
    writeFileAtomic(file, JSON.stringify(state)); // old writer: copies marker, publishes no hint
  });
  expect(real.statSync(file).size).toBe(frozen.size);
  expect(fs.statSync(file)).toBe(frozen);
  const after = disk(root);
  expect(after.readGeneration).toBe(before.readGeneration);
  expect(after.entries[key]!.version).toBe(before.entries[key]!.version);
  expect(after.entries[key]!.updatedAt).toBe(before.entries[key]!.updatedAt);
  expect(after.entries[key]!.updatedBy).toEqual(newOwner);
  expect(after.entries[key]!.value).toEqual(record(newOwner)); // independent canonical oracle
  expect(fs.existsSync(signalFile) ? real.readFileSync(signalFile, "utf8") : undefined).toBe(signal);
  return after.entries[key]!;
};

afterEach(async () => {
  await Promise.all(brokers.splice(0).map((broker) => broker.close()));
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const nextReads = {
  get: (store: MeshStore) => store.get(key),
  list: (store: MeshStore) => store.list("topology/participants/")[0],
  listAll: (store: MeshStore) => store.listAll("topology/participants/")[0],
  listAllShared: (store: MeshStore) => store.listAllShared("topology/participants/")[0],
  stateToken: (store: MeshStore) => { store.stateToken(); return store.get(key); },
};
describe("legacy owner revalidation after confirmWritable", () => {
  for (const [method, read] of Object.entries(nextReads)) it(`next ${method} returns the canonical new owner`, async () => {
    const { root, writer, reader } = await setup();
    const expected = await warmAndHandoff(root, writer, reader);
    await reader.confirmWritable();
    expect(read(reader)).toEqual(expected);
  });
});

describe("fresh canonical fallback without confirmation", () => {
  for (const method of ["get", "listAll", "listAllShared", "stateToken"] as const) it(`${method} cannot trust a copied marker and repeated stat`, async () => {
    const { root, writer, reader } = await setup();
    const expected = await warmAndHandoff(root, writer, reader);
    if (method === "get") expect(reader.get(key, { fresh: true })).toEqual(expected);
    else if (method === "stateToken") { reader.stateToken({ fresh: true }); expect(reader.get(key)).toEqual(expected); }
    else expect(reader[method]("topology/participants/", { fresh: true })).toEqual([expected]);
  });
});

it("real receiver directory rejects the former owner and admits the new owner after legacy handoff without confirmation", async () => {
  const { root, writer, reader } = await setup();
  const directory = new ParticipantDirectory(reader, {
    enabled: true, hostId: target.id, rootId: target.id, identity: target, heartbeatMs: 60_000, leaseMs: 60_000,
  });
  directories.push(directory);
  directory.registerSource(() => [directory.root({ id: target.id, name: "Main", kind: "main", status: "idle",
    runner: "pi", transport: "host", updatedAt: 1, pendingMessages: false, local: true })]);
  await directory.refresh();
  for (const owner of [oldOwner, newOwner]) await writer.put({
    key: keyFor("topology/hosts/", owner.id), identity: owner, value: {
      format: 1, id: owner.id, rootId: "session:root", identity: owner, startedAt: 1,
      updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
    },
  });
  await writer.put({ key: "topology/subscriptions/security", identity: target, value: {
    format: 1, id: "security", from: sourceId, events: ["pi.agent_settled"], to: target.id,
    delivery: "followUp", triggerTurn: false, once: true, afterSequence: writer.latestSequence(),
    createdAt: 1, updatedAt: 1, createdBy: target,
  } satisfies FabricLifecycleSubscription });
  // Bootstrap the receiver's real directory on the final pre-handoff canonical snapshot.
  expect(directory.get(sourceId, undefined, { fresh: true })).toMatchObject({ ownerHostId: oldOwner.id, stale: false });
  await warmAndHandoff(root, writer, reader);
  const delivered: FabricLifecycleEvent[] = [];
  const receiver = new LifecycleBroker(reader, target, directory,
    { enabled: true, pollMs: 20, maxReadEvents: 100 }, (_subscription, event) => { delivered.push(event); });
  brokers.push(receiver);
  for (const [owner, occurredAt] of [[oldOwner, 1], [newOwner, 2]] as const) await writer.publish({
    topic: FABRIC_PARTICIPANT_LIFECYCLE_TOPIC, kind: "pi.agent_settled", from: { id: sourceId, name: "shared", kind: "actor" },
    data: { version: 1, event: "pi.agent_settled", source: {
      id: sourceId, name: "shared", kind: "actor", rootId: "session:root", runner: "pi",
      ownerHostId: owner.id, ownerIdentityId: owner.id,
    }, occurredAt },
  });
  receiver.start();
  await vi.waitFor(() => expect(delivered).toHaveLength(1), { timeout: 2_000, interval: 10 });
  expect(delivered.map((event) => event.occurredAt)).toEqual([2]);
  expect(directory.get(sourceId, undefined, { fresh: true })).toMatchObject({ ownerHostId: newOwner.id, stale: false });
  await vi.waitFor(() => expect(receiver.list()).toEqual([]), { timeout: 2_000, interval: 10 });
}, 5_000);
