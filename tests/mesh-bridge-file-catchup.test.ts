import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, expect, it, vi } from "vitest";
import { RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePresence, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";

const roots: string[] = [];
const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
// A bounded observation window lets real fs.watch notifications reach the agent, not a mocked notifier.
const settle = async () => { await new Promise(resolve => setTimeout(resolve, 30)); await turn(); };
const until = async (check: () => boolean) => {
  const deadline = performance.now() + 3_000;
  while (!check() && performance.now() < deadline) await new Promise(resolve => setTimeout(resolve, 5));
  expect(check()).toBe(true);
};
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const presence = (id: string, now: number): BridgePresence => {
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: id.slice(8) };
  return {
    hosts: [{ record: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 60_000 },
      expiresAt: now + 60_000, observedAt: now }],
    participants: [{ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
      name: id, label: id, status: "idle", runner: "pi", transport: "host", capabilities: ["steer"],
      sessionId: id.slice(8), startedAt: now, updatedAt: now, controlProtocol: "v1" }],
    reserved: [],
  };
};
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-file-catchup-"));
  roots.push(root);
  const store = new MeshStore(root, 64 * 1024, 500, { stateBackend: "file" });
  cleanups.push(() => store.closeState());
  const entries: Record<string, MeshStateEntry> = {};
  let version = 0;
  const now = Date.now();
  for (let index = 0; index < 100; index++) {
    const native = presence(`session:native-${index}-00000000`, now);
    const host = native.hosts[0]!.record;
    for (const [kind, value] of [["hosts", host], ["participants", { ...native.participants[0]!, description: "x".repeat(24_000) }]] as const) {
      const entryKey = key(kind, host.id);
      entries[entryKey] = { key: entryKey, value, version: ++version, updatedAt: now, updatedBy: host.identity };
    }
  }
  fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: 1, entries }));
  const side = new StoreBridgeSide(store, "hub");
  const input = new PassThrough(), output = new PassThrough();
  const serving = serveBridgeAgent(side, input, output);
  const remote = new RemoteBridgeSide(output, input);
  cleanups.push(async () => { remote.close(); input.end(); await serving; output.end(); });
  return { root, store, side, remote, now };
};

// Keep: the real #13645 performance failure, measured at the agent RPC/file-backend boundary.
// No wall-time-only regression gate: the copy budget catches the old implementation deterministically.
it("catches up 200 events against 2.5 MB state without cloning the topology per notification", async () => {
  const f = fixture();
  const stateBytes = fs.statSync(path.join(f.root, "state.json")).size;
  expect(stateBytes).toBeGreaterThan(2_400_000);
  expect(stateBytes).toBeLessThan(2_600_000);
  let clonedBytes = 0;
  const listAll = f.store.listAll.bind(f.store);
  vi.spyOn(f.store, "listAll").mockImplementation((prefix, options) => {
    const entries = listAll(prefix, options);
    // Measure real copy-on-read materialization, not the mirror's separate transaction view.
    if (prefix?.startsWith("topology/")) clonedBytes += Buffer.byteLength(JSON.stringify(entries));
    return entries;
  });
  const rename = vi.spyOn(fs, "renameSync");
  const changes: string[] = [];
  await f.remote.hello();
  const unsubscribe = await f.remote.subscribeChanges(kind => changes.push(kind), error => { throw error; });
  expect(unsubscribe).toBeTypeOf("function");
  cleanups.push(() => unsubscribe?.());
  const incoming = presence("session:hub-root-00000000", f.now);
  const started = performance.now();
  let count = 0;
  for (let batch = 0; batch < 4; batch++) {
    // Presence can change between catch-up pages. Keep the real mirror commit + fs notification
    // in the measured path, rather than testing the already-cheap events-only notification.
    await f.remote.mirror({ ...incoming, participants: incoming.participants.map(p => ({ ...p, status: `page-${batch}` })) });
    const events: Array<{ event: BridgePublish }> = Array.from({ length: 50 }, (_, index) => ({ event: {
      topic: "fleet.work.catchup", kind: "ask", from: incoming.hosts[0]!.record.identity,
      to: "session:native-0-00000000", text: String(batch * 50 + index), data: { bridge: { from: "hub", id: `event-${batch * 50 + index}` } },
    } }));
    for (let offset = 0; offset < events.length;) {
      const prefix = await f.remote.publishBatch!(events.slice(offset));
      expect(prefix.length).toBeGreaterThan(0);
      offset += prefix.length;
      count += prefix.length;
      await turn();
    }
    await settle();
  }
  const elapsedMs = performance.now() - started;
  const writes = rename.mock.calls.filter(([, target]) => String(target) === path.join(f.root, "state.json")).length;
  const delivered = f.store.read({ limit: 500 });
  expect(count).toBe(200);
  expect(delivered.map(event => event.text)).toEqual(Array.from({ length: 200 }, (_, index) => String(index)));
  expect(changes).toContain("events");
  // These four actual mirror edits still commit independently; event delivery adds no state rewrite.
  expect(writes).toBe(4);
  // Capture the metric before the intentional public copy below.
  console.log(JSON.stringify({ stateBytes, events: count, stateWrites: writes, topologyCloneBytes: clonedBytes, elapsedMs: Math.round(elapsedMs) }));
  expect(clonedBytes).toBe(0);
  // Wall time is reported, not a flaky CI threshold: the copy/write budgets gate the regression.
  // Public presence is still a detached copy; the zero-copy view never escapes this subscriber.
  const exposed = await f.side.presence();
  exposed.participants[0]!.status = "caller mutation";
  expect((await f.side.presence()).participants[0]!.status).not.toBe("caller mutation");
}, 30_000);

it("still detects file-backed participant edits, same-version replacements and mirror removal", async () => {
  const f = fixture();
  const changes: string[] = [];
  const unsubscribe = f.side.subscribeChanges(kind => changes.push(kind), error => { throw error; });
  cleanups.push(unsubscribe);
  await settle();
  changes.length = 0;
  // Copy-on-read is intentionally retained on the public API.
  const participant = f.store.listAll("topology/participants/")[0]!;
  await f.store.put({ key: participant.key, identity: participant.updatedBy, value: { ...participant.value as object, status: "running" } });
  await until(() => changes.includes("presence"));
  expect(changes).toContain("presence");
  changes.length = 0;
  // A legacy writer can change content without changing any revision label. Object identity,
  // not version, owns the digest cache. Strip generation markers just as a legacy writer does.
  const file = path.join(f.root, "state.json");
  const state = JSON.parse(fs.readFileSync(file, "utf8"));
  delete state.readGeneration;
  delete state.readJournalHash;
  state.entries[participant.key].value.status = "legacy edit";
  const replacement = path.join(f.root, "legacy.tmp");
  fs.writeFileSync(replacement, JSON.stringify(state));
  fs.renameSync(replacement, file);
  await until(() => changes.includes("presence"));
  expect(changes).toContain("presence");
  changes.length = 0;
  await f.remote.mirror(presence("session:hub-root-00000000", f.now));
  await until(() => changes.includes("presence"));
  changes.length = 0;
  await f.remote.mirror({ hosts: [], participants: [] });
  await until(() => changes.includes("presence"));
  expect(changes).toContain("presence");
});
