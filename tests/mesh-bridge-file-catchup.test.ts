import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, expect, it, vi } from "vitest";
import { BRIDGE_LEASE_MS, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePresence, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { COMMIT_OUTBOX_PREFIX } from "../src/mesh/commit-outbox.js";
import { readHostLease } from "../src/topology/host-leases.js";

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
  const cloneSites = new Map<string, { calls: number; bytes: number }>();
  const listAll = f.store.listAll.bind(f.store);
  vi.spyOn(f.store, "listAll").mockImplementation((prefix, options) => {
    const entries = listAll(prefix, options);
    // Measure real copy-on-read materialization, not the mirror's separate transaction view.
    if (prefix?.startsWith("topology/")) {
      const bytes = Buffer.byteLength(JSON.stringify(entries));
      clonedBytes += bytes;
      const site = `${phase}: ${new Error().stack?.split("\n").slice(2, 6).join(" | ")}`;
      const previous = cloneSites.get(site) ?? { calls: 0, bytes: 0 };
      cloneSites.set(site, { calls: previous.calls + 1, bytes: previous.bytes + bytes });
    }
    return entries;
  });
  const stateFile = path.join(f.root, "state.json");
  const readEntries = (file: fs.PathLike) => (JSON.parse(fs.readFileSync(file, "utf8")) as {
    entries: Record<string, MeshStateEntry>;
  }).entries;
  const stateEdits: Array<{ page: number; phase: string; topology: string[]; recorded: MeshStateEntry[];
    retired: MeshStateEntry[]; other: string[]; installed: ReturnType<typeof readHostLease>[] }> = [];
  let page = -1, phase = "setup";
  const renameSync = fs.renameSync;
  const rename = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (String(to) === stateFile) {
      // Inspect the actual atomic replacement, not writeBatch intent/call counts. A
      // retirement may rewrite FILE state but must change ONLY fulfilled outbox rows.
      const before = readEntries(stateFile), after = readEntries(from);
      const keys = [...new Set([...Object.keys(before), ...Object.keys(after)])]
        .filter(key => JSON.stringify(before[key]) !== JSON.stringify(after[key]));
      const recorded = keys.filter(key => key.startsWith(COMMIT_OUTBOX_PREFIX) && after[key]).map(key => after[key]!);
      const retired = keys.filter(key => key.startsWith(COMMIT_OUTBOX_PREFIX) && !after[key]).map(key => before[key]!);
      stateEdits.push({ page, phase, topology: keys.filter(key => key.startsWith("topology/")), recorded, retired,
        other: keys.filter(key => !key.startsWith("topology/") && !key.startsWith(COMMIT_OUTBOX_PREFIX)),
        installed: retired.map(entry => readHostLease(f.root, (entry.value as { payload: { id: string } }).payload.id)) });
    }
    return renameSync(from, to);
  });
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
    page = batch;
    phase = "mirror";
    await f.remote.mirror({ ...incoming, participants: incoming.participants.map(p => ({ ...p, status: `page-${batch}` })) });
    const writesBeforeEvents = stateEdits.length;
    phase = "events";
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
    phase = "notification";
    await settle();
    expect(stateEdits).toHaveLength(writesBeforeEvents); // events AND their notifications write no state
  }
  const elapsedMs = performance.now() - started;
  const writes = rename.mock.calls.filter(([, target]) => String(target) === stateFile).length;
  const delivered = f.store.read({ limit: 500 });
  expect(count).toBe(200);
  expect(delivered.map(event => event.text)).toEqual(Array.from({ length: 200 }, (_, index) => String(index)));
  expect(changes).toContain("events");
  // Four semantic mirror commits, each followed by retirement of its ONE fulfilled
  // lease row. This is a behavioral budget, not permission for eight arbitrary copies.
  expect(stateEdits.filter(edit => edit.topology.length > 0)).toHaveLength(4);
  expect(stateEdits.filter(edit => edit.retired.length > 0)).toHaveLength(4);
  expect(writes).toBe(stateEdits.length);
  for (let batch = 0; batch < 4; batch++) {
    const edits = stateEdits.filter(edit => edit.page === batch);
    expect(edits).toHaveLength(2);
    const [mirror, retirement] = edits;
    expect(mirror).toMatchObject({ phase: "mirror", retired: [], other: [] });
    expect(mirror!.topology.sort()).toEqual((batch === 0 ? [key("hosts", incoming.hosts[0]!.record.id),
      key("participants", incoming.hosts[0]!.record.id)] : [key("participants", incoming.hosts[0]!.record.id)]).sort());
    expect(mirror!.recorded).toHaveLength(1);
    expect(retirement).toMatchObject({ phase: "mirror", topology: [], recorded: [], other: [] });
    expect(retirement!.retired).toEqual(mirror!.recorded); // exact recording, not a newer row
    const row = retirement!.retired[0]!.value as { kind: string; payload: { id: string; rootId: string;
      identityId: string; startedAt: number; expiresAt: number } };
    expect(row.kind).toBe("lease");
    const installed = retirement!.installed[0]!;
    expect(installed).toMatchObject({ id: row.payload.id, rootId: row.payload.rootId,
      identityId: row.payload.identityId, startedAt: row.payload.startedAt,
      mirror: { sourceObservedAt: f.now, sourceExpiresAt: f.now + 60_000 } });
    // A repeat of the SAME source sample may retain its earlier, shorter deadline;
    // retirement is not authority to make that observation fresh again.
    expect(installed.expiresAt).toBeLessThanOrEqual(row.payload.expiresAt);
    expect(installed.expiresAt).toBe(installed.mirror!.observedAt + Math.min(BRIDGE_LEASE_MS,
      installed.mirror!.sourceExpiresAt - installed.mirror!.sourceObservedAt));
  }
  // Capture the metric before the intentional public copy below.
  console.log(JSON.stringify({ stateBytes, events: count, stateWrites: writes, topologyCloneBytes: clonedBytes,
    cloneSites: [...cloneSites], elapsedMs: Math.round(elapsedMs) }));
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
  await settle();
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
  await settle();
  expect(changes).toContain("presence");
  await f.remote.mirror(presence("session:hub-root-00000000", f.now));
  await settle();
  changes.length = 0;
  await f.remote.mirror({ hosts: [], participants: [] });
  await settle();
  expect(changes).toContain("presence");
});
