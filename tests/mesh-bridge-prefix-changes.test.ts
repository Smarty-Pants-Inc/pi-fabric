import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import { MeshStore } from "../src/mesh/store.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { hostLeasePath, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Keep: smarty-dev#16879, the real fs.watch -> subscribeChanges read-amplification incident.
// No notifier/snapshot/fence replacements; spies below only count calls to real implementations.
const NOW = 1_900_000_000_000;
const prefixes = ["topology/hosts/", "topology/participants/", "topology/handoffs/"];
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const roots: string[] = [];
const cleanup: Array<() => void> = [];
afterEach(() => {
  for (const close of cleanup.splice(0).reverse()) close();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
type NativeWatchEvent = {
  at: string;
  elapsedMs: number;
  phase: string;
  watched: string;
  name: "change";
  type: string;
  filename: string | null;
};
type NativeWatchTrace = {
  startedAt: number;
  phase: string;
  watcherCount: number;
  dropped: number;
  events: NativeWatchEvent[];
};
type NativeWatchImplementation = (...args: Parameters<typeof fs.watch>) => fs.FSWatcher;
type NativeWatchSpy = {
  (...args: Parameters<typeof fs.watch>): fs.FSWatcher;
  getMockImplementation?: () => NativeWatchImplementation | undefined;
  mockImplementation: (implementation: NativeWatchImplementation) => unknown;
};
const installNativeWatchTrace = (trace: NativeWatchTrace) => {
  const current = fs.watch as unknown as NativeWatchSpy;
  const alreadyMocked = vi.isMockFunction(fs.watch);
  const previous = alreadyMocked ? current.getMockImplementation?.() : undefined;
  if (alreadyMocked && !previous) throw new Error("cannot trace fs.watch mock without its implementation");
  const base: NativeWatchImplementation = previous ?? (fs.watch.bind(fs) as NativeWatchImplementation);
  const spy = (alreadyMocked ? current : vi.spyOn(fs, "watch")) as unknown as NativeWatchSpy;
  spy.mockImplementation((...args) => {
    const watcher = base(...args);
    trace.watcherCount++;
    const target = args[0];
    const watched = path.basename(typeof target === "string" ? target : target.toString()).slice(0, 80);
    watcher.on("change", (eventType, filename) => {
      try {
        if (trace.events.length >= 128) {
          trace.dropped++;
          return;
        }
        const now = Date.now();
        trace.events.push({
          at: new Date(now).toISOString(), elapsedMs: Math.max(0, now - trace.startedAt),
          phase: trace.phase.slice(0, 96), watched, name: "change", type: String(eventType).slice(0, 40),
          filename: filename === null ? null : String(filename).slice(0, 120),
        });
      } catch { /* diagnostic listeners must not affect the real watcher callback */ }
    });
    return watcher;
  });
};
const summarizeError = (error: unknown) => {
  try {
    return (error instanceof Error ? `${error.name}: ${error.message}` : String(error)).slice(0, 300);
  } catch { return "<unprintable error>"; }
};
const captureDiagnostic = async <T>(read: () => T | Promise<T>) => {
  try { return { ok: true as const, value: await read() }; }
  catch (error) { return { ok: false as const, error: summarizeError(error) }; }
};
type WaitDiagnosticContext = {
  store: MeshStore;
  side: StoreBridgeSide;
  fences: { mock: { calls: { length: number }; results: Array<{ type: string }> } };
  nativeTrace: NativeWatchTrace;
  failures: Error[];
};
const summarizePublicPresence = (presence: Awaited<ReturnType<StoreBridgeSide["presence"]>>) => ({
  hosts: {
    count: presence.hosts.length,
    sample: presence.hosts.slice(0, 8).map(host => ({
      id: host.record.id.slice(0, 120), name: host.record.identity.name.slice(0, 120), expiresAt: host.expiresAt,
    })),
  },
  participants: {
    count: presence.participants.length,
    sample: presence.participants.slice(0, 8).map(participant => ({
      id: participant.id.slice(0, 120), name: participant.name.slice(0, 120), status: participant.status,
    })),
  },
  reserved: { count: presence.reserved.length, sample: presence.reserved.slice(0, 8).map(id => id.slice(0, 120)) },
  partial: presence.partial ?? false,
  droppedCount: presence.droppedCount ?? 0,
});
const dumpTimeoutDiagnostics = async (f: WaitDiagnosticContext, phase: string, waitError: unknown) => {
  try {
    const revisionBefore = await captureDiagnostic(() => f.store.stateRevision() ?? null);
    const publicPresence = await captureDiagnostic(async () => summarizePublicPresence(await f.side.presence()));
    const revisionAfter = await captureDiagnostic(() => f.store.stateRevision() ?? null);
    const diagnostic = {
      type: "mesh-bridge-prefix wait rejected", phase: phase.slice(0, 120), waitError: summarizeError(waitError),
      native: {
        platform: process.platform, arch: process.arch, node: process.version.slice(0, 64),
        uv: process.versions.uv?.slice(0, 32) ?? null, sqlite: process.versions.sqlite?.slice(0, 32) ?? null,
        os: os.type().slice(0, 64), release: os.release().slice(0, 64),
      },
      eventTrace: {
        watcherCount: f.nativeTrace.watcherCount, retained: f.nativeTrace.events.length, dropped: f.nativeTrace.dropped,
        events: f.nativeTrace.events.slice(-64).map(event => ({ ...event })),
      },
      revisions: { beforePublicRead: revisionBefore, afterPublicRead: revisionAfter },
      fences: {
        calls: f.fences.mock.calls.length, results: f.fences.mock.results.length,
        resultTypes: f.fences.mock.results.slice(-32).map(result => result.type),
      },
      publicPresence,
      watcherFailureCount: f.failures.length,
      watcherFailures: f.failures.slice(-8).map(summarizeError),
    };
    try { console.error(`[mesh-bridge-prefix-diagnostic] ${JSON.stringify(diagnostic)}`); } catch { /* preserve wait failure */ }
  } catch { /* diagnostics must never replace the original wait rejection */ }
};
const markPhase = (f: { nativeTrace: NativeWatchTrace }, phase: string) => { f.nativeTrace.phase = phase; };
const settle = () => new Promise(resolve => setTimeout(resolve, 320));
const until = async (f: WaitDiagnosticContext, phase: string, condition: () => boolean) => {
  markPhase(f, phase);
  try {
    await vi.waitFor(() => expect(condition()).toBe(true), { timeout: 5_000, interval: 10 });
  } catch (error) {
    await dumpTimeoutDiagnostics(f, phase, error);
    throw error;
  }
};
const fixture = async (fileOnly = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-prefix-"));
  roots.push(root);
  const nativeTrace: NativeWatchTrace = { startedAt: Date.now(), phase: "fixture setup", watcherCount: 0, dropped: 0, events: [] };
  installNativeWatchTrace(nativeTrace);
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite" });
  cleanup.push(() => store.closeState());
  const id = "session:native-00000000";
  const identity = { id, name: "native", kind: "main" as const, sessionId: "native-00000000" };
  const host = { format: 1, id, rootId: id, identity, startedAt: NOW - 10_000, updatedAt: NOW, expiresAt: NOW + 1_000 };
  const participant = { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
    name: "native", status: "idle", runner: "pi", transport: "host", capabilities: ["steer"],
    sessionId: identity.sessionId, startedAt: host.startedAt, updatedAt: NOW, controlProtocol: "v1" };
  const handoff = { format: 1, id: "session:moved-00000000", sessionId: "moved-00000000", name: "moved",
    link: "far", handedOffAt: NOW, operator: "operator", hub: "hub" };
  await store.writeBatch({ identity, ops: [
    { kind: "put", key: key("hosts", id), value: host },
    { kind: "put", key: key("participants", id), value: participant },
    { kind: "put", key: key("handoffs", handoff.id), value: handoff },
  ] });
  const entry = store.get(key("participants", id), { fresh: true })!;
  if (fileOnly) {
    await store.delete({ key: entry.key });
    writeParticipantFile(root, entry);
  }
  const renew = (expiresAt: number) => writeHostLease(root, { id, rootId: id, identityId: id,
    startedAt: host.startedAt, updatedAt: NOW, expiresAt });
  renew(NOW + 60_000);
  const side = new StoreBridgeSide(store, "far", () => NOW);
  const changes: BridgeChange[] = [], failures: Error[] = [];
  cleanup.push(side.subscribeChanges(kind => changes.push(kind), error => failures.push(error)));
  await settle();
  const reads = vi.spyOn(store, "listAll");
  const shared = vi.spyOn(store, "listAllShared");
  const fences = vi.spyOn(store, "withStateWriteFence");
  const clear = () => { reads.mockClear(); shared.mockClear(); fences.mockClear(); changes.length = 0; };
  const expectPrefixes = (expected: string[]) => {
    expect([...new Set(reads.mock.calls.map(call => call[0]))].sort()).toEqual([...expected].sort());
    expect(shared).not.toHaveBeenCalled(); // no full SQLite materialization escape hatch
    expect(failures).toEqual([]);
  };
  return { root, store, side, id, identity, host, participant, handoff, entry, renew, changes, failures, fences, clear, expectPrefixes, nativeTrace };
};

describe("real SQLite bridge prefix invalidation", () => {
  it("does no topology reads for unchanged hints, unrelated commits or checkpoints", async () => {
    const f = await fixture();
    // A real filesystem hint with no logical change (the seeded-before regression).
    const now = new Date();
    markPhase(f, "unchanged hint (state.db utimes)");
    fs.utimesSync(path.join(f.root, "state.db"), now, now);
    await until(f, "unchanged hint (state.db utimes)", () => f.fences.mock.calls.length > 0);
    await settle();
    f.expectPrefixes([]);
    expect(f.changes).toEqual([]);
    f.clear();
    markPhase(f, "unrelated commit");
    await f.store.put({ key: "unrelated/activity", value: { tick: 1 }, identity: f.identity });
    await until(f, "unrelated commit", () => f.fences.mock.calls.length > 0);
    await settle();
    f.expectPrefixes([]);
    expect(f.changes).toEqual([]);
    f.clear();
    const raw = new DatabaseSync(path.join(f.root, "state.db"));
    markPhase(f, "checkpoint");
    try { raw.exec("PRAGMA wal_checkpoint(TRUNCATE)"); } finally { raw.close(); }
    await until(f, "checkpoint", () => f.fences.mock.calls.length > 0);
    await settle();
    f.expectPrefixes([]);
    expect(f.changes).toEqual([]);
  });

  it.each(["same connection", "external connection"])("reloads only the changed prefix through %s", async connection => {
    const f = await fixture();
    const writer = connection === "same connection" ? f.store : await SqliteStateStore.open(f.root, 64 * 1024, 100);
    if (writer !== f.store) cleanup.push(() => (writer as SqliteStateStore).close());
    const mutations = [
      { kind: "hosts", id: f.id, value: { ...f.host, identity: { ...f.identity, name: "renamed" } } },
      { kind: "participants", id: f.id, value: { ...f.participant, status: "working" } },
      { kind: "handoffs", id: f.handoff.id, value: { ...f.handoff, link: "elsewhere" } },
    ];
    for (const mutation of mutations) {
      f.clear();
      const phase = `${connection} connection ${mutation.kind} mutation`;
      markPhase(f, phase);
      await writer.put({ key: key(mutation.kind, mutation.id), value: mutation.value, identity: f.identity });
      await until(f, phase, () => f.changes.includes("presence"));
      await settle();
      f.expectPrefixes([`topology/${mutation.kind}/`]);
      const projection = await f.side.presence(); // public cross-check outside read-count window
      if (mutation.kind === "hosts") expect(projection.hosts[0]!.record.identity.name).toBe("renamed");
      if (mutation.kind === "participants") expect(projection.participants[0]!.status).toBe("working");
      if (mutation.kind === "handoffs") expect(projection.reserved).toContain(f.handoff.id);
    }
  });

  it("observes file-only lease/participant renewal and removal without rereading SQLite prefixes", async () => {
    const f = await fixture(true);
    const revision = f.store.stateRevision();
    markPhase(f, "file-only lease renewal");
    f.renew(NOW + 90_000);
    await until(f, "file-only lease renewal", () => f.changes.includes("lease"));
    f.expectPrefixes([]);
    expect((await f.side.presence()).hosts[0]!.expiresAt).toBe(NOW + 90_000);
    f.clear();
    markPhase(f, "file-only participant renewal");
    writeParticipantFile(f.root, { ...f.entry, value: { ...f.participant, status: "working" } });
    await until(f, "file-only participant renewal", () => f.changes.includes("presence"));
    f.expectPrefixes([]);
    expect((await f.side.presence()).participants[0]!.status).toBe("working");
    f.clear();
    markPhase(f, "file-only participant removal");
    fs.unlinkSync(path.join(f.root, "participants", `${f.entry.key.split("/").at(-1)}.json`));
    await until(f, "file-only participant removal", () => f.changes.includes("presence"));
    f.expectPrefixes([]);
    expect((await f.side.presence()).participants).toEqual([]);
    f.clear();
    markPhase(f, "file-only shortened lease removal");
    fs.unlinkSync(hostLeasePath(f.root, f.id));
    await until(f, "file-only shortened lease removal", () => f.changes.includes("presence")); // a shortened lease is not a renewal
    f.expectPrefixes([]);
    expect((await f.side.presence()).hosts[0]!.expiresAt).toBe(f.host.expiresAt);
    expect(f.store.stateRevision()).toBe(revision);
  });

  it("delivers the last real update in a burst after an earlier notification", async () => {
    const f = await fixture();
    const observed: Array<Promise<string | undefined>> = [];
    cleanup.push(f.side.subscribeChanges(kind => {
      if (kind === "presence") observed.push(f.side.presence().then(value => value.participants[0]?.status));
    }, error => f.failures.push(error)));
    const put = (status: string) => f.store.put({ key: key("participants", f.id), identity: f.identity,
      value: { ...f.participant, status } });
    markPhase(f, "burst first update");
    await put("first");
    await until(f, "burst first notification", () => observed.length > 0);
    markPhase(f, "burst final update writes");
    for (let index = 0; index < 12; index++) await put(`burst-${index}`);
    await until(f, "burst final notification", () => observed.length > 1);
    expect((await Promise.all(observed)).at(-1)).toBe("burst-11");
    expect(f.failures).toEqual([]);
  });

  it("fences an unchanged pre-COMMIT hint and retains its retry when COMMIT supplies no later hint", async () => {
    const watch = fs.watch.bind(fs), watchers: fs.FSWatcher[] = [];
    vi.spyOn(fs, "watch").mockImplementation((...args: Parameters<typeof fs.watch>) => {
      const watcher = watch(...args);
      watchers.push(watcher);
      return watcher;
    });
    const f = await fixture();
    const raw = new DatabaseSync(path.join(f.root, "state.db"));
    try {
      raw.exec("BEGIN IMMEDIATE");
      const stateKey = key("participants", f.id);
      raw.prepare("UPDATE kv SET value = ? WHERE key = ?").run(JSON.stringify({ ...f.participant, status: "committed" }), stateKey);
      raw.exec("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE name = 'commit_no'");
      raw.prepare("INSERT INTO changes(commit_no, key, version, deleted) SELECT CAST(meta.value AS INTEGER), kv.key, kv.version, 0 FROM meta, kv WHERE meta.name = 'commit_no' AND kv.key = ?").run(stateKey);
      const now = new Date();
      markPhase(f, "pre-COMMIT metadata hint and fence contention");
      fs.utimesSync(path.join(f.root, "state.db"), now, now);
      await until(f, "pre-COMMIT metadata hint and fence contention", () => f.fences.mock.results.some(result => result.type === "throw"));
      expect(f.changes).toEqual([]);
      f.expectPrefixes([]); // old rows look unchanged, but the pending writer MUST still be fenced
      // Keep the subscription/retry alive, but remove every native watcher before COMMIT. Thus no
      // post-COMMIT hint can rescue a dropped no-op observation; the latched busy retry must do it.
      for (const watcher of watchers) watcher.close();
      markPhase(f, "pre-COMMIT retained retry after COMMIT");
      raw.exec("COMMIT");
      await until(f, "pre-COMMIT retained retry after COMMIT", () => f.changes.includes("presence"));
      f.expectPrefixes(["topology/participants/"]);
      expect((await f.side.presence()).participants[0]!.status).toBe("committed");
    } finally {
      try { raw.exec("ROLLBACK"); } catch { /* already committed */ }
      raw.close();
    }
  });

  it("conservatively reloads all prefixes when the committed feed no longer covers its baseline", async () => {
    const f = await fixture();
    const raw = new DatabaseSync(path.join(f.root, "state.db"));
    try {
      markPhase(f, "committed-feed gap");
      raw.exec("BEGIN IMMEDIATE");
      raw.prepare("UPDATE kv SET value = ? WHERE key = ?").run(JSON.stringify({ ...f.participant, status: "after-gap" }), key("participants", f.id));
      raw.exec("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE name = 'commit_no'; DELETE FROM changes; COMMIT");
    } finally { raw.close(); }
    await until(f, "committed-feed gap", () => f.changes.includes("presence"));
    f.expectPrefixes(prefixes);
    expect((await f.side.presence()).participants[0]!.status).toBe("after-gap");
  });
});
