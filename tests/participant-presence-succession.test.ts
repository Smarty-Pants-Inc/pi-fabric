// Keep reason: smarty-dev#14115 is a routing-authority/data-safety boundary. Deterministic
// KV interleavings reach lease-loss and CAS races a real Main flow cannot arrange cheaply.
// This is the public KV carrier, not a reimplementation of succession. The companion NATS
// suite exercises authenticated broker CAS and the real FabricRuntimeState Main receiver.
// Deletes are RECORD ONLY: close/rollback never removes an entry, even from this fixture.
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { KvParticipantDirectory, type ParticipantLeaseGate } from "../src/topology/participant-view.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import { KvConflictError, type FabricTransport, type TransportKv, type TransportKvEntry, type TransportKvEvent } from "../src/transport/types.js";

const MACHINE = "machine-a", ROOT = "session:presence-resume", SESSION = "presence-resume";
const OLD = "predecessor-activation", NEXT = "resumed-activation";
const token = (id: string) => createHash("sha256").update(id).digest("hex").slice(0, 32);
const pkey = (id = ROOT) => `${MACHINE}.p.${token(id)}`;
const hkey = (id = ROOT) => `${MACHINE}.h.${token(id)}`;
const clone = <T>(value: T): T => structuredClone(value);
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

/** Atomic CAS plus event-driven snapshot watches; no clocks, TTL simulation or authority logic. */
class RecordOnlyKv implements TransportKv {
  readonly rows = new Map<string, TransportKvEntry<unknown>>();
  readonly deletes: Array<{ key: string; revision: number }> = [];
  readonly mutations: Array<{ key: string; expected: number; revision: number }> = [];
  readonly #watchers = new Set<{ events: TransportKvEvent<unknown>[]; wake(): void }>();
  #revision = 0;
  beforeGet?: (key: string) => Promise<void> | void;
  afterGet?: (key: string, entry: TransportKvEntry<unknown> | undefined) => Promise<void> | void;
  beforePut?: (key: string, expected: number) => Promise<void> | void;
  constructor(readonly ttlMs: number) {}
  seed(key: string, owner: string, value: unknown): TransportKvEntry<unknown> {
    const entry = { key, machine: key.split(".")[0]!, owner, value: clone(value), revision: ++this.#revision };
    this.rows.set(key, entry);
    for (const watcher of this.#watchers) { watcher.events.push({ type: "put", entry: clone(entry) }); watcher.wake(); }
    return clone(entry);
  }
  async get<V>(key: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined> {
    signal?.throwIfAborted();
    await this.beforeGet?.(key);
    const entry = clone(this.rows.get(key));
    await this.afterGet?.(key, entry);
    signal?.throwIfAborted();
    return entry as TransportKvEntry<V> | undefined;
  }
  async put<V>(key: string, owner: string, value: V, expected: number, signal?: AbortSignal): Promise<number> {
    signal?.throwIfAborted();
    await this.beforePut?.(key, expected);
    signal?.throwIfAborted();
    if ((this.rows.get(key)?.revision ?? 0) !== expected) throw new KvConflictError(key, expected);
    const entry = this.seed(key, owner, value);
    this.mutations.push({ key, expected, revision: entry.revision });
    return entry.revision;
  }
  async delete(key: string, revision: number): Promise<boolean> { this.deletes.push({ key, revision }); return false; }
  async *watch<V>(_prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    const watcher = { events: [] as TransportKvEvent<unknown>[], wake: () => {} };
    this.#watchers.add(watcher);
    const abort = () => watcher.wake();
    signal.addEventListener("abort", abort, { once: true });
    try {
      for (const entry of this.rows.values()) yield { type: "put", entry: clone(entry) as TransportKvEntry<V> };
      yield { type: "ready" };
      while (!signal.aborted) {
        const next = deferred(); watcher.wake = next.resolve;
        if (!watcher.events.length) await next.promise;
        while (!signal.aborted && watcher.events.length) yield watcher.events.shift()! as TransportKvEvent<V>;
      }
    } finally { signal.removeEventListener("abort", abort); this.#watchers.delete(watcher); }
  }
}

function root(incarnation = NEXT, change: Partial<FabricParticipantRecord> = {}): FabricParticipantRecord {
  return { format: 1, id: ROOT, rootId: ROOT, kind: "root", ownerHostId: ROOT, ownerIdentityId: ROOT,
    ownerIncarnation: incarnation, sessionId: SESSION, name: "resumed-main", status: "idle", runner: "pi",
    transport: "host", capabilities: ["steer", "followUp"], controlProtocol: "v1",
    startedAt: incarnation === OLD ? 1_000 : 2_000, updatedAt: incarnation === OLD ? 1_100 : 2_100, ...change };
}
function hostLease(incarnation = NEXT, change: Partial<FabricKvHostLease> = {}): FabricKvHostLease {
  return { id: ROOT, identityId: ROOT, rootId: ROOT, incarnation, pid: process.pid,
    pidStartTime: "12345", bootId: "fixture-boot", updatedAt: 2_000, expiresAt: 17_000, ...change };
}
const opened: KvParticipantDirectory[] = [];
afterEach(async () => { await Promise.all(opened.splice(0).map(view => view.close())); });
function fixture() {
  const presence = new RecordOnlyKv(600_000), leases = new RecordOnlyKv(15_000);
  let live = true;
  const abort = new AbortController();
  const gate: ParticipantLeaseGate = { live: () => live, signal: abort.signal };
  const transport: FabricTransport = { kind: "nats", self: { machine: MACHINE, hostId: ROOT },
    kv: bucket => bucket === "presence" ? presence : leases,
    async send() { throw new Error("KV-only seam must not send"); }, async *inbox() { throw new Error("KV-only seam must not receive"); },
    async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {} };
  const view = (incarnation = NEXT) => {
    const value = new KvParticipantDirectory({ transport, lease: gate, identityId: ROOT, rootId: ROOT,
      ownerIncarnation: incarnation, operationTimeoutMs: 1_000 });
    opened.push(value); value.start(); return value;
  };
  const stale = (change: Partial<FabricParticipantRecord> = {}) => {
    const record = root(OLD, change);
    return presence.seed(pkey(), `${record.ownerHostId}#${record.ownerIncarnation}`, record);
  };
  const lease = (incarnation = NEXT, change: Partial<FabricKvHostLease> = {}) => {
    const value = hostLease(incarnation, change);
    return leases.seed(hkey(), `${value.id}#${value.incarnation}`, value);
  };
  const observer = () => {
    const value = new KvParticipantDirectory({ transport: { ...transport, self: { machine: "machine-b", hostId: "observer-host" } },
      lease: gate, identityId: "observer", rootId: "observer", ownerIncarnation: "observer-activation" });
    opened.push(value); value.start(); return value;
  };
  return { presence, leases, view, observer, stale, lease, abort, lose: () => { live = false; } };
}
async function ready(view: KvParticipantDirectory) {
  const signal = AbortSignal.timeout(1_000);
  while (!view.ready) await view.whenChanged(ROOT, signal);
}
async function liveRoute(view: KvParticipantDirectory) {
  await ready(view);
  const signal = AbortSignal.timeout(1_000);
  while (!view.resolve(ROOT)?.live) await view.whenChanged(ROOT, signal);
  return view.resolve(ROOT)!;
}

// Acceptance ledger: initial own Main only; stale same-lineage replacement; valid current
// host lease read directly; monotonic live gate rechecked across awaits; one fenced CAS;
// refusal leaves exact bytes/revision intact; child admission and normal create unchanged.
describe("#14115 initial own Main presence succession", () => {
  it("fresh create stays create-only and becomes routable", async () => {
    const f = fixture(); f.lease(); const view = f.view();
    const publication = await view.publish(root());
    expect(f.presence.rows.get(pkey())).toMatchObject({ owner: `${ROOT}#${NEXT}`, revision: publication.revision, value: root() });
    expect(await liveRoute(view)).toMatchObject({ live: true, ownerIncarnation: NEXT });
    expect(f.presence.mutations).toEqual([{ key: pkey(), expected: 0, revision: publication.revision }]);
  });

  it("preserves placement run-id metadata through the authenticated NATS presence projection", async () => {
    const f = fixture(); f.lease(); const view = f.view();
    await view.publish(root(NEXT, { placementRunId: "sandbox-run-1" }));
    expect(await liveRoute(view)).toMatchObject({ live: true });
    expect(view.get(ROOT)).toMatchObject({ id: ROOT, placementRunId: "sandbox-run-1" });
  });

  it.each(["idle", "stopped"])("resumes the same Main immediately while its %s predecessor presence remains inside the 600s TTL", async status => {
    const f = fixture(), previous = f.stale({ status }); f.lease();
    const view = f.view(); await ready(view);
    expect(view.resolve(ROOT)).toMatchObject({ live: false, ownerIncarnation: OLD });
    const publication = await view.publish(root());
    expect(publication.revision).toBeGreaterThan(previous.revision);
    expect(await liveRoute(view)).toMatchObject({ live: true, ownerIdentityId: ROOT, ownerIncarnation: NEXT });
    expect(f.presence.rows.get(pkey())?.value).toEqual(root());
    expect(f.presence.mutations).toEqual([{ key: pkey(), expected: previous.revision, revision: publication.revision }]);
    expect(f.presence.deletes).toEqual([]);
  });

  it("uses the acquired exclusive lease, not unordered predecessor wall-clock timestamps, as succession proof", async () => {
    const f = fixture(); f.stale({ startedAt: 3_000, updatedAt: 3_100 }); f.lease();
    const view = f.view(); await view.publish(root());
    expect(f.presence.rows.get(pkey())?.value).toEqual(root());
    expect(await liveRoute(view)).toMatchObject({ live: true, ownerIncarnation: NEXT });
  });

  it.each([OLD, "newer-predecessor-activation"])("does not displace a predecessor with a valid live %s host lease", async incarnation => {
    const f = fixture(), previous = f.stale({ ownerIncarnation: incarnation,
      ...(incarnation === OLD ? {} : { startedAt: 3_000, updatedAt: 3_100 }) });
    f.lease(incarnation); const view = f.view(), observer = f.observer();
    expect(await liveRoute(observer)).toMatchObject({ live: true, ownerIncarnation: incarnation });
    await expect(view.publish(root())).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
    expect(observer.resolve(ROOT)).toMatchObject({ live: true, ownerIncarnation: incarnation });
    expect(f.presence.rows.get(pkey())).toEqual(previous);
    expect(f.presence.deletes).toEqual([]);
  });

  it.each([
    ["different identity", { ownerIdentityId: "session:someone-else" }],
    ["different session", { sessionId: "another-native-session" }],
    ["different root lineage", { rootId: "session:another-root" }],
    ["different owner host", { ownerHostId: "another-host" }],
    ["non-root predecessor", { kind: "agent" }],
    ["same incarnation", { ownerIncarnation: NEXT }],
  ] as const)("refuses %s without touching presence", async (_label, change) => {
    const f = fixture(), previous = f.stale(change); f.lease();
    await expect(f.view().publish(root())).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID" });
    expect(f.presence.rows.get(pkey())).toEqual(previous);
    expect(f.presence.deletes).toEqual([]);
  });

  it.each([
    ["missing", undefined],
    ["foreign identity", { identityId: "session:other" }],
    ["foreign lineage", { rootId: "session:other" }],
    ["other incarnation", { incarnation: OLD }],
    ["invalid process proof", { pid: 0 }],
  ] as const)("requires a directly read valid current lease (%s)", async (_label, change) => {
    const f = fixture(), previous = f.stale();
    if (change) f.lease("incarnation" in change ? change.incarnation : NEXT, change);
    await expect(f.view().publish(root())).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
    expect(f.presence.rows.get(pkey())).toEqual(previous);
    expect(f.presence.deletes).toEqual([]);
  });

  it("does not trust a ready watch's cached current lease after the authoritative lease changed", async () => {
    const f = fixture(), previous = f.stale(); f.lease(); const view = f.view(); await ready(view);
    // Suppress only the notification: model a watch lag, NOT a successful lease read.
    f.leases.rows.set(hkey(), { ...f.leases.rows.get(hkey())!, owner: `${ROOT}#${OLD}`, value: hostLease(OLD) });
    await expect(view.publish(root())).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
    expect(f.presence.rows.get(pkey())).toEqual(previous);
  });

  it.each(["presence read", "lease read"])("cannot publish after ownership is lost across the %s await", async boundary => {
    const f = fixture(), previous = f.stale(); f.lease();
    const kv = boundary === "presence read" ? f.presence : f.leases;
    kv.afterGet = () => { f.lose(); };
    await expect(f.view().publish(root())).rejects.toBeDefined();
    expect(f.presence.rows.get(pkey())).toEqual(previous);
    expect(f.presence.deletes).toEqual([]);
  });

  it.each(["replacement", "same-owner new revision"])("undoes the takeover when the post-CAS lease read sees a %s", async change => {
    const f = fixture(), previous = f.stale(); f.lease();
    f.presence.beforePut = (_key, expected) => {
      if (expected === previous.revision) f.lease(change === "replacement" ? "later-owner" : NEXT);
    };
    const view = f.view();
    await expect(view.publish(root())).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
    const restored = f.presence.rows.get(pkey())!;
    expect(restored).toMatchObject({ owner: previous.owner, value: previous.value });
    expect(f.presence.mutations).toEqual([
      { key: pkey(), expected: previous.revision, revision: restored.revision - 1 },
      { key: pkey(), expected: restored.revision - 1, revision: restored.revision },
    ]);
    await ready(view);
    expect(view.resolve(ROOT)).toMatchObject({ live: false });
    await view.close();
    expect(f.presence.deletes).toEqual([]); // A refused takeover was never remembered as owned.
    expect(f.presence.rows.get(pkey())).toEqual(restored);
  });

  it.each(["unavailable", "local cutoff", "local abort"])("undoes and refuses when post-CAS validation encounters %s", async failure => {
    const f = fixture(), previous = f.stale(); f.lease();
    let reads = 0;
    f.leases.afterGet = () => {
      if (++reads !== 2) return;
      if (failure === "unavailable") throw new Error("authoritative lease read unavailable");
      if (failure === "local abort") f.abort.abort(new Error("lease lost after CAS"));
      else f.lose();
    };
    const view = f.view();
    await expect(view.publish(root())).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
    expect(f.presence.rows.get(pkey())).toMatchObject({ owner: previous.owner, value: previous.value });
    expect(f.presence.mutations).toHaveLength(2);
    expect(f.presence.mutations[1]!.expected).toBe(f.presence.mutations[0]!.revision);
    await view.close();
    expect(f.presence.deletes).toEqual([]);
  });

  it("leaves the rightful new lease holder untouched if the fenced undo loses", async () => {
    const f = fixture(), previous = f.stale(); f.lease();
    let competitor: TransportKvEntry<unknown> | undefined;
    f.presence.beforePut = (_key, expected) => {
      if (expected === previous.revision) f.lease("rightful-new-owner");
      else if (expected > previous.revision) competitor = f.presence.seed(pkey(), `${ROOT}#rightful-new-owner`, root("rightful-new-owner"));
    };
    const view = f.view(), observer = f.observer();
    await expect(view.publish(root())).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_UNAVAILABLE" });
    expect(competitor).toBeDefined();
    expect(f.presence.rows.get(pkey())).toEqual(competitor);
    expect(f.presence.mutations).toHaveLength(1); // No retry/adoption after the undo conflict.
    expect(await liveRoute(observer)).toMatchObject({ live: true, ownerIncarnation: "rightful-new-owner" });
    await view.close();
    expect(f.presence.rows.get(pkey())).toEqual(competitor);
    expect(f.presence.deletes).toEqual([]);
  });

  it.each([
    ["host", { id: "another-host" }],
    ["root", { rootId: "another-root" }],
    ["identity", { identityId: "another-identity" }],
    ["incarnation", { incarnation: "another-incarnation" }],
  ] as const)("readers never treat presence as live with a mismatched lease %s", async (_field, change) => {
    const f = fixture();
    f.presence.seed(pkey(), `${ROOT}#${NEXT}`, root());
    f.lease(NEXT, change);
    const observer = f.observer(); await ready(observer);
    expect(observer.resolve(ROOT)).toMatchObject({ live: false });
    expect(observer.get(ROOT)).toBeUndefined();
    expect(observer.list()).toEqual([]);
    expect(observer.list({ includeStale: true })).toMatchObject([{ id: ROOT, stale: true }]);
    f.lease();
    expect(await liveRoute(observer)).toMatchObject({ live: true, ownerIncarnation: NEXT });
    expect(observer.get(ROOT)).toMatchObject({ id: ROOT, stale: false });
  });

  it.each(["owner", "machine", "absent"])("readers require the exact authenticated lease entry (%s)", async mismatch => {
    const f = fixture();
    f.presence.seed(pkey(), `${ROOT}#${NEXT}`, root());
    if (mismatch !== "absent") f.leases.seed(mismatch === "machine" ? hkey().replace(MACHINE, "machine-c") : hkey(),
      mismatch === "owner" ? `${ROOT}#another-owner` : `${ROOT}#${NEXT}`, hostLease());
    const observer = f.observer(); await ready(observer);
    expect(observer.resolve(ROOT)).toMatchObject({ live: false });
    expect(observer.get(ROOT)).toBeUndefined();
    expect(observer.list()).toEqual([]);
    f.lease();
    expect(await liveRoute(observer)).toMatchObject({ live: true, ownerIncarnation: NEXT });
  });

  it("does not retry/adopt a concurrent replacement after its read revision loses the CAS", async () => {
    const f = fixture(); f.stale(); f.lease();
    let competitor: TransportKvEntry<unknown> | undefined;
    f.presence.beforePut = (_key, revision) => {
      if (revision > 0 && !competitor) competitor = f.presence.seed(pkey(), `${ROOT}#winner`, root("winner", { name: "concurrent-winner" }));
    };
    await expect(f.view().publish(root())).rejects.toMatchObject({ code: "FABRIC_KV_CONFLICT" });
    expect(competitor, "the succession CAS must be reached, not rejected before the race").toBeDefined();
    expect(f.presence.rows.get(pkey())).toEqual(competitor);
    expect(f.presence.mutations).toEqual([]);
    expect(f.presence.deletes).toEqual([]);
  });

  it("two concurrent initial publishers of one acquired incarnation have exactly one CAS winner", async () => {
    const f = fixture(); const previous = f.stale(); f.lease();
    const bothRead = deferred(); let reads = 0;
    f.presence.afterGet = async () => { if (++reads === 2) bothRead.resolve(); await bothRead.promise; };
    const values = [root(NEXT, { name: "resume-a" }), root(NEXT, { name: "resume-b" })];
    const results = await Promise.allSettled(values.map(value => f.view().publish(value)));
    expect(results.filter(result => result.status === "fulfilled")).toHaveLength(1);
    expect(results.filter(result => result.status === "rejected")).toHaveLength(1);
    expect(results.find(result => result.status === "rejected")).toMatchObject({ status: "rejected", reason: { code: "FABRIC_KV_CONFLICT" } });
    const winner = results.findIndex(result => result.status === "fulfilled");
    expect(f.presence.rows.get(pkey())?.value).toEqual(values[winner]);
    expect(f.presence.mutations).toMatchObject([{ key: pkey(), expected: previous.revision }]);
    expect(f.presence.deletes).toEqual([]);
  });

  it("different incarnation resumers cannot both succeed under the one current host lease", async () => {
    const f = fixture(); f.stale(); f.lease();
    const results = await Promise.allSettled([f.view().publish(root()), f.view("loser").publish(root("loser"))]);
    expect(results.map(result => result.status)).toEqual(["fulfilled", "rejected"]);
    expect(f.presence.rows.get(pkey())?.value).toEqual(root());
  });

  it("prelaunch child create never inherits the Main-only takeover permission", async () => {
    const f = fixture(); f.lease();
    const child = root(NEXT, { id: "child:run", kind: "agent", parentId: ROOT });
    const previous = f.presence.seed(pkey(child.id), `${ROOT}#${OLD}`, { ...child, ownerIncarnation: OLD });
    await expect(f.view().publishBeforeLaunch(child)).rejects.toBeDefined();
    expect(f.presence.rows.get(pkey(child.id))).toEqual(previous);
    expect(f.presence.deletes).toEqual([]);
  });

  it("prelaunch admission cannot take over even an otherwise eligible own root", async () => {
    const f = fixture(), previous = f.stale(); f.lease();
    await expect(f.view().publishBeforeLaunch(root())).rejects.toBeDefined();
    expect(f.presence.rows.get(pkey())).toEqual(previous);
    expect(f.presence.deletes).toEqual([]);
  });

  it("fresh child prelaunch still creates a routable participant", async () => {
    const f = fixture(); f.lease(); const view = f.view();
    const child = root(NEXT, { id: "child:fresh", kind: "agent", parentId: ROOT });
    const receipt = await view.publishBeforeLaunch(child);
    await ready(view);
    const signal = AbortSignal.timeout(1_000);
    while (!view.resolve(child.id)?.live) await view.whenChanged(child.id, signal);
    expect(view.get(child.id)).toMatchObject({ id: child.id, ownerIncarnation: NEXT, stale: false });
    expect(f.presence.rows.get(pkey(child.id))).toMatchObject({ revision: receipt.revision, value: child });
    expect(f.presence.mutations).toEqual([{ key: pkey(child.id), expected: 0, revision: receipt.revision }]);
  });

  it("does not expand succession to an already-owned update whose revision was replaced", async () => {
    const f = fixture(); f.lease(); const view = f.view(); await view.publish(root());
    const replacement = f.stale({ name: "foreign-replacement-after-initial-create" });
    await expect(view.publish(root(NEXT, { name: "changed-local-name" }))).rejects.toBeDefined();
    expect(f.presence.rows.get(pkey())).toEqual(replacement);
    expect(f.presence.deletes).toEqual([]);
  });

  it.each(["presence owner", "lease owner", "presence schema", "missing predecessor session"])("refuses malformed or unproven predecessor evidence (%s)", async damage => {
    const f = fixture(); f.stale(); f.lease();
    if (damage === "presence owner") f.presence.rows.set(pkey(), { ...f.presence.rows.get(pkey())!, owner: `${ROOT}#not-the-record-owner` });
    if (damage === "lease owner") f.leases.rows.set(hkey(), { ...f.leases.rows.get(hkey())!, owner: `${ROOT}#not-the-lease-owner` });
    if (damage === "presence schema") f.presence.rows.set(pkey(), { ...f.presence.rows.get(pkey())!, value: { ...root(OLD), controlProtocol: "legacy" } });
    if (damage === "missing predecessor session") f.presence.rows.set(pkey(), { ...f.presence.rows.get(pkey())!, value: { ...root(OLD), sessionId: undefined } });
    const previous = clone(f.presence.rows.get(pkey()));
    await expect(f.view().publish(root())).rejects.toBeDefined();
    expect(f.presence.rows.get(pkey())).toEqual(previous);
    expect(f.presence.deletes).toEqual([]);
  });

  it("ordinary child publication cannot use Main-only takeover either", async () => {
    const f = fixture(); f.lease();
    const child = root(NEXT, { id: "actor:run", kind: "actor", parentId: ROOT });
    const previous = f.presence.seed(pkey(child.id), `${ROOT}#${OLD}`, { ...child, ownerIncarnation: OLD });
    await expect(f.view().publish(child)).rejects.toBeDefined();
    expect(f.presence.rows.get(pkey(child.id))).toEqual(previous);
  });
});
