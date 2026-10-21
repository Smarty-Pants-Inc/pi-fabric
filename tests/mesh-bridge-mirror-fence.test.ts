import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide, type StoreBridgeMirrorPhaseObservation, type StoreBridgeSideOptions } from "../src/mesh/bridge.js";
import { COMMIT_OUTBOX_PREFIX, CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshSnapshotRetriesExhaustedError, MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { hostLeasePath, readHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: #15310's actual bridge writer slow tail, and the trust/data-safety boundary
// between optimistic topology admission and the real SQLite prepare/effects. No mocked proof,
// holds(), admission or fence result. fleet-isolation-setup supplies the normal Node SQLite fixture.
const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
const roots: string[] = [];
const cleanups: Array<() => void> = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0).reverse()) cleanup();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const peer = (name: string, now: number) => {
  const id = `session:${name}-00000000`;
  const identity: MeshIdentity = { id, name, kind: "main", sessionId: name };
  const host: FabricHostRecord = { format: 1, id, rootId: id, identity,
    startedAt: now, updatedAt: now, expiresAt: now + 60_000, incarnationToken: `${name}-A` };
  const participant: FabricParticipantRecord = { format: 1, id, kind: "root", rootId: id,
    ownerHostId: id, ownerIdentityId: id, name, label: name, status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer"], sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1" };
  return { id, identity, host, participant,
    presence: { hosts: [{ record: host, expiresAt: host.expiresAt, observedAt: now }], participants: [participant] } };
};

const fixture = async (backend: "sqlite" | "file" = "sqlite", warm = true, options: StoreBridgeSideOptions = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-mirror-fence-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "hub"), 64 * 1024, 100, { stateBackend: backend });
  cleanups.push(() => mesh.closeState());
  expect(mesh.stateBackend).toBe(backend);
  const now = Date.now(), source = peer("mirror-source", now), native = peer("mirror-native", now);
  let clock = now;
  const side = new StoreBridgeSide(mesh, "far", () => clock, false, options);
  await mesh.put({ key: key("hosts", native.id), value: native.host, identity: native.identity });
  await mesh.put({ key: key("participants", native.id), value: native.participant, identity: native.identity });
  // An invalid handoff is reserved state, not authority to remove a real native's lease.
  await mesh.put({ key: key("handoffs", native.id), value: { id: native.id }, identity: native.identity });
  await mesh.put({ key: "probe/unrelated", value: 0, identity: native.identity });
  if (warm) await side.mirror(source.presence); // initial replay is deliberately outside the steady budget
  const raw = backend === "sqlite" ? new sqlite.DatabaseSync(path.join(mesh.root, "state.db")) : undefined;
  if (raw) {
    raw.exec("PRAGMA busy_timeout = 0");
    cleanups.push(() => { if (raw.isTransaction) raw.exec("ROLLBACK"); raw.close(); });
  }
  let externalCommits = 0;
  // Same independent native writer pattern as capture-fence: real kv + complete change-feed,
  // high-water, commit and byte accounting, without touching any production proof implementation.
  const write = (stateKey: string, value: unknown) => {
    if (!raw) throw new Error("SQLite writer required");
    raw.exec("BEGIN IMMEDIATE");
    try {
      const prior = raw.prepare("SELECT bytes, updated_by FROM kv WHERE key = ?").get(stateKey)!;
      const text = JSON.stringify(value), by = String(prior.updated_by);
      const bytes = Buffer.byteLength(text) + Buffer.byteLength(by) + 3 * Buffer.byteLength(stateKey) + 96;
      const revision = Number(raw.prepare("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE name = 'high_water' RETURNING value").get()!.value);
      const commit = Number(raw.prepare("UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE name = 'commit_no' RETURNING value").get()!.value);
      raw.prepare("UPDATE kv SET value = ?, version = ?, updated_at = ?, bytes = ? WHERE key = ?")
        .run(text, revision, now, bytes, stateKey);
      raw.prepare("UPDATE meta SET value = CAST(value AS INTEGER) + ? WHERE name = 'state_bytes'").run(bytes - Number(prior.bytes));
      raw.prepare("INSERT INTO changes(commit_no, key, version, deleted) VALUES (?, ?, ?, 0)").run(commit, stateKey, revision);
      raw.exec("COMMIT");
      externalCommits += 1;
    } catch (error) { raw.exec("ROLLBACK"); throw error; }
  };
  const nextPresence = () => {
    clock = now + 1_000;
    return { hosts: [{ record: { ...source.host, updatedAt: clock, expiresAt: clock + 60_000 },
      expiresAt: clock + 60_000, observedAt: clock }],
      participants: [{ ...source.participant, label: "fresh-source", updatedAt: clock }] };
  };
  return { root, mesh, side, raw, now, source, native, write, nextPresence,
    externalCommits: () => externalCommits };
};
type Fixture = Awaited<ReturnType<typeof fixture>>;

// Observe only REAL StatementSync range reads during the acquired prepare callback. Neither
// optimistic/pre-lock scans nor afterCommit scans are charged to this preparation budget.
const observe = (f: Fixture) => {
  let inside = false, pending = false, injecting = false;
  let before: (() => void) | undefined;
  const stats = { attempts: 0, scans: 0, afterCommits: 0, acquired: 0 };
  const exec = sqlite.DatabaseSync.prototype.exec;
  vi.spyOn(sqlite.DatabaseSync.prototype, "exec").mockImplementation(function (this: InstanceType<typeof sqlite.DatabaseSync>, sql: string) {
    const statement = sql.trim().toUpperCase();
    // Writer ingress: commit on the independent connection AFTER the early plan, immediately
    // before this batch takes its actual SQLite writer. Never attempt a commit inside its lock.
    if (statement === "BEGIN IMMEDIATE" && pending && !injecting && this !== f.raw &&
        this.location() === path.join(f.mesh.root, "state.db")) {
      pending = false;
      injecting = true;
      try { before?.(); } finally { injecting = false; }
    }
    return exec.call(this, sql);
  });
  type All = (this: { sourceSQL: string }, ...args: unknown[]) => unknown[];
  const statements = sqlite.StatementSync.prototype as unknown as { all: All };
  const all = statements.all;
  vi.spyOn(statements, "all").mockImplementation(function (this: { sourceSQL: string }, ...args: unknown[]) {
    if (inside && this.sourceSQL.includes("key >= ?") && typeof args[0] === "string" &&
        /^topology\/(hosts|participants|handoffs)\//.test(args[0])) stats.scans += 1;
    return all.apply(this, args);
  });
  const writeBatch = f.mesh.writeBatch.bind(f.mesh);
  vi.spyOn(f.mesh, "writeBatch").mockImplementation(async input => {
    if (input.lockClass !== "bridge" || !input.prepare) return writeBatch(input);
    pending = true;
    const { prepare, afterCommit } = input;
    try {
      return await writeBatch({ ...input, prepare: view => {
        stats.attempts += 1;
        if (stats.attempts > 6) throw new Error("unbounded mirror retry (test safety bound)");
        if (f.raw) {
          // A second writer must really be excluded, not merely a mocked fence callback.
          let busy = false;
          try { f.raw.exec("BEGIN IMMEDIATE"); f.raw.exec("ROLLBACK"); }
          catch (error) { busy = ((error as { errcode?: number }).errcode! & 0xff) === 5; }
          expect(busy).toBe(true);
          stats.acquired += 1;
        }
        inside = true;
        try { return prepare(view); } finally { inside = false; }
      }, ...(afterCommit ? { afterCommit: (view: Parameters<typeof afterCommit>[0]) => {
        stats.afterCommits += 1;
        return afterCommit(view);
      } } : {}) });
    } finally { pending = false; }
  });
  return { stats, setBefore: (callback?: () => void) => { before = callback; },
    reset: () => { Object.assign(stats, { attempts: 0, scans: 0, afterCommits: 0, acquired: 0 }); } };
};
const ownedState = (f: Fixture) => f.mesh.listAll("", { fresh: true }).filter(entry =>
  entry.key === key("hosts", f.source.id) || entry.key === key("participants", f.source.id) || entry.key.startsWith(COMMIT_OUTBOX_PREFIX));
const leaseBytes = (f: Fixture) => fs.readFileSync(hostLeasePath(f.mesh.root, f.source.id));
const send = (f: Fixture, text: string) => f.side.publish({ topic: "fleet.work.mirror-fence", kind: "ask",
  from: f.source.identity, to: f.native.id, text, data: { bridge: { from: "far", id: `mirror-${text}` } } }, [f.source.id]);

describe("passive mirror phase observers", () => {
  it("reports positive finite recovery and warmed SQLite mirror phases", async () => {
    const observations: StoreBridgeMirrorPhaseObservation[] = [];
    const f = await fixture("sqlite", false, { observeMirrorPhase: observation => { observations.push(observation); } });
    await expect(f.side.mirror(f.source.presence)).resolves.toBeUndefined();
    expect(new Set(observations.map(({ phase }) => phase))).toEqual(new Set(["compute", "leaseRead"]));
    for (const { holdMs } of observations) {
      expect(Number.isFinite(holdMs)).toBe(true);
      expect(holdMs).toBeGreaterThan(0);
    }
    expect(readHostLease(f.mesh.root, f.source.id)).toBeDefined();
    observations.length = 0;
    await expect(f.side.mirror(f.nextPresence())).resolves.toBeUndefined();
    expect(new Set(observations.map(({ phase }) => phase))).toEqual(new Set(["compute", "leaseRead", "leaseDigest"]));
    for (const { holdMs } of observations) {
      expect(Number.isFinite(holdMs)).toBe(true);
      expect(holdMs).toBeGreaterThan(0);
    }
    expect(f.side.holds(f.source.id, f.source.identity)).toBe(true);
    expect(f.mesh.get(key("participants", f.source.id), { fresh: true })?.value).toMatchObject({ label: "fresh-source" });
  });

  it("ignores observer throws without changing real mirror and publication results", async () => {
    const phases = new Set<StoreBridgeMirrorPhaseObservation["phase"]>();
    const f = await fixture("sqlite", false, { observeMirrorPhase: observation => {
      phases.add(observation.phase);
      throw new Error("advisory phase observer failed");
    } });
    await expect(f.side.mirror(f.source.presence)).resolves.toBeUndefined();
    await expect(f.side.mirror(f.nextPresence())).resolves.toBeUndefined();
    expect(phases).toEqual(new Set(["compute", "leaseRead", "leaseDigest"]));
    expect(f.side.holds(f.source.id, f.source.identity)).toBe(true);
    expect(f.mesh.get(key("hosts", f.source.id), { fresh: true })?.value).toMatchObject({ id: f.source.id, remoteHost: "far" });
    expect(f.mesh.get(key("participants", f.source.id), { fresh: true })?.value).toMatchObject({ label: "fresh-source" });
    expect(f.mesh.get(key("hosts", f.native.id), { fresh: true })?.value).toMatchObject({ id: f.native.id });
    expect(readHostLease(f.mesh.root, f.source.id)).toMatchObject({ incarnationToken: f.source.host.incarnationToken,
      mirrorLease: 1, updatedAt: f.now + 1_000, mirror: { remoteHost: "far", sourceObservedAt: f.now + 1_000 } });
    const first = await send(f, "observer-first"), second = await send(f, "observer-second");
    expect(first).toMatchObject({ sequence: expect.any(Number) });
    expect(second.sequence).toBeGreaterThan(first.sequence);
    expect(f.mesh.read({ limit: 100 }).filter(event => event.topic === "fleet.work.mirror-fence").map(event => event.text))
      .toEqual(["observer-first", "observer-second"]);
  });
});

// Three intended RED cases on the passive-instrumentation HEAD: its third steady attempt
// performs a full topology compute and effects instead of a finite typed no-effects refusal.
describe("steady SQLite mirror preparation has finite optimistic custody (#15310)", () => {
  it.each(["topology", "file-addition", "file-update"] as const)("three real %s invalidations refuse without fenced topology scans or effects, then useful progress resumes", async mode => {
    const f = await fixture();
    const fileEntry = (id: string, version: number): MeshStateEntry => ({ key: key("participants", id), version,
      updatedAt: f.now + version, updatedBy: f.native.identity,
      value: { ...f.native.participant, id, rootId: id, ownerHostId: id, ownerIdentityId: id, status: `running-${version}` } });
    if (mode === "file-update") writeParticipantFile(f.mesh.root, fileEntry("session:existing-file-00000000", 1));
    const initialFiles = fs.existsSync(path.join(f.mesh.root, "participants")) ? fs.readdirSync(path.join(f.mesh.root, "participants")) : [];
    const beforeState = ownedState(f), beforeLease = leaseBytes(f), sequence = f.mesh.latestSequence();
    const observed = observe(f);
    let mutations = 0;
    observed.setBefore(() => {
      mutations += 1;
      const revision = f.mesh.stateRevision();
      if (mode === "topology") {
        const kinds = ["hosts", "participants", "handoffs"] as const;
        const kind = kinds[(mutations - 1) % kinds.length]!;
        const value = kind === "hosts" ? f.native.host : kind === "participants" ? f.native.participant : { id: f.native.id };
        f.write(key(kind, f.native.id), { ...value, ingress: mutations });
        expect(f.mesh.stateRevision()).not.toBe(revision);
      } else {
        const id = mode === "file-update" ? "session:existing-file-00000000" : `session:added-file-${mutations}`;
        writeParticipantFile(f.mesh.root, fileEntry(id, mutations + 1));
        // Atomic updates move the directory; make the difference explicit on coarse clocks too.
        const seconds = f.now / 1000 + mutations;
        fs.utimesSync(path.join(f.mesh.root, "participants"), seconds, seconds);
        expect(f.mesh.stateRevision(), "file-only invalidation has no supporting SQL commit").toBe(revision);
      }
    });
    let refusal: unknown;
    try { await f.side.mirror(f.nextPresence()); } catch (error) { refusal = error; }
    console.log(JSON.stringify({ mirrorFence: mode, mutations, externalCommits: f.externalCommits(), ...observed.stats,
      refusal: refusal instanceof Error ? refusal.name : null }));
    expect.soft(refusal).toBeInstanceOf(MeshSnapshotRetriesExhaustedError);
    expect.soft(refusal).toMatchObject({ attempts: 3 });
    expect.soft(mutations).toBe(3);
    expect.soft(observed.stats.attempts).toBe(3);
    expect.soft(observed.stats.acquired).toBe(3);
    expect.soft(observed.stats.scans, "third steady fallback must not scan under prepare custody").toBe(0);
    expect.soft(observed.stats.afterCommits).toBe(0);
    expect.soft(ownedState(f), "no mirror row, version or outbox recording on refusal").toEqual(beforeState);
    expect.soft(leaseBytes(f), "no lease effect on refusal").toEqual(beforeLease);
    expect.soft(f.mesh.latestSequence()).toBe(sequence);
    expect(f.externalCommits()).toBe(mode === "topology" ? 3 : 0);
    if (mode === "file-update") expect(fs.readdirSync(path.join(f.mesh.root, "participants"))).toEqual(initialFiles);
    // A transient exhaustion must not latch stale admission. Remove churn, retry the REAL mirror,
    // then publish actual commands through actual ownership admission and the event append path.
    observed.setBefore();
    observed.reset();
    await f.side.mirror(f.nextPresence());
    expect(observed.stats.attempts).toBe(1);
    expect(observed.stats.scans).toBe(0);
    expect(f.side.holds(f.source.id, f.source.identity)).toBe(true);
    expect(readHostLease(f.mesh.root, f.source.id)).toMatchObject({ incarnationToken: f.source.host.incarnationToken,
      mirrorLease: 1, updatedAt: f.now + 1_000, mirror: { remoteHost: "far", sourceObservedAt: f.now + 1_000 } });
    expect(readHostLease(f.mesh.root, f.source.id)!.expiresAt).toBeGreaterThan(f.now + 1_000);
    const first = await send(f, "first"), second = await send(f, "second");
    expect(second.sequence).toBeGreaterThan(first.sequence);
    expect(f.mesh.read({ after: sequence, limit: 100 }).filter(event => event.topic === "fleet.work.mirror-fence").map(event => event.text))
      .toEqual(["first", "second"]);
  });

  it("an unrelated native commit passes one proof; one relevant invalidation replans and succeeds without a locked scan", async () => {
    for (const relevant of [false, true]) {
      const f = await fixture(), observed = observe(f);
      observed.setBefore(() => {
        observed.setBefore();
        f.write(relevant ? key("hosts", f.native.id) : "probe/unrelated", relevant ? { ...f.native.host, ingress: 1 } : 1);
      });
      await f.side.mirror(f.nextPresence());
      expect(f.externalCommits()).toBe(1);
      expect(observed.stats.attempts).toBe(relevant ? 2 : 1);
      expect(observed.stats.acquired).toBe(relevant ? 2 : 1);
      expect(observed.stats.afterCommits).toBe(1);
      expect(observed.stats.scans).toBe(0);
      expect(f.side.holds(f.source.id, f.source.identity)).toBe(true);
      await send(f, "control");
      expect(f.mesh.read({ limit: 100 }).filter(event => event.topic === "fleet.work.mirror-fence").map(event => event.text)).toEqual(["control"]);
      vi.restoreAllMocks();
    }
  });

  it("initial complete recovery really replays a crash row, and final withdrawal retains its authoritative fallback", async () => {
    const f = await fixture("sqlite", false);
    const identity: MeshIdentity = { id: "bridge:far", name: "far", kind: "main" };
    const outbox = new CommitOutbox(f.mesh, "bridge/far", identity, {});
    const lease = { id: f.source.id, rootId: f.source.id, identityId: f.source.id, startedAt: f.source.host.startedAt,
      incarnationToken: f.source.host.incarnationToken, updatedAt: f.now, expiresAt: f.now + 30_000 };
    await f.mesh.writeBatch({ identity, ops: [], prepare: () => outbox.plan().stage([
      { kind: "put", key: key("hosts", f.source.id), identity: f.source.identity, value: { ...f.source.host, remoteHost: "far" } },
    ], [{ kind: "lease", key: `lease:${f.source.id}`, payload: lease }]) });
    expect(readHostLease(f.mesh.root, f.source.id)).toBeUndefined();
    const observed = observe(f), rename = vi.spyOn(fs, "renameSync");
    await f.side.mirror({ hosts: [], participants: [] });
    expect(observed.stats.scans).toBeGreaterThan(0); // initial recovery is NOT a zero-scan steady pass
    expect(rename.mock.calls.filter(([, to]) => String(to) === hostLeasePath(f.mesh.root, f.source.id))).toHaveLength(1);
    expect(f.mesh.get(key("hosts", f.source.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.mesh.root, f.source.id)).toBeUndefined();
    await f.side.mirror(f.source.presence);
    expect(readHostLease(f.mesh.root, f.source.id)).toBeDefined();
    observed.reset();
    let mutations = 0;
    observed.setBefore(() => { mutations += 1; f.write(key("hosts", f.native.id), { ...f.native.host, ingress: mutations }); });
    await f.side.withdraw();
    expect(observed.stats.attempts).toBeLessThanOrEqual(3);
    expect(observed.stats.scans).toBeGreaterThan(0); // final withdrawal still has its existing fallback owner
    expect(f.mesh.get(key("hosts", f.source.id), { fresh: true })).toBeUndefined();
    expect(f.mesh.get(key("participants", f.source.id), { fresh: true })).toBeUndefined();
    expect(readHostLease(f.mesh.root, f.source.id)).toBeUndefined();
    expect(f.mesh.get(key("hosts", f.native.id), { fresh: true })?.value).toMatchObject({ id: f.native.id });
  });

  it("file backend keeps full authoritative mirror/withdrawal rather than pretending to have a SQLite revision proof", async () => {
    const f = await fixture("file");
    expect(f.mesh.stateView()).toBeUndefined();
    const observed = observe(f);
    await f.side.mirror(f.nextPresence());
    expect(observed.stats.attempts).toBe(1);
    expect(f.side.holds(f.source.id, f.source.identity)).toBe(true);
    expect(f.mesh.get(key("participants", f.source.id), { fresh: true })?.value).toMatchObject({ label: "fresh-source" });
    await send(f, "file");
    expect(f.mesh.read({ limit: 100 }).filter(event => event.topic === "fleet.work.mirror-fence").map(event => event.text)).toEqual(["file"]);
    await f.side.withdraw();
    expect(readHostLease(f.mesh.root, f.source.id)).toBeUndefined();
    expect(f.mesh.get(key("hosts", f.source.id), { fresh: true })).toBeUndefined();
  });

  it("real native/other-link takeovers still refuse publication; an unrelated writer still permits it", async () => {
    for (const takeover of ["native", "other-link", "unrelated"] as const) {
      const f = await fixture();
      expect(f.side.holds(f.source.id, f.source.identity)).toBe(true);
      const mirrored = f.mesh.get(key("hosts", f.source.id), { fresh: true })!.value as Record<string, unknown>;
      const fence = f.mesh.withStateWriteFence.bind(f.mesh);
      let writes = 0;
      vi.spyOn(f.mesh, "withStateWriteFence").mockImplementation(<T>(operation: () => T): T => {
        if (writes++ === 0) {
          f.write(takeover === "unrelated" ? "probe/unrelated" : key("hosts", f.source.id),
            takeover === "unrelated" ? 1 : takeover === "native" ? f.source.host : { ...mirrored, remoteHost: "other" });
        }
        return fence(operation);
      });
      if (takeover === "unrelated") await expect(send(f, takeover)).resolves.toMatchObject({ sequence: expect.any(Number) });
      else await expect(send(f, takeover)).rejects.toThrow("no longer bound");
      expect(f.externalCommits()).toBe(1);
      expect(f.side.holds(f.source.id, f.source.identity)).toBe(takeover === "unrelated");
      expect(f.mesh.read({ limit: 100 }).filter(event => event.topic === "fleet.work.mirror-fence").map(event => event.text))
        .toEqual(takeover === "unrelated" ? ["unrelated"] : []);
      vi.restoreAllMocks();
    }
  });
});
