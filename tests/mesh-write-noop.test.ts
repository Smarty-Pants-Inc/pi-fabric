// #18131: caller coalescing must not redefine puts or elide ownership/CAS/state fences.
// Keep reason: data safety at the publication/ownership boundary; real backends and directory.
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { MeshIdentity } from "../src/mesh/store.js";
import type { ParticipantDirectoryOptions } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord, FabricHostRecord } from "../src/topology/types.js";

let Store: typeof import("../src/mesh/store.js").MeshStore;
let Directory: typeof import("../src/topology/participant-directory.js").ParticipantDirectory;
let fences: typeof import("../src/mesh/commit-outbox.js");
let leases: typeof import("../src/topology/host-leases.js");
let locks: typeof import("../src/mesh/mesh-lock.js");
let removal: typeof import("../src/storage/verified-remove.js");
const identity: MeshIdentity = { id: "session:noop", name: "noop", kind: "main", sessionId: "noop" };
const hashed = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const participantKey = hashed("topology/participants/", identity.id);
const hostKey = hashed("topology/hosts/", identity.id);
const removeFixture = fs.rmSync.bind(fs);

beforeAll(async () => {
  vi.stubEnv("PI_FABRIC_LOCK_STATS", "1");
  vi.resetModules();
  ({ MeshStore: Store } = await import("../src/mesh/store.js"));
  ({ ParticipantDirectory: Directory } = await import("../src/topology/participant-directory.js"));
  fences = await import("../src/mesh/commit-outbox.js");
  leases = await import("../src/topology/host-leases.js");
  locks = await import("../src/mesh/mesh-lock.js");
  removal = await import("../src/storage/verified-remove.js");
  expect(locks.lockStats).toBeDefined(); // Never report an uninstrumented zero as proof.
});
afterAll(() => { vi.unstubAllEnvs(); });

// No product deletion is executed locally (#4664): record lock/census/withdrawal removals
// and emulate them by renaming this fixture's own entries. The real verifier is dry-run only.
const recordDeletes = (root: string) => {
  const grave = path.join(root, "removed");
  fs.mkdirSync(grave);
  let sequence = 0;
  const emulate = (target: fs.PathLike, force = false, empty = false): void => {
    const raw = String(target);
    let resolved: string;
    try { resolved = path.join(fs.realpathSync(path.dirname(raw)), path.basename(raw)); }
    catch { resolved = path.resolve(raw); }
    if (!resolved.startsWith(root + path.sep) || resolved.startsWith(grave + path.sep)) {
      throw new Error(`Refused deletion outside fixture: ${resolved}`);
    }
    if (!fs.existsSync(resolved)) {
      if (force) return;
      throw Object.assign(new Error("Missing emulated deletion"), { code: "ENOENT" });
    }
    if (empty && fs.readdirSync(resolved).length) throw Object.assign(new Error("Directory not empty"), { code: "ENOTEMPTY" });
    fs.renameSync(resolved, path.join(grave, String(++sequence)));
  };
  vi.spyOn(fs, "rmSync").mockImplementation((target, options) => emulate(target, options?.force));
  vi.spyOn(fs, "unlinkSync").mockImplementation(target => emulate(target));
  vi.spyOn(fs, "rmdirSync").mockImplementation(target => emulate(target, false, true));
  vi.spyOn(fsp, "rm").mockImplementation(async (target, options) => emulate(target, options?.force));
  vi.spyOn(fsp, "unlink").mockImplementation(async target => emulate(target));
  vi.spyOn(fsp, "rmdir").mockImplementation(async target => emulate(target, false, true));
  const verify = removal.verifiedRemove;
  vi.spyOn(removal, "verifiedRemove").mockImplementation((target, judged, options) => {
    const result = verify(target, judged, { ...options, dryRun: true,
      fallback: () => { throw new Error("Real deletion fallback forbidden"); } });
    if (result.removed) emulate(target);
    return result;
  });
};

const fixture = async (backend: "file" | "sqlite", extra: Partial<ParticipantDirectoryOptions> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-write-noop-"));
  recordDeletes(root);
  const mesh = new Store(path.join(root, "mesh"), 65536, 100, { stateBackend: backend });
  expect(mesh.stateBackend).toBe(backend); // SQLite fallback is not SQLite coverage.
  let generation = 0;
  const record: FabricParticipantRecord = {
    format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    kind: "root", name: "noop", status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"],
    cwd: root, sessionId: "noop", startedAt: Date.now(), updatedAt: 1, controlProtocol: "v1", label: "N-1",
  };
  const directory = new Directory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id,
    identity, reapDeadHosts: false,
    preparePublicationFence: () => { const prepared = generation; return () => prepared === generation; },
    withPublicationFence: operation => operation(), ...extra });
  directory.registerSource(() => [{ ...record }]);
  const acquired = vi.spyOn(locks.lockStats!, "acquired");
  const batch = vi.spyOn(mesh, "writeBatch");
  const confirm = vi.spyOn(mesh, "confirmWritable");
  const counts = () => {
    const own = acquired.mock.calls.filter(([at]) => at === mesh.root);
    return { writers: own.length, batches: own.filter(([, kind]) => kind === "writeBatch").length,
      confirmations: own.filter(([, kind]) => kind === "heartbeat/confirm").length };
  };
  const clear = () => { acquired.mockClear(); batch.mockClear(); confirm.mockClear(); };
  const close = async () => {
    try { directory.suspend(); await directory.close(); mesh.closeState(); }
    finally { vi.restoreAllMocks(); removeFixture(root, { recursive: true, force: true }); }
  };
  try { await directory.refresh(); clear(); }
  catch (error) { await close(); throw error; }
  return { root, mesh, directory, record, batch, confirm, counts, clear, close,
    changeGeneration: () => { generation++; } };
};

type Fixture = Awaited<ReturnType<typeof fixture>>;
const assertGuarded = (f: Fixture) => {
  expect(f.batch).toHaveBeenCalledOnce();
  expect(f.batch.mock.calls[0]![0].beforeCommit).toBeTypeOf("function");
  expect(f.batch.mock.calls[0]![0].commitOutbox).toBeTypeOf("function");
};

for (const backend of ["file", "sqlite"] as const) describe(`real directory idle publication (${backend})`, () => {
  it("identical and timestamp-only change refreshes already issue no batch and acquire zero writers", async () => {
    const f = await fixture(backend);
    try {
      const before = f.mesh.get(participantKey, { fresh: true });
      const confirmed = f.directory.confirmedAt();
      for (let at = 0; at < 3; at++) {
        await f.directory.refreshPresence();
        f.record.updatedAt++;
      }
      expect(f.batch).not.toHaveBeenCalled();
      expect(f.confirm).not.toHaveBeenCalled();
      expect(f.counts()).toEqual({ writers: 0, batches: 0, confirmations: 0 });
      expect(f.mesh.get(participantKey, { fresh: true })).toEqual(before);
      expect(f.directory.confirmedAt()).toBe(confirmed); // An idle skip is not new custody proof.
      console.info("MESH_NOOP_BASELINE " + JSON.stringify({ backend, path: "refreshPresence", ...f.counts() }));
    } finally { await f.close(); }
  });

  it("changed content writes once through the ownership guard and outbox", async () => {
    const f = await fixture(backend);
    try {
      const before = f.mesh.get(participantKey, { fresh: true })!;
      f.record.status = "running";
      await f.directory.refreshPresence();
      assertGuarded(f);
      expect(f.counts().batches).toBe(1);
      expect(f.mesh.get(participantKey, { fresh: true })!.version).toBeGreaterThan(before.version);
      expect(f.mesh.get(participantKey, { fresh: true })!.value).toMatchObject({ status: "running" });
    } finally { await f.close(); }
  });

  it("a full idle heartbeat renews its real file lease but retains required shared confirmation", async () => {
    const f = await fixture(backend);
    try {
      const prior = leases.readHostLease(f.mesh.root, identity.id)!;
      const now = Date.now() + 5_000;
      vi.spyOn(Date, "now").mockReturnValue(now);
      await f.directory.refresh();
      expect(leases.readHostLease(f.mesh.root, identity.id)!.updatedAt).toBeGreaterThan(prior.updatedAt);
      expect(f.batch).not.toHaveBeenCalled();
      expect(f.confirm).toHaveBeenCalledOnce();
      expect(f.counts().confirmations).toBe(1);
      console.info("MESH_NOOP_BASELINE " + JSON.stringify({ backend, path: "refresh", ...f.counts() }));
    } finally { await f.close(); }
  });

  it("near-expiry compatibility host/session leases still advance under a guarded batch", async () => {
    const f = await fixture(backend);
    try {
      const oldIdentity: MeshIdentity = { id: "old-reader", name: "old-reader", kind: "agent" };
      const before = f.mesh.get(hostKey, { fresh: true })!;
      const old: FabricHostRecord = { format: 1, id: oldIdentity.id, rootId: oldIdentity.id,
        identity: oldIdentity, startedAt: Date.now(), updatedAt: Date.now(), expiresAt: Date.now() + 120_000 };
      await f.mesh.put({ key: hashed("topology/hosts/", old.id), value: old, identity: oldIdentity });
      f.clear();
      vi.spyOn(Date, "now").mockReturnValue((before.value as FabricHostRecord).expiresAt - 1);
      await f.directory.refresh();
      assertGuarded(f);
      const after = f.mesh.get(hostKey, { fresh: true })!;
      expect(after.version).toBeGreaterThan(before.version);
      expect((after.value as FabricHostRecord).expiresAt).toBeGreaterThan((before.value as FabricHostRecord).expiresAt);
      expect(f.counts().batches).toBe(1);
    } finally { await f.close(); }
  });

  it("a changed preparation generation is reselected before publishing, never reused as idle proof", async () => {
    let replace = false;
    let f: Fixture;
    f = await fixture(backend, { withPublicationFence: async operation => {
      if (replace) { replace = false; f.changeGeneration(); f.record.status = "running"; }
      return operation();
    } });
    try {
      replace = true;
      await f.directory.refreshPresence();
      assertGuarded(f);
      expect(f.mesh.get(participantKey, { fresh: true })!.value).toMatchObject({ status: "running" });
      expect(f.counts().batches).toBe(1);
    } finally { await f.close(); }
  });

  it("an opaque publication effect is not inferred idle from an empty or equal payload", async () => {
    let enabled = false;
    const committed = vi.fn();
    const f = await fixture(backend, { publicationBatch: () => ({
      ops: enabled ? [{ kind: "put", key: "test/advancing-fence", value: { epoch: 1 } }] : [], committed,
    }) });
    try {
      enabled = true;
      await f.directory.refreshPresence();
      const before = f.mesh.get("test/advancing-fence", { fresh: true })!.version;
      f.clear(); committed.mockClear();
      await f.directory.refreshPresence();
      assertGuarded(f);
      expect(f.counts().batches).toBe(1);
      expect(committed).toHaveBeenCalledOnce();
      expect(f.mesh.get("test/advancing-fence", { fresh: true })!.version).toBeGreaterThan(before);
    } finally { await f.close(); }
  });

  it("concurrent participant ownership change preserves the selected CAS instead of overwriting the successor", async () => {
    let takeover = false;
    let f: Fixture;
    const successor: MeshIdentity = { id: "successor", name: "successor", kind: "main" };
    f = await fixture(backend, { withPublicationFence: async operation => {
      if (takeover) {
        takeover = false;
        const old = f.mesh.get(participantKey, { fresh: true })!.value as FabricParticipantRecord;
        await f.mesh.put({ key: participantKey, identity: successor,
          value: { ...old, ownerHostId: successor.id, ownerIdentityId: successor.id, ownerIncarnation: "successor-epoch" } });
      }
      return operation();
    } });
    try {
      f.record.status = "running";
      takeover = true;
      await f.directory.refreshPresence();
      assertGuarded(f);
      const selected = f.batch.mock.calls[0]![0].ops.find(op => op.key === participantKey)!;
      expect(selected.ifVersion).toBeTypeOf("number");
      expect(selected.onConflict).toBeTypeOf("function");
      expect(f.mesh.get(participantKey, { fresh: true })).toMatchObject({ updatedBy: successor,
        value: { ownerHostId: successor.id, ownerIncarnation: "successor-epoch", status: "idle" } });
      expect(f.counts().batches).toBe(1);
    } finally { await f.close(); }
  });

  it("repeated quiescent full refresh is an actual empty guarded batch, not normal idle publication", async () => {
    const f = await fixture(backend);
    try {
      await f.directory.quiesce();
      const stamp = f.mesh.stateStamp();
      f.clear();
      await f.directory.refresh();
      assertGuarded(f);
      expect(f.batch.mock.calls[0]![0].ops).toEqual([]);
      expect(f.counts().batches).toBe(1);
      expect(f.mesh.stateStamp()).toBe(stamp);
      console.info("MESH_NOOP_BASELINE " + JSON.stringify({ backend, path: "quiescent-refresh", ...f.counts() }));
    } finally { await f.close(); }
  });

  it("ordinary identical puts still allocate revisions; payload equality is not a batch no-op contract", async () => {
    const f = await fixture(backend);
    try {
      const prior = f.mesh.get(participantKey, { fresh: true })!;
      const [result] = await f.mesh.writeBatch({ identity, ops: [{ kind: "put", key: participantKey, value: prior.value }] });
      expect(result!.applied).toBe(true);
      expect(result!.version).toBeGreaterThan(prior.version);
      expect(f.counts().batches).toBe(1);
    } finally { await f.close(); }
  });

  it("empty state fences, skipped CAS payloads and throwing ownership guards still acquire custody", async () => {
    const f = await fixture(backend);
    try {
      await fences.withStateFence(f.mesh, identity, view => { expect(view.get(participantKey)).toBeDefined(); });
      expect(f.counts().batches).toBe(1);
      f.clear();
      const payload = { toJSON: vi.fn(() => { throw new Error("CAS must precede serialization"); }) };
      const [skipped] = await f.mesh.writeBatch({ identity, ops: [{ kind: "put", key: participantKey,
        value: payload, ifVersion: 0, onConflict: "skip" }] });
      expect(skipped!.applied).toBe(false);
      expect(payload.toJSON).not.toHaveBeenCalled();
      expect(f.counts().batches).toBe(1);
      f.clear();
      const outbox = vi.fn();
      await expect(f.mesh.writeBatch({ identity, ops: [], beforeCommit: () => { throw new Error("owner changed"); },
        commitOutbox: outbox })).rejects.toThrow("owner changed");
      expect(outbox).not.toHaveBeenCalled();
      expect(f.counts().batches).toBe(1);
    } finally { await f.close(); }
  });
});

it("SQLite's existing proof rejects stale selected state but cannot attest external lease/registry epochs", async () => {
  const f = await fixture("sqlite");
  try {
    const selected = f.mesh.stateView()!;
    const prior = selected.view.get(participantKey)!;
    expect(f.mesh.stateUnchangedSince(selected.revision, [participantKey])).toBe(true);
    await f.mesh.put({ key: "unrelated/value", value: 1, identity });
    expect(f.mesh.stateUnchangedSince(selected.revision, [participantKey])).toBe(true);
    await f.mesh.put({ key: participantKey, value: { ...(prior.value as object), status: "running" }, identity });
    expect(selected.view.get(participantKey)).toEqual(prior); // A pinned old view never becomes fresh by itself.
    expect(f.mesh.stateUnchangedSince(selected.revision, [participantKey])).toBe(false);
    f.record.status = "running";
    const current = f.mesh.stateView()!;
    f.changeGeneration();
    const before = leases.readHostLease(f.mesh.root, identity.id)!;
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 5_000);
    await f.directory.refreshPresence(); // File lease renewal; it is not a state commit.
    expect(leases.readHostLease(f.mesh.root, identity.id)!.updatedAt).toBeGreaterThan(before.updatedAt);
    expect(f.mesh.stateUnchangedSince(current.revision, [participantKey])).toBe(true);
  } finally { await f.close(); }
});
