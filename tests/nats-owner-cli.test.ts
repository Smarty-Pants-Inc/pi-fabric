// expected-skip: no NATS_BIN/PATH executable or non-Linux host: real authenticated Path B Main needs Linux/nats-server.
// Keep reasons: routing authority/trust boundary and data safety. Fake KV exercises ambiguity and
// missing-source failures; the real Main/broker flow proves the user path and read-only KV behavior.
// Product KV deletions are RECORD ONLY (SDK backstop too); shutdown uses real broker lease expiry.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { Bucket } from "@nats-io/kv";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ownershipReceipt, runNatsOwnerCli, type OwnershipReceipt } from "../src/mesh/nats-auth/owner-cli.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import { KvConflictError, type FabricTransport, type TransportKv, type TransportKvEntry, type TransportKvEvent } from "../src/transport/types.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../src/topology/types.js";
import { MeshStore } from "../src/mesh/store.js";
import { handOffNativeIdentity } from "../src/mesh/bridge-handoff.js";
import { hostLeasePath } from "../src/topology/host-leases.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { openNatsConnection } from "../src/transport/nats/connection.js";
import { LEASES_BUCKET, PRESENCE_BUCKET, NatsKvStore, openNatsKv } from "../src/transport/nats/kv.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, PATH_B_POLICY_ENV, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const SESSION = "b3b3b3b3-1111-4000-8000-000000000003", ROOT = `session:${SESSION}`;
const MACHINE = "machine-a", INCARNATION = "owner-incarnation", NOW = 2_000;
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const key = (machine: string, kind: "p" | "h", id = ROOT): string => `${machine}.${kind}.${hash(id).slice(0, 32)}`;
const clone = <T>(value: T): T => structuredClone(value);
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}

/** Copied snapshot/CAS fake from participant-presence-succession; no liveness model. */
class RecordOnlyKv implements TransportKv {
  readonly rows = new Map<string, TransportKvEntry<unknown>>();
  readonly deletes: Array<{ key: string; revision: number }> = [];
  readonly mutations: Array<{ key: string; expected: number; revision: number }> = [];
  readonly #watchers = new Set<{ events: TransportKvEvent<unknown>[]; wake(): void }>();
  #revision = 0;
  constructor(readonly ttlMs: number) {}
  seed(entryKey: string, owner: string, value: unknown): TransportKvEntry<unknown> {
    const entry = { key: entryKey, machine: entryKey.split(".")[0]!, owner, value: clone(value), revision: ++this.#revision };
    this.rows.set(entryKey, entry);
    for (const watcher of this.#watchers) { watcher.events.push({ type: "put", entry: clone(entry) }); watcher.wake(); }
    return clone(entry);
  }
  async get<V>(entryKey: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined> {
    signal?.throwIfAborted(); return clone(this.rows.get(entryKey)) as TransportKvEntry<V> | undefined;
  }
  async put<V>(entryKey: string, owner: string, value: V, expected: number, signal?: AbortSignal): Promise<number> {
    signal?.throwIfAborted();
    if ((this.rows.get(entryKey)?.revision ?? 0) !== expected) throw new KvConflictError(entryKey, expected);
    const entry = this.seed(entryKey, owner, value);
    this.mutations.push({ key: entryKey, expected, revision: entry.revision });
    return entry.revision;
  }
  async delete(entryKey: string, revision: number): Promise<boolean> { this.deletes.push({ key: entryKey, revision }); return false; }
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
function participant(incarnation = INCARNATION): FabricParticipantRecord {
  return { format: 1, id: ROOT, rootId: ROOT, kind: "root", ownerHostId: ROOT, ownerIdentityId: ROOT,
    ownerIncarnation: incarnation, sessionId: SESSION, name: "fixture Main", status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], controlProtocol: "v1", startedAt: 1_000, updatedAt: 2_000 };
}
function hostLease(incarnation = INCARNATION): FabricKvHostLease {
  return { id: ROOT, rootId: ROOT, identityId: ROOT, incarnation, pid: process.pid,
    pidStartTime: "12345", bootId: "fixture-boot", updatedAt: 2_000, expiresAt: 17_000 };
}
const views: KvParticipantDirectory[] = [], roots: string[] = [];
afterEach(async () => {
  try { await Promise.all(views.splice(0).map(view => view.close())); }
  finally {
    vi.restoreAllMocks(); vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});
function tempRoot(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-owner-")); roots.push(root); return root;
}
function fixture() {
  const presence = new RecordOnlyKv(600_000), leases = new RecordOnlyKv(15_000);
  const abort = new AbortController();
  const transport: FabricTransport = { kind: "nats", self: { machine: MACHINE, hostId: "owner-reader:fixture" },
    kv: bucket => bucket === "presence" ? presence : leases,
    async send() { throw new Error("KV-only seam cannot send"); }, async *inbox() { throw new Error("KV-only seam cannot receive"); },
    async drainState() { throw new Error("KV-only seam cannot drain"); }, async close() {} };
  const view = new KvParticipantDirectory({ transport, lease: { live: () => false, signal: abort.signal },
    identityId: "owner-reader:fixture", ownerIncarnation: "reader-incarnation", rootId: ROOT });
  views.push(view);
  function seed(machine = MACHINE, live = true) {
    const p = participant(machine === MACHINE ? INCARNATION : "other-incarnation");
    presence.seed(key(machine, "p"), `${ROOT}#${p.ownerIncarnation}`, p);
    if (live) leases.seed(key(machine, "h"), `${ROOT}#${p.ownerIncarnation}`, hostLease(p.ownerIncarnation));
  }
  function read(meshRoot?: string) {
    view.start();
    return ownershipReceipt(view, ROOT, { machine: MACHINE, now: () => NOW,
      ...(meshRoot === undefined ? {} : { meshRoot }), signal: AbortSignal.timeout(1_000) });
  }
  async function assertNoWrites() {
    await view.close();
    expect(presence.mutations).toEqual([]); expect(presence.deletes).toEqual([]);
    expect(leases.mutations).toEqual([]); expect(leases.deletes).toEqual([]);
  }
  return { presence, leases, view, abort, seed, read, assertNoWrites };
}

describe("NATS ownership receipt snapshots", () => {
  it("returns exact owned receipt/exit 0, including the owner's machine when the reader is on that machine", async () => {
    const f = fixture(); f.seed();
    expect(await f.read()).toEqual({ exitCode: 0, receipt: { format: 1, checkedAt: new Date(NOW).toISOString(),
      rootId: ROOT, sessionId: SESSION, reader: { machine: MACHINE }, verdict: "owned",
      owner: { machine: MACHINE, hostId: ROOT, identityId: ROOT, incarnation: INCARNATION, status: "idle", presenceRevision: 1,
        lease: { revision: 1, incarnation: INCARNATION, updatedAt: 2_000, expiresAt: 17_000 } },
      nats: { rows: [{ machine: MACHINE, ownerHostId: ROOT, ownerIdentityId: ROOT, ownerIncarnation: INCARNATION,
        capabilities: ["steer", "followUp"], live: true, kind: "root", status: "idle", rootId: ROOT, sessionId: SESSION,
        updatedAt: 2_000, presenceRevision: 1, lease: { revision: 1, incarnation: INCARNATION, updatedAt: 2_000, expiresAt: 17_000 } }] }, file: null } });
    await f.assertNoWrites();
  });
  it("returns absent/exit 1 for a ready empty snapshot", async () => {
    const f = fixture();
    expect(await f.read()).toMatchObject({ exitCode: 1, receipt: { verdict: "absent", owner: null, nats: { rows: [] } } });
    await f.assertNoWrites();
  });
  it("returns stale/exit 1 for presence without a lease", async () => {
    const f = fixture(); f.seed(MACHINE, false);
    const result = await f.read();
    expect(result).toMatchObject({ exitCode: 1, receipt: { verdict: "stale", owner: null, nats: { rows: [{ live: false }] } } });
    expect(result.receipt.nats.rows[0]!.lease).toBeUndefined();
    await f.assertNoWrites();
  });
  it.each([true, false])("returns ambiguous/exit 1 with both rows (second live: %s)", async secondLive => {
    const f = fixture(); f.seed(); f.seed("machine-b", secondLive);
    expect(await f.read()).toMatchObject({ exitCode: 1, receipt: { verdict: "ambiguous", owner: null,
      nats: { rows: [{ machine: MACHINE, live: true }, { machine: "machine-b", live: secondLive }] } } });
    await f.assertNoWrites();
  });
  it("does not turn a failed/not-ready source into known absence or success", async () => {
    const f = fixture(); f.abort.abort();
    const result = await ownershipReceipt(f.view, ROOT, { machine: MACHINE, signal: AbortSignal.abort() });
    expect(result).toMatchObject({ exitCode: 1, receipt: { verdict: "stale", owner: null, unknown: ["nats"], error: expect.any(String) } });
    await f.assertNoWrites();
  });
});

async function populateMesh(store: MeshStore, mirrored = true): Promise<void> {
  // Real handoff, on an empty mesh: no destructive retirement path is exercised.
  await handOffNativeIdentity(store, { session: SESSION, link: "nats", operator: "fixture-operator", now: () => 1_500, hostname: "fixture-hub" });
  if (mirrored) {
    const identity = { id: ROOT, name: "fixture Main", kind: "main" as const, sessionId: SESSION };
    await store.put({ identity, key: `topology/hosts/${hash(ROOT)}`, value: { format: 1, id: ROOT, rootId: ROOT, identity,
      incarnationToken: INCARNATION, remoteHost: "nats", startedAt: 1_000, updatedAt: NOW, expiresAt: 17_000, message: "do-not-print-content" } });
    await store.put({ identity, key: `topology/participants/${hash(ROOT)}`, value: { ...participant(), remoteHost: "nats", message: "do-not-print-content" } });
  }
}
async function fileMesh(mirrored = true): Promise<string> {
  const root = tempRoot(), store = new MeshStore(root, 256 * 1024, 500);
  try { await populateMesh(store, mirrored); return root; }
  finally { store.closeState(); }
}
function treeSnapshot(root: string): Array<{ path: string; size: string; mode: string; mtimeNs: string }> {
  const output: Array<{ path: string; size: string; mode: string; mtimeNs: string }> = [];
  function walk(file: string) {
    const stat = fs.lstatSync(file, { bigint: true });
    output.push({ path: path.relative(root, file), size: String(stat.size), mode: String(stat.mode), mtimeNs: String(stat.mtimeNs) });
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) walk(path.join(file, name));
  }
  walk(root); return output;
}
describe("file-side handoff readback without opening MeshStore", () => {
  it("reads the real SQLite writer's moved-marker/WAL mesh like the file backend, changing no DB or WAL bytes", async () => {
    const fileRoot = await fileMesh(), sqliteRoot = tempRoot();
    const store = new MeshStore(sqliteRoot, 256 * 1024, 500, { stateBackend: "sqlite" });
    try {
      await populateMesh(store);
      expect(store.stateBackend).toBe("sqlite");
      expect(JSON.parse(fs.readFileSync(path.join(sqliteRoot, "state.json"), "utf8"))).toMatchObject({ format: "sqlite", movedTo: "state.db" });
      // Keep the real writer open so this tests live WAL data, not only checkpointed state.
      const db = path.join(sqliteRoot, "state.db"), wal = `${db}-wal`;
      expect(fs.statSync(wal).size).toBeGreaterThan(0);
      const beforeDb = fs.readFileSync(db), beforeWal = fs.readFileSync(wal);
      // SQLite WAL readers may update read marks/locks in -shm by design. Exclude ONLY
      // that shared-memory file; DB/WAL, census, directories and all other files stay exact.
      const snapshotWithoutShm = () => treeSnapshot(sqliteRoot).filter(row => row.path !== "state.db-shm");
      const beforeTree = snapshotWithoutShm(), f = fixture(); f.seed();
      const fileResult = await f.read(fileRoot), sqliteResult = await f.read(sqliteRoot);
      expect(sqliteResult).toEqual({ ...fileResult, receipt: { ...fileResult.receipt,
        file: { ...fileResult.receipt.file, meshRoot: sqliteRoot } } });
      expect(fs.readFileSync(db)).toEqual(beforeDb); expect(fs.readFileSync(wal)).toEqual(beforeWal);
      expect(snapshotWithoutShm()).toEqual(beforeTree); await f.assertNoWrites();
    } finally { store.closeState(); }
  });
  it.each(["missing-db", "missing-sidecars", "corrupt-db", "nats-marker", "unknown-backend"])("fails closed without creating/writing state for SQLite fault %s", async fault => {
    const mesh = tempRoot(), store = new MeshStore(mesh, 256 * 1024, 500, { stateBackend: "sqlite" });
    await populateMesh(store);
    if (fault !== "unknown-backend") store.closeState();
    const db = path.join(mesh, "state.db");
    if (fault === "missing-db") fs.renameSync(db, `${db}.saved`);
    if (fault === "corrupt-db") fs.writeFileSync(db, "not-a-database");
    if (fault === "nats-marker") {
      const marker = JSON.parse(fs.readFileSync(path.join(mesh, "state.json"), "utf8")); marker.natsStream = "FABRIC_STATE";
      fs.writeFileSync(path.join(mesh, "state.json"), JSON.stringify(marker));
    }
    if (fault === "unknown-backend") {
      // Fault injection is setup only; the receipt always opens with readOnly:true.
      const sqlite = process.getBuiltinModule("node:sqlite");
      const writer = new sqlite.DatabaseSync(db);
      try { writer.prepare("UPDATE meta SET value = ? WHERE name = 'backend'").run("unknown"); }
      finally { writer.close(); }
    }
    const before = treeSnapshot(mesh).filter(row => row.path !== "state.db-shm"), bytes = fs.existsSync(db) ? fs.readFileSync(db) : null;
    const f = fixture(); f.seed();
    expect(await f.read(mesh)).toMatchObject({ exitCode: 1, receipt: { verdict: "owned", unknown: ["file"], file: null } });
    expect(fs.existsSync(db) ? fs.readFileSync(db) : null).toEqual(bytes);
    expect(treeSnapshot(mesh).filter(row => row.path !== "state.db-shm")).toEqual(before); await f.assertNoWrites();
    store.closeState();
  });
  it("shows a real in-force handoff and link mirrors, leaving every path/size/mode/mtimeNs unchanged", async () => {
    const mesh = await fileMesh(), before = treeSnapshot(mesh), f = fixture(); f.seed();
    const result = await f.read(mesh);
    expect(result).toMatchObject({ exitCode: 0, receipt: { verdict: "owned", file: { meshRoot: mesh,
      handoff: { link: "nats", handedOffAt: 1_500, operator: "fixture-operator", hub: "fixture-hub", inForce: true },
      mirror: { host: { id: ROOT, remoteHost: "nats", incarnationToken: INCARNATION, expiresAt: 17_000 },
        participant: { id: ROOT, remoteHost: "nats", ownerIncarnation: INCARNATION } } } } });
    expect(JSON.stringify(result)).not.toContain("do-not-print-content");
    expect(treeSnapshot(mesh)).toEqual(before); await f.assertNoWrites();
  });
  it("reports no marker/mirror as null, not unknown, in a valid file state", async () => {
    const mesh = tempRoot(); fs.writeFileSync(path.join(mesh, "state.json"), JSON.stringify({ format: 1, entries: {} }));
    const f = fixture(); f.seed();
    expect(await f.read(mesh)).toMatchObject({ exitCode: 0, receipt: { file: { handoff: null, mirror: null } } });
    await f.assertNoWrites();
  });
  it("reports a handoff voided by a returned native participant file, without writing or locking", async () => {
    const mesh = await fileMesh(false), f = fixture(); f.seed();
    fs.mkdirSync(path.join(mesh, "participants"));
    const pkey = `topology/participants/${hash(ROOT)}`;
    fs.writeFileSync(path.join(mesh, "participants", `${hash(ROOT)}.json`), JSON.stringify({ format: 1, key: pkey,
      value: participant(), version: 1, updatedAt: NOW, updatedBy: { id: ROOT, name: "Main", kind: "main" } }));
    const before = treeSnapshot(mesh);
    expect(await f.read(mesh)).toMatchObject({ exitCode: 0, receipt: { file: { handoff: { inForce: false } } } });
    expect(treeSnapshot(mesh)).toEqual(before); await f.assertNoWrites();
  });
  it("reports a live native Main session lease as voiding the handoff", async () => {
    const mesh = await fileMesh(false), f = fixture(); f.seed();
    fs.mkdirSync(path.join(mesh, "host-leases"));
    fs.writeFileSync(hostLeasePath(mesh, ROOT), JSON.stringify({ format: 1,
      id: ROOT, rootId: ROOT, identityId: ROOT, updatedAt: NOW, expiresAt: NOW - 1,
      session: { id: SESSION, startedAt: 1_000, updatedAt: NOW, expiresAt: NOW + 10_000 } }));
    const before = treeSnapshot(mesh);
    expect(await f.read(mesh)).toMatchObject({ exitCode: 0, receipt: { file: { handoff: { inForce: false } } } });
    expect(treeSnapshot(mesh)).toEqual(before); await f.assertNoWrites();
  });
  it.each(["handoff", "handoff-key", "state", "participant", "lease", "lease-session", "lease-writer", "missing-state", "switched-db"])("fails closed for corrupt/missing %s while preserving the NATS observation", async fault => {
    const mesh = await fileMesh(), f = fixture(); f.seed();
    if (fault === "state") fs.writeFileSync(path.join(mesh, "state.json"), "{}");
    if (fault === "handoff") {
      const file = path.join(mesh, "state.json"), doc = JSON.parse(fs.readFileSync(file, "utf8"));
      doc.entries[`topology/handoffs/${hash(ROOT)}`].value.link = "invalid link";
      fs.writeFileSync(file, JSON.stringify(doc));
    }
    if (fault === "handoff-key") {
      const file = path.join(mesh, "state.json"), doc = JSON.parse(fs.readFileSync(file, "utf8"));
      const proper = `topology/handoffs/${hash(ROOT)}`, wrong = "topology/handoffs/not-a-root-hash";
      doc.entries[wrong] = { ...doc.entries[proper], key: wrong };
      delete doc.entries[proper];
      fs.writeFileSync(file, JSON.stringify(doc));
    }
    if (fault === "participant") {
      fs.mkdirSync(path.join(mesh, "participants"));
      fs.writeFileSync(path.join(mesh, "participants", `${hash(ROOT)}.json`), "not-json");
    }
    if (fault === "lease") {
      fs.mkdirSync(path.join(mesh, "host-leases"));
      fs.writeFileSync(hostLeasePath(mesh, ROOT), "{}");
    }
    if (fault === "lease-session" || fault === "lease-writer") {
      fs.mkdirSync(path.join(mesh, "host-leases"));
      fs.writeFileSync(hostLeasePath(mesh, ROOT), JSON.stringify({ format: 1,
        id: ROOT, rootId: ROOT, identityId: ROOT, updatedAt: NOW, expiresAt: NOW + 15_000,
        [fault === "lease-session" ? "session" : "writer"]: { invalid: true } }));
    }
    if (fault === "missing-state") fs.renameSync(path.join(mesh, "state.json"), path.join(mesh, "state.saved"));
    if (fault === "switched-db") fs.writeFileSync(path.join(mesh, "state.db"), "not-a-file-mesh");
    const before = treeSnapshot(mesh);
    expect(await f.read(mesh)).toMatchObject({ exitCode: 1, receipt: { verdict: "owned", unknown: ["file"], error: expect.any(String), file: null } });
    expect(treeSnapshot(mesh)).toEqual(before); await f.assertNoWrites();
  });
  it("does not create a missing --mesh root", async () => {
    const root = path.join(tempRoot(), "missing"), f = fixture(); f.seed();
    expect(await f.read(root)).toMatchObject({ exitCode: 1, receipt: { unknown: ["file"] } });
    expect(fs.existsSync(root)).toBe(false); await f.assertNoWrites();
  });
});

const validArgs = ["--session", SESSION, "--creds", "/unused/agent.creds", "--ca", "/unused/ca.pem", "--servers", "tls://localhost:4222", "--machine", MACHINE, "--no-mesh"];
async function cli(args: string[]) {
  let stdout = "", stderr = "";
  const code = await runNatsOwnerCli(args, { out: value => { stdout += value; }, err: value => { stderr += value; } });
  const lines = stdout.trim().split("\n").filter(Boolean);
  return { code, stdout, stderr, lines, receipt: lines.length ? JSON.parse(lines[0]!) as OwnershipReceipt : undefined };
}
describe("owner CLI input and failure output", () => {
  it.each([
    [], [...validArgs, "--machine", "machine-b"], [...validArgs, "--unexpected", "x"],
    validArgs.map(value => value === SESSION ? "not-a-uuid" : value),
    validArgs.map(value => value === "/unused/agent.creds" ? "relative.creds" : value),
    validArgs.map(value => value === "tls://localhost:4222" ? "nats://localhost:4222" : value),
    [...validArgs, "--timeout-ms", "0"], [...validArgs, "--org", "sys"], [...validArgs, "--path-b-machines", "machine-a,machine-a"],
    // The file-side marker is requested unless skipping it is explicit: neither or both refuse.
    validArgs.filter(value => value !== "--no-mesh"), [...validArgs, "--mesh", "/unused/mesh"], [...validArgs, "--no-mesh"],
  ].map(args => ({ args })))("returns usage/exit 2 without echoing raw arguments ($args)", async ({ args }) => {
    const result = await cli(args);
    expect(result.code).toBe(2); expect(result.lines).toHaveLength(1); expect(result.receipt).toHaveProperty("error");
    expect(result.stderr).toContain("Usage:");
  });
  it("prints one safe JSON object on connection/policy failure", async () => {
    vi.stubEnv("PI_FABRIC_MESH_NATS", "invalid-policy-do-not-print");
    const result = await cli(validArgs);
    expect(result).toMatchObject({ code: 1, stderr: "", receipt: { verdict: "stale", unknown: ["connection", "nats"], owner: null } });
    expect(result.lines).toHaveLength(1); expect(result.stdout).not.toContain("invalid-policy-do-not-print");
  });
  it("is reachable as `pi-fabric nats owner` through the standalone CLI entry", async () => {
    const out = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const err = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      const { runFabricCli } = await import("../src/cli/index.js");
      expect(await runFabricCli(["nats", "owner", "--session", "not-a-uuid"])).toBe(2);
      expect(err.mock.calls.flat().join("")).toContain("pi-fabric nats owner");
      expect(JSON.parse(out.mock.calls.flat().join(""))).toMatchObject({ format: 1, unknown: ["connection"] });
    } finally { out.mockRestore(); err.mockRestore(); }
  });
});

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: ownership receipt needs an executable NATS_BIN");
function main(root: string, sessionId: string) {
  const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
    getSessionName: () => "ownership Main", appendEntry() {}, sendMessage() {} } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus() {}, notify() {} } } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: path.resolve("dist/index.js"),
    worker: path.join(root, "unused-worker.mjs"), residentHost: path.join(root, "unused-resident.mjs"), skills: root } });
  return { runtime, context };
}
function configFor(root: string, broker: PathBHarness) {
  return normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: path.join(root, "mesh"),
      nats: { ...broker.runtimeOptions(broker.users.machineA), operatorMachine: "fabric-ops" } },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } });
}
async function snapshot(kv: TransportKv): Promise<Map<string, TransportKvEntry<unknown>>> {
  const stop = new AbortController(), rows = new Map<string, TransportKvEntry<unknown>>();
  try {
    for await (const event of kv.watch(">", AbortSignal.any([stop.signal, AbortSignal.timeout(5_000)]))) {
      if (event.type === "put") rows.set(event.entry.key, event.entry);
      if (event.type === "ready") return rows;
    }
    throw new Error("snapshot never ready");
  } finally { stop.abort(); }
}
async function leaseGone(kv: TransportKv, leaseKey: string): Promise<void> {
  const stop = new AbortController();
  try {
    let present = false;
    for await (const event of kv.watch(">", AbortSignal.any([stop.signal, AbortSignal.timeout(25_000)]))) {
      if (event.type === "put" && event.entry.key === leaseKey) present = true;
      if (event.type === "gone" && event.key === leaseKey) return;
      if (event.type === "ready" && !present) return;
    }
    throw new Error("lease watch ended before expiry");
  } finally { stop.abort(); }
}

describe.skipIf(!binary || process.platform !== "linux")("owner CLI with a real authenticated NATS Main", () => {
  it("reads the broker owner/lease exactly, adds no KV keys, and refuses owned after the Main stops", async () => {
    // Prevent product deletion paths, including accidental duplicate adapters, from deleting real keys.
    const deletes: string[] = [];
    vi.spyOn(Bucket.prototype, "delete").mockImplementation(async () => { throw new Error("broker deletes forbidden"); });
    vi.spyOn(Bucket.prototype, "purge").mockImplementation(async () => { throw new Error("broker purges forbidden"); });
    vi.spyOn(NatsKvStore.prototype, "delete").mockImplementation(async entryKey => { deletes.push(entryKey); return false; });
    const root = tempRoot(), sessionId = randomUUID(), id = `session:${sessionId}`;
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined); vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent")); vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
    let broker: PathBHarness | undefined, inspector: Awaited<ReturnType<typeof openNatsConnection>> | undefined;
    const owner = main(root, sessionId);
    try {
      broker = await createPathBHarness({ binary: binary!, tls: {}, org: "tristar" });
      for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
      expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
      for (const [name, value] of Object.entries(PATH_B_POLICY_ENV)) vi.stubEnv(name, value);
      vi.stubEnv("PI_FABRIC_MESH_NATS", undefined);
      await owner.runtime.initialize(owner.context, configFor(root, broker));
      inspector = await openNatsConnection({ ...broker.runtimeOptions(broker.users.machineB), role: "agent" });
      const presence = await openNatsKv(inspector.js, { bucket: PRESENCE_BUCKET }, false, inspector.onStatus);
      const leases = await openNatsKv(inspector.js, { bucket: LEASES_BUCKET }, false, inspector.onStatus);
      const pkey = key(MACHINE, "p", id), hkey = key(MACHINE, "h", id);
      const beforeP = await snapshot(presence), beforeL = await snapshot(leases);
      const brokerPresence = beforeP.get(pkey)!, brokerLease = beforeL.get(hkey)!;
      expect(brokerPresence).toBeDefined(); expect(brokerLease).toBeDefined();
      const args = ["--session", id, "--creds", broker.users.machineA.credsPath, "--ca", broker.tls!.caFile,
        "--servers", `tls://localhost:${new URL(broker.serverUrl).port}`, "--machine", MACHINE, "--org", "tristar",
        "--path-b-machines", PATH_B_MACHINES.join(","), "--timeout-ms", "5000", "--no-mesh"];
      const beforeDeletes = deletes.length;
      const result = await cli(args);
      expect(result, result.stdout).toMatchObject({ code: 0, stderr: "", receipt: { verdict: "owned", rootId: id, sessionId,
        owner: { machine: MACHINE, hostId: (brokerPresence.value as FabricParticipantRecord).ownerHostId,
          identityId: (brokerPresence.value as FabricParticipantRecord).ownerIdentityId,
          incarnation: (brokerPresence.value as FabricParticipantRecord).ownerIncarnation,
          presenceRevision: brokerPresence.revision,
          lease: { revision: brokerLease.revision, incarnation: (brokerLease.value as FabricKvHostLease).incarnation,
            updatedAt: (brokerLease.value as FabricKvHostLease).updatedAt, expiresAt: (brokerLease.value as FabricKvHostLease).expiresAt } } } });
      expect(result.lines).toHaveLength(1); expect(deletes).toHaveLength(beforeDeletes);
      // The alternate shared policy source, raw UUID syntax and requested file source go
      // through the same real CLI entry, not a mock connection or a hand-built receipt.
      vi.stubEnv("PI_FABRIC_MESH_NATS", JSON.stringify(PATH_B_PERMISSION_POLICY));
      const mesh = tempRoot(), stateFile = path.join(mesh, "state.json");
      fs.writeFileSync(stateFile, JSON.stringify({ format: 1, entries: {} }));
      const fileArgs = [...args.filter(value => value !== "--no-mesh").map(value => value === id ? sessionId : value), "--mesh", mesh];
      const fileBefore = treeSnapshot(mesh), withFile = await cli(fileArgs);
      expect(withFile).toMatchObject({ code: 0, receipt: { verdict: "owned", file: { meshRoot: mesh, handoff: null, mirror: null } } });
      expect(treeSnapshot(mesh)).toEqual(fileBefore);
      fs.writeFileSync(stateFile, "{}");
      const corruptBefore = treeSnapshot(mesh), corrupt = await cli(fileArgs);
      expect(corrupt).toMatchObject({ code: 1, receipt: { verdict: "owned", unknown: ["file"], file: null } });
      expect(treeSnapshot(mesh)).toEqual(corruptBefore);
      expect(deletes).toHaveLength(beforeDeletes);
      expect([...(await snapshot(presence)).keys()].sort()).toEqual([...beforeP.keys()].sort());
      expect([...(await snapshot(leases)).keys()].sort()).toEqual([...beforeL.keys()].sort());
      const stopped = leaseGone(leases, hkey);
      await owner.runtime.shutdown("exit");
      await stopped;
      const stoppedP = await snapshot(presence), stoppedL = await snapshot(leases);
      const after = await cli(args);
      expect(after.code).toBe(1); expect(after.receipt?.verdict).not.toBe("owned"); expect(after.receipt?.unknown).toBeUndefined();
      expect([...(await snapshot(presence)).keys()].sort()).toEqual([...stoppedP.keys()].sort());
      expect([...(await snapshot(leases)).keys()].sort()).toEqual([...stoppedL.keys()].sort());
    } finally {
      try { await owner.runtime.shutdown("exit"); }
      finally { try { await inspector?.close(); } finally { await broker?.stop(); } }
    }
  }, 60_000);
});
