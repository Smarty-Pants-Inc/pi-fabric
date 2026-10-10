import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { trackedTemporaries, writeFileAtomic } from "../src/core/atomic-write.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { sweepAbandonedStateTemporaries } from "../src/mesh/temp-janitor.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { readHostLeaseCurrent, readHostLeases } from "../src/topology/host-leases.js";
import { MAIN_RELOAD_LEASE_MS, ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFiles } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#6622: under mesh-lock load, exits abandoned in-flight state temps and left
// the exiting participant's directory entries fresh.

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map((directory) => directory.close().catch(() => undefined)));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-exit-cleanup-"));
  roots.push(root);
  return path.join(root, "mesh");
};
const writer: MeshIdentity = { id: "session:writer", name: "main", kind: "main", sessionId: "writer" };
const preparedTemps = (meshRoot: string): string[] =>
  fs.readdirSync(meshRoot).filter((name) => name.includes(".prepared.tmp"));
// A live holder that never releases: the store must not take its lock over.
const holdLock = (meshRoot: string): (() => void) => {
  const lockPath = path.join(meshRoot, ".lock");
  fs.mkdirSync(lockPath, { mode: 0o700 });
  fs.writeFileSync(path.join(lockPath, "owner"), `stuck\n${process.pid}\n${Date.now()}\n`);
  return () => fs.rmSync(lockPath, { recursive: true, force: true });
};

// A child Fabric process: `write` stages a large state write that waits for the busy lock;
// `directory` publishes a participant, then begins close() while the lock is busy.
const CHILD = `
  import { createJiti } from "jiti";
  import { pathToFileURL } from "node:url";
  import fs from "node:fs";
  const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
  const { MeshStore } = await jiti.import("./src/mesh/store.ts");
  const { ParticipantDirectory } = await jiti.import("./src/topology/participant-directory.ts");
  const [mode, root, ready, go] = process.argv.slice(1);
  const waitFor = async (check) => { const end = Date.now() + 20000; while (!check()) { if (Date.now() > end) process.exit(3); await new Promise(r => setTimeout(r, 10)); } };
  if (mode === "write") {
    const mesh = new MeshStore(root, 8 * 1024 * 1024, 1000, { lockTimeoutMs: 60000 });
    void mesh.put({ key: "big", value: "x".repeat(1024 * 1024), identity: { id: "session:child", name: "c", kind: "main" } }).catch(() => undefined);
    await waitFor(() => fs.readdirSync(root).some(n => n.startsWith("state.json." + process.pid + ".") && n.endsWith(".prepared.tmp")));
    fs.writeFileSync(ready, "");
    await waitFor(() => fs.existsSync(go));
    process.exit(0); // a Main /quit or a worker exit while the write still waits
  } else {
    const identity = { id: "session:quit", name: "main", kind: "agent", sessionId: "quit" };
    const directory = new ParticipantDirectory(new MeshStore(root, 65536, 1000, { lockTimeoutMs: 60000 }), {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60000, leaseMs: 120000, reapDeadHosts: false,
    });
    directory.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id,
      ownerIdentityId: identity.id, name: "main", status: "idle", runner: "pi", transport: "host", capabilities: [],
      cwd: "/tmp", sessionId: "quit", startedAt: 1, updatedAt: 2, pendingMessages: false, controlProtocol: "v1" }]);
    await directory.start();
    fs.writeFileSync(ready, "");
    await waitFor(() => fs.existsSync(go));
    void directory.close(); // its withdrawal now waits for the busy lock
    await new Promise(r => setTimeout(r, 200));
    process.exit(0); // the host's bounded shutdown ends before close finishes
  }`;

const runChild = (mode: "write" | "directory", meshRoot: string) => {
  const ready = path.join(meshRoot, `../${mode}.ready`);
  const go = path.join(meshRoot, `../${mode}.go`);
  const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, mode, meshRoot, ready, go],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  closed.catch(() => undefined);
  return {
    child, closed, stderr: () => stderr,
    ready: () => vi.waitFor(() => expect(fs.existsSync(ready), stderr).toBe(true), { timeout: 20_000, interval: 20 }),
    go: () => fs.writeFileSync(go, ""),
  };
};

describe("state temp cleanup (smarty-dev#6622)", () => {
  it("an exit while a staged write waits for the busy mesh lock leaves no prepared temp", async () => {
    const meshRoot = tempRoot();
    await new MeshStore(meshRoot, 8 * 1024 * 1024, 1_000).put({ key: "seed", value: 1, identity: writer });
    const release = holdLock(meshRoot);
    try {
      const run = runChild("write", meshRoot);
      await run.ready();
      expect(preparedTemps(meshRoot).filter((name) => name.startsWith(`state.json.${run.child.pid}.`))).toHaveLength(1);
      run.go();
      expect(await run.closed, run.stderr()).toEqual({ code: 0, signal: null });
      expect(preparedTemps(meshRoot)).toEqual([]);
    } finally { release(); }
  }, 30_000);

  it("kill -9 mid-write leaves a temp that the host reaper's janitor removes once its pid is dead and 60 s old", async () => {
    const meshRoot = tempRoot();
    const mesh = new MeshStore(meshRoot, 8 * 1024 * 1024, 1_000, { lockTimeoutMs: 5_000 });
    await mesh.put({ key: "seed", value: 1, identity: writer });
    const release = holdLock(meshRoot);
    let orphan: string;
    try {
      const run = runChild("write", meshRoot);
      await run.ready();
      run.child.kill("SIGKILL");
      expect(await run.closed).toMatchObject({ signal: "SIGKILL" });
      const left = preparedTemps(meshRoot);
      expect(left).toHaveLength(1); // no hook runs on SIGKILL
      orphan = left[0]!;
      expect(fs.statSync(path.join(meshRoot, orphan)).size).toBeGreaterThan(1024 * 1024);
    } finally { release(); }
    // A live pid's temp (our parent's) and a fresh dead-pid temp are never touched.
    const live = `state.json.${process.ppid}.${randomUUID()}.prepared.tmp`;
    fs.writeFileSync(path.join(meshRoot, live), "live");
    await mesh.put({ key: "after", value: 2, identity: writer });
    // Too young: kept.
    expect(sweepAbandonedStateTemporaries(meshRoot)).toEqual([]);
    const old = new Date(Date.now() - 2 * 60_000);
    fs.utimesSync(path.join(meshRoot, orphan), old, old);
    fs.utimesSync(path.join(meshRoot, live), old, old);
    // The existing host-reaper path runs the janitor (no new unit).
    await reapDeadHostRecords(mesh, writer, { ownHostId: "session:writer" });
    expect(preparedTemps(meshRoot)).toEqual([live]);
    expect(mesh.get("after", { fresh: true })?.value).toBe(2);
  }, 30_000);

  it("own-pid temps are tracked only while staged; a failed staging leaves nothing", () => {
    const meshRoot = tempRoot();
    fs.mkdirSync(meshRoot, { recursive: true });
    writeFileAtomic(path.join(meshRoot, "ok.json"), "{}");
    expect(trackedTemporaries()).toEqual([]);
    // The target is a directory: the rename fails, the temp is removed and released.
    fs.mkdirSync(path.join(meshRoot, "dir"));
    fs.writeFileSync(path.join(meshRoot, "dir", "child"), "");
    expect(() => writeFileAtomic(path.join(meshRoot, "dir"), "{}", { renameRetries: 1 })).toThrow();
    expect(trackedTemporaries()).toEqual([]);
    expect(fs.readdirSync(meshRoot).filter((name) => name.endsWith(".tmp"))).toEqual([]);
    // Our own pid's temp is never janitor work, however old.
    const own = path.join(meshRoot, `state.json.${process.pid}.${randomUUID()}.prepared.tmp`);
    fs.writeFileSync(own, "");
    const old = new Date(Date.now() - 10 * 60_000);
    fs.utimesSync(own, old, old);
    expect(sweepAbandonedStateTemporaries(meshRoot, { dead: () => true })).toEqual([]);
  });
});

const record = (id: string, kind: "root" | "agent", hostId: string): FabricParticipantRecord => ({
  format: 1, id, kind, rootId: hostId, ownerHostId: hostId, ownerIdentityId: hostId,
  ...(kind === "agent" ? { parentId: hostId } : {}),
  name: id, status: kind === "root" ? "idle" : "running", runner: "pi", transport: kind === "root" ? "host" : "process",
  capabilities: [], cwd: "/tmp/project", ...(kind === "root" ? { sessionId: "reload", pendingMessages: false } : {}),
  startedAt: 1, updatedAt: 2, controlProtocol: "v1",
} as FabricParticipantRecord);

// A reloading Main with one child, under a short mesh lock timeout; its exit hook is returned.
const reloadingDirectory = async (meshRoot: string, warnings: string[]) => {
  const identity: MeshIdentity = { id: "session:reload", name: "main", kind: "main", sessionId: "reload" };
  const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000, { lockTimeoutMs: 200 });
  // Install the process-wide atomic-write temp hook before measuring the directory's
  // own exit hook: this fixture must also work when run without the state-temp tests.
  await mesh.put({ key: "fixture/seed", value: true, identity });
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 200, reapDeadHosts: false,
    onWithdrawalFailure: (message) => warnings.push(message),
  });
  directory.registerSource(() => [record(identity.id, "root", identity.id), record("agent:child", "agent", identity.id)]);
  directories.push(directory);
  const listeners = process.listeners("exit");
  await directory.start();
  const added = process.listeners("exit").filter((listener) => !listeners.includes(listener));
  expect(added).toHaveLength(1);
  await directory.quiesce("reload");
  // A peer reads the shared directory: is the child still listed, and is it fresh?
  const observer = new ParticipantDirectory(mesh, {
    enabled: true, hostId: "session:observer", rootId: "session:observer", reapDeadHosts: false,
    identity: { id: "session:observer", name: "observer", kind: "main", sessionId: "observer" },
  });
  const child = (now: number) => observer.list({ includeStale: true, fresh: true }, now).find((entry) => entry.id === "agent:child");
  return { identity, mesh, directory, hook: added[0]!, child, hooked: () => process.listeners("exit").includes(added[0]!) };
};

// A failed withdrawal keeps its entries, stops renewing the lease and keeps the exit hook,
// so the entries lapse within that lease, and log once (smarty-dev#6622, review round 2).
const expectLapse = async (setup: Awaited<ReturnType<typeof reloadingDirectory>>, meshRoot: string, warnings: string[]) => {
  const { identity, child, hook, hooked } = setup;
  expect(child(Date.now())).toMatchObject({ stale: false });
  expect(hooked()).toBe(true);
  expect(warnings).toHaveLength(1);
  expect(warnings[0]).toMatch(/participant withdrawal for session:reload did not commit/);
  const lease = readHostLeaseCurrent(meshRoot, identity.id)!;
  expect(lease).toBeDefined();
  // Several heartbeats later nothing renewed the lease.
  await new Promise((resolve) => setTimeout(resolve, 500));
  expect(readHostLeaseCurrent(meshRoot, identity.id)).toEqual(lease);
  // Within one (reload) lease the leftover child lapses on its own.
  expect(lease.expiresAt - Date.now()).toBeLessThanOrEqual(MAIN_RELOAD_LEASE_MS);
  expect(child(lease.expiresAt + 1)).toMatchObject({ stale: true });
  await setup.directory.close();
  expect(warnings).toHaveLength(1);
  // An exit drops this incarnation's lease file; the stored lease bounds the rest.
  (hook as () => void)();
  expect(readHostLeaseCurrent(meshRoot, identity.id)).toBeUndefined();
  expect(child(lease.expiresAt + 1)).toMatchObject({ stale: true });
  process.removeListener("exit", hook);
};

describe("directory withdrawal on exit (smarty-dev#6622)", () => {
  it("a reload withdrawal that exhausts its lock retries lets the entries expire with their unrenewed lease", async () => {
    const meshRoot = tempRoot();
    const warnings: string[] = [];
    const setup = await reloadingDirectory(meshRoot, warnings);
    const release = holdLock(meshRoot);
    try { await setup.directory.close(); } finally { release(); }
    expect(setup.mesh.listAll("topology/participants/", { fresh: true }).map((entry) => (entry.value as { id: string }).id))
      .toContain("agent:child");
    await expectLapse(setup, meshRoot, warnings);
  }, 20_000);

  // An exit while close() still awaits its withdrawal is an un-withdrawn close: the hook must
  // not preserve the reload lease (smarty-dev#6622, review round 4).
  it("an exit while a reload close() awaits a lock-blocked withdrawal never preserves the reload lease", async () => {
    const meshRoot = tempRoot();
    const warnings: string[] = [];
    const setup = await reloadingDirectory(meshRoot, warnings);
    const { identity, child, hook, hooked } = setup;
    const lease = readHostLeaseCurrent(meshRoot, identity.id)!;
    expect(lease).toBeDefined();
    const release = holdLock(meshRoot);
    let closing: Promise<void> | undefined;
    try {
      const batches = vi.spyOn(setup.mesh, "writeBatch");
      closing = setup.directory.close();
      // close() is now inside its withdrawal, blocked on the held mesh lock.
      await vi.waitFor(() => expect(batches).toHaveBeenCalled(), { timeout: 5_000, interval: 5 });
      expect(warnings).toEqual([]);
      expect(hooked()).toBe(true);
      (hook as () => void)(); // the process exits here
      expect(readHostLeaseCurrent(meshRoot, identity.id)).toBeUndefined();
      await closing;
    } finally { release(); await closing?.catch(() => undefined); }
    process.removeListener("exit", hook);
    // The child was never withdrawn; with no lease file it lapses within one (reload) lease.
    expect(setup.mesh.listAll("topology/participants/", { fresh: true }).map((entry) => (entry.value as { id: string }).id))
      .toContain("agent:child");
    expect(readHostLeaseCurrent(meshRoot, identity.id)).toBeUndefined();
    expect(lease.expiresAt - Date.now()).toBeLessThanOrEqual(MAIN_RELOAD_LEASE_MS);
    expect(child(lease.expiresAt + 1)).toMatchObject({ stale: true });
  }, 20_000);

  it("a reload withdrawal whose delete is always skipped on a conflict counts as not done", async () => {
    const meshRoot = tempRoot();
    const warnings: string[] = [];
    const setup = await reloadingDirectory(meshRoot, warnings);
    const { mesh, identity } = setup;
    const writeBatch = mesh.writeBatch.bind(mesh);
    let contended = 0;
    // A concurrent rewrite of the child moves its version before every withdrawal commit.
    vi.spyOn(mesh, "writeBatch").mockImplementation(async (input) => {
      if (input.ops.length > 0 && input.ops.every((op) => op.kind === "delete")) {
        for (const op of input.ops) {
          const current = mesh.get(op.key, { fresh: true });
          if ((current?.value as { id?: string } | undefined)?.id !== "agent:child") continue;
          contended++;
          await mesh.put({ key: op.key, value: { ...(current!.value as object), updatedAt: Date.now() }, identity });
        }
      }
      return writeBatch(input);
    });
    await setup.directory.close();
    expect(contended).toBeGreaterThanOrEqual(3);
    vi.mocked(mesh.writeBatch).mockRestore();
    expect(mesh.listAll("topology/participants/", { fresh: true }).map((entry) => (entry.value as { id: string }).id))
      .toContain("agent:child");
    await expectLapse(setup, meshRoot, warnings);
  }, 20_000);

  it("a skipped delete is retried: one conflict still withdraws and retires the exit hook", async () => {
    const meshRoot = tempRoot();
    const warnings: string[] = [];
    const setup = await reloadingDirectory(meshRoot, warnings);
    const { mesh, identity } = setup;
    const writeBatch = mesh.writeBatch.bind(mesh);
    let contended = 0;
    vi.spyOn(mesh, "writeBatch").mockImplementation(async (input) => {
      const op = input.ops.find((candidate) => candidate.kind === "delete" &&
        (mesh.get(candidate.key, { fresh: true })?.value as { id?: string } | undefined)?.id === "agent:child");
      if (op && contended++ === 0) {
        const current = mesh.get(op.key, { fresh: true })!;
        await mesh.put({ key: op.key, value: { ...(current.value as object), updatedAt: Date.now() }, identity });
      }
      return writeBatch(input);
    });
    await setup.directory.close();
    expect(contended).toBe(2);
    expect(mesh.listAll("topology/participants/", { fresh: true }).map((entry) => (entry.value as { id: string }).id))
      .toEqual([identity.id]);
    expect(setup.hooked()).toBe(false);
    expect(warnings).toEqual([]);
  }, 20_000);

  it("/reload while the mesh lock is busy leaves no stale entry", async () => {
    const meshRoot = tempRoot();
    const identity: MeshIdentity = { id: "session:reload", name: "main", kind: "main", sessionId: "reload" };
    const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000, { lockTimeoutMs: 300 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false,
    });
    directory.registerSource(() => [record(identity.id, "root", identity.id), record("agent:child", "agent", identity.id)]);
    directories.push(directory);
    await directory.start();
    const ids = () => mesh.listAll("topology/participants/", { fresh: true })
      .map((entry) => (entry.value as { id: string }).id).sort();
    expect(ids()).toEqual(["agent:child", identity.id]);
    await directory.quiesce("reload");
    // A peer's write holds the lock across this runtime's first withdrawal attempt.
    const release = holdLock(meshRoot);
    const freed = new Promise<void>((resolve) => setTimeout(() => { release(); resolve(); }, 450));
    const listeners = process.listeners("exit").length;
    await directory.close();
    await freed;
    // A committed withdrawal retires the exit hook.
    expect(process.listeners("exit").length).toBe(listeners - 1);
    // Only the reloading root survives, as a bounded reload lease; the child is withdrawn.
    expect(ids()).toEqual([identity.id]);
    const root = mesh.listAll("topology/participants/", { fresh: true })[0]!.value as { status: string; reloadUntil?: number };
    expect(root.status).toBe("reloading");
    expect(root.reloadUntil).toBeGreaterThan(Date.now());
    expect(readParticipantFiles(meshRoot, { maxAgeMs: 0 }).map((entry) => (entry.value as { id: string }).id))
      .not.toContain("agent:child");
  }, 15_000);

  // A resident's idle exit under a held mesh lock must end within its shutdown window
  // (residency-outbox F6): one batch, at most two lock waits (smarty-dev#6622, round 3).
  it("a full close withdraws records and host entry in one batch and waits at most two lock timeouts", async () => {
    const meshRoot = tempRoot();
    const identity: MeshIdentity = { id: "session:idle", name: "main", kind: "main", sessionId: "idle" };
    const open = async () => {
      const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000, { lockTimeoutMs: 300 });
      const warnings: string[] = [];
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false,
        onWithdrawalFailure: (message) => warnings.push(message),
      });
      directory.registerSource(() => [record(identity.id, "root", identity.id), record("agent:child", "agent", identity.id)]);
      directories.push(directory);
      await directory.start();
      return { mesh, directory, warnings, batches: vi.spyOn(mesh, "writeBatch") };
    };
    const ownKeys = (mesh: MeshStore) => [...mesh.listAll("topology/participants/", { fresh: true }), ...mesh.listAll("topology/hosts/", { fresh: true })]
      .filter((entry) => entry.updatedBy.id === identity.id);

    const free = await open();
    expect(ownKeys(free.mesh).some((entry) => entry.key.startsWith("topology/hosts/"))).toBe(true);
    expect(ownKeys(free.mesh).some((entry) => entry.key.startsWith("topology/participants/"))).toBe(true);
    await free.directory.close();
    expect(free.batches).toHaveBeenCalledOnce();
    expect(ownKeys(free.mesh)).toEqual([]);
    expect(free.warnings).toEqual([]);

    const busy = await open();
    const release = holdLock(meshRoot);
    const started = Date.now();
    try { await busy.directory.close(); } finally { release(); }
    const elapsed = Date.now() - started;
    expect(busy.batches).toHaveBeenCalledTimes(2);
    expect(elapsed).toBeLessThan(3 * 300 + 400);
    expect(busy.warnings).toHaveLength(1);
    expect(readHostLeases(meshRoot).has(identity.id)).toBe(false);
  }, 20_000);

  it("an exit while close() waits for the busy lock does not leave the host lease fresh", async () => {
    const meshRoot = tempRoot();
    const run = runChild("directory", meshRoot);
    await run.ready();
    expect(readHostLeases(meshRoot).get("session:quit")?.expiresAt).toBeGreaterThan(Date.now());
    const release = holdLock(meshRoot);
    try {
      run.go();
      expect(await run.closed, run.stderr()).toEqual({ code: 0, signal: null });
      expect(readHostLeases(meshRoot).has("session:quit")).toBe(false);
    } finally { release(); }
  }, 30_000);
});
