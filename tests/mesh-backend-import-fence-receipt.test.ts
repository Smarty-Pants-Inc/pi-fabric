import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readPhysicalHostIdentity, type StaleLockNote } from "../src/core/atomic-write.js";
import { ownPidNamespace } from "../src/core/pid-namespace.js";
import { meshBackendStatus } from "../src/mesh/backend-migration.js";
import { IMPORT_FENCE_RECEIPT, recoverKilledImportLock } from "../src/mesh/import-fence-receipt.js";
import { main, meshWriterCensusSync } from "../src/mesh/mesh-backend-cli.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#9501: a SIGKILLed import leaves its own fence (.lock, custody.lock). The exact rerun's readiness gate
// takes its census before the fence and used to require fabric@unknown for that dead owner forever. The import's
// receipt lets the CLI preflight hand exactly that owner to the mesh lock's ordinary stale recovery; everything else
// (live holder, other namespace, other bytes, other root/host, legacy residue, protocol 3) stays the gate's.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const dirs: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

const fileRoot = async (): Promise<string> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-import-fence-"));
  dirs.push(root);
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
  await store.put({ key: "seed/a", value: { n: 1 }, identity });
  store.closeState();
  return root;
};

/** The real CLI with the real (pre-fence and fenced) census; no built-in readers, no foreign pid scan. */
const cli = async (argv: string[]): Promise<{ code: number; out: string; err: string }> => {
  let out = "";
  let err = "";
  const root = argv[argv.indexOf("--root") + 1]!;
  const code = await main(argv, { gateCensus: meshWriterCensusSync(root), builtinReaders: [], procScan: { listPids: () => [] },
    stdout: (text) => { out += text; }, stderr: (text) => { err += text; } });
  return { code, out, err };
};

const CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const migration = await jiti.import("./src/mesh/backend-migration.ts");
const root = process.argv.at(-1);
await migration.importMeshState(root, { lockProtocol: 3, beforeOpen: () => { process.kill(process.pid, "SIGKILL"); for (;;) { /* never */ } } });
`;
/** A real import, SIGKILLed under its fence right before it would open state.db (beforeOpen). */
const killedImport = (root: string): Promise<{ pid: number; signal: NodeJS.Signals | null; stderr: string }> => new Promise((resolve, reject) => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, root], { cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe"] });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.once("error", reject);
  child.once("close", (_code, signal) => resolve({ pid: child.pid!, signal, stderr }));
});

// P2 data-safety regression: fault only at the real fs.renameSync boundary, never stub the stale proof or gate.
// Children keep NODE_OPTIONS and the unchanged deletion guard; the captured rename delegates to that guard/native fs.
const RECOVERY_CHILD = `
import fs from "node:fs";
import path from "node:path";
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
if (!process[Symbol.for("pi-fabric.test-deletion-guard")]) throw new Error("recovery child lost deletion guard");
const [root, boundary] = process.argv.slice(-2);
const rename = fs.renameSync;
fs.renameSync = function(from, to) {
  const canonical = String(from) === path.join(root, ".lock") && String(to).startsWith(path.join(root, ".lock.dead."));
  const ticket = String(to) === String(from) + ".recovered";
  if ((canonical && boundary !== "before-ticket") || (ticket && boundary === "before-ticket")) {
    if (boundary === "after-lock") rename.call(fs, from, to);
    process.kill(process.pid, "SIGKILL");
    for (;;) { /* no continuation after the fault */ }
  }
  return rename.call(fs, from, to);
};
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const cli = await jiti.import("./src/mesh/mesh-backend-cli.ts");
process.exitCode = await cli.main(["import", "--root", root], {
  gateCensus: cli.meshWriterCensusSync(root), builtinReaders: [], procScan: { listPids: () => [] },
});
`;
const killedRecovery = (root: string, boundary: string): Promise<{ signal: NodeJS.Signals | null; stderr: string }> => new Promise((resolve, reject) => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", RECOVERY_CHILD, root, boundary], {
    cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe"], timeout: 30_000,
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.once("error", reject);
  child.once("close", (_code, signal) => resolve({ signal, stderr }));
});

/** A pid that ran and exited (ESRCH now). */
const deadPid = (): Promise<number> => new Promise((resolve, reject) => {
  const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
  child.once("error", reject);
  child.once("close", () => resolve(child.pid!));
});

const lockOwner = (root: string, lock = ".lock"): string | undefined => {
  try { return fs.readFileSync(path.join(root, lock, "owner"), "utf8"); } catch { return undefined; }
};
const deadFences = (root: string): string[] => fs.readdirSync(root).filter(name => name.startsWith(".lock.dead."));
const queueNames = (root: string): string[] => { try { return fs.readdirSync(meshLockQueueDirectory(root)).sort(); } catch { return []; } };
const MESH_TOKEN = "11111111-2222-4333-8444-555555555555";
/** FIFO tickets as lock-queue.ts names them: the forged holder's own, and an unrelated waiter's. */
const placeTickets = (root: string, pid: number): { own: string; other: string } => {
  const queue = meshLockQueueDirectory(root);
  fs.mkdirSync(queue, { recursive: true, mode: 0o700 });
  const own = `${"1".padStart(24, "0")}-${pid}-${MESH_TOKEN}`;
  const other = `${"2".padStart(24, "0")}-${pid}-99999999-2222-4333-8444-555555555555`;
  for (const name of [own, other]) fs.writeFileSync(path.join(queue, name), "", { mode: 0o600 });
  return { own, other };
};

interface Forged { mesh: string; custody: string; receipt: Record<string, unknown> }
/** Import residue with the requested owner format; `patch` alters the receipt only, never its owner's namespace. */
const forge = async (root: string, pid: number, patch: Record<string, unknown> = {}, ownerProtocol: 1 | 2 | 3 = 1): Promise<Forged> => {
  const namespace = await ownPidNamespace();
  const mesh = `${MESH_TOKEN}\n${pid}\n${Date.now()}\n${ownerProtocol >= 2 ? "123456\n" : ""}${ownerProtocol === 3 ? `${namespace}\n` : ""}`;
  const custody = `66666666-7777-4888-8999-aaaaaaaaaaaa\n${pid}\n${Date.now()}\n`;
  for (const [lock, owner] of [[".lock", mesh], ["custody.lock", custody]] as const) {
    fs.mkdirSync(path.join(root, lock), { mode: 0o700 });
    fs.writeFileSync(path.join(root, lock, "owner"), owner, { mode: 0o600 });
  }
  const real = fs.realpathSync(root);
  const stat = fs.statSync(real, { bigint: true });
  const host = readPhysicalHostIdentity()!;
  const receipt = { format: 1, kind: "fabric-mesh-import-fence", root: real, rootDev: String(stat.dev), rootIno: String(stat.ino),
    pid, start: "123456", machineId: host.machineId, bootId: host.bootId, namespace, mesh, custody, ...patch };
  fs.writeFileSync(path.join(root, IMPORT_FENCE_RECEIPT), JSON.stringify(receipt), { mode: 0o600 });
  return { mesh, custody, receipt };
};

const linuxIdentity = process.platform === "linux" && readPhysicalHostIdentity() !== undefined;

describe.skipIf(!linuxIdentity)("smarty-dev#9501 killed import fence receipt", () => {
  it("the exact rerun of a SIGKILLed import converges through the real CLI gate", async () => {
    const root = await fileRoot();
    const killed = await killedImport(root);
    expect(killed.signal, killed.stderr).toBe("SIGKILL");
    // The killed import's own residue, and the receipt it wrote under the fence before beforeOpen.
    const mesh = lockOwner(root)!;
    expect(mesh.split("\n")[1]).toBe(String(killed.pid));
    expect(mesh.split("\n")[4]).toBe(await ownPidNamespace());
    expect(lockOwner(root, "custody.lock")!.split("\n")[1]).toBe(String(killed.pid));
    const receipt = JSON.parse(fs.readFileSync(path.join(root, IMPORT_FENCE_RECEIPT), "utf8")) as Record<string, unknown>;
    expect(receipt).toMatchObject({ format: 1, pid: killed.pid, mesh, custody: lockOwner(root, "custody.lock") });
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);

    // Its FIFO ticket, when admission was open (the census reports it once the owner is gone).
    const ticket = queueNames(root).find(name => name.endsWith(`-${killed.pid}-${mesh.split("\n")[0]}`));
    const rerun = await cli(["import", "--root", root]);
    expect(rerun.err).toContain(`recovered the mesh .lock of killed import pid ${killed.pid}`);
    // The killed holder's FIFO ticket was retired (renamed, not deleted) under the same proof.
    if (ticket) {
      expect(rerun.err).toContain(`ticket ${ticket} retired`);
      expect(queueNames(root)).toContain(`${ticket}.recovered`);
    }
    expect(queueNames(root).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name))).toEqual([]);
    expect(rerun.err).not.toContain("fabric@unknown");
    expect(rerun.code, rerun.err).toBe(0);
    expect((await meshBackendStatus(root)).backend).toBe("sqlite");
    // Recovery renamed the dead owner's directory into a retained receipt; the fence's own recovery took custody.lock.
    expect(deadFences(root)).toHaveLength(1);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(fs.existsSync(path.join(root, "custody.lock"))).toBe(false);
  }, 60_000);

  it("lock and ticket residue together: the CLI rerun recovers both, then the fence recovers custody.lock", async () => {
    const root = await fileRoot();
    const pid = await deadPid();
    await forge(root, pid, {}, 3);
    const { own, other } = placeTickets(root, pid);
    const rerun = await cli(["import", "--root", root]);
    expect(rerun.err).toContain(`ticket ${own} retired`);
    // The unrelated ticket (another token of the same pid) is never touched by the preflight.
    expect(rerun.err).toContain(`lock-ticket ${other}`);
    expect(rerun.code).toBe(3);
    expect(queueNames(root)).toEqual([other, `${own}.recovered`].sort());
    // Without that unrelated waiter's ticket, the same residue converges.
    const clean = await fileRoot();
    await forge(clean, await deadPid(), {}, 3);
    const ticket = placeTickets(clean, Number(lockOwner(clean)!.split("\n")[1]));
    fs.renameSync(path.join(meshLockQueueDirectory(clean), ticket.other), path.join(meshLockQueueDirectory(clean), `${ticket.other}.test-retired`));
    const converged = await cli(["import", "--root", clean]);
    expect(converged.code, converged.err).toBe(0);
    expect(converged.err).toContain(`ticket ${ticket.own} retired`);
    expect((await meshBackendStatus(clean)).backend).toBe("sqlite");
    expect(fs.existsSync(path.join(clean, "custody.lock"))).toBe(false);
  }, 60_000);

  it.each(["before-ticket", "before-lock", "after-lock"])("recovery killed at %s native rename boundary reruns through the real CLI gate", async boundary => {
    const root = await fileRoot();
    const pid = await deadPid();
    const { mesh, custody } = await forge(root, pid, {}, 3);
    const { own, other } = placeTickets(root, pid);
    const queue = meshLockQueueDirectory(root);
    // Isolate the exact owner's ticket; unrelated active tickets still refuse (covered by the preceding test).
    fs.renameSync(path.join(queue, other), path.join(queue, `${other}.test-retired`));
    const killed = await killedRecovery(root, boundary);
    expect(killed.signal, killed.stderr).toBe("SIGKILL");
    expect(lockOwner(root)).toBe(boundary === "after-lock" ? undefined : mesh);
    expect(lockOwner(root, "custody.lock")).toBe(custody);
    expect(deadFences(root)).toHaveLength(boundary === "after-lock" ? 1 : 0);
    // RED on the old head at after-lock: its canonical owner is gone but this ticket remains fabric@unknown.
    // Earlier interruption either retains the ticket AND canonical proof, or has already retired the ticket.
    const activeAtKill = fs.existsSync(path.join(queue, own));
    const retiredAtKill = fs.existsSync(path.join(queue, `${own}.recovered`));
    const rerun = await cli(["import", "--root", root]);
    expect(rerun.code, rerun.err).toBe(0);
    expect(rerun.err).not.toContain("fabric@unknown");
    expect(activeAtKill).toBe(boundary === "before-ticket");
    expect(retiredAtKill).toBe(boundary !== "before-ticket");
    expect((await meshBackendStatus(root)).backend).toBe("sqlite");
    expect(queueNames(root)).toEqual([`${other}.test-retired`, `${own}.recovered`].sort());
    expect(deadFences(root)).toHaveLength(1);
    expect(lockOwner(root)).toBeUndefined();
    expect(lockOwner(root, "custody.lock")).toBeUndefined();
  }, 60_000);

  it("legacy residue without a receipt stays fabric@unknown (compatibility limit)", async () => {
    const root = await fileRoot();
    await forge(root, await deadPid());
    fs.rmSync(path.join(root, IMPORT_FENCE_RECEIPT));
    const before = lockOwner(root);
    const rerun = await cli(["import", "--root", root]);
    expect(rerun.code).toBe(3);
    expect(rerun.err).toContain("fabric@unknown");
    expect(lockOwner(root)).toBe(before);
    expect(deadFences(root)).toEqual([]);
  }, 30_000);

  it.each(["local", "foreign"])("a live import-owned holder is never touched; the gate refuses it as fabric@unknown without waiting (%s namespace)", async context => {
    const root = await fileRoot();
    const namespace = (await ownPidNamespace())!;
    const { mesh } = await forge(root, process.ppid, context === "foreign"
      ? { namespace: namespace.replace(/:(\d+)$/, (_m, n: string) => `:${Number(n) + 1}`) } : {});
    placeTickets(root, process.ppid);
    const queued = queueNames(root);
    expect(await recoverKilledImportLock(root)).toBeUndefined();
    const started = Date.now();
    const rerun = await cli(["import", "--root", root]);
    expect(rerun.code).toBe(3);
    expect(rerun.err).toContain("fabric@unknown");
    expect(Date.now() - started).toBeLessThan(20_000);
    expect(queueNames(root)).toEqual(queued);
    expect(lockOwner(root)).toBe(mesh);
    expect(deadFences(root)).toEqual([]);
  }, 30_000);

  // Trust-boundary/data-safety regression: the mutable receipt is not authority for a legacy holder's namespace.
  it.each([1, 2] as const)("a forged local receipt cannot detach a LIVE foreign namespace-less protocol-%s holder invisible here", async protocol => {
    const root = await fileRoot();
    const pid = process.ppid;
    const kill = process.kill.bind(process);
    expect(kill(pid, 0)).toBe(true); // a genuinely live holder; only this namespace's view is injected below
    const { mesh, custody } = await forge(root, pid, {}, protocol);
    placeTickets(root, pid);
    const queued = queueNames(root);
    const state = fs.readFileSync(path.join(root, "state.json"));
    // Like mesh-lock-namespace's looksDead seam: a live foreign PID answers ESRCH in the recovering namespace.
    vi.spyOn(process, "kill").mockImplementation(((target: number, signal?: string | number) => {
      if (target === pid && signal === 0) throw Object.assign(new Error("foreign pid invisible here"), { code: "ESRCH" });
      return kill(target, signal as never);
    }) as typeof process.kill);
    expect.soft(await recoverKilledImportLock(root, protocol)).toBeUndefined();
    const rerun = await cli(["import", "--root", root, "--lock-protocol", String(protocol)]);
    expect(rerun.code, rerun.err).toBe(3);
    expect(rerun.err).toContain("fabric@unknown");
    expect(lockOwner(root)).toBe(mesh);
    expect(lockOwner(root, "custody.lock")).toBe(custody);
    expect(queueNames(root)).toEqual(queued);
    expect(deadFences(root)).toEqual([]);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    expect(fs.readFileSync(path.join(root, "state.json")).equals(state)).toBe(true);
    expect(kill(pid, 0)).toBe(true);
  }, 30_000);

  it.each([1, 2] as const)("a DEAD namespace-less protocol-%s holder with a matching receipt still needs the explicit override", async protocol => {
    const root = await fileRoot();
    const pid = await deadPid();
    const { mesh, custody } = await forge(root, pid, {}, protocol);
    placeTickets(root, pid);
    const queued = queueNames(root);
    const state = fs.readFileSync(path.join(root, "state.json"));
    const rerun = await cli(["import", "--root", root, "--lock-protocol", String(protocol)]);
    expect(rerun.code, rerun.err).toBe(3);
    expect(rerun.err).toContain("fabric@unknown");
    expect(rerun.err).not.toContain("recovered the mesh .lock");
    expect(lockOwner(root)).toBe(mesh);
    expect(lockOwner(root, "custody.lock")).toBe(custody);
    expect(queueNames(root)).toEqual(queued);
    expect(deadFences(root)).toEqual([]);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    expect(fs.readFileSync(path.join(root, "state.json")).equals(state)).toBe(true);
  }, 30_000);

  it.each([1, 2] as const)("--accept-unready fabric@unknown imports a verified-dead legacy protocol-%s holder without losing state", async protocol => {
    const root = await fileRoot();
    await forge(root, await deadPid(), {}, protocol);
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", "session:0a1b2c3d-0000-4000-8000-00000000c943");
    const rerun = await cli(["import", "--root", root, "--lock-protocol", String(protocol), "--accept-unready", "fabric@unknown"]);
    expect(rerun.code, rerun.err).toBe(0);
    expect((await meshBackendStatus(root)).backend).toBe("sqlite");
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
    try { expect(store.get("seed/a")?.value).toEqual({ n: 1 }); }
    finally { store.closeState(); }
  }, 30_000);

  it.each([1, 2, 3] as const)("recovers a dead receipt-matched namespaced owner, and only it, under protocol %s", async protocol => {
    const root = await fileRoot();
    const pid = await deadPid();
    await forge(root, pid, {}, 3);
    const { own, other } = placeTickets(root, pid);
    expect(await recoverKilledImportLock(root, protocol)).toContain(`killed import pid ${pid}`);
    // Only the dead owner's own ticket (its pid and token) leaves the ticket namespace, renamed and retained.
    expect(queueNames(root)).toEqual([other, `${own}.recovered`].sort());
    expect(lockOwner(root)).toBeUndefined();
    expect(deadFences(root)).toHaveLength(1);
    // custody.lock is the fence's own recovery, never the preflight's.
    expect(lockOwner(root, "custody.lock")).toBeDefined();
  });

  const refusals: Array<[string, (root: string, forged: Forged) => Record<string, unknown> | void, (1 | 2 | 3)?]> = [
    ["a successor holds .lock (other owner bytes)", (root) => {
      fs.writeFileSync(path.join(root, ".lock", "owner"), `99999999-2222-4333-8444-555555555555\n${process.ppid}\n${Date.now()}\n`);
    }],
    ["custody.lock is not the receipt's", (root) => {
      fs.writeFileSync(path.join(root, "custody.lock", "owner"), `99999999-7777-4888-8999-aaaaaaaaaaaa\n1\n${Date.now()}\n`);
    }],
    ["another root inode", () => ({ rootIno: "1" })],
    ["another root path", () => ({ root: "/elsewhere" })],
    ["another machine", () => ({ machineId: "0".repeat(32) })],
    ["another boot", () => ({ bootId: "00000000-0000-4000-8000-000000000000" })],
    ["another PID namespace (live holder elsewhere on this host shares machine and boot)", (_root, forged) => {
      const own = String(forged.receipt.namespace);
      return { namespace: own.replace(/:(\d+)$/, (_m, n: string) => `:${Number(n) + 1}`) };
    }],
    ["an unknown (empty) PID namespace", () => ({ namespace: "" })],
    ["an unproven (malformed) PID namespace", () => ({ namespace: "linux:not-a-namespace" })],
    ["the receipt's pid is not the owner's", () => ({ pid: 1 })],
    ["an unrecognized receipt format", () => ({ format: 2 })],
    ["protocol 3 never upgrades a namespace-less record", () => undefined, 3],
  ];
  it.each(refusals)("does nothing when %s", async (_label, alter, protocol) => {
    const root = await fileRoot();
    const pid = await deadPid();
    const forged = await forge(root, pid);
    placeTickets(root, pid);
    const queued = queueNames(root);
    const patch = alter(root, forged);
    if (patch) fs.writeFileSync(path.join(root, IMPORT_FENCE_RECEIPT), JSON.stringify({ ...forged.receipt, ...patch }));
    const mesh = lockOwner(root);
    expect(await recoverKilledImportLock(root, protocol ?? 1)).toBeUndefined();
    expect(lockOwner(root)).toBe(mesh);
    expect(deadFences(root)).toEqual([]);
    expect(queueNames(root)).toEqual(queued);
    // Exercise the real gate too: foreign receipt context and unproven protocol-3 legacy records must still refuse,
    // with the canonical owner and every FIFO ticket intact; no accept-unready or census weakening.
    const rerun = await cli(["import", "--root", root, "--lock-protocol", String(protocol ?? 1)]);
    expect(rerun.code, rerun.err).toBe(3);
    expect(rerun.err).toContain("fabric@unknown");
    expect(lockOwner(root)).toBe(mesh);
    expect(queueNames(root)).toEqual(queued);
    expect(deadFences(root)).toEqual([]);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  });

  it("a torn receipt does nothing", async () => {
    const root = await fileRoot();
    await forge(root, await deadPid());
    fs.writeFileSync(path.join(root, IMPORT_FENCE_RECEIPT), "{\"format\":1,");
    const mesh = lockOwner(root);
    expect(await recoverKilledImportLock(root)).toBeUndefined();
    expect(lockOwner(root)).toBe(mesh);
  });

  it("clearStaleLock checks the expected owner inside the recovery, after its own read", async () => {
    const root = await fileRoot();
    const { mesh, receipt } = await forge(root, await deadPid(), {}, 3);
    const lock = new MeshLock(root, { lockProtocol: 1 }, () => undefined);
    const ownerPath = path.join(lock.lockPath, "owner");
    // A dead owner, but not the expected bytes: refused although it is recoverable.
    expect(await lock.clearStaleLock(ownerPath, Date.now() + 2_000, {}, { owner: `${mesh}x`, namespace: String(receipt.namespace) })).toBe(false);
    expect(lockOwner(root)).toBe(mesh);
    // The expected owner with a foreign namespace: the existing reclaimVerdict refuses it and says why.
    const note: StaleLockNote = {};
    const foreign = String(receipt.namespace).replace(/:(\d+)$/, (_m, n: string) => `:${Number(n) + 1}`);
    expect(await lock.clearStaleLock(ownerPath, Date.now() + 2_000, note, { owner: mesh, namespace: foreign })).toBe(false);
    expect(note.verdict).toBeDefined();
    expect(lockOwner(root)).toBe(mesh);
    expect(await lock.clearStaleLock(ownerPath, Date.now() + 2_000, {}, { owner: mesh, namespace: String(receipt.namespace) })).toBe(true);
    expect(lockOwner(root)).toBeUndefined();
  });
});
