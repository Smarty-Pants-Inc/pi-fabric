import childProcess from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorManager } from "../src/actors/manager.js";
import * as offlineRemoval from "../src/actors/remove-offline.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import * as verifiedRemoval from "../src/storage/verified-remove.js";
import { fleetSessionArchive } from "../src/storage/session-archive.js";
import { processStartTime } from "../src/residency/process-identity.js";
import {
  readResidentRequestDecision, residentRemovalsPath, residentRoot,
  type ResidentCommand, type ResidentCommandResponse, type ResidentHostConfig,
} from "../src/residency/protocol.js";
import { hostLeasePath, writeHostLease } from "../src/topology/host-leases.js";
import { actorTreeDeletionHooks, mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// #4664: never execute a product deletion, INCLUDING metadata/locks. All nine fs deletion APIs
// record their calls; metadata disappearance is emulated by rename into this fixture's own trash.
// An actor tree delete is recorded only, leaving every byte and inode in place. The sole real
// recursive deletion below is teardown of our own mkdtemp fixture, after ResidentHost.close().
// No dead-root-fixture, CLI, worker or native deletion helper is run. Linux flock/setpriv helpers
// remain real. Darwin/Windows override ONLY the synchronous verdict callback, after Linux-native
// Main/state/participant locks are acquired, and restore before any subsequent await or I/O path.
// This proves the host's platform verdict, NOT native Darwin/Windows fence/locking behavior.
const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
const realFixtureTeardown = fs.rmSync.bind(fs);
const GUIDANCE = "stop the Main, or let a host start finish it";
const stale = () => Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS;
const gone = { pid: 2147483647, host: os.hostname(), startTime: "1" };
type Call = { api: string; path: string; tree: boolean };
const TREE_API = process.platform === "linux" ? "verifiedRemove" : "rmSync";
const realVerifiedRemove = verifiedRemoval.verifiedRemove;

const restorePlatform = () => Object.defineProperty(process, "platform", nativePlatform);
afterEach(() => {
  restorePlatform();
  delete actorTreeDeletionHooks.beforeCheck;
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
  syncBuiltinESMExports();
});

const treeSnapshot = (directory: string): Array<{ name: string; dev: number; ino: number; bytes: string | null }> => {
  const entries: ReturnType<typeof treeSnapshot> = [];
  const visit = (file: string) => {
    const stat = fs.lstatSync(file);
    entries.push({ name: path.relative(directory, file), dev: stat.dev, ino: stat.ino,
      bytes: stat.isDirectory() ? null : fs.readFileSync(file).toString("hex") });
    if (stat.isDirectory()) {
      for (const name of fs.readdirSync(file).sort()) visit(path.join(file, name));
    }
  };
  visit(directory);
  return entries;
};

const guardDeletions = (root: string, actorRoots: string[], calls: Call[], fenced: () => boolean, violations: string[]) => {
  const trash = path.join(root, "fixture-trash");
  fs.mkdirSync(trash, { mode: 0o700 });
  let sequence = 0;
  const emulate = (api: string, input: fs.PathLike, options?: fs.RmOptions) => {
    const file = path.resolve(input instanceof URL ? fileURLToPath(input) : String(input));
    // A dirfd-relative deletion still has to resolve into this fixture; nothing external is allowed.
    const resolved = file.startsWith("/proc/") ? path.join(fs.realpathSync(path.dirname(file)), path.basename(file)) : file;
    const inActorTree = actorRoots.some(actorRoot => resolved === actorRoot ||
      (resolved.startsWith(actorRoot + path.sep) && /^[a-f0-9]{32}(?:[\\/]|$)/.test(path.relative(actorRoot, resolved))));
    const metadataLock = /\.lock(?:[\\/]|$)/.test(resolved);
    const protectedData = inActorTree && !metadataLock;
    // `tree` identifies a whole-directory delete, not an empty-queue metadata unlink.
    const tree = protectedData && fs.existsSync(resolved) && fs.lstatSync(resolved).isDirectory();
    calls.push({ api, path: resolved, tree });
    if (!resolved.startsWith(root + path.sep)) throw new Error(`Unsafe product deletion outside fixture: ${api} ${resolved}`);
    if (protectedData) return; // recorded only, including nested data; locks still get rename emulation
    try { fs.renameSync(resolved, path.join(trash, String(sequence++))); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" || !options?.force) throw error;
    }
  };
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => emulate("rmSync", file, options));
  vi.spyOn(fs, "rmdirSync").mockImplementation((file) => emulate("rmdirSync", file));
  vi.spyOn(fs, "unlinkSync").mockImplementation((file) => emulate("unlinkSync", file));
  vi.spyOn(fs, "rm").mockImplementation(((file: fs.PathLike, optionsOrCallback: fs.RmOptions | fs.NoParamCallback,
    callback?: fs.NoParamCallback) => {
    const options = typeof optionsOrCallback === "function" ? undefined : optionsOrCallback;
    const done = typeof optionsOrCallback === "function" ? optionsOrCallback : callback!;
    try { emulate("rm", file, options); done(null); } catch (error) { done(error as NodeJS.ErrnoException); }
  }) as typeof fs.rm);
  vi.spyOn(fs, "rmdir").mockImplementation(((file: fs.PathLike, optionsOrCallback: fs.RmDirOptions | fs.NoParamCallback,
    callback?: fs.NoParamCallback) => {
    const done = typeof optionsOrCallback === "function" ? optionsOrCallback : callback!;
    try { emulate("rmdir", file); done(null); } catch (error) { done(error as NodeJS.ErrnoException); }
  }) as typeof fs.rmdir);
  vi.spyOn(fs, "unlink").mockImplementation((file, callback) => {
    try { emulate("unlink", file); callback(null); } catch (error) { callback(error as NodeJS.ErrnoException); }
  });
  vi.spyOn(fs.promises, "rm").mockImplementation(async (file, options) => emulate("promises.rm", file, options));
  vi.spyOn(fs.promises, "rmdir").mockImplementation(async (file) => emulate("promises.rmdir", file));
  vi.spyOn(fs.promises, "unlink").mockImplementation(async (file) => emulate("promises.unlink", file));

  // Keep the real verifier's judgment/walk, but use its production dry-run option before any mutation.
  vi.spyOn(verifiedRemoval, "verifiedRemove").mockImplementation((target, judged, options) => {
    const file = path.resolve(target);
    const inActorTree = actorRoots.some(actorRoot => path.dirname(file) === actorRoot && /^[a-f0-9]{32}$/.test(path.basename(file)));
    if (process.platform !== "linux" || !inActorTree) return realVerifiedRemove(target, judged, options);
    const now = fs.lstatSync(file, { throwIfNoEntry: false });
    calls.push({ api: "verifiedRemove", path: file, tree: now?.isDirectory() === true });
    if (!fenced()) violations.push(`${file}: not under the Main publication fence`);
    if (!judged || !now || now.dev !== judged.dev || now.ino !== judged.ino) violations.push(`${file}: judged inode is not the current tree`);
    const result = realVerifiedRemove(target, judged, { ...options, dryRun: true });
    if (!result.removed) violations.push(`${file}: refused (${result.reason})`);
    return result;
  });

  // A native delete bypasses fs spies. Record and refuse it (also any unexpected CLI/worker),
  // allowing only the existing Linux lock helpers used by this cleanup route.
  const spawn = childProcess.spawn, spawnSync = childProcess.spawnSync;
  const allowedLock = (file: string, args: readonly string[] | undefined) =>
    (file === "setpriv" && args?.[0] === "--pdeathsig" && args[1] === "KILL" && args[2] === "flock") ||
    (file === "flock" && args?.[0] === "-s" && args[1] === "-n" && args[2]?.startsWith(root + path.sep) && args[3] === "true");
  const refusedChild = (api: string, file: unknown): never => {
    calls.push({ api: /fabric-win-remove|\/proc\/self\/fd/.test(String(file)) ? "native-helper-delete" : api,
      path: String(file), tree: false });
    throw new Error(`Unexpected product child (recorded, not executed): ${api} ${String(file)}`);
  };
  vi.spyOn(childProcess, "spawn").mockImplementation(((...args: Parameters<typeof spawn>) => {
    if (!allowedLock(args[0], args[1])) return refusedChild("spawn", args[0]);
    return spawn(...args);
  }) as typeof spawn);
  vi.spyOn(childProcess, "spawnSync").mockImplementation(((...args: Parameters<typeof spawnSync>) => {
    if (!allowedLock(args[0], args[1])) return refusedChild("spawnSync", args[0]);
    return spawnSync(...args);
  }) as typeof spawnSync);
  for (const api of ["exec", "execFile", "execSync", "execFileSync"] as const) {
    vi.spyOn(childProcess, api).mockImplementation((...args: unknown[]) => refusedChild(api, args[0]));
  }
  syncBuiltinESMExports();
};

const safeFixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-platform-verdict-"));
  const meshRoot = path.join(root, "mesh"), rootId = "session:platform-verdict";
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "platform-verdict", cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(root, "actors", "project"), sessionActorRoot: path.join(root, "actors", "session"),
    residencyRoot: residentRoot(meshRoot, rootId), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, stateBackend: "file", actorScope: "project", actorPollMs: 60_000, residentIdleExitMs: 60_000 },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention },
    workerPath: path.join(root, "unused-worker.mjs"), fabricExtensionPath: path.join(root, "unused-extension.js"),
    piBinary: "unused-pi", claudeBinary: "unused-claude", vedaBinary: "unused-veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const calls: Call[] = [], violations: string[] = [];
  guardDeletions(root, [config.actorRoot, config.sessionActorRoot!], calls,
    () => mainPublicationFenced(meshRoot, rootId), violations);
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const host = new ResidentHost(config, () => {});
  try {
    await host.start();
    // No actor activation/completion polling is needed for removal; prevent unrelated metadata
    // creation from racing the full-tree snapshot. The real host request dispatcher stays live.
    host.actors.pauseForRelease();
  }
  catch (error) {
    await host.close();
    realFixtureTeardown(root, { recursive: true, force: true });
    throw error;
  }
  const close = async () => {
    restorePlatform();
    // All deletion guards stay installed until every host/actor/participant finisher settles.
    try { await host.close(); }
    finally { realFixtureTeardown(root, { recursive: true, force: true }); }
  };
  return { root, config, host, calls, violations, close };
};
type Fixture = Awaited<ReturnType<typeof safeFixture>>;

// Direct request-file submission exercises the actual ResidentHost dispatcher and accepted removal
// decision/removals reporting without spawning the CLI (which is unrelated to this callback fix).
const removeRequest = async (f: Fixture, id: string, operator: boolean): Promise<{ response: ResidentCommandResponse; requestId: string }> => {
  const requestId = randomUUID();
  const command: ResidentCommand = operator
    ? { format: 2, rootId: f.config.rootId, requestId, operation: "operatorActor", action: "remove", id,
      confirmDeadRoot: f.config.rootId, createdAt: Date.now() }
    : { format: 1, rootId: f.config.rootId, requestId, operation: "removeActor", id, createdAt: Date.now() };
  fs.writeFileSync(path.join(f.config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify(command));
  const file = path.join(f.config.residencyRoot, "responses", `${requestId}.json`);
  const deadline = Date.now() + 10_000;
  while (!fs.existsSync(file)) {
    if (Date.now() >= deadline) throw new Error("ResidentHost request did not produce a response");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  return { response: JSON.parse(fs.readFileSync(file, "utf8")) as ResidentCommandResponse, requestId };
};

const seed = (f: Fixture, lease: "absent" | "expired", participant: "missing" | "stale", mainProcess = gone) => {
  expect(fs.existsSync(hostLeasePath(f.config.meshRoot, f.config.rootId))).toBe(false);
  if (lease === "expired") {
    writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
      startedAt: stale(), updatedAt: stale(), expiresAt: stale() });
  }
  if (participant === "stale") {
    writeParticipantFile(f.config.meshRoot, {
      key: "topology/participants/" + createHash("sha256").update(f.config.rootId).digest("hex"),
      value: { id: f.config.rootId, rootId: f.config.rootId, kind: "root", ownerHostId: f.config.rootId,
        ownerIdentityId: f.config.rootId, status: "idle", mainProcess },
      version: 1, updatedAt: stale(), updatedBy: { id: f.config.rootId, name: "main", kind: "main" },
    });
  }
};

const probe = async (f: Fixture, platform: "darwin" | "win32" | "linux", operator: boolean,
  lease: "absent" | "expired", participant: "missing" | "stale") => {
  expect(f.host.actors).toBeInstanceOf(ActorDirectory);
  expect(f.host.actors).toBeInstanceOf(ActorManager);
  const actor = await f.host.actors.create({ name: "kept-history", instructions: "Work", model: "fixture/visible",
    residency: "durable", transport: "process", extensions: false });
  // Finish ordinary stop metadata before pinning the tree; the removal request still runs its
  // acceptance check, sticky operator escalation, registry revocation and final host callback.
  await f.host.actors.stop(actor.id, undefined, true);
  const tree = path.join(f.config.actorRoot, actor.id);
  fs.mkdirSync(path.join(tree, "history"), { recursive: true });
  fs.writeFileSync(path.join(tree, "session.jsonl"), "history\n");
  fs.writeFileSync(path.join(tree, "history", "run.bin"), Buffer.from([0, 255, 13, 10, 42]));
  const snapshot = treeSnapshot(tree);
  const mainProcess = platform === "linux" ? gone
    : { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid)! };
  expect(mainProcess.startTime).toBeDefined();
  seed(f, lease, participant, mainProcess);
  const hooks: Array<{ at: string; fenced: boolean }> = [], verdicts: string[] = [];
  const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
  actorTreeDeletionHooks.beforeCheck = () => { hooks.push({ at: "beforeCheck", fenced: fenced() }); };
  actorTreeDeletionHooks.beforeDelete = () => { hooks.push({ at: "beforeDelete", fenced: fenced() }); };
  const actualCheck = offlineRemoval.fencedOperatorCheck;
  vi.spyOn(offlineRemoval, "fencedOperatorCheck").mockImplementation((mesh, identity, rootId, verdict) =>
    actualCheck(mesh, identity, rootId, get => {
      // The REAL host-supplied callback, inside the REAL state transaction and participant lock.
      expect(fenced()).toBe(true);
      Object.defineProperty(process, "platform", { ...nativePlatform, value: platform });
      verdicts.push(process.platform);
      try { verdict(get); } finally { restorePlatform(); }
    }));
  const result = await removeRequest(f, actor.id, operator);
  // A pre-stopped, never-activated actor has no behind-run retry/backoff to join. Its immediate
  // response is the settled cleanup refusal, and the real saved obligation proves pending custody.
  expect(f.host.actors.removalSettled(actor.id)).toBeUndefined();
  expect(process.platform).toBe(nativePlatform.value);
  // smarty-dev#16444 (R-keep-all-traces): a removal that reaches the (recorded) tree delete first moved the session trace
  // into the fleet archive by rename (same inode, same bytes); a refused removal keeps every entry in place.
  const reached = f.calls.some(call => call.tree);
  const isTrace = (entry: { name: string }) => /^session\.jsonl/.test(entry.name);
  expect(treeSnapshot(tree)).toEqual(reached ? snapshot.filter(entry => !isTrace(entry)) : snapshot); // every byte and inode (including root/nested dirs)
  if (reached) {
    const archived = treeSnapshot(fleetSessionArchive(f.config.meshRoot, f.config.rootId, actor.id))
      .filter(entry => entry.name.endsWith(".jsonl") && entry.name !== "MANIFEST.jsonl");
    expect(archived.map(({ dev, ino, bytes }) => ({ dev, ino, bytes })))
      .toEqual(snapshot.filter(isTrace).map(({ dev, ino, bytes }) => ({ dev, ino, bytes })));
  }
  expect(fenced()).toBe(false);
  expect(readResidentRequestDecision(f.config.residencyRoot, result.requestId)).toMatchObject({
    state: "committed", id: actor.id, operation: operator ? "operatorActor" : "removeActor",
  });
  const registry = new ActorRegistryStore(f.config.actorRoot);
  expect(registry.records().some(row => row.id === actor.id)).toBe(false); // accepted/revoked, not an admission failure
  expect(f.calls.some(call => call.api === "native-helper-delete")).toBe(false);
  expect(f.violations).toEqual([]);
  expect(f.calls.some(call => !call.tree && call.api === "rmSync")).toBe(true); // metadata emulation actually ran
  return { ...result, actor, hooks, verdicts, treeDeletes: f.calls.filter(call => call.tree) };
};

const cases = (["darwin", "win32"] as const).flatMap(platform =>
  (["absent", "expired"] as const).flatMap(lease =>
    (["missing", "stale"] as const).map(participant => ({ platform, lease, participant }))));

describe.skipIf(process.platform !== "linux")("resident operator cleanup uses the Main-identity verdict on every platform (#1031)", () => {
  it.each(cases)("$platform: $lease root lease, $participant root participant refuses after acceptance; history/inodes kept",
    async ({ platform, lease, participant }) => {
      const f = await safeFixture();
      try {
        const result = await probe(f, platform, true, lease, participant);
        expect(result.verdicts).toEqual([platform]); // setup/absent hook cannot be the RED
        expect({ response: result.response, hooks: result.hooks, treeDeletes: result.treeDeletes }).toMatchObject({
          response: { ok: true, cleaned: false, pending: expect.stringContaining(GUIDANCE) },
          hooks: [{ at: "beforeCheck", fenced: true }], treeDeletes: [],
        }); // pristine baseline instead reaches beforeDelete + recorded rmSync(tree), reports cleaned
        expect(result.response.pending).toMatch(participant === "missing" ? /no root participant record/ : /non-Linux/);
        const marker = JSON.parse(fs.readFileSync(path.join(f.config.actorRoot, `removal-${result.actor.id}.json`), "utf8"));
        expect(marker).toMatchObject({ id: result.actor.id, operator: true, owner: { rootId: f.config.rootId, residency: "durable" } });
        expect(f.host.actors.cleanupObligation(result.actor.id)).toMatchObject({ id: result.actor.id, status: "stopped",
          removal: { state: expect.stringContaining(GUIDANCE) } });
        expect(f.host.actors.pendingRemovals()).toContainEqual(expect.objectContaining({ id: result.actor.id, state: expect.stringContaining(GUIDANCE) }));
        const saved = JSON.parse(fs.readFileSync(residentRemovalsPath(f.config.residencyRoot), "utf8"));
        expect(saved.removals).toContainEqual(expect.objectContaining({ id: result.actor.id, state: expect.stringContaining(GUIDANCE) }));
      } finally { await f.close(); }
    }, 30_000);

  it("Linux identity control: stale recorded Main with a provably gone pid/start and expired lease reaches recorded tree delete", async () => {
    const f = await safeFixture();
    try {
      const result = await probe(f, "linux", true, "expired", "stale");
      expect(result.verdicts).toEqual(["linux"]);
      expect(result.response).toMatchObject({ ok: true });
      expect(result.response.pending).toBeUndefined();
      expect(result.hooks).toEqual([{ at: "beforeCheck", fenced: true }, { at: "beforeDelete", fenced: true }]);
      expect(result.treeDeletes).toEqual([{ api: TREE_API, path: path.join(f.config.actorRoot, result.actor.id), tree: true }]);
      expect(f.host.actors.cleanupObligation(result.actor.id)).toBeUndefined();
    } finally { await f.close(); }
  }, 30_000);

  it("ordinary non-operator removal counterexample: no Main identity check, reaches recorded delete with no root evidence", async () => {
    const f = await safeFixture();
    try {
      const result = await probe(f, "darwin", false, "absent", "missing");
      expect(result.verdicts).toEqual([]); // no platform override: ordinary route stays Linux-native
      expect(result.response).toMatchObject({ ok: true });
      expect(result.response.pending).toBeUndefined();
      expect(result.hooks).toEqual([{ at: "beforeCheck", fenced: true }, { at: "beforeDelete", fenced: true }]);
      expect(result.treeDeletes).toEqual([{ api: TREE_API, path: path.join(f.config.actorRoot, result.actor.id), tree: true }]);
      expect(f.host.actors.cleanupObligation(result.actor.id)).toBeUndefined();
    } finally { await f.close(); }
  }, 30_000);
});
