import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import * as verifier from "../src/storage/verified-remove.js";
import { actorTreeDeletionHooks, mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { fixture, mainDirectory } from "./helpers/dead-root-fixture.js";
import { RESIDENT_HOST_FORMAT } from "../src/residency/protocol.js";
import { ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import { processHarness } from "./helpers/process-harness.js";

// smarty-dev#8090, LOCAL recorded-call boundary probe (#4664: it must never remove actor-tree bytes). Only the
// actor-tree removal itself is intercepted: the live remove's verifiedRemove of a directory at or
// under an actor root, and every raw fs.rmSync / fs.promises.rm / fs.rmdirSync of one (fallback, nonpersistent
// close), is recorded with the kernel fence state at that instant and NOT performed. The fence (real
// flock), the final check, the hooks' ordering and the Main publication are all real. This proves the local delete
// boundary only (Decision C: supplemental); the actual deletion race is hosted (actor-remove-live-process.test.ts).
type Call = { at: string; fenced: boolean; path?: string };

// Protected: the exact actor-tree directories passed in (an actor's own directory, or a nonpersistent manager's
// whole actor root) and directories under them. Permitted: metadata lock directories (`<file>.lock` and its
// `.lock.dead.<hash>` takeover fence), which the registry and binding stores create and release beside the trees.
const METADATA_LOCK = /\.lock(\.dead\.[0-9a-f]+)?$/;
const guardActorTrees = (roots: string[], fenced: () => boolean, calls: Call[]) => {
  const inTree = (file: unknown) => {
    const resolved = path.resolve(String(file));
    if (METADATA_LOCK.test(path.basename(resolved))) return false;
    return roots.some(root => resolved === path.resolve(root) || resolved.startsWith(path.resolve(root) + path.sep)) &&
      fs.existsSync(resolved) && fs.statSync(resolved).isDirectory();
  };
  const record = (file: unknown) => { calls.push({ at: "rm", path: path.resolve(String(file)), fenced: fenced() }); };
  const rmSync = fs.rmSync.bind(fs), rmdirSync = fs.rmdirSync.bind(fs) as (file: fs.PathLike, options?: fs.RmDirOptions) => void, rm = fs.promises.rm.bind(fs.promises);
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { if (inTree(file)) record(file); else rmSync(file, options); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(((file: fs.PathLike, options?: fs.RmDirOptions) => {
    if (inTree(file)) record(file); else rmdirSync(file, options);
  }) as typeof fs.rmdirSync);
  vi.spyOn(fs.promises, "rm").mockImplementation(async (file, options) => { if (inTree(file)) record(file); else await rm(file, options); });
  // The live remove's boundary: the fence must be held at the call itself (not only at the hooks). Restored by
  // vi.restoreAllMocks; non-targets run the real verifier.
  const verified = verifier.verifiedRemove;
  vi.spyOn(verifier, "verifiedRemove").mockImplementation((target, judged, options) => {
    if (!inTree(target)) return verified(target, judged, options);
    record(target);
    expect(calls.at(-1)?.fenced, `verifiedRemove(${target}) without the held fence`).toBe(true);
    return { removed: true, bytes: 0 };
  });
};
const observeHooks = (fenced: () => boolean, calls: Call[], during?: Partial<Record<"beforeCheck" | "beforeDelete", () => Promise<void>>>) => {
  actorTreeDeletionHooks.beforeCheck = async () => { calls.push({ at: "beforeCheck", fenced: fenced() }); await during?.beforeCheck?.(); };
  actorTreeDeletionHooks.beforeDelete = async () => { calls.push({ at: "beforeDelete", fenced: fenced() }); await during?.beforeDelete?.(); };
};

afterEach(() => {
  delete actorTreeDeletionHooks.beforeCheck;
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe.skipIf(!["linux", "darwin", "win32"].includes(process.platform))("smarty-dev#16016 resident removal presence cleanup", () => {
  it.each([
    { state: "absent after the first failed cleanup", deletePresence: true, completes: true },
    { state: "still present", deletePresence: false, completes: false },
  ])("$state: a lock-timeout refresh cannot strand or bypass the cleanup marker", { timeout: 15_000 }, async ({ deletePresence, completes }) => {
    const f = await fixture();
    const send = async (command: Record<string, unknown>) => {
      const requestId = randomUUID();
      fs.writeFileSync(path.join(f.config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({
        format: RESIDENT_HOST_FORMAT, rootId: f.config.rootId, requestId, createdAt: Date.now(), ...command,
      }));
      const responsePath = path.join(f.config.residencyRoot, "responses", `${requestId}.json`);
      await vi.waitFor(() => expect(fs.existsSync(responsePath)).toBe(true), { timeout: 10_000 });
      return JSON.parse(fs.readFileSync(responsePath, "utf8"));
    };
    let failingPresence: ReturnType<typeof vi.spyOn> | undefined;
    let failingHeartbeat: ReturnType<typeof vi.spyOn> | undefined;
    let suppressingScheduledRefresh: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const created = await send({ operation: "createActor", request: { scope: "project", name: "presence cleanup", instructions: "Work.", residency: "durable" } });
      expect(created.ok).toBe(true);
      const actor = created.actor;
      const presenceKey = `actors/${f.config.sessionId}/${actor.id}`;
      await vi.waitFor(() => expect(f.host.mesh.get(presenceKey, { fresh: true })).toBeDefined());
      const marker = path.join(f.config.actorRoot, `removal-${actor.id}.json`);
      const removalsPath = path.join(f.config.residencyRoot, "removals.json");
      const readRemovals = (): Array<{ id?: string }> => fs.existsSync(removalsPath)
        ? JSON.parse(fs.readFileSync(removalsPath, "utf8")).removals : [];
      // Let create-time participant updates settle, then hold resident refreshes at one lock-timeout
      // so a later heartbeat cannot hide the pending-deletion regression.
      await new Promise<void>(resolve => setTimeout(resolve, 1_100));
      const lockTimeout = async (): Promise<void> => { throw new MeshLockTimeoutError("injected presence refresh contention", 1, 0); };
      failingPresence = vi.spyOn(f.host.participants, "refreshPresence").mockImplementation(lockTimeout);
      failingHeartbeat = vi.spyOn(f.host.participants, "refresh").mockImplementation(lockTimeout);
      suppressingScheduledRefresh = vi.spyOn(f.host.participants, "scheduleRefresh").mockImplementation(() => {});

      const first = await send({ operation: "removeActor", id: actor.id });
      expect(first).toMatchObject({ ok: true, pending: expect.stringContaining("presence deletion pending") });
      expect(fs.existsSync(marker)).toBe(true);
      await vi.waitFor(() => expect(readRemovals().some(item => item.id === actor.id)).toBe(true));
      const existingPresence = f.host.mesh.get(presenceKey, { fresh: true });
      expect(existingPresence).toBeDefined();

      if (deletePresence) {
        await f.host.mesh.delete({ key: presenceKey, ifVersion: existingPresence!.version });
        expect(f.host.mesh.get(presenceKey, { fresh: true })).toBeUndefined();
      }
      const refreshesBeforeRetry = failingPresence.mock.calls.length;
      const retried = await send({ operation: "removeActor", id: actor.id });
      if (completes) {
        expect(retried).toMatchObject({ ok: true });
        expect(retried.pending).toBeUndefined();
        expect(fs.existsSync(marker)).toBe(false);
        await vi.waitFor(() => expect(readRemovals().some(item => item.id === actor.id)).toBe(false));
        expect(failingPresence.mock.calls).toHaveLength(refreshesBeforeRetry);
        expect(f.host.mesh.get(presenceKey, { fresh: true })).toBeUndefined();
      } else {
        expect(retried).toMatchObject({ ok: true, pending: expect.stringContaining("presence deletion pending") });
        expect(fs.existsSync(marker)).toBe(true);
        expect(f.host.mesh.get(presenceKey, { fresh: true })).toBeDefined();
        expect(readRemovals()).toContainEqual(expect.objectContaining({ id: actor.id }));
        expect(failingPresence.mock.calls.length).toBeGreaterThan(refreshesBeforeRetry);
      }
    } finally {
      vi.restoreAllMocks();
      await f.close();
    }
  });
});

describe.skipIf(process.platform !== "linux")("live actor-tree removal boundary, recorded calls (smarty-dev#8090)", () => {
  it("the real CLI's operator removal checks, awaits and removes only while the fence is held; a Main publish inside is refused", async () => {
    const f = await fixture();
    const main = mainDirectory(f.config.meshRoot, f.config.rootId);
    try {
      const actor = await f.create("recorded");
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      // pi-fabric#1031: the allowed operator removal needs explicit proof its Main is gone (stale record, pid+start gone).
      f.mainParticipant(Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS);
      const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
      const calls: Call[] = [];
      let refused: unknown;
      guardActorTrees([dir], fenced, calls);
      observeHooks(fenced, calls, { beforeDelete: async () => { refused = await main.refresh().then(() => "published", error => error); } });
      const removed = await f.cli(actor.id, ["--confirm-dead-root", f.config.rootId]);
      expect(removed).toMatchObject({ code: 0, err: "" });
      expect(calls).toEqual([
        { at: "beforeCheck", fenced: true }, { at: "beforeDelete", fenced: true }, { at: "rm", path: path.resolve(dir), fenced: true }]);
      expect(refused).toMatchObject({ code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n"); // recorded, never performed
      expect(fenced()).toBe(false);
      await expect(main.refresh()).resolves.toBeUndefined();
    } finally { vi.restoreAllMocks(); await main.close(); await f.close(); }
  }, 60_000);

  it.each(["beforeCheck", "beforeDelete"] as const)("an operator removal that joins an ordinary one paused under the fence at %s keeps its provenance: the operator check runs and keeps the tree (direct manager, supplemental)", async window => {
    const f = await fixture();
    const main = mainDirectory(f.config.meshRoot, f.config.rootId);
    try {
      const actor = await f.create("joined");
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      await main.refresh(); // a live Main with a fresh participant: ordinary removal is allowed, operator removal is not
      const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
      const calls: Call[] = [];
      let paused!: () => void;
      const atWindow = new Promise<void>(resolve => { paused = resolve; });
      let resume!: () => void;
      const resumed = new Promise<void>(resolve => { resume = resolve; });
      guardActorTrees([dir], fenced, calls);
      observeHooks(fenced, calls, { [window]: async () => { paused(); await resumed; } });
      const ordinary = f.host.actors.remove(actor.id, { wait: false });
      await atWindow; // the ordinary cleanup holds the fence; at beforeCheck its final check has not run, at beforeDelete it has
      const joined = f.host.actors.remove(actor.id, { wait: false, operator: true });
      resume();
      const [first, second] = await Promise.all([ordinary, joined]);
      expect(calls.filter(call => call.at === "rm")).toEqual([]);
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
      for (const result of [first, second]) expect(result).toMatchObject({ cleaned: false, pending: expect.stringMatching(/kept|live|fresh/i) });
      expect(fenced()).toBe(false);
    } finally { vi.restoreAllMocks(); await main.close(); await f.close(); }
  }, 60_000);

  // Timing regression: the operator caller joins while the ordinary cleanup's lazy verifier import resolves (after
  // the bindings await and any operator check placed before the import). The import must precede the final operator
  // check, so the joined provenance is checked and the tree kept; the recorded verifier is never reached. Seam: the
  // dynamic import's destructure reads the module namespace's verifiedRemove export, so a getter on it (the guard's
  // spy, configurable under Vitest's module runner) observes exactly that resolution. Only an access whose caller is
  // src/actors/manager.ts launches the join (binding and mesh lock releases read the same export from other modules).
  it("an operator removal that joins while the ordinary cleanup's verifier import resolves gets the final operator check and keeps the tree", async () => {
    const f = await fixture();
    const main = mainDirectory(f.config.meshRoot, f.config.rootId);
    let restoreExport = () => {};
    let joined: ReturnType<typeof f.host.actors.remove> | undefined;
    try {
      const actor = await f.create("import-join");
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      await main.refresh(); // a live Main with a fresh participant: ordinary removal is allowed, operator removal is not
      const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
      const calls: Call[] = [];
      guardActorTrees([dir], fenced, calls);
      observeHooks(fenced, calls);
      const recorded = verifier.verifiedRemove; // the guard's recording spy
      const descriptor = Object.getOwnPropertyDescriptor(verifier, "verifiedRemove");
      expect(descriptor?.configurable, "verifiedRemove namespace export is not configurable").toBe(true);
      const managerSource = path.join("src", "actors", "manager.ts");
      let launched = false;
      Object.defineProperty(verifier, "verifiedRemove", { configurable: true, enumerable: descriptor!.enumerable ?? false, get() {
        const caller = (new Error().stack ?? "").split("\n").filter(line => /^\s+at /.test(line))[1] ?? "";
        if (!launched && caller.includes(managerSource)) {
          launched = true; // sticky before remove() runs its synchronous escalation
          calls.push({ at: "join", fenced: fenced() });
          joined = f.host.actors.remove(actor.id, { wait: false, operator: true });
          joined.catch(() => undefined);
        }
        return recorded;
      } });
      restoreExport = () => { Object.defineProperty(verifier, "verifiedRemove", descriptor!); restoreExport = () => {}; };
      const ordinary = f.host.actors.remove(actor.id, { wait: false });
      const first = await ordinary;
      expect(joined, "the verifier import never resolved through the namespace export").toBeDefined();
      const second = await joined!;
      expect(calls).toEqual([
        { at: "beforeCheck", fenced: true }, { at: "beforeDelete", fenced: true }, { at: "join", fenced: true }]);
      expect(vi.mocked(recorded).mock.calls.filter(([target]) => path.resolve(String(target)) === path.resolve(dir))).toEqual([]);
      for (const result of [first, second]) expect(result).toMatchObject({ cleaned: false, pending: expect.stringMatching(/kept|live|fresh/i) });
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(fenced()).toBe(false);
    } finally {
      restoreExport(); // the exact spy descriptor, before any mock restore
      await joined?.catch(() => undefined);
      await main.close(); await f.close();
      vi.restoreAllMocks(); // the guard stays installed through every close and the temp-root teardown
    }
  }, 60_000);

  it("counterexample: an ordinary removal with a live Main still removes the tree, only under the fence", async () => {
    const f = await fixture();
    const main = mainDirectory(f.config.meshRoot, f.config.rootId);
    try {
      const actor = await f.create("ordinary");
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      await main.refresh();
      const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
      const calls: Call[] = [];
      guardActorTrees([dir], fenced, calls);
      observeHooks(fenced, calls);
      const result = await f.host.actors.remove(actor.id);
      expect(result).not.toMatchObject({ cleaned: false });
      expect(calls).toEqual([
        { at: "beforeCheck", fenced: true }, { at: "beforeDelete", fenced: true }, { at: "rm", path: path.resolve(dir), fenced: true }]);
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n"); // recorded, never performed
      expect(fenced()).toBe(false);
    } finally { vi.restoreAllMocks(); await main.close(); await f.close(); }
  }, 60_000);

  // pi-fabric#1031 ruling item 1: off Linux the removal is main's existing unfenced path (the publication helper's
  // non-Linux branch is owned by another writer), so the former win32-refusal assertion is gone; Linux without flock fails closed.
  it("fails closed without a kernel fence: no flock keeps the tree (direct manager, supplemental)", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("no-flock");
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      const calls: Call[] = [];
      guardActorTrees([dir], () => false, calls);
      observeHooks(() => false, calls);
      const empty = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-no-flock-"));
      vi.stubEnv("PATH", empty); // no setpriv/flock: the fence cannot be taken
      const outcome = await f.host.actors.remove(actor.id, { operator: true }).then(result => JSON.stringify(result), error => String(error));
      vi.unstubAllEnvs();
      fs.rmdirSync(empty);
      expect(outcome).toMatch(/flock|fence/i);
      expect(calls).toEqual([]);
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
    } finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); await f.close(); }
  }, 60_000);

  // smarty-dev#8090: an operator upgrade of an accepted ordinary removal is durable
  // before it returns, a failed save refuses it, and a recovered manager without the resident's final check keeps the
  // tree for a marked cleanup. (The legacy unmarked marker stays ordinary: tests/actor-manager.test.ts "preserves the
  // pre-commit presence obligation across a different-session restart".) Direct manager, supplemental; every
  // directory removal under the actor root is recorded, never performed (#4664).
  it("persists an operator upgrade of a pending removal before it returns; a failed save refuses it; a recovered manager keeps the tree", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-recorded-persist-"));
    const actorRoot = path.join(root, "actors");
    const rootId = "session:recorded-persist";
    const identity = { id: rootId, name: "main", kind: "main" as const, sessionId: "recorded-persist" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    const manager = (session: string) => new ActorManager(session, identity, mesh, meshConfig, agents, () => {},
      { actorRoot, rootId, persistent: true, closeGraceMs: 100 });
    const fenced = () => mainPublicationFenced(mesh.root, rootId);
    const calls: Call[] = [];
    guardActorTrees([actorRoot], fenced, calls);
    const row = (id: string) => new ActorRegistryStore(actorRoot).records().find(record => record.id === id) as
      { removal?: { requestedAt?: number; operator?: true } } | undefined;
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const runId = "c".repeat(32);
    vi.spyOn(agents, "run").mockImplementation(async (_request, _signal, onSpawned) => {
      onSpawned?.({ id: runId } as Parameters<NonNullable<typeof onSpawned>>[0]);
      await gate; // a held drain that ignores its abort
      return { id: runId, status: "stopped", text: "", usage: {} } as unknown as Awaited<ReturnType<typeof agents.run>>;
    });
    const first = manager("recorded-persist");
    let restored: ActorManager | undefined;
    try {
      const actor = await first.create({ name: "upgraded", instructions: "Work.", responseMode: "text" });
      const dir = path.join(actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "history.sentinel"), "history\n");
      first.tell(actor.id, "go");
      for (const until = Date.now() + 10_000; first.status(actor.id).inFlightRun?.id !== runId;) {
        if (Date.now() > until) throw new Error("run never started");
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      const accepted = await first.remove(actor.id, { wait: false });
      expect(accepted.pending).toMatch(/pending behind its in-flight run/);
      const requestedAt = row(actor.id)?.removal?.requestedAt;
      expect(requestedAt).toEqual(expect.any(Number));
      expect(row(actor.id)?.removal?.operator).toBeUndefined();

      // Save failure: a registry update fault refuses the escalation and rolls the flag back; nothing is accepted.
      const update = ActorRegistryStore.prototype.update;
      const fault = vi.spyOn(ActorRegistryStore.prototype, "update").mockImplementation(async () => { throw new Error("registry update fault"); });
      await expect(first.remove(actor.id, { wait: false, operator: true })).rejects.toThrow(/operator removal was not saved/);
      fault.mockRestore();
      expect(ActorRegistryStore.prototype.update).toBe(update);
      expect(row(actor.id)?.removal).toMatchObject({ requestedAt });
      expect(row(actor.id)?.removal).not.toHaveProperty("operator");
      expect(first.status(actor.id).removal).toBeDefined(); // still the accepted ordinary removal

      // The upgrade is on disk when the call returns (no flush in between), with the original acceptance time.
      const upgraded = await first.remove(actor.id, { wait: false, operator: true });
      expect(upgraded.pending).toMatch(/pending behind its in-flight run/);
      expect(row(actor.id)?.removal).toMatchObject({ requestedAt, operator: true });

      // The run ends: this manager has no resident final check, so the marked cleanup keeps the tree.
      release();
      const marker = path.join(actorRoot, `removal-${actor.id}.json`);
      const state = () => first.cleanupObligation(actor.id)?.removal?.state ?? "";
      for (const until = Date.now() + 10_000; !(fs.existsSync(marker) && /tree kept/.test(state()));) {
        if (Date.now() > until) throw new Error(`cleanup never refused: ${state()}`);
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      expect(JSON.parse(fs.readFileSync(marker, "utf8"))).toMatchObject({ id: actor.id, operator: true });
      await first.close();

      // Recovered manager, same root, no beforeOperatorCleanup: it handles the marked cleanup and keeps the tree.
      restored = manager("recorded-persist-restored");
      await restored.finishPendingRemovals().catch(() => undefined);
      await restored.removalSettled(actor.id)?.catch(() => undefined);
      expect(JSON.parse(fs.readFileSync(marker, "utf8"))).toMatchObject({ id: actor.id, operator: true });
      expect(calls.filter(call => call.at === "rm")).toEqual([]);
      expect(fs.readFileSync(path.join(dir, "history.sentinel"), "utf8")).toBe("history\n");
      expect(fenced()).toBe(false);
    } finally {
      release();
      await first.close().catch(() => undefined);
      await restored?.close().catch(() => undefined);
      await agents.close();
      // Owned temp root (its parent, not an actor-tree target, so the guard passes it through). The guard stays
      // installed through it, so a late finisher could never perform a real actor-tree delete.
      fs.rmSync(root, { recursive: true, force: true });
      vi.restoreAllMocks();
    }
  }, 60_000);

  // smarty-dev#8090 source audit P2: a REJECTED operator escalation must not leave a durable operator bit in either
  // store. Reachable dual state: an ordinary delayed removal is accepted (row saved), its held run ends, and every
  // registry revocation fails after #commitRemove wrote its unmarked cleanup marker, so the short retries run out with
  // a live accepted row AND an unmarked marker. An operator escalation whose required row save fails is refused; a
  // restarted manager's ordinary retry stays ordinary (no operator callback). Every directory removal under the actor
  // root is recorded, never performed, and the cleanup stops at beforeDelete (#4664).
  it("a rejected operator escalation of a live accepted removal leaves the row and the cleanup marker ordinary across a restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-recorded-rollback-"));
    const actorRoot = path.join(root, "actors");
    const rootId = "session:recorded-rollback";
    const identity = { id: rootId, name: "main", kind: "main" as const, sessionId: "recorded-rollback" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    const operatorCheck = vi.fn();
    const manager = (session: string) => new ActorManager(session, identity, mesh, meshConfig, agents, () => {},
      { actorRoot, rootId, persistent: true, closeGraceMs: 100, removalRetryMs: 5, beforeOperatorCleanup: operatorCheck });
    const fenced = () => mainPublicationFenced(mesh.root, rootId);
    const calls: Call[] = [];
    guardActorTrees([actorRoot], fenced, calls);
    observeHooks(fenced, calls, { beforeDelete: async () => { throw new Error("controlled stop before the tree delete"); } });
    const row = (id: string) => new ActorRegistryStore(actorRoot).records().find(record => record.id === id) as
      { removal?: { requestedAt?: number; operator?: true } } | undefined;
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const runId = "d".repeat(32);
    vi.spyOn(agents, "run").mockImplementation(async (_request, _signal, onSpawned) => {
      onSpawned?.({ id: runId } as Parameters<NonNullable<typeof onSpawned>>[0]);
      await gate;
      return { id: runId, status: "stopped", text: "", usage: {} } as unknown as Awaited<ReturnType<typeof agents.run>>;
    });
    const first = manager("recorded-rollback");
    let restored: ActorManager | undefined;
    try {
      const actor = await first.create({ name: "refused", instructions: "Work.", responseMode: "text" });
      const dir = path.join(actorRoot, actor.id);
      const marker = path.join(actorRoot, `removal-${actor.id}.json`);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "history.sentinel"), "history\n");
      first.tell(actor.id, "go");
      for (const until = Date.now() + 10_000; first.status(actor.id).inFlightRun?.id !== runId;) {
        if (Date.now() > until) throw new Error("run never started");
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      const accepted = await first.remove(actor.id, { wait: false });
      expect(accepted.pending).toMatch(/pending behind its in-flight run/);
      const requestedAt = row(actor.id)?.removal?.requestedAt;
      expect(requestedAt).toEqual(expect.any(Number));

      // Every registry update fails from here: revocation never commits, the short retries run out.
      const fault = vi.spyOn(ActorRegistryStore.prototype, "update").mockImplementation(async () => { throw new Error("registry update fault"); });
      const settled = first.removalSettled(actor.id);
      release();
      await settled;
      expect(first.status(actor.id).removal).toBeDefined(); // the live accepted removal is retained
      expect(row(actor.id)?.removal).toMatchObject({ requestedAt });
      expect(row(actor.id)?.removal).not.toHaveProperty("operator");
      expect(JSON.parse(fs.readFileSync(marker, "utf8"))).toMatchObject({ id: actor.id });
      expect(JSON.parse(fs.readFileSync(marker, "utf8"))).not.toHaveProperty("operator");

      // The operator escalation: a marker write would succeed, the required row save fails, so it is refused.
      await expect(first.remove(actor.id, { wait: false, operator: true })).rejects.toThrow(/operator removal was not saved/);
      expect(row(actor.id)?.removal).toMatchObject({ requestedAt });
      expect(row(actor.id)?.removal).not.toHaveProperty("operator");
      expect(JSON.parse(fs.readFileSync(marker, "utf8"))).not.toHaveProperty("operator");
      fault.mockRestore();
      await first.close();

      // Restart: the ordinary retry stays ordinary (no operator check), and stops at beforeDelete.
      restored = manager("recorded-rollback-restored");
      await restored.remove(actor.id).catch(() => undefined);
      expect(operatorCheck).not.toHaveBeenCalled();
      expect(calls.map(call => call.at)).toEqual(["beforeCheck", "beforeDelete"]);
      expect(JSON.parse(fs.readFileSync(marker, "utf8"))).not.toHaveProperty("operator");
      expect(fs.readFileSync(path.join(dir, "history.sentinel"), "utf8")).toBe("history\n");
      expect(fenced()).toBe(false);
    } finally {
      release();
      await first.close().catch(() => undefined);
      await restored?.close().catch(() => undefined);
      await agents.close();
      fs.rmSync(root, { recursive: true, force: true }); // owned temp root; the guard stays installed through it
      vi.restoreAllMocks();
    }
  }, 60_000);

  // smarty-dev#8090 overlap P2: an operator escalation's row save is PENDING while the held run ends and the finisher
  // starts. The finisher must wait for the escalation's decision before it persists the bit into its cleanup marker or
  // runs the operator check: refused, the removal stays ordinary (no marker bit, no operator callback, also after a
  // restart); accepted, the bit reaches the marker and the operator check runs. Directory removals under the actor root
  // are recorded, never performed, and every cleanup stops at a controlled beforeDelete throw (#4664).
  it.each(["refused", "accepted"] as const)("an operator escalation %s while its row save is paused and the finisher starts decides the marker's provenance", async outcome => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-recorded-overlap-"));
    const actorRoot = path.join(root, "actors");
    const rootId = "session:recorded-overlap";
    const identity = { id: rootId, name: "main", kind: "main" as const, sessionId: "recorded-overlap" };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    const operatorCheck = vi.fn();
    const manager = (session: string) => new ActorManager(session, identity, mesh, meshConfig, agents, () => {},
      { actorRoot, rootId, persistent: true, closeGraceMs: 100, removalRetryMs: 5, beforeOperatorCleanup: operatorCheck });
    const fenced = () => mainPublicationFenced(mesh.root, rootId);
    const calls: Call[] = [];
    guardActorTrees([actorRoot], fenced, calls);
    observeHooks(fenced, calls, { beforeDelete: async () => { throw new Error("controlled stop before the tree delete"); } });
    const row = (id: string) => new ActorRegistryStore(actorRoot).records().find(record => record.id === id) as
      { removal?: { requestedAt?: number; operator?: true } } | undefined;
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const runId = "e".repeat(32);
    vi.spyOn(agents, "run").mockImplementation(async (_request, _signal, onSpawned) => {
      onSpawned?.({ id: runId } as Parameters<NonNullable<typeof onSpawned>>[0]);
      await gate;
      return { id: runId, status: "stopped", text: "", usage: {} } as unknown as Awaited<ReturnType<typeof agents.run>>;
    });
    const first = manager("recorded-overlap");
    let restored: ActorManager | undefined;
    let resumeAll = () => {};
    try {
      const actor = await first.create({ name: "overlap", instructions: "Work.", responseMode: "text" });
      const dir = path.join(actorRoot, actor.id);
      const marker = path.join(actorRoot, `removal-${actor.id}.json`);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "history.sentinel"), "history\n");
      first.tell(actor.id, "go");
      for (const until = Date.now() + 10_000; first.status(actor.id).inFlightRun?.id !== runId;) {
        if (Date.now() > until) throw new Error("run never started");
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      const accepted = await first.remove(actor.id, { wait: false });
      expect(accepted.pending).toMatch(/pending behind its in-flight run/);
      const requestedAt = row(actor.id)?.removal?.requestedAt;
      expect(requestedAt).toEqual(expect.any(Number));

      // The held run ends and the drain's registry save (update 0) pauses; the operator escalation's row save queues
      // behind it. Resumed, the drain ends and the finisher may start while the escalation's own save (update 1) is
      // paused, undecided. Refused, update 1 fails; every other update is real.
      const update = ActorRegistryStore.prototype.update;
      const pause = () => { let open!: () => void, entered!: () => void;
        return { open: new Promise<void>(resolve => { open = resolve; }), entered: new Promise<void>(resolve => { entered = resolve; }),
          resume: () => open(), enter: () => entered() }; };
      const drainSave = pause(), rowSave = pause();
      resumeAll = () => { drainSave.resume(); rowSave.resume(); };
      let index = 0;
      vi.spyOn(ActorRegistryStore.prototype, "update").mockImplementation(async function (this: ActorRegistryStore, ...args) {
        const at = index++;
        if (at === 0) { drainSave.enter(); await drainSave.open; }
        if (at === 1) {
          rowSave.enter(); await rowSave.open;
          if (outcome === "refused") throw new Error("escalation row save fault");
        }
        return update.apply(this, args);
      } as typeof update);
      const settled = first.removalSettled(actor.id);
      release();
      await drainSave.entered;
      const escalation = first.remove(actor.id, { wait: false, operator: true });
      escalation.catch(() => undefined);
      drainSave.resume();
      await rowSave.entered;
      await new Promise(resolve => setTimeout(resolve, 300)); // a finisher that does not wait writes its marker now
      expect(fs.existsSync(marker) ? JSON.parse(fs.readFileSync(marker, "utf8")) : {}).not.toHaveProperty("operator");
      expect(operatorCheck).not.toHaveBeenCalled();
      expect(row(actor.id)?.removal).toMatchObject({ requestedAt }); // the drain's save did not persist the pending bit
      expect(row(actor.id)?.removal).not.toHaveProperty("operator");
      rowSave.resume();
      if (outcome === "refused") await expect(escalation).rejects.toThrow(/operator removal was not saved/);
      else await expect(escalation).resolves.toMatchObject({ removed: true });
      await settled;
      expect(calls.filter(call => call.at === "rm")).toEqual([]);
      expect(calls.length).toBeGreaterThan(0);
      expect(calls.every(call => call.fenced)).toBe(true);
      expect(row(actor.id)).toBeUndefined(); // revoked by the finisher
      if (outcome === "accepted") {
        // The accepted upgrade reached the marker and its operator check ran under the fence.
        expect(JSON.parse(fs.readFileSync(marker, "utf8"))).toMatchObject({ id: actor.id, operator: true });
        expect(operatorCheck).toHaveBeenCalled();
      } else {
        expect(JSON.parse(fs.readFileSync(marker, "utf8"))).not.toHaveProperty("operator");
        expect(operatorCheck).not.toHaveBeenCalled();
        await first.close();
        // Restart: the ordinary retry stays ordinary (no operator callback) and stops at beforeDelete.
        restored = manager("recorded-overlap-restored");
        await restored.remove(actor.id).catch(() => undefined);
        expect(operatorCheck).not.toHaveBeenCalled();
        expect(JSON.parse(fs.readFileSync(marker, "utf8"))).not.toHaveProperty("operator");
      }
      expect(calls.filter(call => call.at === "rm")).toEqual([]);
      expect(fs.readFileSync(path.join(dir, "history.sentinel"), "utf8")).toBe("history\n");
      expect(fenced()).toBe(false);
    } finally {
      release(); resumeAll();
      await first.close().catch(() => undefined);
      await restored?.close().catch(() => undefined);
      await agents.close();
      fs.rmSync(root, { recursive: true, force: true }); // owned temp root; the guard stays installed through it
      vi.restoreAllMocks();
    }
  }, 60_000);

  // Decision C RECORDED boundary, native processes, NOT E2E: a real resident child (ResidentHost from config.json), a
  // separate real Main process (ParticipantDirectory) and the real `fabric-actors` CLI in its own process over the
  // resident request channel, with a real flock. The resident runs with recordTreeDeletes (argv): its whole actor-tree
  // removal (verifiedRemove, or the raw fs.rmSync fallback) is recorded with the fence state and not performed;
  // everything else is real. This process guards its own actor root the same way (the seed and the persistent host
  // close never delete trees, and guards stay through
  // teardown). The real deletion is the hosted actor-remove-live-process.test.ts (b).
  it("native processes, recorded delete: a Main publish while the live operator removal awaits beforeDelete is refused; resumed, it succeeds fenced and the Main publishes after", async () => {
    const f = await fixture();
    const h = processHarness();
    const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
    guardActorTrees([f.config.actorRoot], fenced, []);
    try {
      const actor = await f.create("recorded-process");
      expect(actor.id).toMatch(/^[a-f0-9]{32}$/);
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      await f.killResident(); // persistent host close: trees kept; stale owner and a stale Main record are left
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      const resident = await h.resident(f.config.residencyRoot, "beforeDelete", { recordTreeDeletes: true });
      await resident.arm();
      const cli = h.remover("none", ["remove", "--resident", f.config.residencyRoot, "--actor", actor.id,
        "--mesh-root", f.config.meshRoot, "--confirm-dead-root", f.config.rootId]);
      expect(await resident.next("paused", 60_000)).toMatchObject({ at: "beforeDelete" });
      expect(fenced()).toBe(true);
      expect(await main.publish()).toMatchObject({ ok: false, code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      resident.send("continue");
      const exit = await cli.next("exit", 60_000);
      expect(exit).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(exit.out as string).cleaned).not.toBe(false);
      const calls = await resident.report();
      expect(calls).toContainEqual({ kind: "remove", id: actor.id, options: expect.objectContaining({ operator: true }) });
      expect(calls.filter(call => call.kind === "hook")).toEqual([
        { kind: "hook", name: "beforeCheck", fenced: true }, { kind: "hook", name: "beforeDelete", fenced: true }]);
      expect(calls.filter(call => call.kind === "rm" && call.path === path.resolve(dir))).toEqual([
        { kind: "rm", path: path.resolve(dir), fenced: true }]);
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n"); // recorded, never performed
      expect(fenced()).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
      await resident.stop();
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
    } finally { await h.close(); await f.close(); vi.restoreAllMocks(); } // guard kept through every close and the temp-root teardown
  }, 120_000);

  it("a nonpersistent manager's close removes its actor root only through the fenced helper", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-recorded-close-"));
    const actorRoot = path.join(root, "actors");
    const rootId = "session:recorded-close";
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 1, transport: "process", sessionExport: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), sweepPath: path.resolve("dist/storage/sweep-main.js") });
    const actors = new ActorManager("recorded-close", { id: rootId, name: "main", kind: "main", sessionId: "recorded-close" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: false }, agents, () => {}, { actorRoot, rootId });
    try {
      fs.mkdirSync(path.join(actorRoot, "a1"), { recursive: true }); fs.writeFileSync(path.join(actorRoot, "a1", "session.jsonl"), "history\n");
      const fenced = () => mainPublicationFenced(mesh.root, rootId);
      const calls: Call[] = [];
      guardActorTrees([actorRoot], fenced, calls);
      observeHooks(fenced, calls);
      await actors.close();
      const rm = calls.filter(call => call.at === "rm");
      expect(rm).toEqual([{ at: "rm", path: path.resolve(actorRoot), fenced: true }]);
      expect(calls.findIndex(call => call.at === "beforeDelete")).toBeLessThan(calls.indexOf(rm[0]!));
      expect(fs.readFileSync(path.join(actorRoot, "a1", "session.jsonl"), "utf8")).toBe("history\n");
      expect(fenced()).toBe(false);
    } finally {
      vi.restoreAllMocks();
      await agents.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);
});
