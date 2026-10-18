import { execFile } from "node:child_process";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as offlineRemoval from "../src/actors/remove-offline.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import { actorTreeDeletionHooks, mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { fixture, mainDirectory, type DeadRootFixture } from "./helpers/dead-root-fixture.js";

// Issue #10189 (RED first): a LEGACY session actor cleanup marker `removal-<32hex>.json` whose registry row is already
// revoked, owner.rootId = this resident's exact root, owner.residency = "session", and no operator flag. The resident
// host claims durable cleanup only, and its operator handler (src/residency/host.ts operatorActor) selects durable
// registry rows or in-flight commits, never a cleanup marker, so `fabric-actors remove` cannot finish it.
//
// Acceptance runs the REAL `bin/fabric-actors` (compiled dist/actors-cli.js: the parent builds first) in its own node
// process over the actual resident request channel to the in-process ResidentHost of the fixture. Not src main(), not
// manager.remove. Decision C: native operator-bin integration with a RECORDED tree delete (supplemental); the real
// deletion is the hosted-only test at the end. Not an installed Pi product E2E.
//
// LOCAL tests (names start "recorded:"): every fs.rmSync / fs.promises.rm / fs.rmdirSync of a directory at or under
// the session actor root is recorded with the kernel fence state at that instant and NOT performed (#4664, same guard
// as tests/actor-remove-live-fence.test.ts). The fence, the hooks, the operator check and the marker are real.
// Safe local selection: bunx vitest run tests/actor-legacy-session-cleanup.test.ts -t "recorded:"
type Call = { at: string; fenced: boolean; path?: string };

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
};
const observe = (fenced: () => boolean, calls: Call[]) => {
  actorTreeDeletionHooks.beforeCheck = () => { calls.push({ at: "beforeCheck", fenced: fenced() }); };
  actorTreeDeletionHooks.beforeDelete = () => { calls.push({ at: "beforeDelete", fenced: fenced() }); };
  // The REAL host-supplied operator verdict inside the REAL state transaction and participant lock.
  const actual = offlineRemoval.fencedOperatorCheck;
  vi.spyOn(offlineRemoval, "fencedOperatorCheck").mockImplementation((mesh, identity, rootId, verdict) =>
    actual(mesh, identity, rootId, get => { calls.push({ at: "operatorCheck", fenced: fenced() }); verdict(get); }));
};

afterEach(() => {
  delete actorTreeDeletionHooks.beforeCheck;
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
});

const run = promisify(execFile);
/** The real operator bin, in its own process; the in-process resident answers over its request channel. */
const bin = async (f: DeadRootFixture, action: "remove" | "stop", actor: string, ...flags: string[]) => {
  const cli = path.resolve("bin/fabric-actors");
  if (!fs.existsSync(path.resolve("dist/actors-cli.js"))) throw new Error("dist/actors-cli.js missing: run `bun run build` first");
  try {
    const { stdout, stderr } = await run(process.execPath, [cli, action, "--resident", f.config.residencyRoot, "--actor", actor,
      "--mesh-root", f.config.meshRoot, ...flags], { timeout: 60_000 });
    return { code: 0, out: stdout, err: stderr };
  } catch (error) {
    const failed = error as { code?: unknown; stdout?: string; stderr?: string };
    if (typeof failed.code !== "number") throw error;
    return { code: failed.code, out: failed.stdout ?? "", err: failed.stderr ?? "" };
  }
};

const ID = "a1b2c3d4e5f60718293a4b5c6d7e8f90"; // a 32-hex actor id never present in any registry
const NAME = "legacy-worker";

/** The legacy shape: an empty session registry (row revoked), the actor tree with history and an unflagged
 * session-owned marker for this exact root. The resident is LIVE; its Main is stale with an independently dead pid. */
const seedLegacy = (f: DeadRootFixture, options: { rootId?: string } = {}) => {
  const sessionRoot = path.join(f.config.actorRoot, f.config.sessionId);
  fs.mkdirSync(sessionRoot, { recursive: true, mode: 0o700 });
  new ActorRegistryStore(sessionRoot).write([]);
  const tree = path.join(sessionRoot, ID);
  fs.mkdirSync(path.join(tree, "history"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(tree, "session.jsonl"), "history\n");
  fs.writeFileSync(path.join(tree, "history", "run.jsonl"), "run\n");
  const marker = path.join(sessionRoot, `removal-${ID}.json`);
  fs.writeFileSync(marker, JSON.stringify({ id: ID, sessionDir: tree, presenceKey: `actors/${f.config.sessionId}/${ID}`,
    owner: { name: NAME, rootId: options.rootId ?? f.config.rootId, residency: "session", requestedAt: Date.now() - 60_000 } }));
  f.mainParticipant(Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS); // default record: pid 2147483647 never exists
  const registered = () => new ActorRegistryStore(sessionRoot).records().some(record => record.id === ID);
  const history = () => fs.readFileSync(path.join(tree, "session.jsonl"), "utf8");
  return { sessionRoot, tree, marker, registered, history };
};

// Linux-only: the Main publication fence (real flock) is the availability requirement.
describe.skipIf(process.platform !== "linux")("legacy session actor cleanup marker via the real fabric-actors bin (#10189)", () => {
  it("recorded: an exact-id operator remove finishes the legacy session marker under the fence; the tree delete is recorded, not performed", async () => {
    const f = await fixture();
    const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
    const calls: Call[] = [];
    try {
      const s = seedLegacy(f);
      guardActorTrees([s.sessionRoot], fenced, calls);
      observe(fenced, calls);
      // Before the request: a revoked row, an unflagged marker, nothing attempted.
      expect(s.registered()).toBe(false);
      const before = JSON.parse(fs.readFileSync(s.marker, "utf8"));
      expect(before).toMatchObject({ id: ID, owner: { rootId: f.config.rootId, residency: "session" } });
      expect(before).not.toHaveProperty("operator");
      expect(calls).toEqual([]);

      // Resident LIVE: no --main-stopped (that flag is offline-only).
      const removed = await bin(f, "remove", ID, "--confirm-dead-root", f.config.rootId);
      expect(removed).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(ID)?.catch(() => undefined);
      expect(fs.existsSync(s.marker)).toBe(false);

      expect(f.host.actors.cleanupObligation(ID)).toBeUndefined();
      expect(f.host.actors.pendingRemovals().map(entry => entry.id)).not.toContain(ID);
      expect(s.registered()).toBe(false); // stays revoked, never resurrected
      // The operator check ran under the held fence, between the hooks; the tree delete only after it.
      expect(calls).toEqual([
        { at: "beforeCheck", fenced: true }, { at: "operatorCheck", fenced: true }, { at: "beforeDelete", fenced: true },
        { at: "rm", path: path.resolve(s.tree), fenced: true }]);
      expect(s.history()).toBe("history\n"); // recorded, never performed
      expect(fenced()).toBe(false);
    } finally { await f.close(); vi.restoreAllMocks(); } // guard kept through teardown
  }, 120_000);

  it("recorded: counterexample, a live Main keeps the history and the marker", async () => {
    const f = await fixture();
    const main = mainDirectory(f.config.meshRoot, f.config.rootId);
    const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
    const calls: Call[] = [];
    try {
      const s = seedLegacy(f);
      await main.refresh(); // a fresh root participant whose Main (this process) is alive
      guardActorTrees([s.sessionRoot], fenced, calls);
      observe(fenced, calls);
      await bin(f, "remove", ID, "--confirm-dead-root", f.config.rootId);
      await f.host.actors.removalSettled(ID)?.catch(() => undefined);
      expect(calls.filter(call => call.at === "rm")).toEqual([]);
      expect(JSON.parse(fs.readFileSync(s.marker, "utf8"))).toMatchObject({ id: ID, owner: { rootId: f.config.rootId, residency: "session" } });
      expect(s.history()).toBe("history\n");
      expect(s.registered()).toBe(false);
      expect(fenced()).toBe(false);
    } finally { await main.close(); await f.close(); vi.restoreAllMocks(); }
  }, 120_000);

  it("recorded: counterexample, a Main published after operator acceptance vetoes the final check; the saved operator retry completes", async () => {
    const f = await fixture();
    const main = mainDirectory(f.config.meshRoot, f.config.rootId);
    const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
    const calls: Call[] = [];
    let mainOpen = true;
    try {
      const s = seedLegacy(f); // Main independently dead at acceptance
      guardActorTrees([s.sessionRoot], fenced, calls);
      observe(fenced, calls);
      // The host has committed the operator request; the fence is not yet taken. A real Main publishes now.
      const originalRemove = f.host.actors.remove.bind(f.host.actors);
      let publishOnce = true;
      vi.spyOn(f.host.actors, "remove").mockImplementation(async (id, options) => {
        if (id === ID && options?.operator && publishOnce) { publishOnce = false; await main.refresh(); }
        return originalRemove(id, options);
      });
      const first = await bin(f, "remove", ID, "--confirm-dead-root", f.config.rootId);
      expect(first).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(first.out)).toMatchObject({ cleaned: false, pending: expect.any(String) });
      await f.host.actors.removalSettled(ID)?.catch(() => undefined);
      expect(publishOnce).toBe(false);
      expect(JSON.parse(fs.readFileSync(s.marker, "utf8"))).toMatchObject({ id: ID, operator: true });
      expect(f.host.actors.cleanupObligation(ID)).toBeDefined();
      expect(calls.some(call => call.at === "operatorCheck")).toBe(true);
      expect(calls.filter(call => call.at === "operatorCheck").every(call => call.fenced)).toBe(true);
      expect(calls.filter(call => call.at === "beforeDelete" || call.at === "rm")).toEqual([]);
      expect(s.history()).toBe("history\n");
      expect(fenced()).toBe(false);

      // Main closes; its stale, independently gone pid record returns. The exact-id operator retry completes.
      await main.close(); mainOpen = false;
      f.mainParticipant(Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS);
      calls.length = 0;
      const retry = await bin(f, "remove", ID, "--confirm-dead-root", f.config.rootId);
      expect(retry).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(ID)?.catch(() => undefined);
      expect(fs.existsSync(s.marker)).toBe(false);
      expect(f.host.actors.cleanupObligation(ID)).toBeUndefined();
      expect(s.registered()).toBe(false);
      expect(calls).toEqual([
        { at: "beforeCheck", fenced: true }, { at: "operatorCheck", fenced: true }, { at: "beforeDelete", fenced: true },
        { at: "rm", path: path.resolve(s.tree), fenced: true }]);
      expect(s.history()).toBe("history\n"); // recorded, never performed
      expect(fenced()).toBe(false);
    } finally { if (mainOpen) await main.close(); await f.close(); vi.restoreAllMocks(); }
  }, 120_000);

  it("recorded: counterexample, a foreign-root legacy marker is rejected", async () => {
    const f = await fixture();
    const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
    const calls: Call[] = [];
    try {
      const s = seedLegacy(f, { rootId: "session:other-root" });
      const bytes = fs.readFileSync(s.marker, "utf8");
      guardActorTrees([s.sessionRoot], fenced, calls);
      observe(fenced, calls);
      const result = await bin(f, "remove", ID, "--confirm-dead-root", f.config.rootId);
      expect(result.code).toBe(1);
      expect(result.err).toMatch(/fabric-actors:/);
      expect(calls).toEqual([]);
      expect(fs.readFileSync(s.marker, "utf8")).toBe(bytes);
      expect(s.history()).toBe("history\n");
    } finally { await f.close(); vi.restoreAllMocks(); }
  }, 120_000);

  it.each([
    ["remove by the owner name alias", "remove", NAME],
    ["stop by the exact id", "stop", ID],
    ["stop by the owner name alias", "stop", NAME],
  ] as const)("recorded: counterexample, %s cannot select the legacy cleanup", async (_label, action, selector) => {
    const f = await fixture();
    const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
    const calls: Call[] = [];
    try {
      const s = seedLegacy(f);
      const bytes = fs.readFileSync(s.marker, "utf8");
      guardActorTrees([s.sessionRoot], fenced, calls);
      observe(fenced, calls);
      const result = await bin(f, action, selector, "--confirm-dead-root", f.config.rootId);
      expect(result.code).toBe(1);
      expect(calls).toEqual([]);
      expect(fs.readFileSync(s.marker, "utf8")).toBe(bytes);
      expect(s.history()).toBe("history\n");
      expect(s.registered()).toBe(false);
    } finally { await f.close(); vi.restoreAllMocks(); }
  }, 120_000);

  it("recorded: counterexample, an exact-id dry run mutates nothing", async () => {
    const f = await fixture();
    const fenced = () => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
    const calls: Call[] = [];
    try {
      const s = seedLegacy(f);
      const bytes = fs.readFileSync(s.marker, "utf8");
      guardActorTrees([s.sessionRoot], fenced, calls);
      observe(fenced, calls);
      // Whether it reports the actor or refuses is not the point; that it mutates nothing is.
      await bin(f, "remove", ID, "--confirm-dead-root", f.config.rootId, "--dry-run");
      expect(calls).toEqual([]);
      expect(fs.readFileSync(s.marker, "utf8")).toBe(bytes);
      expect(s.history()).toBe("history\n");
      expect(s.registered()).toBe(false);
    } finally { await f.close(); vi.restoreAllMocks(); }
  }, 120_000);

  // HOSTED ONLY. NEVER RUN LOCALLY (#4664: it performs a real actor-tree deletion). The parent selects only the
  // "recorded:" tests locally; this one runs only in hosted CI as the real-path red/green proof. No interception.
  it.runIf(hostedOnly("legacy session actor deletion"))("hosted real deletion: the real bin's exact-id operator remove deletes the legacy session tree and its marker", async () => {
    const f = await fixture();
    try {
      const s = seedLegacy(f);
      expect(fs.existsSync(s.tree)).toBe(true);
      const removed = await bin(f, "remove", ID, "--confirm-dead-root", f.config.rootId);
      expect(removed).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(ID)?.catch(() => undefined);
      expect(fs.existsSync(s.marker)).toBe(false);
      expect(fs.existsSync(s.tree)).toBe(false);
      expect(f.host.actors.cleanupObligation(ID)).toBeUndefined();
      expect(s.registered()).toBe(false);
      expect(mainPublicationFenced(f.config.meshRoot, f.config.rootId)).toBe(false);
    } finally { await f.close(); }
  }, 120_000);
});
