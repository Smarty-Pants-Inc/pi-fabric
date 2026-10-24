// The live actor-tree removal (#finishCleanup -> deleteRootActorTree) must never move anything outside the judged tree
// when the tree or an ANCESTOR of it is swapped after the removal's final check. Since smarty-dev#16444
// (R-keep-all-traces) the tree is archived, never deleted: one rename of the judged directory into
// `<meshRoot>/session-archive/<root>/<actor>` (moveActorTreeToArchive). The rename keeps the old verifiedRemove
// guarantees on this path (pi-fabric#1568 round 3):
// - an ancestor swapped for a symlink is REFUSED: the judged route's parent, reopened without following a link, must
//   be the held parent (assertRoute); nothing moves or is recorded, the cleanup stays pending, and the retry on the
//   restored root archives the real tree (and nothing of the sentinel);
// - a same-name directory swapped in after the judgment is REFUSED by the pinned identity before the rename;
// - one swapped in inside the last window (after that check, at rename(2) itself) is caught by the post-check and
//   moved back; the judged tree is never touched.
// Linux only: the held-parent rename and the Main publication fence are Linux paths.
// Counterexamples: an in-tree symlink present at judgment is archived as a link (never followed) and the tree still
// goes; a missing tree stays idempotent on every fence platform.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { fleetSessionArchive } from "../src/storage/session-archive.js";
import { actorTreeDeletionHooks } from "../src/topology/main-publication-fence.js";

const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
  for (const close of closers.splice(0)) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const ROOT_ID = "session:swap";
/** Every entry below `root` by relative path: its inode and its bytes (a link: its target). */
const snapshot = (root: string): Record<string, string> => {
  const out: Record<string, string> = {};
  const walk = (directory: string, relative: string): void => {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name), key = relative ? `${relative}/${name}` : name;
      const stat = fs.lstatSync(file);
      if (stat.isDirectory()) { out[key] = `dir:${stat.ino}`; walk(file, key); }
      else if (stat.isSymbolicLink()) out[key] = `link:${stat.ino}:${fs.readlinkSync(file)}`;
      else out[key] = `file:${stat.ino}:${fs.readFileSync(file).toString("hex")}`;
    }
  };
  walk(root, "");
  return out;
};

const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-ancestor-swap-"));
  roots.push(root);
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  const actors = new ActorManager("swap", { id: ROOT_ID, name: "main", kind: "main", sessionId: "swap" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
    { actorRoot, persistent: true, meshCursorPath: path.join(actorRoot, "mesh-cursor.json") });
  closers.push(() => actors.close(), () => agents.close());
  const actor = await actors.create({ name: "victim", instructions: "x", topics: ["t.x"], responseMode: "text" });
  const sessionDir = path.join(actorRoot, actor.id);
  fs.mkdirSync(path.join(sessionDir, "sub"), { recursive: true });
  fs.writeFileSync(path.join(sessionDir, "sub", "file.txt"), "actor data");
  fs.writeFileSync(path.join(sessionDir, "session.jsonl"), "history\n");
  // The sentinel: a directory OUTSIDE the actor root, laid out so the redirected path exists inside it.
  const sentinel = path.join(root, "sentinel");
  fs.mkdirSync(path.join(sentinel, actor.id, "sub"), { recursive: true });
  fs.writeFileSync(path.join(sentinel, actor.id, "sub", "precious.txt"), "must survive");
  fs.writeFileSync(path.join(sentinel, "outside.txt"), "must survive");
  // Every sentinel entry keeps its inode and bytes (metadata the fixture's own stores write through a swapped root
  // may be added beside them; nothing is moved or removed).
  const sentinelBefore = snapshot(sentinel);
  const sentinelIntact = () => { const now = snapshot(sentinel); return Object.entries(sentinelBefore).every(([name, entry]) => now[name] === entry); };
  const fleet = fleetSessionArchive(mesh.root, ROOT_ID);
  const archived = fleetSessionArchive(mesh.root, ROOT_ID, actor.id);
  /** The fleet archive's manifest records (none before the first intent). */
  const manifest = (): Array<Record<string, any>> => {
    const file = path.join(fleet, "MANIFEST.jsonl");
    return fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
  };
  /** Nothing of this actor (and nothing of the sentinel) is in the fleet archive. */
  const nothingArchived = () => !fs.existsSync(fleet) || fs.readdirSync(fleet).every(name => name === "MANIFEST.jsonl");
  return { actorRoot, sessionDir, sentinel, sentinelIntact, actor, actors, fleet, archived, manifest, nothingArchived };
};

describe("actor-tree archive rename vs a tree or ancestor swapped after the judgment", () => {
  it.runIf(process.platform === "linux")("ancestor (the actor root) swapped for a symlink after the check: refused, nothing moved or recorded, the sentinel survives and the retry archives only the real tree", async () => {
    const { actorRoot, sessionDir, sentinel, sentinelIntact, actor, actors, archived, manifest, nothingArchived } = await setup();
    const before = snapshot(sessionDir);
    const real = `${actorRoot}.real`;
    let swaps = 0;
    actorTreeDeletionHooks.beforeDelete = () => {
      if (swaps++) return; // one swap: the retry below runs against the restored root
      // After the final check (the judgment), before the rename: the actor root becomes a symlink to the sentinel.
      fs.renameSync(actorRoot, real);
      fs.symlinkSync(sentinel, actorRoot, "dir");
    };
    let restored = false;
    const restore = () => {
      if (restored || swaps === 0) return;
      restored = true;
      fs.unlinkSync(actorRoot);
      fs.renameSync(real, actorRoot);
    };
    try {
      const result = await actors.remove(actor.id);
      expect(swaps).toBe(1);
      expect(sentinelIntact()).toBe(true);
      // Refused, not silently dropped: the obligation and the real tree remain for a retry, every entry unchanged.
      expect(result).toMatchObject({ removed: true, cleaned: false,
        pending: expect.stringMatching(/actor tree not archived.*route to the actor directory changed/) });
      expect(fs.existsSync(path.join(real, `removal-${actor.id}.json`))).toBe(true);
      expect(snapshot(path.join(real, actor.id))).toEqual(before);
      // Refused before the intent: nothing moved, nothing recorded.
      expect(nothingArchived()).toBe(true);
      expect(manifest()).toEqual([]);
    } finally { restore(); }
    // The retained obligation drains once the root is real again: the judged tree (same inodes, same bytes) and
    // nothing else is in the archive.
    const retry = await actors.remove(actor.id);
    expect(retry).not.toHaveProperty("pending");
    expect(fs.existsSync(sessionDir)).toBe(false);
    expect(snapshot(archived)).toEqual(before);
    expect(Object.keys(snapshot(archived)).some(name => /precious|outside/.test(name))).toBe(false);
    expect(sentinelIntact()).toBe(true);
    expect(manifest().map(line => [line.phase, line.from, line.to])).toEqual([
      ["MOVING", sessionDir, archived], ["MOVED-TO", sessionDir, archived]]);
  }, 30_000);

  it.runIf(process.platform === "linux")("the session directory replaced by a same-name directory after the check: refused by the pinned identity, neither is moved, cleanup stays pending", async () => {
    const { sessionDir, sentinelIntact, actor, actors, manifest, nothingArchived } = await setup();
    const before = snapshot(sessionDir);
    const judgedAway = `${sessionDir}.judged`;
    let replacement: Record<string, string> | undefined;
    actorTreeDeletionHooks.beforeDelete = () => {
      fs.renameSync(sessionDir, judgedAway);
      fs.mkdirSync(sessionDir);
      fs.writeFileSync(path.join(sessionDir, "replacement.txt"), "not the judged tree");
      replacement = { root: `dir:${fs.lstatSync(sessionDir).ino}`, ...snapshot(sessionDir) };
    };
    const result = await actors.remove(actor.id);
    expect(result).toMatchObject({ removed: true, cleaned: false,
      pending: expect.stringMatching(/actor tree not archived.*replaced before the archive rename.*nothing moved/) });
    // The replacement never left its name (same inode), and the judged tree is untouched where it was moved.
    expect({ root: `dir:${fs.lstatSync(sessionDir).ino}`, ...snapshot(sessionDir) }).toEqual(replacement);
    expect(snapshot(judgedAway)).toEqual(before);
    expect(sentinelIntact()).toBe(true);
    expect(nothingArchived()).toBe(true);
    expect(manifest()).toEqual([]);
  }, 30_000);

  it.runIf(process.platform === "linux")("a same-name directory swapped in at rename(2) itself (after the last pre-check): the post-check refuses and moves it back; the judged tree is untouched", async () => {
    const { sessionDir, sentinelIntact, actor, actors, fleet, manifest, nothingArchived } = await setup();
    const before = snapshot(sessionDir);
    const pinned = fs.lstatSync(sessionDir);
    const judgedAway = `${sessionDir}.judged`;
    const rename = fs.renameSync.bind(fs);
    let replacementIno: number | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      const stat = fs.lstatSync(source, { throwIfNoEntry: false });
      if (replacementIno === undefined && stat?.ino === pinned.ino && stat.dev === pinned.dev &&
          path.dirname(String(destination)) === fleet) {
        // The narrowest window: the source passed its pinned check; it is swapped right before the syscall.
        rename(sessionDir, judgedAway);
        fs.mkdirSync(sessionDir);
        fs.writeFileSync(path.join(sessionDir, "replacement.txt"), "not the judged tree");
        replacementIno = fs.lstatSync(sessionDir).ino;
      }
      rename(source, destination);
    });
    const result = await actors.remove(actor.id);
    expect(replacementIno).toBeDefined();
    expect(result).toMatchObject({ removed: true, cleaned: false,
      pending: expect.stringMatching(/actor tree not archived.*moved back; refusing/) });
    expect(fs.lstatSync(sessionDir).ino).toBe(replacementIno); // moved back to its own name
    expect(fs.readFileSync(path.join(sessionDir, "replacement.txt"), "utf8")).toBe("not the judged tree");
    expect(snapshot(judgedAway)).toEqual(before); // the judged tree never moved
    expect(sentinelIntact()).toBe(true);
    expect(nothingArchived()).toBe(true);
    // The intent was recorded before the rename; its outcome says it failed.
    expect(manifest().map(line => line.phase)).toEqual(["MOVING", "MOVE-FAILED"]);
  }, 30_000);

  it.runIf(process.platform === "linux")("an in-tree symlink present at judgment is archived as a link, not followed, and the tree still goes", async () => {
    const { sessionDir, sentinel, sentinelIntact, actor, actors, archived } = await setup();
    // Laid out BEFORE the removal, so the judged tree already holds it.
    fs.symlinkSync(path.join(sentinel, actor.id, "sub"), path.join(sessionDir, "link"), "dir");
    const before = snapshot(sessionDir);
    const result = await actors.remove(actor.id);
    expect(result).not.toHaveProperty("pending");
    expect(fs.existsSync(sessionDir)).toBe(false);
    expect(snapshot(archived)).toEqual(before);
    expect(fs.lstatSync(path.join(archived, "link")).isSymbolicLink()).toBe(true);
    expect(sentinelIntact()).toBe(true);
  }, 30_000);

  // Fence-supported platforms only (unsupported ones refuse before any change).
  it.runIf(["linux", "darwin", "win32"].includes(process.platform))("a session directory already gone before the removal: the cleanup completes (idempotent)", async () => {
    const { sessionDir, actor, actors } = await setup();
    fs.rmSync(sessionDir, { recursive: true, force: true });
    const result = await actors.remove(actor.id);
    expect(result).not.toHaveProperty("pending");
    expect(fs.existsSync(sessionDir)).toBe(false);
  }, 30_000);
});
