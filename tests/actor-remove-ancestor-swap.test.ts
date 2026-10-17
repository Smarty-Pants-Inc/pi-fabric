// smarty-dev#10074: the live actor-tree delete (#finishCleanup -> deleteRootActorTree) must not be redirected outside
// the actor root when an ANCESTOR of the session directory is swapped for a symlink to a sentinel directory after the
// removal's final check. A path-based recursive rmSync followed it; verifiedRemove (#7916) removes only the judged
// inode, through a NOFOLLOW parent, and a refusal keeps the cleanup pending. Counterexamples: an in-tree symlink that
// exists at judgment is removed as a link (never followed) and the tree still goes; a missing tree stays idempotent.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { actorTreeDeletionHooks } from "../src/topology/main-publication-fence.js";

const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  delete actorTreeDeletionHooks.beforeDelete;
  for (const close of closers.splice(0)) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-ancestor-swap-"));
  roots.push(root);
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  const actors = new ActorManager("swap", { id: "session:swap", name: "main", kind: "main", sessionId: "swap" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
    { actorRoot, persistent: true, meshCursorPath: path.join(actorRoot, "mesh-cursor.json") });
  closers.push(() => actors.close(), () => agents.close());
  const actor = await actors.create({ name: "victim", instructions: "x", topics: ["t.x"], responseMode: "text" });
  const sessionDir = path.join(actorRoot, actor.id);
  fs.mkdirSync(path.join(sessionDir, "sub"), { recursive: true });
  fs.writeFileSync(path.join(sessionDir, "sub", "file.txt"), "actor data");
  // The sentinel: a directory OUTSIDE the actor root, laid out so the redirected path exists inside it.
  const sentinel = path.join(root, "sentinel");
  fs.mkdirSync(path.join(sentinel, actor.id, "sub"), { recursive: true });
  fs.writeFileSync(path.join(sentinel, actor.id, "sub", "precious.txt"), "must survive");
  fs.writeFileSync(path.join(sentinel, "outside.txt"), "must survive");
  const sentinelIntact = () => fs.existsSync(path.join(sentinel, actor.id, "sub", "precious.txt")) &&
    fs.existsSync(path.join(sentinel, "outside.txt"));
  return { actorRoot, sessionDir, sentinel, sentinelIntact, actor, actors };
};

describe("smarty-dev#10074: actor-tree delete vs an ancestor swapped for a symlink", () => {
  it.runIf(process.platform === "linux")("ancestor (the actor root) swapped right before the delete: the sentinel survives and cleanup stays pending", async () => {
    const { actorRoot, sessionDir, sentinel, sentinelIntact, actor, actors } = await setup();
    const real = `${actorRoot}.real`;
    let swaps = 0;
    actorTreeDeletionHooks.beforeDelete = () => {
      if (swaps++) return; // one swap: the retry below runs against the restored root
      // After the final check (the judgment), before the delete: the actor root becomes a symlink to the sentinel.
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
      // Refused, not silently dropped: the obligation and the real tree remain for a retry.
      expect(result).toMatchObject({ removed: true, cleaned: false, pending: expect.stringContaining("actor tree not removed") });
      expect(fs.existsSync(path.join(real, `removal-${actor.id}.json`))).toBe(true);
      expect(fs.existsSync(path.join(real, actor.id, "sub", "file.txt"))).toBe(true);
    } finally { restore(); }
    // The retained obligation drains once the root is real again.
    const retry = await actors.remove(actor.id);
    expect(retry).not.toHaveProperty("pending");
    expect(fs.existsSync(sessionDir)).toBe(false);
    expect(sentinelIntact()).toBe(true);
  }, 30_000);

  it.runIf(process.platform === "linux")("the session directory replaced by a same-name directory after the check: neither is removed, cleanup stays pending", async () => {
    const { sessionDir, sentinelIntact, actor, actors } = await setup();
    const judgedAway = `${sessionDir}.judged`;
    actorTreeDeletionHooks.beforeDelete = () => {
      fs.renameSync(sessionDir, judgedAway);
      fs.mkdirSync(sessionDir);
      fs.writeFileSync(path.join(sessionDir, "replacement.txt"), "not the judged tree");
    };
    const result = await actors.remove(actor.id);
    expect(result).toMatchObject({ removed: true, cleaned: false, pending: expect.stringContaining("actor tree not removed") });
    expect(fs.readFileSync(path.join(sessionDir, "replacement.txt"), "utf8")).toBe("not the judged tree");
    expect(fs.existsSync(path.join(judgedAway, "sub", "file.txt"))).toBe(true);
    expect(sentinelIntact()).toBe(true);
  }, 30_000);

  it.runIf(process.platform === "linux")("an in-tree symlink present at judgment is removed as a link, not followed, and the tree still goes", async () => {
    const { sessionDir, sentinel, sentinelIntact, actor, actors } = await setup();
    // Laid out BEFORE the removal, so the judged tree already holds it (no mtime change after the judgment).
    fs.symlinkSync(path.join(sentinel, actor.id, "sub"), path.join(sessionDir, "link"), "dir");
    const result = await actors.remove(actor.id);
    expect(result).not.toHaveProperty("pending");
    expect(fs.existsSync(sessionDir)).toBe(false);
    expect(sentinelIntact()).toBe(true);
  }, 30_000);

  // Fence-supported platforms only (unsupported ones refuse before any delete, smarty-dev#8090).
  it.runIf(["linux", "darwin", "win32"].includes(process.platform))("a session directory already gone before the removal: the cleanup completes (idempotent)", async () => {
    const { sessionDir, actor, actors } = await setup();
    fs.rmSync(sessionDir, { recursive: true, force: true });
    const result = await actors.remove(actor.id);
    expect(result).not.toHaveProperty("pending");
    expect(fs.existsSync(sessionDir)).toBe(false);
  }, 30_000);
});
