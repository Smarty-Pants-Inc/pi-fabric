import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { awaitAgentCwd } from "../src/agents/manager.js";

// smarty-dev#1668: a spawn runs in parallel with the `git worktree add` that creates its cwd.
// Git creates the directory before its checkout finishes, so the spawn must wait for the whole
// add, and fail when the add fails.
const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", ["-c", "user.email=t@example.invalid", "-c", "user.name=t", ...args], { cwd, encoding: "utf8" });

/** A repository whose checkout of slow.txt runs `smudge` (a slow or failing filter). */
const slowRepository = (smudge: string): { root: string; repo: string } => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-cwd-")));
  roots.push(root);
  const repo = path.join(root, "repo");
  fs.mkdirSync(repo);
  git(repo, "init", "-q");
  fs.writeFileSync(path.join(repo, ".gitattributes"), "slow.txt filter=slow\n");
  fs.writeFileSync(path.join(repo, "slow.txt"), "complete\n");
  git(repo, "add", ".");
  git(repo, "commit", "-qm", "init");
  git(repo, "config", "filter.slow.smudge", smudge);
  git(repo, "config", "filter.slow.required", "true");
  return { root, repo };
};

const worktreeAdd = (repo: string, target: string): Promise<number | null> => {
  const child = spawn("git", ["worktree", "add", "-q", "-b", "wt", target], { cwd: repo, stdio: "ignore" });
  return new Promise((resolve) => child.on("exit", (code) => resolve(code)));
};

describe.skipIf(process.platform === "win32")("awaitAgentCwd beside a parallel git worktree add", () => {
  it("returns only after the checkout finishes, with the full tree", async () => {
    const { root, repo } = slowRepository("sleep 2; cat");
    const target = path.join(root, "wt");
    let addedAt = 0;
    const added = worktreeAdd(repo, target).then((code) => { addedAt = Date.now(); return code; });
    const cwd = await awaitAgentCwd(root, target);
    const resolvedAt = Date.now();
    expect(await added).toBe(0);
    expect(cwd).toBe(target);
    expect(resolvedAt).toBeGreaterThanOrEqual(addedAt - 50);
    expect(fs.readFileSync(path.join(cwd, "slow.txt"), "utf8")).toBe("complete\n");
  }, 60_000);

  it("fails when the worktree add fails instead of spawning in its leftovers", async () => {
    const { root, repo } = slowRepository("sleep 1; exit 1");
    const target = path.join(root, "wt");
    const added = worktreeAdd(repo, target);
    await expect(awaitAgentCwd(root, target)).rejects.toThrow(/Invalid Fabric agent cwd/);
    expect(await added).not.toBe(0);
  }, 60_000);

  it("resolves a plain directory and finished locked worktrees at once", async () => {
    const { root, repo } = slowRepository("cat");
    const plain = path.join(root, "plain");
    fs.mkdirSync(plain);
    const addLocked = path.join(root, "add-locked");
    git(repo, "worktree", "add", "-q", "--lock", "--reason", "kept", "-b", "add-locked", addLocked);
    // `git worktree lock` with no reason leaves an empty `locked` file (review/astra round 3).
    const noReason = path.join(root, "no-reason");
    git(repo, "worktree", "add", "-q", "-b", "no-reason", noReason);
    git(repo, "worktree", "lock", noReason);
    const reason = path.join(root, "reason");
    git(repo, "worktree", "add", "-q", "-b", "reason", reason);
    git(repo, "worktree", "lock", "--reason", "x", reason);
    expect(fs.readFileSync(path.join(repo, ".git/worktrees/no-reason/locked"), "utf8")).toBe("");
    const started = Date.now();
    for (const dir of [plain, addLocked, noReason, reason]) expect(await awaitAgentCwd(root, dir)).toBe(dir);
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  it("waits while git's own initializing lock is present", async () => {
    const { root, repo } = slowRepository("cat");
    const target = path.join(root, "init");
    git(repo, "worktree", "add", "-q", "-b", "init", target);
    const lock = path.join(repo, ".git/worktrees/init/locked");
    fs.writeFileSync(lock, "initializing");
    setTimeout(() => fs.rmSync(lock), 700);
    const started = Date.now();
    expect(await awaitAgentCwd(root, target)).toBe(target);
    expect(Date.now() - started).toBeGreaterThanOrEqual(650);
  });

  it("still fails a cwd that never appears, with the next-message hint", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-cwd-")));
    roots.push(root);
    await expect(awaitAgentCwd(root, path.join(root, "missing"))).rejects.toThrow(
      /ENOENT.*call spawn in the next message/,
    );
  }, 10_000);
});
