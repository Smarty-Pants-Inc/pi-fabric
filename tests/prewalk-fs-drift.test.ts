import { execFileSync, spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rm, stat, utimes, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { PrewalkDriftTracker } from "../src/prewalk/fs-drift.js";
import { isFabricStateRelativePath } from "../src/core/fabric-state-paths.js";

const HAS_GIT = (() => {
  try {
    return spawnSync("git", ["--version"], { stdio: "ignore" }).status === 0;
  } catch {
    return false;
  }
})();

const tempRoots: string[] = [];

const tempRoot = async (): Promise<string> => {
  const root = await mkdtemp(path.join(os.tmpdir(), "prewalk-drift-"));
  tempRoots.push(root);
  return root;
};

afterEach(async () => {
  await Promise.all(
    tempRoots.splice(0).map((root) => rm(root, { recursive: true, force: true })),
  );
});

describe("isFabricStateRelativePath", () => {
  it("accepts separator forms and rejects similar prefixes", () => {
    expect(isFabricStateRelativePath(".pi/fabric")).toBe(true);
    expect(isFabricStateRelativePath(".pi/fabric/mcp-cache.json")).toBe(true);
    expect(isFabricStateRelativePath(".pi\\fabric\\mcp-cache.json")).toBe(true);
    expect(isFabricStateRelativePath("src/.pi/fabric/state.json")).toBe(false);
    // A directory that merely starts with the same name is tracked.
    expect(isFabricStateRelativePath(".pi/fabricated/state.json")).toBe(false);
    expect(isFabricStateRelativePath(".pi/fabrication.json")).toBe(false);
    expect(isFabricStateRelativePath("src/app.ts")).toBe(false);
  });
});

describe("PrewalkDriftTracker", () => {
  describe.each([false, true])("runtime exclusion before file cap (git=%s)", (git) => {
    it.skipIf(git && !HAS_GIT)("keeps source edits detectable when Fabric state exceeds the cap", async () => {
      const root = await tempRoot();
      if (git) execFileSync("git", ["init", "-q", root]);
      await mkdir(path.join(root, ".pi", "fabric"), { recursive: true });
      await writeFile(path.join(root, "app.ts"), "before");
      for (let i = 0; i < 4; i++) {
        await writeFile(path.join(root, ".pi", "fabric", `${i}.json`), "{}");
      }
      const tracker = new PrewalkDriftTracker({ maxTrackedFiles: 1 });
      await tracker.captureBaseline("session-1", root);
      await writeFile(path.join(root, "app.ts"), "after: genuine source edit");
      expect(await tracker.evaluate("session-1", root)).toMatchObject({ files: ["app.ts"], modified: 1 });
      expect(await tracker.evaluate("session-1", root)).toBeUndefined();
    });
  });

  it("detects added, modified, and deleted files between baselines", async () => {
    const root = await tempRoot();
    await writeFile(path.join(root, "a.txt"), "alpha");
    const tracker = new PrewalkDriftTracker();
    await tracker.captureBaseline("session-1", root);

    await writeFile(path.join(root, "a.txt"), "alpha-plus");
    await writeFile(path.join(root, "b.txt"), "beta");
    const drift = await tracker.evaluate("session-1", root);

    expect(drift?.added).toBe(1);
    expect(drift?.modified).toBe(1);
    expect(drift?.deleted).toBe(0);
    expect(drift?.files.slice().sort()).toEqual(["a.txt", "b.txt"]);

    await rm(path.join(root, "a.txt"));
    const second = await tracker.evaluate("session-1", root);
    expect(second?.deleted).toBe(1);
    expect(second?.files).toEqual(["a.txt"]);
  });

  it("advances the baseline so the same change never fires twice", async () => {
    const root = await tempRoot();
    await writeFile(path.join(root, "a.txt"), "alpha");
    const tracker = new PrewalkDriftTracker();
    await tracker.captureBaseline("session-1", root);

    await writeFile(path.join(root, "a.txt"), "alpha-plus");
    expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
    expect(await tracker.evaluate("session-1", root)).toBeUndefined();
  });

  it("claims mtime-only churn once, records the content hash, then filters repeats", async () => {
    const root = await tempRoot();
    const file = path.join(root, "a.txt");
    await writeFile(file, "alpha");
    const tracker = new PrewalkDriftTracker();
    await tracker.captureBaseline("session-1", root);

    // First sighting: the baseline holds no content hash yet, so the churn
    // still claims — and teaches the fresh baseline a.txt's SHA-1.
    let stamp = await stat(file);
    await utimes(file, stamp.atime, new Date(stamp.mtimeMs + 5_000));
    const first = await tracker.evaluate("session-1", root);
    expect(first).toMatchObject({ modified: 1, unchanged: 0, files: ["a.txt"] });

    // Repeat churn with identical content is filtered as mtime-only noise.
    stamp = await stat(file);
    await utimes(file, stamp.atime, new Date(stamp.mtimeMs + 5_000));
    expect(await tracker.evaluate("session-1", root)).toBeUndefined();
    // Content actually changing claims again regardless of the recorded hash.
    await writeFile(file, "alpha-2");
    expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
  });

  describe.each([false, true])("hash continuity (git=%s)", (git) => {
    it.skipIf(git && !HAS_GIT)("retains learned hashes across idle scans, not across resets or unverified changes", async () => {
      const root = await tempRoot();
      if (git) execFileSync("git", ["init", "-q", root]);
      const file = path.join(root, "a.txt");
      await writeFile(file, "alpha");
      // A second file keeps single-file deletion observable: an empty manifest
      // would trigger the wholesale-collapse re-anchor instead of a report.
      await writeFile(path.join(root, "b.txt"), "keeper");
      const tracker = new PrewalkDriftTracker();
      await tracker.captureBaseline("session-1", root);
      const touch = async () => {
        const stamp = await stat(file);
        await utimes(file, stamp.atime, new Date(stamp.mtimeMs + 5_000));
      };
      await touch();
      expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
      for (let i = 0; i < 3; i++) expect(await tracker.evaluate("session-1", root)).toBeUndefined();
      await touch();
      expect(await tracker.evaluate("session-1", root)).toBeUndefined();
      await writeFile(file, "bravo"); // Same size, genuinely different content.
      expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
      await tracker.evaluate("session-1", root);
      await touch();
      expect(await tracker.evaluate("session-1", root)).toBeUndefined();
      // Over-size drift must forget the old hash rather than carrying it forward.
      await writeFile(file, "x".repeat((16 << 20) + 1));
      expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
      await writeFile(file, "bravo");
      expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
      await tracker.captureBaseline("session-2", root);
      await touch();
      expect(await tracker.evaluate("session-2", root)).toMatchObject({ modified: 1 });
      await rm(file);
      expect(await tracker.evaluate("session-1", root)).toMatchObject({ deleted: 1 });
      await writeFile(file, "bravo");
      expect(await tracker.evaluate("session-1", root)).toMatchObject({ added: 1 });
      await touch();
      expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
      const other = await tempRoot();
      await writeFile(path.join(other, "a.txt"), "bravo");
      expect(await tracker.evaluate("session-1", other)).toBeUndefined();
      const stamp = await stat(path.join(other, "a.txt"));
      await utimes(path.join(other, "a.txt"), stamp.atime, new Date(stamp.mtimeMs + 5_000));
      expect(await tracker.evaluate("session-1", other)).toMatchObject({ modified: 1 });
    });
  });

  it("filters hashed churn while still reporting real content changes", async () => {
    const root = await tempRoot();
    const churned = path.join(root, "a.txt");
    const real = path.join(root, "b.txt");
    await writeFile(churned, "alpha");
    await writeFile(real, "beta");
    const tracker = new PrewalkDriftTracker();
    await tracker.captureBaseline("session-1", root);

    // Real content change claims and records a.txt's hash in the new baseline.
    await writeFile(churned, "alpha-2");
    expect(await tracker.evaluate("session-1", root)).toMatchObject({
      modified: 1,
      files: ["a.txt"],
    });

    // a.txt churns with identical content while b.txt actually changes.
    const stamp = await stat(churned);
    await utimes(churned, stamp.atime, new Date(stamp.mtimeMs + 5_000));
    await writeFile(real, "beta-2");
    const second = await tracker.evaluate("session-1", root);
    expect(second).toMatchObject({ modified: 1, unchanged: 1, files: ["b.txt"] });
  });

  it("baselines silently on first evaluation and claims only later drift", async () => {
    const root = await tempRoot();
    await writeFile(path.join(root, "a.txt"), "alpha");
    const tracker = new PrewalkDriftTracker();

    expect(await tracker.evaluate("session-new", root)).toBeUndefined();
    await writeFile(path.join(root, "a.txt"), "alpha-plus");
    expect(await tracker.evaluate("session-new", root)).toMatchObject({ modified: 1 });
  });

  it("keeps baselines isolated per session", async () => {
    const root = await tempRoot();
    await writeFile(path.join(root, "a.txt"), "alpha");
    const tracker = new PrewalkDriftTracker();
    await tracker.captureBaseline("session-1", root);

    await writeFile(path.join(root, "a.txt"), "alpha-plus");
    expect(await tracker.evaluate("session-2", root)).toBeUndefined();
    expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
  });

  it("skips node_modules and .git in git-less walks", async () => {
    const root = await tempRoot();
    await mkdir(path.join(root, "node_modules", "dep"), { recursive: true });
    await writeFile(path.join(root, "node_modules", "dep", "index.js"), "v1");
    await writeFile(path.join(root, "watched.ts"), "one");
    const tracker = new PrewalkDriftTracker();
    await tracker.captureBaseline("session-1", root);

    await writeFile(path.join(root, "node_modules", "dep", "index.js"), "v2-changed");
    expect(await tracker.evaluate("session-1", root)).toBeUndefined();

    await writeFile(path.join(root, "watched.ts"), "one-changed");
    expect(await tracker.evaluate("session-1", root)).toMatchObject({ modified: 1 });
  });

  it.skipIf(!HAS_GIT)(
    "uses the git index so ignored artifacts never register as drift",
    async () => {
      const root = await tempRoot();
      execFileSync("git", ["init", "-q", root]);
      await writeFile(path.join(root, ".gitignore"), "ignored/\n");
      await mkdir(path.join(root, "ignored"));
      await writeFile(path.join(root, "ignored", "artifact.js"), "v1");
      await writeFile(path.join(root, "watched.ts"), "one");
      const tracker = new PrewalkDriftTracker();
      await tracker.captureBaseline("session-1", root);

      await writeFile(path.join(root, "ignored", "artifact.js"), "v2-changed");
      await writeFile(path.join(root, "ignored", "fresh.js"), "new file");
      expect(await tracker.evaluate("session-1", root)).toBeUndefined();

      await writeFile(path.join(root, "watched.ts"), "one-changed");
      const drift = await tracker.evaluate("session-1", root);
      expect(drift?.files).toEqual(["watched.ts"]);
      expect(drift?.modified).toBe(1);
    },
  );

  it("rebaselines without claiming when the tree overflows the file cap", async () => {
    const root = await tempRoot();
    for (let index = 0; index < 4; index += 1) {
      await writeFile(path.join(root, `file-${index}.txt`), String(index));
    }
    const tracker = new PrewalkDriftTracker({ maxTrackedFiles: 3 });
    await tracker.captureBaseline("session-1", root);

    await writeFile(path.join(root, "file-0.txt"), "zero-changed");
    expect(await tracker.evaluate("session-1", root)).toBeUndefined();
  });

  it("excludes Fabric's own state directory and leaves other tool directories to project ignore rules", async () => {
    const root = await tempRoot();
    await mkdir(path.join(root, ".pi", "fabric"), { recursive: true });
    await mkdir(path.join(root, ".idea"), { recursive: true });
    await writeFile(path.join(root, ".pi", "fabric", "mcp-cache.json"), "{}\n");
    await writeFile(path.join(root, ".idea", "notes.md"), "one");
    await writeFile(path.join(root, "watched.ts"), "one");
    const tracker = new PrewalkDriftTracker();
    await tracker.captureBaseline("session-1", root);

    // A Fabric cache refresh is bookkeeping, not task work: it never claims.
    await writeFile(path.join(root, ".pi", "fabric", "mcp-cache.json"), "{\"servers\":[]}\n");
    expect(await tracker.evaluate("session-1", root)).toBeUndefined();

    // Another tool's directory is not Fabric's to hardcode. In a Git work tree
    // the project's own ignore rules decide; in a plain tree it still counts.
    await writeFile(path.join(root, ".idea", "notes.md"), "two");
    await writeFile(path.join(root, "watched.ts"), "one-changed");
    const drift = await tracker.evaluate("session-1", root);
    expect(drift?.files.slice().sort()).toEqual([".idea/notes.md", "watched.ts"]);
    // Reported separators are canonical on every platform, so drift evidence
    // reads the same whether it came from the git listing or a Windows walk.
    expect(drift?.files.every((file) => !file.includes("\\"))).toBe(true);
    expect(drift?.modified).toBe(2);
  });
});
