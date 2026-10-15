import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

// This is a real-bundle entry test. Fail before the first test if the checked-in
// CLI no longer takes the hard dry-run / native-fence path audited for this case.
const cliBundle = fs.readFileSync(path.resolve("dist/storage/retention-cli.js"), "utf8");
for (const marker of [
  "dryRun: mode !== \"--apply\"",
  "if (options.dryRun)",
  "lockFile(lock, 0, true)",
  "WOULD_FREE_GIB",
]) {
  if (!cliBundle.includes(marker)) throw new Error(`retention CLI safety proof is unknown: missing ${marker}`);
}

const roots: string[] = [];
const parentDeletes: Array<{ op: string; target: string }> = [];

afterEach(() => {
  // Restore the parent recorders before the only permitted teardown deletion.
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  parentDeletes.splice(0);
});

const writePrivate = (file: string, bytes: string): void => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, bytes, { mode: 0o600 });
  fs.chmodSync(file, 0o600);
};

const snapshot = (root: string): string => {
  const entries: unknown[] = [];
  const visit = (file: string, relative: string): void => {
    const stat = fs.lstatSync(file);
    const kind = stat.isDirectory() ? "directory" : stat.isFile() ? "file" : stat.isSymbolicLink() ? "symlink" : "other";
    const bytes = stat.isFile() ? fs.readFileSync(file, "utf8") : stat.isSymbolicLink() ? fs.readlinkSync(file) : undefined;
    entries.push([relative, kind, stat.dev, stat.ino, stat.mode, stat.size, stat.mtimeMs, stat.ctimeMs, bytes]);
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) visit(path.join(file, name), path.join(relative, name));
  };
  visit(root, ".");
  return JSON.stringify(entries);
};

const recordParentDeleters = (): void => {
  // Parent retention deleters and their non-Linux fallbacks must not run in this
  // test. The real registry release is recorded too; it is emulated below by
  // moving the private lock aside, never by deleting it.
  vi.spyOn(fs, "rmSync").mockImplementation(((target: fs.PathLike) => {
    parentDeletes.push({ op: "rm", target: String(target) });
  }) as never);
  vi.spyOn(fs, "unlinkSync").mockImplementation(((target: fs.PathLike) => {
    parentDeletes.push({ op: "unlink", target: String(target) });
  }) as never);
  vi.spyOn(fs, "rmdirSync").mockImplementation(((target: fs.PathLike) => {
    parentDeletes.push({ op: "rmdir", target: String(target) });
  }) as never);
};

const makeFixture = () => {
  const tmp = process.env.TMPDIR;
  if (!tmp) throw new Error("TMPDIR is required for the private fixture");
  const privateRoot = fs.mkdtempSync(path.join(tmp, "fs-retlock-1072-r4-"));
  roots.push(privateRoot);
  const meshRoot = path.join(privateRoot, "scratchmesh");
  const residencyRoot = path.join(meshRoot, "residency", "scratch-host");
  const actorsRoot = path.join(meshRoot, "actors");
  const actorRoot = path.join(actorsRoot, "private-actor");
  fs.mkdirSync(actorRoot, { recursive: true, mode: 0o700 });
  for (const directory of [meshRoot, path.join(meshRoot, "residency"), residencyRoot, path.join(meshRoot, "actors"), actorsRoot, actorRoot]) {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    fs.chmodSync(directory, 0o700);
  }

  const sessionFile = path.join(actorRoot, "session.jsonl");
  const oldBackup = `${sessionFile}.20200101T000000000Z.bak`;
  const newestBackup = `${sessionFile}.20200102T000000000Z.bak`;
  writePrivate(sessionFile, "live-session\n");
  writePrivate(oldBackup, "old-backup-bytes\n");
  writePrivate(newestBackup, "newest-backup-bytes\n");
  fs.utimesSync(oldBackup, 1_000, 1_000);
  fs.utimesSync(newestBackup, 1_001, 1_001);

  let deadFixturePid = true;
  try { process.kill(2147483647, 0); deadFixturePid = false; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
  if (!deadFixturePid) throw new Error("fixture PID 2147483647 is unexpectedly resident");

  const actor = { id: "private-actor", rootId: "scratch-root", status: "idle", sessionFile };
  writePrivate(path.join(actorsRoot, "actors.json"), JSON.stringify({ format: 1, actors: [actor] }));
  writePrivate(path.join(residencyRoot, "host.lock"), JSON.stringify({ pid: 2147483647 }) + "\n");
  writePrivate(path.join(residencyRoot, "config.json"), JSON.stringify({
    format: 1, rootId: "scratch-root", residencyRoot, meshRoot,
  }) + "\n");
  writePrivate(path.join(meshRoot, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }) + "\n");

  return { privateRoot, meshRoot, actorsRoot, actorRoot, sessionFile, oldBackup, newestBackup,
    hostLock: path.join(residencyRoot, "host.lock"), registryLock: path.join(actorsRoot, "actors.json.lock") };
};

const runRealCli = (meshRoot: string) => {
  const env = { ...process.env };
  // No inherited preload or loader is permitted for the product entry process.
  delete env.NODE_OPTIONS;
  delete env.NODE_PATH;
  delete env.PI_FABRIC_COMMIT_STATS;
  env.PI_FABRIC_LOCK_STATS = "0";
  const argv = [path.resolve("dist/storage/retention-cli.js"), meshRoot, "--dry-run"];
  const result = spawnSync(process.execPath, argv, {
    cwd: process.cwd(),
    env,
    encoding: "utf8",
    timeout: 30_000,
    maxBuffer: 16 * 1024 * 1024,
  });
  return { argv, result };
};

const parseCli = (run: ReturnType<typeof runRealCli>) => {
  expect(run.argv).toEqual([path.resolve("dist/storage/retention-cli.js"), expect.any(String), "--dry-run"]);
  expect(run.argv.some((arg) => ["--apply", "--report", "--runs-older-than"].includes(arg))).toBe(false);
  expect(run.result.error).toBeUndefined();
  expect(run.result.status, run.result.stderr).toBe(0);
  return JSON.parse(run.result.stdout) as {
    dryRun: boolean; changes: Array<{ path: string; beforeBytes: number; afterBytes: number }>;
    removedRuns: string[]; bytesBefore: number; bytesAfter: number;
    skipped: Array<{ path: string; reason: string }>;
  };
};

describe.skipIf(process.platform !== "linux")("retention CLI real entry with a live actor-registry owner (#1072 round 4)", () => {
  it("holds the API lock across the child, then proves the unheld counterexample is non-vacuous", async () => {
    recordParentDeleters(); // Installed before fixture setup, including the parent release path.
    const testFixture = makeFixture();
    const store = new ActorRegistryStore(testFixture.actorsRoot);
    const label = "retention-cli557";

    let firstReleaseRecorded = false;
    let firstPlan: ReturnType<typeof parseCli> | undefined;
    await store.withLock(async () => {
      const ownerFile = path.join(testFixture.registryLock, "owner");
      const ownerBytes = fs.readFileSync(ownerFile, "utf8");
      const ownerInode = fs.lstatSync(ownerFile).ino;
      const ownerSlots = ownerBytes.split("\n");
      expect(ownerSlots).toHaveLength(6);
      expect(ownerSlots[0]).toMatch(/^[0-9a-f-]{36}$/);
      expect(ownerSlots[1]).toBe(String(process.pid));
      expect(ownerSlots[2]).toMatch(/^\d+$/);
      expect(ownerSlots[3]).toMatch(/^\d+$/);
      expect(ownerSlots[4]).toBe(label);
      expect(ownerSlots[5]).toBe("");
      expect(() => process.kill(Number(ownerSlots[1]), 0)).not.toThrow();
      const [token, pid, created, start] = ownerSlots;
      const heldOwnerForms = [
        ownerBytes,
        `${token}\n${pid}\n${created}\n`,
        `${token}\n${pid}\n${created}\n${start}\n`,
      ];

      for (const ownerForm of heldOwnerForms) {
        fs.writeFileSync(ownerFile, ownerForm, { encoding: "utf8", mode: 0o600 });
        fs.chmodSync(ownerFile, 0o600);
        const before = snapshot(testFixture.privateRoot);
        const run = runRealCli(testFixture.meshRoot);
        const plan = parseCli(run);
        if (ownerForm === ownerBytes) firstPlan = plan;
        expect(run.result.stderr).toContain("WOULD_FREE_GIB=0.000");
        expect(plan).toMatchObject({ dryRun: true, changes: [], removedRuns: [], bytesBefore: 0, bytesAfter: 0 });
        expect(plan.skipped).toEqual([{
          path: testFixture.actorsRoot,
          reason: expect.stringMatching(/holder/),
        }]);
        expect(snapshot(testFixture.privateRoot)).toBe(before);
        expect(fs.readFileSync(ownerFile, "utf8")).toBe(ownerForm);
        expect(fs.lstatSync(ownerFile).ino).toBe(ownerInode);
        expect(() => process.kill(Number(pid), 0)).not.toThrow();
        expect(parentDeletes).toEqual([]);
      }
      // Restore the API-written five-slot owner before withLock's real release check.
      fs.writeFileSync(ownerFile, ownerBytes, { encoding: "utf8", mode: 0o600 });
      fs.chmodSync(ownerFile, 0o600);
    }, undefined, label);
    firstReleaseRecorded = parentDeletes.length === 1 && parentDeletes[0]!.op === "rm" && parentDeletes[0]!.target === testFixture.registryLock;
    expect(firstReleaseRecorded).toBe(true);
    expect(parentDeletes.filter((call) => call.op !== "rm" || call.target !== testFixture.registryLock)).toEqual([]);
    // The recorded API release is emulated by a rename, not an rm; this also
    // makes the later unheld control's canonical path available.
    const firstAside = path.join(testFixture.actorsRoot, ".fixture-released-lock-1");
    fs.renameSync(testFixture.registryLock, firstAside);

    let secondReleaseRecorded = false;
    let secondPlan: ReturnType<typeof parseCli> | undefined;
    await store.withLock(async () => {
      const childAside = path.join(testFixture.actorsRoot, ".fixture-active-lock-aside");
      fs.renameSync(testFixture.registryLock, childAside);
      try {
        const before = snapshot(testFixture.privateRoot);
        const run = runRealCli(testFixture.meshRoot);
        secondPlan = parseCli(run);
        expect(run.result.stderr).toContain("WOULD_FREE_GIB=0.000");
        expect(secondPlan).toMatchObject({
          dryRun: true,
          removedRuns: [],
          bytesBefore: fs.statSync(testFixture.oldBackup).size,
          bytesAfter: 0,
        });
        expect(secondPlan!.changes).toEqual([{
          path: testFixture.oldBackup,
          beforeBytes: fs.statSync(testFixture.oldBackup).size,
          afterBytes: 0,
        }]);
        expect(secondPlan!.skipped).toEqual([]);
        expect(snapshot(testFixture.privateRoot)).toBe(before);
        expect(fs.readFileSync(testFixture.oldBackup, "utf8")).toBe("old-backup-bytes\n");
        expect(fs.readFileSync(testFixture.newestBackup, "utf8")).toBe("newest-backup-bytes\n");
      } finally {
        fs.renameSync(childAside, testFixture.registryLock);
      }
    }, undefined, label);
    secondReleaseRecorded = parentDeletes.length === 2 && parentDeletes[1]!.op === "rm" && parentDeletes[1]!.target === testFixture.registryLock;
    expect(secondReleaseRecorded).toBe(true);
    expect(parentDeletes.filter((call) => call.op !== "rm" || call.target !== testFixture.registryLock)).toEqual([]);
    expect(fs.existsSync(testFixture.oldBackup)).toBe(true);
    expect(fs.existsSync(testFixture.newestBackup)).toBe(true);
    expect(firstPlan).toBeDefined();
    expect(secondPlan).toBeDefined();
  }, 30_000);
});
