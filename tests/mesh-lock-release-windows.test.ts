import { spawn, spawnSync, type ChildProcessWithoutNullStreams, type SpawnOptionsWithoutStdio } from "node:child_process";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { windowsMeshCacheFixture } from "./windows-mesh-cache-fixture.js";
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import {
  encodeMeshRetireRequest, forgetWindowsMeshCapability, meshRetirementName, prepareWindowsMeshCapability, releaseWindowsMeshLock,
  windowsApiPathUnits, windowsLogicalPathUnits, windowsMeshReleaseTestHooks, type WindowsMeshCapability, type WindowsMeshCheckpoint,
  type WindowsMeshReleaseChildSample,
} from "../src/storage/windows-mesh-release.js";

// smarty-dev#9569 Commit A, hosted Windows only. Keep reason: data safety and the native trust boundary under real
// NTFS sharing, handle rename and process kills (T2 kill partitions and parent death, T3 extra children before and
// after retirement, T5 retained-capability lifetime, the A slice of T6 (sibling debris never blocks), T9 on disk).
// None of this is reachable by a mock or off Windows: an off-platform skip is NOT evidence. This suite deletes real
// files (through the native helper and its own temp fixtures), so the local deletion detector keeps it hosted-only.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const SKIP = "skip reason: real NTFS handle rename/sharing and the digest-pinned native helper exist only on hosted Windows CI";

const dirs: string[] = [];
const tempDir = (): string => {
  const dir = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fmr-")));
  dirs.push(dir);
  return dir;
};
// Only caches live under the actual profile; locks, checkpoints and copied packages stay in TMPDIR.
const cacheDir = (): string => {
  const dir = windowsMeshCacheFixture("fmr-");
  dirs.push(dir);
  return dir;
};
const record = (name: string): string => `${name}-${process.pid}-${Math.random().toString(16).slice(2)}\n${process.pid}\n${Date.now()}\n`;
/** Publish a lock like an acquisition: directory, then the owner with exclusive create. */
const acquire = (root: string, owner: string, base = ".lock"): string => {
  const lock = path.join(root, base);
  fs.mkdirSync(lock);
  fs.writeFileSync(path.join(lock, "owner"), owner, { flag: "wx" });
  return lock;
};
const ownerOf = (lock: string): string | undefined => { try { return fs.readFileSync(path.join(lock, "owner"), "utf8"); } catch { return undefined; } };
const exists = (file: string): boolean => { try { fs.lstatSync(file); return true; } catch { return false; } };

/** The request the module sends, built from the current state (for children the test drives and kills itself). */
const nativeRequest = (lock: string, owner: string, deadlineMs = 10_000) => {
  const dir = fs.lstatSync(lock, { bigint: true });
  const file = fs.lstatSync(path.join(lock, "owner"), { bigint: true });
  const bytes = Buffer.from(owner, "utf8");
  const retired = path.join(path.dirname(lock), meshRetirementName(path.basename(lock), dir.dev, dir.ino, bytes));
  const message = encodeMeshRetireRequest({
    lockPath: path.toNamespacedPath(lock), retiredPath: path.toNamespacedPath(retired), dirVolume: dir.dev, dirIndex: dir.ino,
    ownerVolume: file.dev, ownerIndex: file.ino, ownerFileTime: file.mtimeNs / 100n + 116_444_736_000_000_000n, ownerSize: file.size,
    record: bytes, deadlineMs,
  });
  return { retired, message };
};
// Every async child is registered BEFORE a wait/assertion and has a finite OS timeout. Teardown kills and joins
// pending children on failures too. No Bun parent: PATH's Node is used explicitly for all JavaScript child fixtures.
const NODE = "node";
type JoinedChild = {
  child: ChildProcessWithoutNullStreams; done: Promise<{ code: number | null; stdout: string }>; isClosed: () => boolean;
  finishBeforeTeardown?: boolean;
};
const children: JoinedChild[] = [];
const orphanHelpers = new Set<number>();
// Registered before the parent starts: teardown can recover its helper PID even if the checkpoint wait/assertion fails.
const helperPidFiles: string[] = [];
function joinedSpawn(file: string, args: string[], options: SpawnOptionsWithoutStdio = {}): JoinedChild {
  const child = spawn(file, args, { ...options, stdio: "pipe", windowsHide: true, timeout: 12_000 });
  let stdout = "";
  let closed = false;
  child.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString("utf8"); });
  child.stderr.resume();
  child.on("error", () => { /* close still follows a failed spawn; the receipt assertion reports failure */ });
  child.stdin.on("error", () => { /* an early exit may close stdin before its request is sent */ });
  const done = new Promise<{ code: number | null; stdout: string }>(resolve => child.on("close", code => {
    closed = true;
    resolve({ code, stdout });
  }));
  const entry = { child, done, isClosed: () => closed };
  children.push(entry);
  return entry;
}
// No inherited hook is forwarded: this helper strips the entire prefix, then sets only the explicit request.
const startChild = (capability: WindowsMeshCapability, message: Buffer, checkpoint?: { name: WindowsMeshCheckpoint; dir: string }) => {
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.toUpperCase().startsWith("FABRIC_WIN_MESH_TEST_")));
  const entry = joinedSpawn(capability.absolutePath, ["--mesh-retire-v1"], {
    env: { ...env, ...(checkpoint ? { FABRIC_WIN_MESH_TEST_CHECKPOINT: checkpoint.name, FABRIC_WIN_MESH_TEST_DIR: checkpoint.dir } : {}) },
  });
  entry.child.stdin.end(message);
  return entry;
};

/** Transpile the ACTUAL two-module primitive for a plain Node parent, without a loader/Bun or compiler child. */
const nodeFixtureModule = (digest: string): string => {
  const dir = tempDir();
  fs.writeFileSync(path.join(dir, "package.json"), JSON.stringify({ type: "module" }));
  for (const name of ["windows-mesh-release", "verified-remove"]) {
    const source = fs.readFileSync(path.resolve("src", "storage", `${name}.ts`), "utf8");
    const js = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText;
    const pin = name === "verified-remove" ? `const __FABRIC_NATIVE_DIGESTS__ = ${JSON.stringify(JSON.stringify({ "fabric-win-remove.exe": digest }))};\n` : "";
    fs.writeFileSync(path.join(dir, `${name}.js`), pin + js);
  }
  return pathToFileURL(path.join(dir, "windows-mesh-release.js")).href;
};
/** Bounded wait (test only) for a file to appear. */
const waitFor = async (file: string, ms = 15_000): Promise<void> => {
  for (const end = Date.now() + ms; !exists(file); ) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${file}`);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const alive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; throw error; }
};
const waitGone = async (pid: number, ms = 15_000): Promise<void> => {
  for (const end = Date.now() + ms; alive(pid); ) {
    if (Date.now() > end) throw new Error(`helper ${pid} still running`);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};

describe.skipIf(!hosted)(`Windows mesh release primitive, hosted (smarty-dev#9569; ${SKIP})`, () => {
  let root: string;
  let samples: WindowsMeshReleaseChildSample[];
  let capability: WindowsMeshCapability;
  beforeEach(() => {
    root = tempDir();
    samples = [];
    forgetWindowsMeshCapability();
    windowsMeshReleaseTestHooks.cacheRoot = path.join(cacheDir(), "cache");
    windowsMeshReleaseTestHooks.afterChild = sample => { samples.push(sample); };
    capability = prepareWindowsMeshCapability();
  });
  afterEach(async () => {
    // Join BOTH publishers even if an assertion/wait failed; never leave a paused helper or Node parent behind.
    // A parent owning a native child gets its bounded natural exit first, so even failure before its PID marker
    // is observed cannot interrupt the parent between spawn and recording that child. Direct children are killed.
    for (const entry of children) if (!entry.isClosed() && !entry.finishBeforeTeardown) entry.child.kill();
    await Promise.all(children.splice(0).map(entry => entry.done));
    for (const file of helperPidFiles.splice(0)) {
      if (!exists(file)) continue; // parent failed before spawning a helper
      const pid = Number(fs.readFileSync(file, "utf8").trim());
      if (Number.isSafeInteger(pid) && pid > 0) orphanHelpers.add(pid);
    }
    // Start all orphan joins before awaiting any: one failed join must not abandon another owned helper.
    const joined = await Promise.allSettled([...orphanHelpers].map(async pid => {
      if (alive(pid)) { try { process.kill(pid); } catch { /* exited */ } }
      await waitGone(pid, 5000);
    }));
    orphanHelpers.clear();
    const failures = joined.filter((result): result is PromiseRejectedResult => result.status === "rejected");
    if (failures.length) throw new AggregateError(failures.map(result => result.reason), "owned helpers did not terminate");
    for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
    forgetWindowsMeshCapability();
    for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
  }, 30_000);

  it("T5: materializes the exact digest in the private cache and releases with THAT executable (no fallback)", () => {
    const boot = windowsHelperBootstrap();
    expect(boot).toBeDefined();
    expect(capability).toEqual({ absolutePath: path.join(fs.realpathSync.native(windowsMeshReleaseTestHooks.cacheRoot!), boot!.digest, "fabric-win-remove.exe"), digest: boot!.digest, meshAbi: 1 });
    const owner = record("a");
    const lock = acquire(root, owner);
    const result = releaseWindowsMeshLock(lock, owner, capability);
    expect(result).toMatchObject({ outcome: "released", cleanup: "clean" });
    expect(samples).toEqual([expect.objectContaining({ absolutePath: capability.absolutePath, digest: capability.digest, outcome: "released", attempt: 1 })]);
    expect(exists(lock)).toBe(false);
    expect(exists((result as { retiredPath: string }).retiredPath)).toBe(false);
    acquire(root, record("b")); // B enters
  });

  it("T1: a delayed release after B acquired is not-owner and leaves B byte-identical", () => {
    const a = record("a"), b = record("b");
    const lock = acquire(root, a);
    expect(releaseWindowsMeshLock(lock, a, capability).outcome).toBe("released");
    acquire(root, b);
    const before = fs.lstatSync(path.join(lock, "owner"), { bigint: true });
    expect(releaseWindowsMeshLock(lock, a, capability)).toEqual({ outcome: "not-owner" });
    expect(ownerOf(lock)).toBe(b);
    expect(fs.lstatSync(path.join(lock, "owner"), { bigint: true }).ino).toBe(before.ino);
  });

  it("T1: B replacing L between the JS snapshot and the native guard is refused by exact id and full record", async () => {
    const a = record("a"), b = record("b");
    const lock = acquire(root, a);
    const { message } = nativeRequest(lock, a);
    // A's directory leaves the name (as a reclaimer would) and B publishes a new incarnation before A's helper opens.
    const aside = path.join(root, "aside");
    fs.renameSync(lock, aside);
    acquire(root, b);
    const { done } = startChild(capability, message);
    const { code, stdout } = await done;
    expect([code, stdout]).toEqual([5, "FMR1 not-owner guard directory-replaced\n"]);
    expect(ownerOf(lock)).toBe(b);
  });

  it("T1/T8: while the guard is held a by-name rename of L fails; then the release commits", async () => {
    const owner = record("a");
    const lock = acquire(root, owner);
    const gate = tempDir();
    const { retired, message } = nativeRequest(lock, owner);
    const { done } = startChild(capability, message, { name: "before-commit", dir: gate });
    await waitFor(path.join(gate, "before-commit.paused"));
    expect(() => fs.renameSync(lock, path.join(root, "stolen"))).toThrow();
    expect(ownerOf(lock)).toBe(owner);
    fs.writeFileSync(path.join(gate, "go"), "");
    expect(await done).toEqual({ code: 0, stdout: "FMR1 released cleanup clean\n" });
    expect(exists(lock) || exists(retired)).toBe(false);
  });

  for (const name of ["before-commit", "after-commit", "after-owner", "after-directory"] as const) {
    it(`T2: killing the helper at ${name} leaves a state where B is safe and A's retry is correct`, async () => {
      const a = record("a");
      const lock = acquire(root, a);
      const gate = tempDir();
      const { retired, message } = nativeRequest(lock, a);
      const { child, done } = startChild(capability, message, { name, dir: gate });
      await waitFor(path.join(gate, `${name}.paused`));
      child.kill();
      const { stdout } = await done;
      expect(stdout).toBe(""); // no terminal record: the caller cannot know the phase
      if (name === "before-commit") {
        expect(ownerOf(lock)).toBe(a); // complete canonical proof, nothing retired
        expect(exists(retired)).toBe(false);
        expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "released", cleanup: "clean" });
        return;
      }
      expect(exists(lock)).toBe(false); // committed: canonical is free whatever cleanup did
      if (name === "after-commit") expect(ownerOf(retired)).toBe(a);
      if (name === "after-owner") expect(ownerOf(retired)).toBeUndefined();
      const b = record("b");
      acquire(root, b);
      expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject(name === "after-directory"
        ? { outcome: "not-owner" } : { outcome: "released", cleanup: "clean", retiredPath: retired });
      expect(exists(retired)).toBe(false);
      expect(ownerOf(lock)).toBe(b);
    });
  }

  for (const expire of [true, false]) {
    it(`CR-A-04: immediately before retired-owner disposition, ${expire ? "an expired logical deadline preserves the owner" : "a non-expired deadline permits normal cleanup"}`, async () => {
      const a = record("a"), b = record("b");
      const lock = acquire(root, a);
      const originalOwner = fs.lstatSync(path.join(lock, "owner"), { bigint: true });
      const gate = tempDir();
      const { retired, message } = nativeRequest(lock, a);
      const { done } = startChild(capability, message, { name: "before-owner-dispose", dir: gate });
      // This checkpoint is AFTER the first W5 deadline check, sameRecord and finalPath on the held retired owner.
      // We do not wait out the watchdog: the one-shot go command only shortens the main-thread logical deadline.
      await waitFor(path.join(gate, "before-owner-dispose.paused"), 8000);
      expect(exists(lock)).toBe(false);
      expect(ownerOf(retired)).toBe(a);
      acquire(root, b); // B enters while A is paused off-canonical with its verified owner handle still held.
      const successor = fs.lstatSync(path.join(lock, "owner"), { bigint: true });
      // Publish the closed command atomically: seeing go must not race its contents being written.
      fs.writeFileSync(path.join(gate, "go.ready"), expire ? "expire-deadline\n" : "");
      fs.renameSync(path.join(gate, "go.ready"), path.join(gate, "go"));
      expect(await done).toEqual({
        code: 0, stdout: `FMR1 released cleanup ${expire ? "pending-deadline" : "clean"}\n`,
      }); // a real watchdog termination is NOT an accepted result
      if (expire) {
        // Removing only the new immediate check would still produce pending-deadline at the AFTER-owner check,
        // but would already have disposed A's owner. These disk assertions distinguish that unsafe mutation.
        expect(ownerOf(retired)).toBe(a);
        const preserved = fs.lstatSync(path.join(retired, "owner"), { bigint: true });
        expect([preserved.dev, preserved.ino]).toEqual([originalOwner.dev, originalOwner.ino]);
      } else {
        expect(exists(retired)).toBe(false); // the checkpoint must not unconditionally suppress normal cleanup
      }
      expect(ownerOf(lock)).toBe(b);
      const after = fs.lstatSync(path.join(lock, "owner"), { bigint: true });
      expect([after.dev, after.ino]).toEqual([successor.dev, successor.ino]);
    }, 20_000);
  }

  it("T2/T4: a watchdog-terminated child after commit is unknown, and the SAME framed reconciliation cleans T", () => {
    const a = record("a");
    const lock = acquire(root, a);
    windowsMeshReleaseTestHooks.checkpoint = { name: "after-commit", dir: tempDir() }; // never released: the watchdog fires
    windowsMeshReleaseTestHooks.deadlineMs = 1500;
    const result = releaseWindowsMeshLock(lock, a, capability);
    expect(result).toMatchObject({ outcome: "released", cleanup: "clean", history: expect.arrayContaining([
      expect.stringContaining("attempt 1: release-outcome-unknown"), "attempt 2: released",
    ]) });
    expect(result.history).toHaveLength(2);
    expect(samples.map(sample => sample.outcome)).toEqual(["release-outcome-unknown", "released"]);
    expect(exists((result as { retiredPath: string }).retiredPath)).toBe(false);
    expect(exists(lock)).toBe(false);
    acquire(root, record("b"));
  });

  it("T4: repeated unknown before commit stays unknown (at most two children), then a same-record retry releases", () => {
    const a = record("a");
    const lock = acquire(root, a);
    windowsMeshReleaseTestHooks.checkpoint = { name: "before-commit", dir: tempDir() };
    windowsMeshReleaseTestHooks.deadlineMs = 1500;
    expect(releaseWindowsMeshLock(lock, a, capability).outcome).toBe("release-outcome-unknown");
    expect(samples).toHaveLength(2);
    expect(ownerOf(lock)).toBe(a);
    delete windowsMeshReleaseTestHooks.checkpoint;
    expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "released", cleanup: "clean" });
  });

  for (const phase of ["before-commit", "after-commit"] as const) {
    it(`T2: parent death at ${phase} cannot leave a helper running or reach B`, async () => {
      const a = record("a"), b = record("b");
      const lock = acquire(root, a);
      // Keep the original full R and bigint object identities BEFORE framing the helper request.
      const id = (stat: fs.BigIntStats) => [stat.dev, stat.ino];
      const canonical = () => ({
        directory: id(fs.lstatSync(lock, { bigint: true })),
        owner: id(fs.lstatSync(path.join(lock, "owner"), { bigint: true })),
        record: fs.readFileSync(path.join(lock, "owner")),
        entries: fs.readdirSync(lock).sort(),
      });
      const original = canonical();
      expect(original.record).toEqual(Buffer.from(a, "utf8"));
      expect(original.entries).toEqual(["owner"]);
      const absent = (file: string) => {
        // Only ENOENT is absence; sharing/access errors must fail, not masquerade as clean state.
        expect(fs.lstatSync(file, { bigint: true, throwIfNoEntry: false }), file).toBeUndefined();
        expect(fs.readdirSync(path.dirname(file)), file).not.toContain(path.basename(file));
      };
      const gate = tempDir();
      const { retired, message } = nativeRequest(lock, original.record.toString("utf8"), 3000);
      const retirement = () => {
        const directory = fs.lstatSync(retired, { bigint: true, throwIfNoEntry: false });
        if (!directory) { absent(retired); return { state: "absent" }; }
        expect(directory.isDirectory(), retired).toBe(true);
        expect(id(directory), retired).toEqual(original.directory);
        const entries = fs.readdirSync(retired).sort(); // inaccessible/pending state is diagnostic failure, not absence
        if (entries.length === 0) return { state: "empty-original", directory: id(directory) };
        expect(entries, retired).toEqual(["owner"]); // no arbitrary entries, reparse points or replacement T
        const owner = fs.lstatSync(path.join(retired, "owner"), { bigint: true });
        expect(owner.isFile(), retired).toBe(true);
        expect(id(owner), retired).toEqual(original.owner);
        const bytes = fs.readFileSync(path.join(retired, "owner"));
        expect(bytes, retired).toEqual(original.record);
        return { state: "original-full-proof", directory: id(directory), owner: id(owner), record: bytes };
      };
      const report = (evidence: unknown) => console.info("T2 parent-death evidence", JSON.stringify(evidence,
        (_key, value: unknown) => typeof value === "bigint" ? value.toString() : value));
      const startedAt = Date.now();
      const requestFile = path.join(gate, "request.bin");
      fs.writeFileSync(requestFile, message);
      // A plain node parent that starts the helper and is then killed (no TypeScript loader, no Bun).
      const helperPidFile = path.join(gate, "helper.pid");
      helperPidFiles.push(helperPidFile);
      const parent = joinedSpawn(NODE, ["-e", [
        "const { spawn } = require('node:child_process'); const fs = require('node:fs');",
        "const child = spawn(process.argv[1], ['--mesh-retire-v1'], { stdio: ['pipe', 'ignore', 'ignore'], windowsHide: true,",
        "  env: { ...process.env, FABRIC_WIN_MESH_TEST_CHECKPOINT: process.argv[3], FABRIC_WIN_MESH_TEST_DIR: process.argv[4] } });",
        "child.on('error', () => {}); child.stdin.on('error', () => {});",
        "if (child.pid) fs.writeFileSync(process.argv[5], String(child.pid));",
        "child.stdin.end(fs.readFileSync(process.argv[2]));",
      ].join("\n"), capability.absolutePath, requestFile, phase, gate, helperPidFile]);
      parent.finishBeforeTeardown = true;
      parent.child.stdin.end();
      await waitFor(path.join(gate, `${phase}.paused`), 8000);
      const checkpointAt = Date.now();
      const helper = Number(fs.readFileSync(path.join(gate, `${phase}.paused`), "utf8").trim());
      orphanHelpers.add(helper);
      expect(Number.isSafeInteger(helper) && helper > 0).toBe(true);
      expect(Number(fs.readFileSync(helperPidFile, "utf8").trim())).toBe(helper);
      expect(alive(helper)).toBe(true);
      let successor: ReturnType<typeof canonical>;
      if (phase === "after-commit") {
        absent(lock);
        const atCheckpoint = retirement();
        expect(atCheckpoint.state).toBe("original-full-proof");
        // Publish B WHILE A's native helper is paused, before parent death or permission to continue.
        acquire(root, b);
        successor = canonical();
        expect(successor.record).toEqual(Buffer.from(b, "utf8"));
        absent(path.join(gate, "go"));
        expect(alive(helper)).toBe(true);
        report({ phase, event: "B-published-before-parent-kill", helper, checkpointAt, retirement: atCheckpoint, successor });
      }
      parent.child.kill();
      const parentReceipt = await parent.done; // actual close barrier, not just kill delivery
      const parentJoinedAt = Date.now();
      const helperAliveBeforeGo = alive(helper);
      // Never release the checkpoint: parent death + the native deadline must leave the original full-proof T
      // after W4. Otherwise this test could pass via the first orphan completing, without testing reconciliation.
      const goSent = false;
      await waitGone(helper, 5000); // native watchdog is 3 s, including forced parent death without go
      const helperGoneAt = Date.now();
      report({ phase, helper, parentPid: parent.child.pid, parentReceipt, helperAliveBeforeGo, goSent,
        startedAt, checkpointAt, parentJoinedAt, helperGoneAt, elapsedMs: helperGoneAt - startedAt,
        helperGone: !alive(helper), nativeTerminalReceipt: "unobserved (stdio ignored); no exit phase inferred", original });
      expect(parentReceipt.code).not.toBe(0);
      if (phase === "before-commit") {
        expect(canonical()).toEqual(original); // complete original canonical proof AND both object ids survive
        absent(retired);
        report({ phase, canonical: canonical(), retirement: retirement() });
        // A's explicit same-R release succeeds; do not pretend B could reap a live A.
        expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "released", cleanup: "clean" });
        absent(lock);
        absent(retired);
        acquire(root, b);
        expect(canonical().record).toEqual(Buffer.from(b, "utf8"));
      } else {
        // Keep reason: data safety + bounded cleanup liveness. No go was sent; original proof MUST survive the
        // parent's death and watchdog. A new wrapper call has no prior JS frame: it must rediscover S from T.
        const afterDeath = retirement();
        expect(afterDeath.state).toBe("original-full-proof");
        report({ phase, retirement: afterDeath, successor: canonical() });
        expect(canonical()).toEqual(successor!);
        const retry = releaseWindowsMeshLock(lock, a, capability); // release ONLY original full R, never replay work
        const afterRetry = retirement();
        report({ phase, retry, retirement: afterRetry, successor: canonical() });
        expect(retry).toMatchObject({ outcome: "released", cleanup: "clean", retiredPath: retired });
        expect(afterRetry.state).toBe("absent");
        expect(canonical()).toEqual(successor!); // full R, directory id, owner id, exact entry set
        const childCount = samples.length;
        expect(releaseWindowsMeshLock(lock, a, capability)).toEqual({ outcome: "not-owner" });
        expect(samples).toHaveLength(childCount); // repeated explicit retry: no child, no changes to B or T
        expect(retirement().state).toBe("absent");
        expect(canonical()).toEqual(successor!);
      }
    }, 25_000);
  }

  for (const phase of ["after-commit", "after-owner"] as const) {
    it(`T2/T4: SAME original native frame and a new-state same-R call clean only T after ${phase}`, async () => {
      // Two independent fixtures distinguish actual native same-frame resume from TS receipt rediscovery.
      for (const mode of ["same-frame", "new-state"] as const) {
        const ownRoot = tempDir();
        const a = record("a"), b = record("b");
        const lock = acquire(ownRoot, a);
        const { retired, message } = nativeRequest(lock, a);
        const gate = tempDir();
        const first = startChild(capability, message, { name: phase, dir: gate });
        await waitFor(path.join(gate, `${phase}.paused`));
        acquire(ownRoot, b); // B published while the old helper is still alive and paused at T
        const successor = () => ({
          dir: fs.lstatSync(lock, { bigint: true }).ino,
          owner: fs.lstatSync(path.join(lock, "owner"), { bigint: true }).ino,
          bytes: fs.readFileSync(path.join(lock, "owner")), names: fs.readdirSync(lock),
        });
        const before = successor();
        first.child.kill();
        expect((await first.done).stdout).toBe(""); // join BEFORE any cleanup attempt
        expect(ownerOf(retired)).toBe(phase === "after-commit" ? a : undefined);
        if (mode === "same-frame") {
          expect(await startChild(capability, message).done).toEqual({ code: 0, stdout: "FMR1 released cleanup clean\n" });
        } else {
          expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "released", cleanup: "clean", retiredPath: retired });
        }
        expect(exists(retired)).toBe(false);
        expect(successor()).toEqual(before);
        const count = samples.length;
        expect(releaseWindowsMeshLock(lock, a, capability)).toEqual({ outcome: "not-owner" });
        expect(samples).toHaveLength(count);
        expect(successor()).toEqual(before);
      }
    }, 25_000);
  }

  for (const variant of ["wrong-S", "foreign-owner", "full-record-differs", "torn", "extra", "reparse"] as const) {
    it(`T2/T4: retired resume protects ${variant}, never touching B or unjudged content`, async () => {
      const a = record("a"), b = record("b");
      const lock = acquire(root, a);
      const { retired, message } = nativeRequest(lock, a);
      const originalDirectory = fs.lstatSync(lock, { bigint: true });
      let resumeMessage = message;
      fs.renameSync(lock, retired); // fixture represents W4, retaining original ids/full record
      if (variant === "wrong-S" || variant === "reparse") {
        const aside = path.join(root, "original-T");
        fs.renameSync(retired, aside);
        if (variant === "reparse") fs.symlinkSync(aside, retired, "junction");
        else { fs.mkdirSync(retired); fs.writeFileSync(path.join(retired, "owner"), a); }
      } else if (variant === "foreign-owner") {
        fs.renameSync(path.join(retired, "owner"), path.join(root, "original-owner"));
        fs.writeFileSync(path.join(retired, "owner"), record("foreign"), { flag: "wx" });
      } else if (variant === "full-record-differs" || variant === "torn") {
        fs.writeFileSync(path.join(retired, "owner"), variant === "torn" ? "partial" : a.replace("a-", "z-"));
      } else fs.writeFileSync(path.join(retired, "unjudged"), "keep");
      if (variant === "full-record-differs") {
        // Match ids/time/size deliberately: only the independent FULL original R check can refuse this owner.
        const file = fs.lstatSync(path.join(retired, "owner"), { bigint: true });
        resumeMessage = encodeMeshRetireRequest({
          lockPath: path.toNamespacedPath(lock), retiredPath: path.toNamespacedPath(retired),
          dirVolume: originalDirectory.dev, dirIndex: originalDirectory.ino, ownerVolume: file.dev, ownerIndex: file.ino,
          ownerFileTime: file.mtimeNs / 100n + 116_444_736_000_000_000n, ownerSize: file.size, record: Buffer.from(a), deadlineMs: 10_000,
        });
      }
      acquire(root, b);
      const capture = () => ({
        dir: fs.lstatSync(lock, { bigint: true }).ino, owner: fs.lstatSync(path.join(lock, "owner"), { bigint: true }).ino,
        bytes: fs.readFileSync(path.join(lock, "owner")), entries: fs.readdirSync(lock),
        retiredId: fs.lstatSync(retired, { bigint: true }).ino, retiredEntries: fs.readdirSync(retired).sort(),
        retiredOwner: fs.readFileSync(path.join(retired, "owner")),
      });
      const before = capture();
      const result = await startChild(capability, resumeMessage).done;
      expect(result).toEqual(variant === "wrong-S" || variant === "reparse"
        ? { code: 3, stdout: "FMR1 refused commit destination-exists\n" }
        : { code: 0, stdout: `FMR1 released cleanup ${variant === "extra" ? "pending-extra-entries" : "pending-owner-unverified"}\n` });
      expect(capture()).toEqual(before);
      if (variant === "extra") expect(fs.readFileSync(path.join(retired, "unjudged"), "utf8")).toBe("keep");
    });
  }

  it("T2/T4: an empty-receipt frame has NO file-delete authority if an owner appears before native inspection", async () => {
    const a = record("a"), b = record("b");
    const lock = acquire(root, a);
    const dir = fs.lstatSync(lock, { bigint: true });
    const { retired } = nativeRequest(lock, a);
    fs.renameSync(lock, retired);
    const message = encodeMeshRetireRequest({
      lockPath: path.toNamespacedPath(lock), retiredPath: path.toNamespacedPath(retired), dirVolume: dir.dev, dirIndex: dir.ino,
      ownerVolume: 0n, ownerIndex: 0n, ownerFileTime: 0n, ownerSize: BigInt(Buffer.byteLength(a)), record: Buffer.from(a), deadlineMs: 10_000,
    });
    acquire(root, b);
    // This frame represents a prior empty judgment. Even a complete matching R that appeared meanwhile is kept.
    const originalOwner = fs.lstatSync(path.join(retired, "owner"), { bigint: true }).ino;
    const successorDirectory = fs.lstatSync(lock, { bigint: true }).ino;
    const successorOwner = fs.lstatSync(path.join(lock, "owner"), { bigint: true }).ino;
    expect(await startChild(capability, message).done).toEqual({ code: 0, stdout: "FMR1 released cleanup pending-owner-unverified\n" });
    expect(ownerOf(retired)).toBe(a);
    expect(fs.lstatSync(path.join(retired, "owner"), { bigint: true }).ino).toBe(originalOwner);
    expect(ownerOf(lock)).toBe(b);
    expect([fs.lstatSync(lock, { bigint: true }).ino, fs.lstatSync(path.join(lock, "owner"), { bigint: true }).ino])
      .toEqual([successorDirectory, successorOwner]);
  });

  it("T3: an extra child before retirement refuses with the owner intact (JS W1 and native W3)", async () => {
    const a = record("a");
    const lock = acquire(root, a);
    fs.writeFileSync(path.join(lock, "owner.released.0f0f"), "debris");
    expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "refused-before-retirement" });
    expect(ownerOf(lock)).toBe(a);
    // Arriving after the JS snapshot: the native held-handle entry check refuses too.
    const other = path.join(tempDir(), "x");
    fs.mkdirSync(other);
    const lock2 = acquire(other, a);
    const { message } = nativeRequest(lock2, a);
    fs.writeFileSync(path.join(lock2, "raw"), "unknown");
    expect(await startChild(capability, message).done).toEqual({ code: 3, stdout: "FMR1 refused owner unexpected-entries\n" });
    expect(ownerOf(lock2)).toBe(a);
  });

  it("T3: a late child in the retired directory survives, cleanup is pending, and B acquires", async () => {
    const a = record("a");
    const lock = acquire(root, a);
    const gate = tempDir();
    const { retired, message } = nativeRequest(lock, a);
    const { done } = startChild(capability, message, { name: "after-commit", dir: gate });
    await waitFor(path.join(gate, "after-commit.paused"));
    fs.writeFileSync(path.join(retired, "late"), "unjudged");
    fs.writeFileSync(path.join(gate, "go"), "");
    expect(await done).toEqual({ code: 0, stdout: "FMR1 released cleanup pending-extra-entries\n" });
    expect(fs.readFileSync(path.join(retired, "late"), "utf8")).toBe("unjudged");
    expect(ownerOf(retired)).toBe(a); // kept as the retired object's proof
    acquire(root, record("b"));
  });

  it("T6 (A slice): sibling release debris never blocks a free canonical lock and is never touched", () => {
    const released = path.join(root, ".lock.released.11111111-2222-3333-4444-555555555555");
    const legacy = path.join(root, `r.${"f".repeat(64)}`);
    for (const dir of [released, legacy]) { fs.mkdirSync(dir); fs.writeFileSync(path.join(dir, "raw"), "keep"); }
    const a = record("a");
    const lock = acquire(root, a);
    expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "released", cleanup: "clean" });
    acquire(root, record("b"));
    for (const dir of [released, legacy]) expect(fs.readFileSync(path.join(dir, "raw"), "utf8")).toBe("keep");
  });

  it("T9: an occupied retirement destination is never replaced (JS and native), with its sentinel intact", async () => {
    const a = record("a");
    const lock = acquire(root, a);
    const { retired, message } = nativeRequest(lock, a);
    fs.mkdirSync(retired);
    fs.writeFileSync(path.join(retired, "sentinel"), "mine");
    expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "refused-before-retirement" });
    expect(await startChild(capability, message).done).toEqual({ code: 3, stdout: "FMR1 refused commit destination-exists\n" });
    expect(fs.readFileSync(path.join(retired, "sentinel"), "utf8")).toBe("mine");
    expect(ownerOf(lock)).toBe(a);
  });

  it("T9: at an exactly 150-unit root with a non-BMP component the retired owner is 223 logical units", () => {
    const base = tempDir();
    const pad = 150 - base.length - 1 - 2; // separator, then padding, then one surrogate pair
    expect(pad).toBeGreaterThan(0);
    const longRoot = path.join(base, `${"p".repeat(pad)}\u{1F600}`);
    expect(longRoot.length).toBe(150);
    fs.mkdirSync(longRoot);
    const a = record("a");
    const lock = acquire(longRoot, a);
    const result = releaseWindowsMeshLock(lock, a, capability);
    expect(result).toMatchObject({ outcome: "released", cleanup: "clean" });
    const retiredOwner = path.join((result as { retiredPath: string }).retiredPath, "owner");
    expect(windowsLogicalPathUnits(path.toNamespacedPath(retiredOwner))).toBe(223);
    expect(windowsApiPathUnits(path.toNamespacedPath(retiredOwner))).toBe(longRoot.startsWith("\\\\") ? 229 : 227);
    acquire(longRoot, record("b"));
  });

  it("T5: the package dist removed during a held section; release uses the captured cache executable", () => {
    forgetWindowsMeshCapability();
    const boot = windowsHelperBootstrap()!;
    const dist = path.join(tempDir(), "pkg", "dist");
    fs.mkdirSync(path.join(dist, "native"), { recursive: true });
    fs.writeFileSync(path.join(dist, "native", "fabric-win-remove.exe"), boot.bytes);
    windowsMeshReleaseTestHooks.bootstrapCandidates = [path.join(dist, "native", "fabric-win-remove.exe")];
    windowsMeshReleaseTestHooks.cacheRoot = path.join(cacheDir(), "cache");
    const held = prepareWindowsMeshCapability();
    const a = record("a");
    const lock = acquire(root, a);
    // Execute the EXACT node -e cleanup from the real package build, but against our copied package root.
    // Do not invoke the full build or remove the repository's shared dist. A plain Node child owns this cleanup.
    const packageFile = path.resolve("package.json");
    const copiedPackage = path.join(path.dirname(dist), "package.json");
    fs.copyFileSync(packageFile, copiedPackage);
    const build = (JSON.parse(fs.readFileSync(copiedPackage, "utf8")) as { scripts: { build: string } }).scripts.build;
    const cleanup = /(?:^|&&)\s*node -e "([^"]+)"\s*(?:&&|$)/.exec(build);
    expect(cleanup, "the actual build must still expose its Node cleanup boundary").not.toBeNull();
    const cleaned = spawnSync(NODE, ["-e", cleanup![1]!], { cwd: path.dirname(dist), encoding: "utf8", windowsHide: true, timeout: 10_000 });
    expect({ status: cleaned.status, error: cleaned.error, stderr: cleaned.stderr }).toEqual({ status: 0, error: undefined, stderr: "" });
    expect(exists(dist)).toBe(false);
    expect(createHash("sha256").update(fs.readFileSync(held.absolutePath)).digest("hex")).toBe(boot.digest);
    expect(releaseWindowsMeshLock(lock, a, held)).toMatchObject({ outcome: "released", cleanup: "clean" });
    expect(samples).toEqual([expect.objectContaining({ absolutePath: held.absolutePath, digest: boot.digest, outcome: "released", attempt: 1 })]);
    acquire(root, record("b"));
    // The running process keeps its established capability; a cold start without a trusted bootstrap fails closed.
    expect(prepareWindowsMeshCapability()).toBe(held);
    forgetWindowsMeshCapability();
    expect(() => prepareWindowsMeshCapability()).toThrow(/no trusted packaged Windows helper/);
  });

  it("T5 extension: kill a plain Node parent after actual closed staging and BEFORE publication; next prepare ignores it", async () => {
    forgetWindowsMeshCapability();
    const boot = windowsHelperBootstrap()!;
    const cache = cacheDir();
    const gate = tempDir();
    const stagedMarker = path.join(gate, "staged.json");
    const module = nodeFixtureModule(boot.digest);
    const parent = joinedSpawn(NODE, ["--input-type=module", "-e", [
      "import fs from 'node:fs';",
      "if (process.versions.bun) throw new Error('plain Node required');",
      "const { prepareWindowsMeshCapability, windowsMeshReleaseTestHooks: hooks } = await import(process.argv[1]);",
      "hooks.bootstrapCandidates = [process.argv[2]]; hooks.cacheRoot = process.argv[3];",
      // The observer runs in-process only, exactly after the close+rehash and before runVerified(publish).
      // Atomics.wait is a test-only bounded pause, not a production timer/poller or an inherited environment hook.
      "hooks.afterCacheStaged = staging => {",
      "  fs.writeFileSync(process.argv[4], JSON.stringify({ staging, pid: process.pid }));",
      "  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10000);",
      "  throw new Error('staging parent was not killed within its bounded observation window');",
      "};",
      "prepareWindowsMeshCapability(); throw new Error('unexpected publication');",
    ].join("\n"), module, boot.file, cache, stagedMarker]);
    parent.child.stdin.end();
    await waitFor(stagedMarker, 8000);
    const observed = JSON.parse(fs.readFileSync(stagedMarker, "utf8")) as { staging: string; pid: number };
    expect(observed.pid).toBe(parent.child.pid);
    expect(path.dirname(observed.staging)).toBe(cache);
    expect(path.basename(observed.staging)).toMatch(/^\.staging-/);
    expect(exists(path.join(cache, boot.digest))).toBe(false);
    expect(fs.readdirSync(root)).toEqual([]); // no canonical directory/owner existed at this capability boundary
    expect(fs.readFileSync(path.join(observed.staging, "fabric-win-remove.exe"))).toEqual(boot.bytes);
    parent.child.kill();
    const ended = await parent.done;
    expect(ended.code).not.toBe(0);
    expect(exists(path.join(cache, boot.digest))).toBe(false);
    expect(fs.readdirSync(root)).toEqual([]);
    windowsMeshReleaseTestHooks.cacheRoot = cache;
    windowsMeshReleaseTestHooks.bootstrapCandidates = [boot.file];
    const cap = prepareWindowsMeshCapability();
    expect(cap.absolutePath).toBe(path.join(cache, boot.digest, "fabric-win-remove.exe"));
    expect(fs.readFileSync(path.join(observed.staging, "fabric-win-remove.exe"))).toEqual(boot.bytes); // ignored, never promoted
    expect(fs.readdirSync(root)).toEqual([]);
  });

  it("T5 extension: two actual publisher children race, both join, exactly one wins without replacing any digest", async () => {
    forgetWindowsMeshCapability();
    const boot = windowsHelperBootstrap()!;
    const cache = cacheDir();
    windowsMeshReleaseTestHooks.cacheRoot = cache;
    const old = path.join(cache, "0".repeat(64));
    fs.mkdirSync(old);
    fs.writeFileSync(path.join(old, "fabric-win-remove.exe"), "old build");
    const dest = path.join(cache, boot.digest);
    const stagings = [1, 2].map(n => {
      const dir = path.join(cache, `.staging-race-${n}`);
      fs.mkdirSync(dir);
      fs.writeFileSync(path.join(dir, "fabric-win-remove.exe"), boot.bytes);
      fs.writeFileSync(path.join(dir, "who"), String(n));
      return dir;
    });
    // Start BOTH real native publisher children before awaiting either. No sequential spawnSync and no Bun.
    const publishers = stagings.map(staging => {
      const entry = joinedSpawn(boot.file, ["--cache-publish-v1", path.toNamespacedPath(staging), path.toNamespacedPath(dest)]);
      entry.child.stdin.end();
      return entry;
    });
    const results = await Promise.all(publishers.map(entry => entry.done)); // both joined before assertions
    expect(results.map(result => result.code).sort()).toEqual([0, 6]);
    const winner = results.findIndex(result => result.code === 0);
    const loser = 1 - winner;
    // cachePublish uses WinCRT printf (CRLF); meshOut's strict WriteFile LF result parser stays unchanged.
    expect(results[winner]!.stdout.replace(/\r\n/g, "\n")).toBe("ok published\n");
    expect(results[loser]!.stdout.replace(/\r\n/g, "\n")).toBe("exists 0 published\n");
    expect(fs.readFileSync(path.join(dest, "who"), "utf8")).toBe(String(winner + 1));
    expect(exists(stagings[winner]!)).toBe(false);
    expect(fs.readFileSync(path.join(stagings[loser]!, "who"), "utf8")).toBe(String(loser + 1));
    const published = fs.lstatSync(dest, { bigint: true });
    const cap = prepareWindowsMeshCapability();
    expect(cap.absolutePath).toBe(path.join(dest, "fabric-win-remove.exe"));
    expect(fs.lstatSync(dest, { bigint: true }).ino).toBe(published.ino);
    expect(fs.readFileSync(path.join(dest, "who"), "utf8")).toBe(String(winner + 1));
    expect(fs.readFileSync(path.join(old, "fabric-win-remove.exe"), "utf8")).toBe("old build");
  });

  it("T5: a tampered published digest is refused, never overwritten", () => {
    forgetWindowsMeshCapability();
    const boot = windowsHelperBootstrap()!;
    const cache = cacheDir();
    windowsMeshReleaseTestHooks.cacheRoot = cache;
    fs.mkdirSync(path.join(cache, boot.digest));
    fs.writeFileSync(path.join(cache, boot.digest, "fabric-win-remove.exe"), "tampered");
    expect(() => prepareWindowsMeshCapability()).toThrow(/never overwritten/);
    expect(fs.readFileSync(path.join(cache, boot.digest, "fabric-win-remove.exe"), "utf8")).toBe("tampered");
  });

  it("T5: tampering the captured executable after entry fails closed; restoring the exact bytes lets the same record release", () => {
    const a = record("a");
    const lock = acquire(root, a);
    const bytes = fs.readFileSync(capability.absolutePath);
    fs.writeFileSync(capability.absolutePath, "tampered");
    expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "helper-unavailable/not-started" });
    expect(samples).toHaveLength(0); // nothing was started
    expect(ownerOf(lock)).toBe(a);
    fs.writeFileSync(capability.absolutePath, bytes);
    expect(releaseWindowsMeshLock(lock, a, capability)).toMatchObject({ outcome: "released", cleanup: "clean" });
  });
});
