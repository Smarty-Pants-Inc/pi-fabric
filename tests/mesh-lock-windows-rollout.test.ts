import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";

// expected-skip: outside the registered hosted Windows baseline runner: needs its actual built pinned OLD dist.
// Keep reason (#9569 T11): data safety/trust boundary. This MUST NOT run locally, even when selecting it directly.
// The verified-remove VALUE import also excludes collection via the unchanged product-deletion detector.
// No old-release lookalike, source transpilation, return injection, platform override, or fabricated process receipt.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true" && process.env.FABRIC_9569_ROLLOUT === "1";
const pinnedBase = "dcc0825cb005ebfc37c37514f159cdb3570281f4";
const fixture = fileURLToPath(new URL("./fixtures/mesh-windows-rollout.mjs", import.meta.url));
const hash = (file: string): string => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
interface Snapshot {
  directory: { dev: string; ino: string; size: string; mtimeNs: string };
  owner: { dev: string; ino: string; size: string; mtimeNs: string };
  record: string;
}
interface Sample { absolutePath: string; digest: string; observedHelperSha256: string; outcome: string; attempt: number }
interface Result {
  pid: number; role: string; project: string; status: string; entered: number; entry?: Snapshot; error?: string;
  samples: Sample[];
  nativeTerminals: { executable: string; pid?: number; code: number | null; signal: string | null; mode: string; joinedAtUtc: string }[];
  artifacts: { meshSha256: string; packagedHelperSha256: string; nodeSha256: string; nodeVersion: string };
}
interface Terminal { pid: number | undefined; code: number | null; signal: NodeJS.Signals | null; error: string | undefined; stderr: string; joinedAtUtc: string }
interface Owned {
  child: ChildProcessWithoutNullStreams; control: string; project: string; old: boolean; closed: boolean;
  terminal?: Terminal; done: Promise<Terminal>;
}
const children: Owned[] = [];
let ownedRoot: string | undefined;
let activeProtocol: number | undefined;
const json = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const state = (root: string): Snapshot => {
  const identity = (file: string) => {
    const s = fs.lstatSync(file, { bigint: true });
    return { dev: String(s.dev), ino: String(s.ino), size: String(s.size), mtimeNs: String(s.mtimeNs) };
  };
  const lock = path.join(root, ".lock");
  return { directory: identity(lock), owner: identity(path.join(lock, "owner")), record: fs.readFileSync(path.join(lock, "owner"), "utf8") };
};
const go = (entry: Owned): void => { fs.writeFileSync(path.join(entry.control, "go"), "continue\n"); };
// fs.watch is the normal checkpoint event source. The one-shot deadline catches a missed event or a failed child;
// there is no Main polling or process-aliveness probe. A file is not a terminal receipt: only close sets closed.
const checkpoint = async <T>(entry: Owned, name: string): Promise<T> => {
  const file = path.join(entry.control, `${name}.json`);
  await new Promise<void>((resolve, reject) => {
    let finished = false;
    const finish = (error?: Error) => {
      if (finished) return;
      finished = true; clearTimeout(timer); watcher.close();
      if (error) reject(error); else resolve();
    };
    const inspect = () => { if (fs.existsSync(file)) finish(); };
    const watcher = fs.watch(entry.control, inspect);
    const timer = setTimeout(() => finish(fs.existsSync(file) ? undefined : new Error(`T11 checkpoint deadline: ${file}`)), 35_000);
    watcher.on("error", error => finish(error));
    void entry.done.then(receipt => {
      if (fs.existsSync(file)) finish(); else finish(new Error(`child closed before ${name}: ${JSON.stringify(receipt)}`));
    });
    inspect();
  });
  return json<T>(file);
};
const start = (project: string, root: string, role: string, protocol: number, old: boolean): Owned => {
  if (!ownedRoot) throw new Error("missing owned fixture root");
  const control = fs.mkdtempSync(path.join(ownedRoot, "child-"));
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.toUpperCase().startsWith("FABRIC_WIN_MESH_TEST_")));
  // PATH's Node, never Bun or a TS loader. A finite outer timeout is a failure, never successful drain evidence.
  const child = spawn("node", [fixture, project, root, control, role, String(protocol)], {
    env: { ...env, FABRIC_9569_ROLLOUT_CHILD_CURRENT: String(!old), PI_FABRIC_LOCK_STATS: "0" },
    stdio: "pipe", windowsHide: true, timeout: 90_000,
  });
  let stderr = "", error: string | undefined;
  child.stdout.resume();
  child.stderr.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-16_384); });
  child.on("error", caught => { error = caught.message; });
  child.stdin.on("error", () => { /* an early child failure is reported by its terminal receipt */ });
  let entry: Owned;
  const done = new Promise<Terminal>(resolve => child.once("close", (code, signal) => {
    const receipt = { pid: child.pid, code, signal, error, stderr, joinedAtUtc: new Date().toISOString() };
    entry.closed = true; entry.terminal = receipt; resolve(receipt);
  }));
  entry = { child, control, project, old, closed: false, done };
  children.push(entry); // register before any asynchronous wait/assertion
  child.stdin.end();
  return entry;
};
const join = async (entry: Owned, status: string): Promise<Result> => {
  const terminal = await entry.done;
  expect(terminal, `actual process close: ${JSON.stringify(terminal)}`).toMatchObject({ code: 0, signal: null, error: undefined });
  const result = json<Result>(path.join(entry.control, "result.json"));
  expect(result).toMatchObject({ pid: terminal.pid, project: entry.project, status });
  expect(result.error).toBeUndefined();
  return result;
};

describe.skipIf(!hosted)("T11 Windows owned-cohort rollout gate (hosted-only; not a real-root inventory oracle)", () => {
  afterEach(async () => {
    // Failure path: release EVERY owned Node checkpoint first, including checkpoints not reached yet. Then join
    // ALL children concurrently (never fail-fast). Do not kill a parent while its synchronous native child runs;
    // the product wrapper owns finite native timeouts/joins. Outer Node timeout is 90s and fails the receipt.
    for (const entry of children) if (!entry.closed) go(entry);
    const terminal = await Promise.all(children.map(entry => entry.done));
    const abnormal = terminal.filter(receipt => receipt.signal || receipt.error || receipt.code !== 0);
    const terminalReceipt = {
      schema: "fabric-windows-mesh-rollout-terminal/1", protocol: activeProtocol, fixtureRoot: ownedRoot,
      processes: children.map(entry => {
        let result: unknown;
        try { result = json(path.join(entry.control, "result.json")); } catch (error) { result = { unreadableResult: String(error) }; }
        return { old: entry.old, project: entry.project, control: entry.control, terminal: entry.terminal, result };
      }),
    };
    // Preserve actual terminal receipts on assertion/checkpoint failure as well as on the green path.
    console.log(`WINDOWS_MESH_ROLLOUT_TERMINAL: ${JSON.stringify(terminalReceipt)}`);
    if (activeProtocol && process.env.FABRIC_9569_ROLLOUT_RESULTS) {
      fs.writeFileSync(path.join(process.env.FABRIC_9569_ROLLOUT_RESULTS, `t11-p${activeProtocol}-terminal.json`), `${JSON.stringify(terminalReceipt, null, 2)}\n`);
    }
    children.length = 0; activeProtocol = undefined;
    if (abnormal.length) {
      // Keep owned fixtures for hosted forensics instead of deleting beneath any ambiguous child outcome.
      const kept = ownedRoot; ownedRoot = undefined;
      throw new Error(`T11 abnormal terminal receipt(s); fixtures retained at ${kept}: ${JSON.stringify(abnormal)}`);
    }
    if (ownedRoot) fs.rmSync(ownedRoot, { recursive: true, force: true });
    ownedRoot = undefined;
  }, 100_000);

  for (const protocol of [1, 2]) it(`T11 protocol ${protocol}: pinned OLD can delete NEW B; only terminal old-cohort drain admits the all-new canary`, async () => {
    activeProtocol = protocol;
    // These inputs are mandatory when registered by FABRIC_9569_ROLLOUT=1, NOT another silent skip. The baseline runner builds
    // this exact git archive, then supplies its unchanged dist and candidate dist before invoking T11/T12.
    expect(process.env.FABRIC_9569_ROLLOUT_BASE_HEAD).toBe(pinnedBase);
    const baseline = fs.realpathSync.native(process.env.FABRIC_9569_ROLLOUT_BASE_PROJECT!);
    const candidate = fs.realpathSync.native(process.env.FABRIC_9569_ROLLOUT_CURRENT_PROJECT!);
    expect(baseline).not.toBe(candidate);
    expect(process.env.FABRIC_9569_ROLLOUT_CURRENT_HEAD).toMatch(/^[a-f0-9]{40}$/);
    const candidateDigest = hash(path.join(candidate, "dist/native/fabric-win-remove.exe"));
    // Independent packaged bootstrap check; all actual canary release calls below still use the BUILT public API.
    expect(windowsHelperBootstrap()?.digest).toBe(candidateDigest);
    expect(fs.existsSync(path.join(baseline, "dist/storage/windows-mesh-release.js"))).toBe(false);
    ownedRoot = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "f11-")));
    const root = path.join(ownedRoot, "root"); fs.mkdirSync(root);
    const rootId = fs.lstatSync(root, { bigint: true });
    const oldCohort: Owned[] = [];
    let inventorySealed = false;
    const admitted: string[] = [];
    const launchUpdated = (role: string): Owned => {
      if (!inventorySealed || oldCohort.some(entry => !entry.closed)) throw new Error("T11 gate closed: old cohort has not terminally drained");
      // Only actual successful terminal receipts admit this deterministic fixture inventory. No filesystem marker,
      // empty lock, PID scan, helper capability or new build can stand in for old-process completion.
      if (oldCohort.some(entry => !entry.terminal || entry.terminal.code !== 0 || entry.terminal.signal || entry.terminal.error)) {
        throw new Error("T11 gate closed: old cohort terminal failure");
      }
      admitted.push(new Date().toISOString());
      return start(candidate, root, role, protocol, false);
    };
    const oldA = start(baseline, root, "old-release", protocol, true); oldCohort.push(oldA);
    const oldPause = await checkpoint<{ pid: number; boundary: string; snapshot: Snapshot }>(oldA, "release-paused");
    expect(oldPause.boundary).toBe("old-after-real-check-before-path-rename");
    expect(oldPause.snapshot.record.split("\n")[1]).toBe(String(oldA.child.pid));
    // Include a real OLD public waiter, not just the holder, in the complete controller-owned cohort.
    const oldWaiter = start(baseline, root, "contender", protocol, true); oldCohort.push(oldWaiter);
    inventorySealed = true; // seals only THIS owned inventory, never claims to discover unknown real-root processes
    await checkpoint(oldWaiter, "started");
    expect(() => launchUpdated("current-release")).toThrow("gate closed");
    const waited = await join(oldWaiter, "excluded");
    expect(waited.entered).toBe(0);
    expect(oldA.closed).toBe(false);
    expect(() => launchUpdated("current-release")).toThrow("gate closed");
    expect(admitted).toEqual([]);

    // Deliberate gate BYPASS, confined to our disposable root: force A aside while paused after its real check,
    // then enter B using NEW public code. This is test revocation, NOT a production live-owner death judgment.
    fs.renameSync(path.join(root, ".lock"), path.join(root, "forced-old"));
    const negativeB = start(candidate, root, "holder", protocol, false);
    const negativeHeld = await checkpoint<{ snapshot: Snapshot }>(negativeB, "holding");
    expect(state(root)).toEqual(negativeHeld.snapshot);
    expect(negativeHeld.snapshot.record).not.toBe(oldPause.snapshot.record);
    expect(() => launchUpdated("current-release")).toThrow("gate closed");
    go(oldA);
    const oldResult = await join(oldA, "completed");
    // The REAL pinned old rename+cleanup destroys B's canonical owner. New B alone does NOT make overlap safe.
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(fs.readFileSync(path.join(root, "forced-old", "owner"), "utf8")).toBe(oldPause.snapshot.record);
    go(negativeB);
    const negativeResult = await join(negativeB, "completed");
    expect(oldCohort.every(entry => entry.closed && entry.terminal?.code === 0)).toBe(true);
    expect(oldResult.artifacts.meshSha256).toBe(hash(path.join(baseline, "dist/mesh.js")));
    expect(negativeResult.artifacts.meshSha256).toBe(hash(path.join(candidate, "dist/mesh.js")));

    // NOW, and not before actual close of the entire OLD cohort, admit the all-new public paused-release canary.
    const currentA = launchUpdated("current-release");
    const currentPause = await checkpoint<{ boundary: string; snapshot: Snapshot; helper: string }>(currentA, "release-paused");
    expect(currentPause.boundary).toBe("current-after-real-check-before-native-guard");
    expect(hash(currentPause.helper)).toBe(candidateDigest);
    fs.renameSync(path.join(root, ".lock"), path.join(root, "forced-current"));
    const currentB = launchUpdated("holder");
    const held = await checkpoint<{ snapshot: Snapshot }>(currentB, "holding");
    expect(held.snapshot.record).not.toBe(currentPause.snapshot.record);
    const before = state(root);
    go(currentA);
    const currentResult = await join(currentA, "completed");
    expect(state(root)).toEqual(before); // full bytes and exact bigint identity/time/size survive stale A
    expect(currentResult.samples.map(sample => sample.outcome)).toEqual(["not-owner"]); // actual native refusal
    expect(currentResult.nativeTerminals).toEqual(expect.arrayContaining([expect.objectContaining({
      executable: currentPause.helper, mode: "sync-joined", code: 5, signal: null,
    })]));
    const currentC = launchUpdated("contender");
    expect((await join(currentC, "excluded")).entered).toBe(0);
    expect(state(root)).toEqual(before);
    go(currentB);
    const holderResult = await join(currentB, "completed");
    expect(holderResult.samples.map(sample => sample.outcome)).toEqual(["released"]);
    expect(holderResult.nativeTerminals).toEqual(expect.arrayContaining([expect.objectContaining({
      executable: holderResult.samples[0]!.absolutePath, mode: "sync-joined", code: 0, signal: null,
    })]));
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    // C can now enter by the same public API; no recursive fixture cleanup supplies acquisition success.
    const successor = launchUpdated("holder");
    await checkpoint(successor, "holding"); go(successor);
    const successorResult = await join(successor, "completed");
    expect(successorResult.entered).toBe(1);
    expect(successorResult.samples.map(sample => sample.outcome)).toEqual(["released"]);
    for (const entry of [currentA, currentB, successor]) {
      const result = json<Result>(path.join(entry.control, "result.json"));
      expect(result.artifacts.packagedHelperSha256).toBe(candidateDigest);
      for (const sample of result.samples) {
        expect(sample).toMatchObject({ digest: candidateDigest, observedHelperSha256: candidateDigest, attempt: 1 });
        expect(sample.absolutePath).toBe(path.join(entry.control, "cache", candidateDigest, "fabric-win-remove.exe"));
      }
    }
    const receipt = {
      schema: "fabric-windows-mesh-rollout/1", protocol, baselineHead: pinnedBase,
      candidateHead: process.env.FABRIC_9569_ROLLOUT_CURRENT_HEAD,
      fixtureRoot: root, rootIdentity: { dev: String(rootId.dev), ino: String(rootId.ino) },
      negative: { bypassedGate: true, oldCheckpoint: oldPause, newB: negativeHeld.snapshot, canonicalDestroyedByActualOldRelease: true },
      inventoryScope: "complete owned fixture cohort ONLY; not an oracle for unknown production writers/launchers",
      oldCohort: oldCohort.map(entry => ({ terminal: entry.terminal, result: json<Result>(path.join(entry.control, "result.json")) })),
      admittedAtUtc: admitted, canary: { checkpoint: currentPause, successorPreserved: before,
        nativeStaleRelease: currentResult, holderRelease: holderResult, nextAcquisition: successorResult },
      allOwnedProcesses: children.map(entry => ({ old: entry.old, terminal: entry.terminal })),
      fixtureSha256: hash(fixture), recordedAtUtc: new Date().toISOString(),
    };
    expect(children.every(entry => entry.closed)).toBe(true);
    if (process.env.FABRIC_9569_ROLLOUT_RESULTS) {
      fs.writeFileSync(path.join(process.env.FABRIC_9569_ROLLOUT_RESULTS, `t11-p${protocol}.json`), `${JSON.stringify(receipt, null, 2)}\n`);
    }
    console.log(`WINDOWS_MESH_ROLLOUT: ${JSON.stringify(receipt)}`);
  }, 180_000);
});
