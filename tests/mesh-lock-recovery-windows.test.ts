import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { windowsMeshCacheFixture } from "./windows-mesh-cache-fixture.js";
// Deliberate VALUE import: unchanged local deletion detector excludes this REAL native Windows suite.
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import {
  forgetWindowsMeshCapability, prepareWindowsMeshCapability, windowsMeshReleaseTestHooks,
  windowsApiPathUnits, windowsLogicalPathUnits, type WindowsMeshCapability,
} from "../src/storage/windows-mesh-release.js";
import {
  encodeMeshDebrisRecoveryRequest, encodeMeshOwnerRecoveryRequest, interpretMeshRecoveryResult,
  removeWindowsMeshDebris, retireWindowsMeshOwner, windowsMeshRecoveryFence, windowsMeshRecoveryTestHooks,
  type WindowsMeshRecoveryEntry, type WindowsMeshRecoveryCheckpoint,
} from "../src/storage/windows-mesh-recovery.js";

// Keep reason: C/D cross-owner destructive trust boundary and crash partition, smarty-dev#9569 T6/T7/T8/T9.
// Real NTFS handles/deletions and actual kill+terminal joins; NO mocked native result is platform evidence.
// Caller policy/raw/torn/live/unproven classification belongs to B-policy's tests, not native permission inference.
// Expected platform skip is not Windows evidence. No Bun/.ts/src-worker fixtures, and no grandchildren.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const SKIP = "skip reason: actual NTFS guarded C/D native transactions require hosted Windows and the pinned helper";
const dirs: string[] = [];
const temp = (): string => {
  const dir = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fmc-")));
  dirs.push(dir);
  return dir;
};
const exists = (file: string): boolean => { try { fs.lstatSync(file); return true; } catch { return false; } };
const read = (file: string): string => fs.readFileSync(file, "utf8");
const identity = (file: string): string => { const stat = fs.lstatSync(file, { bigint: true }); return `${stat.dev}:${stat.ino}`; };
const publish = (root: string, record: string, base = ".lock"): string => {
  const lock = path.join(root, base);
  fs.mkdirSync(lock);
  fs.writeFileSync(path.join(lock, "owner"), record, { flag: "wx" });
  return lock;
};
const snapshot = (lock: string) => ({ directory: fs.lstatSync(lock, { bigint: true }), owner: fs.lstatSync(path.join(lock, "owner"), { bigint: true }) });
type Receipt = { code: number | null; signal: NodeJS.Signals | null; stdout: string };
type Joined = { child: ChildProcessWithoutNullStreams; done: Promise<Receipt>; closed: () => boolean };
const children: Joined[] = [];
/** Registered before any assertion/wait; finite OS timeout, all failures still terminally joined in teardown. */
const joinedSpawn = (file: string, args: string[], env?: NodeJS.ProcessEnv): Joined => {
  const child = spawn(file, args, { stdio: "pipe", windowsHide: true, timeout: 12_000, ...(env ? { env } : {}) });
  let stdout = "", closed = false;
  child.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString("utf8"); if (stdout.length > 8192) child.kill(); });
  child.stderr.resume();
  child.stdin.on("error", () => { /* early child exit; receipt is authoritative */ });
  child.on("error", () => { /* Node emits close after failed spawn too */ });
  const done = new Promise<Receipt>(resolve => child.on("close", (code, signal) => { closed = true; resolve({ code, signal, stdout }); }));
  const entry = { child, done, closed: () => closed };
  children.push(entry);
  return entry;
};
const start = (cap: WindowsMeshCapability, mode: "owner" | "debris", request: Buffer, checkpoint?: { name: WindowsMeshRecoveryCheckpoint; dir: string }): Joined => {
  const env: NodeJS.ProcessEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.toUpperCase().startsWith("FABRIC_WIN_MESH_TEST_")));
  if (checkpoint) { env.FABRIC_WIN_MESH_TEST_CHECKPOINT = checkpoint.name; env.FABRIC_WIN_MESH_TEST_DIR = checkpoint.dir; }
  const entry = joinedSpawn(cap.absolutePath, [mode === "owner" ? "--mesh-recover-owner-v1" : "--mesh-recover-debris-v1"], env);
  entry.child.stdin.end(request);
  return entry;
};
/** Bounded test-only checkpoint wait. Production uses the native event/timer watchdog, not polling. */
const paused = async (gate: string, name: string): Promise<void> => {
  const file = path.join(gate, `${name}.paused`);
  for (const end = Date.now() + 8000; !exists(file); ) {
    if (Date.now() >= end) throw new Error(`checkpoint not reached: ${name}`);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const proceed = (gate: string): void => { fs.writeFileSync(path.join(gate, "go"), ""); };
const interpreted = (receipt: Receipt, mode: "owner" | "debris", pid?: number) =>
  interpretMeshRecoveryResult({ status: receipt.code, signal: receipt.signal, stdout: receipt.stdout, pid }, mode);

/** A REAL terminally joined Node child supplies dead PID evidence. Each surviving file has a separate full receipt;
 * no file's native authority relies on a deleted neighbor. This exercises the native boundary, not caller policy. */
const deadRecords = async (): Promise<string[]> => {
  const fixture = joinedSpawn("node", ["-e", "process.stdout.write(String(process.pid))"]);
  fixture.child.stdin.end();
  const receipt = await fixture.done;
  expect(receipt.code).toBe(0);
  const pid = Number(receipt.stdout);
  expect(Number.isSafeInteger(pid) && pid > 0).toBe(true);
  return Array.from({ length: 3 }, () => `${randomUUID()}\n${pid}\n${Date.now()}\n`);
};
const deadTree = (root: string, records: readonly string[]): { lock: string; items: WindowsMeshRecoveryEntry[] } => {
  const lock = path.join(root, ".lock");
  const first = path.join(lock, "r.first"), inner = path.join(first, "r.inner");
  fs.mkdirSync(inner, { recursive: true });
  const files = [path.join(first, "owner"), path.join(lock, `owner.released.${randomUUID()}`), path.join(inner, "owner")];
  files.forEach((file, i) => fs.writeFileSync(file, records[i]!, { flag: "wx" }));
  // Deliberately OWNER FIRST, then another independent proof, deeper proof, child-first empty dirs, top L LAST.
  const items = [...files.map((file, i) => ({ path: file, stat: fs.lstatSync(file, { bigint: true }), record: records[i]! })),
    ...[inner, first, lock].map(file => ({ path: file, stat: fs.lstatSync(file, { bigint: true }) }))];
  return { lock, items };
};
const rejudgeSurvivors = (items: readonly WindowsMeshRecoveryEntry[]): WindowsMeshRecoveryEntry[] => items.filter(item => exists(item.path)).map(item => ({
  path: item.path, stat: fs.lstatSync(item.path, { bigint: true }), ...(item.record === undefined ? {} : { record: read(item.path) }),
}));

describe.skipIf(!hosted)(`native Windows mesh C/D recovery (${SKIP})`, () => {
  let root: string, cap: WindowsMeshCapability;
  beforeEach(() => {
    root = temp();
    expect(windowsHelperBootstrap()).toBeDefined(); // value import intentionally retained for detector + bootstrap pin
    forgetWindowsMeshCapability();
    const cache = windowsMeshCacheFixture("fmc-");
    dirs.push(cache);
    windowsMeshReleaseTestHooks.cacheRoot = path.join(cache, "cache");
    cap = prepareWindowsMeshCapability();
  });
  afterEach(async () => {
    try {
      // ALL children, including real dead-proof fixture producers, are registered immediately and have OS deadlines.
      // Native children have independent watchdogs too. No assertion/timeout path bypasses these terminal joins.
      for (const child of children) if (!child.closed()) child.child.kill();
      await Promise.all(children.splice(0).map(child => child.done));
    } finally {
      for (const key of Object.keys(windowsMeshRecoveryTestHooks)) delete (windowsMeshRecoveryTestHooks as Record<string, unknown>)[key];
      for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
      forgetWindowsMeshCapability();
      for (const dir of dirs.splice(0)) {
        if (!path.basename(dir).startsWith("fmc-")) throw new Error("unsafe fixture cleanup");
        fs.rmSync(dir, { recursive: true, force: true });
      }
    }
  }, 30_000);

  it("C retains the exact original owner in F permanently, reuses the captured executable, never collects F", () => {
    const original = `${randomUUID()}\n987654321\n123\n`;
    const lock = publish(root, original);
    const supplied = snapshot(lock), fence = windowsMeshRecoveryFence(lock, supplied.directory, original);
    const proofId = identity(path.join(lock, "owner"));
    const samples: unknown[] = [];
    windowsMeshRecoveryTestHooks.afterChild = sample => { samples.push(sample); };
    expect(retireWindowsMeshOwner(lock, original, supplied.directory, supplied.owner, cap)).toEqual({ outcome: "recovered" });
    expect(exists(lock)).toBe(false);
    expect(read(path.join(fence, "owner"))).toBe(original);
    expect(identity(path.join(fence, "owner"))).toBe(proofId);
    expect(samples).toEqual([{ absolutePath: cap.absolutePath, digest: cap.digest, mode: "owner", outcome: "recovered" }]);
    const b = `${randomUUID()}\n${process.pid}\n456\n`;
    publish(root, b);
    const bId = identity(path.join(lock, "owner"));
    // Same original R AND S, never a fresh successor snapshot authorized by an earlier judgment.
    expect(retireWindowsMeshOwner(lock, original, supplied.directory, supplied.owner, cap).outcome).toBe("refused");
    expect(read(path.join(lock, "owner"))).toBe(b);
    expect(identity(path.join(lock, "owner"))).toBe(bId);
    const retained = [{ path: path.join(fence, "owner"), stat: supplied.owner, record: original }, { path: fence, stat: supplied.directory }];
    expect(removeWindowsMeshDebris(fence, retained, cap).outcome).toBe("refused");
    expect(read(path.join(fence, "owner"))).toBe(original);
  });

  it("C delayed provided-S source cannot detach B; stale owner S also refuses even with unchanged full bytes", async () => {
    const a = `${randomUUID()}\n987654321\n123\n`, b = `${randomUUID()}\n${process.pid}\n456\n`;
    const lock = publish(root, a), supplied = snapshot(lock);
    const request = encodeMeshOwnerRecoveryRequest(lock, a, supplied.directory, supplied.owner, 10_000);
    fs.renameSync(lock, path.join(root, "aside"));
    publish(root, b);
    const bId = identity(path.join(lock, "owner"));
    const delayed = start(cap, "owner", request);
    expect(interpreted(await delayed.done, "owner", delayed.child.pid).outcome).toBe("refused");
    expect(identity(path.join(lock, "owner"))).toBe(bId);
    expect(read(path.join(lock, "owner"))).toBe(b);
    // Same directory, same bytes but a new owner object must NOT be re-authorized from a fresh lstat.
    const lock2 = publish(temp(), a), s2 = snapshot(lock2);
    fs.renameSync(path.join(lock2, "owner"), path.join(root, "saved-owner"));
    fs.writeFileSync(path.join(lock2, "owner"), a, { flag: "wx" });
    expect(retireWindowsMeshOwner(lock2, a, s2.directory, s2.owner, cap).outcome).toBe("refused");
    expect(read(path.join(lock2, "owner"))).toBe(a);
  });

  it("C occupied F is never replaced/emptied; extra canonical child refuses with original proof intact", () => {
    const original = `${randomUUID()}\n987654321\n123\n`;
    const lock = publish(root, original), s = snapshot(lock);
    const fence = windowsMeshRecoveryFence(lock, s.directory, original);
    fs.mkdirSync(fence);
    fs.writeFileSync(path.join(fence, "sentinel"), "permanent");
    const fenceId = identity(fence), ownerId = identity(path.join(lock, "owner"));
    expect(retireWindowsMeshOwner(lock, original, s.directory, s.owner, cap).outcome).toBe("refused");
    expect(read(path.join(fence, "sentinel"))).toBe("permanent");
    expect(identity(fence)).toBe(fenceId);
    expect(identity(path.join(lock, "owner"))).toBe(ownerId);
    const other = publish(temp(), original), otherS = snapshot(other);
    fs.writeFileSync(path.join(other, "raw"), "unknown");
    expect(retireWindowsMeshOwner(other, original, otherS.directory, otherS.owner, cap)).toMatchObject({ outcome: "refused", reason: "owner: unexpected-entries" });
    expect(read(path.join(other, "owner"))).toBe(original);
  });

  it("C busy-sharing guard refuses without cleanup, then succeeds only after the conflicting child joins", async () => {
    const original = `${randomUUID()}\n987654321\n123\n`;
    const lock = publish(root, original), supplied = snapshot(lock), gate = temp();
    const request = encodeMeshOwnerRecoveryRequest(lock, original, supplied.directory, supplied.owner, 10_000);
    const first = start(cap, "owner", request, { name: "before-commit", dir: gate });
    await paused(gate, "before-commit");
    expect(retireWindowsMeshOwner(lock, original, supplied.directory, supplied.owner, cap)).toMatchObject({ outcome: "refused", reason: "guard: directory-not-held" });
    expect(read(path.join(lock, "owner"))).toBe(original);
    proceed(gate);
    expect(interpreted(await first.done, "owner", first.child.pid)).toEqual({ outcome: "recovered" });
    expect(read(path.join(windowsMeshRecoveryFence(lock, supplied.directory, original), "owner"))).toBe(original);
  });

  for (const name of ["before-commit", "after-commit"] as const) {
    it(`C actual helper kill/join at ${name} is unknown, and preserves the canonical/F crash partition`, async () => {
      const original = `${randomUUID()}\n987654321\n123\n`, lock = publish(root, original), supplied = snapshot(lock), gate = temp();
      const fence = windowsMeshRecoveryFence(lock, supplied.directory, original);
      const child = start(cap, "owner", encodeMeshOwnerRecoveryRequest(lock, original, supplied.directory, supplied.owner, 10_000), { name, dir: gate });
      await paused(gate, name);
      child.child.kill();
      expect(interpreted(await child.done, "owner", child.child.pid).outcome).toBe("recovery-outcome-unknown");
      if (name === "before-commit") {
        expect(read(path.join(lock, "owner"))).toBe(original);
        expect(exists(fence)).toBe(false);
      } else {
        expect(exists(lock)).toBe(false);
        expect(read(path.join(fence, "owner"))).toBe(original);
        const b = `${randomUUID()}\n${process.pid}\n456\n`;
        publish(root, b);
        expect(retireWindowsMeshOwner(lock, original, supplied.directory, supplied.owner, cap).outcome).toBe("refused");
        expect(read(path.join(lock, "owner"))).toBe(b);
      }
    });
  }

  for (let index = 0; index < 6; index++) {
    it(`D real kill after deletion ${index} (owner-first included) leaves independent records/empty dirs; fresh judgments finish`, async () => {
      const records = await deadRecords();
      const { lock, items } = deadTree(root, records), gate = temp();
      const name = `debris-after-${index}` as const;
      const child = start(cap, "debris", encodeMeshDebrisRecoveryRequest(lock, items, 10_000), { name, dir: gate });
      await paused(gate, name); // not an injected return: actual disposition+close completed at every boundary
      child.child.kill();
      expect(interpreted(await child.done, "debris", child.child.pid).outcome).toBe("recovery-outcome-unknown");
      for (let i = 0; i < items.length; i++) {
        expect(exists(items[i]!.path), items[i]!.path).toBe(i > index);
        if (i > index && items[i]!.record !== undefined) expect(read(items[i]!.path)).toBe(items[i]!.record);
      }
      if (index < items.length - 1) {
        // NEW independent judgments of surviving complete records (terminal fixture PIDs already joined above),
        // plus fresh EMPTY directory observations. No disappeared record or prior native result is reused as proof.
        const survivors = rejudgeSurvivors(items);
        for (const item of survivors) if (item.record !== undefined) expect(records).toContain(item.record);
        expect(removeWindowsMeshDebris(lock, survivors, cap)).toEqual({ outcome: "recovered" });
      }
      expect(exists(lock)).toBe(false);
    }, 25_000);
  }

  it("D late canonical O is unlisted and survives selected deletes/final empty-only removal (NEVER detach L)", async () => {
    const { lock, items } = deadTree(root, await deadRecords()), gate = temp();
    const directoryId = identity(lock);
    const child = start(cap, "debris", encodeMeshDebrisRecoveryRequest(lock, items, 10_000), { name: "debris-verified", dir: gate });
    await paused(gate, "debris-verified");
    const successor = `${randomUUID()}\n${process.pid}\n${Date.now()}\n`, ownerPath = path.join(lock, "owner");
    fs.writeFileSync(ownerPath, successor, { flag: "wx" });
    const ownerId = identity(ownerPath);
    proceed(gate);
    const receipt = await child.done;
    expect(receipt.code).toBe(2);
    expect(receipt.stdout).toBe("FMREC1 partial delete entry-delete\n");
    expect(interpreted(receipt, "debris", child.child.pid).outcome).toBe("recovery-outcome-unknown");
    expect(read(ownerPath)).toBe(successor);
    expect(identity(ownerPath)).toBe(ownerId);
    expect(identity(lock)).toBe(directoryId); // not a sibling quarantine/detach
    for (const item of items.slice(0, -1)) expect(exists(item.path)).toBe(false);
  });

  it("D ALL-before-ANY refuses a changed LAST file/late preflight unknown without deleting the FIRST proof", async () => {
    const { lock, items } = deadTree(root, await deadRecords());
    const firstId = identity(items[0]!.path);
    fs.writeFileSync(items[2]!.path, "changed after independent death judgments");
    expect(removeWindowsMeshDebris(lock, items, cap).outcome).toBe("refused");
    expect(identity(items[0]!.path)).toBe(firstId);
    expect(read(items[0]!.path)).toBe(items[0]!.record);
    const other = deadTree(temp(), await deadRecords());
    fs.writeFileSync(path.join(other.lock, "raw"), "unjudged dependent payload");
    expect(removeWindowsMeshDebris(other.lock, other.items, cap).outcome).toBe("refused");
    for (const item of other.items) expect(exists(item.path)).toBe(true);
    expect(read(path.join(other.lock, "raw"))).toBe("unjudged dependent payload");
  });

  it("D refuses raw/recordless request positions before starting any child; empty recognized dirs still recover", () => {
    const lock = path.join(root, ".lock"), empty = path.join(lock, "r.empty");
    fs.mkdirSync(empty, { recursive: true });
    fs.writeFileSync(path.join(lock, "raw"), "no independent owner record");
    const items = [path.join(lock, "raw"), empty, lock].map(file => ({ path: file, stat: fs.lstatSync(file, { bigint: true }) }));
    const samples: unknown[] = [];
    windowsMeshRecoveryTestHooks.afterChild = sample => { samples.push(sample); };
    expect(removeWindowsMeshDebris(lock, items, cap).outcome).toBe("refused");
    expect(samples).toEqual([]);
    expect(read(path.join(lock, "raw"))).toBe("no independent owner record");
    const other = path.join(temp(), ".lock"), child = path.join(other, "r.empty");
    fs.mkdirSync(child, { recursive: true });
    expect(removeWindowsMeshDebris(other, [child, other].map(file => ({ path: file, stat: fs.lstatSync(file, { bigint: true }) })), cap)).toEqual({ outcome: "recovered" });
  });

  it("C/D native malformed/trailing framed input refuses before any mutation", async () => {
    const original = `${randomUUID()}\n987654321\n123\n`, lock = publish(root, original), supplied = snapshot(lock);
    const message = encodeMeshOwnerRecoveryRequest(lock, original, supplied.directory, supplied.owner, 10_000);
    for (const bad of [message.subarray(0, message.length - 1), Buffer.concat([message, Buffer.from("x")])]) {
      const child = start(cap, "owner", bad);
      expect(interpreted(await child.done, "owner", child.child.pid).outcome).toBe("refused");
      expect(read(path.join(lock, "owner"))).toBe(original);
    }
    const tree = deadTree(temp(), [original, original, original]);
    const request = encodeMeshDebrisRecoveryRequest(tree.lock, tree.items, 10_000);
    for (const bad of [request.subarray(0, request.length - 1), Buffer.concat([request, Buffer.from("x")])]) {
      const child = start(cap, "debris", bad);
      expect(interpreted(await child.done, "debris", child.child.pid).outcome).toBe("refused");
      for (const item of tree.items) expect(exists(item.path)).toBe(true);
    }
  });

  it("T9 real Unicode 150-unit root, custody retained owner 239 logical / 243 API, no long-path registry assumption", () => {
    const base = temp();
    const longRoot = `${base}${path.sep}${"界".repeat(150 - base.length - 3)}😀`;
    expect(longRoot.length).toBe(150);
    fs.mkdirSync(longRoot);
    const original = `${randomUUID()}\n987654321\n123\n`, lock = publish(longRoot, original, "custody.lock"), supplied = snapshot(lock);
    const fence = windowsMeshRecoveryFence(lock, supplied.directory, original), owner = path.join(fence, "owner");
    expect(windowsLogicalPathUnits(path.toNamespacedPath(owner))).toBe(239);
    expect(windowsApiPathUnits(path.toNamespacedPath(owner))).toBe(243);
    expect(retireWindowsMeshOwner(lock, original, supplied.directory, supplied.owner, cap)).toEqual({ outcome: "recovered" });
    expect(read(owner)).toBe(original);
    const emptyLock = path.join(longRoot, ".lock");
    const childPrefix = path.join(emptyLock, "r.");
    const emptyChild = `${childPrefix}${"a".repeat(239 - childPrefix.length)}`;
    expect(emptyChild.length).toBe(239); // D also exercises its actual strict logical boundary, not just C's F.
    fs.mkdirSync(emptyChild, { recursive: true });
    const selected = [emptyChild, emptyLock].map(file => ({ path: file, stat: fs.lstatSync(file, { bigint: true }) }));
    expect(() => encodeMeshDebrisRecoveryRequest(emptyLock, [{ ...selected[0]!, path: `${emptyChild}x` }, selected[1]!])).toThrow(/240/);
    expect(removeWindowsMeshDebris(emptyLock, selected, cap)).toEqual({ outcome: "recovered" });
    expect(read(owner)).toBe(original); // D did not sweep permanent sibling F
  });
});
