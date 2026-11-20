// Keep: cross-process participant liveness/presence safety; a real external replacement must not be swallowed by a local listing cache.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import type { BigIntStats, Stats } from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshStateEntry } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { readParticipantFiles, writeParticipantFile } from "../src/topology/participant-files.js";

const key = (kind: string, id: string) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
const leafIdentity = (file: string) => {
  const stat = fs.statSync(file, { bigint: true });
  return `${stat.dev}:${stat.ino}`;
};
const readDisk = fs.readFileSync.bind(fs);

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-participant-replacement-"));
  const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
  const now = Date.now();
  const id = "session:participant-replacement-00000000";
  const identity = { id, name: "participant-replacement", kind: "main" as const,
    sessionId: "participant-replacement-00000000" };
  const incarnationToken = "participant-replacement-owner";
  const host = { format: 1, id, rootId: id, identity, incarnationToken,
    startedAt: now - 1_000, updatedAt: now, expiresAt: now + 30_000 };
  await store.put({ key: key("hosts", id), identity, value: host });
  writeHostLease(root, { id, rootId: id, identityId: id, incarnationToken,
    startedAt: host.startedAt, updatedAt: now, expiresAt: now + 60_000 });
  const participant = { format: 1, id, kind: "root", rootId: id, ownerHostId: id,
    ownerIdentityId: id, ownerIncarnation: incarnationToken, name: identity.name, status: "idle",
    runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: identity.sessionId, startedAt: host.startedAt, updatedAt: now, controlProtocol: "v1" };
  const entry: MeshStateEntry = { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity, value: participant };
  writeParticipantFile(root, entry);
  return { root, store, side: new StoreBridgeSide(store, "remote"), id, identity, host, participant, entry,
    file: path.join(root, "participants", `${entry.key.split("/").at(-1)}.json`) };
};

const childWriter = String.raw`
import fs from "node:fs";
import { pathToFileURL } from "node:url";
const input = JSON.parse(fs.readFileSync(0, "utf8"));
const { writeParticipantFile } = await import(pathToFileURL(input.writer).href);
const updatedAt = Date.now();
const entry = { ...input.entry, version: input.entry.version + 1, updatedAt,
  value: { ...input.entry.value, status: "running", updatedAt } };
writeParticipantFile(input.root, entry);
process.stdout.write(JSON.stringify({ pid: process.pid, bun: typeof Bun !== "undefined", entry }));
`;

const heldParentStat = (directory: string) => {
  const nativeStat = fs.statSync.bind(fs);
  const actual = nativeStat(directory) as Stats;
  const actualBig = nativeStat(directory, { bigint: true }) as BigIntStats;
  const normal = Object.assign(Object.create(Object.getPrototypeOf(actual)), actual) as Stats;
  normal.mtimeMs = 0;
  normal.mtime = new Date(0);
  const bigint = Object.assign(Object.create(Object.getPrototypeOf(actualBig)), actualBig) as BigIntStats;
  bigint.mtimeMs = 0n;
  bigint.mtimeNs = 0n;
  bigint.mtime = new Date(0);
  let normalReads = 0, bigintReads = 0;
  const spy = vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
    if (typeof target === "string" && path.resolve(target) === directory) {
      if (options?.bigint) { bigintReads++; return bigint; }
      normalReads++;
      return normal;
    }
    return nativeStat(target, options);
  }) as typeof fs.statSync);
  return {
    normal, bigint, actual, actualBig,
    reads: () => ({ normal: normalReads, bigint: bigintReads }),
    restore: () => spy.mockRestore(),
  };
};

const statEvidence = (stat: Stats | BigIntStats) => ({
  dev: String(stat.dev), ino: String(stat.ino), size: String(stat.size),
  mtimeMs: String(stat.mtimeMs), mtimeNs: "mtimeNs" in stat ? String(stat.mtimeNs) : undefined,
  mtime: stat.mtime.toISOString(), prototype: Object.getPrototypeOf(stat)?.constructor?.name,
});

const exerciseReplacement = async (parentMode: "natural" | "held-old-parent") => {
  const f = await fixture();
  const directory = path.dirname(f.file);
  let parent: ReturnType<typeof heldParentStat> | undefined;
  let close: (() => void) | undefined;
  let rawWatch: fs.FSWatcher | undefined;
  let restoreReadSpy: (() => void) | undefined;
  let restoreReaddirSpy: (() => void) | undefined;
  let restoreSnapshotSpy: (() => void) | undefined;
  let participantJsonReads = () => 0;
  try {
    if (parentMode === "held-old-parent") parent = heldParentStat(directory);
    const readdir = vi.spyOn(fs, "readdirSync");
    restoreReaddirSpy = () => readdir.mockRestore();
    const directoryLists = () => readdir.mock.calls.filter(([target]) =>
      typeof target === "string" && path.resolve(target) === directory).length;
    const beforeBaselineLists = directoryLists();
    const baseline = await f.side.presence();
    expect(baseline.hosts).toHaveLength(1);
    expect(baseline.participants).toEqual([f.participant]);
    expect(f.store.get(f.entry.key, { fresh: true })).toBeUndefined(); // native, file-only participant
    expect(directoryLists() - beforeBaselineLists).toBe(1); // real strict listing warmed the complete cache
    if (parent) {
      expect(parent.reads().normal).toBeGreaterThan(0); // the reader used the exact parent-only seam
      expect(Number(parent.normal.mtimeMs)).toBe(0);
      expect(parent.bigint.mtimeMs).toBe(0n);
      expect(parent.bigint.mtimeNs).toBe(0n);
      expect(parent.normal.mtime.getTime()).toBe(0);
      expect(parent.bigint.mtime.getTime()).toBe(0);
      expect(Date.now() - Number(parent.normal.mtimeMs)).toBeGreaterThanOrEqual(100);
      expect(parent.normal.dev).toBe(parent.actual.dev);
      expect(parent.normal.ino).toBe(parent.actual.ino);
      expect(parent.normal.size).toBe(parent.actual.size);
      expect(Object.getPrototypeOf(parent.normal)).toBe(Object.getPrototypeOf(parent.actual));
      expect(parent.bigint.dev).toBe(parent.actualBig.dev);
      expect(parent.bigint.ino).toBe(parent.actualBig.ino);
      expect(parent.bigint.size).toBe(parent.actualBig.size);
      expect(Object.getPrototypeOf(parent.bigint)).toBe(Object.getPrototypeOf(parent.actualBig));
    }
    const beforeLeaf = leafIdentity(f.file);
    const nativeEvents: string[] = [];
    const changes: string[] = [], failures: string[] = [];
    let nativeRename = false;
    let finishNative!: () => void;
    const nativeReceipt = new Promise<void>(resolve => { finishNative = resolve; });
    rawWatch = fs.watch(directory, (event, name) => {
      const receipt = `${event}:${String(name)}`;
      nativeEvents.push(receipt);
      if (String(name) === path.basename(f.file)) { nativeRename = true; finishNative(); }
    });
    const readSnapshots = vi.spyOn(f.store, "withStateReadSnapshot");
    restoreSnapshotSpy = () => readSnapshots.mockRestore();
    let finishNotice!: (result: "changed" | "failed") => void;
    const notice = new Promise<"changed" | "failed">(resolve => { finishNotice = resolve; });
    close = f.side.subscribeChanges(kind => {
      changes.push(kind);
      if (changes.length === 1) finishNotice("changed");
    }, error => {
      failures.push(error.message);
      finishNotice("failed");
    });
    const baselineSnapshots = readSnapshots.mock.calls.length;
    expect(baselineSnapshots).toBeGreaterThan(0);

    const readSpy = vi.spyOn(fs, "readFileSync");
    restoreReadSpy = () => readSpy.mockRestore();
    participantJsonReads = () => readSpy.mock.calls.filter(([target]) =>
      typeof target === "string" && path.resolve(target) === f.file).length;
    const readsBeforeReplacement = participantJsonReads();
    const parentBeforeWrite = parent ? { normal: statEvidence(fs.statSync(directory)),
      bigint: statEvidence(fs.statSync(directory, { bigint: true })) } : undefined;
    const child = spawnSync("bun", ["--eval", childWriter], {
      cwd: process.cwd(), input: JSON.stringify({ root: f.root, entry: f.entry,
        writer: path.resolve("src/topology/participant-files.ts") }),
      encoding: "utf8", timeout: 10_000, maxBuffer: 1024 * 1024, windowsHide: true,
    });
    const childDetails = { pid: child.stdout ? (() => { try { return JSON.parse(child.stdout).pid; } catch { return undefined; } })() : undefined,
      parentPid: process.pid, status: child.status, signal: child.signal, error: child.error?.message,
      stdout: child.stdout.trim(), stderr: child.stderr.trim() };
    if (child.error || child.status !== 0) {
      throw new Error(`External Bun writer failed (not an F2 RED): ${JSON.stringify(childDetails)}`);
    }
    let childReceipt: { pid: number; bun: boolean; entry: MeshStateEntry };
    try { childReceipt = JSON.parse(child.stdout) as typeof childReceipt; }
    catch { throw new Error(`External Bun writer returned invalid receipt (not an F2 RED): ${JSON.stringify(childDetails)}`); }
    if (!childReceipt.bun || childReceipt.pid === process.pid) {
      throw new Error(`Writer was not a different Bun process (not an F2 RED): ${JSON.stringify(childDetails)}`);
    }
    const updatedEntry = childReceipt.entry;
    const updatedParticipant = updatedEntry.value as typeof f.participant;
    expect(updatedEntry.version).toBe(f.entry.version + 1);
    expect(updatedParticipant).toMatchObject({ status: "running", updatedAt: updatedEntry.updatedAt });
    const afterLeaf = leafIdentity(f.file);
    expect(afterLeaf).not.toBe(beforeLeaf); // real bigint leaf identity changed across the child write
    expect(f.store.get(f.entry.key, { fresh: true })).toBeUndefined();
    const parentAfterWrite = parent ? { normal: statEvidence(fs.statSync(directory)),
      bigint: statEvidence(fs.statSync(directory, { bigint: true })) } : undefined;
    if (parent) {
      expect(parentBeforeWrite?.normal).toEqual(statEvidence(parent.normal));
      expect(parentBeforeWrite?.bigint).toEqual(statEvidence(parent.bigint));
      expect(parentAfterWrite?.normal).toEqual(parentBeforeWrite?.normal);
      expect(parentAfterWrite?.bigint).toEqual(parentBeforeWrite?.bigint);
      expect(fs.statSync(directory)).toEqual(parent.normal);
      expect(fs.statSync(directory, { bigint: true })).toEqual(parent.bigint);
    }

    let timeout: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<"deadline">(resolve => { timeout = setTimeout(() => resolve("deadline"), 2_000); });
    const outcome = await Promise.race([
      Promise.all([notice, nativeReceipt]).then(([result]) => result), deadline,
    ]);
    if (timeout !== undefined) clearTimeout(timeout);
    if (outcome !== "changed") {
      const freshCached = await f.side.presence(); // diagnostic only, after the native-notification bound
      const disk = JSON.parse(readDisk(f.file, "utf8")) as { value?: { status?: string } };
      const diagnostic = {
        classification: nativeRename && readSnapshots.mock.calls.length > baselineSnapshots &&
          disk.value?.status === "running" && freshCached.participants[0]?.status === "idle"
          ? "F2 reproduced: real leaf evidence caused a fresh capture, but strict participant read returned stale cached content"
          : "not an F2 RED (check fixture, child import/write, native watch, or capture receipts)",
        parentMode, baselineStatus: baseline.participants[0]?.status,
        diskStatus: disk.value?.status, diskLeafIdentity: afterLeaf, beforeLeafIdentity: beforeLeaf,
        cachedFreshReadStatus: freshCached.participants[0]?.status,
        child: childDetails, childBun: childReceipt.bun, nativeEvents, nativeRename, changes, failures,
        parent: parent ? { stable: true, reads: parent.reads(), heldNormal: statEvidence(parent.normal),
          heldBigint: statEvidence(parent.bigint), beforeWrite: parentBeforeWrite, afterWrite: parentAfterWrite } : "natural metadata",
        stateReadSnapshots: { baseline: baselineSnapshots, after: readSnapshots.mock.calls.length },
        participantJsonReadsSinceReplacement: participantJsonReads() - readsBeforeReplacement,
      };
      throw new Error(`No external participant presence notification within 2000 ms: ${JSON.stringify(diagnostic)}`);
    }
    expect(failures).toEqual([]);
    expect(changes).toEqual(["presence"]);
    expect(nativeRename).toBe(true);
    expect(readSnapshots.mock.calls.length).toBeGreaterThan(baselineSnapshots);
    expect(participantJsonReads()).toBeGreaterThan(readsBeforeReplacement);
    const fresh = await f.side.presence(); // canonical read is intentionally after notification
    expect(JSON.parse(readDisk(f.file, "utf8"))).toMatchObject(updatedEntry);
    expect(fresh.participants).toEqual([updatedParticipant]);
    expect(f.store.get(f.entry.key, { fresh: true })).toBeUndefined();
    if (parent) {
      expect(parent.reads().normal).toBeGreaterThan(0);
      expect(parent.reads().bigint).toBeGreaterThan(0);
      expect(fs.statSync(directory)).toEqual(parent.normal);
      expect(fs.statSync(directory, { bigint: true })).toEqual(parent.bigint);
    }
  } finally {
    close?.();
    rawWatch?.close();
    restoreReadSpy?.();
    restoreReaddirSpy?.();
    restoreSnapshotSpy?.();
    parent?.restore();
    f.store.closeState();
    fs.rmSync(f.root, { recursive: true, force: true });
  }
};

describe("cross-process mesh participant replacement (F2)", () => {
  it("reports a real different-Bun-process replacement with natural parent metadata", async () => {
    await exerciseReplacement("natural");
  }, 15_000);

  it("revalidates a replacement while only the parent stat result is held at an older stamp", async () => {
    await exerciseReplacement("held-old-parent");
  }, 15_000);

  it("retains parsed-entry and array identity on unchanged strict maxAgeMs:0 reads", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "participant-strict-read-cache-"));
    const now = Date.now();
    const id = "session:participant-strict-cache-00000000";
    const identity = { id, name: "strict-cache", kind: "main" as const };
    const entry: MeshStateEntry = { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity,
      value: { id, kind: "root", rootId: id, status: "idle" } };
    const file = path.join(root, "participants", `${entry.key.split("/").at(-1)}.json`);
    let restoreReadSpy: (() => void) | undefined;
    try {
      writeParticipantFile(root, entry);
      const readSpy = vi.spyOn(fs, "readFileSync");
      restoreReadSpy = () => readSpy.mockRestore();
      const participantJsonReads = () => readSpy.mock.calls.filter(([target]) =>
        typeof target === "string" && path.resolve(target) === file).length;
      const first = readParticipantFiles(root, { maxAgeMs: 0 });
      const readsAfterFirst = participantJsonReads();
      expect(readsAfterFirst).toBeGreaterThan(0);
      const second = readParticipantFiles(root, { maxAgeMs: 0 });
      expect(second).toBe(first);
      expect(second[0]).toBe(first[0]);
      expect(participantJsonReads()).toBe(readsAfterFirst);
    } finally {
      restoreReadSpy?.();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
