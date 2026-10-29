import fs from "node:fs";
import { spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";
import { scriptSpawnArgs } from "../src/agents/transports/process-utils.js";
import { shortTmpdirName } from "../src/agents/short-tmpdir.js";

// Data-safety/trust-boundary regression: synthetic roots and process observations
// exercise the actual sweep and custody rename. Owner/worker identities are
// synthetic; the marker-contract test reads only this test process's own birth.
const HOUR = 3_600_000;
const NOW = Date.now();
const OLD = NOW - 48 * HOUR;
const OWNER = 2_147_483_646;
const WORKER = 2_147_483_647;
const roots: string[] = [];
const raw = new Map([
  ["events.jsonl", Buffer.from('{"text":"raw é"}\n'.repeat(400))],
  ["session.jsonl", Buffer.from('{"type":"session"}\n{"torn":')],
  ["task.txt", Buffer.from("private agent prompt π\n")],
  ["reply.json", Buffer.from('{"text":"saved raw result λ"}')],
  ["lifecycle.jsonl", Buffer.from('{"event":"exit"}\n')],
  ["handoff-session/source.jsonl", Buffer.from("full fork source\n")],
]);
const age = (file: string): void => {
  const stat = fs.lstatSync(file);
  if (stat.isDirectory()) for (const name of fs.readdirSync(file)) age(path.join(file, name));
  fs.utimesSync(file, new Date(OLD), new Date(OLD));
};
const fixture = (owner: Record<string, unknown> = {}) => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-dead-owner-")); roots.push(tempRoot);
  const root = path.join(tempRoot, "pi-fabric-runs-owner");
  const run = path.join(root, "run");
  fs.mkdirSync(run, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({
    pid: OWNER, uid: process.getuid?.(), processStartTime: "100", startedAt: OLD, heartbeatAt: OLD, orphanedAt: OLD, ...owner,
  }), { mode: 0o600 });
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({
    id: "run", status: "completed", transport: "process", sessionId: String(WORKER), finishedAt: OLD,
  }), { mode: 0o600 });
  for (const [name, bytes] of raw) {
    const file = path.join(run, name); fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, bytes, { mode: 0o600 });
  }
  age(root);
  const sweep = (extra: Record<string, unknown> = {}) => sweepTempRunRoots({
    tempRoot, now: NOW, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: 24 * HOUR, ...extra,
  });
  return { root, run, tempRoot, sweep };
};
const gone = () => vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("absent"), { code: "ESRCH" }); });
const proc = (f: ReturnType<typeof fixture>, options: { uid?: number | undefined; ticks?: string; unreadable?: boolean; deniedDirectory?: boolean } = {}) => {
  const read = fs.readFileSync, stat = fs.lstatSync;
  const observed = fs.lstatSync(f.root);
  vi.spyOn(process, "kill").mockImplementation(pid => {
    if (pid === OWNER) return true;
    throw Object.assign(new Error("absent"), { code: "ESRCH" });
  });
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    if (String(file) === `/proc/${OWNER}`) {
      if (options.deniedDirectory) throw Object.assign(new Error("denied"), { code: "EACCES" });
      return Object.assign(Object.create(Object.getPrototypeOf(observed)), observed, { uid: options.uid ?? process.getuid?.() });
    }
    return (stat as (...args: unknown[]) => fs.Stats)(file, ...args);
  }) as typeof fs.lstatSync);
  return vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (String(file) === `/proc/${OWNER}/stat`) {
      if (options.unreadable) throw Object.assign(new Error("denied"), { code: "EACCES" });
      return `${OWNER} (name with ) parentheses) S ${Array(18).fill("0").join(" ")} ${options.ticks ?? "100"}\n`;
    }
    return (read as (...args: unknown[]) => unknown)(file, ...args);
  }) as typeof fs.readFileSync);
};
const receipts = (root: string): Array<{ state: string; source: string; destination: string }> => {
  const archive = `${root}.trace-archive`;
  if (!fs.existsSync(archive)) return [];
  return fs.readdirSync(archive).map(name => JSON.parse(fs.readFileSync(path.join(archive, name, "custody.json"), "utf8")));
};
const verifyArchive = (f: ReturnType<typeof fixture>): void => {
  const receipt = receipts(f.root).find(row => row.source === f.run && row.state === "moved");
  expect(receipt).toBeDefined();
  for (const [name, bytes] of raw) expect(fs.readFileSync(path.join(receipt!.destination, name)), name).toEqual(bytes);
};
const snapshot = (root: string): unknown => {
  const stat = fs.lstatSync(root);
  return { ino: stat.ino, mtime: stat.mtimeMs, ctime: stat.ctimeMs,
    contents: stat.isDirectory() ? Object.fromEntries(fs.readdirSync(root).sort().map(name => [name, snapshot(path.join(root, name))]))
      : fs.readFileSync(root).toString("hex") };
};
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe.skipIf(process.platform !== "linux")("dead run-root owner custody", () => {
  it.each([undefined, null])("archives an idle dead owner with closedAt=%s on its first maintenance pass", closedAt => {
    const f = fixture({ closedAt }); gone();
    expect(f.sweep().removedRoots).toEqual([f.root]);
    expect(fs.existsSync(f.root)).toBe(false);
    verifyArchive(f);
  });
  it("uses the same whole-tree custody for explicitly closed roots, without compacting raw bytes", () => {
    const f = fixture({ closedAt: OLD, childrenStopped: true }); gone();
    expect(f.sweep().removedRuns).toEqual([f.run]); verifyArchive(f);
  });
  it("archives a legacy same-uid root with no saved birth identity only after checked PID absence", () => {
    const f = fixture({ uid: undefined, processStartTime: undefined }); gone();
    expect(f.sweep().removedRoots).toEqual([f.root]); verifyArchive(f);
  });
  it("accepts checked same-uid owner PID reuse, not the new process as the recorded owner", () => {
    const f = fixture(); proc(f, { ticks: "200" });
    expect(f.sweep().removedRoots).toEqual([f.root]); verifyArchive(f);
  });
  it.each(["live", "missing saved birth", "unreadable proc", "unreadable proc directory", "malformed proc", "other uid", "saved other uid", "unknown liveness"])("keeps an unclosed root for %s", kind => {
    const f = fixture(kind === "missing saved birth" ? { processStartTime: undefined }
      : kind === "saved other uid" ? { uid: (process.getuid?.() ?? 0) + 1 } : {});
    const reads = proc(f, { ticks: kind === "malformed proc" ? "bad" : "100", unreadable: kind === "unreadable proc",
      deniedDirectory: kind === "unreadable proc directory", uid: kind === "other uid" ? (process.getuid?.() ?? 0) + 1 : undefined });
    if (kind === "unknown liveness") vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    const before = snapshot(f.root);
    expect(f.sweep().removedRoots).toEqual([]);
    expect(snapshot(f.root)).toEqual(before);
    if (["other uid", "saved other uid", "unknown liveness", "unreadable proc directory"].includes(kind)) {
      expect(reads.mock.calls.some(([file]) => String(file) === `/proc/${OWNER}/stat`)).toBe(false);
    }
  });
  it.each(["heartbeat", "file append", "directory activity", "future heartbeat"])("requires a full idle grace period after %s", kind => {
    const f = fixture(kind === "heartbeat" ? { heartbeatAt: NOW - HOUR } : kind === "future heartbeat" ? { heartbeatAt: NOW + HOUR } : {}); gone();
    if (kind === "file append") fs.appendFileSync(path.join(f.run, "events.jsonl"), "recent\n");
    if (kind === "directory activity") fs.utimesSync(f.run, new Date(NOW), new Date(NOW));
    expect(f.sweep().removedRoots).toEqual([]); expect(fs.existsSync(f.run)).toBe(true);
  });
  it.each(["live worker", "worker pid reuse", "unresolved", "nonterminal", "unknown identity", "unknown file"])("keeps worker custody for %s even with a dead owner", kind => {
    const f = fixture(); gone();
    if (["live worker", "worker pid reuse"].includes(kind)) {
      const kill = process.kill;
      vi.spyOn(process, "kill").mockImplementation((pid, signal) => pid === WORKER ? true : kill(pid, signal));
    }
    if (kind === "worker pid reuse") {
      const file = path.join(f.run, "status.json");
      fs.writeFileSync(file, JSON.stringify({ status: "completed", transport: "process", sessionId: String(WORKER), processStartTime: "1", finishedAt: OLD }));
    }
    if (kind === "unresolved") fs.writeFileSync(path.join(f.run, "unresolved-worker.json"), "{}");
    if (kind === "nonterminal") fs.writeFileSync(path.join(f.run, "status.json"), JSON.stringify({ status: "running", transport: "process", sessionId: String(WORKER) }));
    if (kind === "unknown identity") fs.writeFileSync(path.join(f.run, "status.json"), JSON.stringify({ status: "completed" }));
    if (kind === "unknown file") fs.writeFileSync(path.join(f.run, "unknown"), "keep");
    age(f.root);
    expect(f.sweep().removedRoots).toEqual([]); expect(fs.existsSync(f.run)).toBe(true);
  });
  it("dry-run reports candidates and exact retained file bytes without markers, receipts, recovery or compaction", () => {
    const f = fixture({ closedAt: null }); gone(); const before = snapshot(f.tempRoot);
    const bytes = [...raw.values()].reduce((total, value) => total + value.length, fs.statSync(path.join(f.run, "status.json")).size);
    const result = f.sweep({ dryRun: true, minIntervalMs: HOUR });
    expect(result).toMatchObject({ removedRoots: [], removedRuns: [], candidates: [{ path: f.run, bytes }] });
    expect(snapshot(f.tempRoot)).toEqual(before);
    expect(fs.existsSync(path.join(f.tempRoot, ".pi-fabric-runs-sweep.json"))).toBe(false);
  });
  it("dry-run previews closed roots without touching their trace bytes", () => {
    const f = fixture({ closedAt: OLD }); gone(); const before = snapshot(f.tempRoot);
    expect(f.sweep({ dryRun: true })).toMatchObject({ candidates: [{ path: f.run }] });
    expect(snapshot(f.tempRoot)).toEqual(before);
  });
  it.each(["archive", "dry-run", "fresh scratch"])("preserves raw private TMPDIR custody: %s", mode => {
    const f = fixture(); gone();
    const temporary = path.join(f.tempRoot, shortTmpdirName("run")), bytes = Buffer.from("private raw trace\n");
    fs.mkdirSync(temporary, { mode: 0o700 });
    fs.writeFileSync(path.join(temporary, "lifecycle.jsonl"), bytes, { mode: 0o600 });
    const file = path.join(f.run, "status.json"), record = JSON.parse(fs.readFileSync(file, "utf8"));
    fs.writeFileSync(file, JSON.stringify({ ...record, temporaryDirectory: temporary }));
    age(f.root); age(temporary);
    if (mode === "fresh scratch") fs.utimesSync(path.join(temporary, "lifecycle.jsonl"), new Date(NOW), new Date(NOW));
    const before = snapshot(f.tempRoot);
    const result = f.sweep({ dryRun: mode === "dry-run" });
    if (mode === "archive") {
      expect(result.removedRoots).toEqual([f.root]); verifyArchive(f);
      const held = receipts(f.root).find(row => row.source === temporary)!;
      expect(held?.state).toBe("moved");
      expect(fs.readFileSync(path.join(held.destination, "lifecycle.jsonl"))).toEqual(bytes);
      expect(fs.existsSync(temporary)).toBe(false);
    } else {
      expect(snapshot(f.tempRoot)).toEqual(before);
      if (mode === "dry-run") expect(result.candidates).toContainEqual({ path: temporary, bytes: bytes.length });
      else expect(result.removedRoots).toEqual([]);
    }
  });
  it("the real detached entry honors dry-run instead of silently archiving the request", async () => {
    const f = fixture({ closedAt: null }); const before = snapshot(f.tempRoot);
    const [runtime, ...args] = await scriptSpawnArgs(path.resolve("src/storage/sweep-main.ts"), [JSON.stringify({
      tempRoot: f.tempRoot, dryRun: true, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: 24 * HOUR,
    })]);
    const child = spawnSync(runtime!, args, { encoding: "utf8", timeout: 15_000 });
    expect(child.status, child.stderr).toBe(0);
    expect(JSON.parse(child.stdout)).toMatchObject({ removedRoots: [], removedRuns: [], candidates: [{ path: f.run, bytes: expect.any(Number) }] });
    expect(snapshot(f.tempRoot)).toEqual(before);
  });
  it("archives multi-GiB sparse traces by whole-root rename without reading their payload", () => {
    const f = fixture(); gone();
    const file = path.join(f.run, "events.jsonl"), size = 3 * 1024 ** 3;
    fs.truncateSync(file, size); age(f.root);
    const inode = fs.statSync(file).ino, read = vi.spyOn(fs, "readFileSync");
    expect(f.sweep().removedRoots).toEqual([f.root]);
    expect(read.mock.calls.some(([input]) => String(input) === file)).toBe(false);
    const held = receipts(f.root).find(row => row.source === f.run);
    if (typeof held?.destination !== "string") throw new Error("Missing custody destination");
    expect(fs.statSync(path.join(held.destination, "events.jsonl"))).toMatchObject({ ino: inode, size });
  });
  it("keeps the original and all bytes on an archive rename refusal", () => {
    const f = fixture(); gone(); const before = snapshot(f.root), rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === f.run) throw Object.assign(new Error("cross-device refusal"), { code: "EXDEV" });
      return rename(from, to);
    });
    expect(f.sweep().removedRoots).toEqual([]);
    expect(snapshot(f.root)).toEqual(before);
    expect(receipts(f.root).every(row => row.state === "rename-failed")).toBe(true);
  });
  it("keeps the tree if its owner marker changes during the worker safety walk", () => {
    const f = fixture(); gone();
    const kill = process.kill; let changed = false;
    vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (pid === WORKER && !changed) {
        changed = true;
        fs.writeFileSync(path.join(f.root, ".fabric-owner.json"), JSON.stringify({ pid: OWNER, startedAt: OLD, heartbeatAt: NOW }));
      }
      return kill(pid, signal);
    });
    expect(f.sweep().removedRoots).toEqual([]);
    expect(fs.existsSync(f.run)).toBe(true); expect(receipts(f.root)).toEqual([]);
  });
  it("does not let ENODEV recovery probe a live or unrelated child process", () => {
    const f = fixture(), id = "a".repeat(32), run = path.join(f.root, id);
    fs.renameSync(f.run, run);
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id, status: "completed", transport: "process", sessionId: String(WORKER), processStartTime: "100" }));
    fs.writeFileSync(path.join(run, "unresolved-worker.json"), JSON.stringify({ reason: "Owned scope observation failed: Error: ENODEV" }));
    age(f.root);
    vi.spyOn(process, "kill").mockImplementation(pid => {
      if (pid === OWNER) throw Object.assign(new Error("gone"), { code: "ESRCH" });
      if (pid === WORKER) return true;
      throw new Error("Unexpected process probe");
    });
    const read = fs.readFileSync, reads = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file).startsWith("/proc/")) throw Object.assign(new Error("denied"), { code: "EACCES" });
      return Reflect.apply(read, fs, [file, ...args]);
    }) as typeof fs.readFileSync);
    const before = snapshot(f.root);
    expect(f.sweep().removedRoots).toEqual([]);
    expect(snapshot(f.root)).toEqual(before);
    expect(reads.mock.calls.some(([file]) => String(file).startsWith("/proc/"))).toBe(false);
  });
  it("persists same-uid owner birth identity when marking/heartbeating/closing new roots", () => {
    const f = fixture();
    fs.unlinkSync(path.join(f.root, ".fabric-owner.json"));
    markRunRootActive(f.root, NOW); markRunRootClosed(f.root, NOW, true);
    expect(JSON.parse(fs.readFileSync(path.join(f.root, ".fabric-owner.json"), "utf8"))).toMatchObject({
      pid: process.pid, uid: process.getuid?.(), processStartTime: expect.stringMatching(/^\d+$/), closedAt: NOW,
    });
  });
});
