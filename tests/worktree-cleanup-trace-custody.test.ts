import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { retainCleanupRoot, WorktreeManager } from "../src/agents/worktree-manager.js";
import * as atomicWrites from "../src/core/atomic-write.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import * as cloneFirst from "../src/agents/clone-first-worktree.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { fakeManagerHeapWorker, heapRunResult } from "./fixtures/manager-heap-probe.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentResultPath, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// Data-safety keep reason: real filesystem custody and the real AgentManager
// setup-failure rollback. Git creation/destruction is recorded, NEVER executed.
// Owned raw fixture roots are deliberately retained for inspection, not swept.
const payloads = new Map([
  ["sessions/raw.JSONL", Buffer.from('{"raw":1}\n{"torn":')],
  ["sessions/nested/capture.NdJsOn.backup", Buffer.from([0, 255, 13, 10, 123])],
  ["reclaim/old.session.jsonl.BAK", Buffer.from("reclaim raw bytes\n")],
  ["traces/stream.ndjson", Buffer.from('{"trace":1}\n')],
  ["node_modules/.cache/.pi/raw.trace.NDJSON", Buffer.from("ignored raw trace\n")],
  ["unclassified.data", Buffer.from("not a suffix inventory\n")],
]);
const seed = (root: string): void => {
  for (const [name, bytes] of payloads) {
    const file = path.join(root, name);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, bytes);
  }
};
const verify = (root: string): void => {
  for (const [name, bytes] of payloads) expect(fs.readFileSync(path.join(root, name)), name).toEqual(bytes);
};
interface Receipt { source: string; destination: string; state: string; }
const receipts = (root: string): Receipt[] => {
  const area = path.join(root, ".pi", "fabric", "worktrees.trace-archive");
  if (!fs.existsSync(area)) return [];
  // Inspect custody containers only; never inventory/copy a retained tree.
  return fs.readdirSync(area).filter(name => name.startsWith("cleanup-"))
    .map(name => JSON.parse(fs.readFileSync(path.join(area, name, "custody.json"), "utf8")) as Receipt);
};
const moved = (root: string, source: string): Receipt => {
  const receipt = receipts(root).find(row => row.source === source && row.state === "moved");
  expect(receipt, `missing whole-root custody for ${source}`).toBeDefined();
  verify(receipt!.destination);
  return receipt!;
};
const ownedFixtures: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  const evidence = process.env.PI_TEST_CUSTODY_EVIDENCE_DIR;
  for (const root of ownedFixtures.splice(0)) {
    if (!evidence) continue;
    const target = path.join(evidence, path.basename(root));
    fs.mkdirSync(target, { recursive: true, mode: 0o700 });
    const manifest: unknown[] = []; let count = 0, bytes = 0;
    const exportOwned = (file: string, relative: string): void => {
      if (++count > 4096) throw new Error("Owned custody evidence entry bound exceeded");
      const stat = fs.lstatSync(file, { bigint: true });
      const type = stat.isSymbolicLink() ? "symlink" : stat.isDirectory() ? "directory" : stat.isFile() ? "file" : "special";
      manifest.push({ path: relative, type, dev: String(stat.dev), ino: String(stat.ino), mode: String(stat.mode), size: String(stat.size),
        ...(stat.isSymbolicLink() ? { link: fs.readlinkSync(file) } : {}) });
      if (type === "directory") {
        fs.mkdirSync(path.join(target, "raw", relative), { recursive: true, mode: 0o700 });
        for (const name of fs.readdirSync(file).sort()) exportOwned(path.join(file, name), path.join(relative, name));
      } else if (type === "file") {
        bytes += Number(stat.size); if (bytes > 64 * 1024 * 1024) throw new Error("Owned custody evidence byte bound exceeded");
        fs.writeFileSync(path.join(target, "raw", relative), fs.readFileSync(file), { mode: 0o600, flag: "wx" });
      }
    };
    exportOwned(root, "");
    fs.writeFileSync(path.join(target, "manifest.json"), JSON.stringify({ root, TMPDIR: process.env.TMPDIR,
      umask: process.umask(), userBus: process.env.DBUS_SESSION_BUS_ADDRESS ?? null, runtimeDirectory: process.env.XDG_RUNTIME_DIR ?? null, manifest }, null, 2));
  }
});
const fixture = (checkoutMode?: number) => {
  const owned = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-minimal-custody-")));
  ownedFixtures.push(owned);
  const root = checkoutMode === undefined ? owned : path.join(owned, "checkout");
  if (checkoutMode !== undefined) { fs.mkdirSync(root, { mode: 0o700 }); fs.chmodSync(root, checkoutMode); }
  vi.stubEnv("HOME", path.join(owned, "home")); vi.stubEnv("USERPROFILE", path.join(owned, "home"));
  vi.stubEnv("XDG_STATE_HOME", path.join(owned, "state")); vi.stubEnv("LOCALAPPDATA", path.join(owned, "local-app-data"));
  vi.stubEnv("TMPDIR", owned); vi.stubEnv("TMP", owned); vi.stubEnv("TEMP", owned);
  console.info(`retained cleanup fixture: ${owned}`);
  const gitCalls: string[][] = [];
  const created: string[] = [];
  const manager = new WorktreeManager();
  let rejectSetup = false;
  vi.spyOn(processUtils, "executeFile").mockImplementation(async (command, args) => {
    if (command !== "git") {
      if (rejectSetup) throw new Error("bounded setup failure");
      throw new Error(`unexpected native command: ${command}`);
    }
    gitCalls.push([...args]);
    if (args.includes("--show-toplevel")) return { stdout: root + "\n", stderr: "" };
    if (args.includes("--show-prefix")) return { stdout: "", stderr: "" };
    if (args[0] === "rev-parse") return { stdout: "a".repeat(40) + "\n", stderr: "" };
    // Old behavior RED records worktree remove/branch -D without deleting bytes.
    return { stdout: "", stderr: "" };
  });
  vi.spyOn(cloneFirst, "addCloneFirstWorktree").mockImplementation(async options => {
    fs.mkdirSync(options.dest, { recursive: true }); seed(options.dest); created.push(options.dest);
    return { cloned: false, message: "recorded Git creation; owned real payload only" };
  });
  const noGitDelete = () => expect(gitCalls.filter(args =>
    args[0] === "worktree" && args[1] === "remove" || args[0] === "branch" || args.includes("prune"))).toEqual([]);
  return { root, manager, created, gitCalls, noGitDelete, failSetup: () => { rejectSetup = true; },
    create: (id = "same-id") => manager.create(id, root, "custody") };
};

// Observe actual opened FD paths and successful native fsyncs, not just call counts.
// Inject only after the root rename: preparation still uses real durable writes.
const faultAfterRename = (source: string, fault: "destination" | "source" | "moved-receipt") => {
  const events: string[] = [], opened = new Map<number, string>();
  const error = Object.assign(new Error(`${fault} durability EIO`), { code: "EIO" });
  const open = fs.openSync, sync = fs.fsyncSync, rename = fs.renameSync, write = atomicWrites.writeJsonAtomic;
  let destination = "", prepared: Buffer | undefined;
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
    const fd = open(file, flags, mode); opened.set(fd, path.resolve(String(file))); return fd;
  });
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (String(from) === source) prepared = fs.readFileSync(path.join(path.dirname(String(to)), "custody.prepared.json"));
    rename(from, to);
    if (String(from) === source) {
      destination = String(to); events.push("rename");
      fs.mkdirSync(source); fs.writeFileSync(path.join(source, "replacement.jsonl"), "replacement owner\n");
    }
  });
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    const file = opened.get(fd);
    const barrier = destination && fs.fstatSync(fd).isDirectory()
      ? file === path.dirname(destination) ? "destination" : file === path.dirname(source) ? "source" : undefined
      : undefined;
    if (barrier) {
      events.push(`${barrier}:attempt`);
      if (fault === barrier) throw error;
    }
    sync(fd);
    if (barrier) events.push(`${barrier}:success`);
  });
  vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, data, options) => {
    if ((data as { state?: string }).state === "moved") {
      events.push("moved-receipt:attempt");
      if (fault === "moved-receipt") throw error;
    }
    write(file, data, options);
  });
  return { events, error, prepared: () => prepared };
};

describe("common worktree cleanup whole-root trace custody", () => {
  it.skipIf(process.platform === "win32").each([0o775, 0o777, 0o1777, 0o700])(
    "P1 archive ancestor manager create-to-cleanup native mode %s settles the lease only after durable whole-root custody", async mode => {
      const f = fixture(mode), lease = await f.create(), ino = fs.lstatSync(lease.path).ino;
      const file = path.join(lease.path, "sessions/raw.JSONL"), fileIno = fs.lstatSync(file).ino, fd = fs.openSync(file, "a");
      const unsafe = mode === 0o775 || mode === 0o777;
      const expected = unsafe ? path.join(process.env.XDG_STATE_HOME!, "pi-fabric", "trace-archive")
        : path.join(f.root, ".pi", "fabric", "worktrees.trace-archive");
      const write = atomicWrites.writeJsonAtomic;
      let row: (Receipt & { archiveRefusals?: unknown[] }) | undefined, receiptFile: string | undefined;
      vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, data, options) => {
        write(file, data, options);
        if ((data as Receipt).source === lease.path && (data as Receipt).state === "moved") {
          // The lease is live through publication's real durability barriers.
          expect(f.manager.get("same-id")).toEqual(lease);
          row = data as typeof row; receiptFile = String(file);
        }
      });
      try {
        expect(f.manager.get("same-id")).toEqual(lease);
        await expect(f.manager.cleanup("same-id", true)).resolves.toBe(true);
        expect(row).toBeDefined(); expect(receiptFile).toBeDefined();
        expect(path.dirname(path.dirname(row!.destination))).toBe(expected);
        expect(JSON.parse(fs.readFileSync(receiptFile!, "utf8"))).toMatchObject({ state: "moved", source: lease.path });
        expect(JSON.parse(fs.readFileSync(path.join(path.dirname(receiptFile!), "custody.prepared.json"), "utf8"))).toMatchObject({ state: "prepared", source: lease.path });
        expect(fs.lstatSync(row!.destination).ino).toBe(ino); verify(row!.destination);
        expect(fs.lstatSync(path.join(row!.destination, "sessions/raw.JSONL")).ino).toBe(fileIno);
        expect(fs.lstatSync(path.dirname(row!.destination)).mode & 0o777).toBe(0o700);
        expect(fs.lstatSync(f.root).mode & 0o7777).toBe(mode);
        if (unsafe) {
          expect(row!.archiveRefusals).toHaveLength(2);
          expect(fs.existsSync(path.join(f.root, ".pi", "fabric", "worktrees.trace-archive"))).toBe(false);
        }
        fs.writeSync(fd, "late-original-tail");
        expect(fs.readFileSync(path.join(row!.destination, "sessions/raw.JSONL"))).toEqual(Buffer.concat([payloads.get("sessions/raw.JSONL")!, Buffer.from("late-original-tail")]));
        expect(fs.fstatSync(fd).ino).toBe(fileIno);
        expect(fs.existsSync(lease.path)).toBe(false); expect(f.manager.get("same-id")).toBeUndefined();
        f.noGitDelete();
      } finally { fs.closeSync(fd); }
    });

  it("renames the entire root, preserving inode/raw bytes and leaving Git debt visible", async () => {
    const f = fixture(); const lease = await f.create();
    const ino = fs.statSync(lease.path).ino;
    const warnings = vi.spyOn(console, "warn");
    const copy = vi.spyOn(fs, "cpSync"), remove = vi.spyOn(fs, "rmSync");
    await expect(f.manager.cleanup("same-id", true)).resolves.toBe(true);
    f.noGitDelete();
    const receipt = moved(f.root, lease.path);
    expect(fs.existsSync(lease.path)).toBe(false);
    expect(fs.statSync(receipt.destination).ino).toBe(ino);
    if (process.platform !== "win32") expect(fs.statSync(path.dirname(receipt.destination)).mode & 0o777).toBe(0o700);
    expect(warnings.mock.calls.flat().join("\n")).toMatch(/registration cleanup deferred/);
    expect(warnings.mock.calls.flat().join("\n")).toMatch(/branch deletion deferred/);
    expect(copy).not.toHaveBeenCalled();
    // Receipt staging can unlink its OWN now-missing temporary, never a root.
    expect(remove.mock.calls.some(([target, options]) => String(target) === lease.path ||
      String(target).startsWith(receipt.destination) || options?.recursive)).toBe(false);
    expect(f.manager.get("same-id")).toBeUndefined();
  });

  it("keeps no-lease false and missing-worktree metadata cleanup true without Git destruction", async () => {
    const f = fixture();
    await expect(f.manager.cleanup("unknown", true)).resolves.toBe(false);
    const lease = await f.create();
    // Retain the fixture ourselves to exercise missing canonical metadata, not rm.
    fs.renameSync(lease.path, path.join(f.root, "already-held"));
    await expect(f.manager.cleanup("same-id", true)).resolves.toBe(true);
    expect(f.manager.get("same-id")).toBeUndefined(); verify(path.join(f.root, "already-held")); f.noGitDelete();
  });

  it("allocates fresh custody for a repeated ID without clobbering earlier raw trees", async () => {
    const f = fixture(); const first = await f.create();
    await f.manager.cleanup("same-id"); const a = moved(f.root, first.path);
    await f.create(); await f.manager.cleanup("same-id", true);
    const rows = receipts(f.root).filter(row => row.state === "moved");
    expect(rows).toHaveLength(2); expect(rows[1]!.destination).not.toBe(rows[0]!.destination);
    verify(a.destination); rows.forEach(row => verify(row.destination)); f.noGitDelete();
  });

  it("EXDEV throws visibly and retains original inode/lease without copy or delete fallback", async () => {
    const f = fixture(); const lease = await f.create(); const ino = fs.statSync(lease.path).ino;
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (String(source) === lease.path) throw Object.assign(new Error("cross-device refusal"), { code: "EXDEV" });
      rename(source, destination);
    });
    const warnings = vi.spyOn(console, "warn"), copy = vi.spyOn(fs, "cpSync"), remove = vi.spyOn(fs, "rmSync");
    await expect(f.manager.cleanup("same-id", true)).rejects.toThrow(/EXDEV/);
    verify(lease.path); expect(fs.statSync(lease.path).ino).toBe(ino); expect(f.manager.get("same-id")).toEqual(lease);
    expect(receipts(f.root)).toContainEqual(expect.objectContaining({ source: lease.path, state: "rename-failed" }));
    expect(warnings.mock.calls.flat().join("\n")).toMatch(/EXDEV/);
    expect(copy).not.toHaveBeenCalled();
    expect(remove.mock.calls.some(([target, options]) => String(target) === lease.path || options?.recursive)).toBe(false); f.noGitDelete();
  });

  it("confirmed cross-device shadow is deferred, keeps the lease and original, and never reports cleaned", async () => {
    const f = fixture(); const lease = await f.create(); const ino = fs.lstatSync(lease.path).ino;
    const lstat = fs.lstatSync, rename = fs.renameSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: { bigint?: boolean; throwIfNoEntry?: boolean }) => {
      const stat = lstat(file, options as { bigint: false; throwIfNoEntry: false });
      return String(file) === lease.path && !options?.bigint && stat
        ? Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { dev: stat.dev + 1 }) : stat;
    }) as typeof fs.lstatSync);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === lease.path) throw Object.assign(new Error("EXDEV mounted worktree"), { code: "EXDEV" });
      rename(from, to);
    });
    await expect(f.manager.cleanup("same-id", true)).rejects.toMatchObject({ code: "CLEANUP_CUSTODY_DEFERRED",
      cleanupStatus: { state: process.platform === "linux" ? "held-in-place" : "copy-failed", originalRetained: true, source: lease.path } });
    expect(f.manager.get("same-id")).toEqual(lease); expect(fs.lstatSync(lease.path).ino).toBe(ino); verify(lease.path); f.noGitDelete();
    const rows = receipts(f.root); expect(rows).toHaveLength(1);
    expect(rows[0]!.state).not.toBe("moved");
    if (process.platform === "linux") verify(rows[0]!.destination);
  });

  it("fresh container allocation failure leaves the original untouched", async () => {
    const f = fixture(); const lease = await f.create();
    vi.spyOn(fs, "mkdtempSync").mockImplementation(() => { throw new Error("allocation denied"); });
    await expect(f.manager.cleanup("same-id", true)).rejects.toThrow(/allocation denied/);
    verify(lease.path); expect(f.manager.get("same-id")).toEqual(lease); f.noGitDelete();
  });

  it.each([false, true])("never rolls back or recaptures a new canonical occupant after rename (receipt failure=%s)", async failReceipt => {
    const f = fixture(); const lease = await f.create(); const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      rename(source, destination);
      if (String(source) === lease.path) {
        fs.mkdirSync(source); fs.writeFileSync(path.join(String(source), "new-owner.jsonl"), "new canonical bytes\n");
      }
    });
    if (failReceipt) {
      const write = atomicWrites.writeJsonAtomic;
      vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, data, options) => {
        if ((data as { state?: string }).state === "moved") throw new Error("receipt publication failed");
        write(file, data, options);
      });
      await expect(f.manager.cleanup("same-id", true)).rejects.toThrow(/retained at.*receipt/);
    } else await f.manager.cleanup("same-id", true);
    expect(fs.readFileSync(path.join(lease.path, "new-owner.jsonl"), "utf8")).toBe("new canonical bytes\n");
    const row = receipts(f.root)[0]!; verify(row.destination);
    await expect(f.manager.cleanup("same-id", true)).resolves.toBe(false);
    expect(fs.readFileSync(path.join(lease.path, "new-owner.jsonl"), "utf8")).toBe("new canonical bytes\n"); f.noGitDelete();
  });

  it.skipIf(process.platform === "win32")("preserves symlinks, FIFO inode and post-rename open-FD appends", async () => {
    const f = fixture(); const lease = await f.create();
    const outside = path.join(f.root, "outside"); fs.mkdirSync(outside); seed(outside);
    fs.symlinkSync(outside, path.join(lease.path, "linked"), "dir");
    const fifo = path.join(lease.path, "capture.JSONL.pipe"); execFileSync("mkfifo", [fifo]);
    const fifoIno = fs.lstatSync(fifo).ino;
    const fd = fs.openSync(path.join(lease.path, "sessions/raw.JSONL"), "a");
    try {
      await f.manager.cleanup("same-id", true); const row = moved(f.root, lease.path);
      fs.writeSync(fd, "late append");
      expect(fs.readFileSync(path.join(row.destination, "sessions/raw.JSONL"))).toEqual(Buffer.concat([payloads.get("sessions/raw.JSONL")!, Buffer.from("late append")]));
      expect(fs.lstatSync(path.join(row.destination, "linked")).isSymbolicLink()).toBe(true); verify(outside);
      expect(fs.lstatSync(path.join(row.destination, "capture.JSONL.pipe")).isFIFO()).toBe(true);
      expect(fs.lstatSync(path.join(row.destination, "capture.JSONL.pipe")).ino).toBe(fifoIno); f.noGitDelete();
    } finally { fs.closeSync(fd); }
  });

  it("the shared helper retains a file root with a durable timed receipt and immutable preparation", () => {
    const f = fixture(); const source = path.join(f.root, "result.jsonl");
    fs.writeFileSync(source, "raw result\n"); const ino = fs.statSync(source).ino;
    const barriers: Array<{ ino: number; directory: boolean }> = [];
    const sync = fs.fsyncSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const stat = fs.fstatSync(fd); barriers.push({ ino: stat.ino, directory: stat.isDirectory() }); sync(fd);
    });
    const before = Date.now();
    const row = retainCleanupRoot(source, path.join(f.root, "held", "nested"), "file-agent", "resident result");
    expect(row).toBeDefined(); expect(fs.statSync(row!.destination).ino).toBe(ino);
    expect(fs.readFileSync(row!.destination, "utf8")).toBe("raw result\n");
    const receipt = JSON.parse(fs.readFileSync(row!.receipt, "utf8"));
    expect(receipt).toMatchObject({ source, destination: row!.destination, id: "file-agent", reason: "resident result", state: "moved" });
    expect(receipt.time).toBeGreaterThanOrEqual(before); expect(receipt.time).toBeLessThanOrEqual(Date.now());
    expect(JSON.parse(fs.readFileSync(path.join(path.dirname(row!.receipt), "custody.prepared.json"), "utf8"))).toMatchObject({ state: "prepared", source });
    expect(barriers.some(row => !row.directory)).toBe(true);
    if (process.platform !== "win32") {
      for (const directory of [path.dirname(row!.receipt), path.join(f.root, "held", "nested"), path.join(f.root, "held"), f.root]) {
        expect(barriers).toContainEqual({ ino: fs.statSync(directory).ino, directory: true });
      }
    }
    f.noGitDelete();
  });

  it.skipIf(process.platform === "win32").each(["moved-receipt", "source"] as const)(
    "destination barrier succeeds before source barrier or moved-receipt failure (%s)", async fault => {
      const f = fixture(); const lease = await f.create(); const ino = fs.statSync(lease.path).ino;
      const probe = faultAfterRename(lease.path, fault);
      const warnings = vi.spyOn(console, "warn"), copy = vi.spyOn(fs, "cpSync"), remove = vi.spyOn(fs, "rmSync");
      let failure: (Error & { retainedRoot?: NonNullable<ReturnType<typeof retainCleanupRoot>> }) | undefined;
      try { await f.manager.cleanup("same-id", true); } catch (error) { failure = error as typeof failure; }
      expect(failure).toMatchObject({ cause: probe.error, retainedRoot: { source: lease.path } });
      expect(probe.events).toEqual(fault === "moved-receipt"
        ? ["rename", "destination:attempt", "destination:success", "source:attempt", "source:success", "moved-receipt:attempt"]
        : ["rename", "destination:attempt", "destination:success", "source:attempt"]);
      const row = failure!.retainedRoot!;
      verify(row.destination); expect(fs.statSync(row.destination).ino).toBe(ino);
      const prepared = fs.readFileSync(path.join(path.dirname(row.receipt), "custody.prepared.json"));
      expect(prepared).toEqual(probe.prepared());
      expect(JSON.parse(prepared.toString())).toMatchObject({ source: lease.path, destination: row.destination,
        id: "same-id", reason: "agent cleanup whole-root custody", time: expect.any(Number), state: "prepared" });
      expect(JSON.parse(fs.readFileSync(row.receipt, "utf8")).state).toBe("prepared");
      expect(warnings.mock.calls.flat().join("\n")).toMatch(/retained at.*receipt/);
      expect(f.manager.get("same-id")).toBeUndefined();
      await expect(f.manager.cleanup("same-id", true)).resolves.toBe(false);
      expect(fs.readFileSync(path.join(lease.path, "replacement.jsonl"), "utf8")).toBe("replacement owner\n");
      expect(copy).not.toHaveBeenCalled();
      expect(remove.mock.calls.some(([target, options]) => String(target) === lease.path ||
        String(target).startsWith(row.destination) || options?.recursive)).toBe(false); f.noGitDelete();
    });

  it.skipIf(process.platform === "win32")("destination barrier EIO keeps held bytes/preparation without source barrier, moved receipt or retry capture", async () => {
    const f = fixture(); const lease = await f.create(); const ino = fs.statSync(lease.path).ino;
    const probe = faultAfterRename(lease.path, "destination");
    const warnings = vi.spyOn(console, "warn"), copy = vi.spyOn(fs, "cpSync"), remove = vi.spyOn(fs, "rmSync");
    let failure: (Error & { retainedRoot?: NonNullable<ReturnType<typeof retainCleanupRoot>> }) | undefined;
    try { await f.manager.cleanup("same-id", true); } catch (error) { failure = error as typeof failure; }
    expect(failure).toMatchObject({ cause: probe.error, retainedRoot: { source: lease.path } });
    expect(probe.events).toEqual(["rename", "destination:attempt"]);
    const row = failure!.retainedRoot!;
    verify(row.destination); expect(fs.statSync(row.destination).ino).toBe(ino);
    const prepared = fs.readFileSync(path.join(path.dirname(row.receipt), "custody.prepared.json"));
    expect(prepared).toEqual(probe.prepared());
    expect(JSON.parse(prepared.toString())).toMatchObject({ source: lease.path, destination: row.destination, state: "prepared" });
    expect(JSON.parse(fs.readFileSync(row.receipt, "utf8")).state).toBe("prepared");
    expect(warnings.mock.calls.flat().join("\n")).toMatch(/retained at.*EIO/);
    expect(f.manager.get("same-id")).toBeUndefined();
    await expect(f.manager.cleanup("same-id", true)).resolves.toBe(false);
    expect(fs.readFileSync(path.join(lease.path, "replacement.jsonl"), "utf8")).toBe("replacement owner\n");
    expect(copy).not.toHaveBeenCalled();
    expect(remove.mock.calls.some(([target, options]) => String(target) === lease.path ||
      String(target).startsWith(row.destination) || options?.recursive)).toBe(false); f.noGitDelete();
  });

  it("a failed durable preparation barrier refuses before moving any source", async () => {
    const f = fixture(); const lease = await f.create(); const rename = vi.spyOn(fs, "renameSync");
    vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("durability barrier failed"); });
    await expect(f.manager.cleanup("same-id", true)).rejects.toThrow(/durability barrier failed/);
    verify(lease.path); expect(rename.mock.calls.some(([source]) => String(source) === lease.path)).toBe(false); f.noGitDelete();
  });

  it.skipIf(process.platform === "win32")("refuses even a dangling root symlink without resolving, moving or deleting its entry", () => {
    const f = fixture(); const source = path.join(f.root, "raw-link"), archive = path.join(f.root, "held");
    fs.symlinkSync("absent-target.JSONL", source); const ino = fs.lstatSync(source).ino;
    // A dangling target may become live before rename; moving its link alone is
    // not referent custody. Nested links inside an ordinary root remain allowed.
    const link = fs.readlinkSync(source, { encoding: "buffer" });
    let failure: { cleanupStatus?: { receipt: string; destination: string } } | undefined;
    try { retainCleanupRoot(source, archive, "symlink-agent"); } catch (error) { failure = error as typeof failure; }
    expect(failure).toMatchObject({ code: "CLEANUP_CUSTODY_DEFERRED", cleanupStatus: { state: "held-in-place", originalRetained: true, source } });
    const row = JSON.parse(fs.readFileSync(failure!.cleanupStatus!.receipt, "utf8"));
    expect(row).toMatchObject({ state: "held-in-place", reason: "root-is-symlink", entryPath: source, readlink: { encoding: "base64", bytes: link.toString("base64") } });
    expect(row.verifiedCopyPath).toBeUndefined(); expect(fs.existsSync(failure!.cleanupStatus!.destination)).toBe(false);
    expect(fs.lstatSync(source).ino).toBe(ino);
    expect(fs.readlinkSync(source, { encoding: "buffer" })).toEqual(link); f.noGitDelete();
  });

  it("refuses a live root symlink to Git-reachable raw trees without consuming the lease or touching the referent", async () => {
    const f = fixture(); const lease = await f.create();
    fs.mkdirSync(path.join(f.root, ".git"));
    const target = path.join(f.root, "untracked-live-raw"); fs.renameSync(lease.path, target);
    fs.symlinkSync(target, lease.path, process.platform === "win32" ? "junction" : "dir");
    const rootIno = fs.lstatSync(lease.path).ino, targetIno = fs.statSync(target).ino;
    const file = path.join(target, "sessions/raw.JSONL"), fileIno = fs.statSync(file).ino;
    const fd = fs.openSync(file, "a"), warnings = vi.spyOn(console, "warn");
    const rename = vi.spyOn(fs, "renameSync"), copy = vi.spyOn(fs, "cpSync"), remove = vi.spyOn(fs, "rmSync");
    try {
      const link = fs.readlinkSync(lease.path, { encoding: "buffer" });
      const failure = await f.manager.cleanup("same-id", true).catch(error => error);
      expect(failure).toMatchObject({ code: "CLEANUP_CUSTODY_DEFERRED", cleanupStatus: { state: "held-in-place", originalRetained: true, source: lease.path } });
      const row = JSON.parse(fs.readFileSync(failure.cleanupStatus.receipt, "utf8"));
      expect(row).toMatchObject({ state: "held-in-place", reason: "root-is-symlink", entryPath: lease.path, readlink: { encoding: "base64", bytes: link.toString("base64") } });
      expect(failure.cleanupStatus.verifiedCopyPath).toBeUndefined(); expect(fs.existsSync(failure.cleanupStatus.destination)).toBe(false);
      expect(f.manager.get("same-id")).toEqual(lease); expect(receipts(f.root)).toEqual([]);
      expect(fs.realpathSync(lease.path)).toBe(target); verify(target); verify(lease.path);
      expect(fs.lstatSync(lease.path).ino).toBe(rootIno); expect(fs.statSync(target).ino).toBe(targetIno);
      expect(fs.statSync(file).ino).toBe(fileIno); expect(fs.fstatSync(fd).ino).toBe(fileIno);
      expect(path.relative(f.root, fs.realpathSync(lease.path))).toBe("untracked-live-raw");
      expect(warnings.mock.calls.flat().join("\n")).toMatch(/cleanup refused.*root symlink/);
      expect(warnings.mock.calls.flat().join("\n")).not.toMatch(/cleanup retained whole root/);
      expect(rename.mock.calls.some(([from]) => String(from) === lease.path)).toBe(false);
      expect(copy).not.toHaveBeenCalled();
      expect(remove.mock.calls.some(([file, options]) => String(file) === lease.path || String(file).startsWith(target) || options?.recursive)).toBe(false); f.noGitDelete();
      fs.writeSync(fd, "late-original-tail");
      expect(fs.readFileSync(file)).toEqual(Buffer.concat([payloads.get("sessions/raw.JSONL")!, Buffer.from("late-original-tail")]));
      await expect(f.manager.cleanup("same-id", true)).rejects.toThrow(/root symlink/);
      expect(f.manager.get("same-id")).toEqual(lease); f.noGitDelete();
    } finally { fs.closeSync(fd); }
  });

  it("refuses a root replaced by a live symlink during durable preparation and keeps lease and immutable receipt", async () => {
    const f = fixture(); const lease = await f.create(); const ino = fs.lstatSync(lease.path).ino;
    const target = path.join(f.root, "replacement-referent"), write = atomicWrites.writeJsonAtomic;
    let prepared: Buffer | undefined;
    vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, data, options) => {
      write(file, data, options);
      if (String(file).endsWith(`${path.sep}custody.json`) && (data as Receipt).state === "prepared") {
        prepared = fs.readFileSync(path.join(path.dirname(String(file)), "custody.prepared.json"));
        fs.renameSync(lease.path, target);
        fs.symlinkSync(target, lease.path, process.platform === "win32" ? "junction" : "dir");
      }
    });
    await expect(f.manager.cleanup("same-id", true)).rejects.toThrow(/changed before rename/);
    const row = receipts(f.root)[0]!; expect(row.state).toBe("rename-failed");
    expect(fs.readFileSync(path.join(path.dirname(row.destination), "custody.prepared.json"))).toEqual(prepared);
    expect(fs.lstatSync(row.destination, { throwIfNoEntry: false })).toBeUndefined();
    expect(fs.lstatSync(target).ino).toBe(ino); verify(target); verify(lease.path);
    expect(f.manager.get("same-id")).toEqual(lease); f.noGitDelete();
  });

  it("a committed root substitution never forgets the lease or reports cleaned on retry or ID reuse", async () => {
    const f = fixture(); const lease = await f.create(); const ino = fs.lstatSync(lease.path).ino;
    const target = path.join(f.root, "uncaptured-original"), rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === lease.path) {
        rename(lease.path, target);
        fs.symlinkSync(target, lease.path, process.platform === "win32" ? "junction" : "dir");
      }
      rename(from, to);
    });
    let failure: (Error & { movedEntry?: NonNullable<ReturnType<typeof retainCleanupRoot>> }) | undefined;
    try { await f.manager.cleanup("same-id", true); } catch (error) { failure = error as typeof failure; }
    expect(failure?.message).toMatch(/changed during rename.*custody unproven/);
    expect("retainedRoot" in failure!).toBe(false); expect("cleanupStatus" in failure!).toBe(false);
    expect(f.manager.get("same-id")).toEqual(lease);
    const row = failure!.movedEntry!;
    expect(fs.lstatSync(row.destination).isSymbolicLink()).toBe(true);
    expect(JSON.parse(fs.readFileSync(row.receipt, "utf8")).state).toBe("prepared");
    expect(fs.lstatSync(target).ino).toBe(ino); verify(target);
    fs.mkdirSync(lease.path); fs.writeFileSync(path.join(lease.path, "new-owner.JSONL"), "new owner\n");
    await expect(f.manager.cleanup("same-id", true)).rejects.toBe(failure);
    await expect(f.create()).rejects.toBe(failure);
    expect(f.manager.get("same-id")).toEqual(lease); verify(target);
    expect(fs.readFileSync(path.join(lease.path, "new-owner.JSONL"), "utf8")).toBe("new owner\n"); f.noGitDelete();
  });

  it.each((["manager", "resident"] as const).flatMap(owner => (["substitution", "receipt-eio"] as const).map(fault => ({ owner, fault }))))("actual $owner cleanup first failure and same-id retry preserve the custody boundary ($fault)", async ({ owner, fault }) => {
    const f = fixture(), consumed = vi.fn(); installInProcessResidentFence();
    fs.mkdirSync(path.join(f.root, ".git")); // Original raw target stays Git-reachable on the substitution failure.
    let source: string, id: string, cleanup: () => Promise<unknown>, close: () => Promise<void>;
    if (owner === "manager") {
      vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 64));
      const manager = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, budgetUsd: 0, sessionExport: false }, {
        runRoot: path.join(f.root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), onResultConsumed: consumed,
      });
      const result = await manager.run({ task: "F1 real manager custody", transport: "process", extensions: false });
      id = result.id; source = manager.runDirectory(id)!;
      cleanup = () => manager.cleanup(id); close = async () => { manager.config.retainRuns = true; await manager.close(); };
    } else {
      const rootId = "session:F1-custody", meshRoot = path.join(f.root, "mesh");
      const config: ResidentHostConfig = { format: 1, rootId, sessionId: "F1-custody", cwd: f.root, projectRoot: f.root,
        meshRoot, actorRoot: path.join(f.root, "actors"), residencyRoot: residentRoot(meshRoot, rootId), fullCodeMode: true,
        agents: { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false }, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" };
      const mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
      const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: rootId, rootId,
        identity: { id: rootId, name: "F1 fixture Main", kind: "main", sessionId: config.sessionId }, reapDeadHosts: false });
      const client = new ResidencyClient({ config, mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget, onResultConsumed: consumed });
      id = "f".repeat(32); source = path.join(config.residencyRoot, "runs", id);
      const record = heapRunResult(id, 64); fs.mkdirSync(source, { recursive: true });
      fs.writeFileSync(path.join(source, "status.json"), JSON.stringify(record));
      const metadata = path.join(config.residencyRoot, "agents", `${id}.json`); fs.mkdirSync(path.dirname(metadata), { recursive: true });
      fs.writeFileSync(metadata, JSON.stringify({ format: 1, rootId, id, runDirectory: source, createdAt: 1, updatedAt: 2, handle: record }));
      const result = residentResultPath(config.residencyRoot, id); fs.mkdirSync(path.dirname(result), { recursive: true }); fs.writeFileSync(result, JSON.stringify(record));
      cleanup = () => client.cleanupAgent(id); close = async () => { await client.close(); await participants.close(); };
    }
    seed(source); consumed.mockClear(); const ino = fs.lstatSync(source).ino, original = path.join(f.root, "original-raw");
    const rename = fs.renameSync, write = atomicWrites.writeJsonAtomic; let destination = "", once = false;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === source) {
        if (fault === "substitution") { rename(source, original); fs.symlinkSync(original, source, process.platform === "win32" ? "junction" : "dir"); }
        destination = String(to);
      }
      rename(from, to);
    });
    vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, data, options) => {
      if (fault === "receipt-eio" && !once && (data as Receipt).source === source && (data as Receipt).state === "moved") {
        once = true; throw Object.assign(new Error("F1 ordinary moved receipt EIO"), { code: "EIO" });
      }
      write(file, data, options);
    });
    try {
      await expect(cleanup()).rejects.toThrow(fault === "substitution" ? /custody unproven/ : /receipt EIO/);
      expect(consumed).not.toHaveBeenCalled();
      expect(JSON.parse(fs.readFileSync(path.join(path.dirname(destination), "custody.prepared.json"), "utf8")).state).toBe("prepared");
      if (fault === "substitution") {
        verify(original); expect(fs.lstatSync(original).ino).toBe(ino);
        await expect(cleanup()).rejects.toThrow(); expect(consumed).not.toHaveBeenCalled(); verify(original);
        fs.mkdirSync(source); fs.writeFileSync(path.join(source, "replacement.JSONL"), "new canonical owner\n");
        await expect(cleanup()).rejects.toThrow(); expect(consumed).not.toHaveBeenCalled();
        expect(fs.readFileSync(path.join(source, "replacement.JSONL"), "utf8")).toBe("new canonical owner\n"); verify(original);
      } else {
        verify(destination); expect(fs.lstatSync(destination).ino).toBe(ino);
        await expect(cleanup()).resolves.toMatchObject({ cleaned: true }); verify(destination);
      }
      f.noGitDelete();
    } finally { await close(); }
  });

  it.each([false, true])("actual AgentManager setup-failure rollback calls common cleanup without public cleanup (EXDEV=%s)", async exdev => {
    const f = fixture(); f.failSetup(); const warnings = vi.spyOn(console, "warn");
    if (exdev) {
      const rename = fs.renameSync;
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        if (f.created.includes(String(source))) throw Object.assign(new Error("rollback cross-device refusal"), { code: "EXDEV" });
        rename(source, destination);
      });
    }
    const manager = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0 }, {
      runRoot: path.join(f.root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    const publicCleanup = vi.spyOn(manager, "cleanup"); // call-through only
    try {
      await expect(manager.spawn({ task: "automatic rollback", transport: "process", worktree: true,
        worktreeSetup: "fixture setup rejection" })).rejects.toThrow(/Worktree setup command exited/);
      expect(publicCleanup).not.toHaveBeenCalled(); expect(f.created).toHaveLength(1); f.noGitDelete();
      if (exdev) {
        verify(f.created[0]!); expect(warnings.mock.calls.flat().join("\n")).toMatch(/EXDEV/);
        expect(receipts(f.root)).toContainEqual(expect.objectContaining({ state: "rename-failed" }));
      } else { moved(f.root, f.created[0]!); expect(fs.existsSync(f.created[0]!)).toBe(false); }
    } finally { await manager.close(); }
  });
});
