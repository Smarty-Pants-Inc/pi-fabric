import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidencyClient } from "../src/residency/client.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { executeFile } from "../src/agents/transports/process-utils.js";
import { residentResultPath, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { markUnresolvedWorker } from "../src/storage/retention.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// Keep: data safety at the public recovered-resident cleanup boundary. Real
// filesystem objects, bytes, inodes and open FDs; Git is RECORDCALL only, never executed.
vi.mock("../src/agents/transports/process-utils.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/agents/transports/process-utils.js")>(),
  executeFile: vi.fn(),
}));

const fixtures: Array<{ root: string; client: ResidencyClient; participants: ParticipantDirectory }> = [];
const sha256 = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
const write = (file: string, bytes: string | Buffer) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, bytes, { mode: 0o600 });
};
interface CustodyReceipt {
  source: string;
  destination: string;
  state: "prepared" | "moved" | "rename-failed";
  deferred: string[];
}
const receipts = (archive: string): CustodyReceipt[] => fs.readdirSync(archive, { withFileTypes: true })
  .filter(entry => entry.isDirectory())
  .map(entry => JSON.parse(fs.readFileSync(path.join(archive, entry.name, "custody.json"), "utf8")) as CustodyReceipt);
const custodied = (archive: string, source: string): string => {
  const receipt = receipts(archive).find(entry => entry.source === source && entry.state === "moved");
  expect(receipt, `missing moved-root custody receipt for ${source}`).toBeDefined();
  const container = path.dirname(receipt!.destination);
  expect(path.dirname(container)).toBe(archive);
  if (process.platform !== "win32") expect(fs.statSync(container).mode & 0o777).toBe(0o700);
  return receipt!.destination;
};

const fixture = (options: { retainRuns?: boolean; absent?: boolean; worktree?: boolean } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-cleanup-archive-"));
  const rootId = "session:cleanup-trace-archive";
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "cleanup-trace-archive", cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: options.retainRuns ?? true },
    mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const identity = { id: rootId, name: "fixture Main", kind: "main" as const, sessionId: config.sessionId };
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: rootId, rootId, identity, reapDeadHosts: false });
  const consumed = vi.fn();
  const client = new ResidencyClient({ config, mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget, onResultConsumed: consumed });
  fixtures.push({ root, client, participants });
  const id = "a".repeat(32);
  const run = path.join(config.residencyRoot, "runs", id);
  const worktree = path.join(root, ".pi", "fabric", "worktrees", id);
  const branch = `pi-fabric/fixture-${id.slice(0, 8)}`;
  if (options.worktree) fs.mkdirSync(worktree, { recursive: true, mode: 0o700 });
  const record = {
    id, name: "exited fixture worker", status: "completed", text: "exact final result", task: "work",
    runner: "pi", transport: "process", sessionId: "2147483647", cwd: options.worktree ? worktree : root,
    startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  };
  const metadata = path.join(config.residencyRoot, "agents", `${id}.json`);
  const result = residentResultPath(config.residencyRoot, id);
  write(metadata, JSON.stringify({ format: 1, rootId, id, runDirectory: run, createdAt: 1, updatedAt: 2,
    handle: { ...record, ...(options.worktree ? { worktree, branch } : {}) }, worktreeGitRoot: root }));
  write(result, JSON.stringify(record));
  if (!options.absent) write(path.join(run, "status.json"), JSON.stringify(record));
  const archive = `${config.residencyRoot}.trace-archive`;
  const decisions = path.join(config.residencyRoot, "decisions");
  const originals = new Map<string, Buffer>();
  const identities = new Map<string, { dev: number; ino: number }>();
  const add = (relative: string, base = run) => {
    const bytes = Buffer.from(`${path.basename(base)}/${relative}\0raw\r\n\xff\xfe`, "latin1");
    const file = path.join(base, relative);
    write(file, bytes); originals.set(file, bytes);
    const { dev, ino } = fs.lstatSync(file); identities.set(file, { dev, ino });
    return bytes;
  };
  const addTraces = () => {
    for (const name of ["session.jsonl", "events.ndjson", "session.jsonl.20261001.bak", "backups/old.ndjson.backup", "nested/child/raw.jsonl.reclaimed"])
      add(name);
    write(path.join(run, "nested", "child", "status.json"), JSON.stringify({ ...record, id: "b".repeat(32) }));
  };
  const expectMetadata = () => {
    expect(fs.existsSync(metadata)).toBe(true); expect(fs.existsSync(result)).toBe(true);
    expect(client.hasAgent(id)).toBe(true); expect(consumed).not.toHaveBeenCalled();
  };
  const expectOriginals = () => {
    for (const [file, bytes] of originals) {
      expect(fs.readFileSync(file), file).toEqual(bytes);
      expect(fs.lstatSync(file)).toMatchObject(identities.get(file)!);
    }
    expectMetadata();
  };
  const expectCustody = () => {
    for (const [file, bytes] of originals) {
      const source = file.startsWith(`${run}${path.sep}`) ? run : worktree;
      const archived = path.join(custodied(archive, source), path.relative(source, file));
      expect(fs.readFileSync(archived), file).toEqual(bytes);
      expect(sha256(fs.readFileSync(archived))).toBe(sha256(bytes));
      expect(fs.lstatSync(archived), file).toMatchObject(identities.get(file)!);
    }
  };
  const recordGit = () => vi.mocked(executeFile).mockImplementation(async (command, args) => {
    expect(command).toBe("git");
    expect([...args]).toEqual(["worktree", "list", "--porcelain"]);
    return { stdout: `worktree ${worktree}\nHEAD fixture\nbranch refs/heads/${branch}\n`, stderr: "" };
  });
  return { root, config, client, id, run, metadata, result, archive, decisions, worktree, branch, record, consumed,
    originals, add, addTraces, expectMetadata, expectOriginals, expectCustody, recordGit };
};

beforeEach(() => {
  installInProcessResidentFence();
  vi.mocked(executeFile).mockReset().mockRejectedValue(new Error("unexpected fixture Git command"));
});
afterEach(async () => {
  vi.restoreAllMocks();
  for (const { root, client, participants } of fixtures.splice(0)) {
    await client.close(); await participants.close();
    fs.rmSync(root, { recursive: true, force: true }); // Only this test's private fixture teardown.
  }
});

describe("ResidencyClient.cleanupAgent whole-root custody without deletion", () => {
  it.each([true, false])("moves full raw/nested/backup bytes with identical inodes (retainRuns=%s)", async retainRuns => {
    const f = fixture({ retainRuns }); f.addTraces();
    const rootIdentity = fs.lstatSync(f.run);
    const remove = vi.spyOn(fs, "rmSync");
    const copy = vi.spyOn(fs, "copyFileSync");
    await expect(f.client.cleanupAgent(f.id)).resolves.toMatchObject({ cleaned: true });
    const moved = custodied(f.archive, f.run);
    expect(fs.lstatSync(moved)).toMatchObject({ dev: rootIdentity.dev, ino: rootIdentity.ino });
    f.expectCustody();
    expect(copy).not.toHaveBeenCalled();
    expect(remove.mock.calls.some(([file]) => String(file) === f.run || String(file).startsWith(f.run + path.sep))).toBe(false);
    for (const file of [f.run, f.metadata, f.result]) expect(fs.existsSync(file), file).toBe(false);
    expect(f.client.hasAgent(f.id)).toBe(false);
    expect(f.consumed).toHaveBeenCalledExactlyOnceWith(f.id);
    expect(executeFile).not.toHaveBeenCalled();
  });

  it("keeps original traces, metadata and result without committing if the archive is unavailable", async () => {
    const f = fixture(); f.addTraces(); write(f.archive, "archive is unavailable");
    const warn = vi.spyOn(diagnostics, "warn");
    await expect(f.client.cleanupAgent(f.id)).rejects.toThrow();
    f.expectOriginals(); expect(warn).toHaveBeenCalled();
    expect(fs.existsSync(f.decisions)).toBe(false); expect(executeFile).not.toHaveBeenCalled();
  });

  it.each(["EXDEV", "EACCES"])("visibly refuses %s rename, keeping the original and known-ID commitment (no copy fallback)", async code => {
    const f = fixture(); f.addTraces();
    const failure = Object.assign(new Error(`injected ${code} root rename failure`), { code });
    const nativeRename = fs.renameSync.bind(fs);
    const rename = vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (String(source) === f.run) throw failure;
      nativeRename(source, destination);
    });
    const warn = vi.spyOn(diagnostics, "warn");
    const copy = vi.spyOn(fs, "copyFileSync");
    const error = await f.client.cleanupAgent(f.id).catch((error: Error) => error);
    expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id, operation: "cleanup" });
    expect((error as Error).message).toContain(code);
    expect((error as Error).message).toContain(f.run);
    expect((error as Error).message).toContain("Do not retry or reassign");
    expect(rename.mock.calls.some(([source]) => String(source) === f.run)).toBe(true);
    expect(warn.mock.calls.flat().join(" ")).toContain(code); expect(copy).not.toHaveBeenCalled();
    f.expectOriginals();
    expect(receipts(f.archive)).toMatchObject([{ source: f.run, state: "rename-failed" }]);
    const decisions = fs.readdirSync(f.decisions); expect(decisions).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(f.decisions, decisions[0]!), "utf8"))).toMatchObject({ state: "committed", id: f.id });
    expect(executeFile).not.toHaveBeenCalled();
  });

  it("keeps originals and an uncommitted request when fresh custody-directory preparation fails", async () => {
    const f = fixture(); f.addTraces();
    vi.spyOn(fs, "mkdtempSync").mockImplementation(() => { throw new Error("injected custody mkdtemp failure"); });
    await expect(f.client.cleanupAgent(f.id)).rejects.toThrow("injected custody mkdtemp failure");
    f.expectOriginals(); expect(fs.existsSync(f.decisions)).toBe(false);
  });

  it.each([false, true])("ordinary status-only or absent-run cleanup preserves receipt consumption (absent=%s)", async absent => {
    const f = fixture({ absent });
    if (absent) write(f.archive, "absent run needs no archive");
    await expect(f.client.cleanupAgent(f.id)).resolves.toMatchObject({ cleaned: true });
    if (!absent) expect(JSON.parse(fs.readFileSync(path.join(custodied(f.archive, f.run), "status.json"), "utf8"))).toEqual(f.record);
    for (const file of [f.run, f.metadata, f.result]) expect(fs.existsSync(file), file).toBe(false);
    expect(f.client.hasAgent(f.id)).toBe(false); expect(f.consumed).toHaveBeenCalledExactlyOnceWith(f.id);
  });

  it.each(["unresolved", "live root", "unknown root", "live descendant", "unknown descendant"] as const)("refuses a terminal saved result with %s before custody, fence or Git", async state => {
    const f = fixture({ worktree: true }); f.addTraces();
    if (state === "unresolved") markUnresolvedWorker(f.run, "known worker exit is unresolved");
    else {
      const root = state.endsWith("root");
      write(path.join(f.run, ...(root ? ["status.json"] : ["nested", "child", "status.json"])), JSON.stringify({
        ...f.record, id: root ? f.id : "b".repeat(32), sessionId: state.startsWith("live") ? String(process.pid) : undefined,
      }));
    }
    await expect(f.client.cleanupAgent(f.id, true)).rejects.toThrow(/worker may still be running|unknown (root|descendant) identity/);
    f.expectOriginals(); expect(fs.existsSync(f.worktree)).toBe(true);
    expect(fs.existsSync(f.archive)).toBe(false); expect(fs.existsSync(f.decisions)).toBe(false); expect(executeFile).not.toHaveBeenCalled();
  });

  it("moves all ignored/nontrace worktree evidence but only records Git list, visibly deferring registration/branch cleanup", async () => {
    const f = fixture({ worktree: true }); f.addTraces();
    for (const name of [".git", "src/main.ts", ".pi/ignored/session.jsonl", ".pi/ignored/events.ndjson.bak", ".pi/ignored/extensionless-evidence", "node_modules/pkg/cli.js"])
      f.add(name, f.worktree);
    f.recordGit(); const warn = vi.spyOn(diagnostics, "warn");
    const rootIdentity = fs.lstatSync(f.worktree);
    const outcome = await f.client.cleanupAgent(f.id, true);
    expect(outcome).toMatchObject({ cleaned: true, deferred: expect.arrayContaining([expect.stringContaining("registration"), expect.stringContaining(f.branch)]) });
    f.expectCustody();
    expect(fs.lstatSync(custodied(f.archive, f.worktree))).toMatchObject({ dev: rootIdentity.dev, ino: rootIdentity.ino });
    expect(vi.mocked(executeFile).mock.calls.map(([, args]) => args)).toEqual([["worktree", "list", "--porcelain"]]);
    expect(warn.mock.calls.flat().join(" ")).toContain("deferred");
    expect(receipts(f.archive).every(receipt => receipt.deferred.some(note => note.includes("registration")))).toBe(true);
    for (const file of [f.run, f.worktree, f.metadata, f.result]) expect(fs.existsSync(file), file).toBe(false);
  });

  it.skipIf(process.platform === "win32")("preserves actual symlinks literally and FIFOs in both roots without dereferencing/copying", async () => {
    const f = fixture({ worktree: true }); f.addTraces(); f.recordGit();
    const external = path.join(f.root, "external", "outside.ndjson"); write(external, "outside stays outside\n");
    const entries: Array<{ source: string; relative: string; ino: number; target?: string }> = [];
    for (const source of [f.run, f.worktree]) {
      f.add("real-dir/raw.jsonl.backup", source); f.add("node_modules/pkg/cli.js", source);
      fs.mkdirSync(path.join(source, "node_modules", ".bin"), { mode: 0o700 });
      for (const [relative, target, type] of [
        ["file-link.jsonl", external, "file"], ["directory-link", "real-dir", "dir"],
        ["broken.ndjson", "missing-raw.ndjson", "file"], ["node_modules/.bin/tool", "../pkg/cli.js", "file"],
      ] as const) {
        const file = path.join(source, relative); fs.symlinkSync(target, file, type);
        entries.push({ source, relative, ino: fs.lstatSync(file).ino, target });
      }
      const relative = "raw.fifo"; const file = path.join(source, relative);
      execFileSync("mkfifo", [file]); entries.push({ source, relative, ino: fs.lstatSync(file).ino });
    }
    await expect(f.client.cleanupAgent(f.id, true)).resolves.toMatchObject({ cleaned: true });
    f.expectCustody();
    for (const entry of entries) {
      const file = path.join(custodied(f.archive, entry.source), entry.relative);
      const stat = fs.lstatSync(file); expect(stat.ino).toBe(entry.ino);
      if (entry.target) { expect(stat.isSymbolicLink()).toBe(true); expect(fs.readlinkSync(file)).toBe(entry.target); }
      else expect(stat.isFIFO()).toBe(true);
    }
    expect(fs.readFileSync(external, "utf8")).toBe("outside stays outside\n");
  });

  it("keeps FD appends after each move and after cleanup, and never deletes new canonical old-path occupants", async () => {
    const f = fixture({ worktree: true }); f.addTraces(); f.recordGit();
    // Deliberate same-UID injection, NOT evidence of a legitimate product producer.
    // The live/unknown recorded-producer controls above remain independently enforced.
    const sources = [f.run, f.worktree];
    const opened = sources.map(source => {
      const relative = "open-raw.ndjson.backup"; const original = f.add(relative, source);
      const file = path.join(source, relative);
      return { source, relative, original, fd: fs.openSync(file, "a"), ino: fs.lstatSync(file).ino };
    });
    const nativeRename = fs.renameSync.bind(fs);
    const newRaw = Buffer.from("new canonical occupant must survive\n");
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      nativeRename(source, destination);
      const entry = opened.find(entry => entry.source === String(source));
      if (entry) {
        fs.writeSync(entry.fd, "after root rename\n");
        write(path.join(entry.source, "session.jsonl"), newRaw);
      }
    });
    try {
      await expect(f.client.cleanupAgent(f.id, true)).resolves.toMatchObject({ cleaned: true });
      for (const entry of opened) {
        fs.writeSync(entry.fd, "after cleanup returned\n");
        const file = path.join(custodied(f.archive, entry.source), entry.relative);
        expect(fs.lstatSync(file).ino).toBe(entry.ino);
        expect(fs.readFileSync(file)).toEqual(Buffer.concat([entry.original, Buffer.from("after root rename\nafter cleanup returned\n")]));
        expect(fs.readFileSync(path.join(entry.source, "session.jsonl"))).toEqual(newRaw);
      }
    } finally { for (const entry of opened) fs.closeSync(entry.fd); }
  });

  it("preserves a same-UID append before cleanup too (timing counterexample)", async () => {
    const f = fixture(); f.addTraces();
    const file = path.join(f.run, "session.jsonl"); fs.appendFileSync(file, "early raw append\n");
    f.originals.set(file, fs.readFileSync(file)); f.add("backups/early.ndjson.backup");
    await expect(f.client.cleanupAgent(f.id)).resolves.toMatchObject({ cleaned: true }); f.expectCustody();
  });

  it("uses a fresh custody root when the same canonical run ID is later reused, preserving the first tree", async () => {
    const f = fixture(); f.addTraces();
    const metadata = fs.readFileSync(f.metadata); const result = fs.readFileSync(f.result);
    const firstRaw = fs.readFileSync(path.join(f.run, "session.jsonl"));
    const first = await f.client.cleanupAgent(f.id);
    const firstTree = first.custody!.find(entry => entry.source === f.run)!.destination;
    write(f.metadata, metadata); write(f.result, result); write(path.join(f.run, "status.json"), JSON.stringify(f.record));
    const nextRaw = Buffer.from("new canonical run generation raw\n"); write(path.join(f.run, "session.jsonl"), nextRaw);
    const nextInode = fs.lstatSync(path.join(f.run, "session.jsonl")).ino;
    const second = await f.client.cleanupAgent(f.id);
    const secondTree = second.custody!.find(entry => entry.source === f.run)!.destination;
    expect(firstTree).not.toBe(secondTree);
    expect(fs.readFileSync(path.join(firstTree, "session.jsonl"))).toEqual(firstRaw);
    expect(fs.readFileSync(path.join(secondTree, "session.jsonl"))).toEqual(nextRaw);
    expect(fs.lstatSync(path.join(secondTree, "session.jsonl")).ino).toBe(nextInode);
    expect(receipts(f.archive)).toHaveLength(2); expect(f.consumed).toHaveBeenCalledTimes(2);
  });

  it("does not mutate Git or commit when archive preparation fails after registration validation", async () => {
    const f = fixture({ worktree: true }); f.addTraces(); f.add(".pi/ignored/trace.ndjson", f.worktree);
    write(f.archive, "archive is unavailable"); f.recordGit();
    await expect(f.client.cleanupAgent(f.id, true)).rejects.toThrow();
    f.expectOriginals(); expect(fs.existsSync(f.decisions)).toBe(false);
    expect(vi.mocked(executeFile).mock.calls.map(([, args]) => args)).toEqual([["worktree", "list", "--porcelain"]]);
  });

  it("retains both raw trees and records paths if the second root rename fails after run custody; no overwrite rollback", async () => {
    const f = fixture({ worktree: true }); f.addTraces(); f.add(".pi/ignored/extensionless-evidence", f.worktree); f.recordGit();
    const nativeRename = fs.renameSync.bind(fs);
    const failure = Object.assign(new Error("injected second root rename failure"), { code: "EXDEV" });
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (String(source) === f.worktree) throw failure;
      nativeRename(source, destination);
      if (String(source) === f.run) write(path.join(f.run, "new-occupant.jsonl"), "keep new occupant too\n");
    });
    const warn = vi.spyOn(diagnostics, "warn");
    const error = await f.client.cleanupAgent(f.id, true).catch((error: Error) => error);
    expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id, operation: "cleanup" });
    expect((error as Error).message).toContain("injected second root rename failure");
    const moved = custodied(f.archive, f.run);
    expect((error as Error).message).toContain(moved);
    expect((error as Error).message).toContain(f.worktree);
    for (const [file, bytes] of f.originals) {
      const current = file.startsWith(f.run + path.sep) ? path.join(moved, path.relative(f.run, file)) : file;
      expect(fs.readFileSync(current)).toEqual(bytes);
    }
    expect(fs.readFileSync(path.join(f.run, "new-occupant.jsonl"), "utf8")).toBe("keep new occupant too\n");
    f.expectMetadata(); expect(warn).toHaveBeenCalled();
    expect(receipts(f.archive)).toEqual(expect.arrayContaining([
      expect.objectContaining({ source: f.run, state: "moved" }), expect.objectContaining({ source: f.worktree, state: "rename-failed" }),
    ]));
    expect(vi.mocked(executeFile).mock.calls.map(([, args]) => args)).toEqual([["worktree", "list", "--porcelain"]]);
  });

  it("keeps post-commit receipt-consumption failure as known-ID uncertainty with custodied raw intact", async () => {
    const f = fixture(); f.addTraces();
    const nativeRemove = fs.rmSync.bind(fs);
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (String(file) === f.metadata) throw new Error("injected metadata consumption failure");
      nativeRemove(file, options);
    });
    const error = await f.client.cleanupAgent(f.id).catch((error: Error) => error);
    expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id, operation: "cleanup" });
    expect((error as Error).message).toContain("Do not retry or reassign");
    f.expectMetadata(); f.expectCustody();
  });

  it("retains registration and cancellation checks before custody and commitment", async () => {
    const f = fixture({ worktree: true }); f.addTraces();
    vi.mocked(executeFile).mockResolvedValue({ stdout: `worktree ${f.root}\n`, stderr: "" });
    await expect(f.client.cleanupAgent(f.id, true)).rejects.toThrow(/not registered/);
    f.expectOriginals(); expect(fs.existsSync(f.archive)).toBe(false); expect(fs.existsSync(f.decisions)).toBe(false);
    const controller = new AbortController();
    vi.mocked(executeFile).mockImplementation(async () => {
      controller.abort(new Error("cancelled during registration"));
      return { stdout: `worktree ${f.worktree}\n`, stderr: "" };
    });
    await expect(f.client.cleanupAgent(f.id, true, controller.signal)).rejects.toThrow("cancelled during registration");
    f.expectOriginals(); expect(fs.existsSync(f.archive)).toBe(false); expect(fs.existsSync(f.decisions)).toBe(false);
    expect(vi.mocked(executeFile).mock.calls.every(([, args]) => args[1] === "list")).toBe(true);
  });

  it("cancellation after the first move preserves custody and the untouched second root without rollback", async () => {
    const f = fixture({ worktree: true }); f.addTraces(); f.add("worktree.ndjson", f.worktree); f.recordGit();
    const controller = new AbortController(); const nativeRename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      nativeRename(source, destination);
      if (String(source) === f.run) controller.abort(new Error("cancelled after root move"));
    });
    const error = await f.client.cleanupAgent(f.id, true, controller.signal).catch((error: Error) => error);
    expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id });
    expect((error as Error).message).toContain("cancelled after root move");
    const moved = custodied(f.archive, f.run);
    for (const [file, bytes] of f.originals) expect(fs.readFileSync(file.startsWith(f.run + path.sep) ? path.join(moved, path.relative(f.run, file)) : file)).toEqual(bytes);
    f.expectMetadata(); expect(fs.existsSync(f.worktree)).toBe(true);
  });

  it("rejects an already-cancelled request and branch deletion without a worktree before any custody", async () => {
    const f = fixture(); f.addTraces(); const controller = new AbortController(); controller.abort(new Error("already cancelled"));
    await expect(f.client.cleanupAgent(f.id, false, controller.signal)).rejects.toThrow("already cancelled");
    await expect(f.client.cleanupAgent(f.id, true)).rejects.toThrow("no worktree branch to delete");
    f.expectOriginals(); expect(fs.existsSync(f.archive)).toBe(false); expect(fs.existsSync(f.decisions)).toBe(false);
  });
});
