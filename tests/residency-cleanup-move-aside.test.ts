import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidencyClient } from "../src/residency/client.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { executeFile } from "../src/agents/transports/process-utils.js";
import { residentCleanupErrorFields, residentResultPath, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { markUnresolvedWorker } from "../src/storage/retention.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { shortTmpdirName } from "../src/agents/short-tmpdir.js";
import { preserveCancellationOutcome } from "../src/async-settlement.js";

// Keep: data safety and partial-failure custody through the actual public offline
// cleanupAgent path. Git is RECORDCALL: destructive argv are recorded, NEVER run.
vi.mock("../src/agents/transports/process-utils.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/agents/transports/process-utils.js")>(),
  executeFile: vi.fn(),
}));

const { observed, ownedRoots } = vi.hoisted(() => ({ observed: new Set<string>(), ownedRoots: new Set<string>() }));
vi.mock("../src/core/atomic-write.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/core/atomic-write.js")>();
  return { ...actual, writeJsonAtomic: (...args: Parameters<typeof actual.writeJsonAtomic>) => {
    const result = actual.writeJsonAtomic(...args);
    const [file, value] = args;
    // Observe actual publication, including lexical WorktreeManager helper calls.
    // Never predict an allocator path or enumerate an unrelated host archive.
    if (path.basename(file) === "custody.json" && value && typeof value === "object" &&
      "source" in value && typeof value.source === "string" &&
      [...ownedRoots].some(root => (value.source as string).startsWith(root + path.sep))) observed.add(file);
    return result;
  } };
});
interface Receipt {
  id: string; reason: string; time: number; source: string; destination: string;
  state: "prepared" | "moved" | "rename-failed" | "held-in-place" | "copy-failed";
  originalRetained?: true; verifiedCopyPath?: string;
}
const fixtures: Array<{ root: string; client: ResidencyClient; participants: ParticipantDirectory }> = [];
const write = (file: string, bytes: string | Buffer) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, bytes, { mode: 0o600 });
};
const receipts = (namespace: string): Receipt[] => [...observed]
  .map(file => JSON.parse(fs.readFileSync(file, "utf8")) as Receipt)
  .filter(receipt => receipt.source.startsWith(namespace + path.sep));
const held = (archive: string, source: string): string => {
  const receipt = receipts(archive).find(entry => entry.source === source && entry.state === "moved");
  expect(receipt, `missing moved receipt for ${source}`).toBeDefined();
  expect(path.isAbsolute(receipt!.destination)).toBe(true);
  expect(receipt!.destination).not.toBe(source);
  if (process.platform !== "win32") expect(fs.statSync(path.dirname(receipt!.destination)).mode & 0o777).toBe(0o700);
  return receipt!.destination;
};
const fixture = (options: { absent?: boolean; worktree?: boolean; retainRuns?: boolean; noResult?: boolean; temporary?: boolean } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-move-aside-"));
  ownedRoots.add(root);
  const rootId = "session:cleanup-move-aside";
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "cleanup-move-aside", cwd: root, projectRoot: root,
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
  const makeClient = () => new ResidencyClient({ config, mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget, onResultConsumed: consumed });
  const client = makeClient();
  const tracked = { root, client, participants }; fixtures.push(tracked);
  const restartClient = async () => { await tracked.client.close(); tracked.client = makeClient(); return tracked.client; };
  const id = "a".repeat(32);
  const run = path.join(config.residencyRoot, "runs", id);
  const worktree = path.join(root, ".pi", "fabric", "worktrees", id);
  const branch = `pi-fabric/fixture-${id.slice(0, 8)}`;
  if (options.worktree) fs.mkdirSync(worktree, { recursive: true, mode: 0o700 });
  const temporary = path.join(root, shortTmpdirName(id));
  if (options.temporary) fs.mkdirSync(temporary, { mode: 0o700 });
  const record = {
    ...(options.temporary ? { temporaryDirectory: temporary } : {}),
    id, name: "exited fixture worker", status: "completed", text: "exact final result", task: "work",
    runner: "pi", transport: "process", sessionId: "2147483647", cwd: options.worktree ? worktree : root,
    startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  };
  const metadata = path.join(config.residencyRoot, "agents", `${id}.json`);
  const result = residentResultPath(config.residencyRoot, id);
  write(metadata, JSON.stringify({ format: 1, rootId, id, runDirectory: run, createdAt: 1, updatedAt: 2,
    handle: { ...record, ...(options.worktree ? { worktree, branch } : {}) }, worktreeGitRoot: root }));
  if (!options.noResult) write(result, JSON.stringify(record));
  if (!options.absent) write(path.join(run, "status.json"), JSON.stringify(record));
  // Test namespace only: actual paths come from the pass-through receipt observer.
  const archive = root;
  const proposedArchive = `${config.residencyRoot}.trace-archive`;
  const decisions = path.join(config.residencyRoot, "decisions");
  const originals = new Map<string, { bytes: Buffer; dev: number; ino: number }>();
  const remember = (file: string) => {
    const { dev, ino } = fs.lstatSync(file); originals.set(file, { bytes: fs.readFileSync(file), dev, ino });
  };
  remember(metadata); if (!options.noResult) remember(result);
  const add = (relative: string, base = run) => {
    const bytes = Buffer.from(`${path.basename(base)}/${relative}\0raw\r\n\xff\xfe`, "latin1");
    const file = path.join(base, relative); write(file, bytes); remember(file); return bytes;
  };
  const addTraces = () => {
    for (const name of ["session.jsonl", "events.ndjson", "case-variant/session.JSONL", "Mixed.NdJsOn.backup", "session.jsonl.20261001.bak", "backups/old.ndjson.backup", "nested/child/raw.jsonl.reclaimed"])
      add(name);
    write(path.join(run, "nested", "child", "status.json"), JSON.stringify({ ...record, id: "b".repeat(32) }));
  };
  const sources = () => [run, ...(options.temporary ? [temporary] : []), ...(options.worktree ? [worktree] : []), metadata, result];
  const expectBytes = (moved: string[] = []) => {
    for (const [file, original] of originals) {
      const source = sources().find(source => file === source || file.startsWith(source + path.sep))!;
      const current = moved.includes(source) ? path.join(held(archive, source), path.relative(source, file)) : file;
      expect(fs.readFileSync(current), file).toEqual(original.bytes);
      expect(fs.lstatSync(current), file).toMatchObject({ dev: original.dev, ino: original.ino });
    }
  };
  const expectUnconsumed = () => expect(consumed).not.toHaveBeenCalled();
  const recordGit = () => vi.mocked(executeFile).mockImplementation(async (command, args) => {
    expect(command).toBe("git");
    // Deliberately record even unsafe baseline argv so RED cannot execute Git deletion.
    return { stdout: args.join(" ") === "worktree list --porcelain" ? `worktree ${worktree}\nHEAD fixture\nbranch refs/heads/${branch}\n` : "", stderr: "" };
  });
  const expectCommitted = () => {
    const entries = fs.readdirSync(decisions); expect(entries).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(decisions, entries[0]!), "utf8"))).toMatchObject({ state: "committed", id });
  };
  return { root, config, client, restartClient, id, run, temporary, metadata, result, archive, proposedArchive, decisions, worktree, branch, record, consumed,
    originals, remember, sources, add, addTraces, expectBytes, expectUnconsumed, recordGit, expectCommitted };
};

beforeEach(() => {
  installInProcessResidentFence();
  vi.mocked(executeFile).mockReset().mockRejectedValue(new Error("unexpected fixture Git command"));
});
afterEach(async () => {
  vi.restoreAllMocks();
  for (const { root, client, participants } of fixtures.splice(0)) {
    await client.close(); await participants.close();
    // The command owner stages exports inside the authentic inherited private TMPDIR
    // and copies/readbacks them before ordinary wrapper cleanup. Production selectors are scrubbed.
    const evidence = process.env.PI_TEST_CUSTODY_EVIDENCE_DIR;
    if (evidence) {
      fs.mkdirSync(evidence, { recursive: true }); const destination = path.join(evidence, path.basename(root));
      if (process.platform === "win32") fs.cpSync(root, destination, { recursive: true });
      else execFileSync("cp", ["-a", "--", root, destination]);
      for (const receipt of receipts(root)) if (!receipt.destination.startsWith(root + path.sep) && fs.existsSync(receipt.destination)) {
        const target = path.join(evidence, `${path.basename(root)}-${path.basename(path.dirname(receipt.destination))}`);
        if (process.platform === "win32") fs.cpSync(path.dirname(receipt.destination), target, { recursive: true });
        else execFileSync("cp", ["-a", "--", path.dirname(receipt.destination), target]);
      }
    }
    console.log(`retained offline cleanup fixture: ${root}`); // Preserve raw originals and receipts for the evidence owner.
  }
});

describe("public resident cleanup moves aside all four evidence roots", () => {
  it("allowlists native deferred status without turning post-move EIO or malformed fields into copy custody", () => {
    const f = fixture();
    const status = { state: "held-in-place", originalRetained: true, source: f.run,
      destination: path.join(f.root, "observed-copy"), receipt: path.join(f.root, "observed-receipt.json"),
      verifiedCopyPath: path.join(f.root, "observed-copy"), arbitrary: "never-on-wire" };
    expect(residentCleanupErrorFields({ code: "CLEANUP_CUSTODY_DEFERRED", cleanupStatus: status })).toEqual({
      errorCode: "CLEANUP_CUSTODY_DEFERRED", cleanupStatus: { state: "held-in-place", originalRetained: true,
        source: status.source, destination: status.destination, receipt: status.receipt, verifiedCopyPath: status.verifiedCopyPath },
    });
    for (const malformed of [{ ...status, originalRetained: false }, { ...status, verifiedCopyPath: true },
      { ...status, state: "copy-failed" }, { ...status, receipt: "relative" }, { ...status, source: undefined }]) {
      expect(residentCleanupErrorFields({ code: "CLEANUP_CUSTODY_DEFERRED", cleanupStatus: malformed })).toEqual({});
    }
    expect(residentCleanupErrorFields({ code: "EIO", retainedRoot: status, cleanupStatus: status })).toEqual({});
    expect(residentCleanupErrorFields({ retainedRoot: status })).toEqual({});
  });

  it.each([true, false])("preserves all four payloads/inodes and durable reason/time/id/path receipts (retainRuns=%s)", async retainRuns => {
    const f = fixture({ worktree: true, retainRuns }); f.addTraces(); f.recordGit();
    for (const name of [".git", "src/main.ts", ".pi/ignored/session.jsonl", ".pi/ignored/events.ndjson.bak", "node_modules/pkg/cli.js"])
      f.add(name, f.worktree);
    const rootInodes = new Map(f.sources().map(source => [source, fs.lstatSync(source)]));
    const synced = new Set<string>();
    const nativeSync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const stat = fs.fstatSync(fd); synced.add(`${stat.dev}:${stat.ino}`); nativeSync(fd);
    });
    const rm = vi.spyOn(fs, "rmSync"); const unlink = vi.spyOn(fs, "unlinkSync"); const copy = vi.spyOn(fs, "copyFileSync");
    const nativeRename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (f.sources().includes(String(source))) {
        f.expectCommitted();
        const receipt = JSON.parse(fs.readFileSync(path.join(path.dirname(String(destination)), "custody.json"), "utf8"));
        expect(receipt).toMatchObject({ state: "prepared", id: f.id, source: String(source), destination: String(destination) });
        // Windows does not support directory fsync; the receipt file still owes it.
        const barriers = [path.join(path.dirname(String(destination)), "custody.json"),
          ...(process.platform === "win32" ? [] : [path.dirname(String(destination)), path.dirname(path.dirname(String(destination)))])];
        for (const file of barriers) {
          const stat = fs.statSync(file); expect(synced.has(`${stat.dev}:${stat.ino}`), `prepared durable ${file}`).toBe(true);
        }
      }
      nativeRename(source, destination);
    });
    const warn = vi.spyOn(diagnostics, "warn"); const before = Date.now();
    await expect(f.client.cleanupAgent(f.id, true)).resolves.toEqual({ cleaned: true });
    const after = Date.now(); const all = receipts(f.archive); expect(all).toHaveLength(4);
    const reasons = ["run", "worktree", "agent metadata", "resident result"];
    for (const [index, source] of f.sources().entries()) {
      const destination = held(f.archive, source); const receipt = all.find(entry => entry.source === source)!;
      expect(receipt).toMatchObject({ id: f.id, source, destination, state: "moved" });
      expect(receipt.reason).toContain(reasons[index]!); expect(receipt.time).toBeGreaterThanOrEqual(before); expect(receipt.time).toBeLessThanOrEqual(after);
      expect(fs.lstatSync(destination)).toMatchObject({ dev: rootInodes.get(source)!.dev, ino: rootInodes.get(source)!.ino });
      const receiptStat = fs.statSync(path.join(path.dirname(destination), "custody.json"));
      expect(synced.has(`${receiptStat.dev}:${receiptStat.ino}`)).toBe(true);
      expect(fs.existsSync(source)).toBe(false);
    }
    f.expectBytes(f.sources()); expect(f.client.hasAgent(f.id)).toBe(false); expect(f.consumed).toHaveBeenCalledExactlyOnceWith(f.id);
    expect(copy).not.toHaveBeenCalled();
    for (const calls of [rm.mock.calls, unlink.mock.calls]) expect(calls.some(([file]) => f.sources().some(source => String(file) === source || String(file).startsWith(source + path.sep)))).toBe(false);
    expect(vi.mocked(executeFile).mock.calls.map(([, args]) => args)).toEqual([["worktree", "list", "--porcelain"]]);
    expect(warn.mock.calls.flat().join(" ")).toContain("registration"); expect(warn.mock.calls.flat().join(" ")).toContain(f.branch);
    expect(warn.mock.calls.flat().join(" ")).toContain("deferred");
  });

  it.each((["run", "worktree", "metadata", "result"] as const).flatMap(stage => [false, true].map(crossDevice => ({ stage, crossDevice }))))("EXDEV at $stage (cross-device metadata=$crossDevice) retains originals and never acknowledges", async ({ stage, crossDevice }) => {
    const f = fixture({ worktree: true }); f.addTraces(); f.add("ignored/raw.NDJSON", f.worktree); f.recordGit();
    const target = f[stage]; const sources = f.sources(); const moved = sources.slice(0, sources.indexOf(target));
    const nativeRename = fs.renameSync.bind(fs); const replacement = Buffer.from("new canonical occupant, not old evidence\n");
    const lstat = fs.lstatSync.bind(fs); let deviceProbe = false;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file, options) => {
      const stat = lstat(file, options);
      if (deviceProbe && String(file) === target && stat) {
        deviceProbe = false; return new Proxy(stat, { get: (value, key) => key === "dev" ? Number(value.dev) + 1 : Reflect.get(value, key) });
      }
      return stat;
    }) as typeof fs.lstatSync);
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (String(source) === target) { deviceProbe = crossDevice; throw Object.assign(new Error(`injected ${stage} EXDEV`), { code: "EXDEV" }); }
      nativeRename(source, destination);
      if (moved.includes(String(source))) {
        if (String(source) === f.run || String(source) === f.worktree) write(path.join(String(source), "new.jsonl"), replacement);
        else write(String(source), replacement);
      }
    });
    const warn = vi.spyOn(diagnostics, "warn"); const signal = new AbortController().signal;
    const error = await f.client.cleanupAgent(f.id, true, signal).catch((error: Error) => error);
    expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id, operation: "cleanup" });
    expect((error as Error).message).toContain(crossDevice ? "custody deferred" : "EXDEV"); expect((error as Error).message).toContain(target); expect((error as Error).message).toContain("Do not retry or reassign");
    f.expectBytes(moved); f.expectUnconsumed(); f.expectCommitted();
    const cleanupStatus = (error as Error & { cleanupStatus?: Receipt }).cleanupStatus;
    if (crossDevice) {
      expect(cleanupStatus).toMatchObject({ source: target, state: process.platform === "linux" ? "held-in-place" : "copy-failed", originalRetained: true });
      expect(receipts(f.archive)).toContainEqual(expect.objectContaining({ source: target, state: cleanupStatus!.state, originalRetained: true }));
      if (process.platform === "linux") {
        expect(cleanupStatus!.verifiedCopyPath).toEqual(expect.any(String));
        for (const [file, original] of f.originals) if (file === target || file.startsWith(target + path.sep))
          expect(fs.readFileSync(path.join(cleanupStatus!.verifiedCopyPath!, path.relative(target, file)))).toEqual(original.bytes);
      } else expect(cleanupStatus!.verifiedCopyPath).toBeUndefined();
    } else { expect(cleanupStatus).toBeUndefined(); expect(receipts(f.archive)).toContainEqual(expect.objectContaining({ source: target, state: "rename-failed" })); }
    const settled = preserveCancellationOutcome({ value: undefined, terminationReason: "runtime_error" as const }, signal);
    expect(settled).toMatchObject({ residentOutcomes: [expect.objectContaining({ id: f.id, state: "committed", operation: "cleanup",
      ...(crossDevice ? { cleanupStatus } : {}),
    })] });
    for (const source of moved) {
      expect((error as Error).message).toContain(held(f.archive, source));
      expect(fs.readFileSync(source === f.run || source === f.worktree ? path.join(source, "new.jsonl") : source)).toEqual(replacement);
    }
    expect(warn.mock.calls.flat().join(" ")).toContain(target);
    expect(vi.mocked(executeFile).mock.calls.map(([, args]) => args)).toEqual([["worktree", "list", "--porcelain"]]);
  });

  it("keeps a durable prepared receipt and moved bytes if post-move receipt publication fails", async () => {
    const f = fixture(); f.addTraces(); const nativeRename = fs.renameSync.bind(fs); let moved = "";
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (moved && String(destination) === path.join(path.dirname(moved), "custody.json")) throw Object.assign(new Error("injected post-move receipt failure"), { code: "EIO" });
      nativeRename(source, destination);
      if (String(source) === f.run) { moved = String(destination); write(path.join(f.run, "replacement.jsonl"), "new occupant"); }
    });
    const error = await f.client.cleanupAgent(f.id).catch((error: Error) => error);
    expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id }); expect((error as Error).message).toContain(moved);
    const prepared = JSON.parse(fs.readFileSync(path.join(path.dirname(moved), "custody.prepared.json"), "utf8"));
    expect(prepared).toMatchObject({ source: f.run, destination: moved, state: "prepared", id: f.id });
    expect(fs.readFileSync(path.join(moved, "session.jsonl"))).toEqual(f.originals.get(path.join(f.run, "session.jsonl"))!.bytes);
    expect(fs.readFileSync(path.join(f.run, "replacement.jsonl"), "utf8")).toBe("new occupant");
    expect(fs.existsSync(f.metadata)).toBe(true); expect(fs.existsSync(f.result)).toBe(true); f.expectUnconsumed(); f.expectCommitted();
  });

  it.each([false, true])("legacy status-only / absent run still holds metadata and saved result (absent=%s)", async absent => {
    const f = fixture({ absent });
    await expect(f.client.cleanupAgent(f.id)).resolves.toEqual({ cleaned: true });
    f.expectBytes([f.metadata, f.result]); expect(receipts(f.archive)).toHaveLength(absent ? 2 : 3);
    expect(executeFile).not.toHaveBeenCalled(); expect(f.consumed).toHaveBeenCalledExactlyOnceWith(f.id);
  });

  it("accepts an absent optional saved result without manufacturing a custody move", async () => {
    const f = fixture({ noResult: true }); f.addTraces();
    await expect(f.client.cleanupAgent(f.id)).resolves.toEqual({ cleaned: true });
    f.expectBytes([f.run, f.metadata]); expect(receipts(f.archive)).toHaveLength(2);
  });

  it.each(["unresolved", "live root", "unknown root", "live descendant", "unknown descendant"] as const)("retains terminal-plus-strict-descendant liveness veto: %s", async state => {
    const f = fixture({ worktree: true }); f.addTraces();
    if (state === "unresolved") markUnresolvedWorker(f.run, "known worker exit unresolved");
    else {
      const root = state.endsWith("root");
      write(path.join(f.run, ...(root ? ["status.json"] : ["nested", "child", "status.json"])), JSON.stringify({
        ...f.record, id: root ? f.id : "b".repeat(32), sessionId: state.startsWith("live") ? String(process.pid) : undefined,
      }));
    }
    await expect(f.client.cleanupAgent(f.id, true)).rejects.toThrow(/worker may still be running|unknown (root|descendant) identity/);
    f.expectBytes(); f.expectUnconsumed(); expect(receipts(f.archive)).toEqual([]); expect(fs.existsSync(f.decisions)).toBe(false); expect(executeFile).not.toHaveBeenCalled();
  });

  it("refuses nonterminal status before any effect", async () => {
    const f = fixture(); write(f.result, JSON.stringify({ ...f.record, status: "running" })); write(path.join(f.run, "status.json"), JSON.stringify({ ...f.record, status: "running" }));
    await expect(f.client.cleanupAgent(f.id)).rejects.toThrow(/running durable Fabric agent/);
    expect(receipts(f.archive)).toEqual([]); expect(fs.existsSync(f.decisions)).toBe(false); f.expectUnconsumed();
  });

  it("checks registration and cancellation before commitment or archive creation", async () => {
    const f = fixture({ worktree: true }); f.addTraces();
    vi.mocked(executeFile).mockResolvedValue({ stdout: `worktree ${f.root}\n`, stderr: "" });
    await expect(f.client.cleanupAgent(f.id, true)).rejects.toThrow(/not registered/);
    const controller = new AbortController();
    vi.mocked(executeFile).mockImplementation(async () => { controller.abort(new Error("cancelled during registration")); return { stdout: `worktree ${f.worktree}\n`, stderr: "" }; });
    await expect(f.client.cleanupAgent(f.id, true, controller.signal)).rejects.toThrow("cancelled during registration");
    f.expectBytes(); f.expectUnconsumed(); expect(receipts(f.archive)).toEqual([]); expect(fs.existsSync(f.decisions)).toBe(false);
  });

  it.each(["run", "result"] as const)("cancellation after %s move does not consume/ack and never rolls back custody", async stage => {
    const f = fixture({ worktree: true }); f.addTraces(); f.recordGit(); const controller = new AbortController();
    const nativeRename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => { nativeRename(source, destination); if (String(source) === f[stage]) controller.abort(new Error(`cancelled after ${stage} move`)); });
    const error = await f.client.cleanupAgent(f.id, true, controller.signal).catch((error: Error) => error);
    expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id }); expect((error as Error).message).toContain(`cancelled after ${stage} move`);
    const moved = stage === "run" ? [f.run] : f.sources(); f.expectBytes(moved); f.expectUnconsumed(); f.expectCommitted();
    expect(receipts(f.archive).filter(receipt => receipt.state === "moved")).toHaveLength(moved.length);
  });

  it("rejects pre-cancel and deleteBranch without a worktree before custody", async () => {
    const f = fixture(); f.addTraces(); const controller = new AbortController(); controller.abort(new Error("already cancelled"));
    await expect(f.client.cleanupAgent(f.id, false, controller.signal)).rejects.toThrow("already cancelled");
    await expect(f.client.cleanupAgent(f.id, true)).rejects.toThrow("no worktree branch to delete");
    f.expectBytes(); f.expectUnconsumed(); expect(receipts(f.archive)).toEqual([]); expect(fs.existsSync(f.decisions)).toBe(false);
  });

  it("archive failure leaves all sources intact and preserves known-ID commitment", async () => {
    const f = fixture({ worktree: true }); f.addTraces(); f.recordGit(); write(f.proposedArchive, "not a directory");
    await expect(f.client.cleanupAgent(f.id, true)).rejects.toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id });
    f.expectBytes(); f.expectUnconsumed(); f.expectCommitted();
    expect(vi.mocked(executeFile).mock.calls.map(([, args]) => args)).toEqual([["worktree", "list", "--porcelain"]]);
  });

  it("fresh public clients retain repeated canonical IDs in fresh containers without reusing or deleting held roots", async () => {
    const f = fixture(); f.addTraces(); const metadata = fs.readFileSync(f.metadata); const result = fs.readFileSync(f.result);
    await f.client.cleanupAgent(f.id); const first = held(f.archive, f.run); const firstRaw = fs.readFileSync(path.join(first, "session.jsonl"));
    write(f.metadata, metadata); write(f.result, result); write(path.join(f.run, "status.json"), JSON.stringify(f.record)); write(path.join(f.run, "session.jsonl"), "second generation\n");
    // A single client must never re-adopt replacements after its completed moves.
    // A separate lifecycle can discover the reused canonical ID independently.
    await (await f.restartClient()).cleanupAgent(f.id);
    const roots = receipts(f.archive).filter(receipt => receipt.source === f.run); expect(roots).toHaveLength(2); expect(roots[0]!.destination).not.toBe(roots[1]!.destination);
    expect(fs.readFileSync(path.join(first, "session.jsonl"))).toEqual(firstRaw); expect(receipts(f.archive)).toHaveLength(6);
  });

  it("constructs lower/upper suffix traces without a case-insensitive alias", () => {
    const f = fixture(); f.addTraces();
    const lower = path.join(f.run, "session.jsonl"), upper = path.join(f.run, "case-variant/session.JSONL");
    expect(fs.realpathSync(lower).toLowerCase()).not.toBe(fs.realpathSync(upper).toLowerCase());
    expect(fs.readFileSync(lower)).not.toEqual(fs.readFileSync(upper));
    expect(fs.readFileSync(lower)).toEqual(f.originals.get(lower)!.bytes); expect(fs.readFileSync(upper)).toEqual(f.originals.get(upper)!.bytes);
  });

  it("retains the authenticated recorded private TMPDIR, including late-FD tails and replacement occupants", async () => {
    const f = fixture({ temporary: true }); f.addTraces(); f.add("scratch/raw.NDJSON", f.temporary);
    const file = path.join(f.temporary, "scratch/raw.NDJSON"), original = fs.readFileSync(file), inode = fs.statSync(f.temporary).ino;
    const fd = fs.openSync(file, "a"), rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      rename(source, destination);
      if (String(source) === f.temporary) { write(path.join(f.temporary, "replacement.JSONL"), "new scratch occupant"); fs.writeSync(fd, "late tail"); }
    });
    try {
      await expect(f.client.cleanupAgent(f.id)).resolves.toEqual({ cleaned: true });
      const moved = held(f.archive, f.temporary); expect(fs.statSync(moved).ino).toBe(inode);
      expect(fs.readFileSync(path.join(moved, "scratch/raw.NDJSON"))).toEqual(Buffer.concat([original, Buffer.from("late tail")]));
      expect(fs.readFileSync(path.join(f.temporary, "replacement.JSONL"), "utf8")).toBe("new scratch occupant");
      expect(receipts(f.archive)).toHaveLength(4); expect(f.consumed).toHaveBeenCalledExactlyOnceWith(f.id);
    } finally { fs.closeSync(fd); }
  });

  it.each(["absent", "unrecorded", "wrong-name", "in-run", "symlink"] as const)("does not manufacture or adopt an unauthenticated optional TMPDIR: %s", async kind => {
    const f = fixture({ temporary: true });
    let temporary = f.temporary;
    if (kind === "absent") fs.rmdirSync(temporary);
    if (kind === "unrecorded") { const { temporaryDirectory: _, ...record } = f.record; write(path.join(f.run, "status.json"), JSON.stringify(record)); }
    if (kind === "wrong-name") { temporary = path.join(f.root, "not-owned-scratch"); write(path.join(temporary, "raw.jsonl"), "do not adopt"); }
    if (kind === "in-run") { temporary = path.join(f.run, shortTmpdirName(f.id)); write(path.join(temporary, "raw.jsonl"), "already retained with run"); }
    if (kind === "symlink") {
      fs.rmdirSync(temporary); const target = path.join(f.root, "foreign-target"); write(path.join(target, "raw.jsonl"), "link target untouched");
      fs.symlinkSync(target, temporary, process.platform === "win32" ? "junction" : "dir");
    }
    if (kind !== "unrecorded") write(path.join(f.run, "status.json"), JSON.stringify({ ...f.record, temporaryDirectory: temporary }));
    await expect(f.client.cleanupAgent(f.id)).resolves.toEqual({ cleaned: true });
    expect(receipts(f.archive).some(row => row.source === temporary)).toBe(false);
    if (kind === "wrong-name" || kind === "symlink" || kind === "unrecorded") expect(fs.lstatSync(temporary).isDirectory() || fs.lstatSync(temporary).isSymbolicLink()).toBe(true);
    if (kind === "in-run") expect(fs.readFileSync(path.join(held(f.archive, f.run), path.relative(f.run, temporary), "raw.jsonl"), "utf8")).toBe("already retained with run");
  });

  it.each(["cancel", "EXDEV", "receipt"] as const)("private TMPDIR %s preserves committed custody, originals and no-consumption", async fault => {
    const f = fixture({ temporary: true }); f.addTraces(); f.add("raw.ndjson", f.temporary);
    const controller = new AbortController(), rename = fs.renameSync.bind(fs); let destination = "";
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(source) === f.temporary && fault === "EXDEV") throw Object.assign(new Error("private TMPDIR EXDEV"), { code: "EXDEV" });
      if (destination && String(target) === path.join(path.dirname(destination), "custody.json") && fault === "receipt") throw new Error("private TMPDIR receipt unavailable");
      rename(source, target);
      if (String(source) === f.temporary) { destination = String(target); if (fault === "cancel") controller.abort(new Error("private TMPDIR cancelled")); }
    });
    const error = await f.client.cleanupAgent(f.id, false, controller.signal).catch((error: Error) => error);
    expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: f.id, operation: "cleanup" });
    expect((error as Error).message).toContain("Do not retry or reassign");
    f.expectUnconsumed(); f.expectCommitted();
    if (fault === "receipt") {
      expect(JSON.parse(fs.readFileSync(path.join(path.dirname(destination), "custody.prepared.json"), "utf8"))).toMatchObject({ source: f.temporary, state: "prepared" });
      expect(fs.readFileSync(path.join(destination, "raw.ndjson"))).toEqual(f.originals.get(path.join(f.temporary, "raw.ndjson"))!.bytes);
    } else f.expectBytes(fault === "EXDEV" ? [f.run] : [f.run, f.temporary]);
    expect(fs.existsSync(f.metadata)).toBe(true); expect(fs.existsSync(f.result)).toBe(true);
  });

  it.skipIf(process.platform === "win32")("preserves symlinks/FIFOs in both roots and open-FD late appends, without deleting canonical replacements", async () => {
    const f = fixture({ worktree: true }); f.addTraces(); f.recordGit(); const outside = path.join(f.root, "outside.ndjson"); write(outside, "outside untouched\n");
    const entries: Array<{ source: string; relative: string; ino: number; target?: string }> = [];
    const opened = [f.run, f.worktree, f.metadata, f.result].map(source => {
      const file = source === f.metadata || source === f.result ? source : path.join(source, "open.NDJSON.backup");
      if (file !== source) write(file, "before\n");
      return { source, file, original: fs.readFileSync(file), fd: fs.openSync(file, "a"), ino: fs.lstatSync(file).ino };
    });
    for (const source of [f.run, f.worktree]) {
      f.add("real-dir/raw.jsonl.backup", source);
      for (const [relative, target, type] of [["file-link.jsonl", outside, "file"], ["directory-link", "real-dir", "dir"], ["broken.ndjson", "missing", "file"]] as const) {
        const file = path.join(source, relative); fs.symlinkSync(target, file, type); entries.push({ source, relative, ino: fs.lstatSync(file).ino, target });
      }
      const relative = "raw.fifo"; const file = path.join(source, relative); execFileSync("mkfifo", [file]); entries.push({ source, relative, ino: fs.lstatSync(file).ino });
    }
    const nativeRename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      nativeRename(source, destination); const entry = opened.find(entry => entry.source === String(source));
      if (entry) { fs.writeSync(entry.fd, "after move\n"); if (entry.file === entry.source) write(entry.source, "new canonical file\n"); else write(path.join(entry.source, "session.jsonl"), "new canonical root\n"); }
    });
    try {
      await expect(f.client.cleanupAgent(f.id, true)).resolves.toEqual({ cleaned: true });
      for (const entry of opened) {
        fs.writeSync(entry.fd, "after cleanup\n"); const current = path.join(held(f.archive, entry.source), path.relative(entry.source, entry.file));
        expect(fs.lstatSync(current).ino).toBe(entry.ino); expect(fs.readFileSync(current)).toEqual(Buffer.concat([entry.original, Buffer.from("after move\nafter cleanup\n")]));
        expect(fs.readFileSync(entry.file === entry.source ? entry.source : path.join(entry.source, "session.jsonl"), "utf8")).toBe(entry.file === entry.source ? "new canonical file\n" : "new canonical root\n");
      }
      for (const entry of entries) {
        const current = path.join(held(f.archive, entry.source), entry.relative); const stat = fs.lstatSync(current); expect(stat.ino).toBe(entry.ino);
        if (entry.target) { expect(stat.isSymbolicLink()).toBe(true); expect(fs.readlinkSync(current)).toBe(entry.target); } else expect(stat.isFIFO()).toBe(true);
      }
      expect(fs.readFileSync(outside, "utf8")).toBe("outside untouched\n");
    } finally { for (const entry of opened) fs.closeSync(entry.fd); }
  });
});
