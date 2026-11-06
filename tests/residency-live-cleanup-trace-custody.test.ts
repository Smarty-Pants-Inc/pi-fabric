import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentRoot, readResidentRequestDecision, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import type { ActorManager } from "../src/actors/manager.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import * as cloneFirst from "../src/agents/clone-first-worktree.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { markUnresolvedWorker } from "../src/storage/retention.js";

beforeEach(() => installInProcessResidentFence());

// Data-safety keep reason: the real provider/client/host/manager/worktree boundary.
// Only Git's destructive/native seam is recorded, NEVER executed. All actual trees,
// worker exit checks, request decisions, root renames and API handlers remain real.
const payloads = new Map([
  ["sessions/raw.JSONL", Buffer.from('{"message":"raw"}\n{"torn":')],
  ["sessions/nested/capture.NdJsOn.backup", Buffer.from([0, 255, 13, 10, 123])],
  ["reclaim/old.session.jsonl.BAK", Buffer.from("original reclaim bytes\n")],
  ["traces/stream.ndjson", Buffer.from("{\"trace\":1}\n")],
  ["unclassified.data", Buffer.from("whole tree, not a suffix allowlist\n")],
  ["node_modules/.cache/.pi/raw.trace.NDJSON", Buffer.from("ignored raw trace\n")],
]);
const seed = (root: string): void => {
  for (const [name, bytes] of payloads) {
    const file = path.join(root, name);
    fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, bytes);
  }
};
const verify = (root: string): void => {
  for (const [name, bytes] of payloads) expect(fs.readFileSync(path.join(root, name)), name).toEqual(bytes);
};
interface Move { source: string; destination: string; state: string; }
const receipts = (root: string): Move[] => {
  const found: Move[] = [];
  const walk = (directory: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) walk(file);
      else if (entry.name === "custody.json") found.push(JSON.parse(fs.readFileSync(file, "utf8")) as Move);
    }
  };
  walk(root); return found;
};
const moved = (root: string, source: string): Move => {
  const receipt = receipts(root).find(row => row.source === source && row.state === "moved");
  expect(receipt, `missing whole-root custody for ${source}`).toBeDefined();
  expect(fs.existsSync(source)).toBe(false);
  verify(receipt!.destination);
  return receipt!;
};

const fixture = async (retainRuns = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-live-custody-"));
  const rootId = "session:live-cleanup-custody";
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "live-cleanup-custody", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(path.join(root, "mesh"), rootId), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, transport: "file", hostTransport: "file" },
    retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const gitCalls: string[][] = [];
  let rejectSetup = false;
  const execute = processUtils.executeFile;
  vi.spyOn(processUtils, "executeFile").mockImplementation(async (command, args, options) => {
    if (rejectSetup && (command === "/bin/sh" || command === (process.env.ComSpec || "cmd.exe"))) throw new Error("bounded setup failure");
    if (command !== "git") return execute(command, args, options);
    gitCalls.push([...args]);
    if (args.includes("--show-toplevel")) return { stdout: root + "\n", stderr: "" };
    if (args.includes("--show-prefix")) return { stdout: "", stderr: "" };
    if (args[0] === "rev-parse") return { stdout: "a".repeat(40) + "\n", stderr: "" };
    // Deliberately no delete: baseline calls are evidence of the destructive seam,
    // not a claim that a real Git worktree was deleted locally.
    return { stdout: "", stderr: "" };
  });
  vi.spyOn(cloneFirst, "addCloneFirstWorktree").mockImplementation(async options => {
    fs.mkdirSync(options.dest, { recursive: true }); seed(options.dest);
    return { cloned: false, message: "owned fixture only; no native Git creation/deletion" };
  });
  const identity = { id: rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: rootId, rootId, identity, reapDeadHosts: false });
  participants.registerSource(() => [{ format: 1, id: rootId, rootId, kind: "root", name: "Main", status: "idle",
    ownerHostId: rootId, ownerIdentityId: rootId, sessionId: config.sessionId, cwd: root,
    runner: "pi", transport: "host", capabilities: ["fabric"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() }]);
  const host = new ResidentHost(config, () => {});
  await participants.start(); await host.start();
  const main = { id: rootId, local: false } as FabricMainAgentTarget;
  const client = new ResidencyClient({ config, mesh, participants, mainAgent: main, commandTimeoutMs: 5_000 });
  const provider = new AgentsProvider(host.agents, host.actors as unknown as ActorManager,
    new GlobalActorRegistry(path.join(root, "global-actors"), 64 * 1024), main, participants, undefined, host.lifecycle, undefined, client, false);
  const context: FabricInvocationContext = { cwd: root, signal: undefined, extensionContext: {} as FabricInvocationContext["extensionContext"],
    parentToolCallId: "custody", nestedToolCallId: "cleanup", update() {}, activity() {} };
  const launch = async (worktree = true, task = "fixture cleanup") => {
    const handle = await client.spawnAgent({ task, transport: "process", residency: "durable", worktree }, AbortSignal.timeout(5_000));
    await host.agents.wait(handle.id, { timeoutMs: 5_000 });
    const run = host.agents.runDirectory(handle.id)!; seed(run);
    return { id: handle.id, run, worktree: handle.worktree };
  };
  const dispose = async () => {
    await client.close(); await host.close(); await participants.close();
    vi.restoreAllMocks();
    // This exact mkdtemp root owns ALL fixture trees, including the renamed roots.
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
  };
  const noGitDelete = () => expect(gitCalls.filter(args => args[0] === "worktree" && args[1] === "remove" || args[0] === "branch" || args.includes("prune"))).toEqual([]);
  return { root, config, host, client, provider, context, gitCalls, launch, dispose, noGitDelete, failSetup: () => { rejectSetup = true; } };
};

describe("live cleanup whole-root trace custody", () => {
  it("real provider request reaches live host/manager/worktree; both roots move and destructive Git is never called", async () => {
    const f = await fixture();
    try {
      const run = await f.launch();
      const worktreeIno = fs.statSync(run.worktree!).ino, runIno = fs.statSync(run.run).ino;
      const cleanup = vi.spyOn(f.host.agents, "cleanup"); // call-through, not a fake receiver
      const wire: Array<Record<string, unknown>> = [];
      const rm = fs.rmSync;
      vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
        if (String(target).startsWith(path.join(f.config.residencyRoot, "responses") + path.sep) && fs.existsSync(target)) {
          wire.push(JSON.parse(fs.readFileSync(target, "utf8")) as Record<string, unknown>);
        }
        return rm(target, options);
      });
      const outcome = await f.provider.invoke("cleanup", { id: run.id, deleteBranch: true }, f.context);
      expect(cleanup).toHaveBeenCalledWith(run.id, true);
      f.noGitDelete();
      const worktree = moved(f.root, run.worktree!), retainedRun = moved(f.root, run.run);
      expect(fs.statSync(worktree.destination).ino).toBe(worktreeIno);
      expect(fs.statSync(retainedRun.destination).ino).toBe(runIno);
      expect(wire).toContainEqual(expect.objectContaining({ ok: true, cleaned: true,
        custody: expect.arrayContaining([{ source: run.run, destination: retainedRun.destination }, { source: run.worktree, destination: worktree.destination }]),
        deferred: expect.arrayContaining([expect.stringMatching(/registration cleanup deferred/), expect.stringMatching(/branch deletion deferred/)]) }));
      expect(wire).toHaveLength(1);
      // Compare the actual public result with the actual host values, not model-
      // retyped paths or a replacement cleanup receiver. The receipt/byte checks
      // above independently require the real roots and deferred Git work.
      expect(outcome).toEqual({ cleaned: true, custody: wire[0]!.custody, deferred: wire[0]!.deferred });
      const decisions = fs.readdirSync(path.join(f.config.residencyRoot, "decisions"))
        .map(name => readResidentRequestDecision(f.config.residencyRoot, name.slice(0, -5)));
      expect(decisions).toContainEqual(expect.objectContaining({ operation: "cleanup", id: run.id, state: "committed" }));
    } finally { await f.dispose(); }
  });

  it("moves a run without a worktree and preserves ordinary field-less legacy success", async () => {
    const f = await fixture();
    try {
      const run = await f.launch(false);
      await f.client.cleanupAgent(run.id);
      moved(f.root, run.run); f.noGitDelete();
    } finally { await f.dispose(); }
    // Actual host/manager cleanup with no custody/deferred fields has the same
    // response shape as an older host. Keep the normal public result unchanged.
    const legacy = await fixture(true);
    try {
      const run = await legacy.launch(false);
      const cleanup = vi.spyOn(legacy.host.agents, "cleanup"); // actual call-through
      await expect(legacy.provider.invoke("cleanup", { id: run.id }, legacy.context)).resolves.toEqual({ cleaned: true });
      expect(cleanup).toHaveBeenCalledExactlyOnceWith(run.id, false);
      const retained = await cleanup.mock.results[0]!.value;
      expect(retained).not.toHaveProperty("custody"); expect(retained).not.toHaveProperty("deferred");
      verify(run.run); expect(receipts(legacy.root)).toEqual([]); legacy.noGitDelete();
    } finally { await legacy.dispose(); }
  });

  it("retains the configured run root when retainRuns=true while still putting the worktree into custody", async () => {
    const f = await fixture(true);
    try {
      const run = await f.launch(); await f.client.cleanupAgent(run.id, true);
      moved(f.root, run.worktree!); verify(run.run); f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it("retains symlinks and late open-FD appends without walking or copying their targets", async () => {
    const f = await fixture();
    let fd: number | undefined;
    try {
      const run = await f.launch();
      const outside = path.join(f.root, "outside"); fs.mkdirSync(outside); seed(outside);
      fs.symlinkSync(outside, path.join(run.worktree!, "linked"), "dir");
      fd = fs.openSync(path.join(run.worktree!, "sessions/raw.JSONL"), "a");
      await f.client.cleanupAgent(run.id);
      const receipt = moved(f.root, run.worktree!);
      fs.writeSync(fd, "late raw tail");
      expect(fs.readFileSync(path.join(receipt.destination, "sessions/raw.JSONL"))).toEqual(Buffer.concat([payloads.get("sessions/raw.JSONL")!, Buffer.from("late raw tail")]));
      expect(fs.lstatSync(path.join(receipt.destination, "linked")).isSymbolicLink()).toBe(true); verify(outside); f.noGitDelete();
    } finally { if (fd !== undefined) fs.closeSync(fd); await f.dispose(); }
  });

  it.each(["worktree", "run"] as const)("EXDEV during %s custody is visible, keeps raw bytes, and never falls back to copying/deleting", async kind => {
    const f = await fixture();
    try {
      const run = await f.launch(); const target = kind === "run" ? run.run : run.worktree!;
      const rename = fs.renameSync;
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        if (String(source) === target) throw Object.assign(new Error("injected cross-device custody refusal"), { code: "EXDEV" });
        return rename(source, destination);
      });
      const copy = vi.spyOn(fs, "cpSync");
      const error = await f.client.cleanupAgent(run.id, true).catch((error: Error) => error);
      expect(error).toBeInstanceOf(Error); expect((error as Error).message).toMatch(/EXDEV/);
      expect(error).toMatchObject({ residentOutcome: { id: run.id, state: "committed", operation: "cleanup" } });
      expect(f.host.agents.runDirectory(run.id)).toBe(run.run);
      if (kind === "run") moved(f.root, run.worktree!);
      verify(target); f.noGitDelete(); expect(copy).not.toHaveBeenCalled();
      expect(receipts(f.root)).toContainEqual(expect.objectContaining({ source: target, state: "rename-failed" }));
    } finally { await f.dispose(); }
  });

  it("does not remove new canonical path occupants after the original whole roots move", async () => {
    const f = await fixture();
    try {
      const run = await f.launch(); const rename = fs.renameSync;
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        rename(source, destination);
        if ([run.run, run.worktree].includes(String(source))) {
          fs.mkdirSync(source, { recursive: true }); seed(String(source));
          fs.writeFileSync(path.join(String(source), "new-owner.jsonl"), "new canonical raw bytes\n");
        }
      });
      await f.client.cleanupAgent(run.id, true);
      for (const source of [run.run, run.worktree!]) {
        verify(source); expect(fs.readFileSync(path.join(source, "new-owner.jsonl"), "utf8")).toBe("new canonical raw bytes\n");
        const retained = receipts(f.root).find(row => row.source === source && row.state === "moved");
        expect(retained).toBeDefined(); verify(retained!.destination);
      }
      f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it("post-commit cancellation keeps the known cleanup ID and moved roots, never rolls them back", async () => {
    const f = await fixture();
    try {
      const run = await f.launch(); const abort = new AbortController(); const rename = fs.renameSync;
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        rename(source, destination);
        if (String(source) === run.worktree) abort.abort(new Error("caller cancelled after root custody"));
      });
      const result = await f.client.cleanupAgent(run.id, true, abort.signal).catch((error: Error) => error);
      expect(result).toMatchObject({ residentOutcome: { id: run.id, state: "committed", operation: "cleanup" } });
      // Host already committed the effect: request cancellation cannot undo it.
      await vi.waitFor(() => { moved(f.root, run.run); moved(f.root, run.worktree!); }, { timeout: 2_000, interval: 10 });
      f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it("fresh mkdtemp failure preserves originals before any Git bookkeeping", async () => {
    const f = await fixture();
    try {
      const run = await f.launch();
      const mkdtemp = fs.mkdtempSync;
      vi.spyOn(fs, "mkdtempSync").mockImplementation((prefix, options) => {
        if (String(prefix).includes(".trace-archive")) throw new Error("custody allocation denied");
        return mkdtemp(prefix, options);
      });
      await expect(f.client.cleanupAgent(run.id, true)).rejects.toThrow(/custody allocation denied/);
      verify(run.run); verify(run.worktree!); f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it.each(["live-process", "unknown-process", "missing-process", "external-native"] as const)("keeps nested %s exit uncertainty ahead of custody, including after terminal parent settlement", async state => {
    const f = await fixture();
    let nested: string | undefined;
    try {
      const run = await f.launch(); nested = path.join(run.run, "nested", "held"); fs.mkdirSync(nested, { recursive: true }); seed(nested);
      fs.writeFileSync(path.join(nested, "status.json"), JSON.stringify({ status: "completed", finishedAt: 1,
        transport: state === "external-native" ? "tmux" : "process",
        ...(state === "missing-process" ? {} : { sessionId: state === "live-process" ? String(process.pid) : "unknown-native-identity" }) }));
      await expect(f.client.cleanupAgent(run.id, true)).rejects.toThrow(/exit.*unconfirmed|checked.*exit|worker may still be running/);
      verify(run.run); verify(run.worktree!); verify(nested); expect(receipts(f.root)).toEqual([]); f.noGitDelete();
    } finally {
      // Only this test's owned synthetic nested receipt; never a real worker tree.
      if (nested) fs.rmSync(nested, { recursive: true, force: true });
      await f.dispose();
    }
  });

  it("an already-cancelled live cleanup never enters the request fence or moves a tree", async () => {
    const f = await fixture();
    try {
      const run = await f.launch(); const abort = new AbortController(); abort.abort(new Error("before dispatch"));
      await expect(f.client.cleanupAgent(run.id, true, abort.signal)).rejects.toThrow(/before dispatch/);
      verify(run.run); verify(run.worktree!); expect(receipts(f.root)).toEqual([]); f.noGitDelete();
      const decisions = fs.readdirSync(path.join(f.config.residencyRoot, "decisions"))
        .map(name => readResidentRequestDecision(f.config.residencyRoot, name.slice(0, -5)));
      expect(decisions.filter(row => row?.operation === "cleanup")).toEqual([]);
    } finally { await f.dispose(); }
  });

  it("unresolved-worker uncertainty still refuses before root custody or Git effects", async () => {
    const f = await fixture();
    try {
      const run = await f.launch(); markUnresolvedWorker(run.run, "fixture unknown worker");
      await expect(f.client.cleanupAgent(run.id, true)).rejects.toThrow(/lost track|unknown worker/);
      verify(run.run); verify(run.worktree!); expect(receipts(f.root)).toEqual([]); f.noGitDelete();
      // Remove only this test's marker so fixture close can join the known real exited worker.
      fs.rmSync(path.join(run.run, "unresolved-worker.json"), { force: true });
    } finally { await f.dispose(); }
  });

  it("automatic rollback makes an EXDEV refusal visible even when the original setup error owns the response", async () => {
    const f = await fixture();
    try {
      f.failSetup(); const rename = fs.renameSync; const warnings = vi.spyOn(console, "warn");
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        if (String(source).includes(`${path.sep}worktrees${path.sep}`)) throw Object.assign(new Error("rollback cross-device refusal"), { code: "EXDEV" });
        rename(source, destination);
      });
      await expect(f.client.spawnAgent({ task: "rollback EXDEV", transport: "process", residency: "durable", worktree: true,
        worktreeSetup: "fixture setup rejection" }, AbortSignal.timeout(5_000))).rejects.toThrow(/Worktree setup command exited/);
      const failed = receipts(f.root).find(row => row.source.includes(`${path.sep}worktrees${path.sep}`));
      expect(failed).toMatchObject({ state: "rename-failed" }); verify(failed!.source);
      expect(warnings.mock.calls.flat().join("\n")).toMatch(/EXDEV/); f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it("common worktree cleanup preserves the automatic launch-failure rollback tree without any public cleanup call", async () => {
    const f = await fixture();
    try {
      // Setup fails after the ACTUAL WorktreeManager.create registered its lease.
      // The actual manager catch then invokes WorktreeManager.cleanup(id,true).
      f.failSetup();
      await expect(f.client.spawnAgent({ task: "rollback", transport: "process", residency: "durable", worktree: true,
        worktreeSetup: "fixture setup rejection" }, AbortSignal.timeout(5_000))).rejects.toThrow(/Worktree setup command exited/);
      f.noGitDelete();
      const worktreeMove = receipts(f.root).find(row => row.source.includes(`${path.sep}worktrees${path.sep}`));
      expect(worktreeMove).toMatchObject({ state: "moved" }); verify(worktreeMove!.destination);
    } finally { await f.dispose(); }
  });
});
