import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentRoot, residentResultPath, readResidentRequestDecision, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import type { ActorManager } from "../src/actors/manager.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import * as cloneFirst from "../src/agents/clone-first-worktree.js";
import { shortTmpdirName } from "../src/agents/short-tmpdir.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

beforeEach(() => installInProcessResidentFence());

// Data-safety keep reason: actual provider -> client -> file mesh -> LIVE host ->
// manager -> WorktreeManager. Only native Git/setup is recorded, never executed.
// These are owned synthetic fixtures; no real session or worktree is deleted.
const payloads = new Map([
  ["sessions/raw.JSONL", Buffer.from('{"raw":true}\n{"torn":')],
  ["sessions/nested/capture.NdJsOn.backup", Buffer.from([0, 255, 13, 10, 123])],
  ["reclaim/old.session.jsonl.BAK", Buffer.from("original reclaim bytes\n")],
  ["node_modules/.cache/.pi/raw.trace.NDJSON", Buffer.from("ignored raw trace\n")],
  ["unclassified.data", Buffer.from("whole root, not a suffix allowlist\n")],
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
interface Move { source: string; destination: string; state: string; id: string; reason: string; time: number; }
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
const held = (root: string, source: string, id: string): Move => {
  const receipt = receipts(root).find(row => row.source === source && row.state === "moved");
  expect(receipt, `missing custody for ${source}`).toBeDefined();
  expect(receipt).toMatchObject({ source, id, reason: expect.any(String), time: expect.any(Number) });
  expect(receipt!.reason.length).toBeGreaterThan(0); expect(receipt!.time).toBeGreaterThan(0);
  expect(fs.existsSync(receipt!.destination)).toBe(true);
  expect(fs.statSync(path.dirname(receipt!.destination)).mode & 0o777).toBe(0o700);
  return receipt!;
};

const fixture = async (retainRuns = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-live-move-aside-"));
  const rootId = "session:live-cleanup-move-aside";
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "live-cleanup-move-aside", cwd: root, projectRoot: root,
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
    return { stdout: "", stderr: "" }; // destructive Git calls recorded, NEVER performed
  });
  vi.spyOn(cloneFirst, "addCloneFirstWorktree").mockImplementation(async options => {
    fs.mkdirSync(options.dest, { recursive: true }); seed(options.dest);
    return { cloned: false, message: "owned fixture; native Git boundary recorded only" };
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
  const launch = async (worktree = true) => {
    const handle = await client.spawnAgent({ task: "fixture cleanup", transport: "process", residency: "durable", worktree }, AbortSignal.timeout(5_000));
    await host.agents.wait(handle.id, { timeoutMs: 5_000 });
    const run = host.agents.runDirectory(handle.id)!; seed(run);
    // Exercise the real recorded/owned short-TMPDIR path without relaxing the
    // fleet's short-name budget. This private fixture is recorded after worker exit.
    const temporary = path.join(root, shortTmpdirName(handle.id)); fs.mkdirSync(temporary, { mode: 0o700 }); seed(temporary);
    const statusFile = path.join(run, "status.json");
    const status = JSON.parse(fs.readFileSync(statusFile, "utf8"));
    fs.writeFileSync(statusFile, JSON.stringify({ ...status, temporaryDirectory: temporary }));
    const metadata = path.join(config.residencyRoot, "agents", `${handle.id}.json`);
    const result = residentResultPath(config.residencyRoot, handle.id);
    return { id: handle.id, run, temporary, metadata, result, worktree: handle.worktree };
  };
  const noGitDelete = () => expect(gitCalls.filter(args => args[0] === "worktree" && args[1] === "remove" || args[0] === "branch" || args.includes("prune"))).toEqual([]);
  const dispose = async () => {
    await client.close(); await host.close(); await participants.close(); vi.restoreAllMocks();
    // Retain raw fixtures and receipts for the evidence owner, rather than reaping them.
    console.log(`retained live cleanup fixture: ${root}`);
  };
  return { root, config, host, client, provider, context, launch, dispose, noGitDelete, failSetup: () => { rejectSetup = true; } };
};

describe("live cleanup move-aside custody", () => {
  it("actual provider/live host retains worktree, run, recorded TMPDIR, metadata and result with bytes/inodes/receipts and open-FD tails", async () => {
    const f = await fixture(); const fds: number[] = [];
    try {
      const run = await f.launch();
      const roots = [run.worktree!, run.run, run.temporary, run.metadata, run.result];
      const inodes = roots.map(source => fs.statSync(source).ino);
      const files = [path.join(run.worktree!, "sessions/raw.JSONL"), path.join(run.run, "sessions/raw.JSONL"), path.join(run.temporary, "sessions/raw.JSONL"), run.metadata, run.result];
      const bytes = files.map(file => fs.readFileSync(file));
      for (const file of files) fds.push(fs.openSync(file, "a"));
      await expect(f.provider.invoke("cleanup", { id: run.id, deleteBranch: true }, f.context)).resolves.toEqual({ cleaned: true });
      for (const [index, source] of roots.entries()) {
        const receipt = held(f.root, source, run.id);
        expect(fs.existsSync(source)).toBe(false); expect(fs.statSync(receipt.destination).ino).toBe(inodes[index]);
        if (index < 3) verify(receipt.destination);
        const file = index < 3 ? path.join(receipt.destination, "sessions/raw.JSONL") : receipt.destination;
        expect(fs.readFileSync(file)).toEqual(bytes[index]);
        fs.writeSync(fds[index]!, "late-open-FD-tail");
        expect(fs.readFileSync(file)).toEqual(Buffer.concat([bytes[index]!, Buffer.from("late-open-FD-tail")]));
      }
      expect(receipts(f.root)).toHaveLength(5); f.noGitDelete();
      const decisions = fs.readdirSync(path.join(f.config.residencyRoot, "decisions"))
        .map(name => readResidentRequestDecision(f.config.residencyRoot, name.slice(0, -5)));
      expect(decisions).toContainEqual(expect.objectContaining({ operation: "cleanup", id: run.id, state: "committed" }));
    } finally { for (const fd of fds) fs.closeSync(fd); await f.dispose(); }
  });

  it("retainRuns leaves run and TMPDIR untouched but LIVE host still retains metadata and saved result", async () => {
    const f = await fixture(true);
    try {
      const run = await f.launch(false);
      const metadata = fs.readFileSync(run.metadata), result = fs.readFileSync(run.result);
      await expect(f.provider.invoke("cleanup", { id: run.id }, f.context)).resolves.toEqual({ cleaned: true });
      const metaHeld = held(f.root, run.metadata, run.id), resultHeld = held(f.root, run.result, run.id);
      expect(fs.readFileSync(metaHeld.destination)).toEqual(metadata); expect(fs.readFileSync(resultHeld.destination)).toEqual(result);
      verify(run.run); verify(run.temporary); expect(receipts(f.root)).toHaveLength(2); f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it("replacement canonical occupants survive every successful root move", async () => {
    const f = await fixture();
    try {
      const run = await f.launch(); const sources = [run.worktree!, run.run, run.temporary, run.metadata, run.result];
      const directories = new Set(sources.slice(0, 3)); const rename = fs.renameSync;
      const replacements = new Map([run.metadata, run.result].map(source => [source,
        JSON.stringify({ ...JSON.parse(fs.readFileSync(source, "utf8")), replacementRaw: "new canonical bytes" }) + "\n"]));
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        rename(source, destination);
        if (sources.includes(String(source))) {
          if (directories.has(String(source))) { fs.mkdirSync(source, { recursive: true }); seed(String(source)); }
          else fs.writeFileSync(source, replacements.get(String(source))!);
        }
      });
      await f.client.cleanupAgent(run.id, true);
      for (const source of sources) {
        held(f.root, source, run.id);
        if (directories.has(source)) verify(source); else expect(fs.readFileSync(source, "utf8")).toBe(replacements.get(source));
      }
      f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it.each(["run", "temporary", "metadata", "result"] as const)("partial EXDEV at %s preserves previously held and remaining original raw objects with committed known ID", async kind => {
    const f = await fixture();
    try {
      const run = await f.launch(); const target = run[kind];
      const raw = fs.statSync(target).isDirectory() ? undefined : fs.readFileSync(target);
      const rename = fs.renameSync;
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        if (String(source) === target) throw Object.assign(new Error("fixture custody cross-device refusal"), { code: "EXDEV" });
        rename(source, destination);
      });
      const copy = vi.spyOn(fs, "cpSync");
      const error = await f.client.cleanupAgent(run.id, true).catch((error: Error) => error);
      expect(error).toBeInstanceOf(Error); expect((error as Error).message).toMatch(/EXDEV/);
      expect(error).toMatchObject({ residentOutcome: { id: run.id, state: "committed", operation: "cleanup" } });
      if (raw) expect(fs.readFileSync(target)).toEqual(raw); else verify(target);
      expect(receipts(f.root)).toContainEqual(expect.objectContaining({ source: target, id: run.id, state: "rename-failed" }));
      verify(held(f.root, run.worktree!, run.id).destination);
      if (kind !== "run") verify(held(f.root, run.run, run.id).destination);
      if (kind === "metadata" || kind === "result") verify(held(f.root, run.temporary, run.id).destination);
      if (kind === "result") held(f.root, run.metadata, run.id);
      expect(copy).not.toHaveBeenCalled(); f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it.each(["run", "temporary", "metadata", "result"] as const)("post-move receipt failure at %s leaves immutable prepared receipt and original inode held, never rolled back", async kind => {
    const f = await fixture();
    try {
      const run = await f.launch(); const target = run[kind]; const inode = fs.statSync(target).ino;
      const original = fs.statSync(target).isDirectory() ? undefined : fs.readFileSync(target);
      let destination: string | undefined; const rename = fs.renameSync;
      vi.spyOn(fs, "renameSync").mockImplementation((source, dest) => {
        if (destination && String(dest) === path.join(path.dirname(destination), "custody.json")) throw new Error("fixture moved receipt publication failed");
        rename(source, dest);
        if (String(source) === target) destination = String(dest);
      });
      const error = await f.client.cleanupAgent(run.id, true).catch((error: Error) => error);
      expect(error).toBeInstanceOf(Error); expect((error as Error).message).toMatch(/receipt update failed/);
      expect(error).toMatchObject({ residentOutcome: { id: run.id, state: "committed", operation: "cleanup" } });
      expect(destination).toBeDefined(); expect(fs.existsSync(target)).toBe(false);
      expect(fs.statSync(destination!).ino).toBe(inode);
      if (original) expect(fs.readFileSync(destination!)).toEqual(original); else verify(destination!);
      const prepared = JSON.parse(fs.readFileSync(path.join(path.dirname(destination!), "custody.prepared.json"), "utf8"));
      expect(prepared).toMatchObject({ source: target, destination, id: run.id, state: "prepared", reason: expect.any(String), time: expect.any(Number) });
      f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it("automatic setup-failure rollback uses actual common WorktreeManager custody without a public cleanup call", async () => {
    const f = await fixture();
    try {
      f.failSetup();
      await expect(f.client.spawnAgent({ task: "rollback", transport: "process", residency: "durable", worktree: true,
        worktreeSetup: "fixture setup rejection" }, AbortSignal.timeout(5_000))).rejects.toThrow(/Worktree setup command exited/);
      const move = receipts(f.root).find(row => row.source.includes(`${path.sep}worktrees${path.sep}`));
      expect(move).toMatchObject({ state: "moved", reason: expect.any(String), time: expect.any(Number) });
      verify(move!.destination); f.noGitDelete();
    } finally { await f.dispose(); }
  });
});
