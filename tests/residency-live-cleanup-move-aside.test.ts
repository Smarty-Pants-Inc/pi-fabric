import fs from "node:fs";
import { execFileSync } from "node:child_process";
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
import { RUN_IDENTITY_FILE } from "../src/agents/run-identity.js";
import { preserveCancellationOutcome } from "../src/async-settlement.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

const { observed, namespaces } = vi.hoisted(() => ({ observed: new Set<string>(), namespaces: new Map<string, Set<string>>() }));
vi.mock("../src/core/atomic-write.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/core/atomic-write.js")>();
  return { ...actual, writeJsonAtomic: (...args: Parameters<typeof actual.writeJsonAtomic>) => {
    const result = actual.writeJsonAtomic(...args);
    const [file, value] = args;
    // Pass through actual durable receipt publication, including lexical helper
    // calls inside WorktreeManager. Only our owned source namespace/IDs qualify.
    if (path.basename(file) === "custody.json" && value && typeof value === "object" &&
      "source" in value && typeof value.source === "string" && "id" in value && typeof value.id === "string" &&
      [...namespaces].some(([root, ids]) => (value.source as string).startsWith(root + path.sep) || ids.has(value.id as string))) observed.add(file);
    return result;
  } };
});
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
  return [...observed].map(file => JSON.parse(fs.readFileSync(file, "utf8")) as Move)
    .filter(move => move.source.startsWith(root + path.sep) || namespaces.get(root)?.has(move.id));
};
const held = (root: string, source: string, id: string): Move => {
  const receipt = receipts(root).find(row => row.source === source && row.state === "moved");
  expect(receipt, `missing custody for ${source}`).toBeDefined();
  expect(receipt).toMatchObject({ source, id, reason: expect.any(String), time: expect.any(Number) });
  expect(receipt!.reason.length).toBeGreaterThan(0); expect(receipt!.time).toBeGreaterThan(0);
  expect(fs.existsSync(receipt!.destination)).toBe(true);
  const container = path.dirname(receipt!.destination);
  if (process.platform !== "win32") expect(fs.statSync(container).mode & 0o777).toBe(0o700);
  else {
    // Independent native ACL readback: Unix mode bits cannot establish Windows privacy.
    const script = "$a=Get-Acl -LiteralPath $env:FABRIC_ACL_PROBE; $sid=[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value; [pscustomobject]@{protected=$a.AreAccessRulesProtected;owner=([System.Security.Principal.NTAccount]$a.Owner).Translate([System.Security.Principal.SecurityIdentifier]).Value;current=$sid;rules=@($a.Access|%{[pscustomobject]@{sid=$_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value;allow=$_.AccessControlType.ToString();inherited=$_.IsInherited}})}|ConvertTo-Json -Depth 5 -Compress";
    const acl = JSON.parse(execFileSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", script],
      { encoding: "utf8", env: { ...process.env, FABRIC_ACL_PROBE: container } }));
    expect(acl.protected).toBe(true); expect(acl.owner).toBe(acl.current);
    expect(acl.rules.length).toBeGreaterThan(0);
    for (const rule of acl.rules) { expect([acl.current, "S-1-5-18", "S-1-5-32-544"]).toContain(rule.sid); expect(rule.allow).toBe("Allow"); expect(rule.inherited).toBe(false); }
  }
  return receipt!;
};

const fixture = async (retainRuns = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-live-move-aside-"));
  namespaces.set(root, new Set());
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
  const temporaryDirectories = new Set<string>();
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
    // Read the prelaunch native receipt, never overwrite the manager's binding.
    const identity = JSON.parse(fs.readFileSync(path.join(run, RUN_IDENTITY_FILE), "utf8"));
    expect(identity).toMatchObject({ runId: handle.id, runDirectory: fs.realpathSync(run) });
    const temporary = identity.temporaryDirectory as string;
    expect(path.isAbsolute(temporary)).toBe(true); expect(fs.statSync(temporary).isDirectory()).toBe(true); seed(temporary);
    temporaryDirectories.add(temporary);
    const relative = path.relative(run, temporary);
    const externalTemporary = relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative);
    if (process.platform === "win32") expect(externalTemporary).toBe(false);
    else { expect(externalTemporary).toBe(true); expect(host.agents.status(handle.id)).toMatchObject({ temporaryDirectory: temporary }); }
    const ids = namespaces.get(root) ?? new Set<string>(); ids.add(handle.id); namespaces.set(root, ids);
    const metadata = path.join(config.residencyRoot, "agents", `${handle.id}.json`);
    const result = residentResultPath(config.residencyRoot, handle.id);
    return { id: handle.id, run, temporary, externalTemporary, metadata, result, worktree: handle.worktree };
  };
  const noGitDelete = () => expect(gitCalls.filter(args => args[0] === "worktree" && args[1] === "remove" || args[0] === "branch" || args.includes("prune"))).toEqual([]);
  const dispose = async () => {
    // Stage inside the launcher's owned TMPDIR before host/wrapper teardown.
    // The outer runner copies and independently hashes this stage before cleanup;
    // these copies do not establish inode custody (the test assertions do).
    try {
      const evidence = process.env.PI_TEST_CUSTODY_EVIDENCE_DIR;
      if (evidence) {
        fs.mkdirSync(evidence, { recursive: true });
        const exports: Array<{ source: string; destination: string }> = [];
        const exportRoot = (source: string, destination: string): void => {
          if (process.platform === "win32") fs.cpSync(source, destination, { recursive: true });
          else execFileSync("cp", ["-a", "--", source, destination]);
          exports.push({ source, destination });
        };
        exportRoot(root, path.join(evidence, path.basename(root)));
        for (const temporary of temporaryDirectories) if (!temporary.startsWith(root + path.sep) && fs.existsSync(temporary)) {
          exportRoot(temporary, path.join(evidence, `${path.basename(root)}-${path.basename(temporary)}`));
        }
        for (const receipt of receipts(root)) if (!receipt.destination.startsWith(root + path.sep) && fs.existsSync(receipt.destination)) {
          exportRoot(path.dirname(receipt.destination), path.join(evidence, `${path.basename(root)}-${path.basename(path.dirname(receipt.destination))}`));
        }
        fs.writeFileSync(path.join(evidence, `${path.basename(root)}-export.json`), JSON.stringify({ exports, method: "pre-teardown staging copy; no inode claim" }));
      }
    } finally { await client.close(); await host.close(); await participants.close(); vi.restoreAllMocks(); }
    console.log(`retained live cleanup fixture: ${root}`);
  };
  return { root, config, host, client, provider, context, launch, dispose, noGitDelete, failSetup: () => { rejectSetup = true; } };
};

describe("live cleanup move-aside custody", () => {
  it("actual provider/live host retains worktree, run, recorded TMPDIR, metadata and result with bytes/inodes/receipts and open-FD tails", async () => {
    const f = await fixture(); const fds: number[] = [];
    try {
      const run = await f.launch();
      const roots = [run.worktree!, run.run, ...(run.externalTemporary ? [run.temporary] : []), run.metadata, run.result];
      const inodes = roots.map(source => fs.statSync(source).ino);
      const directories = new Set([run.worktree!, run.run, ...(run.externalTemporary ? [run.temporary] : [])]);
      const files = roots.map(source => directories.has(source) ? path.join(source, "sessions/raw.JSONL") : source);
      const bytes = files.map(file => fs.readFileSync(file));
      for (const file of files) fds.push(fs.openSync(file, "a"));
      await expect(f.provider.invoke("cleanup", { id: run.id, deleteBranch: true }, f.context)).resolves.toEqual({ cleaned: true });
      for (const [index, source] of roots.entries()) {
        const receipt = held(f.root, source, run.id);
        expect(fs.existsSync(source)).toBe(false); expect(fs.statSync(receipt.destination).ino).toBe(inodes[index]);
        if (directories.has(source)) verify(receipt.destination);
        const file = directories.has(source) ? path.join(receipt.destination, "sessions/raw.JSONL") : receipt.destination;
        expect(fs.readFileSync(file)).toEqual(bytes[index]);
        fs.writeSync(fds[index]!, "late-open-FD-tail");
        expect(fs.readFileSync(file)).toEqual(Buffer.concat([bytes[index]!, Buffer.from("late-open-FD-tail")]));
      }
      if (!run.externalTemporary) verify(path.join(held(f.root, run.run, run.id).destination, path.relative(run.run, run.temporary)));
      expect(receipts(f.root)).toHaveLength(run.externalTemporary ? 5 : 4); f.noGitDelete();
      const decisions = fs.readdirSync(path.join(f.config.residencyRoot, "decisions"))
        .map(name => readResidentRequestDecision(f.config.residencyRoot, name.slice(0, -5)));
      expect(decisions).toContainEqual(expect.objectContaining({ operation: "cleanup", id: run.id, state: "committed" }));
    } finally { for (const fd of fds) fs.closeSync(fd); await f.dispose(); }
  });

  it("retainRuns leaves run and TMPDIR untouched but LIVE host still retains metadata and saved result", async () => {
    const f = await fixture(true);
    try {
      const run = await f.launch();
      const metadata = fs.readFileSync(run.metadata), result = fs.readFileSync(run.result);
      await expect(f.provider.invoke("cleanup", { id: run.id }, f.context)).resolves.toEqual({ cleaned: true });
      const metaHeld = held(f.root, run.metadata, run.id), resultHeld = held(f.root, run.result, run.id);
      expect(fs.readFileSync(metaHeld.destination)).toEqual(metadata); expect(fs.readFileSync(resultHeld.destination)).toEqual(result);
      verify(held(f.root, run.worktree!, run.id).destination);
      verify(run.run); verify(run.temporary); expect(receipts(f.root)).toHaveLength(3); f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it("replacement canonical occupants survive every successful root move", async () => {
    const f = await fixture();
    try {
      const run = await f.launch(); const sources = [run.worktree!, run.run, ...(run.externalTemporary ? [run.temporary] : []), run.metadata, run.result];
      const directories = new Set([run.worktree!, run.run, ...(run.externalTemporary ? [run.temporary] : [])]); const rename = fs.renameSync;
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

  it.each(((process.platform === "win32" ? ["worktree", "run", "metadata", "result"] : ["worktree", "run", "temporary", "metadata", "result"]) as Array<"worktree" | "run" | "temporary" | "metadata" | "result">).flatMap(kind => [false, true].map(crossDevice => ({ kind, crossDevice }))))("partial EXDEV at $kind (cross-device metadata=$crossDevice) retains originals and structured committed outcome", async ({ kind, crossDevice }) => {
    const f = await fixture();
    try {
      const run = await f.launch(); const target = run[kind]!;
      const raw = fs.statSync(target).isDirectory() ? undefined : fs.readFileSync(target);
      const rename = fs.renameSync; const lstat = fs.lstatSync.bind(fs); let deviceProbe = false;
      vi.spyOn(fs, "lstatSync").mockImplementation(((file, options) => {
        const stat = lstat(file, options);
        if (deviceProbe && String(file) === target && stat) {
          deviceProbe = false; return new Proxy(stat, { get: (value, key) => key === "dev" ? Number(value.dev) + 1 : Reflect.get(value, key) });
        }
        return stat;
      }) as typeof fs.lstatSync);
      vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        if (String(source) === target) { deviceProbe = crossDevice; throw Object.assign(new Error("fixture custody cross-device refusal"), { code: "EXDEV" }); }
        rename(source, destination);
      });
      const signal = new AbortController().signal;
      const error = await f.provider.invoke("cleanup", { id: run.id, deleteBranch: true }, { ...f.context, signal }).catch((error: Error) => error);
      expect(error).toBeInstanceOf(Error); expect((error as Error).message).toMatch(crossDevice ? /custody deferred/ : /EXDEV/);
      expect(error).toMatchObject({ residentOutcome: { id: run.id, state: "committed", operation: "cleanup" } });
      if (raw) expect(fs.readFileSync(target)).toEqual(raw); else verify(target);
      const cleanupStatus = (error as Error & { cleanupStatus?: { state: string; source: string; originalRetained: true; verifiedCopyPath?: string } }).cleanupStatus;
      if (crossDevice) {
        expect(cleanupStatus).toMatchObject({ source: target, originalRetained: true, state: process.platform === "linux" ? "held-in-place" : "copy-failed" });
        expect(receipts(f.root)).toContainEqual(expect.objectContaining({ source: target, id: run.id, state: cleanupStatus!.state, originalRetained: true }));
        if (process.platform === "linux") {
          expect(cleanupStatus!.verifiedCopyPath).toEqual(expect.any(String));
          if (raw) expect(fs.readFileSync(cleanupStatus!.verifiedCopyPath!)).toEqual(raw); else verify(cleanupStatus!.verifiedCopyPath!);
        } else expect(cleanupStatus!.verifiedCopyPath).toBeUndefined();
      } else { expect(cleanupStatus).toBeUndefined(); expect(receipts(f.root)).toContainEqual(expect.objectContaining({ source: target, id: run.id, state: "rename-failed" })); }
      const settled = preserveCancellationOutcome({ value: undefined, terminationReason: "runtime_error" as const }, signal);
      expect(settled).toMatchObject({ residentOutcomes: [expect.objectContaining({ id: run.id, state: "committed", operation: "cleanup",
        ...(crossDevice ? { cleanupStatus } : {}),
      })] });
      if (kind !== "worktree") verify(held(f.root, run.worktree!, run.id).destination);
      if (kind !== "worktree" && kind !== "run") verify(held(f.root, run.run, run.id).destination);
      if (kind === "metadata" || kind === "result") verify(run.externalTemporary ? held(f.root, run.temporary, run.id).destination
        : path.join(held(f.root, run.run, run.id).destination, path.relative(run.run, run.temporary)));
      if (kind === "result") held(f.root, run.metadata, run.id);
      expect(f.host.agents.hasRunCustody(run.id)).toBe(kind === "worktree" || kind === "run" || kind === "temporary"); f.noGitDelete();
    } finally { await f.dispose(); }
  });

  it.each((process.platform === "win32" ? ["run", "metadata", "result"] : ["run", "temporary", "metadata", "result"]) as Array<"run" | "temporary" | "metadata" | "result">)("post-move receipt failure at %s leaves immutable prepared receipt and original inode held, never rolled back", async kind => {
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
