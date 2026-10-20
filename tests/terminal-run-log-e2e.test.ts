import "./fixtures/conversation-host.js";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { FabricAgentLog } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { readRunLogPage } from "../src/log-tail.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentAgentMetadata, type ResidentHostConfig } from "../src/residency/protocol.js";
import { readRunLogBuffer, readRunLogText } from "../src/run-log-source.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";
import { AgentTranscriptReader } from "../src/ui/transcript-reader.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

// Incident regression/data safety: the actual SDK reads a test-created file; the
// actual built worker exits; only then may its owner retire plaintext. No fake
// worker/Pi, codec seam, recorded unlink, source-log rewrite, or in-test build.
// Run this suite AFTER building the candidate (including the restore CLI module).
const workerPath = path.resolve("dist/worker.js");
const restorePath = path.resolve("bin/fabric-restore-run-log");
const fixtureExtension = path.resolve("tests/fixtures/durable-pi-extension.ts");
const piBinary = path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js");
const marker = "terminal-log-proof α😀";
const answer = `result:${marker}`;
const digest = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
const fixtures: Array<ReturnType<typeof residentDrainTemp>> = [];
const closers: Array<() => void | Promise<void>> = [];

afterEach(async context => {
  // Keep both private siblings and their isolated environment until every owner is closed.
  const sandboxes = fixtures.splice(0);
  try {
    const failures: unknown[] = [];
    for (const close of closers.splice(0).reverse()) {
      try { await close(); } catch (error) { failures.push(error); }
    }
    // Do not remove a sandbox if an owner could still have native custody.
    if (failures.length) throw new AggregateError(failures, "run-log proof owner close failed; sandbox retained");
    if (context.task.result?.state === "fail" && process.env.FABRIC_RUN_LOG_PROOF_DIR) {
      const diagnosticDirectory = path.join(process.env.FABRIC_RUN_LOG_PROOF_DIR, "private-diagnostics");
      fs.mkdirSync(diagnosticDirectory, { recursive: true, mode: 0o700 });
      fs.chmodSync(diagnosticDirectory, 0o700);
      for (const [index, fixture] of sandboxes.entries()) {
        const root = fixture.root;
        const readJsonSidecar = (suffix: string): unknown => {
          const file = path.join(root, `worker-terminal.json.${suffix}`);
          if (!fs.existsSync(file)) return null;
          const text = fs.readFileSync(file, "utf8");
          try { return JSON.parse(text); } catch { return { invalidJson: true, raw: text }; }
        };
        const stderrFile = path.join(root, "worker-terminal.json.stderr");
        const runner = context.task.name.includes("pi-durable") ? "pi-durable" : "pi";
        const report = {
          runner,
          startup: {
            bootstrap: readJsonSidecar("bootstrap.json"),
            exit: readJsonSidecar("exit.json"),
            stderr: fs.existsSync(stderrFile) ? fs.readFileSync(stderrFile, "utf8") : null,
            uncaught: readJsonSidecar("uncaught.json"),
          },
        };
        const reportFile = path.join(diagnosticDirectory, `terminal-run-log-${runner}-${index}.json`);
        fs.writeFileSync(reportFile, JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
        fs.chmodSync(reportFile, 0o600);
      }
    }
    for (const fixture of sandboxes) fixture.assertNoOutsideDeletion();
    for (const fixture of sandboxes) fs.rmSync(path.dirname(fixture.root), { recursive: true, force: true });
  } finally {
    vi.unstubAllEnvs();
  }
});

interface TerminalReceipt {
  sha256: string;
  bytes: number;
  exitCode: number;
  status: string;
  plainPresent: boolean;
  compressedPresent: boolean;
}

// This is an observation wrapper, not a worker implementation. The synchronous
// exit listener runs in the true worker, before the manager receives native
// close and starts compression. Its receipt is OUTSIDE the managed run tree.
function observingWorker(root: string, receipt: string): string {
  const entry = path.join(root, "observe-worker.mjs");
  fs.writeFileSync(entry, `
import fs from "node:fs";
import { createHash } from "node:crypto";
const argument = name => process.argv[process.argv.indexOf(name) + 1];
// This monitor observes uncaught loader/import failures without changing Node's default crash handling.
process.once("uncaughtExceptionMonitor", (error, origin) => {
  try {
    const details = error instanceof Error
      ? { name: error.name, message: error.message, stack: error.stack ?? null }
      : { name: typeof error, message: String(error), stack: null };
    fs.writeFileSync(${JSON.stringify(receipt + ".uncaught.json")}, JSON.stringify({ ...details, origin }), { mode: 0o600 });
  } catch {
    // Best-effort evidence must not take ownership of the crash.
  }
});
fs.writeFileSync(${JSON.stringify(receipt + ".bootstrap.json")}, JSON.stringify({ argv: process.argv, execPath: process.execPath }), { mode: 0o600 });
const stderrWrite = process.stderr.write.bind(process.stderr);
process.stderr.write = (...args) => {
  fs.appendFileSync(${JSON.stringify(receipt + ".stderr")}, args[0], { mode: 0o600 });
  return stderrWrite(...args);
};
process.once("exit", exitCode => {
  fs.writeFileSync(${JSON.stringify(receipt + ".exit.json")}, JSON.stringify({ exitCode,
    status: fs.existsSync(argument("--status-file")) ? fs.readFileSync(argument("--status-file"), "utf8") : null }), { mode: 0o600 });
  const log = argument("--log-file");
  const bytes = fs.readFileSync(log);
  const status = JSON.parse(fs.readFileSync(argument("--status-file"), "utf8"));
  fs.writeFileSync(${JSON.stringify(receipt)}, JSON.stringify({
    sha256: createHash("sha256").update(bytes).digest("hex"), bytes: bytes.length,
    exitCode, status: status.status, plainPresent: fs.existsSync(log),
    compressedPresent: fs.existsSync(log + ".zst")
  }), { mode: 0o600 });
});
await import(${JSON.stringify(pathToFileURL(workerPath).href)});
`);
  return entry;
}

function isolateEnvironment(root: string, workspace: string): string {
  // Do not inherit the invoking agent's scope, budget, lineage, actor session,
  // mesh, or SDK profile. Keep profile, caches, and tmp isolated; the fixture's
  // child environment below supplies a disposable sibling HOME/USERPROFILE.
  for (const key of Object.keys(process.env)) {
    if (key.startsWith("PI_FABRIC_") || key === "PI_SESSION_ID") vi.stubEnv(key, undefined);
  }
  for (const [key, directory] of Object.entries({
    XDG_CONFIG_HOME: "config",
    XDG_DATA_HOME: "data", XDG_CACHE_HOME: "cache", XDG_STATE_HOME: "state", PI_CODING_AGENT_DIR: "agent",
    TMPDIR: "tmp", TMP: "tmp", TEMP: "tmp",
  })) {
    const value = path.join(root, directory);
    fs.mkdirSync(value, { recursive: true, mode: 0o700 });
    vi.stubEnv(key, value);
  }
  fs.mkdirSync(workspace, { recursive: true, mode: 0o700 });
  const fixture = path.join(workspace, "fixture.txt");
  fs.writeFileSync(fixture, marker);
  vi.stubEnv("DURABLE_TEST_FILE", fixture);
  vi.stubEnv("DURABLE_TEST_FAUX_MODULE", import.meta.resolve("@earendil-works/pi-ai/providers/faux"));
  const agentDir = path.join(root, "agent");
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({
    extensions: [fixtureExtension], defaultProvider: "durable-offline", defaultModel: "test",
    compaction: { enabled: false }, retry: { enabled: false },
  }));
  return agentDir;
}

// Execute the shipped bin entry, not main() or a test transpilation. A close
// receipt, rather than polling or a sleep, proves each CLI has actually exited.
async function restore(cwd: string, args: string[]) {
  const child = spawn(process.execPath, [restorePath, ...args], {
    cwd, env: process.env, stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "", stderr = "";
  child.stdout.on("data", chunk => { stdout += chunk; });
  child.stderr.on("data", chunk => { stderr += chunk; });
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; child.kill("SIGKILL"); }, 5_000);
  try {
    const exit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal }));
    });
    expect(timedOut, "restore CLI exceeded its finite process deadline").toBe(false);
    expect(exit).toEqual({ code: 0, signal: null });
    expect(stderr).toBe("");
    const result = JSON.parse(stdout.trim()) as { kind: string; originalBytes: number };
    return { ...exit, result };
  } finally {
    clearTimeout(timer);
  }
}

function readerContext(cwd: string): FabricInvocationContext {
  return { cwd, signal: undefined, parentToolCallId: "log-proof", nestedToolCallId: "log",
    extensionContext: {} as ExtensionContext, update() {} };
}

describe("terminal run-log user path against the built candidate", () => {
  it.for(["pi", "pi-durable"] as const)("retains, reads and additively restores the true terminal %s worker log", { timeout: 45_000 }, async (runner, context) => {
    if (process.platform === "win32") context.skip("platform: Windows compression is disabled because durable directory sync is unavailable; real imported-frame reader/restore coverage runs in run-log-readers-compression.test.ts");
    // Missing build on a supported platform is a failure, never a silently skipped proof.
    expect(fs.existsSync(workerPath), "build the candidate before running this suite").toBe(true);
    expect(fs.existsSync(path.resolve("dist/storage/run-log-restore.js"))).toBe(true);
    const fixture = residentDrainTemp("terminal-run-log-e2e-");
    fixtures.push(fixture);
    const root = fixture.root;
    const workspace = path.join(root, "workspace");
    isolateEnvironment(root, workspace);
    for (const [key, value] of Object.entries(fixture.childEnvironment(process.env.NODE_OPTIONS ?? ""))) {
      vi.stubEnv(key, value);
    }
    const receiptFile = path.join(root, "worker-terminal.json");
    const entry = observingWorker(root, receiptFile);
    const meshConfig = DEFAULT_FABRIC_CONFIG.mesh;
    const meshRoot = path.join(root, "mesh");
    const mesh = new MeshStore(meshRoot, meshConfig.maxEventBytes, meshConfig.maxReadEvents);
    closers.push(() => mesh.closeState());
    // These are fixture identities, not a discovered live host/session.
    const identity = { id: "session:terminal-log-proof", sessionId: "terminal-log-proof", name: "main", kind: "main" } satisfies MeshIdentity;
    const residencyRoot = residentRoot(meshRoot, identity.id);
    const agentsConfig = { ...DEFAULT_FABRIC_CONFIG.agents, runner, transport: "process" as const,
      extensions: true, retainRuns: true, sessionExport: false as const, timeoutMs: 20_000, maxConcurrent: 1 };
    const manager = new AgentManager(workspace, agentsConfig, {
      workerPath: entry, runRoot: path.join(residencyRoot, "runs"), fullCodeMode: false,
      meshRoot, mainAgentId: identity.id, fabricSessionId: identity.sessionId,
      projectRoot: workspace, piBinary: runner === "pi" ? piBinary : path.join(root, "forbidden-native-fallback"),
    });
    closers.push(() => manager.close());
    const result = await manager.run({ task: "read fixture", model: "durable-offline/test", tools: ["read"], worktree: false });
    expect(result.status, result.error).toBe("completed");
    expect(result.text).toBe(answer);
    expect(result.toolCalls).toBe(1);
    expect(result.worktree).toBeUndefined();

    // Logical completion can precede native close. This public join owns that
    // ordering and must not resolve while worker/codec custody is outstanding.
    await manager.finalizeRunLog(result.id);
    const terminal = JSON.parse(fs.readFileSync(receiptFile, "utf8")) as TerminalReceipt;
    expect(terminal).toMatchObject({ exitCode: 0, status: "completed", plainPresent: true, compressedPresent: false });
    expect(terminal.bytes).toBeGreaterThan(0);
    const runDirectory = manager.runDirectory(result.id)!;
    expect(path.dirname(receiptFile)).not.toBe(runDirectory);
    const logFile = path.join(runDirectory, "events.jsonl");
    const encodedFile = `${logFile}.zst`;
    expect(fs.existsSync(logFile)).toBe(false);
    expect(fs.existsSync(encodedFile)).toBe(true);
    const encoded = fs.readFileSync(encodedFile);
    const decoded = readRunLogBuffer(logFile);
    expect(decoded.length).toBe(terminal.bytes);
    expect(digest(decoded)).toBe(terminal.sha256);
    expect(readRunLogText(logFile)).toBe(decoded.toString("utf8"));
    expect(decoded.toString("utf8")).toContain(answer);

    const page = readRunLogPage(logFile, 5_000);
    expect(page.hasMore).toBe(false);
    expect(page.lines.length).toBeGreaterThan(0);
    expect(page.lines.map(line => line.raw).join("\n")).toContain(answer);
    expect(manager.readLog(result.id, { lines: 5_000 }).events).toEqual(page.lines);
    const source = { id: result.id, status: result.status, logFile };
    const dashboard = new AgentTranscriptReader();
    const dashboardCompressed = dashboard.read(source);
    expect(dashboardCompressed.unavailable).toBeUndefined();
    expect(dashboardCompressed.entries.some(item => item.kind === "assistant" && item.text?.includes(answer))).toBe(true);
    const native = new NativeConversationReader();
    const nativeCompressed = native.read({ ...source, eventsFile: logFile });
    expect(nativeCompressed.unavailable).toBeUndefined();
    expect(nativeCompressed.messages.some(message => message.role === "assistant" &&
      message.content.some(block => block.type === "text" && block.text === answer))).toBe(true);

    // Real receiver setup only: no second worker/host is started for readback.
    // The durable registration points at THE SAME manager-owned run directory.
    const participants = new ParticipantDirectory(mesh, { enabled: false, hostId: identity.id, rootId: identity.id,
      identity, heartbeatMs: 1_000, leaseMs: 5_000 });
    closers.push(() => participants.close());
    const mainAgent: FabricMainAgentTarget = {
      id: identity.id, local: true, matches: id => id === "main" || id === identity.id,
      info: () => ({ id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
        cwd: workspace, local: true, pendingMessages: false, updatedAt: 1 }),
      deliverAgent: () => ({ queued: true, messageId: "unused-reader-delivery", routed: "main" }),
    };
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: identity.id, sessionId: identity.sessionId!, cwd: workspace, projectRoot: workspace,
      meshRoot, actorRoot: path.join(root, "actors"), residencyRoot, fullCodeMode: false,
      agents: agentsConfig, mesh: meshConfig, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath, fabricExtensionPath: path.resolve("dist/index.js"), piBinary,
      claudeBinary: "unused", vedaBinary: "unused",
    };
    const client = new ResidencyClient({ config, mesh, participants, mainAgent });
    closers.push(() => client.close());
    const metadata: ResidentAgentMetadata = {
      format: RESIDENT_HOST_FORMAT, rootId: identity.id, id: result.id, runDirectory,
      handle: { id: result.id, name: result.name, status: result.status, runner, transport: "process", cwd: workspace },
      createdAt: result.startedAt, updatedAt: result.updatedAt,
    };
    fs.mkdirSync(path.join(residencyRoot, "agents"), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(residencyRoot, "agents", `${result.id}.json`), JSON.stringify(metadata));
    expect(client.readAgentLog(result.id, { lines: 5_000 }).events).toEqual(page.lines);

    // Register an idle actor, retain the real compressed artifact through the
    // actual archive store, then use ActorManager's normal retained-run reader.
    // This proves archive readback, not a second actor activation/inference run.
    const actors = new ActorManager(identity.sessionId!, identity, mesh, meshConfig, manager, () => {},
      { actorRoot: config.actorRoot, persistent: true });
    closers.push(() => actors.close());
    const actor = await actors.create({ name: "log-reader", instructions: "Remain idle.", responseMode: "text", model: "durable-offline/test" });
    const archives = new ActorLogStore(mesh, meshConfig, DEFAULT_FABRIC_CONFIG.retention);
    expect(actor.sessionFile).toBeDefined();
    const actorSessionFile = actor.sessionFile!;
    await archives.retainRun({ sessionFile: actorSessionFile, lastRunId: result.id }, result.id, runDirectory);
    const archiveFile = path.join(path.dirname(actorSessionFile), "runs", result.id, "events.jsonl");
    expect(fs.existsSync(archiveFile)).toBe(false);
    expect(fs.readFileSync(`${archiveFile}.zst`)).toEqual(encoded);
    expect(digest(readRunLogBuffer(archiveFile))).toBe(terminal.sha256);
    expect(actors.readLog(actor.id, { type: "run", runId: result.id, lines: 5_000 }).run?.events).toEqual(page.lines);

    const lifecycle = new LifecycleBroker(mesh, identity, participants,
      { enabled: false, pollMs: 1_000, maxReadEvents: meshConfig.maxReadEvents }, async () => {});
    const globals = new GlobalActorRegistry(path.join(root, "global-agent"), meshConfig.maxEventBytes);
    const provider = new AgentsProvider(manager, actors, globals, mainAgent, participants, undefined, lifecycle,
      () => false, undefined, false);
    closers.push(() => provider.close());
    const publicLog = await provider.invoke("log", { id: result.id, lines: 5_000 }, readerContext(workspace)) as FabricAgentLog;
    expect(publicLog.events).toEqual(page.lines);

    const beforeRestore = fs.readdirSync(runDirectory).sort();
    const dryRun = await restore(workspace, [encodedFile]);
    expect(dryRun.result).toMatchObject({ kind: "dry-run", originalBytes: terminal.bytes });
    expect(fs.readdirSync(runDirectory).sort()).toEqual(beforeRestore);
    expect(fs.existsSync(logFile)).toBe(false);
    expect(fs.readFileSync(encodedFile)).toEqual(encoded);
    const applied = await restore(workspace, ["--apply", encodedFile]);
    expect(applied.result).toMatchObject({ kind: "restored", originalBytes: terminal.bytes });
    const restored = fs.readFileSync(logFile);
    expect(restored).toEqual(decoded);
    expect(digest(restored)).toBe(terminal.sha256);
    expect(fs.readFileSync(encodedFile)).toEqual(encoded);
    expect(manager.status(result.id)).toMatchObject({ status: "completed", text: answer });
    expect(manager.readLog(result.id, { lines: 5_000 }).events).toEqual(page.lines);
    expect((await provider.invoke("log", { id: result.id, lines: 5_000 }, readerContext(workspace)) as FabricAgentLog).events).toEqual(page.lines);
    expect(client.readAgentLog(result.id, { lines: 5_000 }).events).toEqual(page.lines);
    expect(dashboard.read(source).entries).toEqual(dashboardCompressed.entries);
    expect(native.read({ ...source, eventsFile: logFile }).messages).toEqual(nativeCompressed.messages);
    // Restoring the original is additive; the independently retained archive
    // remains compressed-only and readable, with the same encoded bytes.
    expect(fs.existsSync(archiveFile)).toBe(false);
    expect(fs.readFileSync(`${archiveFile}.zst`)).toEqual(encoded);
    expect(actors.readLog(actor.id, { type: "run", runId: result.id, lines: 5_000 }).run?.events).toEqual(page.lines);

    // Explicit opt-in evidence contains only hashes, sizes, reader names and
    // observed exit results. Never serialize records, paths, IDs or raw content.
    // The caller supplies an existing evidence directory; we never remove it.
    const proofDirectory = process.env.FABRIC_RUN_LOG_PROOF_DIR;
    if (proofDirectory) fs.writeFileSync(path.join(proofDirectory, `terminal-run-log-${runner}.json`), JSON.stringify({
      hashes: { terminal: terminal.sha256, decoded: digest(decoded), restored: digest(restored),
        compressedBefore: digest(encoded), compressedAfter: digest(fs.readFileSync(encodedFile)) },
      byteCounts: { terminal: terminal.bytes, decoded: decoded.length, restored: restored.length, compressed: encoded.length },
      readers: ["readRunLogBuffer", "readRunLogText", "readRunLogPage", "AgentManager.readLog", "agents.log",
        "AgentTranscriptReader", "NativeConversationReader", "ResidencyClient.readAgentLog", "ActorManager.readLog"],
      exitResults: { worker: terminal.exitCode, restoreDryRun: dryRun.code, restoreApply: applied.code },
    }, null, 2) + "\n", { mode: 0o600 });
  });
});
