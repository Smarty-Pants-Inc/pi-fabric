import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { FabricRuntimeState } from "../../../src/fabric-runtime-state.js";
import { CapturedToolCatalog } from "../../../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, type FabricConfig } from "../../../src/config.js";
import { AgentCompletionInbox } from "../../../src/agents/completion-inbox.js";
import { CompletionJournal } from "../../../src/agents/completion-journal.js";
import type { AgentHandleInfo, AgentTransportHandle } from "../../../src/agents/types.js";
import { ProcessTransport } from "../../../src/agents/transports/process-transport.js";
import { runTreeResourceVeto, canRemoveTerminalRun } from "../../../src/storage/retention.js";
import { residentRoot } from "../../../src/residency/protocol.js";
import { ResidencyClient } from "../../../src/residency/client.js";
import { residentProcessAlive } from "../../../src/residency/process-identity.js";
import { formatNatsDiagnostic } from "../../helpers/nats-diagnostics.js";

/** Real native Pi context -> production FabricRuntimeState.initialize -> production
 * AgentsProvider -> root session manager (B4), or ResidencyClient -> compiled
 * launcher/ResidentHost (B5), then the compiled native worker.
 * No hand-composed directory, control plane, completion producer or inbox.
 * Only inference is synthetic. Observers below delegate every custody operation.
 * HOSTED AUTHOR-ONLY: terminal presence withdrawal and graceful host lease release
 * can physically KV.delete. No local non-forwarding cleanup probe or fake release
 * is installed. The test config is an explicit host bootstrap, not a fleet profile.
 */
export default function natsCompletionParent(pi: ExtensionAPI): void {
  if (process.env.GITHUB_ACTIONS !== "true" || process.env.FABRIC_NATIVE_PRODUCT_DELETE_HOSTED !== "1") {
    throw new Error("HOSTED AUTHOR-ONLY: actual runtime/resident terminal withdrawal or graceful release may physically KV.delete");
  }
  const root = process.env.FABRIC_NATIVE_BOUNDARY_ROOT!;
  const repo = fileURLToPath(new URL("../../../", import.meta.url));
  const meshRoot = path.join(root, "mesh");
  const output = (name: string, value: unknown) => fs.writeFileSync(path.join(root, name), JSON.stringify(value));
  const append = (name: string, value: unknown) => fs.appendFileSync(path.join(root, name), JSON.stringify(value) + "\n");
  // Source runtime uses the build's checked native manifest, as Vitest setup does.
  (globalThis as { __FABRIC_NATIVE_DIGESTS__?: string }).__FABRIC_NATIVE_DIGESTS__ = JSON.stringify(
    JSON.parse(fs.readFileSync(path.join(repo, "dist/native/manifest.json"), "utf8")).helpers ?? {});
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(repo, "dist/index.js"), worker: path.join(repo, "dist/worker.js"),
    residentHost: path.join(repo, "dist/residency/launcher.js"), skills: path.join(repo, "skillsets"),
  } });
  let context: ExtensionContext;
  // Capture ONLY the client initialize() constructs, not another composition.
  // The explicit recovery command below delegates its public ensureHost().
  let resident: ResidencyClient | undefined;
  const startResident = ResidencyClient.prototype.start;
  ResidencyClient.prototype.start = function (this: ResidencyClient) {
    if (this.options.config.meshRoot === meshRoot) resident = this;
    return startResident.call(this);
  };
  // Session B4 uses the manager initialize() actually constructed. These are
  // observers of its real launches/publications, not a second manager/engine.
  const b4 = new Map<string, { run: string; mode: string; native: AgentTransportHandle; primaryClosed: boolean; treeClosed: boolean | null }>();
  let faultId: string | undefined;
  let successorSpawn: Promise<AgentHandleInfo> | undefined;
  let successorFailure: unknown;
  // NATS spawn resolves only AFTER presence admission and native launch. Observe
  // the actual manager's queued state while retaining that same transaction;
  // awaiting its return here would deadlock the held predecessor's test gate.
  const observeSuccessorQueue = async (start: () => Promise<AgentHandleInfo>): Promise<AgentHandleInfo> => {
    let queued!: (handle: AgentHandleInfo) => void;
    const receipt = new Promise<AgentHandleInfo>(resolve => { queued = resolve; });
    const check = () => {
      const handle = runtime.agents.list().find(row => row.name === "native-nats-b4-successor" && row.status === "queued");
      if (handle) queued(handle as AgentHandleInfo);
    };
    const unsubscribe = runtime.agents.subscribeUi(check);
    try {
      successorSpawn = start();
      void successorSpawn.catch(error => { successorFailure = error; });
      check();
      return await Promise.race([receipt, successorSpawn]);
    } finally { unsubscribe(); }
  };
  const gate = path.join(root, "nats-b4-worker-exit-gate.json");
  const launch = ProcessTransport.prototype.launch;
  ProcessTransport.prototype.launch = async function (this: ProcessTransport, request) {
    const mode = /^native-nats-b4-(i|ii|iii|successor)$/.exec(request.name)?.[1];
    if (!mode) return launch.call(this, request);
    const logIndex = request.workerArguments.indexOf("--log-file");
    if (logIndex < 0) throw new Error("B4 actual worker launch has no log path");
    const run = path.dirname(request.workerArguments[logIndex + 1]!);
    const predecessor = [...b4.entries()].find(([, entry]) => entry.mode !== "successor");
    append("nats-b4-launches.jsonl", { id: request.id, mode, run, needs: request.needs,
      ...(predecessor ? { predecessor: { id: predecessor[0], primaryClosed: predecessor[1].primaryClosed,
        treeClosed: predecessor[1].treeClosed, veto: runTreeResourceVeto(predecessor[1].run, 0, undefined, true, true) ?? null } } : {}) });
    const native = await launch.call(this, mode === "ii" || mode === "iii" ? { ...request,
      childEnvironment: environment => {
        const inherited = request.childEnvironment ? request.childEnvironment(environment) : environment;
        return { ...inherited, FABRIC_NATIVE_WORKER_EXIT_GATE: gate,
          NODE_OPTIONS: `${inherited.NODE_OPTIONS ?? ""} --import=${pathToFileURL(path.join(repo, "tests/transport-faults/native-fixtures/worker-exit-gate.mjs")).href}` };
      } } : request);
    const entry = { run, mode, native, primaryClosed: false, treeClosed: native.treeClosed === undefined ? null : false as boolean | null };
    b4.set(request.id, entry);
    if (mode === "iii") faultId = request.id;
    void native.closed?.then(() => {
      entry.primaryClosed = true;
      append("nats-b4-closes.jsonl", { id: request.id, event: "native-close", sessionId: native.sessionId });
    }, () => { /* rejection is not close proof; join below still rejects */ });
    void native.treeClosed?.then(() => { entry.treeClosed = true; }, () => { /* production retains the debt */ });
    // Only stop is injected. Never replace isAlive/lostContact/closed/treeClosed
    // or make up manager status. All other handles and calls delegate unchanged.
    return { ...native, stop: async (...args: Parameters<AgentTransportHandle["stop"]>) => {
      if (faultId !== request.id) return native.stop(...args);
      const status = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
      const pid = Number(native.sessionId);
      const births = fs.readFileSync(process.env.PI_FABRIC_TEST_LAUNCH_LOG!, "utf8").split("\n").filter(Boolean)
        .map(line => JSON.parse(line) as { pid: number; started: string });
      const birth = births.find(row => row.pid === pid);
      const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
      const started = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
      if (!birth || !birth.started || started !== birth.started) throw new Error("B4 stop seam has no exact independently recorded live worker birth");
      append("nats-b4-stop-failures.jsonl", { seam: "E4_STOP_FAIL_KEEP_WORKER_ALIVE", id: request.id, pid, birth,
        statusSessionId: status.sessionId, alive: true, gate: JSON.parse(fs.readFileSync(gate, "utf8")),
        placementAbsent: !fs.existsSync(path.join(run, "placement.json")),
        unresolved: fs.existsSync(path.join(run, "unresolved-worker.json")),
        veto: runTreeResourceVeto(run, 0, undefined, true, true) ?? null });
      // Observe the live numeric-PID veto BEFORE #markLost can mask it with its
      // unresolved marker. No signal, fake exit or synthetic diagnostic hook.
      throw new Error("E4_STOP_FAIL_KEEP_WORKER_ALIVE: native exit unconfirmed");
    } };
  };
  const save = CompletionJournal.prototype.save;
  CompletionJournal.prototype.save = function (this: CompletionJournal, result, admittedRecipient) {
    const entry = this.meshRoot === meshRoot ? b4.get(result.id) : undefined;
    save.call(this, result, admittedRecipient);
    if (entry) {
      const status = runtime.agents.status(result.id);
      append("nats-b4-settlements.jsonl", { result,
        envelopes: fs.readdirSync(path.join(meshRoot, "agent-completions")).filter(file => /^[a-f0-9]{64}\.json$/.test(file)),
        source: JSON.parse(fs.readFileSync(path.join(meshRoot, "agent-completions",
          `${createHash("sha256").update(result.id).digest("hex")}.json`), "utf8")),
        veto: runTreeResourceVeto(entry.run, 0, undefined, true, true) ?? null,
        lostContact: "lostContact" in status ? status.lostContact ?? null : null,
        unresolved: fs.existsSync(path.join(entry.run, "unresolved-worker.json")) });
    }
  };
  const passes = new Set<Promise<void>>();
  let journalFailure: unknown;
  let sawFirstPass!: () => void;
  const firstPass = new Promise<void>(resolve => { sawFirstPass = resolve; });
  const drain = CompletionJournal.prototype.drain;
  CompletionJournal.prototype.drain = async function (this: CompletionJournal, ...args: Parameters<CompletionJournal["drain"]>) {
    if (this.meshRoot !== meshRoot) return drain.apply(this, args);
    const pass = drain.apply(this, args); passes.add(pass);
    try { await pass; }
    catch (error) { journalFailure = error; throw error; }
    finally { passes.delete(pass); sawFirstPass(); }
  };
  const enqueue = AgentCompletionInbox.prototype.enqueue;
  AgentCompletionInbox.prototype.enqueue = function (this: AgentCompletionInbox, result, delivered, prepare) {
    append("nats-completion-enqueued.jsonl", { pid: process.pid, sessionId: context.sessionManager.getSessionId(), result });
    return enqueue.call(this, result, () => {
      delivered?.();
      append("nats-completion-delivered.jsonl", { pid: process.pid, sessionId: context.sessionManager.getSessionId(), id: result.id });
    }, prepare);
  };

  // Reuse FILE's inode-bound observation: actual receipt fsync -> receipt rename
  // -> post-rename directory fsync -> BEFORE archive. Record ORIGINAL source and
  // claim here, not a reconstructed expected envelope after archival.
  let sequence = 0;
  const syncedInodes = new Map<string, number>();
  const syncedDirectories = new Map<string, number>();
  const receiptRenames = new Map<string, number>();
  const inode = (stat: fs.Stats) => `${stat.dev}:${stat.ino}`;
  const sync = fs.fsyncSync; const rename = fs.renameSync;
  fs.fsyncSync = fd => {
    sync(fd);
    let file: string;
    try { file = fs.readlinkSync(`/proc/self/fd/${fd}`); } catch { return; }
    if (!file.startsWith(meshRoot + path.sep)) return;
    const stat = fs.fstatSync(fd); const at = ++sequence;
    if (stat.isDirectory()) syncedDirectories.set(file, at);
    else syncedInodes.set(inode(stat), at);
  };
  fs.renameSync = (source, target) => {
    const destination = String(target);
    if (path.dirname(destination) === path.join(meshRoot, "agent-completions", "receipts")) {
      syncedDirectories.delete(path.dirname(destination));
      rename(source, target);
      receiptRenames.set(destination, ++sequence);
      return;
    }
    if (path.dirname(destination) === path.join(meshRoot, "agent-completions")) {
      const candidateFile = path.join(meshRoot, "agent-completions", "attempts", path.basename(destination));
      if (fs.existsSync(candidateFile)) {
        const candidate = JSON.parse(fs.readFileSync(candidateFile, "utf8"));
        append("nats-completion-promotions.jsonl", { seam: "E4_OBSERVE_NATIVE_ATTEMPT_PROMOTION", pid: process.pid,
          sessionId: context.sessionManager.getSessionId(), candidate,
          supervisorAlive: residentProcessAlive(candidate.supervisor.pid, candidate.supervisor.processStartedAt),
          receiptPresent: fs.existsSync(path.join(meshRoot, "agent-completions", "receipts", path.basename(destination))),
          envelopePresentBeforeRename: fs.existsSync(destination) });
      }
    }
    if (path.dirname(destination) === path.join(meshRoot, "agent-completions", "archive")) {
      const receiptFile = path.join(meshRoot, "agent-completions", "receipts", path.basename(destination));
      const bodySyncSequence = syncedInodes.get(inode(fs.statSync(receiptFile))) ?? 0;
      const receiptRenameSequence = receiptRenames.get(receiptFile) ?? 0;
      const directorySyncSequence = syncedDirectories.get(path.dirname(receiptFile)) ?? 0;
      append("nats-completion-order.jsonl", { pid: process.pid, event: "before-archive",
        receipt: JSON.parse(fs.readFileSync(receiptFile, "utf8")),
        source: JSON.parse(fs.readFileSync(source, "utf8")),
        claim: runtime.mesh.get(`residency/completion-claims/${path.basename(destination, ".json")}`, { fresh: true }),
        bodySyncSequence, receiptRenameSequence, directorySyncSequence, archiveAttemptSequence: ++sequence });
    }
    rename(source, target);
  };

  const checkpoint = async () => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([firstPass, new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error("Actual runtime did not start its completion journal")), 30_000);
      })]);
      // Join passes the actual ResidencyClient already started. Do not initiate
      // a parallel/new drainer beside production's serialized delivery pass.
      await Promise.all([...passes]);
      if (journalFailure) throw journalFailure;
      if (successorFailure) throw successorFailure;
      runtime.completionIdleSignal();
    } finally { clearTimeout(timer); }
  };
  pi.on("session_start", async (_event, ctx) => {
    context = ctx;
    if (!ctx.isProjectTrusted()) throw new Error("NATS native fixture requires CLI --approve, not a fake trusted context");
    const config: FabricConfig = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.fullCodeMode = false;
    config.mesh = { ...config.mesh, enabled: true, transport: "nats", hostTransport: "nats", stateBackend: "file", root: meshRoot,
      nats: JSON.parse(fs.readFileSync(path.join(root, "nats-settings.json"), "utf8")) };
    config.agents = { ...config.agents, runner: "pi", transport: "process", maxConcurrent: 1, budgetUsd: 0,
      timeoutMs: 60_000, retainRuns: true, sessionExport: false, notifyOnComplete: true, nice: 19 };
    config.mcp.enabled = false; config.memory.enabled = false; config.jev.enabled = false; config.records.enabled = false;
    const sessionId = ctx.sessionManager.getSessionId();
    let stage = "initialize";
    const starting = () => output("nats-completion-starting.json", { pid: process.pid, sessionId, stage,
      // Native attribution selectors only, never credentials or the NATS config.
      inherited: Object.fromEntries(["PI_FABRIC_SESSION_ID", "PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_PARENT_RUN",
        "PI_FABRIC_ACTOR_ID", "PI_FABRIC_RUN_ROOT"].map(key => [key, process.env[key] ?? null])) });
    try {
      starting();
      await runtime.initialize(ctx, config);
      stage = "completion checkpoint"; starting();
      await checkpoint();
      output("nats-completion-ready.json", { pid: process.pid, sessionId, main: runtime.mainAgentInfo(ctx),
        meshRoot: runtime.mesh.root, carrier: runtime.config.mesh.transport, hostTransport: runtime.config.mesh.hostTransport,
        nats: runtime.config.mesh.nats,
        stateBackend: runtime.config.mesh.stateBackend, observationHealth: runtime.observationHealth() });
    } catch (error) {
      const safeFailure = formatNatsDiagnostic({ pid: process.pid, sessionId, stage, error });
      fs.writeFileSync(path.join(root, "nats-completion-failed.json"), safeFailure);
      // Pi also prints/reports the thrown error; never rethrow the raw credential-bearing cause.
      throw new Error(safeFailure);
    }
  });
  pi.registerCommand("native-nats-completion-spawn", { description: "Spawn through the actual runtime provider", handler: async (args, ctx) => {
    const residency = args.trim() || "durable";
    if (residency !== "durable" && residency !== "session") throw new Error("Unknown B5 residency");
    // Host-authorized fixture command, not generated code or model authorization.
    // The registry/provider still derives real caller attribution and native scope.
    const handle = await runtime.registry.invoke("agents.spawn", {
      task: "Return the native NATS E4 same-machine result.", model: "boundary-offline/offline", thinking: "off",
      residency, runner: "pi", transport: "process", needs: ["local"], extensions: false, tools: [],
    }, { cwd: ctx.cwd, extensionContext: ctx, signal: AbortSignal.timeout(60_000),
      parentToolCallId: "native-nats-e4", nestedToolCallId: "native-nats-e4-spawn",
      approve: async () => {}, audits: [], maxResultChars: 32_000, update() {} }) as AgentHandleInfo;
    const rootId = runtime.mainAgentInfo(ctx).id;
    output("nats-completion-spawned.json", { pid: process.pid, handle,
      run: residency === "durable" ? path.join(residentRoot(meshRoot, rootId), "runs", handle.id) : runtime.agents.runDirectory(handle.id),
      residencyRoot: residentRoot(meshRoot, rootId), residency });
  } });
  pi.registerCommand("native-nats-resident-ensure", { description: "Recover through initialize's actual ResidencyClient and compiled launcher", handler: async () => {
    if (!resident) throw new Error("Actual runtime ResidencyClient was not observed");
    const owner = await resident.ensureHost();
    output("nats-resident-restarted.json", { pid: process.pid, owner });
  } });
  pi.registerCommand("native-nats-b4-spawn", { description: "Spawn B4 through the actual root registry", handler: async (args, ctx) => {
    const mode = args.trim();
    if (!["i", "ii", "iii", "successor"].includes(mode)) throw new Error("Unknown B4 case");
    const start = () => runtime.registry.invoke("agents.spawn", {
      name: `native-nats-b4-${mode}`, task: `Return the native NATS E4 B4 ${mode} same-machine result.`,
      model: "boundary-offline/offline", thinking: "off", residency: "session", runner: "pi", transport: "process",
      needs: ["local"], extensions: false, tools: [],
    }, { cwd: ctx.cwd, extensionContext: ctx, signal: AbortSignal.timeout(60_000),
      parentToolCallId: "native-nats-e4-b4", nestedToolCallId: `native-nats-e4-b4-${mode}`,
      approve: async () => {}, audits: [], maxResultChars: 32_000, update() {} }) as Promise<AgentHandleInfo>;
    const handle = mode === "successor" ? await observeSuccessorQueue(start) : await start();
    output(`nats-b4-spawn-${mode}.json`, { pid: process.pid, handle, run: runtime.agents.runDirectory(handle.id) });
  } });
  pi.registerCommand("native-nats-b4-status", { description: "Observe root manager public status without foreground consumption", handler: async id => {
    const entry = b4.get(id.trim());
    const status = runtime.agents.status(id.trim());
    output("nats-b4-status.json", { status, ...(entry ? { run: entry.run,
      primaryClosed: entry.primaryClosed, treeClosed: entry.treeClosed,
      veto: runTreeResourceVeto(entry.run, 0, undefined, true, true) ?? null,
      collectible: canRemoveTerminalRun(entry.run), retained: fs.existsSync(entry.run) } : {}) });
  } });
  pi.registerCommand("native-nats-b4-stop", { description: "Explicit stop without consuming the journal result", handler: async id => {
    const result = await runtime.agents.stop(id.trim(), { consume: false });
    output("nats-b4-stopped.json", { result });
  } });
  pi.registerCommand("native-nats-b4-restore-stop", { description: "Restore the same actual transport before cleanup", handler: async () => {
    faultId = undefined;
  } });
  pi.registerCommand("native-nats-b4-join", { description: "Join captured native/tree close after birth-checked test kill", handler: async id => {
    const entry = b4.get(id.trim());
    if (!entry?.native.closed) throw new Error("B4 worker has no captured native close");
    faultId = undefined;
    await runtime.agents.stop(id.trim(), { consume: false });
    await entry.native.closed;
    await entry.native.treeClosed;
    // The earlier queued snapshot was not a successful NATS spawn receipt. Join
    // the real registry invocation now that native exit can release its permit.
    await successorSpawn;
    output("nats-b4-joined.json", { id: id.trim(), status: runtime.agents.status(id.trim()),
      veto: runTreeResourceVeto(entry.run, 0, undefined, true, true) ?? null });
  } });
  pi.registerCommand("native-nats-completion-checkpoint", { description: "Await root-owned journal reconciliation", handler: async () => {
    await checkpoint();
    output("nats-completion-checkpoint.json", { pid: process.pid,
      claims: runtime.mesh.listAll("residency/completion-claims/"), observationHealth: runtime.observationHealth() });
  } });
  pi.on("session_shutdown", async () => {
    try { await runtime.shutdown("reload"); } // Preserve this exact Main lineage for native session restart.
    finally {
      AgentCompletionInbox.prototype.enqueue = enqueue; CompletionJournal.prototype.drain = drain;
      CompletionJournal.prototype.save = save; ProcessTransport.prototype.launch = launch;
      ResidencyClient.prototype.start = startResident;
      fs.fsyncSync = sync; fs.renameSync = rename;
    }
  });
}
