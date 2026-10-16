import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { FabricRuntimeState } from "../../../src/fabric-runtime-state.js";
import { CapturedToolCatalog } from "../../../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, type FabricConfig } from "../../../src/config.js";
import { AgentCompletionInbox } from "../../../src/agents/completion-inbox.js";
import { CompletionJournal } from "../../../src/agents/completion-journal.js";
import type { AgentHandleInfo } from "../../../src/agents/types.js";
import { residentRoot } from "../../../src/residency/protocol.js";

/** Real native Pi context -> production FabricRuntimeState.initialize -> production
 * AgentsProvider -> ResidencyClient -> compiled launcher/ResidentHost/worker.
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
    await runtime.initialize(ctx, config);
    await checkpoint();
    const sessionId = ctx.sessionManager.getSessionId();
    output("nats-completion-ready.json", { pid: process.pid, sessionId, main: runtime.mainAgentInfo(ctx),
      meshRoot: runtime.mesh.root, carrier: runtime.config.mesh.transport, hostTransport: runtime.config.mesh.hostTransport,
      stateBackend: runtime.config.mesh.stateBackend, observationHealth: runtime.observationHealth() });
  });
  pi.registerCommand("native-nats-completion-spawn", { description: "Spawn through the actual runtime provider", handler: async (_args, ctx) => {
    // Host-authorized fixture command, not generated code or model authorization.
    // The registry/provider still derives real caller attribution and native scope.
    const handle = await runtime.registry.invoke("agents.spawn", {
      task: "Return the native NATS E4 same-machine result.", model: "boundary-offline/offline", thinking: "off",
      residency: "durable", runner: "pi", transport: "process", needs: ["local"], extensions: false, tools: [],
    }, { cwd: ctx.cwd, extensionContext: ctx, signal: AbortSignal.timeout(60_000),
      parentToolCallId: "native-nats-e4", nestedToolCallId: "native-nats-e4-spawn",
      approve: async () => {}, audits: [], maxResultChars: 32_000, update() {} }) as AgentHandleInfo;
    const rootId = runtime.mainAgentInfo(ctx).id;
    output("nats-completion-spawned.json", { pid: process.pid, handle,
      run: path.join(residentRoot(meshRoot, rootId), "runs", handle.id), residencyRoot: residentRoot(meshRoot, rootId) });
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
      fs.fsyncSync = sync; fs.renameSync = rename;
    }
  });
}
