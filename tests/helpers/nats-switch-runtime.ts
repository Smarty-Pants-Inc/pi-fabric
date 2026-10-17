// Native Path B switch fixture. No transport, directory, control handler, admission
// journal, broker ACK or actor manager is replaced. Only Pi's host UI/context is
// deterministic. Admission instrumentation belongs to the separately supplied probe.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricConfig } from "../../src/config.js";
import type { FabricRuntimeState } from "../../src/fabric-runtime-state.js";
import { waitForControlProbeEvidence } from "./nats-path-b-probe.js";
import type { SwitchRoleSnapshot } from "./nats-switch-probe.js";

export interface SwitchRuntimeOptions {
  root: string;
  meshRoot: string;
  agentDir: string;
  sessionId: string;
  name: string;
  config: FabricConfig;
  /** Absolute module exporting installSwitchProbe; no implicit local probe. */
  probeModule?: string;
  probeRole?: "owner" | "sender";
  /** Start registry producers fenced; release only after the FILE mirror gate. */
  producerFence?: boolean;
  workerPath?: string;
  extensionPath?: string;
}
export interface SwitchSend {
  id: string;
  ref: "agents.followUp" | "agents.steer" | "agents.ask";
  args: Record<string, unknown>;
}
export interface SwitchProbeContext {
  runtime: FabricRuntimeState;
  context: ExtensionContext;
  invoke(ref: string, args: Record<string, unknown>): Promise<unknown>;
  options: SwitchRuntimeOptions;
}
/** Swap seam for the shared send-to-admission probe, NOT another protocol.
 * hold/waitForIngress gate BEFORE the real journal, without pausing the inbox.
 * send/snapshot/reconcile own the lane's PubAck, owner ACK, ACK-of-ACK, admission
 * counts, event+archive hashes, command IDs and final marked-write timestamps.
 * installSwitchProbe runs before initialize so public method instrumentation can
 * observe bootstrap; deferred hooks must not access uninitialized runtime getters.
 */
export interface SwitchProbe {
  hold(): Promise<unknown>;
  waitForIngress(): Promise<unknown>;
  release(): Promise<unknown>;
  send(input: SwitchSend): Promise<unknown>;
  snapshot(): Promise<unknown>;
  reconcile(ids: readonly string[]): unknown | Promise<unknown>;
  waitForEvidence?(ids: readonly string[], timeoutMs?: number): Promise<unknown>;
  afterInitialize?(): unknown;
  lifecycleSnapshot?(afterShutdown?: boolean): unknown;
  close?(): Promise<void>;
}
export interface SwitchBridgeOptions {
  hubMesh: string;
  workMesh: string;
  root: string;
  hubBackend: "sqlite" | "file";
  workBackend: "file";
}
export interface SwitchMirrorStatus {
  linked: boolean;
  checkedAt: number;
  nativeHostIds: { hub: string[]; work: string[] };
  mirrors: Array<{ mesh: "hub" | "work"; id: string; observedAt?: number; expiresAt: number; fresh: boolean }>;
}

// Node, not Bun, supplies node:sqlite. The existing host jiti loads .ts imports;
// inherited NODE_OPTIONS retains the fleet's deletion guard in every Node child.
const BOOTSTRAP = String.raw`
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
const hostEntry = import.meta.resolve('@earendil-works/pi-coding-agent');
const requireHost = createRequire(hostEntry);
const manifestPath = requireHost.resolve('jiti/package.json');
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const { createJiti } = await import(pathToFileURL(path.resolve(path.dirname(manifestPath), manifest.exports['./static'].import)).href);
const resolveHost = createJiti(hostEntry);
const virtualModules = {};
for (const specifier of ['typebox', 'typebox/compile', 'typebox/value', '@earendil-works/pi-agent-core', '@earendil-works/pi-tui', '@earendil-works/pi-ai/compat', '@earendil-works/pi-ai/oauth', '@earendil-works/pi-ai/providers/all', '@earendil-works/pi-coding-agent']) {
  virtualModules[specifier] = await import(resolveHost.esmResolve(specifier));
}
virtualModules['@earendil-works/pi-ai'] = virtualModules['@earendil-works/pi-ai/compat'];
const jiti = createJiti(import.meta.url, { moduleCache: false, tryNative: false, virtualModules, fsCache: path.join(process.env.TMPDIR, 'switch-jiti-cache') });
const fixture = await jiti.import(process.argv[1]);
await fixture.runSwitchChild(process.argv[2], process.argv[3]);
`;
const helperPath = fileURLToPath(import.meta.url).replace(/\.js$/, ".ts");
type Reply = { id?: number; ok?: boolean; result?: unknown; error?: { message: string; code?: string }; event?: string };
function descendants(pid: number): number[] {
  if (process.platform !== "linux") throw new Error("switch native process inventory requires Linux");
  const file = `/proc/${pid}/task/${pid}/children`;
  let children: number[];
  try { children = fs.readFileSync(file, "utf8").trim().split(/\s+/).filter(Boolean).map(Number); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
  return children.flatMap(child => [child, ...descendants(child)]);
}

export class SwitchProcess {
  readonly child: ChildProcess;
  readonly events: Reply[] = [];
  readonly #pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void; timer: NodeJS.Timeout }>();
  #next = 0;
  #stderr = "";
  #stopped = false;
  closeResult: unknown;
  terminal?: { pid: number | undefined; exitCode: number | null; signalCode: NodeJS.Signals | null; completedAt: number; closeResult: unknown; preStopDescendants: number[]; failure?: unknown; events: Reply[]; stderr: string };
  constructor(readonly mode: "runtime" | "bridge", configFile: string, ownedRoot: string) {
    const env: NodeJS.ProcessEnv = {};
    for (const key of ["PATH", "NODE_OPTIONS", "TMPDIR", "TMP", "TEMP", "LANG", "SYSTEMROOT", "WINDIR"])
      if (process.env[key] !== undefined) env[key] = process.env[key];
    // Keep HOME outside the inherited deletion ceiling: that guard rejects a
    // ceiling containing homedir(). All Pi/Fabric writable profile selectors
    // below still point to this fixture; no live profile is loaded by the host.
    Object.assign(env, { HOME: os.homedir(), XDG_CONFIG_HOME: path.join(ownedRoot, "config"), XDG_DATA_HOME: path.join(ownedRoot, "data"),
      XDG_CACHE_HOME: path.join(ownedRoot, "cache"), PI_OFFLINE: "1", PI_FABRIC_FIXTURE: "1" });
    this.child = spawn(process.execPath, ["--input-type=module", "-e", BOOTSTRAP, helperPath, mode, configFile], {
      cwd: process.cwd(), env, stdio: ["ignore", "ignore", "pipe", "ipc"],
    });
    this.child.stderr!.on("data", chunk => { this.#stderr = (this.#stderr + String(chunk)).slice(-8_192); });
    this.child.on("message", value => {
      const reply = value as Reply;
      if (reply.event) {
        this.events.push(reply);
        if (reply.event === "checkpoint-trace" && (reply.result as { stage?: string })?.stage === "entry")
          console.info(JSON.stringify({ type: "switch-checkpoint-entry", ...reply.result as object }));
      }
      if (reply.id === undefined) return;
      const pending = this.#pending.get(reply.id);
      if (!pending) return;
      this.#pending.delete(reply.id); clearTimeout(pending.timer);
      if (reply.ok) pending.resolve(reply.result);
      else pending.reject(Object.assign(new Error(reply.error?.message ?? "switch child failed"), { code: reply.error?.code,
        ...(reply.error && "details" in reply.error ? { details: reply.error.details } : {}),
        ...(reply.error && "stack" in reply.error ? { stack: reply.error.stack } : {}) }));
    });
    this.child.on("error", error => this.#fail(error));
    this.child.on("exit", (code, signal) => this.#fail(new Error(`owned switch ${mode} child exited (${code ?? signal}); ${this.#stderr}`)));
  }
  #fail(error: Error): void {
    for (const pending of this.#pending.values()) { clearTimeout(pending.timer); pending.reject(error); }
    this.#pending.clear();
  }
  request<T = unknown>(method: string, args: unknown = {}, timeoutMs = 20_000): Promise<T> {
    if (this.#stopped || !this.child.connected) return Promise.reject(new Error("owned switch child is not connected"));
    const id = ++this.#next;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => { this.#pending.delete(id); reject(new Error(`switch child ${method} deadline (${timeoutMs}ms)`)); }, timeoutMs);
      this.#pending.set(id, { resolve: value => resolve(value as T), reject, timer });
      this.child.send!({ id, method, args }, error => { if (error) { clearTimeout(timer); this.#pending.delete(id); reject(error); } });
    });
  }
  async stop(): Promise<void> {
    if (this.#stopped) return;
    let failure: unknown;
    const preStopDescendants = this.child.pid === undefined ? [] : descendants(this.child.pid);
    try { if (this.child.connected) this.closeResult = await this.request("close", {}, 15_000); }
    catch (error) { failure = error; }
    finally {
      this.#stopped = true;
      if (this.child.exitCode === null && this.child.signalCode === null) {
        const exited = once(this.child, "exit");
        // A close reply is not terminal. Forced cleanup is never STOPALL success.
        const term = setTimeout(() => { failure ??= new Error("switch child required forced stop"); this.child.kill("SIGTERM"); }, 2_000);
        const kill = setTimeout(() => this.child.kill("SIGKILL"), 4_000);
        try { await exited; } finally { clearTimeout(term); clearTimeout(kill); }
      }
      this.terminal = { pid: this.child.pid, exitCode: this.child.exitCode, signalCode: this.child.signalCode,
        completedAt: Date.now(), closeResult: this.closeResult, preStopDescendants, events: [...this.events], stderr: this.#stderr };
    }
    if (this.mode === "runtime" && preStopDescendants.length) failure ??= new Error("runtime has unaccounted resident/worker descendants; STOPALL coverage UNKNOWN");
    if (this.mode === "bridge") {
      const agent = (this.closeResult as { agent?: { pid?: number; exitCode?: number; signalCode?: string | null } })?.agent;
      if (!agent || agent.exitCode !== 0 || agent.signalCode !== null || preStopDescendants.some(pid => pid !== agent.pid))
        failure ??= new Error("bridge/remote-agent inventory or terminal proof incomplete");
    }
    if (this.child.exitCode !== 0 || this.child.signalCode !== null) failure ??= new Error("owned switch child did not exit normally");
    if (failure) {
      this.terminal!.failure = switchError(failure);
      throw failure;
    }
  }
}
export function switchError(error: unknown): unknown {
  return error instanceof Error ? { name: error.name, message: error.message, stack: error.stack,
    ...("details" in error ? { details: error.details } : {}),
    ...(error instanceof AggregateError ? { errors: error.errors.map(switchError) } : {}) } : { message: String(error) };
}
export function createSwitchRoot(): string {
  const parent = fs.realpathSync(process.env.TMPDIR || os.tmpdir());
  return fs.mkdtempSync(path.join(parent, "fabric-nats-switch-"));
}
function persistRuntimeOptions(options: SwitchRuntimeOptions): string {
  fs.mkdirSync(options.root, { recursive: true, mode: 0o700 });
  fs.mkdirSync(options.agentDir, { recursive: true, mode: 0o700 });
  const configFile = path.join(options.root, "runtime.json");
  fs.writeFileSync(configFile, JSON.stringify(options), { mode: 0o600 });
  return configFile;
}
export function startSwitchRuntime(options: SwitchRuntimeOptions): SwitchProcess {
  return new SwitchProcess("runtime", persistRuntimeOptions(options), options.root);
}
export function startSwitchBridge(options: SwitchBridgeOptions): SwitchProcess {
  fs.mkdirSync(options.root, { recursive: true, mode: 0o700 });
  const configFile = path.join(options.root, "bridge.json");
  fs.writeFileSync(configFile, JSON.stringify(options), { mode: 0o600 });
  return new SwitchProcess("bridge", configFile, options.root);
}
export const switchSeenRoot = (meshRoot: string, hostId: string): string =>
  path.join(meshRoot, "control-seen", createHash("sha256").update(hostId).digest("hex").slice(0, 32));

export class SwitchBoundaryMissingError extends Error {
  readonly code = "NATS_SWITCH_BRIDGE_NATIVE_PRESENCE_MISSING";
  constructor(readonly status: SwitchMirrorStatus) {
    super("Native NATS roots have no fresh FILE bridge mirrors; retain NATS, do not flip or synthesize presence");
  }
}
export function requireSwitchMirrors(status: SwitchMirrorStatus, hubId: string, workId: string): void {
  if (!status.linked || !status.nativeHostIds.hub.includes(hubId) || !status.nativeHostIds.work.includes(workId) ||
    !status.mirrors.some(row => row.mesh === "hub" && row.id === workId && row.fresh) ||
    !status.mirrors.some(row => row.mesh === "work" && row.id === hubId && row.fresh)) throw new SwitchBoundaryMissingError(status);
}

export interface SwitchConsumerSnapshot {
  stream: string;
  consumer: string;
  numPending: number;
  numAckPending: number;
  delivered: number;
  ackFloor: number;
  filters: string[];
}
export type SwitchProbeSnapshot = SwitchRoleSnapshot;
export interface SwitchReconciliation { lost: string[]; duplicates: string[] }
export interface SwitchRollbackInput {
  owner: SwitchProcess;
  sender: SwitchProcess;
  bridge: SwitchProcess;
  ownerOptions: SwitchRuntimeOptions;
  senderOptions: SwitchRuntimeOptions;
  bridgeOptions: SwitchBridgeOptions;
  consumers(): Promise<SwitchConsumerSnapshot[]>;
  inFlight: SwitchSend[];
  postFlip: SwitchSend[];
  onRestart(owner: SwitchProcess, sender: SwitchProcess, bridge: SwitchProcess): void;
  onPhase?(event: { type: string; label: string; phase: string; at: number; elapsedMs: number; phaseMs: number; evidence?: unknown }): void;
}
/** Exact STOPALL rehearsal. The controller's IPC producer gate is local test
 * orchestration, NOT a fleet software fence. No invented release receipt, inbox
 * delete/recreate/purge, coordinator or recovery retry is introduced here. */
export async function runSwitchRollback(input: SwitchRollbackInput) {
  const { owner, sender, bridge, ownerOptions, senderOptions } = input;
  const rollbackStartedAt = Date.now();
  const host = (options: SwitchRuntimeOptions) => `session:${options.sessionId}`;
  const timeline: Array<{ phase: string; at: number; elapsedMs: number; phaseMs: number; evidence?: unknown }> = [];
  const mark = (phase: string, evidence?: unknown): number => {
    const at = Date.now();
    const event = { type: "switch-phase", label: "pre-projection", phase, at, elapsedMs: at - rollbackStartedAt,
      phaseMs: at - (timeline.at(-1)?.at ?? rollbackStartedAt), ...(evidence === undefined ? {} : { evidence }) };
    timeline.push(event);
    console.info(JSON.stringify(event));
    input.onPhase?.(event);
    return at;
  };
  const readPair = async (a: SwitchProcess, b: SwitchProcess) => Promise.all([
    a.request<SwitchProbeSnapshot>("probeSnapshot"), b.request<SwitchProbeSnapshot>("probeSnapshot"),
  ]);
  const combined = async (a: SwitchProcess, b: SwitchProcess, requests: readonly SwitchSend[], carrier: "file" | "nats") => {
    let pair = await readPair(a, b);
    const manifest = pair[1]!.sends.filter(send => requests.some(request => request.id === send.probeId));
    if (manifest.length !== requests.length || new Set(manifest.map(send => send.commandId)).size !== requests.length ||
      manifest.some(send => send.result?.accepted !== true)) throw new Error("UNKNOWN/notRun/refused or incomplete native command manifest; retain carrier");
    const ids = manifest.map(send => send.commandId);
    await Promise.all([a.request("probeWaitForEvidence", { ids }), b.request("probeWaitForEvidence", { ids })]);
    const proof = await waitForControlProbeEvidence(manifest, async () => {
      pair = await readPair(a, b); return pair.flatMap(snapshot => snapshot.events);
    }, carrier);
    if (proof.perSend.some(send => !send.accepted || !send.allStagesVerified || send.logicalAdmissionCount !== 1))
      throw new Error("native per-command business/stage evidence incomplete");
    return { ...proof, manifest, snapshots: pair };
  };
  const initialSnapshots = await readPair(owner, sender);
  const mirrors = await bridge.request<SwitchMirrorStatus>("mirrors");
  if (!mirrors.linked) throw new Error("continuous bridge is not linked; retain NATS");
  mark("continuous-bridge-linked", { linked: mirrors.linked });
  if (input.inFlight.length === 0) throw new Error("rollback requires actual sends in flight");
  await owner.request("probeHold");
  const sends = Promise.all(input.inFlight.map(send => sender.request("probeSend", send, 30_000)));
  // Attach rejection handling immediately while waiting for the ingress event.
  void sends.catch(() => undefined);
  try {
    const ingress = await owner.request<{ observed: boolean; source: string }>("probeIngress");
    if (ingress.observed !== true || !["receiver-ingress", "broker-pending"].includes(ingress.source))
      throw new Error("shared probe did not prove real receiver ingress or broker pending");
    const heldSnapshot = await owner.request<SwitchProbeSnapshot>("probeSnapshot");
    if (heldSnapshot.mainJournalSha256 !== initialSnapshots[0]!.mainJournalSha256 ||
      heldSnapshot.events.some(event => event.commandId === (ingress as { commandId?: string }).commandId && event.type === "main-followup-journal-commit"))
      throw new Error("real ingress hold did not remain before native Main journal");
    mark("in-flight-observed-before-journal", { ingress, consumers: await input.consumers(), journalUnchanged: true });
    mark("quiesce-start");
    await Promise.all([owner.request("stopProducers"), sender.request("stopProducers")]);
    mark("quiesce-complete-local-producer-gate-receivers-running");
  } finally { await owner.request("probeRelease"); }
  const apiResults = await sends;
  if (apiResults.some(value => !value || typeof value !== "object" || (value as { accepted?: boolean }).accepted !== true))
    throw new Error("in-flight native API result is UNKNOWN/notRun/refused");
  const preProof = await combined(owner, sender, input.inFlight, "nats");
  const perSend = preProof.perSend;
  for (let index = 0; index < 2; index++) if (JSON.stringify(preProof.snapshots[index]!.eventEvidence) !== JSON.stringify(initialSnapshots[index]!.eventEvidence))
    throw new Error("NATS changed source events/archive bytes");
  const ids = preProof.manifest.map(send => send.commandId);
  const preReconciled = await Promise.all([owner.request<SwitchReconciliation>("probeReconcile", { ids }), sender.request<SwitchReconciliation>("probeReconcile", { ids })]);
  if (preReconciled.some(row => row.lost.length || row.duplicates.length)) throw new Error("native pre-stop journal reconciliation failed");
  mark("native-business-and-shared-stages-complete", { perSend, manifest: preProof.manifest, preReconciled });
  // Owner ACK return is the normal event source. Consumer-info has no event
  // callback for ack-floor convergence, so bounded RPC rechecks (25ms/5s) are
  // only a safety net for the server's ACK-of-ACK bookkeeping after that event.
  const deadline = Date.now() + 5_000;
  const drainSnapshots: Array<{ at: number; consumers: SwitchConsumerSnapshot[] }> = [];
  for (;;) {
    const rows = await input.consumers();
    if (rows.length === 0 || rows.some(row => !Number.isSafeInteger(row.numPending) || !Number.isSafeInteger(row.numAckPending) || row.numPending < 0 || row.numAckPending < 0))
      throw new Error("missing or invalid broker drain consumers");
    drainSnapshots.push({ at: Date.now(), consumers: rows });
    if (rows.every(row => row.numPending === 0 && row.numAckPending === 0)) break;
    if (Date.now() >= deadline) throw new Error("command and ACK consumers did not drain to 0/0; retain NATS");
    await delay(25);
  }
  const initialZeroAt = mark("initial-all-native-inbox-consumers-zero", drainSnapshots.at(-1));
  const stopAllStartedAt = mark("stop-all-start");
  await Promise.all([owner.stop(), sender.stop(), bridge.stop()]);
  const stopped = [owner, sender, bridge].map(process => process.terminal);
  if (stopped.some(row => !row || row.exitCode !== 0 || row.signalCode !== null)) throw new Error("STOPALL native terminal proof missing");
  const stopAllAt = mark("stop-all-completed", stopped);
  // Broker remains up ONLY for read-only inventory. No process may be relaunched
  // before this frozen query; bridge remote-agent termination is in closeResult.
  const frozenConsumers = await input.consumers();
  const identity = (rows: SwitchConsumerSnapshot[]) => JSON.stringify(rows.map(row =>
    ({ stream: row.stream, consumer: row.consumer, filters: row.filters })).sort((a, b) => `${a.stream}/${a.consumer}`.localeCompare(`${b.stream}/${b.consumer}`)));
  if (identity(frozenConsumers) !== identity(drainSnapshots.at(-1)!.consumers)) throw new Error("frozen native inbox inventory changed");
  const frozenRecheckAt = mark("all-stopped-all-native-inbox-recheck", frozenConsumers);
  if (frozenConsumers.some(row => row.numPending !== 0 || row.numAckPending !== 0)) {
    mark("4a-required-unsafe-unimplemented-fail-closed", { attempts: 0 });
    throw new Error("4a owner-only NATS drain required; unsafe/unimplemented, retain NATS configs");
  }
  mark("4a-not-needed", { attempts: 0, durationMs: 0, reason: "all-stopped global inventory 0/0" });
  const checkpointCoverage = [owner, sender].map(process => (process.closeResult as { lifecycle?: { covered?: boolean; entries?: number } })?.lifecycle);
  if (checkpointCoverage.some(row => row?.covered !== true)) throw new Error("checkpoint trace coverage UNKNOWN");
  if (checkpointCoverage.some(row => row!.entries !== 0)) throw new Error("native custody checkpoint hit; retain NATS and escalate actual entry trace");
  // The local seen-root formula is identical across carriers on this revision;
  // retain it in place. Never copy into a guessed agent/fabric directory.
  const fileOptions = (options: SwitchRuntimeOptions): SwitchRuntimeOptions => ({ ...options,
    producerFence: true,
    config: { ...options.config, mesh: { ...options.config.mesh, transport: "file", hostTransport: "file" } } });
  const nextOwnerOptions = fileOptions(ownerOptions), nextSenderOptions = fileOptions(senderOptions);
  const flipStartedAt = mark("config-flip-start");
  const ownerFile = persistRuntimeOptions(nextOwnerOptions), senderFile = persistRuntimeOptions(nextSenderOptions);
  const readbacks = [ownerFile, senderFile].map(file => JSON.parse(fs.readFileSync(file, "utf8")) as SwitchRuntimeOptions);
  for (const [index, original] of [ownerOptions, senderOptions].entries()) {
    const readback = readbacks[index]!;
    if (readback.config.mesh.transport !== "file" || readback.config.mesh.hostTransport !== "file" ||
      readback.meshRoot !== original.meshRoot || readback.agentDir !== original.agentDir || readback.sessionId !== original.sessionId ||
      readback.config.mesh.root !== original.config.mesh.root || readback.config.mesh.stateBackend !== original.config.mesh.stateBackend)
      throw new Error("paired FILE config readback or same-root/backend/identity invariant failed");
  }
  const configFlipAt = mark("both-configs-file-readback", readbacks.map(options => ({ hostId: host(options), root: options.root,
    meshRoot: options.meshRoot, agentDir: options.agentDir, stateBackend: options.config.mesh.stateBackend,
    transport: options.config.mesh.transport, hostTransport: options.config.mesh.hostTransport,
    seenRoot: switchSeenRoot(options.meshRoot, host(options)) })));
  const restartStartedAt = mark("restart-start");
  const nextBridge = startSwitchBridge(input.bridgeOptions);
  const nextOwner = new SwitchProcess("runtime", ownerFile, nextOwnerOptions.root);
  const nextSender = new SwitchProcess("runtime", senderFile, nextSenderOptions.root);
  // Register every new handle immediately, including before any ready can fail.
  input.onRestart(nextOwner, nextSender, nextBridge);
  const restartReady = await Promise.all([nextOwner.request("ready"), nextSender.request("ready"), nextBridge.request("ready")]);
  const readyAt = mark("all-bridge-agent-runtimes-restarted-file", restartReady);
  let fileMirrors: SwitchMirrorStatus | undefined;
  const mirrorDeadline = Date.now() + 10_000;
  for (;;) {
    const candidate = await nextBridge.request<SwitchMirrorStatus>("mirrors");
    const targetMirrors = candidate.mirrors.filter(row =>
      (row.mesh === "hub" && row.id === host(senderOptions)) ||
      (row.mesh === "work" && row.id === host(ownerOptions)));
    const freshAfterRestart = targetMirrors.length === 2 && targetMirrors.every(row =>
      row.fresh && row.observedAt !== undefined && row.observedAt >= restartStartedAt && row.observedAt <= candidate.checkedAt);
    if (candidate.linked && freshAfterRestart) { fileMirrors = candidate; break; }
    if (Date.now() >= mirrorDeadline) throw new Error("fresh native FILE mirrors did not appear after restart; keep FILE publishers fenced");
    await delay(25);
  }
  if (!fileMirrors) throw new Error("fresh native FILE mirror gate did not produce a status; keep FILE publishers fenced");
  requireSwitchMirrors(fileMirrors, host(ownerOptions), host(senderOptions));
  // Validate the real full status above, but keep historical capture identifiers
  // out of printable evidence. Only this rehearsal's fresh owners are reported.
  const reportedMirrors = fileMirrors.mirrors.filter(row =>
    (row.mesh === "hub" && row.id === host(senderOptions)) || (row.mesh === "work" && row.id === host(ownerOptions)));
  const freshFileMirrorGateAt = mark("fresh-file-mirrors-gated", { linked: fileMirrors.linked, checkedAt: fileMirrors.checkedAt,
    nativeHostIds: { hub: fileMirrors.nativeHostIds.hub.filter(id => id === host(ownerOptions)),
      work: fileMirrors.nativeHostIds.work.filter(id => id === host(senderOptions)) },
    mirrors: reportedMirrors, omittedHistoricalMirrors: fileMirrors.mirrors.length - reportedMirrors.length });
  await Promise.all([nextOwner.request("resumeProducers"), nextSender.request("resumeProducers")]);
  const publishersReleasedAt = mark("file-producers-released");
  const reconciled = await Promise.all([nextOwner.request<SwitchReconciliation>("probeReconcile", { ids }), nextSender.request<SwitchReconciliation>("probeReconcile", { ids })]);
  for (const row of reconciled) if (!Array.isArray(row.lost) || !Array.isArray(row.duplicates) || row.lost.length || row.duplicates.length)
    throw new Error("pre-flip commands lost or duplicated after restart");
  mark("preflip-manifest-reconciled", reconciled);
  const postFlip = [];
  for (const send of input.postFlip) postFlip.push(await nextSender.request("probeSend", send, 30_000));
  const fileProof = await combined(nextOwner, nextSender, input.postFlip, "file");
  const postIds = fileProof.manifest.map(send => send.commandId);
  const postReconciled = await Promise.all([nextOwner.request<SwitchReconciliation>("probeReconcile", { ids: postIds }), nextSender.request<SwitchReconciliation>("probeReconcile", { ids: postIds })]);
  if (postReconciled.some(row => row.lost.length || row.duplicates.length)) throw new Error("post-FILE native manifest reconciliation failed");
  mark("postflip-file-sends-verified", { perSend: fileProof.perSend, manifest: fileProof.manifest, postReconciled });
  const completedAt = Date.now();
  return { phase: "file-verified" as const, label: "pre-projection", timeline, perSend, apiResults, drainSnapshots,
    frozenConsumers, stopped, checkpointCoverage, recovery4a: { attempts: 0, durationMs: 0, status: "not-needed" },
    preManifest: preProof.manifest, preReconciled, reconciled, postFlip, filePerSend: fileProof.perSend, postReconciled,
    eventEvidence: { before: initialSnapshots.map(row => row.eventEvidence), afterNats: preProof.snapshots.map(row => row.eventEvidence),
      afterFile: fileProof.snapshots.map(row => row.eventEvidence), natsBytesUnchanged: true },
    sameRoots: readbacks.map(options => ({ root: options.root, meshRoot: options.meshRoot, hostId: host(options),
      stateBackend: options.config.mesh.stateBackend, seenRoot: switchSeenRoot(options.meshRoot, host(options)) })),
    timings: { startedAt: rollbackStartedAt, initialZeroAt, stopAllStartedAt, stopAllAt, frozenRecheckAt, flipStartedAt,
      configFlipAt, restartStartedAt, readyAt, freshFileMirrorGateAt, publishersReleasedAt, completedAt,
      stopAllMs: stopAllAt - stopAllStartedAt, frozenRecheckMs: frozenRecheckAt - stopAllAt,
      configFlipReadbackMs: configFlipAt - flipStartedAt, startMs: readyAt - restartStartedAt,
      mirrorWarmupGapMs: freshFileMirrorGateAt - readyAt, inclusiveColdStartMirrorMs: freshFileMirrorGateAt - restartStartedAt,
      publisherOutageMs: publishersReleasedAt - timeline.find(row => row.phase === "quiesce-start")!.at,
      totalRollbackMs: completedAt - rollbackStartedAt },
    limits: ["shared v2 ownerAckAtNs/allStagesVerified does not inspect published ACK operation acceptance; accepted API + journal reconciliation independently required",
      "FILE inbox confirmations qualify the in-memory native reader cursor, not broker/durable ACK", "actors are extra/not exercised"],
    coverage: { operations: [...new Set([...input.inFlight, ...input.postFlip].map(send => send.ref))],
      actorModelBoundary: "not supplied by this orchestration fixture", seenDirectoryHandoff: "same mesh/control-seen host hash; retained in place" } };
}

/** Called only by owned Node children, never on helper import. */
export async function runSwitchChild(mode: string, configFile: string): Promise<void> {
  const input = JSON.parse(fs.readFileSync(configFile, "utf8")) as SwitchRuntimeOptions & SwitchBridgeOptions;
  let closed = false;
  let initialization: Promise<unknown>;
  let dispatch: (method: string, args: Record<string, unknown>) => Promise<unknown>;
  let close: () => Promise<unknown>;
  if (mode === "runtime") {
    const options: SwitchRuntimeOptions = input;
    const relative = path.relative(options.meshRoot, options.agentDir);
    if (!relative || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative)))
      throw new Error("switch fixture agent directory must be outside mesh");
    Object.assign(process.env, { PI_CODING_AGENT_DIR: options.agentDir, PI_FABRIC_PROJECT_ROOT: options.root,
      PI_FABRIC_RUN_ROOT: path.join(options.root, "runs"), PI_FABRIC_DATA_ROOT: path.join(options.root, "data") });
    const [{ FabricRuntimeState }, { CapturedToolCatalog }] = await Promise.all([
      import("../../src/fabric-runtime-state.js"), import("../../src/capture/catalog.js"),
    ]);
    const messages: Array<{ message: unknown; options: unknown }> = [];
    const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
      getSessionName: () => options.name, appendEntry() {}, sendMessage(message: unknown, deliveryOptions: unknown) {
        messages.push({ message, options: deliveryOptions });
      },
    } as unknown as ExtensionAPI;
    const context = { cwd: options.root, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
      isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => undefined },
      sessionManager: { getSessionId: () => options.sessionId, getSessionFile: () => undefined,
        getBranch: () => [], getLeafId: () => null, getEntries: () => [] }, ui: { setStatus() {}, notify() {} },
    } as unknown as ExtensionContext;
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
      extension: options.extensionPath ?? path.resolve("dist/index.js"), worker: options.workerPath ?? path.join(options.root, "unused-worker.mjs"),
      residentHost: path.join(options.root, "unused-resident.mjs"), skills: options.root,
    } });
    const invoke = (ref: string, args: Record<string, unknown>) => runtime.registry.invoke(ref, args, {
      cwd: options.root, signal: undefined, parentToolCallId: "switch-rehearsal", nestedToolCallId: ref, extensionContext: context,
      update() {}, approve: async () => {}, audits: [], maxResultChars: 30_000,
    });
    let probe: SwitchProbe | undefined;
    let producersStopped = options.producerFence === true;
    initialization = (async () => {
      if (options.probeModule) {
        const module = await import(pathToFileURL(options.probeModule).href) as {
          installSwitchProbe(context: SwitchProbeContext): SwitchProbe | Promise<SwitchProbe>;
        };
        probe = await module.installSwitchProbe({ runtime, context, invoke, options });
      }
      await runtime.initialize(context, options.config);
      const lifecycle = probe?.afterInitialize?.();
      return { lifecycle, pid: process.pid, hostId: `session:${options.sessionId}`, meshRoot: runtime.mesh.root,
        stateBackend: runtime.mesh.stateBackend, transport: runtime.config.mesh.transport,
        probeInstalled: Boolean(probe), seenRoot: switchSeenRoot(runtime.mesh.root, `session:${options.sessionId}`) };
    })();
    close = async () => {
      let lifecycle: unknown;
      try {
        probe?.lifecycleSnapshot?.();
        await runtime.shutdown("exit");
        lifecycle = probe?.lifecycleSnapshot?.(true);
        return { lifecycle, shutdownCompletedAt: Date.now() };
      } finally { await probe?.close?.(); }
    };
    dispatch = async (method, args) => {
      if (method === "ready") return initialization;
      await initialization;
      if (method === "routes") return runtime.participantInfos({ includeStale: true });
      if (method === "messages") return messages;
      // Producer-only gate: receiver inbox, ACK handling, leases and bridge run.
      // In particular this NEVER invokes control.pause() before broker drain.
      if (method === "stopProducers") { producersStopped = true; return { stoppedAt: Date.now() }; }
      if (method === "resumeProducers") { producersStopped = false; return { resumedAt: Date.now() }; }
      if (method === "invoke" || method === "probeSend") {
        if (producersStopped) throw new Error("switch producers stopped");
        return method === "invoke" ? invoke(String(args.ref), args.args as Record<string, unknown>) : requireProbe().send(args as unknown as SwitchSend);
      }
      if (method === "probeHold") return requireProbe().hold();
      if (method === "probeIngress") return requireProbe().waitForIngress();
      if (method === "probeRelease") return requireProbe().release();
      if (method === "probeSnapshot") return requireProbe().snapshot();
      if (method === "probeReconcile") return requireProbe().reconcile(args.ids as string[]);
      if (method === "probeWaitForEvidence") {
        const installed = requireProbe();
        if (!installed.waitForEvidence) throw new Error("shared probe local stage barrier required");
        return installed.waitForEvidence(args.ids as string[]);
      }
      throw new Error(`unknown switch runtime method: ${method}`);
    };
    function requireProbe(): SwitchProbe {
      if (!probe) throw Object.assign(new Error("Shared send-to-admission probe is not installed"), { code: "NATS_SWITCH_PROBE_REQUIRED" });
      return probe;
    }
  } else {
    const options: SwitchBridgeOptions = input;
    const [{ MeshStore }, { MeshBridge, StoreBridgeSide, RemoteBridgeSide, serveBridgeAgent }, { readHostLeases, MIRROR_FRESH_MS }] = await Promise.all([
      import("../../src/mesh/store.js"), import("../../src/mesh/bridge.js"), import("../../src/topology/host-leases.js"),
    ]);
    if (mode === "bridge-agent") {
      const store = new MeshStore(options.workMesh, 65_536, 1_000, { stateBackend: options.workBackend });
      try { await serveBridgeAgent(new StoreBridgeSide(store, "hub", Date.now, true), process.stdin, process.stdout); }
      finally { store.closeState(); }
      return;
    }
    const hub = new MeshStore(options.hubMesh, 65_536, 1_000, { stateBackend: options.hubBackend });
    const local = new StoreBridgeSide(hub, "work", Date.now, true);
    const agent = spawn(process.execPath, ["--input-type=module", "-e", BOOTSTRAP, helperPath, "bridge-agent", configFile], {
      cwd: process.cwd(), env: process.env, stdio: ["pipe", "pipe", "inherit"],
    });
    const remote = new RemoteBridgeSide(agent.stdout!, agent.stdin!);
    const bridge = new MeshBridge({ localName: "hub", remoteName: "work", local, remote,
      cursorPath: path.join(options.root, "cursor.json"), stopMs: 2_000 });
    let failure: unknown;
    initialization = bridge.start().then(() => ({ pid: process.pid, agentPid: agent.pid, linked: true }));
    const running = initialization.then(() => bridge.run()).catch(error => { failure = error; });
    const stage = (stage: string) => process.send?.({ event: "bridge-close-stage", result: { stage, at: Date.now(), pid: process.pid,
      agentPid: agent.pid, agentExitCode: agent.exitCode, agentSignalCode: agent.signalCode } });
    close = async () => {
      const errors: unknown[] = [];
      stage("bridge.stop-enter");
      try { await bridge.stop(); stage("bridge.stop-complete"); } catch (error) { errors.push(error); }
      // MeshBridge.stop already owns remote.close/EOF. Idempotent close here is
      // failure cleanup only; do not change product stdio or swallow its EPIPE.
      try { remote.close(); stage("remote.close-and-eof-complete"); } catch (error) { errors.push(error); }
      if (agent.exitCode === null && agent.signalCode === null) {
        const exited = once(agent, "exit");
        let forced = false;
        const term = setTimeout(() => { forced = true; agent.kill("SIGTERM"); }, 3_000);
        const kill = setTimeout(() => agent.kill("SIGKILL"), 5_000);
        try { await exited; } catch (error) { errors.push(error); }
        finally { clearTimeout(term); clearTimeout(kill); }
        if (forced) errors.push(new Error("remote bridge agent required forced shutdown"));
      }
      stage("agent-exit-observed");
      try { await running; } catch (error) { errors.push(error); }
      try { hub.closeState(); } catch (error) { errors.push(error); }
      stage("running-joined-state-closed");
      if (agent.exitCode !== 0 || agent.signalCode !== null) errors.push(new Error(`remote bridge agent terminal status failed: pid=${agent.pid} exit=${agent.exitCode} signal=${agent.signalCode}`));
      if (errors.length) throw new AggregateError(errors, "native bridge close failed; normal terminal proof unavailable");
      return { agent: { pid: agent.pid, exitCode: agent.exitCode, signalCode: agent.signalCode, completedAt: Date.now() },
        bridgeStopped: true, cursorPath: path.join(options.root, "cursor.json") };
    };
    dispatch = async (method) => {
      if (method === "ready") return initialization;
      await initialization;
      if (failure) throw failure;
      if (method === "mirrors") {
        // This is the product mirror commit, not hand-written topology. It is a
        // deterministic barrier; it cannot manufacture missing native presence.
        await bridge.syncPresence();
        const [hubPresence, workPresence] = await Promise.all([local.presence(), remote.presence()]);
        const work = new MeshStore(options.workMesh, 65_536, 1_000, { stateBackend: options.workBackend });
        try {
          const now = Date.now();
          const mirrors = ([ ["hub", hub], ["work", work] ] as const).flatMap(([mesh, store]) =>
            [...readHostLeases(store.root).values()].filter(lease => lease.mirror).map(lease => ({ mesh, id: lease.id,
              observedAt: lease.mirror!.observedAt, expiresAt: lease.expiresAt,
              fresh: lease.expiresAt > now && now - lease.mirror!.observedAt < MIRROR_FRESH_MS && lease.mirror!.observedAt <= now })));
          return { linked: true, checkedAt: now, nativeHostIds: { hub: hubPresence.hosts.map(row => row.record.id),
            work: workPresence.hosts.map(row => row.record.id) }, mirrors } satisfies SwitchMirrorStatus;
        } finally { work.closeState(); }
      }
      throw new Error(`unknown switch bridge method: ${method}`);
    };
  }
  const deadline = setTimeout(() => { void finish().finally(() => process.exit(124)); }, 180_000);
  let finishing: Promise<unknown> | undefined;
  const finish = () => finishing ??= (async () => { closed = true; clearTimeout(deadline); return close(); })();
  process.on("message", (value: { id: number; method: string; args: Record<string, unknown> }) => {
    // Concurrent IPC means a held send cannot block ingress/stop/release commands.
    void (async () => {
      try {
        const result = value.method === "close" ? await finish() : await dispatch(value.method, value.args);
        process.send?.({ id: value.id, ok: true, result }, () => {
          if (value.method === "close") { process.disconnect?.(); process.exit(0); }
        });
      } catch (error) {
        process.send?.({ id: value.id, ok: false, error: { message: error instanceof Error ? error.message : String(error),
          ...(error instanceof Error ? { stack: error.stack } : {}), details: switchError(error),
          ...(typeof error === "object" && error !== null && "code" in error ? { code: error.code } : {}) } }, () => {
            // Failed close must not hang until SIGTERM and masquerade as exit 0.
            if (value.method === "close") { process.disconnect?.(); process.exit(1); }
          });
      }
    })();
  });
  process.once("SIGTERM", () => { void finish().finally(() => process.exit(0)); });
  process.once("disconnect", () => { void finish().finally(() => process.exit(0)); });
}
