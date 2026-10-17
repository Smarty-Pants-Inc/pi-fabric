import { createHash, randomUUID } from "node:crypto";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { MeshStore } from "../../src/mesh/store.js";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricRuntimeState } from "../../src/fabric-runtime-state.js";

export type ControlProbeOperation = "followUp" | "steer";

export type ProbeBackend = "FILE-fixed" | "SQLite-fixed" | "NATS Path B";
export type ProbeStateBackend = "file" | "sqlite";
export type ProbeLoadShape = "steady-14-ops-per-second" | "synchronized-burst";
export interface ProbeNatsOptions {
  readonly servers: string | readonly string[];
  readonly credentials: string;
  readonly machine: string;
  readonly custodyStream: string;
  readonly livenessStream: string;
  readonly pathBMachines: readonly string[];
  readonly tls?: { readonly ca?: string; readonly caFile?: string };
  readonly operatorMachine?: string;
}
export interface ProbeMeshRoots {
  readonly hub?: string;
  readonly work?: string;
  readonly owner?: string;
  readonly sender?: string;
}
export interface ControlProbeOptions {
  readonly n: number;
  readonly backend: ProbeBackend;
  readonly transport: "file" | "nats";
  readonly nats?: ProbeNatsOptions;
  readonly natsOwner?: ProbeNatsOptions;
  readonly natsSender?: ProbeNatsOptions;
  readonly runtimeConfig?: Record<string, unknown>;
  readonly presenceProjectionLabel?: "pre-projection" | "enabled";
  readonly auxiliaryProcesses?: readonly { readonly label: string; readonly pid: number }[];
  readonly headroomLimitBytes?: number;
  readonly capacityAdmission?: Record<string, unknown>;
  readonly meshRoots?: ProbeMeshRoots;
  readonly stateBackendHub?: ProbeStateBackend;
  readonly stateBackendWork?: ProbeStateBackend;
  readonly loadShape?: ProbeLoadShape;
  readonly ratePerSecond?: number;
  readonly senderProcesses?: number;
  readonly serverLabel?: string;
  readonly syncInterval?: string;
  readonly deadlineMs?: number;
  readonly holdAdmission?: boolean;
  readonly holdWaitMs?: number;
  readonly operation?: ControlProbeOperation;
}
interface WorkerReady {
  readonly role: "owner" | "sender";
  readonly sessionId: string;
  readonly participantId: string;
  readonly meshRoot: string;
  readonly agentDir: string;
  readonly mainJournal: string;
  readonly stateBackend: string;
  readonly transport: string;
  readonly ownerHostId: string;
  readonly ownerIdentityId: string;
  readonly ownerIncarnation: string;
}
export interface ControlProbeEvent {
  readonly type: string;
  readonly observationId?: string;
  readonly atNs?: string;
  readonly carrier?: "nats" | "file";
  readonly envelopeId?: string;
  readonly commandId?: string;
  readonly kind?: "command" | "ack";
  readonly operation?: string;
  readonly targetId?: string;
  readonly sequence?: number;
  readonly duplicate?: boolean;
  readonly attempt?: number;
  readonly messageId?: string;
  readonly role?: "owner" | "sender";
  readonly hostId?: string;
}
type WorkerEvent = ControlProbeEvent;

export interface ControlProbeSendResult {
  readonly probeId: string;
  readonly invocationAtNs: string;
  readonly invocationAtEpochNs?: string;
  readonly acceptedAckObservedAtNs?: string;
  readonly acceptedAckObservedAtEpochNs?: string;
  readonly resolvedAtNs?: string;
  readonly accepted: boolean;
  readonly result?: unknown;
  readonly error?: string;
  readonly lost?: boolean;
}

type SendResult = ControlProbeSendResult;

/** A correlation manifest, not an admission assertion. Results and events may come from different roles/processes. */
export interface ControlProbeSend {
  readonly probeId: string;
  readonly commandId: string;
  readonly result?: ControlProbeSendResult | undefined;
}

const repoRoot = fileURLToPath(new URL("../..", import.meta.url));
const workerPath = path.join(repoRoot, "scripts/nats-path-b-probe-worker.mjs");
const ROOT_INBOX_PREFIX = "topology/inbox/";
const DEFAULT_DEADLINE_MS = 30_000;

class WorkerClient {
  readonly child: ChildProcessWithoutNullStreams;
  readonly ready: Promise<WorkerReady>;
  readonly #pending = new Map<string, { resolve(value: unknown): void; reject(error: Error): void; timer: NodeJS.Timeout }>();
  readonly #closed: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  #readyResolve!: (value: WorkerReady) => void;
  #readyReject!: (error: Error) => void;
  #readySettled = false;
  #stderr = "";
  #stdoutLine = "";
  #peakRssBytes: number | undefined;

  constructor(configPath: string, env: NodeJS.ProcessEnv) {
    this.child = spawn(process.env.PI_FABRIC_PROBE_RUNTIME ?? "bun", [workerPath, configPath], {
      cwd: repoRoot, env, stdio: ["pipe", "pipe", "pipe"],
    });
    this.ready = new Promise((resolve, reject) => { this.#readyResolve = resolve; this.#readyReject = reject; });
    this.#closed = new Promise(resolve => this.child.once("close", (code, signal) => resolve({ code, signal })));
    this.child.stdout.setEncoding("utf8");
    this.child.stdout.on("data", (chunk: string) => {
      this.#stdoutLine += chunk;
      for (;;) {
        const end = this.#stdoutLine.indexOf("\n");
        if (end < 0) break;
        const line = this.#stdoutLine.slice(0, end);
        this.#stdoutLine = this.#stdoutLine.slice(end + 1);
        this.#handleLine(line);
      }
    });
    this.child.stderr.setEncoding("utf8");
    this.child.stderr.on("data", (chunk: string) => {
      this.#stderr = (this.#stderr + chunk).slice(-8_000);
    });
    this.child.once("error", error => this.#fail(error));
    this.child.once("close", (code, signal) => this.#fail(new Error(
      `probe worker closed (code=${code}, signal=${signal})${this.#stderr ? `: ${this.#stderr}` : ""}`)));
  }

  #handleLine(line: string): void {
    let value: unknown;
    try { value = JSON.parse(line); } catch { this.#fail(new Error("probe worker emitted invalid JSON")); return; }
    if (!value || typeof value !== "object") return;
    const record = value as Record<string, unknown>;
    if (record.type === "ready") {
      this.#readySettled = true;
      this.#readyResolve(record as unknown as WorkerReady);
    } else if (record.type === "fatal") {
      const error = new Error(`probe worker startup failed: ${String(record.error)}`);
      if (!this.#readySettled) { this.#readySettled = true; this.#readyReject(error); }
      this.#fail(error);
    } else if (record.type === "response" && typeof record.requestId === "string") {
      const pending = this.#pending.get(record.requestId);
      if (!pending) return;
      this.#pending.delete(record.requestId);
      clearTimeout(pending.timer);
      if (typeof record.error === "string") pending.reject(new Error(record.error));
      else pending.resolve(record.value);
    }
  }

  #fail(error: Error): void {
    if (!this.#readySettled) { this.#readySettled = true; this.#readyReject(error); }
    for (const [id, pending] of this.#pending) {
      this.#pending.delete(id);
      clearTimeout(pending.timer);
      pending.reject(error);
    }
  }

  sampleRss(): number | undefined {
    const current = this.child.pid === undefined ? undefined : processRssBytes(this.child.pid);
    if (current !== undefined) this.#peakRssBytes = Math.max(this.#peakRssBytes ?? 0, current);
    return this.#peakRssBytes;
  }

  get peakRssBytes(): number | undefined { return this.#peakRssBytes; }

  request<T>(type: string, body: Record<string, unknown> = {}, timeoutMs = 45_000): Promise<T> {
    const requestId = randomUUID();
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(requestId);
        reject(new Error(`probe worker request timed out: ${type}`));
      }, timeoutMs);
      timer.unref();
      this.#pending.set(requestId, { resolve: resolve as (value: unknown) => void, reject, timer });
      this.child.stdin.write(`${JSON.stringify({ type, requestId, ...body })}\n`, error => {
        if (!error) return;
        const pending = this.#pending.get(requestId);
        if (!pending) return;
        this.#pending.delete(requestId);
        clearTimeout(pending.timer);
        pending.reject(error);
      });
    });
  }

  async stop(): Promise<void> {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    try { await this.request("shutdown", {}, 60_000); } catch { /* close below reports ownership evidence */ }
    this.child.stdin.end();
    const closed = await Promise.race([this.#closed, sleep(10_000).then(() => undefined)]);
    if (!closed) throw new Error("owned probe worker did not exit after graceful shutdown");
    if (closed.code !== 0) throw new Error(`probe worker exit status ${closed.code} (${closed.signal})`);
  }
}

function processRssBytes(pid: number): number | undefined {
  try {
    const status = fs.readFileSync(`/proc/${pid}/status`, "utf8");
    const match = /^VmRSS:\s+(\d+)\s+kB$/m.exec(status);
    return match ? Number(match[1]) * 1_024 : undefined;
  } catch { return undefined; }
}

const privateRunDir = (): string => {
  const parent = fs.realpathSync.native(process.env.TMPDIR?.trim() || os.tmpdir());
  const dir = fs.realpathSync.native(fs.mkdtempSync(path.join(parent, "nats-path-b-probe-")));
  fs.chmodSync(dir, 0o700);
  return dir;
};

const sha256File = (file: string): string | null => {
  try { return createHash("sha256").update(fs.readFileSync(file)).digest("hex"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
};

function archiveTree(dir: string): { readonly sha256: string | null; readonly fileCount: number } {
  const hash = createHash("sha256");
  let fileCount = 0;
  const visit = (current: string, relative: string): void => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const child = path.join(current, entry.name);
      const childRelative = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isDirectory()) visit(child, childRelative);
      else if (entry.isFile()) {
        hash.update(childRelative).update("\0").update(fs.readFileSync(child)).update("\0");
        fileCount++;
      } else if (entry.isSymbolicLink()) {
        hash.update(childRelative).update("\0symlink\0").update(fs.readlinkSync(child)).update("\0");
        fileCount++;
      }
    }
  };
  try { visit(dir, ""); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { sha256: null, fileCount: 0 };
    throw error;
  }
  return { sha256: hash.digest("hex"), fileCount };
}

export function eventEvidence(root: string) {
  const archiveConfig = path.join(root, "event-archive.json");
  const configSha256 = sha256File(archiveConfig);
  let archiveDir: string | undefined;
  if (configSha256 !== null) {
    const parsed = JSON.parse(fs.readFileSync(archiveConfig, "utf8")) as { dir?: unknown };
    if (typeof parsed.dir !== "string" || !path.isAbsolute(parsed.dir)) throw new Error(`invalid mesh event archive config: ${archiveConfig}`);
    archiveDir = parsed.dir;
  }
  const archive = archiveDir ? archiveTree(archiveDir) : { sha256: null, fileCount: 0 };
  return {
    eventsJsonlSha256: sha256File(path.join(root, "events.jsonl")),
    eventArchive: { configured: archiveDir !== undefined, configSha256, treeSha256: archive.sha256, fileCount: archive.fileCount },
  };
}

function cursorKey(participantId: string): string {
  const token = createHash("sha256").update(participantId).digest("hex").slice(0, 32);
  return `${ROOT_INBOX_PREFIX}${token}`;
}

function readCursorSnapshots(participants: readonly { id: string; root: string; backend: ProbeStateBackend }[]) {
  const stores = new Map<string, MeshStore>();
  const output: Record<string, unknown> = {};
  try {
    for (const participant of participants) {
      const root = path.resolve(participant.root);
      let mesh = stores.get(root);
      if (!mesh) {
        mesh = new MeshStore(root, 65_536, 1_000, { stateBackend: participant.backend });
        if (mesh.stateBackend !== participant.backend) throw new Error(`requested ${participant.backend} but cursor reader opened ${mesh.stateBackend} at ${root}`);
        stores.set(root, mesh);
      }
      const entry = mesh.get(cursorKey(participant.id), { fresh: true });
      output[participant.id] = entry ? { version: entry.version, value: entry.value } : null;
    }
    return output;
  } finally { for (const mesh of stores.values()) mesh.closeState(); }
}

function readJsonLines(file: string): WorkerEvent[] {
  try {
    return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as WorkerEvent);
  } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
}

function fileControlEventEvidence(root: string, commandIds: ReadonlySet<string>) {
  const file = path.join(root, "events.jsonl");
  try {
    const matched = fs.readFileSync(file, "utf8").split("\n").filter(Boolean).flatMap(line => {
      try {
        const event = JSON.parse(line) as { topic?: unknown; data?: unknown };
        const body = event.data && typeof event.data === "object" && !Array.isArray(event.data)
          ? event.data as Record<string, unknown> : undefined;
        if (!body || typeof body.commandId !== "string" || !commandIds.has(body.commandId)) return [];
        return [{ topic: event.topic, commandId: body.commandId }];
      } catch { return []; }
    });
    return { commandAppends: matched.filter(event => event.topic === "fabric.control.command").length,
      acceptedAckAppends: matched.filter(event => event.topic === "fabric.control.ack").length, matched };
  } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return { commandAppends: 0, acceptedAckAppends: 0, matched: [] }; throw error; }
}

function expectedCommandId(input: {
  senderHostId: string; ownerHostId: string; ownerIdentityId: string; targetId: string; operation: string; idempotencyKey: string;
}): string {
  return createHash("sha256").update(JSON.stringify([input.senderHostId, input.ownerHostId, input.ownerIdentityId,
    input.targetId, input.operation, input.idempotencyKey, 0])).digest("hex");
}

/** The same assembler serves the one-shot probe and externally managed real Main processes.
 * Combine actual owner and sender events here; a role-local snapshot is deliberately not full proof.
 * Stage timestamps are same-host epoch nanoseconds, except the explicitly monotonic sender pair.
 */
export function collectControlProbeEvidence(sends: readonly ControlProbeSend[], inputEvents: readonly ControlProbeEvent[], transport: "file" | "nats") {
  const seen = new Set<string>();
  const events = inputEvents.filter(event => {
    if (!event.observationId) return true;
    if (seen.has(event.observationId)) return false;
    seen.add(event.observationId);
    return true;
  });
  return sends.map(item => {
    const result = item.result;
    const records = events.filter(event => event.commandId === item.commandId);
    const ownerCommit = records.find(event => event.role === "owner" && event.type === "main-followup-journal-commit");
    const commandPubAck = records.find(event => event.role === "sender" && event.type === "transport-puback" && event.kind === "command");
    const commandPublished = commandPubAck ?? records.find(event => event.role === "sender" && event.type === "file-append" && event.kind === "command");
    const ownerAck = records.find(event => event.role === "owner" && (event.type === "transport-puback" && event.kind === "ack" ||
      event.type === "file-append" && event.kind === "ack"));
    const ackOfAck = records.find(event => event.role === "owner" && event.type === "inbox-ack-confirmed" && event.kind === "command");
    const senderAckOfAck = records.find(event => event.role === "sender" && event.type === "inbox-ack-confirmed" && event.kind === "ack");
    const invocationAtNs = result?.invocationAtNs || null;
    const invocationAtEpochNs = result?.invocationAtEpochNs ?? null;
    const acceptedAckObservedAtNs = result?.acceptedAckObservedAtNs ?? null;
    const acceptedAckObservedAtEpochNs = result?.acceptedAckObservedAtEpochNs ?? null;
    const latencyMs = invocationAtNs && acceptedAckObservedAtNs
      ? Number(BigInt(acceptedAckObservedAtNs) - BigInt(invocationAtNs)) / 1_000_000 : null;
    const sameHostSecondaryMs = invocationAtEpochNs && ownerCommit?.atNs
      ? Number(BigInt(ownerCommit.atNs) - BigInt(invocationAtEpochNs)) / 1_000_000 : null;
    const logicalAdmissionCount = records.filter(event => event.role === "owner" && event.type === "main-followup-journal-commit").length;
    return {
      probeId: item.probeId,
      commandId: item.commandId,
      messageId: ownerCommit?.messageId ?? (result?.result && typeof result.result === "object"
        ? (result.result as Record<string, unknown>).messageId ?? null : null),
      accepted: result?.accepted ?? false,
      invocationAtNs, invocationAtEpochNs, acceptedAckObservedAtNs, acceptedAckObservedAtEpochNs,
      primaryLatencyMs: latencyMs,
      logicalAdmissionCount,
      pubAckAtNs: commandPubAck?.atNs ?? null,
      commandPublishedAtNs: commandPublished?.atNs ?? null,
      ownerAckAtNs: ownerAck?.atNs ?? null,
      ackOfAckAtNs: ackOfAck?.atNs ?? null,
      senderAckOfAckAtNs: senderAckOfAck?.atNs ?? null,
      ownerJournalCommitAtNs: ownerCommit?.atNs ?? null,
      secondarySameHostJournalCommitMs: sameHostSecondaryMs,
      redeliveryAttempts: records.filter(event => event.role === "owner" && event.type === "inbox-ack-confirmed" && event.kind === "command")
        .reduce((sum, event) => sum + Math.max(0, (event.attempt ?? 1) - 1), 0),
      allStagesVerified: result?.accepted === true && latencyMs !== null && logicalAdmissionCount === 1 &&
        !!commandPublished && !!ownerAck && (transport === "file" || !!ackOfAck && !!senderAckOfAck),
      error: result?.error ?? null,
    };
  });
}

export interface ControlProbeInstallation {
  readonly runtime: FabricRuntimeState;
  readonly context: ExtensionContext;
  readonly invoke: (ref: string, args: Record<string, unknown>) => Promise<unknown>;
  readonly role: "owner" | "sender";
  /** Caller-owned private file outside the mesh. Retain this same file across graceful process restarts. */
  readonly evidenceFile: string;
  readonly ingressTimeoutMs?: number;
}

export interface ControlProbeInvocation {
  readonly probeId: string;
  readonly operation: ControlProbeOperation;
  readonly targetId: string;
  readonly message: string;
  readonly data?: unknown;
}

type ProbeStoredRecord = { readonly version: 1; readonly hostId: string; readonly role: "owner" | "sender"; readonly writtenAt: number } &
  ({ readonly type: "event"; readonly event: ControlProbeEvent } | { readonly type: "send"; readonly send: ControlProbeSend });
let suppliedRuntimeProbeActive = false;

/** Install BEFORE initialize on one real Main per process. No runtime, transport, bridge, identity,
 * root, producer fence or shutdown is created/owned here. Optional runtime imports stay first-use.
 * Explicit hold/release replaces only the private worker's pre-acceptControl wait for this Main.
 * close releases any held ingress, waits owned sends and restores instrumentation; it never deletes evidence.
 */
export async function installControlProbe(input: ControlProbeInstallation) {
  if (suppliedRuntimeProbeActive) throw new Error("only one supplied-runtime control probe may be installed per process");
  suppliedRuntimeProbeActive = true;
  try {
    const [{ AgentMessageRouter }, { observeFabricPathBProbe }] = await Promise.all([
      import("../../src/providers/agents-message-router.js"), import("../../src/transport/probe-hooks.js"),
    ]);
    const hostId = `session:${input.context.sessionManager.getSessionId()}`;
    const evidenceFile = path.resolve(input.evidenceFile);
    fs.mkdirSync(path.dirname(evidenceFile), { recursive: true, mode: 0o700 });
    const read = (): ProbeStoredRecord[] => {
      let text: string;
      try { text = fs.readFileSync(evidenceFile, "utf8"); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
      return text.split("\n").filter(Boolean).map(line => {
        const record = JSON.parse(line) as ProbeStoredRecord;
        if (record.version !== 1 || record.hostId !== hostId || record.role !== input.role ||
          !["event", "send"].includes(record.type)) throw new Error("control probe evidence identity/schema mismatch");
        return record;
      });
    };
    read();
    const evidenceListeners = new Set<() => void>();
    const notifyEvidence = () => { for (const listener of evidenceListeners) listener(); };
    let evidenceError: unknown;
    const append = (body: { type: "event"; event: ControlProbeEvent } | { type: "send"; send: ControlProbeSend }): void => {
      try { fs.appendFileSync(evidenceFile, `${JSON.stringify({ version: 1, hostId, role: input.role, writtenAt: Date.now(), ...body })}\n`, { mode: 0o600 }); }
      catch (error) { evidenceError = error; }
    };
    const assertEvidence = (): void => { if (evidenceError) throw evidenceError; };
    let closed = false;
    let ingress: { observed: true; source: "receiver-ingress"; commandId: string; operation: string; atNs: string } | undefined;
    let enter: ((value: NonNullable<typeof ingress>) => void) | undefined;
    let entered: Promise<NonNullable<typeof ingress>> | undefined;
    let releaseHold: (() => void) | undefined;
    let held: Promise<void> | undefined;
    const pending = new Set<Promise<ControlProbeSendResult>>();
    const assertOpen = (): void => { if (closed) throw new Error("control probe installation is closed"); assertEvidence(); };
    const original = AgentMessageRouter.prototype.acceptControl;
    const wrapped: typeof original = async function (this: InstanceType<typeof AgentMessageRouter>, command, ...args) {
      if (held && this.mainAgent.local && this.mainAgent.id === hostId &&
        (command.operation === "followUp" || command.operation === "steer")) {
        if (!ingress) {
          ingress = { observed: true, source: "receiver-ingress", commandId: command.commandId,
            operation: command.operation, atNs: process.hrtime.bigint().toString() };
          enter?.(ingress);
        }
        await held;
      }
      return original.call(this, command, ...args);
    };
    AgentMessageRouter.prototype.acceptControl = wrapped;
    const restoreObserver = observeFabricPathBProbe(event => {
      append({ type: "event", event: { ...event, role: input.role, hostId, observationId: randomUUID() } });
      notifyEvidence();
    });
    const release = () => { releaseHold?.(); held = undefined; releaseHold = undefined; return { released: true }; };
    const snapshot = () => {
      assertEvidence();
      const records = read();
      const events = records.flatMap(row => row.type === "event" ? [row.event] : []);
      const sends = records.flatMap(row => row.type === "send" ? [row.send] : []);
      const root = input.runtime.mesh.root;
      const relative = path.relative(root, evidenceFile);
      if (!relative || relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative))
        throw new Error("control probe evidence file must be outside the mesh");
      const journal = path.join(root, "main-followups", `${encodeURIComponent(input.context.sessionManager.getSessionId())}.json`);
      const nativeJson = (file: string): Record<string, unknown> | undefined => {
        try { return JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>; }
        catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
      };
      const currentJournal = nativeJson(journal);
      const delivered = nativeJson(`${journal}.delivered`);
      if (currentJournal && (currentJournal.version !== 1 || !Array.isArray(currentJournal.items)) ||
        delivered && (delivered.version !== 1 || !Array.isArray(delivered.ids))) throw new Error("invalid native Main admission journal/index");
      const nativeAdmissions = (currentJournal?.items as Array<{ deliveryId?: string; id?: string }> | undefined ?? [])
        .flatMap(item => typeof item.deliveryId === "string" ? [{ commandId: item.deliveryId, messageId: item.id ?? null }] : []);
      const nativeConsumedIds = (delivered?.ids as unknown[] | undefined ?? []).filter((id): id is string => typeof id === "string");
      return { hostId, role: input.role, roleEvidenceOnly: true as const, events, sends, nativeAdmissions, nativeConsumedIds,
        perSend: collectControlProbeEvidence(sends, events, input.runtime.config.mesh.transport),
        commandIds: [...new Set([...events.flatMap(event => event.commandId ? [event.commandId] : []), ...sends.map(send => send.commandId)])],
        lastMarkedAt: records.reduce((last, row) => Math.max(last, row.writtenAt), 0),
        eventEvidence: eventEvidence(root),
        mainJournalSha256: sha256File(journal),
      };
    };
    return {
      hold() {
        assertOpen();
        if (held) throw new Error("control probe admission hold is already armed");
        ingress = undefined;
        entered = new Promise(resolve => { enter = resolve; });
        held = new Promise(resolve => { releaseHold = resolve; });
        return { armed: true };
      },
      async waitForIngress() {
        assertOpen();
        if (!entered) throw new Error("arm the control probe hold before waiting for ingress");
        let timer: NodeJS.Timeout | undefined;
        try {
          return await Promise.race([entered, new Promise<never>((_resolve, reject) => {
            timer = setTimeout(() => reject(new Error("control probe ingress deadline exceeded")), input.ingressTimeoutMs ?? 10_000);
          })]);
        } finally { if (timer) clearTimeout(timer); }
      },
      release,
      async send(request: ControlProbeInvocation) {
        assertOpen();
        const route = input.runtime.participantInfos().find(participant => participant.id === request.targetId);
        if (!route) throw new Error("control probe target has no native participant route");
        const commandId = expectedCommandId({ senderHostId: hostId, ownerHostId: route.ownerHostId ?? route.id,
          ownerIdentityId: route.ownerIdentityId ?? route.id, targetId: route.id,
          operation: request.operation, idempotencyKey: request.probeId });
        const task = (async (): Promise<ControlProbeSendResult> => {
          const invocationAtNs = process.hrtime.bigint().toString();
          const epochNow = () => (BigInt(Math.round(performance.timeOrigin * 1_000_000)) + BigInt(Math.round(performance.now() * 1_000_000))).toString();
          const invocationAtEpochNs = epochNow();
          let result: ControlProbeSendResult;
          try {
            const value = await input.invoke(`agents.${request.operation}`, { id: request.targetId, message: request.message,
              idempotencyKey: request.probeId, ...(request.data === undefined ? {} : { data: request.data }) });
            const acceptedAckObservedAtNs = process.hrtime.bigint().toString();
            const acceptedAckObservedAtEpochNs = epochNow();
            const ack = value as { acknowledged?: boolean; accepted?: boolean } | undefined;
            result = { probeId: request.probeId, invocationAtNs, invocationAtEpochNs, acceptedAckObservedAtNs,
              acceptedAckObservedAtEpochNs, accepted: ack?.acknowledged === true && ack?.accepted !== false, result: value };
          } catch (error) {
            result = { probeId: request.probeId, invocationAtNs, invocationAtEpochNs, resolvedAtNs: process.hrtime.bigint().toString(),
              accepted: false, error: error instanceof Error ? error.message : String(error) };
          }
          append({ type: "send", send: { probeId: request.probeId, commandId, result } });
          notifyEvidence();
          assertEvidence();
          return result;
        })();
        pending.add(task);
        try { return await task; } finally { pending.delete(task); }
      },
      snapshot,
      /** Event-driven local stage barrier. Await owner AND sender barriers before combining their snapshots. */
      async waitForEvidence(ids: readonly string[], timeoutMs = 8_000) {
        assertOpen();
        await new Promise<void>((resolve, reject) => {
          const finish = (error?: unknown) => {
            clearTimeout(timer); evidenceListeners.delete(check);
            error ? reject(error) : resolve();
          };
          const check = () => {
            try {
              assertOpen();
              const rows = read();
              const events = rows.flatMap(row => row.type === "event" ? [row.event] : []);
              const nats = input.runtime.config.mesh.transport === "nats";
              const has = (id: string, type: string, kind?: "command" | "ack") => events.some(event =>
                event.commandId === id && event.type === type && (kind === undefined || event.kind === kind));
              if (ids.every(id => input.role === "owner"
                ? has(id, "main-followup-journal-commit") && has(id, nats ? "transport-puback" : "file-append", "ack") && (!nats || has(id, "inbox-ack-confirmed", "command"))
                : rows.some(row => row.type === "send" && row.send.commandId === id && row.send.result?.accepted) &&
                  has(id, nats ? "transport-puback" : "file-append", "command") && (!nats || has(id, "inbox-ack-confirmed", "ack")))) finish();
            } catch (error) { finish(error); }
          };
          const timer = setTimeout(() => finish(new Error("control probe native stage deadline exceeded")), timeoutMs);
          evidenceListeners.add(check);
          check();
        });
        return snapshot();
      },
      reconcile(ids: readonly string[]) {
        const proof = snapshot();
        const count = (id: string) => proof.events.filter(event => event.commandId === id && event.type === "main-followup-journal-commit").length;
        const nativeCount = (id: string) => proof.nativeAdmissions.filter(item => item.commandId === id).length;
        return { role: input.role, roleEvidenceOnly: true as const,
          lost: ids.filter(id => input.role === "owner" ? count(id) === 0 || nativeCount(id) === 0 && !proof.nativeConsumedIds.includes(id)
            : !proof.sends.some(send => send.commandId === id && send.result?.accepted)),
          // Repeated sender results are not duplicate admissions. Only the owner can measure those.
          duplicates: input.role === "owner" ? ids.filter(id => count(id) > 1 || nativeCount(id) > 1) : [],
          nativeAdmissions: proof.nativeAdmissions, nativeConsumedIds: proof.nativeConsumedIds, events: proof.events, sends: proof.sends };
      },
      async close() {
        if (closed) return;
        closed = true;
        release();
        notifyEvidence();
        await Promise.allSettled([...pending]);
        restoreObserver();
        if (AgentMessageRouter.prototype.acceptControl === wrapped) AgentMessageRouter.prototype.acceptControl = original;
        suppliedRuntimeProbeActive = false;
        assertEvidence();
      },
    };
  } catch (error) { suppliedRuntimeProbeActive = false; throw error; }
}

function latencyStats(samples: readonly number[]) {
  const sorted = [...samples].sort((a, b) => a - b);
  const quantile = (p: number): number | null => sorted.length ? sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)]! : null;
  return { count: sorted.length, medianMs: sorted.length ? (sorted.length % 2 ? sorted[(sorted.length - 1) / 2]! :
    (sorted[sorted.length / 2 - 1]! + sorted[sorted.length / 2]!) / 2) : null,
    p95Ms: quantile(0.95), p99Ms: quantile(0.99) };
}

const isAccepted = (result: unknown): boolean => !!result && typeof result === "object" &&
  (result as Record<string, unknown>).accepted === true;
const timeoutError = (text: string | undefined): boolean => text !== undefined && /timeout|timed out|acknowledg.{0,12}timeout/i.test(text);

async function waitForFile(file: string, timeoutMs: number): Promise<void> {
  const until = Date.now() + timeoutMs;
  while (!fs.existsSync(file) && Date.now() < until) await sleep(10);
  if (!fs.existsSync(file)) throw new Error(`probe hold entry was not observed within ${timeoutMs} ms`);
}

async function invokeWithDeadline(client: WorkerClient, body: Record<string, unknown>, timeoutMs: number): Promise<SendResult> {
  try { return await client.request<SendResult>("invoke", body, timeoutMs); }
  catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { probeId: String(body.probeId), invocationAtNs: "", accepted: false, error: message, lost: /timed out/i.test(message) };
  }
}

function workerEnv(agentDir: string, projectRoot: string): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value === undefined || key.startsWith("PI_FABRIC_") || key === "PI_CODING_AGENT_DIR" || key === "SMARTY_AGENT") continue;
    env[key] = value;
  }
  env.PI_CODING_AGENT_DIR = agentDir;
  env.PI_FABRIC_PROJECT_ROOT = projectRoot;
  return env;
}

export async function runControlProbe(options: ControlProbeOptions) {
  if (!Number.isSafeInteger(options.n) || options.n < 1 || options.n > 500) throw new Error("N must be an integer in [1, 500]");
  if (options.transport === "nats" && !options.nats && !(options.natsOwner && options.natsSender)) {
    throw new Error("NATS transport options are required for both owner and sender roles");
  }
  const runDir = privateRunDir();
  const runId = randomUUID();
  const loadShape = options.loadShape ?? "steady-14-ops-per-second";
  if (options.holdAdmission && loadShape !== "synchronized-burst") throw new Error("owner admission hold is supported only with synchronized-burst load");
  const deadlineMs = options.deadlineMs ?? DEFAULT_DEADLINE_MS;
  const holdWaitMs = options.holdWaitMs ?? 5_000;
  const createdRoots: string[] = [];
  const workers: WorkerClient[] = [];
  const allEvents = new Map<string, WorkerEvent[]>();
  let holdProof: Record<string, unknown> | undefined;
  let rssTimer: NodeJS.Timeout | undefined;
  let peakControllerRssBytes: number | undefined;
  const peakAuxiliaryRssBytes = new Map((options.auxiliaryProcesses ?? []).map(process => [process.label, undefined as number | undefined]));
  const sampleRss = () => {
    for (const worker of workers) worker.sampleRss();
    const controller = processRssBytes(process.pid);
    if (controller !== undefined) peakControllerRssBytes = Math.max(peakControllerRssBytes ?? 0, controller);
    for (const auxiliary of options.auxiliaryProcesses ?? []) {
      const current = processRssBytes(auxiliary.pid);
      if (current !== undefined) peakAuxiliaryRssBytes.set(auxiliary.label, Math.max(peakAuxiliaryRssBytes.get(auxiliary.label) ?? 0, current));
    }
  };
  try {
    const projectRoot = path.join(runDir, "project");
    fs.mkdirSync(projectRoot, { recursive: true, mode: 0o700 });
    const suppliedMeshRoot = options.meshRoots?.hub ?? options.meshRoots?.owner ?? options.meshRoots?.work ?? options.meshRoots?.sender;
    const sharedFileRoot = path.join(runDir, "mesh-shared");
    const ownerRoot = path.resolve(options.meshRoots?.hub ?? options.meshRoots?.owner ??
      (options.transport === "file" ? suppliedMeshRoot ?? sharedFileRoot : path.join(runDir, "mesh-hub")));
    const senderRoot = path.resolve(options.meshRoots?.work ?? options.meshRoots?.sender ??
      (options.transport === "file" ? ownerRoot : path.join(runDir, "mesh-work")));
    if (!suppliedMeshRoot && !options.meshRoots?.hub && !options.meshRoots?.owner) createdRoots.push(ownerRoot);
    if (!options.meshRoots?.work && !options.meshRoots?.sender && senderRoot !== ownerRoot && !suppliedMeshRoot) createdRoots.push(senderRoot);
    const providedOwnerRoot = !!(options.meshRoots?.hub ?? options.meshRoots?.owner);
    const providedSenderRoot = !!(options.meshRoots?.work ?? options.meshRoots?.sender);
    for (const [root, provided] of [[ownerRoot, providedOwnerRoot], [senderRoot, providedSenderRoot]] as const) {
      if (provided && !fs.existsSync(root)) throw new Error(`externally seeded mesh root does not exist: ${root}`);
      if (!provided) fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    }
    if (options.transport === "file" && ownerRoot !== senderRoot) {
      throw new Error("FILE transport requires owner and sender to use the same shared --mesh-root");
    }
    const hubBackend = options.stateBackendHub ?? (options.backend === "SQLite-fixed" ? "sqlite" : "file");
    const workBackend = options.stateBackendWork ?? (options.backend === "SQLite-fixed" ? "sqlite" : "file");
    const seededRoots = new Set<string>();
    for (const [root, backend, provided] of [[ownerRoot, hubBackend, providedOwnerRoot], [senderRoot, workBackend, providedSenderRoot]] as const) {
      if (provided || seededRoots.has(root)) continue;
      seededRoots.add(root);
      const archiveDir = path.join(root, "probe-event-archive");
      fs.mkdirSync(archiveDir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(root, "event-archive.json"), `${JSON.stringify({ version: 1, dir: archiveDir })}\n`, { mode: 0o600 });
      const seed = new MeshStore(root, 65_536, 1_000, { stateBackend: backend });
      if (seed.stateBackend !== backend) throw new Error(`refusing ${backend} probe root seeded as ${seed.stateBackend}: ${root}`);
      try {
        await seed.publish({ topic: "fabric.probe.seed", kind: "seed", to: "probe-seed",
          from: { id: `probe-seed-${seededRoots.size}`, name: "probe-seed", kind: "main" }, data: { runId } });
      } finally { seed.closeState(); }
    }
    const natsConfigFor = (role: "owner" | "sender"): ProbeNatsOptions | undefined => {
      if (options.transport !== "nats") return undefined;
      const base = role === "owner" ? options.natsOwner ?? options.nats! : options.natsSender ?? options.nats!;
      return { ...base, operatorMachine: base.operatorMachine ?? "fabric-ops" };
    };
    const workerConfigPath = (role: "owner" | "sender", index: number, agentDir: string, meshRoot: string,
      stateBackend: ProbeStateBackend, eventLogFile: string, hold?: { enabled: true; enteredFile: string; releaseFile: string; operation?: ControlProbeOperation }) => {
      const configPath = path.join(runDir, "worker-config", `${role}-${index}.json`);
      fs.mkdirSync(path.dirname(configPath), { recursive: true, mode: 0o700 });
      fs.mkdirSync(agentDir, { recursive: true, mode: 0o700 });
      const config = { role, projectRoot, agentDir, meshRoot, stateBackend, transport: options.transport,
        nats: natsConfigFor(role), runtimeConfig: options.runtimeConfig, eventLogFile, ...(hold ? { hold } : {}) };
      fs.writeFileSync(configPath, `${JSON.stringify(config)}\n`, { mode: 0o600 });
      return configPath;
    };
    const holdDir = path.join(runDir, "hold");
    const holdEnteredFile = path.join(holdDir, "entered.json");
    const holdReleaseFile = path.join(holdDir, "release");
    const ownerAgentDir = path.join(runDir, "agent-dirs", "owner-0");
    const ownerEventFile = path.join(runDir, "receipt-logs", "owner-0.jsonl");
    const ownerConfig = workerConfigPath("owner", 0, ownerAgentDir, ownerRoot, hubBackend, ownerEventFile,
      options.holdAdmission ? { enabled: true, enteredFile: holdEnteredFile, releaseFile: holdReleaseFile,
        ...(options.operation ? { operation: options.operation } : {}) } : undefined);
    const owner = new WorkerClient(ownerConfig, workerEnv(ownerAgentDir, projectRoot));
    workers.push(owner);
    rssTimer = setInterval(sampleRss, 50);
    rssTimer.unref();
    sampleRss();
    const ownerReady = await owner.ready;
    if (ownerReady.stateBackend !== hubBackend) throw new Error(`silent state-backend fallback refused for owner: requested ${hubBackend}, runtime opened ${ownerReady.stateBackend}`);
    const senderProcesses = options.senderProcesses ?? (loadShape === "synchronized-burst" ? options.n : Math.min(8, options.n));
    if (!Number.isSafeInteger(senderProcesses) || senderProcesses < 1 || senderProcesses > options.n) throw new Error("senderProcesses must be in [1, N]");
    const senders: Array<{ client: WorkerClient; ready: WorkerReady; eventFile: string }> = [];
    for (let index = 0; index < senderProcesses; index++) {
      const agentDir = path.join(runDir, "agent-dirs", `sender-${index}`);
      const eventFile = path.join(runDir, "receipt-logs", `sender-${index}.jsonl`);
      const configPath = workerConfigPath("sender", index, agentDir, senderRoot, workBackend, eventFile);
      const client = new WorkerClient(configPath, workerEnv(agentDir, projectRoot));
      workers.push(client);
      const ready = await client.ready;
      if (ready.stateBackend !== workBackend) throw new Error(`silent state-backend fallback refused for sender ${index}: requested ${workBackend}, runtime opened ${ready.stateBackend}`);
      senders.push({ client, ready, eventFile });
    }
    const ownerRoute = await senders[0]!.client.request<{ ready: boolean; participant: Record<string, unknown> }>("wait-owner", { ownerId: ownerReady.participantId }, 35_000);
    if (!ownerRoute.ready) throw new Error("sender's live NATS participant view did not resolve the owner");
    const route = ownerRoute.participant;
    const ownerHostId = typeof route.ownerHostId === "string" ? route.ownerHostId : ownerReady.participantId;
    const ownerIdentityId = typeof route.ownerIdentityId === "string" ? route.ownerIdentityId : ownerReady.participantId;
    const cursorParticipants = [
      { id: ownerReady.participantId, root: ownerRoot, backend: hubBackend },
      ...senders.map(({ ready }) => ({ id: ready.participantId, root: senderRoot, backend: workBackend })),
    ];
    const rootLabels = new Map<string, string>();
    rootLabels.set(ownerRoot, ownerRoot === senderRoot ? "shared" : "hub");
    rootLabels.set(senderRoot, ownerRoot === senderRoot ? "shared" : "work");
    const roots = [...new Set([ownerRoot, senderRoot])];
    const beforeEvents = Object.fromEntries(roots.map(root => [rootLabels.get(root)!, eventEvidence(root)]));
    const beforeCursors = readCursorSnapshots(cursorParticipants);
    const sentAt = new Date().toISOString();
    const sends = Array.from({ length: options.n }, (_, index) => {
      const sender = senders[index % senders.length]!;
      const probeId = `pathb-${runId}-${index + 1}`;
      const commandId = expectedCommandId({ senderHostId: sender.ready.participantId, ownerHostId, ownerIdentityId,
        targetId: ownerReady.participantId, operation: options.operation ?? "followUp", idempotencyKey: probeId });
      return { index, probeId, commandId, sender, promise: undefined as Promise<SendResult> | undefined };
    });
    const startSend = (item: typeof sends[number]): Promise<SendResult> => {
      const request = invokeWithDeadline(item.sender.client, { probeId: item.probeId, targetId: ownerReady.participantId,
        operation: options.operation ?? "followUp", message: `Path B admission probe ${item.probeId}`, data: { probeId: item.probeId }, deadlineMs }, deadlineMs + 12_000);
      item.promise = request;
      return request;
    };
    let completed: SendResult[] = [];
    if (options.holdAdmission) {
      const first = sends[0]!;
      const pending = sends.map(startSend);
      let firstSettled = false;
      void pending[0]!.finally(() => { firstSettled = true; }).catch(() => undefined);
      await waitForFile(holdEnteredFile, holdWaitMs);
      const heldJournalBytes = sha256File(ownerReady.mainJournal);
      await sleep(100);
      const senderPendingWhileHeld = !firstSettled;
      const journalUnchangedWhileHeld = sha256File(ownerReady.mainJournal) === heldJournalBytes;
      fs.mkdirSync(holdDir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(holdReleaseFile, `${new Date().toISOString()}\n`, { mode: 0o600 });
      completed = await Promise.all(pending);
      holdProof = { releasedBy: "file", senderPendingWhileHeld, journalUnchangedWhileHeld,
        commandId: first.commandId, heldJournalSha256: heldJournalBytes };
    } else if (loadShape === "synchronized-burst") {
      completed = await Promise.all(sends.map(startSend));
    } else {
      const base = performance.now();
      const rate = options.ratePerSecond ?? 14;
      const pending: Promise<SendResult>[] = [];
      for (const item of sends) {
        const delay = base + item.index * 1_000 / rate - performance.now();
        if (delay > 0) await sleep(delay);
        pending.push(startSend(item));
      }
      completed = await Promise.all(pending);
    }
    const commandIds = new Set(sends.map(item => item.commandId));
    const eventFiles = [ownerEventFile, ...senders.map(sender => sender.eventFile)];
    const readAllEvents = () => eventFiles.flatMap(readJsonLines);
    const flushAll = async () => { await Promise.all(workers.map(client => client.request("flush", {}, 5_000).catch(() => undefined))); };
    const readyForEvidence = (events: WorkerEvent[]): boolean => {
      const ownerEvents = events.filter(event => event.role === "owner");
      if (options.transport === "nats") return completed.filter(result => result.accepted).every(result => {
        const send = sends.find(item => item.probeId === result.probeId);
        return !!send && ownerEvents.some(event => event.type === "inbox-ack-confirmed" && event.commandId === send.commandId && event.kind === "command") &&
          events.some(event => event.role === "sender" && event.type === "inbox-ack-confirmed" && event.commandId === send.commandId && event.kind === "ack");
      });
      return completed.filter(result => result.accepted).every(result => {
        const send = sends.find(item => item.probeId === result.probeId);
        return !!send && ownerEvents.some(event => event.type === "main-followup-journal-commit" && event.commandId === send.commandId);
      });
    };
    const stageDeadline = Date.now() + 8_000;
    let events: WorkerEvent[] = [];
    do {
      await flushAll();
      events = readAllEvents();
      if (readyForEvidence(events)) break;
      await sleep(50);
    } while (Date.now() < stageDeadline);
    events = readAllEvents();
    for (const event of events) {
      if (!event.commandId || !commandIds.has(event.commandId)) continue;
      const list = allEvents.get(event.commandId) ?? [];
      list.push(event);
      allEvents.set(event.commandId, list);
    }
    const afterEvents = Object.fromEntries(roots.map(root => [rootLabels.get(root)!, eventEvidence(root)]));
    const afterCursors = readCursorSnapshots(cursorParticipants);
    const fileControlEvents = options.transport === "file" ? fileControlEventEvidence(ownerRoot, commandIds) : null;
    sampleRss();
    await flushAll();
    const allRecords = [...allEvents.entries()];
    const acceptedCount = completed.filter(isAccepted).length;
    const sendRecords = collectControlProbeEvidence(sends.map(item => ({ probeId: item.probeId, commandId: item.commandId,
      result: completed.find(candidate => candidate.probeId === item.probeId) })), events, options.transport);
    const acceptedSamples = sendRecords.flatMap(record => typeof record.primaryLatencyMs === "number" ? [record.primaryLatencyMs] : []);
    const refused = completed.filter(result => !result.accepted && !result.lost).length;
    const ackTimeouts = completed.filter(result => result.lost || timeoutError(result.error)).length;
    const redeliveries = sendRecords.reduce((sum, record) => sum + Number(record.redeliveryAttempts), 0);
    const duplicateAdmissions = sendRecords.reduce((sum, record) => sum + Math.max(0, Number(record.logicalAdmissionCount) - 1), 0);
    const lostMessages = sends.length - completed.filter(result => !result.lost).length;
    const eventUnchanged = JSON.stringify(beforeEvents) === JSON.stringify(afterEvents);
    const cursorUnchanged = JSON.stringify(beforeCursors) === JSON.stringify(afterCursors);
    return {
      schemaVersion: 1,
      metricContract: {
        primary: "send -> admission-ACK (sender clock)",
        primaryDefinition: "Public agents.followUp/steer API invocation to the sender observing acknowledged:true; same-process monotonic clock; conservative upper bound on send-to-committed-admission.",
        secondary: "owner Main follow-up journal commit minus sender invocation in the same-host epoch clock domain; reported only for same-host process placements.",
        stageTimestampClock: "process performance.timeOrigin + performance.now(), converted to epoch nanoseconds; same-host only.",
        sourceEvidence: {
          fileOwnerAckAfterHandler: "src/topology/control-plane.ts:3035-3123, 2989-3022, 3265-3304; src/providers/agents-message-router.ts:1015-1132; src/main-agent.ts:694-858",
          fileSenderWaitsForAcceptedAck: "src/topology/control-plane.ts:1966-2035; src/transport/file.ts:270-292",
          natsOwnerAckAfterHandlerAndOutcomeSave: "src/topology/control-plane.ts:2608-2681, 2544-2560, 2347-2440",
          natsSenderWaitsForAcceptedAck: "src/topology/control-plane.ts:2089-2183, 2035-2039; src/transport/nats/inbox.ts:585-600",
          durableMainJournal: "src/main-agent.ts:1015-1017; writeFileAtomic durable barriers in src/core/atomic-write.ts:433-474",
        },
      },
      run: {
        startedAt: sentAt,
        n: options.n,
        backend: options.backend,
        stateBackend: { hub: hubBackend, work: workBackend, actualHub: ownerReady.stateBackend, actualWork: senders[0]!.ready.stateBackend },
        placement: "same-host separate Main processes",
        loadShape,
        ...(loadShape === "steady-14-ops-per-second" ? { aggregateRatePerSecond: options.ratePerSecond ?? 14 } : {}),
        senderProcesses: senders.length,
        ownerSessionId: ownerReady.sessionId,
        ownerParticipantId: ownerReady.participantId,
        meshRoots: { hub: ownerRoot === senderRoot ? "shared" : "hub", work: ownerRoot === senderRoot ? "shared" : "work" },
        server: options.transport === "nats" ? options.serverLabel ?? "external NATS server" : "not applicable",
        natsSyncInterval: options.transport === "nats" ? options.syncInterval ?? "unknown; not queried" : null,
        presenceProjection: options.presenceProjectionLabel ?? "pre-projection",
        barClaimable: options.presenceProjectionLabel === "enabled",
      },
      primary: { label: "send -> admission-ACK (sender clock)", ...latencyStats(acceptedSamples) },
      secondarySameHostJournalCommitMs: latencyStats(sendRecords.flatMap(record => typeof record.secondarySameHostJournalCommitMs === "number"
        ? [record.secondarySameHostJournalCommitMs] : [])),
      counts: { refusals: refused, redeliveries, duplicateAdmissions, lostMessages, ackTimeouts },
      perSend: sendRecords,
      evidence: {
        eventsJsonlAndArchiveBefore: beforeEvents,
        eventsJsonlAndArchiveAfter: afterEvents,
        eventsUnchanged: eventUnchanged,
        rootInboxCursorsBefore: beforeCursors,
        rootInboxCursorsAfter: afterCursors,
        cursorsUnchanged: cursorUnchanged,
        ...(holdProof ? { ownerAdmissionHold: holdProof } : {}),
        ...(fileControlEvents ? { fileControlEvents } : {}),
      },
      stageEventCount: allRecords.reduce((sum, [, records]) => sum + records.length, 0),
      capacityAdmission: options.capacityAdmission ?? null,
      capacityObservation: (() => {
        const senderPeaks = senders.map(sender => sender.client.peakRssBytes).filter((value): value is number => value !== undefined);
        const writerPeakRssBytes = senderPeaks.length ? Math.max(...senderPeaks) : null;
        const ownerPeakRssBytes = owner.peakRssBytes ?? null;
        const auxiliary = Object.fromEntries(peakAuxiliaryRssBytes);
        const auxiliaryBytes = [...peakAuxiliaryRssBytes.values()].reduce<number>((sum, value) => sum + (value ?? 0), 0);
        const readerAndControllerBytes = (ownerPeakRssBytes ?? 0) + (peakControllerRssBytes ?? 0) + auxiliaryBytes;
        const estimateBytes = writerPeakRssBytes === null ? null : Math.ceil((writerPeakRssBytes * options.n + readerAndControllerBytes) * 1.2);
        return { sampler: "Linux /proc/<pid>/status VmRSS, sampled every 50ms", senderWriterCount: senders.length,
          senderPeakRssBytes: senderPeaks, peakWriterRssBytes: writerPeakRssBytes, ownerMainReaderPeakRssBytes: ownerPeakRssBytes,
          controllerPeakRssBytes: peakControllerRssBytes ?? null, auxiliaryReaderPeakRssBytes: auxiliary,
          formula: "(peakWriterRssBytes * N + ownerMainReader + controller + auxiliary readers) * 1.20",
          headroomLimitBytes: options.headroomLimitBytes ?? null, estimateWith20PercentBytes: estimateBytes,
          withinHeadroom: estimateBytes !== null && options.headroomLimitBytes !== undefined ? estimateBytes <= options.headroomLimitBytes : null };
      })(),
      checks: {
        acceptedCount,
        allAcceptedHaveMainJournalCommit: sendRecords.filter(record => record.accepted).every(record => record.logicalAdmissionCount === 1 && record.ownerJournalCommitAtNs !== null),
        natsLeavesMeshEventsAndCursorsUnchanged: options.transport === "nats" ? eventUnchanged && cursorUnchanged : null,
      },
    };
  } finally {
    if (rssTimer) clearInterval(rssTimer);
    sampleRss();
    await Promise.allSettled(workers.reverse().map(worker => worker.stop()));
    for (const root of [...new Set(createdRoots)].reverse()) {
      if (root.startsWith(runDir + path.sep)) fs.rmSync(root, { recursive: true, force: true });
    }
    fs.rmSync(runDir, { recursive: true, force: true });
  }
}

export async function runFileCounterprobe(options: Omit<ControlProbeOptions, "transport" | "backend" | "nats" | "meshRoots" | "stateBackendHub" | "stateBackendWork"> & {
  readonly stateBackend?: ProbeStateBackend;
}) {
  const backend = options.stateBackend === "sqlite" ? "SQLite-fixed" : "FILE-fixed";
  return runControlProbe({ ...options, backend, transport: "file", stateBackendHub: options.stateBackend ?? "file",
    stateBackendWork: options.stateBackend ?? "file" });
}

export function participantViewAddress(participantId: string, machine: string) {
  const token = createHash("sha256").update(participantId).digest("hex").slice(0, 32);
  return { bucket: "fabric_presence", key: `${machine}.p.${token}`, hash: "sha256(participantId).hex.slice(0,32)" };
}
