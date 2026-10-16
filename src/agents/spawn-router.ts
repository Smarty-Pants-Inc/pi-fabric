import { execFile, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { constants, realpathSync } from "node:fs";
import { lstat, mkdir, open, rename } from "node:fs/promises";
import { hostname } from "node:os";
import path from "node:path";
import { readPiSessionHeader } from "../core/pi-session-header.js";
import { isFabricThinking, type FabricThinking } from "../thinking.js";
import type { FabricAgentRouterConfig } from "./router-config.js";
import type { MeshStore, MeshIdentity } from "../mesh/store.js";
import type { FabricParticipantInfo } from "../topology/types.js";
import type { ResidentRouterCaller } from "../residency/protocol.js";
import type { AgentsProvider } from "../providers/agents-provider.js";
import type { FabricInvocationContext } from "../protocol.js";
import type { AgentRunRequest } from "./types.js";
import type { FabricAgentRunner } from "../config.js";
import { resolveAvailablePiModel } from "../core/model-resolution.js";

/** Native-only authority. Never copy this from a creation request or action arguments. */
export interface SpawnRouterAdmission {
  mesh: MeshStore;
  requesterSessionId: string;
  role: string;
  targetRootId: string;
  targetHostId: string;
}

interface AdmissionRequest {
  model?: string | undefined;
  thinking?: FabricThinking | undefined;
  runner?: string | undefined;
  name?: string | undefined;
  cwd?: string | undefined;
  task?: string | undefined;
  instructions?: string | undefined;
  sha256?: string | undefined;
}

/** Build attribution authority only from a directory-validated resident caller. */
export const authenticatedSpawnRouterAdmission = (mesh: MeshStore, caller: ResidentRouterCaller | undefined,
  participant: FabricParticipantInfo | undefined, rootId: string, hostId: string, kind: "spawn" | "actor"): SpawnRouterAdmission | undefined => {
  if (!caller || !participant || participant.stale || participant.remoteHost !== undefined ||
    participant.id !== caller.id || participant.rootId !== rootId || caller.rootId !== rootId ||
    !caller.sessionId || participant.sessionId !== caller.sessionId || participant.kind !== caller.kind ||
    participant.ownerHostId !== caller.ownerHostId || participant.ownerIdentityId !== caller.ownerIdentityId) return undefined;
  return { mesh, requesterSessionId: participant.sessionId!, role: participant.role ?? (kind === "actor" ? "actor" : "task-agent"),
    targetRootId: rootId, targetHostId: hostId };
};

export interface SpawnRouterBinding { model?: string; thinking: FabricThinking }
export interface SpawnRouterResult extends SpawnRouterBinding {
  decisionId: string;
  /** Only an enforce pick overrides the request; defaults remain dynamic. */
  selected?: SpawnRouterBinding;
}
export interface SpawnRouterPick extends SpawnRouterBinding { model: string; reason?: string; policyVersion?: string }

export interface SpawnRouterStartOptions {
  meshRoot: string;
  decisionId: string;
  agentId: string;
  sessionId: string | null;
  /** Host-only seed/header lookup; never persisted in the ledger. */
  sessionFile?: string;
  model: string | null;
  thinking: FabricThinking | null;
  launch?: { runId: string | null; pid: number | null; processStartTime: string | null };
  verified?: boolean;
  evidenceRef?: string | null;
  /** Separate native caller snapshot, not fields on the task/actor request. */
  admission?: SpawnRouterAdmission | undefined;
  spawnKind?: "spawn" | "actor" | undefined;
  request?: AdmissionRequest | undefined;
  /** Native manager's base cwd; task cwd is resolved relative to it. */
  cwd?: string | undefined;
}

/** Fixed, privacy-safe code used when admission created no started record. */
export const SPAWN_ROUTER_START_FAILED = "start-failed";
/** Fixed, privacy-safe code used when native launch custody is unresolved. */
export const SPAWN_ROUTER_START_UNKNOWN = "start-unknown";
export interface SpawnRouterRequest {
  kind: "spawn" | "actor";
  role: string;
  name: string | null;
  cwd: string;
  project: string;
  taskDigest: string;
  /** UTF-8 bytes; null for an owner-only file instruction source. */
  taskLength: number | null;
  task?: string;
  parentId: string;
  requestedComplexity?: "simple" | "normal" | "complex" | "delicate";
  host: string;
  defaults: { model: string | null; thinking: FabricThinking };
}

const digest = (text: string): string => createHash("sha256").update(text).digest("hex");
const MAX_OUTPUT_BYTES = 64 * 1024;
// decisions/ is host-internal: the public mesh provider forbids put/delete here.
const ADMISSION_PREFIX = "decisions/spawn-router/admissions/";
const ADMISSION_TTL_MS = 10 * 60_000;
const MAX_ADMISSIONS = 4096;
const DECISION_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;
const admissionIdentity: MeshIdentity = { id: "spawn-router", name: "Spawn router", kind: "agent" };
const admissionValid = (admission: SpawnRouterAdmission | undefined): admission is SpawnRouterAdmission =>
  !!admission && !!admission.mesh && typeof admission.mesh.root === "string" && typeof admission.mesh.writeBatch === "function" &&
  [admission.requesterSessionId, admission.role, admission.targetRootId, admission.targetHostId]
    .every(value => typeof value === "string" && value.length > 0);
const canonicalCwd = (cwd: string): string => {
  try { return realpathSync(cwd); } catch { return path.resolve(cwd); }
};
const admissionDigest = (admission: SpawnRouterAdmission, kind: "spawn" | "actor", request: AdmissionRequest, cwd: string): string =>
  digest(JSON.stringify({ requesterSessionId: admission.requesterSessionId, targetRootId: admission.targetRootId,
    targetHostId: admission.targetHostId, role: admission.role, kind,
    cwd: canonicalCwd(kind === "spawn" ? path.resolve(cwd, request.cwd ?? ".") : cwd),
    model: request.model?.trim() || null, thinking: request.thinking ?? null, runner: request.runner ?? null,
    name: request.name?.trim() ?? null,
    taskDigest: kind === "spawn" ? digest(request.task ?? "")
      : request.instructions !== undefined ? digest(request.instructions) : request.sha256 ?? null }));

const issueAdmission = async (decisionId: string, admission: SpawnRouterAdmission | undefined,
  kind: "spawn" | "actor", request: AdmissionRequest | undefined, cwd: string): Promise<void> => {
  if (!admissionValid(admission) || !request) return;
  const binding = admissionDigest(admission, kind, request, cwd);
  await admission.mesh.writeBatch({ identity: admissionIdentity, ops: [], prepare: view => {
    const now = Date.now();
    const entries = view.listAll(ADMISSION_PREFIX);
    const expired = entries.filter(entry => {
      const value = entry.value as { expiresAt?: unknown } | null;
      return typeof value?.expiresAt === "number" && value.expiresAt <= now;
    });
    if (entries.length - expired.length >= MAX_ADMISSIONS) throw new Error("admission-unavailable");
    return [...expired.map(entry => ({ kind: "delete" as const, key: entry.key, ifVersion: entry.version })),
      { kind: "put" as const, key: `${ADMISSION_PREFIX}${decisionId}`, ifVersion: 0,
        value: { format: 1, decisionId, binding, state: "issued", issuedAt: now, expiresAt: now + ADMISSION_TTL_MS } }];
  } });
};

/** The existing mesh transaction/CAS is the sole consume fence, across processes/backends. */
type AttributionRefusal = "decision-unknown" | "decision-replayed" | "decision-mismatch";
const consumeAdmission = async (options: Pick<SpawnRouterStartOptions, "meshRoot" | "decisionId" | "admission" | "spawnKind" | "request" | "cwd">): Promise<AttributionRefusal | undefined> => {
  const { admission, decisionId, spawnKind, request, cwd } = options;
  if (!DECISION_ID.test(decisionId) || admission === undefined) return "decision-unknown";
  if (!admissionValid(admission) || !spawnKind || !request || cwd === undefined ||
    canonicalCwd(admission.mesh.root) !== canonicalCwd(options.meshRoot)) return "decision-mismatch";
  let binding: string;
  try { binding = admissionDigest(admission, spawnKind, request, cwd); }
  catch { return "decision-mismatch"; }
  try {
    let expiresAt = 0;
    let refusal: AttributionRefusal = "decision-unknown";
    const results = await admission.mesh.writeBatch({ identity: admissionIdentity, ops: [], prepare: view => {
      expiresAt = 0;
      refusal = "decision-unknown";
      const key = `${ADMISSION_PREFIX}${decisionId}`;
      const entry = view.get(key);
      const value = entry?.value as Record<string, unknown> | undefined;
      const now = Date.now();
      if (!entry || entry.updatedBy.id !== admissionIdentity.id || !value || value.format !== 1 || value.decisionId !== decisionId ||
        typeof value.issuedAt !== "number" || typeof value.expiresAt !== "number" ||
        !Number.isSafeInteger(value.issuedAt) || !Number.isSafeInteger(value.expiresAt) || value.issuedAt < 0 ||
        value.issuedAt > now || value.expiresAt <= now || value.expiresAt <= value.issuedAt ||
        value.expiresAt - value.issuedAt > ADMISSION_TTL_MS) return [];
      if (value.state === "consumed") { refusal = "decision-replayed"; return []; }
      if (value.state !== "issued" || value.binding !== binding) { refusal = "decision-mismatch"; return []; }
      expiresAt = value.expiresAt;
      return [{ kind: "put", key, ifVersion: entry.version, value: { ...value, state: "consumed" } }];
    }, beforeCommit: () => {
      if (expiresAt && Date.now() >= expiresAt) throw new Error("admission-expired");
    } });
    return results.length === 1 && results[0]?.applied === true ? undefined : refusal;
  } catch { return "decision-unknown"; } // Unavailable authority can never attribute success.
};

const routerEnv = (): NodeJS.ProcessEnv => {
  const env: NodeJS.ProcessEnv = { PATH: "/usr/bin:/bin" };
  if (process.platform === "win32") {
    // A copied Windows environment is case-sensitive in JS even though native
    // environment names are not. Never inherit Path or a caller's COMSPEC.
    const hostEnv = new Map(Object.entries(process.env).map(([name, value]) => [name.toUpperCase(), value]));
    const root = hostEnv.get("SYSTEMROOT");
    if (!root || !path.win32.isAbsolute(root)) throw new Error("command-error");
    const system = path.win32.join(root, "System32");
    env.SystemRoot = root;
    env.PATH = `${system};${root}`;
    env.COMSPEC = path.win32.join(system, "cmd.exe");
    // libuv adds these Windows process essentials even with an explicit env.
    // Make that exact allowlist explicit; never inherit the rest of the host.
    for (const name of ["HOMEDRIVE", "HOMEPATH", "LOGONSERVER", "SYSTEMDRIVE", "TEMP", "USERDOMAIN", "USERNAME", "USERPROFILE", "WINDIR"]) {
      const value = hostEnv.get(name);
      if (value !== undefined) env[name] = value;
    }
  }
  for (const name of ["HOME", "LANG", "TZ"]) {
    if (process.env[name] !== undefined) env[name] = process.env[name];
  }
  return env;
};
// The final assertions also reject trailing line terminators, which $ alone accepts.
const reasonCode = (value: string): string =>
  /^(?:policy:task\/[a-z0-9-]{1,32}|capacity-hold|taskclass:[a-z0-9-]{1,32}|default|unavailable|normal\.implementation)$(?![\s\S])/.test(value) ? value : "other";
const policyVersionCode = (value: string): string =>
  /^(?:v[0-9]+(?:-[a-z0-9.]{1,32})?|[0-9]+\.[0-9]+(?:\.[0-9]+)?|[0-9a-f]{7,40})$(?![\s\S])/.test(value) ? value : "unknown";
const MAX_LEDGER_BYTES = 8 * 1024 * 1024;
const ledgerWrites = new Map<string, Promise<void>>();

// Serialize this process's rotation/appends; each record is a single O_APPEND write.
const writeDecision = async (dir: string, record: string): Promise<void> => {
  const pending = (ledgerWrites.get(dir) ?? Promise.resolve()).catch(() => {}).then(async () => {
    await mkdir(dir, { recursive: true, mode: 0o700 });
    const directory = await lstat(dir);
    const uid = process.getuid?.();
    if (!directory.isDirectory() || (uid !== undefined &&
      (directory.uid !== uid || (directory.mode & 0o077) !== 0))) throw new Error("unsafe-directory");
    const file = path.join(dir, "decisions.jsonl");
    const openLedger = async () => {
      // lstat also rejects links on platforms without O_NOFOLLOW.
      const existing = await lstat(file).catch((error: NodeJS.ErrnoException) => {
        if (error.code !== "ENOENT") throw error;
        return undefined;
      });
      if (existing && !existing.isFile()) throw new Error("unsafe-ledger");
      const handle = await open(file, constants.O_WRONLY | constants.O_APPEND | constants.O_CREAT |
        (constants.O_NOFOLLOW ?? 0) | constants.O_NONBLOCK, 0o600);
      try {
        const info = await handle.stat();
        const entry = await lstat(file);
        if (!info.isFile() || !entry.isFile() || info.dev !== entry.dev || info.ino !== entry.ino) {
          throw new Error("unsafe-ledger");
        }
        return { handle, size: info.size };
      } catch (error) { await handle.close(); throw error; }
    };
    let ledger = await openLedger();
    if (ledger.size + Buffer.byteLength(record) > MAX_LEDGER_BYTES) {
      await ledger.handle.close();
      await rename(file, `${file}.1`);
      ledger = await openLedger();
    }
    try { await ledger.handle.appendFile(record); }
    finally { await ledger.handle.close(); }
  });
  ledgerWrites.set(dir, pending);
  try { await pending; }
  finally { if (ledgerWrites.get(dir) === pending) ledgerWrites.delete(dir); }
};

const appendStartRecord = async (meshRoot: string, record: Record<string, unknown>): Promise<void> => {
  try {
    await writeDecision(path.join(meshRoot, "router"), `${JSON.stringify(record)}\n`);
  } catch {
    // A read-only/full mesh must not change or fail a spawn.
    console.warn("[pi-fabric] spawn router decision log unavailable");
  }
};

/** Append the applied binding only after this exact native admission wins consumption. */
export const recordSpawnRouterStart = async (options: SpawnRouterStartOptions): Promise<void> => {
  if (!options.decisionId) return;
  const refusal = await consumeAdmission(options);
  if (refusal) { await recordAttributionRefused(options, refusal); return; }
  let sessionId = options.sessionId;
  if (options.sessionFile) {
    try { sessionId = readPiSessionHeader(options.sessionFile)?.id ?? sessionId; } catch { /* No native session yet. */ }
  }
  await appendStartRecord(options.meshRoot, {
    ts: new Date().toISOString(), kind: "start", decisionId: options.decisionId,
    agentId: options.agentId, sessionId,
    launch: options.launch ?? { runId: null, pid: null, processStartTime: null },
    host: hostname(), model: options.model, thinking: options.thinking,
    verified: options.verified === true, evidenceRef: options.evidenceRef ?? null,
  });
};

const recordAttributionRefused = async (options: Pick<SpawnRouterStartOptions, "meshRoot" | "decisionId">, error: AttributionRefusal): Promise<void> => {
  await appendStartRecord(options.meshRoot, { ts: new Date().toISOString(), kind: "start-refused",
    decisionId: DECISION_ID.test(options.decisionId) ? options.decisionId : null,
    agentId: null, sessionId: null, launch: { runId: null, pid: null, processStartTime: null },
    host: hostname(), model: null, thinking: null, verified: false, evidenceRef: null, error });
};

/** Provider retains this custody until publication; failures consume the same admission. */
export const recordSpawnRouterStartFailed = async (options: Pick<SpawnRouterStartOptions,
  "meshRoot" | "decisionId" | "admission" | "spawnKind" | "request" | "cwd">): Promise<void> => {
  if (!options.decisionId) return;
  const refusal = await consumeAdmission(options);
  if (refusal) { await recordAttributionRefused(options, refusal); return; }
  await appendStartRecord(options.meshRoot, {
    ts: new Date().toISOString(), kind: "start-failed", decisionId: options.decisionId,
    agentId: null, sessionId: null, launch: { runId: null, pid: null, processStartTime: null },
    host: hostname(), model: null, thinking: null, verified: false, evidenceRef: null,
    error: SPAWN_ROUTER_START_FAILED,
  });
};

/** A native launch may have started a worker without returning a confirmable handle. */
export const recordSpawnRouterStartUnknown = async (options: Pick<SpawnRouterStartOptions,
  "meshRoot" | "decisionId" | "admission" | "spawnKind" | "request" | "cwd">): Promise<void> => {
  if (!options.decisionId) return;
  const refusal = await consumeAdmission(options);
  if (refusal) { await recordAttributionRefused(options, refusal); return; }
  await appendStartRecord(options.meshRoot, {
    ts: new Date().toISOString(), kind: "start-unknown", decisionId: options.decisionId,
    agentId: null, sessionId: null, launch: { runId: null, pid: null, processStartTime: null },
    host: hostname(), model: null, thinking: null, verified: false, evidenceRef: null,
    error: SPAWN_ROUTER_START_UNKNOWN,
  });
};

// Join retirement, not close: descendants may retain inherited pipes forever.
const runRouter = (command: string[], input: SpawnRouterRequest, timeoutMs: number, signal?: AbortSignal): Promise<string> =>
  new Promise((resolve, reject) => {
    if (!command.length) { reject(new Error("missing-command")); return; }
    if (!path.isAbsolute(command[0]!)) { reject(new Error("invalid-command")); return; }
    if (signal?.aborted) { reject(new Error("aborted")); return; }
    let child: ReturnType<typeof spawn>;
    let environment: NodeJS.ProcessEnv;
    try {
      environment = routerEnv();
      child = spawn(command[0]!, command.slice(1), {
        cwd: input.cwd, shell: false, env: environment, detached: process.platform !== "win32", stdio: ["pipe", "pipe", "ignore"],
      });
    } catch { reject(new Error("command-error")); return; }
    let settled = false;
    let retirement: Promise<void> | undefined;
    let exited = false;
    const nativeExit = new Promise<void>(done => {
      child.once("exit", () => { exited = true; done(); });
      child.once("error", () => { if (child.pid === undefined) { exited = true; done(); } });
    });
    let output = "";
    let bytes = 0;
    const directKill = (): void => {
      try { child.kill("SIGKILL"); } catch { /* Already exited (or spawn failed). */ }
    };
    const kill = (): Promise<void> => retirement ??= Promise.resolve().then(async () => {
      if (process.platform === "win32" && child.pid) {
        // Await the closed helper before returning; an asynchronous taskkill
        // otherwise races the caller's temp cleanup and leaves a live router.
        const taskkill = path.win32.join(environment.SystemRoot!, "System32", "taskkill.exe");
        await new Promise<void>(done => {
          try {
            execFile(taskkill, ["/T", "/F", "/PID", String(child.pid)], {
              env: environment, windowsHide: true, timeout: 1000, killSignal: "SIGKILL",
            }, error => { if (error) directKill(); done(); });
          } catch { directKill(); done(); }
        });
      } else {
        try { if (child.pid) process.kill(-child.pid, "SIGKILL"); else directKill(); }
        catch { directKill(); }
      }
      if (!exited) {
        // Do not wait for descendant-held close, or hang on a failed native kill.
        await new Promise<void>(done => {
          const timer = setTimeout(done, 1000);
          void nativeExit.then(() => { clearTimeout(timer); done(); });
        });
      }
    });
    const finish = (error?: string): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      child.stdin!.destroy();
      child.stdout!.destroy();
      void kill().then(() => {
        if (error) reject(new Error(error));
        else resolve(output);
      });
    };
    const stop = (reason: string): void => finish(reason);
    const abort = (): void => stop("aborted");
    const timer = setTimeout(() => stop("timeout"), timeoutMs);
    signal?.addEventListener("abort", abort, { once: true });
    child.on("error", () => stop("command-error"));
    child.stdin!.on("error", () => stop("stdin-error"));
    child.stdout!.on("error", () => stop("stdout-error"));
    child.stdout!.setEncoding("utf8");
    child.stdout!.on("data", (chunk: string) => {
      if (settled) return;
      bytes += Buffer.byteLength(chunk);
      if (bytes > MAX_OUTPUT_BYTES) stop("output-too-large");
      else output += chunk;
    });
    // Retire descendants even if a router exits without closing inherited pipes.
    child.once("exit", () => { void kill(); });
    child.once("close", (code) => finish(code === 0 ? undefined : "nonzero-exit"));
    child.stdin!.end(`${JSON.stringify(input)}\n`);
    if (signal?.aborted) abort();
  });

/** First-use adapter only. Off returns no receipt; only selected overrides defaults. */
export const routeAgentCreation = async (options: {
  config: FabricAgentRouterConfig;
  meshRoot: string;
  kind: SpawnRouterRequest["kind"];
  role: string;
  name?: string;
  cwd: string;
  project: string;
  task?: string;
  taskDigest?: string;
  parentId: string;
  complexity?: SpawnRouterRequest["requestedComplexity"];
  defaults: SpawnRouterBinding;
  explicit: boolean;
  validateModel: (model: string) => string;
  /** Host-only native target/caller plus the actual prepared request (including seed text). */
  admission?: SpawnRouterAdmission;
  admissionRequest?: AdmissionRequest;
  /** Base cwd before resolving a relative request.cwd (not the selected router cwd). */
  admissionCwd?: string;
  signal?: AbortSignal;
}): Promise<SpawnRouterResult | undefined> => {
  const config = options.config;
  const mode = config.mode ?? "off";
  if (mode !== "shadow" && mode !== "enforce") return undefined;
  const admission = options.admission && { ...options.admission };
  const admissionRequest = options.admissionRequest && { ...options.admissionRequest };
  const admissionCwd = options.admissionCwd ?? options.cwd;
  // Keep the config normalizer out of this lazy entry: sharing a tiny runtime
  // helper creates another chunk in every host's eager static closure.
  const timeoutMs = typeof config.timeoutMs === "number" && Number.isFinite(config.timeoutMs)
    ? Math.max(200, Math.min(5000, Math.round(config.timeoutMs))) : 1500;
  const request: SpawnRouterRequest = {
    kind: options.kind, role: options.role, name: options.name ?? null,
    cwd: options.cwd, project: options.project,
    taskDigest: options.taskDigest ?? digest(options.task ?? ""),
    taskLength: options.task === undefined ? null : Buffer.byteLength(options.task),
    ...(config.includeTask && options.task !== undefined ? { task: options.task } : {}),
    parentId: options.parentId,
    ...(options.complexity ? { requestedComplexity: options.complexity } : {}),
    host: hostname(), defaults: { model: options.defaults.model ?? null, thinking: options.defaults.thinking },
  };
  // Identify policy commands without storing executable paths or arguments.
  const commandDigest = digest(JSON.stringify(config.command));
  const started = performance.now();
  let pick: SpawnRouterPick | null = null;
  let actual = request.defaults;
  let error: string | null = null;
  let selected: SpawnRouterBinding | undefined;
  if (!options.explicit) {
    try {
      const output = await runRouter(config.command, request, timeoutMs, options.signal);
      let parsed: unknown;
      try { parsed = JSON.parse(output); } catch { throw new Error("invalid-json"); }
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("invalid-output");
      const value = parsed as Record<string, unknown>;
      if (typeof value.model !== "string" || !value.model.trim() || value.model.length > 256 || !isFabricThinking(value.thinking) ||
        (value.reason !== undefined && (typeof value.reason !== "string" || value.reason.length > 2000)) ||
        (value.policyVersion !== undefined && (typeof value.policyVersion !== "string" || value.policyVersion.length > 256))) {
        throw new Error("invalid-output");
      }
      let model: string;
      try { model = options.validateModel(value.model.trim()); } catch { throw new Error("unknown-or-denied-model"); }
      pick = { model, thinking: value.thinking,
        ...(typeof value.reason === "string" ? { reason: reasonCode(value.reason) } : {}),
        ...(typeof value.policyVersion === "string" ? { policyVersion: policyVersionCode(value.policyVersion) } : {}) };
      if (mode === "enforce") {
        selected = { model: pick.model, thinking: pick.thinking };
        actual = { model: pick.model, thinking: pick.thinking };
      }
    } catch (cause) {
      // Only fixed adapter error codes are emitted; never router stderr or task.
      error = cause instanceof Error ? cause.message : "router-error";
    }
  }
  const decisionId = randomUUID();
  try {
    await issueAdmission(decisionId, admission, options.kind,
      admissionRequest && { ...admissionRequest, ...selected }, admissionCwd);
  } catch { console.warn("[pi-fabric] spawn router admission unavailable"); }
  const decision = {
    ts: new Date().toISOString(), decisionId, kind: "decision", spawnKind: request.kind, mode, commandDigest,
    decision: options.explicit ? "explicit" : selected ? "enforce" : "default",
    pick, requested: actual, actual, // `actual` is the one-release compatibility alias of `requested`.
    fallbackReason: !options.explicit && !selected ? error : null,
    latencyMs: Math.round((performance.now() - started) * 1000) / 1000, error,
  };
  try {
    const dir = path.join(options.meshRoot, "router");
    await writeDecision(dir, `${JSON.stringify(decision)}\n`);
  } catch {
    // A read-only/full mesh must not change launch selection or fail the spawn.
    console.warn("[pi-fabric] spawn router decision log unavailable");
  }
  const binding: SpawnRouterBinding = {
    ...(actual.model ? { model: actual.model } : {}),
    thinking: actual.thinking,
  };
  return { ...binding, decisionId, ...(selected ? { selected } : {}) };
};

/** Keep provider routing/identity preparation out of the eager registration graph. */
export const routeProviderCreation = async <T extends AdmissionRequest & {
  runner?: FabricAgentRunner; complexity?: AgentRunRequest["complexity"]; residency?: AgentRunRequest["residency"];
}>(provider: AgentsProvider, kind: "spawn" | "actor", args: Record<string, unknown>, request: T,
  context: FabricInvocationContext): Promise<{ request: T; admission: SpawnRouterAdmission } | undefined> => {
  const config = provider.manager.config.router;
  if (config?.mode !== "shadow" && config?.mode !== "enforce") return undefined;
  const self = provider.participants.self();
  request = { ...request };
  // Copy the trusted runtime identity ONCE, before cwd/router awaits. Never use action arguments.
  const admission: SpawnRouterAdmission = { mesh: provider.actorManager.mesh,
    requesterSessionId: self.sessionId ?? context.extensionContext.sessionManager?.getSessionId() ?? "",
    role: self.role ?? (kind === "actor" ? "actor" : "task-agent"), targetRootId: self.rootId,
    targetHostId: request.residency === "durable" ? `resident:${digest(self.rootId).slice(0, 24)}`
      : provider.manager.runtimeHostId ?? (provider.mainAgent.local ? provider.mainAgent.id : "") };
  const runner = request.runner ?? provider.manager.config.runner;
  const defaultModel = request.model ?? provider.manager.defaultModel(runner);
  const selected = await routeAgentCreation({ config, meshRoot: admission.mesh.root, kind, role: admission.role,
    ...(request.name !== undefined ? { name: request.name } : {}),
    cwd: kind === "spawn" ? await provider.manager.resolveCwd(request.cwd, context.signal) : provider.manager.cwd,
    project: self.projectRoot ?? self.project ?? provider.manager.cwd,
    ...(kind === "spawn" ? { task: String(args.task) }
      : typeof args.instructions === "string" ? { task: args.instructions }
      : typeof args.sha256 === "string" ? { taskDigest: args.sha256 } : {}),
    parentId: self.id, defaults: { ...(defaultModel ? { model: defaultModel } : {}), thinking: request.thinking ?? provider.manager.config.thinking },
    admission, admissionRequest: request, admissionCwd: provider.manager.cwd,
    ...(request.complexity ? { complexity: request.complexity } : {}),
    explicit: (typeof args.model === "string" && !!args.model.trim()) || args.thinking !== undefined,
    validateModel: model => {
      provider.manager.assertModelAllowed(model, runner);
      if (runner !== "pi" && model === provider.manager.defaultModel(runner)) return model;
      const resolved = resolveAvailablePiModel(model, { aliases: provider.modelsConfig().aliases,
        available: context.extensionContext.modelRegistry.getAvailable().map(model => ({ provider: String(model.provider), id: String(model.id) })),
        exact: true, closest: false });
      const canonical = `${resolved.provider}/${resolved.id}`;
      provider.manager.assertModelAllowed(canonical, runner);
      return canonical;
    }, ...(context.signal ? { signal: context.signal } : {}) });
  if (!selected) return undefined;
  return { admission, request: { ...request, ...selected.selected, spawnRouterDecisionId: selected.decisionId,
    ...(kind === "spawn" && request.residency !== "durable" ? { meshRoot: admission.mesh.root } : {}) } };
};
