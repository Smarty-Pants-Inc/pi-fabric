import { execFile, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { lstat, mkdir, open, rename } from "node:fs/promises";
import { hostname } from "node:os";
import path from "node:path";
import { readPiSessionHeader } from "../core/pi-session-header.js";
import { isFabricThinking, type FabricThinking } from "../thinking.js";
import type { FabricAgentRouterConfig } from "./router-config.js";

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
}

/** Fixed, privacy-safe code used when admission created no started record. */
export const SPAWN_ROUTER_START_FAILED = "start-failed";
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

/** Append the applied binding after its agent/actor record exists. */
export const recordSpawnRouterStart = async (options: SpawnRouterStartOptions): Promise<void> => {
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

/** Append one fixed, task-free failure when admission did not create a record. */
export const recordSpawnRouterStartFailed = async (options: Pick<SpawnRouterStartOptions, "meshRoot" | "decisionId">): Promise<void> => {
  await appendStartRecord(options.meshRoot, {
    ts: new Date().toISOString(), kind: "start-failed", decisionId: options.decisionId,
    agentId: null, sessionId: null, launch: { runId: null, pid: null, processStartTime: null },
    host: hostname(), model: null, thinking: null, verified: false, evidenceRef: null,
    error: SPAWN_ROUTER_START_FAILED,
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
  signal?: AbortSignal;
}): Promise<SpawnRouterResult | undefined> => {
  const config = options.config;
  const mode = config.mode ?? "off";
  if (mode !== "shadow" && mode !== "enforce") return undefined;
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
