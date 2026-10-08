import { execFile, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, mkdir, open, rename } from "node:fs/promises";
import { hostname } from "node:os";
import path from "node:path";
import { isFabricThinking, type FabricThinking } from "../thinking.js";
import type { FabricAgentRouterConfig } from "./router-config.js";

export interface SpawnRouterBinding { model?: string; thinking: FabricThinking }
export interface SpawnRouterPick extends SpawnRouterBinding { model: string; reason?: string; policyVersion?: string }
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
  for (const name of ["HOME", "LANG", "TZ"]) {
    if (process.env[name] !== undefined) env[name] = process.env[name];
  }
  return env;
};
const reasonCode = (value: string, task?: string): string =>
  value.length > 0 && !/[^a-z0-9_.:-]/.test(value) && (!task || !value.includes(task)) ? value.slice(0, 64) : "redacted";
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

// Never wait for close after a timeout: descendants may retain inherited pipes.
const runRouter = (command: string[], input: SpawnRouterRequest, timeoutMs: number, signal?: AbortSignal): Promise<string> =>
  new Promise((resolve, reject) => {
    if (!command.length) { reject(new Error("missing-command")); return; }
    if (!path.isAbsolute(command[0]!)) { reject(new Error("invalid-command")); return; }
    if (signal?.aborted) { reject(new Error("aborted")); return; }
    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(command[0]!, command.slice(1), {
        cwd: input.cwd, shell: false, env: routerEnv(), detached: process.platform !== "win32", stdio: ["pipe", "pipe", "ignore"],
      });
    } catch { reject(new Error("command-error")); return; }
    let settled = false;
    let retired = false;
    let output = "";
    let bytes = 0;
    const kill = (): void => {
      if (retired) return;
      retired = true;
      try {
        if (process.platform === "win32" && child.pid) {
          // Never search PATH for the tree killer either.
          const root = process.env.SystemRoot;
          const taskkill = path.win32.join(root && path.win32.isAbsolute(root) ? root : "C:\\Windows", "System32", "taskkill.exe");
          execFile(taskkill, ["/T", "/F", "/PID", String(child.pid)], {
            env: routerEnv(), windowsHide: true, timeout: 1000,
          }, () => {});
        } else if (child.pid) process.kill(-child.pid, "SIGKILL");
        else child.kill("SIGKILL");
      } catch { /* Already retired (or spawn failed). */ }
    };
    const finish = (error?: string): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      if (error) reject(new Error(error));
      else resolve(output);
      kill();
      child.stdin!.destroy();
      child.stdout!.destroy();
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
    child.once("exit", kill);
    child.once("close", (code) => finish(code === 0 ? undefined : "nonzero-exit"));
    child.stdin!.end(`${JSON.stringify(input)}\n`);
    if (signal?.aborted) abort();
  });

/** First-use adapter only. Undefined means preserve the original static binding. */
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
}): Promise<SpawnRouterBinding | undefined> => {
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
  // The digest joins request metadata without storing raw task text in the log.
  const { task: _task, ...privateRequest } = request;
  const requestDigest = digest(JSON.stringify(privateRequest));
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
        ...(typeof value.reason === "string" ? { reason: reasonCode(value.reason, options.task) } : {}),
        ...(typeof value.policyVersion === "string" ? { policyVersion: reasonCode(value.policyVersion, options.task) } : {}) };
      if (mode === "enforce") {
        selected = { model: pick.model, thinking: pick.thinking };
        actual = { model: pick.model, thinking: pick.thinking };
      }
    } catch (cause) {
      // Only fixed adapter error codes are emitted; never router stderr or task.
      error = cause instanceof Error ? cause.message : "router-error";
    }
  }
  const decision = {
    ts: new Date().toISOString(), requestDigest, kind: request.kind, mode,
    decision: options.explicit ? "explicit" : selected ? "enforce" : "default",
    pick, actual, latencyMs: Math.round((performance.now() - started) * 1000) / 1000, error,
  };
  try {
    const dir = path.join(options.meshRoot, "router");
    await writeDecision(dir, `${JSON.stringify(decision)}\n`);
  } catch {
    // A read-only/full mesh must not change launch selection or fail the spawn.
    console.warn("[pi-fabric] spawn router decision log unavailable");
  }
  return selected;
};
