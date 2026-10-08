import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFile, mkdir } from "node:fs/promises";
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
  requestedComplexity?: "simple" | "normal" | "complex";
  host: string;
  defaults: { model: string | null; thinking: FabricThinking };
}

const digest = (text: string): string => createHash("sha256").update(text).digest("hex");
const MAX_OUTPUT_BYTES = 64 * 1024;

// The process and its pipes are retired before this promise settles. No shell,
// stderr contents, or unbounded stdout can enter the decision ledger.
const runRouter = (command: string[], input: SpawnRouterRequest, timeoutMs: number, signal?: AbortSignal): Promise<string> =>
  new Promise((resolve, reject) => {
    if (!command.length) { reject(new Error("missing-command")); return; }
    if (signal?.aborted) { reject(new Error("aborted")); return; }
    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(command[0]!, command.slice(1), {
        cwd: input.cwd, shell: false, detached: process.platform !== "win32", stdio: ["pipe", "pipe", "ignore"],
      });
    } catch { reject(new Error("command-error")); return; }
    let error: string | undefined;
    let output = "";
    let bytes = 0;
    const kill = (): void => {
      try {
        if (process.platform !== "win32" && child.pid) process.kill(-child.pid, "SIGKILL");
        else child.kill("SIGKILL");
      } catch { /* Already retired (or spawn failed). */ }
    };
    const stop = (reason: string): void => { error ??= reason; kill(); };
    const abort = (): void => stop("aborted");
    const timer = setTimeout(() => stop("timeout"), timeoutMs);
    signal?.addEventListener("abort", abort, { once: true });
    child.on("error", () => stop("command-error"));
    child.stdin!.on("error", () => stop("stdin-error"));
    child.stdout!.on("error", () => stop("stdout-error"));
    child.stdout!.setEncoding("utf8");
    child.stdout!.on("data", (chunk: string) => {
      bytes += Buffer.byteLength(chunk);
      if (bytes > MAX_OUTPUT_BYTES) stop("output-too-large");
      else output += chunk;
    });
    // Retire descendants even if a router exits without closing inherited pipes.
    child.once("exit", kill);
    child.once("close", (code) => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      if (error) reject(new Error(error));
      else if (code !== 0) reject(new Error("nonzero-exit"));
      else resolve(output);
    });
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
        ...(typeof value.reason === "string" ? { reason: value.reason } : {}),
        ...(typeof value.policyVersion === "string" ? { policyVersion: value.policyVersion } : {}) };
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
    await mkdir(dir, { recursive: true, mode: 0o700 });
    await appendFile(path.join(dir, "decisions.jsonl"), `${JSON.stringify(decision)}\n`, { mode: 0o600 });
  } catch {
    // A read-only/full mesh must not change launch selection or fail the spawn.
    console.warn("[pi-fabric] spawn router decision log unavailable");
  }
  return selected;
};
