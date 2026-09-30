import { spawn } from "node:child_process";
import { StringDecoder } from "node:string_decoder";

const OUTPUT_MAX_BYTES = 1024 * 1024;
const LINE_MAX_CHARS = 1024 * 1024;
const CONTROL_TIMEOUT_MS = 30_000;

export type JevFabricState = "running" | "exited" | "failed" | "timed_out" | "cancelled";

/** A `status`/`wait`/`stop` result: running state or the final receipt. */
export interface JevFabricReceipt {
  id: string;
  state: JevFabricState;
  exitCode?: number | null;
  timedOut?: boolean;
  cancelled?: boolean;
  label?: string;
}

export interface JevFabricEvent {
  sequence?: number;
  type: string;
  data?: Record<string, unknown>;
  after?: number;
  next?: number;
}

export interface JevFabricFollowEnd {
  reason: "finished" | "timeout";
  next: number;
  receipt: JevFabricReceipt;
}

export interface JevFabricListedJob {
  id: string;
  /** `starting` until the worker is ready; then a status state. */
  state: JevFabricState | "starting";
  label?: string;
  startedAt?: number;
}

export class JevFabricError extends Error {
  constructor(message: string, readonly code: number | null) { super(message); this.name = "JevFabricError"; }
}

const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseJson = (text: string, label: string): unknown => {
  try { return JSON.parse(text); } catch { throw new JevFabricError(`jev-fabric ${label} returned malformed JSON`, null); }
};

export const parseReceipt = (value: unknown): JevFabricReceipt => {
  if (!object(value) || typeof value.id !== "string" || typeof value.state !== "string")
    throw new JevFabricError("jev-fabric returned an unrecognized job state", null);
  return value as unknown as JevFabricReceipt;
};

interface RunOptions {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  signal?: AbortSignal;
  /** Host-side ceiling for the CLI call itself, separate from the job lifetime. */
  timeoutMs?: number;
  onLine?: (line: string) => void;
}

/**
 * Thin argv client for the jev-fabric CLI. Every call is a literal argv spawn
 * (no shell) against one explicit storage home. Budgets, deadlines and job
 * semantics stay in the binary; this only frames JSON.
 */
export class JevFabricCli {
  constructor(readonly binary: string, readonly home: string) {}

  #env(env: NodeJS.ProcessEnv | undefined): NodeJS.ProcessEnv {
    return { ...(env ?? process.env), JEV_FABRIC_HOME: this.home };
  }

  run(args: readonly string[], options: RunOptions = {}): Promise<string> {
    return new Promise((resolve, reject) => {
      if (options.signal?.aborted) { reject(options.signal.reason ?? new Error("aborted")); return; }
      const child = spawn(this.binary, ["--", ...args], {
        cwd: options.cwd,
        env: this.#env(options.env),
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
      });
      const stdout: Buffer[] = [];
      let stdoutBytes = 0;
      let stderr = "";
      let pending = "";
      const decoder = new StringDecoder("utf8");
      let settled = false;
      const finish = (error: Error | undefined, value?: string): void => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        options.signal?.removeEventListener("abort", abort);
        if (error) reject(error); else resolve(value ?? "");
      };
      const abort = (): void => {
        child.kill("SIGTERM");
        finish(options.signal?.reason instanceof Error ? options.signal.reason : new Error("aborted"));
      };
      const timer = options.timeoutMs ? setTimeout(() => {
        child.kill("SIGTERM");
        finish(new JevFabricError(`jev-fabric ${args[0]} did not answer within ${options.timeoutMs} ms`, null));
      }, options.timeoutMs) : undefined;
      timer?.unref?.();
      options.signal?.addEventListener("abort", abort, { once: true });
      child.stdout.on("data", (chunk: Buffer) => {
        if (options.onLine) {
          pending += decoder.write(chunk);
          let index: number;
          while ((index = pending.indexOf("\n")) >= 0) {
            const line = pending.slice(0, index).trim();
            pending = pending.slice(index + 1);
            if (line) options.onLine(line);
          }
          if (pending.length > LINE_MAX_CHARS) { child.kill("SIGTERM"); finish(new JevFabricError(`jev-fabric ${args[0]} line exceeded ${LINE_MAX_CHARS} characters`, null)); }
          return;
        }
        if (stdoutBytes + chunk.length > OUTPUT_MAX_BYTES) { child.kill("SIGTERM"); finish(new JevFabricError(`jev-fabric ${args[0]} output exceeded ${OUTPUT_MAX_BYTES} bytes`, null)); return; }
        stdoutBytes += chunk.length;
        stdout.push(chunk);
      });
      child.stderr.on("data", (chunk: Buffer) => { if (stderr.length < 4096) stderr += chunk.toString("utf8"); });
      child.once("error", (error: NodeJS.ErrnoException) => {
        finish(error.code === "ENOENT"
          ? new JevFabricError(`jev-fabric executable not found: ${this.binary}. Ask the user before installing it (curl -fsSL https://raw.githubusercontent.com/monotykamary/jev-fabric/main/install.sh | sh), or set executor.jevFabric.binary.`, null)
          : error);
      });
      child.once("close", (code) => {
        if (options.onLine) {
          const rest = (pending + decoder.end()).trim();
          if (rest) options.onLine(rest);
        }
        if (code === 0) { finish(undefined, Buffer.concat(stdout).toString("utf8")); return; }
        const message = stderr.trim().split("\n").at(-1)?.slice(0, 500) || `exit ${code}`;
        finish(new JevFabricError(`jev-fabric ${args[0]} failed: ${message}`, code));
      });
    });
  }

  async start(argv: readonly string[], options: { cwd: string; env?: NodeJS.ProcessEnv; timeoutMs: number; label?: string }): Promise<string> {
    const label = options.label ? ["--label", options.label] : [];
    const text = await this.run(["start", "--timeout-ms", String(options.timeoutMs), ...label, "--", ...argv], {
      cwd: options.cwd, ...(options.env ? { env: options.env } : {}), timeoutMs: CONTROL_TIMEOUT_MS,
    });
    const value = parseJson(text, "start");
    if (!object(value) || typeof value.id !== "string" || !value.id) throw new JevFabricError("jev-fabric start returned no job id", null);
    return value.id;
  }

  async status(id: string): Promise<JevFabricReceipt> {
    return parseReceipt(parseJson(await this.run(["status", id], { timeoutMs: CONTROL_TIMEOUT_MS }), "status"));
  }

  async stop(id: string): Promise<JevFabricReceipt> {
    return parseReceipt(parseJson(await this.run(["stop", id], { timeoutMs: CONTROL_TIMEOUT_MS }), "stop"));
  }

  async list(): Promise<{ jobs: JevFabricListedJob[]; truncated: boolean }> {
    const value = parseJson(await this.run(["list"], { timeoutMs: CONTROL_TIMEOUT_MS }), "list");
    if (!object(value) || !Array.isArray(value.jobs)) throw new JevFabricError("jev-fabric list returned an unrecognized result", null);
    const jobs = value.jobs.filter((job): job is JevFabricListedJob => object(job) && typeof job.id === "string" && typeof job.state === "string");
    return { jobs, truncated: value.truncated === true };
  }

  /**
   * One bounded live `follow` call: every retained event after the cursor as
   * the worker publishes it, then one `follow.end`. The ceiling never stops the job.
   */
  async follow(id: string, after: number, options: { timeoutMs: number; signal?: AbortSignal; onEvent: (event: JevFabricEvent) => void }): Promise<JevFabricFollowEnd> {
    let end: JevFabricFollowEnd | undefined;
    await this.run(["follow", "--timeout-ms", String(options.timeoutMs), id, String(after)], {
      ...(options.signal ? { signal: options.signal } : {}),
      timeoutMs: options.timeoutMs + CONTROL_TIMEOUT_MS,
      onLine: (line) => {
        const value = parseJson(line, "follow");
        if (!object(value) || typeof value.type !== "string") return;
        if (value.type === "follow.end") {
          end = {
            reason: value.reason === "finished" ? "finished" : "timeout",
            next: typeof value.next === "number" ? value.next : after,
            receipt: parseReceipt(value.receipt),
          };
          return;
        }
        options.onEvent(value as unknown as JevFabricEvent);
      },
    });
    if (!end) throw new JevFabricError("jev-fabric follow ended without a follow.end record", null);
    return end;
  }
}
