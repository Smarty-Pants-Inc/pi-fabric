import fs from "node:fs";
import { readFile, unlink } from "node:fs/promises";
import { closeScratch, createScratch } from "../storage/scratch.js";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { BashOperations } from "@earendil-works/pi-coding-agent";
import type { PiShellToolName } from "./pi-tools.js";

export const DEFAULT_SHELL_HANG_MS = 120_000;
export const SHELL_HANG_MAX_MS = 600_000;
const SHELL_HANG_SNAPSHOT_BYTES = 8_000;
export const SHELL_TAIL_BYTES = 1024 * 1024;
export const SHELL_LOG_BYTES = 8 * 1024 * 1024;
export const SHELL_COMPLETED_HANDLES = 256;
const SHELL_COMPLETED_MAX_AGE_MS = 24 * 60 * 60 * 1_000;
const LOG_HEADER = "[Bounded shell log: starts with retained pre-spill tail; 8 MiB total cap, then further output is omitted. Not a full-output archive.]\n";
const LOG_TRUNCATED = "\n[Shell log truncated: disk limit reached; subsequent output omitted.]\n";

const posixQuote = (value: string): string =>
  "'" + value.replaceAll("'", "'\\''") + "'";

const powershellQuote = (value: string): string =>
  "'" + value.replaceAll("'", "''") + "'";

export const wrapShellCommandForPid = (
  command: string,
  pidPath: string,
  tool: PiShellToolName,
): string =>
  tool === "powershell"
    ? `Set-Content -LiteralPath ${powershellQuote(pidPath)} -Value $PID\n${command}`
    // Git Bash `$$` is an MSYS pid; Node and taskkill need /proc/$$/winpid.
    : `printf '%s\\n' "$(cat /proc/$$/winpid 2>/dev/null || printf '%s' "$$")" > ${posixQuote(pidPath)}\n${command}`;

export const formatShellHangNotice = (input: {
  elapsedMs: number;
  pid?: number;
  logPath: string;
}): string => {
  const seconds = Math.max(1, Math.round(input.elapsedMs / 1_000));
  const pid = input.pid !== undefined ? ` (pid ${input.pid})` : "";
  return `[Still running after ${seconds}s${pid}. Bounded live output (may be truncated): ${input.logPath}]`;
};

export const appendShellHangNotice = (output: string, notice: string): string =>
  output ? `${output}\n\n${notice}` : notice;

export const parseShellPid = (text: string): number | undefined => {
  const pid = Number(text.trim());
  return Number.isSafeInteger(pid) && pid > 1 ? pid : undefined;
};

type FabricShellJobStatus = "running" | "spilled" | "exited" | "killed";

export interface FabricShellJobInfo {
  id: string;
  tool: PiShellToolName;
  command: string;
  pid?: number;
  logPath?: string;
  startedAt: number;
  spilledAt?: number;
  finishedAt?: number;
  status: FabricShellJobStatus;
  exitCode?: number | null;
}

export interface FabricShellJobHandle {
  readonly id: string;
  readonly tool: PiShellToolName;
  readonly command: string;
  readonly abort: AbortController;
  readonly startedAt: number;
  readonly pidPath: string;
  pid?: number;
  logPath?: string;
  spilled: boolean;
  finished: boolean;
  append(data: Buffer): void;
  snapshotText(maxBytes?: number): string;
  persistLog(): Promise<string>;
  readPid(): Promise<number | undefined>;
  spill(): void;
  whenSpill(): Promise<void>;
  finish(exitCode?: number | null, footer?: string): Promise<void>;
}

class FabricShellJob implements FabricShellJobHandle {
  readonly id: string;
  readonly tool: PiShellToolName;
  readonly command: string;
  readonly abort = new AbortController();
  readonly startedAt = Date.now();
  readonly pidPath: string;
  pid?: number;
  logPath?: string;
  spilled = false;
  finished = false;
  spilledAt?: number;
  finishedAt?: number;
  exitCode?: number | null;
  status: FabricShellJobStatus = "running";
  #tail = Buffer.alloc(0);
  #omitted = false;
  readonly #directory: string;
  #descriptor: number | undefined;
  #logBytes = 0;
  #logTruncated = false;
  #spill = new AbortController();
  #pidRead: Promise<number | undefined> | undefined;

  constructor(tool: PiShellToolName, command: string, readonly onFinish: () => void, tempRoot: string) {
    this.id = randomUUID();
    this.tool = tool;
    this.command = command;
    this.#directory = createScratch("shell", tempRoot);
    this.pidPath = path.join(this.#directory, "child.pid");
  }

  append(data: Buffer): void {
    if (this.finished) return;
    const keep = Math.max(0, SHELL_TAIL_BYTES - data.length);
    if (this.#tail.length + data.length > SHELL_TAIL_BYTES) this.#omitted = true;
    // Copy slices: a tiny view must not pin an arbitrarily large input buffer.
    this.#tail = Buffer.concat([
      this.#tail.subarray(Math.max(0, this.#tail.length - keep)),
      data.subarray(Math.max(0, data.length - SHELL_TAIL_BYTES)),
    ]);
    this.#writeLog(data);
  }

  #writeLog(data: Buffer): void {
    if (this.#descriptor === undefined || this.#logTruncated) return;
    const available = Math.max(0, SHELL_LOG_BYTES - Buffer.byteLength(LOG_TRUNCATED) - this.#logBytes);
    try {
      const chunk = data.subarray(0, available);
      // Bounded synchronous writes avoid an unbounded WriteStream backpressure queue.
      let offset = 0;
      while (offset < chunk.length) {
        const written = fs.writeSync(this.#descriptor, chunk, offset);
        if (written <= 0) throw new Error("Shell log write made no progress");
        offset += written;
      }
      this.#logBytes += chunk.length;
      if (chunk.length < data.length) {
        fs.writeSync(this.#descriptor, LOG_TRUNCATED);
        this.#logTruncated = true;
      }
    } catch {
      // Never crash the subprocess data handler on ENOSPC. The header already
      // disclaims completeness; close the descriptor and stop accepting output.
      try { fs.closeSync(this.#descriptor); } catch {}
      this.#descriptor = undefined;
      this.#logTruncated = true;
    }
  }

  snapshotText(maxBytes = SHELL_HANG_SNAPSHOT_BYTES): string {
    const limit = Number.isFinite(maxBytes) ? Math.max(0, Math.floor(maxBytes)) : SHELL_TAIL_BYTES;
    const slice = this.#tail.subarray(Math.max(0, this.#tail.length - limit));
    const truncated = this.#omitted || slice.length < this.#tail.length;
    return `${truncated ? "[Output truncated; retained tail follows]\n" : ""}${slice.toString("utf8")}`;
  }

  async persistLog(): Promise<string> {
    if (this.logPath) return this.logPath;
    if (this.finished) throw new Error("Shell job finished before a log was requested");
    const logPath = path.join(this.#directory, "output.log");
    try {
      this.#descriptor = fs.openSync(logPath, "wx", 0o600);
      fs.writeSync(this.#descriptor, LOG_HEADER);
      this.#logBytes = Buffer.byteLength(LOG_HEADER);
      if (this.#omitted) this.#writeLog(Buffer.from("[Pre-spill output truncated: only the last 1 MiB was retained.]\n"));
      this.#writeLog(this.#tail);
      this.logPath = logPath;
      return logPath;
    } catch (error) {
      if (this.#descriptor !== undefined) { try { fs.closeSync(this.#descriptor); } catch {} }
      this.#descriptor = undefined;
      try { fs.unlinkSync(logPath); } catch {}
      throw error;
    }
  }

  async readPid(): Promise<number | undefined> {
    if (this.pid !== undefined) return this.pid;
    this.#pidRead ??= (async () => {
      for (let attempt = 0; attempt < 25; attempt += 1) {
        try {
          const pid = parseShellPid(await readFile(this.pidPath, "utf8"));
          if (pid !== undefined) {
            this.pid = pid;
            return pid;
          }
        } catch {
          // Pid file is written by the child after spawn.
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      return undefined;
    })();
    return this.#pidRead;
  }

  spill(): void {
    if (this.spilled || this.finished) return;
    this.spilled = true;
    this.spilledAt = Date.now();
    this.status = "spilled";
    if (!this.#spill.signal.aborted) this.#spill.abort();
  }

  whenSpill(): Promise<void> {
    if (this.spilled || this.#spill.signal.aborted) return Promise.resolve();
    return new Promise((resolve) => {
      this.#spill.signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }

  async finish(exitCode?: number | null, footer?: string): Promise<void> {
    if (this.finished) return;
    // A fast exit can beat the provider's persistLog continuation after spill.
    const persistence = this.spilled && !this.logPath ? this.persistLog() : undefined;
    this.finished = true;
    await persistence?.catch(() => undefined);
    this.finishedAt = Date.now();
    if (exitCode !== undefined) this.exitCode = exitCode;
    if (this.status === "running") {
      this.status = this.abort.signal.aborted ? "killed" : "exited";
    } else if (this.abort.signal.aborted && this.status === "spilled") {
      this.status = "killed";
    }
    if (footer) this.#writeLog(Buffer.from(footer.endsWith("\n") ? footer : `${footer}\n`));
    if (this.#descriptor !== undefined) { try { fs.closeSync(this.#descriptor); } catch {} }
    this.#descriptor = undefined;
    this.#tail = Buffer.alloc(0);
    this.#omitted = false;
    if (!this.#spill.signal.aborted) this.#spill.abort();
    await unlink(this.pidPath).catch(() => undefined);
    if (this.logPath) closeScratch(this.#directory);
    else { try { fs.rmSync(this.#directory, { recursive: true, force: true }); } catch {} }
    this.onFinish();
  }

  info(): FabricShellJobInfo {
    return {
      id: this.id,
      tool: this.tool,
      command: this.command,
      ...(this.pid !== undefined ? { pid: this.pid } : {}),
      ...(this.logPath ? { logPath: this.logPath } : {}),
      startedAt: this.startedAt,
      ...(this.spilledAt !== undefined ? { spilledAt: this.spilledAt } : {}),
      ...(this.finishedAt !== undefined ? { finishedAt: this.finishedAt } : {}),
      status: this.status,
      ...(this.exitCode !== undefined ? { exitCode: this.exitCode } : {}),
    };
  }
}

export const trackShellOperations = (
  inner: BashOperations,
  job: FabricShellJobHandle,
  tool: PiShellToolName,
): BashOperations => ({
  exec: (command, cwd, options) =>
    inner.exec(wrapShellCommandForPid(command, job.pidPath, tool), cwd, {
      ...options,
      onData: (data) => {
        job.append(data);
        options.onData(data);
      },
    }),
});

export class FabricShellJobStore {
  readonly #jobs = new Map<string, FabricShellJob>();

  constructor(readonly tempRoot = tmpdir()) {}

  #prune(): void {
    const completed = [...this.#jobs.values()].filter((job) => job.finished);
    completed.sort((a, b) => (a.finishedAt ?? 0) - (b.finishedAt ?? 0));
    for (const [index, job] of completed.entries()) {
      if (index < completed.length - SHELL_COMPLETED_HANDLES || Date.now() - (job.finishedAt ?? 0) >= SHELL_COMPLETED_MAX_AGE_MS) this.#jobs.delete(job.id);
    }
  }

  begin(tool: PiShellToolName, command: string): FabricShellJob {
    this.#prune();
    const job = new FabricShellJob(tool, command, () => this.#prune(), this.tempRoot);
    this.#jobs.set(job.id, job);
    return job;
  }

  get(id: string): FabricShellJob | undefined {
    this.#prune();
    return this.#jobs.get(id);
  }

  list(): FabricShellJobInfo[] {
    this.#prune();
    return [...this.#jobs.values()].map((job) => job.info());
  }

  waiting(): FabricShellJob[] {
    return [...this.#jobs.values()].filter((job) => !job.spilled && !job.finished);
  }

  live(): FabricShellJob[] {
    return [...this.#jobs.values()].filter((job) => !job.finished);
  }

  spillWaiting(): number {
    const jobs = this.waiting();
    for (const job of jobs) job.spill();
    return jobs.length;
  }

  killWaiting(): number {
    const jobs = this.waiting();
    for (const job of jobs) {
      if (!job.abort.signal.aborted) job.abort.abort(new Error("Command aborted"));
    }
    return jobs.length;
  }

  async close(): Promise<void> {
    const live = this.live();
    for (const job of live) {
      if (!job.abort.signal.aborted) job.abort.abort(new Error("Fabric session ended"));
    }
    await Promise.allSettled(live.map((job) => job.finish(null, "\n\n[Process ended: session closed]\n")));
    this.#jobs.clear();
  }
}

export const raceShellHang = async <T>(options: {
  execute: (signal: AbortSignal) => Promise<T>;
  parentSignal: AbortSignal | undefined;
  hangMs: number;
  immediate?: boolean;
  job: FabricShellJobHandle;
}): Promise<{ status: "done"; value: T } | { status: "error"; error: unknown } | { status: "spilled" }> => {
  const { job, parentSignal, hangMs } = options;
  const onParentAbort = (): void => {
    if (job.spilled || job.finished || job.abort.signal.aborted) return;
    job.abort.abort(parentSignal?.reason ?? new Error("Command aborted"));
  };
  if (parentSignal) {
    if (parentSignal.aborted) onParentAbort();
    else parentSignal.addEventListener("abort", onParentAbort, { once: true });
  }
  const detachParent = (): void => {
    parentSignal?.removeEventListener("abort", onParentAbort);
  };

  let hangTimer: ReturnType<typeof setTimeout> | undefined;
  const hang = new Promise<"spill">((resolve) => {
    const finish = (): void => resolve("spill");
    if (hangMs > 0) {
      hangTimer = setTimeout(() => {
        job.spill();
        finish();
      }, hangMs);
      hangTimer.unref?.();
    }
    if (options.immediate) {
      void job.readPid().then(() => {
        if (!job.finished) job.spill();
      });
    }
    void job.whenSpill().then(finish);
  });

  const execute = options.execute(job.abort.signal).then(
    (value) => ({ status: "done" as const, value }),
    (error) => ({ status: "error" as const, error }),
  );

  try {
    const first = await Promise.race([execute, hang]);
    if (first === "spill") {
      if (job.finished) return execute;
      job.spill();
      detachParent();
      void execute.then(async (result) => {
        if (job.finished) return;
        if (result.status === "done") {
          await job.finish(0, "\n\n[Process exited with code 0]\n");
          return;
        }
        const message = result.error instanceof Error ? result.error.message : String(result.error);
        await job.finish(null, "\n\n[" + message + "]\n");
      });
      return { status: "spilled" };
    }
    detachParent();
    return first;
  } finally {
    if (hangTimer) clearTimeout(hangTimer);
  }
};
