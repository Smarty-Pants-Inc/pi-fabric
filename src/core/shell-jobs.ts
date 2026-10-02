import fs from "node:fs";
import { readFile, unlink } from "node:fs/promises";
import { closeScratch, processAlive, ScratchScope } from "../storage/scratch.js";
import { fabricDataRoot } from "../storage/temp-root.js";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { BashOperations } from "@earendil-works/pi-coding-agent";
import type { PiShellToolName } from "./pi-tools.js";
import { ShellMonitor, type ShellMonitorOptions, type ShellMonitorBatch } from "./shell-monitor.js";

export { DEFAULT_SHELL_HANG_MS, SHELL_HANG_MAX_MS } from "./shell-limits.js";
const SHELL_HANG_SNAPSHOT_BYTES = 8_000;
export const SHELL_TAIL_BYTES = 1024 * 1024;
export const SHELL_LOG_BYTES = 8 * 1024 * 1024;
export const SHELL_COMPLETED_HANDLES = 256;
const SHELL_SPILL_PID_WAIT_MS = 10_000;
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

type FabricShellJobStatus = "running" | "spilled" | "exited" | "failed" | "killed" | "timed_out";

export interface FabricShellJobOptions {
  cwd?: string;
  ownerId?: string;
  description?: string;
  monitor?: ShellMonitorOptions;
}

export interface FabricShellJobEvent {
  type: "started" | "spilled" | "monitor" | "finished" | "acknowledged" | "stopping";
  job: FabricShellJobInfo;
  output?: string;
}


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
  cwd?: string;
  ownerId?: string;
  description?: string;
  lastOutputAt?: number;
  monitor?: ShellMonitorOptions;
  lastEvent?: ShellMonitorBatch & { at: number };
  eventCount: number;
  unread: boolean;
  stopping: boolean;
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
  exitCode?: number | null;
  spilled: boolean;
  finished: boolean;
  append(data: Buffer): void;
  snapshotText(maxBytes?: number): string;
  persistLog(): Promise<string>;
  readPid(): Promise<number | undefined>;
  waitForPid(timeoutMs?: number): Promise<number | undefined>;
  spill(): void;
  whenSpill(): Promise<void>;
  finish(exitCode?: number | null, footer?: string): Promise<void>;
  operationStarted(): void;
  operationExited(): Promise<void>;
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
  /** The "finished" event went out; finish() sets finished before its awaits (smarty-dev#2216). */
  announced = false;
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
  #monitor: ShellMonitor | undefined;
  #deadline: ReturnType<typeof setTimeout> | undefined;
  #timedOut = false;
  #finishPromise: Promise<void> | undefined;
  #operationPending = false;
  lastOutputAt?: number;
  lastEvent?: ShellMonitorBatch & { at: number };
  eventCount = 0;
  unread = false;

  constructor(tool: PiShellToolName, command: string, readonly onChange: (type: FabricShellJobEvent["type"], output?: string) => void, tempRoot: string, readonly scratch: ScratchScope, readonly options: FabricShellJobOptions = {}) {
    this.id = randomUUID();
    this.tool = tool;
    this.command = command;
    this.#directory = scratch.create("shell", tempRoot);
    this.pidPath = path.join(this.#directory, "child.pid");
    if (options.monitor) {
      this.#monitor = new ShellMonitor(options.monitor, (batch) => {
        this.lastEvent = { ...batch, at: Date.now() };
        this.eventCount += batch.lines.length + batch.omitted;
        this.unread = true;
        // The terminal event includes final output; do not race a second wakeup.
        if (!this.finished) this.onChange("monitor");
      });
      this.#deadline = setTimeout(() => {
        this.#timedOut = true;
        this.stop("Monitor deadline reached");
      }, options.monitor.timeoutMs);
      this.#deadline.unref?.();
    }
  }

  stop(reason = "Stopped by user or agent"): boolean {
    if (this.finished || this.abort.signal.aborted) return false;
    if (this.#deadline) clearTimeout(this.#deadline);
    this.#deadline = undefined;
    this.#monitor?.close(false);
    this.abort.abort(new Error(reason));
    this.onChange("stopping");
    return true;
  }

  acknowledge(): void {
    this.unread = false;
    this.onChange("acknowledged");
  }

  append(data: Buffer): void {
    if (this.finished) return;
    if (data.length > 0) this.lastOutputAt = Date.now();
    this.#monitor?.append(data);
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
      this.#watchLatePid();
      return undefined;
    })();
    return this.#pidRead;
  }

  // A spilled result must carry the pid, but a starved runner can start the shell after the
  // hang timer and the 250 ms bounded read (smarty-dev#883). Wait for the late watch until the
  // shell writes its pid, the job ends, or the bound passes.
  async waitForPid(timeoutMs = SHELL_SPILL_PID_WAIT_MS): Promise<number | undefined> {
    const deadline = Date.now() + timeoutMs;
    await this.readPid();
    while (this.pid === undefined && !this.finished && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    return this.pid;
  }

  // A shell that starts slowly (Git Bash on Windows CI can take over a second) writes its pid
  // after the bounded read above gave up. Keep looking until the job finishes, so a late pid
  // still reaches info(), list() and later readPid() calls (smarty-dev#883).
  #watchLatePid(): void {
    const deadline = Date.now() + 5 * 60 * 1_000;
    const timer = setInterval(() => {
      if (this.finished || this.pid !== undefined || Date.now() > deadline) {
        clearInterval(timer);
        return;
      }
      void readFile(this.pidPath, "utf8").then((text) => {
        const pid = parseShellPid(text);
        if (pid === undefined || this.pid !== undefined || this.finished) return;
        this.pid = pid;
        this.#pidRead = Promise.resolve(pid);
        clearInterval(timer);
      }, () => undefined);
    }, 100);
    timer.unref?.();
  }

  spill(): void {
    if (this.spilled || this.finished) return;
    this.spilled = true;
    this.spilledAt = Date.now();
    this.status = "spilled";
    this.onChange("spilled");
    if (!this.#spill.signal.aborted) this.#spill.abort();
  }

  whenSpill(): Promise<void> {
    if (this.spilled || this.#spill.signal.aborted) return Promise.resolve();
    return new Promise((resolve) => {
      this.#spill.signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }

  /** Unknown files veto every scratch sweep, even after this owner dies. */
  operationStarted(): void {
    if (this.#operationPending) return;
    fs.writeFileSync(path.join(this.#directory, "operation.pending"), "Exit not confirmed\n", { mode: 0o600, flag: "wx" });
    this.#operationPending = true;
  }

  /** Only the underlying operations API's successful exit result is cleanup authority. */
  async operationExited(): Promise<void> {
    try { await unlink(path.join(this.#directory, "operation.pending")); }
    catch { return; } // Housekeeping failure retains the veto, never changes a shell result.
    this.#operationPending = false;
    if (this.finished) {
      await this.#finishPromise;
      await this.#cleanup();
    }
  }

  finish(exitCode?: number | null, footer?: string): Promise<void> {
    return this.#finishPromise ??= this.#finish(exitCode, footer);
  }

  async #finish(exitCode?: number | null, footer?: string): Promise<void> {
    // A fast exit can beat the provider's persistLog continuation after spill.
    const persistence = this.spilled && !this.logPath ? this.persistLog() : undefined;
    const output = this.snapshotText(2000);
    this.finished = true;
    if (this.#deadline) clearTimeout(this.#deadline);
    this.#deadline = undefined;
    this.#monitor?.close(!this.abort.signal.aborted);
    await persistence?.catch(() => undefined);
    this.finishedAt = Date.now();
    if (this.exitCode === undefined && exitCode !== undefined) this.exitCode = exitCode;
    this.status = this.#timedOut ? "timed_out" : this.abort.signal.aborted ? "killed" : this.exitCode === 0 ? "exited" : "failed";
    this.unread = this.spilled;
    if (footer) this.#writeLog(Buffer.from(footer.endsWith("\n") ? footer : `${footer}\n`));
    if (this.#descriptor !== undefined) { try { fs.closeSync(this.#descriptor); } catch {} }
    this.#descriptor = undefined;
    this.#tail = Buffer.alloc(0);
    this.#omitted = false;
    if (!this.#spill.signal.aborted) this.#spill.abort();
    await this.#cleanup();
    this.announced = true;
    this.onChange("finished", [output, footer?.slice(-1000)].filter(Boolean).join("\n"));
  }

  async #cleanup(): Promise<void> {
    // A provider can finish/abort while launch or the real operation is still unresolved.
    // Do not close the owner marker or remove any durable liveness evidence in that case.
    if (this.#operationPending) return;
    let childAlive = this.pid !== undefined && processAlive(this.pid);
    try {
      // The asynchronous PID cache is not authority: the child may have written already.
      childAlive ||= processAlive(Number(fs.readFileSync(this.pidPath, "utf8").trim()));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") return;
    }
    if (!childAlive) await unlink(this.pidPath).catch(() => undefined);
    if (this.logPath || childAlive) closeScratch(this.#directory);
    else {
      try { fs.rmSync(this.#directory, { recursive: true, force: true }); this.scratch.forget(this.#directory); } catch {}
    }
  }

  async outputText(maxBytes = 8000): Promise<string> {
    if (!this.finished) return this.snapshotText(maxBytes);
    if (!this.logPath) return "No retained output log.";
    const file = await fs.promises.open(this.logPath, "r");
    try {
      const size = (await file.stat()).size;
      const length = Math.min(size, Math.max(1, Math.min(32000, maxBytes)));
      const buffer = Buffer.alloc(length);
      const { bytesRead } = await file.read(buffer, 0, length, size - length);
      return `${size > length ? "[Bounded output tail]\n" : ""}${buffer.subarray(0, bytesRead).toString("utf8")}`;
    } finally { await file.close(); }
  }

  info(): FabricShellJobInfo {
    return {
      id: this.id,
      tool: this.tool,
      command: this.command,
      ...this.options,
      ...(this.options.monitor ? { monitor: { ...this.options.monitor } } : {}),
      ...(this.lastOutputAt !== undefined ? { lastOutputAt: this.lastOutputAt } : {}),
      ...(this.lastEvent ? { lastEvent: { ...this.lastEvent, lines: [...this.lastEvent.lines] } } : {}),
      eventCount: this.eventCount,
      unread: this.unread,
      stopping: !this.finished && this.abort.signal.aborted,
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

// Test-only seam (smarty-dev#883): PI_FABRIC_TEST_PID_DELAY_MS delays the shell's pid write, the
// way a starved runner starts the shell late, so a real Pi session can prove the late-pid path.
// Ignored unless set to a positive integer.
const testPidDelay = (tool: PiShellToolName): string => {
  const ms = Number(process.env.PI_FABRIC_TEST_PID_DELAY_MS);
  if (!Number.isInteger(ms) || ms <= 0) return "";
  return tool === "powershell" ? `Start-Sleep -Milliseconds ${ms}\n` : `sleep ${ms / 1000}\n`;
};

export const trackShellOperations = (
  inner: BashOperations,
  job: FabricShellJobHandle,
  tool: PiShellToolName,
): BashOperations => ({
  exec: async (command, cwd, options) => {
    job.operationStarted();
    const result = await inner.exec(testPidDelay(tool) + wrapShellCommandForPid(command, job.pidPath, tool), cwd, {
      ...options,
      onData: (data) => {
        job.append(data);
        options.onData(data);
      },
    });
    job.exitCode = result.exitCode;
    await job.operationExited();
    return result;
  },
});

export class FabricShellJobStore {
  readonly #jobs = new Map<string, FabricShellJob>();
  readonly #listeners = new Set<(event: FabricShellJobEvent) => void>();
  #closed = false;
  readonly #closing = new AbortController();

  subscribe(listener: (event: FabricShellJobEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  #emit(event: FabricShellJobEvent): void {
    if (this.#closed) return;
    for (const listener of this.#listeners) {
      try { listener(event); } catch { /* Observers cannot break shell execution. */ }
    }
  }

  readonly #scratch = new ScratchScope();
  #closePromise: Promise<void> | undefined;

  constructor(readonly tempRoot = fabricDataRoot()) {}

  #prune(): void {
    // finishedAt precedes async log/PID cleanup. Only announced jobs are safe to evict:
    // otherwise retention pressure can remove the last reload hold before the inbox sees it.
    const completed = [...this.#jobs.values()].filter((job) => job.announced && job.finishedAt !== undefined);
    completed.sort((a, b) => (a.finishedAt ?? 0) - (b.finishedAt ?? 0));
    for (const [index, job] of completed.entries()) {
      if (index < completed.length - SHELL_COMPLETED_HANDLES || Date.now() - (job.finishedAt ?? 0) >= SHELL_COMPLETED_MAX_AGE_MS) this.#jobs.delete(job.id);
    }
  }

  begin(tool: PiShellToolName, command: string, options: FabricShellJobOptions = {}): FabricShellJob {
    if (this.#closed) throw new Error("Shell job store is closed");
    this.#prune();
    const job = new FabricShellJob(tool, command, (type, output) => {
      this.#emit({ type, job: job.info(), ...(output ? { output } : {}) });
      if (type === "finished") this.#prune();
    }, this.tempRoot, this.#scratch, options);
    this.#jobs.set(job.id, job);
    this.#emit({ type: "started", job: job.info() });
    return job;
  }

  /** Event-driven, bounded observation. Timeout/cancellation never stops the job. */
  waitFor(id: string, options: { after?: number; timeoutMs: number; signal?: AbortSignal | undefined }): Promise<{ task: FabricShellJobInfo; timedOut: boolean }> {
    options.signal?.throwIfAborted();
    this.#closing.signal.throwIfAborted();
    const job = this.get(id);
    if (!job) throw new Error(`Unknown shell task: ${id}`);
    if (!Number.isInteger(options.timeoutMs) || options.timeoutMs < 1 || options.timeoutMs > 300_000)
      throw new Error("Task observation timeoutMs must be an integer from 1 to 300000");
    if (options.after !== undefined && (!Number.isSafeInteger(options.after) || options.after < 0 || options.after > job.eventCount))
      throw new Error("Task observation after must be an existing event cursor");
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      let unsubscribe = () => {};
      const cleanup = () => {
        if (timer) clearTimeout(timer);
        unsubscribe();
        options.signal?.removeEventListener("abort", abort);
        this.#closing.signal.removeEventListener("abort", closing);
      };
      const abort = () => { cleanup(); reject(options.signal?.reason ?? new Error("Task observation cancelled")); };
      const closing = () => { cleanup(); reject(new Error("Shell job store is closed")); };
      const complete = (timedOut: boolean) => { cleanup(); resolve({ task: job.info(), timedOut }); };
      const check = () => {
        if (job.info().finishedAt !== undefined || (options.after !== undefined && job.eventCount > options.after)) complete(false);
      };
      unsubscribe = this.subscribe(event => { if (event.job.id === id) check(); });
      options.signal?.addEventListener("abort", abort, { once: true });
      this.#closing.signal.addEventListener("abort", closing, { once: true });
      timer = setTimeout(() => complete(true), options.timeoutMs);
      // Subscribe before inspecting: an exit or monitor batch cannot fall into a gap.
      check();
    });
  }

  stop(id: string): boolean {
    const job = this.get(id);
    if (!job) throw new Error(`Unknown shell task: ${id}`);
    return job.stop();
  }

  acknowledge(id: string): void { this.get(id)?.acknowledge(); }

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

  /** Jobs whose "finished" event is not sent yet: a reload would drop that notice (smarty-dev#2216). */
  unannounced(): number {
    return [...this.#jobs.values()].filter((job) => !job.announced).length;
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

  close(): Promise<void> {
    this.#closed = true;
    return this.#closePromise ??= this.#close();
  }

  async #close(): Promise<void> {
    this.#closing.abort(new Error("Shell job store is closed"));
    this.#listeners.clear();
    const live = this.live();
    for (const job of live) {
      if (!job.abort.signal.aborted) job.abort.abort(new Error("Fabric session ended"));
    }
    await Promise.allSettled([...this.#jobs.values()].map((job) => job.finish(null, "\n\n[Process ended: session closed]\n")));
    await this.#scratch.close();
    this.#jobs.clear();
  }
}

export const raceShellHang = async <T>(options: {
  execute: (signal: AbortSignal) => Promise<T>;
  parentSignal: AbortSignal | undefined;
  hangMs: number;
  immediate?: boolean;
  job: FabricShellJobHandle;
}): Promise<{ status: "done"; value: T } | { status: "error"; error: unknown } | { status: "spilled"; auto: boolean }> => {
  const { job, parentSignal, hangMs } = options;
  // True only when the hang timer spilled the job; a manual (Ctrl+B) or explicit handoff is not.
  let auto = false;
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
        auto = !job.spilled && !job.finished;
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

  // Hold before invoking the tool: validation/middleware may delay the actual spawn.
  job.operationStarted();
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
          await job.finish(0, `\n\n[Process exited with code ${job.exitCode ?? 0}]\n`);
          return;
        }
        const message = result.error instanceof Error ? result.error.message : String(result.error);
        await job.finish(null, "\n\n[" + message + "]\n");
      });
      return { status: "spilled", auto };
    }
    detachParent();
    return first;
  } finally {
    if (hangTimer) clearTimeout(hangTimer);
  }
};
