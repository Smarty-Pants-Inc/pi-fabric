import fs from "node:fs";
import path from "node:path";
import type { BashOperations } from "@earendil-works/pi-coding-agent";
import type { JevFabricCli, JevFabricEvent, JevFabricReceipt } from "./client.js";

/** One `follow` call's ceiling; re-armed until the job is terminal. */
const FOLLOW_CEILING_MS = 300_000;

type ExecOptions = Parameters<BashOperations["exec"]>[2];

const resolveShell = (shellPath: string | undefined): string => {
  if (shellPath) {
    if (!fs.existsSync(shellPath)) throw new Error(`Custom shell path not found: ${shellPath}`);
    return shellPath;
  }
  return fs.existsSync("/bin/bash") ? "/bin/bash" : "bash";
};

const text = (value: string): Buffer => Buffer.from(value, "utf8");

/**
 * Streams a jev-fabric job into `onData` until its receipt is final. Bounded
 * previews are what jev-fabric retains: dropped bytes and evicted events are
 * disclosed inline, never silently joined. Aborting `signal` stops the job;
 * aborting `detached` only lets go of it.
 */
export async function followJevFabricJob(
  cli: JevFabricCli,
  jobId: string,
  options: { onData: (data: Buffer) => void; signal?: AbortSignal | undefined; detached?: AbortSignal | undefined },
): Promise<JevFabricReceipt> {
  const { onData, signal, detached } = options;
  let stopping: Promise<unknown> | undefined;
  const stop = (): void => { stopping ??= cli.stop(jobId).catch(() => undefined); };
  if (signal?.aborted) stop();
  else signal?.addEventListener("abort", stop, { once: true });
  const onEvent = (event: JevFabricEvent): void => {
    if (event.type === "follow.loss") {
      onData(text(`\n[jev-fabric: events ${(event.after ?? 0) + 1}..${event.next ?? "?"} aged out before this session read them]\n`));
      return;
    }
    if (event.type !== "process.output" || !event.data) return;
    const omitted = typeof event.data.omittedBytes === "number" ? event.data.omittedBytes : 0;
    if (omitted > 0) onData(text(`\n[jev-fabric: ${omitted} ${String(event.data.stream ?? "output")} bytes omitted]\n`));
    if (typeof event.data.text === "string" && event.data.text) onData(text(event.data.text));
  };
  try {
    let after = 0;
    for (;;) {
      detached?.throwIfAborted();
      const end = await cli.follow(jobId, after, { timeoutMs: FOLLOW_CEILING_MS, onEvent, ...(detached ? { signal: detached } : {}) });
      after = end.next;
      if (end.reason === "finished") return end.receipt;
    }
  } finally {
    signal?.removeEventListener("abort", stop);
    await stopping;
  }
}

const exitCodeOf = (receipt: JevFabricReceipt): number | null =>
  typeof receipt.exitCode === "number" ? receipt.exitCode : receipt.state === "exited" ? 0 : null;

export interface JevFabricLaunch {
  cli: JevFabricCli;
  taskId: string;
  /** Private directory for launch scripts; must survive this Pi process. */
  scriptDirectory: string;
  shellPath?: string | undefined;
  /** Job lifetime when the call gives no explicit `timeout`. */
  defaultTimeoutMs: number;
  maxTimeoutMs: number;
  label?: string | undefined;
  detached: AbortSignal;
  onStarted: (jobId: string, scriptPath: string) => Promise<void> | void;
  /** The job reached a final receipt (not a detach). */
  onFinished: () => Promise<void> | void;
}

/**
 * Local-bash-compatible operations whose process is owned by jev-fabric. The
 * command runs from a private script, so quoting and argv limits never apply;
 * cwd and the (spawn-hooked) environment are inherited by the durable worker.
 * Shell middleware wraps these operations exactly like the local backend.
 */
export function createJevFabricBashOperations(launch: JevFabricLaunch): BashOperations {
  return {
    exec: async (command: string, cwd: string, { onData, signal, timeout, env }: ExecOptions) => {
      if (signal?.aborted) throw new Error("aborted");
      try { await fs.promises.access(cwd); } catch {
        throw new Error(`Working directory does not exist: ${cwd}\nCannot execute bash commands.`);
      }
      const shell = resolveShell(launch.shellPath);
      await fs.promises.mkdir(launch.scriptDirectory, { recursive: true, mode: 0o700 });
      const scriptPath = path.join(launch.scriptDirectory, `${launch.taskId}.sh`);
      await fs.promises.writeFile(scriptPath, command, { mode: 0o600, flag: "w" });
      const timeoutMs = timeout !== undefined && timeout > 0
        ? Math.min(launch.maxTimeoutMs, Math.ceil(timeout * 1000))
        : launch.defaultTimeoutMs;
      let jobId: string;
      try {
        jobId = await launch.cli.start([shell, scriptPath], {
          cwd, ...(env ? { env } : {}), timeoutMs, ...(launch.label ? { label: launch.label } : {}),
        });
      } catch (error) {
        await fs.promises.rm(scriptPath, { force: true });
        throw error;
      }
      await launch.onStarted(jobId, scriptPath);
      const receipt = await followJevFabricJob(launch.cli, jobId, { onData, signal, detached: launch.detached });
      await launch.onFinished();
      if (signal?.aborted) throw new Error("aborted");
      if (receipt.state === "timed_out") throw new Error(`timeout:${timeout ?? Math.round(timeoutMs / 1000)}`);
      return { exitCode: exitCodeOf(receipt) };
    },
  };
}

/**
 * Operations that reattach to an existing job instead of launching one, so a
 * reattached task's output still passes through the active shell middleware.
 */
export function attachJevFabricBashOperations(cli: JevFabricCli, jobId: string, detached: AbortSignal): BashOperations {
  return {
    exec: async (_command: string, _cwd: string, { onData, signal }: ExecOptions) => {
      const receipt = await followJevFabricJob(cli, jobId, { onData, signal, detached });
      if (signal?.aborted) throw new Error("aborted");
      return { exitCode: exitCodeOf(receipt) };
    },
  };
}
