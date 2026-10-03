import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { terminateWindowsTree } from "../child-process-tree.js";
import type { VerifyStatus } from "./types.js";

export interface RunCommandOptions {
  cwd: string;
  timeoutMs: number;
  signal?: AbortSignal | undefined;
}

export interface CommandResult {
  status: VerifyStatus;
  exitCode: number | null;
  output: string;
  outputBytes: number;
  outputOmittedBytes: number;
  outputDigest: string;
  error?: string;
}

const COMMAND_OUTPUT_MAX_BYTES = 32 * 1024;

const errorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const truncateUtf8 = (
  value: string,
  maxBytes: number,
): { value: string; omittedBytes: number } => {
  const bytes = Buffer.from(value, "utf8");
  if (bytes.length <= maxBytes) return { value, omittedBytes: 0 };
  let end = maxBytes;
  while (end > 0 && (bytes[end]! & 0xc0) === 0x80) end--;
  const bounded = bytes.subarray(0, end).toString("utf8");
  return { value: bounded, omittedBytes: bytes.length - end };
};

const terminateProcessTree = async (child: ChildProcess): Promise<void> => {
  if (process.platform === "win32") {
    await terminateWindowsTree(child);
    return;
  }
  if (child.pid === undefined) return;
  try {
    process.kill(-child.pid, "SIGKILL");
  } catch {
    try {
      child.kill("SIGKILL");
    } catch {
      // The process group may already have exited.
    }
  }
};

// Shell evidence is trusted input. Output is streamed into a byte-bounded
// prefix while a hash and byte count cover the complete stdout/stderr stream.
// POSIX shells lead detached process groups so timeout/abort can kill the
// group. Windows joins successful taskkill tree cleanup; a failed helper
// retains an alarmed uncertain-tree fence rather than trusting a parent-only kill.
export const runCommand = (
  command: string,
  options: RunCommandOptions,
): Promise<CommandResult> =>
  new Promise((resolve) => {
    let settled = false;
    let outputBytes = 0;
    const outputChunks: Buffer[] = [];
    let retainedBytes = 0;
    const outputHash = createHash("sha256");
    let timer: NodeJS.Timeout | undefined;
    let terminationReason: string | undefined;
    let termination: Promise<void> | undefined;
    let child: ChildProcess;

    const collect = (chunk: Buffer | string): void => {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      outputBytes += bytes.length;
      outputHash.update(bytes);
      if (retainedBytes >= COMMAND_OUTPUT_MAX_BYTES) return;
      const retained = bytes.subarray(
        0,
        Math.min(bytes.length, COMMAND_OUTPUT_MAX_BYTES - retainedBytes),
      );
      outputChunks.push(retained);
      retainedBytes += retained.length;
    };
    const finish = (
      status: VerifyStatus,
      exitCode: number | null,
      error?: string,
    ): void => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      if (options.signal) options.signal.removeEventListener("abort", abort);
      const retained = Buffer.concat(outputChunks);
      const boundedOutput = truncateUtf8(retained.toString("utf8"), retained.length);
      resolve({
        status,
        exitCode,
        output: boundedOutput.value,
        outputBytes,
        outputOmittedBytes: outputBytes - Buffer.byteLength(boundedOutput.value, "utf8"),
        outputDigest: `sha256:${outputHash.digest("hex")}`,
        ...(error !== undefined ? { error } : {}),
      });
    };
    const terminate = (reason: string): void => {
      if (terminationReason !== undefined) return;
      terminationReason = reason;
      if (timer) clearTimeout(timer);
      termination = terminateProcessTree(child);
      if (process.platform === "win32") {
        void termination.then(() => {
          const fallback = setTimeout(() => {
            child.stdout?.removeListener("data", collect);
            child.stderr?.removeListener("data", collect);
            child.stdout?.destroy();
            child.stderr?.destroy();
            finish("error", null, reason);
          }, 100);
          fallback.unref?.();
        });
      }
    };
    const abort = (): void => terminate("aborted");

    try {
      child = spawn(command, {
        shell: true,
        cwd: options.cwd,
        detached: process.platform !== "win32",
        windowsHide: true,
      });
    } catch (error) {
      finish("error", null, errorMessage(error));
      return;
    }
    child.stdout?.on("data", collect);
    child.stderr?.on("data", collect);
    child.once("error", (error) => finish("error", null, errorMessage(error)));
    child.once("close", (code) => {
      void (async () => {
        if (termination) await termination;
        if (terminationReason !== undefined) {
          finish("error", null, terminationReason);
          return;
        }
        const exitCode = typeof code === "number" ? code : null;
        if (exitCode === null) {
          finish("error", null, "process terminated by signal");
          return;
        }
        finish(exitCode === 0 ? "confirmed" : "violated", exitCode);
      })();
    });
    if (options.timeoutMs > 0) {
      timer = setTimeout(
        () => terminate(`timeout after ${options.timeoutMs}ms`),
        options.timeoutMs,
      );
      timer.unref?.();
    }
    if (options.signal) {
      options.signal.addEventListener("abort", abort, { once: true });
      if (options.signal.aborted) abort();
    }
  });
