import { ExecutionDeadline } from "./execution-deadline.js";
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { constants } from "node:fs";
import { access, realpath } from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import type { Duplex } from "node:stream";
import { StringDecoder } from "node:string_decoder";
import { mainExecutionCeilingAbortReason, preserveCancellationOutcome, runAbortable, settleWithin, shareCancellationEffects } from "../async-settlement.js";
import { piBashExitMetadata } from "../core/pi-bash-error.js";
import { isPiShellRef } from "../core/pi-tools.js";
import type { FabricHostCall, FabricKernelRuntime, FabricSandboxOptions, FabricSandboxResult } from "./kernel.js";
import { CPYTHON_CHILD_SOURCE } from "./cpython-child-source.js";
import { linuxCPythonNetworkFilter } from "./cpython-linux-sandbox.js";

const MAX_FRAME_BYTES = 16 * 1024 * 1024;
const HOST_SETTLE_MS = 250;
// Bounds only the wait for a killed child's exit diagnosis after its IPC broke.
const PIPE_DIAGNOSIS_MS = 2_000;
// No blanket mach* allowance: Mach services can broker effects outside the
// file/network policy. CPython's standard-library startup needs none here.
const MACOS_PROFILE = "(version 1) (deny default) (allow process-exec) (allow process-fork) (allow file-read*) (allow sysctl-read)";

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const errorText = (error: unknown): string => error instanceof Error ? error.message : String(error);

const executable = async (binary: string, cwd: string): Promise<string> => {
  // Windows stores executables with PATHEXT suffixes ("python3" -> "python3.exe");
  // probe the variants spawn would find instead of failing on the bare name.
  const extensions = process.platform === "win32"
    ? String(process.env.PATHEXT ?? ".COM;.EXE;.BAT;.CMD").split(";").filter(Boolean)
    : [];
  const variants = (candidate: string): string[] =>
    extensions.length && !/\.[A-Za-z0-9]+$/.test(candidate)
      ? [candidate, ...extensions.map((extension) => candidate + extension)]
      : [candidate];
  const candidates = path.isAbsolute(binary) || binary.includes("/") || binary.includes("\\")
    ? [path.resolve(cwd, binary)]
    : (process.env.PATH ?? "").split(path.delimiter).map((directory) => path.resolve(cwd, directory || ".", binary));
  for (const candidate of candidates) {
    for (const variant of variants(candidate)) {
      try {
        await access(variant, constants.X_OK);
        return await realpath(variant);
      } catch {
        // Continue only during executable discovery, never after a failed spawn.
      }
    }
  }
  throw new Error(`CPython executable not found: ${binary}. Install Python 3 or set executor.cpython.binary to a trusted executable's absolute path.`);
};

// The isolation flags are exported so the sandbox tests can probe whether this
// kernel can actually start the same sandbox, instead of treating an installed
// bwrap as a usable one. The seccomp filter rides in on fd 4, so a probe can
// leave the seccomp arguments off.
export const LINUX_BWRAP_ISOLATION_ARGS = ["--ro-bind", "/", "/", "--unshare-all", "--die-with-parent", "--new-session", "--proc", "/proc", "--dev", "/dev"] as const;
export const LINUX_BWRAP_SECCOMP_ARGS = ["--seccomp", "4"] as const;

const launch = async (binary: string, enforce: boolean, cwd: string): Promise<{ command: string; args: string[]; seccomp?: Buffer }> => {
  const python = await executable(binary, cwd);
  const args = ["-I", "-B", "-u", "-c", CPYTHON_CHILD_SOURCE];
  if (!enforce) return { command: python, args };
  if (process.platform === "darwin") {
    try { await access("/usr/bin/sandbox-exec", constants.X_OK); }
    catch { throw new Error("Schema enforce CPython requires /usr/bin/sandbox-exec on macOS; no unsandboxed fallback is permitted."); }
    return { command: "/usr/bin/sandbox-exec", args: ["-p", MACOS_PROFILE, python, ...args] };
  }
  if (process.platform === "linux") {
    // Only system locations: a workspace/PATH shim must not masquerade as the
    // security boundary. The selected interpreter itself is trusted config.
    let bwrap: string | undefined;
    for (const candidate of ["/usr/bin/bwrap", "/bin/bwrap"]) {
      try { await access(candidate, constants.X_OK); bwrap = candidate; break; } catch { /* Try the other system path. */ }
    }
    if (!bwrap) throw new Error("Schema enforce CPython requires bubblewrap (/usr/bin/bwrap). Install bubblewrap and enable unprivileged user namespaces; no unsandboxed fallback is permitted.");
    return {
      command: bwrap,
      args: [...LINUX_BWRAP_ISOLATION_ARGS, ...LINUX_BWRAP_SECCOMP_ARGS, "--chdir", cwd, "--", python, ...args],
      seccomp: linuxCPythonNetworkFilter(process.arch),
    };
  }
  throw new Error(`Schema enforce CPython has no supported OS sandbox on ${process.platform}; no unsandboxed fallback is permitted.`);
};

// Windows stdio[3] is an anonymous pipe, not a socket, so the child dials a
// loopback listener instead; a one-time token proves the caller is our child.
const createIpcListener = (): Promise<{ server: net.Server; port: number; token: string }> =>
  new Promise((resolve, reject) => {
    const server = net.createServer();
    const token = randomBytes(32).toString("hex");
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") reject(new Error("Fabric CPython IPC listener failed to bind"));
      else resolve({ server, port: address.port, token });
    });
  });

/** A fresh CPython process per call. Only enforce=true adds an OS security boundary. */
export class CPythonRuntime implements FabricKernelRuntime {
  constructor(readonly binary = "python3", readonly enforce = false) {}

  async execute(code: string, hostCall: FabricHostCall, options: FabricSandboxOptions): Promise<FabricSandboxResult> {
    const failure = (terminationReason: FabricSandboxResult["terminationReason"], error: string): FabricSandboxResult =>
      ({ value: undefined, logs: [], terminationReason, error });
    if (options.signal?.aborted) return failure("aborted", "Execution cancelled");
    if (!Number.isSafeInteger(options.memoryLimitBytes) || options.memoryLimitBytes < 1) {
      return failure("runtime_error", "CPython memory limit must be a positive safe integer");
    }
    if (!Number.isFinite(options.timeoutMs) || options.timeoutMs < 1) return failure("runtime_error", "CPython timeout must be positive");
    const executionDeadline = options.executionDeadline ?? new ExecutionDeadline(options);
    let command: Awaited<ReturnType<typeof launch>>;
    try { command = await launch(this.binary, this.enforce, options.cwd ?? process.cwd()); }
    catch (error) { return failure("runtime_error", errorText(error)); }
    // Resolve first, then check before spawn: no orphan on cancellation during discovery.
    if (options.signal?.aborted) return failure("aborted", "Execution cancelled");
    if (executionDeadline.reached) return executionDeadline.timeoutResult([]);
    // Windows cannot inherit a socket through stdio; the child connects back instead.
    const ipc = process.platform === "win32" ? await createIpcListener() : undefined;
    // Binding is asynchronous too: cancellation or deadline expiry here must
    // close the listener without starting a guest just to kill it later.
    if (executionDeadline.reached || options.signal?.aborted) {
      ipc?.server.close();
      return options.signal?.aborted ? failure("aborted", "Execution cancelled") : executionDeadline.timeoutResult([]);
    }

    return new Promise<FabricSandboxResult>((resolve) => {
      const hostAbort = new AbortController();
      shareCancellationEffects(hostAbort.signal, options.signal);
      const hostTasks = new Set<Promise<void>>();
      const callIds = new Set<number>();
      // Host-owned response ids are unique within this fresh execution. Native
      // write callbacks cannot prove that the guest admitted an observation.
      let nextResponseId = 0;
      const pendingReceipts = new Map<number, { id: number; commit: () => void }>();
      const logs: string[] = [];
      const partialLogs = ["", ""];
      const decoders = [new StringDecoder("utf8"), new StringDecoder("utf8")];
      const maxLogChars = Math.max(0, options.maxLogChars ?? 100_000);
      let logChars = 0;
      let truncated = false;
      let settled = false;
      let finishing = false;
      let buffer = Buffer.alloc(0);
      let child: ReturnType<typeof spawn>;
      try {
        child = spawn(command.command, command.args, {
          cwd: options.cwd ?? process.cwd(),
          // -I ignores PYTHON* and user site packages; -B avoids bytecode writes.
          // Keep ordinary environment for trusted native code, not a false secrecy claim.
          env: ipc ? { ...process.env, FABRIC_IPC_PORT: String(ipc.port), FABRIC_IPC_TOKEN: ipc.token } : process.env,
          detached: process.platform !== "win32",
          stdio: command.seccomp ? ["ignore", "pipe", "pipe", "pipe", "pipe"] : ["ignore", "pipe", "pipe", "pipe"],
        });
      } catch (error) {
        ipc?.server.close();
        resolve(failure("runtime_error", `CPython process failed: ${errorText(error)}`));
        return;
      }
      let channel: Duplex | net.Socket | undefined;
      let expectedToken: string | undefined = ipc?.token;
      let childClosed = child.pid === undefined;
      child.once("close", () => { childClosed = true; });
      let logsFinalized = false;
      const appendLog = (index: number, text: string): void => {
        // Settlement ends IPC admission, not the drain of already-written logs.
        if (logsFinalized || truncated) return;
        const available = Math.max(0, maxLogChars - logChars);
        const retained = text.slice(0, available);
        logChars += retained.length;
        const lines = ((partialLogs[index] ?? "") + retained).split(/\r?\n/);
        partialLogs[index] = lines.pop() ?? "";
        for (const line of lines) logs.push(line.replace(/\r$/, ""));
        if (retained.length !== text.length) truncated = true;
      };
      const finish = async (result: Omit<FabricSandboxResult, "logs">, unawaitedHostCalls = false): Promise<void> => {
        if (settled) return;
        if (result.terminationReason === "completed" && executionDeadline.reached) {
          hostAbort.abort(executionDeadline.reason);
          result = executionDeadline.timeoutResult([]);
        }
        settled = true;
        pendingReceipts.clear();
        executionDeadline.clear();
        options.signal?.removeEventListener("abort", abort);
        const interrupted = result.terminationReason !== "completed" || hostAbort.signal.aborted || hostTasks.size > 0 || unawaitedHostCalls;
        if (!hostAbort.signal.aborted) hostAbort.abort(new Error(result.error ?? "CPython execution ended"));
        preserveCancellationOutcome(result, hostAbort.signal, interrupted);
        channel?.destroy();
        ipc?.server.close();
        if (child.pid && process.platform !== "win32") {
          try { process.kill(-child.pid, "SIGKILL"); } catch { /* The process group may already have exited. */ }
        }
        child.kill("SIGKILL");
        // TCP results and Windows stderr pipes arrive independently. "exit" is
        // not a drain barrier: keep the readers until "close" (all stdio closed),
        // or the existing bounded reap grace if a descendant holds a pipe open.
        // IPC and host authority have already ended; this waits only for logs/cwd.
        if (!childClosed) {
          await new Promise<void>((resolveClose) => {
            const done = (): void => {
              clearTimeout(timer);
              child.removeListener("close", done);
              resolveClose();
            };
            const timer = setTimeout(done, 250);
            timer.unref?.();
            child.once("close", done);
          });
        }
        child.stdout?.destroy();
        child.stderr?.destroy();
        for (let index = 0; index < decoders.length; index++) appendLog(index, decoders[index]!.end());
        logsFinalized = true;
        for (const text of partialLogs) if (text) logs.push(text);
        if (truncated) logs.push("[Pi Fabric log output truncated]");
        if (result.terminationReason === "completed" && executionDeadline.reached) {
          result = preserveCancellationOutcome(executionDeadline.timeoutResult([]), hostAbort.signal, true);
        }
        resolve({ ...result, logs });
      };
      const abort = (): void => {
        const reason = mainExecutionCeilingAbortReason(options.signal);
        if (reason && !hostAbort.signal.aborted) hostAbort.abort(reason);
        void finish({ value: undefined, terminationReason: "aborted", error: "Execution cancelled" });
      };
      const fail = (message: string): void => void finish({ value: undefined, terminationReason: "runtime_error", error: message });
      // A child that dies at startup (bwrap without user namespaces) resets its
      // pipes before "close" reports its exit status and stderr. Let that
      // diagnosis settle the run instead of racing it with a bare EPIPE or
      // ECONNRESET. The bridge itself ends at once: host calls are aborted and
      // no further guest frame is admitted (finishing). Only the diagnosis
      // waits, and for at most PIPE_DIAGNOSIS_MS, since a descendant that keeps
      // stdout/stderr open can hold back "close".
      let pipeError: string | undefined;
      const failPipe = (message: string): void => {
        if (settled || finishing) return;
        pipeError = message;
        finishing = true;
        pendingReceipts.clear();
        hostAbort.abort(new Error(message));
        if (child.pid && process.platform !== "win32") {
          try { process.kill(-child.pid, "SIGKILL"); } catch { /* The process group may already have exited. */ }
        }
        child.kill("SIGKILL");
        setTimeout(() => fail(`${message}; CPython process did not report its exit`), PIPE_DIAGNOSIS_MS).unref?.();
      };
      const expireDeadline = (): void => {
        if (settled) return;
        // A recorded pipe failure is the real cause; the deadline only ends its diagnosis wait.
        if (pipeError) { fail(`${pipeError}; CPython process did not report its exit`); return; }
        hostAbort.abort(executionDeadline.reason);
        void finish(executionDeadline.timeoutResult([]));
      };
      const scheduleDeadline = (): void => executionDeadline.scheduleDeadline(expireDeadline, true);
      const send = (message: any, delivered?: () => void): void => {
        // A terminal guest result closes its reply channel while issued host
        // work may still be settling. Its late replies are no longer consumed.
        if (settled || finishing || !channel || channel.destroyed) return;
        if (executionDeadline.reached) { expireDeadline(); return; }
        try {
          if (delivered) {
            const responseId = ++nextResponseId;
            message = { ...message, responseId };
            pendingReceipts.set(responseId, { id: message.id, commit: delivered });
          }
          const frame = JSON.stringify(message) + "\n";
          const bytes = Buffer.byteLength(frame);
          if (bytes > MAX_FRAME_BYTES || channel.writableLength + bytes > MAX_FRAME_BYTES * 2) {
            fail("CPython IPC frame or write buffer exceeds its 16 MiB frame limit");
            return;
          }
          if (executionDeadline.reached) { expireDeadline(); return; }
          channel.write(frame, (error) => {
            if (settled || finishing) return;
            if (error) { failPipe(`CPython IPC failed: ${error.message}`); return; }
            // A native write is not guest admission. Only a correlated ack
            // from the receiver may commit the pending consumption receipt.
            if (!channel || channel.destroyed || !channel.writable) return;
            if (executionDeadline.reached) { expireDeadline(); return; }
          });
        } catch (error) { fail(`CPython IPC serialization failed: ${errorText(error)}`); }
      };
      const handleMessage = (message: unknown): void => {
        if (settled || finishing) return;
        if (!record(message)) { fail("Invalid CPython IPC message"); return; }
        if (executionDeadline.reached) { expireDeadline(); return; }
        if (message.type === "response_ack") {
          if (!channel || channel.destroyed || !channel.writable) return;
          const receipt = pendingReceipts.get(message.responseId as number);
          if (!receipt || receipt.id !== message.id) return;
          pendingReceipts.delete(message.responseId as number);
          receipt.commit();
          return;
        }
        if (message.type === "result") {
          const result = message.result;
          if (!record(result) || !["completed", "runtime_error"].includes(String(result.terminationReason)) ||
              (result.error !== undefined && typeof result.error !== "string")) {
            fail("Invalid CPython terminal result"); return;
          }
          finishing = true;
          const unawaitedHostCalls = hostTasks.size > 0;
          if (result.terminationReason !== "completed") hostAbort.abort(new Error(String(result.error ?? "Python guest failed")));
          void (async () => {
            const done = await settleWithin(hostTasks, HOST_SETTLE_MS);
            if (!done) {
              hostAbort.abort(new Error("Fabric guest execution ended before its host calls settled"));
              await settleWithin(hostTasks, HOST_SETTLE_MS);
            }
            // stdout/stderr are separate channels; allow their ready data to drain.
            await new Promise<void>((done) => setImmediate(done));
            finish({
              value: result.value,
              terminationReason: result.terminationReason as "completed" | "runtime_error",
              ...(typeof result.error === "string" ? { error: result.error } : {}),
            }, unawaitedHostCalls);
          })();
          return;
        }
        if (message.type !== "call" || !Number.isSafeInteger(message.id) || (message.id as number) < 1 ||
            typeof message.ref !== "string" || !message.ref || message.ref.length > 512 || !record(message.args) ||
            callIds.has(message.id as number) || hostTasks.size >= 256) {
          fail("Invalid or excessive CPython host call"); return;
        }
        const id = message.id as number;
        const ref = message.ref;
        const args = message.args;
        callIds.add(id);
        try {
          const floor = options.minimumTimeoutMsForHostCall?.(ref, args);
          if (executionDeadline.extend(floor)) scheduleDeadline();
        } catch (error) { fail(`CPython deadline policy failed: ${errorText(error)}`); return; }
        if (executionDeadline.reached) { expireDeadline(); return; }
        const task = runAbortable(hostAbort.signal, () => hostCall(ref, args, hostAbort.signal)).then(
          (value) => send({ type: "response", id, ok: true, value }, () => options.onHostResultDelivered?.(args)),
          (error) => send({ type: "response", id, ok: false, error: errorText(error), ...(isPiShellRef(ref) ? { bashExit: piBashExitMetadata(error) } : {}) }),
        ).finally(() => { hostTasks.delete(task); callIds.delete(id); });
        hostTasks.add(task);
      };
      const onData = (chunk: Buffer): void => {
        if (settled || finishing) return;
        buffer = Buffer.concat([buffer, chunk]);
        if (expectedToken !== undefined) {
          const handshake = buffer.indexOf(10);
          if (handshake === -1) {
            if (buffer.length > 4096) fail("Invalid CPython IPC handshake");
            return;
          }
          let verified = false;
          try {
            const hello = JSON.parse(buffer.subarray(0, handshake).toString("utf8"));
            verified = record(hello) && hello.type === "hello" && hello.token === expectedToken;
          } catch { /* Fail closed below. */ }
          if (!verified) { fail("Invalid CPython IPC handshake"); return; }
          buffer = buffer.subarray(handshake + 1);
          expectedToken = undefined;
          send({ type: "execute", code, strings: options.strings ?? {}, memoryLimitBytes: options.memoryLimitBytes });
          if (settled || finishing) return;
        }
        let newline: number;
        while ((newline = buffer.indexOf(10)) !== -1) {
          if (newline > MAX_FRAME_BYTES) { fail("CPython IPC frame exceeds 16 MiB"); return; }
          const frame = buffer.subarray(0, newline).toString("utf8");
          buffer = buffer.subarray(newline + 1);
          try { handleMessage(JSON.parse(frame)); }
          catch (error) { fail(`Invalid CPython IPC: ${errorText(error)}`); return; }
          if (settled || finishing) return;
        }
        if (buffer.length > MAX_FRAME_BYTES) fail("CPython IPC frame exceeds 16 MiB");
      };
      const attach = (socket: Duplex | net.Socket): void => {
        channel = socket;
        if (socket instanceof net.Socket) socket.setNoDelay(true);
        socket.on("data", onData);
        socket.on("error", (error) => failPipe(`CPython IPC failed: ${error.message}`));
      };
      child.stdout?.on("data", (chunk: Buffer) => { if (!logsFinalized) appendLog(0, decoders[0]!.write(chunk)); });
      child.stderr?.on("data", (chunk: Buffer) => { if (!logsFinalized) appendLog(1, decoders[1]!.write(chunk)); });
      child.on("error", (error) => fail(`CPython process failed: ${error.message}${this.enforce ? "; OS sandbox is required (no native fallback)" : ""}`));
      child.on("close", (exitCode, signal) => {
        if (settled || (finishing && !pipeError)) return;
        const diagnostics = [...logs, ...partialLogs].join("\n").slice(-4000);
        fail(`${pipeError ? `${pipeError}; ` : ""}CPython ${this.enforce ? "sandbox " : ""}process exited before returning a result (${signal ?? exitCode}).${this.enforce ? " Verify OS sandbox availability/user namespaces; no unsandboxed fallback is permitted." : ""}${diagnostics ? `\n${diagnostics}` : ""}`);
      });
      options.signal?.addEventListener("abort", abort, { once: true });
      if (options.signal?.aborted) { abort(); return; }
      if (command.seccomp) {
        const filterPipe = child.stdio[4] as Duplex;
        filterPipe.on("error", (error) => failPipe(`CPython sandbox filter failed: ${error.message}`));
        filterPipe.end(command.seccomp);
      }
      scheduleDeadline();
      if (ipc) {
        // Windows: the child dials the loopback listener and proves the token first.
        ipc.server.on("connection", (socket) => {
          if (channel) { socket.destroy(); return; }
          ipc.server.close();
          attach(socket);
        });
      } else {
        attach(child.stdio[3] as Duplex);
        send({ type: "execute", code, strings: options.strings ?? {}, memoryLimitBytes: options.memoryLimitBytes });
      }
    });
  }
}
