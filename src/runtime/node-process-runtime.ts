import { ExecutionDeadline } from "./execution-deadline.js";
import { spawn } from "node:child_process";
import { mainExecutionCeilingAbortReason, preserveCancellationOutcome, runAbortable, settleWithin, shareCancellationEffects } from "../async-settlement.js";
import { piBashExitMetadata } from "../core/pi-bash-error.js";
import { isPiShellRef } from "../core/pi-tools.js";
import { guestSetupSource } from "./quickjs-runtime.js";
import type { FabricHostCall, FabricSandboxOptions, FabricSandboxResult } from "./kernel.js";
import { NODE_PROCESS_CHILD_SOURCE } from "./node-process-child-source.js";
import { createGuestStackMap, remapGuestErrorText } from "./guest-stack-map.js";
import { transpileFabricCodeWithSourceMap } from "./type-checker.js";
import {
  resolveScriptRuntime,
  resolveScriptRuntimeSync,
} from "../agents/transports/process-utils.js";

interface ChildCallMessage {
  type: "call";
  id: number;
  ref: string;
  args: Record<string, unknown>;
}

interface ChildResultMessage {
  type: "result";
  result: FabricSandboxResult;
}

interface ChildResponseAckMessage {
  type: "response_ack";
  id: number;
  responseId: number;
}

type ChildMessage = ChildCallMessage | ChildResultMessage | ChildResponseAckMessage;

const HOST_TASK_SETTLE_GRACE_MS = 250;

export class NodeProcessRuntime {
  readonly #interpreter: "node" | "bun";

  constructor(interpreter: "node" | "bun" = "node") {
    this.#interpreter = interpreter;
  }

  async execute(
    code: string,
    hostCall: FabricHostCall,
    options: FabricSandboxOptions,
  ): Promise<FabricSandboxResult> {
    if (options.signal?.aborted) {
      return {
        value: undefined,
        logs: [],
        terminationReason: "aborted",
        error: "Execution cancelled",
      };
    }
    if (!Number.isSafeInteger(options.memoryLimitBytes) || options.memoryLimitBytes < 1) {
      return {
        value: undefined,
        logs: [],
        terminationReason: "runtime_error",
        error: "Process memory limit must be a positive safe integer",
      };
    }
    if (!Number.isFinite(options.timeoutMs) || options.timeoutMs < 1) {
      return {
        value: undefined,
        logs: [],
        terminationReason: "runtime_error",
        error: "Process timeout must be positive",
      };
    }

    // Bun evaluates --eval input as ESM and ignores V8 heap flags, so the
    // Bun child gets the guest source bare; Node needs the module + heap flags.
    // Bun resolution stays async: Pi may legitimately run under Node while bun
    // is only available on PATH, and the sync variant does no PATH lookup.
    const runtimeOptions = this.#interpreter === "bun" ? { requireBun: true } : { requireNode: true };
    const interpreterPath = this.#interpreter === "bun"
      ? await resolveScriptRuntime(runtimeOptions)
      : resolveScriptRuntimeSync(runtimeOptions);
    // Bun resolution is async: recheck before acquiring any child process.
    if (options.signal?.aborted) {
      return {
        value: undefined,
        logs: [],
        terminationReason: "aborted",
        error: "Execution cancelled",
      };
    }
    // Preparation still consumes the execution budget even though it precedes spawn.
    const startedAt = Date.now();
    // Complete fallible guest preparation before spawn. There must be no
    // ownerless child if transpilation, stack mapping or setup throws.
    const guestBundle = options.transpiledCode === undefined
      ? transpileFabricCodeWithSourceMap(code)
      : { code: options.transpiledCode, sourceMap: options.transpiledSourceMap };
    const guestStackMap = createGuestStackMap(guestBundle.sourceMap);
    const guestLineCount = guestBundle.code.split("\n").length;
    const setup = guestSetupSource(options.piToolCanonicalFields, options.piTools !== false);
    const child = spawn(
      interpreterPath,
      this.#interpreter === "bun"
        ? ["--eval", NODE_PROCESS_CHILD_SOURCE]
        : [
            // Guest import() through importModuleDynamically needs the vm
            // modules flag; Bun's vm ignores the option (see __fabricImport).
            "--experimental-vm-modules",
            `--max-old-space-size=${Math.max(16, Math.floor(options.memoryLimitBytes / (1024 * 1024)))}`,
            "--input-type=module",
            "--eval",
            NODE_PROCESS_CHILD_SOURCE,
          ],
      { stdio: ["ignore", "ignore", "ignore", "ipc"] },
    );
    const hostAbortController = new AbortController();
    shareCancellationEffects(hostAbortController.signal, options.signal);
    const executionDeadline = options.executionDeadline ?? new ExecutionDeadline(options);
    let abortHandler: (() => void) | undefined;
    let settled = false;
    let finishing = false;
    const hostTasks = new Set<Promise<void>>();
    let nextResponseId = 0;
    const pendingReceipts = new Map<number, { id: number; commit: () => void }>();

    return new Promise<FabricSandboxResult>((resolve) => {
      const finish = (result: FabricSandboxResult, unawaitedHostCalls = false): void => {
        if (settled) return;
        if (result.terminationReason === "completed" && executionDeadline.reached) {
          hostAbortController.abort(executionDeadline.reason);
          result = executionDeadline.timeoutResult([]);
        }
        settled = true;
        pendingReceipts.clear();
        executionDeadline.clear();
        if (abortHandler) options.signal?.removeEventListener("abort", abortHandler);
        if (!hostAbortController.signal.aborted && (result.terminationReason !== "completed" || hostTasks.size > 0 || unawaitedHostCalls)) {
          hostAbortController.abort(new Error(result.error ?? "Process execution stopped"));
        }
        preserveCancellationOutcome(result, hostAbortController.signal);
        child.removeAllListeners();
        if (child.connected) child.disconnect();
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
        resolve(result);
      };
      const expireDeadline = (): void => {
        if (settled) return;
        hostAbortController.abort(executionDeadline.reason);
        finish(executionDeadline.timeoutResult([]));
      };
      const scheduleDeadline = (): void => executionDeadline.scheduleDeadline(expireDeadline, true);
      const send = (message: any, delivered?: () => void): void => {
        if (settled || finishing || !child.connected) return;
        if (executionDeadline.reached) { expireDeadline(); return; }
        const failSend = (error: Error): void => finish({
          value: undefined, logs: [], terminationReason: "runtime_error",
          error: `Process IPC failed: ${error.message}`,
        });
        try {
          if (delivered) {
            const responseId = ++nextResponseId;
            message = { ...message, responseId };
            pendingReceipts.set(responseId, { id: message.id, commit: delivered });
          }
          child.send(message, (error) => {
            if (settled || finishing) return;
            if (error) { failSend(error); return; }
            if (!child.connected) return;
            if (executionDeadline.reached) { expireDeadline(); return; }
            // Write completion is not admission; wait for the guest ack.
          });
        } catch (error) { failSend(error instanceof Error ? error : new Error(String(error))); }
      };
      const extendDeadline = (ref: string, args: Record<string, unknown>): void => {
        const requested = options.minimumTimeoutMsForHostCall?.(ref, args);
        if (executionDeadline.extend(requested)) scheduleDeadline();
      };

      abortHandler = () => {
        // Preserve only the Main watchdog reason for host observers (e.g. a local actor ASK).
        const reason = mainExecutionCeilingAbortReason(options.signal);
        if (reason && !hostAbortController.signal.aborted) hostAbortController.abort(reason);
        finish({
          value: undefined,
          logs: [],
          terminationReason: "aborted",
          error: "Execution cancelled",
        });
      };
      options.signal?.addEventListener("abort", abortHandler, { once: true });

      child.on("message", (raw: unknown) => {
        if (settled || finishing || typeof raw !== "object" || raw === null) return;
        const message = raw as ChildMessage;
        if (executionDeadline.reached) { expireDeadline(); return; }
        if (message.type === "response_ack") {
          if (!child.connected) return;
          const receipt = pendingReceipts.get(message.responseId);
          if (!receipt || receipt.id !== message.id) return;
          pendingReceipts.delete(message.responseId);
          receipt.commit();
          return;
        }
        if (message.type === "result") {
          finishing = true;
          const unawaitedHostCalls = hostTasks.size > 0;
          executionDeadline.clear();
          if (message.result.terminationReason !== "completed" && !hostAbortController.signal.aborted) {
            hostAbortController.abort(new Error(message.result.error ?? "Process execution stopped"));
          }
          void (async () => {
            const completed = await settleWithin(hostTasks, HOST_TASK_SETTLE_GRACE_MS);
            if (!completed && !hostAbortController.signal.aborted) {
              hostAbortController.abort(
                new Error("Fabric guest execution ended before its host calls settled"),
              );
              await settleWithin(hostTasks, HOST_TASK_SETTLE_GRACE_MS);
            }
            finish(
              message.result.error === undefined
                ? message.result
                : {
                    ...message.result,
                    error: remapGuestErrorText(message.result.error, guestStackMap, guestLineCount),
                  },
              unawaitedHostCalls,
            );
          })();
          return;
        }
        if (message.type !== "call") return;
        extendDeadline(message.ref, message.args);
        if (executionDeadline.reached) { expireDeadline(); return; }
        if (message.ref === "fabric.$timer") {
          const ms = Math.max(0, Number(message.args?.ms ?? 0));
          let timer: NodeJS.Timeout | undefined;
          const timerTask = new Promise<void>((resolveTask) => {
            timer = setTimeout(() => {
              if (!settled && !finishing && executionDeadline.reached) expireDeadline();
              if (!settled && !finishing) {
                send({ type: "response", id: message.id, ok: true, value: undefined });
              }
              resolveTask();
            }, ms);
            timer!.unref?.();
          });
          hostTasks.add(timerTask);
          void timerTask.finally(() => {
            hostTasks.delete(timerTask);
            if (timer) clearTimeout(timer);
          });
          return;
        }
        const task = runAbortable(hostAbortController.signal, () =>
          hostCall(message.ref, message.args, hostAbortController.signal),
        ).then(
          (value) => {
            if (settled || finishing || !child.connected) return;
            if (executionDeadline.reached) { expireDeadline(); return; }
            // Serialize before admission: getters/toJSON can spend the remaining
            // budget, and must not acknowledge an undelivered observation.
            const response = JSON.parse(JSON.stringify({ type: "response", id: message.id, ok: true, value }));
            if (executionDeadline.reached) { expireDeadline(); return; }
            send(response, () => options.onHostResultDelivered?.(message.args));
          },
        ).catch((error) => {
          if (executionDeadline.reached) { expireDeadline(); return; }
          send({
              type: "response",
              id: message.id,
              ok: false,
              error: error instanceof Error ? error.message : String(error),
              bashExit: isPiShellRef(message.ref) ? piBashExitMetadata(error) : undefined,
            });
        });
        hostTasks.add(task);
        void task.finally(() => hostTasks.delete(task));
      });
      child.once("error", (error) => {
        finish({
          value: undefined,
          logs: [],
          terminationReason: "runtime_error",
          error: `Process executor failed: ${error.message}`,
        });
      });
      child.once("exit", (exitCode, signal) => {
        if (settled) return;
        const detail = signal ? `signal ${signal}` : `exit code ${exitCode ?? "unknown"}`;
        finish({
          value: undefined,
          logs: [],
          terminationReason: "runtime_error",
          error: `Process executor exited before returning a result (${detail}); it may have exceeded its memory limit`,
        });
      });

      // A synchronous startup hook can also abort before the listener is installed.
      // From this point a child exists, so cancellation must use owned teardown.
      if (options.signal?.aborted) { abortHandler(); return; }
      scheduleDeadline();
      send({
        type: "execute",
        setup,
        code: guestBundle.code,
        strings: options.strings ?? {},
        tokenBudget: options.tokenBudget,
        maxLogChars: options.maxLogChars ?? 100_000,
      });
    });
  }
}

export class BunProcessRuntime extends NodeProcessRuntime {
  constructor() {
    super("bun");
  }
}
