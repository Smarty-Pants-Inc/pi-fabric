import type { ExecutionDeadline } from "./execution-deadline.js";
import type { HumanWaitDeadlinePause, PausableDeadlineClock } from "./deadline-pause.js";

/**
 * Adapt upstream's human-wait clock to the fork's shared deadline. The shared
 * object must reflect the pause too: registry/effect boundaries read it directly.
 * Only the executable budget pauses; the host-issued absolute ceiling never does.
 * Keep this bridge here (the auto-merged kernel contract), with type-only imports.
 */
export const humanWaitDeadlineClock = (
  getDeadline: () => ExecutionDeadline,
  options: Pick<FabricSandboxOptions, "maximumDeadlineAt" | "maximumDeadlineReason">,
  schedule: () => void,
  expire: () => void,
): PausableDeadlineClock => {
  let resume: ((remainingMs: number) => void) | undefined;
  return {
    remainingMs: () => getDeadline().at - Date.now(),
    suspend: () => {
      const deadline = getDeadline();
      const maximum = options.maximumDeadlineAt ?? Infinity;
      const expired = deadline.reached;
      const expiredReason = expired ? deadline.reason : undefined;
      const prototype = Object.getPrototypeOf(deadline);
      const read = (name: "at" | "reached" | "reason") =>
        Object.getOwnPropertyDescriptor(prototype, name)!.get!.bind(deadline);
      const readAt = read("at");
      const readReached = read("reached");
      const readReason = read("reason");
      const names = ["at", "reached", "reason", "clear"] as const;
      const previous = new Map(names.map(name => [name, Object.getOwnPropertyDescriptor(deadline, name)]));
      const clear = deadline.clear.bind(deadline);
      const extend = deadline.extend.bind(deadline);
      let timer: ReturnType<typeof setTimeout> | undefined;
      let closed = false;
      let paused = true;
      let ceilingReason: Error | undefined;
      const ceilingReached = (): boolean => {
        if (ceilingReason) return true;
        if (Date.now() < maximum) return false;
        ceilingReason ??= options.maximumDeadlineReason ?? new Error("Execution reached the host deadline");
        return true;
      };
      clear();
      Object.defineProperties(deadline, {
        at: { configurable: true, get: () => paused ? Math.min(expired ? readAt() : Infinity, maximum) : readAt() },
        reached: { configurable: true, get: () => paused ? expired || ceilingReached() : readReached() },
        reason: { configurable: true, get: () => expiredReason ?? (ceilingReached() ? ceilingReason! : readReason()) },
        clear: { configurable: true, value: () => {
          closed = true;
          clearTimeout(timer);
          clear();
        } },
      });
      const armCeiling = (): void => {
        if (closed || !Number.isFinite(maximum)) return;
        timer = setTimeout(() => {
          if (expired || ceilingReached()) expire();
          else armCeiling(); // An early timer never proves expiry.
        }, Math.min(2_147_483_647, Math.max(0, maximum - Date.now())));
        timer.unref?.();
      };
      if (expired) expire();
      else armCeiling();
      resume = (remainingMs) => {
        clearTimeout(timer);
        // extend() uses the paused reached getter. It can restore executable
        // budget after a long wait, but cannot revive an expired hard ceiling.
        if (!closed && !expired && !ceilingReached()) extend(remainingMs);
        paused = false;
        for (const name of names) {
          const descriptor = previous.get(name);
          if (descriptor) Object.defineProperty(deadline, name, descriptor);
          else Reflect.deleteProperty(deadline, name);
        }
        // Preserve the opaque ceiling cause through cancellation/result packing,
        // even when the executable deadline had not originally been clamped.
        if (ceilingReason && !expired) Object.defineProperties(deadline, {
          at: { configurable: true, get: () => maximum },
          reached: { configurable: true, get: () => true },
          reason: { configurable: true, get: () => ceilingReason! },
        });
        if (!closed) schedule();
      };
    },
    resume: (remainingMs) => {
      const restore = resume;
      resume = undefined;
      restore?.(remainingMs);
    },
  };
};

// Language-neutral execution contract shared by all Fabric kernel backends.
export type FabricKernel = "typescript" | "python";

export type FabricSandboxTerminationReason =
  | "completed"
  | "runtime_error"
  | "timed_out"
  | "aborted";

/** Immutable resident fence facts, never reconstructed from guest-controlled prose. */
export interface FabricResidentOutcomeReceipt {
  readonly requestId: string;
  readonly state: "committed" | "unknown" | "expired";
  /** Expiry is replay classification, not proof that the mutation was rejected. */
  readonly expired?: true;
  readonly operation: string;
  readonly entityKind: "agent" | "actor";
  readonly id?: string;
  readonly ownerHostId?: string;
}

export interface FabricSandboxResult {
  value: unknown;
  logs: string[];
  terminationReason: FabricSandboxTerminationReason;
  error?: string;
  /** Host-side reconciliation data, independent of verbose guest error prose. */
  residentOutcomes?: FabricResidentOutcomeReceipt[];
  /** Host-only deadline cause. Guest error text is never a ceiling identity. */
  deadlineReason?: Error;
}

export interface FabricSandboxOptions {
  timeoutMs: number;
  /** Host-only shared clamp record, including runtime host-call floor extensions. */
  executionDeadline?: ExecutionDeadline;
  /** Host-owned absolute ceiling; host-call floors cannot extend it. */
  maximumDeadlineAt?: number;
  /** Opaque host-issued cause, used only when this runtime is clamped to that ceiling. */
  maximumDeadlineReason?: Error;
  memoryLimitBytes: number;
  /** Optional uninterrupted guest CPU limit. Await host work/timers to yield. */
  maxCpuSliceMs?: number;
  maxPendingTimers?: number;
  maxLogChars?: number;
  strings?: Record<string, string>;
  tokenBudget?: number;
  /** Host-owned initial shared workflow spending; subsequent snapshots use the internal bridge. */
  workflowSpentTokens?: number;
  signal?: AbortSignal;
  cwd?: string;
  /** Host-only receipt callback after serialization and final deadline admission. */
  onHostResultDelivered?(args: Record<string, unknown>): void;
  minimumTimeoutMsForHostCall?(
    ref: string,
    args: Record<string, unknown>,
  ): number | undefined;
  /** True for a host call that waits for a person (executor.humanWaitRefs).
   * The program deadline is paused while any such call is in flight. */
  isHumanWaitHostCall?(ref: string, args: Record<string, unknown>): boolean;
  /** Host-only registration so nested human waits can pause this enclosing runtime. */
  registerHumanWaitPause?(pause: HumanWaitDeadlinePause): void;
  /** Declared core override fields must not be consumed as built-in aliases. */
  piToolCanonicalFields?: Record<string, string[]>;
  /** False in orchestration-only mode, where the guest has no usable `pi`; guides runtime hints. */
  piTools?: boolean;
  transpiledCode?: string;
  transpiledSourceMap?: string;
}

export type FabricHostCall = (
  ref: string,
  args: Record<string, unknown>,
  signal: AbortSignal,
) => Promise<unknown>;

export interface FabricKernelRuntime {
  execute(
    code: string,
    hostCall: FabricHostCall,
    options: FabricSandboxOptions,
  ): Promise<FabricSandboxResult>;
}
