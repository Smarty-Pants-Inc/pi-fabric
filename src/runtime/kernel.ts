import type { ExecutionDeadline } from "./execution-deadline.js";

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
