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
  readonly state: "committed" | "unknown";
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
}

export interface FabricSandboxOptions {
  timeoutMs: number;
  memoryLimitBytes: number;
  /** Optional uninterrupted guest CPU limit. Await host work/timers to yield. */
  maxCpuSliceMs?: number;
  maxPendingTimers?: number;
  maxLogChars?: number;
  strings?: Record<string, string>;
  tokenBudget?: number;
  signal?: AbortSignal;
  cwd?: string;
  minimumTimeoutMsForHostCall?(
    ref: string,
    args: Record<string, unknown>,
  ): number | undefined;
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
