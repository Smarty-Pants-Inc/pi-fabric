import { AsyncLocalStorage } from "node:async_hooks";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
import { performance } from "node:perf_hooks";

export type NativeCleanupKind = "product-custody-protection" | "test-only-acl-readback" | "diagnostic-probe";
export type NativeCleanupEvent = {
  type: "native-cleanup-diagnostic";
  event: "phase-start" | "phase-end" | "native-start" | "native-end";
  fixture: string;
  runId: string;
  phase: string;
  kind?: NativeCleanupKind;
  sequence: number;
  at: string;
  elapsedMs?: number;
  outcome?: "ok" | "error";
  errorName?: string;
  errorCode?: string;
  status?: number | null;
};
export type NativeCleanupAttemptOptions = {
  fixture: string;
  runId: string;
  emit?: (event: NativeCleanupEvent) => void;
};
export type NativeCleanupPhaseToken = { readonly attempt: NativeCleanupAttempt; readonly phase: string; readonly startedAt: number };
export type NativeCleanupAttempt = {
  run<T>(phase: string, kind: NativeCleanupKind, operation: () => T): T;
  beginPhase(phase: string): NativeCleanupPhaseToken;
  endPhase(token: NativeCleanupPhaseToken, outcome: "ok" | "error"): void;
  measure<T>(phase: string, operation: () => T | Promise<T>): Promise<T>;
  close(): void;
};

type NativeCleanupContext = NativeCleanupAttemptOptions & { phase: string; kind: NativeCleanupKind };
const current = new AsyncLocalStorage<NativeCleanupContext>();
const attempts = new Map<string, NativeCleanupAttempt>();
let observerUsers = 0;
let originalExecFileSync: typeof childProcess.execFileSync | undefined;
let sequence = 0;

const safeEmit = (context: NativeCleanupContext, event: Omit<NativeCleanupEvent, "type" | "fixture" | "runId" | "phase" | "at" | "sequence">): void => {
  const row: NativeCleanupEvent = {
    type: "native-cleanup-diagnostic", fixture: context.fixture, runId: context.runId,
    phase: context.phase, kind: context.kind, sequence: ++sequence, at: new Date().toISOString(), ...event,
  };
  try { (context.emit ?? (value => console.info(JSON.stringify(value))))(row); } catch { /* Diagnostics never alter custody behavior. */ }
};

const diagnosticError = (error: unknown): Pick<NativeCleanupEvent, "errorName" | "errorCode" | "status"> => {
  if (!error || typeof error !== "object") return { errorName: typeof error };
  const value = error as { name?: unknown; code?: unknown; status?: unknown };
  return {
    ...(typeof value.name === "string" ? { errorName: value.name } : {}),
    ...(typeof value.code === "string" ? { errorCode: value.code } : {}),
    ...(typeof value.status === "number" || value.status === null ? { status: value.status } : {}),
  };
};

const observedExecFileSync = function (this: unknown, ...args: Parameters<typeof childProcess.execFileSync>): ReturnType<typeof childProcess.execFileSync> {
  const original = originalExecFileSync;
  if (!original) throw new Error("Native cleanup diagnostic observer is not installed");
  const context = current.getStore();
  if (!context) return Reflect.apply(original, this, args) as ReturnType<typeof childProcess.execFileSync>;
  const startedAt = performance.now();
  safeEmit(context, { event: "native-start" });
  try {
    const result = Reflect.apply(original, this, args) as ReturnType<typeof childProcess.execFileSync>;
    safeEmit(context, { event: "native-end", elapsedMs: Number((performance.now() - startedAt).toFixed(1)), outcome: "ok" });
    return result;
  } catch (error) {
    safeEmit(context, { event: "native-end", elapsedMs: Number((performance.now() - startedAt).toFixed(1)), outcome: "error", ...diagnosticError(error) });
    throw error;
  }
} as typeof childProcess.execFileSync;

const installObserver = (): void => {
  if (observerUsers++ !== 0) return;
  originalExecFileSync = childProcess.execFileSync;
  childProcess.execFileSync = observedExecFileSync;
  syncBuiltinESMExports();
};
const removeObserver = (): void => {
  if (--observerUsers !== 0) return;
  if (childProcess.execFileSync === observedExecFileSync && originalExecFileSync) {
    childProcess.execFileSync = originalExecFileSync;
    syncBuiltinESMExports();
  }
  originalExecFileSync = undefined;
};

/** Register one attempt by its explicit run ID. Independent attempts never share ambient attribution. */
export const createNativeCleanupAttempt = (options: NativeCleanupAttemptOptions): NativeCleanupAttempt => {
  if (attempts.has(options.runId)) throw new Error(`Native cleanup diagnostics already registered for ${options.runId}`);
  let closed = false;
  const attempt: NativeCleanupAttempt = {
    run<T>(phase: string, kind: NativeCleanupKind, operation: () => T): T {
      if (closed) return operation();
      return current.run({ ...options, phase, kind }, operation);
    },
    beginPhase(phase: string): NativeCleanupPhaseToken {
      const token = { attempt, phase, startedAt: performance.now() };
      const context: NativeCleanupContext = { ...options, phase, kind: "diagnostic-probe" };
      safeEmit(context, { event: "phase-start" });
      return token;
    },
    endPhase(token: NativeCleanupPhaseToken, outcome: "ok" | "error"): void {
      if (token.attempt !== attempt) return;
      const context: NativeCleanupContext = { ...options, phase: token.phase, kind: "diagnostic-probe" };
      safeEmit(context, { event: "phase-end",
        elapsedMs: Number((performance.now() - token.startedAt).toFixed(1)), outcome });
    },
    async measure<T>(phase: string, operation: () => T | Promise<T>): Promise<T> {
      const token = attempt.beginPhase(phase);
      try {
        const value = await operation();
        attempt.endPhase(token, "ok");
        return value;
      } catch (error) {
        attempt.endPhase(token, "error");
        throw error;
      }
    },
    close(): void {
      if (closed) return;
      closed = true;
      if (attempts.get(options.runId) === attempt) attempts.delete(options.runId);
      removeObserver();
    },
  };
  attempts.set(options.runId, attempt);
  installObserver();
  return attempt;
};

const withAttempt = <T>(runId: string, phase: string, kind: NativeCleanupKind, operation: () => T): T => {
  const attempt = attempts.get(runId);
  return attempt ? attempt.run(phase, kind, operation) : operation();
};

/** Wrap the real synchronous custody root operation only for a registered target attempt. */
export const withProductCleanupProtection = <T>(runId: string, phase: string, operation: () => T): T =>
  withAttempt(runId, phase, "product-custody-protection", operation);

/** Distinguish the fixture's independent ACL readback from product DACL protection. */
export const withTestOnlyAclReadback = <T>(runId: string, phase: string, operation: () => T): T =>
  withAttempt(runId, phase, "test-only-acl-readback", operation);

/** Public only for the bounded generic pass-through/overlap probe. */
export const withDiagnosticProbe = <T>(runId: string, phase: string, operation: () => T): T =>
  withAttempt(runId, phase, "diagnostic-probe", operation);
