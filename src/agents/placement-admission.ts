import { spawn } from "node:child_process";
import os from "node:os";
import {
  PLACEMENT_DEADLINE_MS,
  PLACEMENT_MAX_BYTES,
  decodePlacementAcknowledgement,
  decodePlacementAnswer,
  isPlacementPlace,
  isPlacementQueue,
  placementInvocation,
  placementRegistrationRequest,
  placementReleaseRequest,
  snapshotPlacementRequest,
  type PlacementAcknowledgementPlaceholder,
  type PlacementNativeIdentity,
  type PlacementPlace,
  type PlacementQueue,
  type PlacementReleaseProof,
  type PlacementRequest,
  type PlacementWireRequest,
  type PlacementShadowObservation,
} from "./placement-contract.js";
import { normalizePlacementDeciderConfig, type FabricPlacementDeciderConfig } from "./placement-decider-config.js";

export type {
  PlacementRequest, PlacementPlace, PlacementQueue, PlacementShadowObservation,
  PlacementGrant, PlacementNativeIdentity, PlacementReleaseProof,
} from "./placement-contract.js";
export type { FabricPlacementDeciderConfig, PlacementDeciderConfig } from "./placement-decider-config.js";
export type PlacementRefusalReason = "disabled" | "invalid-request" | "unavailable" | "timeout" | "invalid-response" | "mode-mismatch" | "helper-teardown-unconfirmed";
export type PlacementDecisionResult = PlacementPlace | PlacementQueue;
export class PlacementAdmissionError extends Error {
  constructor(readonly reason: PlacementRefusalReason) {
    super(`Placement admission refused: ${reason}`);
    this.name = "PlacementAdmissionError";
  }
}
/** A real writer QUEUE is durable work ownership, never a local permit or launch grant. */
export class PlacementQueueError extends Error {
  constructor(readonly request: PlacementRequest, readonly queue: PlacementQueue) {
    super(`Placement queued: ${queue.reason}; retry_after=${queue.retry_after}`);
    this.name = "PlacementQueueError";
  }
}
export type PlacementLifecycleResult =
  | { kind: "unverified-acknowledgement"; registered: false; released: false; acknowledgement: PlacementAcknowledgementPlaceholder }
  | { kind: "refused"; registered: false; released: false; reason: PlacementRefusalReason };

/**
 * Host-only transport/shape boundary. A PLACE retains the complete outer answer
 * and grant, not an unforgeable native permit. The native protected admission
 * verifier/create fence is a separate mandatory gate; no public spawn inputs.
 */
export interface PlacementDecisionOptions { /** Shared monotonic deadline for context resolution + this operation. */ deadline?: number }
export interface PlacementDecider {
  /** Only live PLACE/QUEUE. All disabled/invalid/unavailable results throw, never repick. */
  decide(request: PlacementRequest, options?: PlacementDecisionOptions): Promise<PlacementPlace | PlacementQueue>;
  /** Distinct, non-executable observer API. Never calls live decide in shadow mode. */
  observe(request: PlacementRequest, options?: PlacementDecisionOptions): Promise<PlacementShadowObservation>;
  register(grant: PlacementPlace, nativeIdentity: PlacementNativeIdentity): Promise<PlacementLifecycleResult>;
  release(launch: PlacementPlace, proof: PlacementReleaseProof): Promise<PlacementLifecycleResult>;
}
interface CliResult {
  stdout: string;
  /** Kept separate, bounded, never parsed as protocol or echoed into error reasons. */
  stderr: string;
  exitCode: number | null;
  failure?: "unavailable" | "timeout" | "invalid-response" | "helper-teardown-unconfirmed";
}
const MAX_DIAGNOSTICS_BYTES = 64 * 1024;
const HELPER_TEARDOWN_TIMEOUT_MS = 2_000;
const confirmHelperTeardown = async (pid: number | undefined, close: Promise<void>, isClosed: () => boolean): Promise<boolean> => {
  const deadline = performance.now() + HELPER_TEARDOWN_TIMEOUT_MS;
  if (!isClosed()) {
    const remaining = deadline - performance.now();
    if (remaining <= 0) return false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const closed = await Promise.race([
      close.then(() => true),
      new Promise<boolean>(resolve => { timer = setTimeout(() => resolve(false), remaining); }),
    ]);
    if (timer) clearTimeout(timer);
    if (!closed) return false;
  }
  if (process.platform === "win32" || pid === undefined) return true;
  while (performance.now() < deadline) {
    try { process.kill(-pid, 0); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return true; }
    const remaining = deadline - performance.now();
    if (remaining <= 0) break;
    await new Promise(resolve => setTimeout(resolve, Math.min(25, remaining)));
  }
  try { process.kill(-pid, 0); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH"; }
};
const warnUnconfirmedHelperTeardown = (pid: number | undefined): void => {
  const pgid = process.platform === "win32" ? "unavailable" : pid ?? "unknown";
  console.warn(`[pi-fabric] placement helper teardown unconfirmed pid=${pid ?? "unknown"} pgid=${pgid}`);
};
const cliEnvironment = (): NodeJS.ProcessEnv => {
  // No inherited NODE_OPTIONS, PYTHONPATH, workspace env or prompt content.
  const env: NodeJS.ProcessEnv = { PATH: process.platform === "win32" ? "" : "/usr/bin:/bin" };
  for (const key of ["HOME", "LANG", "TZ", "SystemRoot", "SYSTEMROOT", "USERPROFILE", "TEMP", "TMP"]) {
    if (process.env[key] !== undefined) env[key] = process.env[key];
  }
  return env;
};

/** The sole CLI runner, shared by decide/feedback/cancel. No probes, RPC loops or retries. */
const runPlacementCli = (
  command: readonly string[], request: PlacementWireRequest, deadline: number,
): Promise<CliResult> => new Promise(resolve => {
  const invocation = placementInvocation(command, request);
  if (performance.now() >= deadline) { resolve({ stdout: "", stderr: "", exitCode: null, failure: "timeout" }); return; }
  const child = spawn(invocation.executable, invocation.args, {
    shell: false, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
    cwd: os.homedir(), env: cliEnvironment(), detached: process.platform !== "win32",
  });
  let settled = false;
  let aborting = false;
  let childClosed = false;
  let resolveChildClose: () => void = () => {};
  const childClose = new Promise<void>(resolve => { resolveChildClose = resolve; });
  let bytes = 0;
  let diagnosticBytes = 0;
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  const diagnostics = (): string => Buffer.concat(stderr).toString("utf8");
  const finish = (result: CliResult): void => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    child.stdin.destroy(); child.stdout.destroy(); child.stderr.destroy();
    resolve(result);
  };
  const abort = (failure: NonNullable<CliResult["failure"]>): void => {
    if (settled || aborting) return;
    aborting = true;
    const pid = child.pid;
    // Only this invocation's process group, including any inherited pipe owners.
    try {
      if (process.platform !== "win32" && pid !== undefined) process.kill(-pid, "SIGKILL");
      else child.kill("SIGKILL");
    } catch { /* The close and process-group checks below decide whether teardown is proven. */ }
    void confirmHelperTeardown(pid, childClose, () => childClosed).then(confirmed => {
      if (!confirmed) {
        warnUnconfirmedHelperTeardown(pid);
        finish({ stdout: "", stderr: diagnostics(), exitCode: null, failure: "helper-teardown-unconfirmed" });
      } else finish({ stdout: "", stderr: diagnostics(), exitCode: null, failure });
    });
  };
  // Exactly the remaining aggregate budget, not a new timeout for each step.
  const timer = setTimeout(() => abort("timeout"), Math.max(0, deadline - performance.now()));
  child.on("error", () => abort("unavailable"));
  child.stdin.on("error", () => abort("unavailable"));
  child.stdout.on("error", () => abort("invalid-response"));
  child.stderr.on("error", () => abort("unavailable"));
  child.stdout.on("data", (chunk: Buffer) => {
    if (settled) return;
    bytes += chunk.length;
    if (bytes > PLACEMENT_MAX_BYTES) { abort("invalid-response"); return; }
    stdout.push(chunk);
  });
  child.stderr.on("data", (chunk: Buffer) => {
    const kept = chunk.subarray(0, Math.max(0, MAX_DIAGNOSTICS_BYTES - diagnosticBytes));
    diagnosticBytes += kept.length;
    if (kept.length) stderr.push(kept);
  });
  child.on("close", (exitCode, signal) => {
    childClosed = true;
    resolveChildClose();
    if (settled || aborting) return;
    if (performance.now() >= deadline) { abort("timeout"); return; }
    if (signal !== null) { finish({ stdout: "", stderr: diagnostics(), exitCode, failure: "unavailable" }); return; }
    try {
      // Reject malformed UTF-8 rather than repairing an authority-bearing ID.
      const text = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(stdout));
      finish({ stdout: text, stderr: diagnostics(), exitCode });
    } catch { finish({ stdout: "", stderr: diagnostics(), exitCode, failure: "invalid-response" }); }
  });
  child.stdin.end(invocation.stdin);
});
const refusal = (reason: PlacementRefusalReason): never => { throw new PlacementAdmissionError(reason); };
const lifecycleRefusal = (reason: PlacementRefusalReason): PlacementLifecycleResult => ({ kind: "refused", registered: false, released: false, reason });

export class CliPlacementDecider implements PlacementDecider {
  private readonly config: FabricPlacementDeciderConfig | undefined;
  constructor(config?: FabricPlacementDeciderConfig) {
    // Copy/validate even direct host construction, not just config.ts callers.
    this.config = normalizePlacementDeciderConfig(config);
  }
  async decide(request: PlacementRequest, options: PlacementDecisionOptions = {}): Promise<PlacementPlace | PlacementQueue> {
    const deadline = options.deadline ?? performance.now() + PLACEMENT_DEADLINE_MS;
    if (!this.config || this.config.mode === "off") return refusal("disabled");
    if (this.config.mode !== "enforce") return refusal("mode-mismatch");
    const answer = await this.answer(request, deadline);
    if (isPlacementPlace(answer) || isPlacementQueue(answer)) return answer;
    return refusal("mode-mismatch");
  }
  async observe(request: PlacementRequest, options: PlacementDecisionOptions = {}): Promise<PlacementShadowObservation> {
    const deadline = options.deadline ?? performance.now() + PLACEMENT_DEADLINE_MS;
    if (!this.config || this.config.mode === "off") return refusal("disabled");
    if (this.config.mode !== "shadow") return refusal("mode-mismatch");
    const answer = await this.answer(request, deadline);
    if (!isPlacementPlace(answer) && !isPlacementQueue(answer)) return answer;
    return refusal("mode-mismatch");
  }
  private async answer(request: PlacementRequest, deadline: number) {
    let intended: PlacementRequest;
    try { intended = snapshotPlacementRequest(request); } catch { return refusal("invalid-request"); }
    let result: CliResult;
    try { result = await runPlacementCli(this.config!.command, intended, deadline); } catch { return refusal("unavailable"); }
    if (result.failure) return refusal(result.failure);
    let answer;
    try { answer = decodePlacementAnswer(result.stdout, result.exitCode, intended); } catch { return refusal("invalid-response"); }
    if (performance.now() >= deadline) return refusal("timeout");
    return answer;
  }
  async register(grant: PlacementPlace, nativeIdentity: PlacementNativeIdentity): Promise<PlacementLifecycleResult> {
    const deadline = performance.now() + PLACEMENT_DEADLINE_MS;
    if (!this.config || this.config.mode !== "enforce") return lifecycleRefusal("disabled");
    let request: PlacementWireRequest;
    try { request = placementRegistrationRequest(grant, nativeIdentity); } catch { return lifecycleRefusal("invalid-request"); }
    return this.lifecycle(request, deadline);
  }
  async release(launch: PlacementPlace, proof: PlacementReleaseProof): Promise<PlacementLifecycleResult> {
    const deadline = performance.now() + PLACEMENT_DEADLINE_MS;
    if (!this.config || this.config.mode !== "enforce") return lifecycleRefusal("disabled");
    let request: PlacementWireRequest;
    try { request = placementReleaseRequest(launch, proof); } catch { return lifecycleRefusal("invalid-request"); }
    return this.lifecycle(request, deadline);
  }
  private async lifecycle(request: PlacementWireRequest, deadline: number): Promise<PlacementLifecycleResult> {
    try {
      const result = await runPlacementCli(this.config!.command, request, deadline);
      if (result.failure) return lifecycleRefusal(result.failure);
      // Receipt/cancel ACKs are NOT PLACE, registration acceptance or release proof.
      const acknowledgement = decodePlacementAcknowledgement(result.stdout, result.exitCode);
      if (performance.now() >= deadline) return lifecycleRefusal("timeout");
      return { kind: "unverified-acknowledgement", registered: false, released: false, acknowledgement };
    } catch { return lifecycleRefusal("invalid-response"); }
  }
}

export const createPlacementDecider = (config?: FabricPlacementDeciderConfig): PlacementDecider => new CliPlacementDecider(config);
