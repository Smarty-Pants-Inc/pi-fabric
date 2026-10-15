import { randomUUID } from "node:crypto";
import type { ChildProcess } from "node:child_process";
import {
  AttemptExitUnconfirmedError, ContainmentUnavailableError, failClosedContainment,
  type AttemptContainment, type AttemptExitReceipt, type AttemptLaunchReceipt, type AttemptMembership,
} from "./attempt-containment-contract.js";
import type { ResidentChildLifetime } from "./child-lifetime.js";
import type { ResidentLauncherIdentity, ResidentLaunchSpec, ResidentProcessIdentity } from "./handover.js";

/** Private native IPC, not the resident command / owner-file protocol. */
export const LAUNCHER_CONTAINMENT_ABI = "fabric-launcher-containment-1" as const;
const MAX_WAIT_MS = 120_000;
const MAX_PENDING = 16;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
type Operation = "membership" | "join";
type Receipt = AttemptMembership | AttemptExitReceipt;
type Row = Record<string, unknown>;

export interface LauncherContainmentBinding {
  launchAttestation: AttemptLaunchReceipt;
  launcher: ResidentLauncherIdentity;
  child: ResidentProcessIdentity;
  generation: string;
}

/** Node process and ChildProcess satisfy this interface. No caller-supplied event source per request. */
export interface LauncherContainmentChannel {
  readonly connected?: boolean;
  send?: (message: object, callback: (error: Error | null) => void) => boolean;
  on(event: string, listener: (...args: any[]) => void): unknown;
  off(event: string, listener: (...args: any[]) => void): unknown;
}
export interface LauncherContainmentClient extends AttemptContainment { dispose(): void; }
export interface LauncherContainmentServer {
  attemptMembership(): Promise<AttemptMembership>;
  /** Launcher-local only after native child exit. Never signals or asks the host to exit. */
  joinAttemptExit(deadlineAtMs: number): Promise<AttemptExitReceipt>;
  readonly pending: number;
  dispose(): void;
}

const row = (value: unknown): value is Row => typeof value === "object" && value !== null && !Array.isArray(value);
const keys = (value: Row, expected: string[]): boolean =>
  Object.keys(value).length === expected.length && expected.every(key => Object.hasOwn(value, key));
const text = (value: unknown, max = 256): value is string => typeof value === "string" && value.length > 0 && value.length <= max;
const integer = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value > 0;
const pids = (value: unknown): value is number[] => Array.isArray(value) && value.every(integer) && new Set(value).size === value.length;
const identity = (value: unknown): value is ResidentProcessIdentity =>
  row(value) && keys(value, ["pid", "processStartTime"]) && integer(value.pid) && text(value.processStartTime);
const attestation = (value: unknown): value is AttemptLaunchReceipt => row(value) &&
  keys(value, ["attemptId", "scope", "cgroupPath", "launchedInScope", "at"]) && text(value.attemptId) &&
  text(value.scope) && text(value.cgroupPath, 4096) && value.launchedInScope === true && integer(value.at) && value.at <= Date.now();
const launcher = (value: unknown): value is ResidentLauncherIdentity => row(value) &&
  keys(value, ["pid", "processStartTime", "token", "entry", "runtime"]) && integer(value.pid) &&
  text(value.processStartTime) && text(value.token) && text(value.entry, 4096) && text(value.runtime, 4096);
const validBinding = (value: unknown): value is LauncherContainmentBinding => row(value) &&
  keys(value, ["launchAttestation", "launcher", "child", "generation"]) && attestation(value.launchAttestation) &&
  launcher(value.launcher) && identity(value.child) && text(value.generation);
const unavailable = (reason: string): ContainmentUnavailableError => new ContainmentUnavailableError(reason);

function pinBinding(value: LauncherContainmentBinding): LauncherContainmentBinding {
  if (!validBinding(value)) throw unavailable("Launcher containment requires a complete launcher-issued launch binding");
  return Object.freeze({ launchAttestation: Object.freeze({ ...value.launchAttestation }),
    launcher: Object.freeze({ ...value.launcher }), child: Object.freeze({ ...value.child }), generation: value.generation });
}
function sameBinding(value: unknown, expected: LauncherContainmentBinding): boolean {
  if (!validBinding(value)) return false;
  return value.generation === expected.generation &&
    Object.keys(expected.launchAttestation).every(key => value.launchAttestation[key as keyof AttemptLaunchReceipt] === expected.launchAttestation[key as keyof AttemptLaunchReceipt]) &&
    Object.keys(expected.launcher).every(key => value.launcher[key as keyof ResidentLauncherIdentity] === expected.launcher[key as keyof ResidentLauncherIdentity]) &&
    value.child.pid === expected.child.pid && value.child.processStartTime === expected.child.processStartTime;
}
function assertDeadline(deadlineAt: number): void {
  const now = Date.now();
  if (!integer(deadlineAt) || deadlineAt <= now || deadlineAt - now > MAX_WAIT_MS) {
    throw unavailable("Containment request needs a future absolute deadline within 120 seconds");
  }
}
function timedOut(operation: Operation, binding: LauncherContainmentBinding): Error {
  return operation === "join"
    ? new AttemptExitUnconfirmedError({ attemptId: binding.launchAttestation.attemptId, remainingPids: [] })
    : unavailable("Launcher containment membership deadline elapsed; native attempt custody is retained");
}
function validateReceipt(value: unknown, operation: Operation, binding: LauncherContainmentBinding, deadlineAt: number): Receipt {
  const launch = binding.launchAttestation;
  if (!row(value) || value.attemptId !== launch.attemptId || value.scope !== launch.scope) {
    throw unavailable("Containment receipt does not match the owned launch attempt");
  }
  if (operation === "membership") {
    if (!keys(value, ["attemptId", "scope", "pids", "populated"]) || !pids(value.pids) ||
        typeof value.populated !== "boolean" || (!value.populated && value.pids.length !== 0)) {
      throw unavailable("Invalid launcher attempt membership receipt");
    }
    return { attemptId: launch.attemptId, scope: launch.scope, pids: [...value.pids], populated: value.populated };
  }
  if (!keys(value, ["attemptId", "scope", "populated", "at"]) || value.populated !== false ||
      !integer(value.at) || value.at < launch.at || value.at > Date.now() || value.at > deadlineAt) {
    throw unavailable("Invalid launcher whole-attempt exit receipt");
  }
  return { attemptId: launch.attemptId, scope: launch.scope, populated: false, at: value.at };
}
function serializeError(error: unknown, operation: Operation | undefined, binding: LauncherContainmentBinding): Row {
  if (operation === "join" && error instanceof AttemptExitUnconfirmedError &&
      error.attemptId === binding.launchAttestation.attemptId && pids(error.remainingPids)) {
    return { name: error.name, code: error.code, reason: error.message,
      attemptId: error.attemptId, remainingPids: [...error.remainingPids] };
  }
  const failure = error instanceof ContainmentUnavailableError && text(error.reason, 4096)
    ? error : unavailable("Launcher containment receipt is unavailable; retain the current attempt");
  return { name: "ContainmentUnavailableError", code: failure.code, reason: failure.reason };
}
function deserializeError(value: unknown, operation: Operation, binding: LauncherContainmentBinding): Error {
  if (row(value) && keys(value, ["name", "code", "reason"]) && value.name === "ContainmentUnavailableError" &&
      value.code === "CONTAINMENT_UNAVAILABLE" && text(value.reason, 4096)) return unavailable(value.reason);
  if (operation === "join" && row(value) && keys(value, ["name", "code", "reason", "attemptId", "remainingPids"]) &&
      value.name === "AttemptExitUnconfirmedError" && value.code === "ATTEMPT_EXIT_UNCONFIRMED" &&
      text(value.reason, 4096) && value.attemptId === binding.launchAttestation.attemptId && pids(value.remainingPids)) {
    return new AttemptExitUnconfirmedError({ attemptId: value.attemptId, remainingPids: value.remainingPids });
  }
  return unavailable("Invalid launcher containment error receipt");
}

export function createLauncherContainmentClient(options: {
  channel: LauncherContainmentChannel;
  binding: LauncherContainmentBinding;
  timeoutMs?: number;
}): LauncherContainmentClient {
  const binding = pinBinding(options.binding);
  const channel = options.channel;
  const timeoutMs = options.timeoutMs ?? 5_000;
  if (!integer(timeoutMs) || timeoutMs > MAX_WAIT_MS) throw unavailable("Invalid containment IPC timeout");
  let closed = false;
  const pending = new Map<string, { operation: Operation; deadlineAt: number; timer: NodeJS.Timeout;
    resolve(value: Receipt): void; reject(error: Error): void }>();
  const finish = (id: string, error?: Error, receipt?: Receipt): void => {
    const request = pending.get(id);
    if (!request) return;
    pending.delete(id); clearTimeout(request.timer);
    if (error) request.reject(error); else request.resolve(receipt!);
  };
  const lost = (): void => {
    closed = true;
    for (const id of pending.keys()) finish(id, unavailable("Owned launcher IPC channel was lost; retain the resident"));
  };
  const message = (value: unknown): void => {
    if (!row(value) || value.abi !== LAUNCHER_CONTAINMENT_ABI || typeof value.requestId !== "string") return;
    const request = pending.get(value.requestId);
    if (!request) return; // Other IPC protocols, stale receipts and unrelated requests have no authority.
    try {
      if (Date.now() >= request.deadlineAt) throw timedOut(request.operation, binding);
      if (value.kind !== "response" || value.operation !== request.operation || !sameBinding(value.binding, binding) ||
          typeof value.ok !== "boolean" || !keys(value, ["abi", "kind", "requestId", "operation", "binding", "ok", value.ok ? "receipt" : "error"])) {
        throw unavailable("Launcher containment response correlation/binding is invalid");
      }
      if (!value.ok) throw deserializeError(value.error, request.operation, binding);
      finish(value.requestId, undefined, validateReceipt(value.receipt, request.operation, binding, request.deadlineAt));
    } catch (error) { finish(value.requestId, error instanceof Error ? error : unavailable("Invalid containment response")); }
  };
  channel.on("message", message); channel.on("disconnect", lost); channel.on("error", lost);
  const request = (operation: Operation, attemptId: string, deadlineAt: number): Promise<Receipt> => {
    try {
      if (attemptId !== binding.launchAttestation.attemptId) throw unavailable("Host containment cannot select a different launch attempt");
      assertDeadline(deadlineAt);
      if (closed || channel.connected !== true || !channel.send) throw unavailable("Owned launcher IPC channel is unavailable");
      if (pending.size >= MAX_PENDING) throw unavailable("Containment IPC request custody is full");
    } catch (error) { return Promise.reject(error); }
    const requestId = randomUUID();
    return new Promise<Receipt>((resolve, reject) => {
      const timer = setTimeout(() => finish(requestId, timedOut(operation, binding)), Math.max(0, deadlineAt - Date.now()));
      pending.set(requestId, { operation, deadlineAt, timer, resolve, reject });
      try {
        // send(false) is native backpressure, not failed delivery. Its callback/error event owns failure.
        channel.send!({ abi: LAUNCHER_CONTAINMENT_ABI, kind: "request", requestId, operation, binding, deadlineAt },
          error => { if (error) lost(); });
      } catch { lost(); }
    });
  };
  return {
    launchInScope: (_spec: ResidentLaunchSpec, _attemptId: string) => Promise.reject(unavailable("Host has no containment launch authority")),
    attemptMembership: id => request("membership", id, Date.now() + timeoutMs) as Promise<AttemptMembership>,
    joinAttemptExit: (id, deadlineAt) => request("join", id, deadlineAt) as Promise<AttemptExitReceipt>,
    dispose: () => { lost(); channel.off("message", message); channel.off("disconnect", lost); channel.off("error", lost); },
  };
}

export function createLauncherContainmentServer(options: {
  child: ChildProcess;
  native: Pick<ResidentChildLifetime, "exited">;
  binding: LauncherContainmentBinding;
  isCurrent(): boolean;
  containment?: AttemptContainment;
}): LauncherContainmentServer {
  const binding = pinBinding(options.binding);
  const child = options.child;
  const native = options.native;
  const containment = options.containment ?? failClosedContainment;
  let disposed = false;
  let disconnected = false;
  // Losing a deadline/channel does NOT cancel an adapter call or release native child custody.
  const pending = new Set<Promise<Receipt>>();
  const exchanges = new Set<string>();
  const assertCurrent = (): void => {
    if (disposed || child.pid !== binding.child.pid || !options.isCurrent()) {
      throw unavailable("Launcher no longer owns this exact child/attempt generation");
    }
  };
  const perform = async (operation: Operation, deadlineAt: number): Promise<Receipt> => {
    assertCurrent(); assertDeadline(deadlineAt);
    if (operation === "join" && !native.exited) {
      throw unavailable("A live host cannot certify its own exit; only launcher custody after native child exit may join");
    }
    if (pending.size >= MAX_PENDING) throw unavailable("Retained containment adapter custody is full");
    const owned = Promise.resolve().then(async () => {
      assertCurrent(); assertDeadline(deadlineAt);
      const value = operation === "membership"
        ? await containment.attemptMembership(binding.launchAttestation.attemptId)
        : await containment.joinAttemptExit(binding.launchAttestation.attemptId, deadlineAt);
      assertCurrent();
      if (Date.now() >= deadlineAt) throw timedOut(operation, binding);
      return validateReceipt(value, operation, binding, deadlineAt);
    });
    pending.add(owned);
    void owned.then(() => pending.delete(owned), () => pending.delete(owned));
    let timer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([owned, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(timedOut(operation, binding)), Math.max(0, deadlineAt - Date.now()));
      })]);
    } finally { if (timer) clearTimeout(timer); }
  };
  const reply = (requestId: string, operation: string, receipt?: Receipt, error?: unknown): void => {
    if (disposed || disconnected || child.connected !== true || !child.send) return;
    // A queued response cannot publish a positive receipt after the launcher rescoping hook changed current.
    if (receipt) {
      try { assertCurrent(); }
      catch (failure) { receipt = undefined; error = failure; }
    }
    const response = { abi: LAUNCHER_CONTAINMENT_ABI, kind: "response", requestId, operation, binding,
      ...(receipt ? { ok: true, receipt } : { ok: false, error: serializeError(error, operation === "join" ? "join" : undefined, binding) }) };
    try { child.send(response, failure => { if (failure) disconnected = true; }); }
    catch { disconnected = true; }
  };
  const message = (value: unknown): void => {
    if (!row(value) || value.abi !== LAUNCHER_CONTAINMENT_ABI || value.kind !== "request" ||
        typeof value.requestId !== "string" || !UUID.test(value.requestId)) return;
    const id = value.requestId;
    const operation = text(value.operation, 32) ? value.operation : "unsupported";
    try {
      assertCurrent();
      if (!keys(value, ["abi", "kind", "requestId", "operation", "binding", "deadlineAt"]) ||
          !sameBinding(value.binding, binding) || (operation !== "membership" && operation !== "join") ||
          typeof value.deadlineAt !== "number") throw unavailable("Unsupported or unbound launcher containment request");
      assertDeadline(value.deadlineAt);
      if (exchanges.has(id)) throw unavailable("Containment requestId is already in custody");
      if (disconnected || child.connected !== true) throw unavailable("Owned child IPC channel is unavailable");
      exchanges.add(id);
      // Only the captured child's event may enter. No root, path, target or launch command is accepted.
      const result = perform(operation, value.deadlineAt);
      void result.then(receipt => reply(id, operation, receipt), error => reply(id, operation, undefined, error))
        .finally(() => exchanges.delete(id));
    } catch (error) { reply(id, operation, undefined, error); }
  };
  const lost = (): void => { disconnected = true; };
  child.on("message", message); child.on("disconnect", lost); child.on("error", lost);
  return {
    attemptMembership: () => perform("membership", Date.now() + 5_000) as Promise<AttemptMembership>,
    joinAttemptExit: deadlineAt => perform("join", deadlineAt) as Promise<AttemptExitReceipt>,
    get pending() { return pending.size; },
    dispose: () => { disposed = true; child.off("message", message); child.off("disconnect", lost); child.off("error", lost); },
  };
}
