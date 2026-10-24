import fs from "node:fs";
import type { AgentRunRecord, AgentTransportAdapter, AgentTransportHandle, AgentTransportLaunch } from "../../agents/types.js";
import { writeJsonAtomic } from "../../core/atomic-write.js";
import { diagnostics } from "../../core/diagnostics.js";
import { SandboxPlacementHttp } from "./placement-http.js";
import { SANDBOX_PLACEMENT_WAIT_MS, SANDBOX_READY_TIMEOUT_MS, type SandboxBinding, type SandboxPlacement, type SandboxSpawnIntent, type SandboxSpawnResult, type SandboxTaskResult, type SandboxTerminalRecord, type SandboxTerminalState, type SandboxTransportConfig, type SandboxTransportDependencies, writeSandboxTerminalRecord } from "./types.js";
import { buildSandboxIntent } from "./intent.js";
import { sandboxResultPrefix, sandboxTaskMessage } from "../../agents/sandbox-auto-return.js";

const EMPTY_USAGE = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } as const;
const NAME = /^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$/u;
const HOST = /^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/u;
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/u;
const SANDBOX_POD_PENDING_POLL_MS = 250;
const isTimestamp = (value: unknown): value is string => typeof value === "string" && TIMESTAMP.test(value) && Number.isFinite(Date.parse(value)) && new Date(Date.parse(value)).toISOString().replace(".000Z", "Z") === value;

export class SandboxUnsupportedError extends Error {
  readonly code = "sandbox.unsupported";
  constructor() { super('Fabric agent transport "sandbox" is disabled; set agents.sandbox.enabled=true'); this.name = "SandboxUnsupportedError"; }
}
export class SandboxUnavailableError extends Error {
  readonly code = "sandbox.unavailable";
  constructor() { super("Sandbox transport dependencies are not configured"); this.name = "SandboxUnavailableError"; }
}
export class SandboxPlacementRefusedError extends Error {
  readonly code = "sandbox.placement.refused";
  constructor(readonly placementCode: string) { super(`Sandbox placement refused the run (${placementCode})`); this.name = "SandboxPlacementRefusedError"; }
}
export class SandboxSpawnTimeoutError extends Error {
  readonly code = "sandbox.spawn.timeout";
  constructor() { super(`Sandbox placement was not admitted within ${SANDBOX_PLACEMENT_WAIT_MS} ms`); this.name = "SandboxSpawnTimeoutError"; }
}
export class SandboxReadyTimeoutError extends Error {
  readonly code = "sandbox.ready.timeout";
  constructor(readonly runId: string, readonly timeoutMs: number) { super(`Sandbox child presence for ${runId} was not observed within ${timeoutMs} ms`); this.name = "SandboxReadyTimeoutError"; }
}
export class SandboxPodPendingTimeoutError extends Error {
  readonly code = "FABRIC_SANDBOX_POD_PENDING_TIMEOUT";
  constructor(readonly runId: string, readonly timeoutMs: number, readonly field: string) {
    super(`Sandbox Placement field ${field} remained pending for run ${runId} beyond ${timeoutMs} ms`);
    this.name = "SandboxPodPendingTimeoutError";
  }
}
export class SandboxProtocolError extends Error {
  readonly code = "sandbox.protocol.invalid";
  constructor(detail: string) { super(detail); this.name = "SandboxProtocolError"; }
}

const isObject = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const abortIfNeeded = (signal?: AbortSignal): void => signal?.throwIfAborted();

const timeoutSleep = (milliseconds: number, signal?: AbortSignal): Promise<void> => new Promise((resolve, reject) => {
  if (signal?.aborted) { reject(signal.reason); return; }
  const timer = setTimeout(() => { cleanup(); resolve(); }, Math.max(0, milliseconds));
  timer.unref?.();
  const abort = () => { cleanup(); reject(signal?.reason); };
  const cleanup = () => { clearTimeout(timer); signal?.removeEventListener("abort", abort); };
  signal?.addEventListener("abort", abort, { once: true });
});

const protocolField = (field: string, detail = "is invalid"): SandboxProtocolError =>
  new SandboxProtocolError(`Placement ${field} ${detail}`);

type BindingCheck = { ready: true; binding: SandboxBinding } | { ready: false; field: string; binding: SandboxBinding };

const validateSpawnResult = (value: unknown, runId: string): SandboxSpawnResult => {
  const keys = ["runId", "status", "code", "detail", "retryAfterSeconds", "host", "claimRef", "claimUID", "podUID"];
  if (!isObject(value)) throw protocolField("spawn result response");
  const missing = keys.find(key => !Object.hasOwn(value, key));
  if (missing) throw protocolField(`spawn result field ${missing}`, "is missing");
  const extra = Object.keys(value).find(key => !keys.includes(key));
  if (extra) throw protocolField(`spawn result field ${extra}`, "is unexpected");
  if (value.runId !== runId) throw protocolField("spawn result field runId", "does not match the requested run");
  if (!["refused", "delayed", "admitted"].includes(String(value.status))) throw protocolField("spawn result field status");
  if (typeof value.code !== "string") throw protocolField("spawn result field code");
  if (typeof value.detail !== "string" || !value.detail || value.detail.length > 1024) throw protocolField("spawn result field detail");
  if (!(value.retryAfterSeconds === null || Number.isSafeInteger(value.retryAfterSeconds))) throw protocolField("spawn result field retryAfterSeconds");
  if (!(value.host === null || typeof value.host === "string")) throw protocolField("spawn result field host");
  if (!(value.claimRef === null || isObject(value.claimRef))) throw protocolField("spawn result field claimRef");
  if (!(value.claimUID === null || typeof value.claimUID === "string")) throw protocolField("spawn result field claimUID");
  if (!(value.podUID === null || typeof value.podUID === "string")) throw protocolField("spawn result field podUID");
  if (value.status === "refused" && value.retryAfterSeconds !== null) throw protocolField("spawn result field retryAfterSeconds", "must be null for refused status");
  if (value.status === "delayed" && (typeof value.retryAfterSeconds !== "number" || value.retryAfterSeconds < 1 || value.retryAfterSeconds > 3_600)) {
    throw protocolField("spawn result field retryAfterSeconds", "violates the delayed status invariant");
  }
  if (value.status === "admitted" && (typeof value.host !== "string" || !HOST.test(value.host))) throw protocolField("spawn result field host");
  if (value.claimRef !== null) {
    const ref = value.claimRef as Record<string, unknown>;
    const refMissing = ["namespace", "name", "uid"].find(key => !Object.hasOwn(ref, key));
    if (refMissing) throw protocolField(`spawn result field claimRef.${refMissing}`, "is missing");
    const refExtra = Object.keys(ref).find(key => !["namespace", "name", "uid"].includes(key));
    if (refExtra) throw protocolField(`spawn result field claimRef.${refExtra}`, "is unexpected");
    if (ref.namespace !== "smarty-island") throw protocolField("spawn result field claimRef.namespace");
    if (ref.name !== `spawn-${runId}`) throw protocolField("spawn result field claimRef.name", "does not match runId");
    if (typeof ref.uid !== "string" || !NAME.test(ref.uid)) throw protocolField("spawn result field claimRef.uid");
    if (value.claimUID !== ref.uid) throw protocolField("spawn result field claimUID", "does not match claimRef.uid");
    if (value.podUID !== null && (value.status !== "admitted" || !NAME.test(value.podUID))) throw protocolField("spawn result field podUID");
  } else if (value.claimUID !== null) {
    throw protocolField("spawn result field claimRef", "is null while claimUID is present");
  } else if (value.podUID !== null) {
    throw protocolField("spawn result field claimRef", "is null while podUID is present");
  }
  if (value.status !== "admitted" && value.podUID !== null) throw protocolField("spawn result field podUID", "must be null before admission");
  return value as unknown as SandboxSpawnResult;
};

const validateBinding = (value: unknown, intent: SandboxSpawnIntent, admitted: SandboxSpawnResult): BindingCheck => {
  const fields = ["runId", "status", "claimRef", "claimUID", "podUID", "podName", "host", "org", "role", "model", "provider", "thinking", "expectedSeconds", "deadline", "executionDeadline", "admittedAt"];
  if (!isObject(value)) throw protocolField("binding response");
  const missing = fields.find(field => !Object.hasOwn(value, field));
  if (missing) throw protocolField(`binding field ${missing}`, "is missing");
  const extra = Object.keys(value).find(field => !fields.includes(field));
  if (extra) throw protocolField(`binding field ${extra}`, "is unexpected");
  if (value.runId !== intent.runId) throw protocolField("binding field runId", "does not match the requested run");
  if (value.status !== "admitted") throw protocolField("binding field status", "is not admitted");
  if (!isTimestamp(value.admittedAt)) throw protocolField("binding field admittedAt");
  if (!isTimestamp(value.executionDeadline)) throw protocolField("binding field executionDeadline");
  if (typeof value.host !== "string" || !HOST.test(value.host)) throw protocolField("binding field host");
  if (value.host !== admitted.host) throw protocolField("binding field host", "does not match the admitted result");
  if (value.org !== intent.org) throw protocolField("binding field org", "does not match the intent");
  if (value.role !== intent.role) throw protocolField("binding field role", "does not match the intent");
  if (value.model !== intent.model) throw protocolField("binding field model", "does not match the intent");
  if (value.thinking !== intent.thinking) throw protocolField("binding field thinking", "does not match the intent");
  if (!(value.provider === null || typeof value.provider === "string" && NAME.test(value.provider))) throw protocolField("binding field provider");
  if (typeof value.expectedSeconds !== "number" || !Number.isSafeInteger(value.expectedSeconds) || value.expectedSeconds < 1 || value.expectedSeconds > 604_800) {
    throw protocolField("binding field expectedSeconds");
  }
  if (value.expectedSeconds !== intent.expectedSeconds) throw protocolField("binding field expectedSeconds", "does not match the intent");
  if (!(value.deadline === null || isTimestamp(value.deadline))) throw protocolField("binding field deadline");
  if (value.deadline !== intent.deadline) throw protocolField("binding field deadline", "does not match the intent");
  const expectedExecutionDeadline = Date.parse(value.admittedAt) + value.expectedSeconds * 1_000;
  const frozenDeadline = value.deadline ? Math.min(expectedExecutionDeadline, Date.parse(value.deadline)) : expectedExecutionDeadline;
  if (Date.parse(value.executionDeadline) !== frozenDeadline) throw protocolField("binding field executionDeadline", "does not match admittedAt/expectedSeconds/deadline");

  let claim: Record<string, unknown> | null;
  if (value.claimRef === null) claim = null;
  else if (!isObject(value.claimRef)) throw protocolField("binding field claimRef");
  else {
    claim = value.claimRef;
    const claimFields = ["namespace", "name", "uid"];
    const claimMissing = claimFields.find(field => !Object.hasOwn(claim!, field));
    if (claimMissing) throw protocolField(`binding field claimRef.${claimMissing}`, "is missing");
    const claimExtra = Object.keys(claim).find(field => !claimFields.includes(field));
    if (claimExtra) throw protocolField(`binding field claimRef.${claimExtra}`, "is unexpected");
    if (claim.namespace !== "smarty-island") throw protocolField("binding field claimRef.namespace");
    if (claim.name !== `spawn-${intent.runId}`) throw protocolField("binding field claimRef.name", "does not match runId");
    if (typeof claim.uid !== "string" || !NAME.test(claim.uid)) throw protocolField("binding field claimRef.uid");
    if (value.claimUID !== claim.uid) throw protocolField("binding field claimUID", "does not match claimRef.uid");
    if (admitted.claimRef !== null && (claim.namespace !== admitted.claimRef.namespace || claim.name !== admitted.claimRef.name || claim.uid !== admitted.claimRef.uid)) {
      throw protocolField("binding field claimRef", "does not match the admitted result");
    }
    if (admitted.claimUID !== null && value.claimUID !== admitted.claimUID) throw protocolField("binding field claimUID", "does not match the admitted result");
  }
  if (claim === null) {
    if (value.claimUID !== null) throw protocolField("binding field claimUID", "is present without claimRef");
    if (value.podUID !== null) throw protocolField("binding field podUID", "is present without claimRef");
    if (value.podName !== null) throw protocolField("binding field podName", "is present without claimRef");
    return { ready: false, field: "claimRef", binding: value as unknown as SandboxBinding };
  }
  if (value.podUID === null) {
    if (value.podName !== null && (typeof value.podName !== "string" || !HOST.test(value.podName))) throw protocolField("binding field podName");
    return { ready: false, field: "podUID", binding: value as unknown as SandboxBinding };
  }
  if (typeof value.podUID !== "string" || !NAME.test(value.podUID)) throw protocolField("binding field podUID");
  if (typeof value.podName !== "string" || !HOST.test(value.podName)) throw protocolField("binding field podName");
  // Pod UIDs may change when Placement recreates a Pod for the same run; runId and Claim UID are the stable binding.
  return { ready: true, binding: value as unknown as SandboxBinding };
};

const responseErrorIsRetryable = (error: unknown): boolean => {
  if (!isObject(error)) return true;
  if (typeof error.status === "number") return error.status === 503 || error.status >= 500;
  if (typeof error.code === "string") return ["PLACEMENT_UNAVAILABLE", "ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "FETCH_FAILED"].includes(error.code);
  return true;
};

const argValue = (args: readonly string[], key: string): string | undefined => {
  for (let index = 0; index < args.length; index += 2) if (args[index] === key) return args[index + 1];
  return undefined;
};
const requiredArg = (args: readonly string[], key: string): string => {
  const value = argValue(args, key);
  if (!value) throw new SandboxProtocolError(`Sandbox launch requires ${key}`);
  return value;
};

const toRecord = (base: AgentRunRecord, state: AgentRunRecord["status"], text: string, now: number, options: {
  exitCode?: number; error?: string; usage?: AgentRunRecord["usage"]; turns?: number; toolCalls?: number; outcome?: AgentRunRecord["outcome"];
} = {}): AgentRunRecord => ({
  ...base,
  status: state,
  text,
  updatedAt: now,
  ...(state === "completed" || state === "failed" || state === "stopped" || state === "timed_out" ? { finishedAt: now } : {}),
  ...(options.exitCode !== undefined ? { exitCode: options.exitCode } : {}),
  ...(options.error !== undefined ? { error: options.error } : {}),
  ...(options.usage ? { usage: options.usage } : {}),
  ...(options.turns !== undefined ? { turns: options.turns } : {}),
  ...(options.toolCalls !== undefined ? { toolCalls: options.toolCalls } : {}),
  ...(options.outcome ? { outcome: options.outcome } : {}),
});

const terminalAt = (now: number): string => new Date(now).toISOString().replace(".000Z", "Z");

export class SandboxTransport implements AgentTransportAdapter {
  readonly kind = "sandbox" as const;
  readonly #now: () => number;
  readonly #sleep: (milliseconds: number, signal?: AbortSignal) => Promise<void>;

  readonly dependencies: SandboxTransportDependencies;
  constructor(readonly config: SandboxTransportConfig, dependencies: SandboxTransportDependencies = {}) {
    this.dependencies = { ...dependencies, placement: dependencies.placement ?? new SandboxPlacementHttp(config.placementUrl) };
    this.#now = dependencies.now ?? Date.now;
    this.#sleep = dependencies.sleep ?? timeoutSleep;
  }

  async available(): Promise<boolean> { return true; }

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    const runId = request.id;
    let terminalWritten = false;
    const writeTerminal = async (state: SandboxTerminalState, options: { exitCode?: number; reason?: string } = {}): Promise<void> => {
      if (terminalWritten) return;
      const record: SandboxTerminalRecord = {
        placementRunId: runId,
        state,
        at: terminalAt(this.#now()),
        ...(options.exitCode !== undefined ? { exitCode: options.exitCode } : {}),
        ...(options.reason ? { reason: options.reason } : {}),
      };
      try {
        await writeSandboxTerminalRecord(this.dependencies.terminal, record);
        terminalWritten = true;
      } catch (error) {
        const code = isObject(error) && typeof error.code === "string" ? error.code : "FABRIC_PLACEMENT_TERMINAL_WRITE_FAILED";
        diagnostics.warn("src/transport/sandbox/transport.ts",
          `[pi-fabric] Placement terminal receipt for ${runId} failed (${code}); execution result is unchanged.`);
      }
    };
    const failBeforeStart = async (error: Error, reason: string): Promise<never> => {
      await writeTerminal("never-started", { reason }).catch(() => undefined);
      throw error;
    };

    if (!this.config.enabled) return failBeforeStart(new SandboxUnsupportedError(), "sandbox transport is disabled");
    const { placement, mesh, org, terminal: terminalKv } = this.dependencies;
    if (this.dependencies.meshTransport !== "nats" || !placement || !mesh || !terminalKv || !org) {
      return failBeforeStart(new SandboxUnavailableError(), "sandbox NATS/placement/result integration is not configured");
    }
    const taskFile = requiredArg(request.workerArguments, "--task-file");
    const statusFile = requiredArg(request.workerArguments, "--status-file");
    const timeoutMs = Number(requiredArg(request.workerArguments, "--timeout-ms"));
    let task: string;
    try { task = fs.readFileSync(taskFile, "utf8"); }
    catch { return failBeforeStart(new SandboxProtocolError("Sandbox task input is unavailable"), "task input was unavailable"); }
    const parentSession = request.parentSession;
    if (!parentSession || !NAME.test(parentSession)) return failBeforeStart(new SandboxProtocolError("Sandbox intent has no trusted parent session"), "parent session is unavailable");
    let intent: SandboxSpawnIntent;
    try {
      intent = buildSandboxIntent({
        runId,
        cwd: request.cwd,
        needs: request.needs ?? [],
        ...(request.complexity ? { complexity: request.complexity } : {}),
        ...(request.profile ? { profile: request.profile } : {}),
        ...(request.branch !== undefined ? { branch: request.branch } : {}),
        ...(request.sessionImport !== undefined ? { sessionImport: request.sessionImport } : {}),
        parentSession,
        org,
        timeoutMs,
        now: this.#now(),
        workerArguments: request.workerArguments,
        ...(this.dependencies.resolveRepository ? { resolveRepository: this.dependencies.resolveRepository } : {}),
      });
    } catch (error) {
      return failBeforeStart(error instanceof Error ? error : new SandboxProtocolError("Invalid sandbox intent"), "sandbox intent validation failed");
    }

    let admitted: SandboxSpawnResult;
    try { admitted = await this.#waitForAdmission(placement, intent, request.signal); }
    catch (error) {
      const typed = error instanceof Error ? error : new SandboxProtocolError("Placement admission failed");
      const code = isObject(typed) && typeof typed.code === "string" ? typed.code : undefined;
      return failBeforeStart(typed, typed instanceof SandboxPlacementRefusedError ? `placement refused (${typed.placementCode})` : code === "sandbox.spawn.timeout" ? "placement admission timed out" : "placement admission failed");
    }

    let binding: SandboxBinding;
    try { binding = await this.#waitForBinding(placement, intent, admitted, request.signal); }
    catch (error) {
      const typed = error instanceof Error ? error : new SandboxProtocolError("Placement binding field response failed");
      return failBeforeStart(typed, typed instanceof SandboxPodPendingTimeoutError ? "sandbox Pod or Claim binding remained pending" : "admitted binding was invalid");
    }
    const deadlineAt = Date.parse(binding.executionDeadline!);
    const deadlineRemaining = deadlineAt - this.#now();
    if (deadlineRemaining <= 0) return failBeforeStart(new SandboxReadyTimeoutError(runId, this.config.readyTimeoutMs), "execution deadline expired before child presence");
    const readyTimeoutMs = Math.min(this.config.readyTimeoutMs, SANDBOX_READY_TIMEOUT_MS, deadlineRemaining);

    let child: { id: string; placementRunId: string };
    const presenceSignal = request.signal
      ? AbortSignal.any([request.signal, AbortSignal.timeout(readyTimeoutMs)])
      : AbortSignal.timeout(readyTimeoutMs);
    try {
      child = await mesh.waitForPresence(runId, { timeoutMs: readyTimeoutMs, signal: presenceSignal });
      if (!child || typeof child.id !== "string" || !child.id || child.placementRunId !== runId) throw new SandboxProtocolError("NATS presence did not match the placement run id");
    } catch {
      return failBeforeStart(new SandboxReadyTimeoutError(runId, this.config.readyTimeoutMs), "child NATS presence was not observed; placement custody may remain active");
    }

    const startedAt = this.#now();
    let base: AgentRunRecord = {
      id: runId,
      name: request.name,
      task,
      runner: "pi",
      transport: "sandbox",
      cwd: request.cwd,
      status: "running",
      startedAt,
      updatedAt: startedAt,
      turns: 0,
      toolCalls: 0,
      text: "",
      usage: { ...EMPTY_USAGE },
      ...(intent.model ? { model: intent.model } : {}),
      warnings: ["Sandbox task execution and result custody use the island Fabric mesh."],
    };
    try { writeJsonAtomic(statusFile, base, { space: 2, durable: true }); }
    catch { return failBeforeStart(new SandboxProtocolError("Could not persist sandbox run status before delivery"), "local run status could not be persisted"); }
    let resultWait: Promise<SandboxTaskResult>;
    try { resultWait = mesh.awaitResult(child.id, runId, deadlineAt, request.signal); }
    catch (error) { resultWait = Promise.reject(error); }
    void resultWait.catch(() => undefined);
    const resultPrefix = sandboxResultPrefix(runId);
    const returnInstructions = `Fabric automatically returns your final assistant text to the spawner when this task settles. Do not send a second FABRIC-RESULT after it has been returned. If automatic return is unavailable, send exactly one result follow-up with agents.followUp({ id: ${JSON.stringify(parentSession)}, message: ${JSON.stringify(resultPrefix)} + answer }); the message must begin with that exact prefix. Use ordinary followUps only for updates.`;
    const deliveredTask = sandboxTaskMessage(runId, `${task}\n\n${returnInstructions}`);
    try { await mesh.deliverTask(child.id, deliveredTask, request.signal); }
    catch {
      base = { ...base, warnings: [...(base.warnings ?? []), "Task delivery acknowledgement was not received; awaiting the single result without retry."] };
      try { writeJsonAtomic(statusFile, base, { space: 2, durable: true }); } catch { /* the running status was persisted before the single delivery attempt */ }
    }

    let settled = false;
    let lostContact: string | undefined;
    let resolveClosed!: () => void;
    const closed = new Promise<void>(resolve => { resolveClosed = resolve; });
    let finishWork: Promise<void> | undefined;
    const finish = (work: () => Promise<void>): Promise<void> => {
      if (finishWork) return finishWork;
      finishWork = (async () => {
        settled = true;
        try { await work(); }
        catch {
          lostContact = "sandbox terminal/result publication failed; placement custody retained";
          writeJsonAtomic(statusFile, toRecord(base, "failed", "", this.#now(), {
            error: lostContact,
            outcome: "indeterminate",
          }), { space: 2, durable: true });
        } finally { resolveClosed(); }
      })();
      return finishWork;
    };
    const terminal = async (state: SandboxTerminalState, status: AgentRunRecord["status"], text: string, options: {
      exitCode?: number; error?: string; usage?: AgentRunRecord["usage"]; turns?: number; toolCalls?: number; outcome?: AgentRunRecord["outcome"]; reason?: string;
    } = {}): Promise<void> => {
      const now = this.#now();
      writeJsonAtomic(statusFile, toRecord(base, status, text, now, options), { space: 2, durable: true });
      await writeTerminal(state, { ...(options.exitCode !== undefined ? { exitCode: options.exitCode } : {}), ...(options.reason ? { reason: options.reason } : {}) });
    };

    const stopChild = async (reason: string): Promise<boolean> => {
      if (!mesh.stop) return false;
      try {
        const receipt = await mesh.stop(child.id, runId);
        if (!receipt.confirmed) {
          lostContact = receipt.reason ?? reason;
          request.onUnconfirmedExit?.(lostContact);
          return false;
        }
        lostContact = undefined;
        return true;
      } catch {
        lostContact = reason;
        request.onUnconfirmedExit?.(reason);
        return false;
      }
    };

    const handle: AgentTransportHandle = {
      kind: this.kind,
      sessionId: child.id,
      relaunchable: false,
      controls: false,
      livenessPollIntervalMs: 1_000,
      lostContact: () => lostContact,
      closed,
      isAlive: async () => !settled,
      stop: async () => {
        if (settled) return;
        if (!(await stopChild("sandbox stop was not confirmed"))) {
          throw new Error(`Sandbox child stop was not confirmed: ${lostContact ?? "placement custody retained"}`);
        }
        await finish(() => terminal("failed", "stopped", "", { error: "Sandbox task stopped", reason: "child stop was confirmed" }));
      },
    };

    void (async () => {
      try {
        const result = await this.#awaitResult(resultWait, deadlineAt);
        if (settled) return;
        this.#validateTaskResult(result, child.id, runId);
        lostContact = undefined;
        const successful = result.status === "completed" && (result.exitCode === undefined || result.exitCode === 0) && !result.error;
        const finalStatus = successful ? "completed" : result.status === "completed" ? "failed" : result.status;
        const terminalState = successful ? "finished" : "failed";
        const exitCode = result.exitCode ?? (successful ? 0 : 1);
        await finish(() => terminal(terminalState, finalStatus, result.text, {
          exitCode,
          ...(result.error ? { error: "Sandbox child reported a failed task" } : {}),
          ...(result.usage ? { usage: result.usage } : {}),
          ...(result.turns !== undefined ? { turns: result.turns } : {}),
          ...(result.toolCalls !== undefined ? { toolCalls: result.toolCalls } : {}),
          ...(!successful ? { reason: "sandbox task did not complete successfully" } : {}),
        }));
      } catch (error) {
        if (settled) return;
        const timedOut = error instanceof SandboxSpawnTimeoutError || (isObject(error) && error.code === "sandbox.execution.timeout");
        const reason = timedOut ? "sandbox task exceeded its bound execution deadline" : "sandbox result path failed";
        if (await stopChild(reason)) {
          await finish(() => terminal("failed", timedOut ? "timed_out" : "failed", "", {
            error: timedOut ? "Sandbox task timed out" : "Sandbox result could not be retrieved",
            reason,
          }));
        } else {
          const now = this.#now();
          const reason = timedOut ? "sandbox task timed out; remote exit is unconfirmed" : "sandbox result path failed; remote exit is unconfirmed";
          await finish(async () => {
            writeJsonAtomic(statusFile, toRecord(base, timedOut ? "timed_out" : "failed", "", now, {
              error: timedOut ? "Sandbox task timed out; remote exit is unconfirmed" : "Sandbox result path failed; remote exit is unconfirmed",
              outcome: "indeterminate",
            }), { space: 2, durable: true });
            await writeTerminal("failed", { reason });
          });
        }
      }
    })();
    return handle;
  }

  async #waitForAdmission(placement: SandboxPlacement, intent: SandboxSpawnIntent, signal?: AbortSignal): Promise<SandboxSpawnResult> {
    const deadline = this.#now() + SANDBOX_PLACEMENT_WAIT_MS;
    let last: SandboxSpawnResult | undefined;
    let hasSpawnResult = false;
    let retry = 0;
    while (this.#now() < deadline) {
      abortIfNeeded(signal);
      const remaining = Math.max(1, deadline - this.#now());
      const operationSignal = signal ? AbortSignal.any([signal, AbortSignal.timeout(remaining)]) : AbortSignal.timeout(remaining);
      try {
        const snapshot = hasSpawnResult
          ? await placement.getSpawn(intent.runId, operationSignal)
          : await placement.requestSpawn(intent, operationSignal);
        last = validateSpawnResult(snapshot, intent.runId);
        hasSpawnResult = true;
      } catch (error) {
        if (!responseErrorIsRetryable(error)) throw error;
        await this.#sleep(Math.min(1_000 * 2 ** Math.min(retry++, 5), remaining), signal);
        continue;
      }
      if (last.status === "refused") throw new SandboxPlacementRefusedError(last.code);
      if (last.status === "admitted") return last;
      try {
        for await (const snapshot of placement.watchSpawn(intent.runId, operationSignal)) {
          abortIfNeeded(signal);
          last = validateSpawnResult(snapshot, intent.runId);
          if (last.status === "refused") throw new SandboxPlacementRefusedError(last.code);
          if (last.status === "admitted") return last;
          if (this.#now() >= deadline) break;
        }
      } catch (error) {
        if (error instanceof SandboxPlacementRefusedError || !responseErrorIsRetryable(error)) throw error;
      }
      if (this.#now() >= deadline) break;
      const retryMs = Math.max(1_000, (last.retryAfterSeconds ?? 1) * 1_000);
      await this.#sleep(Math.min(retryMs, deadline - this.#now()), signal);
    }
    throw new SandboxSpawnTimeoutError();
  }

  async #waitForBinding(placement: SandboxPlacement, intent: SandboxSpawnIntent, admitted: SandboxSpawnResult, signal?: AbortSignal): Promise<SandboxBinding> {
    const timeoutMs = Math.min(this.config.readyTimeoutMs, SANDBOX_READY_TIMEOUT_MS);
    let deadline = this.#now() + timeoutMs;
    let pendingField = "claimRef";
    while (true) {
      abortIfNeeded(signal);
      const remaining = Math.ceil(deadline - this.#now());
      if (remaining <= 0) throw new SandboxPodPendingTimeoutError(intent.runId, timeoutMs, pendingField);
      const timeoutSignal = AbortSignal.timeout(remaining);
      const operationSignal = signal ? AbortSignal.any([signal, timeoutSignal]) : timeoutSignal;
      let response: unknown;
      try { response = await placement.getBinding(intent.runId, operationSignal); }
      catch (error) {
        if (signal?.aborted) throw signal.reason ?? error;
        if (timeoutSignal.aborted || this.#now() >= deadline) throw new SandboxPodPendingTimeoutError(intent.runId, timeoutMs, pendingField);
        throw protocolField("binding field response", "could not be read");
      }
      const checked = validateBinding(response, intent, admitted);
      deadline = Math.min(deadline, Date.parse(checked.binding.executionDeadline!));
      if (checked.ready) return checked.binding;
      pendingField = checked.field;
      const sleepMs = Math.min(SANDBOX_POD_PENDING_POLL_MS, Math.ceil(deadline - this.#now()));
      if (sleepMs <= 0) throw new SandboxPodPendingTimeoutError(intent.runId, timeoutMs, pendingField);
      try { await this.#sleep(sleepMs, operationSignal); }
      catch (error) {
        if (signal?.aborted) throw signal.reason ?? error;
        if (timeoutSignal.aborted || this.#now() >= deadline) throw new SandboxPodPendingTimeoutError(intent.runId, timeoutMs, pendingField);
        throw error;
      }
    }
  }

  async #awaitResult(resultWait: Promise<SandboxTaskResult>, deadlineAt: number): Promise<SandboxTaskResult> {
    const remaining = deadlineAt - this.#now();
    if (remaining <= 0) throw Object.assign(new Error("Sandbox task execution deadline expired"), { code: "sandbox.execution.timeout" });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        resultWait,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(Object.assign(new Error("Sandbox task execution deadline expired"), { code: "sandbox.execution.timeout" })), remaining);
          timer.unref?.();
        }),
      ]);
    } finally { if (timer) clearTimeout(timer); }
  }

  #validateTaskResult(result: SandboxTaskResult, childSession: string, runId: string): void {
    if (!result || result.runId !== runId || result.participantId !== childSession ||
      !["completed", "failed", "stopped", "timed_out"].includes(result.status) || typeof result.text !== "string" ||
      result.exitCode !== undefined && !Number.isSafeInteger(result.exitCode)) {
      throw new SandboxProtocolError("Sandbox child result did not match the bound run and session");
    }
  }
}
