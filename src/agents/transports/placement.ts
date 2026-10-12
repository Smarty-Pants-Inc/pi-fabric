import fs from "node:fs";
import path from "node:path";
import type { AgentPlacementConfig } from "../placement-config.js";
import type { AgentRunRecord, AgentTransportHandle, AgentTransportLaunch } from "../types.js";
import { writeJsonAtomic } from "../../core/atomic-write.js";
import { executeFile } from "./process-utils.js";
import { assertTransportLaunchAllowed } from "./launch-authority.js";
import { taskAgentEnvironment } from "../task-environment.js";
import { normalizeAgentRequires, RequiredInputMissingError } from "../input-validation.js";

interface Receipt { rc: number | string; text: string; stderr?: string }

/** The fleet launcher returns rc=3 and a concrete PREFLIGHT_REFUSED receipt when a --require path is absent on target. */
const requiredInputRefusal = (error: unknown, requires: readonly string[] | undefined): string | undefined => {
  const value = error as { code?: unknown; stdout?: unknown; stderr?: unknown };
  if (value.code !== 3 && value.code !== "3" || !requires?.length) return undefined;
  const output = [value.stdout, value.stderr].filter((part): part is string => typeof part === "string").join("\n");
  const line = output.split(/\r?\n/u).find(entry => entry.startsWith("PREFLIGHT_REFUSED: "));
  if (!line) return undefined;
  return requires.find(file => line === `PREFLIGHT_REFUSED: ${file}` || line.startsWith(`PREFLIGHT_REFUSED: ${file}:`));
};

const render = (template: string, values: Record<string, string>): string => template.replace(/\{([a-zA-Z]+)\}/g, (_match, key: string) => {
  if (values[key] === undefined) throw new Error(`Placement placeholder unavailable: ${key}`);
  return values[key]!;
});
const receipt = (input: unknown): Receipt | undefined => {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw new Error("Invalid placement result receipt");
  const value = input as Record<string, unknown>;
  if (value.rc === null) return undefined;
  if (!(typeof value.rc === "number" && Number.isSafeInteger(value.rc) || typeof value.rc === "string" && value.rc.trim()) || typeof value.text !== "string" || value.stderr !== undefined && typeof value.stderr !== "string") {
    throw new Error("Placement receipt requires terminal rc and result text");
  }
  return value as unknown as Receipt;
};

/** One-shot task protocol. rc is a native-exit receipt, NOT the existence of result.md.
 * No timers/processes survive outside manager-owned launch/isAlive/stop operations.
 */
export const launchPlacedTask = async (request: AgentTransportLaunch, config: AgentPlacementConfig): Promise<AgentTransportHandle> => {
  const requires = normalizeAgentRequires(request.requires);
  const args = new Map<string, string>();
  for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
  const required = (flag: string): string => {
    const value = args.get(flag);
    if (!value) throw new Error(`Placement requires ${flag}`);
    return value;
  };
  const task = fs.readFileSync(required("--task-file"), "utf8");
  const statusFile = required("--status-file");
  const logFile = required("--log-file");
  const timeoutMs = Number(required("--timeout-ms"));
  const startedAt = Date.now();
  const deadline = startedAt + timeoutMs;
  const values: Record<string, string> = {
    id: request.id, cwd: request.cwd, task, minutes: String(Math.max(1, Math.ceil(timeoutMs / 60_000))),
    model: args.get("--model") ?? "", thinking: args.get("--thinking") ?? "high",
  };
  const environment = taskAgentEnvironment();
  // The launcher binds its inbox notification to the Pi caller, not this child.
  environment.PI_SESSION_ID = args.get("--fabric-session-id") ?? environment.PI_SESSION_ID;
  const command = async (template: string[], limit = config.commandTimeoutMs, signal?: AbortSignal, extraArguments: string[] = []) => {
    const argv = template.map(entry => render(entry, values));
    // The fleet launcher's current repeatable preflight flag is --require.
    // Keep paths literal and before the prompt separator, never as task text.
    const separator = argv.indexOf("--");
    argv.splice(separator < 0 ? argv.length : separator, 0, ...extraArguments);
    return executeFile(argv[0]!, argv.slice(1), { cwd: request.cwd, env: environment, timeoutMs: Math.max(1, limit), ...(signal ? { signal } : {}), killSignal: "SIGKILL" });
  };
  const audit = (type: string, data: Record<string, unknown>) => fs.appendFileSync(logFile, JSON.stringify({ type, ts: Date.now(), id: request.id, ...data }) + "\n", { mode: 0o600 });
  const base: AgentRunRecord = {
    id: request.id, name: request.name, task, runner: "pi", transport: "process", cwd: request.cwd,
    status: "running", startedAt, updatedAt: startedAt, turns: 0, toolCalls: 0, text: "",
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    ...(values.model ? { model: values.model } : {}),
  };
  let exited = false, terminal = false;
  let debt: string | undefined;
  let lastPoll = 0, lastError: string | undefined;
  let polling: Promise<void> | undefined, stopping: Promise<void> | undefined;
  const save = (status: AgentRunRecord["status"], text = "", error?: string, rc?: number, stderr?: string) => {
    terminal = status !== "running";
    const now = Date.now();
    writeJsonAtomic(statusFile, { ...base, status, text, updatedAt: now,
      ...(terminal ? { finishedAt: now } : {}), ...(error ? { error } : {}),
      ...(rc !== undefined ? { exitCode: rc } : {}), ...(stderr ? { stderr } : {}),
      warnings: ["Remote one-shot placement: no streaming, controls, tool telemetry, or usage accounting.", ...(debt ? [debt] : [])],
    });
  };
  const readReceipt = async (limit: number): Promise<Receipt | undefined> => {
    if (config.resultCommand) return receipt(JSON.parse((await command(config.resultCommand, limit)).stdout));
    const directory = values.resultDir!;
    let rc: string;
    try { rc = fs.readFileSync(path.join(directory, "rc"), "utf8").trim(); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
    if (!rc) return undefined;
    // Read text only AFTER terminal rc. A partial result file never settles a run.
    const text = fs.readFileSync(path.join(directory, "result.md"), "utf8");
    let stderr: string | undefined;
    try { stderr = fs.readFileSync(path.join(directory, "stderr.log"), "utf8"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    return receipt({ rc, text, ...(stderr !== undefined ? { stderr } : {}) });
  };
  const consume = (result: Receipt, forced?: "stopped" | "timed_out") => {
    exited = true;
    debt = undefined;
    const rc = /^-?\d+$/.test(String(result.rc)) ? Number(result.rc) : undefined;
    const status = rc === 0 ? "completed" : forced ?? (rc === 124 || /timeout/i.test(String(result.rc)) ? "timed_out" : "failed");
    save(status, result.text, status === "completed" ? undefined : `Remote task ${status} (rc=${result.rc})`, rc, result.stderr);
    audit("placement.result", { host: values.host, status, rc: result.rc });
  };
  const stop = (status: "stopped" | "timed_out" = "stopped"): Promise<void> => {
    if (exited || terminal && debt) return Promise.resolve();
    if (stopping) return stopping;
    stopping = (async () => {
      await polling;
      if (exited) return;
      const stopDeadline = Date.now() + Math.min(config.commandTimeoutMs, 5_000);
      try {
        if (!values.host) throw new Error("Launcher did not return a target host");
        await command(config.cancelCommand, Math.max(1, stopDeadline - Date.now()));
        while (Date.now() < stopDeadline) {
          const result = await readReceipt(Math.max(1, stopDeadline - Date.now()));
          if (result) { consume(result, status); return; }
          await new Promise(resolve => setTimeout(resolve, Math.min(config.pollIntervalMs, Math.max(1, stopDeadline - Date.now()))));
        }
        throw new Error("Cancellation returned without a terminal rc receipt");
      } catch (error) {
        debt = `Remote execution exit unconfirmed; custody retained: ${String(error)}`;
        request.onUnconfirmedExit?.(debt);
        save(status, "", status === "timed_out" ? `Agent timed out after ${timeoutMs}ms; ${debt}` : debt, undefined, lastError);
        audit("placement.exit_unconfirmed", { reason: debt });
      }
    })().finally(() => { stopping = undefined; });
    return stopping;
  };
  const refresh = async (): Promise<void> => {
    if (terminal || polling) return polling;
    if (Date.now() >= deadline) { await stop("timed_out"); return; }
    if (Date.now() - lastPoll < config.pollIntervalMs) return;
    lastPoll = Date.now();
    polling = (async () => {
      try {
        const result = await readReceipt(Math.min(config.commandTimeoutMs, Math.max(1, deadline - Date.now())));
        if (result) consume(result);
      } catch (error) { lastError = String(error); }
    })().finally(() => { polling = undefined; });
    await polling;
  };
  const handle: AgentTransportHandle = {
    kind: "process", sessionId: `placement:${request.id}`, relaunchable: false, controls: false,
    livenessPollIntervalMs: config.pollIntervalMs,
    lostContact: () => debt, stopDebt: () => debt,
    isAlive: async () => { await refresh(); return !terminal; },
    stop: () => stop(),
  };
  // Rendering/read errors above are prelaunch. After exec starts, failure cannot
  // prove that nothing ran remotely: retain custody and never spawn locally.
  assertTransportLaunchAllowed(request);
  try {
    let output: string;
    try { output = (await command(config.command, config.commandTimeoutMs, request.signal, (requires ?? []).flatMap(file => ["--require", file]))).stdout; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") throw Object.assign(error as Error, { launchOutcome: "unlaunched" });
      const missing = requiredInputRefusal(error, requires);
      if (missing !== undefined) {
        const index = requires?.indexOf(missing);
        throw new RequiredInputMissingError(missing, index !== undefined && index >= 0 ? index : undefined);
      }
      debt = `Placement launch outcome unknown: ${String(error)}`;
      request.onUnconfirmedExit?.(debt);
      save("failed", "", debt, undefined, String((error as { stderr?: string }).stderr ?? ""));
      audit("placement.launch_unknown", { reason: debt });
      return handle;
    }
    const accepted = output.match(/^RYZEN2_TASK_ACCEPTED ([A-Za-z0-9._-]+) on ([A-Za-z0-9][A-Za-z0-9._-]*)\b/m);
    if (!accepted || accepted[1] !== request.id) {
      debt = "Placement launch outcome unknown: missing or mismatched RYZEN2_TASK_ACCEPTED receipt";
      request.onUnconfirmedExit?.(debt);
      save("failed", "", debt);
      audit("placement.launch_unknown", { reason: debt });
      return handle;
    }
    values.host = accepted[2]!;
    if (config.sshAliases && Object.hasOwn(config.sshAliases, values.host)) values.sshAlias = config.sshAliases[values.host]!;
    if ([config.resultDirectory ?? "", ...(config.resultCommand ?? []), ...config.cancelCommand].some(entry => entry.includes("{sshAlias}")) && !values.sshAlias) {
      throw new Error(`Placement SSH alias unavailable for accepted host: ${values.host}`);
    }
    if (config.resultDirectory) values.resultDir = render(config.resultDirectory, values);
    writeJsonAtomic(path.join(path.dirname(statusFile), "placement.json"), { id: request.id, host: values.host, output, deadline, ...(values.resultDir ? { resultDirectory: values.resultDir } : {}) });
    audit("placement.remote", { host: values.host, needs: request.needs ?? [], deadline });
    save("running");
    return handle;
  } catch (error) {
    if ((error as { launchOutcome?: string }).launchOutcome === "unlaunched") throw error;
    // A disk/template failure after launcher acceptance is not a safe rollback.
    // Reuse the manager's unknown-launch fence; never release or relaunch it.
    throw Object.assign(error instanceof Error ? error : new Error(String(error)), {
      launchOutcome: "unknown", cleanupPending: true, transport: "process", sessionId: handle.sessionId,
    });
  }
};
