#!/usr/bin/env node

import fs from "node:fs";
import { followUpFile, followUpState, followUpMessageId, releaseFollowUpPayload } from "./agents/follow-up-delivery.js";
import path from "node:path";
import { fileURLToPath } from "node:url";
<<<<<<< HEAD
import { randomUUID } from "node:crypto";
=======
>>>>>>> upstream-v0.105.0
import type { ChildProcess, SpawnOptions } from "node:child_process";
import { assertWorkerRuntime, writeWorkerStartupFailure } from "./worker/startup.js";

import { StringDecoder } from "node:string_decoder";
import type { ImageContent } from "@earendil-works/pi-ai";
import type {
  AgentRunRecord,
  AgentRunStatus,
} from "./agents/types.js";
import { applyChildPriority } from "./agents/priority.js";
import { saveWorkerCompletion } from "./agents/completion-journal.js";
import { taskAgentEnvironment } from "./agents/task-environment.js";
import { applyTaskReturnAddress } from "./agents/task-return-address.js";
import { retryableProviderError } from "./worker/provider-error.js";
import { copyFabricProvenance, type FabricTurnProvenance } from "./fabric-provenance.js";
import { ActivationSession } from "./worker/activation-session.js";

let crossSpawn: typeof import("cross-spawn");

const NODE_SCRIPT_EXTENSIONS = new Set([".js", ".cjs", ".mjs", ".ts", ".cts", ".mts"]);

// Extensionless launchers such as ~/.local/bin/pi start with
// `#!/usr/bin/env node`; a Herdr child's server PATH need not contain node.
// Only the PATH-dependent `env` form is rewritten; an absolute interpreter
// already works. ponytail: `env -S` flags are not replayed; revisit if a
// launcher needs them.
const nodeShebang = (command: string): boolean => {
  if (!path.isAbsolute(command)) return false;
  let fd: number | undefined;
  try {
    fd = fs.openSync(command, "r");
    const head = Buffer.alloc(256);
    const line = head.subarray(0, fs.readSync(fd, head, 0, head.length, 0)).toString("utf8").split(/\r?\n/, 1)[0] ?? "";
    return /^#!\s*\S*\/env\s+(?:-S\s+)?node(?:\s|$)/.test(line);
  } catch {
    return false;
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
};

const spawnCli = (
  command: string,
  args: readonly string[],
  options: SpawnOptions,
): ChildProcess => NODE_SCRIPT_EXTENSIONS.has(path.extname(command).toLowerCase()) || nodeShebang(command)
  ? crossSpawn(process.execPath, [command, ...args], options)
  : crossSpawn(command, [...args], options);

type ClaudeCliModule = typeof import("./agents/claude-cli.js");
type VedaCliModule = typeof import("./agents/veda-cli.js");
type CompactControlModule = typeof import("./agents/compact-control.js");
type WorkerOptionsModule = typeof import("./worker/options.js");
type WorkerRunRecordModule = typeof import("./worker/run-record.js");
type WorkerSessionExportModule = typeof import("./worker/session-export.js");
type WorkerEventProjectionModule = typeof import("./worker/event-projection.js");
const loadWorkerEventProjection = async (): Promise<WorkerEventProjectionModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/event-projection.js");
  const sourceModulePath = "./worker/event-projection.ts";
  return import(sourceModulePath) as Promise<WorkerEventProjectionModule>;
};

type WorkerRunLogModule = typeof import("./worker/run-log.js");
const loadWorkerRunLog = async (): Promise<WorkerRunLogModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/run-log.js");
  const sourceModulePath = "./worker/run-log.ts";
  return import(sourceModulePath) as Promise<WorkerRunLogModule>;
};

type WorkerModelControlModule = typeof import("./worker/model-control.js");

const loadWorkerModelControl = async (): Promise<WorkerModelControlModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/model-control.js");
  const sourceModulePath = "./worker/model-control.ts";
  return import(sourceModulePath) as Promise<WorkerModelControlModule>;
};

type WorkerRecoveryModule = typeof import("./worker/recovery-watchdog.js");

const loadWorkerRecovery = async (): Promise<WorkerRecoveryModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/recovery-watchdog.js");
  const sourceModulePath = "./worker/recovery-watchdog.ts";
  return import(sourceModulePath) as Promise<WorkerRecoveryModule>;
};

<<<<<<< HEAD
type WorkerToolCallStreamGuardModule = typeof import("./worker/tool-call-stream-guard.js");
const loadToolCallStreamGuard = async (): Promise<WorkerToolCallStreamGuardModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/tool-call-stream-guard.js");
  const sourceModulePath = "./worker/tool-call-stream-guard.ts";
  return import(sourceModulePath) as Promise<WorkerToolCallStreamGuardModule>;
};

type AgentResultModule = typeof import("./agents/result.js");
=======
type AgentResultModule = typeof import("./worker/result.js");
>>>>>>> upstream-v0.105.0

const loadAgentResult = async (): Promise<AgentResultModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/result.js");
  const sourceModulePath = "./worker/result.ts";
  return import(sourceModulePath) as Promise<AgentResultModule>;
};

type WorkerQuestionsModule = typeof import("./worker/questions.js");

const loadWorkerQuestions = async (): Promise<WorkerQuestionsModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/questions.js");
  const sourceModulePath = "./worker/questions.ts";
  return import(sourceModulePath) as Promise<WorkerQuestionsModule>;
};

const loadWorkerOptions = async (): Promise<WorkerOptionsModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/options.js");
  const sourceModulePath = "./worker/options.ts";
  return import(sourceModulePath) as Promise<WorkerOptionsModule>;
};

const loadWorkerRunRecord = async (): Promise<WorkerRunRecordModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/run-record.js");
  const sourceModulePath = "./worker/run-record.ts";
  return import(sourceModulePath) as Promise<WorkerRunRecordModule>;
};

const loadWorkerSessionExport = async (): Promise<WorkerSessionExportModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./worker/session-export.js");
  const sourceModulePath = "./worker/session-export.ts";
  return import(sourceModulePath) as Promise<WorkerSessionExportModule>;
};

const loadCompactControl = async (): Promise<CompactControlModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./agents/compact-control.js");
  const sourceModulePath = "./agents/compact-control.ts";
  return import(sourceModulePath) as Promise<CompactControlModule>;
};

const loadClaudeCli = async (): Promise<ClaudeCliModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./agents/claude-cli.js");
  const sourceModulePath = "./agents/claude-cli.ts";
  return import(sourceModulePath) as Promise<ClaudeCliModule>;
};

const loadVedaCli = async (): Promise<VedaCliModule> => {
  if (!import.meta.url.endsWith(".ts")) return import("./agents/veda-cli.js");
  const sourceModulePath = "./agents/veda-cli.ts";
  return import(sourceModulePath) as Promise<VedaCliModule>;
};

const MAX_STDERR_CHARS = 20_000;
const STEER_READ_CHUNK_BYTES = 256 * 1024;
const MAX_STEER_LINE_BYTES = 64 * 1024;
const MAX_STEER_COMMANDS_PER_POLL = 256;
const MAX_CLAUDE_PENDING_INPUTS = 256;
const MAX_CLAUDE_PENDING_TOOLS = 1_000;
const KILL_GRACE_MS = 5_000;

const extractText = (message: Record<string, unknown>): string => {
  const content = message.content;
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter(
      (part): part is { type: "text"; text: string } =>
        typeof part === "object" &&
        part !== null &&
        (part as Record<string, unknown>).type === "text" &&
        typeof (part as Record<string, unknown>).text === "string",
    )
    .map((part) => part.text)
    .join("");
};

const readImages = (filePath: string | undefined): ImageContent[] => {
  if (!filePath) return [];
  const parsed: unknown = JSON.parse(fs.readFileSync(filePath, "utf8"));
  if (!Array.isArray(parsed)) throw new Error("Agent images file must contain an array");
  const images: ImageContent[] = [];
  for (const value of parsed) {
    if (
      typeof value !== "object" ||
      value === null ||
      Array.isArray(value) ||
      (value as { type?: unknown }).type !== "image" ||
      typeof (value as { data?: unknown }).data !== "string" ||
      typeof (value as { mimeType?: unknown }).mimeType !== "string"
    ) {
      throw new Error("Agent images file contains an invalid image block");
    }
    images.push({
      type: "image",
      data: (value as { data: string }).data,
      mimeType: (value as { mimeType: string }).mimeType,
    });
  }
  return images;
};

const numberField = (value: unknown): number => (typeof value === "number" ? value : 0);

const stringField = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value.trim() : undefined;

const assistantError = (message: Record<string, unknown>): string => {
  const details: string[] = [];
  const direct = stringField(message.errorMessage) ?? stringField(message.error);
  if (direct) details.push(direct);
  if (Array.isArray(message.diagnostics)) {
    for (const diagnostic of message.diagnostics) {
      if (typeof diagnostic !== "object" || diagnostic === null || Array.isArray(diagnostic)) continue;
      const record = diagnostic as Record<string, unknown>;
      const nested =
        typeof record.error === "object" && record.error !== null && !Array.isArray(record.error)
          ? (record.error as Record<string, unknown>)
          : undefined;
      const detail = stringField(nested?.message) ?? stringField(record.message);
      if (detail) details.push(detail);
    }
  }
  const unique = [...new Set(details)];
  const provider = stringField(message.provider);
  const model = stringField(message.model);
  const source = [provider, model].filter((value): value is string => Boolean(value)).join("/");
  const summary = unique.join(" · ") || "Pi agent reported an error";
  return `${source ? `${source}: ` : ""}${summary}`.slice(0, MAX_STDERR_CHARS);
};

const runnerLabel = (runner: string): string =>
  runner === "claude" ? "Claude" : runner === "veda" ? "Veda" : "Pi";

const terminateChild = (child: ChildProcess, signal: NodeJS.Signals): void => {
  if (!child.pid) return;
  try {
    process.kill(process.platform === "win32" ? child.pid : -child.pid, signal);
  } catch { /* child process group already exited */ }
};



let crashContext: { statusFile: string; record: AgentRunRecord } | undefined;
let runRecordHelpers: WorkerRunRecordModule | undefined;
let terminalWritten = false;
let flushRunLog: (() => void) | undefined;
const writeCrashStatus = (error: unknown): void => {
<<<<<<< HEAD
  flushRunLog?.();
  if (!crashContext || !runRecordHelpers || terminalWritten) return;
=======
  if (terminalWritten) return;
>>>>>>> upstream-v0.105.0
  try {
    if (crashContext && runRecordHelpers) {
      runRecordHelpers.writeCrashRunRecord(crashContext.statusFile, crashContext.record, error);
    } else {
      writeWorkerStartupFailure(process.argv, error);
    }
  } catch {
    // Best effort: if the crash-status write itself fails, #monitor falls back
    // to "Agent transport exited without a result".
  }
};
process.on("uncaughtException", (error) => {
  writeCrashStatus(error);
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : error}\n`);
  process.exit(1);
});
process.on("unhandledRejection", (error) => {
  writeCrashStatus(error);
  process.stderr.write(`Unhandled rejection: ${error instanceof Error ? error.stack ?? error.message : error}\n`);
  process.exit(1);
});

const main = async (): Promise<void> => {
<<<<<<< HEAD
  const [optionHelpers, loadedRunRecordHelpers, sessionExportHelpers, {parseStructuredValue, validateAgentResult}, { PiModelControl }, { PiEventProjection }, { PiRecoveryWatchdog, PI_PROVIDER_RESUME_DELAYS_MS, recoveryTimeScale }, { createRunLogWriter, compactTerminalRunLog, MAX_EVENT_LINE_CHARS }, { ToolCallStreamGuard }] = await Promise.all([
=======
  assertWorkerRuntime();
  crossSpawn = (await import("cross-spawn")).default;
  const [optionHelpers, loadedRunRecordHelpers, sessionExportHelpers, {parseStructuredValue, validateAgentResult}, { PiModelControl }, { PiEventProjection }, { PiRecoveryWatchdog }] = await Promise.all([
>>>>>>> upstream-v0.105.0
    loadWorkerOptions(),
    loadWorkerRunRecord(),
    loadWorkerSessionExport(),
    loadAgentResult(),
    loadWorkerModelControl(),
    loadWorkerEventProjection(),
    loadWorkerRecovery(),
    loadWorkerRunLog(),
    loadToolCallStreamGuard(),
  ]);
  runRecordHelpers = loadedRunRecordHelpers;
  const {
    applyUsage,
    createRunningRecord,
    emptyUsage,
    extractUsageDelta,
    latestRunText,
    updateRunRecord,
    writeRunRecord,
  } = loadedRunRecordHelpers;
  const options = optionHelpers.parseWorkerOptions();
  const sessionExporter = options.sessionExportFile
    ? new sessionExportHelpers.SessionExporter({
        file: options.sessionExportFile,
        sessionId: options.id,
        cwd: options.cwd,
        agentName: options.name,
      })
    : undefined;
  const thinking =
    options.thinking === "off" ||
    options.thinking === "minimal" ||
    options.thinking === "low" ||
    options.thinking === "medium" ||
    options.thinking === "high" ||
    options.thinking === "xhigh" ||
    options.thinking === "max"
      ? options.thinking
      : undefined;
  const task = fs.readFileSync(options.taskFile, "utf8");
  const taskProvenance = fs.existsSync(options.taskFile + ".provenance.json")
    ? copyFabricProvenance(JSON.parse(fs.readFileSync(options.taskFile + ".provenance.json", "utf8"))) : undefined;
  const deliveryDirectory = path.join(path.dirname(options.taskFile), "deliveries");
  fs.mkdirSync(deliveryDirectory, { recursive: true, mode: 0o700 });
  const images = readImages(options.imagesFile);
  const record = createRunningRecord(options, task, thinking, Date.now());
  if (options.runner === "pi") {
    // The manager removes the old status to fence terminal verdicts on relaunch.
    // History is explicitly handed across that boundary, not a Pi resume target.
    if (options.runnerSessionIds?.length) record.runnerSessionIds = [...options.runnerSessionIds];
    // Also accept a matching prior status for a direct worker restart.
    try {
      const prior = JSON.parse(fs.readFileSync(options.statusFile, "utf8")) as AgentRunRecord;
      if (prior.id === options.id) {
        const ids = [...(record.runnerSessionIds ?? []), ...(Array.isArray(prior.runnerSessionIds) ? prior.runnerSessionIds : []), prior.runnerSessionId]
          .filter((id): id is string => typeof id === "string" && Boolean(id.trim()));
        if (ids.length) record.runnerSessionIds = [...new Set(ids)];
      }
    } catch { /* first launch or malformed prior status: observe the live child */ }
  }
  writeRunRecord(options.statusFile, record);
  const emitLifecycle = (
    event: string,
    data?: Record<string, unknown>,
  ): void => {
    try {
      fs.mkdirSync(path.dirname(options.lifecycleFile), { recursive: true });
      fs.appendFileSync(
        options.lifecycleFile,
        JSON.stringify({ version: 1, event, occurredAt: Date.now(), ...(data ? { data } : {}) }) + "\n",
        { encoding: "utf8", mode: 0o600 },
      );
    } catch {
      // Lifecycle telemetry is best-effort and must not fail the child run.
    }
  };
  crashContext = { statusFile: options.statusFile, record };
  process.stdout.write(`[pi-fabric] ${options.name}\n${task}\n\n`);
  fs.mkdirSync(path.dirname(options.logFile), { recursive: true });
  // Other processes tail this file while the run is live (transcript reader,
  // dashboards, preview trees). The previous buffered createWriteStream visibly
  // stalled mid-run — append synchronously so events are durable immediately.
  const runLog = createRunLogWriter((text) => {
    try {
      fs.appendFileSync(options.logFile, text, { encoding: "utf8", mode: 0o600 });
    } catch {
      // Event logging is best-effort and must not fail the child run.
    }
  });
  const appendLog = runLog.raw;
  flushRunLog = runLog.flush;
  const sessionStream =
    options.runner === "claude" && options.sessionFile
      ? fs.createWriteStream(options.sessionFile, { flags: "a", mode: 0o600 })
      : undefined;
  sessionStream?.on("error", () => {});

  const schema = options.schemaFile
    ? fs.readFileSync(options.schemaFile, "utf8")
    : undefined;
  const activationWindow = options.inferenceContext === "activation";
  const activationSession = activationWindow
    ? new ActivationSession(options.sessionFile!, path.dirname(options.statusFile), options.cwd)
    : undefined;
  const recoveryScale = recoveryTimeScale();
  const persistentPiTask = options.runner === "pi" && options.transport === "process" &&
    !options.actorId && !options.actorName && !options.residentStartupProbe;
  const piSessionFile = options.sessionFile ?? (persistentPiTask ? path.join(path.dirname(options.statusFile), "session.jsonl") : undefined);
  let piRetrySdk: string | undefined;
  // Activation windows stay on the CLI, which owns their process-wide native
  // compaction fence. The SDK entry customizes only ordinary task retries.
  if (persistentPiTask && options.inferenceContext !== "activation") {
    const profileModule = import.meta.url.endsWith(".ts") ? "./worker/retry-profile.ts" : "./worker/retry-profile.js";
    const { prepareRetryProfile, resolveRetrySdk } = await import(profileModule) as typeof import("./worker/retry-profile.js");
    if (prepareRetryProfile(options.cwd, path.join(path.dirname(options.statusFile), "pi-agent"), process.env, recoveryScale)) {
      piRetrySdk = resolveRetrySdk(options.piBinary);
      // Opaque/custom launchers are a supported fallback, not a degraded run.
      // Keep profile-selection telemetry separate from result/cleanup warnings.
      if (!piRetrySdk) appendLog(`${JSON.stringify({ type: "fabric_retry_profile", mode: "launcher", reason: "sdk_unavailable", message: "Selected Pi launcher has no discoverable native SDK; preserving its retry settings and canonical auth path (Fabric same-session recovery remains enabled)" })}\n`);
    }
  }
  const piArguments = ["--mode", "rpc"];
  if (piSessionFile) piArguments.push("--session", activationSession?.file ?? piSessionFile);
  else piArguments.push("--no-session");
  if (!options.extensions) piArguments.push("--no-extensions");
  if (options.judgment) piArguments.push("--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-approve", "--no-auto-compaction");
  if (options.residentStartupProbe) {
    // Explicit Fabric -e still loads. Do not execute unrelated profile hooks,
    // skills, prompts, context files or compaction merely to decide on rollback.
    piArguments.push("--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files");
  }
  const activationNonce = activationWindow ? randomUUID() : undefined;
  const residentProbeNonce = options.residentStartupProbe ? randomUUID() : undefined;
  let activationHookPath: string | undefined;
  if (activationWindow) {
    const hookPath = fileURLToPath(new URL(
      import.meta.url.endsWith(".ts") ? "./worker/activation-window.ts" : "./worker/activation-window.js",
      import.meta.url,
    ));
    if (!fs.existsSync(hookPath)) throw new Error("Activation window hook is missing");
    activationHookPath = fs.realpathSync(hookPath);
    // CLI extensions precede discovered extensions. Install the compaction
    // guard before any other hook can start summarization work.
    piArguments.push("-e", hookPath, "--no-auto-compaction");
  }
  if (options.fabricExtensionPath) piArguments.push("-e", options.fabricExtensionPath);
<<<<<<< HEAD
  const deliveryHook = fileURLToPath(new URL(
    import.meta.url.endsWith(".ts") ? "./worker/principal-delivery.ts" : "./worker/principal-delivery.js", import.meta.url));
  piArguments.push("-e", deliveryHook);
  // A session file can contain only a seeded header, which Pi replaces at
  // startup. --no-session has no file at all. Observe the live SessionManager
  // instead of guessing either identity from the launch arguments.
  const sessionIdHook = fileURLToPath(new URL(
    import.meta.url.endsWith(".ts") ? "./worker/session-id.ts" : "./worker/session-id.js", import.meta.url));
  piArguments.push("-e", sessionIdHook);
  // smarty-dev#967: a structured Pi run replies through one tool call, never its final text.
  const replyTool = options.replyTool === true && options.runner === "pi" && schema !== undefined;
  const replyFile = replyTool ? path.join(path.dirname(options.statusFile), "reply.json") : undefined;
  let replyHookPath: string | undefined;
  if (replyTool) {
    const hookPath = fileURLToPath(new URL(
      import.meta.url.endsWith(".ts") ? "./worker/reply-tool.ts" : "./worker/reply-tool.js",
      import.meta.url,
    ));
    if (!fs.existsSync(hookPath)) throw new Error("Reply tool hook is missing");
    replyHookPath = fs.realpathSync(hookPath);
    fs.rmSync(replyFile!, { force: true });
    piArguments.push("-e", hookPath);
  }
  // smarty-dev#2184: every Pi actor run gets the bash timeout, also a native-tool one that runs
  // with --no-extensions (an explicit -e still loads). It comes after Fabric's -e so Fabric's
  // foreground-wait guard judges the caller's own timeout; with Fabric loaded, the second hook finds
  // the timeout set and does nothing.
  if (options.actorId) {
    const hookPath = fileURLToPath(new URL(
      import.meta.url.endsWith(".ts") ? "./guards/actor-bash-hook.ts" : "./guards/actor-bash-hook.js",
      import.meta.url,
    ));
    if (!fs.existsSync(hookPath)) throw new Error("Actor bash timeout hook is missing");
    piArguments.push("-e", hookPath);
  }
  if (options.runner === "pi" && options.routeHeader) {
    const hookPath = fileURLToPath(new URL(
      import.meta.url.endsWith(".ts") ? "./guards/model-route-hook.ts" : "./guards/model-route-hook.js",
      import.meta.url,
    ));
    if (!fs.existsSync(hookPath)) throw new Error("Model route header hook is missing");
    // Explicit -e is loaded even with --no-extensions: attribution is not optional.
    piArguments.push("-e", hookPath);
  }
  const piTools = replyTool ? [...options.tools, "fabric_reply"] : options.tools;
  if (piTools.length > 0) piArguments.push("--tools", piTools.join(","));
=======
  if (options.writePolicy) {
    // The guard loads even with --no-extensions; other runners cannot enforce it.
    if (options.runner !== "pi") throw new Error(`Write confinement requires the Pi runner, not ${options.runner}`);
    const guard = import.meta.url.endsWith(".ts") ? "./agents/write-guard.ts" : "./agents/write-guard.js";
    piArguments.push("-e", fileURLToPath(new URL(guard, import.meta.url)));
  }
  if (options.tools.length > 0) piArguments.push("--tools", options.tools.join(","));
>>>>>>> upstream-v0.105.0
  else piArguments.push("--no-tools"); // explicit empty allowlist => no tools, not Pi defaults
  if (options.model) piArguments.push("--model", options.model);
  if (thinking) piArguments.push("--thinking", thinking);
  if (options.systemPrompt) piArguments.push(options.judgment ? "--system-prompt" : "--append-system-prompt", options.systemPrompt);
  if (schema) {
    piArguments.push(
      "--append-system-prompt",
      replyTool
        ? `Reply by calling the fabric_reply tool exactly once, as your last step. Its arguments are your reply and must match this schema:\n${schema}\nText outside that call is not delivered.`
        : `Your final response must contain only JSON matching this schema, without Markdown fences:\n${schema}`,
    );
  }
  const claudeCli = options.runner === "claude" ? await loadClaudeCli() : undefined;
  const vedaCli = options.runner === "veda" ? await loadVedaCli() : undefined;
  const childArguments =
    options.runner === "claude"
      ? claudeCli!.buildClaudeArguments({
          tools: options.tools,
          extensions: options.extensions,
          // Actors and handoffs already have a session file; one-shot Claude runs opt in explicitly.
          persistentSession: Boolean(options.sessionFile) || options.persistSession === true,
          ...(options.model ? { model: options.model } : {}),
          ...(thinking ? { thinking } : {}),
          ...(options.systemPrompt ? { systemPrompt: options.systemPrompt } : {}),
          ...(schema ? { schema } : {}),
          ...(options.runnerSessionId ? { runnerSessionId: options.runnerSessionId } : {}),
          name: options.name,
        })
      : options.runner === "veda"
        ? vedaCli!.buildVedaArguments({
            backend: options.vedaBackend,
            persona: options.vedaPersona,
            ...(options.model ? { model: options.model } : {}),
            ...(thinking ? { thinking } : {}),
            tools: options.tools,
            // Isolate selection and conversation state per child run so
            // parallel Fabric agents never share Veda session state.
            session: `fabric-${options.id}`,
          })
        : piArguments;
  const childBinary =
    options.runner === "claude"
      ? options.claudeBinary
      : options.runner === "veda"
        ? options.vedaBinary
        : options.piBinary;

  // smarty-dev#1579: agents.nice lowers CPU and IO priority. This dedicated worker lowers its own
  // spawning (main) thread before the fork, so the child, every thread it starts and its tools
  // inherit it. Setting the child's pid after spawn races Linux per-thread priority.
  if (options.nice) {
    applyChildPriority(process.pid, options.nice, (message) =>
      appendLog(`${JSON.stringify({ type: "fabric_priority_error", error: message })}\n`));
  }
  // smarty-dev#2339 F4: a nested actor gets its own default, never its parent's override.
  const childEnvironment = applyTaskReturnAddress(
    options.actorId ? { ...process.env } : taskAgentEnvironment(), process.argv.slice(2),
  );
  delete childEnvironment.PI_FABRIC_ACTOR_BASH_TIMEOUT_S;
  // A task child has its own identity and reply contract, not its actor parent's.
  for (const key of ["PI_FABRIC_ACTOR_ID", "PI_FABRIC_ACTOR_NAME", "PI_FABRIC_ACTOR_SESSION_FILE",
    "PI_FABRIC_REPLY_SCHEMA_FILE", "PI_FABRIC_REPLY_FILE", "PI_FABRIC_REPLY_HOOK",
    "PI_FABRIC_SPAWNER_ID", "PI_FABRIC_SPAWNER_KIND", "PI_FABRIC_SPAWNER_RUN"]) {
    delete childEnvironment[key];
  }
  // A nested explicit-model task must never inherit its parent's route attribution.
  delete childEnvironment.PI_FABRIC_ROUTE_HEADER;
  if (options.routeHeader) childEnvironment.PI_FABRIC_ROUTE_HEADER = options.routeHeader;
  if (options.actorId && options.bashTimeoutSeconds !== undefined &&
    Number.isInteger(options.bashTimeoutSeconds) && options.bashTimeoutSeconds >= 0) {
    childEnvironment.PI_FABRIC_ACTOR_BASH_TIMEOUT_S = String(options.bashTimeoutSeconds);
  }
  // smarty-dev#2088: ordinary process children write as task agents, not as their parent's role.
  // The fleet governor derives the lane from cwd; explicit actors keep their own role environment.
  const taskEntryPath = fileURLToPath(new URL(import.meta.url.endsWith(".ts") ? "./worker/task-entry.ts" : "./worker/task-entry.js", import.meta.url));
  const spawnChild = (): ChildProcess => spawnCli(piRetrySdk ? taskEntryPath : childBinary,
    piRetrySdk ? [piRetrySdk, String(recoveryScale), ...childArguments] : childArguments, {
    cwd: options.cwd,
    detached: process.platform !== "win32",
    env: {
      ...childEnvironment,
      ...(options.inheritedSessionPins && options.inheritedSessionPins.length > 0
        ? {
            PI_MULTIPROVIDER_SESSION_PINS: JSON.stringify(options.inheritedSessionPins),
          }
        : {}),
      // Preserve the selected launcher for Fabric loaded inside this child.
      // Herdr's server environment need not contain the owner's binary pin.
      ...(options.runner === "pi" ? { PI_FABRIC_PI_BINARY: options.piBinary } : {}),
      PI_FABRIC_RESIDENT_PROBE_WORKER_PID: residentProbeNonce ? String(process.pid) : "",
      PI_FABRIC_RESIDENT_PROBE_NONCE: residentProbeNonce ?? "",
      PI_FABRIC_ACTIVATION_WORKER_PID: activationWindow ? String(process.pid) : "",
      PI_FABRIC_ACTIVATION_NONCE: activationNonce ?? "",
      PI_FABRIC_ACTIVATION_HOOK: activationHookPath ?? "",
      PI_FABRIC_DELIVERY_DIR: deliveryDirectory,
      // Own run only, never the shared parent/nested run root.
      PI_FABRIC_AGENT_RUN_DIR: path.dirname(options.statusFile),
      PI_FABRIC_DEPTH: String(options.depth),
      PI_FABRIC_PARENT_RUN: options.id,
      // A child Fabric intersects its own thinking.bounds with these.
      ...(options.thinkingBounds ? { PI_FABRIC_THINKING_BOUNDS: options.thinkingBounds } : {}),
      PI_FABRIC_AGENT_NAME: options.name,
      ...(options.spawner ? {
        PI_FABRIC_SPAWNER_ID: options.spawner.id,
        PI_FABRIC_SPAWNER_KIND: options.spawner.kind,
        PI_FABRIC_SPAWNER_RUN: options.spawner.runId ?? "",
      } : {}),
      ...(options.mainAgentId ? { PI_FABRIC_MAIN_AGENT_ID: options.mainAgentId } : {}),
      ...(options.fabricSessionId ? { PI_FABRIC_SESSION_ID: options.fabricSessionId } : {}),
      PI_FABRIC_GRANTED_RISKS: options.grantedRisks.join(","),
      PI_FABRIC_FULL_CODE_MODE: String(options.fullCodeMode),
      // Never leak an ambient native-kernel selector into a non-Fabric runner.
      PI_FABRIC_KERNEL: options.kernel,
      PI_FABRIC_PYTHON_RUNTIME: options.pythonRuntime,
      // Native tool allowlist for nested-call enforcement: full-code children
      // reach pi.* through fabric_exec, which is not gated by the --tools
      // allowlist, so both Pi and captured-tool providers enforce this
      // child-side allowlist before preparation, discovery, and invocation.
      PI_FABRIC_TOOL_ALLOWLIST: JSON.stringify(options.tools),
      ...(replyTool
        ? { PI_FABRIC_REPLY_SCHEMA_FILE: options.schemaFile!, PI_FABRIC_REPLY_FILE: replyFile!, PI_FABRIC_REPLY_HOOK: replyHookPath! }
        : {}),
      ...(options.actorId ? { PI_FABRIC_ACTOR_ID: options.actorId } : {}),
      ...(options.actorName ? { PI_FABRIC_ACTOR_NAME: options.actorName } : {}),
      ...(options.actorId && options.sessionFile ? { PI_FABRIC_ACTOR_SESSION_FILE: options.sessionFile } : {}),
      PI_FABRIC_CAPABILITY_REQUIREMENTS: JSON.stringify(
        options.capabilityRequirements ?? [],
      ),
      PI_FABRIC_CAPABILITY_DIGEST: options.capabilityDigest ?? "",
      ...(options.meshRoot ? { PI_FABRIC_MESH_ROOT: options.meshRoot } : {}),
      ...(options.projectRoot ? { PI_FABRIC_PROJECT_ROOT: options.projectRoot } : {}),
      ...(options.ownerHostId ? { PI_FABRIC_OWNER_HOST_ID: options.ownerHostId } : {}),
      ...(options.ownerIdentityId
        ? { PI_FABRIC_OWNER_IDENTITY_ID: options.ownerIdentityId }
        : {}),
      ...(options.runRoot ? { PI_FABRIC_RUN_ROOT: options.runRoot } : {}),
      // Supported child contract (docs/agents.md "Child environment contract").
      ...(options.lineage ? { PI_FABRIC_LINEAGE: options.lineage } : {}),
      ...(options.writePolicy ? { PI_FABRIC_WRITE_POLICY: options.writePolicy } : {}),
      // Exactly the manager's derived scope; undefined drops an inherited value.
      PI_FABRIC_SCOPE: options.scope,
      PI_FABRIC_SCOPE_FILE: undefined,
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  let child = spawnChild();
  let childExited = false;
  let piControlLive = false;
  let nativeActivity = false;
  let deferredFollowUpSettle = false;
  let replayAfterStart = false;
  let retainedPiQueues: { steering: string[]; followUp: string[] } | undefined;
  let providerResumeAttempts = 0;
  let resumePrompt = false;
  let providerAborted = false;
  let cancelResumeWait: (() => void) | undefined;
  let stderr = "";
  let outputBuffer = "";
  // Veda emits a single JSON document on stdout (progress goes to stderr, and
  // with --json progress is suppressed entirely). Buffer it raw and parse it
  // once the child closes instead of treating stdout as NDJSON lines.
  let vedaOutput = "";
  let vedaParsed: Record<string, unknown> | undefined;
  let eventProjection = options.runner === "pi" ? new PiEventProjection() : undefined;
  let outputDecoder = new StringDecoder("utf8");
  let stderrDecoder = new StringDecoder("utf8");
  let terminalStatus: AgentRunStatus | undefined;
  // A dropped assistant message_end may hold the run's answer or its error.
  // Until a later assistant message_end is processed, the run must not
  // complete with the earlier (stale) text.
  // smarty-dev#1907.
  let lostResult: string | undefined;
  let terminalError: string | undefined;
  let sawAgentError = false;
  let retryPending = false;
  let piSettledSuccessfully = false;
  let hasFinalText = false;
  let hasFinalResult = false;
  let producedFinalAnswer = false;

  const update = (): void => updateRunRecord(options.statusFile, record);
  // agents.childQuestions "route": child dialogs wait for a parent ui_response.
  const questionRelay = options.childQuestionTimeoutMs !== undefined && options.runner === "pi" && options.steerFile
    ? new (await loadWorkerQuestions()).ChildQuestionRelay(options.childQuestionTimeoutMs, {
        emit: (question) => emitLifecycle("question", { ...question }),
        send: (frame) => child.stdin?.write(`${JSON.stringify(frame)}\n`),
        blocked: (since) => {
          if (since === undefined) delete record.blockedOn;
          else record.blockedOn = { since };
          update();
        },
      })
    : undefined;
  let killTimer: NodeJS.Timeout | undefined;
  let closeTimer: NodeJS.Timeout | undefined;
  const recoveryWatchdog = new PiRecoveryWatchdog((error) => failStalledChild(error), recoveryScale);
  const toolCallStreamGuard = new ToolCallStreamGuard((error) => {
    if (terminalStatus) return;
    terminalStatus = "failed";
    terminalError = error.message;
    record.error = error.message;
    record.errorCode = error.code;
    update();
    appendLog(`${JSON.stringify({ type: "fabric_runaway_error", errorCode: error.code,
      error: error.message, model: error.model, effort: error.effort, bytes: error.bytes,
      elapsedMs: error.elapsedMs, contentIndex: error.contentIndex })}\n`);
    process.stderr.write(`${error.name}: ${error.message}\n`);
    killChild();
  }, () => ({ model: record.model ?? "unknown", effort: record.thinking ?? "unknown" }));
  const killChild = (): void => {
    recoveryWatchdog.dispose();
    toolCallStreamGuard.dispose();
    if (closeTimer) clearTimeout(closeTimer);
    cancelResumeWait?.();
    if (!childExited) {
      terminateChild(child, "SIGTERM");
      killTimer ??= setTimeout(() => terminateChild(child, "SIGKILL"), KILL_GRACE_MS);
      killTimer.unref();
    }
    child.stdin?.end();
  };
  const failStalledChild = (error: string): void => {
    if (terminalStatus) return;
    terminalStatus = "failed";
    terminalError = error;
    record.error = error;
    update();
    appendLog(`${JSON.stringify({ type: "fabric_recovery_error", error })}\n`);
    killChild();
  };
  const closeChild = (): void => {
    piControlLive = false;
    child.stdin?.end();
    recoveryWatchdog.suspend();
    if (closeTimer || killTimer) return;
    // RPC EOF requests disposal, but a stuck extension can prevent process exit.
    closeTimer = setTimeout(() => {
      const error = `${terminalError ?? "Child Pi settled"}; child did not exit after stdin closed for ${KILL_GRACE_MS}ms`;
      // fabric_reply writes after assistant message_end; inspect the durable
      // reply now too. Post-drain reply/schema validation remains authoritative.
      hasFinalResult = Boolean(hasFinalText || (replyFile && fs.existsSync(replyFile)));
      if (piSettledSuccessfully && hasFinalResult && modelControl.ready && !terminalStatus &&
          !terminalError && !sawAgentError && !lostResult) {
        const warning = `${error}; preserving final result and terminating child`;
        record.warnings = [...(record.warnings ?? []), warning].slice(-20);
        appendLog(`${JSON.stringify({ type: "worker_warning", warning })}\n`);
        process.stderr.write(`[pi-fabric] ${warning}\n`);
        // Persist the result and warning BEFORE signalling the owned group.
        // Keep the public record running until close drains the streams and
        // reply/schema validation finishes; terminal records can be collected
        // immediately by the manager. Forced exit must not erase this result.
        update();
        terminalStatus = "completed";
        killChild();
      } else {
        failStalledChild(error);
      }
    }, KILL_GRACE_MS);
    closeTimer.unref();
  };

  // Auth checks and model_select hooks can be slow under concurrent launches.
  // Startup and admission share the overall run timeout below, not a shorter cap.
  const sendPiDelivery = (message: string, provenance: FabricTurnProvenance | undefined, delivery: "steer" | "followUp", images?: readonly ImageContent[]): void => {
    if (!provenance?.principal) {
      child.stdin?.write(JSON.stringify({ type: delivery === "steer" ? "steer" : "follow_up", message }) + "\n");
      return;
    }
    const id = randomUUID();
    fs.writeFileSync(path.join(deliveryDirectory, id + ".json"), JSON.stringify({ message, provenance, delivery, images }), { mode: 0o600 });
    child.stdin?.write(JSON.stringify({ type: "prompt", message: "/fabric-delivery " + id, streamingBehavior: delivery }) + "\n");
  };
  const hasUnsettledFollowUps = (): boolean => {
    const directory = path.join(path.dirname(deliveryDirectory), "follow-ups");
    try {
      return fs.readdirSync(directory).some(name => {
        if (!/^[0-9a-f-]{36}\.json$/.test(name)) return false;
        const state = followUpState(path.join(directory, name));
        return state !== "delivered" && state !== "cancelled";
      });
    } catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  };
  const replayTrackedFollowUps = (): void => {
    // The old native child's held map and input preflight disappear on resume.
    // Its worker-owned envelopes do not. Reissue only unsettled tracked IDs;
    // the replacement hook deduplicates command/steer-log replay by that ID.
    for (const name of fs.readdirSync(deliveryDirectory)) {
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$/.test(name)) continue;
      const file = path.join(deliveryDirectory, name);
      try {
        const item = JSON.parse(fs.readFileSync(file, "utf8"));
        const id = name.slice(0, -5);
        if (item.followUpId !== id || item.delivery !== "followUp") continue;
        const receipt = followUpFile(path.dirname(deliveryDirectory), id);
        if (followUpState(receipt) !== "queued") { releaseFollowUpPayload(receipt); continue; }
        child.stdin?.write(JSON.stringify({ type: "prompt", message: "/fabric-delivery " + id, streamingBehavior: "followUp" }) + "\n");
      } catch { /* Retain malformed/uncertain envelopes; never invent delivery. */ }
    }
  };
  let activationWindowReady = false;
  let residentProbeReady = false;
  // Optional worker-only edge: never load native estimation in Main registration.
  const admissionModule = options.runner === "pi" && options.actorId && !activationWindow
    ? await import(import.meta.url.endsWith(".ts") ? "./worker/context-admission.ts" : "./worker/context-admission.js") as typeof import("./worker/context-admission.js")
    : undefined;
  const estimateActorInput = admissionModule
    ? await admissionModule.loadActorInputEstimator(options.piBinary) : undefined;
  if (admissionModule && !estimateActorInput) appendLog(`${JSON.stringify({ type: "fabric_context_admission", mode: "launcher", reason: "native_estimator_unavailable" })}\n`);
  let contextAdmission: InstanceType<NonNullable<typeof admissionModule>["ActorContextAdmission"]> | undefined;
  const dispatchPiPrompt = (): void => {
    if (terminalStatus) return;
    const message = resumePrompt ? "Continue the task from the existing session. Do not repeat completed work." : task;
    if (taskProvenance?.principal) sendPiDelivery(message, taskProvenance, "steer", resumePrompt ? [] : images);
    else child.stdin?.write(`${JSON.stringify({ type: "prompt", message, ...(!resumePrompt && images.length > 0 ? { images } : {}) })}\n`);
    // ctx.isIdle() remains true during asynchronous prompt preflight. Do not
    // issue a second prompt in that gap; wait for the native agent_start.
    replayAfterStart = true;
  };
  const createModelControl = (): InstanceType<typeof PiModelControl> => new PiModelControl(options.id, options.model, thinking, {
    send(frame) {
      if (terminalStatus) return;
      child.stdin?.write(`${JSON.stringify(frame)}\n`);
    },
    observed(model) {
      if (record.model === model) return;
      record.model = model;
      update();
    },
    admitted(model, effectiveThinking) {
      if (terminalStatus) return;
      if ((options.routeHeader || options.judgment) && (!thinking || !["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(effectiveThinking ?? "") || effectiveThinking !== thinking)) {
        modelControl.fail("routed effort pin was not admitted; task was not sent");
        return;
      }
      if (activationWindow && !activationWindowReady) {
        modelControl.fail("activation window hook did not acknowledge readiness; task was not sent");
        return;
      }
      if (model) record.model = model;
      if (["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(effectiveThinking ?? "")) {
        record.thinking = effectiveThinking as NonNullable<AgentRunRecord["thinking"]>;
      }
      if (options.routeHeader) {
        if (record.model) record.admittedModel = record.model;
        if (effectiveThinking) record.admittedThinking = effectiveThinking as NonNullable<AgentRunRecord["thinking"]>;
      }
      update();
      // The launcher authorizes this harmless isolated startup test. Exercise
      // the real worker/Pi/model/extension path, but never prompt a business actor
      // (or invoke inference/tools) merely to decide whether rollback is safe.
      if (options.residentStartupProbe) {
        if (!residentProbeReady) {
          modelControl.fail("resident Fabric extension did not acknowledge loaded-generation readiness");
          return;
        }
        terminalStatus = "completed";
        record.text = "resident worker startup verified";
        appendLog(`${JSON.stringify({ type: "fabric_resident_worker_ready", workerPath: fileURLToPath(import.meta.url), fabricExtensionPath: options.fabricExtensionPath })}\n`);
        closeChild();
        return;
      }
      if (admissionModule && estimateActorInput) {
        contextAdmission = new admissionModule.ActorContextAdmission(options.id,
          resumePrompt ? "Continue the task from the existing session. Do not repeat completed work." : task,
          options.systemPrompt ?? "", estimateActorInput, {
            send(frame) { if (!terminalStatus) child.stdin?.write(`${JSON.stringify(frame)}\n`); },
            ready: dispatchPiPrompt,
            fail(error) { modelControl.fail(error); },
            compact(tokens, contextWindow, reason) {
              appendLog(`${JSON.stringify({ type: "fabric_context_compaction", phase: "before_dispatch", tokens, contextWindow, reason })}\n`);
            },
          });
        contextAdmission.start();
      } else dispatchPiPrompt();
    },
    fail(error) {
      if (terminalStatus) return;
      terminalStatus = "failed";
      terminalError = error;
      record.error = error;
      update();
      appendLog(`${JSON.stringify({ type: "fabric_model_error", requestedModel: options.model, model: record.model, error })}\n`);
      killChild();
    },
<<<<<<< HEAD
  }, activationWindow, options.residentStartupProbe === true, Boolean(options.routeHeader));
  let modelControl = createModelControl();
=======
  }, options.modelAdmission);
>>>>>>> upstream-v0.105.0

  // Attributed token telemetry. Every usage-bearing child event emits one
  // tokens.usage lifecycle entry identified by this run/actor/runner/depth.
  // The manager drains these alongside the pi.* lifecycle stream and appends
  // them to the budget ledger, replacing the old per-settle flat attribution.
  const lastEmittedUsage = emptyUsage();
  const emitTokenUsage = (
    delta?: {
      input: number;
      output: number;
      cacheRead: number;
      cacheWrite: number;
      cost: number;
    },
    attribution?: { model?: string | undefined; provider?: string | undefined },
    journalMessage?: unknown,
  ): void => {
    const snapshot = record.usage;
    if (
      snapshot.input === lastEmittedUsage.input &&
      snapshot.output === lastEmittedUsage.output &&
      snapshot.cacheRead === lastEmittedUsage.cacheRead &&
      snapshot.cacheWrite === lastEmittedUsage.cacheWrite &&
      snapshot.cost === lastEmittedUsage.cost
    ) {
      return;
    }
    emitLifecycle("tokens.usage", {
      runId: options.id,
      name: options.name,
      runner: options.runner,
      depth: options.depth,
      ...(options.actorId ? { actorId: options.actorId } : {}),
      ...(options.actorName ? { actorName: options.actorName } : {}),
      cumulativeTokens:
        snapshot.input + snapshot.output + snapshot.cacheRead + snapshot.cacheWrite,
      input: delta?.input ?? 0,
      output: delta?.output ?? 0,
      cacheRead: delta?.cacheRead ?? 0,
      cacheWrite: delta?.cacheWrite ?? 0,
      cost: delta?.cost ?? snapshot.cost,
    });
    // Mirror the emitted payload into the pi-format usage export (when the
    // host enabled agents.sessionExport) so tokscale/ccusage can attribute
    // subagent tokens and cost. Export intentionally never writes content.
    sessionExporter?.push(
      {
        input: delta?.input ?? 0,
        output: delta?.output ?? 0,
        cacheRead: delta?.cacheRead ?? 0,
        cacheWrite: delta?.cacheWrite ?? 0,
        cost: delta?.cost ?? snapshot.cost,
      },
      attribution?.model ?? record.model ?? options.model,
      attribution?.provider,
      undefined,
      // Only actor journals are a durable accounting source. Ordinary task
      // sessions live in the disposable run directory, so their exports must
      // remain countable. Activation turns are retained under sessionFile, not
      // the isolated child session that retain() removes at settlement.
      options.actorId && options.sessionFile && journalMessage
        ? sessionExportHelpers.journalTurnId(options.sessionFile, journalMessage)
        : undefined,
    );
    lastEmittedUsage.input = snapshot.input;
    lastEmittedUsage.output = snapshot.output;
    lastEmittedUsage.cacheRead = snapshot.cacheRead;
    lastEmittedUsage.cacheWrite = snapshot.cacheWrite;
    lastEmittedUsage.cost = snapshot.cost;
  };

  const { ChildCompactControl } = await loadCompactControl();
  const createCompactControl = (): InstanceType<typeof ChildCompactControl> => new ChildCompactControl(options.id, {
    send(frame) {
      if (!child.stdin || child.stdin.writableEnded || child.stdin.destroyed) {
        throw new Error("Child Pi stdin closed before compaction could start");
      }
      child.stdin.write(`${JSON.stringify(frame)}\n`);
    },
    close: closeChild,
    update(status) {
      record.compaction = status;
      update();
    },
  });

  let compactControl = createCompactControl();

  // Preemptive per-child token guard. timeoutMs bounds wall time and budgetUsd
  // bounds cost, but a single runaway child can still blow its own context
  // before Pi core compacts. When maxTokens is set and the child's cumulative
  // token usage crosses it, terminate the child like a timeout so the run
  // settles with a terminal status instead of burning to the hour deadline.
  // The error string is model-facing: the parent agent reads it verbatim, so it
  // must name the config key and remedy, and front-load the numbers before TUI
  // truncation.
  const enforceTokenLimit = (): void => {
    if (terminalStatus || !options.maxTokens || options.maxTokens <= 0) return;
    const total =
      record.usage.input +
      record.usage.output +
      record.usage.cacheRead +
      record.usage.cacheWrite;
    if (total <= options.maxTokens) return;
    terminalStatus = "timed_out";
    terminalError =
      `Fabric token limit reached: ${total} tokens (limit ${options.maxTokens} set by agents.maxTokensPerChild); terminating child. ` +
      `The count is cumulative across the run and includes cache reads/writes, so it grows every turn; ` +
      `raise or disable agents.maxTokensPerChild in /fabric settings (0 disables), or split the task into smaller agent runs.`;
    killChild();
  };

  type ClaudeInputKind = "initial" | "steer" | "follow_up";
  const claudeTools = new Map<string, string>();
  const claudeCompletedUsage = emptyUsage();
  const claudeCurrentUsage = emptyUsage();
  const syncClaudeUsage = (): void => {
    record.usage = {
      input: claudeCompletedUsage.input + claudeCurrentUsage.input,
      output: claudeCompletedUsage.output + claudeCurrentUsage.output,
      cacheRead: claudeCompletedUsage.cacheRead + claudeCurrentUsage.cacheRead,
      cacheWrite: claudeCompletedUsage.cacheWrite + claudeCurrentUsage.cacheWrite,
      cost: claudeCompletedUsage.cost,
    };
  };

  const claudeSentInputs: Array<{ kind: ClaudeInputKind; message: string }> = [];
  const claudeSteering: string[] = [];
  const claudeFollowUps: string[] = [];
  let claudeSteeringMode: "all" | "one-at-a-time" = "one-at-a-time";
  let claudeFollowUpMode: "all" | "one-at-a-time" = "one-at-a-time";
  let claudeCanFollowUp = false;
  let claudeResultSeen = false;
  const enqueueClaudeControl = (queue: string[], message: string): void => {
    const pendingInputs = claudeSentInputs.length + claudeSteering.length + claudeFollowUps.length;
    if (pendingInputs >= MAX_CLAUDE_PENDING_INPUTS) return;
    queue.push(message);
  };
  let claudeCloseTimer: NodeJS.Timeout | undefined;

  const updateClaudeQueue = (): void => {
    const sentSteering = claudeSentInputs
      .filter((entry) => entry.kind === "steer")
      .map((entry) => entry.message);
    const sentFollowUps = claudeSentInputs
      .filter((entry) => entry.kind === "follow_up")
      .map((entry) => entry.message);
    record.pendingMessages = {
      steering: [...sentSteering, ...claudeSteering],
      followUp: [...sentFollowUps, ...claudeFollowUps],
    };
    update();
  };

  const writeClaudeInput = (
    kind: ClaudeInputKind,
    message: string,
    inputImages: readonly ImageContent[] = [],
  ): void => {
    if (claudeCloseTimer) clearTimeout(claudeCloseTimer);
    claudeCloseTimer = undefined;
    if (!child.stdin || child.stdin.writableEnded || child.stdin.destroyed) return;
    claudeSentInputs.push({ kind, message });
    if (kind === "follow_up") claudeCanFollowUp = false;
    child.stdin.write(
      `${JSON.stringify(claudeCli!.claudeUserMessage(message, inputImages))}\n`,
    );
    updateClaudeQueue();
  };

  const flushClaudeSteering = (): void => {
    if (claudeSteering.length === 0) return;
    const alreadySent = claudeSentInputs.some((entry) => entry.kind === "steer");
    if (claudeSteeringMode === "one-at-a-time" && alreadySent) return;
    const count = claudeSteeringMode === "all" ? claudeSteering.length : 1;
    for (const message of claudeSteering.splice(0, count)) {
      writeClaudeInput("steer", message);
    }
  };

  const flushClaudeFollowUps = (): void => {
    if (claudeFollowUps.length === 0 || claudeSteering.length > 0) return;
    if (claudeSentInputs.some((entry) => entry.kind === "steer")) return;
    const alreadySent = claudeSentInputs.some((entry) => entry.kind === "follow_up");
    if (claudeFollowUpMode === "one-at-a-time" && alreadySent) return;
    const count = claudeFollowUpMode === "all" ? claudeFollowUps.length : 1;
    for (const message of claudeFollowUps.splice(0, count)) {
      writeClaudeInput("follow_up", message);
    }
  };

  const scheduleClaudeClose = (): void => {
    if (claudeCloseTimer || terminalStatus) return;
    claudeCloseTimer = setTimeout(() => {
      claudeCloseTimer = undefined;
      if (
        claudeSentInputs.length === 0 &&
        claudeSteering.length === 0 &&
        claudeFollowUps.length === 0
      ) {
        child.stdin?.end();
      }
    }, 300);
    claudeCloseTimer.unref();
  };

  const processClaudeEvent = (event: Record<string, unknown>): void => {
    if (event.type === "system" && event.subtype === "init") {
      const sessionId = stringField(event.session_id);
      if (sessionId) record.runnerSessionId = sessionId;
      const model = stringField(event.model);
      if (model && !record.model) record.model = model;
      update();
      return;
    }
    if (event.type === "assistant") {
      const message = event.message;
      if (typeof message !== "object" || message === null || Array.isArray(message)) return;
      const assistant = message as Record<string, unknown>;
      const text = extractText(assistant);
      if (text) {
        record.text = latestRunText(text);
        process.stdout.write(`\n${text}\n`);
      }
      const content = assistant.content;
      if (Array.isArray(content)) {
        for (const block of content) {
          if (typeof block !== "object" || block === null || Array.isArray(block)) continue;
          const part = block as Record<string, unknown>;
          if (part.type !== "tool_use") continue;
          const id = stringField(part.id);
          const name = stringField(part.name);
          if (!id || !name || claudeTools.has(id)) continue;
          claudeTools.set(id, name);
          while (claudeTools.size > MAX_CLAUDE_PENDING_TOOLS) {
            const oldestToolId = claudeTools.keys().next().value;
            if (oldestToolId === undefined) break;
            claudeTools.delete(oldestToolId);
          }
          record.toolCalls++;
          record.currentTool = name;
          process.stdout.write(`→ ${name}\n`);
        }
      }
      const usage = assistant.usage;
      if (typeof usage === "object" && usage !== null && !Array.isArray(usage)) {
        const values = usage as Record<string, unknown>;
        const delta = {
          input: numberField(values.input_tokens),
          output: numberField(values.output_tokens),
          cacheRead: numberField(values.cache_read_input_tokens),
          cacheWrite: numberField(values.cache_creation_input_tokens),
          cost: 0,
        };
        claudeCurrentUsage.input += delta.input;
        claudeCurrentUsage.output += delta.output;
        claudeCurrentUsage.cacheRead += delta.cacheRead;
        claudeCurrentUsage.cacheWrite += delta.cacheWrite;
        syncClaudeUsage();
        emitTokenUsage(delta, { model: record.model ?? stringField(event.model) });
      }
      if (typeof event.error === "string") {
        sawAgentError = true;
        terminalError = event.error;
      }
      enforceTokenLimit();
      update();
      return;
    }
    if (event.type === "user") {
      const message = event.message;
      if (typeof message !== "object" || message === null || Array.isArray(message)) return;
      const content = (message as Record<string, unknown>).content;
      if (!Array.isArray(content)) return;
      for (const block of content) {
        if (typeof block !== "object" || block === null || Array.isArray(block)) continue;
        const part = block as Record<string, unknown>;
        if (part.type !== "tool_result") continue;
        const id = stringField(part.tool_use_id);
        if (id) claudeTools.delete(id);
      }
      const current = [...claudeTools.values()].at(-1);
      if (current) record.currentTool = current;
      else delete record.currentTool;
      update();
      return;
    }
    if (event.type === "stream_event") {
      const streamEvent = event.event;
      if (typeof streamEvent !== "object" || streamEvent === null || Array.isArray(streamEvent)) return;
      const stream = streamEvent as Record<string, unknown>;
      if (stream.type !== "content_block_start") return;
      const contentBlock = stream.content_block;
      if (typeof contentBlock !== "object" || contentBlock === null || Array.isArray(contentBlock)) return;
      const block = contentBlock as Record<string, unknown>;
      const name = stringField(block.name);
      if (block.type === "tool_use" && name) {
        record.currentTool = name;
        update();
      }
      return;
    }
    if (event.type !== "result") return;
    claudeResultSeen = true;
    const sessionId = stringField(event.session_id);
    if (sessionId) record.runnerSessionId = sessionId;
    const resultText = typeof event.result === "string" ? event.result : "";
    if (resultText) record.text = latestRunText(resultText);
    if (event.structured_output !== undefined) record.value = event.structured_output;
    record.turns += Math.max(0, Math.floor(numberField(event.num_turns)));
    const resultUsage =
      typeof event.usage === "object" && event.usage !== null && !Array.isArray(event.usage)
        ? (event.usage as Record<string, unknown>)
        : undefined;
    // The result frame supersedes the assistant-frame stream for this turn:
    // fold any unreported assistant tokens into the result delta so cumulative
    // attribution stays exact without double-counting assistant emissions.
    const resultDelta = resultUsage
      ? {
          input: numberField(resultUsage.input_tokens) - claudeCurrentUsage.input,
          output: numberField(resultUsage.output_tokens) - claudeCurrentUsage.output,
          cacheRead:
            numberField(resultUsage.cache_read_input_tokens) - claudeCurrentUsage.cacheRead,
          cacheWrite:
            numberField(resultUsage.cache_creation_input_tokens) - claudeCurrentUsage.cacheWrite,
          cost: Math.max(0, numberField(event.total_cost_usd)),
        }
      : { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: Math.max(0, numberField(event.total_cost_usd)) };
    claudeCompletedUsage.input += resultUsage
      ? numberField(resultUsage.input_tokens)
      : claudeCurrentUsage.input;
    claudeCompletedUsage.output += resultUsage
      ? numberField(resultUsage.output_tokens)
      : claudeCurrentUsage.output;
    claudeCompletedUsage.cacheRead += resultUsage
      ? numberField(resultUsage.cache_read_input_tokens)
      : claudeCurrentUsage.cacheRead;
    claudeCompletedUsage.cacheWrite += resultUsage
      ? numberField(resultUsage.cache_creation_input_tokens)
      : claudeCurrentUsage.cacheWrite;
    claudeCompletedUsage.cost += Math.max(0, numberField(event.total_cost_usd));
    claudeCurrentUsage.input = 0;
    claudeCurrentUsage.output = 0;
    claudeCurrentUsage.cacheRead = 0;
    claudeCurrentUsage.cacheWrite = 0;
    syncClaudeUsage();
    emitTokenUsage(resultDelta, { model: record.model ?? stringField(event.model) });
    enforceTokenLimit();
    const failed = event.is_error === true || event.subtype !== "success";
    if (failed) {
      sawAgentError = true;
      const errors = Array.isArray(event.errors)
        ? event.errors.filter((value): value is string => typeof value === "string").join(" · ")
        : "";
      terminalError = errors || resultText || `Claude returned ${String(event.subtype ?? "an error")}`;
      claudeSteering.splice(0);
      claudeFollowUps.splice(0);
    } else {
      sawAgentError = false;
      if (!terminalStatus) terminalError = undefined;
    }
    if (failed || terminalStatus) claudeSentInputs.splice(0);
    else claudeSentInputs.shift();
    claudeCanFollowUp = !failed && !terminalStatus;
    delete record.currentTool;
    updateClaudeQueue();
    if (failed || terminalStatus) {
      child.stdin?.end();
      return;
    }
    flushClaudeSteering();
    if (claudeSentInputs.length === 0 && claudeSteering.length === 0) {
      flushClaudeFollowUps();
    }
    if (
      claudeSentInputs.length === 0 &&
      claudeSteering.length === 0 &&
      claudeFollowUps.length === 0
    ) {
      scheduleClaudeClose();
    }
  };

  const processEvent = (line: string): void => {
    if (process.env.PI_FABRIC_INJECT_CRASH === "stream") throw new Error("simulated stream crash");
    if (!line.trim()) return;
    sessionStream?.write(`${line}\n`);
    let event: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(line);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        runLog.event(line, undefined);
        return;
      }
      event = parsed as Record<string, unknown>;
    } catch {
      runLog.event(line, undefined);
      return;
    }
    runLog.event(line, event);
    if (options.runner === "claude") {
      processClaudeEvent(event);
      return;
    }
    if (event.type === "fabric_runner_session") {
      const sessionId = stringField(event.sessionId);
      if (event.runId === options.id && sessionId && record.runnerSessionId !== sessionId) {
        record.runnerSessionId = sessionId;
        record.runnerSessionIds ??= [];
        if (!record.runnerSessionIds.includes(sessionId)) record.runnerSessionIds.push(sessionId);
        update();
      }
      return;
    }
    compactControl.observe(event);
    if (activationWindow && event.type === "fabric_activation_window_ready") {
      if (event.runId === options.id && event.nonce === activationNonce &&
          event.policy === "activation" && event.protocol === 1 && event.hook === activationHookPath && !activationWindowReady) {
        activationWindowReady = true;
      } else modelControl.fail("activation window readiness does not match the selected run/policy/hook");
      return;
    }
    if (residentProbeNonce && event.type === "fabric_resident_extension_ready") {
      if (event.runId === options.id && event.nonce === residentProbeNonce &&
          event.protocol === 1 && event.extension === options.fabricExtensionPath && !residentProbeReady) {
        residentProbeReady = true;
      } else modelControl.fail("resident readiness does not match the selected run/extension/nonce");
      return;
    }
    if (modelControl.observe(event)) return;
    if (contextAdmission?.observe(event)) return;
    if (event.type === "message_start" || event.type === "message_update") {
      const message = event.message;
      if (typeof message === "object" && message !== null && !Array.isArray(message)) {
        modelControl.observeAssistant(message as Record<string, unknown>);
      }
    }
    // Context refusal is deterministic for an actor's admitted input. Native
    // retries cannot make it fit; stop at the first structured failure, also
    // for full-history actors or a preflight extension that reports an error.
    const eventMessage = event.type === "message_end" && typeof event.message === "object" && event.message !== null
      ? event.message as Record<string, unknown> : undefined;
    const contextError = eventMessage?.stopReason === "error" ? stringField(eventMessage.errorMessage)
      : event.type === "extension_error" || (event.type === "response" && event.success === false) ? stringField(event.error)
      : event.type === "auto_retry_start" ? stringField(event.errorMessage) : undefined;
    if (options.actorId && !terminalStatus && contextError && /Context exceeds window:/i.test(contextError)) {
      terminalStatus = "failed";
      terminalError = contextError;
      record.error = contextError;
      sawAgentError = true;
      update();
      killChild();
      return;
    }
    if (!terminalStatus) {
      toolCallStreamGuard.observe(event);
      if (terminalStatus) return;
    }
    if (event.type === "message_update" && !terminalStatus) {
      const delta = event.assistantMessageEvent as Record<string, unknown> | undefined;
      if (delta && ["text_delta", "thinking_delta", "toolcall_delta"].includes(String(delta.type)) &&
          typeof delta.delta === "string" && delta.delta.length > 0) {
        if (!record.inferenceStarted) { record.inferenceStarted = true; update(); }
      }
    }
    if (!terminalStatus) recoveryWatchdog.observe(event);
    if (event.type === "agent_start") {
      nativeActivity = true;
      deferredFollowUpSettle = false;
      if (options.runner === "pi" && replayAfterStart) {
        replayAfterStart = false;
        piControlLive = true;
        const retained = retainedPiQueues;
        retainedPiQueues = undefined;
        // Tracked IDs replay from durable envelopes, never a second time as
        // anonymous text from the old child's native queue snapshot.
        for (const text of retained?.steering ?? []) if (!followUpMessageId(text)) sendPiDelivery(text, undefined, "steer");
        for (const text of retained?.followUp ?? []) if (!followUpMessageId(text)) sendPiDelivery(text, undefined, "followUp");
        replayTrackedFollowUps();
        pollSteer();
      }
      emitLifecycle("pi.agent_start", {
        ...(record.runnerSessionId ? { runnerSessionId: record.runnerSessionId } : {}),
        ...(record.fabricSessionId ? { fabricSessionId: record.fabricSessionId } : {}),
      });
      retryPending = false;
      piSettledSuccessfully = false;
      hasFinalText = false;
      hasFinalResult = false;
      // Starting a retry is not proof of acceptance: preserve the error and timer
      // until the provider starts a new assistant response.
      return;
    }
    if (event.type === "auto_retry_start" && !terminalStatus) {
      retryPending = true;
      recoveryWatchdog.arm(terminalError ?? stringField(event.errorMessage) ?? "Pi retry stalled");
      return;
    }
    if (event.type === "auto_retry_end" && !terminalStatus) {
      retryPending = false;
      if (event.success === false) {
        sawAgentError = true;
        terminalError = stringField(event.finalError) ?? terminalError ?? "Pi retries exhausted";
        recoveryWatchdog.arm(terminalError);
      }
      return;
    }
    if (event.type === "response" && event.command === "prompt" && event.success === false) {
      sawAgentError = true;
      if (!terminalStatus) terminalError = typeof event.error === "string" ? event.error : "Pi rejected the prompt";
      closeChild();
      return;
    }
    if (event.type === "extension_ui_request") {
      const method = event.method;
      if (
        typeof event.id === "string" &&
        (method === "select" || method === "confirm" || method === "input" || method === "editor") &&
        !questionRelay?.request(event)
      ) {
        child.stdin?.write(
          `${JSON.stringify({ type: "extension_ui_response", id: event.id, cancelled: true })}\n`,
        );
      }
      return;
    }
    if (event.type === "tool_execution_start") {
      record.inferenceStarted = true;
      record.toolCalls++;
      if (typeof event.toolName === "string") {
        record.currentTool = event.toolName;
        record.currentToolStartedAt = Date.now();
        process.stdout.write(`→ ${event.toolName}\n`);
      }
      update();
      return;
    }
    if (event.type === "tool_execution_end") {
      if (event.isError === true) {
        emitLifecycle("pi.tool_error", {
          ...(typeof event.toolCallId === "string" ? { toolCallId: event.toolCallId } : {}),
          ...(typeof event.toolName === "string" ? { toolName: event.toolName } : {}),
        });
      }
      delete record.currentTool;
      delete record.currentToolStartedAt;
      update();
      return;
    }
    if (event.type === "turn_end") {
      emitLifecycle("pi.turn_end", {
        ...(typeof event.turnIndex === "number" ? { turnIndex: event.turnIndex } : {}),
      });
      record.turns++;
      update();
      return;
    }
    if (event.type === "queue_update") {
      const steering = Array.isArray(event.steering)
        ? event.steering.filter((value): value is string => typeof value === "string")
        : [];
      const followUp = Array.isArray(event.followUp)
        ? event.followUp.filter((value): value is string => typeof value === "string")
        : [];
      record.pendingMessages = { steering, followUp };
      update();
      return;
    }
    if (event.type === "message_end") {
      const message = event.message;
      if (typeof message !== "object" || message === null || Array.isArray(message)) return;
      const messageRecord = message as Record<string, unknown>;
      if (messageRecord.role !== "assistant") return;
      lostResult = undefined;
      const text = extractText(messageRecord);
      if (text || (messageRecord.stopReason !== "error" && messageRecord.stopReason !== "aborted")) {
        record.inferenceStarted = true;
      }
      hasFinalText = Boolean(text) && messageRecord.stopReason !== "error" &&
        messageRecord.stopReason !== "aborted" && messageRecord.stopReason !== "toolUse";
      producedFinalAnswer ||= hasFinalText;
      if (text) {
        record.text = latestRunText(text);
        process.stdout.write(`\n${text}\n`);
      }
      const usageDelta = extractUsageDelta(messageRecord);
      applyUsage(record, messageRecord);
      emitTokenUsage(usageDelta, {
        model: stringField(messageRecord.model),
        provider: stringField(messageRecord.provider),
      }, messageRecord);
      modelControl.observeAssistant(messageRecord);
      enforceTokenLimit();
      if ((messageRecord.stopReason === "error" || messageRecord.stopReason === "aborted") && !terminalStatus) {
        sawAgentError = true;
        providerAborted ||= messageRecord.stopReason === "aborted";
        terminalError = assistantError(messageRecord);
        recoveryWatchdog.arm(terminalError);
      } else {
        recoveryWatchdog.clear();
        sawAgentError = false;
        // Once a terminal cause is set (e.g. the per-child token guard), keep it;
        // a later non-error message_end must not clobber the reason we are
        // terminating for.
        if (!terminalStatus) terminalError = undefined;
      }
      update();
      return;
    }
    if (event.type === "agent_end") {
      emitLifecycle("pi.agent_end", { willRetry: event.willRetry === true });
      retryPending = event.willRetry === true;
      if (retryPending && !terminalStatus) recoveryWatchdog.arm(terminalError ?? "Pi announced a retry but did not resume");
      return;
    }
    if (event.type === "agent_settled") {
      nativeActivity = false;
      emitLifecycle("pi.agent_settled");
      if (!retryPending) {
        // Settlement ends automatic work, not necessarily successfully: native
        // compaction failures/aborts need not emit an assistant error message.
        // Older Pi frames omit outcome; retain their existing result checks.
        piSettledSuccessfully = event.outcome !== "error" && event.outcome !== "aborted" && !sawAgentError && !terminalError;
        providerAborted ||= event.outcome === "aborted";
        // Tool-only assistant events precede the tool's durable reply write.
        hasFinalResult = Boolean(hasFinalText || (replyFile && fs.existsSync(replyFile)));
        // Pull controls that landed with the final stream events before deciding
        // whether this one-shot child can close. A queued compact keeps stdin
        // open until its correlated response and compaction_end are observed.
        if (!piSettledSuccessfully) piControlLive = false;
        pollSteer();
        // sendUserMessage may still be in asynchronous input admission. A
        // completed native boundary is not proof that tracked payloads were
        // consumed. Keep stdin alive for their deferred native turn/cancellation.
        deferredFollowUpSettle = piSettledSuccessfully && hasUnsettledFollowUps();
        if (!deferredFollowUpSettle) compactControl.childSettled();
      }
      return;
    }
    if (event.type === "compaction_end") {
      emitLifecycle("pi.session_compact", {
        ...(typeof event.reason === "string" ? { reason: event.reason } : {}),
        ...(typeof event.willRetry === "boolean" ? { willRetry: event.willRetry } : {}),
      });
      return;
    }
    if (event.type === "extension_error") {
      const error = typeof event.error === "string" ? event.error : "Extension error";
      stderr = `${stderr}\n${error}`.trim().slice(-MAX_STDERR_CHARS);
      update();
    }
  };

  const startChildInput = (): void => {
    child.stdin?.on("error", () => {});
    if (options.runner === "claude") {
      writeClaudeInput("initial", task, images);
    } else if (options.runner === "veda") {
      // Veda reads the prompt from stdin when no positional prompt is given.
      // Mirror its <system_instructions> wrapping so systemPrompt and schema
      // instructions reach the backend model.
      const sections: string[] = [];
      if (options.systemPrompt) {
        sections.push(`<system_instructions>\n${options.systemPrompt}\n</system_instructions>`);
      }
      if (schema) {
        sections.push(
          `Your final response must contain only JSON matching this schema, without Markdown fences:\n${schema}`,
        );
      }
      sections.push(task);
      child.stdin?.write(sections.join("\n\n"));
      child.stdin?.end();
    } else {
      modelControl.start();
    }
  };

  // Tail a control file (steer.jsonl) the parent appends to and forward each
  // queued command to the child pi over its RPC stdin. This is the fabric
  // steering channel: the orchestrator (or any peer via the mesh relay) can
  // interject a steer / follow_up / queue-mode command between the child's
  // turns without stopping and respawning it, preserving its context. The
  // Consume only while the admitted child has writable stdin. During recovery
  // backoff/startup, the durable cursor stays put for the replacement child.
  let steerOffset = 0;
  let steerRemainder = Buffer.alloc(0);
  let skippingOversizedSteerLine = false;
  const pollSteer = (): void => {
    if (!options.steerFile || terminalStatus || (options.runner === "pi" &&
        (!piControlLive || childExited || !modelControl.ready || !child.stdin?.writable || child.stdin.writableEnded || child.stdin.destroyed))) return;
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(options.steerFile, "r");
    } catch {
      return;
    }
    try {
      const size = fs.fstatSync(descriptor).size;
      if (size < steerOffset) {
        steerOffset = 0;
        steerRemainder = Buffer.alloc(0);
        skippingOversizedSteerLine = false;
      }
      if (size <= steerOffset) return;
      const length = Math.min(size - steerOffset, STEER_READ_CHUNK_BYTES);
      const buffer = Buffer.allocUnsafe(length);
      const bytesRead = fs.readSync(descriptor, buffer, 0, length, steerOffset);
      steerOffset += bytesRead;
      let combined = Buffer.concat([steerRemainder, buffer.subarray(0, bytesRead)]);
      if (skippingOversizedSteerLine) {
        const skippedLineEnd = combined.indexOf(0x0a);
        if (skippedLineEnd < 0) return;
        combined = combined.subarray(skippedLineEnd + 1);
        skippingOversizedSteerLine = false;
      }
      const newline = combined.lastIndexOf(0x0a);
      if (newline < 0) {
        if (combined.length > MAX_STEER_LINE_BYTES) {
          steerRemainder = Buffer.alloc(0);
          skippingOversizedSteerLine = true;
        } else {
          steerRemainder = Buffer.from(combined);
        }
        return;
      }
      const remainder = combined.subarray(newline + 1);
      if (remainder.length > MAX_STEER_LINE_BYTES) {
        steerRemainder = Buffer.alloc(0);
        skippingOversizedSteerLine = true;
      } else {
        steerRemainder = Buffer.from(remainder);
      }
      let processedCommands = 0;
      for (const raw of combined.subarray(0, newline + 1).toString("utf8").split("\n")) {
        if (processedCommands >= MAX_STEER_COMMANDS_PER_POLL) break;
        if (Buffer.byteLength(raw, "utf8") > MAX_STEER_LINE_BYTES) continue;
        const line = raw.trim();
        if (!line) continue;
        processedCommands += 1;
<<<<<<< HEAD
        let command: { type?: string; message?: string; mode?: string; instructions?: string; provenance?: unknown; followUpId?: string };
=======
        let command: { type?: string; message?: string; mode?: string; instructions?: string; requestId?: string };
>>>>>>> upstream-v0.105.0
        try {
          command = JSON.parse(line);
        } catch {
          continue;
        }
        try {
          if (options.runner === "claude") {
            if (claudeCloseTimer) clearTimeout(claudeCloseTimer);
            claudeCloseTimer = undefined;
            if (command.type === "steer" && typeof command.message === "string") {
              enqueueClaudeControl(claudeSteering, command.message);
              flushClaudeSteering();
            } else if (command.type === "follow_up" && typeof command.message === "string") {
              enqueueClaudeControl(claudeFollowUps, command.message);
              if (claudeCanFollowUp && claudeSentInputs.length === 0) flushClaudeFollowUps();
            } else if (
              command.type === "set_steering_mode" &&
              (command.mode === "all" || command.mode === "one-at-a-time")
            ) {
              claudeSteeringMode = command.mode;
              flushClaudeSteering();
            } else if (
              command.type === "set_follow_up_mode" &&
              (command.mode === "all" || command.mode === "one-at-a-time")
            ) {
              claudeFollowUpMode = command.mode;
              if (claudeCanFollowUp && claudeSentInputs.length === 0) flushClaudeFollowUps();
            }
            updateClaudeQueue();
          } else if (options.runner === "veda") {
            // Steering is unsupported for the veda runner: Veda executes one
            // headless prompt per invocation. The command is dropped, never
            // forwarded to pi-style stdin frames.
          } else if (command.type === "steer" && typeof command.message === "string") {
            sendPiDelivery(command.message, copyFabricProvenance(command.provenance), "steer");
          } else if (command.type === "follow_up" && typeof command.message === "string") {
            if (command.followUpId && /^[0-9a-f-]{36}$/.test(command.followUpId)) {
              const id = command.followUpId;
              const receipt = followUpFile(path.dirname(deliveryDirectory), id);
              if (followUpState(receipt) !== "queued") { releaseFollowUpPayload(receipt); continue; }
              fs.writeFileSync(path.join(deliveryDirectory, id + ".json"), JSON.stringify({
                message: command.message, provenance: command.provenance, delivery: "followUp", followUpId: id,
              }), { mode: 0o600 });
              child.stdin?.write(JSON.stringify({ type: "prompt", message: "/fabric-delivery " + id, streamingBehavior: "followUp" }) + "\n");
            } else sendPiDelivery(command.message, copyFabricProvenance(command.provenance), "followUp");
          } else if (command.type === "set_steering_mode" && typeof command.mode === "string") {
            child.stdin?.write(JSON.stringify({ type: "set_steering_mode", mode: command.mode }) + "\n");
          } else if (command.type === "set_follow_up_mode" && typeof command.mode === "string") {
            child.stdin?.write(JSON.stringify({ type: "set_follow_up_mode", mode: command.mode }) + "\n");
          } else if (command.type === "compact") {
            compactControl.queue(command.instructions);
          } else if (command.type === "ui_response") {
            questionRelay?.respond(command);
          }
        } catch {
          /* stdin closed (settled/stopped child); a late steer is dropped */
        }
      }
    } finally {
      fs.closeSync(descriptor);
    }
  };
  const steerTimer = options.steerFile ? setInterval(() => {
    pollSteer();
    if (deferredFollowUpSettle && !nativeActivity && !hasUnsettledFollowUps()) {
      deferredFollowUpSettle = false;
      compactControl.childSettled();
    }
  }, 200) : undefined;
  steerTimer?.unref?.();

  // smarty-dev#1907: an event line above the cap is dropped, not fatal. The run
  // keeps its child; the warning names the event and its size, and a bounded
  // prefix is kept as evidence. Memory stays bounded: the rest of the line is
  // counted, never buffered.
  let oversized: { chars: number; prefix: string } | undefined;
  let oversizedCount = 0;
  const startOversizedEvent = (text: string): void => {
    toolCallStreamGuard.discardedEvent();
    oversized = { chars: text.length, prefix: text.slice(0, MAX_EVENT_LINE_CHARS) };
  };
  const finishOversizedEvent = (): void => {
    if (!oversized) return;
    const { chars, prefix } = oversized;
    oversized = undefined;
    oversizedCount += 1;
    let artifactPath: string | undefined;
    try {
      artifactPath = path.join(
        path.dirname(options.logFile),
        oversizedCount === 1 ? "oversized-event-prefix.txt" : `oversized-event-prefix-${oversizedCount}.txt`,
      );
      fs.writeFileSync(artifactPath, prefix, { encoding: "utf8", mode: 0o600 });
    } catch {
      artifactPath = undefined;
    }
    const head = prefix.slice(0, 4096);
    const type = /^\{"type":"([^"]{1,64})"/.exec(head)?.[1] ?? "unknown";
    const tool = /"toolName":"([^"]{1,128})"/.exec(head)?.[1];
    const role = /"role":"([^"]{1,64})"/.exec(head)?.[1];
    const warning =
      `Dropped an oversized agent event line (${type}${tool ? `, tool ${tool}` : ""}, ` +
      `${chars} characters > ${MAX_EVENT_LINE_CHARS})` +
      (artifactPath ? `; first ${prefix.length} characters saved to: ${artifactPath}` : "");
    appendLog(`${JSON.stringify({ type: "worker_warning", warning })}\n`);
    process.stderr.write(`[pi-fabric] ${warning}\n`);
    record.warnings = [...(record.warnings ?? []), warning].slice(-20);
    // Never strand a correlated admission response behind the event-line cap.
    // Only its bounded envelope is inspected; the history remains discarded.
    contextAdmission?.observeOversizedResponse(prefix, chars);
    if (type === "message_end" && (role === undefined || role === "assistant")) lostResult = warning;
    update();
  };
  const discardOversized = (text: string): void => {
    if (oversized) oversized.chars += text.length;
  };

  const recordStderr = (text: string): void => {
    if (!text) return;
    appendLog(`${JSON.stringify({ type: "worker_stderr", text })}\n`);
    process.stderr.write(text);
    stderr = `${stderr}${text}`.slice(-MAX_STDERR_CHARS);
  };
  const consumeOutput = (decoded: string, projected = false): void => {
    if (options.runner === "veda") {
      vedaOutput += decoded;
      return;
    }
    outputBuffer += eventProjection && !projected ? eventProjection.write(decoded) : decoded;
    while (true) {
      const newline = outputBuffer.indexOf("\n");
      if (oversized) {
        // Still inside a dropped line: count it up to its end, keep nothing.
        discardOversized(newline < 0 ? outputBuffer : outputBuffer.slice(0, newline));
        outputBuffer = newline < 0 ? "" : outputBuffer.slice(newline + 1);
        if (newline < 0) break;
        finishOversizedEvent();
        continue;
      }
      if (newline < 0) {
        // Redundant Pi lifecycle history and large image data have already been
        // elided while streaming. The cap still bounds every other event field.
        if (outputBuffer.length > MAX_EVENT_LINE_CHARS) {
          startOversizedEvent(outputBuffer);
          outputBuffer = "";
        }
        break;
      }
      if (newline > MAX_EVENT_LINE_CHARS) {
        startOversizedEvent(outputBuffer.slice(0, newline));
        outputBuffer = outputBuffer.slice(newline + 1);
        finishOversizedEvent();
        continue;
      }
      const line = outputBuffer.slice(0, newline).replace(/\r$/, "");
      outputBuffer = outputBuffer.slice(newline + 1);
      processEvent(line);
    }
  };
  const attachChildStreams = (): void => {
    child.stdout?.on("data", (chunk: Buffer) => consumeOutput(outputDecoder.write(chunk)));
    child.stderr?.on("data", (chunk: Buffer) => recordStderr(stderrDecoder.write(chunk)));
    child.stderr?.on("error", () => {});
  };
  attachChildStreams();
  startChildInput();

  const timeout = setTimeout(() => {
    if (terminalStatus) return;
    terminalStatus = "timed_out";
    terminalError = `Agent timed out after ${options.timeoutMs}ms`;
    if (options.runner === "pi" && !modelControl.ready) {
      terminalError += "; Pi model admission did not complete; task was not sent";
    }
    killChild();
  }, options.timeoutMs);
  timeout.unref();

  const stop = (): void => {
    if (terminalStatus) return;
    terminalStatus = "stopped";
    terminalError = "Agent stopped";
    killChild();
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  process.once("SIGHUP", stop);

  let exitCode: number | null;
  while (true) {
    exitCode = await new Promise<number | null>((resolve) => {
      child.once("error", (error) => {
        terminalStatus = "failed";
        terminalError = error.message;
        // Wait for close as well: a spawn error still owns its stream handles.
      });
      child.once("close", (code) => { childExited = true; piControlLive = false; resolve(code); });
    });
    if (closeTimer) clearTimeout(closeTimer);
    closeTimer = undefined;
    if (killTimer) clearTimeout(killTimer);
    killTimer = undefined;
    // Drain the failed attempt before deciding whether it can be resumed.
    if (options.runner === "pi") {
      consumeOutput(outputDecoder.end());
      // end() returns already projected text, so do not project it a second time.
      consumeOutput(eventProjection!.end(), true);
      if (oversized) finishOversizedEvent();
      else if (outputBuffer.trim()) processEvent(outputBuffer);
      outputBuffer = "";
      recordStderr(stderrDecoder.end());
      toolCallStreamGuard.clear();
      if (closeTimer) clearTimeout(closeTimer);
      closeTimer = undefined;
    }
    if (!persistentPiTask || terminalStatus || providerAborted || producedFinalAnswer ||
        (replyFile && fs.existsSync(replyFile)) || lostResult || !modelControl.ready ||
        record.compaction?.status === "queued" || record.compaction?.status === "in_flight" ||
        !piSessionFile || !fs.existsSync(piSessionFile)) break;
    const errorMessage = terminalError ?? stderr.trim();
    if (!errorMessage) break;
    if (!retryableProviderError(errorMessage)) break;
    recoveryWatchdog.arm(errorMessage);
    const delayMs = PI_PROVIDER_RESUME_DELAYS_MS[providerResumeAttempts];
    if (delayMs === undefined) {
      terminalError = `${errorMessage}; Pi provider recovery exhausted 3 same-session resumes within the 10-minute bound; session retained: ${piSessionFile}`;
      sawAgentError = true;
      break;
    }
    const waitMs = delayMs * recoveryScale;
    if (waitMs >= recoveryWatchdog.remainingMs) {
      terminalError = `${errorMessage}; Pi provider recovery cannot resume within the 10-minute bound (600000ms); session retained: ${piSessionFile}`;
      sawAgentError = true;
      break;
    }
    recoveryWatchdog.observe({ type: "auto_retry_start", delayMs: waitMs, errorMessage });
    const resume = { attempt: providerResumeAttempts + 1, maxAttempts: PI_PROVIDER_RESUME_DELAYS_MS.length,
      delayMs: waitMs, error: errorMessage, sessionFile: piSessionFile };
    record.warnings = [...(record.warnings ?? []), `Pi provider recovery: same-session resume ${resume.attempt}/${resume.maxAttempts} scheduled after ${waitMs}ms`].slice(-20);
    update();
    emitLifecycle("run.resumed", { ...resume, phase: "scheduled" });
    appendLog(`${JSON.stringify({ type: "fabric_provider_resume", ...resume, phase: "scheduled" })}\n`);
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, waitMs);
      cancelResumeWait = () => { clearTimeout(timer); resolve(); };
    });
    cancelResumeWait = undefined;
    if (terminalStatus) break;
    providerResumeAttempts++;
    emitLifecycle("run.resumed", { ...resume, phase: "starting" });
    appendLog(`${JSON.stringify({ type: "fabric_provider_resume", ...resume, phase: "starting" })}\n`);
    // --session <exact path> is Pi's noninteractive resume selector. --continue
    // alone selects the most recent session, which may belong to another task.
    resumePrompt = true;
    retainedPiQueues = record.pendingMessages ? {
      steering: [...record.pendingMessages.steering], followUp: [...record.pendingMessages.followUp],
    } : undefined;
    sawAgentError = false;
    retryPending = false;
    terminalError = undefined;
    piSettledSuccessfully = false;
    stderr = "";
    activationWindowReady = false;
    outputDecoder = new StringDecoder("utf8");
    stderrDecoder = new StringDecoder("utf8");
    eventProjection = new PiEventProjection();
    contextAdmission = undefined;
    modelControl = createModelControl();
    compactControl = createCompactControl();
    child = spawnChild();
    childExited = false;
    attachChildStreams();
    startChildInput();
  }

  if (steerTimer) clearInterval(steerTimer);
  questionRelay?.close();
  delete record.blockedOn;
  if (claudeCloseTimer) clearTimeout(claudeCloseTimer);
  clearTimeout(timeout);
  if (killTimer) clearTimeout(killTimer);
  if (closeTimer) clearTimeout(closeTimer);
  recoveryWatchdog.dispose();
  toolCallStreamGuard.dispose();
  if (options.runner === "pi" && !modelControl.ready && !terminalStatus) {
    terminalStatus = "failed";
    terminalError = `Child Pi exited before requested model admission completed; task was not sent${stderr.trim() ? `: ${stderr.trim()}` : ""}`;
  }
  if (process.env.PI_FABRIC_INJECT_CRASH === "close") throw new Error("simulated close crash");
  if (options.runner === "veda") {
    vedaOutput += outputDecoder.end();
  } else if (options.runner !== "pi") {
    outputBuffer += outputDecoder.end();
  }
  if (options.runner !== "pi") recordStderr(stderrDecoder.end());
  if (options.runner === "veda") {
    const trimmed = vedaOutput.trim();
    if (trimmed) {
      try {
        const parsed = parseStructuredValue(trimmed);
        if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
          vedaParsed = parsed as Record<string, unknown>;
          const text = stringField(vedaParsed.text) ?? "";
          if (text) {
            record.text = latestRunText(text);
            process.stdout.write(`\n${text}\n`);
          }
          const sessionId = stringField(vedaParsed.sessionId);
          if (sessionId) record.runnerSessionId = sessionId;
          const usage = vedaParsed.usage;
          if (typeof usage === "object" && usage !== null && !Array.isArray(usage)) {
            const values = usage as Record<string, unknown>;
            const input = numberField(values.inputTokens);
            const output = numberField(values.outputTokens);
            const cacheRead = numberField(values.cachedTokens);
            const cost = typeof values.costUsd === "number" ? values.costUsd : 0;
            record.usage = { input, output, cacheRead, cacheWrite: 0, cost };
            emitTokenUsage({ input, output, cacheRead, cacheWrite: 0, cost }, { model: stringField(vedaParsed.model) });
          }
          const envelopeErrors: string[] = [];
          const error = stringField(vedaParsed.error);
          if (error) envelopeErrors.push(error);
          // navigator-plan gates the response on a <program> design block and
          // the worker persona on a <worker_report>; both exit non-zero and
          // report failure only via design/worker fields, not envelope.error.
          for (const key of ["design", "worker"] as const) {
            const gate = vedaParsed[key];
            if (typeof gate !== "object" || gate === null || Array.isArray(gate)) continue;
            const status = gate as Record<string, unknown>;
            if (status.ok !== false) continue;
            const details = Array.isArray(status.errors)
              ? status.errors.filter((entry): entry is string => typeof entry === "string").join("; ")
              : [stringField(status.reason), stringField(status.detail)]
                  .filter((entry): entry is string => entry !== undefined)
                  .join(": ");
            envelopeErrors.push(`Veda ${key} failed${details ? `: ${details}` : ""}`);
          }
          if (envelopeErrors.length > 0) {
            sawAgentError = true;
            terminalError = envelopeErrors.join("\n");
          }
          record.turns += 1;
          update();
        }
      } catch {
        // Unparseable stdout; the generic failed-record path reports stderr.
      }
    }
  } else if (oversized) {
    discardOversized(outputBuffer);
    outputBuffer = "";
    finishOversizedEvent();
  } else if (outputBuffer.trim()) {
    processEvent(outputBuffer);
  }
  runLog.flush();
  record.exitCode = exitCode;
  record.stderr = stderr.slice(-MAX_STDERR_CHARS);
  if (
    record.compaction?.status === "queued" ||
    record.compaction?.status === "in_flight"
  ) {
    const error = terminalError ?? "Child Pi exited before the queued compaction completed";
    record.compaction = {
      ...record.compaction,
      status: "failed",
      updatedAt: Date.now(),
      finishedAt: Date.now(),
      error,
    };
    if (!terminalStatus) {
      terminalStatus = "failed";
      terminalError = error;
    }
  }
  if (lostResult && !terminalStatus) {
    terminalStatus = "failed";
    terminalError = `Agent's final assistant result was lost: ${lostResult}`;
  }
  record.finishedAt = Date.now();
  record.updatedAt = record.finishedAt;
  const childCompleted =
    exitCode === 0 &&
    !sawAgentError &&
    (options.runner === "pi" ||
      (options.runner === "claude" &&
        claudeResultSeen &&
        claudeSentInputs.length === 0 &&
        claudeSteering.length === 0 &&
        claudeFollowUps.length === 0) ||
      (options.runner === "veda" && vedaParsed !== undefined));
  record.status = terminalStatus ?? (childCompleted ? "completed" : "failed");
  if (terminalError) record.error = terminalError;
  if (record.status === "failed" && !record.error) {
    record.error =
      stderr.trim() ||
      (exitCode === 0
        ? `${runnerLabel(options.runner)} agent reported an error before exiting`
        : `${runnerLabel(options.runner)} exited with code ${exitCode ?? "unknown"}`);
  }
  if (record.status === "completed" && replyFile) {
    // The reply is the tool call's arguments, and nothing else: final text is never parsed for it,
    // not even JSON-only text (smarty-dev#967; the roles name the tool since smarty-dev#1105).
    try {
      record.value = JSON.parse(fs.readFileSync(replyFile, "utf8")) as unknown;
      record.replyVia = "tool";
    } catch {
      // No fabric_reply call.
    }
    if (record.value === undefined) {
      record.status = "failed";
      const snippet = record.text.trim().slice(0, 200);
      record.error = `Directive reply missing: the run ended without a fabric_reply call${snippet ? ` (final text: ${snippet}${record.text.trim().length > 200 ? "…" : ""})` : ""}`;
    }
  }
  if (record.status === "completed" && options.schemaFile) {
    try {
      const schema = JSON.parse(fs.readFileSync(options.schemaFile, "utf8")) as Record<
        string,
        unknown
      >;
      validateAgentResult(record, schema);
    } catch (error) {
      record.status = "failed";
      const reason = error instanceof Error ? error.message : String(error);
      const output = record.text.trim();
      const snippet = output.slice(0, 200);
      record.error = `Structured agent output was invalid: ${reason}${snippet ? ` (output: ${snippet}${output.length > 200 ? "…" : ""})` : ""}`;
    }
  }
  delete record.currentTool;
  await new Promise<void>((resolve) =>
    sessionStream ? sessionStream.end(resolve) : resolve(),
  );
  // Child close drained stdout/stderr, decoder tails and held log events above.
  // The result is now terminal, including reply/schema validation. Compact only
  // this quiescent source, before publishing terminal status: manager settlement
  // and actor/residency retention can copy/remove the run as soon as it appears.
  // Failure is best-effort and must never change or mask the original run result.
  const logCompaction = compactTerminalRunLog(options.logFile, record.status);
  if (logCompaction.compactionSkipped || logCompaction.error) {
    record.compactionSkipped = logCompaction.compactionSkipped ??
      `Terminal run-log compaction failed; full log retained: ${logCompaction.error}`;
  }
  try {
    activationSession?.retain();
  } catch (error) {
    record.status = "failed";
    record.error = `${record.error ? record.error + "\n" : ""}Activation journal retention failed: ${String(error)}`;
  }
  // The owning Main may already be dead. Publish through its immutable launch return address.
  saveWorkerCompletion(options.statusFile, record);
  writeRunRecord(options.statusFile, record);
  terminalWritten = true;
  process.stdout.write(`\n[pi-fabric] ${record.status}\n`);
  process.exitCode = record.status === "completed" ? 0 : 1;
};

main().catch((error) => {
  console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
  writeCrashStatus(error);
  process.exit(1);
});
