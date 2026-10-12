import type { ImageContent } from "@earendil-works/pi-ai";
import type { FabricScope } from "../protocol.js";
import type { FabricKernel } from "../runtime/kernel.js";
import type { FabricThinking } from "../thinking.js";
import type { FabricAgentLineage, FabricWritePolicy } from "./child-env.js";
import type { AgentChildQuestionResponse, AgentUsage } from "./types.js";

export type BuiltInFabricAgentRunner = "pi" | "pi-durable" | "claude" | "veda";

/**
 * What a runner honors. Fabric refuses a request that needs an undeclared
 * capability before admission, budget, or worktree side effects.
 */
export interface FabricRunnerCapabilities {
  /** The child runs Fabric itself (`recursive: true`, fabric_exec in full code mode). */
  recursiveFabric: boolean;
  steer: boolean;
  followUp: boolean;
  /** Long-lived sessions for persistent actors. */
  persistentSessions: boolean;
  /** Explicit `kernel` choices. */
  kernels: boolean;
  /** Session seeds: trajectory handoff and `seed: "branch"`. */
  handoff: boolean;
  modelDiscovery: boolean;
  imageInput: boolean;
  compaction: boolean;
  /** Routed child dialogs (`agents.childQuestions: "route"`). */
  questions: boolean;
  /** Hosted runs that can park and resume (`sleep`/`wake`). */
  sleep: boolean;
  /** Write confinement (`readOnly`, `writableRoots`, `shell`). */
  writePolicy: boolean;
}

const FABRIC_RUNNER_CAPABILITY_NAMES = [
  "recursiveFabric",
  "steer",
  "followUp",
  "persistentSessions",
  "kernels",
  "handoff",
  "modelDiscovery",
  "imageInput",
  "compaction",
  "questions",
  "sleep",
  "writePolicy",
] as const satisfies readonly (keyof FabricRunnerCapabilities)[];

export interface FabricRunnerModelInfo {
  runner: string;
  provider: string;
  id: string;
  name: string;
  key: string;
  [key: string]: unknown;
}

export interface FabricRunnerModelContext {
  cwd: string;
  refresh: boolean;
}

/** Launch facts shared by worker and hosted runners. All values are JSON. */
export interface FabricRunnerLaunchContext {
  /** Fabric run id. Hosted runners use it as the idempotency key. */
  id: string;
  name: string;
  task: string;
  cwd: string;
  runDirectory: string;
  residency: "session" | "durable";
  /** Run deadline in epoch milliseconds. */
  deadlineAt: number;
  depth: number;
  lineage: FabricAgentLineage;
  tools: readonly string[];
  model?: string;
  thinking?: FabricThinking;
  kernel?: FabricKernel;
  recursive?: boolean;
  /** Present only for runners declaring `imageInput`. */
  images?: readonly ImageContent[];
  schema?: Record<string, unknown>;
  systemPrompt?: string;
  /** Pi session file seeding the run; present only for runners declaring `handoff`. */
  sessionFile?: string;
  /** Effective confinement; present only for runners declaring `writePolicy`. */
  writePolicy?: FabricWritePolicy;
  actorId?: string;
  actorName?: string;
  /** Host-issued scope narrowed for this run (`pi-fabric/scope`); absent for unscoped sessions. */
  scope?: Readonly<FabricScope>;
}

export interface FabricWorkerLaunchContext extends FabricRunnerLaunchContext {
  files: {
    taskFile: string;
    statusFile: string;
    lifecycleFile: string;
    logFile: string;
    steerFile: string;
    schemaFile?: string;
    imagesFile?: string;
  };
  /** Fabric's own Pi/Claude/Veda worker launch, for adapters that wrap it. */
  fabricWorker: { workerPath: string; workerArguments: readonly string[] };
}

export interface FabricWorkerLaunch {
  workerPath: string;
  workerArguments: string[];
}

export interface FabricHostedRunContext extends FabricRunnerLaunchContext {
  /** Equal to `id`; submitting twice with one key must start at most one run. */
  idempotencyKey: string;
}

export type FabricHostedLiveness =
  | "running"
  | "sleeping"
  | "settled"
  | "cancelled"
  | "interrupted"
  | "unknown";

export type FabricRunStopReason = "requested" | "timeout" | "shutdown";

export interface FabricHostedProgress {
  turns?: number;
  toolCalls?: number;
  /** null clears the current tool. */
  currentTool?: string | null;
  /** Latest assistant text; Fabric keeps a bounded tail. */
  text?: string;
}

/** Events appended to the run's transcript log (`events.jsonl`). */
export type FabricTranscriptEvent =
  | {
      type: "message_end";
      message: {
        role: "user" | "assistant";
        content: string | Array<{ type: "text"; text: string }>;
      };
    }
  | { type: "tool_execution_start"; toolCallId?: string; toolName: string; args?: unknown }
  | {
      type: "tool_execution_end";
      toolCallId?: string;
      toolName: string;
      result?: unknown;
      isError?: boolean;
    }
  | { type: "extension_error"; error: string };

export interface FabricRunnerQuestion {
  method: "select" | "confirm" | "input" | "editor";
  title: string;
  message?: string;
  options?: string[];
  placeholder?: string;
  prefill?: string;
  timeoutMs?: number;
}

export type FabricRunnerAnswer = AgentChildQuestionResponse;

export interface FabricHostedReporter {
  progress(update: FabricHostedProgress): void;
  /** Cumulative usage for the whole run; Fabric records the increase. */
  usage(total: AgentUsage): void;
  transcript(event: FabricTranscriptEvent): void;
  /** Routed like a Pi child dialog: direct UI when available, else a decision. */
  question(question: FabricRunnerQuestion): Promise<FabricRunnerAnswer>;
  finish(result: {
    status: "completed" | "failed" | "stopped";
    output: string;
    structured?: unknown;
  }): void;
  /** Fabric never retries; `retryable` is reported to the caller only. */
  fail(failure: { error: string; retryable?: boolean }): void;
}

interface FabricRunnerBase {
  readonly id: string;
  readonly label: string;
  readonly capabilities: Readonly<FabricRunnerCapabilities>;
  /**
   * Absolute path of an ES module that registers this adapter when imported.
   * Required for `residency: "durable"`: the resident host imports it before
   * launching or re-attaching the run.
   */
  readonly residentModule?: string;
  models?(context: FabricRunnerModelContext): readonly FabricRunnerModelInfo[] | Promise<readonly FabricRunnerModelInfo[]>;
  defaultModel?(): string | undefined;
  normalizeModel?(model: string): string | Promise<string>;
  mapTools?(tools: readonly string[]): readonly string[] | Promise<readonly string[]>;
}

/** Fabric spawns `workerPath` under the selected transport; see "Worker protocol". */
export interface FabricWorkerRunner extends FabricRunnerBase {
  readonly kind: "worker";
  launch(context: FabricWorkerLaunchContext): FabricWorkerLaunch | Promise<FabricWorkerLaunch>;
  /** Called before the transport kills the process. */
  stop?(run: { id: string; runDirectory: string }): void | Promise<void>;
}

/** The adapter owns execution (for example a daemon); Fabric spawns no process. */
export interface FabricHostedRunner extends FabricRunnerBase {
  readonly kind: "hosted";
  /** Pure: returns a JSON locator (at most 8 KiB) that Fabric persists before `start`. */
  prepare(context: FabricHostedRunContext): unknown;
  start(locator: unknown, context: FabricHostedRunContext, reporter: FabricHostedReporter): void | Promise<void>;
  attach(locator: unknown, context: FabricHostedRunContext, reporter: FabricHostedReporter): void | Promise<void>;
  liveness(locator: unknown): FabricHostedLiveness | Promise<FabricHostedLiveness>;
  stop(locator: unknown, reason: FabricRunStopReason): { confirmed: boolean } | Promise<{ confirmed: boolean }>;
  abort?(locator: unknown, reason: FabricRunStopReason): void | Promise<void>;
  sleep?(locator: unknown): void | Promise<void>;
  wake?(locator: unknown): void | Promise<void>;
  steer?(locator: unknown, message: string, data?: unknown): void | Promise<void>;
  followUp?(locator: unknown, message: string, data?: unknown): void | Promise<void>;
}

export type FabricRunnerAdapter = FabricWorkerRunner | FabricHostedRunner;

/** Canonical built-in runner ids; src/config.ts inlines the same set. */
export const BUILT_IN_RUNNER_IDS: ReadonlySet<string> = new Set(["pi", "pi-durable", "claude", "veda"]);
/** Canonical runner id syntax; eager modules inline an identical copy. */
export const RUNNER_ID_PATTERN = /^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/;
const MAX_RUNNER_ID_CHARS = 64;
const MAX_LABEL_CHARS = 80;

export const isFabricRunnerId = (value: unknown): value is string =>
  typeof value === "string" && value.length <= MAX_RUNNER_ID_CHARS && RUNNER_ID_PATTERN.test(value);

// A globalThis singleton: a second copy of this module (another installed
// pi-fabric, or a runner extension's own dependency) shares registrations.
const REGISTRY_SYMBOL = Symbol.for("pi-fabric.runnerRegistry.v1");
const registryHost = globalThis as unknown as Record<symbol, Map<string, FabricRunnerAdapter> | undefined>;
const adapters: Map<string, FabricRunnerAdapter> =
  registryHost[REGISTRY_SYMBOL] ?? (registryHost[REGISTRY_SYMBOL] = new Map());

const fabricWorkerLaunch = (context: FabricWorkerLaunchContext): FabricWorkerLaunch => ({
  workerPath: context.fabricWorker.workerPath,
  workerArguments: [...context.fabricWorker.workerArguments],
});

const builtIn = (
  id: BuiltInFabricAgentRunner,
  label: string,
  capabilities: FabricRunnerCapabilities,
): FabricWorkerRunner => Object.freeze({
  kind: "worker" as const,
  id,
  label,
  capabilities: Object.freeze(capabilities),
  launch: fabricWorkerLaunch,
});

// Built-ins keep their model and tool handling in the manager; this table
// carries only the capability facts checked uniformly for every runner.
for (const adapter of [
  ...(["pi", "pi-durable"] as const).map((id) => builtIn(id, id === "pi" ? "Pi" : "Pi Durable", {
    recursiveFabric: true,
    steer: true,
    followUp: true,
    persistentSessions: true,
    kernels: true,
    handoff: true,
    modelDiscovery: true,
    imageInput: true,
    compaction: true,
    questions: true,
    sleep: false,
    writePolicy: true,
  })),
  builtIn("claude", "Claude", {
    recursiveFabric: false,
    steer: true,
    followUp: true,
    persistentSessions: true,
    kernels: false,
    handoff: false,
    modelDiscovery: true,
    imageInput: true,
    compaction: false,
    questions: false,
    sleep: false,
    writePolicy: false,
  }),
  builtIn("veda", "Veda", {
    recursiveFabric: false,
    steer: false,
    followUp: false,
    persistentSessions: false,
    kernels: false,
    handoff: false,
    modelDiscovery: false,
    // Veda has always accepted (and dropped) image blocks; refusing them now
    // would change built-in behavior.
    imageInput: true,
    compaction: false,
    questions: false,
    sleep: false,
    writePolicy: false,
  }),
]) {
  if (!adapters.has(adapter.id)) adapters.set(adapter.id, adapter);
}

const requireFunction = (adapter: Record<string, unknown>, id: string, name: string): void => {
  if (typeof adapter[name] !== "function") {
    throw new Error(`Fabric runner ${id} must implement ${name}()`);
  }
};

const optionalFunction = (adapter: Record<string, unknown>, id: string, name: string): void => {
  if (adapter[name] !== undefined && typeof adapter[name] !== "function") {
    throw new Error(`Fabric runner ${id}: ${name} must be a function`);
  }
};

// Hosted runs have no Fabric child process, so these cannot be honored.
const HOSTED_UNSUPPORTED: readonly (keyof FabricRunnerCapabilities)[] = [
  "recursiveFabric",
  "kernels",
  "compaction",
  "persistentSessions",
];

const validateAdapter = (value: unknown): FabricRunnerAdapter => {
  if (!value || typeof value !== "object") throw new Error("Fabric runner adapter must be an object");
  const adapter = value as Record<string, unknown>;
  const id = adapter.id;
  if (!isFabricRunnerId(id)) throw new Error(`Invalid Fabric runner id: ${JSON.stringify(id)}`);
  if (BUILT_IN_RUNNER_IDS.has(id)) throw new Error(`Cannot replace built-in Fabric runner: ${id}`);
  if (typeof adapter.label !== "string" || !adapter.label.trim() || adapter.label.length > MAX_LABEL_CHARS) {
    throw new Error(`Fabric runner ${id} needs a label of 1-${MAX_LABEL_CHARS} characters`);
  }
  if (adapter.kind !== "worker" && adapter.kind !== "hosted") {
    throw new Error(`Fabric runner ${id} must declare kind "worker" or "hosted"`);
  }
  const capabilities = adapter.capabilities as Record<string, unknown> | undefined;
  const missing = FABRIC_RUNNER_CAPABILITY_NAMES.filter(
    (name) => typeof capabilities?.[name] !== "boolean",
  );
  if (missing.length > 0) {
    throw new Error(`Fabric runner ${id} must declare boolean capabilities: ${missing.join(", ")}`);
  }
  const caps = capabilities as unknown as FabricRunnerCapabilities;
  if (adapter.residentModule !== undefined) {
    if (
      typeof adapter.residentModule !== "string" ||
      !/^(?:\/|[A-Za-z]:[\\/]|file:)/.test(adapter.residentModule) ||
      adapter.residentModule.length > 4_096
    ) {
      throw new Error(`Fabric runner ${id}: residentModule must be an absolute path or file: URL`);
    }
  }
  for (const name of ["models", "defaultModel", "normalizeModel", "mapTools"]) {
    optionalFunction(adapter, id, name);
  }
  if (caps.modelDiscovery) requireFunction(adapter, id, "models");
  if (adapter.kind === "worker") {
    requireFunction(adapter, id, "launch");
    optionalFunction(adapter, id, "stop");
    if (caps.sleep) throw new Error(`Fabric runner ${id}: sleep is available to hosted runners only`);
  } else {
    for (const name of ["prepare", "start", "attach", "liveness", "stop"]) requireFunction(adapter, id, name);
    for (const name of ["abort", "sleep", "wake", "steer", "followUp"]) optionalFunction(adapter, id, name);
    const unsupported = HOSTED_UNSUPPORTED.filter((name) => caps[name]);
    if (unsupported.length > 0) {
      throw new Error(`Fabric runner ${id}: hosted runners cannot declare ${unsupported.join(", ")}`);
    }
    if (caps.steer) requireFunction(adapter, id, "steer");
    if (caps.followUp) requireFunction(adapter, id, "followUp");
    if (caps.sleep) {
      requireFunction(adapter, id, "sleep");
      requireFunction(adapter, id, "wake");
    }
  }
  return value as FabricRunnerAdapter;
};

const bound = <T>(adapter: Record<string, unknown>, name: string): T | undefined =>
  typeof adapter[name] === "function"
    ? ((adapter[name] as (...args: unknown[]) => unknown).bind(adapter) as T)
    : undefined;

/**
 * Register a runner for this process. Registration is process-local: a durable
 * run needs the adapter registered in the resident host too (`residentModule`).
 * Returns an unregister function.
 */
export const registerAgentRunner = (adapter: FabricRunnerAdapter): (() => void) => {
  const checked = validateAdapter(adapter);
  if (adapters.has(checked.id)) throw new Error(`Fabric runner already registered: ${checked.id}`);
  const source = checked as unknown as Record<string, unknown>;
  const methods = checked.kind === "worker"
    ? ["launch", "stop"]
    : ["prepare", "start", "attach", "liveness", "stop", "abort", "sleep", "wake", "steer", "followUp"];
  const registered = Object.freeze(Object.fromEntries([
    ["kind", checked.kind],
    ["id", checked.id],
    ["label", checked.label],
    ["capabilities", Object.freeze(Object.fromEntries(
      FABRIC_RUNNER_CAPABILITY_NAMES.map((name) => [name, checked.capabilities[name]]),
    ))],
    ...(checked.residentModule ? [["residentModule", checked.residentModule]] : []),
    ...["models", "defaultModel", "normalizeModel", "mapTools", ...methods]
      .map((name) => [name, bound(source, name)])
      .filter(([, method]) => method !== undefined),
  ])) as unknown as FabricRunnerAdapter;
  adapters.set(checked.id, registered);
  return () => {
    if (adapters.get(checked.id) === registered) adapters.delete(checked.id);
  };
};

export const getAgentRunner = (id: string): FabricRunnerAdapter | undefined => adapters.get(id);

export const listAgentRunners = (): readonly FabricRunnerAdapter[] => [...adapters.values()];

export const requireAgentRunner = (id: unknown): FabricRunnerAdapter => {
  const adapter = typeof id === "string" ? adapters.get(id) : undefined;
  if (!adapter) {
    throw new Error(
      `Unsupported Fabric agent runner: ${JSON.stringify(id)}. Register it with registerAgentRunner() from pi-fabric/runners before use.`,
    );
  }
  return adapter;
};
