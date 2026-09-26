import type { ExtensionEvent } from "@earendil-works/pi-coding-agent";
import type { FabricAgentRunner, FabricAgentTransport, FabricPythonRuntime } from "../config.js";
import type { FabricThinking } from "../thinking.js";
import type { FabricLogLine, AgentRunRecord, AgentUsage } from "../agents/types.js";
import type { FabricCapabilityRequirement } from "../components/types.js";
import type { FabricKernel } from "../runtime/kernel.js";
import type { FabricParticipantResidency } from "../topology/types.js";

// Pi's extension event union is closed; every member we want the actor host
// to observe must appear in FABRIC_ACTOR_PI_HOST_EVENTS below. `project_trust`
// uses pi's dedicated trust handler, and pi 0.86's `cache_warming_decision` is
// a host-internal prompt-cache maintenance control event, so neither is an
// actor observation and both are excluded here.
//
// pi 0.87 added two events, and neither is an actor observation yet:
//   - `agent_before_settle` is an actionable boundary that exists so a handler
//     can draft session entries and force one continuation. Fabric observes
//     lifecycle boundaries (`turn_end`, `agent_end`, `agent_settled`) and
//     republishes the ones it cares about through FABRIC_LIFECYCLE_EVENTS, which
//     has no `agent_before_settle` topic.
//   - `context_with_system` runs on the full transcript including system
//     messages and its result is sent verbatim. Exposing it to actors would let
//     them rewrite the system prompt per request, which Fabric deliberately
//     avoids to keep the cached system prefix byte-stable.
//   - `provider_stream_event` (newer Pi) fires for every raw provider stream chunk: far
//     too frequent for an actor mailbox. Excluding it is a no-op on Pi versions that
//     do not define it.
// To observe one of these as an actor event, add it to
// FABRIC_ACTOR_PI_HOST_EVENTS below and give it a FABRIC_LIFECYCLE_EVENTS topic.
export type FabricActorPiHostEvent = Exclude<
  ExtensionEvent["type"],
  | "project_trust"
  | "cache_warming_decision"
  | "agent_before_settle"
  | "context_with_system"
  | "provider_stream_event"
>;

const defineFabricActorPiHostEvents = <
  const Events extends readonly FabricActorPiHostEvent[],
>(
  events: Exclude<FabricActorPiHostEvent, Events[number]> extends never ? Events : never,
): Events => events;

export const FABRIC_ACTOR_PI_HOST_EVENTS = defineFabricActorPiHostEvents([
  "resources_discover",
  "session_start",
  "session_info_changed",
  "session_before_switch",
  "session_before_fork",
  "session_before_compact",
  "session_compact",
  "session_compact_failed",
  "session_shutdown",
  "session_before_tree",
  "session_tree",
  "input",
  "before_agent_start",
  "agent_start",
  "agent_end",
  "agent_settled",
  "turn_start",
  "turn_end",
  "message_start",
  "message_update",
  "message_end",
  "ui_prompt_start",
  "ui_prompt_end",
  "context",
  "before_provider_headers",
  "before_provider_request",
  "after_provider_response",
  "tool_execution_start",
  "tool_call",
  "tool_execution_update",
  "tool_result",
  "tool_execution_end",
  "model_select",
  "thinking_level_select",
  "user_bash",
]);

export const FABRIC_ACTOR_HOST_EVENTS = [
  ...FABRIC_ACTOR_PI_HOST_EVENTS,
  "tool_error",
] as const;

export type FabricActorHostEvent = (typeof FABRIC_ACTOR_HOST_EVENTS)[number];

const FABRIC_ACTOR_HOST_EVENT_SET: ReadonlySet<string> = new Set(FABRIC_ACTOR_HOST_EVENTS);

export const isFabricActorHostEvent = (value: unknown): value is FabricActorHostEvent =>
  typeof value === "string" && FABRIC_ACTOR_HOST_EVENT_SET.has(value);

export type FabricActorInferenceContext = "full-history" | "activation";

export function validateActorInferenceContext(value: unknown, runner?: string): asserts value is FabricActorInferenceContext | undefined {
  if (value !== undefined && value !== "full-history" && value !== "activation") {
    throw new Error(`Invalid actor inference context: ${String(value)}`);
  }
  if (value === "activation" && runner !== undefined && runner !== "pi") {
    throw new Error("Activation inference context requires the Pi runner");
  }
}

const COALESCE_KEY_PATTERN = /^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$/;

/** A dotted path into a mesh event's data, such as "payload.number" (smarty-dev#705). */
export function validateActorCoalesceKey(value: unknown): asserts value is string | undefined {
  if (value === undefined) return;
  if (typeof value !== "string" || value.length > 200 || !COALESCE_KEY_PATTERN.test(value)) {
    throw new Error(`Invalid actor coalesceKey: ${String(value)} (use a dotted path such as payload.number)`);
  }
}

export type FabricActorDelivery = "mailbox" | "steer" | "followUp" | "nextTurn";
export type FabricActorResponseMode = "text" | "directive";
export type FabricActorStatus = "idle" | "queued" | "running" | "stopped";
export type FabricActorBindingScope = "session" | "project";
export type FabricActorStorageScope = "session" | "project";

export interface FabricActorRunBinding {
  model?: string;
  thinking?: FabricThinking;
}

interface FabricActorBindingView extends FabricActorRunBinding {
  scope: "session";
  sessionId: string;
  updatedAt?: number;
}

interface FabricActorProjectDefaults extends FabricActorRunBinding {
  scope: "project";
}

export interface FabricActorValidWhileSource {
  version: 1;
  source: string;
}

export interface FabricActorValidityDecision {
  valid: boolean;
  reason?: string;
}

export type FabricActorActivation =
  | {
      kind: "hostEvent";
      id: string;
      source: string;
      sequence: number;
      createdAt: number;
      event: FabricActorHostEvent;
      mainRevision: number;
      taskRevision: number;
      signal?: unknown;
    }
  | {
      kind: "direct";
      id: string;
      source: string;
      sequence: number;
      createdAt: number;
    }
  | {
      kind: "mesh";
      id: string;
      source: string;
      sequence: number;
      createdAt: number;
      topic: string;
    };

export interface FabricActorValidityFacts {
  activation: FabricActorActivation;
  current: {
    latestActivationSequence: number;
    mainRevision: number;
    taskRevision: number;
    idle: boolean;
    now: number;
  };
}

export interface FabricActorRequest {
  /** Storage and visibility boundary. Defaults to mesh.actorScope for compatibility. */
  scope?: FabricActorStorageScope;
  name: string;
  instructions: string;
  /** Asynchronous observations of session-bound Pi events plus synthetic tool_error. */
  events?: FabricActorHostEvent[];
  topics?: string[];
  /** Defaults to mailbox. steer/followUp require an explicit triggerTurn choice. */
  delivery?: FabricActorDelivery;
  responseMode?: FabricActorResponseMode;
  /** Required for steer/followUp; must be false or omitted for mailbox/nextTurn. */
  triggerTurn?: boolean;
  coalesce?: boolean;
  /**
   * A dotted path into a mesh event's data, such as "payload.number". A queued event of the
   * same topic with the same value there is replaced by the newer one, in its queue place.
   */
  coalesceKey?: string;
  /** session actors stop with their Pi host; durable actors transfer to a resident host. */
  residency?: FabricParticipantResidency;
  runner?: FabricAgentRunner;
  /** Omitted/inherit resolves at creation; the actor keeps one language for its session. */
  kernel?: FabricKernel | "inherit";
  /** Host-only backend snapshot for persistent/resident sessions; not a provider argument. */
  pythonRuntime?: FabricPythonRuntime;
  model?: string;
  thinking?: FabricThinking;
  tools?: string[];
  transport?: FabricAgentTransport;
  timeoutMs?: number;
  /**
   * Fabric capability for the actor. Defaults to true (today's behavior: a Pi
   * actor is recursively Fabric-equipped with the host-required fabric_exec
   * tool). Set false to disable Fabric for a Pi actor: the activation runs with
   * extensions:false and recursive:false so fabric_exec is not injected and the
   * actor cannot call agents.* or mesh.*; the host still manages its mailbox
   * and delivery (same model as a Claude actor). This does not restrict the
   * actor's ordinary tool allowlist. Fixed at creation.
   */
  extensions?: boolean;
  /** Inference only; journals remain complete. Omitted means full-history. */
  inferenceContext?: FabricActorInferenceContext;
  /** Exact Fabric actions committed before every actor run. Optional entries do not block a run. */
  requires?: readonly (string | FabricCapabilityRequirement)[];
  /** Serialized guest predicate evaluated before work and before delivery. */
  validWhile?: FabricActorValidWhileSource;
}

export interface FabricActorInfo {
  id: string;
  scope: FabricActorStorageScope;
  name: string;
  /** sha256 (hex) of the actor's default instructions, as setInstructions stored them. */
  instructionsDigest?: string;
  /** Length of those instructions, in UTF-16 code units (JavaScript string length). */
  instructionsLength?: number;
  rootId?: string;
  /** The creating root's project; its project agent receives the actor's work (smarty-dev#878). */
  project?: string;
  status: FabricActorStatus;
  runner: FabricAgentRunner;
  kernel?: FabricKernel;
  pythonRuntime?: FabricPythonRuntime;
  events: FabricActorHostEvent[];
  topics: string[];
  delivery: FabricActorDelivery;
  responseMode: FabricActorResponseMode;
  triggerTurn: boolean;
  coalesce: boolean;
  coalesceKey?: string;
  residency?: FabricParticipantResidency;
  /** Effective value for this caller after session bindings overlay project defaults. */
  model?: string;
  /** Effective value for this caller after session bindings overlay project defaults. */
  thinking?: FabricThinking;
  binding?: FabricActorBindingView;
  projectDefaults?: FabricActorProjectDefaults;
  tools?: string[];
  timeoutMs?: number;
  extensions?: boolean;
  inferenceContext?: FabricActorInferenceContext;
  requirements?: FabricCapabilityRequirement[];
  capabilityDigest?: string;
  missingCapabilities?: string[];
  validWhile?: FabricActorValidWhileSource;
  queued: number;
  messages: number;
  createdAt: number;
  updatedAt: number;
  lastRunId?: string;
  lastError?: string;
  sessionFile?: string;
  logDir?: string;
}

export interface FabricActorLog {
  actorId: string;
  actorName: string;
  sessionFile: string;
  logDir: string;
  session: FabricLogLine[];
  sessionHasMore: boolean;
  sessionBefore?: number;
  run?: {
    runId: string;
    eventsFile: string;
    status?: AgentRunRecord;
    events: FabricLogLine[];
    hasMore: boolean;
    before?: number;
  };
  retainedRuns: string[];
}

export interface FabricActorMessage {
  id: string;
  actorId: string;
  actorName: string;
  direction: "in" | "out";
  source: string;
  createdAt: number;
  text?: string;
  data?: unknown;
  action?: "silent" | "message" | "stop";
  runId?: string;
  usage?: AgentUsage;
  error?: string;
  stale?: boolean;
  reason?: string;
}

export interface FabricActorDirective {
  action: "silent" | "message" | "stop";
  message?: string;
  data?: unknown;
}

export interface FabricActorDeliveryRequest {
  actor: FabricActorInfo;
  message: FabricActorMessage;
  delivery: Exclude<FabricActorDelivery, "mailbox">;
  triggerTurn: boolean;
}

/**
 * A project-independent actor template stored in the global registry
 * (the user's agent dir, not a project mesh). It carries only the actor
 * definition (the same fields as FabricActorRequest) plus identity and
 * timestamps — never any history (messages, session transcript, or run logs).
 * Global actors are not live: they are stamped into a project via import,
 * which creates a fresh live actor with no inherited history.
 */
export interface GlobalActorDefinition extends FabricActorRequest {
  id: string;
  createdAt: number;
  updatedAt: number;
  // Redeclared required: the registry always materializes these (defaults
  // applied on create and load), so they are never undefined on a stored
  // template. Keeping them required avoids undefined creeping into merges and
  // spreads under exactOptionalPropertyTypes.
  events: FabricActorHostEvent[];
  topics: string[];
  delivery: FabricActorDelivery;
  responseMode: FabricActorResponseMode;
  triggerTurn: boolean;
  coalesce: boolean;
  residency?: FabricParticipantResidency;
  runner: FabricAgentRunner;
}
