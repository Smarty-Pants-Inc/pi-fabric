import { FABRIC_ACTOR_HOST_EVENTS } from "../actors/types.js";
import { AGENT_WAIT_MAX_MS } from "../agents/wait-bound.js";
import { MAX_AGENT_REQUIRED_INPUTS, MAX_AGENT_REQUIRED_INPUT_BYTES } from "../host-compatibility.js";
import { MAX_ACTOR_BASH_TIMEOUT_S } from "../guards/actor-bash-timeout.js";
import {
  MAX_COMPACTION_INSTRUCTIONS_CHARS,
  MAX_PRESERVE_ITEM_CHARS,
  MAX_PRESERVE_ITEMS,
} from "../compaction/instructions.js";
import { FABRIC_LIFECYCLE_EVENTS } from "../lifecycle/types.js";
import type { FabricActionDescriptor } from "../protocol.js";

// RUNNER_ID_PATTERN (src/agents/runner-registry.ts) as source text; kept in sync by a test.
const RUNNER_ID_SOURCE = "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$";

const runProperties = {
  task: { type: "string", description: "A self-contained task for the child agent" },
  askId: { type: "string", minLength: 1, maxLength: 512, description: "Whole opaque Ask-ID value, copied byte-for-byte (at most 512 UTF-8 bytes; no newlines or control characters). Omitted inherits the spawning task's askId; otherwise WARN and MISSING." },
  name: { type: "string" },
  needs: { type: "array", items: { type: "string", minLength: 1 }, description: "Required placement target capabilities, NFKC-normalized then lowercased and split on commas/Unicode White_Space; tokens must match [a-z0-9._-], canonical local always pins to the Main host, and any unmet need stays local and is audited." },
  requires: { type: "array", maxItems: MAX_AGENT_REQUIRED_INPUTS, items: { type: "string", minLength: 1, maxLength: MAX_AGENT_REQUIRED_INPUT_BYTES }, description: "Literal absolute selected-host input paths, each at most 4096 UTF-8 bytes, without dot/dot-dot segments, line breaks or control characters. Local process spawns check existence before starting; unsupported transports refuse requires with AgentInputError; remote launches preflight via repeated --require PATH arguments." },
  runner: {
    type: "string",
    pattern: RUNNER_ID_SOURCE,
    maxLength: 64,
    description: "Execution harness: pi (default), pi-durable (opt-in isolated durable Pi host), claude, veda, or a runner registered through pi-fabric/runners. Defaults to agents.runner.",
  },
  kernel: {
    type: "string",
    enum: ["typescript", "python", "inherit"],
    description: "Fabric execution language. Omitted/inherit uses the caller executor.kernel; concrete choices require Pi with extensions enabled. Python uses the configured backend; CPython is native execution.",
  },
  transport: {
    type: "string",
    enum: ["auto", "process", "sandbox", "tmux", "screen", "localterm", "herdr"],
  },
  model: {
    type: "string",
    description:
      "Pi provider/id copied from agents.models({ runner: \"pi\" }), a configured models.aliases name, or a search term resolved to the closest authenticated model (recency from pi-model-sort breaks ties). Reuse returned keys; never infer version numbers from agent names. Exact keys win; near-miss IDs resolve to the closest visible model on the same provider. Handles report the canonical model. Without host model policy, Claude runtime values and Veda backend models/aliases are forwarded verbatim. Under active policy, Claude aliases must resolve through its native CLI catalog; Veda requires backend pi and an exact visible provider/model (unresolved aliases/defaults are refused).",
  },
  routeClass: { type: "string", pattern: "^[a-z][a-z0-9:-]{0,63}$", description: "Explicit class, never inferred from task text. model: auto supports bounded-lookup/status-groom on spawn and task:merge-additive, task:ci-test-fixture, task:exact-checks on spawn/run. Live requires trusted liveClasses opt-in and clear protection." },
  protected: { type: "boolean", description: "Trusted issue/PR protection snapshot, never task text: true for review/security/audit/named passes/needs-security-pass; false only for known-clear state. Omitted stays unknown and excluded from routing." },
  modelReason: {
    type: "string",
    maxLength: 200,
    description: "Named exception for an explicit model selection, recorded on the run/actor and spawn event. Required and non-blank for agents.modelPolicy.requireReason (defaults: gpt-6-astra, opus, claude on any provider). Claude/Opus exceptions must name an issue (#N or an http(s) /issues/N URL); arbitrary reasons do not grant an exception. Omit model to use the role default. See smarty-dev#3134.",
  },
  opusReason: {
    type: "string",
    maxLength: 200,
    description: "Compatibility alias for modelReason; modelReason wins when both are supplied. Claude/Opus exceptions must name an issue (#N or an http(s) /issues/N URL).",
  },
  persona: {
    type: "string",
    description: "Veda persona name for this run, such as frontend, reviewer, worker, or a custom persona.",
  },
  thinking: {
    type: "string",
    enum: ["off", "minimal", "low", "medium", "high", "xhigh", "max"],
  },
  thinkingBounds: {
    type: "object",
    properties: {
      min: { type: "string", enum: ["off", "minimal", "low", "medium", "high", "xhigh", "max"] },
      max: { type: "string", enum: ["off", "minimal", "low", "medium", "high", "xhigh", "max"] },
    },
    additionalProperties: false,
    description: "Optional child thinking bounds; must lie inside this session's bounds (omitted ends inherit). Child Fabric sessions never widen them. thinking outside the effective bounds is clamped and reported as requestedThinking.",
  },
  tools: { type: "array", items: { type: "string" } },
  nice: {
    type: "integer",
    minimum: 0,
    maximum: 19,
    description: "Unix niceness for this child and its tools. Only raises agents.nice, never lowers it.",
  },
  bashIdleSeconds: {
    type: "integer",
    minimum: 0,
    maximum: MAX_ACTOR_BASH_TIMEOUT_S,
    description: "Pi runs: kill a blocking bash call after this many seconds with no output (default 180), also when the call sets its own timeout; 0 = no idle limit.",
  },
  timeoutMs: {
    type: "number",
    description:
      "Optional hard wall-clock limit in milliseconds for lane/audit timeboxes, capped at 86400000 (24 hours). One-shot tasks may shorten agents.timeoutMs only with values >=300000 (5 minutes); smaller shortenings are ignored with a typed timeoutNote to protect real work from tiny model-chosen budgets. Omit to keep agents.timeoutMs (24 hours by default). Persistent actor overrides only extend their default.",
  },
  extensions: { type: "boolean" },
  recursive: { type: "boolean" },
  cwd: {
    type: "string",
    description: "Filesystem execution directory for leaf or recursive Pi runs; relative paths resolve from the caller cwd. Does not change project/mesh ownership or grant target project trust.",
  },
  worktree: { type: "boolean" },
  worktreeSetup: {
    type: "string",
    maxLength: 8192,
    description: "Shell command run in a new worktree before the child starts; overrides agents.worktree.setup. Non-zero exit fails the run.",
  },
  seed: {
    type: "string",
    enum: ["task", "branch", "snippet"],
    description: "Child context (Pi only). task (default): the task alone. branch: a non-blocking fork of this conversation up to its last completed turn, then the task. snippet: the last seedMessages user/assistant texts prefixed to the task.",
  },
  seedMessages: { type: "integer", minimum: 1, maximum: 50, description: "snippet only; default 12" },
  writePolicy: {
    type: "string",
    enum: ["unrestricted"],
    description: "Audited opt-out of the process task default on non-overlapping layouts (write/edit restricted to cwd and run/tmp; host-owned run records excluded; shells allowed). A run dir inside cwd skips only the implicit default and logs #11705; explicit writableRoots are unchanged. Cannot be combined with readOnly/writableRoots/shell or escape inherited confinement.",
  },
  readOnly: { type: "boolean", description: "Pi only: refuse write/edit (and shell unless shell: \"unconfined\") in the child" },
  writableRoots: {
    type: "array",
    items: { type: "string", maxLength: 4096 },
    maxItems: 32,
    description: "Pi only: confine write/edit to these directories (relative to the child cwd; default the cwd). Shell is refused unless shell: \"unconfined\".",
  },
  scope: {
    type: "object",
    properties: {
      grants: {
        type: "array",
        maxItems: 64,
        items: {
          type: "object",
          properties: {
            resource: { type: "string", minLength: 3, maxLength: 512 },
            actions: { type: "array", minItems: 1, maxItems: 3, items: { type: "string", enum: ["read", "write", "execute"] } },
          },
          required: ["resource", "actions"],
          additionalProperties: false,
        },
      },
    },
    required: ["grants"],
    additionalProperties: false,
    description: "Narrow this session's host-issued scope for the child (each grant must be covered by a parent grant; resource <ns>:<path>, <ns>:<path>/*, <ns>:<path>/**, <ns>:*). Omit to inherit it; refused in an unscoped session.",
  },
  shell: {
    type: "string",
    enum: ["deny", "unconfined"],
    description: "With a write policy, bash is refused by default; unconfined allows it and is not a sandbox",
  },
  schema: { type: "object", description: "Optional JSON Schema for validated structured output" },
  systemPrompt: {
    type: "string",
    description: "Optional extra system prompt body for this child run. Pi runners merge it below component guidance and forward it via --system-prompt; Claude runners receive it via --append-system-prompt. Useful for reliability-focused prompt rules on models with weak behavioral defaults.",
  },
  persistSession: {
    type: "boolean",
    description: "Claude runner only: persist the Claude Code transcript for hook-based observability. Defaults to false.",
  },
};

const strictModelProperty = {
  ...runProperties.model,
  description: "Pi exact provider/id or model id copied from agents.models({ runner: \"pi\" }), or an exact configured models.aliases name. Selectors requiring closest-match ranking are refused with candidate keys; pass an exact key or configure an alias. Handles report the canonical model. Without host model policy, Claude runtime values and Veda backend models/aliases are forwarded verbatim. Under active policy, Claude aliases must resolve through its native CLI catalog; Veda requires backend pi and an exact visible provider/model (unresolved aliases/defaults are refused).",
};

const runSchema = {
  type: "object",
  properties: { ...runProperties,
    pinModel: { type: "string", description: "Explicit role pin for model: auto with a named task class." },
    pinThinking: runProperties.thinking,
  },
  required: ["task"],
  additionalProperties: false,
};

const residencySchema = {
  type: "string",
  enum: ["session", "durable"],
  description: "session stops with the current Pi host; durable transfers execution to Fabric's hidden resident host.",
};

const actorSlotClaimSchema = {
  type: "object",
  description: "Exact won claim revision for an enforced native review slot. Reuse unchanged with the same idempotencyKey; never refresh or steal after refusal/uncertainty.",
  properties: {
    key: { type: "string", minLength: 1 },
    ifVersion: { type: "integer", minimum: 1, maximum: Number.MAX_SAFE_INTEGER },
  },
  required: ["key", "ifVersion"],
  additionalProperties: false,
};

const actorBindingScopeSchema = {
  type: "string",
  enum: ["session", "project", "global"],
  description: "session (default) changes this root's live session binding or a foreign caller's local overlay; project pins the shared default and requires ownership; global updates a non-live template.",
};

const actorInvocationProperties = {
  id: { type: "string" },
  message: { type: "string" },
  data: {},
  model: {
    ...runProperties.model,
    description: "Optional model pinned only for this actor activation. Pi overrides refuse ranked closest matches; use an exact model id or configured alias.",
  },
  thinking: runProperties.thinking,
};

const residentIdempotencyKeySchema = {
  type: "string",
  minLength: 1,
  maxLength: 256,
  description: "Stable creation retry identity. Enforced native slots require a caller-supplied key and unchanged request: retries retain the original admitted ID across restarts, and retired slots never reopen. Ordinary durable create/spawn uses a host-local receipt cache (last 256 completed requests, up to 10 minutes); ordinary session/global creation is not deduplicated.",
};

const spawnSchema = {
  ...runSchema,
  properties: {
    ...runProperties, residency: residencySchema,
    complexity: { type: "string", enum: ["simple", "normal", "complex", "delicate"], description: "Caller complexity hint for the optional host spawn router; delicate is a distinct delicate-code class, never mapped to complex or inferred from task text." },
    profile: { type: "string", enum: ["lead", "lane", "task", "org", "actor"], description: "Optional sandbox placement profile." },
    branch: { type: "string", minLength: 1, maxLength: 255, pattern: "^[A-Za-z0-9][A-Za-z0-9._/-]*$", description: "Optional sandbox ASCII Git branch name (without refs/heads/); omit when unused, never null." },
    sessionImport: { type: "object", properties: { session: { type: "string", minLength: 36, maxLength: 36, pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" }, manifestSha256: { type: "string", minLength: 64, maxLength: 64, pattern: "^[0-9a-f]{64}$" } }, required: ["session", "manifestSha256"], additionalProperties: false, description: "Optional session capsule import pin; requires sandbox transport, branch and a lead/lane profile (defaults to lane for an import). Omit when unused, never null." },
    idempotencyKey: residentIdempotencyKeySchema,
    model: { ...runProperties.model, description: `${strictModelProperty.description} \"auto\" records a finite Choice; trusted liveClasses can launch it, otherwise the child runs pinModel/pinThinking.` },
    pinModel: { type: "string", description: "Role's required Pi model pin; overrides agents.modelRouting.pinModel." },
    pinThinking: { ...runProperties.thinking, description: "Role's required effort pin; overrides agents.modelRouting.pinThinking. Never inferred from the default medium effort." },
  },
};

const handoffCompactionSchema = {
  anyOf: [
    { type: "boolean" },
    {
      type: "object",
      properties: {
        instructions: {
          type: "string",
          maxLength: MAX_COMPACTION_INSTRUCTIONS_CHARS,
          description: "Custom compaction instructions for the inherited trajectory",
        },
        preserve: {
          type: "array",
          items: { type: "string", maxLength: MAX_PRESERVE_ITEM_CHARS },
          maxItems: MAX_PRESERVE_ITEMS,
          description: "Explicit bounded facts the trajectory summary must preserve",
        },
      },
      additionalProperties: false,
    },
  ],
  description:
    "Compact the inherited trajectory with Fabric's deterministic compactor before the executor resumes it. `true` applies the default summary; an object customizes instructions and bounded preserve facts. Omitted keeps the full raw trajectory.",
};

const handoffSchema = {
  type: "object",
  properties: {
    task: {
      type: "string",
      description: "Optional instructions for the executor in addition to the inherited trajectory",
    },
    name: runProperties.name,
    kernel: runProperties.kernel,
    transport: runProperties.transport,
    model: {
      ...strictModelProperty,
      description: "Explicit Pi exact provider/id, model id, or configured alias target that will continue the inherited trajectory. Closest-match selectors are refused with candidate keys.",
    },
    thinking: runProperties.thinking,
    routeClass: runProperties.routeClass,
    protected: runProperties.protected,
    tools: runProperties.tools,
    timeoutMs: runProperties.timeoutMs,
    extensions: runProperties.extensions,
    recursive: runProperties.recursive,
    schema: runProperties.schema,
    compact: handoffCompactionSchema,
  },
  required: ["model"],
  additionalProperties: false,
};

const idSchema = {
  type: "object",
  properties: { id: { type: "string" } },
  required: ["id"],
  additionalProperties: false,
};

const waitSchema = {
  type: "object",
  properties: {
    id: { type: "string" },
    timeoutMs: { type: "number", minimum: 1_000, description: `Clamped to ${AGENT_WAIT_MAX_MS / 60_000} min: a foreground wait over the bash guard's limit blocks steers (smarty-dev#854). In an interactive Main, clamped to 60 s, and the bound returns the live status with waitTimedOut: true (smarty-dev#2119)` },
  },
  required: ["id"],
  additionalProperties: false,
};

const lifecycleEventSchema = {
  type: "string",
  enum: [...FABRIC_LIFECYCLE_EVENTS],
};

const activationFilterSchema = {
  type: "array",
  maxItems: 32,
  description: "Skip-only rules checked before a queued mesh or host event runs the model: preset names (hold, never-message-events) or rule objects { id, source?, topic?, kind?, where?, unless? }. A skipped event is logged as 'filtered: <rule id>' and counted; it never acts or replies.",
  items: {
    anyOf: [
      { type: "string", enum: ["hold", "never-message-events"] },
      { type: "object" },
    ],
  },
};
const actorInstructionsProperties = {
  instructions: { type: "string" },
  instructionsFile: { type: "string", minLength: 1, description: "Instructions file on the owning host under agents.instructionsRoot (default ~/.local/share/smarty-dev/factory/current/). Regular UTF-8 file, max 512 KB; no '..' or symlink escape." },
  sha256: { type: "string", pattern: "^[a-f0-9]{64}$", description: "Exact lowercase SHA256 of the instructionsFile bytes. Required with instructionsFile; mutually exclusive with inline instructions." },
};
const actorInstructionsSourceSchema = {
  oneOf: [
    { required: ["instructions"], not: { anyOf: [{ required: ["instructionsFile"] }, { required: ["sha256"] }] } },
    { required: ["instructionsFile", "sha256"], not: { required: ["instructions"] } },
  ],
};
export const AGENTS_ACTION_DESCRIPTORS: FabricActionDescriptor[] = [
  {
    name: "run",
    description: "Run a child agent through Pi or Claude Code and wait for its final result",
    inputSchema: runSchema,
    risk: "agent",
  },
  {
    name: "handoff",
    description:
      "Schedule a Pi trajectory handoff after the current outer fabric_exec result, then wait for implementation at that boundary",
    inputSchema: handoffSchema,
    risk: "agent",
  },
  {
    name: "spawn",
    description:
      "Start a child agent through Pi or Claude Code and return a handle immediately. For independent launches, await Promise.allSettled and inspect every result so one rejection does not abort pending sibling calls at program exit. Unread detached results are batched for the immediate spawner at the next safe turn boundary (or wake the idle spawner) when agents.notifyOnComplete is enabled. wait/join and terminal status acknowledge results and retract pending notifications. Use wait when this program needs the result; do not poll status in a loop.",
    inputSchema: spawnSchema,
    risk: "agent",
  },
  {
    name: "wait",
    description: "Wait for a previously spawned child agent and acknowledge its result, suppressing a duplicate completion notification. Bounded by timeoutMs (default and at most 5 min): a child still running then keeps running, the wait throws, and its result arrives as a completion message after the turn",
    effect: { kind: "emission", ordering: "commutative", resources: ["agents.completions"] },
    inputSchema: waitSchema,
    risk: "read",
  },
  {
    name: "join",
    description: "Alias for agents.wait: wait for a previously spawned child agent with the same bound, progress and completion-notification behavior",
    effect: { kind: "emission", ordering: "commutative", resources: ["agents.completions"] },
    inputSchema: waitSchema,
    risk: "read",
  },
  {
    name: "status",
    description: "Get the latest status of any known project participant. A terminal child-agent result acknowledges its pending completion notification; running status does not.",
    effect: { kind: "emission", ordering: "commutative", resources: ["agents.completions"] },
    inputSchema: idSchema,
    risk: "read",
  },
  {
    name: "list",
    description: "List agent participants locally, across the current lineage, or across the project",
    inputSchema: {
      type: "object",
      properties: { scope: { type: "string", enum: ["local", "lineage", "project"] } },
      additionalProperties: false,
    },
    risk: "read",
  },
  {
    name: "members",
    description: "List the unified project topology of roots, agents, and actors",
    inputSchema: {
      type: "object",
      properties: {
        scope: { type: "string", enum: ["local", "lineage", "project"] },
        kinds: {
          type: "array",
          items: { type: "string", enum: ["root", "agent", "actor"] },
        },
        includeStale: { type: "boolean" },
      },
      additionalProperties: false,
    },
    risk: "read",
  },
  {
    name: "self",
    description: "Return this caller's intrinsic participant identity in the unified topology",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    risk: "read",
  },
  {
    name: "spawner",
    description: "Return this child's immediate spawning participant and activation run. Use id: 'spawner' with agents.followUp/steer to reply to it. An actor spawner is NOT the lineage root returned by agents.main; no root fallback when the binding is absent.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    risk: "read",
  },
  {
    name: "main",
    description:
      "Return the root user-facing Main Pi agent target. The stable alias main is also accepted by agents.steer and agents.followUp.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    risk: "read",
  },
  {
    name: "sessions",
    description: "List all live root Pi session agents in the project, including the current lineage root and peers, with symmetric participant identities.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    risk: "read",
  },
  {
    name: "peers",
    description: "List other live root Pi sessions sharing this project mesh, with their role and project when their runtime publishes them. The dashboard-owning session remains Main; these targets are named peers.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    risk: "read",
  },
  {
    name: "projectAgent",
    description: "Return this session's interactive project lead by normalized repository origin, using the lead id recorded at launch (SMARTY_LEAD_SESSION or .local/lead) to resolve ambiguity and moved lanes. Unrecorded bridge mirrors cannot claim leadership. Throws a named error when unresolved or ambiguous.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    risk: "read",
  },
  {
    name: "subscribe",
    description:
      "Create a durable source-qualified participant lifecycle subscription. Events are delivered to Main by default or to another participant through steer/follow-up routing.",
    inputSchema: {
      type: "object",
      properties: {
        from: { type: "string", description: "Exact source participant id; main means this lineage root" },
        events: { type: "array", minItems: 1, items: lifecycleEventSchema },
        to: { type: "string", description: "Target participant id; defaults to main" },
        delivery: { type: "string", enum: ["steer", "followUp"] },
        triggerTurn: { type: "boolean" },
        once: { type: "boolean" },
      },
      required: ["from", "events", "delivery", "triggerTurn"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "subscriptions",
    description: "List durable participant lifecycle subscriptions, optionally filtered by source or target",
    inputSchema: {
      type: "object",
      properties: { from: { type: "string" }, to: { type: "string" } },
      additionalProperties: false,
    },
    risk: "read",
  },
  {
    name: "unsubscribe",
    description: "Remove a participant lifecycle subscription",
    inputSchema: idSchema,
    risk: "agent",
  },
  {
    name: "models",
    description:
      "List models exposed by the selected runner. Claude models are enumerated from the installed Claude Code runtime, not hard-coded.",
    inputSchema: {
      type: "object",
      properties: {
        runner: { type: "string", pattern: RUNNER_ID_SOURCE, maxLength: 64 },
        refresh: { type: "boolean" },
      },
      additionalProperties: false,
    },
    risk: "execute",
  },
  {
    name: "switchModel",
    description:
      "Switch Main's live Pi session model in place. The model selector accepts an exact provider/id, a configured models.aliases name (alias chains try each target in order until one is authenticated), an exact model id, or a search term; inexact terms resolve to the closest match, preferring recently used models (via pi-model-sort usage, when present).",
    inputSchema: {
      type: "object",
      properties: {
        model: {
          type: "string",
          description: "provider/id, alias name, or search term",
        },
        provider: { type: "string" },
      },
      required: ["model"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "stop",
    description: "Stop a local or remotely owned agent or actor that advertises the stop capability. An actor holding queued, parked, overflow or dead-letter work is refused with the counts unless dropQueued: true, which moves that work to <actor dir>/stopped-queues/<stamp>-<sha256-12>.jsonl (never deleted; the result names it) and stops. agents.requeueArchived restores it.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        dropQueued: { type: "boolean", description: "Actors only: stop even with queued work, archiving it instead of refusing." },
      },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "requeueArchived",
    description: "Feed a stop archive (the stoppedQueue.archive path a dropQueued stop returned) to a live, usually recreated, actor through its normal arrival path and activation filter, once per archived item. Idempotent: the actor records the item ids it was offered, so a second call adds nothing. Items the actor cannot take (a full queue and overflow, and no dead-letter room within 90% of its bounds) come back in notQueued. Their notQueued ledger lines do not count as requeued, so a later call offers them again.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        archive: { type: "string", description: "<actor dir>/stopped-queues/<stamp>-<sha256-12>.jsonl" },
      },
      required: ["id", "archive"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "cleanup",
    description: "Remove a completed agent's run files and optional Git worktree",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        deleteBranch: { type: "boolean" },
      },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "write",
  },
  {
    name: "slotStatus",
    description: "Read native slot-admission status. session (default) inspects this runtime's store; durable queries the actual existing selected resident host/store without starting one. Diagnostic only: cannot enable enforcement, import existing authority (#13036), or grant a claim.",
    inputSchema: {
      type: "object",
      properties: { residency: residencySchema },
      additionalProperties: false,
    },
    risk: "read",
  },
  {
    name: "create",
    description:
      'Create a persistent actor with inline instructions or instructionsFile + sha256 verified by its owning host (max 512 KB, factory root only). Use scope "global" for a reusable template.',
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string" },
        ...actorInstructionsProperties,
        events: {
          type: "array",
          items: {
            type: "string",
            enum: [...FABRIC_ACTOR_HOST_EVENTS],
          },
        },
        topics: { type: "array", items: { type: "string" } },
        eventsStartAt: {
          oneOf: [
            { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER },
            { type: "string", minLength: 1, pattern: "^\\d{4}-\\d{2}-\\d{2}T" },
          ],
          description: "Epoch milliseconds or an ISO-8601 timestamp for the first NATS durable; omitted replays all retained events.",
        },
        delivery: {
          type: "string",
          enum: ["mailbox", "steer", "followUp", "nextTurn"],
        },
        responseMode: { type: "string", enum: ["text", "directive"] },
        triggerTurn: { type: "boolean" },
        coalesce: { type: "boolean" },
        coalesceKey: { type: "string", description: "Dotted path into a mesh event's data (such as payload.number). Queued events of the same topic and scalar value share one run: newest primary payload plus sequence-ordered coalesced history (bounded full data and metadata; explicit older-omission count/range and read-back hint)." },
        dedupeKey: { type: "string", maxLength: 200, pattern: "^[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*$", description: "Opt-in occurrence path into the full mesh event (such as data.key). Persistent durable actors skip the last 256 successfully completed keys across restart. Never inferred from coalesceKey." },
        activation: {
          type: "object",
          description: "Optional minimum interval per actor/source session for host agent_settled only. First is admitted immediately; the latest event inside the interval is admitted once at the original window end, without sliding the deadline. Persistent pending delivery survives manager close/restart and is restored on start, re-armed if not yet due. Stop, removal, redefinition or halt drops it. Omitted or 0 is off.",
          properties: { minIntervalMs: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER } },
          required: ["minIntervalMs"],
          additionalProperties: false,
        },
        batchWindowMs: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER, description: "Batch all matched observations for this leading-edge window (ms). Default 0 keeps single-event behavior. Direct asks, steer and Main-origin messages flush pending work immediately." },
        batchMax: { type: "integer", minimum: 1, maximum: 1000, description: "Maximum events per activation; reaching this count flushes the window. Default 50. Queue limits and receipts remain per event." },
        batchMaxBytes: { type: "integer", minimum: 1, maximum: Number.MAX_SAFE_INTEGER, description: "Aggregate serialized event/image byte budget per activation. Default 256 KiB; flush before crossing it. A single oversized event runs alone." },
        activationFilter: activationFilterSchema,
        deadLetterReplayMaxAgeMs: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER, description: "Dead-letter replay age from each entry's recorded time (default 6 h). Older entries are archived, never deleted. 0 = unlimited." },
        routeClass: { type: "string", enum: ["status-groom"], description: "Per-activation shadow Choice for checks/grooming producing a status line or no-op; explicit model/thinking pins required." },
        protected: { type: "boolean", description: "Trusted protection snapshot; true for review/security/audit/needs-security-pass. Omitted excludes before Jev." },
        residency: residencySchema,
        idempotencyKey: residentIdempotencyKeySchema,
        slotClaim: actorSlotClaimSchema,
        runner: runProperties.runner,
        kernel: runProperties.kernel,
        model: strictModelProperty,
        modelReason: runProperties.modelReason,
        opusReason: runProperties.opusReason,
        thinking: runProperties.thinking,
        tools: runProperties.tools,
        transport: runProperties.transport,
        timeoutMs: runProperties.timeoutMs,
        nice: runProperties.nice,
        bashTimeoutSeconds: { type: "integer", minimum: 0, maximum: MAX_ACTOR_BASH_TIMEOUT_S, description: "Default timeout in seconds for a bash call without one in this actor's runs (default 600, maximum 2147483); 0 = no default timeout." },
        extensions: runProperties.extensions,
        inferenceContext: { type: "string", enum: ["full-history", "activation"], description: "Inference-only activation window (Pi only); journals remain complete. Default full-history." },
        requires: {
          type: "array",
          maxItems: 128,
          description: "Exact Fabric provider.action refs committed before every actor run. Object entries may be optional.",
          items: {
            oneOf: [
              { type: "string", minLength: 3, maxLength: 256 },
              {
                type: "object",
                properties: {
                  ref: { type: "string", minLength: 3, maxLength: 256 },
                  optional: { type: "boolean" },
                },
                required: ["ref"],
                additionalProperties: false,
              },
            ],
          },
        },
        records: {
          type: "object",
          description: "Host-maintained records topic for validWhile; default 512 entries (cap 4096), maxAgeMs defaults to 6 h; unknown records fail open.",
          properties: {
            topic: { type: "string", pattern: "^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$" },
            maxEntries: { type: "integer", minimum: 1 },
            maxAgeMs: { type: "integer", minimum: 1, maximum: Number.MAX_SAFE_INTEGER, default: 21_600_000 },
          },
          required: ["topic"],
          additionalProperties: false,
        },
        validWhile: {
          type: "object",
          properties: { version: { const: 1 }, source: { type: "string" } },
          required: ["version", "source"],
          additionalProperties: false,
        },
        scope: {
          type: "string",
          enum: ["session", "project", "global"],
          description: "session isolates the actor to the root Pi session; project shares it across sessions; global creates a non-live template.",
        },
      },
      required: ["name"],
      allOf: [actorInstructionsSourceSchema],
      oneOf: [
        {
          properties: {
            delivery: { const: "mailbox" },
            triggerTurn: { const: false },
          },
        },
        {
          properties: {
            delivery: { const: "nextTurn" },
            triggerTurn: { const: false },
          },
          required: ["delivery"],
        },
        {
          properties: { delivery: { enum: ["steer", "followUp"] } },
          required: ["delivery", "triggerTurn"],
        },
      ],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "ask",
    description: "Send a message to a persistent actor through its live owner and wait for its next response. Optional model/thinking values apply only to this activation.",
    inputSchema: {
      type: "object",
      properties: actorInvocationProperties,
      required: ["id", "message"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "tell",
    description: "Queue a message through a persistent actor's live owner without waiting. Optional model/thinking values apply only to this activation.",
    inputSchema: {
      type: "object",
      properties: { ...actorInvocationProperties,
        idempotencyKey: { type: "string", minLength: 1, maxLength: 200, description: "For remote messages, reuse the same key and unchanged input on an outcome-unknown or FABRIC_PARTICIPANT_STALE retry." } },
      required: ["id", "message"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "steer",
    description:
      "Steer Main, a running one-shot agent between turns, or a persistent actor through its mailbox. Optional priority interrupt requires owning-root/supervisor or host allowlist authority, aborts at most once per Main turn, and has a 60-second sender cooldown. It delivers the steer next without retrying the tool. Default delivery is unchanged. Non-local targets route over the project mesh.",
    inputSchema: {
      type: "object",
      properties: { id: { type: "string" }, message: { type: "string" }, data: {},
        priority: { type: "string", enum: ["interrupt"], description: "Main only; separate interrupt authority required. One abort per turn, 60-second sender cooldown. Abort its tool, then deliver the steer next." },
        idempotencyKey: { type: "string", minLength: 1, maxLength: 200, description: "For remote messages, reuse the same key and unchanged input on a FABRIC_PARTICIPANT_STALE retry." } },
      required: ["id", "message"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "followUp",
    description:
      "Queue a follow-up for Main, a task or an actor. Local Pi tasks have a 10 minute delivery deadline (override with deadlineMs), one sender alarm if late, and remain queued until a boundary or cancelFollowUp. Explicit deadlines require a local Pi task.",
    inputSchema: {
      type: "object",
      properties: { id: { type: "string" }, message: { type: "string" }, data: {}, deadlineMs: { type: "integer", minimum: 1 },
        idempotencyKey: { type: "string", minLength: 1, maxLength: 200, description: "For remote messages, reuse the same key and unchanged input on a FABRIC_PARTICIPANT_STALE retry." } },
      required: ["id", "message"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "cancelFollowUp",
    description: "Cancel one deadline-tracked follow-up to a local Pi task by its receipt messageId. Does not stop the receiver. Returns delivered if delivery already won the race.",
    inputSchema: {
      type: "object",
      properties: { id: { type: "string" }, messageId: { type: "string" } },
      required: ["id", "messageId"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setSteeringMode",
    description:
      "Set how queued steer messages are delivered to a running one-shot agent: all at once after the current turn, or one per turn (default). Local agent only.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        mode: { type: "string", enum: ["all", "one-at-a-time"] },
      },
      required: ["id", "mode"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setFollowUpMode",
    description:
      "Set how queued follow-up messages are delivered to a one-shot agent: all when it finishes, or one per completion (default). Local agent only.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        mode: { type: "string", enum: ["all", "one-at-a-time"] },
      },
      required: ["id", "mode"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "compact",
    description:
      "Request an advisory compaction of a running Pi-runner child agent's context at its next safe boundary (between its own turns), preserving the child's accumulated context. Rejected for Claude-runner children. The child pi core applies the compaction; Fabric only forwards the intent.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        instructions: {
          type: "string",
          description: "Optional custom compaction instructions forwarded to the child pi",
        },
      },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "actorStatus",
    description: "Read one persistent actor's status",
    inputSchema: idSchema,
    risk: "read",
  },
  {
    name: "instructions",
    description:
      "Read a live actor's current instruction text with its sha256 instructionsDigest and instructionsLength. Use it to check the text before and after agents.setInstructions. Writes nothing.",
    inputSchema: idSchema,
    risk: "read",
  },
  {
    name: "actors",
    description:
      'List persistent actors. Default scope "project" lists live actors in this Fabric session; scope "global" lists project-independent templates in the global registry.',
    inputSchema: {
      type: "object",
      properties: { scope: { type: "string", enum: ["project", "global"] } },
      additionalProperties: false,
    },
    risk: "read",
  },
  {
    name: "messages",
    description: "Read a persistent actor's bounded inbox and outbox history",
    inputSchema: {
      type: "object",
      properties: { id: { type: "string" }, limit: { type: "number", minimum: 1 } },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "read",
  },
  {
    name: "setModel",
    description:
      "Change or clear an actor model binding. Main setModel targets (own or remote session:<id>) are refused before mutation; deferred to smarty-dev#4153 pending a cancellation-aware native commit guard.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        model: { type: "string" },
        modelReason: runProperties.modelReason,
        opusReason: runProperties.opusReason,
        scope: actorBindingScopeSchema,
      },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setThinking",
    description:
      "Change or clear an actor reasoning-effort binding, or set this session's own live Main via session:<id>. Main changes use Pi native next-turn state, return previous/current readback, and support session scope only. Remote Main changes are not supported yet (smarty-dev#4153).",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        thinking: runProperties.thinking,
        scope: actorBindingScopeSchema,
      },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setTools",
    description:
      "Replace a persistent actor's tool allowlist. Takes effect on its next queued message; an empty list disables optional tools.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        tools: runProperties.tools,
        scope: { type: "string", enum: ["project", "global"] },
      },
      required: ["id", "tools"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setCoalesceKey",
    description: "Set (a dotted path into a mesh event's data, such as payload.number) or clear (null) the actor's queue coalesce key. Matching queued events share one run with the newest primary payload and sequence-ordered coalesced history (bounded full data and metadata; explicit older-omission count/range and read-back hint). A running activation is never replaced.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        coalesceKey: { type: ["string", "null"] },
        scope: { type: "string", enum: ["project", "global"] },
      },
      required: ["id", "coalesceKey"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setBatching",
    description: "Set actor window batching. First matched observation anchors the deadline; batchMax flushes early. Direct asks, steer and Main-origin messages flush older observations first, then run separately. Positive windows retain events instead of coalescing them. Applies to future runs without replacing an active activation.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        batchWindowMs: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER },
        batchMax: { type: "integer", minimum: 1, maximum: 1000 },
        batchMaxBytes: { type: "integer", minimum: 1, maximum: Number.MAX_SAFE_INTEGER },
        scope: { type: "string", enum: ["project", "global"] },
      },
      required: ["id"],
      anyOf: [{ required: ["batchWindowMs"] }, { required: ["batchMax"] }, { required: ["batchMaxBytes"] }],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setActivationFilter",
    description: "Set or clear (null or []) the actor's skip-only activation filter: preset names (hold, never-message-events) or rule objects. Invalid rules are rejected. Applies from the next queued event and resets filterSkipped. If the new filter would skip already queued or overflow items, it is refused with the count unless dropQueued: true, which moves those items to the actor's dead-letter queue (never deleted; movedToDeadLetter on the result). Optional expiresAt (epoch ms, live actors only) clears on the next event or poll, with an actor audit message.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        activationFilter: { anyOf: [activationFilterSchema, { type: "null" }] },
        expiresAt: { type: "number", description: "Epoch milliseconds; clear at/after this time on the next event or poll. Live actors only." },
        deadLetterReplayMaxAgeMs: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER, description: "Also set the replay age limit in milliseconds; 0 = unlimited. Omitted preserves the actor's current limit (default 6 h)." },
        dropQueued: { type: "boolean", description: "Live actors only: move queued items the new filter skips to the dead-letter queue instead of refusing." },
        scope: { type: "string", enum: ["project", "global"] },
      },
      required: ["id", "activationFilter"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setNice",
    description: "Set an actor's Unix niceness (0-19) for future runs; it only raises agents.nice, never lowers it.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        nice: runProperties.nice,
        scope: { type: "string", enum: ["project", "global"] },
      },
      required: ["id", "nice"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setInferenceContext",
    description: "Select a same-ID actor inference policy for future activations. Running work keeps its snapshot; journals remain complete.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        inferenceContext: { type: "string", enum: ["full-history", "activation"] },
        scope: { type: "string", enum: ["project", "global"] },
      },
      required: ["id", "inferenceContext"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setTopics",
    description: "Replace a locally owned ordinary actor's exact mesh-topic subscriptions, or a global template's topics. Native slot-bound topics are immutable; live changes from or into protected topics are refused. An empty list stops topic intake while accepted work drains; direct messages, addressed mesh events and host-event subscriptions are unchanged. Restoring a topic accepts new events again, without replaying events already passed by the monitor.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        topics: { type: "array", items: { type: "string" } },
        scope: { type: "string", enum: ["project", "global"] },
      },
      required: ["id", "topics"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setEvents",
    description: "Replace a persistent actor's session-bound Pi and synthetic tool_error event subscriptions",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        events: {
          type: "array",
          items: {
            type: "string",
            enum: [...FABRIC_ACTOR_HOST_EVENTS],
          },
        },
      },
      required: ["id", "events"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setDeliveryPolicy",
    description:
      "Replace a project actor or global template delivery policy. steer/followUp require an explicit triggerTurn choice; mailbox/nextTurn require false.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        delivery: {
          type: "string",
          enum: ["mailbox", "steer", "followUp", "nextTurn"],
        },
        triggerTurn: { type: "boolean" },
        scope: { type: "string", enum: ["project", "global"] },
      },
      required: ["id", "delivery", "triggerTurn"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "clearMessages",
    description: "Clear a persistent actor's recorded message history",
    inputSchema: idSchema,
    risk: "write",
  },
  {
    name: "resetSession",
    description:
      "Start a persistent actor's next run on a fresh Pi session. The owning Main requests resident reset directly; an admitted activation settles at the fenced boundary while other resident commands remain serviceable. Explicit stop cancels work and any pending reset; it is not preparation for repair. The old session is archived beside it (2 kept). Instructions, topics, bindings, the queue and messages are kept.",
    inputSchema: idSchema,
    risk: "agent",
  },
  {
    name: "remove",
    description:
      'Stop and remove a persistent actor. Default scope "project" removes a live project actor; scope "global" removes a project-independent template from the global registry. Refused before any effect (FABRIC_ACTOR_QUEUED_WORK, with the counts) while the actor keeps stop archives or held dead letters, or holds queued, parked, overflow, callerless in-flight or pending settled-window work; an ask in flight whose caller waits does not refuse it. dropQueued: true archives all of that work to <actor dir>/stopped-queues/ first and keeps it with the removed tree.',
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        scope: { type: "string", enum: ["project", "global"] },
        dropQueued: { type: "boolean" },
      },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "setInstructions",
    description:
      'Replace an actor\'s default instruction (its persona / system-prompt body). Default scope "project" edits a live project actor; scope "global" edits a project-independent template. Takes effect on the actor\'s next queued message. Pass inline instructions or instructionsFile + sha256 (owning-host factory root, max 512 KB). A new body more than 80% shorter than the current one is refused unless replace is true.',
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        ...actorInstructionsProperties,
        scope: { type: "string", enum: ["project", "global"] },
        replace: { type: "boolean" },
      },
      required: ["id"],
      allOf: [actorInstructionsSourceSchema],
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "import",
    description:
      "Import a project-independent template from the global registry into the current project as a fresh live actor with no inherited history (no messages, session, or run logs). Identify the template by id or name; optionally rename the imported actor with \"as\" to avoid colliding with a live actor.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Template id or name (one of id/name required)" },
        name: { type: "string", description: "Template name (one of id/name required)" },
        as: { type: "string", description: "Optional new name for the imported live actor" },
        slotClaim: actorSlotClaimSchema,
        idempotencyKey: residentIdempotencyKeySchema,
      },
      additionalProperties: false,
    },
    risk: "agent",
  },
  {
    name: "export",
    description:
      "Write a live project actor's definition to the global registry as a project-independent template, without any history (no messages, session, or run logs). This is a write, not a read: it requires write: true, and throws without it. Read a live actor's instructions with agents.instructions. Remove a template with agents.remove({ id, scope: \"global\" }). Throws on a name collision unless overwrite is true.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string" },
        write: {
          type: "boolean",
          description: "Must be true: confirms the write of a global template",
        },
        overwrite: { type: "boolean" },
      },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "write",
  },
  {
    name: "log",
    description:
      "Read an actor or agent run's LLM/agent log: the actor's session transcript (session.jsonl) and/or a retained run's event stream (events.jsonl: tool calls, model responses, usage). Actors retain their last runs so logs survive after success.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Actor ID/name or agent run ID" },
        type: {
          type: "string",
          enum: ["session", "run", "all"],
          description:
            "session = actor session transcript (default for actors); run = last retained run's events; all = both",
        },
        lines: { type: "number", minimum: 1, description: "Page line limit (default 200)" },
        before: {
          type: "number",
          minimum: 0,
          description: "Exclusive byte offset returned by a previous page. Requires beforeGeneration; an unbound cursor returns cursor-stale rather than silently reading wrong bytes.",
        },
        beforeGeneration: {
          type: "string",
          description: "Required with before: previous generation (agent generation, actor sessionGeneration or run.generation). Actor type must be session or run, not all. On cursor-stale, re-read from the start without the cursor pair.",
        },
        runId: { type: "string", description: "Specific retained run (default: actor's last run)" },
      },
      required: ["id"],
      additionalProperties: false,
    },
    risk: "read",
  },
];

// send is the public steer spelling; both use the same validation, routing and authority.
AGENTS_ACTION_DESCRIPTORS.push({ ...AGENTS_ACTION_DESCRIPTORS.find(action => action.name === "steer")!, name: "send" });

// Explicit spelling for callers; keep create as the backwards-compatible API.
AGENTS_ACTION_DESCRIPTORS.push({
  ...AGENTS_ACTION_DESCRIPTORS.find(action => action.name === "create")!,
  name: "createActor",
});
