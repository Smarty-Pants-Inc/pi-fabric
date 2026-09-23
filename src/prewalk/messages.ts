import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { FabricPrewalkMode } from "../config.js";
import { MAX_PREWALK_PLAN_PROMPTS } from "./controller.js";
import type {
  FabricPrewalkPlanCheckpoint,
  PrewalkController,
} from "./controller.js";
import type { PendingFabricHandoff } from "./handoff.js";

// Startup-cheap Prewalk message shapes: custom-message types, the plan and arm
// checkpoints, request-time directive filters, and the trajectory rearm text.
// Everything here is pure strings, host interfaces and type-only edges, so
// registering the extension and running an idle session never have to compile
// the boundary engine.
// Frontier plan checkpoint: the boundary that would have handed off instead asks
// Main to record the approach first. Upstream prewalk nudges the plan on turn one,
// before any discovery; this lands on the mutation boundary, so the plan is written
// with the exploration already done and the reasoning still on the frontier model.
export const PREWALK_PLAN_MESSAGE_TYPE = "pi-fabric-prewalk-plan";

export const prewalkPlanPrompt = (model: string): string =>
  [
    `Prewalk plan checkpoint → ${model}: no plan is recorded for this task, so this boundary is withheld — the executor inherits only this transcript.`,
    "Raw reasoning replay is not guaranteed: when the executor cannot replay this model's thinking, a bounded advisory digest of its recent lines may be delivered with the continuation, but it is deliberation, not commitments, and does not replace the explicit plan.",
    "Record the plan now with prewalk.plan({ outcome, steps, verification, risks }) inside fabric_exec: the outcome, the remaining steps in execution order with the exact files, symbols, commands and checks, the risks and edge cases, and how each step is verified.",
    "Keep it concrete enough that a less capable model finishes without re-deriving the design. Then continue the task on this model — the handoff fires at the next successful mutation once the plan is recorded.",
  ].join(" ");

// Delivered as a steer so it lands before the next LLM call: the frontier model
// answers the checkpoint before it can touch another file. triggerTurn covers a
// boundary that ended Main's turn. Custom messages never fire `input`, so this can
// never be captured as the next prewalk task.
export const deliverPrewalkPlanCheckpoint = (
  pi: ExtensionAPI,
  checkpoint: FabricPrewalkPlanCheckpoint,
): boolean => {
  const { arm, mutation } = checkpoint;
  const files = Array.isArray(mutation.args?.files)
    ? (mutation.args.files as unknown[]).filter(
        (entry): entry is string => typeof entry === "string",
      )
    : [];
  try {
    pi.sendMessage(
      {
        customType: PREWALK_PLAN_MESSAGE_TYPE,
        content: prewalkPlanPrompt(arm.model),
        display: false,
        details: {
          mode: arm.mode,
          model: arm.model,
          trigger: mutation.ref,
          ...(arm.task ? { task: arm.task } : {}),
          ...(files.length > 0 ? { files } : {}),
        },
      },
      { deliverAs: "steer", triggerTurn: true },
    );
    return true;
  } catch {
    // A missed checkpoint must never fail the boundary; the caller reopens it.
    return false;
  }
};
export const PREWALK_ARMED_MESSAGE_TYPE = "pi-fabric-prewalk-armed";
export const PREWALK_CONTINUE_MESSAGE_TYPE = "pi-fabric-prewalk-continue";
const prewalkContinuationId = (message: unknown): string | undefined => {
  if (typeof message !== "object" || message === null) return undefined;
  const custom = message as { role?: unknown; customType?: unknown; details?: unknown };
  if (custom.role !== "custom" || custom.customType !== PREWALK_CONTINUE_MESSAGE_TYPE) {
    return undefined;
  }
  if (typeof custom.details !== "object" || custom.details === null) return undefined;
  const details = custom.details as { mode?: unknown; continuationId?: unknown };
  // Identity filtering applies to in-place continuations only: they carry the
  // accept/settle lifecycle. The trajectory verify prompt shares this custom
  // type but has no continuation identity and must always reach Main.
  if (details.mode !== "in-place") return undefined;
  return typeof details.continuationId === "string" ? details.continuationId : "";
};

export const filterPrewalkContinuationMessages = <Message>(
  messages: Message[],
  accept: (continuationId: string) => boolean,
  pending?: { continuationId: string; message: Message },
): { messages: Message[]; changed: boolean } => {
  let changed = false;
  let delivered = false;
  const filtered = messages.filter((message) => {
    const continuationId = prewalkContinuationId(message);
    if (continuationId === undefined) return true;
    const keep = continuationId.length > 0 && accept(continuationId);
    if (keep) delivered = true;
    if (!keep) changed = true;
    return keep;
  });
  // Under one-at-a-time steering, pending user steers drain before the queued
  // continuation, so the executor's first requests would run without the task,
  // plan or digest (LQ1). Inject the canonical payload until the queued original
  // itself reaches a request; the accept callback marks the delivery, keeping
  // settlement keyed to the first sighting. Steer order is untouched —
  // injection only adds context the queue would deliver later anyway.
  if (!delivered && pending && accept(pending.continuationId)) {
    filtered.push(pending.message);
    changed = true;
  }
  return { messages: changed ? filtered : messages, changed };
};

// Planning directives (arm advisory, plan checkpoint) are phase-scoped
// guidance for the frontier model while its arm cycle is live. Once a handoff
// claims the arm — or the arm is off — carrying them into requests invites the
// executor to treat planning as still open. Persisted history is untouched:
// only the request projection drops them, mirroring the stale-continuation
// filter above and upstream prewalk's transient plan nudge.
const isPrewalkPlanningDirective = <Message>(
  message: Message,
): boolean => {
  const custom = message as { role?: unknown; customType?: unknown };
  return (
    custom !== null &&
    typeof custom === "object" &&
    custom.role === "custom" &&
    (custom.customType === PREWALK_ARMED_MESSAGE_TYPE ||
      custom.customType === PREWALK_PLAN_MESSAGE_TYPE)
  );
};

export const filterPrewalkPlanningDirectives = <Message>(
  messages: Message[],
  visible: boolean,
): { messages: Message[]; changed: boolean } => {
  if (visible) return { messages, changed: false };
  let changed = false;
  const filtered = messages.filter((message) => {
    if (!isPrewalkPlanningDirective(message)) return true;
    changed = true;
    return false;
  });
  return { messages: changed ? filtered : messages, changed };
};

// Advisory arm-time framing, delivered as a hidden nextTurn custom message:
// LLM-visible, TUI-hidden, and never fired as an `input` event, so it cannot
// be captured as the next prewalk task and never triggers a turn by itself.
export const prewalkArmedPrompt = (
  mode: FabricPrewalkMode, model: string, requirePlan = true,
): string => [
  `Prewalk armed → ${model} (${mode}): ${requirePlan
    ? "this session owes a recorded plan before handoff. Record it with prewalk.plan({ outcome, steps, verification, risks }) inside fabric_exec; after that, "
    : ""}the first successful pi.edit / pi.write / schema.commit — or file changes produced by shell commands — hands off automatically; ${
    mode === "trajectory"
      ? "the executor takes over the requested work there, and a hidden follow-up asks you to verify its work and summarize when it finishes."
      : `this session switches to ${model} and keeps working.`
  }`,
  ...(requirePlan ? [
    `A mutation boundary without a recorded plan is withheld and asks again (up to ${MAX_PREWALK_PLAN_PROMPTS} reminders), then hands off unplanned with a warning. Plan before your first edit; Fabric delivers the recorded plan directly to the executor.`,
  ] : []),
  "Reads never fire a handoff. Trigger reports mark the handoff moment only — the workspace is the source of truth, so verify file state with reads before continuing.",
].join("\n");

const customMessageText = (content: unknown): string | undefined => {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    const parts = content
      .filter(
        (block): block is { type: "text"; text: string } =>
          typeof block === "object" &&
          block !== null &&
          (block as { type?: unknown }).type === "text" &&
          typeof (block as { text?: unknown }).text === "string",
      )
      .map((block) => block.text);
    return parts.length > 0 ? parts.join("\n") : undefined;
  }
  return undefined;
};

// Pileup guard: only skip when an identical armed prompt already persists in
// the branch, so re-arming with a different mode/model still announces itself.
export const hasPrewalkArmedPrompt = (
  entries: ReadonlyArray<unknown>,
  content: string,
): boolean =>
  entries.some((entry) => {
    if (typeof entry !== "object" || entry === null) return false;
    const candidate = entry as { type?: unknown; customType?: unknown; content?: unknown };
    return (
      candidate.type === "custom_message" &&
      candidate.customType === PREWALK_ARMED_MESSAGE_TYPE &&
      customMessageText(candidate.content) === content
    );
  });
// Appended to the replaced boundary tool result so the framing persists with
// what Main keeps seeing, anchoring every later turn. Advisory only: the directive
// is text, not a gate — prewalk.requirePlan gates the claim itself. Shell writes DO
// count as triggers when prewalk.detectShellWrites is enabled (the fs-drift fallback
// claims them).
const TRAJECTORY_REARM_DIRECTIVE = [
  "Prewalk handoff completed — the executor's result above is final; don't redo it.",
  "Prewalk re-armed: on the next request, restate remaining steps (skip if trivial), then make changes via pi.edit / pi.write or shell file changes in fabric_exec to hand off again.",
  "A hidden follow-up turn verifies the executor's work and summarizes; keep any fixes scoped to what verification fails.",
].join("\n");

export const withTrajectoryRearmDirective = (
  text: string,
  pending: PendingFabricHandoff,
  handoff: Record<string, unknown>,
  controller: PrewalkController,
  sessionId: string,
): string =>
  pending.kind === "prewalk-trajectory" &&
  handoff.completed === true &&
  controller.isArmed(sessionId)
    ? `${text}\n\n${TRAJECTORY_REARM_DIRECTIVE}`
    : text;
