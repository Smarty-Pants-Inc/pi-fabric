import { randomUUID } from "node:crypto";
import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import type { FabricResultFormat } from "../config.js";
import type { FabricCallAudit } from "../core/action-registry.js";
import { FABRIC_NESTED_TOOL_CALL_ID_PREFIX as NESTED_TOOL_CALL_ID_PREFIX } from "../protocol.js";
import type { FabricExecutionResult } from "../execution-service.js";
import type {
  FabricInvocationActivityUpdate,
  FabricInvocationContext,
} from "../protocol.js";
import { snapshotHandoffSession } from "../agents/handoff.js";
import { queueHandoffCompletion } from "../agents/handoff-completion.js";
import { queueHandoffFailureContinuation } from "../agents/handoff-continuation.js";
import type {
  AgentSessionSeed,
  AgentToolResultMessage,
} from "../agents/types.js";
import {
  buildThinkingDigest,
  thinkingTransferPolicy,
  type ThinkingTransferInput,
} from "../agents/thinking-transfer.js";
import { PREWALK_CONTINUE_MESSAGE_TYPE } from "./messages.js";
import { setModelSafely } from "./model-switch.js";
import type {
  FabricPrewalkPlanCheckpoint,
  FabricPrewalkClaim,
  FabricPrewalkReadiness,
  PrewalkContinuationMessage,
  PrewalkController,
} from "./controller.js";
import { prewalkPlanText } from "./plan.js";
import type { PrewalkFsDrift } from "./fs-drift.js";

// The boundary engine: claiming a handoff, switching Main in place, and running
// the trajectory executor. It is reachable only from the lazily loaded Fabric
// runtime, so its executor dependencies never compile during extension
// registration. Message shapes live in ./messages.js and the return half in
// ./return.js.
const PREWALK_CONTINUE_PROMPT = [
  "Continue the existing task in this same session under the new executor model.",
  "Do not stop merely because the model changed or because the first mutation succeeded.",
  "Finish what the user actually asked for: complete the remaining implementation steps, check the matching call sites for consistency, and run the relevant verification before reporting completion. If the request was read-only — a plan, a review, an investigation — the deliverable is the answer, not a code change.",
  "Once the relevant checks pass, report completion once and stop: do not re-run unchanged passing checks, repeat finished closeout, or re-derive decisions already made — reopen work only for a failed check, contradicting evidence, or a changed request.",
  "Report completion with concrete identifiers — relay links, PR and issue numbers, commit hashes, and artifact paths verbatim so the user can follow up without digging.",
].join(" ");
// Forced continuation after a completed trajectory handoff: Main must not
// settle idle at the boundary. The executor's implementation is the source of
// truth — Main verifies it with real checks and reports, redoing nothing.
const PREWALK_TRAJECTORY_VERIFY_PROMPT = [
  "Prewalk trajectory handoff complete: the executor's implementation above is final — do not redo it.",
  "Continue now: run the relevant verification (matching test module, build, or an equivalent probe) and check the changed call sites for consistency, then summarize what the executor implemented and how the checks went.",
  "Relay concrete identifiers from the executor's report verbatim — links, PR and issue numbers, commit hashes, and file paths — so the user can follow up without expanding the tool result.",
  "If a check fails, fix only the failing part; keep the fix scoped. If this verification already happened in this turn, respond with the summary only.",
].join(" ");

// Forced reply after a trajectory handoff that settled without completing
// (failed / stopped / timed out): the terminating boundary suppresses Main's
// inference, so without a queued follow-up nobody would ever tell the user.
const PREWALK_TRAJECTORY_INCOMPLETE_PROMPT = [
  "Prewalk trajectory ended without completing: the executor's result above is final — do not redo its work.",
  "Tell the user now, briefly: how the executor ended, why, and what it still managed in the workspace — relay any links, PR and issue numbers, and commit hashes it produced verbatim.",
  "Propose the next step (retry, adjust, or continue manually) and stop; do not take over the implementation unprompted.",
].join(" ");

// Thrown-boundary variant for terminating boundaries: the handoff failed
// before its continuation even started (unavailable model, missing auth,
// queue failure). In-place failures keep the run alive and need no queued
// copy; trajectory boundaries still end the turn silently.
const PREWALK_FAILURE_PROMPT = [
  "A prewalk handoff at this boundary failed — the boundary result above is final; do not retry the handoff autonomously.",
  "Tell the user now, briefly: that the handoff failed and why (from the result above), relaying any identifiers verbatim, and propose the next step.",
  "The task stays re-armed where applicable; wait for the user's direction instead of redoing anything yourself.",
].join(" ");
const PREWALK_FAILURE_MESSAGE_TYPE = "pi-fabric-prewalk-failure";
// Hidden boundary follow-ups queue best-effort after the handoff settles: the
// persisted boundary result stays authoritative, so a missed turn must never
// fail or mask the handoff outcome itself.
const queuePrewalkFollowUp = (
  extension: ExtensionAPI,
  customType: string,
  content: string,
  details: Record<string, unknown>,
): void => {
  try {
    extension.sendMessage(
      { customType, content, display: false, details },
      { deliverAs: "followUp", triggerTurn: true },
    );
  } catch {
    // Swallow: a missed follow-up turn must not fail the handoff.
  }
};
const prewalkTriggerField = (
  pending: PendingFabricHandoff,
): Record<string, unknown> => ({
  ref: pending.triggerRef,
  ...(pending.triggerSeq !== undefined ? { seq: pending.triggerSeq } : {}),
  ...(pending.triggerFiles && pending.triggerFiles.length > 0
    ? {
        files: pending.triggerFiles,
        ...(pending.triggerFilesTruncated
          ? { truncated: pending.triggerFilesTruncated }
          : {}),
      }
    : {}),
});
export interface BoundaryHandoffRunner {
  executeHandoff(
    args: Record<string, unknown>,
    context: FabricInvocationContext,
    sessionSeed: AgentSessionSeed,
  ): Promise<Record<string, unknown>>;
}

export interface PendingFabricHandoff {
  kind: "explicit" | "prewalk-in-place" | "prewalk-trajectory";
  args: Record<string, unknown>;
  audit: FabricCallAudit;
  resultFormat: FabricResultFormat;
  triggerRef?: string;
  // Session-monotonic claim order from the controller; rides result trigger
  // fields so follow-ups and audit surfaces can reference the Nth claim.
  triggerSeq?: number;
  // Filesystem-drift trigger evidence, bounded by the drift tracker's report
  // cap; absent for audited mutation triggers.
  triggerFiles?: string[];
  triggerFilesTruncated?: number;
  // Absent for explicit handoffs, which do not participate in the plan gate.
  readiness?: FabricPrewalkReadiness;
}
export const claimFabricHandoff = (
  controller: PrewalkController,
  execution: FabricExecutionResult,
  sessionId: string,
  resultFormat: FabricResultFormat,
): PendingFabricHandoff | FabricPrewalkPlanCheckpoint | undefined => {
  if (execution.handoffRequest) {
    controller.completeTask();
    let audit: FabricCallAudit | undefined;
    for (let index = execution.audits.length - 1; index >= 0; index--) {
      const candidate = execution.audits[index];
      if (candidate?.ref === "agents.handoff") {
        audit = candidate;
        break;
      }
    }
    if (!audit) {
      throw new Error("Deferred agents.handoff request has no matching Fabric audit");
    }
    return {
      kind: "explicit",
      args: execution.handoffRequest,
      audit,
      resultFormat,
    };
  }

  const outcome = controller.claim(execution.audits, sessionId);
  if (!outcome) return undefined;
  if (outcome.kind === "prewalk-plan") return outcome;
  const pending = buildPrewalkPending(outcome, resultFormat);
  execution.audits.push(pending.audit);
  return pending;
};

// Filesystem-fallback claim path (PREWALK_FS_DRIFT_REF): reached when an armed
// session ran a successful Pi shell call inside the program but no audited pi.edit /
// pi.write / schema.commit fired — heredocs, sed -i, formatter binaries. The
// rest of the boundary pipeline (in-place switch or trajectory fork) is
// identical; only the trigger evidence differs.
export const claimFabricFsDriftHandoff = (
  controller: PrewalkController,
  execution: FabricExecutionResult,
  sessionId: string,
  drift: PrewalkFsDrift,
  resultFormat: FabricResultFormat,
): PendingFabricHandoff | FabricPrewalkPlanCheckpoint | undefined => {
  const outcome = controller.claimFsDrift(sessionId, drift.files);
  if (!outcome) return undefined;
  if (outcome.kind === "prewalk-plan") return outcome;
  const pending = buildPrewalkPending(outcome, resultFormat);
  if (drift.files.length > 0) {
    pending.triggerFiles = drift.files;
    if (drift.truncated > 0) pending.triggerFilesTruncated = drift.truncated;
  }
  execution.audits.push(pending.audit);
  return pending;
};

const buildPrewalkPending = (
  claim: FabricPrewalkClaim,
  resultFormat: FabricResultFormat,
): PendingFabricHandoff => {
  const inPlace = claim.arm.mode === "in-place";
  const readiness = claim.readiness;
  const task = [
    claim.arm.task,
    ...(!inPlace && readiness.kind === "planned" ? [prewalkPlanText(readiness.plan)] : []),
  ].filter(Boolean).join("\n\n");
  const nestedToolCallId = `${NESTED_TOOL_CALL_ID_PREFIX}prewalk_${randomUUID()}`;
  const args = {
    model: claim.arm.model,
    name: inPlace ? "In-place Prewalk" : "Prewalk trajectory executor",
    ...(task ? { task } : {}),
    // Thinking applies to the child executor only; in-place keeps Main's level.
    ...(!inPlace && claim.arm.thinking ? { thinking: claim.arm.thinking } : {}),
  };
  const audit: FabricCallAudit = {
    ref: inPlace ? "fabric.prewalk" : "agents.handoff",
    nestedToolCallId,
    startedAt: Date.now(),
    tool: inPlace ? "prewalk" : "handoff",
    provider: inPlace ? "fabric" : "agents",
    args: {
      ...args, seq: claim.seq, readiness: readiness.kind,
      ...(readiness.kind === "unplanned" ? { planPrompts: readiness.prompts } : {}),
    },
  };
  return {
    kind: inPlace ? "prewalk-in-place" : "prewalk-trajectory",
    args,
    audit,
    resultFormat,
    triggerRef: claim.mutation.ref,
    triggerSeq: claim.seq,
    readiness,
  };
};
const modelForKey = (key: string, context: ExtensionContext) => {
  const separator = key.indexOf("/");
  if (separator <= 0 || separator === key.length - 1) {
    throw new Error("Prewalk requires a provider/model executor target");
  }
  const model = context.modelRegistry.find(
    key.slice(0, separator),
    key.slice(separator + 1),
  );
  if (!model) throw new Error(`Prewalk model is unavailable: ${key}`);
  return model;
};
const runInPlacePrewalk = async (
  controller: PrewalkController,
  extension: ExtensionAPI,
  pending: PendingFabricHandoff,
  context: ExtensionContext,
): Promise<Record<string, unknown>> => {
  const modelKey = String(pending.args.model ?? "");
  context.ui.setStatus("fabric-prewalk", `switching Main → ${modelKey}`);
  const model = modelForKey(modelKey, context);
  // Snapshot the pre-switch reasoning channel and branch. In-place handoff
  // cannot rewrite Pi's ground-truth log, so foreign thinking stays
  // unreplayable for the new model; bridge continuity with the bounded digest.
  const sourceModel = context.model
    ? {
        provider: context.model.provider,
        modelId: context.model.id,
        api: context.modelRegistry.find(context.model.provider, context.model.id)?.api,
      }
    : undefined;
  const transfer: ThinkingTransferInput = {
    ...(sourceModel ? { source: sourceModel } : {}),
    target: {
      provider: model.provider,
      modelId: model.id,
      api: model.api,
      reasoning: model.reasoning,
      ...((model.compat as { requiresThinkingAsText?: boolean } | undefined)
        ?.requiresThinkingAsText !== undefined
        ? {
            requiresThinkingAsText: (model.compat as { requiresThinkingAsText?: boolean })
              .requiresThinkingAsText,
          }
        : {}),
    },
  };
  const branch = context.sessionManager.getBranch();
  const returnModel = context.model;
  if (!returnModel) throw new Error("Prewalk cannot determine Main return model");
  const returnModelKey = `${returnModel.provider}/${returnModel.id}`;
  const continuationId = randomUUID();
  const switched = await extension.setModel(model);
  if (!switched) {
    throw new Error(`No authentication configured for prewalk model: ${modelKey}`);
  }
  // Record the borrow as soon as the switch succeeds: the continuation below
  // can still fail to queue, and a failed rollback must leave recovery data.
  controller.borrowMain(returnModelKey);

  let continuationMessage: PrewalkContinuationMessage | undefined;
  try {
    // One hidden continuation delivers everything the executor needs: the
    // task, the recorded plan, and — when the reasoning channel is not
    // replayable — a bounded advisory digest of the frontier model's
    // deliberation. It is sent as a passive context message (triggerTurn
    // false): the host defers it to the end of the boundary turn and appends
    // it after the tool results, so the executor's next request carries it
    // without a queued turn of its own. The controller copy is the canonical
    // payload for the context hook, which injects it into any earlier request
    // that would otherwise run without it (LQ1: competing steers no longer
    // delay it, and no completion-only request follows).
    const transferPolicy = thinkingTransferPolicy(transfer);
    const digest = transferPolicy !== "preserved"
      ? buildThinkingDigest(branch, transfer)
      : undefined;
    const taskText =
      typeof pending.args.task === "string" && pending.args.task.trim().length > 0
        ? pending.args.task
        : undefined;
    continuationMessage = {
      role: "custom",
      customType: PREWALK_CONTINUE_MESSAGE_TYPE,
      content: [
        PREWALK_CONTINUE_PROMPT,
        ...(taskText ? [taskText] : []),
        ...(pending.readiness?.kind === "planned" ? [prewalkPlanText(pending.readiness.plan)] : []),
        ...(digest ? [digest.content] : []),
      ].join("\n\n"),
      display: false,
      details: {
        mode: "in-place",
        model: modelKey,
        continuationId,
        returnModel: returnModelKey,
        trigger: pending.triggerRef,
        ...(digest
          ? {
              thinkingTransfer: {
                policy: transferPolicy,
                citedBlocks: digest.citedBlocks,
                target: modelKey,
              },
            }
          : {}),
      },
      timestamp: Date.now(),
    };
    extension.sendMessage(continuationMessage, { triggerTurn: false });
  } catch (error) {
    const restored = await setModelSafely(extension, returnModel);
    if (!restored) {
      // Main is stuck on the executor: disarm so the next mutation cannot hand
      // off again, but keep the borrow so a later session start or /fabric
      // reload retries the return instead of losing Main.
      controller.cancel();
      throw new Error(
        `Prewalk could not queue its continuation or return Main to ${returnModelKey}`,
        { cause: error },
      );
    }
    // Main is back: discharge the borrow and let the armed task survive.
    controller.clearBorrowed();
    throw error;
  }

  controller.beginContinuation(continuationId, returnModelKey, continuationMessage);
  context.ui.notify(
    `Prewalk is continuing in Main with ${modelKey}, then returning to ${returnModelKey}.`,
    "info",
  );
  context.ui.setStatus("fabric-prewalk", `continuing Main → ${modelKey}`);
  return {
    prewalk: true,
    mode: "in-place",
    continued: true,
    status: "continued",
    model: modelKey,
    trigger: prewalkTriggerField(pending),
  };
};
export const runFabricHandoffAtBoundary = async (
  controller: PrewalkController,
  runner: BoundaryHandoffRunner,
  extension: ExtensionAPI,
  pending: PendingFabricHandoff,
  outerToolResult: AgentToolResultMessage,
  context: ExtensionContext,
  activity?: (update: FabricInvocationActivityUpdate) => void,
): Promise<Record<string, unknown>> => {
  const model = String(pending.args.model ?? "");
  const inPlace = pending.kind === "prewalk-in-place";
  context.ui.setStatus(
    "fabric-prewalk",
    inPlace ? `switching Main → ${model}` : `handing off trajectory → ${model}`,
  );
  try {
    if (inPlace) {
      const result = await runInPlacePrewalk(controller, extension, pending, context);
      pending.audit.success = true;
      pending.audit.result = result;
      pending.audit.endedAt = Date.now();
      activity?.({ type: "progress", message: `Main continuing in place with ${model}` });
      return result;
    }

    const seed = snapshotHandoffSession(
      context.sessionManager,
      context.model,
      outerToolResult,
      outerToolResult.toolCallId,
    );
    const invocation: FabricInvocationContext = {
      cwd: context.cwd,
      signal: context.signal,
      parentToolCallId: outerToolResult.toolCallId,
      nestedToolCallId: pending.audit.nestedToolCallId,
      extensionContext: context,
      update(message) {
        context.ui.setStatus("fabric-prewalk", message);
        activity?.({ type: "progress", message });
      },
      ...(activity ? { activity } : {}),
      attachPreview(preview) {
        pending.audit.preview = preview;
      },
    };
    const result = await runner.executeHandoff(pending.args, invocation, seed);
    const completed = result.completed === true;
    pending.audit.success = completed;
    pending.audit.result = result;
    pending.audit.endedAt = Date.now();
    const continuing = queueHandoffFailureContinuation(extension, context, result);
    // An explicitly armed executor must not immediately hand off its next write.
    if (continuing) controller.cancel();
    if (!continuing && pending.kind === "prewalk-trajectory") {
      // Main is never left idle after a delegated implementation: queue a
      // hidden follow-up the same way in-place does. Completed handoffs get
      // verify-and-summarize; non-completed ones get report-and-propose so a
      // failed, stopped, or timed-out executor never ends the turn in silence.
      if (completed) {
        queuePrewalkFollowUp(
          extension,
          PREWALK_CONTINUE_MESSAGE_TYPE,
          PREWALK_TRAJECTORY_VERIFY_PROMPT,
          { mode: "trajectory", model, trigger: pending.triggerRef },
        );
      } else {
        queuePrewalkFollowUp(
          extension,
          PREWALK_FAILURE_MESSAGE_TYPE,
          PREWALK_TRAJECTORY_INCOMPLETE_PROMPT,
          {
            mode: "trajectory",
            model,
            status: result.status,
            ...(typeof result.error === "string" ? { error: result.error } : {}),
            trigger: pending.triggerRef,
          },
        );
      }
    }
    if (!continuing && pending.kind === "explicit") {
      queueHandoffCompletion(extension, pending.args, result);
    }
    context.ui.setStatus(
      "fabric-prewalk",
      continuing ? "handoff failed; executor continuing directly"
        : completed ? "trajectory executor implemented" : `trajectory ${String(result.status ?? "failed")}`,
    );
    return {
      ...(pending.kind === "prewalk-trajectory"
        ? { prewalk: true, mode: "trajectory", trigger: prewalkTriggerField(pending) }
        : {}),
      ...result,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (inPlace) controller.failHandoff();
    pending.audit.success = false;
    pending.audit.error = message;
    pending.audit.endedAt = Date.now();
    const failure = { handedOff: false, continued: false, completed: false, status: "failed", error: message };
    const continuing = !inPlace && queueHandoffFailureContinuation(extension, context, { ...failure, error });
    if (continuing) controller.cancel();
    if (!continuing && pending.kind.startsWith("prewalk-") && !inPlace) {
      // In-place failures do not terminate the boundary: Main keeps running in
      // the same turn with the failed result in context, so a queued report
      // would only add a duplicate turn. Trajectory failures still end the
      // turn silently, so they queue the report-and-propose reply.
      queuePrewalkFollowUp(
        extension,
        PREWALK_FAILURE_MESSAGE_TYPE,
        PREWALK_FAILURE_PROMPT,
        { mode: inPlace ? "in-place" : "trajectory", trigger: pending.triggerRef, error: message },
      );
    }
    if (!continuing && pending.kind === "explicit") {
      queueHandoffCompletion(extension, pending.args, failure);
    }
    context.ui.setStatus("fabric-prewalk", continuing ? "handoff failed; executor continuing directly"
      : inPlace ? "in-place continuation failed" : "trajectory handoff failed");
    return {
      ...(pending.kind.startsWith("prewalk-")
        ? {
            prewalk: true,
            mode: inPlace ? "in-place" : "trajectory",
            trigger: prewalkTriggerField(pending),
          }
        : {}),
      ...failure,
    };
  } finally {
    if (!inPlace) {
      const status = controller.completeTask();
      if (status.state === "armed") {
        context.ui.setStatus("fabric-prewalk", `armed → ${status.model}`);
      }
    }
  }
};
