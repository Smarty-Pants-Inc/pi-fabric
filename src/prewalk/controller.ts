import type { FabricPrewalkMode } from "../config.js";
import type { FabricCallAudit } from "../core/action-registry.js";
import { isFabricThinking, type FabricThinking } from "../thinking.js";
import type { PrewalkPlan } from "./plan.js";

// Plan nudges per arm cycle before the claim fails open. A model that ignores the
// checkpoint twice still gets its handoff rather than a stalled session.
export const MAX_PREWALK_PLAN_PROMPTS = 2;

const PREWALK_TRIGGER_REFS = new Set([
  "pi.edit",
  "pi.write",
  "schema.commit",
]);

// Synthesized audit ref for filesystem-drift claims: writes made through
// Pi shell calls (or any call whose file effects audits cannot see) detected by the
// stat-manifest fallback while armed. Never a real call; trigger ref only.
const PREWALK_FS_DRIFT_REF = "fs.drift";

export interface FabricPrewalkArm {
  mode: FabricPrewalkMode;
  model: string;
  sessionId: string;
  armedAt: number;
  alwaysRearm: boolean;
  task?: string;
  thinking?: FabricThinking;
}

interface FabricPrewalkContinuation extends FabricPrewalkArm {
  continuationId: string;
  returnModel: string;
  accepted: boolean;
}

// The readiness kind a claim snapshotted, kept on the handing-off and
// continuation statuses so read-only surfaces can distinguish a consumed
// recorded plan from an arm that never recorded one.
export type FabricPrewalkClaimedReadiness = "planned" | "disabled" | "unplanned";

export type FabricPrewalkStatus =
  | { state: "idle" }
  | ({ state: "armed" } & FabricPrewalkArm)
  | ({ state: "handing_off" } & FabricPrewalkArm & { claimedReadiness: FabricPrewalkClaimedReadiness })
  | ({ state: "continuation_pending" } & FabricPrewalkContinuation & { claimedReadiness: FabricPrewalkClaimedReadiness });

// Captured at claim time; delivery must not infer readiness from the later state.
export type FabricPrewalkReadiness =
  | { kind: "planned"; plan: PrewalkPlan }
  | { kind: "disabled" }
  | { kind: "unplanned"; prompts: number };

export interface FabricPrewalkClaim {
  kind: "prewalk-claim";
  arm: FabricPrewalkArm;
  mutation: FabricCallAudit;
  // Session-monotonic claim order, stamped when a mutation actually claims
  // the arm (dsh's commit-order habit, adapted). Never resets on cancel or
  // re-arm, so a later claim can never recycle an earlier number.
  seq: number;
  readiness: FabricPrewalkReadiness;
}

// A mutation boundary reached while the arm still owes a plan: nothing hands off,
// the arm stays armed, and Main is asked to record the approach with prewalk.plan.
// Readiness is that recorded artifact, and the plan rides the same transcript into
// the later handoff.
export interface FabricPrewalkPlanCheckpoint {
  kind: "prewalk-plan";
  arm: FabricPrewalkArm;
  mutation: FabricCallAudit;
}

export type FabricPrewalkClaimOutcome =
  | FabricPrewalkClaim
  | FabricPrewalkPlanCheckpoint;

export interface FabricPrewalkSettlement {
  continuationId: string;
  returnModel: string;
  executorModel: string;
}

// Survives cancel(), session_start, and settle so a new session that inherited
// the executor — or a mid-continuation cancel — can still return Main.
export interface FabricPrewalkBorrowedMain {
  returnModel: string;
  executorModel: string;
}

// The canonical in-place continuation message, stored at handoff so the
// request-context hook can front-load the payload: under one-at-a-time
// steering, pending user steers drain before the queued follow-up, and the
// executor's first requests would otherwise run without the task, plan or
// digest (LQ1). Injection is context-only; the queued original remains the
// wake-up and the ground-truth transcript copy.
export interface PrewalkContinuationMessage {
  role: "custom";
  customType: string;
  content: string;
  display: boolean;
  details: Record<string, unknown>;
  timestamp: number;
}

const normalizedTask = (value: string | undefined): string | undefined => {
  const task = value?.trim();
  return task ? task.slice(0, 20_000) : undefined;
};

export class PrewalkController {
  #status: FabricPrewalkStatus = { state: "idle" };
  #settling = new Set<string>();
  #claimSeq = new Map<string, number>();
  #borrowed: FabricPrewalkBorrowedMain | undefined;
  #requirePlan = false;
  #plan: PrewalkPlan | undefined;
  #planPrompts = 0;
  #pendingMessage: PrewalkContinuationMessage | undefined;

  status(): FabricPrewalkStatus {
    return structuredClone(this.#status);
  }

  arm(input: {
    model: string;
    mode?: FabricPrewalkMode;
    sessionId: string;
    task?: string;
    alwaysRearm?: boolean;
    requirePlan?: boolean;
    thinking?: FabricThinking;
  }): FabricPrewalkStatus {
    const model = input.model.trim();
    if (!model.includes("/")) throw new Error("Prewalk requires a provider/model executor target");
    if (input.thinking !== undefined && !isFabricThinking(input.thinking)) {
      throw new Error(`Invalid prewalk thinking level: ${String(input.thinking)}`);
    }
    const task = normalizedTask(input.task);
    this.#settling.clear();
    // Each arm cycle owes its own plan checkpoint: a re-arm is a new task.
    this.#requirePlan = input.requirePlan === true;
    this.#plan = undefined;
    this.#planPrompts = 0;
    this.#pendingMessage = undefined;
    this.#status = {
      state: "armed",
      mode: input.mode ?? "in-place",
      model,
      sessionId: input.sessionId,
      armedAt: Date.now(),
      alwaysRearm: input.alwaysRearm === true,
      ...(task ? { task } : {}),
      ...(input.thinking ? { thinking: input.thinking } : {}),
    };
    return this.status();
  }

  observeTask(sessionId: string, task: string): FabricPrewalkStatus {
    if (
      this.#status.state !== "armed" ||
      this.#status.sessionId !== sessionId ||
      this.#status.task
    ) {
      return this.status();
    }
    const normalized = normalizedTask(task);
    if (normalized) {
      // A recaptured task is a new task: the plan recorded for the previous one
      // does not cover it, so this arm owes a fresh checkpoint.
      this.#plan = undefined;
      this.#planPrompts = 0;
      this.#status = { ...this.#status, task: normalized };
    }
    return this.status();
  }

  isArmed(sessionId?: string): boolean {
    return (
      this.#status.state === "armed" &&
      (sessionId === undefined || this.#status.sessionId === sessionId)
    );
  }

  // Readiness is an artifact, not a delivered nudge: the arm stops owing a plan
  // only once the frontier model records one. A recorded plan survives a failed
  // handoff, so a retry never asks for the same plan twice; arm(), completeTask()
  // and a recaptured task reset it.
  planCheckpointRequired(sessionId?: string): boolean {
    return this.#planOwed(sessionId) && this.#planPrompts < MAX_PREWALK_PLAN_PROMPTS;
  }

  // True while the armed session owes a plan, even after the reminder budget
  // is spent. A claimed handoff carries its own readiness snapshot.
  planRequired(sessionId?: string): boolean {
    return this.#planOwed(sessionId);
  }

  planReady(sessionId?: string): boolean {
    return this.isArmed(sessionId) && this.#plan !== undefined;
  }

  // Bounded readiness snapshot for status surfaces and the prewalk.status action.
  planState(sessionId?: string): { required: boolean; ready: boolean; prompts: number } {
    return {
      required: this.#planOwed(sessionId),
      ready: this.planReady(sessionId),
      prompts: this.#planPrompts,
    };
  }

  // Own the bounded plan independently of the caller's mutable input and tool
  // result. The claim snapshots it for explicit executor delivery.
  submitPlan(sessionId: string, plan: PrewalkPlan): boolean {
    if (!this.#planOwed(sessionId)) return false;
    this.#plan = structuredClone(plan);
    return true;
  }

  #planOwed(sessionId?: string): boolean {
    return this.isArmed(sessionId) && this.#requirePlan && this.#plan === undefined;
  }

  // Host delivery can fail; give the nudge back so the next mutation retries it
  // instead of spending budget on a message nobody saw.
  reopenPlanCheckpoint(): void {
    this.#planPrompts = Math.max(0, this.#planPrompts - 1);
  }

  // Ownership of Main's return transfers the moment the executor model is
  // actually selected — before the continuation is built or delivered. A
  // delivery that never queues must still be able to put Main back, and a
  // rollback that also fails must leave recovery data for session start or
  // /fabric reload instead of stranding the session on the executor.
  borrowMain(returnModel: string): boolean {
    if (this.#status.state !== "handing_off" || this.#status.mode !== "in-place") {
      return false;
    }
    this.#borrowed = {
      returnModel,
      executorModel: this.#status.model,
    };
    return true;
  }

  beginContinuation(
    continuationId: string,
    returnModel: string,
    message?: PrewalkContinuationMessage,
  ): FabricPrewalkStatus {
    if (this.#status.state !== "handing_off" || this.#status.mode !== "in-place") {
      return this.status();
    }
    this.borrowMain(returnModel);
    this.#pendingMessage = message ? structuredClone(message) : undefined;
    this.#status = {
      ...this.#status,
      state: "continuation_pending",
      continuationId,
      returnModel,
      accepted: false,
    };
    return this.status();
  }

  borrowedReturn(): FabricPrewalkBorrowedMain | undefined {
    return this.#borrowed ? { ...this.#borrowed } : undefined;
  }

  clearBorrowed(): void {
    this.#borrowed = undefined;
  }

  // A restarted process has no in-memory borrow record, but the transcript can
  // still prove Main is owed a return. Adopt only the recovery tuple: the
  // continuation's delivery and settlement lifecycle belonged to the process
  // that handed off, so it must never replay from history. Refuses while
  // another borrow or a live handoff/continuation owns the record.
  hydrateBorrowedMain(input: FabricPrewalkBorrowedMain): boolean {
    if (this.#borrowed) return false;
    if (this.#status.state === "handing_off" || this.#status.state === "continuation_pending") {
      return false;
    }
    if (!input.returnModel.trim() || !input.executorModel.trim()) return false;
    if (input.returnModel === input.executorModel) return false;
    this.#borrowed = {
      returnModel: input.returnModel,
      executorModel: input.executorModel,
    };
    return true;
  }

  acceptContinuation(sessionId: string, continuationId: string): boolean {
    if (
      this.#status.state !== "continuation_pending" ||
      this.#status.sessionId !== sessionId ||
      this.#status.continuationId !== continuationId
    ) {
      return false;
    }
    this.#status = { ...this.#status, accepted: true };
    return true;
  }

  // Canonical payload for this session's in-flight continuation, for the
  // request-context hook to inject while competing steers still drain ahead
  // of the queued original. Read-only clone; absent once the continuation
  // settles, re-arms or cancels.
  pendingContinuationMessage(
    sessionId: string,
  ): { continuationId: string; message: PrewalkContinuationMessage } | undefined {
    if (this.#status.state !== "continuation_pending" || this.#status.sessionId !== sessionId) {
      return undefined;
    }
    if (!this.#pendingMessage) return undefined;
    return {
      continuationId: this.#status.continuationId,
      message: structuredClone(this.#pendingMessage),
    };
  }

  takeContinuationSettlement(sessionId: string): FabricPrewalkSettlement | undefined {
    if (
      this.#status.state !== "continuation_pending" ||
      this.#status.sessionId !== sessionId ||
      !this.#status.accepted ||
      this.#settling.has(this.#status.continuationId)
    ) {
      return undefined;
    }
    this.#settling.add(this.#status.continuationId);
    return {
      continuationId: this.#status.continuationId,
      returnModel: this.#status.returnModel,
      executorModel: this.#status.model,
    };
  }

  finishContinuation(sessionId: string, continuationId: string): boolean {
    if (
      this.#status.state !== "continuation_pending" ||
      this.#status.sessionId !== sessionId ||
      this.#status.continuationId !== continuationId ||
      !this.#settling.delete(continuationId)
    ) {
      return false;
    }
    this.completeTask();
    return true;
  }

  failHandoff(): FabricPrewalkStatus {
    if (this.#status.state !== "handing_off") return this.status();
    const failed = this.#status;
    // Rebuild from the arm fields: the claimed readiness belongs to the failed
    // handoff and must not leak into the re-armed status.
    this.#status = {
      state: "armed",
      mode: failed.mode,
      model: failed.model,
      sessionId: failed.sessionId,
      armedAt: failed.armedAt,
      alwaysRearm: failed.alwaysRearm,
      ...(failed.task ? { task: failed.task } : {}),
      ...(failed.thinking ? { thinking: failed.thinking } : {}),
    };
    return this.status();
  }

  // A settle without a handoff is not consumption: the arm survives until a
  // matching mutation actually claims it (or the user runs `/fabric prewalk
  // --off`). Only handoff completion goes through completeTask / alwaysRearm.
  // The captured task text belongs to the settled turn, so drop it and let the
  // next input recapture — otherwise tomorrow's unrelated prompt would ride on
  // yesterday's task.
  settleTask(sessionId: string): boolean {
    if (
      this.#status.state !== "armed" ||
      this.#status.sessionId !== sessionId
    ) {
      return false;
    }
    const armed = this.#status;
    if (armed.task !== undefined) {
      this.#status = {
        state: "armed",
        mode: armed.mode,
        model: armed.model,
        sessionId: armed.sessionId,
        armedAt: armed.armedAt,
        alwaysRearm: armed.alwaysRearm,
        ...(armed.thinking ? { thinking: armed.thinking } : {}),
      };
    }
    return true;
  }

  completeTask(): FabricPrewalkStatus {
    if (this.#status.state === "idle") return this.status();
    // The next arm cycle plans again: a fresh task deserves its own checkpoint.
    this.#plan = undefined;
    this.#planPrompts = 0;
    this.#pendingMessage = undefined;
    if (!this.#status.alwaysRearm) {
      this.cancel();
      return this.status();
    }
    this.#status = {
      state: "armed",
      mode: this.#status.mode,
      model: this.#status.model,
      sessionId: this.#status.sessionId,
      armedAt: Date.now(),
      alwaysRearm: true,
      ...(this.#status.thinking ? { thinking: this.#status.thinking } : {}),
    };
    return this.status();
  }

  claim(audits: FabricCallAudit[], sessionId: string): FabricPrewalkClaimOutcome | undefined {
    if (!this.isArmed(sessionId) || this.#status.state !== "armed") return undefined;
    if (audits.some((audit) => audit.ref === "agents.handoff" && audit.success === true)) {
      this.completeTask();
      return undefined;
    }
    const mutation = audits.find(
      (audit) => PREWALK_TRIGGER_REFS.has(audit.ref) && audit.success === true,
    );
    if (!mutation) return undefined;
    const arm = this.#snapshotArm();
    if (!arm) return undefined;
    return this.#claim(arm, mutation);
  }

  // Filesystem-fallback claim for writes audits cannot attribute (shell
  // heredocs, sed -i, formatter binaries). The drift file list rides on the
  // synthesized mutation audit for dashboard/debug visibility and is already
  // caller-bounded.
  claimFsDrift(sessionId: string, files: readonly string[]): FabricPrewalkClaimOutcome | undefined {
    if (!this.isArmed(sessionId)) return undefined;
    const arm = this.#snapshotArm();
    if (!arm) return undefined;
    const mutation: FabricCallAudit = {
      ref: PREWALK_FS_DRIFT_REF,
      nestedToolCallId: "fs-drift",
      startedAt: Date.now(),
      success: true,
      ...(files.length > 0 ? { args: { files: [...files] } } : {}),
    };
    return this.#claim(arm, mutation);
  }

  #claim(arm: FabricPrewalkArm, mutation: FabricCallAudit): FabricPrewalkClaimOutcome {
    const checkpoint = this.#takePlanCheckpoint(arm, mutation);
    if (checkpoint) return checkpoint;
    const readiness: FabricPrewalkReadiness = this.#plan
      ? { kind: "planned", plan: structuredClone(this.#plan) }
      : this.#requirePlan
        ? { kind: "unplanned", prompts: this.#planPrompts }
        : { kind: "disabled" };
    const seq = this.#nextClaimSeq(arm.sessionId);
    this.#status = { state: "handing_off", ...arm, claimedReadiness: readiness.kind };
    return { kind: "prewalk-claim", arm, mutation, seq, readiness };
  }

  // The boundary that would hand off asks again instead — bounded, so a model that
  // ignores the checkpoint cannot deadlock an armed session, but generous enough
  // that the frontier keeps working on the plan it was asked for.
  #takePlanCheckpoint(
    arm: FabricPrewalkArm,
    mutation: FabricCallAudit,
  ): FabricPrewalkPlanCheckpoint | undefined {
    if (!this.#planOwed(arm.sessionId)) return undefined;
    if (this.#planPrompts >= MAX_PREWALK_PLAN_PROMPTS) return undefined;
    this.#planPrompts += 1;
    return { kind: "prewalk-plan", arm, mutation };
  }

  #nextClaimSeq(sessionId: string): number {
    const seq = (this.#claimSeq.get(sessionId) ?? 0) + 1;
    this.#claimSeq.set(sessionId, seq);
    return seq;
  }

  #snapshotArm(): FabricPrewalkArm | undefined {
    if (this.#status.state !== "armed") return undefined;
    const armed = this.#status;
    return {
      mode: armed.mode,
      model: armed.model,
      sessionId: armed.sessionId,
      armedAt: armed.armedAt,
      alwaysRearm: armed.alwaysRearm,
      ...(armed.task ? { task: armed.task } : {}),
      ...(armed.thinking ? { thinking: armed.thinking } : {}),
    };
  }

  cancel(): void {
    this.#settling.clear();
    this.#requirePlan = false;
    this.#plan = undefined;
    this.#planPrompts = 0;
    this.#pendingMessage = undefined;
    this.#status = { state: "idle" };
  }
}
