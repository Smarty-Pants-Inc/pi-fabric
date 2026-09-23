import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricState } from "../fabric-state.js";
import { resolveFabricIdentity } from "../main-agent.js";
import {
  PREWALK_ARMED_MESSAGE_TYPE,
  hasPrewalkArmedPrompt,
  prewalkArmedPrompt,
} from "./messages.js";

// The single arm path shared by `/fabric prewalk` and alwaysRearm session
// auto-arm, so drift baseline, hidden armed advisory, and status chip never
// drift apart between entry points.
export const armFabricPrewalkSession = async (
  state: FabricState,
  context: ExtensionContext,
  pi: ExtensionAPI,
  input: { model: string; task?: string },
): Promise<void> => {
  const { prewalk } = state.config;
  const sessionId = context.sessionManager.getSessionId();
  state.prewalk.arm({
    model: input.model,
    mode: prewalk.mode,
    sessionId,
    ...(input.task ? { task: input.task } : {}),
    ...(prewalk.thinking ? { thinking: prewalk.thinking } : {}),
    alwaysRearm: prewalk.alwaysRearm,
    requirePlan: prewalk.requirePlan,
  });
  // Anchor the shell-write drift window at arm time so the first
  // bash-running boundary diffs against the pre-task tree state; only
  // snapshot when the fs fallback can actually claim.
  if (prewalk.detectShellWrites) {
    await state.prewalkDrift.captureBaseline(sessionId, context.cwd);
  }
  // Hidden advisory framing, queued for the next prompt (rules before the
  // task when the caller submits one). nextTurn never triggers a turn;
  // custom messages never fire `input`, so observeTask ignores it.
  const armedPrompt = prewalkArmedPrompt(prewalk.mode, input.model, prewalk.requirePlan);
  if (!hasPrewalkArmedPrompt(context.sessionManager.getBranch(), armedPrompt)) {
    pi.sendMessage(
      {
        customType: PREWALK_ARMED_MESSAGE_TYPE,
        content: armedPrompt,
        display: false,
        details: { mode: prewalk.mode, model: input.model },
      },
      { deliverAs: "nextTurn" },
    );
  }
  context.ui.setStatus("fabric-prewalk", `armed (${prewalk.mode}) → ${input.model}`);
};

// alwaysRearm covers unarmed starts too: every Main session (and `/fabric reload`)
// opens armed, not only sessions following a completed handoff. Prerequisites
// mirror the `/fabric prewalk` command gates, minus interactive model
// selection — auto-arm is non-interactive and reads `prewalk.model`.
//
// Returns a human-readable reason when auto-arm is configured but skipped for
// health reasons; undefined when it armed or when the feature is off / an arm
// already exists.
export const autoArmFabricPrewalk = async (
  state: FabricState,
  context: ExtensionContext,
  pi: ExtensionAPI,
): Promise<string | undefined> => {
  const { prewalk } = state.config;
  if (prewalk.enabled === false || !prewalk.alwaysRearm) return undefined;
  // Participants inherit Main's settings and full-code mode, but must execute
  // their assigned work instead of handing it off again on their first write.
  if (resolveFabricIdentity(context.sessionManager.getSessionId()).identity.kind !== "main") {
    return undefined;
  }
  // initialize() cancels any prior arm at session start; a non-idle status
  // means another path armed first — never clobber it.
  if (state.prewalk.status().state !== "idle") return undefined;
  // A failed in-place return left Main on the executor: auto-arming now would
  // hand the next mutation back to the same executor and capture it as the
  // new boundary model, losing the original Main. restoreBorrowedInPlaceMain
  // runs first at session start and reload, so this only triggers when that
  // recovery also failed; an explicit /fabric prewalk arm still overrides.
  const borrowed = state.prewalk.borrowedReturn();
  if (
    borrowed &&
    context.model &&
    `${context.model.provider}/${context.model.id}` === borrowed.executorModel
  ) {
    return "Fabric prewalk auto-arm skipped: Main is still on the executor after a failed in-place return. Restart the session or run /fabric reload to retry the return, or arm prewalk explicitly.";
  }
  if (!state.config.fullCodeMode || state.config.schema.mode === "enforce") {
    return "Fabric prewalk auto-arm skipped: requires full code mode with Schema enforce mode disabled.";
  }
  if (prewalk.mode === "trajectory" && !state.config.agents.enabled) {
    return "Fabric prewalk auto-arm skipped: trajectory mode requires agents.enabled.";
  }
  const model = prewalk.model?.trim();
  if (!model || !model.includes("/")) {
    return "Fabric prewalk auto-arm skipped: set prewalk.model (provider/model) in /fabric settings so sessions arm without the interactive picker.";
  }
  await armFabricPrewalkSession(state, context, pi, { model });
  return undefined;
};
