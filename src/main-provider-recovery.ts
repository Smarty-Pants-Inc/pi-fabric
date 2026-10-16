import type { AssistantMessage } from "@earendil-works/pi-ai";
import { notifyDiagnostic, notifyPrincipalDiagnosticOnce, resetPrincipalDiagnosticTurn } from "./core/diagnostics.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { resolveFabricIdentity, sendFabricMessage } from "./fabric-provenance.js";
import { extensionContextLive } from "./session-liveness.js";

/** Final settlement is after Pi's own retry/overflow recovery, unlike agent_end (#4012). */
export const registerMainProviderRecovery = (pi: ExtensionAPI, options: {
  halted(): boolean;
  report(message: string, context: ExtensionContext): Promise<unknown>;
  /** Main's drain owns a non-retryable error: compaction once, then alarm (smarty-dev#8742). */
  ownsNonRetryable?(): boolean;
}): void => {
  let retried = false;
  let reported = false;
  let generation = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const cancel = (): void => {
    generation++;
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  const reset = (): void => { cancel(); retried = false; reported = false; };
  pi.on("agent_start", (_event, context) => { resetPrincipalDiagnosticTurn(pi, context); cancel(); });
  pi.on("input", (_event, context) => { resetPrincipalDiagnosticTurn(pi, context); cancel(); });
  pi.on("session_start", (_event, context) => { resetPrincipalDiagnosticTurn(pi, context); reset(); });
  pi.on("session_shutdown", reset);
  pi.on("session_before_switch", (_event, context) => { resetPrincipalDiagnosticTurn(pi, context); reset(); });
  pi.on("session_tree", reset);
  pi.on("agent_settled", async (event, context) => {
    cancel();
    const { identity, mainAgentId } = resolveFabricIdentity(context.sessionManager.getSessionId());
    if (identity.kind !== "main" || identity.id !== mainAgentId) return;
    if (context.signal?.aborted || options.halted() ||
      (event as { outcome?: string }).outcome === "aborted") return;
    const entries = context.sessionManager.getEntries();
    let last: AssistantMessage | undefined;
    for (let i = entries.length - 1; i >= Math.max(0, entries.length - 50); i--) {
      const entry = entries[i]!;
      if (entry.type === "message" && entry.message.role === "assistant") {
        last = entry.message;
        break;
      }
    }
    if (!last || last.stopReason === "aborted") return;
    if (last.stopReason !== "error") { reset(); return; }
    const current = generation;
    // smarty-dev#10718: dispose/replacement invalidates this ctx and emits no session_shutdown here.
    // Every continuation below (await, backoff, idle poll) retires quietly on a stale ctx instead of
    // throwing Pi's stale-ctx error out of a timer, which is uncaught and exits Pi. No timer of this
    // settle is pending when live() runs (wakeWhenIdle clears it first), so retiring is just returning.
    const live = (): boolean => generation === current && extensionContextLive(context);
    // The classifier loads at the first failed settle: it is shared with the lazy Main drain and
    // would otherwise split into its own eager startup chunk.
    const { classifyProviderError } = await import("./provider-error-class.js");
    if (!live()) return;
    // A blind retry of a request the provider rejects as non-retryable fails the same way.
    const classified = classifyProviderError(last);
    if (classified?.retryable === false && options.ownsNonRetryable?.() === true) {
      const message = `Provider rejected Main's request as non-retryable (${classified.short}); compacting once to recover`;
      if (context.hasUI) notifyPrincipalDiagnosticOnce(pi, context, "provider-non-retryable", message, "warning");
      reset();
      return;
    }
    // Optional provider utilities must not enter registration's eager graph.
    const { isContextOverflow } = await import("@earendil-works/pi-ai/compat");
    if (!live() || context.signal?.aborted || options.halted() ||
      isContextOverflow(last, context.model?.contextWindow ?? 0)) return;
    if (retried) {
      if (reported) return;
      reported = true;
      // smarty-dev#8742: only the classifier's allowlisted short form (HTTP status, a known error type,
      // a fixed code) leaves this Main. A provider can echo prompt or secret text in errorMessage; that
      // text stays in the failed assistant message in the local session log.
      const message = `BLOCKED: provider error: ${classifyProviderError(last)?.short ?? "unknown provider error"}`;
      // Persist/display without waking the failed lane for a third attempt.
      sendFabricMessage(pi, { customType: "pi-fabric-provider-blocked", content: message, display: true },
        { deliverAs: "followUp", triggerTurn: false });
      context.ui.notify(message, "error");
      try { await options.report(message, context); }
      catch (error) {
        if (live()) notifyDiagnostic(context, "src/main-provider-recovery.ts", `Provider error report delivery failed: ${String(error)}`, "error");
      }
      return;
    }
    // An owner input, another turn, session replacement, or Escape cancels this wake.
    // Later settlement hooks may compact past the backoff. Keep the wake pending
    // until Pi is idle; a busy sample is not a triggered retry and spends no budget.
    const wakeWhenIdle = (): void => {
      timer = undefined;
      if (!live() || context.signal?.aborted || options.halted()) return;
      if (!context.isIdle() || (context as { isSettling?: () => boolean }).isSettling?.()) {
        timer = setTimeout(wakeWhenIdle, 100);
        timer.unref?.();
        return;
      }
      retried = true;
      sendFabricMessage(pi, {
        customType: "pi-fabric-provider-retry", display: true,
        content: "Continue the interrupted turn after the provider error. This is Fabric's one automatic retry.",
      }, { deliverAs: "followUp", triggerTurn: true });
    };
    timer = setTimeout(wakeWhenIdle, 1_000);
    timer.unref?.();
  });
};
