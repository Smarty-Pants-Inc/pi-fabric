import type { AssistantMessage } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { resolveFabricIdentity, sendFabricMessage } from "./fabric-provenance.js";

/** Final settlement is after Pi's own retry/overflow recovery, unlike agent_end (#4012). */
export const registerMainProviderRecovery = (pi: ExtensionAPI, options: {
  halted(): boolean;
  report(message: string, context: ExtensionContext): Promise<unknown>;
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
  pi.on("agent_start", cancel);
  pi.on("input", cancel);
  pi.on("session_shutdown", reset);
  pi.on("session_before_switch", reset);
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
    // Optional provider utilities must not enter registration's eager graph.
    const { isContextOverflow } = await import("@earendil-works/pi-ai/compat");
    if (generation !== current || context.signal?.aborted || options.halted() ||
      isContextOverflow(last, context.model?.contextWindow ?? 0)) return;
    if (retried) {
      if (reported) return;
      reported = true;
      const message = `BLOCKED: provider error: ${last.errorMessage?.split(/\r?\n/, 1)[0] || "Unknown provider error"}`;
      // Persist/display without waking the failed lane for a third attempt.
      sendFabricMessage(pi, { customType: "pi-fabric-provider-blocked", content: message, display: true },
        { deliverAs: "followUp", triggerTurn: false });
      context.ui.notify(message, "error");
      try { await options.report(message, context); }
      catch (error) { context.ui.notify(`Provider error report delivery failed: ${String(error)}`, "error"); }
      return;
    }
    retried = true;
    // An owner input, another turn, session replacement, or Escape cancels this wake.
    timer = setTimeout(() => {
      timer = undefined;
      if (generation !== current || context.signal?.aborted || options.halted() || !context.isIdle()) return;
      sendFabricMessage(pi, {
        customType: "pi-fabric-provider-retry", display: true,
        content: "Continue the interrupted turn after the provider error. This is Fabric's one automatic retry.",
      }, { deliverAs: "followUp", triggerTurn: true });
    }, 1_000);
    timer.unref?.();
  });
};
