import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { PerCallShadowRouter, PerCallSettings } from "./per-call-route.js";

/**
 * Cheap, always-registered hooks for per-call shadow routing (smarty-dev#2890). Every handler reads the
 * mode first; with "off" (the default) nothing is imported and nothing is written. With "shadow" the router
 * module loads on the first LLM call. No handler returns a value and none calls setModel: the context, the
 * model and the response are exactly what they would be without this module.
 */
export function registerLazyPerCallRouter(pi: ExtensionAPI, getSettings: () => PerCallSettings | undefined): void {
  let router: PerCallShadowRouter | undefined;
  let loading: Promise<PerCallShadowRouter> | undefined;
  let turnIndex = 0;
  const settings = (): PerCallSettings | undefined => { try { return getSettings(); } catch { return undefined; } };
  const withRouter = (use: (router: PerCallShadowRouter) => void): void => {
    const run = (r: PerCallShadowRouter): void => { try { use(r); } catch { /* shadow only */ } };
    if (router) { run(router); return; }
    if (!loading) {
      const pending: Promise<PerCallShadowRouter> = import("./per-call-route.js").then(module => {
        const created = new module.PerCallShadowRouter({ settings });
        // A session that ended while this loaded keeps its router out of the next session.
        if (loading === pending) router = created;
        return created;
      });
      loading = pending;
    }
    void loading.then(run, () => undefined);
  };
  const sessionId = (context: ExtensionContext): string => {
    try { return context.sessionManager.getSessionId() ?? "unknown"; } catch { return "unknown"; }
  };
  pi.on("turn_start", (event) => { turnIndex = event.turnIndex; });
  pi.on("context", (event, context) => {
    if (settings()?.perCall?.mode !== "shadow") return undefined;
    let contextTokens: number | null = null;
    try { contextTokens = context.getContextUsage()?.tokens ?? null; } catch { /* estimate instead */ }
    const snapshot = { sessionId: sessionId(context), turnIndex, messages: event.messages,
      model: context.model ? `${context.model.provider}/${context.model.id}` : null, contextTokens };
    withRouter(r => r.onContext(snapshot));
    return undefined;
  });
  pi.on("message_end", (event, context) => {
    if ((!router && !loading) || event.message.role !== "assistant") return undefined;
    const id = sessionId(context);
    withRouter(r => r.onMessageEnd(id, event.message));
    return undefined;
  });
  pi.on("session_shutdown", async () => {
    if (!loading) return;
    const current = loading;
    // A closed router has aborted its Jev work; a later session in this process gets a fresh one.
    router = undefined;
    loading = undefined;
    const loaded = await current.catch(() => undefined);
    await loaded?.close().catch(() => undefined);
  });
}
