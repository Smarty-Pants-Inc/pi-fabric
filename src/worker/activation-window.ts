import fs from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";
import * as nodeModule from "node:module";
import { getCurrentSystemMessage, type Provider } from "@earendil-works/pi-ai";
import { buildSessionContext, getPackageDir, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
type AgentMessage = ReturnType<typeof buildSessionContext>["messages"][number];

const isSystem = (message: AgentMessage): boolean => (message as { role?: string }).role === "system";
const systemHead = (messages: readonly AgentMessage[]): string =>
  JSON.stringify(getCurrentSystemMessage(messages as Parameters<typeof getCurrentSystemMessage>[0]) ?? null);

/** A native startup snapshot, never a guessed prompt marker or a journal edit. */
export class ActivationWindow {
  private readonly prior: string[];
  /** The startup system records: an immutable witness (smarty-dev#390). */
  private readonly witness: readonly string[];
  private system: readonly string[];
  private current: string[] = [];

  constructor(messages: readonly AgentMessage[]) {
    // Pi journals system-prompt changes as role "system" records. It strips them
    // before the context hook and restores their replayed head after it, so the
    // conversation snapshot leaves them out; verifySystem checks them instead.
    this.prior = messages.filter(message => !isSystem(message)).map(message => JSON.stringify(message));
    this.witness = Object.freeze(messages.filter(isSystem).map(message => JSON.stringify(message)));
    this.system = this.witness;
  }

  /**
   * Checks the system prompt that reaches the model. The journal's system records must
   * start with the startup witness and may only grow during the activation, and the
   * running context's system head must be the replay of exactly those records: a
   * rewritten earlier record, in the journal or in the running context, fails closed.
   */
  verifySystem(journal: readonly AgentMessage[], running: readonly AgentMessage[]): void {
    const records = journal.filter(isSystem);
    const encoded = records.map(message => JSON.stringify(message));
    if (this.system.some((record, index) => encoded[index] !== record)) {
      throw new Error("Activation window lost its native system records");
    }
    this.system = encoded;
    if (systemHead(running) !== systemHead(records)) {
      throw new Error("Activation window lost its native system prompt");
    }
  }

  project(messages: AgentMessage[]): AgentMessage[] {
    const encoded = messages.map(message => JSON.stringify(message));
    if (this.prior.some((message, index) => encoded[index] !== message)) {
      throw new Error("Activation window lost its native history boundary");
    }
    const result = messages.slice(this.prior.length);
    const current = encoded.slice(this.prior.length);
    if (result[0]?.role !== "user" || this.current.some((message, index) => current[index] !== message)) {
      throw new Error("Activation window lost current activation messages");
    }
    this.current = current;
    return result;
  }
}

// ponytail: Pi catches extension exceptions and continues with the old context.
// A synchronous exit of this disposable child is the fail-closed boundary, not
// throw/abort/shutdown (which can leave a request or automatic retry runnable).
function failClosed(error: unknown): never {
  // An accidentally loaded hook in an owner/TUI must never exit that owner.
  // Missing launch binding also means no readiness ACK, so its worker cannot prompt.
  if (String(process.ppid) !== process.env.PI_FABRIC_ACTIVATION_WORKER_PID ||
      !process.env.PI_FABRIC_ACTIVATION_NONCE) {
    throw new Error(`Activation window is not in a disposable worker: ${String(error)}`);
  }
  try {
    fs.writeSync(2, `Fabric activation window failed: ${String(error)}\n`);
  } finally {
    process.exit(78);
  }
}

/** Loaded only by an explicitly selected actor worker; adds no tools or trust. */
export default async function activationWindow(pi: ExtensionAPI): Promise<void> {
  let window: ActivationWindow | undefined;
  try {
    if (String(process.ppid) !== process.env.PI_FABRIC_ACTIVATION_WORKER_PID || !process.env.PI_FABRIC_ACTIVATION_NONCE) {
      throw new Error("Activation window requires the worker launch binding");
    }
    // Jiti aliases the pi-ai root to compat.js and misresolves static subpaths.
    // Use the selected host's pure request estimator, not a local heuristic or
    // another copy of the host/provider/UI barrel. Package lookup supports
    // hoisted and symlinked installs without requiring a require export.
    const findPackageJSON = nodeModule.findPackageJSON;
    let estimatorUrl = "@earendil-works/pi-ai/utils/estimate";
    if (typeof findPackageJSON === "function") {
      const hostBase = pathToFileURL(path.join(getPackageDir(), "package.json"));
      const aiPackage = findPackageJSON("@earendil-works/pi-ai", hostBase);
      if (!aiPackage) throw new Error("Native request estimator package is missing");
      estimatorUrl = pathToFileURL(path.join(path.dirname(aiPackage), "dist", "utils", "estimate.js")).href;
    }
    // Bun source workers use native package imports (no Jiti root alias) and
    // do not yet implement findPackageJSON; Node hosts take the bound path above.
    const { estimateContextTokens } = await import(estimatorUrl) as typeof import("@earendil-works/pi-ai/utils/estimate");
    pi.on("session_start", (_event, ctx) => {
      try {
        const hook = fs.realpathSync(fileURLToPath(import.meta.url));
        if (window || ctx.mode !== "rpc" || !process.env.PI_FABRIC_PARENT_RUN ||
            !process.env.PI_FABRIC_ACTIVATION_NONCE || process.env.PI_FABRIC_ACTIVATION_HOOK !== hook) {
          throw new Error("Activation window requires a fresh Fabric RPC worker");
        }
        window = new ActivationWindow(buildSessionContext(ctx.sessionManager.getBranch()).messages);
        // Pi redirects ordinary stdout writes to stderr in RPC mode. This bound
        // protocol ACK must use stdout itself, not the redirected logging stream.
        fs.writeSync(1, `${JSON.stringify({
          type: "fabric_activation_window_ready",
          runId: process.env.PI_FABRIC_PARENT_RUN,
          nonce: process.env.PI_FABRIC_ACTIVATION_NONCE,
          policy: "activation",
          protocol: 1,
          hook,
        })}\n`);
      } catch (error) {
        failClosed(error);
      }
    });
    pi.on("context", event => {
      try {
        if (!window) throw new Error("Activation window is not initialized");
        return { messages: window.project(event.messages) };
      } catch (error) {
        return failClosed(error);
      }
    });
    // context_with_system is still a transform phase: later handlers can replace
    // its output. Pi resolves the provider and assembles headers only AFTER all
    // context transforms and its own convertToLlm/normalization. Decorate that
    // resolved provider's public dispatch entry points, not a context snapshot.
    // prepareRequest retains this exact provider across the awaited header hook;
    // a later provider registration cannot replace the dispatch being admitted.
    // Reinstall per request so refresh/registration during a transform is covered.
    // No private host fields, alternate AI runtime, auth or provider composition.
    const guarded = new WeakSet<Provider>();
    pi.on("before_provider_headers", (_event, ctx) => {
      try {
        if (!window || !ctx.model) throw new Error("Activation window is not initialized");
        // A context transform may select a different session model after Pi
        // captured this request's model. Guard all registered model providers,
        // not just ctx.model (the live selector), and use the dispatch argument
        // as the authoritative window below. getAll is the host's loaded snapshot.
        const providers = new Set(ctx.modelRegistry.getAll().map(model => model.provider));
        providers.add(ctx.model.provider);
        const admit = (model: Parameters<Provider["streamSimple"]>[0], context: Parameters<Provider["streamSimple"]>[1]): void => {
          try {
            // This is Pi's final converted, normalized request context, including
            // the effective system/tools and current tool results. Do not estimate
            // earlier: a raw over-window context may be reduced by a later hook.
            const tokens = estimateContextTokens(context.messages).tokens;
            if (tokens > model.contextWindow) {
              failClosed(`Context exceeds window: estimated ${tokens} input tokens, window ${model.contextWindow}`);
            }
            window!.verifySystem(buildSessionContext(ctx.sessionManager.getBranch()).messages, context.messages);
          } catch (error) {
            failClosed(error);
          }
        };
        for (const id of providers) {
          const provider = ctx.modelRegistry.getProvider(id);
          if (!provider || typeof provider.streamSimple !== "function" || typeof provider.stream !== "function") {
            throw new Error("Activation window requires native provider dispatch support");
          }
          if (guarded.has(provider)) continue;
          const stream = provider.stream;
          const streamSimple = provider.streamSimple;
          provider.stream = (model, context, options) => {
            admit(model, context);
            return stream.call(provider, model, context, options);
          };
          provider.streamSimple = (model, context, options) => {
            admit(model, context);
            return streamSimple.call(provider, model, context, options);
          };
          guarded.add(provider);
        }
      } catch (error) {
        failClosed(error);
      }
    });
    // Native summarization bypasses the context hook. Never summarize either
    // historical messages or incomplete current tool exchanges under this policy.
    pi.on("session_before_compact", () => failClosed("Compaction is unsupported during an activation window"));
    pi.on("session_before_tree", () => ({ cancel: true }));
    pi.on("session_before_switch", () => ({ cancel: true }));
    pi.on("session_before_fork", () => ({ cancel: true }));
  } catch (error) {
    failClosed(error);
  }
}
