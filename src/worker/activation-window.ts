import fs from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";
import * as nodeModule from "node:module";
import { getCurrentSystemMessage, type Provider, type ProviderRequestOptions } from "@earendil-works/pi-ai";
import { buildSessionContext, convertToLlm, getPackageDir, sessionEntryToContextMessages, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
type AgentMessage = ReturnType<typeof buildSessionContext>["messages"][number];

const isSystem = (message: AgentMessage): boolean => (message as { role?: string }).role === "system";
const systemHead = (messages: readonly AgentMessage[], checkpoint = false): string => {
  const head = getCurrentSystemMessage(messages as Parameters<typeof getCurrentSystemMessage>[0]);
  // Pi timestamps a native compaction checkpoint when it appends the marker.
  // Only that timestamp may differ; raw system records remain byte-witnessed.
  if (head && checkpoint) {
    const { timestamp: _timestamp, ...semanticHead } = head;
    return JSON.stringify(semanticHead);
  }
  return JSON.stringify(head ?? null);
};

/** A native startup snapshot, never a guessed prompt marker or a journal edit. */
export class ActivationWindow {
  private readonly prior: string[];
  /** The startup system records: an immutable witness (smarty-dev#390). */
  private readonly witness: readonly string[];
  private system: readonly string[];
  private current: string[] = [];
  private compacted = false;
  private pending: { summary: string; messages: string[] } | undefined;

  authorizeCompaction(summary: string, messages: AgentMessage[]): void {
    if (this.pending) throw new Error("Activation compaction is already pending");
    this.pending = { summary, messages: messages.map(message => JSON.stringify(message)) };
  }

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
    if (systemHead(running, this.compacted) !== systemHead(records, this.compacted)) {
      throw new Error("Activation window lost its native system prompt");
    }
  }

  project(messages: AgentMessage[]): AgentMessage[] {
    const encoded = messages.map(message => JSON.stringify(message));
    if (this.pending) {
      const summary = messages[0] as { role?: string; summary?: string } | undefined;
      if (summary?.role !== "compactionSummary" || summary.summary !== this.pending.summary ||
          this.pending.messages.some((message, index) => encoded[index + 1] !== message)) {
        throw new Error("Activation window lost its authorized compaction");
      }
      // Native checkpoint metadata (timestamp/tokensBefore) is host-owned. The
      // summary and every retained message are checked before accepting it.
      this.prior.length = 0;
      this.current = encoded.slice(0, this.pending.messages.length + 1);
      this.pending = undefined;
      this.compacted = true;
    }
    if (this.prior.some((message, index) => encoded[index] !== message)) {
      throw new Error("Activation window lost its native history boundary");
    }
    const result = messages.slice(this.prior.length);
    const current = encoded.slice(this.prior.length);
    if ((!this.compacted && result[0]?.role !== "user") || this.current.some((message, index) => current[index] !== message)) {
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
    const { estimateTextTokens } = await import(estimatorUrl) as typeof import("@earendil-works/pi-ai/utils/estimate");
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
    // Both context_with_system and before_provider_request are transform phases.
    // Pi resolves the provider and assembles headers after context normalization,
    // but that provider builds its wire payload and awaits onPayload even later.
    // Decorate the resolved provider to wrap (not replace) that awaited callback:
    // admission must follow the COMPLETE native before_provider_request chain.
    // prepareRequest retains this exact provider across the awaited header hook;
    // a later provider registration cannot replace the dispatch being admitted.
    // Reinstall per request so refresh/registration during a transform is covered.
    // No private host fields, alternate AI runtime, auth or provider composition.
    const guarded = new WeakSet<Provider>();
    let lastPayload: { tokens: number; contextWindow: number; overheadTokens: number } | undefined;
    pi.on("turn_end", async event => {
      try {
        if (!window || !lastPayload || !event.toolResults.length) return;
        // Measure the actual last dispatched payload plus this completed batch,
        // not usage from before the tools or a carried journal's token count.
        const completedBatch = convertToLlm([event.message, ...event.toolResults]);
        const nextTokens = lastPayload.tokens + estimateTextTokens(JSON.stringify(completedBatch));
        if (nextTokens < lastPayload.contextWindow * 0.8) return;
        // Stable package-local first-use edge; idle activation registration stays cheap.
        const { compactActivationTools } = await import("./activation-compaction.js");
        // Compaction may only replace our witnessed append-only activation,
        // not legitimize a prior boundary handler rewriting its messages.
        window.project(event.context.contextMessages.filter(message => !isSystem(message)));
        let plan = compactActivationTools(event);
        const retained = plan?.messages ?? event.context.contextMessages.filter(message => !isSystem(message));
        const projectedTokens = lastPayload.overheadTokens + estimateTextTokens(JSON.stringify(convertToLlm(retained))) +
          estimateTextTokens(plan?.summary ?? "");
        // Prefer preserving the latest batch. Only compact it when the bounded
        // older history still cannot fit; keep every raw output in the journal.
        if (projectedTokens > lastPayload.contextWindow) plan = compactActivationTools(event, { includeLatest: true });
        if (!plan) return; // Irreducible instructions/schemas still refuse once at final admission.
        window.authorizeCompaction(plan.summary, plan.messages);
        return { entries: plan.entries };
      } catch (error) {
        return failClosed(error);
      }
    });
    pi.on("before_provider_headers", (_event, ctx) => {
      try {
        if (!window || !ctx.model) throw new Error("Activation window is not initialized");
        // A context transform may select a different session model after Pi
        // captured this request's model. Guard all registered model providers,
        // not just ctx.model (the live selector), and use the dispatch argument
        // as the authoritative window below. getAll is the host's loaded snapshot.
        const providers = new Set(ctx.modelRegistry.getAll().map(model => model.provider));
        providers.add(ctx.model.provider);
        const verify = (context: Parameters<Provider["streamSimple"]>[1]): void => {
          try {
            // Compaction checkpoints replay a single system head; audit the raw
            // append-only system records, not that compacted projection.
            window!.verifySystem(ctx.sessionManager.getBranch().flatMap(entry =>
              entry.type === "message" ? sessionEntryToContextMessages(entry) : []), context.messages);
          } catch (error) {
            failClosed(error);
          }
        };
        const guardPayload = (
          model: Parameters<Provider["streamSimple"]>[0], options: ProviderRequestOptions | undefined,
          context: Parameters<Provider["streamSimple"]>[1],
        ): NonNullable<ProviderRequestOptions["onPayload"]> => async (payload, requestModel) => {
          try {
            const replacement = await options?.onPayload?.(payload, requestModel);
            // Undefined retains the input, including any in-place mutations.
            const final = replacement === undefined ? payload : replacement;
            if (!final || typeof final !== "object" || Array.isArray(final)) {
              throw new Error("Activation window requires a JSON provider request object");
            }
            // Google supplies SDK parameters, not wire JSON. Both native Google
            // APIs put options.signal in config.abortSignal; OpenAI/Anthropic
            // keep signal/timeout in separate SDK request options and fetch in
            // client options (qualified native artifact 623f5790). Exclude only
            // that qualified control path, never arbitrary similarly named data.
            // Copy before serialization so even a signal's toJSON is not called.
            const google = model.api === "google-generative-ai" || model.api === "google-vertex";
            let abortSignal: AbortSignal | undefined;
            let data = final;
            if (google) {
              if (![Object.prototype, null].includes(Object.getPrototypeOf(final))) {
                throw new Error("Activation window cannot admit a non-JSON provider payload");
              }
              const parameters = { ...final } as Record<string, unknown>;
              const config = parameters.config;
              if (config !== undefined) {
                if (!config || typeof config !== "object" || Array.isArray(config) ||
                    ![Object.prototype, null].includes(Object.getPrototypeOf(config))) {
                  throw new Error("Activation window requires a JSON Google config object");
                }
                const { abortSignal: control, ...fields } = config as Record<string, unknown>;
                if (control !== undefined && !(control instanceof AbortSignal)) {
                  throw new Error("Activation window requires a native Google AbortSignal control");
                }
                abortSignal = control;
                parameters.config = fields;
              }
              data = parameters;
            }
            // Estimate all context-bearing data: system/messages, tool schemas,
            // and API-specific fields, with no stale assistant-usage shortcut.
            // JSON framing is conservative; unknown non-JSON data fails closed.
            const encoded = JSON.stringify(data, (_key, value: unknown) => {
              if (typeof value === "function" || typeof value === "symbol" || typeof value === "bigint" ||
                  (typeof value === "number" && !Number.isFinite(value)) ||
                  (value && typeof value === "object" && !Array.isArray(value) &&
                    ![Object.prototype, null].includes(Object.getPrototypeOf(value)))) {
                throw new Error("Activation window cannot admit a non-JSON provider payload");
              }
              return value;
            });
            if (encoded === undefined) throw new Error("Activation window cannot serialize the provider payload");
            const admitted: unknown = JSON.parse(encoded);
            if (!admitted || typeof admitted !== "object" || Array.isArray(admitted)) {
              throw new Error("Activation window requires a JSON provider request object");
            }
            const tokens = estimateTextTokens(encoded);
            if (tokens > model.contextWindow) {
              failClosed(`Context exceeds window: estimated ${tokens} input tokens, window ${model.contextWindow}`);
            }
            const conversationTokens = estimateTextTokens(JSON.stringify(context.messages.filter(message => message.role !== "system")));
            lastPayload = { tokens, contextWindow: model.contextWindow, overheadTokens: Math.max(0, tokens - conversationTokens) };
            // Ordinary wire payloads dispatch the admitted JSON snapshot, not
            // stateful getters/toJSON that could change at the next serialization.
            if (!google) return admitted;
            // Dispatch a detached root: an extension may retain and mutate the
            // original parameters after admission (smarty-dev#3337). Carry only
            // the live cancellation control by identity, without serializing it.
            // Config remains mutable for the SDK's own schema normalization.
            if (abortSignal !== undefined) {
              const config = (admitted as Record<string, unknown>).config;
              if (!config || typeof config !== "object" || Array.isArray(config)) {
                throw new Error("Activation window lost the Google config object");
              }
              Object.defineProperty(config, "abortSignal", {
                value: abortSignal, enumerable: true, configurable: true, writable: true,
              });
            }
            return admitted;
          } catch (error) {
            return failClosed(error);
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
            verify(context);
            // Preserve the API-specific conditional options type while copying it.
            const guardedOptions = { ...options } as NonNullable<typeof options>;
            guardedOptions.onPayload = guardPayload(model, options, context);
            return stream.call(provider, model, context, guardedOptions);
          };
          provider.streamSimple = (model, context, options) => {
            verify(context);
            return streamSimple.call(provider, model, context, { ...options, onPayload: guardPayload(model, options, context) });
          };
          guarded.add(provider);
        }
      } catch (error) {
        failClosed(error);
      }
    });
    // Unbound manual/automatic summarization bypasses the context hook. Only
    // the witnessed native turn_end drafts above may compact this activation.
    pi.on("session_before_compact", () => failClosed("Compaction is unsupported during an activation window"));
    pi.on("session_before_tree", () => ({ cancel: true }));
    pi.on("session_before_switch", () => ({ cancel: true }));
    pi.on("session_before_fork", () => ({ cancel: true }));
  } catch (error) {
    failClosed(error);
  }
}
