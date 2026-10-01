import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AgentRunResult } from "./types.js";
import { fabricHostIdentity, fabricProvenanceSupported, sendFabricMessage } from "../fabric-provenance.js";

export const AGENT_COMPLETION_MESSAGE_TYPE = "pi-fabric-agent-complete";
const SUMMARY_CHARS = 4_000;
const BATCH_CHARS = 16_000;
const IDLE_BATCH_MS = 40;

type Completion = Pick<AgentRunResult, "id" | "name" | "status" | "text" | "error" | "startedAt" | "finishedAt" | "completionDelivery">;
type PendingCompletion = { result: Completion; delivered: (() => void) | undefined };
type CompletionMessage = { customType: string; content: string; display: boolean; details: { ids: string[] } };

const oneLine = (text: string): string => text.replace(/[\u0000-\u001f\u007f]/g, " ");
const clip = (text: string, limit: number): string =>
  text.length > limit ? `${text.slice(0, limit)}\n[truncated; use agents.wait({id}) for the full result]` : text;

/** Own notifications until the tool batch ends, so a late wait can still retract them. */
export class AgentCompletionInbox {
  readonly #pending = new Map<string, PendingCompletion>();
  readonly #acknowledged = new Set<string>();
  readonly #handed = new Map<string, (() => void) | undefined>();
  readonly #unsubscribe: Array<() => void> = [];
  #context: ExtensionContext;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #suspended = false;
  #closed = false;

  constructor(readonly pi: ExtensionAPI, context: ExtensionContext) {
    this.#context = context;
    const subscribe = (event: string, handler: (...handlerArgs: any[]) => unknown): void => {
      if (typeof pi.on !== "function") return;
      const unsubscribe = (pi.on as (name: string, fn: (...fnArgs: any[]) => unknown) => unknown)(event, handler);
      if (typeof unsubscribe === "function") this.#unsubscribe.push(unsubscribe as () => void);
    };
    subscribe("turn_end", (event, ctx) => {
        this.#context = ctx;
        this.#confirmHeld();
        const stopReason = event.message?.role === "assistant" ? event.message.stopReason : undefined;
        if (ctx.signal?.aborted || stopReason === "aborted" || stopReason === "error") {
          this.#suspended = true;
          return;
        }
        this.#flush();
      });
    // Any new run ends a suspension, not only typed input: after an abort the inbox must not
    // start a run by itself, but results parked since then join the next run Main makes for any
    // reason. A voice- or peer-driven session may never see typed input again, and its results
    // stayed parked for hours (smarty-dev#733). A run started by a triggered custom message
    // (peer delivery, voice) emits agent_start but neither input nor before_agent_start, so
    // agent_start ends the suspension; the results then go in at that run's first turn_end.
    subscribe("agent_start", (_event, ctx) => {
        this.#context = ctx;
        this.#suspended = false;
      });
    subscribe("before_agent_start", (_event, ctx) => {
        this.#context = ctx;
        // A prompt-started run: its results join the first inference.
        this.#suspended = false;
        // Capable Pi drains nextTurn after hooks; legacy Pi needs the hook result.
        let message: CompletionMessage | undefined;
        this.#flush(value => {
          if (!fabricProvenanceSupported(this.pi)) { message = value; return; }
          sendFabricMessage(this.pi, value, { deliverAs: "nextTurn", triggerTurn: false },
            () => fabricHostIdentity(ctx.sessionManager.getSessionId()), "actor", "mesh");
        });
        return message ? { message } : undefined;
      });
    subscribe("context", (_event, ctx) => { this.#context = ctx; this.#confirmHeld(); });
    subscribe("turn_start", (_event, ctx) => { this.#context = ctx; this.#confirmHeld(); });
    subscribe("agent_settled", (_event, ctx) => {
        if (this.#context.signal?.aborted || ctx.signal?.aborted) this.#suspended = true;
        this.#context = ctx;
        this.#confirmHeld();
        this.#schedule();
      });
    subscribe("input", (_event, ctx) => {
        this.#context = ctx;
        this.#suspended = false;
      });
    subscribe("session_tree", (_event, ctx) => {
        this.#context = ctx;
        // Navigation abandons this frontier, not the visible run history.
        for (const { result, delivered } of this.#pending.values()) {
          this.#acknowledged.add(result.id);
          this.#confirmDelivery(delivered);
        }
        this.#pending.clear();
      });
  }

  enqueue(result: Completion, delivered?: () => void): void {
    if (this.#closed) return;
    if (this.#acknowledged.has(result.id)) {
      if (this.#handed.has(result.id)) {
        this.#handed.set(result.id, delivered ?? this.#handed.get(result.id));
        this.#confirmHeld();
      } else this.#confirmDelivery(delivered);
      return;
    }
    if (this.#pending.has(result.id)) return;
    this.#pending.set(result.id, {
      result: {
        id: result.id, name: result.name, status: result.status, startedAt: result.startedAt,
        ...(result.finishedAt !== undefined ? { finishedAt: result.finishedAt } : {}),
        text: clip(result.text, SUMMARY_CHARS),
        ...(result.completionDelivery ? { completionDelivery: result.completionDelivery } : {}),
        ...(result.error !== undefined ? { error: clip(result.error, SUMMARY_CHARS) } : {}),
      },
      delivered,
    });
    if (this.#context.hasUI) {
      const failure = result.status !== "completed";
      const detail = failure && result.error ? `: ${oneLine(result.error).slice(0, 180)}` : "";
      this.#context.ui.notify(`Agent ${oneLine(result.name).slice(0, 80)} ${result.status}${detail}`, failure ? "warning" : "info");
    }
    this.#schedule();
  }

  acknowledge(id: string): void {
    this.#acknowledged.add(id);
    this.#pending.delete(id);
    this.#handed.delete(id);
  }

  close(): void {
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    for (const unsubscribe of this.#unsubscribe) unsubscribe();
    this.#pending.clear();
    this.#acknowledged.clear();
    this.#handed.clear();
  }

  #confirmHeld(): void {
    if (!this.#handed.size) return;
    const getEntries = this.#context.sessionManager?.getEntries;
    if (typeof getEntries !== "function") {
      // Compatibility for hosts without session inspection; modern Pi requires its persisted carrier.
      for (const delivered of this.#handed.values()) this.#confirmDelivery(delivered);
      this.#handed.clear();
      return;
    }
    const entries = getEntries.call(this.#context.sessionManager);
    // A completion carrier is newly appended. Keep this observation bounded even on huge sessions.
    for (let index = entries.length - 1; index >= Math.max(0, entries.length - 512); index--) {
      const entry = entries[index];
      if (entry?.type !== "custom_message" || entry.customType !== AGENT_COMPLETION_MESSAGE_TYPE) continue;
      const ids = (entry.details as { ids?: unknown } | undefined)?.ids;
      if (!Array.isArray(ids)) continue;
      for (const id of ids) {
        if (typeof id !== "string" || !this.#handed.has(id)) continue;
        const delivered = this.#handed.get(id);
        this.#handed.delete(id);
        this.#confirmDelivery(delivered);
      }
    }
  }

  #confirmDelivery(delivered: (() => void) | undefined): void {
    try { delivered?.(); } catch {
      // The durable envelope remains queued and retries its receipt on the next poll.
    }
  }

  #schedule(): void {
    if (this.#closed || this.#timer || this.#suspended || !this.#pending.size) return;
    // Never enqueue into Pi during an active tool batch. turn_end owns that path.
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.#context.isIdle() && !this.#context.hasPendingMessages()) this.#flush();
    }, IDLE_BATCH_MS);
    this.#timer.unref?.();
  }

  #flush(deliver: (message: CompletionMessage) => void = (message) =>
    sendFabricMessage(this.pi, message, { deliverAs: "steer", triggerTurn: true }, () => fabricHostIdentity(this.#context.sessionManager.getSessionId()), "steer", "mesh")): void {
    if (this.#closed || this.#suspended || this.#context.signal?.aborted || !this.#pending.size) return;
    const batch = [...this.#pending.values()].slice(0, 32);
    const perResult = Math.max(0, Math.min(SUMMARY_CHARS, Math.floor(BATCH_CHARS / batch.length) - 320));
    const content = [
      "Unread background agent results (batched). Incorporate relevant results into the current task. These are run outcomes, not new user requests. Do not restart completed work or reply merely to acknowledge stale/superseded results. A completed run does not necessarily mean its assignment is complete.",
      ...batch.map(({ result }) => {
        const seconds = Math.round(Math.max(0, (result.finishedAt ?? Date.now()) - result.startedAt) / 1_000);
        const summary = [result.error, result.text].filter(Boolean).join("\n");
        const redelivery = result.completionDelivery?.redeliveredFrom;
        const provenance = redelivery ? ` [re-delivered from dead Main session ${oneLine(redelivery)}]` : "";
        return `Agent ${oneLine(result.name).slice(0, 80)} (${result.id}) ${result.status} after ${seconds}s${provenance}:\n${clip(summary || "no result", perResult)}`;
      }),
    ].join("\n\n");
    deliver({
      customType: AGENT_COMPLETION_MESSAGE_TYPE,
      content,
      display: false,
      details: { ids: batch.map(({ result }) => result.id) },
    });
    for (const { result, delivered } of batch) {
      this.#pending.delete(result.id);
      this.#acknowledged.add(result.id);
      this.#handed.set(result.id, delivered);
    }
    this.#confirmHeld();
  }
}
