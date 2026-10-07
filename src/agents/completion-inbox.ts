import { fabricWarn } from "../core/diagnostics.js";
import fs from "node:fs";
import { ChildCompletionClaimLostError } from "../result-consumption.js";
import { syncPathNamespace } from "../core/atomic-write.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AgentRunResult } from "./types.js";
import { fabricProvenanceSupported, sendFabricMessage } from "../fabric-provenance.js";

export const AGENT_COMPLETION_MESSAGE_TYPE = "pi-fabric-agent-complete";
const SUMMARY_CHARS = 4_000;
const BATCH_CHARS = 16_000;
const IDLE_BATCH_MS = 40;

type Completion = Pick<AgentRunResult, "id" | "name" | "status" | "text" | "error" | "startedAt" | "finishedAt" | "completionDelivery">;
type PendingCompletion = { result: Completion; delivered: (() => void) | undefined; prepare: (() => void) | undefined };
type CompletionMessage = { customType: string; content: string; display: boolean; details: { ids: string[] } };

const oneLine = (text: string): string => text.replace(/[\u0000-\u001f\u007f]/g, " ");
const clip = (text: string, limit: number): string =>
  text.length > limit ? `${text.slice(0, limit)}\n[truncated; use agents.wait({id}) for the full result]` : text;

/** Own notifications until the tool batch ends, so a late wait can still retract them. */
export class AgentCompletionInbox {
  readonly #pending = new Map<string, PendingCompletion>();
  readonly #acknowledged = new Set<string>();
  readonly #handed = new Map<string, (() => void) | undefined>();
  readonly #receiptRetries = new Set<() => void>();
  readonly #unsubscribe: Array<() => void> = [];
  #context: ExtensionContext;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #suspended = false;
  #closed = false;
  #receiptSource = "";
  #receiptOffset = 0;
  #persistedIds = new Set<string>();
  #receiptFault: string | undefined;

  constructor(readonly pi: ExtensionAPI, context: ExtensionContext, readonly commitBatch?: (ids: string[]) => void) {
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
        this.#flush((value, result) => {
          if (!fabricProvenanceSupported(this.pi)) { message = value; return; }
          sendFabricMessage(this.pi, value, { deliverAs: "nextTurn", triggerTurn: false },
            { id: result.id, name: result.name, kind: "agent" }, "actor", "mesh");
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
        // Navigation is not durable publication. Leave these outcomes unread in the journal.
        this.#pending.clear();
      });
  }

  enqueue(result: Completion, delivered?: () => void, prepare?: () => void): void {
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
      delivered, prepare,
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
    for (const receipt of this.#receiptRetries) this.#confirmDelivery(receipt);
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    for (const unsubscribe of this.#unsubscribe) unsubscribe();
    this.#pending.clear();
    this.#acknowledged.clear();
    this.#handed.clear();
    this.#receiptRetries.clear();
  }

  #confirmHeld(): void {
    if (!this.#handed.size) return;
    try {
      // Pi inserts entries in memory before I/O, and defers a fresh file until the first
      // assistant message. Neither getEntries nor an uninspectable host is a durable receipt.
      const ids = this.#sessionReceipt();
      for (const [id, delivered] of this.#handed) {
        if (ids.has(id)) {
          this.#confirmDelivery(delivered);
          this.#handed.delete(id);
        }
      }
      this.#receiptFault = undefined;
    } catch (error) {
      const diagnostic = `Fabric completion carrier remains unconfirmed: ${String(error).slice(0, 1000)}`;
      if (diagnostic !== this.#receiptFault) fabricWarn(diagnostic);
      this.#receiptFault = diagnostic;
    }
  }

  /** Index only complete JSONL lines after syncing the opened file and its reopenable namespace. */
  #sessionReceipt(): ReadonlySet<string> {
    const manager = this.#context.sessionManager;
    const file = manager?.getSessionFile?.();
    if (!file) return new Set(); // In-memory sessions cannot consume a durable source.
    let fd: number;
    try { fd = fs.openSync(file, process.platform === "win32" ? "r+" : "r"); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      this.#receiptSource = "";
      this.#receiptOffset = 0;
      this.#persistedIds.clear();
      return this.#persistedIds;
    }
    try {
      const stat = fs.fstatSync(fd);
      if (!stat.isFile()) throw new Error("Session receipt is not a regular file");
      fs.fsyncSync(fd);
      syncPathNamespace(file, stat);
      const sessionId = manager.getSessionId();
      const source = JSON.stringify([file, stat.dev, stat.ino, sessionId]);
      if (source !== this.#receiptSource || stat.size < this.#receiptOffset) {
        this.#receiptSource = source;
        this.#receiptOffset = 0;
        this.#persistedIds.clear();
      }
      const buffer = Buffer.allocUnsafe(1 << 20);
      let position = this.#receiptOffset;
      let lineStart = position;
      let carry: Buffer[] = [];
      while (position < stat.size) {
        const count = fs.readSync(fd, buffer, 0, Math.min(buffer.length, stat.size - position), position);
        if (count <= 0) throw new Error("Session receipt shortened while reading");
        const view = buffer.subarray(0, count);
        let start = 0;
        for (let newline = view.indexOf(10); newline !== -1; newline = view.indexOf(10, start)) {
          const line = Buffer.concat([...carry, view.subarray(start, newline)]).toString("utf8");
          carry = [];
          if (lineStart === 0) {
            const header = JSON.parse(line) as { type?: string; id?: string };
            if (header.type !== "session" || header.id !== sessionId) throw new Error("Session receipt identity mismatch");
          } else if (line.includes(AGENT_COMPLETION_MESSAGE_TYPE)) {
            const entry = JSON.parse(line) as { type?: string; customType?: string; details?: { ids?: unknown } };
            if (entry.type === "custom_message" && entry.customType === AGENT_COMPLETION_MESSAGE_TYPE && Array.isArray(entry.details?.ids)) {
              for (const id of entry.details.ids) if (typeof id === "string") this.#persistedIds.add(id);
            }
          }
          start = newline + 1;
          lineStart = position + start;
          this.#receiptOffset = lineStart;
        }
        position += count;
        if (start < count) carry.push(Buffer.from(view.subarray(start)));
      }
      return this.#persistedIds;
    } finally { fs.closeSync(fd); }
  }

  #confirmDelivery(delivered: (() => void) | undefined): boolean {
    if (!delivered) return true;
    try { delivered(); this.#receiptRetries.delete(delivered); return true; } catch {
      this.#receiptRetries.add(delivered);
      this.#schedule();
      return false;
    }
  }

  #schedule(): void {
    if (this.#closed || this.#timer || (!this.#receiptRetries.size && (this.#suspended || !this.#pending.size))) return;
    // Never enqueue into Pi during an active tool batch. turn_end owns that path.
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      for (const receipt of this.#receiptRetries) this.#confirmDelivery(receipt);
      if (this.#context.isIdle() && !this.#context.hasPendingMessages()) this.#flush();
      if (this.#receiptRetries.size) this.#schedule();
    }, IDLE_BATCH_MS);
    this.#timer.unref?.();
  }

  #flush(deliver: (message: CompletionMessage, result: Completion) => void = (message, result) =>
    sendFabricMessage(this.pi, message, { deliverAs: "steer", triggerTurn: true },
      { id: result.id, name: result.name, kind: "agent" }, "steer", "mesh")): void {
    if (this.#closed || this.#suspended || this.#context.signal?.aborted || !this.#pending.size) return;
    const batch = [...this.#pending.values()].slice(0, 32);
    const perResult = Math.max(0, Math.min(SUMMARY_CHARS, Math.floor(BATCH_CHARS / batch.length) - 320));
    // Preparation is non-consuming: a later failure leaves every outcome unread.
    try {
      for (const { prepare } of batch) prepare?.();
    } catch { this.#schedule(); return; }
    // One turn has one sender: attribute each child separately on capable Pi.
    // Claim only the group about to be sent; unclaimed Main receipts require a durable carrier.
    const groups = fabricProvenanceSupported(this.pi) ? batch.map(item => [item]) : [batch];
    try {
      for (const group of groups) {
        const content = [
          "Unread background agent results (batched). Incorporate relevant results into the current task. These are run outcomes, not new user requests. Do not restart completed work or reply merely to acknowledge stale/superseded results. A completed run does not necessarily mean its assignment is complete.",
          ...group.map(({ result }) => {
            const seconds = Math.round(Math.max(0, (result.finishedAt ?? Date.now()) - result.startedAt) / 1_000);
            const summary = [result.error, result.text].filter(Boolean).join("\n");
            const redelivery = result.completionDelivery?.redeliveredFrom;
            const provenance = redelivery ? ` [re-delivered from dead Main session ${oneLine(redelivery)}]` : "";
            return `Agent ${oneLine(result.name).slice(0, 80)} (${result.id}) ${result.status} after ${seconds}s${provenance}:\n${clip(summary || "no result", perResult)}`;
          }),
        ].join("\n\n");
        try {
          this.commitBatch?.(group.map(({ result }) => result.id));
        } catch (error) {
          if (error instanceof ChildCompletionClaimLostError) {
            // Another owner won. Suppress only losing notices, never its archive/receipt.
            for (const id of error.ids) { this.#pending.delete(id); this.#acknowledged.add(id); }
          }
          this.#schedule(); return;
        }
        const claimed = !!this.commitBatch || group.some(({ prepare }) => !!prepare);
        let sent = false;
        try {
          deliver({
            customType: AGENT_COMPLETION_MESSAGE_TYPE,
            content,
            display: false,
            details: { ids: group.map(({ result }) => result.id) },
          }, group[0]!.result);
          sent = true;
        } finally {
          // A claimed actor send may fail after Pi accepted it: choose at-most-once.
          // Unclaimed Main completions retain retry-on-send-failure and durable receipt barriers.
          if (claimed || sent) for (const { result, delivered } of group) {
            this.#pending.delete(result.id);
            this.#acknowledged.add(result.id);
            if (claimed) this.#confirmDelivery(delivered);
            else this.#handed.set(result.id, delivered);
          }
        }
      }
    } finally {
      this.#confirmHeld();
    }
  }
}
