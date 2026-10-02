import { assistantStreamEvent } from "./assistant-stream-event.js";

export const PI_RECOVERY_TIMEOUT_MS = 60_000;
export const PI_RECOVERY_MAX_MS = 10 * 60_000;
export const PI_PROVIDER_RESUME_DELAYS_MS = [30_000, 60_000, 120_000] as const;

/** Test-only acceleration of the real process path; production defaults stay fixed. */
export const recoveryTimeScale = (): number => {
  const scale = Number(process.env.PI_FABRIC_TEST_RECOVERY_TIME_SCALE ?? 1);
  return Number.isFinite(scale) && scale > 0 && scale <= 1 ? scale : 1;
};

/** Bounds stalled Pi error recovery, not healthy inference or tool execution. */
export class PiRecoveryWatchdog {
  #timer: ReturnType<typeof setTimeout> | undefined;
  #reason: string | undefined;
  #disposed = false;
  #incidentStartedAt: number | undefined;
  #incidentReason: string | undefined;
  #deadline = 0;
  readonly #timeoutMs: number;
  readonly #maxMs: number;
  readonly #fail: (error: string) => void;

  constructor(fail: (error: string) => void, scale = 1) {
    this.#fail = fail;
    this.#timeoutMs = PI_RECOVERY_TIMEOUT_MS * scale;
    this.#maxMs = PI_RECOVERY_MAX_MS * scale;
  }

  get remainingMs(): number {
    return this.#incidentStartedAt === undefined ? this.#maxMs :
      Math.max(0, this.#incidentStartedAt + this.#maxMs - Date.now());
  }

  arm(reason: string): void {
    if (this.#disposed) return;
    const alreadyStalled = this.#reason !== undefined;
    this.#reason = reason;
    this.#incidentReason = reason;
    this.#incidentStartedAt ??= Date.now();
    // Repeated errors and lifecycle chatter are not progress. A suspended
    // no-progress timer still enforces the absolute incident bound.
    if (this.#timer && alreadyStalled) return;
    this.#schedule(Date.now() + this.#timeoutMs);
  }

  #schedule(deadline: number): void {
    if (this.#timer) clearTimeout(this.#timer);
    const bound = this.#incidentStartedAt! + this.#maxMs;
    this.#deadline = Math.min(deadline, bound);
    this.#timer = setTimeout(() => {
      const error = this.#reason ?? this.#incidentReason;
      const bounded = this.#deadline === bound;
      this.dispose();
      this.#fail(bounded
        ? `${error}; Pi provider recovery exceeded the 10-minute bound (${PI_RECOVERY_MAX_MS}ms); session retained; terminating child`
        : `${error}; no model stream or tool event for ${PI_RECOVERY_TIMEOUT_MS}ms; terminating child`);
    }, Math.max(0, this.#deadline - Date.now()));
    this.#timer.unref?.();
  }

  /** Acceptance ends the no-progress timer; successful completion ends the incident. */
  observe(event: Record<string, unknown>): void {
    if (this.#disposed) return;
    if (event.type === "auto_retry_start" && typeof event.delayMs === "number" &&
        Number.isFinite(event.delayMs) && event.delayMs >= 0) {
      this.arm(typeof event.errorMessage === "string" ? event.errorMessage : this.#reason ?? "Pi retry stalled");
      this.#schedule(Math.max(this.#deadline, Date.now() + event.delayMs + this.#timeoutMs));
      return;
    }
    if (!this.#reason) return;
    if (["tool_execution_start", "tool_execution_update", "tool_execution_end"].includes(String(event.type))) {
      this.progress();
      return;
    }
    if (event.type !== "message_start" && event.type !== "message_update") return;
    const isRecord = (value: unknown): value is Record<string, unknown> =>
      typeof value === "object" && value !== null && !Array.isArray(value);
    const message = event.message;
    if (isRecord(message) && (message.stopReason === "error" || message.stopReason === "aborted")) return;
    if (event.type === "message_start" && isRecord(message) && message.role === "assistant") {
      // Pi emits this when the provider starts the retried response, not when
      // retry scheduling begins. Silent reasoning after acceptance is healthy
      // inference, governed by overall/idle deadlines and the incident cap.
      this.suspend();
      return;
    }
    if (event.type === "message_update") {
      const stream = assistantStreamEvent(event);
      if (stream) {
        if (["text_delta", "thinking_delta", "toolcall_delta"].includes(String(stream.type))) {
          // Whitespace is not recovery progress. Tool argument whitespace is
          // still counted by ToolCallStreamGuard toward its time/byte bounds.
          if (typeof stream.delta === "string" && /\S/.test(stream.delta)) this.progress();
        } else if (["text_start", "text_end", "thinking_start", "thinking_end", "toolcall_start", "toolcall_end"].includes(String(stream.type))) {
          this.progress();
        }
        // Never let an error or empty delta reuse an old output snapshot as progress.
        return;
      }
    }
    // Some adapters publish assistant output snapshots without a stream envelope.
    if (!isRecord(message) || message.role !== "assistant") return;
    const content = message.content;
    if ((typeof content === "string" && content.length > 0) ||
        (Array.isArray(content) && content.some((block) => isRecord(block) && (
          (block.type === "text" && typeof block.text === "string" && block.text.length > 0) ||
          (block.type === "thinking" && typeof block.thinking === "string" && block.thinking.length > 0) ||
          block.type === "toolCall"
        )))) this.progress();
  }

  progress(): void {
    if (!this.#reason || this.#disposed) return;
    const reason = this.#reason;
    this.suspend();
    this.arm(reason);
  }

  /** Suspend only the no-progress deadline; recovery still has an absolute cap. */
  suspend(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#reason = undefined;
    if (!this.#disposed && this.#incidentStartedAt !== undefined) {
      this.#schedule(this.#incidentStartedAt + this.#maxMs);
    }
  }

  clear(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#reason = undefined;
    this.#incidentStartedAt = undefined;
    this.#incidentReason = undefined;
  }

  dispose(): void {
    this.clear();
    this.#disposed = true;
  }
}
