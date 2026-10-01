export const PI_RECOVERY_TIMEOUT_MS = 60_000;

/** Bounds stalled Pi error recovery, not healthy inference or tool execution. */
export class PiRecoveryWatchdog {
  #timer: ReturnType<typeof setTimeout> | undefined;
  #reason: string | undefined;
  #disposed = false;

  constructor(private readonly fail: (error: string) => void) {}

  arm(reason: string): void {
    if (this.#disposed) return;
    this.#reason = reason;
    // Repeated errors, retry announcements and lifecycle chatter are not progress.
    if (this.#timer) return;
    this.#timer = setTimeout(() => {
      const error = this.#reason;
      this.dispose();
      this.fail(`${error}; no model stream or tool event for ${PI_RECOVERY_TIMEOUT_MS}ms; terminating child`);
    }, PI_RECOVERY_TIMEOUT_MS);
    this.#timer.unref?.();
  }

  /** Model output and tool activity refresh recovery; errors and retry chatter do not. */
  observe(event: Record<string, unknown>): void {
    if (!this.#reason || this.#disposed) return;
    if (["tool_execution_start", "tool_execution_update", "tool_execution_end"].includes(String(event.type))) {
      this.progress();
      return;
    }
    if (event.type !== "message_start" && event.type !== "message_update") return;
    const isRecord = (value: unknown): value is Record<string, unknown> =>
      typeof value === "object" && value !== null && !Array.isArray(value);
    const message = event.message;
    if (isRecord(message) && (message.stopReason === "error" || message.stopReason === "aborted")) return;
    if (event.type === "message_update") {
      // Legacy Pi RPC uses assistantMessageEvent; native harness streams use event.
      const stream = event.assistantMessageEvent ?? event.event;
      if (isRecord(stream)) {
        if (["text_delta", "thinking_delta", "toolcall_delta"].includes(String(stream.type))) {
          if (typeof stream.delta === "string" && stream.delta.length > 0) this.progress();
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
    this.clear();
    this.arm(reason);
  }

  clear(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#reason = undefined;
  }

  dispose(): void {
    this.clear();
    this.#disposed = true;
  }
}
