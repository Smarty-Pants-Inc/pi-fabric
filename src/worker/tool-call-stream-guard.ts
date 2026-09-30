export const TOOL_CALL_WHITESPACE_TIMEOUT_MS = 60_000;
export const TOOL_CALL_WHITESPACE_MAX_BYTES = 64 * 1024;

export class RunawayToolCallStreamError extends Error {
  readonly code = "RUNAWAY_TOOL_CALL_STREAM" as const;
  constructor(
    readonly elapsedMs: number,
    readonly bytes: number,
    readonly contentIndex: number,
    readonly model: string,
    readonly effort: string,
  ) {
    super(`runaway: whitespace-only tool-call stream for ${Number((elapsedMs / 1000).toFixed(3))}s / ${bytes} bytes (${model}, ${effort})`);
    this.name = "RunawayToolCallStreamError";
  }
}

type EventRecord = Record<string, unknown>;
const isRecord = (value: unknown): value is EventRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

interface CallStream {
  meaningful: boolean;
  bytes: number;
  startedAt?: number;
  timer?: ReturnType<typeof setTimeout>;
}

/** Only an all-whitespace argument prefix is bounded, never legitimate JSON or tool execution. */
export class ToolCallStreamGuard {
  #calls = new Map<number, CallStream>();
  #disposed = false;
  #incomplete = false;

  constructor(
    private readonly fail: (error: RunawayToolCallStreamError) => void,
    private readonly attribution: () => { model: string; effort: string },
  ) {}

  observe(event: EventRecord): void {
    if (this.#disposed) return;
    if (event.type === "agent_start" ||
        ((event.type === "message_start" || event.type === "message_end") &&
         isRecord(event.message) && event.message.role === "assistant")) {
      this.clear();
      this.#incomplete = false;
      return;
    }
    if (this.#incomplete || event.type !== "message_update" || !isRecord(event.assistantMessageEvent)) return;
    const delta = event.assistantMessageEvent;
    if (typeof delta.contentIndex !== "number") return;
    const index = delta.contentIndex;
    if (delta.type === "toolcall_end") {
      this.#remove(index);
      return;
    }
    if (delta.type !== "toolcall_start" && delta.type !== "toolcall_delta") return;
    if (delta.type === "toolcall_start") this.#remove(index);
    let call = this.#calls.get(index);
    if (!call) {
      call = { meaningful: false, bytes: 0 };
      this.#calls.set(index, call);
      // Providers may supply initial argument text at start, before any delta.
      // Empty parsed {} is a scaffold, not evidence of meaningful argument text.
      const content = isRecord(delta.partial) ? delta.partial.content : undefined;
      const block: unknown = Array.isArray(content) ? content[index] : undefined;
      if (isRecord(block)) {
        if (isRecord(block.arguments) && Object.keys(block.arguments).length > 0) {
          call.meaningful = true;
        } else if (delta.type === "toolcall_start" && typeof block.partialJson === "string" && block.partialJson.length > 0) {
          this.#append(index, call, block.partialJson);
        } else if (typeof block.partialJson === "string" && /\S/.test(block.partialJson)) {
          call.meaningful = true;
        }
      }
    }
    if (delta.type === "toolcall_delta" && typeof delta.delta === "string") {
      this.#append(index, call, delta.delta);
    }
  }

  #append(index: number, call: CallStream, text: string): void {
    if (this.#disposed || call.meaningful) return;
    if (/\S/.test(text)) {
      // Permanently exempt this call: later whitespace cannot erase the JSON prefix.
      call.meaningful = true;
      if (call.timer) clearTimeout(call.timer);
      delete call.timer;
      return;
    }
    call.bytes += Buffer.byteLength(text, "utf8");
    if (call.startedAt === undefined) {
      call.startedAt = Date.now();
      call.timer = setTimeout(() => this.#abort(index, call), TOOL_CALL_WHITESPACE_TIMEOUT_MS);
      call.timer.unref?.();
    }
    if (call.bytes >= TOOL_CALL_WHITESPACE_MAX_BYTES ||
        Date.now() - call.startedAt >= TOOL_CALL_WHITESPACE_TIMEOUT_MS) this.#abort(index, call);
  }

  #abort(index: number, call: CallStream): void {
    if (this.#disposed) return;
    const { model, effort } = this.attribution();
    const error = new RunawayToolCallStreamError(
      Date.now() - (call.startedAt ?? Date.now()), call.bytes, index, model ?? "unknown", effort ?? "unknown",
    );
    this.dispose();
    this.fail(error);
  }

  #remove(index: number): void {
    const call = this.#calls.get(index);
    if (call?.timer) clearTimeout(call.timer);
    this.#calls.delete(index);
  }

  /** A dropped RPC frame may contain meaningful arguments: do not infer whitespace from an incomplete message. */
  discardedEvent(): void {
    this.clear();
    this.#incomplete = true;
  }

  clear(): void {
    for (const index of this.#calls.keys()) this.#remove(index);
  }

  dispose(): void {
    this.clear();
    this.#disposed = true;
  }
}
