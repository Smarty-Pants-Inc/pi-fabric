/**
 * The records service's wire protocol (C10): newline-delimited JSON over a unix socket.
 *
 *   request   {"id": 7, "method": "append", "token": "…", "args": {…}}
 *   response  {"id": 7, "ok": true, "result": …} | {"id": 7, "ok": false, "error": {message, code?, retryable?}}
 *   cancel    {"id": 8, "method": "cancel", "args": {"target": 7}}   (no response)
 *
 * Every method except `register` and `hello` needs a token; the service derives the caller's
 * principal from it, never from `args`. Closing the connection cancels its calls in flight.
 */
export const MAX_LINE_BYTES = 1024 * 1024;
/**
 * What one response's records may add up to (JSON bytes). A page, a replay or a history stops
 * near it and returns `next`, always with at least one record: one record's JSON is at most
 * ~450 KiB (64 KiB of text, escaped, plus 64 KiB of data), so a response always fits a line.
 */
export const RESPONSE_BUDGET_BYTES = 768 * 1024;

/** The longest prefix of `items` whose JSON fits the budget, never fewer than one item. */
export const withinBudget = <T>(items: readonly T[], budget = RESPONSE_BUDGET_BYTES, atLeastOne = true): T[] => {
  let used = 0;
  const kept: T[] = [];
  for (const item of items) {
    const size = Buffer.byteLength(JSON.stringify(item)) + 1;
    if ((kept.length > 0 || !atLeastOne) && used + size > budget) break;
    kept.push(item);
    used += size;
  }
  return kept;
};

export interface WireRequest { id: number; method: string; token?: string; args?: unknown }
export interface WireError { message: string; code?: string; retryable?: boolean }
export type WireResponse = { id: number; ok: true; result: unknown } | { id: number; ok: false; error: WireError };

/** An error the service sends as is; anything else becomes a generic message. */
export class RecordsServiceError extends Error {
  constructor(message: string, readonly code: string, readonly retryable = false) { super(message); }
}

export const wireError = (error: unknown): WireError => {
  const value = error as { message?: unknown; code?: unknown; retryable?: unknown } | undefined;
  const message = typeof value?.message === "string" ? value.message : String(error);
  return {
    message,
    ...(typeof value?.code === "string" ? { code: value.code } : {}),
    ...(value?.retryable === true ? { retryable: true } : {}),
  };
};

/** The error a client throws for a failed call: same message, code and retryability. */
export const errorFromWire = (error: WireError): Error & { code?: string; retryable?: boolean } =>
  Object.assign(new Error(error.message), { ...(error.code ? { code: error.code } : {}), ...(error.retryable ? { retryable: true } : {}) });

/** Split a byte stream into lines, refusing an overlong one. */
export class LineReader {
  #buffer = "";
  constructor(readonly onLine: (line: string) => void, readonly onOverflow: () => void) {}
  push(chunk: string): void {
    this.#buffer += chunk;
    let index: number;
    while ((index = this.#buffer.indexOf("\n")) >= 0) {
      const line = this.#buffer.slice(0, index);
      this.#buffer = this.#buffer.slice(index + 1);
      // The limit holds for every whole line, however the stream was split.
      if (Buffer.byteLength(line) > MAX_LINE_BYTES) {
        this.#buffer = "";
        this.onOverflow();
        return;
      }
      if (line.trim()) this.onLine(line);
    }
    if (Buffer.byteLength(this.#buffer) > MAX_LINE_BYTES) {
      this.#buffer = "";
      this.onOverflow();
    }
  }
}
