/** An ordinary bash exit, classified at the execute boundary before middleware. */
class PiBashExitError extends Error {
  constructor(message: string, readonly exitCode: number, readonly output: string) {
    super(message);
  }
}

// Pi currently exposes process status in its native exception text. Parse that
// contract only at tool execution, never from a preflight/approval error or a
// decorated tool_result. Keep the guest independent of display formatting.
export function classifyPiBashError(error: unknown): unknown {
  if (!(error instanceof Error)) return error;
  const match = /(?:^|\n\n)Command exited with code (\d+)$/.exec(error.message);
  if (!match) return error;
  const exitCode = Number(match[1]);
  if (!Number.isSafeInteger(exitCode) || exitCode <= 0) return error;
  return new PiBashExitError(error.message, exitCode, error.message.slice(0, match.index));
}

// Pi 0.99 returns ordinary nonzero exits instead of throwing. Classify only
// at the execute boundary, before middleware can redact or replace the result.
export function classifyPiBashResult(result: {
  content: ReadonlyArray<{ type: string; text?: string }>;
  isError?: boolean;
  structuredContent?: unknown;
}): unknown {
  const text = result.content.filter((part) => part.type === "text").map((part) => part.text ?? "").join("\n");
  const structured = result.structuredContent;
  const code = typeof structured === "object" && structured !== null && "exit_code" in structured
    ? structured.exit_code : undefined;
  if (!result.isError || typeof code !== "number" || !Number.isSafeInteger(code) || code <= 0) return new Error(text || "Pi bash failed");
  const suffix = `\n\nCommand exited with code ${code}`;
  const output = text.endsWith(suffix) ? text.slice(0, -suffix.length)
    : text === `Command exited with code ${code}` ? "" : text;
  return new PiBashExitError(text, code, output);
}

// Display cleanup only: this never decides whether an error is a native exit.
// Work exclusively with the final text so redacted/cleared content stays gone.
function bashResultOutput(original: PiBashExitError, text: string): string {
  const index = text.indexOf(original.message);
  if (index >= 0) {
    // The unchanged native message locates its status suffix unambiguously,
    // even when stdout or annotations contain identical status-looking lines.
    return text.slice(0, index + original.output.length) + text.slice(index + original.message.length);
  }

  // Middleware may trim, redact, split content blocks, or normalize newlines.
  // Remove a remaining native status line only when there is one candidate.
  // With no marker or multiple candidates, return the processed text verbatim.
  const marker = new RegExp(`(?:^|\\r?\\n\\r?\\n)Command exited with code ${original.exitCode}(?=\\r?\\n|$)`, "g");
  const match = marker.exec(text);
  if (!match || marker.exec(text)) return text;
  return text.slice(0, match.index) + text.slice(match.index + match[0].length);
}

/** Keep native exit status independent of middleware display transformations. */
export function piBashResultError(original: unknown, text: string): Error {
  if (original instanceof PiBashExitError) {
    // Pi returns only the effective isError, not the origin of a middleware
    // veto. Text replacement alone cannot distinguish redaction from a veto;
    // explicit middleware failure provenance is a separate contract concern.
    return new PiBashExitError(text, original.exitCode, bashResultOutput(original, text));
  }
  return new Error(text.trim() || "Pi bash failed");
}

/** Only provider-classified exits may cross a runtime bridge as settle metadata. */
export function piBashExitMetadata(error: unknown): { exitCode: number; output: string } | undefined {
  return error instanceof PiBashExitError
    ? { exitCode: error.exitCode, output: error.output }
    : undefined;
}
