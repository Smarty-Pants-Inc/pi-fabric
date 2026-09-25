import { Value } from "typebox/value";

const extractBalancedJson = (text: string, start: number): string | null => {
  const open = text[start];
  if (open !== "{" && open !== "[") return null;
  const close = open === "{" ? "}" : "]";
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === open) depth++;
    else if (ch === close) {
      depth--;
      if (depth === 0) return text.slice(start, i + 1);
    }
  }
  return null;
};

/** Lenient, for runner machine output that may carry log lines (Veda stdout). */
export const parseStructuredValue = (text: string): unknown => {
  const trimmed = text.trim();
  try {
    return JSON.parse(trimmed);
  } catch {
    // Whole text is not JSON; try extraction below.
  }
  const fenced = trimmed.match(/```(?:json)?\s*\n([\s\S]*?)\n```/i);
  if (fenced?.[1]) {
    try {
      return JSON.parse(fenced[1].trim());
    } catch {
      // Fenced block is not JSON; try balanced extraction below.
    }
  }
  const start = trimmed.search(/[{\[]/);
  if (start >= 0) {
    const balanced = extractBalancedJson(trimmed, start);
    if (balanced) return JSON.parse(balanced);
  }
  return JSON.parse(trimmed);
};

// The structured-result contract: the reply is one JSON value, or it has exactly one code fence
// and the fence holds the value. Unfenced text around a value is not searched: the search took
// incidental braces or a bracketed name from prose, and an object picked out of prose would
// pass a contract violation as a valid result (smarty-dev#576).
export const parseStructuredResult = (text: string): unknown => {
  const trimmed = text.trim();
  let reason: string;
  try {
    return JSON.parse(trimmed);
  } catch (error) {
    reason = error instanceof Error ? error.message : String(error);
  }
  // Every code fence counts, whatever its language tag; the one fence must be json or untagged.
  const fences = [...trimmed.matchAll(/```([^\n`]*)\n([\s\S]*?)\n```/g)];
  const tag = fences[0]?.[1]?.trim() ?? "";
  if (fences.length > 1) {
    reason = `found ${fences.length} code fences`;
  } else if (fences.length === 1 && tag !== "" && tag.toLowerCase() !== "json") {
    reason = `the code fence is tagged ${tag}, not json`;
  } else if (fences.length === 1) {
    try {
      return JSON.parse(fences[0]![2]!);
    } catch (error) {
      reason = `in the code fence: ${error instanceof Error ? error.message : String(error)}`;
    }
  }
  throw new Error(`the reply must be one JSON value, or have one code fence around it (${reason})`);
};

export function validateAgentResult<T extends { status: string; text: string; value?: unknown; error?: string }>(record: T, schema?: Record<string, unknown>): T {
  if (record.status !== "completed" || !schema) return record;
  try {
    const value = record.value ?? parseStructuredResult(record.text);
    if (!Value.Check(schema, value)) {
      const errors = [...Value.Errors(schema, value)].slice(0, 5).map((error) => error.message).join("; ");
      throw new Error(errors || "value does not match schema");
    }
    record.value = value;
  } catch (error) {
    record.status = "failed";
    const reason = error instanceof Error ? error.message : String(error);
    const output = record.text.trim();
    const snippet = output.slice(0, 200);
    record.error = `Structured agent output was invalid: ${reason}${snippet ? ` (output: ${snippet}${output.length > 200 ? "…" : ""})` : ""}`;
  }
  return record;
}
