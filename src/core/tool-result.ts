import type { AgentToolResult, ExtensionRunner } from "@earendil-works/pi-coding-agent";
import type { JsonValue } from "@earendil-works/pi-ai";
import { runAbortable } from "../async-settlement.js";
import type { FabricInvocationContext } from "../protocol.js";

// Shape admission, not another secret redactor. Redaction belongs exclusively
// to the same tool_result middleware that transforms model-facing content.
// Do not serialize opaque values: getters/toJSON can reveal data only later,
// after the middleware has finished. Bound traversal and reject the whole field.
const jsonPayload = (value: unknown): JsonValue | undefined => {
  const visiting = new WeakSet<object>();
  let remaining = 100_000;
  const copy = (nested: unknown, depth: number): JsonValue => {
    if (--remaining < 0 || depth > 64) throw new Error("unbounded payload");
    if (nested === null || typeof nested === "string" || typeof nested === "boolean") return nested;
    if (typeof nested === "number" && Number.isFinite(nested)) return nested;
    if (typeof nested !== "object" || nested === null || visiting.has(nested)) throw new Error("opaque payload");
    const array = Array.isArray(nested);
    const prototype = Object.getPrototypeOf(nested);
    if (prototype !== (array ? Array.prototype : Object.prototype) && !(prototype === null && !array)) throw new Error("opaque prototype");
    visiting.add(nested);
    const descriptors = Object.getOwnPropertyDescriptors(nested);
    if (Object.getOwnPropertySymbols(nested).length) throw new Error("symbol payload");
    const result: JsonValue[] | { [key: string]: JsonValue } = array ? [] : {};
    const keys = Object.keys(descriptors).filter(key => !(array && key === "length"));
    if (array && keys.length !== (nested as unknown[]).length) throw new Error("sparse payload");
    for (const [index, key] of keys.entries()) {
      const descriptor = descriptors[key]!;
      if (!descriptor.enumerable || !("value" in descriptor) || (array && key !== String(index))) throw new Error("opaque property");
      Object.defineProperty(result, key, { value: copy(descriptor.value, depth + 1), enumerable: true, writable: true, configurable: true });
    }
    visiting.delete(nested);
    return result;
  };
  try { return copy(value, 0); } catch { return undefined; }
};

/** Shared nested-provider choke point: finish middleware before any result fan-out. */
export async function emitNestedToolResult(
  runner: ExtensionRunner,
  event: { toolName: string; toolCallId: string; input: Record<string, unknown> },
  original: Omit<AgentToolResult<unknown>, "structuredContent"> & { structuredContent?: unknown },
  isError: boolean,
  context: Pick<FabricInvocationContext, "signal" | "attachPreview">,
  declaresStructuredOutput = false,
): Promise<AgentToolResult<unknown> & { isError: boolean }> {
  const structured = declaresStructuredOutput || original.structuredContent !== undefined;
  const structuredContent = jsonPayload(original.structuredContent);
  const details = structured ? jsonPayload(original.details) : original.details;
  const patch = await runAbortable(context.signal, () => runner.emitToolResult({
    type: "tool_result", ...event, content: original.content,
    ...(structuredContent !== undefined ? { structuredContent } : {}),
    // Pi invalidates structuredContent when content is replaced, but *not*
    // details. Quarantine that second structured channel from post-hook
    // observers; keep it only on an unchanged result or an explicit replacement.
    // Legacy tools without structured output retain their 0.87 details contract.
    details: structured ? undefined : details,
    isError,
    ...(original.usage !== undefined ? { usage: original.usage } : {}),
  }));
  const result = { ...original, details, isError: patch?.isError ?? isError };
  delete result.structuredContent;
  if (patch?.content !== undefined) result.content = patch.content;
  if (structured) result.details = patch ? jsonPayload(patch.details) : details;
  else if (patch?.details !== undefined) result.details = patch.details;
  const effectiveStructured = patch?.structuredContent !== undefined
    ? jsonPayload(patch.structuredContent)
    : patch?.content !== undefined ? undefined : structuredContent;
  if (effectiveStructured !== undefined) result.structuredContent = effectiveStructured;
  if (patch?.usage !== undefined) result.usage = patch.usage;
  // A failed shell skips its normal successful-result preview. Replace any
  // retained pre-hook progress with the effective text before outer Fabric
  // envelopes, transcript entries, or actors can observe the audit preview.
  // Preserve the envelope and other preview metadata; never restore raw output.
  if (result.isError) context.attachPreview?.({
    result: result.content.filter(part => part.type === "text").map(part => part.text).join("\n"),
  });
  return result as AgentToolResult<unknown> & { isError: boolean };
}
