import type { FabricKernel } from "./runtime/kernel.js";
import { normalizeRunDisplay } from "./run-display.js";
import { repairFabricGuestCode } from "./runtime/guest-code-repair.js";

const OPTIONAL_FABRIC_EXEC_KEYS = [
  "payloads",
  "strings",
  "resultFormat",
  "tokenBudget",
  "agentBudget",
  "timeoutMs",
  "timeout_ms",
  "maxOutputTokens",
  "max_output_tokens",
  "display",
] as const;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const looksLikeJsonObject = (text: string): boolean =>
  text.startsWith("{") && text.endsWith("}");

const looksLikeJsonString = (text: string): boolean =>
  text.startsWith('"') && text.endsWith('"');

const parseJsonObject = (text: string): Record<string, unknown> | undefined => {
  const trimmed = text.trim();
  if (!looksLikeJsonObject(trimmed) && !looksLikeJsonString(trimmed)) return undefined;
  try {
    let parsed: unknown = JSON.parse(trimmed);
    // One extra unwrap: models sometimes JSON-encode the object twice.
    if (typeof parsed === "string") {
      const inner = parsed.trim();
      if (!looksLikeJsonObject(inner)) return undefined;
      parsed = JSON.parse(inner);
    }
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
};

const asStringRecord = (record: Record<string, unknown>): Record<string, string> | undefined => {
  if (Object.values(record).some((value) => typeof value !== "string")) return undefined;
  return record as Record<string, string>;
};

// smarty-dev#2340: payloads are literal strings. A value that is only a
// placeholder token (`__CONTENT__`) or a single file reference (`@/path`,
// `file:///path`) means the model expected the host to expand it; the program
// would silently run against the token text instead. Ordinary text that merely
// contains such a token, and the empty string, stay valid.
// `(?![\s\S])` pins the true end of the value: no trailing-newline leniency,
// independent of regex flags.
const PAYLOAD_PLACEHOLDER = /^__[A-Z_]+__$/;
const PAYLOAD_FILE_REFERENCE = /^(?:@\/|file:\/\/)\S*$/;

const PAYLOADS_LITERAL_ERROR =
  "payloads are literal: read the file with a native tool first and pass its content";

// Exact whole-value contract: no trimming, so padded or multi-line values are
// ordinary text and pass through unchanged.
const isPayloadPlaceholder = (value: string): boolean =>
  PAYLOAD_PLACEHOLDER.test(value) || PAYLOAD_FILE_REFERENCE.test(value);

const assertLiteralPayloads = (payloads: Record<string, string>): Record<string, string> => {
  for (const [key, value] of Object.entries(payloads)) {
    if (isPayloadPlaceholder(value)) {
      throw new Error(
        `fabric_exec payload ${JSON.stringify(key)} is only a placeholder or file reference (${JSON.stringify(value)}); ${PAYLOADS_LITERAL_ERROR}.`,
      );
    }
  }
  return payloads;
};

// Silent repair for the named-payload map. The declared shape is
// Record<string, string>, but models stringify nested maps (the highest-entropy
// escaped field in an otherwise flat tool), which strict schema validation
// rejects at the cost of a zero-work round trip. `strings` is a legacy alias:
// the name collides with the JSON string type and taught models to pass one.
// Placeholder-only values throw unless `validate` is false
// (render previews must never throw).
const normalizeFabricExecStrings = (
  input: unknown,
  validate = true,
): Record<string, string> | undefined => {
  let record: Record<string, string> | undefined;
  if (isRecord(input)) record = asStringRecord(input);
  else if (typeof input === "string") {
    const parsed = parseJsonObject(input);
    record = parsed ? asStringRecord(parsed) : undefined;
  }
  return record && validate ? assertLiteralPayloads(record) : record;
};

// Shared by prepareArguments and execute: every runtime (QuickJS, node process,
// Monty, CPython) receives payloads only through this resolution.
export const resolveFabricExecPayloads = (params: {
  payloads?: unknown;
  strings?: unknown;
}, options: { validate?: boolean } = {}): Record<string, string> | undefined =>
  normalizeFabricExecStrings(params.payloads, options.validate)
    ?? normalizeFabricExecStrings(params.strings, options.validate);

export const prepareFabricExecArguments = (input: unknown, kernel: FabricKernel = "typescript"): unknown => {
  if (typeof input === "string") return prepareFabricExecArguments({ code: input }, kernel);
  if (!isRecord(input)) return input;

  let prepared = input;
  const writable = (): Record<string, unknown> => {
    if (prepared === input) prepared = { ...input };
    return prepared;
  };

  if (Array.isArray(prepared.code) && prepared.code.every((line) => typeof line === "string")) {
    writable().code = prepared.code.join("\n");
  }
  if (kernel === "typescript" && typeof prepared.code === "string") {
    const repaired = repairFabricGuestCode(prepared.code);
    if (repaired !== prepared.code) writable().code = repaired;
  }

  if (kernel === "typescript" && typeof prepared.code === "string") {
    const match = /^\s*\/\/\s*@options:\s*([^\r\n]*)/.exec(prepared.code);
    if (match) {
      const options: unknown = JSON.parse(match[1]!);
      if (!isRecord(options)) throw new Error("@options must be a JSON object");
      for (const [key, value] of Object.entries(options)) {
        if (key !== "timeout_ms" && key !== "max_output_tokens") throw new Error(`Unsupported @options key: ${key}`);
        if (!Number.isSafeInteger(value) || (value as number) < 1) throw new Error(`${key} must be a positive integer`);
        if (prepared[key] !== undefined && prepared[key] !== value) throw new Error(`Conflicting ${key} options`);
        writable()[key] = value;
      }
    }
  }
  if (prepared.max_output_tokens != null) {
    if (prepared.maxOutputTokens != null && prepared.maxOutputTokens !== prepared.max_output_tokens) throw new Error("Conflicting maxOutputTokens and max_output_tokens");
    writable().maxOutputTokens = prepared.max_output_tokens;
    delete writable().max_output_tokens;
  }

  for (const key of OPTIONAL_FABRIC_EXEC_KEYS) {
    if (!Object.hasOwn(prepared, key)) continue;
    if (prepared[key] === null || prepared[key] === undefined) delete writable()[key];
  }

  const display = prepared.display;
  if (typeof display === "string" || isRecord(display)) {
    const normalized = normalizeRunDisplay(display);
    if (normalized) writable().display = normalized;
    else delete writable().display;
  }

  const hasPayloads = Object.hasOwn(prepared, "payloads");
  const hasStrings = Object.hasOwn(prepared, "strings");
  if (hasPayloads || hasStrings) {
    const raw = hasPayloads ? prepared.payloads : prepared.strings;
    const normalized = normalizeFabricExecStrings(raw);
    if (normalized) {
      if (prepared.payloads !== normalized) writable().payloads = normalized;
    } else if (!hasPayloads) {
      writable().payloads = raw;
    }
    if (hasStrings) delete writable().strings;
  }

  return prepared;
};
