import { Value } from "typebox/value";
import { isJevModelId } from "./routes.js";
import type { JevJson, JevRequest, JevResponse, JevAnswer } from "./types.js";

export function jsonText(value: unknown, maxBytes: number, label: string): string {
  let nodes = 0;
  const visit = (v: unknown, depth: number): void => {
    if (++nodes > 100_000 || depth > 32) throw new Error(`${label} exceeds JSON complexity limits`);
    if (v === null || typeof v === "string" || typeof v === "boolean") return;
    if (typeof v === "number" && Number.isFinite(v)) return;
    if (Array.isArray(v)) { for (const item of v) visit(item, depth + 1); return; }
    if (typeof v === "object" && v !== null && Object.getPrototypeOf(v) === Object.prototype) {
      for (const item of Object.values(v)) visit(item, depth + 1);
      return;
    }
    throw new Error(`${label} must contain only finite JSON values`);
  };
  visit(value, 0);
  const text = JSON.stringify(value);
  if (Buffer.byteLength(text) > maxBytes) throw new Error(`${label} exceeds ${maxBytes} bytes`);
  return text;
}
export const object = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);
const description = (v: unknown): boolean => typeof v === "string" || object(v) || Array.isArray(v);
const keysOnly = (v: Record<string, unknown>, keys: string[]) => Object.keys(v).every(k => keys.includes(k));

export function checkRequest(value: unknown, maxBytes: number): asserts value is JevRequest {
  jsonText(value, maxBytes, "Jev request");
  if (!object(value) || !keysOnly(value, ["state", "questions", "model"]) || !description(value.state) ||
      !object(value.questions) || Object.keys(value.questions).length < 1 || Object.keys(value.questions).length > 128 ||
      (value.model !== undefined && (typeof value.model !== "string" || !isJevModelId(value.model)))) {
    throw new Error("Invalid Jev request: provide state, 1–128 questions, and an optional model ID");
  }
  for (const [id, q] of Object.entries(value.questions)) {
    if (!id || id.length > 128 || !object(q) || !keysOnly(q, ["type", "instructions", "criteria"]) || !description(q.instructions))
      throw new Error("Invalid Jev question: each question needs a type and complete instructions");
    if (q.type === "choice") {
      if (!object(q.criteria) || Object.keys(q.criteria).length < 1 || Object.keys(q.criteria).length > 255 ||
          !Object.entries(q.criteria).every(([key, v]) => key.length > 0 && key.length <= 256 && (v === null || description(v))))
        throw new Error("Choice requires 1–255 named options with descriptions or null");
    } else if (q.type === "score") {
      if (!Array.isArray(q.criteria) || q.criteria.length < 2 || q.criteria.length > 10 || !q.criteria.every(description))
        throw new Error("Score requires 2–10 ordered descriptive levels");
    } else if (q.type === "noul") {
      if (q.criteria !== undefined && (!object(q.criteria) || !keysOnly(q.criteria, ["true", "false"]) || !Object.values(q.criteria).every(description)))
        throw new Error("Noul criteria may describe true and false only");
    } else throw new Error("Jev supports choice, score, and noul—not free-text generation");
  }
}
const probability = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v) && v >= 0 && v <= 1;
export function checkResponse(value: unknown, request: JevRequest): JevResponse {
  const invalid = (): never => { throw new Error("TypeSafe returned an invalid typed response"); };
  if (!object(value) || typeof value.model !== "string" || value.model.length > 128 || !object(value.answers) || !object(value.usage)) return invalid();
  const usage = value.usage;
  if (![usage.input_tokens, usage.output_tokens].every(v => Number.isSafeInteger(v) && (v as number) >= 0)) return invalid();
  if (Object.keys(value.answers).length !== Object.keys(request.questions).length) return invalid();
  const entries: Array<[string, JevAnswer]> = [];
  for (const [id, question] of Object.entries(request.questions)) {
    const a = Object.hasOwn(value.answers, id) ? value.answers[id] : undefined;
    if (!object(a) || a.type !== question.type) return invalid();
    if (question.type === "noul") {
      if (!probability(a.noul)) return invalid();
      entries.push([id, { type: "noul", noul: a.noul }]);
      continue;
    }
    const keys = question.type === "choice" ? Object.keys(question.criteria) : question.criteria.map((_, i) => String(i));
    const p = a.probabilities;
    if (!probability(a.confidence) || !object(p) || Object.keys(p).length !== keys.length ||
        !keys.every(k => Object.hasOwn(p, k) && probability(p[k])) ||
        Math.abs(Object.values(p).reduce<number>((sum, v) => sum + (v as number), 0) - 1) > 0.02) return invalid();
    const probabilities = Object.fromEntries(keys.map(k => [k, p[k] as number]));
    if (question.type === "choice") {
      if (typeof a.choice !== "string" || !Object.hasOwn(question.criteria, a.choice)) return invalid();
      entries.push([id, { type: "choice", choice: a.choice, confidence: a.confidence, probabilities }]);
    } else {
      if (typeof a.score !== "number" || !Number.isFinite(a.score) || a.score < 0 || a.score > keys.length - 1) return invalid();
      // Rubrics come from the request, not arbitrary server-produced content.
      const legend = Object.fromEntries(question.criteria.map((level, i) => [String(i), level]));
      entries.push([id, { type: "score", score: a.score, confidence: a.confidence, probabilities, legend }]);
    }
  }
  return { model: value.model, answers: Object.fromEntries(entries), usage: {
    input_tokens: usage.input_tokens as number, output_tokens: usage.output_tokens as number,
  } };
}

const schemaKeys = new Set(["type", "properties", "required", "additionalProperties", "items", "enum", "const", "anyOf", "oneOf", "allOf", "minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "description", "title"]);
export function checkSchema(schema: unknown): asserts schema is Record<string, unknown> {
  jsonText(schema, 16_384, "Program schema");
  let nodes = 0;
  const visit = (s: unknown, depth: number): void => {
    if (!object(s) || ++nodes > 128 || depth > 12 || Object.keys(s).some(k => !schemaKeys.has(k)))
      throw new Error("Unsupported program schema; use the documented bounded JSON Schema subset (no references or regexes)");
    if (s.type !== undefined && !["object", "array", "string", "number", "integer", "boolean", "null"].includes(s.type as string)) throw new Error("Invalid schema type");
    if (s.properties !== undefined) {
      if (!object(s.properties)) throw new Error("Schema properties must be an object");
      Object.values(s.properties).forEach(v => visit(v, depth + 1));
    }
    if (s.required !== undefined && (!Array.isArray(s.required) || !s.required.every(v => typeof v === "string"))) throw new Error("Schema required must be a string array");
    if (s.additionalProperties !== undefined && typeof s.additionalProperties !== "boolean") visit(s.additionalProperties, depth + 1);
    if (s.items !== undefined) visit(s.items, depth + 1);
    for (const k of ["anyOf", "oneOf", "allOf"]) if (s[k] !== undefined) {
      if (!Array.isArray(s[k]) || s[k].length < 1 || s[k].length > 16) throw new Error("Invalid schema alternatives");
      s[k].forEach(v => visit(v, depth + 1));
    }
    for (const k of ["minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties"])
      if (s[k] !== undefined && (typeof s[k] !== "number" || !Number.isFinite(s[k]) || (!['minimum', 'maximum'].includes(k) && (!Number.isSafeInteger(s[k]) || s[k] < 0)))) throw new Error("Invalid schema bound");
    if (s.enum !== undefined && (!Array.isArray(s.enum) || s.enum.length === 0 || s.enum.length > 256)) throw new Error("Invalid schema enum");
  };
  visit(schema, 0);
}
export function checkValue(schema: Record<string, unknown>, value: unknown, label: string): asserts value is JevJson {
  jsonText(value, 32_768, label);
  if (!Value.Check(schema, value)) throw new Error(`${label} does not match its declared schema`);
}
