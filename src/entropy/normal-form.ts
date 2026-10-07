// Internal compatibility plans. The declared schema is never rewritten.
// A proof is a re-derivation from a bounded rule language, not a claim made
// by an imported artifact. No observation can authorize a new semantic map.
import { Value } from "typebox/value";
import { useNormalized } from "../verified/policy.js";
import { stableJsonHash } from "../core/stable-hash.js";
import { shapeSignature } from "./fingerprint.js";

export interface NormalFormEvidenceSummary {
  normalizedCalls: number;
  rulesApplied: number;
  succeeded: number;
  subsequentFailures: number;
}

// Witness counts are observations, not manufactured task successes. A call
// can be normalized and still fail authorization, approval, or execution.
export const normalFormEvidenceSummary = (
  traces: readonly import("./types.js").EntropyTraceInput[],
): NormalFormEvidenceSummary => {
  const summary = { normalizedCalls: 0, rulesApplied: 0, succeeded: 0, subsequentFailures: 0 };
  for (const trace of traces) for (const operation of trace.operations) {
    if (!operation.normalization || !isNormalFormWitness(operation.normalization)) continue;
    summary.normalizedCalls++;
    summary.rulesApplied += operation.normalization.rules.length;
    if (operation.outcome === "succeeded") summary.succeeded++;
    else summary.subsequentFailures++;
  }
  return summary;
};

import {
  NORMAL_FORM_VERSION, MAX_NORMAL_FORM_RULES,
  isNormalFormPlan, isNormalFormWitness,
  normalFormRecord as isRecord, normalFormSafeKey as safeKey,
  type NormalFormPlan, type NormalFormRule, type NormalFormWitness,
} from "./normal-form-witness.js";
export {
  NORMAL_FORM_VERSION, MAX_NORMAL_FORM_RULES, MAX_NORMAL_FORM_PLANS,
  isNormalFormPlan, isNormalFormWitness,
  type NormalFormRuleKind, type NormalFormRule, type NormalFormPlan, type NormalFormWitness,
} from "./normal-form-witness.js";
export interface NormalFormResult {
  args: Record<string, unknown>;
  witness?: NormalFormWitness;
}

const form = (value: string): string | undefined =>
  /^[a-zA-Z][a-zA-Z0-9 _-]*$/.test(value) ? value.toLowerCase().replace(/[ _-]/g, "") : undefined;
const accepts = (schema: unknown, args: unknown): boolean => {
  try { return isRecord(schema) && Value.Check(schema, args); } catch { return false; }
};
const uniqueForms = (values: readonly string[]): Map<string, string> => {
  const groups = new Map<string, string[]>();
  for (const value of values) {
    const key = form(value);
    if (!key) continue;
    const group = groups.get(key) ?? [];
    group.push(value);
    groups.set(key, group);
  }
  return new Map([...groups].filter(([, group]) => group.length === 1).map(([key, group]) => [key, group[0]!]));
};

// These four conventions are host-authored compatibility semantics:
// spelling-only key/enum forms, lossless numeric encoding, and omission of
// non-nullable optional fields. Open objects and schema combinators are not
// grounds for inventing an interpretation. Unsupported schemas pass through.
export const deriveNormalFormPlan = (ref: string, schema: unknown): NormalFormPlan | undefined => {
  if (!ref || ref.length > 1_024 || !isRecord(schema) || schema.type !== "object" ||
      schema.additionalProperties !== false || !isRecord(schema.properties) ||
      ["patternProperties", "oneOf", "anyOf", "allOf", "$ref", "if", "not"].some((key) => Object.hasOwn(schema, key))) return undefined;
  const keys = Object.keys(schema.properties).sort();
  if (keys.length > MAX_NORMAL_FORM_RULES || !keys.every(safeKey)) return undefined;
  const required = new Set(Array.isArray(schema.required) ? schema.required : []);
  const forms = new Set(uniqueForms(keys).values());
  const rules: NormalFormRule[] = [];
  for (const key of keys) {
    const property = schema.properties[key];
    if (!isRecord(property)) continue;
    if (forms.has(key)) rules.push({ kind: "key-form", key });
    if (!required.has(key) && !accepts(property, null)) rules.push({ kind: "optional-null", key });
    if (property.type === "number" || property.type === "integer") rules.push({ kind: "numeric-string", key });
    if (property.type === "string" && Array.isArray(property.enum) &&
        property.enum.every((value) => typeof value === "string") &&
        uniqueForms(property.enum).size > 0) rules.push({ kind: "enum-form", key });
  }
  if (rules.length === 0) return undefined;
  return { version: NORMAL_FORM_VERSION, ref, baseSchemaDigest: stableJsonHash(schema), rules: rules.slice(0, MAX_NORMAL_FORM_RULES) };
};


export const provesNormalFormPlan = (plan: NormalFormPlan, schema: unknown): boolean => {
  if (!isNormalFormPlan(plan)) return false;
  try {
    const derived = deriveNormalFormPlan(plan.ref, schema);
    return derived !== undefined && stableJsonHash(derived) === stableJsonHash(plan);
  } catch { return false; }
};

export const applyNormalFormPlan = (
  ref: string,
  schema: unknown,
  args: Record<string, unknown>,
  plan?: NormalFormPlan,
): NormalFormResult => {
  const unchanged = { args };
  // This early identity law protects every canonical capability, including
  // rare enum members and nullable values. Successful candidates satisfy it
  // too, which establishes idempotence without a corpus-dependent gate.
  const canonical = accepts(schema, args);
  const proven = !canonical && plan !== undefined && plan.ref === ref && provesNormalFormPlan(plan, schema);
  if (!useNormalized(canonical, proven, true, true) || !plan) return unchanged;
  const properties = (schema as { properties: Record<string, Record<string, unknown>> }).properties;
  const candidate = { ...args };
  const applied: NormalFormRule[] = [];
  const keyRules = plan.rules.filter((rule) => rule.kind === "key-form");
  const forms = uniqueForms(keyRules.map((rule) => rule.key));
  const targets = new Set<string>();
  for (const key of Object.keys(args).sort()) {
    if (Object.hasOwn(properties, key)) continue;
    const normalized = form(key);
    const target = normalized ? forms.get(normalized) : undefined;
    if (!target) continue;
    // Never discard a competing canonical value or choose between aliases.
    if (Object.hasOwn(args, target) || targets.has(target)) return unchanged;
    targets.add(target);
    Object.defineProperty(candidate, target, { value: args[key], enumerable: true, configurable: true, writable: true });
    delete candidate[key];
    applied.push({ kind: "key-form", key: target });
  }
  for (const rule of plan.rules) {
    if (rule.kind === "key-form" || !Object.hasOwn(candidate, rule.key)) continue;
    const value = candidate[rule.key];
    const property = properties[rule.key]!;
    if (accepts(property, value)) continue;
    if (rule.kind === "optional-null" && (value === null || value === undefined)) {
      delete candidate[rule.key];
      applied.push(rule);
    } else if (rule.kind === "numeric-string" && typeof value === "string") {
      const number = Number(value);
      if (Number.isFinite(number) && String(number) === value && accepts(property, number)) {
        candidate[rule.key] = number;
        applied.push(rule);
      }
    } else if (rule.kind === "enum-form" && typeof value === "string") {
      const normalized = form(value);
      const target = normalized ? uniqueForms(property.enum as string[]).get(normalized) : undefined;
      if (target !== undefined && accepts(property, target)) {
        candidate[rule.key] = target;
        applied.push(rule);
      }
    }
  }
  // A partial repair is not a successful correction. Return original input
  // on every refusal so authoritative validation reports the real failure.
  if (!useNormalized(canonical, proven, applied.length > 0, accepts(schema, candidate))) return unchanged;
  return {
    args: candidate,
    witness: {
      version: NORMAL_FORM_VERSION,
      baseSchemaDigest: plan.baseSchemaDigest,
      beforeShape: stableJsonHash(shapeSignature(args)),
      afterShape: stableJsonHash(shapeSignature(candidate)),
      rules: applied,
    },
  };
};
