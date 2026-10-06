// Structural trace validation only; execution semantics remain in normal-form.ts.
// Keep compiler, schema and policy-kernel imports off the registration graph.
export const NORMAL_FORM_VERSION = 1 as const;
export const MAX_NORMAL_FORM_RULES = 128;
export const MAX_NORMAL_FORM_PLANS = 1_000;
const RULE_KINDS = ["key-form", "enum-form", "numeric-string", "optional-null"] as const;
export type NormalFormRuleKind = typeof RULE_KINDS[number];
export interface NormalFormRule { kind: NormalFormRuleKind; key: string }
export interface NormalFormPlan {
  version: typeof NORMAL_FORM_VERSION;
  ref: string;
  baseSchemaDigest: string;
  rules: NormalFormRule[];
}
export interface NormalFormWitness {
  version: typeof NORMAL_FORM_VERSION;
  baseSchemaDigest: string;
  beforeShape: string;
  afterShape: string;
  rules: NormalFormRule[];
}
export const normalFormRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
export const normalFormSafeKey = (key: string): boolean => key.length <= 128 &&
  !["__proto__", "prototype", "constructor"].includes(key);
const isRule = (value: unknown): value is NormalFormRule => normalFormRecord(value) &&
  Object.keys(value).length === 2 && typeof value.key === "string" && normalFormSafeKey(value.key) &&
  RULE_KINDS.includes(value.kind as NormalFormRuleKind);
const isDigest = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
export const isNormalFormPlan = (value: unknown): value is NormalFormPlan => normalFormRecord(value) &&
  Object.keys(value).length === 4 && value.version === NORMAL_FORM_VERSION &&
  typeof value.ref === "string" && value.ref.length > 0 && value.ref.length <= 1_024 &&
  isDigest(value.baseSchemaDigest) && Array.isArray(value.rules) &&
  value.rules.length > 0 && value.rules.length <= MAX_NORMAL_FORM_RULES && value.rules.every(isRule);
export const isNormalFormWitness = (value: unknown): value is NormalFormWitness => normalFormRecord(value) &&
  Object.keys(value).length === 5 && value.version === NORMAL_FORM_VERSION &&
  isDigest(value.baseSchemaDigest) && isDigest(value.beforeShape) && isDigest(value.afterShape) &&
  Array.isArray(value.rules) && value.rules.length > 0 && value.rules.length <= MAX_NORMAL_FORM_RULES && value.rules.every(isRule);
