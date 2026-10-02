import type { JevJson } from "../jev/types.js";

export const VERDICTS = ["moving", "stalled", "dependency", "agent_decision", "human_decision", "unknown"] as const;
export const ACTIONS = ["none", "collect_evidence", "wait_dependency", "request_agent_decision", "request_human_decision", "review_stall"] as const;
export type Verdict = typeof VERDICTS[number];
export const MAX_INPUT_BYTES = 32_768;
export interface JudgeRequest {
  questionClass: "item-stalled";
  itemRef: string;
  evidenceRefs: { url: string; revision: string; observedAt: string }[];
  evidence: { facts: Record<string, JevJson>; excerpts: string[] };
  allowedVerdicts: Verdict[];
  timeboxMs: number;
  budget: { maxEvaluations: number; maxAgents: number; maxTokens: number };
  requestKey: string;
}
export interface JudgeReply {
  verdict: Verdict;
  confidence: number | null;
  evidenceLinks: string[];
  nextAction: { kind: typeof ACTIONS[number]; owner: string; targetRef: string };
}
export interface JudgeEnvelope extends JudgeReply {
  confidenceProvenance: "jev-distribution" | "agent-self-report" | "unavailable";
  decisionId: string;
  cost: { usd: number | null; basis: "reported" | "unknown"; tokens: number; evaluations: number; agents: number };
  status: "completed" | "unknown";
  reasonCode: string;
}
const fail = (): never => { throw new Error("invalid_input"); };
export function object(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return fail();
  const record = value as Record<string, unknown>;
  if (Object.keys(record).length !== keys.length || !keys.every(key => Object.hasOwn(record, key))) return fail();
  return record;
}
function text(value: unknown, max: number): asserts value is string {
  if (typeof value !== "string" || value.length === 0 || value.length > max || /[\u0000-\u001f]/.test(value)) fail();
}
function integer(value: unknown, min: number, max: number): void {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) fail();
}
function json(value: unknown, depth = 0): void {
  if (depth > 6) fail();
  if (value === null || typeof value === "boolean") return;
  if (typeof value === "number" && Number.isFinite(value)) return;
  if (typeof value === "string" && value.length <= 4096 && !value.includes("\0")) return;
  if (!value || typeof value !== "object") fail();
  if (Object.keys(value as object).length > 128) fail();
  for (const [key, child] of Object.entries(value as object)) {
    if (key.length > 128 || ["__proto__", "constructor", "prototype"].includes(key)) fail();
    json(child, depth + 1);
  }
}
export function parseJudgeRequest(value: unknown): JudgeRequest {
  if (Buffer.byteLength(JSON.stringify(value) ?? "") > MAX_INPUT_BYTES) fail();
  const v = object(value, ["questionClass", "itemRef", "evidenceRefs", "evidence", "allowedVerdicts", "timeboxMs", "budget", "requestKey"]);
  if (v.questionClass !== "item-stalled") fail();
  text(v.itemRef, 512); text(v.requestKey, 256);
  if (!Array.isArray(v.evidenceRefs) || v.evidenceRefs.length > 32) fail();
  const urls = new Set<string>();
  for (const ref of v.evidenceRefs as unknown[]) {
    const r = object(ref, ["url", "revision", "observedAt"]);
    text(r.url, 2048); text(r.revision, 128); text(r.observedAt, 32);
    let url: URL; try { url = new URL(r.url); } catch { return fail(); }
    if (url.protocol !== "https:" || url.username || url.password || urls.has(r.url)) fail();
    urls.add(r.url);
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(r.observedAt) || !Number.isFinite(Date.parse(r.observedAt)) || new Date(r.observedAt).toISOString().slice(0, 19) !== r.observedAt.slice(0, 19)) fail();
  }
  const evidence = object(v.evidence, ["facts", "excerpts"]);
  if (!evidence.facts || typeof evidence.facts !== "object" || Array.isArray(evidence.facts)) fail();
  json(evidence.facts);
  if (!Array.isArray(evidence.excerpts) || evidence.excerpts.length > 32 || !evidence.excerpts.every(e => typeof e === "string" && e.length <= 4096 && !e.includes("\0"))) fail();
  if (!Array.isArray(v.allowedVerdicts) || !v.allowedVerdicts.includes("unknown") || v.allowedVerdicts.length < 1 || v.allowedVerdicts.length > VERDICTS.length || new Set(v.allowedVerdicts).size !== v.allowedVerdicts.length || !v.allowedVerdicts.every(verdict => VERDICTS.includes(verdict))) fail();
  integer(v.timeboxMs, 1, 300_000);
  const budget = object(v.budget, ["maxEvaluations", "maxAgents", "maxTokens"]);
  integer(budget.maxEvaluations, 0, 1); integer(budget.maxAgents, 0, 1); integer(budget.maxTokens, 1, 20_000);
  return v as unknown as JudgeRequest;
}
export function replySchema(request: JudgeRequest): Record<string, unknown> {
  return { type: "object", additionalProperties: false, required: ["verdict", "confidence", "evidenceLinks", "nextAction"], properties: {
    verdict: { type: "string", enum: request.allowedVerdicts },
    confidence: { anyOf: [{ type: "number", minimum: 0, maximum: 1 }, { type: "null" }] },
    evidenceLinks: { type: "array", maxItems: 32, uniqueItems: true, items: { type: "string", minLength: 1, maxLength: 2048 } },
    nextAction: { type: "object", additionalProperties: false, required: ["kind", "owner", "targetRef"], properties: {
      kind: { type: "string", enum: ACTIONS }, owner: { type: "string", enum: ["dev-lead", "knowledge-lead"] }, targetRef: { type: "string", const: request.itemRef },
    } },
  } };
}
export function validateReply(value: unknown, request: JudgeRequest): JudgeReply {
  const v = object(value, ["verdict", "confidence", "evidenceLinks", "nextAction"]);
  if (!request.allowedVerdicts.includes(v.verdict as Verdict) || !(v.confidence === null || typeof v.confidence === "number" && Number.isFinite(v.confidence) && v.confidence >= 0 && v.confidence <= 1)) throw new Error("invalid_schema");
  if (!Array.isArray(v.evidenceLinks) || v.evidenceLinks.length > 32 || new Set(v.evidenceLinks).size !== v.evidenceLinks.length) throw new Error("invalid_schema");
  if (!v.evidenceLinks.every(link => request.evidenceRefs.some(ref => ref.url === link)) || v.verdict !== "unknown" && v.evidenceLinks.length === 0) throw new Error("invalid_citation");
  const action = object(v.nextAction, ["kind", "owner", "targetRef"]);
  if (!ACTIONS.includes(action.kind as typeof ACTIONS[number]) || !["dev-lead", "knowledge-lead"].includes(action.owner as string) || action.targetRef !== request.itemRef) throw new Error("invalid_schema");
  const expected = recommendation(v.verdict as Verdict, request.itemRef);
  if (action.kind !== expected.kind || action.owner !== expected.owner) throw new Error("invalid_schema");
  return v as unknown as JudgeReply;
}
export function recommendation(verdict: Verdict, targetRef: string): JudgeReply["nextAction"] {
  const kind = { moving: "none", stalled: "review_stall", dependency: "wait_dependency", agent_decision: "request_agent_decision", human_decision: "request_human_decision", unknown: "collect_evidence" } as const;
  return { kind: kind[verdict], owner: verdict === "unknown" ? "knowledge-lead" : "dev-lead", targetRef };
}
