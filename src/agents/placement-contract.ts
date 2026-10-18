/**
 * Swappable placement wire/CLI boundary, from placement_contract.json v1.
 * Shape and cross-binding validation is NOT protected-writer authentication or
 * native create authority. Callers must retain the full answer and revalidate
 * it, the protected writer and the native create fence immediately before use.
 * No CLI flags are published: the host-configured argv is used literally and
 * the operation is carried only by one prompt-free JSON object on stdin.
 */
export const PLACEMENT_DEADLINE_MS = 2_000;
export const PLACEMENT_MAX_BYTES = 1024 * 1024;
export const PLACEMENT_EXIT = { place: 0, queue: 75, shadow: 78 } as const;
const SCHEMA = "smarty.placement.v1";
const SHADOW_SCHEMA = "smarty.placement.shadow.v1";

export type PlacementThinking = "off" | "minimal" | "none" | "low" | "medium" | "high" | "xhigh" | "max";
export type PlacementQueueReason =
  | "AGENT_CAPACITY_EXHAUSTED" | "FACTS_UNKNOWN" | "CI_FACTS_UNKNOWN"
  | "HUMAN_RESERVE_PROTECTED" | "ROUTE_UNHEALTHY" | "PROVIDER_CAPACITY_UNKNOWN"
  | "QUALITY_PROFILE_UNAVAILABLE" | "POLICY_CONFLICT" | "BINDING_UNVERIFIED"
  | "DECIDER_UNAVAILABLE" | "REQUEST_INVALID" | "DEADLINE_EXPIRED" | "QUEUE_FULL"
  | "HUMAN_DEMAND_SATURATED";
export interface PlacementEvidence {
  reason: PlacementQueueReason;
  host?: string;
  route_id?: string;
  evidence_ref?: string;
}
export interface PlacementProvenance { policy_digest: string; facts_digest: string; writer_epoch: number }
export interface PlacementDecideRequest {
  schema: typeof SCHEMA;
  operation: "decide";
  launch_id: string;
  agent_name: string;
  generation: number;
  /** Always enforced on the wire. Host mode is never caller authority. */
  mode: "enforced";
  assignment: { org_id: string; work_id: string };
  role: string;
  complexity: "simple" | "normal" | "complex" | "delicate";
  required_profile: string;
  interaction: { class: "live-human" | "background-agent"; origin_ref: string };
  requirements: { capabilities: string[]; runtime: "pi-rpc"; source_pin: string; memory_profile_ref: string };
  requested?: { host?: string | null; model?: string | null; thinking?: PlacementThinking | null };
  deadline_at?: string;
}
/** The complete protected CLAIMED result: never project this down to a triplet. */
export interface PlacementProtectedGrant {
  schemaVersion: 2;
  status: "CLAIMED";
  requestId: string;
  allocationRequestId: string;
  requesterSession?: string;
  launchId: string;
  launchSpecId: string;
  configDigest: string;
  expiresAt: string;
  reservationId: string;
  fence: number;
  host: string;
  model: string;
  thinking: PlacementThinking;
  requesterRef: string;
  drainerSpawnerRef: string;
  launchAllowed: true;
}
export interface PlacementPlace {
  schema: typeof SCHEMA;
  decision: "PLACE";
  mode: "enforced";
  launch_id: string;
  generation: number;
  org_id: string;
  decision_revision: number;
  host: string;
  model: string;
  thinking: PlacementThinking;
  route_id: string;
  pool_id: string;
  gateway_policy_digest: string;
  reason: "CHEAPEST_ELIGIBLE";
  evidence: PlacementEvidence[];
  reservation_id: string;
  fence: number;
  create_before: string;
  provenance: PlacementProvenance;
  grant: PlacementProtectedGrant;
}
export interface PlacementQueue {
  schema: typeof SCHEMA;
  decision: "QUEUE";
  mode: "enforced";
  launch_id?: string;
  generation?: number;
  reason: PlacementQueueReason;
  retry_after: number;
  evidence: PlacementEvidence[];
  queue_id?: string;
  decision_revision?: number;
  provenance?: Partial<PlacementProvenance>;
}
export interface PlacementReleaseRequest {
  schema: typeof SCHEMA;
  operation: "release";
  launch_id: string;
  generation: number;
  reservation_id: string;
  fence: number;
  native_launch_id?: string;
  config_digest?: string;
}
export interface PlacementFeedbackRequest {
  schema: typeof SCHEMA;
  operation: "feedback";
  launch_id: string;
  generation: number;
  reservation_id: string;
  fence: number;
  native_launch_id: string;
  config_digest: string;
  kind: "registered" | "started" | "terminal" | "stopped";
  event_id: string;
  session_id: string;
  evidence_ref: string;
}
export interface PlacementShadow {
  schema: typeof SHADOW_SCHEMA;
  kind: "SHADOW_OBSERVATION";
  launch_id: string;
  generation: number;
  pool_status: "NOT_ENFORCED";
  hypothetical:
    | { decision: "PLACE"; host: string; model: string; thinking: PlacementThinking; pool_id: string }
    | { decision: "QUEUE"; reason: PlacementQueueReason; retry_after: number };
  actual_baseline_ref: string | null;
  observation_id?: string;
  at?: string;
  launch_kind?: "lead" | "lane" | "task" | "actor";
  org_id?: string;
  owner_ref?: string;
  agent_ref?: string;
  policy_digest?: string;
  facts_digest?: string;
}
export type PlacementRequest = PlacementDecideRequest;
export type PlacementWireRequest = PlacementRequest | PlacementReleaseRequest | PlacementFeedbackRequest;
export type PlacementAnswer = PlacementPlace | PlacementQueue | PlacementShadow;
/** Full contextual grant; the protected inner grant alone lacks generation/org binding. */
export type PlacementGrant = PlacementPlace;
export type PlacementShadowObservation = PlacementShadow;

/**
 * Native launch-record/receipt schema is not published yet. Only these already
 * published receipt references may be mapped. receipt is opaque, never copied
 * onto the wire, and cannot establish registration or terminal/non-start proof.
 */
export interface PlacementNativeIdentityPlaceholder {
  launchId: string;
  configDigest: string;
  eventId: string;
  sessionId: string;
  evidenceRef: string;
  receipt: unknown;
}
export interface PlacementReleaseProofPlaceholder { receipt: unknown }
export type PlacementNativeIdentity = PlacementNativeIdentityPlaceholder;
export type PlacementReleaseProof = PlacementReleaseProofPlaceholder;
export interface PlacementAcknowledgementPlaceholder { readonly wire: unknown }

export class PlacementContractError extends Error {
  constructor(path: string) { super(`Invalid placement contract: ${path}`); this.name = "PlacementContractError"; }
}
const invalid = (path: string): never => { throw new PlacementContractError(path); };
type Check = (value: unknown, path: string) => void;
type Obj = Record<string, unknown>;
const object = (value: unknown, path: string): Obj => {
  if (!value || typeof value !== "object" || Array.isArray(value) ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(value))) invalid(path);
  return value as Obj;
};
const closed = (required: Record<string, Check>, optional: Record<string, Check> = {}): Check => (value, path) => {
  const input = object(value, path);
  for (const key of Object.keys(input)) if (!Object.hasOwn(required, key) && !Object.hasOwn(optional, key)) invalid(`${path}.${key}`);
  for (const [key, check] of Object.entries(required)) {
    if (!Object.hasOwn(input, key)) invalid(`${path}.${key}`);
    check(input[key], `${path}.${key}`);
  }
  for (const [key, check] of Object.entries(optional)) if (Object.hasOwn(input, key)) check(input[key], `${path}.${key}`);
};
const literal = (expected: unknown): Check => (value, path) => { if (value !== expected) invalid(path); };
const enumeration = (values: readonly string[]): Check => (value, path) => {
  if (typeof value !== "string" || !values.includes(value)) invalid(path);
};
const id: Check = (value, path) => {
  if (typeof value !== "string" || [...value].length < 1 || [...value].length > 200 || /[\u0000-\u001f\u007f]/u.test(value)) invalid(path);
};
const auditRef: Check = (value, path) => {
  if (typeof value !== "string" || value.length < 1 || value.length > 160 || /[^!-?A-~]/u.test(value)) invalid(path);
};
const counter: Check = (value, path) => { if (!Number.isSafeInteger(value) || (value as number) < 1) invalid(path); };
const digest: Check = (value, path) => {
  if (typeof value !== "string" || !/^sha256:[0-9a-f]{64}$(?![\s\S])/u.test(value)) invalid(path);
};
const configDigest: Check = (value, path) => {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$(?![\s\S])/u.test(value)) invalid(path);
};
/** Compare at microsecond precision; Date.parse alone both truncates and repairs invalid dates. */
const timestampMicros = (value: unknown, path: string): bigint => {
  if (typeof value !== "string") return invalid(path);
  const match = /^(\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d)(?:\.(\d{1,6}))?Z$(?![\s\S])/u.exec(value);
  if (!match) return invalid(path);
  const millis = Date.parse(`${match[1]}Z`);
  if (!Number.isFinite(millis) || new Date(millis).toISOString().slice(0, 19) !== match[1]) return invalid(path);
  return BigInt(millis) * 1000n + BigInt((match[2] ?? "").padEnd(6, "0"));
};
const timestamp: Check = (value, path) => { timestampMicros(value, path); };
const thinking = enumeration(["off", "minimal", "none", "low", "medium", "high", "xhigh", "max"]);
const reason = enumeration(["AGENT_CAPACITY_EXHAUSTED", "FACTS_UNKNOWN", "CI_FACTS_UNKNOWN", "HUMAN_RESERVE_PROTECTED",
  "ROUTE_UNHEALTHY", "PROVIDER_CAPACITY_UNKNOWN", "QUALITY_PROFILE_UNAVAILABLE", "POLICY_CONFLICT", "BINDING_UNVERIFIED",
  "DECIDER_UNAVAILABLE", "REQUEST_INVALID", "DEADLINE_EXPIRED", "QUEUE_FULL", "HUMAN_DEMAND_SATURATED"]);
const retryAfter: Check = (value, path) => {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 86400) invalid(path);
};
const nullable = (check: Check): Check => (value, path) => { if (value !== null) check(value, path); };
const array = (check: Check, unique = false): Check => (value, path) => {
  if (!Array.isArray(value) || value.length > 100) return invalid(path);
  if (unique && new Set(value).size !== value.length) invalid(path);
  for (const entry of value) check(entry, `${path}[]`);
};
const evidence = array(closed({ reason }, { host: id, route_id: id, evidence_ref: id }));
const provenanceFields = { policy_digest: digest, facts_digest: digest, writer_epoch: counter };
const provenance = closed(provenanceFields);
const knownProvenance: Check = (value, path) => {
  closed({}, provenanceFields)(value, path);
  if (!Object.keys(value as Obj).length) invalid(path);
};
const decide = closed({
  schema: literal(SCHEMA), operation: literal("decide"), launch_id: id, agent_name: id, generation: counter, mode: literal("enforced"),
  assignment: closed({ org_id: id, work_id: id }), role: id, complexity: enumeration(["simple", "normal", "complex", "delicate"]), required_profile: id,
  interaction: closed({ class: enumeration(["live-human", "background-agent"]), origin_ref: id }),
  requirements: closed({ capabilities: array(id, true), runtime: literal("pi-rpc"), source_pin: id, memory_profile_ref: id }),
}, { requested: closed({}, { host: nullable(id), model: nullable(id), thinking: nullable(thinking) }), deadline_at: timestamp });
const grant = closed({
  schemaVersion: literal(2), status: literal("CLAIMED"), requestId: id, allocationRequestId: id, launchId: id, launchSpecId: id,
  configDigest, expiresAt: timestamp, reservationId: id, fence: counter, host: id, model: id, thinking,
  requesterRef: id, drainerSpawnerRef: id, launchAllowed: literal(true),
}, { requesterSession: id });
const place = closed({
  schema: literal(SCHEMA), decision: literal("PLACE"), mode: literal("enforced"), launch_id: id, generation: counter, org_id: id,
  decision_revision: counter, host: id, model: id, thinking, route_id: id, pool_id: id, gateway_policy_digest: digest,
  reason: literal("CHEAPEST_ELIGIBLE"), evidence, reservation_id: id, fence: counter, create_before: timestamp, provenance, grant,
});
const queue: Check = (value, path) => {
  closed({ schema: literal(SCHEMA), decision: literal("QUEUE"), mode: literal("enforced"), reason, retry_after: retryAfter, evidence },
    { launch_id: id, generation: counter, queue_id: id, decision_revision: counter, provenance: knownProvenance })(value, path);
  const q = value as PlacementQueue;
  if ((q.launch_id === undefined) !== (q.generation === undefined)) invalid(`${path}.identity`);
  if (q.launch_id === undefined && !["REQUEST_INVALID", "BINDING_UNVERIFIED", "DECIDER_UNAVAILABLE"].includes(q.reason)) invalid(`${path}.identity`);
  if (q.queue_id !== undefined && (q.launch_id === undefined || ["REQUEST_INVALID", "BINDING_UNVERIFIED"].includes(q.reason))) invalid(`${path}.queue_id`);
};
const release: Check = (value, path) => {
  closed({ schema: literal(SCHEMA), operation: literal("release"), launch_id: id, generation: counter, reservation_id: id, fence: counter },
    { native_launch_id: id, config_digest: configDigest })(value, path);
  const r = value as PlacementReleaseRequest;
  if ((r.native_launch_id === undefined) !== (r.config_digest === undefined)) invalid(`${path}.native_identity`);
};
const feedback = closed({
  schema: literal(SCHEMA), operation: literal("feedback"), launch_id: id, generation: counter, reservation_id: id, fence: counter,
  native_launch_id: id, config_digest: configDigest, kind: enumeration(["registered", "started", "terminal", "stopped"]),
  event_id: id, session_id: id, evidence_ref: id,
});
const hypothetical: Check = (value, path) => {
  const h = object(value, path);
  if (h.decision === "PLACE") closed({ decision: literal("PLACE"), host: id, model: id, thinking, pool_id: id })(value, path);
  else closed({ decision: literal("QUEUE"), reason, retry_after: retryAfter })(value, path);
};
const shadow = closed({
  schema: literal(SHADOW_SCHEMA), kind: literal("SHADOW_OBSERVATION"), launch_id: id, generation: counter,
  pool_status: literal("NOT_ENFORCED"), hypothetical, actual_baseline_ref: nullable(id),
}, { observation_id: auditRef, at: timestamp, launch_kind: enumeration(["lead", "lane", "task", "actor"]),
  org_id: auditRef, owner_ref: auditRef, agent_ref: auditRef, policy_digest: digest, facts_digest: digest });

export function validatePlacementRequest(value: unknown): asserts value is PlacementWireRequest {
  const input = object(value, "request");
  if (input.operation === "decide") decide(value, "request");
  else if (input.operation === "release") release(value, "request");
  else if (input.operation === "feedback") feedback(value, "request");
  else invalid("request.operation");
}
export function validatePlacementPlace(value: unknown): asserts value is PlacementPlace { place(value, "place"); validateGrantBindings(value as PlacementPlace); }
const equal = (actual: unknown, expected: unknown, path: string): void => { if (actual !== expected) invalid(path); };
const validateGrantBindings = (p: PlacementPlace): void => {
  equal(p.grant.allocationRequestId, p.launch_id, "grant.allocationRequestId");
  equal(p.grant.reservationId, p.reservation_id, "grant.reservationId");
  equal(p.grant.fence, p.fence, "grant.fence");
  equal(p.grant.host, p.host, "grant.host");
  equal(p.grant.model, p.model, "grant.model");
  equal(p.grant.thinking, p.thinking, "grant.thinking");
  if (timestampMicros(p.create_before, "create_before") > timestampMicros(p.grant.expiresAt, "grant.expiresAt")) invalid("create_before");
};
/** Public pre-create recheck: preserve the original trusted request, never repick. */
export function validatePlacementBindings(p: PlacementPlace, request: PlacementDecideRequest, now = Date.now()): void {
  validatePlacementRequest(request);
  validatePlacementPlace(p);
  equal(p.launch_id, request.launch_id, "place.launch_id");
  equal(p.generation, request.generation, "place.generation");
  equal(p.org_id, request.assignment.org_id, "place.org_id");
  if (!Number.isSafeInteger(now)) invalid("now");
  const nowMicros = BigInt(now) * 1000n;
  if (timestampMicros(p.create_before, "create_before") <= nowMicros || timestampMicros(p.grant.expiresAt, "grant.expiresAt") <= nowMicros) invalid("place.expired");
  if (request.deadline_at !== undefined && timestampMicros(request.deadline_at, "request.deadline_at") <= nowMicros) invalid("request.deadline_at");
  for (const field of ["host", "model", "thinking"] as const) {
    const pin = request.requested?.[field];
    if (pin !== undefined && pin !== null) equal(p[field], pin, `place.${field}`);
  }
}

/** Strict JSON: duplicate decoded keys, nonfinite numbers, trailing text and deep input are refused. */
export const parsePlacementJson = (text: string): unknown => {
  if (Buffer.byteLength(text, "utf8") > PLACEMENT_MAX_BYTES) invalid("json.size");
  let at = 0;
  const ws = (): void => { while (at < text.length && /[\x20\t\r\n]/u.test(text[at]!)) at++; };
  const string = (): string => {
    const start = at++;
    while (at < text.length) {
      const char = text[at++];
      if (char === "\\") { at++; continue; }
      if (char === '"') {
        try { return JSON.parse(text.slice(start, at)) as string; } catch { return invalid("json.string"); }
      }
    }
    return invalid("json.string");
  };
  const value = (depth: number): unknown => {
    if (depth > 32) invalid("json.depth");
    ws();
    const char = text[at];
    if (char === '"') return string();
    if (char === "{" || char === "[") {
      at++; ws();
      const isObject = char === "{";
      const end = isObject ? "}" : "]";
      const result: Obj | unknown[] = isObject ? Object.create(null) as Obj : [];
      const keys = new Set<string>();
      if (text[at] === end) { at++; return result; }
      while (true) {
        ws();
        if (isObject) {
          if (text[at] !== '"') invalid("json.key");
          const key = string();
          if (keys.has(key)) invalid("json.duplicate_key");
          keys.add(key); ws();
          if (text[at++] !== ":") invalid("json.colon");
          (result as Obj)[key] = value(depth + 1);
        } else (result as unknown[]).push(value(depth + 1));
        ws();
        if (text[at] === end) { at++; return result; }
        if (text[at++] !== ",") invalid("json.separator");
      }
    }
    const token = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/u.exec(text.slice(at))?.[0];
    if (!token) return invalid("json.value");
    at += token.length;
    const parsed: unknown = JSON.parse(token);
    if (typeof parsed === "number" && !Number.isFinite(parsed)) invalid("json.number");
    return parsed;
  };
  const parsed = value(0); ws();
  if (at !== text.length) invalid("json.trailing");
  return parsed;
};
const freeze = <T>(value: T): T => {
  if (value !== null && typeof value === "object") {
    for (const item of Object.values(value)) freeze(item);
    Object.freeze(value);
  }
  return value;
};
export const placementInvocation = (command: readonly string[], request: PlacementWireRequest): { executable: string; args: string[]; stdin: string } => {
  validatePlacementRequest(request);
  const stdin = JSON.stringify(request);
  if (Buffer.byteLength(stdin, "utf8") > PLACEMENT_MAX_BYTES) invalid("request.size");
  return { executable: command[0]!, args: command.slice(1), stdin };
};
/** Takes a private, immutable snapshot before async IO so request mutation cannot alter bindings. */
export const snapshotPlacementRequest = (request: PlacementDecideRequest): PlacementDecideRequest => {
  validatePlacementRequest(request);
  if (request.operation !== "decide") invalid("request.operation");
  if (request.deadline_at !== undefined && timestampMicros(request.deadline_at, "request.deadline_at") <= BigInt(Date.now()) * 1000n) invalid("request.deadline_at");
  return freeze(parsePlacementJson(JSON.stringify(request)) as PlacementDecideRequest);
};
export const decodePlacementAnswer = (stdout: string, exitCode: number | null, request: PlacementDecideRequest, now = Date.now()): PlacementAnswer => {
  validatePlacementRequest(request);
  if (request.operation !== "decide") invalid("request.operation");
  const answer = parsePlacementJson(stdout);
  const input = object(answer, "answer");
  if (exitCode === PLACEMENT_EXIT.place) {
    validatePlacementBindings(answer as PlacementPlace, request, now);
  } else if (exitCode === PLACEMENT_EXIT.queue) {
    queue(answer, "queue");
    if (input.launch_id !== undefined) {
      equal(input.launch_id, request.launch_id, "queue.launch_id");
      equal(input.generation, request.generation, "queue.generation");
    }
  } else if (exitCode === PLACEMENT_EXIT.shadow) {
    shadow(answer, "shadow");
    equal(input.launch_id, request.launch_id, "shadow.launch_id");
    equal(input.generation, request.generation, "shadow.generation");
  } else invalid("exit_code");
  return freeze(answer as PlacementAnswer);
};
export const isPlacementPlace = (answer: PlacementAnswer): answer is PlacementPlace => "decision" in answer && answer.decision === "PLACE";
export const isPlacementQueue = (answer: PlacementAnswer): answer is PlacementQueue => "decision" in answer && answer.decision === "QUEUE";

export const placementRegistrationRequest = (p: PlacementPlace, native: PlacementNativeIdentityPlaceholder): PlacementFeedbackRequest => {
  validatePlacementPlace(p);
  equal(native.launchId, p.grant.launchId, "native.launchId");
  equal(native.configDigest, p.grant.configDigest, "native.configDigest");
  const result: PlacementFeedbackRequest = {
    schema: SCHEMA, operation: "feedback", launch_id: p.launch_id, generation: p.generation,
    reservation_id: p.reservation_id, fence: p.fence, native_launch_id: native.launchId, config_digest: native.configDigest,
    kind: "registered", event_id: native.eventId, session_id: native.sessionId, evidence_ref: native.evidenceRef,
  };
  validatePlacementRequest(result);
  return result;
};
/** This is cancel only. proof is intentionally NOT projected into an invented wire field. */
export const placementReleaseRequest = (p: PlacementPlace, _proof: PlacementReleaseProofPlaceholder): PlacementReleaseRequest => {
  validatePlacementPlace(p);
  return {
    schema: SCHEMA, operation: "release", launch_id: p.launch_id, generation: p.generation,
    reservation_id: p.reservation_id, fence: p.fence, native_launch_id: p.grant.launchId, config_digest: p.grant.configDigest,
  };
};
export const decodePlacementAcknowledgement = (stdout: string, exitCode: number | null): PlacementAcknowledgementPlaceholder => {
  // Exit zero here is only transport success, never a decide PLACE or receipt proof.
  if (exitCode !== 0) invalid("acknowledgement.exit_code");
  const wire = parsePlacementJson(stdout);
  object(wire, "acknowledgement");
  // ACK schema/verification is unpublished. This never returns registration or release authority.
  return freeze({ wire });
};
