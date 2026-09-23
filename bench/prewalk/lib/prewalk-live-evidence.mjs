// Pure, strict analysis of live Pi event streams and persisted session entries.
// Assistant timestamps are message creation times, not response completion times.
export const PREWALK_MESSAGE_PREFIX = "pi-fabric-prewalk-";
const object = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const time = (value) => Number.isFinite(value) ? value : null;
const span = (start, end) => Number.isFinite(start) && Number.isFinite(end) && end >= start ? end - start : null;

export function parseJsonLines(text, source = "evidence") {
  if (typeof text !== "string") throw new Error(`${source}: expected a string`);
  const records = [];
  for (const [index, line] of text.split("\n").entries()) {
    if (!line.trim()) continue;
    let value;
    try { value = JSON.parse(line); }
    catch { throw new Error(`Malformed ${source} JSONL at line ${index + 1}`); }
    if (!object(value)) throw new Error(`Malformed ${source} JSONL at line ${index + 1}: expected a JSON object`);
    records.push(value);
  }
  if (!records.length) throw new Error(`${source}: recording is empty`);
  return records;
}

// Check lifecycle completeness separately from parsing or task quality. Raw
// session_compact_failed is retained: it can describe a benign host rejection.
export function assertCompleteRecording(events, telemetry, options = {}) {
  const requireSessionHeader = options.requireSessionHeader !== false;
  const headerId = events[0]?.type === "session" ? events[0].id : undefined;
  if ((requireSessionHeader && headerId === undefined) || !events.some((e) => e.type === "agent_settled")) {
    throw new Error(requireSessionHeader
      ? "Incomplete live recording: session header or agent_settled missing"
      : "Incomplete live recording: agent_settled missing");
  }
  const start = telemetry.find((e) => e.type === "session_start");
  if (!start || !telemetry.some((e) => e.type === "session_shutdown")) {
    throw new Error("Incomplete telemetry: session_start or session_shutdown missing");
  }
  // RPC recordings have no synthetic session header event; callers pass the
  // real sessionId observed from get_state instead.
  const expectedSessionId = options.sessionId ?? headerId;
  if (expectedSessionId !== undefined && start.sessionId !== expectedSessionId) {
    throw new Error("Mismatched live/telemetry session identity");
  }
  if (telemetry.some((e) => e.type === "turn_limit")) throw new Error("Canary turn limit reached");
  for (const event of events) {
    if (event.type !== "message_end") continue;
    if (!object(event.message)) throw new Error("Malformed message_end: message missing");
    if (event.message.role === "assistant" && ["error", "aborted", "length"].includes(event.message.stopReason)) {
      // A recovery canary aborts the executor on purpose; performance cells
      // keep the strict rule and fail on any non-complete assistant.
      if (options.allowAborted === true && event.message.stopReason === "aborted") continue;
      throw new Error(`Assistant did not complete: ${event.message.stopReason}`);
    }
  }
}

export function livePrewalkMessages(events) {
  return events.flatMap((event) => {
    const m = event.message;
    if (event.type !== "message_end" || m?.role !== "custom" ||
        typeof m.customType !== "string" || !m.customType.startsWith(PREWALK_MESSAGE_PREFIX)) return [];
    return [{ source: "live", customType: m.customType, details: m.details ?? null, at: time(m.timestamp) }];
  });
}

export function persistedPrewalkMessages(entries) {
  return entries.flatMap((e) => {
    if (e.type !== "custom_message" || typeof e.customType !== "string" ||
        !e.customType.startsWith(PREWALK_MESSAGE_PREFIX)) return [];
    return [{ source: "session", id: typeof e.id === "string" ? e.id : null,
      customType: e.customType, details: e.details ?? null,
      at: time(typeof e.timestamp === "string" ? Date.parse(e.timestamp) : e.timestamp) }];
  });
}

export function prewalkMessageKey(message) {
  const d = object(message.details) ? message.details : {};
  // A continuation ID is authoritative. Other messages are paired by metadata
  // and occurrence, never by searching their text or collapsing repeats.
  return JSON.stringify([message.customType, d.continuationId ?? null, d.model ?? null, d.trigger ?? null]);
}

export function mergePrewalkMessages(live, persisted) {
  const rows = live.map((m) => ({ customType: m.customType, details: m.details, at: m.at, sources: ["live"] }));
  const available = new Map();
  live.forEach((m, index) => {
    const key = prewalkMessageKey(m);
    if (!available.has(key)) available.set(key, []);
    available.get(key).push(index);
  });
  for (const m of persisted) {
    const index = available.get(prewalkMessageKey(m))?.shift();
    if (index !== undefined) {
      rows[index].sources.push("session");
      if (rows[index].at === null) rows[index].at = m.at;
      if (m.id) rows[index].sessionEntryId = m.id;
    } else {
      rows.push({ customType: m.customType, details: m.details, at: m.at, sources: ["session"],
        ...(m.id ? { sessionEntryId: m.id } : {}) });
    }
  }
  return rows;
}

export function assistantTimeline(events) {
  return events.flatMap((e) => {
    const m = e.message;
    if (e.type !== "message_end" || m?.role !== "assistant") return [];
    return [{ at: time(m.timestamp), model: typeof m.provider === "string" && typeof m.model === "string"
      ? `${m.provider}/${m.model}` : null, usage: m.usage ?? null, stopReason: m.stopReason ?? null }];
  });
}
export function toolResultUsages(events) {
  return events.filter((e) => e.type === "message_end" && e.message?.role === "toolResult" && e.message.usage)
    .map((e) => e.message.usage);
}
const USAGE_FIELDS = ["input", "output", "cacheRead", "cacheWrite", "totalTokens"];
const numeric = (value) => typeof value === "number" && Number.isFinite(value) && value >= 0;
export function aggregateUsage(timeline, toolUsages = []) {
  const perModel = Object.create(null);
  const add = (key, u) => {
    if (!object(u) || !key || USAGE_FIELDS.some((f) => !numeric(u[f])) || !numeric(u.cost?.total)) {
      throw new Error("Incomplete or invalid recorded usage/model identity");
    }
    const bucket = perModel[key] ??= { requests: 0, input: 0, output: 0, cacheRead: 0, cacheWrite: 0,
      totalTokens: 0, recordedCostEstimateUsd: 0 };
    bucket.requests++;
    for (const f of USAGE_FIELDS) bucket[f] += u[f];
    bucket.recordedCostEstimateUsd += u.cost.total;
  };
  for (const item of timeline) {
    // Failed attempts may have no usage; never turn successful missing usage into zero cost.
    if (!item.usage && ["error", "aborted"].includes(item.stopReason)) continue;
    add(item.model, item.usage);
  }
  for (const u of toolUsages) add("nested-tool-usage", u);
  return perModel;
}
export function usageMatches(actual, expected, tolerance = 1e-9) {
  if (!numeric(tolerance)) throw new Error("Invalid usage tolerance");
  const mismatches = [];
  if (!object(actual) || !object(expected)) return { ok: false, mismatches: [{ reason: "missing usage" }] };
  const keys = new Set([...Object.keys(actual), ...Object.keys(expected)]);
  for (const key of keys) {
    for (const field of ["requests", ...USAGE_FIELDS, "recordedCostEstimateUsd"]) {
      const a = actual[key]?.[field], b = expected[key]?.[field];
      if (!numeric(a) || !numeric(b) || Math.abs(a - b) > (field === "recordedCostEstimateUsd" ? tolerance : 0)) {
        mismatches.push({ key, field, actual: a, expected: b });
      }
    }
  }
  return { ok: mismatches.length === 0, mismatches };
}

export function telemetryTimeline(events) {
  const modelSelects = [], compaction = [], boundaries = {};
  for (const e of events) {
    if (e.type === "model_select") modelSelects.push({ at: time(e.at), model: e.model ?? null, previous: e.previous ?? null });
    else if (["compaction", "compaction_failed"].includes(e.type)) {
      compaction.push({ type: e.type, at: time(e.at), reason: e.reason ?? null, error: e.error ?? null });
    } else if (["session_start", "before_agent_start", "agent_settled", "session_shutdown"].includes(e.type)) {
      // First task start, last settled/shutdown boundary. No per-turn double counting.
      if (e.type.startsWith("session_") || e.type === "agent_settled" || boundaries[e.type] === undefined) {
        boundaries[e.type] = time(e.at);
      }
    }
  }
  return { modelSelects, compaction, boundaries };
}

// Arrival-clock duration only: never subtract a host callback timestamp from
// a stdout arrival timestamp. Pair attempts in order and return the LAST one,
// matching the last terminal compaction record selected below.
export function arrivalCompactionBounds(events, arrivals) {
  if (arrivals === null || arrivals === undefined) return null;
  if (arrivals.length !== events.length) throw new Error("Incomplete arrival log: line count differs");
  let previous = -Infinity, active = null, last = null;
  events.forEach((e, index) => {
    const a = arrivals[index];
    if (a?.lineIndex !== index || !Number.isFinite(a.arrivalMs) || a.arrivalMs < previous) {
      throw new Error(`Invalid arrival line alignment/clock at index ${index}`);
    }
    previous = a.arrivalMs;
    if (e.type === "compaction_start") {
      if (active !== null) throw new Error("Overlapping compaction attempts");
      active = a.arrivalMs;
      last = { startAt: active, endAt: null };
    } else if (e.type === "compaction_end") {
      if (active === null) throw new Error("Compaction end without start");
      last = { startAt: active, endAt: a.arrivalMs };
      active = null;
    }
  });
  return last;
}

export function attributePhases({ timeline, telemetry, compactionArrival = null }) {
  const last = telemetry?.compaction.at(-1);
  const phases = {
    handoffAt: null, returnAt: null, assistantSpanMs: null, preHandoffMainMs: null,
    executorIntervalMs: null, executorAssistantSpanMs: null, returnMs: null,
    compaction: last ? { outcome: last.type, at: last.at, reason: last.reason, error: last.error,
      attemptMs: span(compactionArrival?.startAt, compactionArrival?.endAt) } : null,
    provenance: {
      assistant: "assistant message creation timestamps (start-to-start, not response duration)",
      modelBoundaries: "extension model_select observation timestamps",
      return: "return model_select to session_shutdown (not a Main-model response)",
      compaction: "last raw extension terminal event; host label is not Fabric compact.status",
      compactionAttempt: "paired stdout arrival timestamps only; absent pairs remain null",
    }, missing: [],
  };
  const times = timeline.map((m) => m.at);
  phases.assistantSpanMs = times.length >= 2 ? span(times[0], times.at(-1)) : null;
  if (phases.assistantSpanMs === null) phases.missing.push("fewer than two valid ordered assistant timestamps");
  if (last && phases.compaction.attemptMs === null) phases.missing.push("compaction attempt start/end arrival pair missing");
  const selects = telemetry?.modelSelects ?? [];
  const [handoff, back] = selects;
  if (!handoff) {
    phases.missing.push("no model_select recorded (Prewalk OFF or recorder gap)");
    return phases;
  }
  const main = timeline[0]?.model;
  if (selects.length > 2 || !main || handoff.previous !== main || handoff.model === main ||
      !handoff.model || (back && (back.model !== main || back.previous !== handoff.model))) {
    phases.missing.push("model selections are not one Main/executor roundtrip");
    return phases;
  }
  phases.handoffAt = time(handoff.at);
  phases.preHandoffMainMs = span(times[0], handoff.at);
  if (phases.preHandoffMainMs === null) phases.missing.push("first assistant/handoff timestamp missing or unordered");
  if (!back) phases.missing.push("return model_select missing");
  else {
    phases.returnAt = time(back.at);
    phases.executorIntervalMs = span(handoff.at, back.at);
    phases.returnMs = span(back.at, telemetry?.boundaries.session_shutdown);
    if (phases.executorIntervalMs === null) phases.missing.push("handoff/return timestamps missing or unordered");
    if (phases.returnMs === null) phases.missing.push("return/shutdown timestamps missing or unordered");
  }
  const executor = timeline.filter((m) => m.model === handoff.model && Number.isFinite(m.at) &&
    Number.isFinite(handoff.at) && m.at >= handoff.at && (!back || m.at <= back.at));
  phases.executorAssistantSpanMs = executor.length >= 2 ? span(executor[0].at, executor.at(-1).at) : null;
  if (phases.executorAssistantSpanMs === null) phases.missing.push("fewer than two executor message starts inside model interval");
  return phases;
}

// Parse the indented "key: value" text a prewalk.status tool result carries.
// Nesting is two-space indentation; a blank value line opens a nested object.
// Malformed text throws so a bad status can never be treated as evidence.
export function parseIndentedStatus(text) {
  if (typeof text !== "string") throw new Error("Status text must be a string");
  const root = {};
  const stack = [{ indent: -1, value: root }];
  for (const rawLine of text.split("\n")) {
    const line = rawLine.replace(/\r$/, "");
    if (!line.trim()) continue;
    const indent = line.length - line.trimStart().length;
    const content = line.trim();
    const separator = content.indexOf(":");
    if (separator <= 0) throw new Error(`Malformed status line: ${JSON.stringify(line)}`);
    const key = content.slice(0, separator).trim();
    const value = content.slice(separator + 1).trim();
    while (stack.length > 1 && indent <= stack[stack.length - 1].indent) stack.pop();
    if (value === "") {
      const child = {};
      stack[stack.length - 1].value[key] = child;
      stack.push({ indent, value: child });
    } else {
      stack[stack.length - 1].value[key] = value;
    }
  }
  return root;
}

// A prewalk.status observation may arrive as bare indented text, nested inside
// a batched Fabric envelope (descriptions list, a `status:` block, a helper
// appendix), or as a JSON envelope. Only a structurally valid status that opens
// with the `state:` field and carries a `runtime` object is accepted: a quoted
// block scalar, a descriptions-only projection, foreign paths or malformed text
// yield null so the caller records "unobserved" instead of judging text that
// merely contains matching keys.
const statusShape = (value) =>
  object(value) && typeof value.state === "string" && value.state.length > 0 && object(value.runtime)
    ? value
    : null;

const tryParseStatus = (text) => {
  const opener = text.split("\n").find((line) => line.trim().length > 0);
  if (opener === undefined || !opener.trimStart().startsWith("state:")) return null;
  try {
    return statusShape(parseIndentedStatus(text));
  } catch {
    return null;
  }
};

export function extractPrewalkStatus(text) {
  if (typeof text !== "string") return null;
  const trimmed = text.trim();
  if (trimmed.startsWith("{")) {
    try {
      const parsed = JSON.parse(trimmed);
      return statusShape(object(parsed) && object(parsed.status) ? parsed.status : parsed);
    } catch {
      return null;
    }
  }
  const direct = tryParseStatus(text);
  if (direct !== null) return direct;
  const lines = text.split("\n");
  for (let index = 0; index < lines.length; index += 1) {
    if (lines[index].replace(/\r$/, "").trim() !== "status:") continue;
    const indent = lines[index].length - lines[index].trimStart().length;
    const block = [];
    for (let scan = index + 1; scan < lines.length; scan += 1) {
      const line = lines[scan].replace(/\r$/, "");
      if (!line.trim()) { block.push(""); continue; }
      const lineIndent = line.length - line.trimStart().length;
      if (lineIndent <= indent) break;
      // Dedent one level so the block parses like a standalone status text.
      block.push(line.slice(Math.min(lineIndent, indent + 2)));
    }
    const nested = tryParseStatus(block.join("\n"));
    if (nested !== null) return nested;
  }
  return null;
}

// --- Opt-in request contract (fixture runs) --------------------------------
// A frozen JSON contract lets the recorder project message text into bounded
// marker positions (never raw prompt text) and lets the verifier assert what
// the executor's requests carried. Markers are declared by the probe, so
// contracts stay provider-agnostic and privacy-bounded.
const MAX_CONTRACT_MARKERS = 32;
const MAX_MARKER_LENGTH = 200;
const MAX_SCAN_MATCHES = 64;
const MAX_SCAN_CHARS = 2_000_000;
const MARKER_NAME = /^[a-z0-9][a-z0-9-]*$/;
const CONTRACT_FIELDS = ["markers", "exactlyOnce", "present", "ordered", "absentBefore"];

const markerNameList = (value, key, markers, source) => {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throw new Error(`${source}: ${key} must be an array of marker names`);
  const names = [...new Set(value)];
  for (const name of names) {
    if (typeof name !== "string" || !Object.prototype.hasOwnProperty.call(markers, name)) {
      throw new Error(`${source}: ${key} references undeclared marker ${JSON.stringify(name)}`);
    }
  }
  return names;
};

export function parseRequestContract(text, source = "request contract") {
  let parsed;
  try { parsed = JSON.parse(text); } catch { throw new Error(`${source}: invalid JSON`); }
  if (!object(parsed)) throw new Error(`${source}: expected a JSON object`);
  for (const key of Object.keys(parsed)) {
    if (!CONTRACT_FIELDS.includes(key)) throw new Error(`${source}: unknown field ${JSON.stringify(key)}`);
  }
  if (!object(parsed.markers)) throw new Error(`${source}: markers object required`);
  const markers = {};
  for (const [name, value] of Object.entries(parsed.markers)) {
    if (!MARKER_NAME.test(name)) throw new Error(`${source}: invalid marker name ${JSON.stringify(name)}`);
    if (typeof value !== "string" || value.length === 0 || value.length > MAX_MARKER_LENGTH) {
      throw new Error(`${source}: marker ${name} must be a 1..${MAX_MARKER_LENGTH} character string`);
    }
    markers[name] = value;
  }
  const count = Object.keys(markers).length;
  if (count === 0 || count > MAX_CONTRACT_MARKERS) throw new Error(`${source}: 1..${MAX_CONTRACT_MARKERS} markers required`);
  const contract = {
    markers,
    exactlyOnce: markerNameList(parsed.exactlyOnce, "exactlyOnce", markers, source),
    present: markerNameList(parsed.present, "present", markers, source),
    ordered: markerNameList(parsed.ordered, "ordered", markers, source),
    absentBefore: markerNameList(parsed.absentBefore, "absentBefore", markers, source),
  };
  if (contract.exactlyOnce.some((name) => contract.present.includes(name))) {
    throw new Error(`${source}: a marker cannot be both exactlyOnce and present`);
  }
  return contract;
}

// Bounded positional projection: only user and custom message text is scanned;
// output is [messageIndex, partIndex, offset] per occurrence, never message
// text. Roles outside user/custom (system, assistant, toolResult) are ignored.
export function scanRequestMessages(messages, markers, limits = {}) {
  const maxMatches = limits.maxMatches ?? MAX_SCAN_MATCHES;
  const maxChars = limits.maxChars ?? MAX_SCAN_CHARS;
  const matches = {};
  for (const name of Object.keys(markers)) matches[name] = [];
  let scanned = 0;
  let truncated = false;
  for (let index = 0; index < messages.length; index += 1) {
    const message = messages[index];
    if (!object(message) || (message.role !== "user" && message.role !== "custom")) continue;
    const parts = typeof message.content === "string"
      ? [message.content]
      : Array.isArray(message.content)
        ? message.content.map((part) => object(part) && part.type === "text" && typeof part.text === "string" ? part.text : null)
        : [];
    for (let partIndex = 0; partIndex < parts.length; partIndex += 1) {
      const text = parts[partIndex];
      if (text === null || text === undefined) continue;
      if (scanned + text.length > maxChars) return { matches, truncated: true };
      scanned += text.length;
      for (const [name, literal] of Object.entries(markers)) {
        let from = 0;
        for (;;) {
          const at = text.indexOf(literal, from);
          if (at < 0) break;
          if (matches[name].length >= maxMatches) { truncated = true; break; }
          matches[name].push([index, partIndex, at]);
          from = at + literal.length;
        }
      }
    }
  }
  return { matches, truncated };
}

const comparePositions = (a, b) => {
  for (let index = 0; index < a.length; index += 1) {
    if (a[index] !== b[index]) return a[index] - b[index];
  }
  return 0;
};

// Recorded marker positions come from scanRequestMessages: ascending
// [messageIndex, partIndex, offset] triples of nonnegative safe integers.
// Anything else is tampered or foreign evidence — never evaluate it.
const malformedMatchPositions = (list) => {
  if (!Array.isArray(list)) return true;
  let previous = null;
  for (const position of list) {
    if (!Array.isArray(position) || position.length !== 3
      || position.some((value) => !Number.isSafeInteger(value) || value < 0)) return true;
    if (previous !== null && comparePositions(position, previous) < 0) return true;
    previous = position;
  }
  return false;
};

// Verifier-side evidence shape: contiguous request indices, matching contract
// sha, an untruncated scan, a recorded list for every declared marker, and
// well-formed positions.
export function requestContractEvidenceProblems(records, contract, contractSha256) {
  const problems = [];
  records.forEach((record, index) => {
    const label = `request ${index + 1}`;
    if (record.requestIndex !== index + 1) problems.push(`${label} carries requestIndex ${JSON.stringify(record.requestIndex)}`);
    const evidence = record.requestEvidence;
    if (!object(evidence)) { problems.push(`${label}: requestEvidence missing`); return; }
    if (evidence.contractSha256 !== contractSha256) problems.push(`${label}: contract sha256 mismatch`);
    if (evidence.layout !== "context") problems.push(`${label}: unexpected layout ${JSON.stringify(evidence.layout)}`);
    if (evidence.truncated !== false) problems.push(`${label}: marker scan truncated`);
    const matches = object(evidence.matches) ? evidence.matches : null;
    if (!matches) { problems.push(`${label}: matches missing`); return; }
    for (const name of Object.keys(contract.markers)) {
      if (!Array.isArray(matches[name])) { problems.push(`${label}: marker ${name} not recorded`); continue; }
      if (malformedMatchPositions(matches[name])) problems.push(`${label}: marker ${name} positions malformed`);
    }
  });
  return problems;
}

// Payload semantics: exactly-once and present markers must hold in every
// executor request, ordered markers must first appear in order, and
// absentBefore markers must not appear before the executor switch.
export function requestContractPayloadProblems(records, contract, executorModel) {
  const problems = [];
  if (records.length === 0) return ["no request_context records"];
  const executorRecords = records.filter((record) => record.model === executorModel);
  if (executorRecords.length === 0) return [`no request_context record for executor ${executorModel}`];
  const firstExecutorIndex = executorRecords[0].requestIndex;
  // Malformed or unrecorded positions make payload semantics untrustworthy:
  // report the malformation without evaluating counts or ordering over it.
  const malformed = [];
  for (const record of records) {
    const matches = object(record.requestEvidence?.matches) ? record.requestEvidence.matches : null;
    for (const name of Object.keys(contract.markers)) {
      if (malformedMatchPositions(matches ? matches[name] : undefined)) {
        malformed.push(`request ${record.requestIndex}: marker ${JSON.stringify(name)} positions malformed or unrecorded`);
      }
    }
  }
  if (malformed.length > 0) return malformed;
  const count = (record, name) => {
    const list = record.requestEvidence?.matches?.[name];
    return Array.isArray(list) ? list.length : 0;
  };
  for (const record of executorRecords) {
    for (const name of contract.exactlyOnce) {
      const seen = count(record, name);
      if (seen !== 1) problems.push(`request ${record.requestIndex}: ${JSON.stringify(name)} appears ${seen} times (exactly once required)`);
    }
    for (const name of contract.present) {
      if (count(record, name) === 0) problems.push(`request ${record.requestIndex}: ${JSON.stringify(name)} missing`);
    }
  }
  let previous = null;
  for (const name of contract.ordered) {
    let position = null;
    for (const record of executorRecords) {
      const rows = record.requestEvidence?.matches?.[name];
      if (Array.isArray(rows) && rows.length > 0) {
        position = [record.requestIndex, rows[0][0], rows[0][1], rows[0][2]];
        break;
      }
    }
    if (position === null) { problems.push(`ordered marker ${JSON.stringify(name)} missing from executor requests`); continue; }
    if (previous !== null && comparePositions(position, previous) <= 0) {
      problems.push(`ordered marker ${JSON.stringify(name)} appears before the previous ordered marker`);
    }
    previous = position;
  }
  for (const record of records) {
    if (record.requestIndex >= firstExecutorIndex) continue;
    for (const name of contract.absentBefore) {
      if (count(record, name) > 0) problems.push(`request ${record.requestIndex}: ${JSON.stringify(name)} present before the executor switch`);
    }
  }
  return problems;
}

export const CHECK_STATUSES = ["pass", "fail", "unobserved"];

// Tri-state required-check ledger. A missing observation is neither a pass nor
// a failure; ok() requires every recorded check to be an observed pass, so an
// unobserved required check blocks completion exactly like a failure.
export function createCheckLedger() {
  const checks = [];
  const names = new Set();
  const snapshot = () => checks.map((check) => ({ ...check }));
  const ok = () => checks.length > 0 && checks.every((check) => check.status === "pass");
  const axes = () => {
    const grouped = {};
    for (const check of checks) {
      const axis = check.axis ?? "lifecycle";
      grouped[axis] = grouped[axis] ?? [];
      grouped[axis].push({ name: check.name, status: check.status });
    }
    return grouped;
  };
  return {
    // `axis` keeps three separate questions from collapsing into one another:
    // runtime/handoff lifecycle, write-scope compliance, and artifact quality.
    // A clean child exit says nothing about whether its artifact passed a test.
    add(name, status, detail = null, axis = "lifecycle") {
      if (typeof name !== "string" || name.length === 0) throw new Error("Check needs a name");
      if (!CHECK_STATUSES.includes(status)) throw new Error(`Invalid check status: ${status}`);
      if (typeof axis !== "string" || axis.length === 0) throw new Error(`Invalid check axis: ${axis}`);
      if (names.has(name)) throw new Error(`Duplicate check: ${name}`);
      names.add(name);
      checks.push({ name, status, detail, axis });
      return status === "pass";
    },
    checks: snapshot,
    axes,
    ok,
    toJSON: () => ({ ok: ok(), checks: snapshot(), axes: axes() }),
  };
}

// --- Opt-in independent task verification ----------------------------------
// A receipt is diagnostic evidence produced by the runner or an equivalent
// harness. A spec therefore only names a relative path inside the work
// directory: no command text ever comes from the cell, so task verification can
// never become an execution channel for the model under test.
const SAFE_RELATIVE = /^(?!\/)(?!.*(?:^|\/)\.\.(?:\/|$))[A-Za-z0-9._/-]+$/;

export function parseTaskCheckSpec(text, source = "task-check spec") {
  let parsed;
  try { parsed = JSON.parse(text); } catch { throw new Error(`${source}: not valid JSON`); }
  if (!object(parsed)) throw new Error(`${source}: expected a JSON object`);
  for (const key of Object.keys(parsed)) {
    if (key !== "testFile" && key !== "artifact") throw new Error(`${source}: unknown field ${JSON.stringify(key)}`);
  }
  const relative = (value, name) => {
    if (typeof value !== "string" || !SAFE_RELATIVE.test(value)) {
      throw new Error(`${source}: ${name} must be a relative POSIX path inside the work directory`);
    }
    return value;
  };
  const testFile = relative(parsed.testFile, "testFile");
  if (!/\.(?:mjs|cjs|js|ts)$/.test(testFile)) throw new Error(`${source}: testFile must be a Node test module`);
  return { testFile, artifact: parsed.artifact === undefined ? testFile : relative(parsed.artifact, "artifact") };
}

// The artifact verdict is the receipt's own axis: a lifecycle-clean run with
// failing tests reports here, never as a runner problem. `artifactSha256`
// re-binds the receipt to current content (null = missing, undefined = not
// re-checked).
export function taskCheckReceiptProblems(receipt, options = {}) {
  if (!object(receipt)) return ["receipt is not an object"];
  const problems = [];
  const counts = object(receipt.counts) ? receipt.counts : null;
  if (counts === null) problems.push("counts missing");
  else {
    for (const key of ["tests", "pass", "fail"]) {
      if (!Number.isInteger(counts[key]) || counts[key] < 0) problems.push(`counts.${key} missing`);
    }
  }
  if (!Object.prototype.hasOwnProperty.call(receipt, "exitCode")) problems.push("exitCode missing");
  if (receipt.ok !== true) problems.push("receipt.ok is not true");
  if (receipt.exitCode !== 0) problems.push(`test command exited ${JSON.stringify(receipt.exitCode ?? null)}`);
  if (counts !== null && Number.isInteger(counts.tests) && Number.isInteger(counts.fail) && Number.isInteger(counts.pass)) {
    if (counts.tests <= 0) problems.push("no tests were executed");
    else if (counts.fail > 0) problems.push(`tests failed: ${counts.fail} of ${counts.tests}`);
    if (counts.pass + counts.fail > counts.tests) problems.push("counts.pass + counts.fail exceeds counts.tests");
  }
  const artifact = object(receipt.artifact) ? receipt.artifact : null;
  if (artifact === null) problems.push("artifact binding missing");
  else {
    if (typeof artifact.path !== "string" || artifact.path.length === 0) problems.push("artifact.path missing");
    if (!/^[0-9a-f]{64}$/.test(String(artifact.sha256After ?? ""))) problems.push("artifact.sha256After malformed");
    if (artifact.unchangedDuringCheck !== true) problems.push("artifact changed while its check ran");
    if (options.artifactSha256 === null) problems.push("artifact missing at re-verification");
    else if (options.artifactSha256 !== undefined && options.artifactSha256 !== artifact.sha256After) {
      problems.push("artifact content changed after the check ran");
    }
  }
  return problems;
}

export function analyzeCell({ name, liveEvents, persistedEntries = [], telemetryEvents = [], compactionArrival = null }) {
  const live = livePrewalkMessages(liveEvents), persisted = persistedPrewalkMessages(persistedEntries);
  const timeline = assistantTimeline(liveEvents), telemetry = telemetryTimeline(telemetryEvents);
  return { name, prewalkMessages: mergePrewalkMessages(live, persisted),
    perModel: aggregateUsage(timeline, toolResultUsages(liveEvents)),
    phases: attributePhases({ timeline, telemetry, compactionArrival }), assistantCount: timeline.length,
    modelSelects: telemetry.modelSelects, compactionEvents: telemetry.compaction,
    livePrewalkCount: live.length, persistedPrewalkCount: persisted.length };
}
