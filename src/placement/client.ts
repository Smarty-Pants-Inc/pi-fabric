/** Exact v1 SpawnIntent. Quality pins and needs are preserved, never downgraded or sorted. */
export type PlacementIntent = {
  v: 1;
  runId: string;
  kind: "process" | "remote" | "sandbox";
  org: string;
  parentSession: string;
  role: string;
  complexity: "simple" | "normal" | "complex" | "delicate";
  model: string;
  thinking: "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";
  needs: readonly string[];
  repository: string;
  cwd: string;
  expectedSeconds: number;
  timeoutSeconds: number;
  deadline: string | null;
};

const refusalCodes = ["INTENT_INVALID", "POLICY_DENIED", "UNSUPPORTED_NEED", "DEADLINE_EXPIRED"] as const;
const delayCodes = ["CAPACITY_BUSY", "CI_BUSY", "FACTS_STALE", "QUOTA_EMPTY", "PLACEMENT_UNAVAILABLE", "START_PENDING", "TEMPLATE_CONCURRENCY"] as const;
const errorCodes = ["NOT_FOUND", "RUN_ID_CONFLICT", "METHOD_NOT_ALLOWED", "PLACEMENT_UNAVAILABLE"] as const;
const localCodes = [
  "LOCAL_TIMEOUT", "LOCAL_NETWORK_ERROR",
  "LOCAL_PROTOCOL_ERROR", "LOCAL_WATCH_ENDED", "LOCAL_KIND_UNKNOWN", "LOCAL_INTENT_INVALID",
  "LOCAL_TERMINAL_WRITER_UNAVAILABLE", "LOCAL_BINDING_NOT_ADMITTED", "LOCAL_BINDING_MISMATCH", "LOCAL_TRANSPORT_UNSUPPORTED",
] as const;
type LocalCode = typeof localCodes[number];
type RefusalCode = typeof refusalCodes[number] | typeof errorCodes[number] | LocalCode;
type ClaimRef = { namespace: "smarty-island"; name: string; uid: string };

/** Discriminated by status. No server detail, raw body, exception or credential is exposed. */
export type PlacementOutcome =
  | { status: "refused"; code: RefusalCode }
  | { status: "delayed"; retryAfterSeconds: number; code: typeof delayCodes[number] }
  | { status: "admitted"; host: string; claimRef: ClaimRef | null; claimUID: string | null; podUID: string | null };

type Refused = Extract<PlacementOutcome, { status: "refused" }>;
type AdmittedBindingFields = {
  claimRef: ClaimRef | null;
  claimUID: string | null;
  podUID: string | null;
  podName: string | null;
  host: string;
  org: string;
  role: string;
  model: string;
  provider: string | null;
  thinking: PlacementIntent["thinking"];
  expectedSeconds: number;
  deadline: string | null;
  executionDeadline: string;
  admittedAt: string;
};
/** Exact sixteen-field projection. parentSession is internal and is never accepted or returned. */
export type PlacementBinding = { runId: string } & (
  | ({ status: "admitted" } & AdmittedBindingFields)
  | ({ status: "delayed" | "refused" } & { [K in keyof AdmittedBindingFields]: null })
);
export type PlacementBindingOutcome = PlacementBinding | Refused;
const refuse = (code: RefusalCode): Refused => ({ status: "refused", code });
const intentKeys = ["v", "runId", "kind", "org", "parentSession", "role", "complexity", "model", "thinking", "needs", "repository", "cwd", "expectedSeconds", "timeoutSeconds", "deadline"] as const;
const resultKeys = ["runId", "status", "code", "detail", "retryAfterSeconds", "host", "claimRef", "claimUID", "podUID"];
const bindingKeys = ["runId", "status", "claimRef", "claimUID", "podUID", "podName", "host", "org", "role", "model", "provider", "thinking", "expectedSeconds", "deadline", "executionDeadline", "admittedAt"];
const knownProviders = ["anthropic", "azure", "bedrock", "cohere", "deepseek", "google", "gemini", "groq", "mistral", "ollama", "openai", "openrouter", "qwen", "vertex", "xai", "cliproxyapi"] as const;
const runPattern = /^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$/;
const namePattern = /^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}$/;
const hostPattern = /^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/;
const maxResultBytes = 16_384; // More than the maximum UTF-8 nine-key result; also bounds each NDJSON line.
const maxWatchEvents = 16; // Contract default; never consume an unbounded stream.

class LocalFailure extends Error {
  constructor(readonly code: LocalCode) { super(code); }
}
const invalid = (): never => { throw new LocalFailure("LOCAL_PROTOCOL_ERROR"); };
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const keysAre = (value: unknown, keys: readonly string[]): value is Record<string, unknown> =>
  record(value) && Reflect.ownKeys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
// JS $ also matches before a final LF; require a true full match, like Python re.fullmatch.
const matches = (value: unknown, pattern: RegExp, maximum = Infinity): value is string =>
  typeof value === "string" && value.length <= maximum && pattern.exec(value)?.[0] === value;
const member = <T extends string>(value: unknown, options: readonly T[]): value is T =>
  typeof value === "string" && (options as readonly string[]).includes(value);
const integer = (value: unknown, max: number): value is number => typeof value === "number" && Number.isInteger(value) && value >= 1 && value <= max;
const detailValid = (value: unknown): boolean => typeof value === "string" && value.length > 0 && [...value].length <= 1024 && !/[\u0000-\u001f]/.test(value);
const timestampValid = (value: unknown): value is string =>
  matches(value, /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$/)
  && !value.startsWith("0000") && Number.isFinite(Date.parse(value))
  && new Date(value).toISOString() === value.replace("Z", ".000Z");
const sameIntent = (a: PlacementIntent, b: PlacementIntent): boolean => intentKeys.every(key => key === "needs"
  ? a.needs.length === b.needs.length && a.needs.every((need, i) => need === b.needs[i])
  : a[key] === b[key]);

/** Native JSON syntax validation plus a depth-bounded walk rejecting duplicate (also escaped) keys. */
function strictJson(text: string): unknown {
  let value: unknown;
  try { value = JSON.parse(text); } catch { return invalid(); }
  let position = 0;
  const whitespace = () => { while (/[ \t\r\n]/.test(text[position] ?? "!") && position < text.length) position++; };
  const string = (): string => {
    const start = position++;
    while (position < text.length) {
      const char = text[position++];
      if (char === "\\") position++;
      else if (char === '"') return JSON.parse(text.slice(start, position)) as string;
    }
    return invalid();
  };
  const visit = (depth: number): void => {
    if (depth > 16) invalid();
    whitespace();
    const char = text[position];
    if (char === '"') { string(); return; }
    if (char === "{" || char === "[") {
      position++;
      whitespace();
      const end = char === "{" ? "}" : "]";
      const seen = new Set<string>();
      if (text[position] !== end) {
        while (true) {
          whitespace();
          if (char === "{") {
            const key = string();
            if (seen.has(key)) invalid();
            seen.add(key);
            whitespace();
            position++; // Colon; native JSON.parse has already checked all syntax.
          }
          visit(depth + 1);
          whitespace();
          if (text[position] !== ",") break;
          position++;
        }
      }
      position++;
    } else {
      const start = position;
      while (position < text.length && !/[\s,\]}]/.test(text[position]!)) position++;
      const literal = text.slice(start, position);
      const primitive: unknown = JSON.parse(literal);
      // contract.py distinguishes JSON ints from floats, including 15.0/15e0.
      if (typeof primitive === "number" && (!Number.isFinite(primitive) || /[.eE]/.test(literal))) invalid();
    }
  };
  visit(0);
  return value;
}

function serializeIntent(intent: PlacementIntent): { body: string; intent: PlacementIntent } | undefined {
  try {
    if (!keysAre(intent, intentKeys)) return;
    const body = JSON.stringify(intent);
    const value = strictJson(body);
    if (!keysAre(value, intentKeys) || value.v !== 1 || !matches(value.runId, runPattern)
      || !member(value.kind, ["process", "remote", "sandbox"])
      || !member(value.complexity, ["simple", "normal", "complex", "delicate"])
      || !member(value.thinking, ["off", "minimal", "low", "medium", "high", "xhigh", "max"])) return;
    for (const key of ["org", "role", "parentSession", "model"]) if (!matches(value[key], namePattern)) return;
    const needs = value.needs;
    if (!Array.isArray(needs) || needs.length > 64
      || !needs.every((need: unknown, i: number) => matches(need, namePattern) && (i === 0 || needs[i - 1] < need))) return;
    if (!matches(value.repository, /^https:\/\/[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\/[A-Za-z0-9_-][A-Za-z0-9_.-]*\/[A-Za-z0-9_-][A-Za-z0-9_.-]*$/, 512)) return;
    if (!matches(value.cwd, /^(?:\.|[A-Za-z0-9_.@+-][A-Za-z0-9_.@+ -]*(?:\/[A-Za-z0-9_.@+-][A-Za-z0-9_.@+ -]*)*)$/, 1024)
      || (value.cwd !== "." && value.cwd.split("/").some(part => part === "." || part === ".." || part.endsWith(" ")))) return;
    if (!integer(value.expectedSeconds, 604800) || !integer(value.timeoutSeconds, 604800) || value.expectedSeconds > value.timeoutSeconds) return;
    if (value.deadline !== null && !timestampValid(value.deadline)) return;
    // All permitted intent characters are ASCII; compact key order does not change canonical byte length.
    if (Buffer.byteLength(body) > 4096) return;
    return { body, intent: value as PlacementIntent };
  } catch { return; }
}

function validateResult(value: unknown, runId: string, kind: PlacementIntent["kind"] | undefined): PlacementOutcome {
  if (!keysAre(value, resultKeys) || value.runId !== runId || !detailValid(value.detail)) return invalid();
  let claim: ClaimRef | null = null;
  if (value.claimRef !== null) {
    if (!keysAre(value.claimRef, ["namespace", "name", "uid"]) || value.claimRef.namespace !== "smarty-island"
      || value.claimRef.name !== `spawn-${runId}` || !matches(value.claimRef.uid, namePattern)) return invalid();
    claim = { namespace: "smarty-island", name: `spawn-${runId}`, uid: value.claimRef.uid };
  }
  if (value.claimUID !== (claim?.uid ?? null)
    || (value.podUID !== null && (!matches(value.podUID, namePattern) || value.status !== "admitted" || claim === null))
    || (kind !== undefined && kind !== "sandbox" && claim !== null)) return invalid();
  if (value.status === "refused" && member(value.code, refusalCodes)) {
    if (value.retryAfterSeconds !== null || value.host !== null) return invalid();
    return refuse(value.code);
  }
  if (value.status === "delayed" && member(value.code, delayCodes)) {
    if (!integer(value.retryAfterSeconds, 3600) || value.host !== null) return invalid();
    return { status: "delayed", code: value.code, retryAfterSeconds: value.retryAfterSeconds };
  }
  if (value.status === "admitted" && value.code === "ADMITTED") {
    if (value.retryAfterSeconds !== null || !matches(value.host, hostPattern, 253)
      || (claim !== null && value.podUID === null)
      || (kind === "sandbox" && (claim === null || value.podUID === null))) return invalid();
    if (kind === undefined) return refuse("LOCAL_KIND_UNKNOWN");
    // Detail is discarded; only the canonical admission projection is exposed to callers.
    return { status: "admitted", host: value.host, claimRef: claim, claimUID: claim?.uid ?? null, podUID: value.podUID as string | null };
  }
  return invalid();
}

function validateBinding(value: unknown, runId: string, intent: PlacementIntent | undefined): PlacementBindingOutcome {
  if (!keysAre(value, bindingKeys) || value.runId !== runId || !member(value.status, ["admitted", "delayed", "refused"])) return invalid();
  if (value.status !== "admitted") {
    if (bindingKeys.slice(2).some(key => value[key] !== null)) return invalid();
    return value as PlacementBinding;
  }
  if (!timestampValid(value.admittedAt) || !timestampValid(value.executionDeadline)
    || !matches(value.host, hostPattern, 253)) return invalid();
  for (const key of ["org", "role", "model"]) if (!matches(value[key], namePattern)) return invalid();
  if (!member(value.thinking, ["off", "minimal", "low", "medium", "high", "xhigh", "max"])
    || !integer(value.expectedSeconds, 604800) || (value.deadline !== null && !timestampValid(value.deadline))) return invalid();
  const model = value.model as string;
  const prefix = model.slice(0, model.indexOf("/"));
  const provider = model.includes("/") && member(prefix, knownProviders) ? prefix : null;
  if (value.provider !== provider) return invalid();
  const deadline = Math.min(Date.parse(value.admittedAt) + value.expectedSeconds * 1000,
    value.deadline === null ? Infinity : Date.parse(value.deadline as string));
  if (Date.parse(value.executionDeadline) !== deadline) return invalid();
  if (value.claimRef === null) {
    if (value.claimUID !== null || value.podUID !== null || value.podName !== null) return invalid();
  } else if (!keysAre(value.claimRef, ["namespace", "name", "uid"]) || value.claimRef.namespace !== "smarty-island"
    || value.claimRef.name !== `spawn-${runId}` || !matches(value.claimRef.uid, namePattern)
    || value.claimUID !== value.claimRef.uid || !matches(value.podUID, namePattern) || !matches(value.podName, hostPattern, 253)) return invalid();
  // The wire intentionally has no kind. Reads cannot authorize an unknown original intent.
  if (intent === undefined) return refuse("LOCAL_KIND_UNKNOWN");
  if ((intent.kind === "sandbox") !== (value.claimRef !== null)
    || (["org", "role", "model", "thinking", "expectedSeconds", "deadline"] as const).some(key => value[key] !== intent[key])) return invalid();
  return value as PlacementBinding;
}

const mediaType = (response: Response, watch: boolean): boolean => {
  const type = response.headers.get("content-type");
  return type !== null && matches(type, watch
    ? /^application\/x-ndjson(?:\s*;\s*charset=utf-8)?$/i
    : /^application\/json(?:\s*;\s*charset=utf-8)?$/i);
};
const decode = (bytes: Uint8Array): unknown => {
  try { return strictJson(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes)); }
  catch { return invalid(); }
};

async function readJson(response: Response, signal: AbortSignal): Promise<unknown> {
  if (!mediaType(response, false) || !response.body) return invalid();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const next = await reader.read();
      signal.throwIfAborted();
      if (next.done) break;
      size += next.value.length;
      if (size > maxResultBytes) return invalid();
      chunks.push(next.value);
    }
    return decode(Buffer.concat(chunks, size));
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

/** LF-delimited bytes: only decode complete, size-bounded lines; split UTF-8 sequences stay intact. */
async function* readLines(response: Response, signal: AbortSignal): AsyncGenerator<unknown> {
  if (!mediaType(response, true) || !response.body) return invalid();
  const reader = response.body.getReader();
  let chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const next = await reader.read();
      signal.throwIfAborted();
      if (next.done) {
        if (size !== 0) invalid(); // No admission from a truncated/unframed last line.
        return;
      }
      let start = 0;
      for (let i = 0; i <= next.value.length; i++) {
        if (i !== next.value.length && next.value[i] !== 10) continue;
        const part = next.value.subarray(start, i);
        size += part.length;
        if (size > maxResultBytes) invalid();
        chunks.push(part);
        if (i < next.value.length) {
          signal.throwIfAborted();
          yield decode(Buffer.concat(chunks, size));
          chunks = [];
          size = 0;
        }
        start = i + 1;
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

async function httpError(response: Response, signal: AbortSignal): Promise<Refused> {
  const value = await readJson(response, signal);
  const codes: Record<number, RefusalCode> = {
    400: "INTENT_INVALID", 403: "POLICY_DENIED", 404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED", 409: "RUN_ID_CONFLICT", 413: "INTENT_INVALID", 415: "INTENT_INVALID", 503: "PLACEMENT_UNAVAILABLE",
  };
  const code = codes[response.status];
  if (!code || !keysAre(value, ["code", "detail"]) || value.code !== code || !detailValid(value.detail)) return invalid();
  return refuse(code);
}
const failure = (error: unknown, signal: AbortSignal): Refused =>
  refuse(signal.aborted ? "LOCAL_TIMEOUT" : error instanceof LocalFailure ? error.code : "LOCAL_NETWORK_ERROR");

/**
 * Contract-only client: no launch, retries, picker fallback, credentials or mint calls.
 * The Placement alpha uses one plain-HTTP service or tailnet NodePort.
 * Invalid constructor options throw only a fixed TypeError.
 *
 * requestSpawn snapshots the first validated intent before sending (including uncertain POSTs).
 * A new client can read existing runs, but refuses admissions with LOCAL_KIND_UNKNOWN:
 * v1 reads alone cannot prove the original intent-kind and quality-pin invariants.
 */
export class PlacementClient {
  readonly #url: string;
  readonly #requestTimeoutMs: number;
  readonly #watchTimeoutMs: number;
  readonly #intents = new Map<string, PlacementIntent>();

  constructor(options: { url: string; requestTimeoutMs: number; watchTimeoutMs: number }) {
    try {
      const url = new URL(options.url);
      if (url.protocol !== "http:" || url.username || url.password || url.pathname !== "/" || url.search || url.hash
        || !integer(options.requestTimeoutMs, 2_147_483_647) || !integer(options.watchTimeoutMs, 2_147_483_647)) throw new Error();
      this.#url = url.origin;
      this.#requestTimeoutMs = options.requestTimeoutMs;
      this.#watchTimeoutMs = Math.min(options.watchTimeoutMs, 30_000);
    } catch { throw new TypeError("Invalid placement client configuration"); }
  }

  async requestSpawn(intent: PlacementIntent): Promise<PlacementOutcome> {
    const serialized = serializeIntent(intent);
    if (!serialized) return refuse("INTENT_INVALID");
    const { runId } = serialized.intent;
    // Preserve the original pins and kind even after uncertain POSTs or conflicting replays.
    if (!this.#intents.has(runId)) this.#intents.set(runId, serialized.intent);
    return this.#request("/v1/spawns", runId, serialized.body, serialized.intent);
  }

  async getSpawn(runId: string): Promise<PlacementOutcome> {
    if (!matches(runId, runPattern)) return refuse("INTENT_INVALID");
    return this.#request(`/v1/spawns/${runId}`, runId);
  }

  async getBinding(runId: string): Promise<PlacementBindingOutcome> {
    if (!matches(runId, runPattern)) return refuse("INTENT_INVALID");
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.#requestTimeoutMs);
    try {
      const response = await this.#fetch(`/v1/spawns/${runId}/binding`, controller.signal);
      if (response.status !== 200) return await httpError(response, controller.signal);
      return validateBinding(await readJson(response, controller.signal), runId, this.#intents.get(runId));
    } catch (error) { return failure(error, controller.signal); }
    finally { clearTimeout(timer); controller.abort(); }
  }

  async #request(path: string, runId: string, body?: string, submittedIntent?: PlacementIntent): Promise<PlacementOutcome> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.#requestTimeoutMs);
    try {
      const response = await this.#fetch(path, controller.signal, body);
      if (response.status !== 200) return await httpError(response, controller.signal);
      const value = await readJson(response, controller.signal);
      const original = this.#intents.get(runId);
      if (submittedIntent !== undefined && (original === undefined || !sameIntent(original, submittedIntent))) return invalid();
      return validateResult(value, runId, original?.kind);
    } catch (error) { return failure(error, controller.signal); }
    finally { clearTimeout(timer); controller.abort(); }
  }

  /**
   * One bounded NDJSON observation: <=16 outcomes and <=min(watchTimeoutMs, 30s).
   * Header wait also observes requestTimeoutMs. Terminal results stop immediately.
   * EOF/network/timeout without admission never grants permission; reconnect explicitly.
   * Early consumer return cancels the response. The deadline also aborts a paused consumer's socket.
   */
  async *watchSpawn(runId: string): AsyncGenerator<PlacementOutcome> {
    if (!matches(runId, runPattern)) { yield refuse("INTENT_INVALID"); return; }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.#watchTimeoutMs);
    const headersTimer = setTimeout(() => controller.abort(), this.#requestTimeoutMs);
    let events = 0;
    try {
      const seconds = Math.max(1, Math.ceil(this.#watchTimeoutMs / 1000));
      const response = await this.#fetch(`/v1/spawns/${runId}/watch?timeoutSeconds=${seconds}&maxEvents=${maxWatchEvents}`, controller.signal);
      clearTimeout(headersTimer);
      if (response.status !== 200) { yield await httpError(response, controller.signal); return; }
      for await (const value of readLines(response, controller.signal)) {
        controller.signal.throwIfAborted();
        const outcome = validateResult(value, runId, this.#intents.get(runId)?.kind);
        events++;
        const terminal = outcome.status !== "delayed" || events >= maxWatchEvents;
        if (terminal) controller.abort(); // Close before yielding, even if the consumer never resumes.
        yield outcome;
        if (terminal) return;
      }
      yield refuse("LOCAL_WATCH_ENDED");
    } catch (error) {
      if (events < maxWatchEvents) yield failure(error, controller.signal);
    } finally {
      clearTimeout(timer);
      clearTimeout(headersTimer);
      controller.abort();
    }
  }

  async #fetch(path: string, signal: AbortSignal, body?: string): Promise<Response> {
    const headers: Record<string, string> = {};
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      headers["Content-Length"] = String(Buffer.byteLength(body));
    }
    const response = await fetch(`${this.#url}${path}`, {
      method: body === undefined ? "GET" : "POST", headers, ...(body === undefined ? {} : { body }),
      signal, redirect: "manual", cache: "no-store", credentials: "omit",
    });
    return response;
  }
}
