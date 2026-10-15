/**
 * smarty-dev#8487, smarty-dev#6509: which activation failures may stop gating routing on their own.
 *
 * A block only expires for a clearly transient cause: a provider 5xx or throttling 429,
 * a stream error or network disconnect, a timeout, a preparation or lock timeout, a lost
 * activation window, a registry update veto that committed nothing, a child that exited before
 * model admission, or a missing directive reply
 * after one of those. Credential, permission, security-policy, quota, rejected-request (4xx
 * other than 429), context and other preparation failures (a model not allowed, invalid
 * instructions) stay blocked, and so does anything not recognised: fail closed.
 * A permanent cause found anywhere wins: every line of the failure text and every recorded
 * worker warning is scanned for one before any transient evidence is considered, so a
 * multiline failure whose last line is a 502 but whose earlier line is a 401 stays blocked.
 */
import type { ActorRegistryVetoReason } from "./registry-store.js";

export interface ActivationFailureClass {
  code: string;
  transient: boolean;
  /** Why: "transient: …", "permanent: …" or "fail-closed: …", with the matched text. */
  classification: string;
  /** Safe canonical diagnostic, when the cause must not expose its preparation wrapper. */
  reason?: string;
}

/** Codes whose block expires after the bounded backoff; every other code needs a completed run.
 * smarty-dev#6509: a preparation or lock timeout is transient too ("preparation-timeout"). */
export const TRANSIENT_ACTIVATION_BLOCK_CODES: ReadonlySet<string> = new Set([
  "provider-transient", "activation-window-lost", "child-exit-before-admission", "preparation-timeout", "registry-update-vetoed",
]);

/** smarty-dev#6509: a block's class. Derived from its code, never trusted from a stored record:
 * a transient block expires after its cooldown and gets a probe; a hard block (auth, config, a
 * model not allowed, invalid instructions, anything unrecognised) stays until a run completes,
 * the cause is fixed, or the actor is re-created. */
export type ActivationBlockClass = "transient" | "hard";
export const activationBlockClass = (code: string): ActivationBlockClass =>
  TRANSIENT_ACTIVATION_BLOCK_CODES.has(code) ? "transient" : "hard";

/** A deadline that ran out: a preparation, lock or provider timeout. */
const TIMEOUT = /\btimed out\b|\btimeout\b|\bETIMEDOUT\b|deadline exceeded/i;

type Rule = readonly [code: string, why: string, pattern: RegExp];

const PERMANENT: readonly Rule[] = [
  ["context-overflow", "context window exceeded", /Context exceeds window/i],
  ["auth", "credential or authentication failure",
    /\b401\b|\bunauthori[sz]ed\b|\bunauthenticated\b|authentication|\binvalid[ _-]?(?:api[ _-]?key|x-api-key|token|credentials?|grant)\b|\b(?:no|missing)\s+(?:api key|credentials?)\b|\b(?:api key|credentials?)\s+(?:was\s+)?not found\b|\btoken (?:has |was )?(?:expired|revoked)\b|\boauth\b|\bnot logged in\b|\blogin required\b|\bre-?login\b/i],
  ["permission", "permission denied", /\b403\b|\bforbidden\b|permission[ _]denied|insufficient[ _]permissions?|access[ _]denied|\bEACCES\b|\bEPERM\b/i],
  ["security-policy", "refused by a security or content policy",
    /security[ _-]polic|content[ _-]?(?:policy|filter)|policy[ _]violation|safety (?:system|filter)|\brefused by\b|blocked by (?:the )?(?:policy|guard|security)|prompt[ _]injection/i],
  ["quota", "account quota or billing limit",
    /insufficient_quota|quota (?:exceeded|reached)|exceeded (?:your )?(?:current )?quota|\bbilling\b|out of budget|available balance|usage limit reached|GoUsageLimitError|FreeUsageLimitError|spend(?:ing)? limit/i],
  ["invalid-request", "request rejected by the provider (4xx, not throttling)",
    /\b(?:400|404|405|409|410|413|415|422)\b|\bbad request\b|invalid[ _]request|unprocessable|not_found_error|model[ _]not[ _]found|unknown model|unsupported (?:model|parameter|value)|maximum context length|context.{0,20}(?:overflow|exceeded|too long)|prompt is too long|too many tokens/i],
];

/** A bare number is a status only with a cue; "500 ms" or "502 tokens" is not one. */
const NOT_A_STATUS = String.raw`(?!\s*(?:ms|s|seconds?|bytes|tokens|lines)\b)`;

const PROVIDER_TRANSIENT: readonly (readonly [why: string, pattern: RegExp])[] = [
  ["provider 5xx or overload", new RegExp(
    String.raw`\b(?:HTTP\/[\d.]+\s+|status(?:\s*code)?[:=\s]+|code[:=\s]+|error[:=\s]+|[:(]\s*)5\d\d\b${NOT_A_STATUS}|^\s*5\d\d\b${NOT_A_STATUS}` +
    String.raw`|\bbad gateway\b|service[ _]unavailable|gateway time-?out|internal server error|\bserver_error\b|\boverloaded(?:_error)?\b|server_is_overloaded` +
    String.raw`|currently experiencing high demand|upstream connect error|reset before headers`, "im")],
  ["provider throttling (429)", /\b429\b|too many requests|rate[ _-]?limit(?:ed|_error|_exceeded)?\b|\bthrottl/i],
  ["model stream disconnect",
    /premature(?:ly)? clos(?:e|ed)|stream[^\n]{0,80}(?:disconnect|clos(?:e|ed)|ended before|ended without|interrupted)|\bstream(?:ing)?[ _-]?error\b|other side closed|socket hang up|socket connection was closed|websocket (?:error|closed)|http2 request did not get a response|(?:^|[:·]\s*)terminated\b/im],
  ["network failure",
    /fetch failed|network[ _]?error|connection (?:reset|closed|lost|error|refused)|\bECONNRESET\b|\bECONNREFUSED\b|\bEPIPE\b|\bENETUNREACH\b|\bEHOSTUNREACH\b|\bENOTFOUND\b|\bEAI_AGAIN\b|getaddrinfo/i],
  ["timeout", TIMEOUT],
];

/** Worker evidence that the run hit a provider or stream error before it ended. */
const PROVIDER_RECOVERY_EVIDENCE = /Pi provider recovery|interrupted by a model stream error/i;

const quote = (match: string): string => JSON.stringify(match.trim().slice(0, 80));

/** A V8 stack frame is context for the error above it; its path or line number is not a cause. */
const STACK_FRAME = /^at\s/;

/** The worker quotes at most 200 characters of model prose, then "…" and ")", as a final-text or
 * output snippet (src/worker.ts). That prose is not evidence of a cause. For the permanent scan
 * only the quote is cut (up to the first ")" that ends a line, else the rest of its line), so a
 * real error line after it is still scanned; any prose left over can only keep a block. */
const MODEL_TEXT_QUOTE = /\s*\((?:final text|output): (?:[\s\S]{0,201}?\)(?=[ \t]*(?:\r?\n|$))|[^\n]*)/gi;
/** For transient evidence everything from the first quote on is cut: fail closed. */
const withoutModelText = (text: string): string =>
  text.replace(/\s*\((?:final text|output): [\s\S]*$/i, "");

/** The non-empty, non-stack-frame lines of a failure text. */
const lines = (text: string): string[] =>
  text.split(/\r?\n/).map(line => line.trim()).filter(line => line && !STACK_FRAME.test(line));

const permanentIn = (text: string, where = ""): ActivationFailureClass | undefined => {
  for (const [code, why, pattern] of PERMANENT) {
    const match = pattern.exec(text);
    if (match) return { code, transient: false, classification: `permanent: ${why} (matched ${quote(match[0])}${where})` };
  }
  return undefined;
};

/** smarty-dev#8487 review: a permanent cause anywhere in the failure wins. The terminal line is
 * scanned first, then every other line of the full failure text, then the worker warnings. */
const permanentAnywhere = (input: { lastError: string; error?: string | undefined; evidence?: readonly string[] | undefined }):
  ActivationFailureClass | undefined => {
  const scan = (texts: readonly string[]): string =>
    texts.flatMap(text => lines(text.replace(MODEL_TEXT_QUOTE, ""))).join("\n");
  return permanentIn(scan([input.lastError]))
    ?? permanentIn(scan([input.error ?? ""]), " in another failure line")
    ?? permanentIn(scan(input.evidence ?? []), " in a worker warning");
};

const providerTransientIn = (text: string): { why: string; match: string } | undefined => {
  for (const [why, pattern] of PROVIDER_TRANSIENT) {
    const match = pattern.exec(text);
    if (match) return { why, match: match[0] };
  }
  return undefined;
};

/**
 * Classify one failed activation. lastError is the reported terminal error line; error is the
 * full failure text; evidence is the run's worker warnings, when the run produced any.
 */
const WINDOW_LOST = /^[ \t]*(?:Fabric activation window failed: )?(?:Error: )?(Activation window lost (?:current activation messages|its authorized compaction|its native history boundary|its native system records|its native system prompt|the Google config object))[ \t]*\r?$/m;
// Persisted legacy records only: the old store's exact error, optionally wrapped by #prepare
// or Error.toString(). Live failures require the typed cause, never a message match.
const REGISTRY_VETO = /^(?:(?:Error|ActorPreparationError): )?(?:Actor [\w:-]+ preparation \([\w-]+\): )?(?:ActorRegistryUpdateVetoedError: )?Actor registry update was vetoed by its validation and not committed; retry$/;
export const classifyActivationFailure = (input: {
  lastError: string; error?: string | undefined; evidence?: readonly string[] | undefined; preparation?: boolean | undefined;
  /** The preparation error is a timeout by type (a preparation, launch or mesh lock deadline). */
  preparationTimeout?: boolean | undefined;
  /** The preparation cause is the store's typed veto (not an arbitrary retryable error). */
  registryVeto?: boolean | ActorRegistryVetoReason | undefined;
  /** Restore only: migrate untyped legacy text, or normalize an already stored veto code. */
  persistedRegistryVeto?: "legacy" | "known-code" | undefined;
}): ActivationFailureClass => {
  const permanent = permanentAnywhere(input);
  const knownVeto = input.persistedRegistryVeto === "known-code";
  if (!permanent && (input.registryVeto || knownVeto ||
      input.persistedRegistryVeto === "legacy" && REGISTRY_VETO.test(input.lastError))) {
    const candidate = input.registryVeto || (knownVeto
      ? /^actor registry validation veto \(([a-z-]+)\)$/.exec(input.lastError)?.[1] : undefined);
    const reason = typeof candidate === "string" && /^(?:cancelled|revision|ownership|actor-state|caller-validation|generation|payload-generation|lock|deadline)$/.test(candidate)
      ? candidate : "caller-validation";
    return { code: "registry-update-vetoed", transient: true, reason: `actor registry validation veto (${reason})`,
      classification: `transient: actor registry validation veto (${reason}) committed nothing; retry after backoff` };
  }
  if (input.preparation || input.preparationTimeout) {
    // smarty-dev#6509: a preparation that only ran out of time (its own deadline, a model/auth
    // launch deadline, a mesh or registry lock wait) is transient; any other preparation failure,
    // and a timeout with a permanent cause anywhere in its text, stays a hard block.
    const timeout = permanent ? undefined
      : input.preparationTimeout ? "a preparation deadline" : TIMEOUT.exec(withoutModelText(input.lastError))?.[0];
    if (timeout) {
      return { code: "preparation-timeout", transient: true,
        classification: `transient: activation preparation timed out (${input.preparationTimeout ? timeout : `matched ${quote(timeout)}`})` };
    }
    if (input.preparation || !permanent) {
      return { code: "failing-preparation", transient: false,
        classification: "permanent: activation preparation kept failing past its retry budget" };
    }
    return permanent;
  }
  // Permanent causes first, across the whole failure; only then any transient evidence.
  if (permanent) return permanent;
  const line = withoutModelText(input.lastError);
  // smarty-dev#9890: every terminal "Activation window lost …" (its messages, history boundary,
  // system records or authorized compaction) is one transient class; the next run rebuilds the window.
  // Only the worker's own refusal counts: a whole line that is exactly one of activation-window.ts's messages,
  // optionally behind its "Fabric activation window failed: " and "Error: " prefixes. A mention inside another
  // error stays fail-closed.
  const windowLost = WINDOW_LOST.exec(line);
  if (windowLost) {
    return { code: "activation-window-lost", transient: true,
      classification: `transient: activation window lost (matched ${quote(windowLost[0])})` };
  }
  if (/Child Pi exited before requested model admission completed/i.test(line)) {
    return { code: "child-exit-before-admission", transient: true,
      classification: "transient: child Pi exited before model admission; the task was not sent" };
  }
  if (/Directive reply missing/i.test(line)) {
    // Transient only after a provider or stream error in the same run; a model that
    // simply did not reply stays blocked. No permanent cause is anywhere in the failure here.
    // Cut the model quote from the whole text before removing the terminal line, so prose on a
    // quote's later lines is never left behind as transient evidence.
    const earlier = withoutModelText(input.error ?? "").split(line).join("\n");
    const evidence = lines([earlier, ...(input.evidence ?? []).map(withoutModelText)].join("\n")).join("\n");
    const recovery = PROVIDER_RECOVERY_EVIDENCE.exec(evidence);
    const cause = providerTransientIn(evidence) ?? (recovery ? { why: "provider recovery", match: recovery[0] } : undefined);
    return cause
      ? { code: "provider-transient", transient: true,
        classification: `transient: directive reply missing after a ${cause.why} (matched ${quote(cause.match)})` }
      : { code: "unknown", transient: false,
        classification: "fail-closed: directive reply missing without a provider or stream error" };
  }
  const cause = providerTransientIn(line);
  if (cause) {
    return { code: "provider-transient", transient: true,
      classification: `transient: ${cause.why} (matched ${quote(cause.match)})` };
  }
  return { code: "unknown", transient: false, classification: "fail-closed: no transient cause recognised" };
};
