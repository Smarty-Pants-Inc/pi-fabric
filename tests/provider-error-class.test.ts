import { describe, expect, it } from "vitest";
import { classifyModelRefusal, classifyProviderError, hasRecordedModelRefusal } from "../src/provider-error-class.js";

// smarty-dev#8742: 26 Mains sat 13.5 h in provider-backoff on a gateway 409 that no retry could fix.
const affinity = "compaction_affinity_missing: the signed compaction block belongs to another upstream account";
const anthropic = (status: number, type: string, message: string) =>
  `${status} ${JSON.stringify({ type: "error", error: { type, message } })}`;
const failed = (errorMessage: string, extra: Record<string, unknown> = {}) => ({ stopReason: "error", errorMessage, ...extra });

describe("classifyModelRefusal (smarty-dev#14952)", () => {
  it("recognizes only Pi's structured raw stop reason, and only on errors", () => {
    expect(classifyModelRefusal({ stopReason: "error", rawStopReason: "refusal", errorMessage: "private stop detail" }))
      .toEqual({ kind: "model-refusal" });
    expect(classifyModelRefusal({ stopReason: "error", errorMessage: "The model refused to complete the request" })).toBeUndefined();
    expect(classifyModelRefusal({ stopReason: "stop", rawStopReason: "refusal" })).toBeUndefined();
  });

  it("keeps status, body, diagnostic, and fallback-text errors on the provider path", () => {
    const rawReasonWith5xx = failed(anthropic(503, "api_error", "The model refused to complete the request"), { rawStopReason: "refusal" });
    expect(classifyModelRefusal(rawReasonWith5xx)).toBeUndefined();
    expect(classifyProviderError(rawReasonWith5xx)).toMatchObject({ retryable: true, status: 503 });

    const rawReasonWithBody = failed(JSON.stringify({ type: "error", error: { type: "api_error", message: "refusal" } }), { rawStopReason: "refusal" });
    expect(classifyModelRefusal(rawReasonWithBody)).toBeUndefined();
    expect(classifyProviderError(rawReasonWithBody)).toMatchObject({ retryable: true });

    const fallbackOnly = failed("The model refused to complete the request");
    expect(classifyModelRefusal(fallbackOnly)).toBeUndefined();
    expect(classifyProviderError(fallbackOnly)).toMatchObject({ retryable: true, basis: "default" });

    const statusBearingFallback = failed("503 The model refused to complete the request");
    expect(classifyModelRefusal(statusBearingFallback)).toBeUndefined();
    expect(classifyProviderError(statusBearingFallback)).toMatchObject({ retryable: true, status: 503 });

    expect(classifyModelRefusal(failed("private stop detail", { rawStopReason: "refusal", diagnostics: [{ type: "provider_limit" }] })))
      .toBeUndefined();

    const recorded = failed("private stop detail", { rawStopReason: "refusal",
      diagnostics: [{ type: "fabric_model_refusal", details: { rawStopReason: "refusal" } }] });
    expect(classifyModelRefusal(recorded)).toBeUndefined();
    expect(hasRecordedModelRefusal(recorded)).toBe(true);
    const marker = { type: "fabric_model_refusal", details: { rawStopReason: "refusal" } };
    expect(hasRecordedModelRefusal(failed("private stop detail", { rawStopReason: "refusal", diagnostics: [marker] }))).toBe(true);
    expect(hasRecordedModelRefusal(failed(anthropic(503, "api_error", "temporary"), { rawStopReason: "refusal", diagnostics: [marker] })))
      .toBe(false);
    expect(hasRecordedModelRefusal(failed(JSON.stringify({ type: "error", error: { type: "api_error", message: "temporary" } }),
      { rawStopReason: "refusal", diagnostics: [marker] }))).toBe(false);
    expect(hasRecordedModelRefusal(failed("private stop detail", { rawStopReason: "refusal", diagnostics: [
      marker, { type: "provider_limit" },
    ] }))).toBe(false);
    expect(hasRecordedModelRefusal(failed("private stop detail", { diagnostics: [{ type: "provider_limit" }] }))).toBe(false);
  });
});

describe("classifyProviderError (smarty-dev#8742)", () => {
  it("classifies refusals and Pi's explicit non-retryable recovery without provider text", () => {
    expect(classifyProviderError(failed("private refusal detail", { rawStopReason: "refusal" })))
      .toMatchObject({ retryable: false, short: "model refusal", basis: "diagnostic" });
    expect(classifyProviderError(failed("private network diagnostic", { recovery: { retryable: false } })))
      .toMatchObject({ retryable: false, short: "non-retryable", basis: "diagnostic" });
  });

  it("ignores turns that did not fail", () => {
    expect(classifyProviderError(undefined)).toBeUndefined();
    expect(classifyProviderError({ stopReason: "stop" })).toBeUndefined();
    expect(classifyProviderError({ stopReason: "aborted", errorMessage: "409" })).toBeUndefined();
  });

  it("classifies the incident's gateway 409 from status and body, as Anthropic and OpenAI SDKs format it", () => {
    expect(classifyProviderError(failed(anthropic(409, "invalid_request_error", affinity)))).toEqual({
      retryable: false, status: 409, type: "invalid_request_error", code: "compaction_affinity_missing", basis: "body",
      short: "409 invalid_request_error: compaction_affinity_missing",
    });
    const openai = `openai API error (409): ${JSON.stringify({ type: "invalid_request_error", message: affinity })}`;
    expect(classifyProviderError(failed(openai))).toMatchObject({ retryable: false, status: 409 });
    expect(classifyProviderError(failed(`409: ${JSON.stringify({ code: "compaction_affinity_missing" })}`)))
      .toMatchObject({ retryable: false, status: 409, basis: "body" });
  });

  it("keeps a throttling or bare 409 retryable (X-Should-Retry is invisible to extensions)", () => {
    expect(classifyProviderError(failed(anthropic(409, "rate_limit_error", "slow down")))).toMatchObject({ retryable: true, status: 409 });
    expect(classifyProviderError(failed("409 status code (no body)"))).toMatchObject({ retryable: true, status: 409, basis: "status" });
  });

  it.each([
    [400, "invalid_request_error", "messages.3: thinking block signature is invalid"],
    [401, "authentication_error", "invalid x-api-key"],
    [403, "permission_error", "workspace disabled"],
    [404, "not_found_error", "model: claude-nope"],
    [413, "request_too_large", "request exceeds the maximum size"],
  ])("treats HTTP %s %s as non-retryable", (status, type, message) => {
    expect(classifyProviderError(failed(anthropic(status, type, message)))).toMatchObject({ retryable: false, status, basis: "body" });
  });

  it.each([
    anthropic(429, "rate_limit_error", "rate limited"),
    anthropic(500, "api_error", "Internal server error"),
    anthropic(529, "overloaded_error", "Overloaded"),
    "503 upstream connect error or disconnect/reset before headers",
    "408 Request Timeout",
    "Connection error.",
    "fetch failed",
    "terminated",
    "Anthropic stream ended without a stop reason",
  ])("keeps %s retryable (today's backoff)", (errorMessage) => {
    expect(classifyProviderError(failed(errorMessage))?.retryable).toBe(true);
  });

  // Retry decisions follow status/structured fields, not affinity tokens echoed in prose.
  it("keeps 503 retryable when body.message echoes compaction_affinity_missing", () => {
    const secret = "SECRET-fake-provider-key echoed private prompt";
    const errorMessage = `503 ${JSON.stringify({ message: `upstream quoted: ${affinity}; ${secret}` })}`;
    const verdict = classifyProviderError(failed(errorMessage))!;
    expect(verdict.short).toBe("503: compaction_affinity_missing");
    expect(JSON.stringify(verdict)).not.toMatch(/SECRET|private prompt|fake-provider-key/);
    expect(verdict).toMatchObject({ retryable: true, status: 503, basis: "status" });
  });

  it.each([408, 429, 500, 503, 529, 599])("keeps HTTP %s retryable despite structured affinity code or type", (status) => {
    for (const field of ["code", "type"]) {
      const errorMessage = `${status} ${JSON.stringify({ [field]: "compaction_affinity_missing" })}`;
      expect(classifyProviderError(failed(errorMessage))).toMatchObject({ retryable: true, status, basis: "status" });
    }
  });

  it.each(["code", "type"])("keeps 400 body.%s affinity non-retryable", (field) => {
    const errorMessage = `400 ${JSON.stringify({ [field]: "compaction_affinity_missing" })}`;
    expect(classifyProviderError(failed(errorMessage))).toMatchObject({ retryable: false, status: 400, basis: "body" });
  });

  it("uses the normal 400 status rule when only body.message quotes affinity", () => {
    const errorMessage = `400 ${JSON.stringify({ message: `request quoted: ${affinity}` })}`;
    expect(classifyProviderError(failed(errorMessage))).toMatchObject({ retryable: false, status: 400, basis: "status" });
  });

  it("keeps 400 rate_limit_error retryable when its message merely mentions affinity", () => {
    expect(classifyProviderError(failed(anthropic(400, "rate_limit_error", `rate limited; quoted: ${affinity}`))))
      .toMatchObject({ retryable: true, status: 400, basis: "body" });
  });

  it("does not use raw-text affinity fallback when a parsed body.message survives without status", () => {
    const errorMessage = JSON.stringify({ message: `upstream quoted: ${affinity}` });
    expect(classifyProviderError(failed(errorMessage))).toMatchObject({ retryable: true, basis: "default" });
  });

  it("keeps a provider plan limit (Pi provider_limit diagnostic) on backoff", () => {
    expect(classifyProviderError(failed("429 smarty_limit", { diagnostics: [{ type: "provider_limit", timestamp: 1 }] })))
      .toMatchObject({ retryable: true, basis: "diagnostic" });
  });

  it("keeps CLIProxyAPI's restart-time 400 retryable, as Pi does (smarty-dev#1856)", () => {
    expect(classifyProviderError(failed("400 unknown provider for model claude-opus-5"))).toMatchObject({ retryable: true, status: 400, basis: "text" });
    expect(classifyProviderError(failed("openai API error (400): unknown provider for model gpt-6"))).toMatchObject({ retryable: true, status: 400, basis: "text" });
    const gateway = `400 ${JSON.stringify({ error: { message: "unknown provider for model claude-opus-5", type: "invalid_request_error" } })}`;
    expect(classifyProviderError(failed(gateway))).toMatchObject({ retryable: true, status: 400, type: "invalid_request_error", basis: "body" });
    expect(classifyProviderError(failed(anthropic(400, "invalid_request_error", "unknown provider for model claude-opus-5")))).toMatchObject({ retryable: true, basis: "body" });
  });

  // PR #841 code review P1: the routing-miss text must never override an explicit status, type or code.
  it.each([
    ["401 unknown provider for model claude-opus-5", 401],
    ["403 unknown provider for model claude-opus-5", 403],
    ["404 unknown provider for model claude-opus-5", 404],
    [anthropic(401, "authentication_error", "unknown provider for model claude-opus-5"), 401],
    [anthropic(403, "permission_error", "unknown provider for model claude-opus-5"), 403],
    [anthropic(400, "authentication_error", "unknown provider for model claude-opus-5"), 400],
    [anthropic(400, "permission_error", "unknown provider for model claude-opus-5"), 400],
    [anthropic(400, "not_found_error", "unknown provider for model claude-opus-5"), 400],
    [anthropic(400, "invalid_request_error", "model claude-nope is not allowed: unknown provider for model claude-nope"), 400],
    [`openai API error (401): ${JSON.stringify({ type: "authentication_error", message: "unknown provider for model gpt-6" })}`, 401],
    [`400: ${JSON.stringify({ error: { message: "unknown provider for model x", type: "invalid_request_error", code: "model_not_found" } })}`, 400],
    ["400 invalid x-api-key; unknown provider for model claude-opus-5", 400],
  ])("keeps %s non-retryable despite the routing-miss text", (errorMessage, status) => {
    expect(classifyProviderError(failed(errorMessage))).toMatchObject({ retryable: false, status });
  });

  it("reads the type of a truncated body", () => {
    const truncated = `400 {"type":"error","error":{"type":"invalid_request_error","message":"${"x".repeat(50)}... [truncated 9000 chars]`;
    expect(classifyProviderError(failed(truncated))).toMatchObject({ retryable: false, status: 400, basis: "body" });
  });

  it("falls back to the exact gateway token only when no status or body survived", () => {
    expect(classifyProviderError(failed(`Provider error: ${affinity}`))).toMatchObject({ retryable: false, basis: "text", short: "compaction_affinity_missing" });
    expect(classifyProviderError(failed("the request was invalid"))).toMatchObject({ retryable: true, basis: "default", short: "unclassified" });
  });

  // PR #841 security review P1: the short form reaches peers, ops.owner and the mesh bridge.
  it("never copies provider text into the short form: only status, a known type and a fixed code", () => {
    const secret = "SECRET-sk-ant-0123 echoed prompt: the user's password is hunter2";
    const cases: Array<[string, string]> = [
      [anthropic(400, "invalid_request_error", `line one\n${secret} ${"y".repeat(400)}`), "400 invalid_request_error"],
      [anthropic(403, `${secret}_error`, secret), "403"],                       // unknown type: dropped
      [`400: ${JSON.stringify({ code: secret, message: secret })}`, "400"],     // unknown code: dropped
      [anthropic(409, "invalid_request_error", `${affinity} ${secret}`), "409 invalid_request_error: compaction_affinity_missing"],
      [`openai API error (401): ${JSON.stringify({ type: "authentication_error", message: secret })}`, "401 authentication_error"],
      [`400 {"type":"error","error":{"type":"invalid_request_error","message":"${secret}... [truncated 9000 chars]`, "400 invalid_request_error"],
      [`Provider error: ${affinity}; ${secret}`, "compaction_affinity_missing"],
      [`socket hang up: ${secret}`, "unclassified"],
      [`503 ${secret}`, "503"],
    ];
    for (const [errorMessage, short] of cases) {
      const verdict = classifyProviderError(failed(errorMessage))!;
      expect(verdict.short).toBe(short);
      expect(JSON.stringify(verdict)).not.toMatch(/SECRET|hunter2|echoed/);
    }
    expect(classifyProviderError(failed(`429 ${secret}`, { diagnostics: [{ type: "provider_limit" }] }))!.short).toBe("provider_limit");
  });
});
