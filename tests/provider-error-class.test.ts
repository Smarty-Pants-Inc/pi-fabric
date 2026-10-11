import { describe, expect, it } from "vitest";
import { classifyProviderError } from "../src/provider-error-class.js";

// smarty-dev#8742: 26 Mains sat 13.5 h in provider-backoff on a gateway 409 that no retry could fix.
const affinity = "compaction_affinity_missing: the signed compaction block belongs to another upstream account";
const anthropic = (status: number, type: string, message: string) =>
  `${status} ${JSON.stringify({ type: "error", error: { type, message } })}`;
const failed = (errorMessage: string, extra: Record<string, unknown> = {}) => ({ stopReason: "error", errorMessage, ...extra });

describe("classifyProviderError (smarty-dev#8742)", () => {
  it("ignores turns that did not fail", () => {
    expect(classifyProviderError(undefined)).toBeUndefined();
    expect(classifyProviderError({ stopReason: "stop" })).toBeUndefined();
    expect(classifyProviderError({ stopReason: "aborted", errorMessage: "409" })).toBeUndefined();
  });

  it("classifies the incident's gateway 409 from status and body, as Anthropic and OpenAI SDKs format it", () => {
    expect(classifyProviderError(failed(anthropic(409, "invalid_request_error", affinity)))).toEqual({
      retryable: false, status: 409, basis: "body",
      short: `409 invalid_request_error: ${affinity}`,
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

  it("keeps a provider plan limit (Pi provider_limit diagnostic) on backoff", () => {
    expect(classifyProviderError(failed("429 smarty_limit", { diagnostics: [{ type: "provider_limit", timestamp: 1 }] })))
      .toMatchObject({ retryable: true, basis: "diagnostic" });
  });

  it("keeps CLIProxyAPI's restart-time 400 retryable, as Pi does (smarty-dev#1856)", () => {
    expect(classifyProviderError(failed("400 unknown provider for model claude-opus-5"))).toMatchObject({ retryable: true, status: 400, basis: "text" });
  });

  it("reads the type of a truncated body", () => {
    const truncated = `400 {"type":"error","error":{"type":"invalid_request_error","message":"${"x".repeat(50)}... [truncated 9000 chars]`;
    expect(classifyProviderError(failed(truncated))).toMatchObject({ retryable: false, status: 400, basis: "body" });
  });

  it("falls back to the exact gateway token only when no status or body survived", () => {
    expect(classifyProviderError(failed(`Provider error: ${affinity}`))).toMatchObject({ retryable: false, basis: "text" });
    expect(classifyProviderError(failed("the request was invalid"))).toMatchObject({ retryable: true, basis: "default" });
  });

  it("keeps the short form on one bounded line", () => {
    const short = classifyProviderError(failed(anthropic(400, "invalid_request_error", `line one\nline two ${"y".repeat(400)}`)))!.short;
    expect(short).not.toContain("\n");
    expect(short.length).toBeLessThanOrEqual(160);
  });
});
