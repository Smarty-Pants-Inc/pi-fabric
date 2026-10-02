import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { prepareRetryProfile, PI_TASK_RETRY_SETTINGS } from "../src/worker/retry-profile.js";
import { retryableProviderError } from "../src/worker/provider-error.js";
import { isRetryableAssistantError } from "@earendil-works/pi-ai/compat";
import type { AssistantMessage } from "@earendil-works/pi-ai";
const roots: string[] = [];
const setup = () => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retry-profile-")); roots.push(cwd);
  const original = path.join(cwd, "original"); fs.mkdirSync(original);
  const settings = path.join(original, "settings.json");
  fs.writeFileSync(settings, JSON.stringify({ extensions: ["../hooks/one.ts", "!hooks/two.ts"], packages: ["../package"], sessionDir: "sessions" }));
  fs.writeFileSync(path.join(original, "models.json"), "{}");
  return { cwd, original, settings, target: path.join(cwd, "child"), env: { PI_CODING_AGENT_DIR: original } };
};
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
describe("native task retry profile", () => {
  it("changes only the child's retry settings and retains relative resource meanings", () => {
    const s = setup(); const before = fs.readFileSync(s.settings, "utf8");
    expect(prepareRetryProfile(s.cwd, s.target, s.env)).toBe(s.target);
    const child = JSON.parse(fs.readFileSync(path.join(s.target, "settings.json"), "utf8"));
    expect(child.retry).toEqual(PI_TASK_RETRY_SETTINGS);
    expect(child.retry).toEqual({ maxRetries: 6, baseDelayMs: 5000, maxAgentDelayMs: 160000 });
    expect(child.extensions).toEqual([path.resolve(s.original, "../hooks/one.ts"), "!" + path.resolve(s.original, "hooks/two.ts")]);
    expect(child.packages).toEqual([path.resolve(s.original, "../package")]);
    expect(child.sessionDir).toBe("sessions");
    expect(fs.realpathSync(path.join(s.target, "models.json"))).toBe(path.join(s.original, "models.json"));
    expect(fs.readFileSync(s.settings, "utf8")).toBe(before);
    expect(s.env.PI_CODING_AGENT_DIR).toBe(s.original);
  });
  it.each(["global", "project"])("preserves any explicit %s retry settings, including disabled retry", scope => {
    const s = setup(); const file = scope === "global" ? s.settings : path.join(s.cwd, ".pi", "settings.json");
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const before = JSON.stringify({ retry: { enabled: false, maxRetries: 1, baseDelayMs: 77 } });
    fs.writeFileSync(file, before);
    expect(prepareRetryProfile(s.cwd, s.target, s.env)).toBeUndefined();
    expect(fs.readFileSync(file, "utf8")).toBe(before);
    expect(fs.existsSync(s.target)).toBe(false);
  });
  it("does not mask malformed settings", () => {
    const s = setup(); fs.writeFileSync(s.settings, "{");
    expect(() => prepareRetryProfile(s.cwd, s.target, s.env)).toThrow();
    expect(fs.existsSync(s.target)).toBe(false);
  });
});
describe("Pi-equivalent transient provider classification", () => {
  it.each(["503 server_is_overloaded", "502 Bad Gateway", "429 Too Many Requests", "500 internal error", "fetch failed", "other side closed", "terminated", "stream ended before a terminal response event", "socket hang up", "400 invalid_request_error", "401 unauthorized", "insufficient_quota", "Monthly usage limit reached", "billing exhausted"])("classifies %s like Pi", errorMessage => {
    expect(retryableProviderError(errorMessage)).toBe(isRetryableAssistantError({ stopReason: "error", errorMessage } as AssistantMessage));
  });
  it("does not retry context overflow or deterministic 400 errors with retry-like text", () => {
    expect(retryableProviderError("400 invalid request: 503 tokens")).toBe(false);
    expect(retryableProviderError("maximum context length exceeded, service unavailable")).toBe(false);
  });
});
