import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { pathToFileURL } from "node:url";
import { applyTaskRetryDefaults, prepareRetryProfile, PI_TASK_RETRY_SETTINGS, taskRetrySettings, resolveRetrySdk } from "../src/worker/retry-profile.js";
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
  it("changes only the child's in-memory retry settings and keeps the canonical profile", async () => {
    const s = setup(); const before = fs.readFileSync(s.settings, "utf8");
    expect(prepareRetryProfile(s.cwd, s.target, s.env)).toBe(s.original);
    const binary = path.resolve(process.env.FABRIC_OVERLOAD_TEST_PI_BINARY ?? "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
    const sdkDirectory = resolveRetrySdk(binary)!;
    expect(sdkDirectory).toBe(path.dirname(fs.realpathSync(binary)));
    const { SettingsManager } = await import(pathToFileURL(path.join(sdkDirectory, "index.js")).href);
    const main = SettingsManager.create(s.cwd, s.original);
    const child = SettingsManager.create(s.cwd, s.original);
    const mainRetry = main.getRetrySettings();
    applyTaskRetryDefaults(child);
    expect(child.getRetrySettings()).toEqual({ enabled: true, ...PI_TASK_RETRY_SETTINGS });
    expect(main.getRetrySettings()).toEqual(mainRetry);
    expect(taskRetrySettings(0.05)).toEqual({ maxRetries: 6, baseDelayMs: 250, maxAgentDelayMs: 8000 });
    expect(child.getGlobalSettings().extensions).toEqual(["../hooks/one.ts", "!hooks/two.ts"]);
    expect(child.getGlobalSettings().packages).toEqual(["../package"]);
    expect(fs.existsSync(s.target)).toBe(false);
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
  it("keeps task defaults across native setters and reload without changing Main or persisting retry", async () => {
    const s = setup();
    const binary = path.resolve(process.env.FABRIC_OVERLOAD_TEST_PI_BINARY ?? "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
    const { SettingsManager } = await import(pathToFileURL(path.join(path.dirname(binary), "index.js")).href);
    const main = SettingsManager.create(s.cwd, s.original);
    const child = SettingsManager.create(s.cwd, s.original);
    const mainRetry = main.getRetrySettings();
    applyTaskRetryDefaults(child);
    for (const mutate of [() => child.setSteeringMode("all"), () => child.setFollowUpMode("all"), () => child.reload()]) {
      await mutate();
      await child.flush();
      expect(child.getRetrySettings()).toEqual({ enabled: true, ...PI_TASK_RETRY_SETTINGS });
      expect(main.getRetrySettings()).toEqual(mainRetry);
      expect(child.getGlobalSettings()).not.toHaveProperty("retry");
      expect(JSON.parse(fs.readFileSync(s.settings, "utf8"))).not.toHaveProperty("retry");
    }
    // A native explicit retry control must also supersede the fallback immediately.
    child.setRetryEnabled(false);
    expect(child.getRetrySettings()).toEqual({ ...mainRetry, enabled: false });
    await child.flush();
  });
  it.each(["global", "project"])("honors explicit %s retry settings introduced after task admission and reloaded", async scope => {
    const s = setup();
    const binary = path.resolve(process.env.FABRIC_OVERLOAD_TEST_PI_BINARY ?? "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
    const { SettingsManager } = await import(pathToFileURL(path.join(path.dirname(binary), "index.js")).href);
    const child = SettingsManager.create(s.cwd, s.original, { projectTrusted: true });
    applyTaskRetryDefaults(child);
    expect(child.getRetrySettings()).toEqual({ enabled: true, ...PI_TASK_RETRY_SETTINGS });
    const file = scope === "global" ? s.settings : path.join(s.cwd, ".pi", "settings.json");
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const retry = { enabled: false, maxRetries: 1, baseDelayMs: 77, maxAgentDelayMs: 99 };
    fs.writeFileSync(file, JSON.stringify({ retry }));
    await child.reload();
    child.setSteeringMode("all"); child.setFollowUpMode("all");
    await child.flush();
    expect(child.getRetrySettings()).toEqual(retry);
    expect(JSON.parse(fs.readFileSync(file, "utf8")).retry).toEqual(retry);
  });
  it("serializes Main and task refresh updates through the installed FileAuthStorageBackend", async () => {
    const s = setup();
    const mainPath = path.join(s.original, "auth.json");
    fs.writeFileSync(mainPath, "{}"); // Synthetic stores only; never use the launching profile.
    const profile = prepareRetryProfile(s.cwd, s.target, s.env) ?? s.original;
    const binary = path.resolve(process.env.FABRIC_OVERLOAD_TEST_PI_BINARY ?? "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
    const { FileAuthStorageBackend } = await import(pathToFileURL(path.join(path.dirname(binary), "core/auth-storage.js")).href);
    const main = new FileAuthStorageBackend(mainPath);
    const task = new FileAuthStorageBackend(path.join(profile, "auth.json"));
    let release!: () => void;
    let acquired!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const entered = new Promise<void>(resolve => { acquired = resolve; });
    let mainHeld = false;
    let overlappingLocks = false;
    const mainUpdate = main.withLockAsync(async (current: string) => {
      mainHeld = true; acquired();
      await held;
      mainHeld = false;
      return { result: undefined, next: JSON.stringify({ ...JSON.parse(current), main: true }) };
    }, { signal: AbortSignal.timeout(5000) });
    await entered;
    const taskUpdate = task.withLockAsync(async (current: string) => {
      overlappingLocks ||= mainHeld;
      return { result: undefined, next: JSON.stringify({ ...JSON.parse(current), task: true }) };
    }, { signal: AbortSignal.timeout(5000) });
    // Hold a real asynchronous refresh long enough for an alias lock to enter.
    await new Promise(resolve => setTimeout(resolve, 200));
    release();
    await Promise.all([mainUpdate, taskUpdate]);
    const contents = JSON.parse(fs.readFileSync(mainPath, "utf8"));
    if (process.env.FABRIC_OVERLOAD_TEST_EVIDENCE_DIR) {
      fs.mkdirSync(process.env.FABRIC_OVERLOAD_TEST_EVIDENCE_DIR, { recursive: true });
      fs.writeFileSync(path.join(process.env.FABRIC_OVERLOAD_TEST_EVIDENCE_DIR, "auth-lock.json"), JSON.stringify({ binary, overlappingLocks, contents }, null, 2));
    }
    expect({ overlappingLocks, contents }).toEqual({ overlappingLocks: false, contents: { main: true, task: true } });
  });
  it("never substitutes Fabric's peer SDK for an opaque or missing launcher", () => {
    const s = setup();
    const launcher = path.join(s.original, "custom-launcher.js");
    fs.writeFileSync(launcher, "// custom Pi launcher");
    expect(resolveRetrySdk(launcher)).toBeUndefined();
    expect(resolveRetrySdk(path.join(s.cwd, "missing-pi"))).toBeUndefined();
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
