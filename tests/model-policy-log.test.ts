import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { appendOpusPolicyLog, isClaudeModel, opusIssueReference } from "../src/agents/model-policy-log.js";

const probe = vi.hoisted(() => ({
  mkdirSync: vi.fn(), openSync: vi.fn(), writeSync: vi.fn(), closeSync: vi.fn(),
  homedir: vi.fn(), hostname: vi.fn(),
}));
// Import only real constants; no test can reach an actual filesystem write.
vi.mock("node:fs", async importOriginal => {
  const { constants } = await importOriginal<typeof import("node:fs")>();
  return { default: { constants, mkdirSync: probe.mkdirSync, openSync: probe.openSync,
    writeSync: probe.writeSync, closeSync: probe.closeSync } };
});
vi.mock("node:os", () => ({ default: { homedir: probe.homedir, hostname: probe.hostname } }));

const home = path.resolve("mock-opus-private-home");
const directory = path.join(home, ".local", "state", "smarty", "route");
const input = { role: "task", model: "provider/claude-opus-5.5", ref: "#123", reason: "Review #123" };
const calls: string[] = [];
beforeEach(() => {
  vi.resetAllMocks();
  calls.length = 0;
  probe.homedir.mockReturnValue(home);
  probe.hostname.mockReturnValue("audit-host");
  probe.mkdirSync.mockImplementation(() => { calls.push("mkdir"); });
  probe.openSync.mockImplementation(() => { calls.push("open"); return 41; });
  probe.writeSync.mockImplementation((_fd: number, line: Buffer) => { calls.push("write"); return line.length; });
  probe.closeSync.mockImplementation(() => { calls.push("close"); });
});

// Decision C keep reasons: issue-reference trust boundary, data-safe single append,
// and storage failures/short writes that a real user flow cannot cheaply induce.
describe("Opus policy issue-reference trust boundary", () => {
  it.each([
    ["#1", "#1"], ["Review #123 before shipping", "#123"], ["Review (#42).", "#42"],
    ["https://github.com/org/repo/issues/314", "https://github.com/org/repo/issues/314"],
    ["See http://tracker.example/issues/7.", "http://tracker.example/issues/7"],
    ["See https://tracker.example/issues/7#discussion", "https://tracker.example/issues/7#discussion"],
    ["See HTTPS://tracker.example/issues/7?view=full", "HTTPS://tracker.example/issues/7?view=full"],
    ["https://tracker.example/docs/#123; tracked by #42", "#42"],
  ])("extracts a real issue reference from %s", (reason, expected) => {
    expect(opusIssueReference(reason)).toBe(expected);
  });

  it.each([
    undefined, null, 123, {}, ["#1"], "", "urgent", "Compatibility probe", "approved by Paul",
    "#0", "#-1", "#01", "#1abc", "tag#23", "https://example.com", "https://example.com/docs/#123",
    "https://example.com/?issue=#123", "https://example.com/pulls/123", "https://example.com/issues/0",
    "https://example.com/issues/01", "https://example.com/issues/12extra", "https://example.com/issues/12/comments",
    "https://example.com/?redirect=/issues/12", "https:///issues/12", "file:///repo/issues/12",
  ])("returns the refusal signal for arbitrary/non-issue reasons: %j", reason => {
    expect(opusIssueReference(reason)).toBeUndefined();
  });
});

describe("Claude/Opus model identifiers", () => {
  it.each(["opus", "OpUs", "provider/opus", "Provider/ClAuDe-OpUs-5-5", "provider/claude-sonnet-4",
    "claude", "claude-haiku", "gateway/provider/opus-5.5", "bedrock/us.anthropic.claude-opus-4-6"])("recognizes %s", model => {
    expect(isClaudeModel(model)).toBe(true);
  });
  it.each([undefined, "", "gpt-5.6-sol", "provider/gpt-6-astra", "provider/octopus", "provider/myclaudemodel",
    "provider/opuslike", "provider/claudette"])("does not match unrelated identifiers: %s", model => {
    expect(isClaudeModel(model)).toBe(false);
  });
  it("recognizes the native runner independently of a model pin", () => {
    expect(isClaudeModel(undefined, "claude")).toBe(true);
    expect(isClaudeModel("provider/gpt", "CLAUDE")).toBe(true);
    expect(isClaudeModel(undefined, "pi")).toBe(false);
  });
});

describe("Opus exception data-safe audit call probe", () => {
  it.each(["task", "actor:reviewer"])("appends exactly one complete private JSON line for %s", role => {
    const before = Date.now();
    expect(appendOpusPolicyLog({ ...input, role })).toBeUndefined();
    const after = Date.now();
    expect(os.homedir()).toBe(home);
    expect(calls).toEqual(["mkdir", "open", "write", "close"]);
    expect(probe.mkdirSync).toHaveBeenCalledExactlyOnceWith(directory, { recursive: true, mode: 0o700 });
    const [file, flags, mode] = probe.openSync.mock.calls[0]!;
    expect(file).toBe(path.join(directory, "opus-exceptions.jsonl"));
    const requiredFlags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND;
    expect(flags & requiredFlags).toBe(requiredFlags);
    expect(flags & fs.constants.O_TRUNC).toBe(0);
    expect(mode).toBe(0o600);
    expect(probe.writeSync).toHaveBeenCalledTimes(1);
    const [fd, line, ...extra] = probe.writeSync.mock.calls[0]!;
    expect(fd).toBe(41);
    expect(extra).toEqual([]); // writeSync(fd, Buffer): one call with the entire line.
    expect(Buffer.isBuffer(line)).toBe(true);
    const text = (line as Buffer).toString("utf8");
    expect(text.split("\n")).toHaveLength(2);
    expect(text.endsWith("\n")).toBe(true);
    const record = JSON.parse(text);
    expect(record).toEqual({ ts: expect.any(String), role, model: input.model, ref: input.ref,
      reason: input.reason, launcher: "fabric", host: "audit-host" });
    expect(record.ts).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    expect(new Date(record.ts).toISOString()).toBe(record.ts);
    expect(Date.parse(record.ts)).toBeGreaterThanOrEqual(before);
    expect(Date.parse(record.ts)).toBeLessThanOrEqual(after);
    expect(probe.closeSync).toHaveBeenCalledExactlyOnceWith(41);
  });

  it("records refusal context with null ref and safely escapes newlines/Unicode", () => {
    const reason = 'Arbitrary reason\n{"forged":true}\n☃';
    expect(appendOpusPolicyLog({ ...input, ref: null, reason })).toBeUndefined();
    const line = probe.writeSync.mock.calls[0]![1] as Buffer;
    expect(line.toString("utf8").split("\n")).toHaveLength(2);
    expect(JSON.parse(line.toString("utf8"))).toMatchObject({ ref: null, reason });
  });

  it.each([0, 1])("returns a bounded short-write warning, closes, and never retries (%s bytes)", bytes => {
    probe.writeSync.mockReturnValue(bytes);
    const warning = appendOpusPolicyLog(input);
    expect(warning).toMatch(/short write/i);
    expect(warning!.length).toBeLessThanOrEqual(200);
    expect(probe.writeSync).toHaveBeenCalledTimes(1);
    expect(probe.closeSync).toHaveBeenCalledExactlyOnceWith(41);
  });

  it.each(["homedir", "hostname", "mkdirSync", "openSync", "writeSync", "closeSync"] as const)(
    "contains %s failure without throwing or leaking error data", stage => {
      probe[stage].mockImplementation(() => { throw new Error(`SECRET ${"x".repeat(10_000)}`); });
      let warning: string | undefined;
      expect(() => { warning = appendOpusPolicyLog(input); }).not.toThrow();
      expect(warning).toMatch(/Opus policy audit log .*failed/);
      expect(warning!.length).toBeLessThanOrEqual(200);
      expect(warning).not.toContain("SECRET");
      expect(probe.writeSync.mock.calls.length).toBe(stage === "writeSync" || stage === "closeSync" ? 1 : 0);
      expect(probe.closeSync.mock.calls.length).toBe(stage === "writeSync" || stage === "closeSync" ? 1 : 0);
    },
  );

  it("contains combined write/close failure and still only attempts one close", () => {
    probe.writeSync.mockImplementation(() => { throw new Error("write failed"); });
    probe.closeSync.mockImplementation(() => { throw new Error("close failed"); });
    expect(appendOpusPolicyLog(input)).toMatch(/failed/);
    expect(probe.writeSync).toHaveBeenCalledTimes(1);
    expect(probe.closeSync).toHaveBeenCalledExactlyOnceWith(41);
  });
});
