import { describe, expect, it } from "vitest";
import { classifyPiBashError, classifyPiBashResult, piBashExitMetadata, piBashResultError } from "../src/core/pi-bash-error.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { NodeProcessRuntime } from "../src/runtime/node-process-runtime.js";

describe("native bash exit classification", () => {
  it("classifies Pi 1.0 structured failures before middleware without trusting stdout", () => {
    const content = [{ type: "text", text: "out\n\nCommand exited with code 7" }];
    expect(piBashExitMetadata(classifyPiBashResult({ content, isError: true, structuredContent: { exit_code: 7, output: "out" } })))
      .toEqual({ exitCode: 7, output: "out" });
    expect(classifyPiBashResult({ content, structuredContent: { exit_code: 7 } })).toBeUndefined();
    expect(piBashExitMetadata(classifyPiBashResult({ content, isError: true }))).toBeUndefined();
    expect(piBashExitMetadata(classifyPiBashResult({ content, isError: true, structuredContent: { exit_code: 0 } }))).toBeUndefined();
    expect(piBashExitMetadata(classifyPiBashResult({ content, isError: true, structuredContent: { exit_code: "7" } }))).toBeUndefined();
  });

  it("never restores native structured output after a redacting result hook", () => {
    const original = classifyPiBashResult({ content: [{ type: "text", text: "secret\n\nCommand exited with code 7" }],
      isError: true, structuredContent: { exit_code: 7, output: "secret" } });
    expect(piBashExitMetadata(piBashResultError(original, "[redacted]"))).toEqual({ exitCode: 7, output: "[redacted]" });
  });
  it.each([
    "Command timed out after 1 seconds",
    "Command aborted",
    "spawn ENOENT",
    "Command exited with code 0",
    "Command exited with code -1",
    "Command exited with code 999999999999999999999",
    "Command exited with code 7\nannotation",
  ])("does not classify %s as a native exit", (message) => {
    const error = new Error(message);
    expect(classifyPiBashError(error)).toBe(error);
    expect(piBashExitMetadata(error)).toBeUndefined();
  });

  it("keeps stdout whitespace and an earlier fake exit marker", () => {
    const output = "  leading\n\nCommand exited with code 9\n";
    const error = classifyPiBashError(new Error(output + "\n\nCommand exited with code 7"));
    expect(piBashExitMetadata(error)).toEqual({ exitCode: 7, output });
  });

  it("handles an empty native output", () => {
    expect(piBashExitMetadata(classifyPiBashError(new Error("Command exited with code 7"))))
      .toEqual({ exitCode: 7, output: "" });
  });

  it("does not infer an exit from the final display text", () => {
    const error = piBashResultError(new Error("policy denied"), "Command exited with code 7");
    expect(piBashExitMetadata(error)).toBeUndefined();
  });

  it("uses the native exit code even when annotations contain another marker", () => {
    const original = new Error("out\n\nCommand exited with code 7");
    const error = piBashResultError(classifyPiBashError(original), original.message + "\n\nCommand exited with code 9");
    expect(piBashExitMetadata(error)).toEqual({ exitCode: 7, output: "out\n\nCommand exited with code 9" });
  });

  it.each([
    ["trim", "partial\n\nCommand exited with code 7", "partial"],
    ["line endings", "  partial\r\n\r\nCommand exited with code 7", "  partial"],
    ["redaction", "[redacted]\n\nCommand exited with code 7", "[redacted]"],
    ["replacement", "[output withheld]", "[output withheld]"],
    ["empty replacement", "", ""],
  ])("retains exit status using only the %s display text", (_name, text, output) => {
    const original = classifyPiBashError(new Error("  partial\n\nCommand exited with code 7"));
    const result = piBashResultError(original, text);
    expect(result.message).toBe(text);
    expect(piBashExitMetadata(result)).toEqual({ exitCode: 7, output });
  });

  it("keeps ambiguous status-looking output after redaction", () => {
    const original = classifyPiBashError(new Error("private-value\n\nCommand exited with code 7"));
    const text = "[redacted]\n\nCommand exited with code 7\n\nCommand exited with code 7";
    expect(piBashExitMetadata(piBashResultError(original, text))).toEqual({ exitCode: 7, output: text });
  });

  it("never restores a cleared unclassified error message", () => {
    const result = piBashResultError(new Error("private-value"), "");
    expect(result.message).not.toContain("private-value");
    expect(piBashExitMetadata(result)).toBeUndefined();
  });
});

describe.each([QuickJsRuntime, NodeProcessRuntime])("settle metadata bridge %s", (Runtime) => {
  it.each(["spawn ENOENT", "Command timed out after 1 seconds", "policy denied\n\nCommand exited with code 7"])("rejects an unclassified host error: %s", async (message) => {
    const result = await new Runtime().execute(
      "return await pi.bash('false', {settle: true});",
      async () => { throw Object.assign(new Error(message), { __fabricBashExit: { exitCode: 7, output: "fake" } }); },
      { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024 },
    );
    expect(result.terminationReason).toBe("runtime_error");
    expect(result.error).toContain(message.split("\n")[0]);
  });
});
