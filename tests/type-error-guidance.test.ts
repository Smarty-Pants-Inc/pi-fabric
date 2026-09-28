import { describe, expect, it } from "vitest";
import { GUEST_TYPE_DECLARATIONS } from "../src/runtime/guest-types.js";
import { typeCheckFabricCode } from "../src/runtime/type-checker.js";
import { typeErrorRecoveryHint } from "../src/type-error-guidance.js";
import {
  CORE_TOOL_NAMES,
  coreToolProperties,
} from "../src/runtime/core-tool-properties.js";

const typeError = (message: string, line = 1, column = 1) => ({ line, column, message });

describe("typeErrorRecoveryHint", () => {
  it("treats pi.bash cwd as a supported option since #71", () => {
    const checked = typeCheckFabricCode('await pi.bash({ command: "ls", cwd: "/tmp" });', GUEST_TYPE_DECLARATIONS);
    expect(checked.errors).toHaveLength(0);
  });

  it("guides unsupported pi.bash stdin toward a file input", () => {
    expect(typeErrorRecoveryHint(
      'await pi.bash({ command: "gh issue create --body-file -", stdin: π.body });',
      [{
        line: 1,
        column: 61,
        message:
          "Object literal may only specify known properties, and 'stdin' does not exist in type 'PiCommandArgument & PiBashOptions'.",
      }],
    )).toContain("pi.write(path, content)");
  });

  it("recognizes the real TypeScript diagnostics for unsupported bash options", () => {
    const cases = [
      {
        code: 'await pi.bash({ command: "gh issue create --body-file -", stdin: π.body });',
        expected: "pi.write(path, content)",
      },
    ];

    for (const { code, expected } of cases) {
      const checked = typeCheckFabricCode(code, GUEST_TYPE_DECLARATIONS);
      expect(checked.errors.length).toBeGreaterThan(0);
      expect(typeErrorRecoveryHint(code, checked.errors)).toContain(expected);
    }
  });

  it("guides malformed edit payloads toward named payloads", () => {
    expect(typeErrorRecoveryHint(
      'await pi.edit({ path: "x", oldText: "a", newText: "broken });',
      [{ line: 1, column: 55, message: "Unterminated string literal." }],
    )).toContain("top-level `payloads`");
  });

  it("routes unknown properties to the core tool that owns them", () => {
    expect(typeErrorRecoveryHint(
      'return pi.edit({ path: "x", oldText: "a", newText: "b", settle: true });',
      [typeError(
        "Object literal may only specify known properties, and 'settle' does not exist in type 'PiEditArgument'.",
        1,
        58,
      )],
    )).toContain("belongs to `pi.bash`");
  });

  it("resolves two-arg option bags from the type alias", () => {
    expect(typeErrorRecoveryHint(
      'return pi.read("index.ts", { settle: true });',
      [typeError(
        "Object literal may only specify known properties, and 'settle' does not exist in type 'PiReadOptions'.",
        1,
        26,
      )],
    )).toContain("belongs to `pi.bash`");
  });

  it("falls back to the enclosing pi call for shared-bag type names", () => {
    expect(typeErrorRecoveryHint(
      'return pi.write({ path: "x", content: "a", timeout: 5 });',
      [typeError(
        "Object literal may only specify known properties, and 'timeout' does not exist in type 'PiContentArgument'.",
        1,
        43,
      )],
    )).toContain("belongs to `pi.bash`");
  });

  it("points nested strings at the outer fabric_exec arguments", () => {
    expect(typeErrorRecoveryHint(
      'return pi.bash({ command: "echo", strings: { payload: "x" } });',
      [typeError(
        "Object literal may only specify known properties, and 'strings' does not exist in type 'PiCommandArgument & PiBashOptions'.",
        1,
        35,
      )],
    )).toContain("outer `fabric_exec` arguments");
  });

  it("generalizes the cross-tool hint across the remaining tools", () => {
    expect(typeErrorRecoveryHint(
      'return pi.read({ path: "a", context: 3 });',
      [typeError(
        "Object literal may only specify known properties, and 'context' does not exist in type 'PiReadArgument'.",
        1,
        28,
      )],
    )).toContain("belongs to `pi.grep`");
  });

  it("stays silent when the property is valid for the called tool", () => {
    expect(typeErrorRecoveryHint(
      'return pi.read({ path: "a", limit: 5 });',
      [typeError(
        "Object literal may only specify known properties, and 'limit' does not exist in type 'PiReadArgument'.",
        1,
        28,
      )],
    )).toBeUndefined();
  });

  it("stays silent for properties outside every core tool schema", () => {
    expect(typeErrorRecoveryHint(
      'return pi.edit({ pth: "x", oldText: "a", newText: "b" });',
      [typeError(
        "Object literal may only specify known properties, and 'pth' does not exist in type 'PiEditArgument'.",
        1,
        22,
      )],
    )).toBeUndefined();
  });

  it("derives the property registry from the guest type declarations", () => {
    expect(CORE_TOOL_NAMES).toEqual([
      "read", "bash", "powershell", "edit", "write", "grep", "find", "ls",
    ]);
    expect(coreToolProperties().get("settle")).toEqual(["bash", "powershell"]);
    expect(coreToolProperties().get("timeout")).toEqual(["bash", "powershell"]);
    expect(coreToolProperties().get("edits")).toEqual(["edit"]);
    expect(coreToolProperties().get("context")).toEqual(["grep"]);
    expect(coreToolProperties().get("content")).toEqual(["write"]);
    expect(coreToolProperties().get("path") ?? []).toEqual(
      expect.arrayContaining(["read", "edit", "write", "grep", "find", "ls"]),
    );
  });

  it("calls out Promise.all tuple arity mismatches", () => {
    expect(typeErrorRecoveryHint(
      "const [first, second, third] = await Promise.all([pi.read('a'), pi.read('b')]);",
      [{ line: 1, column: 23, message: "Tuple type '[string, string]' of length '2' has no element at index '2'." }],
    )).toContain("one binding per promise");
  });

  it("distinguishes literal payload interpolation from executor variables", () => {
    expect(typeErrorRecoveryHint(
      'return pi.edit({ path: "x", oldText: `<script src="${CONTEXT_PATH}/x">`, newText: `<script src="${CONTEXT_PATH}/y">` });',
      [{ line: 1, column: 57, message: "Cannot find name 'CONTEXT_PATH'." }],
    )).toContain("being evaluated by the Fabric TypeScript program");
  });

  it("stays absent for semantic errors and unrelated code", () => {
    expect(typeErrorRecoveryHint(
      'await pi.edit({ path: "x", all: true });',
      [{ line: 1, column: 15, message: "Property oldText is missing." }],
    )).toBeUndefined();
    expect(typeErrorRecoveryHint(
      "return missingValue;",
      [{ line: 1, column: 8, message: "':' expected." }],
    )).toBeUndefined();
    expect(typeErrorRecoveryHint(
      'await pi.read({ path: "x", cwd: "y" });',
      [{
        line: 1,
        column: 28,
        message:
          "Object literal may only specify known properties, and 'cwd' does not exist in type 'PiReadArgument'.",
      }],
    )).toContain("belongs to `pi.bash`");
  });

  it("guides unquoted pi.read paths toward quoted strings", () => {
    expect(typeErrorRecoveryHint(
      "return await pi.read(/tmp/x);",
      [{ line: 1, column: 20, message: "Invalid character." }],
    )).toContain("quote filesystem paths");
  });
});
