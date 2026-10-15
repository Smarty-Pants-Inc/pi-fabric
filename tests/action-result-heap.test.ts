import { mkdirSync, mkdtempSync } from "node:fs";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildSync } from "esbuild";
import { beforeAll, describe, expect, it } from "vitest";
import {
  boundedPreviewValue, boundedResult, failedResultError,
  previewArgs, previewResult, truncateString,
} from "../src/core/action-result.js";
import { headlineArg } from "../src/core/call-preview.js";
import { detachedPreview } from "../src/core/preview.js";

// Keep reason: recorded P0 #10720; ordinary equality tests miss sliced-parent
// retention. Child V8 GC probes measure the actual public preview paths.
const repo = fileURLToPath(new URL("../", import.meta.url));
let probe: string;
beforeAll(() => {
  const output = path.join(repo, ".local", "out");
  mkdirSync(output, { recursive: true });
  const directory = mkdtempSync(path.join(output, "action-preview-heap-"));
  probe = path.join(directory, "probe.mjs");
  buildSync({
    entryPoints: [fileURLToPath(new URL("./fixtures/preview-heap-probe.ts", import.meta.url))],
    outfile: probe, bundle: true, platform: "node", format: "esm", packages: "external",
  });
  // Intentionally no rm/unlink: parent owns hosted cleanup, artifacts are evidence.
}, 15_000);

const nativePreview = (value: unknown, max: number): unknown => {
  if (value === undefined || value === null || typeof value !== "object") return value;
  try {
    const json = JSON.stringify(value);
    if (json.length <= max) return JSON.parse(json) as unknown;
    return { fabricTruncated: true, originalChars: json.length, preview: json.slice(0, Math.max(1, max - 100)) };
  } catch {
    const text = String(value);
    return text.length <= max ? text : `${text.slice(0, max)}…`;
  }
};

describe("detached preview contracts (#10720)", () => {
  it("copies the exact UTF-16 prefix, including lone surrogates and short input", () => {
    for (const text of ["", "short", "a\ud800b\udfff", "a😀b", "quote\" slash\\\n"]) {
      for (const max of [0, 1, 2, 3, 4, 96]) {
        expect(detachedPreview(text, max)).toBe(text.slice(0, max));
        expect(truncateString(text, max)).toBe(text.length <= max ? text : `${text.slice(0, max)}…`);
      }
    }
  });

  it("preserves exact native JSON prefixes and originalChars across preview budgets", () => {
    const shared = { escaped: "\n\t\"\\\ud800😀" };
    const values: unknown[] = [
      {}, [], { nested: [shared, shared], omitted: undefined, nonfinite: NaN },
      { output: "x\"\udfff😀".repeat(500) },
      new Date("2026-01-02T00:00:00Z"),
      { wrapped: [new Number(4), new String("hello"), new Boolean(false)] },
      { toJSON: () => ({ result: "hook".repeat(200) }) },
      { toJSON: () => undefined },
      { toJSON: () => { throw new Error("hook error"); } },
      { big: 1n },
      Object.assign(Object.create(null) as object, { value: "x".repeat(1_000) }),
      new Proxy({ key: "value".repeat(300) }, {}),
    ];
    const rawJSON = (JSON as typeof JSON & { rawJSON?: (text: string) => unknown }).rawJSON;
    if (rawJSON) values.push({ raw: rawJSON("12345678901234567890") });
    const cycle: { self?: unknown } = {}; cycle.self = cycle; values.push(cycle);
    for (const value of values) {
      for (const max of [0, 1, 128, 512, 16_000]) {
        expect(boundedPreviewValue(value, max)).toEqual(nativePreview(value, max));
      }
    }
    expect(boundedPreviewValue("unbounded primitive", 1)).toBe("unbounded primitive");
  });

  it("keeps under-budget results lossless and preserves JSON failure behavior", () => {
    const value = { nested: ["\ud800", undefined, NaN], toJSON: () => ({ changed: true }) };
    const result = boundedResult(value, 512);
    expect(result.value).toBe(value);
    expect(result).toEqual({ value, chars: JSON.stringify(value).length, truncated: false });
    expect(boundedResult(undefined, 512)).toEqual({ value: undefined, chars: 4, truncated: false });
    const large = { text: "\ud800😀\\\"".repeat(1_000) };
    const json = JSON.stringify(large);
    expect(boundedResult(large, 512)).toEqual({
      value: { fabricTruncated: true, originalChars: json.length, preview: json.slice(0, 312) },
      chars: json.length, truncated: true,
    });
    const cycle: { self?: unknown } = {}; cycle.self = cycle;
    for (const bad of [cycle, 1n, () => 1, Symbol("bad"), { toJSON: () => undefined }]) {
      expect(() => boundedResult(bad, 512)).toThrow("Fabric action returned a non-JSON-serializable value:");
    }
    let calls = 0;
    expect(boundedPreviewValue({ toJSON() { calls++; return undefined; } }, 512)).toBe("[object Object]");
    expect(calls).toBe(1);
  });

  it("retains existing string/headline, nested-preview and key-count behavior", () => {
    expect(headlineArg({ query: "  line1\nline2\ttab  " })).toBe("line1 line2 tab");
    expect(headlineArg({ query: "x".repeat(200) }, 10)).toBe(`${"x".repeat(9)}…`);
    expect(failedResultError({ status: "failed", error: "  boom  " })).toBe("boom");
    const args = { content: "x".repeat(20_000), nested: { text: "y".repeat(20_000) } };
    expect(previewArgs("pi.write", args)).toEqual({
      content: `${"x".repeat(16_000)}…`, nested: nativePreview(args.nested, 16_000),
    });
    expect(previewResult({ nested: args.nested })).toEqual({ nested: nativePreview(args.nested, 16_000) });
    const keys = Object.fromEntries(Array.from({ length: 40 }, (_, index) => [`key-${index}`, "small"]));
    expect(Object.keys(previewArgs("pi.read", keys))).toHaveLength(32);
    expect(Object.keys(previewResult(keys) as object)).toHaveLength(32);
  });

  it("detects the actual sliced-parent failure in a negative control", () => {
    const child = spawnSync(process.execPath, ["--expose-gc", probe, "retained-slice-control"], {
      cwd: repo, encoding: "utf8", timeout: 14_000, maxBuffer: 1024 * 1024,
    });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr || child.stdout).toBe(0);
    const receipt = JSON.parse(child.stdout) as { count: number; heapDelta: number };
    expect(receipt.count).toBe(100);
    expect(receipt.heapDelta).toBeGreaterThan(100 * 1024 * 1024);
    console.info("preview heap negative control", receipt);
  }, 15_000);

  it.each([
    "action-string", "action-json", "action-nested-args", "action-nested-result", "action-failed",
  ])("%s releases 100 unique 4 MiB parents (<5 MiB retained)", mode => {
    const child = spawnSync(process.execPath, ["--expose-gc", probe, mode], {
      cwd: repo, encoding: "utf8", timeout: 14_000, maxBuffer: 1024 * 1024,
    });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr || child.stdout).toBe(0);
    const receipt = JSON.parse(child.stdout) as { count: number; heapDelta: number; heapCap: number };
    expect(receipt.count).toBe(100);
    expect(receipt.heapDelta).toBeLessThan(receipt.heapCap);
    console.info("preview heap", receipt);
  }, 15_000);
});
