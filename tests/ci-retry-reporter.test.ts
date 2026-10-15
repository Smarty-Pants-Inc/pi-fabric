import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import type { TestCase } from "vitest/node";
import CiRetryReporter from "../scripts/ci-retry-reporter.js";

const source = "tests/ci-retry-reporter.test.ts";
const realFrame = `${process.cwd()}/${source}:42:7`;

it.each([
  { label: "real frame", name: "TypeError", frame: realFrame, detail: `first failure (TypeError): ${source}:42:7` },
  { label: "hostile identifier name", name: "ghpCANARYSECRET789", frame: realFrame, detail: `first failure (Error): ${source}:42:7` },
  { label: "hostile name with spaces", name: "Error token=CANARY-SECRET-123", frame: realFrame, detail: `first failure (Error): ${source}:42:7` },
  { label: "forged missing file", name: "Error", frame: `${process.cwd()}/token-CANARY-SECRET-456.ts:1:1`, detail: "first failure: unavailable" },
  { label: "traversal", name: "Error", frame: `${process.cwd()}/tests/../${source}:42:7`, detail: "first failure: unavailable" },
  { label: "directory", name: "Error", frame: `${process.cwd()}/tests:1:1`, detail: "first failure: unavailable" },
  { label: "oversized coordinates", name: "Error", frame: `${process.cwd()}/${source}:12345678:7`, detail: "first failure: unavailable" },
  { label: "Windows separators", name: "SyntaxError", frame: realFrame.replace(/\//g, "\\"), detail: `first failure (SyntaxError): ${source}:42:7` },
])("reports $label without disclosing error data", ({ name, frame, detail }) => {
  const summary = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "ci-retry-reporter-")), "summary.md");
  const token = "token=CANARY-SECRET-123";
  const record = 'private-record={"name":"CANARY-PRIVATE-RECORD","balance":987654}';
  const error = new TypeError(`${token}\n${record}`);
  error.name = name;
  error.stack = `${error.name}: ${error.message}\n    at privateFunction (${frame})`;
  const test = {
    module: { moduleId: path.join(process.cwd(), "tests/ci-retry-reporter.test.ts") },
    fullName: "retry privacy canary",
    diagnostic: () => ({ retryCount: 1 }),
    result: () => ({ state: "passed", errors: [error] }),
  } as unknown as TestCase;
  let output = "";
  const stdout = vi.spyOn(process.stdout, "write").mockImplementation(chunk => { output += String(chunk); return true; });
  vi.stubEnv("GITHUB_STEP_SUMMARY", summary);
  try {
    const reporter = new CiRetryReporter();
    reporter.onTestCaseResult(test);
    reporter.onTestRunEnd();
    for (const text of [output, fs.readFileSync(summary, "utf8")]) {
      for (const privateText of [token, record, "CANARY-PRIVATE-RECORD", "ghpCANARYSECRET789", "CANARY-SECRET-456", "privateFunction", process.cwd()]) {
        expect(text).not.toContain(privateText);
      }
      expect(text).toContain("retry privacy canary (retried 1x, final passed)");
      expect(text).toContain(detail);
    }
  } finally {
    stdout.mockRestore();
    vi.unstubAllEnvs();
  }
});
