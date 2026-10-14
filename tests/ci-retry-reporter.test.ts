import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import type { TestCase } from "vitest/node";
import CiRetryReporter from "../scripts/ci-retry-reporter.js";

it("reports a retry's class and source location without disclosing its error data", () => {
  const summary = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "ci-retry-reporter-")), "summary.md");
  const token = "token=CANARY-SECRET-123";
  const record = 'private-record={"name":"CANARY-PRIVATE-RECORD","balance":987654}';
  const error = new TypeError(`${token}\n${record}`);
  error.stack = `${error.name}: ${error.message}\n    at privateFunction (${process.cwd()}/tests/ci-retry-reporter.test.ts:42:7)`;
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
      for (const privateText of [token, record, "CANARY-PRIVATE-RECORD", "privateFunction", process.cwd()]) {
        expect(text).not.toContain(privateText);
      }
      expect(text).toContain("retry privacy canary (retried 1x, final passed)");
      expect(text).toContain("first failure (TypeError): tests/ci-retry-reporter.test.ts:42:7");
    }
  } finally {
    stdout.mockRestore();
    vi.unstubAllEnvs();
  }
});
