import fs from "node:fs";
import path from "node:path";
import type { Reporter, TestCase } from "vitest/node";

// Lists every test that CI retried (smarty-dev#7651), on stdout and in the GitHub step summary. A retried test
// passed only on a later attempt or failed every attempt: either way it belongs on #7651 until fixed.
export default class CiRetryReporter implements Reporter {
  readonly #retried: string[] = [];

  onTestCaseResult(test: TestCase): void {
    const retries = test.diagnostic()?.retryCount ?? 0;
    if (retries === 0) return;
    const result = test.result();
    let detail = "  first failure: unavailable";
    // Vitest 4 retains errors in attempt order, including when a later retry passes.
    const error = result.errors?.[0];
    if (error) {
      const root = process.cwd().replace(/\\/g, "/") + "/";
      const location = error.stack?.split("\n").map(line => {
        const normalized = line.replace(/\\/g, "/").trim();
        if (!normalized.startsWith("at ") || !normalized.includes(root) || normalized.includes("/node_modules/")) return undefined;
        return normalized.slice(normalized.lastIndexOf(root) + root.length).match(/^([^()\r\n]+:\d+:\d+)\)?$/)?.[1];
      }).find(location => location !== undefined && location.length <= 300);
      // Error messages can contain credentials or private records. Emit only the class and source location.
      if (location) detail = `  first failure (${error.name ?? "Error"}): ${location}`;
    }
    const moduleId = path.relative(process.cwd(), test.module.moduleId).replace(/\\/g, "/");
    this.#retried.push(`${moduleId} > ${test.fullName} (retried ${retries}x, final ${result.state})\n${detail}`);
  }

  onTestRunEnd(): void {
    const text = `CI-RETRIED TESTS (smarty-dev#7651): ${this.#retried.length}\n${(this.#retried.length ? this.#retried : ["none"]).map((line) => `- ${line}`).join("\n")}\n`;
    process.stdout.write(`\n${text}`);
    if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, `\n### ${text}`);
  }
}
