import fs from "node:fs";
import type { Reporter, TestCase } from "vitest/node";

// Lists every test that CI retried (smarty-dev#7651), on stdout and in the GitHub step summary. A retried test
// passed only on a later attempt or failed every attempt: either way it belongs on #7651 until fixed.
export default class CiRetryReporter implements Reporter {
  readonly #retried: string[] = [];

  onTestCaseResult(test: TestCase): void {
    const retries = test.diagnostic()?.retryCount ?? 0;
    if (retries > 0) this.#retried.push(`${test.module.moduleId} > ${test.fullName} (retried ${retries}x, final ${test.result().state})`);
  }

  onTestRunEnd(): void {
    const text = `CI-RETRIED TESTS (smarty-dev#7651): ${this.#retried.length}\n${(this.#retried.length ? this.#retried : ["none"]).map((line) => `- ${line}`).join("\n")}\n`;
    process.stdout.write(`\n${text}`);
    if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, `\n### ${text}`);
  }
}
