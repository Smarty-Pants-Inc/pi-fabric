import fs from "node:fs";
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
      const frame = error.stack?.split("\n").find(line => {
        const normalized = line.replace(/\\/g, "/");
        return line.trimStart().startsWith("at ") && normalized.includes(root) && !normalized.includes("/node_modules/");
      });
      const message = error.message.split(/\r?\n/).slice(0, 2).map(line => `    ${line.slice(0, 240)}`).join("\n");
      detail = `  first failure (${error.name ?? "Error"}):\n${message}${frame ? `\n    ${frame.trim().slice(0, 300)}` : ""}`;
    }
    this.#retried.push(`${test.module.moduleId} > ${test.fullName} (retried ${retries}x, final ${result.state})\n${detail}`);
  }

  onTestRunEnd(): void {
    const text = `CI-RETRIED TESTS (smarty-dev#7651): ${this.#retried.length}\n${(this.#retried.length ? this.#retried : ["none"]).map((line) => `- ${line}`).join("\n")}\n`;
    process.stdout.write(`\n${text}`);
    if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, `\n### ${text}`);
  }
}
