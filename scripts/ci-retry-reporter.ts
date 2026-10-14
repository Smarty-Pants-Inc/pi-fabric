import fs from "node:fs";
import path from "node:path";
import type { Reporter, TestCase } from "vitest/node";

const errorClasses = new Set(["Error", "AssertionError", "TypeError", "RangeError", "ReferenceError", "SyntaxError", "EvalError", "URIError", "AggregateError"]);

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
      const name = typeof error.name === "string" && errorClasses.has(error.name) ? error.name : "Error";
      const safeRelative = (file: string): boolean => /^[A-Za-z0-9._\/-]{1,200}$/.test(file) &&
        !path.isAbsolute(file) && !file.split("/").includes("..");
      try {
        const root = fs.realpathSync(process.cwd());
        for (const line of typeof error.stack === "string" ? error.stack.split("\n") : []) {
          const normalized = line.replace(/\\/g, "/").trim();
          if (!normalized.startsWith("at ")) continue;
          const location = normalized.endsWith(")") ? normalized.slice(normalized.lastIndexOf("(") + 1, -1) : normalized.slice(3);
          const frame = location.match(/^([^()\r\n]+):([0-9]{1,7}):([0-9]{1,7})$/);
          if (!frame || !path.isAbsolute(frame[1]!) || frame[1]!.split("/").includes("..")) continue;
          const candidate = path.relative(process.cwd(), frame[1]!).replace(/\\/g, "/");
          if (!safeRelative(candidate) || candidate.split("/").includes("node_modules")) continue;
          const resolved = path.resolve(root, candidate);
          if (!fs.existsSync(resolved)) continue;
          const file = fs.realpathSync(resolved);
          const relative = path.relative(root, file).replace(/\\/g, "/");
          if (!safeRelative(relative) || relative.split("/").includes("node_modules") || !fs.statSync(file).isFile()) continue;
          // Error metadata is untrusted: emit only a validated class and an existing in-root file location.
          detail = `  first failure (${name}): ${relative}:${frame[2]}:${frame[3]}`;
          break;
        }
      } catch { /* Unverifiable source location: keep the unavailable diagnostic. */ }
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
