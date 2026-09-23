import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const runner = path.join(projectRoot, "bench", "prewalk", "prewalk-canary-run.mjs");
const verifier = path.join(projectRoot, "bench", "prewalk", "verify-prewalk-canary.mjs");
const fakePiSource = path.join(projectRoot, "bench", "prewalk", "fixtures", "fake-canary-pi.mjs");

const roots: string[] = [];
const temporary = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "prewalk-task-check-"));
  roots.push(dir);
  return dir;
};
afterAll(() => {
  for (const dir of roots) fs.rmSync(dir, { recursive: true, force: true });
});

// Two tests, one failure: the artifact is genuinely not passing, which is the
// case a clean child exit must never be allowed to hide.
const FAILING_TEST = [
  "import test from 'node:test';",
  "import assert from 'node:assert/strict';",
  "test('holds', () => { assert.equal(1, 1); });",
  "test('loses', () => { assert.equal(1, 2); });",
  "",
].join("\n");

interface Receipt {
  ok: boolean;
  exitCode: number;
  counts: Record<string, number | null>;
  artifact: { sha256After: string | null; unchangedDuringCheck: boolean };
}

// The runner passes Pi-style flags to the configured binary, so the fake must
// run through its shebang exactly like the existing canary subprocess tests.
const makeFakePi = (root: string) => {
  const fakePi = path.join(root, "fake-pi");
  fs.copyFileSync(fakePiSource, fakePi);
  fs.chmodSync(fakePi, 0o755);
  return fakePi;
};

describe.skipIf(process.platform === "win32")("prewalk-canary independent task verification", () => {
  it("records a failing artifact without failing the lifecycle and keeps the axes apart", () => {
    const root = temporary();
    const cwd = path.join(root, "work");
    fs.mkdirSync(path.join(cwd, "tests"), { recursive: true });
    fs.writeFileSync(path.join(cwd, "tests", "artifact.test.mjs"), FAILING_TEST);
    const spec = path.join(root, "spec.json");
    fs.writeFileSync(spec, JSON.stringify({ testFile: "tests/artifact.test.mjs" }) + "\n");
    const promptFile = path.join(root, "prompt.txt");
    fs.writeFileSync(promptFile, "one-shot cell\n");
    const out = path.join(root, "cell");

    const run = spawnSync(
      process.execPath,
      [runner, "--out", out, "--cwd", cwd, "--prompt-file", promptFile, "--pi-binary", makeFakePi(root), "--task-check", spec],
      { encoding: "utf8", timeout: 60_000, env: { ...process.env, FAKE_PI_MODE: "ok" } },
    );
    // The lifecycle is clean: a failing task check must not become a runner problem.
    expect(run.status).toBe(0);
    const finished = JSON.parse(fs.readFileSync(path.join(out, "finished.json"), "utf8")) as {
      ok: boolean;
      problems: string[];
    };
    expect(finished.ok).toBe(true);
    expect(finished.problems).toEqual([]);

    const receipt = JSON.parse(fs.readFileSync(path.join(out, "task-check.json"), "utf8")) as Receipt;
    expect(receipt.ok).toBe(false);
    expect(receipt.exitCode).toBe(1);
    expect(receipt.counts).toMatchObject({ tests: 2, pass: 1, fail: 1 });
    expect(receipt.artifact.unchangedDuringCheck).toBe(true);
    expect(receipt.artifact.sha256After).toMatch(/^[0-9a-f]{64}$/);

    const started = JSON.parse(fs.readFileSync(path.join(out, "started.json"), "utf8")) as {
      cwd: string;
      taskCheck: { sha256: string } | null;
    };
    expect(started.cwd).toBe(cwd);
    expect(started.taskCheck?.sha256).toMatch(/^[0-9a-f]{64}$/);

    const verify = spawnSync(
      process.execPath,
      [verifier, "--run", out, "--task-check-report", path.join(out, "task-check.json")],
      { encoding: "utf8", timeout: 30_000 },
    );
    expect(verify.status).toBe(1);
    const report = JSON.parse(verify.stdout) as {
      checks: Array<{ name: string; status: string; detail: unknown }>;
      axes: Record<string, Array<{ name: string; status: string }>>;
    };
    const artifact = report.checks.find((check) => check.name === "task-verification");
    expect(artifact?.status).toBe("fail");
    expect(JSON.stringify(artifact?.detail)).toContain("tests failed: 1 of 2");
    expect(report.axes.artifact).toEqual([{ name: "task-verification", status: "fail" }]);
    expect(report.checks.find((check) => check.name === "finished-ok")?.status).toBe("pass");
    expect(report.checks.find((check) => check.name === "recording-complete")?.status).toBe("pass");
    // No scope report was supplied, so no scope verdict is invented.
    expect(report.axes.scope).toBeUndefined();
  });

  it("refuses a spec that carries a command or escapes the work directory", () => {
    const root = temporary();
    const cwd = path.join(root, "work");
    fs.mkdirSync(cwd, { recursive: true });
    fs.writeFileSync(path.join(cwd, "a.test.mjs"), "// noop\n");
    const promptFile = path.join(root, "prompt.txt");
    fs.writeFileSync(promptFile, "cell\n");
    const cases: Array<[string, string]> = [
      ["command", '{"testFile":"a.test.mjs","command":"rm -rf /"}'],
      ["escape", '{"testFile":"../../a.test.mjs"}'],
      ["absolute", '{"testFile":"/etc/passwd"}'],
    ];
    for (const [name, body] of cases) {
      const spec = path.join(root, `spec-${name}.json`);
      fs.writeFileSync(spec, body + "\n");
      const out = path.join(root, `cell-${name}`);
      const result = spawnSync(
        process.execPath,
        [runner, "--out", out, "--cwd", cwd, "--prompt-file", promptFile, "--pi-binary", "/bin/false", "--task-check", spec],
        { encoding: "utf8", timeout: 30_000, env: { ...process.env, FAKE_PI_MODE: "ok" } },
      );
      expect(result.status).toBe(1);
      expect(`${result.stdout}${result.stderr}`).toMatch(/unknown field|relative POSIX path/);
      // Rejected before launch: no cell is started at all.
      expect(fs.existsSync(path.join(out, "started.json"))).toBe(false);
    }
  });

  it("normalizes a trailing separator on --cwd and --out before containment and provenance", () => {
    const root = temporary();
    const cwd = path.join(root, "work");
    fs.mkdirSync(path.join(cwd, "tests"), { recursive: true });
    fs.writeFileSync(path.join(cwd, "tests", "artifact.test.mjs"), FAILING_TEST);
    const spec = path.join(root, "spec.json");
    fs.writeFileSync(spec, JSON.stringify({ testFile: "tests/artifact.test.mjs" }) + "\n");
    const promptFile = path.join(root, "prompt.txt");
    fs.writeFileSync(promptFile, "one-shot cell\n");
    const out = path.join(root, "cell");

    const run = spawnSync(
      process.execPath,
      [
        runner,
        "--out", out + path.sep,
        "--cwd", cwd + path.sep,
        "--prompt-file", promptFile,
        "--pi-binary", makeFakePi(root),
        "--task-check", spec,
      ],
      { encoding: "utf8", timeout: 60_000, env: { ...process.env, FAKE_PI_MODE: "ok" } },
    );
    expect(run.status, `${run.stdout}${run.stderr}`).toBe(0);
    const receipt = JSON.parse(fs.readFileSync(path.join(out, "task-check.json"), "utf8")) as Receipt;
    expect(receipt.artifact.unchangedDuringCheck).toBe(true);
    expect(receipt.counts).toMatchObject({ tests: 2, pass: 1, fail: 1 });
    // Provenance carries the resolved directory, not the raw argument.
    const started = JSON.parse(fs.readFileSync(path.join(out, "started.json"), "utf8")) as { cwd: string };
    expect(started.cwd).toBe(cwd);
  });
});
