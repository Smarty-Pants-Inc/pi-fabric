import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const runner = path.join(projectRoot, "bench", "prewalk", "prewalk-canary-run.mjs");
const fakePiSource = path.join(projectRoot, "bench", "prewalk", "fixtures", "fake-canary-pi.mjs");

const roots: string[] = [];
const temporary = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "prewalk-canary-abort-"));
  roots.push(dir);
  return dir;
};
afterAll(() => {
  for (const dir of roots) fs.rmSync(dir, { recursive: true, force: true });
});

interface Finished {
  ok: boolean;
  timedOut: boolean;
  problems: string[];
  exit: { code: number | null; signal: string | null };
  gracefulAbort: { attempted: boolean; response: string | null; settled: boolean } | null;
}

// The runner passes Pi-style flags to the configured binary, so the fake must
// run through its shebang (copied and made executable), exactly like the
// existing canary subprocess tests; plain `node <fixture>` would receive those
// flags as node options and exit immediately.
const makeFakePi = (root: string) => {
  const fakePi = path.join(root, "fake-pi");
  fs.copyFileSync(fakePiSource, fakePi);
  fs.chmodSync(fakePi, 0o755);
  return fakePi;
};

const runCell = (root: string, name: string, extra: string[], options: { rpc?: boolean; timeoutMs?: number } = {}) => {
  const out = path.join(root, name);
  const cwd = path.join(root, "cwd");
  fs.mkdirSync(cwd, { recursive: true });
  const promptFile = path.join(root, "prompt.txt");
  fs.writeFileSync(promptFile, "hang until the timeout, then abort\n");
  return spawnSync(
    process.execPath,
    [
      runner,
      "--out",
      out,
      "--cwd",
      cwd,
      "--prompt-file",
      promptFile,
      "--pi-binary",
      makeFakePi(root),
      ...(options.rpc === false ? [] : ["--rpc", "--rpc-runs", "2"]),
      ...extra,
    ],
    { encoding: "utf8", timeout: options.timeoutMs ?? 90_000, env: { ...process.env, FAKE_PI_MODE: "rpc-hang" } },
  );
};

describe.skipIf(process.platform === "win32")("prewalk-canary-run graceful abort", () => {
  it("--abort-grace-seconds aborts through the documented RPC command before escalation", () => {
    const root = temporary();
    const result = runCell(root, "grace", ["--timeout-seconds", "2", "--abort-grace-seconds", "5"]);
    expect(result.status).toBe(1);
    const started = JSON.parse(fs.readFileSync(path.join(root, "grace", "started.json"), "utf8")) as { abortGraceSeconds: number };
    expect(started.abortGraceSeconds).toBe(5);
    const finished = JSON.parse(fs.readFileSync(path.join(root, "grace", "finished.json"), "utf8")) as Finished;
    expect(finished.timedOut).toBe(true);
    expect(finished.gracefulAbort).toEqual({ attempted: true, response: "accepted", settled: true });
    expect(finished.exit.code).toBe(0);
    expect(finished.problems.join("\n")).toContain("graceful RPC abort completed");
    expect(fs.readFileSync(path.join(root, "grace", "telemetry.jsonl"), "utf8")).toContain("session_shutdown");
  });

  it("the default timeout still hard-kills the process group without an RPC abort", () => {
    const root = temporary();
    const result = runCell(root, "hard", ["--timeout-seconds", "2"]);
    expect(result.status).toBe(1);
    const finished = JSON.parse(fs.readFileSync(path.join(root, "hard", "finished.json"), "utf8")) as Finished;
    expect(finished.timedOut).toBe(true);
    expect(finished.gracefulAbort).toEqual({ attempted: false, response: null, settled: false });
    expect(finished.problems.join("\n")).toContain("killed the owned process group");
  });

  it("rejects a graceful abort outside RPC mode", () => {
    const root = temporary();
    const result = runCell(root, "invalid", ["--timeout-seconds", "2", "--abort-grace-seconds", "5"], {
      rpc: false,
      timeoutMs: 30_000,
    });
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("--abort-grace-seconds requires --rpc");
  });
});
