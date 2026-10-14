import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

// Match the native Node config probes in test-fleet-isolation.test.ts. Only the
// platform selector is simulated; config evaluation, ownership and exit are real.
const windowsConfigProbe = String.raw`
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { registerHooks } from "node:module";
registerHooks({ resolve(specifier, context, nextResolve) {
  if (specifier.endsWith(".js") && specifier.startsWith(".") && context.parentURL?.startsWith("file:")) {
    const source = new URL(specifier.slice(0, -3) + ".ts", context.parentURL);
    if (fs.existsSync(fileURLToPath(source))) return nextResolve(source.href, context);
  }
  return nextResolve(specifier, context);
}});
Object.defineProperty(process, "platform", { value: "win32" });
const { default: config } = await import(pathToFileURL(path.resolve("vitest.config.ts")).href);
const { isolatedTestTemp, isolateTestFleetEnvironment } = await import(pathToFileURL(path.resolve("scripts/test-temp.ts")).href);
const root = config.test.env.TMPDIR;
// smarty-dev#11145 r5: a private run child of the job root, never the shared job root itself.
assert.equal(path.dirname(root), path.join(process.env.RUNNER_TEMP, "pi-fabric-vitest-job"));
assert.match(path.basename(root), /^pi-fabric-vitest-run-[A-Za-z0-9]{6}$/);
assert.deepEqual(isolatedTestTemp("pi-fabric-vitest-"), { TMPDIR: root, TMP: root, TEMP: root });
const configFleet = path.dirname(process.env.PI_CODING_AGENT_DIR);
const fileFleet = path.dirname(isolateTestFleetEnvironment().PI_CODING_AGENT_DIR);
assert.notEqual(configFleet, fileFleet, "fleet state must remain unique per file");
for (const fleet of [configFleet, fileFleet]) assert.equal(path.dirname(fleet), root);
if (process.env.WINDOWS_TEMP_PROBE_CHILD === "1") {
  console.log(JSON.stringify({ root, fleets: [configFleet, fileFleet] }));
  process.exit(Number(process.env.WINDOWS_TEMP_PROBE_EXIT));
}
const sentinel = path.join(root, "live-parent-sentinel");
fs.writeFileSync(sentinel, "parent still running");
const child = spawnSync(process.execPath, process.execArgv, {
  env: { ...process.env, WINDOWS_TEMP_PROBE_CHILD: "1" }, encoding: "utf8", timeout: 10_000,
});
assert.ifError(child.error);
assert.equal(child.status, Number(process.env.WINDOWS_TEMP_PROBE_EXIT), child.stderr);
const sibling = JSON.parse(child.stdout.trim());
assert.equal(sibling.root, root, "parent and child must share the stable Windows TMP");
assert(fs.existsSync(root), "exiting config probe deleted the live shared Windows TMP parent");
assert.equal(fs.readFileSync(sentinel, "utf8"), "parent still running");
for (const fleet of sibling.fleets) {
  assert(![configFleet, fileFleet].includes(fleet), "processes must not share fleet state");
  assert(!fs.existsSync(fleet), "child-owned fleet roots must be exit-cleaned");
}
for (const fleet of [configFleet, fileFleet]) assert(fs.existsSync(fleet), "live parent's fleet root was removed");
const afterChild = fs.mkdtempSync(path.join(root, "worker-after-child-"));
fs.rmSync(afterChild, { recursive: true, force: true });
console.log(JSON.stringify({ root, sentinel, fleets: [configFleet, fileFleet, ...sibling.fleets] }));
`;

const repo = fileURLToPath(new URL("../", import.meta.url));

describe("test temporary-storage isolation", () => {
  it.each([0, 7])("preserves shared Windows TMP after config-probe exit %i while cleaning owned fleet roots", (exitCode) => {
    const runnerTemp = mkdtempSync(join(tmpdir(), "windows-runner-temp-probe-"));
    try {
      const result = spawnSync(process.execPath, ["--experimental-transform-types", "--input-type=module", "-e", windowsConfigProbe], {
        cwd: repo,
        env: { ...process.env, RUNNER_TEMP: runnerTemp, TMPDIR: runnerTemp, TMP: runnerTemp, TEMP: runnerTemp,
          WINDOWS_TEMP_PROBE_CHILD: "0", WINDOWS_TEMP_PROBE_EXIT: String(exitCode) },
        encoding: "utf8", timeout: 10_000,
      });
      expect(result.error).toBeUndefined();
      expect(result.status, result.stderr).toBe(0);
      const resultPaths = JSON.parse(result.stdout.trim()) as { root: string; sentinel: string; fleets: string[] };
      // The parent has exited too: neither process owns the stable MSYS mount.
      expect(existsSync(resultPaths.root)).toBe(true);
      expect(readFileSync(resultPaths.sentinel, "utf8")).toBe("parent still running");
      expect(new Set(resultPaths.fleets).size).toBe(4);
      for (const fleet of resultPaths.fleets) expect(existsSync(fleet)).toBe(false);
      // Only this fixture's owner removes the simulated runner directory.
    } finally {
      rmSync(runnerTemp, { recursive: true, force: true });
    }
  });
  // smarty-dev#11145 r5: the Windows job temp adopts only a run directory this helper created.
  const helper = new URL("../scripts/test-temp.ts", import.meta.url).href;
  const windowsTemp = (env: Record<string, string>) => {
    const result = spawnSync("bun", ["-e", `
      Object.defineProperty(process, "platform", { value: "win32" });
      const { isolatedTestTemp } = await import(${JSON.stringify(helper)});
      const first = isolatedTestTemp("pi-fabric-vitest-");
      Object.assign(process.env, first); // as vitest.config does
      const again = isolatedTestTemp("pi-fabric-vitest-");
      process.stdout.write(JSON.stringify({ first: first.TMPDIR, again: again.TMPDIR, marker: process.env.PI_TEST_TEMP_JOB_DIR }));
    `], { env: { ...process.env, ...env }, encoding: "utf8", timeout: 10_000 });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(0);
    return JSON.parse(result.stdout) as { first: string; again: string; marker: string };
  };

  it("never adopts a pre-existing job-named directory named by a forged marker", () => {
    const fixture = mkdtempSync(join(tmpdir(), "forged-job-"));
    try {
      const forged = join(fixture, "pi-fabric-vitest-job");
      mkdirSync(forged);
      writeFileSync(join(forged, "victim"), "keep");
      const run = windowsTemp({ RUNNER_TEMP: fixture, TMPDIR: forged, TMP: forged, TEMP: forged, PI_TEST_TEMP_JOB_DIR: forged });
      expect(run.first).not.toBe(forged); // 2432cef9..906efce0: adopted as the guard's delete root
      expect(dirname(run.first)).toBe(join(fixture, "pi-fabric-vitest-job"));
      expect(basename(run.first)).toMatch(/^pi-fabric-vitest-run-[A-Za-z0-9]{6}$/);
      if (process.platform !== "win32") expect(statSync(run.first).mode & 0o777).toBe(0o700); // no mode bits on Windows
      expect(readFileSync(join(forged, "victim"), "utf8")).toBe("keep");
    } finally {
      rmSync(fixture, { recursive: true, force: true });
    }
  });

  it("never adopts the shared job root itself, even when the marker names it", () => {
    const fixture = mkdtempSync(join(tmpdir(), "forged-root-"));
    try {
      const jobRoot = join(fixture, "pi-fabric-vitest-job");
      mkdirSync(jobRoot);
      const run = windowsTemp({ RUNNER_TEMP: fixture, TMPDIR: jobRoot, TMP: jobRoot, TEMP: jobRoot, PI_TEST_TEMP_JOB_DIR: jobRoot });
      expect(run.first).not.toBe(jobRoot);
      expect(basename(run.first)).toMatch(/^pi-fabric-vitest-run-[A-Za-z0-9]{6}$/);
    } finally {
      rmSync(fixture, { recursive: true, force: true });
    }
  });

  // POSIX only: Windows has no uid or mode bits, so there the name and type checks stand alone.
  it.skipIf(process.platform === "win32")("never adopts a run-named directory with another mode", () => {
    const fixture = mkdtempSync(join(tmpdir(), "forged-run-"));
    try {
      const lookalike = join(fixture, "pi-fabric-vitest-job", "pi-fabric-vitest-run-AbCdEf");
      mkdirSync(lookalike, { recursive: true, mode: 0o755 });
      chmodSync(lookalike, 0o755);
      const run = windowsTemp({ RUNNER_TEMP: fixture, TMPDIR: lookalike, TMP: lookalike, TEMP: lookalike, PI_TEST_TEMP_JOB_DIR: lookalike });
      expect(run.first).not.toBe(lookalike);
      expect(basename(run.first)).toMatch(/^pi-fabric-vitest-run-[A-Za-z0-9]{6}$/);
    } finally {
      rmSync(fixture, { recursive: true, force: true });
    }
  });

  it("reuses its own run directory across re-entry and an inheriting worker", () => {
    const fixture = mkdtempSync(join(tmpdir(), "own-run-"));
    try {
      const runner = join(fixture, "runner");
      const caller = join(fixture, "caller");
      mkdirSync(runner);
      mkdirSync(caller);
      const run = windowsTemp({ RUNNER_TEMP: runner, TMPDIR: caller, TMP: caller, TEMP: caller, PI_TEST_TEMP_JOB_DIR: "" });
      expect(dirname(run.first)).toBe(join(runner, "pi-fabric-vitest-job"));
      expect(existsSync(join(caller, "pi-fabric-vitest-job"))).toBe(false);
      expect(run.again).toBe(run.first); // no job/job or run/run nesting on re-entry
      expect(run.marker).toBe(run.first);
      // A worker inherits TMPDIR and the marker from the config process.
      const worker = windowsTemp({ RUNNER_TEMP: runner, TMPDIR: run.first, TMP: run.first, TEMP: run.first, PI_TEST_TEMP_JOB_DIR: run.first });
      expect(worker.first).toBe(run.first);
      expect(worker.again).toBe(run.first);
    } finally {
      rmSync(fixture, { recursive: true, force: true });
    }
  });

  it("keeps workers and inherited child tools away from real session caches", () => {
    expect(tmpdir()).toBe(process.env.TMPDIR);
    expect(process.env.TMP).toBe(tmpdir());
    expect(process.env.TEMP).toBe(tmpdir());
    expect(basename(tmpdir())).toMatch(/^pi-(fabric|fovea|contour)-vitest-/);
    expect(statSync(tmpdir()).isDirectory()).toBe(true);
  });

  it.each([0, 7])("removes mkdtemp-owned artifacts when the runner exits with %i", (exitCode) => {
    const helper = new URL("../scripts/test-temp.ts", import.meta.url).href;
    const result = spawnSync("bun", ["-e", `
      import { isolatedTestTemp } from ${JSON.stringify(helper)};
      import { writeFileSync } from "node:fs";
      import { join } from "node:path";
      const env = isolatedTestTemp("test-temp-probe-");
      writeFileSync(join(env.TMPDIR, "cache.json"), "temporary");
      process.stdout.write(JSON.stringify(env));
      process.exit(${exitCode});
    `], { env: { ...process.env, RUNNER_TEMP: "" }, encoding: "utf8", timeout: 10_000 });
    expect(result.error).toBeUndefined();
    expect(result.stderr).toBe("");
    expect(result.status).toBe(exitCode);
    const env = JSON.parse(result.stdout) as Record<string, string>;
    expect(env.TMPDIR).toBe(env.TMP);
    expect(env.TEMP).toBe(env.TMP);
    expect(existsSync(env.TMPDIR!)).toBe(false);
  });
});
