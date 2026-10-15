import { spawnSync } from "node:child_process";
import fs, { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { sweepStaleTestTemp, testTempOwnerMarker, testTempProcessIdentity } from "../scripts/test-temp.js";

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
const { default: config } = await import(pathToFileURL(path.resolve("vitest.config.cts")).href);
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

describe.skipIf(process.platform === "win32")("launcher stale-root trust", () => {
  const marker = { version: 1, pid: 12345, startIdentity: "recorded-boot:123" };
  const fixture = () => mkdtempSync(join(tmpdir(), "stale-root-fixture-"));
  const markedRoot = (base: string) => {
    const root = mkdtempSync(join(base, "pi-fabric-vitest-"));
    writeFileSync(join(root, testTempOwnerMarker), JSON.stringify(marker), { mode: 0o600 });
    return root;
  };
  // Record requested removals, never authorize a stale-root sweep to delete a
  // real process's temp tree. Only the fixture owner performs actual teardown.
  const recordedSweep = (base: string, identity: (pid: number) => string | null | undefined) => {
    const remove = vi.spyOn(fs, "rmSync").mockImplementation(() => {});
    try {
      sweepStaleTestTemp(base, identity);
      return remove.mock.calls.map(([path]) => String(path));
    } finally { remove.mockRestore(); }
  };

  it("requests removal of a harness-marked private root with a confirmed dead owner pid", () => {
    const base = fixture();
    try {
      const root = markedRoot(base);
      const seen: number[] = [];
      expect(recordedSweep(base, (pid) => { seen.push(pid); return null; })).toEqual([root]);
      expect(seen).toEqual([marker.pid]);
      expect(existsSync(root)).toBe(true); // recorder, not a real stale-root deletion
    } finally { rmSync(base, { recursive: true, force: true }); }
  });

  it("requests removal when a reused pid has a different native start identity", () => {
    const base = fixture();
    try {
      const root = markedRoot(base);
      expect(recordedSweep(base, () => "new-boot:456")).toEqual([root]);
    } finally { rmSync(base, { recursive: true, force: true }); }
  });

  it("preserves live-owned and unverifiable roots despite a matching harness marker", () => {
    const base = fixture();
    try {
      markedRoot(base);
      expect(recordedSweep(base, () => marker.startIdentity)).toEqual([]);
      expect(recordedSweep(base, () => undefined)).toEqual([]);
    } finally { rmSync(base, { recursive: true, force: true }); }
  });

  it("preserves markerless malformed unsafe-mode and nested lookalike roots", () => {
    const base = fixture();
    try {
      const markerless = mkdtempSync(join(base, "pi-fabric-vitest-"));
      writeFileSync(join(markerless, "sentinel"), "keep");
      const malformed = markedRoot(base);
      writeFileSync(join(malformed, testTempOwnerMarker), JSON.stringify({ version: 1, pid: 12345 }));
      const unsafe = markedRoot(base);
      chmodSync(unsafe, 0o755);
      const unsafeMarker = markedRoot(base);
      chmodSync(join(unsafeMarker, testTempOwnerMarker), 0o644);
      const container = join(base, "not-a-run");
      mkdirSync(container);
      markedRoot(container);
      expect(recordedSweep(base, () => null)).toEqual([]);
      expect(readFileSync(join(markerless, "sentinel"), "utf8")).toBe("keep");
    } finally { rmSync(base, { recursive: true, force: true }); }
  });

  it("preserves symlinked roots markers and caller bases", () => {
    const base = fixture();
    try {
      const target = markedRoot(base);
      const link = join(base, "pi-fabric-vitest-AbCdEf");
      symlinkSync(target, link, "dir");
      const markerLinkRoot = mkdtempSync(join(base, "pi-fabric-vitest-"));
      symlinkSync(join(target, testTempOwnerMarker), join(markerLinkRoot, testTempOwnerMarker));
      const callerLink = join(base, "caller-link");
      symlinkSync(base, callerLink, "dir");
      expect(recordedSweep(base, () => marker.startIdentity)).toEqual([]);
      // Only target is eligible; neither symlink can become a deletion root.
      expect(recordedSweep(base, () => null)).toEqual([target]);
      expect(recordedSweep(callerLink, () => null)).toEqual([]);
    } finally { rmSync(base, { recursive: true, force: true }); }
  });

  it.each(["root", "marker", "parent"])("refuses a %s replacement after ownership was read", (replacement) => {
    const base = fixture();
    const savedParent = `${base}-saved`;
    try {
      const root = markedRoot(base);
      let replaced = false;
      expect(recordedSweep(base, () => {
        if (!replaced) {
          replaced = true;
          if (replacement === "root") {
            renameSync(root, `${root}-saved`);
            mkdirSync(root, { mode: 0o700 });
            writeFileSync(join(root, testTempOwnerMarker), JSON.stringify(marker), { mode: 0o600 });
          } else if (replacement === "marker") {
            renameSync(join(root, testTempOwnerMarker), join(root, "saved-marker"));
            writeFileSync(join(root, testTempOwnerMarker), JSON.stringify(marker), { mode: 0o600 });
          } else {
            renameSync(base, savedParent);
            mkdirSync(base, { mode: 0o700 });
          }
        }
        return null;
      })).toEqual([]);
      expect(replaced).toBe(true);
    } finally {
      rmSync(base, { recursive: true, force: true });
      rmSync(savedParent, { recursive: true, force: true });
    }
  });

  it("reads the current owner's native identity and confirms an exited child is dead", () => {
    expect(typeof testTempProcessIdentity(process.pid)).toBe("string");
    const child = spawnSync(process.execPath, ["-e", ""], { timeout: 10_000 });
    expect(child.error).toBeUndefined();
    expect(child.status).toBe(0);
    expect(testTempProcessIdentity(child.pid)).toBeNull();
  });

  it("reuses a launcher's inherited root without child teardown deleting it", () => {
    const helper = new URL("../scripts/test-temp.ts", import.meta.url).href;
    const result = spawnSync("bun", ["-e", `
      import assert from "node:assert/strict";
      import fs from "node:fs";
      import { spawnSync } from "node:child_process";
      import { launcherTestTemp, cleanupIsolatedTestTemp } from ${JSON.stringify(helper)};
      const temp = launcherTestTemp();
      Object.assign(process.env, temp);
      const child = spawnSync("bun", ["-e", ${JSON.stringify(`
        import { isolatedTestTemp, cleanupIsolatedTestTemp } from ${JSON.stringify(helper)};
        const temp = isolatedTestTemp("pi-fabric-vitest-");
        cleanupIsolatedTestTemp();
        console.log(JSON.stringify(temp));
      `)}], { env: process.env, encoding: "utf8", timeout: 10000 });
      assert.ifError(child.error);
      assert.equal(child.status, 0, child.stderr);
      assert.deepEqual(JSON.parse(child.stdout), temp);
      assert(fs.existsSync(temp.TMPDIR));
      cleanupIsolatedTestTemp();
      assert(!fs.existsSync(temp.TMPDIR));
    `], { env: { ...process.env, RUNNER_TEMP: "" }, encoding: "utf8", timeout: 15_000 });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(0);
  });
});

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

  it.skipIf(process.platform === "win32").each([0, 7])("runs registered teardown before process exit %i without deleting its caller", (exitCode) => {
    const caller = mkdtempSync(join(tmpdir(), "teardown-probe-"));
    try {
      const config = new URL("../vitest.config.cts", import.meta.url).href;
      const result = spawnSync("bun", ["-e", `
        import assert from "node:assert/strict";
        import fs from "node:fs";
        import path from "node:path";
        const caller = process.env.TMPDIR;
        const { default: config } = await import(${JSON.stringify(config)});
        const root = config.test.env.TMPDIR;
        assert.equal(path.dirname(root), caller);
        assert.deepEqual(config.test.globalSetup, ["./scripts/test-temp-global-setup.ts"]);
        for (const project of config.test.projects) assert.equal(project.test.globalSetup, undefined);
        const { default: setup } = await import(path.resolve(config.test.globalSetup[0]));
        const teardown = setup();
        try { fs.writeFileSync(path.join(root, "artifact"), "temporary"); }
        finally { teardown(); }
        assert(!fs.existsSync(root));
        teardown();
        assert(fs.existsSync(caller));
        console.log(JSON.stringify({root}));
        process.exit(${exitCode});
      `], { cwd: repo, env: { ...process.env, RUNNER_TEMP: "", TMPDIR: caller, TMP: caller, TEMP: caller }, encoding: "utf8", timeout: 15_000 });
      expect(result.error).toBeUndefined();
      expect(result.status, result.stderr).toBe(exitCode);
      const paths = JSON.parse(result.stdout.trim()) as { root: string };
      expect(existsSync(paths.root)).toBe(false);
      expect(readdirSync(caller)).toEqual([]);
    } finally {
      rmSync(caller, { recursive: true, force: true });
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
