import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
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
assert.equal(root, path.join(process.env.RUNNER_TEMP, "pi-fabric-vitest-job"));
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
