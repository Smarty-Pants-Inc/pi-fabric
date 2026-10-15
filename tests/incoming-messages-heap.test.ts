import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildSync } from "esbuild";
import { describe, expect, it } from "vitest";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const probe = path.join(root, "tests/fixtures/incoming-heap-probe.ts");
const MiB = 1024 * 1024;

const runProbe = (mode?: "--slice-control"): Record<string, unknown> => {
  // Bundle local TS only, in memory. Node evaluates stdin from the repo root so
  // external host packages resolve in this repo, not an unrelated /tmp directory.
  const bundle = buildSync({
    absWorkingDir: root,
    entryPoints: [probe],
    bundle: true,
    write: false,
    platform: "node",
    format: "esm",
    target: "node24",
    packages: "external",
  }).outputFiles[0]!.text;
  // Do not use process.execPath: the Vitest parent may itself be running on Bun.
  const result = spawnSync("node", [
    "--expose-gc", "--max-old-space-size=2048", "--input-type=module", "-", ...(mode ? [mode] : []),
  ], {
    cwd: root,
    input: bundle,
    encoding: "utf8",
    timeout: 90_000,
    killSignal: "SIGKILL",
    maxBuffer: 1024 * 1024,
  });
  expect(result.error, result.error?.message).toBeUndefined();
  expect(result.signal, result.stderr).toBeNull();
  expect(result.status, result.stderr).toBe(0);
  return JSON.parse(result.stdout.trim()) as Record<string, unknown>;
};

// Keep reason: P0 #10720's historical index/string-parent retention, measured
// through the real registered renderer with production-sized state on native V8.
describe("incoming display index heap counterprobe (smarty-dev#10720)", () => {
  it("releases 100 × 4 MiB parents and historical objects while the index stays live", () => {
    const report = runProbe();
    expect(report).toMatchObject({
      runtime: "node-v8",
      historyCount: 100,
      parentBytes: 4 * MiB,
      retainedCapBytes: 5 * MiB,
      oldSuppressed: true,
      appendSuppressed: true,
      sameLengthResetShown: true,
      stableIdsSuppress: true,
      shrinkResetShown: true,
      versionResetShown: true,
      sessionResetShown: true,
      slicedIdProbe: true,
      historySuppressed: true,
      weakRefCount: 101,
      branchCollected: true,
      entriesCollected: true,
      collected: true,
      heapUnderCap: true,
      rendererLive: true,
      usableAfterGc: true,
    });
    expect(report.retainedBytes).toBeTypeOf("number");
    expect(report.retainedBytes as number).toBeLessThan(5 * MiB);
  }, 100_000);

  it("confirms undetached 32-character ID slices retain over 350 MiB on native V8", () => {
    const report = runProbe("--slice-control");
    expect(report).toMatchObject({
      runtime: "node-v8",
      held: 100,
      parentBytes: 4 * MiB,
      uniqueIds: true,
      slicedIdProbe: true,
      retainsParents: true,
    });
    expect(report.delta).toBeTypeOf("number");
    expect(report.delta as number).toBeGreaterThan(350 * MiB);
  }, 100_000);
});
