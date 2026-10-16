import { spawnSync } from "node:child_process";
import path from "node:path";
import { buildSync } from "esbuild";
import { describe, expect, it } from "vitest";

// #10720: real retained output, not source-pattern assertions. A separate V8
// process gives a clean heap baseline and GC without changing Vitest's pool.
// Bundle in memory: no artifact cleanup/deletions, installation or dist build.
describe("bounded tail previews release oversized parents", () => {
  it("preserves output bytes without retaining large sliced parents", () => {
    const bundle = buildSync({
      entryPoints: [path.resolve("tests/fixtures/tail-preview-heap-probe.ts")],
      bundle: true, write: false, platform: "node", format: "esm", target: "node24",
      packages: "external",
    });
    const result = spawnSync(process.execPath, ["--expose-gc", "--input-type=module"], {
      input: bundle.outputFiles[0]!.text,
      encoding: "utf8", timeout: 90_000, maxBuffer: 1024 * 1024,
      cwd: path.resolve("."),
    });
    expect(result.error, result.stderr).toBeUndefined();
    expect(result.status, result.stderr).toBe(0);
    const report = JSON.parse(result.stdout.trim()) as {
      size: number; count: number; limit: number; controlMin: number;
      samples: Array<{ name: string; retainedBytes: number; limitBytes: number; pass: boolean }>;
    };
    expect(report.samples.map(sample => sample.name)).toEqual([
      "negative-control", "detached-prefix", "structured-head-tail", "actor-state-excerpts",
      "activation-tool-excerpt", "projection-held-key", "native-reader-error",
      "participant-trimmed-fields", "classifier-reason-and-evidence", "compaction-alarm-fields",
    ]);
    expect(report.count).toBe(100);
    expect(report.size).toBe(4 * 1024 * 1024);
    expect(report.controlMin).toBe(350 * 1024 * 1024);
    expect(report.samples[0]!.retainedBytes).toBeGreaterThan(report.controlMin);
    for (const sample of report.samples.slice(1)) {
      expect(sample.retainedBytes, sample.name).toBeLessThan(sample.limitBytes);
    }
    console.info("tail preview retained-heap receipts", JSON.stringify(report));
  }, 100_000);
});
