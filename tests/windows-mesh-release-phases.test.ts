import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

// This scoped script has no declaration companion; exercise its actual exports, not source-text assertions.
const { PHASE_PAIRS, aggregatePhases, phaseOrder, releaseChildEnv, validateCapability,
  validateNoopResult, validateNoopSample, validateReleaseSamples } = await import(`../scripts/${"windows-mesh-release-phases"}.mjs`);

// Keep reason (#1191): trust-boundary identity checks and malformed/partial measurement failure paths,
// without native helpers, lock mutation, deletion, or a real MeshStore. Characterization, not a product bug fix.
const capability = { absolutePath: "C:\\Users\\runner\\pi-fabric\\native\\pinned\\fabric-win-remove.exe", digest: "a".repeat(64), meshAbi: 1 };
const head = "b".repeat(40);
const noopResult = () => ({ status: 0, signal: null, error: null, stdout: "FMRCAP1 mesh-retire-v1 abi=1\r\n", stderr: "" });
const noop = (elapsedMs = 7) => ({ ...noopResult(), absolutePath: capability.absolutePath, digest: capability.digest, elapsedMs });
const release = (elapsedMs = 10) => ({ absolutePath: capability.absolutePath, digest: capability.digest,
  elapsedMs, attempt: 1, outcome: "released" });
const pair = (index: number, a = 7, b = 10) => ({ index, order: phaseOrder(index), a: noop(a), b: [release(b)] });
const input = () => ({ head, capability, packagedDigest: capability.digest,
  pairs: Array.from({ length: PHASE_PAIRS }, (_, index) => pair(index)), warmups: [pair(0, 999, 999)] });

describe("Windows release phase characterization (#1191)", () => {
  it("aggregates exactly 100 alternating pairs, excludes the reported warmup, and makes only a heuristic decision", () => {
    const measured = input();
    const result = aggregatePhases(measured);
    expect(result.measuredPairCount).toBe(100);
    expect(result.pairs).toEqual(measured.pairs);
    expect(result.warmups.pairs).toEqual(measured.warmups);
    expect(result.phases.a).toMatchObject({ head, helperDigest: capability.digest, status: "measured", count: 100, p50Ms: 7, p95Ms: 7, totalMs: 700 });
    expect(result.phases.b).toMatchObject({ count: 100, p50Ms: 10, p95Ms: 10, totalMs: 1000 });
    expect(result.heuristic).toMatchObject({ aOverB: 0.7, decision: "suggests-process-transport-class" });
    expect(result.phases.c).toMatchObject({ status: "not-separable", count: 0, p50Ms: null, p95Ms: null, totalMs: null });
    expect(result.residual.samples).toEqual(new Array(100).fill(3));
    expect(result.residual.provenance).toContain("NOT pure verification");
    measured.pairs[0] = pair(0, 12, 10);
    expect(aggregatePhases(measured).residual.samples[0]).toBe(-2);
    measured.pairs = measured.pairs.map((_, index) => pair(index, 1, index + 1));
    expect(aggregatePhases(measured).phases.b).toMatchObject({ p50Ms: 50, p95Ms: 95 });
    expect(aggregatePhases(measured).heuristic.decision).toBe("inconclusive");
    measured.pairs = measured.pairs.map((_, index) => pair(index, 0, 0));
    expect(aggregatePhases(measured).heuristic).toMatchObject({ aOverB: null, decision: "inconclusive" });
  });

  it("rejects missing, extra, reordered, retried and unreleased operations rather than accepting misleading data", () => {
    for (const count of [0, 99, 101]) {
      const measured = input();
      measured.pairs = Array.from({ length: count }, (_, index) => pair(index));
      expect(() => aggregatePhases(measured)).toThrow(/exactly 100/);
    }
    const reordered = input();
    reordered.pairs[1]!.order = ["a", "b"];
    expect(() => aggregatePhases(reordered)).toThrow(/index\/order/);
    const sparse = input();
    delete sparse.pairs[0];
    expect(() => aggregatePhases(sparse)).toThrow(/index\/order/);
    expect(() => aggregatePhases({ ...input(), warmups: [] })).toThrow(/warmup/);
    expect(() => aggregatePhases({ ...input(), head: "unknown" })).toThrow(/exact head/);
    for (const samples of [[], [release(), release()], [{ ...release(), attempt: 2 }], [{ ...release(), outcome: "not-owner" }]]) {
      expect(() => validateReleaseSamples(samples, capability)).toThrow();
    }
    const invalid = input();
    invalid.pairs[0]!.b.push(release());
    expect(() => aggregatePhases(invalid)).toThrow(/exactly one release/);
  });

  it("pins both modes to the same captured adapter capability and requires finite nonnegative elapsed samples", () => {
    expect(() => validateCapability(capability, capability.digest)).not.toThrow();
    for (const changed of [{ ...capability, digest: "c".repeat(64) }, { ...capability, absolutePath: "relative.exe" }, { ...capability, meshAbi: 2 }]) {
      expect(() => validateCapability(changed, capability.digest)).toThrow();
    }
    for (const mismatch of [{ absolutePath: `${capability.absolutePath}.other` }, { digest: "c".repeat(64) }]) {
      expect(() => validateNoopSample({ ...noop(), ...mismatch }, capability)).toThrow(/path\/digest/);
      expect(() => validateReleaseSamples([{ ...release(), ...mismatch }], capability)).toThrow(/path\/digest/);
    }
    for (const elapsedMs of [NaN, Infinity, -Infinity, -1]) {
      expect(() => validateNoopSample(noop(elapsedMs), capability)).toThrow(/elapsed/);
      expect(() => validateReleaseSamples([release(elapsedMs)], capability)).toThrow(/elapsed/);
    }
    const invalid = input();
    invalid.pairs[0]!.a.elapsedMs = NaN;
    expect(() => aggregatePhases(invalid)).toThrow(/elapsed/);
    const overflow = input();
    overflow.pairs = overflow.pairs.map((_, index) => pair(index, Number.MAX_VALUE, Number.MAX_VALUE));
    expect(() => aggregatePhases(overflow)).toThrow(/nonfinite total/);
    expect(() => validateReleaseSamples([release(0)], capability)).not.toThrow();
  });

  it("strictly validates capability output/join status and strips inherited checkpoint variables case-insensitively", () => {
    expect(() => validateNoopResult(noopResult())).not.toThrow();
    expect(() => validateNoopResult({ ...noopResult(), stdout: "FMRCAP1 mesh-retire-v1 abi=1\n" })).not.toThrow();
    for (const changed of [{ status: 1 }, { status: null }, { signal: "SIGTERM" }, { error: "timeout" }, { error: "" }, { stderr: "unexpected" },
      { stdout: "" }, { stdout: "FMRCAP1 mesh-retire-v1 abi=1" }, { stdout: `${noopResult().stdout}extra\n` }]) {
      expect(() => validateNoopResult({ ...noopResult(), ...changed })).toThrow(/exact output/);
    }
    expect(releaseChildEnv({ PATH: "kept", FABRIC_WIN_MESH_TEST_CHECKPOINT: "before-commit",
      fabric_win_mesh_test_dir: "blocked", FABRIC_OTHER: "kept" })).toEqual({ PATH: "kept", FABRIC_OTHER: "kept" });
  });

  it.skipIf(process.platform === "win32")("driver declares non-Windows an expected skip before fixture/native work", () => {
    // Runs only the driver's non-Windows guard; no native or deleting path is reached.
    const child = spawnSync(process.execPath, [fileURLToPath(new URL("../scripts/benchmark-windows-mesh-release.mjs", import.meta.url))],
      { encoding: "utf8", timeout: 5000, maxBuffer: 4096, env: { ...process.env, GITHUB_ACTIONS: "true" } });
    expect(child.error).toBeUndefined();
    expect(child.signal).toBeNull();
    expect(child.status).toBe(4);
    expect(child.stdout).toContain("SKIP-EXPECTED:");
    expect(child.stdout).not.toContain("WINDOWS_MESH_RELEASE_PHASE:");
    expect(child.stderr).toBe("");
  });
});
