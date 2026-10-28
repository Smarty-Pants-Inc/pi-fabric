import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
// @ts-expect-error The standalone Node measurement script deliberately has no build-time TS dependency.
import { prepareSnapshot, seedFixture, runExperiment, failureProbe, instrumentCanonical, verifySentinels } from "../scripts/benchmark-archive-lock-hold.mjs";

// Keep: data safety plus the recorded same-PID head+successor admission incident (COMMS-MEASURE).
// No teardown: source sentinels, real archive copies, partial failure files and receipts remain.
// Explicit candidate validation supplies a pinned bundle through ARCHIVE_LOCK_HOLD_BUNDLE.
// Local runs without it require ARCHIVE_LOCK_HOLD_SOURCE_REF; hosted CI uses committed HEAD.
// Neither fallback reads concurrently edited worktree source.
// Timing observations are reported, never a millisecond performance threshold.
const linux = process.platform === "linux";
const allocationRoot = process.env.ARCHIVE_LOCK_HOLD_TEST_ROOT ?? os.tmpdir();
let root: string, bundle: string;
let fixture: ReturnType<typeof seedFixture>;

beforeAll(async () => {
  if (!linux) return; // /proc descriptor attribution is Linux-only; portable archive checks live in existing suites.
  root = fs.mkdtempSync(path.join(allocationRoot, "archive-lock-hold-test-"));
  bundle = process.env.ARCHIVE_LOCK_HOLD_BUNDLE ?? "";
  if (!bundle) {
    const ref = process.env.ARCHIVE_LOCK_HOLD_SOURCE_REF ?? (process.env.CI === "true" ? "HEAD" : undefined);
    assert.ok(ref, "set ARCHIVE_LOCK_HOLD_BUNDLE to an explicitly prepared/pinned bundle, or ARCHIVE_LOCK_HOLD_SOURCE_REF to an immutable commit");
    const head = execFileSync("git", ["rev-parse", `${ref}^{commit}`], { encoding: "utf8" }).trim();
    const snapshot = await prepareSnapshot({ ref: head, out: path.join(root, "committed-source") });
    bundle = snapshot.bundle;
  }
  fixture = seedFixture({ fixture: path.join(root, "fixture"), entries: 16 });
}, 60000);

describe.skipIf(!linux)("real archive HOLD/WAIT receipts (retained fixtures)", () => {
  it("keeps host source discovery distinct from actor retained-run inventory", () => {
    expect(fixture.sourceRuns).not.toBe(fixture.retainedRuns);
    expect(fs.readdirSync(fixture.sourceRuns)).toHaveLength(16);
    expect(fs.readdirSync(fixture.retainedRuns)).toHaveLength(16);
    expect(verifySentinels(fixture, false)).toMatchObject({ sourceFiles: 3, retainedFiles: 16, markerRetained: true });
  });

  it("measures actual canonical publication/release, not a withTryLock budget callback", async () => {
    const api = await import(pathToFileURL(bundle).href);
    const mesh = path.join(root, "boundary-mesh"); fs.mkdirSync(mesh, { mode: 0o700 });
    const lock = new api.MeshLock(mesh, { lockProtocol: 1, lockTimeoutMs: 5000, lockHeldEventMs: 0 }, () => {});
    const trace = instrumentCanonical(mesh, "boundary-test");
    await lock.withTryLock(async () => { assert.equal(fs.existsSync(path.join(mesh, ".lock")), false); }, 50);
    expect(trace.holds).toHaveLength(0);
    await trace.acquire(lock, "actual-canonical", () => {
      assert.equal(fs.existsSync(path.join(mesh, ".lock", "owner")), true);
    });
    const receipt = trace.finish();
    expect(receipt.holds).toHaveLength(1);
    expect(receipt.holds[0]).toMatchObject({ releaseConfirmed: true, request: "actual-canonical", method: "mkdir", releaseMethod: "rmdir" });
    expect(receipt.holds[0].publicationAfterMs).toBeLessThanOrEqual(receipt.callbacks[0].enteredMs);
    expect(receipt.callbacks[0].leftMs).toBeLessThanOrEqual(receipt.holds[0].releaseAfterMs);
    expect(fs.existsSync(path.join(mesh, ".lock"))).toBe(false);
  });

  it("retains real fsync/copies and cross-process ticket receipts while the archive process is blocked OFF-lock", async () => {
    const result = await runExperiment({ bundle, fixture: fixture.fixture, out: path.join(root, "run"), samples: 1, slowMs: 1200, budgetMs: 5000 });
    const sample = result.results[0], barriers = sample.archive.value.barriers;
    const injected = barriers.filter((b: { injectedStartMs?: number }) => b.injectedStartMs !== undefined);
    expect(injected).toHaveLength(1);
    expect(injected[0]).toMatchObject({ targeted: true, underlyingSucceeded: true });
    expect(sample.archive.pid).not.toBe(sample.contender.pid);
    expect(sample.observation.ownTickets).toHaveLength(2);
    for (const ticket of sample.observation.ownTickets) expect(Number(ticket.split("-")[1])).toBe(sample.archive.pid);
    expect(sample.archive.trace.holds).toHaveLength(2);
    expect(sample.contender.trace.holds).toHaveLength(1);
    expect(sample.analysis.archiveCanonicalOverlapMs).toBe(0);
    expect(sample.analysis.fsyncCalls).toBeGreaterThanOrEqual(6);
    expect(sample.analysis.discovery.value.found).toBe(1);
    expect(sample.analysis.inventory.entries).toBe(16);
    expect(sample.terminals).toEqual([{ code: 0, signal: null }, { code: 0, signal: null }]);
    expect(result.after).toMatchObject({ sourceFiles: 3, retainedFiles: 16, sourceEntries: 16, retainedEntries: 17, markerRetained: true });
    // Admission order is an observation, not a timing gate. Both old and new queue policies
    // must preserve exclusion/data. Compare the raw event ordering in the separate 40k run.
    expect(typeof sample.analysis.contenderAcquiredDuringInjectedStall).toBe("boolean");
  }, 60000);

  it("refuses an archive fsync failure without discharging source custody or changing retained raw sentinels", async () => {
    const result = await failureProbe({ bundle, fixture: fixture.fixture, out: path.join(root, "failure") });
    expect(result.refused.code).toBe("FABRIC_ACTOR_RUN_ARCHIVE_REFUSED");
    expect(result.collectionInvoked).toBe(false);
    expect(result.rawSourcesRetained).toBe(true);
    expect(result.barriers.some((b: { targeted: boolean; underlyingSucceeded: boolean }) => b.targeted && b.underlyingSucceeded)).toBe(true);
    expect(verifySentinels(fixture, false)).toMatchObject({ sourceFiles: 3, retainedFiles: 16, markerRetained: true });
  });
});
