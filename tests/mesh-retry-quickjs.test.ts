import { expect, it } from "vitest";
import {
  isMeshLockTimeout, MESH_INCARNATION_PENDING_CODE, MESH_LOCK_TIMEOUT_CODE,
  MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE, MeshIncarnationPendingError,
  MeshLockTimeoutError, MeshSnapshotRetriesExhaustedError,
} from "../src/core/atomic-write.js";
import { isMeshRetryableBusy, MESH_STATE_WAL_CAP_CODE } from "../src/mesh/state-backend.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";

// Keep: the real host-to-guest trust boundary lost the two non-timeout refusals
// introduced for smarty-dev#12036. The host only throws; no store or deletion runs.
it("preserves only the three mesh retry codes with canonical guest names and narrow timeout classification", async () => {
  const cases = [
    { error: new MeshIncarnationPendingError(), code: MESH_INCARNATION_PENDING_CODE,
      name: "MeshIncarnationPendingError", hostRetryable: true, timeout: false },
    { error: new MeshSnapshotRetriesExhaustedError(3), code: MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE,
      name: "MeshSnapshotRetriesExhaustedError", hostRetryable: true, timeout: false },
    { error: new MeshLockTimeoutError(" host-only holder", 2, 100), code: MESH_LOCK_TIMEOUT_CODE,
      name: "MeshLockTimeoutError", hostRetryable: true, timeout: true },
    ...[
      [MESH_INCARNATION_PENDING_CODE, "MeshIncarnationPendingError"],
      [MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE, "MeshSnapshotRetriesExhaustedError"],
      [MESH_LOCK_TIMEOUT_CODE, "MeshLockTimeoutError"],
    ].map(([code, name]) => ({ error: Object.assign(new Error("legacy narrow mesh refusal"), { code }),
      code, name, hostRetryable: true, timeout: code === MESH_LOCK_TIMEOUT_CODE })),
    { error: Object.assign(new Error("WAL cap"), { code: MESH_STATE_WAL_CAP_CODE }), code: null,
      name: "HostFixtureError", hostRetryable: true, timeout: false },
    { error: Object.assign(new Error("raw SQLite busy"), { code: "FABRIC_MESH_STATE_BUSY", errcode: 5 }), code: null,
      name: "HostFixtureError", hostRetryable: true, timeout: false },
    { error: Object.assign(new Error("unrecognized retry-looking code"), { code: "FABRIC_MESH_RETRYABLE_BUSY" }), code: null,
      name: "HostFixtureError", hostRetryable: false, timeout: false },
    { error: Object.assign(new Error("not an own whitelist entry"), { code: "__proto__" }), code: null,
      name: "HostFixtureError", hostRetryable: false, timeout: false },
  ];
  for (const fixture of cases) {
    Object.assign(fixture.error, { name: "HostFixtureError", retryable: true, attempts: 99,
      holder: "host-only", maxGapMs: 999, peer: "host-only", busyCode: "host-only",
      secret: "host-only", cause: { secret: "host-only" } });
    expect(isMeshRetryableBusy(fixture.error)).toBe(fixture.hostRetryable);
    expect(isMeshLockTimeout(fixture.error)).toBe(fixture.timeout);
  }

  // An unrelated error must reach the existing Fabric metadata path without
  // probing driver metadata merely because it carries a retry-looking code.
  Object.defineProperty(cases[8]!.error, "errcode", { get() { throw new Error("unrelated driver getter must not run"); } });

  const calls: number[] = [];
  const result = await executeAfterAdmission(
    (signal, startClock) => new QuickJsRuntime().execute(`
const retryCodes = ${JSON.stringify([MESH_INCARNATION_PENDING_CODE, MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE, MESH_LOCK_TIMEOUT_CODE])};
const failures = [];
for (let index = 0; index < ${cases.length}; index++) {
  try { await tools.call({ ref: "fixture.throw", args: { index } }); }
  catch (error) {
    failures.push({ name: error.name, code: error.code ?? null, message: error.message,
      isError: error instanceof Error, retryable: retryCodes.includes(error.code),
      timeout: error.code === ${JSON.stringify(MESH_LOCK_TIMEOUT_CODE)},
      extraKeys: ["attempts", "holder", "maxGapMs", "peer", "retryable", "busyCode", "errcode", "secret", "cause"].filter(key => key in error),
      keys: Object.keys(error).sort() });
  }
}
return failures;
`, async (reference, args) => {
      startClock();
      expect(reference).toBe("fabric.$call");
      expect(args.ref).toBe("fixture.throw");
      const index = (args.args as { index: number }).index;
      calls.push(index);
      throw cases[index]!.error;
    }, { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024, signal }),
    () => calls.length > 0,
  );

  expect(result.error).toBeUndefined();
  expect(result.terminationReason).toBe("completed");
  expect(calls).toEqual(cases.map((_, index) => index));
  expect(result.value).toEqual(cases.map(({ error, code, name, timeout }) => ({
    name, code, message: error.message, isError: true, retryable: code !== null, timeout,
    extraKeys: [], keys: code === null ? ["message", "name"] : ["code", "message", "name"],
  })));
});
