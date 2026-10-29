// Keep reason: data safety and a source/runtime trust boundary. These tests use the real
// journal, but synthetic gateway rows do not qualify live gateway or native recovery capture.
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { openRecoveryJournal, type RecoveryJournal } from "../src/recovery/recovery-journal.js";
import {
  GatewayJoinCapture, GatewayJoinError,
  type GatewayAttemptCorrelation, type GatewayCapacitySource, type GatewayRequestRow,
} from "../src/recovery/gateway-joins.js";

const open: RecoveryJournal[] = [];
const identity: GatewayAttemptCorrelation = {
  sessionId: "session-a", acceptedInputId: "input-a", toolCallId: "call-a", effectKey: "effect-a",
  attemptId: "attempt-a", hostId: "host-a", podId: "pod-a", bootId: "boot-a",
  ownerIncarnation: "owner-a", authorityRevision: 7,
};
const expectedSource = { sourceId: "capacity", scopeId: "cohort-a", captureId: "capture-a" };
const options = () => ({ deadlineMs: Date.now() + 30_000 });
const request = () => ({ ...expectedSource, ...options() });
function row(overrides: Partial<GatewayRequestRow> = {}): GatewayRequestRow {
  return {
    sourceId: "capacity", scopeId: "cohort-a", sourceRecordId: "record-a", gatewayRequestId: "request-a",
    providerAttemptId: "provider-a", stage: "started", requestStartedAt: "source-clock:100",
    timestamps: { started: "source-clock:100", ended: null }, status: "stream-open", billing: null,
    correlation: { kind: "exact", value: { ...identity } }, ...overrides,
  };
}
async function fixture() {
  const directory = await mkdtemp(join(tmpdir(), "gateway-joins-"));
  const journal = await openRecoveryJournal({ directory, storeId: "gateway-test" });
  open.push(journal);
  return { directory, journal, capture: new GatewayJoinCapture(journal) };
}
function inventory(capture: GatewayJoinCapture, attempts: readonly GatewayAttemptCorrelation[] = [identity]) {
  return capture.inventory({ attempts, expectedSources: [expectedSource] });
}
afterEach(async () => {
  await Promise.all(open.splice(0).map(journal => journal.close()));
  // Retain journal fixtures, including incomplete prefixes; no trace truncation/cleanup.
});

describe("append-only gateway joins over the recovery journal", () => {
  it("retains an interrupted prefix, separate retries, source billing and stable sequences on reopen", async () => {
    const { directory, journal, capture } = await fixture();
    const first = row();
    const retry = row({
      sourceRecordId: "record-b", gatewayRequestId: "request-b", providerAttemptId: "provider-b",
      retryOf: { gatewayRequestId: "request-a", providerAttemptId: "provider-a" },
      requestStartedAt: "source-clock:105", stage: "finished", timestamps: { started: 105, ended: 125 },
      status: { code: 200, upstream: "complete" }, billing: { amount: "0.0031", currency: "USD", tokens: 81 },
      metadata: { route: "normal-route", retryGroupId: "source-group-a", timestampUnit: "source-native" },
    });
    const firstReceipt = await capture.append(first, options());
    await capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [first, retry] });
    const before = capture.records();
    expect(inventory(capture).requests.map(item => item.row.gatewayRequestId)).toEqual(["request-a", "request-b"]);
    expect(inventory(capture).complete).toBe(true);
    await journal.close();
    const recovered = await openRecoveryJournal({ directory, storeId: "gateway-test", required: true });
    open.push(recovered);
    const reopened = new GatewayJoinCapture(recovered);
    expect(reopened.records()).toEqual(before);
    expect(await reopened.append(first, options())).toEqual({ ...firstReceipt, existing: true });
    const report = inventory(reopened);
    expect(report.requests[0]?.row.stage).toBe("started"); // No invented end/status/charge after a cut.
    expect(report.requests[0]?.row.billing).toBeNull();
    expect(report.requests[1]?.row).toEqual(retry);
    expect(report.attempts[0]?.sourceRecordIds).toEqual(["record-a", "record-b"]);
    expect(recovered.entries()).toHaveLength(3);
  });

  it("joins model invocations before any tool/effect exists, including retries with no successful response", async () => {
    const { capture } = await fixture();
    const { toolCallId: _call, effectKey: _effect, ...modelAttempt } = identity;
    const retryAttempt = { ...modelAttempt, attemptId: "model-retry", podId: "pod-successor", bootId: "boot-successor" };
    const { providerAttemptId: _provider, ...noResponse } = row({ correlation: { kind: "exact", value: modelAttempt } });
    const retry = row({ sourceRecordId: "record-retry", gatewayRequestId: "request-retry", stage: "interrupted",
      correlation: { kind: "exact", value: retryAttempt }, providerAttemptId: "provider-retry",
      retryOf: { gatewayRequestId: "request-a" }, status: { reason: "connection-cut" }, billing: null });
    await capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [noResponse, retry] });
    const report = inventory(capture, [modelAttempt, retryAttempt]);
    expect(report.complete).toBe(true);
    expect(report.requests.map(item => item.status)).toEqual(["matched", "matched"]);
    expect(report.requests.map(item => item.row.stage)).toEqual(["started", "interrupted"]);
    expect(report.attempts.map(item => item.correlation.toolCallId)).toEqual([undefined, undefined]);
    expect(report.attempts.map(item => item.correlation.effectKey)).toEqual([undefined, undefined]);
  });

  it("adds lifecycle observations instead of overwriting the original start or source values", async () => {
    const { capture } = await fixture();
    const first = row();
    const finished = row({ sourceRecordId: "record-end", stage: "finished", status: 200,
      timestamps: { sourceEnd: "another-clock:20" }, billing: { units: "source-native", value: 0 } });
    await capture.append(first, options());
    await capture.append(finished, options());
    expect(capture.records().filter(record => record.kind === "request").map(record => record.row))
      .toEqual([first, finished]);
    expect(inventory(capture).complete).toBe(false); // No source enumeration receipt yet.
  });

  it("imports identical rows idempotently but rejects changed source-record bytes without overwrite", async () => {
    const { capture, journal } = await fixture();
    const snapshot = { snapshotId: "snapshot-a", complete: true, rows: [row()] };
    const first = await capture.importSnapshot(request(), snapshot);
    const again = await capture.importSnapshot(request(), snapshot);
    expect(again.source).toEqual({ ...first.source, existing: true });
    expect(again.requests[0]).toEqual({ ...first.requests[0], existing: true });
    const bytes = journal.entries();
    await expect(capture.append(row({ billing: { amount: 999 } }), options()))
      .rejects.toMatchObject({ reason: "record-conflict" });
    await expect(capture.importSnapshot(request(), { ...snapshot, snapshotId: "changed" }))
      .rejects.toMatchObject({ reason: "record-conflict" });
    await expect(capture.importSnapshot(request(), { ...snapshot,
      rows: [row(), row({ sourceRecordId: "record-new", gatewayRequestId: "request-new" })],
    })).rejects.toMatchObject({ reason: "record-conflict" });
    expect(journal.entries()).toEqual(bytes);
  });

  it("replays source values with different object key ordering at the same stable sequence", async () => {
    const { capture, journal } = await fixture();
    const original = row({ billing: { amount: "0.01", currency: "USD" } });
    const first = await capture.append(original, options());
    const reordered = { ...Object.fromEntries(Object.entries(original).reverse()),
      billing: { currency: "USD", amount: "0.01" },
    } as unknown as GatewayRequestRow;
    expect(await capture.append(reordered, options())).toEqual({ seq: first.seq, existing: true });
    expect(journal.entries()).toHaveLength(1);
  });

  it("rejects rebinding a physical request to another runtime or original request start", async () => {
    const { capture, journal } = await fixture();
    await capture.append(row(), options());
    await expect(capture.append(row({ sourceRecordId: "record-wrong",
      correlation: { kind: "exact", value: { ...identity, bootId: "other-boot" } } }), options()))
      .rejects.toMatchObject({ reason: "request-conflict" });
    await expect(capture.append(row({ sourceRecordId: "record-restamped", requestStartedAt: 999 }), options()))
      .rejects.toMatchObject({ reason: "request-conflict" });
    expect(journal.entries()).toHaveLength(1);
  });

  it("keeps an actual durable import prefix after a later append fails, without asserting source completeness", async () => {
    const { directory, journal } = await fixture();
    let appends = 0;
    const interruptedJournal: RecoveryJournal = {
      append: (key, bytes, bounds) => ++appends === 2 ? Promise.reject(new Error("capture interrupted")) : journal.append(key, bytes, bounds),
      get: key => journal.get(key), entries: () => journal.entries(), close: () => journal.close(),
    };
    const capture = new GatewayJoinCapture(interruptedJournal);
    await expect(capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [
      row(), row({ sourceRecordId: "record-b", gatewayRequestId: "request-b" }),
    ] })).rejects.toThrow("capture interrupted");
    await journal.close();
    const recovered = await openRecoveryJournal({ directory, storeId: "gateway-test", required: true });
    open.push(recovered);
    const report = inventory(new GatewayJoinCapture(recovered));
    expect(report.requests.map(item => item.row.gatewayRequestId)).toEqual(["request-a"]);
    expect(report.sources[0]?.status).toBe("missing");
    expect(report.unsealedSourceRecordIds).toEqual(["record-a"]);
    expect(report.complete).toBe(false);
  });

  it("fails completeness when a source-declared retry parent invocation is missing", async () => {
    const { capture } = await fixture();
    await capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [row({
      retryOf: { gatewayRequestId: "missing-original", providerAttemptId: "missing-provider" },
    })] });
    const report = inventory(capture);
    expect(report.missingRequests).toEqual([{
      sourceId: "capacity", gatewayRequestId: "missing-original", providerAttemptId: "missing-provider",
    }]);
    expect(report.complete).toBe(false);
  });

  it("retains newly learned provider IDs as another observation, never rewriting a start", async () => {
    const { capture } = await fixture();
    const { providerAttemptId: _provider, ...start } = row();
    await capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [
      start, row({ sourceRecordId: "record-end", stage: "finished" }),
    ] });
    expect(inventory(capture).requests.map(item => item.row.providerAttemptId)).toEqual([undefined, "provider-a"]);
    await expect(capture.append(row({ sourceRecordId: "record-rebind", providerAttemptId: "other-provider" }), options()))
      .rejects.toMatchObject({ reason: "request-conflict" });
  });

  it("keeps source-declared missing and ambiguous joins even if timestamps or one candidate look plausible", async () => {
    const { capture } = await fixture();
    await capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [
      row({ correlation: { kind: "missing", reason: "no runtime context at dispatch" } }),
      row({ sourceRecordId: "record-b", gatewayRequestId: "request-b", correlation: {
        kind: "ambiguous", reason: "two contexts attached", candidates: [identity, { ...identity, attemptId: "other" }],
      } }),
    ] });
    const report = inventory(capture);
    expect(report.requests.map(item => item.status)).toEqual(["missing-correlation", "ambiguous-correlation"]);
    expect(report.attempts[0]?.status).toBe("missing-request");
    expect(report.complete).toBe(false);
  });

  it.each(["sessionId", "acceptedInputId", "toolCallId", "effectKey", "hostId", "podId", "bootId", "ownerIncarnation", "authorityRevision"] as const)
  ("reports wrong %s without accepting a partial identity match", async field => {
    const { capture } = await fixture();
    const wrong = { ...identity, [field]: field === "authorityRevision" ? "7" : "wrong" };
    await capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true,
      rows: [row({ correlation: { kind: "exact", value: wrong } })] });
    const report = inventory(capture);
    expect(report.requests[0]?.status).toBe("identity-mismatch");
    expect(report.requests[0]?.row.correlation).toEqual({ kind: "exact", value: wrong });
    expect(report.attempts[0]?.status).toBe("missing-request");
    expect(report.complete).toBe(false);
  });

  it("accounts unmatched requests, absent expected request IDs, absent attempts and duplicate attempt identities", async () => {
    const { capture } = await fixture();
    await capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [row()] });
    const other = { ...identity, attemptId: "not-in-source" };
    const unmatched = inventory(capture, [other]);
    expect(unmatched.requests[0]?.status).toBe("unmatched-attempt");
    expect(unmatched.attempts[0]?.status).toBe("missing-request");
    const missing = capture.inventory({ attempts: [identity], expectedSources: [expectedSource],
      expectedRequests: [{ sourceId: "capacity", gatewayRequestId: "absent" }] });
    expect(missing.missingRequests).toEqual([{ sourceId: "capacity", gatewayRequestId: "absent" }]);
    expect(missing.complete).toBe(false);
    const ambiguous = inventory(capture, [identity, { ...identity }]);
    expect(ambiguous.requests[0]?.status).toBe("ambiguous-attempt");
    expect(ambiguous.attempts.map(item => item.status)).toEqual(["ambiguous-attempt", "ambiguous-attempt"]);
    expect(ambiguous.complete).toBe(false);
  });

  it("makes absent, unavailable, empty and incomplete sources explicit, retaining historical absence", async () => {
    const { capture } = await fixture();
    expect(inventory(capture).sources[0]?.status).toBe("missing");
    await capture.captureFrom(undefined, request());
    expect(inventory(capture).sources[0]?.status).toBe("missing");
    const broken: GatewayCapacitySource = {
      sourceId: "capacity", readGatewayRequests: async () => { throw new Error("source offline"); },
    };
    await capture.captureFrom(broken, { ...request(), captureId: "capture-offline" });
    await capture.importSnapshot({ ...request(), captureId: "capture-partial" }, {
      snapshotId: "snapshot-partial", complete: false, reason: "source has an inaccessible retained range", rows: [],
    });
    await capture.importSnapshot({ ...request(), captureId: "capture-empty" }, {
      snapshotId: "snapshot-empty", complete: true, rows: [],
    });
    const report = capture.inventory({ attempts: [identity], expectedSources: [
      expectedSource, { ...expectedSource, captureId: "capture-offline" },
      { ...expectedSource, captureId: "capture-partial" }, { ...expectedSource, captureId: "capture-empty" },
    ] });
    expect(report.sources.map(source => source.status)).toEqual(["missing", "unavailable", "incomplete", "complete"]);
    expect(report.sourceHistory).toHaveLength(4);
    expect(report.attempts[0]?.status).toBe("missing-request");
    expect(report.complete).toBe(false);
  });

  it("passes original source deadlines/scope and captures all rows without matching/filtering by attempt", async () => {
    const { capture } = await fixture();
    const signal = new AbortController().signal;
    const input = { ...request(), signal };
    const seen: unknown[] = [];
    const source: GatewayCapacitySource = { sourceId: "capacity", readGatewayRequests: async query => {
      seen.push(query);
      return { snapshotId: "snapshot-a", complete: true, rows: [row(), row({
        sourceRecordId: "record-unknown", gatewayRequestId: "request-unknown",
        correlation: { kind: "exact", value: { ...identity, attemptId: "unknown" } },
      })] };
    } };
    await capture.captureFrom(source, input);
    expect(seen).toEqual([{ scopeId: input.scopeId, deadlineMs: input.deadlineMs, signal }]);
    const report = inventory(capture);
    expect(report.requests.map(item => item.status)).toEqual(["matched", "unmatched-attempt"]);
    expect(report.complete).toBe(false);
    await expect(capture.captureFrom({ ...source, sourceId: "wrong-source" }, request()))
      .rejects.toMatchObject({ reason: "source-conflict" });
  });

  it("detaches mutable inputs before queuing and returns fresh copies after reads", async () => {
    const { capture } = await fixture();
    const mutable = row({ billing: { amount: "0.01" } });
    const pending = capture.append(mutable, options());
    (mutable.billing as Record<string, string>).amount = "999";
    await pending;
    const returned = capture.records();
    const stored = returned.find(record => record.kind === "request");
    if (stored?.kind !== "request") throw new Error("expected retained request");
    (stored.row.billing as Record<string, string>).amount = "888";
    const reread = capture.records().find(record => record.kind === "request");
    expect(reread?.kind === "request" && reread.row.billing).toEqual({ amount: "0.01" });
  });

  it("serializes conflicting concurrent wrappers and reports unavailable storage rather than authoritative absence", async () => {
    const { capture, journal } = await fixture();
    const second = new GatewayJoinCapture(journal);
    const results = await Promise.allSettled([
      capture.append(row(), options()),
      second.append(row({ sourceRecordId: "record-rebound", requestStartedAt: 500 }), options()),
    ]);
    expect(results.map(result => result.status)).toEqual(["fulfilled", "rejected"]);
    expect(results[1]?.status === "rejected" && results[1].reason).toBeInstanceOf(GatewayJoinError);
    await journal.close();
    expect(() => inventory(capture)).toThrow();
    expect(() => capture.records()).toThrow();
    await expect(capture.append(row(), options())).rejects.toBeDefined();
  });

  it("refuses source/scope and duplicate event imports before writing any rows", async () => {
    const { capture, journal } = await fixture();
    for (const outside of [row({ sourceId: "other-source" }), row({ scopeId: "other-scope" })]) {
      await expect(capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [row(), outside] }))
        .rejects.toMatchObject({ reason: "source-conflict" });
    }
    await expect(capture.importSnapshot(request(), { snapshotId: "snapshot-a", complete: true, rows: [row(), row()] }))
      .rejects.toMatchObject({ reason: "invalid-record" });
    expect(journal.entries()).toHaveLength(0);
  });

  it("does not swallow malformed retained gateway data or non-JSON source values", async () => {
    const { capture, journal } = await fixture();
    await expect(capture.append(row({ billing: Number.NaN }), options())).rejects.toMatchObject({ reason: "invalid-record" });
    await expect(capture.append(row({ status: undefined } as unknown as GatewayRequestRow), options()))
      .rejects.toMatchObject({ reason: "invalid-record" });
    await journal.append("recovery:gateway-join:v1:malformed", new TextEncoder().encode("{bad-json"), options());
    expect(() => inventory(capture)).toThrow(GatewayJoinError);
  });
});
