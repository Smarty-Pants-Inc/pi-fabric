// Keep reason: data safety at the intent/attempt/result durability boundary. These use the
// real fsynced journal, not installed native, replicated storage, Kubernetes or gateway APIs.
// All temporary raw evidence is retained; no cleanup/truncation/compaction runs here.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  createRecoveryAttemptLedger, RECOVERY_ATTEMPT_NAMESPACE,
  type RecoveryAttemptRequest, type RecoveryAttemptEventRequest, type RecoveryAttemptTraceReference,
} from "../src/recovery/attempt-ledger.js";
import { openRecoveryJournal, type RecoveryJournal } from "../src/recovery/recovery-journal.js";

const sha = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const raw = (bytes: Uint8Array | string) => {
  const copy = Uint8Array.from(typeof bytes === "string" ? Buffer.from(bytes) : bytes);
  return { bytes: copy, sha256: sha(copy) };
};
const trace = (bytes: Uint8Array | string = '{"complete":true}\n{"interrupted":'): RecoveryAttemptTraceReference => {
  const value = raw(bytes);
  return { traceId: "raw-trace", storage: { storeId: "trace-store", objectKey: "attempt/raw", writerHostId: "origin-host", receipt: "fixture-trace-commit" }, retainedFrom: 0, byteLength: value.bytes.length, ...value, interrupted: true };
};
const request = (attemptId = "try-1", attemptNumber = 1): RecoveryAttemptRequest => ({
  identity: { sessionId: "session", acceptedInputId: "input", toolCallId: "call", effectKey: "logical-effect", attemptId, attemptNumber,
    hostId: "origin-host", podId: "pod", bootId: "boot", ownerIncarnation: "owner-1", authorityRevision: "revision-1" },
  toolVersion: "tool-v1", intent: raw(new Uint8Array([0, 255, 1, 10])), payload: raw('{ "value": 1 }\n'),
  destination: { kind: "fixture-destination", target: "logical-target" }, observedAt: 1, traces: [trace()],
});
const event = (attempt = request(), eventId = "committed"): RecoveryAttemptEventRequest => ({ identity: attempt.identity, eventId, observedAt: 2,
  state: "committed", result: raw(new Uint8Array([255, 0, 10, 13])), receipt: raw(' {"saved":"original"}\n'), traces: attempt.traces });
const options = () => ({ deadlineMs: Date.now() + 30_000 });
async function fixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-attempt-"));
  const journal = await openRecoveryJournal({ directory, storeId: "attempt-store" });
  return { directory, journal, ledger: createRecoveryAttemptLedger(journal) };
}

// No source mock implements append durability; every successful append goes through real files.
describe("recovery attempt ledger", () => {
  it("retains identical logical effects as distinct physical attempts and exact binary evidence after reopen", async () => {
    const f = await fixture();
    let reopened: RecoveryJournal | undefined;
    try {
      const first = request(), second = request("try-2", 2);
      const a = await f.ledger.recordAttempt(first, options()), b = await f.ledger.recordAttempt(second, options());
      expect(a.seq).not.toBe(b.seq);
      const completed = await f.ledger.appendEvent(event(second), options());
      expect(completed.seq).toBeGreaterThan(b.seq);
      await f.journal.close();
      reopened = await openRecoveryJournal({ directory: f.directory, storeId: "attempt-store", required: true });
      const ledger = createRecoveryAttemptLedger(reopened);
      expect(ledger.entries()).toHaveLength(3);
      expect(ledger.readAttempt("session", "try-1")!.attempt.intent).toEqual(first.intent);
      expect(ledger.readAttempt("session", "try-2")!.events[0]!.result).toEqual(event(second).result);
      expect(ledger.readAttempt("session", "try-1")!.attempt.traces[0]!.bytes).toEqual(Uint8Array.from(first.traces[0]!.bytes));
      expect(await ledger.recordAttempt(first, options())).toEqual({ ...a, existing: true });
      expect(ledger.entries()).toHaveLength(3);
    } finally { await f.journal.close(); await reopened?.close(); }
  });

  it("copies every request byte/identity/destination/trace and deadline before queue waits", async () => {
    const f = await fixture();
    try {
      const input = request(), bounds = options(), expected = request();
      Object.assign(input, { payload: { bytes: Buffer.from(input.payload.bytes), sha256: input.payload.sha256 } });
      const queued = f.ledger.recordAttempt(input, bounds);
      input.intent.bytes.fill(4); input.payload.bytes.fill(5); input.traces[0]!.bytes.fill(6);
      Object.assign(input.identity, { hostId: "changed-host" }); Object.assign(input.destination, { target: "changed-target" });
      Object.assign(input.traces[0]!.storage, { receipt: "changed-receipt" }); bounds.deadlineMs = 0;
      await queued;
      expect(f.ledger.readAttempt("session", "try-1")!.attempt).toMatchObject(expected);
      const completed = event(expected), pending = f.ledger.appendEvent(completed, options());
      completed.result!.bytes.fill(7); completed.receipt!.bytes.fill(8); completed.traces[0]!.bytes.fill(9);
      await pending;
      expect(f.ledger.readAttempt("session", "try-1")!.events[0]!.result).toEqual(event(request()).result);
      const read = f.ledger.readAttempt("session", "try-1")!;
      read.attempt.payload.bytes.fill(0); read.events[0]!.result!.bytes.fill(0); read.attempt.traces[0]!.bytes.fill(0);
      expect(f.ledger.readAttempt("session", "try-1")!.attempt.payload).toEqual(request().payload);
      expect(f.ledger.readAttempt("session", "try-1")!.events[0]!.result).toEqual(event().result);
    } finally { await f.journal.close(); }
  });

  it("appends state/result observations without replacing earlier uncertain or dispatched bytes", async () => {
    const f = await fixture();
    try {
      await f.ledger.recordAttempt(request(), options());
      await f.ledger.appendEvent({ ...event(), eventId: "dispatch", state: "dispatched" }, options());
      const before = f.journal.entries().map(row => ({ ...row, bytes: Uint8Array.from(row.bytes) }));
      await f.ledger.appendEvent({ ...event(), eventId: "uncertain", state: "uncertain", result: raw("unknown") }, options());
      await f.ledger.appendEvent(event(), options());
      expect(f.ledger.readAttempt("session", "try-1")!.events.map(row => row.state)).toEqual(["dispatched", "uncertain", "committed"]);
      expect(f.journal.entries().slice(0, 2)).toEqual(before);
      expect(f.ledger.readAttempt("session", "try-1")!.events[1]!.result).toEqual(raw("unknown"));
    } finally { await f.journal.close(); }
  });

  it("fsyncs completion evidence before a caller can ACK and reconciles exactly the same event", async () => {
    const f = await fixture();
    try {
      await f.ledger.recordAttempt(request(), options());
      const flush = vi.spyOn(fs, "fsyncSync");
      let result: { seq: number; existing: boolean };
      try {
        result = await f.ledger.appendEvent(event(), options()).then(receipt => {
          expect(flush.mock.calls.length).toBeGreaterThan(0);
          expect(f.journal.entries()).toHaveLength(2);
          return receipt; // Only now may an owner issue its completion ACK.
        });
      } finally { flush.mockRestore(); }
      expect(await f.ledger.appendEvent(event(), options())).toEqual({ seq: result!.seq, existing: true });
      await expect(f.ledger.appendEvent({ ...event(), result: raw("changed-result") }, options())).rejects.toMatchObject({ reason: "conflict" });
      expect(f.journal.entries()).toHaveLength(2);
    } finally { await f.journal.close(); }
  });

  it.each(["payload", "intent", "destination", "toolVersion", "acceptedInputId"])("rejects changed same-key %s without overwriting the original", async field => {
    const f = await fixture();
    try {
      await f.ledger.recordAttempt(request(), options());
      const second = request("try-2", 2);
      if (field === "payload" || field === "intent") Object.assign(second, { [field]: raw("changed") });
      if (field === "destination") Object.assign(second, { destination: { kind: "fixture-destination", target: "another-target" } });
      if (field === "toolVersion") Object.assign(second, { toolVersion: "v2" });
      if (field === "acceptedInputId") Object.assign(second.identity, { acceptedInputId: "another-input" });
      await expect(f.ledger.recordAttempt(second, options())).rejects.toMatchObject({ reason: "conflict" });
      expect(f.ledger.entries()).toHaveLength(1);
      expect(f.ledger.readAttempt("session", "try-1")!.attempt.payload).toEqual(request().payload);
    } finally { await f.journal.close(); }
  });

  it("serializes relationship validation across separate facades sharing a journal", async () => {
    const f = await fixture();
    try {
      const other = createRecoveryAttemptLedger(f.journal), changed = { ...request("try-2", 2), payload: raw("different") };
      const results = await Promise.allSettled([f.ledger.recordAttempt(request(), options()), other.recordAttempt(changed, options())]);
      expect(results.map(row => row.status)).toEqual(["fulfilled", "rejected"]);
      expect(results[1]).toMatchObject({ reason: { reason: "conflict" } });
      expect(f.journal.entries()).toHaveLength(1);
      // Rejection does not poison the queue; a genuine identical-payload retry is retained.
      await Promise.all([other.recordAttempt(request("try-2", 2), options()), f.ledger.recordAttempt(request("try-3", 3), options())]);
      expect(f.ledger.entries().map(row => row.identity.attemptId)).toEqual(["try-1", "try-2", "try-3"]);
    } finally { await f.journal.close(); }
  });

  it("does not permit reused physical ordinals, changed physical identity or orphan events", async () => {
    const f = await fixture();
    try {
      await expect(f.ledger.appendEvent(event(), options())).rejects.toMatchObject({ reason: "not-found" });
      await f.ledger.recordAttempt(request(), options());
      await expect(f.ledger.recordAttempt(request("another-id", 1), options())).rejects.toMatchObject({ reason: "conflict" });
      await expect(f.ledger.appendEvent({ ...event(), identity: { ...request().identity, bootId: "different-boot" } }, options())).rejects.toMatchObject({ reason: "conflict" });
      await expect(f.ledger.recordAttempt({ ...request(), observedAt: 9 }, options())).rejects.toMatchObject({ reason: "conflict" });
      expect(f.journal.entries()).toHaveLength(1);
    } finally { await f.journal.close(); }
  });

  it("retains every raw prefix and rejects shortening or changing an earlier byte", async () => {
    const f = await fixture();
    try {
      await f.ledger.recordAttempt(request(), options());
      const original = request().traces[0]!.bytes, longer = Buffer.concat([original, Buffer.from("true")]);
      await f.ledger.appendEvent({ ...event(), traces: [trace(longer)] }, options());
      expect(f.ledger.entries().map(row => Buffer.from(row.traces[0]!.bytes))).toEqual([Buffer.from(original), longer]);
      await expect(f.ledger.appendEvent({ ...event(), eventId: "shortened", traces: [trace("short")] }, options())).rejects.toMatchObject({ reason: "conflict" });
      const changed = Buffer.concat([Buffer.from("X"), longer.subarray(1), Buffer.from("more")]);
      await expect(f.ledger.appendEvent({ ...event(), eventId: "rewritten", traces: [trace(changed)] }, options())).rejects.toMatchObject({ reason: "conflict" });
    } finally { await f.journal.close(); }
  });

  it("refuses digest mismatches, missing real provenance and committed events without raw/result evidence", async () => {
    const f = await fixture();
    try {
      expect(() => f.ledger.recordAttempt({ ...request(), payload: { bytes: Buffer.from("changed"), sha256: request().payload.sha256 } }, options())).toThrow("digest mismatch");
      expect(() => f.ledger.recordAttempt({ ...request(), traces: [{ ...trace(), retainedFrom: 1 as 0 }] }, options())).toThrow("Full raw trace prefix");
      expect(() => f.ledger.recordAttempt({ ...request(), traces: [{ ...trace(), storage: { ...trace().storage, receipt: "" } }] }, options())).toThrow("storage receipt");
      expect(() => f.ledger.recordAttempt({ ...request(), traces: [] }, options())).toThrow("raw trace storage reference");
      const { result: _result, receipt: _receipt, ...withoutResult } = event();
      expect(() => f.ledger.appendEvent(withoutResult, options())).toThrow("result or receipt");
      expect(() => f.ledger.appendEvent({ ...event(), traces: [] }, options())).toThrow("retained raw trace");
      expect(f.journal.entries()).toHaveLength(0);
    } finally { await f.journal.close(); }
  });

  it("preserves an actual interrupted journal tail and raw killed trace while recovering in a new segment", async () => {
    const f = await fixture();
    let reopened: RecoveryJournal | undefined;
    const tailPath = path.join(f.directory, "raw-0000000000000002-00000000-0000-0000-0000-000000000000.jsonl");
    const interrupted = Buffer.from('{"version":1,"interrupted":');
    try {
      const first = await f.ledger.recordAttempt(request(), options());
      await f.journal.close();
      fs.writeFileSync(tailPath, interrupted, { flag: "wx" });
      reopened = await openRecoveryJournal({ directory: f.directory, storeId: "attempt-store", required: true });
      const ledger = createRecoveryAttemptLedger(reopened);
      expect(await ledger.recordAttempt(request(), options())).toEqual({ ...first, existing: true });
      await ledger.recordAttempt(request("try-2", 2), options());
      expect(fs.readFileSync(tailPath)).toEqual(interrupted);
      expect(fs.readdirSync(f.directory).filter(name => name.startsWith("raw-0000000000000002-"))).toHaveLength(2);
      expect(ledger.readAttempt("session", "try-1")!.attempt.traces[0]!.bytes).toEqual(Uint8Array.from(request().traces[0]!.bytes));
    } finally { await f.journal.close(); await reopened?.close(); }
  });

  it("retains corrupt attempt-domain bytes but refuses an authoritative parsed ledger view", async () => {
    const f = await fixture();
    try {
      await f.journal.append(RECOVERY_ATTEMPT_NAMESPACE + "future", Buffer.from('{"version":99}'), options());
      expect(() => f.ledger.entries()).toThrow("Invalid retained attempt record");
      expect(f.journal.entries()).toHaveLength(1);
    } finally { await f.journal.close(); }
  });

  it("never turns expired, aborted or closed-store failures into absence or a completion ACK", async () => {
    const f = await fixture();
    try {
      await expect(f.ledger.recordAttempt(request(), { deadlineMs: 0 })).rejects.toMatchObject({ reason: "deadline" });
      const controller = new AbortController(); controller.abort();
      await expect(f.ledger.recordAttempt(request(), { ...options(), signal: controller.signal })).rejects.toMatchObject({ reason: "aborted" });
      expect(f.ledger.entries()).toHaveLength(0);
      await f.journal.close();
      expect(() => f.ledger.readAttempt("session", "absent")).toThrow("closed");
    } finally { await f.journal.close(); }
  });
});
