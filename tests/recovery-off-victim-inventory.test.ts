// Keep reason: data safety and the off-victim evidence trust boundary. Real filesystem
// journal recovery + externally copied raw traces are used. Host/source receipts are FIXTURE
// provenance, not installed distributed topology, native, Kubernetes or gateway integration.
// Retain every fixture, including interrupted raw bytes; never remove or truncate traces.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  createRecoveryAttemptLedger, RECOVERY_ATTEMPT_NAMESPACE,
  type RecoveryAttemptRequest, type RecoveryAttemptTraceReference,
} from "../src/recovery/attempt-ledger.js";
import {
  buildRecoveryOffVictimInventory, recoveryInventoryCohortSha256,
  type RecoveryInventoryCohort, type RecoveryInventorySourceManifest, type RecoveryOffVictimSource,
  type RecoveryOffVictimProvenance, type RecoveryOffVictimReadPolicy,
} from "../src/recovery/off-victim-inventory.js";
import { openRecoveryJournal, type RecoveryJournalRecord } from "../src/recovery/recovery-journal.js";

const sha = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const evidence = (value: Uint8Array | string) => {
  const bytes = typeof value === "string" ? Buffer.from(value) : Uint8Array.from(value);
  return { bytes, sha256: sha(bytes) };
};
const bounds = () => ({ deadlineMs: Date.now() + 30_000 });
const provenance = (storeId = "evidence-store"): RecoveryOffVictimProvenance => ({ sourceHostId: "survivor-host", sourceId: "external-source", storeId,
  receipt: "fixture-source-read-receipt", dependencyHostIds: ["survivor-host"] });
const cohort = (): RecoveryInventoryCohort => ({ cohortId: "frozen-cohort", frozenAt: 10, sessions: [{ sessionId: "session-a", attempts: [
  { attemptId: "try-1", eventIds: ["completed"], traceIds: ["raw-trace"] },
  { attemptId: "try-2", eventIds: ["completed"], traceIds: ["raw-trace"] },
] }] });
const storeFile = (file: string, bytes: Uint8Array): void => {
  const fd = fs.openSync(file, "wx");
  try { fs.writeFileSync(fd, bytes); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
};
const sealedManifest = (expected: RecoveryInventoryCohort, records: readonly RecoveryJournalRecord[]): RecoveryInventorySourceManifest => ({
  cohortSha256: recoveryInventoryCohortSha256(expected), sealed: true, sealReceipt: "fixture-closed-enumeration-receipt",
  entries: records.map(row => ({ seq: row.seq, key: row.key, sha256: sha(row.bytes) })),
  // Fixture source-object enumeration: independent of the inventory's record decoder.
  traces: ["try-1", "try-2"].filter(attemptId => records.some(row => row.key.includes(attemptId))).map(attemptId => {
    const full = Buffer.concat([Buffer.from('{"line":1}\n{"interrupted":'), Buffer.from([255, 0]), Buffer.from("raw-suffix\n")]);
    return { sessionId: "session-a", attemptId, traceId: "raw-trace", storeId: "trace-store", objectKey: `${attemptId}.bin`, byteLength: full.length, sha256: sha(full) };
  }),
});

async function fixture(extraUnknown = false) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "recovery-off-victim-"));
  const original = path.join(root, "origin"), external = path.join(root, "external");
  fs.mkdirSync(original); fs.mkdirSync(path.join(original, "traces"));
  const journal = await openRecoveryJournal({ directory: path.join(original, "journal"), storeId: "evidence-store" });
  const ledger = createRecoveryAttemptLedger(journal), expected = cohort();
  const prefixes = new Map<string, Buffer>(), completed = new Map<string, Buffer>();
  let records: readonly RecoveryJournalRecord[];
  try {
    for (const [index, attemptId] of ["try-1", "try-2"].entries()) {
      const prefix = Buffer.concat([Buffer.from('{"line":1}\n{"interrupted":'), Buffer.from([255, 0])]);
      const final = Buffer.concat([prefix, Buffer.from("raw-suffix\n")]);
      prefixes.set(attemptId, prefix); completed.set(attemptId, final);
      storeFile(path.join(original, "traces", `${attemptId}.bin`), final);
      const ref = (bytes: Uint8Array): RecoveryAttemptTraceReference => ({ traceId: "raw-trace", storage: { storeId: "trace-store", objectKey: `${attemptId}.bin`,
        writerHostId: "victim-host", receipt: "fixture-trace-store-commit" }, retainedFrom: 0, byteLength: bytes.length, ...evidence(bytes), interrupted: true });
      const request: RecoveryAttemptRequest = {
        identity: { sessionId: "session-a", acceptedInputId: "input-a", toolCallId: "call-a", effectKey: "same-logical-effect", attemptId, attemptNumber: index + 1,
          hostId: "victim-host", podId: "pod-a", bootId: "boot-a", ownerIncarnation: "owner-a", authorityRevision: "revision-a" },
        toolVersion: "v1", intent: evidence("saved-intent"), payload: evidence("identical-payload"), destination: { kind: "fixture-sink", target: "target-a" },
        observedAt: 1, traces: [ref(prefix)],
      };
      await ledger.recordAttempt(request, bounds());
      await ledger.appendEvent({ identity: request.identity, eventId: "completed", observedAt: 2, state: "committed",
        result: evidence("original-result"), receipt: evidence("destination-receipt"), traces: [ref(final)] }, bounds());
    }
    await journal.append("carrier:foreign-domain", Buffer.from("retained other-domain evidence"), bounds());
    if (extraUnknown) await journal.append(RECOVERY_ATTEMPT_NAMESPACE + "future", Buffer.from('{"version":99,"unknown":"retained"}'), bounds());
    records = journal.entries();
  } finally { await journal.close(); }
  // A retained external copy, read through the actual REQUIRED journal reader. This is a
  // filesystem boundary test, not a claim that cp supplies live replica or fault isolation.
  fs.cpSync(original, external, { recursive: true });
  storeFile(path.join(external, "source-manifest.json"), Buffer.from(JSON.stringify(sealedManifest(expected, records!))));
  fs.renameSync(original, path.join(root, "origin-inaccessible")); // preserve bytes; no victim path available
  const calls: RecoveryOffVictimReadPolicy[] = [];
  const assertPolicy = (policy: RecoveryOffVictimReadPolicy) => {
    calls.push(policy);
    expect(policy).toEqual({ observerHostId: "observer-host", victimHostId: "victim-host", allowVictimFallback: false });
    expect(Object.isFrozen(policy)).toBe(true);
    expect(fs.existsSync(original)).toBe(false);
  };
  const source: RecoveryOffVictimSource = {
    sourceHostId: "survivor-host", sourceId: "external-source",
    readManifest: async policy => {
      assertPolicy(policy);
      return { status: "present", value: JSON.parse(fs.readFileSync(path.join(external, "source-manifest.json"), "utf8")), provenance: provenance() };
    },
    readJournal: async policy => {
      assertPolicy(policy);
      const recovered = await openRecoveryJournal({ directory: path.join(external, "journal"), storeId: "evidence-store", required: true });
      try { return { status: "present", value: { complete: true, records: recovered.entries() }, provenance: provenance() }; }
      finally { await recovered.close(); }
    },
    readTrace: async (reference, policy) => {
      assertPolicy(policy);
      const bytes = fs.readFileSync(path.join(external, "traces", reference.storage.objectKey));
      return { status: "present", value: bytes.subarray(0, reference.byteLength), provenance: provenance("trace-store") };
    },
  };
  const inventory = (reader: RecoveryOffVictimSource | undefined = source, denominator = expected) => buildRecoveryOffVictimInventory({ observerHostId: "observer-host", victimHostId: "victim-host", cohort: denominator, ...(reader ? { source: reader } : {}) });
  return { source, expected, external, records: records!, calls, prefixes, completed, inventory };
}

// B6-A1's exact real-journal shape: attempt 1, foreign namespace 2, committed event 3.
async function sequenceFixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "b6-inventory-sequences-"));
  const raw = Buffer.from("immutable raw attempt trace\n");
  storeFile(path.join(root, "raw-trace.bin"), raw);
  const trace: RecoveryAttemptTraceReference = { traceId: "trace-1", storage: { storeId: "trace-store", objectKey: "trace-1.bin",
    writerHostId: "victim-host", receipt: "fixture-commit" }, retainedFrom: 0, byteLength: raw.length, ...evidence(raw), interrupted: false };
  const expected: RecoveryInventoryCohort = { cohortId: "frozen", frozenAt: 1, sessions: [{ sessionId: "s", attempts: [
    { attemptId: "a", eventIds: ["done"], traceIds: ["trace-1"] },
  ] }] };
  const journal = await openRecoveryJournal({ directory: path.join(root, "journal"), storeId: "evidence-store" });
  let records: readonly RecoveryJournalRecord[];
  try {
    const ledger = createRecoveryAttemptLedger(journal);
    const request: RecoveryAttemptRequest = {
      identity: { sessionId: "s", acceptedInputId: "input", toolCallId: "call", effectKey: "effect", attemptId: "a", attemptNumber: 1,
        hostId: "victim-host", podId: "pod", bootId: "boot", ownerIncarnation: "owner", authorityRevision: "rev" },
      toolVersion: "tool-v1", intent: evidence("intent"), payload: evidence("payload"), destination: { kind: "file", target: "target" }, observedAt: 1, traces: [trace],
    };
    await ledger.recordAttempt(request, bounds());
    await journal.append("foreign:interior-record", Buffer.from("retained non-attempt namespace"), bounds());
    await ledger.appendEvent({ identity: request.identity, eventId: "done", observedAt: 2, state: "committed",
      result: evidence("result"), receipt: evidence("sink receipt"), traces: [trace] }, bounds());
    records = journal.entries();
  } finally { await journal.close(); }
  const inventory = async (manifestRecords = records, snapshotRecords = records) => {
    const manifest: RecoveryInventorySourceManifest = {
      cohortSha256: recoveryInventoryCohortSha256(expected), sealed: true, sealReceipt: "fixture-closed-enumeration",
      entries: manifestRecords.map(row => ({ seq: row.seq, key: row.key, sha256: sha(row.bytes) })),
      traces: [{ sessionId: "s", attemptId: "a", traceId: "trace-1", storeId: "trace-store", objectKey: "trace-1.bin", byteLength: raw.length, sha256: sha(raw) }],
    };
    storeFile(path.join(root, "source-manifest.json"), Buffer.from(JSON.stringify(manifest)));
    storeFile(path.join(root, "source-snapshot.json"), Buffer.from(JSON.stringify(snapshotRecords)));
    const source: RecoveryOffVictimSource = {
      sourceHostId: "survivor-host", sourceId: "external-source",
      readManifest: async () => ({ status: "present", value: manifest, provenance: provenance() }),
      readJournal: async () => ({ status: "present", value: { complete: true, records: snapshotRecords }, provenance: provenance() }),
      readTrace: async () => ({ status: "present", value: fs.readFileSync(path.join(root, "raw-trace.bin")), provenance: provenance("trace-store") }),
    };
    const result = await buildRecoveryOffVictimInventory({ observerHostId: "observer-host", victimHostId: "victim-host", cohort: expected, source });
    storeFile(path.join(root, "inventory-result.json"), Buffer.from(JSON.stringify(result)));
    console.info(JSON.stringify({ fixture: root, manifestSequences: manifestRecords.map(row => row.seq), journalSequences: snapshotRecords.map(row => row.seq), complete: result.complete, issues: result.issues }));
    return result;
  };
  return { expected, records: records!, raw, inventory };
}

describe("off-victim recovery inventory", () => {
  it("accepts and retains the audit's contiguous [1,2,3] journal including its foreign interior row", async () => {
    const f = await sequenceFixture(), result = await f.inventory();
    expect(f.records.map(row => row.seq)).toEqual([1, 2, 3]);
    expect(result.complete).toBe(true); expect(result.issues).toEqual([]);
    expect(result.manifest!.entries.map(row => row.seq)).toEqual([1, 2, 3]);
    expect(result.journalRecords).toEqual(f.records);
    expect(result.journalRecords[1]).toMatchObject({ key: "foreign:interior-record", bytes: Uint8Array.from(Buffer.from("retained non-attempt namespace")) });
    expect(result.unknownRecords).toEqual([]);
    expect(result.attempts[0]!.availability).toBe("present"); expect(result.events[0]!.availability).toBe("present");
    expect(result.traces.every(row => row.availability === "present" && Buffer.from(row.bytes!).equals(f.raw))).toBe(true);
  });

  it.each(["manifest", "journal", "both"] as const)("rejects the audit's [1,3] omission from %s without discarding evidence or the denominator", async target => {
    const f = await sequenceFixture(), omitted = f.records.filter(row => row.seq !== 2);
    const manifestRecords = target === "journal" ? f.records : omitted, snapshotRecords = target === "manifest" ? f.records : omitted;
    const result = await f.inventory(manifestRecords, snapshotRecords);
    expect(omitted.map(row => row.seq)).toEqual([1, 3]);
    expect(result.cohort).toEqual(f.expected); expect(result.journalRecords).toEqual(snapshotRecords);
    expect(result.manifest!.entries.map(row => row.seq)).toEqual(manifestRecords.map(row => row.seq));
    expect(result.attempts).toHaveLength(1); expect(result.events).toHaveLength(1); expect(result.traces).toHaveLength(2);
    expect(result.attempts[0]!.availability).toBe("present"); expect(result.events[0]!.availability).toBe("present");
    expect(result.traces.every(row => row.availability === "present" && Buffer.from(row.bytes!).equals(f.raw))).toBe(true);
    expect(result.complete).toBe(false);
    expect(result.issues.some(issue => issue.startsWith("Noncontiguous source manifest sequence:"))).toBe(target !== "journal");
    expect(result.issues.some(issue => issue.startsWith("Noncontiguous journal sequence:"))).toBe(target !== "manifest");
  });

  it.each((["manifest", "journal", "both"] as const).flatMap(target => [0, 2].map(start => ({ target, start }))))(
    "rejects a non-one start of $start in $target independently of namespace decoding", async ({ target, start }) => {
      const f = await sequenceFixture(), shifted = f.records.map(row => ({ ...row, seq: row.seq + start - 1 }));
      const manifestRecords = target === "journal" ? f.records : shifted, snapshotRecords = target === "manifest" ? f.records : shifted;
      const result = await f.inventory(manifestRecords, snapshotRecords);
      expect(result.cohort).toEqual(f.expected); expect(result.journalRecords).toEqual(snapshotRecords);
      expect(result.manifest!.entries.map(row => row.seq)).toEqual(manifestRecords.map(row => row.seq));
      expect(result.attempts).toHaveLength(1); expect(result.events).toHaveLength(1); expect(result.traces).toHaveLength(2);
      expect(result.traces.every(row => row.availability === "present" && Buffer.from(row.bytes!).equals(f.raw))).toBe(true);
      expect(result.complete).toBe(false);
      expect(result.issues.some(issue => issue.startsWith("Noncontiguous source manifest sequence:"))).toBe(target !== "journal");
      expect(result.issues.some(issue => issue.startsWith("Noncontiguous journal sequence:"))).toBe(target !== "manifest");
    },
  );
  it("recovers every frozen retry/event/full interrupted prefix through external filesystem reads with the victim path inaccessible", async () => {
    const f = await fixture(), result = await f.inventory();
    expect(result.complete).toBe(true);
    expect(result.topologyQualification).toBe("not-established-by-inventory");
    expect(result.sessions).toEqual([{ sessionId: "session-a", expected: true, availability: "present" }]);
    expect(result.attempts.map(row => row.attemptId)).toEqual(["try-1", "try-2"]);
    expect(result.events).toHaveLength(2); expect(result.traces).toHaveLength(4);
    expect(result.journalRecords).toEqual(f.records);
    expect(result.unknownRecords).toHaveLength(0);
    for (const row of result.traces) {
      expect(row.availability).toBe("present");
      expect(Buffer.from(row.bytes!)).toEqual(row.eventId ? f.completed.get(row.attemptId) : f.prefixes.get(row.attemptId));
      expect(row.reference!.retainedFrom).toBe(0);
      expect(row.provenance!.sourceHostId).toBe("survivor-host");
    }
    expect(f.calls).toHaveLength(6);
    expect(Object.isFrozen(result.cohort.sessions[0]!.attempts)).toBe(true);
    expect(result.issues).toEqual([]);
  });

  it("keeps the full frozen denominator and unknowns when no external source exists", async () => {
    const result = await buildRecoveryOffVictimInventory({ observerHostId: "observer-host", victimHostId: "victim-host", cohort: cohort() });
    expect(result.complete).toBe(false);
    expect(result.sessions).toHaveLength(1); expect(result.attempts).toHaveLength(2); expect(result.events).toHaveLength(2); expect(result.traces).toHaveLength(2);
    expect(result.attempts.every(row => row.availability === "unknown")).toBe(true);
    expect(result.issues.join("\n")).toContain("no victim fallback");
  });

  it("requires an explicit sealed source manifest, not just a complete-looking snapshot", async () => {
    const f = await fixture();
    const absent: RecoveryOffVictimSource = { ...f.source, readManifest: async () => ({ status: "missing", detail: "No retained enumeration" }) };
    const result = await f.inventory(absent);
    expect(result.complete).toBe(false); expect(result.attempts).toHaveLength(2);
    expect(result.issues.join("\n")).toContain("Source manifest missing");
    const unsealed: RecoveryOffVictimSource = { ...f.source, readManifest: async () => ({ status: "present", value: { ...sealedManifest(f.expected, f.records), sealed: false }, provenance: provenance() }) };
    expect((await f.inventory(unsealed)).complete).toBe(false);
  });

  it("fails when a whole physical retry is omitted from a successful snapshot", async () => {
    const f = await fixture();
    const missing = f.records.filter(record => !record.key.includes("try-2"));
    const source: RecoveryOffVictimSource = { ...f.source, readJournal: async () => ({ status: "present", value: { complete: true, records: missing }, provenance: provenance() }) };
    const result = await f.inventory(source);
    expect(result.complete).toBe(false);
    expect(result.attempts.find(row => row.attemptId === "try-2")).toMatchObject({ availability: "unknown", expected: true });
    expect(result.issues.join("\n")).toContain("Missing sealed journal record");
    // Even an enumeration omitting the retry cannot erase the independent denominator.
    const omittedFromBoth: RecoveryOffVictimSource = { ...source, readManifest: async () => ({ status: "present", value: sealedManifest(f.expected, missing), provenance: provenance() }) };
    const both = await f.inventory(omittedFromBoth);
    expect(both.complete).toBe(false);
    // A whole-history gap removes evidence authority: absent expected rows stay unknown.
    expect(both.attempts.find(row => row.attemptId === "try-2")).toMatchObject({ availability: "unknown" });
    expect(both.events.find(row => row.attemptId === "try-2")).toMatchObject({ availability: "unknown" });
    expect(both.traces.find(row => row.attemptId === "try-2")).toMatchObject({ availability: "unknown" });
  });

  it("lists a completely missing expected session rather than deriving sessions from successes", async () => {
    const f = await fixture();
    const expected: RecoveryInventoryCohort = { ...f.expected, sessions: [...f.expected.sessions, { sessionId: "never-observed", attempts: [{ attemptId: "not-started", eventIds: ["missing-event"], traceIds: ["missing-trace"] }] }] };
    const source: RecoveryOffVictimSource = { ...f.source, readManifest: async () => ({ status: "present", value: sealedManifest(expected, f.records), provenance: provenance() }) };
    const result = await f.inventory(source, expected);
    expect(result.complete).toBe(false);
    expect(result.sessions).toContainEqual({ sessionId: "never-observed", expected: true, availability: "missing" });
    expect(result.attempts.find(row => row.sessionId === "never-observed")!.availability).toBe("missing");
    expect(result.events.find(row => row.sessionId === "never-observed")!.availability).toBe("missing");
    expect(result.traces.find(row => row.sessionId === "never-observed")!.availability).toBe("missing");
  });

  it("retains unexpected retries/late events/traces and fails the frozen expectation", async () => {
    const f = await fixture();
    const expected: RecoveryInventoryCohort = { ...f.expected, sessions: [{ sessionId: "session-a", attempts: [{ attemptId: "try-1", eventIds: [], traceIds: [] }] }] };
    const source: RecoveryOffVictimSource = { ...f.source, readManifest: async () => ({ status: "present", value: sealedManifest(expected, f.records), provenance: provenance() }) };
    const result = await f.inventory(source, expected);
    expect(result.complete).toBe(false); expect(result.attempts).toHaveLength(2); expect(result.events).toHaveLength(2); expect(result.traces).toHaveLength(4);
    expect(result.attempts[1]!.expected).toBe(false);
    expect(result.events.every(row => !row.expected)).toBe(true);
    expect(result.traces.every(row => !row.expected)).toBe(true);
  });

  it("preserves duplicate and unknown source records without collapsing them", async () => {
    const f = await fixture(true), result = await f.inventory();
    expect(result.complete).toBe(false); expect(result.unknownRecords).toHaveLength(1);
    expect(Buffer.from(result.unknownRecords[0]!.record.bytes).toString()).toContain('"unknown":"retained"');
    const duplicate: RecoveryOffVictimSource = { ...f.source, readJournal: async () => ({ status: "present", value: { complete: true, records: [...f.records, f.records[0]!] }, provenance: provenance() }) };
    const duplicated = await f.inventory(duplicate);
    expect(duplicated.complete).toBe(false); expect(duplicated.journalRecords).toHaveLength(f.records.length + 1);
    expect(duplicated.attempts.filter(row => row.attemptId === "try-1")).toHaveLength(2);
    expect(duplicated.issues.join("\n")).toContain("Repeated, unordered or invalid");
  });

  it("rejects victim observer/source before reads, and refuses hidden victim dependencies", async () => {
    const f = await fixture();
    await expect(buildRecoveryOffVictimInventory({ observerHostId: "victim-host", victimHostId: "victim-host", cohort: f.expected, source: f.source })).rejects.toThrow("observer");
    await expect(f.inventory({ ...f.source, sourceHostId: "victim-host" })).rejects.toThrow("source");
    expect(f.calls).toHaveLength(0);
    const source: RecoveryOffVictimSource = { ...f.source, readJournal: async () => ({ status: "present", value: { complete: true, records: f.records }, provenance: { ...provenance(), dependencyHostIds: ["survivor-host", "victim-host"] } }) };
    const result = await f.inventory(source);
    expect(result.complete).toBe(false); expect(result.journalProvenance).toBeUndefined(); expect(result.journalRecords).toHaveLength(f.records.length);
    expect(result.issues.join("\n")).toContain("victim-dependent");
  });

  it("does not count embedded hashes/raw bytes as a substitute for externally retrievable trace evidence", async () => {
    const f = await fixture();
    const missing: RecoveryOffVictimSource = { ...f.source, readTrace: async () => ({ status: "missing", detail: "External trace object unavailable" }) };
    const result = await f.inventory(missing);
    expect(result.complete).toBe(false); expect(result.traces).toHaveLength(4);
    expect(result.traces.every(row => row.availability === "missing" && row.reference!.bytes.length > 0)).toBe(true);
    const mismatch: RecoveryOffVictimSource = { ...f.source, readTrace: async ref => ({ status: "present", value: new Uint8Array(ref.byteLength).fill(9), provenance: provenance("trace-store") }) };
    const wrong = await f.inventory(mismatch);
    expect(wrong.complete).toBe(false); expect(wrong.traces.every(row => row.availability === "unknown" && row.bytes!.length > 0)).toBe(true);
    expect(wrong.issues.join("\n")).toContain("Exact raw trace prefix mismatch");
  });

  it("does not infer topology from a trace's path or accept victim-dependent trace read provenance", async () => {
    const f = await fixture();
    const source: RecoveryOffVictimSource = { ...f.source, readTrace: async ref => ({ status: "present", value: ref.bytes,
      provenance: { ...provenance("trace-store"), dependencyHostIds: ["survivor-host", "victim-host"] } }) };
    const result = await f.inventory(source);
    expect(result.complete).toBe(false); expect(result.traces.every(row => row.availability === "unknown")).toBe(true);
    expect(result.topologyQualification).toBe("not-established-by-inventory");
  });

  it("retains unknown reads and snapshots the frozen cohort before any source await", async () => {
    const f = await fixture();
    const expected = cohort();
    let release!: () => void;
    const waiting = new Promise<void>(resolve => { release = resolve; });
    const source: RecoveryOffVictimSource = { ...f.source, readManifest: async policy => { await waiting; return f.source.readManifest(policy); },
      readTrace: async () => { throw new Error("External object read uncertain"); } };
    const pending = f.inventory(source, expected);
    Object.assign(expected.sessions[0]!, { sessionId: "changed-after-read-start" }); release();
    const result = await pending;
    expect(result.cohort.sessions[0]!.sessionId).toBe("session-a");
    expect(result.complete).toBe(false); expect(result.traces.every(row => row.availability === "unknown")).toBe(true);
    expect(result.issues.join("\n")).toContain("External object read uncertain");
  });

  it("fails and retains unreferenced raw objects or a sealed interrupted suffix outside the last journal prefix", async () => {
    const f = await fixture(), original = sealedManifest(f.expected, f.records);
    const extra = { sessionId: "unknown-session", attemptId: "unknown-attempt", traceId: "orphan-trace", storeId: "trace-store", objectKey: "unreferenced.bin", byteLength: 9, sha256: sha(Buffer.from("raw-orphan")) };
    const source: RecoveryOffVictimSource = { ...f.source, readManifest: async () => ({ status: "present", value: { ...original, traces: [...original.traces, extra] }, provenance: provenance() }) };
    const result = await f.inventory(source);
    expect(result.complete).toBe(false);
    expect(result.traces.find(row => row.traceId === "orphan-trace")).toMatchObject({ availability: "unknown", sourceManifestEntry: extra });
    expect(result.attempts.find(row => row.attemptId === "unknown-attempt")).toMatchObject({ expected: false, availability: "unknown" });
    expect(result.sessions.find(row => row.sessionId === "unknown-session")).toMatchObject({ expected: false });
    const larger = { ...original.traces[0]!, byteLength: original.traces[0]!.byteLength + 1, sha256: "a".repeat(64) };
    const suffix: RecoveryOffVictimSource = { ...f.source, readManifest: async () => ({ status: "present", value: { ...original, traces: [larger, original.traces[1]!] }, provenance: provenance() }) };
    const retained = await f.inventory(suffix);
    expect(retained.complete).toBe(false);
    expect(retained.traces.find(row => row.sourceManifestEntry?.sha256 === larger.sha256)).toMatchObject({ availability: "unknown", sourceManifestEntry: larger });
    expect(retained.issues.join("\n")).toContain("Full sealed raw trace differs");
  });

  it("fails a wrong-cohort manifest and duplicate frozen identities instead of guessing", async () => {
    const f = await fixture();
    const source: RecoveryOffVictimSource = { ...f.source, readManifest: async () => ({ status: "present", value: { ...sealedManifest(f.expected, f.records), cohortSha256: "0".repeat(64) }, provenance: provenance() }) };
    expect((await f.inventory(source)).complete).toBe(false);
    await expect(f.inventory(f.source, { ...f.expected, sessions: [...f.expected.sessions, ...f.expected.sessions] })).rejects.toThrow("repeated frozen session");
  });
});
