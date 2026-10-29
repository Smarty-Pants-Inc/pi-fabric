import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import type {
  B6DurabilityHarness, B6DurabilityHarnessFactory, B6NativeDescriptorFixtureOptions,
} from "./contract.js";
import type {
  RecoveryDurableAppendRequest, RecoveryDurableAppendResult,
  RecoveryDurableEntryRequest, RecoveryNativeEntryRegistrationRequest,
  RecoveryNativeFlushReceipt,
} from "./contract.js";

const controlBytes = (): Uint8Array => Uint8Array.from([0x00, 0x43, 0xff, 0x0a, 0x7f]);
const tuple = (h: B6DurabilityHarness, deliveryId = h.deliveryId, recordKind: "control-seen" | "carrier" = "carrier"): RecoveryDurableEntryRequest => ({
  sessionId: h.sessionId, deliveryId, recordKind,
});
const appendRequest = (
  h: B6DurabilityHarness,
  deliveryId = h.deliveryId,
  recordKind: "control-seen" | "carrier" = "control-seen",
  deadlineMs = h.originalInputDeadlineMs,
  signal?: AbortSignal,
): RecoveryDurableAppendRequest => ({
  sessionId: h.sessionId, deliveryId, recordKind, deadlineMs, ...(signal ? { signal } : {}),
});
const requireDurable = (result: RecoveryDurableAppendResult): number => {
  expect(result.durable).toBe(true);
  if (!result.durable) throw new Error(`Expected durable append; received ${result.reason}`);
  return result.seq;
};
const seedAndCommitControl = async (
  h: B6DurabilityHarness,
  deliveryId = h.deliveryId,
  data = controlBytes(),
): Promise<number> => {
  h.seedControlSeen({ deliveryId, bytes: data, originalDeadlineMs: h.originalInputDeadlineMs });
  return requireDurable(await h.adapter.awaitDurableAppend(appendRequest(h, deliveryId, "control-seen")));
};
const makeDescriptor = (
  h: B6DurabilityHarness,
  deliveryId = h.deliveryId,
  options: Omit<B6NativeDescriptorFixtureOptions, "deliveryId"> = {},
): RecoveryNativeEntryRegistrationRequest => h.nativeDescriptorFixture({ ...options, deliveryId });
const expectNoPiFileIo = (h: B6DurabilityHarness): void => {
  expect(h.observe().nativePiFileIo).toEqual({ opens: 0, reads: 0, writes: 0, fsyncs: 0 });
};
const mutate = (request: RecoveryNativeEntryRegistrationRequest, patch: Record<string, unknown>): void => {
  Object.assign(request as unknown as Record<string, unknown>, patch);
};
const refusedNative = async (
  h: B6DurabilityHarness,
  request: RecoveryNativeEntryRegistrationRequest,
  allowed: readonly ("conflict" | "store-unavailable" | "superseded")[] = ["conflict", "store-unavailable"],
): Promise<void> => {
  const result = await h.adapter.registerNativeEntry(request);
  expect(result.registered).toBe(false);
  if (!result.registered) expect(allowed).toContain(result.reason);
};
const nativeEntryDeliveryId = (sessionId: string, entryId: string): string =>
  `pi-native-entry:v1:${createHash("sha256").update(JSON.stringify([sessionId, entryId]), "utf8").digest("hex")}`;
const registerAndCommitCarrier = async (
  h: B6DurabilityHarness,
  request = makeDescriptor(h),
): Promise<number> => {
  expect(await h.adapter.registerNativeEntry(request)).toMatchObject({ registered: true });
  return requireDurable(await h.adapter.awaitDurableAppend(appendRequest(h, request.deliveryId, "carrier", request.deadlineMs)));
};

/** Registers callable Vitest cases against either a consumer fake or a real-store harness.
 * Every case receives an isolated harness and closes it in finally. Keep reason: native trust
 * boundary and acknowledged-work data safety; this suite is not a Pi crash/recovery proof. */
export function runConformance(factory: B6DurabilityHarnessFactory): void {
  const test = (name: string, run: (h: B6DurabilityHarness) => Promise<void> | void) => it(name, async () => {
    const h = await factory.create();
    try {
      await run(h);
    } finally {
      try { expectNoPiFileIo(h); } finally { await h.close(); }
    }
  });

  describe(`l7-b6-durability-v1 / ${factory.name}`, () => {
    test("registration is correlation only; only the bounded commit creates a sequence and readable bytes", async h => {
      await h.recover("recovered");
      await seedAndCommitControl(h);
      const request = makeDescriptor(h);
      expect(await h.adapter.registerNativeEntry(request)).toEqual({ registered: true, existing: false });
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "not-found" });
      expect(h.observe().committed.filter(row => row.recordKind === "carrier")).toHaveLength(0);

      const gate = h.gateNextCommit();
      let finished = false;
      let pending: Promise<RecoveryDurableAppendResult> | undefined;
      try {
        pending = h.adapter.awaitDurableAppend(appendRequest(h, request.deliveryId, "carrier", request.deadlineMs))
          .then(result => { finished = true; return result; }, error => { finished = true; throw error; });
        await gate.entered;
        expect(finished).toBe(false);
        expect(h.observe().committed.filter(row => row.recordKind === "carrier")).toHaveLength(0);
        gate.release();
      } finally {
        gate.release();
        if (pending) await Promise.allSettled([pending]);
      }
      const seq = requireDurable(await pending!);
      const read = await h.adapter.readDurableEntry(tuple(h));
      expect(read).toMatchObject({ found: true, seq });
      if (read.found) expect(read.bytes).toEqual(request.bytes);
    });

    test("control-seen preserves exact seeded bytes and never restamps its original input deadline", async h => {
      await h.recover("recovered");
      const exact = controlBytes();
      h.setNow(h.originalInputDeadlineMs - 1);
      h.seedControlSeen({ bytes: exact, originalDeadlineMs: h.originalInputDeadlineMs });
      exact.fill(0); // The harness-only seed owns a copy, not the caller's mutable array.
      const first = await h.adapter.awaitDurableAppend(appendRequest(h, h.deliveryId, "control-seen", h.originalInputDeadlineMs));
      const seq = requireDurable(first);
      const committed = h.observe().committed[0]!;
      expect(committed.originalDeadlineMs).toBe(h.originalInputDeadlineMs);
      expect(committed.bytes).toEqual(controlBytes());
      h.advanceNow(2);
      expect(await h.adapter.awaitDurableAppend(appendRequest(h, h.deliveryId, "control-seen", h.originalInputDeadlineMs)))
        .toEqual({ durable: false, reason: "deadline" });
      expect(h.observe().committed).toHaveLength(1);
      expect(h.observe().committed[0]).toMatchObject({ seq, originalDeadlineMs: h.originalInputDeadlineMs });
    });

    test("identical tuple appends serialize, are idempotent, and reuse one sequence", async h => {
      await h.recover("recovered");
      h.seedControlSeen({ bytes: controlBytes(), originalDeadlineMs: h.originalInputDeadlineMs });
      const request = appendRequest(h);
      const gate = h.gateNextCommit();
      let first: Promise<RecoveryDurableAppendResult> | undefined;
      let second: Promise<RecoveryDurableAppendResult> | undefined;
      try {
        first = h.adapter.awaitDurableAppend(request);
        await gate.entered;
        second = h.adapter.awaitDurableAppend({ ...request });
        gate.release();
      } finally {
        gate.release();
        await Promise.allSettled([...(first ? [first] : []), ...(second ? [second] : [])]);
      }
      const firstSeq = requireDurable(await first!);
      expect(requireDurable(await second!)).toBe(firstSeq);
      expect(requireDurable(await h.adapter.awaitDurableAppend(request))).toBe(firstSeq);
      expect(h.observe().committed).toHaveLength(1);
      expect(h.observe().committed[0]?.seq).toBe(firstSeq);
    });

    test("record kind and bound session isolate the same delivery id", async h => {
      await h.recover("recovered");
      const data = controlBytes();
      const controlSeq = await seedAndCommitControl(h, h.deliveryId, data);
      expect(await h.adapter.readDurableEntry(tuple(h, h.deliveryId, "carrier"))).toEqual({ found: false, reason: "not-found" });
      const descriptor = makeDescriptor(h);
      const carrierSeq = await registerAndCommitCarrier(h, descriptor);
      expect(carrierSeq).not.toBe(controlSeq);
      const control = await h.adapter.readDurableEntry(tuple(h, h.deliveryId, "control-seen"));
      const carrier = await h.adapter.readDurableEntry(tuple(h, h.deliveryId, "carrier"));
      expect(control).toMatchObject({ found: true, seq: controlSeq });
      expect(carrier).toMatchObject({ found: true, seq: carrierSeq });
      if (control.found) expect(control.bytes).toEqual(data);
      if (carrier.found) expect(carrier.bytes).toEqual(descriptor.bytes);

      const before = h.observe().committed;
      const otherSession = `${h.sessionId}-other`;
      expect(await h.adapter.readDurableEntry({ sessionId: otherSession, deliveryId: h.deliveryId, recordKind: "control-seen" }))
        .toEqual({ found: false, reason: "superseded" });
      expect(await h.adapter.readDurableEntry({ sessionId: otherSession, deliveryId: h.deliveryId, recordKind: "carrier" }))
        .toEqual({ found: false, reason: "superseded" });
      expect(await h.adapter.awaitDurableAppend({ ...appendRequest(h), sessionId: otherSession }))
        .toEqual({ durable: false, reason: "superseded" });
      expect(await h.adapter.awaitDurableAppend({ ...appendRequest(h, h.deliveryId, "carrier"), sessionId: otherSession }))
        .toEqual({ durable: false, reason: "superseded" });
      expect(h.observe().committed).toEqual(before);
    });

    test("derived native entries register, commit and read without fabricated input/control records", async h => {
      await h.recover("recovered");
      const entryId = "maintenance-entry-17";
      const derivedDeliveryId = nativeEntryDeliveryId(h.sessionId, entryId);
      const descriptor = makeDescriptor(h, derivedDeliveryId, {
        entryId, deadlineMs: h.originalInputDeadlineMs + 30_000,
      });
      const seq = await registerAndCommitCarrier(h, descriptor);
      expect(h.observe().committed).toHaveLength(1);
      expect(h.observe().committed[0]).toMatchObject({
        sessionId: h.sessionId, deliveryId: derivedDeliveryId, recordKind: "carrier",
        seq, originalDeadlineMs: descriptor.deadlineMs,
      });
      expect(await h.adapter.readDurableEntry(tuple(h, derivedDeliveryId))).toMatchObject({ found: true, seq });
    });

    test("derived native delivery IDs bind exact session and entryId", async h => {
      await h.recover("recovered");
      const entryId = "terminal-entry-29";
      const wrongHash = `pi-native-entry:v1:${"f".repeat(64)}`;
      const bad = makeDescriptor(h, wrongHash, { entryId });
      await refusedNative(h, bad, ["conflict"]);
      const rightId = nativeEntryDeliveryId(h.sessionId, entryId);
      const wrongSession = makeDescriptor(h, rightId, { entryId });
      mutate(wrongSession, { sessionId: `${h.sessionId}-other` });
      await refusedNative(h, wrongSession, ["conflict", "superseded"]);
      expect(h.observe().committed).toHaveLength(0);
    });

    test("uncommitted registration is discarded on recovery and native re-emission can barrier it", async h => {
      await h.recover("recovered");
      const entryId = "control-marker-entry-31";
      const delivery = nativeEntryDeliveryId(h.sessionId, entryId);
      const first = makeDescriptor(h, delivery, { entryId });
      expect(await h.adapter.registerNativeEntry(first)).toEqual({ registered: true, existing: false });
      expect(await h.adapter.readDurableEntry(tuple(h, delivery))).toEqual({ found: false, reason: "not-found" });
      expect(h.observe().committed).toHaveLength(0);

      await h.reopen("recovered"); // Successful reconstruction of an empty committed log.
      expect(h.observe().registrations).toHaveLength(0);
      expect(await h.adapter.readDurableEntry(tuple(h, delivery))).toEqual({ found: false, reason: "not-found" });
      expect(await h.adapter.awaitDurableAppend(appendRequest(h, delivery, "carrier", first.deadlineMs)))
        .toEqual({ durable: false, reason: "not-found" });

      const reemitted = makeDescriptor(h, delivery, { entryId });
      expect(await h.adapter.registerNativeEntry(reemitted)).toEqual({ registered: true, existing: false });
      const seq = requireDurable(await h.adapter.awaitDurableAppend(appendRequest(h, delivery, "carrier", reemitted.deadlineMs)));
      const read = await h.adapter.readDurableEntry(tuple(h, delivery));
      expect(read).toMatchObject({ found: true, seq });
      if (read.found) expect(read.bytes).toEqual(reemitted.bytes);
      expect(h.observe().committed).toHaveLength(1);
    });

    test("absence is not authoritative until successful REQUIRED committed-log recovery", async h => {
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "store-unavailable" });
      await h.reopen("unknown");
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "store-unavailable" });
      await h.reopen("unavailable");
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "store-unavailable" });
      await h.reopen("recovered");
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "not-found" });
    });

    test("store loss invalidates cached authority until successful committed-log recovery", async h => {
      await h.recover("recovered");
      await seedAndCommitControl(h);
      const descriptor = makeDescriptor(h);
      const seq = await registerAndCommitCarrier(h, descriptor);
      h.invalidateAvailability();
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "store-unavailable" });
      expect(await h.adapter.awaitDurableAppend(appendRequest(h, descriptor.deliveryId, "carrier", descriptor.deadlineMs)))
        .toEqual({ durable: false, reason: "store-unavailable" });
      expect(await h.adapter.registerNativeEntry(descriptor)).toEqual({ registered: false, reason: "store-unavailable" });
      await h.reopen("unknown");
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "store-unavailable" });
      await h.reopen("unavailable");
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "store-unavailable" });
      await h.reopen("recovered");
      const restored = await h.adapter.readDurableEntry(tuple(h));
      expect(restored).toMatchObject({ found: true, seq });
      if (restored.found) expect(restored.bytes).toEqual(descriptor.bytes);
    });

    test("valid native receipts cover exact serializer bytes for both closed receipt variants", async h => {
      await h.recover("recovered");
      const fileReceipt = makeDescriptor(h, `${h.deliveryId}-file`, { receiptKind: "file-fsync" });
      const ownedReceipt = makeDescriptor(h, `${h.deliveryId}-owned`, { receiptKind: "owned-journal-commit" });
      for (const descriptor of [fileReceipt, ownedReceipt]) {
        expect(await h.adapter.registerNativeEntry(descriptor)).toEqual({ registered: true, existing: false });
        const seq = requireDurable(await h.adapter.awaitDurableAppend(appendRequest(h, descriptor.deliveryId, "carrier", descriptor.deadlineMs)));
        const read = await h.adapter.readDurableEntry(tuple(h, descriptor.deliveryId));
        expect(read).toMatchObject({ found: true, seq });
        if (read.found) expect(read.bytes).toEqual(descriptor.bytes);
      }
      expect(fileReceipt.bytes.at(-1)).toBe(0x0a);
      expect(fileReceipt.bytes.length).toBe(fileReceipt.location.length + 1);
      expect(ownedReceipt.flushReceipt.kind).toBe("owned-journal-commit");
    });

    test("native provenance is an explicit trusted fixture, not JSON/hash shape", async h => {
      await h.recover("recovered");
      const trusted = makeDescriptor(h);
      const untrustedClone: RecoveryNativeEntryRegistrationRequest = {
        ...trusted, location: { ...trusted.location }, bytes: new Uint8Array(trusted.bytes),
        flushReceipt: { ...trusted.flushReceipt },
      };
      expect(await h.adapter.registerNativeEntry(trusted)).toEqual({ registered: true, existing: false });
      expect(await h.adapter.registerNativeEntry(untrustedClone)).toEqual({ registered: true, existing: true });

      const unproved = makeDescriptor(h, `${h.deliveryId}-unproved`, { trusted: false });
      await refusedNative(h, unproved, ["conflict"]);
    });

    test("an earlier call deadline and a live signal do not change registration identity or sequence", async h => {
      await h.recover("recovered");
      const lifecycleDeadline = h.originalInputDeadlineMs + 30_000;
      const descriptor = makeDescriptor(h, `${h.deliveryId}-lifecycle`, {
        entryId: "same-lifecycle-entry", deadlineMs: lifecycleDeadline,
      });
      const seq = await registerAndCommitCarrier(h, descriptor);
      const retryDeadline = h.now() + 1_000;
      const signal = new AbortController().signal;
      const retry: RecoveryNativeEntryRegistrationRequest = {
        ...descriptor, location: { ...descriptor.location }, bytes: new Uint8Array(descriptor.bytes),
        flushReceipt: { ...descriptor.flushReceipt }, deadlineMs: retryDeadline, signal,
      };
      expect(await h.adapter.registerNativeEntry(retry)).toEqual({ registered: true, existing: true });
      expect(await h.adapter.awaitDurableAppend(appendRequest(h, retry.deliveryId, "carrier", retryDeadline, signal)))
        .toEqual({ durable: true, seq });
      expect(h.observe().committed).toMatchObject([{ seq, originalDeadlineMs: lifecycleDeadline }]);
      expect(h.observe().committed.filter(row => row.recordKind === "carrier")).toHaveLength(1);
    });

    test("native registration rejects missing final LF", async h => {
      await h.recover("recovered");
      const descriptor = makeDescriptor(h);
      descriptor.bytes[descriptor.bytes.length - 1] = 0;
      await refusedNative(h, descriptor);
    });

    test("native registration checks exact location and session file", async h => {
      await h.recover("recovered");
      const wrongOffset = makeDescriptor(h, `${h.deliveryId}-offset`);
      mutate(wrongOffset, { location: { ...wrongOffset.location, offset: wrongOffset.location.offset + 1 } });
      await refusedNative(h, wrongOffset);
      const wrongLength = makeDescriptor(h, `${h.deliveryId}-length`);
      mutate(wrongLength, { location: { ...wrongLength.location, length: wrongLength.location.length + 1 } });
      await refusedNative(h, wrongLength);
      const wrongFile = makeDescriptor(h, `${h.deliveryId}-file`);
      mutate(wrongFile, { location: { ...wrongFile.location, file: `${h.sessionFile}.other` } });
      await refusedNative(h, wrongFile);
    });

    test("native registration checks bound session and serialized actual entryId", async h => {
      await h.recover("recovered");
      const wrongSession = makeDescriptor(h, `${h.deliveryId}-session`);
      mutate(wrongSession, { sessionId: `${h.sessionId}-other` });
      await refusedNative(h, wrongSession, ["conflict", "superseded"]);
      const wrongEntryId = makeDescriptor(h, `${h.deliveryId}-entry`);
      mutate(wrongEntryId, { entryId: `${wrongEntryId.entryId}-forged` });
      await refusedNative(h, wrongEntryId);
    });

    test("native registration checks the exact SHA-256 including final LF", async h => {
      await h.recover("recovered");
      const descriptor = makeDescriptor(h);
      mutate(descriptor, { bytesSha256: "0".repeat(64) });
      await refusedNative(h, descriptor);
    });

    test("native registration rejects incomplete file-fsync coverage and non-durable namespace", async h => {
      await h.recover("recovered");
      const shortReceipt = makeDescriptor(h, `${h.deliveryId}-short`);
      if (shortReceipt.flushReceipt.kind !== "file-fsync") throw new Error("Expected fixture file-fsync receipt");
      const shortEnd = shortReceipt.location.offset + shortReceipt.location.length;
      mutate(shortReceipt, { flushReceipt: { ...shortReceipt.flushReceipt, bytesThrough: shortEnd } });
      await refusedNative(h, shortReceipt);
      const namespace = makeDescriptor(h, `${h.deliveryId}-namespace`);
      if (namespace.flushReceipt.kind !== "file-fsync") throw new Error("Expected fixture file-fsync receipt");
      mutate(namespace, { flushReceipt: { ...namespace.flushReceipt, namespaceDurable: false } });
      await refusedNative(h, namespace);
    });

    test("owned custody or empty-prefix receipt is not successful append proof", async h => {
      await h.recover("recovered");
      await seedAndCommitControl(h);
      const descriptor = makeDescriptor(h, h.deliveryId, { receiptKind: "owned-journal-commit" });
      const empty: RecoveryNativeFlushReceipt = {
        kind: "owned-journal-commit", file: h.sessionFile, bytes: 0,
        sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      };
      mutate(descriptor, { flushReceipt: empty });
      await refusedNative(h, descriptor);
      expect(await h.adapter.awaitDurableAppend(appendRequest(h, descriptor.deliveryId, "carrier", descriptor.deadlineMs)))
        .toEqual({ durable: false, reason: "not-found" });
      expect(h.observe().committed.filter(row => row.recordKind === "carrier")).toHaveLength(0);
    });

    test("registration copies mutable control and native byte arrays; reads return detached bytes", async h => {
      await h.recover("recovered");
      const originalControl = controlBytes();
      h.seedControlSeen({ bytes: originalControl, originalDeadlineMs: h.originalInputDeadlineMs });
      originalControl.fill(0);
      const controlSeq = requireDurable(await h.adapter.awaitDurableAppend(appendRequest(h)));
      const descriptor = makeDescriptor(h);
      const originalCarrier = new Uint8Array(descriptor.bytes);
      expect(await h.adapter.registerNativeEntry(descriptor)).toEqual({ registered: true, existing: false });
      descriptor.bytes.fill(0);
      const carrierSeq = requireDurable(await h.adapter.awaitDurableAppend(appendRequest(h, descriptor.deliveryId, "carrier", descriptor.deadlineMs)));
      const control = await h.adapter.readDurableEntry(tuple(h, h.deliveryId, "control-seen"));
      const carrier = await h.adapter.readDurableEntry(tuple(h));
      expect(control).toMatchObject({ found: true, seq: controlSeq });
      expect(carrier).toMatchObject({ found: true, seq: carrierSeq });
      if (control.found) expect(control.bytes).toEqual(controlBytes());
      if (carrier.found) {
        expect(carrier.bytes).toEqual(originalCarrier);
        carrier.bytes.fill(0);
        const reread = await h.adapter.readDurableEntry(tuple(h));
        if (reread.found) expect(reread.bytes).toEqual(originalCarrier);
      }
    });

    test("changed immutable registration conflicts and poisons later barrier/read as superseded", async h => {
      await h.recover("recovered");
      await seedAndCommitControl(h);
      const first = makeDescriptor(h);
      const seq = await registerAndCommitCarrier(h, first);
      const changed = makeDescriptor(h, first.deliveryId, { entryId: `${first.entryId}-changed` });
      expect(await h.adapter.registerNativeEntry(changed)).toEqual({ registered: false, reason: "conflict" });
      expect(await h.adapter.awaitDurableAppend(appendRequest(h, first.deliveryId, "carrier", first.deadlineMs)))
        .toEqual({ durable: false, reason: "superseded" });
      expect(await h.adapter.readDurableEntry(tuple(h, first.deliveryId, "carrier")))
        .toEqual({ found: false, reason: "superseded" });
      expect(h.observe().committed.find(row => row.recordKind === "carrier")?.seq).toBe(seq);
    });

    test("a later valid larger-prefix receipt strengthens the same binding and sequence", async h => {
      await h.recover("recovered");
      await seedAndCommitControl(h);
      const prefix = new TextEncoder().encode('{"fixture":"same-prefix"}\n');
      const first = makeDescriptor(h, h.deliveryId, { entryId: "same-entry", prefixBytes: prefix });
      const seq = await registerAndCommitCarrier(h, first);
      const initialReceipt = h.observe().registrations[0]!.flushReceipt;
      const firstCoverage = initialReceipt.kind === "file-fsync" ? initialReceipt.bytesThrough : initialReceipt.bytes;
      const stronger = makeDescriptor(h, h.deliveryId, {
        entryId: "same-entry", prefixBytes: prefix, trailingCommittedBytes: new Uint8Array(64).fill(0x61),
      });
      expect(await h.adapter.registerNativeEntry(stronger)).toEqual({ registered: true, existing: true });
      const strengthenedReceipt = h.observe().registrations[0]!.flushReceipt;
      const strongerCoverage = strengthenedReceipt.kind === "file-fsync" ? strengthenedReceipt.bytesThrough : strengthenedReceipt.bytes;
      expect(strongerCoverage).toBeGreaterThan(firstCoverage);
      expect(requireDurable(await h.adapter.awaitDurableAppend(appendRequest(h, stronger.deliveryId, "carrier", stronger.deadlineMs)))).toBe(seq);
      expect(h.observe().committed.filter(row => row.recordKind === "carrier")).toHaveLength(1);
    });

    test("abort and expiry before commit never report a durable append", async h => {
      await h.recover("recovered");
      h.seedControlSeen({ bytes: controlBytes(), originalDeadlineMs: h.originalInputDeadlineMs });
      const controller = new AbortController();
      controller.abort();
      expect(await h.adapter.awaitDurableAppend(appendRequest(h, h.deliveryId, "control-seen", h.originalInputDeadlineMs, controller.signal)))
        .toEqual({ durable: false, reason: "aborted" });
      h.setNow(h.originalInputDeadlineMs);
      expect(await h.adapter.awaitDurableAppend(appendRequest(h)))
        .toEqual({ durable: false, reason: "deadline" });
      expect(h.observe().committed).toHaveLength(0);
    });

    test("abort while held at the pre-commit gate cannot publish or ACK", async h => {
      await h.recover("recovered");
      h.seedControlSeen({ bytes: controlBytes(), originalDeadlineMs: h.originalInputDeadlineMs });
      const controller = new AbortController();
      const gate = h.gateNextCommit();
      let pending: Promise<RecoveryDurableAppendResult> | undefined;
      try {
        pending = h.adapter.awaitDurableAppend(appendRequest(h, h.deliveryId, "control-seen", h.originalInputDeadlineMs, controller.signal));
        await gate.entered;
        controller.abort();
        gate.release();
      } finally {
        gate.release();
        if (pending) await Promise.allSettled([pending]);
      }
      expect(await pending!).toEqual({ durable: false, reason: "aborted" });
      expect(h.observe().committed).toHaveLength(0);
    });

    test("deadline while held at the pre-commit gate cannot publish or ACK", async h => {
      await h.recover("recovered");
      h.seedControlSeen({ bytes: controlBytes(), originalDeadlineMs: h.originalInputDeadlineMs });
      const deadline = h.now() + 10;
      const gate = h.gateNextCommit();
      let pending: Promise<RecoveryDurableAppendResult> | undefined;
      try {
        pending = h.adapter.awaitDurableAppend(appendRequest(h, h.deliveryId, "control-seen", deadline));
        await gate.entered;
        h.advanceNow(11);
        gate.release();
      } finally {
        gate.release();
        if (pending) await Promise.allSettled([pending]);
      }
      expect(await pending!).toEqual({ durable: false, reason: "deadline" });
      expect(h.observe().committed).toHaveLength(0);
    });

    test("abort while queued behind a committing tuple cannot create a second commit", async h => {
      await h.recover("recovered");
      h.seedControlSeen({ bytes: controlBytes(), originalDeadlineMs: h.originalInputDeadlineMs });
      const gate = h.gateNextCommit();
      const controller = new AbortController();
      let first: Promise<RecoveryDurableAppendResult> | undefined;
      let second: Promise<RecoveryDurableAppendResult> | undefined;
      try {
        first = h.adapter.awaitDurableAppend(appendRequest(h));
        await gate.entered;
        second = h.adapter.awaitDurableAppend(appendRequest(h, h.deliveryId, "control-seen", h.originalInputDeadlineMs, controller.signal));
        controller.abort();
        gate.release();
      } finally {
        gate.release();
        await Promise.allSettled([...(first ? [first] : []), ...(second ? [second] : [])]);
      }
      const seq = requireDurable(await first!);
      expect(await second!).toEqual({ durable: false, reason: "aborted" });
      expect(h.observe().committed).toHaveLength(1);
      expect(h.observe().committed[0]?.seq).toBe(seq);
    });

    test("deadline while queued behind a committing tuple cannot restamp or duplicate it", async h => {
      await h.recover("recovered");
      h.seedControlSeen({ bytes: controlBytes(), originalDeadlineMs: h.originalInputDeadlineMs });
      const gate = h.gateNextCommit();
      const shortDeadline = h.now() + 10;
      let first: Promise<RecoveryDurableAppendResult> | undefined;
      let second: Promise<RecoveryDurableAppendResult> | undefined;
      try {
        first = h.adapter.awaitDurableAppend(appendRequest(h));
        await gate.entered;
        second = h.adapter.awaitDurableAppend(appendRequest(h, h.deliveryId, "control-seen", shortDeadline));
        h.advanceNow(11);
        gate.release();
      } finally {
        gate.release();
        await Promise.allSettled([...(first ? [first] : []), ...(second ? [second] : [])]);
      }
      const seq = requireDurable(await first!);
      expect(await second!).toEqual({ durable: false, reason: "deadline" });
      expect(h.observe().committed).toHaveLength(1);
      expect(h.observe().committed[0]?.seq).toBe(seq);
    });

    test("lost write acknowledgement recovers the exact committed sequence without a duplicate", async h => {
      await h.recover("recovered");
      await seedAndCommitControl(h);
      const descriptor = makeDescriptor(h);
      expect(await h.adapter.registerNativeEntry(descriptor)).toEqual({ registered: true, existing: false });
      h.loseNextWriteAck();
      let result: RecoveryDurableAppendResult | undefined;
      let rejected = false;
      try {
        result = await h.adapter.awaitDurableAppend(appendRequest(h, descriptor.deliveryId, "carrier", descriptor.deadlineMs));
      } catch {
        rejected = true; // Unknown post-commit outcome is also a valid adapter boundary.
      }
      const allowedUnknownOutcome = rejected ||
        (result !== undefined && result.durable === false && result.reason === "store-unavailable");
      expect(allowedUnknownOutcome).toBe(true);
      const beforeReopen = h.observe().committed.filter(row => row.recordKind === "carrier");
      expect(beforeReopen).toHaveLength(1);
      const committedSeq = beforeReopen[0]!.seq;

      await h.reopen("unknown");
      expect(await h.adapter.readDurableEntry(tuple(h))).toEqual({ found: false, reason: "store-unavailable" });
      await h.reopen("recovered");
      const recoveredEntry = await h.adapter.readDurableEntry(tuple(h));
      expect(recoveredEntry).toMatchObject({ found: true, seq: committedSeq });
      if (recoveredEntry.found) expect(recoveredEntry.bytes).toEqual(descriptor.bytes);
      expect(await h.adapter.awaitDurableAppend(appendRequest(h, descriptor.deliveryId, "carrier", descriptor.deadlineMs)))
        .toEqual({ durable: true, seq: committedSeq });
      expect(h.observe().committed.filter(row => row.recordKind === "carrier")).toHaveLength(1);
      expect(h.observe().nextSeq).toBe(committedSeq + 1);
    });
  });
}
