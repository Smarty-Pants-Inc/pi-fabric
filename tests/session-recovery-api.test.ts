// Keep reason: data safety. Conflicting recovery providers must not silently choose a writer.
// API/type fixtures guard byte/receipt boundaries; they do not implement or qualify B6 storage.
import { createHash } from "node:crypto";
import { createEventBus } from "@earendil-works/pi-coding-agent";
import { describe, expect, expectTypeOf, it } from "vitest";
import {
  registerSessionRecoveryAdapters, sessionRecoveryAdapters,
  type SessionRecoveryAdapters, type RecoveryDurabilityAdapter, type RecoveryDurableEntryRequest,
  type RecoveryDurableAppendRequest, type RecoveryDurableAppendResult,
  type RecoveryDurableEntryResult, type RecoveryToolResultRead,
  type RecoveryNativeEntryLocation, type RecoveryNativeFlushReceipt,
  type RecoveryNativeEntryRegistrationRequest, type RecoveryNativeEntryRegistrationResult,
} from "../src/recovery/l7-interfaces.js";
import type * as PublicRecovery from "../src/index.js";

function providers(): SessionRecoveryAdapters {
  return {
    maintenance: Object.fromEntries(["admitMaintenance", "beginMaintenance", "prepareMaintenance", "consumeMaintenance", "adoptMaintenance", "read", "listPending", "withMaintenanceAttempt", "assertMaintenanceGate", "cancelMaintenance", "reconcileAfterJoin", "captureTaskAbort", "releaseLostMaintenance"].map(name => [name, () => { throw new Error("maintenance not used by this fixture"); }])) as unknown as SessionRecoveryAdapters["maintenance"],
    identity: { validate: async () => { throw new Error("not used"); }, verifyPredecessor: async () => {} },
    inputs: { readRecoveryInput: async () => { throw new Error("not used"); }, admitContinuation: async () => { throw new Error("not used"); } },
    effects: { settlePendingIntents: async () => ({ settled: [], uncertain: 0 }), readToolResult: async () => { throw new Error("not used"); } },
  };
}

describe("session recovery provider registration", () => {
  it("pins the B4 read envelope and B6 exact-byte companion read without changing append arguments", async () => {
    expectTypeOf<RecoveryToolResultRead["source"]>().toEqualTypeOf<"native-entry" | "adapter-original">();
    expectTypeOf<RecoveryToolResultRead["postHook"]>().toEqualTypeOf<boolean>();
    expectTypeOf<keyof RecoveryDurableEntryRequest>().toEqualTypeOf<"sessionId" | "deliveryId" | "recordKind">();
    expectTypeOf<Extract<RecoveryDurableEntryResult, { found: true }>["bytes"]>().toEqualTypeOf<Uint8Array>();
    expectTypeOf<Extract<RecoveryDurableEntryResult, { found: false }>["reason"]>().toEqualTypeOf<"not-found" | "store-unavailable" | "superseded">();
    const bytes = new TextEncoder().encode(' {"role":"toolResult","content":[]}\n');
    const reads: RecoveryDurableEntryRequest[] = [];
    const adapter: RecoveryDurabilityAdapter = {
      awaitDurableAppend: async () => ({ durable: true, seq: 7 }),
      readDurableEntry: async request => { reads.push(request); return { found: true, seq: 7, bytes }; },
      registerNativeEntry: async () => ({ registered: true, existing: false }),
    };
    const request: RecoveryDurableEntryRequest = { sessionId: "s", deliveryId: "d", recordKind: "carrier" };
    const result = await adapter.readDurableEntry(request);
    expect(reads).toEqual([request]);
    expect(result).toEqual({ found: true, seq: 7, bytes });
    if (result.found) expect(result.bytes).toBe(bytes); // Type/fixture proof only, not a B6 implementation.
  });

  it("pins the additive readonly registration types and preserves both existing closed APIs", () => {
    expectTypeOf<PublicRecovery.RecoveryNativeEntryLocation>().toEqualTypeOf<RecoveryNativeEntryLocation>();
    expectTypeOf<PublicRecovery.RecoveryNativeFlushReceipt>().toEqualTypeOf<RecoveryNativeFlushReceipt>();
    expectTypeOf<PublicRecovery.RecoveryNativeEntryRegistrationRequest>().toEqualTypeOf<RecoveryNativeEntryRegistrationRequest>();
    expectTypeOf<PublicRecovery.RecoveryNativeEntryRegistrationResult>().toEqualTypeOf<RecoveryNativeEntryRegistrationResult>();
    expectTypeOf<RecoveryNativeEntryLocation>().toEqualTypeOf<{
      readonly file: string; readonly offset: number; readonly length: number;
    }>();
    expectTypeOf<RecoveryNativeFlushReceipt>().toEqualTypeOf<
      | { readonly kind: "file-fsync"; readonly file: string; readonly bytesThrough: number; readonly namespaceDurable: true }
      | { readonly kind: "owned-journal-commit"; readonly file: string; readonly bytes: number; readonly sha256: string }
    >();
    expectTypeOf<RecoveryNativeEntryRegistrationRequest>().toEqualTypeOf<{
      readonly sessionId: string; readonly deliveryId: string; readonly recordKind: "carrier";
      readonly entryId: string; readonly location: RecoveryNativeEntryLocation;
      readonly bytes: Uint8Array; readonly bytesSha256: string;
      readonly flushReceipt: RecoveryNativeFlushReceipt; readonly deadlineMs: number; readonly signal?: AbortSignal;
    }>();
    expectTypeOf<RecoveryNativeEntryRegistrationResult>().toEqualTypeOf<
      | { readonly registered: true; readonly existing: boolean }
      | { readonly registered: false; readonly reason: "conflict" | "deadline" | "aborted" | "store-unavailable" | "superseded" }
    >();
    expectTypeOf<RecoveryDurabilityAdapter["registerNativeEntry"]>().toEqualTypeOf<
      (request: RecoveryNativeEntryRegistrationRequest) => Promise<RecoveryNativeEntryRegistrationResult>
    >();
    expectTypeOf<keyof RecoveryDurabilityAdapter>().toEqualTypeOf<"awaitDurableAppend" | "readDurableEntry" | "registerNativeEntry">();
    expectTypeOf<RecoveryDurableAppendRequest>().toEqualTypeOf<{
      readonly sessionId: string; readonly deliveryId: string; readonly recordKind: "control-seen" | "carrier";
      readonly deadlineMs: number; readonly signal?: AbortSignal;
    }>();
    expectTypeOf<RecoveryDurableAppendResult>().toEqualTypeOf<
      | { readonly durable: true; readonly seq: number }
      | { readonly durable: false; readonly reason: "deadline" | "aborted" | "store-unavailable" | "superseded" | "not-found" }
    >();
    expectTypeOf<RecoveryDurableEntryRequest>().toEqualTypeOf<{
      readonly sessionId: string; readonly deliveryId: string; readonly recordKind: "control-seen" | "carrier";
    }>();
    expectTypeOf<RecoveryDurableEntryResult>().toEqualTypeOf<
      | { readonly found: true; readonly seq: number; readonly bytes: Uint8Array }
      | { readonly found: false; readonly reason: "not-found" | "store-unavailable" | "superseded" }
    >();
    // Maintenance is a distinct admission owner, never a fabricated input.
    expectTypeOf<keyof SessionRecoveryAdapters>().toEqualTypeOf<"identity" | "inputs" | "effects" | "maintenance">();
  });

  it("exercises both native receipt variants through a fixture, separately from the durable barrier", async () => {
    const bytes = new TextEncoder().encode(JSON.stringify({ id: "entry-1", message: { role: "toolResult", content: "résultat" } }) + "\n");
    const prefix = new TextEncoder().encode('{"type":"session","id":"s"}\n');
    const committedPrefix = new Uint8Array([...prefix, ...bytes]);
    const file = "/retained/session.jsonl";
    const receipts: RecoveryNativeFlushReceipt[] = [
      { kind: "file-fsync", file, bytesThrough: committedPrefix.length, namespaceDurable: true },
      { kind: "owned-journal-commit", file, bytes: committedPrefix.length, sha256: createHash("sha256").update(committedPrefix).digest("hex") },
    ];
    for (const flushReceipt of receipts) {
      const registrations: RecoveryNativeEntryRegistrationRequest[] = [];
      const calls: string[] = [];
      const adapter: RecoveryDurabilityAdapter = {
        registerNativeEntry: async request => { calls.push("register"); registrations.push(request); return { registered: true, existing: false }; },
        awaitDurableAppend: async () => { calls.push("barrier"); return { durable: true, seq: 7 }; },
        readDurableEntry: async () => { calls.push("read"); return { found: true, seq: 7, bytes }; },
      };
      const signal = new AbortController().signal;
      const request: RecoveryNativeEntryRegistrationRequest = {
        sessionId: "s", deliveryId: "pi-native-entry:v1:" + createHash("sha256").update(JSON.stringify(["s", "entry-1"]), "utf8").digest("hex"),
        recordKind: "carrier", entryId: "entry-1", location: { file, offset: prefix.length, length: bytes.length - 1 },
        bytes, bytesSha256: createHash("sha256").update(bytes).digest("hex"), flushReceipt,
        deadlineMs: 1_900_000_000_000, signal,
      };
      expect(await adapter.registerNativeEntry(request)).toEqual({ registered: true, existing: false });
      expect(registrations).toEqual([request]);
      expect(calls).toEqual(["register"]); // Registration does not itself return a seq/durable ACK.
      const tuple = { sessionId: request.sessionId, deliveryId: request.deliveryId, recordKind: request.recordKind };
      expect(await adapter.awaitDurableAppend({ ...tuple, deadlineMs: request.deadlineMs, signal })).toEqual({ durable: true, seq: 7 });
      expect(await adapter.readDurableEntry(tuple)).toEqual({ found: true, seq: 7, bytes });
      expect(calls).toEqual(["register", "barrier", "read"]);
      expect(bytes.length).toBe(request.location.length + 1);
      expect(bytes.at(-1)).toBe(10);
      expect(request.bytesSha256).not.toBe(createHash("sha256").update(bytes.subarray(0, -1)).digest("hex"));
    }
  });

  it("accepts existing correlation and every closed registration refusal without adding barrier reasons", async () => {
    const results: RecoveryNativeEntryRegistrationResult[] = [
      { registered: true, existing: true },
      ...(["conflict", "deadline", "aborted", "store-unavailable", "superseded"] as const).map(reason => ({ registered: false as const, reason })),
    ];
    const bytes = new TextEncoder().encode(JSON.stringify({ id: "user-entry", message: { role: "user", content: "original input" } }) + "\n");
    for (const result of results) {
      const register: RecoveryDurabilityAdapter["registerNativeEntry"] = async () => result;
      expect(await register({
        sessionId: "s", deliveryId: "b5-user-delivery", recordKind: "carrier", entryId: "user-entry",
        location: { file: "/retained/session.jsonl", offset: 0, length: bytes.length - 1 }, bytes,
        bytesSha256: createHash("sha256").update(bytes).digest("hex"),
        flushReceipt: { kind: "file-fsync", file: "/retained/session.jsonl", bytesThrough: bytes.length, namespaceDurable: true },
        deadlineMs: 1_900_000_000_000,
      })).toBe(result); // Result-shape fixture only; it performs no validation/store I/O.
    }
  });

  it("keeps compiler-error counterexamples for missing registration, bad arguments and union widening", () => {
    // Deliberately never invoked: tsc must reject these; vitest does not typecheck.
    const counterexamples = (adapter: RecoveryDurabilityAdapter, request: RecoveryNativeEntryRegistrationRequest) => {
      // @ts-expect-error The new method is mandatory, not an optional adapter capability.
      const missingRegistration: RecoveryDurabilityAdapter = { awaitDurableAppend: adapter.awaitDurableAppend, readDurableEntry: adapter.readDurableEntry };
      // @ts-expect-error Native-entry registration never introduces another control-seen record.
      void adapter.registerNativeEntry({ ...request, recordKind: "control-seen" });
      const { deadlineMs: _deadline, ...unbounded } = request;
      // @ts-expect-error Registration is always bounded by a required absolute deadline.
      void adapter.registerNativeEntry(unbounded);
      // @ts-expect-error Exact bytes remain Uint8Array, not parsed JSON or a string.
      void adapter.registerNativeEntry({ ...request, bytes: "{}\n" });
      // @ts-expect-error A file receipt must affirm namespace durability.
      const badReceipt: RecoveryNativeFlushReceipt = { kind: "file-fsync", file: "f", bytesThrough: 3, namespaceDurable: false };
      // @ts-expect-error Registration's closed refusal union has no not-found.
      const missing: RecoveryNativeEntryRegistrationResult = { registered: false, reason: "not-found" };
      // @ts-expect-error Correlation success has existing, not a durability sequence.
      const falseAck: RecoveryNativeEntryRegistrationResult = { registered: true, seq: 7 };
      // @ts-expect-error Conflict is additive only on registration, never on the fixed barrier.
      const widenedBarrier: RecoveryDurableAppendResult = { durable: false, reason: "conflict" };
      // @ts-expect-error Conflict is not added to the fixed companion read either.
      const widenedRead: RecoveryDurableEntryResult = { found: false, reason: "conflict" };
      // @ts-expect-error The existing barrier accepts no entryId argument.
      void adapter.awaitDurableAppend({ sessionId: "s", deliveryId: "d", recordKind: "carrier", deadlineMs: 1, entryId: "e" });
      // @ts-expect-error The existing companion accepts no deadline argument.
      void adapter.readDurableEntry({ sessionId: "s", deliveryId: "d", recordKind: "carrier", deadlineMs: 1 });
      // @ts-expect-error Request fields are readonly.
      request.deadlineMs = 0;
      // @ts-expect-error Location fields are readonly too; Uint8Array contents still need copying.
      request.location.length = 0;
      void [missingRegistration, badReceipt, missing, falseAck, widenedBarrier, widenedRead];
    };
    expect(counterexamples).toBeTypeOf("function");
  });

  it("leaves ordinary sessions inert", () => {
    expect(sessionRecoveryAdapters({ events: createEventBus() })).toBeUndefined();
  });

  it("composes independently registered providers before startup", () => {
    const pi = { events: createEventBus() }, adapters = providers();
    registerSessionRecoveryAdapters(pi, { inputs: adapters.inputs });
    registerSessionRecoveryAdapters(pi, { effects: adapters.effects });
    registerSessionRecoveryAdapters(pi, { identity: adapters.identity });
    registerSessionRecoveryAdapters(pi, { maintenance: adapters.maintenance });
    expect(sessionRecoveryAdapters(pi)).toEqual(adapters);
  });

  it("does not invent missing effect or input implementations", () => {
    const pi = { events: createEventBus() };
    const unsubscribe = registerSessionRecoveryAdapters(pi, { identity: providers().identity });
    expect(() => sessionRecoveryAdapters(pi)).toThrow("requires identity, inputs, effects and maintenance");
    unsubscribe();
    expect(sessionRecoveryAdapters(pi)).toBeUndefined();
  });

  it("reports conflicting providers outside Pi's exception-swallowing event listener", () => {
    const pi = { events: createEventBus() }, adapters = providers();
    registerSessionRecoveryAdapters(pi, adapters);
    const unsubscribe = registerSessionRecoveryAdapters(pi, { inputs: providers().inputs });
    expect(() => sessionRecoveryAdapters(pi)).toThrow("Conflicting session recovery inputs providers");
    unsubscribe();
    expect(sessionRecoveryAdapters(pi)).toEqual(adapters);
  });

  it("does not let a malformed callback disappear into Pi's swallowed listener errors", () => {
    const pi = { events: createEventBus() };
    registerSessionRecoveryAdapters(pi, null as never);
    expect(() => sessionRecoveryAdapters(pi)).toThrow("Malformed session recovery provider registration");
  });

  it("rejects a malformed composed API before invoking any startup work", () => {
    const pi = { events: createEventBus() }, adapters = providers();
    registerSessionRecoveryAdapters(pi, { ...adapters, effects: {} as never });
    expect(() => sessionRecoveryAdapters(pi)).toThrow("Malformed session recovery provider API");
  });

  it("permits an identical repeated provider without adding another admission owner", () => {
    const pi = { events: createEventBus() }, adapters = providers();
    registerSessionRecoveryAdapters(pi, adapters);
    registerSessionRecoveryAdapters(pi, { inputs: adapters.inputs });
    expect(sessionRecoveryAdapters(pi)).toEqual(adapters);
  });
});
