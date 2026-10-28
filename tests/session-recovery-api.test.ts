// Keep reason: data safety. Conflicting recovery providers must not silently choose a writer.
import { createEventBus } from "@earendil-works/pi-coding-agent";
import { describe, expect, expectTypeOf, it } from "vitest";
import {
  registerSessionRecoveryAdapters, sessionRecoveryAdapters,
  type SessionRecoveryAdapters, type RecoveryDurabilityAdapter, type RecoveryDurableEntryRequest,
  type RecoveryDurableEntryResult, type RecoveryToolResultRead,
} from "../src/recovery/l7-interfaces.js";

function providers(): SessionRecoveryAdapters {
  return {
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
    };
    const request: RecoveryDurableEntryRequest = { sessionId: "s", deliveryId: "d", recordKind: "carrier" };
    const result = await adapter.readDurableEntry(request);
    expect(reads).toEqual([request]);
    expect(result).toEqual({ found: true, seq: 7, bytes });
    if (result.found) expect(result.bytes).toBe(bytes); // Type/fixture proof only, not a B6 implementation.
  });

  it("leaves ordinary sessions inert", () => {
    expect(sessionRecoveryAdapters({ events: createEventBus() })).toBeUndefined();
  });

  it("composes independently registered providers before startup", () => {
    const pi = { events: createEventBus() }, adapters = providers();
    registerSessionRecoveryAdapters(pi, { inputs: adapters.inputs });
    registerSessionRecoveryAdapters(pi, { effects: adapters.effects });
    registerSessionRecoveryAdapters(pi, { identity: adapters.identity });
    expect(sessionRecoveryAdapters(pi)).toEqual(adapters);
  });

  it("does not invent missing effect or input implementations", () => {
    const pi = { events: createEventBus() };
    const unsubscribe = registerSessionRecoveryAdapters(pi, { identity: providers().identity });
    expect(() => sessionRecoveryAdapters(pi)).toThrow("requires identity, inputs and effects");
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
