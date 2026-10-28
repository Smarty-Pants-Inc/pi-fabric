// Keep reason: data safety. Conflicting recovery providers must not silently choose a writer.
import { createEventBus } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import {
  registerSessionRecoveryAdapters, sessionRecoveryAdapters,
  type SessionRecoveryAdapters,
} from "../src/recovery/l7-interfaces.js";

function providers(): SessionRecoveryAdapters {
  return {
    identity: { validate: async () => { throw new Error("not used"); }, verifyPredecessor: async () => {} },
    inputs: { readRecoveryInput: async () => { throw new Error("not used"); }, admitContinuation: async () => { throw new Error("not used"); } },
    effects: { settlePendingIntents: async () => ({ settled: [], uncertain: 0 }), readToolResult: async () => { throw new Error("not used"); } },
  };
}

describe("session recovery provider registration", () => {
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
