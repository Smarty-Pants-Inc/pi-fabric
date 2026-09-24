import { describe, expect, it, vi } from "vitest";
import { FabricProviderBindings } from "../src/core/provider-bindings.js";
import { bindingStep, type BindingLife, type BindingEvent } from "../src/verified/generated/lifecycle-kernel.js";
import type { FabricProvider } from "../src/protocol.js";

const deferred = () => { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; };
const provider = (): FabricProvider => ({ name: "owned", description: "Owned resource", async list() { return []; }, async describe() { return undefined; }, async invoke() {}, close: vi.fn() });
// General *_open_complete and exact transition laws cover the state space.
// Fixed ABI vectors retain phase tags, unequal counters and real JS immutability.
describe("compiled binding lifecycle", () => {
  it("encodes close reservation and independent blockers without mutating input", () => {
    const life: BindingLife = { $: "Life", phase: { $: "Retiring" }, owner: false, revoked: true, holds: 0n, calls: 0n };
    const before = structuredClone(life);
    const closing = bindingStep(life, { $: "Close" });
    expect(closing).toEqual({ $: "Outcome", next: { ...life, phase: { $: "Closing" } }, command: { $: "StartClose" } });
    expect(bindingStep(closing.next, { $: "Close" })).toEqual({ $: "Outcome", next: closing.next, command: { $: "Denied" } });
    expect(life).toEqual(before);
    const blocked: BindingLife[] = [
      { ...life, owner: true }, { ...life, holds: 4n }, { ...life, calls: 7n },
      { ...life, phase: { $: "Active" } }, { ...life, phase: { $: "Closed" } }, { ...life, phase: { $: "Failed" } },
    ];
    for (const input of blocked) {
      const unchanged = structuredClone(input);
      expect(bindingStep(input, { $: "Close" })).toEqual({ $: "Outcome", next: unchanged, command: { $: "Denied" } });
      expect(input).toEqual(unchanged);
    }
  });

  it("distinguishes admission channels, cleanup authority and counter fields", () => {
    const cases = [
      ["Staged", false, false, true], ["Active", false, false, true],
      ["Retiring", false, false, true], ["Retiring", true, true, true],
      ["Failed", true, true, true], ["Failed", false, false, false],
      ["Active", true, false, false], ["Closing", false, true, false], ["Closed", false, true, false],
    ] as const;
    for (const [phase, revoked, cleanup, accepted] of cases) {
      const life: BindingLife = { $: "Life", phase: { $: phase }, owner: true, revoked, holds: 4n, calls: 7n };
      const before = structuredClone(life);
      for (const [start, end, count, nextCount] of [["Begin", "End", "calls", 8n], ["Retain", "Release", "holds", 5n]] as const) {
        const result = bindingStep(life, { $: start, cleanup });
        expect(result).toEqual({ $: "Outcome", next: accepted ? { ...before, [count]: nextCount } : before, command: { $: accepted ? "Granted" : "Denied" } });
        if (accepted) expect(bindingStep(result.next, { $: end })).toEqual({ $: "Outcome", next: before, command: { $: "Granted" } });
      }
      expect(life).toEqual(before);
    }
  });

  it("preserves record fields across event tags and refuses underflow/terminal reopening", () => {
    const life: BindingLife = { $: "Life", phase: { $: "Staged" }, owner: true, revoked: false, holds: 4n, calls: 7n };
    const cases: Array<{ event: BindingEvent; next: BindingLife }> = [
      { event: { $: "Inspect" }, next: life },
      { event: { $: "Activate" }, next: { ...life, phase: { $: "Active" } } },
      { event: { $: "Retire" }, next: { ...life, phase: { $: "Retiring" } } },
      { event: { $: "DropOwner" }, next: { ...life, owner: false } },
      { event: { $: "Revoke" }, next: { ...life, phase: { $: "Retiring" }, owner: false, revoked: true } },
      { event: { $: "Fail" }, next: { ...life, phase: { $: "Failed" }, owner: false, revoked: true } },
    ];
    for (const { event, next } of cases) expect(bindingStep(life, event)).toEqual({ $: "Outcome", next, command: { $: "Granted" } });
    const empty: BindingLife = { ...life, phase: { $: "Failed" }, holds: 0n, calls: 0n };
    expect(bindingStep(empty, { $: "End" })).toEqual({ $: "Outcome", next: empty, command: { $: "Denied" } });
    expect(bindingStep(empty, { $: "Release" })).toEqual({ $: "Outcome", next: empty, command: { $: "Denied" } });
    for (const phase of ["Closing", "Closed", "Failed"] as const) {
      const terminal: BindingLife = { ...life, phase: { $: phase } };
      expect(bindingStep(terminal, { $: "Activate" })).toEqual({ $: "Outcome", next: terminal, command: { $: "Denied" } });
    }
    const closing: BindingLife = { ...life, phase: { $: "Closing" } };
    const closed = { ...closing, phase: { $: "Closed" } };
    expect(bindingStep(closing, { $: "Complete" })).toEqual({ $: "Outcome", next: closed, command: { $: "Granted" } });
  });
});

describe("production binding ownership", () => {
  it("keeps real operations and exact holds until settlement despite cancellation and repeated release", async () => {
    const bindings = new FabricProviderBindings(); const owned = provider(); const lease = bindings.mount(owned);
    const releaseA = bindings.retain([lease.bindingId]); const releaseB = bindings.retain([lease.bindingId]);
    const gate = deferred(); const pending = bindings.track(lease.bindingId, () => gate.promise);
    bindings.revoke(lease.bindingId);
    expect(() => bindings.beginInvocation(lease.bindingId)).toThrow();
    expect(() => bindings.retain([lease.bindingId])).toThrow();
    await Promise.all([releaseA(), releaseA(), lease.release()]);
    expect(bindings.entries()[0]).toMatchObject({ inFlight: 1, retainers: 1 });
    expect(owned.close).not.toHaveBeenCalled();
    gate.resolve(); await pending;
    await vi.waitFor(() => expect(bindings.entries()[0]?.inFlight).toBe(0));
    expect(owned.close).not.toHaveBeenCalled();
    await releaseB(); expect(owned.close).toHaveBeenCalledOnce();
  });

  it("does not invent lease release during shutdown or trust mutable status counters", async () => {
    const bindings = new FabricProviderBindings(); const owned = provider(); const lease = bindings.mount(owned);
    const release = bindings.retain([lease.bindingId]);
    bindings.binding(lease.bindingId)!.retainers = 0;
    await bindings.close();
    expect(bindings.entries()[0]).toMatchObject({ ownerRetained: false, retainers: 1, inFlight: 0 });
    expect(owned.close).not.toHaveBeenCalled();
    await release(); expect(owned.close).toHaveBeenCalledOnce();
  });

  it("quarantines failures without losing cleanup work or admitting new ordinary work", async () => {
    const bindings = new FabricProviderBindings(); const owned = provider(); const lease = bindings.mount(owned);
    const end = bindings.beginInvocation(lease.bindingId);
    bindings.quarantine(lease.bindingId, new Error("cleanup failed"));
    const finishCleanup = bindings.beginInvocation(lease.bindingId, true);
    expect(() => bindings.beginInvocation(lease.bindingId)).toThrow();
    await end(); await end();
    expect(bindings.entries()[0]).toMatchObject({ inFlight: 1, closeError: "cleanup failed" });
    await finishCleanup(); await lease.release(); await bindings.close();
    expect(bindings.entries()[0]?.inFlight).toBe(0);
    expect(owned.close).not.toHaveBeenCalled();
  });

  it("keeps quarantine when a pending provider close later fulfills", async () => {
    const bindings = new FabricProviderBindings(); const owned = provider(); const gate = deferred(); const entered = deferred();
    owned.close = vi.fn(async () => { entered.resolve(); await gate.promise; });
    const lease = bindings.mount(owned);
    const closing = lease.release(); await entered.promise;
    bindings.quarantine(lease.bindingId, new Error("ownership failure during close"));
    const rejected = expect(closing).rejects.toThrow("ownership failure during close");
    gate.resolve(); await rejected;
    expect(bindings.entries()[0]).toMatchObject({ state: "retiring", closeError: "ownership failure during close" });
    expect(bindings.binding(lease.bindingId)).toBeUndefined();
  });

  it("awaits actual asynchronous catalog finalizers before closing their provider", async () => {
    const bindings = new FabricProviderBindings(); const owned = provider(); const gate = deferred(); const entered = deferred();
    owned.subscribeCatalog = () => async () => { entered.resolve(); await gate.promise; };
    const lease = bindings.mount(owned);
    const closing = lease.release(); await entered.promise;
    expect(owned.close).not.toHaveBeenCalled();
    expect(() => bindings.beginInvocation(lease.bindingId, true)).toThrow();
    gate.resolve(); await closing;
    expect(owned.close).toHaveBeenCalledOnce();
  });

  it("reserves close before provider callbacks and quarantines catalog finalizer failure", async () => {
    const bindings = new FabricProviderBindings(); const owned = provider(); const gate = deferred();
    const lease = bindings.mount(owned);
    owned.close = vi.fn(async () => {
      expect(() => bindings.beginInvocation(lease.bindingId, true)).toThrow();
      bindings.revoke(lease.bindingId);
      await gate.promise;
    });
    const closing = lease.release();
    await vi.waitFor(() => expect(owned.close).toHaveBeenCalledOnce());
    expect(() => bindings.mount(owned, { overwrite: true })).toThrow(/close has begun/);
    gate.resolve(); await closing;
    expect(owned.close).toHaveBeenCalledOnce();
    const bad = provider(); bad.subscribeCatalog = () => () => { throw new Error("unsubscribe failed"); };
    const failed = bindings.mount(bad);
    await expect(failed.release()).rejects.toThrow("unsubscribe failed");
    expect(bindings.entries()[0]?.closeError).toBe("unsubscribe failed");
    expect(bad.close).not.toHaveBeenCalled();
  });
});
