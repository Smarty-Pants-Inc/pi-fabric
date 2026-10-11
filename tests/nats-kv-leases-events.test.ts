import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { KV, KvEntry } from "@nats-io/kv";
import type { JetStreamClient } from "@nats-io/jetstream";
import type { FabricHostLease } from "../src/topology/host-leases.js";
import { LeaseWaitTimeoutError, NatsKvLeaseStore, newLeaseIncarnation, type LeaseSnapshot } from "../src/topology/nats-kv-leases.js";

class Events implements AsyncIterableIterator<KvEntry> {
  private queued: KvEntry[] = [];
  private nextEvent: ((result: IteratorResult<KvEntry>) => void) | undefined;
  stopped = false;
  [Symbol.asyncIterator](): AsyncIterableIterator<KvEntry> { return this; }
  next(): Promise<IteratorResult<KvEntry>> {
    const entry = this.queued.shift();
    if (entry) return Promise.resolve({ done: false, value: entry });
    if (this.stopped) return Promise.resolve({ done: true, value: undefined });
    return new Promise(resolve => { this.nextEvent = resolve; });
  }
  push(): void {
    const value = {} as KvEntry; // Waiting treats entries as hints, never ownership.
    if (this.nextEvent) { this.nextEvent({ done: false, value }); this.nextEvent = undefined; }
    else this.queued.push(value);
  }
  stop(): void { this.stopped = true; this.nextEvent?.({ done: true, value: undefined }); }
}
const construct = (kv: Partial<KV>, js: Partial<JetStreamClient> = {}): NatsKvLeaseStore =>
  new (NatsKvLeaseStore as unknown as { new(kv: KV, js: JetStreamClient, bucket: string,
    maxLeaseMs: number, durability: string): NatsKvLeaseStore })(kv as KV, js as JetStreamClient, "TEST", 1_000, "operator-attested");
const lease = (): FabricHostLease => ({ id: "fake-timer", rootId: "root", identityId: "owner",
  startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 500 });

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe("NATS lease event-only acquisition (short leases ONLY with fake timers)", () => {
  it("has exactly one total failure timer; only KV events trigger retries, renew events never re-arm it", async () => {
    vi.useFakeTimers();
    const events = new Events(), store = construct({ watch: vi.fn().mockResolvedValue(events) });
    const acquire = vi.spyOn(store, "acquire").mockResolvedValue(undefined);
    const waiting = store.acquireWaiting(lease, newLeaseIncarnation(), { waitMs: 1_000 });
    const rejected = expect(waiting).rejects.toBeInstanceOf(LeaseWaitTimeoutError);
    await vi.advanceTimersByTimeAsync(0);
    expect(acquire).toHaveBeenCalledTimes(1); expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(120);
    expect(acquire).toHaveBeenCalledTimes(1); // No short logical-expiry wake.
    events.push(); await vi.advanceTimersByTimeAsync(0);
    expect(acquire).toHaveBeenCalledTimes(2); expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(700);
    events.push(); await vi.advanceTimersByTimeAsync(0);
    expect(acquire).toHaveBeenCalledTimes(3); expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(180); await rejected;
    expect(acquire).toHaveBeenCalledTimes(3); expect(events.stopped).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("aborts a waiting watch and reaps its event pump without an expiry timer", async () => {
    vi.useFakeTimers();
    const events = new Events(), store = construct({ watch: vi.fn().mockResolvedValue(events) });
    vi.spyOn(store, "acquire").mockResolvedValue(undefined);
    const controller = new AbortController();
    const waiting = store.acquireWaiting(lease, newLeaseIncarnation(), { waitMs: 1_000, signal: controller.signal });
    const rejected = expect(waiting).rejects.toThrow("cancelled");
    await vi.advanceTimersByTimeAsync(0); controller.abort(new Error("cancelled")); await rejected;
    expect(events.stopped).toBe(true); expect(vi.getTimerCount()).toBe(0);
  });
  it("publishes relative per-message TTL for acquire and CAS renewal, not max_age or a client timer", async () => {
    vi.useFakeTimers(); vi.setSystemTime(10_000);
    const incarnation = newLeaseIncarnation(), proposal = lease();
    const create = vi.fn().mockResolvedValue(1), update = vi.fn();
    const get = vi.fn().mockResolvedValue(null), publish = vi.fn().mockResolvedValue({ seq: 2 });
    const store = construct({ get, create, update }, { publish });
    const owned = (await store.acquire(proposal, incarnation))!;
    expect(create.mock.calls[0]?.[2]).toBe("1s");
    await vi.advanceTimersByTimeAsync(100);
    get.mockResolvedValue({ operation: "PUT", key: createHash("sha256").update(proposal.id).digest("hex"),
      revision: 1, json: () => ({ format: 1, lease: proposal, incarnation }) });
    const renewed: LeaseSnapshot = await store.renew(owned, lease());
    expect(publish).toHaveBeenCalledWith(expect.stringContaining("$KV.TEST."), expect.any(String),
      { expect: { lastSubjectSequence: 1 }, ttl: "1s", retries: 1 });
    expect(update).not.toHaveBeenCalled(); expect(renewed.revision).toBe(2);
    expect(vi.getTimerCount()).toBe(0);
  });
});
