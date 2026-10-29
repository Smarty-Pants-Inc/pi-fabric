// Keep reason: data safety / native-receipt trust boundary. Synthetic broker expiry is short;
// receipt persistence and custody use the actual filesystem implementation. Native B2 e2e is separate.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  readLocalHostLeaseProcessProof, TransportHostLease, TransportHostLeaseProofError,
  type TransportHostLeaseOptions, type TransportHostLeaseReceipt, type TransportHostLeaseValue,
  type TransportHostLeasePredecessorSnapshot,
} from "../src/topology/host-leases.js";
import { KvConflictError, type TransportKv, type TransportKvEntry, type TransportKvEvent } from "../src/transport/types.js";

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};

class ExpiringKv implements TransportKv {
  readonly ttlMs = 15_000;
  readonly watching = deferred<void>();
  readonly writes: { revision: number; owner: string }[] = [];
  readonly entries = new Map<string, TransportKvEntry<unknown>>();
  readonly listeners = new Set<(event: TransportKvEvent) => void>();
  sequence = 0;
  beforePut: (() => void) | undefined;
  ack: Promise<void> | undefined;
  onGet: (() => void) | undefined;
  async get<V>(key: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined> {
    signal?.throwIfAborted();
    this.onGet?.();
    return this.entries.get(key) as TransportKvEntry<V> | undefined;
  }
  async put<V>(key: string, owner: string, value: V, revision: number, signal?: AbortSignal): Promise<number> {
    signal?.throwIfAborted();
    this.beforePut?.();
    this.writes.push({ revision, owner });
    if ((this.entries.get(key)?.revision ?? 0) !== revision) throw new KvConflictError(key, revision);
    const entry = { key, machine: key.split(".")[0]!, owner, value, revision: ++this.sequence };
    this.entries.set(key, entry);
    this.emit({ type: "put", entry });
    await this.ack;
    return entry.revision;
  }
  async delete(key: string, revision: number, signal?: AbortSignal): Promise<boolean> {
    signal?.throwIfAborted();
    if (this.entries.get(key)?.revision !== revision) return false;
    this.expire(key, "deleted");
    return true;
  }
  expire(key: string, reason: "deleted" | "expired" = "expired", notify = true): void {
    const entry = this.entries.get(key);
    this.entries.delete(key);
    if (notify) this.emit({ type: "gone", key, machine: key.split(".")[0]!, revision: entry?.revision ?? ++this.sequence, reason });
  }
  emit(event: TransportKvEvent): void { for (const listener of this.listeners) listener(event); }
  async *watch<V>(_prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    const queue: TransportKvEvent[] = [{ type: "ready" }];
    let wake = deferred<void>();
    const notify = (event: TransportKvEvent): void => { queue.push(event); wake.resolve(); };
    const abort = (): void => wake.resolve();
    this.listeners.add(notify);
    signal.addEventListener("abort", abort, { once: true });
    this.watching.resolve();
    try {
      while (!signal.aborted) {
        const event = queue.shift();
        if (event) yield event as TransportKvEvent<V>;
        else { await wake.promise; wake = deferred<void>(); }
      }
    } finally {
      this.listeners.delete(notify);
      signal.removeEventListener("abort", abort);
    }
  }
}

const fixtures: { dir: string; leases: TransportHostLease[]; timers: ReturnType<typeof setTimeout>[] }[] = [];
afterEach(async () => {
  for (const fixture of fixtures.splice(0)) {
    for (const timer of fixture.timers) clearTimeout(timer);
    await Promise.all(fixture.leases.map(lease => lease.release().catch(() => false)));
    fs.rmSync(fixture.dir, { recursive: true, force: true }); // Only this test's mkdtemp tree.
  }
});

const setup = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "portable-host-lease-"));
  const fixture = { dir, leases: [] as TransportHostLease[], timers: [] as ReturnType<typeof setTimeout>[] };
  fixtures.push(fixture);
  const kv = new ExpiringKv();
  const make = (incarnation: string, extra: Partial<TransportHostLeaseOptions> = {}) => {
    const lease = new TransportHostLease({ kv, self: { machine: "portable", hostId: "session:durable" },
      rootId: "root:durable", identityId: "identity:durable", incarnation, localStateDir: dir, ...extra });
    fixture.leases.push(lease);
    return lease;
  };
  const file = () => path.join(dir, fs.readdirSync(dir).find(name => /^transport-host-lease-.*\.json$/.test(name))!);
  const receipt = () => JSON.parse(fs.readFileSync(file(), "utf8")) as TransportHostLeaseReceipt;
  const seed = async () => {
    const old = make("old");
    await old.acquire();
    return { old, previous: receipt(), bytes: fs.readFileSync(file(), "utf8") };
  };
  const later = (operation: () => void, ms = 30) => { fixture.timers.push(setTimeout(operation, ms)); };
  const successor = (validate: (previous: TransportHostLeaseReceipt | undefined, signal: AbortSignal) => Promise<void>,
    extra: Partial<TransportHostLeaseOptions> = {}) => make("new", {
      startupSuccessor: { verifyPredecessor: async (snapshot: TransportHostLeasePredecessorSnapshot, signal: AbortSignal) => {
        expect(snapshot.bytes).toBe(snapshot.receipt === undefined ? undefined : fs.readFileSync(file(), "utf8"));
        await validate(snapshot.receipt, signal);
      } }, ...extra });
  const waitForWatch = async (acquiring: Promise<unknown>) => {
    await Promise.race([kv.watching.promise, acquiring.then(() => { throw new Error("admitted before watch"); })]);
  };
  return { dir, kv, make, file, receipt, seed, later, successor, waitForWatch };
};

// No process is killed here: B2's native validator is supplied by composition, not invented here.
// Expected skip off Linux: even the new writer's native process identity is currently /proc-only.
describe.skipIf(process.platform !== "linux")("portable startup successor", () => {
  it("validates explicit receipt absence asynchronously before reservation or KV admission", async () => {
    const s = setup(), entered = deferred<void>(), allow = deferred<void>();
    const next = s.successor(async (previous, signal) => {
      expect(previous).toBeUndefined(); expect(signal.aborted).toBe(false);
      entered.resolve(); await allow.promise;
    });
    const pending = next.acquire();
    await Promise.race([entered.promise, pending.then(() => { throw new Error("validator was skipped"); })]);
    expect(s.kv.entries.size).toBe(0);
    expect(fs.readdirSync(s.dir).filter(name => name.endsWith(".json"))).toEqual([]);
    allow.resolve();
    await pending;
    expect(next.live()).toBe(true);
    expect(s.receipt()).toMatchObject({ incarnation: "new", releasedCleanly: false });
  });

  it.each([false, true])("validator refusal preserves exact native receipt (clean=%s) and KV", async clean => {
    const s = setup(), { old, previous } = await s.seed();
    // A different boot must never substitute for the higher-level handoff validator.
    const remote = { ...previous, bootId: "00000000-0000-4000-8000-000000000001", releasedCleanly: clean };
    fs.writeFileSync(s.file(), JSON.stringify(remote));
    const bytes = fs.readFileSync(s.file(), "utf8"), entry = await s.kv.get(old.key);
    const denied = new Error("B2 native exclusion unresolved");
    const next = s.successor(async observed => { expect(observed).toEqual(remote); throw denied; });
    await expect(next.acquire()).rejects.toBe(denied);
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect(await s.kv.get(old.key)).toEqual(entry);
    expect(s.kv.writes).toHaveLength(1);
    expect(next.live()).toBe(false);
    expect(() => next.acquire()).toThrow();
  });

  it("passes original noncanonical UTF-8 bytes and parsed receipt under actual custody", async () => {
    const s = setup(), { old, previous } = await s.seed(); s.kv.expire(old.key);
    const bytes = `\n ${JSON.stringify(previous, null, 2)}\n`;
    fs.writeFileSync(s.file(), bytes);
    const next = s.make("new", { startupSuccessor: { verifyPredecessor: async snapshot => {
      expect(snapshot).toEqual({ receipt: previous, bytes });
      expect(Object.isFrozen(snapshot)).toBe(true);
      expect(Object.isFrozen(snapshot.receipt)).toBe(true);
      expect(fs.existsSync(`${s.file()}.custody/custody.lock/owner`)).toBe(true);
    } } });
    await next.acquire();
    expect(next.live()).toBe(true);
  });

  it("rejects a byte-only receipt change during validation, not canonical JSON equality", async () => {
    const s = setup(), { old, previous } = await s.seed(); s.kv.expire(old.key);
    const changed = JSON.stringify(previous, null, 2);
    const next = s.successor(async () => { fs.writeFileSync(s.file(), changed); });
    await expect(next.acquire()).rejects.toMatchObject({ reason: "invalid" });
    expect(fs.readFileSync(s.file(), "utf8")).toBe(changed);
    expect(s.kv.writes).toHaveLength(1);
    expect(s.kv.entries.size).toBe(0);
  });

  it("rejects byte-only receipt replacement during wait and retains those bytes", async () => {
    const s = setup(), { old, previous } = await s.seed();
    const next = s.successor(async () => {}), pending = next.acquire();
    const rejected = expect(pending).rejects.toMatchObject({ reason: "conflict" });
    await s.waitForWatch(pending);
    const changed = JSON.stringify(previous, null, 2);
    fs.writeFileSync(s.file(), changed);
    s.kv.expire(old.key);
    await rejected;
    expect(fs.readFileSync(s.file(), "utf8")).toBe(changed);
    expect(s.kv.writes).toHaveLength(1);
  });

  it("refuses REQUIRED receipt absence rather than treating it as a fresh session", async () => {
    const s = setup(), denied = new Error("REQUIRED receipt missing");
    const next = s.successor(async previous => { expect(previous).toBeUndefined(); throw denied; });
    await expect(next.acquire()).rejects.toBe(denied);
    expect(s.kv.entries.size).toBe(0);
    expect(fs.readdirSync(s.dir).filter(name => name.endsWith(".json"))).toEqual([]);
    expect(s.kv.writes).toHaveLength(0);
  });

  it("waits for actual short KV expiry, then reserves under durable custody before one create-only write", async () => {
    const s = setup(), { old, previous, bytes } = await s.seed();
    const seen: (TransportHostLeaseReceipt | undefined)[] = [];
    const next = s.successor(async observed => { seen.push(observed); });
    const pending = next.acquire();
    await s.waitForWatch(pending);
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect(next.live()).toBe(false);
    s.kv.beforePut = () => {
      expect(s.receipt()).toMatchObject({ incarnation: "new", releasedCleanly: false });
      expect(fs.existsSync(`${s.file()}.custody/custody.lock/owner`)).toBe(true);
      expect(s.kv.entries.has(next.key)).toBe(false);
    };
    s.later(() => s.kv.expire(old.key));
    await pending;
    expect(seen[0]).toEqual(previous);
    expect(next.live()).toBe(true);
    expect(s.kv.writes).toEqual([{ revision: 0, owner: old.owner }, { revision: 0, owner: next.owner }]);
    expect((await s.kv.get(next.key))?.owner).toBe(next.owner);
    expect(s.kv.listeners.size).toBe(0);
  });

  it("allows a clean predecessor release during the wait, validating the changed exact receipt", async () => {
    const s = setup(), { old } = await s.seed();
    const seen: (TransportHostLeaseReceipt | undefined)[] = [];
    const next = s.successor(async previous => { seen.push(previous); });
    const pending = next.acquire();
    await s.waitForWatch(pending);
    expect(await old.release()).toBe(true);
    await pending;
    expect(seen.some(previous => previous?.releasedCleanly)).toBe(true);
    expect((await s.kv.get(next.key))?.owner).toBe(next.owner);
    expect(s.kv.writes).toHaveLength(2);
  });

  it.each(["owner", "root", "identity", "malformed", "process", "revision"] as const)("refuses %s ownership without overwriting KV or receipt", async field => {
    const s = setup(), { old, bytes } = await s.seed();
    const current = s.kv.entries.get(old.key)!;
    const value = { ...(current.value as TransportHostLeaseValue) };
    const entry = { ...current, value: field === "malformed" ? null : value };
    if (field === "owner") entry.owner = "foreign#incarnation";
    if (field === "root") value.rootId = "root:foreign";
    if (field === "identity") value.identityId = "identity:foreign";
    if (field === "process") Object.assign(value, { bootId: "00000000-0000-4000-8000-000000000001" });
    if (field === "revision") entry.revision = 0;
    s.kv.entries.set(old.key, entry);
    const next = s.successor(async () => {});
    await expect(next.acquire()).rejects.toMatchObject({ reason: "conflict" });
    expect(s.kv.entries.get(old.key)).toEqual(entry);
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect(s.kv.writes).toHaveLength(1);
  });

  it("refuses a foreign replacement arriving while waiting for the exact predecessor", async () => {
    const s = setup(), { old, bytes } = await s.seed();
    const next = s.successor(async () => {}), pending = next.acquire();
    const rejected = expect(pending).rejects.toMatchObject({ reason: "conflict" });
    await s.waitForWatch(pending);
    const foreign = { ...s.kv.entries.get(old.key)!, owner: "foreign#owner", revision: 71 };
    s.kv.entries.set(old.key, foreign);
    s.kv.emit({ type: "put", entry: foreign });
    await rejected;
    expect(s.kv.entries.get(old.key)).toEqual(foreign);
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect(s.kv.writes).toHaveLength(1);
  });

  it("never treats unrelated watch events or a false gone event as authoritative absence", async () => {
    const s = setup(), { old, bytes } = await s.seed();
    const stop = new AbortController();
    const next = s.successor(async () => {}), pending = next.acquire(stop.signal);
    const rejected = expect(pending).rejects.toThrow();
    await s.waitForWatch(pending);
    s.kv.emit({ type: "gone", key: `${old.key}-other`, machine: "portable", revision: 4, reason: "expired" });
    s.kv.emit({ type: "gone", key: old.key, machine: "portable", revision: 4, reason: "expired" });
    s.later(() => stop.abort(new Error("finished negative observation")));
    await rejected;
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect((await s.kv.get(old.key))?.owner).toBe(old.owner);
    expect(s.kv.writes).toHaveLength(1);
    expect(s.kv.listeners.size).toBe(0);
    expect(next.signal.aborted).toBe(true);
  });

  it("refuses a renewing exact old owner rather than extending the startup wait or overwriting it", async () => {
    const s = setup(), { old, bytes } = await s.seed();
    const next = s.successor(async () => {}), pending = next.acquire();
    const rejected = expect(pending).rejects.toMatchObject({ reason: "conflict" });
    await s.waitForWatch(pending);
    await old.renew();
    await rejected;
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect((await s.kv.get(old.key))?.owner).toBe(old.owner);
    expect(s.kv.writes.every(write => write.owner === old.owner)).toBe(true);
  });

  it("bounds a live predecessor wait at TTL+safety with no overwrite", async () => {
    const s = setup(), { old, bytes } = await s.seed();
    const next = s.successor(async () => {});
    await expect(next.acquire()).rejects.toMatchObject({ reason: "timeout" });
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect((await s.kv.get(old.key))?.owner).toBe(old.owner);
    expect(s.kv.writes).toHaveLength(1);
    expect(next.signal.aborted).toBe(true);
    expect(s.kv.listeners.size).toBe(0);
  }, 20_000);

  it("uses the one TTL safety-net reread when the expiry watch event is missing", async () => {
    const s = setup(), { old } = await s.seed();
    const next = s.successor(async () => {}), pending = next.acquire();
    await s.waitForWatch(pending);
    s.later(() => s.kv.expire(old.key, "expired", false));
    await pending;
    expect(next.live()).toBe(true);
    expect(s.kv.writes).toHaveLength(2);
  }, 18_000);

  it.each(["abort", "release"] as const)("%s during wait preserves predecessor and closes acquisition", async action => {
    const s = setup(), { old, bytes } = await s.seed(), stop = new AbortController();
    const next = s.successor(async () => {}), pending = next.acquire(stop.signal);
    const rejected = expect(pending).rejects.toThrow();
    await s.waitForWatch(pending);
    if (action === "abort") stop.abort(new Error("stop startup"));
    else expect(await next.release()).toBe(false);
    await rejected;
    expect(next.live()).toBe(false);
    expect(next.signal.aborted).toBe(true);
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect((await s.kv.get(old.key))?.owner).toBe(old.owner);
    expect(s.kv.writes).toHaveLength(1);
    expect(s.kv.listeners.size).toBe(0);
  });

  it("abort bounds a stalled validator, with no later receipt write", async () => {
    const s = setup(), entered = deferred<void>(), allow = deferred<void>(), stop = new AbortController();
    const next = s.successor(async () => { entered.resolve(); await allow.promise; });
    const pending = next.acquire(stop.signal), rejected = expect(pending).rejects.toThrow("cancel validation");
    await Promise.race([entered.promise, pending]);
    stop.abort(new Error("cancel validation"));
    await rejected;
    allow.resolve();
    await Promise.resolve();
    expect(s.kv.entries.size).toBe(0);
    expect(fs.readdirSync(s.dir).filter(name => name.endsWith(".json"))).toEqual([]);
  });

  it("serializes competing successors and never lets the loser replace the winning reservation", async () => {
    const s = setup(), { old } = await s.seed();
    const validate = async () => {};
    const first = s.successor(validate), second = s.make("other", { startupSuccessor: { verifyPredecessor: validate } });
    const p1 = first.acquire(), p2 = second.acquire();
    const results = Promise.allSettled([p1, p2]);
    await s.waitForWatch(p1);
    s.later(() => s.kv.expire(old.key));
    const settled = await results;
    expect(settled.filter(result => result.status === "fulfilled")).toHaveLength(1);
    const winner = first.live() ? first : second;
    expect(s.receipt().incarnation).toBe(winner === first ? "new" : "other");
    expect((await s.kv.get(old.key))?.owner).toBe(winner.owner);
    expect(s.kv.writes).toHaveLength(2);
    const loser = winner === first ? second : first;
    expect(await loser.release()).toBe(false);
    expect((await s.kv.get(old.key))?.owner).toBe(winner.owner);
  });

  it("a second stopped startup during wait leaves the old exact receipt for a fresh acquisition object", async () => {
    const s = setup(), { old, previous } = await s.seed(), stop = new AbortController();
    const first = s.successor(async () => {}), pending = first.acquire(stop.signal);
    const rejected = expect(pending).rejects.toThrow();
    await s.waitForWatch(pending);
    stop.abort(new Error("second interruption")); await rejected;
    expect(s.receipt()).toEqual(previous);
    expect(() => first.acquire()).toThrow();
    s.kv.expire(old.key);
    const next = s.make("third", { startupSuccessor: { verifyPredecessor: async observed => { expect(observed.receipt).toEqual(previous); } } });
    await next.acquire();
    expect(s.receipt().incarnation).toBe("third");
    expect(s.kv.writes).toHaveLength(2);
  });

  it("retains a failed durable reservation as the exact predecessor for the next startup", async () => {
    const s = setup(), { old } = await s.seed();
    s.kv.expire(old.key);
    const first = s.successor(async () => {});
    s.kv.beforePut = () => { throw new Error("interrupted after durable reservation"); };
    await expect(first.acquire()).rejects.toThrow("interrupted after durable reservation");
    const reserved = s.receipt();
    expect(reserved).toMatchObject({ incarnation: "new", releasedCleanly: false });
    s.kv.beforePut = undefined;
    const next = s.make("third", { startupSuccessor: { verifyPredecessor: async observed => { expect(observed.receipt).toEqual(reserved); } } });
    await next.acquire();
    expect(s.receipt().incarnation).toBe("third");
    expect(s.kv.writes).toHaveLength(2);
  });

  it("does not reset local admission cutoff at a delayed create PubAck", async () => {
    const s = setup(), { old } = await s.seed();
    s.kv.expire(old.key);
    const ack = deferred<void>();
    let clock = 100;
    const next = s.successor(async () => {}, { monotonic: () => clock });
    s.kv.ack = ack.promise;
    s.kv.beforePut = () => { expect(s.receipt().incarnation).toBe("new"); clock += 13_001; ack.resolve(); };
    await expect(next.acquire()).rejects.toThrow();
    expect(next.live()).toBe(false);
    expect(next.signal.aborted).toBe(true);
    expect(next.revision).toBeGreaterThan(0);
    expect(s.kv.writes).toHaveLength(2);
  });

  it("abort during an uncertain create retains the reservation and cannot admit on a late PubAck", async () => {
    const s = setup(), { old } = await s.seed(); s.kv.expire(old.key);
    const ack = deferred<void>(), published = deferred<void>(), stop = new AbortController();
    const next = s.successor(async () => {});
    s.kv.ack = ack.promise;
    s.kv.beforePut = () => { published.resolve(); };
    const pending = next.acquire(stop.signal), rejected = expect(pending).rejects.toThrow("crash after reservation");
    await Promise.race([published.promise, pending]);
    stop.abort(new Error("crash after reservation"));
    await rejected;
    const reservation = s.receipt();
    expect(reservation).toMatchObject({ incarnation: "new", releasedCleanly: false });
    expect((await s.kv.get(next.key))?.owner).toBe(next.owner);
    ack.resolve();
    await Promise.resolve(); await Promise.resolve();
    expect(next.live()).toBe(false);
    expect(() => next.acquire()).toThrow();
    s.kv.ack = undefined; s.kv.beforePut = undefined;
    const third = s.make("third", { startupSuccessor: { verifyPredecessor: async snapshot => {
      expect(snapshot.receipt).toEqual(reservation);
    } } });
    const acquired = third.acquire();
    // Only native absence, not the cancelled acquisition's object state, permits creation.
    s.later(() => s.kv.expire(next.key));
    await acquired;
    expect(third.live()).toBe(true);
    expect(s.kv.writes).toHaveLength(3);
  });

  it("a competing KV writer between absence and create wins without a CAS takeover or retry", async () => {
    const s = setup(), { old } = await s.seed(); s.kv.expire(old.key);
    const next = s.successor(async () => {});
    const foreign = { key: next.key, machine: "portable", owner: "foreign#incarnation", value: null, revision: 71 };
    s.kv.beforePut = () => { s.kv.entries.set(next.key, foreign); };
    await expect(next.acquire()).rejects.toMatchObject({ reason: "conflict" });
    expect(s.kv.entries.get(next.key)).toEqual(foreign);
    expect(s.kv.writes).toHaveLength(2);
    expect(s.kv.writes[1]?.revision).toBe(0);
    expect(() => next.acquire()).toThrow();
  });

  it("keeps the default local alive-predecessor refusal and single-use acquisition unchanged", async () => {
    const s = setup(), { old, bytes } = await s.seed();
    expect(s.receipt()).toMatchObject({ pid: process.pid, bootId: readLocalHostLeaseProcessProof().bootId });
    const next = s.make("new");
    await expect(next.acquire()).rejects.toEqual(expect.objectContaining<Partial<TransportHostLeaseProofError>>({ reason: "alive" }));
    expect(fs.readFileSync(s.file(), "utf8")).toBe(bytes);
    expect((await s.kv.get(old.key))?.owner).toBe(old.owner);
    expect(s.kv.writes).toHaveLength(1);
    expect(() => next.acquire()).toThrow();
  });
});
