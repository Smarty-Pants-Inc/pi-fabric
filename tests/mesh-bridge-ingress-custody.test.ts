import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG, MeshArchive } from "../src/mesh/archive.js";
import { MeshBridge, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// KEEP: data safety and a trust boundary. Exercise actual held ingress,
// canonical lock custody, receipt bytes and SQLite refusals. Hooks only arrange
// contention at real filesystem/driver boundaries; none supplies an append result.
const BUDGET = 400;
const BATCH_PREFIX_IO_DELAY_MS = 60;
const cleanups: Array<() => void | Promise<unknown>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
const key = (kind: string, id: string) => `topology/${kind}/${hash(id)}`;
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
const within = async <T>(work: Promise<T>, label: string, ms = 3_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`custody fixture deadline: ${label}`)), ms);
    })]);
  } finally { clearTimeout(timer); }
};
const outcome = <T>(work: Promise<T>) => work.then(
  value => ({ ok: true as const, value }), error => ({ ok: false as const, error: error as Error & { code?: string } }));
const livePath = (root: string) => path.join(root, "events.jsonl");
const liveText = (root: string) => fs.existsSync(livePath(root)) ? fs.readFileSync(livePath(root), "utf8") : "";
const liveEvents = (root: string): MeshEvent[] => liveText(root).trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
const receiptPath = (root: string, dedupeKey: string, suffix = ".json") =>
  path.join(root, "event-receipts", hash(dedupeKey) + suffix);
const tickets = (root: string): string[] => {
  try { return fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort(); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
// Normal wake source: fs.watch of the real admission directory. One finite timeout
// bounds a lost notification; there is no load generator or recurring poll timer.
const queuedBehindHolder = async (root: string) => {
  const changed = deferred<void>();
  const check = () => { if (tickets(root).length >= 2) changed.resolve(); };
  const watcher = fs.watch(meshLockQueueDirectory(root), check);
  try { check(); await within(changed.promise, "initial ingress queued behind canonical holder"); }
  finally { watcher.close(); }
};
const native = async (mesh: MeshStore, id: string) => {
  const now = Date.now();
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: id };
  const host = { format: 1, id, rootId: id, identity, incarnationToken: `native:${id}`,
    startedAt: now, updatedAt: now, expiresAt: now + 120_000 };
  await mesh.put({ key: key("hosts", id), identity, value: host });
  writeHostLease(mesh.root, { id, rootId: id, identityId: id, incarnationToken: host.incarnationToken,
    startedAt: now, updatedAt: now, expiresAt: host.expiresAt });
  writeParticipantFile(mesh.root, { key: key("participants", id), version: 1, updatedAt: now, updatedBy: identity,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: id,
      label: id, status: "idle", runner: "pi", transport: "host", capabilities: ["steer"],
      startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return identity;
};
const fixture = async (archived = false) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-ingress-custody-"));
  cleanups.push(() => { fs.rmSync(base, { recursive: true, force: true }); });
  const root = path.join(base, "hub"), archiveDir = path.join(root, "archive");
  if (archived) {
    fs.mkdirSync(archiveDir, { recursive: true });
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archiveDir }));
  }
  const hub = new MeshStore(root, 65_536, 100, { stateBackend: "sqlite", lockTimeoutMs: BUDGET });
  const far = new MeshStore(path.join(base, "far"), 65_536, 100, { stateBackend: "file" });
  cleanups.push(() => { hub.closeState(); far.closeState(); });
  const sender = await native(far, "session:custody-far");
  const side = new StoreBridgeSide(hub, "far"), remote = new StoreBridgeSide(far, "hub");
  const bridge = new MeshBridge({ local: side, remote, localName: "hub", remoteName: "far",
    cursorPath: path.join(base, "cursor.json"), stopMs: 40 });
  cleanups.push(() => bridge.stop());
  await bridge.start();
  expect(side.holds(sender.id, sender)).toBe(true);
  const event = (id: string): BridgePublish => ({ topic: "fabric.control.ack", kind: "ack", from: sender,
    to: "session:custody-hub", text: id, data: { commandId: id, bridge: { from: "far", id } } });
  const publish = (events: BridgePublish[], batch: boolean) => batch
    ? side.publishBatch(events.map(event => ({ event, held: [sender.id] })))
    : side.publish(events[0]!, [sender.id]).then(result => [result]);
  return { root, hub, far, side, bridge, sender, event, publish, archiveDir };
};
// A separate real MeshLock owns .lock across an explicitly bounded test gate. The
// product under test still uses only synchronous sections. No owner files are forged.
const canonicalHolder = (root: string) => {
  const entered = deferred<void>(), released = deferred<void>();
  let held = false;
  const work = new MeshLock(root, { lockTimeoutMs: 2_000 }, () => undefined).withLockAcrossAwait(async () => {
    held = true; entered.resolve();
    try { await released.promise; } finally { held = false; }
  });
  const release = () => released.resolve();
  cleanups.push(async () => { release(); await work; });
  return { entered: entered.promise, work, release, held: () => held };
};
const sameFile = (fd: number, file: string) => {
  try { const a = fs.fstatSync(fd), b = fs.statSync(file); return a.isFile() && a.dev === b.dev && a.ino === b.ino; }
  catch { return false; }
};

// Mandatory lead gate: post-append CAS must not inherit try-zero OR the retained
// initial admission. The second variant also distinguishes ordinary B from 2B:
// its blocker releases after B but before 2B, so an extended CAS would wrongly pass.
describe("held keyed ingress: ordinary post-append receipt custody", () => {
  const inputDedupeKey = (input: BridgePublish) =>
    `bridge-publish:${hash(JSON.stringify(["far", input.data.bridge.id]))}`;
  const assertIdentity = (events: MeshEvent[], inputs: BridgePublish[]) => {
    expect(events.map(event => event.dedupeKey)).toEqual(inputs.map(inputDedupeKey));
    expect(events.map(event => event.text)).toEqual(inputs.map(input => input.text));
    expect(events.map(event => (event.data as BridgePublish["data"]).bridge.id))
      .toEqual(inputs.map(input => input.data.bridge.id));
  };
  const pendingReceiptNames = (root: string) => fs.readdirSync(path.join(root, "event-receipts"))
    .filter(name => /^[a-f0-9]{64}\.pending\.json$/.test(name)).sort();
  const finalReceiptNames = (root: string) => fs.readdirSync(path.join(root, "event-receipts"))
    .filter(name => /^[a-f0-9]{64}\.json$/.test(name)).sort();

  for (const batch of [false, true]) {
    it.each([false, true])(`${batch ? "batch" : "single"}: receipt finalization uses a fresh nonzero ordinary budget (exhaust=%s)`, async exhaust => {
      const f = await fixture(), inputs = [f.event("receipt-a"), ...(batch ? [f.event("receipt-b")] : [])];
      const barrier = deferred<void>();
      let initial: string[] = [], atBarrier: string[] = [], holder: ReturnType<typeof canonicalHolder> | undefined;
      let firstBatchAppendDelayMs: number | undefined;
      const append = fs.appendFileSync.bind(fs);
      vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
        append(file, data, options);
        if (file === livePath(f.root)) {
          initial = tickets(f.root);
          if (batch && firstBatchAppendDelayMs === undefined) {
            // Simulate one actual slow first append after its bytes are written. The existing
            // between-event 50 ms work bound must return that durable prefix before the suffix.
            const began = performance.now();
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, BATCH_PREFIX_IO_DELAY_MS);
            firstBatchAppendDelayMs = performance.now() - began;
          }
        }
      });
      const sync = fs.fsyncSync.bind(fs);
      vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
        sync(fd);
        if (!holder && sameFile(fd, livePath(f.root))) {
          atBarrier = tickets(f.root);
          holder = canonicalHolder(f.root); // Acquires the now-free real canonical lock.
          barrier.resolve();
        }
      });
      let settled = false;
      const publishing = outcome(f.publish(inputs, batch)).then(result => { settled = true; return result; });
      let releaseTimer: ReturnType<typeof setTimeout> | undefined;
      try {
        await within(barrier.promise, "real off-lock live fsync");
        await within(holder!.entered, "receipt blocker owns canonical lock");
        await turn();
        expect(initial.length, "observed actual initial admission at the append").toBeGreaterThan(0);
        expect(atBarrier.filter(name => initial.includes(name)), "initial admission retired before receipt work").toEqual([]);
        expect(settled, "receipt CAS cannot fail immediately as try-zero").toBe(false);
        const receiptTickets = tickets(f.root);
        expect(receiptTickets.length, "ordinary receipt acquisition queues behind the real holder").toBeGreaterThanOrEqual(2);
        expect(receiptTickets.some(name => initial.includes(name)), "never borrow retained initial ticket for CAS").toBe(false);

        const committed = liveEvents(f.root), bytes = liveText(f.root), prefix = inputs.slice(0, liveEvents(f.root).length);
        expect(committed.length, "the first append batch is a nonempty durable prefix").toBeGreaterThan(0);
        expect(committed.length).toBeLessThanOrEqual(inputs.length);
        if (batch) {
          expect(firstBatchAppendDelayMs).toBeGreaterThanOrEqual(50);
          expect(committed).toHaveLength(1);
        }
        assertIdentity(committed, prefix);
        expect(committed.map(event => event.sequence)).toEqual(
          committed.map((event, index) => committed[0]!.sequence + index));
        expect(f.hub.read({ after: 0 })).toEqual(committed);
        expect(new Set(committed.map(event => event.id)).size).toBe(committed.length);
        for (const [index, event] of committed.entries()) {
          const dedupeKey = inputDedupeKey(prefix[index]!);
          expect(event.dedupeKey).toBeTruthy();
          expect(event.dedupeKey).toBe(dedupeKey);
          expect(fs.existsSync(receiptPath(f.root, dedupeKey))).toBe(false);
          expect(JSON.parse(fs.readFileSync(receiptPath(f.root, dedupeKey, ".pending.json"), "utf8")))
            .toMatchObject({ dedupeKey, eventId: event.id, reservedSequence: event.sequence });
        }
        for (const input of inputs.slice(committed.length)) {
          const dedupeKey = inputDedupeKey(input);
          expect(committed.some(event => event.dedupeKey === dedupeKey)).toBe(false);
          expect(fs.existsSync(receiptPath(f.root, dedupeKey))).toBe(false);
          expect(fs.existsSync(receiptPath(f.root, dedupeKey, ".pending.json"))).toBe(false);
        }
        expect(pendingReceiptNames(f.root)).toEqual(prefix.map(input => `${hash(inputDedupeKey(input))}.pending.json`).sort());
        expect(finalReceiptNames(f.root)).toEqual([]);

        const waitBegan = performance.now();
        releaseTimer = setTimeout(holder!.release, exhaust ? BUDGET + 160 : 100);
        const result = await within(publishing, "ordinary receipt budget outcome");
        const waitedMs = performance.now() - waitBegan;
        console.log(JSON.stringify({ gate: "receipt-custody", batch, exhaust, waitedMs,
          budgetMs: BUDGET, committed: committed.length, inputCount: inputs.length, ok: result.ok }));
        if (exhaust) {
          expect(result.ok, "a receipt is NOT allowed the extended initial 2B window").toBe(false);
          if (!result.ok) expect(result.error.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
          expect(waitedMs).toBeGreaterThanOrEqual(BUDGET * .7);
          expect(holder!.held(), "ordinary receipt refusal precedes the blocker's later release").toBe(true);
        } else {
          expect(result.ok, "post-append canonical contention must wait, not use try-zero").toBe(true);
          if (result.ok) {
            expect(result.value).toHaveLength(committed.length);
            expect(result.value.map(receipt => receipt.sequence)).toEqual(committed.map(event => event.sequence));
          }
          expect(waitedMs).toBeGreaterThanOrEqual(70);
        }
        expect(liveText(f.root)).toBe(bytes);
        expect(liveEvents(f.root)).toEqual(committed);
        for (const input of inputs.slice(committed.length)) {
          const dedupeKey = inputDedupeKey(input);
          expect(fs.existsSync(receiptPath(f.root, dedupeKey))).toBe(false);
          expect(fs.existsSync(receiptPath(f.root, dedupeKey, ".pending.json"))).toBe(false);
        }

        holder!.release(); await holder!.work;
        // This explicit caller reconciliation settles only the committed prefix and cannot
        // append a line. The uncommitted suffix is resumed separately below, never by the failed call.
        const reconciled = await within(f.publish(prefix, batch), "explicit committed-prefix reconciliation");
        expect(reconciled).toHaveLength(committed.length);
        expect(reconciled.map(receipt => receipt.sequence)).toEqual(committed.map(event => event.sequence));
        expect(liveText(f.root)).toBe(bytes);
        expect(f.hub.read({ after: 0 })).toEqual(committed);
        for (const event of committed) {
          expect(JSON.parse(fs.readFileSync(receiptPath(f.root, event.dedupeKey!), "utf8"))).toEqual(event);
          expect(fs.existsSync(receiptPath(f.root, event.dedupeKey!, ".pending.json"))).toBe(false);
        }
        expect(pendingReceiptNames(f.root)).toEqual([]);
        expect(finalReceiptNames(f.root)).toEqual(committed.map(event => `${hash(event.dedupeKey!)}.json`).sort());

        let completed = committed.length, batchCalls = 0;
        while (completed < inputs.length) {
          expect(batchCalls).toBeLessThan(inputs.length);
          const remaining = inputs.slice(completed), before = liveEvents(f.root);
          const next = await within(f.publish(remaining, true), "bounded explicit batch suffix publication");
          expect(next.length).toBeGreaterThan(0);
          expect(next.length).toBeLessThanOrEqual(remaining.length);
          const after = liveEvents(f.root), appended = after.slice(completed);
          expect(after.slice(0, completed)).toEqual(before);
          expect(appended).toHaveLength(next.length);
          assertIdentity(appended, remaining.slice(0, next.length));
          expect(next.map(receipt => receipt.sequence)).toEqual(appended.map(event => event.sequence));
          completed += next.length;
          batchCalls++;
        }

        const allEvents = liveEvents(f.root);
        expect(allEvents).toHaveLength(inputs.length);
        assertIdentity(allEvents, inputs);
        expect(allEvents.map(event => event.sequence)).toEqual(
          allEvents.map((event, index) => allEvents[0]!.sequence + index));
        expect(new Set(allEvents.map(event => event.id)).size).toBe(inputs.length);
        expect(new Set(allEvents.map(event => event.dedupeKey)).size).toBe(inputs.length);
        expect(f.hub.read({ after: 0 })).toEqual(allEvents);
        expect(pendingReceiptNames(f.root)).toEqual([]);
        for (const event of allEvents) {
          expect(JSON.parse(fs.readFileSync(receiptPath(f.root, event.dedupeKey!), "utf8"))).toEqual(event);
          expect(fs.existsSync(receiptPath(f.root, event.dedupeKey!, ".pending.json"))).toBe(false);
        }
        expect(finalReceiptNames(f.root)).toEqual(allEvents.map(event => `${hash(event.dedupeKey!)}.json`).sort());
      } finally {
        clearTimeout(releaseTimer); holder?.release(); await publishing;
      }
    });
  }
});
it.each(["native-takeover", "same-owner-renewal"] as const)("rechecks ownership after real initial queue wait: %s", async change => {
  const f = await fixture(), holder = canonicalHolder(f.root);
  await within(holder.entered, "initial canonical holder");
  const publishing = outcome(f.publish([f.event(change)], false));
  try {
    await queuedBehindHolder(f.root);
    const previous = readHostLease(f.root, f.sender.id)!;
    if (change === "native-takeover") {
      await native(f.hub, f.sender.id); // Real SQLite commit + native participant/lease files while .lock is held.
      expect(f.side.holds(f.sender.id, f.sender)).toBe(false);
    } else {
      const now = Date.now(), mirror = previous.mirror!;
      writeHostLease(f.root, { ...previous, updatedAt: mirror.sourceObservedAt + 1, expiresAt: now + 15_000,
        mirror: { ...mirror, observedAt: now, sourceObservedAt: mirror.sourceObservedAt + 1,
          sourceUpdatedAt: mirror.sourceObservedAt + 1, sourceExpiresAt: mirror.sourceExpiresAt + 30_000 } });
      expect(readHostLease(f.root, f.sender.id)?.incarnationToken).toBe(previous.incarnationToken);
      expect(f.side.holds(f.sender.id, f.sender)).toBe(true);
    }
    expect(holder.held()).toBe(true);
    holder.release(); await holder.work;
    const result = await within(publishing, "queued ownership admission");
    if (change === "native-takeover") {
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.error.message).toContain("no longer bound");
      expect(liveText(f.root)).toBe("");
    } else {
      expect(result.ok).toBe(true);
      const [event] = liveEvents(f.root);
      expect(event).toMatchObject({ text: change, verification: "bridge", from: f.sender });
      expect(JSON.parse(fs.readFileSync(receiptPath(f.root, event!.dedupeKey!), "utf8"))).toEqual(event);
      expect(liveEvents(f.root)).toHaveLength(1);
    }
  } finally { holder.release(); await publishing; }
});

for (const stage of ["initial-queue", "sqlite-fence"] as const) {
  it.each(["stop", "withdraw"] as const)(`${stage}: %s prevents any late append after blocker release`, async action => {
    const f = await fixture();
    const holder = stage === "initial-queue" ? canonicalHolder(f.root) : undefined;
    if (holder) await within(holder.entered, "cancellation queue holder");
    const raw = new DatabaseSync(path.join(f.root, "state.db"));
    raw.exec("PRAGMA busy_timeout = 0");
    let sqliteHeld = false;
    const release = () => { holder?.release(); if (sqliteHeld) { raw.exec("ROLLBACK"); sqliteHeld = false; } };
    cleanups.push(() => { release(); raw.close(); });
    const refused = deferred<void>(), exec = DatabaseSync.prototype.exec;
    if (stage === "sqlite-fence") {
      raw.exec("BEGIN IMMEDIATE"); sqliteHeld = true;
      vi.spyOn(DatabaseSync.prototype, "exec").mockImplementation(function (this: DatabaseSync, sql) {
        try { return exec.call(this, sql); }
        catch (error) { if (this !== raw && sql === "BEGIN IMMEDIATE") refused.resolve(); throw error; }
      });
    }
    const publishing = outcome(f.publish([f.event(`${stage}-${action}`)], false));
    try {
      if (holder) await queuedBehindHolder(f.root);
      else await within(refused.promise, "real SQLite BEGIN refusal");
      const cancel = action === "stop" ? f.bridge.stop() : f.side.withdraw();
      const cancelled = outcome(cancel);
      // fence()/withdraw() synchronously revokes admission; only its state cleanup
      // may await SQLite. Release after a check-phase turn, not before cancellation.
      await turn();
      release();
      expect((await within(cancelled, "cancel settlement")).ok).toBe(true);
      const result = await within(publishing, "publication settled after cancellation");
      expect(liveText(f.root), "cancelled ingress must not leave late durable bytes").toBe("");
      expect(result.ok, "cancelled pending publication must refuse").toBe(false);
      expect(fs.existsSync(path.join(f.root, "event-receipts"))
        ? fs.readdirSync(path.join(f.root, "event-receipts")).filter(name => /^[a-f0-9]{64}\.(pending\.)?json$/.test(name)) : []).toEqual([]);
    } finally { release(); await publishing; }
  });
}

it.each([false, true])("does not helper-retry a real archive/pending-intent recovery append followed by SQLite refusal (batch=%s)", async batch => {
  const f = await fixture(true);
  const seed = await f.hub.publish({ topic: "legacy.history", from: f.sender, text: "seed" });
  const restored = await f.hub.publish({ topic: "legacy.history", from: f.sender, text: "acknowledged archived suffix" });
  const archive = new MeshArchive(f.archiveDir, f.root);
  expect(archive.lookup(restored.sequence)).toEqual(restored);
  const input = f.event("recovery-then-busy");
  // Same durable crash image as mesh-control-archive-optional / mesh-store:
  // archive and reservation survive, live suffix is lost, BOOT requires promotion.
  fs.writeFileSync(livePath(f.root), JSON.stringify(seed) + "\n");
  fs.writeFileSync(path.join(f.archiveDir, "BOOT"), "previous-boot");
  const dedupeKey = `bridge-publish:${hash(JSON.stringify(["far", input.data.bridge.id]))}`;
  fs.mkdirSync(path.join(f.root, "event-receipts"), { recursive: true });
  // A positive DIFFERENT archived identity proves this pending reservation absent.
  // Reboot recovery still appends the acknowledged suffix before settling it and
  // before the new event's ownership fence. No fake append-success seam is used.
  fs.writeFileSync(receiptPath(f.root, dedupeKey, ".pending.json"), JSON.stringify({
    dedupeKey, reservedSequence: restored.sequence, eventId: "absent-pending-reservation", liveOffset: 0, archiveDir: f.archiveDir,
  }));
  const raw = new DatabaseSync(path.join(f.root, "state.db"));
  raw.exec("PRAGMA busy_timeout = 0");
  let held = false, recoveredBytes = "", refusedAfterRecovery = false;
  cleanups.push(() => { if (held) raw.exec("ROLLBACK"); raw.close(); });
  const append = fs.appendFileSync.bind(fs);
  vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
    append(file, data, options);
    if (file === livePath(f.root) && String(data).includes(restored.id) && !recoveredBytes) {
      recoveredBytes = liveText(f.root);
      raw.exec("BEGIN IMMEDIATE"); held = true;
    }
  });
  const exec = DatabaseSync.prototype.exec;
  vi.spyOn(DatabaseSync.prototype, "exec").mockImplementation(function (this: DatabaseSync, sql) {
    try { return exec.call(this, sql); }
    catch (error) {
      if (this !== raw && sql === "BEGIN IMMEDIATE" && held && recoveredBytes) {
        refusedAfterRecovery = true;
        // Make the next real attempt immediately writable. An unsafe helper retry
        // would now append the requested event and incorrectly report success.
        exec.call(raw, "ROLLBACK"); held = false;
      }
      throw error;
    }
  });
  const result = await within(outcome(f.publish([input], batch)), "recovery then fence refusal");
  expect(recoveredBytes).toBe(JSON.stringify(seed) + "\n" + JSON.stringify(restored) + "\n");
  expect(refusedAfterRecovery, "real SQLite refusal followed actual recovery append").toBe(true);
  expect(result.ok, "any possible recovery effect irrevocably consumes helper retry").toBe(false);
  if (!result.ok) expect(result.error.code).toBe("FABRIC_MESH_LOCK_TIMEOUT");
  expect(liveText(f.root)).toBe(recoveredBytes);
  expect(f.hub.read({ after: 0 })).toEqual([seed, restored]);
  expect(archive.lookup(restored.sequence)).toEqual(restored);
  expect(f.hub.latestSequence()).toBe(restored.sequence);
  expect(fs.existsSync(receiptPath(f.root, dedupeKey))).toBe(false);
  // The helper must surface the refusal, not consume the caller's retry. A new
  // explicit invocation can append exactly once after the prior effects settle.
  expect((await f.publish([input], batch))).toHaveLength(1);
  const events = liveEvents(f.root);
  expect(events.slice(0, 2)).toEqual([seed, restored]);
  expect(events.filter(event => event.text === input.text)).toHaveLength(1);
  expect(events).toHaveLength(3);
});
