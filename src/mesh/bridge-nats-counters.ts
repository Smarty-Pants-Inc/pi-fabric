/**
 * The NATS link's crossing counters (smarty-dev#15310, A' lane L5, PR 2): how many events each leg carried or
 * refused, for `nats-link status`. The running link counts in memory and writes one small snapshot,
 * `<stateDir>/counters.json` (0600), at most once a second and on close; `status` reads it from another
 * process. Counters are cumulative across restarts (a start continues from the last snapshot) and are an
 * operator signal only: exactly-once and authority live in the ledger and the gates, never here.
 *
 * Names are fixed: a leg counts with `add(name)`, and an unknown name is a programming error.
 */
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";

export const NATS_LINK_COUNTERS_FILE = "counters.json";

/**
 * - control.*: PR 1 control commands and ACKs (toNats = file -> NATS, toFile = NATS -> file).
 * - topic.toNats: hub fleet.work / ops.owner events delivered to a NATS root as a keyed followUp (N3).
 * - republish.*: NATS-published fleet.work events republished into the file mesh (N4): accepted (appended),
 *   duplicate (already republished: the ledger or stream sequence said so), refused (gate or sender check).
 * - relay.*: commands and ACKs between a spoke's root and a NATS root through the hub relay.
 */
export const NATS_LINK_COUNTER_NAMES = [
  "control.toNats", "control.toFile", "control.refused",
  "topic.toNats", "topic.refused",
  "republish.accepted", "republish.duplicate", "republish.refused",
  "relay.toNats", "relay.toFile", "relay.refused",
] as const;
export type NatsLinkCounterName = typeof NATS_LINK_COUNTER_NAMES[number];

export interface NatsLinkCountersSnapshot {
  readonly v: 1;
  readonly link: string;
  /** The process that wrote this snapshot. */
  readonly pid: number;
  /** When counting began (the first start whose snapshot this continues). */
  readonly since: string;
  /** When this process started counting. */
  readonly startedAt: string;
  readonly updatedAt: string;
  readonly counters: Readonly<Record<NatsLinkCounterName, number>>;
  /** When each counter last moved. */
  readonly last: Readonly<Partial<Record<NatsLinkCounterName, string>>>;
}

const zero = (): Record<NatsLinkCounterName, number> =>
  Object.fromEntries(NATS_LINK_COUNTER_NAMES.map((name) => [name, 0])) as Record<NatsLinkCounterName, number>;

const isIso = (value: unknown): value is string => typeof value === "string" && Number.isFinite(Date.parse(value));

/** The last snapshot in `stateDir`, or undefined when there is none or it is not a valid snapshot. */
export const readNatsLinkCounters = (stateDir: string): NatsLinkCountersSnapshot | undefined => {
  let value: unknown;
  try { value = JSON.parse(fs.readFileSync(path.join(stateDir, NATS_LINK_COUNTERS_FILE), "utf8")); } catch { return undefined; }
  if (typeof value !== "object" || value === null) return undefined;
  const snapshot = value as Partial<NatsLinkCountersSnapshot>;
  if (snapshot.v !== 1 || typeof snapshot.link !== "string" || !Number.isSafeInteger(snapshot.pid) ||
    !isIso(snapshot.since) || !isIso(snapshot.startedAt) || !isIso(snapshot.updatedAt) ||
    typeof snapshot.counters !== "object" || snapshot.counters === null) return undefined;
  const counters = zero();
  for (const name of NATS_LINK_COUNTER_NAMES) {
    const count = (snapshot.counters as Record<string, unknown>)[name];
    if (count === undefined) continue;
    if (!Number.isSafeInteger(count) || (count as number) < 0) return undefined;
    counters[name] = count as number;
  }
  const last: Partial<Record<NatsLinkCounterName, string>> = {};
  for (const name of NATS_LINK_COUNTER_NAMES) {
    const at = (snapshot.last as Record<string, unknown> | undefined)?.[name];
    if (isIso(at)) last[name] = at;
  }
  return { v: 1, link: snapshot.link, pid: snapshot.pid!, since: snapshot.since!, startedAt: snapshot.startedAt!,
    updatedAt: snapshot.updatedAt!, counters, last };
};

export interface NatsLinkCountersOptions {
  /** Longest a counted event waits before the snapshot is written (default 1 s). */
  readonly flushMs?: number;
  readonly now?: () => number;
}

/** The running link's counters. One instance per link process; `close()` writes the final snapshot. */
export class NatsLinkCounters {
  readonly #file: string;
  readonly #link: string;
  readonly #flushMs: number;
  readonly #now: () => number;
  readonly #counters: Record<NatsLinkCounterName, number>;
  readonly #last: Partial<Record<NatsLinkCounterName, string>>;
  readonly #since: string;
  readonly #startedAt: string;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;

  constructor(readonly stateDir: string, link: string, options: NatsLinkCountersOptions = {}) {
    this.#file = path.join(stateDir, NATS_LINK_COUNTERS_FILE);
    this.#link = link;
    this.#flushMs = options.flushMs ?? 1_000;
    this.#now = options.now ?? Date.now;
    // Continue a snapshot of this link; a snapshot of another link (a reused stateDir) starts over.
    const previous = readNatsLinkCounters(stateDir);
    const carried = previous?.link === link ? previous : undefined;
    this.#counters = { ...zero(), ...carried?.counters };
    this.#last = { ...carried?.last };
    this.#startedAt = new Date(this.#now()).toISOString();
    this.#since = carried?.since ?? this.#startedAt;
  }

  add(name: NatsLinkCounterName, by = 1): void {
    if (!(NATS_LINK_COUNTER_NAMES as readonly string[]).includes(name)) throw new TypeError(`unknown nats-link counter ${String(name)}`);
    if (!Number.isSafeInteger(by) || by < 0) throw new TypeError("a counter moves by a non-negative integer");
    if (this.#closed || by === 0) return;
    this.#counters[name] += by;
    this.#last[name] = new Date(this.#now()).toISOString();
    if (this.#timer === undefined) {
      this.#timer = setTimeout(() => { this.#timer = undefined; this.flush(); }, this.#flushMs);
      this.#timer.unref?.();
    }
  }

  get(name: NatsLinkCounterName): number { return this.#counters[name]; }

  snapshot(): NatsLinkCountersSnapshot {
    return { v: 1, link: this.#link, pid: process.pid, since: this.#since, startedAt: this.#startedAt,
      updatedAt: new Date(this.#now()).toISOString(), counters: { ...this.#counters }, last: { ...this.#last } };
  }

  /** Write the snapshot now. A failed write is reported to the caller; counting goes on. */
  flush(): void {
    if (this.#timer !== undefined) { clearTimeout(this.#timer); this.#timer = undefined; }
    writeJsonAtomic(this.#file, this.snapshot(), { mode: 0o600 });
  }

  /** Final snapshot; later add() calls are ignored. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.flush();
  }
}

/** Open the counters for a link process (writes the first snapshot at once, so status sees the start). */
export const openNatsLinkCounters = (stateDir: string, link: string, options: NatsLinkCountersOptions = {}): NatsLinkCounters => {
  const counters = new NatsLinkCounters(stateDir, link, options);
  counters.flush();
  return counters;
};
