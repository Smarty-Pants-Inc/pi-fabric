import fs from "node:fs";
import type { FileHandle } from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import type { MeshEvent } from "./event-log.js";
import { MESH_ARCHIVE_CONFIG } from "./archive.js";

/** Calls count native attempts (including rejection); bytes count only returned bytes.
 * readFile is one attempt, not also its nested native reads. Parse counters include failures.
 * Popped events/bytes exclude discovery and retained frontier work, which have separate totals.
 */
export interface RecoveryReadCost {
  events: number;
  eventBytes: number;
  maxSliceMs: number;
  archiveReadBytes: number;
  archiveReadCalls: number;
  archiveParseAttempts: number;
  liveReadBytes: number;
  liveReadCalls: number;
  liveParseAttempts: number;
  // Oldest-live/hint probes plus synchronous committed-boundary/horizon event reads.
  probeReadBytes: number;
  probeReadCalls: number;
  probeParseAttempts: number;
  // Archive configuration, seals, abort maps, and synchronous generation metadata.
  metadataReadBytes: number;
  metadataReadCalls: number;
  metadataParseAttempts: number;
  discoveryReadCalls: number;
  // #validate path-stat attempts only, not FileHandle.stat/open or discovery.
  sourceValidationCalls: number;
  // Explicit generation accessor checks; horizon's generation I/O is also in metadata totals.
  generationProbes: number;
  horizonMs: number;
  stages: number;
  skippedRecords: number;
}

/** Internal failure envelope: diagnostics only, never a partial recovery page. */
export class RecoveryReadError extends Error {
  readonly cost: RecoveryReadCost;
  constructor(cause: unknown, cost: RecoveryReadCost) {
    super("Mesh recovery read failed", { cause });
    this.cost = { ...cost };
  }
}

export interface RecoveryHint { sequence: number; offset: number }

/** Access to the existing committed boundary and optional live read accelerators. */
export interface RecoveryReaderAccess {
  root: string;
  maxEventBytes: number;
  // Recovery-only optional attempt sink; ordinary consumers need not supply one.
  horizon(cost?: RecoveryReadCost): number;
  generation(cost?: RecoveryReadCost): number;
  hint(after: number, generation: number, inode: number): RecoveryHint | undefined;
  remember(after: number, generation: number, inode: number, lines: RecoveryHint[], boundary?: RecoveryHint): void;
}

type MaybeAsync<T> = T | Promise<T>;
interface Projection { sequence: number; bytes: number; id?: string }
interface Source {
  file: string;
  kind: "archive" | "live" | "probe";
  order: number;
  quantum: number;
  identity?: { dev: number; ino: number };
  generation?: number;
  requiredEnd: number;
  physical: number;
  observedEnd: number;
  lineStart: number;
  chunk?: Buffer | undefined;
  chunkStart: number;
  chunkOffset: number;
  stopped: boolean;
  lastSequence: number;
  head?: Projection | undefined;
  hintAfter?: number;
  boundary?: RecoveryHint;
  hints: RecoveryHint[];
  // Only the oldest-live probe has a bounded window. Data streams have no archive line cap.
  limit?: number;
}
interface Day {
  streams: Source[];
  heap: Source[];
  pending?: Source | undefined;
  aborted?: Record<string, string> | undefined;
}
interface Stage {
  after: number;
  horizon: number;
  archive?: string;
  days: string[];
  nextDay: number;
  day?: Day | undefined;
  live?: Source;
  phase: "archive" | "live" | "done";
  considered: number;
  lastSequence: number;
}
interface Slot { through: number; stage: Stage }
interface ByteLine { bytes: Buffer | undefined; start: number; end: number; rawBytes: number }

const codeOf = (error: unknown): unknown =>
  typeof error === "object" && error !== null && "code" in error ? error.code : undefined;
const changed = (): Error => new Error("Mesh recovery source changed during read");
const PAGE_POSITIONS = 10_000;
const CHUNK_MAX = 256 * 1024;
const SEAL_HINT_PARSE_MAX_BYTES = 1024 * 1024;

/** One call's work clock. Resolved helper promises do not reset the active-work quantum. */
class Work {
  readonly used = new Set<Source>();
  readonly cost: RecoveryReadCost = {
    events: 0, eventBytes: 0, maxSliceMs: 0,
    archiveReadBytes: 0, archiveReadCalls: 0, archiveParseAttempts: 0,
    liveReadBytes: 0, liveReadCalls: 0, liveParseAttempts: 0,
    probeReadBytes: 0, probeReadCalls: 0, probeParseAttempts: 0,
    metadataReadBytes: 0, metadataReadCalls: 0, metadataParseAttempts: 0, discoveryReadCalls: 0,
    sourceValidationCalls: 0, generationProbes: 0, horizonMs: 0, stages: 0, skippedRecords: 0,
  };
  #started = performance.now();
  #records = 0;
  constructor(readonly signal: AbortSignal) {}
  check(): void { this.signal.throwIfAborted(); }
  finishSlice(): void {
    this.cost.maxSliceMs = Math.max(this.cost.maxSliceMs, performance.now() - this.#started);
  }
  async wait<T>(operation: () => MaybeAsync<T>): Promise<T> {
    this.check();
    const result = await operation();
    this.check();
    return result;
  }
  async io<T>(operation: () => Promise<T>, account?: (result: T) => void): Promise<T> {
    this.check();
    this.finishSlice();
    let result: T;
    try { result = await operation(); }
    finally { this.#started = performance.now(); }
    // A completed native read is work even if cancellation wins before its caller resumes.
    account?.(result);
    this.check();
    return result;
  }
  checkpoint(records = 0): Promise<void> | undefined {
    this.check();
    this.#records += records;
    if (this.#records < 128 && performance.now() - this.#started < 8) return undefined;
    this.#records = 0;
    return this.io(() => new Promise<void>(resolve => setImmediate(resolve)));
  }
  async close(handle: FileHandle): Promise<void> {
    // Cancellation must never bypass descriptor cleanup, even if open/read just completed.
    try { this.check(); }
    finally {
      this.finishSlice();
      await handle.close();
      this.#started = performance.now();
      this.check();
    }
  }
}

/**
 * Recovery only: one serialized, forward continuation owned by one bridge side. No FDs survive
 * a refill. Normal synchronous delivery reads/tails do not use this reader.
 *
 * The maximum synchronous slice is approximately max(8 ms, one line's decode + JSON.parse).
 * The occupancy target covers the 4 MiB production writer maximum, not arbitrarily large
 * historical JSON. Complete archive lines above that envelope remain readable without a cutoff.
 */
export class RecoveryReader {
  #slot: Slot | undefined;
  #queue: Promise<void> = Promise.resolve();
  constructor(
    readonly access: RecoveryReaderAccess,
    readonly project: (event: MeshEvent) => string | undefined,
    readonly signal: AbortSignal,
  ) {
    signal.addEventListener("abort", () => { this.#slot = undefined; }, { once: true });
  }

  read(after: number): Promise<{ ids: string[]; through: number; done: boolean; cost: RecoveryReadCost }> {
    const result = this.#queue.then(() => this.#read(after));
    this.#queue = result.then(() => undefined, () => undefined);
    return result;
  }

  async #read(after: number): Promise<{ ids: string[]; through: number; done: boolean; cost: RecoveryReadCost }> {
    const work = new Work(this.signal);
    try {
      work.check();
      const started = performance.now();
      // The adapter uses latestCursor's last anchor, or complete committed live lines, never
      // the reservation counter. Capture once, before any discovery, and do not chase writes.
      let horizon: number;
      try { horizon = this.access.horizon(work.cost); }
      finally { work.cost.horizonMs = performance.now() - started; }
      const pause = work.checkpoint();
      if (pause) await work.wait(() => pause);
      if (after >= horizon) {
        this.#slot = undefined;
        work.check();
        work.finishSlice();
        return { ids: [], through: after, done: true, cost: work.cost };
      }
      if (this.#slot?.through !== after || this.#slot.stage.horizon > horizon) this.#slot = undefined;
      if (!this.#slot) this.#slot = { through: after, stage: await work.wait(() => this.#openStage(after, horizon, work)) };
      else {
        // Cached heads and unread chunks are accelerators, not authority after replacement.
        const stage = this.#slot.stage;
        for (const source of stage.day?.streams ?? (stage.live ? [stage.live] : [])) {
          await work.wait(() => this.#validate(source, work));
          // Sources missing at discovery may have no identity/native stat to yield on.
          const pause = work.checkpoint(1);
          if (pause) await work.wait(() => pause);
        }
      }
      const ids: string[] = [];
      let through = after;
      while (work.cost.events < PAGE_POSITIONS) {
        const stage = this.#slot.stage;
        const pending = this.#next(stage, work);
        const next = pending instanceof Promise ? await work.wait(() => pending) : pending;
        work.check();
        if (next) {
          work.cost.events++;
          work.cost.eventBytes += next.bytes;
          if (next.id !== undefined) ids.push(next.id);
          through = next.sequence;
        } else {
          // Exhaustion certifies the whole stage, including holes and exact-ID-aborted tails.
          through = stage.horizon;
          if (stage.horizon >= horizon) break;
          this.#slot.stage = await work.wait(() => this.#openStage(stage.horizon, horizon, work));
        }
      }
      // Include just-finished days and all sources whose cached heads/EOF were used this call.
      for (const source of work.used) await work.wait(() => this.#validate(source, work));
      work.check();
      for (const source of work.used) {
        if (source.kind === "live" && source.identity && source.generation !== undefined) {
          this.access.remember(source.hintAfter!, source.generation, source.identity.ino, source.hints, source.boundary);
        }
        const pause = work.checkpoint(1);
        if (pause) await work.wait(() => pause);
      }
      const done = through >= horizon;
      if (done) this.#slot = undefined;
      else this.#slot.through = through;
      work.check();
      work.finishSlice();
      return { ids, through, done, cost: work.cost };
    } catch (error) {
      this.#slot = undefined;
      work.finishSlice();
      throw new RecoveryReadError(error, work.cost);
    }
  }

  async #readJson<T>(file: string, work: Work, optionalHintParseByteCeiling?: number): Promise<T | undefined> {
    work.check();
    let bytes: Buffer;
    try {
      bytes = await work.io(() => {
        work.cost.metadataReadCalls++;
        return fs.promises.readFile(file);
      }, result => { work.cost.metadataReadBytes += result.length; });
    } catch (error) { work.check(); if (codeOf(error) === "ENOENT") return undefined; throw error; }
    // Optional pruning hints may fall back to full discovery, but their entire native
    // read still counts. Check raw bytes before decoding or attempting JSON.parse.
    if (optionalHintParseByteCeiling !== undefined && bytes.length > optionalHintParseByteCeiling) return undefined;
    work.cost.metadataParseAttempts++;
    const value = JSON.parse(bytes.toString("utf8")) as T;
    const pause = work.checkpoint();
    if (pause) await work.wait(() => pause);
    return value;
  }

  async #names(directory: string, work: Work): Promise<string[]> {
    try { return await work.io(() => {
      work.cost.discoveryReadCalls++;
      return fs.promises.readdir(directory);
    }); }
    catch (error) { work.check(); if (codeOf(error) === "ENOENT") return []; throw error; }
  }

  async #numeric(directory: string, work: Work): Promise<string[]> {
    const names = await work.wait(() => this.#names(directory, work));
    const numeric: string[] = [];
    for (const name of names) {
      if (/^\d+$/.test(name)) numeric.push(name);
      const pause = work.checkpoint(1);
      if (pause) await work.wait(() => pause);
    }
    return numeric.sort();
  }

  async #openStage(after: number, horizon: number, work: Work): Promise<Stage> {
    work.cost.stages++;
    const stage: Stage = { after, horizon, days: [], nextDay: 0, phase: "live", considered: after, lastSequence: after };
    const config = await work.wait(() => this.#readJson<{ version?: unknown; dir?: unknown }>(
      path.join(this.access.root, MESH_ARCHIVE_CONFIG), work));
    if (config !== undefined) {
      if (!config || config.version !== 1 || typeof config.dir !== "string" || !path.isAbsolute(config.dir)) {
        throw new Error("Invalid mesh event archive configuration");
      }
      const oldest = await work.wait(() => this.#oldestLive(work));
      if (oldest !== undefined && after + 1 < oldest) {
        stage.archive = config.dir;
        for (const year of await work.wait(() => this.#numeric(config.dir as string, work))) {
          for (const month of await work.wait(() => this.#numeric(path.join(config.dir as string, year), work))) {
            for (const day of await work.wait(() => this.#numeric(path.join(config.dir as string, year, month), work))) {
              stage.days.push(path.join(year, month, day));
              const pause = work.checkpoint(1);
              if (pause) await work.wait(() => pause);
            }
          }
        }
        stage.phase = "archive";
      }
    }
    return stage;
  }

  #source(file: string, kind: Source["kind"], quantum: number, order = 0): Source {
    return { file, kind, order, quantum, requiredEnd: 0, physical: 0, observedEnd: 0, lineStart: 0,
      chunkStart: 0, chunkOffset: 0, stopped: false, lastSequence: 0, hints: [] };
  }

  #generation(work: Work): number {
    work.check();
    work.cost.generationProbes++;
    return this.access.generation(work.cost);
  }

  #assertStat(source: Source, stat: fs.Stats): void {
    if (source.identity && (source.identity.dev !== stat.dev || source.identity.ino !== stat.ino)) throw changed();
    // A proven above-H/torn suffix may be cut back. Unclassified read-ahead may not vanish
    // and then masquerade as positive EOF, even when no visible line needed those bytes yet.
    if (stat.size < Math.max(source.requiredEnd, source.stopped ? 0 : Math.max(source.physical, source.observedEnd))) throw changed();
  }

  async #validate(source: Source, work: Work): Promise<fs.Stats | undefined> {
    work.check();
    if (!source.identity) return;
    work.used.add(source);
    const stat = await work.io(() => {
      work.cost.sourceValidationCalls++;
      return fs.promises.stat(source.file);
    });
    this.#assertStat(source, stat);
    if (source.generation !== undefined && this.#generation(work) !== source.generation) throw changed();
    return stat;
  }

  async #refill(source: Source, work: Work): Promise<boolean> {
    work.check();
    let handle: FileHandle | undefined;
    try {
      // Look up open at use time: native FileHandle.read is also the production I/O meter's
      // observation boundary. Do not capture open or substitute a stream/readFile here.
      await work.io(() => fs.promises.open(source.file, "r").then(opened => {
        // Capture custody before io's post-await cancellation check can throw.
        handle = opened;
      }));
      const stat = await work.io(() => handle!.stat());
      this.#assertStat(source, stat);
      source.identity ??= { dev: stat.dev, ino: stat.ino };
      source.observedEnd = Math.max(source.observedEnd, stat.size);
      work.used.add(source);
      if (source.kind !== "archive") {
        const generation = this.#generation(work);
        if (source.generation !== undefined && source.generation !== generation) throw changed();
        if (source.generation === undefined && source.hintAfter !== undefined) {
          const hint = this.access.hint(source.hintAfter, generation, stat.ino);
          if (hint && hint.offset > 0 && hint.offset <= stat.size) {
            const byte = Buffer.allocUnsafe(1);
            const result = await work.io(() => {
              work.cost.probeReadCalls++;
              return handle!.read(byte, 0, 1, hint.offset - 1);
            }, read => { work.cost.probeReadBytes += read.bytesRead; });
            if (result.bytesRead === 1 && byte[0] === 0x0a) {
              source.physical = source.lineStart = source.requiredEnd = hint.offset;
              source.lastSequence = hint.sequence;
              source.boundary = hint;
            }
          }
        }
        source.generation = generation;
      }
      let end = Math.min(stat.size, source.limit ?? Infinity);
      if (source.physical >= end) {
        const current = await work.wait(() => this.#validate(source, work));
        end = Math.min(current!.size, source.limit ?? Infinity);
        if (source.physical >= end) return false;
        source.observedEnd = Math.max(source.observedEnd, current!.size);
      }
      const bytes = Buffer.allocUnsafe(Math.min(source.quantum, end - source.physical));
      const result = await work.io(() => {
        work.cost[`${source.kind}ReadCalls`]++;
        return handle!.read(bytes, 0, bytes.length, source.physical);
      }, read => { work.cost[`${source.kind}ReadBytes`] += read.bytesRead; });
      if (result.bytesRead === 0) {
        // Zero before an observed end is not EOF. Revalidate, then fail without publishing
        // coverage; a disappearing unclassified suffix cannot turn this into successful done.
        this.#assertStat(source, await work.io(() => handle!.stat()));
        await work.wait(() => this.#validate(source, work));
        throw new Error("Mesh recovery read made no progress before the observed end");
      }
      source.chunkStart = source.physical;
      source.physical += result.bytesRead;
      source.chunk = bytes.subarray(0, result.bytesRead);
      source.chunkOffset = 0;
      this.#assertStat(source, await work.io(() => handle!.stat()));
      // Compare the open inode with the current path after every refill (including renames).
      await work.wait(() => this.#validate(source, work));
      return true;
    } catch (error) {
      work.check();
      if (!handle && !source.identity && codeOf(error) === "ENOENT") return false;
      throw error;
    } finally {
      if (handle) await work.close(handle);
    }
  }

  #stop(source: Source, rewind: number): void {
    source.stopped = true;
    source.physical = source.lineStart = rewind;
    source.chunk = undefined;
    source.chunkOffset = 0;
  }

  #line(source: Source, work: Work): MaybeAsync<ByteLine | undefined> {
    work.check();
    if (source.stopped) return undefined;
    const chunk = source.chunk;
    if (chunk && source.chunkOffset < chunk.length) {
      const newline = chunk.indexOf(0x0a, source.chunkOffset);
      if (newline >= 0) {
        const start = source.lineStart;
        const length = newline - source.chunkOffset;
        const bytes = source.kind !== "archive" && length > this.access.maxEventBytes
          ? undefined : chunk.subarray(source.chunkOffset, newline);
        source.chunkOffset = newline + 1;
        source.lineStart = source.chunkStart + newline + 1;
        return { bytes, start, end: source.lineStart, rawBytes: source.lineStart - start };
      }
    }
    return this.#splitLine(source, work);
  }

  async #splitLine(source: Source, work: Work): Promise<ByteLine | undefined> {
    work.check();
    const start = source.lineStart;
    let fragments: Buffer[] = [];
    let length = 0, oversized = false;
    for (;;) {
      const pause = work.checkpoint();
      if (pause) await work.wait(() => pause);
      if (!source.chunk || source.chunkOffset >= source.chunk.length) {
        source.chunk = undefined;
        if (!await work.wait(() => this.#refill(source, work))) {
          // Stable end: no newline means no event. Retain the byte start, never a decoded
          // partial UTF-8 string, and re-read it after a higher horizon refreshes discovery.
          this.#stop(source, start);
          return undefined;
        }
      }
      const chunk = source.chunk!;
      const newline = chunk.indexOf(0x0a, source.chunkOffset);
      const end = newline < 0 ? chunk.length : newline;
      const fragment = chunk.subarray(source.chunkOffset, end);
      length += fragment.length;
      if (source.kind !== "archive" && length > this.access.maxEventBytes) {
        fragments = []; oversized = true; // Preserve normal live oversized-line skipping.
      } else if (!oversized && fragment.length) fragments.push(fragment);
      source.chunkOffset = newline < 0 ? chunk.length : newline + 1;
      if (newline < 0) continue;
      source.lineStart = source.chunkStart + newline + 1;
      const bytes = oversized ? undefined : fragments.length === 1 ? fragments[0]! : Buffer.concat(fragments, length);
      return { bytes, start, end: source.lineStart, rawBytes: source.lineStart - start };
    }
  }

  #parse(line: ByteLine, source: Source, work: Work): MeshEvent | undefined {
    if (!line.bytes) { work.cost.skippedRecords++; return undefined; }
    if (source.kind === "archive") work.cost.archiveParseAttempts++;
    else if (source.kind === "live") work.cost.liveParseAttempts++;
    else work.cost.probeParseAttempts++;
    const end = line.bytes.at(-1) === 0x0d ? line.bytes.length - 1 : line.bytes.length;
    try {
      const event = JSON.parse(line.bytes.toString("utf8", 0, end)) as MeshEvent | null;
      if (event && Number.isSafeInteger(event.sequence) && event.sequence > 0 && typeof event.id === "string") return event;
    } catch { /* malformed complete records grant no recovery identity */ }
    work.cost.skippedRecords++;
    return undefined;
  }

  async #oldestLive(work: Work): Promise<number | undefined> {
    const source = this.#source(path.join(this.access.root, "events.jsonl"), "probe", CHUNK_MAX);
    source.limit = this.access.maxEventBytes + 1;
    for (;;) {
      const line = await work.wait(() => this.#line(source, work));
      if (!line) return undefined;
      source.requiredEnd = line.end;
      const event = this.#parse(line, source, work);
      const pause = work.checkpoint(1);
      if (pause) await work.wait(() => pause);
      if (event) return event.sequence;
    }
  }

  #projectLine(line: ByteLine, source: Source, stage: Stage, work: Work, aborted: Record<string, string> | undefined, after: number): Projection | undefined {
    const event = this.#parse(line, source, work);
    if (event && event.sequence > stage.horizon) {
      // This address may be cut back and replaced before the next stage. Cache no projection
      // or read-ahead from above H, even if this call's fresh horizon has already grown.
      this.#stop(source, line.start);
      return undefined;
    }
    source.requiredEnd = line.end;
    if (!event) return undefined;
    stage.considered = Math.max(stage.considered, event.sequence);
    if (event.sequence <= source.lastSequence) { work.cost.skippedRecords++; return undefined; }
    source.lastSequence = event.sequence;
    if (source.kind === "live") {
      const hint = { sequence: event.sequence, offset: line.end };
      source.hints.push(hint);
      if (source.hints.length > 128) source.hints.shift();
      if (event.sequence <= source.hintAfter!) source.boundary = hint;
    }
    if (event.sequence <= after || aborted?.[event.sequence] === event.id) return undefined;
    const id = this.project(event);
    return { sequence: event.sequence, bytes: line.rawBytes, ...(id !== undefined ? { id } : {}) };
  }

  // Buffered complete records stay synchronous inside the yielding reader. Creating several
  // already-resolved promises per row costs more than the actual single-pass JSON projection.
  #head(source: Source, stage: Stage, work: Work, aborted?: Record<string, string>, after = stage.after): MaybeAsync<Projection | undefined> {
    for (;;) {
      const line = this.#line(source, work);
      if (line instanceof Promise) return this.#resumeHead(line, source, stage, work, aborted, after);
      if (!line) return undefined;
      const head = this.#projectLine(line, source, stage, work, aborted, after);
      const pause = work.checkpoint(1);
      if (pause) return this.#yieldHead(pause, head, source, stage, work, aborted, after);
      if (head || source.stopped) return head;
    }
  }

  async #resumeHead(pending: Promise<ByteLine | undefined>, source: Source, stage: Stage, work: Work,
    aborted: Record<string, string> | undefined, after: number): Promise<Projection | undefined> {
    const line = await work.wait(() => pending);
    if (!line) return undefined;
    const head = this.#projectLine(line, source, stage, work, aborted, after);
    const pause = work.checkpoint(1);
    if (pause) await work.wait(() => pause);
    if (head || source.stopped) return head;
    return work.wait(() => this.#head(source, stage, work, aborted, after));
  }

  async #yieldHead(pause: Promise<void>, head: Projection | undefined, source: Source, stage: Stage, work: Work,
    aborted: Record<string, string> | undefined, after: number): Promise<Projection | undefined> {
    await work.wait(() => pause);
    if (head || source.stopped) return head;
    return work.wait(() => this.#head(source, stage, work, aborted, after));
  }

  #less(left: Source, right: Source): boolean {
    return left.head!.sequence < right.head!.sequence ||
      left.head!.sequence === right.head!.sequence && left.order < right.order;
  }
  #push(heap: Source[], source: Source): void {
    let index = heap.length;
    heap.push(source);
    while (index > 0) {
      const parent = (index - 1) >>> 1;
      if (!this.#less(source, heap[parent]!)) break;
      heap[index] = heap[parent]!;
      index = parent;
    }
    heap[index] = source;
  }
  #pop(heap: Source[]): Source {
    const first = heap[0]!, last = heap.pop()!;
    if (heap.length) {
      let index = 0;
      for (;;) {
        let child = index * 2 + 1;
        if (child >= heap.length) break;
        if (child + 1 < heap.length && this.#less(heap[child + 1]!, heap[child]!)) child++;
        if (!this.#less(heap[child]!, last)) break;
        heap[index] = heap[child]!;
        index = child;
      }
      heap[index] = last;
    }
    return first;
  }

  async #enterDay(stage: Stage, work: Work): Promise<Day | undefined> {
    while (stage.nextDay < stage.days.length) {
      const directory = path.join(stage.archive!, stage.days[stage.nextDay++]!);
      const aborted = await work.wait(() => this.#readJson<Record<string, string>>(path.join(directory, "ABORTED.json"), work));
      const seal = await work.wait(() => this.#readJson<{ files?: Record<string, { lastSequence: number }> | null }>(
        path.join(directory, "SEAL.json"), work, SEAL_HINT_PARSE_MAX_BYTES));
      let maximum = 0;
      if (seal) {
        if (seal.files === undefined || seal.files === null) {
          throw new TypeError("Cannot convert undefined or null to object");
        }
        for (const name in seal.files) {
          if (!Object.hasOwn(seal.files, name)) continue;
          maximum = Math.max(maximum, seal.files[name]!.lastSequence);
          const pause = work.checkpoint(1);
          if (pause) await work.wait(() => pause);
        }
        if (maximum <= stage.after) continue;
      }
      const names: string[] = [];
      for (const name of await work.wait(() => this.#names(directory, work))) {
        const sealedFile = seal?.files?.[name];
        if (name.endsWith(".jsonl") && !(sealedFile && sealedFile.lastSequence <= stage.after)) names.push(name);
        const pause = work.checkpoint(1);
        if (pause) await work.wait(() => pause);
      }
      const quantum = Math.max(16 * 1024, Math.min(CHUNK_MAX, Math.floor(8 * 1024 * 1024 / Math.max(1, names.length))));
      const day: Day = { streams: [], heap: [], aborted };
      for (const [order, name] of names.entries()) {
        const source = this.#source(path.join(directory, name), "archive", quantum, order);
        day.streams.push(source);
        source.head = await work.wait(() => this.#head(source, stage, work, aborted));
        if (source.head) this.#push(day.heap, source);
      }
      return day;
    }
    return undefined;
  }

  async #resumeNext<T>(pending: Promise<T>, apply: (value: T) => void, stage: Stage, work: Work): Promise<Projection | undefined> {
    const value = await work.wait(() => pending);
    apply(value);
    return work.wait(() => this.#next(stage, work));
  }

  #next(stage: Stage, work: Work): MaybeAsync<Projection | undefined> {
    for (;;) {
      const pause = work.checkpoint();
      if (pause) return this.#resumeNext(pause, () => undefined, stage, work);
      let head: Projection | undefined;
      if (stage.phase === "archive") {
        if (!stage.day) return this.#resumeNext(this.#enterDay(stage, work), day => {
          stage.day = day;
          if (!day) stage.phase = "live";
        }, stage, work);
        const day = stage.day;
        if (day.pending) {
          const source = day.pending;
          const pending = this.#head(source, stage, work, day.aborted);
          if (pending instanceof Promise) return this.#resumeNext(pending, value => {
            source.head = value;
            if (value) this.#push(day.heap, source);
            day.pending = undefined;
          }, stage, work);
          source.head = pending;
          if (source.head) this.#push(day.heap, source);
          day.pending = undefined;
        }
        if (!day.heap.length) { stage.day = undefined; continue; }
        const source = this.#pop(day.heap);
        head = source.head!;
        source.head = undefined;
        day.pending = source;
      } else if (stage.phase === "live") {
        if (!stage.live) {
          if (stage.considered >= stage.horizon) { stage.phase = "done"; return undefined; }
          stage.live = this.#source(path.join(this.access.root, "events.jsonl"), "live", CHUNK_MAX);
          stage.live.hintAfter = stage.considered;
        }
        // Sequential handover: archive-aborted tails remain covered, not reintroduced by live.
        const live = stage.live;
        const pending = live.head ?? this.#head(live, stage, work, undefined, live.hintAfter);
        if (pending instanceof Promise) return this.#resumeNext(pending, value => {
          live.head = value;
          if (!value) stage.phase = "done";
        }, stage, work);
        head = pending;
        live.head = undefined;
        if (!head) { stage.phase = "done"; return undefined; }
      } else return undefined;
      if (head.sequence <= stage.lastSequence) { work.cost.skippedRecords++; continue; }
      stage.lastSequence = head.sequence;
      return head;
    }
  }
}
