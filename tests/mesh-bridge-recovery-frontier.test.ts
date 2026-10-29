import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import { performance } from "node:perf_hooks";
import { afterEach, expect, it } from "vitest";
import { BRIDGE_PROTOCOL_VERSION, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MESH_ARCHIVE_CONFIG, archiveFileName } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
import type { RecoveryReadCost } from "../src/mesh/recovery-reader.js";

// Native-FS scale regression for the accepted O(T) discovery/frontier-validation cut.
const roots: string[] = [];
const DAY = "2026/01/01";
const PEER = "synthetic-peer";
const TOPIC_PREFIX = "mesh.recovery.frontier.topic-";
const PAGE_POSITIONS = 10_000;
const SLICE_BUDGET_MS = 250;
const writer = { id: "fixture-writer", name: "writer", kind: "main" as const };

interface Fixture {
  root: string;
  archive: string;
  dayDirectory: string;
  store: MeshStore;
  topicCount: number;
  requiredTopic: string;
  archiveBytes: number;
}

const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-recovery-frontier-"));
  roots.push(root);
  return root;
};

const eventIdFor = (sequence: number): string => sequence === 1 ? "requiredID" : `position-${sequence}`;
const topicFor = (index: number): string => `${TOPIC_PREFIX}${String(index).padStart(6, "0")}`;
const row = (sequence: number, topic: string): MeshEvent => ({
  id: `archive-event-${sequence}`,
  sequence,
  topic,
  kind: "message",
  from: writer,
  createdAt: Date.UTC(2026, 0, 1),
  data: { bridge: { from: PEER, id: eventIdFor(sequence) } },
});
const line = (event: MeshEvent): string => `${JSON.stringify(event)}\n`;
const sha256 = (bytes: string): string => createHash("sha256").update(bytes).digest("hex");

function createFixture(topicCount: number): Fixture {
  const root = scratch();
  const archive = path.join(root, "history");
  const dayDirectory = path.join(archive, ...DAY.split("/"));
  fs.mkdirSync(dayDirectory, { recursive: true });

  const files: Record<string, { lines: number; firstSequence: number; lastSequence: number; sha256: string }> = {};
  let archiveBytes = 0;
  let liveHead: MeshEvent | undefined;
  for (let index = 0; index < topicCount; index++) {
    // Reverse sequence order relative to legal lexicographic topic order. Sequence 1,
    // including requiredID, is deliberately hidden in the final topic name.
    const topic = topicFor(index);
    const sequence = topicCount - index;
    const event = row(sequence, topic);
    if (sequence === topicCount) liveHead = event;
    const bytes = line(event);
    const name = archiveFileName(topic);
    fs.writeFileSync(path.join(dayDirectory, name), bytes);
    archiveBytes += Buffer.byteLength(bytes);
    files[name] = { lines: 1, firstSequence: sequence, lastSequence: sequence, sha256: sha256(bytes) };
  }

  const seal = { version: 1, day: DAY, files };
  fs.writeFileSync(path.join(dayDirectory, "SEAL.json"), `${JSON.stringify(seal)}\n`);
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), `${JSON.stringify({ version: 1, dir: archive })}\n`);
  // Match retention: only the committed head remains live; its complete archive row is sealed too.
  fs.writeFileSync(path.join(root, "events.jsonl"), line(liveHead!));
  fs.writeFileSync(path.join(root, "sequence"), String(topicCount));
  fs.writeFileSync(path.join(root, "generation"), "0\n");

  return { root, archive, dayDirectory, store: new MeshStore(root, 64 * 1024, 500), topicCount,
    requiredTopic: topicFor(topicCount - 1), archiveBytes };
}

interface NativeSnapshot {
  archiveReadCalls: number;
  archiveReadBytes: number;
  liveReadCalls: number;
  liveReadBytes: number;
  probeReadCalls: number;
  probeReadBytes: number;
  metadataReadCalls: number;
  metadataReadBytes: number;
  discoveryReadCalls: number;
  sourceValidationCalls: number;
  archiveParseAttempts: number;
  liveParseAttempts: number;
  probeParseAttempts: number;
  metadataParseAttempts: number;
  jsonParseAttempts: number;
  generationReadCalls: number;
  generationReadBytes: number;
  archiveOpenCalls: number;
  archiveCloseCalls: number;
  probeOpenCalls: number;
  probeCloseCalls: number;
  handleStatCalls: number;
  failedReadCalls: number;
  failedReadBytes: number;
  failedOpenCalls: number;
  openHandleCount: number;
  openFdCount: number;
}

type ReadKind = "archive" | "live" | "probe";
type ObserverOptions = {
  onDiscovery?: (directory: string) => void;
  onSourceStat?: (file: string) => void;
};

function observeNative(fixture: Fixture, options: ObserverOptions = {}) {
  const root = fixture.root;
  const topicCount = fixture.topicCount;
  const archive = path.join(root, "history");
  const archivePrefix = `${archive}${path.sep}`;
  const eventsFile = path.join(root, "events.jsonl");
  const configFile = path.join(root, MESH_ARCHIVE_CONFIG);
  const generationFile = path.join(root, "generation");
  const descriptorKinds = new Map<number, ReadKind>();
  const activeHandles = new Set<Awaited<ReturnType<typeof fs.promises.open>>>();
  const nativeHandleClose = new Map<Awaited<ReturnType<typeof fs.promises.open>>, () => Promise<void>>();
  const counters: Omit<NativeSnapshot, "openHandleCount" | "openFdCount"> = {
    archiveReadCalls: 0, archiveReadBytes: 0, liveReadCalls: 0, liveReadBytes: 0,
    probeReadCalls: 0, probeReadBytes: 0, metadataReadCalls: 0, metadataReadBytes: 0,
    discoveryReadCalls: 0, sourceValidationCalls: 0,
    archiveParseAttempts: 0, liveParseAttempts: 0, probeParseAttempts: 0, metadataParseAttempts: 0,
    jsonParseAttempts: 0, generationReadCalls: 0, generationReadBytes: 0,
    archiveOpenCalls: 0, archiveCloseCalls: 0, probeOpenCalls: 0, probeCloseCalls: 0,
    handleStatCalls: 0, failedReadCalls: 0, failedReadBytes: 0, failedOpenCalls: 0,
  };
  let insideFullRead = false;
  let pendingHorizonEventParse = 0;
  let pendingAsyncProbeEventParse = 0;
  let restored = false;

  const pathString = (value: unknown): string | undefined => typeof value === "string" ? value : undefined;
  const readKind = (value: unknown): ReadKind | undefined => {
    const file = pathString(value);
    if (!file) return undefined;
    if (file === eventsFile) return "probe";
    if (file.startsWith(archivePrefix) && file.endsWith(".jsonl")) return "archive";
    return undefined;
  };
  const metadataPath = (value: unknown): boolean => {
    const file = pathString(value);
    return file === configFile || file === generationFile ||
      (file !== undefined && file.startsWith(archivePrefix) &&
        (path.basename(file) === "SEAL.json" || path.basename(file) === "ABORTED.json"));
  };
  const countRead = (kind: ReadKind, bytes: number): void => {
    if (kind === "archive") { counters.archiveReadCalls++; counters.archiveReadBytes += bytes; }
    else if (kind === "live") { counters.liveReadCalls++; counters.liveReadBytes += bytes; }
    else { counters.probeReadCalls++; counters.probeReadBytes += bytes; }
  };
  const countOpen = (kind: ReadKind): void => {
    if (kind === "archive") counters.archiveOpenCalls++;
    else if (kind === "probe") counters.probeOpenCalls++;
  };
  const countClose = (kind: ReadKind): void => {
    if (kind === "archive") counters.archiveCloseCalls++;
    else if (kind === "probe") counters.probeCloseCalls++;
  };
  const byteLength = (value: Buffer | string): number => Buffer.isBuffer(value) ? value.length : Buffer.byteLength(value);

  const nativeReadFile = fs.promises.readFile;
  const nativeReadFileSync = fs.readFileSync;
  const nativeOpen = fs.promises.open;
  const nativeOpenSync = fs.openSync;
  const nativeReadSync = fs.readSync;
  const nativeCloseSync = fs.closeSync;
  const nativeReaddir = fs.promises.readdir;
  const nativeStat = fs.promises.stat;
  const nativeParse = JSON.parse;

  fs.promises.readFile = (async (...args: Parameters<typeof nativeReadFile>) => {
    const file = args[0];
    const kind = readKind(file);
    const metadata = metadataPath(file);
    if (metadata) counters.metadataReadCalls++;
    if (pathString(file) === generationFile) counters.generationReadCalls++;
    if (kind) countRead(kind, 0);
    try {
      const result = await nativeReadFile(...args);
      const bytes = byteLength(result as Buffer | string);
      if (metadata) counters.metadataReadBytes += bytes;
      if (pathString(file) === generationFile) counters.generationReadBytes += bytes;
      if (kind) {
        if (kind === "archive") counters.archiveReadBytes += bytes;
        else if (kind === "live") counters.liveReadBytes += bytes;
        else counters.probeReadBytes += bytes;
      }
      return result;
    } catch (error) {
      if (metadata || kind) counters.failedReadCalls++;
      throw error;
    }
  }) as typeof fs.promises.readFile;

  fs.readFileSync = ((...args: Parameters<typeof nativeReadFileSync>) => {
    const file = args[0];
    const kind = readKind(file);
    const metadata = metadataPath(file);
    if (metadata) counters.metadataReadCalls++;
    if (pathString(file) === generationFile) counters.generationReadCalls++;
    if (kind) countRead(kind, 0);
    const parent = insideFullRead;
    insideFullRead = true;
    try {
      const result = nativeReadFileSync(...args);
      const bytes = byteLength(result as Buffer | string);
      if (metadata) counters.metadataReadBytes += bytes;
      if (pathString(file) === generationFile) counters.generationReadBytes += bytes;
      if (kind) {
        if (kind === "archive") counters.archiveReadBytes += bytes;
        else if (kind === "live") counters.liveReadBytes += bytes;
        else counters.probeReadBytes += bytes;
      }
      return result;
    } catch (error) {
      if (metadata || kind) counters.failedReadCalls++;
      throw error;
    } finally { insideFullRead = parent; }
  }) as typeof fs.readFileSync;

  fs.openSync = ((...args: Parameters<typeof nativeOpenSync>) => {
    const kind = readKind(args[0]);
    if (kind) countOpen(kind);
    try {
      const descriptor = nativeOpenSync(...args);
      if (kind) descriptorKinds.set(descriptor, kind);
      return descriptor;
    } catch (error) {
      if (kind) counters.failedOpenCalls++;
      throw error;
    }
  }) as typeof fs.openSync;

  fs.readSync = ((...args: Parameters<typeof nativeReadSync>) => {
    const kind = descriptorKinds.get(args[0]);
    if (!kind || insideFullRead) return nativeReadSync(...args);
    countRead(kind, 0);
    try {
      const bytes = nativeReadSync(...args);
      if (kind === "archive") counters.archiveReadBytes += bytes;
      else if (kind === "live") counters.liveReadBytes += bytes;
      else {
        counters.probeReadBytes += bytes;
        if (bytes > 1) pendingHorizonEventParse++;
      }
      return bytes;
    } catch (error) {
      counters.failedReadCalls++;
      throw error;
    }
  }) as typeof fs.readSync;

  fs.closeSync = ((descriptor: number) => {
    const kind = descriptorKinds.get(descriptor);
    if (kind) countClose(kind);
    const result = nativeCloseSync(descriptor);
    descriptorKinds.delete(descriptor);
    return result;
  }) as typeof fs.closeSync;

  fs.promises.open = (async (...args: Parameters<typeof nativeOpen>) => {
    const kind = readKind(args[0]);
    if (kind) countOpen(kind);
    let handle: Awaited<ReturnType<typeof nativeOpen>>;
    try { handle = await nativeOpen(...args); }
    catch (error) {
      if (kind) counters.failedOpenCalls++;
      throw error;
    }
    if (kind) {
      activeHandles.add(handle);
      const close = handle.close.bind(handle);
      nativeHandleClose.set(handle, close);
      const read = handle.read;
      const stat = handle.stat;
      handle.read = (async (...readArgs: Parameters<typeof read>) => {
        countRead(kind, 0);
        try {
          const result = await Reflect.apply(read, handle, readArgs) as Awaited<ReturnType<typeof read>>;
          if (kind === "archive") counters.archiveReadBytes += result.bytesRead;
          else if (kind === "live") counters.liveReadBytes += result.bytesRead;
          else {
            counters.probeReadBytes += result.bytesRead;
            if (result.bytesRead > 0) pendingAsyncProbeEventParse++;
          }
          return result;
        } catch (error) {
          counters.failedReadCalls++;
          throw error;
        }
      }) as typeof handle.read;
      handle.stat = (async (...statArgs: Parameters<typeof stat>) => {
        counters.handleStatCalls++;
        return Reflect.apply(stat, handle, statArgs) as ReturnType<typeof stat>;
      }) as typeof handle.stat;
      handle.close = async () => {
        countClose(kind);
        try { return await close(); }
        finally {
          if (handle.fd === -1) {
            activeHandles.delete(handle);
            nativeHandleClose.delete(handle);
          }
        }
      };
    }
    return handle;
  }) as typeof fs.promises.open;

  fs.promises.readdir = (async (...args: Parameters<typeof nativeReaddir>) => {
    const directory = pathString(args[0]);
    const discovered = directory === archive || (directory !== undefined && directory.startsWith(archivePrefix));
    if (discovered) counters.discoveryReadCalls++;
    try {
      const result = await nativeReaddir(...args);
      if (discovered && directory !== undefined) options.onDiscovery?.(directory);
      return result;
    } catch (error) { throw error; }
  }) as typeof fs.promises.readdir;

  fs.promises.stat = (async (...args: Parameters<typeof nativeStat>) => {
    const file = pathString(args[0]);
    const sourcePath = file === eventsFile || (file !== undefined && file.startsWith(archivePrefix) && file.endsWith(".jsonl"));
    if (sourcePath) counters.sourceValidationCalls++;
    try {
      const result = await nativeStat(...args);
      if (sourcePath && file !== undefined) options.onSourceStat?.(file);
      return result;
    } catch (error) { throw error; }
  }) as typeof fs.promises.stat;

  JSON.parse = ((...args: Parameters<typeof nativeParse>) => {
    const text = args[0];
    const input = typeof text === "string" ? text : String(text);
    counters.jsonParseAttempts++;
    try {
      const parsed = Reflect.apply(nativeParse, JSON, args) as unknown;
      if (parsed && typeof parsed === "object" && "topic" in parsed && typeof parsed.topic === "string" &&
          "sequence" in parsed && Number.isSafeInteger(parsed.sequence)) {
        if (parsed.sequence === topicCount && pendingHorizonEventParse > 0) {
          counters.probeParseAttempts++;
          pendingHorizonEventParse--;
        } else if (parsed.sequence === topicCount && pendingAsyncProbeEventParse > 0) {
          counters.probeParseAttempts++;
          pendingAsyncProbeEventParse--;
        } else counters.archiveParseAttempts++;
      } else counters.metadataParseAttempts++;
      return parsed;
    } catch (error) {
      // Malformed event-shaped input is still an event parse attempt.
      if (input.includes('"topic"') && input.includes('"sequence"') && input.includes('"id"')) {
        counters.archiveParseAttempts++;
      } else counters.metadataParseAttempts++;
      throw error;
    }
  }) as typeof JSON.parse;

  syncBuiltinESMExports();

  const snapshot = (): NativeSnapshot => ({ ...counters,
    openHandleCount: activeHandles.size, openFdCount: descriptorKinds.size });
  const restore = (): void => {
    if (restored) return;
    restored = true;
    fs.promises.readFile = nativeReadFile;
    fs.readFileSync = nativeReadFileSync;
    fs.promises.open = nativeOpen;
    fs.openSync = nativeOpenSync;
    fs.readSync = nativeReadSync;
    fs.closeSync = nativeCloseSync;
    fs.promises.readdir = nativeReaddir;
    fs.promises.stat = nativeStat;
    JSON.parse = nativeParse;
    syncBuiltinESMExports();
  };
  const closeLeakedHandles = async (): Promise<void> => {
    for (const handle of [...activeHandles]) {
      const close = nativeHandleClose.get(handle);
      if (close && handle.fd !== -1) await close().catch(() => undefined);
      if (handle.fd === -1) activeHandles.delete(handle);
    }
    for (const descriptor of [...descriptorKinds.keys()]) {
      try { nativeCloseSync(descriptor); } catch { /* report the pre-cleanup leak in the snapshot */ }
      descriptorKinds.delete(descriptor);
    }
  };
  return { snapshot, restore, closeLeakedHandles };
}

interface TimedResult<T> {
  value?: T;
  error?: unknown;
  elapsedMs: number;
  maxCheckGapMs: number;
  checkTurns: number;
}

async function withCheckPhaseHeartbeat<T>(operation: () => Promise<T>): Promise<TimedResult<T>> {
  let finished = false;
  let turns = 0;
  let previous = performance.now();
  let maxCheckGapMs = 0;
  const heartbeat = (): void => {
    const now = performance.now();
    maxCheckGapMs = Math.max(maxCheckGapMs, now - previous);
    previous = now;
    turns++;
    if (!finished) setImmediate(heartbeat);
  };
  setImmediate(heartbeat);
  const started = performance.now();
  let value: T | undefined;
  let error: unknown;
  try { value = await operation(); }
  catch (failure) { error = failure; }
  const ended = performance.now();
  maxCheckGapMs = Math.max(maxCheckGapMs, ended - previous);
  finished = true;
  await new Promise<void>(resolve => setImmediate(resolve));
  return { ...(value !== undefined ? { value } : {}), ...(error !== undefined ? { error } : {}),
    elapsedMs: ended - started, maxCheckGapMs, checkTurns: turns };
}

const copyCost = (cost: RecoveryReadCost | undefined): RecoveryReadCost | undefined => cost ? {
  ...cost,
  // Older recovery snapshots may predate these two accounting fields; default only
  // those absent fields so scale/coverage evidence is emitted before accounting assertions.
  metadataParseAttempts: cost.metadataParseAttempts ?? 0,
  sourceValidationCalls: cost.sourceValidationCalls ?? 0,
} : undefined;

const COST_KEYS: Array<keyof RecoveryReadCost> = [
  "events", "eventBytes", "maxSliceMs", "archiveReadBytes", "archiveReadCalls", "archiveParseAttempts",
  "liveReadBytes", "liveReadCalls", "liveParseAttempts", "probeReadBytes", "probeReadCalls", "probeParseAttempts",
  "metadataReadBytes", "metadataReadCalls", "metadataParseAttempts", "discoveryReadCalls", "sourceValidationCalls",
  "generationProbes", "horizonMs", "stages", "skippedRecords",
];

function addCosts(costs: RecoveryReadCost[]): RecoveryReadCost {
  const total: RecoveryReadCost = {
    events: 0, eventBytes: 0, maxSliceMs: 0,
    archiveReadBytes: 0, archiveReadCalls: 0, archiveParseAttempts: 0,
    liveReadBytes: 0, liveReadCalls: 0, liveParseAttempts: 0,
    probeReadBytes: 0, probeReadCalls: 0, probeParseAttempts: 0,
    metadataReadBytes: 0, metadataReadCalls: 0, metadataParseAttempts: 0,
    discoveryReadCalls: 0, sourceValidationCalls: 0,
    generationProbes: 0, horizonMs: 0, stages: 0, skippedRecords: 0,
  };
  for (const cost of costs) {
    for (const key of COST_KEYS) {
      total[key] = key === "maxSliceMs" ? Math.max(total[key], cost[key]) : total[key] + cost[key];
    }
  }
  return total;
}

const NATIVE_COUNTER_KEYS: Array<keyof Omit<NativeSnapshot, "openHandleCount" | "openFdCount">> = [
  "archiveReadCalls", "archiveReadBytes", "liveReadCalls", "liveReadBytes", "probeReadCalls", "probeReadBytes",
  "metadataReadCalls", "metadataReadBytes", "discoveryReadCalls", "sourceValidationCalls",
  "archiveParseAttempts", "liveParseAttempts", "probeParseAttempts", "metadataParseAttempts", "jsonParseAttempts",
  "generationReadCalls", "generationReadBytes", "archiveOpenCalls", "archiveCloseCalls", "probeOpenCalls", "probeCloseCalls", "handleStatCalls",
  "failedReadCalls", "failedReadBytes", "failedOpenCalls",
];

function nativeDelta(current: NativeSnapshot, previous: NativeSnapshot): NativeSnapshot {
  const difference = {} as NativeSnapshot;
  for (const key of NATIVE_COUNTER_KEYS) difference[key] = current[key] - previous[key];
  difference.openHandleCount = current.openHandleCount;
  difference.openFdCount = current.openFdCount;
  return difference;
}

interface PageRecord {
  page: { ids: string[]; through: number; done: boolean };
  cost?: RecoveryReadCost;
  timing: TimedResult<{ ids: string[]; through: number; done: boolean }>;
  native: NativeSnapshot;
}

interface SuccessfulScan {
  pages: PageRecord[];
  ids: string[];
  failure?: unknown;
  native: NativeSnapshot;
  memoryBefore: { rss: number; heapUsed: number };
  memoryAfter: { rss: number; heapUsed: number };
  elapsedMs: number;
  maxCheckGapMs: number;
  checkTurns: number;
}

async function runSuccessfulScan(fixture: Fixture): Promise<SuccessfulScan> {
  const side = new StoreBridgeSide(fixture.store, PEER);
  const observer = observeNative(fixture);
  const pages: PageRecord[] = [];
  const ids: string[] = [];
  const memory = process.memoryUsage();
  const memoryBefore = { rss: memory.rss, heapUsed: memory.heapUsed };
  let cursor = 0;
  let failure: unknown;
  let elapsedMs = 0, maxCheckGapMs = 0, checkTurns = 0;
  let previousNative = observer.snapshot();
  let native!: NativeSnapshot;
  let memoryAfter!: { rss: number; heapUsed: number };
  const maxAttempts = Math.ceil(fixture.topicCount / PAGE_POSITIONS) + 2;
  try {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const timing = await withCheckPhaseHeartbeat(() => side.bridgedIds(cursor));
      elapsedMs += timing.elapsedMs;
      maxCheckGapMs = Math.max(maxCheckGapMs, timing.maxCheckGapMs);
      checkTurns += timing.checkTurns;
      if (!timing.value) { failure = timing.error ?? new Error("Recovery returned no page"); break; }
      const page = timing.value;
      ids.push(...page.ids);
      const cost = copyCost(side.recoveryReadCost);
      const currentNative = observer.snapshot();
      pages.push({ page, ...(cost ? { cost } : {}), timing, native: nativeDelta(currentNative, previousNative) });
      previousNative = currentNative;
      if (page.done) break;
      if (page.through <= cursor) { failure = new Error("Recovery page did not advance"); break; }
      cursor = page.through;
    }
  } catch (error) {
    failure ??= error;
  } finally {
    const finalMemory = process.memoryUsage();
    memoryAfter = { rss: finalMemory.rss, heapUsed: finalMemory.heapUsed };
    native = observer.snapshot();
    observer.restore();
    await observer.closeLeakedHandles();
  }
  return { pages, ids, ...(failure !== undefined ? { failure } : {}), native, memoryBefore, memoryAfter,
    elapsedMs, maxCheckGapMs, checkTurns };
}

interface CancellationRecord {
  kind: "initial-discovery" | "resumed-validation";
  triggered: boolean;
  pageReturned: boolean;
  error?: { name: string; message: string; hasPageFields: boolean };
  cost?: RecoveryReadCost;
  native: NativeSnapshot;
  timing: TimedResult<{ ids: string[]; through: number; done: boolean }>;
  priorCost?: RecoveryReadCost;
  primeTiming?: TimedResult<{ ids: string[]; through: number; done: boolean }>;
  primeMaxCheckGapMs?: number;
  fenceSettled: boolean;
}

const errorRecord = (error: unknown): CancellationRecord["error"] => error instanceof Error
  ? { name: error.name, message: error.message,
      hasPageFields: "ids" in error || "through" in error || "done" in error }
  : undefined;

async function runInitialDiscoveryCancellation(fixture: Fixture): Promise<CancellationRecord> {
  const side = new StoreBridgeSide(fixture.store, PEER);
  let scheduled = false;
  let fencePromise: Promise<void> | undefined;
  let triggered = false;
  const scheduleFence = (): void => {
    if (scheduled) return;
    scheduled = true;
    setImmediate(() => { triggered = true; fencePromise = side.fence(); });
  };
  const observer = observeNative(fixture, {
    onDiscovery: directory => { if (directory === fixture.dayDirectory) scheduleFence(); },
  });
  let timing!: CancellationRecord["timing"];
  let cost: RecoveryReadCost | undefined;
  let native!: NativeSnapshot;
  try {
    timing = await withCheckPhaseHeartbeat(() => side.bridgedIds(0));
    await new Promise<void>(resolve => setImmediate(resolve));
    if (fencePromise) await fencePromise;
    cost = copyCost(side.recoveryReadCost);
    native = observer.snapshot();
  } finally {
    observer.restore();
    await observer.closeLeakedHandles();
  }
  const failure = errorRecord(timing.error);
  return { kind: "initial-discovery", triggered, pageReturned: timing.value !== undefined,
    ...(failure ? { error: failure } : {}), ...(cost ? { cost } : {}),
    native, timing, fenceSettled: fencePromise !== undefined };
}

async function runResumedValidationCancellation(fixture: Fixture): Promise<CancellationRecord> {
  const side = new StoreBridgeSide(fixture.store, PEER);
  const primeTiming = await withCheckPhaseHeartbeat(() => side.bridgedIds(0));
  const priorCost = copyCost(side.recoveryReadCost);
  const after = primeTiming.value?.through ?? 0;
  let scheduled = false;
  let fencePromise: Promise<void> | undefined;
  let triggered = false;
  const scheduleFence = (): void => {
    if (scheduled) return;
    scheduled = true;
    setImmediate(() => { triggered = true; fencePromise = side.fence(); });
  };
  const observer = observeNative(fixture, {
    onSourceStat: file => { if (file.startsWith(`${fixture.archive}${path.sep}`)) scheduleFence(); },
  });
  let timing!: CancellationRecord["timing"];
  let cost: RecoveryReadCost | undefined;
  let native!: NativeSnapshot;
  try {
    timing = await withCheckPhaseHeartbeat(() => side.bridgedIds(after));
    await new Promise<void>(resolve => setImmediate(resolve));
    if (fencePromise) await fencePromise;
    cost = copyCost(side.recoveryReadCost);
    native = observer.snapshot();
  } finally {
    observer.restore();
    await observer.closeLeakedHandles();
  }
  const failure = errorRecord(timing.error);
  return { kind: "resumed-validation", triggered, pageReturned: timing.value !== undefined,
    ...(failure ? { error: failure } : {}), ...(cost ? { cost } : {}),
    ...(priorCost ? { priorCost } : {}), native, timing, primeTiming, primeMaxCheckGapMs: primeTiming.maxCheckGapMs,
    fenceSettled: fencePromise !== undefined };
}

const expectedIds = (count: number): string[] => Array.from({ length: count }, (_, index) => eventIdFor(index + 1));
const costParseTotal = (cost: RecoveryReadCost): number =>
  cost.archiveParseAttempts + cost.liveParseAttempts + cost.probeParseAttempts + cost.metadataParseAttempts;

function expectCostMatchesNative(cost: RecoveryReadCost, native: NativeSnapshot, attempts = 1): void {
  expect(cost.archiveReadCalls).toBe(native.archiveReadCalls);
  expect(cost.archiveReadBytes).toBe(native.archiveReadBytes);
  expect(cost.liveReadCalls).toBe(native.liveReadCalls);
  expect(cost.liveReadBytes).toBe(native.liveReadBytes);
  expect(cost.probeReadCalls).toBe(native.probeReadCalls);
  expect(cost.probeReadBytes).toBe(native.probeReadBytes);
  expect(cost.metadataReadCalls).toBe(native.metadataReadCalls);
  expect(cost.metadataReadBytes).toBe(native.metadataReadBytes);
  expect(cost.archiveParseAttempts).toBe(native.archiveParseAttempts);
  expect(cost.liveParseAttempts).toBe(native.liveParseAttempts);
  expect(cost.probeParseAttempts).toBe(native.probeParseAttempts);
  expect(cost.metadataParseAttempts).toBe(native.metadataParseAttempts);
  expect(cost.discoveryReadCalls).toBe(native.discoveryReadCalls);
  expect(cost.sourceValidationCalls).toBe(native.sourceValidationCalls);
  expect(cost.generationProbes + attempts).toBe(native.generationReadCalls);
  expect(cost.horizonMs).toBeGreaterThanOrEqual(0);
  expect(Number.isFinite(cost.horizonMs)).toBe(true);
  expect(costParseTotal(cost)).toBe(native.jsonParseAttempts);
}

function emitEvidence(fixture: Fixture, scan: SuccessfulScan, initial: CancellationRecord, resumed?: CancellationRecord): void {
  const first = scan.pages[0];
  const second = scan.pages[1];
  const aggregate = addCosts(scan.pages.flatMap(record => record.cost ? [record.cost] : []));
  const memory = scan.memoryAfter;
  console.log(`EVIDENCE ${JSON.stringify({ kind: "first-page", topics: fixture.topicCount,
    positions: first?.cost?.events ?? 0, ids: first?.page.ids.length ?? 0, through: first?.page.through ?? null,
    done: first?.page.done ?? null, elapsedMs: first?.timing.elapsedMs ?? null,
    maxSliceMs: first?.cost?.maxSliceMs ?? null, maxCheckGapMs: first?.timing.maxCheckGapMs ?? null,
    checkTurns: first?.timing.checkTurns ?? 0, publicCost: first?.cost ?? null, native: first?.native ?? null,
    rssBefore: scan.memoryBefore.rss, rssAfter: memory.rss, heapUsedBefore: scan.memoryBefore.heapUsed,
    heapUsedAfter: memory.heapUsed })}`);
  if (second) {
    console.log(`EVIDENCE ${JSON.stringify({ kind: "resumed-validation-page", topics: fixture.topicCount,
      positions: second.cost?.events ?? 0, ids: second.page.ids.length, through: second.page.through, done: second.page.done,
      elapsedMs: second.timing.elapsedMs, maxSliceMs: second.cost?.maxSliceMs ?? null,
      maxCheckGapMs: second.timing.maxCheckGapMs, checkTurns: second.timing.checkTurns,
      publicCost: second.cost ?? null, native: second.native })}`);
  } else {
    console.log(`EVIDENCE ${JSON.stringify({ kind: "resumed-validation-page", topics: fixture.topicCount,
      status: "not-applicable-single-page-day", reason: "all positions fit the unchanged 10000-position page" })}`);
  }
  console.log(`EVIDENCE ${JSON.stringify({ kind: "aggregate", topics: fixture.topicCount,
    positions: scan.ids.length, pages: scan.pages.length, done: scan.pages.at(-1)?.page.done ?? false,
    elapsedMs: scan.elapsedMs, maxSliceMs: aggregate.maxSliceMs, maxCheckGapMs: scan.maxCheckGapMs,
    checkTurns: scan.checkTurns, rssBefore: scan.memoryBefore.rss, rssAfter: memory.rss,
    heapUsedBefore: scan.memoryBefore.heapUsed, heapUsedAfter: memory.heapUsed,
    expectedArchiveBytes: fixture.archiveBytes, publicCost: aggregate, native: scan.native })}`);
  console.log(`EVIDENCE ${JSON.stringify({ kind: "cancellation", topics: fixture.topicCount,
    initialDiscovery: { triggered: initial.triggered, pageReturned: initial.pageReturned,
      elapsedMs: initial.timing.elapsedMs, maxCheckGapMs: initial.timing.maxCheckGapMs,
      cost: initial.cost ?? null, native: initial.native },
    resumedValidation: resumed ? { triggered: resumed.triggered, pageReturned: resumed.pageReturned,
      primeElapsedMs: resumed.primeTiming?.elapsedMs ?? null, primeMaxCheckGapMs: resumed.primeMaxCheckGapMs ?? null,
      elapsedMs: resumed.timing.elapsedMs, maxCheckGapMs: resumed.timing.maxCheckGapMs,
      priorCost: resumed.priorCost ?? null, cost: resumed.cost ?? null, native: resumed.native } :
      { status: "not-applicable-single-page-day" } })}`);
}

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 300_000);

it.each([100_000, 3_321])("measures native recovery over a %s-topic one-row archive day", async topicCount => {
  const fixture = createFixture(topicCount);
  const initialCancellation = await runInitialDiscoveryCancellation(fixture);
  const resumedCancellation = topicCount > PAGE_POSITIONS
    ? await runResumedValidationCancellation(fixture)
    : undefined;
  // The clean scan is a fresh-side positive control after cancellation and proves complete coverage.
  const scan = await runSuccessfulScan(fixture);

  // Emit positive cardinality, page, slice, memory and native-work evidence before assertions.
  emitEvidence(fixture, scan, initialCancellation, resumedCancellation);

  const expected = expectedIds(topicCount);
  const expectedPages = Math.ceil(topicCount / PAGE_POSITIONS);
  expect(scan.failure).toBeUndefined();
  expect(scan.pages).toHaveLength(expectedPages);
  expect(scan.ids).toHaveLength(topicCount);
  expect(new Set(scan.ids).size).toBe(topicCount);
  expect(scan.ids).toEqual(expected);
  expect(BRIDGE_PROTOCOL_VERSION).toBe(2);
  expect(fixture.requiredTopic > topicFor(0)).toBe(true);
  expect(scan.pages[0]?.page.ids[0]).toBe("requiredID");
  expect(scan.pages[0]?.page.ids.length).toBe(Math.min(PAGE_POSITIONS, topicCount));
  expect(scan.pages[0]?.cost?.sourceValidationCalls).toBeGreaterThanOrEqual(topicCount);
  for (let index = 0; index < scan.pages.length; index++) {
    const record = scan.pages[index]!;
    const start = index * PAGE_POSITIONS;
    const end = Math.min(start + PAGE_POSITIONS, topicCount);
    expect(record.page.ids).toEqual(expected.slice(start, end));
    expect(record.page.through).toBe(end);
    expect(record.page.done).toBe(end === topicCount);
    expect(Object.keys(record.page).sort()).toEqual(["done", "ids", "through"]);
    expect(record.page.ids.length).toBeLessThanOrEqual(PAGE_POSITIONS);
    expect(record.cost).toBeDefined();
    expect(record.cost!.events).toBe(end - start);
    expectCostMatchesNative(record.cost!, record.native);
    if (index > 0 && topicCount > PAGE_POSITIONS) {
      expect(record.cost!.sourceValidationCalls).toBeGreaterThanOrEqual(topicCount);
    }
    expect(record.cost!.maxSliceMs).toBeLessThan(SLICE_BUDGET_MS);
    expect(record.timing.maxCheckGapMs).toBeLessThan(SLICE_BUDGET_MS);
  }

  const aggregate = addCosts(scan.pages.flatMap(record => record.cost ? [record.cost] : []));
  expectCostMatchesNative(aggregate, scan.native, scan.pages.length);
  expect(scan.pages[0]?.cost?.archiveReadCalls).toBe(topicCount);
  expect(scan.pages[0]?.cost?.archiveReadBytes).toBe(fixture.archiveBytes);
  expect(scan.pages[0]?.cost?.archiveParseAttempts).toBe(topicCount);
  expect(scan.pages[0]?.cost?.metadataReadCalls).toBeGreaterThan(0);
  expect(scan.pages[0]?.cost?.metadataParseAttempts).toBeGreaterThan(0);
  expect(scan.pages[0]?.cost?.discoveryReadCalls).toBeGreaterThan(0);
  expect(aggregate.archiveReadCalls).toBe(scan.native.archiveReadCalls);
  expect(aggregate.archiveReadBytes).toBe(scan.native.archiveReadBytes);
  expect(aggregate.liveReadCalls).toBe(scan.native.liveReadCalls);
  expect(aggregate.liveReadBytes).toBe(scan.native.liveReadBytes);
  expect(aggregate.probeReadCalls).toBe(scan.native.probeReadCalls);
  expect(aggregate.probeReadBytes).toBe(scan.native.probeReadBytes);
  expect(aggregate.metadataReadCalls).toBe(scan.native.metadataReadCalls);
  expect(aggregate.metadataReadBytes).toBe(scan.native.metadataReadBytes);
  expect(aggregate.discoveryReadCalls).toBe(scan.native.discoveryReadCalls);
  expect(aggregate.sourceValidationCalls).toBe(scan.native.sourceValidationCalls);
  expect(aggregate.archiveParseAttempts).toBe(topicCount);
  expect(aggregate.liveParseAttempts).toBe(scan.native.liveParseAttempts);
  expect(aggregate.probeParseAttempts).toBe(scan.native.probeParseAttempts);
  expect(aggregate.metadataParseAttempts).toBe(scan.native.metadataParseAttempts);
  expect(costParseTotal(aggregate)).toBe(scan.native.jsonParseAttempts);
  expect(scan.native.archiveParseAttempts + scan.native.liveParseAttempts + scan.native.probeParseAttempts)
    .toBe(scan.native.jsonParseAttempts - scan.native.metadataParseAttempts);
  expect(aggregate.archiveReadBytes).toBe(fixture.archiveBytes);
  expect(scan.native.failedReadCalls).toBeGreaterThanOrEqual(1); // Missing ABORTED.json is a counted 0-byte attempt.
  expect(scan.native.failedReadBytes).toBe(0);
  expect(scan.native.archiveOpenCalls).toBe(scan.native.archiveCloseCalls);
  expect(scan.native.probeOpenCalls).toBe(scan.native.probeCloseCalls);
  expect(scan.native.openHandleCount).toBe(0);
  expect(scan.native.openFdCount).toBe(0);

  expect(initialCancellation.triggered).toBe(true);
  expect(initialCancellation.pageReturned).toBe(false);
  expect(initialCancellation.error?.message).toMatch(/fenced|cancelled/i);
  expect(initialCancellation.error?.hasPageFields).toBe(false);
  expect(initialCancellation.fenceSettled).toBe(true);
  expect(initialCancellation.cost).toBeDefined();
  expectCostMatchesNative(initialCancellation.cost!, initialCancellation.native);
  expect(initialCancellation.cost!.discoveryReadCalls).toBe(initialCancellation.native.discoveryReadCalls);
  expect(initialCancellation.cost!.sourceValidationCalls).toBe(initialCancellation.native.sourceValidationCalls);
  expect(initialCancellation.cost!.maxSliceMs).toBeLessThan(SLICE_BUDGET_MS);
  expect(initialCancellation.timing.maxCheckGapMs).toBeLessThan(SLICE_BUDGET_MS);
  expect(initialCancellation.native.archiveOpenCalls).toBe(initialCancellation.native.archiveCloseCalls);
  expect(initialCancellation.native.probeOpenCalls).toBe(initialCancellation.native.probeCloseCalls);
  expect(initialCancellation.native.openHandleCount).toBe(0);
  expect(initialCancellation.native.openFdCount).toBe(0);
  expect(costParseTotal(initialCancellation.cost!)).toBe(initialCancellation.native.jsonParseAttempts);

  if (resumedCancellation) {
    expect(resumedCancellation.triggered).toBe(true);
    expect(resumedCancellation.pageReturned).toBe(false);
    expect(resumedCancellation.error?.message).toMatch(/fenced|cancelled/i);
    expect(resumedCancellation.error?.hasPageFields).toBe(false);
    expect(resumedCancellation.fenceSettled).toBe(true);
    expect(resumedCancellation.primeTiming?.value?.done).toBe(false);
    expect(resumedCancellation.primeMaxCheckGapMs).toBeLessThan(SLICE_BUDGET_MS);
    expect(resumedCancellation.priorCost?.maxSliceMs).toBeLessThan(SLICE_BUDGET_MS);
    expect(resumedCancellation.priorCost?.archiveReadCalls).toBe(topicCount);
    expect(resumedCancellation.cost).toBeDefined();
    expect(resumedCancellation.cost!.events).toBe(0);
    expect(resumedCancellation.cost!.archiveReadCalls).toBe(0);
    expect(resumedCancellation.cost!.archiveReadBytes).toBe(0);
    expect(resumedCancellation.cost!.sourceValidationCalls).toBe(resumedCancellation.native.sourceValidationCalls);
    expect(resumedCancellation.cost!.sourceValidationCalls).toBeGreaterThan(0);
    expectCostMatchesNative(resumedCancellation.cost!, resumedCancellation.native);
    expect(resumedCancellation.cost!.probeReadCalls).toBe(resumedCancellation.native.probeReadCalls);
    expect(resumedCancellation.cost!.metadataReadCalls).toBe(resumedCancellation.native.metadataReadCalls);
    expect(resumedCancellation.cost!.maxSliceMs).toBeLessThan(SLICE_BUDGET_MS);
    expect(resumedCancellation.timing.maxCheckGapMs).toBeLessThan(SLICE_BUDGET_MS);
    expect(resumedCancellation.native.archiveOpenCalls).toBe(resumedCancellation.native.archiveCloseCalls);
    expect(resumedCancellation.native.probeOpenCalls).toBe(resumedCancellation.native.probeCloseCalls);
    expect(resumedCancellation.native.openHandleCount).toBe(0);
    expect(resumedCancellation.native.openFdCount).toBe(0);
    expect(costParseTotal(resumedCancellation.cost!)).toBe(resumedCancellation.native.jsonParseAttempts);
  }
}, 300_000);
