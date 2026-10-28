import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { archiveFileName, MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import type { RecoveryReadCost } from "../src/mesh/recovery-reader.js";

const roots: string[] = [];
const PEER = "recovery-peer";
const TOPIC_PREFIX = "mesh.recovery.cost.";
const DAY = "2025/01/01";
const writer = { id: "test-writer", name: "test-writer", kind: "main" as const };
const row = (sequence: number, topic = `${TOPIC_PREFIX}${sequence % 3}`): MeshEvent => ({
  id: `mesh-event-${sequence}`,
  sequence,
  topic,
  kind: "message",
  from: writer,
  createdAt: Date.UTC(2025, 0, 1),
  data: { bridge: { from: PEER, id: `origin-${sequence}` } },
});
const raw = (event: MeshEvent): string => `${JSON.stringify(event)}\n`;
const digest = (bytes: string): string => createHash("sha256").update(bytes).digest("hex");
const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-recovery-cost-"));
  roots.push(root);
  return root;
};

afterEach(() => {
  vi.restoreAllMocks();
  syncBuiltinESMExports();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function fixture(rows: MeshEvent[], liveHead: MeshEvent) {
  const root = scratch();
  const archive = path.join(root, "history");
  const directory = path.join(archive, DAY);
  fs.mkdirSync(directory, { recursive: true });
  const topics = new Map<string, MeshEvent[]>();
  for (const event of rows) {
    const topicRows = topics.get(event.topic) ?? [];
    topicRows.push(event);
    topics.set(event.topic, topicRows);
  }
  const files: Record<string, unknown> = {};
  for (const [topic, topicRows] of topics) {
    const bytes = topicRows.map(raw).join("");
    const name = archiveFileName(topic);
    fs.writeFileSync(path.join(directory, name), bytes);
    files[name] = {
      lines: topicRows.length,
      firstSequence: topicRows[0]!.sequence,
      lastSequence: topicRows.at(-1)!.sequence,
      sha256: digest(bytes),
    };
  }
  fs.writeFileSync(path.join(directory, "SEAL.json"), `${JSON.stringify({ version: 1, day: DAY, files })}\n`);
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
  fs.writeFileSync(path.join(root, "events.jsonl"), raw(liveHead));
  fs.writeFileSync(path.join(root, "sequence"), String(liveHead.sequence));
  const store = new MeshStore(root, 64 * 1024, 500);
  const side = new StoreBridgeSide(store, PEER);
  return { root, archive, store, side };
}

interface ArchiveWorkMeter {
  archiveOpenCalls: number;
  archiveReadCalls: number;
  archiveReadBytes: number;
  archiveCloseCalls: number;
  fixtureEventParses: number;
}

function observeArchiveWork(
  archive: string,
  onIO?: (kind: "open" | "read", readCalls: number) => void,
) {
  const nativeOpen = fs.promises.open;
  const nativeParse = JSON.parse;
  const handles: Array<Awaited<ReturnType<typeof nativeOpen>>> = [];
  const meter: ArchiveWorkMeter = {
    archiveOpenCalls: 0,
    archiveReadCalls: 0,
    archiveReadBytes: 0,
    archiveCloseCalls: 0,
    fixtureEventParses: 0,
  };
  let restored = false;
  JSON.parse = function (text: string, ...args: any[]) {
    const value = Reflect.apply(nativeParse, JSON, [text, ...args]);
    if (value && typeof value === "object" && typeof value.topic === "string" &&
        value.topic.startsWith(TOPIC_PREFIX) && Number.isSafeInteger(value.sequence)) {
      meter.fixtureEventParses++;
    }
    return value;
  } as typeof JSON.parse;
  fs.promises.open = async (...args) => {
    const handle = await nativeOpen(...args);
    if (typeof args[0] === "string" && args[0].startsWith(`${archive}${path.sep}`) && args[0].endsWith(".jsonl")) {
      meter.archiveOpenCalls++;
      handles.push(handle);
      const nativeRead = handle.read.bind(handle);
      const nativeClose = handle.close.bind(handle);
      handle.read = (async (...readArgs: any[]) => {
        const result = await Reflect.apply(nativeRead, handle, readArgs);
        meter.archiveReadCalls++;
        meter.archiveReadBytes += result.bytesRead;
        onIO?.("read", meter.archiveReadCalls);
        return result;
      }) as typeof handle.read;
      handle.close = async () => {
        meter.archiveCloseCalls++;
        return nativeClose();
      };
      onIO?.("open", meter.archiveReadCalls);
    }
    return handle;
  };
  syncBuiltinESMExports();
  return {
    meter,
    handles,
    restore() {
      if (restored) return;
      restored = true;
      fs.promises.open = nativeOpen;
      JSON.parse = nativeParse;
      syncBuiltinESMExports();
    },
  };
}

function logMetric(line: string, name: string): number {
  const match = line.match(new RegExp(`(?:^|\\s)${name}=(\\d+)(?:\\s|$)`));
  expect(match, `recovery log must include ${name}: ${line}`).not.toBeNull();
  return Number(match![1]);
}

function expectNativeDiagnostic(cost: RecoveryReadCost | undefined, meter: ArchiveWorkMeter): RecoveryReadCost {
  expect(cost, "the public side diagnostic must retain attempted recovery cost").toBeDefined();
  const actual = cost!;
  expect(actual.archiveReadCalls).toBe(meter.archiveReadCalls);
  expect(actual.archiveReadBytes).toBe(meter.archiveReadBytes);
  expect(actual.archiveReadCalls).toBeGreaterThan(0);
  expect(actual.archiveReadBytes).toBeGreaterThan(0);
  expect(actual.archiveParseAttempts).toBeGreaterThan(0);
  expect(meter.fixtureEventParses).toBeGreaterThanOrEqual(actual.archiveParseAttempts);
  return actual;
}

function expectCancellationLog(
  line: string,
  cost: RecoveryReadCost,
  meter: ArchiveWorkMeter,
): void {
  expect(line).toContain("pages=0");
  expect(line).toContain("completed=false");
  expect(line).toContain("cancelled=true");
  expect(logMetric(line, "attemptedPages")).toBe(1);
  expect(logMetric(line, "inspectedEvents")).toBe(cost.events);
  expect(logMetric(line, "eventBytes")).toBe(cost.eventBytes);
  expect(logMetric(line, "archiveReadCalls")).toBe(meter.archiveReadCalls);
  expect(logMetric(line, "archiveReadBytes")).toBe(meter.archiveReadBytes);
  expect(logMetric(line, "archiveParseAttempts")).toBe(cost.archiveParseAttempts);
}

function recoveryLog(logs: string[]): string {
  const line = logs.find(value => value.startsWith("recovery (toLocal): "));
  expect(line, "MeshBridge.start must emit the local recovery result").toBeDefined();
  return line!;
}

type Mode = "success" | "completed-page" | "partial-read" | "open-handle";
type ReaderPage = {
  ids: string[];
  through: number;
  done: boolean;
  cost: RecoveryReadCost;
};
type AdapterPage = Awaited<ReturnType<StoreBridgeSide["bridgedIds"]>>;

async function bootstrapCase(mode: Mode) {
  const rows = Array.from({ length: 20_001 }, (_, index) => row(index + 1));
  const local = fixture(rows, rows.at(-1)!);
  const remote = new StoreBridgeSide(new MeshStore(scratch(), 64 * 1024, 500), "recovery-hub");
  const cursorPath = path.join(local.root, "cursor.json");
  const savedCursor = JSON.stringify({
    format: 1,
    local: "recovery-hub",
    remote: PEER,
    toLocal: { after: 0, mark: 0 },
    toRemote: { after: 20_001, mark: 0 },
  });
  fs.writeFileSync(cursorPath, savedCursor);

  const logs: string[] = [];
  const readerPages: ReaderPage[] = [];
  const adapterPages: AdapterPage[] = [];
  const adapterErrors: unknown[] = [];
  let fence: Promise<void> | undefined;

  const createReader = local.store.createRecoveryReader.bind(local.store);
  vi.spyOn(local.store, "createRecoveryReader").mockImplementation((project, signal) => {
    const reader = createReader(project, signal);
    const read = reader.read.bind(reader);
    reader.read = (after: number) => read(after).then(page => {
      readerPages.push(page);
      if (mode === "completed-page" && !fence) fence = local.side.fence();
      return page;
    });
    return reader;
  });

  const bridge = new MeshBridge({
    localName: "recovery-hub",
    remoteName: PEER,
    local: local.side,
    remote,
    cursorPath,
    log: line => logs.push(line),
  });
  const bridgedIds = local.side.bridgedIds.bind(local.side);
  vi.spyOn(local.side, "bridgedIds").mockImplementation(async after => {
    try {
      const page = await bridgedIds(after);
      adapterPages.push(page);
      return page;
    } catch (error) {
      adapterErrors.push(error);
      throw error;
    }
  });

  const observation = observeArchiveWork(local.archive, (kind, readCalls) => {
    if (fence) return;
    if (mode === "open-handle" && kind === "open") fence = local.side.fence();
    if (mode === "partial-read" && kind === "read" && readCalls === 2) fence = local.side.fence();
  });
  let startError: unknown;
  let started = false;
  let relayError: { code?: string } | undefined;
  try {
    try {
      await bridge.start();
      started = true;
    } catch (error) {
      startError = error;
    }

    if (mode !== "success") {
      expect(fence, "the requested cancellation boundary must be reached").toBeDefined();
      await fence;
      try {
        await bridge.deliverRelay("other-peer", {
          topic: "fabric.control.command",
          kind: "command",
          from: writer,
          to: "session:recipient",
          data: { bridge: { from: "other-peer", id: "readiness-probe" } },
        });
      } catch (error) {
        relayError = error as { code?: string };
      }
    }

    const cursorUnchanged = fs.readFileSync(cursorPath, "utf8") === savedCursor;
    const openFDs = observation.handles.filter(handle => handle.fd !== -1).length;
    expect(openFDs).toBe(0);
    expect(observation.meter.archiveCloseCalls).toBe(observation.meter.archiveOpenCalls);
    expect(cursorUnchanged).toBe(true);

    if (mode === "success") {
      expect(startError).toBeUndefined();
      expect(started).toBe(true);
      expect(adapterErrors).toHaveLength(0);
      expect(adapterPages.map(page => page.through)).toEqual([10_000, 20_000, 20_001]);
      expect(readerPages.map(page => page.through)).toEqual([10_000, 20_000, 20_001]);
      expect(readerPages.reduce((sum, page) => sum + page.cost.events, 0)).toBe(20_001);
      expect(local.side.recoveryReadCost?.events).toBe(1);

      const line = recoveryLog(logs);
      expect(line).toContain("pages=3");
      expect(line).toContain("completed=true");
      expect(line).toContain("cancelled=false");
      expect(logMetric(line, "inspectedEvents")).toBe(20_001);
      expect(logMetric(line, "archiveReadCalls")).toBe(observation.meter.archiveReadCalls);
      expect(logMetric(line, "archiveReadBytes")).toBe(observation.meter.archiveReadBytes);

      const beforeFence = { ...observation.meter };
      await local.side.fence();
      await expect(local.side.bridgedIds(0)).rejects.toThrow(/fenced|cancelled/i);
      expect(observation.meter).toEqual(beforeFence);
      expect(local.side.recoveryReadCost).toBeUndefined();
    } else {
      expect(startError).toBeInstanceOf(Error);
      expect(started).toBe(false);
      expect(adapterPages).toHaveLength(0);
      expect(adapterErrors).toHaveLength(1);
      for (const key of ["ids", "through", "done"]) {
        expect(adapterErrors[0]).not.toHaveProperty(key);
      }
      expect(relayError?.code).toBe("FABRIC_RELAY_DESTINATION_UNAVAILABLE");
      expect(cursorUnchanged).toBe(true);

      const line = recoveryLog(logs);
      if (mode === "completed-page") {
        expect(readerPages).toHaveLength(1);
        expect(readerPages[0]!.ids).toHaveLength(10_000);
        expect(readerPages[0]!.through).toBe(10_000);
        expect(readerPages[0]!.done).toBe(false);
        expect(readerPages[0]!.cost.events).toBe(10_000);
        const cost = expectNativeDiagnostic(local.side.recoveryReadCost, observation.meter);
        expect(cost.events).toBe(10_000);
        expectCancellationLog(line, cost, observation.meter);
      } else if (mode === "partial-read") {
        expect(readerPages).toHaveLength(0);
        expect(observation.meter.archiveReadCalls).toBe(2);
        const cost = expectNativeDiagnostic(local.side.recoveryReadCost, observation.meter);
        // During priming, parsed/read work may precede any popped page position; bytes and calls
        // are the attempted-work proof here, not a requirement that cost.events be nonzero.
        expectCancellationLog(line, cost, observation.meter);
      } else {
        expect(readerPages).toHaveLength(0);
        expect(observation.meter.archiveOpenCalls).toBeGreaterThan(0);
        expect(observation.meter.archiveReadCalls).toBe(0);
        expect(observation.meter.archiveReadBytes).toBe(0);
        expect(local.side.recoveryReadCost?.archiveReadCalls).toBe(0);
        expect(local.side.recoveryReadCost?.archiveReadBytes).toBe(0);
        expect(line).toContain("pages=0");
        expect(line).toContain("cancelled=true");
        expect(logMetric(line, "archiveReadCalls")).toBe(0);
        expect(logMetric(line, "archiveReadBytes")).toBe(0);
      }
    }

    return {
      mode,
      startError: startError instanceof Error ? { name: startError.name, message: startError.message } : null,
      readerPages: readerPages.map(page => ({ through: page.through, done: page.done, cost: page.cost })),
      adapterPages: adapterPages.map(page => ({ through: page.through, done: page.done, ids: page.ids.length })),
      publicCost: local.side.recoveryReadCost ? { ...local.side.recoveryReadCost } : undefined,
      native: { ...observation.meter },
      openFDs,
      cursorUnchanged,
      relayErrorCode: relayError?.code,
      logs,
    };
  } finally {
    observation.restore();
    await bridge.stop();
  }
}

it("control: uncancelled bootstrap returns ordered pages and logs actual native work; pre-work fence does no new I/O", async () => {
  await bootstrapCase("success");
});

it("retains cost when a completed real page is fenced before the adapter post-await check", async () => {
  await bootstrapCase("completed-page");
});

it("retains truthful partial native archive cost when cancellation interrupts the first page", async () => {
  await bootstrapCase("partial-read");
});

it("control: fencing after archive open closes the handle without reading or publishing a page", async () => {
  await bootstrapCase("open-handle");
});
