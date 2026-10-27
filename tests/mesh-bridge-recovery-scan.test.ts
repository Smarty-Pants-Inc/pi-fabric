import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { performance } from "node:perf_hooks";
import { afterEach, expect, it, vi } from "vitest";
import { MeshBridge, RemoteBridgeSide, StoreBridgeSide, serveBridgeAgent } from "../src/mesh/bridge.js";
import { MESH_ARCHIVE_CONFIG, archiveFileName } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

// Data-safety/performance regression: a quiet link's old destination mark must not
// monopolize the transport thread while searching mostly unrelated archived history.
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-recovery-scan-"));
  roots.push(root);
  return root;
};

const fixture = (count: number, days: number) => {
  const root = scratch(), archive = path.join(root, "history");
  const mark = 1_000_000;
  const from = { id: "fixture-writer", name: "writer", kind: "main" as const };
  let last!: MeshEvent;
  const expected: string[] = [];
  for (let day = 0, index = 0; day < days; day++) {
    const directory = path.join(archive, "2026", "01", String(day + 1).padStart(2, "0"));
    fs.mkdirSync(directory, { recursive: true });
    const topics = new Map<string, MeshEvent[]>();
    const end = Math.floor(count * (day + 1) / days);
    while (index < end) {
      index++;
      const recovered = index === Math.floor(count / 3) || index === count - 1;
      const topic = `fixture.chatter.${index % 3}`;
      last = { id: `fixture-${index}`, sequence: mark + index, createdAt: Date.UTC(2026, 0, day + 1),
        topic, kind: "chatter", from, text: "unrelated event history", ...(recovered ? index === count - 1
          ? { id: `bridge-relay-receipt:spoke:original-${index}`, verification: "bridge" as const }
          : { data: { bridge: { from: "spoke", id: `original-${index}` } } } : {}) };
      if (recovered) expected.push(`original-${index}`);
      const rows = topics.get(topic) ?? [];
      rows.push(last);
      topics.set(topic, rows);
    }
    const files: Record<string, unknown> = {};
    for (const [topic, rows] of topics) {
      const name = archiveFileName(topic);
      fs.writeFileSync(path.join(directory, name), rows.map(row => JSON.stringify(row) + "\n").join(""));
      files[name] = { lines: rows.length, firstSequence: rows[0]!.sequence, lastSequence: rows.at(-1)!.sequence, sha256: "fixture" };
    }
    fs.writeFileSync(path.join(directory, "SEAL.json"), JSON.stringify({ files }));
  }
  // Retention leaves only the head live, forcing the real archived-read path.
  fs.writeFileSync(path.join(root, "events.jsonl"), JSON.stringify(last) + "\n");
  fs.writeFileSync(path.join(root, "sequence"), String(last.sequence));
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
  return { store: new MeshStore(root, 64 * 1024, 500), mark, head: last.sequence, expected };
};

it("recovers 770k old-mark positions across archived days within startup and slice budgets", async () => {
  const f = fixture(770_000, 15);
  const side = new StoreBridgeSide(f.store, "spoke");
  let archiveReads = 0, archiveReadBytes = 0;
  const readFile = fs.readFileSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
    const result = readFile(...args);
    if (String(args[0]).startsWith(path.join(f.store.root, "history")) && String(args[0]).endsWith(".jsonl")) {
      archiveReads++;
      archiveReadBytes += Buffer.byteLength(result);
    }
    return result;
  }) as typeof fs.readFileSync);
  const ids: string[] = [];
  let cursor = f.mark, pages = 0, turns = 0, maxSliceMs = 0;
  let previous = performance.now(), finished = false;
  // A check-phase heartbeat measures thread occupancy, not promise/microtask latency.
  const heartbeat = () => {
    const now = performance.now();
    maxSliceMs = Math.max(maxSliceMs, now - previous);
    previous = now;
    turns++;
    if (!finished) setImmediate(heartbeat);
  };
  setImmediate(heartbeat);
  const started = performance.now();
  try {
    for (;;) {
      const page = await side.bridgedIds(cursor);
      pages++;
      ids.push(...page.ids);
      expect(page.through).toBeGreaterThan(cursor);
      cursor = page.through;
      if (page.done) break;
    }
  } finally {
    finished = true;
    maxSliceMs = Math.max(maxSliceMs, performance.now() - previous);
  }
  const elapsedMs = performance.now() - started;
  console.log(JSON.stringify({ recoveryScale: { positions: 770_000, days: 15, pages, turns, archiveReads, archiveReadBytes, elapsedMs, maxSliceMs } }));
  expect(cursor).toBe(f.head);
  expect(ids).toEqual(f.expected);
  expect(maxSliceMs).toBeLessThan(1_000);
  expect(elapsedMs).toBeLessThan(60_000);
  expect(turns).toBeGreaterThan(1);
}, 300_000);

it("returns advancing empty ID pages and yields transport callbacks before EOF", async () => {
  const f = fixture(30_003, 3);
  const toAgent = new PassThrough(), fromAgent = new PassThrough();
  const side = new StoreBridgeSide(f.store, "spoke");
  const serving = serveBridgeAgent(side, toAgent, fromAgent);
  const remote = new RemoteBridgeSide(fromAgent, toAgent, 1_000);
  try {
    const first = await remote.bridgedIds(f.mark);
    expect(first.done).toBe(false);
    expect(first.ids).toEqual([]);
    expect(first.through).toBeGreaterThan(f.mark);
    expect(first.through).toBeLessThan(f.head);
    // Old protocol-2 callers already accept short pages, including empty IDs.
    const seen = new Set(first.ids);
    let cursor = first.through;
    for (;;) {
      const page = await remote.bridgedIds(cursor);
      page.ids.forEach(id => seen.add(id));
      if (page.done) break;
      expect(page.through).toBeGreaterThan(cursor);
      cursor = page.through;
    }
    expect([...seen]).toEqual(f.expected);
  } finally {
    remote.close();
    toAgent.end();
    await serving;
  }
});

const recoveringBridge = (f: ReturnType<typeof fixture>, budgetMs = 60_000) => {
  const remote = new StoreBridgeSide(new MeshStore(scratch(), 64 * 1024, 500), "hub");
  const local = new StoreBridgeSide(f.store, "spoke");
  const cursorPath = path.join(scratch(), "cursor.json");
  const saved = JSON.stringify({ format: 1, local: "hub", remote: "spoke",
    toLocal: { after: 0, mark: f.mark }, toRemote: { after: f.head, mark: 0 } });
  fs.writeFileSync(cursorPath, saved);
  const logs: string[] = [];
  const bridge = new MeshBridge({ localName: "hub", remoteName: "spoke", local, remote, cursorPath,
    startDeadlineMs: budgetMs, log: line => logs.push(line) });
  return { bridge, local, logs, cursorPath, saved };
};

it("cancels recovery between pages without changing saved recovery coordinates", async () => {
  const f = fixture(30_003, 3), b = recoveringBridge(f);
  const read = b.local.bridgedIds.bind(b.local);
  let stop: Promise<void> | undefined;
  const spy = vi.spyOn(b.local, "bridgedIds").mockImplementation(async after => {
    const page = await read(after);
    setImmediate(() => { stop = b.bridge.stop(); });
    return page;
  });
  const started = performance.now();
  await expect(b.bridge.start()).rejects.toThrow(/cancelled|fenced/);
  await stop;
  expect(performance.now() - started).toBeLessThan(5_000);
  expect(spy).toHaveBeenCalledTimes(1);
  expect(fs.readFileSync(b.cursorPath, "utf8")).toBe(b.saved);
  expect(b.logs.join("\n")).toMatch(/recovery \(toLocal\): .*completed=false cancelled=true/);
});

it("checks the startup budget inside recovery, without granting readiness or rebasing the mark", async () => {
  const f = fixture(30_003, 3), b = recoveringBridge(f, 20);
  const read = b.local.bridgedIds.bind(b.local);
  vi.spyOn(b.local, "bridgedIds").mockImplementation(async after => {
    await new Promise(resolve => setTimeout(resolve, 25));
    return read(after);
  });
  try {
    await expect(b.bridge.start()).rejects.toThrow(/startup budget/);
    expect(fs.readFileSync(b.cursorPath, "utf8")).toBe(b.saved);
    expect(b.logs.join("\n")).toMatch(/completed=false cancelled=true/);
  } finally { await b.bridge.stop(); }
});

it("logs the complete archived bridge/relay recovery cost and leaves the destination mark unchanged", async () => {
  const f = fixture(30_003, 3), b = recoveringBridge(f);
  try {
    await b.bridge.start();
    expect(b.logs.join("\n")).toMatch(/recovery \(toLocal\): mark=1000000 horizon=1030003 pages=4 inspectedEvents=30003 eventBytes=\d+ maxSliceMs=\d+ matchingIds=2 elapsedMs=\d+ completed=true cancelled=false/);
    expect(JSON.parse(fs.readFileSync(b.cursorPath, "utf8")).toLocal.mark).toBe(f.mark);
  } finally { await b.bridge.stop(); }
});
