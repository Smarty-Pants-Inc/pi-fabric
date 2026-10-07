import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";

const roots: string[] = [];
const from: MeshIdentity = { id: "session:tail0000", name: "main", kind: "main" };
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bridge-tail-"));
  roots.push(root);
  return root;
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("durable bridge byte cursors (smarty-dev#2854)", () => {
  it("bounds idle/restart event reads on two copies of a 50 MiB log without sequence hints", async () => {
    const root = scratch();
    const original = path.join(root, "synthetic.jsonl");
    const descriptor = fs.openSync(original, "w");
    let sequence = 0;
    let bytes = 0;
    while (bytes < 50 * 1024 * 1024) {
      const event: MeshEvent = { id: `fleet-${++sequence}`, sequence, topic: "fleet.telemetry", kind: "event", from,
        createdAt: Date.now(), text: "x".repeat(850) };
      const line = JSON.stringify(event) + "\n";
      fs.writeSync(descriptor, line);
      bytes += Buffer.byteLength(line);
    }
    fs.closeSync(descriptor);
    const hubRoot = path.join(root, "hub"), farRoot = path.join(root, "far");
    for (const target of [hubRoot, farRoot]) {
      fs.mkdirSync(target);
      fs.copyFileSync(original, path.join(target, "events.jsonl"));
      fs.writeFileSync(path.join(target, "sequence"), String(sequence));
    }
    const cursorPath = path.join(root, "cursor.json");
    // Fresh Store instances on every restart ensure an in-memory hint cannot mask a rescan.
    const create = () => new MeshBridge({ localName: "dev1", remoteName: "forge", cursorPath, presenceMs: 60_000,
      local: new StoreBridgeSide(new MeshStore(hubRoot, 256 * 1024, 500), "forge"),
      remote: new StoreBridgeSide(new MeshStore(farRoot, 256 * 1024, 500), "dev1") });
    const reads = vi.spyOn(fs, "readSync");
    const first = create();
    await first.start();
    // Startup reads only bounded end-of-file anchors, not 100 MiB of history.
    expect(reads.mock.results.reduce((sum, result) => sum + Number(result.value), 0))
      .toBeLessThan(2 * 1024 * 1024);
    await first.step();
    const saved = fs.readFileSync(cursorPath, "utf8");
    expect(JSON.parse(saved).toRemote.offset).toBe(bytes);
    await first.stop();
    const second = create();
    await second.start();
    await second.step();
    reads.mockClear();
    for (let poll = 0; poll < 40; poll++) {
      expect(await second.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
    }
    expect(reads).toHaveBeenCalledTimes(80); // exactly one line-boundary byte per side/poll
    expect((reads.mock.calls as unknown[][]).every(call => call[3] === 1 && (call[4] as number) > 0)).toBe(true);
    expect(fs.readFileSync(cursorPath, "utf8")).toBe(saved); // no idle checkpoints
    await second.stop();
  });

  it("negotiates tail RPC and falls back for a legacy v1 agent", async () => {
    const store = new MeshStore(scratch(), 4096, 100);
    const input = new PassThrough(), output = new PassThrough();
    const serving = serveBridgeAgent(new StoreBridgeSide(store, "dev1"), input, output);
    const remote = new RemoteBridgeSide(output, input);
    try {
      await remote.hello();
      const head = await remote.latestCursor();
      await store.publish({ topic: "fleet.work.test", from, to: "remote", text: "new" });
      const page = await remote.tail(head.through, head.offset);
      expect(page.events.map(e => e.text)).toEqual(["new"]);
      expect(page.offset).toBeGreaterThan(head.offset!);
    } finally { remote.close(); input.end(); await serving; }

    const legacyIn = new PassThrough(), legacyOut = new PassThrough();
    const operations: string[] = [];
    legacyIn.on("data", chunk => {
      const request = JSON.parse(String(chunk));
      operations.push(request.op);
      const result = request.op === "hello" ? { version: 1 } : request.op === "latestSequence" ? 7 : { events: [], through: request.args.after };
      legacyOut.write(JSON.stringify({ id: request.id, ok: true, result }) + "\n");
    });
    const legacy = new RemoteBridgeSide(legacyOut, legacyIn);
    try {
      await legacy.hello();
      expect(await legacy.latestCursor()).toEqual({ through: 7 });
      expect(await legacy.tail(7, 123)).toEqual({ events: [], through: 7 });
      expect(operations).toEqual(["hello", "latestSequence", "read"]);
    } finally { legacy.close(); }
  });

  it("migrates a sequence-only cursor without skipping a concurrent append", async () => {
    const store = new MeshStore(scratch(), 4096, 100);
    const side = new StoreBridgeSide(store, "forge");
    const first = await store.publish({ topic: "fleet.work.test", from, to: "remote", text: "first" });
    const read = side.read.bind(side);
    let late: MeshEvent | undefined;
    vi.spyOn(side, "read").mockImplementation(async (...args) => {
      const page = await read(...args);
      late = await store.publish({ topic: "fleet.work.test", from, to: "remote", text: "late" });
      return page;
    });
    const page = await side.tail(0);
    expect(page.events.map(e => e.id)).toEqual([first.id]);
    expect((await side.tail(page.through, page.offset)).events.map(e => e.id)).toEqual([late!.id]);
  });

  it("does not adopt an unreadable last line as a sequence/offset anchor", async () => {
    const store = new MeshStore(scratch(), 4096, 100);
    const side = new StoreBridgeSide(store, "forge");
    const first = await store.publish({ topic: "fleet.work.test", from, to: "remote", text: "pending" });
    fs.appendFileSync(path.join(store.root, "events.jsonl"), "not-json\n");
    const page = await side.tail(0);
    expect(page.events.map(e => e.id)).toEqual([first.id]);
    expect(page.offset).toBeUndefined();
    const next = await store.publish({ topic: "fleet.work.test", from, to: "remote", text: "next" });
    const resumed = await side.tail(page.through);
    expect(resumed.events.map(e => e.id)).toEqual([next.id]);
    expect(resumed.offset).toBe(store.latestOffset());
  });

  it("stops a byte cursor before a frame-budget boundary and reports oversize skips", async () => {
    const store = new MeshStore(scratch(), 4096, 100);
    const side = new StoreBridgeSide(store, "forge");
    const head = await side.latestCursor();
    for (const text of ["a".repeat(2500), "b".repeat(600), "c".repeat(600)]) {
      await store.publish({ topic: "fleet.work.test", from, to: "remote", text });
    }
    const page = await side.tail(head.through, head.offset, 1000);
    expect(page.skipped).toHaveLength(1);
    expect(page.events.map(e => e.text?.[0])).toEqual(["b"]);
    expect(page.through).toBe(2);
    const next = await side.tail(page.through, page.offset, 1000);
    expect(next.events.map(e => e.text?.[0])).toEqual(["c"]);
    expect(next.through).toBe(3);
  });

  it.each([false, true])("passes reservation holes without sequence reconciliation (archive=%s)", async (archived) => {
    const root = scratch();
    if (archived) {
      const archive = scratch();
      fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
    }
    const store = new MeshStore(root, 4096, 100);
    const side = new StoreBridgeSide(store, "forge");
    await store.publish({ topic: "fleet.work.test", from, to: "remote", text: "handled" });
    const head = await side.latestCursor();
    // A crashed publisher may reserve sequences without committing events.
    fs.writeFileSync(path.join(root, "sequence"), "8");
    const next = await store.publish({ topic: "fleet.work.test", from, to: "remote", text: "after hole" });
    expect(next.sequence).toBe(9);
    const lookup = vi.spyOn(store, "nextEventAfter");
    const reconcile = vi.spyOn(side, "read");
    const page = await side.tail(head.through, head.offset);
    expect(page.events.map(event => event.id)).toEqual([next.id]);
    expect(page.through).toBe(next.sequence);
    expect(page.offset).toBe(store.latestOffset());
    expect(lookup).toHaveBeenCalledWith(head.through);
    expect(reconcile).not.toHaveBeenCalled();
    lookup.mockClear();
    expect(await side.tail(page.through, page.offset)).toEqual({ events: [], through: page.through, offset: page.offset });
    expect(lookup).not.toHaveBeenCalled(); // idle polls remain byte-only
  });

  it("reconciles generation changes through the archive, including work removed from the live log", async () => {
    const root = scratch(), archive = path.join(root, "archive");
    fs.mkdirSync(archive);
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
    const store = new MeshStore(root, 1024, 3, { maxEventLogBytes: 3000, retainedEventLogBytes: 1200 });
    const side = new StoreBridgeSide(store, "forge");
    const head = await side.latestCursor();
    for (let i = 0; i < 10; i++) await store.publish({ topic: "fleet.work.test", from, to: "remote", text: String(i).repeat(500) });
    await store.settleCompaction();
    expect(store.oldestSequence()).toBeGreaterThan(1);
    let after = head.through, offset: number | undefined = head.offset;
    const texts: string[] = [];
    for (let pageNo = 0; pageNo < 10; pageNo++) {
      const page = await side.tail(after, offset);
      texts.push(...page.events.map(e => e.text!));
      if (page.through === after && page.offset === offset) break;
      after = page.through; offset = page.offset;
    }
    expect(texts.map(t => t[0])).toEqual(Array.from({ length: 10 }, (_, i) => String(i)));
    expect(offset).toBe(store.latestOffset());
  });
});
