import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-idle-hints-"));
  roots.push(root);
  const events = Array.from({ length: 2_000 }, (_, i): MeshEvent => ({
    id: `event-${i + 1}`, sequence: i + 1, topic: i % 2 ? "even" : "odd", kind: "message",
    from: { id: "writer", name: "writer", kind: "main" }, text: "x".repeat(600), createdAt: i,
  }));
  const log = path.join(root, "events.jsonl");
  fs.writeFileSync(log, events.map(event => JSON.stringify(event) + "\n").join(""));
  fs.writeFileSync(path.join(root, "generation"), "0");
  return { root, events, log, store: new MeshStore(root, 64 * 1024, 500) };
};
const readBytes = (run: () => MeshEvent[]) => {
  const reads = vi.spyOn(fs, "readSync");
  try {
    const events = run();
    return { events, bytes: reads.mock.results.reduce((sum, result) => sum + Number(result.value), 0) };
  } finally { reads.mockRestore(); }
};

describe("idle readers behind the recent event hint window (smarty-dev#2039)", () => {
  it("keeps several paused boundaries across interleaved pages and appends, including filters", () => {
    const { store, log, events } = fixture();
    store.read({ after: 1_000, limit: 500 });
    store.read({ after: 1_200, limit: 500 });
    store.read({ after: 1_500, limit: 500 });
    const size = fs.statSync(log).size;
    for (const after of [1_000, 1_200, 1_500, 1_000]) {
      const read = readBytes(() => store.read({ after, limit: 500 }));
      expect(read.events).toEqual(events.slice(after, after + 500));
      expect(read.bytes).toBeLessThan(size * 0.55); // suffix only, not 1.5 MB historical prefix
    }
    const appended = { ...events[0]!, id: "appended", sequence: 2_001 };
    fs.appendFileSync(log, JSON.stringify(appended) + "\n");
    const reference = new MeshStore(store.root, 64 * 1024, 500);
    for (const input of [{ after: 1_000, topic: "even", limit: 200 }, { after: 1_200, limit: 500 }, { after: 2_000 }]) {
      expect(store.read(input)).toEqual(reference.read(input));
    }
    expect(store.read({ after: 2_000 })).toEqual([appended]);
  });

  it("invalidates paused anchors on generation changes and atomic replacement", () => {
    const { store, log, events, root } = fixture();
    store.read({ after: 1_000, limit: 500 }); // its boundary is outside the recent 128 lines
    const retained = events.slice(950);
    fs.writeFileSync(log, retained.map(event => JSON.stringify(event) + "\n").join(""));
    fs.writeFileSync(path.join(root, "generation"), "1");
    expect(store.read({ after: 1_000, limit: 500 })).toEqual(events.slice(1_000, 1_500));
    const replacement = path.join(root, "replacement.jsonl");
    fs.writeFileSync(replacement, events.slice(990).map(event => JSON.stringify(event) + "\n").join(""));
    fs.renameSync(replacement, log); // new inode without a generation bump
    expect(store.read({ after: 1_000, limit: 500 })).toEqual(events.slice(1_000, 1_500));
  });

  it("falls back to a full scan when an in-place rewrite moves a paused line boundary", () => {
    const { store, log, events } = fixture();
    store.read({ after: 1_000, limit: 500 });
    // Remove one byte from the first line, shifting every remembered offset off its newline.
    fs.writeFileSync(log, fs.readFileSync(log, "utf8").replace('"' + "x".repeat(600) + '"', '"' + "x".repeat(599) + '"'));
    expect(store.read({ after: 1_000, limit: 500 })).toEqual(events.slice(1_000, 1_500));
  });

  it("rechecks an EOF anchor for a new event without rescanning old events", () => {
    const { store, log, events } = fixture();
    store.read({ after: 2_000 });
    expect(readBytes(() => store.read({ after: 2_000 })).bytes).toBeLessThan(100);
    const event = { ...events[0]!, id: "next", sequence: 2_001 };
    fs.appendFileSync(log, JSON.stringify(event) + "\n");
    const next = readBytes(() => store.read({ after: 2_000 }));
    expect(next.events).toEqual([event]);
    expect(next.bytes).toBeLessThan(2_000);
  });
});
