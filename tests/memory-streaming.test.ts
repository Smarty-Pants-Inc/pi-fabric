import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { buildSync } from "esbuild";
import { describe, expect, it, vi } from "vitest";
import { createFileSystemMemorySource } from "../src/memory/fs-source.js";
import { reconstructSessionLineage } from "../src/memory/lineage.js";
import { readJsonlRecords } from "../src/memory/jsonl.js";
import { normalizeRecords, normalizeSession } from "../src/memory/normalize.js";
import { probeMemorySnapshot } from "./fixtures/memory-snapshot-probe.js";

const tempFile = (lines: readonly string[]): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-memory-streaming-"));
  const file = path.join(root, "session.jsonl");
  fs.writeFileSync(file, lines.join("\n") + "\n", "utf8");
  return file;
};

const message = (id: string, parentId: string | null, text: string) => ({
  type: "message",
  id,
  parentId,
  message: { role: "user", content: text },
});

describe("memory JSONL streaming", () => {
  it("streams invalid lines without changing header, ordinal, or duplicate-id behavior", () => {
    const file = tempFile([
      JSON.stringify({ type: "session", id: "streamed", cwd: "/work" }),
      "not json",
      JSON.stringify(["not a record"]),
      JSON.stringify({ type: "model_change", id: "structural", parentId: null }),
      JSON.stringify(message("same", null, "first")),
      JSON.stringify(message("same", "same", "second")),
    ]);
    expect([...readJsonlRecords(file)]).toHaveLength(5);
    const normalized = normalizeSession(file, 1_000);
    expect(normalized.header).toEqual({ sessionId: "streamed", cwd: "/work" });
    expect(normalized.entries.map((entry) => [entry.index, entry.entryId])).toEqual([
      [0, "same"],
      [1, "same"],
    ]);
    expect(normalized.indexCoverage).toEqual({ complete: false, reasons: ["duplicate_entry_id"] });
  });

  it("accepts a one-record-at-a-time iterable and preserves active parent-branch selection", () => {
    const records = [
      { type: "session", id: "branching", cwd: "/work" },
      message("root", null, "root"),
      message("left", "root", "left"),
      message("right", "root", "right"),
      message("leaf", "right", "leaf"),
    ];
    const iterable = (function* () {
      for (const record of records) yield record;
    })();
    const normalized = normalizeRecords(
      iterable,
      { sessionFile: "branching.jsonl" },
      1_000,
      { lineage: reconstructSessionLineage(tempFile(records.map((record) => JSON.stringify(record))), "active") },
    );
    expect(normalized.entries.map((entry) => entry.entryId)).toEqual(["root", "right", "leaf"]);
  });

  it("streams a multibyte UTF-8 boundary and a missing final newline", () => {
    let line = "";
    for (let count = 65_000; count < 66_000; count += 1) {
      const candidate = JSON.stringify(message("boundary", null, "x".repeat(count) + "😀"));
      const emojiOffset = Buffer.from(candidate, "utf8").indexOf(Buffer.from("😀", "utf8"));
      if (emojiOffset % (64 * 1024) >= 64 * 1024 - 3) {
        line = candidate;
        break;
      }
    }
    expect(line).not.toBe("");
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-memory-boundary-"));
    const file = path.join(root, "boundary.jsonl");
    fs.writeFileSync(file, line, "utf8");
    const records = [...readJsonlRecords(file)];
    expect(records).toHaveLength(1);
    expect((records[0] as { message: { content: string } }).message.content.endsWith("😀")).toBe(true);
  });

  it("preserves the historical decoded-UTF8 revision and full records for malformed bytes", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-memory-revision-"));
    const file = path.join(root, "malformed.jsonl");
    const header = { type: "session", id: "bytes", cwd: "/work" };
    const full = message("full", "replacement", "x".repeat(70_000) + "😀");
    const raw = Buffer.concat([
      Buffer.from(JSON.stringify(header) + "\r\n \n[\"not a record\"]\n", "utf8"),
      Buffer.from([0x7b, 0xff, 0x0a]),
      Buffer.from('{"type":"message","id":"replacement","parentId":null,"message":{"role":"user","content":"', "utf8"),
      Buffer.from([0xff]),
      Buffer.from('"}}\n{torn\n' + JSON.stringify(full), "utf8"),
    ]);
    fs.writeFileSync(file, raw);
    const decodedRevision = crypto.createHash("sha256").update(raw.toString("utf8")).digest("hex");
    expect(decodedRevision).not.toBe(crypto.createHash("sha256").update(raw).digest("hex"));
    const source = createFileSystemMemorySource({ id: "bytes", root });
    const listed = await source.listSessions({ limit: 1 });
    const sessions = "sessions" in listed ? listed.sessions : listed;
    expect(sessions[0]?.revision).toBe(decodedRevision);
    const snapshot = await source.loadSession(path.basename(file), {});
    expect(snapshot?.revision).toBe(decodedRevision);
    expect(snapshot?.records).toEqual([header, message("replacement", null, "\uFFFD"), full]);
    expect(snapshot?.sessionId).toBe("bytes");
    expect(snapshot?.metadata?.cwd).toBe("/work");
  });

  // Data-safety regression for delta-audit finding 3. The standalone fixture
  // uses public loadSession, real streams, and an append at hash.digest.
  it("pairs the revision with exactly the records read before an append at hash completion", async () => {
    const result = await probeMemorySnapshot();
    expect(result.appended).toBe(true);
    expect(result.fileAppended).toBe(true);
    // Seeded two-pass candidate hashes the old file but returns new records:
    // this assertion is RED without relying only on the number of reads.
    expect(result.revision).toBe(result.expectedRevision);
    expect(result.consistent).toBe(true);
    expect(result.fullRecords).toBe(true);
    expect(result.hasAppend).toBe(false);
    expect(result.records).toEqual(result.initial);
  });

  it.each(["error", "abort"] as const)("releases the load stream and descriptor on %s without returning partial records", async (failure) => {
    const file = tempFile([
      JSON.stringify({ type: "session", id: "cleanup", cwd: "/work" }),
      JSON.stringify(message("full", null, "x".repeat(1_024))),
    ]);
    const source = createFileSystemMemorySource({ id: "cleanup", root: path.dirname(file) });
    const controller = new AbortController();
    const reason = new Error(`injected read ${failure}`);
    const nativeCreate = fs.createReadStream;
    const streams: fs.ReadStream[] = [];
    const descriptors: number[] = [];
    const closes: Promise<void>[] = [];
    const seam = vi.spyOn(fs, "createReadStream").mockImplementation((name, options) => {
      const readOptions = typeof options === "string" ? { encoding: options } : options;
      const input = nativeCreate(name, { ...readOptions, highWaterMark: 64 });
      streams.push(input);
      input.once("open", (fd) => descriptors.push(fd));
      closes.push(new Promise<void>((resolve) => input.once("close", () => resolve())));
      input.once("data", () => {
        if (failure === "abort") controller.abort(reason);
        else input.destroy(reason);
      });
      return input;
    });
    try {
      const loading = source.loadSession(path.basename(file), { signal: controller.signal });
      if (failure === "abort") await expect(loading).rejects.toBe(reason);
      else await expect(loading).resolves.toBeNull();
      await Promise.all(closes);
      expect(streams).toHaveLength(1);
      expect(streams[0]?.destroyed).toBe(true);
      expect(streams[0]?.closed).toBe(true);
      expect(descriptors).toHaveLength(1);
      expect(() => fs.fstatSync(descriptors[0]!)).toThrow();
    } finally {
      seam.mockRestore();
      for (const input of streams) input.destroy();
      await Promise.all(closes);
    }
  });

  it("loads the filesystem source through its async stream path without losing records", async () => {
    const file = tempFile([
      JSON.stringify({ type: "session", id: "source", cwd: "/work" }),
      JSON.stringify(message("one", null, "one")),
      "{torn",
      JSON.stringify(message("two", "one", "two")),
    ]);
    const source = createFileSystemMemorySource({ id: "stream", root: path.dirname(file) });
    const snapshot = await source.loadSession(path.basename(file), {});
    expect(snapshot?.records.map((record) => record.id ?? record.type)).toEqual(["source", "one", "two"]);
  });

  it("keeps the actual Node filesystem source load below the 2x 50MiB heap-growth budget", () => {
    const bundleDir = path.resolve(".local/out");
    fs.mkdirSync(bundleDir, { recursive: true });
    const bundle = path.join(bundleDir, "memory-heap-probe.mjs");
    buildSync({
      entryPoints: [path.resolve("tests/fixtures/memory-heap-probe.ts")],
      bundle: true,
      format: "esm",
      platform: "node",
      packages: "external",
      outfile: bundle,
    });
    const runProbe = (mode: "baseline" | "streaming" | "normalize" | "lineage") => spawnSync("node", ["--expose-gc", bundle, mode], {
      encoding: "utf8",
      timeout: 30_000,
      env: process.env,
    });
    const baselineResult = runProbe("baseline");
    expect(baselineResult.status, baselineResult.stderr).toBe(0);
    const baseline = JSON.parse(baselineResult.stdout.trim()) as { bytes: number; records: number; parseSamples: number; heapDelta: number };
    expect(baseline.parseSamples).toBeGreaterThan(1);
    expect(baseline.bytes).toBeGreaterThanOrEqual(50 * 1024 * 1024);
    expect(baseline.records).toBeGreaterThan(1);
    expect(baseline.heapDelta).toBeGreaterThan(100 * 1024 * 1024);

    for (const mode of ["streaming", "normalize", "lineage"] as const) {
      const result = runProbe(mode);
      expect(result.status, `${mode}: ${result.stderr}`).toBe(0);
      const probe = JSON.parse(result.stdout.trim()) as { bytes: number; records: number; listed: number; parseSamples: number; heapDelta: number };
      expect(probe.parseSamples, mode).toBeGreaterThan(1);
      expect(probe.bytes, mode).toBeGreaterThanOrEqual(50 * 1024 * 1024);
      expect(probe.records, mode).toBeGreaterThan(1);
      expect(probe.listed, mode).toBe(1);
      expect(probe.heapDelta, mode).toBeLessThan(100 * 1024 * 1024);
    }
  });
});
