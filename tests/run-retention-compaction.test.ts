import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import childProcess from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { compactTerminalRunEvents, pruneActorRunArchives, markRunRootActive, markRunRootClosed, sweepTempRunRoots, FABRIC_RUN_ROOT_PREFIX, runTreeExitVeto } from "../src/storage/retention.js";
import { ActorLogStore } from "../src/actors/log-store.js";

// KEEP: data-safety seam injection around the ACTUAL native subprocess/syscall, never a stub move.
const traceMoveSeam = (body: (from: string, to: string, move: () => void) => void) => {
  if (os.type() === "Linux" || os.type() === "Windows_NT") {
    const spawn = childProcess.spawnSync;
    return vi.spyOn(childProcess, "spawnSync").mockImplementation(((command, args, options) => {
      if (!/fabric-(?:win-)?trace-move(?:\.exe)?$/.test(String(command))) return spawn(command, args, options);
      let result: ReturnType<typeof spawn> | undefined;
      body(String(args![0]), String(args![1]), () => { result = spawn(command, args, options); });
      if (!result) throw new Error("test seam must execute the actual native helper");
      return result;
    }) as typeof childProcess.spawnSync);
  }
  const rename = fs.renameSync, link = fs.linkSync;
  const a = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => body(String(from), String(to), () => rename(from, to)));
  const b = vi.spyOn(fs, "linkSync").mockImplementation((from, to) => body(String(from), String(to), () => link(from, to)));
  return { mockRestore: () => { a.mockRestore(); b.mockRestore(); } };
};
const roots: string[] = [];
const HOUR = 3600000;
const now = 10 * HOUR;
const log = Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ sequence }) + "\n").join("");
const make = (record: Record<string, unknown> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-tail-")); roots.push(root);
  const run = path.join(root, "runs", "old"); fs.mkdirSync(run, { recursive: true });
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", finishedAt: 4 * HOUR, transport: "process", sessionId: "2147483647", ...record }));
  fs.writeFileSync(path.join(run, "events.jsonl"), log);
  fs.writeFileSync(path.join(run, "reply.json"), '{"text":"retained result"}');
  return { root, run };
};
const snapshot = (run: string): unknown[] => {
  const entries: unknown[] = [];
  const visit = (file: string, relative: string): void => {
    const stat = fs.lstatSync(file);
    const identity = [relative, stat.dev, stat.ino, stat.mode, stat.size, stat.mtimeMs, stat.ctimeMs];
    if (stat.isDirectory()) {
      entries.push(["directory", ...identity]);
      for (const name of fs.readdirSync(file).sort()) visit(path.join(file, name), relative ? path.join(relative, name) : name);
    } else if (stat.isFile()) entries.push(["file", ...identity, fs.readFileSync(file)]);
    else if (stat.isSymbolicLink()) entries.push(["symlink", ...identity, fs.readlinkSync(file)]);
    else entries.push(["other", ...identity]);
  };
  visit(run, "");
  return entries;
};
const archivedTraceContains = (run: string, expected: Buffer): boolean => {
  const archive = path.join(run, "session-archive");
  const visit = (directory: string): boolean => fs.readdirSync(directory, { withFileTypes: true }).some(entry => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory() ? visit(file) : entry.isFile() && entry.name.endsWith(".jsonl") && fs.readFileSync(file).equals(expected);
  });
  try { return visit(archive); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
};
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("six-hour terminal run compaction", () => {
  // KEEP: F1 known freshness-policy gap, NOT a freshness fix. Real repeated publication and subsequent
  // artifact collection must preserve every generation's raw bytes even when the root was backdated.
  it.runIf(os.type() === "Linux" || os.type() === "Windows_NT")("F1 known gap retains late ndjson and both raw generations through repeated native publication and backdated-root pruning", () => {
    const evidence = process.env.PI_C1_F1_EVIDENCE;
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-f1-future-"));
    const run = path.join(root, "runs", "old"), file = path.join(run, "events.jsonl");
    fs.mkdirSync(run, { recursive: true });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", finishedAt: 4 * HOUR,
      transport: "process", sessionId: "2147483647" }));
    const first = Buffer.from(log), late = Buffer.from('{"late":"post-native-custody root refresh"}\n');
    const lateFile = path.join(run, "late.ndjson"), future = new Date(Date.now() + 120_000);
    fs.writeFileSync(file, first);
    const spawn = childProcess.spawnSync;
    const moves: { from: string; to: string }[] = [];
    const held: string[] = [];
    let injected = false;
    const seam = vi.spyOn(childProcess, "spawnSync").mockImplementation(((command, args, options) => {
      const result = spawn(command, args, options); // Always execute the actual authenticated native helper.
      if (/fabric-(?:win-)?trace-move(?:\.exe)?$/.test(String(command))) {
        const from = String(args![0]), to = String(args![1]);
        expect(result.status).toBe(0); expect(result.stdout).toBe("moved\n");
        moves.push({ from, to });
        if (from === file && path.basename(to) === "events.jsonl") {
          held.push(to);
          expect(fs.existsSync(file)).toBe(false);
          if (!injected) {
            expect(fs.readFileSync(to)).toEqual(first);
            fs.writeFileSync(lateFile, late, { flag: "wx" });
            fs.utimesSync(run, future, future); // After the real raw-to-held syscall, before parentChanged.
            injected = true;
          }
        }
      }
      return result;
    }) as typeof childProcess.spawnSync);
    const archive = path.join(run, "session-archive");
    const copies = (): { to: string; sha256: string; bytes: number }[] => fs.readFileSync(path.join(archive, "MANIFEST.jsonl"), "utf8")
      .trim().split("\n").map(line => JSON.parse(line)).filter(record => record.phase === "COPY-READBACK" && record.from === file);
    const digest = (bytes: Buffer): string => createHash("sha256").update(bytes).digest("hex");
    try {
      // This branch records the accepted, named F1 policy gap; success is not proof of freshness.
      expect(compactTerminalRunEvents(run, { now })).toBe(true);
      expect(injected).toBe(true); expect(moves).toHaveLength(2); expect(held).toHaveLength(1);
      const afterFirstMtime = fs.statSync(run).mtimeMs;
      const firstCopy = copies()[0]!;
      expect(firstCopy).toMatchObject({ sha256: digest(first), bytes: first.length });
      expect(fs.readFileSync(firstCopy.to)).toEqual(first); expect(fs.readFileSync(held[0]!)).toEqual(first);
      expect(fs.readFileSync(lateFile)).toEqual(late);
      const firstDerivative = fs.readFileSync(file);
      expect(firstDerivative.includes(Buffer.from('"fabricTruncated"'))).toBe(true);

      // Force a genuinely eligible second generation, not an idempotent/no-op compaction call.
      fs.appendFileSync(file, Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ generation: 2, sequence }) + "\n").join(""));
      const second = fs.readFileSync(file);
      expect(compactTerminalRunEvents(run, { now })).toBe(true);
      expect(moves).toHaveLength(4); expect(held).toHaveLength(2); expect(held[1]).not.toBe(held[0]);
      const allCopies = copies(); expect(allCopies).toHaveLength(2);
      const secondCopy = allCopies[1]!;
      expect(secondCopy.to).not.toBe(firstCopy.to);
      expect(secondCopy).toMatchObject({ sha256: digest(second), bytes: second.length });
      const secondDerivative = fs.readFileSync(file);
      expect(secondDerivative).not.toEqual(firstDerivative);
      expect(secondDerivative.includes(Buffer.from('"generation":2'))).toBe(true);
      const movesAfterPublication = moves.length;
      expect(compactTerminalRunEvents(run, { now })).toBe(false);
      expect(moves).toHaveLength(movesAfterPublication);

      // Exercise normal public retention with multiple eligible nontrace artifacts, not a no-op prune.
      const bloat = ["reply.json", "task.txt", "schema.json", path.join("tmp", "cache.bin")];
      fs.mkdirSync(path.join(run, "tmp"));
      for (const name of bloat) fs.writeFileSync(path.join(run, name), "eligible nontrace bloat");
      fs.utimesSync(run, 1, 1); // Deliberately backdate the fixture root before a later retention cycle.
      expect(pruneActorRunArchives({ runsDirectory: path.dirname(run), retentionMs: 0, now })).toEqual([]);
      for (const name of bloat) expect(fs.existsSync(path.join(run, name)), name).toBe(false);
      expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toContain('"status":"completed"');
      expect(fs.readFileSync(lateFile)).toEqual(late);
      expect(fs.readFileSync(file)).toEqual(secondDerivative);
      for (const [index, bytes] of [first, second].entries()) {
        expect(fs.readFileSync(held[index]!)).toEqual(bytes);
        expect(fs.readFileSync(allCopies[index]!.to)).toEqual(bytes);
        expect(digest(fs.readFileSync(held[index]!))).toBe(digest(bytes));
        expect(digest(fs.readFileSync(allCopies[index]!.to))).toBe(digest(bytes));
      }
      if (evidence) {
        const savedRoot = path.join(evidence, path.basename(root));
        const saved = (file: string): string => path.join(savedRoot, path.relative(root, file));
        // Evidence export is additive/exclusive: never invoke a replacing copy or a cleanup outside TMPDIR.
        for (const source of [file, lateFile, path.join(run, "status.json"), path.join(archive, "MANIFEST.jsonl"),
          ...held, ...allCopies.map(copy => copy.to)]) {
          fs.mkdirSync(path.dirname(saved(source)), { recursive: true });
          fs.writeFileSync(saved(source), fs.readFileSync(source), { flag: "wx" });
        }
        fs.writeFileSync(path.join(savedRoot, "observation.json"), JSON.stringify({
          requirement: "F1 known gap: future-cycle byte safety, not root freshness", run: saved(run), injected, moves,
          futureMtime: future.getTime(), afterFirstMtime, backdatedBeforePrune: 1000,
          first: { held: saved(held[0]!), archive: saved(firstCopy.to), sha256: digest(first), bytes: first.length },
          second: { held: saved(held[1]!), archive: saved(secondCopy.to), sha256: digest(second), bytes: second.length },
          late: { file: saved(lateFile), sha256: digest(late), bytes: late.length }, collectedNontrace: bloat,
        }, null, 2) + "\n", { flag: "wx" });
      }
    } finally { seam.mockRestore(); }
    // Export trace witnesses before smarty-test-env retires its ordinary synthetic fixture TMPDIR.
  });

  // KEEP: R3 data safety. Exercise the public entry point and the actual namespace syscall, not a private helper.
  it("R3 refuses a late trace occupant at the original-to-custody move without overwriting either trace", () => {
    const { run } = make();
    const file = path.join(run, "events.jsonl"), original = Buffer.from(log), before = fs.statSync(file);
    const late = Buffer.from('{"late":"occupied raw custody destination"}\n');
    let destination: string | undefined;
    const seam = traceMoveSeam((from, to, move) => {
      if (!destination && String(from) === file && path.basename(String(to)) === "events.jsonl") {
        destination = String(to);
        fs.writeFileSync(destination, late, { flag: "wx" });
      }
      move();
    });
    try {
      const compacted = compactTerminalRunEvents(run, { now });
      expect(destination).toBeDefined();
      expect.soft(fs.readFileSync(destination!)).toEqual(late);
      expect.soft(fs.readFileSync(file)).toEqual(original);
      expect.soft(fs.statSync(file).ino).toBe(before.ino);
      expect.soft(compacted).toBe(false);
      expect(archivedTraceContains(run, original)).toBe(true);
    } finally { seam.mockRestore(); }
  });

  it("R3 refuses a late trace occupant at refused-derivative evidence without overwriting any raw bytes", () => {
    const { run } = make();
    const file = path.join(run, "events.jsonl"), original = Buffer.from(log), before = fs.statSync(file);
    const append = Buffer.from('{"late":"open raw inode append"}\n');
    const occupant = Buffer.from('{"late":"occupied refused derivative destination"}\n');
    const writer = fs.openSync(file, "a");
    let published = false, destination: string | undefined, held: string | undefined;
    const evidenceSeam = traceMoveSeam((from, to, move) => {
      if (from === file && path.basename(to) === "events.jsonl") held = to;
      if (!destination && from === file && path.basename(to) === "refused-derivative.jsonl") {
        destination = to;
        fs.writeFileSync(destination, occupant, { flag: "wx" });
      }
      move();
      if (!published && to === file && from.endsWith(".tmp")) {
        published = true;
        fs.writeSync(writer, append); // Force refusal after actual public derivative publication.
      }
    });
    try {
      expect(compactTerminalRunEvents(run, { now })).toBe(false);
      expect(published).toBe(true);
      expect(destination).toBeDefined();
      expect.soft(fs.readFileSync(destination!)).toEqual(occupant);
      expect(held).toBeDefined();
      expect(fs.readFileSync(held!)).toEqual(Buffer.concat([original, append]));
      expect(fs.statSync(held!).ino).toBe(before.ino);
      expect(archivedTraceContains(run, original)).toBe(true);
      // The refused derivative must survive too, either still public or under another evidence name.
      const derivativeExists = [file, ...fs.readdirSync(path.dirname(held!)).map(name => path.join(path.dirname(held!), name))]
        .some(candidate => fs.readFileSync(candidate).includes(Buffer.from('"fabricTruncated"')));
      expect(derivativeExists).toBe(true);
    } finally { evidenceSeam.mockRestore(); fs.closeSync(writer); }
  });

  it.runIf(os.type() === "Linux" || os.type() === "Windows_NT")("native derivative source swaps preserve raw A/B/C and never cleanup-unlink a late private occupant", () => {
    const { run } = make();
    const file = path.join(run, "events.jsonl"), original = Buffer.from(log), before = fs.statSync(file);
    const rawB = Buffer.from('{"raw":"B swapped into derivative source"}\n');
    const rawC = Buffer.from('{"raw":"C late private occupant"}\n');
    let temp: string | undefined, saved: string | undefined, held: string | undefined;
    const unlinks: string[] = [], unlink = fs.unlinkSync;
    const unlinkSeam = vi.spyOn(fs, "unlinkSync").mockImplementation(name => { unlinks.push(String(name)); return unlink(name); });
    const seam = traceMoveSeam((from, to, move) => {
      if (from === file && path.basename(to) === "events.jsonl") held = to;
      if (to === file && from.endsWith(".tmp")) {
        temp = from; saved = from + ".saved.jsonl";
        fs.renameSync(from, saved); fs.writeFileSync(from, rawB, { flag: "wx" });
        move(); // Actual native syscall moves B, not the derivative whose inode was judged.
        fs.writeFileSync(from, rawC, { flag: "wx" });
      } else move();
    });
    try {
      expect(compactTerminalRunEvents(run, { now })).toBe(false);
      expect(temp).toBeDefined(); expect(saved).toBeDefined(); expect(held).toBeDefined();
      expect(fs.readFileSync(file)).toEqual(rawB); expect(fs.readFileSync(temp!)).toEqual(rawC);
      expect(fs.readFileSync(saved!).includes(Buffer.from('"fabricTruncated"'))).toBe(true);
      expect(fs.readFileSync(held!)).toEqual(original); expect(fs.statSync(held!).ino).toBe(before.ino);
      expect(archivedTraceContains(run, original)).toBe(true); expect(unlinks).toEqual([]);
    } finally { seam.mockRestore(); unlinkSeam.mockRestore(); }
  });

  it.runIf(os.type() === "Linux" || os.type() === "Windows_NT")("native publication never unlinks a late occupant at its vacated private source name", () => {
    const { run } = make(); const file = path.join(run, "events.jsonl"), raw = Buffer.from('{"raw":"late private source"}\n');
    let temp: string | undefined, occupant: fs.Stats | undefined;
    const seam = traceMoveSeam((from, to, move) => {
      move();
      if (to === file && from.endsWith(".tmp")) {
        temp = from; fs.writeFileSync(from, raw, { flag: "wx" }); occupant = fs.statSync(from);
      }
    });
    try {
      expect(compactTerminalRunEvents(run, { now })).toBe(true);
      expect(temp).toBeDefined(); expect(fs.readFileSync(temp!)).toEqual(raw);
      expect(fs.statSync(temp!).ino).toBe(occupant!.ino);
      expect(fs.readFileSync(file).includes(Buffer.from('"fabricTruncated"'))).toBe(true);
      expect(archivedTraceContains(run, Buffer.from(log))).toBe(true);
    } finally { seam.mockRestore(); }
  });

  it.runIf(os.type() === "Linux" || os.type() === "Windows_NT")("unknown verdict after an actual native custody move never retries or falls back to legacy restoration", () => {
    const { run } = make(); const file = path.join(run, "events.jsonl"), before = fs.statSync(file);
    const spawn = childProcess.spawnSync; let held: string | undefined, moves = 0;
    const seam = vi.spyOn(childProcess, "spawnSync").mockImplementation(((command, args, options) => {
      const result = spawn(command, args, options);
      if (/fabric-(?:win-)?trace-move(?:\.exe)?$/.test(String(command))) {
        moves++;
        if (String(args![0]) === file) {
          held = String(args![1]); expect(result.status).toBe(0);
          return { ...result, stdout: "unreadable verdict\n" }; // The real syscall already moved A.
        }
      }
      return result;
    }) as typeof childProcess.spawnSync);
    const link = vi.spyOn(fs, "linkSync");
    try {
      expect(compactTerminalRunEvents(run, { now })).toBe(false);
      expect(moves).toBe(1); expect(held).toBeDefined(); expect(fs.existsSync(file)).toBe(false);
      expect(fs.readFileSync(held!)).toEqual(Buffer.from(log)); expect(fs.statSync(held!).ino).toBe(before.ino);
      expect(archivedTraceContains(run, Buffer.from(log))).toBe(true); expect(link).not.toHaveBeenCalled();
    } finally { seam.mockRestore(); link.mockRestore(); }
  });

  it("R1 refuses an append at the final source-check-to-namespace-change seam and restores every byte", () => {
    const { run } = make();
    const file = path.join(run, "events.jsonl"), before = fs.statSync(file);
    const late = Buffer.from('{"late":"unarchived append"}\n');
    let injected = false;
    const seam = traceMoveSeam((from, to, move) => {
      if (!injected && (String(from) === file || String(to) === file)) {
        injected = true;
        fs.appendFileSync(file, late);
      }
      move();
    });
    try {
      expect(compactTerminalRunEvents(run, { now })).toBe(false);
      expect(injected).toBe(true);
      expect(fs.readFileSync(file)).toEqual(Buffer.concat([Buffer.from(log), late]));
      expect(fs.statSync(file).ino).toBe(before.ino);
      expect(archivedTraceContains(run, Buffer.from(log))).toBe(true);
    } finally { seam.mockRestore(); }
  });

  it.each(["open-inode append", "late public occupant", "custody veto"])("R1 preserves custody and refuses a %s during no-clobber publication", kind => {
    const { run } = make();
    const file = path.join(run, "events.jsonl"), before = fs.statSync(file), original = Buffer.from(log);
    const late = Buffer.from('{"late":"must survive"}\n');
    const writer = fs.openSync(file, "a");
    let injected = false;
    const seam = traceMoveSeam((from, to, move) => {
      if (!injected && String(to) === file && String(from).endsWith(".tmp")) {
        injected = true;
        if (kind === "late public occupant") fs.writeFileSync(file, late, { flag: "wx" });
        else {
          move();
          if (kind === "open-inode append") fs.writeSync(writer, late);
          else fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", finishedAt: 4 * HOUR, transport: "process", sessionId: "2147483647", cleanupPending: true }));
          return;
        }
      }
      move();
    });
    try {
      expect(compactTerminalRunEvents(run, { now })).toBe(false);
      expect(injected).toBe(true);
      const expected = kind === "open-inode append" ? Buffer.concat([original, late]) : original;
      expect(fs.readFileSync(file)).toEqual(kind === "late public occupant" ? late : expected);
      if (kind !== "late public occupant") expect(fs.statSync(file).ino).toBe(before.ino);
      const custody = fs.readdirSync(path.join(run, "session-archive")).find(name => name.startsWith(".events-custody-"));
      expect(custody).toBeDefined();
      expect(fs.readFileSync(path.join(run, "session-archive", custody!, "events.jsonl"))).toEqual(expected);
      expect(archivedTraceContains(run, original)).toBe(true);
    } finally { seam.mockRestore(); fs.closeSync(writer); }
  });

  it("R1 refuses a root refresh while preparing the derivative without backdating it or moving the raw source", () => {
    const { run } = make();
    const file = path.join(run, "events.jsonl"), before = fs.statSync(file), write = fs.writeSync;
    let refreshed: number | undefined;
    const seam = vi.spyOn(fs, "writeSync").mockImplementation(((...args: Parameters<typeof write>) => {
      const result = write(...args);
      const data: unknown = args[1];
      if (Buffer.isBuffer(data) && data.includes(Buffer.from('"fabricTruncated"'))) {
        const future = new Date(Date.now() + 60_000); fs.utimesSync(run, future, future);
        refreshed = fs.statSync(run).mtimeMs;
      }
      return result;
    }) as typeof fs.writeSync);
    try {
      expect(compactTerminalRunEvents(run, { now })).toBe(false);
      expect(refreshed).toBeDefined();
      expect(fs.statSync(run).mtimeMs).toBe(refreshed);
      expect(fs.statSync(file).ino).toBe(before.ino);
      expect(fs.readFileSync(file)).toEqual(Buffer.from(log));
    } finally { seam.mockRestore(); }
  });

  it("R1 rehashes the full held original rather than accepting a same-length rewrite with restored mtime", () => {
    const { run } = make();
    const file = path.join(run, "events.jsonl");
    fs.utimesSync(file, 1000, 1000); // Exact whole-ms mtime: metadata alone must not detect the rewrite.
    const before = fs.statSync(file);
    const changed = Buffer.from(log); changed[0] = 0x21;
    let injected = false;
    const seam = traceMoveSeam((from, to, move) => {
      if (!injected && String(from) === file) {
        injected = true;
        fs.writeFileSync(file, changed); fs.utimesSync(file, before.atime, before.mtime);
      }
      move();
    });
    try {
      expect(compactTerminalRunEvents(run, { now })).toBe(false);
      expect(injected).toBe(true);
      expect(fs.readFileSync(file)).toEqual(changed);
      expect(fs.statSync(file).ino).toBe(before.ino);
      expect(archivedTraceContains(run, Buffer.from(log))).toBe(true);
    } finally { seam.mockRestore(); }
  });

  it("atomically keeps the final 200 events at six hours even below the byte cap, with status/result and TTL unchanged", () => {
    const { root, run } = make();
    fs.utimesSync(run, 4 * HOUR / 1000, 4 * HOUR / 1000);
    const directoryMtime = fs.statSync(run).mtimeMs;
    const status = fs.readFileSync(path.join(run, "status.json"), "utf8");
    const inode = fs.statSync(path.join(run, "events.jsonl")).ino;
    const originalEvents = Buffer.from(log);
    expect(compactTerminalRunEvents(run, { now: now - 1 })).toBe(false);
    expect(compactTerminalRunEvents(run, { now })).toBe(true);
    expect(archivedTraceContains(run, originalEvents)).toBe(true);
    const custody = fs.readdirSync(path.join(run, "session-archive")).find(name => name.startsWith(".events-custody-"));
    expect(custody).toBeDefined();
    const held = path.join(run, "session-archive", custody!, "events.jsonl");
    expect(fs.statSync(held).ino).toBe(inode);
    expect(fs.readFileSync(held)).toEqual(originalEvents);
    const lines = fs.readFileSync(path.join(run, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(lines[0]).toMatchObject({ fabricTruncated: true });
    expect(lines.slice(1)).toEqual(Array.from({ length: 200 }, (_, i) => ({ sequence: i + 201 })));
    expect(fs.statSync(path.join(run, "events.jsonl")).ino).not.toBe(inode);
    // Only status/receipt timestamps govern this run's TTL. Namespace mutations advance the directory mtime;
    // restoring its old value would erase an external writer's freshness if one arrived during collection.
    expect(fs.statSync(run).mtimeMs).toBeGreaterThan(directoryMtime);
    expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(status);
    expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe('{"text":"retained result"}');
    const before = snapshot(run);
    expect(compactTerminalRunEvents(run, { now })).toBe(false);
    expect(snapshot(run)).toEqual(before);
    expect(pruneActorRunArchives({ runsDirectory: path.join(root, "runs"), retentionMs: 7 * 24 * HOUR, now })).toEqual([]);
    expect(pruneActorRunArchives({ runsDirectory: path.join(root, "runs"), retentionMs: 7 * 24 * HOUR, now: 4 * HOUR + 7 * 24 * HOUR })).toEqual([]);
    expect(fs.existsSync(run)).toBe(true);
    expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(status);
    expect(archivedTraceContains(run, originalEvents)).toBe(true);
    expect(fs.statSync(held).ino).toBe(inode);
    expect(fs.readFileSync(held)).toEqual(originalEvents);
    expect(fs.existsSync(path.join(run, "reply.json"))).toBe(false);
  });

  it.each([
    [1024, 1000, 1],
    [1024, 1024, 1],
    [256 * 1024, 8190, 32],
    [256 * 1024, 8192, 32],
  ])("preserves a near-limit unmarked log byte-for-byte (cap=%i, eventBytes=%i, count=%i)", (cap, eventBytes, count) => {
    const { run } = make();
    const event = JSON.stringify({ text: "x".repeat(eventBytes - Buffer.byteLength('{"text":""}\n')) }) + "\n";
    expect(Buffer.byteLength(event)).toBe(eventBytes);
    const file = path.join(run, "events.jsonl"); fs.writeFileSync(file, event.repeat(count));
    const before = snapshot(run); const directoryMtime = fs.statSync(run).mtimeMs;
    const changes: unknown[] = [];
    const options = { now, terminalRunEventsMaxBytes: cap, onCompact: (change: unknown) => changes.push(change) };
    expect(compactTerminalRunEvents(run, { ...options, dryRun: true })).toBe(false);
    expect(snapshot(run)).toEqual(before);
    expect(compactTerminalRunEvents(run, options)).toBe(false);
    expect(snapshot(run)).toEqual(before);
    expect(fs.statSync(run).mtimeMs).toBe(directoryMtime);
    expect(changes).toEqual([]);
  });

  it("compacts exactly 201 events even when bytes fit, then is idempotent", () => {
    const { run } = make();
    const file = path.join(run, "events.jsonl");
    const events = Array.from({ length: 201 }, (_, sequence) => JSON.stringify({ sequence }) + "\n");
    const originalEvents = Buffer.from(events.join(""));
    fs.writeFileSync(file, originalEvents);
    const before = snapshot(run);
    expect(compactTerminalRunEvents(run, { now, dryRun: true })).toBe(true);
    expect(snapshot(run)).toEqual(before);
    expect(compactTerminalRunEvents(run, { now })).toBe(true);
    const lines = fs.readFileSync(file, "utf8").trim().split("\n");
    expect(JSON.parse(lines[0]!)).toMatchObject({ fabricTruncated: true });
    expect(lines.slice(1).join("\n") + "\n").toBe(events.slice(1).join(""));
    expect(archivedTraceContains(run, originalEvents)).toBe(true);
    const after = snapshot(run);
    expect(compactTerminalRunEvents(run, { now })).toBe(false);
    expect(snapshot(run)).toEqual(after);
  });

  it("compacts a closed managed one-shot root and retains trace bytes past its deletion age", () => {
    const { root, run } = make();
    const managed = path.join(root, FABRIC_RUN_ROOT_PREFIX + "closed");
    fs.mkdirSync(managed);
    fs.renameSync(run, path.join(managed, "old"));
    const old = path.join(managed, "old");
    const originalEvents = Buffer.from(log);
    const status = fs.readFileSync(path.join(old, "status.json"), "utf8");
    markRunRootActive(managed, 4 * HOUR); markRunRootClosed(managed, 4 * HOUR, true);
    const sweep = (at: number) => sweepTempRunRoots({ tempRoot: root, now: at, orphanedTempRunRetentionMs: 24 * HOUR, oneShotRunRetentionMs: 24 * HOUR });
    expect(sweep(now).removedRuns).toEqual([]);
    expect(fs.readFileSync(path.join(old, "events.jsonl"), "utf8").trim().split("\n")).toHaveLength(201);
    expect(archivedTraceContains(old, originalEvents)).toBe(true);
    expect(sweep(28 * HOUR)).toEqual({ removedRuns: [], removedRoots: [] });
    expect(fs.existsSync(old)).toBe(true);
    expect(fs.readFileSync(path.join(old, "status.json"), "utf8")).toBe(status);
    expect(archivedTraceContains(old, originalEvents)).toBe(true);
    expect(fs.existsSync(path.join(old, "reply.json"))).toBe(false);
  });

  it.each([{ status: "running" }, { status: "queued" }, { status: "unknown" }, { sessionId: undefined }, { sessionId: String(process.pid) }, { cleanupPending: true }])("leaves nonterminal/unknown/uncertain custody untouched: %j", record => {
    const { run } = make(record); const before = snapshot(run);
    expect(compactTerminalRunEvents(run, { now })).toBe(false);
    expect(snapshot(run)).toEqual(before);
  });

  it.each([undefined, "unknown", "herdr", "localterm"])("requires persisted root exit evidence for %s custody in compaction and collection", transport => {
    const { root, run } = make({ transport });
    const before = snapshot(run); const mtime = fs.statSync(run).mtimeMs;
    expect(compactTerminalRunEvents(run, { now, dryRun: true })).toBe(false);
    expect(compactTerminalRunEvents(run, { now })).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: path.join(root, "runs"), retentionMs: 7 * 24 * HOUR, now: 8 * 24 * HOUR })).toEqual([]);
    expect(snapshot(run)).toEqual(before); expect(fs.statSync(run).mtimeMs).toBe(mtime);
    const managed = path.join(root, FABRIC_RUN_ROOT_PREFIX + "unknown"); fs.mkdirSync(managed);
    const moved = path.join(managed, "old"); fs.renameSync(run, moved);
    markRunRootActive(managed, 4 * HOUR); markRunRootClosed(managed, 4 * HOUR, true);
    const movedBefore = snapshot(moved);
    const movedMtime = fs.statSync(moved).mtimeMs;
    const sweep = (at: number) => sweepTempRunRoots({ tempRoot: root, now: at, orphanedTempRunRetentionMs: 24 * HOUR, oneShotRunRetentionMs: 24 * HOUR });
    expect(sweep(now)).toEqual({ removedRuns: [], removedRoots: [] });
    expect(snapshot(moved)).toEqual(movedBefore); expect(fs.statSync(moved).mtimeMs).toBe(movedMtime);
    expect(sweep(28 * HOUR)).toEqual({ removedRuns: [], removedRoots: [] });
    expect(snapshot(moved)).toEqual(movedBefore); expect(fs.statSync(moved).mtimeMs).toBe(movedMtime);
  });

  it("never treats an admitted tree with a missing saved root record as recordless pre-launch rollback", () => {
    const { root, run } = make(); fs.unlinkSync(path.join(run, "status.json"));
    const before = snapshot(run); const mtime = fs.statSync(run).mtimeMs;
    expect(runTreeExitVeto(run, 0, undefined, true)).toMatch(/unknown root identity/);
    expect(compactTerminalRunEvents(run, { now })).toBe(false);
    expect(snapshot(run)).toEqual(before); expect(fs.statSync(run).mtimeMs).toBe(mtime);
    const managed = path.join(root, FABRIC_RUN_ROOT_PREFIX + "missing-record"); fs.mkdirSync(managed);
    const moved = path.join(managed, "old"); fs.renameSync(run, moved);
    fs.writeFileSync(path.join(moved, "task.txt"), "admitted worker input");
    markRunRootActive(managed, 4 * HOUR); markRunRootClosed(managed, 4 * HOUR, true);
    const movedBefore = snapshot(moved); const movedMtime = fs.statSync(moved).mtimeMs;
    expect(sweepTempRunRoots({ tempRoot: root, now: 100 * HOUR, orphanedTempRunRetentionMs: 24 * HOUR, oneShotRunRetentionMs: 24 * HOUR })).toEqual({ removedRuns: [], removedRoots: [] });
    expect(snapshot(moved)).toEqual(movedBefore); expect(fs.statSync(moved).mtimeMs).toBe(movedMtime);
  });

  it("does not infer pre-launch rollback from an input-only legacy tree under a closed owner", () => {
    const { root } = make(); const managed = path.join(root, FABRIC_RUN_ROOT_PREFIX + "input-only");
    const run = path.join(managed, "unknown"); fs.mkdirSync(run, { recursive: true });
    fs.writeFileSync(path.join(run, "task.txt"), "no durable admission or worker exit record");
    markRunRootActive(managed, 1); markRunRootClosed(managed, 1, true);
    const before = snapshot(run); const mtime = fs.statSync(run).mtimeMs;
    expect(sweepTempRunRoots({ tempRoot: root, now: 100 * HOUR, orphanedTempRunRetentionMs: 24 * HOUR, oneShotRunRetentionMs: 24 * HOUR })).toEqual({ removedRuns: [], removedRoots: [] });
    expect(snapshot(run)).toEqual(before); expect(fs.statSync(run).mtimeMs).toBe(mtime);
  });

  it("dry-run reports eligibility without changing bytes, inode or timestamps", () => {
    const { run } = make(); const before = snapshot(run);
    expect(compactTerminalRunEvents(run, { now, dryRun: true } as Parameters<typeof compactTerminalRunEvents>[1] & { dryRun: boolean })).toBe(true);
    expect(snapshot(run)).toEqual(before);
  });

  it("existing owned actor maintenance keeps the newest ordinary backup, including numeric suffixes, and preserves malformed orphans", () => {
    const { root } = make(); const sessionFile = path.join(root, "session.jsonl");
    const names = ["session.jsonl.20260927T150000000Z.bak", "session.jsonl.20260927T150000000Z-2.bak", "session.jsonl.20260927T150000000Z-10.bak", "session.jsonl.20260926T150000000Z.bak", "session.jsonl.20260925T150000000Z.id.orphan-noheader.bak"];
    for (const name of names) fs.writeFileSync(path.join(root, name), name);
    // Rotation order is trusted data (smarty-dev#7916): each backup's mtime follows its rotation.
    for (const [order, index] of [3, 0, 1, 2].entries()) fs.utimesSync(path.join(root, names[index]!), 1_000 + order, 1_000 + order);
    fs.writeFileSync(sessionFile, "live session");
    const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 7 * 24 * HOUR });
    store.pruneRuns({ sessionFile }, now);
    expect(fs.readdirSync(root).filter(name => name.endsWith(".bak")).sort()).toEqual([names[2], names[4]].sort());
    expect(fs.readFileSync(sessionFile, "utf8")).toBe("live session");
    const archive = path.join(root, "session-archive");
    const records = fs.readFileSync(path.join(archive, "MANIFEST.jsonl"), "utf8").trim().split("\n")
      .map(line => JSON.parse(line) as { phase: string; from: string; to: string; sha256: string; bytes: number });
    const copies = records.filter(record => record.phase === "COPY-READBACK");
    const moves = records.filter(record => record.phase === "MOVED-TO");
    expect(copies).toHaveLength(3); expect(moves).toHaveLength(3);
    expect(fs.readdirSync(archive).filter(name => name !== "MANIFEST.jsonl")).toHaveLength(6);
    for (const name of [names[0]!, names[1]!, names[3]!]) {
      const bytes = Buffer.from(name), sha256 = createHash("sha256").update(bytes).digest("hex");
      const copy = copies.find(record => record.from === path.join(root, name));
      const move = moves.find(record => record.from === path.join(root, name));
      expect(copy).toMatchObject({ bytes: bytes.length, sha256 });
      expect(move).toMatchObject({ bytes: bytes.length, sha256 });
      expect(copy!.to).not.toBe(move!.to);
      expect(fs.readFileSync(copy!.to)).toEqual(bytes);
      expect(fs.readFileSync(move!.to)).toEqual(bytes);
    }
  });
});
