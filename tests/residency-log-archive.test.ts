import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { appendResidentLog, pruneResidentChildLogs, pruneWedgeEvidence } from "../src/residency/launcher.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// Keep: data safety. These are real retention/rotation entry points and real files, not mocked deletion calls.
const fixtures: string[] = [];
const fixture = () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-archive-"));
  fixtures.push(base);
  const root = path.join(base, "resident");
  fs.mkdirSync(root, { mode: 0o700 });
  return { base, root, archive: `${root}.trace-archive` };
};
const filesBelow = (directory: string): string[] => {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory() ? filesBelow(file) : entry.isFile() ? [file] : [];
  });
};
const sha256 = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
const expectArchived = (archive: string, original: Buffer) => {
  const files = filesBelow(archive);
  const payload = files.find(file => fs.readFileSync(file).equals(original));
  expect(payload, `missing exact archived bytes (${sha256(original)})`).toBeDefined();
  expect(sha256(fs.readFileSync(payload!))).toBe(sha256(original));
  // Verify the durable receipt actually binds the digest/byte count to the archived payload.
  type FileReceipt = { sha256: string; bytes: number; to: string };
  const receipts = fs.readFileSync(path.join(archive, "MANIFEST.jsonl"), "utf8").trim().split("\n")
    .map(line => JSON.parse(line) as FileReceipt & { kind: string; files?: Array<{ path: string; sha256: string; bytes: number }> });
  const receipt = receipts.flatMap<FileReceipt>(row => row.kind === "file" ? [row] : (row.files ?? []).map(file =>
    ({ ...file, to: path.join(row.to, file.path) }))).find(row => row.sha256 === sha256(original));
  expect(receipt).toBeDefined();
  expect(receipt!.bytes).toBe(original.length);
  expect(fs.readFileSync(receipt!.to)).toEqual(original);
  expect(sha256(fs.readFileSync(receipt!.to))).toBe(receipt!.sha256);
};
const stale = (file: string) => {
  const old = new Date(Date.now() - 8 * 24 * 60 * 60 * 1_000);
  fs.utimesSync(file, old, old);
};
const wedgesFixture = (root: string) => {
  const wedges = path.join(root, "wedges");
  fs.mkdirSync(wedges, { mode: 0o700 });
  for (let n = 0; n < 7; n++) fs.mkdirSync(path.join(wedges, `2026-10-${String(n + 1).padStart(2, "0")}`), { mode: 0o700 });
  return wedges;
};
afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  vi.restoreAllMocks();
  for (const base of fixtures.splice(0)) fs.rmSync(base, { recursive: true, force: true });
});

describe("resident launcher archive-before-loss", () => {
  it("preserves both the existing .1 and every repeated rollover generation with exact bytes and durable SHA256", () => {
    const { root, archive } = fixture();
    const file = path.join(root, "launcher.log");
    const generations = [Buffer.from("previous\0\r\n\xff", "latin1"), Buffer.from("current\0\r\n\xfe", "latin1")];
    fs.writeFileSync(`${file}.1`, generations[0]!);
    fs.writeFileSync(file, generations[1]!);
    for (let n = 0; n < 4; n++) {
      const next = Buffer.from(`generation-${n}\0\r\n`, "latin1");
      appendResidentLog(file, next, 1);
      expect(fs.readFileSync(file)).toEqual(next);
      expect(fs.readFileSync(`${file}.1`)).toEqual(generations.at(-1));
      generations.push(next);
    }
    for (const bytes of generations.slice(0, -1)) expectArchived(archive, bytes);
    // Neither age pruning nor further rollover is allowed to reap the archives.
    pruneResidentChildLogs(root, path.join(root, "child-active.log"), Date.now() + 365 * 24 * 60 * 60 * 1_000);
    for (const bytes of generations.slice(0, -1)) expectArchived(archive, bytes);
  });

  it("does not overwrite a late .1 generation created after the final absence check", () => {
    const { root, archive } = fixture();
    const file = path.join(root, "launcher.log"), previous = `${file}.1`;
    const original = Buffer.from("current raw generation"), late = Buffer.from("late unarchived generation");
    fs.writeFileSync(file, original);
    const lstat = fs.lstatSync;
    let absentChecks = 0, injected = false;
    vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
      const found = lstat(...args);
      if (String(args[0]) === previous && !found && ++absentChecks === 2) {
        fs.writeFileSync(previous, late); injected = true;
      }
      return found;
    }) as typeof fs.lstatSync);
    appendResidentLog(file, "next", 1);
    expect(injected).toBe(true);
    expect(fs.readFileSync(previous)).toEqual(late);
    expect(fs.readFileSync(file)).toEqual(Buffer.concat([original, Buffer.from("next")]));
    expectArchived(archive, original);
  });

  it("does not rotate or archive an empty append, an empty generation, or a below-cap append", () => {
    const { root, archive } = fixture();
    const file = path.join(root, "child-current.log");
    fs.writeFileSync(file, "");
    appendResidentLog(file, "", 0);
    expect(fs.existsSync(`${file}.1`)).toBe(false);
    appendResidentLog(file, "small", 0); // An empty current generation must not be rotated even at a zero cap.
    appendResidentLog(file, " addition", 64);
    expect(fs.readFileSync(file, "utf8")).toBe("small addition");
    expect(fs.existsSync(`${file}.1`)).toBe(false);
    expect(fs.existsSync(archive)).toBe(false);
  });

  it("waives the cap and continues appending without touching either generation when archiving fails", () => {
    const { root, archive } = fixture();
    const file = path.join(root, "child-current.log");
    fs.writeFileSync(file, "current raw bytes");
    fs.writeFileSync(`${file}.1`, "older raw bytes");
    fs.writeFileSync(archive, "archive path unavailable");
    expect(() => appendResidentLog(file, "\nstill logging", 1)).not.toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe("current raw bytes\nstill logging");
    expect(fs.readFileSync(`${file}.1`, "utf8")).toBe("older raw bytes");
  });

  it.each(["rotation", "rotation-prior", "child", "wedge"] as const)("keeps source bytes when the real archive readback detects corruption (%s)", operation => {
    const { root, archive } = fixture();
    const wedges = operation === "wedge" ? wedgesFixture(root) : undefined;
    const file = wedges ? path.join(wedges, "2026-10-01", "trace.ndjson") : path.join(root, "child-old.log");
    const original = Buffer.from("original raw trace\0\r\n\xff", "latin1");
    fs.writeFileSync(file, original); stale(file);
    fs.writeFileSync(`${file}.1`, "previous generation");
    const archivedFds = new Set<number>();
    const open = fs.openSync, close = fs.closeSync, write = fs.writeSync, copy = fs.copyFileSync;
    let corrupted = false;
    vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
      const fd = open(...args);
      if (String(args[0]).startsWith(`${archive}${path.sep}`) &&
          (operation !== "rotation-prior" || String(args[0]).endsWith(".1"))) archivedFds.add(fd);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { archivedFds.delete(fd); close(fd); });
    vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, ...args: unknown[]) => {
      const count = (write as (...args: unknown[]) => number)(fd, ...args);
      if (!corrupted && count > 0 && archivedFds.has(fd)) {
        corrupted = true;
        write(fd, Buffer.from([1]), 0, 1, 0);
      }
      return count;
    }) as typeof fs.writeSync);
    vi.spyOn(fs, "copyFileSync").mockImplementation((from, to, flags) => {
      copy(from, to, flags);
      if (!corrupted && String(to).startsWith(`${archive}${path.sep}`) &&
          (operation !== "rotation-prior" || String(to).endsWith(".1"))) {
        corrupted = true;
        const fd = open(to, "r+");
        try { write(fd, Buffer.from([1]), 0, 1, 0); } finally { close(fd); }
      }
    });
    const rotating = operation === "rotation" || operation === "rotation-prior";
    const run = () => rotating ? appendResidentLog(file, "next", 1)
      : operation === "child" ? pruneResidentChildLogs(root, path.join(root, "child-active.log")) : pruneWedgeEvidence(wedges!);
    expect(run).not.toThrow();
    expect(corrupted, "the real archive copy must be exercised").toBe(true);
    expect(fs.readFileSync(file)).toEqual(rotating ? Buffer.concat([original, Buffer.from("next")]) : original);
    expect(fs.readFileSync(`${file}.1`, "utf8")).toBe("previous generation");
  });

  it.each(["rotation", "child", "wedge"] as const)("retains a same-inode rewrite with restored mtime after archival (%s)", operation => {
    const { root, archive } = fixture();
    const wedges = operation === "wedge" ? wedgesFixture(root) : undefined;
    const oldest = wedges && path.join(wedges, "2026-10-01");
    const current = path.join(root, "child-old.log");
    const file = oldest ? path.join(oldest, "trace.jsonl") : operation === "rotation" ? `${current}.1` : current;
    const before = Buffer.from("original raw generation\n"), after = Buffer.from("modified raw generation\n");
    expect(after.length).toBe(before.length);
    fs.writeFileSync(file, before);
    const time = new Date(Date.now() - 8 * 24 * 60 * 60 * 1_000);
    fs.utimesSync(file, time, time);
    if (operation === "rotation") fs.writeFileSync(current, "current");
    let rewritten = false;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (target !== (oldest || file) || rewritten) return;
      rewritten = true;
      fs.writeFileSync(file, after);
      fs.utimesSync(file, time, time);
    };
    if (operation === "rotation") appendResidentLog(current, "next", 1);
    else if (operation === "child") pruneResidentChildLogs(root, path.join(root, "child-active.log"));
    else pruneWedgeEvidence(wedges!);
    expect(rewritten).toBe(true);
    expect(fs.readFileSync(file)).toEqual(after);
    if (operation === "rotation") expect(fs.readFileSync(current, "utf8")).toBe("currentnext");
    expectArchived(archive, before);
  });

  it("archives stale child current and .1 generations but keeps the active pair, fresh logs and nonlogs", () => {
    const { root, archive } = fixture();
    const originals = new Map<string, Buffer>();
    for (const name of ["child-old.log", "child-old.log.1", "child-older.log.1"]) {
      const bytes = Buffer.from(`${name}\0original\r\n`);
      originals.set(name, bytes);
      fs.writeFileSync(path.join(root, name), bytes); stale(path.join(root, name));
    }
    for (const name of ["child-active.log", "child-active.log.1", "child-fresh.log", "owner.json"]) {
      fs.writeFileSync(path.join(root, name), name);
      if (name !== "child-fresh.log") stale(path.join(root, name));
    }
    pruneResidentChildLogs(root, path.join(root, "child-active.log"));
    for (const [name, bytes] of originals) {
      expect(fs.existsSync(path.join(root, name))).toBe(false);
      expectArchived(archive, bytes);
    }
    for (const name of ["child-active.log", "child-active.log.1", "child-fresh.log", "owner.json"])
      expect(fs.readFileSync(path.join(root, name), "utf8")).toBe(name);
  });

  it("retains stale child bytes on archival failure and does not throw during child startup", () => {
    const { root, archive } = fixture();
    const file = path.join(root, "child-old.log.1");
    fs.writeFileSync(file, "never lose me"); stale(file);
    fs.writeFileSync(archive, "unavailable");
    expect(() => pruneResidentChildLogs(root, path.join(root, "child-active.log"))).not.toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe("never lose me");
  });

  it("archives all old wedge files including extensionless tails and nested jsonl/ndjson before pruning", () => {
    const { root, archive } = fixture();
    const wedges = wedgesFixture(root);
    const bytes: Buffer[] = [];
    for (const old of ["2026-10-01", "2026-10-02"]) {
      const dir = path.join(wedges, old);
      fs.mkdirSync(path.join(dir, "nested"));
      for (const name of ["child-log-tail", "status", "nested/session.jsonl", "nested/events.ndjson"]) {
        const raw = Buffer.from(`${old}/${name}\0\r\n\xff`, "latin1");
        bytes.push(raw); fs.writeFileSync(path.join(dir, name), raw);
      }
    }
    fs.mkdirSync(path.join(wedges, "reports"));
    fs.writeFileSync(path.join(wedges, "reports", "report.json"), "report retained");
    pruneWedgeEvidence(wedges);
    expect(fs.readdirSync(wedges).filter(name => name !== "reports")).toHaveLength(5);
    for (const raw of bytes) expectArchived(archive, raw);
    pruneWedgeEvidence(wedges);
    for (const raw of bytes) expectArchived(archive, raw);
    expect(fs.readFileSync(path.join(wedges, "reports", "report.json"), "utf8")).toBe("report retained");
  });

  it("does not recursively delete a late nested trace absent from the verified archive inventory", () => {
    const { root, archive } = fixture();
    const wedges = wedgesFixture(root);
    const oldest = path.join(wedges, "2026-10-01");
    const original = Buffer.from("archived original trace\0\r\n");
    fs.mkdirSync(path.join(oldest, "nested"));
    fs.writeFileSync(path.join(oldest, "nested", "original.jsonl"), original);
    let injected = false;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (target !== oldest || injected) return;
      injected = true;
      fs.writeFileSync(path.join(oldest, "nested", "late.ndjson"), "late unarchived trace");
    };
    pruneWedgeEvidence(wedges);
    expect(injected).toBe(true);
    expect(fs.readFileSync(path.join(oldest, "nested", "late.ndjson"), "utf8")).toBe("late unarchived trace");
    expect(fs.readFileSync(path.join(oldest, "nested", "original.jsonl"))).toEqual(original);
    expectArchived(archive, original);
  });

  it("keeps logs and wedge evidence when archive fsync fails on any platform", () => {
    const { root } = fixture();
    const file = path.join(root, "child-current.log");
    fs.writeFileSync(file, "current"); fs.writeFileSync(`${file}.1`, "previous");
    const fsync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => {
      throw Object.assign(new Error("injected archive fsync failure"), { code: "EIO" });
    });
    appendResidentLog(file, "next", 1);
    expect(fsync).toHaveBeenCalled();
    expect(fs.readFileSync(file, "utf8")).toBe("currentnext");
    expect(fs.readFileSync(`${file}.1`, "utf8")).toBe("previous");
    fsync.mockClear();
    const old = path.join(root, "child-old.log");
    fs.writeFileSync(old, "old raw bytes"); stale(old);
    pruneResidentChildLogs(root, file);
    expect(fsync).toHaveBeenCalled();
    expect(fs.readFileSync(old, "utf8")).toBe("old raw bytes");
    fsync.mockClear();
    const wedges = wedgesFixture(root), evidence = path.join(wedges, "2026-10-01", "trace.jsonl");
    fs.writeFileSync(evidence, "old evidence");
    pruneWedgeEvidence(wedges);
    expect(fsync).toHaveBeenCalled();
    expect(fs.readFileSync(evidence, "utf8")).toBe("old evidence");
    expect(fs.readdirSync(wedges)).toHaveLength(7);
  });

  it("retains old wedge directories and returns to watchdog recovery when archiving is unavailable", () => {
    const { root, archive } = fixture();
    const wedges = wedgesFixture(root);
    const file = path.join(wedges, "2026-10-01", "trace.jsonl");
    fs.writeFileSync(file, "original evidence");
    fs.writeFileSync(archive, "unavailable");
    expect(() => pruneWedgeEvidence(wedges)).not.toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe("original evidence");
    expect(fs.readdirSync(wedges)).toHaveLength(7);
  });
});
