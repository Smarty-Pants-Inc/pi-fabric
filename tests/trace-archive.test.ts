import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { archiveTraceCopy, archiveTraceTreeCopy } from "../src/storage/trace-archive.js";

// Data-safety boundary: these copies are the evidence required before callers may delete originals.
const roots: string[] = [];
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-trace-copy-"));
  roots.push(root);
  return { root, file: path.join(root, "source.jsonl"), archive: path.join(root, "archive") };
};
const sha256 = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("verified trace archive copies", () => {
  it.each(["source.jsonl", "messages.ndjson", "arbitrary.audit.log", "no-extension", "empty.jsonl"])
    ("copies exact raw bytes without moving or overwriting %s", name => {
      const { root, archive } = fixture();
      const file = path.join(root, name);
      const raw = name.startsWith("empty") ? Buffer.alloc(0) : Buffer.from([0, 255, 123, 13, 10, 128, 1]);
      fs.writeFileSync(file, raw);
      const before = fs.lstatSync(file);
      const first = archiveTraceCopy(file, archive, { reason: "test" }, before);
      const second = archiveTraceCopy(file, archive);
      expect(first).toMatchObject({ from: file, sha256: sha256(raw), bytes: raw.length });
      expect(first.stat.ino).toBe(before.ino);
      expect(first.stat.dev).toBe(before.dev);
      expect(path.extname(first.to)).toBe(path.extname(file));
      expect(second.to).not.toBe(first.to);
      expect(fs.readFileSync(first.to)).toEqual(raw);
      expect(fs.readFileSync(second.to)).toEqual(raw);
      expect(fs.readFileSync(file)).toEqual(raw);
      expect(fs.lstatSync(file).ino).toBe(before.ino);
    });

  it.each([false, true])("probes the Windows directory-sync branch (corrupt copy: %s)", corrupt => {
    const { root, file, archive } = fixture();
    const raw = Buffer.from("Windows copy/readback evidence");
    fs.writeFileSync(file, raw);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const fsync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory()) throw new Error("Windows cannot fsync directories");
      return fsync(fd);
    });
    if (corrupt) {
      const write = fs.writeSync.bind(fs);
      vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, buffer: Buffer, ...args: unknown[]) => {
        const broken = Buffer.from(buffer);
        broken[0] = broken[0]! ^ 0xff;
        return Reflect.apply(write, fs, [fd, broken, ...args]);
      }) as typeof fs.writeSync);
    }
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      if (corrupt) expect(() => archiveTraceCopy(file, archive)).toThrow(/read.?back|verification/i);
      else {
        const copied = archiveTraceCopy(file, archive);
        expect(fs.readFileSync(copied.to)).toEqual(raw);
        const source = path.join(root, "tree");
        fs.mkdirSync(source);
        fs.writeFileSync(path.join(source, "plain.log"), raw);
        const tree = archiveTraceTreeCopy(source, archive);
        expect(fs.readFileSync(path.join(tree.to, "plain.log"))).toEqual(raw);
      }
      expect(fs.readFileSync(file)).toEqual(raw);
    } finally { Object.defineProperty(process, "platform", platform); }
  });

  it("never appends a manifest receipt to a source named MANIFEST.jsonl", () => {
    const { root } = fixture();
    const file = path.join(root, "MANIFEST.jsonl");
    const raw = Buffer.from("original manifest evidence\\n");
    fs.writeFileSync(file, raw);
    expect(() => archiveTraceCopy(file, root)).toThrow(/manifest|changed/i);
    expect(fs.readFileSync(file)).toEqual(raw);
  });

  it.skipIf(process.platform === "win32")("fails a supported directory-sync error without losing the original", () => {
    const { file, archive } = fixture();
    fs.writeFileSync(file, "must survive a failed directory barrier");
    const sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory()) throw new Error("directory barrier failed");
      return sync(fd);
    });
    expect(() => archiveTraceCopy(file, archive)).toThrow(/directory barrier failed/);
    vi.restoreAllMocks();
    expect(fs.readFileSync(file, "utf8")).toBe("must survive a failed directory barrier");
  });

  it("handles partial writes and more than one streaming chunk", () => {
    const { file, archive } = fixture();
    const raw = Buffer.alloc((1 << 20) + 17, 0xa5);
    fs.writeFileSync(file, raw);
    const write = fs.writeSync.bind(fs);
    vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number) =>
      write(fd, buffer, offset, Math.min(length, 8192))) as typeof fs.writeSync);
    const result = archiveTraceCopy(file, archive);
    expect(result.sha256).toBe(sha256(raw));
    expect(result.bytes).toBe(raw.length);
    expect(fs.readFileSync(result.to)).toEqual(raw);
    expect(fs.readFileSync(file)).toEqual(raw);
  });

  it("refuses a different judged inode without changing the source", () => {
    const { file, archive } = fixture();
    fs.writeFileSync(file, "raw");
    const stat = fs.lstatSync(file);
    expect(() => archiveTraceCopy(file, archive, {}, { dev: stat.dev, ino: stat.ino + 1 })).toThrow(/changed|identity/i);
    expect(fs.readFileSync(file, "utf8")).toBe("raw");
  });

  it.each(["writeSync", "readSync", "fsyncSync"] as const)("throws on %s failure with source intact", operation => {
    const { file, archive } = fixture();
    fs.writeFileSync(file, "irreplaceable raw evidence");
    vi.spyOn(fs, operation).mockImplementation(() => { throw new Error("injected I/O failure"); });
    expect(() => archiveTraceCopy(file, archive)).toThrow(/injected I\/O failure/);
    vi.restoreAllMocks();
    expect(fs.readFileSync(file, "utf8")).toBe("irreplaceable raw evidence");
  });

  it("independently reads back the archive and refuses corrupt copied bytes", () => {
    const { file, archive } = fixture();
    const raw = Buffer.from("original evidence");
    fs.writeFileSync(file, raw);
    const write = fs.writeSync.bind(fs);
    vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, buffer: Buffer, ...args: unknown[]) => {
      const corrupt = Buffer.from(buffer);
      corrupt[0] = corrupt[0]! ^ 0xff;
      return Reflect.apply(write, fs, [fd, corrupt, ...args]);
    }) as typeof fs.writeSync);
    expect(() => archiveTraceCopy(file, archive)).toThrow(/read.?back|hash|verification/i);
    vi.restoreAllMocks();
    expect(fs.readFileSync(file)).toEqual(raw);
  });

  it("throws when the independent archive readback itself fails", () => {
    const { file, archive } = fixture();
    fs.writeFileSync(file, "raw source");
    const open = fs.openSync.bind(fs), read = fs.readSync.bind(fs);
    const readers = new Set<number>();
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, flags: number | string, mode?: number) => {
      const fd = open(target, flags, mode);
      if (typeof target === "string" && target.startsWith(archive + path.sep) &&
          typeof flags === "number" && !(flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR))) readers.add(fd);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "readSync").mockImplementation(((fd: number, ...args: unknown[]) => {
      if (readers.has(fd)) throw new Error("independent archive readback failed");
      return Reflect.apply(read, fs, [fd, ...args]);
    }) as typeof fs.readSync);
    expect(() => archiveTraceCopy(file, archive)).toThrow(/independent archive readback failed/);
    vi.restoreAllMocks();
    expect(fs.readFileSync(file, "utf8")).toBe("raw source");
  });

  it("refuses source metadata changes even if bytes and mtime are restored", () => {
    const { file, archive } = fixture();
    fs.writeFileSync(file, "same bytes");
    const before = fs.lstatSync(file);
    const write = fs.writeSync.bind(fs);
    let changed = false;
    vi.spyOn(fs, "writeSync").mockImplementation(((...args: unknown[]) => {
      const result = Reflect.apply(write, fs, args);
      if (!changed) {
        changed = true;
        fs.chmodSync(file, 0o400);
        fs.utimesSync(file, before.atime, before.mtime);
      }
      return result;
    }) as typeof fs.writeSync);
    expect(() => archiveTraceCopy(file, archive)).toThrow(/changed|stable/i);
    vi.restoreAllMocks();
    expect(fs.readFileSync(file, "utf8")).toBe("same bytes");
  });

  it("does not overwrite a destination which appears at exclusive creation", () => {
    const { file, archive } = fixture();
    fs.writeFileSync(file, "source");
    const open = fs.openSync.bind(fs);
    let collided: string | undefined;
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, flags: number | string, mode?: number) => {
      if (!collided && typeof target === "string" && target.startsWith(archive + path.sep) &&
          typeof flags === "number" && (flags & fs.constants.O_EXCL)) {
        collided = target;
        fs.writeFileSync(target, "existing archive", { flag: "wx" });
      }
      return open(target, flags, mode);
    }) as typeof fs.openSync);
    const copy = archiveTraceCopy(file, archive);
    expect(collided).toBeDefined();
    expect(copy.to).not.toBe(collided);
    expect(fs.readFileSync(collided!, "utf8")).toBe("existing archive");
    expect(fs.readFileSync(copy.to, "utf8")).toBe("source");
  });

  it("refuses symlink files and symlink archive directories", () => {
    const { root, file, archive } = fixture();
    fs.writeFileSync(file, "raw");
    const link = path.join(root, "link.jsonl");
    fs.symlinkSync(file, link);
    expect(() => archiveTraceCopy(link, archive)).toThrow();
    fs.mkdirSync(archive);
    const archiveLink = path.join(root, "archive-link");
    fs.symlinkSync(archive, archiveLink, "dir");
    expect(() => archiveTraceCopy(file, archiveLink)).toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe("raw");
  });
});

describe("verified whole-tree archive copies", () => {
  it("copies every regular file, nested and empty directories, preserving relative names", () => {
    const { root, archive } = fixture();
    const source = path.join(root, "actor");
    fs.mkdirSync(path.join(source, "nested", "empty"), { recursive: true });
    const raw = Buffer.from([0, 255, 13, 10, 1]);
    fs.writeFileSync(path.join(source, "nested", "arbitrary.log"), raw);
    fs.writeFileSync(path.join(source, "trace.ndjson"), "{malformed\n");
    const result = archiveTraceTreeCopy(source, archive, { reason: "tree" }, fs.lstatSync(source));
    expect(result.from).toBe(source);
    expect(result.files).toEqual([
      { path: "nested/arbitrary.log", bytes: raw.length, sha256: sha256(raw) },
      { path: "trace.ndjson", bytes: 11, sha256: sha256(Buffer.from("{malformed\n")) },
    ]);
    expect(fs.readFileSync(path.join(result.to, "nested", "arbitrary.log"))).toEqual(raw);
    expect(fs.readFileSync(path.join(source, "nested", "arbitrary.log"))).toEqual(raw);
    expect(fs.statSync(path.join(result.to, "nested", "empty")).isDirectory()).toBe(true);
  });

  it("refuses archives inside the source, including via a symlink ancestor", () => {
    const { root } = fixture();
    const source = path.join(root, "actor");
    fs.mkdirSync(source);
    const alias = path.join(root, "alias");
    fs.symlinkSync(source, alias, "dir");
    expect(() => archiveTraceTreeCopy(source, path.join(source, "archive"))).toThrow(/outside|inside/i);
    expect(() => archiveTraceTreeCopy(source, path.join(alias, "archive"))).toThrow(/outside|inside|symlink/i);
    expect(fs.readdirSync(source)).toEqual([]);
  });

  it("refuses any symlink rather than silently omitting unknown tree entries", () => {
    const { root, archive } = fixture();
    const source = path.join(root, "actor");
    fs.mkdirSync(source);
    fs.writeFileSync(path.join(source, "raw.log"), "raw");
    fs.symlinkSync(path.join(source, "raw.log"), path.join(source, "linked.log"));
    expect(() => archiveTraceTreeCopy(source, archive)).toThrow(/symlink|regular|unsupported/i);
    expect(fs.readFileSync(path.join(source, "raw.log"), "utf8")).toBe("raw");
  });

  it.skipIf(process.platform === "win32")("refuses a real socket entry rather than omitting unknown data", async () => {
    const { root, archive } = fixture();
    const source = path.join(root, "actor");
    fs.mkdirSync(source);
    fs.writeFileSync(path.join(source, "raw.log"), "raw");
    const server = net.createServer();
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(path.join(source, "unknown.sock"), resolve);
    });
    try {
      expect(() => archiveTraceTreeCopy(source, archive)).toThrow(/unsupported|regular/i);
      expect(fs.readFileSync(path.join(source, "raw.log"), "utf8")).toBe("raw");
    } finally { await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve())); }
  });

  it.each(["earlier file", "tree membership"])("rejects changed %s before returning a receipt", change => {
    const { root, archive } = fixture();
    const source = path.join(root, "actor");
    fs.mkdirSync(source);
    fs.writeFileSync(path.join(source, "a.log"), "first");
    fs.writeFileSync(path.join(source, "b.log"), "second");
    const write = fs.writeSync.bind(fs);
    let calls = 0;
    vi.spyOn(fs, "writeSync").mockImplementation(((...args: unknown[]) => {
      const result = Reflect.apply(write, fs, args);
      if (++calls === 2) {
        if (change === "earlier file") fs.writeFileSync(path.join(source, "a.log"), "other");
        else fs.writeFileSync(path.join(source, "late.log"), "new evidence");
      }
      return result;
    }) as typeof fs.writeSync);
    expect(() => archiveTraceTreeCopy(source, archive)).toThrow(/changed|stable|inventory/i);
    vi.restoreAllMocks();
    expect(fs.readFileSync(path.join(source, "b.log"), "utf8")).toBe("second");
    if (change === "earlier file") expect(fs.readFileSync(path.join(source, "a.log"), "utf8")).toBe("other");
    else expect(fs.readFileSync(path.join(source, "late.log"), "utf8")).toBe("new evidence");
  });
});
