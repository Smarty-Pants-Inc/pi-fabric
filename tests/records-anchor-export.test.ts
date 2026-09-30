import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AnchorExport, ANCHORS_PER_SEGMENT } from "../src/records/anchor-export.js";
import { parseAnchors, type RecordsAnchor } from "../src/records/chain.js";
import { lockFile, normalizeServiceConfig } from "../src/records/server.js";

const anchor = (seq: number): RecordsAnchor => ({ org: "test", seq, hash: seq === 0 ? null : seq.toString(16).padStart(64, "0"), at: "2026-09-30T12:00:00.000Z" });
const heartbeat = (dir: string) => JSON.parse(fs.readFileSync(path.join(dir, "HEARTBEAT.json"), "utf8"));
const segment = (dir: string, n: number) => path.join(dir, `anchors-${String(n).padStart(12, "0")}.jsonl`);
const prefix = (text: string): RecordsAnchor[] => text.slice(0, text.lastIndexOf("\n") + 1).split("\n").slice(0, -1).map((line) => JSON.parse(line));
const dirs: string[] = [];
const fresh = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "records-anchor-export-"));
  dirs.push(root);
  const dir = path.join(root, "anchors");
  return { root, dir, writer: new AnchorExport(dir, (file) => lockFile(file, 5)) };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe("records anchor export configuration", () => {
  const base = { org: "test", origin: "node", socket: "/run/test.sock" };
  it("defaults to five minutes beneath the existing public status directory, or the configured export path", () => {
    expect(normalizeServiceConfig(base).anchorExport).toBeUndefined();
    expect(normalizeServiceConfig({ ...base, statusFile: "/var/lib/test-records/status/test.json" }).anchorExport).toEqual({ directory: "/var/lib/test-records/status/anchors", intervalMs: 300_000 });
    expect(normalizeServiceConfig({ ...base, anchorExport: { directory: "/public/anchors", intervalMs: 600_000 } }).anchorExport).toEqual({ directory: "/public/anchors", intervalMs: 600_000 });
  });
  it("bounds invalid/unsafe cadences in the existing service config", () => {
    const config = (intervalMs: unknown) => normalizeServiceConfig({ ...base, anchorExport: { directory: "/public", intervalMs } }).anchorExport!.intervalMs;
    expect(config("1000")).toBe(300_000);
    expect(config(0)).toBe(1_000);
    expect(config(100_000_000)).toBe(86_400_000);
  });
});

describe.skipIf(process.platform === "win32")("append-only records anchor segments", () => {
  it("idle ticks refresh the heartbeat across restarts without appending segment bytes", async () => {
    const { dir, writer } = fresh();
    const now = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-30T12:00:00.000Z"));
    await writer.publish(anchor(0));
    expect(heartbeat(dir)).toEqual({ org: "test", lastSeq: 0, lastHash: null, checkedAt: "2026-09-30T12:00:00.000Z" });
    const before = fs.readFileSync(segment(dir, 1), "utf8");
    for (const checkedAt of ["2026-09-30T12:05:00.000Z", "2026-09-30T12:10:00.000Z"]) {
      now.mockReturnValue(Date.parse(checkedAt));
      await new AnchorExport(dir, (file) => lockFile(file, 5)).publish({ ...anchor(0), at: checkedAt });
      expect(heartbeat(dir)).toEqual({ org: "test", lastSeq: 0, lastHash: null, checkedAt });
      expect(fs.readFileSync(segment(dir, 1), "utf8")).toBe(before);
    }
    expect(fs.readdirSync(dir).filter((file) => file.endsWith(".jsonl"))).toHaveLength(1);
  });

  it("a changed anchor appends once and updates the heartbeat frontier and checkedAt", async () => {
    const { dir, writer } = fresh();
    const now = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-30T12:00:00.000Z"));
    await writer.publish(anchor(0));
    for (const changed of [anchor(1), { ...anchor(1), hash: "f".repeat(64) }]) {
      now.mockReturnValue(Date.now() + 300_000);
      await writer.publish(changed);
      expect(heartbeat(dir)).toEqual({ org: "test", lastSeq: changed.seq, lastHash: changed.hash, checkedAt: new Date(Date.now()).toISOString() });
      const before = fs.readFileSync(segment(dir, 1), "utf8");
      now.mockReturnValue(Date.now() + 300_000);
      await writer.publish(changed);
      expect(heartbeat(dir).checkedAt).toBe(new Date(Date.now()).toISOString());
      expect(fs.readFileSync(segment(dir, 1), "utf8")).toBe(before);
    }
    expect(prefix(fs.readFileSync(segment(dir, 1), "utf8"))).toEqual([anchor(0), anchor(1), { ...anchor(1), hash: "f".repeat(64) }]);
  });

  it("atomically replaces the heartbeat and retries a failed heartbeat rename without duplicate data", async () => {
    const { dir, writer } = fresh();
    const now = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-09-30T12:00:00.000Z"));
    await writer.publish(anchor(1));
    const before = fs.readFileSync(path.join(dir, "HEARTBEAT.json"), "utf8");
    now.mockReturnValue(Date.now() + 300_000);
    const rename = fs.promises.rename.bind(fs.promises);
    let fail = true;
    vi.spyOn(fs.promises, "rename").mockImplementation(async (...args) => {
      if (String(args[1]).endsWith("HEARTBEAT.json")) {
        expect(fs.readFileSync(path.join(dir, "HEARTBEAT.json"), "utf8")).toBe(before);
        expect(fs.statSync(args[0]).mode & 0o777).toBe(0o644);
        expect(JSON.parse(fs.readFileSync(args[0], "utf8"))).toEqual({ org: "test", lastSeq: 2, lastHash: anchor(2).hash, checkedAt: "2026-09-30T12:05:00.000Z" });
        if (fail) { fail = false; throw new Error("heartbeat rename failed"); }
      }
      await rename(...args);
    });
    await expect(writer.publish(anchor(2))).rejects.toThrow("heartbeat rename failed");
    expect(fs.readFileSync(path.join(dir, "HEARTBEAT.json"), "utf8")).toBe(before);
    expect(fs.readdirSync(dir).filter((file) => file.endsWith(".tmp"))).toEqual([]);
    const data = fs.readFileSync(segment(dir, 1), "utf8");
    await writer.publish(anchor(2));
    expect(fs.readFileSync(segment(dir, 1), "utf8")).toBe(data);
    expect(prefix(data)).toEqual([anchor(1), anchor(2)]);
    expect(heartbeat(dir)).toEqual({ org: "test", lastSeq: 2, lastHash: anchor(2).hash, checkedAt: "2026-09-30T12:05:00.000Z" });
    expect(fs.readdirSync(dir).filter((file) => file.endsWith(".tmp"))).toEqual([]);
  });

  it("publishes the existing anchor format and deduplicates across restarts, but retains a changed digest", async () => {
    const { dir, writer } = fresh();
    await writer.publish(anchor(0));
    await writer.publish(anchor(1));
    const before = fs.readFileSync(segment(dir, 1), "utf8");
    const restarted = new AnchorExport(dir, (file) => lockFile(file, 5));
    expect(await restarted.publish({ ...anchor(1), at: "2026-09-30T13:00:00.000Z" })).toEqual(anchor(1));
    expect(fs.readFileSync(segment(dir, 1), "utf8")).toBe(before);
    const changed = { ...anchor(1), hash: "f".repeat(64) };
    await restarted.publish(changed);
    expect(prefix(fs.readFileSync(segment(dir, 1), "utf8"))).toEqual([anchor(0), anchor(1), changed]);
    expect(parseAnchors(prefix(before))).toEqual([{ seq: 1, hash: anchor(1).hash }]);
  });

  it("rolls over at 9,999 anchors without changing any earlier byte or sealed segment", async () => {
    const { dir, writer } = fresh();
    fs.mkdirSync(dir);
    const seeded = Array.from({ length: ANCHORS_PER_SEGMENT - 1 }, (_, n) => `${JSON.stringify(anchor(n + 1))}\n`).join("");
    fs.writeFileSync(segment(dir, 1), seeded, { mode: 0o644 });
    await writer.publish(anchor(9_999));
    const sealed = fs.readFileSync(segment(dir, 1), "utf8");
    expect(sealed).toBe(`${seeded}${JSON.stringify(anchor(9_999))}\n`);
    expect(prefix(sealed)).toHaveLength(9_999);
    await writer.publish(anchor(10_000));
    await writer.publish(anchor(10_001));
    expect(fs.readFileSync(segment(dir, 1), "utf8")).toBe(sealed);
    const second = prefix(fs.readFileSync(segment(dir, 2), "utf8"));
    expect(second).toEqual([anchor(10_000), anchor(10_001)]);
    expect(parseAnchors(prefix(sealed))).toHaveLength(9_999);
    expect(parseAnchors(second)).toHaveLength(2);
  });

  it("sets records-owned 0755/0644 permissions despite a restrictive umask", async () => {
    const { dir, writer } = fresh();
    const mask = process.umask(0o077);
    try { await writer.publish(anchor(1)); await writer.publish(anchor(1)); } finally { process.umask(mask); }
    const directory = fs.statSync(dir);
    const file = fs.statSync(segment(dir, 1));
    expect(directory.mode & 0o777).toBe(0o755);
    expect(file.mode & 0o777).toBe(0o644);
    expect(directory.uid).toBe(process.getuid!());
    expect(file.uid).toBe(process.getuid!());
    const pulse = fs.statSync(path.join(dir, "HEARTBEAT.json"));
    expect(pulse.mode & 0o777).toBe(0o644);
    expect(pulse.uid).toBe(process.getuid!());
    expect(fs.statSync(path.join(dir, ".publish.lock")).mode & 0o777).toBe(0o600);
  });

  it("fsyncs file before new-segment rename and fsyncs O_APPEND lines plus the directory", async () => {
    const { dir, writer } = fresh();
    const events: string[] = [];
    const open = fs.promises.open.bind(fs.promises);
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const file = String(args[0]);
      const handle = await open(...args);
      const sync = handle.sync.bind(handle);
      handle.sync = async () => { events.push(`sync:${file === dir ? "directory" : file.endsWith(".tmp") ? "temp" : file.endsWith(".jsonl") ? "segment" : "ancestor"}`); await sync(); };
      if (typeof args[1] === "number" && (args[1] & fs.constants.O_APPEND)) events.push("O_APPEND");
      return handle;
    });
    const rename = fs.promises.rename.bind(fs.promises);
    vi.spyOn(fs.promises, "rename").mockImplementation(async (...args) => { events.push("rename"); await rename(...args); });
    await writer.publish(anchor(1));
    expect(events.slice(-6)).toEqual(["sync:temp", "rename", "sync:directory", "sync:temp", "rename", "sync:directory"]);
    expect(events.slice(0, -6).length).toBeGreaterThan(0);
    expect(events.slice(0, -6).every((event) => event === "sync:ancestor")).toBe(true);
    events.length = 0;
    await writer.publish(anchor(2));
    expect(events.slice(-6)).toEqual(["O_APPEND", "sync:segment", "sync:directory", "sync:temp", "rename", "sync:directory"]);
    expect(events.slice(0, -6).every((event) => event === "sync:ancestor")).toBe(true);
    events.length = 0;
    await writer.publish(anchor(2));
    expect(events.slice(-5)).toEqual(["sync:segment", "sync:directory", "sync:temp", "rename", "sync:directory"]);
    expect(events.slice(0, -5).every((event) => event === "sync:ancestor")).toBe(true);
  });

  it("a killed mid-append writer leaves a valid prefix, releases its lock and never rewrites the torn tail", async () => {
    const { dir, writer } = fresh();
    await writer.publish(anchor(1));
    const script = `
      import fs from "node:fs";
      import { AnchorExport } from ${JSON.stringify(path.resolve(__dirname, "../src/records/anchor-export.ts"))};
      import { lockFile } from ${JSON.stringify(path.resolve(__dirname, "../src/records/server.ts"))};
      const open = fs.promises.open.bind(fs.promises);
      fs.promises.open = async (...args) => {
        const handle = await open(...args);
        if (typeof args[1] === "number" && (args[1] & fs.constants.O_APPEND)) {
          const write = handle.write.bind(handle);
          handle.write = async (line) => {
            await write(line.subarray(0, Math.floor(line.length / 2)));
            await handle.sync();
            process.kill(process.pid, "SIGKILL");
            await new Promise(() => {});
          };
        }
        return handle;
      };
      await new AnchorExport(${JSON.stringify(dir)}, (file) => lockFile(file, 5)).publish(${JSON.stringify(anchor(2))});
    `;
    const crashed = spawnSync("bun", ["-e", script], { encoding: "utf8", timeout: 20_000 });
    expect(crashed.error, crashed.stderr).toBeUndefined();
    expect(crashed.signal, crashed.stderr).toBe("SIGKILL");
    const torn = fs.readFileSync(segment(dir, 1), "utf8");
    expect(torn.endsWith("\n")).toBe(false);
    expect(prefix(torn)).toEqual([anchor(1)]);
    await new AnchorExport(dir, (file) => lockFile(file, 5)).publish(anchor(2));
    expect(fs.readFileSync(segment(dir, 1), "utf8")).toBe(torn);
    expect(prefix(fs.readFileSync(segment(dir, 2), "utf8"))).toEqual([anchor(2)]);
  }, 30_000);

  it("a failed new-segment rename publishes nothing and a retry creates the first segment", async () => {
    const { dir, writer } = fresh();
    const rename = vi.spyOn(fs.promises, "rename").mockRejectedValueOnce(new Error("crash before rename"));
    await expect(writer.publish(anchor(1))).rejects.toThrow("crash before rename");
    expect(fs.readdirSync(dir).filter((file) => file.endsWith(".jsonl"))).toEqual([]);
    rename.mockRestore();
    await writer.publish(anchor(1));
    expect(prefix(fs.readFileSync(segment(dir, 1), "utf8"))).toEqual([anchor(1)]);
  });

  it("a post-rename directory fsync failure retries durability without duplicating a published anchor", async () => {
    const { dir, writer } = fresh();
    const open = fs.promises.open.bind(fs.promises);
    let fail = true;
    const mocked = vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      if (String(args[0]) === dir) {
        const sync = handle.sync.bind(handle);
        handle.sync = async () => {
          if (fail) { fail = false; throw new Error("directory fsync failed"); }
          await sync();
        };
      }
      return handle;
    });
    await expect(writer.publish(anchor(1))).rejects.toThrow("directory fsync failed");
    const before = fs.readFileSync(segment(dir, 1), "utf8");
    await writer.publish(anchor(1));
    expect(fs.readFileSync(segment(dir, 1), "utf8")).toBe(before);
    expect(prefix(before)).toEqual([anchor(1)]);
    mocked.mockRestore();
  });

  it("serializes concurrent publishers without replacing a numbered segment", async () => {
    const { dir } = fresh();
    await Promise.all(Array.from({ length: 4 }, () => new AnchorExport(dir, (file) => lockFile(file, 5)).publish(anchor(1))));
    expect(prefix(fs.readFileSync(segment(dir, 1), "utf8"))).toEqual([anchor(1)]);
    expect(fs.readdirSync(dir).filter((file) => file.endsWith(".jsonl"))).toHaveLength(1);
  });

  it("refuses malformed complete lines and symlink segments rather than erasing history", async () => {
    const { root, dir, writer } = fresh();
    await writer.publish(anchor(1));
    fs.appendFileSync(segment(dir, 1), "not json\n");
    const malformed = fs.readFileSync(segment(dir, 1), "utf8");
    await expect(writer.publish(anchor(2))).rejects.toThrow();
    expect(fs.readFileSync(segment(dir, 1), "utf8")).toBe(malformed);
    const other = path.join(root, "other");
    fs.writeFileSync(other, `${JSON.stringify(anchor(2))}\n`);
    fs.symlinkSync(other, segment(dir, 2));
    await expect(writer.publish(anchor(3))).rejects.toThrow();
    expect(fs.readFileSync(other, "utf8")).toBe(`${JSON.stringify(anchor(2))}\n`);
  });
});
