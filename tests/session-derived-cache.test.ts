import fs from "node:fs";
import crypto from "node:crypto";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { reconstructSessionLineage } from "../src/memory/lineage.js";
import { normalizeSession } from "../src/memory/normalize.js";
import { fingerprintSource } from "../src/memory/index.js";
import { clearSessionTextCache, sessionCacheUsage, withSessionFileSnapshot } from "../src/memory/session-file-cache.js";
const roots: string[] = [];
const file = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "derived-session-")); roots.push(root); return path.join(root, "session.jsonl"); };
const message = (id: string, text = id, parentId: string | null = null) => JSON.stringify({ type: "message", id, parentId, message: { role: "user", content: [{ type: "text", text }] } }) + "\n";
const probe = (p: string) => withSessionFileSnapshot(p, () => ({ lineage: reconstructSessionLineage(p, "active"), normalized: normalizeSession(p, 100), fingerprint: fingerprintSource(p) }));
afterEach(() => { vi.restoreAllMocks(); clearSessionTextCache(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
describe("derived session cache", () => {
  it("does zero reads, parsing and hashing for unchanged 150 MiB; retains no source text", () => {
    const p = file();
    const line = JSON.stringify({ type: "custom", padding: "x".repeat(1024 * 1024 - 32) }) + "\n";
    const fd = fs.openSync(p, "w");
    for (let n = 0; n < 151; n++) fs.writeSync(fd, line);
    fs.writeSync(fd, message("a")); fs.closeSync(fd);
    expect(fs.statSync(p).size).toBeGreaterThan(150 * 1024 * 1024);
    const warm = probe(p);
    const stats = vi.spyOn(fs, "statSync");
    const reads = vi.spyOn(fs, "readSync"), whole = vi.spyOn(fs, "readFileSync"), parse = vi.spyOn(JSON, "parse"), hash = vi.spyOn(crypto, "createHash");
    const update = vi.spyOn(Object.getPrototypeOf(crypto.createHash("sha256")), "update"); hash.mockClear();
    for (let n = 0; n < 10; n++) {
      const idle = probe(p);
      expect(idle.lineage).toBe(warm.lineage); expect(idle.normalized).toBe(warm.normalized); expect(idle.fingerprint).toBe(warm.fingerprint);
    }
    expect(stats).toHaveBeenCalledTimes(10);
    expect(reads).not.toHaveBeenCalled(); expect(whole).not.toHaveBeenCalled(); expect(parse).not.toHaveBeenCalled(); expect(hash).not.toHaveBeenCalled(); expect(update).not.toHaveBeenCalled();
    expect(sessionCacheUsage()[0]!.pendingBytes).toBe(0);
    expect(warm.normalized.entries).toHaveLength(1);
  }, 30_000);
  it("parses only appended complete lines once across all warm projections", () => {
    const p = file(); fs.writeFileSync(p, message("a")); probe(p);
    const parse = vi.spyOn(JSON, "parse"), reads = vi.spyOn(fs, "readSync");
    const update = vi.spyOn(Object.getPrototypeOf(crypto.createHash("sha256")), "update");
    const appended = message("b", "tail", "a");
    fs.appendFileSync(p, appended); const result = probe(p);
    expect(parse).toHaveBeenCalledTimes(1); expect(reads).toHaveBeenCalledTimes(1);
    const hashedBytes = update.mock.calls.filter(([value]) => Buffer.isBuffer(value)).reduce((sum, [value]) => sum + (value as Buffer).length, 0);
    expect(hashedBytes).toBe(Buffer.byteLength(appended));
    expect(result.lineage.leafId).toBe("b"); expect(result.normalized.entries.map(e => e.text)).toEqual(["a", "tail"]);
    expect(result.fingerprint?.sourceHash).toBe(crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex"));
  });
  it("preserves partial lines and UTF-8 bytes split across append and chunk boundaries", () => {
    const p = file(); const bytes = Buffer.from(message("a", "雪🌍")); const at = bytes.indexOf(Buffer.from("雪")) + 1;
    fs.writeFileSync(p, bytes.subarray(0, at)); expect(probe(p).normalized.entries).toHaveLength(0);
    const parse = vi.spyOn(JSON, "parse"); fs.appendFileSync(p, bytes.subarray(at, bytes.length - 1));
    expect(probe(p).normalized.entries).toHaveLength(0); expect(parse).not.toHaveBeenCalled();
    fs.appendFileSync(p, "\n"); expect(probe(p).normalized.entries[0]!.text).toBe("雪🌍"); expect(parse).toHaveBeenCalledTimes(1);
    normalizeSession(p, 70_000);
    const empty = message("b", "", "a"), textOffset = empty.indexOf('"text":"') + 8;
    const prefix = message("b", "x".repeat(65_535 - textOffset) + "雪🌍", "a");
    expect(Buffer.from(prefix).indexOf(Buffer.from("雪"))).toBe(65_535);
    fs.appendFileSync(p, prefix);
    expect(probe(p).lineage.leafId).toBe("b"); expect(sessionCacheUsage()[0]!.pendingBytes).toBe(0);
    expect(normalizeSession(p, 70_000).entries[1]!.text.endsWith("雪🌍")).toBe(true);
  });
  it.each(["inode", "truncate", "equal-size"])("rebuilds all projections after %s replacement", kind => {
    const p = file(); fs.writeFileSync(p, message("a", "old")); const old = probe(p);
    const next = message("b", kind === "truncate" ? "x" : "new");
    if (kind === "inode") { fs.writeFileSync(p + ".new", next); fs.renameSync(p + ".new", p); }
    else { fs.writeFileSync(p, next); const future = new Date(Date.now() + 2000); fs.utimesSync(p, future, future); }
    const parse = vi.spyOn(JSON, "parse"); const result = probe(p);
    expect(parse.mock.calls.length).toBeGreaterThan(0); expect(result.lineage.leafId).toBe("b"); expect(result.normalized.entries[0]!.text).toBe(kind === "truncate" ? "x" : "new"); expect(result.fingerprint?.sourceHash).not.toBe(old.fingerprint?.sourceHash);
  });
  it("projects changed active lineage after an append without parsing old lines", () => {
    const p = file(); fs.writeFileSync(p, message("a"));
    let lineage = reconstructSessionLineage(p, "active"); normalizeSession(p, 100, { lineage });
    const parse = vi.spyOn(JSON, "parse"); fs.appendFileSync(p, message("b", "tail", "a"));
    lineage = reconstructSessionLineage(p, "active");
    expect(normalizeSession(p, 100, { lineage }).entries.map(e => e.text)).toEqual(["a", "tail"]);
    expect(parse).toHaveBeenCalledTimes(1);
  });
  it("reuses lineage for fresh native branch arrays without holding live entries", () => {
    const p = file(); fs.writeFileSync(p, message("a"));
    const first = reconstructSessionLineage(p, "active", { entries: [{ id: "a" }], leafId: "a", revision: "native:a" });
    const hash = vi.spyOn(crypto, "createHash");
    expect(reconstructSessionLineage(p, "active", { entries: [{ id: "a" }], leafId: "a", revision: "native:a" })).toBe(first);
    expect(hash).not.toHaveBeenCalled();
    const changed = reconstructSessionLineage(p, "active", { entries: [{ id: "different" }], leafId: "a" });
    expect(changed.fingerprint).not.toBe(first.fingerprint);
  });
  it("keeps retained heap bounded after normalizing 150 MiB of real message text", () => {
    const p = file(), bundle = path.join(path.dirname(p), "projection.mjs");
    buildSync({ stdin: { contents: `export {normalizeSession} from ${JSON.stringify(path.resolve("src/memory/normalize.ts"))}; export {reconstructSessionLineage} from ${JSON.stringify(path.resolve("src/memory/lineage.ts"))}; export {fingerprintSource} from ${JSON.stringify(path.resolve("src/memory/index.ts"))};`, resolveDir: process.cwd(), loader: "ts" }, bundle: true, platform: "node", format: "esm", outfile: bundle });
    const result = spawnSync(process.execPath, ["--expose-gc", "--input-type=module", "-e", `
      import fs from 'node:fs'; import {pathToFileURL} from 'node:url';
      const api=await import(pathToFileURL(process.argv[1])); const file=process.argv[2];
      const fd=fs.openSync(file,'w');
      for(let n=0;n<151;n++) fs.writeSync(fd,JSON.stringify({type:'message',id:String(n).padStart(32,'0'),parentId:n?String(n-1).padStart(32,'0'):null,message:{role:'user',content:[{type:'text',text:'x'.repeat(1024*1024)}]}})+'\\n');
      fs.closeSync(fd); global.gc();global.gc();const before=process.memoryUsage().heapUsed;
      api.reconstructSessionLineage(file,'active');api.normalizeSession(file,100);api.fingerprintSource(file);
      global.gc();global.gc();console.log(JSON.stringify({retainedHeap:process.memoryUsage().heapUsed-before,bytes:fs.statSync(file).size}));
    `, bundle, p], { encoding: "utf8", timeout: 30_000 });
    expect(result.status, result.stderr).toBe(0);
    const memory = JSON.parse(result.stdout) as { retainedHeap: number; bytes: number };
    expect(memory.bytes).toBeGreaterThan(150 * 1024 * 1024);
    expect(memory.retainedHeap).toBeLessThan(16 * 1024 * 1024);
  }, 35_000);
  it("separates normalization policies and invalidates unavailable files", () => {
    const p = file(); fs.writeFileSync(p, message("a", "abcdef"));
    expect(normalizeSession(p, 2).entries[0]!.text).not.toBe(normalizeSession(p, 100).entries[0]!.text);
    fs.unlinkSync(p); expect(fingerprintSource(p)).toBeNull(); expect(normalizeSession(p, 2).indexCoverage.reasons).toContain("source_unavailable");
  });
});
