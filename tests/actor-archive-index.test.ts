import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorArchiveIndex } from "../src/agents/actor-archive-index.js";
import { AgentManager } from "../src/agents/manager.js";
import { ACTOR_RUN_ARCHIVE_PENDING_FILE } from "../src/agents/archive-custody.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-archive-index-")); roots.push(root);
  const index = new ActorArchiveIndex(root);
  const runId = (n: number) => n.toString(16).padStart(32, "0");
  const file = (n: number) => path.join(root, runId(n), ACTOR_RUN_ARCHIVE_PENDING_FILE);
  const seed = (n: number, actorId = "actor-a", sessionFile = "/session/a.jsonl") => {
    fs.mkdirSync(path.dirname(file(n)), { recursive: true });
    fs.writeFileSync(file(n), JSON.stringify({ format: 1, runId: runId(n), actorId, sessionFile }));
    return runId(n);
  };
  return { root, index, runId, file, seed };
};
describe("host-wide actor archive source index (#4250)", () => {
  it("does one cold scan for 50 actors and no archive I/O per supervisor lookup", () => {
    const f = setup();
    for (let n = 1; n <= 200; n++) f.seed(n, `actor-${n % 50}`, `/session/${n % 50}.jsonl`);
    const read = vi.spyOn(fs, "readFileSync"), list = vi.spyOn(fs, "readdirSync");
    for (let n = 0; n < 50; n++) expect(f.index.sources(`actor-${n}`, `/session/${n}.jsonl`, 100).size).toBe(4);
    expect(list).toHaveBeenCalledTimes(1); expect(read).toHaveBeenCalledTimes(200);
    read.mockClear(); list.mockClear();
    const stat = vi.spyOn(fs, "lstatSync");
    for (let n = 0; n < 100; n++) f.index.sources(`actor-${n % 50}`, `/session/${n % 50}.jsonl`, 101);
    expect(stat).not.toHaveBeenCalled(); expect(list).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled();
    f.index.sources("actor-0", "/session/0.jsonl", 1101);
    expect(list).not.toHaveBeenCalled(); expect(read).not.toHaveBeenCalled(); // Identity revalidation, no repeated JSON.
  });
  it("refreshes just a locally staged/retired marker without scanning the changed root", () => {
    const f = setup(); f.index.sources("actor-a", "/session/a.jsonl", 100);
    const run = f.seed(1); const list = vi.spyOn(fs, "readdirSync");
    f.index.refreshRun(run);
    expect(f.index.sources("actor-a", "/session/a.jsonl", 101).has(run)).toBe(true);
    fs.rmSync(f.file(1)); f.index.refreshRun(run);
    expect(f.index.sources("actor-a", "/session/a.jsonl", 102).size).toBe(0);
    expect(list).not.toHaveBeenCalled();
  });
  it.each(["replace", "in-place"])("discovers externally changed marker identity (%s), even with restored mtime", change => {
    const f = setup(), run = f.seed(1); f.index.sources("actor-a", "/session/a.jsonl", 100);
    const before = fs.statSync(f.file(1));
    const body = JSON.stringify({ format: 1, runId: run, actorId: "actor-b", sessionFile: "/session/b.jsonl" });
    if (change === "replace") { fs.writeFileSync(f.file(1) + ".new", body); fs.renameSync(f.file(1) + ".new", f.file(1)); }
    else fs.writeFileSync(f.file(1), body);
    fs.utimesSync(f.file(1), before.atime, before.mtime);
    expect(f.index.sources("actor-a", "/session/a.jsonl", 1101).size).toBe(0);
    expect(f.index.sources("actor-b", "/session/b.jsonl", 1101).has(run)).toBe(true);
  });
  it("finds an external marker added inside an existing run without a root generation change", () => {
    const f = setup(); fs.mkdirSync(path.dirname(f.file(1))); f.index.sources("actor-a", "/session/a.jsonl", 100);
    const run = f.seed(1);
    expect(f.index.sources("actor-a", "/session/a.jsonl", 1101).has(run)).toBe(true);
  });
  it("does not use cached custody as unlink authority after a marker is replaced", async () => {
    const f = setup(), run = f.seed(1);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, { runRoot: f.root }); managers.push(manager);
    expect(manager.actorArchiveSources("actor-a", "/session/a.jsonl").has(run)).toBe(true);
    f.seed(1, "actor-b", "/session/b.jsonl");
    await manager.commitActorArchive(run, "actor-a", "/session/a.jsonl");
    expect(fs.existsSync(f.file(1))).toBe(true);
    await manager.commitActorArchive(run, "actor-b", "/session/b.jsonl");
    expect(fs.existsSync(f.file(1))).toBe(false);
  });
  it("keeps unknown/malformed/oversized markers and retries repairs instead of caching absence", () => {
    const f = setup(), run = f.seed(1);
    fs.writeFileSync(f.file(1), "{"); expect(f.index.sources("actor-a", "/session/a.jsonl", 100).size).toBe(0);
    f.seed(1); expect(f.index.sources("actor-a", "/session/a.jsonl", 1101).has(run)).toBe(true);
    fs.writeFileSync(f.file(1), "x".repeat(4097)); expect(f.index.confirmedSource(run, "actor-a", "/session/a.jsonl")).toBeUndefined();
    expect(fs.existsSync(f.file(1))).toBe(true);
    expect(f.index.confirmedSource("../escape", "actor-a", "/session/a.jsonl")).toBeUndefined();
  });
  it("does not bind another opened marker inode to a replace/restore path stamp", () => {
    const f = setup(), run = f.seed(1); f.index.sources("actor-a", "/session/a.jsonl", 100);
    const open = fs.openSync; let raced = false;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: number, ...args: unknown[]) => {
      if (String(file) !== f.file(1) || raced) return Reflect.apply(open, fs, [file, flags, ...args]);
      raced = true; fs.renameSync(f.file(1), f.file(1) + ".saved"); f.seed(1, "actor-b", "/session/b.jsonl");
      const fd = open(file, flags); fs.rmSync(f.file(1)); fs.renameSync(f.file(1) + ".saved", f.file(1)); return fd;
    }) as typeof fs.openSync);
    expect(f.index.confirmedSource(run, "actor-a", "/session/a.jsonl")).toBeUndefined();
    expect(f.index.confirmedSource(run, "actor-a", "/session/a.jsonl")).toBe(path.dirname(f.file(1)));
  });
  it.skipIf(process.platform === "win32")("never discharges cached custody through a replaced symlink run root", () => {
    const f = setup(), run = f.seed(1); f.index.sources("actor-a", "/session/a.jsonl", 100);
    fs.renameSync(f.root, f.root + ".saved"); roots.push(f.root + ".saved"); fs.symlinkSync(f.root + ".saved", f.root, "dir");
    expect(f.index.confirmedSource(run, "actor-a", "/session/a.jsonl")).toBeUndefined();
    expect(fs.existsSync(f.file(1))).toBe(true);
  });
  it.skipIf(process.platform === "win32")("rejects symlinked markers and run directories", () => {
    const f = setup(); f.seed(1); fs.symlinkSync(path.dirname(f.file(1)), path.dirname(f.file(2)), "dir");
    fs.mkdirSync(path.dirname(f.file(3))); fs.symlinkSync(f.file(1), f.file(3));
    expect([...f.index.sources("actor-a", "/session/a.jsonl", 100).keys()]).toEqual([f.runId(1)]);
  });
});
