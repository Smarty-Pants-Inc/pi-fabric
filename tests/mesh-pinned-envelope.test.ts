import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-pinned-")); roots.push(directory); return directory; };
const identity = { id: "session:pinned", name: "main", kind: "main" as const };
afterEach(() => { vi.restoreAllMocks(); for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });
const replace = (file: string, bytes: string) => { const temporary = `${file}.replacement`; fs.writeFileSync(temporary, bytes); fs.renameSync(temporary, file); };
const damaged = (generation: number, body = '"format":1,"entries":{') => `{"readGeneration":"${randomUUID()}","highWater":${generation},${body}`;
const noOp = (mesh: MeshStore, kind: string) => kind === "delete" ? mesh.delete({ key: "test/absent" }) : mesh.writeBatch({ identity,
  ops: [{ kind: "put", key: "test/key", value: "skipped", ifVersion: 999, onConflict: "skip" }],
});
const setup = async () => {
  const directory = root(), mesh = new MeshStore(directory, 1024, 100, { maxStateBytes: 65536 });
  const acked = await mesh.put({ key: "test/key", value: "acked", identity });
  const canonical = path.join(directory, "state.json"), checkpoint = path.join(directory, "state.durable.json"), completion = path.join(directory, "state.durability-completion.json");
  return { directory, mesh, acked, canonical, checkpoint, completion, bytes: fs.readFileSync(checkpoint, "utf8") };
};

describe("#2479 round-six F13 pinned snapshot envelopes", () => {
  for (const kind of ["delete", "batch"]) for (const hint of ["missing", "unreadable"]) {
    it.each(['"format":1,"entries":{', '"format":1,"entries":null}', '"format":1,"entries":{}}garbage'])
    (`restart ${kind} with ${hint} completion preserves N despite intact N+1 header / body %s`, async body => {
      const { directory, acked, canonical, checkpoint, completion, bytes } = await setup();
      replace(canonical, damaged(acked.version + 1, body));
      if (hint === "missing") fs.rmSync(completion); else replace(completion, "unreadable completion");
      const restarted = new MeshStore(directory, 65536, 100);
      expect(restarted.get("test/key", { fresh: true })).toMatchObject({ value: "acked", version: acked.version });
      await noOp(restarted, kind);
      expect(fs.readFileSync(checkpoint, "utf8")).toBe(bytes);
      const fresh = new MeshStore(directory, 65536, 100);
      expect(fresh.get("test/key", { fresh: true })).toMatchObject({ value: "acked", version: acked.version });
      fs.rmSync(canonical);
      expect(new MeshStore(directory, 65536, 100).get("test/key", { fresh: true })?.value).toBe("acked");
      const next = await fresh.put({ key: "test/later", value: "later", identity });
      expect(next.version).toBeGreaterThan(acked.version);
      expect(new MeshStore(directory, 65536, 100).get("test/key", { fresh: true })?.value).toBe("acked");
    });
  }

  it.each(["body", "size", "io"])("rejects %s validation failure of the pinned successor without replacing N", async fault => {
    const { mesh, acked, canonical, checkpoint, completion, bytes } = await setup();
    fs.rmSync(completion);
    const link = fs.linkSync.bind(fs), read = fs.readSync.bind(fs);
    const pins = new Set<number>(), open = fs.openSync.bind(fs);
    vi.spyOn(fs, "linkSync").mockImplementation((source, target) => {
      if (String(source) === canonical) replace(canonical, damaged(acked.version + 1, fault === "size" ? '"format":1,"entries":{' + " ".repeat(65536) : undefined));
      link(source, target);
    });
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      if (String(file).startsWith(`${checkpoint}.`) && String(file).endsWith(".tmp")) pins.add(fd);
      return fd;
    });
    vi.spyOn(fs, "readSync").mockImplementation(((fd: number, ...args: unknown[]) => {
      if (fault === "io" && pins.has(fd)) throw Object.assign(new Error("pinned read unavailable"), { code: "EIO" });
      return (read as (...args: unknown[]) => number)(fd, ...args);
    }) as typeof fs.readSync);
    const barrier = vi.spyOn(fs, "fsync");
    await expect(mesh.put({ key: "test/new", value: 1, identity })).rejects.toThrow();
    expect(barrier).not.toHaveBeenCalled();
    expect(fs.readFileSync(checkpoint, "utf8")).toBe(bytes);
    expect(JSON.parse(fs.readFileSync(completion, "utf8"))).toHaveProperty("error");
    vi.restoreAllMocks(); // descriptor numbers may be reused by a fresh reader
    expect(new MeshStore(mesh.root, 65536, 100).get("test/key", { fresh: true })?.value).toBe("acked");
  });

  it("validates the pinned inode rather than a subsequently replaced canonical path", async () => {
    const { mesh, canonical, checkpoint } = await setup();
    const link = fs.linkSync.bind(fs);
    vi.spyOn(fs, "linkSync").mockImplementation((source, target) => {
      link(source, target);
      if (String(source) === canonical) replace(canonical, damaged(999));
    });
    const next = await mesh.put({ key: "test/new", value: "valid pin", identity });
    expect(JSON.parse(fs.readFileSync(checkpoint, "utf8")).entries["test/new"]).toMatchObject({ value: "valid pin", version: next.version });
    expect(new MeshStore(mesh.root, 65536, 100).get("test/new", { fresh: true })?.value).toBe("valid pin");
  });

  it("rejects concurrent replacement of the private candidate pin after descriptor validation", async () => {
    const { mesh, checkpoint, bytes } = await setup();
    const files = new Map<number, string>(), open = fs.openSync.bind(fs), sync = fs.fsync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsync").mockImplementation((fd, callback) => {
      replace(files.get(fd)!, damaged(999));
      sync(fd, callback);
    });
    await expect(mesh.put({ key: "test/new", value: 1, identity })).rejects.toThrow();
    expect(fs.readFileSync(checkpoint, "utf8")).toBe(bytes);
    expect(fs.readdirSync(mesh.root).filter(file => file.endsWith(".tmp"))).toEqual([]);
  });

  it.skipIf(process.platform === "win32")("rejects a candidate symlink to the validated canonical inode rather than publishing a mutable alias", async () => {
    const { mesh, canonical, checkpoint, bytes } = await setup();
    const files = new Map<number, string>(), open = fs.openSync.bind(fs), sync = fs.fsync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsync").mockImplementation((fd, callback) => {
      const candidate = files.get(fd)!;
      fs.rmSync(candidate); fs.symlinkSync(canonical, candidate);
      sync(fd, callback);
    });
    await expect(mesh.put({ key: "test/new", value: 1, identity })).rejects.toThrow("pinned snapshot changed");
    expect(fs.readFileSync(checkpoint, "utf8")).toBe(bytes);
    expect(fs.lstatSync(checkpoint).isFile()).toBe(true);
    vi.restoreAllMocks();
    fs.rmSync(canonical);
    expect(new MeshStore(mesh.root, 65536, 100).get("test/key", { fresh: true })?.value).toBe("acked");
  });

  it("does not accept a completion hint over a checkpoint with a valid header but malformed envelope", async () => {
    const { mesh, acked, canonical, checkpoint, completion } = await setup();
    const next = await mesh.put({ key: "test/new", value: 1, identity });
    const valid = fs.readFileSync(canonical, "utf8");
    replace(checkpoint, damaged(next.version));
    expect(JSON.parse(fs.readFileSync(completion, "utf8")).generation).toBeGreaterThan(acked.version);
    const fileBarrier = vi.spyOn(fs, "fsync");
    await expect(noOp(new MeshStore(mesh.root, 65536, 100), "delete")).rejects.toThrow();
    expect(fileBarrier).not.toHaveBeenCalled();
    expect(fs.readFileSync(canonical, "utf8")).toBe(valid);
  });

  it("pins and validates the last complete envelope in the legacy concatenated recovery model", async () => {
    const directory = root(), mesh = new MeshStore(directory, 1024, 100, { maxStateBytes: 65536 });
    const legacy = fs.readFileSync(path.resolve("tests/fixtures/mesh-state-pre-atomic-audit.json"), "utf8");
    replace(path.join(directory, "state.json"), '{"format":1,"entries":{},"highWater":1}\n' + legacy);
    await mesh.delete({ key: "test/absent" });
    expect(JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8")).generation).toBe(42);
    fs.rmSync(path.join(directory, "state.json"));
    expect(new MeshStore(directory, 65536, 100).get("test/old", { fresh: true })?.version).toBe(42);
  });
});
