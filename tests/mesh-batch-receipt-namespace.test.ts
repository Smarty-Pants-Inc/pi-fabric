import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshStore } from "../src/mesh/store.js";

// Keep reason: data safety. Exercise batch preparation, namespace invalidation, and
// failures on BOTH sides of the live append with real files and forwarded OS barriers.
const roots: string[] = [];
const from = { id: "session:batch-namespace", name: "batch-namespace", kind: "main" as const };
const packet = (key: string) => ({ topic: "mesh.batch-namespace", from, dedupeKey: key, text: key });
const setup = (archived: boolean) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-batch-namespace-"));
  fs.chmodSync(base, 0o700); roots.push(base);
  const root = path.join(base, "mesh"), receipts = path.join(root, "event-receipts");
  fs.mkdirSync(root, { mode: 0o700 });
  if (archived) {
    const archive = path.join(base, "archive"); fs.mkdirSync(archive, { mode: 0o700 });
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
  }
  const open = () => new MeshStore(root, 4096, 100, { maxEventLogBytes: 230 * 1024 * 1024 });
  return { root, receipts, open, mesh: open() };
};
const held = (root: string) => fs.existsSync(path.join(root, ".lock"));
const observe = (root: string, hook?: (target: string, isHeld: boolean, directory: boolean) => void) => {
  const sync = fs.fsyncSync.bind(fs);
  const calls: Array<{ target: string; held: boolean; directory: boolean }> = [];
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    const target = fs.readlinkSync(`/proc/self/fd/${fd}`), isHeld = held(root), directory = fs.fstatSync(fd).isDirectory();
    calls.push({ target, held: isHeld, directory });
    hook?.(target, isHeld, directory);
    sync(fd);
  });
  return calls;
};
const fail = () => Object.assign(new Error("batch namespace EIO"), { code: "EIO" });
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe.skipIf(process.platform !== "linux")("batch prepared receipt namespace (#18675)", () => {
  for (const archived of [false, true]) {
    it(`prepares once before acquisition, pays fresh receipt leaves, and retries once (archive=${archived})`, async () => {
      const { root, receipts, mesh } = setup(archived);
      // Batch extent is not the subject of this test; retain its real I/O, not its time bound.
      vi.spyOn(performance, "now").mockReturnValue(0);
      const calls = observe(root);
      const inputs = [packet("one"), { topic: "mesh.batch-namespace", from, text: "unkeyed" }, packet("two")];
      const events = await mesh.publishBatch(inputs);
      expect(events).toHaveLength(3);
      expect(calls.filter(call => call.target === receipts && !call.held)).toHaveLength(1);
      expect(calls.filter(call => call.target === receipts && call.held)).toHaveLength(6);
      // Observe the real persisted receipt contract, not just call counts.
      const saved = fs.readdirSync(receipts).map(name => JSON.parse(fs.readFileSync(path.join(receipts, name), "utf8")));
      expect(saved.map(event => event.id).sort()).toEqual([events[0]!.id, events[2]!.id].sort());
      expect(await mesh.publishBatch(inputs.filter(input => "dedupeKey" in input))).toEqual([events[0], events[2]]);
      expect(mesh.read({ limit: 100 })).toEqual(events);
    });

    it(`preparation EIO refuses before any acquisition/append (archive=${archived})`, async () => {
      const { root, receipts, mesh } = setup(archived);
      let faults = 0;
      const calls = observe(root, (target, isHeld) => {
        if (target === receipts && !isHeld && faults++ === 0) throw fail();
      });
      await expect(mesh.publishBatch([packet("prepare")])).rejects.toThrow("batch namespace EIO");
      expect(calls.some(call => call.held)).toBe(false);
      expect(mesh.read({ limit: 100 })).toEqual([]);
      const events = await mesh.publishBatch([packet("prepare")]);
      expect(await mesh.publishBatch([packet("prepare")])).toEqual(events);
      expect(mesh.read({ limit: 100 })).toEqual(events);
    });

    it(`receipt directory replaced after preparation pays the full fallback (archive=${archived})`, async () => {
      const { root, receipts, mesh } = setup(archived);
      const mkdir = fs.mkdirSync.bind(fs);
      let changed = false;
      vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
        if (!changed && String(file) === path.join(root, ".lock")) {
          changed = true; fs.renameSync(receipts, receipts + "-old"); mkdir(receipts, { mode: 0o700 });
        }
        return mkdir(file, options);
      }) as typeof fs.mkdirSync);
      const calls = observe(root);
      const events = await mesh.publishBatch([packet("changed")]);
      expect(changed).toBe(true);
      expect(calls.some(call => call.held && call.directory && call.target === "/")).toBe(true);
      expect(await mesh.publishBatch([packet("changed")])).toEqual(events);
      expect(mesh.read({ limit: 100 })).toEqual(events);
    });

    it(`intent leaf EIO appends nothing and an exact retry commits once (archive=${archived})`, async () => {
      const { root, receipts, mesh } = setup(archived);
      let faults = 0;
      observe(root, (target, isHeld) => { if (target === receipts && isHeld && faults++ === 0) throw fail(); });
      await expect(mesh.publishBatch([packet("intent")])).rejects.toThrow("batch namespace EIO");
      expect(mesh.read({ limit: 100 })).toEqual([]);
      const events = await mesh.publishBatch([packet("intent")]);
      expect(await mesh.publishBatch([packet("intent")])).toEqual(events);
      expect(mesh.read({ limit: 100 })).toEqual(events);
    });
  }

  it("an archived receipt-file EIO leaves a recoverable intent and never republishes the live event", async () => {
    const { root, receipts, mesh, open } = setup(true);
    let faults = 0;
    observe(root, (target, isHeld, directory) => {
      if (isHeld && !directory && path.dirname(target) === receipts && /[a-f0-9]{64}\.json\./.test(path.basename(target)) && faults++ === 0) throw fail();
    });
    await expect(mesh.publishBatch([packet("receipt")])).rejects.toThrow("outcome is uncertain");
    const events = mesh.read({ limit: 100 }); expect(events).toHaveLength(1);
    expect(fs.readdirSync(receipts).filter(name => name.endsWith(".pending.json"))).toHaveLength(1);
    const reopened = open();
    expect(await reopened.publishBatch([packet("receipt")])).toEqual(events);
    expect(await reopened.publishBatch([packet("receipt")])).toEqual(events);
    expect(reopened.read({ limit: 100 })).toEqual(events);
  });
});
