import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { watchBridgeStore, type BridgeChange } from "../src/mesh/change-notifier.js";

// Keep reason: smarty-dev#17515's recorded fleet idle regression did metadata work for every unrelated
// notification. Only notification delivery is controlled; directories, replacements and reads are real.
const roots: string[] = [];
const stops: Array<() => void> = [];
interface Watch {
  directory: string;
  fire: (event: string, name: string | Buffer | null | undefined) => void;
  watcher: EventEmitter & { close: ReturnType<typeof vi.fn> };
}
const fixture = () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-notifier-idle-")); roots.push(base);
  const root = path.join(base, "mesh");
  fs.mkdirSync(path.join(root, "participants"), { recursive: true });
  fs.mkdirSync(path.join(root, "host-leases"));
  const watches: Watch[] = [];
  vi.spyOn(fs, "watch").mockImplementation(((directory: string, fire: Watch["fire"]) => {
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    watches.push({ directory, fire, watcher });
    return watcher as unknown as fs.FSWatcher;
  }) as unknown as typeof fs.watch);
  const changes: BridgeChange[] = [], failures: Error[] = [];
  const latest = (name = "") => watches.filter(watch => watch.directory === path.join(root, name)).at(-1)!;
  const start = (presenceRelevant?: () => boolean) => {
    const stop = watchBridgeStore(root, kind => changes.push(kind), error => failures.push(error), presenceRelevant);
    stops.push(stop);
    return stop;
  };
  return { base, root, watches, latest, changes, failures, start };
};

afterEach(() => {
  for (const stop of stops.splice(0)) stop();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("bridge filesystem notification relevance", () => {
  it("does no metadata or canonical reads for a fleet-shaped burst of irrelevant names", () => {
    const f = fixture(); f.start();
    const stats = vi.spyOn(fs, "statSync"), reads = vi.spyOn(fs, "readFileSync");
    for (let index = 0; index < 1_371; index++) {
      for (const name of ["bridge-cursor.json", "state.json.123.tmp", "state.db-shm", ".lock", "outbox", "journal", "signal"]) {
        f.latest().fire(index % 2 ? "change" : "rename", name);
      }
      f.latest("host-leases").fire("rename", `${index}.json.tmp`);
      f.latest("participants").fire("change", `${index}.lock`);
    }
    expect(stats.mock.calls.length).toBe(0);
    expect(reads.mock.calls.length).toBe(0);
    expect(f.changes).toEqual([]);
    expect(f.failures).toEqual([]);
  });

  it.each(["state.json", "state.db", "state.db-wal", "state.db-journal", "events.jsonl", "generation"])(
    "keeps legacy change kinds and reads %s after both in-place and replacement hints", name => {
      const f = fixture();
      fs.writeFileSync(path.join(f.root, name), "initial");
      const values: string[] = [];
      stops.push(watchBridgeStore(f.root, () => values.push(fs.readFileSync(path.join(f.root, name), "utf8")), error => { throw error; }));
      const stats = vi.spyOn(fs, "statSync");
      fs.writeFileSync(path.join(f.root, name), "in-place");
      f.latest().fire("change", Buffer.from(name));
      fs.writeFileSync(path.join(f.base, "staged"), "replacement");
      fs.renameSync(path.join(f.base, "staged"), path.join(f.root, name));
      f.latest().fire("rename", name);
      expect(values).toEqual(["in-place", "replacement"]);
      expect(stats).toHaveBeenCalledTimes(2);
      f.start();
      f.latest().fire("change", name);
      expect(f.changes).toEqual([name === "events.jsonl" || name === "generation" ? "events" : "presence"]);
    },
  );

  it("skips unneeded presence metadata but still rebinds replaced directories and reads in-place JSON once relevant", () => {
    const f = fixture();
    let interested = false;
    f.start(() => interested);
    const stats = vi.spyOn(fs, "statSync");
    for (let index = 0; index < 1_371; index++) {
      f.latest("host-leases").fire("change", `${index}.json`);
      f.latest("participants").fire("rename", `${index}.json`);
    }
    expect(stats.mock.calls.length).toBe(0);
    expect(f.changes).toEqual([]);

    const old = f.latest("participants");
    fs.renameSync(path.join(f.root, "participants"), path.join(f.base, "old-participants"));
    fs.mkdirSync(path.join(f.root, "participants"));
    f.latest().fire("rename", "participants");
    expect(old.watcher.close).toHaveBeenCalledOnce();
    expect(f.latest("participants")).not.toBe(old);
    expect(f.changes).toEqual([]);

    interested = true;
    fs.writeFileSync(path.join(f.root, "participants", "target.json"), "first");
    f.latest("participants").fire("change", "target.json");
    expect(f.changes).toEqual(["presence"]);
    const observed: string[] = [];
    stops.push(watchBridgeStore(f.root, () => observed.push(fs.readFileSync(path.join(f.root, "participants", "target.json"), "utf8")), error => { throw error; }));
    f.latest("participants").fire("change", "target.json");
    fs.writeFileSync(path.join(f.root, "participants", "target.json"), "later");
    f.latest("participants").fire("change", "target.json");
    expect(observed).toEqual(["first", "later"]);
    expect(f.failures).toEqual([]);
  });

  it("rebinds a child removed and recreated between self and root hints", () => {
    const f = fixture(); f.start();
    const old = f.latest("host-leases");
    fs.renameSync(path.join(f.root, "host-leases"), path.join(f.base, "old-leases"));
    old.fire("rename", "host-leases");
    expect(old.watcher.close).toHaveBeenCalledOnce();
    fs.mkdirSync(path.join(f.root, "host-leases"));
    f.latest().fire("rename", "host-leases");
    expect(f.latest("host-leases")).not.toBe(old);
    f.latest("host-leases").fire("change", "lease.json");
    expect(f.changes).toEqual(["presence", "presence", "presence"]);
    expect(f.failures).toEqual([]);
  });

  it.each([null, undefined])("treats a missing filename (%s) as an unknown relevant hint", name => {
    const f = fixture(); f.start();
    f.latest().fire("change", name);
    expect(f.changes).toEqual(["presence", "events", "presence"]);
    f.changes.length = 0;
    f.latest("participants").fire("rename", name);
    expect(f.changes).toEqual(["presence"]);
    expect(f.failures).toEqual([]);
  });

  it.each([
    { name: null, recreate: false }, { name: null, recreate: true },
    { name: "mesh", recreate: false }, { name: "mesh", recreate: true },
  ])("fails closed on root removal/replacement ($name, recreate=$recreate), even without presence interest", ({ name, recreate }) => {
    const f = fixture(); f.start(() => false);
    fs.renameSync(f.root, path.join(f.base, "old-mesh"));
    if (recreate) fs.mkdirSync(f.root);
    f.latest().fire("rename", name);
    expect(f.failures[0]?.message).toMatch(/root was removed or replaced/);
    expect(f.failures).toHaveLength(1);
    expect(f.watches.every(watch => watch.watcher.close.mock.calls.length === 1)).toBe(true);
    f.latest().fire("change", "events.jsonl");
    expect(f.changes).toEqual([]);
  });

  it("reports native watcher errors once and stops callbacks, so the consumer can re-arm", () => {
    const f = fixture(); f.start();
    f.latest("participants").watcher.emit("error", Object.assign(new Error("Windows watch failed"), { code: "EPERM" }));
    f.latest().watcher.emit("error", new Error("late error"));
    f.latest().fire("change", "events.jsonl");
    expect(f.failures.map(error => error.message)).toEqual([expect.stringContaining("Windows watch failed")]);
    expect(f.changes).toEqual([]);
    f.start();
    f.latest().fire("change", "events.jsonl");
    expect(f.changes).toEqual(["events"]);
  });
});
