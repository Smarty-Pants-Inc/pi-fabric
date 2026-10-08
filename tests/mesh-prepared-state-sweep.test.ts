import childProcess from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const identity = { id: "sweep", name: "sweep", kind: "main" as const };
const rootOf = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-prepared-sweep-"));
  roots.push(root);
  return root;
};
const prepared = (root: string, pid: number, modifiedAt?: number) => {
  const file = path.join(root, `state.json.${pid}.${randomUUID()}.prepared.tmp`);
  fs.writeFileSync(file, "staged state");
  if (modifiedAt !== undefined) fs.utimesSync(file, modifiedAt / 1000, modifiedAt / 1000);
  return file;
};
const deadPid = () => {
  const child = childProcess.spawnSync(process.execPath, ["-e", ""], { timeout: 5_000 });
  expect(child.error).toBeUndefined();
  expect(child.status).toBe(0);
  expect(child.pid).toBeGreaterThan(0);
  return child.pid;
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("abandoned prepared mesh state", () => {
  it("removes a dead PID on open, keeps live/own PIDs and non-matching names, and logs the count", () => {
    const root = rootOf(), pid = deadPid();
    const dead = prepared(root, pid);
    const live = prepared(root, process.ppid);
    const own = prepared(root, process.pid, Date.now() - 2 * 60 * 60_000);
    const uuid = randomUUID();
    const other = ["state.json", `other.json.${pid}.${uuid}.prepared.tmp`,
      `state.json.${pid}.not-a-uuid.prepared.tmp`, `state.json.0${pid}.${uuid}.prepared.tmp`,
      `state.json.${pid}.${uuid}.prepared.tmp.extra`, `state.json.${pid}.${uuid}.tmp`,
      `stateXjson.${pid}.${uuid}.prepared.tmp`, `state.json.0.${uuid}.prepared.tmp`,
      `state.json.-${pid}.${uuid}.prepared.tmp`].map(name => path.join(root, name));
    for (const file of other) fs.writeFileSync(file, "unrelated");
    const directory = path.join(root, `state.json.${pid}.${randomUUID()}.prepared.tmp`);
    fs.mkdirSync(directory);
    let link: string | undefined;
    if (process.platform !== "win32") {
      link = path.join(root, `state.json.${pid}.${randomUUID()}.prepared.tmp`);
      fs.symlinkSync(other[0]!, link);
    }
    const log = vi.spyOn(console, "info").mockImplementation(() => {});
    new MeshStore(root, 65536, 100);
    expect(fs.existsSync(dead)).toBe(false);
    for (const file of [live, own, ...other, directory, ...(link ? [link] : [])]) {
      expect(fs.existsSync(file)).toBe(true);
    }
    expect(log).toHaveBeenCalledExactlyOnceWith("[mesh] Removed 1 abandoned prepared state file(s)");
  });

  it("does not treat EPERM as a dead process", () => {
    const root = rootOf(), pid = deadPid(), file = prepared(root, pid);
    const kill = process.kill;
    vi.spyOn(process, "kill").mockImplementation(((target: number, signal?: number | NodeJS.Signals) => {
      if (target === pid) throw Object.assign(new Error("denied"), { code: "EPERM" });
      return kill(target, signal);
    }) as typeof process.kill);
    new MeshStore(root, 65536, 100);
    expect(fs.existsSync(file)).toBe(true);
  });

  it("removes an hour-old file only when the current live process started after it", () => {
    const root = rootOf();
    const file = prepared(root, process.ppid, Date.UTC(2000, 0, 1));
    new MeshStore(root, 65536, 100);
    expect(fs.existsSync(file)).toBe(false);
  });

  it.each(["older", "unknown", "unreadable"])("keeps an old live PID with %s birth evidence", evidence => {
    const root = rootOf();
    const file = prepared(root, process.ppid, Date.UTC(2000, 0, 1));
    vi.spyOn(childProcess, "execFileSync").mockImplementation(() => {
      if (evidence === "unreadable") throw new Error("unavailable");
      if (evidence === "unknown") return "not a start time";
      return process.platform === "win32" ? "1999-01-01T00:00:00.000Z" : "Fri Jan  1 00:00:00 1999";
    });
    new MeshStore(root, 65536, 100);
    expect(fs.existsSync(file)).toBe(true);
  });

  it("does not query process birth time for a file at or below one hour old", () => {
    const root = rootOf(), now = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(now);
    const file = prepared(root, process.ppid, now - 60 * 60_000);
    const birth = vi.spyOn(childProcess, "execFileSync");
    new MeshStore(root, 65536, 100);
    expect(fs.existsSync(file)).toBe(true);
    expect(birth).not.toHaveBeenCalled();
  });

  it("defers periodic cleanup inside a bounded registry-fenced write", async () => {
    const root = rootOf(), pid = deadPid();
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const store = new MeshStore(root, 65536, 100);
    const file = prepared(root, pid);
    now += 10 * 60_000;
    await store.withTryLock(() => store.put({ key: "sweep/try", value: 1, identity }));
    expect(fs.existsSync(file)).toBe(true);
    await store.put({ key: "sweep/try", value: 2, identity });
    expect(fs.existsSync(file)).toBe(false);
  });

  it("sweeps later writes no more than once every ten minutes and outside custody", async () => {
    const root = rootOf(), pid = deadPid();
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const store = new MeshStore(root, 65536, 100);
    const dead = prepared(root, pid);
    const unlink = fs.unlinkSync;
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
      if (file === dead) expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
      return unlink(file);
    });
    await store.put({ key: "sweep/test", value: 1, identity });
    expect(fs.existsSync(dead)).toBe(true);
    now += 10 * 60_000 - 1;
    await store.put({ key: "sweep/test", value: 2, identity });
    expect(fs.existsSync(dead)).toBe(true);
    now++;
    await store.put({ key: "sweep/test", value: 3, identity });
    expect(fs.existsSync(dead)).toBe(false);
    const next = prepared(root, pid);
    await store.put({ key: "sweep/test", value: 4, identity });
    expect(fs.existsSync(next)).toBe(true);
    expect(store.get("sweep/test")?.value).toBe(4);
  });
});
