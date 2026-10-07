import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProgramStore } from "../src/programs/store.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-program-lock-")); roots.push(root);
  return new ProgramStore(path.join(root, "programs"));
};
const pauseIndex = (store: ProgramStore) => {
  let release!: () => void; let entered!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const ready = new Promise<void>(resolve => { entered = resolve; });
  const rename = fs.promises.rename.bind(fs.promises);
  let paused = false;
  vi.spyOn(fs.promises, "rename").mockImplementation(async (from, to) => {
    if (!paused && to === path.join(store.directory, "index.json")) { paused = true; entered(); await blocked; }
    return rename(from, to);
  });
  return { ready, release };
};

describe("program store owner fencing", () => {
  it("A12 racing stale reapers cannot rename a successor's live lock", async () => {
    const store = fixture(); const pause = pauseIndex(store);
    const lock = path.join(store.directory, ".lock"); fs.mkdirSync(lock, { recursive: true });
    fs.writeFileSync(path.join(lock, "owner"), `dead-owner\n2147483647\n${Date.now() - 60_000}\n`);
    let releaseRename!: () => void; let renameEntered!: () => void;
    const gate = new Promise<void>(resolve => { releaseRename = resolve; });
    const entered = new Promise<void>(resolve => { renameEntered = resolve; });
    // Preserve pauseIndex's wrapper while gating the first recovery rename.
    const rename = vi.mocked(fs.promises.rename).getMockImplementation()!;
    let firstRename = true; const movedOwners: string[] = [];
    vi.spyOn(fs.promises, "rename").mockImplementation(async (from, to) => {
      if (from === lock) {
        if (firstRename) { firstRename = false; renameEntered(); await gate; }
        const owner = fs.readFileSync(path.join(lock, "owner"), "utf8");
        await rename(from, to); movedOwners.push(owner); return;
      }
      return rename(from, to);
    });
    const first = store.save({ name: "first", code: "return 1;" }, "typescript");
    await entered;
    const second = store.save({ name: "second", code: "return 2;" }, "typescript");
    await new Promise(resolve => setTimeout(resolve, 60)); releaseRename();
    try {
      await pause.ready; await new Promise(resolve => setTimeout(resolve, 60));
      expect(movedOwners.every(owner => owner.startsWith("dead-owner\n"))).toBe(true);
    } finally { pause.release(); await Promise.all([first, second]); }
    expect((await store.list()).map(record => record.name).sort()).toEqual(["first", "second"]);
  });
  it("A12 does not steal an aged lock from a paused live writer or lose successor registrations", async () => {
    const store = fixture(); const pause = pauseIndex(store);
    const first = store.save({ name: "first", code: "return 1;" }, "typescript");
    await pause.ready;
    const lock = path.join(store.directory, ".lock");
    const owner = path.join(lock, "owner");
    const old = Date.now() - 60_000;
    if (fs.existsSync(owner)) {
      const lines = fs.readFileSync(owner, "utf8").split("\n"); lines[2] = String(old); fs.writeFileSync(owner, lines.join("\n"));
    }
    fs.utimesSync(lock, new Date(old), new Date(old));
    let completed = false;
    const second = store.save({ name: "second", code: "return 2;" }, "typescript").then(value => { completed = true; return value; });
    try {
      await new Promise(resolve => setTimeout(resolve, 60));
      expect(completed).toBe(false);
    } finally { pause.release(); await Promise.all([first, second]); }
    expect((await store.list()).map(record => record.name).sort()).toEqual(["first", "second"]);
  });
  it("A12 late original-owner release cannot remove a replacement owner's lock", async () => {
    const store = fixture(); const pause = pauseIndex(store);
    const first = store.save({ name: "first", code: "return 1;" }, "typescript");
    await pause.ready;
    const lock = path.join(store.directory, ".lock");
    fs.renameSync(lock, `${lock}.original`);
    fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), `replacement\n${process.pid}\n${Date.now()}\n`);
    pause.release(); await first;
    expect(fs.existsSync(lock)).toBe(true);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toContain("replacement");
  });
  it("A12 refuses age-only recovery when an old lock has no confirmed owner", async () => {
    const store = fixture(); fs.mkdirSync(path.join(store.directory, ".lock"), { recursive: true });
    const old = new Date(Date.now() - 60_000); fs.utimesSync(path.join(store.directory, ".lock"), old, old);
    await expect(store.save({ name: "unsafe", code: "return 1;" }, "typescript")).rejects.toThrow(/lock/);
    expect(await store.list()).toEqual([]);
  });
});
