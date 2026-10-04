import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createCommitStats } from "../src/mesh/commit-stats.js";

const roots: string[] = [];
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-commit-stats-"));
  roots.push(root);
  return { root, file: path.join(root, "stats.jsonl") };
};
const processKey = Symbol.for("pi-fabric.mesh.commit-stats");
const processGlobals = globalThis as typeof globalThis & {
  [processKey]?: { version: number; dispose(): void };
};
afterEach(() => {
  processGlobals[processKey]?.dispose();
  delete processGlobals[processKey];
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const rows = (file: string) => fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));

describe("PI_FABRIC_COMMIT_STATS", () => {
  it("shares one writer across two module generations and reuses older/newer registry versions", async () => {
    const { root, file } = setup();
    vi.useFakeTimers();
    const baselineExitListeners = process.listenerCount("exit");
    const source = fileURLToPath(new URL("../src/mesh/commit-stats.ts", import.meta.url));
    const copies = ["release-a.ts", "release-b.ts"].map(name => path.join(root, name));
    for (const copy of copies) fs.copyFileSync(source, copy);
    const firstModule = await import(/* @vite-ignore */ pathToFileURL(copies[0]!).href);
    const first = firstModule.createCommitStats(file)!;
    first.record(123, ["sessions/a"]);
    vi.advanceTimersByTime(30_000);
    const secondModule = await import(/* @vite-ignore */ pathToFileURL(copies[1]!).href);
    expect(secondModule.createCommitStats).not.toBe(firstModule.createCommitStats);
    const second = secondModule.createCommitStats(path.join(root, "ignored-release-sink"))!;
    expect(second).toBe(first);
    second.record(456, ["topology/hosts/a"]);
    for (const version of [0, 2]) {
      processGlobals[processKey]!.version = version;
      expect(secondModule.createCommitStats(file)).toBe(first);
    }
    expect(vi.getTimerCount()).toBe(1);
    expect(process.listenerCount("exit")).toBe(baselineExitListeners + 1);
    vi.advanceTimersByTime(30_000);
    expect(rows(file)).toHaveLength(1);
    expect(rows(file)[0]).toMatchObject({ commits: 2, bytesWritten: 579,
      byReason: { "legacy-session": { commits: 1, bytesWritten: 123 }, "host-lease": { commits: 1, bytesWritten: 456 } } });
    vi.advanceTimersByTime(60_000);
    expect(rows(file)).toHaveLength(2);
    expect(rows(file)[1]).toMatchObject({ commits: 0, bytesWritten: 0 });
    expect(fs.existsSync(path.join(root, "ignored-release-sink"))).toBe(false);
    processGlobals[processKey]!.dispose();
    expect(vi.getTimerCount()).toBe(0);
    expect(process.listenerCount("exit")).toBe(baselineExitListeners);
  });

  it("keeps disabled opt-in captured across a release reload", async () => {
    const { file } = setup();
    vi.useFakeTimers();
    vi.stubEnv("PI_FABRIC_COMMIT_STATS", "");
    expect(createCommitStats()).toBeUndefined();
    vi.stubEnv("PI_FABRIC_COMMIT_STATS", file);
    vi.resetModules();
    const secondModule = await import("../src/mesh/commit-stats.js");
    expect(secondModule.createCommitStats()).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("appends once per minute, counts each commit once and resets interval buckets", () => {
    const { file } = setup();
    vi.useFakeTimers();
    const stats = createCommitStats(file)!;
    stats.record(123, ["sessions/a", "topology/hosts/a", "topology/hosts/b"]);
    stats.record(456, ["topology/participants/a"]);
    vi.advanceTimersByTime(59_999);
    expect(fs.existsSync(file)).toBe(false);
    vi.advanceTimersByTime(1);
    expect(rows(file)[0]).toMatchObject({ version: 1, pid: process.pid, commits: 2, bytesWritten: 579,
      byReason: { "host-lease+legacy-session": { commits: 1, bytesWritten: 123 },
        participant: { commits: 1, bytesWritten: 456 } } });
    vi.advanceTimersByTime(60_000);
    expect(rows(file)[1]).toMatchObject({ commits: 0, bytesWritten: 0, byReason: {} });
  });

  it("never propagates sink errors and retains counters until an append succeeds", () => {
    const { root } = setup();
    vi.useFakeTimers();
    const directory = path.join(root, "later");
    const file = path.join(directory, "stats");
    const stats = createCommitStats(file)!;
    stats.record(100, ["state/a"]);
    expect(() => vi.advanceTimersByTime(60_000)).not.toThrow();
    fs.mkdirSync(directory);
    vi.advanceTimersByTime(60_000);
    expect(rows(file)[0]).toMatchObject({ commits: 1, bytesWritten: 100 });
  });

  it("records actual state.json bytes after rename; absent deletes, empty batches and CAS failures do not count", async () => {
    const { root, file } = setup();
    vi.useFakeTimers();
    vi.stubEnv("PI_FABRIC_COMMIT_STATS", file);
    vi.resetModules();
    const { MeshStore } = await import("../src/mesh/store.js");
    vi.stubEnv("PI_FABRIC_COMMIT_STATS", path.join(root, "ignored-late-change"));
    const identity = { id: "main", name: "main", kind: "main" as const };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    let bytes = 0;
    await mesh.put({ key: "sessions/a", value: "雪", identity });
    bytes += fs.statSync(path.join(mesh.root, "state.json")).size;
    await mesh.writeBatch({ identity, ops: [
      { kind: "put", key: "topology/hosts/a", value: 1 },
      { kind: "put", key: "topology/participants/a", value: 2 },
    ] });
    bytes += fs.statSync(path.join(mesh.root, "state.json")).size;
    await mesh.delete({ key: "sessions/a" });
    bytes += fs.statSync(path.join(mesh.root, "state.json")).size;
    await mesh.delete({ key: "missing" });
    await mesh.writeBatch({ identity, ops: [] });
    await mesh.confirmWritable();
    await expect(mesh.put({ key: "topology/hosts/a", value: 3, identity, ifVersion: 0 })).rejects.toThrow();
    const rename = vi.spyOn(fs, "renameSync").mockImplementationOnce(() => { throw new Error("rename failed"); });
    await expect(mesh.put({ key: "state/failed", value: 1, identity })).rejects.toThrow("rename failed");
    rename.mockRestore();
    vi.advanceTimersByTime(60_000);
    expect(rows(file)[0]).toMatchObject({ commits: 3, bytesWritten: bytes,
      byReason: { "legacy-session": { commits: 2 }, "host-lease+participant": { commits: 1 } } });
    expect(fs.existsSync(path.join(root, "ignored-late-change"))).toBe(false);
  });

  it("off by default: no timer, collection or late opt-in", async () => {
    const { root, file } = setup();
    vi.useFakeTimers();
    vi.stubEnv("PI_FABRIC_COMMIT_STATS", "");
    vi.resetModules();
    const { MeshStore } = await import("../src/mesh/store.js");
    expect(vi.getTimerCount()).toBe(0);
    vi.stubEnv("PI_FABRIC_COMMIT_STATS", file);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    await mesh.put({ key: "state/a", value: 1, identity: { id: "main", name: "main", kind: "main" } });
    vi.advanceTimersByTime(120_000);
    expect(fs.existsSync(file)).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });
});
