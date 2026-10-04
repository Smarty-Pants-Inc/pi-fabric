import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createCommitStats } from "../src/mesh/commit-stats.js";

const roots: string[] = [];
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-commit-stats-"));
  roots.push(root);
  return { root, file: path.join(root, "stats.jsonl") };
};
afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const rows = (file: string) => fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));

describe("PI_FABRIC_COMMIT_STATS", () => {
  it("appends once per minute, counts each commit once and resets interval buckets", () => {
    const { file } = setup();
    vi.useFakeTimers();
    const stats = createCommitStats(file);
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
    const stats = createCommitStats(file);
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
