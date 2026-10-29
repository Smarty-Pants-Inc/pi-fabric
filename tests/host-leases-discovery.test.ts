// Keep reason: comms-loop responsiveness and lease/custody data safety (smarty-dev#9805).
// Fixtures are intentionally retained: this lane does not execute cleanup/deletion paths.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  hostLeasePath, readHostLease, readHostLeaseCurrent, readHostLeases, readHostLeasesAsync,
  readHostLeasesForIds, withHostLeaseDirectoryCustody, type FabricHostLease,
} from "../src/topology/host-leases.js";
import type { HostLeaseMesh } from "../src/topology/host-lease-lock.js";

const lease = (id: string, updatedAt = 1_000): FabricHostLease => ({
  id, rootId: `root:${id}`, identityId: `identity:${id}`, updatedAt, expiresAt: updatedAt + 60_000,
  incarnationToken: `token:${updatedAt}`,
});
const fixture = (count = 1) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-discovery-"));
  const directory = path.join(root, "host-leases");
  fs.mkdirSync(directory);
  const put = (value: FabricHostLease) => fs.writeFileSync(hostLeasePath(root, value.id), JSON.stringify({ format: 1, ...value }));
  for (let i = 0; i < count; i++) put(lease(`host:${i}`));
  return { root, directory, put };
};
afterEach(() => vi.restoreAllMocks());

describe("complete off-thread lease discovery", () => {
  it("reads all 1229 entries (including expired ones) without synchronous filesystem IO", async () => {
    const { root } = fixture(1229);
    const denySync = () => { throw new Error("synchronous discovery on comms loop"); };
    vi.spyOn(fs, "readdirSync").mockImplementation(denySync);
    vi.spyOn(fs, "statSync").mockImplementation(denySync);
    vi.spyOn(fs, "readFileSync").mockImplementation(denySync);
    let otherTurn = false;
    setImmediate(() => { otherTurn = true; });
    const found = await readHostLeasesAsync(root);
    expect(otherTurn).toBe(true);
    expect(found.size).toBe(1229);
    expect(found.get("host:1228")).toEqual(lease("host:1228"));
    expect(found.get("host:0")!.expiresAt).toBeLessThan(Date.now());
  });

  it("sees preserved-mtime atomic replacement, in-place edits, and complete directory replacement", async () => {
    const { root, directory, put } = fixture(3);
    expect((await readHostLeasesAsync(root)).get("host:0")).toEqual(lease("host:0"));
    const file = hostLeasePath(root, "host:0"), before = fs.statSync(file);
    const temporary = path.join(root, "replacement.tmp");
    fs.writeFileSync(temporary, JSON.stringify({ format: 1, ...lease("host:0", 2_000) }));
    fs.utimesSync(temporary, before.atime, before.mtime);
    fs.renameSync(temporary, file);
    expect((await readHostLeasesAsync(root)).get("host:0")).toEqual(lease("host:0", 2_000));
    put(lease("host:1", 3_000));
    expect((await readHostLeasesAsync(root)).get("host:1")).toEqual(lease("host:1", 3_000));
    fs.renameSync(directory, path.join(root, "retained-host-leases"));
    fs.mkdirSync(directory);
    put(lease("new-host", 4_000));
    expect([...(await readHostLeasesAsync(root)).values()]).toEqual([lease("new-host", 4_000)]);
    expect(readHostLease(root, "host:0")).toBeUndefined();
    fs.renameSync(directory, path.join(root, "retained-next-leases"));
    expect((await readHostLeasesAsync(root)).size).toBe(0);
  });

  it("retains exact bigint file identity when a Windows file id exceeds Number precision", async () => {
    const { root, put } = fixture();
    const stat = fs.promises.stat.bind(fs.promises);
    let ino = 2n ** 54n;
    const original = await stat(hostLeasePath(root, "host:0"), { bigint: true });
    vi.spyOn(fs.promises, "stat").mockImplementation((async (file: fs.PathLike) => {
      await stat(file, { bigint: true });
      return { ...original, ino };
    }) as typeof fs.promises.stat);
    expect((await readHostLeasesAsync(root)).get("host:0")?.updatedAt).toBe(1_000);
    put(lease("host:0", 2_000));
    ino++;
    expect(Number(ino)).toBe(Number(ino - 1n));
    expect((await readHostLeasesAsync(root)).get("host:0")?.updatedAt).toBe(2_000);
  });

  it("reports invalid/misnamed/unreadable files and never caches a failed read", async () => {
    const { root, directory } = fixture();
    fs.writeFileSync(path.join(directory, "misnamed.json"), JSON.stringify({ format: 1, ...lease("elsewhere") }));
    const read = fs.promises.readFile.bind(fs.promises);
    const spy = vi.spyOn(fs.promises, "readFile").mockImplementation((async (file: fs.PathLike, ...rest: unknown[]) => {
      if (String(file) === hostLeasePath(root, "host:0")) throw Object.assign(new Error("busy"), { code: "EBUSY" });
      return (read as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.promises.readFile);
    const problems: string[] = [];
    expect((await readHostLeasesAsync(root, problems)).size).toBe(0);
    expect(problems).toContain(`${hostLeasePath(root, "host:0")}: EBUSY`);
    expect(problems).toContain(`${path.join(directory, "misnamed.json")}: invalid`);
    spy.mockRestore();
    expect((await readHostLeasesAsync(root)).get("host:0")).toEqual(lease("host:0"));
  });

  it("keeps compatibility fallback separate from strict destructive reads", async () => {
    const { root, put } = fixture();
    await readHostLeasesAsync(root);
    put(lease("host:0", 2_000));
    vi.spyOn(fs.promises, "readFile").mockRejectedValue(Object.assign(new Error("denied"), { code: "EACCES" }));
    const problems: string[] = [];
    expect((await readHostLeasesAsync(root, problems)).get("host:0")).toEqual(lease("host:0"));
    expect(problems).toContain(`${hostLeasePath(root, "host:0")}: EACCES`);
    vi.spyOn(fs, "readFileSync").mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EACCES" }); });
    expect(readHostLeaseCurrent(root, "host:0")).toBeUndefined();
  });

  it("does not let a pending async read poison a newer synchronous cache observation", async () => {
    const { root, put } = fixture();
    const read = fs.promises.readFile.bind(fs.promises);
    let release!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const started = new Promise<void>(resolve => { entered = resolve; });
    vi.spyOn(fs.promises, "readFile").mockImplementation((async (file: fs.PathLike) => {
      const text = await read(file, "utf8");
      entered(); await gate; return text;
    }) as typeof fs.promises.readFile);
    const pending = readHostLeasesAsync(root);
    await started;
    put(lease("host:0", 2_000));
    const fresh = readHostLease(root, "host:0");
    release();
    expect((await pending).get("host:0")?.updatedAt).toBe(1_000); // Its own earlier observation, not a cached future answer.
    expect(readHostLease(root, "host:0")).toBe(fresh);
    expect(fresh?.updatedAt).toBe(2_000);
  });

  it("point-reads exact known hosts without discovery, but does not pretend to find root/identity aliases", () => {
    const { root, put } = fixture(1229);
    put({ ...lease("arbitrary-file-id"), rootId: "target-root", identityId: "target-identity" });
    const listing = vi.spyOn(fs, "readdirSync");
    const selected = readHostLeasesForIds(root, ["host:1", "host:1", "target-root"]);
    expect([...selected.keys()]).toEqual(["host:1"]);
    expect(listing).not.toHaveBeenCalled();
    expect(readHostLeases(root).get("arbitrary-file-id")?.rootId).toBe("target-root");
  });

  it("async directory custody still gates arbitrary-id relevant aliases and the whole namespace", async () => {
    const { root, put } = fixture(3);
    put({ ...lease("alias"), rootId: "target-root" });
    const gates: string[] = [];
    const mesh = { root, leaseCustody: async <T>(file: string, operation: () => T | Promise<T>) => {
      gates.push(file); return operation();
    } } as HostLeaseMesh;
    const syncListing = vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("sync enumeration"); });
    let ran = false;
    await withHostLeaseDirectoryCustody(mesh, () => { ran = true; }, new Set(["target-root"]));
    expect(ran).toBe(true);
    expect(gates).toHaveLength(2);
    expect(gates[1]).toBe(hostLeasePath(root, "alias"));
    expect(syncListing).not.toHaveBeenCalled();
  });
});
