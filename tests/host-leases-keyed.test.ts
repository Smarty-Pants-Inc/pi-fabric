import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  hostLeaseExpiry, hostLeasePath, readHostLease, readHostLeaseSnapshot, readHostLeases,
  readHostLeasesFor, writeHostLease, type FabricHostLease,
} from "../src/topology/host-leases.js";

// The presence-sync regression was repeated O(fleet) directory enumeration on every renewal.
// Exercise real lease files and the shared parser/cache, not a mocked lease reader.
describe("keyed host lease reads", () => {
  const roots: string[] = [];
  afterEach(() => {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const setup = (): string => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-leases-keyed-"));
    roots.push(root);
    return root;
  };
  const lease = (id: string, updatedAt = 1_000): FabricHostLease => ({
    id, rootId: `root:${id}`, identityId: `identity:${id}`, updatedAt, expiresAt: updatedAt + 60_000,
    startedAt: 1, incarnationToken: `incarnation:${id}`,
  });
  const keyedReader = (root: string) => {
    const listings = vi.spyOn(fs, "readdirSync");
    return (ids: Iterable<string>): Map<string, FabricHostLease> => {
      const before = listings.mock.calls.length;
      const result = readHostLeasesFor(root, ids);
      expect(listings.mock.calls.length).toBe(before);
      return result;
    };
  };
  const expiry = (leases: ReadonlyMap<string, FabricHostLease>, record: FabricHostLease): number =>
    hostLeaseExpiry(leases, { ...record, identity: { id: record.identityId }, expiresAt: 0 });

  it("matches the full reader across 1000 leases, renewals, rewrites, expiry and recreation", () => {
    const root = setup();
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const expected = new Map<string, FabricHostLease>();
    for (let index = 0; index < 1_000; index++) {
      const record = lease(`host:${index}`, index % 2 ? now : now - 120_000);
      expected.set(record.id, record);
      writeHostLease(root, record);
    }
    const ids = [...expected.keys()];
    const read = keyedReader(root);
    const compare = (): Map<string, FabricHostLease> => {
      // Keyed goes first, so the full reader cannot hide a missed keyed invalidation.
      const selected = read(ids);
      expect(selected).toEqual(expected);
      expect(selected).toEqual(readHostLeases(root));
      const subset = ids.filter((_, index) => index % 29 === 0).reverse();
      expect(read([...subset, ...subset, "absent"])).toEqual(new Map(
        subset.filter(id => expected.has(id)).map(id => [id, expected.get(id)!]),
      ));
      return selected;
    };
    const initial = compare();
    expect([...initial.values()].filter(record => expiry(initial, record) > now)).toHaveLength(500);
    const reads = vi.spyOn(fs, "readFileSync");
    now += 60_001; // No file changed: retain all expired records; liveness belongs to the caller.
    const expired = compare();
    expect(expired.size).toBe(1_000);
    expect([...expired.values()].every(record => expiry(expired, record) <= now)).toBe(true);
    expect(reads.mock.calls.filter(([file]) => String(file).startsWith(path.join(root, "host-leases")))).toHaveLength(0);

    for (let index = 0; index < ids.length; index += 25) {
      const renewed = lease(ids[index]!, now);
      expected.set(renewed.id, renewed);
      writeHostLease(root, renewed);
      const rewritten = lease(ids[index + 1]!, now + 10_000);
      expected.set(rewritten.id, rewritten);
      fs.writeFileSync(hostLeasePath(root, rewritten.id), JSON.stringify({ format: 1, ...rewritten }));
    }
    const renewed = compare();
    expect([...renewed.values()].filter(record => expiry(renewed, record) > now)).toHaveLength(80);
    for (const id of ids.slice(0, 5)) {
      fs.unlinkSync(hostLeasePath(root, id));
      expected.delete(id);
    }
    expect(compare().size).toBe(995);
    for (const id of ids.slice(0, 5)) {
      const recreated = { ...lease(id, now + 20_000), incarnationToken: `successor:${id}` };
      writeHostLease(root, recreated);
      expected.set(id, recreated);
    }
    expect(compare().size).toBe(1_000);
  });

  it("deduplicates names and preserves unrelated cache entries when reading and renewing a subset", () => {
    const root = setup();
    const records = [lease("host:a"), lease("host:b"), lease("host:c")];
    for (const record of records) writeHostLease(root, record);
    const read = keyedReader(root);
    const reads = vi.spyOn(fs, "readFileSync");
    const stats = vi.spyOn(fs, "statSync");
    const all = readHostLeases(root);
    const readCount = (id: string): number => reads.mock.calls.filter(([file]) => file === hostLeasePath(root, id)).length;
    expect(records.map(record => readCount(record.id))).toEqual([1, 1, 1]);
    stats.mockClear();
    expect(read(["host:a", "host:a", "missing", "missing"])).toEqual(new Map([["host:a", records[0]!]]));
    expect(stats.mock.calls.map(([file]) => String(file))).toEqual([
      hostLeasePath(root, "host:a"), hostLeasePath(root, "missing"),
    ]);
    expect(read([]).size).toBe(0);
    expect(read(new Set(["host:b", "host:c"])).size).toBe(2);
    expect(records.map(record => readCount(record.id))).toEqual([1, 1, 1]);

    const renewed = lease("host:a", 2_000);
    writeHostLease(root, renewed);
    expect(read(["host:a"]).get("host:a")).toEqual(renewed);
    const after = read(records.map(record => record.id));
    expect(after.get("host:b")).toBe(all.get("host:b"));
    expect(after.get("host:c")).toBe(all.get("host:c"));
    expect(readHostLease(root, "host:a")).toBe(after.get("host:a"));
    expect(readHostLeaseSnapshot(root, "host:a")?.lease).toBe(after.get("host:a"));
    expect(readHostLeases(root)).toEqual(after);
    expect(records.map(record => readCount(record.id))).toEqual([2, 1, 1]);
  });

  it("rejects invalid/misnamed files and uses only the matching current incarnation for liveness", () => {
    const root = setup();
    const record = lease("host:a");
    const read = keyedReader(root);
    expect(read([record.id]).size).toBe(0); // No lease directory yet.
    writeHostLease(root, record);
    expect(read([record.id])).toEqual(readHostLeases(root));
    const file = hostLeasePath(root, record.id);
    for (const text of [
      "{not json", JSON.stringify({ format: 2, ...record }),
      JSON.stringify({ format: 1, ...record, id: "host:misnamed" }),
      JSON.stringify({ format: 1, ...record, incarnationToken: "" }),
      JSON.stringify({ format: 1, ...record, expiresAt: null }),
    ]) {
      fs.writeFileSync(file, text);
      expect(read([record.id, "host:misnamed"]).size).toBe(0);
      expect(readHostLeases(root).size).toBe(0);
    }
    const current = { ...record, updatedAt: 2_000, incarnationToken: "current-incarnation" };
    writeHostLease(root, current);
    const selected = read([record.id]);
    const full = readHostLeases(root);
    expect(selected).toEqual(full);
    expect(selected.get(record.id)).toEqual(current);
    expect(expiry(selected, current)).toBe(current.expiresAt);
    expect(expiry(selected, record)).toBe(0); // A previous incarnation cannot gain authority.
    expect(expiry(full, record)).toBe(0);
  });

  it.each([false, true])("shares unreadable-file fallback and retries unchanged bytes (cached=%s)", (cached) => {
    const root = setup();
    const original = lease("host:a");
    writeHostLease(root, original);
    if (cached) expect(readHostLeases(root).get(original.id)).toEqual(original);
    const renewed = lease(original.id, 2_000);
    writeHostLease(root, renewed);
    const read = keyedReader(root);
    const file = hostLeasePath(root, original.id);
    const nativeRead = fs.readFileSync.bind(fs);
    const failing = vi.spyOn(fs, "readFileSync").mockImplementation(((name: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (String(name) === file) throw Object.assign(new Error("lease temporarily unreadable"), { code: "EACCES" });
      return (nativeRead as (...args: unknown[]) => unknown)(name, ...rest);
    }) as typeof fs.readFileSync);
    const selected = read([original.id]);
    expect(selected.get(original.id)).toEqual(cached ? original : undefined);
    expect(selected).toEqual(readHostLeases(root));
    expect(failing.mock.calls.filter(([name]) => name === file)).toHaveLength(10); // Five attempts per reader.
    failing.mockRestore();
    // Failed reads must not cache the replacement's stat and freeze the fallback indefinitely.
    expect(read([original.id]).get(original.id)).toEqual(renewed);
    expect(readHostLeases(root).get(original.id)).toEqual(renewed);
  });
});
