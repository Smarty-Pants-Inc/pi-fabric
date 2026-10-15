import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { hostLeasePath, readHostLeases, removeHostLease, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";

// smarty-dev#7978: readHostLeases lists host-leases/ again only when the directory's stamp moved. Every
// listed file is still stat'ed, so an in-place rewrite is seen; a change within the directory's timestamp
// tick is never hidden behind an unchanged stamp.
describe.skipIf(process.platform === "win32")("host lease directory listing cache", () => {
  const roots: string[] = [];
  afterEach(() => {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const lease = (id: string, updatedAt: number): FabricHostLease => ({
    id, rootId: id, identityId: id, updatedAt, expiresAt: updatedAt + 60_000,
  });
  const setup = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-lease-listing-"));
    roots.push(root);
    for (let index = 0; index < 3; index += 1) writeHostLease(root, lease(`host:${index}`, 1_000));
    const dir = path.join(root, "host-leases");
    // Age the directory past its timestamp tick, as any directory last changed a heartbeat ago.
    const aged = (): void => { const past = new Date(Date.now() - 10_000); fs.utimesSync(dir, past, past); };
    aged();
    const listings = vi.spyOn(fs, "readdirSync");
    const leaseListings = () => listings.mock.calls.filter(([at]) => String(at) === dir).length;
    return { root, dir, aged, leaseListings };
  };

  it("reuses the listing of an unchanged directory, and still sees an in-place rewrite", () => {
    const { root, leaseListings } = setup();
    expect([...readHostLeases(root).keys()].sort()).toEqual(["host:0", "host:1", "host:2"]);
    expect(leaseListings()).toBe(1);
    expect(readHostLeases(root).size).toBe(3);
    expect(readHostLeases(root).size).toBe(3);
    expect(leaseListings()).toBe(1);                              // unchanged stamp: no readdir
    // A rewrite in place moves no directory entry, but its own stat still re-reads it.
    fs.writeFileSync(hostLeasePath(root, "host:1"), JSON.stringify({ format: 1, ...lease("host:1", 5_000) }));
    expect(readHostLeases(root).get("host:1")?.updatedAt).toBe(5_000);
    expect(leaseListings()).toBe(1);
  });

  it("lists again once a lease is added, replaced or removed", () => {
    const { root, aged, leaseListings } = setup();
    readHostLeases(root);
    writeHostLease(root, lease("host:new", 2_000));                // an atomic add moves the stamp
    expect(readHostLeases(root).get("host:new")?.updatedAt).toBe(2_000);
    aged();
    writeHostLease(root, lease("host:0", 3_000));                  // an atomic replacement
    expect(readHostLeases(root).get("host:0")?.updatedAt).toBe(3_000);
    aged();
    removeHostLease(root, "host:2");                               // a removal
    expect([...readHostLeases(root).keys()].sort()).toEqual(["host:0", "host:1", "host:new"]);
    expect(leaseListings()).toBe(4);
  });

  it("never trusts a listing taken within the directory's timestamp tick", () => {
    const { root, dir, leaseListings } = setup();
    const now = new Date();
    fs.utimesSync(dir, now, now);                                   // changed just now
    readHostLeases(root);
    readHostLeases(root);
    expect(leaseListings()).toBe(2);
  });

  it("always lists for a caller that collects problems (the writer census)", () => {
    const { root, leaseListings } = setup();
    readHostLeases(root);
    const problems: string[] = [];
    readHostLeases(root, problems);
    readHostLeases(root, problems);
    expect(leaseListings()).toBe(3);
    expect(problems).toEqual([]);
  });
});
