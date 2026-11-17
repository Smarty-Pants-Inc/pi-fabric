import fs from "node:fs";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { hostLeasePath, readHostLeases, removeHostLease, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

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

  it("does not reuse old names across synchronous directory replacement", () => {
    const { root, dir } = setup();
    expect(readHostLeases(root).size).toBe(3);
    fs.renameSync(dir, `${dir}.previous`); fs.mkdirSync(dir);
    // No local writer hint, watcher delivery, or event-loop tick.
    fs.writeFileSync(hostLeasePath(root, "replacement"), JSON.stringify({ format: 1, ...lease("replacement", 9_000) }));
    expect([...readHostLeases(root).keys()]).toEqual(["replacement"]);
  });

  it("reports new invalid files and current unreadability even after a successful cached census", () => {
    const { root, dir } = setup();
    expect(readHostLeases(root, []).size).toBe(3);
    const file = hostLeasePath(root, "host:0"), invalid = path.join(dir, "unknown.json");
    fs.writeFileSync(invalid, "not a lease");
    const read = fs.readFileSync;
    const spy = vi.spyOn(fs, "readFileSync").mockImplementation((at, options) => {
      if (String(at) === file) throw Object.assign(new Error("lease unavailable"), { code: "EACCES" });
      return read(at, options as never);
    });
    const problems: string[] = [];
    readHostLeases(root, problems);
    expect(problems.sort()).toEqual([`${file}: EACCES`, `${invalid}: invalid`].sort());
    spy.mockRestore(); fs.rmSync(invalid);
    const recovered: string[] = [];
    expect(readHostLeases(root, recovered).size).toBe(3);
    expect(recovered).toEqual([]);
  });
});

// Incident #18579: the consumer already knows its host IDs from state. Neither a cold
// directory list nor independently renewed files should enumerate/stat retained orphan leases.
// Keep reason: exact synchronous freshness and per-file I/O are not visible in the UI flow.
describe("state-derived host lease reads", () => {
  it("point-reads live state owners across independent atomic renewals and directory replacement", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-point-read-"));
    const mesh = new MeshStore(root, 256 * 1024, 1000);
    const self = { id: "session:observer", name: "observer", kind: "main" as const };
    const peer = { id: "session:peer", name: "peer", kind: "main" as const };
    const directory = new ParticipantDirectory(mesh, { enabled: true, identity: self,
      hostId: self.id, rootId: self.id, reapDeadHosts: false });
    const now = Date.now(), key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
    const lease: FabricHostLease = { id: peer.id, rootId: peer.id, identityId: peer.id,
      startedAt: 1, incarnationToken: "peer-token", updatedAt: now - 1000, expiresAt: now - 1 };
    try {
      const irrelevant = new Set<string>();
      for (let n = 0; n < 48; n++) {
        const id = `orphan:${n}`; irrelevant.add(hostLeasePath(root, id));
        writeHostLease(root, { ...lease, id, rootId: id, identityId: id, expiresAt: 1 });
      }
      writeHostLease(root, lease);
      await mesh.put({ key: key("topology/hosts/", peer.id), identity: peer,
        value: { format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: 1,
          incarnationToken: lease.incarnationToken, updatedAt: now - 1000, expiresAt: now - 1 } });
      await mesh.put({ key: key("topology/participants/", peer.id), identity: peer,
        value: { format: 1, id: peer.id, kind: "root", rootId: peer.id, ownerHostId: peer.id, ownerIdentityId: peer.id,
          name: peer.name, status: "idle", runner: "pi", transport: "host", controlProtocol: "v1",
          capabilities: ["steer"], startedAt: 1, updatedAt: now } });
      const lists = vi.spyOn(fs, "readdirSync"), stats = vi.spyOn(fs, "statSync"), reads = vi.spyOn(fs, "readFileSync");
      const listed = () => directory.list({ scope: "project", includeStale: true, fresh: true }, now).find(p => p.id === peer.id);
      expect(listed()?.stale).toBe(true);
      for (const expiresAt of [now + 15_000, now - 1, now + 30_000]) {
        // A different process cannot update this module's cache. Fresh reads must use current
        // metadata/bytes, not a same-process invalidation or an asynchronous watch hint.
        execFileSync(process.execPath, ["--input-type=module", "-e",
          'import fs from "node:fs"; const [file, body] = process.argv.slice(1); fs.writeFileSync(file + ".writer.tmp", body); fs.renameSync(file + ".writer.tmp", file);',
          hostLeasePath(root, peer.id), JSON.stringify({ format: 1, ...lease, expiresAt })]);
        expect(listed()?.stale).toBe(expiresAt < now);
      }
      const dir = path.join(root, "host-leases"); fs.renameSync(dir, `${dir}.previous`); fs.mkdirSync(dir);
      fs.writeFileSync(hostLeasePath(root, peer.id), JSON.stringify({ format: 1, ...lease, expiresAt: now - 1 }));
      expect(listed()?.stale).toBe(true);
      expect(lists.mock.calls.filter(([file]) => String(file) === dir)).toEqual([]);
      expect([...stats.mock.calls, ...reads.mock.calls].filter(([file]) => irrelevant.has(String(file)))).toEqual([]);
      expect(stats.mock.calls.filter(([file]) => String(file) === hostLeasePath(root, peer.id)).length).toBeGreaterThan(0);
    } finally {
      vi.restoreAllMocks(); await directory.close(); mesh.closeState(); fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
