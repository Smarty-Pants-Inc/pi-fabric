import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { RootRegistrationGuard, type RootRegistrationIdentity, type RootRegistrationOwner } from "../src/topology/root-registration.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const identity = (sessionId: string, name?: string): RootRegistrationIdentity => ({
  sessionId, rootId: `session:${sessionId}`, fabricSessionId: sessionId, ...(name === undefined ? {} : { name }),
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-private-root-guard-"));
  roots.push(root);
  const live = new Set(["owner-a", "owner-b", "owner-c"]);
  const guard = (id: string) => new RootRegistrationGuard(new MeshStore(root, 64 * 1024, 100), {
    owner: { id, pid: 123, host: "synthetic-host", startTime: "1" },
    ownerAlive: (owner: RootRegistrationOwner) => live.has(owner.id),
  });
  return { root, live, guard };
};

describe("duplicate live root registration", () => {
  it("refuses the same explicit name with different native session IDs before publication", async () => {
    const { root, guard } = setup();
    await guard("owner-a").claim(identity("uuid-a", "project-agent"));
    await expect(guard("owner-b").claim(identity("uuid-b", "project-agent"))).rejects.toMatchObject({ code: "FABRIC_DUPLICATE_LIVE_ROOT" });
    expect(fs.readdirSync(path.join(root, "root-registrations"))).toHaveLength(1);
    expect(fs.existsSync(path.join(root, "state.json"))).toBe(false);
  });

  it.each(["sessionId", "rootId", "fabricSessionId"] as const)("refuses the same live %s even under different names", async (field) => {
    const { guard } = setup();
    const first = identity("uuid-a", "one");
    await guard("owner-a").claim(first);
    await expect(guard("owner-b").claim({ ...identity("uuid-b", "two"), [field]: first[field] })).rejects.toThrow(/Duplicate live Fabric root/);
  });

  // S1: a mailbox recipient is an ID or a name; the reserved sets must be disjoint across roots.
  it.each([
    ["new name equals live root ID", identity("uuid-a", "one"), identity("uuid-b", "session:uuid-a")],
    ["new name equals live native session ID", identity("uuid-a", "one"), identity("uuid-b", "uuid-a")],
    ["live name equals new root ID", identity("uuid-a", "session:uuid-b"), identity("uuid-b", "two")],
    ["live name equals new actor persistence ID", identity("uuid-a", "uuid-b"), identity("uuid-b", "two")],
  ])("refuses name-to-ID recipient overlap: %s", async (_label, first, second) => {
    const { root, guard } = setup();
    await guard("owner-a").claim(first);
    await expect(guard("owner-b").claim(second)).rejects.toMatchObject({ code: "FABRIC_DUPLICATE_LIVE_ROOT" });
    expect(fs.readdirSync(path.join(root, "root-registrations"))).toHaveLength(1);
  });

  it.each([
    ["candidate name equals a published root ID", { rootId: "session:uuid-a", sessionId: "uuid-a", name: "one" }, identity("uuid-b", "session:uuid-a")],
    ["published name equals the candidate root ID", { rootId: "session:uuid-a", sessionId: "uuid-a", name: "session:uuid-b" }, identity("uuid-b", "two")],
  ])("refuses name-to-ID overlap with a live participant lease: %s", async (_label, published, candidate) => {
    const { root } = setup();
    const guard = new RootRegistrationGuard(new MeshStore(root, 64 * 1024, 100), {
      owner: { id: "owner-b", pid: 123, host: "synthetic-host", startTime: "1" },
      ownerAlive: () => true,
      publishedRoots: () => [published],
    });
    await expect(guard.claim(candidate)).rejects.toMatchObject({ code: "FABRIC_DUPLICATE_LIVE_ROOT" });
  });

  it("allows differently named, independent roots and unnamed roots", async () => {
    const { guard } = setup();
    await guard("owner-a").claim(identity("uuid-a", "one"));
    await guard("owner-b").claim(identity("uuid-b", "two"));
    await guard("owner-c").claim(identity("uuid-c"));
  });

  it("does not reserve the generic Main display name for unnamed sessions", async () => {
    const { guard } = setup();
    await guard("owner-a").claim(identity("uuid-a"));
    await guard("owner-b").claim(identity("uuid-b", "  "));
  });

  it("allows same owner's refresh and a new guard instance for reload", async () => {
    const { guard } = setup();
    const root = guard("owner-a");
    await root.claim(identity("uuid-a", "one"));
    await root.claim(identity("uuid-a", "one"));
    await guard("owner-a").claim(identity("uuid-a", "renamed"));
    await guard("owner-b").claim(identity("uuid-b", "one"));
  });

  it("allows dead takeover without letting the predecessor's close erase its successor", async () => {
    const { live, guard } = setup();
    const predecessor = guard("owner-a");
    await predecessor.claim(identity("uuid-a", "one"));
    live.delete("owner-a");
    await guard("owner-b").claim(identity("uuid-a", "one"));
    await predecessor.close();
    await expect(guard("owner-c").claim(identity("uuid-a", "one"))).rejects.toThrow(/Duplicate live Fabric root/);
  });

  it("a dead claim does not hide a newer legacy owner's live participant lease", async () => {
    const { root, live, guard } = setup();
    const first = guard("owner-a");
    await first.claim(identity("uuid-a", "one"));
    live.delete("owner-a");
    const candidate = new RootRegistrationGuard(new MeshStore(root, 64 * 1024, 100), {
      owner: { id: "owner-b", pid: 123, host: "synthetic-host", startTime: "1" },
      ownerAlive: owner => live.has(owner.id),
      publishedRoots: () => [{ rootId: "session:uuid-a", sessionId: "uuid-a", name: "one" }],
    });
    await expect(candidate.claim(identity("uuid-a", "one"))).rejects.toThrow(/live participant lease/);
  });

  it("a dead predecessor's matching owner token permits takeover despite its leftover lease", async () => {
    const { root, live, guard } = setup();
    const first = guard("owner-a");
    await first.claim(identity("uuid-a", "one"));
    live.delete("owner-a");
    const candidate = new RootRegistrationGuard(new MeshStore(root, 64 * 1024, 100), {
      owner: { id: "owner-b", pid: 123, host: "synthetic-host", startTime: "1" },
      ownerAlive: owner => live.has(owner.id),
      publishedRoots: () => [{ rootId: "session:uuid-a", sessionId: "uuid-a", name: "one", rootRegistrationOwnerId: first.ownerId! }],
    });
    await candidate.claim(identity("uuid-a", "one"));
    expect(candidate.ownerId).toBe("owner-b");
  });

  it("same-owner refresh after name takeover is not blocked by the dead predecessor's leftover lease", async () => {
    const { root, live, guard } = setup();
    const first = guard("owner-a");
    await first.claim(identity("uuid-a", "one"));
    live.delete("owner-a");
    const candidate = new RootRegistrationGuard(new MeshStore(root, 64 * 1024, 100), {
      owner: { id: "owner-b", pid: 123, host: "synthetic-host", startTime: "1" },
      ownerAlive: owner => live.has(owner.id),
      publishedRoots: () => [{ rootId: "session:uuid-a", sessionId: "uuid-a", name: "one", rootRegistrationOwnerId: first.ownerId! }],
    });
    await candidate.claim(identity("uuid-b", "one"));
    await candidate.claim(identity("uuid-b", "one"));
  });

  it("a dead predecessor's root ID does not suppress unknown legacy alias warning", async () => {
    const { root, live, guard } = setup();
    await guard("owner-a").claim(identity("uuid-a", "old-name"));
    live.delete("owner-a");
    const warnings: string[] = [];
    const candidate = new RootRegistrationGuard(new MeshStore(root, 64 * 1024, 100), {
      owner: { id: "owner-b", pid: 123, host: "synthetic-host", startTime: "1" },
      ownerAlive: owner => live.has(owner.id),
      publishedRoots: () => [{ rootId: "session:uuid-a", sessionId: "uuid-a", name: "main" }],
      onUnknownNameOwnership: warning => warnings.push(warning),
    });
    await candidate.claim(identity("uuid-b", "independent-name"));
    expect(candidate.ownerId).toBe("owner-b");
    expect(warnings).toEqual([expect.stringMatching(/Cannot verify persisted name ownership/)]);
  });

  it("releases an orderly closed root for a new owner", async () => {
    const { guard } = setup();
    const first = guard("owner-a");
    await first.claim(identity("uuid-a", "one"));
    await first.close();
    await first.close();
    await guard("owner-b").claim(identity("uuid-a", "one"));
  });

  it("serializes simultaneous claimants so only one becomes the owner", async () => {
    const { guard } = setup();
    const result = await Promise.allSettled([
      guard("owner-a").claim(identity("uuid-a", "one")),
      guard("owner-b").claim(identity("uuid-b", "one")),
    ]);
    expect(result.map(item => item.status).sort()).toEqual(["fulfilled", "rejected"]);
  });

  it("refuses a conflicting rename without dropping the previous reservation", async () => {
    const { guard } = setup();
    const first = guard("owner-a");
    await first.claim(identity("uuid-a", "one"));
    await guard("owner-b").claim(identity("uuid-b", "two"));
    await expect(first.claim(identity("uuid-a", "two"))).rejects.toThrow(/Duplicate live Fabric root/);
    await expect(guard("owner-c").claim(identity("uuid-c", "one"))).rejects.toThrow(/Duplicate live Fabric root/);
  });

  it("fails closed on malformed ownership data", async () => {
    const { root, guard } = setup();
    fs.mkdirSync(path.join(root, "root-registrations"));
    fs.writeFileSync(path.join(root, "root-registrations", "broken.json"), "not json");
    await expect(guard("owner-a").claim(identity("uuid-a", "one"))).rejects.toThrow(/root registration/);
  });
});
