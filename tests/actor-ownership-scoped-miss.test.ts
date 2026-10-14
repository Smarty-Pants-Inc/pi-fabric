import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// fv2-11107 F1: an actor restored while a poll's scoped ownership map is held must be decided by a fresh
// per-id directory read, never by the same-root/residency fallback, and its dead letters and queue must
// not be rewritten before ownership is established. Real ActorManager, registry and ParticipantDirectory.
const roots: string[] = [];
const closers: Array<() => Promise<unknown>> = [];
beforeEach(() => { vi.spyOn(console, "warn").mockImplementation(() => {}); });
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const digest = (id: string): string => createHash("sha256").update(id).digest("hex");
const SELF: MeshIdentity = { id: "session:scope", name: "main", kind: "main" };
const OTHER: MeshIdentity = { id: "session:other", name: "main", kind: "main" };
const HOST = `host:${SELF.id}`;

const setup = async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "actor-scoped-miss-")); roots.push(dir);
  const actorRoot = path.join(dir, "actors");
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const writer = new MeshStore(mesh.root, 64 * 1024, 100);
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: "host:observer", rootId: "session:observer",
    identity: { id: "session:observer", name: "main", kind: "main" }, heartbeatMs: 60_000, leaseMs: 120_000,
  });
  closers.push(() => directory.close());
  const putHost = (identity: MeshIdentity) => writer.put({
    key: "topology/hosts/" + digest(`host:${identity.id}`), identity,
    value: { format: 1, id: `host:${identity.id}`, rootId: identity.id, identity, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000 },
  });
  const putActor = (id: string, owner: MeshIdentity) => writer.put({
    key: "topology/participants/" + digest(id), identity: owner,
    value: { format: 1, id, kind: "actor", rootId: owner.id, ownerHostId: `host:${owner.id}`, ownerIdentityId: owner.id,
      name: id, status: "idle", runner: "pi", transport: "host", capabilities: [], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" },
  });
  await putHost(SELF); await putHost(OTHER);
  // Test-owned producer instrumentation (the hooks are a dependency the host supplies, not a manager seam).
  const override: { snapshot?: (fresh?: boolean, actorIds?: readonly string[]) => ReadonlyMap<string, boolean>;
    canManage?: (id: string, fresh?: boolean) => boolean | undefined } = {};
  const snapshot = vi.fn((fresh = true, actorIds?: readonly string[]) => override.snapshot ? override.snapshot(fresh, actorIds) :
    directory.actorOwnership(HOST, { fresh, ...(fresh ? {} : { actorIds }) }));
  const canManageActor = vi.fn((id: string, fresh = true) => {
    if (override.canManage) return override.canManage(id, fresh);
    const participant = directory.get(id, undefined, { fresh });
    return participant ? participant.ownerHostId === HOST : undefined;
  });
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 };
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
  const actors = new ActorManager("scope", SELF, mesh, meshConfig, agents, () => {},
    { actorRoot, persistent: true, claimResidency: "session", canManageActor, snapshotActorOwnership: snapshot });
  closers.push(() => agents.close(), () => actors.close());
  // A second same-root/same-residency writer on the shared registry restores Z (another process in production).
  const peerAgents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "peer-runs") });
  const peer = new ActorManager("scope", SELF, mesh, meshConfig, peerAgents, () => {},
    { actorRoot, persistent: true, claimResidency: "session", canManageActor: () => undefined });
  closers.push(() => peerAgents.close(), () => peer.close());
  return { actors, peer, snapshot, canManageActor, putActor, actorRoot, directory, writer, mesh, override };
};

/** Records (and refuses) every mutating fs call under one actor directory: no truncate/delete executes. */
const recordWrites = (actorDir: string): string[] => {
  const calls: string[] = [];
  const guard = <K extends "writeFileSync" | "appendFileSync" | "renameSync" | "rmSync" | "unlinkSync" | "openSync" | "truncateSync">(name: K) => {
    const original = (fs[name] as (...args: unknown[]) => unknown).bind(fs);
    (vi.spyOn as unknown as (target: object, method: string) => { mockImplementation(fn: unknown): unknown })(fs, name)
      .mockImplementation(((...args: unknown[]) => {
      const target = typeof args[0] === "string" ? path.resolve(args[0]) : undefined;
      const writing = name !== "openSync" || (typeof args[1] === "string" && /[wa+]/.test(args[1]));
      if (target?.startsWith(actorDir + path.sep) && writing) {
        const stack = new Error().stack ?? "";
        calls.push(`${name}:${path.relative(actorDir, target)}${stack.includes("#replayDeadLetters") ? "|replay" : ""}`);
        if (process.env.F1_STACK) process.stderr.write(`F1STACK ${stack}\n`);
        throw Object.assign(new Error(`recorded ${name}`), { code: "EACCES" });
      }
      return original(...args);
    }) as never);
  };
  for (const name of ["writeFileSync", "appendFileSync", "renameSync", "rmSync", "unlinkSync", "openSync", "truncateSync"] as const) guard(name);
  return calls;
};

const files = (dir: string) => new Map(fs.readdirSync(dir, { withFileTypes: true }).filter((entry) => entry.isFile())
  .map((entry) => [entry.name, fs.readFileSync(path.join(dir, entry.name))] as const));
const settle = async () => { for (let index = 0; index < 5; index++) await new Promise((resolve) => setImmediate(resolve)); };

// Actual F1 path: a poll installs the scoped map (snapshot hook answering only the requested ids, as the
// directory's reuse does), #refreshOwnershipNow takes its acquired branch, the nested #loadActors restores
// Z (committed by another process between the poll's registry sync and its scoped refresh), Z is not in
// the scoped map, and on the old head the same-root/residency fallback claimed it and queued its dead
// letters for replay. Every custody mutation under Z's directory is recorded and refused.
describe("fv2-11107 F1 actual scoped reload repro", () => {
  const verdicts: Array<[string, () => boolean | undefined, boolean]> = [
    ["foreign owner false is refused", () => false, false],
    ["unknown (undefined) is refused, not the residency fallback", () => undefined, false],
    ["a throwing fresh read is refused", () => { throw new Error("directory read failed"); }, false],
    ["rightful owner true is permitted", () => true, true],
  ];
  for (const [label, zVerdict, expected] of verdicts) {
    it(label, async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "actor-scoped-reload-")); roots.push(dir);
      const actorRoot = path.join(dir, "actors");
      const registryFile = path.join(actorRoot, "actors.json");
      const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
      const owners = new Map<string, boolean>();
      let zId = "";
      let gapCommit: Buffer | undefined;
      const scopedCalls: string[][] = [];
      const snapshot = vi.fn((fresh = true, actorIds?: readonly string[]) => {
        if (fresh) {
          // A full directory snapshot names every live participant, Z included once it has a record.
          const full = new Map(owners);
          if (zId) { try { const v = zVerdict(); if (v !== undefined) full.set(zId, v); } catch { /* no entry */ } }
          return full;
        }
        scopedCalls.push([...(actorIds ?? [])]);
        // Another process commits Z after this poll's registry sync, before its scoped refresh read.
        if (gapCommit) { fs.writeFileSync(registryFile, gapCommit); gapCommit = undefined; }
        return new Map((actorIds ?? []).filter((id) => owners.has(id)).map((id) => [id, owners.get(id)!] as const));
      });
      const zReads: Array<boolean | undefined> = [];
      const canManageActor = vi.fn((id: string) => {
        if (id !== zId) return owners.get(id);
        zReads.push(true);
        return zVerdict();
      });
      const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 };
      const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "runs") });
      const actors = new ActorManager("scope", SELF, mesh, meshConfig, agents, () => {},
        { actorRoot, persistent: true, claimResidency: "session", canManageActor, snapshotActorOwnership: snapshot });
      closers.push(() => agents.close(), () => actors.close());
      const peerAgents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(dir, "peer-runs") });
      const peer = new ActorManager("scope", SELF, mesh, meshConfig, peerAgents, () => {},
        { actorRoot, persistent: true, claimResidency: "session", canManageActor: () => undefined });
      closers.push(() => peerAgents.close(), () => peer.close());

      const owned = await actors.create({ name: "owned", instructions: "Reply" });
      // The directory first names another owner, so a later poll acquires it (false -> true).
      owners.set(owned.id, false);
      expect(actors.listOwned().map((info) => info.id)).toEqual([]);
      const beforeZ = fs.readFileSync(registryFile);
      const z = await peer.create({ name: "restored", instructions: "Reply" });
      zId = z.id;
      expect(z.residency ?? "session").toBe("session");
      const withZ = fs.readFileSync(registryFile);
      fs.writeFileSync(registryFile, beforeZ); // Z's commit lands only in the sync->refresh gap below
      expect(actors.listOwned().map((info) => info.id)).toEqual([]);
      expect(actors.list().map((info) => info.id)).not.toContain(z.id);
      const zDir = path.join(actorRoot, z.id);
      fs.mkdirSync(zDir, { recursive: true });
      fs.writeFileSync(path.join(zDir, "dead-letter.jsonl"), `${JSON.stringify({ at: 1, source: "mesh:topic",
        event: { id: "dead-1", topic: "t", data: {}, from: OTHER, createdAt: 1 } })}\n`);
      const before = files(zDir);
      owners.set(owned.id, true);
      gapCommit = withZ;
      scopedCalls.length = 0;
      const writes = recordWrites(zDir);
      const ids = actors.listOwned().map((info) => info.id);
      await settle();
      // The scoped map was installed for the owned actor only, and Z was restored by the nested reload.
      expect(scopedCalls[0]).toEqual([owned.id]);
      expect(gapCommit).toBeUndefined();
      expect(actors.list().map((info) => info.id)).toContain(z.id);
      expect(ids).toContain(owned.id);
      expect(ids.includes(z.id)).toBe(expected);
      if (!expected) {
        // The queued replay refuses before any custody write: no replay attempt, no dead-letter write, bytes
        // unchanged. The only other attempt allowed is the later full-snapshot sync parking the (correctly)
        // unowned Z, which removes its never-written empty queue file (a no-op rm, recorded and refused here).
        expect(writes.filter((call) => call.includes("|replay") || call.includes("dead-letter"))).toEqual([]);
        for (const call of writes) expect(call).toMatch(/^rmSync:queue-[0-9a-f]+\.json$/);
        for (const call of writes) expect(before.has(call.slice("rmSync:".length))).toBe(false);
        const after = files(zDir);
        expect([...after.keys()].sort()).toEqual([...before.keys()].sort());
        for (const [name, bytes] of before) expect(after.get(name)?.equals(bytes)).toBe(true);
      }
      expect(zReads.length).toBeGreaterThan(0); // Z was decided by an exact fresh read
    });
  }
});

// Producer gap with the REAL directory: the snapshot hook takes the actual ParticipantDirectory snapshot (X just
// moved false -> true in the directory, so the poll's scoped request is not reused and the map is a real read),
// then, before returning that already-read map, a real second OS process commits Z's registry row and Z's
// participant record. The manager then takes its production path: #refreshOwnershipNow acquired -> nested
// #loadActors restores Z, which the map cannot name -> decision -> queued #replayDeadLetters.
describe("fv2-11107 F1 producer-gap repro (real directory, second-process commit)", () => {
  const cases: Array<[string, MeshIdentity | undefined, boolean, boolean]> = [
    // [label, Z participant owner committed in the gap, fresh read throws, expected owned]
    ["foreign owner committed in the gap is refused", OTHER, false, false],
    ["no participant record (unknown) is refused", undefined, false, false],
    ["a failing fresh read is refused", OTHER, true, false],
    ["rightful owner committed in the gap is permitted", SELF, false, true],
  ];
  for (const [label, zOwner, zThrows, expected] of cases) {
    it(label, async () => {
      const { actors, peer, putActor, actorRoot, directory, writer, mesh, override } = await setup();
      const registryFile = path.join(actorRoot, "actors.json");
      const x = await actors.create({ name: "x", instructions: "Reply" });
      await putActor(x.id, OTHER);
      expect(actors.listOwned().map((info) => info.id)).toEqual([]); // X false: another live owner
      const beforeZ = fs.readFileSync(registryFile);
      const z = await peer.create({ name: "restored", instructions: "Reply" });
      expect(z.residency ?? "session").toBe("session");
      const withZ = fs.readFileSync(registryFile);
      const zKey = "topology/participants/" + digest(z.id);
      // Z exists for the other process only: neither its registry row nor its participant record is visible yet.
      await writer.delete({ key: zKey }); // test-tmp mesh setup only, if the peer published one
      fs.writeFileSync(registryFile, beforeZ);
      expect(actors.listOwned().map((info) => info.id)).toEqual([]);
      expect(actors.list().map((info) => info.id)).not.toContain(z.id);
      const zDir = path.join(actorRoot, z.id);
      fs.mkdirSync(zDir, { recursive: true });
      fs.writeFileSync(path.join(zDir, "dead-letter.jsonl"), `${JSON.stringify({ at: 1, source: "mesh:topic",
        event: { id: "dead-1", topic: "t", data: {}, from: OTHER, createdAt: 1 } })}\n`);
      const before = files(zDir);
      await putActor(x.id, SELF); // the directory change that makes this poll acquire X
      // The second process: commits Z's registry row and (optionally) its participant record.
      const stash = path.join(path.dirname(actorRoot), "z-registry.json");
      fs.writeFileSync(stash, withZ);
      const script = path.join(path.dirname(actorRoot), "commit-z.ts");
      fs.writeFileSync(script, [
        `import fs from "node:fs";`,
        `import { MeshStore } from ${JSON.stringify(path.resolve("src/mesh/store.ts"))};`,
        `const [registry, stash, root, put] = JSON.parse(process.argv[2]!);`,
        `fs.writeFileSync(registry, fs.readFileSync(stash));`,
        `if (put) { const store = new MeshStore(root, 64 * 1024, 100); await store.put(put); store.closeState(); }`,
        `process.exit(0);`,
      ].join("\n"));
      const put = zOwner ? { key: zKey, identity: zOwner, value: { format: 1, id: z.id, kind: "actor", rootId: zOwner.id,
        ownerHostId: `host:${zOwner.id}`, ownerIdentityId: zOwner.id, name: z.id, status: "idle", runner: "pi",
        transport: "host", capabilities: [], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" } } : undefined;
      const scoped: Array<{ ids: string[]; hasZ: boolean; childExit: number | null }> = [];
      let gapArmed = true;
      const snapshotHook = (fresh = true, actorIds?: readonly string[]) => {
        const map = directory.actorOwnership(HOST, { fresh, ...(fresh ? {} : { actorIds }) });
        if (!fresh && gapArmed) {
          gapArmed = false;
          const child = spawnSync("bun", [script, JSON.stringify([registryFile, stash, mesh.root, put ?? null])],
            { encoding: "utf8", timeout: 30_000 });
          scoped.push({ ids: [...(actorIds ?? [])], hasZ: map.has(z.id), childExit: child.status });
          if (child.status !== 0) throw new Error(`commit child failed: ${child.stderr}`);
        }
        return map;
      };
      const zReads: Array<boolean | undefined | "threw"> = [];
      const canManage = (id: string, fresh = true) => {
        if (id === z.id && zThrows) { zReads.push("threw"); throw new Error("directory read failed"); }
        const participant = directory.get(id, undefined, { fresh });
        const verdict = participant ? participant.ownerHostId === HOST : undefined;
        if (id === z.id) zReads.push(verdict);
        return verdict;
      };
      override.snapshot = snapshotHook; override.canManage = canManage;
      const writes = recordWrites(zDir);
      const ids = actors.listOwned().map((info) => info.id);
      await settle();
      delete override.snapshot; delete override.canManage;
      // The poll's scoped map was a real non-reused read naming X, taken before Z existed; the second
      // process committed Z before the map was returned; the nested reload restored Z.
      expect(scoped).toEqual([{ ids: [x.id], hasZ: false, childExit: 0 }]);
      expect(actors.list().map((info) => info.id)).toContain(z.id);
      expect(ids).toContain(x.id);
      expect(ids.includes(z.id)).toBe(expected);
      if (!expected) {
        expect(writes.filter((call) => call.includes("|replay") || call.includes("dead-letter"))).toEqual([]);
        for (const call of writes) expect(call).toMatch(/^rmSync:queue-[0-9a-f]+\.json$/);
        for (const call of writes) expect(before.has(call.slice("rmSync:".length))).toBe(false);
        const after = files(zDir);
        expect([...after.keys()].sort()).toEqual([...before.keys()].sort());
        for (const [name, bytes] of before) expect(after.get(name)?.equals(bytes)).toBe(true);
      }
      expect(zReads.length).toBeGreaterThan(0); // Z was decided by an exact fresh read
      if (zOwner && !zThrows) expect(zReads).toContain(zOwner === SELF);
    }, 60_000);
  }
});

describe("fv2-11107 F1 scoped poll map miss", () => {
  for (const [label, owner, expected] of [
    ["live foreign owner is vetoed", OTHER, false],
    ["rightful owner is permitted", SELF, true],
  ] as const) {
    it(`a newly restored same-root/same-residency actor while the poll map is held: ${label}`, async () => {
      const { actors, peer, snapshot, canManageActor, putActor, actorRoot } = await setup();
      const owned = await actors.create({ name: "owned", instructions: "Reply" });
      await putActor(owned.id, SELF);
      expect(actors.listOwned().map((info) => info.id)).toEqual([owned.id]);
      expect(actors.listOwned().map((info) => info.id)).toEqual([owned.id]); // armed + reused scoped map
      const z = await peer.create({ name: "restored", instructions: "Reply" });
      expect(z.residency ?? "session").toBe("session");
      await putActor(z.id, owner);
      const zDir = path.join(actorRoot, z.id);
      fs.mkdirSync(zDir, { recursive: true });
      const deadLetters = path.join(zDir, "dead-letter.jsonl");
      fs.writeFileSync(deadLetters, `${JSON.stringify({ at: 1, source: "mesh:topic", event: { id: "dead-1", topic: "t", data: {}, from: OTHER, createdAt: 1 } })}\n`);
      const before = files(zDir);
      const writes = recordWrites(zDir);
      snapshot.mockClear(); canManageActor.mockClear();
      const ids = actors.listOwned().map((info) => info.id);
      await settle();
      expect(ids.includes(z.id)).toBe(expected);
      expect(actors.listOwned().map((info) => info.id).includes(z.id)).toBe(expected);
      // Existing map answers stay authoritative: the owned actor remains owned.
      expect(ids).toContain(owned.id);
      if (!expected) {
        // Ownership is refused before any custody mutation: bytes and directory unchanged, no write attempted.
        expect(writes).toEqual([]);
        const after = files(zDir);
        expect([...after.keys()].sort()).toEqual([...before.keys()].sort());
        for (const [name, bytes] of before) expect(after.get(name)?.equals(bytes)).toBe(true);
      }
    });
  }
});
