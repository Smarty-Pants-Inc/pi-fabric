import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import {
  DEAD_ROOT_CACHE_MS, DEAD_ROOT_GRACE_MS, DeadRootCache, deadRootExempt, deadRootSkipsPath, judgeRoot,
} from "../src/actors/dead-root-filter.js";
import { DEFAULT_FABRIC_CONFIG, normalizeDeadRootFilterConfig, normalizeFabricConfig, type FabricDeadRootFilterConfig } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

const MIN = 60_000;
const ROOT = "session:dead-root";
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tmp = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-dead-root-"));
  roots.push(root);
  return root;
};

const lease = (meshRoot: string, hostId: string, expiresAt: number, rootId = hostId) =>
  writeHostLease(meshRoot, { id: hostId, rootId, identityId: hostId, updatedAt: expiresAt - 15_000, expiresAt });
const participantKey = (id: string) => `topology/participants/${createHash("sha256").update(id).digest("hex")}`;
const participant = (meshRoot: string, rootId: string, ownerHostId: string, now: number) =>
  writeParticipantFile(meshRoot, {
    key: participantKey(rootId), version: 1, updatedAt: now,
    updatedBy: { id: ownerHostId, name: "main", kind: "main" },
    value: { format: 1, id: rootId, kind: "root", rootId, ownerHostId, ownerIdentityId: ownerHostId, status: "idle" },
  });
const participantFile = (meshRoot: string, rootId: string) =>
  path.join(meshRoot, "participants", `${participantKey(rootId).slice("topology/participants/".length)}.json`);

describe("dead-root verdicts (fail-open)", () => {
  const now = Date.now();

  it("a lease expired more than 10 min ago and no participant is dead", () => {
    const mesh = tmp();
    lease(mesh, ROOT, now - 20 * MIN);
    expect(judgeRoot(mesh, ROOT, now)).toEqual({ dead: true, reason: "root lease expired 20 min ago; no live participant" });
    // A participant whose owner lease also expired is not live.
    participant(mesh, ROOT, ROOT, now - 20 * MIN);
    expect(judgeRoot(mesh, ROOT, now).dead).toBe(true);
  });

  it("a live lease or a live participant is alive", () => {
    const mesh = tmp();
    lease(mesh, ROOT, now + 10_000);
    expect(judgeRoot(mesh, ROOT, now)).toEqual({ dead: false, reason: "live lease" });
    lease(mesh, ROOT, now - 60 * MIN);
    lease(mesh, "session:other-host", now + 10_000, ROOT);
    participant(mesh, ROOT, "session:other-host", now);
    expect(judgeRoot(mesh, ROOT, now)).toEqual({ dead: false, reason: "live participant" });
    // The Main session sub-lease keeps the root alive too.
    const other = tmp();
    writeHostLease(other, { id: ROOT, rootId: ROOT, identityId: ROOT, updatedAt: now - 30 * MIN, expiresAt: now - 30 * MIN,
      session: { id: "s", startedAt: 1, updatedAt: now, expiresAt: now + 15_000 } });
    expect(judgeRoot(other, ROOT, now).dead).toBe(false);
  });

  it("a lease that expired within the 10 min grace still runs", () => {
    const mesh = tmp();
    lease(mesh, ROOT, now - DEAD_ROOT_GRACE_MS + 1_000);
    expect(judgeRoot(mesh, ROOT, now)).toEqual({ dead: false, reason: "root lease expired recently" });
  });

  it("doubt runs: missing lease and participant, unreadable lease, unreadable participant, clock", () => {
    const missing = tmp();
    expect(judgeRoot(missing, ROOT, now)).toEqual({ dead: false, reason: "no root lease" });
    // A stale participant without any lease file is still doubt: dead needs a lease present.
    participant(missing, ROOT, ROOT, now - 60 * MIN);
    expect(judgeRoot(missing, ROOT, now).dead).toBe(false);

    const garbled = tmp();
    fs.mkdirSync(path.dirname(hostLeasePath(garbled, ROOT)), { recursive: true });
    fs.writeFileSync(hostLeasePath(garbled, ROOT), "{not json");
    expect(judgeRoot(garbled, ROOT, now)).toEqual({ dead: false, reason: "root lease unreadable" });

    const badParticipant = tmp();
    lease(badParticipant, ROOT, now - 60 * MIN);
    fs.mkdirSync(path.join(badParticipant, "participants"), { recursive: true });
    fs.writeFileSync(participantFile(badParticipant, ROOT), "{not json");
    expect(judgeRoot(badParticipant, ROOT, now)).toEqual({ dead: false, reason: "participant record unreadable" });

    const future = tmp();
    writeHostLease(future, { id: ROOT, rootId: ROOT, identityId: ROOT, updatedAt: now + 60 * MIN, expiresAt: now - 60 * MIN });
    expect(judgeRoot(future, ROOT, now)).toEqual({ dead: false, reason: "root lease is from the future" });
    expect(judgeRoot(future, ROOT, Number.NaN).dead).toBe(false);
    expect(judgeRoot(future, "", now).dead).toBe(false);
  });

  it("the cache never reuses a dead verdict; it keeps an alive verdict for 60 s", () => {
    const mesh = tmp();
    let clock = now;
    const cache = new DeadRootCache(mesh, () => clock);
    lease(mesh, ROOT, now - 20 * MIN);
    expect(cache.judge(ROOT).dead).toBe(true);
    expect(cache.judge(ROOT).dead).toBe(true);                      // re-read, still dead
    // A live participant on another host appears; the root lease file is unchanged.
    lease(mesh, "session:other-host", now + 10 * MIN, ROOT);
    participant(mesh, ROOT, "session:other-host", now);
    expect(cache.judge(ROOT)).toEqual({ dead: false, reason: "live participant" });   // same instant: re-read

    // An alive verdict is reused within 60 s (fail-open), then re-read.
    const alive = tmp();
    const third = new DeadRootCache(alive, () => clock);
    lease(alive, ROOT, now + 15_000);
    expect(third.judge(ROOT)).toEqual({ dead: false, reason: "live lease" });
    lease(alive, ROOT, now - 20 * MIN);
    clock = now + DEAD_ROOT_CACHE_MS - 1;
    expect(third.judge(ROOT).dead).toBe(false);                     // within the 60 s bound: cached alive
    clock = now + DEAD_ROOT_CACHE_MS;
    expect(third.judge(ROOT).dead).toBe(true);                      // re-read at 60 s

    const renewed = tmp();
    const second = new DeadRootCache(renewed, () => now);
    lease(renewed, ROOT, now - 20 * MIN);
    expect(second.judge(ROOT).dead).toBe(true);
    lease(renewed, ROOT, now + 15_000);                              // the root came back
    expect(second.judge(ROOT)).toEqual({ dead: false, reason: "live lease" });
  });

  it("exemptions match an id, an id prefix or an exact name; nothing is implicit", () => {
    const config = { exempt: ["0536f1ea", "named-reviewer"] };
    expect(deadRootExempt(config, { id: "0536f1ea-1111-2222", name: "x" })).toBe(true);
    expect(deadRootExempt(config, { id: "ffff", name: "named-reviewer" })).toBe(true);
    expect(deadRootExempt(config, { id: "ffff", name: "dev-supervisor" })).toBe(false);
    expect(deadRootExempt({ exempt: ["dev-supervisor"] }, { id: "ffff", name: "dev-supervisor" })).toBe(true);
  });

  it("config defaults to off; only mode \"on\" turns it on; it is host-only", () => {
    expect(DEFAULT_FABRIC_CONFIG.agents.deadRootFilter).toEqual({ mode: "off", exempt: [] });
    expect(normalizeDeadRootFilterConfig(undefined)).toEqual({ mode: "off", exempt: [] });
    expect(normalizeDeadRootFilterConfig({ mode: "ON", exempt: "x" })).toEqual({ mode: "off", exempt: [] });
    expect(normalizeDeadRootFilterConfig({ mode: "on", exempt: [" a ", "a"] })).toEqual({ mode: "on", exempt: ["a"] });
    expect(normalizeDeadRootFilterConfig({ mode: "on" })).toEqual({ mode: "on", exempt: [] });
    expect(normalizeFabricConfig({ agents: { deadRootFilter: { mode: "on", exempt: ["abc"] } } }).agents.deadRootFilter)
      .toEqual({ mode: "on", exempt: ["abc"] });
  });

  it("an invalid exempt shape disables the filter and logs one config warning", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      expect(normalizeDeadRootFilterConfig({ mode: "on", exempt: "bad" })).toEqual({ mode: "off", exempt: [] });
      expect(normalizeDeadRootFilterConfig({ mode: "on", exempt: "bad" })).toEqual({ mode: "off", exempt: [] });
      expect(warn).toHaveBeenCalledTimes(1);
      expect(String(warn.mock.calls[0]![0])).toContain("agents.deadRootFilter.exempt");
      expect(String(warn.mock.calls[0]![0])).toContain("disabled");
      for (const exempt of [null, 3, { keep: "x" }, ["keep", ""], ["keep", 3], ["  "], ["x".repeat(201)], Array(513).fill("x")]) {
        expect(normalizeDeadRootFilterConfig({ mode: "on", exempt }), JSON.stringify(exempt)?.slice(0, 40)).toEqual({ mode: "off", exempt: [] });
      }
      expect(normalizeFabricConfig({ agents: { deadRootFilter: { mode: "on", exempt: "bad" } } }).agents.deadRootFilter)
        .toEqual({ mode: "off", exempt: [] });
      // Mode off with a bad shape is simply off; nothing to warn about.
      warn.mockClear();
      expect(normalizeDeadRootFilterConfig({ mode: "off", exempt: "other-bad" })).toEqual({ mode: "off", exempt: [] });
      expect(warn).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });
});

const identity: MeshIdentity = { id: ROOT, name: "main", kind: "main", sessionId: "dead-root" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };
const setup = (filter: () => FabricDeadRootFilterConfig | undefined) => {
  const root = tmp();
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("dead-root", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true, claimResidency: "durable", rootId: ROOT, deadRootFilter: filter,
  });
  closers.push(async () => { await actors.close(); await agents.close(); });
  return { root, meshRoot, mesh, actors };
};
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};
const runDirs = (root: string, actorId: string) => {
  const dir = path.join(root, "actors", actorId, "runs");
  return fs.existsSync(dir) ? fs.readdirSync(dir) : [];
};
const skipLines = (meshRoot: string) => fs.existsSync(deadRootSkipsPath(meshRoot))
  ? fs.readFileSync(deadRootSkipsPath(meshRoot), "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>)
  : [];
const outs = (actors: ActorManager, id: string) => actors.messages(id).filter((message) => message.direction === "out").length;
const deadRootSkips = (actors: ActorManager, id: string) => actors.messages(id).filter((message) => message.reason === "filtered: dead-root");

describe("dead-root filter in the actor activation path", () => {
  const on = (exempt: string[] = []): FabricDeadRootFilterConfig => ({ mode: "on", exempt });

  it("skips a durable actor's event under a dead root with no run, and logs one JSONL line", async () => {
    const { root, meshRoot, mesh, actors } = setup(() => on());
    lease(meshRoot, ROOT, Date.now() - 30 * MIN);
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["github.demo"], residency: "durable", coalesce: false });
    const event = await mesh.publish({ topic: "github.demo", kind: "github.webhook", from, text: "wake" });
    await waitFor(() => deadRootSkips(actors, actor.id).length === 1 && actors.status(actor.id).status === "idle");
    expect(runDirs(root, actor.id)).toHaveLength(0);
    expect(outs(actors, actor.id)).toBe(0);
    expect(actors.status(actor.id).queued).toBe(0);
    const lines = skipLines(meshRoot);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toEqual({
      at: expect.any(String), actorId: actor.id, actorName: "reviewer", rootId: ROOT, eventId: event.id,
      topic: "github.demo", reason: "root lease expired 30 min ago; no live participant",
    });
    expect(Number.isFinite(Date.parse(lines[0]!.at as string))).toBe(true);
    // The root becomes live right after the dead verdict (a participant on another host appears; the
    // root lease file is unchanged): the next event runs; no dead verdict is reused.
    lease(meshRoot, "session:other-host", Date.now() + 10 * MIN, ROOT);
    participant(meshRoot, ROOT, "session:other-host", Date.now());
    await mesh.publish({ topic: "github.demo", kind: "github.webhook", from, text: "wake again" });
    await waitFor(() => outs(actors, actor.id) === 1 && actors.status(actor.id).status === "idle");
    expect(runDirs(root, actor.id)).toHaveLength(1);
    expect(deadRootSkips(actors, actor.id)).toHaveLength(1);
    expect(skipLines(meshRoot)).toHaveLength(1);
    // A caller's own ask is never skipped.
    await expect(actors.ask(actor.id, "direct")).resolves.toBeDefined();
    expect(runDirs(root, actor.id)).toHaveLength(2);
  }, 30_000);

  it("runs under a live root, under doubt, when exempt, and with mode off", async () => {
    const cases: Array<{ name: string; filter: () => FabricDeadRootFilterConfig | undefined; prepare: (meshRoot: string) => void }> = [
      { name: "live-lease", filter: () => on(), prepare: (m) => lease(m, ROOT, Date.now() + 10 * MIN) },
      { name: "live-participant", filter: () => on(), prepare: (m) => {
        lease(m, ROOT, Date.now() - 30 * MIN);
        lease(m, "session:other-host", Date.now() + 10 * MIN, ROOT);
        participant(m, ROOT, "session:other-host", Date.now());
      } },
      { name: "missing-both", filter: () => on(), prepare: () => {} },
      { name: "unreadable-lease", filter: () => on(), prepare: (m) => {
        fs.mkdirSync(path.dirname(hostLeasePath(m, ROOT)), { recursive: true });
        fs.writeFileSync(hostLeasePath(m, ROOT), "garbage");
      } },
      { name: "exempt", filter: () => on(["keep-me"]), prepare: (m) => lease(m, ROOT, Date.now() - 30 * MIN) },
      { name: "mode-off", filter: () => ({ mode: "off", exempt: [] }), prepare: (m) => lease(m, ROOT, Date.now() - 30 * MIN) },
      { name: "invalid-exempt", filter: () => ({ mode: "on", exempt: "bad" }) as unknown as FabricDeadRootFilterConfig,
        prepare: (m) => lease(m, ROOT, Date.now() - 30 * MIN) },
      { name: "unconfigured", filter: () => undefined, prepare: (m) => lease(m, ROOT, Date.now() - 30 * MIN) },
      { name: "config-throws", filter: () => { throw new Error("unreadable config"); }, prepare: (m) => lease(m, ROOT, Date.now() - 30 * MIN) },
    ];
    await Promise.all(cases.map(async ({ name, filter, prepare }) => {
      const { root, meshRoot, mesh, actors } = setup(filter);
      prepare(meshRoot);
      const actor = await actors.create({ name: name === "exempt" ? "keep-me" : `actor-${name}`, instructions: "Review.",
        topics: ["github.demo"], residency: "durable", coalesce: false });
      await mesh.publish({ topic: "github.demo", kind: "github.webhook", from, text: "wake" });
      await waitFor(() => outs(actors, actor.id) === 1 && actors.status(actor.id).status === "idle");
      expect(runDirs(root, actor.id), name).toHaveLength(1);
      expect(deadRootSkips(actors, actor.id), name).toHaveLength(0);
      expect(skipLines(meshRoot), name).toHaveLength(0);
    }));
  }, 60_000);

  it("never skips a session actor, even under a dead root", async () => {
    const { root, meshRoot, mesh, actors } = setup(() => on());
    lease(meshRoot, ROOT, Date.now() - 30 * MIN);
    const actor = await actors.create({ name: "session-actor", instructions: "Review.", topics: ["github.demo"], residency: "session", coalesce: false });
    await mesh.publish({ topic: "github.demo", kind: "github.webhook", from, text: "wake" });
    await waitFor(() => outs(actors, actor.id) === 1 && actors.status(actor.id).status === "idle");
    expect(runDirs(root, actor.id)).toHaveLength(1);
    expect(skipLines(meshRoot)).toHaveLength(0);
  }, 30_000);
});
