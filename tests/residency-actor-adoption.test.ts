import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { main } from "../src/actors-cli.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { MeshStore } from "../src/mesh/store.js";
import { actorAdoptionIntentPath, moveActorCustody, recoverActorAdoption, type ActorAdoptionPhase } from "../src/residency/actor-adoption.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { projectOf } from "../src/topology/project-identity.js";

// A seam between the host's precheck and the fenced move: tests change state there.
const adoptionHooks = vi.hoisted(() => ({ beforeMove: undefined as undefined | (() => void) }));
vi.mock("../src/residency/actor-adoption.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/residency/actor-adoption.js")>();
  return { ...actual, moveActorCustody: async (move: Parameters<typeof actual.moveActorCustody>[0]) => {
    adoptionHooks.beforeMove?.();
    return actual.moveActorCustody(move);
  } };
});

// smarty-dev#5919: adopt a dead root's durable actor into a live root.
beforeEach(() => installInProcessResidentFence());
const waitFor = (predicate: () => boolean) => vi.waitFor(() => expect(predicate()).toBe(true), { timeout: 15_000, interval: 30 });

const hostConfig = (meshRoot: string, sessionId: string): ResidentHostConfig => {
  const rootId = `session:${sessionId}`;
  return {
    format: 1, rootId, sessionId, cwd: process.cwd(), projectRoot: process.cwd(),
    meshRoot, actorRoot: path.join(meshRoot, "actors"), sessionActorRoot: path.join(meshRoot, "actors", sessionId),
    residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 4, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 30 }, retention: { ...DEFAULT_FABRIC_CONFIG.retention },
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
};

const startHost = async (config: ResidentHostConfig): Promise<ResidentHost> => {
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const host = new ResidentHost(config, () => {});
  try { await host.start(); } catch (error) { await host.close(); throw error; }
  return host;
};

/** A live Main of the target root: an unexpired root lease plus a live root participant. */
const startLiveMain = async (config: ResidentHostConfig) => {
  const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
  const mesh = new MeshStore(config.meshRoot, 65536, 1000);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: config.rootId, rootId: config.rootId, identity });
  directory.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
    runner: "pi", transport: "host", capabilities: ["fabric"], sessionId: config.sessionId, cwd: config.cwd,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  await directory.start();
  writeHostLease(config.meshRoot, { id: config.rootId, rootId: config.rootId, identityId: config.rootId,
    updatedAt: Date.now(), expiresAt: Date.now() + 120_000 });
  const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 5000 });
  control.start(() => ({ accepted: false }));
  return { identity, directory, control, close: async () => { await control.close(); await directory.close(); } };
};

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-adopt-"));
  const meshRoot = path.join(root, "mesh");
  const deadConfig = hostConfig(meshRoot, "dead-owner"), liveConfig = hostConfig(meshRoot, "live-owner");
  let dead: ResidentHost | undefined = await startHost(deadConfig);
  const live = await startHost(liveConfig);
  const main = await startLiveMain(liveConfig);
  const cli = async (flags: string[]) => {
    let out = "", err = "";
    const code = await mainCli(["adopt", ...flags, "--mesh-root", meshRoot],
      { out: text => { out += text; }, err: text => { err += text; } });
    return { code, out, err };
  };
  const adopt = (actor: string, extra: string[] = ["--confirm-dead-root", deadConfig.rootId]) =>
    cli(["--resident", deadConfig.residencyRoot, "--actor", actor, "--into", liveConfig.rootId, ...extra]);
  const create = async (name: string, scope: "project" | "session" = "project") => {
    const actor = await dead!.actors.create({ name, instructions: `Keep ${name}`, model: "fixture/visible",
      residency: "durable", transport: "process", extensions: false, topics: ["adopt.topic"], scope });
    // Real history before the move: one accepted message and its reply.
    dead!.actors.tell(actor.id, `history for ${name}`);
    await waitFor(() => dead!.actors.messages(actor.id).some(message => message.direction === "out"));
    return actor;
  };
  const rows = (actorRoot: string) => new ActorRegistryStore(actorRoot).records();
  return { root, meshRoot, deadConfig, liveConfig, live, main, adopt, create, rows,
    get dead() { return dead; },
    // A host that went down: close without stopping its durable actors.
    stopDead: async () => { await dead!.close(); dead = undefined; },
    close: async () => {
      for (const host of [dead, live]) {
        if (!host) continue;
        for (const actor of host.actors.listOwned()) await host.actors.stop(actor.id, undefined, true).catch(() => undefined);
        await host.close();
      }
      await main.close();
      fs.rmSync(root, { recursive: true, force: true });
    } };
};
const mainCli = main;

const steerFromNewRoot = async (f: Awaited<ReturnType<typeof fixture>>, actorId: string, text: string) => {
  const receipt = await f.main.control.request(f.live.hostId, actorId, "steer", { message: text }, f.live.identity.id);
  expect(receipt).toMatchObject({ acknowledged: true });
  await waitFor(() => f.live.actors.messages(actorId).some(message => message.direction === "in" &&
    JSON.stringify(message).includes(text)));
};

describe("fabric-actors adopt (smarty-dev#5919)", () => {
  it("adopts from a dead root whose resident host still runs: id, instructions, model, topics and history are kept, and the new root steers it", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("keeper");
      const history = f.dead!.actors.messages(actor.id).map(message => message.id);
      await f.dead!.participants.refresh();
      expect(f.live.participants.get(actor.id, Date.now(), { fresh: true })?.ownerHostId).toBe(f.dead!.hostId);

      const dry = await f.adopt(actor.name, ["--dry-run"]);
      expect(dry).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(dry.out)).toMatchObject({ deadHost: "live", adoption: { actorId: actor.id, scope: "project" } });
      expect(f.rows(f.deadConfig.actorRoot).find(row => row.id === actor.id)?.rootId).toBe(f.deadConfig.rootId);

      const result = await f.adopt(actor.name);
      expect(result).toMatchObject({ code: 0, err: "" });
      const out = JSON.parse(result.out);
      expect(out).toMatchObject({ deadHost: "live", released: { actor: actor.id },
        actor: { id: actor.id, name: "keeper", rootId: f.liveConfig.rootId, model: "fixture/visible" },
        adoption: { fromRootId: f.deadConfig.rootId, intoRootId: f.liveConfig.rootId } });
      const registry = f.rows(f.liveConfig.actorRoot).filter(row => row.id === actor.id);
      expect(registry).toHaveLength(1);
      expect(registry[0]).toMatchObject({ rootId: f.liveConfig.rootId, adoptedFrom: [f.deadConfig.rootId],
        name: "keeper", instructions: "Keep keeper", model: "fixture/visible", topics: ["adopt.topic"], createdAt: actor.createdAt });
      expect(fs.existsSync(actorAdoptionIntentPath(f.liveConfig.residencyRoot, actor.id))).toBe(false);
      const status = f.live.actors.status(actor.id);
      expect(status).toMatchObject({ id: actor.id, rootId: f.liveConfig.rootId, model: "fixture/visible", topics: ["adopt.topic"] });
      expect(f.live.actors.instructions(actor.id)).toBe("Keep keeper");
      expect(f.live.actors.owns(actor.id)).toBe(true);
      expect(f.dead!.actors.owns(actor.id)).toBe(false);
      expect(f.live.actors.messages(actor.id).map(message => message.id)).toEqual(history);
      await f.live.participants.refresh();
      await waitFor(() => f.live.participants.get(actor.id, Date.now(), { fresh: true })?.ownerHostId === f.live.hostId);
      await steerFromNewRoot(f, actor.id, "steer after adoption");
      // No replay: only the one new steer arrived after the move.
      const inbound = f.live.actors.messages(actor.id).filter(message => message.direction === "in");
      expect(inbound.filter(message => JSON.stringify(message).includes("history for keeper"))).toHaveLength(1);
      expect(f.dead!.actors.owns(actor.id)).toBe(false);
    } finally { await f.close(); }
  }, 60_000);

  it("adopts a session-scope actor across registries: row and actor directory move, the source keeps none", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("sessioned", "session");
      const source = f.deadConfig.sessionActorRoot!, target = f.liveConfig.sessionActorRoot!;
      expect(f.rows(source).some(row => row.id === actor.id)).toBe(true);
      const history = f.dead!.actors.messages(actor.id).map(message => message.id);
      const result = await f.adopt(actor.id);
      expect(result).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(result.out)).toMatchObject({ adoption: { scope: "session", sourceActorRoot: source, targetActorRoot: target } });
      expect(f.rows(source).some(row => row.id === actor.id)).toBe(false);
      expect(fs.existsSync(path.join(source, actor.id))).toBe(false);
      expect(f.rows(target).filter(row => row.id === actor.id)).toMatchObject([{ rootId: f.liveConfig.rootId }]);
      expect(fs.existsSync(path.join(target, actor.id))).toBe(true);
      expect(f.live.actors.messages(actor.id).map(message => message.id)).toEqual(history);
      await steerFromNewRoot(f, actor.id, "steer session adoptee");
    } finally { await f.close(); }
  }, 60_000);

  it("adopts offline from a dead root whose resident host is down, under its host fence", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("offline");
      const history = f.dead!.actors.messages(actor.id).map(message => message.id);
      await f.stopDead();
      const result = await f.adopt(actor.id);
      expect(result).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(result.out)).toMatchObject({ deadHost: "stopped", actor: { id: actor.id, rootId: f.liveConfig.rootId } });
      expect(f.rows(f.liveConfig.actorRoot).filter(row => row.id === actor.id)).toHaveLength(1);
      expect(f.live.actors.messages(actor.id).map(message => message.id)).toEqual(history);
      await steerFromNewRoot(f, actor.id, "steer offline adoptee");
    } finally { await f.close(); }
  }, 60_000);

  it("refuses on a live old root (lease veto), on missing or mismatched confirmation, and into a root without a live Main", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("guarded");
      const unchanged = () => {
        expect(f.rows(f.deadConfig.actorRoot).find(row => row.id === actor.id)?.rootId).toBe(f.deadConfig.rootId);
        expect(f.dead!.actors.owns(actor.id)).toBe(true);
      };
      const missing = await f.adopt(actor.id, []);
      expect(missing.code).toBe(1); expect(missing.err).toContain("Missing --confirm-dead-root"); unchanged();
      const mismatched = await f.adopt(actor.id, ["--confirm-dead-root", f.liveConfig.rootId]);
      expect(mismatched.code).toBe(1); expect(mismatched.err).toContain("Mismatched --confirm-dead-root"); unchanged();
      writeHostLease(f.meshRoot, { id: f.deadConfig.rootId, rootId: f.deadConfig.rootId, identityId: f.deadConfig.rootId,
        updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      const leased = await f.adopt(actor.id);
      expect(leased.code).toBe(1); expect(leased.err).toContain("live root lease"); unchanged();
      writeHostLease(f.meshRoot, { id: f.deadConfig.rootId, rootId: f.deadConfig.rootId, identityId: f.deadConfig.rootId,
        updatedAt: 1, expiresAt: 2 });
      // A target root without a live Main (lease lapsed, participant gone) is refused too.
      await f.main.close();
      writeHostLease(f.meshRoot, { id: f.liveConfig.rootId, rootId: f.liveConfig.rootId, identityId: f.liveConfig.rootId,
        updatedAt: 1, expiresAt: 2 });
      const noMain = await f.adopt(actor.id);
      expect(noMain.code).toBe(1); expect(noMain.err).toContain("Adoption target root");
      expect(f.rows(f.deadConfig.actorRoot).find(row => row.id === actor.id)?.rootId).toBe(f.deadConfig.rootId);
    } finally { await f.close(); }
  }, 60_000);

  it("a crash after a committed phase is recovered by the next adopt through the live host channel", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("crashy");
      vi.stubEnv("PI_FABRIC_TEST_ADOPTION_CRASH", "added");
      const crashed = await f.adopt(actor.id);
      expect(crashed.code).toBe(1); expect(crashed.err).toContain("test adoption crash after added");
      expect(fs.existsSync(actorAdoptionIntentPath(f.liveConfig.residencyRoot, actor.id))).toBe(true);
      vi.unstubAllEnvs();
      const retried = await f.adopt(actor.id);
      expect(retried).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(retried.out)).toMatchObject({ actor: { id: actor.id, rootId: f.liveConfig.rootId },
        adoption: { recovered: { [actor.id]: "completed" } } });
      expect(f.rows(f.liveConfig.actorRoot).filter(row => row.id === actor.id)).toHaveLength(1);
      expect(fs.existsSync(actorAdoptionIntentPath(f.liveConfig.residencyRoot, actor.id))).toBe(false);
      await steerFromNewRoot(f, actor.id, "steer after recovery");
    } finally { vi.unstubAllEnvs(); await f.close(); }
  }, 60_000);
});

describe("fabric-actors adopt: the source project fails closed (smarty-dev#5919)", () => {
  const here = projectOf(process.cwd());
  /** Offline dead root (host down), so only the test edits its registry row and config. */
  const offline = async () => {
    const f = await fixture();
    const actor = await f.create("projected");
    await f.stopDead();
    const setRowProject = (project: string | undefined) => {
      const store = new ActorRegistryStore(f.deadConfig.actorRoot);
      store.write(store.records().map(row => {
        if (row.id !== actor.id) return row;
        const { project: _project, ...rest } = row;
        return project === undefined ? rest : { ...rest, project };
      }), { durable: true });
    };
    const configFile = path.join(f.deadConfig.residencyRoot, "config.json");
    const setConfigProject = (project: string | undefined) => {
      const { project: _project, ...rest } = JSON.parse(fs.readFileSync(configFile, "utf8")) as ResidentHostConfig;
      fs.writeFileSync(configFile, JSON.stringify(project === undefined ? rest : { ...rest, project }));
    };
    const unmoved = () => {
      expect(f.rows(f.deadConfig.actorRoot).filter(row => row.id === actor.id)).toMatchObject([{ rootId: f.deadConfig.rootId }]);
      expect(f.rows(f.liveConfig.actorRoot).some(row => row.id === actor.id && row.rootId === f.liveConfig.rootId)).toBe(false);
      expect(f.live.actors.owns(actor.id)).toBe(false);
      expect(fs.existsSync(actorAdoptionIntentPath(f.liveConfig.residencyRoot, actor.id))).toBe(false);
    };
    return { f, actor, setRowProject, setConfigProject, unmoved };
  };

  it("a row without a project takes the dead root's configured one: another project or none is refused, the same project adopts", async () => {
    const { f, actor, setRowProject, setConfigProject, unmoved } = await offline();
    try {
      expect(f.rows(f.deadConfig.actorRoot).find(row => row.id === actor.id)?.project).toBe(here);
      setRowProject(undefined);
      setConfigProject("/elsewhere/other-project");
      for (const extra of [["--dry-run"], ["--confirm-dead-root", f.deadConfig.rootId]]) {
        const foreign = await f.adopt(actor.id, extra);
        expect(foreign.code).toBe(1);
        expect(foreign.err).toContain("belongs to project /elsewhere/other-project");
        unmoved();
      }
      setConfigProject(undefined);
      const unknown = await f.adopt(actor.id);
      expect(unknown.code).toBe(1); expect(unknown.err).toContain("source project unknown"); unmoved();
      setConfigProject(here);
      const same = await f.adopt(actor.id);
      expect(same).toMatchObject({ code: 0, err: "" });
      expect(f.rows(f.liveConfig.actorRoot).filter(row => row.id === actor.id)).toMatchObject([{ rootId: f.liveConfig.rootId }]);
    } finally { await f.close(); }
  }, 60_000);

  it.each([
    ["moved to another project", "/elsewhere/other-project", "belongs to project /elsewhere/other-project"],
    ["dropped", undefined, "source project unknown"],
  ] as const)("a source project %s between the precheck and the fenced recheck is refused", async (_label, changed, message) => {
    const { f, actor, setRowProject, setConfigProject, unmoved } = await offline();
    try {
      setRowProject(undefined);
      setConfigProject(here);
      adoptionHooks.beforeMove = () => { setConfigProject(changed); };
      const raced = await f.adopt(actor.id);
      expect(raced.code).toBe(1); expect(raced.err).toContain(message); unmoved();
      // A row that gains a foreign project under the fence is refused the same way.
      setConfigProject(here);
      adoptionHooks.beforeMove = () => { setRowProject("/elsewhere/other-project"); };
      const rowRaced = await f.adopt(actor.id);
      expect(rowRaced.code).toBe(1); expect(rowRaced.err).toContain("belongs to project /elsewhere/other-project"); unmoved();
    } finally { adoptionHooks.beforeMove = undefined; await f.close(); }
  }, 60_000);
});

describe("two-phase actor custody move (smarty-dev#5919)", () => {
  const id = "0123456789abcdef0123456789abcdef";
  const seed = (shared: boolean) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-adopt-move-"));
    const source = path.join(root, "actors", "dead");
    const target = shared ? source : path.join(root, "actors", "live");
    const store = new ActorRegistryStore(source);
    const row = { id, name: "mover", rootId: "session:dead", residency: "durable", instructions: "Keep moving",
      model: "fixture/visible", topics: ["a.b"], createdAt: 1, updatedAt: 1,
      messages: [{ id: "m1", actorId: id, direction: "in", text: "kept", createdAt: 1 }] };
    const other = { id: "f".repeat(32), name: "bystander", rootId: "session:dead", residency: "durable", instructions: "Stay", createdAt: 1 };
    fs.mkdirSync(path.join(source, id), { recursive: true });
    fs.writeFileSync(path.join(source, id, "session.jsonl"), "{\"kept\":true}\n");
    fs.writeFileSync(path.join(source, id, "queue-cursor.json"), JSON.stringify({ format: 1, items: [{ id: "q1" }] }));
    store.write([row, other], { durable: true });
    const intentFile = path.join(root, "residency", "adoptions", `${id}.json`);
    const move = (fault?: ActorAdoptionPhase) => moveActorCustody({ actorId: id, fromRootId: "session:dead", intoRootId: "session:live",
      sourceActorRoot: source, targetActorRoot: target, intentFile,
      ...(fault ? { fault: (phase: ActorAdoptionPhase) => { if (phase === fault) throw new Error(`crash after ${phase}`); } } : {}) });
    /** Every registry row naming the actor: exactly one at every crash point (or the intent). */
    const named = () => [...new Set([source, target])].flatMap(dir => new ActorRegistryStore(dir).records()
      .filter(candidate => candidate.id === id).map(candidate => ({ dir, rootId: candidate.rootId, row: candidate })));
    return { root, source, target, intentFile, move, named, store, close: () => fs.rmSync(root, { recursive: true, force: true }) };
  };

  const committed = (s: ReturnType<typeof seed>) => {
    const rows = s.named();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ dir: s.target, rootId: "session:live" });
    expect(rows[0]!.row).toMatchObject({ name: "mover", instructions: "Keep moving", model: "fixture/visible",
      topics: ["a.b"], adoptedFrom: ["session:dead"] });
    expect(new ActorRegistryStore(s.target).messages(rows[0]!.row).map(message => (message as { id: string }).id)).toEqual(["m1"]);
    expect(fs.readFileSync(path.join(s.target, id, "session.jsonl"), "utf8")).toContain("kept");
    expect(fs.existsSync(path.join(s.target, id, "queue-cursor.json"))).toBe(true);
    if (s.source !== s.target) expect(fs.existsSync(path.join(s.source, id))).toBe(false);
    expect(new ActorRegistryStore(s.source).records().some(candidate => candidate.name === "bystander")).toBe(true);
    expect(fs.existsSync(s.intentFile)).toBe(false);
  };

  it.each([false, true])("moves without a crash (shared registry: %s)", async shared => {
    const s = seed(shared);
    try { await s.move(); committed(s); } finally { s.close(); }
  });

  it.each([
    ["prepared", false, "rolledBack"], ["copied", false, "rolledBack"], ["removed", false, "rolledForward"], ["added", false, "completed"],
    ["prepared", true, "rolledBack"], ["added", true, "completed"],
  ] as const)("a crash after %s (shared registry: %s) recovers with no lost or duplicated actor", async (phase, shared, outcome) => {
    const s = seed(shared);
    try {
      await expect(s.move(phase)).rejects.toThrow(`crash after ${phase}`);
      expect(fs.existsSync(s.intentFile)).toBe(true);
      // Never two runnable rows; zero rows only while the intent snapshot holds the actor.
      const during = s.named();
      expect(during.length).toBeLessThanOrEqual(1);
      if (during.length === 0) expect(JSON.parse(fs.readFileSync(s.intentFile, "utf8")).row).toMatchObject({ id, rootId: "session:live" });
      expect(await recoverActorAdoption(s.intentFile)).toBe(outcome);
      expect(await recoverActorAdoption(s.intentFile)).toBe("none");
      if (outcome === "rolledBack") {
        expect(s.named()).toMatchObject([{ dir: s.source, rootId: "session:dead" }]);
        expect(fs.existsSync(path.join(s.source, id, "session.jsonl"))).toBe(true);
        if (!shared) expect(fs.existsSync(path.join(s.target, id))).toBe(false);
        expect(fs.existsSync(s.intentFile)).toBe(false);
        // The authorized retry then completes the move.
        await s.move();
      }
      committed(s);
    } finally { s.close(); }
  });

  it("refuses a row that another root already adopted", async () => {
    const s = seed(false);
    try {
      await s.move();
      await expect(s.move()).rejects.toThrow(/Unknown Fabric actor/);
      committed(s);
    } finally { s.close(); }
  });
});
