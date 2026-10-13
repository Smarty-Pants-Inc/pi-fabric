// smarty-dev#11105 item 2 — Decision C supplemental, RECORDED, NOT E2E. Installed Ryzen1 shape: one actor remove,
// then a concurrent repeat remove while the finisher's commit is in flight (actor already absent from the actor map,
// registry revocation not yet committed). The repeat call must join that commit and answer its actual result, not
// "Unknown Fabric actor", and never a false "cleaned". Exercised through the resident receiver's own calls
// (`owns` then `remove(..., {wait:false})`, src/residency/host.ts #removeResidentActor) on ActorManager and on
// ActorDirectory with the actor in the primary and in the secondary scope.
// Every actor-tree delete is guarded: beforeDelete throws (controlled), any actor-tree rmSync is recorded, not run;
// metadata.lock passes normally.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { main } from "../src/actors-cli.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { actorTreeDeletionHooks } from "../src/topology/main-publication-fence.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

type Shape = "manager" | "directory-primary" | "directory-secondary" | "manager-revocation-fails" | "directory-revocation-fails";
const settledState = async (promise: Promise<unknown>): Promise<"pending" | "settled"> =>
  Promise.race([promise.then(() => "settled" as const, () => "settled" as const),
    new Promise<"pending">(resolve => setTimeout(() => resolve("pending"), 50))]);

describe.skipIf(process.platform !== "linux")("remove during the finisher's commit (smarty-dev#11105 item 2)", () => {
  it.each<Shape>(["manager", "directory-primary", "directory-secondary", "manager-revocation-fails", "directory-revocation-fails"])(
    "%s: a repeat remove joins the in-flight commit and answers its actual result", async (shape) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-finish-answer-"));
      const roots = { project: path.join(root, "project-actors"), session: path.join(root, "session-actors") };
      const actorRoot = shape === "directory-secondary" ? roots.project : roots.session;
      const plainManager = shape.startsWith("manager");
      // Incomplete commit: the held revocation save fails once (the finisher's later retry is not held).
      const failRevocation = shape.endsWith("revocation-fails");
      const rootId = "session:finish";
      const identity = { id: rootId, name: "main", kind: "main" as const, sessionId: "finish" };
      const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
      const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
      const operatorCheck = vi.fn(async () => undefined);
      const rec: string[] = [];
      const rmSync = fs.rmSync.bind(fs);
      const inTree = (r: string) => [roots.project, roots.session].some(dir => r === dir || r.startsWith(dir + path.sep));
      vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
        const r = path.resolve(String(file));
        if (inTree(r) && fs.existsSync(r) && fs.statSync(r).isDirectory() && !path.basename(r).endsWith(".lock")) {
          rec.push(`rm ${r}`); return;
        }
        rmSync(file, options);
      });
      actorTreeDeletionHooks.beforeDelete = async () => { rec.push("beforeDelete"); throw new Error("controlled stop"); };
      let release!: () => void;
      const gate = new Promise<void>(resolve => { release = resolve; });
      const runId = "f".repeat(32);
      vi.spyOn(agents, "run").mockImplementation(async (_r, _s, onSpawned) => {
        onSpawned?.({ id: runId } as Parameters<NonNullable<typeof onSpawned>>[0]);
        await gate;
        return { id: runId, status: "stopped", text: "", usage: {} } as unknown as Awaited<ReturnType<typeof agents.run>>;
      });
      // Controlled registry-update pause: the revocation save of the commit (the only update after the marker exists).
      let marker = "";
      let releaseCommit!: () => void;
      const commitHeld = new Promise<void>(resolve => { releaseCommit = resolve; });
      let paused!: () => void;
      const atPause = new Promise<void>(resolve => { paused = resolve; });
      let held = false;
      const update = ActorRegistryStore.prototype.update;
      vi.spyOn(ActorRegistryStore.prototype, "update").mockImplementation(async function (this: ActorRegistryStore, select) {
        if (!held && marker && fs.existsSync(marker)) {
          held = true; paused(); await commitHeld;
          if (failRevocation) throw new Error("controlled revocation failure");
        }
        return update.call(this, select) as never;
      });
      const options = { actorRoot, rootId, persistent: true, closeGraceMs: 100, removalRetryMs: 5, beforeOperatorCleanup: operatorCheck };
      const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
      let settled: Promise<void> | undefined;
      const actors: ActorManager = plainManager
        ? new ActorManager("finish", identity, mesh, meshConfig, agents, () => {}, options)
        : new ActorDirectory(["finish", identity, mesh, meshConfig, agents, () => {}, options], roots, "session");
      try {
        const actor = await actors.create({ name: "finisher", instructions: "Work.", responseMode: "text",
          ...(shape === "directory-secondary" ? { scope: "project" as const } : {}) });
        if (!plainManager) expect(actor.scope).toBe(shape === "directory-secondary" ? "project" : "session");
        marker = path.join(actorRoot, `removal-${actor.id}.json`);
        actors.tell(actor.id, "go");
        for (const until = Date.now() + 10_000; actors.status(actor.id).inFlightRun?.id !== runId;) {
          if (Date.now() > until) throw new Error("run never started");
          await new Promise(r => setTimeout(r, 10));
        }
        // Counterexample kept: an accepted removal behind a live run answers pending at once (wait:false).
        expect(actors.owns(actor.id)).toBe(true);
        expect((await actors.remove(actor.id, { wait: false })).pending).toMatch(/pending behind/);
        settled = actors.removalSettled(actor.id);
        release();
        await atPause;
        // The window: the actor is gone from the actor map, the registry row is not yet revoked.
        expect(() => actors.status(actor.id)).toThrow(/Unknown Fabric actor/);
        expect(new ActorRegistryStore(actorRoot).records().some(r => r.id === actor.id)).toBe(true);
        expect(actors.cleanupObligation(actor.id)).toBeUndefined();
        // Unknown ids and aliases are not claimed by the in-flight finisher.
        const unknown = "0".repeat(32);
        for (const alias of [unknown, actor.name, actor.id.slice(0, 8)]) {
          await expect((async () => actors.remove(alias, { wait: false }))()).rejects.toThrow(/Unknown Fabric actor/);
          if (plainManager) expect(() => actors.owns(alias)).toThrow(/Unknown Fabric actor/);
          else expect(actors.owns(alias)).toBe(false);
        }
        // The resident receiver's gate, then its call; plus a waiting caller and an operator caller.
        let ownsExact: unknown;
        try { ownsExact = actors.owns(actor.id); } catch (error) { ownsExact = String(error); }
        const settle = (p: Promise<unknown>) => p.then(v => ({ v }), (e: unknown) => ({ e: String(e) }));
        const quick = settle(actors.remove(actor.id, { wait: false }));
        const waiting = settle(actors.remove(actor.id, { wait: true }));
        const operator = actors.remove(actor.id, { wait: false, operator: true }).then(
          v => ({ v, markerAtAnswer: JSON.parse(fs.readFileSync(marker, "utf8")) as { operator?: true } }),
          (e: unknown) => ({ e: String(e) }));
        // Each caller waits on the held commit; nothing answers before it.
        expect(await settledState(quick)).toBe("pending");
        expect(await settledState(waiting)).toBe("pending");
        releaseCommit();
        const [q, w, o] = await Promise.all([quick, waiting, operator]);
        if (failRevocation) {
          // The joined callers get the commit's actual failure: not Unknown, not a removal, not cleaned.
          const restored = { status: actors.status(actor.id).id, row: new ActorRegistryStore(actorRoot).records().some(r => r.id === actor.id) };
          fs.appendFileSync(path.resolve(".local/11105-item2-observed.jsonl"), JSON.stringify({ shape, ownsExact, q, w, o, restored }) + "\n");
          expect(ownsExact).toBe(true);
          for (const answer of [q, w, o]) {
            expect(answer).toHaveProperty("e");
            expect((answer as { e: string }).e).toMatch(/controlled revocation failure/);
            expect((answer as { e: string }).e).not.toMatch(/Unknown Fabric actor/);
          }
          expect(restored).toEqual({ status: actor.id, row: true });
          expect(actors.cleanupObligation(actor.id)).toBeUndefined();
          await settled;
          return;
        }
        await settled;
        const results = { ownsExact, q, w, o, rec };
        fs.mkdirSync(path.resolve(".local"), { recursive: true });
        fs.appendFileSync(path.resolve(".local/11105-item2-observed.jsonl"), JSON.stringify({ shape, ...results }) + "\n");
        expect(ownsExact).toBe(true);
        // The commit's real outcome: revoked, tree delete refused (controlled) => actual pending, never cleaned.
        for (const answer of [q, w, o]) {
          expect(answer).not.toHaveProperty("e");
          expect((answer as { v: unknown }).v).toMatchObject({ removed: true, cleaned: false, pending: expect.stringMatching(/controlled stop/) });
        }
        // The operator flag was durable on disk before the operator caller was answered.
        expect((o as { markerAtAnswer: { operator?: true } }).markerAtAnswer.operator).toBe(true);
        expect(new ActorRegistryStore(actorRoot).records().some(r => r.id === actor.id)).toBe(false);
        expect(actors.cleanupObligation(actor.id)).toMatchObject({ id: actor.id, removal: { state: expect.stringMatching(/controlled stop/) } });
        expect(fs.existsSync(path.join(actorRoot, actor.id))).toBe(true);
        expect(rec.length).toBeGreaterThan(0);
        expect(rec.every(entry => entry === "beforeDelete")).toBe(true);
      } finally {
        // Guards stay installed through the gate releases, the finisher's settlement, close and temp-root cleanup.
        release();
        releaseCommit();
        await settled?.catch(() => undefined);
        await actors.close().catch(() => undefined);
        await agents.close().catch(() => undefined);
        fs.rmSync(root, { recursive: true, force: true });
        vi.restoreAllMocks();
        delete actorTreeDeletionHooks.beforeDelete;
      }
    }, 60_000);
});

// The real resident receiver, through its actual request-file channel: the operator path (ResidentHost operatorActor via
// the actors CLI), whose listOwned() lookup no longer sees an actor whose commit is in flight, and the ordinary path
// (ResidentActorClient.removeActor). Only an exact-id, non-dry-run remove joins that commit.
type Receiver = "operator" | "ordinary";
describe.skipIf(process.platform !== "linux")("resident remove during the commit (smarty-dev#11105 item 2)", () => {
  beforeEach(() => installInProcessResidentFence());
  it.each<Receiver>(["operator", "ordinary"])("%s: an exact-id resident remove joins the in-flight commit and answers its actual result", async (receiver) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-finish-operator-"));
    const meshRoot = path.join(root, "mesh");
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:finish-op", sessionId: "finish-op", cwd: process.cwd(), projectRoot: process.cwd(),
      meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:finish-op"),
      fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 4, budgetUsd: 0 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 30 }, retention: { ...DEFAULT_FABRIC_CONFIG.retention },
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
    };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const rec: string[] = [];
    const rmSync = fs.rmSync.bind(fs);
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      const r = path.resolve(String(file));
      if ((r === config.actorRoot || r.startsWith(config.actorRoot + path.sep)) && fs.existsSync(r) && fs.statSync(r).isDirectory()
        && !path.basename(r).endsWith(".lock")) {
        rec.push(`rm ${r}`); return;
      }
      rmSync(file, options);
    });
    actorTreeDeletionHooks.beforeDelete = async () => { rec.push("beforeDelete"); throw new Error("controlled stop"); };
    let marker = "";
    let releaseCommit!: () => void;
    const commitHeld = new Promise<void>(resolve => { releaseCommit = resolve; });
    let paused!: () => void;
    const atPause = new Promise<void>(resolve => { paused = resolve; });
    let held = false;
    const update = ActorRegistryStore.prototype.update;
    vi.spyOn(ActorRegistryStore.prototype, "update").mockImplementation(async function (this: ActorRegistryStore, select) {
      if (!held && marker && fs.existsSync(marker)) { held = true; paused(); await commitHeld; }
      return update.call(this, select) as never;
    });
    const host = new ResidentHost(config, () => {});
    const confirm = ["--confirm-dead-root", config.rootId];
    const cli = async (action: "stop" | "remove", actor: string, flags: string[] = confirm) => {
      let out = "", err = "";
      const code = await main([action, "--resident", config.residencyRoot, "--actor", actor, "--mesh-root", meshRoot, ...flags],
        { out: text => { out += text; }, err: text => { err += text; } });
      return { code, out, err };
    };
    // The ordinary caller: the resident client's own request file, same root; answered as {code, out, err} too.
    const client = new ResidentActorClient(meshRoot, config.rootId);
    const ordinary = (actor: string) => client.removeActor(actor).then(
      v => ({ code: 0, out: JSON.stringify(v), err: "" }), (e: unknown) => ({ code: 1, out: "", err: String(e) }));
    let first: Promise<unknown> | undefined;
    let actorId = "";
    try {
      await host.start();
      const actor = await host.actors.create({ name: "finish-op", instructions: "Run", model: "fixture/visible",
        residency: "durable", transport: "process", extensions: false });
      actorId = actor.id;
      marker = path.join(config.actorRoot, `removal-${actor.id}.json`);
      // Installed shape: one remove (its commit held at revocation), then a concurrent repeat remove.
      first = host.actors.remove(actor.id, { wait: false }).then(v => ({ v }), (e: unknown) => ({ e: String(e) }));
      await atPause;
      expect(host.actors.listOwned().some(a => a.id === actor.id)).toBe(false);
      expect(new ActorRegistryStore(config.actorRoot).records().some(r => r.id === actor.id)).toBe(true);
      // Alias, stop and dry-run never synthesize the finishing actor.
      const refusals = receiver === "operator"
        ? await Promise.all([cli("remove", actor.name), cli("stop", actor.id), cli("remove", actor.id, [...confirm, "--dry-run"])])
        // Ordinary: an unknown id is refused through the same host queue before the joined request.
        : [await ordinary("0".repeat(32))];
      for (const refusal of refusals) {
        expect(refusal.code).not.toBe(0);
        expect(refusal.err).toMatch(receiver === "operator" ? /Unknown Fabric actor/ : /Unknown Fabric actor|does not own/);
      }
      const call = receiver === "operator" ? cli("remove", actor.id) : ordinary(actor.id);
      expect(await Promise.race([call.then(() => "settled"), new Promise(r => setTimeout(() => r("pending"), 1_500))])).toBe("pending");
      // The joined request waiting on the commit must not stall the host queue: an unrelated request through the same
      // client is answered (its ordinary error) while the commit is still held.
      const unrelated = await Promise.race([ordinary("1".repeat(32)),
        new Promise<"stalled">(r => setTimeout(() => r("stalled"), 1_500))]);
      expect(unrelated).toMatchObject({ code: 1, err: expect.stringMatching(/Resident host does not own 1{32}/) });
      expect(held).toBe(true);
      releaseCommit();
      const [op, firstResult] = await Promise.all([call, first]);
      const markerAfter = fs.existsSync(marker) ? JSON.parse(fs.readFileSync(marker, "utf8")) as { operator?: true; pending?: string } : null;
      fs.mkdirSync(path.resolve(".local"), { recursive: true });
      fs.appendFileSync(path.resolve(".local/11105-item2-observed.jsonl"), JSON.stringify({ shape: `resident-${receiver}`, refusals, op, firstResult, markerAfter, rec }) + "\n");
      // The joined call answers the commit's actual result: success, removal pending (controlled stop), not cleaned.
      expect(op).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(op.out)).toMatchObject({ cleaned: false, pending: expect.stringMatching(/controlled stop/),
        ...(receiver === "ordinary" ? { removed: true } : { ok: true, action: "remove" }) });
      expect(firstResult).toMatchObject({ v: { removed: true, cleaned: false } });
      // The operator flag persists only for the operator caller; the ordinary removal stays ordinary.
      expect(markerAfter).not.toBeNull();
      if (receiver === "operator") expect(markerAfter?.operator).toBe(true);
      else expect(markerAfter).not.toHaveProperty("operator");
      expect(new ActorRegistryStore(config.actorRoot).records().some(r => r.id === actor.id)).toBe(false);
      expect(rec.every(entry => entry === "beforeDelete")).toBe(true);
    } finally {
      // Guards stay installed through the gate release, pending settlement, host close and temp-root cleanup.
      releaseCommit();
      await first?.catch(() => undefined);
      if (actorId) await host.actors.removalSettled(actorId)?.catch(() => undefined);
      await host.close().catch(() => undefined);
      fs.rmSync(root, { recursive: true, force: true });
      vi.restoreAllMocks();
      delete actorTreeDeletionHooks.beforeDelete;
    }
  }, 90_000);
});
