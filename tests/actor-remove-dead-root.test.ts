import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { main } from "../src/actors-cli.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorBindingStore } from "../src/actors/binding-store.js";
import { offlineRemovalHooks } from "../src/actors/remove-offline.js";
import { MeshStore, meshProcessStartedAt } from "../src/mesh/store.js";
import { ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import * as fileLock from "../src/residency/file-lock.js";
import { ResidentHost, ResidentHostAlreadyRunning } from "../src/residency/host.js";
import { residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { RESIDENT_HOST_FORMAT } from "../src/residency/protocol.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile, writeParticipantFile } from "../src/topology/participant-files.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MAIN_PUBLICATION_FENCE_TTL_MS, mainPublicationFenced, takeMainPublicationFence } from "../src/topology/main-publication-fence.js";
import type { MeshIdentity } from "../src/mesh/store.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";


/** A real root Main publishing its root participant through ParticipantDirectory. */
const mainDirectory = (meshRoot: string, rootId: string): ParticipantDirectory => {
  const identity: MeshIdentity = { id: rootId, name: "main", kind: "main" };
  const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
    enabled: true, hostId: rootId, rootId, identity, heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false });
  directory.registerSource(() => [{ format: 1, id: rootId, kind: "root", rootId, ownerHostId: rootId, ownerIdentityId: rootId,
    name: "main", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    sessionId: "dead-root", startedAt: Date.now(), updatedAt: Date.now(), pendingMessages: false, controlProtocol: "v1",
    mainProcess: { pid: process.pid, host: os.hostname() } } as FabricParticipantRecord]);
  return directory;
};
const rootParticipantKey = (rootId: string) => "topology/participants/" + createHash("sha256").update(rootId).digest("hex");

// smarty-dev#7817: `fabric-actors remove --confirm-dead-root --main-stopped --evidence` for the two dead-root shapes.
const EVIDENCE = "herdr agent list: no pane for session dead-root\nps: no pi process for session dead-root";
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-dead-root-"));
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:dead-root", sessionId: "dead-root", cwd: process.cwd(), projectRoot: process.cwd(),
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:dead-root"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 4, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 30 }, retention: { ...DEFAULT_FABRIC_CONFIG.retention },
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const host = new ResidentHost(config, () => {});
  try { await host.start(); } catch (error) { await host.close(); throw error; }
  let closed = false;
  const confirmed = ["--confirm-dead-root", config.rootId, "--main-stopped", "--evidence", EVIDENCE];
  const cli = async (actor: string, flags: string[] = confirmed) => {
    let out = "", err = "";
    const code = await main(["remove", "--resident", config.residencyRoot, "--actor", actor, "--mesh-root", meshRoot, ...flags],
      { out: text => { out += text; }, err: text => { err += text; } });
    return { code, out, err };
  };
  const create = (name: string) => host.actors.create({ name, instructions: "Run", model: "fixture/visible",
    residency: "durable", transport: "process", extensions: false });
  const registered = (id: string) => new ActorRegistryStore(config.actorRoot).records().some(actor => actor.id === id);
  const archives = () => {
    const dir = path.join(config.residencyRoot, "archives");
    return fs.existsSync(dir) ? fs.readdirSync(dir).flatMap(name => fs.readdirSync(path.join(dir, name)).map(file => path.join(name, file))) : [];
  };
  // By default the recorded Main process is gone: a pid above pid_max never exists on this host.
  const mainParticipant = (updatedAt: number, ownerHostId = config.rootId,
    mainProcess: { pid?: number; host?: string; startTime?: string } = { pid: 2147483647, host: os.hostname(), startTime: "1" }) => writeParticipantFile(meshRoot, {
    key: "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex"),
    value: { id: config.rootId, rootId: config.rootId, kind: "root", ownerHostId, ownerIdentityId: ownerHostId, mainProcess },
    version: 1, updatedAt, updatedBy: { id: config.rootId, name: "main", kind: "main" },
  });
  const killResident = async () => {
    // A crash leaves owner.json and host.lock behind; their pid no longer exists.
    const owner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8"));
    await host.close(); closed = true;
    for (const name of ["owner.json", "host.lock"]) {
      fs.writeFileSync(path.join(config.residencyRoot, name), JSON.stringify({ ...owner, pid: 2147483647, processStartTime: "1" }));
    }
    // Its Main is gone too: a stale root participant whose recorded Main pid no longer exists.
    mainParticipant(Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS);
  };
  return { config, host, cli, create, registered, archives, mainParticipant, killResident, confirmed, close: async () => {
    if (!closed) { for (const actor of host.actors.listOwned()) await host.actors.stop(actor.id, undefined, true); await host.close(); }
    fs.rmSync(root, { recursive: true, force: true });
  } };
};

describe.skipIf(process.platform !== "linux")("fabric-actors remove on a dead root (smarty-dev#7817)", () => {
  const selfLease = (f: Awaited<ReturnType<typeof fixture>>, incarnationAgeMs: number) =>
    writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
      startedAt: Date.now() - incarnationAgeMs, updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
      writer: { pid: process.pid, host: os.hostname(), releaseSha: "test", lockProtocol: 1, stateBackend: "file", startedAt: meshProcessStartedAt } });
  const fresh = ROOT_PARTICIPANT_FRESH_MS;
  const participantFile = (f: Awaited<ReturnType<typeof fixture>>) =>
    path.join(f.config.meshRoot, "participants", createHash("sha256").update(f.config.rootId).digest("hex") + ".json");
  const expectUnchanged = (f: Awaited<ReturnType<typeof fixture>>, id: string) => {
    expect(f.registered(id)).toBe(true); expect(f.archives()).toEqual([]);
  };

  it.each(["live", "offline"] as const)("%s: no --main-stopped refuses; --main-stopped without evidence refuses", async mode => {
    const f = await fixture();
    try {
      const actor = await f.create("unasserted");
      if (mode === "offline") await f.killResident();
      const unflagged = await f.cli(actor.id, ["--confirm-dead-root", f.config.rootId]);
      expect(unflagged.code).toBe(1);
      expect(unflagged.err).toContain("the root's Main may be running; confirm it is stopped and pass --main-stopped (automatic proof: smarty-dev#7956)");
      const bare = await f.cli(actor.id, ["--confirm-dead-root", f.config.rootId, "--main-stopped"]);
      expect(bare.code).toBe(1); expect(bare.err).toContain("--main-stopped needs --evidence");
      const blank = await f.cli(actor.id, ["--confirm-dead-root", f.config.rootId, "--main-stopped", "--evidence", "  "]);
      expect(blank.code).toBe(1); expect(blank.err).toContain("--main-stopped needs --evidence");
      expectUnchanged(f, actor.id);
    } finally { await f.close(); }
  }, 40_000);

  it.each([
    ["a fresh root participant", fresh / 2, "Main is live: root participant is fresh; confirmation cannot override it"],
    ["a reloading root participant", "reloading", "Main identity cannot be verified: root participant is reloading; refusing (smarty-dev#7956)"],
    ["no root participant record (identity unavailable)", undefined, "no root participant record; automatic proof for roots whose records are gone: smarty-dev#7956"],
    ["a stale participant whose owner lease expired", 2 * fresh, undefined],
  ] as const)("case 1: --main-stopped under a resident-renewed lease with %s", async (_name, participant, refusal) => {
    const f = await fixture();
    try {
      const actor = await f.create("leftover");
      // The root lease, renewed by the resident host itself (this process), with no Main session.
      selfLease(f, 2 * fresh);
      if (participant === "reloading") writeParticipantFile(f.config.meshRoot, {
        key: "topology/participants/" + createHash("sha256").update(f.config.rootId).digest("hex"),
        value: { id: f.config.rootId, rootId: f.config.rootId, kind: "root", ownerHostId: f.config.rootId, status: "reloading",
          mainProcess: { pid: 2147483647, host: os.hostname(), startTime: "1" } },
        version: 1, updatedAt: Date.now() - 2 * fresh, updatedBy: { id: f.config.rootId, name: "main", kind: "main" } });
      else if (participant !== undefined) f.mainParticipant(Date.now() - participant);
      const result = await f.cli(actor.id);
      if (refusal) {
        expect(result.code).toBe(1); expect(result.err).toContain(refusal); expect(result.err).not.toContain("Main has a live root lease");
        expectUnchanged(f, actor.id);
        return;
      }
      expect(result).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(actor.id);
      expect(f.registered(actor.id)).toBe(false);
      // The audit record: operator, root id, time and evidence.
      const audit = f.archives().find(file => file.endsWith(`${actor.id}.operator.json`))!;
      const record = JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "archives", audit), "utf8"));
      expect(record).toMatchObject({ mainStopped: true, rootId: f.config.rootId, operatorAttestation: EVIDENCE,
        operator: { user: process.env.USER ?? null, pid: process.pid }, requestId: expect.any(String) });
      expect(Date.now() - Date.parse(record.assertedAt)).toBeLessThan(60_000);
      // The tool's own snapshot: root lease (the resident's heartbeat), each participant and its pid check.
      expect(record.toolEvidence).toMatchObject({ host: os.hostname(), capturedAt: expect.any(String),
        rootLease: { present: true, writer: { pid: process.pid, isResident: true } } });
      expect(record.toolEvidence.participants).toEqual(expect.arrayContaining([
        expect.objectContaining({ source: "file", ownerHostId: f.config.rootId, lastSeen: expect.any(Number), pid: 2147483647,
          host: os.hostname(), identity: "gone: Main pid 2147483647 does not exist on this host" })]));
      expect(f.archives().map(file => path.basename(file))).toEqual(expect.arrayContaining(["SHA256SUMS", `${actor.id}.registry.json`]));
    } finally { await f.close(); }
  }, 40_000);

  it("case 1: a resident-renewed lease without --main-stopped is a live Main for stop", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("stop-only");
      selfLease(f, 2 * fresh);
      let err = "";
      const code = await main(["stop", "--resident", f.config.residencyRoot, "--actor", actor.id, "--mesh-root", f.config.meshRoot,
        "--confirm-dead-root", f.config.rootId], { out: () => {}, err: text => { err += text; } });
      expect(code).toBe(1); expect(err).toContain("live root lease");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 40_000);

  it("case 1: a fresh root participant owned by the resident host id is Main evidence, not set aside", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("resident-owned");
      selfLease(f, 2 * fresh);
      f.mainParticipant(Date.now(), residentHostId(f.config.rootId));
      const result = await f.cli(actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("Main is live: root participant is fresh");
      expect(f.registered(actor.id)).toBe(true);
      expect(f.archives()).toEqual([]);
    } finally { await f.close(); }
  }, 40_000);

  it("live path: a symlink in the actor tree refuses before the stop; nothing changed", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("live-linked");
      f.mainParticipant(Date.now() - 2 * fresh);
      const actorDir = path.dirname(f.host.actors.status(actor.id).sessionFile!);
      fs.mkdirSync(actorDir, { recursive: true });
      fs.symlinkSync(path.join(f.config.meshRoot, "elsewhere"), path.join(actorDir, "escape"));
      const before = f.host.actors.status(actor.id).status;
      const result = await f.cli(actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("symlink");
      expect(f.host.actors.status(actor.id).status).toBe(before);
      expect(f.registered(actor.id)).toBe(true);
      expect(f.archives()).toEqual([]);
    } finally { await f.close(); }
  }, 40_000);

  it.each([
    ["alive with the recorded start time", "same", os.hostname(), "Main is live: Main process"],
    ["reused (a different start time)", "other", os.hostname(), undefined],
    ["recorded on a foreign host", "same", "other-host", "recorded on host other-host, not this host"],
    ["alive with no recorded start time", "none", os.hostname(), "exists and the record has no start time"],
    ["missing from the record", "nopid", os.hostname(), "no pid in the root participant record"],
  ] as const)("--main-stopped verifies the Main process identity: a pid %s", async (_name, start, host, refusal) => {
    const f = await fixture();
    const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60_000)"], { stdio: "ignore" });
    try {
      const actor = await f.create("identity");
      selfLease(f, 2 * fresh);
      await vi.waitFor(() => expect(processStartTime(child.pid!)).toBeDefined());
      const startTime = start === "same" ? processStartTime(child.pid!) : start === "other" ? "1" : undefined;
      f.mainParticipant(Date.now() - 2 * fresh, f.config.rootId, start === "nopid" ? { host } : { pid: child.pid!, host, ...(startTime ? { startTime } : {}) });
      const result = await f.cli(actor.id);
      if (refusal) {
        expect(result.code).toBe(1); expect(result.err).toContain(refusal);
        expectUnchanged(f, actor.id);
        return;
      }
      expect(result).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(actor.id);
      expect(f.registered(actor.id)).toBe(false);
    } finally { child.kill(); await f.close(); }
  }, 40_000);

  it.each(["live", "offline"] as const)("%s: an unknown actor prefix is reported before any liveness or identity check", async mode => {
    const f = await fixture();
    try {
      const actor = await f.create("prefix");
      if (mode === "offline") { await f.killResident(); fs.unlinkSync(participantFile(f)); }
      // No participant record: identity is unavailable, yet the input error comes first.
      const result = await f.cli(actor.id.slice(0, 8));
      expect(result.code).toBe(1); expect(result.err).toContain(`Unknown Fabric actor: ${actor.id.slice(0, 8)}`);
      const unverifiable = await f.cli(actor.id);
      expect(unverifiable.err).toContain("Main identity cannot be verified: Main process identity unavailable (no root participant record");
      expect(unverifiable.err).not.toContain("Main has a live root lease");
    } finally { await f.close(); }
  }, 40_000);

  it("offline: no root participant record with --main-stopped refuses (identity unavailable)", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("no-record");
      await f.killResident();
      fs.unlinkSync(participantFile(f));
      const result = await f.cli(actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("no root participant record");
      expectUnchanged(f, actor.id);
    } finally { await f.close(); }
  }, 40_000);

  it("offline: the resident startup claim is held for the whole removal; a busy claim refuses", async () => {
    const f = await fixture();
    try {
      const busy = await f.create("busy-claim"), actor = await f.create("claimed");
      for (const id of [busy.id, actor.id]) {
        fs.mkdirSync(path.join(f.config.actorRoot, id), { recursive: true });
        fs.writeFileSync(path.join(f.config.actorRoot, id, "session.jsonl"), "history\n");
      }
      await f.killResident();
      const claim = path.join(f.config.residencyRoot, "host-fence-establish.lock");
      // A resident host is starting: its startup claim is held. The removal refuses and changes nothing.
      const held = await fileLock.lockFile(claim, 0, true);
      try {
        const result = await f.cli(busy.id);
        expect(result.code).toBe(1); expect(result.err).toContain("startup claim is busy");
      } finally { fs.closeSync(held); }
      expectUnchanged(f, busy.id);
      // During the removal, a resident host start on this root is refused.
      const starts: unknown[] = [];
      offlineRemovalHooks.afterStep = async step => {
        if (step !== "revoke") return;
        const host = new ResidentHost(f.config, () => {});
        try { await host.start(); starts.push("started"); } catch (error) { starts.push(error); }
        finally { await host.close().catch(() => undefined); }
      };
      let result: Awaited<ReturnType<typeof f.cli>>;
      try { result = await f.cli(actor.id); } finally { delete offlineRemovalHooks.afterStep; }
      expect(result).toMatchObject({ code: 0, err: "" });
      expect(starts).toHaveLength(1);
      expect(starts[0]).toBeInstanceOf(ResidentHostAlreadyRunning);
      expect(f.registered(actor.id)).toBe(false);
    } finally { await f.close(); }
  }, 40_000);

  it("a removal fence refuses a root Main's participant publication; it publishes once the fence is gone", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-main-fence-"));
    const rootId = "session:fenced-main";
    const directory = mainDirectory(meshRoot, rootId);
    try {
      const release = takeMainPublicationFence(meshRoot, rootId);
      await directory.refresh().catch(() => undefined);
      expect(readParticipantFile(meshRoot, rootParticipantKey(rootId))).toBeUndefined();
      expect(directory.mesh.get(rootParticipantKey(rootId), { fresh: true })).toBeUndefined();
      release();
      await directory.refresh();
      expect(directory.mesh.get(rootParticipantKey(rootId), { fresh: true })?.value).toMatchObject({ id: rootId, kind: "root" });
    } finally { await directory.close(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
  }, 40_000);

  it.each(["live", "offline"] as const)("%s: a remove dry run against a live root reports the refusal and changes nothing", async mode => {
    const f = await fixture();
    try {
      const actor = await f.create("dry-live");
      if (mode === "offline") await f.killResident();
      f.mainParticipant(Date.now()); // the root's Main is live
      const before = fs.readdirSync(f.config.residencyRoot).sort();
      const dry = await f.cli(actor.id, [...f.confirmed, "--dry-run"]);
      expect(dry.code).toBe(1); expect(dry.err).toContain("would refuse: Main is live: root participant is fresh");
      expectUnchanged(f, actor.id);
      // Offline, nothing is written at all (the live host keeps its own request bookkeeping).
      if (mode === "offline") expect(fs.readdirSync(f.config.residencyRoot).sort()).toEqual(before);
      expect(f.host.actors.status(actor.id).status).not.toBe("stopped");
      expect(fs.existsSync(path.join(f.config.meshRoot, "main-publication-fences"))).toBe(false);
    } finally { await f.close(); }
  }, 40_000);

  it("a crash between the fence's temp write and its link leaves no fence, and the Main publishes", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-fence-crash-"));
    const rootId = "session:fence-crash";
    const link = vi.spyOn(fs, "linkSync").mockImplementationOnce(() => { throw Object.assign(new Error("crash"), { code: "EIO" }); });
    const directory = mainDirectory(meshRoot, rootId);
    try {
      expect(() => takeMainPublicationFence(meshRoot, rootId)).toThrow("crash");
      // A leftover temp file (a crash before its unlink) is never the fence.
      fs.writeFileSync(path.join(meshRoot, "main-publication-fences", "x.123.abc.tmp"), "{");
      expect(fs.readdirSync(path.join(meshRoot, "main-publication-fences"))).toEqual(["x.123.abc.tmp"]);
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(false);
      await directory.refresh();
      expect(directory.mesh.get(rootParticipantKey(rootId), { fresh: true })?.value).toMatchObject({ id: rootId, kind: "root" });
    } finally { link.mockRestore(); await directory.close(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
  }, 40_000);

  it.each(["refused", "seen"] as const)("live host remove: a Main publication during the cleanup is %s; the tree is never deleted under a fresh participant", async outcome => {
    const f = await fixture();
    try {
      const actor = await f.create("live-cleanup");
      selfLease(f, 2 * fresh);
      f.mainParticipant(Date.now() - 2 * fresh);
      const actorDir = path.dirname(f.host.actors.status(actor.id).sessionFile!);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      const stale = readParticipantFile(f.config.meshRoot, rootParticipantKey(f.config.rootId))!;
      const attempts: string[] = [];
      offlineRemovalHooks.beforeFinalCheck = async () => {
        if (!fs.existsSync(path.join(f.config.residencyRoot, "operator-removals", `${actor.id}.json`))) return;
        if (outcome === "refused") {
          // The root's Main restarts during the cleanup: under the fence its publication is refused.
          const directory = mainDirectory(f.config.meshRoot, f.config.rootId);
          try { await directory.refresh(); attempts.push("published"); } catch (error) { attempts.push(String(error)); }
          attempts.push(readParticipantFile(f.config.meshRoot, rootParticipantKey(f.config.rootId))?.updatedAt === stale.updatedAt &&
            directory.mesh.get(rootParticipantKey(f.config.rootId), { fresh: true }) === undefined ? "unpublished" : "fresh");
          await directory.close().catch(() => undefined);
        } else {
          // A publication that landed before the fence: the final check sees it and refuses the delete.
          f.mainParticipant(Date.now());
          attempts.push("fresh");
        }
      };
      let result: Awaited<ReturnType<typeof f.cli>>;
      try {
        result = await f.cli(actor.id);
        await f.host.actors.removalSettled(actor.id);
      } finally { delete offlineRemovalHooks.beforeFinalCheck; }
      expect(result.code).toBe(0);
      expect(f.registered(actor.id)).toBe(false); // revoked before the cleanup
      if (outcome === "refused") {
        expect(attempts.at(-1)).toBe("unpublished");
        expect(fs.existsSync(actorDir)).toBe(false);
      } else {
        // The fresh participant stops the delete; the cleanup obligation stays pending.
        expect(fs.existsSync(path.join(actorDir, "session.jsonl"))).toBe(true);
        expect(fs.existsSync(path.join(f.config.residencyRoot, "operator-removals", `${actor.id}.json`))).toBe(true);
      }
    } finally { await f.close(); }
  }, 40_000);

  it("an expired fence is stale on every host: a foreign-host fence past expiresAt is replaced", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-fence-expiry-"));
    const rootId = "session:fence-expiry";
    try {
      const file = path.join(meshRoot, "main-publication-fences", createHash("sha256").update(rootId).digest("hex") + ".json");
      fs.mkdirSync(path.dirname(file), { recursive: true });
      const foreign = { format: 1, pid: 1, host: "other-host", startTime: null, createdAt: new Date().toISOString() };
      fs.writeFileSync(file, JSON.stringify({ ...foreign, expiresAt: Date.now() + 60_000 }));
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(true);
      expect(() => takeMainPublicationFence(meshRoot, rootId)).toThrow("Another operator actor removal");
      fs.writeFileSync(file, JSON.stringify({ ...foreign, expiresAt: Date.now() - 1 }));
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(false);
      const release = takeMainPublicationFence(meshRoot, rootId);
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ pid: process.pid, host: os.hostname(),
        expiresAt: expect.any(Number) });
      expect(release.expiresAt - Date.now()).toBeGreaterThan(MAIN_PUBLICATION_FENCE_TTL_MS - 60_000);
      release();
      expect(fs.existsSync(file)).toBe(false);
    } finally { fs.rmSync(meshRoot, { recursive: true, force: true }); }
  });

  it("offline: a remover past its fence deadline refuses before deleting the tree", async () => {
    const f = await fixture();
    const now = Date.now;
    try {
      const actor = await f.create("deadline");
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      await f.killResident();
      offlineRemovalHooks.afterStep = step => {
        if (step === "bindings") Date.now = () => now() + MAIN_PUBLICATION_FENCE_TTL_MS + 1_000;
      };
      let result: Awaited<ReturnType<typeof f.cli>>;
      try { result = await f.cli(actor.id); } finally { Date.now = now; delete offlineRemovalHooks.afterStep; }
      expect(result.code).toBe(1);
      expect(JSON.parse(result.out)).toMatchObject({ cleaned: false, pending: expect.stringContaining("fence expired") });
      expect(fs.readFileSync(path.join(actorDir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(true);
    } finally { Date.now = now; await f.close(); }
  }, 40_000);

  it("offline: a Main publishing between a check and the tree delete is refused; the tree is never deleted under a fresh participant", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("main-races");
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      await f.killResident();
      const stale = readParticipantFile(f.config.meshRoot, rootParticipantKey(f.config.rootId))!;
      const outcomes: string[] = [];
      offlineRemovalHooks.afterStep = async step => {
        if (step !== "bindings") return;
        // Between the last check and the tree delete: the root's Main starts and publishes.
        const directory = mainDirectory(f.config.meshRoot, f.config.rootId);
        try { await directory.refresh(); outcomes.push("published"); } catch (error) { outcomes.push(String(error)); }
        finally { await directory.close().catch(() => undefined); }
        const current = readParticipantFile(f.config.meshRoot, rootParticipantKey(f.config.rootId));
        const state = directory.mesh.get(rootParticipantKey(f.config.rootId), { fresh: true });
        outcomes.push(current?.updatedAt === stale.updatedAt && state === undefined ? "unpublished" : "fresh participant visible");
      };
      let result: Awaited<ReturnType<typeof f.cli>>;
      try { result = await f.cli(actor.id); } finally { delete offlineRemovalHooks.afterStep; }
      // The Main's publication was refused, so no fresh participant existed when the tree went.
      expect(outcomes.at(-1)).toBe("unpublished");
      expect(result).toMatchObject({ code: 0, err: "" });
      expect(fs.existsSync(actorDir)).toBe(false);
      // The fence is released afterwards: the Main can publish again.
      const directory = mainDirectory(f.config.meshRoot, f.config.rootId);
      try {
        await directory.refresh();
        const published = [directory.mesh.get(rootParticipantKey(f.config.rootId), { fresh: true }),
          readParticipantFile(f.config.meshRoot, rootParticipantKey(f.config.rootId))].filter(entry => entry && entry.updatedAt > stale.updatedAt);
        expect(published.length).toBeGreaterThan(0);
      } finally { await directory.close(); }
    } finally { await f.close(); }
  }, 40_000);

  it("live path: the CLI wait holds no periodic timer and leaves nothing pending after settle", async () => {
    const f = await fixture();
    const interval = vi.spyOn(globalThis, "setInterval");
    const timeouts: Array<ReturnType<typeof setTimeout>> = [];
    const realSetTimeout = globalThis.setTimeout;
    const timeout = vi.spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
      const handle = realSetTimeout(...args); timeouts.push(handle); return handle;
    }) as typeof setTimeout);
    const cleared = vi.spyOn(globalThis, "clearTimeout");
    try {
      const actor = await f.create("timer");
      selfLease(f, 2 * fresh);
      f.mainParticipant(Date.now() - 2 * fresh);
      let result: Awaited<ReturnType<typeof f.cli>>;
      try { result = await f.cli(actor.id); } finally { timeout.mockRestore(); }
      expect(result).toMatchObject({ code: 0, err: "" });
      expect(interval).not.toHaveBeenCalled();
      // The one ref'd deadline timer is cleared when the wait settles.
      const keepAlive = timeouts.find(handle => typeof handle === "object" && (handle as NodeJS.Timeout).hasRef?.());
      expect(cleared.mock.calls.some(([handle]) => handle === keepAlive)).toBe(true);
    } finally { interval.mockRestore(); cleared.mockRestore(); await f.close(); }
  }, 40_000);

  it("case 1: a live root lease written by another process (a Main) is never set aside", async () => {
    const f = await fixture();
    try {
      const other = await f.create("kept");
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
        writer: { pid: 1, host: os.hostname(), releaseSha: "test", lockProtocol: 1, stateBackend: "file", startedAt: 1 } });
      expect((await f.cli(other.id)).err).toContain("live root lease");
      expect(f.registered(other.id)).toBe(true);
    } finally { await f.close(); }
  }, 40_000);

  it("case 2: --evidence-file is read as text, capped at 64 KiB, into the audit record", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("evidence-file");
      await f.killResident();
      const file = path.join(f.config.meshRoot, "evidence.txt");
      fs.writeFileSync(file, "x".repeat(70 * 1024));
      const result = await f.cli(actor.id, ["--confirm-dead-root", f.config.rootId, "--main-stopped", "--evidence-file", file]);
      expect(result).toMatchObject({ code: 0, err: "" });
      const archive = JSON.parse(result.out).archive;
      const record = JSON.parse(fs.readFileSync(path.join(archive, `${actor.id}.operator.json`), "utf8"));
      expect(record.operatorAttestation).toHaveLength(64 * 1024);
    } finally { await f.close(); }
  }, 40_000);

  it.each(["live", "offline"] as const)("%s: without provable file ownership (no getuid) removal refuses before any change", async mode => {
    const f = await fixture();
    const getuid = process.getuid;
    try {
      const actor = await f.create("no-owner-proof");
      f.mainParticipant(Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS);
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      if (mode === "offline") await f.killResident();
      const status = mode === "live" ? f.host.actors.status(actor.id).status : undefined;
      (process as { getuid?: unknown }).getuid = undefined;
      let result: Awaited<ReturnType<typeof f.cli>>;
      try { result = await f.cli(actor.id); } finally { (process as { getuid?: unknown }).getuid = getuid; }
      expect(result.code).toBe(1); expect(result.err).toContain("file ownership cannot be proven (smarty-dev#7858)");
      expect(f.registered(actor.id)).toBe(true); expect(f.archives()).toEqual([]);
      expect(fs.readFileSync(path.join(actorDir, "session.jsonl"), "utf8")).toBe("history\n");
      if (mode === "live") expect(f.host.actors.status(actor.id).status).toBe(status);
    } finally { (process as { getuid?: unknown }).getuid = getuid; await f.close(); }
  }, 40_000);

  it("case 2: a symlink in the actor tree refuses before the archive; nothing deleted, registry untouched", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("linked");
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      const outside = path.join(f.config.meshRoot, "outside.txt"); fs.writeFileSync(outside, "keep");
      fs.symlinkSync(outside, path.join(actorDir, "escape"));
      await f.killResident();
      const result = await f.cli(actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("symlink");
      expect(f.registered(actor.id)).toBe(true);
      expect(f.archives()).toEqual([]);
      expect(fs.readFileSync(path.join(actorDir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(fs.readFileSync(outside, "utf8")).toBe("keep");
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(false);
    } finally { await f.close(); }
  }, 40_000);

  it.each(["revoke", "bindings", "tree"] as const)("case 2: --main-stopped, then a Main participant appearing after the %s step aborts the next destructive step and keeps the marker", async step => {
    const f = await fixture();
    try {
      const actor = await f.create("racing");
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      await f.killResident();
      const bindings = vi.spyOn(ActorBindingStore.prototype, "delete");
      const presence = vi.spyOn(MeshStore.prototype, "delete");
      offlineRemovalHooks.afterStep = done => {
        if (done !== step) return;
        f.mainParticipant(Date.now());
      };
      let result: Awaited<ReturnType<typeof f.cli>>;
      try { result = await f.cli(actor.id); } finally { delete offlineRemovalHooks.afterStep; }
      expect(result.code).toBe(1);
      expect(JSON.parse(result.out)).toMatchObject({ cleaned: false, pending: expect.stringContaining("Main is live: root participant is fresh") });
      // The kept removal record carries the operator's audited assertion.
      const marker = JSON.parse(fs.readFileSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`), "utf8"));
      expect(marker.operatorAssertion).toMatchObject({ mainStopped: true, rootId: f.config.rootId, operatorAttestation: EVIDENCE,
        operator: { user: process.env.USER ?? null }, assertedAt: expect.any(String) });
      expect(bindings).toHaveBeenCalledTimes(step === "revoke" ? 0 : 1);
      expect(fs.existsSync(actorDir)).toBe(step !== "tree");
      expect(presence).not.toHaveBeenCalled();
      bindings.mockRestore(); presence.mockRestore();
    } finally { await f.close(); }
  }, 40_000);

  it("case 2: a file swapped between the walk and the removal stops the removal and keeps the marker", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("swapped");
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      await f.killResident();
      const withLock = ActorRegistryStore.prototype.withLock;
      const spy = vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function (this: ActorRegistryStore, ...args) {
        // After the walk and the archive, before the tree removal: replace a walked file.
        const replacement = path.join(actorDir, "replacement");
        fs.writeFileSync(replacement, "swapped\n"); fs.renameSync(replacement, path.join(actorDir, "session.jsonl"));
        return withLock.apply(this, args as Parameters<typeof withLock>);
      });
      let result: Awaited<ReturnType<typeof f.cli>>;
      try { result = await f.cli(actor.id); } finally { spy.mockRestore(); }
      expect(result.code).toBe(1);
      expect(JSON.parse(result.out)).toMatchObject({ cleaned: false, pending: expect.stringContaining("changed before removal") });
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(true);
      expect(fs.readFileSync(path.join(actorDir, "session.jsonl"), "utf8")).toBe("swapped\n");
      expect(f.archives().map(file => path.basename(file))).toEqual(expect.arrayContaining([`${actor.id}.tar`, "SHA256SUMS"]));
    } finally { await f.close(); }
  }, 40_000);

  it("case 2: removes offline under host.lock with a dead resident; refuses while host.lock is held or has a waiter", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("orphan");
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      await f.killResident();
      const lock = path.join(f.config.residencyRoot, "host.lock");

      // Held: a live holder of the kernel fence.
      const held = await fileLock.lockFile(lock, 0, true);
      try {
        const result = await f.cli(actor.id);
        expect(result.code).toBe(1); expect(result.err).toContain("host.lock is held");
      } finally { fs.closeSync(held); }
      expect(f.registered(actor.id)).toBe(true);

      // A waiter: a host blocks on host.lock right after our claim; it wins.
      const real = fileLock.lockFile;
      const spy = vi.spyOn(fileLock, "lockFile").mockImplementation(async (file, wait, parent) => {
        const fd = await real(file, wait, parent);
        if (file === lock) {
          spawn("flock", ["-x", lock, "true"], { stdio: "ignore" }).unref();
          const inode = fs.statSync(lock).ino;
          await vi.waitFor(() => expect(fs.readFileSync("/proc/locks", "utf8").split("\n")
            .some(line => line.includes("->") && line.includes(`:${inode} `))).toBe(true), { timeout: 5_000, interval: 20 });
        }
        return fd;
      });
      try {
        const result = await f.cli(actor.id);
        expect(result.code).toBe(1); expect(result.err).toContain("waiter");
      } finally { spy.mockRestore(); }
      expect(f.registered(actor.id)).toBe(true);
      expect(f.archives()).toEqual([]);
      // The waiter got and released the fence; it left the dead holder records unchanged.
      await vi.waitFor(() => expect(fs.readFileSync("/proc/locks", "utf8").includes(`:${fs.statSync(lock).ino} `)).toBe(false), { timeout: 5_000 });

      // A stale Main participant with no live lease does not block the asserted removal.
      f.mainParticipant(Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS);
      // A stopped root's dry run runs every check and shows the plan with the audit it would keep.
      const dry = await f.cli(actor.id, [...f.confirmed, "--dry-run"]);
      expect(dry).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(dry.out)).toMatchObject({ offline: true, dryRun: true, actor: { id: actor.id },
        plan: { archiveRoot: path.join(f.config.residencyRoot, "archives"),
          audit: { rootId: f.config.rootId, operatorAttestation: EVIDENCE, toolEvidence: { participants: expect.any(Array) } } } });
      expect(f.registered(actor.id)).toBe(true);

      const removed = await f.cli(actor.id);
      expect(removed).toMatchObject({ code: 0, err: "" });
      const output = JSON.parse(removed.out);
      expect(output).toMatchObject({ offline: true, cleaned: true, actor: { id: actor.id } });
      expect(f.registered(actor.id)).toBe(false);
      expect(fs.existsSync(actorDir)).toBe(false);
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(false);
      const archived = f.archives();
      expect(archived.map(file => path.basename(file)).sort()).toEqual(["SHA256SUMS", `${actor.id}.operator.json`, `${actor.id}.registry.json`, `${actor.id}.tar`].sort());
      expect(JSON.parse(fs.readFileSync(path.join(output.archive, `${actor.id}.operator.json`), "utf8")))
        .toMatchObject({ rootId: f.config.rootId, operatorAttestation: EVIDENCE, operator: { user: process.env.USER ?? null } });
      const tar = path.join(output.archive, `${actor.id}.tar`);
      expect(fs.readFileSync(path.join(output.archive, "SHA256SUMS"), "utf8")).toContain(createHash("sha256").update(fs.readFileSync(tar)).digest("hex"));
    } finally { await f.close(); }
  }, 40_000);
});

describe("ResidentActorClient wait is event-driven (smarty-dev#7817)", () => {
  const fakeResident = () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-client-wait-"));
    const rootId = "session:client-wait";
    const dir = residentRoot(meshRoot, rootId);
    fs.mkdirSync(dir, { recursive: true });
    const owner = path.join(dir, "owner.json");
    fs.writeFileSync(owner, JSON.stringify({ format: 1, hostId: residentHostId(rootId), pid: process.pid, token: "t", startedAt: 1, readyAt: 1,
      requestFence: 1, commands: ["operatorActor"] }));
    const request = async (): Promise<{ requestId: string; format: number }> => {
      for (;;) {
        const files = fs.existsSync(path.join(dir, "requests")) ? fs.readdirSync(path.join(dir, "requests")).filter(name => name.endsWith(".json")) : [];
        if (files.length) return JSON.parse(fs.readFileSync(path.join(dir, "requests", files[0]!), "utf8"));
        await new Promise(resolve => setImmediate(resolve));
      }
    };
    return { client: new ResidentActorClient(meshRoot, rootId), dir, owner, request,
      close: () => fs.rmSync(meshRoot, { recursive: true, force: true }) };
  };

  it("resolves on a response written after the call with no timer ticks", async () => {
    const r = fakeResident();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    try {
      const pending = r.client.operatorActor("stop", "actor", { dryRun: true });
      const { requestId } = await r.request();
      writeJsonAtomic(path.join(r.dir, "responses", `${requestId}.json`), { format: RESIDENT_HOST_FORMAT, requestId, ok: true, completedAt: Date.now() });
      await expect(pending).resolves.toMatchObject({ ok: true, requestId });
    } finally { vi.useRealTimers(); r.close(); }
  });

  it("detects a host exit through the watch", async () => {
    const r = fakeResident();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    try {
      const pending = r.client.operatorActor("stop", "actor", { dryRun: true });
      await r.request();
      fs.unlinkSync(r.owner);
      await expect(pending).rejects.toThrow(/exited during actor request|outcome/i);
    } finally { vi.useRealTimers(); r.close(); }
  });

  it("fails the request, undispatched, when fs.watch throws", async () => {
    const r = fakeResident();
    const watch = vi.spyOn(fs, "watch").mockImplementation(() => { throw Object.assign(new Error("watch unavailable"), { code: "ENOSPC" }); });
    try {
      await expect(r.client.operatorActor("stop", "actor", { dryRun: true })).rejects.toThrow("Cannot watch the resident host's responses");
      expect(fs.readdirSync(path.join(r.dir, "requests"))).toEqual([]);
    } finally { watch.mockRestore(); r.close(); }
  });
});
