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
import { MAIN_PUBLISH_GRACE_MS, mainAbsenceClock } from "../src/residency/operator-safety.js";
import * as fileLock from "../src/residency/file-lock.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// smarty-dev#7817: `fabric-actors remove --confirm-dead-root` for the two dead-root shapes.
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
  const cli = async (actor: string, flags: string[] = ["--confirm-dead-root", config.rootId]) => {
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
  const mainParticipant = (updatedAt: number, ownerHostId = config.rootId) => writeParticipantFile(meshRoot, {
    key: "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex"),
    value: { id: config.rootId, rootId: config.rootId, kind: "root", ownerHostId, ownerIdentityId: ownerHostId },
    version: 1, updatedAt, updatedBy: { id: config.rootId, name: "main", kind: "main" },
  });
  const killResident = async () => {
    // A crash leaves owner.json and host.lock behind; their pid no longer exists.
    const owner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8"));
    await host.close(); closed = true;
    for (const name of ["owner.json", "host.lock"]) {
      fs.writeFileSync(path.join(config.residencyRoot, name), JSON.stringify({ ...owner, pid: 2147483647, processStartTime: "1" }));
    }
  };
  const absenceFile = path.join(config.residencyRoot, "main-absence.json");
  /** The absence clock moves past the grace; the bound record itself is never rewritten. */
  const advanceClock = () => { mainAbsenceClock.now = () => Date.now() + MAIN_PUBLISH_GRACE_MS + 1_000; };
  /** First attempt refuses and records the absence; then the recorded absence is aged past the grace. */
  const establishAbsence = async (actor: string) => {
    const first = await cli(actor);
    expect(first.code).toBe(1); expect(first.err).toContain("absence not yet established");
    const record = JSON.parse(fs.readFileSync(absenceFile, "utf8"));
    expect(Date.now() - record.absentSince).toBeLessThan(60_000);
    advanceClock();
  };
  return { config, host, cli, create, registered, archives, mainParticipant, killResident, establishAbsence, advanceClock, absenceFile, close: async () => {
    mainAbsenceClock.now = () => Date.now();
    if (!closed) { for (const actor of host.actors.listOwned()) await host.actors.stop(actor.id, undefined, true); await host.close(); }
    fs.rmSync(root, { recursive: true, force: true });
  } };
};

describe.skipIf(process.platform !== "linux")("fabric-actors remove on a dead root (smarty-dev#7817)", () => {
  const selfLease = (f: Awaited<ReturnType<typeof fixture>>, incarnationAgeMs: number) =>
    writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
      startedAt: Date.now() - incarnationAgeMs, updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
      writer: { pid: process.pid, host: os.hostname(), releaseSha: "test", lockProtocol: 1, stateBackend: "file", startedAt: meshProcessStartedAt } });
  const grace = MAIN_PUBLISH_GRACE_MS;

  it.each([
    ["a participant record fresher than the grace", 2 * grace, grace / 2, "live: root participant published within the grace"],
    ["a stale participant whose owner lease expired", 2 * grace, 2 * grace, undefined],
  ] as const)("case 1: a resident-renewed lease with %s", async (_name, incarnationAgeMs, participantAgeMs, refusal) => {
    const f = await fixture();
    try {
      const actor = await f.create("leftover");
      // The root lease, renewed by the resident host itself (this process), with no Main session.
      selfLease(f, incarnationAgeMs);
      if (participantAgeMs !== undefined) f.mainParticipant(Date.now() - participantAgeMs);
      const result = await f.cli(actor.id);
      if (refusal) {
        expect(result.code).toBe(1); expect(result.err).toContain("live root lease"); expect(result.err).toContain(refusal);
        expect(f.registered(actor.id)).toBe(true);
        expect(f.archives()).toEqual([]);
        return;
      }
      expect(result).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(actor.id);
      expect(f.registered(actor.id)).toBe(false);
      expect(f.archives().map(file => path.basename(file))).toEqual(expect.arrayContaining(["SHA256SUMS", `${actor.id}.registry.json`]));
    } finally { await f.close(); }
  }, 40_000);

  it("case 1: no participant during a Main restart refuses until absence holds for the whole grace; a reappearing participant resets it", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("restarting");
      selfLease(f, 2 * grace); // the resident outlived the grace long ago; that alone proves nothing
      const absence = path.join(f.config.residencyRoot, "main-absence.json");
      const participantFile = path.join(f.config.meshRoot, "participants", createHash("sha256").update(f.config.rootId).digest("hex") + ".json");
      const refusedWith = async (text: string) => {
        const result = await f.cli(actor.id);
        expect(result.code).toBe(1); expect(result.err).toContain(text);
        expect(f.registered(actor.id)).toBe(true); expect(f.archives()).toEqual([]);
      };
      // First attempt: absence is only now observed, and recorded.
      await refusedWith("absence not yet established");
      const first = JSON.parse(fs.readFileSync(absence, "utf8"));
      expect(Date.now() - first.absentSince).toBeLessThan(60_000);
      // The restarted Main publishes: absence resets.
      f.mainParticipant(Date.now());
      await refusedWith("live: root participant published within the grace");
      expect(fs.existsSync(absence)).toBe(false);
      // It vanishes again: a new window starts, still short of the grace.
      fs.unlinkSync(participantFile);
      await refusedWith("absence not yet established");
      // A dry run reads only.
      const recorded = fs.readFileSync(absence, "utf8");
      expect((await f.cli(actor.id, ["--dry-run"])).code).toBe(0);
      expect(fs.readFileSync(absence, "utf8")).toBe(recorded);
      // A lease incarnation change restarts the window, even once the clock has passed the grace.
      f.advanceClock();
      selfLease(f, 3 * grace);
      await refusedWith("absence not yet established");
      expect(JSON.parse(fs.readFileSync(absence, "utf8")).absentSince).toBeGreaterThan(JSON.parse(recorded).absentSince);
      // Only an absence held for the full grace, same incarnation, proceeds.
      mainAbsenceClock.now = () => Date.now() + 2 * grace + 2_000;
      const removed = await f.cli(actor.id);
      expect(removed).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(actor.id);
      expect(f.registered(actor.id)).toBe(false);
    } finally { await f.close(); }
  }, 40_000);

  it("case 1: a fresh root participant owned by the resident host id is Main evidence, not set aside", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("resident-owned");
      selfLease(f, 2 * grace);
      f.mainParticipant(Date.now(), residentHostId(f.config.rootId));
      const result = await f.cli(actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("live: root participant published within the grace");
      expect(f.registered(actor.id)).toBe(true);
      expect(f.archives()).toEqual([]);
    } finally { await f.close(); }
  }, 40_000);

  it("live path: a symlink in the actor tree refuses before the stop; nothing changed", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("live-linked");
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

  it("case 2 offline: the absence record needs the full grace in the current generation; forged, future and stale records are fresh; a dry run seeing Main clears it", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("absent-offline");
      await f.killResident();
      const read = () => JSON.parse(fs.readFileSync(f.absenceFile, "utf8"));
      const refusesFresh = async () => {
        const result = await f.cli(actor.id);
        expect(result.code).toBe(1); expect(result.err).toContain("absence not yet established");
        expect(Date.now() - read().absentSince).toBeLessThan(60_000);
        expect(f.registered(actor.id)).toBe(true); expect(f.archives()).toEqual([]);
      };
      // First offline absence: no shortcut; it refuses and records.
      await refusesFresh();
      // Still inside the grace: refuses again with the same record.
      const since = read().absentSince;
      await refusesFresh(); expect(read().absentSince).toBe(since);
      const generation = read().generation;
      // A pre-seeded record of another generation is ignored and rewritten fresh.
      fs.writeFileSync(f.absenceFile, JSON.stringify({ format: 1, rootId: f.config.rootId, absentSince: 1, generation: "forged" }));
      await refusesFresh(); expect(read().generation).toBe(generation);
      // A record whose nonce is not the current one (rotated or forged) is fresh.
      fs.writeFileSync(f.absenceFile, JSON.stringify({ ...read(), absentSince: Date.now() - 2 * MAIN_PUBLISH_GRACE_MS, nonce: "f".repeat(64) }));
      await refusesFresh();
      // A future-dated record is fresh.
      fs.writeFileSync(f.absenceFile, JSON.stringify({ ...read(), absentSince: Date.now() + MAIN_PUBLISH_GRACE_MS }));
      await refusesFresh();
      // A record older than the current Main generation's start is fresh.
      fs.writeFileSync(path.join(f.config.residencyRoot, "main-generation.json"), JSON.stringify({ nonce: "n1", pid: 2147483647 }));
      await refusesFresh();
      const started = fs.statSync(path.join(f.config.residencyRoot, "main-generation.json")).mtimeMs;
      fs.writeFileSync(f.absenceFile, JSON.stringify({ ...read(), absentSince: started - 1_000 }));
      await refusesFresh();
      // A dry run that sees Main clears the record.
      f.mainParticipant(Date.now());
      expect((await f.cli(actor.id, ["--dry-run"])).code).toBe(0);
      expect(fs.existsSync(f.absenceFile)).toBe(false);
      expect(fs.existsSync(`${f.absenceFile}.nonce`)).toBe(false);
      fs.unlinkSync(path.join(f.config.meshRoot, "participants", createHash("sha256").update(f.config.rootId).digest("hex") + ".json"));
      // Only an absence held for the full grace in this generation proceeds (the generation began long ago).
      await f.establishAbsence(actor.id);
      expect(await f.cli(actor.id)).toMatchObject({ code: 0, err: "" });
      expect(f.registered(actor.id)).toBe(false);
    } finally { await f.close(); }
  }, 40_000);

  it("case 2 offline: a dead resident while a Main is starting (no participant yet) refuses, even with an old record", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("starting-main");
      await f.killResident();
      // The Main is starting: it has written its generation, not yet its participant record.
      fs.writeFileSync(f.absenceFile, JSON.stringify({ format: 1, rootId: f.config.rootId, absentSince: Date.now() - 2 * MAIN_PUBLISH_GRACE_MS, generation: "x", nonce: "n" }));
      fs.writeFileSync(path.join(f.config.residencyRoot, "main-generation.json"), JSON.stringify({ nonce: "starting", pid: process.pid }));
      const result = await f.cli(actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("absence not yet established");
      expect(f.registered(actor.id)).toBe(true); expect(f.archives()).toEqual([]);
    } finally { await f.close(); }
  }, 40_000);

  it.each(["live", "offline"] as const)("%s: without provable file ownership (no getuid) removal refuses before any change", async mode => {
    const f = await fixture();
    const getuid = process.getuid;
    try {
      const actor = await f.create("no-owner-proof");
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
      expect(fs.existsSync(f.absenceFile)).toBe(false);
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
      await f.establishAbsence(actor.id);
      const result = await f.cli(actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("symlink");
      expect(f.registered(actor.id)).toBe(true);
      expect(f.archives()).toEqual([]);
      expect(fs.readFileSync(path.join(actorDir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(fs.readFileSync(outside, "utf8")).toBe("keep");
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(false);
    } finally { await f.close(); }
  }, 40_000);

  it.each(["revoke", "bindings", "tree"] as const)("case 2: a Main going live after the %s step aborts the next destructive step and keeps the marker", async step => {
    const f = await fixture();
    try {
      const actor = await f.create("racing");
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      await f.killResident();
      await f.establishAbsence(actor.id);
      const bindings = vi.spyOn(ActorBindingStore.prototype, "delete");
      const presence = vi.spyOn(MeshStore.prototype, "delete");
      offlineRemovalHooks.afterStep = done => {
        if (done !== step) return;
        writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
          startedAt: Date.now(), updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      };
      let result: Awaited<ReturnType<typeof f.cli>>;
      try { result = await f.cli(actor.id); } finally { delete offlineRemovalHooks.afterStep; }
      expect(result.code).toBe(1);
      expect(JSON.parse(result.out)).toMatchObject({ cleaned: false, pending: expect.stringContaining("live root lease") });
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(true);
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
      await f.establishAbsence(actor.id);
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
      await f.establishAbsence(actor.id);
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

      const dry = await f.cli(actor.id, ["--dry-run"]);
      expect(dry).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(dry.out)).toMatchObject({ offline: true, dryRun: true, actor: { id: actor.id } });
      expect(f.registered(actor.id)).toBe(true);

      const removed = await f.cli(actor.id);
      expect(removed).toMatchObject({ code: 0, err: "" });
      const output = JSON.parse(removed.out);
      expect(output).toMatchObject({ offline: true, cleaned: true, actor: { id: actor.id } });
      expect(f.registered(actor.id)).toBe(false);
      expect(fs.existsSync(actorDir)).toBe(false);
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(false);
      const archived = f.archives();
      expect(archived.map(file => path.basename(file)).sort()).toEqual(["SHA256SUMS", `${actor.id}.registry.json`, `${actor.id}.tar`].sort());
      const tar = path.join(output.archive, `${actor.id}.tar`);
      expect(fs.readFileSync(path.join(output.archive, "SHA256SUMS"), "utf8")).toContain(createHash("sha256").update(fs.readFileSync(tar)).digest("hex"));
    } finally { await f.close(); }
  }, 40_000);
});
