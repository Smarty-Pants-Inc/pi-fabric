import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { assertResidentChannelOwned, main, resolveResidentDirectory, windowsOwnerSids } from "../src/actors-cli.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { residentProcessAlive } from "../src/residency/process-identity.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { hostLeasePath, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore } from "../src/mesh/store.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

beforeEach(() => installInProcessResidentFence());
const waitFor = (predicate: () => boolean) => vi.waitFor(() => expect(predicate()).toBe(true), { timeout: 15_000, interval: 30 });
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-operator-"));
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:dead-owner", sessionId: "dead-owner", cwd: process.cwd(), projectRoot: process.cwd(),
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:dead-owner"),
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
  const confirm = ["--confirm-dead-root", config.rootId];
  const cli = async (action: "stop" | "remove", actor: string, flags: string[] = [], resident = config.residencyRoot) => {
    let out = "", err = "";
    const code = await main([action, "--resident", resident, "--actor", actor, "--mesh-root", meshRoot, ...flags],
      { out: text => { out += text; }, err: text => { err += text; } });
    return { code, out, err };
  };
  const create = (name: string) => host.actors.create({ name, instructions: "Run", model: "fixture/visible",
    residency: "durable", transport: "process", extensions: false });
  return { root, host, config, cli, create, confirm, close: async () => {
    for (const actor of host.actors.listOwned()) await host.actors.stop(actor.id, undefined, true);
    await host.close(); fs.rmSync(root, { recursive: true, force: true });
  } };
};

// These are native-platform tests: no /proc census, platform mocks or skips.
describe("same-user resident actor operator", () => {
  it("confirmed CLI stops/drains only one actor then removes its registry and participant while the other keeps running", async () => {
    const f = await fixture();
    try {
      const victim = await f.create("victim"), survivor = await f.create("survivor");
      f.host.actors.tell(victim.id, "HANG_WITH_PROGRESS");
      f.host.actors.tell(survivor.id, "HANG_WITH_PROGRESS");
      await waitFor(() => !!f.host.actors.status(victim.id).inFlightRun && !!f.host.actors.status(survivor.id).inFlightRun);
      const runId = f.host.actors.status(victim.id).inFlightRun!.id;
      await waitFor(() => ("turns" in f.host.agents.status(runId) && Number((f.host.agents.status(runId) as { turns?: number }).turns) > 0));
      const runPid = Number(f.host.agents.status(runId).sessionId);
      await f.host.participants.refresh();
      expect(f.host.participants.get(victim.id, Date.now(), { fresh: true })).toBeDefined();

      const dry = await f.cli("stop", victim.name, ["--dry-run", ...f.confirm], path.basename(f.config.residencyRoot).slice(0, 12));
      expect(dry).toMatchObject({ code: 0, err: "" });
      expect(f.host.actors.status(victim.id).status).toBe("running");
      const stopped = await f.cli("stop", victim.name, f.confirm);
      expect(stopped).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(stopped.out).actor.status).toBe("stopped");
      expect(f.host.actors.status(victim.id).inFlightRun).toBeUndefined();
      expect(residentProcessAlive(runPid)).toBe(false);
      expect(f.host.actors.status(survivor.id).status).toBe("running");
      const removed = await f.cli("remove", victim.id, f.confirm);
      expect(removed).toMatchObject({ code: 0, err: "" });
      await waitFor(() => !new ActorRegistryStore(f.config.actorRoot).records().some(actor => actor.id === victim.id));
      await f.host.participants.refresh();
      expect(f.host.participants.get(victim.id, Date.now(), { fresh: true })).toBeUndefined();
      expect(f.host.participants.get(survivor.id, Date.now(), { fresh: true })).toBeDefined();
      expect(f.host.actors.status(survivor.id).status).toBe("running");
    } finally { await f.close(); }
  }, 40_000);

  it("remove an in-flight actor uses terminal drain, normal registry revocation and presence cleanup", async () => {
    const f = await fixture();
    try {
      const victim = await f.create("remove-running"), survivor = await f.create("other");
      f.host.actors.tell(victim.id, "HANG_WITH_PROGRESS");
      f.host.actors.tell(survivor.id, "HANG_WITH_PROGRESS");
      await waitFor(() => !!f.host.actors.status(victim.id).inFlightRun && !!f.host.actors.status(survivor.id).inFlightRun);
      const runId = f.host.actors.status(victim.id).inFlightRun!.id;
      await waitFor(() => ("turns" in f.host.agents.status(runId) && Number((f.host.agents.status(runId) as { turns?: number }).turns) > 0));
      const runPid = Number(f.host.agents.status(runId).sessionId);

      expect(await f.cli("remove", victim.name, f.confirm)).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(victim.id);
      await f.host.participants.refresh();
      expect(new ActorRegistryStore(f.config.actorRoot).records().some(actor => actor.id === victim.id)).toBe(false);
      expect(f.host.participants.get(victim.id, Date.now(), { fresh: true })).toBeUndefined();
      expect(residentProcessAlive(runPid)).toBe(false);
      expect(f.host.actors.status(survivor.id).status).toBe("running");
    } finally { await f.close(); }
  }, 40_000);

  it("a live file-only root lease vetoes confirmed control", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("leased");
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      const result = await f.cli("remove", actor.id, f.confirm);
      expect(result.code).toBe(1); expect(result.err).toContain("live root lease");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);

  it.each(["unreadable", "unparseable"] as const)("a cached expired lease cannot authorize stop/remove/dry-run when current data is %s", async fault => {
    const f = await fixture();
    const read = fs.readFileSync;
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const actor = await f.create("damaged-lease");
      const file = hostLeasePath(f.config.meshRoot, f.config.rootId);
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        updatedAt: 1, expiresAt: 2 });
      const expired = readHostLease(f.config.meshRoot, f.config.rootId);
      expect(expired?.expiresAt).toBe(2);
      // Replacement changes the inode; lstat/stat still succeed. A peer reader
      // may retain the old lease on EACCES, but operator authority must not.
      writeHostLease(f.config.meshRoot, { ...expired!, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      if (fault === "unreadable") spy = vi.spyOn(fs, "readFileSync").mockImplementation((target, options) => {
        if (target === file) throw Object.assign(new Error("current lease inaccessible"), { code: "EACCES" });
        return read(target, options as never);
      });
      else fs.writeFileSync(file, "{unparseable current lease");
      expect(fs.lstatSync(file).isFile()).toBe(true); expect(fs.statSync(file).isFile()).toBe(true);
      if (fault === "unreadable") expect(readHostLease(f.config.meshRoot, f.config.rootId)).toEqual(expired);
      for (const action of ["stop", "remove"] as const) for (const flags of [f.confirm, ["--dry-run"]]) {
        const refused = await f.cli(action, actor.id, flags);
        expect(refused.code).toBe(1); expect(refused.err).toContain("root lease is unreadable or invalid");
        expect(refused.err).toContain("--confirm-dead-root");
        expect(f.host.actors.status(actor.id).status).toBe("idle");
        expect(new ActorRegistryStore(f.config.actorRoot).records().some(row => row.id === actor.id)).toBe(true);
      }
    } finally { spy?.mockRestore(); await f.close(); }
  }, 30_000);

  it("live shared-state root presence is independently checked by the executor", async () => {
    const f = await fixture();
    const identity = { id: f.config.rootId, name: "Main", kind: "main" as const, sessionId: f.config.sessionId };
    const mainDirectory = new ParticipantDirectory(new MeshStore(f.config.meshRoot, 65536, 100), {
      enabled: true, hostId: f.config.rootId, rootId: f.config.rootId, identity,
    });
    mainDirectory.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
      runner: "pi", transport: "host", capabilities: ["fabric"], sessionId: f.config.sessionId, cwd: f.config.cwd,
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
    try {
      const actor = await f.create("shared-lease"); await mainDirectory.start();
      fs.rmSync(hostLeasePath(f.config.meshRoot, f.config.rootId), { force: true });
      const result = await f.cli("stop", actor.id, f.confirm);
      expect(result.code).toBe(1); expect(result.err).toContain("live root lease");
    } finally { await mainDirectory.close(); await f.close(); }
  }, 30_000);

  it("an orphan shared host lease also blocks control even without a participant", async () => {
    const f = await fixture();
    const identity = { id: f.config.rootId, name: "Main", kind: "main" as const, sessionId: f.config.sessionId };
    const directory = new ParticipantDirectory(new MeshStore(f.config.meshRoot, 65536, 100), {
      enabled: true, hostId: f.config.rootId, rootId: f.config.rootId, identity,
    });
    try {
      const actor = await f.create("bare-shared-lease"); await directory.start();
      fs.rmSync(hostLeasePath(f.config.meshRoot, f.config.rootId), { force: true });
      expect(f.host.participants.get(f.config.rootId, Date.now(), { fresh: true })).toBeUndefined();
      expect((await f.cli("remove", actor.id, f.confirm)).err).toContain("live root lease");
    } finally { await directory.close(); await f.close(); }
  }, 30_000);

  it.each(["directory", "symlink"] as const)("a same-name CWD %s never redirects packaged stop/remove to another valid resident", async kind => {
    const f = await fixture();
    let other: Awaited<ReturnType<typeof fixture>> | undefined;
    const run = promisify(execFile);
    try {
      other = await fixture();
      const target = await f.create("same-name"), wrongRoot = await other.create("same-name");

      const selector = path.basename(f.config.residencyRoot);
      let cwd = path.dirname(other.config.residencyRoot);
      if (kind === "symlink") {
        cwd = path.join(f.root, "cwd"); fs.mkdirSync(cwd);
        fs.symlinkSync(other.config.residencyRoot, path.join(cwd, selector), "dir");
      }
      for (const action of ["stop", "remove"] as const) {
        const result = await run(process.execPath, [path.resolve("bin/fabric-actors"), action,
          "--resident", selector, "--actor", target.name, ...f.confirm], {
          cwd, env: { ...process.env, PI_FABRIC_MESH_ROOT: f.config.meshRoot },
        });
        expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, action, resident: f.config.residencyRoot });
        expect(other.host.actors.status(wrongRoot.id).status).toBe("idle");
        expect(new ActorRegistryStore(other.config.actorRoot).records().some(row => row.id === wrongRoot.id)).toBe(true);
        if (action === "stop") expect(f.host.actors.status(target.id).status).toBe("stopped");
        else expect(new ActorRegistryStore(f.config.actorRoot).records().some(row => row.id === target.id)).toBe(false);
      }
      const ambiguous = path.join(f.config.meshRoot, "residency", selector + "0");
      fs.mkdirSync(ambiguous);
      await expect(run(process.execPath, [path.resolve("bin/fabric-actors"), "remove",
        "--resident", selector, "--actor", target.name, ...f.confirm], {
        cwd, env: { ...process.env, PI_FABRIC_MESH_ROOT: f.config.meshRoot },
      })).rejects.toMatchObject({ code: 1, stderr: expect.stringContaining("Ambiguous resident prefix") });
      expect(other.host.actors.status(wrongRoot.id).status).toBe("idle");
    } finally { if (other) await other.close(); await f.close(); }
  }, 40_000);

  it.each(["stop", "remove"] as const)("%s requires missing/mismatched/matching exact root confirmation", async action => {
    const f = await fixture();
    try {
      const actor = await f.create("confirm-target");
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        updatedAt: 1, expiresAt: 2 });
      // A dry run runs every check: without confirmation it refuses as the real run would.
      expect(await f.cli(action, actor.id, ["--dry-run"])).toMatchObject({ code: 1, err: expect.stringContaining("Missing --confirm-dead-root") });
      const dry = await f.cli(action, actor.id, ["--dry-run", ...f.confirm]);
      expect(dry).toMatchObject({ code: 0, err: "" });
      const evidence = JSON.parse(dry.out).operatorEvidence;
      expect(evidence).toMatchObject({ rootId: f.config.rootId, mainSessionId: f.config.sessionId,
        lastLeaseTime: 1, leaseExpiresAt: 2, liveLease: false, operatorCheck: expect.stringContaining("herdr agent list / ps") });
      for (const flags of [[], ["--confirm-dead-root", "session:wrong"], ["--confirm-dead-root", f.config.rootId + " "]]) {
        const refused = await f.cli(action, actor.id, flags);
        expect(refused.code).toBe(1);
        expect(refused.err).toContain(flags.length ? "Mismatched --confirm-dead-root" : "Missing --confirm-dead-root");
        expect(refused.err).toContain(JSON.stringify(evidence));
        expect(f.host.actors.status(actor.id).status).toBe("idle");
      }
      expect(await f.cli(action, actor.id, ["--dry-run", "--confirm-dead-root", "session:wrong"]))
        .toMatchObject({ code: 1, err: expect.stringContaining("Mismatched --confirm-dead-root") });
      expect(await f.cli(action, actor.name, f.confirm)).toMatchObject({ code: 0, err: "" });
      if (action === "stop") expect(f.host.actors.status(actor.id).status).toBe("stopped");
      else expect(new ActorRegistryStore(f.config.actorRoot).records().some(row => row.id === actor.id)).toBe(false);
    } finally { await f.close(); }
  }, 30_000);

  it.each(["stop", "remove"] as const)("%s refuses an unexpired lease (a live Main) even when confirmed; a live-path dry run refuses it too", async action => {
    const f = await fixture();
    try {
      const actor = await f.create("live-lease");
      const updatedAt = Date.now(), expiresAt = updatedAt + 60_000;
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId, updatedAt, expiresAt });
      expect(await f.cli(action, actor.id, f.confirm)).toMatchObject({ code: 1, err: expect.stringContaining("live root lease") });
      // The resident host's dry run runs every check: a live Main refuses, reporting the same evidence.
      const dry = await f.cli(action, actor.id, ["--dry-run", ...f.confirm]);
      expect(dry.code).toBe(1); expect(dry.err).toContain("Main has a live root lease");
      expect(dry.err).toContain(`"lastLeaseTime":${updatedAt},"leaseExpiresAt":${expiresAt},"liveLease":true`);
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);

  it.each(["stop", "remove"] as const)("%s rechecks the current lease immediately before actor commit", async action => {
    const f = await fixture();
    try {
      const actor = await f.create("lease-race"), stop = f.host.actors.stop.bind(f.host.actors);
      vi.spyOn(f.host.actors, "stop").mockImplementationOnce((...args) => {
        writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
          updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
        return stop(...args);
      });
      expect(await f.cli(action, actor.id, f.confirm)).toMatchObject({ code: 1, err: expect.stringContaining("live root lease") });
      expect(f.host.actors.status(actor.id).status).toBe("idle");
      expect(new ActorRegistryStore(f.config.actorRoot).records().some(row => row.id === actor.id)).toBe(true);
    } finally { await f.close(); }
  }, 30_000);

  it("symlinked resident request channels refuse before dispatch", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("channel-symlink"), requests = path.join(f.config.residencyRoot, "requests");
      fs.renameSync(requests, requests + "-real");
      fs.symlinkSync(requests + "-real", requests, "dir");
      for (const action of ["stop", "remove"] as const) {
        expect(await f.cli(action, actor.id, f.confirm)).toMatchObject({ code: 1, err: expect.stringContaining("not owned by this OS user") });
      }
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);

  it("unknown actor, invalid wire flags, removed override and old hosts fail cleanly", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("known");
      expect(await f.cli("remove", actor.id.slice(0, 8), f.confirm)).toMatchObject({ code: 1, err: expect.stringContaining("Unknown Fabric actor") });
      expect(await f.cli("stop", actor.id, ["--force-live"])).toMatchObject({ code: 1, err: expect.stringContaining("Usage:") });
      const client = new ResidentActorClient(f.config.meshRoot, f.config.rootId);
      await expect(client.operatorActor("remove", actor.id, { confirmDeadRoot: 1 as unknown as string })).rejects.toThrow("Invalid resident operator");
      await expect(client.operatorActor("stop", actor.id)).rejects.toThrow("Missing --confirm-dead-root");
      await expect(client.operatorActor("remove", actor.id, { confirmDeadRoot: "session:wrong" })).rejects.toThrow("Mismatched --confirm-dead-root");
      const ownerPath = path.join(f.config.residencyRoot, "owner.json"), owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
      fs.writeFileSync(ownerPath, JSON.stringify({ ...owner, commands: owner.commands.filter((op: string) => op !== "operatorActor") }));
      await expect(client.operatorActor("remove", actor.id, { confirmDeadRoot: f.config.rootId })).rejects.toThrow("older release");
      expect(fs.readdirSync(path.join(f.config.residencyRoot, "requests"))).toHaveLength(0);
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);

  it("packaged bin requires confirmation, supports dry-run and normal removal, and exits cleanly for unknown ids", async () => {
    const f = await fixture(), run = promisify(execFile);
    const argv = (action: string, id: string) => [path.resolve("bin/fabric-actors"), action,
      "--resident", f.config.residencyRoot, "--actor", id, "--mesh-root", f.config.meshRoot];
    try {
      const actor = await f.create("bin-target");
      await expect(run(process.execPath, argv("remove", actor.id))).rejects.toMatchObject({ code: 1, stderr: expect.stringContaining("Missing --confirm-dead-root") });
      const dry = await run(process.execPath, [...argv("remove", actor.name), "--dry-run", ...f.confirm]);
      expect(JSON.parse(dry.stdout).operatorEvidence).toMatchObject({ rootId: f.config.rootId, mainSessionId: f.config.sessionId, lastLeaseTime: null });
      const result = await run(process.execPath, [...argv("remove", actor.name), ...f.confirm]);
      expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, action: "remove", resident: f.config.residencyRoot });
      await f.host.participants.refresh();
      expect(f.host.participants.get(actor.id, Date.now(), { fresh: true })).toBeUndefined();
      await expect(run(process.execPath, [...argv("stop", "unknown-id"), ...f.confirm])).rejects.toMatchObject({ code: 1, stderr: expect.stringContaining("Unknown Fabric actor") });
    } finally { await f.close(); }
  }, 30_000);
});

describe("resident channel owner check", () => {
  const userSid = "S-1-5-21-1-2-3-1001", otherSid = "S-1-5-21-1-2-3-1002";
  const channel = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-owner-"));
    const files = [root, path.join(root, "config.json"), path.join(root, "requests")];
    fs.writeFileSync(files[1]!, "{}"); fs.mkdirSync(files[2]!);
    return { root, files, close: () => fs.rmSync(root, { recursive: true, force: true }) };
  };

  it("win32 fails closed when the owner SID query is unavailable, even with a matching uid", () => {
    const c = channel();
    try {
      const uid = () => fs.statSync(c.root).uid;
      for (const probe of [{ platform: "win32" as const, getuid: uid },
        { platform: "win32" as const, getuid: uid, windowsOwnerSids: () => { throw new Error("spawn powershell.exe ENOENT"); } },
        // The real query on a host without powershell.exe must refuse too.
        ...(process.platform === "win32" ? [] : [{ platform: "win32" as const, getuid: uid, windowsOwnerSids }])]) {
        expect(() => assertResidentChannelOwned(c.files, probe)).toThrow(/Cannot verify that the resident channel is owned by this Windows user.*refusing stop\/remove/);
      }
    } finally { c.close(); }
  });

  it("win32 refuses a foreign or malformed owner SID and accepts only all-matching SIDs", () => {
    const c = channel();
    try {
      const win = (owners: string[], user = userSid) => ({ platform: "win32" as const, windowsOwnerSids: () => ({ owners, user }) });
      expect(() => assertResidentChannelOwned(c.files, win([userSid, otherSid, userSid]))).toThrow(`not owned by this OS user: ${c.files[1]}`);
      expect(() => assertResidentChannelOwned(c.files, win([userSid, "BUILTIN\\Administrators", userSid]))).toThrow("not owned by this OS user");
      expect(() => assertResidentChannelOwned(c.files, win([userSid, userSid]))).toThrow("malformed SID query result");
      expect(() => assertResidentChannelOwned(c.files, win(["", "", ""], ""))).toThrow("malformed SID query result");
      expect(() => assertResidentChannelOwned(c.files, win([userSid, userSid, userSid]))).not.toThrow();
    } finally { c.close(); }
  });

  it("posix behavior is unchanged: uid match passes, mismatch and symlinks refuse; a missing uid fails closed", () => {
    const c = channel();
    try {
      const uid = fs.statSync(c.root).uid;
      expect(() => assertResidentChannelOwned(c.files, { platform: "linux", getuid: () => uid })).not.toThrow();
      expect(() => assertResidentChannelOwned(c.files, { platform: "darwin", getuid: () => uid + 1 })).toThrow(`not owned by this OS user: ${c.root}`);
      expect(() => assertResidentChannelOwned(c.files, { platform: "linux" })).toThrow("no uid available");
      if (process.platform !== "win32") {
        expect(() => assertResidentChannelOwned(c.files)).not.toThrow();
        const link = path.join(c.root, "link"); fs.symlinkSync(c.files[2]!, link, "dir");
        expect(() => assertResidentChannelOwned([link], { platform: "linux", getuid: () => uid })).toThrow("not owned by this OS user");
      }
    } finally { c.close(); }
  });

  it("simulated win32 stop/remove refuse before dispatch when ownership cannot be proven", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("win32-target");
      for (const action of ["stop", "remove"] as const) {
        for (const flags of [f.confirm, ["--dry-run"]]) {
          let err = "";
          const code = await main([action, "--resident", f.config.residencyRoot, "--actor", actor.id, "--mesh-root", f.config.meshRoot, ...flags],
            { out: () => {}, err: text => { err += text; } },
            { platform: "win32", getuid: () => fs.statSync(f.config.residencyRoot).uid, windowsOwnerSids: () => { throw new Error("Get-Acl failed"); } });
          expect(code).toBe(1);
          expect(err).toContain("Cannot verify that the resident channel is owned by this Windows user (Get-Acl failed); refusing stop/remove");
        }
      }
      expect(fs.readdirSync(path.join(f.config.residencyRoot, "requests"))).toHaveLength(0);
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);
});

describe("resident selector boundary", () => {
  it.each(["directory", "symlink"] as const)("bare prefixes ignore a same-name CWD %s and fail closed on ambiguity or no match", kind => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-selector-"));
    const meshRoot = path.join(root, "mesh"), parent = path.join(meshRoot, "residency");
    const cwd = path.join(root, "cwd"), outside = path.join(root, "outside"), prefix = "ab";
    const resident = path.join(parent, prefix + "0".repeat(62));
    fs.mkdirSync(resident, { recursive: true }); fs.mkdirSync(cwd); fs.mkdirSync(outside);
    if (kind === "directory") fs.mkdirSync(path.join(cwd, prefix));
    else fs.symlinkSync(outside, path.join(cwd, prefix), "dir");
    const spy = vi.spyOn(process, "cwd").mockReturnValue(cwd);
    try {
      expect(resolveResidentDirectory(prefix, meshRoot)).toBe(fs.realpathSync(resident));
      const second = path.join(parent, prefix + "1".repeat(62)); fs.mkdirSync(second);
      expect(() => resolveResidentDirectory(prefix, meshRoot)).toThrow("Ambiguous resident prefix");
      fs.rmSync(second, { recursive: true }); fs.rmSync(resident, { recursive: true });
      expect(() => resolveResidentDirectory(prefix, meshRoot)).toThrow("Unknown resident");
      expect(() => resolveResidentDirectory("not-hex", meshRoot)).toThrow("must be hexadecimal");
    } finally { spy.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("explicit paths accept only non-symlink directories whose realpath is a direct residency child", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-selector-"));
    const meshRoot = path.join(root, "mesh"), parent = path.join(meshRoot, "residency");
    const resident = path.join(parent, "ab".repeat(32)), outside = path.join(root, "outside");
    fs.mkdirSync(resident, { recursive: true }); fs.mkdirSync(outside);
    const nested = path.join(resident, "nested"); fs.mkdirSync(nested);
    const link = path.join(parent, "link"); fs.symlinkSync(resident, link, "dir");
    const outsideLink = path.join(root, "outside-link"); fs.symlinkSync(resident, outsideLink, "dir");
    const file = path.join(parent, "file"); fs.writeFileSync(file, "not a directory");
    const spy = vi.spyOn(process, "cwd").mockReturnValue(root);
    try {
      expect(resolveResidentDirectory(resident, meshRoot)).toBe(fs.realpathSync(resident));
      expect(resolveResidentDirectory(path.join(".", "mesh", "residency", path.basename(resident)), meshRoot)).toBe(fs.realpathSync(resident));
      for (const selector of [outside, nested, `.${path.sep}outside`]) {
        expect(() => resolveResidentDirectory(selector, meshRoot)).toThrow("direct child of the configured residency directory");
      }
      for (const selector of [link, link + path.sep, outsideLink]) {
        expect(() => resolveResidentDirectory(selector, meshRoot)).toThrow("must not be a symlink");
      }
      expect(() => resolveResidentDirectory(file, meshRoot)).toThrow("not a directory");
    } finally { spy.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("stop/remove refuse another valid resident's explicit path or a selector symlink before sending a request", async () => {
    const f = await fixture();
    let other: Awaited<ReturnType<typeof fixture>> | undefined;
    try {
      other = await fixture();
      const actor = await other.create("wrong-root");
      const link = path.join(f.config.meshRoot, "residency", "link");
      fs.symlinkSync(other.config.residencyRoot, link, "dir");
      const validLink = path.join(f.config.meshRoot, "residency", "valid-link");
      fs.symlinkSync(f.config.residencyRoot, validLink, "dir");
      for (const action of ["stop", "remove"] as const) {
        const outside = await f.cli(action, actor.name, [], other.config.residencyRoot);
        expect(outside.code).toBe(1); expect(outside.err).toContain("direct child of the configured residency directory");
        for (const selector of [link, validLink]) {
          const symlink = await f.cli(action, actor.name, [], selector);
          expect(symlink.code).toBe(1); expect(symlink.err).toContain("must not be a symlink");
        }
        expect(other.host.actors.status(actor.id).status).toBe("idle");
        expect(fs.readdirSync(path.join(other.config.residencyRoot, "requests"))).toHaveLength(0);
      }
    } finally { if (other) await other.close(); await f.close(); }
  }, 30_000);
});
