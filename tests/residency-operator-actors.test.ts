import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile, spawn, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import { once } from "node:events";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { main, resolveResidentDirectory } from "../src/actors-cli.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { mainGenerationPath } from "../src/residency/handover.js";
import { mainMarkerPath } from "../src/residency/main-marker.js";
import * as mainPublication from "../src/residency/main-publication-fence.js";
import { processStartTime, residentProcessAlive } from "../src/residency/process-identity.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { hostLeasePath, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore } from "../src/mesh/store.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// Census regressions use this test's real process tree, not unrelated fleet
// Mains (or sibling Vitest workers). Production still scans all same-user PIDs.
beforeEach(() => {
  installInProcessResidentFence();
  if (process.platform !== "linux") return;
  const readdir = fs.readdirSync.bind(fs);
  vi.spyOn(fs, "readdirSync").mockImplementation(((directory: fs.PathLike, options?: never) => {
    const entries = readdir(directory, options);
    if (directory !== "/proc") return entries;
    const parents = new Map<string, string>();
    for (const name of entries as unknown as string[]) {
      if (!/^\d+$/.test(name)) continue;
      try {
        const stat = fs.readFileSync(`/proc/${name}/stat`, "utf8");
        parents.set(name, stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[1]!);
      } catch { /* A vanished process is not part of the test census. */ }
    }
    const ours = (name: string): boolean => {
      const visited = new Set<string>();
      while (!visited.has(name)) {
        if (name === String(process.pid)) return true;
        visited.add(name);
        const parent = parents.get(name);
        if (!parent) return false;
        name = parent;
      }
      return false;
    };
    return (entries as unknown as string[]).filter(ours);
  }) as typeof fs.readdirSync);
});
const waitFor = (predicate: () => boolean) => vi.waitFor(() => expect(predicate()).toBe(true), { timeout: 15_000, interval: 30 });
const stopChild = async (child: ChildProcess) => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const done = once(child, "exit"); child.kill("SIGTERM"); await done;
};
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
  const child = spawn(process.execPath, ["-e", "console.log('ready');setInterval(()=>{},1000)"], { stdio: ["ignore", "pipe", "pipe"] });
  await once(child.stdout!, "data");
  fs.writeFileSync(mainGenerationPath(config.residencyRoot), JSON.stringify({ rootId: config.rootId,
    sessionId: config.sessionId, pid: child.pid, processStartTime: processStartTime(child.pid!), nonce: "test", releaseRoot: process.cwd() }));
  const host = new ResidentHost(config, () => {});
  try { await host.start(); } catch (error) { await stopChild(child); await host.close(); throw error; }
  const cli = async (action: "stop" | "remove", actor: string, flags: string[] = [], resident = config.residencyRoot) => {
    let out = "", err = "";
    const code = await main([action, "--resident", resident, "--actor", actor, "--mesh-root", meshRoot, ...flags],
      { out: text => { out += text; }, err: text => { err += text; } });
    return { code, out, err };
  };
  const create = (name: string) => host.actors.create({ name, instructions: "Run", model: "fixture/visible",
    residency: "durable", transport: "process", extensions: false });
  return { root, host, config, child, cli, create, close: async () => {
    await stopChild(child);
    for (const actor of host.actors.listOwned()) await host.actors.stop(actor.id, undefined, true);
    await host.close(); fs.rmSync(root, { recursive: true, force: true });
  } };
};

describe("non-Linux resident actor operator", () => {
  it.each(["win32", "darwin"] as const)("%s refuses without /proc evidence but permits explicit force-live", async platform => {
    const f = await fixture();
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const actor = await f.create("platform-control");
      await stopChild(f.child);
      spy = vi.spyOn(process, "platform", "get").mockReturnValue(platform);
      for (const action of ["stop", "remove"] as const) {
        const result = await f.cli(action, actor.id);
        expect(result.code).toBe(1);
        expect(result.err).toContain("operator dead-root control needs Linux /proc evidence; pass --force-live after confirming");
        expect(f.host.actors.status(actor.id).status).toBe("idle");
      }
      expect((await f.cli("stop", actor.id, ["--force-live"])).code).toBe(0);
      expect((await f.cli("remove", actor.id, ["--force-live"])).code).toBe(0);
    } finally { spy?.mockRestore(); await f.close(); }
  }, 30_000);
});

describe.skipIf(process.platform !== "linux")("same-user resident actor operator", () => {
  it.each(["target", "other", "reused PID"] as const)("unbound Pi Main with a %s marker", async binding => {
    const f = await fixture();
    let replacement: ChildProcess | undefined;
    try {
      const actor = await f.create("marked-main"); await stopChild(f.child);
      const cli = path.join(f.root, "pi-runtime", "cli.js");
      fs.mkdirSync(path.dirname(cli), { recursive: true });
      fs.writeFileSync(cli, "process.title='pi';console.log('ready');setInterval(()=>{},1000)");
      const env: NodeJS.ProcessEnv = { ...process.env };
      for (const key of ["PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_SESSION_ID", "PI_SESSION_ID", "PI_FABRIC_ROLE_SESSION",
        "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_RESIDENT_CONFIG"]) delete env[key];
      replacement = spawn(process.execPath, [cli], { env, stdio: ["ignore", "pipe", "pipe"] });
      await once(replacement.stdout!, "data");
      const pid = replacement.pid!, birth = processStartTime(pid)!;
      const startTime = binding === "reused PID" ? String(BigInt(birth) - 1n) : birth;
      const file = mainMarkerPath(f.config.meshRoot, pid, startTime);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, JSON.stringify({ pid, startTime,
        rootId: binding === "target" ? f.config.rootId : "session:elsewhere",
        sessionId: binding === "target" ? f.config.sessionId : "elsewhere", createdAt: Date.now() }));
      for (const action of ["stop", "remove"] as const) {
        const result = await f.cli(action, actor.id, ["--dry-run"]);
        if (binding === "other") expect(result).toMatchObject({ code: 0, err: "" });
        else {
          expect(result.code).toBe(1);
          expect(result.err).toContain(binding === "target" ? `Fabric marker for PID ${pid}` : `PID ${pid}: Main without a Fabric marker`);
        }
      }
      if (binding !== "other") await stopChild(replacement);
      // Left-behind markers (including a mismatched birth) do not veto a dead process.
      expect((await f.cli("stop", actor.id)).code).toBe(0);
    } finally { if (replacement) await stopChild(replacement); await f.close(); }
  }, 30_000);


  it("after Main exits, CLI stops/drains only one actor then removes its registry and participant while the other keeps running", async () => {
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
      await stopChild(f.child);
      const dry = await f.cli("stop", victim.name, ["--dry-run"], path.basename(f.config.residencyRoot).slice(0, 12));
      expect(dry).toMatchObject({ code: 0, err: "" });
      expect(f.host.actors.status(victim.id).status).toBe("running");
      const stopped = await f.cli("stop", victim.name);
      expect(stopped).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(stopped.out).actor.status).toBe("stopped");
      expect(f.host.actors.status(victim.id).inFlightRun).toBeUndefined();
      expect(residentProcessAlive(runPid)).toBe(false);
      expect(f.host.actors.status(survivor.id).status).toBe("running");
      const removed = await f.cli("remove", victim.id);
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
      await stopChild(f.child);
      expect(await f.cli("remove", victim.name)).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(victim.id);
      await f.host.participants.refresh();
      expect(new ActorRegistryStore(f.config.actorRoot).records().some(actor => actor.id === victim.id)).toBe(false);
      expect(f.host.participants.get(victim.id, Date.now(), { fresh: true })).toBeUndefined();
      expect(residentProcessAlive(runPid)).toBe(false);
      expect(f.host.actors.status(survivor.id).status).toBe("running");
    } finally { await f.close(); }
  }, 40_000);

  it("refuses a live Main even without a lease; force-live is explicit and dry-run is non-mutating", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("live-root");
      for (const action of ["stop", "remove"] as const) {
        const refused = await f.cli(action, actor.id);
        expect(refused.code).toBe(1); expect(refused.err).toContain("Main process is still alive");
      }
      expect((await f.cli("remove", actor.id, ["--force-live", "--dry-run"])).code).toBe(0);
      expect(f.host.actors.status(actor.id).status).toBe("idle");
      expect((await f.cli("stop", actor.id, ["--force-live"])).code).toBe(0);
      expect(f.host.actors.status(actor.id).status).toBe("stopped");
      expect((await f.cli("remove", actor.id, ["--force-live"])).code).toBe(0);
    } finally { await f.close(); }
  }, 30_000);

  it("malformed recorded start times refuse stop/remove for a live PID, whether generation or inbox is the only identity", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("malformed-start");
      const generationFile = mainGenerationPath(f.config.residencyRoot);
      const generation = JSON.parse(fs.readFileSync(generationFile, "utf8"));
      const inbox = path.join(f.config.meshRoot, "main-followups", `${f.config.sessionId}.owner.json`);
      fs.mkdirSync(path.dirname(inbox), { recursive: true });
      for (const source of ["generation", "inbox"]) {
        for (const started of ["not-a-start-time", "123oops", " 123", "1e6", "", 123]) {
          fs.rmSync(generationFile, { force: true }); fs.rmSync(inbox, { force: true });
          if (source === "generation") fs.writeFileSync(generationFile, JSON.stringify({ ...generation, processStartTime: started }));
          else fs.writeFileSync(inbox, JSON.stringify({ rootId: f.config.rootId, sessionId: f.config.sessionId,
            pid: f.child.pid, processStartedAt: started }));
          for (const action of ["stop", "remove"] as const) {
            const refused = await f.cli(action, actor.id);
            expect(refused.code).toBe(1); expect(refused.err).toContain("liveness unknown");
            expect(refused.err).toContain("--force-live");
            expect(f.host.actors.status(actor.id).status).toBe("idle");
          }
        }
      }
      expect((await f.cli("stop", actor.id, ["--force-live"])).code).toBe(0);
    } finally { await f.close(); }
  }, 30_000);

  it("an old dead generation cannot authorize control while a root-bound new Main has not published yet", async () => {
    const f = await fixture();
    let replacement: ChildProcess | undefined;
    try {
      const actor = await f.create("starting-main"); await stopChild(f.child);
      const environment = { ...process.env, PI_FABRIC_SESSION_ID: f.config.sessionId, PI_FABRIC_MAIN_AGENT_ID: f.config.rootId };
      for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_RESIDENT_CONFIG"]) delete environment[key as keyof typeof environment];
      replacement = spawn(process.execPath, ["-e", "console.log('ready');setInterval(()=>{},1000)"], {
        env: environment,
        stdio: ["ignore", "pipe", "pipe"],
      });
      await once(replacement.stdout!, "data");
      for (const action of ["stop", "remove"] as const) {
        const refused = await f.cli(action, actor.id);
        expect(refused.code).toBe(1); expect(refused.err).toContain("current root/session binding");
        expect(f.host.actors.status(actor.id).status).toBe("idle");
      }
      await stopChild(replacement);
      expect((await f.cli("stop", actor.id)).code).toBe(0);
    } finally { if (replacement) await stopChild(replacement); await f.close(); }
  }, 30_000);

  it.each(["unbound", "different root", "different session"] as const)("native-session Main before first publication: %s", async binding => {
    const f = await fixture();
    let replacement: ChildProcess | undefined;
    try {
      const actor = await f.create("native-starting-main"); await stopChild(f.child);
      // A native --session resume need not expose any optional root/session env.
      // Use the release census's Pi CLI path signature, without publishing new
      // generation/lease evidence or inspecting the native session's contents.
      const cli = path.join(f.root, "pi-runtime", "cli.js"), session = path.join(f.root, "native-session.jsonl");
      fs.mkdirSync(path.dirname(cli), { recursive: true });
      fs.writeFileSync(cli, "console.log('ready');setInterval(()=>{},1000)");
      fs.writeFileSync(session, JSON.stringify({ type: "session", version: 3, id: f.config.sessionId }) + "\n");
      const environment: NodeJS.ProcessEnv = { ...process.env };
      for (const key of ["PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_SESSION_ID", "PI_SESSION_ID", "PI_FABRIC_ROLE_SESSION",
        "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_RESIDENT_CONFIG"]) delete environment[key];
      if (binding === "different root") environment.PI_FABRIC_MAIN_AGENT_ID = "session:other-root";
      if (binding === "different session") environment.PI_SESSION_ID = "other-session";
      replacement = spawn(process.execPath, [cli, "--session", session], { env: environment, stdio: ["ignore", "pipe", "pipe"] });
      await once(replacement.stdout!, "data");
      for (const action of ["stop", "remove"] as const) {
        const dry = await f.cli(action, actor.id, ["--dry-run"]);
        if (binding === "unbound") {
          expect(dry.code).toBe(1); expect(dry.err).toContain(`PID ${replacement.pid}: Main without a Fabric marker (older release or still starting); retry after it publishes, or confirm and pass --force-live`);
          const refused = await f.cli(action, actor.id);
          expect(refused.code).toBe(1); expect(refused.err).toContain("--force-live");
        } else expect(dry).toMatchObject({ code: 0, err: "" });
        expect(f.host.actors.status(actor.id).status).toBe("idle");
        expect(new ActorRegistryStore(f.config.actorRoot).records().some(row => row.id === actor.id)).toBe(true);
      }
      expect((await f.cli("remove", actor.id, ["--force-live", "--dry-run"])).code).toBe(0);
      if (binding === "unbound") await stopChild(replacement);
      expect((await f.cli("stop", actor.id)).code).toBe(0);
    } finally { if (replacement) await stopChild(replacement); await f.close(); }
  }, 30_000);

  it.each(["stop", "remove"] as const)("refuses %s when a new Main publishes its generation between initial check and commit", async action => {
    const f = await fixture();
    const fence = mainPublication.withMainPublicationFence;
    let replacement: ChildProcess | undefined;
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const actor = await f.create("generation-race"); await stopChild(f.child);
      replacement = spawn(process.execPath, ["-e", "console.log('ready');setInterval(()=>{},1000)"], { stdio: ["ignore", "pipe", "pipe"] });
      await once(replacement.stdout!, "data");
      const file = mainGenerationPath(f.config.residencyRoot), old = JSON.parse(fs.readFileSync(file, "utf8"));
      spy = vi.spyOn(mainPublication, "withMainPublicationFence").mockImplementationOnce(async (meshRoot, rootId, operation, wait) => {
        // Real Main publication takes and releases the very same lock before the
        // operator acquires it. The old evidence already passed its first check.
        await fence(meshRoot, rootId, () => fs.writeFileSync(file, JSON.stringify({ ...old,
          nonce: "new-main", pid: replacement!.pid, processStartTime: processStartTime(replacement!.pid!) })));
        return fence(meshRoot, rootId, operation, wait);
      });
      const refused = await f.cli(action, actor.id);
      expect(refused.code).toBe(1); expect(refused.err).toContain("Main process is still alive");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
      expect(new ActorRegistryStore(f.config.actorRoot).records().some(row => row.id === actor.id)).toBe(true);
    } finally { spy?.mockRestore(); if (replacement) await stopChild(replacement); await f.close(); }
  }, 30_000);

  it.each(["generation", "lease"])("rejects a newer %s even if its PID/lease is already dead at the commit recheck", async kind => {
    const f = await fixture();
    const fence = mainPublication.withMainPublicationFence;
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const actor = await f.create("changed-evidence"); await stopChild(f.child);
      const file = mainGenerationPath(f.config.residencyRoot), old = JSON.parse(fs.readFileSync(file, "utf8"));
      spy = vi.spyOn(mainPublication, "withMainPublicationFence").mockImplementationOnce(async (meshRoot, rootId, operation, wait) => {
        await fence(meshRoot, rootId, () => {
          if (kind === "generation") fs.writeFileSync(file, JSON.stringify({ ...old, nonce: "new-but-dead" }));
          else writeHostLease(meshRoot, { id: rootId, rootId, identityId: rootId, updatedAt: Date.now(), expiresAt: Date.now() - 1 });
        });
        return fence(meshRoot, rootId, operation, wait);
      });
      const refused = await f.cli("remove", actor.id);
      expect(refused.code).toBe(1); expect(refused.err).toContain("changed before operator commit");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { spy?.mockRestore(); await f.close(); }
  }, 30_000);

  it("the held Main startup/lease publication lock refuses an operator commit", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("startup-lock"); await stopChild(f.child);
      await mainPublication.withMainPublicationFence(f.config.meshRoot, f.config.rootId, async () => {
        for (const action of ["stop", "remove"] as const) {
          const refused = await f.cli(action, actor.id);
          expect(refused.code).toBe(1); expect(refused.err).toContain("publication/startup is in progress");
          expect(f.host.actors.status(actor.id).status).toBe("idle");
        }
      });
      expect((await f.cli("stop", actor.id)).code).toBe(0);
    } finally { await f.close(); }
  }, 30_000);

  it("unreadable current process evidence refuses even when the recorded Main PID is dead", async () => {
    const f = await fixture();
    const read = fs.readFileSync;
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const actor = await f.create("unknown-current-main"); await stopChild(f.child);
      spy = vi.spyOn(fs, "readFileSync").mockImplementation((file, options) => {
        if (file === `/proc/${process.pid}/environ`) throw Object.assign(new Error("unreadable current runtime"), { code: "EACCES" });
        return read(file, options as never);
      });
      const refused = await f.cli("remove", actor.id);
      expect(refused.code).toBe(1); expect(refused.err).toContain("liveness is unknown");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { spy?.mockRestore(); await f.close(); }
  }, 30_000);

  it("Main's real initial lease publication waits on the same fence before arming startup heartbeats", async () => {
    const f = await fixture();
    const identity = { id: f.config.rootId, name: "Main", kind: "main" as const, sessionId: f.config.sessionId };
    const directory = new ParticipantDirectory(new MeshStore(f.config.meshRoot, 65536, 100), {
      enabled: true, hostId: f.config.rootId, rootId: f.config.rootId, identity,
    });
    const refresh = vi.spyOn(directory, "refresh");
    let starting: Promise<void> | undefined;
    try {
      await mainPublication.withMainPublicationFence(f.config.meshRoot, f.config.rootId, async () => {
        starting = directory.start();
        await new Promise(resolve => setTimeout(resolve, 100));
        expect(refresh).not.toHaveBeenCalled();
        expect(fs.existsSync(hostLeasePath(f.config.meshRoot, f.config.rootId))).toBe(false);
      });
      await starting;
      expect(refresh).toHaveBeenCalled();
      expect(fs.existsSync(hostLeasePath(f.config.meshRoot, f.config.rootId))).toBe(true);
    } finally { await starting?.catch(() => undefined); refresh.mockRestore(); await directory.close(); await f.close(); }
  }, 30_000);

  it("failed Main startup admission cannot let a waiting external refresh publish around the fence", async () => {
    const f = await fixture();
    const identity = { id: f.config.rootId, name: "Main", kind: "main" as const, sessionId: f.config.sessionId };
    const directory = new ParticipantDirectory(new MeshStore(f.config.meshRoot, 65536, 100), {
      enabled: true, hostId: f.config.rootId, rootId: f.config.rootId, identity,
    });
    const error = new Error("Main publication fence unavailable");
    const spy = vi.spyOn(mainPublication, "withMainPublicationFence").mockRejectedValueOnce(error);
    try {
      const results = await Promise.allSettled([directory.start(), directory.refresh()]);
      expect(results).toEqual([{ status: "rejected", reason: error }, { status: "rejected", reason: error }]);
      expect(fs.existsSync(hostLeasePath(f.config.meshRoot, f.config.rootId))).toBe(false);
      spy.mockRestore();
      await directory.start();
      expect(fs.existsSync(hostLeasePath(f.config.meshRoot, f.config.rootId))).toBe(true);
    } finally { spy.mockRestore(); await directory.close(); await f.close(); }
  }, 30_000);

  it("an unreadable exact configured worker launch is not mistaken for a Main", async () => {
    const f = await fixture();
    const read = fs.readFileSync;
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const actor = await f.create("known-worker");
      f.host.actors.tell(actor.id, "HANG_WITH_PROGRESS");
      await waitFor(() => !!f.host.actors.status(actor.id).inFlightRun);
      const run = f.host.actors.status(actor.id).inFlightRun!.id;
      await waitFor(() => "turns" in f.host.agents.status(run) && Number((f.host.agents.status(run) as { turns?: number }).turns) > 0);
      const pid = Number(f.host.agents.status(run).sessionId);
      await stopChild(f.child);
      spy = vi.spyOn(fs, "readFileSync").mockImplementation((file, options) => {
        if (file === `/proc/${pid}/environ`) throw Object.assign(new Error("worker exec transition"), { code: "EACCES" });
        return read(file, options as never);
      });
      const dry = await f.cli("remove", actor.id, ["--dry-run"]);
      expect(dry).toMatchObject({ code: 0, err: "" });
      expect(f.host.actors.status(actor.id).status).toBe("running");
    } finally { spy?.mockRestore(); await f.close(); }
  }, 30_000);

  it("a live file-only root lease vetoes control after Main process exits", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("leased"); await stopChild(f.child);
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      const result = await f.cli("remove", actor.id);
      expect(result.code).toBe(1); expect(result.err).toContain("live root lease");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);

  it.each(["unreadable", "unparseable"] as const)("a cached expired lease cannot authorize stop/remove/dry-run when current data is %s", async fault => {
    const f = await fixture();
    const read = fs.readFileSync;
    let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const actor = await f.create("damaged-lease"); await stopChild(f.child);
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
      for (const action of ["stop", "remove"] as const) for (const flags of [[], ["--dry-run"]]) {
        const refused = await f.cli(action, actor.id, flags);
        expect(refused.code).toBe(1); expect(refused.err).toContain("root lease is unreadable or invalid");
        expect(refused.err).toContain("--force-live");
        expect(f.host.actors.status(actor.id).status).toBe("idle");
        expect(new ActorRegistryStore(f.config.actorRoot).records().some(row => row.id === actor.id)).toBe(true);
      }
    } finally { spy?.mockRestore(); await f.close(); }
  }, 30_000);

  it.each(["stop", "remove"] as const)("%s rechecks current lease readability after initial cached-expired evidence passed", async action => {
    const f = await fixture();
    const fence = mainPublication.withMainPublicationFence, read = fs.readFileSync;
    let fenceSpy: ReturnType<typeof vi.spyOn> | undefined, readSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const actor = await f.create("lease-read-race"); await stopChild(f.child);
      const file = hostLeasePath(f.config.meshRoot, f.config.rootId);
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        updatedAt: 1, expiresAt: 2 });
      expect(readHostLease(f.config.meshRoot, f.config.rootId)?.expiresAt).toBe(2);
      fenceSpy = vi.spyOn(mainPublication, "withMainPublicationFence").mockImplementationOnce(async (meshRoot, rootId, operation, wait) => {
        writeHostLease(meshRoot, { id: rootId, rootId, identityId: rootId, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
        readSpy = vi.spyOn(fs, "readFileSync").mockImplementation((target, options) => {
          if (target === file) throw Object.assign(new Error("replaced lease inaccessible"), { code: "EACCES" });
          return read(target, options as never);
        });
        return fence(meshRoot, rootId, operation, wait);
      });
      const refused = await f.cli(action, actor.id);
      expect(refused.code).toBe(1); expect(refused.err).toContain("root lease is unreadable or invalid");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
      expect(new ActorRegistryStore(f.config.actorRoot).records().some(row => row.id === actor.id)).toBe(true);
    } finally { fenceSpy?.mockRestore(); readSpy?.mockRestore(); await f.close(); }
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
      const actor = await f.create("shared-lease"); await mainDirectory.start(); await stopChild(f.child);
      fs.rmSync(hostLeasePath(f.config.meshRoot, f.config.rootId), { force: true });
      const result = await f.cli("stop", actor.id);
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
      const actor = await f.create("bare-shared-lease"); await directory.start(); await stopChild(f.child);
      fs.rmSync(hostLeasePath(f.config.meshRoot, f.config.rootId), { force: true });
      expect(f.host.participants.get(f.config.rootId, Date.now(), { fresh: true })).toBeUndefined();
      expect((await f.cli("remove", actor.id)).err).toContain("live root lease");
    } finally { await directory.close(); await f.close(); }
  }, 30_000);

  it("inbox PID evidence, malformed safety data and symlinked channels fail closed", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("inbox-owner");
      const inbox = path.join(f.config.meshRoot, "main-followups", `${f.config.sessionId}.owner.json`);
      fs.mkdirSync(path.dirname(inbox), { recursive: true });
      fs.writeFileSync(inbox, JSON.stringify({ rootId: f.config.rootId, sessionId: f.config.sessionId,
        pid: f.child.pid, processStartedAt: processStartTime(f.child.pid!) }));
      fs.rmSync(mainGenerationPath(f.config.residencyRoot));
      expect((await f.cli("stop", actor.id)).err).toContain("Main process is still alive");
      await stopChild(f.child);
      fs.writeFileSync(inbox, "{bad JSON");
      expect((await f.cli("remove", actor.id)).code).toBe(1);
      fs.writeFileSync(inbox, JSON.stringify({ rootId: f.config.rootId, sessionId: f.config.sessionId, pid: f.child.pid }));
      const requestDir = path.join(f.config.residencyRoot, "requests");
      fs.renameSync(requestDir, requestDir + "-real");
      fs.symlinkSync(requestDir + "-real", requestDir, "dir");
      expect((await f.cli("remove", actor.id)).err).toContain("not owned by this OS user");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
    } finally { await f.close(); }
  }, 30_000);

  it("packaged bin enforces live-Main refusal, dead-Main removal and clean unknown-id exit", async () => {
    const f = await fixture();
    const run = promisify(execFile);
    const argv = (action: string, id: string) => [path.resolve("bin/fabric-actors"), action,
      "--resident", f.config.residencyRoot, "--actor", id, "--mesh-root", f.config.meshRoot];
    try {
      const actor = await f.create("bin-target");
      await expect(run(process.execPath, argv("remove", actor.id))).rejects.toMatchObject({ code: 1,
        stderr: expect.stringContaining("Main process is still alive") });
      await stopChild(f.child);
      const result = await run(process.execPath, argv("remove", actor.name));
      expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, action: "remove", resident: f.config.residencyRoot });
      await f.host.participants.refresh();
      expect(f.host.participants.get(actor.id, Date.now(), { fresh: true })).toBeUndefined();
      await expect(run(process.execPath, argv("stop", "unknown-id"))).rejects.toMatchObject({ code: 1,
        stderr: expect.stringContaining("Unknown Fabric actor") });
    } finally { await f.close(); }
  }, 30_000);

  it.each(["directory", "symlink"] as const)("a same-name CWD %s never redirects packaged stop/remove to another valid resident", async kind => {
    const f = await fixture();
    let other: Awaited<ReturnType<typeof fixture>> | undefined;
    const run = promisify(execFile);
    try {
      other = await fixture();
      const target = await f.create("same-name"), wrongRoot = await other.create("same-name");
      await stopChild(f.child); await stopChild(other.child);
      const selector = path.basename(f.config.residencyRoot);
      let cwd = path.dirname(other.config.residencyRoot);
      if (kind === "symlink") {
        cwd = path.join(f.root, "cwd"); fs.mkdirSync(cwd);
        fs.symlinkSync(other.config.residencyRoot, path.join(cwd, selector), "dir");
      }
      for (const action of ["stop", "remove"] as const) {
        const result = await run(process.execPath, [path.resolve("bin/fabric-actors"), action,
          "--resident", selector, "--actor", target.name], {
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
        "--resident", selector, "--actor", target.name], {
        cwd, env: { ...process.env, PI_FABRIC_MESH_ROOT: f.config.meshRoot },
      })).rejects.toMatchObject({ code: 1, stderr: expect.stringContaining("Ambiguous resident prefix") });
      expect(other.host.actors.status(wrongRoot.id).status).toBe("idle");
    } finally { if (other) await other.close(); await f.close(); }
  }, 40_000);

  it("unknown actor, invalid flags, missing process evidence and ambiguous resident prefixes fail cleanly", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("known"); await stopChild(f.child);
      const unknown = await f.cli("remove", "not-an-actor");
      expect(unknown.code).toBe(1); expect(unknown.err).toContain("Unknown Fabric actor");
      expect(f.host.actors.status(actor.id).status).toBe("idle");
      const client = new ResidentActorClient(f.config.meshRoot, f.config.rootId);
      await expect(client.operatorActor("remove", actor.id, { forceLive: "yes" as unknown as boolean })).rejects.toThrow("Invalid resident operator");
      const ownerPath = path.join(f.config.residencyRoot, "owner.json");
      const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
      fs.writeFileSync(ownerPath, JSON.stringify({ ...owner, commands: owner.commands.filter((op: string) => op !== "operatorActor") }));
      await expect(client.operatorActor("remove", actor.id)).rejects.toThrow("older release");
      expect(fs.readdirSync(path.join(f.config.residencyRoot, "requests"))).toHaveLength(0);
      fs.writeFileSync(ownerPath, JSON.stringify(owner));
      fs.rmSync(mainGenerationPath(f.config.residencyRoot));
      expect((await f.cli("remove", actor.id)).err).toContain("no recorded identity");
      fs.mkdirSync(path.join(f.config.meshRoot, "residency", path.basename(f.config.residencyRoot).slice(0, 12) + "0"));
      expect(() => resolveResidentDirectory(path.basename(f.config.residencyRoot).slice(0, 12), f.config.meshRoot)).toThrow("Ambiguous");
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
      const actor = await other.create("wrong-root"); await stopChild(other.child);
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
