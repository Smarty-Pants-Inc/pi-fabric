import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, RESIDENT_IDLE_EXIT_MS } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { readResidentIdleExit, residentIdleExitPath } from "../src/residency/idle-exit.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { hostLeasePath, readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";
import { MAIN_RELOAD_LEASE_MS, ParticipantDirectory } from "../src/topology/participant-directory.js";

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function fixture(idleExitMs = RESIDENT_IDLE_EXIT_MS) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-idle-exit-"));
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = { format: 1, rootId: "session:idle-exit", sessionId: "idle-exit", cwd: root,
    projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:idle-exit"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, residentIdleExitMs: idleExitMs }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  const configPath = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  let now = Date.now(), exited = 0;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const log = vi.spyOn(console, "error").mockImplementation(() => {});
  const host = new ResidentHost(config, () => { exited++; });
  cleanups.push(async () => { await host.close(); fs.rmSync(root, { recursive: true, force: true }); });
  await host.start();
  await sleep(150); // Actual request ticks, deterministic host clock.
  return { root, config, configPath, host, log, exited: () => exited, now: () => now,
    advance: async (ms: number) => { now += ms; await sleep(150); } };
}

function rootLease(f: Awaited<ReturnType<typeof fixture>>, expiresAt: number) {
  writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
    updatedAt: f.now(), expiresAt });
}

describe("resident dead-root idle retirement (smarty-dev#6086)", () => {
  it("exits once after the default ten-minute continuous window and releases owner, lease and fence", async () => {
    const f = await fixture();
    await f.advance(RESIDENT_IDLE_EXIT_MS - 1);
    expect(f.exited()).toBe(0);
    await f.advance(1);
    expect(f.exited()).toBe(1);
    expect(readResidentIdleExit(f.config.residencyRoot, f.config.rootId)).toMatchObject({
      reason: "root-dead-idle", pid: process.pid, idleMs: RESIDENT_IDLE_EXIT_MS,
      token: JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "owner.json"), "utf8")).token,
    });
    expect(f.log.mock.calls.filter(([line]) => String(line).startsWith("resident exiting:"))).toEqual([
      ["resident exiting: root dead, no actors, idle 10 min"],
    ]);
    await f.advance(RESIDENT_IDLE_EXIT_MS);
    expect(f.exited()).toBe(1);
    await f.host.close();
    expect(fs.existsSync(path.join(f.config.residencyRoot, "owner.json"))).toBe(false);
    expect(readHostLeaseCurrent(f.config.meshRoot, f.host.hostId)).toBeUndefined();
    const successor = new ResidentHost(f.config);
    try { await successor.start(); } finally { await successor.close(); }
  });

  it("a live Main host lease without a participant vetoes exit and renewal resets the window", async () => {
    const f = await fixture(2_000);
    rootLease(f, f.now() + 10_000);
    await f.advance(5_000);
    expect(f.exited()).toBe(0);
    fs.rmSync(hostLeasePath(f.config.meshRoot, f.config.rootId));
    await f.advance(1_000); // first absent-root sample
    await f.advance(1_999);
    expect(f.exited()).toBe(0);
    rootLease(f, f.now() + 10_000); // before final fresh exit validation
    await f.advance(1);
    expect(f.exited()).toBe(0);
    fs.rmSync(hostLeasePath(f.config.meshRoot, f.config.rootId));
    await f.advance(1_000);
    await f.advance(2_000);
    expect(f.exited()).toBe(1);
  });

  it("honours Main's actual 180-second reload lease before starting the idle window", async () => {
    const f = await fixture(2_000);
    const identity = { id: f.config.rootId, name: "Main", kind: "main" as const, sessionId: f.config.sessionId };
    const mainMesh = new MeshStore(f.config.meshRoot, 64 * 1024, 100);
    const main = new ParticipantDirectory(mainMesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false });
    cleanups.push(async () => { await main.close(); mainMesh.closeState(); });
    main.registerSource(() => [{ format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id,
      ownerIdentityId: identity.id, kind: "root", name: "Main", status: "idle", runner: "pi", transport: "host",
      capabilities: ["fabric"], controlProtocol: "v1", sessionId: identity.sessionId, cwd: f.root, startedAt: 1, updatedAt: f.now() }]);
    await main.start();
    await f.advance(1_000);
    await main.quiesce("reload");
    await main.close();
    await f.advance(MAIN_RELOAD_LEASE_MS - 1);
    expect(f.exited()).toBe(0);
    await f.advance(2);
    expect(f.exited()).toBe(0);
    await f.advance(1_000); // observe expiry after the bounded root-observation cache
    await f.advance(2_000);
    expect(f.exited()).toBe(1);
  });

  it.each(["idle", "stopped"] as const)("keeps a dead-root resident with an owned %s actor", async status => {
    const f = await fixture(2_000);
    const actor = await f.host.actors.create({ name: "retained", instructions: "wait", residency: "durable" });
    if (status === "stopped") await f.host.actors.stop(actor.id);
    await f.advance(10_000);
    expect(f.host.actors.hasOwnedActors()).toBe(true);
    expect(f.exited()).toBe(0);
    await f.host.actors.remove(actor.id);
    await f.advance(1_000);
    await f.advance(1_999);
    expect(f.exited()).toBe(0);
    await f.advance(1);
    expect(f.exited()).toBe(1);
  });

  it.each(["requests", "processing"])("a file in %s vetoes exit and removal starts a new full window", async directory => {
    const f = await fixture(2_000);
    const file = path.join(f.config.residencyRoot, directory, "pending.json");
    if (directory === "requests") {
      const rename = fs.renameSync;
      vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
        if (String(source) === file) throw new Error("request deliberately queued");
        return rename(source, target);
      });
    }
    fs.writeFileSync(file, "{}");
    await f.advance(10_000);
    expect(f.exited()).toBe(0);
    fs.rmSync(file);
    await f.advance(1_000);
    await f.advance(1_999);
    expect(f.exited()).toBe(0);
    await f.advance(1);
    expect(f.exited()).toBe(1);
  });

  it("confirms actor and root state freshly before exit, never trusts a reused empty observation", async () => {
    const f = await fixture(2_000);
    const actors = vi.spyOn(f.host.actors, "hasOwnedActors").mockReturnValue(false);
    await f.advance(1_999); // cache an empty observation
    actors.mockReturnValue(true);
    await f.advance(1); // cached says empty; fresh must veto
    expect(f.exited()).toBe(0);
    expect(actors).toHaveBeenLastCalledWith();
  });

  it("0 disables even a dead and empty host, and accepted live policy edits can disable/re-enable", async () => {
    const f = await fixture(0);
    await f.advance(10 * RESIDENT_IDLE_EXIT_MS);
    expect(f.exited()).toBe(0);
    const replacePolicy = (residentIdleExitMs: number) => {
      fs.writeFileSync(f.configPath + ".tmp", JSON.stringify({ ...f.config, mesh: { ...f.config.mesh, residentIdleExitMs } }));
      fs.renameSync(f.configPath + ".tmp", f.configPath);
    };
    replacePolicy(2_000);
    await f.advance(1_000);
    await f.advance(1_999);
    expect(f.exited()).toBe(0);
    replacePolicy(0);
    await f.advance(1);
    await f.advance(10_000);
    expect(f.exited()).toBe(0);
    replacePolicy(2_000);
    await f.advance(1_000);
    await f.advance(2_000);
    expect(f.exited()).toBe(1);
  });

  it.each(["queued", "running"])("an active %s agent vetoes exit", async status => {
    const f = await fixture(2_000);
    const agents = vi.spyOn(f.host.agents, "listForUi").mockReturnValue([{ id: "active", status }] as never);
    await f.advance(10_000);
    expect(f.exited()).toBe(0);
    agents.mockRestore();
    await f.advance(1_000);
    await f.advance(2_000);
    expect(f.exited()).toBe(1);
  });

  it("counts a request processed wholly between cached actor/root samples as activity", async () => {
    const f = await fixture(2_000);
    await f.advance(1_900);
    const requestId = "short-request";
    fs.writeFileSync(path.join(f.config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({
      format: 1, requestId, rootId: f.config.rootId, operation: "cleanup", id: "missing-agent", createdAt: f.now(),
    }));
    await sleep(150);
    expect(fs.existsSync(path.join(f.config.residencyRoot, "requests", `${requestId}.json`))).toBe(false);
    await f.advance(100); // old window elapsed, but only 100 ms since the request
    expect(f.exited()).toBe(0);
    await f.advance(1_900);
    expect(f.exited()).toBe(1);
  });

  it("an unreadable processing queue cannot certify emptiness", async () => {
    const f = await fixture(2_000);
    const read = fs.readdirSync;
    vi.spyOn(fs, "readdirSync").mockImplementation(((directory: fs.PathLike, ...args: unknown[]) => {
      if (String(directory) === path.join(f.config.residencyRoot, "processing")) throw new Error("EACCES fixture");
      return Reflect.apply(read, fs, [directory, ...args]);
    }) as typeof fs.readdirSync);
    await f.advance(10_000);
    expect(f.exited()).toBe(0);
  });

  it("invalid Main lease bytes veto retirement rather than treating unreadability as death", async () => {
    const f = await fixture(2_000);
    fs.writeFileSync(hostLeasePath(f.config.meshRoot, f.config.rootId), "invalid lease");
    await f.advance(10_000);
    expect(f.exited()).toBe(0);
    expect(fs.existsSync(residentIdleExitPath(f.config.residencyRoot))).toBe(false);
  });
});
