import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import {
  RESIDENT_COMMANDS, RESIDENT_HOST_FORMAT, RESIDENT_REQUEST_NUDGE_FILE, residentHostId, residentRequestNudgePath, residentRoot,
  type ResidentHostConfig,
} from "../src/residency/protocol.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

// smarty-dev#11107: the client's best-effort request nudge must be an atomic replace,
// never a write through whatever already sits at the nudge pathname.
// Fixture mirrors the modern "rollback-safe resident actor command envelopes" fake owner in
// tests/resident-command-compatibility.test.ts, plus a minimal responder on requests/.

const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => { while (cleanups.length) await cleanups.pop()!(); });

const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-client-nudge-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:nudge", sessionId: "nudge",
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:nudge"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const requests = path.join(config.residencyRoot, "requests");
  const responses = path.join(config.residencyRoot, "responses");
  fs.mkdirSync(requests, { recursive: true });
  fs.mkdirSync(responses, { recursive: true });
  // Live owner (this process) advertising the modern command set and abandonment fence.
  fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify({
    format: 1, rootId: config.rootId, hostId: residentHostId(config.rootId), pid: process.pid,
    startedAt: Date.now(), readyAt: Date.now(), commands: RESIDENT_COMMANDS, requestFence: 1,
  }));

  // Minimal owner responder: admits only requests/*.json, answers each once.
  const seenRequests: string[] = [];
  const answered = new Set<string>();
  const scan = () => {
    let names: string[];
    try { names = fs.readdirSync(requests); } catch { return; }
    for (const name of names) {
      if (!name.endsWith(".json") || answered.has(name)) continue;
      let command: { requestId?: string; operation?: string };
      try { command = JSON.parse(fs.readFileSync(path.join(requests, name), "utf8")); } catch { continue; }
      answered.add(name); seenRequests.push(name);
      const response = { format: RESIDENT_HOST_FORMAT, requestId: command.requestId, ok: true, actors: [], completedAt: Date.now() };
      const tmp = path.join(responses, `.${name}.tmp`);
      fs.writeFileSync(tmp, JSON.stringify(response));
      fs.renameSync(tmp, path.join(responses, name));
      fs.rmSync(path.join(requests, name), { force: true });
    }
  };
  const watcher = fs.watch(requests, () => scan());
  const poll = setInterval(scan, 25); // fallback should an fs event be coalesced
  cleanups.push(() => { watcher.close(); clearInterval(poll); });

  const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 1_000 });
  participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"], cwd: config.cwd, sessionId: config.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  await participants.start();
  cleanups.push(() => participants.close());
  const client = new ResidencyClient({ config, mesh, participants, mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget });
  cleanups.push(() => client.close());

  // Innocent private file outside the residency root.
  const privateDir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-client-nudge-private-"));
  cleanups.push(() => fs.rmSync(privateDir, { recursive: true, force: true }));
  const target = path.join(privateDir, "secret.txt");
  const targetBytes = Buffer.from("innocent private bytes\n");
  fs.writeFileSync(target, targetBytes, { mode: 0o600 });

  const nudge = residentRequestNudgePath(config.residencyRoot);
  return { config, client, nudge, target, targetBytes, seenRequests, requests };
};

const timedActors = async (client: ResidencyClient) => {
  const started = Date.now();
  const actors = await client.actors();
  return { actors, elapsed: Date.now() - started };
};

describe("ResidencyClient request nudge (smarty-dev#11107)", () => {
  it("writes the nudge as JSON naming the admitted request file; requests/ stays the only admitted input", async () => {
    const { client, nudge, seenRequests, config } = await setup();
    expect(path.dirname(nudge)).toBe(config.residencyRoot);
    expect(path.basename(nudge)).toBe(RESIDENT_REQUEST_NUDGE_FILE);
    const { actors, elapsed } = await timedActors(client);
    expect(actors).toEqual([]);
    expect(elapsed).toBeLessThan(3_000);
    expect(seenRequests).toHaveLength(1);
    const value = JSON.parse(fs.readFileSync(nudge, "utf8"));
    expect(value).toEqual({ requestId: expect.any(String) });
    expect(`${value.requestId}.json`).toBe(seenRequests[0]);
    // The nudge is not a request and was never admitted as one.
    expect(seenRequests.every(name => !name.includes(RESIDENT_REQUEST_NUDGE_FILE))).toBe(true);
    expect(fs.lstatSync(nudge).isFile()).toBe(true);
  });

  it("does not write through a nudge symlink; replaces the alias with a regular JSON file", async () => {
    const { client, nudge, target, targetBytes, seenRequests } = await setup();
    fs.symlinkSync(target, nudge);
    expect(fs.lstatSync(nudge).isSymbolicLink()).toBe(true);
    const before = fs.statSync(target);
    const { actors, elapsed } = await timedActors(client);
    expect(actors).toEqual([]);
    expect(elapsed).toBeLessThan(3_000);
    expect(fs.readFileSync(target).equals(targetBytes)).toBe(true);
    const after = fs.statSync(target);
    expect({ ino: after.ino, size: after.size, mtimeMs: after.mtimeMs, mode: after.mode })
      .toEqual({ ino: before.ino, size: before.size, mtimeMs: before.mtimeMs, mode: before.mode });
    const stat = fs.lstatSync(nudge);
    expect(stat.isSymbolicLink()).toBe(false);
    expect(stat.isFile()).toBe(true);
    expect(stat.ino).not.toBe(after.ino);
    const value = JSON.parse(fs.readFileSync(nudge, "utf8"));
    expect(`${value.requestId}.json`).toBe(seenRequests[0]);
  });

  it.skipIf(process.platform === "win32")("does not write through a nudge hardlink to a private file", async () => {
    const { client, nudge, target, targetBytes, seenRequests } = await setup();
    try { fs.linkSync(target, nudge); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EXDEV") return; // different filesystems: not constructible
      throw error;
    }
    expect(fs.statSync(target).nlink).toBe(2);
    const { actors, elapsed } = await timedActors(client);
    expect(actors).toEqual([]);
    expect(elapsed).toBeLessThan(3_000);
    expect(fs.readFileSync(target).equals(targetBytes)).toBe(true);
    expect(fs.statSync(target).nlink).toBe(1);
    expect(fs.lstatSync(nudge).ino).not.toBe(fs.statSync(target).ino);
    expect(`${JSON.parse(fs.readFileSync(nudge, "utf8")).requestId}.json`).toBe(seenRequests[0]);
  });
});
