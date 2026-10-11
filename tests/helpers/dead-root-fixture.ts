import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { main } from "../../src/actors-cli.js";
import { ActorRegistryStore } from "../../src/actors/registry-store.js";
import { ROOT_PARTICIPANT_FRESH_MS } from "../../src/residency/operator-safety.js";
import { ResidentHost } from "../../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../../src/residency/protocol.js";
import { writeParticipantFile } from "../../src/topology/participant-files.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import { MeshStore, type MeshIdentity } from "../../src/mesh/store.js";
import type { FabricParticipantRecord } from "../../src/topology/types.js";

/** A real root Main publishing its root participant through ParticipantDirectory (in this process, or in a
 * separate Main process: tests/fixtures/process-harness-child.mjs). */
export const mainDirectory = (meshRoot: string, rootId: string,
  mainProcess: { pid: number; host: string; startTime?: string } = { pid: process.pid, host: os.hostname() },
  { heartbeatMs = 60_000 }: { heartbeatMs?: number } = {}): ParticipantDirectory => {
  const identity: MeshIdentity = { id: rootId, name: "main", kind: "main" };
  const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
    enabled: true, hostId: rootId, rootId, identity, heartbeatMs, leaseMs: Math.max(120_000, 2 * heartbeatMs), reapDeadHosts: false });
  directory.registerSource(() => [{ format: 1, id: rootId, kind: "root", rootId, ownerHostId: rootId, ownerIdentityId: rootId,
    name: "main", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    sessionId: "dead-root", startedAt: Date.now(), updatedAt: Date.now(), pendingMessages: false, controlProtocol: "v1",
    mainProcess } as FabricParticipantRecord]);
  return directory;
};

export const rootParticipantKey = (rootId: string) => "topology/participants/" + createHash("sha256").update(rootId).digest("hex");

// smarty-dev#7817: `fabric-actors remove --confirm-dead-root --main-stopped --evidence` for the two dead-root shapes.
export const EVIDENCE = "herdr agent list: no pane for session dead-root\nps: no pi process for session dead-root";

/** A resident root with durable actors; `killResident` leaves it as a crashed resident and a dead Main leave it. */
export const fixture = async (options: { actorRoot?: string } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-dead-root-"));
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:dead-root", sessionId: "dead-root", cwd: process.cwd(), projectRoot: process.cwd(),
    meshRoot, actorRoot: options.actorRoot ?? path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:dead-root"),
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
    key: rootParticipantKey(config.rootId),
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
  }, root };
};
export type DeadRootFixture = Awaited<ReturnType<typeof fixture>>;
