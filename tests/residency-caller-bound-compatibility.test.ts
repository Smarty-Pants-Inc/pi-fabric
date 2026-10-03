import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_COMMANDS, RESIDENT_HOST_FORMAT, residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { mergeBaseResidentOwner, receiveMergeBaseSpawn } from "./fixtures/merge-base-resident-spawn.js";

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-caller-compat-"));
  const identity = { id: "session:caller-compat", name: "Main", kind: "main" as const, sessionId: "caller-compat" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: identity.id, sessionId: identity.sessionId,
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: mesh.root, actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(mesh.root, identity.id), fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents,
    mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  participants.registerSource(() => [{
    format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    kind: "root", name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host",
    capabilities: ["fabric"], cwd: config.cwd, sessionId: identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
  }]);
  await participants.start();
  const client = new ResidencyClient({ config, mesh, participants, commandTimeoutMs: 2_000,
    mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget });
  const requests = path.join(config.residencyRoot, "requests");
  const responses = path.join(config.residencyRoot, "responses");
  fs.mkdirSync(requests, { recursive: true }); fs.mkdirSync(responses, { recursive: true });
  const publishOwner = (owner: object) => fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify(owner));
  const launch = vi.fn(async () => {
    await mesh.publish({ topic: "fabric.control.command", from: identity,
      data: { targetId: "session:unrelated-Main", operation: "followUp", message: "legacy unbound task" } });
    throw new Error("Legacy host launched an unbound task");
  });
  const consume = async (file: string) => {
    const command = JSON.parse(fs.readFileSync(path.join(requests, file), "utf8"));
    const response = await receiveMergeBaseSpawn(command, identity.id, file.slice(0, -5), { spawn: launch }, () => {});
    fs.writeFileSync(path.join(responses, file), JSON.stringify(response));
    fs.rmSync(path.join(requests, file));
    return command;
  };
  const noPublication = () => {
    expect(launch).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fabric.control.command" })).toEqual([]);
    expect(fs.readdirSync(requests)).toEqual([]);
    const decisions = path.join(config.residencyRoot, "decisions");
    expect(fs.existsSync(decisions) ? fs.readdirSync(decisions) : []).toEqual([]);
  };
  return { root, config, mesh, requests, client, publishOwner, launch, consume, noPublication,
    close: async () => { await client.close(); await participants.close(); fs.rmSync(root, { recursive: true, force: true }); } };
};

describe("caller-bound durable spawn mixed-version refusal", () => {
  it.each(["merge-base owner", "commands only", "capability only"] as const)("refuses %s before dispatch, launch or Main-bound publication", async kind => {
    const f = await fixture();
    const owner = mergeBaseResidentOwner(f.config.rootId);
    f.publishOwner({ ...owner,
      ...(kind === "commands only" ? { commands: RESIDENT_COMMANDS } : {}),
      ...(kind === "capability only" ? { callerBoundSpawn: 1 } : {}),
    });
    let done = false;
    // A live legacy receiver watches the same durable queue; stop and join it
    // even when the pre-fix client dispatches an unsafe launch.
    const receiver = (async () => { while (!done) {
      for (const file of fs.readdirSync(f.requests)) await f.consume(file);
      await delay(10);
    } })();
    try {
      const error = await f.client.spawnAgent({ task: "must not launch unbound", transport: "process" }).catch(error => error);
      f.noPublication();
      expect(error).toMatchObject({ name: "ResidentCommandUnsupportedError" });
      expect(error.message).toContain(owner.hostId);
      expect(error.message).toMatch(/caller-bound spawn/);
      expect(error.message).toMatch(/reload|handover/);
      expect(error.message).toContain("No request was dispatched");
    } finally { done = true; await receiver; await f.close(); }
  });

  it("legacy receiver rejects the actual bound envelope if rollback replaces the owner after publication", async () => {
    const f = await fixture();
    const controller = new AbortController();
    f.publishOwner({ ...mergeBaseResidentOwner(f.config.rootId), hostId: residentHostId(f.config.rootId),
      commands: RESIDENT_COMMANDS, callerBoundSpawn: 1 });
    const outcome = f.client.spawnAgent({ task: "persisted bound launch", transport: "process" }, controller.signal).catch(error => error);
    try {
      await vi.waitFor(() => expect(fs.readdirSync(f.requests)).toHaveLength(1));
      // Unclaimed request survives a replacement/rollback. The owner check can
      // no longer help: only the receiver's compiled-in wire ABI rejects it.
      f.publishOwner(mergeBaseResidentOwner(f.config.rootId));
      const envelope = await f.consume(fs.readdirSync(f.requests)[0]!);
      const error = await outcome;
      expect(f.launch).not.toHaveBeenCalled();
      expect(envelope).toMatchObject({ format: 1, operation: "spawnBound", caller: {
        id: f.config.rootId, sessionId: f.config.sessionId,
        returnAddress: { spawnerId: f.config.rootId, spawnerSessionId: f.config.sessionId },
      } });
      expect(error).toMatchObject({ name: "ResidentCommandUnsupportedError", message: expect.stringContaining("spawnBound") });
      expect(f.mesh.read({ topic: "fabric.control.command" })).toEqual([]);
      // The only allowed outcome is a parser refusal, never an unbound launch.
      expect(error.message).not.toContain("Legacy host launched");
    } finally { controller.abort(); await outcome; await f.close(); }
  });
});
