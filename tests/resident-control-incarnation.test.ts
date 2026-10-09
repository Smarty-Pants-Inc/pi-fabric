import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { CONTROL_STALE_INCARNATION, FabricControlPlane } from "../src/topology/control-plane.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

it("real resident relaunch keeps host/actor IDs but refuses the queued predecessor stop, then admits a fresh stop once", async () => {
  installInProcessResidentFence();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-control-incarnation-"));
  const identity = { id: "session:incarnation", name: "Main", kind: "main" as const, sessionId: "incarnation" };
  const config: ResidentHostConfig = {
    format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    sessionActorRoot: path.join(root, "session-actors"), residencyRoot: residentRoot(path.join(root, "mesh"), identity.id),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const mesh = new MeshStore(config.meshRoot, 65_536, 1_000);
  const main = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false });
  main.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Main", status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root,
    sessionId: identity.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  const sender = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
  const hosts: ResidentHost[] = [];
  let pending: Promise<unknown> | undefined;
  try {
    await main.start(); sender.start(() => ({ accepted: false }));
    const previous = new ResidentHost(config); hosts.push(previous); await previous.start();
    const actor = await previous.actors.create({ name: "durable-review", instructions: "wait", residency: "durable" });
    await previous.participants.refresh();
    const old = main.get(actor.id, undefined, { fresh: true })!;
    expect(old.ownerIncarnation).toBe(previous.control.incarnation);
    previous.control.pause();
    pending = sender.request(old.ownerHostId, actor.id, "stop", { ownerIncarnation: old.ownerIncarnation }, old.ownerIdentityId,
      { idempotencyKey: "before-relaunch", routedRemoteHost: null }).catch(error => error);
    await vi.waitFor(() => expect(mesh.read({ topic: "fabric.control.command" })).toHaveLength(1));
    await previous.close();
    const oldIncarnation = previous.control.incarnation;
    const replacement = new ResidentHost(config); hosts.push(replacement);
    expect(replacement.hostId).toBe(previous.hostId);
    const stop = vi.spyOn(ActorManager.prototype, "stop");
    await replacement.start();
    expect(replacement.control.incarnation).not.toBe(oldIncarnation);
    expect(await pending).toMatchObject({ code: CONTROL_STALE_INCARNATION });
    expect(stop).not.toHaveBeenCalled();
    expect(replacement.actors.status(actor.id)).toMatchObject({ id: actor.id, name: actor.name });
    await replacement.participants.refresh();
    const fresh = main.get(actor.id, undefined, { fresh: true })!;
    expect(fresh.ownerIncarnation).toBe(replacement.control.incarnation);
    for (let i = 0; i < 2; i++) await expect(sender.request(fresh.ownerHostId, actor.id, "stop", { ownerIncarnation: fresh.ownerIncarnation }, fresh.ownerIdentityId,
      { idempotencyKey: "after-relaunch", routedRemoteHost: null })).resolves.toMatchObject({ acknowledged: true });
    expect(stop).toHaveBeenCalledOnce();
    expect(mesh.read({ topic: "fabric.control.ack" }).at(-1)!.data).toMatchObject({ ownerIncarnation: replacement.control.incarnation });
  } finally {
    await sender.close(); await pending;
    for (const host of hosts.reverse()) await host.close();
    await main.close(); vi.restoreAllMocks();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 20_000);
