import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, expect, it } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#6729 canary check 5: tell to a durable actor while its root's actor directory is read-only.
const mainParticipants = (config: ResidentHostConfig) => {
  const identity = { id: config.rootId, name: "live Main", kind: "main" as const, sessionId: config.sessionId };
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const participants = new ParticipantDirectory(mesh, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
  });
  participants.registerSource(() => [{
    format: 1, id: identity.id, rootId: identity.id, kind: "root", name: identity.name, status: "idle",
    ownerHostId: identity.id, ownerIdentityId: identity.id, sessionId: identity.sessionId,
    runner: "pi", transport: "host", capabilities: ["fabric"], controlProtocol: "v1",
    startedAt: Date.now(), updatedAt: Date.now(),
  }]);
  return participants;
};

beforeEach(() => installInProcessResidentFence());

const rootUser = process.getuid?.() === 0;

it.skipIf(process.platform === "win32" || rootUser)("a tell to a durable actor whose root actor directory is read-only is queued or definitely refused", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-readonly-tell-"));
  const sessionId = "readonly-tell";
  const rootId = `session:${sessionId}`;
  const meshRoot = path.join(root, "mesh");
  const sessionActorRoot = path.join(meshRoot, "actors", sessionId);
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId, cwd: root, projectRoot: root,
    meshRoot, actorRoot: path.join(meshRoot, "actors"), sessionActorRoot, residencyRoot: residentRoot(meshRoot, rootId),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const host = new ResidentHost(config);
  const participants = mainParticipants(config);
  let control: FabricControlPlane | undefined;
  let chmodded = false;
  try {
    await participants.start();
    await host.start();
    const client = new ResidencyClient({ config, mesh: host.mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget });
    control = new FabricControlPlane(host.mesh, { id: rootId, name: "main", kind: "main", sessionId },
      { enabled: true, hostId: rootId, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    control.start(() => ({ accepted: false }));
    const actor = await client.createActor({ name: "readonly tell", instructions: "Reply.", residency: "durable", scope: "session",
      transport: "process", responseMode: "text", delivery: "mailbox" }, AbortSignal.timeout(10_000));
    expect(fs.existsSync(path.join(sessionActorRoot, actor.id))).toBe(true);
    const before = host.actors.messages(actor.id).length;

    fs.chmodSync(sessionActorRoot, 0o500);
    chmodded = true;
    // The resident host renews its leases every 5 s; let at least two renewals see the read-only directory.
    await new Promise(resolve => setTimeout(resolve, Number(process.env.C5_READONLY_SETTLE_MS ?? 12_000)));
    let outcome: { ok: true; value: unknown } | { ok: false; error: Error };
    const started = Date.now();
    try {
      outcome = { ok: true, value: await control.request(client.hostId, actor.id, "followUp", { message: "PR 1000 head def", ownerIncarnation: participants.get(actor.id, undefined, { fresh: true })!.ownerIncarnation }, client.hostId) };
    } catch (error) {
      outcome = { ok: false, error: error as Error };
    }
    const elapsedMs = Date.now() - started;
    await new Promise(resolve => setTimeout(resolve, 500));
    const status = host.actors.status(actor.id);
    const inbound = host.actors.messages(actor.id).slice(before).filter(message => message.direction === "in");
    const queueFiles = fs.readdirSync(path.join(sessionActorRoot, actor.id)).filter(name => name.startsWith("queue-"));
    const queued = queueFiles.some(name => fs.readFileSync(path.join(sessionActorRoot, actor.id, name), "utf8").includes("PR 1000 head def"));
    const evidence = JSON.stringify({
      elapsedMs, outcome: outcome.ok ? outcome.value : { error: outcome.error.message },
      status: status.status, queued: (status as { queued?: number }).queued, inbound: inbound.length, queueFiles, queuedOnDisk: queued,
      inFlight: host.actors.inFlightCount(),
    });
    if (process.env.C5_REPRO_OUT) fs.appendFileSync(process.env.C5_REPRO_OUT, evidence + "\n");
    if (outcome.ok) {
      // An acknowledged tell must be held by the receiver: queued, in flight, or recorded inbound.
      expect(inbound.length + host.actors.inFlightCount() + ((status as { queued?: number }).queued ?? 0)).toBeGreaterThan(0);
    } else {
      // A refusal must be definite, never "outcome unknown" while nothing was queued.
      expect(outcome.error.message).not.toMatch(/outcome is unknown/);
      expect(outcome.error).toMatchObject({ code: "FABRIC_CONTROL_NOT_DELIVERED", notDelivered: true });
      expect(inbound).toHaveLength(0);
      // Writable again: the owner resumes, answers the withdrawn command as expired (never runs it),
      // and the resend the error calls safe is queued exactly once.
      fs.chmodSync(sessionActorRoot, 0o700);
      chmodded = false;
      const resent = await control.request(client.hostId, actor.id, "followUp", { message: "PR 1000 head def", ownerIncarnation: participants.get(actor.id, undefined, { fresh: true })!.ownerIncarnation }, client.hostId, { timeoutMs: 20_000 });
      expect(resent).toMatchObject({ acknowledged: true });
      const delivered = host.actors.messages(actor.id).slice(before)
        .filter(message => message.direction === "in" && JSON.stringify(message.data).includes("PR 1000 head def"));
      expect(delivered).toHaveLength(1);
    }
  } finally {
    if (chmodded) fs.chmodSync(sessionActorRoot, 0o700);
    await control?.close();
    await host.close();
    await participants.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 60_000);
