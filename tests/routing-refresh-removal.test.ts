import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { openNodeSqlite } from "../src/mesh/state-sqlite.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep: #2386/#9805's real directory/control boundary, corrupt-state refusal, and data safety
// after an owner ACK failure. Only private scratch roots and held Main journals; no model/agent run.
const roots: string[] = [];
const stores: MeshStore[] = [];
const directories: ParticipantDirectory[] = [];
const planes: FabricControlPlane[] = [];
const mains: MainAgentController[] = [];
const cleanups: Array<() => void> = [];
const who = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
const key = (prefix: string, id: string): string => prefix + createHash("sha256").update(id).digest("hex");
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const unknown = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];
const actors = (identity: MeshIdentity) => ({ identity, status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
  validateDirectMessage: () => undefined } as unknown as Ports[1]);

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) cleanup();
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  for (const main of mains.splice(0)) main.closeFollowUpDrain();
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const store of stores.splice(0)) store.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = async (backend: "file" | "sqlite", bootstrap = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-routing-removal-")); roots.push(root);
  const meshRoot = path.join(root, "mesh"), senderId = "session:sender", targetId = "session:target";
  const mesh = () => {
    const store = new MeshStore(meshRoot, 65_536, 1_000, { stateBackend: backend });
    stores.push(store); return store;
  };
  const directory = (id: string) => {
    const d = new ParticipantDirectory(mesh(), { enabled: true, hostId: id, rootId: id, identity: who(id),
      heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false, routingLease: { waitMs: 5, pollMs: 1 } });
    directories.push(d); return d;
  };
  const ownerDirectory = directory(targetId), senderDirectory = directory(senderId);
  const presence: FabricParticipantRecord = { format: 1, id: targetId, rootId: targetId, kind: "root",
    ownerHostId: targetId, ownerIdentityId: targetId, ownerIncarnation: ownerDirectory.ownerIncarnation,
    name: "target", status: "running", runner: "pi", transport: "host", capabilities: ["followUp", "steer"],
    controlProtocol: "v1", startedAt: 1, updatedAt: Date.now() };
  ownerDirectory.registerSource(() => [presence]);
  await ownerDirectory.refresh();
  if (!bootstrap) await senderDirectory.refresh();
  // Open the observer's SQLite connection before introducing a writer: schema/open contention
  // is a separate boundary, not a reason to silently use a different backend.
  expect(senderDirectory.get(targetId, undefined, { fresh: true })).toMatchObject({ ownerIdentityId: targetId, stale: false });
  const piSend = vi.fn();
  const main = new MainAgentController({ on: () => () => {}, sendMessage: piSend, getThinkingLevel: () => "off" } as unknown as ExtensionAPI,
    targetId, true, root, "target");
  mains.push(main);
  const journal = path.join(root, "main-followups.json");
  main.attachFollowUpDrain({ isIdle: () => false, hasPendingMessages: () => false,
    sessionManager: { getEntries: () => [] } } as unknown as ExtensionContext, 60_000, journal);
  const plane = (id: string, store: MeshStore, ownerIncarnation?: string) => {
    const directory = id === targetId ? ownerDirectory : senderDirectory;
    const p = new FabricControlPlane(store, who(id), { enabled: true, hostId: id, pollMs: 20,
      acknowledgementTimeoutMs: 2_000, captureOwnerLease: (...args) => directory.captureControlOwnerLease(...args),
      ...(ownerIncarnation ? { ownerIncarnation } : {}) });
    planes.push(p); return p;
  };
  const sender = plane(senderId, mesh()), owner = plane(targetId, mesh(), presence.ownerIncarnation);
  const receive = new AgentMessageRouter(unknown, actors(who(targetId)), main, ownerDirectory, owner, b => b);
  const accept = vi.fn((...args: Parameters<AgentMessageRouter["acceptControl"]>) => receive.acceptControl(...args));
  owner.start(accept); sender.start(() => ({ accepted: false }));
  const send = new AgentMessageRouter(unknown, actors(who(senderId)), {
    id: senderId, local: true, matches: (id: string) => id === senderId,
  } as unknown as Ports[2], senderDirectory, sender, b => b);
  const items = () => fs.existsSync(journal) ? JSON.parse(fs.readFileSync(journal, "utf8")).items as Array<Record<string, unknown>> : [];
  const commands = () => sender.mesh.read({ topic: "fabric.control.command", limit: 20 }).filter(e => e.kind !== "cancel");
  return { root, meshRoot, senderDirectory, ownerDirectory, sender, owner, send, main, presence, targetId, senderId,
    accept, piSend, journal, items, commands };
};

const assertAdmission = (f: Awaited<ReturnType<typeof fixture>>, message: string, publications = 1) => {
  expect(f.accept).toHaveBeenCalledOnce();
  expect(f.commands()).toHaveLength(publications);
  expect(new Set(f.commands().map(event => (event.data as { commandId: string }).commandId)).size).toBe(1);
  const event = f.commands()[0]!;
  expect(event.from.id).toBe(f.senderId);
  expect(event.data).toMatchObject({ targetId: f.targetId, ownerIncarnation: f.presence.ownerIncarnation, message,
    data: { proof: "unchanged" } });
  expect(f.items()).toEqual([expect.objectContaining({ message, data: { proof: "unchanged" },
    deliveryId: (event.data as { commandId: string }).commandId, from: expect.objectContaining({ id: f.senderId }),
    provenance: expect.objectContaining({ sender: expect.objectContaining({ id: f.senderId, verified: "mesh" }) }) })]);
  expect(f.main.queueDepth().pendingFollowUps).toBe(1);
  expect(f.piSend).not.toHaveBeenCalled(); // held/journalled, never falsely claimed delivered to Pi
};

describe("routing refresh removal on the Main admission boundary", () => {
  it.each(["file", "sqlite"] as const)("recovers an unconfirmed %s observer and durably admits once (#2386)", async backend => {
    const f = await fixture(backend, true);
    expect(f.senderDirectory.routingUnavailable()).toContain("no confirmed view");
    await expect(f.send.routeMessage(f.targetId, "bootstrap", { proof: "unchanged" }, "followUp"))
      .resolves.toMatchObject({ queued: true, acknowledged: true });
    assertAdmission(f, "bootstrap");
    expect(f.senderDirectory.canConsumeMesh()).toBe(false);
  });

  it("a live validated SQLite target admits through the original 2.5s writer burst (#9805)", async () => {
    const f = await fixture("sqlite", true);
    const holder = openNodeSqlite(path.join(f.meshRoot, "state.db")); holder.exec("BEGIN IMMEDIATE");
    let held = true;
    const release = () => { if (held) { held = false; holder.exec("ROLLBACK"); holder.close(); } };
    const timer = setTimeout(release, 2_500);
    cleanups.push(() => { clearTimeout(timer); release(); });
    await expect(f.send.routeMessage(f.targetId, "writer burst", { proof: "unchanged" }, "followUp"))
      .resolves.toMatchObject({ queued: true, acknowledged: true });
    assertAdmission(f, "writer burst");
  });

  it("publishes a live validated target before releasing an unrelated SQLite writer", async () => {
    const f = await fixture("sqlite", true);
    const holder = openNodeSqlite(path.join(f.meshRoot, "state.db")); holder.exec("BEGIN IMMEDIATE");
    let held = true, publishedWhileHeld = false;
    const release = () => { if (held) { held = false; holder.exec("ROLLBACK"); holder.close(); } };
    cleanups.push(release);
    const publish = f.sender.mesh.publish.bind(f.sender.mesh);
    vi.spyOn(f.sender.mesh, "publish").mockImplementation(async input => {
      const event = await publish(input);
      if (input.topic === "fabric.control.command" && input.kind !== "cancel") {
        publishedWhileHeld = held;
        release(); // The real receiver's shared claim can now run; no arbitrary latency threshold.
      }
      return event;
    });
    try {
      await expect(f.send.routeMessage(f.targetId, "writer independent", { proof: "unchanged" }, "followUp"))
        .resolves.toMatchObject({ queued: true, acknowledged: true });
      expect(publishedWhileHeld).toBe(true);
      assertAdmission(f, "writer independent");
    } finally { release(); }
  }, 15_000);

  it.each(["absent", "stale", "closed-lineage", "non-interactive", "replaced-epoch"] as const)("refuses %s without Main admission", async state => {
    const f = await fixture("file", true);
    const target = state === "absent" ? "session:absent" : f.targetId;
    if (state === "stale") {
      const hostKey = key("topology/hosts/", f.targetId), host = f.owner.mesh.get(hostKey)!.value as Record<string, unknown>;
      const expiresAt = Date.now() - 120_000;
      await f.owner.mesh.put({ key: hostKey, identity: who(f.targetId), value: { ...host, expiresAt } });
      writeHostLease(f.meshRoot, { ...readHostLease(f.meshRoot, f.targetId)!, expiresAt });
    } else if (state === "closed-lineage") await f.ownerDirectory.closeLineage();
    else if (state === "non-interactive" || state === "replaced-epoch") {
      // A new authenticated publication by the same owner is not permission to deliver
      // through a stale epoch or to a noninteractive Main.
      await f.owner.mesh.put({ key: key("topology/participants/", f.targetId), identity: who(f.targetId),
        value: { ...f.presence, ...(state === "non-interactive" ? { interactive: false } : { ownerIncarnation: "replacement" }),
          updatedAt: Date.now() + 1 } });
    }
    await expect(f.send.routeMessage(target, "must refuse", undefined, "followUp")).rejects.toThrow();
    expect(f.items()).toEqual([]); expect(f.accept).not.toHaveBeenCalled();
    if (state !== "replaced-epoch") expect(f.commands()).toEqual([]);
    else expect(f.commands()).toHaveLength(1); // Real receiver fenced the stale route before its handler.
  });

  it.each(["", "{", "{}", "null"])("refuses corrupt canonical bytes %j without rewriting or admitting (#2386)", async damaged => {
    const f = await fixture("file", true), file = path.join(f.meshRoot, "state.json"), healthy = fs.readFileSync(file, "utf8");
    fs.writeFileSync(file, damaged);
    try {
      for (const kind of ["followUp", "steer"] as const) {
        await expect(f.send.routeMessage(f.targetId, "never reinterpret as absence", undefined, kind))
          .rejects.toMatchObject({ code: "FABRIC_DIRECTORY_UNAVAILABLE", retryable: true });
      }
      expect(f.items()).toEqual([]); expect(f.commands()).toEqual([]); expect(f.accept).not.toHaveBeenCalled();
      expect(fs.readFileSync(file, "utf8")).toBe(damaged);
    } finally { fs.writeFileSync(file, healthy); }
  });

  it("a failed ACK and its cached failure never become a fresh send or second Main side effect", async () => {
    const f = await fixture("file", true);
    // The owner admits and durably journals, but returns an application failure. Its
    // retained failure is a real ACK, not a mocked sender request rejection.
    const accept = f.accept.getMockImplementation()!;
    f.accept.mockImplementation(async (...args) => {
      expect(await accept(...args)).toMatchObject({ accepted: true });
      return { accepted: false, error: "owner failed after durable admission" };
    });
    const options = { idempotencyKey: "failed-ack-once" };
    await expect(f.send.routeMessage(f.targetId, "one side effect", { proof: "unchanged" }, "followUp", undefined, options))
      .rejects.toThrow("owner failed after durable admission");
    assertAdmission(f, "one side effect");
    // Explicit same-key retry may republish the same command identity; its retained
    // failure must not run the handler or journal again. No automatic fresh-key retry.
    await expect(f.send.routeMessage(f.targetId, "one side effect", { proof: "unchanged" }, "followUp", undefined, options))
      .rejects.toThrow("owner failed after durable admission");
    assertAdmission(f, "one side effect", 2);
    const acknowledgements = f.sender.mesh.read({ topic: "fabric.control.ack", limit: 20 });
    expect(acknowledgements).toHaveLength(2);
    for (const event of acknowledgements) expect(event.data).toMatchObject({
      commandId: (f.commands()[0]!.data as { commandId: string }).commandId,
      accepted: false, error: "owner failed after durable admission",
    });
  });
});
