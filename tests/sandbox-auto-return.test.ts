import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  sandboxAutoReturnForChildRoot,
  sandboxResultPrefix,
  sandboxTaskMessage,
} from "../src/agents/sandbox-auto-return.js";
import { fabricTurnProvenance } from "../src/fabric-provenance.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import type { FabricTransport } from "../src/transport/types.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricParticipantRecord, FabricParticipantSource } from "../src/topology/types.js";
import { PATH_B_MACHINES, createPathBHarness, resolveNatsBinary } from "./helpers/nats-cluster.js";
import { PATH_B_OPERATOR_MACHINE } from "../src/mesh/nats-auth/permissions.js";
import { createSandboxFabricMesh } from "../src/transport/sandbox/fabric-mesh.js";
import { captureSandboxFinalAnswer, waitForSandboxFinalAnswer } from "../src/agents/completion-inbox.js";

const spawner = { id: "session:sandbox-parent", kind: "main" as const };
const trustedProvenance = {
  v: 1,
  channel: "fabric",
  via: "followUp",
  sender: { id: spawner.id, kind: "main", verified: "mesh" },
};
const childReturn = (runId: string) => sandboxAutoReturnForChildRoot({
  runId,
  spawner,
  isRoot: true,
});

const taskReceipt = (runId: string, provenance: unknown = trustedProvenance) => ({
  message: sandboxTaskMessage(runId, "return the requested result"),
  provenance,
});

const natsBinary = resolveNatsBinary();
if (!natsBinary && process.env.PI_FABRIC_REQUIRE_NATS === "1") {
  throw new Error("PI_FABRIC_REQUIRE_NATS=1 but no nats-server executable was found");
}
const describeRealNats = describe.skipIf(!natsBinary || process.platform !== "linux");

const provisionRealNats = async (fixture: Awaited<ReturnType<typeof createPathBHarness>>): Promise<void> => {
  const environment = fixture.adminEnv(fixture.users.bootstrap);
  const previous = new Map(Object.keys(environment).map(name => [name, process.env[name]]));
  for (const [name, value] of Object.entries(environment)) process.env[name] = value;
  try {
    const { runTransportAdmin } = await import("../src/transport/nats/admin.js");
    const result = await runTransportAdmin([
      "provision", "--ca-file", fixture.tls!.caFile, "--machines", PATH_B_MACHINES.join(","),
    ]);
    if (result !== 0) throw new Error(`sandbox real-NATS provision failed with exit ${result}`);
  } finally {
    for (const [name, value] of previous) {
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
    }
  }
};

const liveRoute = async (directory: ParticipantDirectory, id: string) => {
  const view = directory.participantView;
  if (!view) throw new Error("NATS participant view is unavailable");
  const signal = AbortSignal.timeout(10_000);
  try {
    while (!view.resolve(id)?.live) await view.whenChanged(id, signal);
    return view.resolve(id)!;
  } catch (error) {
    throw new Error(`NATS participant route ${id} did not become live`, { cause: error });
  }
};

describe("sandbox child auto-return", () => {
  it("returns plain final assistant text once after a trusted task settles", async () => {
    const runId = "sandbox-auto-plain-1";
    const autoReturn = childReturn(runId)!;
    expect(autoReturn.receiveTask(taskReceipt(runId))).toBe(true);
    autoReturn.recordAssistantText("  plain final answer\n");
    const send = vi.fn(async (message: string) => ({ queued: true, message }));

    await expect(autoReturn.sendAutomaticResult("completed", send)).resolves.toEqual({
      queued: true,
      message: `${sandboxResultPrefix(runId)}  plain final answer\n`,
    });
    await expect(autoReturn.sendAutomaticResult("completed", send)).resolves.toBeUndefined();
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("does not return a result when the task did not settle successfully", async () => {
    for (const outcome of ["failed", "aborted", "unknown"]) {
      const autoReturn = childReturn(`sandbox-auto-${outcome}`)!;
      expect(autoReturn.receiveTask(taskReceipt(autoReturn.runId))).toBe(true);
      autoReturn.recordAssistantText("not a completed result");
      const send = vi.fn(async (message: string) => message);

      await expect(autoReturn.sendAutomaticResult(outcome, send)).resolves.toBeUndefined();
      expect(send).not.toHaveBeenCalled();
    }
  });

  it("keeps the first successful explicit result and never sends it twice", async () => {
    const runId = "sandbox-auto-explicit-1";
    const autoReturn = childReturn(runId)!;
    expect(autoReturn.receiveTask(taskReceipt(runId))).toBe(true);
    autoReturn.recordAssistantText("automatic answer must lose");
    const firstReceipt = { queued: true, messageId: "first-result" };
    const send = vi.fn(async (message: string) => ({ ...firstReceipt, message }));

    const first = await autoReturn.sendExplicitResult(
      spawner.id,
      `${sandboxResultPrefix(runId)}first explicit answer`,
      () => send(`${sandboxResultPrefix(runId)}first explicit answer`),
    );
    if (!first.handled) throw new Error("trusted explicit result was not handled");
    const duplicate = await autoReturn.sendExplicitResult(
      spawner.id,
      `${sandboxResultPrefix(runId)}later answer must not replace it`,
      () => send(`${sandboxResultPrefix(runId)}later answer must not replace it`),
    );
    const automatic = await autoReturn.sendAutomaticResult("completed", send);

    expect(first).toMatchObject({ handled: true, result: { messageId: "first-result", message: `${sandboxResultPrefix(runId)}first explicit answer` } });
    expect(duplicate).toEqual(first);
    expect(automatic).toEqual(first.result);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("shares an in-flight explicit result so concurrent followUps cannot duplicate it", async () => {
    const runId = "sandbox-auto-race-1";
    const autoReturn = childReturn(runId)!;
    expect(autoReturn.receiveTask(taskReceipt(runId))).toBe(true);
    let release!: (value: { messageId: string }) => void;
    const send = vi.fn(() => new Promise<{ messageId: string }>(resolve => { release = resolve; }));
    const first = autoReturn.sendExplicitResult(spawner.id, `${sandboxResultPrefix(runId)}first`, send);
    const second = autoReturn.sendExplicitResult(spawner.id, `${sandboxResultPrefix(runId)}second`, send);
    await Promise.resolve();
    release({ messageId: "one-send" });

    await expect(Promise.all([first, second])).resolves.toEqual([
      { handled: true, result: { messageId: "one-send" } },
      { handled: true, result: { messageId: "one-send" } },
    ]);
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("does not let a failed explicit send suppress one automatic result", async () => {
    const runId = "sandbox-auto-explicit-failed-1";
    const autoReturn = childReturn(runId)!;
    expect(autoReturn.receiveTask(taskReceipt(runId))).toBe(true);
    autoReturn.recordAssistantText("final after explicit refusal");
    await expect(autoReturn.sendExplicitResult(
      spawner.id,
      `${sandboxResultPrefix(runId)}failed explicit attempt`,
      async () => { throw new Error("control refused"); },
    )).rejects.toThrow("control refused");
    const send = vi.fn(async (message: string) => ({ message }));

    await expect(autoReturn.sendAutomaticResult("completed", send)).resolves.toEqual({
      message: `${sandboxResultPrefix(runId)}final after explicit refusal`,
    });
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("rejects an untrusted task and does not arm automatic return", async () => {
    const runId = "sandbox-auto-untrusted-1";
    const autoReturn = childReturn(runId)!;
    expect(autoReturn.receiveTask(taskReceipt(runId, {
      ...trustedProvenance,
      sender: { id: "session:attacker", kind: "main", verified: "mesh" },
    }))).toBe(false);
    autoReturn.recordAssistantText("must not be sent");
    const send = vi.fn(async (message: string) => message);

    await expect(autoReturn.sendAutomaticResult("completed", send)).resolves.toBeUndefined();
    expect(send).not.toHaveBeenCalled();
    expect(autoReturn.hasTaskReceipt).toBe(false);
  });

  it("does not arm without a valid placement run, root identity, and bound Main spawner", () => {
    expect(sandboxAutoReturnForChildRoot({ runId: "../escape", spawner, isRoot: true })).toBeUndefined();
    expect(sandboxAutoReturnForChildRoot({ runId: "sandbox-auto-invalid-root", spawner, isRoot: false })).toBeUndefined();
    expect(sandboxAutoReturnForChildRoot({ runId: "sandbox-auto-no-spawner", spawner: undefined, isRoot: true })).toBeUndefined();
    expect(sandboxAutoReturnForChildRoot({
      runId: "sandbox-auto-alias-spawner",
      spawner: { id: "main", kind: "main" },
      isRoot: true,
    })).toBeUndefined();
    expect(sandboxAutoReturnForChildRoot({
      runId: "sandbox-auto-wrong-spawner",
      spawner: { id: "actor:parent", kind: "actor" },
      isRoot: true,
    })).toBeUndefined();
  });
});

describeRealNats("sandbox auto-return over a real authenticated NATS broker", () => {
  it("receives the bound task and returns its result through real NATS control", async () => {
    if (!natsBinary) throw new Error("NATS fixture is unavailable");
    const fixture = await createPathBHarness({ binary: natsBinary, tls: {}, org: "tristar" });
    let parentTransport: FabricTransport | undefined;
    let childTransport: FabricTransport | undefined;
    let parentDirectory: ParticipantDirectory | undefined;
    let childDirectory: ParticipantDirectory | undefined;
    let parentControl: FabricControlPlane | undefined;
    let childControl: FabricControlPlane | undefined;
    let parentMesh: MeshStore | undefined;
    let childMesh: MeshStore | undefined;
    const resultAbort = new AbortController();
    try {
      await provisionRealNats(fixture);
      const runId = `sandbox-nats-${randomUUID().slice(0, 8)}`;
      const parentSession = randomUUID();
      const childSession = randomUUID();
      const parentId = `session:${parentSession}`;
      const childId = `session:${childSession}`;
      const parentIdentity: MeshIdentity = { id: parentId, name: "sandbox parent", kind: "main", sessionId: parentSession };
      const childIdentity: MeshIdentity = { id: childId, name: "sandbox child", kind: "main", sessionId: childSession };
      const parentMeshRoot = path.join(fixture.root, `parent-mesh-${parentSession}`);
      const childMeshRoot = path.join(fixture.root, `child-mesh-${childSession}`);
      const parentStateDir = path.join(fixture.root, `parent-state-${parentSession}`);
      const childStateDir = path.join(fixture.root, `child-state-${childSession}`);
      for (const directory of [parentMeshRoot, childMeshRoot, parentStateDir, childStateDir]) {
        fs.mkdirSync(directory, { mode: 0o700 });
      }
      parentMesh = new MeshStore(parentMeshRoot, 64 * 1024, 1_000);
      childMesh = new MeshStore(childMeshRoot, 64 * 1024, 1_000);
      parentTransport = await openNatsTransport({
        ...fixture.runtimeOptions(fixture.users.machineA), hostId: parentId, name: `sandbox-parent-${parentSession}`,
      });
      childTransport = await openNatsTransport({
        ...fixture.runtimeOptions(fixture.users.machineB), hostId: childId, name: `sandbox-child-${childSession}`,
      });
      parentDirectory = new ParticipantDirectory(parentMesh, {
        enabled: true, transport: parentTransport, transportStateDir: parentStateDir,
        hostId: parentId, rootId: parentId, identity: parentIdentity,
      });
      childDirectory = new ParticipantDirectory(childMesh, {
        enabled: true, transport: childTransport, transportStateDir: childStateDir,
        hostId: childId, rootId: childId, identity: childIdentity,
      });
      await parentDirectory.start();
      await childDirectory.start();
      const now = Date.now();
      const rootRecord = (id: string, hostId: string, sessionId: string, placementRunId?: string): FabricParticipantRecord => ({
        format: 1, id, kind: "root", rootId: id,
        ownerHostId: hostId, ownerIdentityId: id,
        name: id, status: "idle", runner: "pi", transport: "host",
        capabilities: ["steer", "followUp", "fabric"], sessionId,
        ...(placementRunId ? { placementRunId } : {}),
        startedAt: now, updatedAt: now, controlProtocol: "v1",
      });
      await parentDirectory.publishBeforeLaunch(rootRecord(parentId, parentId, parentSession));
      await childDirectory.publishBeforeLaunch(rootRecord(childId, childId, childSession, runId));
      const parentView = parentDirectory.participantView;
      const childView = childDirectory.participantView;
      if (!parentView || !childView) throw new Error("NATS participant view did not start");
      const [childRoute, parentRoute] = await Promise.all([
        liveRoute(parentDirectory, childId), liveRoute(childDirectory, parentId),
      ]);
      const makeControl = (
        mesh: MeshStore,
        identity: MeshIdentity,
        transport: FabricTransport,
        directory: ParticipantDirectory,
        machine: "machine-a" | "machine-b",
      ): FabricControlPlane => new FabricControlPlane(mesh, identity, {
        enabled: true, hostId: identity.id, transport, participants: directory.participantView!,
        ownerIncarnation: directory.ownerIncarnation, controlIncarnationFence: "enforce",
        expectedNatsInboxStream: `FABRIC_IN_${machine}`, relayOperatorMachine: PATH_B_OPERATOR_MACHINE,
        canConsumeMesh: () => directory.canConsumeMesh(), pollMs: 20, acknowledgementTimeoutMs: 5_000,
      });
      parentControl = makeControl(parentMesh, parentIdentity, parentTransport, parentDirectory, "machine-a");
      childControl = makeControl(childMesh, childIdentity, childTransport, childDirectory, "machine-b");
      const autoReturn = sandboxAutoReturnForChildRoot({
        runId, spawner: { id: parentId, kind: "main" }, isRoot: true,
      });
      if (!autoReturn) throw new Error("valid sandbox root and bound spawner did not arm auto-return");
      let taskAccepted = false;
      let returnedResult = false;
      parentControl.start((command, from, _signal, verification) => {
        if (command.operation === "followUp" && verification && command.message) {
          returnedResult = captureSandboxFinalAnswer(from, verification, command.message);
        }
        return { accepted: true, messageId: command.commandId };
      });
      childControl.start((command, from, _signal, verification) => {
        if (command.operation === "followUp" && verification && command.message) {
          taskAccepted = autoReturn.receiveTask({
            message: command.message,
            provenance: fabricTurnProvenance(from, "followUp", verification),
          });
        }
        return { accepted: true, messageId: command.commandId };
      });
      const sandboxParticipant = {
        ...rootRecord(childId, childRoute.ownerHostId, childSession, runId),
        ownerIdentityId: childRoute.ownerIdentityId,
        ownerIncarnation: childRoute.ownerIncarnation,
        interactive: true,
        local: false,
        stale: false,
      };
      const sandboxParticipants: Pick<FabricParticipantSource, "get" | "list"> = {
        get: id => id === childId ? sandboxParticipant : undefined,
        list: () => [sandboxParticipant],
      };
      const sandboxMesh = createSandboxFabricMesh({
        transport: parentTransport,
        participants: sandboxParticipants,
        participantView: parentView,
        control: parentControl,
        canSend: () => parentDirectory!.canConsumeMesh(),
      });
      const child = await sandboxMesh.waitForPresence(runId, { timeoutMs: 10_000 });
      const resultWait = sandboxMesh.awaitResult(child.id, runId, Date.now() + 10_000, resultAbort.signal);
      void resultWait.catch(() => undefined);
      await sandboxMesh.deliverTask(child.id, sandboxTaskMessage(runId, "complete the real NATS task"));
      expect(taskAccepted).toBe(true);
      autoReturn.recordAssistantText("result returned over authenticated NATS");
      const returnRoute = parentRoute;
      if (!returnRoute.live) throw new Error("parent NATS return route is not live");
      await autoReturn.sendAutomaticResult("completed", message => childControl!.request(
        returnRoute.ownerHostId,
        parentId,
        "followUp",
        { ownerIncarnation: returnRoute.ownerIncarnation, message, triggerTurn: true },
        returnRoute.ownerIdentityId,
        { idempotencyKey: `sandbox-result-${runId}`, timeoutMs: 5_000 },
      ));

      await expect(resultWait).resolves.toMatchObject({
        runId, participantId: childId, status: "completed", text: "result returned over authenticated NATS",
      });
      expect(returnedResult).toBe(true);
    } finally {
      resultAbort.abort(new Error("real-NATS sandbox test finished"));
      await parentControl?.close().catch(() => undefined);
      await childControl?.close().catch(() => undefined);
      await parentDirectory?.close().catch(() => undefined);
      await childDirectory?.close().catch(() => undefined);
      parentMesh?.closeState();
      childMesh?.closeState();
      await parentTransport?.close().catch(() => undefined);
      await childTransport?.close().catch(() => undefined);
      await fixture.stop();
    }
  }, 45_000);
});
