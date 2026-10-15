// Keep: task-to-Main trust boundary; refusal must precede durable startup publication.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TaskEscalationTargetError } from "../src/agents/task-return-address.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { claimStartingHost } from "../src/topology/starting-host.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

const SPAWNER = "session:aaaaaaaa-1111-4111-8111-111111111111";
const TARGET = "session:bbbbbbbb-2222-4222-8222-222222222222";
const identity: MeshIdentity = { id: "task-child", name: "Task", kind: "agent" };
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const roots: string[] = [], planes: FabricControlPlane[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const plane of planes.splice(0)) await plane.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
});

const fixture = async (options: { spawner?: string; ancestors?: readonly string[]; allowlist?: readonly string[]; task?: boolean; presence?: "live" | "reloading" | undefined } = {}) => {
  // Bind every authority-bearing field explicitly: the test runner itself may be a child.
  vi.stubEnv("PI_FABRIC_TASK_PROCESS_CHILD", options.task === false ? "" : "1");
  vi.stubEnv("PI_FABRIC_PARENT_RUN", "authority-regression-run");
  vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
  vi.stubEnv("PI_FABRIC_SPAWNER_ID", options.spawner ?? SPAWNER);
  vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", SPAWNER);
  vi.stubEnv("PI_FABRIC_SPAWNER_SESSION_ID", (options.spawner ?? SPAWNER).slice(8));
  vi.stubEnv("PI_FABRIC_SPAWNER_CHAIN", JSON.stringify(options.ancestors ?? []));
  vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", JSON.stringify(options.allowlist ?? []));
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "starting-main-task-authority-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 1_000);
  const claim = await claimStartingHost({ meshRoot: root, cwd: root, id: TARGET, sessionId: TARGET.slice(8),
    lockProtocol: 1, stateBackend: "file" });
  expect(claim).toBeDefined();
  const plane = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id }); planes.push(plane);
  const queue = vi.spyOn(plane, "queueStartingFollowUp"); // Calls the real durable admission.
  const request = vi.spyOn(plane, "request");
  const record = { id: TARGET, rootId: TARGET, kind: "root", name: "startup-lead", local: false,
    status: options.presence === "reloading" ? "reloading" : "idle", stale: options.presence === "reloading",
    ownerHostId: TARGET, ownerIdentityId: TARGET, ownerIncarnation: "predecessor",
    capabilities: ["followUp", "steer"], controlProtocol: "v1" } as FabricParticipantInfo;
  const router = new AgentMessageRouter(
    { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0],
    { identity, status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); } } as unknown as Ports[1],
    { id: "session:task-host", local: true, matches: (id: string) => id === "session:task-host" } as unknown as Ports[2],
    { get: (id: string) => options.presence === "live" && id === TARGET ? record : undefined,
      list: () => options.presence ? [record] : [], lastKnown: () => undefined, scheduleRefresh: () => {} },
    plane, binding => binding,
  );
  return { mesh, router, queue, request };
};

const expectUnpublished = (mesh: MeshStore) => {
  expect(mesh.listAll("topology/starting-followups/", { fresh: true })).toEqual([]);
  expect(mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual([]);
  expect(mesh.read({ topic: "fabric.control.startup-inbox", limit: 100 })).toEqual([]);
};

describe("spawn-bound Main authority before startup inbox admission", () => {
  it.each([
    ["exact startup-only", TARGET, undefined],
    ["exact reloading predecessor", TARGET, "reloading"],
    ["UUID", TARGET.slice(8), "live"],
    ["published name", "startup-lead", "live"],
  ] as const)("refuses an unauthorized %s without a durable row or event", async (_label, selector, presence) => {
    const f = await fixture({ presence });
    await expect(f.router.routeMessage(selector, "must not publish", undefined, "followUp"))
      .rejects.toBeInstanceOf(TaskEscalationTargetError);
    expect(f.queue).not.toHaveBeenCalled();
    expect(f.request).not.toHaveBeenCalled();
    expectUnpublished(f.mesh);
  });

  it.each([
    ["spawner", { spawner: TARGET }, TARGET],
    ["task-local main", { spawner: TARGET }, "main"],
    ["ancestor", { ancestors: [TARGET] }, TARGET],
    ["exact allowlist", { allowlist: [TARGET] }, TARGET],
    ["ordinary peer", { task: false }, TARGET],
    ["reloading predecessor", { spawner: TARGET, presence: "reloading" as const }, TARGET],
    ["UUID allowlist", { allowlist: [TARGET], presence: "live" as const }, TARGET.slice(8)],
    ["named allowlist", { allowlist: [TARGET], presence: "live" as const }, "startup-lead"],
  ] as const)("preserves authorized %s durable startup followUp", async (_label, options, selector) => {
    const f = await fixture(options);
    // Ambient mutation after construction cannot enlarge or remove the bound address.
    vi.stubEnv("PI_FABRIC_TASK_ESCALATION_TARGETS", "[]");
    const receipt = await f.router.routeMessage(selector, "authorized startup", { unchanged: true }, "followUp",
      undefined, { idempotencyKey: "authority-allowed" });
    expect(receipt).toMatchObject({ queued: true, reason: "startup-inbox-durable", triggered: false });
    expect(f.queue).toHaveBeenCalledOnce();
    expect(f.queue.mock.calls[0]![0]).toBe(TARGET);
    expect(f.request).not.toHaveBeenCalled();
    const rows = f.mesh.listAll("topology/starting-followups/", { fresh: true });
    expect(rows).toHaveLength(1);
    expect(rows[0]!.value).toMatchObject({ event: { from: identity, data: {
      targetId: TARGET, triggerTurn: true, message: "authorized startup", data: { unchanged: true },
    } } });
    expect(f.mesh.read({ topic: "fabric.control.command", limit: 100 })).toHaveLength(1);
  });
});
