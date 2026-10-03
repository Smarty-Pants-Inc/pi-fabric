import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";

const identity: MeshIdentity = { id: "session:owner", kind: "main", name: "main", sessionId: "owner" };
const info = { id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
  cwd: "/repo/project", sessionId: "owner", startedAt: 1, updatedAt: 2, pendingMessages: false, local: true } as const;

const directory = (meshRoot: string, self = identity): ParticipantDirectory => new ParticipantDirectory(
  new MeshStore(meshRoot, 64 * 1024, 1000),
  { enabled: true, hostId: self.id, rootId: self.id, identity: self, heartbeatMs: 100, leaseMs: 10_000 },
);

describe("root participant session names", () => {
  describe.each([false, true])("launch-name roster (mesh enabled=%s)", (enabled) => {
    it.each([
      [undefined, undefined, "main"],
      [undefined, "  explicit-lead  ", "explicit-lead"],
      ["  fabric-v2  ", undefined, "main"],
      ["_lead", undefined, "_lead"], ["-lead", undefined, "-lead"],
      ["a".repeat(61), undefined, "a".repeat(61)],
      ["a".repeat(64), undefined, "a".repeat(64)],
      ["a".repeat(65), "explicit-lead", "explicit-lead"],
      [".lead", "explicit-lead", "explicit-lead"],
      ["Lead One", "explicit-lead", "explicit-lead"],
      ["fabric-v2", "explicit-lead", "fabric-v2"],
      ["bad/name", "  explicit-lead  ", "explicit-lead"],
      ["fabric-v2@x", "bad/name", "main"],
    ] as const)("filters agent=%j Pi name=%j as %j, preserving role metadata", async (agentName, sessionName, expected) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-launch-roster-"));
      vi.stubEnv("SMARTY_AGENT_NAME", agentName);
      vi.stubEnv("SMARTY_ROLE", "project-agent@x");
      vi.stubEnv("PI_FABRIC_ROLE", undefined);
      const owner = new ParticipantDirectory(new MeshStore(root, 64 * 1024, 1000), {
        enabled, hostId: identity.id, rootId: identity.id, identity,
      });
      const record = owner.root(info, true, sessionName);
      owner.registerSource(() => [record]);
      try {
        await owner.start();
        expect(record).toMatchObject({ name: expected, role: "project-agent", id: identity.id,
          rootId: identity.id, ownerIdentityId: identity.id });
        expect(owner.list({ name: expected, kinds: ["root"] }))
          .toEqual([expect.objectContaining({ name: expected, id: identity.id })]);
        expect(owner.list({ name: "project-agent", kinds: ["root"] })).toEqual([]);
        expect(owner.list({ name: expected.toUpperCase(), kinds: ["root"] })).toEqual([]);
      } finally {
        await owner.close(); vi.unstubAllEnvs();
        fs.rmSync(root, { recursive: true, force: true });
      }
    });
  });

  it.each([
    [undefined, "main"], ["", "main"], ["  \t ", "main"],
    ["  lucky-ios-lead  ", "lucky-ios-lead"], ["Lead 1_test.ok", "Lead 1_test.ok"],
    ["a".repeat(60), "a".repeat(60)], ["a".repeat(61), "main"],
    ["bad/name", "main"], ["_lead", "main"], ["lead\nother", "main"],
    ["lead\tother", "main"], ["lead@forge", "main"], ["🚀lead", "main"],
  ])("publishes session name %j as %j without changing identity", (sessionName, expected) => {
    const owner = directory(path.join(os.tmpdir(), "unused-root-name"));
    const original = owner.root(info);
    expect(owner.root(info, true, sessionName)).toEqual({ ...original, name: expected });
    expect(owner.root(info, false, sessionName)).toMatchObject({ name: expected, kind: "root",
      id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      interactive: false, capabilities: ["fabric"] });
  });

  it("republishes renames, clearing and invalid names on the existing heartbeat while retaining labels", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-root-name-presence-"));
    const owner = directory(path.join(root, "mesh"));
    const reader = directory(path.join(root, "mesh"), { id: "session:reader", kind: "main", name: "main" });
    let sessionName: string | undefined = "lucky-ios-lead";
    owner.registerSource(() => [owner.root(info, true, sessionName)]);
    try {
      await owner.start();
      const label = owner.self().label;
      expect(label).toMatch(/^PRO-/);
      const check = (name: string) => {
        expect(reader.get(identity.id)).toMatchObject({ name, kind: "root", rootId: identity.id,
          ownerHostId: identity.id, ownerIdentityId: identity.id, label });
        expect(reader.peers()).toEqual([expect.objectContaining({ id: identity.id, name: name === "main" ? label : name, label })]);
        expect(reader.mesh.get("sessions/owner")?.value).toMatchObject({ name: name === "main" ? label : name, label });
      };
      check(sessionName);
      for (const [next, expected] of [["renamed-lead", "renamed-lead"], [undefined, "main"],
        ["bad/name", "main"], ["a".repeat(61), "main"]] as const) {
        sessionName = next;
        await vi.waitFor(() => check(expected), { timeout: 3000 });
      }
      expect(owner.get("main")?.id).toBe(identity.id);
      expect(owner.options.identity).toEqual(identity);
    } finally {
      await reader.close(); await owner.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

const main = (cwd: string, sessionId: string, initialName?: string) => {
  let sessionName = initialName;
  const sendMessage = vi.fn();
  const pi = { on: vi.fn(() => () => {}), events: { emit: vi.fn() },
    getThinkingLevel: () => "off", getSessionName: () => sessionName, sendMessage,
  } as unknown as ExtensionAPI;
  const context = { cwd, mode: "rpc", hasUI: false, isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => ({ ok: true }) },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(cwd, "unused-extension.mjs"), worker: path.join(cwd, "unused-worker.mjs"),
    residentHost: path.join(cwd, "unused-resident.mjs"), skills: cwd,
  } });
  const invoke = (ref: string, args: Record<string, unknown> = {}) => runtime.registry.invoke(ref, args, {
    cwd, signal: undefined, parentToolCallId: "root-name-probe", nestedToolCallId: ref,
    extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 10_000,
  });
  return { runtime, context, invoke, sendMessage, rename: (name: string | undefined) => { sessionName = name; } };
};

it("lists the Pi-named Main through agents.peers/members and reaches its current name after a heartbeat rename", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-root-name-runtime-"));
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
    agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false },
  });
  const owner = main(root, "aaaaaaaa-0000-4000-8000-000000000001", "lucky-ios-lead");
  const reviewer = main(root, "bbbbbbbb-0000-4000-8000-000000000002");
  const ownerId = "session:aaaaaaaa-0000-4000-8000-000000000001";
  // A delivered batch is recorded before the following inbox read, as on a real Pi host.
  const held = { holdsBatch: () => true, holdsSteer: () => false };
  try {
    await owner.runtime.initialize(owner.context, config);
    await reviewer.runtime.initialize(reviewer.context, config);
    const check = async (name: string) => {
      expect(await reviewer.invoke("agents.peers")).toEqual([expect.objectContaining({ id: ownerId, name })]);
      expect(await reviewer.invoke("agents.members", { kinds: ["root"] })).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: ownerId, name, kind: "root", rootId: ownerId, ownerHostId: ownerId, ownerIdentityId: ownerId }),
      ]));
    };
    await check("lucky-ios-lead");
    expect(await reviewer.invoke("agents.self")).toMatchObject({ name: "main", kind: "root" });
    const label = owner.runtime.participantInfos().find(p => p.id === ownerId)?.label;
    const deliverByName = async (name: string) => {
      // Age the shadow, not the directory's live presence leases: recovery now validates
      // the same unambiguous current project roster as routing.
      const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() - 61_000);
      try {
        await reviewer.runtime.mesh.publish({ topic: "fleet.work.root-name", kind: "ask", to: name,
          from: { id: "review-actor", name: "review-actor", kind: "actor" }, text: `hello ${name}` });
      } finally { clock.mockRestore(); }
      expect((await owner.runtime.nextRootInbox(held))?.events).toEqual(expect.arrayContaining([
        expect.objectContaining({ to: name, text: `hello ${name}` }),
      ]));
    };
    await deliverByName("lucky-ios-lead");
    owner.rename("renamed-lead");
    await vi.waitFor(() => check("renamed-lead"), { timeout: 8000, interval: 100 });
    expect(owner.runtime.participantInfos().find(p => p.id === ownerId)?.label).toBe(label);
    await deliverByName("renamed-lead");
  } finally {
    await reviewer.runtime.shutdown(); await owner.runtime.shutdown();
    vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
}, 20_000);

// Separate runtimes, directories and control planes; only the mesh path is shared.
// Exercise both mixed-generation state publication and the fleet's file-only policy.
const acrossRoots = async (filesOnly: boolean, run: (roots: {
  owner: ReturnType<typeof main>; reviewer: ReturnType<typeof main>; duplicate: ReturnType<typeof main>;
  ownerId: string; reviewerId: string; duplicateId: string;
}) => Promise<void>) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cross-root-name-"));
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  const meshRoot = path.join(root, "mesh");
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
    agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false },
  });
  const ownerSession = "aaaaaaaa-0000-4000-8000-000000000001";
  const reviewerSession = "bbbbbbbb-0000-4000-8000-000000000002";
  const duplicateSession = "cccccccc-0000-4000-8000-000000000003";
  const owner = main(root, ownerSession, "lucky-ios-lead");
  const reviewer = main(root, reviewerSession);
  const duplicate = main(root, duplicateSession, "other-lead");
  const ownerId = `session:${ownerSession}`;
  const reviewerId = `session:${reviewerSession}`;
  const duplicateId = `session:${duplicateSession}`;
  try {
    if (filesOnly) await new MeshStore(meshRoot, 64 * 1024, 1000).put({
      key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files", hostLeases: "files" },
      identity: { id: ownerId, kind: "main", name: "main", sessionId: ownerSession },
    });
    await owner.runtime.initialize(owner.context, config);
    await reviewer.runtime.initialize(reviewer.context, config);
    await duplicate.runtime.initialize(duplicate.context, config);
    // Prove the file-only leg really has no state/legacy participant fallback.
    if (filesOnly) {
      expect(reviewer.runtime.mesh.listAll("topology/participants/", { fresh: true })).toEqual([]);
      expect(reviewer.runtime.mesh.listAll("sessions/", { fresh: true })).toEqual([]);
    } else {
      expect(reviewer.runtime.mesh.listAll("topology/participants/", { fresh: true })).toHaveLength(3);
    }
    await run({ owner, reviewer, duplicate, ownerId, reviewerId, duplicateId });
  } finally {
    await duplicate.runtime.shutdown(); await reviewer.runtime.shutdown(); await owner.runtime.shutdown();
    vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
};

const received = (target: ReturnType<typeof main>, senderId: string, text: string, delivery: string) => {
  expect(target.sendMessage).toHaveBeenLastCalledWith(expect.objectContaining({
    customType: "pi-fabric-agent-message", content: expect.stringContaining(text),
    details: expect.objectContaining({ from: expect.objectContaining({ id: senderId }), delivery }),
  }), expect.objectContaining({ deliverAs: delivery, triggerTurn: true }));
};

describe.each([false, true])("cross-root participant name routing (filesOnly=%s)", (filesOnly) => {
  it("discovers Pi names with role-only launch metadata, reports duplicates and resolves the new session after relaunch (#3860)", async () => {
    await acrossRoots(filesOnly, async ({ owner, reviewer, duplicate, ownerId, duplicateId, reviewerId }) => {
      reviewer.rename("reviewer");
      vi.stubEnv("SMARTY_ROLE", "project-agent@abcdef123456");
      vi.stubEnv("SMARTY_AGENT_NAME", undefined);
      owner.rename("fabric-v2");
      const members = () => reviewer.invoke("agents.members", { kinds: ["root"], name: "fabric-v2" });
      await vi.waitFor(async () => expect(await members()).toEqual([
        expect.objectContaining({ id: ownerId, name: "fabric-v2", sessionId: ownerId.slice(8) }),
      ]), { timeout: 8000, interval: 100 });
      expect(await reviewer.invoke("agents.members", { name: "absent" })).toEqual([]);
      expect(reviewer.runtime.participantInfos({ scope: "project", name: "fabric-v2" }))
        .toEqual([expect.objectContaining({ id: ownerId })]);
      expect(await reviewer.invoke("mesh.members", { kinds: ["root"], name: "fabric-v2" }))
        .toEqual([expect.objectContaining({ id: ownerId })]);
      await expect(reviewer.invoke("agents.followUp", { id: "fabric-v2", message: "role reply" }))
        .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
      received(owner, reviewerId, "role reply", "followUp");
      duplicate.rename("fabric-v2");
      await vi.waitFor(async () => expect(await members()).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: ownerId }), expect.objectContaining({ id: duplicateId }),
      ])), { timeout: 8000, interval: 100 });
      expect(await members()).toHaveLength(2);
      for (const action of ["followUp", "steer", "tell"]) {
        await expect(reviewer.invoke(`agents.${action}`, { id: "fabric-v2", message: "never choose newest" }))
          .rejects.toThrow(`Ambiguous Fabric participant: fabric-v2 (${[ownerId, duplicateId].sort().join(", ")}); use an exact id`);
      }
      expect(duplicate.sendMessage).not.toHaveBeenCalled();
      await duplicate.runtime.shutdown();
      const config = owner.runtime.config;
      await owner.runtime.shutdown();
      const next = main(owner.context.cwd, "dddddddd-0000-4000-8000-000000000004", "fabric-v2");
      try {
        await next.runtime.initialize(next.context, config);
        const nextId = "session:dddddddd-0000-4000-8000-000000000004";
        await vi.waitFor(async () => expect(await members()).toEqual([
          expect.objectContaining({ id: nextId, name: "fabric-v2", sessionId: nextId.slice(8) }),
        ]), { timeout: 8000, interval: 100 });
        await expect(reviewer.invoke("agents.followUp", { id: "fabric-v2", message: "relaunch reply" }))
          .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
        received(next, reviewerId, "relaunch reply", "followUp");
      } finally { await next.runtime.shutdown(); }
    });
  }, 30_000);
  it("delivers followUp/steer/tell by published name and forgets the old name after a heartbeat rename", async () => {
    await acrossRoots(filesOnly, async ({ owner, reviewer, ownerId, reviewerId }) => {
      for (const [action, targetKey, delivery] of [["followUp", "id", "followUp"],
        ["steer", "to", "steer"], ["tell", "to", "followUp"]] as const) {
        const text = `cross-root ${action}`;
        await expect(reviewer.invoke(`agents.${action}`, { [targetKey]: "lucky-ios-lead", message: text }))
          .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
        received(owner, reviewerId, text, delivery);
      }
      expect(owner.sendMessage).toHaveBeenCalledTimes(3);
      expect(reviewer.sendMessage).not.toHaveBeenCalled();
      owner.rename("renamed-lead");
      await vi.waitFor(async () => expect(await reviewer.invoke("agents.members", { kinds: ["root"] }))
        .toEqual(expect.arrayContaining([expect.objectContaining({ id: ownerId, name: "renamed-lead" })])),
      { timeout: 8000, interval: 100 });
      for (const action of ["followUp", "steer"] as const) {
        await expect(reviewer.invoke(`agents.${action}`, { id: "lucky-ios-lead", message: "must not arrive" }))
          .rejects.toThrow("Unknown Fabric participant: lucky-ios-lead");
        expect(owner.sendMessage).toHaveBeenCalledTimes(action === "followUp" ? 3 : 4);
        await expect(reviewer.invoke(`agents.${action}`, { to: "renamed-lead", message: `renamed ${action}` }))
          .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
        received(owner, reviewerId, `renamed ${action}`, action);
      }
      expect(reviewer.sendMessage).not.toHaveBeenCalled();
    });
  }, 30_000);

  describe.each(["tell", "steer", "followUp"] as const)("actor/root selectors for agents.%s", (action) => {
    it.each(["actor-only", "same-name", "actor-prefix", "root-only"] as const)("routes or refuses %s before publication", async (scenario) => {
      await acrossRoots(filesOnly, async ({ owner, reviewer, duplicate, ownerId, reviewerId }) => {
        // Keep the real ActorDirectory/ActorManager name and unique-prefix resolver.
        // Intercept only mailbox delivery so no worker is started by these routing tests.
        const actor = await reviewer.runtime.actors.create({
          name: scenario === "same-name" ? "lucky-ios-lead" : "lane-worker",
          instructions: "Routing regression; do not start a worker.", runner: "pi",
        });
        expect(actor.status).toBe("idle");
        const tell = vi.spyOn(reviewer.runtime.actors, "tell")
          .mockReturnValue({ queued: true, messageId: "actor-mailbox" });
        const selector = scenario === "actor-prefix" ? actor.id.slice(0, 8)
          : scenario === "actor-only" ? actor.name : "lucky-ios-lead";
        if (scenario === "actor-prefix") {
          owner.rename(selector);
          await vi.waitFor(async () => expect(await reviewer.invoke("agents.members", { kinds: ["root"] }))
            .toEqual(expect.arrayContaining([expect.objectContaining({ id: ownerId, name: selector })])),
          { timeout: 8000, interval: 100 });
        }
        expect(reviewer.runtime.actors.status(scenario === "root-only" ? actor.name : selector).id).toBe(actor.id);
        const publish = vi.spyOn(reviewer.runtime.mesh, "publish");
        const commandsBefore = reviewer.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 });
        const text = `${scenario} ${action}`;
        const result = reviewer.invoke(`agents.${action}`, { id: selector, message: text });
        if (scenario === "same-name" || scenario === "actor-prefix") {
          const failure = await result.catch((error: unknown) => error);
          expect(failure).toBeInstanceOf(Error);
          expect((failure as Error).message).toContain(`Ambiguous Fabric participant: ${selector}`);
          expect((failure as Error).message).toContain(actor.id);
          expect((failure as Error).message).toContain(ownerId);
          expect(publish).not.toHaveBeenCalled();
          expect(reviewer.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual(commandsBefore);
          expect(tell).not.toHaveBeenCalled();
          expect(owner.sendMessage).not.toHaveBeenCalled();
        } else if (scenario === "actor-only") {
          await expect(result).resolves.toMatchObject({ routed: "local", queued: true, messageId: "actor-mailbox" });
          expect(tell).toHaveBeenCalledExactlyOnceWith(actor.id, text, undefined, expect.any(Object));
          expect(publish).not.toHaveBeenCalled();
          expect(owner.sendMessage).not.toHaveBeenCalled();
        } else {
          await expect(result).resolves.toMatchObject({ routed: "mesh", acknowledged: true });
          received(owner, reviewerId, text, action === "tell" ? "followUp" : action);
          expect(tell).not.toHaveBeenCalled();
        }
        expect(reviewer.sendMessage).not.toHaveBeenCalled();
        expect(duplicate.sendMessage).not.toHaveBeenCalled();
      });
    }, 20_000);
  });

  it("refuses duplicate live root names with both ids, without publishing or delivering to either", async () => {
    await acrossRoots(filesOnly, async ({ owner, reviewer, duplicate, ownerId, duplicateId, reviewerId }) => {
      duplicate.rename("lucky-ios-lead");
      await vi.waitFor(async () => expect(await reviewer.invoke("agents.members", { kinds: ["root"] }))
        .toEqual(expect.arrayContaining([expect.objectContaining({ id: duplicateId, name: "lucky-ios-lead" })])),
      { timeout: 8000, interval: 100 });
      const commandsBefore = reviewer.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 });
      for (const action of ["followUp", "steer", "tell"] as const) {
        const failure = await reviewer.invoke(`agents.${action}`, { to: "lucky-ios-lead", message: "never guess" })
          .catch((error: unknown) => error);
        expect(failure).toBeInstanceOf(Error);
        expect((failure as Error).message).toContain("Ambiguous Fabric participant: lucky-ios-lead");
        expect((failure as Error).message).toContain(ownerId);
        expect((failure as Error).message).toContain(duplicateId);
      }
      // A root with the same name must not silently choose itself either.
      await expect(owner.invoke("agents.followUp", { id: "lucky-ios-lead", message: "not even self" }))
        .rejects.toThrow("Ambiguous Fabric participant: lucky-ios-lead");
      expect(reviewer.runtime.mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual(commandsBefore);
      expect(owner.sendMessage).not.toHaveBeenCalled();
      expect(duplicate.sendMessage).not.toHaveBeenCalled();
      expect(reviewer.sendMessage).not.toHaveBeenCalled();
      await expect(reviewer.invoke("agents.followUp", { id: ownerId, message: "exact id still works" }))
        .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
      received(owner, reviewerId, "exact id still works", "followUp");
      await duplicate.runtime.shutdown();
      await expect(reviewer.invoke("agents.followUp", { id: "lucky-ios-lead", message: "only live root" }))
        .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
      received(owner, reviewerId, "only live root", "followUp");
      expect(duplicate.sendMessage).not.toHaveBeenCalled();
    });
  }, 30_000);

  it("keeps an unnamed root reachable by exact id, bare session UUID and its local main alias", async () => {
    await acrossRoots(filesOnly, async ({ owner, reviewer, ownerId, reviewerId }) => {
      expect(await reviewer.invoke("agents.self")).toMatchObject({ id: reviewerId, name: "main" });
      for (const id of [reviewerId, reviewerId.slice("session:".length)]) {
        await expect(owner.invoke("agents.followUp", { id, message: `unnamed ${id}` }))
          .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
        received(reviewer, ownerId, `unnamed ${id}`, "followUp");
      }
      await expect(reviewer.invoke("agents.followUp", { id: "main", message: "local unnamed main" }))
        .resolves.toMatchObject({ routed: "main", queued: true });
      received(reviewer, reviewerId, "local unnamed main", "followUp");
      expect(owner.sendMessage).not.toHaveBeenCalled();
    });
  }, 30_000);
});
