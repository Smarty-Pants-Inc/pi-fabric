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

const identity: MeshIdentity = { id: "session:owner", kind: "main", name: "main", sessionId: "owner" };
const info = { id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
  cwd: "/repo/project", sessionId: "owner", startedAt: 1, updatedAt: 2, pendingMessages: false, local: true } as const;

const directory = (meshRoot: string, self = identity): ParticipantDirectory => new ParticipantDirectory(
  new MeshStore(meshRoot, 64 * 1024, 1000),
  { enabled: true, hostId: self.id, rootId: self.id, identity: self, heartbeatMs: 100, leaseMs: 10_000 },
);

describe("root participant session names", () => {
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
  const pi = { on: vi.fn(() => () => {}), events: { emit: vi.fn() },
    getThinkingLevel: () => "off", getSessionName: () => sessionName, sendMessage: vi.fn(),
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
  return { runtime, context, invoke, rename: (name: string) => { sessionName = name; } };
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
      await reviewer.runtime.mesh.publish({ topic: "fleet.work.root-name", kind: "ask", to: name,
        from: { id: "review-actor", name: "review-actor", kind: "actor" }, text: `hello ${name}` });
      // Only bypass the inbox's existing 60-second steer grace, not publication or routing.
      const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
      try {
        expect((await owner.runtime.nextRootInbox(held))?.events).toEqual(expect.arrayContaining([
          expect.objectContaining({ to: name, text: `hello ${name}` }),
        ]));
      } finally { clock.mockRestore(); }
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
