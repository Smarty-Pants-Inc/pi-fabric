import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";

const topic = "fabric.topology.root-collision";
const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
});
const temp = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-root-collision-"));
  roots.push(root);
  return root;
};
const setup = async (filesOnly: boolean) => {
  const meshRoot = path.join(temp(), "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 1000);
  const make = (sessionId: string, initialName: string, metadataSession = sessionId) => {
    let name = initialName;
    const identity: MeshIdentity = { id: `session:${sessionId}`, name: "main", kind: "main", sessionId };
    const reported = vi.fn();
    const options = { enabled: true, identity, hostId: identity.id, rootId: identity.id,
      heartbeatMs: 100, leaseMs: 10_000, reapDeadHosts: false as const, onRootCollision: reported };
    const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1000), options);
    directory.registerSource(() => [directory.root({ id: identity.id, name: "Main", kind: "main",
      status: "idle", runner: "pi", transport: "host", cwd: meshRoot, sessionId: metadataSession,
      startedAt: 1, updatedAt: Date.now(), pendingMessages: false, local: true }, true, name)]);
    directories.push(directory);
    return { directory, reported, rename: (next: string) => { name = next; } };
  };
  if (filesOnly) await mesh.put({ key: LIVENESS_POLICY_KEY,
    value: { version: 1, participants: "files", hostLeases: "files" },
    identity: { id: "policy", name: "policy", kind: "main" } });
  return { mesh, make };
};

describe.each([false, true])("live root collision advisory (filesOnly=%s)", filesOnly => {
  it("alerts on initial duplicate-name registration, with both IDs, once across heartbeats", async () => {
    const { mesh, make } = await setup(filesOnly);
    const owner = make("owner", "net-lead");
    const fork = make("fork", "net-lead");
    await owner.directory.refresh();
    await fork.directory.refresh();
    expect(fork.reported).toHaveBeenCalledOnce();
    expect(fork.reported).toHaveBeenCalledWith(expect.objectContaining({
      reason: "duplicate-name", name: "net-lead", ids: ["session:fork", "session:owner"],
    }));
    await fork.directory.refresh();
    await fork.directory.refresh();
    await vi.waitFor(() => expect(mesh.read({ topic, limit: 100 })).toHaveLength(1));
    expect(mesh.read({ topic, limit: 100 })[0]).toMatchObject({ kind: "alert", data: {
      reason: "duplicate-name", name: "net-lead", ids: ["session:fork", "session:owner"],
    } });
    expect(fork.reported).toHaveBeenCalledOnce();
    // Advisory only: retain the existing routing guard rather than silently renaming a live root.
    expect(fork.directory.sessions()).toHaveLength(2);
  });

  it("detects a heartbeat rename but ignores unnamed roots and withdrawn owners", async () => {
    const { make } = await setup(filesOnly);
    const owner = make("owner", "main");
    const fork = make("fork", "main");
    await owner.directory.refresh();
    await fork.directory.refresh();
    expect(fork.reported).not.toHaveBeenCalled();
    owner.rename("net-lead");
    fork.rename("other-lead");
    await owner.directory.refresh();
    await fork.directory.refresh();
    expect(fork.reported).not.toHaveBeenCalled();
    fork.rename("net-lead");
    await fork.directory.refresh();
    expect(fork.reported).toHaveBeenCalledOnce();
    await owner.directory.close();
    fork.rename("new-name");
    await fork.directory.refresh();
    fork.rename("net-lead");
    await fork.directory.refresh();
    expect(fork.reported).toHaveBeenCalledOnce();
  });

  it("alerts on duplicate live session metadata even with distinct display names", async () => {
    const { make } = await setup(filesOnly);
    const owner = make("owner", "net-lead", "shared-session");
    const fork = make("fork", "other-lead", "shared-session");
    await owner.directory.refresh();
    await fork.directory.refresh();
    expect(fork.reported).toHaveBeenCalledWith(expect.objectContaining({
      reason: "duplicate-session", sessionId: "shared-session", ids: ["session:fork", "session:owner"],
    }));
  });

  it("ignores expired peer leases even while their root records remain", async () => {
    const { make } = await setup(filesOnly);
    const owner = make("owner", "net-lead");
    const fork = make("fork", "net-lead");
    await owner.directory.refresh();
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 120_000);
    try {
      expect(fork.directory.list({ scope: "project", kinds: ["root"], includeStale: true, fresh: true }))
        .toEqual([expect.objectContaining({ id: "session:owner", stale: true })]);
      await fork.directory.refresh();
      expect(fork.reported).not.toHaveBeenCalled();
    } finally { clock.mockRestore(); }
  });

  it("does not let a failing advisory UI callback stop root publication", async () => {
    const { mesh, make } = await setup(filesOnly);
    const owner = make("owner", "net-lead");
    const fork = make("fork", "net-lead");
    fork.reported.mockImplementation(() => { throw new Error("UI disposed"); });
    await owner.directory.refresh();
    await expect(fork.directory.refresh()).resolves.toBeUndefined();
    expect(fork.reported).toHaveBeenCalledOnce();
    expect(fork.directory.sessions()).toHaveLength(2);
    await vi.waitFor(() => expect(mesh.read({ topic, limit: 100 })).toHaveLength(1));
  });

  it("does not alert when mesh publication is disabled", async () => {
    const { make } = await setup(filesOnly);
    const owner = make("owner", "net-lead");
    const fork = make("fork", "net-lead");
    fork.directory.options.enabled = false;
    await owner.directory.refresh();
    await fork.directory.refresh();
    expect(fork.reported).not.toHaveBeenCalled();
  });
});

it("surfaces a duplicate-root advisory in the runtime without starting a turn", async () => {
  const root = temp();
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: path.join(root, "mesh") }, agents: { enabled: false },
    residency: { enabled: false }, records: { enabled: false }, mcp: { enabled: false },
    memory: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
  const make = (sessionId: string) => {
    const notify = vi.fn();
    const sendMessage = vi.fn();
    const pi = { on: vi.fn(() => () => {}), events: { emit: vi.fn() }, getSessionName: () => "net-lead",
      getThinkingLevel: () => "off", sendMessage } as unknown as ExtensionAPI;
    const context = { cwd: root, mode: "rpc", hasUI: true, isProjectTrusted: () => true,
      isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getEntries: () => [],
        getBranch: () => [], getLeafId: () => null }, ui: { notify, setStatus: vi.fn() },
    } as unknown as ExtensionContext;
    return { runtime: new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
      extension: path.join(root, "unused.mjs"), worker: path.join(root, "unused-worker.mjs"),
      residentHost: path.join(root, "unused-resident.mjs"), skills: root,
    } }), context, notify, sendMessage };
  };
  const owner = make("owner");
  const fork = make("fork");
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  try {
    await owner.runtime.initialize(owner.context, config);
    await fork.runtime.initialize(fork.context, config);
    expect(fork.notify).toHaveBeenCalledWith(expect.stringContaining("Duplicate live Fabric root"), "warning");
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("net-lead"));
    expect(fork.sendMessage).not.toHaveBeenCalled();
  } finally {
    await fork.runtime.shutdown();
    await owner.runtime.shutdown();
  }
});
