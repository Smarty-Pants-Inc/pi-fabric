// Keep reason: PR #1684's recovery failure must not tear down Main's entire Fabric runtime.
// Main activation, actor publication, provider dispatch and control ACKs are real; only OS launch is replaced.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { FabricState } from "../src/fabric-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { residentGenerationErrorPath } from "../src/residency/generation-exit.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { RESIDENT_HOST_FORMAT, residentHostId, residentRoot, type ResidentHostOwner } from "../src/residency/protocol.js";
import { RuntimeStateBuiltins } from "../src/runtime-state-builtins.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";

vi.mock("../src/residency/file-lock.js", async original => ({
  ...await original<typeof import("../src/residency/file-lock.js")>(), kernelFenceAvailable: () => true,
}));
vi.mock("../src/agents/transports/process-utils.js", async original => ({
  ...await original<typeof import("../src/agents/transports/process-utils.js")>(), spawnDetached: vi.fn(),
}));

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const fixtures: Array<{ close(): Promise<void> }> = [];
afterEach(async () => {
  for (const fixture of fixtures.splice(0)) await fixture.close();
  vi.restoreAllMocks();
  vi.mocked(spawnDetached).mockReset();
  vi.unstubAllEnvs();
});

const fixture = () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-startup-recovery-")));
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  vi.stubEnv("SMARTY_ROLE", undefined);
  vi.stubEnv("SMARTY_AGENT", undefined);
  vi.stubEnv("HERDR_PANE_ID", undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
  vi.spyOn(os, "loadavg").mockReturnValue([0, 0, 0]);
  const warning = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
  const sessionId = randomUUID(), rootId = `session:${sessionId}`;
  const actorId = "a".repeat(32), hostId = residentHostId(rootId);
  const meshRoot = path.join(root, "mesh"), residencyRoot = residentRoot(meshRoot, rootId);
  const launcher = path.join(root, "runtime", "residency", "launcher.mjs");
  fs.mkdirSync(path.dirname(launcher), { recursive: true });
  fs.writeFileSync(launcher, "export default {};\n");
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: meshRoot, transport: "file", hostTransport: "file" },
    agents: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false },
  });
  fs.mkdirSync(path.join(root, ".pi"), { recursive: true });
  fs.writeFileSync(path.join(root, ".pi", "fabric.json"), JSON.stringify(config));
  // This retained durable definition is deliberately not owned/published by Main's session manager.
  const actorRoot = path.join(meshRoot, "actors", sessionId);
  fs.mkdirSync(actorRoot, { recursive: true });
  fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: [{
    id: actorId, name: "restored-actor", instructions: "Reply to asks", createdAt: Date.now(),
    rootId, residency: "durable", status: "idle", topics: [], runner: "pi",
  }] }));
  const pi = { events: { emit() {} }, getThinkingLevel: () => "off", getSessionName: () => "recovery-main",
    appendEntry() {}, sendMessage() {} } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "json", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getBranch: () => [], getLeafId: () => undefined, getEntries: () => [] },
    ui: { setStatus() {}, notify() {} },
  } as unknown as ExtensionContext;
  const main = new FabricState(pi, new CapturedToolCatalog(), { paths: {
    extension: launcher, worker: launcher, residentHost: launcher, skills: root,
  } });
  let client: ResidencyClient | undefined;
  const start = ResidencyClient.prototype.start;
  vi.spyOn(ResidencyClient.prototype, "start").mockImplementation(function (this: ResidencyClient) {
    // Shorten only the existing bounded wait; keep the real watchdog, startup and publication paths.
    this.options.startupTimeoutMs = 1_000;
    client = this;
    start.call(this);
  });
  const identity: MeshIdentity = { id: hostId, name: "resident", kind: "agent" };
  const residentMesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const directory = new ParticipantDirectory(residentMesh, {
    enabled: true, hostId, rootId, identity, reapDeadHosts: false,
  });
  let status = "stopping";
  directory.registerSource((): FabricParticipantRecord[] => [{
    format: 1, id: actorId, name: "restored-actor", kind: "actor", rootId,
    ownerHostId: hostId, ownerIdentityId: hostId, parentId: rootId, residency: "durable",
    runner: "pi", transport: "host", status, capabilities: status === "idle" ? ["ask"] : [],
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
  }]);
  const control = new FabricControlPlane(residentMesh, identity, {
    enabled: true, hostId, ownerIncarnation: directory.ownerIncarnation, pollMs: 20,
  });
  control.start(command => ({ accepted: true, result: {
    id: command.commandId, actorId: command.targetId, actorName: "restored-actor", direction: "out",
    source: "direct", createdAt: Date.now(), text: `ACK: ${command.message}`,
  } }));
  let published = false;
  const publish = async (nextStatus = "idle") => {
    status = nextStatus;
    if (!published) { await directory.start(); published = true; }
    else await directory.refreshPresence();
  };
  const owner: ResidentHostOwner = { format: RESIDENT_HOST_FORMAT, hostId, pid: process.pid,
    processStartTime: processStartTime(process.pid), token: randomUUID(), startedAt: Date.now(), readyAt: Date.now(),
    maintenanceReady: 1, releaseRoot: path.resolve(path.dirname(launcher), "../.."),
  };
  const ready = (launchToken?: string) => {
    fs.mkdirSync(residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(residencyRoot, "owner.json"), JSON.stringify({ ...owner, ...(launchToken ? { launchToken } : {}) }));
    fs.writeFileSync(path.join(residencyRoot, "maintenance-ready.json"), JSON.stringify({ token: owner.token }));
  };
  const closed = new Promise<void>(() => {});
  const handle = { pid: process.pid, closed, isAlive: async () => true, stop: vi.fn(async () => {}),
    lostContact() {}, waitForClose: () => closed } as unknown as Awaited<ReturnType<typeof spawnDetached>>;
  const invoke = (ref: string, args: Record<string, unknown> = {}) => main.registry.invoke(ref, args, {
    cwd: root, signal: AbortSignal.timeout(5_000), parentToolCallId: "startup-recovery", nestedToolCallId: ref,
    extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 50_000,
  });
  const usable = async () => {
    expect(main.initialized).toBe(true);
    const registry = main.registry;
    expect((await main.ensure(context)).current()).toBe(true);
    expect(main.registry).toBe(registry); // No failed activation/reinitialization on the next Fabric use.
    await expect(invoke("agents.main")).resolves.toMatchObject({ id: rootId, local: true });
    const event = await invoke("mesh.publish", { topic: "test.recovery", text: "Main still works" });
    await expect(invoke("mesh.read", { topic: "test.recovery" })).resolves.toContainEqual(event);
  };
  const f = { main, context, rootId, actorId, hostId, directory, owner, warning, handle, ready, publish, invoke, usable,
    get client() { return client; },
    async close() {
      await main.shutdown("reload");
      await control.close();
      await directory.close();
      residentMesh.closeState();
      // Preserve fixture evidence; no session/launch traces are deleted by this suite.
    },
  };
  fixtures.push(f);
  return f;
};

describe("Main's bounded resident startup recovery", () => {
  it("keeps providers usable after a failed host start, and the existing watchdog restores the actor", async () => {
    const f = fixture();
    vi.mocked(spawnDetached).mockImplementation(async (_file, args) => {
      const token = args[args.indexOf("--launch-token") + 1]!;
      const file = residentGenerationErrorPath(f.client!.options.config.residencyRoot, token);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, JSON.stringify({ format: 1, generation: token, writer: "host", pid: process.pid,
        error: "fixture resident startup refused", occurredAt: Date.now() }));
      return f.handle;
    });
    await f.main.initialize(f.context);
    expect(f.warning.mock.calls.flat().join("\n")).toContain("Resident startup recovery incomplete (Fabric resident host failed to start: fixture resident startup refused)");
    expect(f.handle.stop).toHaveBeenCalled();
    await f.usable();
    await expect(f.invoke("agents.members", { scope: "lineage", kinds: ["actor"] })).resolves.toEqual([]);

    // Do not ask/ensureActor/ensureHost to trigger repair: only the original watchdog's backoff may retry.
    vi.mocked(spawnDetached).mockImplementation(async (_file, args) => {
      f.ready(args[args.indexOf("--launch-token") + 1]);
      await f.publish();
      return f.handle;
    });
    await vi.waitFor(async () => {
      expect(await f.invoke("agents.members", { scope: "lineage", kinds: ["actor"] }))
        .toEqual(expect.arrayContaining([expect.objectContaining({ id: f.actorId, ownerHostId: f.hostId, stale: false })]));
    }, { timeout: 15_000, interval: 100 });
    await expect(f.invoke("agents.ask", { id: f.actorId, message: "after background recovery" }))
      .resolves.toMatchObject({ actorId: f.actorId, text: "ACK: after background recovery" });
    await f.usable();
    await f.main.shutdown("reload");
    await expect(f.client!.waitForStartupRecovery()).rejects.toThrow("client is closed");
  }, 25_000);

  it("continues activation when a healthy resident leaves a durable actor unpublished (e.g. parked)", async () => {
    const f = fixture();
    f.ready(); // Ready owner, but no actor source: the real publication loop must exhaust its bound.
    await f.main.initialize(f.context);
    expect(f.warning.mock.calls.flat().join("\n")).toContain(`Timed out publishing ask-capable durable Fabric actors from ${f.hostId}: ${f.actorId}`);
    expect(spawnDetached).not.toHaveBeenCalled(); // A parked actor is not permission to replace a healthy host.
    await f.usable();
    await expect(f.invoke("agents.members", { scope: "lineage", kinds: ["actor"] })).resolves.toEqual([]);
    await f.publish(); // Later ordinary publication is visible without reactivating Main.
    await expect(f.invoke("agents.ask", { id: f.actorId, message: "after publication" }))
      .resolves.toMatchObject({ actorId: f.actorId, text: "ACK: after publication" });
    expect(spawnDetached).not.toHaveBeenCalled();
  }, 10_000);

  it("still waits for ask-capable publication before provider admission and the first ordinary ask", async () => {
    const f = fixture();
    f.ready();
    await f.publish("stopping"); // Retained, routable owner presence is not an ask-capable actor.
    const checked = deferred();
    const get = ParticipantDirectory.prototype.get;
    vi.spyOn(ParticipantDirectory.prototype, "get").mockImplementation(function (this: ParticipantDirectory, ...args) {
      const result = get.apply(this, args);
      if (f.client?.options.participants === this && args[0] === f.actorId && !result?.capabilities.includes("ask")) checked.resolve();
      return result;
    });
    const install = RuntimeStateBuiltins.prototype.install;
    vi.spyOn(RuntimeStateBuiltins.prototype, "install").mockImplementation(function (this: RuntimeStateBuiltins, component) {
      if (component.definition.name === "fabric.provider.agents") {
        expect(f.client!.options.participants.get(f.actorId, undefined, { fresh: true }))
          .toMatchObject({ id: f.actorId, stale: false, capabilities: expect.arrayContaining(["ask"]) });
      }
      return install.call(this, component);
    });
    const initialized = f.main.initialize(f.context);
    void initialized.catch(() => {}); // Observe activation failure even if an ordering regression rejects before checked.
    await checked.promise;
    expect(f.main.initialized).toBe(false);
    expect(() => f.main.registry).toThrow("has not activated");
    await f.publish();
    await initialized;
    await expect(f.invoke("agents.ask", { id: f.actorId, message: "first ordinary ask" }))
      .resolves.toMatchObject({ actorId: f.actorId, text: "ACK: first ordinary ask" });
    expect(f.warning.mock.calls.flat().join("\n")).not.toContain("Resident startup recovery incomplete");
    expect(spawnDetached).not.toHaveBeenCalled();
  }, 10_000);
});
