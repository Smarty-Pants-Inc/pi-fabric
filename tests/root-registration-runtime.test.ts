import fs from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { FabricState } from "../src/fabric-state.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { RootInbox } from "../src/topology/root-inbox.js";
import { RootRegistrationGuard } from "../src/topology/root-registration.js";

const directories: string[] = [];
const runtimes: FabricRuntimeState[] = [];
afterEach(async () => {
  for (const runtime of runtimes.splice(0)) await runtime.shutdown();
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of directories.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = () => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-private-root-runtime-"));
  directories.push(cwd);
  const meshRoot = path.join(cwd, "mesh");
  for (const key of ["PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_SESSION_ID", "PI_FABRIC_OWNER_HOST_ID", "PI_FABRIC_OWNER_IDENTITY_ID"]) vi.stubEnv(key, "");
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "profile"));
  vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  const handlers = new Map<string, (...args: any[]) => any>();
  let name = "fixture-root";
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    on: vi.fn((event, handler) => { handlers.set(event, handler); return () => { handlers.delete(event); }; }),
    getThinkingLevel: () => "off", getSessionName: () => name, sendMessage: vi.fn(), sendUserMessage: vi.fn(),
  } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: true, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { find: vi.fn(), getAvailable: () => [] },
    sessionManager: { getSessionId: () => "synthetic-new-session", getSessionFile: () => undefined, getBranch: () => [], getEntries: () => [], getLeafId: () => undefined },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: path.join(cwd, "unused.mjs"), worker: path.join(cwd, "unused.mjs"), residentHost: path.join(cwd, "unused.mjs"), skills: cwd } });
  runtimes.push(runtime);
  const config = normalizeFabricConfig({ mcp: { enabled: false, cache: { enabled: false } }, mesh: { enabled: true }, agents: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, records: { enabled: false } });
  const incumbent = new RootRegistrationGuard(new MeshStore(meshRoot, 64 * 1024, 100), {
    owner: { id: "synthetic-incumbent", pid: process.pid, host: os.hostname(), startTime: "" },
  });
  return { cwd, meshRoot, runtime, context, config, incumbent, handlers, pi, setName(value: string) { name = value; } };
};

describe("runtime duplicate-root admission", () => {
  it.each(["name", "native session", "root lineage", "actor persistence lineage"])("refuses duplicate %s before followUps, inbox, and participant publication", async kind => {
    const f = fixture();
    await f.incumbent.claim({ sessionId: "synthetic-incumbent-session", rootId: "session:synthetic-incumbent-session", fabricSessionId: "synthetic-incumbent-session", name: kind === "name" ? "fixture-root" : "other-name" });
    if (kind === "native session") f.context.sessionManager.getSessionId = () => "synthetic-incumbent-session";
    if (kind === "root lineage") vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", "session:synthetic-incumbent-session");
    if (kind === "actor persistence lineage") vi.stubEnv("PI_FABRIC_SESSION_ID", "synthetic-incumbent-session");
    const inbox = vi.spyOn(RootInbox.prototype, "start");
    const drain = vi.spyOn(MainAgentController.prototype, "attachFollowUpDrain");
    const publish = vi.spyOn(ParticipantDirectory.prototype, "start");
    await expect(f.runtime.initialize(f.context, f.config)).rejects.toThrow(/Duplicate live Fabric root/);
    expect(inbox).not.toHaveBeenCalled(); expect(drain).not.toHaveBeenCalled(); expect(publish).not.toHaveBeenCalled();
    expect(f.context.ui.notify).toHaveBeenCalledWith(expect.stringMatching(/Duplicate live Fabric root/), "error");
    await f.runtime.shutdown();
    await expect(f.incumbent.claim({ sessionId: "synthetic-incumbent-session", rootId: "session:synthetic-incumbent-session", fabricSessionId: "synthetic-incumbent-session", name: "fixture-root" })).resolves.toBeUndefined();
  });

  it("rechecks persisted names and loudly alerts on a conflicting live rename", async () => {
    const f = fixture();
    await f.incumbent.claim({ sessionId: "synthetic-other", rootId: "session:synthetic-other", fabricSessionId: "synthetic-other", name: "reserved-name" });
    const starts = vi.spyOn(RootInbox.prototype, "start");
    await f.runtime.initialize(f.context, f.config);
    const inbox = starts.mock.instances[0] as RootInbox;
    expect(inbox.names()).toContain("fixture-root");
    expect(f.runtime.participantInfos({ kinds: ["root"] })).toEqual([
      expect.objectContaining({ name: "fixture-root", sessionId: "synthetic-new-session" }),
    ]);
    f.setName("reserved-name");
    const warn = vi.spyOn(console, "error").mockImplementation(() => {});
    await f.handlers.get("session_info_changed")?.({}, f.context);
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/Duplicate live Fabric root/));
    expect(f.context.ui.notify).toHaveBeenCalledWith(expect.stringMatching(/Duplicate live Fabric root/), "error");
    expect(inbox.names()).toContain("fixture-root");
    expect(inbox.names()).not.toContain("reserved-name");
  });

  it("allows same-owner reinitialization and releases the claim on shutdown", async () => {
    const f = fixture();
    await f.runtime.initialize(f.context, f.config);
    expect(fs.readdirSync(path.join(f.meshRoot, "root-registrations"))).toHaveLength(1);
    await f.runtime.initialize(f.context, f.config);
    expect(fs.readdirSync(path.join(f.meshRoot, "root-registrations"))).toHaveLength(1);
    await f.runtime.shutdown();
    expect(fs.readdirSync(path.join(f.meshRoot, "root-registrations"))).toHaveLength(0);
    expect(f.handlers.has("session_info_changed")).toBe(false);
  });

  it("refuses an already-live native session published by a runtime predating root claims", async () => {
    const f = fixture();
    const id = "session:synthetic-new-session";
    const legacy = new ParticipantDirectory(new MeshStore(f.meshRoot, 64 * 1024, 100), {
      enabled: true, hostId: id, rootId: id,
      identity: { id, name: "main", kind: "main", sessionId: "synthetic-new-session" },
    });
    legacy.registerSource(() => [legacy.root({
      id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      sessionId: "synthetic-new-session", cwd: f.cwd, updatedAt: Date.now(), pendingMessages: false, local: true,
    })]);
    try {
      await legacy.start();
      await expect(f.runtime.initialize(f.context, f.config)).rejects.toThrow(/Duplicate live Fabric root/);
      await f.runtime.shutdown();
      expect(legacy.sessions().some(root => root.id === id)).toBe(true);
    } finally { await legacy.close(); }
  });

  it("refuses effective actor persistence lineage equal to a legacy root's native session", async () => {
    const f = fixture();
    const sessionId = "synthetic-legacy-session";
    const id = `session:${sessionId}`;
    const legacy = new ParticipantDirectory(new MeshStore(f.meshRoot, 64 * 1024, 100), {
      enabled: true, hostId: id, rootId: id, identity: { id, name: "main", kind: "main", sessionId },
    });
    legacy.registerSource(() => [legacy.root({ id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host", sessionId, cwd: f.cwd, updatedAt: Date.now(), pendingMessages: false, local: true })]);
    vi.stubEnv("PI_FABRIC_SESSION_ID", sessionId);
    try {
      await legacy.start();
      await expect(f.runtime.initialize(f.context, f.config)).rejects.toThrow(/Duplicate live Fabric root/);
    } finally { await legacy.close(); }
  });

  it("retains ownership continuously while same-owner initialize closes and reopens resources", async () => {
    const f = fixture();
    await f.runtime.initialize(f.context, f.config);
    const original = RootRegistrationGuard.prototype.claim;
    const contender = new RootRegistrationGuard(new MeshStore(f.meshRoot, 64 * 1024, 100), { owner: { id: "synthetic-competitor", pid: process.pid, host: os.hostname(), startTime: "" } });
    vi.spyOn(RootRegistrationGuard.prototype, "claim").mockImplementationOnce(async function(this: RootRegistrationGuard, identity) {
      expect(fs.readdirSync(path.join(f.meshRoot, "root-registrations"))).toHaveLength(1);
      await expect(original.call(contender, { sessionId: "synthetic-competitor", rootId: "session:synthetic-competitor", fabricSessionId: "synthetic-competitor", name: "fixture-root" })).rejects.toThrow(/Duplicate live Fabric root/);
      await original.call(this, identity);
    });
    await f.runtime.initialize(f.context, f.config);
  });

  it("native reload shutdown keeps a live reservation until its same owner resumes", async () => {
    const f = fixture();
    await f.runtime.initialize(f.context, f.config);
    await f.runtime.shutdown("reload");
    expect(fs.readdirSync(path.join(f.meshRoot, "root-registrations"))).toHaveLength(1);
    const contender = new RootRegistrationGuard(new MeshStore(f.meshRoot, 64 * 1024, 100), { owner: { id: "synthetic-competitor", pid: process.pid, host: os.hostname(), startTime: "" } });
    await expect(contender.claim({ sessionId: "synthetic-competitor", rootId: "session:synthetic-competitor", fabricSessionId: "synthetic-competitor", name: "fixture-root" })).rejects.toThrow(/Duplicate live Fabric root/);
    await f.runtime.initialize(f.context, f.config);
    await f.runtime.shutdown();
    expect(fs.readdirSync(path.join(f.meshRoot, "root-registrations"))).toHaveLength(0);
  });

  it("mesh-disabled roots never read, replay, or write the persistent source followUp journal", async () => {
    const f = fixture();
    f.config.mesh.enabled = false;
    const journal = path.join(f.meshRoot, "main-followups", "synthetic-new-session.json");
    fs.mkdirSync(path.dirname(journal), { recursive: true });
    const bytes = JSON.stringify({ version: 1, items: [{ id: "synthetic-inherited-message", from: { id: "synthetic-sender", kind: "main", name: "sender" }, message: "source root mailbox payload", sentAt: Date.now() - 10_000 }] });
    fs.writeFileSync(journal, bytes);
    const reads = vi.spyOn(fs, "readFileSync");
    const writes = vi.spyOn(fs, "writeFileSync");
    const removes = vi.spyOn(fs, "rmSync");
    const attach = vi.spyOn(MainAgentController.prototype, "attachFollowUpDrain");
    await f.runtime.initialize(f.context, f.config);
    expect(f.pi.sendMessage).not.toHaveBeenCalled();
    const main = attach.mock.instances[0] as MainAgentController;
    expect(main.deliverAgent({ from: { id: "synthetic-local", name: "Local", kind: "agent" }, message: "independent local followUp", delivery: "followUp" })).toMatchObject({ queued: true, routed: "main" });
    expect(f.pi.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ content: expect.stringContaining("independent local followUp") }), expect.anything());
    await f.runtime.shutdown();
    for (const spy of [reads, writes, removes]) expect(spy.mock.calls.filter(args => String(args[0]).startsWith(journal))).toHaveLength(0);
    reads.mockRestore(); writes.mockRestore(); removes.mockRestore();
    expect(fs.readFileSync(journal, "utf8")).toBe(bytes);
  });

  it.each(["copied-lead-name", "independent-fresh-name"])("warns about unknown legacy alias ownership while admitting named root %s", async name => {
    const f = fixture(); f.setName(name);
    const sessionId = "synthetic-unclaimed-legacy";
    const id = `session:${sessionId}`;
    const legacy = new ParticipantDirectory(new MeshStore(f.meshRoot, 64 * 1024, 100), {
      enabled: true, hostId: id, rootId: id, identity: { id, name: "main", kind: "main", sessionId },
    });
    legacy.registerSource(() => [legacy.root({ id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host", sessionId, cwd: f.cwd, updatedAt: Date.now(), pendingMessages: false, local: true })]);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await legacy.start();
      await f.runtime.initialize(f.context, f.config);
      expect(warning).toHaveBeenCalledWith(expect.stringMatching(/Cannot verify persisted name ownership.*synthetic-unclaimed-legacy/));
      expect(f.context.ui.notify).toHaveBeenCalledWith(expect.stringMatching(/Cannot verify persisted name ownership/), "warning");
      expect(f.runtime.initialized).toBe(true);
      expect(f.runtime.participantInfos({ kinds: ["root"] })).toContainEqual(expect.objectContaining({ name }));
      expect(await f.runtime.queueUserMessage("main", "independent root still usable", "followUp")).toMatchObject({ queued: true, routed: "main" });
      expect(f.pi.sendUserMessage).toHaveBeenCalledWith("independent root still usable", { deliverAs: "followUp" });
    } finally { await legacy.close(); }
  });

  it.each(["name", "native session", "root lineage", "actor lineage", "independent"])("checks fresh mirrored-root lease overlap: %s", async kind => {
    const f = fixture();
    const sessionId = "synthetic-mirrored-session";
    const id = `session:${sessionId}`;
    const remoteIdentity = { id: "synthetic-mirrored-owner", name: "Remote", kind: "main" as const, sessionId };
    const remoteHost = "synthetic-remote-host";
    const mesh = new MeshStore(f.meshRoot, 64 * 1024, 100);
    const stamp = Date.now();
    const record = new ParticipantDirectory(mesh, { enabled: true, hostId: remoteIdentity.id, rootId: id, identity: remoteIdentity }).root({
      id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host", sessionId,
      cwd: f.cwd, updatedAt: stamp, pendingMessages: false, local: true,
    }, true, "mirrored-live-name", "synthetic-remote-process-owner");
    const key = (prefix: string, value: string) => prefix + createHash("sha256").update(value).digest("hex");
    await mesh.put({ key: key("topology/hosts/", remoteIdentity.id), identity: remoteIdentity, value: {
      format: 1, id: remoteIdentity.id, rootId: id, identity: remoteIdentity, remoteHost,
      startedAt: stamp, updatedAt: stamp, expiresAt: stamp + 60_000,
    } });
    await mesh.put({ key: key("topology/participants/", id), identity: remoteIdentity, value: { ...record, remoteHost } });
    if (kind === "name") f.setName("mirrored-live-name");
    if (kind === "native session") f.context.sessionManager.getSessionId = () => sessionId;
    if (kind === "root lineage") vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", id);
    if (kind === "actor lineage") vi.stubEnv("PI_FABRIC_SESSION_ID", sessionId);
    if (kind === "independent") {
      await f.runtime.initialize(f.context, f.config);
      expect(f.runtime.initialized).toBe(true);
    } else await expect(f.runtime.initialize(f.context, f.config)).rejects.toThrow(/Duplicate live Fabric root/);
  });

  it("lazy replacement retirement releases a reload-retained claim without activating engines", async () => {
    const f = fixture(); f.config.mesh.announce = false;
    fs.mkdirSync(path.join(f.cwd, ".pi"), { recursive: true });
    fs.writeFileSync(path.join(f.cwd, ".pi", "fabric.json"), JSON.stringify(f.config));
    const first = new FabricState(f.pi, new CapturedToolCatalog(), { runtimeLoader: async () => ({ FabricRuntimeState }) });
    const load = vi.fn(async () => ({ FabricRuntimeState }));
    const replacement = new FabricState(f.pi, new CapturedToolCatalog(), { runtimeLoader: load });
    try {
      await first.bootstrap(f.context); await first.ensure(f.context);
      await first.shutdown("reload");
      await replacement.bootstrap(f.context);
      expect(replacement.shouldEagerlyActivate(f.context)).toBe(false);
      expect(load).not.toHaveBeenCalled();
      await replacement.shutdown();
      expect(load).not.toHaveBeenCalled();
      const observer = new ParticipantDirectory(new MeshStore(f.meshRoot, 64 * 1024, 100), { enabled: true, hostId: "synthetic-retirement-observer", rootId: "synthetic-retirement-observer", identity: { id: "synthetic-retirement-observer", name: "observer", kind: "main" } });
      expect(observer.list({ kinds: ["root"], fresh: true }).filter(root => root.id === "session:synthetic-new-session")).toHaveLength(0);
      await expect(f.incumbent.claim({ sessionId: "synthetic-retirement-successor", rootId: "session:synthetic-retirement-successor", fabricSessionId: "synthetic-retirement-successor", name: "fixture-root" })).resolves.toBeUndefined();
    } finally {
      await first.shutdown(); await replacement.shutdown(); await f.incumbent.close();
      // Same genuine synthetic owner cleans any retained file when this regression is RED.
      await f.runtime.initialize(f.context, f.config); await f.runtime.shutdown();
    }
  });

  it("does not create root claims with mesh disabled or inside a task participant", async () => {
    const f = fixture();
    f.config.mesh.enabled = false;
    await f.runtime.initialize(f.context, f.config);
    expect(fs.existsSync(path.join(f.meshRoot, "root-registrations"))).toBe(false);
    f.config.mesh.enabled = true;
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "synthetic-task-id");
    await f.runtime.initialize(f.context, f.config);
    expect(fs.existsSync(path.join(f.meshRoot, "root-registrations"))).toBe(false);
  });
});
