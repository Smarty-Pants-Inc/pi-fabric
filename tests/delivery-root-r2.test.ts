import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { registerFabricCommand } from "../src/commands/fabric.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { FabricState } from "../src/fabric-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentDeliveryPrefix, RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { projectOf } from "../src/topology/project-identity.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import type { FabricUiController } from "../src/ui/controller.js";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.unstubAllEnvs();
});
const original = "session:22222222-2222-4222-8222-222222222222";
const successor = "session:33333333-3333-4333-8333-333333333333";
const identity = (id: string): MeshIdentity => ({ id, name: "main", kind: "main", sessionId: id.slice(8) });
const key = (prefix: string, id = original) => prefix + createHash("sha256").update(id).digest("hex");
const record = (id: string, cwd: string): FabricParticipantRecord => ({
  format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
  name: "main", status: "idle", runner: "pi", transport: "host", role: "project-agent", project: projectOf(cwd), cwd,
  sessionId: id.slice(8), capabilities: ["steer", "followUp", "fabric"], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1",
});
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r2-"));
  cleanups.push(async () => { fs.rmSync(root, { recursive: true, force: true }); });
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", path.join(root, "mesh"));
  vi.stubEnv("PI_FABRIC_ROLE", "project-agent");
  vi.stubEnv("PI_FABRIC_SESSION_ID", "");
  vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", "");
  vi.stubEnv("SMARTY_LEAD_SESSION", successor);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000, { readCacheMs: 0 });
  await mesh.put({ key: LIVENESS_POLICY_KEY, identity: identity(original), value: { version: 1, participants: "files" } });
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: original, rootId: original, identity: identity(original), reapDeadHosts: false });
  directory.registerSource(() => [record(original, root)]);
  cleanups.push(() => directory.close());
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: original, sessionId: original.slice(8), cwd: root, projectRoot: root, project: projectOf(root),
    meshRoot: mesh.root, actorRoot: path.join(mesh.root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50 },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot);
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const host = new ResidentHost(config, () => {});
  cleanups.push(() => host.close());
  await host.start();
  const lead = new ParticipantDirectory(mesh, { enabled: true, hostId: successor, rootId: successor, identity: identity(successor), reapDeadHosts: false });
  lead.registerSource(() => [record(successor, root)]);
  cleanups.push(() => lead.close());
  await lead.refresh();
  expect(host.participants.list({ scope: "project", kinds: ["root"], fresh: true }).map(p => p.id)).toContain(successor);
  return { root, mesh, directory, config, host };
};
const seedClosure = (mesh: MeshStore) => mesh.put({ key: key("topology/lineage-closures/"), identity: identity(original), value: {
  format: 1, rootId: original, ownerHostId: original, ownerIdentityId: original, closedAt: Date.now(),
} });
const send = (host: ResidentHost, text: string) => host.actors.onDeliver({
  actor: { id: "actor:supervisor", name: "supervisor", project: host.config.project } as Parameters<typeof host.actors.onDeliver>[0]["actor"],
  message: { id: text, actorId: "actor:supervisor", actorName: "supervisor", direction: "out", source: "actor", createdAt: Date.now(), text },
  delivery: "steer", triggerTurn: true,
});
const assertMailbox = async (mesh: MeshStore, total = 1) => {
  await vi.waitFor(() => expect(mesh.listAll("residency/deliveries/", { fresh: true })).toHaveLength(total));
  expect(mesh.listAll(residentDeliveryPrefix(original), { fresh: true })).toHaveLength(total);
  expect(mesh.listAll(residentDeliveryPrefix(successor), { fresh: true })).toHaveLength(0);
};
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(r => { resolve = r; });
  return { promise, resolve };
};
const onPlatform = async (platform: "native" | "win32", operation: () => Promise<void>): Promise<void> => {
  const original = Object.getOwnPropertyDescriptor(process, "platform")!;
  try {
    if (platform !== "native") Object.defineProperty(process, "platform", { ...original, value: platform });
    await operation();
  } finally { Object.defineProperty(process, "platform", original); }
};
const publishDuringAbsence = (mesh: MeshStore, root: string) => {
  const file = path.join(mesh.root, "participants", key("", original) + ".json");
  const stat = fs.statSync;
  let fired = false;
  const spy = vi.spyOn(fs, "statSync").mockImplementation((...args: Parameters<typeof fs.statSync>) => {
    if (String(args[0]) === file && !fired) {
      try { return stat(...args); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        fired = true;
        // The observer retains ENOENT while the resumed, file-only root becomes present.
        writeParticipantFile(mesh.root, { key: key("topology/participants/"), value: record(original, root), version: 2, updatedAt: Date.now(), updatedBy: identity(original) });
        throw error;
      }
    }
    return stat(...args);
  });
  return { spy, fired: () => fired };
};

describe("Security R2 S2 terminal lineage versus runtime disposal", () => {
  it("ordinary directory disposal never certifies lineage death", async () => {
    const { mesh, directory } = await fixture();
    await directory.refresh();
    await directory.close();
    expect(mesh.get(key("topology/lineage-closures/"), { fresh: true })).toBeUndefined();
    expect(directory.lineageAlive(original)).toBe(true);
  });

  it.each([
    { operation: "reload", fail: false }, { operation: "reload", fail: true }, { operation: "bootstrap", fail: true },
  ] as const)("$operation preserves creating mailbox and orphan lineage before publication (failure=$fail)", async ({ operation, fail }) => {
    const { root, mesh, host, config } = await fixture();
    fs.mkdirSync(path.join(root, ".pi"), { recursive: true });
    fs.writeFileSync(path.join(root, ".pi", "fabric.json"), JSON.stringify({
      fullCodeMode: true, mcp: { enabled: false, cache: { enabled: false } }, memory: { enabled: false },
      agents: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
      mesh: { enabled: true, actorScope: "project", actorPollMs: 50 },
    }));
    const unused = path.join(root, "unused.mjs"); fs.writeFileSync(unused, "export default {};");
    let command!: (text: string, context: ExtensionContext) => Promise<void>;
    const pi = {
      events: { emit: vi.fn(), on: vi.fn(() => () => {}) }, on: vi.fn(), getThinkingLevel: () => "off", sendMessage: vi.fn(),
      registerCommand: (_name: string, definition: { handler: typeof command }) => { command = definition.handler; },
    } as unknown as ExtensionAPI;
    const context = {
      cwd: root, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
      sessionManager: { getSessionId: () => original.slice(8), getSessionFile: () => undefined, getBranch: () => [], getEntries: () => [], getLeafId: () => undefined },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const capturedTools = new CapturedToolCatalog();
    const state = new FabricState(pi, capturedTools, { paths: { extension: unused, worker: unused, residentHost: unused, skills: root } });
    cleanups.push(() => state.shutdown("exit"));
    await state.bootstrap(context); await state.ensure(context);
    const actor = await state.actors.create({ name: "session-orphan", instructions: "Keep original lineage.", residency: "session" });
    const pid = process.pid, session = context.sessionManager.getSessionId();
    // Seed after the old runtime's startup sweep; only the replacement may prune.
    await new Promise<void>(resolve => setImmediate(resolve));
    await new Promise<void>(resolve => setImmediate(resolve));
    const expiredRun = path.join(config.actorRoot, actor.id, "runs", "expired-before-reload");
    fs.mkdirSync(expiredRun, { recursive: true });
    fs.writeFileSync(path.join(expiredRun, "status.json"), JSON.stringify({ status: "completed",
      transport: "process", sessionId: "2147483646", finishedAt: 1 }));
    registerFabricCommand(pi, { state, capturedTools, fabricUi: { stop: vi.fn() } as unknown as FabricUiController, applyFabricMode: vi.fn(), suspendToolCapture: vi.fn() });
    const registry = path.join(config.actorRoot, "actors.json");
    const entered = deferred(), release = deferred();
    const start = ParticipantDirectory.prototype.start;
    vi.spyOn(ParticipantDirectory.prototype, "start").mockImplementation(async function(this: ParticipantDirectory) {
      if (this.options.hostId === original) {
        entered.resolve(); await release.promise;
        if (fail) throw new Error("replacement publication failed");
      }
      return start.call(this);
    });
    if (fail) state.setActivationHook(() => { throw new Error("activation failed"); });
    const pending = (operation === "reload" ? command("reload", context) : state.bootstrap(context)).then(() => undefined, error => error);
    await entered.promise;
    expect(process.pid).toBe(pid); expect(context.sessionManager.getSessionId()).toBe(session);
    const next = new ActorManager(successor.slice(8), identity(successor), mesh, config.mesh, host.agents, () => {}, {
      actorRoot: config.actorRoot, persistent: true, rootId: successor, claimResidency: "session", project: config.project, role: "project-agent", adoptionGraceMs: 0,
      canManageActor: id => { const p = host.participants.get(id, Date.now(), { fresh: true }); return p ? p.ownerHostId === successor : undefined; },
      lineageAlive: id => host.participants.lineageAlive(id),
    });
    cleanups.push(() => next.close());
    const before = fs.readFileSync(registry, "utf8");
    let firstId = "";
    try {
      send(host, "during-replacement"); await assertMailbox(mesh);
      firstId = (mesh.listAll(residentDeliveryPrefix(original), { fresh: true })[0]!.value as { id: string }).id;
      next.listOwned(); await new Promise(r => setTimeout(r, 100));
      expect(next.owns(actor.id)).toBe(false); expect(next.status(actor.id).rootId).toBe(original);
      expect(fs.readFileSync(registry, "utf8")).toBe(before);
      expect(fs.existsSync(expiredRun)).toBe(true);
      expect(mesh.get(key("topology/lineage-closures/"), { fresh: true })).toBeUndefined();
    } finally { release.resolve(); await pending; }
    if (fail) expect(await pending).toBeInstanceOf(Error);
    else expect(await pending).toBeUndefined();
    // Replacement may already have admitted the first envelope to the SAME
    // Main's durable journal. That is not mailbox loss or cross-root delivery.
    const followups = path.join(mesh.root, "main-followups");
    const journalFile = path.join(followups, `${encodeURIComponent(session)}.json`);
    const readJournal = () => fs.existsSync(journalFile) ? fs.readFileSync(journalFile, "utf8") : "";
    const retained = mesh.listAll(residentDeliveryPrefix(original), { fresh: true }).some(e => (e.value as { id: string }).id === firstId);
    expect(retained || readJournal().includes(firstId)).toBe(true);
    send(host, "after-replacement");
    // Windows can admit/delete an envelope between observer polls. Yield past
    // the drainer's cache window and check same-root durable custody, not a
    // transient mailbox snapshot (successful publication must permit delivery).
    if (!fail) {
      await new Promise<void>(resolve => setImmediate(resolve));
      await new Promise(resolve => setTimeout(resolve, 2_200));
    }
    await vi.waitFor(() => {
      const retained = mesh.listAll(residentDeliveryPrefix(original), { fresh: true })
        .some(e => (e.value as { message: string }).message === "after-replacement");
      const journal = readJournal();
      const admitted = journal ? (JSON.parse(journal) as { items: { message: string; deliveryId?: string }[] }).items
        .some(item => item.message === "after-replacement" && item.deliveryId?.startsWith(`resident:${original}:`)) : false;
      expect(retained || admitted).toBe(true);
      if (!fail) expect(admitted).toBe(true); // Exercise the previously missed destination.
    });
    if (!fail) await vi.waitFor(() => expect(fs.existsSync(expiredRun)).toBe(false));
    else expect(fs.existsSync(expiredRun)).toBe(true);
    expect(mesh.listAll(residentDeliveryPrefix(successor), { fresh: true })).toHaveLength(0);
    expect(host.participants.lineageAlive(original)).toBe(true);
    expect(mesh.get(key("topology/lineage-closures/"), { fresh: true })).toBeUndefined();
    expect(next.owns(actor.id)).toBe(false); expect(next.status(actor.id).rootId).toBe(original);
  }, 30_000);
});

describe("Security R2 S3 file-only resume interleavings", () => {
  it.each(["native", "win32"] as const)("keeps exact-owner custody when the root resumes before mesh handoff (%s)", async platform => onPlatform(platform, async () => {
    const { mesh, directory, host, config } = await fixture();
    await seedClosure(mesh);
    const put = host.mesh.put.bind(host.mesh);
    let resumed = false, retainedBeforeHandoff = false;
    const handoff = vi.spyOn(host.mesh, "put").mockImplementation(async request => {
      if (request.key.startsWith("residency/deliveries/")) {
        const outbox = path.join(config.residencyRoot, "delivery-outbox");
        retainedBeforeHandoff = fs.readdirSync(outbox).some(file => {
          const record = JSON.parse(fs.readFileSync(path.join(outbox, file), "utf8"));
          return record.rootId === original && record.message === "waited-resume";
        });
        await directory.refresh(); resumed = true;
      }
      return put(request);
    });
    try {
      send(host, "waited-resume"); await assertMailbox(mesh);
      expect(retainedBeforeHandoff).toBe(true); expect(resumed).toBe(true);
      expect(directory.lineageAlive(original)).toBe(true);
    } finally { handoff.mockRestore(); }
  }));

  it.each(["native", "win32"] as const)("host close joins exact-owner handoff and retains its outbox on failure (%s)", async platform => onPlatform(platform, async () => {
    const { config, host } = await fixture();
    const entered = deferred(), release = deferred();
    vi.spyOn(host.mesh, "put").mockImplementation(async () => {
      entered.resolve(); await release.promise; throw new Error("custody unavailable");
    });
    send(host, "closing-custody");
    await entered.promise;
    let closed = false;
    const closing = host.close().then(() => { closed = true; });
    try {
      await new Promise(r => setTimeout(r, 50)); expect(closed).toBe(false);
    } finally { release.resolve(); await closing; }
    const outbox = path.join(config.residencyRoot, "delivery-outbox");
    const items = fs.readdirSync(outbox).map(file => JSON.parse(fs.readFileSync(path.join(outbox, file), "utf8")));
    expect(items).toEqual([expect.objectContaining({ rootId: original, message: "closing-custody", from: expect.objectContaining({ kind: "actor" }) })]);
  }));

  it.each(["native", "win32"] as const)("actor delivery ignores obsolete successor-custody selection faults (%s)", async platform => onPlatform(platform, async () => {
    const { mesh, host } = await fixture();
    await seedClosure(mesh);
    const fence = vi.spyOn(host.mesh, "exclusive").mockRejectedValueOnce(new Error("custody unavailable"));
    try { send(host, "unknown-custody"); await assertMailbox(mesh); expect(fence).not.toHaveBeenCalled(); }
    finally { fence.mockRestore(); }
  }));

  it.each(["session", "durable"] as const)("retains registry custody while %s adoption waits for the resumed root's mesh fence", async residency => {
    const { mesh, directory, host, config } = await fixture();
    const actor = await host.actors.create({ name: "fenced-orphan", instructions: "Keep original lineage.", residency });
    await host.close(); await seedClosure(mesh);
    const registry = path.join(config.actorRoot, "actors.json"), before = fs.readFileSync(registry, "utf8");
    const exclusive = mesh.exclusive.bind(mesh);
    let resumed = false, registryHeld = false, meshHeld = false;
    const fence = vi.spyOn(mesh, "exclusive").mockImplementation(async operation => {
      registryHeld = fs.existsSync(path.join(registry + ".lock", "owner"));
      await directory.refresh(); resumed = true;
      return exclusive(() => { meshHeld = fs.existsSync(path.join(mesh.root, ".lock", "owner")); return operation(); });
    });
    const next = new ActorManager(successor.slice(8), identity(successor), mesh, config.mesh, host.agents, () => {}, {
      actorRoot: config.actorRoot, persistent: true, rootId: successor, claimResidency: residency, project: config.project, role: "project-agent", adoptionGraceMs: 0,
      canManageActor: () => undefined, lineageAlive: id => host.participants.lineageAlive(id),
    });
    cleanups.push(() => next.close());
    try {
      next.listOwned(); await vi.waitFor(() => expect(resumed).toBe(true));
      await vi.waitFor(() => expect(meshHeld).toBe(true));
      expect(registryHeld).toBe(true);
      expect(next.owns(actor.id)).toBe(false); expect(next.status(actor.id).rootId).toBe(original);
      expect(fs.readFileSync(registry, "utf8")).toBe(before);
    } finally { fence.mockRestore(); }
  });

  it("invalidates the previous close receipt before any resumed root file publication", async () => {
    const { root, mesh, directory } = await fixture();
    await seedClosure(mesh);
    const write = fs.renameSync;
    let checked = false, invalidated = false;
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === path.join(mesh.root, "participants", key("") + ".json")) {
        checked = true;
        invalidated = mesh.get(key("topology/lineage-closures/"), { fresh: true }) === undefined;
      }
      return write(from, to);
    });
    try { await directory.refresh(); } finally { spy.mockRestore(); }
    expect(checked).toBe(true); expect(invalidated).toBe(true); expect(directory.lineageAlive(original)).toBe(true);
    expect(fs.existsSync(path.join(root, "mesh", "participants", key("") + ".json"))).toBe(true);
  });

  it("failed receipt invalidation cannot activate or publish a resumed root", async () => {
    const { mesh, directory } = await fixture();
    await seedClosure(mesh);
    vi.spyOn(mesh, "delete").mockRejectedValueOnce(new Error("invalidation failed"));
    await expect(directory.refresh()).rejects.toThrow("invalidation failed");
    expect(fs.existsSync(path.join(mesh.root, "participants", key("") + ".json"))).toBe(false);
  });

  it.each(["native", "win32"] as const)("keeps exact-owner delivery when a resumed file appears before publication, without inference (%s)", async platform => onPlatform(platform, async () => {
    const { root, mesh, host } = await fixture();
    await seedClosure(mesh);
    const lineage = vi.spyOn(host.participants, "lineageAlive");
    const put = host.mesh.put.bind(host.mesh);
    let published = false;
    const handoff = vi.spyOn(host.mesh, "put").mockImplementation(async request => {
      if (request.key.startsWith("residency/deliveries/")) {
        writeParticipantFile(mesh.root, { key: key("topology/participants/"), value: record(original, root),
          version: 2, updatedAt: Date.now(), updatedBy: identity(original) });
        published = true;
      }
      return put(request);
    });
    try {
      send(host, "resume-directive"); await assertMailbox(mesh);
      expect(published).toBe(true); expect(lineage).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(mesh.root, "participants", key("") + ".json"))).toBe(true);
    } finally { handoff.mockRestore(); }
  }));

  it.each(["session", "durable"] as const)("keeps %s registry lineage when the resumed file appears during the locked adoption recheck", async residency => {
    const { root, mesh, host, config } = await fixture();
    const actor = await host.actors.create({ name: "resume-orphan", instructions: "Keep original lineage.", residency });
    await host.close(); await seedClosure(mesh);
    const registry = path.join(config.actorRoot, "actors.json"), before = fs.readFileSync(registry, "utf8");
    let race: ReturnType<typeof publishDuringAbsence> | undefined;
    const lock = ActorRegistryStore.prototype.withLock;
    const lockSpy = vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(async function<T>(this: ActorRegistryStore, operation: () => T): Promise<T> {
      // Preflight has already selected the orphan; inject only after taking the registry fence.
      return lock.call(this, () => { race ??= publishDuringAbsence(mesh, root); return operation(); }) as Promise<T>;
    });
    const next = new ActorManager(successor.slice(8), identity(successor), mesh, config.mesh, host.agents, () => {}, {
      actorRoot: config.actorRoot, persistent: true, rootId: successor, claimResidency: residency, project: config.project, role: "project-agent", adoptionGraceMs: 0,
      canManageActor: () => undefined, lineageAlive: id => host.participants.lineageAlive(id),
    });
    cleanups.push(() => next.close());
    try {
      next.listOwned(); await vi.waitFor(() => expect(race?.fired()).toBe(true));
      await new Promise(r => setTimeout(r, 100));
      expect(next.owns(actor.id)).toBe(false); expect(next.status(actor.id).rootId).toBe(original);
      expect(fs.readFileSync(registry, "utf8")).toBe(before);
    } finally { race?.spy.mockRestore(); lockSpy.mockRestore(); }
  });
});
