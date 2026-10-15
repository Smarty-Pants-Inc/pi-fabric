// Keep: #12785 real registry/mesh trust boundary and data-safety regression.
// All history-removal routes below are recorded/throwing seams, never executed.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SessionActorOrphans } from "../src/actors/session-orphans.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { registerMainInbox } from "../src/topology/stall-alarms.js";
import { readHostLeaseCurrent, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile, withParticipantFileTryLock } from "../src/topology/participant-files.js";
import { ownProcessIncarnation } from "../src/core/atomic-write.js";
import * as participantFiles from "../src/topology/participant-files.js";
import * as tempJanitor from "../src/mesh/temp-janitor.js";
import { deadHostRecords, reapDeadHostRecords, DIRECTORY_RETENTION_MS } from "../src/topology/host-reaper.js";

const hash = (id: string) => createHash("sha256").update(id).digest("hex");
const keyOf = (id: string) => `topology/participants/${hash(id)}`;
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { vi.restoreAllMocks(); for (const close of cleanups.splice(0).reverse()) await close(); });
const missingPid = () => {
  for (let pid = 2147483647; pid > 2147483627; pid--) {
    try { process.kill(pid, 0); } catch (e) { if ((e as NodeJS.ErrnoException).code === "ESRCH") return pid; }
  }
  throw new Error("No absent PID for fixture");
};
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "owner-root-gone-"));
  const mesh = new MeshStore(root, 65536, 100);
  const old: MeshIdentity = { id: "session:old", sessionId: "old", kind: "main", name: "same display" };
  const current: MeshIdentity = { id: "session:new", sessionId: "new", kind: "main", name: "same display" };
  const at = Date.now() - 300000;
  const id = "b".repeat(32);
  const store = new ActorRegistryStore(path.join(root, "actors", "old"));
  const actor = { id, name: "supervisor", rootId: old.id, project: "trusted-project", residency: "session",
    instructions: "Observe only", runner: "pi", status: "running", createdAt: at - 1000, updatedAt: at, messages: [] };
  store.write([actor]);
  const oldOwner = path.join(root, "main-followups", "old.owner.json");
  const newOwner = path.join(root, "main-followups", "new.owner.json");
  // This native receiver operation, not mesh.put, is the launch grant source.
  (registerMainInbox as (...args: unknown[]) => string)(root, old, "old", undefined, "native-lead", "trusted-project");
  const original = JSON.parse(fs.readFileSync(oldOwner, "utf8"));
  fs.writeFileSync(oldOwner, JSON.stringify({ ...original, pid: missingPid(), processStartedAt: "1" }));
  fs.utimesSync(oldOwner, new Date(at), new Date(at));
  (registerMainInbox as (...args: unknown[]) => string)(root, current, "new", undefined, "native-lead", "trusted-project");
  const lease: FabricHostLease = { id: old.id, rootId: old.id, identityId: old.id, updatedAt: at, expiresAt: at,
    writer: { pid: original.pid === process.pid ? missingPid() : original.pid, host: os.hostname(), startedAt: at,
      releaseSha: "test", lockProtocol: 1, stateBackend: "file" } };
  writeHostLease(root, lease);
  writeHostLease(root, { ...lease, id: current.id, rootId: current.id, identityId: current.id,
    updatedAt: Date.now(), expiresAt: Date.now() + 60000, writer: { ...lease.writer!, pid: process.pid } });
  const legacy = `actors/old/${id}`;
  const clock = vi.spyOn(Date, "now").mockReturnValue(at);
  try {
    await mesh.put({ identity: old, key: legacy, value: { ...actor, queued: 7, inFlightRun: { id: "run" }, preparing: { id: "prep" } } });
    await mesh.put({ identity: old, key: keyOf(id), value: { format: 1, id, kind: "actor", rootId: old.id,
      ownerHostId: old.id, ownerIdentityId: old.id, name: actor.name, runner: "pi", residency: "session", transport: "host",
      capabilities: ["steer", "followUp", "fabric", "stop"], status: "running", controlProtocol: "v1",
      startedAt: actor.createdAt, updatedAt: at, actorQueued: 7, actorRun: { id: "run", startedAt: at } } });
    writeParticipantFile(root, mesh.get(keyOf(id))!);
  } finally { clock.mockRestore(); }
  const history = path.join(root, "actors", "old", id, "session.jsonl");
  fs.mkdirSync(path.dirname(history), { recursive: true }); fs.writeFileSync(history, "private history sentinel\n");
  const repair = new SessionActorOrphans(mesh, current, "new", path.join(root, "actors"), () => true);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: current.id, rootId: current.id, identity: current,
    reapDeadHosts: false, terminalActor: entry => repair.validateTombstone(entry) });
  cleanups.push(async () => { await directory.close(); await repair.close(); fs.rmSync(root, { recursive: true, force: true }); });
  return { root, mesh, old, current, id, actor, store, lease, oldOwner, newOwner, legacy, history, repair, directory };
};

const observer = (f: Awaited<ReturnType<typeof fixture>>, sessionId: string) => {
  const identity: MeshIdentity = { ...f.current, id: `session:${sessionId}`, sessionId };
  registerMainInbox(f.root, identity, sessionId, undefined, "native-lead", "trusted-project");
  writeHostLease(f.root, { ...readHostLeaseCurrent(f.root, f.current.id)!, id: identity.id, rootId: identity.id,
    identityId: identity.id, updatedAt: Date.now(), expiresAt: Date.now() + 2 * DIRECTORY_RETENTION_MS });
  const repair = new SessionActorOrphans(f.mesh, identity, sessionId, path.join(f.root, "actors"), () => true);
  cleanups.push(() => repair.close());
  return repair;
};
const persisted = (f: Awaited<ReturnType<typeof fixture>>) => [f.mesh.get(f.legacy, { fresh: true })!,
  f.mesh.get(keyOf(f.id), { fresh: true })!,
  JSON.parse(fs.readFileSync(path.join(f.root, "participants", `${hash(f.id)}.json`), "utf8"))];

const invokeContext: FabricInvocationContext = { cwd: process.cwd(), signal: undefined, parentToolCallId: "record-only",
  nestedToolCallId: "record-only", extensionContext: {} as FabricInvocationContext["extensionContext"], update() {}, activity() {} };
const providerFor = (f: Awaited<ReturnType<typeof fixture>>) => {
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents,
    { runRoot: path.join(f.root, "runs"), hostId: f.current.id, workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
  const mainAgent: FabricMainAgentTarget = { id: f.current.id, local: true, interactive: true,
    matches: id => id === "main" || id === f.current.id,
    info: () => ({ id: f.current.id, name: "Main", kind: "main", runner: "pi", transport: "host", status: "idle",
      updatedAt: Date.now(), pendingMessages: false, local: true, sessionId: "new" }),
    deliverAgent: () => ({ queued: true, messageId: "record-only", routed: "main" }) };
  const actors = new ActorDirectory(["new", f.current, f.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60000 }, manager, () => {},
    { persistent: true, claimResidency: "session", project: "trusted-project", rootId: f.current.id, mainAgent, lineageAlive: () => true }],
    { project: path.join(f.root, "actors"), session: path.join(f.root, "actors", "new") }, "project");
  const forbidden = vi.fn((..._args: unknown[]): Promise<any> => { throw new Error("Recorded destructive intent, forbidden in local tests"); });
  vi.spyOn(ActorManager.prototype, "remove").mockImplementation(forbidden);
  const lifecycle = new LifecycleBroker(f.mesh, f.current, f.directory, { enabled: false, pollMs: 60000, maxReadEvents: 100 }, () => {});
  const provider = new AgentsProvider(manager, actors, new GlobalActorRegistry(f.root, 65536), mainAgent, f.directory, undefined, lifecycle,
    () => true, { removeActor: forbidden } as any);
  cleanups.push(() => provider.close());
  return { provider, actors, forbidden };
};

describe("persistent old-root settlement (#12785)", () => {
  it.each(["native", "win32"] as const)("repairs raw legacy, native state and participant file on %s; keeps history and host-exit restart reason", async platform => {
    const original = Object.getOwnPropertyDescriptor(process, "platform")!;
    if (platform === "win32") Object.defineProperty(process, "platform", { ...original, value: "win32" });
    try {
      const f = await fixture();
      await f.repair.reconcile();
      expect(f.store.records()[0]).toMatchObject({ status: "stopped", stopReason: "host-exit" });
      expect(f.mesh.get(f.legacy, { fresh: true })?.value).toMatchObject({ status: "stopped", stopReason: "owner-root-gone", capabilities: [] });
      expect(f.mesh.get(keyOf(f.id), { fresh: true })?.value).toMatchObject({ status: "stopped", capabilities: [] });
      const file = JSON.parse(fs.readFileSync(path.join(f.root, "participants", `${hash(f.id)}.json`), "utf8"));
      expect(file.value).toMatchObject({ status: "stopped", capabilities: [] });
      expect(file.value.actorRun).toBeUndefined(); expect(file.value.actorQueued).toBeUndefined();
      expect(fs.readFileSync(f.history, "utf8")).toBe("private history sentinel\n");
      await f.repair.reconcile();
      expect(f.directory.list({ includeStale: true, fresh: true }).find(p => p.id === f.id)).toMatchObject({ status: "stopped", capabilities: [] });
    } finally { Object.defineProperty(process, "platform", original); }
  });

  it("refuses an observer-forged tombstone for a LIVE owner; normal live ownership stays unchanged", async () => {
    const f = await fixture();
    writeHostLease(f.root, { ...f.lease, updatedAt: Date.now(), expiresAt: Date.now() + 60000,
      writer: { ...f.lease.writer!, pid: process.pid } });
    const running = f.mesh.get(keyOf(f.id))!;
    await f.mesh.put({ identity: f.current, key: keyOf(f.id), value: { ...(running.value as object), status: "stopped",
      stopReason: "owner-root-gone", ownerRootGoneAt: Date.now(), ownerRootGoneBy: f.current.id, capabilities: ["steer", "stop"] } });
    // An invalid observer envelope cannot shadow the authentic owner-written file.
    for (const fresh of [true, false]) expect(f.directory.list({ includeStale: true, fresh }).find(p => p.id === f.id))
      .toMatchObject({ status: "running", ownerIdentityId: f.old.id, capabilities: ["steer", "followUp", "fabric", "stop"] });
    expect(await f.repair.reconcile()).toBe(0);
    expect(await f.repair.retire(f.id, () => {})).toBeUndefined();
    expect(f.store.records()[0]!.status).toBe("running");
    expect(f.mesh.get(keyOf(f.id))?.updatedBy.id).toBe(f.current.id);
    expect(JSON.parse(fs.readFileSync(path.join(f.root, "participants", `${hash(f.id)}.json`), "utf8"))).toEqual({ format: 1, ...running });
  });

  it.each(["native", "win32"] as const)("ignores forged capabilities/transient/routing payloads on %s even on an authenticated terminal generation", async platform => {
    const original = Object.getOwnPropertyDescriptor(process, "platform")!;
    if (platform === "win32") Object.defineProperty(process, "platform", { ...original, value: "win32" });
    try {
      const f = await fixture(); await f.repair.reconcile();
      const entry = f.mesh.get(keyOf(f.id), { fresh: true })!;
      const poison = { ...(entry.value as object), capabilities: ["steer", "followUp", "fabric", "stop"],
        actorRun: { id: "fake", startedAt: Date.now() }, actorPreparing: { id: "fake" }, actorQueued: 99,
        inFlightRun: { id: "fake" }, preparing: { id: "fake" }, pendingMessages: true, remoteHost: "fake-router",
        name: "forged-display", retiredAt: Date.now(), retiredBy: "forged-retirer" };
      await f.mesh.put({ identity: f.current, key: entry.key, value: poison });
      writeParticipantFile(f.root, { ...f.mesh.get(entry.key)!, updatedAt: Date.now() + 1 });
      const view = f.directory.list({ includeStale: true, fresh: true }).find(p => p.id === f.id)!;
      expect(view).toMatchObject({ status: "stopped", capabilities: [], ownerIdentityId: f.old.id, name: f.actor.name });
      expect(view).not.toHaveProperty("retiredAt");
      expect(view).not.toHaveProperty("retiredBy");
      for (const field of ["actorRun", "actorPreparing", "actorQueued", "inFlightRun", "preparing", "pendingMessages", "remoteHost"]) {
        expect(view).not.toHaveProperty(field);
      }
      await f.repair.reconcile();
      expect(f.mesh.get(entry.key, { fresh: true })?.value).toMatchObject({ capabilities: [] });
      expect(f.mesh.get(entry.key)?.value).not.toHaveProperty("actorRun");
    } finally { Object.defineProperty(process, "platform", original); }
  });

  it("does not trust a public orphan marker or observer attribution without its private receipt", async () => {
    const f = await fixture(); const entry = f.mesh.get(keyOf(f.id))!;
    await f.mesh.put({ identity: f.current, key: entry.key, value: { ...(entry.value as object), status: "stopped",
      stopReason: "owner-root-gone", ownerRootGoneAt: Date.now(), ownerRootGoneBy: f.current.id, settledAt: Date.now(),
      sessionOrphan: { directorySettlement: { by: f.current.id, at: Date.now() } }, capabilities: [] } });
    expect(f.repair.validateTombstone(f.mesh.get(entry.key)!)).toBeUndefined();
    expect(f.directory.list({ includeStale: true, fresh: true }).find(p => p.id === f.id))
      .toMatchObject({ status: "running", ownerIdentityId: f.old.id });
  });

  it("rechecks uncached root proof when only the old lease changes, not the tombstone bytes", async () => {
    const f = await fixture(); await f.repair.reconcile();
    expect(f.directory.list({ includeStale: true }).find(p => p.id === f.id)?.status).toBe("stopped");
    writeHostLease(f.root, { ...f.lease, expiresAt: Date.now() + 60000, updatedAt: Date.now() });
    expect(f.directory.list({ includeStale: true }).find(p => p.id === f.id)).toBeUndefined();
    expect(f.repair.validateTombstone(f.mesh.get(f.legacy)!)).toBeUndefined();
  });

  it("keeps terminal history readable after the authenticating observer exits", async () => {
    const f = await fixture(); await f.repair.reconcile();
    const owner = JSON.parse(fs.readFileSync(f.newOwner, "utf8"));
    fs.writeFileSync(f.newOwner, JSON.stringify({ ...owner, pid: missingPid(), processStartedAt: "1", retired: true }));
    writeHostLease(f.root, { ...f.lease, id: f.current.id, rootId: f.current.id, identityId: f.current.id });
    expect(f.directory.list({ includeStale: true, fresh: true }).find(p => p.id === f.id)).toMatchObject({ status: "stopped", capabilities: [] });
    expect(fs.existsSync(path.join(f.root, "participants", `${hash(f.id)}.json`))).toBe(true);
  });

  it.each(["unexpired", "reload", "live", "fresh-inbox"] as const)("does not settle/retire a merely %s old owner", async mode => {
    const f = await fixture();
    if (mode === "unexpired") writeHostLease(f.root, { ...f.lease, expiresAt: Date.now() + 60000 });
    if (mode === "reload") writeHostLease(f.root, { ...f.lease, reloadUntil: Date.now() + 60000 });
    if (mode === "live") writeHostLease(f.root, { ...f.lease, writer: { ...f.lease.writer!, pid: process.pid } });
    if (mode === "fresh-inbox") fs.utimesSync(f.oldOwner, new Date(), new Date());
    expect(await f.repair.reconcile()).toBe(0);
    expect(await f.repair.retire(f.id, () => {})).toBeUndefined();
    expect(f.store.records()[0]!.status).toBe("running");
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "running" });
  });

  it.each(["old-lease", "current-launcher", "current-activation", "registry"] as const)("rechecks %s at the actual settlement commit boundary", async mode => {
    const f = await fixture();
    const batch = f.mesh.writeBatch.bind(f.mesh); let fired = false;
    vi.spyOn(f.mesh, "writeBatch").mockImplementation(input => batch(input.beforeCommit ? { ...input, beforeCommit: () => {
      if (!fired) {
        fired = true;
        if (mode === "old-lease") writeHostLease(f.root, { ...f.lease, expiresAt: Date.now() + 60000 });
        if (mode === "registry") f.store.write([{ ...f.store.records()[0], updatedAt: Date.now(), name: "successor" }]);
        if (mode === "current-launcher" || mode === "current-activation") {
          const owner = JSON.parse(fs.readFileSync(f.newOwner, "utf8"));
          fs.writeFileSync(f.newOwner, JSON.stringify({ ...owner,
            ...(mode === "current-launcher" ? { nativeLauncher: "different-lead" } : { activationId: "different-activation" }) }));
        }
      }
      input.beforeCommit!();
    } } : input));
    await f.repair.reconcile();
    expect(fired).toBe(true);
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "running" });
    expect(f.store.records()[0]!.sessionOrphan).not.toHaveProperty("directorySettlement");
  });

  it("does not wait on an actual publisher holding the actor participant key", async () => {
    const f = await fixture(); const incarnation = await ownProcessIncarnation();
    const batch = f.mesh.writeBatch.bind(f.mesh); let held = false;
    vi.spyOn(f.mesh, "writeBatch").mockImplementation(input => batch(input.beforeCommit ? { ...input, beforeCommit: () => {
      held = true;
      withParticipantFileTryLock(f.mesh, keyOf(f.id), incarnation, () => input.beforeCommit!());
    } } : input));
    await f.repair.reconcile();
    expect(held).toBe(true); expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "running" });
    vi.restoreAllMocks(); await f.repair.reconcile();
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "stopped" });
  });

  it("cannot downgrade a concurrent private retirement selected before asynchronous key preparation", async () => {
    const f = await fixture(); await f.repair.reconcile();
    const prepare = participantFiles.prepareParticipantFileLock; let fired = false;
    const retiredAt = Date.now();
    vi.spyOn(participantFiles, "prepareParticipantFileLock").mockImplementation(async (...args) => {
      await prepare(...args);
      if (!fired && args[1] === keyOf(f.id)) {
        fired = true;
        const row = f.store.records()[0]!, marker = row.sessionOrphan as any;
        // A concurrent authorized private outbox commit, not caller mesh metadata.
        f.store.write([{ ...row, sessionOrphan: { ...marker, directorySettlement: {
          ...marker.directorySettlement, retiredAt, retiredBy: f.current.id } } }]);
      }
    });
    await f.repair.reconcile();
    expect(fired).toBe(true);
    expect(f.store.records()[0]!.sessionOrphan).toHaveProperty("directorySettlement.retiredAt", retiredAt);
    vi.restoreAllMocks(); await f.repair.reconcile();
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ retiredAt, retiredBy: f.current.id });
  });

  it("uses the private intent as a retry outbox after a pre-mesh-commit failure, never a deletion", async () => {
    const f = await fixture(); const batch = f.mesh.writeBatch.bind(f.mesh); let failed = false;
    vi.spyOn(f.mesh, "writeBatch").mockImplementation(input => batch(input.beforeCommit ? { ...input, beforeCommit: () => {
      input.beforeCommit!();
      if (!failed) { failed = true; throw new Error("simulated crash after private intent"); }
    } } : input));
    await f.repair.reconcile();
    expect(f.store.records()[0]!.sessionOrphan).toHaveProperty("directorySettlement");
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "running" });
    vi.restoreAllMocks(); await f.repair.reconcile();
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "stopped" });
    expect(fs.readFileSync(f.history, "utf8")).toBe("private history sentinel\n");
  });

  it.each(["different", "missing-old", "display-only", "inherited"] as const)("fails closed on %s launcher retirement while same-project truth repair remains allowed", async mode => {
    const f = await fixture();
    const file = mode === "different" ? f.newOwner : f.oldOwner;
    const owner = JSON.parse(fs.readFileSync(file, "utf8"));
    if (mode === "different") owner.nativeLauncher = "other-native-lead";
    else delete owner.nativeLauncher;
    owner.name = "native-lead";
    fs.writeFileSync(file, JSON.stringify(owner));
    if (file === f.oldOwner) fs.utimesSync(file, new Date(f.lease.updatedAt), new Date(f.lease.updatedAt));
    if (mode === "inherited") f.store.write([{ ...f.actor, mainAgentId: f.current.id, task: { mainAgentId: f.current.id } }]);
    await f.repair.reconcile();
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "stopped" });
    await expect(f.repair.retire(f.id, () => {})).rejects.toThrow(/trusted native launcher/);
    expect(f.mesh.get(f.legacy)?.value).not.toHaveProperty("retiredAt");
  });

  it("public agents.remove retires idempotently, retaining mesh/file/private history and issuing ZERO destructive calls", async () => {
    const f = await fixture(); const { provider, forbidden } = providerFor(f);
    const operations: Array<{ kind: string; key: string }> = [];
    const batch = f.mesh.writeBatch.bind(f.mesh);
    vi.spyOn(f.mesh, "writeBatch").mockImplementation(input => batch({ ...input, prepare: view => {
      const ops = [...input.ops, ...(input.prepare?.(view) ?? [])];
      for (const op of ops) {
        if (op.key === f.legacy || op.key === keyOf(f.id)) {
          operations.push({ kind: op.kind, key: op.key });
          if (op.kind === "delete") throw new Error("Recorded forbidden participant deletion");
        }
      }
      return input.prepare ? ops.slice(input.ops.length) : [];
    } }));
    const result = await provider.invoke("remove", { id: f.id }, invokeContext);
    expect(result).toEqual({ removed: true, retired: true, id: f.id, historyPreserved: true });
    expect(forbidden).not.toHaveBeenCalled();
    expect(operations.length).toBeGreaterThan(0); expect(operations.every(op => op.kind === "put")).toBe(true);
    const legacy = f.mesh.get(f.legacy, { fresh: true })!;
    expect(legacy.value).toMatchObject({ status: "stopped", capabilities: [], retiredAt: expect.any(Number), retiredBy: f.current.id });
    expect(f.mesh.get(keyOf(f.id))?.value).toMatchObject({ retiredAt: expect.any(Number) });
    expect(JSON.parse(fs.readFileSync(path.join(f.root, "participants", `${hash(f.id)}.json`), "utf8")).value).toMatchObject({ retiredAt: expect.any(Number) });
    const bytes = f.store.snapshot().bytes;
    const again = await provider.invoke("remove", { id: f.id }, invokeContext);
    expect(again).toEqual(result);
    expect(f.store.snapshot().bytes).toBe(bytes);
    expect(f.mesh.get(f.legacy)?.version).toBe(legacy.version);
    expect(fs.readFileSync(f.history, "utf8")).toBe("private history sentinel\n");
    expect(f.repair.list().find(actor => actor.id === f.id)).toBeDefined();
  });

  it("public retirement rejects changed native identity and actor/print attribution without destructive fallthrough", async () => {
    const f = await fixture(); const { provider, forbidden } = providerFor(f);
    const owner = JSON.parse(fs.readFileSync(f.newOwner, "utf8"));
    fs.writeFileSync(f.newOwner, JSON.stringify({ ...owner, nativeLauncher: "different-native", name: "same display" }));
    await expect(provider.invoke("remove", { id: f.id, ownerRootGoneBy: f.current.id }, invokeContext)).rejects.toThrow(/trusted native launcher/);
    expect(forbidden).not.toHaveBeenCalled();
    expect(f.mesh.get(f.legacy)?.value).not.toHaveProperty("retiredAt");
  });

  it("local exact-id/name/prefix and finishing-removal routes retain precedence (recorded calls only)", async () => {
    const f = await fixture(); const { actors, provider, forbidden } = providerFor(f);
    const local = await actors.create({ name: f.actor.name, scope: "session", instructions: "No events or activations" });
    // Invoke the real selection path but replace its destructive target with a recorded return.
    forbidden.mockImplementation((...args: unknown[]) => Promise.resolve({ removed: true, id: args[0] }) as any);
    for (const id of [local.id, local.name, local.id.slice(0, 12)]) {
      expect(await provider.invoke("remove", { id }, invokeContext)).toMatchObject({ id: local.id });
    }
    expect(forbidden).toHaveBeenCalledTimes(3);
    expect(f.store.records()[0]!.sessionOrphan ?? {}).not.toHaveProperty("directorySettlement.retiredAt");
    vi.spyOn(ActorManager.prototype, "finishingRemoval").mockImplementation(function (this: ActorManager, id) {
      return this === actors && id === f.id ? ({ id } as any) : undefined;
    });
    expect(await actors.retireSessionOrphan(f.id, () => {})).toBeUndefined();
    expect(forbidden).toHaveBeenCalledTimes(3);
  });

  it("a paused actor directory admits neither settlement nor retirement", async () => {
    const f = await fixture(); const { actors, provider, forbidden } = providerFor(f);
    actors.pauseForRelease();
    expect(await actors.reconcileSessionOrphans()).toBe(0);
    await expect(provider.invoke("remove", { id: f.id }, invokeContext)).rejects.toThrow(/paused for release/);
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "running" });
    expect(forbidden).not.toHaveBeenCalled();
    actors.resumeAfterRelease();
  });

  it.each(["old-lease", "current-launcher", "current-incarnation"] as const)("public retirement rechecks %s after selection under commit custody", async mode => {
    const f = await fixture(); await f.repair.reconcile(); const { provider, forbidden } = providerFor(f);
    const batch = f.mesh.writeBatch.bind(f.mesh); let attempted = false, fired = false;
    vi.spyOn(f.mesh, "writeBatch").mockImplementation(input => batch({ ...input, prepare: view => {
      const ops = input.prepare?.(view) ?? [];
      attempted = ops.some(op => op.kind === "put" && typeof op.value === "object" && op.value !== null && "retiredAt" in op.value);
      return ops;
    }, ...(input.beforeCommit ? { beforeCommit: () => {
      if (attempted && !fired) {
        fired = true;
        if (mode === "old-lease") writeHostLease(f.root, { ...f.lease, expiresAt: Date.now() + 60000 });
        if (mode === "current-incarnation") writeHostLease(f.root, { ...readHostLeaseCurrent(f.root, f.current.id)!, incarnationToken: "replacement" });
        if (mode === "current-launcher") {
          const owner = JSON.parse(fs.readFileSync(f.newOwner, "utf8"));
          fs.writeFileSync(f.newOwner, JSON.stringify({ ...owner, nativeLauncher: "changed-after-selection" }));
        }
      }
      input.beforeCommit!();
    } } : {}) }));
    await expect(provider.invoke("remove", { id: f.id }, invokeContext)).rejects.toThrow(/proof changed|launcher|activation/);
    expect(fired).toBe(true); expect(forbidden).not.toHaveBeenCalled();
    expect(f.mesh.get(f.legacy)?.value).not.toHaveProperty("retiredAt");
    expect(f.store.records()[0]!.sessionOrphan ?? {}).not.toHaveProperty("directorySettlement.retiredAt");
  });

  it.each(["actor", "print", "unrelated-project", "invalid-native"] as const)("%s lineage/display/payload cannot grant observer settlement authority", async mode => {
    const f = await fixture();
    const owner = JSON.parse(fs.readFileSync(f.newOwner, "utf8"));
    if (mode === "print") { delete owner.nativeLauncher; delete owner.nativeProject; }
    if (mode === "unrelated-project") { owner.nativeLauncher = "different"; owner.nativeProject = "other-project"; }
    if (mode === "invalid-native") owner.nativeLauncher = "\nspoof";
    owner.name = "native-lead";
    fs.writeFileSync(f.newOwner, JSON.stringify(owner));
    const repair = mode === "actor" ? new SessionActorOrphans(f.mesh, { ...f.current, kind: "actor" }, "new", path.join(f.root, "actors")) : f.repair;
    if (repair !== f.repair) cleanups.push(() => repair.close());
    await repair.reconcile();
    expect(f.store.records()[0]).toMatchObject({ status: "stopped", stopReason: "host-exit" });
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "running" });
    await expect(repair.retire(f.id, () => {})).rejects.toThrow(/native/);
  });

  it("an unnamed interactive project's native grant permits truth repair, never launcher retirement", async () => {
    const f = await fixture(); const owner = JSON.parse(fs.readFileSync(f.newOwner, "utf8"));
    delete owner.nativeLauncher; fs.writeFileSync(f.newOwner, JSON.stringify(owner));
    await f.repair.reconcile();
    expect(f.mesh.get(f.legacy)?.value).toMatchObject({ status: "stopped", capabilities: [] });
    await expect(f.repair.retire(f.id, () => {})).rejects.toThrow(/trusted native launcher/);
  });

  it("same-session relaunch still restarts the private host-exit actor with the SAME id, not observer adoption", async () => {
    const f = await fixture(); await f.repair.reconcile();
    registerMainInbox(f.root, f.old, "old", undefined, "native-lead", "trusted-project");
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents,
      { runRoot: path.join(f.root, "restart-runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    const owner = new ActorManager("old", f.old, f.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60000 }, manager, () => {},
      { actorRoot: path.join(f.root, "actors", "old"), persistent: true, claimResidency: "session", rootId: f.old.id, project: "trusted-project" });
    cleanups.push(async () => { await owner.close(); await manager.close(); });
    expect(owner.status(f.id)).toMatchObject({ id: f.id, status: "idle", rootId: f.old.id });
    expect(owner.status(f.id).sessionOrphan).toBeUndefined();
    expect(f.repair.validateTombstone(f.mesh.get(f.legacy)!)).toBeUndefined();
    expect(fs.readFileSync(f.history, "utf8")).toBe("private history sentinel\n");
  });

  it("native maintenance uses authenticated DIRECTORY_RETENTION_MS (24h), never a whole-sweep veto (recorded deletion seams)", async () => {
    const f = await fixture(); await f.repair.reconcile();
    const nativeFile = path.join(f.root, "participants", `${hash(f.id)}.json`);
    let entry = f.mesh.get(keyOf(f.id))!;
    const settledAt = f.repair.validateTombstone(entry)!.settledAt!;
    const b = observer(f, "retention-b"), c = observer(f, "retention-c");
    const maintenanceClock = vi.spyOn(Date, "now").mockReturnValue(settledAt + 2 * 3600000);
    try {
      await b.reconcile(); await b.retire(f.id, () => {});
      const retired = f.mesh.get(entry.key)!;
      await f.mesh.put({ identity: retired.updatedBy, key: entry.key, value: { ...(retired.value as object), actorQueued: 99 } });
      await c.reconcile();
      for (const copy of persisted(f)) expect(c.validateTombstone(copy)).toMatchObject({ settledAt, retiredBy: "session:retention-b" });
      entry = f.mesh.get(entry.key)!;
    } finally { maintenanceClock.mockRestore(); }
    const hostKey = `topology/hosts/${hash(f.old.id)}`;
    await f.mesh.put({ identity: f.old, key: hostKey, value: { id: f.old.id, expiresAt: f.lease.expiresAt } });
    const ordinaryId = "c".repeat(32), fakeId = "d".repeat(32);
    const clock = vi.spyOn(Date, "now").mockReturnValue(settledAt + 100 * DIRECTORY_RETENTION_MS);
    try {
      // A refreshed/forged future envelope cannot renew the private 24h bound.
      await f.mesh.put({ identity: entry.updatedBy, key: entry.key, value: entry.value });
      entry = f.mesh.get(entry.key)!; writeParticipantFile(f.root, entry);
      for (const id of [ordinaryId, fakeId]) {
        await f.mesh.put({ identity: id === ordinaryId ? f.old : f.current, key: keyOf(id), value: { ...(entry.value as object), id,
          ...(id === ordinaryId ? { status: "running", stopReason: undefined } : {
            settledAt: settledAt + 100 * DIRECTORY_RETENTION_MS, updatedAt: settledAt + 100 * DIRECTORY_RETENTION_MS }) } });
        writeParticipantFile(f.root, f.mesh.get(keyOf(id))!);
      }
    } finally { clock.mockRestore(); }
    const bytes = fs.readFileSync(nativeFile, "utf8");
    const snapshots = participantFiles.readParticipantFiles(f.root).map(row => JSON.stringify(row));
    const shared: string[] = [], files: string[] = [];
    // No product deletion code is executed, even if a regression selects the protected record.
    vi.spyOn(participantFiles, "sweepParticipantLockLeftovers").mockResolvedValue(undefined);
    vi.spyOn(tempJanitor, "sweepAbandonedStateTemporaries").mockReturnValue([]);
    vi.spyOn(fs, "unlinkSync").mockImplementation(() => { throw new Error("Forbidden product unlink"); });
    vi.spyOn(participantFiles, "removeParticipantFileIf").mockImplementation(async (_mesh, key, predicate) => {
      const current = participantFiles.readParticipantFile(f.root, key)!;
      if (predicate(current)) { files.push(key); throw new Error("Recorded file delete, not executed"); }
      return false;
    });
    let sharedProbe = true, proofAvailable = true;
    const probe = {
      root: f.root, listAll: f.mesh.listAll.bind(f.mesh), custody: async <T>(operation: () => T | Promise<T>) => operation(),
      writeBatch: vi.fn(async (input: Parameters<MeshStore["writeBatch"]>[0]) => {
        proofAvailable = true; // Recheck proof at the shared/file commit seams, not only selection.
        if (sharedProbe) {
          for (const op of input.ops) if (op.kind === "delete" && (!op.condition || op.condition(key => f.mesh.get(key)))) shared.push(op.key);
          throw new Error("Recorded shared delete, not executed");
        }
        // The file-only probe skips ALL shared intents. No prepare/outbox/lease removal.
        return [];
      }),
    };
    const options = { ownHostId: f.current.id, now: settledAt + 7 * 60 * 60 * 1000,
      terminalActor: (candidate: Parameters<typeof f.repair.validateTombstone>[0]) => proofAvailable ? f.repair.validateTombstone(candidate) : undefined };
    const recordedReap = async (at: typeof options, hideSelectionProof = false) => {
      sharedProbe = true; proofAvailable = !hideSelectionProof;
      await expect(reapDeadHostRecords(probe as unknown as MeshStore, f.current, at)).rejects.toThrow("Recorded shared delete");
      sharedProbe = false; proofAvailable = !hideSelectionProof;
      await reapDeadHostRecords(probe as unknown as MeshStore, f.current, at);
    };
    const selected = deadHostRecords(probe, options);
    expect(selected.filter(row => row.entry.key === entry.key)).toEqual([]);
    for (const id of [ordinaryId, fakeId]) expect(selected.filter(row => row.entry.key === keyOf(id))).toHaveLength(2);
    await recordedReap(options);
    expect(shared).toEqual(expect.arrayContaining([hostKey, keyOf(ordinaryId), keyOf(fakeId)]));
    expect(shared).not.toContain(entry.key);
    expect(files).toEqual(expect.arrayContaining([keyOf(ordinaryId), keyOf(fakeId)]));
    expect(files).not.toContain(entry.key);
    shared.length = 0; files.length = 0;
    const expired = { ...options, now: settledAt + DIRECTORY_RETENTION_MS };
    expect(deadHostRecords(probe, expired).filter(row => row.entry.key === entry.key)).toHaveLength(2);
    await recordedReap(expired);
    expect(shared).toContain(entry.key); expect(files).toContain(entry.key);
    shared.length = 0; files.length = 0;
    // Even a selected terminal must survive if private proof appears before deletion.
    await recordedReap(options, true);
    expect(shared).not.toContain(entry.key); expect(files).not.toContain(entry.key);
    expect(shared).toContain(keyOf(fakeId)); expect(files).toContain(keyOf(fakeId));
    expect(DIRECTORY_RETENTION_MS).toBe(24 * 60 * 60 * 1000);
    expect(fs.readFileSync(nativeFile, "utf8")).toBe(bytes);
    expect(participantFiles.readParticipantFiles(f.root).map(row => JSON.stringify(row))).toEqual(snapshots);
    expect(f.mesh.get(entry.key)?.version).toBe(entry.version);
    expect(f.mesh.get(f.legacy)).toBeDefined();
    expect(fs.readFileSync(f.history, "utf8")).toBe("private history sentinel\n");
  });

  it("FVR-01: alternating authorized observers keep receipt/time stable, including retirement and real repair", async () => {
    const f = await fixture(); await f.repair.reconcile();
    const initial = f.store.snapshot().bytes, copies = persisted(f);
    const settledAt = f.repair.validateTombstone(copies[0])!.settledAt!;
    const b = observer(f, "observer-b"), c = observer(f, "observer-c");
    const clock = vi.spyOn(Date, "now");
    for (const [index, repair] of [b, c, b, f.repair, c].entries()) {
      clock.mockReturnValue(settledAt + (index + 1) * 1000);
      await repair.reconcile();
      expect(f.store.snapshot().bytes).toBe(initial);
      expect(persisted(f)).toEqual(copies);
    }
    clock.mockReturnValue(settledAt + 3600000);
    await b.retire(f.id, () => {});
    for (const entry of persisted(f)) expect(c.validateTombstone(entry)).toMatchObject({ settledAt, retiredBy: "session:observer-b" });
    const entry = f.mesh.get(keyOf(f.id))!;
    await f.mesh.put({ identity: entry.updatedBy, key: entry.key, value: { ...(entry.value as object), actorQueued: 99 } });
    clock.mockReturnValue(settledAt + 2 * 3600000);
    await c.reconcile();
    for (const entry of persisted(f)) expect(c.validateTombstone(entry)).toMatchObject({ settledAt, capabilities: [], retiredBy: "session:observer-b" });
    expect(f.mesh.get(keyOf(f.id))!.value).not.toHaveProperty("actorQueued");
    const options = { ownHostId: f.current.id, terminalActor: (entry: Parameters<typeof c.validateTombstone>[0]) => c.validateTombstone(entry) };
    expect(deadHostRecords(f.mesh, { ...options, now: settledAt + 7 * 3600000 }).filter(row => row.entry.key === entry.key)).toHaveLength(0);
    expect(deadHostRecords(f.mesh, { ...options, now: settledAt + DIRECTORY_RETENTION_MS }).filter(row => row.entry.key === entry.key)).toHaveLength(2);
    expect(fs.readFileSync(f.history, "utf8")).toBe("private history sentinel\n");
  });

  it.each(["pre-mesh", "post-shared"] as const)("FVR-01: repeated %s interrupted observer repairs retain every persisted provenance", async mode => {
    const f = await fixture(); await f.repair.reconcile();
    const a = persisted(f), settledAt = f.repair.validateTombstone(a[0])!.settledAt!;
    const b = observer(f, "observer-b"), c = observer(f, "observer-c"), d = observer(f, "observer-d");
    // A real repair is needed, not a receipt minted for an ordinary observer.
    await f.mesh.put({ identity: f.current, key: f.legacy, value: { ...(a[0].value as object), actorQueued: 99 } });
    const batch = f.mesh.writeBatch.bind(f.mesh);
    let failures = 0;
    const spy = vi.spyOn(f.mesh, "writeBatch").mockImplementation(input => batch(input.beforeCommit ? {
      ...input, ...(mode === "pre-mesh" ? { beforeCommit: () => { input.beforeCommit!(); failures++; throw new Error("recorded pre-mesh failure"); } }
        : { afterCommit: () => { failures++; throw new Error("recorded file publication failure"); } }),
    } : input));
    await b.reconcile();
    await c.reconcile();
    expect(failures).toBe(2);
    for (const entry of persisted(f)) expect(d.validateTombstone(entry)).toMatchObject({ settledAt, capabilities: [] });
    expect(d.validateTombstone(a[2])).toMatchObject({ settledAt });
    expect((f.store.records()[0]!.sessionOrphan as any).directorySettlement.persisted.length).toBeLessThanOrEqual(3);
    spy.mockRestore();
    await d.reconcile();
    for (const entry of persisted(f)) expect(d.validateTombstone(entry)).toMatchObject({ settledAt, capabilities: [] });
    expect(f.mesh.get(f.legacy)!.value).not.toHaveProperty("actorQueued");
    expect(fs.readFileSync(f.history, "utf8")).toBe("private history sentinel\n");
  });

  it("FVR-01: migrates an already persisted current/previous outbox without losing its older live copies", async () => {
    const f = await fixture(); await f.repair.reconcile();
    const copies = persisted(f), settledAt = f.repair.validateTombstone(copies[0])!.settledAt!;
    const row = f.store.records()[0]!, marker = row.sessionOrphan as any;
    // Exact old-format shape: B's private intent followed A, but no B mesh copy committed.
    f.store.write([{ ...row, sessionOrphan: { ...marker, directorySettlement: {
      by: "session:interrupted-b", at: settledAt + 1000, previous: { by: f.current.id, at: settledAt },
    } } }]);
    const c = observer(f, "observer-c");
    for (const entry of copies) expect(c.validateTombstone(entry)).toMatchObject({ settledAt });
    await c.reconcile();
    for (const entry of persisted(f)) expect(c.validateTombstone(entry)).toMatchObject({ settledAt, capabilities: [] });
    expect((f.store.records()[0]!.sessionOrphan as any).directorySettlement).toMatchObject({ by: "session:observer-c", settledAt });
    expect(fs.readFileSync(f.history, "utf8")).toBe("private history sentinel\n");
  });

  it("the ordinary committed presence pass repairs raw advertisements without actors/list/status reads", async () => {
    const f = await fixture(); let resolve!: () => void;
    const completed = new Promise<void>(done => { resolve = done; });
    // Use an actual fresh native host, not the fixture's manually seeded observer lease.
    const identity: MeshIdentity = { ...f.current, id: "session:heartbeat", sessionId: "heartbeat" };
    registerMainInbox(f.root, identity, "heartbeat", undefined, "native-lead", "trusted-project");
    const presence = new ParticipantDirectory(f.mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 60000, reapDeadHosts: false, presencePassMs: 25000,
      presencePass: async () => { await f.repair.reconcile(); resolve(); }, terminalActor: entry => f.repair.validateTombstone(entry) });
    cleanups.push(() => presence.close());
    presence.registerSource(() => [presence.root({ id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi",
      transport: "host", cwd: process.cwd(), sessionId: "heartbeat", updatedAt: Date.now(), pendingMessages: false, local: true })]);
    // Advance the existing elapsed-cadence fact, not a timer/poll loop.
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 25001);
    await presence.start();
    expect(presence.writeStalled()).toBeUndefined();
    expect(presence.routingUnavailable()).toBeUndefined();
    await completed;
    expect(f.mesh.get(f.legacy, { fresh: true })?.value).toMatchObject({ status: "stopped", stopReason: "owner-root-gone" });
  });
});
