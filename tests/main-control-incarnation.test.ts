import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { CONTROL_STALE_INCARNATION, FabricControlPlane } from "../src/topology/control-plane.js";

it.each(["steer", "followUp", "stop", "ask"] as const)("real Main reload preserves session messages but fences pre-reload %s execution", async operation => {
  const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
  const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
  const { normalizeFabricConfig } = await import("../src/config.js");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-control-incarnation-"));
  const meshRoot = path.join(root, "mesh");
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root); vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  const sessionId = "7514aaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
  const ctx = () => ({ cwd: root, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
    isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
      getLeafId: () => null, getEntries: () => [] }, ui: { setStatus: () => {}, notify: () => {} } }) as unknown as ExtensionContext;
  const sendMessage = vi.fn();
  const host = { on: () => () => {}, events: { emit: () => {}, on: () => () => {} }, sendMessage,
    appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "incarnation-main" } as unknown as ExtensionAPI;
  const config = normalizeFabricConfig({ fullCodeMode: false, mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } });
  const runtimes: InstanceType<typeof FabricRuntimeState>[] = [];
  const ownerPlanes: FabricControlPlane[] = [];
  const start = FabricControlPlane.prototype.start;
  vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation(function (this: FabricControlPlane, ...args) {
    if (this.options.hostId === `session:${sessionId}`) ownerPlanes.push(this);
    return start.apply(this, args);
  });
  const createRuntime = () => {
    const value = new FabricRuntimeState(host, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(root, "unused.mjs"), skills: root,
    } }); runtimes.push(value); return value;
  };
  const mesh = new MeshStore(meshRoot, 65_536, 1_000);
  const sender = new FabricControlPlane(mesh, { id: "session:observer", name: "observer", kind: "main" },
    { enabled: true, hostId: "session:observer", pollMs: 20, acknowledgementTimeoutMs: 10_000 });
  const observer = new ParticipantDirectory(mesh, { enabled: true, hostId: "session:observer", rootId: "session:observer",
    identity: { id: "session:observer", name: "observer", kind: "main" } });
  let pending: Promise<unknown> | undefined;
  try {
    const first = createRuntime(); await first.initialize(ctx(), config);
    const old = observer.get(`session:${sessionId}`, undefined, { fresh: true })!;
    expect(old.ownerIncarnation).toBe(ownerPlanes[0]!.incarnation);
    ownerPlanes[0]!.pause(); sender.start(() => ({ accepted: false }));
    const input = { message: "before reload", ownerIncarnation: old.ownerIncarnation, triggerTurn: false };
    pending = (operation === "ask"
      ? sender.requestResult(old.ownerHostId, old.id, operation, input, old.ownerIdentityId, { routedRemoteHost: null })
      : sender.request(old.ownerHostId, old.id, operation, input, old.ownerIdentityId, { routedRemoteHost: null })).catch(error => error);
    await vi.waitFor(() => expect(mesh.read({ topic: "fabric.control.command" })).toHaveLength(1));
    await first.shutdown("reload");
    const second = createRuntime(); await second.initialize(ctx(), config);
    const fresh = observer.get(old.id, undefined, { fresh: true })!;
    expect(fresh.ownerIncarnation).toBe(ownerPlanes[1]!.incarnation);
    expect(fresh.ownerIncarnation).not.toBe(old.ownerIncarnation);
    if (operation === "steer" || operation === "followUp") {
      expect(await pending).toMatchObject({ acknowledged: true });
      expect(sendMessage).toHaveBeenCalledOnce();
    } else {
      expect(await pending).toMatchObject({ code: CONTROL_STALE_INCARNATION });
      expect(sendMessage).not.toHaveBeenCalled();
    }
    sendMessage.mockClear();
    await expect(sender.request(fresh.ownerHostId, fresh.id, "steer", { message: "after reload", ownerIncarnation: fresh.ownerIncarnation, triggerTurn: false },
      fresh.ownerIdentityId, { routedRemoteHost: null, idempotencyKey: "fresh-main" })).resolves.toMatchObject({ acknowledged: true });
    expect(sendMessage).toHaveBeenCalledOnce();
    await expect(sender.request(fresh.ownerHostId, fresh.id, "steer", { message: "after reload", ownerIncarnation: fresh.ownerIncarnation, triggerTurn: false },
      fresh.ownerIdentityId, { routedRemoteHost: null, idempotencyKey: "fresh-main" })).resolves.toMatchObject({ acknowledged: true });
    expect(sendMessage).toHaveBeenCalledOnce();
  } finally {
    await sender.close(); await pending;
    for (const runtime of runtimes.reverse()) await runtime.shutdown("exit");
    await observer.close(); vi.restoreAllMocks(); vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 20_000);

it.each(["stalled", "failed", "expired"] as const)("real Main queues controls until its epoch is durably published (%s)", async publication => {
  const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
  const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
  const { normalizeFabricConfig } = await import("../src/config.js");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "main-publication-fence-"));
  const meshRoot = path.join(root, "mesh");
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root); vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  const sessionId = "7514ffff-bbbb-cccc-dddd-eeeeeeeeeeee";
  const ownerId = `session:${sessionId}`;
  const admitExecution = vi.fn(() => ({ accepted: true }));
  const ctx = { cwd: root, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
    isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
      getLeafId: () => null, getEntries: () => [] }, ui: { setStatus: () => {}, notify: () => {} } } as unknown as ExtensionContext;
  const sendMessage = vi.fn();
  const host = { on: () => () => {}, events: { emit: () => {}, on: () => () => {} }, sendMessage,
    appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "publication-main" } as unknown as ExtensionAPI;
  const config = normalizeFabricConfig({ fullCodeMode: false, mesh: { enabled: true, root: meshRoot, actorPollMs: 20 },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } });
  const runtime = new FabricRuntimeState(host, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
    residentHost: path.join(root, "unused.mjs"), skills: root,
  } });
  const mesh = new MeshStore(meshRoot, 65_536, 1_000);
  const sender = new FabricControlPlane(mesh, { id: "session:observer", name: "observer", kind: "main" },
    { enabled: true, hostId: "session:observer", pollMs: 20, acknowledgementTimeoutMs: 5_000 });
  const observer = new ParticipantDirectory(mesh, { enabled: true, hostId: "session:observer", rootId: "session:observer",
    identity: { id: "session:observer", name: "observer", kind: "main" } });
  // Keep a real predecessor record durable while the replacement's publication waits.
  const predecessor = new ParticipantDirectory(mesh, { enabled: true, hostId: ownerId, rootId: ownerId,
    identity: { id: ownerId, name: "predecessor", kind: "main" }, reapDeadHosts: false });
  predecessor.registerSource(() => [{ format: 1, id: ownerId, kind: "root", rootId: ownerId,
    ownerHostId: ownerId, ownerIdentityId: ownerId, name: "predecessor", status: "idle", runner: "pi",
    transport: "host", capabilities: ["steer", "followUp", "stop", "ask"], cwd: root, sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  let ownerPlane!: FabricControlPlane;
  let ownerDirectory!: ParticipantDirectory;
  let attempts = 0; let release!: () => void;
  const gate = new Promise<void>(done => { release = done; });
  const refresh = ParticipantDirectory.prototype.refresh;
  vi.spyOn(ParticipantDirectory.prototype, "refresh").mockImplementation(async function (this: ParticipantDirectory, ...args) {
    if (this.options.hostId === ownerId && this !== predecessor) {
      ownerDirectory = this;
      if (++attempts === 1 && publication === "failed") throw new Error("injected initial publication failure");
      await gate;
    }
    return refresh.apply(this, args);
  });
  const start = FabricControlPlane.prototype.start;
  vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation(function (this: FabricControlPlane, ...args) {
    if (this.options.hostId === ownerId) {
      ownerPlane = this;
      return start.call(this, (command, from, signal, verification) => {
        // Inject a supported execution target: Main itself does not support stop.
        // The actual runtime's publication/consumption/claim/ACK path is unchanged.
        if (command.operation === "stop") return admitExecution();
        return args[0](command, from, signal, verification);
      });
    }
    return start.apply(this, args);
  });
  const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const originals = new Map<string, Record<string, unknown>>();
  const publish = mesh.publish.bind(mesh);
  vi.spyOn(mesh, "publish").mockImplementation(input => {
    if (input.topic !== "fabric.control.command") return publish(input);
    const data = input.data;
    return publish({ ...input, data: (at: number) => {
      const wire = (typeof data === "function" ? data(at) : data) as Record<string, unknown>;
      if (wire.message !== "epoch stripped") return wire;
      originals.set(wire.commandId as string, wire);
      const { ownerIncarnation: _epoch, ...stripped } = wire;
      return stripped; // Remove ONLY the epoch, preserving origin and native event/commit times.
    } });
  });
  let initializing: Promise<void> | undefined;
  const pending: Promise<unknown>[] = [];
  try {
    await predecessor.refresh();
    const activationStartedAfter = Date.now();
    initializing = runtime.initialize(ctx, config);
    await vi.waitFor(() => expect(ownerDirectory).toBeDefined());
    if (publication === "failed") {
      await initializing;
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("Initial mesh publish failed"));
    }
    expect(ownerDirectory.canConsumeMesh()).toBe(false);
    const old = observer.get(ownerId, undefined, { fresh: true })!;
    expect(old.ownerIncarnation).toBe(predecessor.ownerIncarnation);
    expect(old.ownerIncarnation).not.toBe(ownerPlane.incarnation);
    sender.start(() => ({ accepted: false }));
    // Current well-formed commands are queued, not admitted, until publication.
    await new Promise(done => setTimeout(done, 5));
    const input = { message: "while unpublished", triggerTurn: false, ownerIncarnation: ownerPlane.incarnation };
    const options = { routedRemoteHost: null, timeoutMs: publication === "expired" ? 200 : 5_000 };
    // Observe one attempt for expiry: request() intentionally resends proven-notRun messages.
    pending.push((publication === "expired"
      ? sender.requestResult(ownerId, ownerId, "steer", input, ownerId, options)
      : sender.request(ownerId, ownerId, "steer", input, ownerId, options)).catch(error => error));
    if (publication !== "expired") {
      pending.push(sender.request(old.ownerHostId, old.id, "stop", { ownerIncarnation: old.ownerIncarnation }, old.ownerIdentityId,
        { routedRemoteHost: null }).catch(error => error));
      pending.push(sender.request(ownerId, ownerId, "stop", { ownerIncarnation: ownerPlane.incarnation }, ownerId,
        { routedRemoteHost: null }).catch(error => error));
      // The sender still resolves the durable predecessor after the new activation starts.
      // Drop only that captured epoch on the wire; timestamps cannot authorize a downgrade.
      for (const operation of ["stop", "ask", "steer", "followUp"] as const) {
        const strippedInput = { message: "epoch stripped", ownerIncarnation: old.ownerIncarnation, triggerTurn: false };
        pending.push((operation === "ask"
          ? sender.requestResult(old.ownerHostId, old.id, operation, strippedInput, old.ownerIdentityId, { routedRemoteHost: null })
          : sender.request(old.ownerHostId, old.id, operation, strippedInput, old.ownerIdentityId, { routedRemoteHost: null }))
          .catch(error => error));
      }
    }
    await vi.waitFor(() => expect(mesh.read({ topic: "fabric.control.command" })).toHaveLength(pending.length));
    const queuedCommands = mesh.read({ topic: "fabric.control.command" });
    const queued = queuedCommands[0]!.data as { deadlineAt: number };
    if (publication !== "expired") {
      expect(originals.size).toBe(4);
      for (const event of queuedCommands.filter(event => (event.data as { message?: string }).message === "epoch stripped")) {
        const wire = event.data as { commandId: string; requestCreatedAt: number };
        const { ownerIncarnation, ...expected } = originals.get(wire.commandId)!;
        expect(ownerIncarnation).toBe(old.ownerIncarnation);
        expect(event.data).toEqual(expected);
        expect(wire.requestCreatedAt).toBeGreaterThan(activationStartedAfter);
        expect(event.createdAt).toBeGreaterThan(activationStartedAfter);
      }
    }
    await new Promise(done => setTimeout(done, publication === "expired" ? Math.max(0, queued.deadlineAt - Date.now() + 10) : 100));
    expect(sendMessage).not.toHaveBeenCalled();
    expect(admitExecution).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fabric.control.ack" })).toHaveLength(0);
    expect(mesh.listAll("topology/control-seen/")).toEqual([]);
    release(); await initializing;
    await vi.waitFor(() => expect(ownerDirectory.canConsumeMesh()).toBe(true));
    const published = observer.get(ownerId, undefined, { fresh: true })!;
    expect(published.ownerIncarnation).toBe(ownerPlane.incarnation);
    if (publication === "expired") {
      expect(await pending[0]).toMatchObject({ message: "Fabric control command expired; not delivered, safe to resend." });
      expect(sendMessage).not.toHaveBeenCalled();
      expect(mesh.read({ topic: "fabric.control.ack" })[0]!.data).toMatchObject({ accepted: false, notRun: true, ownerIncarnation: published.ownerIncarnation });
    } else {
      await vi.waitFor(() => expect(mesh.read({ topic: "fabric.control.ack" })).toHaveLength(7));
      expect(sendMessage).toHaveBeenCalledOnce();
      expect(admitExecution).toHaveBeenCalledOnce();
      expect(await pending[0]).toMatchObject({ acknowledged: true });
      expect(await pending[1]).toMatchObject({ code: CONTROL_STALE_INCARNATION });
      expect(await pending[2]).toMatchObject({ acknowledged: true });
      for (const refused of pending.slice(3)) {
        expect(await refused).toMatchObject({ name: "FabricControlStaleIncarnationError", code: CONTROL_STALE_INCARNATION });
      }
      const claims = mesh.listAll("topology/control-seen/");
      expect(claims).toHaveLength(2); // only the two well-formed current commands
      expect(mesh.read({ topic: "fabric.control.command" })).toHaveLength(7); // no automatic resend
      for (const commandId of originals.keys()) {
        expect(claims.some(entry => (entry.value as { commandId: string }).commandId === commandId)).toBe(false);
        expect(mesh.read({ topic: "fabric.control.ack" })).toContainEqual(expect.objectContaining({ data: expect.objectContaining({
          commandId, accepted: false, errorCode: CONTROL_STALE_INCARNATION, staleIncarnation: null,
        }) }));
      }
      expect(sendMessage).toHaveBeenCalledOnce();
      expect(admitExecution).toHaveBeenCalledOnce();
      expect(mesh.read({ topic: "fabric.control.ack" }).every(event =>
        (event.data as { ownerIncarnation: string }).ownerIncarnation === published.ownerIncarnation)).toBe(true);
    }
  } finally {
    release(); await initializing;
    await sender.close(); await Promise.all(pending);
    await runtime.shutdown("exit"); await observer.close(); await predecessor.close();
    vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true });
  }
}, 20_000);
