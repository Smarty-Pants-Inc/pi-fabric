import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { FabricControlPlane, type FabricControlAcceptance } from "../src/topology/control-plane.js";

// Keep reason: #9751 data-safety/teardown boundary. Real Main admission, real outcome/log
// stores and public runtime lifecycle; only supplied Pi APIs and ACK failure are simulated.
it("ordinary actor close rejection stays cached and releases its read cache without acquiring native retry semantics", async () => {
  const { createNativeActorFixture } = await import("./helpers/native-actor-ingress.js");
  const { MeshBackgroundQueue } = await import("../src/core/atomic-write.js");
  const { ActorRegistryStore } = await import("../src/actors/registry-store.js");
  const f = await createNativeActorFixture({ ensure: false }); // no native actors/callbacks
  const failure = new Error("ordinary notification teardown failed");
  const release = vi.spyOn(ActorRegistryStore.prototype, "releaseReadCache");
  const close = vi.spyOn(MeshBackgroundQueue.prototype, "close").mockRejectedValueOnce(failure);
  try {
    const first = f.actors.close();
    await expect(first).rejects.toBe(failure);
    expect(release.mock.contexts.filter(store => (store as InstanceType<typeof ActorRegistryStore>).sameLocation(new ActorRegistryStore(f.actorRoot)))).toHaveLength(1);
    const second = f.actors.close();
    await expect(second).rejects.toBe(failure); expect(second).toBe(first);
    expect(close).toHaveBeenCalledOnce();
  } finally {
    vi.restoreAllMocks();
    // The failed ordinary manager is terminal/cached. Its watchers/timers were
    // already joined before the injected notification failure; close other owners.
    f.managers.splice(0); await f.close();
  }
});

// Expected platform gap: this ordinary-failure counterexample pins a real Linux
// host flock; it is not a native callback/release qualification.
it.skipIf(process.platform !== "linux")("ordinary resident failed close does not retry or release a retained fence on the second call", async () => {
  const { ResidentHost } = await import("../src/residency/host.js");
  const { ResidentRequestRetention } = await import("../src/residency/retention.js");
  const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
  const { RESIDENT_HOST_FORMAT } = await import("../src/residency/protocol.js");
  const locks = await import("../src/residency/file-lock.js");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ordinary-resident-close-"));
  const residencyRoot = path.join(root, "resident"), lockPath = path.join(residencyRoot, "host.lock");
  const lock = vi.spyOn(locks, "lockFile"); // real kernel acquisition; capture ONLY this fixture's returned fd
  const host = new ResidentHost({ format: RESIDENT_HOST_FORMAT, rootId: "session:ordinary-close", sessionId: "ordinary-close",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot,
    fullCodeMode: false, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" });
  let fd: number | undefined, pinned: { dev: number; ino: number } | undefined;
  try {
    await host.start();
    fd = await lock.mock.results[lock.mock.calls.findIndex(([file]) => file === lockPath)]!.value;
    const stat = fs.fstatSync(fd!); pinned = { dev: stat.dev, ino: stat.ino };
    const ownerFile = path.join(residencyRoot, "owner.json"), owner = fs.readFileSync(ownerFile);
    const failure = new Error("ordinary retention teardown failed");
    const realClose = ResidentRequestRetention.prototype.close;
    const retention = vi.spyOn(ResidentRequestRetention.prototype, "close").mockImplementationOnce(function (this: InstanceType<typeof ResidentRequestRetention>) {
      realClose.call(this); throw failure; // stop its timer first; this is NOT native-busy settlement
    });
    await expect(host.close()).rejects.toBe(failure);
    await expect(host.close()).resolves.toBeUndefined();
    expect(retention).toHaveBeenCalledOnce(); // predependency behavior: no new retry after ordinary failure
    expect(fs.readFileSync(ownerFile)).toEqual(owner);
    expect({ dev: fs.fstatSync(fd!).dev, ino: fs.fstatSync(fd!).ino }).toEqual(pinned);
  } finally {
    vi.restoreAllMocks();
    // Explicit fixture-owner cleanup is NOT another host.close/release receipt.
    // Join each still-live component before closing our captured descriptor/root.
    await host.actors?.close().catch(() => undefined);
    await host.lifecycle?.close().catch(() => undefined);
    await host.control?.close().catch(() => undefined);
    await host.agents?.close().catch(() => undefined);
    await host.participants?.close().catch(() => undefined);
    host.mesh?.closeState();
    if (fd !== undefined && pinned) {
      try { const stat = fs.fstatSync(fd); if (stat.dev === pinned.dev && stat.ino === pinned.ino) fs.closeSync(fd); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "EBADF") throw error; }
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

const cases = [
  { flow: "exit", failure: "post-append" },
  { flow: "reload", failure: "post-append" },
  { flow: "new", failure: "post-append" },
  { flow: "resume", failure: "post-append" },
  { flow: "reinitialize", failure: "post-append" },
  { flow: "exit", failure: "pre-append" },
] as const;
const until = (assertion: () => void) => vi.waitFor(assertion, { timeout: 4_000, interval: 20 });
const bounded = async <T,>(operation: Promise<T>): Promise<T> => {
  let done = false;
  // Observe rejection immediately, but preserve it for the caller's resolves assertion.
  void operation.then(() => { done = true; }, () => { done = true; });
  await until(() => expect(done).toBe(true));
  return operation;
};
const hash = (value: string) => createHash("sha256").update(value).digest("hex");

it.each(cases)("real Main $flow resolves with one private-body-free warning after $failure ACK uncertainty, then recovers saved acceptance", async ({ flow, failure }) => {
  const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
  const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
  const { normalizeFabricConfig } = await import("../src/config.js");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "control-close-main-"));
  const meshRoot = path.join(root, "mesh");
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  const oldId = "9751aaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
  const targetId = "9751bbbb-bbbb-cccc-dddd-eeeeeeeeeeee";
  const oldRoot = `session:${oldId}`;
  const targetRoot = `session:${targetId}`;
  const oldFile = path.join(root, "old.jsonl"), targetFile = path.join(root, "target.jsonl");
  const entries = new Map<string, unknown[]>([[oldId, []], [targetId, []]]);
  // Native session files carry an identity header; rotation's receipt reader verifies it.
  fs.writeFileSync(oldFile, JSON.stringify({ type: "session", id: oldId }) + "\n");
  fs.writeFileSync(targetFile, JSON.stringify({ type: "session", id: targetId }) + "\n");
  let activeId = oldId;
  const notify = vi.fn();
  const context = (sessionId: string, sessionFile: string) => ({
    cwd: root, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
    isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => sessionFile,
      getBranch: () => [], getLeafId: () => null, getEntries: () => entries.get(sessionId)! },
    ui: { setStatus: () => {}, notify },
  }) as unknown as ExtensionContext;
  const oldContext = context(oldId, oldFile), targetContext = context(targetId, targetFile);
  // The supplied Pi host persists its native receipt before returning. This prevents a
  // legitimate Main journal replay being mistaken for repeated control-handler admission.
  const sendMessage = vi.fn((message: { customType: string; details?: unknown }) => {
    const entry = { type: "custom_message", customType: message.customType, details: message.details };
    entries.get(activeId)!.push(entry);
    fs.appendFileSync(activeId === oldId ? oldFile : targetFile, JSON.stringify(entry) + "\n");
  });
  const host = { on: () => () => {}, events: { emit: () => {}, on: () => () => {} }, sendMessage,
    appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "close-lifecycle-main" } as unknown as ExtensionAPI;
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: meshRoot, actorPollMs: 20, controlIncarnationFence: "enforce" },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false },
    memory: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } });
  const runtime = new FabricRuntimeState(host, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
    residentHost: path.join(root, "unused.mjs"), skills: root,
  } });
  const ownerPlanes: FabricControlPlane[] = [];
  const start = FabricControlPlane.prototype.start;
  vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation(function (this: FabricControlPlane, ...args) {
    ownerPlanes.push(this); return start.apply(this, args); // real Main handler, no replacement
  });
  const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const closeState = vi.spyOn(MeshStore.prototype, "closeState"); // call through to the actual late cleanup
  const mesh = new MeshStore(meshRoot, 65_536, 1_000);
  const seen = new MeshStore(path.join(meshRoot, "control-seen", hash(oldRoot).slice(0, 32)), 65_536, 1_000);
  const commandId = `9751-close-${flow}-${failure}`;
  const outcomeKey = `topology/control-seen/${hash(`${oldRoot}\0${commandId}`)}`;
  const privateBody = "PRIVATE-MAIN-MESSAGE-BODY-9751";
  let failAck = true, attempts = 0;
  const publish = MeshStore.prototype.publish;
  vi.spyOn(MeshStore.prototype, "publish").mockImplementation(async function (this: MeshStore, input) {
    const failing = failAck && input.topic === "fabric.control.ack" && input.from.id === oldRoot;
    if (failing) {
      attempts++;
      if (failure === "pre-append") throw new Error("Uncertain ACK completion (not a proven no-write refusal)");
    }
    const event = await publish.call(this, input);
    if (failing) throw new Error("ACK appended but completion is uncertain");
    return event;
  });
  const acks = () => mesh.read({ topic: "fabric.control.ack", limit: 1_000 })
    .filter(event => (event.data as { commandId?: string }).commandId === commandId);
  const closeWarnings = () => warning.mock.calls.map(call => call.map(String).join(" "))
    .filter(line => line.includes("Control close has unsettled acknowledgement obligations"));
  try {
    await bounded(runtime.initialize(oldContext, config));
    expect(runtime.initialized).toBe(true);
    expect(runtime.mainAgentInfo().id).toBe(oldRoot);
    const oldMesh = runtime.mesh, oldPlane = ownerPlanes[0]!;
    const originalEpoch = oldPlane.incarnation;
    const now = Date.now();
    await mesh.publish({ topic: "fabric.control.command", kind: "steer",
      from: { id: "session:close-observer", name: "observer", kind: "main" }, to: oldRoot,
      data: { version: 1, commandId, targetId: oldRoot, operation: "steer", replyTo: "session:close-observer",
        ownerIncarnation: originalEpoch, message: privateBody, triggerTurn: false,
        requestCreatedAt: now, requestedAt: now, deadlineAt: now + 60_000 } });
    await until(() => {
      expect(attempts).toBe(1);
      expect(acks()).toHaveLength(failure === "pre-append" ? 0 : 1);
    });
    oldPlane.pause();
    await expect(bounded(oldPlane.checkpointForRelease())).rejects.toThrow(/unsettled publication obligations/);
    expect(() => oldPlane.releaseCheckpointReceipt()).toThrow(/unavailable|unsettled/);
    const saved = seen.get(outcomeKey, { fresh: true })!.value as {
      acceptance: FabricControlAcceptance; ownerIncarnation: string; sequence: number;
    };
    expect(saved.ownerIncarnation).toBe(originalEpoch);
    expect(saved.acceptance).toMatchObject({ accepted: true, messageId: expect.any(String), triggered: false });
    expect(saved.sequence).toBeGreaterThan(0);
    expect(sendMessage).toHaveBeenCalledOnce();
    expect(acks()).toHaveLength(failure === "pre-append" ? 0 : 1);
    const beforeRecovery = acks().length;
    const switched = flow === "new" || flow === "resume";
    if (flow === "reinitialize") {
      // Exercise #closeInternal through the public replacement path as well as shutdown.
      failAck = false;
      await expect(bounded(runtime.initialize(oldContext, config))).resolves.toBeUndefined();
      expect(runtime.initialized).toBe(true);
    } else {
      await expect(bounded(runtime.shutdown(flow, switched ? targetFile : undefined))).resolves.toBeUndefined();
      expect(runtime.initialized).toBe(false);
      failAck = false;
    }
    expect(closeState.mock.contexts.filter(store => store === oldMesh)).toHaveLength(1);
    expect(attempts).toBe(1); // close did not replay an ambiguous append
    expect(closeWarnings()).toHaveLength(1);
    const diagnostic = closeWarnings()[0]!;
    expect(diagnostic).toContain("ACK"); expect(diagnostic).toContain(commandId); expect(diagnostic).toContain(outcomeKey);
    expect(diagnostic).toMatch(/uncertain/); expect(diagnostic).toMatch(/[Ss]aved/);
    expect(diagnostic).toMatch(/Checkpoint\/release remains refused/);
    expect(diagnostic).not.toContain(privateBody);
    expect(diagnostic).not.toContain(saved.acceptance.messageId!);
    // The warning is not a release receipt, even after close has resolved.
    await expect(bounded(oldPlane.checkpointForRelease())).rejects.toThrow(/unsettled publication obligations/);
    expect(() => oldPlane.releaseCheckpointReceipt()).toThrow(/unavailable|unsettled/);
    expect(seen.get(outcomeKey, { fresh: true })!.value).toEqual(saved);
    if (switched) {
      const intent = JSON.parse(fs.readFileSync(path.join(meshRoot, "main-followups", "rotations",
        `${hash(path.resolve(targetFile))}.json`), "utf8")) as { oldRoot: string; targetSessionFile: string };
      expect(intent).toMatchObject({ oldRoot, targetSessionFile: path.resolve(targetFile) });
      activeId = targetId;
      await bounded(runtime.initialize(targetContext, config));
      expect(runtime.initialized).toBe(true);
      expect(runtime.mainAgentInfo().id).toBe(targetRoot);
      expect(ownerPlanes.at(-1)!.options.hostId).toBe(targetRoot);
      expect(acks()).toHaveLength(beforeRecovery); // a new root does not own old-root control custody
      const targetMesh = runtime.mesh;
      // Explicitly resume the original native session: do not pretend the new root can recover it.
      await bounded(runtime.shutdown("resume", oldFile));
      expect(runtime.initialized).toBe(false);
      expect(closeState.mock.contexts.filter(store => store === targetMesh)).toHaveLength(1);
      activeId = oldId;
      await bounded(runtime.initialize(oldContext, config));
    } else if (flow !== "reinitialize") {
      await bounded(runtime.initialize(oldContext, config));
    }
    expect(runtime.initialized).toBe(true);
    expect(runtime.mainAgentInfo().id).toBe(oldRoot);
    const recoveredPlane = ownerPlanes.at(-1)!;
    expect(recoveredPlane.incarnation).not.toBe(originalEpoch);
    await until(() => expect(acks()).toHaveLength(beforeRecovery + 1));
    const recoveredAck = acks().at(-1)!;
    expect(recoveredAck.from.id).toBe(oldRoot);
    expect(recoveredAck.to).toBe("session:close-observer");
    expect(recoveredAck.data).toEqual({ version: 1, commandId, targetId: oldRoot,
      ownerIncarnation: originalEpoch, ...saved.acceptance });
    expect(seen.get(outcomeKey, { fresh: true })!.value).toEqual(saved);
    expect(mesh.read({ topic: "fabric.control.command", limit: 1_000 })
      .filter(event => (event.data as { commandId?: string }).commandId === commandId)).toHaveLength(1);
    expect(sendMessage).toHaveBeenCalledOnce();
    expect(closeWarnings()).toHaveLength(1);
    await bounded(runtime.shutdown("exit"));
    expect(runtime.initialized).toBe(false);
    expect(closeWarnings()).toHaveLength(1);
  } finally {
    failAck = false;
    // On RED, the first close has fenced the plane; this second public teardown can finish
    // cleanup. It does not replace/mock control.close or turn a failed assertion into success.
    await bounded(runtime.shutdown("exit")).catch(() => undefined);
    mesh.closeState(); seen.closeState();
    vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true });
  }
}, 25_000);
