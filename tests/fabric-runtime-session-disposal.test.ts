import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeases } from "../src/topology/host-leases.js";

// smarty-dev#8881 / smarty-dev#10718: AgentSession.dispose() invalidates the extension ctx and pi but
// emits no session_shutdown, so FabricState's lifecycle lease still reads current. The runtime's
// lifecycleCurrent trusted that token alone: the disposed session's participant heartbeat/change refresh
// kept ticking against the stale ctx (renewing a dead session's presence, or failing loudly once a
// source reached the stale pi). A stale bound ctx must retire the runtime's background work silently.
const STALE = "This extension ctx is stale after session replacement or reload.";

const roots: string[] = [];
const cleanup: Array<() => Promise<unknown> | unknown> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

const invalidatable = <T extends object>(target: T): { value: T; invalidate: () => void } => {
  let stale = false;
  const value = new Proxy(target, {
    get(object, key, receiver) {
      if (stale) throw new Error(STALE);
      return Reflect.get(object, key, receiver);
    },
  });
  return { value, invalidate: () => { stale = true; } };
};

describe("FabricRuntimeState after a native session dispose (smarty-dev#8881)", () => {
  it.each([false, true])("a disposed bound ctx stops the heartbeat silently (lease already retired=%s)", async leaseRetires => {
    const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
    const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
    const { normalizeFabricConfig } = await import("../src/config.js");
    const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-8881-rt-")));
    roots.push(base);
    const meshRoot = path.join(base, "mesh");
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
    const sessionId = "8881aaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const session = invalidatable({
      cwd: base, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
      isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
        getLeafId: () => null, getEntries: () => [] },
      ui: { setStatus: () => {}, notify: () => {} },
    });
    const host = invalidatable({
      on: () => () => {}, events: { emit: () => {}, on: () => () => {} },
      sendMessage: () => {}, appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "rt-main",
    });
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: meshRoot, actorPollMs: 20 }, agents: { enabled: false }, residency: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false } });
    const runtime = new FabricRuntimeState(host.value as unknown as ExtensionAPI, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(base, "unused.mjs"), skills: base,
    } });
    cleanup.push(() => runtime.shutdown("exit").catch(() => undefined));
    const directories: ParticipantDirectory[] = [];
    const register = ParticipantDirectory.prototype.registerSource;
    vi.spyOn(ParticipantDirectory.prototype, "registerSource").mockImplementation(function (this: ParticipantDirectory, source) {
      if (!directories.includes(this)) directories.push(this);
      return register.call(this, source);
    });
    const warn = vi.spyOn(console, "warn");
    const faults: unknown[] = [];
    const onFault = (error: unknown) => { faults.push(error); };
    process.on("uncaughtException", onFault);
    process.on("unhandledRejection", onFault);
    cleanup.push(() => { process.off("uncaughtException", onFault); process.off("unhandledRejection", onFault); });

    // Cover the old generation-only lease and FabricState's new freshness-aware lease.
    let leaseLive = true;
    await runtime.initialize(session.value as unknown as ExtensionContext, config, { lifecycle: () => !leaseRetires || leaseLive });
    const [directory] = directories;
    expect(directory?.heartbeatArmed).toBe(true);
    expect(runtime.lifecycleCurrent).toBe(true);           // live ctx: the lifecycle holds
    const hostId = `session:${sessionId}`;
    const renewedAt = (): number => readHostLeases(meshRoot).get(hostId)?.updatedAt ?? 0;
    expect(renewedAt()).toBeGreaterThan(0);

    // session.dispose(): Pi invalidates ctx and pi, emits nothing.
    session.invalidate();
    host.invalidate();
    leaseLive = false;
    if (leaseRetires) {
      // The facade's token can notice disposal first. That must not bypass local timer teardown.
      expect(runtime.lifecycleCurrent).toBe(false);
      expect(directory!.heartbeatArmed).toBe(false);
    }
    const disposedAt = renewedAt();
    // Past one full heartbeat (5 s), with nothing but the heartbeat's own tick probing the lifecycle:
    // its first tick retires it, nothing renews the dead session, nothing is reported.
    await new Promise((resolve) => setTimeout(resolve, 6_000));
    expect(directory!.heartbeatArmed).toBe(false);
    expect(renewedAt()).toBe(disposedAt);
    expect(runtime.lifecycleCurrent).toBe(false);          // the stale ctx is a retired lifecycle
    const lines = warn.mock.calls.map((call: unknown[]) => call.map(String).join(" "));
    expect(lines.filter((line) => /ctx is stale|degraded|background operation failed|no live session ctx/.test(line))).toEqual([]);
    expect(faults.map(String)).toEqual([]);
  }, 30_000);

  it("a disposed ctx closes control admission before the first heartbeat; a same-epoch live rebind reopens it", async () => {
    const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
    const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
    const { normalizeFabricConfig } = await import("../src/config.js");
    const { FabricControlPlane } = await import("../src/topology/control-plane.js");
    const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-8881-ctl-")));
    roots.push(base);
    const meshRoot = path.join(base, "mesh");
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
    const sessionId = "8881cccc-bbbb-cccc-dddd-eeeeeeeeeeee";
    const session = () => invalidatable({
      cwd: base, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
      isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
        getLeafId: () => null, getEntries: () => [] },
      ui: { setStatus: () => {}, notify: () => {} },
    });
    const host = {
      on: () => () => {}, events: { emit: () => {}, on: () => () => {} },
      sendMessage: () => {}, appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "rt-main",
    };
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: meshRoot, actorPollMs: 20 }, agents: { enabled: false }, residency: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false } });
    const runtime = new FabricRuntimeState(host as unknown as ExtensionAPI, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(base, "unused.mjs"), skills: base,
    } });
    cleanup.push(() => runtime.shutdown("exit").catch(() => undefined));
    const directories: ParticipantDirectory[] = [];
    const register = ParticipantDirectory.prototype.registerSource;
    vi.spyOn(ParticipantDirectory.prototype, "registerSource").mockImplementation(function (this: ParticipantDirectory, source) {
      if (!directories.includes(this)) directories.push(this);
      return register.call(this, source);
    });
    const planes: InstanceType<typeof FabricControlPlane>[] = [];
    const start = FabricControlPlane.prototype.start;
    vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation(function (this: InstanceType<typeof FabricControlPlane>, handler) {
      planes.push(this);
      return start.call(this, handler);
    });
    const pause = vi.spyOn(FabricControlPlane.prototype, "pause");
    const resume = vi.spyOn(FabricControlPlane.prototype, "resume");
    const warn = vi.spyOn(console, "warn");
    const first = session();
    await runtime.initialize(first.value as unknown as ExtensionContext, config, { lifecycle: () => true });
    const [directory] = directories;
    expect(directory?.heartbeatArmed).toBe(true);
    expect(pause).not.toHaveBeenCalled();
    const [plane] = planes;
    await vi.waitFor(() => expect(plane?.options.canConsumeMesh?.()).toBe(true), { timeout: 3_000 });

    // Dispose: receiver admission closes at once (no heartbeat or other timer needed), and the
    // control poll pauses long before a 5 s heartbeat.
    first.invalidate();
    expect(plane!.options.canConsumeMesh?.()).toBe(false);
    await vi.waitFor(() => expect(pause).toHaveBeenCalledTimes(1), { timeout: 1_000, interval: 10 });
    expect(directory!.heartbeatArmed).toBe(false);
    expect(runtime.lifecycleCurrent).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(pause).toHaveBeenCalledTimes(1);
    expect(resume).not.toHaveBeenCalled();

    // A same-epoch live rebind (FabricState.ensure) reopens control, heartbeat and root reads.
    const live = session();
    runtime.bindLifecycle(live.value as unknown as ExtensionContext, () => true);
    expect(resume).toHaveBeenCalledTimes(1);
    expect(directory!.heartbeatArmed).toBe(true);
    expect(runtime.lifecycleCurrent).toBe(true);
    expect(plane!.options.canConsumeMesh?.()).toBe(true);
    runtime.bindLifecycle(live.value as unknown as ExtensionContext, () => true);
    expect(resume).toHaveBeenCalledTimes(1);
    const lines = warn.mock.calls.map((call: unknown[]) => call.map(String).join(" "));
    expect(lines.filter((line) => /ctx is stale|degraded|background operation failed|no live session ctx/.test(line))).toEqual([]);
  }, 30_000);

  it("a successor disposed while resumeLineage awaits never reads its stale ctx for succession", async () => {
    const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
    const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
    const { normalizeFabricConfig } = await import("../src/config.js");
    const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-8881-succ-")));
    roots.push(base);
    const meshRoot = path.join(base, "mesh");
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
    const session = (sessionId: string) => invalidatable({
      cwd: base, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
      isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
        getLeafId: () => null, getEntries: () => [] },
      ui: { setStatus: () => {}, notify: () => {} },
    });
    const host = {
      on: () => () => {}, events: { emit: () => {}, on: () => () => {} },
      sendMessage: () => {}, appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "rt-main",
    };
    const config = () => normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: meshRoot, actorPollMs: 20 }, agents: { enabled: false }, residency: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false } });
    const runtime = new FabricRuntimeState(host as unknown as ExtensionAPI, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(base, "unused.mjs"), skills: base,
    } });
    cleanup.push(() => runtime.shutdown("exit").catch(() => undefined));
    const first = session("8881aaaa-0000-cccc-dddd-eeeeeeeeeeee");
    await runtime.initialize(first.value as unknown as ExtensionContext, config(), { lifecycle: () => true });

    // Successor activation: gate its resumeLineage mesh write, dispose its session in that window.
    const resume = ParticipantDirectory.prototype.resumeLineage;
    let release: (() => void) | undefined;
    let gate = true;
    vi.spyOn(ParticipantDirectory.prototype, "resumeLineage").mockImplementation(async function (this: ParticipantDirectory) {
      if (gate) { gate = false; await new Promise<void>((resolve) => { release = resolve; }); }
      return resume.call(this);
    });
    first.invalidate();
    const second = session("8881bbbb-0000-cccc-dddd-eeeeeeeeeeee");
    const pending = runtime.initialize(second.value as unknown as ExtensionContext, config(), { lifecycle: () => true });
    await vi.waitFor(() => expect(release).toBeTypeOf("function"), { timeout: 5_000 });
    second.invalidate();
    release!();
    const error = await pending.then(() => undefined, (reason: unknown) => reason);
    // The successor retires as a lifecycle fence, never by reading its stale ctx after the await.
    expect(String(error)).toMatch(/lifecycle retired/);
    expect(String(error)).not.toMatch(/ctx is stale/);
    expect(runtime.lifecycleCurrent).toBe(false);
  }, 30_000);

  // F-01: dispose while successor attribution waits on actual mesh custody. Nothing durable commits.
  it.each(["recordMainSuccessor", "confirmMainSuccessor"] as const)(
    "a successor disposed while %s awaits custody writes no attribution and opens no receiver", async helper => {
    const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
    const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
    const { normalizeFabricConfig } = await import("../src/config.js");
    const { MeshStore } = await import("../src/mesh/store.js");
    const { acquireMeshCustodyLock } = await import("../src/mesh/custody-lock.js");
    const { stageMainSuccessor } = await import("../src/topology/stall-alarms.js");
    const { RootInbox } = await import("../src/topology/root-inbox.js");
    const { createHash } = await import("node:crypto");
    const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-8881-f01-")));
    roots.push(base);
    const meshRoot = path.join(base, "mesh");
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
    const session = (sessionId: string, file: string) => invalidatable({
      cwd: base, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
      isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => file, getBranch: () => [],
        getLeafId: () => null, getEntries: () => [] },
      ui: { setStatus: () => {}, notify: () => {} },
    });
    const host = {
      on: () => () => {}, events: { emit: () => {}, on: () => () => {} },
      sendMessage: () => {}, appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "rt-main",
    };
    const config = () => normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: meshRoot, actorPollMs: 20 }, agents: { enabled: false }, residency: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false } });
    const runtime = new FabricRuntimeState(host as unknown as ExtensionAPI, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(base, "unused.mjs"), skills: base,
    } });
    cleanup.push(() => runtime.shutdown("exit").catch(() => undefined));
    const firstId = "8881aaaa-f010-cccc-dddd-eeeeeeeeeeee", secondId = "8881bbbb-f010-cccc-dddd-eeeeeeeeeeee";
    const firstFile = path.join(base, "first.jsonl"), secondFile = path.join(base, "second.jsonl");
    const first = session(firstId, firstFile);
    await runtime.initialize(first.value as unknown as ExtensionContext, config(), { lifecycle: () => true });
    const box = path.join(meshRoot, "main-followups");
    const ownerFile = path.join(box, `${encodeURIComponent(firstId)}.owner.json`);
    const firstRoot = (JSON.parse(fs.readFileSync(ownerFile, "utf8")) as { rootId: string }).rootId;
    const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
    const successorFile = path.join(box, "successors", `${hash(firstRoot)}.json`);
    const intentFile = path.join(box, "rotations", `${hash(path.resolve(secondFile))}.json`);
    // Confirm needs a staged native rotation; record runs for any local predecessor.
    if (helper === "confirmMainSuccessor") await stageMainSuccessor(new MeshStore(meshRoot, 64 * 1024, 500), firstRoot, firstId, secondFile);

    // Gate exactly the chosen helper's custody call: once it is entered, the test takes the actual
    // custody lock, then lets the helper wait on it. No fixed sleeps: every step is an observed gate.
    const custody = MeshStore.prototype.custody;
    let entered: (() => void) | undefined;
    let proceed: (() => void) | undefined;
    const inHelper = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { proceed = resolve; });
    let waiting: Promise<unknown> | undefined;
    vi.spyOn(MeshStore.prototype, "custody").mockImplementation(function (this: InstanceType<typeof MeshStore>, operation, timeout) {
      if (!waiting && new Error().stack?.includes(helper)) {
        entered!();
        waiting = held.then(() => custody.call(this, operation, timeout));
        return waiting as never;
      }
      return custody.call(this, operation, timeout);
    });
    const starts = vi.spyOn(RootInbox.prototype, "start");
    first.invalidate();
    const second = session(secondId, secondFile);
    const before = [ownerFile, successorFile, intentFile].map(file => fs.existsSync(file) ? fs.readFileSync(file, "utf8") : undefined);
    const startsBefore = starts.mock.calls.length;
    const activation = runtime.initialize(second.value as unknown as ExtensionContext, config(), { lifecycle: () => true });
    await inHelper;
    const after = helper === "confirmMainSuccessor"
      ? [ownerFile, successorFile, intentFile].map(file => fs.existsSync(file) ? fs.readFileSync(file, "utf8") : undefined) : before;
    const releaseCustody = await acquireMeshCustodyLock(meshRoot);
    let released = false;
    cleanup.push(() => { if (!released) releaseCustody(); });
    proceed!();
    // Observed pending: the helper's custody acquisition is still waiting on the held lock.
    let settled = false;
    void waiting!.then(() => { settled = true; }, () => { settled = true; });
    await new Promise(resolve => setImmediate(resolve));
    expect(settled).toBe(false);
    second.invalidate();                                  // native dispose: no session_shutdown
    released = true;
    releaseCustody();
    const error = await activation.then(() => undefined, (reason: unknown) => reason);
    expect(String(error)).toMatch(/lifecycle retired/);
    expect(String(error)).not.toMatch(/ctx is stale/);
    // Nothing from the stale activation committed: owner, successor and rotation intent unchanged
    // from the moment the helper began waiting, and no receiver start/admission.
    const now = [ownerFile, successorFile, intentFile].map(file => fs.existsSync(file) ? fs.readFileSync(file, "utf8") : undefined);
    expect(now).toEqual(after);
    if (helper === "confirmMainSuccessor") expect(now[2]).toBeDefined();
    else expect(now).toEqual(before);
    expect(starts.mock.calls.length).toBe(startsBefore);
    expect(runtime.lifecycleCurrent).toBe(false);
  }, 30_000);
});
