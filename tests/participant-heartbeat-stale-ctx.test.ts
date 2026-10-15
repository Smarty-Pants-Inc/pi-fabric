import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  type AgentSession,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeases } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#10004: after a /reload or session replacement a Main printed, every heartbeat (~5 s),
// "[pi-fabric] participant heartbeat/change refresh: background operation failed: This extension ctx
// is stale after session replacement or reload", and its heartbeat stopped. The background tick checked
// the lifecycle lease only before its awaits; whatever the refresh then reached on a ctx/pi Pi had
// invalidated surfaced as a generic failure, the retired epoch's timer kept ticking, and nothing re-armed.
const STALE = "This extension ctx is stale after session replacement or reload. Do not use a captured pi or command ctx after ctx.reload().";

const roots: string[] = [];
const cleanup: Array<() => Promise<unknown> | unknown> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

const lines = (warn: ReturnType<typeof vi.spyOn>): string[] =>
  warn.mock.calls.map((call: unknown[]) => call.map(String).join(" "));
const staleFailures = (warn: ReturnType<typeof vi.spyOn>): string[] => lines(warn).filter((line) => /ctx is stale/.test(line));
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Pi's ctx/pi after invalidate(): every read throws the stale message (mirrors ExtensionRunner.invalidate). */
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

describe("participant heartbeat with a stale ctx after reload (smarty-dev#10004)", () => {
  it("a refresh that reaches a ctx Pi invalidated stops the heartbeat quietly; a live rebind re-arms one timer", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-10004-dir-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:sd10004", sessionId: "sd10004", name: "Main", kind: "main" };
    // The lease still reads current: Pi invalidated the ctx a source reaches, not the token.
    let ctx = invalidatable({ name: "main" });
    const notices: number[] = [];
    const record = (): FabricParticipantRecord => ({
      format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      kind: "root", name: ctx.value.name, status: "idle", capabilities: ["steer", "followUp", "fabric"],
      runner: "pi", transport: "host", controlProtocol: "v1", sessionId: "sd10004", cwd: process.cwd(), startedAt: 1, updatedAt: Date.now(),
    });
    const directory = new ParticipantDirectory(new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000), {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 10_000,
      reapDeadHosts: false, live: () => true, onLifecycleLost: (ticks) => { notices.push(ticks); },
    });
    directory.registerSource(() => [record()]);
    cleanup.push(() => directory.close());
    const warn = vi.spyOn(console, "warn");
    await directory.start();
    const renewedAt = (): number => readHostLeases(path.join(root, "mesh")).get(identity.id)?.updatedAt ?? 0;

    ctx.invalidate();
    await sleep(800);
    // Eight heartbeats: no per-tick stale failure, one visible notice, and the timer is cleared.
    expect(staleFailures(warn)).toEqual([]);
    expect(notices).toHaveLength(1);
    expect(lines(warn).filter((line) => /heartbeat is stopped until a live session rebinds/.test(line))).toHaveLength(1);
    expect(directory.degraded).toBe(true);
    expect(directory.heartbeatArmed).toBe(false);
    const stoppedAt = renewedAt();
    await sleep(300);
    expect(renewedAt()).toBe(stoppedAt);

    // The successor's ensure rebinds a live ctx: exactly one timer again, and presence refreshes.
    ctx = invalidatable({ name: "main" });
    directory.resume();
    directory.resume();
    expect(directory.heartbeatArmed).toBe(true);
    await vi.waitFor(() => expect(renewedAt()).toBeGreaterThan(stoppedAt), { timeout: 3_000 });
    expect(directory.degraded).toBe(false);
    expect(staleFailures(warn)).toEqual([]);
  }, 15_000);

  it("smarty-dev#8881: an owner retired during an in-flight change refresh suspends quietly, with no degraded notice", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-8881-dir-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:sd8881", sessionId: "sd8881", name: "Main", kind: "main" };
    let ownerLive = true;
    let retireDuringNextRead = false;
    let staleReads = 0;
    const notices: number[] = [];
    const record = (): FabricParticipantRecord => ({
      format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      kind: "root", name: "main", status: "idle", capabilities: ["steer", "followUp", "fabric"],
      runner: "pi", transport: "host", controlProtocol: "v1", sessionId: "sd8881", cwd: process.cwd(), startedAt: 1, updatedAt: Date.now(),
    });
    const directory = new ParticipantDirectory(new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000), {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 10_000,
      reapDeadHosts: false, live: () => ownerLive, onLifecycleLost: (ticks) => { notices.push(ticks); },
    });
    directory.registerSource(() => {
      // The refresh passed its pre-await liveness check; the SDK then disposes the session and the
      // source reaches its invalidated ctx. The owner lifecycle already reads retired.
      if (retireDuringNextRead) {
        retireDuringNextRead = false;
        ownerLive = false;
        staleReads += 1;
        throw new Error(STALE);
      }
      return [record()];
    });
    cleanup.push(() => directory.close());
    const warn = vi.spyOn(console, "warn");
    await directory.start();
    const renewedAt = (): number => readHostLeases(path.join(root, "mesh")).get(identity.id)?.updatedAt ?? 0;
    expect(renewedAt()).toBeGreaterThan(0);

    retireDuringNextRead = true;
    directory.scheduleRefresh();
    await vi.waitFor(() => expect(staleReads).toBe(1), { timeout: 3_000 });
    await sleep(500);
    // A retired owner is not a degraded one: no notice, no generic failure, no lifecycle-lost op.
    expect(staleFailures(warn)).toEqual([]);
    expect(lines(warn).filter((line) => /degraded|background operation failed/.test(line))).toEqual([]);
    expect(notices).toEqual([]);
    expect(directory.degraded).toBe(false);
    expect(directory.heartbeatArmed).toBe(false);
    const stoppedAt = renewedAt();
    await sleep(300);
    expect(renewedAt()).toBe(stoppedAt);

    // A fresh live owner rebinds: one timer again and presence renews.
    ownerLive = true;
    directory.resume();
    expect(directory.heartbeatArmed).toBe(true);
    await vi.waitFor(() => expect(renewedAt()).toBeGreaterThan(stoppedAt), { timeout: 3_000 });
    expect(lines(warn).filter((line) => /degraded|background operation failed|ctx is stale/.test(line))).toEqual([]);
    expect(notices).toEqual([]);
  }, 15_000);

  it("re-initialize stops the retired epoch's heartbeat at once; only the successor's timer runs and renews", async () => {
    const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
    const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
    const { normalizeFabricConfig } = await import("../src/config.js");
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-10004-rt-"));
    roots.push(base);
    const meshRoot = path.join(base, "mesh");
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
    const sessionId = "10004aaa-bbbb-cccc-dddd-eeeeeeeeeeee";
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
    const config = () => normalizeFabricConfig({ fullCodeMode: false,
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
    const warn = vi.spyOn(console, "warn");

    let generation = 1;
    const first = session();
    await runtime.initialize(first.value as unknown as ExtensionContext, config(), { lifecycle: () => generation === 1 });
    const [old] = directories;
    expect(old?.heartbeatArmed).toBe(true);
    const hostId = `session:${sessionId}`;
    const renewedAt = (): number => readHostLeases(meshRoot).get(hostId)?.updatedAt ?? 0;
    expect(renewedAt()).toBeGreaterThan(0);

    // Reload: Pi invalidates the old ctx; the successor initialize starts a new epoch.
    first.invalidate();
    generation = 2;
    const second = session();
    const reinit = runtime.initialize(second.value as unknown as ExtensionContext, config(), { lifecycle: () => generation === 2 });
    // Synchronously, before teardown awaits: the retired epoch's timer is already cleared.
    expect(old!.heartbeatArmed).toBe(false);
    await reinit;
    const reloadedAt = Date.now();
    expect(directories.length).toBe(2);
    expect(directories.filter((directory) => directory.heartbeatArmed)).toEqual([directories[1]]);
    // A successor ensure() that rebinds the same epoch never arms a second timer, nor the old one.
    runtime.bindLifecycle(second.value as unknown as ExtensionContext, () => generation === 2);
    expect(directories.filter((directory) => directory.heartbeatArmed)).toEqual([directories[1]]);

    // One full heartbeat (5 s) plus margin: presence renews from the successor, with no stale failure.
    await vi.waitFor(() => expect(renewedAt()).toBeGreaterThan(reloadedAt), { timeout: 8_000, interval: 100 });
    expect(staleFailures(warn)).toEqual([]);
    expect(directories.filter((directory) => directory.heartbeatArmed)).toEqual([directories[1]]);
  }, 30_000);

  const fabricEntry = path.resolve("dist/index.js");
  it.skipIf(!fs.existsSync(fabricEntry))("a real Pi session: after /reload no stale lines and the successor renews presence", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-10004-real-")));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    const meshRoot = path.join(root, "mesh");
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ mesh: { enabled: true, announce: true, root: meshRoot } }));
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
    const warn = vi.spyOn(console, "warn");
    const faults: unknown[] = [];
    const onFault = (error: unknown) => { faults.push(error); };
    process.on("uncaughtException", onFault);
    process.on("unhandledRejection", onFault);
    cleanup.push(() => { process.off("uncaughtException", onFault); process.off("unhandledRejection", onFault); });
    const faux = fauxProvider({ tokensPerSecond: 1_000 });
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      additionalExtensionPaths: [fabricEntry],
    });
    await loader.reload();
    const sessionManager = SessionManager.inMemory(root);
    const { session } = await createAgentSession({ cwd: root, agentDir, modelRuntime, model: faux.getModel(), resourceLoader: loader, sessionManager });
    const sessions: AgentSession[] = [session];
    cleanup.push(() => { for (const value of sessions) value.dispose(); });
    // A bound host (as in the TUI) so Pi emits session_start to the reloaded extensions.
    await session.bindExtensions({ shutdownHandler: () => {} });
    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })), fauxAssistantMessage("done")]);
    await session.prompt("activate");
    const hostId = `session:${sessionManager.getSessionId()}`;
    expect(readHostLeases(meshRoot).get(hostId)?.updatedAt ?? 0).toBeGreaterThan(0);
    await session.reload();
    const reloadedAt = Date.now();
    await vi.waitFor(() => expect(readHostLeases(meshRoot).get(hostId)?.updatedAt ?? 0).toBeGreaterThan(reloadedAt),
      { timeout: 12_000, interval: 200 });
    await sleep(5_500);
    expect(staleFailures(warn)).toEqual([]);
    expect(lines(warn).filter((line) => /background operation failed/.test(line))).toEqual([]);
    expect(faults.map(String)).toEqual([]);
  }, 60_000);
});
