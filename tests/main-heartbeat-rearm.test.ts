import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricState } from "../src/fabric-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeases } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#5962 / #4313: a Main's own participant heartbeat must survive a session replacement
// or /reload inside the Main. Pi invalidates the old generation's ctx and pi; the successor must
// re-arm the heartbeat with its live ctx, and when no live ctx can be bound the directory must say
// so visibly (warning + fleet ops event) and mark itself degraded instead of vanishing silently.
const STALE = "This extension ctx is stale after session replacement or reload. Do not use a captured pi or command ctx after ctx.reload().";

const roots: string[] = [];
const cleanup: Array<() => Promise<unknown> | unknown> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

const staleWarnings = (warn: ReturnType<typeof vi.spyOn>): string[] =>
  warn.mock.calls.map((call: unknown[]) => call.map(String).join(" ")).filter((line: string) => /ctx is stale/.test(line));

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

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const harness = () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-main-hb-"));
  roots.push(base);
  const meshRoot = path.join(base, "mesh");
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
  fs.mkdirSync(path.join(base, ".pi"), { recursive: true });
  // Default mesh.announce (false): only first use, or a re-arm, publishes this Main.
  fs.writeFileSync(path.join(base, ".pi", "fabric.json"), JSON.stringify({ fullCodeMode: false,
    mesh: { enabled: true, root: meshRoot, actorPollMs: 20 }, agents: { enabled: false }, residency: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false } }));
  const session = (sessionId: string) => invalidatable({
    cwd: base, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
    isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
      getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: () => {}, notify: () => {} },
  });
  const host = () => invalidatable({
    on: () => () => {}, events: { emit: () => {}, on: () => () => {} },
    sendMessage: () => {}, appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "hb-main",
  });
  const paths = {
    extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
    residentHost: path.join(base, "unused.mjs"), skills: base,
  };
  /** One extension generation: Pi loads a fresh FabricState per reload/session runtime. */
  const generation = () => {
    const pi = host();
    const state = new FabricState(pi.value as unknown as ExtensionAPI, new CapturedToolCatalog(), { paths });
    cleanup.push(() => state.shutdown("exit").catch(() => undefined));
    return { pi, state };
  };
  /** index.ts session_start: bootstrap, then activate only when shouldEagerlyActivate says so. */
  const sessionStart = async (state: FabricState, context: ExtensionContext): Promise<void> => {
    await state.bootstrap(context);
    if (state.shouldEagerlyActivate(context)) await state.ensure(context);
  };
  const leaseAge = (hostId: string): number => {
    const updatedAt = readHostLeases(meshRoot).get(hostId)?.updatedAt;
    return updatedAt === undefined ? Number.POSITIVE_INFINITY : Date.now() - updatedAt;
  };
  return { base, meshRoot, session, generation, sessionStart, leaseAge };
};

describe("a Main's own heartbeat across reload and session replacement (smarty-dev#5962, #4313)", () => {
  it.each([
    ["reload", "5962bbbb-0000-0000-0000-000000000001", "5962bbbb-0000-0000-0000-000000000001"],
    ["new", "5962bbbb-0000-0000-0000-000000000002", "5962bbbb-0000-0000-0000-000000000003"],
  ])("after %s the Main's lease keeps renewing (age < 15 s) with no stale-ctx error", async (reason, firstId, nextId) => {
    const h = harness();
    const warn = vi.spyOn(console, "warn");
    const faults: unknown[] = [];
    const onFault = (error: unknown) => { faults.push(error); };
    process.on("uncaughtException", onFault);
    process.on("unhandledRejection", onFault);
    cleanup.push(() => { process.off("uncaughtException", onFault); process.off("unhandledRejection", onFault); });

    // Generation 1: the Main publishes itself on first Fabric use (heartbeat armed).
    const one = h.generation();
    const first = h.session(firstId);
    await h.sessionStart(one.state, first.value as unknown as ExtensionContext);
    await one.state.ensure(first.value as unknown as ExtensionContext);
    expect(h.leaseAge(`session:${firstId}`)).toBeLessThan(2_000);

    // Pi: session_shutdown to the old generation, then invalidate its ctx and pi.
    await one.state.shutdown(reason);
    first.invalidate();
    one.pi.invalidate();

    // Generation 2: a fresh extension instance gets session_start with the live ctx.
    const two = h.generation();
    const next = h.session(nextId);
    await h.sessionStart(two.state, next.value as unknown as ExtensionContext);

    // Three heartbeats (5 s) with no Fabric tool use in the successor.
    await sleep(16_000);
    expect(h.leaseAge(`session:${nextId}`)).toBeLessThan(15_000);
    expect(staleWarnings(warn)).toEqual([]);
    expect(faults.map(String)).toEqual([]);
  }, 40_000);

  it("a heartbeat that can never rebind a live ctx raises one notice and marks the participant degraded", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-main-hb-lost-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:lost", sessionId: "lost", name: "Main", kind: "main" };
    let live = true;
    const notices: number[] = [];
    const record = (): FabricParticipantRecord => ({
      format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      kind: "root", name: "main", status: "idle", capabilities: ["steer", "followUp", "fabric"],
      runner: "pi", transport: "host", controlProtocol: "v1", sessionId: "lost", cwd: process.cwd(), startedAt: 1, updatedAt: Date.now(),
    });
    const options = {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 10_000,
      reapDeadHosts: false as const, live: () => live,
      onLifecycleLost: (ticks: number) => { notices.push(ticks); },
    };
    const directory = new ParticipantDirectory(new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000), options);
    directory.registerSource(() => [record()]);
    cleanup.push(() => directory.close());
    const warn = vi.spyOn(console, "warn");
    await directory.start();
    expect(directory.degraded).toBe(false);

    // The lifecycle retires and nothing ever rebinds it: two ticks stay quiet, the third reports once.
    live = false;
    await sleep(1_000);
    expect(notices).toHaveLength(1);
    expect(notices[0]).toBeGreaterThan(2);
    expect(directory.degraded).toBe(true);
    expect(warn.mock.calls.map((call) => String(call[0])).filter((line) => /no live session ctx/.test(line))).toHaveLength(1);
    expect(staleWarnings(warn)).toEqual([]);

    // A live rebind clears the degraded mark and renews the lease again.
    live = true;
    await vi.waitFor(() => expect(directory.degraded).toBe(false), { timeout: 2_000 });
  }, 15_000);

  it("the Main runtime routes a lost lifecycle to the fleet ops topic", async () => {
    const h = harness();
    const directories: ParticipantDirectory[] = [];
    const register = ParticipantDirectory.prototype.registerSource;
    vi.spyOn(ParticipantDirectory.prototype, "registerSource").mockImplementation(function (this: ParticipantDirectory, source) {
      if (!directories.includes(this)) directories.push(this);
      return register.call(this, source);
    });
    const one = h.generation();
    const sessionId = "5962bbbb-0000-0000-0000-000000000004";
    const first = h.session(sessionId);
    await h.sessionStart(one.state, first.value as unknown as ExtensionContext);
    await one.state.ensure(first.value as unknown as ExtensionContext);
    const [directory] = directories;
    const lost = (directory as unknown as { options: { onLifecycleLost?: (ticks: number) => void } }).options.onLifecycleLost;
    expect(lost).toBeTypeOf("function");
    lost!(3);
    const mesh = new MeshStore(h.meshRoot, 64 * 1024, 1_000);
    await vi.waitFor(() => {
      const events = mesh.read({ topic: "ops.fabric.presence" });
      expect(JSON.stringify(events)).toContain("fabric.presence.degraded");
      expect(JSON.stringify(events)).toContain(`session:${sessionId}`);
    }, { timeout: 3_000 });
  }, 20_000);
});
