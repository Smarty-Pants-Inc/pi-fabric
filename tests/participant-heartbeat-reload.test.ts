import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeases } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#5962: after a Main's /reload, the participant heartbeat/change refresh kept
// reading the ExtensionContext captured before ctx.reload() and logged, every tick,
// "background operation failed: This extension ctx is stale after session replacement or reload".
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
const reloadable = <T extends object>(target: T): { value: T; reload: () => void } => {
  let stale = false;
  const value = new Proxy(target, {
    get(object, key, receiver) {
      if (stale) throw new Error(STALE);
      return Reflect.get(object, key, receiver);
    },
  });
  return { value, reload: () => { stale = true; } };
};

describe("participant heartbeat across ctx.reload() (smarty-dev#5962)", () => {
  it("a retired lifecycle skips heartbeat ticks quietly and renews again once rebound", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-hb-reload-"));
    roots.push(root);
    const identity: MeshIdentity = { id: "session:hb", sessionId: "hb", name: "Main", kind: "main" };
    let ctx = reloadable({ name: "main" });
    let live = true;
    const record = (): FabricParticipantRecord => ({
      format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
      kind: "root", name: ctx.value.name, status: "idle", capabilities: ["steer", "followUp", "fabric"],
      runner: "pi", transport: "host", controlProtocol: "v1", sessionId: "hb", cwd: process.cwd(), startedAt: 1, updatedAt: Date.now(),
    });
    const directory = new ParticipantDirectory(new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000), {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 10_000,
      reapDeadHosts: false, live: () => live,
    });
    directory.registerSource(() => [record()]);
    cleanup.push(() => directory.close());
    const observer = new ParticipantDirectory(new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000), {
      enabled: true, hostId: "observer", rootId: "observer", identity: { ...identity, id: "observer" },
    });
    const warn = vi.spyOn(console, "warn");
    await directory.start();
    const renewedAt = (): number => readHostLeases(path.join(root, "mesh")).get(identity.id)?.updatedAt ?? 0;
    expect(observer.get(identity.id, Date.now(), { fresh: true })).toMatchObject({ name: "main", stale: false });
    const before = renewedAt();
    expect(before).toBeGreaterThan(0);

    // ctx.reload(): the captured ctx goes stale and the lifecycle lease retires together.
    ctx.reload();
    live = false;
    directory.scheduleRefresh();
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(staleWarnings(warn)).toEqual([]);
    expect(renewedAt()).toBe(before);

    // The next ensure() rebinds a live ctx: the same heartbeat renews the participant again.
    ctx = reloadable({ name: "main" });
    live = true;
    await vi.waitFor(() => expect(renewedAt()).toBeGreaterThan(before), { timeout: 3_000 });
    expect(staleWarnings(warn)).toEqual([]);
  }, 15_000);

  it("a real FabricRuntimeState keeps renewing its Main through a reload without reading the stale ctx", async () => {
    const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
    const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
    const { normalizeFabricConfig } = await import("../src/config.js");
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-hb-runtime-"));
    roots.push(base);
    const meshRoot = path.join(base, "mesh");
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
    const sessionId = "5962aaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const session = () => reloadable({
      cwd: base, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
      isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
        getLeafId: () => null, getEntries: () => [] },
      ui: { setStatus: () => {}, notify: () => {} },
    });
    const host = reloadable({
      on: () => () => {}, events: { emit: () => {}, on: () => () => {} },
      sendMessage: () => {}, appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "hb-main",
    });
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: meshRoot, actorPollMs: 20 }, agents: { enabled: false }, residency: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false } });
    const runtime = new FabricRuntimeState(host.value as unknown as ExtensionAPI, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(base, "unused.mjs"), skills: base,
    } });
    const observer = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
      enabled: true, hostId: "observer", rootId: "observer", identity: { id: "observer", name: "o", kind: "main", sessionId: "o" },
    });
    const main = (): FabricParticipantRecord | undefined =>
      observer.list({ kinds: ["root"], includeStale: true, fresh: true }).find((entry) => entry.sessionId === sessionId);
    const warn = vi.spyOn(console, "warn");
    const faults: unknown[] = [];
    const onFault = (error: unknown) => { faults.push(error); };
    process.on("uncaughtException", onFault);
    process.on("unhandledRejection", onFault);
    cleanup.push(() => { process.off("uncaughtException", onFault); process.off("unhandledRejection", onFault); });
    cleanup.push(() => runtime.shutdown("exit").catch(() => undefined));

    let generation = 1;
    const first = session();
    runtime.bindLifecycle?.(first.value as unknown as ExtensionContext, () => generation === 1);
    await runtime.initialize(first.value as unknown as ExtensionContext, config);
    expect(main()).toMatchObject({ name: "hb-main", model: "faux/m1" });
    const hostId = main()!.ownerHostId;
    const renewedAt = (): number => readHostLeases(meshRoot).get(hostId)?.updatedAt ?? 0;
    expect(renewedAt()).toBeGreaterThan(0);

    // ctx.reload(): Pi invalidates the captured ctx and pi; the lease this ctx came with retires.
    first.reload();
    host.reload();
    generation = 2;
    const reloadedAt = Date.now();
    // One full heartbeat (5 s) plus margin: the timer fires against the retired binding.
    await new Promise((resolve) => setTimeout(resolve, 5_600));
    expect(staleWarnings(warn)).toEqual([]);
    expect(renewedAt()).toBeLessThan(reloadedAt);

    // The next ensure() binds the live ctx of the current lifecycle: the heartbeat renews again,
    // from the last live snapshot where the retired pi can no longer be read.
    const second = session();
    runtime.bindLifecycle?.(second.value as unknown as ExtensionContext, () => generation === 2);
    await vi.waitFor(() => expect(renewedAt()).toBeGreaterThan(reloadedAt), { timeout: 7_000, interval: 100 });
    expect(main()).toMatchObject({ name: "hb-main", model: "faux/m1", stale: false });
    expect(staleWarnings(warn)).toEqual([]);
    expect(faults.map(String)).toEqual([]);
  }, 30_000);
});
