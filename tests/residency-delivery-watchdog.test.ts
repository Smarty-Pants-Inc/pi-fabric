import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { kernelFenceAvailable } from "../src/residency/file-lock.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, type ResidentHostConfig, type ResidentPiModelState } from "../src/residency/protocol.js";

// Only the fence capability is mocked: no launcher, resident, Pi or model runs.
vi.mock("../src/residency/file-lock.js", async (original) => ({
  ...await original<typeof import("../src/residency/file-lock.js")>(),
  kernelFenceAvailable: vi.fn(() => true),
}));

const owned: Array<{ root: string; client: ResidencyClient }> = [];
function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-delivery-watchdog-"));
  const rootId = `session:${path.basename(root)}`;
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId, sessionId: path.basename(root), cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true,
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 250 }, agents: { ...DEFAULT_FABRIC_CONFIG.agents },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "worker.js", fabricExtensionPath: "index.js",
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  const mesh = new MeshStore(config.meshRoot, 1_048_576, 1_000);
  const delivered: string[] = [];
  const main = { id: rootId, local: true, deliverAgent: vi.fn((input: { message: string }) => {
    delivered.push(input.message);
    return { queued: true, messageId: input.message, routed: "main" as const };
  }) } as unknown as FabricMainAgentTarget;
  const piModelState = vi.fn<() => ResidentPiModelState>(() => ({ available: [], aliases: {} }));
  const client = new ResidencyClient({ config, mesh, mainAgent: main, participants: {} as never, piModelState });
  owned.push({ root, client });
  const startHost = vi.spyOn(client, "ensureHost").mockResolvedValue({} as Awaited<ReturnType<typeof client.ensureHost>>);
  const actors = (status = "idle") => {
    fs.mkdirSync(config.actorRoot, { recursive: true });
    fs.writeFileSync(path.join(config.actorRoot, "actors.json"), JSON.stringify({ actors: [
      { id: "actor", rootId, residency: "durable", status },
    ] }));
  };
  const put = (id: string, key = id, ifVersion = 0) => mesh.put({
    key: `${residentDeliveryPrefix(rootId)}${key}`, ifVersion,
    identity: { id: residentHostId(rootId), name: "resident", kind: "main" },
    value: { format: RESIDENT_HOST_FORMAT, rootId, id, message: id,
      from: { id: "actor", name: "actor", kind: "actor" }, delivery: "followUp", triggerTurn: true, createdAt: Date.now() },
  });
  return { root, config, mesh, main, delivered, client, startHost, actors, put, piModelState };
}

function watches() {
  const watchers: Array<EventEmitter & { close: ReturnType<typeof vi.fn>; unref: ReturnType<typeof vi.fn> }> = [];
  const callbacks: Array<(event: string, name: string | Buffer | null) => void> = [];
  const watch = vi.spyOn(fs, "watch").mockImplementation(((...args: unknown[]) => {
    callbacks.push(args.at(-1) as typeof callbacks[number]);
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn(), unref: vi.fn() });
    watcher.close.mockImplementation(() => watcher.emit("close"));
    watchers.push(watcher);
    return watcher;
  }) as unknown as typeof fs.watch);
  return { watch, watchers, notify: (name: string | Buffer | null = "state.json", event = "rename", index = 0) => {
    expect(callbacks[index], "delivery directory watcher must be installed").toBeTypeOf("function");
    callbacks[index]!(event, name);
  } };
}

afterEach(async () => {
  vi.useRealTimers();
  for (const { root, client } of owned.splice(0)) {
    await client.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
  vi.restoreAllMocks();
  vi.mocked(kernelFenceAvailable).mockReturnValue(true);
});

describe("delivery wake preserves the independent resident watchdog", () => {
  it("checks new durable work within 5s without scanning delivery state before 30s", async () => {
    const h = fixture();
    watches();
    vi.useFakeTimers();
    const scans = vi.spyOn(h.mesh, "listAll");
    h.client.start();
    await vi.advanceTimersByTimeAsync(0);
    const initialScans = scans.mock.calls.length;
    h.actors();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(h.startHost).toHaveBeenCalled();
    expect(scans).toHaveBeenCalledTimes(initialScans);
    await vi.advanceTimersByTimeAsync(24_999);
    expect(scans).toHaveBeenCalledTimes(initialScans);
    await vi.advanceTimersByTimeAsync(1);
    expect(scans.mock.calls.length).toBeGreaterThan(initialScans);
  });

  it("delivery backpressure retries at 1s but does not postpone the watchdog", async () => {
    const h = fixture();
    const watch = watches();
    h.actors();
    await h.put("blocked");
    vi.mocked(h.main.deliverAgent).mockImplementation(() => { throw new Error("receiver full"); });
    vi.useFakeTimers();
    h.client.start();
    await vi.advanceTimersByTimeAsync(0);
    for (let i = 0; i < 100; i++) watch.notify();
    await vi.advanceTimersByTimeAsync(999);
    expect(h.main.deliverAgent).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(h.main.deliverAgent).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(4_000);
    expect(h.startHost).toHaveBeenCalled();
    expect(h.main.deliverAgent).toHaveBeenCalledTimes(6);
  });

  it("keeps watchdog failure backoff while delivery events continue", async () => {
    const h = fixture();
    const watch = watches();
    h.actors();
    h.startHost.mockRejectedValue(new Error("offline host start"));
    vi.useFakeTimers();
    h.client.start();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(h.startHost).toHaveBeenCalledTimes(1);
    for (let i = 0; i < 20; i++) { watch.notify(); await vi.advanceTimersByTimeAsync(100); }
    expect(h.startHost).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(8_000);
    expect(h.startHost).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(h.startHost).toHaveBeenCalledTimes(3);
    await h.client.close();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(h.startHost).toHaveBeenCalledTimes(3);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("syncs models once on idempotent start, supports explicit sync, and cannot restart after close", async () => {
    const h = fixture();
    const watch = watches();
    const models: ResidentPiModelState = { available: [], aliases: {}, defaultModel: "offline/default" };
    h.piModelState.mockReturnValue(models);
    fs.mkdirSync(h.config.residencyRoot, { recursive: true });
    vi.useFakeTimers();
    h.client.start();
    h.client.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.piModelState).toHaveBeenCalledTimes(1);
    expect(watch.watchers).toHaveLength(1);
    expect(watch.watchers[0]!.unref).toHaveBeenCalled();
    expect(h.config.piModels).toEqual(models);
    expect(h.config.piModels).not.toBe(models);
    models.defaultModel = "offline/updated";
    expect(h.config.piModels?.defaultModel).toBe("offline/default");
    h.client.syncPiModels();
    expect(h.piModelState).toHaveBeenCalledTimes(2);
    expect(JSON.parse(fs.readFileSync(path.join(h.config.residencyRoot, "config.json"), "utf8")).piModels.defaultModel)
      .toBe("offline/updated");
    await h.client.close();
    h.client.start();
    watch.notify();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(watch.watchers).toHaveLength(1);
    expect(watch.watchers[0]!.close).toHaveBeenCalledTimes(1);
    expect(h.piModelState).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("isolates a throwing record and does not readmit healthy records on retry", async () => {
    const h = fixture();
    watches();
    await h.put("a-blocked");
    await h.put("b-healthy");
    const normal = vi.mocked(h.main.deliverAgent).getMockImplementation()!;
    let blocked = true;
    vi.mocked(h.main.deliverAgent).mockImplementation((input) => {
      if (input.message === "a-blocked" && blocked) throw new Error("one record rejected");
      return normal(input);
    });
    vi.useFakeTimers();
    h.client.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.delivered).toEqual(["b-healthy"]);
    expect(h.mesh.listAll(residentDeliveryPrefix(h.config.rootId), { fresh: true })).toHaveLength(1);
    blocked = false;
    await vi.advanceTimersByTimeAsync(1_000);
    expect(h.delivered).toEqual(["b-healthy", "a-blocked"]);
    expect(h.mesh.listAll(residentDeliveryPrefix(h.config.rootId), { fresh: true })).toHaveLength(0);
  });

  it("drains a new version at the same key promptly despite a watch storm during deletion", async () => {
    const h = fixture();
    const watch = watches();
    await h.put("slot");
    const originalDelete = h.mesh.delete.bind(h.mesh);
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    vi.spyOn(h.mesh, "delete").mockImplementationOnce(async (input) => {
      await blocked;
      const receipt = await originalDelete(input);
      // Reuse the key only AFTER conditional deletion. This is new work, not
      // an old receiver-retained envelope deserving a 1s backpressure floor.
      await h.put("replacement", "slot", receipt.version);
      return receipt;
    });
    vi.useFakeTimers();
    try {
      h.client.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(h.delivered).toEqual(["slot"]);
      for (let i = 0; i < 100; i++) watch.notify(Buffer.from("state.json"));
    } finally {
      // Never strand teardown behind the test's own blocked delete on failure.
      release();
    }
    // Fake timers defer a zero-delay timer created while advancing to the
    // next 1ms tick. This remains prompt and must not wait for retry's 1000ms.
    await vi.advanceTimersByTimeAsync(1);
    expect(h.delivered).toEqual(["slot", "replacement"]);
    expect(h.mesh.listAll(residentDeliveryPrefix(h.config.rootId), { fresh: true })).toHaveLength(0);
  });

  // fs.watch filenames may be absent (Windows), strings, or Buffers. Native
  // path.join above exercises the actual platform rather than skipping win32.
  it.each([null, Buffer.from("state.json"), "events.jsonl"])("accepts fs.watch filename %s and coalesces an event burst", async (name) => {
    const h = fixture();
    const watch = watches();
    vi.useFakeTimers();
    h.client.start();
    await vi.advanceTimersByTimeAsync(0);
    await h.put("burst");
    const scans = vi.spyOn(h.mesh, "listAll");
    for (let i = 0; i < 100; i++) watch.notify(name);
    await vi.advanceTimersByTimeAsync(0);
    expect(h.delivered).toEqual(["burst"]);
    // A successful nonempty drain may re-read once to detect retained/new work;
    // 100 notifications must still cause one drain, not 100 full scans.
    expect(scans.mock.calls.length).toBeGreaterThan(0);
    expect(scans.mock.calls.length).toBeLessThanOrEqual(2);
  });

  it("reconciles a missing mesh directory and installs a watcher after recreation", async () => {
    const h = fixture();
    const watch = watches();
    fs.rmSync(h.mesh.root, { recursive: true, force: true });
    watch.watch.mockImplementationOnce(() => { throw Object.assign(new Error("directory absent"), { code: "ENOENT" }); });
    vi.useFakeTimers();
    h.client.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.delivered).toEqual([]);
    await h.put("after-recreation");
    await vi.advanceTimersByTimeAsync(29_999);
    expect(h.delivered).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(h.delivered).toEqual(["after-recreation"]);
    expect(watch.watchers).toHaveLength(1);
    await h.put("watched-again");
    watch.notify(null);
    await vi.advanceTimersByTimeAsync(0);
    expect(h.delivered).toEqual(["after-recreation", "watched-again"]);
  });
});
