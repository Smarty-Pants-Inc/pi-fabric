import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshBackgroundRetry } from "../src/core/atomic-write.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { captureSafetyTick, type SafetyTickCapture } from "./helpers/resident-safety-tick.js";

// smarty-dev#11107: requests/ is drained on native fs.watch events plus one slow unref'd safety
// tick; the 50 ms poll exists only when the watch is unavailable. No product deletion runs here:
// retention is observed through recorded calls only.
const platform = vi.hoisted(() => ({ retentionV2: process.platform !== "win32" }));
vi.mock("../src/storage/retention-platform.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../src/storage/retention-platform.js")>()),
  retentionV2Enabled: () => platform.retentionV2,
}));

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const waitUntil = async (done: () => boolean, ms = 3_000) => {
  for (let waited = 0; waited < ms && !done(); waited += 10) await sleep(10);
  return done();
};
const roots: string[] = [];
const hosts: ResidentHost[] = [];
let safety: SafetyTickCapture;
beforeEach(() => { safety = captureSafetyTick(); });
afterEach(async () => {
  vi.restoreAllMocks();
  safety.restore();
  platform.retentionV2 = process.platform !== "win32";
  for (const host of hosts.splice(0)) await host.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = (agents: Partial<ResidentHostConfig["agents"]> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-request-watch-"));
  roots.push(root);
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:request-watch", sessionId: "request-watch",
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:request-watch"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, ...agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, residentIdleExitMs: 0 },
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  const file = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(file, JSON.stringify(config));
  const old = new Date(Date.now() - 3_600_000);
  fs.utimesSync(file, old, old);
  const host = new ResidentHost(config);
  hosts.push(host);
  const dir = (name: string) => path.join(config.residencyRoot, name);
  // An invalid command still gets a durable (ok:false) response: enough to observe admission.
  const request = (id: string) => {
    fs.writeFileSync(path.join(dir("requests"), `${id}.json.tmp`), JSON.stringify({ format: -1, requestId: id }));
    fs.renameSync(path.join(dir("requests"), `${id}.json.tmp`), path.join(dir("requests"), `${id}.json`));
  };
  const responded = (ids: string[]) => ids.every(id => fs.existsSync(path.join(dir("responses"), `${id}.json`)));
  return { config, host, dir, request, responded };
};

const intervals = () => {
  const spy = vi.spyOn(globalThis, "setInterval");
  return { spy, periods: () => spy.mock.calls.map(call => call[1]) };
};

describe("resident request watch (smarty-dev#11107)", () => {
  it("drains on a watch event with no 50 ms interval; the only periodic timers are slow", async () => {
    const timers = intervals();
    const { host, request, responded } = fixture();
    await host.start();
    const started = performance.now();
    request("watch-one");
    expect(await waitUntil(() => responded(["watch-one"]))).toBe(true);
    expect(performance.now() - started).toBeLessThan(2_000); // far below the 5 s safety tick
    // Host-owned intervals: the 5 s safety tick and the stuck-preparing check. Other components'
    // own cadences are not this host's; none of them is the removed 50/100 ms request/maintenance tick.
    expect(timers.periods()).toContain(5_000);
    expect(timers.periods()).toContain(10_000);
    expect(timers.periods()).not.toContain(50);
    expect(timers.periods()).not.toContain(100);
  });

  it("drains the startup backlog explicitly and a burst larger than one 32-entry batch", async () => {
    const { host, request, responded } = fixture();
    fs.mkdirSync(path.join(host.config.residencyRoot, "requests"), { recursive: true });
    const backlog = Array.from({ length: 5 }, (_, index) => `backlog-${index}`);
    for (const id of backlog) request(id);
    await host.start();
    expect(await waitUntil(() => responded(backlog))).toBe(true);
    const burst = Array.from({ length: 80 }, (_, index) => `burst-${index}`);
    for (const id of burst) request(id);
    // Three bounded batches, back to back: not one batch per safety tick.
    expect(await waitUntil(() => responded(burst), 4_000)).toBe(true);
  });

  it("does not hot-spin on a >32 backlog whose first batch cannot be claimed; retries on a real event", async () => {
    const { host, dir, request, responded } = fixture();
    await host.start();
    const rename = fs.renameSync;
    let stuck = true;
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      const name = path.basename(String(source));
      if (stuck && name.startsWith("stuck-") && name.endsWith(".json") && path.dirname(String(source)) === dir("requests")) {
        throw Object.assign(new Error("rename refused"), { code: "EBUSY" });
      }
      return rename(source, target);
    });
    const ids = Array.from({ length: 40 }, (_, index) => `stuck-${String(index).padStart(2, "0")}`);
    for (const id of ids) request(id);
    await sleep(300); // events coalesce; the no-progress batch ends the drain
    const readdir = vi.spyOn(fs, "readdirSync");
    const scans = () => readdir.mock.calls.filter(([target]) => String(target) === dir("requests")).length;
    await sleep(500);
    // Before: entries.length > 32 re-queued a setImmediate rescan forever (thousands per 500 ms).
    expect(scans()).toBeLessThanOrEqual(2);
    // The stalled queue is retried on the next real event once the cause clears.
    stuck = false;
    request("unstick");
    expect(await waitUntil(() => responded([...ids, "unstick"]), 4_000)).toBe(true);
  });

  it("does not lose a request that arrives while a drain is in progress", async () => {
    const { host, request, responded } = fixture();
    await host.start();
    const readdir = fs.readdirSync;
    let injected = false;
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, options?: unknown) => {
      const result = (readdir as (target: fs.PathLike, options?: unknown) => unknown)(target, options);
      // Arrives after this pass listed requests/: only the dirty bit can pick it up.
      if (!injected && String(target).endsWith(`${path.sep}requests`) && (result as string[]).includes("first.json")) {
        injected = true;
        request("racing");
      }
      return result;
    }) as typeof fs.readdirSync);
    request("first");
    expect(await waitUntil(() => responded(["first", "racing"]))).toBe(true);
    expect(injected).toBe(true);
  });

  it("re-attempts a request drain deferred by MeshBackgroundRetry after its backoff, not the safety tick", async () => {
    const { host, request, responded } = fixture();
    await host.start();
    const run = MeshBackgroundRetry.prototype.run;
    let deferred = 0;
    vi.spyOn(MeshBackgroundRetry.prototype, "run").mockImplementation(function (this: MeshBackgroundRetry, operation, reset) {
      if (this.label === "resident request poll" && deferred === 0) {
        deferred++;
        Object.defineProperty(this, "waitMs", { configurable: true, get: () => 50 });
        setTimeout(() => { delete (this as { waitMs?: number }).waitMs; }, 50);
        return Promise.resolve("retry" as const);
      }
      return run.call(this, operation, reset);
    });
    const started = performance.now();
    request("deferred");
    expect(await waitUntil(() => responded(["deferred"]))).toBe(true);
    expect(deferred).toBe(1);
    expect(performance.now() - started).toBeLessThan(2_000);
  });

  it("drains a delivery outbox larger than one 32-item flush from a single wake", async () => {
    const { host, dir, config } = fixture();
    await host.start();
    fs.mkdirSync(dir("delivery-outbox"), { recursive: true });
    for (let index = 0; index < 70; index++) {
      const id = `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`;
      fs.writeFileSync(path.join(dir("delivery-outbox"), `${id}.json`), JSON.stringify({ format: RESIDENT_HOST_FORMAT, id,
        rootId: config.rootId, from: { id: "agent-x", name: "x", kind: "agent" }, delivery: "followUp", triggerTurn: false,
        message: `m${index}`, createdAt: Date.now() }));
    }
    const pending = () => fs.readdirSync(dir("delivery-outbox")).filter(entry => entry.endsWith(".json")).length;
    await safety.tick(); // one wake; later batches continue on their own, not per 5 s tick
    expect(await waitUntil(() => pending() === 0, 4_000)).toBe(true);
  });

  it("falls back to the 50 ms poll, with one warning, when fs.watch throws for requests/", async () => {
    const watch = fs.watch;
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target).endsWith(`${path.sep}requests`)) throw Object.assign(new Error("watch unavailable"), { code: "ENOSPC" });
      return (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
    }) as typeof fs.watch);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const timers = intervals();
    const { host, request, responded } = fixture();
    await host.start();
    expect(timers.periods()).toContain(50);
    request("fallback");
    expect(await waitUntil(() => responded(["fallback"]))).toBe(true);
    expect(warn.mock.calls.filter(([line]) => String(line).includes("resident request watch unavailable"))).toHaveLength(1);
    // The 20 Hz fallback nudges requests only: no delivery-outbox flush rides on it.
    // An outbox item written behind the host's back (no enqueue/write event) stays until the safety tick.
    const outbox = path.join(host.config.residencyRoot, "delivery-outbox");
    fs.mkdirSync(outbox, { recursive: true });
    const id = "00000000-0000-4000-8000-000000000001";
    fs.writeFileSync(path.join(outbox, `${id}.json`), JSON.stringify({ format: RESIDENT_HOST_FORMAT, id,
      rootId: host.config.rootId, from: { id: "agent-x", name: "x", kind: "agent" }, delivery: "followUp", triggerTurn: false,
      message: "m", createdAt: Date.now() }));
    await sleep(300); // ~6 fallback ticks
    expect(fs.existsSync(path.join(outbox, `${id}.json`))).toBe(true);
    await safety.tick();
    expect(await waitUntil(() => !fs.existsSync(path.join(outbox, `${id}.json`)))).toBe(true);
  });

  it("falls back once on a watcher error event and close joins the native watcher close", async () => {
    const watch = fs.watch;
    let watcher: fs.FSWatcher | undefined;
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const created = (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
      if (String(target).endsWith(`${path.sep}requests`)) watcher = created;
      return created;
    }) as typeof fs.watch);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { host, request, responded } = fixture();
    await host.start();
    expect(watcher).toBeDefined();
    watcher!.emit("error", new Error("watcher died"));
    watcher!.emit("error", new Error("watcher died again"));
    request("after-error");
    expect(await waitUntil(() => responded(["after-error"]))).toBe(true);
    expect(warn.mock.calls.filter(([line]) => String(line).includes("resident request watch unavailable"))).toHaveLength(1);
    await host.close(); // must not hang on an already-closed handle
  });

  it("a native watch error (Node closes the handle, emits only error, never close) cannot hang close()", async () => {
    const watch = fs.watch;
    let watcher: fs.FSWatcher | undefined;
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const created = (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
      if (String(target).endsWith(`${path.sep}requests`)) watcher = created;
      return created;
    }) as typeof fs.watch);
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const { host, request, responded } = fixture();
    await host.start();
    let closeEvents = 0;
    watcher!.on("close", () => { closeEvents++; });
    // Test-only: drive Node's own native failure path (lib/internal/fs/watchers.js onchange, status < 0).
    (watcher as unknown as { _handle: { onchange: (status: number, event: string, file: string) => void } })._handle.onchange(-28, "", "requests");
    request("after-native-error");
    expect(await waitUntil(() => responded(["after-native-error"]))).toBe(true);
    const started = performance.now();
    await host.close();
    expect(performance.now() - started).toBeLessThan(2_500);
    expect(closeEvents).toBe(0);
  });

  it("keeps request-watch close custody when an error precedes a delayed actual close", async () => {
    const watch = fs.watch;
    let actuallyClosed = false;
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (!String(target).endsWith(`${path.sep}requests`)) return (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
      const fake = new EventEmitter() as EventEmitter & { close(): void };
      let closing = false;
      fake.close = () => {
        if (closing) return;
        closing = true;
        setTimeout(() => { actuallyClosed = true; fake.emit("close"); }, 200);
      };
      setTimeout(() => fake.emit("error", new Error("watcher died")), 10);
      return fake as unknown as fs.FSWatcher;
    }) as typeof fs.watch);
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const { host } = fixture();
    await host.start();
    await sleep(50); // the error has been emitted; its actual close is still pending
    expect(actuallyClosed).toBe(false);
    await host.close();
    expect(actuallyClosed).toBe(true);
  });

  it("the 50 ms staged-phase timer is the bounded exception: gone once the staged phase ends", async () => {
    const timers = intervals();
    const cleared = vi.spyOn(globalThis, "clearInterval");
    const { host, config } = fixture();
    const attempt = { id: "plan-11107", kind: "fallback" as const };
    const staged = new ResidentHost(config, () => {}, undefined, {
      launcher: { pid: process.pid, processStartTime: "0", token: "launcher", entry: "entry", runtime: "runtime" } as never,
      spec: { releaseRoot: "/release/A", digest: "digest" } as never, attempt });
    hosts.splice(hosts.indexOf(host), 1, staged);
    vi.spyOn(AgentManager.prototype, "spawn").mockResolvedValue({ id: "probe" } as never);
    vi.spyOn(AgentManager.prototype, "wait").mockResolvedValue({ status: "completed", text: "resident worker startup verified" } as never);
    vi.spyOn(AgentManager.prototype, "cleanup").mockResolvedValue({ cleaned: true });
    await staged.start();
    const index = timers.spy.mock.calls.findIndex(call => call[1] === 50);
    expect(index).toBeGreaterThanOrEqual(0); // active staged phase: the approved 50 ms exception
    const releaseTimer = timers.spy.mock.results[index]!.value;
    expect(cleared.mock.calls.some(([timer]) => timer === releaseTimer)).toBe(false);
    fs.writeFileSync(path.join(config.residencyRoot, "handover.json"), JSON.stringify({ plan: { id: attempt.id }, phase: "fallback" }));
    expect(await waitUntil(() => cleared.mock.calls.some(([timer]) => timer === releaseTimer))).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")).attempt).toBeUndefined();
    // No further 50 ms interval is armed after the phase: requests are back on watch + safety tick.
    expect(timers.periods().filter(period => period === 50)).toHaveLength(1);
  });

  it("joins the request watcher's native close before close() returns", async () => {
    const watch = fs.watch;
    let closed = false;
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const created = (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
      if (String(target).endsWith(`${path.sep}requests`)) created.once("close", () => { closed = true; });
      return created;
    }) as typeof fs.watch);
    const { host } = fixture();
    await host.start();
    await host.close();
    expect(closed).toBe(true);
  });

  it.each([true, false])("an idle safety tick that is not due does no reference scan, config stat or actor listing (retention v2: %s)", async (retentionV2) => {
    platform.retentionV2 = retentionV2;
    const { host, config } = fixture();
    await host.start();
    await safety.tick();
    const due = vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(false);
    const sweep = vi.spyOn(ResidentRequestRetention.prototype, "sweep");
    const references = vi.spyOn(host.agents, "retentionReferences");
    const listOwned = vi.spyOn(host.actors, "listOwned");
    const stat = vi.spyOn(fs, "statSync");
    const configStats = () => stat.mock.calls.filter(([target]) => String(target) === path.join(config.residencyRoot, "config.json")).length;
    // Retention-changing events (agent settlement, actor removal) wake maintenance; not due, it is a no-op.
    for (let index = 0; index < 5; index++) await safety.tick();
    expect(due).toHaveBeenCalled();
    expect(references).not.toHaveBeenCalled();
    expect(sweep).not.toHaveBeenCalled();
    // Maintenance lists owned actors without arguments; presence publication's listOwned(true) is not retention.
    expect(listOwned.mock.calls.filter(call => call.length === 0 || call[0] === undefined)).toHaveLength(0);
    // The slow tick refreshes the accepted overlay (one cached stat each), never the maintenance path.
    expect(configStats()).toBeLessThanOrEqual(5);
    due.mockReturnValue(true);
    await safety.tick();
    expect(references).toHaveBeenCalled();
    expect(sweep).toHaveBeenCalled();
  });

  it("reports stuck preparation on its own threshold timer, not the retention tick", async () => {
    const timers = intervals();
    const { host } = fixture({ stuckPreparingMs: 300 });
    const due = vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(false);
    await host.start();
    const report = vi.spyOn(host.actors, "reportStuckPreparing");
    expect(timers.periods()).toContain(300);
    expect(await waitUntil(() => report.mock.calls.length >= 2, 2_000)).toBe(true);
    expect(report.mock.calls[0]![0]).toBe(300);
    due.mockRestore();
  });
});
