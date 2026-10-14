// smarty-dev#11172 Decision C. Reason: incident #11172 (idle resident wakes) and its failure paths.
// Named E2E: idle-wakes main/resident, owner fv2-11172wake/event-lead; PASS is <=1 wake/s and no call
// site above 1/min. These unit probes pin the host's event-driven request queue and deadline maintenance.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import {
  RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, type ResidentDeliveryRecord, type ResidentHostConfig,
} from "../src/residency/protocol.js";
import * as fileLock from "../src/residency/file-lock.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { MeshStore } from "../src/mesh/store.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

beforeEach(() => installInProcessResidentFence());

const roots: string[] = [];
const hosts: ResidentHost[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const host of hosts.splice(0)) await host.close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = (stuckPreparingMs?: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-request-wakes-"));
  roots.push(root);
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:wakes", sessionId: "wakes",
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, ...(stuckPreparingMs ? { stuckPreparingMs } : {}) }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  const configPath = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  const old = new Date(Date.now() - 3_600_000);
  fs.utimesSync(configPath, old, old); // settled: its stamp is cached
  const idle = vi.fn();
  const host = new ResidentHost(config, idle);
  hosts.push(host);
  const requests = path.join(config.residencyRoot, "requests");
  const responses = path.join(config.residencyRoot, "responses");
  return { root, config, configPath, host, idle, requests, responses };
};

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const waitUntil = async (done: () => boolean, ms = 5_000) => {
  const until = performance.now() + ms;
  while (performance.now() < until && !done()) await sleep(5);
  return done();
};
/** Client shape: temp file + rename into requests/ (an unsupported op still gets a durable response). */
const submit = (requests: string, id: string) => {
  const temp = path.join(requests, `.${id}.tmp`);
  fs.writeFileSync(temp, JSON.stringify({ format: RESIDENT_HOST_FORMAT, requestId: id, operation: "probe-unsupported" }));
  fs.renameSync(temp, path.join(requests, `${id}.json`));
};
const pending = (requests: string) => fs.readdirSync(requests).filter(entry => entry.endsWith(".json")).length;
const answered = (responses: string, ids: string[]) => ids.every(id => fs.existsSync(path.join(responses, `${id}.json`)));
const ids = (prefix: string, count: number) => Array.from({ length: count }, (_, index) => `${prefix}-${index}`);
const watchesOf = (watch: { mock: { calls: unknown[][] } }, target: string) =>
  watch.mock.calls.filter(([watched]) => String(watched) === target).length;
const stackHas = (frame: string) => (new Error().stack ?? "").includes(frame);
/** Settles to "hung" instead of hanging the suite. */
const within = <T>(work: Promise<T>, ms: number) =>
  Promise.race([work.then(value => ({ value }), (error: unknown) => ({ error })), sleep(ms).then(() => "hung" as const)]);

describe("event-driven resident request queue (smarty-dev#11172)", () => {
  it("picks a request up from a real requests/ watch within 50 ms, with no poll timer", async () => {
    const { host, requests, responses } = fixture();
    await host.start();
    await sleep(200); // startup drains and the first maintenance pass settle
    const intervals = vi.spyOn(globalThis, "setInterval");
    const timeouts = vi.spyOn(globalThis, "setTimeout");
    const latencies: number[] = [];
    for (const id of ids("pickup", 5)) {
      const started = performance.now();
      submit(requests, id);
      expect(await waitUntil(() => !fs.existsSync(path.join(requests, `${id}.json`)), 2_000)).toBe(true);
      latencies.push(performance.now() - started);
      expect(await waitUntil(() => answered(responses, [id]))).toBe(true);
    }
    latencies.sort((a, b) => a - b);
    // Counterexample: the base host found a request only on its next 50 ms interval tick.
    expect(latencies[2]).toBeLessThan(50);
    expect(intervals.mock.calls.filter(([, ms]) => ms === 50)).toHaveLength(0);
    expect(timeouts.mock.calls.filter(([, ms]) => ms === 50)).toHaveLength(0);
  });

  it("drains a startup backlog and a concurrent burst larger than one 32-entry pass", async () => {
    const { host, requests, responses } = fixture();
    fs.mkdirSync(requests, { recursive: true });
    const backlog = ids("backlog", 40);
    for (const id of backlog) submit(requests, id);
    await host.start();
    expect(await waitUntil(() => answered(responses, backlog), 10_000)).toBe(true);
    const burst = ids("burst", 70);
    for (const id of burst) submit(requests, id); // one synchronous burst: coalesced watch events
    expect(await waitUntil(() => answered(responses, burst), 10_000)).toBe(true);
    expect(pending(requests)).toBe(0);
  });

  it("backs off when every rename of a >32 backlog fails, then drains once renames succeed", async () => {
    const { host, requests, responses } = fixture();
    await host.start();
    await sleep(200);
    const rename = fs.renameSync;
    let failing = true;
    vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
      // Only the host's admission rename (requests/ -> processing/) fails, e.g. a sharing violation.
      if (failing && path.dirname(String(from)) === requests && path.dirname(String(to)) !== requests) {
        throw Object.assign(new Error("busy"), { code: "EBUSY" });
      }
      return rename(from, to);
    }) as typeof fs.renameSync);
    const readdir = fs.readdirSync;
    let scans = 0;
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === requests) scans++;
      return (readdir as (...args: unknown[]) => unknown)(target, ...rest);
    }) as typeof fs.readdirSync);
    const backlog = ids("stuck", 40);
    for (const id of backlog) submit(requests, id);
    await sleep(200); // the submissions' own watch events each legitimately kick one pass
    scans = 0;
    await sleep(1_500);
    // Counterexample (CODE-1): a pass with no successful rename must not re-run immediately.
    // The capped 50 ms-doubling (to 1 s) backoff still retries, but only a handful of scans in 1.5 s.
    expect(scans).toBeGreaterThan(0);
    expect(scans).toBeLessThan(20);
    expect(pending(requests)).toBe(40);
    failing = false;
    expect(await waitUntil(() => answered(responses, backlog), 10_000)).toBe(true);
    expect(pending(requests)).toBe(0);
  });

  it("fails closed before readiness when the request queue cannot be watched", async () => {
    const { config, host, idle, requests } = fixture();
    const watch = fs.watch;
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === requests) throw Object.assign(new Error("watch unsupported"), { code: "ENOSYS" });
      return (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
    }) as typeof fs.watch);
    const intervals = vi.spyOn(globalThis, "setInterval");
    await expect(host.start()).rejects.toThrow(/request watch unavailable/);
    expect(fs.existsSync(path.join(config.residencyRoot, "maintenance-ready.json"))).toBe(false);
    expect(intervals.mock.calls.filter(([, ms]) => ms === 50)).toHaveLength(0);
    expect(idle).not.toHaveBeenCalled();
  });

  it("re-registers once and rescans on a watcher error; a repeated error is terminal, never a poll", async () => {
    const { host, idle, requests, responses } = fixture();
    const watch = fs.watch;
    const created: fs.FSWatcher[] = [];
    const spy = vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const watcher = (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
      if (String(target) === requests) created.push(watcher);
      return watcher;
    }) as typeof fs.watch);
    const intervals = vi.spyOn(globalThis, "setInterval");
    await host.start();
    expect(watchesOf(spy, requests)).toBe(1);
    // A request lands while the watch is broken: the bounded rescan finds it.
    submit(requests, "during-error");
    created[0]!.emit("error", Object.assign(new Error("overflow"), { code: "ENOSPC" }));
    expect(watchesOf(spy, requests)).toBe(2);
    expect(await waitUntil(() => answered(responses, ["during-error"]))).toBe(true);
    // The re-registered watch is live.
    submit(requests, "after-reregister");
    expect(await waitUntil(() => answered(responses, ["after-reregister"]))).toBe(true);
    expect(idle).not.toHaveBeenCalled();
    created[1]!.emit("error", new Error("again"));
    expect(idle).toHaveBeenCalledWith("error");
    expect(watchesOf(spy, requests)).toBe(2);
    expect(intervals.mock.calls.filter(([, ms]) => ms === 50)).toHaveLength(0);
  });

  it("treats a native self-close as a watch loss (bounded contract), but never its own close", async () => {
    const { host, idle, requests, responses } = fixture();
    const watch = fs.watch;
    const created: fs.FSWatcher[] = [];
    const spy = vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const watcher = (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
      if (String(target) === requests) created.push(watcher);
      return watcher;
    }) as typeof fs.watch);
    await host.start();
    submit(requests, "during-close");
    created[0]!.emit("close"); // the native watch closed itself: not silently stranded
    expect(watchesOf(spy, requests)).toBe(2);
    expect(await waitUntil(() => answered(responses, ["during-close"]))).toBe(true);
    created[1]!.emit("close");
    expect(idle).toHaveBeenCalledWith("error");
    expect(watchesOf(spy, requests)).toBe(2);
  });

  it("re-evaluates the exact due time when another store moves a schedule EARLIER (schedules.json watch)", async () => {
    const { config, host, idle } = fixture();
    const watch = fs.watch;
    const order: string[] = [];
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === config.meshRoot) order.push("mesh-watch");
      return (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
    }) as typeof fs.watch);
    const original = MeshStore.prototype.nextScheduleDueAt;
    const hostDue: Array<number | undefined> = [];
    vi.spyOn(MeshStore.prototype, "nextScheduleDueAt").mockImplementation(function (this: MeshStore) {
      const due = original.call(this);
      if (this === host.mesh) { order.push("sample"); hostDue.push(due); }
      return due;
    });
    await host.start();
    // Registered before the initial idle sample, so no cross-process write can fall between them.
    expect(order[0]).toBe("mesh-watch");
    await sleep(50);
    const seen = { mock: { results: [] as Array<{ value: unknown }> }, mockClear: () => { hostDue.length = 0; } };
    Object.defineProperty(seen.mock, "results", { get: () => hostDue.map(value => ({ value })) });
    const hostTimers: number[] = [];
    const timeout = globalThis.setTimeout;
    vi.spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
      if ((new Error().stack ?? "").includes("ResidentHost.")) hostTimers.push(Number(args[1] ?? 0));
      return timeout(...args);
    }) as typeof setTimeout);
    // A different MeshStore (no in-process hook into this host): only the file event can tell it.
    const other = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const from = { id: "scheduler", name: "scheduler", kind: "main" as const };
    await other.schedule({ topic: "t.due", from, data: {}, dueAt: Date.now() + 60_000, key: "k" });
    expect(await waitUntil(() => seen.mock.results.some(result => typeof result.value === "number"), 2_000)).toBe(true);
    const earlier = Date.now() + 1_500;
    seen.mockClear();
    await other.schedule({ topic: "t.due", from, data: {}, dueAt: earlier, key: "k" });
    expect(await waitUntil(() => seen.mock.results.some(result => result.value === earlier), 2_000)).toBe(true);
    expect(idle).not.toHaveBeenCalled();
    // No periodic deadline check: no sub-second recurring timer was armed for it.
    expect(hostTimers.filter(ms => ms > 0 && ms < 50)).toEqual([]);
  });
});

describe("startup join watch and retry obligations (smarty-dev#11172 audit CODE-4/5/6)", () => {
  it("CODE-4: a join watch that fails both registrations rejects start, publishes no readiness and frees the fence", async () => {
    const { config, host } = fixture();
    const watch = fs.watch;
    let joinAttempts = 0;
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      // Only the ParticipantDirectory join watch; the host's own root/request watches stay real.
      if (String(target) === config.meshRoot && stackHas("armJoinWake")) {
        joinAttempts++;
        throw Object.assign(new Error("inotify watches exhausted"), { code: "ENOSPC" });
      }
      return (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
    }) as typeof fs.watch);
    const outcome = await within(host.start(), 5_000);
    // Counterexample: the frozen host swallowed this rejection and looped on delay(20) forever.
    expect(outcome).not.toBe("hung");
    expect(outcome).toHaveProperty("error");
    expect(joinAttempts).toBe(2);
    expect(fs.existsSync(path.join(config.residencyRoot, "maintenance-ready.json"))).toBe(false);
    // Startup cleanup released host.lock: a fresh zero-wait claim succeeds.
    const lockPath = path.join(config.residencyRoot, "host.lock");
    const fd = process.platform === "win32" ? fs.openSync(lockPath, "wx", 0o600)
      : await fileLock.lockFile(lockPath, 0, process.platform === "linux");
    fs.closeSync(fd);
  });

  it("CODE-4 counterexample: an initial publication failure after the watch registered keeps start pending until a real refresh", async () => {
    const { config, host } = fixture();
    const original = ParticipantDirectory.prototype.refresh;
    let failed = 0;
    let succeeded = 0;
    vi.spyOn(ParticipantDirectory.prototype, "refresh").mockImplementation(async function (this: ParticipantDirectory) {
      if (failed === 0) { failed++; throw Object.assign(new Error("mesh lock busy"), { code: "EBUSY" }); }
      await original.call(this);
      succeeded++;
    });
    let settled = false;
    let successesAtReady = -1;
    const starting = host.start().then(() => { settled = true; successesAtReady = succeeded; });
    expect(await waitUntil(() => failed === 1, 5_000)).toBe(true);
    await sleep(50);
    expect(host.participants.heartbeatArmed).toBe(true); // retryable: the heartbeat stays armed
    if (!settled) {
      expect(fs.existsSync(path.join(config.residencyRoot, "maintenance-ready.json"))).toBe(false);
      await host.participants.refresh(); // the real locked refresh, instead of waiting a 60 s heartbeat
    }
    expect(await within(starting, 5_000)).not.toBe("hung");
    expect(settled).toBe(true);
    expect(successesAtReady).toBeGreaterThanOrEqual(1); // readiness only after a successful refresh
    expect(fs.existsSync(path.join(config.residencyRoot, "maintenance-ready.json"))).toBe(true);
  });

  it("CODE-5: one transient requests/ read failure with no later matching event still delivers via the bounded retry", async () => {
    const { host, requests, responses } = fixture();
    const watch = fs.watch;
    let suppress = false;
    vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const watcher = (watch as (...args: unknown[]) => fs.FSWatcher)(target, ...rest);
      if (String(target) === requests) {
        // After the injected failure, drop further native request notifications: only a retry can drain.
        const emit = watcher.emit.bind(watcher);
        watcher.emit = ((event: string | symbol, ...args: unknown[]) =>
          event === "change" && suppress ? false : emit(event, ...args)) as typeof watcher.emit;
      }
      return watcher;
    }) as typeof fs.watch);
    await host.start();
    await sleep(200);
    const readdir = fs.readdirSync;
    let failOnce = true;
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (failOnce && String(target) === requests && stackHas("pollRequests")) {
        failOnce = false;
        suppress = true;
        throw Object.assign(new Error("transient"), { code: "EIO" });
      }
      return (readdir as (...args: unknown[]) => unknown)(target, ...rest);
    }) as typeof fs.readdirSync);
    submit(requests, "after-read-error");
    expect(await waitUntil(() => !failOnce, 2_000)).toBe(true); // the native event reached a failing scan
    // Counterexample: the frozen host treated the failed scan as a done empty pass and armed nothing.
    expect(await waitUntil(() => answered(responses, ["after-read-error"]), 5_000)).toBe(true);
    expect(pending(requests)).toBe(0);
  });

  it("CODE-5 counterexample: a successful empty queue arms no request retry", async () => {
    const { host, requests, responses } = fixture();
    await host.start();
    await sleep(200);
    const readdir = fs.readdirSync;
    let scans = 0;
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === requests && stackHas("pollRequests")) scans++;
      return (readdir as (...args: unknown[]) => unknown)(target, ...rest);
    }) as typeof fs.readdirSync);
    submit(requests, "then-empty");
    expect(await waitUntil(() => answered(responses, ["then-empty"]))).toBe(true);
    expect(scans).toBeGreaterThan(0);
    await sleep(100);
    scans = 0;
    await sleep(1_500); // longer than the 1 s failure retry
    expect(scans).toBe(0);
    submit(requests, "later-event"); // later real events still kick normally
    expect(await waitUntil(() => answered(responses, ["later-event"]))).toBe(true);
  });

  // #4664: product row retirement and outbox unlink are recorded, never executed here (hosted CI owns them).
  const recordDeletes = (config: ResidentHostConfig) => {
    const retire = vi.spyOn(CommitOutbox.prototype, "retire").mockResolvedValue(undefined);
    const outbox = path.join(config.residencyRoot, "delivery-outbox");
    const rm = fs.rmSync;
    const unlinked: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (path.dirname(String(target)) === outbox) { unlinked.push(path.basename(String(target))); return; }
      return (rm as (...args: unknown[]) => void)(target, ...rest);
    }) as typeof fs.rmSync);
    return { retire, unlinked, outbox };
  };
  const deliveryRecord = (config: ResidentHostConfig, id: string): ResidentDeliveryRecord => ({
    format: RESIDENT_HOST_FORMAT, source: "fabric-host", id, rootId: config.rootId,
    from: { id: "probe", name: "probe", kind: "agent" }, delivery: "followUp", triggerTurn: false,
    message: `seeded ${id}`, createdAt: Date.now(),
  });
  /** A prior host's durable CommitOutbox delivery row, with no local delivery-outbox/*.json. */
  const seedRow = async (config: ResidentHostConfig, id: string) => {
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents,
      { lockProtocol: config.mesh.lockProtocol, stateBackend: config.mesh.stateBackend });
    const hostId = residentHostId(config.rootId);
    const identity = { id: hostId, name: "Fabric resident host", kind: "agent" as const };
    const outbox = new CommitOutbox(mesh, `residency/${hostId}/deliveries`, identity, { delivery: () => undefined });
    const plan = outbox.plan();
    await mesh.writeBatch({ identity, ops: [],
      prepare: () => plan.stage([], [{ kind: "delivery", key: id, payload: deliveryRecord(config, id) }], { durable: true }) });
    expect(mesh.listAll(outbox.prefix, { fresh: true })).toHaveLength(1);
    mesh.closeState();
  };
  const handedOff = (host: ResidentHost, config: ResidentHostConfig, id: string) =>
    host.mesh.listAll(residentDeliveryPrefix(config.rootId), { fresh: true }).some(entry => entry.key.endsWith(`/${id}`));

  it("CODE-6: a transient recover failure for a durable row with no local outbox file retries at its own deadline", async () => {
    const { config, host } = fixture();
    await seedRow(config, "row-failed-once");
    const { retire, unlinked, outbox } = recordDeletes(config);
    const recover = CommitOutbox.prototype.recover;
    let recovers = 0;
    vi.spyOn(CommitOutbox.prototype, "recover").mockImplementation(function (this: CommitOutbox) {
      recovers++;
      if (recovers === 1) return Promise.reject(Object.assign(new Error("mesh lock busy"), { code: "EBUSY" }));
      return recover.call(this);
    });
    await host.start();
    expect(await waitUntil(() => recovers >= 1, 2_000)).toBe(true);
    expect(fs.existsSync(path.join(outbox, "row-failed-once.json"))).toBe(false); // nothing local to retry
    // No request, event or flush is triggered here: only the retry obligation can recover the row.
    // Counterexample: the frozen host armed nothing without a local file; the row waited for an unrelated flush.
    expect(await waitUntil(() => handedOff(host, config, "row-failed-once"), 5_000)).toBe(true);
    expect(recovers).toBeGreaterThanOrEqual(2);
    expect(retire).toHaveBeenCalled(); // intended row retirement (recorded only)
    expect(unlinked).toContain("row-failed-once.json"); // intended outbox unlink (recorded only)
  });

  it("CODE-6 counterexample: a successful startup recover delivers the durable row", async () => {
    const { config, host } = fixture();
    await seedRow(config, "row-ok");
    const { retire, unlinked } = recordDeletes(config);
    await host.start();
    expect(await waitUntil(() => handedOff(host, config, "row-ok"), 5_000)).toBe(true);
    expect(retire).toHaveBeenCalled();
    expect(unlinked).toContain("row-ok.json");
  });

  it("CODE-6 counterexample: a local outbox file whose handoff fails is still retried by the bounded path", async () => {
    const { config, host } = fixture();
    const { unlinked, outbox } = recordDeletes(config);
    fs.mkdirSync(outbox, { recursive: true });
    fs.writeFileSync(path.join(outbox, "local-retry.json"), JSON.stringify(deliveryRecord(config, "local-retry")));
    const put = MeshStore.prototype.put;
    let failures = 0;
    vi.spyOn(MeshStore.prototype, "put").mockImplementation(function (this: MeshStore, input) {
      // Fail the first handoff and its truncated fallback: the flush fails and the local file remains.
      if (input.key.endsWith("/local-retry") && failures < 2) {
        failures++;
        return Promise.reject(Object.assign(new Error("disk busy"), { code: "EBUSY" }));
      }
      return put.call(this, input);
    });
    await host.start();
    expect(await waitUntil(() => failures === 2, 2_000)).toBe(true);
    expect(await waitUntil(() => handedOff(host, config, "local-retry"), 5_000)).toBe(true);
    expect(unlinked).toContain("local-retry.json");
  });

  it("CODE-7: an unreadable delivery-outbox directory during a recovered flush and its retry probe still retries at its own deadline", async () => {
    const { config, host } = fixture();
    const { unlinked, outbox } = recordDeletes(config);
    fs.mkdirSync(outbox, { recursive: true });
    fs.writeFileSync(path.join(outbox, "unreadable-dir.json"), JSON.stringify(deliveryRecord(config, "unreadable-dir")));
    const readdir = fs.readdirSync;
    let failures = 0;
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      // First: the flush's listing after a successful recover (recovered flag now true). Second: the very
      // next outbox listing, whichever path makes it (the failed-flush retry probe, or a later preflight).
      if (String(target) === outbox && (failures === 1 || (failures === 0 && stackHas("flushDeliveries")))) {
        failures++;
        throw Object.assign(new Error("transient"), { code: "EIO" });
      }
      return (readdir as (...args: unknown[]) => unknown)(target, ...rest);
    }) as typeof fs.readdirSync);
    await host.start();
    // No request, event or delivery is triggered here: only the bounded retry obligation can flush the item.
    // Counterexample: the frozen host's retry probe threw before arming; the trailing catch absorbed it.
    expect(await waitUntil(() => failures === 2, 2_000)).toBe(true);
    expect(await waitUntil(() => handedOff(host, config, "unreadable-dir"), 5_000)).toBe(true);
    expect(unlinked).toContain("unreadable-dir.json");
  });
});

describe("deadline maintenance (smarty-dev#11172)", () => {
  const configStats = (stat: { mock: { calls: unknown[][] } }, file: string) =>
    stat.mock.calls.filter(([target]) => String(target) === file).length;

  it("arms nothing on an empty idle host, wakes at a real retention deadline, and cancels a stale one", async () => {
    const { host, configPath, requests, responses } = fixture();
    await host.start();
    await sleep(300); // the first retention sample completes empty
    const stat = vi.spyOn(fs, "statSync");
    await sleep(500);
    // Counterexample: the base host re-statted config.json on every 100 ms maintenance tick.
    expect(configStats(stat, configPath)).toBe(0);
    // A real deadline (the SAMPLE_INTERVAL_MS sample, here near) is honoured once a pass re-asks.
    let dueAt: number | undefined = Date.now() + 250;
    vi.spyOn(ResidentRequestRetention.prototype, "nextDueAt").mockImplementation(() => dueAt);
    vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(false);
    submit(requests, "arm"); // the drain's end re-schedules maintenance
    expect(await waitUntil(() => answered(responses, ["arm"]))).toBe(true);
    stat.mockClear();
    expect(await waitUntil(() => configStats(stat, configPath) > 0, 2_000)).toBe(true);
    // A far deadline, then none: the stale timer is cancelled, not left to fire.
    dueAt = Date.now() + 400;
    submit(requests, "far");
    expect(await waitUntil(() => answered(responses, ["far"]))).toBe(true);
    dueAt = undefined;
    submit(requests, "none");
    expect(await waitUntil(() => answered(responses, ["none"]))).toBe(true);
    await sleep(50);
    stat.mockClear();
    // Only the dead-root idle window's real (minutes-away) deadline remains armed.
    await sleep(700);
    expect(configStats(stat, configPath)).toBe(0);
  });

  it("checks stuck preparation exactly at nextStuckPreparingAt and not without an episode", async () => {
    // A 200 ms threshold makes the existing check rate limit (min(10 s, threshold)) a short floor.
    const { host, requests, responses } = fixture(200);
    await host.start();
    // The real directory forwarder owes nothing on an actor-free host or with the check disabled.
    expect(host.actors.nextStuckPreparingAt(60_000)).toBeUndefined();
    expect(host.actors.nextStuckPreparingAt(0)).toBeUndefined();
    await sleep(300); // startup passes took their (event-driven) check
    const report = vi.spyOn(host.actors, "reportStuckPreparing").mockImplementation(() => undefined as never);
    await sleep(500);
    // Counterexample: the base host re-checked every 10 s whenever an item was in flight, owed or not.
    expect(report).not.toHaveBeenCalled();
    let dueAt: number | undefined = Date.now() + 300;
    const next = vi.spyOn(host.actors, "nextStuckPreparingAt").mockImplementation(() => dueAt);
    report.mockImplementation((() => { dueAt = undefined; }) as never); // notified: nothing owed
    const armedAt = Date.now();
    submit(requests, "stuck");
    expect(await waitUntil(() => answered(responses, ["stuck"]))).toBe(true);
    expect(next).toHaveBeenCalled();
    expect(await waitUntil(() => report.mock.calls.length > 0, 3_000)).toBe(true);
    expect(Date.now() - armedAt).toBeGreaterThanOrEqual(250);
    await sleep(600);
    expect(report).toHaveBeenCalledTimes(1);
  });

  it("applies a replaced config.json at once from the root watch, without a maintenance tick", async () => {
    const { host, config, configPath } = fixture();
    await host.start();
    await sleep(300);
    const reads = vi.spyOn(fs, "readFileSync");
    const configReads = () => reads.mock.calls.flatMap(([read], index) =>
      String(read) === configPath ? [String(reads.mock.results[index]?.value)] : []);
    const replace = (value: unknown) => {
      fs.writeFileSync(configPath + ".tmp", JSON.stringify(value));
      fs.renameSync(configPath + ".tmp", configPath);
    };
    const started = performance.now();
    replace({ ...config, retention: { ...config.retention, completedRequestMs: 222_222 } });
    expect(await waitUntil(() => configReads().some(text => text.includes("222222")), 2_000)).toBe(true);
    expect(performance.now() - started).toBeLessThan(1_000);
    replace({ ...config, retention: { ...config.retention, completedRequestMs: 333_333 } });
    expect(await waitUntil(() => configReads().some(text => text.includes("333333")), 2_000)).toBe(true);
  });
});
