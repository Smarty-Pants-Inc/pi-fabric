import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import * as fileLock from "../src/residency/file-lock.js";
import { describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries, ResidentWakeRecoveryReader,
  residentWakeIndexPath, RESIDENT_WAKE_INDEX_MAX_ROOTS, RESIDENT_WAKE_RETRY_SCAN_MAX_PAGES,
  RESIDENT_WAKE_RETRY_SCAN_PAGE_ENTRIES } from "../src/residency/wake-index.js";
import * as wake from "../src/residency/wake.js";
import { writeHostLease } from "../src/topology/host-leases.js";

const from = { id: "publisher", name: "publisher", kind: "main" as const };
const fixture = (count: number, canWakeResidents?: () => boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-backoff-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100, canWakeResidents ? { canWakeResidents } : {});
  const residents = Array.from({ length: count }, (_, n) => {
    const rootId = `session:backoff-${n}`;
    const residencyRoot = residentRoot(mesh.root, rootId);
    const config = { rootId, residencyRoot, cwd: root };
    fs.mkdirSync(residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(residencyRoot, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(residencyRoot, "wake-routes.json"), JSON.stringify({ format: 1, rootId,
      hostId: `host:${n}`, configJson: canonicalResidentWakeConfig(config),
      actors: [{ id: `actor-${n}`, name: `listener-${n}`, topics: ["stuck.delivery"] }] }));
    return config;
  });
  const close = () => { vi.restoreAllMocks(); mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); };
  return { root, mesh, residents, close };
};
// Backoff/admission checks need a live originating Main: a dead Main's exact
// restart-config mismatch now has a terminal disposition instead of retry debt.
const keepRootMainsLive = (f: ReturnType<typeof fixture>) => {
  for (const config of f.residents) writeHostLease(f.mesh.root, { id: config.rootId, rootId: config.rootId,
    identityId: config.rootId, updatedAt: Date.now(), expiresAt: Date.now() + 86_400_000 });
};
const failureAt = (root: string) => wake.readWakeJson<{
  delivery: { id: string; sequence: number }; error: string; failedAt: number; nextRetryAt: number;
}>(path.join(root, "wake-failure.json"))!;
const maintenance = (mesh: MeshStore, launch: (file: string, config: ResidentHostConfig) => Promise<void>, now?: number) => {
  const retry = (wake as typeof wake & { retryResidentWakes:
    (mesh: MeshStore, launch?: (file: string, config: ResidentHostConfig) => Promise<void>, now?: number) => Promise<void> }).retryResidentWakes;
  expect(retry, "the existing retention sweep needs a retry entry point").toBeTypeOf("function");
  return retry(mesh, launch, now);
};

describe("publication wake isolation and maintenance backoff", () => {
  it.each(["retired", "malformed", "unreadable"] as const)("round2 retries beyond a pinned first page and only drops retired routes: %s", async mode => {
    const f = fixture(301, () => false);
    const launch = vi.fn(async (_file: string, _config: ResidentHostConfig) => {});
    const overflow = path.join(f.mesh.root, "wake-overflow");
    const quarantine = path.join(f.root, "acknowledged-overflow");
    fs.mkdirSync(overflow, { recursive: true });
    fs.mkdirSync(quarantine);
    // Journal roots precede overflow; the live root is selected from the REAL
    // second page, independent of filesystem enumeration order.
    fs.writeFileSync(residentWakeIndexPath(f.mesh.root), f.residents.slice(0, RESIDENT_WAKE_INDEX_MAX_ROOTS).map((config, n) =>
      JSON.stringify({ format: 1, op: "pending", root: config.residencyRoot, delivery: { id: `pending-${n}`, sequence: n + 1 } }) + "\n").join(""));
    for (const [n, config] of f.residents.entries()) {
      if (n < RESIDENT_WAKE_INDEX_MAX_ROOTS) continue;
      fs.writeFileSync(path.join(overflow, `${path.basename(config.residencyRoot)}.json`), JSON.stringify({
        format: 1, op: "pending", root: config.residencyRoot, delivery: { id: `pending-${n}`, sequence: n + 1 },
      }));
    }
    const reader = new ResidentWakeRecoveryReader(f.mesh.root);
    reader.next();
    const live = reader.next().pending.keys().next().value!;
    reader.close();
    const hiddenRoutes = new Set<string>();
    for (const config of f.residents) {
      if (config.residencyRoot === live) continue;
      const file = path.join(config.residencyRoot, "wake-routes.json");
      if (mode === "retired") fs.renameSync(file, `${file}.retired`);
      else if (mode === "malformed") fs.writeFileSync(file, "not-json");
      else hiddenRoutes.add(file);
    }
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (hiddenRoutes.has(String(file))) throw Object.assign(new Error("unreadable route"), { code: "EACCES" });
      return (open as (...args: unknown[]) => number)(file, ...args);
    }) as typeof fs.openSync);
    // #4664: prove product ACK calls and durable namespace changes, without
    // running its native unlink. Only this fixture's overflow is quarantined.
    const unlink = fs.unlinkSync;
    const removed: string[] = [];
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
      if (path.dirname(String(file)) !== overflow) return unlink(file);
      removed.push(String(file));
      fs.renameSync(file, path.join(quarantine, path.basename(String(file))));
    });
    try {
      for (let n = 0; n < 8; n++) await maintenance(f.mesh, launch);
      expect(launch).toHaveBeenCalledOnce();
      expect(launch.mock.calls[0]?.[1]).toMatchObject({ residencyRoot: live });
      const pending = residentUnacknowledgedDeliveries(f.mesh.root);
      expect(pending.has(live)).toBe(false);
      expect(pending.size).toBe(mode === "retired" ? 0 : 300);
      expect(removed.length).toBe(mode === "retired" ? 173 : 1);
      if (mode === "retired") expect(fs.readFileSync(residentWakeIndexPath(f.mesh.root), "utf8")).toContain('"op":"ack"');
    } finally { f.close(); }
  });

  it("round3 bounds retry candidates and reads only one overflow root per claim custody", async () => {
    const f = fixture(301, () => false);
    const overflow = path.join(f.mesh.root, "wake-overflow");
    const quarantine = path.join(f.root, "claimed-overflow");
    fs.mkdirSync(overflow, { recursive: true });
    fs.mkdirSync(quarantine);
    for (const [n, config] of f.residents.entries()) {
      fs.writeFileSync(path.join(overflow, `${path.basename(config.residencyRoot)}.json`), JSON.stringify({
        format: 1, op: "pending", root: config.residencyRoot, delivery: { id: `bounded-${n}`, sequence: n + 1 },
      }));
    }
    const heldReads: Array<Set<string>> = [];
    let currentReads: Set<string> | undefined;
    const exclusive = f.mesh.exclusive.bind(f.mesh);
    vi.spyOn(f.mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> =>
      exclusive(() => {
        const reads = new Set<string>();
        heldReads.push(reads);
        currentReads = reads;
        try { return operation(); } finally { currentReads = undefined; }
      }, timeout));
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (path.dirname(String(file)) === overflow) currentReads?.add(String(file));
      return (open as (...args: unknown[]) => number)(file, ...args);
    }) as typeof fs.openSync);
    // Record the real ACK namespace change without executing native deletion (#4664).
    const unlink = fs.unlinkSync;
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
      if (path.dirname(String(file)) === overflow) fs.renameSync(file, path.join(quarantine, path.basename(String(file))));
      else unlink(file);
    });
    const launch = vi.fn(async (_file: string, _config: ResidentHostConfig) => {});
    try {
      await maintenance(f.mesh, launch);
      expect(launch).toHaveBeenCalledTimes(wake.RESIDENT_WAKE_RETRY_MAX_CANDIDATES ?? RESIDENT_WAKE_INDEX_MAX_ROOTS);
      // Discovery consumes at most K pages. Every subsequent claim/ACK custody
      // validates one keyed overflow record, never all pending pages.
      expect(heldReads[0]!.size).toBeLessThanOrEqual((RESIDENT_WAKE_RETRY_SCAN_MAX_PAGES ?? 1) *
        (RESIDENT_WAKE_RETRY_SCAN_PAGE_ENTRIES ?? RESIDENT_WAKE_INDEX_MAX_ROOTS));
      expect(Math.max(...heldReads.slice(1).map(reads => reads.size))).toBeLessThanOrEqual(1);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(301 - RESIDENT_WAKE_INDEX_MAX_ROOTS);
    } finally { f.close(); }
  });

  it("round3 page cursor reaches all 5000 pinned roots with bounded custody work", async () => {
    const f = fixture(5_000, () => false);
    const overflow = path.join(f.mesh.root, "wake-overflow");
    fs.mkdirSync(overflow, { recursive: true });
    const now = Date.now();
    for (const [n, config] of f.residents.entries()) {
      const delivery = { id: `fair-${n}`, sequence: n + 1 };
      fs.writeFileSync(path.join(overflow, `${path.basename(config.residencyRoot)}.json`), JSON.stringify({
        format: 1, op: "pending", root: config.residencyRoot, delivery,
      }));
      fs.writeFileSync(path.join(config.residencyRoot, "wake-failure.json"), JSON.stringify({
        format: 1, delivery, nextRetryAt: now + 86_400_000,
      }));
    }
    const visited = new Set<string>();
    const discoveryReads: number[] = [], claimReads: number[] = [], heldMs: number[] = [];
    let transaction: { discovery: boolean; reads: number } | undefined;
    let calls = 0;
    const exclusive = f.mesh.exclusive.bind(f.mesh);
    vi.spyOn(f.mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
      const discovery = calls++ === 0;
      return exclusive(() => {
        transaction = { discovery, reads: 0 };
        const start = performance.now();
        try { return operation(); }
        finally {
          heldMs.push(performance.now() - start);
          (discovery ? discoveryReads : claimReads).push(transaction.reads);
          transaction = undefined;
        }
      }, timeout);
    });
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (transaction && path.dirname(String(file)) === overflow) {
        transaction.reads++;
        const limit = transaction.discovery ? (RESIDENT_WAKE_RETRY_SCAN_MAX_PAGES ?? 1) *
          (RESIDENT_WAKE_RETRY_SCAN_PAGE_ENTRIES ?? RESIDENT_WAKE_INDEX_MAX_ROOTS) : 1;
        if (transaction.reads > limit) throw new Error(`retry custody exceeded ${limit} overflow files (observed ${transaction.reads})`);
        if (!transaction.discovery) visited.add(path.basename(String(file)));
      }
      return (open as (...args: unknown[]) => number)(file, ...args);
    }) as typeof fs.openSync);
    const launch = vi.fn(async (_file: string, _config: ResidentHostConfig) => {});
    try {
      const sweeps = Math.ceil(f.residents.length / (wake.RESIDENT_WAKE_RETRY_MAX_CANDIDATES ?? RESIDENT_WAKE_INDEX_MAX_ROOTS));
      for (let sweep = 0; sweep < sweeps; sweep++) {
        calls = 0;
        await maintenance(f.mesh, launch, now);
      }
      expect(visited.size).toBe(5_000); // First pages remain pinned throughout; later pages cannot starve.
      expect(Math.max(...discoveryReads)).toBeLessThanOrEqual((RESIDENT_WAKE_RETRY_SCAN_MAX_PAGES ?? 1) *
        (RESIDENT_WAKE_RETRY_SCAN_PAGE_ENTRIES ?? RESIDENT_WAKE_INDEX_MAX_ROOTS));
      expect(Math.max(...claimReads)).toBeLessThanOrEqual(1);
      expect(launch).not.toHaveBeenCalled(); // Backoff is still authoritative.
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(5_000);
      console.info(JSON.stringify({ probe: "5000-root-custody", sweeps, rootsVisited: visited.size,
        maxDiscoveryFiles: Math.max(...discoveryReads), maxClaimFiles: Math.max(...claimReads),
        totalHeldMs: heldMs.reduce((sum, ms) => sum + ms, 0), maxHeldMs: Math.max(...heldMs) }));
    } finally { f.close(); }
  }, 30_000);

  it.skipIf(process.platform === "win32").each([["cold", false], ["cold", true], ["warm", false], ["warm", true]] as const)(
    "round2 publication rechecks live admission inside intent custody: %s, open=%s", async (state, staysOpen) => {
      let allowed = false;
      const f = fixture(1, () => allowed);
      const config = f.residents[0]!;
      const launch = vi.fn(async () => {});
      const dispatch = wake.wakeResidentActors;
      vi.spyOn(wake, "wakeResidentActors").mockImplementation((mesh, events, _launch, recovery, ...rest) =>
        (dispatch as (...args: unknown[]) => Promise<void>)(mesh, events, launch, recovery, ...rest));
      vi.spyOn(console, "warn").mockImplementation(() => {});
      let release!: () => void;
      try {
        await f.mesh.publish({ topic: "stuck.delivery", from });
        if (state === "warm") fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify({ pid: process.pid, token: "warm" }));
        allowed = true;
        let entered!: () => void;
        const waiting = new Promise<void>(resolve => { entered = resolve; });
        const held = new Promise<void>(resolve => { release = resolve; });
        const lock = fileLock.lockFile;
        vi.spyOn(fileLock, "lockFile").mockImplementation(async (...args) => {
          const fd = await lock(...args);
          if (args[0] === wake.residentWakeIntentLockPath(config.residencyRoot)) { entered(); await held; }
          return fd;
        });
        const publishing = f.mesh.publish({ topic: "stuck.delivery", from });
        await waiting;
        expect(fs.existsSync(wake.residentWakeRequestPath(config.residencyRoot))).toBe(false);
        allowed = staysOpen;
        release();
        await publishing;
        expect(fs.existsSync(wake.residentWakeRequestPath(config.residencyRoot))).toBe(staysOpen);
        expect(launch).toHaveBeenCalledTimes(staysOpen && state === "cold" ? 1 : 0);
        expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(staysOpen ? 0 : 1);
        expect(fs.existsSync(path.join(config.residencyRoot, "wake-failure.json"))).toBe(false);
      } finally { release?.(); f.close(); }
    });

  it.skipIf(process.platform === "win32").each([false, true])("round2 direct helper rechecks live admission during custody, open=%s", async staysOpen => {
    const f = fixture(1);
    const config = f.residents[0]!;
    const saved = { ...config, fabricExtensionPath: path.join(f.root, "extension.js") };
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(saved));
    const routesFile = path.join(config.residencyRoot, "wake-routes.json");
    const routes = JSON.parse(fs.readFileSync(routesFile, "utf8"));
    fs.writeFileSync(routesFile, JSON.stringify({ ...routes, configJson: canonicalResidentWakeConfig(saved) }));
    let allowed = true;
    let release!: () => void;
    let entered!: () => void;
    const waiting = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    const exclusive = vi.fn(async <T>(operation: () => T): Promise<T> => f.mesh.exclusive(operation));
    const lock = fileLock.lockFile;
    vi.spyOn(fileLock, "lockFile").mockImplementation(async (...args) => {
      const fd = await lock(...args);
      if (args[0] === wake.residentWakeIntentLockPath(config.residencyRoot)) {
        entered(); await held;
        // A competing owner becomes warm while intent custody is held: the
        // open-gate counterexample needs no native child to cover the request.
        fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify({ pid: process.pid, token: "direct" }));
      }
      return fd;
    });
    try {
      const direct = (wake.wakeDormantActor as (root: string, id: string, admission: { allowed: () => boolean; exclusive: typeof exclusive }) => Promise<boolean>)(
        f.mesh.root, "actor-0", { allowed: () => allowed, exclusive });
      await waiting;
      expect(fs.existsSync(wake.residentWakeRequestPath(config.residencyRoot))).toBe(false);
      allowed = staysOpen;
      release();
      if (staysOpen) expect(await direct).toBe(true);
      else await expect(direct).rejects.toThrow("admission closed");
      expect(fs.existsSync(wake.residentWakeRequestPath(config.residencyRoot))).toBe(staysOpen);
      expect(exclusive).toHaveBeenCalledTimes(staysOpen ? 1 : 0);
    } finally { release?.(); f.close(); }
  });
  it.each([1, 3, 10])("unrelated publication does not await %i ConfigMismatch-style failed roots", async count => {
    const f = fixture(count);
    keepRootMainsLive(f);
    let slow = false;
    const launch = vi.fn(async (_file: string, config: ResidentHostConfig) => {
      // Same IPC outcome as a native launcher refusing its restart config. Slow
      // native startup is intentional: an unrelated ACK must never await it.
      if (slow) await new Promise(resolve => setTimeout(resolve, 100));
      throw new wake.ResidentWakeConfigMismatch(config.residencyRoot);
    });
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((mesh, events, _launch, recovery) => dispatch(mesh, events, launch, recovery));
    vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      // The zero-failed-root control uses the same physical mesh and root count.
      let start = performance.now();
      await f.mesh.publish({ topic: "unrelated.control", from });
      const controlMs = performance.now() - start;
      const delivered = await f.mesh.publish({ topic: "stuck.delivery", from });
      expect(launch).toHaveBeenCalledTimes(count);
      for (const config of f.residents) expect(failureAt(config.residencyRoot)).toMatchObject({
        delivery: { id: delivered.id }, error: expect.stringContaining("ResidentWakeConfigMismatch"),
      });
      slow = true;
      start = performance.now();
      const event = await f.mesh.publish({ topic: "unrelated.ack", from });
      const elapsedMs = performance.now() - start;
      console.info(JSON.stringify({ count, controlMs, elapsedMs }));
      expect(elapsedMs).toBeLessThan(controlMs + 75);
      expect(launch).toHaveBeenCalledTimes(count);
      expect(f.mesh.read().at(-1)?.id).toBe(event.id);
    } finally { f.close(); }
  });

  it("a nonmatching publication does not wait for pending-index custody", async () => {
    let allowed = false;
    const f = fixture(2, () => allowed);
    let publishing: ReturnType<MeshStore["publish"]> | undefined;
    let deadline: ReturnType<typeof setTimeout> | undefined;
    let release!: () => void;
    let markExclusiveStarted!: () => void;
    const exclusiveStarted = new Promise<void>(resolve => { markExclusiveStarted = resolve; });
    const exclusiveGate = new Promise<void>(resolve => { release = resolve; });
    const exclusive = f.mesh.exclusive.bind(f.mesh);
    vi.spyOn(f.mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
      markExclusiveStarted();
      await exclusiveGate;
      return exclusive(operation, timeout);
    });
    try {
      // Seed real pending work without launching; only the post-append
      // dispatcher exclusive is gated, never custody needed by the append.
      const matching = await f.mesh.publish({ topic: "stuck.delivery", from });
      const pendingBefore = residentUnacknowledgedDeliveries(f.mesh.root);
      expect(pendingBefore.size).toBe(2);
      for (const delivery of pendingBefore.values()) expect(delivery).toMatchObject({ id: matching.id, sequence: matching.sequence });
      const indexBefore = fs.readFileSync(residentWakeIndexPath(f.mesh.root), "utf8");
      allowed = true;
      publishing = f.mesh.publish({ topic: "unrelated.control", from });
      const outcome = await Promise.race([
        publishing.then(() => "published" as const),
        exclusiveStarted.then(() => "blocked-on-exclusive" as const),
        new Promise<"timed-out">(resolve => { deadline = setTimeout(() => resolve("timed-out"), 1_000); }),
      ]);
      expect(f.mesh.read().at(-1)).toMatchObject({ topic: "unrelated.control", sequence: matching.sequence + 1 });
      expect(outcome).toBe("published");
      const unrelated = await publishing;
      expect(f.mesh.read().at(-1)?.id).toBe(unrelated.id);
      expect(fs.readFileSync(residentWakeIndexPath(f.mesh.root), "utf8")).toBe(indexBefore);
      expect(residentUnacknowledgedDeliveries(f.mesh.root)).toEqual(pendingBefore);
      release();
      await maintenance(f.mesh, async () => {});
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      for (const config of f.residents) expect(wake.readWakeJson<{ id: string }>(
        wake.residentWakeRequestPath(config.residencyRoot))?.id).toBe(matching.id);
    } finally {
      if (deadline) clearTimeout(deadline);
      release?.();
      try { await publishing; } finally { f.close(); }
    }
  });

  it("a malformed retained actor route does not suppress another matching resident", async () => {
    const f = fixture(2, () => false);
    vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const event = await f.mesh.publish({ topic: "stuck.delivery", from });
      // Metadata can change after the real append, before its dispatcher runs.
      const bad = f.residents[0]!.residencyRoot, good = f.residents[1]!.residencyRoot;
      const file = path.join(bad, "wake-routes.json");
      fs.writeFileSync(file, JSON.stringify({ ...JSON.parse(fs.readFileSync(file, "utf8")), actors: [null] }));
      await wake.wakeResidentActors(f.mesh, [event], async () => {});
      expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(good))?.id).toBe(event.id);
      const pending = residentUnacknowledgedDeliveries(f.mesh.root);
      expect(pending.get(bad)?.id).toBe(event.id);
      expect(pending.has(good)).toBe(false);
    } finally { f.close(); }
  });

  it("a matching publication still waits for its own wake, even during retry backoff", async () => {
    const f = fixture(1);
    keepRootMainsLive(f);
    let finish!: () => void;
    let started!: () => void;
    const entered = new Promise<void>(resolve => { started = resolve; });
    const gate = new Promise<void>(resolve => { finish = resolve; });
    const launch = vi.fn(async (_file: string, config: ResidentHostConfig) => {
      if (launch.mock.calls.length === 1) throw new wake.ResidentWakeConfigMismatch(config.residencyRoot);
      started();
      await gate;
    });
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((mesh, events, _launch, recovery) => dispatch(mesh, events, launch, recovery));
    vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await f.mesh.publish({ topic: "stuck.delivery", from });
      let returned = false;
      const publishing = f.mesh.publish({ topic: "stuck.delivery", from }).then(event => { returned = true; return event; });
      await entered;
      expect(returned).toBe(false);
      expect(launch).toHaveBeenCalledTimes(2);
      finish();
      const event = await publishing;
      expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(f.residents[0]!.residencyRoot))?.id).toBe(event.id);
      expect(fs.existsSync(path.join(f.residents[0]!.residencyRoot, "wake-failure.json"))).toBe(false);
    } finally { finish?.(); f.close(); }
  });

  it("the existing ActorManager retention interval retries without bypassing the store wake gate", async () => {
    let allowed = true;
    const f = fixture(1, () => allowed);
    keepRootMainsLive(f);
    const config = f.residents[0]!;
    const configFile = path.join(config.residencyRoot, "config.json");
    const saved = fs.readFileSync(configFile, "utf8");
    const realNow = Date.now.bind(Date);
    let skew = 0;
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    const launch = vi.fn(async () => {
      fs.writeFileSync(wake.residentSleepingPath(config.residencyRoot), JSON.stringify({
        request: wake.readWakeJson(wake.residentWakeRequestPath(config.residencyRoot)),
      }));
    });
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((mesh, events, _launch, recovery) => dispatch(mesh, events, launch, recovery));
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(f.root, "runs"),
    });
    let actors: ActorManager | undefined;
    try {
      fs.writeFileSync(configFile, JSON.stringify({ ...config, cwd: "unapproved-restart" }));
      await f.mesh.publish({ topic: "stuck.delivery", from });
      const retryAt = failureAt(config.residencyRoot).nextRetryAt;
      fs.writeFileSync(configFile, saved);
      allowed = false;
      skew = retryAt - realNow() + 1;
      const retry = (wake as typeof wake & { retryResidentWakes:
        (mesh: MeshStore, launch?: (file: string, config: ResidentHostConfig) => Promise<void>, now?: number) => Promise<void> }).retryResidentWakes;
      expect(retry).toBeTypeOf("function");
      const retries = vi.spyOn(wake as typeof wake & { retryResidentWakes: typeof retry }, "retryResidentWakes")
        .mockImplementation((mesh, _launch, now) => retry(mesh, launch, now));
      const identity = { id: "session:maintenance", name: "main", kind: "main" as const, sessionId: "maintenance" };
      actors = new ActorManager("maintenance", identity, f.mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
        actorRoot: path.join(f.root, "actors"), persistent: true, retentionSweepMs: 20, reapDeadSessionPresence: false,
      });
      await new Promise(resolve => setTimeout(resolve, 80));
      expect(retries).not.toHaveBeenCalled();
      expect(launch).not.toHaveBeenCalled();
      allowed = true;
      await vi.waitFor(() => expect(launch).toHaveBeenCalledOnce(), { timeout: 5_000, interval: 10 });
      await vi.waitFor(() => {
        expect(fs.existsSync(path.join(config.residencyRoot, "wake-failure.json"))).toBe(false);
        expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      }, { timeout: 5_000, interval: 10 });
    } finally { await actors?.close(); await agents.close(); f.close(); }
  });

  it.each([[1, false], [1, true], [2, false], [2, true], [3, false], [3, true],
    [4, false], [4, true], [5, false], [5, true]] as const)(
    "rechecks maintenance admission after custody wait %i (gate stays open: %s)", async (heldCall, staysOpen) => {
    let allowed = true;
    const f = fixture(1, () => allowed);
    keepRootMainsLive(f);
    const config = f.residents[0]!;
    const configFile = path.join(config.residencyRoot, "config.json");
    const saved = fs.readFileSync(configFile, "utf8");
    const launch = vi.fn(async () => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    let release!: () => void;
    try {
      fs.writeFileSync(configFile, JSON.stringify({ ...config, cwd: "unapproved-restart" }));
      await f.mesh.publish({ topic: "stuck.delivery", from });
      const before = fs.readFileSync(path.join(config.residencyRoot, "wake-failure.json"), "utf8");
      vi.spyOn(Date, "now").mockReturnValue(failureAt(config.residencyRoot).nextRetryAt);
      fs.writeFileSync(configFile, saved);
      const retry = wake.retryResidentWakes as (mesh: MeshStore, launch?: (file: string, config: ResidentHostConfig) => Promise<void>,
        now?: number, canWake?: () => boolean) => Promise<void>;
      vi.spyOn(wake, "retryResidentWakes").mockImplementation((mesh, _launch, now, canWake = () => true) =>
        retry(mesh as MeshStore, launch, now, canWake));
      const exclusive = f.mesh.exclusive.bind(f.mesh);
      let entered!: () => void;
      const waiting = new Promise<void>(resolve => { entered = resolve; });
      const held = new Promise<void>(resolve => { release = resolve; });
      let calls = 0;
      vi.spyOn(f.mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
        if (++calls === heldCall) { entered(); await held; }
        return exclusive(operation, timeout);
      });
      const sweeping = f.mesh.retryResidentWakes();
      await waiting;
      allowed = staysOpen;
      release();
      await sweeping;
      expect(launch).toHaveBeenCalledTimes(staysOpen ? 1 : 0);
      // Waits 1-3 cover candidate discovery, observation and keyed revalidation;
      // wait 4 gates intent commit, and wait 5 gates its post-intent ACK. Closing
      // before commit must leave no intent; closing at ACK must still suppress launch.
      expect(fs.existsSync(wake.residentWakeRequestPath(config.residencyRoot))).toBe(staysOpen || heldCall === 5);
      if (!staysOpen) {
        expect(fs.readFileSync(path.join(config.residencyRoot, "wake-failure.json"), "utf8")).toBe(before);
        allowed = true;
        await f.mesh.retryResidentWakes();
        expect(launch).toHaveBeenCalledOnce(); // Revocation retained the durable nudge and released our claim.
      }
    } finally { release?.(); f.close(); }
  });

  it("retains real config refusals, backs off repeated sweeps, and recovers repaired config at the next due retry", async () => {
    const f = fixture(1);
    keepRootMainsLive(f);
    const config = f.residents[0]!;
    const configFile = path.join(config.residencyRoot, "config.json");
    const saved = fs.readFileSync(configFile, "utf8");
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const launch = vi.fn(async () => {
      fs.writeFileSync(wake.residentSleepingPath(config.residencyRoot), JSON.stringify({
        request: wake.readWakeJson(wake.residentWakeRequestPath(config.residencyRoot)),
      }));
    });
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((mesh, events, _launch, recovery) => dispatch(mesh, events, launch, recovery));
    vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      fs.writeFileSync(configFile, JSON.stringify({ ...config, cwd: "unapproved-restart" }));
      const event = await f.mesh.publish({ topic: "stuck.delivery", from });
      const first = failureAt(config.residencyRoot);
      expect(first.error).toContain("ResidentWakeConfigMismatch");
      expect(first.nextRetryAt).toBeGreaterThan(now);
      expect(launch).not.toHaveBeenCalled();
      for (let n = 0; n < 4; n++) await maintenance(f.mesh, launch, now);
      expect(failureAt(config.residencyRoot)).toEqual(first);
      now = first.nextRetryAt;
      await Promise.all([maintenance(f.mesh, launch, now), maintenance(f.mesh, launch, now)]);
      const second = failureAt(config.residencyRoot);
      expect(second.failedAt).toBe(now);
      expect(second.nextRetryAt - now).toBeGreaterThan(first.nextRetryAt - first.failedAt);
      let latest = second;
      for (let n = 0; n < 8; n++) {
        now = latest.nextRetryAt;
        await maintenance(f.mesh, launch, now);
        const next = failureAt(config.residencyRoot);
        expect(next.nextRetryAt - now).toBe(Math.min(60_000, 2 * (latest.nextRetryAt - latest.failedAt)));
        latest = next;
      }
      expect(latest.nextRetryAt - latest.failedAt).toBe(60_000);
      fs.writeFileSync(configFile, saved);
      await maintenance(f.mesh, launch, now);
      expect(launch).not.toHaveBeenCalled();
      now = latest.nextRetryAt;
      await Promise.all([maintenance(f.mesh, launch, now), maintenance(f.mesh, launch, now)]);
      expect(launch).toHaveBeenCalledOnce();
      expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(config.residencyRoot))?.id).toBe(event.id);
      expect(fs.existsSync(path.join(config.residencyRoot, "wake-failure.json"))).toBe(false);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      await maintenance(f.mesh, launch, now);
      expect(launch).toHaveBeenCalledOnce();
    } finally { f.close(); }
  });
});
