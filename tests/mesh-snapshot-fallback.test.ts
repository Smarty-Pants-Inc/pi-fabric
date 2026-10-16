import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { BridgePresence } from "../src/mesh/bridge.js";
import type { MeshBatchOperation, MeshIdentity, MeshStore as Store } from "../src/mesh/store.js";

// smarty-dev#14050. Keep reason: real transport trust boundary, data safety, and a
// deterministic starvation path that a scheduling-only stress test cannot prove.
const identity: MeshIdentity = { id: "snapshot-writer", name: "writer", kind: "main" };
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const hostKey = (id: string): string => `topology/hosts/${hash(id)}`;
const participantKey = (id: string): string => `topology/participants/${hash(id)}`;
const cleanups: Array<() => void | Promise<void>> = [];
let mesh: typeof import("../src/mesh/store.js");
let bridge: typeof import("../src/mesh/bridge.js");
let locks: typeof import("../src/mesh/mesh-lock.js");

beforeAll(async () => {
  // The fleet setup disables diagnostics. Opt in before this module graph loads:
  // measurements below observe the production acquisition hook, not a test timer.
  vi.stubEnv("PI_FABRIC_LOCK_STATS", "");
  vi.resetModules();
  mesh = await import("../src/mesh/store.js");
  bridge = await import("../src/mesh/bridge.js");
  locks = await import("../src/mesh/mesh-lock.js");
});
afterEach(async () => {
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
afterAll(() => {
  const key = Symbol.for("pi-fabric.mesh.lock-stats");
  (globalThis as Record<symbol, { dispose?(): void } | undefined>)[key]?.dispose?.();
  vi.unstubAllEnvs();
});

const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-snapshot-fallback-"));
  const pin = fs.lstatSync(root, { bigint: true });
  const parent = path.dirname(root), parentPin = fs.lstatSync(parent, { bigint: true });
  cleanups.push(() => {
    const current = fs.lstatSync(root, { bigint: true }), owner = fs.lstatSync(parent, { bigint: true });
    if (current.isDirectory() && !current.isSymbolicLink() && current.dev === pin.dev && current.ino === pin.ino &&
      owner.dev === parentPin.dev && owner.ino === parentPin.ino) {
      // CURRENT Node remover: do not bypass the inherited TMPDIR deletion guard.
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  return root;
};
const pause = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
const bounded = async <T>(promise: Promise<T>, label: string, ms = 5_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label}: exceeded ${ms}ms`)), ms);
    })]);
  } finally { clearTimeout(timer); }
};
const waitUntil = async (check: () => boolean, label: string): Promise<void> => {
  const until = Date.now() + 4_000;
  while (!check()) {
    if (Date.now() >= until) throw new Error(`${label}: no receipt`);
    await pause(10);
  }
};

// A separate Node process owns every competing commit. It inherits NODE_OPTIONS,
// including test-deletion-guard.mjs; using Bun here would bypass that safeguard.
// fs.watch is the request event source; the 5ms interval is a bounded safety net
// for coalesced/missed file events. The 50ms heartbeat gives >=11/s burst headroom.
const WRITER = String.raw`
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createJiti } from "jiti";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore } = await jiti.import("./src/mesh/store.ts");
const { writeHostLease } = await jiti.import("./src/topology/host-leases.ts");
const [root, control] = process.argv.slice(1);
const store = new MeshStore(root, 65536, 100, { lockTimeoutMs: 1000, stateBackend: "file" });
const who = { id: "snapshot-writer", name: "writer", kind: "main" };
let stopped = false, busy = false, count = 0, last = 0, next = Date.now(), times = [];
const request = path.join(control, "request.json"), ack = path.join(control, "ack.json");
const receipt = value => {
  fs.writeFileSync(ack + ".tmp", JSON.stringify(value));
  fs.renameSync(ack + ".tmp", ack);
};
async function tick() {
  if (busy || stopped) return;
  let command;
  try { command = JSON.parse(fs.readFileSync(request, "utf8")); } catch {}
  if (command?.id <= last) command = undefined;
  if (!command && Date.now() < next) return;
  busy = true;
  try {
    if (command?.inspect) {
      last = command.id;
      receipt({ id: last, count, times, pid: process.pid, locked: fs.existsSync(path.join(root, ".lock")) });
      return;
    }
    const ops = [{ kind: "put", key: "heartbeat/independent/writer", value: { count: count + 1, pid: process.pid } }, ...(command?.ops ?? [])];
    await store.writeBatch({ identity: who, ops });
    count++; times.push(Date.now());
    if (command?.lease) writeHostLease(root, command.lease);
    if (command) last = command.id;
    next = Date.now() + 50;
    receipt({ id: last, count, times, pid: process.pid, locked: false });
  } catch (error) { console.error(error); stopped = true; process.exitCode = 1; }
  finally { busy = false; }
}
const watch = fs.watch(control, () => void tick());
const interval = setInterval(() => void tick(), 5);
let finish;
const done = new Promise(resolve => { finish = resolve; });
const stop = () => { stopped = true; clearInterval(interval); watch.close(); finish(); };
process.stdin.resume(); process.stdin.once("end", stop);
const deadline = setTimeout(stop, 20000);
console.log("ready");
await done;
clearTimeout(deadline);
while (busy) await new Promise(resolve => setTimeout(resolve, 5));
await store.closeState();
`;
type Receipt = { id: number; count: number; times: number[]; pid: number; locked: boolean };
type Command = { ops?: MeshBatchOperation[]; lease?: Record<string, unknown>; inspect?: boolean };
const writer = async (root: string) => {
  const control = scratch();
  const child: ChildProcessWithoutNullStreams = spawn(process.execPath, ["--input-type=module", "-e", WRITER, root, control], {
    cwd: process.cwd(), env: { ...process.env, PI_FABRIC_MESH_ROOT: root, PI_FABRIC_MESH_STATE_BACKEND: "file", PI_FABRIC_LOCK_STATS: "0" },
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stderr = "", output = "";
  child.stderr.on("data", data => { stderr += String(data); });
  child.stdout.on("data", data => { output += String(data); });
  const closed = new Promise<number | null>((resolve, reject) => {
    child.once("error", reject); child.once("close", resolve);
  });
  cleanups.push(async () => {
    child.stdin.end();
    expect(await bounded(closed, `writer close: ${stderr}`)).toBe(0);
  });
  await waitUntil(() => output.includes("ready"), `writer startup: ${stderr}`);
  const read = fs.readFileSync.bind(fs);
  const latest = (): Receipt | undefined => {
    try { return JSON.parse(read(path.join(control, "ack.json"), "utf8")) as Receipt; } catch { return undefined; }
  };
  let id = 0;
  const request = (command: Command = {}): Receipt => {
    const expected = ++id;
    fs.writeFileSync(path.join(control, "request.json"), JSON.stringify({ id: expected, ...command }));
    // The main JS thread is inside synchronous canonical read/prepare here. Only
    // the independent writer can satisfy this bounded request/ack barrier.
    const until = performance.now() + 2_000;
    const sleeper = new Int32Array(new SharedArrayBuffer(4));
    for (;;) {
      const receipt = latest();
      if (receipt?.id === expected) return receipt;
      if (performance.now() >= until) throw new Error(`writer ack ${expected} missing; ${stderr}`);
      Atomics.wait(sleeper, 0, 0, 2);
    }
  };
  await waitUntil(() => (latest()?.count ?? 0) >= 1, "initial writer commit");
  return { child, latest, request };
};

const seed = async (root: string, large = false): Promise<Store> => {
  const store = new mesh.MeshStore(root, 65536, 100, { lockTimeoutMs: 1000, stateBackend: "file" });
  cleanups.push(() => store.closeState());
  await store.writeBatch({ identity, ops: Array.from({ length: large ? 600 : 1 }, (_, n) => ({
    kind: "put" as const, key: `fixture/retained/${n}`, value: { text: "x".repeat(large ? 960 : 1) },
  })) });
  return store;
};

// Spy only at the public call boundary, always invoking the real implementation.
// This confines the canonical-read barrier to the actual mirror's writeBatch.
const invalidate = (store: Store, competitor: Awaited<ReturnType<typeof writer>>, command: Command = {},
  onInvalidation?: (attempt: number) => void) => {
  const file = path.join(store.root, "state.json"), lock = path.join(store.root, ".lock");
  let active = false, attempts = 0, lockedReads = 0;
  const originalBatch = store.writeBatch.bind(store), read = fs.readFileSync;
  vi.spyOn(store, "writeBatch").mockImplementation(async input => {
    active = true;
    try { return await originalBatch(input); } finally { active = false; }
  });
  vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    const result = (read as (...args: unknown[]) => unknown)(target, ...args);
    if (active && String(target) === file) {
      if (fs.existsSync(lock)) lockedReads++;
      else {
        const before = fs.statSync(file, { bigint: true });
        const committed = competitor.request(command);
        const after = fs.statSync(file, { bigint: true });
        expect(committed.locked).toBe(false);
        expect(`${after.dev}:${after.ino}:${after.mtimeNs}`).not.toBe(`${before.dev}:${before.ino}:${before.mtimeNs}`);
        attempts++;
        onInvalidation?.(attempts);
      }
    }
    return result;
  }) as typeof fs.readFileSync);
  return { attempts: () => attempts, lockedReads: () => lockedReads };
};

const presence = (label: string): Pick<BridgePresence, "hosts" | "participants"> => {
  const now = Date.now(), id = "session:fallback-root-00000000";
  const owner: MeshIdentity = { id, name: "main", kind: "main", sessionId: "fallback-root" };
  return {
    hosts: [{ record: { format: 1, id, rootId: id, identity: owner, startedAt: now - 1000,
      updatedAt: now, expiresAt: now + 60000, incarnationToken: "source-incarnation" }, expiresAt: now + 60000, observedAt: now }],
    participants: [{ format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
      ownerIncarnation: "source-incarnation", name: "main", label, status: "running", runner: "pi", transport: "host",
      capabilities: ["steer", "followUp"], sessionId: "fallback-root", startedAt: now - 1000, updatedAt: now, controlProtocol: "v1" }],
  };
};
const protocol = (store: Store) => {
  const input = new PassThrough(), output = new PassThrough();
  const side = new bridge.StoreBridgeSide(store, "hub");
  const frames: string[] = [];
  output.on("data", chunk => frames.push(String(chunk)));
  const served = bridge.serveBridgeAgent(side, input, output);
  // A real OS stdio pipe on the response leg, like the ssh bridge transport.
  const relay = spawn(process.execPath, ["-e", "process.stdin.pipe(process.stdout)"], { stdio: ["pipe", "pipe", "pipe"] });
  const closed = new Promise<number | null>((resolve, reject) => { relay.once("error", reject); relay.once("close", resolve); });
  output.pipe(relay.stdin);
  const remote = new bridge.RemoteBridgeSide(relay.stdout, input, 5000);
  cleanups.push(async () => {
    input.end(); await bounded(served, "bridge server close"); output.end();
    expect(await bounded(closed, "stdio relay close")).toBe(0);
  });
  return { remote, frames };
};
const noStaging = (root: string): void => {
  expect(fs.readdirSync(root).filter(name => name.endsWith(".prepared.tmp"))).toEqual([]);
  expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
};

describe("bridge callback snapshot fallback (smarty-dev#14050)", () => {
  it("commits fresh mirrors over stdio despite continuous independent same-root commits, and measures actual fallback holds", async () => {
    const root = scratch(), store = await seed(root, true), competitor = await writer(root);
    const bytes = fs.statSync(path.join(root, "state.json")).size;
    expect(bytes).toBeGreaterThan(650_000); expect(bytes).toBeLessThan(850_000);
    // Warm for >=1s, proving commits, not merely a live child or scheduled ticks.
    await waitUntil(() => {
      const times = competitor.latest()?.times ?? [];
      return times.length >= 13 && times.at(-1)! - times[0]! >= 1000;
    }, "writer warm burst");
    const warm = competitor.latest()!;
    const warmMs = warm.times.at(-1)! - warm.times[0]!;
    const burstPerSecond = (warm.times.length - 1) * 1000 / warmMs;
    expect(burstPerSecond).toBeGreaterThanOrEqual(11);
    const startCount = warm.count, startedAt = performance.now();
    const racing = invalidate(store, competitor);
    const transport = protocol(store);
    const holds: number[] = [], refusals: unknown[] = [];
    let previousReads = 0;
    const acquired = locks.lockStats!.acquired.bind(locks.lockStats!);
    vi.spyOn(locks.lockStats!, "acquired").mockImplementation((at, kind, wait, hold) => {
      if (at === root && kind === "bridge" && racing.lockedReads() > previousReads) holds.push(hold);
      previousReads = racing.lockedReads();
      acquired(at, kind, wait, hold);
    });
    for (let sample = 0; sample < 3; sample++) {
      const offered = presence(`FRESH-${sample}`);
      try {
        await transport.remote.mirror(offered);
        const id = offered.hosts[0]!.record.id;
        expect(store.get(participantKey(id), { fresh: true })?.value).toMatchObject({ label: `FRESH-${sample}`, remoteHost: "hub" });
        expect(store.get(hostKey(id), { fresh: true })?.value).toMatchObject({ id, remoteHost: "hub", mirrorLease: 1 });
        const { readHostLease } = await import("../src/topology/host-leases.js");
        const lease = readHostLease(root, id)!;
        expect(lease.mirror?.remoteHost).toBe("hub");
        expect(lease.expiresAt).toBeGreaterThan(Date.now());
      } catch (error) { refusals.push(error); }
    }
    const finishCount = competitor.latest()!.count;
    const elapsedMs = performance.now() - startedAt;
    expect(competitor.child.exitCode).toBeNull(); // alive is not the same as committing
    await waitUntil(() => competitor.latest()!.count > finishCount, "writer resumes after mesh custody");
    const sorted = [...holds].sort((a, b) => a - b);
    console.info("snapshot-fallback-measurement", JSON.stringify({ bytes, entries: 600, burstPerSecond, warmMs,
      warmCommits: warm.count, elapsedMs, concurrentCommits: finishCount - startCount,
      commitsPerSecond: (finishCount - startCount) * 1000 / elapsedMs,
      optimisticInvalidations: racing.attempts(), fallbackReads: racing.lockedReads(), fallbackHoldsMs: holds,
      fallbackHoldMedianMs: sorted[Math.floor(sorted.length / 2)] ?? null, fallbackHoldMaxMs: sorted.at(-1) ?? null,
      refusals: refusals.map(error => ({ message: String(error), code: (error as { code?: string }).code })) }));
    expect((finishCount - startCount) * 1000 / elapsedMs).toBeGreaterThanOrEqual(0.81);
    expect(transport.frames.join("")).toContain('"ok":');
    expect(racing.attempts()).toBeGreaterThanOrEqual(6);
    noStaging(root);
    expect(refusals, "ordinary mirror must fall back before its original 1000ms deadline").toEqual([]);
    expect(holds).toHaveLength(3);
    expect(holds.every(hold => Number.isFinite(hold) && hold > 0)).toBe(true);
  }, 20000);

  it.each([false, true])("preserves typed exhaustion and no effects when the original budget is spent (bounded try=%s)", async boundedTry => {
    const root = scratch(), store = await seed(root), competitor = await writer(root);
    const realNow = Date.now.bind(Date);
    let skew = 0;
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    const racing = invalidate(store, competitor, {}, attempt => { if (!boundedTry && attempt === 2) skew += 2000; });
    const prepare = vi.fn(() => [{ kind: "put" as const, key: "safety/refused/new", value: "must not commit" }]);
    const beforeCommit = vi.fn(), afterCommit = vi.fn(), commitOutbox = vi.fn();
    const write = () => store.writeBatch({ identity, lockClass: "bridge", ops: [], prepare, beforeCommit, afterCommit, commitOutbox });
    await expect(boundedTry ? store.withTryLock(write, 0) : write()).rejects.toBeInstanceOf(mesh.MeshSnapshotRetriesExhaustedError);
    expect(racing.attempts()).toBe(boundedTry ? 1 : 2);
    expect(prepare).not.toHaveBeenCalled(); expect(beforeCommit).not.toHaveBeenCalled();
    expect(afterCommit).not.toHaveBeenCalled(); expect(commitOutbox).not.toHaveBeenCalled();
    expect(store.get("safety/refused/new", { fresh: true })).toBeUndefined();
    noStaging(root);
  });

  it.each(["callback", "encoding"] as const)("refuses without commit effects if fresh fallback %s spends the remaining original budget", async phase => {
    const root = scratch(), store = await seed(root), competitor = await writer(root);
    const realNow = Date.now.bind(Date); let skew = 0;
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    const racing = invalidate(store, competitor);
    const prepare = vi.fn(() => {
      expect(fs.existsSync(path.join(root, ".lock"))).toBe(true);
      if (phase === "callback") skew += 2000;
      return [{ kind: "put" as const, key: "safety/slow-prepare/new", value: { toJSON() {
        if (phase === "encoding") skew += 2000;
        return "must not commit";
      } } }];
    });
    const beforeCommit = vi.fn(), afterCommit = vi.fn(), commitOutbox = vi.fn();
    await expect(store.writeBatch({ identity, lockClass: "bridge", ops: [], prepare, beforeCommit, afterCommit, commitOutbox }))
      .rejects.toBeInstanceOf(mesh.MeshSnapshotRetriesExhaustedError);
    expect(racing.attempts()).toBe(2);
    expect(prepare).toHaveBeenCalledOnce();
    expect(beforeCommit).not.toHaveBeenCalled(); expect(afterCommit).not.toHaveBeenCalled(); expect(commitOutbox).not.toHaveBeenCalled();
    expect(store.get("safety/slow-prepare/new", { fresh: true })).toBeUndefined();
    noStaging(root);
  });

  it("rechecks CAS on the fallback snapshot atomically, with no prefix or callback effects", async () => {
    const root = scratch(), store = await seed(root);
    const before = await store.put({ key: "safety/cas/target", value: "old", identity });
    const competitor = await writer(root);
    const racing = invalidate(store, competitor, { ops: [{ kind: "put", key: before.key, value: "winner" }] });
    const afterCommit = vi.fn(), commitOutbox = vi.fn(), beforeCommit = vi.fn();
    await expect(store.writeBatch({ identity, lockClass: "bridge", prepare: () => [], beforeCommit, afterCommit, commitOutbox, ops: [
      { kind: "put", key: "safety/cas/prefix", value: "must not commit" },
      { kind: "put", key: before.key, value: "loser", ifVersion: before.version },
    ] })).rejects.toMatchObject({ key: before.key, expected: before.version });
    expect(racing.attempts()).toBe(2);
    expect(store.get(before.key, { fresh: true })?.value).toBe("winner");
    expect(store.get("safety/cas/prefix", { fresh: true })).toBeUndefined();
    expect(beforeCommit).not.toHaveBeenCalled(); expect(afterCommit).not.toHaveBeenCalled(); expect(commitOutbox).not.toHaveBeenCalled();
    noStaging(root);
  });

  it("keeps a concurrent native takeover and its lease over the actual remote mirror fallback", async () => {
    const root = scratch(), store = await seed(root), competitor = await writer(root);
    const offered = presence("REMOTE-MUST-NOT-REPLACE-NATIVE"), host = offered.hosts[0]!.record;
    const participant = { ...offered.participants[0]!, label: "NATIVE-WINNER" };
    const lease = { id: host.id, rootId: host.rootId, identityId: host.identity.id, startedAt: host.startedAt,
      incarnationToken: "native-incarnation", updatedAt: Date.now(), expiresAt: Date.now() + 60000 };
    const racing = invalidate(store, competitor, { ops: [
      { kind: "put", key: hostKey(host.id), identity: host.identity, value: host },
      { kind: "put", key: participantKey(host.id), identity: host.identity, value: participant },
    ], lease });
    const transport = protocol(store);
    await transport.remote.mirror(offered);
    expect(racing.attempts()).toBeGreaterThanOrEqual(2); expect(racing.lockedReads()).toBeGreaterThan(0);
    expect(store.get(hostKey(host.id), { fresh: true })?.value).toEqual(host);
    expect(store.get(participantKey(host.id), { fresh: true })?.value).toEqual(participant);
    const { readHostLease } = await import("../src/topology/host-leases.js");
    expect(readHostLease(root, host.id)).toMatchObject(lease);
    expect(readHostLease(root, host.id)?.mirror).toBeUndefined();
    noStaging(root);
  });

  it("keeps bridge static CAS without prepare ineligible for fallback", async () => {
    const root = scratch(), store = await seed(root);
    const before = await store.put({ key: "safety/static-cas/target", value: "retained", identity });
    const competitor = await writer(root), racing = invalidate(store, competitor);
    const beforeCommit = vi.fn(), afterCommit = vi.fn(), commitOutbox = vi.fn();
    await expect(store.writeBatch({ identity, lockClass: "bridge", beforeCommit, afterCommit, commitOutbox, ops: [
      { kind: "put", key: before.key, value: "must not commit", ifVersion: before.version },
    ] })).rejects.toBeInstanceOf(mesh.MeshSnapshotRetriesExhaustedError);
    expect(racing.attempts()).toBeGreaterThan(2);
    expect(racing.lockedReads()).toBe(0);
    expect(store.get(before.key, { fresh: true })).toEqual(before);
    expect(beforeCommit).not.toHaveBeenCalled(); expect(afterCommit).not.toHaveBeenCalled(); expect(commitOutbox).not.toHaveBeenCalled();
    noStaging(root);
  });

  it("excludes independent commits while .lock is held, then lets the same live writer resume", async () => {
    const root = scratch(), store = await seed(root), competitor = await writer(root);
    let count = 0;
    await store.exclusive(() => {
      const observed = competitor.request({ inspect: true });
      expect(observed.locked).toBe(true);
      expect(observed.pid).toBe(competitor.child.pid);
      count = observed.count;
      expect(competitor.latest()!.count).toBe(count);
    });
    expect(competitor.child.exitCode).toBeNull();
    await waitUntil(() => competitor.latest()!.count > count, "writer commits after lock release");
    noStaging(root);
  });
});
