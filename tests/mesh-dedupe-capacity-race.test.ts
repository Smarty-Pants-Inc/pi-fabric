import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { buildSync } from "esbuild";
import { afterEach, expect, it } from "vitest";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

// Keep reason: F1 data safety, https://github.com/Smarty-Pants-Inc/pi-fabric/pull/1927.
// A names-only advisory observation cannot reserve capacity against arbitrary intervening
// writers. These are real 10k-key native-process regressions, not mocked receipt counts.
const BUDGET_MS = 300_000;
const READY_MS = 30_000;
const TOPIC = "mesh.dedupe-capacity-race";
const baseControl = process.env.FABRIC_CAPACITY_RACE_BASE_CONTROL === "1";
type Exit = { code: number | null; signal: NodeJS.Signals | null };
type Worker = { child: ChildProcess; closed: Promise<Exit>; terminal: boolean; diagnostics: () => string };
type Ready = {
  pid: number; node: string; bun: null; barrier: string; names?: number;
  reserve?: number; canonicalOwner?: string | null; lockTimeoutMs: number; variant: string;
};
const workers: Worker[] = [];
const roots: string[] = [];
const bounded = async <T>(work: Promise<T>, ms: number, label: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<T>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} exceeded ${ms} ms`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
const json = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const completed = (file: string, value: unknown = true): void => {
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(value), { mode: 0o600 });
  fs.renameSync(`${file}.tmp`, file);
};

const startWorker = (bundle: string, base: string, role: string, writer: string, count: number,
  cap: number, lockTimeoutMs: number): Worker => {
  // Vitest ordinarily runs under Node; explicitly avoid inheriting Bun as the worker runtime.
  const node = process.versions.bun ? "node" : process.execPath;
  const child = spawn(node, [bundle, role, path.join(base, "mesh"), path.join(base, "control"), writer,
    String(count), String(cap), String(lockTimeoutMs), "--variant", baseControl ? "base-late" : "offlock"], {
    stdio: ["ignore", "pipe", "pipe"], cwd: process.cwd(),
    env: { ...process.env, PI_CODING_AGENT_DIR: base, PI_FABRIC_MESH_ROOT: path.join(base, "mesh"),
      PI_FABRIC_LOCK_STATS: "0", PI_FABRIC_MESH_CHECKPOINT_LOG: "off" },
  });
  let diagnostics = "";
  const capture = (chunk: unknown): void => { diagnostics = (diagnostics + String(chunk)).slice(-65_536); };
  child.stdout!.on("data", capture);
  child.stderr!.on("data", capture);
  child.on("error", capture);
  const deadline = setTimeout(() => child.kill("SIGKILL"), BUDGET_MS);
  let worker: Worker;
  const closed = new Promise<Exit>(resolve => child.once("close", (code, signal) => {
    clearTimeout(deadline);
    worker.terminal = true;
    resolve({ code, signal });
  }));
  worker = { child, closed, terminal: false, diagnostics: () => diagnostics };
  workers.push(worker);
  return worker;
};

// Completed-file rename is the primary event source, with a bounded 100ms safety-net check
// for missed filesystem notifications. Never infer readiness from an elapsed sleep.
const waitReady = (worker: Worker, file: string): Promise<Ready> => new Promise((resolve, reject) => {
  let settled = false;
  const finish = (error?: Error): void => {
    if (settled) return;
    if (!error && !fs.existsSync(file)) return;
    settled = true;
    watcher.close();
    clearInterval(fallback);
    clearTimeout(deadline);
    worker.child.off("close", onClose);
    if (error) reject(error);
    else {
      try { resolve(json<Ready>(file)); } catch (readError) { reject(readError); }
    }
  };
  const onClose = (): void => {
    finish();
    if (!settled) finish(new Error(`native worker exited before ${file}: ${worker.diagnostics()}`));
  };
  const watcher = fs.watch(path.dirname(file), () => finish());
  watcher.on("error", error => finish(error));
  const fallback = setInterval(() => finish(), 100);
  const deadline = setTimeout(() => finish(new Error(`no completed readiness file within ${READY_MS} ms: ${file}; ${worker.diagnostics()}`)), READY_MS);
  worker.child.once("close", onClose);
  if (worker.terminal) onClose();
  else finish();
});
const cleanExit = async (worker: Worker): Promise<void> => {
  const exit = await bounded(worker.closed, BUDGET_MS, "native publisher completion");
  expect(exit, worker.diagnostics()).toEqual({ code: 0, signal: null });
};

const seedOneValidReceipt = (root: string): void => {
  const directory = path.join(root, "event-receipts");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  // Same valid, non-live historical receipt + sequence setup as the independent judge.
  // Every intervening key, unlike this single seed, MUST be published through MeshStore.
  const dedupeKey = "capacity:seed:0";
  const seed: MeshEvent = { id: randomUUID(), dedupeKey, sequence: 1, topic: TOPIC, kind: "message",
    from: { id: "session:capacity-seed", name: "seed", kind: "main" }, createdAt: Date.now() };
  fs.writeFileSync(path.join(directory, `${createHash("sha256").update(dedupeKey).digest("hex")}.json`),
    JSON.stringify(seed), { mode: 0o600 });
  fs.writeFileSync(path.join(root, "sequence"), "1", { mode: 0o600 });
};
const receipts = (root: string): { keys: Set<string>; pending: string[] } => {
  const directory = path.join(root, "event-receipts");
  const names = fs.readdirSync(directory);
  const pending = names.filter(name => name.endsWith(".pending.json"));
  const keys = new Set<string>();
  for (const name of names) {
    assert(/^[a-f0-9]{64}(?:\.pending)?\.json$/.test(name), `unexpected receipt-directory entry: ${name}`);
    if (name.endsWith(".pending.json")) continue;
    const receipt = json<MeshEvent>(path.join(directory, name));
    assert(receipt.dedupeKey && receipt.id && Number.isSafeInteger(receipt.sequence) && receipt.sequence > 0);
    assert.equal(name, `${createHash("sha256").update(receipt.dedupeKey).digest("hex")}.json`);
    assert(!keys.has(receipt.dedupeKey), "duplicate receipt key");
    keys.add(receipt.dedupeKey);
  }
  return { keys, pending };
};
const returnedEvents = (control: string, writer: string, count: number): MeshEvent[] => {
  const prefixes = fs.readFileSync(path.join(control, `${writer}.prefixes.jsonl`), "utf8")
    .trim().split("\n").map(line => JSON.parse(line) as MeshEvent[]);
  assert(prefixes.every(prefix => prefix.length > 0 && prefix.length <= 32), "invalid checkpointed prefix");
  const events = prefixes.flat();
  assert.equal(events.length, count, `${writer} did not complete its returned-prefix contract`);
  events.forEach((event, index) => {
    assert.equal(event.dedupeKey, `capacity:${writer}:${index}`, "per-publisher key order changed");
    assert.equal(event.from.id, `session:capacity-${writer}`);
    assert(event.sequence > (index ? events[index - 1]!.sequence : 1), "per-publisher sequence order changed");
  });
  return events;
};

afterEach(async () => {
  const owned = workers.splice(0);
  for (const worker of owned) if (!worker.terminal) worker.child.kill("SIGTERM");
  try {
    await bounded(Promise.all(owned.map(worker => worker.closed)), 2_000, "native worker graceful cleanup");
  } catch {
    for (const worker of owned) if (!worker.terminal) worker.child.kill("SIGKILL");
    await bounded(Promise.all(owned.map(worker => worker.closed)), 8_000, "native worker forced cleanup");
  }
  assert(owned.every(worker => worker.terminal), "do not delete mesh roots while a native writer remains");
  for (const base of roots.splice(0)) {
    fs.rmSync(base, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
    assert(!fs.existsSync(base), "fresh test-owned root was not removed");
  }
}, 15_000);

const cases = [
  { name: "32-key batch", role: "stale-batch", staleCount: 32, cap: 10_034, lockTimeoutMs: 300_000 },
  { name: "single publish with the normal 10s lock timeout", role: "stale-single", staleCount: 1, cap: 10_003, lockTimeoutMs: 10_000 },
] as const;

// Unmeasured Windows runtime risks another red CI: each native race case gets 300 s; Linux covers this platform-independent publish/capacity logic.
// Measurement: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/19935
for (const scenario of cases) {
  it.skipIf(process.platform === "win32")(`keeps receipt capacity after a stale off-lock observation: ${scenario.name}`, async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-dedupe-capacity-race-"));
    roots.push(base);
    fs.chmodSync(base, 0o700);
    const root = path.join(base, "mesh");
    const control = path.join(base, "control");
    fs.mkdirSync(root, { mode: 0o700 });
    fs.mkdirSync(control, { mode: 0o700 });
    seedOneValidReceipt(root);
    const initial = receipts(root);
    expect(initial.keys.size).toBe(1);
    expect(initial.pending).toEqual([]);
    expect(initial.keys.size + scenario.staleCount + 10_000).toBeLessThan(scenario.cap);

    const bundle = path.join(base, "capacity-race-worker.mjs");
    buildSync({ entryPoints: [path.resolve("tests/fixtures/mesh-dedupe-capacity-race-worker.ts")],
      bundle: true, platform: "node", format: "esm", outfile: bundle });
    const stale = startWorker(bundle, base, scenario.role, "stale", scenario.staleCount, scenario.cap, scenario.lockTimeoutMs);
    const snapshot = await waitReady(stale, path.join(control, "stale.ready.json"));
    expect(snapshot.pid).toBe(stale.child.pid);
    expect(snapshot.node).toBeTruthy();
    expect(snapshot.bun).toBeNull();
    expect(snapshot.lockTimeoutMs).toBe(scenario.lockTimeoutMs);
    expect(snapshot.variant).toBe(baseControl ? "base-late" : "offlock");
    if (baseControl) expect(snapshot.barrier).toBe("base-late-before-publish");
    else {
      expect(snapshot.barrier).toBe("after-real-offlock-readdir");
      expect(snapshot.names).toBe(1);
      expect(snapshot.reserve).toBe(scenario.staleCount);
      expect(snapshot.canonicalOwner).toBeNull();
    }
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);

    const fillTotal = scenario.cap - 1;
    const counts = Array.from({ length: 4 }, (_, index) => Math.floor(fillTotal / 4) + (index < fillTotal % 4 ? 1 : 0));
    const fillers = counts.map((count, index) => startWorker(bundle, base, "fill", `fill-${index}`, count, scenario.cap, scenario.lockTimeoutMs));
    const ready = await Promise.all(fillers.map((worker, index) => waitReady(worker, path.join(control, `fill-${index}.ready.json`))));
    ready.forEach((message, index) => {
      expect(message.pid).toBe(fillers[index]!.child.pid);
      expect(message.bun).toBeNull();
      expect(message.barrier).toBe("fill-ready");
      expect(message.lockTimeoutMs).toBe(scenario.lockTimeoutMs);
    });
    completed(path.join(control, "fill-go"));
    await Promise.all(fillers.map(cleanExit));
    const fillEvents = counts.flatMap((count, index) => returnedEvents(control, `fill-${index}`, count));
    expect(fillEvents).toHaveLength(fillTotal);
    expect(new Set(fillEvents.map(event => event.dedupeKey)).size).toBe(fillTotal);
    const before = receipts(root);
    expect(before.pending).toEqual([]);
    expect(before.keys.size).toBe(scenario.cap);
    assert.deepEqual(before.keys, new Set(["capacity:seed:0", ...fillEvents.map(event => event.dedupeKey!)]));

    // No artificial 93-second delay: the barrier proves that all 10,002/10,033 writes finished
    // after the actual observation and before the stale publication can acquire its lock.
    completed(path.join(control, "stale-release"));
    await cleanExit(stale);
    const lateEvents = returnedEvents(control, "stale", scenario.staleCount);
    const staleDone = json<{ committed: number; paused: boolean; lockTimeoutMs: number }>(path.join(control, "stale.done.json"));
    expect(staleDone.committed).toBe(scenario.staleCount);
    expect(staleDone.paused).toBe(!baseControl);
    expect(staleDone.lockTimeoutMs).toBe(scenario.lockTimeoutMs);
    const samples = fs.readFileSync(path.join(control, "stale.capacity.jsonl"), "utf8").trim().split("\n")
      .map(line => JSON.parse(line) as { committed: number; receiptKeys: number; pending: number });
    expect(samples.length).toBeGreaterThan(0);
    expect(samples.at(-1)!.committed).toBe(scenario.staleCount);
    assert(samples.every((sample, index) => sample.pending === 0 &&
      sample.committed > (index ? samples[index - 1]!.committed : 0)), "prefix checkpoints must settle all intents and make progress");
    assert.deepEqual(json<MeshEvent[]>(path.join(control, "stale.replays.json")), lateEvents);
    const lastFillSequence = Math.max(...fillEvents.map(fill => fill.sequence));
    assert(lateEvents.every(event => event.sequence > lastFillSequence),
      "stale publication was not ordered after the entire intervening fill");
    const expected = [...fillEvents, ...lateEvents];
    const raw = fs.readFileSync(path.join(root, "events.jsonl"), "utf8").trim().split("\n")
      .map(line => JSON.parse(line) as MeshEvent);
    expect(raw).toHaveLength(fillTotal + scenario.staleCount);
    expect(new Set(raw.map(event => event.id)).size).toBe(expected.length);
    expect(new Set(raw.map(event => event.dedupeKey)).size).toBe(expected.length);
    expect(new Set(raw.map(event => event.sequence)).size).toBe(expected.length);
    assert(raw.every((event, index) => event.sequence > (index ? raw[index - 1]!.sequence : 1)), "global sequence order changed");
    const observed = new Map(raw.map(event => [event.id, event]));
    for (const event of expected) assert.deepEqual(observed.get(event.id), event, "a successfully returned event is missing or changed");

    // Also observe the publications through the real MeshStore read API, not just raw JSONL.
    const observer = new MeshStore(root, 4_096, 200_000, { stateBackend: "file", maxDedupeReceipts: scenario.cap,
      lockTimeoutMs: scenario.lockTimeoutMs, maxEventLogBytes: 512 * 1024 * 1024, retainedEventLogBytes: 128 * 1024 * 1024 });
    try {
      const read: MeshEvent[] = [];
      for (;;) {
        const page = observer.read({ topic: TOPIC, after: read.at(-1)?.sequence ?? 0, limit: 1_000 });
        if (!page.length) break;
        read.push(...page);
        assert(read.length <= expected.length, "MeshStore read made no cursor progress or duplicated events");
      }
      assert.deepEqual(read, raw);
    } finally { observer.closeState(); }
    const after = receipts(root);
    expect(after.pending).toEqual([]);
    const publishedKeys = new Set(["capacity:seed:0", ...expected.map(event => event.dedupeKey!)]);
    assert([...after.keys].every(key => publishedKeys.has(key)), "receipt does not belong to a seed or real publication");
    assert(lateEvents.every(event => after.keys.has(event.dedupeKey!)), "new stale-key receipts disappeared during exact replay");
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(workers.every(worker => worker.terminal)).toBe(true);
    // Check every returned prefix too: on a slow disk a short first prefix can overshoot,
    // then a later suffix's serialized pruning can conceal that breach in the final count.
    const maximumReceiptKeys = Math.max(after.keys.size, ...samples.map(sample => sample.receiptKeys));
    console.info("DEDUPE_CAPACITY_RACE " + JSON.stringify({ scenario: scenario.name, baseControl,
      cap: scenario.cap, before: before.keys.size, after: after.keys.size, maximumReceiptKeys,
      stalePrefixSamples: samples, publications: raw.length, exactStaleReplays: lateEvents.length,
      nativePids: [stale, ...fillers].map(worker => worker.child.pid), cleanExits: true }));
    // The primary F1 assertions are deliberately last: first prove identity/order/replay/exits,
    // then fail on the actual receipt overshoot without assuming a 32-event first hold.
    expect(after.keys.size, "final unique receipt keys exceeded capacity").toBeLessThanOrEqual(scenario.cap);
    expect(maximumReceiptKeys, `F1: ${scenario.name} exceeded the hard receipt cap after ${fillTotal} real intervening keys`).toBeLessThanOrEqual(scenario.cap);
  }, BUDGET_MS);
}
