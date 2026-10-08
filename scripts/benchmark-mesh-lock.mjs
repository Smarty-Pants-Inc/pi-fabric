#!/usr/bin/env node
// Fixed-load mesh lock benchmark: the acceptance number of the mesh-lock redesign and, later,
// its CI ratchet (smarty-dev#6477 L9a, smarty-dev#6676). Synthetic only: it seeds its own
// mesh root in $TMPDIR and never opens a live mesh. Runs against dist/ (bun run build first);
// on a shared host run it under nice -n 19. docs/mesh-lock-bench.md explains the load,
// the metrics and the ratchet.
//
//   node scripts/benchmark-mesh-lock.mjs [--duration 120] [--warmup 15] [--state-mb 4.8] ...
//     [--out result.json] [--baseline bench/mesh-lock-baseline.json --max-regress 10] [--gate-timing]
//     [--write-baseline FILE --note TEXT]
//
// Exit status: 0 ok, 1 ratchet regression, 2 invalid run (lost ops, unexpected errors, a
// mismatch with L8's own stats files) or bad usage.
import { AsyncLocalStorage } from "node:async_hooks";
import { fork } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  DEFAULT_LOAD, L8_BOUNDS_MS, LOAD_INSENSITIVE_METRICS, TIMING_METRICS, compareToBaseline, histogramIndex,
  histogramPercentile, planSchedule, processOf, samplePercentile,
} from "./lib/mesh-lock-bench.mjs";

const self = fileURLToPath(import.meta.url);
const repo = path.resolve(path.dirname(self), "..");
const distMesh = path.join(repo, "dist/mesh.js");
const LOCK_STATS_KEY = Symbol.for("pi-fabric.mesh.lock-stats");
const COMMIT_STATS_KEY = Symbol.for("pi-fabric.mesh.commit-stats");
const LOCK_TIMEOUT = "FABRIC_MESH_LOCK_TIMEOUT";
const DIRECTORY_UNAVAILABLE = "FABRIC_DIRECTORY_UNAVAILABLE";
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 500;
const DRAIN_MS = 30_000;

const pad = (value, width) => String(value).padStart(width, "0");
const identityOf = p => ({ id: `bench-p${pad(p, 3)}`, name: `bench-p${pad(p, 3)}`, kind: "main", sessionId: `bench-session-${pad(p, 3)}` });
const registryKey = k => `registry/actors/k${pad(k, 4)}`;
// Fixed-width values: bytes rewritten per op must not drift with counters or clocks.
const registryValue = (k, op) => ({ key: k, op: pad(op, 8), status: "running", note: "r".repeat(320) });
const participantRecord = p => ({ id: identityOf(p).id, kind: "root", status: "idle", hostId: identityOf(p).id, cwd: `/srv/bench/${pad(p, 3)}` });
const hostRecord = (p, now) => ({ hostId: identityOf(p).id, identity: identityOf(p), heartbeatAt: now, expiresAt: now + 600_000 });
const fillerValue = i => ({ id: `a${pad(i, 6)}`, kind: "actor", status: i % 3 ? "idle" : "done", host: `h${pad(i % 32, 2)}`,
  model: "bench-model", summary: "s".repeat(820) });
const codeOf = error => error?.code ?? error?.name ?? "Error";
const emptyPhase = () => ({
  ops: {}, holds: 0, timeouts: 0, tries: 0, commits: 0, bytes: 0, holdMs: 0, classes: {},
  waitHist: new Array(L8_BOUNDS_MS.length + 1).fill(0), holdHist: new Array(L8_BOUNDS_MS.length + 1).fill(0),
  waits: [], holdSamples: [], lagMs: [], receiptMs: [], directoryUnavailable: 0, steerFailed: 0, errors: {}, errorSamples: [],
});

// ---------------------------------------------------------------------------------------------
// Worker: runs its share of the participants, one MeshStore each (as one Main has one store).

const worker = async () => {
  const load = JSON.parse(process.env.MESH_LOCK_BENCH_LOAD);
  const index = Number(process.env.MESH_LOCK_BENCH_WORKER);
  const root = process.env.MESH_LOCK_BENCH_ROOT;
  const leaseDirectory = process.env.MESH_LOCK_BENCH_LEASES;
  const { MeshStore } = await import(pathToFileURL(distMesh).href);
  // The store captured L8's recorder (PI_FABRIC_LOCK_STATS) and the commit counter
  // (PI_FABRIC_COMMIT_STATS) at module load. Wrapping their methods keeps L8's own files
  // exact (cross-checked by the parent) and attributes each record to the op that caused it.
  const lockStats = globalThis[LOCK_STATS_KEY]?.stats;
  const lockRegistry = globalThis[LOCK_STATS_KEY];
  const commitCounter = globalThis[COMMIT_STATS_KEY]?.counter;
  if (!lockStats) throw new Error("L8 lock stats are off in the worker (PI_FABRIC_LOCK_STATS)");
  if (!commitCounter) throw new Error("commit stats are off in the worker (PI_FABRIC_COMMIT_STATS)");
  const context = new AsyncLocalStorage();
  const phases = Object.create(null);
  const phaseOf = name => phases[name] ??= emptyPhase();
  const current = () => phaseOf(context.getStore()?.phase ?? "outside");
  const { acquired, failed } = lockStats;
  lockStats.acquired = (lockRoot, lockClass, waitMs, holdMs) => {
    acquired.call(lockStats, lockRoot, lockClass, waitMs, holdMs);
    const phase = current();
    phase.holds++;
    phase.holdMs += holdMs;
    phase.classes[lockClass] = (phase.classes[lockClass] ?? 0) + 1;
    phase.waitHist[histogramIndex(waitMs)]++;
    phase.holdHist[histogramIndex(holdMs)]++;
    phase.waits.push(waitMs);
    phase.holdSamples.push(holdMs);
  };
  lockStats.failed = (lockRoot, lockClass, waitMs, tried) => {
    failed.call(lockStats, lockRoot, lockClass, waitMs, tried);
    const phase = current();
    if (tried) phase.tries++;
    else phase.timeouts++;
  };
  const record = commitCounter.record;
  commitCounter.record = (bytes, keys) => {
    record.call(commitCounter, bytes, keys);
    const phase = current();
    phase.commits++;
    phase.bytes += bytes;
  };
  const countError = (phaseName, error) => {
    const phase = phaseOf(phaseName);
    const code = codeOf(error);
    phase.errors[code] = (phase.errors[code] ?? 0) + 1;
    if (code !== LOCK_TIMEOUT && phase.errorSamples.length < 5) phase.errorSamples.push(String(error?.stack ?? error));
  };

  const perProcess = Math.ceil(load.participants / load.processes);
  const mine = [];
  for (let p = index * perProcess; p < Math.min(load.participants, (index + 1) * perProcess); p++) mine.push(p);
  // Configured as a Main's runtime store (fabric-runtime-state.ts), idle.
  const options = { lockTimeoutMs: load.lockTimeoutMs, lockProtocol: load.lockProtocol, backgroundReadCacheMs: 5_000, readActive: () => false };
  const stores = new Map(mine.map(p => [p, new MeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS, options)]));
  const reader = new MeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS, options);
  for (const [p, store] of stores) store.get(`topology/hosts/${identityOf(p).id}`);
  const health = new Map(mine.map(p => [p, { okAt: 0, timedOut: false }]));
  // The participant directory's write-stall signal, simplified: the last heartbeat hit the lock
  // timeout, or none committed for two intervals (the peer-lapse refinement is not modelled).
  const stalled = (p, now) => {
    const state = health.get(p);
    return state.timedOut || now - state.okAt > 2 * load.heartbeatS * 1000;
  };
  const writeLease = p => {
    const file = path.join(leaseDirectory, `${identityOf(p).id}.json`);
    fs.writeFileSync(`${file}.tmp`, JSON.stringify({ hostId: identityOf(p).id, renewedAt: Date.now() }));
    fs.renameSync(`${file}.tmp`, file);
  };

  const pending = new Map();
  const inflight = new Set();
  const track = promise => {
    inflight.add(promise);
    void promise.finally(() => inflight.delete(promise));
    return promise;
  };
  const runOp = async (op, t0) => {
    const phase = phaseOf(op.phase);
    phase.ops[op.kind] = (phase.ops[op.kind] ?? 0) + 1;
    phase.lagMs.push(Date.now() - (t0 + op.t));
    const p = op.p;
    const me = identityOf(p);
    const store = stores.get(p);
    try {
      await context.run({ phase: op.phase }, async () => {
        switch (op.kind) {
          case "heartbeat": {
            // An idle Main's tick: renew the presence file lease, read its own host record,
            // then take the lock once without a write (confirmWritable).
            writeLease(p);
            store.get(`topology/hosts/${me.id}`);
            try {
              await store.confirmWritable();
              health.set(p, { okAt: Date.now(), timedOut: false });
            } catch (error) {
              if (codeOf(error) === LOCK_TIMEOUT) health.get(p).timedOut = true;
              throw error;
            }
            return;
          }
          case "hostRenew":
            await store.writeBatch({ identity: me, ops: [{ kind: "put", key: `topology/hosts/${me.id}`, value: now => hostRecord(p, now) }] });
            return;
          case "put":
            await store.put({ key: registryKey(op.key), value: registryValue(op.key, op.id), identity: me });
            return;
          case "delete":
            await store.delete({ key: registryKey(op.key) });
            return;
          case "publish":
            await store.publish({ topic: "bench.messages", from: me, text: "m".repeat(400), data: { op: pad(op.id, 8) } });
            return;
          case "steer": {
            // Route as agents.steer does: a fresh directory read of the target first.
            const target = identityOf(op.to);
            let routed;
            try { routed = store.get(`topology/participants/${target.id}`, { fresh: true }); } catch { routed = undefined; }
            if (!routed || stalled(p, Date.now())) {
              phase.directoryUnavailable++;
              phase.steerFailed++;
              return;
            }
            pending.set(op.id, { sentAt: performance.now(), phase: op.phase });
            await store.publish({ topic: "bench.steer", from: me, to: target.id, text: "s".repeat(200),
              data: { steer: op.id, phase: op.phase, from: p } });
            return;
          }
          case "dirRead": {
            let listed = 0;
            try { listed = store.listAll("topology/participants/", { fresh: true }).length; } catch { listed = 0; }
            if (!listed || stalled(p, Date.now())) phase.directoryUnavailable++;
            return;
          }
          default:
            throw new Error(`unknown op ${op.kind}`);
        }
      });
    } catch (error) {
      if (op.kind === "steer") pending.delete(op.id);
      countError(op.phase, error);
    }
  };

  // One event-log poller per process (the host's bridge/control-plane tail): the target acks a
  // steer with an addressed receipt; the sender times publish-to-receipt.
  const byId = new Map(mine.map(p => [identityOf(p).id, p]));
  let cursor = reader.latestOffset();
  const poll = () => {
    try {
      for (;;) {
        const { events, nextOffset } = reader.tail(cursor, MAX_READ_EVENTS);
        cursor = nextOffset;
        for (const event of events) {
          if (!event.to || !byId.has(event.to)) continue;
          if (event.topic === "bench.steer") {
            const target = byId.get(event.to);
            const { steer, phase, from } = event.data;
            track(context.run({ phase }, () => stores.get(target).publish({ topic: "bench.receipt", from: identityOf(target),
              to: identityOf(from).id, data: { steer } })).catch(error => countError(phase, error)));
          } else if (event.topic === "bench.receipt") {
            const sent = pending.get(event.data.steer);
            if (!sent) continue;
            pending.delete(event.data.steer);
            phaseOf(sent.phase).receiptMs.push(performance.now() - sent.sentAt);
          }
        }
        if (events.length < MAX_READ_EVENTS) break;
      }
    } catch (error) { countError("outside", error); }
  };

  const schedule = planSchedule(load).filter(op => processOf(load, op.p) === index);
  process.on("message", async message => {
    if (message.type === "start") {
      const t0 = message.t0;
      for (const p of mine) health.set(p, { okAt: t0, timedOut: false });
      const poller = setInterval(poll, load.pollMs);
      const done = schedule.map(op => new Promise(resolve => {
        setTimeout(() => resolve(runOp(op, t0)), Math.max(0, t0 + op.t - Date.now()));
      }));
      await Promise.all(done);
      const deadline = Date.now() + DRAIN_MS;
      while (pending.size && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 50));
      process.send({ type: "drained" });
      process.once("message", async () => {
        clearInterval(poller);
        await Promise.allSettled([...inflight]);
        lockRegistry.flush();
        process.send({ type: "result", result: { pid: process.pid, participants: mine.length, phases, missingReceipts: pending.size } },
          () => process.disconnect());
      });
    }
  });
  process.send({ type: "ready" });
};

// ---------------------------------------------------------------------------------------------
// Parent: seed, start the workers on one clock, aggregate, compare.

const FLAGS = {
  participants: "participants", processes: "processes", seed: "seed", duration: "durationS", warmup: "warmupS",
  "state-mb": "stateMb", "heartbeat-s": "heartbeatS", "host-renew-s": "hostRenewS", "put-rate": "putRate",
  "delete-rate": "deleteRate", "publish-rate": "publishRate", "steer-rate": "steerRate", "dir-read-rate": "dirReadRate",
  "registry-keys": "registryKeys", "poll-ms": "pollMs", "lock-timeout-ms": "lockTimeoutMs", "lock-protocol": "lockProtocol",
};
const HARNESS_FLAGS = new Set(["out", "baseline", "max-regress", "write-baseline", "note"]);
const usage = () => [
  "usage: node scripts/benchmark-mesh-lock.mjs [load flags] [--out FILE] [--baseline FILE [--max-regress PCT] [--gate-timing]]",
  "                                          [--write-baseline FILE [--note TEXT]]",
  `load flags (defaults): ${Object.entries(FLAGS).map(([flag, key]) => `--${flag} ${DEFAULT_LOAD[key]}`).join(" ")}`,
].join("\n");
const parseArgs = argv => {
  const load = { ...DEFAULT_LOAD };
  const harness = { maxRegress: 10, gateTiming: false };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") { console.log(usage()); process.exit(0); }
    if (arg === "--gate-timing") { harness.gateTiming = true; continue; }
    const match = /^--([a-z0-9-]+)(?:=(.*))?$/.exec(arg);
    if (!match || (!FLAGS[match[1]] && !HARNESS_FLAGS.has(match[1]))) throw new Error(`unknown argument ${arg}\n${usage()}`);
    const value = match[2] ?? argv[++i];
    if (value === undefined) throw new Error(`--${match[1]} needs a value`);
    if (FLAGS[match[1]]) {
      const number = Number(value);
      if (!Number.isFinite(number) || number < 0) throw new Error(`--${match[1]} must be a non-negative number`);
      load[FLAGS[match[1]]] = number;
    } else if (match[1] === "max-regress") harness.maxRegress = Number(value);
    else harness[match[1].replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = value;
  }
  for (const key of ["participants", "processes", "seed", "registryKeys", "lockProtocol"]) {
    if (!Number.isInteger(load[key])) throw new Error(`${key} must be an integer`);
  }
  if (load.participants < 2 || load.processes < 1 || load.processes > load.participants) throw new Error("need 2+ participants and 1..participants processes");
  if (!(load.durationS > 0) || !(load.heartbeatS > 0) || !(load.pollMs > 0) || !(load.registryKeys > 0)) {
    throw new Error("duration, heartbeat, poll interval and registry keys must be positive");
  }
  if (!Number.isFinite(harness.maxRegress) || harness.maxRegress < 0) throw new Error("--max-regress must be a non-negative number");
  return { load, harness };
};

const seedMesh = async (MeshStore, root, load) => {
  const store = new MeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS, { lockTimeoutMs: 120_000 });
  const identity = { id: "bench-seeder", name: "bench-seeder", kind: "main", sessionId: "bench-seed" };
  const ops = [];
  for (let p = 0; p < load.participants; p++) {
    ops.push({ kind: "put", key: `topology/participants/${identityOf(p).id}`, value: participantRecord(p) });
    ops.push({ kind: "put", key: `topology/hosts/${identityOf(p).id}`, value: now => hostRecord(p, now) });
  }
  for (let k = 0; k < load.registryKeys; k++) ops.push({ kind: "put", key: registryKey(k), value: registryValue(k, 0) });
  await store.writeBatch({ identity, ops });
  // Fleet-like bulk (an actor registry) until state.json reaches --state-mb (decimal MB).
  const statePath = path.join(root, "state.json");
  const target = load.stateMb * 1e6;
  let filler = 0;
  for (let size = fs.statSync(statePath).size; size < target; size = fs.statSync(statePath).size) {
    const count = Math.max(1, Math.min(1000, Math.ceil((target - size) / 1000)));
    await store.writeBatch({ identity, ops: Array.from({ length: count }, () => {
      const i = filler++;
      return { kind: "put", key: `fleet/actors/a${pad(i, 6)}`, value: fillerValue(i) };
    }) });
  }
  return { stateBytes: fs.statSync(statePath).size, entries: ops.length + filler, fillerEntries: filler };
};

const runWorkers = (load, root, leases, scratch) => new Promise((resolve, reject) => {
  const children = [];
  const results = [];
  let ready = 0;
  let drained = 0;
  let failedWith;
  const fail = error => {
    if (failedWith) return;
    failedWith = error;
    for (const child of children) child.kill("SIGTERM");
  };
  let exited = 0;
  for (let index = 0; index < load.processes; index++) {
    const child = fork(self, ["--worker"], {
      env: {
        ...process.env, PI_FABRIC_LOCK_STATS: "1", PI_FABRIC_COMMIT_STATS: path.join(scratch, `commits-${index}.jsonl`),
        MESH_LOCK_BENCH_LOAD: JSON.stringify(load), MESH_LOCK_BENCH_WORKER: String(index), MESH_LOCK_BENCH_ROOT: root,
        MESH_LOCK_BENCH_LEASES: leases,
      },
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    children.push(child);
    child.on("message", message => {
      if (message.type === "ready" && ++ready === load.processes) {
        const t0 = Date.now() + 1_000;
        for (const each of children) each.send({ type: "start", t0 });
      } else if (message.type === "drained" && ++drained === load.processes) {
        for (const each of children) each.send({ type: "finish" });
      } else if (message.type === "result") results[index] = message.result;
    });
    child.once("error", fail);
    // Join every child, also on failure: cleanup must not race a surviving writer.
    child.once("exit", (code, signal) => {
      if (code !== 0 || !results[index]) fail(new Error(`worker ${index} exited ${code ?? signal} without a result`));
      if (++exited === load.processes) failedWith ? reject(failedWith) : resolve(results);
    });
  }
});

/** L8's own files: every acquisition the workers saw must be there, histogram for histogram. */
const readL8Files = root => {
  const directory = path.join(root, "lock-stats");
  const total = { files: 0, n: 0, timeouts: 0, tries: 0, waitHist: new Array(L8_BOUNDS_MS.length + 1).fill(0), holdHist: new Array(L8_BOUNDS_MS.length + 1).fill(0) };
  for (const name of fs.readdirSync(directory).filter(name => name.endsWith(".json"))) {
    const file = JSON.parse(fs.readFileSync(path.join(directory, name), "utf8"));
    total.files++;
    for (const { classes } of file.minutes) {
      for (const bucket of Object.values(classes)) {
        total.n += bucket.n;
        total.timeouts += bucket.timeouts;
        total.tries += bucket.tries;
        bucket.waitHist.forEach((count, i) => { total.waitHist[i] += count; });
        bucket.holdHist.forEach((count, i) => { total.holdHist[i] += count; });
      }
    }
  }
  return total;
};

const mergePhases = phases => {
  const into = emptyPhase();
  for (const phase of phases) {
    if (!phase) continue;
    for (const key of ["holds", "timeouts", "tries", "commits", "bytes", "holdMs", "directoryUnavailable", "steerFailed"]) into[key] += phase[key];
    for (const key of ["ops", "classes", "errors"]) for (const [name, count] of Object.entries(phase[key])) into[key][name] = (into[key][name] ?? 0) + count;
    for (const key of ["waits", "holdSamples", "lagMs", "receiptMs", "errorSamples"]) for (const value of phase[key]) into[key].push(value);
    phase.waitHist.forEach((count, i) => { into.waitHist[i] += count; });
    phase.holdHist.forEach((count, i) => { into.holdHist[i] += count; });
  }
  return into;
};

const round = (value, digits = 3) => Number.isFinite(value) ? Math.round(value * 10 ** digits) / 10 ** digits : value;

const summarize = (load, fixture, schedule, workers, l8, wallMs) => {
  const measure = mergePhases(workers.map(w => w.phases.measure));
  const all = mergePhases(workers.flatMap(w => Object.values(w.phases)));
  const outside = mergePhases(workers.map(w => w.phases.outside));
  const planned = schedule.filter(op => op.phase === "measure");
  const ops = Object.values(measure.ops).reduce((sum, count) => sum + count, 0);
  const plannedByKind = {};
  for (const op of planned) plannedByKind[op.kind] = (plannedByKind[op.kind] ?? 0) + 1;
  const windowMs = load.durationS * 1000;
  const requests = measure.holds + measure.timeouts + measure.tries;
  const unexpectedErrors = Object.fromEntries(Object.entries(all.errors).filter(([code]) => code !== LOCK_TIMEOUT));
  const missingReceipts = workers.reduce((sum, w) => sum + w.missingReceipts, 0);
  const sameHist = (a, b) => a.length === b.length && a.every((count, i) => count === b[i]);
  const validation = {
    plannedOps: planned.length, ranOps: ops, opsMatch: ops === planned.length,
    unexpectedErrors, errorSamples: all.errorSamples.slice(0, 5),
    missingReceipts,
    lockAcquisitionsOutsideOps: outside.holds,
    l8Files: l8.files, l8Acquisitions: l8.n, harnessAcquisitions: all.holds,
    l8Match: l8.files === load.processes && l8.n === all.holds && l8.timeouts === all.timeouts && l8.tries === all.tries &&
      sameHist(l8.waitHist, all.waitHist) && sameHist(l8.holdHist, all.holdHist),
  };
  validation.ok = validation.opsMatch && !Object.keys(unexpectedErrors).length && !missingReceipts && validation.l8Match;
  const loadInsensitive = {
    acquisitionsPerOp: round(requests / ops, 5),
    holdsPerOp: round(measure.holds / ops, 5),
    stateRewritesPerOp: round(measure.commits / ops, 5),
    bytesRewrittenPerOp: round(measure.bytes / ops, 0),
    timeoutsPer10kOps: round(measure.timeouts / ops * 10_000, 3),
  };
  const timing = {
    lockWaitP95Ms: round(samplePercentile(measure.waits, 0.95)),
    lockHoldP95Ms: round(samplePercentile(measure.holdSamples, 0.95)),
    receiptP95Ms: round(samplePercentile(measure.receiptMs, 0.95)),
    lockBusyPct: round(measure.holdMs / windowMs * 100, 2),
  };
  const reported = {
    ops, opsPerS: round(ops / load.durationS, 2), opsByKind: measure.ops,
    lockAcquisitionsPerS: round(measure.holds / load.durationS, 2),
    lockAcquisitionsByClass: measure.classes,
    lockWaitP95BucketMs: histogramPercentile(measure.waitHist, 0.95),
    lockHoldP95BucketMs: histogramPercentile(measure.holdHist, 0.95),
    lockWaitMeanMs: round(measure.waits.reduce((a, b) => a + b, 0) / Math.max(1, measure.waits.length)),
    lockHoldMeanMs: round(measure.holdMs / Math.max(1, measure.holds)),
    lockWaitMaxMs: round(Math.max(0, ...measure.waits)),
    lockHoldMaxMs: round(Math.max(0, ...measure.holdSamples)),
    lockTimeouts: measure.timeouts, boundedTries: measure.tries,
    lockTimeoutsPer10kOps: loadInsensitive.timeoutsPer10kOps,
    opLockTimeoutErrors: measure.errors[LOCK_TIMEOUT] ?? 0,
    directoryUnavailable: measure.directoryUnavailable,
    directoryUnavailablePer10kOps: round(measure.directoryUnavailable / ops * 10_000, 3),
    steers: plannedByKind.steer ?? 0, steersFailed: measure.steerFailed, receipts: measure.receiptMs.length,
    receiptP50Ms: round(samplePercentile(measure.receiptMs, 0.5)),
    stateRewrites: measure.commits, bytesRewritten: measure.bytes,
    scheduleLagP95Ms: round(samplePercentile(measure.lagMs, 0.95)),
  };
  return {
    benchmark: "mesh-lock", schema: 1, store: "file",
    host: { name: os.hostname(), cpus: os.cpus().length, load1: round(os.loadavg()[0], 2), node: process.version, platform: process.platform },
    recordedAt: new Date().toISOString(), wallMs: Math.round(wallMs),
    load, fixture, loadInsensitive, timing, reported, validation,
    codes: { lockTimeout: LOCK_TIMEOUT, directoryUnavailable: DIRECTORY_UNAVAILABLE },
  };
};

const table = (result, comparison) => {
  const rows = [["metric", "class", "value", "baseline", "delta"]];
  const lookup = new Map((comparison?.checks ?? []).map(check => [check.metric, check]));
  const row = (metric, metricClass, value) => {
    const check = lookup.get(metric);
    const delta = check && Number.isFinite(check.regressPct) ? `${check.regressPct >= 0 ? "+" : ""}${round(check.regressPct, 1)}%${check.ok ? "" : " FAIL"}`
      : check?.regressPct === Number.POSITIVE_INFINITY ? `new${check.ok ? "" : " FAIL"}` : "";
    rows.push([metric, metricClass, String(value), check ? String(check.baseline) : "", delta]);
  };
  for (const metric of LOAD_INSENSITIVE_METRICS) row(metric, "ratchet", result.loadInsensitive[metric]);
  for (const metric of TIMING_METRICS) row(metric, "timing", result.timing[metric]);
  row("lockAcquisitionsPerS", "report", result.reported.lockAcquisitionsPerS);
  row("lockWaitP95BucketMs (L8)", "report", result.reported.lockWaitP95BucketMs);
  row("lockHoldP95BucketMs (L8)", "report", result.reported.lockHoldP95BucketMs);
  row("directoryUnavailablePer10kOps", "report", result.reported.directoryUnavailablePer10kOps);
  const widths = rows[0].map((_, i) => Math.max(...rows.map(r => r[i].length)));
  return rows.map(r => r.map((cell, i) => cell.padEnd(widths[i])).join("  ").trimEnd()).join("\n");
};

const main = async () => {
  let parsed;
  try { parsed = parseArgs(process.argv.slice(2)); }
  catch (error) { console.error(error.message); return 2; }
  const { load, harness } = parsed;
  if (!fs.existsSync(distMesh)) { console.error(`${distMesh} is missing: run bun run build first`); return 2; }
  // Seeding is not part of the measurement: the parent records nothing.
  process.env.PI_FABRIC_LOCK_STATS = "0";
  delete process.env.PI_FABRIC_COMMIT_STATS;
  const { MeshStore } = await import(pathToFileURL(distMesh).href);
  const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "mesh-lock-bench-"));
  try {
    const root = path.join(scratch, "mesh");
    const leases = path.join(scratch, "leases");
    fs.mkdirSync(root, { mode: 0o700 });
    fs.mkdirSync(leases, { mode: 0o700 });
    const fixture = await seedMesh(MeshStore, root, load);
    const schedule = planSchedule(load);
    const started = performance.now();
    const workers = await runWorkers(load, root, leases, scratch);
    const result = summarize(load, fixture, schedule, workers, readL8Files(root), performance.now() - started);
    let comparison;
    if (harness.baseline) {
      const baseline = JSON.parse(fs.readFileSync(harness.baseline, "utf8"));
      comparison = compareToBaseline(result, baseline, { maxRegressPct: harness.maxRegress, gateTiming: harness.gateTiming });
      result.ratchet = { baseline: harness.baseline, maxRegressPct: harness.maxRegress, gateTiming: harness.gateTiming, ...comparison };
    }
    if (harness.writeBaseline) {
      if (!result.validation.ok) throw new Error("refusing to write a baseline from an invalid run");
      const { ratchet: _ratchet, ...baseline } = result;
      fs.writeFileSync(harness.writeBaseline, JSON.stringify({ note: harness.note ?? "", ...baseline }, null, 2) + "\n");
    }
    if (harness.out) fs.writeFileSync(harness.out, JSON.stringify(result, null, 2) + "\n");
    console.error(table(result, comparison));
    if (!result.validation.ok) console.error(`INVALID RUN: ${JSON.stringify(result.validation)}`);
    if (comparison && !comparison.ok) console.error(`RATCHET FAILED:\n  ${comparison.problems.join("\n  ")}`);
    console.log(JSON.stringify(result, null, 2));
    return !result.validation.ok ? 2 : comparison && !comparison.ok ? 1 : 0;
  } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
};

if (process.argv[2] === "--worker") {
  worker().catch(error => {
    console.error(error);
    process.exit(1);
  });
} else {
  process.exitCode = await main();
}
