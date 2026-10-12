#!/usr/bin/env node
// Induced-contention probe for mesh lock events: the post-install gate of pi-fabric#905
// (smarty-dev#9290, Decision C). It runs against an installed release's dist/ (by default the
// release this script ships in), on a private mesh root it creates in $TMPDIR; it never opens a
// live mesh. Same harness shape as benchmark-mesh-lock.mjs: forked workers load dist/mesh.js.
//
// It deliberately holds each lock past a waiter's timeout, once per store:
//   sqlite:    a writeBatch holds the SQLite state writer lock; a put with a shorter budget gives up
//              with FABRIC_MESH_STATE_BUSY.
//   mesh-lock: an exclusive() holds .lock; an exclusive() with a shorter budget gives up with
//              FABRIC_MESH_LOCK_TIMEOUT.
// and asserts that each timeout (and the long hold) landed in <root>/lock-stats/events.jsonl with
// the v:1 line shape documented in docs/mesh-lock-stats.md, with no append errors, and that each
// wait-timeout event's timestamp (`at`) is within 1 s of the induced timeout (smarty-dev#9290). The
// induced timeout's time is measured in the waiter: the wall-clock time its acquisition started plus
// the budget it was given, i.e. when its lock wait expired. The probe prints both times and the delta.
//
//   node scripts/probe-mesh-lock-events.mjs [--release DIR] [--dir TMP] [--hold-ms 1500] [--timeout-ms 300] [--json]
//
// DIR is the package root holding dist/mesh.js (default: the checkout or release containing this
// script). TMP (default os.tmpdir()) holds the private root; SQLite state refuses a root below a
// group- or other-writable ancestor without the sticky bit, so pick one without (/tmp is fine).
// The workers run with PI_FABRIC_LOCK_STATS unset, so the release default applies.
// Exit status: 0 pass; 1 gate failed (roll back: no event for an induced timeout, an event more than
// 1 s from it, a wrong shape, or failed appends); 2 invalid run or bad usage.
import { fork } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const self = fileURLToPath(import.meta.url);
const EVENTS_KEY = Symbol.for("pi-fabric.mesh.lock-events");
// A fresh SQLite root needs an imported state.db. The probe's private root is seeded the way the test
// fixtures seed theirs (state-sqlite.ts TEST_FIXTURE_INITIALIZE), in a seed worker only; the holder
// and the waiter then open it as any production store does. Never set for a live mesh.
const SQLITE_SEED_KEY = Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures");
const STATE_BUSY = "FABRIC_MESH_STATE_BUSY";
const LOCK_TIMEOUT = "FABRIC_MESH_LOCK_TIMEOUT";
// The v:1 line shape (keep stable: consumers such as commswatch code against these exact keys).
const HELD_KEYS = ["v", "at", "kind", "store", "class", "heldMs", "holder"];
const TIMEOUT_KEYS = ["v", "at", "kind", "store", "class", "waitedMs", "holder", "waiter"];
const HOLDER_KEYS = ["pid", "process", "op", "host"];
const WAITER_KEYS = ["pid", "process", "op"];
// smarty-dev#9290 acceptance: the wait-timeout event lands within 1 s of the induced timeout.
const EVENT_LATENCY_LIMIT_MS = 1_000;

const usage = (message) => {
  console.error(`probe-mesh-lock-events: ${message}`);
  console.error("usage: node scripts/probe-mesh-lock-events.mjs [--release DIR] [--dir TMP] [--hold-ms N] [--timeout-ms N] [--json]");
  process.exit(2);
};
const parseArgs = (argv) => {
  const options = { release: path.resolve(path.dirname(self), ".."), dir: os.tmpdir(), holdMs: 1_500, timeoutMs: 300, json: false, worker: undefined };
  for (let i = 0; i < argv.length; i++) {
    const [flag, inline] = argv[i].split(/=(.*)/s, 2);
    const value = () => inline ?? argv[++i] ?? usage(`${flag} needs a value`);
    if (flag === "--release") options.release = path.resolve(value());
    else if (flag === "--dir") options.dir = path.resolve(value());
    else if (flag === "--hold-ms") options.holdMs = Number(value());
    else if (flag === "--timeout-ms") options.timeoutMs = Number(value());
    else if (flag === "--json") options.json = true;
    else if (flag === "--worker") options.worker = value();
    else usage(`unknown argument ${argv[i]}`);
  }
  if (!Number.isFinite(options.holdMs) || !Number.isFinite(options.timeoutMs) || options.timeoutMs < 100 ||
    options.holdMs < options.timeoutMs + 700) usage("need --timeout-ms >= 100 and --hold-ms >= timeout + 700 (and > 1000 for a held event)");
  return options;
};
const identityOf = (role) => ({ id: `lock-events-probe-${role}-${process.pid}`, name: `lock-events-probe-${role}`, kind: "main" });
const spin = (ms) => { const until = performance.now() + ms; while (performance.now() < until) { /* hold the lock */ } };
const appendErrors = () => {
  const registry = globalThis[EVENTS_KEY];
  return registry === undefined ? "absent" : registry.events === undefined ? "disabled" : registry.events.errors;
};

// ---------------------------------------------------------------------------------------------
// Workers (forked): one MeshStore each, as one Main has one store.

const worker = async (role, options) => {
  const { MeshStore } = await import(pathToFileURL(path.join(options.release, "dist/mesh.js")).href);
  const { root, store: storeKind, holding } = JSON.parse(process.env.LOCK_EVENTS_PROBE);
  if (role === "seed") globalThis[SQLITE_SEED_KEY] = "create";
  const store = new MeshStore(root, 65_536, 100, {
    stateBackend: storeKind === "sqlite" ? "sqlite" : "file",
    ...(role === "waiter" ? { lockTimeoutMs: options.timeoutMs } : {}),
  });
  const result = { role, pid: process.pid };
  try {
    if (role === "seed") {
      await store.put({ key: "probe/seed", value: 1, identity: identityOf(role) });
      result.outcome = "seeded";
    } else if (role === "holder") {
      const hold = () => { fs.writeFileSync(holding, String(process.pid)); spin(options.holdMs); };
      if (storeKind === "sqlite") {
        await store.writeBatch({ identity: identityOf(role), ops: [], prepare: () => { hold(); return [{ kind: "put", key: "probe/held", value: 1 }]; } });
      } else await store.exclusive(hold);
      result.outcome = "held";
    } else {
      const deadline = Date.now() + 30_000;
      while (!fs.existsSync(holding)) {
        if (Date.now() > deadline) throw new Error("the holder never took the lock");
        await new Promise((resolve) => setTimeout(resolve, 2));
      }
      // The induced timeout's own time: when this wait's budget expires, on the wall clock that
      // stamps events (`at` is Date.now() at append).
      result.waitStartedAt = Date.now();
      result.inducedTimeoutAt = result.waitStartedAt + options.timeoutMs;
      const started = performance.now();
      try {
        if (storeKind === "sqlite") await store.put({ key: "probe/waiter", value: 1, identity: identityOf(role) });
        else await store.exclusive(() => undefined, options.timeoutMs);
        result.outcome = "acquired";
      } catch (error) {
        result.errorAt = Date.now();
        result.outcome = "error";
        result.code = error?.code ?? error?.name;
        result.busyCode = error?.busyCode;
        result.message = String(error?.message ?? error).slice(0, 300);
      }
      result.waitedMs = Math.round(performance.now() - started);
    }
  } finally { store.closeState?.(); }
  result.appendErrors = appendErrors();
  process.send(result);
  process.disconnect();
};

// ---------------------------------------------------------------------------------------------
// Parent: induce one timeout per store, then check events.jsonl.

const run = (role, options, probe) => new Promise((resolve, reject) => {
  const env = { ...process.env, LOCK_EVENTS_PROBE: JSON.stringify(probe) };
  delete env.PI_FABRIC_LOCK_STATS; // the release default (on)
  delete env.PI_FABRIC_MESH_STATE_BACKEND;
  const child = fork(self, ["--worker", role, "--release", options.release, "--hold-ms", String(options.holdMs),
    "--timeout-ms", String(options.timeoutMs)], { env, stdio: ["ignore", "inherit", "pipe", "ipc"] });
  let stderr = "";
  let message;
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.on("message", (value) => { message = value; });
  child.once("error", reject);
  child.once("close", (code) => code === 0 && message ? resolve({ ...message, childPid: child.pid })
    : reject(new Error(`${role} worker exited ${code}: ${stderr.trim().slice(0, 1200)}`)));
});

const sameKeys = (value, keys) => value !== null && typeof value === "object" &&
  JSON.stringify(Object.keys(value)) === JSON.stringify(keys);

const scenario = async (storeKind, options, root, problems) => {
  const probe = { root, store: storeKind, holding: `${root}.holding` };
  fs.rmSync(probe.holding, { force: true });
  try {
    if (storeKind === "sqlite") await run("seed", options, probe);
    const [holder, waiter] = await Promise.all([run("holder", options, probe), run("waiter", options, probe)]);
    const fail = (message) => problems.push(`${storeKind}: ${message}`);
    if (waiter.outcome !== "error") fail(`the waiter was not timed out (${waiter.outcome}); the hold did not outlast its budget`);
    else if (storeKind === "sqlite" && waiter.busyCode !== STATE_BUSY) fail(`the waiter failed with ${waiter.code}/${waiter.busyCode}, not ${STATE_BUSY}: ${waiter.message}`);
    else if (storeKind === "mesh-lock" && waiter.code !== LOCK_TIMEOUT) fail(`the waiter failed with ${waiter.code}, not ${LOCK_TIMEOUT}: ${waiter.message}`);
    for (const side of [holder, waiter]) {
      if (side.appendErrors !== 0) fail(`${side.role} lock-event appender: ${side.appendErrors === "absent" ? "absent (a release without lock events)" : side.appendErrors === "disabled" ? "disabled (PI_FABRIC_LOCK_STATS=0?)" : `${side.appendErrors} failed append(s)`}`);
    }
    return { holder, waiter };
  } finally { fs.rmSync(probe.holding, { force: true }); }
};

const readEvents = (file, problems) => {
  let text;
  try { text = fs.readFileSync(file, "utf8"); } catch (error) {
    problems.push(`${path.basename(file)}: ${error.code === "ENOENT" ? "no events file was written" : error.message}`);
    return [];
  }
  if (text && !text.endsWith("\n")) problems.push("events.jsonl does not end in a newline");
  const events = [];
  for (const line of text.split("\n").filter(Boolean)) {
    if (Buffer.byteLength(line) + 1 > 4096) problems.push("a line exceeds PIPE_BUF (4096 bytes)");
    try { events.push(JSON.parse(line)); } catch { problems.push(`an unparsable line: ${line.slice(0, 120)}`); }
  }
  if (process.platform !== "win32") {
    const mode = fs.statSync(file).mode & 0o777;
    if (mode !== 0o600) problems.push(`events.jsonl has mode ${mode.toString(8)}, not 600`);
  }
  return events;
};

const checkEvents = (storeKind, observed, events, options, problems) => {
  const fail = (message) => problems.push(`${storeKind}: ${message}`);
  const mine = events.filter((event) => event?.store === storeKind);
  const timeouts = mine.filter((event) => event.kind === "wait-timeout");
  const held = mine.filter((event) => event.kind === "held");
  if (timeouts.length !== 1) fail(`expected one wait-timeout event for the induced timeout, found ${timeouts.length}`);
  if (held.length !== 1) fail(`expected one held event for the ${options.holdMs} ms hold, found ${held.length}`);
  const [timeout] = timeouts;
  if (timeout) {
    if (!sameKeys(timeout, TIMEOUT_KEYS)) fail(`wait-timeout keys ${JSON.stringify(Object.keys(timeout))}, not ${JSON.stringify(TIMEOUT_KEYS)}`);
    if (timeout.v !== 1) fail(`wait-timeout v=${timeout.v}, not 1`);
    if (!Number.isInteger(timeout.at) || !Number.isInteger(timeout.waitedMs) || timeout.waitedMs < options.timeoutMs * 0.9) fail(`wait-timeout at/waitedMs invalid: ${JSON.stringify(timeout)}`);
    if (!sameKeys(timeout.holder, HOLDER_KEYS) || timeout.holder.op !== null || typeof timeout.holder.host !== "string") fail(`wait-timeout holder shape: ${JSON.stringify(timeout.holder)}`);
    if (!sameKeys(timeout.waiter, WAITER_KEYS) || timeout.waiter.pid !== observed.waiter.childPid || typeof timeout.waiter.process !== "string" || timeout.waiter.op !== timeout.class) fail(`wait-timeout waiter: ${JSON.stringify(timeout.waiter)} (waiter pid ${observed.waiter.childPid})`);
    // Linux names the holder (.lock owner record; SQLite through /proc/locks).
    if (process.platform === "linux" && timeout.holder?.pid !== observed.holder.childPid) fail(`wait-timeout names holder pid ${timeout.holder?.pid}, not ${observed.holder.childPid}`);
  }
  // The event must land within 1 s of the induced timeout (smarty-dev#9290), not merely by the end.
  let timing;
  if (Number.isInteger(observed.waiter.inducedTimeoutAt)) {
    const eventAt = Number.isInteger(timeout?.at) ? timeout.at : null;
    timing = { inducedTimeoutAt: observed.waiter.inducedTimeoutAt, waiterErrorAt: observed.waiter.errorAt ?? null, eventAt,
      deltaMs: eventAt === null ? null : eventAt - observed.waiter.inducedTimeoutAt, limitMs: EVENT_LATENCY_LIMIT_MS };
    if (timing.deltaMs === null) fail(`no wait-timeout event time to compare with the induced timeout at ${iso(timing.inducedTimeoutAt)}`);
    else if (Math.abs(timing.deltaMs) > EVENT_LATENCY_LIMIT_MS) fail(`the wait-timeout event at ${iso(eventAt)} is ${signed(timing.deltaMs)} ms from the induced timeout at ${iso(timing.inducedTimeoutAt)} (limit ${EVENT_LATENCY_LIMIT_MS} ms)`);
  } else fail("the waiter recorded no induced-timeout time");
  const [hold] = held;
  if (hold) {
    if (!sameKeys(hold, HELD_KEYS) || hold.v !== 1) fail(`held shape: ${JSON.stringify(hold)}`);
    if (!sameKeys(hold.holder, HOLDER_KEYS) || hold.holder.pid !== observed.holder.childPid || hold.holder.op !== hold.class) fail(`held holder: ${JSON.stringify(hold.holder)}`);
    if (!Number.isInteger(hold.heldMs) || hold.heldMs < options.holdMs * 0.9) fail(`held heldMs ${hold.heldMs} < ${options.holdMs}`);
  }
  return { timing, timeout, held: hold };
};
const iso = (ms) => new Date(ms).toISOString();
const signed = (ms) => (ms >= 0 ? `+${ms}` : String(ms));

const main = async (options) => {
  if (!fs.existsSync(path.join(options.release, "dist/mesh.js"))) usage(`${options.release}/dist/mesh.js does not exist (build first, or pass --release)`);
  const root = fs.mkdtempSync(path.join(options.dir, "lock-events-probe-"));
  const problems = [];
  const report = { release: options.release, holdMs: options.holdMs, timeoutMs: options.timeoutMs, scenarios: {} };
  try {
    const observed = {};
    for (const storeKind of ["sqlite", "mesh-lock"]) {
      const scenarioRoot = path.join(root, storeKind);
      fs.mkdirSync(scenarioRoot, { mode: 0o700 });
      observed[storeKind] = await scenario(storeKind, options, scenarioRoot, problems);
      const events = readEvents(path.join(scenarioRoot, "lock-stats", "events.jsonl"), problems);
      report.scenarios[storeKind] = {
        waiter: { code: observed[storeKind].waiter.code, busyCode: observed[storeKind].waiter.busyCode, waitedMs: observed[storeKind].waiter.waitedMs },
        ...checkEvents(storeKind, observed[storeKind], events, options, problems),
      };
    }
  } catch (error) {
    report.invalid = error instanceof Error ? error.message : String(error);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
  report.problems = problems;
  report.verdict = report.invalid ? "invalid" : problems.length ? "fail" : "pass";
  if (options.json) console.log(JSON.stringify(report, null, 2));
  else {
    console.log(`mesh lock events probe (${options.release}): ${report.verdict.toUpperCase()}`);
    for (const [storeKind, entry] of Object.entries(report.scenarios)) {
      console.log(`  ${storeKind}: waiter ${entry.waiter.busyCode ?? entry.waiter.code ?? "acquired"} after ${entry.waiter.waitedMs} ms`);
      if (entry.timing) {
        const { inducedTimeoutAt, eventAt, deltaMs, limitMs } = entry.timing;
        console.log(`    induced timeout at ${iso(inducedTimeoutAt)} (${inducedTimeoutAt}); event at ${eventAt === null ? "none" : `${iso(eventAt)} (${eventAt})`}; delta ${deltaMs === null ? "n/a" : `${signed(deltaMs)} ms`} (limit ±${limitMs} ms)`);
      }
      if (entry.timeout) console.log(`    ${JSON.stringify(entry.timeout)}`);
      if (entry.held) console.log(`    ${JSON.stringify(entry.held)}`);
    }
    if (report.invalid) console.log(`  invalid run: ${report.invalid}`);
    for (const problem of problems) console.log(`  FAIL ${problem}`);
  }
  process.exitCode = report.invalid ? 2 : problems.length ? 1 : 0;
};

const options = parseArgs(process.argv.slice(2));
if (options.worker === "seed" || options.worker === "holder" || options.worker === "waiter") await worker(options.worker, options);
else if (options.worker !== undefined) usage(`unknown worker ${options.worker}`);
else await main(options);
