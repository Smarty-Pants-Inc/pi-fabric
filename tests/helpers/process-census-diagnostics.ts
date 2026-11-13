import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { TestContext } from "vitest";
import { PROCESS_CENSUS_TEST_EVENT } from "../../src/agents/transports/linux-process-census.js";
import { delegatedUserCgroupAvailable } from "../../src/agents/transports/process-transport.js";
import { findExecutable } from "../../src/agents/transports/process-utils.js";

export interface ProcessCensusDiagnostics {
  mark(phase: string, details?: Record<string, unknown>): void;
}

const flag = "PI_TEST_PROCESS_CENSUS_DIAGNOSTICS";
const attempts = new AsyncLocalStorage<object>();
let users = 0;
let previousFlag: string | undefined;
const acquire = (): (() => void) => {
  if (users++ === 0) previousFlag = process.env[flag];
  process.env[flag] = "1";
  let held = true;
  return () => {
    if (!held) return;
    held = false;
    if (--users === 0) {
      if (previousFlag === undefined) delete process.env[flag];
      else process.env[flag] = previousFlag;
      previousFlag = undefined;
    }
  };
};

const token = (value: unknown): string | undefined =>
  typeof value === "string" && /^[a-zA-Z0-9_.:/-]{1,100}$/.test(value) ? value : undefined;
const codeOf = (error: unknown): string => token((error as NodeJS.ErrnoException | undefined)?.code) ?? "UNKNOWN";
const typedError = (error: unknown): Record<string, unknown> => {
  const value = error as { name?: unknown; code?: unknown; errno?: unknown; probe?: unknown } | undefined;
  return { name: token(value?.name) ?? "UnknownError", code: codeOf(error),
    ...(token(value?.errno) ? { errno: token(value?.errno) } : {}),
    ...(typeof value?.probe === "string" ? { probe: value.probe.slice(0, 80) } : {}) };
};
const availability = (): Record<string, unknown> => {
  const candidates = [process.env.XDG_RUNTIME_DIR && path.join(process.env.XDG_RUNTIME_DIR, "bus"),
    process.env.DBUS_SESSION_BUS_ADDRESS?.match(/^unix:path=([^,;]+)/)?.[1]];
  const realBusPresent = candidates.some(candidate => {
    if (!candidate || !path.isAbsolute(candidate)) return false;
    try { return fs.statSync(candidate).isSocket(); } catch { return false; }
  });
  // Match the backend's exact wrapper exclusion, without launching systemd or probing via a subprocess.
  const wrapper = process.env.FABRIC_TASK_SYSTEMD_RUN_WRAPPER;
  const environment = wrapper ? { ...process.env, PATH: (process.env.PATH ?? "").split(path.delimiter)
    .filter(directory => path.resolve(directory, "systemd-run") !== wrapper).join(path.delimiter) } : process.env;
  let systemdRunPath: string | null = null;
  let executableError: Record<string, unknown> | undefined;
  try { systemdRunPath = findExecutable("systemd-run", environment) ?? null; }
  catch (error) { executableError = typedError(error); }
  let delegated: Record<string, unknown>;
  try { delegated = { outcome: delegatedUserCgroupAvailable() ? "available" : "unavailable" }; }
  catch (error) { delegated = { outcome: "error", error: typedError(error) }; }
  return { platform: process.platform, realBusPresent, systemdRunPath,
    ...(executableError ? { executableError } : {}), delegatedUserCgroupAvailable: delegated };
};

const kinds = ["attempt", "full-scan", "cgroup-procs-read", "cgroup-procs-read-error",
  "full-view-request", "attribution-empty", "scope-armed"] as const;
const amount = (value: unknown): number => typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : 0;
// Marks carry metrics, never arbitrary strings, nested test data, environment, or PID arrays.
const metrics = (details: Record<string, unknown> | undefined): Record<string, number | boolean> => {
  const result: Record<string, number | boolean> = Object.create(null) as Record<string, number | boolean>;
  for (const [key, value] of Object.entries(details ?? {}).slice(0, 24)) {
    if (!/^[a-zA-Z][a-zA-Z0-9_]{0,63}$/.test(key)) continue;
    if (typeof value === "boolean" || typeof value === "number" && Number.isFinite(value)) result[key] = value;
  }
  return result;
};

/** Opt-in, test-only event capture. Finish hooks, not callback finally, fence timed-out bodies from retries. */
export async function withProcessCensusDiagnostics(context: TestContext, enabled: boolean,
  run: (diagnostic: ProcessCensusDiagnostics) => Promise<void>): Promise<void> {
  if (!enabled) { await run({ mark() {} }); return; }
  const identity = {};
  const attemptID = `${process.pid}-${randomUUID()}`;
  const started = process.hrtime.bigint();
  const cpu = process.cpuUsage();
  const testName = context.task.name;
  const retryCount = context.task.result?.retryCount;
  const counts = Object.fromEntries(kinds.map(kind => [kind, 0])) as Record<string, number>;
  const byReason: Record<string, Record<string, number>> = { "full-scan": Object.create(null), "full-view-request": Object.create(null) };
  const byErrorCode: Record<string, number> = Object.create(null) as Record<string, number>;
  let entries = 0;
  let pidEntries = 0;
  let dirs = 0;
  let scopedAttempts = 0;
  let unscopedAttempts = 0;
  let lastError: Record<string, unknown> | undefined;
  let lastSource: Record<string, unknown> | undefined;
  let active = true;
  let currentPhase = "test-body";
  let phaseOpen = true;
  let bodyOutcome = "pending";
  let artifact: string | undefined;
  let artifactErrorCode: string | undefined;
  const release = acquire();
  const reportDir = process.env.PI_TEST_REPORT_DIR;
  if (reportDir) {
    try {
      const directory = path.join(reportDir, "process-census-diagnostics");
      fs.mkdirSync(directory, { recursive: true });
      const attemptDir = fs.mkdtempSync(path.join(directory, `${attemptID}-`));
      artifact = path.join(attemptDir, "events.jsonl");
    } catch (error) { artifactErrorCode = codeOf(error); }
  }
  const flush = (kind: string, extra: Record<string, unknown> = {}): void => {
    // Every line is independently parseable and immediately appended, including before a long await.
    try {
      const record = { kind, attemptID, pid: process.pid, testName, ...(retryCount === undefined ? {} : { retryCount }),
        elapsedMs: Number(process.hrtime.bigint() - started) / 1e6, cpuDelta: process.cpuUsage(cpu), currentPhase,
        counters: { counts, byReason, byErrorCode, entries, pidEntries, dirs, scopedAttempts, unscopedAttempts,
          ...(lastError ? { lastError } : {}), ...(lastSource ? { lastSource } : {}) }, ...extra };
      const line = JSON.stringify(record);
      if (artifact) {
        try { fs.appendFileSync(artifact, `${line}\n`, { encoding: "utf8", mode: 0o600 }); return; }
        catch (error) { artifactErrorCode = codeOf(error); artifact = undefined; }
      }
      try { process.stdout.write(`PROCESS_CENSUS_DIAGNOSTIC ${JSON.stringify({ ...record,
        ...(artifactErrorCode ? { artifactErrorCode } : {}) })}\n`); } catch { /* Diagnostics cannot fail a test. */ }
    } catch { /* Even unexpected diagnostic serialization failures must not replace an assertion. */ }
  };
  const listener = (event: { kind: string; [key: string]: unknown }): void => {
    if (!active || attempts.getStore() !== identity) return;
    try {
      if (!event || !kinds.includes(event.kind as typeof kinds[number])) return;
      counts[event.kind] = (counts[event.kind] ?? 0) + 1;
      if (event.kind === "attempt") {
        if (event.scoped === true) scopedAttempts++; else unscopedAttempts++;
        dirs += Array.isArray(event.dirs) ? event.dirs.length : amount(event.dirs);
      }
      if (event.kind === "full-scan") { entries += amount(event.entries); pidEntries += amount(event.pidEntries); }
      if (event.kind === "full-scan" || event.kind === "full-view-request") {
        const reason = token(event.reason) ?? "unknown";
        const bucket = byReason[event.kind]!;
        bucket[reason] = (bucket[reason] ?? 0) + 1;
      }
      if (event.kind === "cgroup-procs-read-error") {
        const code = token(event.code) ?? "UNKNOWN";
        byErrorCode[code] = (byErrorCode[code] ?? 0) + 1;
        lastError = { kind: event.kind, code };
      }
      if (event.kind === "full-scan" && token(event.code)) lastError = { kind: event.kind, code: token(event.code) };
      // Classify sources without exposing fixture paths, process identities, or raw payloads.
      lastSource = { kind: event.kind, ...(token(event.reason) ? { reason: token(event.reason) } : {}),
        ...(typeof event.procRoot === "string" ? { procRoot: event.procRoot === "/proc" ? "/proc" : "alternate" } : {}),
        ...(typeof event.cgroupDirectory === "string" ? { scoped: true } : {}) };
      if (["full-scan", "full-view-request", "cgroup-procs-read-error", "scope-armed"].includes(event.kind)) {
        flush("census-checkpoint", { eventKind: event.kind });
      }
    } catch { /* A malformed diagnostic event cannot affect runtime behavior. */ }
  };
  const diagnostic: ProcessCensusDiagnostics = { mark(phase, details) {
    if (!active || attempts.getStore() !== identity) return;
    try {
      if (phaseOpen) flush("phase-end", { bodyOutcome });
      currentPhase = phase.slice(0, 120);
      phaseOpen = true;
      flush("phase-start", { details: metrics(details) });
    } catch { /* Mark metadata must never change the test result. */ }
  } };
  const finish = (): void => {
    if (!active) return;
    active = false;
    try {
      if (phaseOpen) { flush("phase-end", { bodyOutcome }); phaseOpen = false; }
      flush("test-finished", { lastPhase: currentPhase, bodyOutcome, state: context.task.result?.state ?? "unknown" });
    } finally { process.removeListener(PROCESS_CENSUS_TEST_EVENT, listener); release(); }
  };
  // Register synchronously before entering user code: timeout completion may occur before run settles.
  try { context.onTestFinished(finish); }
  catch { release(); await run({ mark() {} }); return; }
  process.on(PROCESS_CENSUS_TEST_EVENT, listener);
  try { flush("test-start", { availability: availability() }); }
  catch (error) { flush("test-start", { availabilityError: typedError(error) }); }
  flush("phase-start");
  await attempts.run(identity, async () => {
    try { await run(diagnostic); bodyOutcome = "passed"; }
    catch (error) { bodyOutcome = "failed"; throw error; }
    finally {
      if (active) {
        if (phaseOpen) { flush("phase-end", { bodyOutcome }); phaseOpen = false; }
        flush("test-body-end", { bodyOutcome });
      }
    }
  });
}
