#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import type { ChildProcess } from "node:child_process";
import crossSpawn from "cross-spawn";
import { observeResidentOwner, captureDescendants, stopObservedDescendants, type OwnedProcess } from "./launcher-owner.js";
import { watchResidentChild, type ResidentChildLifetime } from "./child-lifetime.js";
import { processStartTime, residentProcessAlive } from "./process-identity.js";
import { lockFile } from "./file-lock.js";
import {
  HANDOVER_STARTUP_MS, residentLaunchSpec, validateLaunchSpec, assertHandoverTopology, assertPreviousLaunchSpec, assertAutomaticReleaseRecovery,
  handoverPath, handoverActive, handoverCustodyPath, handoverOutcomePath,
  readHandoverJson, writeHandoverImmutable, writeHandoverState, writeLaunchSnapshot,
  ownHandoverPlan, mainGenerationCurrent, exactResidentProcess, decideHandover,
  type ResidentLaunchSpec, type ResidentLauncherIdentity, type ResidentHandoverPlan, type ResidentHandoverState,
} from "./handover.js";
import type { ResidentHostConfig, ResidentHostOwner } from "./protocol.js";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { FabricHostLease } from "../topology/host-leases.js";

const NODE_SCRIPT_EXTENSIONS = new Set([".js", ".cjs", ".mjs", ".ts", ".cts", ".mts"]);
const delay = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const parseConfigPath = (argv: readonly string[]): string => {
  const index = argv.indexOf("--config");
  const value = index >= 0 ? argv[index + 1] : undefined;
  if (!value) throw new Error("Missing resident launcher argument: --config");
  return path.resolve(value);
};
const readConfig = (file: string): ResidentHostConfig => {
  const value: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("Invalid Fabric resident host config");
  const config = value as ResidentHostConfig;
  if (typeof config.cwd !== "string" || typeof config.piBinary !== "string") throw new Error("Fabric resident host config is incomplete");
  return config;
};
const writeFailure = (root: string, error: unknown): void => {
  try {
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, "error.json"), JSON.stringify({
      error: error instanceof Error ? error.message : String(error), occurredAt: Date.now(),
      launcherPid: process.pid, launcherBirth: processStartTime(process.pid),
    }, null, 2));
  } catch { /* Diagnostics can never suppress owned recovery. */ }
};

interface Attempt {
  spec?: ResidentLaunchSpec;
  child: ChildProcess;
  startedAt: number;
  logFile: string;
  firstLeaseAt?: number;
  watchdogGivenUp?: boolean;
  recovery?: { hostId: string; evidenceDir: string; restartCount: number };
  native: ResidentChildLifetime;
  stop?: Promise<void>;
  seenOwner: boolean;
  claimedOwner: boolean;
  closingInput: boolean;
  processes: Map<number, OwnedProcess>;
  stderr: string;
}
const numberSetting = (value: number | undefined, fallback: number, minimum = 0): number =>
  Number.isFinite(value) ? Math.max(minimum, value!) : fallback;

// Canonical host-leases.ts filename contract; intentionally read raw bytes rather than
// sharing peers' cached reader. The integration tests write through writeHostLease.
const ownLeasePath = (meshRoot: string, hostId: string): string =>
  path.join(meshRoot, "host-leases", createHash("sha256").update(hostId).digest("hex").slice(0, 32) + ".json");
const readOwnLease = (config: ResidentHostConfig, hostId: string): FabricHostLease | undefined => {
  try {
    const value = JSON.parse(fs.readFileSync(ownLeasePath(config.meshRoot, hostId), "utf8"));
    if (value?.format !== 1 || value.id !== hostId || value.rootId !== config.rootId || value.identityId !== hostId ||
        !Number.isFinite(value.updatedAt) || !Number.isFinite(value.expiresAt) ||
        (value.startedAt !== undefined && !Number.isFinite(value.startedAt))) return undefined;
    return value as FabricHostLease;
  } catch { return undefined; }
};

const procText = (pid: number, name: string): string | undefined => {
  try { return fs.readFileSync(`/proc/${pid}/${name}`, "utf8"); } catch { return undefined; }
};
const procStatFields = (pid: number): string[] | undefined => {
  const text = procText(pid, "stat");
  if (!text) return undefined;
  const close = text.lastIndexOf(")");
  return close < 0 ? undefined : text.slice(close + 2).trim().split(/\s+/);
};
const processCpu = (pid: number): { state?: string; userTicks?: number; systemTicks?: number } => {
  const fields = procStatFields(pid);
  if (!fields) return {};
  const user = Number(fields[11]); const system = Number(fields[12]);
  return { ...(fields[0] ? { state: fields[0] } : {}), ...(Number.isFinite(user + system) ? { userTicks: user, systemTicks: system } : {}) };
};
const tailLines = (file: string, count: number): string => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, "r");
    let offset = fs.fstatSync(fd).size;
    const chunks: Buffer[] = [];
    let bytes = 0, lines = 0;
    // Bound capture even if a pathological log contains a multi-megabyte line.
    while (offset > 0 && lines <= count && bytes < 4 * 1024 * 1024) {
      const size = Math.min(offset, 64 * 1024);
      const chunk = Buffer.allocUnsafe(size);
      offset -= size;
      const read = fs.readSync(fd, chunk, 0, size, offset);
      const data = chunk.subarray(0, read);
      for (const byte of data) if (byte === 10) lines++;
      chunks.unshift(data); bytes += read;
    }
    return Buffer.concat(chunks).toString("utf8").replace(/\r?\n$/, "").split(/\r?\n/).slice(-count).join("\n");
  } catch { return ""; }
  finally { if (fd !== undefined) fs.closeSync(fd); }
};

const waitForExit = (attempt: Attempt, ms: number): Promise<void> => new Promise(resolve => {
  const timer = setTimeout(resolve, ms);
  void attempt.native.exit.then(() => { clearTimeout(timer); resolve(); });
});


const entryCount = (directory: string): string => {
  let dir: fs.Dir | undefined;
  try {
    dir = fs.opendirSync(directory);
    let count = 0;
    while (dir.readSync()) count++;
    return String(count);
  } catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT" ? "0" : "unavailable"; }
  finally { dir?.closeSync(); }
};

async function captureWedgeEvidence(root: string, config: ResidentHostConfig, attempt: Attempt, owner: ResidentHostOwner): Promise<string> {
  const wedges = path.join(root, "wedges");
  fs.mkdirSync(wedges, { recursive: true, mode: 0o700 });
  const iso = new Date().toISOString();
  const base = process.platform === "win32" ? iso.replace(/:/g, "-") : iso;
  let dir = path.join(wedges, base);
  for (let suffix = 1; fs.existsSync(dir); suffix++) dir = path.join(wedges, `${base}-${suffix}`);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const pid = attempt.child.pid;
  if (pid) {
    for (const name of ["status", "stat", "io", "wchan", "stack"]) {
      const text = procText(pid, name);
      if (text !== undefined) fs.writeFileSync(path.join(dir, name), text);
    }
    const rows: string[] = [];
    try {
      for (const tid of fs.readdirSync(`/proc/${pid}/task`).filter(name => /^\d+$/.test(name))) {
        try { rows.push(fs.readFileSync(`/proc/${pid}/task/${tid}/stat`, "utf8").trimEnd()); } catch { /* Thread exited. */ }
      }
    } catch { /* Process may exit while collecting. */ }
    fs.writeFileSync(path.join(dir, "threads.stat"), rows.join("\n") + (rows.length ? "\n" : ""));
    fs.writeFileSync(path.join(dir, "fd-count"), entryCount(`/proc/${pid}/fd`));
  }
  const runs = path.join(root, "runs");
  fs.writeFileSync(path.join(dir, "runs-entry-count"), entryCount(runs));
  fs.writeFileSync(path.join(dir, "child-log-tail"), tailLines(attempt.logFile, 200));
  let leaseText = "unavailable";
  try {
    leaseText = fs.readFileSync(ownLeasePath(config.meshRoot, owner.hostId), "utf8");
  } catch { /* best effort evidence */ }
  fs.writeFileSync(path.join(dir, "lease.json"), leaseText);
  const keep = fs.readdirSync(wedges).filter(name => name !== "reports" && fs.statSync(path.join(wedges, name)).isDirectory()).sort();
  for (const old of keep.slice(0, Math.max(0, keep.length - 5))) fs.rmSync(path.join(wedges, old), { recursive: true, force: true });
  return dir;
}

const signalChild = (attempt: Attempt, signal: NodeJS.Signals): void => {
  if (attempt.native.exited || !attempt.child.pid) return;
  const birth = attempt.processes.get(attempt.child.pid)?.processStartTime;
  if (process.platform === "linux" && birth && processStartTime(attempt.child.pid) !== birth) {
    throw new Error("Resident child incarnation changed before watchdog signal; restart blocked");
  }
  attempt.child.kill(signal);
};

async function terminateWedgedChild(attempt: Attempt, termMs: number, killMs: number): Promise<void> {
  signalChild(attempt, "SIGTERM");
  if (!attempt.native.exited) await waitForExit(attempt, termMs);
  if (!attempt.native.exited) {
    signalChild(attempt, "SIGKILL");
    await waitForExit(attempt, killMs);
  }
  if (!attempt.native.exited) throw new Error("Wedged resident child did not provide a native exit receipt; restart blocked");
  await stopObservedDescendants(attempt, attempt.child.pid);
}

/** Best-effort stop of birth-validated, observed attempt processes only.
 * Neither sampling nor a free host fence proves complete membership/exit.
 * This cleanup must never authorize a fallback after a spawned target.
 */
function stopAttempt(attempt: Attempt): Promise<void> {
  return attempt.stop ??= (async () => {
    // Sampling is only best-effort descendant cleanup. The direct child must
    // also be stopped through its native handle, even when its birth could not
    // be observed. Exclude it from PID-based cleanup and join both operations.
    const observed = stopObservedDescendants(attempt, attempt.child.pid);
    const results = await Promise.allSettled([attempt.native.stop(), observed]);
    // All cleanup has settled before an error is reported to the supervisor.
    for (const result of results) if (result.status === "rejected") throw result.reason;
  })();
}

/** Timing injection is only for deterministic launcher tests; production uses fixed signal deadlines. */
export async function supervise(configPath: string, options: { signal?: AbortSignal; reportWaitMs?: number; termMs?: number; killWaitMs?: number } = {}): Promise<void> {
  const root = path.dirname(configPath);
  const ownerPath = path.join(root, "owner.json");
  const trace = (event: string, extra: Record<string, unknown> = {}): void => {
    try { fs.appendFileSync(path.join(root, "launcher.log"), `${JSON.stringify({ event, at: Date.now(), ...extra })}\n`); }
    catch { /* Audit failure is never permission to abandon recovery. */ }
  };
  const readOwner = (): ResidentHostOwner | undefined => readHandoverJson<ResidentHostOwner>(ownerPath);
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  trace("launcher-started", { pid: process.pid, processStartTime: processStartTime(process.pid), configPath, platform: process.platform });
  if (handoverActive(readHandoverJson<ResidentHandoverState>(handoverPath(root)))) {
    trace("launcher-deferred", { reason: "existing handover custody" }); return;
  }
  const config = readConfig(configPath);
  const entry = fileURLToPath(new URL("./pi-entry.js", import.meta.url));
  const launcher: ResidentLauncherIdentity = { pid: process.pid, processStartTime: processStartTime(process.pid) ?? "",
    token: randomUUID(), entry, runtime: fs.realpathSync(process.execPath) };
  // Minimal fake/legacy configs and non-Linux residency keep their existing
  // one-child behavior, but do not advertise an unproved handover capability.
  let initial: ResidentLaunchSpec | undefined;
  if (process.platform === "linux" && config.format === 1) {
    try { initial = residentLaunchSpec(config, entry, launcher.runtime); }
    catch (error) { trace("handover-capability-deferred", { reason: error instanceof Error ? error.message : String(error) }); }
  }
  let current: Attempt | undefined;
  let stopping = false;
  const stop = (): void => {
    stopping = true;
    if (current) void stopAttempt(current).catch((error) => writeFailure(root, error));
  };
  for (const signal of ["SIGINT", "SIGTERM"] as const) process.on(signal, stop);
  options.signal?.addEventListener("abort", stop, { once: true });
  const start = (spec?: ResidentLaunchSpec, plan?: ResidentHandoverPlan, kind?: "target" | "fallback", frozenConfigPath?: string): Attempt => {
    const launchConfig = spec?.config ?? config;
    const launchEntry = spec?.entry ?? entry;
    const snapshot = spec ? writeLaunchSnapshot(root, spec) : frozenConfigPath ?? configPath;
    const runtime = spec?.runtime ?? launcher.runtime;
    const attemptInfo = plan && kind ? { id: plan.id, kind } : undefined;
    const reportDirectory = path.join(root, "wedges", "reports");
    fs.mkdirSync(reportDirectory, { recursive: true, mode: 0o700 });
    const args = ["--mode", "rpc", "--no-session", "--no-tools", "--no-extensions", "--no-skills",
      "--no-prompt-templates", "--no-context-files", "--extension", launchEntry];
    const nodeOptions = `${process.env.NODE_OPTIONS ?? ""} --report-on-signal --report-signal=SIGUSR2 --report-exclude-env --report-directory=${JSON.stringify(reportDirectory)}`;

    // No shell/string argv. Runtime, entry and binary were resolved in the immutable snapshot.
    const script = NODE_SCRIPT_EXTENSIONS.has(path.extname(launchConfig.piBinary).toLowerCase());
    const child = crossSpawn(script ? runtime : launchConfig.piBinary, script ? [launchConfig.piBinary, ...args] : args, {
      cwd: launchConfig.cwd, detached: process.platform !== "win32", stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env, NODE_OPTIONS: nodeOptions, PI_FABRIC_RESIDENT_CONFIG: snapshot,
        PI_FABRIC_RESIDENT_LAUNCHER: spec ? JSON.stringify(launcher) : "",
        PI_FABRIC_RESIDENT_SPEC_DIGEST: spec?.digest ?? "",
        PI_FABRIC_RESIDENT_ATTEMPT: attemptInfo ? JSON.stringify(attemptInfo) : "",
        // Per-attempt argument, never shared config another client may rewrite.
        PI_FABRIC_RESIDENT_LAUNCH_TOKEN: process.argv.includes("--launch-token")
          ? process.argv[process.argv.indexOf("--launch-token") + 1] ?? "" : "" },
    });
    const logFile = path.join(root, plan ? `child-${plan.id}-${kind}.log` : "child-stderr.log");
    const attempt: Attempt = { ...(spec ? { spec } : {}), child, native: watchResidentChild(child),
      startedAt: Date.now(), logFile, seenOwner: false, claimedOwner: false, closingInput: false, processes: new Map(), stderr: "" };
    child.once("error", (error) => { trace("child-error", { message: error.message, kind }); writeFailure(root, error); });
    void attempt.native.exit.then(({ code, signal }) => {
      // #2010: after a clean owned release this directory may already belong
      // to the next generation. Do not make a late diagnostic mutation there.
      if (!attempt.seenOwner || code !== 0 || signal) trace("child-exit", { pid: child.pid, code, signal, kind, seenOwner: attempt.seenOwner });
    });
    child.once("spawn", () => {
      const birth = child.pid ? processStartTime(child.pid) : undefined;
      if (child.pid && birth) attempt.processes.set(child.pid, { pid: child.pid, processStartTime: birth, ppid: process.pid, state: "S" });
    });
    for (const stream of [child.stdout, child.stderr]) stream?.on("data", (chunk: Buffer) => {
      attempt.stderr = `${attempt.stderr}${chunk}`.slice(-4_000);
      try { fs.appendFileSync(logFile, chunk); } catch { /* best effort */ }
    });
    trace("child-spawned", { pid: child.pid, entry: launchEntry, configPath: snapshot, digest: spec?.digest, transaction: plan?.id, kind });
    current = attempt;
    return attempt;
  };
  const observe = (attempt: Attempt): void => {
    const owner = readOwner();
    const live = owner && residentProcessAlive(owner.pid, owner.processStartTime) ? owner.pid : undefined;
    const observation = observeResidentOwner(live, attempt.child.pid, attempt.claimedOwner);
    attempt.claimedOwner = observation.claimed; attempt.seenOwner ||= observation.observedOwner;
    if (observation.closeInput && !attempt.closingInput) { attempt.closingInput = true; attempt.child.stdin?.end(); }
  };
  const assertFenceFree = async (inode: fs.Stats): Promise<void> => {
    const lock = path.join(root, "host.lock");
    const stat = fs.statSync(lock);
    if (stat.ino !== inode.ino || stat.dev !== inode.dev) throw new Error("Resident kernel fence inode changed");
    const fd = await lockFile(lock, 0, true);
    try {
      const held = fs.fstatSync(fd);
      if (held.ino !== inode.ino || held.dev !== inode.dev) throw new Error("Resident kernel fence inode changed during acquisition");
      const owner = readOwner();
      if (owner && residentProcessAlive(owner.pid, owner.processStartTime)) throw new Error("Another live owner blocks resident fallback");
    } finally { fs.closeSync(fd); }
  };
  const ready = async (attempt: Attempt, plan: ResidentHandoverPlan, spec: ResidentLaunchSpec, kind: "target" | "fallback"): Promise<void> => {
    const deadline = Date.now() + HANDOVER_STARTUP_MS;
    while (!attempt.native.exited && !stopping && Date.now() < deadline) {
      captureDescendants(attempt); observe(attempt);
      if (kind === "target" && !mainGenerationCurrent(root, plan.main)) throw new Error("Main generation lost before terminal release success");
      const owner = readOwner();
      if (owner && owner.pid === attempt.child.pid && exactResidentProcess(owner as { pid: number; processStartTime: string }) &&
          owner.token !== plan.old.token && owner.releaseRoot === spec.releaseRoot && owner.configDigest === spec.digest &&
          owner.attempt?.id === plan.id && owner.attempt.kind === kind && owner.handover?.launcher.token === launcher.token && owner.readyAt > 0) return;
      await delay(50);
    }
    throw new Error(attempt.stderr.trim() || `Resident ${kind} did not prove worker startup`);
  };
  let attempt = start(initial);
  const watchdogRestarts: number[] = [];
  let nextWatchdogAt = 0;
  const restartIfWedged = async (candidate: Attempt): Promise<Attempt | undefined> => {
    const launchConfig = candidate.spec?.config ?? config;
    const watchdog = launchConfig.watchdog;
    if (watchdog?.enabled === false || candidate.watchdogGivenUp || Date.now() < nextWatchdogAt || candidate.native.exited) return undefined;
    nextWatchdogAt = Date.now() + numberSetting(watchdog?.intervalMs, 30_000, 1);
    const stallMs = numberSetting(watchdog?.stallMs, 180_000, 1);
    const coldStartMs = numberSetting(watchdog?.coldStartMs, 900_000);
    const limit = Math.min(3, Math.floor(numberSetting(watchdog?.maxRestartsPerHour, 3, 1)));
    const owner = readOwner();
    if (typeof owner?.hostId !== "string" || owner.pid !== candidate.child.pid || candidate.closingInput || !candidate.child.pid ||
        !residentProcessAlive(candidate.child.pid, owner.processStartTime)) return undefined;
    const lease = readOwnLease(launchConfig, owner.hostId);
    if (!lease || lease.rootId !== launchConfig.rootId || lease.identityId !== owner.hostId) return undefined;
    if (lease.updatedAt < candidate.startedAt) return undefined;
    candidate.firstLeaseAt ??= lease.updatedAt;
    if (Date.now() < candidate.firstLeaseAt + coldStartMs) return undefined;
    const age = Math.max(0, Date.now() - lease.updatedAt);
    if (age <= stallMs) return undefined;
    const cpu = processCpu(candidate.child.pid);
    const evidenceDir = await captureWedgeEvidence(root, launchConfig, candidate, owner);
    const now = Date.now();
    while (watchdogRestarts.length && watchdogRestarts[0]! <= now - 3_600_000) watchdogRestarts.shift();
    if (watchdogRestarts.length >= limit) {
      candidate.watchdogGivenUp = true;
      trace("watchdog-giving-up", { hostId: owner.hostId, pid: candidate.child.pid, leaseAgeMs: age, cpu, evidenceDir, restartCount: watchdogRestarts.length });
      return undefined;
    }
    const restartCount = watchdogRestarts.length + 1;
    const frozenConfigPath = path.join(root, `watchdog-config-${launcher.token}.json`);
    if (!candidate.spec) writeHandoverImmutable(frozenConfigPath, config);
    trace("watchdog-restart", { hostId: owner.hostId, pid: candidate.child.pid, leaseAgeMs: age, cpu, evidenceDir, restartCount });
    if (!candidate.native.exited && process.platform !== "win32") {
      signalChild(candidate, "SIGUSR2");
      await delay(options.reportWaitMs ?? 5_000);
      const reports = path.join(root, "wedges", "reports");
      for (const name of fs.readdirSync(reports)) {
        // Node's default report.<date>.<time>.<pid>.<sequence>.json naming.
        if (!name.includes(`.${candidate.child.pid}.`) || !name.endsWith(".json")) continue;
        try { fs.renameSync(path.join(reports, name), path.join(evidenceDir, name)); } catch { /* The report may still be writing. */ }
      }
    }

    if (stopping) return undefined;
    captureDescendants(candidate);
    await terminateWedgedChild(candidate, options.termMs ?? 30_000, options.killWaitMs ?? 5_000);
    if (stopping) return undefined;
    const restarted = start(candidate.spec, undefined, undefined, frozenConfigPath);
    watchdogRestarts.push(restarted.startedAt);
    restarted.recovery = { hostId: owner.hostId, evidenceDir, restartCount };
    return restarted;
  };
  try {
    while (!stopping) {
      let plan: ResidentHandoverPlan | undefined;
      let custodyFd: number | undefined;
      let inode: fs.Stats | undefined;
      while (!attempt.native.exited && !stopping) {
        observe(attempt);
        const state = readHandoverJson<ResidentHandoverState>(handoverPath(root));
        if (attempt.recovery) {
          const owner = readOwner();
          const launchConfig = attempt.spec?.config ?? config;
          const lease = readOwnLease(launchConfig, attempt.recovery.hostId);
          if (owner?.pid === attempt.child.pid && lease && lease.updatedAt >= attempt.startedAt) {
            writeJsonAtomic(path.join(root, "wedges", "latest.json"), {
              topic: `fleet.residency.${attempt.recovery.hostId}`, kind: "wedge-recovered", ...attempt.recovery, createdAt: Date.now(),
            });
            delete attempt.recovery;
          }
        }
        if (!plan && !handoverActive(state)) {
          const restarted = await restartIfWedged(attempt);
          if (restarted) { attempt = restarted; continue; }
        }
        if (!plan && state?.phase === "custody" && ownHandoverPlan(state.plan, readOwner(), launcher, attempt.child.pid)) {
          try {
            custodyFd = await lockFile(path.join(root, "handover.lock"), 0, true);
            // Re-read under the root transaction lock; a loser cannot consume a
            // different host generation's request, even with the same root.
            const exact = readHandoverJson<ResidentHandoverState>(handoverPath(root));
            if (exact?.phase !== "custody" || JSON.stringify(exact.plan) !== JSON.stringify(state.plan) ||
                !ownHandoverPlan(exact.plan, readOwner(), launcher, attempt.child.pid) || !mainGenerationCurrent(root, exact.plan.main)) {
              throw new Error("Resident custody intent changed before commitment");
            }
            if (!attempt.spec) throw new Error("Resident child has no coherent loaded launch snapshot");
            assertPreviousLaunchSpec(attempt.spec, exact.plan.previous);
            validateLaunchSpec(exact.plan.previous); validateLaunchSpec(exact.plan.target);
            assertHandoverTopology(exact.plan.previous, exact.plan.target);
            if (exact.plan.previous.config.residencyRoot !== root || exact.plan.target.config.residencyRoot !== root) throw new Error("Resident custody root mismatch");
            writeLaunchSnapshot(root, exact.plan.previous); writeLaunchSnapshot(root, exact.plan.target);
            inode = fs.statSync(path.join(root, "host.lock"));
            // No attempt-owned membership/complete exit receipts exist yet.
            // Reject while A still owns its fence, never after spawning B.
            try { assertAutomaticReleaseRecovery(); }
            catch (error) {
              if (decideHandover(root, { id: exact.plan.id, state: "cancelled" }).state === "cancelled") {
                writeHandoverState(root, exact.plan, "cancelled", error instanceof Error ? error.message : String(error));
              }
              throw error;
            }
            // Pin before the cancellation/custody CAS. A cancelled prepare
            // cannot later be consumed by a slow competing commit.
            plan = structuredClone(exact.plan);
            if (!mainGenerationCurrent(root, plan.main) ||
                decideHandover(root, { id: plan.id, state: "custody" }).state !== "custody") {
              plan = undefined; throw new Error("Resident prepare cancelled before custody");
            }
            // Pin in memory BEFORE the durable positive receipt: audit/file errors
            // after this point cannot erase the already-owned recovery obligation.
            writeHandoverImmutable(handoverOutcomePath(root, plan.target), { id: plan.id, attemptedAt: Date.now(), target: plan.target.releaseRoot });
            writeHandoverImmutable(handoverCustodyPath(root, plan.id), { id: plan.id, launcher });
            trace("handover-custody", { transaction: plan.id, old: plan.previous.releaseRoot, new: plan.target.releaseRoot, childPid: attempt.child.pid });
          } catch (error) {
            if (!plan && custodyFd !== undefined) { fs.closeSync(custodyFd); custodyFd = undefined; }
            trace("handover-deferred", { reason: error instanceof Error ? error.message : String(error) });
          }
        }
        await delay(50);
      }
      const exit = await attempt.native.exit;
      if (stopping) { if (custodyFd !== undefined) fs.closeSync(custodyFd); return; }
      if (!plan || !inode) {
        if (!attempt.seenOwner) writeFailure(root, attempt.stderr.trim() || `Pi resident host exited (${exit.signal ?? exit.code ?? "unknown"})`);
        process.exitCode = exit.code ?? 1;
        return;
      }
      let fallbackPublicationAttempted = false;
      try {
        await assertFenceFree(inode);
        // Native failure-proof seam: an inert, bounded pause with the launcher
        // already in custody and A gone. It does not authorize any extra attempt.
        const proofDelay = Number(process.env.PI_FABRIC_TEST_HANDOVER_AFTER_RELEASE_MS);
        if (Number.isInteger(proofDelay) && proofDelay > 0 && proofDelay <= 10_000) await delay(proofDelay);
        let failure: unknown;
        let terminalPublicationAttempted = false;
        let targetAttempted = false;
        if (exit.code === 0 && !exit.signal && mainGenerationCurrent(root, plan.main)) {
          try {
            validateLaunchSpec(plan.target);
            writeHandoverState(root, plan, "starting");
            targetAttempted = true;
            attempt = start(plan.target, plan, "target");
            await ready(attempt, plan, plan.target, "target");
            if (!mainGenerationCurrent(root, plan.main)) throw new Error("Main lost at terminal release boundary");
            terminalPublicationAttempted = true;
            writeHandoverState(root, plan, "complete");
            trace("handover-complete", { transaction: plan.id, pid: attempt.child.pid, release: plan.target.releaseRoot });
            if (custodyFd !== undefined) fs.closeSync(custodyFd);
            continue;
          } catch (error) {
            if (terminalPublicationAttempted) {
              // A durable write can fail after its rename became observable. B
              // may already be serving business work: never cut it to replay A.
              // Retain this owned generation and report uncertainty instead.
              trace("handover-terminal-uncertain", { transaction: plan.id, pid: attempt.child.pid,
                reason: error instanceof Error ? error.message : String(error) });
              if (custodyFd !== undefined) { fs.closeSync(custodyFd); custodyFd = undefined; }
              // Storage may be unreadable too. Keep the native handle without
              // consulting or rewriting transaction files until it exits.
              while (!attempt.native.exited && !stopping) { captureDescendants(attempt); await delay(50); }
              await attempt.native.exit; process.exitCode = 1; return;
            }
            failure = error;
            if (targetAttempted) {
              await stopAttempt(attempt);
              // Sampled ancestry cannot prove membership after reparenting.
              // Scope cut: after attempting B, cleanup never authorizes A
              // fallback. Containment/exit receipts are a separate change.
              throw new Error(`Resident target membership/exit is unproven; fallback blocked: ${failure instanceof Error ? failure.message : String(failure)}`);
            }
          }
        } else failure = new Error("Main lost or A did not release cleanly; supervised A recovery");
        if (stopping) { if (custodyFd !== undefined) fs.closeSync(custodyFd); return; }
        // Exactly one coherent A fallback. Never read config.json here.
        validateLaunchSpec(plan.previous);
        attempt = start(plan.previous, plan, "fallback");
        await ready(attempt, plan, plan.previous, "fallback");
        fallbackPublicationAttempted = true;
        writeHandoverState(root, plan, "fallback", failure instanceof Error ? failure.message : String(failure));
        trace("handover-fallback", { transaction: plan.id, pid: attempt.child.pid, release: plan.previous.releaseRoot });
      } catch (error) {
        if (fallbackPublicationAttempted) {
          // The fallback rename may already have released A's business gate.
          // Mirror target terminal uncertainty: keep the owned handle and do
          // not overwrite an exposed terminal state with blocked or replay it.
          trace("handover-terminal-uncertain", { transaction: plan.id, kind: "fallback", pid: attempt.child.pid,
            reason: error instanceof Error ? error.message : String(error) });
          if (custodyFd !== undefined) { fs.closeSync(custodyFd); custodyFd = undefined; }
          while (!attempt.native.exited && !stopping) { captureDescendants(attempt); await delay(50); }
          await attempt.native.exit; process.exitCode = 1; return;
        }
        // Unknown fence/termination and failed fallback are explicit blocked
        // outcomes. No loop, and no fabricated service success.
        await stopAttempt(attempt).catch(() => undefined);
        try { writeHandoverState(root, plan, "blocked", error instanceof Error ? error.message : String(error)); } catch { /* custody retained */ }
        writeFailure(root, error); process.exitCode = 1; return;
      } finally { if (custodyFd !== undefined) { try { fs.closeSync(custodyFd); } catch { /* closed on success */ } } }
    }
  } finally {
    // A native exit may precede completion of the already-started observed
    // cleanup. Always join it; never abandon a concurrent shutdown pass.
    if (stopping && current) await stopAttempt(current);
    for (const signal of ["SIGINT", "SIGTERM"] as const) process.off(signal, stop);
    options.signal?.removeEventListener("abort", stop);
  }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
if (isMain) {
  const configPath = parseConfigPath(process.argv);
  try { await supervise(configPath); }
  catch (error) { writeFailure(path.dirname(configPath), error); process.exitCode = 1; }
}
