#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import type { ChildProcess } from "node:child_process";
import crossSpawn from "cross-spawn";
import { observeResidentOwner } from "./launcher-owner.js";
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

interface OwnedProcess { pid: number; processStartTime: string; ppid: number; state: string; }
interface Attempt {
  spec?: ResidentLaunchSpec;
  child: ChildProcess;
  native: ResidentChildLifetime;
  stop?: Promise<void>;
  seenOwner: boolean;
  claimedOwner: boolean;
  closingInput: boolean;
  processes: Map<number, OwnedProcess>;
  stderr: string;
}
function processRows(): OwnedProcess[] {
  const rows: OwnedProcess[] = [];
  for (const name of fs.readdirSync("/proc")) {
    if (!/^\d+$/.test(name)) continue;
    try {
      const stat = fs.readFileSync(`/proc/${name}/stat`, "utf8");
      const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
      rows.push({ pid: Number(name), ppid: Number(fields[1]), state: fields[0]!, processStartTime: fields[19]! });
    } catch { /* Exited during observation. */ }
  }
  return rows;
}
function captureDescendants(attempt: Attempt): void {
  if (process.platform !== "linux") return;
  const rows = processRows();
  const selected = new Set(rows.filter((row) => attempt.processes.get(row.pid)?.processStartTime === row.processStartTime).map((row) => row.pid));
  let changed = true;
  while (changed) {
    changed = false;
    for (const row of rows) if (selected.has(row.ppid) && !selected.has(row.pid)) { selected.add(row.pid); changed = true; }
  }
  for (const row of rows) if (selected.has(row.pid)) attempt.processes.set(row.pid, row);
}
function ownedAlive(attempt: Attempt): OwnedProcess[] {
  return processRows().filter((row) => row.state !== "Z" && attempt.processes.get(row.pid)?.processStartTime === row.processStartTime);
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
    const observed = (async () => {
      if (process.platform !== "linux") return;
      captureDescendants(attempt);
      const descendantsAlive = () => ownedAlive(attempt).filter(row => row.pid !== attempt.child.pid);
      for (const signal of ["SIGTERM", "SIGKILL"] as const) {
        for (const row of descendantsAlive().reverse()) {
          if (processStartTime(row.pid) !== row.processStartTime) throw new Error("Owned successor birth became uncertain");
          try { process.kill(row.pid, signal); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
        }
        const deadline = Date.now() + 5_000;
        while (descendantsAlive().length && Date.now() < deadline) { captureDescendants(attempt); await delay(50); }
        if (!descendantsAlive().length) return;
      }
      throw new Error("Observed resident processes did not exit; fallback is blocked");
    })();
    const results = await Promise.allSettled([attempt.native.stop(), observed]);
    // All cleanup has settled before an error is reported to the supervisor.
    for (const result of results) if (result.status === "rejected") throw result.reason;
  })();
}

async function supervise(configPath: string): Promise<void> {
  const root = path.dirname(configPath);
  const ownerPath = path.join(root, "owner.json");
  const trace = (event: string, extra: Record<string, unknown> = {}): void => {
    try { fs.appendFileSync(path.join(root, "launcher.log"), `${JSON.stringify({ event, at: Date.now(), ...extra })}\n`); }
    catch { /* Audit failure is never permission to abandon recovery. */ }
  };
  const readOwner = (): ResidentHostOwner | undefined => readHandoverJson<ResidentHostOwner>(ownerPath);
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  trace("launcher-started", { pid: process.pid, configPath, platform: process.platform });
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
  for (const signal of ["SIGINT", "SIGTERM"] as const) process.on(signal, () => {
    stopping = true;
    if (current) void stopAttempt(current).catch((error) => writeFailure(root, error));
  });
  const start = (spec?: ResidentLaunchSpec, plan?: ResidentHandoverPlan, kind?: "target" | "fallback"): Attempt => {
    const launchConfig = spec?.config ?? config;
    const launchEntry = spec?.entry ?? entry;
    const snapshot = spec ? writeLaunchSnapshot(root, spec) : configPath;
    const runtime = spec?.runtime ?? launcher.runtime;
    const attemptInfo = plan && kind ? { id: plan.id, kind } : undefined;
    const args = ["--mode", "rpc", "--no-session", "--no-tools", "--no-extensions", "--no-skills",
      "--no-prompt-templates", "--no-context-files", "--extension", launchEntry];
    // No shell/string argv. Runtime, entry and binary were resolved in the immutable snapshot.
    const script = NODE_SCRIPT_EXTENSIONS.has(path.extname(launchConfig.piBinary).toLowerCase());
    const child = crossSpawn(script ? runtime : launchConfig.piBinary, script ? [launchConfig.piBinary, ...args] : args, {
      cwd: launchConfig.cwd, detached: process.platform !== "win32", stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env, PI_FABRIC_RESIDENT_CONFIG: snapshot,
        PI_FABRIC_RESIDENT_LAUNCHER: spec ? JSON.stringify(launcher) : "",
        PI_FABRIC_RESIDENT_SPEC_DIGEST: spec?.digest ?? "",
        PI_FABRIC_RESIDENT_ATTEMPT: attemptInfo ? JSON.stringify(attemptInfo) : "",
        // Per-attempt argument, never shared config another client may rewrite.
        PI_FABRIC_RESIDENT_LAUNCH_TOKEN: process.argv.includes("--launch-token")
          ? process.argv[process.argv.indexOf("--launch-token") + 1] ?? "" : "" },
    });
    const attempt: Attempt = { ...(spec ? { spec } : {}), child, native: watchResidentChild(child),
      seenOwner: false, claimedOwner: false, closingInput: false, processes: new Map(), stderr: "" };
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
    const log = path.join(root, plan ? `child-${plan.id}-${kind}.log` : "child-stderr.log");
    for (const stream of [child.stdout, child.stderr]) stream?.on("data", (chunk: Buffer) => {
      attempt.stderr = `${attempt.stderr}${chunk}`.slice(-4_000);
      try { fs.appendFileSync(log, chunk); } catch { /* best effort */ }
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
  try {
    while (!stopping) {
      let plan: ResidentHandoverPlan | undefined;
      let custodyFd: number | undefined;
      let inode: fs.Stats | undefined;
      while (!attempt.native.exited && !stopping) {
        observe(attempt);
        const state = readHandoverJson<ResidentHandoverState>(handoverPath(root));
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
  }
}

const configPath = parseConfigPath(process.argv);
try { await supervise(configPath); }
catch (error) { writeFailure(path.dirname(configPath), error); process.exitCode = 1; }
