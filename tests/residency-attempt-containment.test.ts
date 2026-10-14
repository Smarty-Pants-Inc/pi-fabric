import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { lockFile } from "../src/residency/file-lock.js";
import { checkResidentSessionExit } from "../src/residency/launcher-owner.js";
import { taskScopeShim } from "../src/agents/transports/task-cgroup.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { supervise } from "../src/residency/launcher.js";
import {
  AttemptExitUnconfirmedError,
  ContainmentUnavailableError,
  attemptMembership,
  joinAttemptExit,
  launchInScope,
  retainedAttemptBoundaryCount,
} from "../src/residency/attempt-containment.js";
import { residentLaunchSpec } from "../src/residency/handover.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";

const helperPath = path.resolve("tests/fixtures/resident-attempt-helpers.mjs");
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const roots: string[] = [];

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  // The test-command preload records and no-ops every deletion call locally.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-attempt-test-"));
  roots.push(root);
  return root;
};

const residentHelperBinary = (root: string, mode: string, receiptPath: string, extra: Record<string, string> = {}): string => {
  const wrapper = path.join(root, `resident-helper-${mode}.mjs`);
  const assignments = { PI_RESCOPE_HELPER_MODE: mode, PI_RESCOPE_HELPER_RECEIPT: receiptPath, ...extra };
  const setup = Object.entries(assignments).map(([key, value]) => `process.env[${JSON.stringify(key)}]=${JSON.stringify(value)};`).join("\n");
  fs.writeFileSync(wrapper, `import fs from "node:fs";\n${setup}\nfs.writeFileSync(process.env.PI_RESCOPE_HELPER_RECEIPT + ".wrapper", JSON.stringify({pid:process.pid,args:process.argv.slice(2)}));\ntry { await import(${JSON.stringify(pathToFileURL(helperPath).href)}); } catch (error) { fs.writeFileSync(process.env.PI_RESCOPE_HELPER_RECEIPT + ".import-error", String(error?.stack ?? error)); throw error; }\n`);
  return wrapper;
};

const residentSpec = (root: string, mode: string, receiptPath: string, extra: Record<string, string> = {}) => {
  const piBinary = residentHelperBinary(root, mode, receiptPath, extra);
  const releaseRoot = path.join(root, "fixture-release");
  const dist = path.join(releaseRoot, "dist");
  const entry = path.join(dist, "residency", "pi-entry.js");
  fs.mkdirSync(path.dirname(entry), { recursive: true });
  fs.writeFileSync(entry, "export {};\n");
  fs.writeFileSync(path.join(dist, "worker.js"), "export {};\n");
  fs.writeFileSync(path.join(dist, "index.js"), "export {};\n");
  const config: ResidentHostConfig = {
    format: 1,
    rootId: `session:attempt-${process.pid}`,
    sessionId: `attempt-${process.pid}`,
    cwd: root,
    projectRoot: root,
    meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"),
    residencyRoot: root,
    fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, nice: 19 },
    mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.join(dist, "worker.js"),
    fabricExtensionPath: path.join(dist, "index.js"),
    piBinary,
    claudeBinary: "fixture-claude",
    vedaBinary: "fixture-veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  return residentLaunchSpec(config, entry, findExecutable("bun") ?? process.execPath);
};

const waitForJson = async <T>(directory: string, name: string, timeoutMs = 5_000): Promise<T> => {
  const target = path.join(directory, name);
  if (fs.existsSync(target)) return JSON.parse(fs.readFileSync(target, "utf8")) as T;
  return new Promise<T>((resolve, reject) => {
    const watcher = fs.watch(directory, (_event, filename) => {
      if (String(filename) !== name || !fs.existsSync(target)) return;
      try {
        const value = JSON.parse(fs.readFileSync(target, "utf8")) as T;
        clearTimeout(timer);
        watcher.close();
        resolve(value);
      } catch { /* a create/write event may precede complete fixture bytes */ }
    });
    watcher.once("error", error => { clearTimeout(timer); watcher.close(); reject(error); });
    const timer = setTimeout(() => { watcher.close(); reject(new Error(`deadline waiting for ${target}`)); }, timeoutMs);
  });
};

const procState = (pid: number): { state: string; startTime: string } | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    return { state: fields[0]!, startTime: fields[19]! };
  } catch { return undefined; }
};

const waitForPidExit = async (pid: number, birth: string, timeoutMs = 2_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const current = procState(pid);
    if (!current || current.startTime !== birth || current.state === "Z") return;
    await delay(10); // bounded test-side observation only; production joins are event-driven
  }
  throw new Error(`test helper ${pid} did not exit before its finite deadline`);
};

describe("resident attempt containment admission", () => {
  it.skipIf(process.platform !== "linux")("releases the real gate with a receipt and a wrapper path containing spaces", async () => {
    const root = tempRoot();
    const wrapperBin = path.join(root, "wrapper directory with spaces");
    fs.mkdirSync(wrapperBin);
    fs.writeFileSync(path.join(wrapperBin, "systemd-run"), "#!/bin/sh\nexit 0\n", { mode: 0o700 });
    const scope = fs.readFileSync("/proc/self/cgroup", "utf8").trim().slice(3);
    const receipt = { attemptId: "space-safe-attempt", scope, cgroupPath: scope, launchedInScope: true, at: Date.now() };
    const child = spawn("/bin/sh", ["-c", taskScopeShim(3, receipt.attemptId), "shim", path.join(root, "admitted"), "unit.scope",
      process.execPath, "-e", "console.log(JSON.stringify({receipt:JSON.parse(process.env.PI_FABRIC_RESIDENT_LAUNCH_ATTESTATION),wrapper:process.env.FABRIC_TASK_SYSTEMD_RUN_WRAPPER}))"],
    { stdio: ["ignore", "pipe", "pipe", "pipe"] });
    let stdout = "", stderr = "";
    child.stdout!.on("data", chunk => { stdout += chunk; });
    child.stderr!.on("data", chunk => { stderr += chunk; });
    const exited = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    (child.stdio[3] as import("node:stream").Writable).end(`release ${24 * 1024 ** 3} ${scope} ${JSON.stringify(receipt)} ${wrapperBin}\n`);
    expect(await exited, stderr).toBe(0);
    expect(JSON.parse(stdout)).toEqual({ receipt, wrapper: path.join(wrapperBin, "systemd-run") });
  });

  it("fails the scoped primitive closed without a delegated bus and never runs unscoped", async () => {
    const root = tempRoot();
    const attemptId = `no-bus-${process.pid}-${Date.now()}`;
    const marker = path.join(root, "must-not-run.json");
    vi.stubEnv("XDG_RUNTIME_DIR", "");
    vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", "");
    await expect(launchInScope(residentSpec(root, "clean", marker), attemptId)).rejects.toMatchObject({
      name: "ContainmentUnavailableError",
      reason: expect.any(String),
    });
    expect(fs.existsSync(marker)).toBe(false);
  });

  it.skipIf(process.platform !== "linux")("keeps no-coherent-spec legacy startup available without a launch attestation when the bus is scrubbed", async () => {
    const root = tempRoot();
    const receiptPath = path.join(root, "legacy-host.json");
    const piBinary = residentHelperBinary(root, "serve", receiptPath);
    vi.stubEnv("XDG_RUNTIME_DIR", "");
    vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", "");
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify({ cwd: root, rootId: `session:legacy-${process.pid}`,
      residencyRoot: root, piBinary }));
    const controller = new AbortController();
    const supervision = supervise(configPath, { signal: controller.signal });
    try {
      const receipt = await waitForJson<{ pid: number; processStartTime: string; owner: { launchAttestation?: unknown } }>(root, "legacy-host.json");
      expect(procState(receipt.pid)?.startTime).toBe(receipt.processStartTime);
      expect(receipt.owner.launchAttestation).toBeUndefined();
      const events = (): Array<Record<string, unknown>> => {
        try { return fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").filter(Boolean).map(row => JSON.parse(row)); }
        catch { return []; }
      };
      expect(events().find(row => row.event === "child-spawned")).not.toHaveProperty("scope");
    } finally {
      controller.abort();
      await supervision;
    }
  }, 15_000);

  it("does not turn an unscoped empty session and unrelated free lock into an attempt receipt", async () => {
    if (process.platform !== "linux") return;
    const root = tempRoot();
    const receiptName = "unscoped-child.json";
    const receiptPath = path.join(root, receiptName);
    const starter = spawn(process.execPath, [helperPath, "setsid-parent", receiptPath, "2500"], {
      detached: true,
      stdio: "ignore",
    });
    starter.once("error", () => undefined);
    starter.unref();
    const helper = await waitForJson<{ pid: number; parentPid: number; parentSession: number; parentStartTime: string; processStartTime: string }>(root, receiptName);
    try {
      await waitForPidExit(helper.parentPid, helper.parentStartTime);
      expect(checkResidentSessionExit(helper.parentSession)).toMatchObject({ empty: true, members: [] });

      const lockPath = path.join(root, "unrelated.lock");
      const first = await lockFile(lockPath, 0);
      fs.closeSync(first);
      const reacquired = await lockFile(lockPath, 0);
      fs.closeSync(reacquired);

      const missingAttempt = `unscoped-${process.pid}-${Date.now()}`;
      expect(() => attemptMembership(missingAttempt)).toThrow(ContainmentUnavailableError);
      await expect(joinAttemptExit(missingAttempt, Date.now() + 500)).rejects.toBeInstanceOf(ContainmentUnavailableError);
      expect(procState(helper.pid)?.startTime).toBe(helper.processStartTime);
    } finally {
      try { if (processStartTime(helper.pid) === helper.processStartTime) process.kill(helper.pid, "SIGTERM"); }
      catch { /* finite helper already exited or was reaped */ }
      await waitForPidExit(helper.pid, helper.processStartTime, 1_500);
    }
  }, 8_000);
});

const nativeDelegatedBus = process.platform === "linux" && process.env.PI_RESCOPE_DELEGATED_BUS === "1";
describe.skipIf(!nativeDelegatedBus)("native delegated resident-attempt cgroup", () => {
  it("attests a clean ResidentLaunchSpec scope and returns the kernel populated=0 receipt", async () => {
    const root = tempRoot();
    const receiptPath = path.join(root, "clean-host.json");
    const attemptId = `resident-clean-${process.pid}-${Date.now()}`;
    const launched = await launchInScope(residentSpec(root, "clean", receiptPath), attemptId);
    expect(launched).toMatchObject({ attemptId, scope: expect.stringMatching(/^\//), launchedInScope: true });
    const receipt = await joinAttemptExit(attemptId, Date.now() + 8_000);
    expect(receipt).toMatchObject({ attemptId, scope: launched.scope, populated: false });
    expect(receipt.at).toBeLessThanOrEqual(Date.now());
    const reads = vi.spyOn(fs, "readSync");
    try {
      const cached = await joinAttemptExit(attemptId, Date.now() + 8_000);
      expect(cached).toEqual(receipt);
      expect(await joinAttemptExit(attemptId, Date.now() + 8_000)).toBe(cached);
      expect(reads).not.toHaveBeenCalled();
    } finally { reads.mockRestore(); }
    await expect(joinAttemptExit(attemptId, Number.NaN)).rejects.toBeInstanceOf(AttemptExitUnconfirmedError);
    await expect(joinAttemptExit(attemptId, receipt.at - 1)).rejects.toBeInstanceOf(AttemptExitUnconfirmedError);
    await expect(launchInScope(residentSpec(root, "clean", receiptPath), attemptId)).rejects.toMatchObject({ name: "ContainmentUnavailableError", attemptStarted: false });
    expect(retainedAttemptBoundaryCount()).toBe(0);
  }, 15_000);

  it("retires native descriptors across repeated unique completed attempts while preserving each cached receipt", async () => {
    const root = tempRoot();
    const baseline = retainedAttemptBoundaryCount();
    for (let index = 0; index < 3; index++) {
      const attemptId = `resident-repeat-${process.pid}-${Date.now()}-${index}`;
      const launched = await launchInScope(residentSpec(root, "clean", path.join(root, `repeat-${index}.json`)), attemptId);
      const receipt = await joinAttemptExit(attemptId, Date.now() + 8_000);
      expect(receipt).toMatchObject({ attemptId, scope: launched.scope, populated: false });
      const reads = vi.spyOn(fs, "readSync");
      try {
        const cached = await joinAttemptExit(attemptId, Date.now() + 8_000);
        expect(cached).toEqual(receipt);
        expect(await joinAttemptExit(attemptId, Date.now() + 8_000)).toBe(cached);
        expect(reads).not.toHaveBeenCalled();
      } finally { reads.mockRestore(); }
      expect(retainedAttemptBoundaryCount()).toBe(baseline);
    }
  }, 35_000);

  it("keeps a setsid grandchild in the attempt after its parent exits and joins only at child exit", async () => {
    const root = tempRoot();
    const receiptName = "grandchild.json";
    const receiptPath = path.join(root, receiptName);
    const attemptId = `resident-grandchild-${process.pid}-${Date.now()}`;
    const launched = await launchInScope(residentSpec(root, "setsid-parent", receiptPath, { PI_RESCOPE_HELPER_HOLD_MS: "4200" }), attemptId);
    const helper = await waitForJson<{ pid: number; parentPid: number; parentStartTime: string; processStartTime: string }>(root, receiptName);
    try {
      const membership = attemptMembership(attemptId);
      expect(launched.launchedInScope).toBe(true);
      expect(membership).toMatchObject({ attemptId, scope: launched.scope, populated: true });
      expect(membership.pids).toContain(helper.pid);
      await waitForPidExit(helper.parentPid, helper.parentStartTime);
      expect(procState(helper.pid)?.startTime).toBe(helper.processStartTime);

      await expect(joinAttemptExit(attemptId, Date.now() + 120)).rejects.toMatchObject({
        name: "AttemptExitUnconfirmedError",
        attemptId,
        remainingPids: expect.arrayContaining([helper.pid]),
      });
      const waiting = joinAttemptExit(attemptId, Date.now() + 8_000);
      const early = await Promise.race([waiting.then(() => "receipt"), delay(250).then(() => "still-contained")]);
      expect(early).toBe("still-contained");
      const receipt = await waiting;
      expect(receipt).toMatchObject({ attemptId, scope: launched.scope, populated: false });
      expect(receipt.at).toBeLessThanOrEqual(Date.now());
      expect(procState(helper.pid)).toBeUndefined();
    } finally {
      try { if (processStartTime(helper.pid) === helper.processStartTime) process.kill(helper.pid, "SIGTERM"); }
      catch { /* finite helper already exited or was reaped */ }
      await waitForPidExit(helper.pid, helper.processStartTime, 1_500);
    }
  }, 15_000);

  it("keeps supported ProcessTransport descendants in the same attempt cgroup", async () => {
    const root = tempRoot();
    const receiptName = "nested-transport.json";
    const receiptPath = path.join(root, receiptName);
    const attemptId = `resident-nested-${process.pid}-${Date.now()}`;
    const launched = await launchInScope(residentSpec(root, "nested-process-transport", receiptPath,
      { PI_RESCOPE_PROCESS_TRANSPORT_URL: new URL("../src/agents/transports/process-transport.ts", import.meta.url).href }), attemptId);
    await waitForJson(root, `${receiptName}.wrapper`, 5_000);
    let receipt: { cgroup?: string; nested?: { cgroup: string; pid: number; processStartTime: string }; error?: string };
    try { receipt = await waitForJson(root, receiptName, 13_000); }
    catch (error) {
      const progressPath = `${receiptPath}.progress`;
      const importErrorPath = `${receiptPath}.import-error`;
      const progress = fs.existsSync(progressPath) ? fs.readFileSync(progressPath, "utf8")
        : fs.existsSync(importErrorPath) ? fs.readFileSync(importErrorPath, "utf8") : "no helper progress receipt";
      throw new Error(`${String(error)}; ${progress}`);
    }
    expect(receipt.error, receipt.error).toBeUndefined();
    if (!receipt.nested) throw new Error("ProcessTransport worker did not publish its cgroup receipt");
    expect(receipt.nested.cgroup).toBe(launched.scope);
    expect(procState(receipt.nested.pid)?.startTime).toBeUndefined();
    expect(await joinAttemptExit(attemptId, Date.now() + 8_000)).toMatchObject({ populated: false, scope: launched.scope });
  }, 20_000);

  it("surfaces a cgroup event-source error as typed unconfirmed exit rather than empty membership", async () => {
    const root = tempRoot();
    const receiptPath = path.join(root, "event-error.json");
    const watcher = new EventEmitter() as EventEmitter & { close(): void };
    watcher.close = () => undefined;
    const originalWatch = fs.watch;
    vi.spyOn(fs, "watch").mockImplementation(((filename: fs.PathLike, ...args: unknown[]) => {
      if (String(filename).startsWith("/proc/self/fd/")) return watcher as unknown as fs.FSWatcher;
      return (originalWatch as Function)(filename, ...args);
    }) as typeof fs.watch);
    const attemptId = `resident-event-error-${process.pid}-${Date.now()}`;
    await launchInScope(residentSpec(root, "clean", receiptPath), attemptId);
    watcher.emit("error", Object.assign(new Error("controlled cgroup event failure"), { code: "EIO" }));
    expect(() => attemptMembership(attemptId)).toThrow(AttemptExitUnconfirmedError);
  }, 15_000);
});
