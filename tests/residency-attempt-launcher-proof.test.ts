import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { supervise } from "../dist/residency/launcher.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { residentLaunchSpec } from "../src/residency/handover.js";
import type { AttemptLaunchReceipt } from "../src/residency/attempt-containment.js";
import type { ResidentHostConfig, ResidentHostOwner } from "../src/residency/protocol.js";

const roots: string[] = [];
const nativeDelegatedBus = process.platform === "linux" && process.env.PI_RESCOPE_DELEGATED_BUS === "1";
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
afterEach(() => { vi.unstubAllEnvs(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-launcher-proof-")); roots.push(root); return root;
};
const readOwner = (file: string): (ResidentHostOwner & { launchAttestation?: AttemptLaunchReceipt }) | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as ResidentHostOwner & { launchAttestation?: AttemptLaunchReceipt }; }
  catch { return undefined; }
};
const waitFor = async <T>(read: () => T | undefined, deadlineMs = 20_000): Promise<T> => {
  const deadline = Date.now() + deadlineMs;
  while (Date.now() < deadline) { const value = read(); if (value !== undefined) return value; await delay(20); }
  throw new Error("deadline waiting for real ResidentHost owner publication");
};
const setup = () => {
  const root = tempRoot();
  const residentRoot = path.join(root, "resident"); fs.mkdirSync(residentRoot);
  const runtime = fs.realpathSync(process.execPath);
  const piBinary = path.join(root, "real-resident-host.mjs");
  fs.writeFileSync(piBinary, `import { runResidentHostFromConfigPath } from ${JSON.stringify(pathToFileURL(path.resolve("dist/residency/host.js")).href)};\nprocess.execPath = process.env.FABRIC_TEST_BUNDLED_EXEC;\nconst controller = new AbortController();\nprocess.on("SIGTERM", () => controller.abort());\nprocess.on("SIGINT", () => controller.abort());\nawait runResidentHostFromConfigPath(process.env.PI_FABRIC_RESIDENT_CONFIG, controller.signal);\n`);
  const config: ResidentHostConfig = {
    format: 1, rootId: `session:launcher-proof-${process.pid}`, sessionId: `launcher-proof-${process.pid}`,
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot, fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 1_000, nice: 19 },
    mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary, claudeBinary: "missing-claude", vedaBinary: "missing-veda", kernel: "typescript", pythonRuntime: "monty",
    piModels: { available: [{ provider: "fixture", id: "launcher-proof" }], aliases: {}, defaultModel: "fixture/launcher-proof" },
  };
  const configPath = path.join(residentRoot, "config.json"); fs.writeFileSync(configPath, JSON.stringify(config));
  return { root, residentRoot, configPath, config, runtime };
};
const stop = async (controller: AbortController, running: Promise<void>): Promise<void> => {
  controller.abort();
  // SIGTERM is owned by supervise; this observes its completion without asserting cleanup success.
  await running;
};

describe.skipIf(process.platform !== "linux" || !fs.existsSync("dist/residency/host.js") || !fs.existsSync("dist/residency/launcher.js"))("real ResidentHost launch-attempt proof", () => {
  it.skipIf(!nativeDelegatedBus)("publishes actual cgroup-bound launch attestation and calls containmentControl once for the exact child generation", async () => {
    const f = setup();
    const controller = new AbortController();
    vi.stubEnv("FABRIC_TEST_BUNDLED_EXEC", f.runtime);
    const ownerPath = path.join(f.residentRoot, "owner.json");
    const captured: Array<{ childPid?: number | undefined; childBirth: string; generation: string; binding: unknown; isCurrent(): boolean }> = [];
    const disposed = vi.fn();
    const running = supervise(f.configPath, { signal: controller.signal, containmentControl: options => {
      captured.push({ childPid: options.child.pid, childBirth: options.binding.child.processStartTime,
        generation: options.binding.generation, binding: options.binding, isCurrent: options.isCurrent });
      return { attemptMembership: async () => { throw new Error("not exercised"); },
        joinAttemptExit: async () => { throw new Error("not exercised"); }, pending: 0, dispose: disposed };
    } });
    try {
      const owner = await waitFor(() => readOwner(ownerPath));
      const ownerCgroup = fs.readFileSync(`/proc/${owner.pid}/cgroup`, "utf8").split("\n").find(row => row.startsWith("0::"))?.slice(3);
      expect(owner.launchAttestation).toMatchObject({ attemptId: expect.any(String), scope: ownerCgroup,
        cgroupPath: ownerCgroup, launchedInScope: true, at: expect.any(Number) });
      const receiptPath = path.join(f.residentRoot, `attempt-launch-${owner.launchAttestation!.attemptId}.json`);
      const recordedReceipt = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as AttemptLaunchReceipt;
      expect(recordedReceipt).toEqual(owner.launchAttestation);
      expect(ownerCgroup).toMatch(/^\/.*fabric-resident-attempt-/);
      expect(owner.processStartTime).toBe(processStartTime(owner.pid));
      const deadline = Date.now() + 3_000;
      while (captured.length === 0 && Date.now() < deadline) await delay(10);
      expect(captured).toHaveLength(1);
      const binding = captured[0]!;
      expect(binding.childPid).toBe(owner.pid);
      expect(binding.childBirth).toBe(owner.processStartTime);
      expect(binding.generation).toBe(owner.token);
      expect((binding.binding as { launcher: unknown }).launcher).toEqual(owner.handover?.launcher);
      expect(binding.binding).toMatchObject({
        launchAttestation: owner.launchAttestation,
        child: { pid: owner.pid, processStartTime: owner.processStartTime },
        generation: owner.token,
        launcher: { pid: process.pid, processStartTime: processStartTime(process.pid) },
      });
      expect((binding.binding as { launchAttestation: AttemptLaunchReceipt }).launchAttestation).toEqual(recordedReceipt);
      expect(binding.isCurrent()).toBe(true);
      await delay(250);
      expect(captured).toHaveLength(1);
    } finally { await stop(controller, running); }
    expect(disposed).toHaveBeenCalledOnce();
  }, 35_000);

  it("starts the real compiled ResidentHost from a coherent spec without delegated bus, with no attestation", async () => {
    const f = setup();
    vi.stubEnv("XDG_RUNTIME_DIR", "");
    vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", "");
    vi.stubEnv("FABRIC_TEST_BUNDLED_EXEC", f.runtime);
    const controller = new AbortController();
    const running = supervise(f.configPath, { signal: controller.signal });
    try {
      // Production sees a coherent immutable spec, attempts cgroup admission, then uses the documented pre-spawn
      // unavailable path; no child gets fake scope data.
      const owner = await waitFor(() => readOwner(path.join(f.residentRoot, "owner.json")));
      expect(owner.processStartTime).toBe(processStartTime(owner.pid));
      expect(owner.handover?.abi).toBe("fabric-resident-1");
      expect(owner.launchAttestation).toBeUndefined();
      const spec = residentLaunchSpec(f.config, path.resolve("dist/residency/pi-entry.js"), f.runtime);
      expect(owner.configDigest).toBe(spec.digest);
      const events = fs.readFileSync(path.join(f.residentRoot, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
      expect(events.some(event => event.event === "attempt-containment-unavailable")).toBe(true);
      expect(events.find(event => event.event === "child-spawned")).not.toHaveProperty("scope");
    } finally {
      await stop(controller, running);
    }
  }, 35_000);
});
