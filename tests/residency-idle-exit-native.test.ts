import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { ResidencyClient } from "../src/residency/client.js";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { readResidentIdleExit, residentIdleExitPath } from "../src/residency/idle-exit.js";
import { residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import { residentProcessAlive } from "../src/residency/process-identity.js";
import { readHostLeaseCurrent } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { launchLog, same, stopAllOwned } from "./helpers/owned-processes.js";

const launcherPath = path.resolve("dist/residency/launcher.js");
const hostPath = path.resolve("dist/residency/host.js");
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean, ms = 15_000) {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Resident intentional-exit native deadline");
    await delay(20);
  }
}
const events = (root: string): Array<{ event: string; pid?: number }> => {
  try { return fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)); }
  catch { return []; }
};

// Native fixture owns every launcher/host/helper through launch-time birth receipts.
// Linux supplies those receipts; cross-platform policy/marker checks live in the source suites.
describe.skipIf(process.platform !== "linux" || !fs.existsSync(hostPath))("compiled resident idle exit", () => {
  it("exits 0, leaves the launcher stopped, refuses autonomous relaunch, then a real create request rearms it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-idle-native-"));
    const ownership = launchLog(root);
    const piBinary = path.join(root, "fake-pi.mjs");
    fs.writeFileSync(piBinary, `import {runResidentHostFromConfigPath} from ${JSON.stringify(pathToFileURL(hostPath).href)};\nawait runResidentHostFromConfigPath(process.env.PI_FABRIC_RESIDENT_CONFIG);\n`);
    const meshRoot = path.join(root, "mesh");
    const config: ResidentHostConfig = { format: 1, rootId: "session:idle-native", sessionId: "idle-native", cwd: root,
      projectRoot: root, meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:idle-native"),
      fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, residentIdleExitMs: 2_000 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"), piBinary,
      claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" } };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    const configPath = path.join(config.residencyRoot, "config.json");
    fs.writeFileSync(configPath, JSON.stringify(config));
    const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: config.rootId, rootId: config.rootId,
      identity: { id: config.rootId, name: "Main", kind: "main", sessionId: config.sessionId } });
    const controllers: Array<{ child: ChildProcess; native: ReturnType<typeof watchResidentChild> }> = [];
    const launch = () => {
      const child = spawn(process.execPath, [launcherPath, "--config", configPath], { env: { ...process.env, ...ownership.env }, stdio: "ignore" });
      const native = watchResidentChild(child); controllers.push({ child, native }); return native;
    };
    let client: ResidencyClient | undefined;
    const originalNodeOptions = process.env.NODE_OPTIONS;
    const originalLaunchLog = process.env.PI_FABRIC_TEST_LAUNCH_LOG;
    try {
      const first = launch();
      await until(() => fs.existsSync(path.join(config.residencyRoot, "owner.json")));
      const firstOwner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")) as ResidentHostOwner;
      await until(() => first.exited);
      expect(await first.exit).toMatchObject({ code: 0, signal: null });
      expect(readResidentIdleExit(config.residencyRoot, config.rootId)).toMatchObject({ pid: firstOwner.pid, token: firstOwner.token });
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      expect(readHostLeaseCurrent(meshRoot, firstOwner.hostId)).toBeUndefined();
      expect(residentProcessAlive(firstOwner.pid, firstOwner.processStartTime)).toBe(false);
      expect(events(config.residencyRoot).filter(row => row.event === "child-spawned")).toHaveLength(1);
      expect(fs.readFileSync(path.join(config.residencyRoot, "child-stderr.log"), "utf8").match(/resident exiting: root dead, no actors, idle/g)).toHaveLength(1);
      const autonomous = launch();
      await until(() => autonomous.exited);
      expect(await autonomous.exit).toMatchObject({ code: 0, signal: null });
      expect(events(config.residencyRoot).filter(row => row.event === "child-spawned")).toHaveLength(1);
      expect(events(config.residencyRoot).some(row => row.event === "launcher-idle")).toBe(true);

      // The existing public client startup path clears the stop immediately before its owned launch.
      process.env.NODE_OPTIONS = ownership.env.NODE_OPTIONS;
      process.env.PI_FABRIC_TEST_LAUNCH_LOG = ownership.env.PI_FABRIC_TEST_LAUNCH_LOG;
      client = new ResidencyClient({ config, mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget,
        hostPath: launcherPath, startupTimeoutMs: 30_000, commandTimeoutMs: 10_000 });
      const actor = await client.createActor({ name: "after-retirement", instructions: "wait", residency: "durable", model: "fixture/visible" });
      expect(actor.name).toBe("after-retirement");
      expect(fs.existsSync(residentIdleExitPath(config.residencyRoot))).toBe(false);
      expect(events(config.residencyRoot).filter(row => row.event === "child-spawned")).toHaveLength(2);
      const secondOwner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")) as ResidentHostOwner;
      expect(secondOwner.token).not.toBe(firstOwner.token);
      expect(residentProcessAlive(secondOwner.pid, secondOwner.processStartTime)).toBe(true);
      // An owned actor retains its host even while Main remains absent.
      await delay(config.mesh.residentIdleExitMs + 200);
      expect(residentProcessAlive(secondOwner.pid, secondOwner.processStartTime)).toBe(true);
    } finally {
      if (originalNodeOptions === undefined) delete process.env.NODE_OPTIONS;
      else process.env.NODE_OPTIONS = originalNodeOptions;
      if (originalLaunchLog === undefined) delete process.env.PI_FABRIC_TEST_LAUNCH_LOG;
      else process.env.PI_FABRIC_TEST_LAUNCH_LOG = originalLaunchLog;
      await client?.close();
      for (const controller of controllers) {
        if (!controller.native.exited) controller.child.kill("SIGTERM");
        await controller.native.exit;
      }
      const live = () => ownership.owned().filter(record => same(record) && (() => {
        try { const stat = fs.readFileSync(`/proc/${record.pid}/stat`, "utf8"); return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0] !== "Z"; }
        catch { return false; }
      })());
      await stopAllOwned(live(), 2_000, 5_000);
      await until(() => live().length === 0);
      await participants.close(); mesh.closeState();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 40_000);
});
