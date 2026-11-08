import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidencyClient } from "../src/residency/client.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { mainGenerationPath, residentLaunchSpec, writeLaunchSnapshot } from "../src/residency/handover.js";
import { readResidentDrainReceipt } from "../src/residency/drain.js";
import { residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

// Keep: the committed drain failure/data-safety path cannot be reached cheaply
// in a happy-path flow. Use the real reconcile, proxy, host and durable receipt;
// only checkpoint readback fails. No fixture process or worker is launched.
afterEach(() => vi.restoreAllMocks());

describe.skipIf(process.platform !== "linux")("held release-switch operator diagnosis", () => {
  it("names exact old PID/birth recovery without signalling, retrying or starting B", async () => {
    const privateTemp = residentDrainTemp("fabric-switch-diagnosis-");
    const { root } = privateTemp;
    const [a, b] = ["A", "B"].map(name => {
      const release = path.join(root, name);
      fs.mkdirSync(path.join(release, "dist/residency"), { recursive: true });
      for (const file of ["index.js", "worker.js", "residency/pi-entry.js"]) {
        fs.writeFileSync(path.join(release, "dist", file), "// retained fixture artifact; never executed\n");
      }
      return release;
    }) as [string, string];
    const identity = { id: "session:switch-diagnosis", name: "Main", kind: "main" as const, sessionId: "switch-diagnosis" };
    const config: ResidentHostConfig = {
      format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
      residencyRoot: residentRoot(path.join(root, "mesh"), identity.id), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true, notifyOnComplete: false },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.join(a, "dist/worker.js"), fabricExtensionPath: path.join(a, "dist/index.js"),
      piBinary: process.execPath, claudeBinary: "diagnosis-missing-claude", vedaBinary: "diagnosis-missing-veda",
    };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
    participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
      runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: identity.sessionId,
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
      mainProcess: { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid)! } }]);
    const previous = residentLaunchSpec(config, path.join(a, "dist/residency/pi-entry.js"));
    const launcher = { pid: process.pid, processStartTime: processStartTime(process.pid)!, token: "diagnosis-launcher-secret",
      entry: previous.entry, runtime: previous.runtime };
    const idle = vi.fn();
    const host = new ResidentHost(previous.config, idle, undefined, { launcher, spec: previous });
    const client = new ResidencyClient({ config: { ...config, workerPath: path.join(b, "dist/worker.js"), fabricExtensionPath: path.join(b, "dist/index.js") },
      mesh, participants, hostPath: path.join(b, "dist/residency/launcher.js"),
      mainAgent: { id: identity.id, local: true } as FabricMainAgentTarget, commandTimeoutMs: 2_000 });
    const nativeKill = process.kill.bind(process);
    const signals = vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (signal !== 0) throw new Error("Unexpected process signal in diagnosis-only path");
      return nativeKill(pid, signal);
    });
    const spawn = vi.spyOn(processUtils, "spawnDetached").mockRejectedValue(new Error("Unexpected successor launch"));
    const drain = vi.spyOn(ResidentActorClient.prototype, "releaseDrain");
    try {
      await participants.start();
      writeLaunchSnapshot(config.residencyRoot, previous);
      await host.start();
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const ownerBytes = fs.readFileSync(ownerPath, "utf8");
      const owner = JSON.parse(ownerBytes) as ResidentHostOwner;
      vi.spyOn(host.actors, "releaseCheckpointReceipt").mockImplementation(() => { throw new Error("fixture checkpoint readback failed"); });
      const resume = vi.spyOn(host.actors, "resumeAfterRelease");
      const error = await client.reconcileRelease().then(() => undefined, error => error as Error);
      expect(error).toBeInstanceOf(Error);
      const message = error!.message;
      expect(message).toContain("Resident graceful exit is unsettled");
      expect(message).toContain("fixture checkpoint readback failed");
      expect(message).toContain("A remains held");
      expect(message).toContain("do not retry");
      expect(message).toContain("cannot consume another request");
      expect(message).toContain("root owner's permission");
      expect(message).toContain("recheck owner.json");
      expect(message).toContain(`expected old PID ${owner.pid}`);
      expect(message).toContain(`birth (processStartTime) ${owner.processStartTime}`);
      expect(message).toContain(`kill -TERM -- ${owner.pid}`);
      expect(message).toMatch(/wait for that exact process to exit.*reload the owning Main on the desired release/);
      expect(message).not.toContain("fabric-resident-drain --exit");
      const main = JSON.parse(fs.readFileSync(mainGenerationPath(config.residencyRoot), "utf8")) as { nonce: string };
      for (const secret of [owner.token, launcher.token, main.nonce, root]) expect(message).not.toContain(secret);
      expect(drain).toHaveBeenCalledOnce();
      const response = await drain.mock.results[0]!.value;
      expect(readResidentDrainReceipt(config.residencyRoot, response.drain!.id, response.drain)).toMatchObject({
        phase: "needs-operator", outcome: "needs-operator", admissionHeld: true, terminal: true,
        owner: { pid: owner.pid, processStartTime: owner.processStartTime },
      });
      await client.reconcileRelease();
      await expect(client.ensureHost()).rejects.toThrow(/unsettled/);
      expect(drain).toHaveBeenCalledOnce();
      expect(spawn).not.toHaveBeenCalled();
      expect(signals.mock.calls.every(([, signal]) => signal === 0)).toBe(true);
      expect(host.actors.releaseAdmissionHeld).toBe(true);
      expect(resume).not.toHaveBeenCalled();
      expect(idle).not.toHaveBeenCalled();
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(ownerBytes);
    } finally {
      await client.close();
      await host.close();
      await participants.close();
      mesh.closeState();
      privateTemp.assertNoOutsideDeletion();
    }
  }, 10_000);
});
