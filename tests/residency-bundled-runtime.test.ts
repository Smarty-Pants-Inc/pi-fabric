import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { residentLaunchSpec, writeHandoverState, type ResidentHandoverPlan } from "../src/residency/handover.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";

// Real parent/child attestation, not a process.ppid override in Vitest's process
// facade. This keeps the same production identity checks on Ubuntu and forks.
describe.skipIf(process.platform !== "linux" || !fs.existsSync("dist/worker.js"))("bundled Pi resident runtime attestation", () => {
  it.each(["cold", "target", "fallback"] as const)("attests %s with launcher runtime distinct from Pi execPath", async kind => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bundled-resident-"));
    const realRuntime = fs.realpathSync(process.execPath);
    const bundled = path.join(root, "pi"); fs.copyFileSync(realRuntime, bundled); fs.chmodSync(bundled, 0o700);
    const runtimeDirectory = path.join(root, "runtime"); fs.mkdirSync(runtimeDirectory);
    const successorRuntime = path.join(runtimeDirectory, "node"); fs.copyFileSync(realRuntime, successorRuntime); fs.chmodSync(successorRuntime, 0o700);
    const config: ResidentHostConfig = { format: 1, rootId: "session:bundled", sessionId: "bundled", cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 1_000, nice: 19 }, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: path.resolve("tests/fixtures/resident-probe-pi.mjs"), claudeBinary: "missing-claude", vedaBinary: "missing-veda",
      kernel: "typescript", pythonRuntime: "monty",
      piModels: { available: [{ provider: "fixture", id: "bundled" }], aliases: {}, defaultModel: "fixture/bundled" } };
    const runtime = kind === "cold" ? realRuntime : successorRuntime;
    const spec = residentLaunchSpec(config, path.resolve("dist/residency/pi-entry.js"), runtime);
    const launcher = { pid: process.pid, processStartTime: processStartTime(process.pid)!, token: "fixture-launcher", entry: spec.entry, runtime: realRuntime };
    fs.mkdirSync(config.residencyRoot);
    const file = path.join(config.residencyRoot, "config.json"); fs.writeFileSync(file, JSON.stringify(spec.config));
    const attempt = kind === "cold" ? undefined : { id: "bundled-transaction", kind };
    if (attempt) {
      const plan = { id: attempt.id, launcher, previous: spec, target: spec } as ResidentHandoverPlan;
      writeHandoverState(config.residencyRoot, plan, "starting");
    }
    const child = spawn(runtime, [path.resolve("tests/fixtures/bundled-resident-host.mjs"), file], {
      env: { ...process.env, FABRIC_TEST_BUNDLED_EXEC: bundled, PI_FABRIC_NODE_BINARY: realRuntime,
        PI_FABRIC_RESIDENT_LAUNCHER: JSON.stringify(launcher), PI_FABRIC_RESIDENT_SPEC_DIGEST: spec.digest,
        PI_FABRIC_RESIDENT_ATTEMPT: attempt ? JSON.stringify(attempt) : "" },
      stdio: ["ignore", "ignore", "pipe"],
    });
    let stderr = ""; child.stderr.on("data", data => { stderr += data; });
    const exited = new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("close", () => resolve()); });
    try {
      const deadline = Date.now() + 15_000;
      const ownerFile = path.join(config.residencyRoot, "owner.json");
      while (!fs.existsSync(ownerFile)) {
        if (child.exitCode !== null || child.signalCode !== null || Date.now() > deadline) throw Error(`bundled child did not attest: ${stderr}`);
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      const owner = JSON.parse(fs.readFileSync(ownerFile, "utf8"));
      expect(owner.pid).toBe(child.pid);
      expect(owner.processStartTime).toBe(processStartTime(child.pid!));
      expect(owner.configDigest).toBe(spec.digest);
      expect(owner.handover.launcher).toEqual(launcher);
      expect(realRuntime).not.toBe(bundled);
      expect(owner.attempt?.kind).toBe(attempt?.kind);
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
      await exited;
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 35_000);
});
