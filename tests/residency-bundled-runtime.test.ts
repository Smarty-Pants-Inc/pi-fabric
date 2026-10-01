import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { runResidentHostFromConfigPath } from "../src/residency/host.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { residentLaunchSpec, writeHandoverState, type ResidentHandoverPlan } from "../src/residency/handover.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";

// Like bundled-binary-spawn.test.ts, simulate Pi's distinct execPath while
// retaining the real parent process and its generic script runtime identity.
describe.skipIf(process.platform !== "linux" || !fs.existsSync("dist/worker.js"))("bundled Pi resident runtime attestation", () => {
  it.each(["cold", "target", "fallback"] as const)("attests %s with launcher runtime distinct from Pi execPath", async kind => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bundled-resident-"));
    const realRuntime = fs.realpathSync(process.execPath);
    const bundled = path.join(root, "pi"); fs.copyFileSync(realRuntime, bundled); fs.chmodSync(bundled, 0o700);
    // Successors may pin a different retained generic runtime than launcher A.
    const runtimeDirectory = path.join(root, "runtime"); fs.mkdirSync(runtimeDirectory);
    const successorRuntime = path.join(runtimeDirectory, "node"); fs.copyFileSync(realRuntime, successorRuntime);
    const config: ResidentHostConfig = { format: 1, rootId: "session:bundled", sessionId: "bundled", cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, timeoutMs: 1_000 }, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: path.resolve("tests/fixtures/resident-probe-pi.mjs"), claudeBinary: "missing-claude", vedaBinary: "missing-veda",
      kernel: "typescript", pythonRuntime: "monty",
      piModels: { available: [{ provider: "fixture", id: "bundled" }], aliases: {}, defaultModel: "fixture/bundled" } };
    const spec = residentLaunchSpec(config, path.resolve("dist/residency/pi-entry.js"), kind === "cold" ? realRuntime : successorRuntime);
    const launcher = { pid: process.pid, processStartTime: processStartTime(process.pid)!, token: "fixture-launcher", entry: spec.entry, runtime: realRuntime };
    fs.mkdirSync(config.residencyRoot);
    const file = path.join(config.residencyRoot, "config.json"); fs.writeFileSync(file, JSON.stringify(spec.config));
    const attempt = kind === "cold" ? undefined : { id: "bundled-transaction", kind };
    if (attempt) {
      const plan = { id: attempt.id, launcher, previous: spec, target: spec } as ResidentHandoverPlan;
      writeHandoverState(config.residencyRoot, plan, "starting");
    }
    const exec = process.execPath;
    const ppid = Object.getOwnPropertyDescriptor(process, "ppid")!;
    const abort = new AbortController();
    let running: Promise<void> | undefined;
    let ownerWait: Promise<string> | undefined;
    try {
      Object.defineProperty(process, "ppid", { configurable: true, value: process.pid });
      process.execPath = bundled;
      vi.stubEnv("PI_FABRIC_NODE_BINARY", realRuntime);
      vi.stubEnv("PI_FABRIC_RESIDENT_LAUNCHER", JSON.stringify(launcher));
      vi.stubEnv("PI_FABRIC_RESIDENT_SPEC_DIGEST", spec.digest);
      vi.stubEnv("PI_FABRIC_RESIDENT_ATTEMPT", attempt ? JSON.stringify(attempt) : "");
      running = runResidentHostFromConfigPath(file, abort.signal);
      // A successful owner publication is the cold/staged startup receipt.
      const deadline = Date.now() + 10_000;
      ownerWait = (async () => {
        while (!abort.signal.aborted && !fs.existsSync(path.join(config.residencyRoot, "owner.json"))) {
          if (Date.now() > deadline) throw Error("resident did not publish bundled owner");
          await new Promise(resolve => setTimeout(resolve, 10));
        }
        return "owned";
      })();
      const outcome = await Promise.race([running.then(() => "exit"), ownerWait]);
      expect(outcome).toBe("owned");
      const owner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8"));
      expect(owner.configDigest).toBe(spec.digest);
      expect(owner.handover.launcher.runtime).toBe(realRuntime);
      expect(realRuntime).not.toBe(process.execPath);
      expect(owner.attempt?.kind).toBe(attempt?.kind);
    } finally {
      abort.abort(); await running?.catch(() => undefined); await ownerWait?.catch(() => undefined);
      process.execPath = exec; Object.defineProperty(process, "ppid", ppid); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 35_000);
});
