import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [], managers: ActorManager[] = [], engines: AgentManager[] = [];
const until = async (predicate: () => boolean) => {
  const deadline = Date.now() + 15_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("F36 recovery timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(engines.splice(0).map(engine => engine.close()));
  for (const root of roots.splice(0)) await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

describe("F36 authoritative registry survives changed soft saves", () => {
  it.each((["completion", "setter"] as const).flatMap(mode =>
    (process.platform === "win32" ? ["file"] as const : ["file", "directory"] as const).map(barrier => ({ mode, barrier }))))(
    "retries a failed $barrier barrier after $mode, then normally restores the exact definition after SIGKILL",
    async ({ mode, barrier }) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-registry-f36-")); roots.push(root);
      const child = spawn("bun", [path.resolve("tests/fixtures/atomic-registry-soft-crash.ts"), root, mode, barrier], { stdio: ["ignore", "pipe", "pipe"] });
      const exited = once(child, "exit");
      let output = "", errors = "";
      child.stdout.on("data", chunk => { output += chunk.toString(); });
      child.stderr.on("data", chunk => { errors += chunk.toString(); });
      try { await until(() => output.includes("\n") || child.exitCode !== null); }
      finally { child.kill("SIGKILL"); await exited; }
      expect(output, errors).toContain("\n");
      const produced = JSON.parse(output.trim()) as { actorId: string; instructions: string; accepted?: number; cursor?: number;
        attempts: Array<{ durable: boolean; events: string[]; failed: boolean }> };
      // Neither path requests an explicit durable save. The registry boundary
      // must promote the replacement, and retain debt when that promotion fails.
      expect(produced.attempts.every(attempt => !attempt.durable)).toBe(true);
      expect(produced.attempts.some(attempt => attempt.failed && attempt.events.includes(barrier))).toBe(true);
      const receipt = produced.attempts.at(-1)!;
      expect(receipt.failed).toBe(false);
      expect(receipt.events.slice(0, 2)).toEqual(["file", "rename"]);
      if (process.platform !== "win32") expect(receipt.events[2]).toBe("directory");
      if (mode === "completion") {
        expect(produced.cursor).toBeGreaterThan(produced.accepted!);
        const actorDir = path.join(root, "actors", produced.actorId);
        const queue = fs.readdirSync(actorDir).find(file => /^queue-.+\.json$/.test(file))!;
        expect(JSON.parse(fs.readFileSync(path.join(actorDir, queue), "utf8")).items).toMatchObject([
          { payload: { sequence: produced.accepted, text: "accepted activation B" } },
        ]);
      }
      // Normal fresh-host recovery reads the definition before the queue. No
      // ingress replay, manual salvage, graceful shutdown or prelaunch repair.
      const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
      const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
      }); engines.push(agents);
      const runs = vi.spyOn(agents, "run");
      const actors = new ActorManager("f36", { id: "session:f36", name: "main", kind: "main" }, mesh,
        { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
          actorRoot: path.join(root, "actors"), persistent: true,
          meshCursorPath: path.join(root, "actors", "mesh-cursor.json"), closeGraceMs: 50,
        }); managers.push(actors);
      expect(actors.status(produced.actorId)).toMatchObject({ name: "F36",
        instructionsDigest: createHash("sha256").update(produced.instructions).digest("hex"),
        instructionsLength: produced.instructions.length,
      });
      if (mode === "completion") {
        await until(() => actors.messages(produced.actorId).some(message => message.direction === "out" && message.source === "mesh:f36.work" && !message.error));
        await new Promise(resolve => setTimeout(resolve, 100));
        expect(runs).toHaveBeenCalledTimes(1);
        expect(runs.mock.calls[0]![0].task).toContain("accepted activation B");
        expect(runs.mock.calls[0]![0].systemPrompt).toContain(produced.instructions);
      } else {
        expect(actors.status(produced.actorId).status).toBe("idle");
        expect(runs).not.toHaveBeenCalled();
      }
    }, 30_000,
  );
});
