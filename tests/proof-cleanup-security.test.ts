import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { afterEach, describe, expect, it } from "vitest";
// @ts-expect-error The proof helper is intentionally a small ESM utility.
import { processStartTime, signalOwnedProcess } from "../scripts/proof-process-ownership.mjs";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("installed model-route proof cleanup custody", () => {
  it("rejects a colliding scratch root before touching the foreign files", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "proof-collision-")); roots.push(root);
    const scratch = path.join(root, "scratch"); const out = path.join(root, "out");
    fs.mkdirSync(scratch); fs.writeFileSync(path.join(scratch, "foreign.txt"), "must survive");
    const result = spawnSync(process.execPath, [path.resolve("scripts/prove-model-route-live.mjs"), process.execPath, scratch, out], { encoding: "utf8" });
    expect(result.status).not.toBe(0);
    expect(fs.readFileSync(path.join(scratch, "foreign.txt"), "utf8")).toBe("must survive");
    expect(fs.existsSync(out)).toBe(false);
  });

  it.skipIf(process.platform !== "linux")("does not signal a reused PID with a stale start identity", async () => {
    const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 30000)"], { stdio: "ignore" });
    try {
      await new Promise<void>(resolve => child.once("spawn", () => resolve()));
      const start = processStartTime(child.pid!); expect(start).toBeTruthy();
      const claims = new Map([[child.pid!, "not-the-current-incarnation"]]);
      expect(signalOwnedProcess(claims, child.pid!, "SIGTERM")).toBe(false);
      expect(child.exitCode).toBeNull();
      claims.set(child.pid!, start!);
      expect(signalOwnedProcess(claims, child.pid!, "SIGTERM")).toBe(true);
      await new Promise<void>(resolve => child.once("exit", () => resolve()));
    } finally { if (child.exitCode === null) child.kill("SIGKILL"); }
  });
});
