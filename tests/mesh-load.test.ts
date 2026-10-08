import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-load-"));
  roots.push(root);
  return root;
};
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("mesh-load", () => {
  it("drives real mesh writes and cleans synthetic presence on termination", async () => {
    const root = tempRoot();
    const script = fileURLToPath(new URL("../scripts/mesh-load.ts", import.meta.url));
    const child = spawn("bun", [script, "--root", root, "--target-writes-per-min", "1200", "--target-processes", "2", "--duration", "10", "--profile", "fleet"], { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", chunk => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", chunk => { stderr += chunk; });
    const exitCode = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", code => resolve(code));
    });
    expect(exitCode, stderr).toBe(0);
    const reports = stdout.split("\n").filter(Boolean).map(line => JSON.parse(line) as { workers: number; writesPerMin: number; busyPct: number; timeouts: number });
    expect(reports.length).toBeGreaterThan(0);
    expect(reports.some(report => report.workers >= 2)).toBe(true);
    expect(reports.at(-1)!.writesPerMin).toBeGreaterThanOrEqual(960);
    const remaining = new MeshStore(root, 64 * 1024, 100).listAll().map(entry => entry.key);
    expect(remaining.filter(key => key.includes("mesh-load-") || key.startsWith("actors/mesh-load-") || key.startsWith("topology/participants/mesh-load-"))).toEqual([]);
  }, 30_000);
});
