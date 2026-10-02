import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

let root: string;
let bundle: string;
beforeAll(async () => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-background-crash-"));
  bundle = path.join(root, "probe.mjs");
  await build({ entryPoints: [process.env.MESH_TIMEOUT_BASELINE_FIXTURE ?? "tests/fixtures/mesh-background-crash.ts"], outfile: bundle, bundle: true,
    platform: "node", format: "esm", packages: "external" });
  fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), process.platform === "win32" ? "junction" : "dir");
});
afterAll(() => fs.rmSync(root, { recursive: true, force: true }));
const probe = (mode: string): Promise<{ code: number | null; output: string }> => new Promise((resolve, reject) => {
  const cwd = path.join(root, mode); fs.mkdirSync(cwd);
  const child = spawn(process.execPath, ["--unhandled-rejections=strict", bundle, cwd, mode], { env: process.env });
  let output = "";
  child.stdout.on("data", chunk => { output += chunk; });
  child.stderr.on("data", chunk => { output += chunk; });
  const timeout = setTimeout(() => child.kill("SIGKILL"), 12_000);
  child.on("error", reject);
  child.on("close", code => { clearTimeout(timeout); resolve({ code, output }); });
});
describe("mesh timeout process survival", () => {
  it.each(["compact-request", "compact-commit", "ops-hook", "directory-heartbeat", "directory-change", "actor-presence", "actor-monitor", "lifecycle-cursor", "lifecycle-once", "control-claim", "control-detached-ack", "control-cancel", "root-cursor", "resident-delivery", "foreground"])("survives %s and recovers after the holder releases", async mode => {
    const result = await probe(mode);
    if (process.env.MESH_TIMEOUT_PROBE_LOG_DIR) {
      fs.mkdirSync(process.env.MESH_TIMEOUT_PROBE_LOG_DIR, { recursive: true });
      fs.writeFileSync(path.join(process.env.MESH_TIMEOUT_PROBE_LOG_DIR, `${mode}.log`), `exit=${result.code}\n${result.output}`);
    }
    expect(result.output, result.output).toContain(`SURVIVED ${mode}`);
    expect(result.code, result.output).toBe(0);
    if (mode !== "foreground") {
      const warnings = result.output.split("\n").filter(line => line.includes("mesh lock timeout; retrying"));
      expect(warnings.length, result.output).toBeGreaterThan(0);
      const paths = warnings.map(line => line.split(": mesh lock timeout")[0]);
      expect(new Set(paths).size, result.output).toBe(paths.length);
      expect(result.output).toMatch(/held by pid [0-9]+/);
    }
  });
});
