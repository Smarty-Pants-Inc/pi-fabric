import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { beforeAll, describe, expect, it } from "vitest";

// Real child processes over the src path: a principal (org-agent) process that dies through
// process.exit(1) must still surface a fatal fixture refusal and persist its last diverted line.
// Host detection is real: process.argv[1] points at a CLI beside an actual package manifest.
let root: string;
let driver: string;

const DRIVER = `
import { registerFabricFixture } from ${JSON.stringify(path.resolve("src/guards/fixture-mode.ts"))};
import { configureFabricDiagnostics, diagnostics } from ${JSON.stringify(path.resolve("src/core/diagnostics.ts"))};
const [mode, fakeCli] = process.argv.slice(2);
if (fakeCli) process.argv[1] = fakeCli;
if (mode === "fixture") {
  const calls = [];
  const record = name => (...args) => { calls.push(name); };
  const pi = new Proxy({}, { get: (_target, name) => record(String(name)) });
  registerFabricFixture(pi);
  process.stdout.write("REGISTERED " + JSON.stringify(calls) + "\\n");
} else if (mode === "fatal") {
  configureFabricDiagnostics("principal-session");
  for (let i = 0; i < 20; i++) diagnostics.error("src/flood.ts", "x".repeat(64 * 1024));
  diagnostics.error("src/fatal.ts", "FATAL-MARKER diverted immediately before exit");
  process.exit(1);
} else if (mode === "warning") {
  configureFabricDiagnostics("principal-session");
  diagnostics.warn("src/nonfatal.ts", "NONFATAL-MARKER private warning");
}
`;

beforeAll(async () => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-diagnostics-exit-"));
  driver = path.join(root, "driver.mjs");
  await build({ stdin: { contents: DRIVER, resolveDir: process.cwd() }, outfile: driver,
    bundle: true, packages: "external", platform: "node", format: "esm", logLevel: "silent" });
}, 30000);

const fakeHost = (version: string): string => {
  const hostRoot = fs.mkdtempSync(path.join(root, "host-"));
  const cli = path.join(hostRoot, "cli.js");
  fs.writeFileSync(cli, "");
  fs.writeFileSync(path.join(hostRoot, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version }));
  return cli;
};

const run = (mode: string, extraEnv: Record<string, string> = {}, cli?: string) => {
  const home = fs.mkdtempSync(path.join(root, "home-"));
  const agentDir = path.join(home, "agent");
  const child = spawnSync(process.execPath, [driver, mode, ...(cli ? [cli] : [])], {
    cwd: home, encoding: "utf8", timeout: 15000,
    env: { PATH: process.env.PATH, HOME: home, TMPDIR: home, PI_CODING_AGENT_DIR: agentDir,
      SMARTY_ROLE: "org-agent@test", ...extraEnv },
  });
  const file = path.join(agentDir, "fabric", "diagnostics", "principal-session.jsonl");
  return { ...child, agentDir, file };
};

const records = (file: string) => fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));

describe("principal diagnostics across process.exit", () => {
  it.each(["0.80.6", "0.85.1"])("org-agent fixture on unsupported host %s refuses on stderr and exits 1", version => {
    const result = run("fixture", { PI_FABRIC_FIXTURE: "1" }, fakeHost(version));
    expect(result.error).toBeUndefined();
    expect(result.signal).toBeNull();
    expect(result.status).toBe(1);
    expect(result.stdout).not.toContain("REGISTERED");
    expect(result.stderr).toContain("Refusing read-only fixture: requires Pi >= 0.86.0");
    expect(result.stderr).toContain(`detected ${version}`);
    expect(result.stderr).toContain("Upgrade Pi");
  });

  it("org-agent fixture on an unknown host (no manifest) also refuses visibly", () => {
    const hostRoot = fs.mkdtempSync(path.join(root, "bare-"));
    const cli = path.join(hostRoot, "cli.js");
    fs.writeFileSync(cli, "");
    const result = run("fixture", { PI_FABRIC_FIXTURE: "1" }, cli);
    expect(result.status).toBe(1);
    expect(result.stdout).not.toContain("REGISTERED");
    expect(result.stderr).toContain("Refusing read-only fixture");
  });

  it("persists the diverted diagnostic emitted immediately before process.exit(1) privately and bounded", () => {
    const result = run("fatal");
    expect(result.signal).toBeNull();
    expect(result.status).toBe(1);
    expect(result.stderr).not.toContain("FATAL-MARKER");
    expect(result.stderr).not.toContain("xxxxxxxx");
    expect(fs.existsSync(result.file)).toBe(true);
    const stat = fs.statSync(result.file);
    expect(stat.size).toBeLessThanOrEqual(1024 * 1024);
    if (process.platform !== "win32") {
      expect(stat.mode & 0o777).toBe(0o600);
      expect(fs.statSync(path.dirname(result.file)).mode & 0o077).toBe(0);
    }
    const last = records(result.file).at(-1);
    expect(last).toEqual({ at: expect.any(String), level: "error", source: "src/fatal.ts",
      message: "FATAL-MARKER diverted immediately before exit" });
  });

  it("counterexample: a nonfatal principal Main warning diverts to the log, not stderr", () => {
    const result = run("warning");
    expect(result.status).toBe(0);
    expect(result.stderr).toBe("");
    expect(records(result.file)).toEqual([{ at: expect.any(String), level: "warning",
      source: "src/nonfatal.ts", message: "NONFATAL-MARKER private warning" }]);
    if (process.platform !== "win32") expect(fs.statSync(result.file).mode & 0o777).toBe(0o600);
  });
});
