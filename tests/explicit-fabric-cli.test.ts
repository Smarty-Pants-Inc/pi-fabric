import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const root = process.cwd();
const hostEntry = fs.realpathSync(fileURLToPath(import.meta.resolve("@earendil-works/pi-coding-agent")));
const hostManifest = JSON.parse(fs.readFileSync(path.resolve(path.dirname(hostEntry), "../package.json"), "utf8"));
const cli = path.resolve(path.dirname(hostEntry), "..", hostManifest.bin.pi);

describe("ordinary-discovery compiled Fabric entries", () => {
  it.each(["public", "explicit aliases", "discovered aliases"])("A22 registers one candidate through %s with ordinary discovery enabled", async scenario => {
    const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-entry-cli-"));
    try {
      const agentDir = path.join(scratch, "agent"); fs.mkdirSync(agentDir);
      const installed = path.join(scratch, "installed"); fs.mkdirSync(installed);
      fs.cpSync(path.join(root, "dist"), path.join(installed, "dist"), { recursive: true });
      fs.copyFileSync(path.join(root, "package.json"), path.join(installed, "package.json"));
      fs.symlinkSync(fs.realpathSync(path.join(root, "node_modules")), path.join(installed, "node_modules"), process.platform === "win32" ? "junction" : "dir");
      const candidate = path.join(root, "dist/index.js");
      const bootstrap = path.join(root, "dist/extension-bootstrap.js");
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({
        packages: scenario === "discovered aliases" ? [] : [installed],
        extensions: scenario === "discovered aliases" ? [candidate, bootstrap] : [],
        compaction: { enabled: false }, retry: { enabled: false }, defaultProjectTrust: "never",
      }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
        mcp: { enabled: false }, mesh: { enabled: false }, memory: { enabled: false }, agents: { enabled: false },
        entropy: { enabled: false, compile: false }, ui: { enabled: false }, prewalk: { compactOnReturn: false },
      }));
      const env: NodeJS.ProcessEnv = { ...process.env, PI_CODING_AGENT_DIR: agentDir, PI_OFFLINE: "1" };
      for (const key of Object.keys(env)) if (key.startsWith("PI_FABRIC_") || key.startsWith("HERDR_")) delete env[key];
      const entries = scenario === "public" ? [candidate] : scenario === "explicit aliases" ? [candidate, bootstrap] : [];
      const run = promisify(execFile)(process.execPath, [cli, "--offline", "--mode", "json", "--no-session", "--no-context-files",
        ...entries.flatMap(entry => ["-e", entry]), "-e", path.join(root, "tests/fixtures/explicit-fabric-cli.mjs"),
        "--tools", "fabric_exec,entry_probe", "--provider", "entry-offline", "--model", "fixture", "-p", "Report Fabric entry"],
      { cwd: scratch, env, timeout: 30_000, maxBuffer: 4_000_000 });
      run.child.stdin!.end();
      const { stdout, stderr } = await run;
      expect(stderr).not.toMatch(/Failed to load extension|conflict|Duplicate tool/i);
      const events = stdout.split("\n").filter(line => line.startsWith("{")).map(line => JSON.parse(line));
      const result = events.find(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec");
      expect(result, stderr).toMatchObject({ isError: false });
      // Nested JSON tool results may escape Windows separators more than once.
      const text = JSON.stringify(result.result).replace(/\\+/g, "/");
      expect(text).toContain(candidate.replaceAll("\\", "/"));
      expect(text).not.toContain(installed.replaceAll("\\", "/"));
      expect(stdout).toContain("entry-cli-ok");
    } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
  }, 45_000);
});
