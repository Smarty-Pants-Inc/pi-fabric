import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import os from "node:os";
import path from "node:path";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";

const cli = path.join(path.dirname(fileURLToPath(import.meta.resolve("@earendil-works/pi-coding-agent"))), "cli.js");

// Real Pi loader, runner and RPC shell dispatcher. Only host-version metadata is simulated.
// A factory throw would be caught by Pi, discard the guard, and execute this bash request.
describe("fixture admission through the real Pi CLI", () => {
  it.each(["0.80.6", "0.85.1", "unknown", "current"])("fails closed for host shape %s", async version => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-fixture-cli-"));
    let child: ReturnType<typeof spawn> | undefined;
    let closed: Promise<{ code: number | null; signal: NodeJS.Signals | null }> | undefined;
    try {
      const profile = path.join(root, "profile");
      fs.mkdirSync(profile);
      const guard = path.join(root, "guard.mjs");
      await build({ stdin: { contents: `import { registerFabricFixture } from ${JSON.stringify(path.resolve("src/guards/fixture-mode.ts"))}; export default registerFabricFixture;`, resolveDir: process.cwd() }, outfile: guard,
        bundle: true, packages: "external", platform: "node", format: "esm", logLevel: "silent" });
      const extensions = ["-e", guard];
      if (version !== "current") {
        const hostRoot = path.join(root, "host");
        fs.mkdirSync(hostRoot);
        const fakeCli = path.join(hostRoot, "cli.js");
        fs.writeFileSync(fakeCli, "");
        if (version !== "unknown") fs.writeFileSync(path.join(hostRoot, "package.json"), JSON.stringify({ name: "@earendil-works/pi-coding-agent", version }));
        const prelude = path.join(root, "host-shape.mjs");
        fs.writeFileSync(prelude, `export default () => { process.argv[1] = ${JSON.stringify(fakeCli)}; };`);
        extensions.unshift("-e", prelude);
      }
      const marker = path.join(root, "shell-effect");
      child = spawn(process.execPath, [cli, "--mode", "rpc", "--offline", "--no-session", "--no-extensions", "--no-skills", "--no-context-files", "--no-prompt-templates", "--no-themes", ...extensions], {
        cwd: root, env: { PATH: process.env.PATH, HOME: root, TMPDIR: root, PI_CODING_AGENT_DIR: profile, PI_FABRIC_FIXTURE: "1", PI_OFFLINE: "1" }, stdio: ["pipe", "pipe", "pipe"],
      });
      let stdout = "", stderr = "";
      child.stdout!.on("data", chunk => { stdout += chunk; });
      child.stderr!.on("data", chunk => { stderr += chunk; });
      child.stdin!.on("error", () => {}); // Refused hosts can close before the request is consumed.
      closed = new Promise(resolve => child!.once("close", (code, signal) => resolve({ code, signal })));
      child.stdin!.write(JSON.stringify({ type: "bash", id: "forbidden-shell", command: `touch '${marker}'` }) + "\n");
      const deadline = Date.now() + 15000;
      while (child.exitCode === null && !stdout.includes('"command":"bash"')) {
        assert(Date.now() < deadline, "native host deadline");
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      child.stdin!.end();
      const result = await closed;
      expect(result.signal).toBeNull();
      expect(fs.existsSync(marker)).toBe(false);
      if (version === "current") {
        expect(result.code).toBe(0);
        const response = stdout.split("\n").filter(Boolean).map(line => JSON.parse(line)).find(record => record.id === "forbidden-shell");
        expect(response.success).toBe(false);
        expect(response.error).toContain("read-only fixture");
      } else {
        expect(result.code).toBe(1);
        expect(stdout).not.toContain('"command":"bash"');
        expect(stderr).toContain("Refusing read-only fixture: requires Pi >= 0.86.0");
      }
    } finally {
      if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      if (closed) await closed;
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 25000);
});
