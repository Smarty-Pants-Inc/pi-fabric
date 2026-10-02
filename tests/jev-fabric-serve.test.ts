import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);
const nativeBackend = (() => { try { return require("jev-fabric").binaryPath() as string | undefined; } catch { return undefined; } })();
const hostEntry = fs.realpathSync(fileURLToPath(import.meta.resolve("@earendil-works/pi-coding-agent")));
const hostPackage = JSON.parse(fs.readFileSync(path.resolve(path.dirname(hostEntry), "../package.json"), "utf8"));
const cli = path.resolve(path.dirname(hostEntry), "..", hostPackage.bin.pi);
const fixture = fileURLToPath(new URL("./fixtures/jev-stdin-pi-host.ts", import.meta.url));

// SIGSTOP makes a genuinely pending write deterministic. The native backend
// is not replaced by the JSON protocol fixture. Only the launcher records PID.
describe.skipIf(process.platform === "win32" || !nativeBackend)("native Jev stdin isolation in the real Pi host", () => {
  it.each(["write", "shutdown"])("SEC-9 survives backend exit with a pending write during %s and completes unrelated work", async mode => {
    const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stdin-host-")));
    const extension = path.join(root, "extension.mjs");
    const binary = path.join(root, "backend");
    const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
    fs.writeFileSync(binary, `#!/bin/sh\necho $$ > "$JEV_FABRIC_HOME/backend.pid"\nexec ${quote(nativeBackend!)} "$@"\n`, { mode: 0o755 });
    fs.mkdirSync(path.join(root, "profile"));
    fs.writeFileSync(path.join(root, "profile", "settings.json"), JSON.stringify({ defaultProjectTrust: "never", packages: [], extensions: [], enableInstallTelemetry: false }));
    await build({ entryPoints: [fixture], outfile: extension, bundle: true, platform: "node", format: "esm", packages: "external" });
    const child = spawn(process.execPath, [cli, "--mode", "rpc", "--no-session", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "-e", extension], {
      cwd: root, env: { ...process.env, PI_CODING_AGENT_DIR: path.join(root, "profile"), PI_OFFLINE: "1", SEC9_ROOT: root, SEC9_BINARY: binary, SEC9_MODE: mode }, stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "", stderr = "";
    child.stdout.on("data", chunk => { stdout += chunk.toString(); });
    child.stderr.on("data", chunk => { stderr += chunk.toString(); });
    child.stdin.on("error", () => {}); // Test RPC input, never the backend pipe.
    const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
    const command = async (type: string, fields: Record<string, unknown> = {}) => {
      child.stdin.write(JSON.stringify({ id: type, type, ...fields }) + "\n");
      const deadline = Date.now() + 15_000;
      while (Date.now() < deadline) {
        const response = stdout.split("\n").filter(Boolean).map(line => { try { return JSON.parse(line); } catch { return undefined; } }).find(value => value?.type === "response" && value.id === type);
        if (response) { expect(response, stderr).toMatchObject({ success: true }); return response; }
        if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Pi host died: ${JSON.stringify(await exit)}\n${stderr}`);
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      throw new Error(`Pi RPC ${type} timed out\n${stderr}`);
    };
    const pids = () => ["broken", "healthy"].flatMap(name => {
      const file = path.join(root, name, "backend.pid");
      return fs.existsSync(file) ? [Number(fs.readFileSync(file, "utf8"))] : [];
    });
    try {
      await command("prompt", { message: "/sec9-regression" });
      // A post-failure request must still be handled by the same real Pi PID.
      await command("get_state");
      const result = JSON.parse(fs.readFileSync(path.join(root, "result.json"), "utf8"));
      expect(result, JSON.stringify(result) + stderr).not.toHaveProperty("error");
      expect(result).toMatchObject({ hostPid: child.pid, backendExited: true,
        failures: [{ typed: true, name: "JevFabricServeError" }, { typed: true, name: "JevFabricServeError" }],
        futureFailure: { typed: true }, unrelatedRead: { text: "unrelated-ok\n" }, unrelatedReceipt: { state: "exited", exitCode: 0 } });
      if (mode === "write") expect(result.failures[0].pipeCode).toMatch(/^(EPIPE|ECONNRESET|ERR_STREAM_DESTROYED)$/);
      for (const pid of pids()) expect(() => process.kill(pid, 0)).toThrow();
      child.stdin.end();
      expect(await exit, stderr).toEqual({ code: 0, signal: null });
    } finally {
      // Reap only our own Pi and recorded isolated native backend groups, even
      // when testing the vulnerable head where Pi terminates on EPIPE.
      child.stdin.end();
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await exit;
      for (const pid of pids()) { try { process.kill(-pid, "SIGKILL"); } catch { /* already confirmed exited */ } }
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 30_000);
});
