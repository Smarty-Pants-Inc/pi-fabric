// Keep: same-session startup takeover is an inbox-privacy and process-authority boundary.
// Real-process gate (SIGKILL targets only owned fixture children); intentionally opt-in.
// Parent gate: PI_FABRIC_STARTING_TAKEOVER_REAL=1 bunx vitest run tests/starting-main-takeover.test.ts
// All evidence roots are retained in this invocation's TMPDIR; no cleanup/removal.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const enabled = process.env.PI_FABRIC_STARTING_TAKEOVER_REAL === "1";
const cases = [
  ["live", "keeps the successor pending across expiry while the incumbent receives private followUps"],
  ["death", "takes over after the owned incumbent PID is gone, delivering a retried message exactly once"],
  ["handoff", "takes over immediately after clean handoff while the predecessor PID remains live"],
  ["birth", "treats a recorded old birth as exited despite the same real PID remaining live"],
  ["slow", "continues a >30-second same-session startup after predecessor death and sender close"],
  ["unknown", "fences malformed and legacy-unknown identity before TTL, then permits transitional legacy expiry"],
] as const;

describe("real-process starting Main defensive takeover", () => {
  for (const [name, contract] of cases) {
    it.skipIf(!enabled)(contract, async () => {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), `starting-main-gate-${name}-`));
      const child = spawn(process.env.BUN_BIN ?? "bun", ["scripts/probe-starting-host-takeover.ts", "--case", name], {
        cwd: repo, env: { ...process.env, TMPDIR: tmp, TMP: tmp, TEMP: tmp },
        stdio: ["ignore", "pipe", "pipe"],
      });
      let stdout = "", stderr = "";
      child.stdout.on("data", chunk => { stdout = (stdout + String(chunk)).slice(-32_000); });
      child.stderr.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-32_000); });
      const outcome = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
        // IPC/events drive the probe. This is only its outer finite safety deadline.
        const timer = setTimeout(() => { child.kill("SIGTERM"); reject(new Error(`owned probe deadline; tmp=${tmp}\n${stderr}`)); }, 80_000);
        child.once("error", error => { clearTimeout(timer); reject(error); });
        child.once("exit", (code, signal) => { clearTimeout(timer); resolve({ code, signal }); });
      });
      expect(outcome, `root=${tmp}\nstdout=${stdout}\nstderr=${stderr}`).toEqual({ code: 0, signal: null });
      const lines = stdout.trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatchObject({ case: name, verdict: "PASS", realProcesses: true,
        payloadInspection: "recipient-calls-only" });
      expect(fs.statSync(lines[0].root).isDirectory()).toBe(true);
    }, 85_000);
  }
});
