import { execFile } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

// smarty-dev#7978: the idle wake gate must catch the polling it exists for. Each case runs the real
// gate (scripts/benchmark-idle-cpu.mjs --mode script) on a tiny child under the same preload.
const run = promisify(execFile);
const gate = path.resolve("scripts/benchmark-idle-cpu.mjs");

async function check(source: string, seconds: number, allowlist: (file: string) => unknown[] = () => []) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "idle-wake-gate-"));
  const script = path.join(dir, "child.mjs");
  const out = path.join(dir, "result.json");
  fs.writeFileSync(script, source);
  fs.writeFileSync(path.join(dir, "allowlist.json"), JSON.stringify(allowlist(script)));
  const args = [gate, "--mode", "script", "--script", script, "--seconds", String(seconds), "--warmup-seconds", "0.2",
    "--allowlist", path.join(dir, "allowlist.json"), "--out", out];
  const code = await run(process.execPath, args, { env: { ...process.env, GITHUB_STEP_SUMMARY: "" } })
    .then(() => 0, (error: { code?: number }) => error.code);
  const result = JSON.parse(fs.readFileSync(out, "utf8"));
  fs.rmSync(dir, { recursive: true, force: true });
  return { code, result };
}

describe.runIf(process.platform === "linux")("idle wake gate self-test (smarty-dev#7978)", () => {
  it("reports a 100 ms setInterval at ~10 wakes/s and fails", async () => {
    const { code, result } = await check("setInterval(() => {}, 100);\n", 3);
    expect(code).toBe(1);
    expect(result.timerWakesPerSecond).toBeGreaterThan(8);
    expect(result.timerWakesPerSecond).toBeLessThan(11);
    expect(result.top[0].callsite).toMatch(/child\.mjs:1 <anonymous>$/);
  }, 30_000);

  it("passes one lease-like timer that the allowlist names", async () => {
    const { code, result } = await check("function renewLease() { setInterval(() => {}, 100); }\nrenewLease();\n", 3,
      (file) => [{ callsite: `${file} renewLease`, reason: "self-test lease deadline", issue: "smarty-dev#7978" }]);
    expect(result.top[0]).toMatchObject({ callsite: expect.stringMatching(/child\.mjs:1 renewLease$/), allowlisted: true });
    expect(result.timerWakesPerSecond).toBe(0);
    expect(code).toBe(0);
  }, 30_000);

  it("fails a re-arming setImmediate loop, which no Timeout shows", async () => {
    const { code, result } = await check("function spin() { setImmediate(spin); }\nspin();\n", 2);
    expect(result.top[0]).toMatchObject({ type: "Immediate", callsite: expect.stringMatching(/child\.mjs:1 .*\bspin\b/) });
    expect(result.timerWakesPerSecond).toBeGreaterThan(100);
    expect(result.pollers).toBeGreaterThanOrEqual(1);
    expect(code).toBe(1);
  }, 30_000);

  it("counts an fs.watchFile 1 s stat poll, which no Timeout shows, and fails", async () => {
    const { code, result } = await check("import fs from 'node:fs';\nfs.watchFile(process.argv[1], { interval: 1000 }, () => {});\n", 3);
    expect(result.top[0]).toMatchObject({ type: "StatWatcher", callsite: expect.stringMatching(/child\.mjs:2 <anonymous>$/) });
    expect(result.timerWakesPerSecond).toBeGreaterThanOrEqual(0.9);
    expect(result.statWatchers).toEqual({ watchedFiles: 1, liveResources: 1 });
    expect(code).toBe(1);
  }, 30_000);

  it("fails a 30 s setInterval: under the 1/s total, but one callsite more than once a minute", async () => {
    const { code, result } = await check("setInterval(() => {}, 30_000);\n", 65);
    expect(result.timerWakesPerSecond).toBeLessThanOrEqual(1);
    expect(result.top[0].wakes).toBe(2);
    expect(result.perCallsiteMax).toBe(1);
    expect(result.pollers).toBe(1);
    expect(code).toBe(1);
  }, 90_000);
});
