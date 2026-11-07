import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { describe, expect, it } from "vitest";
import { watchResidentChild } from "../src/residency/child-lifetime.js";

const CLEANUP_MS = 6_000;
const sleep = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
async function until(predicate: () => boolean, ms = 5_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Slow native close fixture deadline");
    await sleep(20);
  }
}

// Windows kills on TERM rather than running POSIX signal handlers. Its native
// receipt behavior is covered in residency-child-lifetime.test.ts.
// This is deliberately a slow cooperative native child, NOT a measurement of
// ResidentHost.close(), Pi persistence, or the real session-store extension.
// Keep reason: CLOSE-OVERRUN's five-second cutoff is a recorded real failure.
describe.skipIf(process.platform === "win32")("slow orderly native resident close", () => {
  it("receives a clean native exit after cleanup longer than five seconds", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-slow-native-close-"));
    const ready = path.join(root, "ready"), started = path.join(root, "started"), complete = path.join(root, "complete");
    const child = spawn(process.execPath, ["-e", `const fs=require('node:fs');
let closing=false;
process.on('SIGTERM',()=>{
  if(closing)return;
  closing=true;
  fs.writeFileSync(${JSON.stringify(started)},'started');
  setTimeout(()=>{fs.writeFileSync(${JSON.stringify(complete)},'complete');process.exit(0);},${CLEANUP_MS});
});
fs.writeFileSync(${JSON.stringify(ready)},'ready');
setInterval(()=>{},1000);`], { stdio: "ignore" });
    const lifetime = watchResidentChild(child);
    child.on("error", () => {});
    try {
      await until(() => fs.existsSync(ready));
      const stopped = lifetime.stop();
      expect(lifetime.stop()).toBe(stopped);
      await until(() => fs.existsSync(started));
      await stopped;
      expect(lifetime.exited).toBe(true);
      expect(await lifetime.exit).toEqual({ code: 0, signal: null });
      expect(fs.readFileSync(complete, "utf8")).toBe("complete");
    } finally {
      if (!lifetime.exited) child.kill("SIGKILL");
      await lifetime.exit;
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);
});
