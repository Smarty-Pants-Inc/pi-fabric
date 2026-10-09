import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentRunRecord } from "../src/agents/types.js";

const managers: AgentManager[] = [];
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = (child: "natural" | "stubborn" | "none") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-finishing-"));
  roots.push(root);
  const worker = path.join(root, "worker.mjs");
  const release = path.join(root, "release-child");
  const pidFile = path.join(root, "child.pid");
  fs.writeFileSync(worker, `
import fs from "node:fs";
import { spawn } from "node:child_process";
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
${child === "none" ? "" : `
const child = spawn(process.execPath, ["-e", ${JSON.stringify(`
const fs = require("node:fs");
fs.writeFileSync(${JSON.stringify(pidFile)}, String(process.pid));
${child === "stubborn" ? 'process.on("SIGTERM", () => {});' : ""}
setInterval(() => { if (fs.existsSync(${JSON.stringify(release)})) process.exit(0); }, 10);
`)}], { stdio: "ignore" });
await new Promise(resolve => { const timer = setInterval(() => {
  if (fs.existsSync(${JSON.stringify(pidFile)})) { clearInterval(timer); resolve(); }
}, 10); });
`}
const now = Date.now();
fs.writeFileSync(args.get("status-file"), JSON.stringify({
  id: args.get("id"), name: args.get("name"), task: "probe", status: "completed", runner: "pi", transport: "process",
  cwd: args.get("cwd"), startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0,
  text: "QUIESCENT", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, exitCode: 0
}));
// Keep the birth anchor alive while the manager captures its descendant.
setTimeout(() => process.exit(0), 700);
`);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 30_000 }, {
    workerPath: worker, runRoot: path.join(root, "runs"),
  });
  managers.push(manager);
  return { root, manager, release, pidFile };
};
const live = (pid: number) => {
  try {
    if (process.platform === "linux") {
      const fields = fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1]!.split(" ");
      return !["Z", "X"].includes(fields[0]!);
    }
    process.kill(pid, 0); return true;
  } catch { return false; }
};

// Linux's birth-safe group custody lets us deterministically inspect and clean
// leaderless descendants; other platforms have separate custody contracts.
describe.skipIf(process.platform !== "linux")("process task finishing exit barrier", () => {
  it("keeps final text readable and wait/join pending until a lingering child exits naturally", async () => {
    const f = fixture("natural");
    const handle = await f.manager.spawn({ task: "final text then child", transport: "process" });
    let waited = false;
    let joined = false;
    const wait = f.manager.wait(handle.id).then(result => { waited = true; return result; });
    const join = f.manager.join(handle.id).then(() => { joined = true; });
    try {
      await vi.waitFor(() => expect((f.manager.status(handle.id) as AgentRunRecord).text).toBe("QUIESCENT"), { timeout: 5_000 });
      const pid = Number(fs.readFileSync(f.pidFile, "utf8"));
      expect(live(pid)).toBe(true);
      expect(f.manager.status(handle.id)).toMatchObject({ status: "finishing", text: "QUIESCENT" });
      expect(f.manager.listForUi().find(run => run.id === handle.id)?.status).toBe("finishing");
      await new Promise(resolve => setTimeout(resolve, 1_000));
      expect(live(pid)).toBe(true);
      expect(waited).toBe(false);
      expect(joined).toBe(false);
      expect(f.manager.status(handle.id).status).toBe("finishing");
      fs.writeFileSync(f.release, "exit");
      const result = await wait;
      await join;
      expect(result).toMatchObject({ status: "completed", text: "QUIESCENT" });
      expect(live(pid)).toBe(false);
      expect(result.warnings ?? []).not.toEqual(expect.arrayContaining([expect.stringContaining("forced cleanup")]));
      expect(f.manager.status(handle.id).status).toBe("completed");
    } finally {
      fs.writeFileSync(f.release, "exit");
      await Promise.all([wait, join]);
    }
  }, 20_000);

  it("cleans a child that never exits and records the forced descendant count", async () => {
    const f = fixture("stubborn");
    const handle = await f.manager.spawn({ task: "final text then stubborn child", transport: "process" });
    try {
      await vi.waitFor(() => expect((f.manager.status(handle.id) as AgentRunRecord).text).toBe("QUIESCENT"), { timeout: 5_000 });
      const pid = Number(fs.readFileSync(f.pidFile, "utf8"));
      expect(live(pid)).toBe(true);
      expect(f.manager.status(handle.id).status).toBe("finishing");
      const result = await f.manager.wait(handle.id);
      expect(result).toMatchObject({ status: "completed", text: "QUIESCENT" });
      expect(live(pid)).toBe(false);
      expect(result.warnings).toContain("finished with forced cleanup of 1 descendants");
      expect((await f.manager.wait(handle.id)).warnings).toContain("finished with forced cleanup of 1 descendants");
    } finally { fs.writeFileSync(f.release, "exit"); }
  }, 30_000);

  it("leaves normal completion unchanged", async () => {
    const f = fixture("none");
    const handle = await f.manager.spawn({ task: "normal", transport: "process" });
    const result = await f.manager.wait(handle.id);
    expect(result).toMatchObject({ status: "completed", text: "QUIESCENT" });
    expect(result.warnings ?? []).not.toEqual(expect.arrayContaining([expect.stringContaining("forced cleanup")]));
    await f.manager.join(handle.id);
    expect(f.manager.status(handle.id).status).toBe("completed");
  });
});
