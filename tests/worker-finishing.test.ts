import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const managers: AgentManager[] = [];
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const workerPath = path.resolve("dist/worker.js");
const live = (pid: number): boolean => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
  } catch { return false; }
};
const fixture = (stubborn: boolean, rootStubborn = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-finishing-"));
  roots.push(root);
  const pidFile = path.join(root, "pid");
  const release = path.join(root, "release");
  const settle = path.join(root, "settle");
  vi.stubEnv("FAKE_PI_BEHAVIOR", rootStubborn ? "finishing-root-stubborn" : stubborn ? "finishing-child-stubborn" : "finishing-child");
  vi.stubEnv("FAKE_PI_FINISHING_PID", pidFile);
  vi.stubEnv("FAKE_PI_FINISHING_RELEASE", release);
  vi.stubEnv("FAKE_PI_FINISHING_SETTLE", settle);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 5_000 }, {
    workerPath, piBinary: path.resolve("tests/fixtures/fake-pi.mjs"), runRoot: path.join(root, "runs"),
  });
  managers.push(manager);
  return { root, manager, pidFile, release, settle };
};

describe.skipIf(process.platform !== "linux" || !fs.existsSync(workerPath))("native worker final settlement and tree exit", () => {
  it("does not infer completion from text; native settlement exposes finishing until natural child exit", async () => {
    const f = fixture(false);
    const handle = await f.manager.spawn({ task: "native final turn", transport: "process" });
    let waited = false;
    let joined = false;
    const wait = f.manager.wait(handle.id).then(result => { waited = true; return result; });
    const join = f.manager.join(handle.id).then(() => { joined = true; });
    try {
      await vi.waitFor(() => expect((f.manager.status(handle.id) as AgentRunRecord).text).toBe("QUIESCENT"), { timeout: 5_000 });
      expect(f.manager.status(handle.id).status).toBe("running");
      fs.writeFileSync(f.settle, "native completion");
      await vi.waitFor(() => expect(f.manager.status(handle.id).status).toBe("finishing"), { timeout: 3_000 });
      const pid = Number(fs.readFileSync(f.pidFile, "utf8"));
      expect(live(pid)).toBe(true);
      const saved = JSON.parse(fs.readFileSync(path.join(f.manager.runDirectory(handle.id)!, "status.json"), "utf8"));
      expect(saved).toMatchObject({ status: "finishing", text: "QUIESCENT" });
      await new Promise(resolve => setTimeout(resolve, 800));
      expect(live(pid)).toBe(true);
      expect(waited).toBe(false);
      expect(joined).toBe(false);
      fs.writeFileSync(f.release, "exit naturally");
      const result = await wait;
      await join;
      expect(result).toMatchObject({ status: "completed", text: "QUIESCENT" });
      expect(live(pid)).toBe(false);
      expect(result.warnings ?? []).not.toEqual(expect.arrayContaining([expect.stringContaining("forced cleanup")]));
    } finally {
      fs.writeFileSync(f.settle, "settle");
      fs.writeFileSync(f.release, "exit");
      await Promise.all([wait, join]);
    }
  }, 20_000);

  it("counts a refusing native Pi root as an owned descendant when closeChild escalates", async () => {
    const f = fixture(true, true);
    fs.writeFileSync(f.settle, "native completion");
    const handle = await f.manager.spawn({ task: "native final then refusing Pi root", transport: "process" });
    try {
      await vi.waitFor(() => expect(f.manager.status(handle.id).status).toBe("finishing"), { timeout: 5_000 });
      const pid = Number(fs.readFileSync(f.pidFile, "utf8"));
      expect(live(pid)).toBe(true);
      const result = await f.manager.wait(handle.id);
      expect(result).toMatchObject({ status: "completed", text: "QUIESCENT" });
      expect(live(pid)).toBe(false);
      expect(result.warnings).toContain("finished with forced cleanup of 1 descendants");
    } finally { await f.manager.stop(handle.id); }
  }, 25_000);

  it("preserves a native final result beyond the inference deadline while forcibly cleaning a refusing descendant", async () => {
    const f = fixture(true);
    fs.writeFileSync(f.settle, "native completion");
    const handle = await f.manager.spawn({ task: "native final then refusing child", transport: "process" });
    try {
      await vi.waitFor(() => expect(f.manager.status(handle.id).status).toBe("finishing"), { timeout: 5_000 });
      const pid = Number(fs.readFileSync(f.pidFile, "utf8"));
      expect(live(pid)).toBe(true);
      const result = await f.manager.wait(handle.id);
      expect(result).toMatchObject({ status: "completed", text: "QUIESCENT" });
      expect(live(pid)).toBe(false);
      expect(result.warnings).toContain("finished with forced cleanup of 1 descendants");
      expect((await f.manager.wait(handle.id)).warnings).toContain("finished with forced cleanup of 1 descendants");
    } finally { fs.writeFileSync(f.release, "exit"); }
  }, 25_000);
});
