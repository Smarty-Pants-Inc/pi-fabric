// These regressions use only APIs already present at 9296a203, so they can run red on the base.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { MeshStore } from "../src/mesh/store.js";
import { boundModelOutput } from "../src/output-budget.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-placement-test-"));
  roots.push(root);
  const short = path.join(root, "short");
  const disk = path.join(root, "disk");
  fs.mkdirSync(short);
  fs.mkdirSync(disk);
  vi.spyOn(os, "tmpdir").mockReturnValue(short);
  vi.stubEnv("PI_FABRIC_TMPDIR", disk);
  for (const key of ["PI_FABRIC_RUN_ROOT", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 1, retainRuns: false }, { sweepPath: path.resolve("dist/storage/sweep-main.js") });
  const jobs = new FabricShellJobStore();
  closers.push(() => agents.close(), () => jobs.close());
  return { root, short, disk, agents, jobs };
};
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

it("places every bulky file-root kind in PI_FABRIC_TMPDIR, not OS temp", async () => {
  const { root, short, disk, agents, jobs } = setup();
  const actors = new ActorManager("test", { id: "session:test", name: "main", kind: "main", sessionId: "test" }, new MeshStore(path.join(root, "mesh"), 64 * 1024, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: false }, agents, () => {});
  closers.push(() => actors.close());
  jobs.begin("bash", "placement");
  await boundModelOutput("x".repeat(2000), 1000);
  const names = fs.readdirSync(disk);
  for (const kind of ["runs", "actors", "budget", "shell", "output"]) expect(names.some(name => name.startsWith(`pi-fabric-${kind}-`))).toBe(true);
  expect(fs.readdirSync(short)).toEqual([]);
});

it("removes finished session shell roots on close, without shortening live-session retention", async () => {
  const { jobs } = setup();
  const job = jobs.begin("bash", "shutdown");
  job.spill();
  const log = await job.persistLog();
  await job.finish(0);
  expect(fs.existsSync(log)).toBe(true);
  await jobs.close();
  expect(fs.existsSync(path.dirname(log))).toBe(false);
});

it("reclaims an abandoned disk-root owner through the manager's existing sweep", async () => {
  const { disk, agents } = setup();
  const abandoned = fs.mkdtempSync(path.join(disk, "pi-fabric-runs-"));
  fs.writeFileSync(path.join(abandoned, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
  await agents.close();
  await vi.waitFor(() => expect(fs.existsSync(abandoned)).toBe(false), { timeout: 20_000 });
}, 30_000);
