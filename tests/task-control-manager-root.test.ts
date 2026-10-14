import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ensureTaskControlRoot, taskControlDirectory } from "../src/worker/terminal-answer.js";

// Real AgentManager + real ProcessTransport (spied, pass-through "recording launcher") + the
// installed fake worker fixture. Only the worker/inference is a fixture. Fixtures are retained.
const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usable = process.platform !== "win32" && !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs(); vi.restoreAllMocks();
});
const agents = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [] };
const options = { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), preparePiModel: async (model: string | undefined) => model, fullCodeMode: false };
const aliasFixture = () => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "manager-root-")));
  const real = path.join(base, "real"), alias = path.join(base, "alias");
  fs.mkdirSync(real, { mode: 0o700 }); fs.symlinkSync(real, alias, "dir");
  return { base, real, alias };
};

describe.skipIf(!usable)("manager bootstrap canonicalizes the trusted host run root once", () => {
  it("launches an ordinary spawn under a symlinked default tmpdir (macOS /var -> /private/var shape)", async () => {
    ensureTaskControlRoot();
    const { real, alias } = aliasFixture();
    vi.stubEnv("TMPDIR", alias); vi.stubEnv("PI_FABRIC_TMPDIR", ""); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = new AgentManager(process.cwd(), agents, options); managers.push(manager);
    const result = await manager.run({ task: "ordinary spawn", transport: "process" });
    expect(result.status).toBe("completed");
    expect(launch).toHaveBeenCalledOnce();
    const runs = fs.readdirSync(real).filter(name => name.startsWith("pi-fabric-runs-"));
    expect(runs).toHaveLength(1);
    const runDirectory = path.join(real, runs[0]!, result.id);
    expect(fs.realpathSync(runDirectory)).toBe(runDirectory);
    expect(fs.statSync(taskControlDirectory(runDirectory)).isDirectory()).toBe(true);
  });

  it("launches under an explicit not-yet-existing host root reached through a symlinked ancestor without creating extra paths", async () => {
    ensureTaskControlRoot();
    const { real, alias } = aliasFixture();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = new AgentManager(process.cwd(), agents, { ...options, runRoot: path.join(alias, "runs") }); managers.push(manager);
    const result = await manager.run({ task: "ordinary spawn", transport: "process" });
    expect(result.status).toBe("completed");
    expect(launch).toHaveBeenCalledOnce();
    expect(fs.readdirSync(real)).toEqual(["runs"]);
    const runDirectory = path.join(real, "runs", result.id);
    expect(fs.statSync(taskControlDirectory(runDirectory)).isDirectory()).toBe(true);
  });

  it("still refuses a child-planted alias BELOW the trusted root (inherited nested root) and leaves the victim untouched", async () => {
    ensureTaskControlRoot();
    const { base, real } = aliasFixture();
    const victim = path.join(base, "victim"); fs.mkdirSync(path.join(victim, "nested"), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(victim, "nested", "marker"), "VICTIM", { mode: 0o600 });
    fs.symlinkSync(victim, path.join(real, "planted"), "dir");
    const snapshot = () => fs.readdirSync(victim, { recursive: true }).map(String).sort();
    const before = snapshot(), beforeStat = fs.lstatSync(path.join(victim, "nested"));
    vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(real, "planted", "nested"));
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = new AgentManager(process.cwd(), agents, options); managers.push(manager);
    await expect(manager.run({ task: "must not launch", transport: "process" })).rejects.toThrow();
    expect(launch).not.toHaveBeenCalled();
    expect(snapshot()).toEqual(before);
    const afterStat = fs.lstatSync(path.join(victim, "nested"));
    expect([afterStat.ino, afterStat.mtimeMs]).toEqual([beforeStat.ino, beforeStat.mtimeMs]);
    expect(fs.readFileSync(path.join(victim, "nested", "marker"), "utf8")).toBe("VICTIM");
  });
});
