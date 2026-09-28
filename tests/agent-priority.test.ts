import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import {
  applyChildPriority,
  effectiveAgentNice,
  parseAgentNice,
  resetChildPriorityLog,
  type ChildPriorityDeps,
} from "../src/agents/priority.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { removeTree } from "../src/agents/rm.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { parseWorkerOptions } from "../src/worker/options.js";

const deps = (overrides: Partial<ChildPriorityDeps> = {}): ChildPriorityDeps & { logs: string[] } => {
  const logs: string[] = [];
  return {
    getPriority: vi.fn(() => 0),
    setPriority: vi.fn(),
    ionice: vi.fn(() => ({ status: 0 })),
    platform: "linux",
    log: (message) => logs.push(message),
    logs,
    ...overrides,
  };
};

beforeEach(() => resetChildPriorityLog());

describe("applyChildPriority (smarty-dev#1579)", () => {
  it("nice 10 calls setPriority with 10 and ionice on Linux", () => {
    const d = deps();
    applyChildPriority(4242, 10, d.log, d);
    expect(d.setPriority).toHaveBeenCalledWith(4242, 10);
    expect(d.ionice).toHaveBeenCalledWith(4242);
    expect(d.logs).toEqual([]);
  });

  it("nice 0 changes nothing", () => {
    const d = deps();
    applyChildPriority(4242, 0, d.log, d);
    expect(d.setPriority).not.toHaveBeenCalled();
    expect(d.ionice).not.toHaveBeenCalled();
  });

  it("keeps an already lower priority instead of failing to raise it", () => {
    const d = deps({ getPriority: vi.fn(() => 15) });
    applyChildPriority(1, 10, d.log, d);
    expect(d.setPriority).not.toHaveBeenCalled();
    expect(d.logs).toEqual([]);
  });

  it("skips ionice off Linux and when ionice is not installed", () => {
    const mac = deps({ platform: "darwin" });
    applyChildPriority(1, 5, mac.log, mac);
    expect(mac.setPriority).toHaveBeenCalledWith(1, 5);
    expect(mac.ionice).not.toHaveBeenCalled();
    const missing = deps({ ionice: vi.fn(() => ({ status: null, error: Object.assign(new Error("spawnSync ionice ENOENT"), { code: "ENOENT" }) })) });
    applyChildPriority(1, 5, missing.log, missing);
    expect(missing.logs).toEqual([]);
  });

  it("logs a failure once per process and never throws", () => {
    const d = deps({
      setPriority: vi.fn(() => { throw new Error("EACCES"); }),
      ionice: vi.fn(() => ({ status: 1, stderr: "ionice: ioprio_set failed" })),
    });
    expect(() => applyChildPriority(1, 10, d.log, d)).not.toThrow();
    expect(() => applyChildPriority(2, 10, d.log, d)).not.toThrow();
    expect(d.setPriority).toHaveBeenCalledTimes(2);
    expect(d.logs).toHaveLength(2);
    expect(d.logs[0]).toContain("EACCES");
    expect(d.logs[1]).toContain("ioprio_set failed");
  });
});

describe("agents.nice config and per-spawn nice", () => {
  it("defaults to 0 and clamps the config to 0..19", () => {
    expect(DEFAULT_FABRIC_CONFIG.agents.nice).toBe(0);
    expect(normalizeFabricConfig({ agents: { nice: 10 } }).agents.nice).toBe(10);
    expect(normalizeFabricConfig({ agents: { nice: 40 } }).agents.nice).toBe(19);
    expect(normalizeFabricConfig({ agents: { nice: -5 } }).agents.nice).toBe(0);
    expect(normalizeFabricConfig({ agents: { nice: "high" } }).agents.nice).toBe(0);
  });

  it("per-spawn nice only raises the config value and is clamped to 19", () => {
    expect(effectiveAgentNice(10, 5)).toBe(10);
    expect(effectiveAgentNice(10, 0)).toBe(10);
    expect(effectiveAgentNice(10, undefined)).toBe(10);
    expect(effectiveAgentNice(0, 15)).toBe(15);
    expect(effectiveAgentNice(10, parseAgentNice(99))).toBe(19);
    expect(() => parseAgentNice("10")).toThrow("Invalid nice");
    const request = normalizeAgentRunRequest({ task: "t", nice: 12 }, { runner: "pi", timeoutMs: 1_000 });
    expect(request.nice).toBe(12);
  });
});

describe.runIf(process.platform === "linux")("the real worker lowers priority before the CLI starts (Linux)", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  afterEach(async () => {
    await Promise.all(managers.splice(0).map((manager) => manager.close()));
    vi.unstubAllEnvs();
    await Promise.all(roots.splice(0).map((root) => removeTree(root)));
  });

  it("every CLI thread, a descendant and the IO priority start lowered", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-agent-nice-real-"));
    roots.push(root);
    const probe = path.join(root, "probe.json");
    vi.stubEnv("FABRIC_PRIORITY_PROBE", probe);
    const fakePi = path.resolve("tests/fixtures/fake-pi-priority.mjs");
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, nice: 15 }, {
      workerPath: path.resolve("src/worker.ts"),
      piBinary: fakePi,
      runRoot: root,
      fullCodeMode: false,
    });
    managers.push(manager);
    const result = await manager.run({ task: "probe", transport: "process", timeoutMs: 20_000 });
    expect(result.status).toBe("completed");
    const seen = JSON.parse(fs.readFileSync(probe, "utf8")) as { threads: number[]; descendant: number; ionice: string | null };
    const expected = Math.max(os.getPriority(), 15);
    expect(seen.threads.length).toBeGreaterThan(1);
    expect(seen.threads.every((nice) => nice === expected)).toBe(true);
    expect(seen.descendant).toBe(expected);
    if (seen.ionice !== null) expect(seen.ionice).toBe("best-effort: prio 7");
  }, 30_000);
});

describe("AgentManager passes the effective nice to the worker", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  afterEach(async () => {
    await Promise.all(managers.splice(0).map((manager) => manager.close()));
    vi.restoreAllMocks();
    await Promise.all(roots.splice(0).map((root) => removeTree(root)));
  });

  const launchNice = async (configNice: number, nice?: number): Promise<number | undefined> => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-agent-nice-"));
    roots.push(root);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, nice: configNice }, {
      runRoot: root,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "nice", transport: "process", ...(nice !== undefined ? { nice } : {}) });
    const options = parseWorkerOptions(["node", "worker.js", ...launch.mock.calls.at(-1)![0].workerArguments]);
    await manager.wait(handle.id);
    return options.nice;
  };

  it("config 10 reaches the worker; a lower per-spawn value cannot lower it; a higher one raises it", async () => {
    expect(await launchNice(0)).toBeUndefined();
    expect(await launchNice(10)).toBe(10);
    expect(await launchNice(10, 3)).toBe(10);
    expect(await launchNice(10, 15)).toBe(15);
  });
});
