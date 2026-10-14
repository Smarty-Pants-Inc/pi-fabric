import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager, managerRunTestHooks } from "../src/agents/manager.js";
import { CONTROL_PUBLISH_UNSUPPORTED_PLATFORM, prepareRouteDispatch, type RunDirectoryPin } from "../src/agents/model-route.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ensureTaskControlRoot } from "../src/worker/terminal-answer.js";

// Fail-closed route-control publication on platforms without a traversable held directory fd.
// Real AgentManager and real filesystem; only the worker is the fake fixture. The platform and
// manager-only fd capability are explicit test seams, so private-dir's real allocation walk remains active.

const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usable = process.platform !== "win32" && !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const managers: AgentManager[] = [];
afterEach(async () => {
  delete managerRunTestHooks.afterRunAllocation;
  delete managerRunTestHooks.platform;
  delete managerRunTestHooks.resolveFdBase;
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs(); vi.restoreAllMocks();
});
const agents = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [] };
const decisionFor = () => {
  const pin = { model: "test/fake", effort: "low" };
  return { decisionId: `d${Date.now()}${Math.random()}`, mode: "shadow", routeClass: "bounded-lookup", parentSessionId: "parent", pin, candidates: [pin],
    shadowChoice: pin, confidence: null, probability: null, reasonCode: "shadow-choice", latencyMs: 0, ...pin };
};
const routed = (ledger: string) => ({ routeDecision: decisionFor(), routeRecord: { ledger, decisionRecorded: true } } as Record<string, unknown>);
const RECEIPT = "route-dispatch-receipt.json", PENDING = "pending-route-outcome.json", SESSION = "route-session.jsonl";
const isRouteControl = (name: string) => [RECEIPT, PENDING, SESSION].includes(path.basename(name)) || name.endsWith(".tmp");
// Private fixtures are retained (same pattern as task-control-route-race); nothing is deleted.
const fixture = () => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "route-platform-")));
  const granted = path.join(base, "granted");
  fs.mkdirSync(granted, { mode: 0o700 });
  return { base, ledger: path.join(base, "ledger.jsonl"), runRoot: path.join(granted, "nested") };
};
const managerFor = (runRoot: string) => {
  // Real nested-manager path: the run root is INHERITED via PI_FABRIC_RUN_ROOT.
  vi.stubEnv("PI_FABRIC_RUN_ROOT", runRoot);
  const manager = new AgentManager(process.cwd(), agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), preparePiModel: async model => model, fullCodeMode: false });
  managers.push(manager);
  return manager;
};
const entries = (root: string) => fs.existsSync(root) ? fs.readdirSync(root, { recursive: true }).map(String).sort() : [];
/** Every entry of a real tree with its identity, size and mtime: any create/delete/rename/write shows up. */
const treeState = (root: string) => {
  const stat = (file: string) => { const seen = fs.lstatSync(file, { bigint: true }); return `${seen.ino}:${seen.size}:${seen.mtimeNs}:${seen.mode}`; };
  return { root: stat(root), entries: entries(root).map(name => [name, stat(path.join(root, name))]) };
};

describe.skipIf(!usable)("routed control publication refuses without a traversable held directory fd", () => {
  const refusesRoutedSpawn = async (simulate: () => void) => {
    ensureTaskControlRoot();
    const { ledger, runRoot } = fixture();
    simulate();
    const allocated: string[] = [];
    managerRunTestHooks.afterRunAllocation = directory => { allocated.push(directory); };
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = managerFor(runRoot);
    await expect(manager.run({ task: "must not launch", transport: "process", ...routed(ledger) } as never))
      .rejects.toMatchObject({ code: "FABRIC_CONTROL_PUBLISH_UNSUPPORTED_PLATFORM" });
    // Refused at the pin, before the post-allocation yield, route import, receipt, session or pending write.
    expect(allocated).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
    expect(entries(runRoot).filter(isRouteControl)).toEqual([]);
    const runs = fs.existsSync(runRoot) ? fs.readdirSync(runRoot) : [];
    for (const run of runs) for (const name of [RECEIPT, PENDING, SESSION]) expect(fs.existsSync(path.join(runRoot, run, name))).toBe(false);
    // No decision/outcome record and no pending fallback anywhere.
    expect(fs.existsSync(ledger)).toBe(false);
  };

  it("routed spawn with simulated win32 refuses (typed) before receipt/session/pending writes", async () => {
    await refusesRoutedSpawn(() => { managerRunTestHooks.platform = "win32"; });
  });

  it("routed spawn with simulated darwin and no manager fd base refuses (typed) before receipt/session/pending writes", async () => {
    await refusesRoutedSpawn(() => {
      managerRunTestHooks.platform = "darwin";
      managerRunTestHooks.resolveFdBase = () => undefined;
    });
  });

  it.runIf(process.platform === "linux")("counterexample: simulated darwin WITH a traversable fd base still publishes the routed receipt", async () => {
    ensureTaskControlRoot();
    const { ledger, runRoot } = fixture();
    managerRunTestHooks.platform = "darwin";
    const manager = managerFor(runRoot);
    const result = await manager.run({ task: "routed spawn", transport: "process", ...routed(ledger) } as never);
    expect(result.status).toBe("completed");
    const receipt = JSON.parse(fs.readFileSync(path.join(runRoot, result.id, RECEIPT), "utf8")) as { runId: string };
    expect(receipt.runId).toBe(result.id);
  });

  const simulations = {
    win32: () => { managerRunTestHooks.platform = "win32"; },
    "darwin without a manager fd base": () => {
      managerRunTestHooks.platform = "darwin";
      managerRunTestHooks.resolveFdBase = () => undefined;
    },
  } as const;
  for (const [label, simulate] of Object.entries(simulations)) {
    it(`a plain non-routed spawn still completes under simulated ${label}`, async () => {
      ensureTaskControlRoot();
      const { runRoot } = fixture();
      simulate();
      let run: string | undefined;
      managerRunTestHooks.afterRunAllocation = directory => { run = directory; };
      const manager = managerFor(runRoot);
      const result = await manager.run({ task: "plain spawn", transport: "process" });
      expect(result.status).toBe("completed");
      expect(run).toBe(path.join(runRoot, result.id));
      expect(entries(path.join(runRoot, result.id)).filter(isRouteControl)).toEqual([]);
    });
  }

  // Direct dispatch: a pin whose `use` hands publication the NAMED directory without a held fd must refuse
  // before any temp is opened, leaving the real run tree byte-for-byte (and mtime) unchanged.
  const namedPinCases: Array<[string, (run: string) => string, string | undefined]> = [
    ["the pinned name itself, receipt publication", run => run, undefined],
    ["the pinned name itself, session publication", run => run, process.cwd()],
    ["another spelling of the same directory, receipt publication", run => `${run}${path.sep}.`, undefined],
  ];
  for (const [label, directoryFor, cwd] of namedPinCases) {
    it(`prepareRouteDispatch refuses a pin supplying ${label} with an undefined fd`, () => {
      const { base, ledger } = fixture();
      const run = path.join(base, "run");
      fs.mkdirSync(run, { mode: 0o700 });
      fs.writeFileSync(path.join(run, "marker"), "RUN", { mode: 0o600 });
      const before = treeState(run);
      let uses = 0;
      const pin: RunDirectoryPin = { run, use: operation => { uses += 1; return operation(directoryFor(run), undefined); } };
      let caught: unknown;
      try { prepareRouteDispatch(decisionFor() as never, cwd, run, "child-id", { ledger, decisionRecorded: true, runPin: pin }); }
      catch (error) { caught = error; }
      expect(caught).toMatchObject({ code: CONTROL_PUBLISH_UNSUPPORTED_PLATFORM });
      expect(CONTROL_PUBLISH_UNSUPPORTED_PLATFORM).toBe("FABRIC_CONTROL_PUBLISH_UNSUPPORTED_PLATFORM");
      // The identity precheck and the refused publication both went through the pin; nothing else ran.
      expect(uses).toBe(2);
      expect(treeState(run)).toEqual(before);
      expect(entries(run)).toEqual(["marker"]);
      expect(fs.existsSync(ledger)).toBe(false);
    });
  }
});
