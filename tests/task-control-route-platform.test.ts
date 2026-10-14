import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager, managerRunTestHooks } from "../src/agents/manager.js";
import * as modelRoute from "../src/agents/model-route.js";
import { RUN_IDENTITY_REFUSED, prepareRouteDispatch, type RunDirectoryPin } from "../src/agents/model-route.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ROUTE_CONTROL_FILES, createHostTaskRun, ensureTaskControlRoot, routeControlFile, taskControlDirectory } from "../src/worker/terminal-answer.js";

// Central route-control publication on every platform, without a manager directory-fd channel.
// Real AgentManager and real filesystem; only the worker is a fixture. macOS/Windows prove path
// layout, not enforced-grant confinement. Windows uses an explicit trusted host run root.

const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usable = !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const windowsHost = process.platform === "win32";
const managers: AgentManager[] = [];
afterEach(async () => {
  delete managerRunTestHooks.afterRunAllocation;
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs(); vi.restoreAllMocks();
});
const agents = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [] };
const decisionFor = (mode: "shadow" | "live" | "judgment" = "shadow") => {
  const pin = { model: "test/fake", effort: "low" };
  return { decisionId: `d${Date.now()}${Math.random()}`, mode, routeClass: "bounded-lookup", parentSessionId: "parent", pin, candidates: [pin],
    shadowChoice: pin, confidence: null, probability: null, reasonCode: mode === "shadow" ? "shadow-choice" : `${mode}-choice`, latencyMs: 0, ...pin };
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
  // Windows requires an explicit trusted host root; inherited nested roots stay refused.
  vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  const manager = new AgentManager(process.cwd(), agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), preparePiModel: async model => model, fullCodeMode: false, runRoot });
  managers.push(manager);
  return manager;
};
const entries = (root: string) => fs.existsSync(root) ? fs.readdirSync(root, { recursive: true }).map(String).sort() : [];
/** Every entry of a real tree with its identity, size and mtime: any create/delete/rename/write shows up. */
const treeState = (root: string) => {
  const stat = (file: string) => { const seen = fs.lstatSync(file, { bigint: true }); return `${seen.ino}:${seen.size}:${seen.mtimeNs}:${seen.mode}`; };
  return { root: stat(root), entries: entries(root).map(name => [name, stat(path.join(root, name))]) };
};

const pinFor = (run: string): RunDirectoryPin => {
  const bound = fs.lstatSync(run, { bigint: true });
  return { run, use(operation) {
    const now = fs.lstatSync(run, { bigint: true });
    if (now.isSymbolicLink() || now.dev !== bound.dev || now.ino !== bound.ino) throw Object.assign(new Error("run identity changed"), { code: RUN_IDENTITY_REFUSED });
    return operation();
  } };
};
const routeDirectory = (run: string) => path.dirname(routeControlFile(run, RECEIPT));

describe.skipIf(!usable)("smarty-dev#12444: central route-control namespace (all platforms)", () => {
  it("retires the unsupported-platform contract: exact route names", () => {
    expect("CONTROL_PUBLISH_UNSUPPORTED_PLATFORM" in modelRoute).toBe(false);
    expect([...ROUTE_CONTROL_FILES]).toEqual([RECEIPT, PENDING, SESSION]);
    expect(() => routeControlFile(path.join(os.tmpdir(), "x"), "status.json" as never)).toThrow();
  });
  it("routeControlFile is pure and read-only fallback is absent-route only", () => {
    const { base } = fixture(), run = path.join(base, "run");
    createHostTaskRun(run);
    const central = routeControlFile(run, RECEIPT);
    expect(central).toBe(path.join(taskControlDirectory(run), "route", RECEIPT));
    expect(fs.existsSync(path.dirname(central)), "never creates").toBe(false);
    expect(routeControlFile(run, RECEIPT, { legacyReadOnly: true })).toBe(path.join(run, RECEIPT));
    prepareRouteDispatch(decisionFor() as never, base, run, "child-id", { ledger: path.join(base, "ledger.jsonl"), decisionRecorded: true });
    expect(routeControlFile(run, RECEIPT, { legacyReadOnly: true })).toBe(central);
  });
  it("an unpinned direct dispatch on a host-allocated run publishes only under central route/", () => {
    const { base, ledger } = fixture(), run = path.join(base, "run");
    createHostTaskRun(run);
    const prepared = prepareRouteDispatch(decisionFor() as never, base, run, "child-id", { ledger, decisionRecorded: true });
    expect(prepared.sessionFile).toBe(routeControlFile(run, SESSION));
    expect(JSON.parse(fs.readFileSync(routeControlFile(run, RECEIPT), "utf8"))).toMatchObject({ runId: "child-id", ledger });
    expect(fs.readdirSync(routeDirectory(run)).sort()).toEqual([RECEIPT, SESSION].sort());
    if (!windowsHost) expect(fs.statSync(routeDirectory(run)).mode & 0o777).toBe(0o700);
    expect(entries(run).filter(isRouteControl)).toEqual([]);
  });
  it("a real routed manager spawn passes no directory fd and keeps controls out of C", async () => {
    ensureTaskControlRoot();
    const { ledger, runRoot } = fixture();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = managerFor(runRoot);
    const result = await manager.run({ task: "routed spawn", transport: "process", ...routed(ledger) } as never);
    expect(result.status).toBe("completed");
    const run = path.join(runRoot, result.id);
    expect(launch).toHaveBeenCalledTimes(1);
    const request = launch.mock.calls[0]![0];
    expect((request as { inheritFds?: number[] }).inheritFds).toBeUndefined();
    expect(request.workerArguments).not.toContain("--session-directory-fd");
    expect(JSON.parse(fs.readFileSync(routeControlFile(run, RECEIPT), "utf8"))).toMatchObject({ runId: result.id });
    expect(entries(run).filter(isRouteControl)).toEqual([]);
  });
  it("a plain non-routed spawn completes with no route control anywhere", async () => {
    ensureTaskControlRoot();
    const { runRoot } = fixture(), manager = managerFor(runRoot);
    const result = await manager.run({ task: "plain spawn", transport: "process" });
    expect(result.status).toBe("completed");
    expect(entries(path.join(runRoot, result.id)).filter(isRouteControl)).toEqual([]);
    expect(fs.existsSync(routeDirectory(path.join(runRoot, result.id)))).toBe(false);
  });
});

describe.skipIf(!usable)("smarty-dev#12444: missing central task-control directory", () => {
  const unallocated = () => {
    const { base, ledger } = fixture(), run = path.join(base, "run");
    fs.mkdirSync(run, { mode: 0o700 });
    fs.writeFileSync(path.join(run, "marker"), "RUN", { mode: 0o600 });
    return { base, ledger, run, before: treeState(run) };
  };
  const nothingAnywhere = (f: ReturnType<typeof unallocated>) => {
    expect(treeState(f.run)).toEqual(f.before);
    expect(entries(f.base).filter(isRouteControl)).toEqual([]);
    expect(fs.existsSync(taskControlDirectory(f.run))).toBe(false);
    expect(fs.existsSync(f.ledger)).toBe(false);
  };
  for (const mode of ["shadow", "live"] as const) {
    it(`${mode}: missing central demotes to record-failed and writes nothing anywhere`, () => {
      const f = unallocated(), decision = decisionFor(mode);
      const prepared = prepareRouteDispatch(decision as never, f.base, f.run, "child-id", { ledger: f.ledger, decisionRecorded: true });
      expect(decision).toMatchObject({ mode: "shadow", reasonCode: "record-failed", model: decision.pin.model });
      expect(prepared.sessionFile).toBeUndefined();
      prepared.outcome({ status: "failed" });
      nothingAnywhere(f);
    });
  }
  it("judgment: missing central throws and writes nothing anywhere", () => {
    const f = unallocated();
    expect(() => prepareRouteDispatch(decisionFor("judgment") as never, f.base, f.run, "child-id", { ledger: f.ledger, decisionRecorded: true })).toThrow();
    nothingAnywhere(f);
  });
  for (const mode of ["shadow", "live", "judgment"] as const) {
    it(`pinned ${mode}: missing central is typed RUN_IDENTITY_REFUSED, never demoted`, () => {
      const f = unallocated(), decision = decisionFor(mode);
      expect(() => prepareRouteDispatch(decision as never, undefined, f.run, "child-id", { ledger: f.ledger, decisionRecorded: true, runPin: pinFor(f.run) }))
        .toThrow(expect.objectContaining({ code: RUN_IDENTITY_REFUSED }));
      expect(decision.reasonCode).not.toBe("record-failed");
      nothingAnywhere(f);
    });
  }
});

describe.skipIf(!usable)("smarty-dev#12444: unsafe route/ namespaces refuse without repair", () => {
  const shapes = [
    { label: "directory alias", plant: (route: string, victim: string) => fs.symlinkSync(victim, route, windowsHost ? "junction" : "dir") },
    { label: "regular file", plant: (route: string) => fs.writeFileSync(route, "NOT-A-DIRECTORY", { mode: 0o600 }) },
    ...(!windowsHost ? [{ label: "non-private directory", plant: (route: string) => { fs.mkdirSync(route, { mode: 0o700 }); fs.chmodSync(route, 0o777); } }] : []),
  ];
  const setup = (shape: typeof shapes[number]) => {
    const { base, ledger } = fixture(), run = path.join(base, "run"), victim = path.join(base, "victim");
    createHostTaskRun(run);
    fs.mkdirSync(victim, { mode: 0o700 }); fs.writeFileSync(path.join(victim, "marker"), "VICTIM", { mode: 0o600 });
    const route = routeDirectory(run); shape.plant(route, victim);
    return { base, ledger, run, victim, route, before: treeState(victim) };
  };
  for (const shape of shapes) {
    it(`pinned central ${shape.label} refuses typed and preserves victim`, () => {
      const f = setup(shape), before = fs.lstatSync(f.route, { bigint: true });
      expect(() => prepareRouteDispatch(decisionFor() as never, f.base, f.run, "child-id", { ledger: f.ledger, decisionRecorded: true, runPin: pinFor(f.run) }))
        .toThrow(expect.objectContaining({ code: RUN_IDENTITY_REFUSED }));
      expect(treeState(f.victim)).toEqual(f.before);
      expect(fs.lstatSync(f.route, { bigint: true }).mode).toBe(before.mode);
      expect(fs.existsSync(f.ledger)).toBe(false);
    });
    it(`unpinned central ${shape.label} demotes without writing`, () => {
      const f = setup(shape), decision = decisionFor();
      prepareRouteDispatch(decision as never, f.base, f.run, "child-id", { ledger: f.ledger, decisionRecorded: true });
      expect(decision).toMatchObject({ mode: "shadow", reasonCode: "record-failed" });
      expect(treeState(f.victim)).toEqual(f.before);
      expect(entries(f.base).filter(isRouteControl)).toEqual([]);
    });
  }
  it("legacyReadOnly refuses an aliased route/ instead of falling back", () => {
    const f = setup(shapes[0]!);
    expect(() => routeControlFile(f.run, RECEIPT, { legacyReadOnly: true })).toThrow();
  });
});
