import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager, managerRunTestHooks } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ensureTaskControlRoot, routeControlFile } from "../src/worker/terminal-answer.js";

// DELTA-SEC-02 residual: a parent with a writable grant swaps the freshly allocated run directory C
// (symlink to a victim, or a real replacement directory) at the post-allocation yield or during model
// preparation. Real AgentManager, real filesystem; only the worker is the fake fixture.
const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
// Linux uses a Landlock-confined swapper; macOS/Windows prove path layout only.
const usable = !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const managers: AgentManager[] = [];
afterEach(async () => {
  delete managerRunTestHooks.afterRunAllocation;
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs(); vi.restoreAllMocks();
});
const agents = { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [] };
const routed = (ledger: string) => {
  const pin = { model: "test/fake", effort: "low" };
  return {
    routeDecision: { decisionId: `d${Date.now()}${Math.random()}`, mode: "shadow", routeClass: "bounded-lookup", parentSessionId: "parent", pin, candidates: [pin],
      shadowChoice: pin, confidence: null, probability: null, reasonCode: "shadow-choice", latencyMs: 0, ...pin },
    routeRecord: { ledger, decisionRecorded: true },
  } as Record<string, unknown>;
};
const RECEIPT = "route-dispatch-receipt.json", PENDING = "pending-route-outcome.json", SESSION = "route-session.jsonl";
const ROUTE_NAMES = [RECEIPT, PENDING, SESSION];
const routeFilesIn = (root: string): string[] => {
  let stat: fs.Stats;
  try { stat = fs.lstatSync(root); } catch { return []; }
  if (!stat.isDirectory() || stat.isSymbolicLink()) return [];
  return fs.readdirSync(root, { withFileTypes: true }).flatMap(entry => {
    const full = path.join(root, entry.name);
    return [...(ROUTE_NAMES.some(name => entry.name.includes(name)) ? [full] : []), ...(entry.isDirectory() && !entry.isSymbolicLink() ? routeFilesIn(full) : [])];
  });
};
const windowsHost = process.platform === "win32";
// Parent mutation runs in an ACTUAL Landlock-confined process granted only root R; the victim is a sibling OUTSIDE R.
// Fixtures are private and retained; nothing is deleted.
const LANDLOCK = path.resolve("dist/native/fabric-landlock");
const confinedParent = process.platform === "linux";
const SWAP_SCRIPT = `const fs = require("node:fs"), path = require("node:path");
const [run, victim, kind] = process.argv.slice(2);
let escape = "written";
try { fs.writeFileSync(path.join(victim, "escape"), "x", { flag: "wx" }); } catch (error) { escape = error.code; }
fs.renameSync(run, run + ".moved");
if (kind === "symlink") fs.symlinkSync(victim, run, "dir"); else fs.mkdirSync(run, { mode: 0o700 });
console.log(JSON.stringify({ type: "CONFINED_SWAP", kind, escape }));
`;
const fixture = () => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "route-race-")));
  const victim = path.join(base, "victim");
  fs.mkdirSync(victim, { mode: 0o700 });
  fs.writeFileSync(path.join(victim, "marker"), "VICTIM", { mode: 0o600 });
  const granted = path.join(base, "granted"); // R: the parent's only writable root
  fs.mkdirSync(granted, { mode: 0o700 });
  const script = path.join(base, "swap.cjs");
  fs.writeFileSync(script, SWAP_SCRIPT, { mode: 0o600 });
  const ledger = path.join(base, "ledger.jsonl");
  const snapshot = () => ({ entries: fs.readdirSync(victim, { recursive: true }).map(String).sort(),
    marker: fs.readFileSync(path.join(victim, "marker"), "utf8"), ino: fs.lstatSync(victim).ino });
  return { base, victim, granted, script, ledger, runRoot: path.join(granted, "nested"), snapshot };
};
type Fixture = ReturnType<typeof fixture>;
type Swap = "symlink" | "replacement";
/** The parent-granted mutation: move the allocated C aside, then put a symlink to the victim or a fresh real directory at C. */
const swap = (f: Fixture, run: string, kind: Swap): void => {
  if (!confinedParent) {
    fs.renameSync(run, `${run}.moved`);
    if (kind === "symlink") fs.symlinkSync(f.victim, run, windowsHost ? "junction" : "dir");
    else fs.mkdirSync(run, { mode: 0o700 });
    return;
  }
  expect(fs.existsSync(LANDLOCK)).toBe(true);
  const stat = fs.statSync(f.granted, { bigint: true });
  const q = JSON.stringify;
  const result = spawnSync(LANDLOCK, ["-c", `exec ${q(process.execPath)} ${q(f.script)} ${q(run)} ${q(f.victim)} ${q(kind)}`], {
    cwd: f.granted, encoding: "utf8", timeout: 5000,
    env: { ...process.env, LC_ALL: "C", PI_FABRIC_LANDLOCK_SHELL: "/bin/sh", PI_FABRIC_LANDLOCK_WRITES: `${stat.dev}:${stat.ino}:${f.granted}` },
  });
  expect(result.error).toBeUndefined();
  expect(result.status, result.stderr).toBe(0);
  const evidence = JSON.parse(result.stdout.trim().split("\n").at(-1)!) as { type: string; kind: Swap; escape: string };
  expect(evidence).toMatchObject({ type: "CONFINED_SWAP", kind });
  // The confinement is real: the same process cannot write the victim outside R.
  expect(["EACCES", "EPERM", "EROFS"]).toContain(evidence.escape);
};
const managerFor = (runRoot: string, preparePiModel: (model: string | undefined) => Promise<string | undefined> = async model => model) => {
  // Windows refuses inherited nested roots; use an explicit trusted host root there.
  if (windowsHost) vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined); else vi.stubEnv("PI_FABRIC_RUN_ROOT", runRoot);
  const manager = new AgentManager(process.cwd(), agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), preparePiModel, fullCodeMode: false,
    ...(windowsHost ? { runRoot } : {}) });
  managers.push(manager);
  return manager;
};
const ledgerText = (ledger: string) => fs.existsSync(ledger) ? fs.readFileSync(ledger, "utf8") : "";

describe.skipIf(!usable)(`DELTA-SEC-02: central route writes [${confinedParent ? "Linux Landlock-confined parent" : "path-layout only; unconfined swapper"}]`, () => {
  for (const kind of ["symlink", "replacement"] as const) {
    it(`refuses the route receipt (typed, nothing written) when C becomes a ${kind} at the post-allocation yield`, async () => {
      ensureTaskControlRoot();
      const f = fixture(), { victim, ledger, runRoot, snapshot } = f;
      const before = snapshot();
      let run: string | undefined, central: string | undefined;
      managerRunTestHooks.afterRunAllocation = async (directory) => { run = directory; central = routeControlFile(directory, RECEIPT); await Promise.resolve(); swap(f, directory, kind); };
      const launch = vi.spyOn(ProcessTransport.prototype, "launch");
      const manager = managerFor(runRoot);
      await expect(manager.run({ task: "must not launch", transport: "process", ...routed(ledger) } as never))
        .rejects.toMatchObject({ code: "FABRIC_RUN_IDENTITY_REFUSED" });
      expect(launch).not.toHaveBeenCalled();
      expect(snapshot()).toEqual(before);
      expect(routeFilesIn(victim)).toEqual([]);
      expect(fs.readdirSync(run!).filter(name => name !== "marker")).toEqual([]);
      expect(routeFilesIn(`${run}.moved`)).toEqual([]);
      expect(fs.existsSync(central!)).toBe(false);
      expect(ledgerText(ledger)).not.toContain("\"outcome\"");
    });

    it(`refuses the model-preparation failure outcome (typed, nothing written) when C becomes a ${kind}`, async () => {
      ensureTaskControlRoot();
      const f = fixture(), { victim, ledger, runRoot, snapshot } = f;
      const before = snapshot();
      let run: string | undefined, central: string | undefined, centralPending: string | undefined;
      managerRunTestHooks.afterRunAllocation = (directory) => { run = directory; central = routeControlFile(directory, RECEIPT); centralPending = routeControlFile(directory, PENDING); };
      const launch = vi.spyOn(ProcessTransport.prototype, "launch");
      const manager = managerFor(runRoot, async () => {
        await Promise.resolve();
        swap(f, run!, kind);
        throw new Error("model preparation failed");
      });
      await expect(manager.run({ task: "must not launch", transport: "process", ...routed(ledger) } as never))
        .rejects.toMatchObject({ code: "FABRIC_RUN_IDENTITY_REFUSED" });
      expect(launch).not.toHaveBeenCalled();
      expect(snapshot()).toEqual(before);
      expect(routeFilesIn(victim)).toEqual([]);
      if (kind === "replacement") expect(fs.readdirSync(run!)).toEqual([]);
      expect(routeFilesIn(`${run}.moved`)).toEqual([]);
      expect(fs.existsSync(central!)).toBe(true);
      expect(fs.existsSync(centralPending!)).toBe(false);
      expect(ledgerText(ledger)).not.toContain("\"outcome\"");
    });
  }

  // Catch-path variant: the outcome's initial identity guard passes; the swap happens inside the ledger append
  // (pass-through fs.openSync hook on the real FS), which then fails, so the pending-publication writer must refuse.
  const ledgerFault = (ledger: string, onFault: () => void) => {
    const original = fs.openSync.bind(fs);
    let fired = false;
    return vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags?: fs.OpenMode, mode?: fs.Mode) => {
      if (!fired && String(file) === ledger && typeof flags === "number" && (flags & fs.constants.O_APPEND) !== 0) {
        fired = true;
        onFault();
        throw Object.assign(new Error("injected ledger append failure"), { code: "EIO" });
      }
      if (flags === undefined) return original(file, fs.constants.O_RDONLY);
      if (mode === undefined) return original(file, flags);
      return original(file, flags, mode);
    }) as typeof fs.openSync);
  };
  for (const kind of ["symlink", "replacement"] as const) {
    it(`refuses the pending publication (typed, nothing written) when C becomes a ${kind} after the outcome guard and the ledger append fails`, async () => {
      ensureTaskControlRoot();
      const f = fixture(), { victim, ledger, runRoot, snapshot } = f;
      const before = snapshot();
      let run: string | undefined, central: string | undefined, centralPending: string | undefined;
      managerRunTestHooks.afterRunAllocation = (directory) => { run = directory; central = routeControlFile(directory, RECEIPT); centralPending = routeControlFile(directory, PENDING); };
      const launch = vi.spyOn(ProcessTransport.prototype, "launch");
      const open = ledgerFault(ledger, () => swap(f, run!, kind));
      const manager = managerFor(runRoot, async () => { throw new Error("model preparation failed"); });
      await expect(manager.run({ task: "must not launch", transport: "process", ...routed(ledger) } as never))
        .rejects.toMatchObject({ code: "FABRIC_RUN_IDENTITY_REFUSED" });
      open.mockRestore();
      expect(launch).not.toHaveBeenCalled();
      expect(fs.existsSync(`${run}.moved`)).toBe(true); // the fault fired and swapped
      expect(snapshot()).toEqual(before);
      expect(routeFilesIn(victim)).toEqual([]);
      if (kind === "replacement") expect(fs.readdirSync(run!)).toEqual([]);
      expect(routeFilesIn(`${run}.moved`)).toEqual([]);
      expect(fs.existsSync(central!)).toBe(true);
      expect(fs.existsSync(centralPending!)).toBe(false);
      expect(ledgerText(ledger)).not.toContain("\"outcome\"");
    });
  }

  it("counterexample: the same ledger fault without a swap retries and commits the failed outcome", async () => {
    ensureTaskControlRoot();
    const { ledger, runRoot } = fixture();
    let run: string | undefined;
    managerRunTestHooks.afterRunAllocation = (directory) => { run = directory; };
    let faulted = false;
    const open = ledgerFault(ledger, () => { faulted = true; });
    const manager = managerFor(runRoot, async () => { throw new Error("model preparation failed"); });
    const error = await manager.run({ task: "prep fails", transport: "process", ...routed(ledger) } as never).then(() => undefined, (caught: unknown) => caught);
    open.mockRestore();
    expect(error).toBeInstanceOf(Error);
    expect((error as { code?: string }).code).not.toBe("FABRIC_RUN_IDENTITY_REFUSED");
    expect(faulted).toBe(true);
    expect(fs.existsSync(`${run}.moved`)).toBe(false);
    expect(fs.existsSync(routeControlFile(run!, PENDING))).toBe(false);
    expect(fs.existsSync(routeControlFile(run!, RECEIPT))).toBe(true);
    expect(routeFilesIn(run!)).toEqual([]);
    expect(ledgerText(ledger)).toMatch(/\"outcome\"/);
  });

  it("permits a normal routed spawn: receipt and session land only in central route/",  async () => {
    ensureTaskControlRoot();
    const { ledger, runRoot } = fixture();
    let run: string | undefined;
    managerRunTestHooks.afterRunAllocation = (directory) => { run = directory; };
    const manager = managerFor(runRoot);
    const result = await manager.run({ task: "routed spawn", transport: "process", ...routed(ledger) } as never);
    expect(result.status).toBe("completed");
    expect(run).toBe(path.join(runRoot, result.id));
    const receipt = JSON.parse(fs.readFileSync(routeControlFile(run!, RECEIPT), "utf8")) as { runId: string };
    expect(receipt.runId).toBe(result.id);
    const sessionFile = routeControlFile(run!, SESSION);
    const session = JSON.parse(fs.readFileSync(sessionFile, "utf8").split("\n")[0]!) as { type: string; id: string; cwd: string };
    expect(session).toMatchObject({ type: "session", id: result.id });
    expect(typeof session.cwd).toBe("string");
    if (!windowsHost) expect((fs.statSync(sessionFile).mode & 0o777)).toBe(0o600);
    expect(fs.readdirSync(path.dirname(sessionFile)).some(name => name.endsWith(".tmp"))).toBe(false);
    expect(routeFilesIn(run!)).toEqual([]);
    expect(ledgerText(ledger)).toContain("\"outcome\"");
  });

  it("permits a normal model-preparation failure: the failed outcome is recorded, no refusal", async () => {
    ensureTaskControlRoot();
    const { ledger, runRoot } = fixture();
    let run: string | undefined;
    managerRunTestHooks.afterRunAllocation = (directory) => { run = directory; };
    const manager = managerFor(runRoot, async () => { throw new Error("model preparation failed"); });
    await expect(manager.run({ task: "prep fails", transport: "process", ...routed(ledger) } as never)).rejects.toThrow(/model preparation failed/);
    // The valid outcome committed; only the central receipt remains.
    expect(fs.existsSync(routeControlFile(run!, RECEIPT))).toBe(true);
    expect(fs.existsSync(routeControlFile(run!, PENDING))).toBe(false);
    expect(routeFilesIn(run!)).toEqual([]);
    expect(ledgerText(ledger)).toMatch(/"outcome".*"status":"failed"|"status":"failed".*"outcome"/);
  });

  it("permits a normal unrouted spawn", async () => {
    ensureTaskControlRoot();
    const { runRoot } = fixture();
    const manager = managerFor(runRoot);
    const result = await manager.run({ task: "plain spawn", transport: "process" });
    expect(result.status).toBe("completed");
    expect(fs.existsSync(path.join(runRoot, result.id, RECEIPT))).toBe(false);
    expect(fs.existsSync(routeControlFile(path.join(runRoot, result.id), RECEIPT))).toBe(false);
  });

  if (windowsHost) it("win32 inherited PI_FABRIC_RUN_ROOT stays refused for routed dispatch", async () => {
    ensureTaskControlRoot();
    const { ledger, runRoot } = fixture();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    vi.stubEnv("PI_FABRIC_RUN_ROOT", runRoot);
    await expect((async () => {
      const manager = new AgentManager(process.cwd(), agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
      managers.push(manager);
      return manager.run({ task: "must not launch", transport: "process", ...routed(ledger) } as never);
    })()).rejects.toThrow();
    expect(launch).not.toHaveBeenCalled();
    expect(routeFilesIn(runRoot)).toEqual([]);
  });
});
