import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { RUN_IDENTITY_REFUSED, prepareRouteDispatch, type RunDirectoryPin } from "../src/agents/model-route.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import {
  FabricTaskControlDirectoryError, createHostTaskRun, ensureTaskControlRoot, readFinalAnswerReceipt, readTaskControlJson,
  removeTaskControlFile, routeControlFile, saveFinalAnswerReceipt, taskControlDirectory, withLegacyTaskControlDirectories, writeTaskControlJson,
} from "../src/worker/terminal-answer.js";

// Fresh grant after the 115079e62 audit (smarty-dev#11772). Real filesystem only, no mocked fs:
//  P1  A routed `bindSession` returns the published route-session path plus the file identity ({dev,ino}).
//      The real built worker (dist/worker.js, launched by a real AgentManager through ProcessTransport) gets
//      `--session-file-identity` and the central path (no manager directory-fd handoff). It refuses when the
//      central session leaf or run binding was swapped before worker validation, and every
//      swapped-in target must stay byte-for-byte unchanged. The ProcessTransport.launch spy only passes the
//      call through: it applies the parent-granted swap and then calls the real launch.
//  P2  Public control write/remove refuse every colon-bearing (Win32 NTFS alternate-data-stream) name around
//      the protected `.legacy-read.json` / `.legacy-snapshot` with FabricTaskControlDirectoryError, and the
//      protected binding stays unchanged. Ordinary colon-free names still work.
// Env-parity convention: the parent supplies an isolated XDG_STATE_HOME outside TMPDIR. Fixtures are kept.

const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usableState = !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
// Central publication is platform-neutral; Linux alone keeps the checked file-fd handoff to Pi.
const usableRoute = usableState;
const originalState = process.env.XDG_STATE_HOME;
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs(); vi.restoreAllMocks();
  process.env.XDG_STATE_HOME = originalState;
});

// ---- P1: route-session identity -----------------------------------------------------------------------

const SESSION = "route-session.jsonl";
const REFUSED = /FABRIC_(ROUTE_SESSION|RUN)_IDENTITY_REFUSED/;
const DECIMAL = /^(0|[1-9][0-9]*)$/;
type Binding = { path: string; identity: { dev: string; ino: string } };
const sameIdentity = (identity: { dev: unknown; ino: unknown }, stat: fs.BigIntStats) =>
  String(identity.dev) === String(stat.dev) && String(identity.ino) === String(stat.ino);
const isLink = (target: string) => { try { return fs.lstatSync(target).isSymbolicLink(); } catch { return false; } };
/** A real tree read without following links: identity, size, mtime, mode, and bytes or link text. Any write or swap shows up. */
const snapshot = (target: string): unknown[] => {
  if (!fs.existsSync(target) && !isLink(target)) return [[target, "absent"]];
  const stat = fs.lstatSync(target, { bigint: true });
  const id = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.mode}`;
  if (stat.isSymbolicLink()) return [[target, id, fs.readlinkSync(target)]];
  if (!stat.isDirectory()) return [[target, id, fs.readFileSync(target, "base64")]];
  return [[target, id], ...fs.readdirSync(target).sort().flatMap(name => snapshot(path.join(target, name)))];
};
/** Victims live OUTSIDE the grant: a lone file, and a directory that has its own route-session.jsonl. */
const victims = (base: string) => {
  const victimFile = path.join(base, "victim.jsonl");
  fs.writeFileSync(victimFile, "VICTIM-FILE\n", { mode: 0o600 });
  const victimDir = path.join(base, "victim-dir");
  fs.mkdirSync(victimDir, { mode: 0o700 });
  fs.writeFileSync(path.join(victimDir, SESSION), "VICTIM-DIR-SESSION\n", { mode: 0o600 });
  fs.writeFileSync(path.join(victimDir, "marker"), "VICTIM", { mode: 0o600 });
  return { victimFile, victimDir };
};

const NOFOLLOW_DIRECTORY = fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0);
/** A real held-fd pin with the manager's semantics: reopen without following links, recheck dev+ino, pass /proc/self/fd/N. */
const pinFor = (run: string): RunDirectoryPin => {
  const bound = fs.lstatSync(run, { bigint: true });
  return {
    run,
    use(operation) {
      const fd = process.platform === "win32" ? undefined : fs.openSync(run, NOFOLLOW_DIRECTORY);
      try {
        const current = fd === undefined ? fs.lstatSync(run, { bigint: true }) : fs.fstatSync(fd, { bigint: true });
        if (current.isSymbolicLink() || fs.realpathSync(run) !== path.resolve(run) || current.dev !== bound.dev || current.ino !== bound.ino) throw Object.assign(new Error("run identity changed"), { code: RUN_IDENTITY_REFUSED });
      } finally { if (fd !== undefined) fs.closeSync(fd); }
      return operation();
    },
  };
};
const decisionFor = () => {
  // Pinned to a model the fake Pi peer advertises, so the positive run completes through the real worker.
  const pin = { model: "openai-codex/gpt-5.6-sol", effort: "high" };
  return { decisionId: `d${Date.now()}${Math.random()}`, mode: "shadow", routeClass: "bounded-lookup", parentSessionId: "parent", pin, candidates: [pin],
    shadowChoice: pin, confidence: null, probability: null, reasonCode: "shadow-choice", latencyMs: 0, ...pin };
};
const bindFixture = () => {
  stateCase();
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "route-session-identity-")));
  const run = path.join(base, "granted", "run");
  createHostTaskRun(run);
  return { base, run, ledger: path.join(base, "ledger.jsonl"), cwd: path.join(base, "worktree") };
};
const expectBinding = (bound: Binding, run: string, cwd: string) => {
  expect(bound).toMatchObject({ path: routeControlFile(run, SESSION), identity: { dev: expect.stringMatching(DECIMAL), ino: expect.stringMatching(DECIMAL) } });
  const stat = fs.lstatSync(bound.path, { bigint: true });
  expect(stat.isFile() && !stat.isSymbolicLink()).toBe(true);
  expect(stat.nlink).toBe(1n);
  expect(Number(stat.mode & 0o777n)).toBe(0o600);
  expect(sameIdentity(bound.identity, stat), "identity is the published inode, not the temp's or the directory's").toBe(true);
  const header = JSON.parse(fs.readFileSync(bound.path, "utf8").split("\n")[0]!) as Record<string, unknown>;
  expect(header).toMatchObject({ type: "session", id: "child-id", cwd });
  expect(fs.readdirSync(path.dirname(bound.path)).some(name => name.endsWith(".tmp"))).toBe(false);
  expect(routeFilesIn(run)).toEqual([]);
};
const routeFilesIn = (root: string): string[] => {
  if (!fs.existsSync(root)) return [];
  return fs.readdirSync(root, { withFileTypes: true }).flatMap(entry => {
    const full = path.join(root, entry.name);
    return [...(/route-(dispatch-receipt|session)|pending-route-outcome/.test(entry.name) ? [full] : []), ...(entry.isDirectory() && !entry.isSymbolicLink() ? routeFilesIn(full) : [])];
  });
};

describe.skipIf(!usableRoute)("P1: bindSession returns the routed session path plus its file identity", () => {
  it("pinned bind: path and decimal {dev,ino} of the published inode", () => {
    const f = bindFixture();
    const dispatch = prepareRouteDispatch(decisionFor() as never, undefined, f.run, "child-id", { ledger: f.ledger, decisionRecorded: true, runPin: pinFor(f.run) });
    const bound = dispatch.bindSession(f.cwd) as Binding;
    expectBinding(bound, f.run, f.cwd);
  });

  it("counterexample: an unpinned direct bind still returns the written file's identity", () => {
    const f = bindFixture();
    const dispatch = prepareRouteDispatch(decisionFor() as never, undefined, f.run, "child-id", { ledger: f.ledger, decisionRecorded: true });
    const bound = dispatch.bindSession(f.cwd) as Binding;
    expectBinding(bound, f.run, f.cwd);
  });
});

// Real worker, fake Pi peer. The peer acts like Pi: it opens the `--session` path it was given and appends to
// it. It records where the append landed, then hands control to the shared fake model peer.
const WORKER = path.resolve("dist/worker.js");
const FAKE_PI_ROUTE = path.resolve("tests/fixtures/fake-pi-route.mjs");
const FAKE_PI_SESSION = `#!/usr/bin/env node
import fs from "node:fs";
const argv = process.argv.slice(2);
const at = argv.indexOf("--session");
const file = at >= 0 ? argv[at + 1] : undefined;
let record = { type: "fake_pi_session", file: file ?? null };
const leaf = process.env.FAKE_PI_LEAF_SWAP;
if (file && leaf) {
  fs.renameSync(leaf, leaf + ".aside");
  fs.writeFileSync(leaf, "FORGED-SESSION\\n", { mode: 0o600, flag: "wx" });
}
if (file) {
  try {
    fs.appendFileSync(file, JSON.stringify({ type: "fake_pi_session_probe" }) + "\\n");
    const stat = fs.statSync(file, { bigint: true });
    record = { ...record, dev: String(stat.dev), ino: String(stat.ino) };
  } catch (error) { record = { ...record, error: error.code ?? String(error) }; }
}
if (process.env.FAKE_SESSION_PROBE) fs.appendFileSync(process.env.FAKE_SESSION_PROBE, JSON.stringify(record) + "\\n");
await import(${JSON.stringify(pathToFileURL(FAKE_PI_ROUTE).href)});
`;
type SwapContext = { run: string; victimFile: string; victimDir: string };
type Swap = { kind: string; scope: "leaf" | "directory" | "planted"; apply: (context: SwapContext, file: string) => { target: string } };
/** Parent-granted mutations after bindSession and before the worker validates. `target` is what the swapped name now points at. */
const swaps: Swap[] = [
  { kind: "session file replaced by a symlink to a victim file outside the grant", scope: "leaf", apply: (c, file) => {
    fs.renameSync(file, `${file}.moved`); fs.symlinkSync(c.victimFile, file, "file"); return { target: c.victimFile };
  } },
  { kind: "session file renamed aside and replaced by a fresh regular file", scope: "leaf", apply: (_c, file) => {
    fs.renameSync(file, `${file}.moved`); fs.writeFileSync(file, "ATTACKER-SESSION\n", { mode: 0o600, flag: "wx" }); return { target: file };
  } },
  { kind: "session file swapped by renaming a pre-staged sibling over it", scope: "leaf", apply: (_c, file) => {
    const staged = path.join(path.dirname(file), ".staged");
    fs.writeFileSync(staged, "STAGED-SESSION\n", { mode: 0o600, flag: "wx" });
    fs.linkSync(file, `${file}.moved`); fs.renameSync(staged, file); return { target: file };
  } },
  { kind: "run directory replaced by a symlink to a victim directory that has a session name", scope: "directory", apply: (c) => {
    fs.renameSync(c.run, `${c.run}.moved`); fs.symlinkSync(c.victimDir, c.run, process.platform === "win32" ? "junction" : "dir"); return { target: path.join(c.victimDir, SESSION) };
  } },
  { kind: "run directory replaced by a real directory holding a fresh session file", scope: "directory", apply: (c) => {
    fs.renameSync(c.run, `${c.run}.moved`); fs.mkdirSync(c.run, { mode: 0o700 });
    fs.writeFileSync(path.join(c.run, SESSION), "REPLACEMENT-DIR-SESSION\n", { mode: 0o600 }); return { target: path.join(c.run, SESSION) };
  } },
];
type Launch = Parameters<ProcessTransport["launch"]>[0];
const workerFixture = () => {
  stateCase();
  ensureTaskControlRoot();
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "route-session-worker-")));
  const { victimFile, victimDir } = victims(base);
  const fakePi = path.join(base, "fake-pi-session.mjs");
  fs.writeFileSync(fakePi, FAKE_PI_SESSION, { mode: 0o700 });
  const scenario = path.join(base, "scenario");
  fs.writeFileSync(scenario, "success", { mode: 0o600 });
  const probe = path.join(base, "session-probe.jsonl");
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
  vi.stubEnv("FAKE_MODEL_SCENARIO", scenario);
  vi.stubEnv("FAKE_SESSION_PROBE", probe);
  const manager = new AgentManager(base, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 20_000, retainRuns: true }, {
    workerPath: WORKER, piBinary: fakePi, runRoot: path.join(base, "granted", "runs"), fullCodeMode: false,
  });
  managers.push(manager);
  const decision = decisionFor();
  const request = { task: "harmless lookup", transport: "process", extensions: false, model: decision.model, thinking: decision.effort,
    routeDecision: decision, routeRecord: { ledger: path.join(base, "ledger.jsonl"), decisionRecorded: true } };
  const probes = () => fs.existsSync(probe)
    ? fs.readFileSync(probe, "utf8").trim().split("\n").map(line => JSON.parse(line) as { file: string | null; dev?: string; ino?: string; error?: string })
    : [];
  return { base, victimFile, victimDir, manager, request, probes };
};
type Seen = { args: string[]; run: string; sessionFile: string; identityArgument: string | undefined; directoryFdArgument: string | undefined; inheritFds: number[] | undefined;
  bound: fs.BigIntStats; bytes: Buffer; target?: string; watched?: string[]; before?: unknown[] };
/**
 * Pass-through launch seam: it records the real launch request, applies the swap (if any) after the manager's
 * bind and before the worker starts, snapshots the victims, and then calls the real ProcessTransport.launch.
 */
const interceptLaunch = (f: ReturnType<typeof workerFixture>, swap?: Swap, writer: "pi" | "claude" = "pi", leafSwap = false) => {
  const seen: { value?: Seen } = {};
  const original = ProcessTransport.prototype.launch;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request: Launch) {
    const args = request.workerArguments;
    if (writer === "claude") {
      args[args.indexOf("--runner") + 1] = "claude";
      args[args.indexOf("--claude-binary") + 1] = path.resolve("tests/fixtures/fake-claude.mjs");
    }
    const value = (flag: string) => { const index = args.indexOf(flag); return index >= 0 ? args[index + 1] : undefined; };
    const sessionFile = value("--session-file")!;
    expect(sessionFile, "routed launch carries --session-file").toBeTruthy();
    const run = path.dirname(value("--status-file")!);
    expect(sessionFile).toBe(routeControlFile(run, SESSION));
    expect(routeFilesIn(run)).toEqual([]);
    if (leafSwap) vi.stubEnv("FAKE_PI_LEAF_SWAP", sessionFile);
    const record: Seen = { args: [...args], run, sessionFile, identityArgument: value("--session-file-identity"), directoryFdArgument: value("--session-directory-fd"),
      inheritFds: (request as Launch & { inheritFds?: number[] }).inheritFds, bound: fs.lstatSync(sessionFile, { bigint: true }), bytes: fs.readFileSync(sessionFile) };
    seen.value = record;
    if (swap) {
      const { target } = swap.apply({ run, victimFile: f.victimFile, victimDir: f.victimDir }, sessionFile);
      record.target = target;
      record.watched = [target, f.victimFile, path.join(f.victimDir, SESSION), path.join(f.victimDir, "marker"), `${sessionFile}.moved`, path.join(`${run}.moved`, SESSION)];
      record.before = record.watched.map(snapshot);
    }
    return original.call(this, request);
  });
  return seen;
};
const settle = async (f: ReturnType<typeof workerFixture>) => {
  const outcome = await f.manager.run(f.request as never).then(result => ({ result, error: undefined }), (error: unknown) => ({ result: undefined, error }));
  const text = outcome.error
    ? `${String((outcome.error as { code?: unknown }).code ?? "")} ${String((outcome.error as Error).message ?? outcome.error)}`
    : `${outcome.result!.error ?? ""}\n${outcome.result!.stderr ?? ""}`;
  return { ...outcome, text };
};
/** Where the original bound session now lives: renamed aside, or inside the moved run directory. */
const movedOriginal = (sessionFile: string) =>
  fs.existsSync(`${sessionFile}.moved`) ? `${sessionFile}.moved` : sessionFile;

describe.skipIf(!usableRoute || !usableState || !fs.existsSync(WORKER))("P1: the real built worker revalidates the bound route session before use", () => {
  it("counterexample: an unswapped routed session passes worker validation and the runner appends to the bound inode", { timeout: 60_000 }, async () => {
    const f = workerFixture();
    const seen = interceptLaunch(f);
    const { result, error, text } = await settle(f);
    expect(error, text).toBeUndefined();
    expect(result!.status, text).toBe("completed");
    const launch = seen.value!;
    // The worker opens central route/ itself; the manager only sends identity.
    expect(JSON.parse(launch.identityArgument!)).toEqual({ dev: String(launch.bound.dev), ino: String(launch.bound.ino) });
    expect(launch.directoryFdArgument).toBeUndefined();
    expect(launch.inheritFds ?? []).toEqual([]);
    expect(routeFilesIn(launch.run)).toEqual([]);
    // The runner's append landed in the bound inode, after the intact header.
    const records = f.probes();
    expect(records).toHaveLength(1);
    expect(records[0]!.error).toBeUndefined();
    expect(sameIdentity(records[0]! as { dev: string; ino: string }, launch.bound)).toBe(true);
    const after = fs.readFileSync(launch.sessionFile);
    expect(after.subarray(0, launch.bytes.length).equals(launch.bytes), "header preserved (append, not truncate)").toBe(true);
    expect(after.toString("utf8")).toContain("fake_pi_session_probe");
    expect(sameIdentity({ dev: launch.bound.dev, ino: launch.bound.ino }, fs.lstatSync(launch.sessionFile, { bigint: true }))).toBe(true);
  });

  it("Claude counterexample: the checked session fd receives conversation writes in the original inode", { timeout: 60_000 }, async () => {
    const f = workerFixture();
    const seen = interceptLaunch(f, undefined, "claude");
    const { result, error, text } = await settle(f);
    expect(error, text).toBeUndefined();
    expect(result!.status, text).toBe("completed");
    const launch = seen.value!;
    const after = fs.readFileSync(launch.sessionFile);
    expect(after.subarray(0, launch.bytes.length).equals(launch.bytes)).toBe(true);
    expect(after.length).toBeGreaterThan(launch.bytes.length);
    expect(sameIdentity({ dev: launch.bound.dev, ino: launch.bound.ino }, fs.lstatSync(launch.sessionFile, { bigint: true }))).toBe(true);
  });

  it("a flag-shaped plain agent name never opts into the route identity protocol", { timeout: 60_000 }, async () => {
    const f = workerFixture();
    const { routeDecision: _decision, routeRecord: _record, ...plain } = f.request;
    const result = await f.manager.run({ ...plain, name: "--session-file-identity" } as never);
    expect(result.status, result.error).toBe("completed");
    expect(f.probes()).toHaveLength(1);
  });

  it("startup retry checks identity for every launch without manager directory fds", { timeout: 60_000 }, async () => {
    stateCase();
    const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "route-session-retry-")));
    const manager = new AgentManager(base, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], retainRuns: true }, {
      workerPath: path.resolve("tests/fixtures/fake-worker-startup-retry.mjs"), runRoot: path.join(base, "runs"), fullCodeMode: false,
    });
    managers.push(manager);
    const original = ProcessTransport.prototype.launch;
    let routedLaunches = 0;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request: Launch) {
      expect(request.inheritFds).toBeUndefined();
      expect(request.workerArguments).not.toContain("--session-directory-fd");
      if (request.workerArguments.includes("--session-file-identity")) routedLaunches++;
      return original.call(this, request);
    });
    const result = await manager.run({ task: "Recover startup", transport: "process", routeDecision: decisionFor() } as never);
    expect(result.status).toBe("completed");
    expect(routedLaunches).toBe(2);
    expect((await manager.run({ task: "plain", transport: "process" })).status).toBe("completed");
  });

  for (const writer of ["pi", "claude"] as const) for (const swap of swaps.filter(candidate => candidate.scope === "leaf")) {
    it(`${writer}: refuses (typed) before the runner starts and leaves the swapped target unchanged: ${swap.kind}`, { timeout: 60_000 }, async () => {
      const f = workerFixture();
      const seen = interceptLaunch(f, swap, writer);
      const { result, text } = await settle(f);
      const launch = seen.value!;
      expect(launch.target, "the swap ran at the launch seam").toBeDefined();
      expect(result?.status, text).not.toBe("completed");
      expect(text).toMatch(REFUSED);
      // The runner never started, so nothing opened or appended to the swapped name.
      expect(f.probes()).toEqual([]);
      expect(launch.watched!.map(snapshot), "swapped target, victims and the moved original are byte-for-byte unchanged").toEqual(launch.before);
      expect(fs.readFileSync(f.victimFile, "utf8")).toBe("VICTIM-FILE\n");
      expect(fs.readFileSync(path.join(f.victimDir, SESSION), "utf8")).toBe("VICTIM-DIR-SESSION\n");
      const original = movedOriginal(launch.sessionFile);
      expect(fs.readFileSync(original).equals(launch.bytes)).toBe(true);
      expect(sameIdentity({ dev: launch.bound.dev, ino: launch.bound.ino }, fs.lstatSync(original, { bigint: true }))).toBe(true);
    });
  }

  // A pre-open C swap invalidates the strict central binding: typed refusal, no runner start.
  for (const writer of ["pi", "claude"] as const) for (const swap of swaps.filter(candidate => candidate.scope === "directory")) {
    it(`${writer}: never writes the session through a swapped run directory: ${swap.kind}`, { timeout: 60_000 }, async () => {
      const f = workerFixture();
      const seen = interceptLaunch(f, swap, writer);
      const { result, text } = await settle(f);
      const launch = seen.value!;
      expect(launch.target).toBeDefined();
      const sessionScoped = [launch.target!, f.victimFile, path.join(f.victimDir, SESSION)];
      const beforeScoped = launch.watched!.map(snapshot).filter((_entry, index) => index === 0 || index === 1 || index === 2);
      expect(sessionScoped.map(snapshot), "swapped-in session targets unchanged").toEqual(beforeScoped);
      for (const record of f.probes()) {
        expect(record.error, `runner session open failed instead of using the bound inode: ${JSON.stringify(record)}`).toBeUndefined();
        expect(sameIdentity(record as { dev: string; ino: string }, launch.bound), `runner appended outside the bound inode: ${JSON.stringify(record)}`).toBe(true);
      }
      expect(result?.status, text).not.toBe("completed");
      expect(text).toMatch(/FABRIC_(ROUTE_SESSION|RUN)_IDENTITY_REFUSED|RouteSessionIdentityRefused/);
      expect(f.probes()).toEqual([]);
      const original = movedOriginal(launch.sessionFile);
      expect(fs.readFileSync(original).subarray(0, launch.bytes.length).equals(launch.bytes)).toBe(true);
      expect(sameIdentity({ dev: launch.bound.dev, ino: launch.bound.ino }, fs.lstatSync(original, { bigint: true }))).toBe(true);
    });
  }
});

// Linux keeps the verified session-file fd through Pi's open. W-1 explicitly waives this on macOS/Windows.
describe.skipIf(!usableRoute || !fs.existsSync(WORKER))("P1b: central route session handoff", () => {
  it.skipIf(process.platform !== "linux")("post-verification leaf swap: original inode receives Pi marker; forged leaf unchanged (waiver W-1 (design §3); Pi gets the plain central path off Linux)", { timeout: 60_000 }, async () => {
    const f = workerFixture();
    const seen = interceptLaunch(f, undefined, "pi", true);
    const { text } = await settle(f);
    const launch = seen.value!;
    const aside = `${launch.sessionFile}.aside`;
    expect(fs.existsSync(aside), `fake Pi performed the swap: ${text}`).toBe(true);
    expect(sameIdentity({ dev: launch.bound.dev, ino: launch.bound.ino }, fs.lstatSync(aside, { bigint: true }))).toBe(true);
    const original = fs.readFileSync(aside, "utf8");
    expect(original.startsWith(launch.bytes.toString("utf8"))).toBe(true);
    expect(original).toContain("fake_pi_session_probe");
    expect(fs.readFileSync(launch.sessionFile, "utf8")).toBe("FORGED-SESSION\n");
    const records = f.probes();
    expect(records).toHaveLength(1);
    expect(sameIdentity(records[0]! as { dev: string; ino: string }, launch.bound)).toBe(true);
    expect(routeFilesIn(launch.run)).toEqual([]);
  });
});

// ---- P2: colon-bearing (NTFS ADS) public control names -----------------------------------------------

const stateCase = () => { process.env.XDG_STATE_HOME = fs.mkdtempSync(path.join(isolatedState!, "session-identity-case-")); };
const freshRun = () => path.join(fs.mkdtempSync(path.join(os.tmpdir(), "task-control-ads-")), "run");
const tree = (target: string): unknown[] => {
  if (!fs.existsSync(target)) return [[target, "absent"]];
  const stat = fs.lstatSync(target, { bigint: true });
  if (!stat.isDirectory()) return [[target, String(stat.ino), Number(stat.mode), String(stat.mtimeNs), fs.readFileSync(target, "base64")]];
  return [[target, String(stat.ino), Number(stat.mode)], ...fs.readdirSync(target).sort().flatMap(name => tree(path.join(target, name)))];
};
const streamNames = [
  ".legacy-read.json::$DATA", ".legacy-read.json:$DATA", ".legacy-read.json:", ".legacy-read.json:evil",
  ".legacy-read.json:evil:$DATA", ".LEGACY-READ.JSON::$DATA", ".legacy-read.json.::$DATA", ".legacy-read.json ::$DATA",
  ".legacy-snapshot::$DATA", ".legacy-snapshot:$DATA", ".legacy-snapshot::$INDEX_ALLOCATION", ".legacy-snapshot:$I30:$INDEX_ALLOCATION",
  ".Legacy-Snapshot::$DATA", ".legacy-snapshot. ::$DATA",
  // Nested under a stream-qualified protected component, with both slash styles.
  ".legacy-snapshot::$DATA/in-run/final-answer.json", ".legacy-snapshot::$INDEX_ALLOCATION\\in-run\\final-answer.json",
  ".legacy-read.json::$DATA/x.json",
  // A stream on an ordinary name, and ordinary prefixes that name the protected stream.
  "notes.json::$DATA", "notes.json:.legacy-read.json", "terminal-controls/a.json::$DATA", "terminal-controls:.legacy-snapshot/a.json",
];
const hostRun = () => {
  stateCase();
  const run = freshRun();
  createHostTaskRun(run);
  writeTaskControlJson(run, "notes.json", { keep: true }); // an ordinary existing control that the stream names alias
  const central = taskControlDirectory(run), bindingFile = path.join(central, ".legacy-read.json");
  const binding = { bytes: fs.readFileSync(bindingFile), ino: fs.lstatSync(bindingFile, { bigint: true }).ino };
  return { run, central, bindingFile, binding, before: tree(central) };
};
const expectUnchanged = (state: ReturnType<typeof hostRun>, context: string) => {
  expect(fs.readFileSync(state.bindingFile).equals(state.binding.bytes), `${context}: binding bytes`).toBe(true);
  expect(fs.lstatSync(state.bindingFile, { bigint: true }).ino, `${context}: binding inode`).toBe(state.binding.ino);
  expect(fs.readdirSync(state.central).some(name => name.includes(":")), `${context}: no colon entry created`).toBe(false);
  expect(tree(state.central), `${context}: protected namespace tree`).toEqual(state.before);
};
const operations = [
  { name: "writeTaskControlJson", call: (run: string, relative: string) => writeTaskControlJson(run, relative, { forged: true }) },
  { name: "removeTaskControlFile", call: (run: string, relative: string) => removeTaskControlFile(run, relative) },
];

describe.skipIf(!usableState)("P2: colon-bearing task control names (NTFS alternate data streams) are refused", () => {
  for (const operation of operations) {
    it(`${operation.name} refuses every stream-qualified name before mutation; binding unchanged`, () => {
      const state = hostRun();
      for (const relative of streamNames) {
        const context = `${operation.name}(${JSON.stringify(relative)})`;
        let thrown: unknown;
        try { operation.call(state.run, relative); } catch (error) { thrown = error; }
        expect(thrown, `${context}: must refuse`).toBeInstanceOf(FabricTaskControlDirectoryError);
        expectUnchanged(state, context);
      }
      expect(readTaskControlJson(state.run, "notes.json")).toEqual({ keep: true });
      expect(withLegacyTaskControlDirectories(state.run, sources => sources)).toEqual([]);
    });
  }

  it("counterexample: ordinary colon-free names write, read and remove; binding stays valid and usable", () => {
    const state = hostRun();
    for (const [relative, value] of [
      ["ordinary.json", { ok: 1 }], ["terminal-controls/a.json", { ok: 2 }], ["terminal-notices/b.json", { ok: 3 }],
      ["legacy-read.json", { ok: 4 }], ["legacy-snapshot-DATA.json", { ok: 5 }], ["$DATA.json", { ok: 6 }],
    ] as const) {
      writeTaskControlJson(state.run, relative, value);
      expect(readTaskControlJson(state.run, relative), relative).toEqual(value);
    }
    removeTaskControlFile(state.run, "ordinary.json");
    expect(readTaskControlJson(state.run, "ordinary.json")).toBeUndefined();
    expect(readTaskControlJson(state.run, "notes.json")).toEqual({ keep: true });
    expect(fs.readFileSync(state.bindingFile).equals(state.binding.bytes)).toBe(true);
    expect(fs.lstatSync(state.bindingFile, { bigint: true }).ino).toBe(state.binding.ino);
    expect(fs.readdirSync(state.central)).not.toContain(".legacy-snapshot");
    expect(withLegacyTaskControlDirectories(state.run, sources => sources)).toEqual([]);
    expect(saveFinalAnswerReceipt(state.run, "run", "done").text).toBe("done");
    expect(readFinalAnswerReceipt(state.run, "run")?.text).toBe("done");
  });
});
