import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunResult, AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import { HOST_POLICY_PATH } from "../src/host-policy.js";

// expected-skip: non-Linux or unavailable user-systemd/cgroup-v2: optional collection coverage only.
// Actor coverage is opt-in, hosted-only; REQUIRED=1 never skips missing capabilities/preconditions.
// Keep reason: #1630 / #16970 unsampled daemon fences a genuine terminal actor and its FIFO successors.

// smarty-dev#8953 / #6829: host-backed E2E of agents.processSlice. Real systemd --user scopes with
// --collect remove an empty scope's cgroup, often before Fabric reads its retained cgroup.events fd
// (ENODEV). No scope, cgroup or systemd mocks: only the root ownership of the test policy file is
// simulated, exactly as tests/host-policy.test.ts does, and only while the policy loads.
const RUNS = 20;
const MAX_BATCHES = 10;
// No recorded/fake GREEN: these modes really launch/kill test-owned native scopes. Do not run locally.
const mode = process.env.FABRIC_SCOPE_E2E_MODE ?? "collection";
const actorMode = mode !== "collection";
const t13 = mode.startsWith("actor-t13-");
const red = mode.endsWith("-red");
if (!["collection", "actor-red", "actor-green", "actor-t13-red", "actor-t13-green"].includes(mode)) {
  throw new Error("Unknown FABRIC_SCOPE_E2E_MODE");
}
if (actorMode) {
  if (process.env.FABRIC_SCOPE_E2E_NATIVE_GRANTED !== "1") throw new Error("P2_PRECONDITION: separate lead native grant required before scope probes");
  // Vitest's generic fixture config uses 022; this opt-in native fixture keeps its requested private 077.
  process.umask(0o077);
}
const skipReason = ((): string | undefined => {
  if (process.platform !== "linux") return "not Linux";
  try { if (fs.statfsSync("/sys/fs/cgroup").type !== 0x63677270) return "/sys/fs/cgroup is not cgroup v2"; }
  catch { return "no /sys/fs/cgroup"; }
  const probe = spawnSync("systemd-run", ["--user", "--scope", "--quiet", "--collect", "true"], { encoding: "utf8", timeout: 10_000 });
  if (probe.error || probe.status !== 0) return `no usable systemd-run --user (${probe.error?.message ?? probe.stderr.trim() ?? probe.status})`;
  return undefined;
})();
// The ubuntu CI leg starts a user systemd manager and sets this: there a missing prerequisite is a failure.
const required = process.env.FABRIC_SCOPE_E2E_REQUIRED === "1";
if (skipReason && !required) console.log(`process-slice-scope-e2e skipped: ${skipReason}`);

const privateSlice = `fabric-e2e-${randomBytes(4).toString("hex")}.slice`;
const slice = t13 ? "batch.slice" : privateSlice;
const temps: string[] = [];
const temp = (label: string): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), `scope-e2e-${label}-`)); temps.push(dir); return dir; };
afterAll(() => {
  // NEVER stop batch.slice, enumerate its units, or adopt a production unit. Actor teardown below
  // uses only directory objects captured from this fixture's still-live worker incarnations.
  if (!actorMode && !skipReason) spawnSync("systemctl", ["--user", "stop", privateSlice], { timeout: 10_000 });
  if (!HOST_POLICY_PATH.startsWith(`${os.tmpdir()}${path.sep}`)) throw new Error("refusing non-temp policy deletion");
  fs.rmSync(path.dirname(HOST_POLICY_PATH), { recursive: true, force: true });
  for (const dir of temps) fs.rmSync(dir, { recursive: true, force: true });
});

// The policy loader requires a root-owned file and ancestors; simulate ownership of that path only.
const loadRootPolicySlice = async (): Promise<string | undefined> => {
  if (!HOST_POLICY_PATH.startsWith(`${os.tmpdir()}${path.sep}`)) throw new Error("test policy path is not private");
  fs.mkdirSync(path.dirname(HOST_POLICY_PATH), { recursive: true, mode: 0o700 });
  fs.writeFileSync(HOST_POLICY_PATH, JSON.stringify({ agents: { processSlice: slice } }), { mode: 0o600 });
  const ancestors = new Set<string>();
  for (let dir = path.dirname(HOST_POLICY_PATH); ; dir = path.dirname(dir)) { ancestors.add(dir); if (path.dirname(dir) === dir) break; }
  const as = (stat: fs.Stats, changes: Partial<fs.Stats>): fs.Stats => Object.assign(Object.create(Object.getPrototypeOf(stat)) as fs.Stats, stat, changes);
  const lstat = fs.lstatSync, open = fs.openSync, fstat = fs.fstatSync;
  let policyFd: number | undefined;
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const stat = (lstat as (...args: unknown[]) => fs.Stats)(file, ...rest);
    const name = String(file);
    return name === HOST_POLICY_PATH || ancestors.has(name) ? as(stat, { uid: 0, ...(ancestors.has(name) ? { mode: stat.mode & ~0o022 } : {}) }) : stat;
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const fd = (open as (...args: unknown[]) => number)(file, ...rest);
    if (String(file) === HOST_POLICY_PATH) policyFd = fd;
    return fd;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, ...rest: unknown[]) => {
    const stat = (fstat as (...args: unknown[]) => fs.Stats)(fd, ...rest);
    return fd === policyFd ? as(stat, { uid: 0 }) : stat;
  }) as typeof fs.fstatSync);
  try {
    const { loadFabricConfig } = await import("../src/config.js");
    const cwd = temp("project");
    fs.mkdirSync(path.join(cwd, ".pi"));
    return loadFabricConfig({ cwd, agentDir: temp("agent"), projectTrusted: true }).agents.processSlice;
  } finally { vi.restoreAllMocks(); }
};

const unresolvedMarkers = (dir: string): string[] => fs.readdirSync(dir, { recursive: true }).map(String)
  .filter(file => path.basename(file) === "unresolved-worker.json");

describe.skipIf(actorMode || (skipReason !== undefined && !required))("agents.processSlice scopes on a real systemd --user (smarty-dev#8953)", () => {
  it(`settles ${RUNS} quick process runs whose empty scopes systemd removes`, async () => {
    if (skipReason) throw new Error(`FABRIC_SCOPE_E2E_REQUIRED=1 but the scope E2E cannot run: ${skipReason}`);
    const processSlice = await loadRootPolicySlice();
    expect(processSlice).toBe(slice); // from the root policy, not agent/project config (#738)
    const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
    const runRoot = temp("runs");
    const manager = new AgentManager(temp("cwd"), { ...DEFAULT_FABRIC_CONFIG.agents, processSlice: slice, retainRuns: true, budgetUsd: 0,
      maxConcurrent: 4 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot, preparePiModel: async model => model });
    // Observe (never alter) the retained cgroup.events reads. Fabric opens that file with fs.openSync
    // (process-utils.ts armTreeObservation); track exactly those descriptors until they close.
    let enodev = 0, scopedReads = 0;
    const eventsFds = new Set<number>();
    const openSync = fs.openSync, closeSync = fs.closeSync, readSync = fs.readSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      const fd = (openSync as (...values: unknown[]) => number)(file, ...rest);
      if (String(file).endsWith("/cgroup.events")) eventsFds.add(fd);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "closeSync").mockImplementation(((fd: number) => {
      eventsFds.delete(fd);
      return closeSync(fd);
    }) as typeof fs.closeSync);
    vi.spyOn(fs, "readSync").mockImplementation(((...args: unknown[]) => {
      const events = eventsFds.has(args[0] as number);
      if (events) scopedReads++;
      try { return (readSync as (...values: unknown[]) => number)(...args); }
      catch (error) { if (events && (error as NodeJS.ErrnoException).code === "ENODEV") enodev++; throw error; }
    }) as typeof fs.readSync);
    try {
      // The removal beats the read in only some runs: batches of RUNS until a cgroup.events read hit ENODEV
      // (at most MAX_BATCHES). Every batch must settle in full.
      const results = [];
      for (let batch = 0; batch < MAX_BATCHES && (batch === 0 || enodev === 0); batch++) {
        const handles = [];
        for (let index = 0; index < RUNS; index++) handles.push(await manager.spawn({ task: `scope e2e ${batch}.${index}`, transport: "process" }));
        results.push(...await Promise.all(handles.map(handle => manager.wait(handle.id, { timeoutMs: 60_000 }))));
        // The tree-exit receipt (or its failure, which writes unresolved-worker.json) lands after the logical
        // result, at native close; give the last scope watcher change time to arrive.
        await new Promise(resolve => setTimeout(resolve, 3_000));
      }
      const unresolved = unresolvedMarkers(runRoot);
      const reasons = unresolved.map(file => String(JSON.parse(fs.readFileSync(path.join(runRoot, file), "utf8")).reason));
      console.log(JSON.stringify({ e2e: "process-slice-scope", kernel: os.release(), slice, runs: results.length, finished: results.length,
        completed: results.filter(result => result.status === "completed").length, enodevCgroupEvents: enodev, cgroupEventsReads: scopedReads,
        unresolved: unresolved.length, firstReason: reasons[0] }));
      expect(results.filter(result => result.status === "completed")).toHaveLength(results.length); // collected == finished
      // No run left as "execution exit unconfirmed; custody retained".
      expect(reasons).toEqual([]);
      expect(scopedReads).toBeGreaterThan(0);
      expect(enodev).toBeGreaterThanOrEqual(1); // the removal race on cgroup.events was really exercised
    } finally {
      vi.restoreAllMocks();
      await manager.close();
    }
  }, 300_000);
});

interface Birth {
  pid: number; started: string; state: string; ppid: number; pgid: number; sid: number; comm: string; cgroup: string;
}
const birth = (pid: number): Birth | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
    const cgroup = fs.readFileSync(`/proc/${pid}/cgroup`, "utf8").trim();
    if (!/^0::\/[^\n]*$/.test(cgroup) || !/^\d+$/.test(fields[19]!)) throw new Error("invalid native identity");
    return { pid, started: fields[19]!, state: fields[0]!, ppid: Number(fields[1]), pgid: Number(fields[2]), sid: Number(fields[3]),
      comm: fs.readFileSync(`/proc/${pid}/comm`, "utf8").trim(), cgroup: cgroup.slice(3) };
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return undefined;
    throw error;
  }
};
const alive = (original: Birth): boolean => {
  const current = birth(original.pid);
  return current?.started === original.started && !["Z", "X", "x"].includes(current.state);
};
// Readiness/terminal status are filesystem events, and native/tree close drive production. This bounded
// 25 ms observation safety net never controls the production census or supplies an exit receipt.
const until = async (condition: () => boolean, timeout: number, label: string): Promise<void> => {
  await vi.waitFor(() => expect(condition(), label).toBe(true), { timeout, interval: 25 });
};

// This opt-in worker lives only in temp(); existing fake-worker.mjs and collection mode are unchanged.
// Python does the actual double fork/setsid, including a dead session/group leader. No op, credential
// access, external service, raw host path in task payloads, or production observation seam is involved.
const actorWorker = (root: string): string => `
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
const root = ${JSON.stringify(root)};
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
const id = args.get('id'), task = fs.readFileSync(args.get('task-file'), 'utf8');
const first = task.includes('scope-case-first'), sentinel = task.includes('scope-case-sentinel');
const read = () => { const s = fs.readFileSync('/proc/self/stat', 'utf8'); const f = s.slice(s.lastIndexOf(')') + 2).trim().split(/\\s+/);
  return { pid: process.pid, started: f[19], cgroup: fs.readFileSync('/proc/self/cgroup', 'utf8').trim().slice(3) }; };
const write = status => { const file = args.get('status-file'); const tmp = file + '.fixture.tmp';
  fs.writeFileSync(tmp, JSON.stringify({ id, name: args.get('name'), task, runner: 'pi', transport: 'process',
    cwd: args.get('cwd'), sessionId: String(process.pid), startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0,
    usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 }, ...status })); fs.renameSync(tmp, file); };
write({ status: 'running', text: '', exitCode: null });
fs.writeFileSync(path.join(root, id + '.ready.json'), JSON.stringify(read()));
const gate = path.join(root, id + '.go');
await new Promise((resolve, reject) => {
  const watcher = fs.watch(root, () => { if (fs.existsSync(gate)) finish(); });
  const timer = setTimeout(() => finish(new Error('fixture gate expired')), 540000);
  function finish(error) { clearTimeout(timer); watcher.close(); error ? reject(error) : resolve(); }
  if (fs.existsSync(gate)) finish();
});
if (first) {
  const python = ${JSON.stringify(`import os, sys, json, ctypes, time
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
assert ctypes.CDLL(None).prctl(15, b'fv2-scope-dmn', 0, 0, 0) == 0
pid = os.getpid()
with open('/proc/self/stat') as f: fields = f.read().rsplit(')', 1)[1].split()
with open('/proc/self/cgroup') as f: cg = f.read().strip()[3:]
record = dict(pid=pid, started=fields[19], cgroup=cg)
with open(sys.argv[1] + '.tmp', 'w') as f: json.dump(record, f)
os.rename(sys.argv[1] + '.tmp', sys.argv[1])
# No inherited IPC or stdio can hold the worker's captured close open.
for fd in range(0, 256):
 try: os.close(fd)
 except OSError: pass
# Finite fail-safe, NOT an exit receipt; the host test must finish/clean up well before this.
time.sleep(600)
`)};
  const child = spawnSync('python3', ['-c', python, path.join(root, id + '.daemon.json')], { stdio: 'ignore', timeout: 5000 });
  if (child.error || child.status !== 0) throw new Error('double fork failed');
  // Parent-side launch returns before the final child writes its identity. Wait on the file event.
  const daemon = path.join(root, id + '.daemon.json');
  if (!fs.existsSync(daemon)) await new Promise((resolve, reject) => {
    const watcher = fs.watch(root, () => { if (fs.existsSync(daemon)) finish(); });
    const timer = setTimeout(() => finish(new Error('daemon identity missing')), 5000);
    function finish(error) { clearTimeout(timer); watcher.close(); error ? reject(error) : resolve(); }
    if (fs.existsSync(daemon)) finish();
  });
}
if (!sentinel) write({ status: 'completed', text: first ? 'terminal answer' : task.includes('scope-case-two') ? 'successor two' : 'successor one',
  finishedAt: Date.now(), exitCode: 0 });
// A genuine terminal file precedes primary exit. Ordinary sampled TERM, not a test signal, exits it.
process.on('SIGTERM', () => process.exit(0));
setInterval(() => {}, 1000);
`;

interface OwnedScope {
  id: string; unit: string; fd: number; dev: number; ino: number; worker: Birth; daemon?: Birth;
}
interface Trial {
  request: AgentTransportLaunch; handle: AgentTransportHandle; scope?: OwnedScope;
  stopResolved: boolean; survivedStop: boolean; native: boolean; tree: boolean; failure?: string;
}

// Check factory ancestry, NOT daemon PPID. The Main launch gate may be the real subreaper (N1).
// argv is used only for this local boolean gate, never logged or copied into evidence.
const underFactoryGate = (): boolean => {
  let pid = process.pid;
  for (let depth = 0; pid > 1 && depth < 64; depth++) {
    const info = birth(pid);
    if (!info) return false;
    const command = fs.readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0");
    if (command.some(arg => arg.endsWith("launch_attempt.py")) && command.includes("gate")) return true;
    pid = info.ppid;
  }
  return false;
};

describe.skipIf(!actorMode)("terminal uncapped actor scope real path (#1630 P2, hosted only)", () => {
  it("cleans an unsampled daemon before publishing once and draining two FIFO successors", async () => {
    // Opt-in actor modes never silently skip/pass unmet prerequisites, even without REQUIRED.
    if (skipReason) throw new Error(`P2_PRECONDITION: ${skipReason}`);
    if (t13 && (process.getuid?.() !== 1000 || process.env.FABRIC_SCOPE_E2E_FACTORY_MAIN !== "1" || !underFactoryGate())) {
      throw new Error("P2_PRECONDITION: T13 requires UID1000 Main under factory launch_attempt.py gate, with FABRIC_SCOPE_E2E_FACTORY_MAIN=1");
    }
    const python = spawnSync("python3", ["-c", "import os,ctypes; assert hasattr(os, 'fork')"], { timeout: 5_000 });
    if (python.error || python.status !== 0) throw new Error("P2_PRECONDITION: python3 fork/setsid/prctl required");
    expect(await loadRootPolicySlice()).toBe(slice);
    const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
    let qualifying = false;
    for (let batch = 0; batch < MAX_BATCHES && !qualifying; batch++) {
      const root = temp(`actor-${batch}`), runRoot = path.join(root, "runs"), meshRoot = path.join(root, "mesh");
      const workerPath = path.join(root, "scope-worker.mjs");
      fs.writeFileSync(workerPath, actorWorker(root));
      const trials = new Map<string, Trial>(), owned: OwnedScope[] = [], settled: AgentRunResult[] = [];
      const writes: Array<{ id: string; fdRelative: boolean; event: Record<string, unknown> | undefined }> = [];
      const launch = ProcessTransport.prototype.launch;
      vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function(this: ProcessTransport, request) {
        const handle = await launch.call(this, request); // exact production handle; never spread/reconstruct authority
        const trial: Trial = { request, handle, stopResolved: false, survivedStop: false, native: false, tree: false };
        trials.set(request.id, trial);
        void handle.closed?.then(() => { trial.native = true; }, error => { trial.failure = String(error); });
        void handle.treeClosed?.then(() => { trial.tree = true; }, error => { trial.failure = String(error); });
        const stop = handle.stop;
        handle.stop = async options => {
          await stop.call(handle, options); // actual sampled stop; no change to signals, sampling or receipts
          trial.stopResolved = true;
          // Read the birth ledger at THIS boundary as well; parent observation latency cannot turn a
          // sampled batch into a qualifying survivor or miss the real post-stop observation.
          const file = path.join(root, `${request.id}.daemon.json`);
          if (trial.scope && fs.existsSync(file)) {
            const saved = JSON.parse(fs.readFileSync(file, "utf8")) as Pick<Birth, "pid" | "started" | "cgroup">;
            const current = birth(saved.pid);
            if (current?.started === saved.started && current.cgroup === trial.scope.worker.cgroup && current.cgroup === saved.cgroup) {
              trial.scope.daemon = current;
              trial.survivedStop = alive(current);
            }
          }
        };
        return handle;
      });
      const flags = new Map<number, { id: string; fdRelative: boolean }>();
      const open = fs.openSync, close = fs.closeSync, write = fs.writeSync, writeFile = fs.writeFileSync;
      vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
        const fd = (open as (...args: unknown[]) => number)(file, ...rest);
        const name = String(file);
        if (name.endsWith("/cgroup.kill")) {
          const directory = fs.statSync(path.dirname(name));
          const scope = owned.find(scope => scope.dev === directory.dev && scope.ino === directory.ino);
          if (scope) flags.set(fd, { id: scope.id, fdRelative: /^\/proc\/self\/fd\/\d+\/cgroup\.kill$/.test(name) });
        }
        return fd;
      }) as typeof fs.openSync);
      vi.spyOn(fs, "closeSync").mockImplementation((fd: number) => { flags.delete(fd); return close(fd); });
      let nestedWrite = false;
      const observeWrite = <T,>(fd: unknown, operation: () => T): T => {
        const control = typeof fd === "number" ? flags.get(fd) : undefined;
        if (!control || nestedWrite) return operation();
        const trial = trials.get(control.id)!;
        const args = trial.request.workerArguments;
        const log = args[args.indexOf("--log-file") + 1]!;
        const records = fs.readFileSync(log, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>);
        const events = records.filter(record => record.type === "scope.cleanup");
        // Capture the actual pre-write log, not a synthetic production seam or post-hoc event.
        expect(events).toHaveLength(1);
        expect(trial.stopResolved).toBe(true);
        expect(trial.native).toBe(true);
        writes.push({ ...control, event: events[0] }); // count attempts too: a second rejected write is still forbidden
        nestedWrite = true;
        try { return operation(); }
        finally { nestedWrite = false; }
      };
      vi.spyOn(fs, "writeSync").mockImplementation(((...args: unknown[]) => observeWrite(args[0], () =>
        (write as (...values: unknown[]) => number)(...args))) as typeof fs.writeSync);
      vi.spyOn(fs, "writeFileSync").mockImplementation(((...args: unknown[]) => observeWrite(args[0], () =>
        (writeFile as (...values: unknown[]) => void)(...args))) as typeof fs.writeFileSync);
      const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, processSlice: slice, retainRuns: true, budgetUsd: 0,
        sessionExport: false, deniedModels: [], maxConcurrent: 2, timeoutMs: 180_000 }, {
        workerPath, runRoot, meshRoot, preparePiModel: async model => model, onSettled: result => { settled.push(result); },
      });
      const actors = new ActorManager("scope-e2e", { id: "session:scope-e2e", name: "main", kind: "main", sessionId: "scope-e2e" },
        new MeshStore(meshRoot, 65536, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, manager, () => {}, {
          actorRoot: path.join(root, "actors"), persistent: true, meshRetentionSweepPath: false, closeGraceMs: 0,
        });
      const retain = async (actorId: string): Promise<Trial> => {
        await until(() => { const id = actors.status(actorId).inFlightRun?.id;
          return !!id && trials.has(id) && fs.existsSync(path.join(root, `${id}.ready.json`)); }, 20_000, "actual actor launch readiness");
        const id = actors.status(actorId).inFlightRun!.id, trial = trials.get(id)!;
        const ready = JSON.parse(fs.readFileSync(path.join(root, `${id}.ready.json`), "utf8")) as Pick<Birth, "pid" | "started" | "cgroup">;
        const worker = birth(ready.pid);
        expect(worker?.started).toBe(ready.started);
        expect(worker?.cgroup).toBe(ready.cgroup);
        const unit = path.posix.basename(ready.cgroup);
        expect(unit).toMatch(/^fabric-scope-[A-Za-z0-9]+\.scope$/);
        expect(unit).not.toMatch(/^fabric-task-/);
        expect(path.posix.basename(path.posix.dirname(ready.cgroup))).toBe(slice);
        if (t13) expect(ready.cgroup).toContain("/user@1000.service/batch.slice/");
        expect(trial.request.workerArguments).toContain("--actor-id");
        const directory = path.resolve("/sys/fs/cgroup", `.${ready.cgroup}`);
        if (!directory.startsWith("/sys/fs/cgroup/")) throw new Error("scope escaped cgroup mount");
        const fd = open(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW);
        const held = fs.fstatSync(fd), byPath = fs.lstatSync(directory);
        const scope: OwnedScope = { id, unit, fd, dev: held.dev, ino: held.ino, worker: worker! };
        owned.push(scope); trial.scope = scope; // retain even if a subsequent prerequisite assertion fails
        expect(held.uid).toBe(process.getuid?.());
        expect([held.dev, held.ino]).toEqual([byPath.dev, byPath.ino]);
        expect(fs.statfsSync(directory).type).toBe(0x63677270);
        expect(trial.handle.cgroup).toBeUndefined(); // uncapped, not a task with renamed metadata
        expect(trial.handle.liveness).toBe("events");
        expect(trial.handle.closed).toBeDefined();
        expect(trial.handle.treeClosed).toBeDefined();
        const control = open(`/proc/self/fd/${fd}/cgroup.kill`, fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW);
        close(control); // availability only, never a fake successful kill
        return trial;
      };
      const release = (trial: Trial): void => { fs.writeFileSync(path.join(root, `${trial.request.id}.go`), "go"); };
      try {
        const sentinelActor = await actors.create({ name: "sentinel", instructions: "fixture", responseMode: "text", coalesce: false, transport: "process" });
        actors.tell(sentinelActor.id, "scope-case-sentinel");
        const sentinel = await retain(sentinelActor.id); // distinct owned sibling scope, held alive behind its gate
        const actor = await actors.create({ name: "terminal", instructions: "fixture", responseMode: "text", coalesce: false, transport: "process" });
        actors.tell(actor.id, "scope-case-first");
        const first = await retain(actor.id);
        actors.tell(actor.id, "scope-case-one");
        actors.tell(actor.id, "scope-case-two");
        await until(() => actors.status(actor.id).queued === 2, 5_000, "two FIFO successors queued before terminal verdict");
        release(first);
        const daemonFile = path.join(root, `${first.request.id}.daemon.json`);
        await until(() => fs.existsSync(daemonFile), 10_000, "double-fork identity");
        const saved = JSON.parse(fs.readFileSync(daemonFile, "utf8")) as Pick<Birth, "pid" | "started" | "cgroup">;
        const daemon = birth(saved.pid);
        // It can already have been killed if sampled; that batch is not qualifying. Never pretend it is.
        if (daemon?.started === saved.started && !["Z", "X", "x"].includes(daemon.state)) {
          expect(daemon.cgroup).toBe(first.scope!.worker.cgroup);
          expect(daemon.cgroup).toBe(saved.cgroup);
          first.scope!.daemon = daemon;
        }
        await until(() => first.stopResolved || first.failure !== undefined, 35_000, "ordinary stop resolves");
        if (first.failure) throw new Error(`P2_NATIVE_DEBT: ${first.failure}`);
        if (red) {
          if (!first.survivedStop || !first.scope!.daemon || !alive(first.scope!.daemon)) continue;
          // Bounded nonsettlement AND the same-birth survivor AFTER actual ordinary stop: no sampler seam.
          await new Promise(resolve => setTimeout(resolve, 12_000));
          expect(alive(first.scope!.daemon)).toBe(true);
          expect(first.native).toBe(true);
          expect(first.tree).toBe(false);
          expect(settled.filter(result => result.id === first.request.id)).toHaveLength(0);
          expect(actors.status(actor.id).queued).toBe(2);
          expect(trials.size).toBe(2); // sentinel + first, neither successor launched/replayed
          expect(writes).toEqual([]);
          qualifying = true;
          const survivor = birth(first.scope!.daemon.pid)!;
          const adopter = birth(survivor.ppid);
          console.info(JSON.stringify({ e2e: "P2_RED", mode, batch, kernel: os.release(), unit: first.scope!.unit,
            daemon: { pid: survivor.pid, started: survivor.started, comm: survivor.comm, state: survivor.state, ppid: survivor.ppid,
              pgid: survivor.pgid, sid: survivor.sid }, adopter: adopter ? { pid: adopter.pid, started: adopter.started, comm: adopter.comm } : null,
            ordinaryStopResolved: true, nativeClosed: first.native,
            treeClosed: first.tree, settlements: 0, queued: 2, nonsettlementMs: 12_000, factoryGate: t13 }));
          throw new Error("P2_RED: same-birth daemon alive after ordinary stop; terminal actor remains unsettled with two fenced successors");
        }
        await until(() => settled.some(result => result.id === first.request.id), 80_000, "terminal actor settles from native AND tree proof");
        const firstWrites = writes.filter(write => write.id === first.request.id);
        if (firstWrites.length === 0) {
          if (daemon && alive(daemon)) throw new Error("P2_GREEN: survivor leaked without a scope write");
          continue; // sampled batch; must obtain a pre-kill root-member record within MAX_BATCHES
        }
        expect(firstWrites).toHaveLength(1);
        expect(firstWrites[0]!.fdRelative).toBe(true);
        expect(firstWrites[0]!.event).toMatchObject({ type: "scope.cleanup", id: first.request.id,
          note: "shared per-user services may be interrupted", members: expect.arrayContaining([{ pid: saved.pid, comm: "fv2-scope-dmn" }]) });
        expect(typeof firstWrites[0]!.event!.ts).toBe("number");
        expect(first.native && first.tree).toBe(true);
        expect(first.failure).toBeUndefined();
        const remaining = birth(saved.pid);
        expect(remaining?.started === saved.started && !["Z", "X", "x"].includes(remaining.state)).toBe(false);
        expect(settled.filter(result => result.id === first.request.id)).toMatchObject([{ status: "completed", text: "terminal answer" }]);
        for (const text of ["successor one", "successor two"]) {
          const next = await retain(actor.id);
          expect(next.request.id).not.toBe(first.request.id);
          expect(settled.filter(result => result.id === first.request.id)).toHaveLength(1);
          release(next);
          await until(() => settled.some(result => result.id === next.request.id), 80_000, text);
          expect(next.native && next.tree).toBe(true);
          expect(settled.find(result => result.id === next.request.id)?.text).toBe(text);
        }
        await until(() => actors.status(actor.id).status === "idle", 5_000, "FIFO actor idle");
        expect(settled.map(result => result.text)).toEqual(["terminal answer", "successor one", "successor two"]);
        expect(writes.filter(write => write.id === first.request.id)).toHaveLength(1);
        expect(actors.messages(actor.id).filter(message => message.direction === "out").map(message => message.text))
          .toEqual(["terminal answer", "successor one", "successor two"]);
        expect(trials.size).toBe(4); // sibling + precisely three launches; no replay
        expect(alive(sentinel.scope!.worker)).toBe(true);
        expect(sentinel.native || sentinel.tree).toBe(false);
        expect(writes.filter(write => write.id === sentinel.request.id)).toHaveLength(0);
        expect(unresolvedMarkers(runRoot)).toEqual([]);
        qualifying = true;
        const adopter = daemon ? birth(daemon.ppid) : undefined;
        console.info(JSON.stringify({ e2e: "P2_GREEN", mode, batch, kernel: os.release(), unit: first.scope!.unit,
          daemon: { pid: saved.pid, started: saved.started, comm: "fv2-scope-dmn", ppid: daemon?.ppid, pgid: daemon?.pgid, sid: daemon?.sid },
          adopter: adopter ? { pid: adopter.pid, started: adopter.started, comm: adopter.comm } : null,
          ordinaryStopResolved: first.stopResolved, nativeClosed: first.native, treeClosed: first.tree, scopeWrites: firstWrites.length,
          settlements: settled.length, orderedSuccessors: 2, launches: trials.size, siblingAlive: true, factoryGate: t13 }));
      } finally {
        // Close ingress BEFORE cleanup can release the RED fence. Only these retained, test-created
        // directory incarnations are touched, including RED survivors. No scan, name stop or PID kill.
        manager.pauseForRelease();
        const closing = actors.close();
        void closing.catch(() => undefined);
        vi.restoreAllMocks(); // teardown writes are not candidate write evidence
        const teardownErrors: unknown[] = [];
        try {
          for (const scope of owned) {
            let descriptorOwned = false;
            try {
              const stat = fs.fstatSync(scope.fd);
              if (stat.dev !== scope.dev || stat.ino !== scope.ino || stat.uid !== process.getuid?.()) {
                throw new Error("owned teardown identity changed; refusing foreign/reused FD");
              }
              descriptorOwned = true;
              try {
                const events = fs.readFileSync(`/proc/self/fd/${scope.fd}/cgroup.events`, "utf8");
                if (/^populated 1$/m.test(events)) {
                  const fd = open(`/proc/self/fd/${scope.fd}/cgroup.kill`, fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW);
                  try { writeFile(fd, "1"); } finally { close(fd); }
                }
              } catch (error) {
                // Removed directory objects cannot retarget a replacement; only already-gone owned births
                // permit collection errors here. No path reopening, fallback signals or blanket slice stop.
                if (!["ENODEV", "ENOENT"].includes((error as NodeJS.ErrnoException).code ?? "") || alive(scope.worker) ||
                  (scope.daemon && alive(scope.daemon))) throw error;
              }
              await until(() => !alive(scope.worker) && (!scope.daemon || !alive(scope.daemon)), 10_000, "owned fixture teardown birth exit");
              console.info(JSON.stringify({ e2e: "P2_TEARDOWN", unit: scope.unit, dev: scope.dev, ino: scope.ino,
                worker: { pid: scope.worker.pid, started: scope.worker.started, alive: false },
                daemon: scope.daemon ? { pid: scope.daemon.pid, started: scope.daemon.started, alive: false } : null }));
            } catch (error) { teardownErrors.push(error); }
            finally { if (descriptorOwned) close(scope.fd); } // never close a detected foreign/reused number
          }
          // Admission/receipt failure before retain(): the unreleased fixture cannot have forked. Join
          // only its original ordinary handle, not a path/PID selected from a failed scope assertion.
          for (const trial of trials.values()) if (!trial.scope) {
            try { await trial.handle.stop(); await trial.handle.waitForClose?.(); }
            catch (error) { teardownErrors.push(error); }
          }
        } finally {
          try { await closing; } finally { await manager.close(); }
        }
        if (teardownErrors.length) throw new AggregateError(teardownErrors, "P2 owned fixture teardown failed; RED is NOT accepted");
      }
    }
    if (!qualifying) throw new Error(`P2_PRECONDITION: no qualifying unsampled daemon in ${MAX_BATCHES} bounded batches (mode=${mode}); NOT skipped coverage`);
  }, 600_000);
});
