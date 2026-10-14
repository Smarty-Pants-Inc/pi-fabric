import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { stopTaskCgroup, TaskCgroupStopUnconfirmedError, TaskScopeAdmissionError } from "../src/agents/transports/task-cgroup.js";
import type { AgentHandleInfo, AgentTransportHandle } from "../src/agents/types.js";
import { taskCgroupUnavailableReason } from "./helpers/task-cgroup-capability.js";

// smarty-dev#8257: real OS/process integration (custom worker, not native Pi E2E). A process task gets a capped systemd --user scope
// (fabric-task-<unique>.scope, MemoryMax + MemorySwapMax=0), and stopping it ends EVERY process
// born in that cgroup, including a setsid grandchild in its own session/PGID and an orphan that
// was reparented away from the worker before stop. No systemd, cgroup or process mocks, except
// the explicitly named older-kernel fault injections below (cgroup.kill hidden; one EPERM pid).
// The suite wrapper may strip the user bus: then this file skips, unless
// FABRIC_TASK_CGROUP_E2E_REQUIRED=1, where a missing prerequisite is a failure.
const GIB = 1024 ** 3;
const skipReason = taskCgroupUnavailableReason();
const required = process.env.FABRIC_TASK_CGROUP_E2E_REQUIRED === "1";
if (skipReason && !required) console.log(`task-cgroup e2e skipped: ${skipReason}`);

type Proc = { pid: number; sid: number; pgid: number; started: string };
const stat = (pid: number): (Proc & { state: string }) | undefined => {
  try {
    const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = text.slice(text.lastIndexOf(")") + 2).trim().split(/\s+/);
    return { pid, state: fields[0]!, pgid: Number(fields[2]), sid: Number(fields[3]), started: fields[19]! };
  } catch { return undefined; }
};
/** A born process survives only if the same birth is still live (not reaped, not a zombie). */
const survivors = (born: Proc[]): number[] => born.filter(proc => {
  const now = stat(proc.pid);
  return now !== undefined && now.started === proc.started && now.state !== "Z" && now.state !== "X";
}).map(proc => proc.pid);

const roots: string[] = [];
const born: Proc[] = [];
const managers: AgentManager[] = [];
const handles: AgentTransportHandle[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs(); // TASK_CGROUP_REPORT must not leak into later tests
  for (const handle of handles.splice(0)) await handle.stop().catch(() => undefined);
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  // Safety net, permitted by the owner: end ONLY test-born pids whose birth still matches.
  for (const pid of survivors(born.splice(0))) { try { process.kill(pid, "SIGKILL"); } catch { /* gone */ } }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const temp = (label: string): string => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), `task-cgroup-${label}-`)));
  roots.push(dir);
  return dir;
};

// The worker's FIRST statement records its cgroup: admission must precede worker import.
// It then births: a setsid shell (own session + PGID) that execs `timeout 300 sleep 300`,
// a background sleep in that session, and an orphan double-forked away from every parent.
const WORKER = `const admittedCgroup = (await import("node:fs")).readFileSync("/proc/self/cgroup", "utf8").trim();
import fs from "node:fs";
import { spawn, spawnSync } from "node:child_process";
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
const report = args.get("report") ?? process.env.TASK_CGROUP_REPORT;
// Diagnostics: a worker failure must be visible to the test, not a silent missing report.
process.on("uncaughtException", error => { fs.writeFileSync(report + ".err", String(error?.stack ?? error)); process.exit(70); });
const statusFile = args.get("status-file");
if (statusFile) fs.writeFileSync(statusFile, JSON.stringify({ id: args.get("id"), name: args.get("name"), task: "cgroup", status: "running",
  runner: args.get("runner") ?? "pi", transport: "process", sessionId: String(process.pid), cwd: args.get("cwd"), startedAt: Date.now(),
  updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", exitCode: null, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
const base = report + ".pid";
spawn("setsid", ["/bin/sh", "-c", 'sleep 300 & echo $! > "$1.bg"; ( sleep 300 & echo $! > "$1.orphan" ); echo $$ > "$1.sh"; exec timeout 300 sleep 300', "sh", base],
  { stdio: "ignore" }).unref();
// A SEPARATE-PGID child in the worker's SAME session (setpgid only, no setsid).
spawn("python3", ["-c", "import os,sys,time; os.setpgid(0, 0); open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(300)", base + ".pgid"],
  { stdio: "ignore" }).unref();
// The principal's exact native command, resolved through the inherited PATH (the task-local wrapper):
// it must EXECUTE (not be refused) and stay inside this same capped leaf.
spawn("/bin/sh", ["-c", 'systemd-run --user --scope -- timeout 300 sleep 600 & echo $! > "$1.nested"; wait', "sh", base],
  { stdio: "ignore" }).unref();
const read = (suffix) => { try { return Number(fs.readFileSync(base + suffix, "utf8").trim()) || undefined; } catch { return undefined; } };
const cgroup = admittedCgroup.startsWith("0::") ? admittedCgroup.slice(3) : admittedCgroup;
const timer = setInterval(() => {
  const pids = [read(".sh"), read(".bg"), read(".orphan"), read(".nested"), read(".pgid")];
  if (pids.some(pid => pid === undefined)) return;
  clearInterval(timer);
  const memory = (dir, file) => { try { return fs.readFileSync("/sys/fs/cgroup" + dir + "/" + file, "utf8").trim(); } catch (e) { return String(e.code); } };
  // The worker may sit in a fresh delegated leaf below its capped scope: report the whole chain.
  const chain = [];
  for (let dir = cgroup; dir !== "/" && dir !== ""; dir = dir.slice(0, dir.lastIndexOf("/")) || "/")
    chain.push({ dir, memoryMax: memory(dir, "memory.max"), swapMax: memory(dir, "memory.swap.max") });
  fs.writeFileSync(report + ".tmp", JSON.stringify({ worker: process.pid, admittedCgroup, cgroup, chain, pids }));
  fs.renameSync(report + ".tmp", report);
}, 20);
// On request, try to RAISE then TIGHTEN the cap through the task-local systemd-run (test-owned scope).
const trigger = setInterval(() => {
  if (!fs.existsSync(base + ".tighten")) return;
  clearInterval(trigger);
  const sh = 'systemd-run --user --scope -p MemoryMax=64G -- true; echo "raise=$?"; systemd-run --user --scope -p MemoryMax=256M -- cat /proc/self/cgroup; echo "tight=$?"';
  const out = spawnSync("/bin/sh", ["-c", sh], { encoding: "utf8" });
  fs.writeFileSync(base + ".tightened", out.stdout + out.stderr);
}, 50);
setInterval(() => {}, 1000);
`;

type Report = { worker: number; admittedCgroup: string; cgroup: string; chain: Array<{ dir: string; memoryMax: string; swapMax: string }>; pids: number[] };
/** The capped unit is the worker's own cgroup or its nearest fabric-task ancestor (delegated leaf below it). */
const cappedScope = (report: Report) => {
  const scope = report.chain.find(entry => /^fabric-task-.+\.scope$/.test(path.posix.basename(entry.dir)));
  expect(scope, `no fabric-task-*.scope in ${report.cgroup}`).toBeDefined();
  return scope!;
};
/** Every pid in the scope subtree (Delegate=yes allows sub-cgroups). */
const subtreeProcs = (dir: string): number[] => {
  const root = `/sys/fs/cgroup${dir}`;
  if (!fs.existsSync(root)) return [];
  try {
    const pids = fs.readFileSync(path.join(root, "cgroup.procs"), "utf8").split("\n").filter(Boolean).map(Number);
    for (const entry of fs.readdirSync(root, { withFileTypes: true })) if (entry.isDirectory()) pids.push(...subtreeProcs(path.posix.join(dir, entry.name)));
    return pids;
  } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT" || (error as NodeJS.ErrnoException).code === "ENODEV") return []; throw error; }
};
const awaitReport = async (file: string): Promise<{ report: Report; born: Proc[] }> => {
  await vi.waitFor(() => {
    if (fs.existsSync(`${file}.err`)) throw new Error(`worker failed: ${fs.readFileSync(`${file}.err`, "utf8")}`);
    expect(fs.existsSync(file), `no report; pid files: ${fs.readdirSync(path.dirname(file)).join(",")}`).toBe(true);
  }, { timeout: 15_000, interval: 50 });
  const report = JSON.parse(fs.readFileSync(file, "utf8")) as Report;
  const scopeDir = cappedScope(report).dir;
  const cmdline = (pid: number) => { try { return fs.readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0").filter(Boolean); } catch { return []; } };
  const unified = (pid: number) => { try { return fs.readFileSync(`/proc/${pid}/cgroup`, "utf8").trim().slice(3); } catch { return undefined; } };
  // The exact `systemd-run --user --scope -- timeout 300 sleep 600` executed: `sleep 600` runs under
  // `timeout 300`, inside this capped scope subtree (a real nested unit would be a sibling run-*.scope).
  await vi.waitFor(() => {
    const sleeper = subtreeProcs(scopeDir).find(pid => cmdline(pid).join(" ") === "sleep 600");
    expect(sleeper, "nested systemd-run command did not execute inside the capped leaf").toBeDefined();
    expect(cmdline(stat(sleeper!)!.pid).length).toBe(2);
    const parent = Number(fs.readFileSync(`/proc/${sleeper}/stat`, "utf8").split(") ")[1]!.split(" ")[1]);
    expect(cmdline(parent).slice(0, 2)).toEqual(["timeout", "300"]);
    expect(unified(sleeper!)).toMatch(new RegExp(`^${scopeDir.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(/|$)`));
    expect(unified(report.pids[3]!) ?? scopeDir).toMatch(new RegExp(`^${scopeDir.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(/|$)`));
  }, { timeout: 10_000, interval: 50 });
  // Census every process in the scope too (e.g. timeout's own sleep child), not only self-reported pids.
  const procs = subtreeProcs(cappedScope(report).dir);
  const all = [...new Set([report.worker, ...report.pids, ...procs])].flatMap(pid => { const s = stat(pid); return s ? [s] : []; });
  born.push(...all);
  return { report, born: all };
};

const expectDetachedGrandchild = (report: Report, procs: Proc[]): void => {
  const worker = procs.find(proc => proc.pid === report.worker)!;
  const setsidShell = procs.find(proc => proc.pid === report.pids[0])!;
  expect(setsidShell).toBeDefined();
  expect(setsidShell.sid).toBe(setsidShell.pid); // new session
  expect(setsidShell.pgid).toBe(setsidShell.pid); // its own PGID
  expect(setsidShell.pgid).not.toBe(worker.pgid);
  expect(setsidShell.sid).not.toBe(worker.sid);
  // Distinct counterexample: a separate PGID that stays in the worker's OWN session.
  const groupOnly = procs.find(proc => proc.pid === report.pids[4])!;
  expect(groupOnly).toBeDefined();
  expect(groupOnly.pgid).toBe(groupOnly.pid);
  expect(groupOnly.pgid).not.toBe(worker.pgid);
  expect(groupOnly.sid).toBe(worker.sid);
  expect(procs.length).toBeGreaterThanOrEqual(6);
};

// Stop-observation mechanism: real fs.watch and a recording cgroup.kill file, not a kernel cgroup.
// No process is signalled. Trust-boundary regression: only populated=0 can confirm subtree stop;
// unchanged populated=1, malformed receipts and watcher failure must keep typed custody debt.
describe("owned cgroup.events stop confirmation (smarty-dev#8257)", () => {
  const fds: number[] = [];
  afterEach(() => { for (const fd of fds.splice(0)) fs.closeSync(fd); });
  const fixture = () => {
    const root = temp("events");
    fs.writeFileSync(path.join(root, "cgroup.procs"), "424242\n");
    fs.writeFileSync(path.join(root, "cgroup.kill"), "");
    fs.writeFileSync(path.join(root, "cgroup.events"), "populated 1\nfrozen 0\n");
    const fd = fs.openSync(root, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY); fds.push(fd);
    const boundary = { relative: "/fixture/fabric-task-events.scope", fd, memoryMax: GIB };
    // Retain the old caller's delay as a negative-control trap: an event wait never calls it.
    const delay = vi.fn(async () => { throw new Error("membership polling delay invoked"); });
    return { root, boundary, delay, options: { termMs: 30, killMs: 100, delay } };
  };

  it("confirms the populated-0 event without a membership polling delay", async () => {
    const f = fixture();
    const watch = fs.watch;
    const watched: string[] = [];
    vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      watched.push(String(args[0]));
      const watcher = watch(...args);
      setImmediate(() => fs.writeFileSync(path.join(f.root, "cgroup.events"), "populated 0\nfrozen 0\n"));
      return watcher;
    }) as typeof fs.watch);
    await expect(stopTaskCgroup(f.boundary, f.options)).resolves.toBe("cgroup.kill");
    expect(watched).toEqual([`/proc/self/fd/${f.boundary.fd}/cgroup.events`]);
    expect(f.delay).not.toHaveBeenCalled();
  });

  it("fences an already-empty receipt and closes the watcher without waiting for another event", async () => {
    const f = fixture();
    fs.writeFileSync(path.join(f.root, "cgroup.events"), "populated 0\nfrozen 0\n");
    const watch = vi.spyOn(fs, "watch");
    await expect(stopTaskCgroup(f.boundary, f.options)).resolves.toBe("cgroup.kill");
    expect(watch).toHaveBeenCalledOnce();
    expect(f.delay).not.toHaveBeenCalled();
  });

  it("a populated scope at the bounded deadline fails typed with exact remaining pids, without polling", async () => {
    const f = fixture();
    f.options.killMs = 20;
    const outcome = await stopTaskCgroup(f.boundary, f.options).catch(error => error);
    expect(outcome).toBeInstanceOf(TaskCgroupStopUnconfirmedError);
    expect(outcome.pids).toEqual([424242]);
    expect(outcome.message).toContain("did not empty");
    expect(f.delay).not.toHaveBeenCalled();
  });

  it("an empty cgroup.procs cannot override a populated subtree receipt at the deadline", async () => {
    const f = fixture();
    f.options.killMs = 20;
    fs.writeFileSync(path.join(f.root, "cgroup.procs"), ""); // descendants may populate nested cgroups
    await expect(stopTaskCgroup(f.boundary, f.options)).rejects.toBeInstanceOf(TaskCgroupStopUnconfirmedError);
    expect(f.delay).not.toHaveBeenCalled();
  });

  it("unreadable populated state and watcher errors fail closed", async () => {
    const f = fixture();
    fs.writeFileSync(path.join(f.root, "cgroup.events"), "frozen 0\n");
    await expect(stopTaskCgroup(f.boundary, f.options)).rejects.toThrow(/populated.*unreadable/);
    fs.writeFileSync(path.join(f.root, "cgroup.events"), "populated 1\n");
    const watch = fs.watch;
    vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      const watcher = watch(...args);
      setImmediate(() => watcher.emit("error", Object.assign(new Error("event watch failed"), { code: "EIO" })));
      return watcher;
    }) as typeof fs.watch);
    await expect(stopTaskCgroup(f.boundary, f.options)).rejects.toThrow(/EIO/);
    expect(f.delay).not.toHaveBeenCalled();
  });
});

describe.skipIf(skipReason !== undefined && !required)("capped task cgroup on a real systemd --user (smarty-dev#8257)", () => {
  it("AgentManager task: default 24GiB scope, public cgroup receipt, and stop leaves zero live pids", async () => {
    if (skipReason) throw new Error(`FABRIC_TASK_CGROUP_E2E_REQUIRED=1 but the E2E cannot run: ${skipReason}`);
    const root = temp("manager");
    const workerPath = path.join(root, "worker.mjs");
    fs.writeFileSync(workerPath, WORKER);
    const reportFile = path.join(root, "report.json");
    vi.stubEnv("TASK_CGROUP_REPORT", reportFile);
    expect(DEFAULT_FABRIC_CONFIG.agents.processMemoryMaxBytes).toBe(24 * GIB);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false, timeoutMs: 120_000 },
      { workerPath, runRoot: path.join(root, "runs"), fullCodeMode: false });
    managers.push(manager);
    let launched: AgentHandleInfo | undefined;
    const handle = await manager.spawn({ task: "cgroup proof", transport: "process", extensions: false }, undefined, undefined, undefined, undefined,
      value => { launched = value; });
    const { report, born: procs } = await awaitReport(reportFile);
    expect(report.admittedCgroup).toBe(`0::${report.cgroup}`); // admitted before the worker's first statement
    const scope = cappedScope(report);
    expect(path.posix.basename(path.posix.dirname(scope.dir))).toBe("app.slice");
    expect(scope.memoryMax).toBe(String(24 * GIB));
    expect(scope.swapMax).toBe("0");
    // Public receipt: manager-level handle info names the worker's cgroup (scope or its fresh leaf),
    // not only the transport handle.
    expect([scope.dir, report.cgroup]).toContain(launched?.cgroup);
    expect([scope.dir, report.cgroup]).toContain((manager.list().find(info => info.id === handle.id) as AgentHandleInfo | undefined)?.cgroup);
    expectDetachedGrandchild(report, procs);
    await manager.stop(handle.id);
    await vi.waitFor(() => expect(survivors(procs)).toEqual([]), { timeout: 10_000, interval: 50 });
    expect(subtreeProcs(scope.dir)).toEqual([]);
  }, 60_000);

  const launchCapped = async (memoryMaxBytes: number, label: string) => {
    const root = temp(label);
    const workerPath = path.join(root, "worker.mjs");
    fs.writeFileSync(workerPath, WORKER);
    const reportFile = path.join(root, "report.json");
    const handle = await new ProcessTransport(undefined, undefined, undefined, memoryMaxBytes)
      .launch({ id: label, name: label, cwd: root, workerPath, workerArguments: ["--report", reportFile] });
    handles.push(handle);
    return { handle, ...(await awaitReport(reportFile)) };
  };

  it("ProcessTransport custom cap: memory.max is the custom value, swap 0, handle.cgroup names the leaf", async () => {
    if (skipReason) throw new Error(`FABRIC_TASK_CGROUP_E2E_REQUIRED=1 but the E2E cannot run: ${skipReason}`);
    const { handle, report, born: procs } = await launchCapped(GIB / 2, "custom");
    const scope = cappedScope(report);
    expect(scope.memoryMax).toBe(String(GIB / 2));
    expect(scope.swapMax).toBe("0");
    expect([scope.dir, report.cgroup]).toContain(handle.cgroup);
    expectDetachedGrandchild(report, procs);
    await handle.stop();
    await vi.waitFor(() => expect(survivors(procs)).toEqual([]), { timeout: 10_000, interval: 50 });
    expect(subtreeProcs(scope.dir)).toEqual([]);
  }, 60_000);

  it("nested systemd-run MemoryMax can tighten the task leaf but never raise it above the host cap", async () => {
    if (skipReason) throw new Error(`FABRIC_TASK_CGROUP_E2E_REQUIRED=1 but the E2E cannot run: ${skipReason}`);
    const { handle, report, born: procs } = await launchCapped(GIB / 2, "tighten");
    const base = `${path.dirname(path.join(roots.at(-1)!, "report.json"))}/report.json.pid`;
    fs.writeFileSync(`${base}.tighten`, "");
    await vi.waitFor(() => expect(fs.existsSync(`${base}.tightened`)).toBe(true), { timeout: 15_000, interval: 50 });
    const out = fs.readFileSync(`${base}.tightened`, "utf8");
    expect(out).toContain("raise=0");
    expect(out).toContain("tight=0");
    expect(out).toContain(`0::${report.cgroup}`); // the tightened command ran in the same leaf
    const leafMax = (dir: string) => Number(fs.readFileSync(`/sys/fs/cgroup${dir}/memory.max`, "utf8").trim());
    // Whichever leaf the wrapper owns now holds at most 256 MiB; nothing on the chain exceeds the host cap.
    expect(Math.min(leafMax(handle.cgroup!), leafMax(cappedScope(report).dir))).toBe(256 * 1024 ** 2);
    expect(leafMax(cappedScope(report).dir)).toBeLessThanOrEqual(GIB / 2);
    expect(fs.readFileSync(`/sys/fs/cgroup${cappedScope(report).dir}/memory.swap.max`, "utf8").trim()).toBe("0");
    await handle.stop();
    await vi.waitFor(() => expect(survivors(procs)).toEqual([]), { timeout: 10_000, interval: 50 });
  }, 60_000);

  it("a 1-byte cap the kernel cannot verify fails admission typed, before the worker runs, with no unconfined replay", async () => {
    if (skipReason) throw new Error(`FABRIC_TASK_CGROUP_E2E_REQUIRED=1 but the E2E cannot run: ${skipReason}`);
    const root = temp("cap1");
    const workerPath = path.join(root, "worker.mjs");
    fs.writeFileSync(workerPath, WORKER);
    const reportFile = path.join(root, "report.json");
    const outcome = await new ProcessTransport(undefined, undefined, undefined, 1)
      .launch({ id: "cap1", name: "cap1", cwd: root, workerPath, workerArguments: ["--report", reportFile] })
      .then(handle => { handles.push(handle); return handle; }, (error: unknown) => error);
    expect(outcome).toBeInstanceOf(TaskScopeAdmissionError);
    expect((outcome as TaskScopeAdmissionError).code).toBe("TASK_SCOPE_ADMISSION_FAILED");
    await new Promise(resolve => setTimeout(resolve, 500));
    expect(fs.readdirSync(root)).toEqual(["worker.mjs"]); // never booted: no report, no pid files, no replay
  }, 60_000);

  it("a refused cooperative worker signal still tears down the owned cgroup", async () => {
    if (skipReason) throw new Error(`FABRIC_TASK_CGROUP_E2E_REQUIRED=1 but the E2E cannot run: ${skipReason}`);
    const { handle, born: procs } = await launchCapped(GIB, "term-refused");
    const kill = process.kill.bind(process);
    vi.spyOn(process, "kill").mockImplementation(((pid: number, signal?: NodeJS.Signals | number) => {
      if (signal === "SIGTERM") throw Object.assign(new Error(`EPERM: kill ${pid}`), { code: "EPERM" });
      return kill(pid, signal);
    }) as typeof process.kill);
    await handle.stop();
    await vi.waitFor(() => expect(survivors(procs)).toEqual([]), { timeout: 10_000, interval: 50 });
  }, 60_000);

  it("older-kernel fallback: with cgroup.kill unavailable, per-pid stop still leaves zero live pids", async () => {
    if (skipReason) throw new Error(`FABRIC_TASK_CGROUP_E2E_REQUIRED=1 but the E2E cannot run: ${skipReason}`);
    const { handle, born: procs } = await launchCapped(GIB, "oldkernel");
    // Fault injection (named): pre-5.14 kernels have no cgroup.kill file.
    const enoent = (file: unknown) => String(file).endsWith("/cgroup.kill")
      ? Object.assign(new Error(`ENOENT: no such file, open '${String(file)}'`), { code: "ENOENT" }) : undefined;
    const { writeFileSync, openSync } = fs, { writeFile } = fs.promises;
    vi.spyOn(fs, "writeFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      const error = enoent(file); if (error) throw error; return (writeFileSync as (...a: unknown[]) => void)(file, ...rest);
    }) as typeof fs.writeFileSync);
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      const error = enoent(file); if (error) throw error; return (openSync as (...a: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    vi.spyOn(fs.promises, "writeFile").mockImplementation((async (file: unknown, ...rest: unknown[]) => {
      const error = enoent(file); if (error) throw error; return (writeFile as (...a: unknown[]) => Promise<void>)(file, ...rest);
    }) as typeof fs.promises.writeFile);
    await handle.stop();
    await vi.waitFor(() => expect(survivors(procs)).toEqual([]), { timeout: 10_000, interval: 50 });
  }, 60_000);

  it("older-kernel fallback: a pid that cannot be signalled yields a typed error naming that pid", async () => {
    if (skipReason) throw new Error(`FABRIC_TASK_CGROUP_E2E_REQUIRED=1 but the E2E cannot run: ${skipReason}`);
    const { handle, report, born: procs } = await launchCapped(GIB, "eperm");
    // The setpgid child leads its own group, so blocking ±stuck refuses BOTH its pid and group signals
    // while every other born pid stays killable: exactly one survivor is expected.
    const stuck = report.pids[4]!;
    const { writeFileSync, openSync } = fs;
    const hidden = (file: unknown) => String(file).endsWith("/cgroup.kill");
    vi.spyOn(fs, "writeFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (hidden(file)) throw Object.assign(new Error("ENOENT"), { code: "ENOENT" });
      return (writeFileSync as (...a: unknown[]) => void)(file, ...rest);
    }) as typeof fs.writeFileSync);
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (hidden(file)) throw Object.assign(new Error("ENOENT"), { code: "ENOENT" });
      return (openSync as (...a: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    const kill = process.kill.bind(process);
    vi.spyOn(process, "kill").mockImplementation(((pid: number, signal?: NodeJS.Signals | number) => {
      if (Math.abs(pid) === stuck) throw Object.assign(new Error(`EPERM: kill ${pid}`), { code: "EPERM" });
      return kill(pid, signal);
    }) as typeof process.kill);
    const outcome = await handle.stop().then(() => undefined, (error: unknown) => error);
    // Exact public contract: the typed error, its code preserved through the handle, and exactly the
    // surviving pid (no generic EPERM, no debt-string matching, no extra/unknown pids).
    expect(outcome, "stop must not report success while an owned pid survives").toBeInstanceOf(TaskCgroupStopUnconfirmedError);
    const typed = outcome as TaskCgroupStopUnconfirmedError;
    expect(typed.code).toBe("TASK_CGROUP_STOP_UNCONFIRMED");
    expect([...typed.pids]).toEqual([stuck]);
    expect([cappedScope(report).dir, report.cgroup]).toContain(typed.cgroup);
    expect(typed.message).toContain(String(stuck));
    vi.restoreAllMocks();
    await handle.stop().catch(() => undefined);
    await vi.waitFor(() => expect(survivors(procs)).toEqual([]), { timeout: 10_000, interval: 50 });
  }, 60_000);
});
