import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  emitProcessCensusTestEvent, linuxProcesses, PROCESS_CENSUS_TEST_EVENT,
} from "../src/agents/transports/linux-process-census.js";

// Keep reason: recorded host-wide census incident smarty-dev#20387 and failing
// CI run 131897366500 need branch receipts, not another host-wide observation.
// Listener failures must not change custody results/errors (a trust boundary).
// https://github.com/Smarty-Pants-Inc/smarty-dev/issues/20387
// All filesystem operations below are recorded, read-only mocks: no native
// process, signal, systemd scope, host /proc read or fixture write is performed.
type CensusEvent = { kind: string; [key: string]: unknown };
const listeners: Array<(event: CensusEvent) => void> = [];
const listen = (listener: (event: CensusEvent) => void) => {
  listeners.push(listener);
  process.on(PROCESS_CENSUS_TEST_EVENT, listener);
};
const collect = () => {
  const events: CensusEvent[] = [];
  listen(event => events.push(event));
  return events;
};
const errno = (code: string) => Object.assign(new Error(`fixture ${code}`), { code });

afterEach(() => {
  for (const listener of listeners.splice(0)) process.removeListener(PROCESS_CENSUS_TEST_EVENT, listener);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function fixture() {
  const procRoot = path.resolve("fixture-process-census", "proc");
  const cgroupDirectory = path.resolve("fixture-process-census", "scope");
  // Count numeric directory entries even if a process vanishes before stat.
  const entries = ["410", "999", "777", "self", "not-a-pid"];
  const files = new Map<string, string>([[path.join(cgroupDirectory, "cgroup.procs"), "410\n"]]);
  const directories = new Map<string, string[]>([[cgroupDirectory, []]]);
  const readFailures = new Map<string, unknown>();
  const walkFailures = new Map<string, unknown>();
  for (const pid of [410, 999]) {
    const fields = Array<string>(20).fill("0");
    fields[0] = "S"; fields[1] = "1"; fields[2] = String(pid); fields[19] = `${pid}00`;
    files.set(path.join(procRoot, String(pid), "stat"), `${pid} (fixture ) worker) ${fields.join(" ")}\n`);
  }
  const read = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor) => {
    const name = String(file);
    if (readFailures.has(name)) throw readFailures.get(name);
    if (!files.has(name)) throw errno("ENOENT");
    return files.get(name)!;
  }) as typeof fs.readFileSync);
  const walk = vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike) => {
    const name = String(file);
    if (walkFailures.has(name)) throw walkFailures.get(name);
    if (name === procRoot) return entries;
    if (!directories.has(name)) throw errno("ENOENT");
    return directories.get(name)!.map(entry => ({ name: entry, isDirectory: () => true }));
  }) as typeof fs.readdirSync);
  return { procRoot, cgroupDirectory, entries, files, directories, readFailures, walkFailures, read, walk };
}
const pids = (members: ReturnType<typeof linuxProcesses>) => members.map(member => member.pid);
const scans = (events: CensusEvent[]) => events.filter(event => event.kind === "full-scan");

describe("test-only process census branch receipts", () => {
  it.each([undefined, "0", "true"])("is inert with diagnostics=%s, without extra walks or entry counts", value => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", value);
    const f = fixture();
    const events = collect();
    // A diagnostic-only count would call filter; the original census uses flatMap.
    const count = vi.fn(() => { throw new Error("unexpected diagnostic count"); });
    Object.defineProperty(f.entries, "filter", { value: count });
    emitProcessCensusTestEvent({ kind: "manual" });
    expect(pids(linuxProcesses(f))).toEqual([410]);
    expect(f.read.mock.calls.map(([file]) => String(file))).toEqual([
      path.join(f.cgroupDirectory, "cgroup.procs"), path.join(f.procRoot, "410", "stat"),
    ]);
    expect(pids(linuxProcesses({ procRoot: f.procRoot }))).toEqual([410, 999]);
    expect(f.walk.mock.calls.map(([file]) => String(file))).toEqual([f.cgroupDirectory, f.procRoot]);
    expect(events).toEqual([]);
    expect(count).not.toHaveBeenCalled();
  });

  it("records each actual nested cgroup.procs read, without a full scan for a live scope", () => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "1");
    const f = fixture();
    const child = path.join(f.cgroupDirectory, "child");
    f.directories.set(f.cgroupDirectory, ["child"]); f.directories.set(child, []);
    f.files.set(path.join(child, "cgroup.procs"), "410\n999\n999\n");
    const events = collect();
    expect(pids(linuxProcesses(f))).toEqual([410, 999]);
    expect(events[0]).toEqual({ kind: "attempt", scoped: true, cgroupDirectory: f.cgroupDirectory, procRoot: f.procRoot });
    expect(events.filter(event => event.kind === "cgroup-procs-read").map(event => event.path)).toEqual([
      path.join(f.cgroupDirectory, "cgroup.procs"), path.join(child, "cgroup.procs"),
    ]);
    expect(f.read.mock.calls.filter(([file]) => String(file).endsWith("cgroup.procs"))).toHaveLength(2);
    expect(scans(events)).toEqual([]);
    expect(f.walk.mock.calls.some(([file]) => String(file) === f.procRoot)).toBe(false);
  });

  it("records no-verified-scope and actual enumeration counts, including vanished PIDs", () => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "1");
    const f = fixture();
    const events = collect();
    expect(pids(linuxProcesses({ procRoot: f.procRoot }))).toEqual([410, 999]);
    expect(events[0]).toEqual({ kind: "attempt", scoped: false, cgroupDirectory: undefined, procRoot: f.procRoot });
    expect(scans(events)).toEqual([expect.objectContaining({ reason: "no-verified-scope", entries: 5, pidEntries: 3 })]);
    expect(f.walk).toHaveBeenCalledTimes(1);
    expect(events.some(event => event.kind === "cgroup-procs-read")).toBe(false);
  });

  it.each(["ENOENT", "EACCES"])("records the original %s cgroup read failure and its fallback", code => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "1");
    const f = fixture();
    const file = path.join(f.cgroupDirectory, "cgroup.procs");
    const error = errno(code);
    f.readFailures.set(file, error);
    const events = collect();
    expect(pids(linuxProcesses(f))).toEqual([410, 999]);
    expect(events.filter(event => event.kind === "cgroup-procs-read")).toHaveLength(1);
    expect(events.filter(event => event.kind === "cgroup-procs-read-error")).toEqual([
      expect.objectContaining({ path: file, code, error: String(error) }),
    ]);
    expect(scans(events)).toEqual([expect.objectContaining({
      reason: "cgroup-unreadable", entries: 5, pidEntries: 3, code, detail: String(error),
    })]);
    expect(f.walk.mock.calls.map(([target]) => String(target))).toEqual([f.procRoot]);
  });

  it.each(["empty", "escape"])("records the actual %s fallback branch", branch => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "1");
    const f = fixture();
    if (branch === "empty") f.files.set(path.join(f.cgroupDirectory, "cgroup.procs"), "\n");
    const events = collect();
    expect(pids(linuxProcesses({ ...f, ownedPids: branch === "escape" ? [410, 999] : [] }))).toEqual([410, 999]);
    expect(scans(events)).toEqual([expect.objectContaining({
      reason: branch === "escape" ? "known-live-escape" : "empty-scoped-snapshot", entries: 5, pidEntries: 3,
    })]);
    expect(f.walk.mock.calls.map(([file]) => String(file))).toEqual([f.cgroupDirectory, f.procRoot]);
  });

  it.each(["malformed", "walk"])("distinguishes %s membership failure without another read", failure => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "1");
    const f = fixture();
    if (failure === "malformed") f.files.set(path.join(f.cgroupDirectory, "cgroup.procs"), "410\nnot-a-pid\n");
    else f.walkFailures.set(f.cgroupDirectory, errno("EACCES"));
    const events = collect();
    expect(pids(linuxProcesses(f))).toEqual([410, 999]);
    expect(scans(events)).toEqual([expect.objectContaining({
      reason: "cgroup-unreadable",
      detail: failure === "malformed" ? "Error: Unreadable cgroup.procs census" : "Error: fixture EACCES",
    })]);
    expect(events.filter(event => event.kind === "cgroup-procs-read")).toHaveLength(1);
    expect(events.some(event => event.kind === "cgroup-procs-read-error")).toBe(false);
    expect(f.read.mock.calls.filter(([file]) => String(file).endsWith("cgroup.procs"))).toHaveLength(1);
  });

  it("cannot let a throwing listener change protected scoped/full/fallback results", () => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "1");
    const f = fixture();
    const events = collect();
    listen(() => { throw new Error("diagnostic subscriber failure"); });
    expect(() => emitProcessCensusTestEvent({ kind: "manual" })).not.toThrow();
    expect(pids(linuxProcesses(f))).toEqual([410]);
    expect(pids(linuxProcesses({ procRoot: f.procRoot }))).toEqual([410, 999]);
    const error = errno("EACCES");
    f.readFailures.set(path.join(f.cgroupDirectory, "cgroup.procs"), error);
    expect(pids(linuxProcesses(f))).toEqual([410, 999]);
    expect(events).toContainEqual(expect.objectContaining({ kind: "cgroup-procs-read-error", code: "EACCES", error: String(error) }));
  });

  it.each(["scoped", "full", "escaped-owned"])("preserves the exact protected stat error on the %s path", mode => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "1");
    const f = fixture();
    const error = errno("EPERM");
    f.readFailures.set(path.join(f.procRoot, mode === "scoped" ? "410" : "999", "stat"), error);
    const events = collect();
    listen(() => { throw new Error("diagnostic subscriber failure"); });
    let caught: unknown;
    try {
      linuxProcesses({ procRoot: f.procRoot,
        ...(mode === "full" ? {} : { cgroupDirectory: f.cgroupDirectory }),
        ...(mode === "escaped-owned" ? { ownedPids: [410, 999] } : {}),
      });
    } catch (failure) { caught = failure; }
    expect(caught).toBe(error);
    if (mode === "full") {
      // All three numeric entries were enumerated, but stat failed on the
      // second attempt: the third candidate was never read or completed.
      expect(scans(events)).toEqual([expect.objectContaining({ entries: 5, pidEntries: 3 })]);
      expect(f.read.mock.calls.map(([file]) => String(file))).toEqual([
        path.join(f.procRoot, "410", "stat"), path.join(f.procRoot, "999", "stat"),
      ]);
    }
  });

  it("preserves a full-enumeration error after recording the original cgroup read error", () => {
    vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "1");
    const f = fixture();
    const readError = errno("EACCES");
    const censusError = errno("EIO");
    f.readFailures.set(path.join(f.cgroupDirectory, "cgroup.procs"), readError);
    f.walkFailures.set(f.procRoot, censusError);
    const events = collect();
    listen(() => { throw new Error("diagnostic subscriber failure"); });
    let caught: unknown;
    try { linuxProcesses(f); } catch (error) { caught = error; }
    expect(caught).toBe(censusError);
    expect(events).toContainEqual(expect.objectContaining({ kind: "cgroup-procs-read-error", code: "EACCES", error: String(readError) }));
    // An unsuccessful enumeration has no entry counts to publish.
    expect(scans(events)).toEqual([]);
  });
});
