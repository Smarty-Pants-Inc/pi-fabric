import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { assertTaskRunRootAdmissible, runPathComponents } from "../src/worker/task-run-path.js";

describe("assertTaskRunRootAdmissible (static platform, Owner OPTION1)", () => {
  it("Windows inherited PI_FABRIC_RUN_ROOT fails closed with the supplied error type", () => {
    expect(() => assertTaskRunRootAdmissible(true, (m) => new RangeError(m), "win32")).toThrow(RangeError);
  });
  it("Windows trusted host root and POSIX inherited root are admitted", () => {
    expect(() => assertTaskRunRootAdmissible(false, undefined, "win32")).not.toThrow();
    expect(() => assertTaskRunRootAdmissible(true, undefined, "linux")).not.toThrow();
  });
});

describe("runPathComponents (static Win32/POSIX semantics)", () => {
  it("drive root is not repeated as a component", () => {
    expect(runPathComponents("C:\\Users\\me\\runs\\abc", "win32")).toEqual({ root: "C:\\", components: ["Users", "me", "runs", "abc"] });
  });
  it("UNC server/share root is not repeated as components", () => {
    expect(runPathComponents("\\\\srv\\share\\runs\\abc", "win32")).toEqual({ root: "\\\\srv\\share\\", components: ["runs", "abc"] });
  });
  it("posix root", () => {
    expect(runPathComponents("/var/runs/abc", "posix")).toEqual({ root: "/", components: ["var", "runs", "abc"] });
  });
});

type Entry = "dir" | "file" | "junction";
/** Decision C: static mocked Win32 filesystem that records every call. */
const loadWin32Guard = async (entries: Record<string, Entry>, realpaths: Record<string, string> = {}) => {
  const calls: string[] = [];
  const key = (p: string) => p.toLowerCase();
  const table = new Map(Object.entries(entries).map(([k, v]) => [key(k), v]));
  const stat = (kind: Entry) => ({ isSymbolicLink: () => kind === "junction", isDirectory: () => kind === "dir", isFile: () => kind === "file" });
  const lstatSync = (p: string) => {
    calls.push(`lstat ${p}`);
    const kind = p === "C:\\" || p === "\\\\srv\\share\\" ? "dir" : table.get(key(p));
    if (!kind) throw Object.assign(new Error("ENOENT"), { code: "ENOENT" });
    return stat(kind);
  };
  const realpathSync = Object.assign((p: string) => p, { native: (p: string) => { calls.push(`realpath ${p}`); return realpaths[p] ?? p; } });
  const mutate = (name: string) => (p: string) => { calls.push(`${name} ${p}`); throw new Error(`guard must not ${name}`); };
  const fakeFs = { lstatSync, realpathSync, mkdirSync: mutate("mkdir"), writeFileSync: mutate("write"), openSync: mutate("open") };
  vi.resetModules();
  vi.doMock("node:fs", () => ({ default: fakeFs, ...fakeFs }));
  vi.doMock("node:path", () => ({ default: path.win32, ...path.win32 }));
  const mod = await import("../src/worker/task-run-path.js");
  return { assert: mod.assertTaskRunPathUnaliased, calls };
};

afterEach(() => { vi.doUnmock("node:fs"); vi.doUnmock("node:path"); vi.resetModules(); });

describe("assertTaskRunPathUnaliased (static mocked Win32 FS)", () => {
  it("prior counterexample: junction under a missing-looking C:\\C: join is now walked and refused, no mutation", async () => {
    const { assert, calls } = await loadWin32Guard({ "C:\\runs": "dir", "C:\\runs\\alias": "junction" });
    expect(() => assert("C:\\runs\\alias\\abc", (m) => new RangeError(m))).toThrow(RangeError);
    expect(calls).toEqual(["lstat C:\\runs", "lstat C:\\runs\\alias"]);
    expect(calls.some((c) => c.includes("C:\\C:"))).toBe(false);
    expect(calls.some((c) => /^(mkdir|write|open) /.test(c))).toBe(false);
  });
  it("UNC junction component is refused without mutation", async () => {
    const { assert, calls } = await loadWin32Guard({ "\\\\srv\\share\\runs": "junction" });
    expect(() => assert("\\\\srv\\share\\runs\\abc")).toThrow(/link alias/);
    expect(calls).toEqual(["lstat \\\\srv\\share\\runs"]);
  });
  it("non-link reparse alias is refused by the native realpath comparison", async () => {
    const { assert } = await loadWin32Guard({ "C:\\runs": "dir" }, { "C:\\runs": "D:\\victim" });
    expect(() => assert("C:\\runs\\abc")).toThrow(/alias/);
  });
  it("ordinary trusted root with a missing future run directory is accepted", async () => {
    const { assert, calls } = await loadWin32Guard({ "C:\\Users": "dir", "C:\\Users\\me": "dir", "C:\\Users\\me\\runs": "dir" });
    expect(() => assert("C:\\Users\\me\\runs\\abc")).not.toThrow();
    expect(calls).toEqual(["lstat C:\\Users", "lstat C:\\Users\\me", "lstat C:\\Users\\me\\runs", "lstat C:\\Users\\me\\runs\\abc", "realpath C:\\Users\\me\\runs"]);
  });
});

describe.skipIf(process.platform === "win32")("assertTaskRunPathUnaliased (real POSIX filesystem)", () => {
  it("refuses a symlinked ancestor and accepts a real root with a missing run dir", async () => {
    const { assertTaskRunPathUnaliased } = await import("../src/worker/task-run-path.js");
    // Fixture retained in the test TMPDIR (no local recursive cleanup).
    const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "task-run-path-")));
    fs.mkdirSync(path.join(base, "real"));
    fs.mkdirSync(path.join(base, "victim"));
    fs.symlinkSync(path.join(base, "victim"), path.join(base, "alias"));
    expect(() => assertTaskRunPathUnaliased(path.join(base, "real", "run1"))).not.toThrow();
    expect(() => assertTaskRunPathUnaliased(path.join(base, "alias", "run1"))).toThrow(/link alias/);
    expect(fs.readdirSync(path.join(base, "victim"))).toEqual([]);
  });
});

describe("AgentManager run-path guard order (static source, DELTA-SEC-02)", () => {
  const source = fs.readFileSync(new URL("../src/agents/manager.ts", import.meta.url), "utf8");
  const at = (needle: string) => { const index = source.indexOf(needle); expect(index, needle).toBeGreaterThan(-1); return index; };
  it("early guard precedes admission (no counter leak) and the prepareRouteDispatch CALL", () => {
    const early = at("assertRunPathSafe(path.join(this.#runRoot, id), this.#inheritedRunRoot);");
    expect(early).toBeLessThan(at("this.#semaphore.admit(this.#currentDepth + 1);"));
    expect(source).toMatch(/assertTaskRunRootAdmissible\(inheritedRoot, makeError\);\n\s*assertTaskRunPathUnaliased\(runDirectory, makeError\);/);
    const call = at("routeDispatch = prepareRouteDispatch(request.routeDecision,");
    expect(early).toBeLessThan(call);
    expect(source.slice(early, call)).not.toMatch(/routeDispatch\s*=/);
  });
  it("late guard runs after model preparation and immediately before the first mkdirPrivate", () => {
    const prep = at("model = await this.prepareModelForAdmission(");
    const late = at("assertRunPathSafe(runDirectory, this.#inheritedRunRoot);");
    const mkdir = at("mkdirPrivate(runDirectory, this.#trustRoot());");
    expect(prep).toBeLessThan(late);
    expect(late).toBeLessThan(mkdir);
    expect(source.slice(late, mkdir)).not.toMatch(/writeJsonAtomic|routeDispatch\?\.outcome|mkdirPrivate\(/);
  });
  it("central route dispatch allocates before pinning, with no yield between them", () => {
    const allocate = at("createHostTaskRun(runDirectory);");
    const pin = at("runPin = pinRunDirectory(runDirectory);");
    const dispatch = at("routeDispatch = prepareRouteDispatch(request.routeDecision,");
    expect(allocate).toBeLessThan(pin);
    expect(source.slice(allocate, pin)).not.toMatch(/await|afterRunAllocation/);
    expect(pin).toBeLessThan(dispatch);
    const late = at("assertRunPathSafe(runDirectory, this.#inheritedRunRoot);");
    const check = source.indexOf("runPin?.use(() => undefined);", late);
    expect(check).toBeGreaterThan(late);
    expect(check).toBeLessThan(at("mkdirPrivate(runDirectory, this.#trustRoot());"));
    for (const gone of ["inheritFds", "--session-directory-fd", "resolveFdBase", "CONTROL_PUBLISH_UNSUPPORTED_PLATFORM", "unsupportedControlPublication"]) expect(source.includes(gone), gone).toBe(false);
  });
  it("refusal never invokes the run-path outcome saver (direct and queued paths)", () => {
    expect(source).toMatch(/!this\.#queued\.has\(id\) && !isUnsafeRunPathError\(error\)\)/);
    expect(source).toMatch(/if \(isUnsafeRunPathError\(error\)\) queued\.runPathUnsafe = true;\n\s*await onStartFailed\?\.\(\);\n\s*this\.#settleQueued/);
    const save = at("#saveQueuedRouteOutcome(queued: QueuedAgent): boolean {");
    const unsafe = source.indexOf("if (queued.runPathUnsafe) {", save);
    expect(unsafe).toBeGreaterThan(save);
    expect(unsafe).toBeLessThan(source.indexOf("queued-result.json", save));
  });
});
