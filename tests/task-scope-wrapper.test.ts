import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { taskScopeWrapperSource } from "../src/agents/transports/task-scope-wrapper.js";

// smarty-dev#8257 MECHANISM tests for the task-local `systemd-run` PATH wrapper. Real /bin/sh executes
// the generated script; the "real" systemd-run is a recording fake, so no unit is ever created here.
// The live cgroup/memory effect of the exact native command is proven by tests/task-cgroup.test.ts.
const ownCgroup = (() => {
  if (process.platform !== "linux") return undefined;
  try {
    const line = fs.readFileSync("/proc/self/cgroup", "utf8").trim();
    return line.startsWith("0::/") && !line.includes("\n") ? line.slice(3) : undefined;
  } catch { return undefined; }
})();
const CAP = 24 * 1024 ** 3;
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scope-wrapper-")); roots.push(root);
  const real = path.join(root, "real-systemd-run");
  const record = path.join(root, "real-args");
  fs.writeFileSync(real, `#!/bin/sh\nprintf '%s\\n' "$@" > '${record}'\nexit 42\n`, { mode: 0o700 });
  const bin = path.join(root, "bin"); fs.mkdirSync(bin);
  const wrapper = path.join(bin, "systemd-run");
  fs.writeFileSync(wrapper, taskScopeWrapperSource(real, ownCgroup!, CAP), { mode: 0o700 });
  const run = (args: string[]) => {
    const result = spawnSync(wrapper, args, { encoding: "utf8", timeout: 10_000,
      env: { PATH: `${bin}:/usr/bin:/bin`, FABRIC_TASK_MEMORY_MAX: String(CAP), FABRIC_TASK_CGROUP: ownCgroup!, FABRIC_TASK_SYSTEMD_RUN_WRAPPER: wrapper } });
    return { ...result, real: fs.existsSync(record) ? fs.readFileSync(record, "utf8").split("\n").slice(0, -1) : undefined };
  };
  return { root, real, wrapper, run };
};
/** The command prints its pid and unified cgroup: proves in-place exec (same PID) and the same leaf. */
const PROBE = ["/bin/sh", "-c", 'echo "$$"; cat /proc/self/cgroup'];

describe.skipIf(!ownCgroup)("task-local systemd-run wrapper (smarty-dev#8257)", () => {
  it("is a standalone shell script", () => {
    const source = taskScopeWrapperSource("/usr/bin/systemd-run", ownCgroup!, CAP);
    expect(source.startsWith("#!/bin/sh")).toBe(true);
    expect(source).not.toMatch(/\b(node|bun)\b/);
  });

  it("runs `--user --scope -- cmd` in place: same PID, same leaf cgroup, no real unit", () => {
    const f = fixture();
    const result = f.run(["--user", "--scope", "--", ...PROBE]);
    expect(result.status, result.stderr).toBe(0);
    const [pid, cgroup] = result.stdout.trim().split("\n");
    expect(Number(pid)).toBe(result.pid); // shell exec preserved the wrapper's PID
    expect(cgroup).toBe(`0::${ownCgroup}`);
    expect(result.real).toBeUndefined();
  });

  it("runs the exact principal command shape (timeout <n> <cmd>) in place", () => {
    const f = fixture();
    const result = f.run(["--user", "--scope", "--", "timeout", "5", "/bin/sh", "-c", "cat /proc/self/cgroup"]);
    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout.trim()).toBe(`0::${ownCgroup}`);
    expect(result.real).toBeUndefined();
  });

  // A finite MemoryMax lowers the REAL leaf's memory.max, so it is never exercised here against the
  // runner's own cgroup; tests/task-cgroup.test.ts proves tighter/raise inside a test-owned scope.
  it.each([
    [["-p", "MemoryMax=infinity", "-p", "MemorySwapMax=64G"]], // a raise request cannot raise anything
    [["--property=MemoryMax=infinity", "--property=MemorySwapMax=0"]],
    [["--unit=escape.scope", "--quiet", "--collect", "-G"]],
    [["-u", "escape.scope", "-q"]],
    [["--scope", "--user"]], // duplicated mode flags
  ])("strips scope options %j and still runs in the capped leaf", (options) => {
    const f = fixture();
    const result = f.run(["--user", "--scope", ...options, "--", ...PROBE]);
    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout.trim().split("\n")[1]).toBe(`0::${ownCgroup}`);
    expect(result.real).toBeUndefined();
  });

  it("accepts the command without a `--` separator", () => {
    const f = fixture();
    const result = f.run(["--user", "--scope", ...PROBE]);
    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout.trim().split("\n")[1]).toBe(`0::${ownCgroup}`);
    expect(result.real).toBeUndefined();
  });

  it.each([
    [["--version"]],
    [["--user", "--unit=svc", "sleep", "1"]], // service mode, not a scope
    [["--scope", "--", "true"]], // system manager scope (no --user)
    [["--user", "--no-such-option", "--", "true"]], // unknown option outside scope mode
  ])("passes non `--user --scope` modes %j to the real executable unchanged", (args) => {
    const f = fixture();
    const result = f.run(args);
    expect(result.real).toEqual(args);
    expect(result.status).toBe(42); // the real executable's own exit status
  });

  it.each([
    [["--slice=other.slice"]],
    [["--no-such-option"]],
    [["--machine=other"]],
    [["-p", "CPUQuota=50%"]],
    [["-p", "MemoryMax=lots"]],
  ])("in `--user --scope` mode refuses %j (never escapes via the real binary)", (options) => {
    const f = fixture();
    const result = f.run(["--user", "--scope", ...options, "--", "true"]);
    expect(result.status).not.toBe(0);
    expect(result.status).not.toBe(42);
    expect(result.real).toBeUndefined();
    expect(result.stderr).toMatch(/fabric task systemd-run/);
  });

  it.each(["-qp", "-qu"])("a %s value before --scope cannot hide scope mode from the shim", (cluster) => {
    const f = fixture();
    const result = f.run(["--user", cluster, cluster === "-qp" ? "MemoryMax=infinity" : "nested.scope", "--scope", "--", "true"]);
    expect(result.status).toBe(1); // unsupported cluster refused, not a real scope
    expect(result.real).toBeUndefined();
    const ordinary = f.run(["--user", cluster, "value", "--", "true"]);
    expect(ordinary.status).toBe(42); // the same non-scope mode still passes through
    expect(ordinary.real).toEqual(["--user", cluster, "value", "--", "true"]);
  });

  it("outside its own leaf it keeps real semantics: `--user --scope` goes to the real binary unchanged", () => {
    const f = fixture();
    fs.writeFileSync(f.wrapper, taskScopeWrapperSource(f.real, `${ownCgroup}-not-this-leaf/fabric-task-x.scope`, CAP), { mode: 0o700 });
    const args = ["--user", "--scope", "--", "true"];
    const result = f.run(args);
    expect(result.real).toEqual(args);
  });

  it("rejects unsafe generator inputs", () => {
    expect(() => taskScopeWrapperSource("systemd-run", ownCgroup!, CAP)).toThrow(/realSystemdRun/);
    expect(() => taskScopeWrapperSource("/usr/bin/systemd-run", "/a/../b", CAP)).toThrow(/cgroupRelative/);
    expect(() => taskScopeWrapperSource("/usr/bin/systemd-run", "/a/$(x)", CAP)).toThrow(/cgroupRelative/);
    expect(() => taskScopeWrapperSource("/usr/bin/systemd-run", ownCgroup!, 0)).toThrow(/memoryMaxBytes/);
  });
});
