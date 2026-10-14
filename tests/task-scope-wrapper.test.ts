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
const USER_UNIT_RULE = /smarty-dev#8257: user-manager units must not escape task memory cap\/agents\.stop/;
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
    [["--user", "--version"]], // informational: systemd-run exits before creating any unit
    [["--user", "--help"]],
    [["--unit=svc", "sleep", "1"]], // system manager service (no --user)
    [["--scope", "--", "true"]], // system manager scope (no --user)
    [["--scope", "--", "sh", "-c", "echo --user"]], // --user only in command text
    [["--unit=svc", "sh", "--user"]], // --user after the first non-option is command text
    [["--description", "--user", "--scope", "--", "true"]], // --user is the description VALUE
    [["--description=--user", "--", "true"]],
  ])("passes non-user modes %j to the real executable unchanged", (args) => {
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
    expect(ordinary.status).toBe(1); // a user-manager service is refused, never dispatched
    expect(ordinary.real).toBeUndefined();
    expect(ordinary.stderr).toMatch(USER_UNIT_RULE);
  });

  // Astra P2: inside the capped leaf any `--user` mode other than `--scope` (service, timer, path,
  // socket, shell, unknown) would start a user-manager unit outside the task memory cap and outside
  // agents.stop. It is refused by name and never reaches the real binary.
  it.each([
    [["--user", "--unit=svc", "sleep", "1"]], // service via --unit=
    [["--user", "-u", "svc", "sleep", "1"]],
    [["--user", "--on-active=30", "true"]], // timer
    [["--user", "--on-calendar", "hourly", "true"]],
    [["--user", "--path-property=PathExists=/tmp/x", "true"]], // path unit
    [["--user", "true"]], // ordinary transient service
    [["--user", "-S"]], // shell service
    [["--user", "-qt", "--", "true"]], // cluster without a value
    [["--user", "--no-such-option", "--", "true"]], // unknown option: never guessed through
    [["-q", "--user", "--", "true"]], // --user after another option
    [["--user", "-qpp", "--scope", "--", "true"]], // value attached in the cluster: must not hide --scope
  ])("inside the leaf refuses user-manager non-scope mode %j without real dispatch", (args) => {
    const f = fixture();
    const result = f.run(args);
    expect(result.status).toBe(1);
    expect(result.real).toBeUndefined();
    expect(result.stderr).toMatch(/fabric task systemd-run/);
    if (!args.includes("--scope")) expect(result.stderr).toMatch(USER_UNIT_RULE);
  });

  // LEAD getopt parity: like getopt_long, a unique long-option prefix resolves to its option (exact
  // name wins) and options may come in any order; `--us --scope` and `--desc x --user --scope` are the
  // in-place scope mode (same PID, same leaf), never the real binary.
  it.each([
    [["--us", "--scope", "--"]],
    [["--desc", "x", "--user", "--scope", "--"]],
    [["--user", "--sco", "--"]],
    [["--desc=x", "--sc", "--qui", "--prop=MemoryMax=infinity", "--us"]], // no `--` separator
    [["--pr", "MemorySwapMax=0", "--us", "--un", "esc.scope", "--scop", "--"]],
  ])("resolves unique abbreviations %j and runs `--user --scope` in place", (options) => {
    const f = fixture();
    const result = f.run([...options, ...PROBE]);
    expect(result.status, result.stderr).toBe(0);
    const [pid, cgroup] = result.stdout.trim().split("\n");
    expect(Number(pid)).toBe(result.pid);
    expect(cgroup).toBe(`0::${ownCgroup}`);
    expect(result.real).toBeUndefined();
  });

  it.each([
    [["--us", "--", "true"]], // abbreviated --user without --scope: a user-manager service
    [["--desc", "x", "--us", "true"]],
    [["--us", "--on-cal", "hourly", "true"]],
  ])("inside the leaf refuses abbreviated user-manager non-scope mode %j", (args) => {
    const f = fixture();
    const result = f.run(args);
    expect(result.status).toBe(1);
    expect(result.real).toBeUndefined();
    expect(result.stderr).toMatch(USER_UNIT_RULE);
  });

  // Ambiguous or unknown long options, or an unknown option that may take a value, make the scan
  // unprovable: fail closed inside the leaf, never forwarded.
  it.each([
    [["--u", "--scope", "--", "true"]], // ambiguous: --user --unit --uid
    [["--user", "--s", "--", "true"]], // ambiguous: --scope --slice ...
    [["--u", "--", "true"]],
    [["--no-such", "--user", "--scope", "--", "true"]], // unknown long option
    [["--userx", "--scope", "--", "true"]],
    [["-X", "value", "--user", "--scope", "--", "true"]], // unknown short option with a value
    [["--quiet=1", "--user", "--scope", "--", "true"]], // malformed known flag
    [["--us=1", "--scope", "--", "true"]], // no-arg flag given a value
  ])("inside the leaf refuses unprovable option scan %j without real dispatch", (args) => {
    const f = fixture();
    const result = f.run(args);
    expect(result.status).toBe(1);
    expect(result.real).toBeUndefined();
    expect(result.stderr).toMatch(/smarty-dev#8257/);
  });

  it.each([
    [["--quiet", "--no-block", "--wait", "-rG", "--unit=svc", "true"]], // known no-arg flags, non-user
    [["--scope", "--", "sh", "--us", "--desc"]], // abbreviations only in command text
    [["--description", "--us", "--scope", "--", "true"]], // abbreviation only as a value
    [["-E", "--us", "--scope", "--", "true"]],
    [["--desc", "--user", "--scope", "--", "true"]], // abbreviated value option: --user is its value
    [["--desc=--user", "--sco", "--", "true"]],
    [["--set", "--us", "--wait", "true"]], // --setenv VALUE
    [["--qui", "--no-b", "--sco", "--", "sh", "--user"]],
  ])("inside the leaf passes provable non-user mode %j unchanged", (args) => {
    const f = fixture();
    const result = f.run(args);
    expect(result.real).toEqual(args);
    expect(result.status).toBe(42);
  });

  it("abbreviations in command text keep `--user --scope` in place", () => {
    const f = fixture();
    const result = f.run(["--user", "--scope", "--", ...PROBE, "--desc", "x", "--us"]);
    expect(result.status, result.stderr).toBe(0);
    expect(Number(result.stdout.trim().split("\n")[0])).toBe(result.pid);
    expect(result.real).toBeUndefined();
  });

  it("flags in command text are not options: `--user --scope -- cmd --unit=svc` still runs in place", () => {
    const f = fixture();
    const result = f.run(["--user", "--scope", "--", ...PROBE, "--unit=svc", "--user"]);
    expect(result.status, result.stderr).toBe(0);
    const [pid, cgroup] = result.stdout.trim().split("\n");
    expect(Number(pid)).toBe(result.pid);
    expect(cgroup).toBe(`0::${ownCgroup}`);
    expect(result.real).toBeUndefined();
  });

  it.each([
    [["--user", "--unit=svc", "sleep", "1"]],
    [["--user", "--on-active=30", "true"]],
    [["--user", "true"]],
    [["--desc", "x", "--user", "--scope", "--", "true"]], // abbreviated: real getopt semantics
    [["--us", "--", "true"]],
    [["--us", "--scope", "--", "true"]],
    [["--u", "--scope", "--", "true"]], // ambiguous: the real binary reports it
  ])("outside its own leaf user-manager mode %j keeps real semantics", (args) => {
    const f = fixture();
    fs.writeFileSync(f.wrapper, taskScopeWrapperSource(f.real, `${ownCgroup}-not-this-leaf/fabric-task-x.scope`, CAP), { mode: 0o700 });
    const result = f.run(args);
    expect(result.real).toEqual(args);
    expect(result.status).toBe(42);
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
