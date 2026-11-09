import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { bashGuardRefusal } from "../src/guards/bash-guard-policy.js";

// Data-safety/trust-boundary tests: command strings are only inspected, never executed.
// Mock both home and the file probe so no test reads or changes a real disable file.
const fixture = vi.hoisted(() => ({
  disabled: false,
  home: vi.fn(() => "/fixture-home"),
  exists: vi.fn(),
  patternLoads: vi.fn(),
  waitLoads: vi.fn(),
  kills: vi.fn<(command: string) => boolean>(),
  wipes: vi.fn<(command: string) => boolean>(),
  waits: vi.fn<(command: string, timeout?: number) => string | undefined>(),
}));
vi.mock("node:os", async original => ({
  ...await original<typeof import("node:os")>(), homedir: fixture.home,
}));
vi.mock("node:fs", async original => ({
  ...await original<typeof import("node:fs")>(), existsSync: fixture.exists,
}));
vi.mock("../src/core/pattern-kill.js", async original => {
  fixture.patternLoads();
  const actual = await original<typeof import("../src/core/pattern-kill.js")>();
  fixture.kills.mockImplementation(actual.killsByPattern);
  fixture.wipes.mockImplementation(actual.wipesTmp);
  return { ...actual, killsByPattern: fixture.kills, wipesTmp: fixture.wipes };
});
vi.mock("../src/guards/foreground-wait.js", async original => {
  fixture.waitLoads();
  const actual = await original<typeof import("../src/guards/foreground-wait.js")>();
  fixture.waits.mockImplementation(actual.foregroundWaitRefusal);
  return { ...actual, foregroundWaitRefusal: fixture.waits };
});

beforeEach(() => {
  vi.clearAllMocks();
  fixture.disabled = false;
  fixture.exists.mockImplementation(() => fixture.disabled);
  vi.stubEnv("SMARTY_ALPHA", undefined);
});
afterEach(() => vi.unstubAllEnvs());

const allowed = [
  "ps aux | grep foo",
  "journalctl -u x | grep -i kill",
  "for p in a b; do echo $p; done",
  'echo "stopped: Bridge"',
  'git commit -m "pkill fix"',
  "sleep 400 && echo done",
  "echo pkill -f foo",
  'echo "killall node; kill $(pgrep -f x)"',
  "printf '%s\\n' 'pkill; killall; rm -rf /tmp/*'",
  "printf '%s\\n' 'kill $(pgrep -f x)'",
  'git commit -m "pkill fix; killall node; rm -rf /tmp/*"',
  "grep -n pkill file",
  "awk '/pkill|killall/ {print $0}' file",
  "ps aux | grep foo | awk '{print $2}'",
  "ps aux | grep foo | awk '{print $2}' | xargs kill",
  "for p in pkill killall; do echo \"$p\"; done",
  "pgrep -f x",
  "pgrep -f x | grep kill",
  "pgrep -f x | xargs echo kill",
  "pgrep -f x; kill 1234",
  "kill 1234",
  "kill $PID",
  "kill $(cat run.pid)",
  "kill $(echo pgrep -f x)",
  "kill '$(pgrep -f x)'",
  "# pkill -f foo\necho done",
  "cat <<'EOF'\npkill -f foo\nkillall node\nrm -rf /tmp/*\nEOF",
  "cat <<EOF\npkill -f foo\nkill $(pgrep -f x)\nrm -rf /var/tmp\nEOF",
  "cat <<-EOF\n\tpkill -f foo\n\tEOF\necho done",
  "cat <<'FIRST' <<SECOND\npkill -f foo\nFIRST\nkillall node\nSECOND\necho done",
  // Intentional limits: inline programs and variable provenance belong to full mode.
  "sh -c 'pkill -f foo'",
  'p=$(pgrep -f x); kill "$p"',
  "rm -rf /tmp/owned/subdir /var/tmp/owned/subdir",
  "rm -rf /tmp/owned/* /var/tmp/owned/.*",
  "rm -rf /tmp/tmp.exact",
  "rm -rf /tmp/*/subdir",
  "rm -f /tmp/*",
  "rm /var/tmp",
  "rm -rf '/tmp/*' \"/var/tmp/.*\"",
  "rm -rf /tmp/\\* /var/tmp/\\?",
  "echo 'rm -rf /tmp/*'",
  "printf '%s\\n' 'rm -rf /var/tmp'",
  "printf '%s' pkill >/tmp/output",
  "echo done >pkill",
];

const refusedKills = [
  "pkill -f foo",
  "killall node",
  "/usr/bin/pkill -f foo",
  '"killall" node',
  "echo ready && killall node",
  'for p in a b; do pkill -f "$p"; done',
  "command pkill -f foo",
  "env LABEL=example killall node",
  "kill $(pgrep -f x)",
  'kill "$(pgrep -f x)"',
  "kill `pgrep -f x`",
  "kill -TERM $(/usr/bin/pgrep -f x)",
  "pgrep -f x | xargs kill",
  "pgrep -f x | xargs -r kill -9",
  "pgrep -f x | xargs -n 1 kill",
  "pgrep -f x | head -n 1 | xargs kill",
  "pgrep -f x |\n xargs kill",
  "pgrep -f x | kill",
  "cat <<'EOF'\necho pkill\nEOF\npkill -f foo",
  "2>/dev/null pkill -f foo",
];
const refusedTmp = [
  "rm -rf /tmp/*",
  "rm -rf /var/tmp",
  "rm -rf /tmp",
  "rm -rf /tmp/ /var/tmp/",
  'rm -rf "/tmp"',
  "rm -rf '/var/tmp'",
  "rm -fr /tmp/.*",
  "rm --recursive -- /tmp/tmp.*",
  "rm -R /tmp/[ab]*",
  'rm -rf "/tmp"/*',
  "rm -rf /var/tmp/?.log",
  "rm -rf /tmp/*/",
];

// Sequential so the cold-import assertion precedes the first full-mode import.
describe.sequential("bash guard policy", () => {
  it("keeps full guard modules unloaded through import and narrow first use", async () => {
    expect(fixture.patternLoads).not.toHaveBeenCalled();
    expect(fixture.waitLoads).not.toHaveBeenCalled();
    expect(await bashGuardRefusal("sleep 400 && echo done")).toBeUndefined();
    expect(await bashGuardRefusal("pkill -f foo")).toMatch(/Blocked:/);
    expect(fixture.patternLoads).not.toHaveBeenCalled();
    expect(fixture.waitLoads).not.toHaveBeenCalled();
    expect(fixture.kills).not.toHaveBeenCalled();
    expect(fixture.wipes).not.toHaveBeenCalled();
    expect(fixture.waits).not.toHaveBeenCalled();
  });

  it.each([undefined, "1", "", "true", "00", " 0", "0 "])("uses narrow mode for env %s", async value => {
    vi.stubEnv("SMARTY_ALPHA", value);
    expect(await bashGuardRefusal("sleep 400 && echo done", 900)).toBeUndefined();
    expect(await bashGuardRefusal("kill $(ps aux | grep x | awk '{print $2}')")).toBeUndefined();
    expect(await bashGuardRefusal("pkill -f foo")).toMatch(/Blocked:/);
    expect(fixture.kills).not.toHaveBeenCalled();
    expect(fixture.wipes).not.toHaveBeenCalled();
    expect(fixture.waits).not.toHaveBeenCalled();
    expect(fixture.home).toHaveBeenCalledTimes(3);
    expect(fixture.exists).toHaveBeenCalledTimes(3);
    expect(fixture.exists).toHaveBeenLastCalledWith(join("/fixture-home", ".config/smarty/bash-guard.disabled"));
  });

  it.each(allowed)("allows narrow command data: %s", async command => {
    for (const value of [undefined, "1"]) {
      vi.stubEnv("SMARTY_ALPHA", value);
      expect(await bashGuardRefusal(command)).toBeUndefined();
    }
    expect(fixture.kills).not.toHaveBeenCalled();
    expect(fixture.wipes).not.toHaveBeenCalled();
    expect(fixture.waits).not.toHaveBeenCalled();
  });

  it.each(refusedKills)("refuses a narrow pattern kill: %s", async command => {
    expect(await bashGuardRefusal(command)).toMatch(/Blocked:.*pgrep/);
  });
  it.each(refusedTmp)("refuses recursive root deletion: %s", async command => {
    expect(await bashGuardRefusal(command)).toMatch(/Blocked: recursive deletion/);
  });

  it("uses original full reasons in kill, tmp, then wait order for exact zero", async () => {
    vi.stubEnv("SMARTY_ALPHA", "0");
    const kill = await bashGuardRefusal("sleep 400; rm -rf /tmp/*; pkill -f foo");
    const { PATTERN_KILL_REASON, TMP_WIPE_REASON } = await import("../src/core/pattern-kill.js");
    expect(kill).toBe(PATTERN_KILL_REASON);
    expect(fixture.wipes).not.toHaveBeenCalled();
    expect(fixture.waits).not.toHaveBeenCalled();
    expect(await bashGuardRefusal("sleep 400; rm -rf /tmp/*")).toBe(TMP_WIPE_REASON);
    expect(fixture.waits).not.toHaveBeenCalled();
    expect(await bashGuardRefusal("sleep 400 && echo done")).toMatch(/foreground wait/);
    expect(fixture.waits).toHaveBeenLastCalledWith("sleep 400 && echo done", undefined);
    expect(await bashGuardRefusal("sleep 400 && echo done", 120)).toBeUndefined();
    expect(fixture.waits).toHaveBeenLastCalledWith("sleep 400 && echo done", 120);
    expect(await bashGuardRefusal("kill $(ps aux | grep x | awk '{print $2}')")).toBe(PATTERN_KILL_REASON);
  });

  it("lets the disable file override zero without disabling narrow safety checks", async () => {
    vi.stubEnv("SMARTY_ALPHA", "0");
    fixture.disabled = true;
    expect(await bashGuardRefusal("sleep 400 && echo done")).toBeUndefined();
    expect(await bashGuardRefusal("pkill -f foo")).toMatch(/Blocked:.*pgrep/);
    expect(await bashGuardRefusal("rm -rf /tmp/tmp.*")).toMatch(/Blocked: recursive deletion/);
    expect(fixture.kills).not.toHaveBeenCalled();
    expect(fixture.wipes).not.toHaveBeenCalled();
    expect(fixture.waits).not.toHaveBeenCalled();
  });

  it("rechecks environment and file on every call after the first import", async () => {
    const command = "sleep 400 && echo done";
    expect(await bashGuardRefusal(command)).toBeUndefined();
    vi.stubEnv("SMARTY_ALPHA", "0");
    expect(await bashGuardRefusal(command)).toMatch(/foreground wait/);
    fixture.disabled = true;
    expect(await bashGuardRefusal(command)).toBeUndefined();
    fixture.disabled = false;
    expect(await bashGuardRefusal(command)).toMatch(/foreground wait/);
    vi.stubEnv("SMARTY_ALPHA", "1");
    expect(await bashGuardRefusal(command)).toBeUndefined();
    vi.stubEnv("SMARTY_ALPHA", "0");
    expect(await bashGuardRefusal(command)).toMatch(/foreground wait/);
    vi.stubEnv("SMARTY_ALPHA", undefined);
    expect(await bashGuardRefusal(command)).toBeUndefined();
    expect(fixture.exists).toHaveBeenCalledTimes(7);
    expect(fixture.home).toHaveBeenCalledTimes(7);
  });
});
