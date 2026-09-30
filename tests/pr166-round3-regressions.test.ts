import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// Scanner DATA ONLY. Never execute these strings or use them as shell source.
// 4242 is a benign recorded PID literal, not a process discovered by a test.
// F17-F20: each refusal has an independent SIMPLE allowance. Unsupported
// formatter options/escapes may widen to common UNKNOWN (both provenance
// possibilities); only the policy with an actual consumer is asserted here.
// The allowance halves deliberately contain no unsupported formatter/binder.
const pairs = [
  {
    finding: "F17 option terminator capture TMP",
    refusal: `D=$(printf -- '%s' /tmp); rm -rf "$D"`,
    simple: `D=$(printf '%s' /own); rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F17 option terminator saved-file TMP",
    refusal: `printf -- '%s\\n' /tmp > .local/paths; xargs -a .local/paths rm -rf`,
    simple: `printf '%s\\n' /own > .local/paths; xargs -a .local/paths rm -rf`,
    blocked: false, wipe: true,
  },
  {
    finding: "F17 double-quoted newline capture TMP",
    refusal: `D=$(printf "%s\\n" /tmp); rm -rf "$D"`,
    simple: `D=$(printf '%s\\n' /own); rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F17 double-quoted newline saved-file TMP",
    refusal: `printf "%s\\n" /tmp > .local/paths; xargs -a .local/paths rm -rf`,
    simple: `printf '%s\\n' /own > .local/paths; xargs -a .local/paths rm -rf`,
    blocked: false, wipe: true,
  },
  {
    finding: "F18 dynamic destination no prior D",
    refusal: `N=D; printf -v "$N" '%s' /tmp; rm -rf "$D"`,
    simple: `D=/own; rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F18 dynamic destination no prior P",
    refusal: `N=P; printf -v "$N" '%s' "$(pgrep worker)"; kill "$P"`,
    simple: `P=4242; kill "$P"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F19 attached destination stale safe D",
    refusal: `D=/own; printf -vD '%s' /tmp; rm -rf "$D"`,
    simple: `D=/own; rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F19 attached destination stale safe P",
    refusal: `P=4242; printf -vP '%s' "$(pgrep worker)"; kill "$P"`,
    simple: `P=4242; kill "$P"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F19 attached destination no prior D",
    refusal: `printf -vD '%s' /tmp; rm -rf "$D"`,
    simple: `D=/own; rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F19 attached destination no prior P",
    refusal: `printf -vP '%s' "$(pgrep worker)"; kill "$P"`,
    simple: `P=4242; kill "$P"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F20 failed earlier redirect before truncate TMP",
    refusal: `printf '%s\\n' /tmp > .local/paths; : < /dev/null/pr166-r3-input > .local/paths; xargs -a .local/paths rm -rf`,
    simple: `printf '%s\\n' /tmp > .local/paths; printf '%s\\n' /own > .local/paths; xargs -a .local/paths rm -rf`,
    blocked: false, wipe: true,
  },
  {
    finding: "F20 failed earlier redirect before truncate PID",
    refusal: `pgrep worker > .local/pids; : < /dev/null/pr166-r3-input > .local/pids; kill "$(cat .local/pids)"`,
    simple: `pgrep worker > .local/pids; printf '%s\\n' 4242 > .local/pids; kill "$(cat .local/pids)"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F20 failed earlier redirect before replacement TMP",
    refusal: `printf '%s\\n' /tmp > .local/paths; printf '%s\\n' /own < /dev/null/pr166-r3-input > .local/paths; xargs -a .local/paths rm -rf`,
    simple: `printf '%s\\n' /tmp > .local/paths; printf '%s\\n' /own > .local/paths; xargs -a .local/paths rm -rf`,
    blocked: false, wipe: true,
  },
  {
    finding: "F20 failed earlier redirect before replacement PID",
    refusal: `pgrep worker > .local/pids; printf '%s\\n' 4242 < /dev/null/pr166-r3-input > .local/pids; kill "$(cat .local/pids)"`,
    simple: `pgrep worker > .local/pids; printf '%s\\n' 4242 > .local/pids; kill "$(cat .local/pids)"`,
    blocked: true, wipe: false,
  },
] as const;

function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  const killWrapper = killsByPattern(command);
  const wipeWrapper = wipesTmp(command);
  expect(result, command).toEqual({ blocked, wipe, exhausted: false });
  expect(killWrapper, command).toBe(result.blocked);
  expect(wipeWrapper, command).toBe(result.wipe);
}

describe("PR166 round3 F17-F20 bounded regressions (DATA ONLY)", () => {
  for (const { finding, refusal, simple, blocked, wipe } of pairs) {
    it(`${finding}: R refusal`, () => check(refusal, blocked, wipe));
    it(`${finding}: SIMPLE A allowance`, () => check(simple, false, false));
  }
});
