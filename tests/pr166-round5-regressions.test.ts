import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY. Never execute these strings, discover a PID, or invoke a shell.
// Owner scope cut: unsupported shell state refuses the WHOLE command, even
// without a destructive consumer. No assertion invents an unsafe allowance.
// Sixteen independent R/SIMPLE pairs; the boundary pairs include the named
// forms together. They are coverage of those command boundaries, not isolated
// attribution to every builtin. Literal 4242 is recorded test data only.
const pairs = [
  {
    finding: "-p export binding (Astra exact)",
    refusal: `D=/own; export -p D=/tmp; rm -rf "$D"`,
    simple: `D=/own; rm -rf "$D"`,
  },
  {
    finding: "-p readonly binding (Astra exact)",
    refusal: `D=/own; readonly -p D=/tmp; rm -rf "$D"`,
    simple: `D=$(mktemp -d); rm -rf "$D"`,
  },
  {
    finding: "-p readonly attribute (Astra exact)",
    refusal: `P=$(pgrep worker); readonly -p P; printf -v P '%s' 4242; kill "$P"`,
    simple: `P=4242; kill "$P"`,
  },
  {
    finding: "F24 readable redirect TMP (Astra exact)",
    refusal: `D=/tmp; readonly D < /dev/null; printf -v D '%s' /own; rm -rf "$D"`,
    simple: `D=/tmp; D=/own; rm -rf "$D"`,
  },
  {
    finding: "F24 persistent prefix TMP (Astra exact)",
    refusal: `D=/own; D=/tmp readonly D; printf -v D '%s' /own; rm -rf "$D"`,
    simple: `D=/own; D=/own; rm -rf "$D"`,
  },
  {
    finding: "F24 readable redirect PID (Astra exact)",
    refusal: `P=$(pgrep worker); readonly P < /dev/null; printf -v P '%s' 4242; kill "$P"`,
    simple: `P=$(pgrep worker); P=4242; kill "$P"`,
  },
  {
    finding: "F24 persistent prefix PID",
    refusal: `P=4242; P=$(pgrep worker) readonly P; printf -v P '%s' 4242; kill "$P"`,
    simple: `P=4242; P=4242; kill "$P"`,
  },
  {
    finding: "F25 UNKNOWN IFS capture (Astra exact)",
    refusal: `read -r IFS <<< :; D=:/tmp; R=$(printf '%s' $D); rm -rf "$R"`,
    simple: `R=$(printf '%s' /own); rm -rf "$R"`,
  },
  {
    finding: "F25 UNKNOWN IFS direct binder (Astra exact)",
    refusal: `read -r IFS <<< :; D=:/tmp; printf -v R '%s' $D; rm -rf "$R"`,
    simple: `R=/own; rm -rf "$R"`,
  },
  {
    finding: "whole-command bare readonly and export diagnostics",
    refusal: `D=/own; readonly D; export -p; readonly -p; export D`,
    simple: `D=/own; printf '%s' "$D"`,
  },
  {
    finding: "whole-command declare and typeset",
    refusal: `declare D=/own; typeset P=4242`,
    simple: `D=/own; P=4242`,
  },
  {
    finding: "whole-command unsupported set option",
    refusal: `set -e; printf '%s' /own`,
    simple: `printf '%s' /own`,
  },
  {
    finding: "whole-command IFS assignment",
    refusal: `IFS=:; printf '%s' /own`,
    simple: `D=/own; printf '%s' "$D"`,
  },
  {
    finding: "whole-command read mapfile readarray",
    refusal: `read -r D <<< /own; mapfile -t A <<< /own; readarray -t B <<< /own`,
    simple: `D=$(mktemp -d); rm -f "$D"/*.json`,
  },
  {
    finding: "whole-command printf -v and nonliteral format",
    refusal: `F='%s'; printf -v D '%s' /own; printf "$F" /own`,
    simple: `D=/tmp; D=/own; printf '%s' "$D"`,
  },
  {
    finding: "whole-command eval source dot local receiver",
    refusal: `bash -c 'eval ":"; source .local/state; . .local/state; local D=/own'`,
    simple: `bash -c 'D=/own; rm -rf "$D"'`,
  },
] as const;

function check(command: string, refusal: boolean): void {
  const result = scanCommand(command);
  // Main-approved API: unsupported state is a separate whole-command refusal.
  const shellState = (result as typeof result & { shellState?: true }).shellState;
  const killWrapper = killsByPattern(command);
  const wipeWrapper = wipesTmp(command);
  expect.soft(typeof result.blocked, command).toBe("boolean");
  expect.soft(typeof result.wipe, command).toBe("boolean");
  expect.soft(result.exhausted, command).toBe(false);
  expect.soft(shellState === undefined || shellState === true, command).toBe(true);
  expect.soft(killWrapper, command).toBe(result.blocked || shellState === true);
  expect.soft(wipeWrapper, command).toBe(result.wipe || shellState === true);
  expect.soft(result.blocked || result.wipe || shellState === true, command).toBe(refusal);
  if (refusal) {
    // Sentinel preflight precedes consumer grants; do not fabricate policy bits.
    expect.soft(result, command).toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  } else {
    expect.soft(result, command).toEqual({ blocked: false, wipe: false, exhausted: false });
  }
}

describe("PR166 round5 scope-cut regressions (DATA ONLY)", () => {
  for (const { finding, refusal, simple } of pairs) {
    it(`${finding}: R refusal`, () => check(refusal, true));
    it(`${finding}: SIMPLE A allowance`, () => check(simple, false));
  }
});
