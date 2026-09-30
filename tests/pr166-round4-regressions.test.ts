import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// Scanner DATA ONLY: never execute a command string or pass one to a shell.
// 4242 is a recorded literal, not a PID discovered by this suite.
// Each R has an independent SIMPLE-A control. UNKNOWN can carry both possible
// origins, but the expected policy bit follows the actual destructive consumer.
const pairs = [
  {
    finding: "F21 expanded format capture (exact Astra bytes)",
    refusal: `F='%s /tmp'; D=$(printf $F); rm -rf "$D"`,
    simple: `F='%s /own'; D=$(printf "$F"); rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F21 expanded format -v (exact Astra bytes)",
    refusal: `F='%s /tmp'; printf -v D $F; rm -rf "$D"`,
    simple: `F='%s /own'; printf -v D "$F"; rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F21 IFS-split format capture",
    refusal: `IFS=:; F='%s:/tmp'; D=$(printf $F); rm -rf "$D"`,
    simple: `D=$(printf '%s' /own); rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F21 IFS-split format -v",
    refusal: `IFS=:; F='%s:/tmp'; printf -v D $F; rm -rf "$D"`,
    simple: `printf -v D '%s' /own; rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F22 echo unknown read destination no prior D (exact Astra bytes)",
    refusal: `N=$(echo -n D); read -r "$N" <<< /tmp; rm -rf "$D"`,
    simple: `D=/own; rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F22 echo unknown read destination no prior P (exact Astra bytes)",
    refusal: `N=$(echo -n P); read -r "$N" < <(pgrep worker); kill "$P"`,
    simple: `P=4242; kill "$P"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F22 cat unknown read destination no prior D",
    refusal: `N=$(cat .local/destination); read -r "$N" <<< /tmp; rm -rf "$D"`,
    simple: `D=$(printf '%s' /own); rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F22 read unknown read destination no prior P",
    refusal: `read -r N <<< P; read -r "$N" < <(pgrep worker); kill "$P"`,
    simple: `P=$(printf '%s' 4242); kill "$P"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F22 unknown mapfile destination no prior D",
    refusal: `read -r N <<< D; mapfile -t "$N" <<< /tmp; rm -rf "\${D[@]}"`,
    simple: `printf -v D '%s' /own; rm -rf "$D"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F22 unknown readarray destination no prior P",
    refusal: `read -r N <<< P; readarray -t "$N" < <(pgrep worker); kill "\${P[@]}"`,
    simple: `printf -v P '%s' 4242; kill "$P"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F22 mixed resolved and unknown read destinations no prior P",
    refusal: `read -r N <<< P; read -r D "$N" < <(pgrep worker); kill "$P"`,
    simple: `P=4242; kill "$P"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F23 diagnostic set preserves TMP positionals",
    refusal: `set -- /tmp; set; rm -rf "$1"`,
    simple: `set -- /tmp; set -- /own; rm -rf "$1"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F23 diagnostic set preserves lookup positionals",
    refusal: `set -- $(pgrep worker); set; kill "$1"`,
    simple: `set -- $(pgrep worker); set -- 4242; kill "$1"`,
    blocked: true, wipe: false,
  },
  {
    finding: "F23 invalid option cannot erase TMP positionals",
    refusal: `set -- /tmp; set -Z -- /own; rm -rf "$1"`,
    simple: `set -- /tmp; set --; rm -rf "$1"`,
    blocked: false, wipe: true,
  },
  {
    finding: "F23 invalid option cannot erase lookup positionals",
    refusal: `set -- $(pgrep worker); set -Z -- 4242; kill "$1"`,
    simple: `set -- $(pgrep worker); set --; kill "$1"`,
    blocked: true, wipe: false,
  },
] as const;

function check(command: string, blocked: boolean, wipe: boolean): void {
  const result = scanCommand(command);
  const killWrapper = killsByPattern(command);
  const wipeWrapper = wipesTmp(command);
  // Compute/check all three entry points even when the frozen baseline is red.
  expect.soft(result, command).toEqual({ blocked, wipe, exhausted: false });
  expect.soft(killWrapper, command).toBe(blocked);
  expect.soft(wipeWrapper, command).toBe(wipe);
}

describe("PR166 round4 F21-F23 bounded regressions (DATA ONLY)", () => {
  for (const { finding, refusal, simple, blocked, wipe } of pairs) {
    it(`${finding}: R refusal`, () => check(refusal, blocked, wipe));
    it(`${finding}: SIMPLE A allowance`, () => check(simple, false, false));
  }
});
