import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// Scanner DATA ONLY: none of these strings may be submitted to a shell.
// The owner requires UNKNOWN/fail-closed for unsupported provenance, not a
// precise shell interpreter. Each refusal has a SIMPLE known-safe counterpart.
// In particular, all-owned fd/read and declaration-array examples are NOT
// allowance promises under that policy; the counterparts avoid those constructs.
const pairs = [
  ["Astra1 export path prefix", 'D=/own; D=/tmp export D; rm -rf "$D"', 'D=/own; rm -rf "$D"'],
  ["Astra1 readonly path prefix", 'D=/own; D=/tmp readonly D; rm -rf "$D"', 'D=/own; rm -rf "$D"'],
  ["Astra1 export PID prefix", 'P=4242; P=$(pgrep worker) export P; kill "$P"', 'P=4242; kill "$P"'],
  ["Astra1 readonly PID prefix", 'P=4242; P=$(pgrep worker) readonly P; kill "$P"', 'P=4242; kill "$P"'],
  ["Astra2 F13 closed fd PID", 'P=$(pgrep worker); read -r -u 3 P 3<&-; kill "$P"', 'P=4242; kill "$P"'],
  ["Astra2 F13 closed fd path", 'D=/tmp; read -r -u 3 D 3<&-; rm -rf "$D"', 'D=/own; rm -rf "$D"'],
  ["F13 missing fd PID", 'P=$(pgrep worker); read -r -u 3 P; kill "$P"', 'P=4242; kill "$P"'],
  ["F13 missing fd path", 'D=/tmp; read -r -u 3 D; rm -rf "$D"', 'D=/own; rm -rf "$D"'],
  ["F13 availability-only PID", 'P=$(pgrep worker); read -r -t 0 P <<< 4242; kill "$P"', 'P=4242; kill "$P"'],
  ["F13 availability-only path", 'D=/tmp; read -r -t 0 D <<< /own; rm -rf "$D"', 'D=/own; rm -rf "$D"'],
  ["Astra3 F16 repeated descriptor read", "printf '%s\\n' /own /tmp > .local/paths; { read -r D; read -r D; rm -rf \"$D\"; } < .local/paths", 'D=/own; rm -rf "$D"'],
  ["Astra3 loop descriptor read", "printf '%s\\n' /own /tmp > .local/paths; while read -r D; do rm -rf \"$D\"; done < .local/paths", 'D=/own; rm -rf "$D"'],
  ["F14 pipeline safe read unsafe PID parent", 'P=$(pgrep worker); printf "%s\\n" 4242 | read -r P; kill "$P"', 'P=$(pgrep worker); P=4242; kill "$P"'],
  ["F14 pipeline safe read unsafe path parent", 'D=/tmp; printf "%s\\n" /own | read -r D; rm -rf "$D"', 'D=/tmp; D=/own; rm -rf "$D"'],
  ["F14 background safe assignment unsafe PID parent", 'P=$(pgrep worker); P=4242 & wait; kill "$P"', 'P=$(pgrep worker); P=4242; kill "$P"'],
  ["F14 background safe assignment unsafe path parent", 'D=/tmp; D=/own & wait; rm -rf "$D"', 'D=/tmp; D=/own; rm -rf "$D"'],
  ["F15 unlinked recreated pathname open PID fd", 'pgrep worker > .local/pids; { rm -f .local/pids; printf "%s\\n" 4242 > .local/pids; read -r -u 3 P; kill "$P"; } 3< .local/pids', 'printf "%s\\n" 4242 > .local/pids; kill 4242'],
  ["F15 unlinked recreated pathname open path fd", 'printf "%s\\n" /tmp > .local/paths; { rm -f .local/paths; printf "%s\\n" /own > .local/paths; read -r -u 3 D; rm -rf "$D"; } 3< .local/paths', 'printf "%s\\n" /own > .local/paths; D=/own; rm -rf "$D"'],
  ["F2 declaration array shared glob", 'declare -a D=(/tmp/*); rm -rf "${D[@]}"', 'D=/own; rm -rf "$D"'],
  ["Astra4 echo option capture", 'D=$(echo -n /tmp); rm -rf "$D"', 'D=$(echo /own); rm -rf "$D"'],
  ["Astra4 tail transformed capture", "D=$(printf '%s\\n' /own /tmp | tail -n 1); rm -rf \"$D\"", "D=$(printf '%s\\n' /own); rm -rf \"$D\""],
  ["unsupported tr sed cut capture", "D=$(printf '%s\\n' /tmp | tr -d '\\n' | sed 's@^@/@' | cut -c 2-); rm -rf \"$D\"", "D=$(printf '%s\\n' /own); rm -rf \"$D\""],
  ["unsupported head tac capture", "D=$(printf '%s\\n' /own /tmp | tac | head -n 1); rm -rf \"$D\"", 'D=$(echo /own); rm -rf "$D"'],
  ["unsupported printf percent-q capture", "D=$(printf '%q' /tmp); rm -rf \"$D\"", "D=$(printf '%s' /own); rm -rf \"$D\""],
] as const;

function check(command: string, refusal: boolean): void {
  const result = scanCommand(command);
  const blocked = killsByPattern(command);
  const wipe = wipesTmp(command);
  // Check both public wrappers against the complete verdict, not just the
  // destructive guard expected from the spelling of this particular command.
  expect(result.exhausted, command).toBe(false);
  expect(blocked, command).toBe(result.blocked);
  expect(wipe, command).toBe(result.wipe);
  expect(blocked || wipe, command).toBe(refusal);
}

describe("PR166 round2 owner-authorized UNKNOWN boundaries (DATA ONLY)", () => {
  for (const [finding, unsafe, simple] of pairs) {
    it(`${finding}: no execution grant`, () => check(unsafe, true));
    it(`${finding}: simple known-safe allowance`, () => check(simple, false));
  }
});
