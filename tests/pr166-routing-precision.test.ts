import { describe, expect, it } from "vitest";
import { expectRound14Migration } from "./pr166-round14-policy.js";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
// R13 intentional A→R cuts; original tuple IDs and bytes remain unchanged.
const round5IntentionalState = new Set<string>([
  'cd /own; pushd -n /tmp; popd -n; popd; rm -rf tmp.*',
  'cd /tmp; pushd /own; pushd /own; popd; rm -rf tmp.*',
]);
function expectRound5Guard(command: string, result: ReturnType<typeof scanCommand>, original: { blocked?: boolean; wipe?: boolean; exhausted?: boolean; overall?: boolean }): void {
  if (expectRound14Migration(command, result)) return;
  const intentional = round5IntentionalState.has(command);
  const originallyRefused = original.blocked === true || original.wipe === true || original.exhausted === true || original.overall === true;
  if (!intentional && !(originallyRefused && result.shellState === true)) expect("shellState" in result, command).toBe(false);
  if (intentional || (originallyRefused && result.shellState === true)) {
    expect(Object.prototype.hasOwnProperty.call(result, "shellState"), command).toBe(true);
    expect(result, command).toEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  } else if (original.overall !== undefined) {
    expect(Object.keys(result).sort(), command).toEqual(["blocked", "exhausted", "wipe"]);
    expect(result.exhausted, command).toBe(original.exhausted ?? false);
    expect(typeof result.blocked, command).toBe("boolean");
    expect(typeof result.wipe, command).toBe("boolean");
    expect(result.blocked || result.wipe, command).toBe(original.overall);
  } else {
    expect(result, command).toEqual(original);
  }
  expect(killsByPattern(command), command).toBe(result.blocked || result.shellState === true);
  expect(wipesTmp(command), command).toBe(result.wipe || result.shellState === true);
}


// DATA ONLY: no command string is executed. Allowed commands are exact tuples
// from the quiescent .local/r1-probes.ts / r1-probes-final.log receipt.
// These precision controls distinguish definite replacement from a possibly
// skipped replacement or append, without discarding possible unsafe file routes.
type Pair = { name: string; policy: "kill" | "tmp"; refuse: string; allow: string };
const pairs: Pair[] = [
  {
    name: "unknown PID alias: recorded append retains lookup, definite recorded overwrite clears it",
    policy: "kill",
    refuse: `F=.local/pids; false && F=.local/else; pgrep worker > "$F"; cat .local/recorded.pid >> .local/pids; xargs -a .local/pids kill`,
    allow: `F=.local/pids; false && F=.local/else; pgrep worker > "$F"; cat .local/recorded.pid > .local/pids; xargs -a .local/pids kill`,
  },
  {
    name: "unknown PID alias: skipped truncate retains lookup, definite truncate clears it",
    policy: "kill",
    refuse: `F=.local/pids; false && F=.local/else; pgrep worker > "$F"; false && : > .local/pids; xargs -a .local/pids kill`,
    allow: `F=.local/pids; false && F=.local/else; pgrep worker > "$F"; : > .local/pids; xargs -a .local/pids kill`,
  },
  {
    name: "unknown shared-list alias: own append retains unsafe paths, definite own overwrite clears them",
    policy: "tmp",
    refuse: `F=.local/dirs; false && F=.local/else; ls /tmp > "$F"; cat .local/own.list >> .local/dirs; xargs -a .local/dirs rm -rf`,
    allow: `F=.local/dirs; false && F=.local/else; ls /tmp > "$F"; cat .local/own.list > .local/dirs; xargs -a .local/dirs rm -rf`,
  },
  {
    name: "regular descriptor observes later unsafe bytes on ambiguous route, recorded overwrite remains allowed",
    policy: "kill",
    refuse: `cd /own; false && cd /else; { pgrep worker > pids; read -ru3 P; kill "$P"; } 3< pids`,
    allow: `pgrep worker > .local/pids; { cat .local/recorded.pid > .local/pids; read -ru3 P; kill "$P"; } 3< .local/pids`,
  },
  {
    name: "conditional unknown prior target cannot narrow away lookup, definite concrete known overwrite clears it",
    policy: "kill",
    refuse: `F=$(cat .local/filename); false && F=.local/else; pgrep worker > "$F"; cat .local/pids | xargs kill`,
    allow: String.raw`F=$(cat .local/filename); false && F=.local/else; pgrep worker > "$F"; printf '%s\n' 4242 > .local/pids; xargs -a .local/pids kill`,
  },
  {
    name: "unknown shared-list alias: skipped truncate retains unsafe paths, definite own overwrite clears them",
    policy: "tmp",
    refuse: `F=.local/dirs; false && F=.local/else; ls /tmp > "$F"; false && : > .local/dirs; xargs -a .local/dirs rm -rf`,
    allow: `F=.local/dirs; false && F=.local/else; ls /tmp > "$F"; cat .local/own.list > .local/dirs; xargs -a .local/dirs rm -rf`,
  },
  {
    name: "conditional popd-n retains shared stack destination for later relative glob",
    policy: "tmp",
    refuse: `cd /own; pushd -n /tmp; false && popd -n; popd; rm -rf tmp.*`,
    allow: `cd /own; pushd -n /tmp; popd -n; popd; rm -rf tmp.*`,
  },
  {
    name: "conditional same-cwd pushd retains earlier shared stack entry for later popd",
    policy: "tmp",
    refuse: `cd /tmp; pushd /own; false && pushd /own; popd; rm -rf tmp.*`,
    allow: `cd /tmp; pushd /own; pushd /own; popd; rm -rf tmp.*`,
  },
];

// Conservative false positives: read/fd provenance UNKNOWN by owner direction.
// Original corpus IDs and command bytes remain unchanged, including historical — allow IDs.
const conservativeFalsePositives = new Set([
  "regular descriptor observes later unsafe bytes on ambiguous route, recorded overwrite remains allowed",
]);

describe("PR166 routing precision — paired DATA only", () => {
  for (const pair of pairs) {
    for (const disposition of ["refuse", "allow"] as const) {
      it(`${pair.name} — ${disposition}`, () => {
        const command = pair[disposition];
        const refused = disposition === "refuse" || conservativeFalsePositives.has(pair.name);
        const blocked = refused && pair.policy === "kill";
        const wipe = refused && pair.policy === "tmp";
        expectRound5Guard(command, scanCommand(command), { blocked, wipe, exhausted: false });
        void 0;
        void 0;
      });
    }
  }
});
