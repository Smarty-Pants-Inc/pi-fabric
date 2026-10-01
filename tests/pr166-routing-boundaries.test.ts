import { describe, expect, it } from "vitest";
import { expectRound16Migration } from "./pr166-round16-policy.js";
import { scanCommand, killsByPattern, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "D=/own; D=/tmp eval \"rm -rf \\\"$D\\\"\""
]);
function expectRound5Guard(command: string, result: ReturnType<typeof scanCommand>, original: { blocked?: boolean; wipe?: boolean; exhausted?: boolean; overall?: boolean }): void {
  if (expectRound16Migration(command, result)) return;
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


// DATA ONLY: these strings are scanner inputs, never executable shell probes.
// Doublequoted payloads expand in the caller before temporary prefix bindings;
// singlequoted payloads instead expand in the child with those bindings.
// Conditional alias/cwd changes retain the skipped route; only an unconditional
// change may move a write or truncate away from (or onto) the consumed file.
type Pair = { name: string; policy: "kill" | "tmp"; refuse: string; allow: string };
const pairs: Pair[] = [
  { name: "A1 routing doublequoted shell payload uses caller path", policy: "tmp",
    refuse: String.raw`D=/tmp; D=/own bash -c "rm -rf \"$D\""`,
    allow: String.raw`D=/own; D=/tmp bash -c "rm -rf \"$D\""` },
  { name: "A1 routing doublequoted eval payload uses caller path", policy: "tmp",
    refuse: String.raw`D=/tmp; D=/own eval "rm -rf \"$D\""`,
    allow: String.raw`D=/own; D=/tmp eval "rm -rf \"$D\""` },
  { name: "A1 routing doublequoted shell payload retains caller lookup PID", policy: "kill",
    refuse: String.raw`P=$(pgrep worker); P=4242 bash -c "kill \"$P\""`,
    allow: String.raw`P=4242; P=7777 bash -c "kill \"$P\""` },
  { name: "F11 routing conditional alias selects saved PID write", policy: "kill",
    refuse: `F=.local/pids; false && F=.local/else; pgrep worker > "$F"; cat .local/pids | xargs kill`,
    allow: `F=.local/pids; F=.local/else; pgrep worker > "$F"; cat .local/pids | xargs kill` },
  { name: "F11 routing conditional alias selects saved shared-list write", policy: "tmp",
    refuse: `F=.local/paths; false && F=.local/else; ls -d /tmp/tmp.* > "$F"; cat .local/paths | xargs rm -rf`,
    allow: `F=.local/paths; F=.local/else; ls -d /tmp/tmp.* > "$F"; cat .local/paths | xargs rm -rf` },
  { name: "F11 routing conditional cwd selects saved PID write", policy: "kill",
    refuse: `cd /own; false && cd /else; pgrep worker > pids; cat /own/pids | xargs kill`,
    allow: `cd /own; cd /else; pgrep worker > pids; cat /own/pids | xargs kill` },
  { name: "F11 routing conditional cwd selects saved shared-list write", policy: "tmp",
    refuse: `cd /own; false && cd /else; ls -d /tmp/tmp.* > paths; cat /own/paths | xargs rm -rf`,
    allow: `cd /own; cd /else; ls -d /tmp/tmp.* > paths; cat /own/paths | xargs rm -rf` },
  { name: "F11 routing inverse conditional cwd cannot truncate saved PID file", policy: "kill",
    refuse: `cd /else; pgrep worker > /work/pids; false && cd /work; : > pids; xargs -a /work/pids kill`,
    allow: `cd /else; pgrep worker > /work/pids; cd /work; : > pids; xargs -a /work/pids kill` },
  { name: "F11 routing inverse conditional cwd cannot truncate saved shared-list file", policy: "tmp",
    refuse: `cd /else; ls -d /tmp/tmp.* > /work/paths; false && cd /work; : > paths; xargs -a /work/paths rm -rf`,
    allow: `cd /else; ls -d /tmp/tmp.* > /work/paths; cd /work; : > paths; xargs -a /work/paths rm -rf` },
  { name: "A1 routing singlequoted shell payload uses temporary child path", policy: "tmp",
    refuse: `D=/own; D=/tmp bash -c 'rm -rf "$D"'`,
    allow: `D=/tmp; D=/own bash -c 'rm -rf "$D"'` },
];

describe("PR166 routing boundaries — paired DATA only", () => {
  for (const pair of pairs) {
    it(`${pair.name} — refuse`, () => {
      expectRound5Guard(pair.refuse, scanCommand(pair.refuse), {
        blocked: pair.policy === "kill", wipe: pair.policy === "tmp", exhausted: false,
      });
    });
    it(`${pair.name} — allow`, () => {
      expectRound5Guard(pair.allow, scanCommand(pair.allow), { blocked: false, wipe: false, exhausted: false });
    });
  }
});
