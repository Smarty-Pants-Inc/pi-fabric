import { describe, expect, it } from "vitest";
import { scanCommand } from "../src/core/pattern-kill.js";

// DATA ONLY: never execute these shell strings, including their harmless counterparts.
// Each half is an independent assertion. A regular-file fd fixes identity, not bytes;
// a process-substitution fd carries bytes, not a request to open the filename they spell.
type Pair = { name: string; policy: "kill" | "tmp"; refuse: string; allow: string };
const pairs: Pair[] = [
  { name: "F11 exact conditional cwd saved-file consumer", policy: "kill",
    refuse: `cd /own; pgrep worker > pids; false && cd /else; cat pids | xargs kill`,
    allow: `cd /own; pgrep worker > pids; cd /else; cat pids | xargs kill` },
  { name: "F11 exact conditional cwd fd3 mirror", policy: "kill",
    refuse: `cd /own; pgrep worker > pids; false && cd /else; { read -r -u 3 P; kill "$P"; } 3< pids`,
    allow: `cd /own; pgrep worker > pids; cd /else; { read -r -u 3 P; kill "$P"; } 3< pids` },
  { name: "A1 F11 nonmatching case cannot overwrite parent PID", policy: "kill",
    refuse: `P=$(pgrep worker); case no in yes) P=4242 ;; esac; kill "$P"`,
    allow: `P=$(pgrep worker); case no in yes) P=4242 ;; esac; P=$!; kill "$P"` },
  { name: "F11 inverse conditional cwd cannot truncate absolute PID file", policy: "kill",
    refuse: `cd /else; pgrep worker > /work/pids; false && cd /work; : > pids; xargs -a /work/pids kill`,
    allow: `cd /else; pgrep worker > /work/pids; cd /work; : > pids; xargs -a /work/pids kill` },
  { name: "F11 zero-iteration until retains PID provenance", policy: "kill",
    refuse: `P=$(pgrep worker); until true; do P=4242; done; kill "$P"`,
    allow: `P=$(pgrep worker); until true; do P=4242; done; P=$!; kill "$P"` },
  { name: "F11 inverse conditional cwd cannot truncate absolute shared-list file", policy: "tmp",
    refuse: `cd /else; ls -d /tmp/tmp.* > /work/paths; false && cd /work; : > paths; xargs -a /work/paths rm -rf`,
    allow: `cd /else; ls -d /tmp/tmp.* > /work/paths; cd /work; : > paths; xargs -a /work/paths rm -rf` },
  { name: "A1 subshell PID binding cannot clear parent provenance", policy: "kill",
    refuse: `P=$(pgrep worker); ( P=4242 ); kill "$P"`,
    allow: `P=$(pgrep worker); ( P=4242 ); P=$!; kill "$P"` },
  { name: "A1 subshell path binding cannot clear parent provenance", policy: "tmp",
    refuse: `D=/tmp; ( D=/own ); rm -rf "$D"`,
    allow: `D=/tmp; ( D=/own ); D=/own; rm -rf "$D"` },
  { name: "F12 computed regular fd3 target sees after-entry PID producer", policy: "kill",
    refuse: `: > .local/pids; { pgrep worker > .local/pids; read -r -u 3 P; kill "$P"; } 3< "$(printf .local/pids)"`,
    allow: `: > .local/pids; { cat .local/recorded.pid > .local/pids; read -r -u 3 P; kill "$P"; } 3< "$(printf .local/pids)"` },
  { name: "F12 computed regular fd3 target sees after-entry shared producer", policy: "tmp",
    refuse: `: > .local/paths; { ls -d /tmp/tmp.* > .local/paths; read -r -u 3 D; rm -rf "$D"; } 3< "$(printf .local/paths)"`,
    allow: `: > .local/paths; { cat .local/owned.list > .local/paths; read -r -u 3 D; rm -rf "$D"; } 3< "$(printf .local/paths)"` },
  { name: "F12 immutable process feed is PID bytes not saved filename contents", policy: "kill",
    refuse: `pgrep worker > .local/pids; { read -r -u 3 P; kill "$P"; } 3< <(pgrep worker)`,
    allow: `pgrep worker > .local/pids; { read -r -u 3 P; kill "$P"; } 3< <(printf '.local/pids\\n')` },
  { name: "F12 immutable process feed is path bytes not saved filename contents", policy: "tmp",
    refuse: `ls -d /tmp/tmp.* > .local/paths; { read -r -u 3 D; rm -rf "$D"; } 3< <(ls -d /tmp/tmp.*)`,
    allow: `ls -d /tmp/tmp.* > .local/paths; { read -r -u 3 D; rm -rf "$D"; } 3< <(printf '.local/paths\\n')` },
  { name: "A1 exact doublequoted shell payload expands in caller", policy: "tmp",
    refuse: String.raw`D=/tmp; D=/own bash -c "rm -rf \"$D\""`,
    allow: String.raw`D=/own; D=/tmp bash -c "rm -rf \"$D\""` },
  { name: "A1 exact doublequoted eval payload expands in caller", policy: "tmp",
    refuse: String.raw`D=/tmp; D=/own eval "rm -rf \"$D\""`,
    allow: String.raw`D=/own; D=/tmp eval "rm -rf \"$D\""` },
  { name: "A1 exact doublequoted shell PID payload preserves caller lookup", policy: "kill",
    refuse: String.raw`P=$(pgrep worker); P=4242 bash -c "kill \"$P\""`,
    allow: String.raw`P=4242; P=7777 bash -c "kill \"$P\""` },
  { name: "A1 singlequoted shell payload instead sees temporary child binding", policy: "tmp",
    refuse: `D=/own; D=/tmp bash -c 'rm -rf "$D"'`,
    allow: `D=/tmp; D=/own bash -c 'rm -rf "$D"'` },
  { name: "F11 exact conditional alias unsafe PID write retains destination", policy: "kill",
    refuse: `F=.local/pids; false && F=.local/else; pgrep worker > "$F"; cat .local/pids | xargs kill`,
    allow: `F=.local/pids; F=.local/else; pgrep worker > "$F"; cat .local/pids | xargs kill` },
  { name: "F11 exact conditional alias unsafe shared-list write retains destination", policy: "tmp",
    refuse: `F=.local/paths; false && F=.local/else; ls -d /tmp/tmp.* > "$F"; cat .local/paths | xargs rm -rf`,
    allow: `F=.local/paths; F=.local/else; ls -d /tmp/tmp.* > "$F"; cat .local/paths | xargs rm -rf` },
  { name: "F11 exact conditional producer cwd retains PID write destination", policy: "kill",
    refuse: `cd /own; false && cd /else; pgrep worker > pids; cat /own/pids | xargs kill`,
    allow: `cd /own; cd /else; pgrep worker > pids; cat /own/pids | xargs kill` },
  { name: "F11 exact conditional producer cwd retains shared-list write destination", policy: "tmp",
    refuse: `cd /own; false && cd /else; ls -d /tmp/tmp.* > paths; cat /own/paths | xargs rm -rf`,
    allow: `cd /own; cd /else; ls -d /tmp/tmp.* > paths; cat /own/paths | xargs rm -rf` },
];

// Conservative false positives: read/fd provenance UNKNOWN by owner direction.
// Original corpus IDs and command bytes remain unchanged, including historical — allow IDs.
const conservativeFalsePositives = new Set([
  "F11 exact conditional cwd fd3 mirror",
  "F12 computed regular fd3 target sees after-entry PID producer",
  "F12 computed regular fd3 target sees after-entry shared producer",
  "F12 immutable process feed is PID bytes not saved filename contents",
  "F12 immutable process feed is path bytes not saved filename contents",
]);

describe("PR166 lifecycle boundaries — paired DATA only", () => {
  for (const pair of pairs) {
    it(`${pair.name} — refuse`, () => {
      expect(scanCommand(pair.refuse)).toEqual({
        blocked: pair.policy === "kill", wipe: pair.policy === "tmp", exhausted: false,
      });
    });
    it(`${pair.name} — allow`, () => {
      expect(scanCommand(pair.allow)).toEqual({
        blocked: conservativeFalsePositives.has(pair.name) && pair.policy === "kill",
        wipe: conservativeFalsePositives.has(pair.name) && pair.policy === "tmp",
        exhausted: false,
      });
    });
  }
});
