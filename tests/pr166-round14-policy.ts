// R14/R15 owner class cuts: exact observed old commands only. No product whitelist.
// Original IDs/command matrices are retained. Values are explicit full refusals,
// never inferred from the scanner under test; unknown commands use old assertions.
import {expect} from "vitest";
import {killsByPattern,wipesTmp,type CommandGuardResult} from "../src/core/pattern-kill.js";
const migrations = new Map<string,CommandGuardResult>(
[
  [
    "kill -0 $(pgrep -f server)",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "printf '%s' /own > .local/prefix; printf '%s' /tmp >> .local/prefix; D=$(cat .local/prefix); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "cd /tmp; env pushd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; env cd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; pushd /tmp; env popd; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; env pushd /elsewhere; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; nice pushd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; nice cd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; pushd /tmp; nice popd; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; nice pushd /elsewhere; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; command pushd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; command cd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; pushd /tmp; command popd; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; command pushd /elsewhere; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; builtin pushd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; builtin cd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; pushd /tmp; builtin popd; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; builtin pushd /elsewhere; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; xargs pushd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /tmp; xargs cd /own; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; pushd /tmp; xargs popd; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "cd /own; xargs pushd /elsewhere; rm -rf *",
    {
      "blocked": false,
      "wipe": false,
      "exhausted": false,
      "shellState": true
    }
  ],
  [
    "date +/tmp > /own/date; printf %s /own > /own/date; D=$(cat /own/date); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "printf '%s\\n' /own > .local/paths; xargs -a .local/paths rm -rf",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "printf '%s\\n' /tmp > .local/paths; printf '%s\\n' /own > .local/paths; xargs -a .local/paths rm -rf",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "pgrep worker > .local/pids; printf '%s\\n' 4242 > .local/pids; kill \"$(cat .local/pids)\"",
    {
      "blocked": true,
      "wipe": false,
      "exhausted": false
    }
  ],
  [
    "D=/tmp; printf '%s' '' /own > .local/r9-prefix; D=$(cat .local/r9-prefix); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "D=/tmp; printf '%s' /own '' > .local/r9-suffix; D=$(cat .local/r9-suffix); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "D=/tmp; printf '%s' '{/tmp,}' > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "D=/tmp; printf '%s' \\{/tmp,\\} > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "D=/tmp; printf '%s' /own > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "F='{/tmp,}'; D=/tmp; printf '%s' \"$F\" > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "D=/tmp\ncat <<'EOF' > .local/r9-lines\n/own\nEOF\nprintf '%s' /own/child >> .local/r9-lines\nD=$(cat .local/r9-lines)\nrm -rf $D",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "D=/tmp\ncat <<'EOF' > .local/r9-lines\n/own\nEOF\nprintf '%s' /tmp >> .local/r9-lines\nD=$(cat .local/r9-lines)\nrm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "D=/tmp\ncat <<'EOF' > .local/r9-lines\nEOF\nprintf '%s' /own >> .local/r9-lines\nD=$(cat .local/r9-lines)\nrm -rf $D",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "D=/tmp; printf '%s' /own > .local/r9-fd; D=$(cat .local/r9-fd); rm -rf \"$D\"",
    {
      "blocked": false,
      "wipe": true,
      "exhausted": false
    }
  ],
  [
    "F=$(cat .local/filename); false && F=.local/else; pgrep worker > \"$F\"; printf '%s\\n' 4242 > .local/pids; xargs -a .local/pids kill",
    {
      "blocked": true,
      "wipe": false,
      "exhausted": false
    }
  ]
]);
export function expectRound14Migration(command:string,actual:CommandGuardResult):boolean {
  const expected=migrations.get(command);if(!expected)return false;
  expect(actual,command).toStrictEqual(expected);
  expect(Object.keys(actual).sort(),command).toStrictEqual(Object.keys(expected).sort());
  expect(killsByPattern(command),command).toBe(expected.blocked||expected.shellState===true);
  expect(wipesTmp(command),command).toBe(expected.wipe||expected.shellState===true);
  return true;
}
