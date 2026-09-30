import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: never execute these strings in a shell, subprocess, or native proof.
// TS \\ encodes a literal backslash; \n encodes LF. No interpreter is requested.
type Case = readonly [id: string, command: string];
type Pair = { id: string; boundary: string; refused: readonly Case[]; simple: readonly Case[] };

const pairs: readonly Pair[] = [
  {
    id: "01", boundary: "arithmetic statement mutates the parent lookup destination",
    refused: [["R01", 'P=4242; Q=$(pgrep -n worker); (( P=Q )); kill "$P"']],
    simple: [
      ["S01scalar", 'P=4242; Q=7777; P=4242; kill "$P"'],
      ["S01subshell", 'P=4242; Q=$(pgrep -n worker); ( P=$Q ); kill "$P"'],
    ],
  },
  {
    id: "02", boundary: "unquoted arithmetic expansion mutates the parent lookup destination",
    refused: [["R02", 'P=4242; Q=$(pgrep -n worker); H=$(( P=Q )); kill "$P"']],
    simple: [["S02scalar", 'P=4242; Q=7777; H=7777; kill "$P"']],
  },
  {
    id: "03", boundary: "quoted arithmetic expansion is still a live parent writer",
    refused: [["R03", 'P=4242; Q=$(pgrep -n worker); H="$(( P=Q ))"; kill "$P"']],
    simple: [
      ["S03scalar", 'P=4242; H="7777"; kill "$P"'],
      ["S03data", "P=4242; H='$(( P=Q ))'; kill \"$P\""],
    ],
  },
  {
    id: "04", boundary: "live cwd/home tilde cannot attest literal path replacement",
    refused: [
      ["R04cwd", 'D=$(ls -d /tmp/tmp.*); cd /tmp; D=~+; rm -rf "$D"'],
      ["R04home", 'D=$(ls -d /tmp/tmp.*); D=~; rm -rf "$D"'],
    ],
    simple: [
      ["S04owned", 'D=$(ls -d /tmp/tmp.*); D=/own/child; rm -rf "$D"'],
      // Quoting/escaping preserves a literal relative non-root filename.
      ["S04quoted", "D=$(ls -d /tmp/tmp.*); cd /own; D='~+'; rm -rf \"$D\""],
      ["S04escaped", 'D=$(ls -d /tmp/tmp.*); cd /own; D=\\~+; rm -rf "$D"'],
      ["S04quotedHome", "D=$(ls -d /tmp/tmp.*); cd /own; D='~'; rm -rf \"$D\""],
      ["S04escapedHome", 'D=$(ls -d /tmp/tmp.*); cd /own; D=\\~; rm -rf "$D"'],
    ],
  },
  {
    id: "05", boundary: "select writes a shared-root path before the body",
    refused: [["R05", 'D=/own\nselect D in /tmp; do\n  rm -rf "$D"\n  break\ndone <<< 1']],
    simple: [
      ["S05scalar", 'D=/own; D=/own/child; rm -rf "$D"'],
      ["S05subshell", 'D=/own/child; ( D=/tmp ); rm -rf "$D"'],
    ],
  },
  {
    id: "06", boundary: "select writes a lookup-selected PID before the body",
    refused: [["R06", 'P=4242\nselect P in $(pgrep worker); do\n  kill "$P"\n  break\ndone <<< 1']],
    simple: [["S06scalar", 'P=4242; P=7777; kill "$P"']],
  },
  {
    id: "07", boundary: "comment backslash-LF cannot hide a state writer by invented separation",
    // One literal contains the comment's actual backslash and LF. Shell DATA only.
    refused: [["R07", 'P=4242; # comment\\\nreadonly P=4242\nP=$(pgrep worker); kill "$P"']],
    simple: [["S07plain", 'P=4242; # comment\nreadonly P=4242\nP=$(pgrep worker); kill "$P"']],
  },
  {
    id: "08", boundary: "quoted heredoc retains an unquoted continued delimiter lookalike as DATA",
    refused: [
      ["R08continuedDelimiter", 'D=/own\ncat <<EO\\\nF\nmarker\nEOF\nD=/tmp\nrm -rf "$D"'],
    ],
    simple: [["S08exact", 'D=/own\ncat <<\'EOF\'\nmarker\nEOF\nreadonly D=/own\nD=/tmp\nrm -rf "$D"']],
  },
];

function expectStateRefusal(id: string, command: string): void {
  const result = scanCommand(command);
  // Unsupported grammar: exact four keys, never a partial policy-only refusal.
  expect(result, id).toStrictEqual({ blocked: false, wipe: false, exhausted: false, shellState: true });
  expect(Object.keys(result).sort(), id).toStrictEqual(["blocked", "exhausted", "shellState", "wipe"]);
  expect(killsByPattern(command), id).toBe(true);
  expect(wipesTmp(command), id).toBe(true);
}

function expectSimpleAllow(id: string, command: string): void {
  const result = scanCommand(command);
  // Preserve the original exact three-key result, including absence of shellState.
  expect(result, id).toStrictEqual({ blocked: false, wipe: false, exhausted: false });
  expect(Object.keys(result).sort(), id).toStrictEqual(["blocked", "exhausted", "wipe"]);
  expect(killsByPattern(command), id).toBe(false);
  expect(wipesTmp(command), id).toBe(false);
}

describe("PR166 round7 paired unsupported parent-state boundaries", () => {
  it("R08quotedBody preserves quoted heredoc DATA and refuses the later real TMP assignment", () => {
    const command = 'D=/own\ncat <<\'EOF\'\nEO\\\nF\nreadonly D=/own\nEOF\nD=/tmp\nrm -rf "$D"';
    expect(scanCommand(command), "R08quotedBody").toStrictEqual({ blocked: false, wipe: true, exhausted: false });
    expect(killsByPattern(command), "R08quotedBody").toBe(false);
    expect(wipesTmp(command), "R08quotedBody").toBe(true);
  });
  for (const pair of pairs) {
    it(`R${pair.id} conservatively refuses ${pair.boundary}`, () => {
      for (const [id, command] of pair.refused) expectStateRefusal(id, command);
    });
    it(`SIMPLE-A${pair.id} preserves scalar, child-local, and literal DATA controls`, () => {
      for (const [id, command] of pair.simple) expectSimpleAllow(id, command);
    });
  }
});
