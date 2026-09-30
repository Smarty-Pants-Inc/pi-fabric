import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY. Never pass these strings to a shell, subprocess, or native proof.
// TS \\ encodes one literal backslash; \n and \t encode LF and TAB respectively.
// Inert single quotes and quoted heredocs must retain BOTH backslash and LF.
type Verdict = "TMP" | "PID" | "STATE" | "ALLOW";
type Case = readonly [id: string, command: string];
type Pair = { id: string; boundary: string; verdict: Verdict; refused: readonly Case[]; simple: readonly Case[] };

const pairs: readonly Pair[] = [
  {
    id: "01", boundary: "F29 continued quoted /tmp replaces a shared-root scalar", verdict: "TMP",
    refused: [["R01", 'D=/tmp; D="/t\\\nmp"; rm -rf "$D"']],
    simple: [
      ["S01plain", 'D=/tmp; D=/own; rm -rf "$D"'],
      ["S01continued", 'D=/tmp; D="/o\\\nwn"; rm -rf "$D"'],
      // Removing this inert backslash-LF globally would invent /tmp.
      ["S01singlequoted", "D=/tmp; D='/t\\\nmp'; rm -rf \"$D\""],
    ],
  },
  {
    id: "02", boundary: "F29 continued quoted /var/tmp replaces a plain owned scalar", verdict: "TMP",
    refused: [["R02", 'D=/own; D="/var/t\\\nmp"; rm -rf "$D"']],
    simple: [
      ["S02plain", 'D=/own; D=/own/child; rm -rf "$D"'],
      ["S02continued", 'D=/own; D="/own/ch\\\nild"; rm -rf "$D"'],
    ],
  },
  {
    id: "03", boundary: "F30 space-indented delimiter cannot install TMP readonly", verdict: "TMP",
    refused: [["R03", 'D=/own\ncat <<\'EOF\'\n EOF\nreadonly D=/own\nEOF\nD=/tmp\nrm -rf "$D"']],
    simple: [["S03exact", 'D=/own\ncat <<\'EOF\'\nmarker\nEOF\nreadonly D=/own\nD=/tmp\nrm -rf "$D"']],
  },
  {
    id: "04", boundary: "F30 trailing-space delimiter cannot install TMP readonly", verdict: "TMP",
    refused: [["R04", 'D=/own\ncat <<\'EOF\'\nEOF \nreadonly D=/own\nEOF\nD=/tmp\nrm -rf "$D"']],
    simple: [["S04exact", 'D=/own\ncat <<\'EOF\'\nmarker\nEOF\nreadonly D\nD=/tmp\nrm -rf "$D"']],
  },
  {
    id: "05", boundary: "F30 space-indented delimiter cannot install PID readonly", verdict: "PID",
    refused: [["R05", 'P=4242\ncat <<\'EOF\'\n EOF\nreadonly P=4242\nEOF\nP=$(pgrep worker)\nkill "$P"']],
    simple: [["S05exact", 'P=4242\ncat <<\'EOF\'\nmarker\nEOF\nreadonly P=4242\nP=$(pgrep worker)\nkill "$P"']],
  },
  {
    id: "06", boundary: "F30 trailing-space delimiter cannot install PID readonly", verdict: "PID",
    refused: [["R06", 'P=4242\ncat <<\'EOF\'\nEOF \nreadonly P=4242\nEOF\nP=$(pgrep worker)\nkill "$P"']],
    simple: [["S06exact", 'P=4242\ncat <<\'EOF\'\nmarker\nEOF\nreadonly P\nP=$(pgrep worker)\nkill "$P"']],
  },
  {
    id: "07", boundary: "F31 quoted tab-stripping cat capture yields exact /tmp", verdict: "TMP",
    refused: [["R07", 'D=/tmp\nD=$(cat <<-\'EOF\'\n\t/tmp\n\tEOF\n)\nrm -rf "$D"']],
    simple: [
      ["S07owned", 'D=/tmp\nD=$(cat <<-\'EOF\'\n\t/own\n\tEOF\n)\nrm -rf "$D"'],
      // Without <<-, the TAB is a genuine byte of the quoted pathname.
      ["S07retainedTab", 'D=/tmp\nD=$(cat <<\'EOF\'\n\t/tmp\nEOF\n)\nrm -rf "$D"'],
    ],
  },
  {
    id: "08", boundary: "F31 all leading body TABs strip, not just delimiter TABs", verdict: "TMP",
    refused: [["R08", 'D=/own\nD=$(cat <<-\'EOF\'\n\t\t/tmp\n\t\tEOF\n)\nrm -rf "$D"']],
    simple: [
      ["S08owned", 'D=/own\nD=$(cat <<-\'EOF\'\n\t\t/own/child\n\t\tEOF\n)\nrm -rf "$D"'],
      ["S08retainedTabs", 'D=/tmp\nD=$(cat <<\'EOF\'\n\t\t/tmp\nEOF\n)\nrm -rf "$D"'],
      // Quoted heredoc DATA retains the slash-LF: it does not capture /tmp.
      ["S08retainedContinuation", 'D=/tmp\nD=$(cat <<\'EOF\'\n/t\\\nmp\nEOF\n)\nrm -rf "$D"'],
    ],
  },
  {
    id: "09", boundary: "executed unsupported TMP body versus quoted lookalike DATA", verdict: "STATE",
    refused: [["R09", 'D=/own\ncat <<\'EOF\'\nmarker\nEOF\nread D\nrm -rf "$D"']],
    simple: [["S09data", 'D=/own\ncat <<\'EOF\'\n EOF\nread D\nreadonly D=/tmp\nD=${D:=/tmp}\nEOF\nrm -rf "$D"']],
  },
  {
    id: "10", boundary: "executed unsupported PID body versus quoted lookalike DATA", verdict: "STATE",
    refused: [["R10", 'P=4242\ncat <<\'EOF\'\nmarker\nEOF\nread P\nkill "$P"']],
    simple: [["S10data", 'P=4242\ncat <<\'EOF\'\nEOF \nread P\nreadonly P=$(pgrep worker)\nP=${P:=4242}\nEOF\nkill "$P"']],
  },
];

function expectVerdict(id: string, command: string, verdict: Verdict): void {
  const expected = {
    blocked: verdict === "PID",
    wipe: verdict === "TMP",
    exhausted: false,
    ...(verdict === "STATE" ? { shellState: true as const } : {}),
  };
  const result = scanCommand(command);
  // Check the complete original three-boolean policy verdict, not merely refusal.
  expect(result, id).toStrictEqual(expected);
  expect(Object.prototype.hasOwnProperty.call(result, "shellState"), id).toBe(verdict === "STATE");
  expect(killsByPattern(command), id).toBe(verdict === "PID" || verdict === "STATE");
  expect(wipesTmp(command), id).toBe(verdict === "TMP" || verdict === "STATE");
}

describe("PR166 round7 paired continuation and quoted-heredoc byte boundaries", () => {
  for (const pair of pairs) {
    it(`R${pair.id} refuses ${pair.boundary}`, () => {
      for (const [id, command] of pair.refused) expectVerdict(id, command, pair.verdict);
    });
    it(`SIMPLE-A${pair.id} preserves exact safe bytes and nonexecuted DATA`, () => {
      for (const [id, command] of pair.simple) expectVerdict(id, command, "ALLOW");
    });
  }
});
