import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// DATA ONLY: command strings must NEVER be dispatched to a shell, subprocess or native tool.
// Definitions-only authoring. Main owns frozen-baseline and final-candidate execution.
// JS \n and \t below are exact physical LF and TAB in scanner inputs.
type Verdict = "STATE" | "PID" | "TMP" | "SAFE";
type Case = readonly [id: string, command: string, verdict: Verdict];
type Pair = { id: string; boundary: string; refused: readonly Case[]; simple: readonly Case[] };

const pairs: Pair[] = [];
for (const [id, wrapper] of [["01", "env"], ["02", "command"], ["03", "timeout 1"]] as const) {
  pairs.push({
    id, boundary: `${wrapper} empty removed receiver is not a parent assignment`,
    refused: [
      [`R${id}-TMP`, `D=/tmp; ${wrapper} D=/own; rm -rf "$D"`, "STATE"],
      [`R${id}-PID`, `P=$(pgrep worker); ${wrapper} P=4242; kill "$P"`, "STATE"],
    ],
    simple: [
      [`S${id}-TMP-scalar`, 'D=/tmp; D=/own; rm -rf "$D"', "SAFE"],
      [`S${id}-PID-scalar`, 'P=$(pgrep worker); P=4242; kill "$P"', "SAFE"],
      [`S${id}-TMP-temporary`, `D=/own; ${wrapper} D=/tmp true; rm -rf "$D"`, "SAFE"],
      [`S${id}-PID-temporary`, `P=4242; ${wrapper} P=7777 true; kill "$P"`, "SAFE"],
    ],
  });
}

pairs.push(
  {
    id: "04", boundary: "quoted aggregate empty prefix preserves printf argument boundaries",
    refused: [
      ["R04-capture", 'D=/tmp; set -- \'\' /tmp; D=$(printf \'%s\' "$@"); rm -rf "$D"', "STATE"],
      ["R04-saved", 'D=/tmp; set -- \'\' /tmp; printf \'%s\' "$@" > .local/r9-prefix; D=$(cat .local/r9-prefix); rm -rf "$D"', "STATE"],
    ],
    simple: [
      ["S04-numeric", 'D=/tmp; set -- \'\' /own; D=$(printf \'%s\' "$2"); rm -rf "$D"', "SAFE"],
      ["S04-literal", 'D=/tmp; D=$(printf \'%s\' \'\' /own); rm -rf "$D"', "SAFE"],
      ["S04-saved-literal", 'D=/tmp; printf \'%s\' \'\' /own > .local/r9-prefix; D=$(cat .local/r9-prefix); rm -rf "$D"', "SAFE"],
    ],
  },
  {
    id: "05", boundary: "quoted aggregate empty suffix preserves printf argument boundaries",
    refused: [
      ["R05-capture", 'D=/tmp; set -- /tmp \'\'; D=$(printf \'%s\' "$@"); rm -rf "$D"', "STATE"],
      ["R05-saved", 'D=/tmp; set -- /tmp \'\'; printf \'%s\' "$@" > .local/r9-suffix; D=$(cat .local/r9-suffix); rm -rf "$D"', "STATE"],
    ],
    simple: [
      ["S05-numeric", 'D=/tmp; set -- /own \'\'; D=$(printf \'%s\' "$1"); rm -rf "$D"', "SAFE"],
      ["S05-literal", 'D=/tmp; D=$(printf \'%s\' /own \'\'); rm -rf "$D"', "SAFE"],
      ["S05-saved-literal", 'D=/tmp; printf \'%s\' /own \'\' > .local/r9-suffix; D=$(cat .local/r9-suffix); rm -rf "$D"', "SAFE"],
    ],
  },
);

for (const [id, coproc, readonly] of [
  ["06", "coproc", false], ["07", "coproc WORKER", false],
  ["08", "coproc", true], ["09", "coproc WORKER", true],
] as const) {
  pairs.push({
    id, boundary: `${coproc} multiline ${readonly ? "phantom readonly" : "replacement"} is child state`,
    refused: readonly ? [
      [`R${id}-TMP`, `D=/own\n${coproc} {\n  readonly D=/own\n}\nD=/tmp\nrm -rf "$D"`, "STATE"],
      [`R${id}-PID`, `P=4242\n${coproc} {\n  readonly P=4242\n}\nP=$(pgrep worker)\nkill "$P"`, "STATE"],
    ] : [
      [`R${id}-TMP`, `D=/tmp\n${coproc} {\n  D=/own\n}\nrm -rf "$D"`, "STATE"],
      [`R${id}-PID`, `P=$(pgrep worker)\n${coproc} {\n  P=4242\n}\nkill "$P"`, "STATE"],
    ],
    simple: readonly ? [
      [`S${id}-TMP-directreadonly`, 'D=/own; readonly D=/own; D=/tmp; rm -rf "$D"', "SAFE"],
      [`S${id}-PID-directreadonly`, 'P=4242; readonly P=4242; P=$(pgrep worker); kill "$P"', "SAFE"],
      [`S${id}-TMP-subshell`, 'D=/own; ( readonly D=/own ); D=/own/child; rm -rf "$D"', "SAFE"],
      [`S${id}-PID-subshell`, 'P=4242; ( readonly P=4242 ); P=4343; kill "$P"', "SAFE"],
      [`S${id}-quotedDATA`, `D=/own; : '${coproc} { readonly D=/own; }'; D=/own/child; rm -rf "$D"`, "SAFE"],
    ] : [
      [`S${id}-TMP-subshell`, 'D=/own; ( D=/tmp ); rm -rf "$D"', "SAFE"],
      [`S${id}-PID-subshell`, 'P=4242; ( P=$(pgrep worker) ); kill "$P"', "SAFE"],
      [`S${id}-TMP-scalar`, 'D=/tmp; D=/own; rm -rf "$D"', "SAFE"],
      [`S${id}-PID-scalar`, 'P=$(pgrep worker); P=4242; kill "$P"', "SAFE"],
      [`S${id}-quotedDATA`, `D=/own; : '${coproc} { D=/tmp; }'; rm -rf "$D"`, "SAFE"],
    ],
  });
}

pairs.push(
  {
    id: "10", boundary: "live shared-root plus empty brace expansion in capture is unproved",
    refused: [["R10-capture", 'D=/tmp; D=$(printf \'%s\' {/tmp,}); rm -rf "$D"', "STATE"]],
    simple: [
      ["S10-quoted", 'D=/tmp; D=$(printf \'%s\' \'{/tmp,}\'); rm -rf "$D"', "SAFE"],
      ["S10-escaped", 'D=/tmp; D=$(printf \'%s\' \\{/tmp,\\}); rm -rf "$D"', "SAFE"],
      ["S10-owned", 'D=/tmp; D=$(printf \'%s\' /own); rm -rf "$D"', "SAFE"],
      ["S10-expandedDATA", 'F=\'{/tmp,}\'; D=/tmp; D=$(printf \'%s\' "$F"); rm -rf "$D"', "SAFE"],
    ],
  },
  {
    id: "11", boundary: "live shared-root plus empty brace expansion in saved stdout is unproved",
    refused: [["R11-saved", 'D=/tmp; printf \'%s\' {/tmp,} > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf "$D"', "STATE"]],
    simple: [
      ["S11-quoted", 'D=/tmp; printf \'%s\' \'{/tmp,}\' > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf "$D"', "SAFE"],
      ["S11-escaped", 'D=/tmp; printf \'%s\' \\{/tmp,\\} > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf "$D"', "SAFE"],
      ["S11-owned", 'D=/tmp; printf \'%s\' /own > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf "$D"', "SAFE"],
      ["S11-expandedDATA", 'F=\'{/tmp,}\'; D=/tmp; printf \'%s\' "$F" > .local/r9-brace; D=$(cat .local/r9-brace); rm -rf "$D"', "SAFE"],
    ],
  },
  {
    id: "12", boundary: "heredoc final LF separates owned record from following shared-root stdout",
    refused: [["R12-capture", 'D=/tmp\nD=$(cat <<\'EOF\'\n/own\nEOF\nprintf \'%s\' /tmp\n)\nrm -rf $D', "TMP"]],
    simple: [
      ["S12-allowned", 'D=/tmp\nD=$(cat <<\'EOF\'\n/own\nEOF\nprintf \'%s\' /own/child\n)\nrm -rf $D', "SAFE"],
      ["S12-singlefilename", 'D=/tmp\nD=$(cat <<\'EOF\'\n/own\nEOF\nprintf \'%s\' /tmp\n)\nrm -rf "$D"', "SAFE"],
      ["S12-empty", 'D=/tmp\nD=$(cat <<\'EOF\'\nEOF\nprintf \'%s\' /own\n)\nrm -rf $D', "SAFE"],
    ],
  },
  {
    id: "13", boundary: "saved heredoc stdout retains interior LF before appended shared-root output",
    refused: [["R13-saved", 'D=/tmp\ncat <<\'EOF\' > .local/r9-lines\n/own\nEOF\nprintf \'%s\' /tmp >> .local/r9-lines\nD=$(cat .local/r9-lines)\nrm -rf $D', "TMP"]],
    simple: [
      ["S13-allowned", 'D=/tmp\ncat <<\'EOF\' > .local/r9-lines\n/own\nEOF\nprintf \'%s\' /own/child >> .local/r9-lines\nD=$(cat .local/r9-lines)\nrm -rf $D', "SAFE"],
      ["S13-singlefilename", 'D=/tmp\ncat <<\'EOF\' > .local/r9-lines\n/own\nEOF\nprintf \'%s\' /tmp >> .local/r9-lines\nD=$(cat .local/r9-lines)\nrm -rf "$D"', "SAFE"],
      ["S13-empty", 'D=/tmp\ncat <<\'EOF\' > .local/r9-lines\nEOF\nprintf \'%s\' /own >> .local/r9-lines\nD=$(cat .local/r9-lines)\nrm -rf $D', "SAFE"],
    ],
  },
  {
    id: "14", boundary: "tab-stripped heredoc preserves final LF in capture and saved output",
    refused: [
      ["R14-capture-tabs", 'D=/tmp\nD=$(cat <<-\'EOF\'\n\t\t/own\n\tEOF\nprintf \'%s\' /tmp\n)\nrm -rf $D', "TMP"],
      ["R14-saved-tabs", 'D=/tmp\ncat <<-\'EOF\' > .local/r9-tabs\n\t\t/own\n\tEOF\nprintf \'%s\' /tmp >> .local/r9-tabs\nD=$(cat .local/r9-tabs)\nrm -rf $D', "TMP"],
    ],
    simple: [
      ["S14-allowned-tabs", 'D=/tmp\nD=$(cat <<-\'EOF\'\n\t\t/own\n\tEOF\nprintf \'%s\' /own/child\n)\nrm -rf $D', "SAFE"],
      ["S14-singlefilename-tabs", 'D=/tmp\nD=$(cat <<-\'EOF\'\n\t\t/own\n\tEOF\nprintf \'%s\' /tmp\n)\nrm -rf "$D"', "SAFE"],
      ["S14-empty-tabs", 'D=/tmp\nD=$(cat <<-\'EOF\'\n\tEOF\nprintf \'%s\' /own\n)\nrm -rf $D', "SAFE"],
    ],
  },
);

pairs.push(
  {
    id: "15", boundary: "live extglob and case fallthrough grammar never grants safe bindings",
    refused: [
      ["R15-extglob", 'D=/own; : +(marker); rm -rf "$D"', "STATE"],
      ["R15-case-fallthrough", 'D=/tmp; case marker in marker) D=/own ;& other) : ;; esac; rm -rf "$D"', "STATE"],
      ["R15-case-retest", 'P=$(pgrep worker); case marker in marker) P=4242 ;;& other) : ;; esac; kill "$P"', "STATE"],
    ],
    simple: [
      ["S15-quoted-extglob", 'D=/own; : \'+(marker)\'; rm -rf "$D"', "SAFE"],
      ["S15-quoted-case", 'P=4242; : \'case marker in marker) P=7777 ;& other) : ;;& esac\'; kill "$P"', "SAFE"],
      ["S15-expandedDATA", 'F=\'+(marker) ;& ;;&\'; D=/own; : "$F"; rm -rf "$D"', "SAFE"],
    ],
  },
  {
    id: "16", boundary: "unsupported descriptor grammar and special paths cannot attest ordinary file output",
    refused: [
      ["R16-dynamicfd", 'D=/own; : {fd}> .local/r9-fd; rm -rf "$D"', "STATE"],
      ["R16-fdmove-output", 'D=/own; : 3>&1-; rm -rf "$D"', "STATE"],
      ["R16-fdmove-input", 'P=4242; : 3<&0-; kill "$P"', "STATE"],
      ["R16-readwrite", 'D=/own; : 3<> .local/r9-fd; rm -rf "$D"', "STATE"],
      ["R16-legacy-file", 'D=/own; printf \'%s\' /own >& .local/r9-fd; rm -rf "$D"', "STATE"],
      ["R16-devfd-input", 'D=/own; cat < /dev/fd/3; rm -rf "$D"', "STATE"],
      ["R16-devfd-output", 'P=4242; printf \'%s\' 4242 > /dev/fd/3; kill "$P"', "STATE"],
      ["R16-special-stdin", 'D=/own; cat < /dev/stdin; rm -rf "$D"', "STATE"],
      ["R16-nonfd0-heredoc", 'D=/own\ncat 3<<\'EOF\'\nmarker\nEOF\nrm -rf "$D"', "STATE"],
    ],
    simple: [
      ["S16-quotedDATA", 'D=/own; : \'{fd}> 3>&1- 3<&0- 3<> >&file /dev/fd/3 3<<EOF\'; rm -rf "$D"', "SAFE"],
      ["S16-ordinary-file", 'D=/tmp; printf \'%s\' /own > .local/r9-fd; D=$(cat .local/r9-fd); rm -rf "$D"', "SAFE"],
      ["S16-ordinary-fd", 'D=/own; : 3> .local/r9-fd; rm -rf "$D"', "SAFE"],
      ["S16-ordinary-dup", 'P=4242; : 3>&1; kill "$P"', "SAFE"],
      ["S16-fd0-heredoc", 'D=/tmp\nD=$(cat 0<<\'EOF\'\n/own\nEOF\n)\nrm -rf "$D"', "SAFE"],
    ],
  },
);

// Main additions after the author's quiescent handoff, before any baseline run:
// the Bash grammar inventory defaults every unproved class to refusal.
pairs.push(
  {
    id: "17", boundary: "remaining unproved grammar classes refuse instead of granting bytes or state",
    refused: [
      ["R17-stderr-pipeline", ': |& :', "STATE"],
      ["R17-negated-pipeline", '! :', "STATE"],
      ["R17-reserved-time", 'time :', "STATE"],
      ["R17-elif", 'if false; then :; elif true; then :; fi', "STATE"],
      ["R17-else", 'if false; then :; else :; fi', "STATE"],
      ["R17-implicit-for", 'for item; do :; done', "STATE"],
      ["R17-output-process", ": >(printf '%s' marker)", "STATE"],
      ["R17-locale", ': $"marker"', "STATE"],
      ["R17-clobber", ': >| .local/r9-file', "STATE"],
      ["R17-both-output", ': &> .local/r9-file', "STATE"],
      ["R17-both-append", ': &>> .local/r9-file', "STATE"],
      ["R17-output-close", ': 3>&-', "STATE"],
      ["R17-multiple-heredocs", "cat <<'FIRST' <<'SECOND'\nfirst\nFIRST\nsecond\nSECOND", "STATE"],
      ["R17-here-string", ': <<< marker', "STATE"],
      ["R17-scalar-append", 'D=/own; D+=/child', "STATE"],
      ["R17-resolved-special", 'F=/dev/fd/3; printf %s /own > "$F"', "STATE"],
      ["R17-network-special", ': > /dev/tcp/example.invalid/1', "STATE"],
    ],
    simple: [
      ["S17-inert", ": '! |& time elif else >(marker) $\"marker\" >| &> &>> >&- <<< D+='", "SAFE"],
      ["S17-if", 'if false; then :; fi', "SAFE"],
      ["S17-explicit-for", 'for item in marker; do :; done', "SAFE"],
      ["S17-pipeline", ': | :', "SAFE"],
      ["S17-input-process", 'cat <(printf %s marker)', "SAFE"],
      ["S17-resolved-regular", 'F=.local/r9-file; printf %s marker > "$F"', "SAFE"],
      ["S17-input-close", ': 3<&-', "SAFE"],
    ],
  },
  {
    id: "18", boundary: "unclosed or misnested reader and compound boundaries cannot attest syntax",
    refused: [
      ["R18-single-quote", ": 'marker", "STATE"],
      ["R18-double-quote", ': "marker', "STATE"],
      ["R18-command-capture", 'D=$(printf %s marker', "STATE"],
      ["R18-backtick", 'D=`printf %s marker', "STATE"],
      ["R18-process", 'cat <(printf %s marker', "STATE"],
      ["R18-group", '( :', "STATE"],
      ["R18-brace-group", '{ :;', "STATE"],
      ["R18-if", 'if true; then :', "STATE"],
      ["R18-loop", 'for item in marker; do :', "STATE"],
      ["R18-case", 'case marker in marker) : ;;', "STATE"],
      ["R18-heredoc", "cat <<'EOF'\nmarker", "STATE"],
    ],
    simple: [
      ["S18-single-quote", ": 'marker'", "SAFE"],
      ["S18-double-quote", ': "marker"', "SAFE"],
      ["S18-command-capture", 'D=$(printf %s marker); : "$D"', "SAFE"],
      ["S18-backtick", 'D=`printf %s marker`; : "$D"', "SAFE"],
      ["S18-group", '( : )', "SAFE"],
      ["S18-brace-group", '{ :; }', "SAFE"],
      ["S18-case", 'case marker in marker) : ;; esac', "SAFE"],
      ["S18-heredoc", "cat <<'EOF'\nmarker\nEOF", "SAFE"],
      ["S18-zero-printf-args", "printf '%s'", "SAFE"],
    ],
  },
);

function expectVerdict([id, command, verdict]: Case): void {
  const expected = verdict === "STATE"
    ? { blocked: false, wipe: false, exhausted: false, shellState: true }
    : { blocked: verdict === "PID", wipe: verdict === "TMP", exhausted: false };
  const actual = scanCommand(command);
  expect(actual, id).toStrictEqual(expected);
  expect(Object.keys(actual).sort(), id).toStrictEqual(Object.keys(expected).sort());
  expect(killsByPattern(command), id).toBe(verdict === "PID" || verdict === "STATE");
  expect(wipesTmp(command), id).toBe(verdict === "TMP" || verdict === "STATE");
}

describe("PR166 round9 DATA-only exact boundaries with SIMPLE controls", () => {
  for (const pair of pairs) {
    it(`R${pair.id} refuses ${pair.boundary}`, () => {
      for (const testCase of pair.refused) expectVerdict(testCase);
    });
    it(`SIMPLE-A${pair.id} preserves scalar, owned and inert DATA controls`, () => {
      for (const testCase of pair.simple) expectVerdict(testCase);
    });
  }
});
