import { describe, expect, it } from "vitest";
import { killsByPattern, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  // R13: a relative/whitespace cwd operand no longer grants parent effects.
  "D=' /tmp '; cd \"$D\"; rm -rf *",
  "IFS=:; P='/tmp/tmp.AbC123:/tmp'; printf -v D '%s\\n' \"$P\"; IFS=' \t\n'; rm -rf $D",
  "IFS=:; P=\"/tmp/tmp.AbC123:/tmp\"; printf -v D \"%s\" $P; rm -rf \"$D\"",
  "IFS=:; D='/tmp:'; cd \"$D\"; rm -rf *",
  "IFS=:; D='/tmp:'; pushd \"$D\"; rm -rf *",
  "A=('/tmp/tmp.AbC123 /tmp'); A+=(/tmp/tmp.Def456); rm -rf \"${A[@]}\"",
  "A=(/tmp/tmp.AbC123); A+=('/tmp/tmp.Def456 /tmp'); rm -rf \"${A[@]}\"",
  "A=('/tmp/tmp.AbC123 /tmp'); rm -rf \"${A[@]}\"",
  "IFS=:; D='/tmp/tmp.AbC123:/tmp'; printf '%s\\n' \"$D\" > .local/dirs; IFS=' \t\n'; cat .local/dirs | xargs rm -rf"
]);
function expectRound5Guard(command: string, result: ReturnType<typeof scanCommand>, original: { blocked?: boolean; wipe?: boolean; exhausted?: boolean; overall?: boolean }): void {
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


// Commands are literal scanner DATA only. Never execute them or import this suite to extract data.
// Each refusal has a recorded-PID / concrete-own-path allowance; descriptor copies are snapshots,
// applied left-to-right, not aliases to a later descriptor binding. No session ownership API is used.
const boundaries: Array<[label: string, command: string, blocked: boolean, wipe: boolean]> = [
  // fd3 must cross the inline-child boundary and then reach the command substitution's cat.
  ["FD-INLINE.01 lookup file inherited by child capture", `pgrep worker > .local/pids; bash -c 'kill $(cat <&3)' 3<.local/pids`, true, false],
  ["FD-INLINE.02 recorded file inherited by child capture", `cat .local/recorded.pid > .local/pids; bash -c 'kill $(cat <&3)' 3<.local/pids`, false, false],
  ["FD-INLINE.03 shared listing inherited by child capture", `ls -d /tmp/tmp.* > .local/dirs; bash -c 'rm -rf $(cat <&3)' 3<.local/dirs`, false, true],
  ["FD-INLINE.04 own listing inherited by child capture", `find /tmp/tmp.AbC123 -name '*.json' > .local/dirs; bash -c 'rm -rf $(cat <&3)' 3<.local/dirs`, false, false],
  ["FD-CAPTURE.01 lookup capture binds child variable", `pgrep worker > .local/pids; bash -c 'P=$(cat <&3); kill "$P"' 3<.local/pids`, true, false],
  ["FD-CAPTURE.02 recorded capture binds child variable", `cat .local/recorded.pid > .local/pids; bash -c 'P=$(cat <&3); kill "$P"' 3<.local/pids`, true, false], // Conservative false positive: read/fd provenance UNKNOWN; ID retained.
  ["FD-CAPTURE.03 shared capture binds child variable", `ls -d /tmp/tmp.* > .local/dirs; bash -c 'D=$(cat <&3); rm -rf "$D"' 3<.local/dirs`, false, true],
  ["FD-CAPTURE.04 own capture binds child variable", `find /tmp/tmp.AbC123 -name '*.json' > .local/dirs; bash -c 'D=$(cat <&3); rm -rf "$D"' 3<.local/dirs`, false, true], // Conservative false positive: read/fd provenance UNKNOWN; ID retained.
  // A late compound redirect opens fd3 before copying it to fd0 for the entire body.
  ["FD-COMPOUND.01 lookup fd3 copied to compound stdin", `pgrep worker > .local/pids; { kill $(cat); } 3<.local/pids <&3`, true, false],
  ["FD-COMPOUND.02 recorded fd3 copied to compound stdin", `cat .local/recorded.pid > .local/pids; { kill $(cat); } 3<.local/pids <&3`, false, false],
  ["FD-COMPOUND.03 shared fd3 copied to compound stdin", `ls -d /tmp/tmp.* > .local/dirs; { rm -rf $(cat); } 3<.local/dirs <&3`, false, true],
  ["FD-COMPOUND.04 own fd3 copied to compound stdin", `find /tmp/tmp.AbC123 -name '*.json' > .local/dirs; { rm -rf $(cat); } 3<.local/dirs <&3`, false, false],
  // read -u selects fd3 even when fd0 has an explicit independent source.
  ["FD-READ.01 lookup read u3 ignores null stdin", `pgrep worker > .local/pids; read -r -u 3 P 3<.local/pids </dev/null; kill "$P"`, true, false],
  ["FD-READ.02 recorded read u3 ignores null stdin", `cat .local/recorded.pid > .local/pids; read -r -u 3 P 3<.local/pids </dev/null; kill "$P"`, true, false],
  ["FD-READ.03 shared read u3 ignores null stdin", `ls -d /tmp/tmp.* > .local/dirs; read -r -u 3 D 3<.local/dirs </dev/null; rm -rf "$D"`, false, true],
  ["FD-READ.04 own read u3 ignores null stdin", `find /tmp/tmp.AbC123 -name '*.json' > .local/dirs; read -r -u 3 D 3<.local/dirs </dev/null; rm -rf "$D"`, false, true],
  ["FD-READ.05 attached u3 lookup with safe fd0", `pgrep worker > .local/pids; read -ru3 P 3<.local/pids <.local/recorded.pid; kill "$P"`, true, false],
  ["FD-READ.06 attached u3 recorded with lookup fd0", `pgrep worker > .local/pids; read -ru3 P 3<.local/recorded.pid <.local/pids; kill "$P"`, true, false],
  ["FD-READ.07 attached u3 shared with own fd0", `ls -d /tmp/tmp.* > .local/dirs; read -ru3 D 3<.local/dirs <.local/own.list; rm -rf "$D"`, false, true],
  ["FD-READ.08 attached u3 own with shared fd0", `ls -d /tmp/tmp.* > .local/dirs; read -ru3 D 3<.local/own.list <.local/dirs; rm -rf "$D"`, false, true],
  // Later overwrite wins for fd3, but cannot retroactively change an earlier fd0 copy.
  ["FD-ORDER.01 later fd3 lookup replaces recorded", `pgrep worker > .local/pids; bash -c 'kill $(cat <&3)' 3<.local/recorded.pid 3<.local/pids`, true, false],
  ["FD-ORDER.02 later fd3 recorded replaces lookup", `pgrep worker > .local/pids; bash -c 'kill $(cat <&3)' 3<.local/pids 3<.local/recorded.pid`, false, false],
  ["FD-ORDER.03 later fd3 shared replaces own", `ls -d /tmp/tmp.* > .local/dirs; bash -c 'rm -rf $(cat <&3)' 3<.local/own.list 3<.local/dirs`, false, true],
  ["FD-ORDER.04 later fd3 own replaces shared", `ls -d /tmp/tmp.* > .local/dirs; bash -c 'rm -rf $(cat <&3)' 3<.local/dirs 3<.local/own.list`, false, false],
  ["FD-SNAPSHOT.01 copied lookup survives later fd3 overwrite", `pgrep worker > .local/pids; { kill $(cat); } 3<.local/pids <&3 3<.local/recorded.pid`, true, false],
  ["FD-SNAPSHOT.02 copied recorded survives later fd3 overwrite", `pgrep worker > .local/pids; { kill $(cat); } 3<.local/recorded.pid <&3 3<.local/pids`, false, false],
  ["FD-SNAPSHOT.03 copied shared survives later fd3 overwrite", `ls -d /tmp/tmp.* > .local/dirs; { rm -rf $(cat); } 3<.local/dirs <&3 3<.local/own.list`, false, true],
  ["FD-SNAPSHOT.04 copied own survives later fd3 overwrite", `ls -d /tmp/tmp.* > .local/dirs; { rm -rf $(cat); } 3<.local/own.list <&3 3<.local/dirs`, false, false],
  ["FD-STDIN.01 final fd0 lookup replaces earlier safe copy", `pgrep worker > .local/pids; { kill $(cat); } 3<.local/recorded.pid <&3 <.local/pids`, true, false],
  ["FD-STDIN.02 final fd0 recorded replaces earlier lookup copy", `pgrep worker > .local/pids; { kill $(cat); } 3<.local/pids <&3 <.local/recorded.pid`, false, false],
  ["FD-STDIN.03 final fd0 shared replaces earlier own copy", `ls -d /tmp/tmp.* > .local/dirs; { rm -rf $(cat); } 3<.local/own.list <&3 <.local/dirs`, false, true],
  ["FD-STDIN.04 final fd0 own replaces earlier shared copy", `ls -d /tmp/tmp.* > .local/dirs; { rm -rf $(cat); } 3<.local/dirs <&3 <.local/own.list`, false, false],
  // Closing fd3 stops a subsequent cat on fd3; closing it after copying fd0 leaves fd0 intact.
  ["FD-CLOSE.01 lookup copied before fd3 close", `pgrep worker > .local/pids; bash -c 'kill $(cat)' 3<.local/pids <&3 3<&-`, true, false],
  ["FD-CLOSE.02 lookup fd3 closed before child capture", `pgrep worker > .local/pids; bash -c 'kill $(cat <&3)' 3<.local/pids 3<&-`, false, false],
  ["FD-CLOSE.03 shared copied before fd3 close", `ls -d /tmp/tmp.* > .local/dirs; bash -c 'rm -rf $(cat)' 3<.local/dirs <&3 3<&-`, false, true],
  ["FD-CLOSE.04 shared fd3 closed before child capture", `ls -d /tmp/tmp.* > .local/dirs; bash -c 'rm -rf $(cat <&3)' 3<.local/dirs 3<&-`, false, false],
  // Child command-local redirects must neither overwrite nor close the parent's fd3.
  ["FD-SCOPE.01 parent lookup survives independent child fd3", `pgrep worker > .local/pids; { bash -c ':' 3<.local/recorded.pid; kill $(cat <&3); } 3<.local/pids`, true, false],
  ["FD-SCOPE.02 parent recorded survives lookup child fd3", `pgrep worker > .local/pids; { bash -c ':' 3<.local/pids; kill $(cat <&3); } 3<.local/recorded.pid`, false, false],
  ["FD-SCOPE.03 parent shared survives independent child fd3", `ls -d /tmp/tmp.* > .local/dirs; { bash -c ':' 3<.local/own.list; rm -rf $(cat <&3); } 3<.local/dirs`, false, true],
  ["FD-SCOPE.04 parent own survives shared child fd3", `ls -d /tmp/tmp.* > .local/dirs; { bash -c ':' 3<.local/dirs; rm -rf $(cat <&3); } 3<.local/own.list`, false, false],
  ["FD-SCOPE.05 child closes fd3 without closing parent lookup", `pgrep worker > .local/pids; { bash -c ':' 3<&-; kill $(cat <&3); } 3<.local/pids`, true, false],
  ["FD-SCOPE.06 child closes fd3 without tainting parent recorded", `pgrep worker > .local/pids; { bash -c ':' 3<&-; kill $(cat <&3); } 3<.local/recorded.pid`, false, false],
  ["FD-SCOPE.07 child closes fd3 without closing parent shared", `ls -d /tmp/tmp.* > .local/dirs; { bash -c ':' 3<&-; rm -rf $(cat <&3); } 3<.local/dirs`, false, true],
  ["FD-SCOPE.08 child closes fd3 without tainting parent own", `ls -d /tmp/tmp.* > .local/dirs; { bash -c ':' 3<&-; rm -rf $(cat <&3); } 3<.local/own.list`, false, false],
  // An explicit operand is independent of the inherited fd3; merely opening it is not consumption.
  ["FD-INDEPENDENT.01 child cat consumes inherited lookup", `pgrep worker > .local/pids; bash -c 'kill $(cat <&3)' 3<.local/pids </dev/null`, true, false],
  ["FD-INDEPENDENT.02 explicit recorded operand ignores inherited lookup", `pgrep worker > .local/pids; bash -c 'kill $(cat .local/recorded.pid)' 3<.local/pids </dev/null`, false, false],
  ["FD-INDEPENDENT.03 child cat consumes inherited shared listing", `ls -d /tmp/tmp.* > .local/dirs; bash -c 'rm -rf $(cat <&3)' 3<.local/dirs </dev/null`, false, true],
  ["FD-INDEPENDENT.04 explicit own operand ignores inherited shared listing", `ls -d /tmp/tmp.* > .local/dirs; bash -c 'rm -rf $(cat .local/own.list)' 3<.local/dirs </dev/null`, false, false],
  // Accepted N4 security binders: read consumes one line; printf -v receives caller-split argv.
  // These templates contain actual newlines/tabs, matching security-gpt61-probes.ts exactly.
  ["N4-READ-LINE.01 first shared line refuses despite later own line", `read -r D <<< '/tmp
/tmp/tmp.AbC123'; rm -rf "$D"`, false, true],
  ["N4-READ-LINE.02 first own line allows despite later shared line", `read -r D <<< '/tmp/tmp.AbC123
/tmp'; rm -rf $D`, false, true],
  ["N4-PRINTF-IFS.01 split argv then reset IFS exposes shared root", `IFS=:; P='/tmp/tmp.AbC123:/tmp'; printf -v D '%s\\n' $P; IFS=' 	
'; rm -rf $D`, false, true],
  ["N4-PRINTF-IFS.02 quoted argv then reset IFS remains one non-root name", `IFS=:; P='/tmp/tmp.AbC123:/tmp'; printf -v D '%s\\n' "$P"; IFS=' 	
'; rm -rf $D`, false, false],
  // Owner-authorized conservative policy: unproved double-quoted formatting cannot grant safe bytes.
  ["N4-PRINTF-IFS.03 conservative refusal: no reset keeps newline suffixed fields non-root", `IFS=:; P="/tmp/tmp.AbC123:/tmp"; printf -v D "%s\\n" $P; rm -rf $D`, false, true],
  ["N4-PRINTF-IFS.04 concatenated format with quoted use remains non-root", `IFS=:; P="/tmp/tmp.AbC123:/tmp"; printf -v D "%s" $P; rm -rf "$D"`, false, false],
  // Additional accepted security boundaries: exact literal DATA from security-gpt61-boundaries.json.
  ["SEC-N4.01 read n4 truncates to shared root", "read -r -n 4 D <<< '/tmp/tmp.AbC123'; rm -rf \"$D\"", false, true],
  ["SEC-N4.02 read n4 preserves own prefix", "read -r -n 4 D <<< '/own/tmp.AbC123'; rm -rf \"$D\"", false, true],
  ["SEC-N4.03 read custom delimiter shared root", "read -r -d : D <<< '/tmp:/tmp/tmp.AbC123'; rm -rf \"$D\"", false, true],
  ["SEC-N4.04 read custom delimiter own root", "read -r -d : D <<< '/tmp/tmp.AbC123:/tmp'; rm -rf \"$D\"", false, true],
  ["SEC-N4.05 read no r escaped shared root", "read D <<< '/t\\mp'; rm -rf \"$D\"", false, true],
  ["SEC-N4.06 read r escaped literal safe", "read -r D <<< '/t\\mp'; rm -rf \"$D\"", false, true],
  ["SEC-N4.07 mapfile first line limit shared", "mapfile -t -n 1 D <<< '/tmp\n/tmp/tmp.AbC123'; rm -rf \"${D[@]}\"", false, true],
  ["SEC-N4.08 mapfile first line limit own", "mapfile -t -n 1 D <<< '/tmp/tmp.AbC123\n/tmp'; rm -rf \"${D[@]}\"", false, true],
  ["SEC-N4.09 mapfile skip first line own", "mapfile -t -s 1 D <<< '/tmp\n/tmp/tmp.AbC123'; rm -rf \"${D[@]}\"", false, true],
  ["SEC-N4.10 mapfile skip first line shared", "mapfile -t -s 1 D <<< '/tmp/tmp.AbC123\n/tmp'; rm -rf \"${D[@]}\"", false, true],
  ["SEC-N2.01 cd unquoted whitespace root", "D=' /tmp '; cd $D; rm -rf *", false, true],
  ["SEC-N2.02 cd quoted whitespace literal safe", "D=' /tmp '; cd \"$D\"; rm -rf *", false, false],
  ["SEC-N2.03 cd colon trim root", "IFS=:; D='/tmp:'; cd $D; rm -rf *", false, true],
  ["SEC-N2.04 cd quoted colon literal safe", "IFS=:; D='/tmp:'; cd \"$D\"; rm -rf *", false, false],
  ["SEC-N2.05 pushd colon trim root", "IFS=:; D='/tmp:'; pushd $D; rm -rf *", false, true],
  ["SEC-N2.06 pushd quoted colon literal safe", "IFS=:; D='/tmp:'; pushd \"$D\"; rm -rf *", false, false],
  ["SEC-N3.01 env unquoted whitespace root", "D=' /tmp '; env -C $D bash -c 'rm -rf *'", false, true],
  ["SEC-N3.02 env quoted whitespace literal safe", "D=' /tmp '; env -C \"$D\" bash -c 'rm -rf *'", false, false],
  ["SEC-N3.03 sudo unquoted whitespace root", "D=' /tmp '; sudo -D $D bash -c 'rm -rf *'", false, true],
  ["SEC-N3.04 sudo quoted whitespace literal safe", "D=' /tmp '; sudo -D \"$D\" bash -c 'rm -rf *'", false, false],
  ["SEC-N3.05 env disappearing empty option arg", "D=''; env -C $D /tmp bash -c 'rm -rf *'", false, true],
  ["SEC-N3.06 env quoted empty option arg not run", "D=''; env -C \"$D\" /tmp bash -c 'rm -rf *'", false, false],
  ["SEC-N6.01 array old element unquoted split root", "A=('/tmp/tmp.AbC123 /tmp'); A+=(/tmp/tmp.Def456); rm -rf ${A[@]}", false, true],
  ["SEC-N6.02 array old element quoted single name safe", "A=('/tmp/tmp.AbC123 /tmp'); A+=(/tmp/tmp.Def456); rm -rf \"${A[@]}\"", false, false],
  ["SEC-N6.03 array new element unquoted split root", "A=(/tmp/tmp.AbC123); A+=('/tmp/tmp.Def456 /tmp'); rm -rf ${A[@]}", false, true],
  ["SEC-N6.04 array new element quoted single name safe", "A=(/tmp/tmp.AbC123); A+=('/tmp/tmp.Def456 /tmp'); rm -rf \"${A[@]}\"", false, false],
  ["SEC-N2.07 array before append unquoted split root", "A=('/tmp/tmp.AbC123 /tmp'); rm -rf ${A[@]}", false, true],
  ["SEC-N2.08 array before append quoted single name safe", "A=('/tmp/tmp.AbC123 /tmp'); rm -rf \"${A[@]}\"", false, false],
  // Final accepted addendum: compound inputs snapshot entry aliases/cwd, not body-mutated state.
  ["ADD-F9.01 compound stdin alias before mutation unsafe", "pgrep worker > .local/pids; F=.local/pids; { F=.local/recorded.pid; read -r P; kill \"$P\"; } < \"$F\"", true, false],
  ["ADD-F9.02 compound stdin alias before mutation recorded", "pgrep worker > .local/pids; F=.local/recorded.pid; { F=.local/pids; read -r P; kill \"$P\"; } < \"$F\"", true, false],
  ["ADD-F9.03 compound stdin cwd before body unsafe", "cd /home/paul/w; pgrep worker > pids; { cd /home/paul/other; read -r P; kill \"$P\"; } < pids", true, false],
  ["ADD-F9.04 compound stdin cwd before body recorded", "cd /home/paul/w; cat .local/recorded.pid > pids; { cd /home/paul/other; read -r P; kill \"$P\"; } < pids", true, false],
  // printf stdout uses the same caller-split argv as -v before saved-file provenance is consumed.
  ["ADD-F9.05 printf stdout caller IFS unquoted", "IFS=:; D='/tmp/tmp.AbC123:/tmp'; printf '%s\\n' $D > .local/dirs; IFS=' \t\n'; cat .local/dirs | xargs rm -rf", false, true],
  ["ADD-F9.06 printf stdout caller IFS quoted", "IFS=:; D='/tmp/tmp.AbC123:/tmp'; printf '%s\\n' \"$D\" > .local/dirs; IFS=' \t\n'; cat .local/dirs | xargs rm -rf", false, false],
];

describe("guard descriptor provenance boundaries", () => {
  it.each(boundaries)("%s", (_label, command, blocked, wipe) => {
    expectRound5Guard(command, scanCommand(command), { blocked, wipe, exhausted: false });
    void 0;
    void 0;
  });
});
