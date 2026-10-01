import { describe, expect, it } from "vitest";
import { killsByPattern, TMP_WIPE_REASON, wipesTmp, scanCommand } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "files=(\"$D\"/*.json); rm -f \"${files[@]}\"",
  "REPLY=/tmp/tmp.AbC123; ls -d /tmp/tmp.* | while read -r -p REPLY -n 1 -u 0 -- p; do rm -rf \"$REPLY\"; done",
  "IFS=:; P='/home/paul/w/own:/tmp'; rm -rf \"$P\"",
  "IFS=,; P='/home/paul/w/own,/var/tmp'; rm -rf \"$P\"",
  "IFS=:; P='/tmp:/var/tmp'; rm -rf \"$P\"",
  "IFS=:,; P='/home/paul/w/own,/tmp:/home/paul/w/other'; rm -rf \"$P\"",
  "IFS=:; P='/tmp/tmp.AbC123:/tmp'; rm -rf \"$P\"",
  "IFS=:; P='/home/paul/w/own:/tmp'; set -- \"$P\"; rm -rf \"$1\"",
  "set -- $(find /tmp/tmp.AbC123 -name '*.json'); rm -rf \"$1\"",
  "printf -v D '%s' /tmp/tmp.AbC123; rm -rf \"$D\"",
  "printf -v P '%s/%s' /tmp 'tmp.*'; rm -rf \"$P\"",
  "printf -v P '%s\\n' /home/paul/w/own /tmp/tmp.AbC123; rm -rf $P",
  "dirs=(/tmp/tmp.AbC123/*.json); dirs+=(/tmp/tmp.AbC123); rm -rf \"${dirs[@]}\"",
  "dirs=($(find /tmp/tmp.AbC123 -name '*.json')); dirs+=(/tmp/tmp.AbC123); rm -rf \"${dirs[@]}\"",
  "dirs=(/tmp/tmp.AbC123); dirs+=(/tmp/tmp.AbC123/*.json); rm -rf \"${dirs[@]}\"",
  "D=$(mktemp -d); dirs=(/tmp/tmp.AbC123/*.json); dirs+=(\"$D\"); rm -rf \"${dirs[@]}\""
]);
// R7 owner precision cut: live tilde expansion is unmodeled. Keep the exact old
// IDs/commands, but refuse these three old allowances before granting path bytes.
const round7IntentionalState = new Set<string>([
  "cd /tmp && cd ~/w && rm -rf tmp.*",
  "rm -rf ~/.cache/wt-*",
  "cd ~/w && sh -c 'cd /tmp' && rm -rf tmp.*",
]);
// R9 owner grammar allowlist: aggregate argv is not a scalar byte proof.
// Preserve the exact old allowance ID/command as an intentional STATE refusal.
const round9IntentionalState = new Set<string>([
  'set -- /tmp/tmp.AbC123 /var/tmp/tmp.Def456; rm -rf "$@"',
]);
// R11 owner UNKNOWN-output cut: exact formerly-allowed conditional/loop
// feeds only; baseline A receipts are recorded separately, IDs/bytes retained.
const round11IntentionalTmp = new Set<string>([
  'cd "$D" && ls | xargs rm -f',
  'cd /tmp/tmp.AbC123 && ls | xargs rm -f',
  "for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do find \"$t\" -maxdepth 1 -name '*.json'; done | xargs rm -f",
  'ls /tmp; for suffix in log tmp; do find /tmp/tmp.AbC123 -name "*.$suffix" -print0; done | xargs -0 rm -f',
  "for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do find \"$t\" -name '*.json'; done > .local/selected.list; cat .local/selected.list | xargs rm -rf",
]);
function expectRound5Guard(command: string, result: ReturnType<typeof scanCommand>, original: { blocked?: boolean; wipe?: boolean; exhausted?: boolean; overall?: boolean }): void {
  if (round11IntentionalTmp.has(command)) {
    expect(result, command).toStrictEqual({ blocked: false, wipe: true, exhausted: false });
    expect(killsByPattern(command), command).toBe(false);
    expect(wipesTmp(command), command).toBe(true);
    return;
  }
  const intentional = round5IntentionalState.has(command) || round7IntentionalState.has(command) || round9IntentionalState.has(command);
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


// smarty-dev#1998: two fleet-wide /tmp wipes on 2026-09-29, then the forms the rule refuses and
// the deletes of an agent's own exact paths it must still allow.
const refused: Array<[string, string]> = [
  ["incident 10:32Z: a Playful lane, verbatim", `rm -rf /tmp/tmp.*`],
  ["incident 05:33Z style: find over /tmp/tmp.* with -exec rm", `find /tmp/tmp.* -maxdepth 1 \\( -name c.md -o -name body.md \\) -exec rm -rf {} +`],
  ["incident 05:33Z style: find /tmp -name 'tmp.*' -exec rm", `find /tmp -maxdepth 1 -name 'tmp.*' -mmin -120 -exec rm -rf {} \\;`],
  ["incident 05:33Z style: a loop over matched dirs", `for d in /tmp/tmp.*; do [ -e "$d/c.md" ] && rm -rf "$d"; done`],
  ["incident 05:33Z style: rm of a find over /tmp", `rm -rf $(find /tmp -maxdepth 1 -name 'tmp.*' -newer /tmp/x)`],
  ["rm -f /tmp/*", `rm -f /tmp/*`],
  ["rm -r -f /tmp/tmp.?????????", `rm -r -f /tmp/tmp.?????????`],
  ["a bracket glob", `rm -rf /tmp/tmp.[A-Z]*`],
  ["/var/tmp", `rm -rf /var/tmp/*`],
  ["/tmp itself", `rm -rf /tmp`],
  ["/tmp/ itself", `rm -rf /tmp/`],
  ["a long flag and --", `rm --recursive --force -- /tmp/tmp.*`],
  ["sudo rm", `sudo rm -rf /tmp/tmp.*`],
  ["a second operand", `rm -rf /tmp/tmp.AbC123 /tmp/tmp.*`],
  ["a glob after an exact dir and ..", `rm -rf /tmp/tmp.AbC123/../*`],
  ["cd /tmp, then a relative glob", `cd /tmp && rm -rf tmp.*`],
  ["ls | xargs rm", `ls -d /tmp/tmp.* | xargs rm -rf`],
  ["find /tmp | xargs rm", `find /tmp -maxdepth 1 -name 'tmp.*' -print0 | xargs -0 rm -rf`],
  ["find /tmp -delete", `find /tmp -name '*.md' -delete`],
  ["find /var/tmp -exec rm", `find /var/tmp -type f -exec rm {} +`],
  ["find -exec sh -c rm", `find /tmp/tmp.* -exec sh -c 'rm -rf "$1"' _ {} \\;`],
  ["a variable that holds a glob", `P=/tmp/tmp.*; rm -rf $P`],
  ["a variable that holds /tmp", `T=/tmp; rm -rf "$T"/*`],
  ["a while loop fed by ls /tmp", `ls -d /tmp/tmp.* | while read d; do rm -rf "$d"; done`],
  ["bash -c", `bash -c 'rm -rf /tmp/tmp.*'`],
  ["ssh", `ssh m4max 'rm -rf /tmp/tmp.*'`],
  ["a heredoc to bash", `bash <<'EOF'\nrm -rf /tmp/tmp.*\nEOF`],
  ["after other commands", `cd ~/w && make && rm -rf /tmp/tmp.* 2>/dev/null; echo done`],
  // From fix-1993's check (smarty-dev ec08fcac, #2093).
  ["a trailing redirection", `rm -rf /tmp/tmp.* 2>/dev/null`],
  ["rm -f -- /var/tmp/x?", `rm -f -- /var/tmp/x?`],
  ["sudo rm -r /tmp/[ab]*", `sudo rm -r /tmp/[ab]*`],
  ["a glob dir with a concrete leaf", `cd ~ && rm -rf /tmp/pytest-*/x`],
  ["/bin/rm", `/bin/rm -rf /tmp/*.log`],
  ["a quoted prefix, then an unquoted glob", `rm -rf "/tmp/"tmp.*`],
  // fix-1993 allowed this as remote; the unquoted glob expands on Dev1 first, and m5/m4max are shared too.
  ["ssh with an unquoted glob", `ssh m5 rm -rf /tmp/tmp.*`],
  // Round 1 on PR #148: review/astra 1, security S1: an assignment's quotes do not protect a later unquoted use.
  ["a single-quoted glob in a variable, expanded unquoted", `P='/tmp/tmp.*'; rm -rf $P`],
  ["a double-quoted glob in a variable, expanded unquoted", `P="/tmp/tmp.*"; rm -rf \${P}`],
  ["an escaped glob in a variable, expanded unquoted", `P=/tmp/tmp.\\*; rm -rf $P`],
  ["a quoted glob built from a variable", `T=/tmp; P="$T/tmp.*"; rm -rf $P`],
  ["an unknown value expanded unquoted next to a /tmp path", `L=$(cat .local/list); ls /tmp; rm -rf $L`],
  // review/astra 2, security S2: a local nested shell inherits the cwd.
  ["cd /tmp, then sh -c with a relative glob", `cd /tmp && sh -c 'rm -rf tmp.*'`],
  ["cd /tmp, then bash -c with a relative glob", `cd /tmp; bash -c "rm -rf *"`],
  ["cd /tmp, then eval", `cd /tmp && eval 'rm -rf tmp.*'`],
  ["cd /tmp, then a substitution", `cd /tmp && echo $(rm -rf tmp.*)`],
  // Round 2 on PR #148, security F1: a /tmp listing fed through a redirect to read or xargs.
  ["a while loop fed by a process substitution", `while read -r d; do rm -rf "$d"; done < <(find /tmp -maxdepth 1 -name 'tmp.*' -mmin -60)`],
  ["xargs fed by a process substitution", `xargs -0 rm -rf < <(find /tmp -maxdepth 1 -name 'tmp.*' -print0)`],
  ["xargs fed by a here-string", `xargs rm -rf <<< "$(ls -d /tmp/tmp.*)"`],
  ["xargs -a with a process substitution", `xargs -a <(ls -d /tmp/tmp.*) rm -rf`],
  // Security F2: an array assigned from a /tmp glob, and mapfile/readarray.
  ["an array of a /tmp glob", `dirs=(/tmp/tmp.*); rm -rf "\${dirs[@]}"`],
  ["a spaced array of a /tmp glob", `dirs=( /tmp/tmp.* ); rm -rf "\${dirs[@]}"`],
  ["mapfile from a /tmp listing", `mapfile -t dirs < <(ls -d /tmp/tmp.*); rm -rf "\${dirs[@]}"`],
  ["readarray from a /tmp listing", `readarray -t dirs < <(ls -d /tmp/tmp.*); rm -rf "\${dirs[@]}"`],
  // Security F3: after cd /tmp, a listing without a path lists /tmp.
  ["cd /tmp, then ls | head | xargs rm", `cd /tmp && ls -t | head -6 | xargs rm -rf`],
  ["cd /tmp, then rm of an ls substitution", `cd /tmp && rm -rf $(ls | grep '^tmp\\.')`],
  ["cd /tmp, then find without a root | xargs rm", `cd /tmp && find -maxdepth 1 -name 'tmp.*' | xargs rm -rf`],
  ["cd /tmp, then a loop fed by ls in a process substitution", `cd /tmp && while read d; do rm -rf "$d"; done < <(ls)`],
  ["an array of a /tmp listing", `dirs=($(ls -d /tmp/tmp.*)); rm -rf "\${dirs[@]}"`],
  ["xargs --arg-file with a process substitution", `xargs --arg-file=<(ls -d /tmp/tmp.*) rm -rf`],
  // Round 3 on PR #148, security F4: redirect listings use the state at the redirect, not script entry.
  ["F4: a loop variable feeds a redirected find listing", `for t in /tmp /var/tmp; do while read -r d; do rm -rf "$d"; done < <(find "$t" -maxdepth 1 -name 'tmp.*' -mmin +60); done`],
  ["F4: an assigned root feeds redirected xargs", `T=/tmp; xargs -0 rm -rf < <(find "$T" -maxdepth 1 -name 'tmp.*' -print0)`],
  ["F4: an assigned root feeds redirected mapfile", `T=/tmp; mapfile -t dirs < <(ls -d "$T"/tmp.*); rm -rf "\${dirs[@]}"`],
  ["F4: an assigned root feeds redirected readarray", `T=/tmp; readarray -t dirs < <(ls -d "$T"/tmp.*); rm -rf "\${dirs[@]}"`],
  ["F4: cd -P to a shared root before a redirected loop", `cd -P /tmp && while read -r d; do rm -rf "$d"; done < <(ls)`],
  ["F4: cd -- to a shared root before a redirected loop", `cd -- /tmp && while read -r d; do rm -rf "$d"; done < <(ls)`],
  ["F4: cd through an assigned root before a redirected loop", `T=/tmp; cd "$T" && while read -r d; do rm -rf "$d"; done < <(ls)`],
  // Security F5: a compound producer's listing survives its closing word/operator before the pipe.
  ["F5: a loop listing both shared roots feeds xargs", `for t in /tmp /var/tmp; do find "$t" -maxdepth 1 -name 'tmp.*' -mmin +60; done | xargs rm -rf`],
  ["F5: a brace group listing both shared roots feeds xargs", `{ ls -d /tmp/tmp.*; ls -d /var/tmp/tmp.*; } | xargs rm -rf`],
  ["F5: a subshell listing a shared root feeds xargs", `(ls -d /tmp/tmp.*) | xargs rm -rf`],
  ["F5: a loop listing both shared roots feeds read", `for t in /tmp /var/tmp; do find "$t" -maxdepth 1 -name 'tmp.*' -mmin +60; done | while read -r d; do rm -rf "$d"; done`],
  ["F5: a brace group listing both shared roots feeds read", `{ ls -d /tmp/tmp.*; ls -d /var/tmp/tmp.*; } | while read -r d; do rm -rf "$d"; done`],
  ["F5: a subshell listing a shared root feeds read", `(ls -d /tmp/tmp.*) | while read -r d; do rm -rf "$d"; done`],
  // Round 4 F6: independent owned sources may clear fallback, not an actual shared feed.
  ["F6: a redirected shared listing survives an intermediary cat", `T=/tmp; while read -r d; do rm -rf "$d"; done < <(find "$T" -maxdepth 1 -name 'tmp.*' | cat)`],
  ["F6: a compound shared listing survives an intermediary cat", `{ ls -d /tmp/tmp.*; } | cat | xargs rm -rf`],
  ["F6: a piped shared listing survives cat before read", `ls -d /tmp/tmp.* | cat | while read -r d; do rm -rf "$d"; done`],
  ["F6: an own-file loop does not clear a later shared redirect", `while read -r d; do rm -f "$d"; done < .local/owned-files.txt; while read -r d; do rm -rf "$d"; done < <(ls -d /tmp/tmp.*)`],
  ["F6: an own-file loop does not clear an earlier shared redirect", `while read -r d; do rm -rf "$d"; done < <(ls -d /tmp/tmp.*); while read -r d; do rm -f "$d"; done < .local/owned-files.txt`],
  ["F6: an output-only redirect does not clear an unsafe pipe", `ls -d /tmp/tmp.* | xargs rm -rf > .local/cleanup.log`],
  ["F6: an fd3 input redirect does not clear actual shared stdin", `find /tmp -name tmp.* -print0 | xargs -0 rm -rf 3< .local/mine.list`],
  ["F6: grouped cat without operands preserves actual shared stdin", `ls -d /tmp/tmp.* | { cat; } | xargs rm -rf`],
  // Round 5 F7/F8: these shell commands are test data only, never executed.
  ["F7.01: inline sh inherits shared stdin for xargs rm", `ls -d /tmp/tmp.* | sh -c 'xargs rm -rf'`],
  ["F7.02: inline sh inherits shared stdin for a read loop", `ls -d /tmp/tmp.* | sh -c 'while read -r d; do rm -rf "$d"; done'`],
  ["F7.03: unsafe inner xargs after an unrelated owned diagnostic", `sh -c 'ls -d /tmp/tmp.AbC123/*.json; ls -d /tmp/tmp.* | xargs rm -rf'`],
  ["F7.04: unsafe inner read loop after an unrelated owned diagnostic", `sh -c 'ls -d /tmp/tmp.AbC123/*.json; ls -d /tmp/tmp.* | while read -r d; do rm -rf "$d"; done'`],
  ["F8.01: implicit read destination REPLY receives shared stdin", `ls -d /tmp/tmp.* | while read -r; do rm -rf "$REPLY"; done`],
  ["F8.02: read defaults to REPLY after delimiter and timeout options", `while read -r -d '' -t 1; do rm -rf "$REPLY"; done < <(find /tmp -maxdepth 1 -name 'tmp.*' -print0)`],
  ["F8.03: read destination after prompt count fd and -- options", `REPLY=/tmp/tmp.AbC123; ls -d /tmp/tmp.* | while read -r -p REPLY -n 1 -u 0 -- p; do rm -rf "$p"; done`],
  ["F8.04: read -a array receives shared stdin", `read -r -d '' -a dirs < <(ls -d /tmp/tmp.*); rm -rf "\${dirs[@]}"`],
  ["F8.05: mapfile defaults to MAPFILE after option arguments and --", `mapfile -t -n 1 -O 0 -s 0 -C : -c 1 -- < <(ls -d /tmp/tmp.*); rm -rf "\${MAPFILE[@]}"`],
  ["F8.06: readarray defaults to MAPFILE after delimiter and --", `readarray -d '' -t -- < <(find /tmp -maxdepth 1 -name 'tmp.*' -print0); rm -rf "\${MAPFILE[@]}"`],
  ["F7.05: brace shared producer feeds inline sh xargs rm", `{ ls -d /tmp/tmp.*; } | sh -c 'xargs rm -rf'`],
  ["F7.06: inherited safe stdin does not suppress a shared inner brace producer", `cat .local/mine.list | sh -c "{ ls -d /tmp/tmp.*; } | xargs rm -rf"`],
  ["F8.07: plain mapfile -t defaults to shared MAPFILE", `mapfile -t < <(ls -d /tmp/tmp.*); rm -rf "\${MAPFILE[@]}"`],
  ["F8.08: plain readarray defaults to shared MAPFILE", `readarray < <(ls -d /tmp/tmp.*); rm -rf "\${MAPFILE[@]}"`],
];

const allowed: Array<[string, string]> = [
  ["an exact mktemp path", `rm -rf /tmp/tmp.AbC123`],
  ["the own dir by variable", `rm -rf "$D"`],
  ["the own dir from mktemp", `D=$(mktemp -d); cd "$D"; rm -rf "$D"`],
  ["a glob below the own dir by variable", `rm -f "$D"/*.json`],
  ["a glob below an exact tmp.XXXX dir", `rm -f /tmp/tmp.AbC123/*.md`],
  ["find below an exact dir with -delete", `find /tmp/tmp.AbC123 -name '*.log' -delete`],
  ["a quoted star is literal", `rm -f "/tmp/tmp.*"`],
  ["an escaped star is literal", `rm -f /tmp/tmp.\\*`],
  ["a relative glob outside /tmp", `rm -rf build/*`],
  ["an absolute glob outside /tmp", `rm -rf /home/paul/w/.local/*.json`],
  ["a listing of /tmp", `ls -la /tmp/tmp.*`],
  ["find /tmp without delete", `find /tmp -maxdepth 1 -name 'tmp.*' -user paul`],
  ["grep for the incident", `grep -rn 'rm -rf /tmp/tmp.*' docs/`],
  ["echo of the rule", `echo "never rm -rf /tmp/tmp.*"`],
  ["a heredoc note", `cat > note.md <<'EOF'\nrm -rf /tmp/tmp.*\nEOF`],
  ["a gh comment that names the command", `gh api repos/o/r/issues/1998/comments -f body='ran rm -rf /tmp/tmp.*'`],
  ["$TMPDIR contents", `rm -rf "$TMPDIR"/*`],
  ["cd elsewhere, then a relative glob", `cd /tmp && cd ~/w && rm -rf tmp.*`],
  ["rm of a list file's names", `cat .local/mine.txt | xargs rm -rf`],
  // From fix-1993's check (smarty-dev ec08fcac, #2093).
  ["an exact dir held in a variable", `D=/tmp/tmp.X1; rm -rf "$D"`],
  ["a single-quoted star", `rm -rf '/tmp/tmp.*'`],
  ["a double-quoted star mid-name", `rm -rf "/tmp/a*b"`],
  ["a relative glob", `rm -f ./*.log`],
  ["a glob under the home cache", `rm -rf ~/.cache/wt-*`],
  ["echo of the command, unquoted", `echo rm -rf /tmp/tmp.*`],
  // Round 1 on PR #148: a quoted use of a glob value is one literal name.
  ["a quoted glob value, used quoted", `P='/tmp/tmp.*'; rm -f "$P"`],
  ["single-quoted variable text is literal", `P='/tmp/*'; rm -f '$P'`],
  ["an unknown value, quoted", `L=$(cat .local/list); ls /tmp; rm -rf "$L"`],
  ["an unknown value, unquoted, no /tmp in the command", `L=$(cat .local/list); rm -rf $L`],
  // review/astra 3: a recorded mktemp dir is owned, even when mktemp names /tmp.
  ["a recorded mktemp -p /tmp dir", `D=$(mktemp -d -p /tmp); rm -rf "$D"`],
  ["a recorded mktemp -p /tmp dir, unquoted", `D=$(mktemp -d -p /tmp); rm -rf $D`],
  ["a glob inside a recorded mktemp dir", `D=$(mktemp -d -p /tmp); rm -rf "$D"/*`],
  ["a mktemp template under /tmp", `D=$(mktemp -d /tmp/wt.XXXXXX); rm -rf "$D"`],
  // review/astra 2: a nested shell under an exact owned dir; a child cd does not leak to the parent.
  ["cd to an exact dir, then sh -c with a glob", `cd /tmp/tmp.AbC123 && sh -c 'rm -rf *'`],
  ["a child cd /tmp does not move the parent", `cd ~/w && sh -c 'cd /tmp' && rm -rf tmp.*`],
  ["ssh does not inherit the local cwd", `cd /tmp && ssh m4max 'rm -rf build/*'`],
  // Round 2 on PR #148: F1 a loop over an own list file, F2 an array below the own dir, F3 ls in an own dir.
  ["a while loop over an own list file — conservative refusal", `while read -r f; do rm -f "$D/$f"; done < "$D/list"`],
  ["an array of a glob below the own dir", `files=("$D"/*.json); rm -f "\${files[@]}"`],
  ["cd to the own dir, then ls | xargs rm", `cd "$D" && ls | xargs rm -f`],
  ["cd to an exact tmp dir, then ls | xargs rm", `cd /tmp/tmp.AbC123 && ls | xargs rm -f`],
  ["cd /tmp, then a recorded mktemp dir", `cd /tmp && D=$(mktemp -d) && rm -rf "$D"`],
  // Round 3 F4/F5 counterparts: an own-directory listing must not become a shared-root feed.
  ["F4: a while loop over an own process-substitution listing — conservative refusal", `while read -r f; do rm -f "$D/$f"; done < <(ls "$D")`],
  ["F4: a recorded mktemp dir feeds an own redirected loop — conservative refusal", `D=$(mktemp -d); while read -r f; do rm -f "$D/$f"; done < <(ls "$D")`],
  ["F4: a loop variable feeds redirected listings of exact own dirs — conservative refusal", `for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do while read -r d; do rm -rf "$d"; done < <(find "$t" -maxdepth 1 -name '*.json'); done`],
  ["F4: an assigned exact own dir feeds redirected xargs", `T=/tmp/tmp.AbC123; xargs -0 rm -f < <(find "$T" -maxdepth 1 -name '*.json' -print0)`],
  ["F4: an assigned exact own dir feeds redirected mapfile — conservative refusal", `T=/tmp/tmp.AbC123; mapfile -t files < <(ls -d "$T"/*.json); rm -f "\${files[@]}"`],
  ["F4: an assigned exact own dir feeds redirected readarray — conservative refusal", `T=/tmp/tmp.AbC123; readarray -t files < <(ls -d "$T"/*.json); rm -f "\${files[@]}"`],
  ["F4: cd -P to an exact own dir before a redirected loop — conservative refusal", `cd -P /tmp/tmp.AbC123 && while read -r f; do rm -f "$f"; done < <(ls)`],
  ["F4: cd -- to an exact own dir before a redirected loop — conservative refusal", `cd -- /tmp/tmp.AbC123 && while read -r f; do rm -f "$f"; done < <(ls)`],
  ["F4: cd through an assigned exact own dir before a redirected loop — conservative refusal", `T=/tmp/tmp.AbC123; cd "$T" && while read -r f; do rm -f "$f"; done < <(ls)`],
  ["F5: a loop listing exact own dirs feeds xargs", `for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do find "$t" -maxdepth 1 -name '*.json'; done | xargs rm -f`],
  ["F5: a brace group listing exact own dirs feeds xargs", `{ ls -d /tmp/tmp.AbC123/*.json; ls -d /var/tmp/tmp.Def456/*.json; } | xargs rm -f`],
  ["F5: a subshell listing an exact own dir feeds xargs", `(ls -d /tmp/tmp.AbC123/*.json) | xargs rm -f`],
  ["F5: a loop listing exact own dirs feeds read — conservative refusal", `for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do find "$t" -maxdepth 1 -name '*.json'; done | while read -r f; do rm -f "$f"; done`],
  ["F5: a brace group listing exact own dirs feeds read — conservative refusal", `{ ls -d /tmp/tmp.AbC123/*.json; ls -d /var/tmp/tmp.Def456/*.json; } | while read -r f; do rm -f "$f"; done`],
  ["F5: a subshell listing an exact own dir feeds read — conservative refusal", `(ls -d /tmp/tmp.AbC123/*.json) | while read -r f; do rm -f "$f"; done`],
  // F6: the reviewer example and explicit owned sources after an unrelated shared-root diagnostic.
  ["F6: reviewer exact-directory pipeline after ls /tmp", `ls /tmp; find /tmp/tmp.AbC123 -name '*.log' -print0 | xargs -0 rm -f`],
  ["F6: an assigned own root feeds a pipeline after ls /tmp", `T=/tmp/tmp.AbC123; ls /tmp; find "$T" -name '*.log' -print0 | xargs -0 rm -f`],
  ["F6: an assigned own root feeds redirected xargs after ls /tmp", `ls /tmp; T=/tmp/tmp.AbC123; xargs -0 rm -f < <(find "$T" -name '*.log' -print0)`],
  ["F6: an own list-file redirect after ls /tmp — conservative refusal", `ls /tmp; while read -r f; do rm -f "$f"; done < .local/owned-files.txt`],
  ["F6: an assigned own root feeds a redirected loop after ls /tmp — conservative refusal", `ls /tmp; T=/tmp/tmp.AbC123; while read -r f; do rm -f "$f"; done < <(find "$T" -name '*.log')`],
  ["F6: an own list-file pipeline after ls /tmp", `ls /tmp; cat .local/owned-files.txt | xargs rm -f`],
  ["F6: a nested own list-file loop after ls /tmp — conservative refusal", `ls /tmp; for i in once; do while read f; do rm -f "$f"; done < .local/mine.list; done`],
  ["F6: an own list-file loop inside if after ls /tmp — conservative refusal", `ls /tmp; if true; then while read f; do rm -f "$f"; done < .local/mine.list; fi`],
  ["F6: a non-emitting for header preserves an exact-directory output after ls /tmp", `ls /tmp; for suffix in log tmp; do find /tmp/tmp.AbC123 -name "*.$suffix" -print0; done | xargs -0 rm -f`],
  // Paired F7/F8 allowances: exact owned sources or a recorded operand unrelated to the feed.
  ["F7.01: inline sh inherits owned stdin for xargs rm", `ls -d /tmp/tmp.AbC123/*.json | sh -c 'xargs rm -rf'`],
  ["F7.02: inline sh inherits owned stdin for a read loop — conservative refusal", `ls -d /tmp/tmp.AbC123/*.json | sh -c 'while read -r d; do rm -rf "$d"; done'`],
  ["F7.03: independent owned inner xargs overrides actual shared outer stdin", `ls -d /tmp/tmp.* | sh -c 'find /tmp/tmp.AbC123 -name "*.json" -print0 | xargs -0 rm -rf'`],
  ["F7.04: independent owned inner read loop overrides actual shared outer stdin — conservative refusal", `ls -d /tmp/tmp.* | sh -c 'while read -r d; do rm -rf "$d"; done < <(find /tmp/tmp.AbC123 -name "*.json")'`],
  ["F8.01: implicit read destination REPLY receives owned stdin — conservative refusal", `ls -d /tmp/tmp.AbC123/*.json | while read -r; do rm -rf "$REPLY"; done`],
  ["F8.02: read defaults to owned REPLY after delimiter and timeout options — conservative refusal", `while read -r -d '' -t 1; do rm -rf "$REPLY"; done < <(find /tmp/tmp.AbC123 -maxdepth 1 -name '*.json' -print0)`],
  ["F8.03: read prompt option argument REPLY is not a destination", `REPLY=/tmp/tmp.AbC123; ls -d /tmp/tmp.* | while read -r -p REPLY -n 1 -u 0 -- p; do rm -rf "$REPLY"; done`],
  ["F8.04: read -a array receives owned stdin — conservative refusal", `read -r -d '' -a dirs < <(ls -d /tmp/tmp.AbC123/*.json); rm -rf "\${dirs[@]}"`],
  ["F8.05: mapfile defaults to owned MAPFILE after option arguments and -- — conservative refusal", `mapfile -t -n 1 -O 0 -s 0 -C : -c 1 -- < <(ls -d /tmp/tmp.AbC123/*.json); rm -rf "\${MAPFILE[@]}"`],
  ["F8.06: readarray defaults to owned MAPFILE after delimiter and -- — conservative refusal", `readarray -d '' -t -- < <(find /tmp/tmp.AbC123 -maxdepth 1 -name '*.json' -print0); rm -rf "\${MAPFILE[@]}"`],
  ["F7.05: brace owned find producer feeds inline sh xargs rm", `{ find /tmp/tmp.AbC123 -name "*.json"; } | sh -c 'xargs rm -rf'`],
  ["F7.06: inherited safe stdin survives inner group cat after a shared diagnostic", `cat .local/mine.list | sh -c "ls /tmp; { cat; } | xargs rm -rf"`],
  ["F8.07: plain mapfile -t defaults to owned MAPFILE — conservative refusal", `mapfile -t < <(ls -d /tmp/tmp.AbC123/*.json); rm -rf "\${MAPFILE[@]}"`],
  ["F8.08: plain readarray defaults to owned MAPFILE — conservative refusal", `readarray < <(ls -d /tmp/tmp.AbC123/*.json); rm -rf "\${MAPFILE[@]}"`],
];

describe("tmp-wipe guard (smarty-dev#1998)", () => {
  it.each(refused)("refuses %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { wipe: true, blocked: false, exhausted: false });
  });

  it.each(allowed)("allows %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { wipe: _label.endsWith(" — conservative refusal"), blocked: false, exhausted: false });
  });

  // Security S3: assignment expansion is bounded (the text is only read, never run).
  it("reads a doubling assignment chain in bounded time", () => {
    let script = "A=xxxxxxxxxxxxxxxx";
    for (let i = 0; i < 40; i++) script += `; A=$A$A$A$A`;
    const start = Date.now();
    expectRound5Guard(`${script}; rm -rf "$A"`, scanCommand(`${script}; rm -rf "$A"`), { wipe: false, blocked: false, exhausted: false });
    expect(Date.now() - start).toBeLessThan(2000);
  });

  it("does not change the kill-by-pattern verdict", () => {
    expect(killsByPattern(`rm -rf /tmp/tmp.*`)).toBe(false);
  });

  it("names the fix in its reason", () => {
    expect(TMP_WIPE_REASON).toContain(`delete only your own mktemp -d path by its exact name ("$D"), #1508/#1998`);
  });
});

// #2275 follow-ups: paired shell-command DATA, never execute these strings.
// Keep the original corpus above byte-for-byte; IDs pair the two verdicts below.
describe("tmp-wipe follow-up regressions (#2275)", () => {
  const refused: Array<[string, string]> = [
    ["F9.01: saved shared listing feeds stdin/read", `ls -d /tmp/tmp.* > .local/selected.list; while read -r d; do rm -rf "$d"; done < .local/selected.list`],
    ["F9.02: saved shared listing feeds xargs -a", `ls -d /tmp/tmp.* > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.03: saved shared listing feeds inline --arg-file", `find /tmp -maxdepth 1 -name 'tmp.*' -print0 > .local/selected.list; xargs -0 --arg-file=.local/selected.list rm -rf`],
    ["F9.04: explicit cat of saved shared output cannot clear provenance", `ls -d /tmp/tmp.* > .local/selected.list; cat .local/selected.list | xargs rm -rf`],
    ["F9.05: resolved aliases identify the same saved shared output", `F=.local/selected.list; G="$F"; ls -d /tmp/tmp.* > "$F"; xargs -a "$G" rm -rf`],
    ["F9.06: append shared output retains unsafe file provenance", `cat .local/owned.list > .local/selected.list; ls -d /tmp/tmp.* >> .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.07: explicit fd1 writes actual shared stdout", `ls -d /tmp/tmp.* 1> .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.08: stdout file differs from an unrelated fd3 file", `ls -d /tmp/tmp.* > .local/selected.list 3> .local/diagnostic.log; xargs -a .local/selected.list rm -rf`],
    ["F9.09: brace producer saves shared output", `{ ls -d /tmp/tmp.*; } > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.10: loop producer saves shared output", `for t in /tmp /var/tmp; do find "$t" -maxdepth 1 -name 'tmp.*'; done > .local/selected.list; cat .local/selected.list | xargs rm -rf`],
    ["F9.11: subshell producer saves shared output", `(ls -d /tmp/tmp.*) > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.12: shared stdout actually reaches the pipe", `ls -d /tmp/tmp.* | cat | xargs rm -rf`],
    ["F9.13: captured cat reads saved shared output", `ls -d /tmp/tmp.* > .local/selected.list; rm -rf $(cat .local/selected.list)`],
    ["F9.14: unsafe overwrite replaces owned output", `find /tmp/tmp.AbC123 -name '*.json' > .local/selected.list; ls -d /tmp/tmp.* > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.15: unrelated owned diagnostic does not clear saved shared output", `ls -d /tmp/tmp.* > .local/selected.list; cat .local/owned.list; xargs -a .local/selected.list rm -rf`],
    ["F9.16: exact native empty-producer regression is still shared provenance", `find /tmp -maxdepth 1 -false > tmp.list; xargs -r rm -rf < tmp.list; printf 'MAIN_F9_TMP_EXECUTED\\n'`],
    ["F10.01: operand-free captured cat consumes inherited shared stdin", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(cat)'`],
    ["F10.02: captured stdin consumer pipeline preserves shared output", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(cat | tr "\\n" " ")'`],
    ["F10.03: cat dash explicitly consumes inherited shared stdin", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(cat -)'`],
    ["F10.04: shared inner producer taints capture with owned inherited stdin", `cat .local/owned.list | sh -c 'rm -rf $(ls -d /tmp/tmp.*)'`],
    ["F10.05: shared captured producer versus independent nonconsumer", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(ls -d /tmp/tmp.*)'`],
    ["F10.06: inherited shared stdin reaches a nested local child capture", `ls -d /tmp/tmp.* | sh -c 'bash -c "rm -rf \\$(cat)"'`],
    ["F10.07: capture binding is unsafe inside its consuming child", `D=/tmp/tmp.AbC123; ls -d /tmp/tmp.* | sh -c 'D=$(cat); rm -rf "$D"'`],
    ["F10.08: fd3 input does not replace inherited shared stdin for capture", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(cat)' 3< .local/owned.list`],
    ["F10.09: captured head consumes inherited shared stdin", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(head -n 1)'`],
    ["F10.10: capture assignment preserves inherited shared stdin", `ls -d /tmp/tmp.* | sh -c 'D=$(cat); rm -rf "$D"'`],
    ["F10.11: exact native empty-producer inherited capture regression", `find /tmp -maxdepth 1 -false | bash -c 'rm -rf $(cat)'; printf 'MAIN_F10_TMP_EXECUTED\\n'`],
    ["N2.01: colon IFS exposes a separate shared-root operand", `IFS=:; P='/home/paul/w/own:/tmp'; rm -rf $P`],
    ["N2.02: comma IFS exposes a separate shared-root operand", `IFS=,; P='/home/paul/w/own,/var/tmp'; rm -rf $P`],
    ["N2.03: default IFS exposes a separate shared-root operand", `P='/home/paul/w/own /tmp'; rm -rf $P`],
    ["N2.04: colon IFS splits two shared roots", `IFS=:; P='/tmp:/var/tmp'; rm -rf $P`],
    ["N2.05: multiple IFS delimiters expose a shared root", `IFS=:,; P='/home/paul/w/own,/tmp:/home/paul/w/other'; rm -rf $P`],
    ["N2.06: splitting an exact-owned-prefix field exposes a shared root", `IFS=:; P='/tmp/tmp.AbC123:/tmp'; rm -rf $P`],
    ["N2.07: unquoted IFS splitting binds a shared second positional", `IFS=:; P='/home/paul/w/own:/tmp'; set -- $P; rm -rf "$2"`],
    ["N3.01: pushd changes relative deletion to a shared root", `pushd /tmp; rm -rf tmp.*`],
    ["N3.02: pushd resolves an assigned shared root", `T=/var/tmp; pushd "$T"; rm -rf *`],
    ["N3.03: env -C changes cwd in the child shell", `env -C /tmp sh -c 'rm -rf tmp.*'`],
    ["N3.04: env --chdir= changes cwd in the child shell", `env --chdir=/tmp sh -c 'rm -rf tmp.*'`],
    ["N3.05: sudo -D changes cwd in the child shell", `sudo -D /tmp sh -c 'rm -rf tmp.*'`],
    ["N3.06: sudo --chdir= changes cwd in the child shell", `sudo --chdir=/tmp sh -c 'rm -rf tmp.*'`],
    ["N3.07: env child cwd applies to a direct relative operand", `env -C /tmp rm -rf tmp.*`],
    ["N3.08: env child deletion uses its shared cwd", `cd /home/paul/w; env -C /tmp sh -c 'rm -rf tmp.*'`],
    ["N3.09: pushd inside a child applies to its deletion", `cd /home/paul/w; (pushd /tmp; rm -rf tmp.*)`],
    ["N3.10: pushd stack rotation returns to a shared root", `pushd /tmp; pushd /tmp/tmp.AbC123; pushd +1; rm -rf *`],
    ["N4.01: set binds a literal shared-root positional operand", `set -- /tmp; rm -rf "$1"`],
    ["N4.02: set binds a glob value whose unquoted use expands", `set -- '/tmp/tmp.*'; rm -rf $1`],
    ["N4.03: set binds captured shared-listing positional provenance", `set -- $(ls -d /tmp/tmp.*); rm -rf "$1"`],
    ["N4.04: read binds a literal shared-root here-string", `read -r D <<< '/tmp'; rm -rf "$D"`],
    ["N4.05: mapfile binds a literal shared-root here-string", `mapfile -t dirs <<< '/tmp'; rm -rf "\${dirs[@]}"`],
    ["N4.06: printf -v binds a literal shared root", `printf -v D '%s' /tmp; rm -rf "$D"`],
    ["N4.07: printf -v assembles an unquoted shared glob", `printf -v P '%s/%s' /tmp 'tmp.*'; rm -rf $P`],
    ["N4.08: set binds aggregate positional operands including a shared root", `set -- /tmp/tmp.AbC123 /var/tmp; rm -rf "$@"`],
    ["N4.09: literal read with custom IFS binds an unsafe second destination", `IFS=:; read -r first D <<< '/home/paul/w/own:/tmp'; rm -rf "$D"`],
    ["N4.10: literal multiline mapfile retains an unsafe later element", `mapfile -t dirs <<< '/tmp/tmp.AbC123\n/tmp'; rm -rf "\${dirs[@]}"`],
    ["N4.11: printf -v cycles its format for an unsafe later operand", `printf -v P '%s\\n' /home/paul/w/own /tmp; rm -rf $P`],
    ["N4.12: bounded printf percent-q identity binds a shared root", `printf -v P '%q' /tmp; rm -rf $P`],
    ["N6.01: array append preserves unsafe original glob elements", `dirs=(/tmp/tmp.*); dirs+=(/tmp/tmp.AbC123); rm -rf "\${dirs[@]}"`],
    ["N6.02: array append preserves unsafe original captured elements", `dirs=($(ls -d /tmp/tmp.*)); dirs+=(/tmp/tmp.AbC123); rm -rf "\${dirs[@]}"`],
    ["N6.03: array append adds unsafe elements to a safe own array", `dirs=(/tmp/tmp.AbC123); dirs+=(/tmp/tmp.*); rm -rf "\${dirs[@]}"`],
    ["N6.04: array append through an owned variable retains old unsafe elements", `D=$(mktemp -d); dirs=(/tmp/tmp.*); dirs+=("$D"); rm -rf "\${dirs[@]}"`],
  ];
  const allowed: Array<[string, string]> = [
    ["F9.01: saved owned listing feeds stdin/read — conservative refusal", `find /tmp/tmp.AbC123 -name '*.json' > .local/selected.list; while read -r d; do rm -rf "$d"; done < .local/selected.list`],
    ["F9.02: saved owned listing feeds xargs -a", `find /tmp/tmp.AbC123 -name '*.json' > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.03: saved owned listing feeds inline --arg-file", `find /tmp/tmp.AbC123 -name '*.json' -print0 > .local/selected.list; xargs -0 --arg-file=.local/selected.list rm -rf`],
    ["F9.04: explicit cat of saved owned output remains independent", `find /tmp/tmp.AbC123 -name '*.json' > .local/selected.list; cat .local/selected.list | xargs rm -rf`],
    ["F9.05: resolved aliases identify the same saved owned output", `F=.local/selected.list; G="$F"; find /tmp/tmp.AbC123 -name '*.json' > "$F"; xargs -a "$G" rm -rf`],
    ["F9.06: append owned output to an owned list remains safe", `cat .local/owned.list > .local/selected.list; find /tmp/tmp.AbC123 -name '*.json' >> .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.07: fd2 does not save the shared stdout as file contents", `ls -d /tmp/tmp.* 2> .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.08: fd3 output does not contaminate a preexisting owned file", `ls -d /tmp/tmp.* 3> .local/diagnostic.log; xargs -a .local/owned.list rm -rf`],
    ["F9.09: brace producer saves owned output", `{ find /tmp/tmp.AbC123 -name '*.json'; } > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.10: loop producer saves owned output", `for t in /tmp/tmp.AbC123 /var/tmp/tmp.Def456; do find "$t" -name '*.json'; done > .local/selected.list; cat .local/selected.list | xargs rm -rf`],
    ["F9.11: subshell producer saves owned output", `(find /tmp/tmp.AbC123 -name '*.json') > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.12: redirected shared stdout does not reach the outgoing pipe", `ls -d /tmp/tmp.* > .local/selected.list | cat | xargs rm -rf`],
    ["F9.13: captured cat reads saved owned output", `find /tmp/tmp.AbC123 -name '*.json' > .local/selected.list; rm -rf $(cat .local/selected.list)`],
    ["F9.14: owned overwrite replaces unsafe output", `ls -d /tmp/tmp.* > .local/selected.list; find /tmp/tmp.AbC123 -name '*.json' > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.15: unrelated shared diagnostic does not taint saved owned output", `ls -d /tmp/tmp.*; cat .local/owned.list > .local/selected.list; xargs -a .local/selected.list rm -rf`],
    ["F9.16: empty producer from an exact own directory remains allowed", `find /tmp/tmp.AbC123 -maxdepth 1 -false > tmp.list; xargs -r rm -rf < tmp.list; printf 'MAIN_F9_TMP_EXECUTED\\n'`],
    ["F10.01: operand-free captured cat consumes inherited owned stdin", `find /tmp/tmp.AbC123 -name '*.json' | sh -c 'rm -rf $(cat)'`],
    ["F10.02: captured stdin consumer pipeline preserves owned output — conservative refusal", `find /tmp/tmp.AbC123 -name '*.json' | sh -c 'rm -rf $(cat | tr "\\n" " ")'`],
    ["F10.03: explicit recorded-file cat ignores inherited shared stdin", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(cat .local/owned.list)'`],
    ["F10.04: owned inner producer remains independent of owned inherited stdin", `cat .local/owned.list | sh -c 'rm -rf $(find /tmp/tmp.AbC123 -name "*.json")'`],
    ["F10.05: nonconsumer capture ignores inherited shared stdin", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(printf "%s" /tmp/tmp.AbC123)'`],
    ["F10.06: inherited owned stdin reaches a nested local child capture", `find /tmp/tmp.AbC123 -name '*.json' | sh -c 'bash -c "rm -rf \\$(cat)"'`],
    ["F10.07: child capture binding does not leak to the parent", `D=/tmp/tmp.AbC123; ls -d /tmp/tmp.* | sh -c 'D=$(cat); :'; rm -rf "$D"`],
    ["F10.08: fd0 recorded input overrides inherited shared stdin for capture", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(cat)' < .local/owned.list`],
    ["F10.09: explicit owned head source ignores inherited shared stdin — conservative refusal", `ls -d /tmp/tmp.* | sh -c 'rm -rf $(head -n 1 .local/owned.list)'`],
    ["F10.10: capture assignment preserves inherited owned stdin", `find /tmp/tmp.AbC123 -name '*.json' | sh -c 'D=$(cat); rm -rf "$D"'`],
    ["F10.11: empty exact-own-directory inherited capture remains allowed", `find /tmp/tmp.AbC123 -maxdepth 1 -false | bash -c 'rm -rf $(cat)'; printf 'MAIN_F10_TMP_EXECUTED\\n'`],
    ["N2.01: quoted colon value is one non-root name", `IFS=:; P='/home/paul/w/own:/tmp'; rm -rf "$P"`],
    ["N2.02: quoted comma value is one non-root name", `IFS=,; P='/home/paul/w/own,/var/tmp'; rm -rf "$P"`],
    ["N2.03: quoted whitespace value is one non-root name", `P='/home/paul/w/own /tmp'; rm -rf "$P"`],
    ["N2.04: quoted colon-separated roots are one non-root name", `IFS=:; P='/tmp:/var/tmp'; rm -rf "$P"`],
    ["N2.05: quoted multiple-delimiter value is one non-root name", `IFS=:,; P='/home/paul/w/own,/tmp:/home/paul/w/other'; rm -rf "$P"`],
    ["N2.06: quoted exact-owned-prefix value is one name", `IFS=:; P='/tmp/tmp.AbC123:/tmp'; rm -rf "$P"`],
    ["N2.07: quoted set operand remains one non-root positional name", `IFS=:; P='/home/paul/w/own:/tmp'; set -- "$P"; rm -rf "$1"`],
    ["N3.01: pushd changes relative deletion to an owned directory", `pushd /tmp/tmp.AbC123; rm -rf tmp.*`],
    ["N3.02: pushd resolves an assigned owned directory", `T=/var/tmp/tmp.Def456; pushd "$T"; rm -rf *`],
    ["N3.03: env -C uses the owned child cwd", `env -C /tmp/tmp.AbC123 sh -c 'rm -rf tmp.*'`],
    ["N3.04: env --chdir= uses the owned child cwd", `env --chdir=/tmp/tmp.AbC123 sh -c 'rm -rf tmp.*'`],
    ["N3.05: sudo -D uses the owned child cwd", `sudo -D /tmp/tmp.AbC123 sh -c 'rm -rf tmp.*'`],
    ["N3.06: sudo --chdir= uses the owned child cwd", `sudo --chdir=/tmp/tmp.AbC123 sh -c 'rm -rf tmp.*'`],
    ["N3.07: env child cwd applies to an owned direct relative operand", `env -C /tmp/tmp.AbC123 rm -rf tmp.*`],
    ["N3.08: env child cwd does not leak to the parent", `cd /home/paul/w; env -C /tmp sh -c ':'; rm -rf tmp.*`],
    ["N3.09: pushd inside a child does not leak cwd to the parent", `cd /home/paul/w; (pushd /tmp); rm -rf tmp.*`],
    ["N3.10: pushd stack rotation returns to an owned directory", `pushd /tmp/tmp.AbC123; pushd /tmp; pushd +1; rm -rf *`],
    ["N4.01: set binds an exact owned positional operand", `set -- /tmp/tmp.AbC123; rm -rf "$1"`],
    ["N4.02: quoted positional use preserves the literal single-name glob", `set -- '/tmp/tmp.*'; rm -rf "$1"`],
    ["N4.03: set binds captured owned-listing positional provenance", `set -- $(find /tmp/tmp.AbC123 -name '*.json'); rm -rf "$1"`],
    ["N4.04: read binds a literal owned here-string — conservative refusal", `read -r D <<< '/tmp/tmp.AbC123'; rm -rf "$D"`],
    ["N4.05: mapfile binds a literal owned here-string — conservative refusal", `mapfile -t dirs <<< '/tmp/tmp.AbC123'; rm -rf "\${dirs[@]}"`],
    ["N4.06: printf -v binds an exact owned directory", `printf -v D '%s' /tmp/tmp.AbC123; rm -rf "$D"`],
    ["N4.07: quoted printf -v glob is a literal single name", `printf -v P '%s/%s' /tmp 'tmp.*'; rm -rf "$P"`],
    ["N4.08: set binds only exact owned aggregate positional operands", `set -- /tmp/tmp.AbC123 /var/tmp/tmp.Def456; rm -rf "$@"`],
    ["N4.09: literal read with custom IFS binds an owned second destination — conservative refusal", `IFS=:; read -r first D <<< '/home/paul/w/own:/tmp/tmp.AbC123'; rm -rf "$D"`],
    ["N4.10: literal multiline mapfile has only owned elements — conservative refusal", `mapfile -t dirs <<< '/tmp/tmp.AbC123\n/var/tmp/tmp.Def456'; rm -rf "\${dirs[@]}"`],
    ["N4.11: printf -v cycles its format for only owned operands", `printf -v P '%s\\n' /home/paul/w/own /tmp/tmp.AbC123; rm -rf $P`],
    ["N4.12: bounded printf percent-q identity preserves an exact own path — conservative refusal", `printf -v P '%q' /tmp/tmp.AbC123; rm -rf $P`],
    ["N6.01: array append preserves safe original own glob elements", `dirs=(/tmp/tmp.AbC123/*.json); dirs+=(/tmp/tmp.AbC123); rm -rf "\${dirs[@]}"`],
    ["N6.02: array append preserves safe original captured own elements", `dirs=($(find /tmp/tmp.AbC123 -name '*.json')); dirs+=(/tmp/tmp.AbC123); rm -rf "\${dirs[@]}"`],
    ["N6.03: array append adds only own elements to a safe own array", `dirs=(/tmp/tmp.AbC123); dirs+=(/tmp/tmp.AbC123/*.json); rm -rf "\${dirs[@]}"`],
    ["N6.04: array append through an owned variable keeps an own array safe", `D=$(mktemp -d); dirs=(/tmp/tmp.AbC123/*.json); dirs+=("$D"); rm -rf "\${dirs[@]}"`],
  ];

  it.each(refused)("refuses %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { wipe: true, blocked: false, exhausted: false });
  });
  it.each(allowed)("allows %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { wipe: _label.endsWith(" — conservative refusal"), blocked: false, exhausted: false });
  });
});

// Intentional conservative false positives above: unsupported fd reads, declaration/array
// bindings, pipeline/background replacements, transformations and reused fds cannot grant
// ownership. Use a standalone literal/scalar or fresh mktemp command instead (DATA only).
describe("PR166 conservative refusal simple allowed counterparts", () => {
  it.each([
    ["fdread: standalone exact literal", `rm -rf /tmp/tmp.AbC123`],
    ["declarationprefix/array: plain scalar", `D=/tmp/tmp.X1; rm -rf "$D"`],
    ["pipeline/backgroundbinding: standalone mktemp", `D=$(mktemp -d); cd "$D"; rm -rf "$D"`],
    ["transformation: plain scalar instead of percent-q", `D=/tmp/tmp.X1; rm -rf "$D"`],
    ["reusedfd: fresh standalone exact literal", `rm -rf /tmp/tmp.AbC123`],
  ])("allows %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { wipe: false, blocked: false, exhausted: false });
  });
});
