import { describe, expect, it } from "vitest";
import { expectRound14Migration } from "./pr166-round14-policy.js";
import { killsByPattern, PATTERN_KILL_REASON, scanCommand, wipesTmp } from "../src/core/pattern-kill.js";

// R5 owner scope cut: only these exact formerly-allowed complex commands migrate.
// IDs/commands and the historical 102 false positives are unchanged.
const round5IntentionalState = new Set<string>([
  "P=~/.local/state/dev-lead-posts; printf -- '- %s NOTE: I ran pkill -f \"retry418.sh\" (name pattern, against #774)\\n' \"$(date -u +%H:%MZ)\" >> \"$P/notes.md\"",
  "REPLY=4242; pgrep -f worker | while read -r -p REPLY -n 1 -u 0 -- p; do kill \"$REPLY\"; done",
  "eval \"kill $PID\"",
  "printf -v P '%s' 4242; kill \"$P\"",
  "PID=$!; printf -v Q '%s' \"$PID\"; kill \"$Q\"",
  "PID=$!; set -- \"$PID\"; kill \"$1\"",
  "pids=(4242); pids+=(4243); kill \"${pids[@]}\"",
  "pids=($(cat .local/server.pid)); pids+=($(cat .local/worker.pid)); kill \"${pids[@]}\"",
  "pids=(4242); pids+=($(cat .local/server.pid)); kill \"${pids[@]}\"",
  "PID=$!; pids=(4242); pids+=(\"$PID\"); kill \"${pids[@]}\""
]);
// R9 owner grammar allowlist: exact old allowances intentionally become STATE.
// Their original IDs/command bytes stay unchanged; no unsafe R becomes A.
const round9IntentionalState = new Set<string>([
  'env -S "kill 4242"',
  'env -S "kill $(cat run.pid)"',
  "env -iS 'kill 4242'",
  'xargs -a run.pids sh -c \'kill "$@"\' _',
  'cat run.pids | xargs sh -c \'kill "$@"\' _',
  'command -v pkill',
  'set -- 4242 4243; kill "$@"',
]);
// R11 owner UNKNOWN-output cut: exact formerly-allowed loop feeds only.
// Original test IDs and command bytes remain unchanged; each baseline A was
// independently observed before this expectation-only migration (#2656).
const round11IntentionalPattern = new Set<string>([
  'for file in .local/server.pid .local/worker.pid; do cat "$file"; done | xargs kill',
  'grep -q ready .local/server.log; for i in once; do cat .local/server.pid; done | xargs kill',
  'for file in .local/server.pid .local/worker.pid; do cat "$file"; done > .local/selected.pids; cat .local/selected.pids | xargs kill',
]);
function expectRound5Guard(command: string, result: ReturnType<typeof scanCommand>, original: { blocked?: boolean; wipe?: boolean; exhausted?: boolean; overall?: boolean }): void {
  if (expectRound14Migration(command, result)) return;
  if (round11IntentionalPattern.has(command)) {
    expect(result, command).toStrictEqual({ blocked: true, wipe: false, exhausted: false });
    expect(killsByPattern(command), command).toBe(true);
    expect(wipesTmp(command), command).toBe(false);
    return;
  }
  const intentional = round5IntentionalState.has(command) || round9IntentionalState.has(command);
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


// smarty-dev#774: the three real incidents verbatim, then the forms the rule refuses and the
// reads and recorded-PID kills it must still allow.
const refused: Array<[string, string]> = [
  ["incident 09-25 14:05Z: the Lucky stub test on Dev1", `pkill -f '[w]atch'`],
  ["incident 09-26 03:05Z: over ssh on m4max", `ssh m4max "pkill -f 'sleep 30' -P 1"`],
  ["incident 09-26 03:05Z: the command itself", `pkill -f 'sleep 30' -P 1`],
  ["incident 09-27 ~23:2xZ: dev-lead's own shell", `pkill -f "retry418.sh"`],
  ["incident 09-27, unquoted", `pkill -f retry418.sh`],
  ["dev-lead's earlier slip", `cd /tmp; pkill -f retry226.sh; nohup ./retry226.sh > /tmp/retry226.log 2>&1 &`],
  ["a pkill after a cat", `cat /tmp/retry278.log; pkill -f "^/bin/bash ./retry278.sh" 2>/dev/null`],
  ["killall", `killall node`],
  ["a pkill by path", `/usr/bin/pkill -U paul sleep`],
  ["a pkill behind sudo and timeout", `sudo timeout 5 pkill -9 vitest`],
  ["a pkill behind env", `env -u GH_TOKEN FOO=1 pkill -f watcher`],
  ["a pkill after &&", `make build && pkill -f server`],
  ["a pkill in a pipeline's last stage", `echo x | pkill -f y`],
  ["a pkill on the next line", `echo start\npkill -f x`],
  ["a pkill after a backslash continuation", `sleep 1 && \\\n  pkill -f x`],
  ["kill $(pgrep …)", `kill $(pgrep -f retry418.sh)`],
  ["kill -9 $(pgrep …)", `kill -9 $(pgrep -f vitest)`],
  ["kill \`pgrep …\`", "kill `pgrep -f server`"],
  ["kill \"$(pgrep …)\"", `kill "$(pgrep -f server)"`],
  ["kill $(pidof …)", `kill $(pidof node)`],
  ["kill $(ps | grep | awk)", `kill $(ps aux | grep '[s]erver' | awk '{print $2}')`],
  ["pgrep | xargs kill", `pgrep -f watch | xargs kill`],
  ["pgrep | xargs -r kill -9", `pgrep -f watch | xargs -r kill -9`],
  ["pgrep |\\n xargs kill", `pgrep -f watch |\n  xargs kill`],
  ["ps | grep | awk | xargs kill", `ps aux | grep '[w]atch' | awk '{print $2}' | xargs kill`],
  ["a captured PID list", `P=$(pgrep -f server); kill $P`],
  ["a for loop over pgrep", `for p in $(pgrep -f server); do kill "$p"; done`],
  ["a while loop fed by pgrep", `pgrep -f server | while read p; do kill $p; done`],
  // Security N5 on pi-fabric#148: the same lookup fed through a redirect.
  ["a while loop fed by a pgrep process substitution", `while read p; do kill "$p"; done < <(pgrep x)`],
  ["xargs kill fed by a pgrep here-string", `xargs kill <<< "$(pgrep x)"`],
  // Round 3 security N8 on #148: a compound producer's lookup survives the closing group/loop.
  ["N8: a brace group lookup feeds xargs kill", `{ pgrep x; } | xargs kill`],
  ["N8: a subshell lookup feeds xargs kill", `(pgrep x) | xargs kill`],
  ["N8: a loop lookup feeds xargs kill", `for name in worker server; do pgrep -f "$name"; done | xargs kill`],
  ["N8: a brace group lookup feeds read then kill", `{ pgrep x; } | while read -r p; do kill "$p"; done`],
  ["N8: a subshell lookup feeds read then kill", `(pgrep x) | while read -r p; do kill "$p"; done`],
  ["N8: a loop lookup feeds read then kill", `for name in worker server; do pgrep -f "$name"; done | while read -r p; do kill "$p"; done`],
  // Round 4 F6: an explicit recorded source must not erase an actual lookup feed elsewhere.
  ["F6: a redirected lookup survives an intermediary cat", `while read -r p; do kill "$p"; done < <(pgrep -f worker | cat)`],
  ["F6: a compound lookup survives an intermediary cat", `{ pgrep -f worker; } | cat | xargs kill`],
  ["F6: a piped lookup survives cat before read", `pgrep -f worker | cat | while read -r p; do kill "$p"; done`],
  ["F6: a PID-file loop does not clear a later lookup redirect", `while read -r p; do kill "$p"; done < .local/server.pid; while read -r p; do kill "$p"; done < <(pgrep -f worker)`],
  ["F6: a PID-file loop does not clear an earlier lookup redirect", `while read -r p; do kill "$p"; done < <(pgrep -f worker); while read -r p; do kill "$p"; done < .local/server.pid`],
  ["F6: an output-only redirect does not clear a lookup pipe", `pgrep -f worker | xargs kill > .local/cleanup.log`],
  ["F6: an fd3 input redirect does not clear actual lookup stdin", `pgrep x | xargs kill 3< .local/server.pid`],
  ["F6: grouped cat without operands preserves actual lookup stdin", `pgrep x | { cat; } | xargs kill`],
  // Round 5 F7/F8: these shell commands are test data only, never executed.
  ["F7.01: inline sh inherits lookup stdin for xargs kill", `pgrep -f worker | sh -c 'xargs kill'`],
  ["F7.02: inline sh inherits lookup stdin for a read loop", `pgrep -f worker | sh -c 'while read -r p; do kill "$p"; done'`],
  ["F7.03: unsafe inner xargs after an unrelated recorded PID diagnostic", `sh -c 'cat .local/server.pid; pgrep -f worker | xargs kill'`],
  ["F7.04: unsafe inner read loop after an unrelated recorded PID diagnostic", `sh -c 'cat .local/server.pid; pgrep -f worker | while read -r p; do kill "$p"; done'`],
  ["F8.01: implicit read destination REPLY receives lookup stdin", `pgrep -f worker | while read -r; do kill "$REPLY"; done`],
  ["F8.02: read defaults to REPLY after delimiter and timeout options", `while read -r -d '' -t 1; do kill "$REPLY"; done < <(pgrep -f worker | tr '\\n' '\\0')`],
  ["F8.03: read destination after prompt count fd and -- options", `REPLY=4242; pgrep -f worker | while read -r -p REPLY -n 1 -u 0 -- p; do kill "$p"; done`],
  ["F8.04: read -a array receives lookup stdin", `read -r -d '' -a pids < <(pgrep -f worker); kill "\${pids[@]}"`],
  ["F8.05: mapfile defaults to MAPFILE after option arguments and --", `mapfile -t -n 1 -O 0 -s 0 -C : -c 1 -- < <(pgrep -f worker); kill "\${MAPFILE[@]}"`],
  ["F8.06: readarray defaults to MAPFILE after delimiter and --", `readarray -d '' -t -- < <(pgrep -f worker | tr '\\n' '\\0'); kill "\${MAPFILE[@]}"`],
  ["F7.05: brace lookup producer feeds inline sh xargs kill", `{ pgrep -f worker; } | sh -c 'xargs kill'`],
  ["F7.06: inherited safe stdin does not suppress a lookup inner brace producer", `cat .local/server.pid | sh -c "{ pgrep x; } | xargs kill"`],
  ["bash -c 'pkill …'", `bash -c 'pkill -f server'`],
  ["bash -lc 'kill $(pgrep …)'", `bash -lc 'kill $(pgrep -f server)'`],
  ["sh -c with pgrep | xargs kill", `sh -c "pgrep -f x | xargs kill"`],
  ["ssh HOST 'kill $(pgrep …)'", `ssh -p 22 m4max 'kill $(pgrep -f "sleep 30")'`],
  ["ssh HOST 'pgrep … | xargs kill'", `ssh m4max 'pgrep -f sleep | xargs kill'`],
  ["ssh HOST bash -c '…'", `ssh m4max bash -c "'pkill -f sleep'"`],
  ["ssh pgrep piped into a local xargs kill", `ssh m4max pgrep -f sleep | xargs kill`],
  ["a heredoc script fed to ssh", `ssh m4max <<'EOF'\ncd /tmp\npkill -f sleep\nEOF`],
  ["a heredoc script fed to bash", `bash <<EOF\nkillall node\nEOF`],
  ["eval", `eval "pkill -f server"`],
  ["a pkill in a command substitution", `echo "$(pkill -f server)"`],
  ["a pkill in a subshell", `(cd /tmp && pkill -f x)`],
  ["a pkill in an if", `if true; then pkill -f x; fi`],
  // review/astra F1 on #105: a lookup expanded into the script a shell, eval or ssh runs.
  ["bash -c \"kill $(pgrep …)\"", `bash -c "kill $(pgrep -f worker)"`],
  ["eval \"kill $(pgrep …)\"", `eval "kill $(pgrep -f worker)"`],
  ["ssh HOST \"kill $(pgrep …)\"", `ssh HOST "kill $(pgrep -f worker)"`],
  ["ssh HOST kill $(pgrep …), unquoted", `ssh HOST kill $(pgrep -f worker)`],
  ["ssh HOST \"kill `pgrep …`\"", "ssh HOST \"kill `pgrep -f worker`\""],
  // review/astra F2 on #105: substitutions in a redirection target and an unquoted heredoc run.
  ["a redirection target", `: >"$(pkill -f worker)"`],
  ["an unquoted redirection target", `echo x > $(pkill -f worker).log`],
  ["single quotes in an unquoted heredoc", `cat <<EOF\n'$(pkill -f worker)'\nEOF`],
  ["a # line in an unquoted heredoc", `cat <<EOF\n# $(pkill -f worker)\nEOF`],
  ["a backtick in an unquoted heredoc", "cat <<EOF\nnote `pkill -f worker`\nEOF"],
  ["a lookup kill in an unquoted heredoc", `cat <<EOF\nstopped: $(kill $(pgrep -f worker))\nEOF`],
  // review/astra F3 on #105: a leading redirection and long or clustered wrapper options.
  ["a leading 2> redirection", `2>/dev/null pkill -f worker`],
  ["a leading >file redirection", `>out.log pkill -f worker`],
  ["sudo --user X", `sudo --user paul pkill -f worker`],
  ["sudo -Eu X", `sudo -Eu paul pkill -f worker`],
  ["env --unset X", `env --unset GH_TOKEN pkill -f worker`],
  ["env --unset=X", `env --unset=GH_TOKEN pkill -f worker`],
  ["env -S 'pkill …'", `env -S 'pkill -f worker'`],
  ["nice -n 5", `nice -n 5 pkill -f worker`],
  ["timeout 5", `timeout 5 pkill -f worker`],
  ["timeout --signal KILL 5", `timeout --signal KILL 5 pkill -f worker`],
  ["nohup", `nohup pkill -f worker &`],
  ["setsid", `setsid pkill -f worker`],
  ["command", `command pkill -f worker`],
  ["exec", `exec pkill -f worker`],
  ["xargs -r kill", `pgrep -f worker | xargs -r kill`],
  ["xargs --max-args 1 kill", `pgrep -f worker | xargs --max-args 1 kill -TERM`],
  ["a lookup kill with a trailing redirection", `kill $(pgrep -f worker) 2>/dev/null`],
  // review/astra F4 on #105: an attached short-option value.
  ["sudo -uroot", `sudo -uroot pkill -f worker`],
  ["sudo --user=root", `sudo --user=root pkill -f worker`],
  // review/astra F5 on #105: lookup output carried into a variable, a wrapper, or a quoted PID.
  ["p=$(pgrep x); kill $p", `p=$(pgrep x); kill $p`],
  ["p=$(pgrep x); kill -9 \"\${p}\"", `p=$(pgrep x); kill -9 "\${p}"`],
  ["a captured lookup, then bash -c", `P=$(pgrep -f worker); bash -c "kill $P"`],
  ["a captured lookup, then eval", `P=$(pgrep -f worker); eval "kill $P"`],
  ["a captured lookup, then ssh", `P=$(pgrep -f worker); ssh HOST "kill $P"`],
  ["a captured lookup inside bash -c", `bash -c 'p=$(pgrep x); kill $p'`],
  ["a captured lookup inside eval", `eval 'p=$(pgrep x); kill $p'`],
  ["a quoted expanded PID in bash -c", `bash -c "kill '$(pgrep -f worker)'"`],
  ["a PGID read by ps", `PGID=$(ps -o pgid= -p "$PID" | tr -d ' '); kill -TERM -- -$PGID`],
  ["ps piped into xargs kill", `ps -o pid= -p "$PID" | xargs kill`],
  ["a PID list from grep alone", `kill $(grep -l worker /proc/*/cmdline | cut -d/ -f3)`],
  ["a captured lookup killed in a substitution", `P=$(pgrep -f worker); echo "$(kill $P)"`],
  // review/astra F6 on #105: lookups expanded into an env -S script or a heredoc script.
  ["env -S \"kill $(pgrep …)\"", `env -S "kill $(pgrep -f worker)"`],
  ["env --split-string \"kill $(pgrep …)\"", `env --split-string "kill $(pgrep -f worker)"`],
  ["env --split-string=\"kill $(pgrep …)\"", `env --split-string="kill $(pgrep -f worker)"`],
  ["an unquoted heredoc to bash with a quoted lookup", `bash <<EOF\nkill '$(pgrep -f worker)'\nEOF`],
  ["an unquoted heredoc to ssh with a quoted lookup", `ssh HOST <<EOF\nkill '$(pgrep -f worker)'\nEOF`],
  // review/astra F7 on #105: env's split string in a short cluster.
  ["env -iS 'pkill …'", `env -iS 'pkill -f worker'`],
  ["env -iS'pkill …'", `env -iS'pkill -f worker'`],
  // review/astra F8 on #105: provenance per operand inside a heredoc or -c script.
  ["a heredoc diagnostic, then a lookup kill", `bash <<EOF\necho "workers: $(pgrep -c worker)"\nkill $(pgrep -f worker)\nEOF`],
  ["a captured lookup killed inside a heredoc", `bash <<'EOF'\np=$(pgrep x); kill $p\nEOF`],
  ["a captured lookup killed inside an unquoted heredoc", `bash <<EOF\np=\\$(pgrep x); kill \\$p\nEOF`],
  ["a bash -c diagnostic, then a lookup kill", `bash -c "echo $(pgrep -c worker); kill $(pgrep -f worker)"`],
  // review/astra F9 on #105: positional parameters after -c, and xargs input.
  ["bash -c 'kill \"$1\"' _ \"$(pgrep …)\"", `bash -c 'kill "$1"' _ "$(pgrep -f worker)"`],
  ["bash -c 'kill \"$@\"' _ $(pgrep …)", `bash -c 'kill "$@"' _ $(pgrep -f worker)`],
  ["sh -c 'kill $0' $(pgrep …)", `sh -c 'kill $0' $(pgrep -f worker)`],
  ["a captured lookup passed as $1", `P=$(pgrep -f worker); bash -c 'kill "$1"' _ "$P"`],
  ["pgrep | xargs sh -c 'kill \"$@\"' _", `pgrep x | xargs sh -c 'kill "$@"' _`],
  ["pgrep | xargs -r sh -c 'kill \"$@\"' _", `pgrep -f worker | xargs -r sh -c 'kill "$@"' _`],
  ["pgrep | xargs -I{} sh -c 'kill {}'", `pgrep -f worker | xargs -I{} sh -c 'kill {}'`],
  ["a grep-selected PID", `kill $(ps aux | grep '[w]orker' | awk '{print $2}')`],
];

const allowed: Array<[string, string]> = [
  ["a string that mentions pkill", `echo "never use pkill"`],
  ["a grep for pkill", `grep -rn pkill src/ docs/`],
  ["a grep for kill $(pgrep", `grep -rn 'kill $(pgrep' .`],
  ["dev-lead's note that quotes the incident",
    `P=~/.local/state/dev-lead-posts; printf -- '- %s NOTE: I ran pkill -f "retry418.sh" (name pattern, against #774)\\n' "$(date -u +%H:%MZ)" >> "$P/notes.md"`],
  ["a heredoc body that describes the incident",
    `cat > /tmp/incident.md <<'EOF'\n**What ran.** lucky-sweep ran this on m4max:\n\`\`\`\npkill -f 'sleep 30' -P 1\n\`\`\`\nEOF`],
  ["a gh api comment that names the forms",
    `gh api repos/o/r/issues/774/comments -f body='Refused: pkill, killall, kill $(pgrep x), pgrep x | xargs kill'`],
  ["a commit message", `git commit -m "fix: refuse pkill and kill \\$(pgrep)"`],
  ["kill with literal PIDs", `kill 12345 12346`],
  ["kill -TERM of a process group", `kill -TERM -- -4242`],
  ["kill %job", `sleep 100 & kill %1`],
  ["kill -0", `kill -0 12345 && echo alive`],
  ["kill -0 of a looked-up PID", `kill -0 $(pgrep -f server)`],
  ["a recorded PID from $!", `./server & PID=$!; sleep 2; kill "$PID"`],
  ["a recorded PID from a file", `kill $(cat /tmp/x/server.pid)`],
  ["a recorded PID with a redirect", `kill "$PID" 2>/dev/null || true`],
  ["pgrep alone", `pgrep -fl vitest`],
  ["pgrep -c", `pgrep -f 'retry22[678]' -c`],
  ["pgrep in a while wait", `while pgrep -f build >/dev/null; do sleep 1; done`],
  ["a check after a recorded kill", `kill $PID; sleep 1; pgrep -f server || echo gone`],
  ["a ps check before a group kill", `ps -o pid,ppid,pgid,command -p $PID; kill -- -$PGID`],
  ["a ps | grep check before a recorded kill", `ps -eo pid,pgid,args | grep server; kill -- -$PGID`],
  ["smarty-reap stop", `bin/smarty-reap stop .local/reap/server.json`],
  ["smarty-reap run", `bin/smarty-reap run .local/reap/server.json -- ./server --port 8080`],
  ["ssh with a literal kill", `ssh m4max 'kill 4242'`],
  ["ssh pgrep alone", `ssh m4max "pgrep -fl 'sleep 30'"`],
  ["bash -c echo pkill", `bash -c 'echo pkill is refused'`],
  ["a comment that mentions pkill", `ls # never pkill here`],
  ["a word that contains pkill", `./pkill-guard-test.sh --dry-run`],
  ["a file named pkill", `cat docs/pkill.md`],
  ["a literal kill after a captured lookup", `P=$(pgrep -f server); kill 4242`],
  ["a comment that holds a pkill after ;", `ls # do not run: sleep 1; pkill -f x`],
  ["pgrep, then a recorded kill on the next line", `pgrep -fl server\nkill "$PID"`],
  // Counterexamples for review/astra on #105.
  ["pgrep piped into a literal kill", `pgrep -f worker | kill 4242`],
  ["pgrep, then a PID file into xargs kill on the next line", `pgrep -fl worker\ncat run.pid | xargs kill`],
  ["env -S with a literal PID", `env -S "kill 4242"`],
  ["env -S with a PID file", `env -S "kill $(cat run.pid)"`],
  ["env -iS with a literal PID", `env -iS 'kill 4242'`],
  ["env -i with another command", `env -i PATH=/usr/bin ls -l`],
  ["an unquoted heredoc to bash with a PID file", `bash <<EOF\nkill '$(cat run.pid)'\nEOF`],
  ["an unquoted heredoc to bash with a literal PID", `bash <<EOF\nkill 4242\nEOF`],
  ["a quoted-delimiter heredoc to bash with a quoted lookup (inert)", `bash <<'EOF'\nkill '$(pgrep -f worker)'\nEOF`],
  ["a quoted-delimiter heredoc note to cat", `cat <<'EOF'\nkill '$(pgrep -f worker)'\nEOF`],
  ["a lookup expanded into a heredoc to cat", `cat <<EOF > note.md\nworkers: '$(pgrep -f worker)'\nEOF`],
  // Counterexamples for F8 and F9.
  ["a heredoc diagnostic, then a PID-file kill", `bash <<EOF\necho "workers: $(pgrep -c worker)"\nkill $(cat run.pid)\nEOF`],
  ["a bash -c diagnostic, then a PID-file kill", `bash -c "echo $(pgrep -c worker); kill $(cat run.pid)"`],
  ["bash -c 'kill \"$1\"' _ 12345", `bash -c 'kill "$1"' _ 12345`],
  ["bash -c with a lookup in another positional", `bash -c 'kill "$1"; echo "$2"' _ 12345 "$(pgrep -c worker)"`],
  ["xargs -a run.pids sh -c 'kill \"$@\"' _", `xargs -a run.pids sh -c 'kill "$@"' _`],
  ["cat run.pids | xargs sh -c 'kill \"$@\"' _", `cat run.pids | xargs sh -c 'kill "$@"' _`],
  ["kill $(cat run.pid)", `kill $(cat run.pid)`],
  ["kill $(< run.pid)", `kill $(< run.pid)`],
  ["kill -9 $PID", `kill -9 $PID`],
  ["kill -9 12345", `kill -9 12345`],
  ["sudo -uroot kill of a literal PID", `sudo -uroot kill 4242`],
  ["sudo --user=root kill of a literal PID", `sudo --user=root kill 4242`],
  ["a recorded PID in bash -c", `PID=$!; bash -c "kill $PID"`],
  ["a PID file piped into xargs kill", `cat run.pid | xargs kill`],
  // N8 counterparts: grouping recorded PID files does not turn them into process-name lookups.
  ["N8: a brace group of recorded PID files feeds xargs kill", `{ cat .local/server.pid; cat .local/worker.pid; } | xargs kill`],
  ["N8: a subshell with a recorded PID file feeds xargs kill", `(cat .local/server.pid) | xargs kill`],
  ["N8: a loop of recorded PID files feeds xargs kill", `for file in .local/server.pid .local/worker.pid; do cat "$file"; done | xargs kill`],
  ["N8: a brace group of recorded PID files feeds read then kill — conservative refusal", `{ cat .local/server.pid; cat .local/worker.pid; } | while read -r p; do kill "$p"; done`],
  ["N8: a subshell with a recorded PID file feeds read then kill — conservative refusal", `(cat .local/server.pid) | while read -r p; do kill "$p"; done`],
  ["N8: a loop of recorded PID files feeds read then kill — conservative refusal", `for file in .local/server.pid .local/worker.pid; do cat "$file"; done | while read -r p; do kill "$p"; done`],
  // F6: the reviewer example and explicit recorded sources after an unrelated diagnostic lookup.
  ["F6: reviewer PID-file loop after diagnostic grep — conservative refusal", `grep -q ready .local/server.log\nwhile read -r p; do kill "$p"; done < .local/server.pid`],
  ["F6: an assigned PID-file redirect after diagnostic grep — conservative refusal", `F=.local/server.pid; grep -q ready .local/server.log; while read -r p; do kill "$p"; done < "$F"`],
  ["F6: an assigned PID file feeds xargs after diagnostic grep", `grep -q ready .local/server.log; F=.local/server.pid; xargs -a "$F" kill`],
  ["F6: a redirected recorded-file cat after diagnostic grep — conservative refusal", `grep -q ready .local/server.log; while read -r p; do kill "$p"; done < <(cat .local/server.pid)`],
  ["F6: a compound recorded source survives cat after diagnostic grep", `grep -q ready .local/server.log; { cat .local/server.pid; } | cat | xargs kill`],
  ["F6: a recorded PID-file pipeline after diagnostic grep", `grep -q ready .local/server.log; cat .local/server.pid | xargs kill`],
  ["F6: a nested recorded PID-file loop after diagnostic grep — conservative refusal", `grep -q ready .local/server.log; for i in once; do while read p; do kill "$p"; done < .local/server.pid; done`],
  ["F6: a recorded PID-file loop inside if after diagnostic grep — conservative refusal", `grep -q ready .local/server.log; if true; then while read p; do kill "$p"; done < .local/server.pid; fi`],
  ["F6: a non-emitting for header preserves recorded PID-file output after diagnostic grep", `grep -q ready .local/server.log; for i in once; do cat .local/server.pid; done | xargs kill`],
  // Paired F7/F8 allowances: recorded PID sources or an operand unrelated to the lookup feed.
  ["F7.01: inline sh inherits recorded PID stdin for xargs kill", `cat .local/server.pid | sh -c 'xargs kill'`],
  ["F7.02: inline sh inherits recorded PID stdin for a read loop — conservative refusal", `cat .local/server.pid | sh -c 'while read -r p; do kill "$p"; done'`],
  ["F7.03: independent recorded inner xargs overrides actual lookup outer stdin", `pgrep -f worker | sh -c 'cat .local/server.pid | xargs kill'`],
  ["F7.04: independent recorded inner read loop overrides actual lookup outer stdin — conservative refusal", `pgrep -f worker | sh -c 'while read -r p; do kill "$p"; done < <(cat .local/server.pid)'`],
  ["F8.01: implicit read destination REPLY receives recorded PID stdin — conservative refusal", `cat .local/server.pid | while read -r; do kill "$REPLY"; done`],
  ["F8.02: read defaults to recorded REPLY after delimiter and timeout options — conservative refusal", `while read -r -d '' -t 1; do kill "$REPLY"; done < <(cat .local/server.pid | tr '\\n' '\\0')`],
  ["F8.03: read prompt option argument REPLY is not a destination", `REPLY=4242; pgrep -f worker | while read -r -p REPLY -n 1 -u 0 -- p; do kill "$REPLY"; done`],
  ["F8.04: read -a array receives recorded PID stdin — conservative refusal", `read -r -d '' -a pids < <(cat .local/server.pid); kill "\${pids[@]}"`],
  ["F8.05: mapfile defaults to recorded MAPFILE after option arguments and -- — conservative refusal", `mapfile -t -n 1 -O 0 -s 0 -C : -c 1 -- < <(cat .local/server.pid); kill "\${MAPFILE[@]}"`],
  ["F8.06: readarray defaults to recorded MAPFILE after delimiter and -- — conservative refusal", `readarray -d '' -t -- < <(cat .local/server.pid | tr '\\n' '\\0'); kill "\${MAPFILE[@]}"`],
  ["F7.05: brace recorded cat producer feeds inline sh xargs kill", `{ cat .local/server.pid; } | sh -c 'xargs kill'`],
  ["F7.06: inherited safe stdin survives inner group cat after a lookup diagnostic", `cat .local/server.pid | sh -c "pgrep x; { cat; } | xargs kill"`],
  ["a pgrep count, then a recorded kill", `N=$(pgrep -c worker); kill "$PID"`],
  ["bash -c \"kill $PID\"", `bash -c "kill $PID"`],
  ["bash -c echo of a lookup", `bash -c 'echo "kill $(pgrep x) is refused"'`],
  ["eval \"kill $PID\"", `eval "kill $PID"`],
  ["ssh HOST kill of a recorded PID file", `ssh HOST "kill $(cat /tmp/w/worker.pid)"`],
  ["a redirection target with date", `: >"$(date +%s).log"`],
  ["a quoted heredoc with a substitution", `cat <<'EOF'\n'$(pkill -f worker)'\nEOF`],
  ["an unquoted heredoc that names pkill", `cat <<EOF\nnever run pkill -f worker ($(date -u +%H:%MZ))\nEOF`],
  ["a leading 2> on pgrep", `2>/dev/null pgrep -f worker`],
  ["sudo --user X kill of a literal PID", `sudo --user paul kill 4242`],
  ["env --unset X grep pkill", `env --unset GH_TOKEN grep -n pkill notes.md`],
  ["command -v pkill", `command -v pkill`],
  ["nohup with redirections", `nohup ./server > server.log 2>&1 &`],
  ["a vitest filter", `./node_modules/.bin/vitest run tests/pattern-kill.test.ts -t pkill`],
];

describe("pattern-kill guard (smarty-dev#774)", () => {
  it.each(refused)("refuses %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { blocked: true, wipe: false, exhausted: false });
  });

  it.each(allowed)("allows %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { blocked: _label.endsWith(" — conservative refusal"), wipe: false, exhausted: false });
  });

  it("names the fix in its reason", () => {
    expect(PATTERN_KILL_REASON).toMatch(/bin\/smarty-reap stop/);
    expect(PATTERN_KILL_REASON).toMatch(/kill <PID>/);
  });
});

// #2275 follow-ups: paired shell-command DATA, never execute these strings.
// N2/N3 literal-name and cwd distinctions are path-only, covered in tmp-wipe.test.ts.
// Keep the original corpus above byte-for-byte; IDs pair the two verdicts below.
describe("pattern-kill follow-up regressions (#2275)", () => {
  const refused: Array<[string, string]> = [
    ["F9.01: saved lookup feeds stdin/read", `pgrep -f worker > .local/selected.pids; while read -r p; do kill "$p"; done < .local/selected.pids`],
    ["F9.02: saved lookup feeds xargs -a", `pgrep -f worker > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.03: saved lookup feeds inline --arg-file", `pgrep -f worker > .local/selected.pids; xargs --arg-file=.local/selected.pids kill`],
    ["F9.04: explicit cat of saved lookup cannot clear provenance", `pgrep -f worker > .local/selected.pids; cat .local/selected.pids | xargs kill`],
    ["F9.05: resolved aliases identify the same saved lookup", `F=.local/selected.pids; G="$F"; pgrep -f worker > "$F"; xargs -a "$G" kill`],
    ["F9.06: append lookup output retains unsafe file provenance", `cat .local/server.pid > .local/selected.pids; pgrep -f worker >> .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.07: explicit fd1 writes actual lookup stdout", `pgrep -f worker 1> .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.08: stdout file differs from an unrelated fd3 file", `pgrep -f worker > .local/selected.pids 3> .local/diagnostic.log; xargs -a .local/selected.pids kill`],
    ["F9.09: brace producer saves lookup output", `{ pgrep -f worker; } > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.10: loop producer saves lookup output", `for name in worker server; do pgrep -f "$name"; done > .local/selected.pids; cat .local/selected.pids | xargs kill`],
    ["F9.11: subshell producer saves lookup output", `(pgrep -f worker) > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.12: lookup stdout actually reaches the pipe", `pgrep -f worker | cat | xargs kill`],
    ["F9.13: captured cat reads saved lookup output", `pgrep -f worker > .local/selected.pids; kill $(cat .local/selected.pids)`],
    ["F9.14: unsafe overwrite replaces recorded output", `cat .local/server.pid > .local/selected.pids; pgrep -f worker > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.15: unrelated recorded diagnostic does not clear saved lookup", `pgrep -f worker > .local/selected.pids; cat .local/server.pid; xargs -a .local/selected.pids kill`],
    ["F9.16: exact native empty-producer regression retains lookup provenance", `grep __guard_followups_no_match__ /dev/null > pid.list; cat pid.list | xargs -r kill; printf 'MAIN_F9_KILL_EXECUTED\\n'`],
    ["F10.01: operand-free captured cat consumes inherited lookup stdin", `pgrep -f worker | sh -c 'kill $(cat)'`],
    ["F10.02: captured stdin consumer pipeline preserves lookup output", `pgrep -f worker | sh -c 'kill $(cat | tr "\\n" " ")'`],
    ["F10.03: cat dash explicitly consumes inherited lookup stdin", `pgrep -f worker | sh -c 'kill $(cat -)'`],
    ["F10.04: inner lookup taints capture with recorded inherited stdin", `cat .local/server.pid | sh -c 'kill $(pgrep -f worker)'`],
    ["F10.05: captured lookup versus independent nonconsumer", `pgrep -f worker | sh -c 'kill $(pgrep -f worker)'`],
    ["F10.06: inherited lookup stdin reaches a nested local child capture", `pgrep -f worker | sh -c 'bash -c "kill \\$(cat)"'`],
    ["F10.07: capture binding is unsafe inside its consuming child", `P=4242; pgrep -f worker | sh -c 'P=$(cat); kill "$P"'`],
    ["F10.08: fd3 input does not replace inherited lookup stdin for capture", `pgrep -f worker | sh -c 'kill $(cat)' 3< .local/server.pid`],
    ["F10.09: captured head consumes inherited lookup stdin", `pgrep -f worker | sh -c 'kill $(head -n 1)'`],
    ["F10.10: capture assignment preserves inherited lookup stdin", `pgrep -f worker | sh -c 'P=$(cat); kill "$P"'`],
    ["F10.11: exact native empty-producer inherited capture regression", `grep __guard_followups_no_match__ /dev/null | bash -c 'kill $(cat)'; printf 'MAIN_F10_KILL_EXECUTED\\n'`],
    ["N4.01: set binds captured lookup to the first positional", `set -- $(pgrep -f worker); kill "$1"`],
    ["N4.02: set binds captured lookup to aggregate positionals", `set -- 4242 $(pgrep -f worker); kill "$@"`],
    ["N4.03: read here-string binds executed lookup output", `read -r P <<< "$(pgrep -f worker)"; kill "$P"`],
    ["N4.04: mapfile here-string binds executed lookup output", `mapfile -t pids <<< "$(pgrep -f worker)"; kill "\${pids[@]}"`],
    ["N4.05: printf -v binds executed lookup output", `printf -v P '%s' "$(pgrep -f worker)"; kill "$P"`],
    ["N4.06: printf -v preserves lookup provenance through a variable", `P=$(pgrep -f worker); printf -v Q '%s' "$P"; kill "$Q"`],
    ["N4.07: set preserves lookup provenance through a variable", `P=$(pgrep -f worker); set -- "$P"; kill "$1"`],
    ["N6.01: array append preserves unsafe original lookup elements", `pids=($(pgrep -f worker)); pids+=(4242); kill "\${pids[@]}"`],
    ["N6.02: array append preserves unsafe captured elements after recorded append", `pids=($(pgrep -f worker)); pids+=($(cat .local/server.pid)); kill "\${pids[@]}"`],
    ["N6.03: array append adds lookup elements to a recorded array", `pids=(4242); pids+=($(pgrep -f worker)); kill "\${pids[@]}"`],
    ["N6.04: array append through a recorded variable retains old lookup elements", `PID=$!; pids=($(pgrep -f worker)); pids+=("$PID"); kill "\${pids[@]}"`],
  ];
  const allowed: Array<[string, string]> = [
    ["F9.01: saved recorded PID file feeds stdin/read — conservative refusal", `cat .local/server.pid > .local/selected.pids; while read -r p; do kill "$p"; done < .local/selected.pids`],
    ["F9.02: saved recorded PID file feeds xargs -a", `cat .local/server.pid > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.03: saved recorded PID file feeds inline --arg-file", `cat .local/server.pid > .local/selected.pids; xargs --arg-file=.local/selected.pids kill`],
    ["F9.04: explicit cat of saved recorded output remains independent", `cat .local/server.pid > .local/selected.pids; cat .local/selected.pids | xargs kill`],
    ["F9.05: resolved aliases identify the same saved recorded output", `F=.local/selected.pids; G="$F"; cat .local/server.pid > "$F"; xargs -a "$G" kill`],
    ["F9.06: append recorded output to a recorded file remains safe", `cat .local/server.pid > .local/selected.pids; cat .local/worker.pid >> .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.07: fd2 does not save lookup stdout as file contents", `pgrep -f worker 2> .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.08: fd3 output does not contaminate a preexisting recorded file", `pgrep -f worker 3> .local/diagnostic.log; xargs -a .local/server.pid kill`],
    ["F9.09: brace producer saves recorded output", `{ cat .local/server.pid; } > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.10: loop producer saves recorded output", `for file in .local/server.pid .local/worker.pid; do cat "$file"; done > .local/selected.pids; cat .local/selected.pids | xargs kill`],
    ["F9.11: subshell producer saves recorded output", `(cat .local/server.pid) > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.12: redirected lookup stdout does not reach the outgoing pipe", `pgrep -f worker > .local/selected.pids | cat | xargs kill`],
    ["F9.13: captured cat reads saved recorded output", `cat .local/server.pid > .local/selected.pids; kill $(cat .local/selected.pids)`],
    ["F9.14: recorded overwrite replaces unsafe output", `pgrep -f worker > .local/selected.pids; cat .local/server.pid > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.15: unrelated lookup diagnostic does not taint saved recorded output", `pgrep -f worker; cat .local/server.pid > .local/selected.pids; xargs -a .local/selected.pids kill`],
    ["F9.16: recorded-file empty-producer counterpart remains allowed", `cat .local/server.pid > pid.list; cat pid.list | xargs -r kill; printf 'MAIN_F9_KILL_EXECUTED\\n'`],
    ["F10.01: operand-free captured cat consumes inherited recorded stdin", `cat .local/server.pid | sh -c 'kill $(cat)'`],
    ["F10.02: captured stdin consumer pipeline preserves recorded output — conservative refusal", `cat .local/server.pid | sh -c 'kill $(cat | tr "\\n" " ")'`],
    ["F10.03: explicit PID-file cat ignores inherited lookup stdin", `pgrep -f worker | sh -c 'kill $(cat .local/server.pid)'`],
    ["F10.04: inner recorded source remains independent of recorded inherited stdin", `cat .local/server.pid | sh -c 'kill $(cat .local/worker.pid)'`],
    ["F10.05: nonconsumer capture ignores inherited lookup stdin", `pgrep -f worker | sh -c 'kill $(printf "%s" 4242)'`],
    ["F10.06: inherited recorded stdin reaches a nested local child capture", `cat .local/server.pid | sh -c 'bash -c "kill \\$(cat)"'`],
    ["F10.07: child capture binding does not leak to the parent", `P=4242; pgrep -f worker | sh -c 'P=$(cat); :'; kill "$P"`],
    ["F10.08: fd0 recorded input overrides inherited lookup stdin for capture", `pgrep -f worker | sh -c 'kill $(cat)' < .local/server.pid`],
    ["F10.09: explicit recorded head source ignores inherited lookup stdin — conservative refusal", `pgrep -f worker | sh -c 'kill $(head -n 1 .local/server.pid)'`],
    ["F10.10: capture assignment preserves inherited recorded stdin", `cat .local/server.pid | sh -c 'P=$(cat); kill "$P"'`],
    ["F10.11: recorded-file inherited capture counterpart remains allowed", `cat .local/server.pid | bash -c 'kill $(cat)'; printf 'MAIN_F10_KILL_EXECUTED\\n'`],
    ["N4.01: set binds a recorded literal PID to the first positional", `set -- 4242; kill "$1"`],
    ["N4.02: set binds only recorded literal aggregate positionals", `set -- 4242 4243; kill "$@"`],
    ["N4.03: read literal here-string lookup text is inert — conservative refusal", `read -r P <<< '$(pgrep -f worker)'; kill "$P"`],
    ["N4.04: mapfile literal here-string lookup text is inert — conservative refusal", `mapfile -t pids <<< '$(pgrep -f worker)'; kill "\${pids[@]}"`],
    ["N4.05: printf -v binds a recorded literal PID", `printf -v P '%s' 4242; kill "$P"`],
    ["N4.06: printf -v preserves recorded provenance through a variable", `PID=$!; printf -v Q '%s' "$PID"; kill "$Q"`],
    ["N4.07: set preserves recorded provenance through a variable", `PID=$!; set -- "$PID"; kill "$1"`],
    ["N6.01: array append preserves recorded original PID elements", `pids=(4242); pids+=(4243); kill "\${pids[@]}"`],
    ["N6.02: array append preserves recorded captured elements", `pids=($(cat .local/server.pid)); pids+=($(cat .local/worker.pid)); kill "\${pids[@]}"`],
    ["N6.03: array append adds recorded elements to a recorded array", `pids=(4242); pids+=($(cat .local/server.pid)); kill "\${pids[@]}"`],
    ["N6.04: array append through a recorded variable keeps a recorded array safe", `PID=$!; pids=(4242); pids+=("$PID"); kill "\${pids[@]}"`],
  ];

  it.each(refused)("refuses %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { blocked: true, wipe: false, exhausted: false });
  });
  it.each(allowed)("allows %s", (_label, command) => {
    expectRound5Guard(command, scanCommand(command), { blocked: _label.endsWith(" — conservative refusal"), wipe: false, exhausted: false });
  });
});
