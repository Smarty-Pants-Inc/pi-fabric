import { expect, it } from "vitest";
import { bashGuardRefusal, DELETE_REASON, SIGNAL_REASON } from "../src/core/literal-bash-guard.js";

// Exact historical F1–F10 witnesses, with authoritative labels in pattern-kill/tmp-wipe tests.
// F38/F39 below are explicitly DERIVED class witnesses: those reviews supplied prose, not code.
// Every string is scanner DATA only; no shell, file contents or historical analyzer is executed.
const witnesses = [
  ["F1 / tmp-wipe.test.ts:133 process-substitution listing", `while read -r d; do rm -rf "$d"; done < <(find /tmp -maxdepth 1 -name 'tmp.*' -mmin -60)`],
  ["F2 / tmp-wipe.test.ts:138 array", `dirs=(/tmp/tmp.*); rm -rf "\${dirs[@]}"`],
  ["F3 / tmp-wipe.test.ts:143 cwd listing", `cd /tmp && ls -t | head -6 | xargs rm -rf`],
  ["F4 / tmp-wipe.test.ts:151 assigned root redirect", `T=/tmp; xargs -0 rm -rf < <(find "$T" -maxdepth 1 -name 'tmp.*' -print0)`],
  ["F5 / tmp-wipe.test.ts:158 compound producer", `{ ls -d /tmp/tmp.*; ls -d /var/tmp/tmp.*; } | xargs rm -rf`],
  ["F6 / tmp-wipe.test.ts:164 intermediate consumer", `T=/tmp; while read -r d; do rm -rf "$d"; done < <(find "$T" -maxdepth 1 -name 'tmp.*' | cat)`],
  ["F7 / tmp-wipe.test.ts:173 inherited shell input", `ls -d /tmp/tmp.* | sh -c 'xargs rm -rf'`],
  ["F8 / tmp-wipe.test.ts:177 implicit read destination", `ls -d /tmp/tmp.* | while read -r; do rm -rf "$REPLY"; done`],
  ["F9 / pattern-kill.test.ts:345 saved lookup", `pgrep -f worker > .local/selected.pids; while read -r p; do kill "$p"; done < .local/selected.pids`],
  ["F10 / pattern-kill.test.ts:361 consuming capture", `pgrep -f worker | sh -c 'kill $(cat)'`],
  ["F38 DERIVED / security r12: heredoc overrides recorded pipe", `cat .local/recorded.pids | xargs kill <<EOF\n$(pgrep worker)\nEOF`],
  ["F39 DERIVED / security r12: visible tee writes then consumes", `pgrep worker | tee .local/selected.pids; xargs -a .local/selected.pids kill`],
] as const;
it.each(witnesses)("refuses historical class: %s", (_id, command) => {
  expect([SIGNAL_REASON, DELETE_REASON]).toContain(bashGuardRefusal(command, "/tmp/session-literal-guard"));
  expect(bashGuardRefusal("kill 4242", "/tmp/session-literal-guard")).toBeUndefined();
  expect(bashGuardRefusal("rm -rf /tmp/session-literal-guard/owned", "/tmp/session-literal-guard")).toBeUndefined();
});
