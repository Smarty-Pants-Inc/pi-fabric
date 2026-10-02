import { describe, expect, it } from "vitest";
import { killsByPattern, wipesTmp } from "../src/core/pattern-kill.js";

// Scanner DATA only: never execute these commands or create destructive fixtures.
const signalActions = [
  ["EXIT trap", `trap 'pkill -f X' EXIT`],
  ["signal trap", `trap -- 'pkill -f X' TERM`],
  ["completion command", `complete -C 'pkill -f X' demo`],
  ["completion attached command", `complete -Cpkill demo`],
  ["completion clustered command", `complete -rC 'pkill -f X' demo`],
  ["completion function selector", `complete -F pkill demo`],
  ["completion word-list substitution", `complete -W '$(pkill -f X)' demo`],
  ["compgen command", `compgen -C 'pkill -f X'`],
  ["compgen word-list substitution", `compgen -W '$(pkill -f X)'`],
  ["quoted printf subscript", `printf -v 'a[$(pkill -f X)]' '%s' text`],
  ["printf backtick subscript", "printf -v 'a[`pkill -f X`]' '%s' text"],
  ["rg preprocessor", `rg --pre pkill X file`],
  ["rg inline preprocessor", `rg --pre=/usr/bin/pkill X file`],
  ["rg hostname selector", `rg --hostname-bin /usr/bin/killall5 X file`],
  ["rg inline hostname selector", `rg --hostname-bin=killall5 X file`],
  ["simple coproc", `coproc pkill -f X`],
  ["named coproc", `coproc JOB { pkill -f X; }`],
] as const;
const deletionActions = [
  ["EXIT trap", `trap 'rm -rf /tmp/*' EXIT`],
  ["completion command", `complete -C 'rm -rf /tmp/*' demo`],
  ["completion word-list substitution", `complete -W '$(rm -rf /tmp/*)' demo`],
  ["compgen command", `compgen -C 'rm -rf /tmp/*'`],
  ["compgen word-list substitution", `compgen -W '$(rm -rf /tmp/*)'`],
  ["quoted printf subscript", `printf -v 'a[$(rm -rf /tmp/*)]' '%s' text`],
  ["rg preprocessor", `rg --pre 'rm -rf /tmp/*' X file`],
  ["simple coproc", `coproc rm -rf /tmp/*`],
  ["named coproc", `coproc JOB { rm -rf /tmp/*; }`],
] as const;
const controls = [
  ["printing EXIT trap", `trap 'echo pkill is refused' EXIT`],
  ["recorded-PID EXIT trap", `trap 'kill 4242' EXIT`],
  ["trap query", `trap -p pkill`],
  ["printing completion command", `complete -C 'echo pkill' demo`],
  ["recorded-PID completion command", `complete -C 'kill 4242' demo`],
  ["ordinary completion function", `complete -F _demo demo`],
  ["ordinary completion word list", `complete -W 'one two' demo`],
  ["printing compgen command", `compgen -C 'echo pkill'`],
  ["ordinary compgen word list", `compgen -W 'one two'`],
  ["printing word-list substitution", `compgen -W '$(echo pkill)'`],
  ["printf without -v", `printf '%s' 'a[$(pkill -f X)]'`],
  ["ordinary printf destination", `printf -v a '%s' pkill`],
  ["harmless printf subscript", `printf -v 'a[$(echo 1)]' '%s' text`],
  ["rg without executable selector", `rg pkill file`],
  ["rg pattern resembling a selector", `rg -- '--pre=pkill' file`],
  ["ordinary rg preprocessor", `rg --pre /usr/bin/cat X file`],
  ["ordinary rg hostname selector", `rg --hostname-bin hostname X file`],
  ["printing coproc", `coproc echo pkill`],
  ["recorded-PID coproc", `coproc kill 4242`],
  ["named printing coproc", `coproc JOB { echo pkill; }`],
  ["ordinary ANSI-C field", String.raw`printf '%s' $'pkill\t-f X'`],
  ["ANSI-C field to another command", String.raw`rg $'rm\t-rf /tmp/*' file`],
  ["unescaped ANSI-C script printing", `bash -c $'echo pkill'`],
  ["ordinary inline recorded PID", `bash -c 'kill 4242'`],
] as const;
function chain(body: string, depth: number): string {
  for (let i = 0; i < depth; i++) body = `echo $(${body})`;
  return body;
}

describe("restored guard unexamined execution (#325 S1-S3)", () => {
  it.each(signalActions)("S1 refuses signal: %s", (_label, command) => {
    expect(killsByPattern(command)).toBe(true);
  });
  it.each(deletionActions)("S1 refuses shared deletion: %s", (_label, command) => {
    expect(wipesTmp(command)).toBe(true);
  });
  it.each(controls)("allows inert/recorded control: %s", (_label, command) => {
    expect(killsByPattern(command)).toBe(false);
    expect(wipesTmp(command)).toBe(false);
  });
  it.each([
    ["tab signal", String.raw`bash -c $'pkill\t-f X'`, killsByPattern],
    ["tab shared deletion", String.raw`bash -c $'rm\t-rf /tmp/*'`, wipesTmp],
    ["hex signal", String.raw`bash -c $'pkill\x09-f X'`, killsByPattern],
    ["octal shared deletion", String.raw`bash -c $'rm\011-rf /tmp/*'`, wipesTmp],
    ["escaped apostrophe before signal", String.raw`bash -c $'echo \'x\'; pkill\t-f X'`, killsByPattern],
    ["eval escaped script", String.raw`eval $'pkill\t-f X'`, killsByPattern],
    ["env split escaped script", String.raw`env -S $'pkill\t-f X'`, killsByPattern],
    ["ssh escaped script", String.raw`ssh HOST $'pkill\t-f X'`, killsByPattern],
    ["trap escaped script", String.raw`trap $'pkill\t-f X' EXIT`, killsByPattern],
  ] as const)("S2 refuses unproved ANSI-C execution: %s", (_label, command, guard) => {
    expect(guard(command)).toBe(true);
  });
  it("S3 refuses a seven-deep protected signal", () => {
    expect(killsByPattern(chain("pkill -f X", 7))).toBe(true);
  });
  it("S3 refuses a seven-deep shared deletion", () => {
    expect(wipesTmp(chain("rm -rf /tmp/*", 7))).toBe(true);
  });
  it("S3 conservatively refuses nonempty unexamined harmless text", () => {
    expect(killsByPattern(chain("echo safe", 7))).toBe(true);
    expect(wipesTmp(chain("echo safe", 7))).toBe(true);
  });
  it("S3 permits a fully examined six-deep harmless chain", () => {
    expect(killsByPattern(chain("echo safe", 6))).toBe(false);
    expect(wipesTmp(chain("echo safe", 6))).toBe(false);
  });
  it("S3 does not refuse an empty unexamined tail", () => {
    expect(killsByPattern(chain("", 7))).toBe(false);
    expect(wipesTmp(chain("", 7))).toBe(false);
  });
});
