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

describe("restored guard fail-closed provenance (#325 S4-S6)", () => {
  it.each([
    ["S6 encoded signal head", String.raw`$'\x70kill' -f X`, killsByPattern],
    ["S6 encoded delete head", String.raw`$'r\x6d' -rf /tmp/*`, wipesTmp],
    ["S6 encoded executor head", String.raw`$'b\x61sh' -c 'pkill -f X'`, killsByPattern],
    ["S6 encoded wrapper head", String.raw`$'e\x6ev' sh -c 'pkill -f X'`, killsByPattern],
    ["S6 encoded wrapped receiver", String.raw`sudo timeout 5 $'\x70kill' -f X`, killsByPattern],
    ["S6 encoded shared-root operand", String.raw`rm -rf $'/t\x6dp'`, wipesTmp],
    ["S6 encoded deletion option", String.raw`rm $'\x2drf' /tmp/*`, wipesTmp],
    ["S6 encoded signal selector", String.raw`kill $'\x34\x32\x34\x32'`, killsByPattern],
    ["S6 encoded find root", String.raw`find $'/t\x6dp' -delete`, wipesTmp],
    ["S6 encoded shell execution option", String.raw`sh $'\x2dc' 'pkill -f X'`, killsByPattern],
    ["S6 encoded positional protected path", String.raw`sh -c 'rm -rf "$1"' _ $'/t\x6dp'`, wipesTmp],
    ["S6 encoded assigned protected path", String.raw`T=$'/t\x6dp'; rm -rf "$T"`, wipesTmp],
    ["S4 lookup argument file", `pgrep -f worker > .local/targets; xargs -a .local/targets kill`, killsByPattern],
    ["S4 lookup appended argument file", `pgrep -f worker >> .local/targets; xargs --arg-file=.local/targets kill`, killsByPattern],
    ["S4 lookup read loop", `pgrep -f worker > .local/targets; while read p; do kill "$p"; done < .local/targets`, killsByPattern],
    ["S4 lookup cat pipeline", `pgrep -f worker > .local/targets; cat .local/targets | xargs kill`, killsByPattern],
    ["S4 lookup cat capture", `pgrep -f worker > .local/targets; kill "$(cat .local/targets)"`, killsByPattern],
    ["S4 resolved path aliases", `F=.local/targets; pgrep -f worker > "$F"; G="$F"; xargs -a "$G" kill`, killsByPattern],
    ["S4 intermediary output", `pgrep -f worker | cat > .local/targets; xargs -a .local/targets kill`, killsByPattern],
    ["S4 inline producer", `sh -c 'pgrep -f worker > .local/targets'; xargs -a .local/targets kill`, killsByPattern],
    ["S4 shared listing argument file", `ls -d /tmp/tmp.* > .local/targets; xargs -a .local/targets rm -rf`, wipesTmp],
    ["S4 shared listing read loop", `find /tmp -maxdepth 1 > .local/targets; while read d; do rm -rf "$d"; done < .local/targets`, wipesTmp],
    ["S4 shared listing cat pipeline", `ls -d /tmp/tmp.* > .local/targets; cat .local/targets | xargs rm -rf`, wipesTmp],
    ["S4 shared listing cat capture", `ls -d /tmp/tmp.* > .local/targets; rm -rf "$(cat .local/targets)"`, wipesTmp],
    ["S4 shared cwd listing output", `cd /tmp; ls > /home/paul/list; xargs -a /home/paul/list rm -rf`, wipesTmp],
    ["S4 numeric output filename", `pgrep -f worker > 1; xargs -a 1 kill`, killsByPattern],
    ["S4 file descriptor combined write", `pgrep -f worker >& .local/targets; xargs -a .local/targets kill`, killsByPattern],
    ["S4 spelling aliases", `pgrep -f worker > ./.local//targets; xargs -a .local/targets kill`, killsByPattern],
    ["S4 compound output", `{ pgrep -f worker; } > .local/targets; xargs -a .local/targets kill`, killsByPattern],
    ["S4 substitution input shortcut", `pgrep -f worker > .local/targets; kill "$(< .local/targets)"`, killsByPattern],
    ["S5 lookup captured stdin", `pgrep -f worker | sh -c 'p=$(cat); kill "$p"'`, killsByPattern],
    ["S5 lookup captured stdin transform", `pgrep -f worker | sh -c 'p=$(head -n 1); kill "$p"'`, killsByPattern],
    ["S5 lookup captured read", `pgrep -f worker | sh -c 'p=$(read p; printf "%s" "$p"); kill "$p"'`, killsByPattern],
    ["S5 shared captured stdin", `ls -d /tmp/tmp.* | sh -c 'd=$(cat); rm -rf "$d"'`, wipesTmp],
    ["S5 shared captured stdin transform", `ls -d /tmp/tmp.* | sh -c 'd=$(head -n 1); rm -rf "$d"'`, wipesTmp],
    ["S5 shared captured read", `ls -d /tmp/tmp.* | sh -c 'd=$(read d; printf "%s" "$d"); rm -rf "$d"'`, wipesTmp],
  ] as const)("refuses %s", (_label, command, guard) => {
    expect(guard(command)).toBe(true);
  });

  it.each([
    ["S6 inert printf field", String.raw`printf '%s' $'\x70kill\t-f X'`],
    ["S6 inert echo field", String.raw`echo $'r\x6d -rf /tmp/*'`],
    ["S6 inert grep field", String.raw`grep $'r\x6d -rf /tmp/*' file`],
    ["S6 ordinary literal PID", `sudo kill 4242`],
    ["S6 ordinary exact path", `rm -rf /tmp/tmp.AbC123`],
    ["S4 preexisting argument PID file", `pgrep -f worker > .local/diagnostic; xargs -a .local/targets kill`],
    ["S4 preexisting read PID file", `pgrep -f worker > .local/diagnostic; while read p; do kill "$p"; done < .local/targets`],
    ["S4 preexisting cat PID file", `pgrep -f worker > .local/diagnostic; cat .local/targets | xargs kill`],
    ["S4 preexisting capture PID file", `pgrep -f worker > .local/diagnostic; kill "$(cat .local/targets)"`],
    ["S4 preexisting aliased PID file", `F=.local/targets; pgrep -f worker > .local/diagnostic; G="$F"; xargs -a "$G" kill`],
    ["S4 preexisting owned argument file", `ls -d /tmp/tmp.* > .local/diagnostic; xargs -a .local/targets rm -rf`],
    ["S4 preexisting owned read file", `ls -d /tmp/tmp.* > .local/diagnostic; while read d; do rm -rf "$d"; done < .local/targets`],
    ["S4 preexisting owned cat file", `ls -d /tmp/tmp.* > .local/diagnostic; cat .local/targets | xargs rm -rf`],
    ["S4 preexisting owned capture file", `ls -d /tmp/tmp.* > .local/diagnostic; rm -rf "$(cat .local/targets)"`],
    ["S4 numeric fd is not a written filename", `pgrep -f worker > .local/diagnostic 2>&1; xargs -a 1 kill`],
    ["S4 non-stdin input is not a written filename", `pgrep -f worker 3< .local/targets; xargs -a .local/targets kill`],
    ["S4 descriptor closure is not a written filename", `pgrep -f worker 2>&-; cat ./- | xargs kill`],
    ["S4 literal PID despite visible output", `pgrep -f worker > .local/targets; kill 4242`],
    ["S4 exact path despite visible output", `ls -d /tmp/tmp.* > .local/targets; rm -rf /tmp/tmp.AbC123`],
    ["S5 captured recorded stdin", `cat .local/server.pid | sh -c 'p=$(cat); kill "$p"'`],
    ["S5 captured independent PID file", `pgrep -f worker | sh -c 'p=$(cat .local/server.pid); kill "$p"'`],
    ["S5 captured independent PID redirect", `pgrep -f worker | sh -c 'p=$(cat < .local/server.pid); kill "$p"'`],
    ["S5 non-consuming signal capture", `pgrep -f worker | sh -c 'p=$(printf 4242); kill "$p"'`],
    ["S5 non-consuming literal signal", `pgrep -f worker | sh -c 'kill 4242'`],
    ["S5 captured owned stdin", `cat .local/mine.list | sh -c 'd=$(cat); rm -rf "$d"'`],
    ["S5 captured independent owned file", `ls -d /tmp/tmp.* | sh -c 'd=$(cat .local/mine.list); rm -rf "$d"'`],
    ["S5 captured independent owned redirect", `ls -d /tmp/tmp.* | sh -c 'd=$(cat < .local/mine.list); rm -rf "$d"'`],
    ["S5 non-consuming delete capture", `ls -d /tmp/tmp.* | sh -c 'd=$(printf /tmp/tmp.AbC123); rm -rf "$d"'`],
    ["S5 non-consuming exact delete", `ls -d /tmp/tmp.* | sh -c 'rm -rf /tmp/tmp.AbC123'`],
    ["S5 non-consuming mktemp after lookup", `pgrep -f worker | sh -c 'D=$(mktemp -d); rm -rf "$D"'`],
    ["S5 non-consuming mktemp after listing", `ls -d /tmp/tmp.* | sh -c 'D=$(mktemp -d); rm -rf "$D"'`],
  ] as const)("allows %s", (_label, command) => {
    expect(killsByPattern(command)).toBe(false);
    expect(wipesTmp(command)).toBe(false);
  });
  it("S4 does not retain output-file facts across tool calls", () => {
    expect(killsByPattern(`pgrep -f worker > .local/targets`)).toBe(false);
    expect(killsByPattern(`xargs -a .local/targets kill`)).toBe(false);
    expect(wipesTmp(`ls -d /tmp/tmp.* > .local/targets`)).toBe(false);
    expect(wipesTmp(`xargs -a .local/targets rm -rf`)).toBe(false);
  });
});

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
