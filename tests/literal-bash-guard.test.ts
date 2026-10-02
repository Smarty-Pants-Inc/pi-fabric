import { describe, expect, it } from "vitest";
import { bashGuardRefusal, DELETE_REASON, SIGNAL_REASON } from "../src/core/literal-bash-guard.js";

const tmpdir = "/tmp/session-literal-guard";
describe("literal-only signal and recursive-delete guard", () => {
  const signals = [
    "pkill -f worker", "killall worker", "/usr/bin/pkill worker", "killall5",
    "kill $(pgrep worker)", "kill `pgrep worker`", "kill $P", 'kill "$P"',
    "P=4242; kill $P", "kill %1", "kill worker", "kill -l", "kill 0", "kill -TERM -1",
    "kill -s TERM 4242", "kill 4242 worker", "kill 42*", "kill {42,43}",
    "pgrep worker | xargs kill", "xargs kill 4242", "sudo kill 4242",
    "sh -c 'kill 4242'", "ssh host 'pkill worker'", "env -S 'kill 4242'",
    "pki\\\nll worker", '"ki"ll $P', "\\kill $P", "kill 4242 > /outside",
    "kill 4242 && printf ok", "kill\r 4242", "kill '4242", "kill $'42'",
  ];
  const deletes = [
    "rm -rf /", "rm -r /tmp", "rm -R /var/tmp", "rm --recursive /outside",
    "rm -rf $D", 'rm -rf "$D"', "D=/own; rm -rf $D", "rm -rf $(printf /own)",
    "rm -rf /tmp/session-literal-guard/../other", "rm -rf /tmp/session-literal-guard/*",
    "rm -rf '/tmp/session-literal-guard/*'", "rm -rf /tmp/session-literal-guard2/a",
    "rm -rf /tmp/session-literal-guard/a /outside", "rm -rf .local/owned",
    "rm -rf /tmp/session-literal-guard", "rm -rf /tmp/session-literal-guard/",
    "rm -rf /tmp/session-literal-guard/a > /outside", "sudo rm -rf /tmp/session-literal-guard/a",
    "find /tmp/session-literal-guard -delete", "shred -r /tmp/session-literal-guard/a",
    "shred --remove /tmp/session-literal-guard/a", "sh -c 'rm -rf /outside'",
    "rm $OPTS /outside", 'rm "$OPTS" /outside', "rm -r -- /tmp/session-literal-guard/$D",
    "rm -ri /tmp/session-literal-guard/a", "TMPDIR=/outside rm -rf /outside/a",
    'env --split-string="rm -rf /outside"', "env -iS'rm -rf /outside'", 'env -S "rm $OPTS /outside"',
  ];
  it.each(signals)("refuses signal class: %s", command => expect(bashGuardRefusal(command, tmpdir)).toBe(SIGNAL_REASON));
  it.each(deletes)("refuses delete class: %s", command => expect(bashGuardRefusal(command, tmpdir)).toBe(DELETE_REASON));
  it.each([
    "kill 4242", "kill -TERM 4242", "kill -9 4242 7777", "kill -00 4242", "kill -- 4242", "kill '4242'",
    "rm -r /tmp/session-literal-guard/a", "rm -rf /tmp/session-literal-guard/a /tmp/session-literal-guard/b",
    "rm -fr /tmp/session-literal-guard/a", "rm -R /tmp/session-literal-guard/a",
    "rm --recursive --force -- /tmp/session-literal-guard/a", "rm -rf '/tmp/session-literal-guard/with space'",
    'rm -rf "/tmp/session-literal-guard/with space"',
    "rm -r /tmp/session-literal-guard/./a", "rm -r /tmp/session-literal-guard//a",
  ])("allows a literal supported form: %s", command => expect(bashGuardRefusal(command, tmpdir)).toBeUndefined());
  it.each([
    "printf '%s' 'pkill worker; rm -rf /tmp'", "echo kill", 'echo "never use pkill"',
    "grep -n pkill README.md", "rg 'rm -rf' docs", "pgrep worker", "ls -al /tmp", "pwd",
    "printf '%s' '$P'", "rm -f /tmp/a", "rm /tmp/a", "find /tmp -type f", "shred /tmp/a",
    "echo ok; git status", "P=4242; printf '%s' $P",
  ])("passes unrelated commands and inert literal DATA: %s", command => {
    // Same historical ID/command; owner-directed round-4 cut refuses all shred forms.
    expect(bashGuardRefusal(command, tmpdir)).toBe(command === "shred /tmp/a" ? DELETE_REASON : undefined);
  });
  it.each([undefined, "", "/", "/tmp", "/var/tmp", "relative", "/tmp/session/../other", "$TMPDIR"])(
    "never grants recursive deletion from an absent/shared/unproved TMPDIR: %s", root => {
      expect(bashGuardRefusal("rm -rf /tmp/session-literal-guard/a", root)).toBe(DELETE_REASON);
      expect(bashGuardRefusal("kill 4242", root)).toBeUndefined();
    },
  );
  it("reads TMPDIR per invocation, never from a command assignment", () => {
    const command = "rm -r /tmp/session-literal-guard/a";
    expect(bashGuardRefusal(command, tmpdir)).toBeUndefined();
    expect(bashGuardRefusal(command, "/tmp/other-session")).toBe(DELETE_REASON);
    expect(bashGuardRefusal("TMPDIR=/tmp/session-literal-guard rm -r /tmp/session-literal-guard/a", "/tmp/other-session")).toBe(DELETE_REASON);
  });
});
