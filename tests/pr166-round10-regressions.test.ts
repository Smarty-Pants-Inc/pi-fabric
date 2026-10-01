import { describe, expect, it } from 'vitest';
import { scanCommand, killsByPattern, wipesTmp } from '../src/core/pattern-kill.js';

// Every string is scanner DATA ONLY. No shell execution or native effects.
const SAFE = { blocked: false, wipe: false, exhausted: false };
const STATE = { ...SAFE, shellState: true };
const PID = { ...SAFE, blocked: true };
const TMP = { ...SAFE, wipe: true };
type Expected = typeof SAFE | typeof STATE;
function check(command: string, expected: Expected): void {
  const actual = scanCommand(command);
  expect(actual, command).toStrictEqual(expected);
  expect(Object.keys(actual).sort(), command).toStrictEqual(Object.keys(expected).sort());
  expect(actual.exhausted, command).toBe(false);
  expect(killsByPattern(command), command).toBe(expected.blocked || 'shellState' in expected);
  expect(wipesTmp(command), command).toBe(expected.wipe || 'shellState' in expected);
}
const quoted = ["'2'", '"2"', String.raw`\2`, '2""', "''2"];
describe('PR166 round10 lexical IO_NUMBER and physical CR', () => {
  quoted.forEach((number, index) => {
    it(`R0${index + 1} refuses quoted or escaped numeric argv saving lookup and shared-root stdout`, () => {
      check(`pgrep -f ${number}> .local/quoted-fd.pids; kill $(cat .local/quoted-fd.pids)`, PID);
      check(`printf '%s\n' /tmp ${number}> .local/quoted-fd.paths; xargs -a .local/quoted-fd.paths rm -rf`, TMP);
    });
    it(`SIMPLE-A0${index + 1} preserves proved numeric descriptors and independent literal output controls`, () => {
      check("pgrep -f worker 2> .local/stderr.pids; kill $(cat .local/stderr.pids)", SAFE);
      check("printf '%s\n' /tmp 2> .local/stderr.paths; xargs -a .local/stderr.paths rm -rf", SAFE);
      check(`printf '%s\n' 4242 ${number}> .local/owned.pids; kill 4242`, SAFE);
      check(`printf '%s\n' /own ${number}> .local/owned.paths; D=/own; rm -rf "$D"`, SAFE);
    });
  });
  it('R06 refuses physical CR before apparent PID replacement', () => {
    check('P=$(pgrep worker);\rP=4242; kill "$P"', STATE);
  });
  it('SIMPLE-A06 preserves LF and space separated PID replacement', () => {
    check('P=$(pgrep worker);\nP=4242; kill "$P"', SAFE);
    check('P=$(pgrep worker); P=4242; kill "$P"', SAFE);
  });
  it('R07 refuses physical CR before apparent path replacement', () => {
    check('D=/tmp;\rD=/own; rm -rf "$D"', STATE);
  });
  it('SIMPLE-A07 preserves LF and space separated path replacement', () => {
    check('D=/tmp;\nD=/own; rm -rf "$D"', SAFE);
    check('D=/tmp; D=/own; rm -rf "$D"', SAFE);
  });
  it('R08 refuses CR in every unproved escaped quoted comment heredoc or redirect boundary', () => {
    for (const command of [
      ': "ordinary\rDATA"; kill 4242',
      ': "ordinary\\\rDATA"; kill 4242',
      ': \\\r; kill 4242',
      ':; # single quote is comment DATA \'\r\'\nkill 4242',
      "cat <<'EOF'\n'\r'\nEOF\nkill 4242",
      ': > .local/file\r; kill 4242',
      ': $(printf "%s" "\r"); kill 4242',
    ]) check(command, STATE);
  });
  it('SIMPLE-A08 retains proven inert single-quoted CR bytes including capture and concatenation', () => {
    for (const command of [
      ": 'ordinary\rDATA'; kill 4242",
      "F='ordinary\rDATA'; : \"$F\"; kill 4242",
      ": before'\r'after; kill 4242",
      ": $(printf '%s' '\r'); kill 4242",
      "D='/own\r'; rm -rf \"$D\"",
      String.raw`: '\r'; kill 4242`,
    ]) check(command, SAFE);
  });
});
