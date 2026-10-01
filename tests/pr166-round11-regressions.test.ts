import { describe, expect, it } from 'vitest';
import { expectRound14Migration } from './pr166-round14-policy.js';
import { scanCommand, killsByPattern, wipesTmp } from '../src/core/pattern-kill.js';

// All commands are inert scanner DATA, never dispatched to Bash.
const SAFE = { blocked: false, wipe: false, exhausted: false };
const TMP = { ...SAFE, wipe: true };
const PID = { ...SAFE, blocked: true };
function check(command: string, expected: typeof SAFE): void {
  const actual = scanCommand(command);
  if (expectRound14Migration(command, actual)) return;
  // R13 owner cut: preserve the old command/ID, but unsupported popd is STATE.
  const state = command === 'cd /own; pushd /tmp; popd; rm -rf *';
  const verdict = state ? { ...SAFE, shellState: true } : expected;
  expect(actual, command).toStrictEqual(verdict);
  expect(Object.keys(actual).sort(), command).toStrictEqual(Object.keys(verdict).sort());
  expect(actual.exhausted, command).toBe(false);
  expect(killsByPattern(command), command).toBe(state || expected.blocked);
  expect(wipesTmp(command), command).toBe(state || expected.wipe);
}
const skipped = [
  "if false; then printf '%s' /own; fi",
  "for item in; do printf '%s' /own; done",
  "while false; do printf '%s' /own; done",
  "until true; do printf '%s' /own; done",
  "false && printf '%s' /own",
  "true || printf '%s' /own",
];
describe('PR166 round11 skipped stdout and wrapped parent cwd', () => {
  skipped.forEach((producer, i) => {
    it(`F36-R${i + 1} declines potentially skipped capture bytes`, () => {
      check(`D=$(${producer}; printf '%s' /tmp); rm -rf "$D"`, TMP);
      check(`P=$(${producer}; printf '%s' 4242); kill "$P"`, PID);
    });
    it(`F36-SIMPLE${i + 1} preserves unconditional composition and silent guards`, () => {
      check("D=$(printf '%s' /own; printf '%s' /tmp); rm -rf \"$D\"", SAFE);
      check("D=$(if false; then :; fi; printf '%s' /own); rm -rf \"$D\"", SAFE);
      check("P=$(printf '%s' 42; printf '%s' 42); kill \"$P\"", SAFE);
    });
  });
  it('F36-R7 declines conditional saved stdout and pipeline/capture composition', () => {
    check("if false; then printf '%s' /own; fi > .local/prefix; printf '%s' /tmp >> .local/prefix; D=$(cat .local/prefix); rm -rf \"$D\"", TMP);
    check("D=$(if false; then printf '%s' /own; fi; printf '%s' /tmp) ; printf '%s' \"$D\" | xargs rm -rf", TMP);
  });
  it('F36-SIMPLE7 preserves unconditional saved bytes and literal-owned capture', () => {
    check("printf '%s' /own > .local/prefix; printf '%s' /tmp >> .local/prefix; D=$(cat .local/prefix); rm -rf \"$D\"", SAFE);
    check("D=$(printf '%s' /own); rm -rf \"$D\"", SAFE);
  });
  ['env', 'nice', 'command', 'builtin', 'xargs'].forEach((wrapper, i) => {
    it(`F37-R${i + 1} withholds wrapped builtin parent cwd and stack credit`, () => {
      check(`cd /tmp; ${wrapper} pushd /own; rm -rf *`, TMP);
      check(`cd /tmp; ${wrapper} cd /own; rm -rf *`, TMP);
      check(`cd /own; pushd /tmp; ${wrapper} popd; rm -rf *`, TMP);
    });
    it(`F37-SIMPLE${i + 1} preserves direct builtins and harmless wrapped leaves`, () => {
      check('cd /tmp; pushd /own; rm -rf *', SAFE);
      check('cd /tmp; cd /own; rm -rf *', SAFE);
      check('cd /own; pushd /tmp; popd; rm -rf *', SAFE);
      check(`cd /own; ${wrapper} pushd /elsewhere; rm -rf *`, SAFE);
    });
  });
  it('F37-R7 keeps a child-local directory change out of the parent', () => {
    check("cd /tmp; bash -c 'cd /own'; rm -rf *", TMP);
    check('cd /tmp; (cd /own); rm -rf *', TMP);
  });
  it('F37-SIMPLE7 retains proved fresh child and direct grammar-prefix controls', () => {
    check("bash -c 'cd /own; rm -rf *'", SAFE);
    check('cd /tmp; { cd /own; }; rm -rf *', SAFE);
    check("cd /own; bash -c 'cd /tmp'; rm -rf *", SAFE);
  });
});
