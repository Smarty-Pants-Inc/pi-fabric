import { describe, expect, it } from 'vitest';
import { scanCommand, killsByPattern, wipesTmp } from '../src/core/pattern-kill.js';
// DATA only: never execute these deletion/lookup commands.
const SAFE = { blocked: false, wipe: false, exhausted: false };
const TMP = { ...SAFE, wipe: true };
const PID = { ...SAFE, blocked: true };
function check(command: string, expected: typeof SAFE): void {
  const actual=scanCommand(command);
  expect(actual,command).toStrictEqual(expected);
  expect(Object.keys(actual).sort(),command).toStrictEqual(Object.keys(expected).sort());
  expect(actual.exhausted,command).toBe(false);
  expect(killsByPattern(command),command).toBe(expected.blocked);
  expect(wipesTmp(command),command).toBe(expected.wipe);
}
describe('PR166 round12 cwd lexical membership and stdout attestation',()=>{
  ['/usr/bin/','./'].forEach((prefix,i)=>{
    it(`F40-R${i+1} path-qualified cd/pushd/popd never mutates parent cwd`,()=>{
      check(`cd /tmp; ${prefix}cd /own; rm -rf *`,TMP);
      check(`cd /tmp; ${prefix}pushd /own; rm -rf *`,TMP);
      check(`cd /own; pushd /tmp > /dev/null; ${prefix}popd; rm -rf *`,TMP);
    });
    it(`F40-SIMPLE${i+1} genuine bare builtins and harmless external receivers`,()=>{
      check('cd /tmp; cd /own; rm -rf *',SAFE);
      check('cd /tmp; pushd /own > /dev/null; rm -rf *',SAFE);
      check('cd /own; pushd /tmp > /dev/null; popd > /dev/null; rm -rf *',SAFE);
      check(`cd /own; ${prefix}pushd /elsewhere; rm -rf *`,SAFE);
    });
  });
  ['/usr/bin/then','./then','/usr/bin/do','./if'].forEach((word,i)=>{
    it(`F40-GRAMMAR-R${i+1} grammar-looking executable argv cannot bind the parent`,()=>{
      check(`D=/tmp; ${word} D=/own; rm -rf "$D"`,TMP);
      check(`P=$(pgrep worker); ${word} P=4242; kill "$P"`,PID);
    });
    it(`F40-GRAMMAR-SIMPLE${i+1} genuine standalone replacements remain supported`,()=>{
      check('D=/tmp; D=/own; rm -rf "$D"',SAFE);
      check('P=$(pgrep worker); P=4242; kill "$P"',SAFE);
      check(`D=/own; ${word} D=/elsewhere; rm -rf "$D"`,SAFE);
    });
  });
  const producers=[
    'cd /tmp; pushd /var > /dev/null; popd',
    'cd /var; pushd /tmp',
    'cd /tmp; cd /var; cd -',
    'CDPATH=/tmp; cd leaf',
  ];
  producers.forEach((producer,i)=>{
    it(`F41-R${i+1} unproved directory stdout cannot attest capture or pipeline operands`,()=>{
      check(`D=$(${producer}); rm -rf $D`,TMP);
      check(`P=$(${producer}); kill "$P"`,PID);
      check(`{ ${producer}; } | xargs rm -rf`,TMP);
    });
    it(`F41-SIMPLE${i+1} redirected-away stack output and proved silent absolute cd stay safe`,()=>{
      check('D=$(cd /own; pushd /private > /dev/null; popd > /dev/null; printf %s /own); rm -rf "$D"',SAFE);
      check('D=$(cd /own; printf %s /own); rm -rf "$D"',SAFE);
      check('D=/own; P=$(cd "$D"; printf %s 4242); kill "$P"',SAFE);
      check('cd /own; pushd /private; rm -rf .local/own-file',SAFE);
    });
  });
  it('F41-R5 saved directory-stack stdout and quoted restored-root capture refuse',()=>{
    check('cd /tmp; pushd /var > /dev/null; popd > /own/stack; D=$(cat /own/stack); rm -rf "$D"',TMP);
    check('D=$(cd /tmp; pushd /var > /dev/null; popd); rm -rf "$D"',TMP);
  });
  it('F41-SIMPLE5 genuine owned output resets saved file facts',()=>{
    check('cd /own; pushd /private > /own/stack; printf %s /own > /own/stack; D=$(cat /own/stack); rm -rf "$D"',SAFE);
    check('D=$(cd /own; pushd /private > /dev/null; printf %s /own); rm -rf "$D"',SAFE);
  });
});
