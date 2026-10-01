import {describe,it,expect} from 'vitest';
import {scanCommand,killsByPattern,wipesTmp} from '../src/core/pattern-kill.js';
// Inert command DATA only: never dispatch these destructive witnesses.
const SAFE={blocked:false,wipe:false,exhausted:false};const STATE={...SAFE,shellState:true};const TMP={...SAFE,wipe:true};const PID={...SAFE,blocked:true};
function check(command:string,expected:typeof SAFE|typeof STATE){const actual=scanCommand(command);expect(actual,command).toStrictEqual(expected);expect(Object.keys(actual).sort(),command).toStrictEqual(Object.keys(expected).sort());expect(killsByPattern(command),command).toBe(expected.blocked||'shellState'in expected);expect(wipesTmp(command),command).toBe(expected.wipe||'shellState'in expected);}
const pairs:[string,string[],string[],typeof SAFE|typeof STATE][]=[
 ['ASTRA15.1-SEC16.F54.exact-test',[`O=-v; P=4242; Q=$(pgrep -n worker); test "$O" 'P[P=Q]'; kill "$P"`],['P=4242; test -v P; kill "$P"'],STATE],
 ['ASTRA15.1-SEC16.F54.exact-bracket',[`O=-v; P=4242; Q=$(pgrep -n worker); [ "$O" 'P[P=Q]' ]; kill "$P"`],['P=4242; [ -v P ]; kill "$P"'],STATE],
 ['ASTRA15.2-SEC16.F55.exact-wrapper',[`D=$(/dev/null/builtin printf '%s' /usr; printf '%s' /tmp); rm -rf "$D"`],[`D=$(builtin printf '%s' /usr; printf '%s' /tmp); rm -rf "$D"`],STATE],
 ['ASTRA15.2-SEC16.F55.exact-shell',[`D=$(/dev/null/bash -c 'printf %s /usr'; printf '%s' /tmp); rm -rf "$D"`],[`D=$(bash -c 'printf %s /usr'; printf '%s' /tmp); rm -rf "$D"`],STATE],
 ['SEC16.F55.path-env-command',[`D=$(/dev/null/env printf '%s' /usr; printf '%s' /tmp); rm -rf "$D"`,`D=$(/dev/null/command printf '%s' /usr; printf '%s' /tmp); rm -rf "$D"`],[`D=$(env printf '%s' /usr; printf '%s' /tmp); rm -rf "$D"`,`D=$(command printf '%s' /usr; printf '%s' /tmp); rm -rf "$D"`],STATE],
 ['SEC16.F55.direct-producer',[`D=$(/dev/null/printf '%s' /usr; printf '%s' /tmp); rm -rf "$D"`,`D=$(/dev/null/echo /usr; printf '%s' /tmp); rm -rf "$D"`],[`D=$(printf '%s' /usr; printf '%s' /tmp); rm -rf "$D"`],TMP],
 ['SEC16.F55.path-silence',[`D=$(/dev/null/true); rm -rf "$D"`],[`D=$(true); rm -rf "$D"`],TMP],
 ['SEC16.F55.nonexecuting-child-admission',[`/dev/null/bash -c 'P=4242; kill "$P"'`,`/dev/null/env true`],[`bash -c 'P=4242; kill "$P"'`,'env true'],STATE],
 ['F54.expanded-test-option',['OP=-v; P=4242; Q=$(pgrep worker); test "$OP" \'P[P=Q]\'; kill "$P"'],['P=4242; test -v P; kill "$P"'],STATE],
 ['F54.expanded-bracket-option',['OP=-v; P=4242; Q=$(pgrep worker); [ "$OP" \'P[P=Q]\' ]; kill "$P"'],['P=4242; [ -v P ]; kill "$P"'],STATE],
 ['F54.expanded-operator',['C=test; OP=-v; P=4242; Q=$(pgrep worker); "$C" "$OP" \'P[P=Q]\'; kill "$P"','C=printf; "$C" %s /own','OP=-v; P=4242; Q=$(pgrep worker); t?st "$OP" \'P[P=Q]\'; kill "$P"'],['printf %s \'test $OP P[P=Q]\'; P=4242; kill "$P"'],STATE],
 ['F55.vanishing-payload',['E=; D=$(bash -c $E \'printf %s /tmp\'); rm -rf "$D"','E=; D=$(sh -c $E \'printf %s /tmp\'); rm -rf "$D"'],['D=$(bash -c \'printf %s /own\'); rm -rf "$D"'],STATE],
 ['F55.expanded-c-slot',['OPT=-c; bash "$OPT" \'P=$(pgrep worker); kill "$P"\'','OPT=-c; sh "$OPT" \'rm -rf /tmp\''],['bash -c \'P=4242; kill "$P"\''],STATE],
 ['F55.expanded-payload',['SCRIPT=; D=$(bash -c "$SCRIPT"); rm -rf "$D"'],['D=$(bash -c \'printf %s /own\'); rm -rf "$D"'],STATE],
 ['F56.verbose-rm-capture',['D=$(rm -v "/own/ /tmp /file"); rm -rf $D'],['D=$(rm -f "/own/ /tmp /file"); rm -rf $D'],TMP],
 ['F56.long-verbose-and-pipe',['D=$(rm --verbose "/own/ /tmp /file"); rm -rf $D','rm -v "/own/ /tmp /file" | xargs rm -rf'],['rm -f "/own/file" | xargs rm -f'],TMP],
 ['F56.appended-option-output',['D=$(cat .local/args | xargs rm -f); rm -rf "$D"'],['D=$(rm -f -- /own/file); rm -rf "$D"'],TMP],
 ['F56.glob-selected-option-output',['D=$(rm -f *); rm -rf "$D"'],['D=$(rm -f -- *); rm -rf "$D"'],TMP],
 ['F56.unproved-shred-output',['D=$(shred -); rm -rf $D'],['D=$(printf %s /own); rm -rf "$D"'],TMP],
 ['F56.unknown-options',['OPTS=-v; D=$(rm "$OPTS" "/own/ /tmp /file"); rm -rf $D','D=$(rm --help); rm -rf $D'],['D=$(rm --force -- "/own/ /tmp /file"); rm -rf $D'],TMP],
 ['F57.pwd-rewrite',['PWD=/own; cd /tmp; rm -rf "$PWD"'],['D=/own; cd /tmp; rm -rf "$D"'],TMP],
 ['F57.oldpwd-rewrite',['OLDPWD=/own; cd /tmp; cd /own; rm -rf "$OLDPWD"'],['D=/own; cd /tmp; cd /own; rm -rf "$D"'],TMP],
 ['F57.last-argument-rewrite',['_=/own; printf %s /tmp; rm -rf "$_"'],['D=/own; printf %s /tmp; rm -rf "$D"'],TMP],
 ['F57.last-pid-rewrite',['_=4242; P=$(pgrep worker); : "$P"; kill "$_"'],['P=4242; : 5555; kill "$P"'],PID],
];
describe('PR166 round16 conservative operator/output/implicit-cell cuts',()=>{for(const [id,refused,simple,verdict] of pairs){it(`${id} R unsupported class grants nothing`,()=>{for(const command of refused)check(command,verdict);});it(`${id} SIMPLE literal counterpart stays allowed`,()=>{for(const command of simple)check(command,SAFE);});}});
