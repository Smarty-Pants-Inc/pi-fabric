import {describe,it,expect} from 'vitest';
import {scanCommand,killsByPattern,wipesTmp} from '../src/core/pattern-kill.js';
// Inert scanner DATA only. Never dispatch these destructive examples.
const SAFE={blocked:false,wipe:false,exhausted:false};const STATE={...SAFE,shellState:true};const TMP={...SAFE,wipe:true};
function check(command:string,expected:typeof SAFE|typeof STATE){const actual=scanCommand(command);expect(actual,command).toStrictEqual(expected);expect(Object.keys(actual).sort(),command).toStrictEqual(Object.keys(expected).sort());expect(killsByPattern(command),command).toBe(expected.blocked||'shellState'in expected);expect(wipesTmp(command),command).toBe(expected.wipe||'shellState'in expected);}
const pairs:[string,string[],string[],typeof SAFE|typeof STATE][]=[
 ['F48.element-assignment', ['D=/own; D[0]=/tmp; rm -rf "$D"','D=/own; D[0]+=/tmp; rm -rf "$D"','D=/own; D["0"]=/tmp; rm -rf "$D"','D=/own; D[0]=/tmp /usr/bin/true; rm -rf "$D"'],['D=/tmp; D=/own; rm -rf "$D"'],STATE],
 ['F48.arithmetic-subscript',['P=4242; Q=$(pgrep -n worker); D[P=Q]=0; kill "$P"','D=/own; D[(P=0)]=/tmp; rm -rf "$D"'],["printf %s 'D[0]=/tmp'; D=/own; rm -rf \"$D\""],STATE],
 ['F49.test-existence',['P=4242; Q=$(pgrep -n worker); test -v "P[P=Q]"; kill "$P"'],['P=4242; test -v P; kill "$P"'],STATE],
 ['F49.bracket-existence',["P=4242; Q=$(pgrep -n worker); [ -v 'P[P=Q]' ]; kill \"$P\""],['P=4242; [ -v P ]; kill "$P"'],STATE],
 ['F50.command-cwd',['cd /own; command cd /tmp; rm -rf *','cd /own; command pushd /tmp; rm -rf *'],['cd /tmp; cd /own; rm -rf *'],STATE],
 ['F50.builtin-cwd',['cd /own; builtin cd /tmp; rm -rf *','cd /own; builtin pushd /tmp; rm -rf *'],["cd /own; bash -c 'cd /tmp'; rm -rf *"],STATE],
 ['F51.zero-then-real-selector',['P=$(pgrep worker); kill -0 -s TERM "$P"','P=$(pgrep worker); kill -s 0 -TERM "$P"','P=$(pgrep worker); kill -00 -9 "$P"'],['kill -0 4242'],STATE],
 ['F51.lookup-xargs-selector',['pgrep worker | xargs kill -0 -s TERM','P=$(pgrep worker); kill -0 "$P"'],['kill -0 -- $(pgrep worker)'],STATE],
 ['F52.pwd-invalid-argv',["D=$(cd /usr; pwd -Q; printf '%s' /tmp); rm -rf \"$D\"","pwd -Q > /own/prefix; printf %s /tmp >> /own/prefix; D=$(cat /own/prefix); rm -rf \"$D\""],['D=$(cd /own; pwd); rm -rf "$D"'],TMP],
 ['F52.pwd-unproved-redirect',["D=$(cd /usr; pwd 9<&- <&9; printf '%s' /tmp); rm -rf \"$D\"","pwd 9<&- <&9 > /own/prefix; printf %s /tmp >> /own/prefix; D=$(cat /own/prefix); rm -rf \"$D\""],['D=$(cd /own; pwd); rm -rf "$D"'],TMP],
 ['F53.wrapper-output',["D=$(env --bad-option printf '%s' /usr; printf '%s' /tmp); rm -rf \"$D\"","D=$(nice -n invalid printf %s /usr; printf %s /tmp); rm -rf \"$D\"",'D=$(env --bad-option true); rm -rf "$D"'],['D=$(env printf %s /own); rm -rf "$D"'],TMP],
 ['F53.shell-startup',["D=$(bash --bad-option -c 'printf %s /usr'; printf '%s' /tmp); rm -rf \"$D\"","D=$(sh --bad-option -c 'printf %s /usr'; printf %s /tmp); rm -rf \"$D\"", "D=$(bash --bad-option -c ':'); rm -rf \"$D\""],["D=$(bash -c 'printf %s /own'); rm -rf \"$D\""],TMP],
];
describe('PR166 Astra14/security15 conservative class admission',()=>{for(const [id,refused,simple,verdict] of pairs){it(`${id} R unsupported class grants nothing`,()=>{for(const command of refused)check(command,verdict);});it(`${id} SIMPLE proven counterpart stays allowed`,()=>{for(const command of simple)check(command,SAFE);});}
 it('F51-list R harmless list is not an exact-zero-byte producer',()=>check('P=$(kill -l TERM; printf %s 4242); kill "$P"',{...SAFE,blocked:true}));
 it('F51-list SIMPLE harmless diagnostic leaf or literal replacement',()=>{check('kill -l TERM; P=4242; kill "$P"',SAFE);check('P=$(kill -00 4242; printf %s 4242); kill "$P"',SAFE);});
});
