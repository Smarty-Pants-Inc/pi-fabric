import {expect} from 'vitest';
import {scanCommand,killsByPattern,wipesTmp} from '../src/core/pattern-kill.js';
// Explicit owner R16 literal-payload class cut. Exact old IDs/commands unchanged; no derived expectations.
const intentionalState=new Set<string>([
  "D=/own; D=/own bash -c \"__pk_arg_2=/own; __pk_arg_3=/own; rm -rf \\\"$D\\\"\"",
  "D=/own; D=/tmp bash -c \"rm -rf \\\"$D\\\"\"",
  "P=4242; P=4242 bash -c \"__pk_arg_2=4242; __pk_arg_3=4242; kill \\\"$P\\\"\"",
  "P=4242; P=7777 bash -c \"kill \\\"$P\\\"\"",
  "P=4242; __pk_arg_2=4242; __pk_arg_3=4242; P=4242 bash -c \"kill \\\"$P\\\"\"",
  "PID=$!; bash -c \"kill $PID\"",
  "bash -c \"echo $(pgrep -c worker); kill $(cat run.pid)\"",
  "bash -c \"kill $PID\"",
  "cat .local/server.pid | sh -c 'bash -c \"kill \\$(cat)\"'",
  "find /tmp/tmp.AbC123 -name '*.json' | sh -c 'bash -c \"rm -rf \\$(cat)\"'"
]);
export function expectRound16Migration(command:string,result:ReturnType<typeof scanCommand>):boolean {
 if(!intentionalState.has(command))return false;
 expect(result,command).toStrictEqual({blocked:false,wipe:false,exhausted:false,shellState:true});
 expect(Object.keys(result).sort(),command).toStrictEqual(['blocked','exhausted','shellState','wipe']);
 expect(killsByPattern(command),command).toBe(true);expect(wipesTmp(command),command).toBe(true);return true;
}
