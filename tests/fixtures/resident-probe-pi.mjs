#!/usr/bin/env node
import readline from 'node:readline';
const emit = event => process.stdout.write(JSON.stringify(event) + '\n');
const mode = process.env.RESIDENT_PROBE_SCENARIO;
const args = process.argv.slice(2);
emit({ type: 'probe_argv', args });
if (mode !== 'missing') emit({ type: 'fabric_resident_extension_ready', protocol: mode === 'protocol' ? 2 : 1,
  runId: mode === 'run' ? 'other' : process.env.PI_FABRIC_PARENT_RUN,
  nonce: mode === 'nonce' ? 'other' : process.env.PI_FABRIC_RESIDENT_PROBE_NONCE,
  extension: mode === 'extension' ? '/other/index.js' : args[args.indexOf('-e') + 1] });
const input = readline.createInterface({ input: process.stdin });
input.on('line', line => {
  const frame = JSON.parse(line); emit({ type: 'probe_received', frame });
  if (frame.type === 'get_state') emit({ type: 'response', command: frame.type, id: frame.id, success: true, data: {} });
  else if (frame.type === 'prompt') throw Error('startup probe invoked inference');
});
input.on('close', () => process.exit(0));
