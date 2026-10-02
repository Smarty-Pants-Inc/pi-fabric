#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';

// The real worker launches this RPC peer anew on each manager retry/resume.
const marker = path.join(process.cwd(), 'native-session-attempts');
const attempt = fs.existsSync(marker) ? Number(fs.readFileSync(marker, 'utf8')) + 1 : 1;
fs.writeFileSync(marker, String(attempt));
const ids = [1, 2].map(n => `01900000-0000-7000-8000-${String((attempt - 1) * 2 + n).padStart(12, '0')}`);
const emit = event => process.stdout.write(JSON.stringify(event) + '\n');
const identity = sessionId => emit({type: 'fabric_runner_session', runId: process.env.PI_FABRIC_PARENT_RUN, sessionId});
identity(ids[0]);
let model = {provider: 'session-test', id: 'offline'};
let buffer = '';
process.stdin.on('data', chunk => {
  buffer += chunk;
  let newline;
  while ((newline = buffer.indexOf('\n')) >= 0) {
    const frame = JSON.parse(buffer.slice(0, newline));
    buffer = buffer.slice(newline + 1);
    const reply = data => emit({type: 'response', id: frame.id, command: frame.type, success: true, data});
    if (frame.type === 'get_state') reply({model, sessionId: ids[0], thinkingLevel: 'off', isStreaming: false, isCompacting: false});
    else if (frame.type === 'set_model') {model = {provider: frame.provider, id: frame.modelId}; reply(model);}
    else if (frame.type === 'set_thinking_level') reply();
    else if (frame.type === 'prompt') {
      emit({type: 'agent_start'});
      identity(ids[1]);
      identity(ids[1]); // duplicate observations must not duplicate history
      if (attempt === 1 && frame.message.includes('startup-retry')) {
        process.stderr.write('No API key found for session-test\n');
        process.exitCode = 1;
        process.stdin.destroy();
      } else {
        emit({type: 'message_end', message: {role: 'assistant', provider: model.provider, model: model.id, content: 'attempt ' + attempt, stopReason: 'stop'}});
        emit({type: 'turn_end', turnIndex: 0});
        // In resume mode leave real work in flight until the test signals the
        // real worker. Do not manufacture a status record or a replacement launch.
        if (attempt > 1) {
          emit({type: 'agent_end'});
          emit({type: 'agent_settled'});
        }
      }
    }
  }
});
process.stdin.on('end', () => process.exit(0));
