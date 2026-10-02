#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
const emit = event => process.stdout.write(JSON.stringify(event) + '\n');
const args = process.argv.slice(2);
const sessionFile = args[args.indexOf('--session') + 1];
const directory = path.dirname(sessionFile);
const attemptFile = path.join(directory, 'queue-attempts');
const attempt = Number(fs.existsSync(attemptFile) ? fs.readFileSync(attemptFile, 'utf8') : 0) + 1;
fs.writeFileSync(attemptFile, String(attempt));
if (!fs.existsSync(sessionFile)) fs.writeFileSync(sessionFile, JSON.stringify({type:'session',id:'queue-session',cwd:process.cwd()})+'\n');
let model = {provider:'queue-test',id:'offline'};
let failed = false;
const pending = {steering:[],followUp:[]};
emit({type:'fabric_runner_session',runId:process.env.PI_FABRIC_PARENT_RUN,sessionId:'queue-session'});
const input = readline.createInterface({input:process.stdin});
input.on('line', line => {
  const frame = JSON.parse(line);
  fs.appendFileSync(path.join(directory,'queue-inputs.jsonl'),JSON.stringify({attempt,...frame})+'\n');
  const reply = data => emit({type:'response',id:frame.id,command:frame.type,success:true,data});
  if (frame.type === 'get_state') reply({model,isStreaming:false,isCompacting:false,thinkingLevel:'off'});
  else if (frame.type === 'set_model') { model={provider:frame.provider,id:frame.modelId}; reply(model); }
  else if (frame.type === 'set_thinking_level') reply();
  else if (frame.type === 'prompt') {
    emit({type:'agent_start'});
    if (attempt > 1) emit({type:'message_start',message:{role:'assistant',provider:model.provider,model:model.id,content:[]}});
    if (attempt > 1) setTimeout(() => {
      // These native queues are consumed, so a further recovery must not replay them.
      emit({type:'queue_update',steering:[],followUp:[]});
      emit({type:'message_end',message:{role:'assistant',provider:model.provider,model:model.id,content:'queues retained',stopReason:'stop'}});
      emit({type:'agent_end'}); emit({type:'agent_settled',outcome:'completed'});
    },600);
  } else if (frame.type === 'steer' || frame.type === 'follow_up') {
    pending[frame.type === 'steer' ? 'steering' : 'followUp'].push(frame.message);
    emit({type:'queue_update',...pending});
    if (attempt === 1 && pending.steering.length && pending.followUp.length && !failed) {
      failed = true;
      emit({type:'message_end',message:{role:'assistant',provider:model.provider,model:model.id,content:[],stopReason:'error',errorMessage:'503 server_is_overloaded'}});
      emit({type:'agent_end',willRetry:false}); emit({type:'agent_settled',outcome:'error'});
    }
  }
});
input.on('close',()=>process.exit(0));
