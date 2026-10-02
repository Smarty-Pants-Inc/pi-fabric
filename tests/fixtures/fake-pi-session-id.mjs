#!/usr/bin/env node
const first = '01900000-0000-7000-8000-000000000001';
const latest = '01900000-0000-7000-8000-000000000002';
const emit = event => process.stdout.write(JSON.stringify(event) + '\n');
const identity = sessionId => emit({type:'fabric_runner_session', runId:process.env.PI_FABRIC_PARENT_RUN, sessionId});
identity(first);
let model = {provider:'session-test', id:'offline'};
let buffer = '';
process.stdin.on('data', chunk => {
  buffer += chunk;
  let newline;
  while ((newline = buffer.indexOf('\n')) >= 0) {
    const frame = JSON.parse(buffer.slice(0,newline));buffer=buffer.slice(newline+1);
    const reply = data => emit({type:'response', id:frame.id, command:frame.type, success:true, data});
    if(frame.type==='get_state') reply({model, sessionId:first, thinkingLevel:'off', isStreaming:false, isCompacting:false});
    else if(frame.type==='set_model') {model={provider:frame.provider,id:frame.modelId};reply(model);}
    else if(frame.type==='set_thinking_level') reply();
    else if(frame.type==='prompt') {
      emit({type:'fake_session_argv', argv:process.argv.slice(2)});
      emit({type:'agent_start'});
      // Stay live long enough to exercise status/listing readback before rotation.
      setTimeout(()=>{
        if(frame.message.includes('rotate')) {
          identity(latest);
          identity(latest); // duplicate observations must not duplicate history
          emit({type:'compaction_end',reason:'manual'});
          identity(''); // malformed observations must not erase the native ID
          emit({type:'fabric_runner_session',runId:'different-run',sessionId:'wrong'});
        }
        emit({type:'message_end',message:{role:'assistant', provider:model.provider,model:model.id,content:'session proof',stopReason:'stop'}});
        emit({type:'agent_end'});
        emit({type:'agent_settled'});
      },600);
    }
  }
});
process.stdin.on('end',()=>process.exit(0));
