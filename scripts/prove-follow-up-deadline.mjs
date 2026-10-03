// Native, offline proof. Build first; usage: nice -n 19 node scripts/prove-follow-up-deadline.mjs PI_CLI SCRATCH OUTPUT
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const [cli, scratch, out] = process.argv.slice(2);
assert(cli && scratch && out, 'PI_CLI SCRATCH OUTPUT required');
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
fs.mkdirSync(scratch, { recursive: true }); fs.mkdirSync(out, { recursive: true });
const transcript = path.join(out, 'native-proof.jsonl'); fs.writeFileSync(transcript, '');
const record = value => fs.appendFileSync(transcript, JSON.stringify(value) + '\n');
const home = path.join(scratch, 'home'), agentDir = path.join(home, '.pi/agent');
fs.mkdirSync(path.join(agentDir, 'extensions'), { recursive: true });
const queue = path.join(scratch, 'queue.json'), calls = path.join(scratch, 'calls.jsonl'), ready = path.join(scratch, 'ready');
const runRoot = path.join(scratch, 'runs');
fs.writeFileSync(queue, '[]');
fs.writeFileSync(path.join(agentDir, 'fabric.json'), JSON.stringify({ autoReload: false, fullCodeMode: true, mcp: { enabled: false }, memory: { enabled: false }, records: { enabled: false }, ui: { enabled: false }, mesh: { root: path.join(scratch, 'mesh') }, agents: { notifyOnComplete: false, nice: 19 }, executor: { timeoutMs: 60000 } }));
fs.writeFileSync(path.join(agentDir, 'settings.json'), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false }, defaultProjectTrust: 'approve' }));
const extension = path.join(agentDir, 'extensions', 'deadline-proof.ts');
const ai = path.join(root, 'node_modules/@earendil-works/pi-ai/dist/index.js');
fs.writeFileSync(extension, `
import fs from 'node:fs';
import os from 'node:os';
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from ${JSON.stringify(ai)};
export default function(pi) {
  const faux = fauxProvider({ provider: 'deadline-proof', models: [{ id: 'offline' }], tokensPerSecond: 100000 });
  const factory = (context, options, state) => {
    const run = process.env.PI_FABRIC_AGENT_RUN_DIR;
    fs.appendFileSync(${JSON.stringify(calls)}, JSON.stringify({ pid: process.pid, argv: process.argv, run, call: state.callCount, messages: context.messages }) + '\\n');
    if (run) {
      if (state.callCount === 1) {
        const gate = process.env.PROOF_GATE;
        const cmd = 'while ! test -f ' + JSON.stringify(gate) + '; do sleep 0.05; done';
        return fauxAssistantMessage([fauxToolCall('fabric_exec', { code: 'return await pi.bash({cmd:' + JSON.stringify(cmd) + ',timeout:60});', resultFormat: 'json' })]);
      }
      return fauxAssistantMessage('deterministic child finished');
    }
    const specs = JSON.parse(fs.readFileSync(${JSON.stringify(queue)}, 'utf8'));
    const spec = specs.shift(); fs.writeFileSync(${JSON.stringify(queue)}, JSON.stringify(specs));
    return spec ? fauxAssistantMessage([fauxToolCall('fabric_exec', { code: spec.code, resultFormat: 'json' }, { id: spec.id })]) : fauxAssistantMessage('proof guest finished');
  };
  faux.setResponses(Array.from({length: 100}, () => factory)); pi.registerProvider(faux.provider);
  pi.on('session_start', (_event, ctx) => {
    if (!process.env.PI_FABRIC_AGENT_RUN_DIR) fs.writeFileSync(${JSON.stringify(ready)}, JSON.stringify({mode:ctx.mode,pid:process.pid,nice:os.getPriority(0)}));
  });
  pi.events.on('fabric.followUp.deadline', alarm => fs.appendFileSync(${JSON.stringify(path.join(scratch, 'alarms.jsonl'))}, JSON.stringify(alarm) + '\\n'));
}
`);
const args = [path.resolve(cli), '--mode', 'rpc', '--offline', '--no-extensions', '--no-skills', '--no-context-files', '--no-prompt-templates', '--no-themes', '--approve', '-e', path.join(root, 'dist/index.js'), '-e', extension, '--provider', 'deadline-proof', '--model', 'offline', '--thinking', 'off', '--tools', 'fabric_exec', '--session-dir', path.join(scratch, 'sessions')];
const env = { PATH: process.env.PATH, HOME: home, TMPDIR: scratch, PI_OFFLINE: '1', PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_PI_BINARY: path.resolve(cli), PI_FABRIC_RUN_ROOT: runRoot, PROOF_GATE: path.join(scratch, 'gate') };
record({ type: 'command', executable: process.execPath, args, cwd: scratch, env });
const child = spawn(process.execPath, args, { cwd: scratch, env, stdio: ['pipe', 'pipe', 'pipe'] });
const events = [], pending = new Map(); let buffer = '', serial = 0, exited = false;
const exit = new Promise(resolve => child.once('close', (code, signal) => { exited = true; resolve({code,signal}); }));
child.stdout.on('data', data => { buffer += data.toString(); let end; while ((end=buffer.indexOf('\n'))>=0) { const line=buffer.slice(0,end); buffer=buffer.slice(end+1); if (!line.trim()) continue; const e=JSON.parse(line); events.push(e); record({type:'rpc',event:e}); if(e.type==='response'){const p=pending.get(e.id);pending.delete(e.id); e.success?p?.resolve(e.data):p?.reject(new Error(e.error));} } });
child.stderr.on('data', data => record({ type: 'stderr', text: data.toString() }));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (check, timeout=60000) => {const until=Date.now()+timeout;while(!check()){assert(!exited,'Pi exited');assert(Date.now()<until,'native proof timeout');await sleep(25);}};
const request = value => new Promise((resolve,reject)=>{const id='rpc-'+(++serial);pending.set(id,{resolve,reject});record({type:'request',...value,id});child.stdin.write(JSON.stringify({...value,id})+'\n');});
async function guest(code) {
  const before = events.length, id = 'guest-'+(++serial);
  fs.writeFileSync(queue, JSON.stringify([{ id, code }])); record({type:'public-fabric-exec',code});
  await request({type:'prompt',message:'Execute '+id});
  await wait(()=>events.slice(before).some(e=>e.type==='agent_settled'));
  const e=events.slice(before).find(e=>e.type==='tool_execution_end'&&e.toolCallId===id);
  assert(e&&!e.isError, JSON.stringify(e??events.slice(before)));
  const text=e.result.content.filter(c=>c.type==='text').map(c=>c.text).join('\n');
  record({type:'public-result',id,result:e.result});
  try { return JSON.parse(text); } catch { throw new Error('non-JSON guest result: '+text); }
}
const results=[]; let failure;
try {
  await wait(()=>fs.existsSync(ready)); const started=JSON.parse(fs.readFileSync(ready)); assert.equal(started.mode,'rpc'); assert.equal(started.nice,19);
  await guest('return await agents.self();');
  for (const lane of ['on-time','late','cancelled']) {
    fs.rmSync(env.PROOF_GATE,{force:true});
    const handle=await guest(`return await agents.spawn({task:'PROOF_CHILD_${lane}',name:'deadline-${lane}',transport:'process',model:'deadline-proof/offline',thinking:'off',tools:['bash'],extensions:true});`);
    const id=handle.id; assert(id,JSON.stringify(handle)); const run=path.join(runRoot,id);
    await wait(()=>{try{return JSON.parse(fs.readFileSync(path.join(run,'status.json'))).currentTool==='fabric_exec';}catch{return false;}});
    const marker='DELIVERY_'+lane;
    const admittedAt = Date.now();
    const receipt=await guest(`return await agents.followUp({id:${JSON.stringify(id)},message:${JSON.stringify(marker)}${lane==='on-time'?'':',deadlineMs:100'}});`);
    if (lane==='on-time') assert(receipt.deadlineAt >= admittedAt + 600000 && receipt.deadlineAt <= Date.now() + 600000, 'default deadline must be ten minutes');
    assert(receipt.messageId&&receipt.deadlineAt,JSON.stringify(receipt));
    if(lane!=='on-time') await wait(()=>fs.existsSync(path.join(scratch,'alarms.jsonl'))&&fs.readFileSync(path.join(scratch,'alarms.jsonl'),'utf8').includes(receipt.messageId));
    const before=await guest(`return await agents.status({id:${JSON.stringify(id)}});`);
    assert.equal(before.followUpDeliveries[0].state,'queued');
    if(lane==='cancelled') {
      const cancelled=await guest(`return await agents.cancelFollowUp({id:${JSON.stringify(id)},messageId:${JSON.stringify(receipt.messageId)}});`); assert.equal(cancelled.state,'cancelled');
    }
    fs.writeFileSync(env.PROOF_GATE,'release');
    await wait(()=>{try{return ['completed','failed','stopped'].includes(JSON.parse(fs.readFileSync(path.join(run,'status.json'))).status);}catch{return false;}});
    await guest(`return await agents.wait({id:${JSON.stringify(id)}});`);
    const after=await guest(`return await agents.status({id:${JSON.stringify(id)}});`); assert.equal(after.status,'completed',JSON.stringify(after));
    const session=fs.readFileSync(path.join(run,'session.jsonl'),'utf8').trim().split('\n').map(l=>JSON.parse(l));
    const delivered=session.filter(e=>e.type==='message'&&e.message?.role==='user'&&JSON.stringify(e.message.content).includes(marker));
    assert.equal(delivered.length,lane==='cancelled'?0:1);
    const alarmFile=path.join(scratch,'alarms.jsonl');
    const alarms=fs.existsSync(alarmFile)?fs.readFileSync(alarmFile,'utf8').trim().split('\n').filter(Boolean).map(l=>JSON.parse(l)).filter(a=>a.messageId===receipt.messageId):[];
    assert.equal(alarms.length,lane==='on-time'?0:1);
    if(alarms.length){assert.equal(alarms[0].currentTool,'fabric_exec');assert(alarms[0].currentToolStartedAt);assert.deepEqual(alarms[0].options,['wait','steer','cancel']);}
    const messages=(await request({type:'get_messages'})).messages;
    const notices=messages.filter(m=>m.customType==='pi-fabric-follow-up-alarm'&&m.details?.messageId===receipt.messageId); assert.equal(notices.length,alarms.length);
    assert.equal(after.followUpDeliveries[0].state,lane==='cancelled'?'cancelled':'delivered');
    fs.copyFileSync(path.join(run,'session.jsonl'),path.join(out,'native-child-'+lane+'.jsonl'));
    results.push({lane,id,workerPid:Number(after.sessionId),messageId:receipt.messageId,alarmCount:alarms.length,deliveryCount:delivered.length,status:after.followUpDeliveries[0],senderSessionAlarms:notices.length}); record({type:'assertions',...results.at(-1)});
    await guest(`return await agents.stop({id:${JSON.stringify(id)}});`);
  }
} catch(error) {failure=error;record({type:'failure',error:String(error.stack??error)});}
finally {
  fs.writeFileSync(env.PROOF_GATE,'release');
  if(!exited){try{await request({type:'abort'});}catch{} child.stdin.end();}
  const closed=await Promise.race([exit,sleep(15000).then(()=>null)]);
  if(!closed){child.kill('SIGKILL');await exit;failure??=new Error('Pi required forced cleanup');}
  const providerCalls = fs.existsSync(calls) ? fs.readFileSync(calls,'utf8').trim().split('\n').filter(Boolean).map(l=>JSON.parse(l)) : [];
  fs.copyFileSync(calls, path.join(out,'native-provider-calls.jsonl'));
  const ownedPids = [...new Set([child.pid, ...results.map(r=>r.workerPid), ...providerCalls.map(c=>c.pid)])].filter(Number.isInteger);
  const live = pid => {try {process.kill(pid,0);return true;} catch(error){return error.code!=='ESRCH';}};
  const until=Date.now()+10000;
  while(ownedPids.some(live)&&Date.now()<until) await sleep(25);
  const remaining=ownedPids.filter(live);
  if(remaining.length)failure??=new Error('native processes still live: '+remaining.join(','));
  for(const call of providerCalls)record({type:'native-provider-call',...call});
  record({type:'cleanup',parentExited:true,exit:closed,ownedPids,remaining});
}
fs.writeFileSync(path.join(out,'native-proof.txt'), `Command: nice -n 19 node scripts/prove-follow-up-deadline.mjs ${cli} ${scratch} ${out}\nResult: ${failure?'FAIL '+failure.message:'PASS: real offline Pi RPC + fresh dist; on-time 0 alarms/1 delivery; late 1 sender-session alarm/1 delivery; cancelled 1 alarm/0 deliveries; all processes closed.'}\n`);
if(failure)throw failure;
console.log(JSON.stringify({result:'PASS',cases:results},null,2));
