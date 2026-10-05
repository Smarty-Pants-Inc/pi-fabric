#!/usr/bin/env node
// Credential-free, network-free native Pi + actual worker/Fabric acceptance lane.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawnSync } from 'node:child_process';
import { SessionManager } from '@earendil-works/pi-coding-agent';
const mode = process.argv[2] ?? 'compound';
const out = path.resolve(process.env.COMPACT_PROBE_OUT ?? fs.mkdtempSync(path.join(os.tmpdir(),'compact-native-')));
fs.mkdirSync(out,{recursive:true});
const cwd = path.join(out,mode);
if (fs.existsSync(cwd)) throw new Error('Lane output already exists; choose a fresh COMPACT_PROBE_OUT: ' + cwd);
fs.mkdirSync(cwd);
const profile = path.join(cwd,'profile'); fs.mkdirSync(profile,{recursive:true});
fs.writeFileSync(path.join(profile,'settings.json'),JSON.stringify({extensions:[path.resolve('tests/fixtures/compact-resume-native-provider.ts')], compaction:{enabled:false,keepRecentTokens:128,reserveTokens:1024}, packages:[]}));
fs.mkdirSync(path.join(cwd,'.pi'),{recursive:true});
fs.writeFileSync(path.join(cwd,'.pi','fabric.json'),JSON.stringify({enabled:true}));
const transcript = path.join(cwd,'transcript.jsonl'); fs.writeFileSync(transcript,'');
const task = mode === 'plain' ? 'Compact. Keep the failing test name in the summary.' : mode.startsWith('restart') || mode === 'receipt' ? 'Continue the task from the existing session. Do not repeat completed work.' : 'Compact first, then start item X';
fs.writeFileSync(path.join(cwd,'task.txt'),task);
const manager = SessionManager.create(cwd,path.join(cwd,'sessions'));
manager.appendMessage({role:'user',content:'Inspect failing test test_X; pause before implementation.\n' + 'Historical inspection evidence; no runnable work here. '.repeat(200),timestamp:Date.now()});
manager.appendMessage({role:'assistant',content:[{type:'text',text:'Inspected test_X; work remains.'}], api:'compact-proof-api',provider:'compact-proof',model:'offline',timestamp:Date.now(),stopReason:'stop',usage:{input:1,output:1,cacheRead:0,cacheWrite:0,totalTokens:2,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}}});
if(mode.startsWith('restart') || mode === 'receipt') {
  manager.appendCustomEntry('fabric-compact-resume',{id:'12345678-1234-1234-1234-123456789abc',resume:'start item X',state:'pending'});
  manager.appendCompaction('Committed; continuation not admitted',manager.getBranch()[0].id,1000);
  fs.appendFileSync(transcript,JSON.stringify({type:'seed-committed',at:Date.now()})+'\n');
  if(mode === 'receipt') manager.appendMessage({role:'user',content:'Resume after compaction: already admitted\n\n[Fabric continuation: 12345678-1234-1234-1234-123456789abc]',timestamp:Date.now()});
}
const source = process.env.COMPACT_PROBE_SOURCE === '1';
const worker = path.resolve(source ? 'src/worker.ts' : 'dist/worker.js');
const fabric = path.resolve(source ? 'src/index.ts' : 'dist/index.js');
const pi = process.env.COMPACT_PROBE_PI ?? process.env.PI ?? path.resolve('node_modules/@earendil-works/pi-coding-agent/dist/cli.js');
for(const input of [worker,fabric,pi]) if(!fs.existsSync(input)) throw new Error('Missing local input: '+input);
const flags = {'id':'compact-native-'+mode,'runner':'pi','name':'compact-native-proof','task-file':path.join(cwd,'task.txt'),'status-file':path.join(cwd,'status.json'),'lifecycle-file':path.join(cwd,'lifecycle.jsonl'),'log-file':path.join(cwd,'events.jsonl'),'cwd':cwd,'pi-binary':pi,'claude-binary':'unused','veda-binary':'unused','veda-backend':'unused','veda-persona':'unused','timeout-ms':'45000','depth':'1','full-code-mode':'true','extensions':'true','tools':'["fabric_exec","write"]','granted-risks':'["read","write","exec"]','transport':'process','fabric-extension':fabric,'model':'compact-proof/offline','thinking':'off','session-file':manager.getSessionFile(),'budget-usd':'0'};
if(mode === 'restart-actor') flags['actor-id'] = 'compact-proof-actor';
const env = {PATH:process.env.PATH, HOME:cwd, TMPDIR:process.env.TMPDIR ?? cwd, PI_CODING_AGENT_DIR:profile, PI_OFFLINE:'1', PI_SKIP_VERSION_CHECK:'1', PI_TELEMETRY:'0', COMPACT_PROBE_TRANSCRIPT:transcript};
const result = spawnSync(source ? 'bun' : process.execPath,[worker,...Object.entries(flags).flatMap(([key,value])=>['--'+key,value])],{cwd:process.cwd(),env,encoding:'utf8',timeout:55000,maxBuffer:4*1024*1024});
fs.writeFileSync(path.join(cwd,'worker-output.log'),(result.stdout??'')+'\nSTDERR\n'+(result.stderr??''));
const status = fs.existsSync(flags['status-file']) ? JSON.parse(fs.readFileSync(flags['status-file'],'utf8')) : null;
const events = fs.readFileSync(transcript,'utf8').trim().split('\n').filter(Boolean).map(line=>JSON.parse(line));
const x = events.filter(e=>e.type === 'X-start');
const complete = events.find(e=>e.type === 'compaction-complete' || e.type === 'seed-committed');
const startup = events.find(e=>e.type === 'delayed-startup-finished');
const prompts = events.filter(e=>e.type === 'native-prompt');
const wire = fs.existsSync(flags['log-file']) ? fs.readFileSync(flags['log-file'],'utf8').trim().split('\n').filter(Boolean).map(line=>JSON.parse(line)) : [];
const contextIndex = wire.findIndex(e=>e.type === 'response' && e.command === 'get_messages' && e.success && String(e.id).startsWith('fabric-context:'));
const assistantIndex = wire.findIndex(e=>e.type === 'message_start' && e.message?.role === 'assistant');
const contextAdmitted = contextIndex >= 0 && contextIndex < assistantIndex;
const errors = [];
if(mode === 'restart-actor' && !contextAdmitted) errors.push('native actor context admission must precede the first assistant');
if(result.status !== 0 || status?.status !== 'completed') errors.push('worker failed: '+(status?.error ?? result.stderr ?? result.error));
if(mode === 'compound' || mode.startsWith('restart')) {
  if(x.length !== 1 || status?.text !== 'X completed') errors.push('expected exactly one completed continuation');
  if(!fs.existsSync(path.join(cwd,'item-X.txt')) || fs.readFileSync(path.join(cwd,'item-X.txt'),'utf8') !== 'X completed') errors.push('X must execute the real write tool');
  if(!complete || !x[0] || x[0].at-complete.at > 60000) errors.push('X must start within one minute of compaction');
}
if(mode === 'receipt' && (x.length || prompts.length !== 1)) errors.push('persisted receipt must not replay its continuation');
if(mode === 'plain' && (x.length || prompts.length !== 1 || !complete)) errors.push('plain compaction must stay idle after its initial prompt');
if(mode.startsWith('restart') && (prompts.length !== 1 || events.some(e=>e.type === 'input-preflight' && e.at < startup?.at))) errors.push('restart preflight must follow delayed startup with only one admitted prompt');
const summary = {mode,pi,worker,fabric,actorContextAdmitted: contextAdmitted,exitCode:result.status,status:status?.status,text:status?.text,error:status?.error,compactionCompletionAt:complete?.at,xStartAt:x[0]?.at,latencyMs:x[0]&&complete ? x[0].at-complete.at : null,prompts:prompts.length,continuations:x.length,errors,transcript};
fs.writeFileSync(path.join(cwd,'result.json'),JSON.stringify(summary,null,2)+'\n'); console.log(JSON.stringify(summary,null,2));
if(errors.length) process.exitCode=1;
