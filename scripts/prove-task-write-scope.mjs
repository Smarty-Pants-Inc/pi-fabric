#!/usr/bin/env node
/** Native Pi Main -> public agents.spawn -> real process child, with a keyless
 * loopback provider. All runtime state is private; no installed state changes.
 * Old releases are probed with the same request, never silently treated as confined.
 * Usage: node scripts/prove-task-write-scope.mjs --fabric-root ROOT --evidence-dir DIR --expect-confined
 * --unconfined-control is a separate, explicit probe for a release without policy support.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
const arg = name => { const i=process.argv.indexOf(name); assert(i>=0 && process.argv[i+1],name); return process.argv[i+1]; };
const root=fs.realpathSync(arg('--fabric-root'));
const evidence=path.resolve(arg('--evidence-dir'));
const unconfinedControl=process.argv.includes('--unconfined-control');
const expectConfined=process.argv.includes('--expect-confined');
assert(!(unconfinedControl && expectConfined),'An unconfined control cannot assert confinement');
const pi=process.env.SMARTY_ROLE_PI_BINARY || '/home/paul/.local/share/smarty-dev/pi-runtime/releases/21ab152c7e43af76468d898bc50085fcd01a515b/node/node_modules/@earendil-works/pi-coding-agent/dist/cli.js';
assert(fs.existsSync(path.join(root,'dist/index.js')),'Build Fabric first');
assert(process.env.TMPDIR,'Private TMPDIR required');
assert(!fs.existsSync(evidence),'Use a fresh evidence directory');
fs.mkdirSync(evidence,{recursive:true});
const scratch=fs.mkdtempSync(path.join(process.env.TMPDIR,'task-write-proof-'));
const cwd=path.join(scratch,'workspace'), tmp=path.join(scratch,'tmp'), agent=path.join(scratch,'agent'), runs=path.join(scratch,'runs'), outside=path.join(scratch,'outside');
for(const dir of [cwd,tmp,agent,runs,outside,path.join(scratch,'home')]) fs.mkdirSync(dir);
const json=(file,value)=>fs.writeFileSync(file,JSON.stringify(value,null,2)+'\n');
const marker='WRITE_SCOPE_PROBE_'+crypto.randomUUID();
const targets={outside:path.join(outside,'exists.txt'),missingOutside:path.join(scratch,'absent','parent','file.txt'),symlink:path.join(cwd,'escape','exists.txt'),missingInside:path.join(cwd,'new','parent','file.txt'),tmp:path.join(tmp,'allowed.txt')};
fs.writeFileSync(targets.outside,'original');
fs.symlinkSync(outside,path.join(cwd,'escape'),'dir');
const calls=[];
for(const [kind,target] of Object.entries(targets)) {
 calls.push({id:`write-${kind}`,name:'write',args:{path:target,content:marker+' '+kind}});
 calls.push({id:`edit-${kind}`,name:'edit',args:{path:target,edits:[{oldText:marker+' '+kind,newText:marker+' edited '+kind}]}});
}
let parent, childCount=0,parentCount=0,handle,terminal,failure;
const events=[], requests=[], snapshots=[];
const toolValue=event=>{const r=event?.result;if(r?.details?.result!==undefined)return r.details.result;const text=r?.content?.filter(c=>c.type==='text').map(c=>c.text).join('\n');try{return JSON.parse(text)}catch{return {text,isError:event?.isError,details:r?.details}}};
const latest=needle=>toolValue([...events].reverse().find(e=>e.type==='tool_execution_end'&&e.toolName==='fabric_exec'&&events.some(s=>s.type==='tool_execution_start'&&s.toolCallId===e.toolCallId&&s.args.code.includes(needle))));
const server=http.createServer((req,res)=>{let body='';req.on('data',chunk=>body+=chunk);req.on('end',()=>{try{
 const payload=JSON.parse(body);requests.push(payload);json(path.join(evidence,'requests.json'),requests);
 res.writeHead(200,{'Content-Type':'text/event-stream'});
 const chunk=(delta,finish_reason=null)=>res.write('data: '+JSON.stringify({id:'offline',object:'chat.completion.chunk',created:1,model:payload.model,choices:[{index:0,delta,finish_reason}]})+'\n\n');
 const text=value=>chunk({role:'assistant',content:value});
 const tool=(name,args,id)=>chunk({role:'assistant',tool_calls:[{index:0,id:id||crypto.randomUUID(),type:'function',function:{name,arguments:JSON.stringify(args)}}]});
 const end=reason=>{chunk({},reason||'stop');res.end('data: [DONE]\n\n')};
 if(payload.model==='child'){
   // The next request follows the previous tool result, before the next mutation.
   const previous=calls[childCount-1];
   if(previous) snapshots.push({kind:previous.id,exists:fs.existsSync(previous.args.path),content:fs.existsSync(previous.args.path)?fs.readFileSync(previous.args.path,'utf8'):null});
   const call=calls[childCount++]; if(call){tool(call.name,call.args,call.id);end('tool_calls')} else {text('WRITE_SCOPE_CHILD_COMPLETE');end()} return;
 }
 assert.equal(payload.model,'parent');parentCount++;
 if(parentCount===1){
   const request={name:'private-write-scope',task:'Deterministic offline tool probe. Only the scripted provider decides tool calls.',model:'write-proof/child',thinking:'off',runner:'pi',transport:'process',extensions:false,tools:['write','edit'],writableRoots:[cwd,tmp],shell:'deny'};
   // Explicit separate control: old Fabric has no write-scope API. Never silently
   // drop a requested grant after validation refuses it.
   if(unconfinedControl){delete request.writableRoots;delete request.shell;}
   // Generic public path permits observing validation on an old descriptor.
   tool('fabric_exec',{code:'return await tools.call({ref:"agents.spawn",args:'+JSON.stringify(request)+'});',resultFormat:'json'});end('tool_calls');
 } else if(parentCount===2){
   handle=latest('agents.spawn');json(path.join(evidence,'spawn-result.json'),handle);
   if(handle?.id){tool('fabric_exec',{code:'return await agents.wait({id:'+JSON.stringify(handle.id)+',timeoutMs:180000});',resultFormat:'json'});end('tool_calls')}
   else{text('WRITE_SCOPE_SPAWN_REFUSED');end()}
 }else{terminal=latest('agents.wait');json(path.join(evidence,'terminal-result.json'),terminal);text('WRITE_SCOPE_PARENT_COMPLETE');end()}
 }catch(error){failure=error;res.destroy(error)}})});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
json(path.join(agent,'models.json'),{providers:{'write-proof':{baseUrl:`http://127.0.0.1:${server.address().port}/v1`,apiKey:'offline-not-a-credential',api:'openai-completions',models:['parent','child'].map(id=>({id,name:id,reasoning:false,input:['text'],contextWindow:128000,maxTokens:4096,cost:{input:0,output:0,cacheRead:0,cacheWrite:0}}))}}});
json(path.join(agent,'settings.json'),{enableInstallTelemetry:false,compaction:{enabled:false},retry:{enabled:false},extensions:[],packages:[]});
json(path.join(agent,'fabric.json'),{configVersion:4,fullCodeMode:false,autoReload:false,executor:{kernel:'typescript',timeoutMs:180000,maxTimeoutMs:180000,resultFormat:'json'},agents:{enabled:true,runner:'pi',transport:'process',maxDepth:2,maxConcurrent:2,extensions:false,deniedModels:[],modelPolicy:{requireReason:[]},retainRuns:true,sessionExport:false,notifyOnComplete:false,timeoutMs:180000},mcp:{enabled:false},mesh:{enabled:true,announce:true,actorScope:'session'},capture:{enabled:false},jev:{enabled:false},prewalk:{enabled:false},ui:{enabled:false},compaction:{engine:'pi'},approvals:{read:'allow',write:'allow',execute:'allow',network:'allow',agent:'allow'}});
const env=Object.fromEntries(Object.entries(process.env).filter(([key])=>!/^(PI_|SMARTY_|HERDR_|ORG_|OPENAI_|ANTHROPIC_|GOOGLE_|GEMINI_|OPENROUTER_|TYPESAFE_|AWS_|AZURE_|AI_GATEWAY_|OTEL_|SENTRY_)/.test(key)));
Object.assign(env,{HOME:path.join(scratch,'home'),XDG_CONFIG_HOME:path.join(scratch,'home/.config'),XDG_DATA_HOME:path.join(scratch,'home/.local/share'),XDG_CACHE_HOME:path.join(scratch,'home/.cache'),TMPDIR:tmp,PI_CODING_AGENT_DIR:agent,PI_OFFLINE:'1',PI_SKIP_VERSION_CHECK:'1',PI_TELEMETRY:'0',PI_FABRIC_PI_BINARY:pi,PI_FABRIC_PROJECT_ROOT:cwd,PI_FABRIC_MESH_ROOT:path.join(scratch,'mesh'),PI_FABRIC_RUN_ROOT:runs});
parent=spawn(process.execPath,[pi,'--mode','rpc','--no-session','--provider','write-proof','--model','parent','--thinking','off','--no-context-files','--no-skills','--no-prompt-templates','--no-themes','--no-extensions','--extension',path.join(root,'dist/index.js'),'--approve'],{cwd,env,stdio:['pipe','pipe','pipe']});
json(path.join(evidence,'launch.json'),{root,pi,scratch,cwd,tmp,targets,marker,pid:parent.pid});
let tail='';let resolveSettled,rejectSettled;
const settled=new Promise((resolve,reject)=>{resolveSettled=resolve;rejectSettled=reject});
const deadline=setTimeout(()=>rejectSettled(new Error('Native proof deadline 180s')),180000);
parent.stdout.on('data',chunk=>{fs.appendFileSync(path.join(evidence,'parent-events.jsonl'),chunk);tail+=chunk;while(tail.includes('\n')){const i=tail.indexOf('\n'),line=tail.slice(0,i);tail=tail.slice(i+1);try{const event=JSON.parse(line);events.push(event);if(event.type==='agent_settled')resolveSettled()}catch{}}});
parent.stderr.on('data',chunk=>fs.appendFileSync(path.join(evidence,'parent-stderr.log'),chunk));
parent.on('error',rejectSettled);
const exit=new Promise(resolve=>parent.once('close',(code,signal)=>resolve({code,signal})));
try{
 parent.stdin.write(JSON.stringify({id:'proof',type:'prompt',message:'Run private deterministic write-scope proof.'})+'\n');
 await settled;if(failure)throw failure;
 const observations=[];
 if(handle?.id){
   const run=path.join(runs,handle.id);fs.cpSync(run,path.join(evidence,'run'),{recursive:true});
   const record=JSON.parse(fs.readFileSync(path.join(run,'status.json'),'utf8'));assert.equal(record.fabricRelease,root);assert.equal(record.status,'completed');assert.equal(record.usage.cost,0);
   const childEvents=fs.readFileSync(path.join(run,'events.jsonl'),'utf8').trim().split('\n').map(line=>JSON.parse(line));
   for(const call of calls){const result=childEvents.find(e=>e.type==='tool_execution_end'&&e.toolCallId===call.id);observations.push({kind:call.id,path:call.args.path,result,...snapshots.find(s=>s.kind===call.id)})}
 }
 // Search every private probe file for the unique bytes, including possible remaps.
 const bytesAt=[];const walk=dir=>{for(const entry of fs.readdirSync(dir,{withFileTypes:true})){const file=path.join(dir,entry.name);if(entry.isDirectory())walk(file);else if(entry.isFile()&&fs.statSync(file).size<1024*1024&&fs.readFileSync(file).includes(Buffer.from(marker)))bytesAt.push(file)}};walk(scratch);
 const summary={root,unconfinedControl,spawn:handle,terminal,observations,bytesAt,childRequests:childCount,cost:0};json(path.join(evidence,'result.json'),summary);
 console.log(JSON.stringify({root,spawn:handle,observations:observations.map(o=>({kind:o.kind,isError:o.result?.isError,text:o.result?.result?.content,exists:o.exists,content:o.content})),bytesAt},null,2));
 if(expectConfined){
   assert(handle?.id,'Confined task must start');
   assert.equal(observations.length,calls.length);
   for(const o of observations){
     const denied=/-(outside|missingOutside|symlink)$/.test(o.kind);
     assert.equal(o.result?.isError,denied,`Wrong receipt for ${o.kind}`);
     if(denied){
       const text=o.result.result.content.filter(c=>c.type==='text').map(c=>c.text).join('\n');
       assert(text.includes(`allowed roots: ${JSON.stringify([cwd,tmp].sort())}`),`Allowed roots missing: ${o.kind}`);
       assert(text.includes(`refused path: ${JSON.stringify(o.path)}`),`Refused path missing: ${o.kind}`);
       assert.equal(o.content,o.kind.endsWith('missingOutside')?null:'original');
     }else assert.equal(o.content,marker+(o.kind.startsWith('edit-')?' edited ':' ')+o.kind.split('-')[1]);
   }
   assert(!fs.existsSync(path.join(scratch,'absent')),'Denied write created its absent parent');
   assert.deepEqual(terminal.writeReceipt.paths,[targets.missingInside,targets.tmp]);
   console.log('TASK_WRITE_SCOPE: PASS (10 native calls; denied receipts agree with disk; 2 valid roots; zero cost)');
 }
}finally{
 clearTimeout(deadline);parent.stdin.end();const timer=setTimeout(()=>parent.kill('SIGTERM'),10000);const nativeExit=await exit;clearTimeout(timer);json(path.join(evidence,'native-exit.json'),nativeExit);await new Promise(resolve=>server.close(resolve));
 assert.equal(nativeExit.code,0,'Private native Pi Main must exit cleanly');
}
