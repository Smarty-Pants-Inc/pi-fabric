#!/usr/bin/env node
// Real installed Pi SDK + built extension. No model calls, SSH, or real credentials.
// Run with a clean environment: node SCRIPT PI_PACKAGE_DIR OUTPUT_DIR.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [piPackage, outputDirectory] = process.argv.slice(2);
assert(piPackage && outputDirectory, "usage: probe-process-placement.mjs PI_PACKAGE_DIR OUTPUT_DIR");
process.umask(0o077);
const root = path.resolve(outputDirectory);
fs.mkdirSync(root, {recursive:true,mode:0o700});
fs.chmodSync(root,0o700);
const profile = path.join(root, "profile");
const cwd = path.join(root, "cwd");
fs.mkdirSync(profile, {recursive:true}); fs.mkdirSync(cwd, {recursive:true});
process.env.HOME = root;
process.env.PI_CODING_AGENT_DIR = profile;
process.env.PI_FABRIC_TMPDIR = path.join(root,"tmp");
process.env.PI_FABRIC_RUNS_ROOT = path.join(root,"runs");
process.env.PI_OFFLINE = "1";
process.env.PI_FABRIC_ROLE = "main";
const extension = path.resolve("dist/index.js");
assert(fs.existsSync(extension), "fresh built extension is required");
assert(fs.existsSync(path.join(piPackage,"dist/index.js")), "installed Pi SDK is required");
const results = path.join(root, "results");
const launcher = path.join(root,"fake-smarty-task-ryzen2.mjs");
fs.writeFileSync(launcher, `import fs from 'node:fs'; import path from 'node:path';
const [mode, root, id, ...argv]=process.argv.slice(2); const dir=path.join(root,id); fs.mkdirSync(dir,{recursive:true});
if(mode==='launch') {
 fs.writeFileSync(path.join(dir,'argv.json'),JSON.stringify(argv));
 fs.writeFileSync(path.join(dir,'result.md'),'PLACEMENT_PROOF_RESULT: '+argv.at(-1));
 console.log('RYZEN2_TASK_ACCEPTED '+id+' on ryzen2 (unit smarty-task-'+id+').');
} else if(mode==='poll') {
 // Simulate native completion after launch without any detached process.
 fs.writeFileSync(path.join(dir,'rc'),'0');
 console.log(JSON.stringify({rc:0,text:fs.readFileSync(path.join(dir,'result.md'),'utf8')}));
} else if(mode==='cancel') { fs.writeFileSync(path.join(dir,'rc'),'143'); }
`);
const placement = {
 default:"remote", capabilities:[],
 command:[process.execPath,launcher,"launch",results,"{id}","--host","auto","--minutes","{minutes}","--cwd","{cwd}","--model","{model}","--thinking","{thinking}","--","{task}"],
 resultCommand:[process.execPath,launcher,"poll",results,"{id}"],
 cancelCommand:[process.execPath,launcher,"cancel",results,"{id}"], pollIntervalMs:10, commandTimeoutMs:1000,
};
fs.writeFileSync(path.join(profile,"fabric.json"),JSON.stringify({
 executor:{kernel:"typescript",fullCodeMode:true}, mcp:{enabled:false},ui:{enabled:false},
 mesh:{enabled:false,root:path.join(root,"mesh")},
 agents:{placement,timeoutMs:10000,sessionExport:false,retainRuns:true,notifyOnComplete:false},
}));
const {createAgentSession,DefaultResourceLoader,ModelRuntime,SettingsManager,SessionManager} = await import(pathToFileURL(path.join(piPackage,"dist/index.js")).href);
const runtime = await ModelRuntime.create({authPath:path.join(profile,"empty-auth.json"),modelsPath:null,refreshOnCreate:false,allowModelNetwork:false});
// A public dummy literal makes the metadata-only model admissible. It is not a
// credential and is never written to a credential store or sent over a network.
runtime.registerProvider("placement-proof", {
 api:"openai-completions",baseUrl:"http://invalid.invalid",apiKey:"offline-proof-not-a-credential",
 models:[{id:"probe",name:"Placement offline probe",reasoning:false,input:["text"],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:100000,maxTokens:1000}],
});
const settingsManager = SettingsManager.inMemory({packages:[],extensions:[],skills:[],promptTemplates:[],themes:[]});
const loader = new DefaultResourceLoader({cwd,agentDir:profile,settingsManager,additionalExtensionPaths:[extension],noSkills:true,noPromptTemplates:true,noThemes:true});
await loader.reload();
const load = loader.getExtensions();
assert.equal(load.errors.length,0,JSON.stringify(load.errors));
const {session} = await createAgentSession({cwd,agentDir:profile,modelRuntime:runtime,model:runtime.getModel("placement-proof","probe"),resourceLoader:loader,settingsManager,sessionManager:SessionManager.inMemory(cwd)});
try {
 await session.bindExtensions({mode:"print",onError:error=>{throw new Error(JSON.stringify(error));}});
 const tool = session.agent.state.tools.find(tool=>tool.name==="fabric_exec");
 assert(tool,"installed Pi must register built fabric_exec");
 const result = await tool.execute("placement-proof-call", {
   code:'const h = await agents.spawn({task: "installed Pi remote task", transport: "process", thinking: "high"}); const result = await agents.wait({id: h.id}); return {handle: h, result};',
   resultFormat:"json",
 }, new AbortController().signal);
 fs.writeFileSync(path.join(root,"fabric-exec-result.json"),JSON.stringify(result,null,2));
 const text = result.content.filter(block=>block.type==="text").map(block=>block.text).join("\n");
 assert(text.includes("PLACEMENT_PROOF_RESULT: installed Pi remote task"),text);
 assert(text.includes('"completed"'),text);
 const entries = fs.readdirSync(results);
 assert.equal(entries.length,1,"exactly one remote launch, no local worker");
 const id=entries[0];
 assert(fs.existsSync(path.join(results,id,"rc")),"native completion marker must arrive");
 const argv=JSON.parse(fs.readFileSync(path.join(results,id,"argv.json"),"utf8"));
 assert.equal(argv.at(-1),"installed Pi remote task");
 const evidence={piPackage,extension,isolatedAgentDir:profile,id,argv,resultText:text,passed:true};
 fs.writeFileSync(path.join(root,"evidence.json"),JSON.stringify(evidence,null,2));
 console.log(JSON.stringify({passed:true,id,result:"PLACEMENT_PROOF_RESULT: installed Pi remote task",evidence:path.join(root,"evidence.json")}));
} finally {
 await session.extensionRunner.emit({type:"session_shutdown",reason:"exit"});
 session.dispose();
}
