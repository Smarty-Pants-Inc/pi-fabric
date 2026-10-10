// Opt-in native actor/task return proof. Build first. Only inference is synthetic.
// Usage: nice -n 19 node scripts/probe-actor-nested-return-cli.mjs OUTPUT
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const out = path.resolve(process.argv[2] ?? process.env.TASK_OUT);
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'native-nested-return-'));
const profile = path.join(scratch, 'profile'), cwd = path.join(scratch, 'workspace'), log = path.join(out, 'native-proof.jsonl');
for (const dir of [profile, cwd, path.join(scratch, 'home'), path.join(scratch, 'tmp')]) fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(log, '');
const record = value => fs.appendFileSync(log, JSON.stringify({ at: Date.now(), ...value }) + '\n');
const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
const candidate = path.join(repo, 'dist/index.js'), fixture = path.join(repo, 'tests/fixtures/actor-nested-return-cli-provider.ts');
const head = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repo, encoding: 'utf8' }).trim();
const diffHash = createHash('sha256').update(execFileSync('git', ['diff', 'HEAD'], { cwd: repo })).digest('hex');
record({ type: 'revision', head, diffHash, candidate, sha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'), cli, piVersion: execFileSync(process.execPath, [cli, '--version'], { encoding: 'utf8' }).trim(), nice: os.getPriority(0), inferenceOnlySynthetic: true });
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ extensions: [fixture], compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true, executor: { kernel: 'typescript', timeoutMs: 120000, maxTimeoutMs: 120000, mainMaxTimeoutMs: 120000 },
  agents: { enabled: true, model: 'nested-proof/offline', recursive: true, transport: 'process', maxDepth: 4, maxConcurrent: 6, maxPerExecution: 40, maxTokensPerChild: 0, nice: 19, timeoutMs: 120000, budgetUsd: 0, retainRuns: true },
  mesh: { enabled: true, persist: true, root: path.join(scratch, 'mesh'), actorPollMs: 20 }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' }, mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, records: { enabled: false } }));
const env = { PATH: process.env.PATH, HOME: path.join(scratch, 'home'), TMPDIR: path.join(scratch, 'tmp'), PI_CODING_AGENT_DIR: profile, PI_OFFLINE: '1',
  PI_FABRIC_PI_BINARY: cli, PI_FABRIC_RUN_ROOT: path.join(scratch, 'runs'), NESTED_PROOF_LOG: log, NESTED_PROOF_SCRATCH: scratch };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (predicate, label, ms = 90000) => { const deadline = Date.now() + ms; while (!predicate()) { assert(Date.now() < deadline, label); await sleep(25); } };
// pi.write may expose the marker before its contents. Observe complete JSON, not existence.
const readWhenReady = async (file, label) => {
  let value;
  await wait(() => {
    try { value = JSON.parse(fs.readFileSync(file, 'utf8')); return true; }
    catch (error) { if (error.code === 'ENOENT' || error instanceof SyntaxError) return false; throw error; }
  }, label);
  return value;
};
const workers = []; let generation = 0;
function launch(sessionFile) {
  const args = [cli, '--mode', 'rpc', '--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '--no-themes', '--approve', '-e', candidate,
    '--provider', 'nested-proof', '--model', 'offline', '--thinking', 'off', '--tools', 'fabric_exec', ...(sessionFile ? ['--session', sessionFile] : ['--session-dir', path.join(profile, 'sessions')])];
  record({ type: 'command', executable: process.execPath, args, cwd, env, generation: ++generation });
  const child = spawn(process.execPath, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
  const events = [], pending = new Map(); let buffer = '', seq = 0, stderr = '';
  const exit = new Promise(resolve => child.once('exit', (code, signal) => { record({ type: 'process-exit', generation, pid: child.pid, code, signal }); resolve({ code, signal }); }));
  child.stdout.setEncoding('utf8'); child.stdout.on('data', chunk => { buffer += chunk; let pos; while ((pos = buffer.indexOf('\n')) >= 0) {
    const line = buffer.slice(0, pos); buffer = buffer.slice(pos + 1); if (!line.trim()) continue;
    let event; try { event = JSON.parse(line); } catch { record({ type: 'non-json-output', line }); continue; }
    events.push(event); record({ type: 'rpc-event', pid: child.pid, event });
    if (event.type === 'response') { const request = pending.get(event.id); pending.delete(event.id); event.success ? request?.resolve(event.data) : request?.reject(new Error(event.error)); }
  } });
  child.stderr.on('data', data => { stderr += data.toString(); record({ type: 'stderr', pid: child.pid, text: data.toString() }); });
  const request = command => new Promise((resolve, reject) => { const id = 'proof-' + (++seq); const timer = setTimeout(() => { pending.delete(id); reject(new Error('RPC timeout ' + command.type + ': ' + stderr.slice(-2000))); }, 120000);
    pending.set(id, { resolve: value => { clearTimeout(timer); resolve(value); }, reject: error => { clearTimeout(timer); reject(error); } }); record({ type: 'rpc-command', pid: child.pid, command: { id, ...command } }); child.stdin.write(JSON.stringify({ id, ...command }) + '\n'); });
  const close = async () => { if (child.exitCode === null && child.signalCode === null) child.stdin.end(); const timer = setTimeout(() => child.kill('SIGKILL'), 90000); const result = await exit; clearTimeout(timer); assert.equal(result.code, 0, stderr); };
  const worker = { child, events, request, close }; workers.push(worker); return worker;
}
let main; const actors = [], cases = [];
const guest = async code => { const before = main.events.length; await main.request({ type: 'prompt', message: 'NESTED_MAIN_CODE:\n' + code });
  await wait(() => main.events.slice(before).some(e => e.type === 'agent_end'), 'Main tool turn did not settle', 120000);
  const result = main.events.slice(before).find(e => e.type === 'tool_execution_end' && e.toolName === 'fabric_exec');
  assert(result && !result.isError && result.result.details?.success, JSON.stringify(result));
  return JSON.parse(result.result.content.filter(p => p.type === 'text').map(p => p.text).join('\n')); };
try {
  main = launch(); await main.request({type:'get_state'});
  const root = await guest('return await agents.main();');
  const actor = await guest(`const actor = await agents.create({name:"native-nested-return",instructions:"Native nested task return proof",model:"nested-proof/offline",delivery:"mailbox",triggerTurn:false,responseMode:"text",transport:"process"}); await agents.tell({id:actor.id,message:"BEGIN_NESTED_RETURN"}); return actor;`);
  actors.push(actor);
  const fencedFile = path.join(scratch,'actor-fenced.json');
  const fenced = await readWhenReady(fencedFile,'actor did not spawn a task');
  assert.equal(fenced.owner.id,root.id,'actor agents.main no longer discovers the owning Main');
  assert.equal(fenced.self.id,actor.id);
  const leafFile = path.join(scratch,'leaf-result.json');
  const leaf = await readWhenReady(leafFile,'task slice did not reach actor');
  assert.equal(leaf.owner.id,actor.id,'task return target is not immediate actor spawner');
  assert.equal(leaf.spawner.id,actor.id);
  const completionDir = path.join(path.dirname(actor.sessionFile),'child-completions');
  await wait(()=>fs.existsSync(path.join(completionDir,fenced.child.id+'.json')),'child result was not archived for the actor');
  const archive = JSON.parse(fs.readFileSync(path.join(completionDir,fenced.child.id+'.result.json'),'utf8'));
  assert.equal(archive.status,'completed'); assert.equal(archive.spawner.id,actor.id);
  const status = await guest(`return await agents.status({id:${JSON.stringify(actor.id)}});`);
  assert(status.inFlightRun?.id);
  const stopped = await guest(`const result = await agents.stop({id:${JSON.stringify(status.inFlightRun.id)}}); return {id:result.id,status:result.status};`);
  assert.equal(stopped.status,'stopped');
  let history;
  await wait(()=>fs.existsSync(path.join(completionDir,fenced.child.id+'.receipt')),'child completion not handed to mailbox');
  history = await guest(`return await agents.messages({id:${JSON.stringify(actor.id)},limit:200});`);
  assert.equal(history.filter(m=>m.id===fenced.child.id&&m.direction==='in').length,1,'task completion missing or duplicated in actor mailbox');
  assert(history.some(m=>m.direction==='in'&&m.data?.message?.includes('NESTED_SLICE_TO_SPAWNER')),'task slice missing from actor mailbox');
  const mainMessages = await main.request({type:'get_messages'});
  const deliveries = (mainMessages.messages??[]).filter(m=>m.customType);
  assert.equal(deliveries.filter(m=>JSON.stringify(m).includes('NESTED_SLICE_TO_SPAWNER')).length,0,'Main received task slice');
  assert.equal(deliveries.filter(m=>m.customType==='pi-fabric-agent-complete'&&(m.details?.ids??[]).includes(fenced.child.id)).length,0,'Main received child completion');
  const nested = {row:'nested-task-return',passed:true,rootId:root.id,actorId:actor.id,taskId:fenced.child.id,taskReturnId:leaf.owner.id,spawner:archive.spawner,actorCompletionCount:1,actorSliceCount:history.filter(m=>m.direction==='in'&&m.data?.message?.includes('NESTED_SLICE_TO_SPAWNER')).length,mainSliceCount:0,mainCompletionCount:0};
  cases.push(nested); record({type:'case-pass',...nested});
  await guest(`return await agents.ask({id:${JSON.stringify(actor.id)},message:"CHECK_OWNING_MAIN"});`);
  const guard = await readWhenReady(path.join(scratch,'owner-guard.json'),'actor did not finish owning-Main guard');
  assert.equal(guard.owner.id,root.id); assert.notEqual(guard.owner.id,actor.id);
  assert(guard.byId.queued&&guard.byAlias.queued);
  const delivered = await main.request({type:'get_messages'});
  const steers=(delivered.messages??[]).filter(m=>m.customType==='pi-fabric-agent-message');
  assert.equal(steers.filter(m=>m.content?.includes('ACTOR_OWNER_STEER_BY_ID')).length,1);
  assert.equal(steers.filter(m=>m.content?.includes('ACTOR_OWNER_STEER_BY_ALIAS')).length,1);
  const owningMain={row:'owning-main-steer',passed:true,rootId:root.id,actorId:actor.id,actorMainId:guard.owner.id,steerByIdCount:1,steerByAliasCount:1};
  cases.push(owningMain); record({type:'case-pass',...owningMain});
  fs.writeFileSync(path.join(out,'candidate-identity.json'),JSON.stringify({head,candidate,cli,root,actor,inferenceOnlySynthetic:true},null,2));
  fs.writeFileSync(path.join(out,'summary.json'),JSON.stringify({passed:true,cases},null,2));
  record({type:'proof-pass',cases}); console.log(JSON.stringify({passed:true,cases},null,2));
} catch (error) {
  record({type:'proof-error',message:error.message,stack:error.stack});
  console.error(error); throw error;
} finally {
  if (main?.child.exitCode === null && main?.child.signalCode === null) {
    for (const actor of actors) await guest(`return await agents.stop({id:${JSON.stringify(actor.id)}});`).catch(error=>record({type:'cleanup-error',message:error.message}));
  }
  for (const worker of workers) await worker.close();
  for (const name of ['runs','mesh','profile']) if(fs.existsSync(path.join(scratch,name))) fs.cpSync(path.join(scratch,name),path.join(out,'native-'+name),{recursive:true});
  fs.rmSync(scratch,{recursive:true,force:true});
}
