// Real installed Pi RPC proof; only Jev fetch and loopback model inference are mocked.
// nice -n 19 node scripts/prove-model-route-live.mjs INSTALLED_PI_CLI $TMPDIR/proof $TASK_OUT/real-cli
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { prepareProofPaths, processStartTime, registerOwnedProcess, ownedProcessAlive, signalOwnedProcess } from './proof-process-ownership.mjs';
const [cli, scratchArg, outArg] = process.argv.slice(2);
assert(cli && scratchArg && outArg, 'INSTALLED_PI_CLI SCRATCH OUTPUT required');
const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const scratch = path.resolve(scratchArg), out = path.resolve(outArg), candidate = path.join(lane, 'dist/index.js');
prepareProofPaths(scratch, out);
const cwd = path.join(scratch, 'workspace'), home = path.join(scratch, 'home'), profile = path.join(scratch, 'profile'), mesh = path.join(scratch, 'mesh');
for (const directory of [cwd, home, profile]) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
const transcript = path.join(out, 'transcript.jsonl'), queue = path.join(scratch, 'queue.json'), failJev = path.join(scratch, 'fail-jev'), fullJournals = path.join(scratch, 'full-journals');
fs.writeFileSync(transcript, ''); fs.writeFileSync(queue, '[]');
const record = row => fs.appendFileSync(transcript, JSON.stringify(row) + '\n');
const pin = 'router-proof/gpt-5-pin', cheap = 'router-proof/gpt-5-cheap';
const requests = [];
let archiveActor, archiveFailureDirectory, failedResponse, stoppedResponse;
const mainExits = [], safetyScenarios = [];
const server = http.createServer(async (req, res) => {
  try {
    let text = ''; for await (const chunk of req) text += chunk;
    const body = JSON.parse(text), routeHeader = req.headers['x-smarty-route'] ?? null;
    const row = { type: 'provider_http_request', model: body.model, effort: body.reasoning_effort ?? null,
      routeHeader, url: req.url, messages: body.messages };
    requests.push(row); record(row);
    const prompt = JSON.stringify(body.messages.filter(message => message.role === 'user').at(-1));
    if (routeHeader && prompt.includes('PROOF_ARCHIVE_FAIL_A')) {
      const decisionId = routeHeader.split(':').at(-1);
      const decision = ledger().find(row => row.type === 'decision' && row.decisionId === decisionId);
      assert.equal(decision.actorId, archiveActor.id);
      archiveFailureDirectory = path.join(archiveActor.logDir, decision.runId);
      fs.mkdirSync(archiveFailureDirectory, { recursive: true, mode: 0o700 }); fs.chmodSync(archiveFailureDirectory, 0o500);
      record({type:'injected_archive_failure',runId:decision.runId,directory:archiveFailureDirectory});
    }
    if (routeHeader && prompt.includes('PROOF_TERMINAL_FAILED')) { failedResponse = res; return; }
    if (routeHeader && prompt.includes('PROOF_TERMINAL_STOPPED')) { stoppedResponse = res; return; }
    const specs = JSON.parse(fs.readFileSync(queue, 'utf8'));
    const spec = !routeHeader ? specs.shift() : undefined;
    if (spec) fs.writeFileSync(queue, JSON.stringify(specs));
    const content = routeHeader ? `OFFLINE_EXECUTION ${body.model} effort=${body.reasoning_effort}` : 'Main proof tool completed';
    const delta = spec ? { role: 'assistant', tool_calls: [{ index: 0, id: spec.id, type: 'function', function: {
      name: 'fabric_exec', arguments: JSON.stringify({ code: spec.code, resultFormat: 'json' }) } }] } : { role: 'assistant', content };
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    const chunk = (delta, finish_reason = null) => ({ id: 'chatcmpl-proof', object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000),
      model: body.model, choices: [{ index: 0, delta, finish_reason }] });
    res.write('data: ' + JSON.stringify(chunk(delta)) + '\n\n');
    res.write('data: ' + JSON.stringify(chunk({}, spec ? 'tool_calls' : 'stop')) + '\n\n');
    res.write('data: ' + JSON.stringify({ ...chunk({}), choices: [], usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } }) + '\n\n');
    res.end('data: [DONE]\n\n');
  } catch (error) { record({ type: 'mock_provider_error', error: String(error) }); res.writeHead(500); res.end(String(error)); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const endpoint = `http://127.0.0.1:${server.address().port}/v1`;
const preload = path.join(scratch, 'jev-boundary.mjs');
fs.writeFileSync(preload, `
import fs from 'node:fs';
const open = fs.openSync, close = fs.closeSync, write = fs.writeFileSync;
const journalFds = new Map();
fs.openSync = (file, ...args) => {
  const fd = open(file, ...args);
  if (typeof file === 'string' && /model-routing-(pending|refused)\\.jsonl$/.test(file)) journalFds.set(fd, file);
  return fd;
};
fs.closeSync = fd => { journalFds.delete(fd); return close(fd); };
fs.writeFileSync = (file, ...args) => {
  if (typeof file === 'number' && journalFds.has(file) && fs.existsSync(process.env.ROUTER_PROOF_FULL_JOURNALS) && fs.fstatSync(file).size >= 8192) {
    fs.appendFileSync(process.env.ROUTER_PROOF_TRANSCRIPT, JSON.stringify({type:'injected_journal_append_efbig',pid:process.pid,journal:journalFds.get(file),record:JSON.parse(String(args[0]))})+'\\n');
    throw Object.assign(new Error('EFBIG: offline full journal append'), {code:'EFBIG'});
  }
  return write(file, ...args);
};
const original = globalThis.fetch;
const boundaryFetch = async (url, options) => {
  const address = typeof url === 'string' ? url : (url.url ?? url.href ?? String(url));
  if (address === 'https://api.typesafe.ai/v1/systemone') {
    const body = JSON.parse(options.body);
    fs.appendFileSync(process.env.ROUTER_PROOF_TRANSCRIPT, JSON.stringify({type:'jev_boundary_request',pid:process.pid,state:body.state,questions:body.questions})+'\\n');
    if (fs.existsSync(process.env.ROUTER_PROOF_FAIL_JEV)) throw new Error('offline injected provider-boundary failure');
    return new Response(JSON.stringify({model:'jev-1.13.0',answers:{route:{type:'choice',choice:'candidate-1',confidence:.95,probabilities:{'candidate-0':.05,'candidate-1':.95}}},usage:{input_tokens:1,output_tokens:1}}), {status:200});
  }
  if (address.startsWith(process.env.ROUTER_PROOF_ENDPOINT + '/')) return original(url, options);
  fs.appendFileSync(process.env.ROUTER_PROOF_TRANSCRIPT, JSON.stringify({type:'blocked_external_network',address})+'\\n');
  throw new Error('Offline proof forbids external network');
};
// setupCli installs undici globals. Keep this process-scoped provider-boundary
// mock authoritative even when the runtime replaces its fetch implementation.
Object.defineProperty(globalThis, 'fetch', {configurable:true, get:()=>boundaryFetch, set:()=>{}});
`);
const config = { autoReload: false, fullCodeMode: true, prewalk: { enabled: false }, components: [], mcp: { enabled: false },
  memory: { enabled: false }, records: { enabled: false }, ui: { enabled: false },
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow', network: 'allow' },
  executor: { kernel: 'typescript', timeoutMs: 120000 }, jev: { credentialCommand: [] },
  agents: { model: pin, thinking: 'high', nice: 19, transport: 'process', timeoutMs: 45000, budgetUsd: 0, maxConcurrent: 4,
    notifyOnComplete: false, retainRuns: false, modelRouting: { pinModel: pin, pinThinking: 'high',
      shadowCandidates: [{ model: cheap, effort: 'medium' }], liveClasses: ['status-groom', 'task:exact-checks'] } },
  mesh: { enabled: true, persist: true, root: mesh, actorPollMs: 20 } };
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify(config));
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ extensions: [path.join(lane, 'tests/fixtures/model-route-live-cli-provider.ts')],
  enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false }, defaultProjectTrust: 'approve' }));
const env = { PATH: process.env.PATH, HOME: home, TMPDIR: scratch, PI_CODING_AGENT_DIR: profile, PI_OFFLINE: '1',
  PI_FABRIC_PI_BINARY: path.resolve(cli), PI_FABRIC_MESH_ROOT: mesh, PI_FABRIC_RUN_ROOT: path.join(scratch, 'runs'),
  NODE_OPTIONS: '--import=' + preload, TYPESAFE_API_KEY: 'offline-proof-not-a-credential',
  ROUTER_PROOF_ENDPOINT: endpoint, ROUTER_PROOF_TRANSCRIPT: transcript, ROUTER_PROOF_FAIL_JEV: failJev, ROUTER_PROOF_FULL_JOURNALS: fullJournals };
const args = [path.resolve(cli), '--mode', 'rpc', '--offline', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files', '--approve',
  '--provider', 'router-proof', '--model', 'gpt-5-pin', '--thinking', 'high', '-e', candidate, '--tools', 'fabric_exec', '--session-dir', path.join(scratch, 'sessions')];
const head = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: lane, encoding: 'utf8' }).trim();
fs.writeFileSync(path.join(out, 'candidate-identity.json'), JSON.stringify({ head, candidate, installedCli: path.resolve(cli),
  candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'),
  piVersion: execFileSync(process.execPath, [cli, '--version'], { env, encoding: 'utf8' }).trim(),
  dirtyTrackedFiles: execFileSync('git', ['status', '--porcelain', '--untracked-files=no'], {cwd:lane,encoding:'utf8'}).trim() }, null, 2));
record({ type: 'proof_command', executable: process.execPath, args, cwd, env: { ...env, TYPESAFE_API_KEY: '<offline fixture sentinel>' }, head,
  candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'),
  piVersion: execFileSync(process.execPath, [cli, '--version'], { env, encoding: 'utf8' }).trim(), config });
let child, exited;
const events = [], pending = new Map(); let buffer = '', serial = 0, stderr = '', failure, proof;
const ownedPids = new Map();
function launchMain() {
  child = spawn(process.execPath, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
  const instance = child;
  registerOwnedProcess(ownedPids, instance.pid, processStartTime(instance.pid));
  exited = new Promise(resolve => instance.once('close', (code, signal) => resolve({ code, signal, pid: instance.pid })));
child.stdout.setEncoding('utf8'); child.stderr.setEncoding('utf8');
child.stderr.on('data', text => { stderr += text; record({ type: 'stderr', text }); });
child.stdout.on('data', text => {
  buffer += text;
  for (;;) {
    const newline = buffer.indexOf('\n'); if (newline < 0) break;
    const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1);
    let event; try { event = JSON.parse(line); } catch { record({ type: 'non_json_stdout', line }); continue; }
    events.push(event); record({ type: 'main_rpc', event });
    if (event.type === 'response' && pending.has(event.id)) {
      const waiter = pending.get(event.id); pending.delete(event.id); clearTimeout(waiter.timer);
      event.success ? waiter.resolve(event.data) : waiter.reject(new Error(event.error));
    }
  }
});
}
launchMain();
const request = frame => new Promise((resolve, reject) => {
  const id = 'rpc-' + (++serial), timer = setTimeout(() => { pending.delete(id); reject(new Error('RPC deadline: ' + frame.type)); }, 90000);
  pending.set(id, { resolve, reject, timer }); record({ type: 'rpc_command', ...frame, id }); child.stdin.write(JSON.stringify({ ...frame, id }) + '\n');
});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (check, timeout = 90000) => { const until = Date.now() + timeout; while (!check()) {
  assert.equal(child.exitCode, null, 'Pi unexpectedly exited: ' + stderr); assert.ok(Date.now() < until, 'Proof observation deadline: ' + stderr); await sleep(25);
} };
async function guest(code, expectedResidentFailure = false) {
  const before = events.length, id = 'guest-' + (++serial);
  fs.writeFileSync(queue, JSON.stringify([{ id, code }])); record({ type: 'public_fabric_exec', id, code });
  await request({ type: 'prompt', message: 'Execute ' + id });
  await wait(() => events.slice(before).some(event => event.type === 'agent_settled'));
  const event = events.slice(before).find(event => event.type === 'tool_execution_end' && event.toolCallId === id);
  assert.ok(event && !event.isError && event.result.details.success, JSON.stringify(event ?? events.slice(before)));
  let text = event.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n');
  if (expectedResidentFailure) {
    assert.match(text, /^ResidentOutcomeUnknownError:/); assert.match(text, /EACCES|EFBIG/);
    const resultStart = text.indexOf('\n\n{'); assert.ok(resultStart >= 0, text); text = text.slice(resultStart + 2);
  }
  const result = JSON.parse(text);
  record({ type: 'public_result', id, result }); return result;
}
const files = dir => !fs.existsSync(dir) ? [] : fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
  const file = path.join(dir, entry.name); return entry.isDirectory() ? files(file) : [file];
});
const json = file => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } };
const owners = () => files(mesh).filter(file => path.basename(file) === 'owner.json').map(file => ({ file, owner: json(file) })).filter(row => row.owner?.pid);
const live = pid => { try { const text = fs.readFileSync(`/proc/${pid}/stat`, 'utf8'); return !['Z', 'X'].includes(text.slice(text.lastIndexOf(')') + 2).split(' ')[0]); } catch { return false; } };
const ledgerPath = path.join(profile, 'fabric/model-routing.jsonl');
const ledger = () => fs.readFileSync(ledgerPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
try {
  const state = await request({ type: 'get_state' }); assert.equal(state.model.provider + '/' + state.model.id, pin);
  const diagnostic = await guest(`try { return { evaluation: await tools.call({ref: 'jev.evaluate', args: {state: {routeClass: 'task:exact-checks', mode: 'shadow', protection: 'clear'}, questions: {route: {type: 'choice', instructions: 'Offline proof', criteria: {'candidate-0': {model: '${pin}', effort: 'high'}, 'candidate-1': {model: '${cheap}', effort: 'medium'}}}}}}) }; } catch(error) { return {error: String(error), stack: error.stack}; }`); record({type:'jev_diagnostic',diagnostic}); assert.ok(diagnostic.evaluation && !diagnostic.error, JSON.stringify(diagnostic));
  const task = await guest(`const handle = await agents.spawn({ task: 'PROOF_TASK_LIVE', modelReason: 'Round 8 task exception', model: 'auto', routeClass: 'task:exact-checks', protected: false }); const result = await agents.wait({ id: handle.id }); return {handle,result};`);
  assert.equal(task.handle.routeDecision.reasonCode, 'live-choice'); assert.equal(task.result.model, cheap); assert.equal(task.result.thinking, 'medium'); assert.equal(task.result.modelReason, 'Round 8 task exception');
  const actor = await guest(`const actor = await agents.create({ name: 'status-groom-proof', modelReason: 'Round 8 actor exception', instructions: 'Bounded status checks only', residency: 'durable', runner: 'pi', transport: 'process', model: '${pin}', thinking: 'high', routeClass: 'status-groom', protected: false, tools: [], extensions: true, events: [], topics: [], delivery: 'mailbox', triggerTurn: false }); const first = await agents.ask({ id: actor.id, message: 'PROOF_ACTOR_LIVE' }); return {actor,first};`);
  assert.ok(actor.first.runId); assert.match(actor.first.text, /gpt-5-cheap/);
  await wait(() => fs.existsSync(path.join(actor.actor.logDir, actor.first.runId, 'route-dispatch-receipt.json')));
  const nativeFirst = fs.readFileSync(transcript, 'utf8').trim().split('\n').map(line => JSON.parse(line)).find(row => row.type === 'native_activation' && row.runId === actor.first.runId);
  assert.ok(nativeFirst?.runDirectory, 'No native resident activation process');
  await wait(() => !fs.existsSync(nativeFirst.runDirectory) && !live(nativeFirst.pid));
  record({type:'cleaned_resident_activation', nativeFirst, archivedReceipt: path.join(actor.actor.logDir, actor.first.runId, 'route-dispatch-receipt.json')});
  const residentOwner = owners(); assert.ok(residentOwner.length > 0, 'No real resident owner');
  const stillLive = await guest(`const h = await agents.spawn({ task: 'PROOF_TASK_OTHER_CLASS', model: 'auto', routeClass: 'task:exact-checks', protected: false }); return await agents.wait({id:h.id});`);
  assert.equal(stillLive.model, cheap);
  fs.writeFileSync(failJev, 'inject only a Jev provider-boundary failure');
  const fallback = await guest(`const h = await agents.spawn({ task: 'PROOF_TASK_ERROR_FALLBACK', model: 'auto', routeClass: 'task:exact-checks', protected: false }); return await agents.wait({id:h.id});`);
  assert.equal(fallback.model, pin); fs.rmSync(failJev);
  assert.ok(!fs.readFileSync(transcript, 'utf8').includes('blocked_external_network'), 'Proof attempted external networking');
  assert.equal(owners()[0].owner.token, residentOwner[0].owner.token);
  const restartMain = async reset => {
    // Reopen the same native Main session: a foreign root cannot publish policy
    // to the existing resident. Keep F4 on the owning-Main refresh path.
    const mainSession = fs.readFileSync(transcript, 'utf8').trim().split('\n').map(line=>JSON.parse(line))
      .find(row=>row.type==='native_activation' && row.pid===child.pid && row.actorId===null && row.sessionFile);
    assert.ok(mainSession?.sessionFile && fs.existsSync(mainSession.sessionFile));
    if (!args.includes('--session')) args.push('--session',mainSession.sessionFile);
    child.stdin.end();
    const exit = await exited; mainExits.push(exit); assert.equal(exit.code, 0);
    if (reset && config.agents.modelRouting) config.agents.modelRouting.revertReset = { 'status-groom': reset };
    fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify(config));
    record({type:'fresh_main',reset:config.agents.modelRouting?.revertReset ?? null,liveClasses:config.agents.modelRouting?.liveClasses ?? null,pid:exit.pid,args,sessionId:mainSession.sessionId});
    buffer = ''; launchMain();
    const state = await request({type:'get_state'}); assert.equal(state.model.provider + '/' + state.model.id, pin);
  };
  const createActor = async name => guest(`return await agents.create({ name: '${name}', instructions: 'Bounded status checks only', residency: 'durable', runner: 'pi', transport: 'process', model: '${pin}', thinking: 'high', routeClass: 'status-groom', protected: false, tools: [], extensions: true, events: [], topics: [], delivery: 'mailbox', triggerTurn: false });`);
  const ask = (id, message) => guest(`return await agents.ask({id:'${id}',message:'${message}'});`);
  const mainRun = (message, pinned = false) => guest(`const h = await agents.spawn({task:'${message}',model:'auto',routeClass:'status-groom',protected:false${pinned ? `,pinModel:'${pin}',pinThinking:'high'` : ''}}); const result = await agents.wait({id:h.id}); return {handle:h,result};`);
  const checkPin = run => { const result = run.result ?? run; assert.match(result.text, /gpt-5-pin/); if (run.result) { assert.equal(result.model,pin); assert.equal(result.thinking,'high'); } };
  const safetyJournal = path.join(profile,'fabric/model-routing-pending.jsonl');
  const safetyRows = () => fs.readFileSync(safetyJournal,'utf8').trim().split('\n').map(line=>JSON.parse(line));

  // F4/manual rollback: remove the whole optional policy from a fresh Main.
  // Neither the resident PID nor owner token may change, and the resident must
  // fail closed exactly as it does for an explicit empty allowlist.
  const originalRouting = config.agents.modelRouting;
  delete config.agents.modelRouting;
  await restartMain('round8-manual-revert-removed-policy');
  const manualMain = await mainRun('PROOF_MANUAL_MAIN_PIN', true); checkPin(manualMain);
  const manualResident = await ask(actor.actor.id,'PROOF_MANUAL_RESIDENT_PIN'); checkPin(manualResident);
  const rollbackOwner = owners(); assert.equal(rollbackOwner[0].owner.pid,residentOwner[0].owner.pid);
  assert.equal(rollbackOwner[0].owner.token,residentOwner[0].owner.token);
  // Explicit later re-enable still works, while class isolation remains intact.
  config.agents.modelRouting = { ...originalRouting, liveClasses: ['task:exact-checks'] }; await restartMain();
  const isolatedTask = await guest(`const h=await agents.spawn({task:'PROOF_ISOLATED_TASK',model:'auto',routeClass:'task:exact-checks',protected:false});return await agents.wait({id:h.id});`);
  assert.equal(isolatedTask.model,cheap); checkPin(await ask(actor.actor.id,'PROOF_ISOLATED_ACTOR_PIN'));
  config.agents.modelRouting = { ...originalRouting, liveClasses: ['status-groom','task:exact-checks'] };
  await restartMain('round8-live-again');
  assert.match((await ask(actor.actor.id,'PROOF_MANUAL_REENABLE')).text,/gpt-5-cheap/);
  assert.equal(owners()[0].owner.token,residentOwner[0].owner.token);
  safetyScenarios.push({finding:'F4-manual-revert',passed:true,removedModelRouting:true,liveClasses:[],mainRunId:manualMain.handle.id,
    residentRunId:manualResident.runId,unchangedResidentOwner:rollbackOwner[0].owner,classIsolation:true,explicitReenable:true});

  // Admission safety: writable journals whose real appends fail cannot grant LIVE.
  await restartMain('astra-r6-F1-full-journals');
  const fullActor = await createActor('status-groom-f1-full');
  const fullFirst = await ask(fullActor.id,'PROOF_F1_FULL_LIVE'); assert.match(fullFirst.text,/gpt-5-cheap/);
  await wait(()=>fs.existsSync(path.join(fullActor.logDir,fullFirst.runId,'route-dispatch-receipt.json')));
  const refusedJournal = path.join(profile,'fabric/model-routing-refused.jsonl');
  const backups = [safetyJournal, refusedJournal].map(file => ({file,text:fs.existsSync(file)?fs.readFileSync(file,'utf8'):''}));
  const prefix = JSON.stringify({type:'committed',receiptId:'full-fixture',at:1,padding:''});
  const full = prefix.slice(0,-2)+'x'.repeat(8192-Buffer.byteLength(prefix)-1)+'"}\n';
  assert.equal(Buffer.byteLength(full),8192);
  for (const {file} of backups) {
    fs.writeFileSync(file,full,{mode:0o600});
    const fd=fs.openSync(file,'a'); assert.equal(fs.fstatSync(fd).size,8192); fs.fsyncSync(fd); fs.closeSync(fd);
    fs.copyFileSync(file,path.join(out,'f1-full-'+path.basename(file)));
  }
  fs.writeFileSync(fullJournals,'Inject only journal append EFBIG; open/fstat/fsync and the decision ledger stay writable');
  for (const {file} of backups) assert.equal(fs.readFileSync(file,'utf8'),full);
  // A new, shorter decision ledger must not bypass the two full journals.
  const ledgerBackup = fs.readFileSync(ledgerPath,'utf8'); fs.writeFileSync(ledgerPath,'');
  await restartMain();
  const fullMain = await mainRun('PROOF_F1_FULL_FRESH_MAIN_PIN'), fullResident = await ask(fullActor.id,'PROOF_F1_FULL_RESIDENT_PIN');
  checkPin(fullMain); checkPin(fullResident);
  assert.equal(fullMain.handle.routeDecision.mode,'shadow');
  assert.equal(fullMain.handle.routeDecision.reasonCode,'record-failed');
  const shortLedgerBytes = fs.statSync(ledgerPath).size; assert.ok(shortLedgerBytes < 8192);
  fs.copyFileSync(ledgerPath,path.join(out,'f1-short-decision-ledger.jsonl'));
  for (const {file} of backups) assert.equal(fs.readFileSync(file,'utf8'),full);
  const appendFaults = fs.readFileSync(transcript,'utf8').trim().split('\n').map(line=>JSON.parse(line)).filter(row=>row.type==='injected_journal_append_efbig');
  assert.ok(appendFaults.some(row=>row.pid===child.pid && row.record.type==='admission'),'Fresh Main bypassed shared admission append');
  assert.ok(appendFaults.some(row=>row.pid!==child.pid && row.record.type==='admission'),'Resident bypassed shared admission append');
  safetyScenarios.push({finding:'admission-append-safety',passed:true,journalBytes:8192,freshMain:true,
    mainPid:child.pid,mainRunId:fullMain.handle.id,residentRunId:fullResident.runId,shortLedgerBytes,appendFaults});
  fs.writeFileSync(ledgerPath,ledgerBackup+fs.readFileSync(ledgerPath,'utf8'));
  fs.rmSync(fullJournals);
  for (const {file,text} of backups) fs.writeFileSync(file,text);

  // F2: native model completion races no mocks. A filesystem denial prevents
  // archival, B completes, and A's sole source receipt must still be usable.
  await restartMain('astra-r3-F2');
  archiveActor = await createActor('status-groom-f2');
  const f2A = await ask(archiveActor.id,'PROOF_ARCHIVE_FAIL_A'); assert.match(f2A.text,/gpt-5-cheap/);
  const nativeA = fs.readFileSync(transcript,'utf8').trim().split('\n').map(line=>JSON.parse(line)).find(row=>row.type==='native_activation' && row.runId===f2A.runId);
  assert.ok(nativeA); await wait(()=>!live(nativeA.pid));
  const f2B = await ask(archiveActor.id,'PROOF_ARCHIVE_OK_B'); assert.match(f2B.text,/gpt-5-cheap/);
  await wait(()=>fs.existsSync(path.join(archiveActor.logDir,f2B.runId,'route-dispatch-receipt.json')));
  assert.ok(fs.existsSync(path.join(nativeA.runDirectory,'route-dispatch-receipt.json')),'A receipt was deleted after B despite failed archive');
  assert.ok(!fs.existsSync(path.join(archiveFailureDirectory,'route-dispatch-receipt.json')));
  const f2C = await ask(archiveActor.id,'PROOF_F2_NEXT_LIVE'); assert.match(f2C.text,/gpt-5-cheap/);
  assert.ok(fs.existsSync(path.join(nativeA.runDirectory,'route-dispatch-receipt.json')));
  fs.chmodSync(archiveFailureDirectory,0o700);
  assert.match((await ask(archiveActor.id,'PROOF_F2_ARCHIVE_RETRY_LIVE')).text,/gpt-5-cheap/);
  await wait(()=>fs.existsSync(path.join(archiveFailureDirectory,'route-dispatch-receipt.json')) && !fs.existsSync(nativeA.runDirectory));
  safetyScenarios.push({finding:'F2',passed:true,failedArchiveRunId:f2A.runId,successfulNextRunId:f2B.runId,
    retainedReceipt:path.join(nativeA.runDirectory,'route-dispatch-receipt.json'),nextLiveRunId:f2C.runId,
    confirmedArchive:path.join(archiveFailureDirectory,'route-dispatch-receipt.json')});

  // F5: B/C are pre-admitted, A-success save is denied, B-failure commits,
  // then C-stop save is denied. Repair must retain original A/B/C timestamps
  // despite physical B/A/C append order and must not cause a quality revert.
  await restartMain('round8-F5');
  const f5Actor = await createActor('status-groom-f5');
  const f5Failed = await guest(`return await agents.spawn({task:'PROOF_TERMINAL_FAILED',model:'auto',routeClass:'status-groom',protected:false});`);
  const f5Stopped = await guest(`return await agents.spawn({task:'PROOF_TERMINAL_STOPPED',model:'auto',routeClass:'status-groom',protected:false});`);
  await wait(()=>failedResponse && stoppedResponse);
  const stateJournal = path.join(profile,'fabric/model-routing-state.jsonl'); fs.chmodSync(stateJournal,0o400);
  const f5Success = await mainRun('PROOF_TERMINAL_SUCCESS_A'); assert.equal(f5Success.result.model,cheap);
  fs.chmodSync(stateJournal,0o600);
  failedResponse.writeHead(400,{'Content-Type':'application/json'}); failedResponse.end(JSON.stringify({error:{message:'Offline proof injected nonretryable invalid request',type:'invalid_request_error',code:'invalid_request'}}));
  const f5FailedResult = await guest(`return await agents.wait({id:'${f5Failed.id}'});`); assert.equal(f5FailedResult.status,'failed');
  fs.chmodSync(stateJournal,0o400);
  const f5StoppedResult = await guest(`await agents.stop({id:'${f5Stopped.id}'}); return await agents.wait({id:'${f5Stopped.id}'});`); assert.equal(f5StoppedResult.status,'stopped');
  if (!stoppedResponse.destroyed) stoppedResponse.end();
  const f5DecisionIds = [f5Success.handle.routeDecision.decisionId,f5Failed.routeDecision.decisionId,f5Stopped.routeDecision.decisionId];
  const committed = new Set(safetyRows().filter(row=>row.type==='committed').map(row=>row.receiptId));
  const f5Pending = safetyRows().filter(row=>row.type==='pending' && f5DecisionIds.includes(row.decision.decisionId) && !committed.has(row.receiptId));
  assert.equal(f5Pending.length,2); assert.deepEqual(f5Pending.map(row=>row.result.status),['completed','stopped']);
  const stateBefore = fs.readFileSync(stateJournal,'utf8');
  const f5Same = await mainRun('PROOF_F5_SAME_MAIN_PIN'); checkPin(f5Same);
  await restartMain();
  const f5Main = await mainRun('PROOF_F5_FRESH_MAIN_PIN'), f5Resident = await ask(f5Actor.id,'PROOF_F5_RESIDENT_PIN'); checkPin(f5Main); checkPin(f5Resident);
  assert.equal(fs.readFileSync(stateJournal,'utf8'),stateBefore);
  fs.chmodSync(stateJournal,0o600); assert.match((await ask(f5Actor.id,'PROOF_F5_REPAIRED_LIVE')).text,/gpt-5-cheap/);
  const savedResults = () => fs.readFileSync(stateJournal,'utf8').trim().split('\n').map(line=>JSON.parse(line)).filter(row=>f5DecisionIds.includes(row.decisionId));
  const repaired = savedResults();
  assert.deepEqual(repaired.map(row=>row.status),['failed','completed','stopped']);
  assert.deepEqual([...repaired].sort((a,b)=>a.at-b.at).map(row=>row.status),['completed','failed','stopped']);
  assert.equal(new Set(repaired.map(row=>row.decisionId)).size,3);
  assert.equal((await mainRun('PROOF_F5_RETRY_NO_DOUBLE_COUNT')).result.model,cheap); assert.equal(savedResults().length,3);
  safetyScenarios.push({finding:'F5',passed:true,successRunId:f5Success.handle.id,failedRunId:f5Failed.id,stoppedRunId:f5Stopped.id,pending:f5Pending,
    sameMainRunId:f5Same.handle.id,mainRunId:f5Main.handle.id,residentRunId:f5Resident.runId,repairedResults:repaired,
    originalOrder:['completed','failed','stopped'],physicalAppendOrder:['failed','completed','stopped'],freshMain:true,idempotent:true,liveRestoredAfterRepair:true});
  assert.ok(!fs.readFileSync(transcript,'utf8').includes('blocked_external_network'),'Proof attempted external networking');
  const rows = ledger(), decisions = rows.filter(row => row.type === 'decision');
  assert.ok(decisions.length >= 5);
  const evidence = decisions.map(decision => {
    const outcome = rows.find(row => row.type === 'outcome' && row.decisionId === decision.decisionId);
    const http = requests.find(row => row.routeHeader?.endsWith(':' + decision.decisionId));
    assert.ok(outcome && http, 'Missing actual provider HTTP/decision/outcome join for ' + decision.decisionId);
    assert.equal(outcome.admittedModel, 'router-proof/' + http.model); assert.equal(outcome.admittedEffort, http.effort);
    const selected = decision.mode === 'live' ? decision : decision.pin;
    assert.equal(outcome.admittedModel, selected.model); assert.equal(outcome.admittedEffort, selected.effort);
    assert.equal(http.routeHeader, `${decision.routeClass}/${encodeURIComponent(decision.model)}-${decision.effort}/${decision.reasonCode}:${decision.decisionId}`);
    const admission = safetyRows().find(row => row.type === 'admission' && row.decisionId === decision.decisionId && row.runId === decision.runId);
    if (decision.mode === 'live') assert.ok(admission, 'LIVE dispatch bypassed the shared durable admission journal');
    const nativeRun = fs.readFileSync(transcript, 'utf8').trim().split('\n').map(line => JSON.parse(line)).find(row => row.type === 'native_activation' && row.runId === decision.runId);
    assert.ok(nativeRun, 'Missing native Pi worker activation'); assert.equal(nativeRun.mode, 'rpc');
    assert.equal(nativeRun.model, outcome.admittedModel); assert.equal(nativeRun.thinking, outcome.admittedEffort);
    return { runId: decision.runId, decisionId: decision.decisionId, actorId: decision.actorId ?? null, routeClass: decision.routeClass,
      mode: decision.mode, reason: decision.reasonCode, pin: decision.pin, shadowChoice: decision.shadowChoice, model: outcome.admittedModel, effort: outcome.admittedEffort, actualHttpHeader: http.routeHeader, status: outcome.status, safetyAdmission: admission ?? null };
  });
  assert.equal(evidence[0].reason, 'live-choice'); assert.equal(evidence[1].reason, 'live-choice');
  assert.ok(evidence.some(row=>row.reason==='jev-error'));
  assert.ok(!rows.some(row=>row.type==='quality' || row.type==='revert'),'Automatic quality revert remains');
  assert.ok(!fs.existsSync(path.join(profile,'fabric/model-routing-quality.jsonl')),'Quality-only journal remains');
  const reasonEvidence = [task.handle.id,actor.first.runId].map(runId => {
    const decision = rows.find(row=>row.type==='decision' && row.runId===runId);
    const outcome = rows.find(row=>row.type==='outcome' && row.runId===runId);
    const expected = runId===task.handle.id ? 'Round 8 task exception' : 'Round 8 actor exception';
    assert.equal(decision.modelReason,expected); assert.equal(outcome.modelReason,expected);
    return {runId,decisionId:decision.decisionId,modelReason:expected};
  });
  proof = { passed: true, head, installedCli: path.resolve(cli), candidate, evidence, actorId: actor.actor.id,
    scope:'LIVE opted-in task/actor classes; manual revert only; automatic quality revert deferred to smarty-dev#4521',
    cleanedResidentActivation: nativeFirst, realResidentOwner: residentOwner, safetyScenarios, modelReason:reasonEvidence,
    candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'),
    mocks: ['Jev fetch boundary', 'loopback OpenAI model/provider HTTP boundary'],
    faultInjection: ['EFBIG append on two valid 8192-byte admission journals', '0500 actor A archive directory', '0400 terminal state journal'] };
  fs.writeFileSync(path.join(out, 'evidence.json'), JSON.stringify(proof, null, 2));
  console.log('PASS: installed Pi RPC + built Fabric; LIVE task/actor, manual Main/resident rollback, class isolation, Jev fallback, F2 custody, F5 repair, modelReason and native HTTP joins.');
} catch (error) { failure = error; record({ type: 'proof_failure', error: String(error), stack: error.stack }); console.error(error); }
finally {
  const native = fs.readFileSync(transcript, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  // Require a recorded native /proc incarnation before cleanup; a reused PID
  // or an owner record without identity is never kill authority.
  for (const row of native.filter(row => row.type === 'native_activation' && row.pid !== child.pid)) {
    registerOwnedProcess(ownedPids, row.pid, row.processStartTime ?? processStartTime(row.pid));
  }
  for (const { file, owner } of owners()) {
    registerOwnedProcess(ownedPids, owner.pid, owner.processStartTime);
    registerOwnedProcess(ownedPids, owner.handover?.launcher?.pid, owner.handover?.launcher?.processStartTime);
    const log = path.join(path.dirname(file), 'launcher.log');
    if (fs.existsSync(log)) for (const line of fs.readFileSync(log, 'utf8').trim().split('\n')) {
      try { const row = JSON.parse(line); if (row.event === 'launcher-started') registerOwnedProcess(ownedPids, row.pid, row.processStartTime); } catch {}
    }
    fs.cpSync(path.dirname(file), path.join(out, 'resident-state'), { recursive: true });
  }
  if (fs.existsSync(path.join(mesh, 'actors'))) fs.cpSync(path.join(mesh, 'actors'), path.join(out, 'actor-state'), { recursive: true });
  if (fs.existsSync(path.join(profile, 'fabric'))) fs.cpSync(path.join(profile, 'fabric'), path.join(out, 'routing-state'), { recursive: true });
  if (fs.existsSync(path.join(scratch, 'sessions'))) fs.cpSync(path.join(scratch, 'sessions'), path.join(out, 'main-sessions'), { recursive: true });
  registerOwnedProcess(ownedPids, child?.pid, processStartTime(child?.pid));
  const claimedPids = [...ownedPids.keys()];
  for (const pid of [...claimedPids].reverse()) signalOwnedProcess(ownedPids, pid, 'SIGTERM');
  const until = Date.now() + 15000; while (claimedPids.some(pid => ownedProcessAlive(ownedPids, pid)) && Date.now() < until) await sleep(25);
  for (const pid of claimedPids) signalOwnedProcess(ownedPids, pid, 'SIGKILL');
  const killUntil = Date.now() + 5000; while (claimedPids.some(pid => ownedProcessAlive(ownedPids, pid)) && Date.now() < killUntil) await sleep(25);
  if (claimedPids.some(pid => ownedProcessAlive(ownedPids, pid))) failure ??= new Error('Isolated resident processes did not exit with verified identities');
  if (child.exitCode === null && child.signalCode === null) child.stdin.end();
  const kill = setTimeout(() => signalOwnedProcess(ownedPids, child.pid, 'SIGTERM'), 10000); const mainExit = await exited; clearTimeout(kill); mainExits.push(mainExit);
  for (const waiter of pending.values()) { clearTimeout(waiter.timer); waiter.reject(new Error('Proof exited')); } pending.clear();
  await new Promise(resolve => server.close(resolve));
  fs.writeFileSync(path.join(out, 'stderr.log'), stderr);
  const allExited = claimedPids.every(pid => !ownedProcessAlive(ownedPids, pid));
  const cleanup = { type: 'proof_cleanup', mainExit, mainExits, ownedPids: [...ownedPids.entries()].map(([pid, startTime]) => ({ pid, startTime })), allExited }; record(cleanup);
  fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ passed: !failure, head, error: failure ? String(failure) : null, ...cleanup }, null, 2));
  fs.writeFileSync(path.join(out, 'bundle-manifest.json'), JSON.stringify(files(path.join(lane, 'dist')).filter(file => /\.(js|mjs)$/.test(file)).map(file => ({ path: path.relative(lane, file), sha256: createHash('sha256').update(fs.readFileSync(file)).digest('hex') })), null, 2));
  if (allExited) fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
