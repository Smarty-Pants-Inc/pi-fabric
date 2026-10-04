// Real installed Pi RPC proof; only Jev fetch and loopback model inference are mocked.
// nice -n 19 node scripts/prove-model-route-live.mjs INSTALLED_PI_CLI $TMPDIR/proof $TASK_OUT/real-cli
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
const [cli, scratchArg, outArg] = process.argv.slice(2);
assert(cli && scratchArg && outArg, 'INSTALLED_PI_CLI SCRATCH OUTPUT required');
const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const scratch = path.resolve(scratchArg), out = path.resolve(outArg), candidate = path.join(lane, 'dist/index.js');
fs.mkdirSync(scratch, { recursive: true, mode: 0o700 }); fs.mkdirSync(out, { recursive: true, mode: 0o700 });
const cwd = path.join(scratch, 'workspace'), home = path.join(scratch, 'home'), profile = path.join(scratch, 'profile'), mesh = path.join(scratch, 'mesh');
for (const directory of [cwd, home, profile]) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
const transcript = path.join(out, 'transcript.jsonl'), queue = path.join(scratch, 'queue.json'), failJev = path.join(scratch, 'fail-jev');
fs.writeFileSync(transcript, ''); fs.writeFileSync(queue, '[]');
const record = row => fs.appendFileSync(transcript, JSON.stringify(row) + '\n');
const pin = 'router-proof/gpt-5-pin', cheap = 'router-proof/gpt-5-cheap';
const requests = [];
const server = http.createServer(async (req, res) => {
  try {
    let text = ''; for await (const chunk of req) text += chunk;
    const body = JSON.parse(text), routeHeader = req.headers['x-smarty-route'] ?? null;
    const row = { type: 'provider_http_request', model: body.model, effort: body.reasoning_effort ?? null,
      routeHeader, url: req.url, messages: body.messages };
    requests.push(row); record(row);
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
  ROUTER_PROOF_ENDPOINT: endpoint, ROUTER_PROOF_TRANSCRIPT: transcript, ROUTER_PROOF_FAIL_JEV: failJev };
const args = [path.resolve(cli), '--mode', 'rpc', '--offline', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files', '--approve',
  '--provider', 'router-proof', '--model', 'gpt-5-pin', '--thinking', 'high', '-e', candidate, '--tools', 'fabric_exec', '--session-dir', path.join(scratch, 'sessions')];
const head = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: lane, encoding: 'utf8' }).trim();
record({ type: 'proof_command', executable: process.execPath, args, cwd, env: { ...env, TYPESAFE_API_KEY: '<offline fixture sentinel>' }, head,
  candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'),
  piVersion: execFileSync(process.execPath, [cli, '--version'], { env, encoding: 'utf8' }).trim(), config });
const child = spawn(process.execPath, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
const exited = new Promise(resolve => child.once('close', (code, signal) => resolve({ code, signal, pid: child.pid })));
const events = [], pending = new Map(); let buffer = '', serial = 0, stderr = '', failure, proof;
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
const request = frame => new Promise((resolve, reject) => {
  const id = 'rpc-' + (++serial), timer = setTimeout(() => { pending.delete(id); reject(new Error('RPC deadline: ' + frame.type)); }, 90000);
  pending.set(id, { resolve, reject, timer }); record({ type: 'rpc_command', ...frame, id }); child.stdin.write(JSON.stringify({ ...frame, id }) + '\n');
});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (check, timeout = 90000) => { const until = Date.now() + timeout; while (!check()) {
  assert.equal(child.exitCode, null, 'Pi unexpectedly exited: ' + stderr); assert.ok(Date.now() < until, 'Proof observation deadline: ' + stderr); await sleep(25);
} };
async function guest(code) {
  const before = events.length, id = 'guest-' + (++serial);
  fs.writeFileSync(queue, JSON.stringify([{ id, code }])); record({ type: 'public_fabric_exec', id, code });
  await request({ type: 'prompt', message: 'Execute ' + id });
  await wait(() => events.slice(before).some(event => event.type === 'agent_settled'));
  const event = events.slice(before).find(event => event.type === 'tool_execution_end' && event.toolCallId === id);
  assert.ok(event && !event.isError && event.result.details.success, JSON.stringify(event ?? events.slice(before)));
  const result = JSON.parse(event.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n'));
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
const ownedPids = new Set();
try {
  const state = await request({ type: 'get_state' }); assert.equal(state.model.provider + '/' + state.model.id, pin);
  const diagnostic = await guest(`try { return { evaluation: await tools.call({ref: 'jev.evaluate', args: {state: {routeClass: 'task:exact-checks', mode: 'shadow', protection: 'clear'}, questions: {route: {type: 'choice', instructions: 'Offline proof', criteria: {'candidate-0': {model: '${pin}', effort: 'high'}, 'candidate-1': {model: '${cheap}', effort: 'medium'}}}}}}) }; } catch(error) { return {error: String(error), stack: error.stack}; }`); record({type:'jev_diagnostic',diagnostic}); assert.ok(diagnostic.evaluation && !diagnostic.error, JSON.stringify(diagnostic));
  const task = await guest(`const handle = await agents.spawn({ task: 'PROOF_TASK_LIVE', model: 'auto', routeClass: 'task:exact-checks', protected: false }); const result = await agents.wait({ id: handle.id }); return {handle,result};`);
  assert.equal(task.handle.routeDecision.reasonCode, 'live-choice'); assert.equal(task.result.model, cheap); assert.equal(task.result.thinking, 'medium');
  const actor = await guest(`const actor = await agents.create({ name: 'status-groom-proof', instructions: 'Bounded status checks only', residency: 'durable', runner: 'pi', transport: 'process', model: '${pin}', thinking: 'high', routeClass: 'status-groom', protected: false, tools: [], extensions: true, events: [], topics: [], delivery: 'mailbox', triggerTurn: false }); const first = await agents.ask({ id: actor.id, message: 'PROOF_ACTOR_LIVE' }); return {actor,first};`);
  assert.ok(actor.first.runId); assert.match(actor.first.text, /gpt-5-cheap/);
  await wait(() => fs.existsSync(path.join(actor.actor.logDir, actor.first.runId, 'route-quality-receipt.json')));
  const nativeFirst = fs.readFileSync(transcript, 'utf8').trim().split('\n').map(line => JSON.parse(line)).find(row => row.type === 'native_activation' && row.runId === actor.first.runId);
  assert.ok(nativeFirst?.runDirectory, 'No native resident activation process');
  await wait(() => !fs.existsSync(nativeFirst.runDirectory) && !live(nativeFirst.pid));
  record({type:'cleaned_resident_activation', nativeFirst, archivedReceipt: path.join(actor.actor.logDir, actor.first.runId, 'route-quality-receipt.json')});
  const beforeQuality = owners(); assert.ok(beforeQuality.length > 0, 'No real resident owner');
  const quality = await guest(`return await agents.routeOutcome({ id: '${actor.first.runId}', routeQuality: 'fail' });`);
  assert.equal(quality.routeQuality, 'fail');
  const qualityRows = fs.readFileSync(path.join(profile, 'fabric/model-routing-quality.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(qualityRows.map(row => row.type), ['pending', 'committed']);
  const next = await guest(`return await agents.ask({ id: '${actor.actor.id}', message: 'PROOF_ACTOR_PINNED' });`); assert.match(next.text, /gpt-5-pin/);
  const stillLive = await guest(`return await agents.run({ task: 'PROOF_TASK_OTHER_CLASS', model: 'auto', routeClass: 'task:exact-checks', protected: false });`);
  assert.equal(stillLive.model, cheap);
  fs.writeFileSync(failJev, 'inject only a Jev provider-boundary failure');
  const fallback = await guest(`return await agents.run({ task: 'PROOF_TASK_ERROR_FALLBACK', model: 'auto', routeClass: 'task:exact-checks', protected: false });`);
  assert.equal(fallback.model, pin); fs.rmSync(failJev);
  assert.ok(!fs.readFileSync(transcript, 'utf8').includes('blocked_external_network'), 'Proof attempted external networking');
  const rows = ledger(), decisions = rows.filter(row => row.type === 'decision');
  assert.equal(decisions.length, 5);
  const evidence = decisions.map(decision => {
    const outcome = rows.find(row => row.type === 'outcome' && row.decisionId === decision.decisionId);
    const http = requests.find(row => row.routeHeader?.endsWith(':' + decision.decisionId));
    assert.ok(outcome && http, 'Missing actual provider HTTP/decision/outcome join for ' + decision.decisionId);
    assert.equal(outcome.admittedModel, 'router-proof/' + http.model); assert.equal(outcome.admittedEffort, http.effort);
    const selected = decision.mode === 'live' ? decision : decision.pin;
    assert.equal(outcome.admittedModel, selected.model); assert.equal(outcome.admittedEffort, selected.effort);
    assert.equal(http.routeHeader, `${decision.routeClass}/${encodeURIComponent(selected.model)}-${selected.effort}/${decision.reasonCode}:${decision.decisionId}`);
    const nativeRun = fs.readFileSync(transcript, 'utf8').trim().split('\n').map(line => JSON.parse(line)).find(row => row.type === 'native_activation' && row.runId === decision.runId);
    assert.ok(nativeRun, 'Missing native Pi worker activation'); assert.equal(nativeRun.mode, 'rpc');
    assert.equal(nativeRun.model, outcome.admittedModel); assert.equal(nativeRun.thinking, outcome.admittedEffort);
    return { runId: decision.runId, decisionId: decision.decisionId, actorId: decision.actorId ?? null, routeClass: decision.routeClass,
      mode: decision.mode, reason: decision.reasonCode, model: outcome.admittedModel, effort: outcome.admittedEffort, actualHttpHeader: http.routeHeader, status: outcome.status };
  });
  assert.equal(evidence[0].reason, 'live-choice'); assert.equal(evidence[1].reason, 'live-choice');
  assert.equal(evidence[2].reason, 'class-reverted'); assert.equal(evidence[3].reason, 'live-choice'); assert.equal(evidence[4].reason, 'jev-error');
  assert.ok(rows.some(row => row.type === 'revert' && row.decisionId === evidence[1].decisionId && row.reason === 'quality-fail'));
  const afterQuality = owners(); assert.equal(afterQuality[0].owner.token, beforeQuality[0].owner.token);
  proof = { passed: true, head, installedCli: path.resolve(cli), candidate, evidence, quality, actorId: actor.actor.id,
    cleanedResidentActivation: nativeFirst, realResidentOwner: beforeQuality, durableQualityFence: qualityRows, mocks: ['Jev fetch boundary', 'loopback OpenAI model/provider HTTP boundary'] };
  fs.writeFileSync(path.join(out, 'evidence.json'), JSON.stringify(proof, null, 2));
  console.log('PASS: installed Pi RPC + built Fabric; public live task/actor, durable quality demotion/pin, class isolation, Jev error fallback, actual HTTP X-Smarty-Route joins.');
} catch (error) { failure = error; record({ type: 'proof_failure', error: String(error), stack: error.stack }); console.error(error); }
finally {
  const native = fs.readFileSync(transcript, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  for (const row of native.filter(row => row.type === 'native_activation')) if (row.pid !== child.pid) ownedPids.add(row.pid);
  for (const { file, owner } of owners()) {
    ownedPids.add(owner.pid); if (owner.handover?.launcher?.pid) ownedPids.add(owner.handover.launcher.pid);
    const log = path.join(path.dirname(file), 'launcher.log');
    if (fs.existsSync(log)) for (const line of fs.readFileSync(log, 'utf8').trim().split('\n')) {
      try { const row = JSON.parse(line); if (row.event === 'launcher-started' && Number.isInteger(row.pid)) ownedPids.add(row.pid); } catch {}
    }
    fs.cpSync(path.dirname(file), path.join(out, 'resident-state'), { recursive: true });
  }
  if (fs.existsSync(path.join(mesh, 'actors'))) fs.cpSync(path.join(mesh, 'actors'), path.join(out, 'actor-state'), { recursive: true });
  if (fs.existsSync(path.join(profile, 'fabric'))) fs.cpSync(path.join(profile, 'fabric'), path.join(out, 'routing-state'), { recursive: true });
  if (fs.existsSync(path.join(scratch, 'sessions'))) fs.cpSync(path.join(scratch, 'sessions'), path.join(out, 'main-sessions'), { recursive: true });
  // Only PIDs learned from this isolated proof root. Never target a preexisting host.
  for (const pid of [...ownedPids].reverse()) if (live(pid)) { try { process.kill(pid, 'SIGTERM'); } catch {} }
  const until = Date.now() + 15000; while ([...ownedPids].some(live) && Date.now() < until) await sleep(25);
  for (const pid of ownedPids) if (live(pid)) { try { process.kill(pid, 'SIGKILL'); } catch {} }
  const killUntil = Date.now() + 5000; while ([...ownedPids].some(live) && Date.now() < killUntil) await sleep(25);
  if ([...ownedPids].some(live)) failure ??= new Error('Isolated resident processes did not exit');
  if (child.exitCode === null && child.signalCode === null) child.stdin.end();
  const kill = setTimeout(() => child.kill('SIGTERM'), 10000); const mainExit = await exited; clearTimeout(kill);
  for (const waiter of pending.values()) { clearTimeout(waiter.timer); waiter.reject(new Error('Proof exited')); } pending.clear();
  await new Promise(resolve => server.close(resolve));
  fs.writeFileSync(path.join(out, 'stderr.log'), stderr);
  const cleanup = { type: 'proof_cleanup', mainExit, ownedPids: [...ownedPids], allExited: [...ownedPids].every(pid => !live(pid)) }; record(cleanup);
  fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ passed: !failure, head, error: failure ? String(failure) : null, ...cleanup }, null, 2));
  fs.writeFileSync(path.join(out, 'bundle-manifest.json'), JSON.stringify(files(path.join(lane, 'dist')).filter(file => /\.(js|mjs)$/.test(file)).map(file => ({ path: path.relative(lane, file), sha256: createHash('sha256').update(fs.readFileSync(file)).digest('hex') })), null, 2));
  fs.rmSync(scratch, { recursive: true, force: true });
}
if (failure) process.exitCode = 1;
