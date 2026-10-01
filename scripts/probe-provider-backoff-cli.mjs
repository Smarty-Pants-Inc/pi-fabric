// Opt-in real CLI regression: build first, then run at nice 19 with an isolated scratch/output root.
// Usage: node scripts/probe-provider-backoff-cli.mjs PI_CLI SCRATCH OUTPUT [FABRIC_ROOT] [--compact-before-retry]
// The compaction variant inserts a real LLM-free manual compaction before the retry followUp.
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const [cli, scratch, out, candidate = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..'), ...modes] = process.argv.slice(2);
const compactBeforeRetry = modes.includes('--compact-before-retry');
// Baseline-only diagnostic bypasses missing guest fields to reach the original native queue bug.
const receiptAnnotation = modes.includes('--untyped-receipts') ? ': any' : '';
assert(cli && scratch && out, 'PI_CLI SCRATCH OUTPUT required');
fs.mkdirSync(out, { recursive: true });
const piVersion = execFileSync(process.execPath, [cli, '--version'], { encoding: 'utf8' }).trim();
assert.equal(piVersion, '0.87.1');
const extension = path.join(scratch, 'loopback.ts');
fs.writeFileSync(extension, `
import { createProvider, fauxProvider, fauxAssistantMessage, createAssistantMessageEventStream } from '@earendil-works/pi-ai';
import fs from 'node:fs';
import os from 'node:os';
export default function(pi) {
  const faux = fauxProvider({ provider: 'r2-loopback', models: [{ id: 'deterministic' }] });
  const stream = (model, context, options) => {
    const output = createAssistantMessageEventStream();
    const message = fauxAssistantMessage('', { stopReason: 'pending' });
    Object.assign(message, { api: model.api, provider: model.provider, model: model.id });
    queueMicrotask(async () => {
      try {
        const payload = await options?.onPayload?.({ lane: process.env.PROBE_LANE }, model) ?? { lane: process.env.PROBE_LANE };
        const response = await fetch(process.env.LOOPBACK_URL, { method: 'POST', body: JSON.stringify(payload), signal: options?.signal });
        await options?.onResponse?.({ status: response.status, headers: Object.fromEntries(response.headers) }, model);
        output.push({ type: 'start', partial: structuredClone(message) });
        const spec = await response.json();
        await options?.onProviderStreamEvent?.(spec, model);
        if (spec.error) throw new Error(spec.error);
        if (spec.code) {
          const block = { type: 'toolCall', id: spec.id, name: 'fabric_exec', arguments: { code: spec.code } };
          message.content = [block];
          output.push({ type: 'toolcall_start', contentIndex: 0, partial: structuredClone(message) });
          output.push({ type: 'toolcall_delta', contentIndex: 0, delta: JSON.stringify(block.arguments), partial: structuredClone(message) });
          output.push({ type: 'toolcall_end', contentIndex: 0, toolCall: block, partial: structuredClone(message) });
          message.stopReason = 'toolUse';
        } else {
          message.content = [{ type: 'text', text: spec.text ?? 'recovered' }];
          output.push({ type: 'text_start', contentIndex: 0, partial: structuredClone(message) });
          output.push({ type: 'text_delta', contentIndex: 0, delta: message.content[0].text, partial: structuredClone(message) });
          output.push({ type: 'text_end', contentIndex: 0, content: message.content[0].text, partial: structuredClone(message) });
          message.stopReason = 'stop';
        }
        output.push({ type: 'done', reason: message.stopReason, message }); output.end(message);
      } catch (error) {
        message.stopReason = options?.signal?.aborted ? 'aborted' : 'error';
        message.errorMessage = String(error.message ?? error);
        output.push({ type: 'error', reason: message.stopReason, error: message }); output.end(message);
      }
    });
    return output;
  };
  pi.registerProvider(createProvider({ id: 'r2-loopback', models: faux.models,
    auth: { apiKey: { name: 'keyless isolated loopback', resolve: async () => ({ auth: {} }) } }, api: { stream, streamSimple: stream } }));
  pi.on('session_start', (_event, ctx) => fs.writeFileSync(process.env.PROBE_READY, JSON.stringify({ mode: ctx.mode, nice: os.getPriority(0), session: ctx.sessionManager.getSessionId(), pid: process.pid })));
}
`);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (check, ms = 30_000) => { const until = Date.now() + ms; while (!check()) { assert(Date.now() < until, 'probe deadline'); await sleep(20); } };
const scripts = { lead: [], receiver: [] }, calls = [], workers = [], report = [];
const server = http.createServer(async (request, response) => {
  let text = ''; for await (const chunk of request) text += chunk;
  const { lane } = JSON.parse(text); const spec = scripts[lane].shift() ?? { text: 'recovered' };
  calls.push({ lane, at: Date.now(), error: spec.error, tool: spec.code ? 'fabric_exec' : undefined, id: spec.id });
  if (spec.block) { request.on('close', () => response.destroy()); return; }
  response.writeHead(200, { 'content-type': 'application/json' }); response.end(JSON.stringify(spec));
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const loopback = `http://127.0.0.1:${server.address().port}`;
function launch(lane) {
  const cwd = path.join(scratch, lane), home = path.join(cwd, 'home'), agentDir = path.join(home, '.pi/agent');
  fs.mkdirSync(agentDir, { recursive: true });
  const mesh = path.join(cwd, 'mesh');
  fs.writeFileSync(path.join(agentDir, 'fabric.json'), JSON.stringify({ autoReload: false, mcp: { enabled: false }, memory: { enabled: false }, records: { enabled: false }, ui: { enabled: false }, mesh: { root: mesh, followUpFlushMs: 0 } }));
  fs.writeFileSync(path.join(agentDir, 'settings.json'), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false, ...(compactBeforeRetry ? { keepRecentTokens: 1 } : {}) } }));
  const ready = path.join(cwd, 'ready.json');
  const args = [cli, '--mode', 'rpc', '--offline', '--no-extensions', '--no-skills', '--no-context-files', '--no-prompt-templates', '--no-themes', '--approve', '-e', path.join(candidate, 'dist/index.js'), '-e', extension, '--provider', 'r2-loopback', '--model', 'deterministic', '--thinking', 'off', '--tools', 'fabric_exec', '--session-dir', path.join(cwd, 'sessions')];
  // No inherited profile, mesh identity, credentials, providers, or startup hooks.
  const child = spawn(process.execPath, args, { cwd, env: { PATH: process.env.PATH, HOME: home, TMPDIR: scratch, PI_OFFLINE: '1', PI_CODING_AGENT_DIR: agentDir, PROBE_LANE: lane, PROBE_READY: ready, LOOPBACK_URL: loopback }, stdio: ['pipe', 'pipe', 'pipe'] });
  fs.writeFileSync(path.join(out, `${lane}-command.json`), JSON.stringify({ executable: process.execPath, args, cwd, home }, null, 2));
  const events = [], pending = new Map(); let next = 0, buffered = '', exited = false;
  const exit = new Promise(resolve => child.once('exit', (code, signal) => { exited = true; for (const p of pending.values()) p.reject(new Error(`CLI exit ${code}/${signal}`)); resolve({ code, signal }); }));
  child.stdout.on('data', data => { fs.appendFileSync(path.join(out, `${lane}-rpc.jsonl`), data); buffered += data.toString(); let end; while ((end = buffered.indexOf('\n')) >= 0) { const line = buffered.slice(0, end); buffered = buffered.slice(end + 1); if (!line.trim()) continue; const event = JSON.parse(line); event.observedAt = Date.now(); events.push(event); if (event.type === 'response') { const p = pending.get(event.id); pending.delete(event.id); event.success ? p?.resolve(event.data) : p?.reject(new Error(event.error)); } } });
  child.stderr.on('data', data => fs.appendFileSync(path.join(out, `${lane}-stderr.log`), data));
  const request = data => new Promise((resolve, reject) => { const id = `${lane}-${++next}`; pending.set(id, { resolve, reject }); child.stdin.write(JSON.stringify({ ...data, id }) + '\n'); });
  const worker = { child, exit, request, events, cwd, ready, mesh, exited: () => exited }; workers.push(worker); return worker;
}
let bridge;
try {
  const a = launch('lead'), b = launch('receiver');
  await wait(() => fs.existsSync(a.ready) && fs.existsSync(b.ready), 60_000);
  const ready = workers.map(w => JSON.parse(fs.readFileSync(w.ready))); ready.forEach(r => { assert.equal(r.mode, 'rpc'); assert.equal(r.nice, 19); });
  const bid = `session:${ready[1].session}`;
  async function guest(code, worker = a) {
    const before = worker.events.length, id = `probe-${path.basename(worker.cwd)}-${before}`;
    scripts[path.basename(worker.cwd)].push({ id, code }, { text: 'guest complete' });
    await worker.request({ type: 'prompt', message: `Execute deterministic public guest ${id}` });
    await wait(() => worker.events.slice(before).some(e => e.type === 'agent_settled'));
    const result = worker.events.slice(before).find(e => e.type === 'tool_execution_end' && e.toolName === 'fabric_exec');
    assert(result && !result.isError, JSON.stringify(result));
    fs.appendFileSync(path.join(out, 'public-guest-results.jsonl'), JSON.stringify({ code, result }) + '\n');
    return result.result;
  }
  // Fabric intentionally initializes optional runtime services at first use, not session_start.
  await guest('return await agents.self();');
  await guest('return await agents.self();', b);
  const { runBridge } = await import(path.join(candidate, 'dist/mesh-bridge.js'));
  const { MeshStore } = await import(path.join(candidate, 'dist/mesh.js'));
  const abort = new AbortController();
  const bridgePromise = runBridge(new Map([['mesh', a.mesh], ['name', 'dev1'], ['remote', 'ryzen2'], ['cursor', path.join(scratch, 'cursor.json')], ['call-timeout-ms', '5000']]), [process.execPath, path.join(candidate, 'bin/mesh-bridge'), 'agent', '--mesh', b.mesh, '--peer', 'dev1'], abort.signal);
  bridge = { abort, promise: bridgePromise };
  const mesh = new MeshStore(a.mesh, 256 * 1024, 500);
  await wait(() => mesh.listAll('topology/participants/', { fresh: true }).some(e => e.value?.id === bid && e.value?.remoteHost === 'ryzen2'));
  const receiverCallBase = calls.filter(c => c.lane === 'receiver').length;
  const receiverCalls = () => calls.filter(c => c.lane === 'receiver').slice(receiverCallBase);
  // Initial provider failure and two permitted 40 KiB followUps force distinct release batches.
  scripts.receiver.push({ error: 'deterministic loopback provider failure 1' }, { error: 'deterministic loopback provider failure 2' }, { text: 'recovered after consecutive backoffs' });
  await b.request({ type: 'prompt', message: compactBeforeRetry ? 'initial failure ' + 'compaction seed '.repeat(3000) : 'initial failure' });
  await wait(() => b.events.some(e => e.type === 'agent_settled' && e.outcome === 'error'));
  const held = await guest(`const receipts = []; for (const text of ["A", "B"]) { const r${receiptAnnotation} = await agents.followUp({ id: ${JSON.stringify(bid)}, message: text.repeat(40000) }); const triggered: boolean | undefined = r.triggered; const reason: string | undefined = r.reason; receipts.push({ ...r, triggered, reason }); } return receipts;`);
  assert.match(JSON.stringify(held), /provider-backoff until/);
  assert.match(JSON.stringify(held), /triggered[^\n]{0,5}false/);
  assert.equal(receiverCalls().length, 1);
  let manualCompaction;
  if (compactBeforeRetry) {
    const before = receiverCalls().length;
    const result = await b.request({ type: 'compact' });
    const end = b.events.findLast(e => e.type === 'compaction_end' && e.reason === 'manual');
    assert(end && !end.aborted && end.result, 'manual compaction did not succeed');
    assert.equal(result.details?.compactor, 'fabric', 'probe must use the built deterministic Fabric compactor');
    assert.equal(result.usage, undefined, 'deterministic compaction unexpectedly reported LLM usage');
    assert.equal(receiverCalls().length, before, 'manual compaction called the failed provider');
    manualCompaction = { at: end.observedAt, result, newProviderCalls: receiverCalls().length - before };
    fs.writeFileSync(path.join(out, 'manual-compaction.json'), JSON.stringify(manualCompaction, null, 2));
  }
  await wait(() => receiverCalls().length >= 2, 70_000);
  await sleep(150);
  assert.equal(receiverCalls().length, 2, 'second batch escaped into native immediate continuation');
  await wait(() => b.events.filter(e => e.type === 'agent_settled' && e.outcome === 'error').length === 2);
  const errors = b.events.filter(e => e.type === 'turn_end' && e.message.stopReason === 'error');
  if (manualCompaction) assert(receiverCalls()[1].at >= manualCompaction.at, 'retry preceded successful compaction');
  else assert(receiverCalls()[1].at - errors[0].observedAt >= 59_990, 'first deadline bypassed');
  const messagesAfterFailure = (await b.request({ type: 'get_messages' })).messages;
  assert.equal(messagesAfterFailure.filter(m => m.role === 'custom' && m.customType === 'pi-fabric-agent-message').length, 1);
  await sleep(Math.max(0, errors[1].observedAt + 119_700 - Date.now()));
  assert.equal(receiverCalls().length, 2, 'second backoff bypassed');
  await wait(() => receiverCalls().length === 3, 10_000);
  await wait(() => b.events.filter(e => e.type === 'agent_settled' && e.outcome === 'completed').length >= 2);
  assert(receiverCalls()[2].at - errors[1].observedAt >= 119_990, 'second deadline bypassed');
  const messages = (await b.request({ type: 'get_messages' })).messages;
  const deliveries = messages.filter(m => m.role === 'custom' && m.customType === 'pi-fabric-agent-message');
  assert.equal(deliveries.length, 2); assert.equal(new Set(deliveries.map(m => m.details.id)).size, 2);
  const receiverMesh = new MeshStore(b.mesh, 256 * 1024, 500);
  const releases = receiverMesh.read({ topic: 'fabric.main.wake', limit: 30 });
  assert.equal(releases.length, 2); assert.deepEqual(releases.map(e => e.data.messageIds.length), [1, 1]);
  report.push({ case: compactBeforeRetry ? 'LLM-free manual compaction and split released batches' : 'consecutive errors and split released batches', manualCompaction, providerCalls: receiverCalls(), firstDelayMs: receiverCalls()[1].at - errors[0].observedAt, secondDelayMs: receiverCalls()[2].at - errors[1].observedAt, deliveredExactlyOnce: deliveries.map(m => m.details.id), releaseEvents: releases, publicReceipts: held });
  // Public lifecycle subscription produces a peer followUp with triggerTurn=false across the bridge.
  const count = receiverCalls().length;
  await guest(`return await agents.subscribe({ from: (await agents.self()).id, to: ${JSON.stringify(bid)}, events: ["pi.agent_settled"], delivery: "followUp", triggerTurn: false, once: true });`);
  await sleep(600); assert.equal(receiverCalls().length, count);
  report.push({ case: 'passive peer lifecycle followUp', newProviderCalls: receiverCalls().length - count });
  const immediate = await guest(`const r = await agents.steer({ id: ${JSON.stringify(bid)}, message: "post-success immediate wake" }); return { ...r, triggered: r.triggered, reason: r.reason };`);
  await wait(() => receiverCalls().length === count + 1); await sleep(200);
  assert.match(JSON.stringify(immediate), /triggered[^\n]{0,5}true/);
  report.push({ case: 'post-success steer', receipt: immediate, newProviderCalls: 1 });
  // Native RPC clear_queue + abort is the documented Escape-equivalent active-run control.
  scripts.receiver.push({ block: true });
  await b.request({ type: 'prompt', message: 'blocked operation for owner Escape control' });
  await wait(() => receiverCalls().length === count + 2);
  await b.request({ type: 'clear_queue' }); await b.request({ type: 'abort' });
  await wait(() => b.events.some(e => e.type === 'agent_settled' && e.outcome === 'aborted'));
  const escapeCount = receiverCalls().length;
  const halted = await guest(`const r = await agents.followUp({ id: ${JSON.stringify(bid)}, message: "must not wake owner-halted lane" }); return { ...r, triggered: r.triggered, reason: r.reason };`);
  assert.match(JSON.stringify(halted), /triggered[^\n]{0,5}false/);
  await sleep(60_500); assert.equal(receiverCalls().length, escapeCount);
  report.push({ case: 'owner Escape-equivalent clear_queue + abort', receipt: halted, observationMs: 60_500, newProviderCalls: 0 });
  for (const w of workers) { const state = await w.request({ type: 'get_state' }); if (state.sessionFile) fs.copyFileSync(state.sessionFile, path.join(out, `${path.basename(w.cwd)}-native.jsonl`)); }
  fs.writeFileSync(path.join(out, 'summary.json'), JSON.stringify({ candidate, cli, piVersion, ready, bridge: 'built runBridge -> built bin/mesh-bridge agent via local stdio; no SSH or credentials', report }, null, 2));
  console.log(JSON.stringify({ ok: true, cases: report.map(r => ({ case: r.case, firstDelayMs: r.firstDelayMs, secondDelayMs: r.secondDelayMs, newProviderCalls: r.newProviderCalls })) }, null, 2));
} finally {
  fs.writeFileSync(path.join(out, 'provider-calls.json'), JSON.stringify(calls, null, 2));
  if (bridge) { bridge.abort.abort(); assert.equal(await bridge.promise, 0); }
  const exits = [];
  for (const w of workers) {
    if (!w.exited()) w.child.stdin.end();
    const kill = setTimeout(() => w.child.kill('SIGKILL'), 10_000);
    exits.push(await w.exit); clearTimeout(kill);
  }
  server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  fs.writeFileSync(path.join(out, 'exits.json'), JSON.stringify(exits));
  exits.forEach(e => assert.equal(e.code, 0, JSON.stringify(e)));
}
