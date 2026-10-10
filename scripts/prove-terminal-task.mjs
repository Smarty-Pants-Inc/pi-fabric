#!/usr/bin/env node
/** #3039: public Fabric APIs in a native Pi parent + real process task, offline.
 * No source imports, fake Pi, credential lookup, live mesh, or build. All runtime
 * state lives in one private mkdtemp below TMPDIR; only evidence survives.
 * Usage: node scripts/prove-terminal-task.mjs --fabric-root /built/package \
 *   --evidence-dir .local/terminal-proof/baseline --expect-terminal false
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';

const NATIVE_PI = '/home/paul/.local/share/smarty-dev/pi-runtime/releases/21ab152c7e43af76468d898bc50085fcd01a515b/node/node_modules/@earendil-works/pi-coding-agent/dist/cli.js';
const GUARD = 180_000;
const option = name => {
  const i = process.argv.indexOf(name);
  if (i < 0 || !process.argv[i + 1] || process.argv[i + 1].startsWith('--')) throw new Error(`Required: ${name}`);
  return process.argv[i + 1];
};
const fabricRoot = fs.realpathSync(option('--fabric-root'));
const evidenceDir = path.resolve(option('--evidence-dir'));
const expectation = option('--expect-terminal');
assert(['true', 'false'].includes(expectation), '--expect-terminal must be true|false');
const expectTerminal = expectation === 'true';
const extension = path.join(fabricRoot, 'dist/index.js');
const worker = path.join(fabricRoot, 'dist/worker.js');
for (const file of [NATIVE_PI, extension, worker]) assert(fs.statSync(file).isFile(), `Missing built artifact: ${file}`);
assert(process.env.TMPDIR, 'Set TMPDIR to a private host temporary directory');
assert(!path.resolve(process.env.TMPDIR).startsWith('/var/tmp'), 'Scratch must not use /var/tmp');
assert(!fs.existsSync(evidenceDir) || fs.readdirSync(evidenceDir).length === 0, 'Use a new/empty --evidence-dir; never mix proof generations');
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR, 'terminal-native-proof-'));
fs.mkdirSync(evidenceDir, { recursive: true });
const hash = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const json = (file, value) => fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
const readJson = file => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } };
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); void promise.catch(() => {}); return { promise, resolve, reject }; };
const bounded = (promise, label, ms = GUARD) => {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Timeout: ${label}`)), ms); })]).finally(() => clearTimeout(timer));
};
// Watch native persisted state, not a sleep that guesses how quickly RPC queues.
const watchUntil = (directory, predicate, label) => new Promise((resolve, reject) => {
  let watcher;
  const timer = setTimeout(() => { watcher?.close(); reject(new Error(`Timeout: ${label}`)); }, GUARD);
  const check = () => {
    try { const result = predicate(); if (result) { clearTimeout(timer); watcher?.close(); resolve(result); } }
    catch (error) { clearTimeout(timer); watcher?.close(); reject(error); }
  };
  watcher = fs.watch(directory, check);
  watcher.on('error', error => { clearTimeout(timer); watcher.close(); reject(error); });
  check();
});
const procIdentity = pid => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    const environment = Object.fromEntries(fs.readFileSync(`/proc/${pid}/environ`, 'utf8').split('\0').flatMap(entry => {
      const i = entry.indexOf('=');
      const key = entry.slice(0, i);
      return ['PI_FABRIC_PARENT_RUN', 'PI_FABRIC_PI_BINARY', 'PI_FABRIC_AGENT_RUN_DIR', 'PI_CODING_AGENT_DIR'].includes(key) ? [[key, entry.slice(i + 1)]] : [];
    }));
    return { pid: Number(pid), ppid: Number(fields[1]), state: fields[0], started: fields[19], executable: fs.readlinkSync(`/proc/${pid}/exe`), environment, command: fs.readFileSync(`/proc/${pid}/cmdline`, 'utf8').replaceAll('\0', ' ').trim() };
  } catch { return undefined; }
};
const descendants = root => {
  const all = fs.readdirSync('/proc').filter(x => /^\d+$/.test(x)).map(procIdentity).filter(Boolean);
  const owned = new Set([root]);
  let previous;
  do { previous = owned.size; for (const p of all) if (owned.has(p.ppid)) owned.add(p.pid); } while (owned.size !== previous);
  return all.filter(p => owned.has(p.pid));
};
const stillOwnedAlive = p => { const now = procIdentity(p.pid); return now && now.started === p.started && now.state !== 'Z'; };

class NativeParent {
  constructor(cwd, env, evidence) {
    this.events = []; this.waiters = []; this.pending = new Map(); this.stderr = '';
    this.child = spawn(process.execPath, [NATIVE_PI, '--mode', 'rpc', '--no-session', '--provider', 'terminal-proof', '--model', 'parent', '--thinking', 'off', '--no-context-files', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-extensions', '--extension', extension, '--approve'], { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
    this.evidence = evidence;
    json(path.join(evidence, 'parent-launch.json'), { executable: process.execPath, nativePi: NATIVE_PI, extension, cwd, pid: this.child.pid });
    this.exit = new Promise(resolve => this.child.once('close', (code, signal) => { this.closed = true; resolve({ code, signal }); }));
    this.child.on('error', error => { for (const item of this.pending.values()) item.reject(error); });
    let tail = '';
    this.child.stdout.on('data', chunk => {
      fs.appendFileSync(path.join(evidence, 'parent-events.jsonl'), chunk);
      tail += chunk.toString('utf8');
      while (tail.includes('\n')) {
        const end = tail.indexOf('\n'), line = tail.slice(0, end); tail = tail.slice(end + 1);
        if (!line.trim()) continue;
        let event;
        try { event = JSON.parse(line); } catch { throw new Error(`Non-RPC parent output: ${line}`); }
        this.events.push(event);
        if (event.type === 'response') { const item = this.pending.get(event.id); if (item) { this.pending.delete(event.id); event.success ? item.resolve(event.data) : item.reject(new Error(event.error)); } }
        for (const item of [...this.waiters]) if (item.test(event)) { this.waiters.splice(this.waiters.indexOf(item), 1); item.resolve(event); }
      }
    });
    this.child.stderr.on('data', chunk => { this.stderr += chunk; fs.appendFileSync(path.join(evidence, 'parent-stderr.log'), chunk); });
  }
  command(frame) {
    const id = crypto.randomUUID();
    return bounded(new Promise((resolve, reject) => { this.pending.set(id, { resolve, reject }); this.child.stdin.write(`${JSON.stringify({ id, ...frame })}\n`); }), `parent RPC ${frame.type}`);
  }
  event(test) {
    const prior = this.events.find(test); if (prior) return Promise.resolve(prior);
    return bounded(new Promise(resolve => this.waiters.push({ test, resolve })), 'native parent event');
  }
  async close() {
    if (this.closed) return this.exit;
    this.child.stdin.end();
    try { return await bounded(this.exit, 'parent graceful close', 15_000); }
    catch { this.child.kill('SIGTERM'); try { return await bounded(this.exit, 'parent TERM close', 10_000); } catch { this.child.kill('SIGKILL'); return bounded(this.exit, 'parent KILL close', 10_000); } }
  }
}

const toolValue = event => {
  if (!event) return undefined;
  const result = event.result;
  if (result?.details?.result !== undefined) return result.details.result;
  const text = result?.content?.filter(x => x.type === 'text').map(x => x.text).join('\n');
  if (text) { try { return JSON.parse(text); } catch { /* retain diagnostic */ } }
  return result?.details ?? { unparsed: text, result };
};
const sse = response => {
  response.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
  const chunk = (delta, finish_reason = null) => response.write(`data: ${JSON.stringify({ id: 'offline', object: 'chat.completion.chunk', created: 1, model: 'offline', choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
  return {
    text: text => chunk({ role: 'assistant', content: text }),
    tool: (name, args, id = crypto.randomUUID()) => chunk({ role: 'assistant', tool_calls: [{ index: 0, id, type: 'function', function: { name, arguments: JSON.stringify(args) } }] }),
    end: (reason = 'stop') => { chunk({}, reason); response.end('data: [DONE]\n\n'); },
  };
};

const reports = [];
let activeParent;
let activeServer;
let owned = [];
let failure;
try {
  for (const scenario of ['final', 'intermediate']) {
    const dir = path.join(scratch, scenario), evidence = path.join(evidenceDir, scenario);
    const agent = path.join(dir, 'agent'), cwd = path.join(dir, 'workspace'), runs = path.join(dir, 'runs');
    for (const folder of [agent, cwd, runs, evidence, path.join(dir, 'home'), path.join(dir, 'tmp')]) fs.mkdirSync(folder, { recursive: true });
    fs.mkdirSync(path.join(cwd, 'src'));
    const marker = path.join(cwd, 'src', scenario === 'final' ? 'forbidden-after-quiescent.mjs' : 'valid-continuation.mjs');
    const markerSource = scenario === 'final' ? 'export const FORBIDDEN_AFTER_QUIESCENT = true;\n' : 'export const VALID_CONTINUATION = true;\n';
    const probe = path.join(cwd, 'read-only.txt'); fs.writeFileSync(probe, 'intermediate native tool input\n');
    const controlText = scenario === 'final' ? `FORBIDDEN_CONTROL: ownership was returned. If you consume this steer, write ${marker} with FORBIDDEN_AFTER_QUIESCENT.` : `VALID_CONTROL: continue valid work and write ${marker}.`;
    const finalText = scenario === 'final' ? 'QUIESCENT\nOwnership returned. Do not revive this task. Native final result preserved.' : 'VALID_CONTINUATION_COMPLETE';
    const ready = deferred(); let held; let parentCalls = 0, childCalls = 0, handle, steer, result, late;
    const requests = [];
    const callTool = (stream, code) => { stream.tool('fabric_exec', { code, resultFormat: 'json', display: { name: 'terminal process proof', description: `${scenario}: public Fabric route` } }); stream.end('tool_calls'); };
    activeServer = http.createServer((request, response) => {
      let body = '';
      request.on('data', chunk => { body += chunk; });
      request.on('end', () => void (async () => {
        const payload = JSON.parse(body); requests.push({ at: Date.now(), payload });
        json(path.join(evidence, 'http-requests.json'), requests);
        const stream = sse(response);
        if (payload.model === 'child') {
          childCalls++;
          if (childCalls === 1) {
            stream.text(scenario === 'final' ? finalText : 'INTERMEDIATE_TOOL_PROGRESS');
            if (scenario === 'intermediate') stream.tool('read', { path: probe });
            held = stream;
            ready.resolve();
            return; // Native provider stream remains open until native queue confirmation.
          }
          assert(JSON.stringify(payload.messages).includes(scenario === 'final' ? 'FORBIDDEN_CONTROL' : 'VALID_CONTROL'), 'Follow-up child request must contain actual queued sender control');
          if (childCalls === 2) { stream.tool('write', { path: marker, content: markerSource }); stream.end('tool_calls'); }
          else { stream.text(scenario === 'final' ? 'REVIVED_AFTER_QUIESCENT' : finalText); stream.end(); }
          return;
        }
        assert.equal(payload.model, 'parent', 'Only private parent/child models may request inference');
        parentCalls++;
        const latest = name => toolValue([...activeParent.events].reverse().find(e => e.type === 'tool_execution_end' && e.toolName === 'fabric_exec' && activeParent.events.some(s => s.type === 'tool_execution_start' && s.toolCallId === e.toolCallId && s.args?.code?.includes(name))));
        if (parentCalls === 1) {
          callTool(stream, `return await agents.spawn({ name: "terminal-${scenario}", task: "NATIVE_PROOF_${scenario.toUpperCase()}: return ownership and do not resume without new grant.", model: "terminal-proof/child", thinking: "off", runner: "pi", transport: "process", extensions: false, tools: ["read", "write"], timeoutMs: ${GUARD} });`);
        } else if (parentCalls === 2) {
          handle = latest('agents.spawn');
          assert(handle?.id, `Public agents.spawn did not return a handle: ${JSON.stringify(handle)}`);
          await bounded(ready.promise, 'real native child stream ready');
          owned = [...owned, ...descendants(activeParent.child.pid)];
          json(path.join(evidence, 'native-processes-at-stream.json'), descendants(activeParent.child.pid));
          callTool(stream, `return await agents.steer({ id: ${JSON.stringify(handle.id)}, message: ${JSON.stringify(controlText)} });`);
        } else if (parentCalls === 3) {
          steer = latest('agents.steer');
          assert(steer?.queued && steer?.messageId, `Public steer failed before child final: ${JSON.stringify(steer)}`);
          const run = path.join(runs, handle.id);
          const queued = await watchUntil(run, () => {
            const status = readJson(path.join(run, 'status.json'));
            return status?.pendingMessages?.steering?.some(text => text.includes(scenario === 'final' ? 'FORBIDDEN_CONTROL' : 'VALID_CONTROL')) ? status : undefined;
          }, 'native Pi queue_update confirms steer BEFORE finish marker');
          json(path.join(evidence, 'native-queue-before-finish.json'), { queuedAt: Date.now(), steer, status: queued });
          held.end(scenario === 'final' ? 'stop' : 'tool_calls'); held = undefined;
          callTool(stream, `return await agents.wait({ id: ${JSON.stringify(handle.id)}, timeoutMs: ${GUARD} });`);
        } else if (parentCalls === 4) {
          result = latest('agents.wait');
          assert(result?.id, `Public wait did not return: ${JSON.stringify(result)}`);
          json(path.join(evidence, 'native-processes-after-join.json'), descendants(activeParent.child.pid));
          callTool(stream, `try { const delivery = await agents.followUp({ id: ${JSON.stringify(handle.id)}, message: "LATE_FOLLOW_UP_MUST_NOT_REVIVE" }); return { accepted: true, delivery }; } catch (error) { return { accepted: false, message: String(error), code: (error as {code?: string}).code, finalAnswerReceiptId: (error as {finalAnswerReceiptId?: string}).finalAnswerReceiptId }; }`);
        } else {
          if (parentCalls === 5) late = latest('agents.followUp');
          stream.text('PROOF_PARENT_FINISHED'); stream.end();
        }
      })().catch(error => { failure ??= error; ready.reject(error); response.destroy(error); }));
    });
    await new Promise(resolve => activeServer.listen(0, '127.0.0.1', resolve));
    const port = activeServer.address().port;
    json(path.join(agent, 'models.json'), { providers: { 'terminal-proof': { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: 'offline-only-not-a-credential', api: 'openai-completions', models: ['parent', 'child'].map(id => ({ id, name: id, reasoning: false, input: ['text'], contextWindow: 128_000, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })) } } });
    json(path.join(agent, 'settings.json'), { enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false }, extensions: [], packages: [] });
    json(path.join(agent, 'fabric.json'), { configVersion: 4, fullCodeMode: false, autoReload: false, executor: { kernel: 'typescript', timeoutMs: GUARD, maxTimeoutMs: GUARD, landlock: { mode: 'off' }, resultFormat: 'json' }, agents: { enabled: true, runner: 'pi', transport: 'process', maxDepth: 2, maxConcurrent: 2, extensions: false, deniedModels: [], modelPolicy: { requireReason: [] }, retainRuns: true, sessionExport: false, notifyOnComplete: false, timeoutMs: GUARD }, mcp: { enabled: false }, mesh: { enabled: true, announce: true, actorScope: 'session' }, capture: { enabled: false }, jev: { enabled: false }, prewalk: { enabled: false }, ui: { enabled: false }, compaction: { engine: 'pi' }, approvals: { read: 'allow', write: 'allow', execute: 'allow', network: 'allow', agent: 'allow' } });
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^(PI_|SMARTY_|HERDR_|ORG_|OPENAI_|ANTHROPIC_|GOOGLE_|GEMINI_|OPENROUTER_|TYPESAFE_|AWS_|AZURE_|AI_GATEWAY_|OTEL_|SENTRY_)/.test(key)));
    Object.assign(env, { HOME: path.join(dir, 'home'), XDG_CONFIG_HOME: path.join(dir, 'home/.config'), XDG_DATA_HOME: path.join(dir, 'home/.local/share'), XDG_CACHE_HOME: path.join(dir, 'home/.cache'), TMPDIR: path.join(dir, 'tmp'), PI_CODING_AGENT_DIR: agent, PI_OFFLINE: '1', PI_SKIP_VERSION_CHECK: '1', PI_TELEMETRY: '0', PI_FABRIC_PI_BINARY: NATIVE_PI, PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_MESH_ROOT: path.join(dir, 'mesh'), PI_FABRIC_RUN_ROOT: runs, PI_FABRIC_AGENT_DIR: path.join(dir, 'exports') });
    activeParent = new NativeParent(cwd, env, evidence);
    const settled = activeParent.event(event => event.type === 'agent_settled');
    await activeParent.command({ type: 'prompt', message: 'Run the deterministic terminal task process proof using Fabric public APIs.' });
    await settled;
    if (failure) throw failure;
    const messages = await activeParent.command({ type: 'get_messages' });
    const state = await activeParent.command({ type: 'get_state' });
    json(path.join(evidence, 'parent-messages.json'), messages);
    json(path.join(evidence, 'parent-state.json'), state);
    const run = path.join(runs, handle.id);
    const record = readJson(path.join(run, 'status.json'));
    const receipt = readJson(path.join(run, 'final-answer.json'));
    fs.cpSync(run, path.join(evidence, 'run'), { recursive: true });
    const notices = (messages?.messages ?? messages ?? []).filter(message => message.customType === 'pi-fabric-target-terminal' || message.details?.code === 'FABRIC_TARGET_TERMINAL');
    const nativeProcesses = readJson(path.join(evidence, 'native-processes-at-stream.json'));
    const taskProcesses = nativeProcesses.filter(p => p.pid !== activeParent.child.pid);
    // CLI Pi hides argv behind its "pi" title; native SDK tasks retain the
    // compiled entry and exact SDK directory. Both must be owned worker children.
    const sdkLaunch = `${path.join(fabricRoot, 'dist/worker/task-entry.js')} ${path.dirname(NATIVE_PI)} `;
    const nativeChild = taskProcesses.find(p => p.ppid === Number(result.sessionId) && (p.command === 'pi' || p.command.includes(sdkLaunch)));
    assert(nativeChild, 'Proof must observe the actual native Pi execution child');
    assert.equal(nativeChild.executable, fs.realpathSync(process.execPath));
    assert.equal(nativeChild.environment.PI_FABRIC_PARENT_RUN, handle.id);
    assert.equal(nativeChild.environment.PI_FABRIC_PI_BINARY, NATIVE_PI);
    assert.equal(nativeChild.environment.PI_CODING_AGENT_DIR, agent);
    assert(record?.fabricRelease === fabricRoot, `Worker selected wrong release: ${record?.fabricRelease}`);
    assert.equal(result.status, 'completed', `Native task unsuccessful: ${JSON.stringify(result)}`);
    assert(taskProcesses.every(p => !stillOwnedAlive(p)), 'Child/execution process alive after public join');
    const checks = { publicParent: true, nativeQueueConfirmedBeforeFinish: true, nativeTaskGoneAfterJoin: true, markerExists: fs.existsSync(marker), childRequests: childCalls, finalTextPreserved: result.text === finalText, lateRefused: late?.accepted === false, senderNotices: notices.length };
    if (scenario === 'final' && expectTerminal) {
      assert.equal(childCalls, 1, 'No provider request may follow task final answer');
      assert.equal(checks.markerExists, false, 'QUIESCENT task consumed forbidden steer');
      assert.equal(result.text, finalText, 'Final answer text changed');
      assert(receipt?.version === 1 && receipt.runId === handle.id && receipt.text === finalText, 'Durable final answer receipt missing or wrong');
      assert.equal(record.finalAnswerReceipt?.id, receipt.id, 'Terminal record must identify native final receipt');
      assert.equal(result.finalAnswerReceipt?.id, receipt.id, 'Public wait must identify native final receipt');
      assert.equal(late?.accepted, false, 'Late followUp must be refused');
      assert.equal(late.code, 'FABRIC_TARGET_TERMINAL', 'Late refusal lost typed terminal code at public guest boundary');
      assert.equal(late.finalAnswerReceiptId, receipt.id, 'Late refusal must identify final receipt');
      const matchingNotices = notices.filter(message => message.details?.messageId === steer.messageId);
      assert.equal(matchingNotices.length, 1, 'Original native sender must receive exactly one typed queued-steer terminal refusal');
      const notice = matchingNotices[0];
      assert.equal(notice.details.code, 'FABRIC_TARGET_TERMINAL');
      assert.equal(notice.details.targetId, handle.id);
      assert.equal(notice.details.delivery, 'steer');
      assert.equal(notice.details.finalAnswerReceiptId, receipt.id);
      assert.equal(notice.details.sender?.id, handle.spawner.id, 'Refusal lost original sender identity');
      assert.equal(readJson(path.join(run, 'terminal-controls', `${steer.messageId}.json`))?.state, 'refused', 'Native terminal-control custody was not settled refused');
    } else if (scenario === 'final') {
      assert(checks.markerExists, 'Baseline must ACTUALLY execute forbidden source write, not just admit steer');
      assert(childCalls > 1, 'Baseline must ACTUALLY issue resumed native provider requests');
      assert.equal(fs.readFileSync(marker, 'utf8'), markerSource);
    } else {
      assert(checks.markerExists, 'ToolUse/intermediate control must permit valid continuation');
      assert(childCalls > 1, 'Intermediate task must continue after toolUse');
      assert.equal(result.text, finalText);
      assert.equal(notices.filter(message => message.details?.messageId === steer.messageId).length, 0, 'Intermediate control was falsely declared terminal');
      if (expectTerminal) assert.equal(readJson(path.join(run, 'terminal-controls', `${steer.messageId}.json`))?.state, 'delivered', 'Native context never consumed valid intermediate control');
    }
    if (checks.markerExists) fs.cpSync(path.join(cwd, 'src'), path.join(evidence, 'native-source-writes'), { recursive: true });
    json(path.join(evidence, 'scenario-result.json'), { scenario, checks, handle, steer, result, late, receipt, notices, markerContent: checks.markerExists ? fs.readFileSync(marker, 'utf8') : null });
    reports.push({ scenario, ...checks, runId: handle.id, queuedMessageId: steer.messageId, finalAnswerReceiptId: receipt?.id });
    const exit = await activeParent.close();
    assert.equal(exit.code, 0, `Native parent did not close cleanly: ${JSON.stringify(exit)}`);
    activeParent = undefined;
    await new Promise(resolve => activeServer.close(resolve)); activeServer = undefined;
  }
} catch (error) { failure ??= error; }
finally {
  if (activeParent) { owned.push(...descendants(activeParent.child.pid)); await activeParent.close().catch(error => { failure ??= error; }); }
  if (activeServer) { activeServer.closeAllConnections(); await new Promise(resolve => activeServer.close(resolve)); }
  // Only processes whose observed PID birth matches our native proof tree. No
  // global name matching. On failures retain scratch if custody is uncertain.
  for (const item of owned.filter(stillOwnedAlive)) { try { process.kill(item.pid, 'SIGKILL'); } catch {} }
  const remaining = owned.filter(stillOwnedAlive);
  if (remaining.length) failure ??= new Error(`Owned native process stop unconfirmed: ${JSON.stringify(remaining)}`);
  const summary = { passed: !failure, expectTerminal, route: 'native RPC parent -> public fabric_exec agents.spawn/steer/wait/followUp -> compiled runtime AgentMessageRouter local owner -> real ProcessTransport/worker/native Pi task', queuedRefusalRoute: expectTerminal && !failure ? 'runtime onTerminalNotice -> native parent custom message, exact original sender/message/receipt (same-owner route; no remote control-plane ACK claim)' : 'Baseline: no sender terminal notice; candidate notice route not yet qualified', slowShutdownQualification: 'Not exercised: no native shutdown hook injected', fabricRoot, nativePi: NATIVE_PI, artifacts: { nativePi: hash(NATIVE_PI), extension: hash(extension), worker: hash(worker) }, scratch, scratchRemoved: !remaining.length, remainingOwned: remaining, scenarios: reports, error: failure?.stack };
  if (!remaining.length) fs.rmSync(scratch, { recursive: true, force: true });
  json(path.join(evidenceDir, 'summary.json'), summary);
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}
if (failure) process.exitCode = 1;
