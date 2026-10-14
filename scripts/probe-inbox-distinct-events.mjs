// Real persisted Pi Main RPC proof: two distinct durable, addressed answer events sharing a key.
// Only model transport is deterministic/keyless; host capabilities, receipts, inbox, and wake timer are native.
// Usage: PI_FABRIC_PROBE_PI=/path/to/pi node scripts/probe-inbox-distinct-events.mjs dist/index.js "$TASK_OUT"
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { pathToFileURL, fileURLToPath } from 'node:url';

const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const candidate = path.resolve(process.argv[2] ?? path.join(lane, 'dist', 'index.js'));
const dist = path.dirname(candidate);
const outArg = process.argv[3] ?? process.env.TASK_OUT;
assert.ok(outArg, 'Supply a retained, owned output directory as argv[3] or TASK_OUT');
assert.ok(fs.existsSync(candidate), `Missing built entry ${candidate}`);
const meshModule = path.join(dist, 'mesh.js');
assert.ok(fs.existsSync(meshModule), `Missing built MeshStore module ${meshModule}`);
const out = path.resolve(outArg);
fs.mkdirSync(out, { recursive: true });
const runDir = path.join(out, `native-inbox-distinct-${Date.now()}-${randomUUID()}`);
const profile = path.join(runDir, 'profile');
const cwd = path.join(runDir, 'workspace');
const home = path.join(runDir, 'home');
const meshRoot = path.join(runDir, 'mesh');
const sessions = path.join(runDir, 'sessions');
const tmp = path.join(runDir, 'tmp');
for (const dir of [runDir, profile, cwd, home, meshRoot, sessions, tmp]) fs.mkdirSync(dir, { recursive: true });

const fixture = path.join(lane, 'tests/fixtures/inbox-distinct-events-cli-provider.ts');
const pi = process.env.PI_FABRIC_PROBE_PI ?? 'pi';
const inferenceFile = path.join(runDir, 'inferences.jsonl');
const hostFile = path.join(runDir, 'host.jsonl');
const rpcFile = path.join(runDir, 'rpc.jsonl');
const stderrFile = path.join(runDir, 'stderr.log');
for (const file of [inferenceFile, hostFile, rpcFile, stderrFile]) fs.writeFileSync(file, '');
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({
  compaction: { enabled: false }, retry: { enabled: false },
}));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({
  fullCodeMode: true,
  prewalk: { enabled: false },
  components: [],
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' },
  executor: { kernel: 'typescript', timeoutMs: 120000 },
  mesh: { enabled: true, announce: true, maxEventBytes: 65536, followUpFlushMs: 1 },
}));
const env = {
  PATH: process.env.PATH ?? '',
  HOME: home,
  TMPDIR: tmp,
  PI_CODING_AGENT_DIR: profile,
  PI_OFFLINE: '1',
  PI_FABRIC_MESH_ROOT: meshRoot,
  PI_FABRIC_INBOX_WAKE_MS: '100',
  PI_FABRIC_INBOX_WAKE_COOLDOWN_MS: '0',
  INBOX_PROOF_INFERENCES: inferenceFile,
  INBOX_PROOF_HOST: hostFile,
};

const startedAt = Date.now();
const totalDeadline = startedAt + 280000;
const remaining = () => Math.max(0, totalDeadline - Date.now());
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const readJsonl = file => {
  if (!fs.existsSync(file)) return [];
  const lines = fs.readFileSync(file, 'utf8').split('\n').filter(Boolean);
  const records = [];
  for (let index = 0; index < lines.length; index++) {
    try { records.push(JSON.parse(lines[index])); }
    catch (error) { if (index !== lines.length - 1) throw error; }
  }
  return records;
};
const readSession = file => readJsonl(file);
const inferences = () => readJsonl(inferenceFile);
const hostRecords = () => readJsonl(hostFile);
const waitFor = async (predicate, label, timeoutMs = 10000) => {
  const deadline = Math.min(totalDeadline, Date.now() + timeoutMs);
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${label} (${Date.now() - startedAt} ms into proof)`);
    await sleep(Math.min(100, Math.max(1, deadline - Date.now())));
  }
};

const { MeshStore } = await import(pathToFileURL(meshModule).href);
const mesh = new MeshStore(meshRoot, 65536, 500);
const allMeshEvents = () => mesh.read({ after: 0, limit: 10000 });
const wakes = () => allMeshEvents().filter(event => event.topic === 'fabric.inbox.wake');
let sessionFile;
let state;
let current;
let proofSender;
let rpcId = 0;
const exits = [];
const prompts = [];
const runRecords = [];
const settled = () => runRecords.filter(item => item.record.type === 'agent_settled');
const inboxEntries = () => readSession(sessionFile).filter(entry =>
  entry.type === 'custom_message' && entry.customType === 'pi-fabric-inbox');
const inboxStateKey = rootId => `topology/inbox/${createHash('sha256').update(rootId).digest('hex').slice(0, 32)}`;
const inboxState = rootId => mesh.get(inboxStateKey(rootId), { fresh: true })?.value;
const eventReceipts = id => inboxEntries().filter(entry => entry.details?.ids?.includes(id));
const wakeReceipts = id => wakes().filter(event => Array.isArray(event.data?.ids) && event.data.ids.includes(id));
const textOf = value => typeof value === 'string' ? value : Array.isArray(value)
  ? value.map(part => typeof part === 'string' ? part : String(part?.text ?? '')).join('') : '';
let rootId;
let failure;
let proof;

const launch = async () => {
  const args = [
    '--mode', 'rpc', '--offline', '--no-extensions', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files',
    '--approve', '--provider', 'inbox-distinct-proof', '--model', 'offline', '-e', candidate, '-e', fixture,
    '--session-dir', sessions,
  ];
  fs.appendFileSync(path.join(runDir, 'commands.jsonl'), JSON.stringify({
    executable: pi, args, cwd,
    candidateSha256: createHash('sha256').update(fs.readFileSync(candidate)).digest('hex'),
    meshModule,
  }) + '\n');
  const child = spawn(pi, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
  const pending = new Map();
  let buffer = '';
  let stderr = '';
  let spawnError;
  let stopping = false;
  let resolveExit;
  const exit = new Promise(resolve => { resolveExit = resolve; });
  child.once('exit', (code, signal) => {
    const result = { pid: child.pid, code, signal };
    exits.push(result);
    for (const { timer, reject } of pending.values()) {
      clearTimeout(timer);
      reject(new Error('Pi RPC process exited before its response'));
    }
    pending.clear();
    resolveExit(result);
  });
  child.once('error', error => {
    spawnError = error;
    for (const { timer, reject } of pending.values()) {
      clearTimeout(timer);
      reject(error);
    }
    pending.clear();
    const result = { pid: child.pid, code: null, signal: null, error: error.message };
    exits.push(result);
    resolveExit(result);
  });
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', chunk => {
    buffer += chunk;
    for (;;) {
      const lf = buffer.indexOf('\n');
      if (lf < 0) break;
      const line = buffer.slice(0, lf).replace(/\r$/, '');
      buffer = buffer.slice(lf + 1);
      if (!line.trim()) continue;
      let record;
      try { record = JSON.parse(line); }
      catch (error) { spawnError ??= new Error(`Invalid RPC JSONL: ${String(error)}`); continue; }
      const receivedAt = Date.now();
      runRecords.push({ receivedAt, record });
      fs.appendFileSync(rpcFile, JSON.stringify({ receivedAt, record }) + '\n');
      if (record.type === 'response' && pending.has(record.id)) {
        const { resolve, reject, timer } = pending.get(record.id);
        clearTimeout(timer);
        pending.delete(record.id);
        if (record.success) resolve(record.data);
        else reject(new Error(record.error ?? `RPC command failed: ${record.command}`));
      }
    }
  });
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', chunk => { stderr += chunk; });
  const request = data => new Promise((resolve, reject) => {
    if (spawnError) { reject(spawnError); return; }
    const id = `proof-${++rpcId}`;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`RPC timeout: ${data.type}`));
    }, Math.min(20000, remaining()));
    pending.set(id, { resolve, reject, timer });
    child.stdin.write(JSON.stringify({ id, ...data }) + '\n');
  });
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    for (const { timer, reject } of pending.values()) {
      clearTimeout(timer);
      reject(new Error('Pi RPC closing through stdin EOF'));
    }
    pending.clear();
    // RPC's documented orderly shutdown: closing stdin lets Pi dispose its active runtime.
    if (child.exitCode === null && child.signalCode === null && !child.stdin.destroyed) child.stdin.end();
    let timeout;
    const exited = await Promise.race([
      exit.then(() => true),
      new Promise(resolve => { timeout = setTimeout(() => resolve(false), 15000); }),
    ]);
    clearTimeout(timeout);
    fs.writeFileSync(stderrFile, stderr);
    if (!exited) throw new Error('Pi RPC did not exit after orderly stdin EOF; no signal/kill fallback is used');
    if (spawnError) throw spawnError;
  };
  current = { child, request, stop, exit };
  state = await request({ type: 'get_state' });
  sessionFile = state.sessionFile;
  rootId = `session:${state.sessionId}`;
  assert.ok(sessionFile, 'Pi must use a persisted native session');
  assert.ok(state.sessionId, 'Pi must report a native session id');
  return current;
};

const publishDurably = async ({ key, deliveryId, text, addressed = true }) => {
  assert.ok(proofSender, 'One stable native proof sender must be used for both events and the replay');
  const event = await mesh.publish({
    topic: 'fleet.work.inbox.cli',
    kind: 'answer',
    from: proofSender,
    ...(addressed ? { to: rootId } : {}),
    data: { key, ...(deliveryId === undefined ? {} : { deliveryId }) },
    text,
  });
  const publishedAt = event.createdAt;
  const durability = await mesh.sync(5000);
  assert.ok(durability.through >= event.sequence, `MeshStore.sync must durably cover event ${event.id}`);
  const stored = mesh.read({ after: event.sequence - 1, limit: 1 }).find(item => item.id === event.id);
  assert.ok(stored, `MeshStore must read back published event ${event.id}`);
  assert.equal(stored.to, addressed ? rootId : undefined);
  assert.equal(stored.kind, 'answer');
  assert.equal(stored.data?.key, key);
  assert.equal(stored.data?.deliveryId, deliveryId);
  assert.equal(stored.text, text);
  return {
    id: event.id,
    sequence: event.sequence,
    publishedAt,
    publicationReturnedAt: Date.now(),
    durableThrough: durability.through,
    senderId: proofSender.id,
    text,
    key,
    deliveryId,
    addressed,
  };
};

const proveNativeDelivery = async (delivery, baseline) => {
  const deadlineMs = Math.max(0, delivery.publishedAt + 90000 - Date.now());
  await waitFor(() => {
    const newInferences = inferences().slice(baseline.inferences);
    const stateNow = inboxState(rootId);
    return eventReceipts(delivery.id).length === 1 && wakeReceipts(delivery.id).length === 1 &&
      newInferences.some(inference => inference.markers?.includes(delivery.text)) &&
      settled().length >= baseline.settled + 1 && Number(stateNow?.after) >= delivery.sequence;
  }, `native inbox delivery, wake, inference, and settle for ${delivery.id} within 90 seconds`, deadlineMs);

  const wake = wakeReceipts(delivery.id);
  const receipt = eventReceipts(delivery.id);
  const newInferences = inferences().slice(baseline.inferences);
  const settledDelta = settled().slice(baseline.settled);
  assert.equal(wake.length, 1, `one native inbox wake must name ${delivery.id}`);
  assert.deepEqual(wake[0].data.ids, [delivery.id]);
  assert.equal(receipt.length, 1, `one native session inbox receipt must name ${delivery.id}`);
  assert.deepEqual(receipt[0].details.ids, [delivery.id]);
  assert.ok(textOf(receipt[0].content).includes(delivery.text), 'persisted native inbox receipt must carry real event text');
  assert.equal(newInferences.length, 1, `exactly one new assistant inference must follow ${delivery.id}`);
  assert.ok(newInferences[0].markers.includes(delivery.text), 'the assistant inference must see the exact published event text');
  assert.equal(newInferences[0].stage, 'inbox-ack');
  assert.equal(newInferences[0].assistantText, 'NATIVE_INBOX_ACK');
  assert.ok(newInferences[0].inboxFramePresent);
  assert.equal(settledDelta.length, 1, `exactly one native agent_settled must follow ${delivery.id}`);
  assert.equal(settledDelta[0].record.outcome, 'completed');
  const wokeAt = wake[0].createdAt;
  const settledAt = settledDelta[0].receivedAt;
  const inferenceAt = newInferences[0].at;
  assert.ok(wokeAt - delivery.publishedAt >= 60000, 'ordinary addressed answer must respect the native 60-second steer grace');
  assert.ok(wokeAt - delivery.publishedAt <= 90000, 'native wake must occur within 90 seconds of durable publication');
  assert.ok(inferenceAt - delivery.publishedAt <= 90000, 'assistant inference must occur within 90 seconds of durable publication');
  assert.ok(settledAt - delivery.publishedAt <= 90000, 'native agent_settled must occur within 90 seconds of durable publication');
  await sleep(500);
  assert.equal(settled().length, baseline.settled + 1, `no second native settle may follow ${delivery.id}`);
  assert.equal(inferences().length, baseline.inferences + 1, `no duplicate model call may follow ${delivery.id}`);
  assert.equal(wakeReceipts(delivery.id).length, 1, `no duplicate native wake may follow ${delivery.id}`);
  return {
    ...delivery,
    wokeAt,
    wakeLatencyMs: wokeAt - delivery.publishedAt,
    settledAt,
    settleLatencyMs: settledAt - delivery.publishedAt,
    inference: newInferences[0],
    nativeInboxEntryId: receipt[0].id,
    nativeWakeEventId: wake[0].id,
    inboxStateAfter: inboxState(rootId)?.after,
    nativeAgentSettled: settledDelta[0].record,
  };
};

try {
  await launch();
  await waitFor(() => hostRecords().length > 0, 'actual extension session_start host-capability record', 15000);
  const host = hostRecords().at(-1);
  assert.equal(host.sessionId, state.sessionId);
  assert.equal(host.mode, 'rpc');
  assert.equal(host.testCapabilityOverride, false, 'the test-only host capability Symbol must not be set');
  assert.equal(host.capabilities?.triggeredMessageQueuesBehindPreflight, true, 'native host must advertise triggered-message queueing');
  assert.equal(host.capabilities?.promptPendingVisible, true, 'native host must expose prompt-pending preflight state');
  assert.equal(host.isPromptPending, 'function');

  const activationSettled = settled().length;
  prompts.push('NATIVE_INBOX_ACTIVATE');
  await current.request({ type: 'prompt', message: prompts.at(-1) });
  await waitFor(() => settled().length > activationSettled, 'activation prompt agent_settled', 30000);
  const activationInferences = inferences();
  assert.equal(activationInferences.length, 2, 'offline activation must make exactly two actual provider inferences');
  assert.deepEqual(activationInferences.map(item => item.stage), ['activation-fabric_exec', 'activation-ready']);
  assert.equal(activationInferences[0].toolCode, 'return 1;');
  assert.equal(activationInferences[1].assistantText, 'NATIVE_INBOX_READY');
  assert.ok(runRecords.some(item => item.record.type === 'tool_execution_start' && item.record.toolName === 'fabric_exec'),
    'the native RPC process must execute the activation fabric_exec');
  assert.equal(settled().length, activationSettled + 1);
  const sessionHeader = readSession(sessionFile)[0];
  assert.equal(sessionHeader?.type, 'session');
  assert.equal(sessionHeader?.id, state.sessionId, 'persisted native session header must match RPC get_state');

  proofSender = { id: 'fleet-tool:lane-watcher', name: 'lane-watcher', kind: 'main' };
  const watcherKey = `watcher-shared-${randomUUID()}`;
  const markerA = `ANSWER_EVENT_A_${randomUUID()}`;
  const markerB = `ANSWER_EVENT_B_${randomUUID()}`;
  const baselineA = { settled: settled().length, inferences: inferences().length };
  const firstEvent = await publishDurably({ key: watcherKey, text: markerA });
  const first = await proveNativeDelivery(firstEvent, baselineA);

  const baselineB = { settled: settled().length, inferences: inferences().length };
  const secondEvent = await publishDurably({ key: watcherKey, text: markerB });
  const second = await proveNativeDelivery(secondEvent, baselineB);
  assert.equal(first.deliveryId, undefined);
  assert.equal(second.deliveryId, undefined);
  assert.notEqual(first.id, second.id);
  assert.equal(first.key, second.key, 'both distinct deliveries must share exactly one watcher key');
  assert.equal(first.senderId, second.senderId, 'both deliveries must have the same real mesh sender identity');

  // Separate explicit-identity control: the primary incident events above carry only data.key.
  const controlId = `CONTROL-${randomUUID()}`;
  const control = await proveNativeDelivery(await publishDurably({ key: watcherKey, deliveryId: controlId,
    text: `ANSWER_CONTROL_${randomUUID()}` }), { settled: settled().length, inferences: inferences().length });
  const baselineNegative = { settled: settled().length, inferences: inferences().length, wakes: wakes().length };
  const replayText = `EXPLICIT_DELIVERYID_REPLAY_${randomUUID()}`;
  const unaddressedText = `UNADDRESSED_EVENT_${randomUUID()}`;
  const replay = await publishDurably({ key: watcherKey, deliveryId: controlId, text: replayText });
  const unaddressed = await publishDurably({ key: watcherKey, deliveryId: `UNADDRESSED-${randomUUID()}`, text: unaddressedText, addressed: false });
  assert.equal(replay.senderId, first.senderId, 'true deliveryId replay must use the original sender identity');
  assert.equal(replay.deliveryId, control.deliveryId, 'true deliveryId replay must repeat the delivered explicit identity');
  await waitFor(() => Number(inboxState(rootId)?.after) >= unaddressed.sequence,
    'native inbox cursor to pass the >60-second explicit replay and unaddressed event', 75000);
  assert.ok(Date.now() - replay.publishedAt >= 60000, 'explicit deliveryId replay must be observed after normal grace');
  assert.ok(Date.now() - unaddressed.publishedAt >= 60000, 'unaddressed control must be observed after normal grace');
  await sleep(750);
  assert.equal(settled().length, baselineNegative.settled, 'replay and unaddressed controls must not start another native turn');
  assert.equal(inferences().length, baselineNegative.inferences, 'replay and unaddressed controls must not cause another assistant inference');
  assert.equal(wakes().length, baselineNegative.wakes, 'replay and unaddressed controls must not publish a native inbox wake');
  assert.equal(eventReceipts(replay.id).length, 0, 'true deliveryId replay must not get a native inbox receipt');
  assert.equal(eventReceipts(unaddressed.id).length, 0, 'unaddressed event must not get a native inbox receipt');
  assert.equal(wakeReceipts(replay.id).length, 0);
  assert.equal(wakeReceipts(unaddressed.id).length, 0);
  const finalInferences = inferences();
  assert.ok(!finalInferences.some(item => item.markers?.includes(replayText)), 'model context must not include explicit replay text');
  assert.ok(!finalInferences.some(item => item.markers?.includes(unaddressedText)), 'model context must not include unaddressed event text');
  const inboxIds = inboxEntries().flatMap(entry => entry.details?.ids ?? []);
  assert.deepEqual(inboxIds, [first.id, second.id, control.id], 'only the two distinct key-only events and explicit-identity control are delivered');
  assert.equal(prompts.length, 1, 'only the one activation prompt is allowed; both event turns must be promptless');
  assert.ok(Date.now() - startedAt < 280000, 'proof must complete inside its 280-second hard work budget');

  proof = {
    passed: true,
    durationMs: Date.now() - startedAt,
    sessionId: state.sessionId,
    sessionFile,
    rootId,
    rootInboxStateKey: inboxStateKey(rootId),
    nativeHost: host,
    activation: { promptCount: prompts.length, inferences: activationInferences, agentSettled: settled()[activationSettled].record },
    watcherKey,
    addressedDistinctEvents: [first, second],
    explicitIdentityControl: control,
    negativeControls: {
      explicitDeliveryIdReplay: replay,
      unaddressedEvent: unaddressed,
      observedAfterGrace: true,
      settledDelta: settled().length - baselineNegative.settled,
      inferenceDelta: inferences().length - baselineNegative.inferences,
      wakeDelta: wakes().length - baselineNegative.wakes,
      rootInboxAfter: inboxState(rootId)?.after,
    },
    promptCount: prompts.length,
    rpcAgentSettled: settled().map(item => ({ at: item.receivedAt, outcome: item.record.outcome })),
    exits,
    retainedRunDirectory: runDir,
  };
} catch (error) {
  failure = error;
  console.error(error);
} finally {
  if (current) {
    try { await current.stop(); }
    catch (error) { failure ??= error; console.error(error); }
  }
  if (exits.length && !exits.every(exit => exit.code === 0 && exit.signal === null)) {
    failure ??= new Error(`Pi RPC did not exit normally: ${JSON.stringify(exits)}`);
  }
  if (proof) {
    proof.passed = !failure;
    proof.durationMs = Date.now() - startedAt;
    if (failure) proof.failure = failure instanceof Error ? failure.message : String(failure);
    proof.exits = exits;
  } else {
    proof = {
      passed: false,
      durationMs: Date.now() - startedAt,
      sessionId: state?.sessionId,
      sessionFile,
      rootId,
      nativeHost: hostRecords().at(-1),
      promptCount: prompts.length,
      observedInferences: inferences(),
      exits,
      failure: failure instanceof Error ? failure.message : String(failure),
      retainedRunDirectory: runDir,
    };
  }
  fs.writeFileSync(path.join(runDir, 'summary.json'), JSON.stringify(proof, null, 2) + '\n');
}

assert.ok(exits.every(exit => exit.code === 0 && exit.signal === null), `Pi RPC must exit normally: ${JSON.stringify(exits)}`);
if (failure) process.exitCode = 1;
else console.log(JSON.stringify({ passed: true, sessionId: state.sessionId, durationMs: proof.durationMs, runDir }));
