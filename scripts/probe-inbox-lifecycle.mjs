#!/usr/bin/env node
// LIVE native Pi TUI lifecycle proof, not RPC or an SDK/custom-host substitute.
// Only inference is synthetic. No agents are spawned by the model; one owned Pi
// CLI runs in a real PTY. Run beneath smarty-reap for an outer containment bound.
// Usage: node scripts/probe-inbox-lifecycle.mjs dist/index.js "$TASK_OUT" [--tee]
// Select the native host via PI_FABRIC_PROBE_PI=/absolute/path/to/cli.js or --pi-cli.
// Selection is required; --pi-cli takes precedence over the environment variable.
// Requires Node >=22, Python 3 and POSIX PTYs. Exit 0 means all assertions passed;
// unavailable host/candidate/PTY is a failure, never a silent acceptance skip.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { stripVTControlCharacters } from 'node:util';
import { fileURLToPath, pathToFileURL } from 'node:url';

const lane = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const usage = 'node scripts/probe-inbox-lifecycle.mjs [dist/index.js] OUTPUT_DIR [--tee] [--budget-ms 180000] [--pi-cli /path/to/cli.js]\nSelect the native Pi CLI with PI_FABRIC_PROBE_PI or --pi-cli (required; --pi-cli takes precedence).';
const positional = [];
let budgetMs = 180000, tee = Boolean(process.stdout.isTTY);
// Pi 1.2.0 isolates message_end details per extension handler. The npm 1.1.0
// host does not, and is NOT a valid native-receipt regression host.
let cli = process.env.PI_FABRIC_PROBE_PI;
for (let i = 2; i < process.argv.length; i++) {
  const arg = process.argv[i];
  if (arg === '--help' || arg === '-h') { console.log(usage); process.exit(0); }
  if (arg === '--tee') tee = true;
  else if (arg === '--no-tee') tee = false;
  else if (arg === '--budget-ms') budgetMs = Number(process.argv[++i]);
  else if (arg === '--pi-cli') cli = process.argv[++i];
  else if (arg.startsWith('--')) throw new Error(`Unknown argument ${arg}\n${usage}`);
  else positional.push(arg);
}
assert.ok(positional.length <= 2 && (positional[1] ?? process.env.TASK_OUT), usage);
assert.ok(Number.isSafeInteger(budgetMs) && budgetMs >= 30000 && budgetMs <= 600000, '--budget-ms must be 30000..600000');
assert.notEqual(process.platform, 'win32', 'This live driver requires POSIX PTYs');
const candidate = path.resolve(positional[0] ?? path.join(lane, 'dist/index.js'));
const fixture = path.join(lane, 'tests/fixtures/inbox-lifecycle-cli-provider.ts');
const meshModule = path.join(path.dirname(candidate), 'mesh.js');
assert.ok(typeof cli === 'string' && cli.trim().length > 0,
  'Select the actual Pi CLI with --pi-cli /path/to/cli.js or PI_FABRIC_PROBE_PI=/path/to/cli.js (required; no default host).');
assert.ok(!cli.includes('/role-pi/'), 'Use the actual Pi executable CLI, not the role-pi launcher');
cli = path.resolve(cli);
for (const file of [candidate, fixture, meshModule, cli]) assert.ok(fs.statSync(file).isFile(), `Missing file (build/install first): ${file}`);
const sha256 = value => createHash('sha256').update(value).digest('hex');
const hashFile = file => sha256(fs.readFileSync(file));
const distSnapshot = () => {
  const result = {};
  const visit = dir => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const file = path.join(dir, entry.name);
      if (entry.isDirectory()) visit(file);
      else if (entry.isFile() && /\.(?:m?js|json)$/.test(file)) result[path.relative(path.dirname(candidate), file)] = hashFile(file);
    }
  };
  visit(path.dirname(candidate));
  return result;
};
const candidateFiles = distSnapshot();
const fixtureSha256 = hashFile(fixture), cliSha256 = hashFile(cli);
const startedAt = Date.now(), deadline = startedAt + budgetMs;
const runId = randomUUID();
const out = path.resolve(positional[1] ?? process.env.TASK_OUT);
fs.mkdirSync(out, { recursive: true });
const runDir = fs.mkdtempSync(path.join(out, 'native-inbox-lifecycle-'));
const dirs = Object.fromEntries(['profile', 'workspace', 'home', 'mesh', 'sessions', 'tmp'].map(name => [name, path.join(runDir, name)]));
for (const dir of Object.values(dirs)) fs.mkdirSync(dir, { mode: 0o700 });
const files = Object.fromEntries(['host.jsonl', 'inferences.jsonl', 'commands.jsonl', 'observations.jsonl',
  'tui.stdout.raw', 'tui.stderr.raw', 'bridge.stderr.raw'].map(name => [name, path.join(runDir, name)]));
for (const file of Object.values(files)) fs.writeFileSync(file, '', { flag: 'wx' });
const append = (file, value) => fs.appendFileSync(file, JSON.stringify({ at: Date.now(), ...value }) + '\n');
const phase = (name, details = {}) => {
  append(files['observations.jsonl'], { phase: name, ...details });
  process.stderr.write(`[inbox-lifecycle] ${name}\n`);
};
const jsonl = (file, final = false) => {
  if (!file || !fs.existsSync(file)) return [];
  const raw = fs.readFileSync(file, 'utf8');
  const lines = raw.split('\n');
  if (!final && !raw.endsWith('\n')) lines.pop();
  return lines.filter(Boolean).map(line => JSON.parse(line));
};
const hosts = () => jsonl(files['host.jsonl']);
const inferences = () => jsonl(files['inferences.jsonl']);
const starts = () => hosts().filter(record => record.event === 'session_start');
const settles = () => hosts().filter(record => record.event === 'agent_settled');
let sessionFile, sessionId, rootId, mesh, child, childExit, failure, shuttingDown = false;
const entries = () => jsonl(sessionFile);
const customs = () => entries().filter(entry => entry.type === 'custom_message');
const allEvents = () => mesh.read({ after: 0, limit: 10000 });
const inboxReceipts = id => customs().filter(entry => entry.customType === 'pi-fabric-inbox' && entry.details?.ids?.includes(id));
const followUpReceipts = marker => customs().filter(entry => entry.customType === 'pi-fabric-agent-message' && String(entry.content).includes(marker));
const acknowledgements = marker => inferences().filter(record => record.stage === 'delivery-ack' && record.markers.includes(marker));
const textOf = content => typeof content === 'string' ? content : Array.isArray(content)
  ? content.filter(part => part.type === 'text').map(part => part.text).join('\n') : '';
const ackEntries = marker => entries().filter(entry => entry.type === 'message' && entry.message?.role === 'assistant' &&
  textOf(entry.message.content).split('\n').includes(`LIFECYCLE_ACK ${marker}`));
const assertAcknowledgement = marker => {
  assert.equal(acknowledgements(marker).length, 1, `Exactly one provider observation of ${marker}, not one per redraw`);
  const persisted = ackEntries(marker);
  assert.equal(persisted.length, 1, `Exactly one native persisted assistant acknowledgement of ${marker}`);
  const turns = hosts().filter(record => record.event === 'turn_end' && record.assistantText?.split('\n').includes(`LIFECYCLE_ACK ${marker}`));
  assert.equal(turns.length, 1, `Exactly one observed native turn ends by acknowledging ${marker}`);
  assert.equal(turns[0].messageEntryId, persisted[0].id, 'Observed native turn must identify the actual persisted assistant entry');
};
const inboxStateKey = () => `topology/inbox/${sha256(rootId).slice(0, 32)}`;
const inboxState = () => mesh.get(inboxStateKey(), { fresh: true })?.value;
const expectedFollowUps = [], expectedInbox = [], replays = [];
const watchers = [];
let changed;
const notify = () => changed?.();
const watch = dir => {
  const watcher = fs.watch(dir, notify);
  watcher.on('error', error => { failure ??= error; notify(); });
  watchers.push(watcher);
};
// Filesystem/child events drive observation. A 250ms bounded safety-net covers
// coalesced/missed fs.watch events; these are proof waits, not product polling.
const waitFor = async (predicate, label, timeoutMs = 30000) => {
  const until = Math.min(deadline, Date.now() + timeoutMs);
  for (;;) {
    if (failure) throw failure;
    if (childExit && !shuttingDown) throw new Error(`Pi exited during ${label}: ${JSON.stringify(childExit)}`);
    if (predicate()) return;
    if (Date.now() >= until) throw new Error(`Timed out waiting for ${label} (${Date.now() - startedAt}ms into ${budgetMs}ms budget)`);
    await new Promise(resolve => {
      const timer = setTimeout(done, Math.min(250, until - Date.now()));
      function done() { clearTimeout(timer); if (changed === done) changed = undefined; resolve(); }
      changed = done;
    });
  }
};
const boundedQuiet = async (ms, verify) => {
  const until = Date.now() + ms;
  assert.ok(until < deadline, 'Not enough proof budget for the declared quiet window');
  await waitFor(() => { verify(); return Date.now() >= until; }, 'bounded quiet observation', ms + 1000);
};

fs.writeFileSync(path.join(dirs.profile, 'settings.json'), JSON.stringify({
  compaction: { enabled: false, keepRecentTokens: 256 }, retry: { enabled: false }, quietStartup: true,
}));
fs.writeFileSync(path.join(dirs.profile, 'fabric.json'), JSON.stringify({
  fullCodeMode: true, prewalk: { enabled: false }, components: [],
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' },
  executor: { kernel: 'typescript', timeoutMs: 30000 },
  mesh: { enabled: true, announce: true, maxEventBytes: 65536, followUpFlushMs: 1 },
}));
// Explicit allowlist: no gateway/API keys, parent profile/identity or live mesh.
const env = {
  PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: dirs.home, TMPDIR: dirs.tmp,
  TERM: 'xterm-256color', LANG: 'C.UTF-8', PI_OFFLINE: '1', PI_CODING_AGENT_DIR: dirs.profile,
  PI_FABRIC_MESH_ROOT: dirs.mesh, PI_FABRIC_INBOX_WAKE_MS: '100', PI_FABRIC_INBOX_WAKE_COOLDOWN_MS: '0',
  INBOX_LIFECYCLE_HOST: files['host.jsonl'], INBOX_LIFECYCLE_INFERENCES: files['inferences.jsonl'],
};
const args = ['--offline', '--no-extensions', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files',
  '--approve', '--provider', 'inbox-lifecycle-proof', '--model', 'offline', '--tui-mode', 'regular',
  '-e', candidate, '-e', fixture, '--session-dir', dirs.sessions];
const argv = /\.[cm]?js$/.test(cli) ? [process.execPath, cli, ...args] : [cli, ...args];
const configFile = path.join(runDir, 'pty-config.json');
fs.writeFileSync(configFile, JSON.stringify({ argv, cwd: dirs.workspace, env,
  info: path.join(runDir, 'pty-child.json'), stderr: files['tui.stderr.raw'], budgetMs: budgetMs + 10000 }));
append(files['commands.jsonl'], { executable: argv[0], args: argv.slice(1), cwd: dirs.workspace, env,
  candidateSha256: hashFile(candidate), candidateFiles, fixtureSha256, cliSha256,
  terminal: { rows: 44, columns: 140, stderr: 'separate raw pipe', stdinAndStdout: 'native PTY' }, budgetMs });

// Python's standard-library PTY avoids a node-pty dependency. It records the real
// stderr separately, relays terminal bytes verbatim, and reaps only its own child
// session/process group. EOF/signals/deadline all enter the same bounded cleanup.
const bridge = String.raw`
import errno, fcntl, json, os, pty, selectors, signal, struct, sys, termios, time
cfg = json.load(open(sys.argv[1]))
err_r, err_w = os.pipe()
pid, master = pty.fork()
if pid == 0:
    os.close(err_r)
    os.dup2(err_w, 2)
    os.close(err_w)
    os.chdir(cfg['cwd'])
    os.execvpe(cfg['argv'][0], cfg['argv'], cfg['env'])
os.close(err_w)
fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack('HHHH', 44, 140, 0, 0))
with open(cfg['info'], 'x') as info:
    json.dump({'pid': pid, 'pgid': pid, 'stdinIsPty': True, 'stdoutIsPty': True}, info)
sel = selectors.DefaultSelector()
for fd, kind in [(master, 'out'), (err_r, 'err'), (0, 'in')]:
    os.set_blocking(fd, False)
    sel.register(fd, selectors.EVENT_READ, kind)
stopping = None
status = None
end = time.monotonic() + cfg['budgetMs'] / 1000

def stop(*_):
    global stopping
    if stopping is None:
        stopping = time.monotonic()
        try: os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError: pass
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGHUP, stop)
pending = bytearray()
with open(cfg['stderr'], 'ab', buffering=0) as errors:
    try:
        while status is None or any(key.data in ('out', 'err') for key in sel.get_map().values()):
            if time.monotonic() >= end: stop()
            if stopping is not None and time.monotonic() - stopping >= 3:
                try: os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError: pass
            if pending:
                try:
                    written = os.write(master, pending)
                    del pending[:written]
                except BlockingIOError: pass
                except OSError: pending.clear()
            for key, _ in sel.select(0.1):
                try: data = os.read(key.fd, 65536)
                except BlockingIOError: continue
                except OSError as exc:
                    if exc.errno != errno.EIO: raise
                    data = b''
                if not data:
                    sel.unregister(key.fd)
                    if key.data == 'in' and status is None: stop()
                    continue
                if key.data == 'in': pending.extend(data)
                elif key.data == 'out':
                    view = memoryview(data)
                    while view:
                        written = os.write(1, view)
                        view = view[written:]
                else: errors.write(data)
            if status is None:
                done, result = os.waitpid(pid, os.WNOHANG)
                if done: status = result
            if status is not None and stopping is not None and time.monotonic() - stopping > 5: break
    finally:
        stop()
        if status is None:
            limit = time.monotonic() + 3
            while time.monotonic() < limit:
                done, result = os.waitpid(pid, os.WNOHANG)
                if done:
                    status = result
                    break
                time.sleep(0.05)
            if status is None:
                try: os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError: pass
                _, status = os.waitpid(pid, 0)
        sel.close()
        os.close(master)
        os.close(err_r)
sys.exit(os.waitstatus_to_exitcode(status) if os.WIFEXITED(status) else 128 + os.WTERMSIG(status))
`;
let exitPromise, hardTimer;
const send = text => {
  assert.ok(child && !childExit && !child.stdin.destroyed, 'PTY child is not accepting input');
  append(files['commands.jsonl'], { keyboard: text, bytesHex: Buffer.from(`${text}\r`).toString('hex') });
  child.stdin.write(`${text}\r`);
};
const validateNativeHost = host => {
  assert.equal(host.mode, 'tui', 'Required live acceptance must use actual interactive TUI, not RPC');
  assert.equal(host.testCapabilityOverride, false, 'Never install the test-only host capabilities Symbol');
  assert.equal(host.capabilities?.triggeredMessageQueuesBehindPreflight, true, 'Native triggered-message queue support is required');
  assert.equal(host.capabilities?.promptPendingVisible, true, 'Native preflight visibility is required');
  assert.equal(host.isPromptPending, 'function');
  assert.equal(host.sessionId, sessionId, 'literal reload must keep the same native session');
};
const assertDeliveries = () => {
  for (const item of expectedFollowUps) {
    const receipts = followUpReceipts(item.marker);
    assert.equal(receipts.length, 1, `Exactly one native followUp entry for ${item.marker}`);
    const items = receipts[0].details?.items ?? [receipts[0].details];
    assert.equal(items.filter(detail => detail?.id === item.nativeMessageId && detail.delivery === 'followUp').length, 1,
      'The actual agents.followUp receipt messageId must identify exactly one native received item');
    assertAcknowledgement(item.marker);
    const received = hosts().filter(record => record.event === 'native_custom_message' &&
      record.message.customType === 'pi-fabric-agent-message' &&
      (record.message.details?.items ?? [record.message.details]).some(detail => detail?.id === item.nativeMessageId));
    assert.equal(received.length, 1, 'Native message_end must observe the actual followUp messageId once');
  }
  for (const item of expectedInbox) {
    const receipts = inboxReceipts(item.event.id);
    assert.equal(receipts.length, 1, `Exactly one native addressed-inbox receipt for ${item.event.id}`);
    assert.ok(String(receipts[0].content).includes(item.marker));
    assertAcknowledgement(item.marker);
    const received = hosts().filter(record => record.event === 'native_custom_message' &&
      record.message.customType === 'pi-fabric-inbox' && record.message.details?.ids?.includes(item.event.id));
    assert.equal(received.length, 1, 'Native message_end must observe the durable event id once');
    const wakes = allEvents().filter(event => event.topic === 'fabric.inbox.wake' && event.data?.ids?.includes(item.event.id));
    assert.equal(wakes.length, 1, `Exactly one native wake for ${item.event.id}`);
    assert.equal(wakes[0].data.reason, 'addressed');
  }
};
const sendFollowUp = async label => {
  const marker = `LIFECYCLE_FU_${label}_${runId}`;
  const before = settles().length, toolsBefore = hosts().filter(record => record.event === 'tool_execution_end').length;
  send(`LIFECYCLE_SEND ${marker}`);
  await waitFor(() => settles().length > before && hosts().filter(record => record.event === 'tool_execution_end').length > toolsBefore &&
    followUpReceipts(marker).length > 0 && acknowledgements(marker).length > 0, `real agents.followUp ${label}`);
  const tool = hosts().filter(record => record.event === 'tool_execution_end').at(-1);
  assert.equal(tool.isError, false, 'The real fabric_exec / agents.followUp call must succeed');
  const apiResult = JSON.parse(tool.result.content.filter(part => part.type === 'text').map(part => part.text).join('\n'));
  assert.equal(apiResult.mainId, rootId);
  assert.equal(apiResult.receipt.queued, true);
  assert.ok(apiResult.receipt.messageId, 'Real agents.followUp must return its native messageId');
  const entry = followUpReceipts(marker)[0];
  const item = { marker, nativeMessageId: apiResult.receipt.messageId, nativeEntryId: entry.id,
    nativeTurnId: entry.provenance?.turnId, apiReceipt: apiResult.receipt, toolResult: tool.result };
  expectedFollowUps.push(item);
  assertDeliveries();
  phase('native-followUp-delivered', item);
  return item;
};
const sender = { id: 'fleet-tool:inbox-lifecycle-driver', name: 'Lifecycle proof sender', kind: 'main' };
const publishInbox = async (label, replay) => {
  const marker = replay?.marker ?? `LIFECYCLE_INBOX_${label}_${runId}`;
  const deliveryId = replay?.deliveryId ?? `lifecycle-inbox-${label}-${runId}`;
  const event = await mesh.publish({ topic: 'fleet.work.inbox.lifecycle', kind: 'answer', from: sender, to: rootId,
    text: replay ? `LIFECYCLE_REPLAY_${label}_${runId}` : marker, data: { key: `lifecycle-shared-${runId}`, deliveryId } });
  const durable = await mesh.sync(5000);
  assert.ok(durable.through >= event.sequence, 'Publication must be durable before claiming delivery');
  assert.equal(mesh.read({ after: event.sequence - 1, limit: 1 })[0]?.id, event.id);
  const item = { marker, deliveryId, event, durableThrough: durable.through };
  if (!replay) {
    await waitFor(() => inboxReceipts(event.id).length > 0 && acknowledgements(marker).length > 0 &&
      settles().at(-1)?.at >= acknowledgements(marker)[0]?.at, `native addressed-inbox delivery ${label}`);
    expectedInbox.push(item);
  } else {
    // A confirmed replay is not new exact-session work, so it does not bypass
    // the native 60s steer grace. Use the existing distinct-events probe's 75s
    // bound; never mistake an ordinary grace wait for a lost delivery.
    await waitFor(() => Number(inboxState()?.after) >= event.sequence, `native inbox cursor past replay ${label}`, 75000);
    assert.equal(inboxReceipts(event.id).length, 0, 'Repeated deliveryId must not get a new receipt');
    assert.ok(!inferences().some(record => record.input.includes(event.text)), 'Replay text must not reach model transport');
    replays.push({ kind: 'durable-addressed-inbox', ...item });
  }
  assertDeliveries();
  phase(replay ? 'durable-inbox-replay-passed-without-redelivery' : 'durable-addressed-inbox-delivered', item);
  return item;
};
const noise = () => {
  const stdout = stripVTControlCharacters(fs.readFileSync(files['tui.stdout.raw'], 'utf8')).replace(/\s+/g, ' ');
  const stderr = stripVTControlCharacters(fs.readFileSync(files['tui.stderr.raw'], 'utf8')).replace(/\s+/g, ' ');
  const patterns = [/ignored[ -]unissued(?:[ -]root[ -]inbox)?[ -]carrier/i, /no saved terminal result/i,
    /participant heartbeat\/change refresh[^\n]*background operation failed/i];
  return patterns.flatMap(pattern => [stdout, stderr].flatMap((text, index) => {
    const match = pattern.exec(text);
    return match ? [{ stream: index ? 'stderr' : 'stdout', pattern: String(pattern), matched: match[0] }] : [];
  }));
};
const stop = async () => {
  shuttingDown = true;
  if (!child || childExit) return;
  // Supported literal /quit first. The bridge owns all signal fallback/reaping.
  try { send('/quit'); } catch { /* bridge may already be closing */ }
  let timer;
  const waitExit = async ms => {
    try { return await Promise.race([exitPromise.then(() => true), new Promise(resolve => { timer = setTimeout(() => resolve(false), ms); })]); }
    finally { clearTimeout(timer); }
  };
  if (!await waitExit(5000)) {
    child.kill('SIGTERM');
    if (!await waitExit(7000)) throw new Error('Owned PTY bridge did not reap Pi within its cleanup bound');
  }
};
const interrupted = signal => { failure ??= new Error(`Driver received ${signal}`); notify(); child?.kill('SIGTERM'); };
const onTerm = () => interrupted('SIGTERM'), onInt = () => interrupted('SIGINT'), onHup = () => interrupted('SIGHUP');
process.on('SIGTERM', onTerm); process.on('SIGINT', onInt); process.on('SIGHUP', onHup);
try {
  const { MeshStore } = await import(pathToFileURL(meshModule).href);
  mesh = new MeshStore(dirs.mesh, 65536, 500);
  watch(runDir); watch(dirs.sessions); watch(dirs.mesh);
  phase('launch-native-interactive-Pi', { runDir, candidate, cli, budgetMs });
  child = spawn(process.env.PI_FABRIC_PROBE_PYTHON ?? 'python3', ['-u', '-c', bridge, configFile], {
    cwd: dirs.workspace, env: { PATH: env.PATH, HOME: dirs.home, LANG: env.LANG }, stdio: ['pipe', 'pipe', 'pipe'],
  });
  exitPromise = new Promise(resolve => {
    child.once('error', error => { failure ??= error; childExit = { error: error.message }; notify(); resolve(); });
    child.once('close', (code, signal) => { childExit = { code, signal }; notify(); resolve(); });
  });
  child.stdin.on('error', error => { if (!shuttingDown) { failure ??= error; notify(); } });
  child.stdout.on('data', chunk => {
    fs.appendFileSync(files['tui.stdout.raw'], chunk);
    if (tee) process.stdout.write(chunk);
    notify();
  });
  child.stderr.on('data', chunk => { fs.appendFileSync(files['bridge.stderr.raw'], chunk); notify(); });
  hardTimer = setTimeout(() => { failure ??= new Error('Hard live-proof work deadline exceeded'); notify(); child.kill('SIGTERM'); }, budgetMs);
  await waitFor(() => starts().length > 0, 'native TUI session_start');
  const host = starts()[0]; sessionId = host.sessionId; sessionFile = host.sessionFile; rootId = `session:${sessionId}`;
  assert.ok(sessionId && sessionFile, 'Pi must persist its real native session');
  validateNativeHost(host);
  watch(path.dirname(sessionFile));
  let baseline = settles().length;
  send('LIFECYCLE_ACTIVATE');
  await waitFor(() => settles().length > baseline && inferences().some(record => record.stage === 'ready'), 'activation via real fabric_exec agents.main');
  assert.equal(entries()[0]?.id, sessionId, 'Native persisted session must match observed session_start');
  assert.equal(inferences().filter(record => record.stage === 'activate-tool').length, 1);
  phase('native-host-activated', { sessionId, sessionFile, rootId, host });
  await sendFollowUp('BEFORE');
  const firstInbox = await publishInbox('BEFORE');

  // These are literal supported editor commands, not ctx.compact()/ctx.reload()
  // stand-ins. A committed native compaction entry and a new extension epoch are
  // required independently of anything the deterministic provider says.
  const compactionsBefore = entries().filter(entry => entry.type === 'compaction').length;
  send('/compact');
  await waitFor(() => {
    const rejected = hosts().find(record => record.event === 'session_compact_failed');
    assert.ok(!rejected, `Native /compact failed or declined: ${JSON.stringify(rejected)}`);
    return entries().filter(entry => entry.type === 'compaction').length > compactionsBefore &&
      hosts().some(record => record.event === 'session_compact');
  }, 'literal /compact committed native compaction');
  const compaction = entries().filter(entry => entry.type === 'compaction').at(-1);
  assert.ok(compaction.summary && compaction.firstKeptEntryId);
  assertDeliveries();
  phase('literal-compact-committed', { compaction });
  assert.deepEqual(noise(), [], 'Forbidden Fabric diagnostic observed in the native TUI by the compaction boundary');
  await publishInbox('AFTER_COMPACT_REPLAY', firstInbox);
  await sendFollowUp('AFTER_COMPACT');
  await publishInbox('AFTER_COMPACT');

  const beforeReload = starts().length;
  send('/reload');
  await waitFor(() => starts().length > beforeReload, 'literal /reload native session_start in a fresh extension epoch', 45000);
  const reloaded = starts().at(-1);
  validateNativeHost(reloaded);
  assert.notEqual(reloaded.epoch, host.epoch, 'Must really rebuild the fixture extension runtime');
  assert.equal(reloaded.pid, host.pid, 'Literal reload, not killing/restarting Pi');
  assert.equal(reloaded.sessionFile, sessionFile, 'Reload must retain the native persisted session');
  assert.ok(hosts().some(record => record.event === 'session_shutdown'), 'Reload must shut down its previous extension runtime');
  assert.ok(entries().some(entry => entry.type === 'compaction' && entry.id === compaction.id), 'Reload must retain the real compaction');
  assertDeliveries();
  phase('literal-reload-committed', { previousEpoch: host.epoch, host: reloaded, retainedCompactionId: compaction.id });
  assert.deepEqual(noise(), [], 'Forbidden Fabric diagnostic observed in the native TUI by the reload boundary');
  await publishInbox('AFTER_RELOAD_REPLAY', firstInbox);
  await sendFollowUp('AFTER_RELOAD');
  await publishInbox('AFTER_RELOAD');

  // Normal native directory/heartbeat activity remains running: no timers or
  // callbacks have been replaced. Cover >2 ordinary 5s heartbeat intervals.
  const heartbeatStart = Date.now();
  await boundedQuiet(12000, () => { assertDeliveries(); assert.deepEqual(noise(), [], 'Forbidden Fabric internals leaked into the real TUI'); });
  assert.ok(settles().every(record => record.outcome === 'completed'), 'All native runs must settle successfully');
  assert.equal(hosts().filter(record => record.event === 'session_compact_failed').length, 0);
  assert.equal(inferences().filter(record => record.stage === 'unexpected-input').length, 0, 'Unexpected native input is not silently acknowledged');
  phase('native-idle-heartbeat-window-complete', { durationMs: Date.now() - heartbeatStart,
    note: 'Observes normal native lifecycle; does not seed a fake missing terminal-result agent.' });
} catch (error) {
  failure ??= error;
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
} finally {
  clearTimeout(hardTimer);
  try { await stop(); } catch (error) { failure ??= error; }
  for (const watcher of watchers) watcher.close();
  process.off('SIGTERM', onTerm); process.off('SIGINT', onInt); process.off('SIGHUP', onHup);
  try {
    if (!failure) {
      assert.equal(childExit?.code, 0, 'Real interactive Pi and its PTY bridge must exit normally');
      assert.equal(childExit?.signal, null);
      assertDeliveries();
      assert.deepEqual(noise(), [], 'Forbidden Fabric internals leaked during reload, redraw or shutdown');
      assert.deepEqual(distSnapshot(), candidateFiles, 'The built candidate changed during this proof; rerun on stable artifacts');
      assert.equal(hashFile(fixture), fixtureSha256, 'Fixture transport changed during this proof');
      assert.equal(hashFile(cli), cliSha256, 'Native Pi CLI changed during this proof');
      jsonl(sessionFile, true); jsonl(files['host.jsonl'], true); jsonl(files['inferences.jsonl'], true);
      for (const item of expectedFollowUps) assert.ok(fs.readFileSync(files['tui.stdout.raw'], 'utf8').includes(item.marker), 'Native TUI must render received followUp text');
    }
  } catch (error) { failure ??= error; }
  let meshEvents = [];
  try { if (mesh) { meshEvents = allEvents(); mesh.closeState(); } } catch (error) { failure ??= error; }
  fs.writeFileSync(path.join(runDir, 'mesh-events.json'), JSON.stringify(meshEvents, null, 2) + '\n');
  const artifacts = {};
  for (const [name, file] of Object.entries(files)) artifacts[name] = { path: file, bytes: fs.statSync(file).size, sha256: hashFile(file) };
  const summary = {
    passed: !failure, result: failure ? 'FAILED' : 'QUIESCENT', runDir, durationMs: Date.now() - startedAt, budgetMs,
    candidate, candidateSha256: candidateFiles[path.basename(candidate)], candidateFiles, cli, cliSha256, fixture, fixtureSha256,
    sessionId, sessionFile, nativeSessionSha256: sessionFile && fs.existsSync(sessionFile) ? hashFile(sessionFile) : undefined,
    rootId, nativeStarts: starts(), compactions: entries().filter(entry => entry.type === 'compaction'),
    followUps: expectedFollowUps, addressedInbox: expectedInbox, replays,
    nativeSettlements: settles(), observedInferences: inferences(),
    nativeTurns: hosts().filter(record => record.event === 'turn_start' || record.event === 'turn_end'),
    nativeReceives: hosts().filter(record => record.event === 'native_custom_message'),
    deliveryObservations: [...expectedFollowUps, ...expectedInbox].map(item => ({ marker: item.marker,
      followUpMessageId: item.nativeMessageId, inboxEventId: item.event?.id,
      nativeAssistantEntryIds: ackEntries(item.marker).map(entry => entry.id), providerObservations: acknowledgements(item.marker) })),
    forbiddenTuiDiagnostics: noise(), childExit, artifacts,
    coverage: { live: ['actual interactive Pi CLI in a PTY', 'built candidate extension', 'real fabric_exec agents.followUp with durable native receipts',
      'real MeshStore durable exact-session addressed inbox publication and wake', 'literal /compact', 'literal /reload in the same Pi process/session',
      'durable addressed-inbox deliveryId replays after both lifecycle boundaries', '12s native idle heartbeat window', 'raw TUI stdout and stderr'],
      notProven: ['production model transport (deliberately keyless)', 'in-flight delivery racing compaction/reload',
        'a missing terminal-result registry condition (no fake agents or nested agent spawn)', 'installed-release acceptance or final video (parent runs these)'] },
    failure: failure instanceof Error ? failure.stack : failure ? String(failure) : undefined,
  };
  fs.writeFileSync(path.join(runDir, 'summary.json'), JSON.stringify(summary, null, 2) + '\n');
  process.stderr.write(`[inbox-lifecycle] ${summary.result} ${path.join(runDir, 'summary.json')}\n`);
  if (!tee) console.log(JSON.stringify({ passed: !failure, result: summary.result, runDir, summary: path.join(runDir, 'summary.json') }));
}
if (failure) process.exitCode = 1;
