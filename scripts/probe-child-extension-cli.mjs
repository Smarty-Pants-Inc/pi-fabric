// Opt-in native proof for pi-fabric#864. Build first. Usage: node scripts/probe-child-extension-cli.mjs OUTPUT_DIR [LABEL]
// The outer Pi loads THIS checkout as a profile package (settings.json packages), exactly like an
// installed release, with no -e for Fabric. Its fabric_exec runs a process-transport child, which must
// load exactly one Fabric entry and complete a fabric_exec tool call.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const out = path.resolve(process.argv[2] ?? process.env.TASK_OUT ?? '.');
const label = process.argv[3] ?? 'probe';
fs.mkdirSync(out, { recursive: true });
const scratch = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'child-extension-'));
const profile = path.join(scratch, 'profile'), cwd = path.join(scratch, 'workspace'), log = path.join(scratch, 'proof.jsonl');
for (const dir of [profile, cwd, path.join(scratch, 'home'), path.join(scratch, 'tmp')]) fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(log, '');
const cli = fileURLToPath(new URL('./cli.js', import.meta.resolve('@earendil-works/pi-coding-agent')));
const fixture = path.join(repo, 'tests/fixtures/child-extension-cli-provider.ts');
const manifest = JSON.parse(fs.readFileSync(path.join(repo, 'package.json'), 'utf8'));
fs.writeFileSync(path.join(profile, 'settings.json'), JSON.stringify({ packages: [repo], extensions: [fixture], compaction: { enabled: false }, retry: { enabled: false }, defaultProjectTrust: 'always' }));
fs.writeFileSync(path.join(profile, 'fabric.json'), JSON.stringify({ fullCodeMode: true, executor: { kernel: 'typescript', timeoutMs: 120000, maxTimeoutMs: 120000, mainMaxTimeoutMs: 120000 },
  agents: { enabled: true, model: 'child-extension-proof/offline', recursive: true, transport: 'process', maxDepth: 2, maxConcurrent: 2, maxPerExecution: 4, maxTokensPerChild: 0, timeoutMs: 120000, budgetUsd: 0, retainRuns: true },
  mesh: { enabled: true, persist: true, root: path.join(scratch, 'mesh'), actorPollMs: 20 }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  approvals: { read: 'allow', write: 'allow', exec: 'allow', agent: 'allow' }, mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, records: { enabled: false } }));
const env = { PATH: process.env.PATH, HOME: path.join(scratch, 'home'), TMPDIR: path.join(scratch, 'tmp'), PI_CODING_AGENT_DIR: profile, PI_OFFLINE: '1',
  PI_FABRIC_PI_BINARY: cli, PI_FABRIC_RUN_ROOT: path.join(scratch, 'runs'), CHILD_EXTENSION_PROOF_LOG: log };
const args = [cli, '--mode', 'rpc', '--offline', '--no-skills', '--no-prompt-templates', '--no-context-files', '--no-themes', '--approve',
  '--provider', 'child-extension-proof', '--model', 'offline', '--thinking', 'off', '--tools', 'fabric_exec', '--session-dir', path.join(profile, 'sessions')];
const child = spawn(process.execPath, args, { cwd, env, stdio: ['pipe', 'pipe', 'pipe'] });
const events = []; let buffer = '', stderr = '';
child.stdout.setEncoding('utf8');
child.stdout.on('data', chunk => { buffer += chunk; let pos; while ((pos = buffer.indexOf('\n')) >= 0) {
  const line = buffer.slice(0, pos); buffer = buffer.slice(pos + 1); if (!line.trim()) continue;
  try { events.push(JSON.parse(line)); } catch { /* non-JSON diagnostics */ } } });
child.stderr.on('data', data => { stderr += data.toString(); });
const exit = new Promise(resolve => child.once('exit', code => resolve(code)));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const redact = text => String(text).split(scratch).join('<scratch>').split(repo).join('<release>');
let summary;
try {
  const code = 'const r = await agents.run({ task: "CHILD_EXTENSION_TASK", transport: "process", model: "child-extension-proof/offline" });\n' +
    'return { status: r.status, toolCalls: r.toolCalls, text: r.text, error: r.error };';
  child.stdin.write(JSON.stringify({ id: 'p1', type: 'prompt', message: 'CHILD_EXTENSION_MAIN_CODE:\n' + code }) + '\n');
  const deadline = Date.now() + 150000;
  while (!events.some(e => e.type === 'agent_end') && Date.now() < deadline && child.exitCode === null) await sleep(50);
  const end = events.find(e => e.type === 'tool_execution_end' && e.toolName === 'fabric_exec');
  let result; try { result = JSON.parse(end.result.content.filter(p => p.type === 'text').map(p => p.text).join('\n')); } catch { result = end ? end.result : null; }
  const rows = fs.readFileSync(log, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  const childStarts = rows.filter(r => r.type === 'native-session-start' && r.runId !== 'main');
  const childToolCalls = rows.filter(r => r.type === 'native-tool-call' && r.runId !== 'main');
  const extensionArgs = childStarts.flatMap(r => r.argv.flatMap((a, i, all) => all[i - 1] === '-e' && /pi-fabric|<release>/.test(redact(a)) && !/\/(worker|guards)\//.test(a) && !/fixtures/.test(a) ? [redact(a)] : []));
  summary = { label, head: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repo, encoding: 'utf8' }).trim(), dirty: execFileSync('git', ['status', '--porcelain', '--untracked-files=no'], { cwd: repo, encoding: 'utf8' }).trim().length > 0,
    packagePiExtensions: manifest.pi.extensions, mainAgentEnd: events.some(e => e.type === 'agent_end'), outerToolError: end ? end.isError : null,
    childResult: result && typeof result === 'object' ? { ...result, error: result.error ? redact(result.error).slice(0, 600) : result.error } : redact(JSON.stringify(result)).slice(0, 600),
    childSessionStarts: childStarts.length, childFabricExtensionArgs: extensionArgs, childNativeToolCalls: childToolCalls.map(r => r.tool) };
  summary.passed = Boolean(result && result.status === 'completed' && result.toolCalls >= 1 && childToolCalls.some(r => r.tool === 'fabric_exec'));
} finally {
  if (child.exitCode === null) child.stdin.end();
  const timer = setTimeout(() => child.kill('SIGKILL'), 15000); await exit; clearTimeout(timer);
  const runs = path.join(scratch, 'runs');
  const stderrTails = [];
  const walk = dir => { if (!fs.existsSync(dir)) return; for (const name of fs.readdirSync(dir)) { const file = path.join(dir, name); const stat = fs.statSync(file);
    if (stat.isDirectory()) walk(file); else if (/stderr|\.log$/.test(name)) { const text = fs.readFileSync(file, 'utf8'); if (/Failed to load extension|conflicts with/.test(text)) stderrTails.push(redact(text.match(/.*(Failed to load extension|conflicts with).*/)[0]).slice(0, 400)); } } };
  walk(runs);
  summary = { ...(summary ?? { label, passed: false }), childLoadErrors: [...new Set(stderrTails)], outerStderrTail: redact(stderr).slice(-400) };
  fs.writeFileSync(path.join(out, `child-extension-${label}.json`), JSON.stringify(summary, null, 2) + '\n');
  console.log(JSON.stringify(summary, null, 2));
  fs.rmSync(scratch, { recursive: true, force: true });
}
process.exitCode = summary.passed ? 0 : 1;
