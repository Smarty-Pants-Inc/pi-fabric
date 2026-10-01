// Native launcher wire fixture: owns the real flock, but substitutes Pi/worker
// startup. Real Pi/worker provenance is proved separately by the two-release TUI harness.
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { execFileSync, spawn } from 'node:child_process';
const read = file => { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } };
const birth = pid => fs.readFileSync(`/proc/${pid}/stat`, 'utf8').split(')').at(-1).trim().split(/\s+/)[19];
const config = read(process.env.PI_FABRIC_RESIDENT_CONFIG);
const root = config.residencyRoot;
const launcher = JSON.parse(process.env.PI_FABRIC_RESIDENT_LAUNCHER);
const attempt = process.env.PI_FABRIC_RESIDENT_ATTEMPT ? JSON.parse(process.env.PI_FABRIC_RESIDENT_ATTEMPT) : undefined;
const release = path.resolve(path.dirname(config.fabricExtensionPath), '..');
if (attempt?.kind === 'target' && process.env.PI_FABRIC_TEST_TARGET_MODE === 'exit') process.exit(7);
const fd = fs.openSync(path.join(root, 'host.lock'), fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW, 0o600);
try { execFileSync('flock', ['-x', '-n', '3'], { stdio: ['ignore', 'ignore', 'inherit', fd] }); }
catch { fs.closeSync(fd); process.exit(0); }
if (attempt?.kind === 'target' && process.env.PI_FABRIC_TEST_TARGET_MODE === 'hang') {
  // Holding an unpublished fence, with a detached descendant which also ignores
  // TERM: a signal is not an exit receipt, nor is killing only the parent enough.
  spawn(process.execPath, ['-e', "process.on('SIGTERM',()=>{});setInterval(()=>{},1000)"], { detached: true, stdio: 'ignore', env: process.env });
  process.on('SIGTERM', () => {});
  process.stdin.resume();
  setInterval(() => {}, 1000);
} else {
  const token = randomUUID();
  const owner = { format: 1, hostId: 'resident:fixture', pid: process.pid, processStartTime: birth(process.pid), token,
    startedAt: Date.now(), readyAt: Date.now(), requestFence: 1, commands: ['releaseChange'],
    releaseRoot: release, configDigest: process.env.PI_FABRIC_RESIDENT_SPEC_DIGEST,
    handover: { abi: 'fabric-resident-1', launcher }, ...(attempt ? { attempt } : {}) };
  if (attempt?.kind === "target" && process.env.PI_FABRIC_TEST_TARGET_MODE === "terminal-block") {
    fs.rmSync(path.join(root, "handover.json")); fs.mkdirSync(path.join(root, "handover.json"));
  }
  fs.writeFileSync(path.join(root, 'owner.json'), JSON.stringify(owner));
  const close = () => { if (read(path.join(root, 'owner.json'))?.token === token) fs.rmSync(path.join(root, 'owner.json'), { force: true }); fs.closeSync(fd); process.exit(0); };
  process.on('SIGTERM', close); process.on('SIGINT', close); process.stdin.on('end', close); process.stdin.resume();
  const timer = setInterval(() => {
    const state = read(path.join(root, 'handover.json'));
    if (!state) return;
    if (attempt && state.plan.id === attempt.id && state.phase === (attempt.kind === 'target' ? 'complete' : 'fallback')) {
      fs.writeFileSync(path.join(root, 'fixture-service.json'), JSON.stringify({ pid: process.pid, config, attempt, at: Date.now() }));
      clearInterval(timer);
    }
    if (!attempt && state.phase === 'custody' && state.plan.old.pid === process.pid && state.plan.old.token === token) {
      const receipt = read(path.join(root, `handover-${state.plan.id}.custody.json`));
      if (receipt?.id === state.plan.id && receipt.launcher.token === launcher.token) {
        fs.writeFileSync(path.join(root, 'fixture-custody-seen.json'), JSON.stringify({ at: Date.now(), owner, receipt }));
        fs.writeFileSync(path.join(root, 'handover.json'), JSON.stringify({ ...state, phase: 'released', at: Date.now() }));
        close();
      }
    }
  }, 10);
}
