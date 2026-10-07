// Fleet-sized hub state, synthetic but shaped like the fleet mesh on Ryzen 1 (smarty-dev#6477).
// Only summary numbers come from the fleet (no real record is copied):
//   state.json ~4.8 MB, ~600 host-lease files, ~500 participant records (~250 of them mirrored
//   remote roots, which the 4 spokes' real Mains and bridges provide), ~300 actor records across
//   ~40 roots (the live resident hosts provide part, the rest is seeded here).
//   node seed.mjs --phase state --release DIR --hub ROOT --plan FILE --state-mb 4.8 --out FILE
//     pads state.json through the release's own MeshStore.writeBatch, before any Main starts;
//   node seed.mjs --phase keep --release DIR --hub ROOT --plan FILE --host-leases 600
//       --participants 20 --actor-records 150 --actor-roots 25 --out FILE
//     clones a live Main's host lease and participant file into seeded hosts and roots (new ids,
//     shifted times), writes the seeded actor registries, then renews every seeded lease and record
//     each 5 s, as their owner hosts would (lock-free atomic renames, like the real heartbeat).
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { argMap, delay, hostLeaseFile, loadCandidate, openStore, participantFile, readJson, residentRoot, writeJson } from './candidate.mjs';

const args = argMap(process.argv.slice(2));
const plan = readJson(args.plan);
const hub = args.hub;
const phase = args.phase;
const text = (bytes, seed) => {
  const words = ['review', 'fabric', 'mesh', 'lease', 'actor', 'bridge', 'session', 'merge', 'queue', 'release', 'canary', 'participant', 'heartbeat', 'spoke', 'ryzen'];
  let out = '';
  for (let i = 0; out.length < bytes; i++) out += `${words[(seed + i * 7) % words.length]} `;
  return out.slice(0, bytes);
};

if (phase === 'state') {
  const candidate = await loadCandidate(args.release);
  const store = openStore(candidate, hub);
  const identity = { id: 'shadow-seed', name: 'shadow-seed', kind: 'agent' };
  const target = Math.round(Number(args['state-mb'] ?? 4.8) * 1_000_000);
  const statePath = path.join(hub, 'state.json');
  const size = () => { try { return fs.statSync(statePath).size; } catch { return 0; } };
  // Record shapes the fleet state holds per participant and actor: a host/root envelope with
  // capabilities, tools and a status summary (~9 KB serialized each, as 4.8 MB / ~500 records).
  let n = 0;
  const now = Date.now();
  while (size() < target) {
    const ops = [];
    for (let i = 0; i < 50; i++, n++) {
      const sessionId = randomUUID();
      const kind = n % 3 === 0 ? 'actor' : 'root';
      ops.push({ kind: 'put', key: `shadow/seed/${kind}/${sessionId}`, value: {
        id: kind === 'actor' ? sessionId.replaceAll('-', '') : `session:${sessionId}`, kind, name: `seed-${kind}-${n}`, sessionId,
        hostId: `host:${randomUUID()}`, rootId: `session:${randomUUID()}`, project: `project-${n % 23}`, cwd: `/home/fleet/lanes/l${n % 40}`,
        status: ['idle', 'running', 'waiting'][n % 3], startedAt: now - n * 1_000, updatedAt: now, expiresAt: now + 45_000, pendingMessages: n % 4,
        tools: ['read', 'grep', 'find', 'ls', 'edit', 'write', 'bash', 'fabric_exec'].slice(0, 3 + (n % 6)),
        capabilities: Array.from({ length: 12 }, (_, i) => ({ name: `cap-${i}`, version: 1 + (i % 3), enabled: i % 4 !== 0 })),
        topics: Array.from({ length: 6 }, (_, i) => `fleet.work.${['github', 'review', 'ci', 'release', 'ops', 'mesh'][i]}.${n % 17}`),
        summary: text(6_500, n), lastEvents: Array.from({ length: 8 }, (_, i) => ({ seq: n * 10 + i, topic: 'fleet.work.github.pull_request', at: now - i * 60_000 })),
      } });
    }
    await store.writeBatch({ identity, ops });
  }
  writeJson(args.out, { phase, entries: n, stateBytes: size(), targetBytes: target, at: Date.now() });
  process.exit(0);
}

if (phase !== 'keep') throw new Error(`unknown --phase ${phase}`);
const candidate = await loadCandidate(args.release, { registry: true });
const template = plan.mains[0];
const leaseDir = path.join(hub, 'host-leases');
// Wait for the template Main's own lease and record, written by the release itself.
let leaseText, recordText;
for (let i = 0; i < 600 && !(leaseText && recordText); i++) {
  try { leaseText = fs.readFileSync(hostLeaseFile(hub, template.id), 'utf8'); recordText = fs.readFileSync(participantFile(hub, template.id), 'utf8'); }
  catch { await delay(500); }
}
if (!leaseText || !recordText) throw new Error('no template lease/record from the first Main');
const templateAt = Date.now();
// Times shift with the clock: a clone renewed now looks as fresh as the template was when read.
const shift = (value, delta) => {
  if (Array.isArray(value)) return value.map(item => shift(item, delta));
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) =>
    [key, typeof item === 'number' && item > 1e12 && item < 1e13 ? item + delta : shift(item, delta)]));
  return value;
};
const clone = (source, sessionId, name) => JSON.parse(source.split(template.sessionId).join(sessionId).split(template.name).join(name));
const existing = (() => { try { return fs.readdirSync(leaseDir).filter(name => name.endsWith('.json')).length; } catch { return 0; } })();
const leaseCount = Math.max(0, Number(args['host-leases'] ?? 600) - existing);
const participantCount = Math.min(leaseCount, Number(args.participants ?? 20));
const seeded = Array.from({ length: leaseCount }, (_, i) => {
  const sessionId = randomUUID();
  return { sessionId, id: `session:${sessionId}`, name: `seed-main-${String(i).padStart(3, '0')}`, record: i < participantCount };
});
const leaseTemplate = JSON.parse(leaseText), recordTemplate = JSON.parse(recordText);
void leaseTemplate; void recordTemplate;
const atomic = (file, value) => {
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, JSON.stringify(value));
  fs.renameSync(temporary, file);
};
const renew = host => {
  const delta = Date.now() - templateAt;
  atomic(hostLeaseFile(hub, host.id), shift(clone(leaseText, host.sessionId, host.name), delta));
  if (host.record) atomic(participantFile(hub, host.id), shift(clone(recordText, host.sessionId, host.name), delta));
};
fs.mkdirSync(leaseDir, { recursive: true });
for (const host of seeded) renew(host);
// Seeded actor registries: the remaining roots' durable actors, written by the release's own store.
const actorRoots = Number(args['actor-roots'] ?? 25), actorRecords = Number(args['actor-records'] ?? 150);
let actorsWritten = 0;
for (let r = 0; r < actorRoots; r++) {
  const rootId = seeded[r]?.id ?? `session:${randomUUID()}`;
  const count = Math.floor(actorRecords / actorRoots) + (r < actorRecords % actorRoots ? 1 : 0);
  const at = Date.now() - 3_600_000;
  const registry = new candidate.ActorRegistryStore(path.join(residentRoot(hub, rootId), 'actors'));
  registry.write(Array.from({ length: count }, (_, a) => ({ id: randomUUID().replaceAll('-', ''), name: `seed-r${r}-a${a}`, rootId, project: 'shadow-load',
    instructions: 'Review pull requests when woken.', status: 'idle', events: [], topics: ['fleet.work.review.requested'], residency: 'durable',
    delivery: 'mailbox', runner: 'pi', responseMode: 'text', requirements: [], createdAt: at, updatedAt: at, messages: [] })));
  actorsWritten += count;
}
const summary = { phase, existingLeases: existing, seededLeases: seeded.length, seededParticipants: participantCount, actorRoots, actorRecords: actorsWritten,
  ready: true, renewals: 0, renewErrors: 0, startedAt: Date.now() };
writeJson(args.out, summary);
// Renew a fifth of the seeded hosts every second: each one every 5 s, like the real heartbeat.
let cursor = 0;
setInterval(() => {
  const slice = Math.ceil(seeded.length / 5);
  for (let i = 0; i < slice; i++) {
    const host = seeded[cursor++ % seeded.length];
    try { renew(host); summary.renewals++; } catch { summary.renewErrors++; }
  }
}, 1_000);
setInterval(() => writeJson(args.out, { ...summary, savedAt: Date.now() }), 10_000);
const stop = () => { writeJson(args.out, { ...summary, savedAt: Date.now() }); process.exit(0); };
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
