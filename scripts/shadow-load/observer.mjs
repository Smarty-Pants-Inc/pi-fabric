// Independent observer: samples the hub mesh lock (<mesh>/.lock present = held), every expected
// participant's lease (its owner host's lease file, plus the record file for resident actors) and
// the participant directory as the candidate's ParticipantDirectory lists it.
//   node observer.mjs --release DIR --hub ROOT --expected FILE --out FILE [--lock-sample-ms 2]
import fs from 'node:fs';
import path from 'node:path';
import { argMap, hostLeaseFile, loadCandidate, openReader, participantFile, readJson, writeJson } from './candidate.mjs';

const args = argMap(process.argv.slice(2));
const candidate = await loadCandidate(args.release, { directory: true });
const hub = args.hub;
const reader = openReader(candidate, hub, `shadow-observer:${process.pid}`);
const lockPath = path.join(hub, '.lock');
const startedAt = Date.now();

let expected = { participants: [] };
let expectedStamp = '';
let expectedIndex = new Map();
const reloadExpected = () => {
  try {
    const stat = fs.statSync(args.expected);
    const stamp = `${stat.mtimeMs}:${stat.size}`;
    if (stamp === expectedStamp) return;
    expected = readJson(args.expected) ?? expected;
    expectedIndex = new Map(expected.participants.map((participant, index) => [participant.id, index]));
    expectedStamp = stamp;
  } catch { /* not written yet */ }
};

// Lock: a fixed-rate sample, independent of the lock state, gives the held fraction per minute.
const lockMinutes = new Map();
setInterval(() => {
  const minute = Math.floor(Date.now() / 60_000);
  const bucket = lockMinutes.get(minute) ?? { samples: 0, held: 0 };
  bucket.samples++;
  if (fs.existsSync(lockPath)) bucket.held++;
  lockMinutes.set(minute, bucket);
}, Number(args['lock-sample-ms'] ?? 2));

// Leases: per participant, the age of its owner host's lease and (actors) of its record envelope.
const lease = new Map();
const leaseOf = id => {
  let row = lease.get(id);
  if (!row) lease.set(id, row = { maxAgeMs: 0, maxAgeAt: 0, maxEnvelopeAgeMs: 0, over: 0, samples: 0, lastHostUpdatedAt: 0, maxGapMs: 0, missingLease: 0 });
  return row;
};
const leaseLimitMs = Number(args['max-lease-s'] ?? 15) * 1_000;
const overEvents = [];
// Timeline in 10 s buckets, uncapped: max lease age, lapsed and missing participants (as indexes into
// the expected list) and directory misses, so the gates can judge any window (before the pin change,
// the reload window, after the bridge restart) exactly.
const BUCKET_MS = 10_000;
const timeline = new Map();
const bucketOf = at => {
  const t = Math.floor(at / BUCKET_MS) * BUCKET_MS;
  let row = timeline.get(t);
  if (!row) timeline.set(t, row = { t, leaseMaxMs: 0, overSamples: 0, over: new Set(), misses: 0, missing: new Set(), listErrors: 0 });
  return row;
};
setInterval(() => {
  reloadExpected();
  const now = Date.now();
  const hostCache = new Map();
  for (const participant of expected.participants) {
    const row = leaseOf(participant.id);
    row.samples++;
    if (!hostCache.has(participant.hostId)) hostCache.set(participant.hostId, readJson(hostLeaseFile(hub, participant.hostId)));
    const hostLease = hostCache.get(participant.hostId);
    const updatedAt = Number(hostLease?.updatedAt ?? 0);
    if (!updatedAt) row.missingLease++;
    if (updatedAt > row.lastHostUpdatedAt) {
      if (row.lastHostUpdatedAt) row.maxGapMs = Math.max(row.maxGapMs, updatedAt - row.lastHostUpdatedAt);
      row.lastHostUpdatedAt = updatedAt;
    }
    let age = now - (row.lastHostUpdatedAt || expected.since || startedAt);
    if (participant.kind === 'actor') {
      const envelope = readJson(participantFile(hub, participant.id));
      const envelopeAge = envelope?.updatedAt ? now - envelope.updatedAt : now - (expected.since || startedAt);
      row.maxEnvelopeAgeMs = Math.max(row.maxEnvelopeAgeMs, envelopeAge);
      age = Math.max(age, envelopeAge);
    }
    if (age > row.maxAgeMs) { row.maxAgeMs = age; row.maxAgeAt = now; }
    const bucket = bucketOf(now);
    bucket.leaseMaxMs = Math.max(bucket.leaseMaxMs, age);
    if (age > leaseLimitMs) {
      row.over++;
      bucket.overSamples++;
      bucket.over.add(expectedIndex.get(participant.id) ?? -1);
      if (overEvents.length < 200) overEvents.push({ at: now, id: participant.id, name: participant.name, kind: participant.kind, ageMs: age });
    }
  }
}, Number(args['lease-sample-ms'] ?? 1_000));

// Directory: every expected participant must be listed and fresh in the candidate's own view.
// Participant count: every listed, non-stale participant (local and mirrored remote) per sample.
const counts = [];
const directory = { samples: 0, misses: 0, missEvents: [], missingIds: new Map(), remote: { min: Infinity, max: 0 }, local: { min: Infinity, max: 0 }, listMs: { max: 0, sum: 0 } };
setInterval(() => {
  reloadExpected();
  if (!expected.participants.length) return;
  const started = performance.now();
  let listed;
  try { listed = reader.list({ scope: 'project', fresh: true, includeStale: true }); }
  catch (error) {
    directory.misses++;
    bucketOf(Date.now()).listErrors++;
    if (directory.missEvents.length < 200) directory.missEvents.push({ at: Date.now(), error: String(error?.message ?? error).slice(0, 200) });
    return;
  }
  const elapsed = performance.now() - started;
  directory.listMs.max = Math.max(directory.listMs.max, elapsed);
  directory.listMs.sum += elapsed;
  directory.samples++;
  const byId = new Map(listed.map(info => [info.id, info]));
  const remote = listed.filter(info => info.remoteHost !== undefined).length;
  if (counts.length < 5_000) counts.push({ at: Date.now(), live: listed.filter(info => !info.stale).length, listed: listed.length, remote });
  directory.remote.min = Math.min(directory.remote.min, remote);
  directory.remote.max = Math.max(directory.remote.max, remote);
  directory.local.min = Math.min(directory.local.min, listed.length - remote);
  directory.local.max = Math.max(directory.local.max, listed.length - remote);
  for (const participant of expected.participants) {
    const info = byId.get(participant.id);
    if (info && !info.stale) continue;
    directory.misses++;
    const bucket = bucketOf(Date.now());
    bucket.misses++;
    bucket.missing.add(expectedIndex.get(participant.id) ?? -1);
    directory.missingIds.set(participant.id, (directory.missingIds.get(participant.id) ?? 0) + 1);
    if (directory.missEvents.length < 200) directory.missEvents.push({ at: Date.now(), id: participant.id, name: participant.name, kind: participant.kind, reason: info ? 'stale' : 'absent' });
  }
}, Number(args['list-ms'] ?? 3_000));

const summary = () => {
  const minutes = [...lockMinutes.entries()].sort((a, b) => a[0] - b[0]).map(([minute, bucket]) => ({
    minute: new Date(minute * 60_000).toISOString().slice(11, 16), samples: bucket.samples, busyPct: Math.round(1000 * bucket.held / bucket.samples) / 10 }));
  const totals = [...lockMinutes.values()].reduce((sum, bucket) => ({ samples: sum.samples + bucket.samples, held: sum.held + bucket.held }), { samples: 0, held: 0 });
  const rows = [...lease.entries()].map(([id, row]) => ({ id, ...row }));
  const names = new Map(expected.participants.map(participant => [participant.id, participant]));
  rows.sort((a, b) => b.maxAgeMs - a.maxAgeMs);
  return {
    pid: process.pid, startedAt, savedAt: Date.now(), expected: expected.participants.length,
    lock: { samples: totals.samples, busyPct: totals.samples ? Math.round(1000 * totals.held / totals.samples) / 10 : null,
      peakMinuteBusyPct: Math.max(0, ...minutes.map(row => row.busyPct)), minutes },
    lease: { limitMs: leaseLimitMs, maxAgeMs: rows[0]?.maxAgeMs ?? null, maxHostGapMs: Math.max(0, ...rows.map(row => row.maxGapMs)),
      participantsOver: rows.filter(row => row.over > 0).length, overSamples: rows.reduce((sum, row) => sum + row.over, 0),
      worst: rows.slice(0, 8).map(row => ({ ...row, name: names.get(row.id)?.name, kind: names.get(row.id)?.kind })), overEvents },
    directory: { samples: directory.samples, misses: directory.misses, missingParticipants: directory.missingIds.size,
      missing: [...directory.missingIds.entries()].slice(0, 20).map(([id, count]) => ({ id, name: names.get(id)?.name, count })),
      missEvents: directory.missEvents, counts, remote: directory.remote,
      timeline: { bucketMs: BUCKET_MS, buckets: [...timeline.values()].sort((a, b) => a.t - b.t).map(row => ({ t: row.t, leaseMaxMs: row.leaseMaxMs,
        overSamples: row.overSamples, over: [...row.over], misses: row.misses, missing: [...row.missing], listErrors: row.listErrors })) }, local: directory.local,
      listMeanMs: directory.samples ? Math.round(directory.listMs.sum / directory.samples) : null, listMaxMs: Math.round(directory.listMs.max) },
  };
};
const save = () => writeJson(args.out, summary());
setInterval(save, 15_000);
const stop = () => { save(); process.exit(0); };
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
