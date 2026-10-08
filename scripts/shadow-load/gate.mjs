#!/usr/bin/env node
// Release gate for the shadow soak (smarty-dev#6477 stage 1): judges a candidate's report.json,
// absolute bounds plus bounds relative to a reference report (the baseline release against itself,
// same harness, same host), since at fleet load the baseline itself fails the absolute-only gates.
//   node gate.mjs --report CANDIDATE/report.json --reference REFERENCE.json [--out DIR] [--json] [margin flags]
// orchestrate.mjs runs the same judgement at the end of a soak when given --reference.
// Exit 0 PASS, 1 FAIL, 2 bad input. Margins are set from measured run-to-run noise (README.md).
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Defaults: README.md, "Relative gates". Set from run-to-run noise of 04930dfd against itself
// (runs c3 and R0, fleet profile, 15 min, epyc1): each margin covers at least twice the difference
// seen between the two runs, and is never tighter than the fs-shcal proposal.
export const DEFAULT_MARGINS = {
  leaseFactor: 1.25, leaseSlackS: 60, lapsedFactor: 1.25, lapsedSlack: 5, restartMissingSlack: 3,
  busyPoints: 5, timeoutsFactor: 1.25, timeoutsSlack: 5,
  canaryRounds: 3, driftPoints: 10,
  maxWaitP99S: 10, waitBuckets: 1,
};
export const WAIT_BOUNDS = [5, 10, 25, 50, 100, 250, 500, 1000, 2000, 3000, 5000, 7500, 10000, 15000, null];

const median = values => values.length ? [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)] : null;
const driftPct = values => { const mid = median(values); return mid ? Math.round(1000 * Math.max(...values.map(value => Math.abs(value - mid))) / mid) / 10 : null; };
const r1 = value => value === null || value === undefined ? null : Math.round(value * 10) / 10;

// The numbers the gates judge, from a finished soak. observed is the observer's summary (the
// report's sibling state/observer.json); without it the window figures are null (pre-timeline reports).
export const gateMetrics = (report, observed) => {
  const loadStart = report.loadStartedAt, loadEnd = report.loadEndedAt;
  const loadMinutes = Math.max(1 / 60, (loadEnd - loadStart) / 60_000);
  const pinAt = report.markers?.find(marker => marker.event === 'pin-change')?.at ?? null;
  const restartAt = report.markers?.find(marker => marker.event === 'bridge-restart')?.at ?? null;
  const buckets = observed?.directory?.timeline?.buckets ?? null;
  const bucketMs = observed?.directory?.timeline?.bucketMs ?? 10_000;
  // A bucket belongs to a window when it starts inside it; the bucket holding the pin change counts
  // to the reload window (no lapse can be the pin's before 15 s anyway).
  const windowOf = (from, to) => {
    if (!buckets) return null;
    const rows = buckets.filter(row => row.t + bucketMs > from && row.t < to);
    return { buckets: rows.length, leaseMaxMs: Math.max(0, ...rows.map(row => row.leaseMaxMs)), overSamples: rows.reduce((sum, row) => sum + row.overSamples, 0),
      lapsed: new Set(rows.flatMap(row => row.over)).size, misses: rows.reduce((sum, row) => sum + row.misses + (row.listErrors ?? 0), 0),
      missing: new Set(rows.flatMap(row => row.missing)).size };
  };
  const prePinEnd = pinAt === null ? loadEnd : Math.floor(pinAt / bucketMs) * bucketMs;
  const counts = observed?.directory?.counts ?? null;
  const prePinCounts = counts ? counts.filter(row => row.at >= loadStart + 60_000 && row.at < (pinAt ?? loadEnd)).map(row => row.live) : null;
  const instrumented = report.lock?.instrumented ?? {};
  const instrumentTimeouts = report.lock?.instrument?.timeouts ?? null;
  const checks = (report.canary ?? []).map(round => round.checks ?? {});
  const passes = prefix => checks.filter(item => Object.entries(item).some(([name, check]) => name.startsWith(prefix) && check.pass)).length;
  const l8 = report.lock?.l8 && !report.lock.l8.error && report.lock.l8.n > 0 ? report.lock.l8 : null;
  return {
    format: 1, candidate: report.candidate, baseline: report.baseline, loadMinutes: r1(loadMinutes), pinAt, restartAt,
    lockStats: report.options?.lockStats ?? null,
    exits: report.unexpectedExits?.length ?? null, restarted: report.restarted ?? null,
    prePin: windowOf(loadStart, prePinEnd),
    reload: windowOf(prePinEnd, loadEnd) ?? (report.lease ? { buckets: null, leaseMaxMs: report.lease.maxLeaseAgeMs, overSamples: report.lease.overSamples,
      lapsed: report.lease.participantsOverLimit, misses: report.lease.directoryMisses, missing: report.lease.missingParticipants, fromSummary: true } : null),
    afterRestart: restartAt === null ? null : windowOf(restartAt, loadEnd + 1) ?? (report.lease ? { misses: report.lease.missesAfterBridgeRestart,
      missing: null, lapsed: null, fromSummary: true } : null),
    lock: { source: 'instrument (every process, both releases)', busyPct: instrumented.busyPct ?? null, peakMinuteBusyPct: instrumented.peakMinuteBusyPct ?? null,
      timeouts: instrumentTimeouts, timeoutsPerMin: instrumentTimeouts === null ? null : r1(instrumentTimeouts / loadMinutes),
      waitP50Ms: instrumented.waitP50Ms ?? null, waitP99Ms: instrumented.waitP99Ms ?? null, maxWaitMs: instrumented.maxWaitMs ?? null,
      acquisitions: instrumented.n ?? null, samplerBusyPct: report.lock?.sampler?.busyPct ?? null,
      l8: l8 ? { busyPct: r1(l8.busyPct), timeouts: l8.timeouts, waitP99Ms: l8.waitP99Ms, processes: l8.processes, n: l8.n } : null },
    canary: { rounds: checks.length, check3: passes('3-'), check4: passes('4-'), check5: passes('5-') },
    count: { maxDriftPct: report.participantCount?.maxDriftPct ?? null, median: report.participantCount?.median ?? null,
      prePinDriftPct: prePinCounts?.length ? driftPct(prePinCounts) : null, prePinSamples: prePinCounts?.length ?? null },
    pin: { pinned: report.pin?.pinned ?? false, due: report.pin?.due ?? null, done: report.pin?.done ?? null, errors: report.pin?.errors?.length ?? null },
    memory: { peakTotalMb: report.memory?.peakTotalMb ?? null, budgetMb: report.memory?.budgetMb ?? report.options?.memBudgetMb ?? null,
      exceeded: report.verdicts?.memory ? !report.verdicts.memory.pass : null },
    cpu: { busyPct: report.cpu?.busyPct ?? null, psiSome10: report.cpu?.psiSome10 ?? null },
  };
};

// A report's metrics: embedded by the harness, else derived (with its sibling state/observer.json if present).
export const metricsOf = file => {
  const report = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (report.metrics?.format === 1) return report.metrics;
  if (!report.loadStartedAt) throw new Error(file + ' is not a finished shadow-soak report');
  let observed = null;
  try { observed = JSON.parse(fs.readFileSync(path.join(path.dirname(file), 'state', 'observer.json'), 'utf8')); } catch { /* summary only */ }
  return gateMetrics(report, observed);
};

const bucketIndex = ms => { const index = WAIT_BOUNDS.findIndex(bound => bound === null || ms <= bound); return index < 0 ? WAIT_BOUNDS.length - 1 : index; };
const has = value => value !== null && value !== undefined && Number.isFinite(Number(value));

// Each gate: pass plus the absolute and relative parts with the numbers they compared.
export const judge = (c, ref, margins = DEFAULT_MARGINS) => {
  const m = { ...DEFAULT_MARGINS, ...margins };
  const part = (label, pass, detail) => ({ label, pass: Boolean(pass), detail });
  const no = (label, what) => part(label, false, 'no data: ' + what);
  const gates = {};
  const lim = (refValue, factor, slack) => has(refValue) ? Math.round(refValue * factor + slack) : null;

  // (a) leases and directory.
  const a = [];
  a.push(has(c.exits) ? part('no unexpected exits', c.exits === 0, 'exits ' + c.exits) : no('no unexpected exits', 'exits'));
  a.push(c.prePin ? part('before the pin: no lapse over 15 s, no directory miss', c.prePin.lapsed === 0 && c.prePin.misses === 0,
    'lapsed ' + c.prePin.lapsed + ', misses ' + c.prePin.misses + ', lease max ' + c.prePin.leaseMaxMs + ' ms') : no('before the pin: no lapse, no miss', 'timeline (old report)'));
  // After the bridge restart: the baseline itself misses a reloaded Main whose lease lapse runs past
  // the restart (R0: 1 participant), so this part is relative too.
  const refRestartMissing = ref.afterRestart ? ref.afterRestart.missing ?? (ref.afterRestart.misses === 0 ? 0 : null) : null;
  const restartLimit = has(refRestartMissing) ? refRestartMissing + m.restartMissingSlack : null;
  a.push(c.afterRestart && has(c.afterRestart.missing) ? part('after the bridge restart: participants missed <= ref + ' + m.restartMissingSlack, restartLimit !== null && c.afterRestart.missing <= restartLimit,
    c.afterRestart.missing + ' (' + c.afterRestart.misses + ' misses) vs limit ' + restartLimit + ' (ref ' + refRestartMissing + ')')
    : no('after the bridge restart: participants missed', 'timeline or restart marker'));
  if (c.reload && ref.reload) {
    const leaseLimit = lim(ref.reload.leaseMaxMs, m.leaseFactor, m.leaseSlackS * 1_000);
    const lapsedLimit = lim(ref.reload.lapsed, m.lapsedFactor, m.lapsedSlack);
    a.push(part('reload window: max lease age <= ref x ' + m.leaseFactor + ' + ' + m.leaseSlackS + ' s', has(c.reload.leaseMaxMs) && leaseLimit !== null && c.reload.leaseMaxMs <= leaseLimit,
      c.reload.leaseMaxMs + ' ms vs limit ' + leaseLimit + ' ms (ref ' + ref.reload.leaseMaxMs + ')'));
    a.push(part('reload window: lapsed participants <= ref x ' + m.lapsedFactor + ' + ' + m.lapsedSlack, has(c.reload.lapsed) && lapsedLimit !== null && c.reload.lapsed <= lapsedLimit,
      c.reload.lapsed + ' vs limit ' + lapsedLimit + ' (ref ' + ref.reload.lapsed + ')'));
  } else a.push(no('reload window relative', 'reload window figures'));
  gates.a = { name: 'leases and directory', parts: a };

  // (b) lock busy and timeouts, relative only.
  const busyLimit = has(ref.lock?.busyPct) ? r1(ref.lock.busyPct + m.busyPoints) : null;
  const toLimit = has(ref.lock?.timeoutsPerMin) ? r1(ref.lock.timeoutsPerMin * m.timeoutsFactor + m.timeoutsSlack) : null;
  gates.b = { name: 'mesh lock busy and timeouts', parts: [
    part('busy <= ref + ' + m.busyPoints + ' points', has(c.lock?.busyPct) && busyLimit !== null && c.lock.busyPct <= busyLimit, c.lock?.busyPct + '% vs limit ' + busyLimit + '% (ref ' + ref.lock?.busyPct + '%)'),
    part('timeouts/min <= ref x ' + m.timeoutsFactor + ' + ' + m.timeoutsSlack, has(c.lock?.timeoutsPerMin) && toLimit !== null && c.lock.timeoutsPerMin <= toLimit,
      c.lock?.timeoutsPerMin + '/min vs limit ' + toLimit + '/min (ref ' + ref.lock?.timeoutsPerMin + ')'),
  ] };

  // (c) canary: check 3 every round; checks 4 and 5 pass rates no worse than the reference minus N rounds.
  const rate = (pass, rounds) => rounds ? pass / rounds : null;
  const relCheck = key => {
    const want = ref.canary?.rounds ? Math.max(0, (ref.canary[key] - m.canaryRounds) / ref.canary.rounds) : null;
    const got = rate(c.canary?.[key], c.canary?.rounds);
    return part('check ' + key.slice(5) + ' pass rate >= ref - ' + m.canaryRounds + ' rounds', want !== null && got !== null && got >= want - 1e-9,
      c.canary?.[key] + '/' + c.canary?.rounds + ' vs >= ' + (want === null ? null : Math.round(want * 100)) + '% (ref ' + ref.canary?.[key] + '/' + ref.canary?.rounds + ')');
  };
  gates.c = { name: 'canary checks 3-5', parts: [
    part('check 3 (publish and read) in every round', c.canary?.rounds > 0 && c.canary.check3 === c.canary.rounds, c.canary?.check3 + '/' + c.canary?.rounds),
    relCheck('check4'), relCheck('check5'),
  ] };

  // (d) participant count.
  const driftLimit = has(ref.count?.maxDriftPct) ? r1(ref.count.maxDriftPct + m.driftPoints) : null;
  // Relative only: the baseline's count swings 45% before the pin already (spoke mirrors 124-248).
  const preLimit = has(ref.count?.prePinDriftPct) ? r1(ref.count.prePinDriftPct + m.driftPoints) : null;
  gates.d = { name: 'participant count drift', parts: [
    ...(preLimit === null ? [] : [part('before the pin: drift <= ref + ' + m.driftPoints + ' points', has(c.count?.prePinDriftPct) && c.count.prePinDriftPct <= preLimit,
      c.count?.prePinDriftPct + '% vs limit ' + preLimit + '% (ref ' + ref.count.prePinDriftPct + '%)')]),
    part('max drift <= ref + ' + m.driftPoints + ' points', has(c.count?.maxDriftPct) && driftLimit !== null && c.count.maxDriftPct <= driftLimit,
      c.count?.maxDriftPct + '% vs limit ' + driftLimit + '% (ref ' + ref.count?.maxDriftPct + '%)'),
  ] };

  // (e) wait p99: absolute at the lock budget, relative in histogram buckets.
  const ci = has(c.lock?.waitP99Ms) ? bucketIndex(c.lock.waitP99Ms) : null, ri = has(ref.lock?.waitP99Ms) ? bucketIndex(ref.lock.waitP99Ms) : null;
  gates.e = { name: 'lock wait p99', parts: [
    part('p99 <= ' + m.maxWaitP99S + ' s (lock budget)', has(c.lock?.waitP99Ms) && c.lock.waitP99Ms <= m.maxWaitP99S * 1_000, '<= ' + c.lock?.waitP99Ms + ' ms'),
    part('p99 at most ' + m.waitBuckets + ' bucket above ref', ci !== null && ri !== null && ci <= ri + m.waitBuckets,
      '<= ' + c.lock?.waitP99Ms + ' ms vs ref <= ' + ref.lock?.waitP99Ms + ' ms (buckets ' + WAIT_BOUNDS.map(bound => bound ?? 'inf').join('/') + ')'),
  ] };

  // (f) self-reloads and memory, absolute.
  gates.f = { name: 'pin change self-reloads', parts: [part('every autoReload Main reloaded, no error', c.pin?.pinned && c.pin.due > 0 && c.pin.done === c.pin.due && c.pin.errors === 0,
    c.pin?.done + '/' + c.pin?.due + ', errors ' + c.pin?.errors)] };
  gates.memory = { name: 'memory', parts: [part('total PSS <= budget', has(c.memory?.peakTotalMb) && c.memory.exceeded !== true && c.memory.peakTotalMb <= c.memory.budgetMb,
    c.memory?.peakTotalMb + ' MB vs ' + c.memory?.budgetMb + ' MB')] };

  for (const gate of Object.values(gates)) gate.pass = gate.parts.every(item => item.pass);
  return { pass: Object.values(gates).every(gate => gate.pass), margins: m, gates };
};

export const renderGate = (verdict, c, ref, labels = {}) => {
  const mark = ok => ok ? 'PASS' : 'FAIL';
  const lines = ['## Release gate: ' + mark(verdict.pass), '',
    'Candidate ' + (labels.candidate ?? c.candidate) + ' against reference ' + (labels.reference ?? ref.candidate) + ' (lock figures: instrument, every process).', '',
    '| gate | verdict | parts |', '|---|---|---|',
    ...Object.entries(verdict.gates).map(([key, gate]) => '| (' + key + ') ' + gate.name + ' | ' + mark(gate.pass) + ' | ' +
      gate.parts.map(item => mark(item.pass) + ' ' + item.label + ': ' + item.detail).join('; ') + ' |'),
    '', 'OVERALL: ' + mark(verdict.pass)];
  return lines.join('\n') + '\n';
};

const argMap = argv => {
  const out = {};
  for (let index = 0; index < argv.length; index++) {
    const flag = argv[index];
    if (!flag.startsWith('--')) throw new Error('Bad argument ' + flag);
    const next = argv[index + 1];
    if (next === undefined || next.startsWith('--')) out[flag.slice(2)] = true; else { out[flag.slice(2)] = next; index++; }
  }
  return out;
};
const MARGIN_FLAGS = { 'lease-factor': 'leaseFactor', 'lease-slack-s': 'leaseSlackS', 'lapsed-factor': 'lapsedFactor', 'lapsed-slack': 'lapsedSlack',
  'restart-missing-slack': 'restartMissingSlack', 'busy-points': 'busyPoints', 'timeouts-factor': 'timeoutsFactor', 'timeouts-slack': 'timeoutsSlack', 'canary-rounds': 'canaryRounds',
  'drift-points': 'driftPoints', 'max-wait-p99-s': 'maxWaitP99S', 'wait-buckets': 'waitBuckets' };
// The harness takes only the --gate-<margin> spelling (some plain names are its absolute-gate flags).
export const marginsFrom = (args, prefixedOnly = false) => {
  const margins = {};
  for (const [flag, key] of Object.entries(MARGIN_FLAGS)) {
    const raw = args['gate-' + flag] ?? (prefixedOnly ? undefined : args[flag]);
    if (raw === undefined) continue;
    const value = Number(raw);
    if (!Number.isFinite(value) || value < 0) throw new Error('Bad --' + flag);
    margins[key] = value;
  }
  return margins;
};

if (process.argv[1] && fs.realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  let args;
  try {
    args = argMap(process.argv.slice(2));
    if (!args.report || !args.reference) throw new Error('usage: gate.mjs --report CANDIDATE/report.json --reference REFERENCE.json [--out DIR] [--json] [--<margin> N]');
    const c = metricsOf(args.report), ref = metricsOf(args.reference);
    const verdict = judge(c, ref, marginsFrom(args));
    const text = renderGate(verdict, c, ref, { candidate: path.resolve(args.report), reference: path.resolve(args.reference) });
    if (args.out) {
      fs.mkdirSync(args.out, { recursive: true });
      fs.writeFileSync(path.join(args.out, 'gate.md'), text);
      fs.writeFileSync(path.join(args.out, 'gate.json'), JSON.stringify({ ...verdict, candidate: c, reference: ref }, null, 2));
    }
    process.stdout.write(args.json ? JSON.stringify({ ...verdict, candidate: c, reference: ref }, null, 2) + '\n' : text);
    process.exit(verdict.pass ? 0 : 1);
  } catch (error) {
    process.stderr.write(String(error?.message ?? error) + '\n');
    process.exit(2);
  }
}
