// Pure parts of the fixed-load mesh lock benchmark (scripts/benchmark-mesh-lock.mjs):
// the seeded load schedule, the L8 histogram percentile, and the CI ratchet comparison.
// smarty-dev#6477 L9a, smarty-dev#6676.

/** Today's defaults: a model of an idle fleet of Mains, not a recorded trace. Rates are fleet-wide per second. */
export const DEFAULT_LOAD = Object.freeze({
  participants: 80,
  processes: 8,
  seed: 6477,
  durationS: 120,
  warmupS: 15,
  stateMb: 4.8,
  heartbeatS: 5,
  hostRenewS: 600,
  putRate: 1,
  deleteRate: 0.25,
  publishRate: 2,
  steerRate: 0.2,
  dirReadRate: 4,
  registryKeys: 200,
  pollMs: 100,
  lockTimeoutMs: 10_000,
  lockProtocol: 1,
});

/** Mirrors LOCK_STATS_BOUNDS_MS in src/mesh/commit-stats.ts; the harness cross-checks it against L8's files. */
export const L8_BOUNDS_MS = Object.freeze([1, 2, 5, 10, 20, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000]);

// ponytail: two metric classes, on purpose. Counts per op (lock requests, holds, state
// rewrites, bytes rewritten, timeouts at a generous 10 s budget) are fixed by the seeded
// schedule and the store's code path, not by how fast the host is: a loaded shared CI host
// gives the same numbers as an idle one, so they make a ratchet that cannot flake. Wait,
// hold, receipt latency and busy % measure the host as much as the code (CPU steal, disk,
// other jobs); on a shared runner they swing by tens of percent run to run, so they are
// always reported but gated only with --gate-timing, on a dedicated runner whose own
// baseline they are compared with.
export const LOAD_INSENSITIVE_METRICS = Object.freeze([
  "acquisitionsPerOp", "holdsPerOp", "stateRewritesPerOp", "bytesRewrittenPerOp", "timeoutsPer10kOps",
]);
export const TIMING_METRICS = Object.freeze(["lockWaitP95Ms", "lockHoldP95Ms", "receiptP95Ms", "lockBusyPct"]);

/** mulberry32: small, fast and identical on every platform. */
export const seededRandom = (seed) => {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
};

const KIND_ORDER = ["heartbeat", "hostRenew", "put", "delete", "publish", "steer", "dirRead"];

/**
 * The whole fleet's planned operations over warm-up plus the measured window, in ms from the
 * start. Every worker derives the same list from the seed and runs its own participants' share.
 * Heartbeats and host renewals are periodic with a seeded phase; the op mix arrives as Poisson
 * streams. The count per window is therefore fixed: a slow host runs ops late, never fewer.
 */
export const planSchedule = (load) => {
  const random = seededRandom(load.seed);
  const totalMs = (load.warmupS + load.durationS) * 1000;
  const ops = [];
  for (let p = 0; p < load.participants; p++) {
    const every = load.heartbeatS * 1000;
    for (let t = random() * every; t < totalMs; t += every) ops.push({ t, kind: "heartbeat", p });
    const renew = load.hostRenewS * 1000;
    if (renew > 0) for (let t = random() * renew; t < totalMs; t += renew) ops.push({ t, kind: "hostRenew", p });
  }
  const streams = [["put", load.putRate], ["delete", load.deleteRate], ["publish", load.publishRate],
    ["steer", load.steerRate], ["dirRead", load.dirReadRate]];
  for (const [kind, rate] of streams) {
    if (!(rate > 0)) continue;
    const gap = () => -Math.log(1 - random()) * 1000 / rate;
    for (let t = gap(); t < totalMs; t += gap()) {
      const op = { t, kind, p: Math.floor(random() * load.participants) };
      if (kind === "put" || kind === "delete") op.key = Math.floor(random() * load.registryKeys);
      if (kind === "steer") {
        let to = Math.floor(random() * (load.participants - 1));
        if (to >= op.p) to++;
        op.to = to;
      }
      ops.push(op);
    }
  }
  ops.sort((a, b) => a.t - b.t || KIND_ORDER.indexOf(a.kind) - KIND_ORDER.indexOf(b.kind) || a.p - b.p);
  ops.forEach((op, id) => {
    op.id = id;
    op.phase = op.t < load.warmupS * 1000 ? "warmup" : "measure";
  });
  return ops;
};

/** Participant p runs in worker process floor(p / perProcess). */
export const processOf = (load, p) => Math.floor(p / Math.ceil(load.participants / load.processes));

export const histogramIndex = (ms) => {
  let index = 0;
  while (index < L8_BOUNDS_MS.length && ms > L8_BOUNDS_MS[index]) index++;
  return index;
};

/** As fabric-mesh-lock-stats: the upper bound of the bucket holding the percentile (Infinity above 10 s). */
export const histogramPercentile = (histogram, fraction) => {
  const total = histogram.reduce((sum, count) => sum + count, 0);
  if (!total) return 0;
  let seen = 0;
  for (let index = 0; index < histogram.length; index++) {
    seen += histogram[index];
    if (seen >= Math.ceil(total * fraction)) return L8_BOUNDS_MS[index] ?? Number.POSITIVE_INFINITY;
  }
  return Number.POSITIVE_INFINITY;
};

/** Nearest-rank percentile of raw samples. */
export const samplePercentile = (samples, fraction) => {
  if (!samples.length) return 0;
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * fraction) - 1))];
};

/** The load fields that must match for two results to be comparable. */
export const loadKey = (load) => JSON.stringify(Object.keys(DEFAULT_LOAD).sort().map(key => [key, load[key]]));

/**
 * Compare a result with a committed baseline. Every metric is "lower is better"; a metric
 * regresses when it exceeds baseline * (1 + maxRegressPct / 100). A zero baseline therefore
 * allows zero (one timeout at a generous budget is a regression). Timing metrics are
 * reported always and fail the check only with gateTiming.
 */
export const compareToBaseline = (result, baseline, { maxRegressPct = 10, gateTiming = false } = {}) => {
  const problems = [];
  if (loadKey(result.load) !== loadKey(baseline.load)) {
    problems.push("the load differs from the baseline's (flags, seed or defaults changed): re-record the baseline");
  }
  const checks = [];
  const check = (metric, metricClass, gated) => {
    const base = baseline[metricClass]?.[metric];
    const current = result[metricClass]?.[metric];
    if (typeof base !== "number" || typeof current !== "number" || !Number.isFinite(current)) {
      checks.push({ metric, class: metricClass, baseline: base, current, gated, ok: !gated, missing: true });
      if (gated) problems.push(`${metric}: missing or not finite`);
      return;
    }
    const limit = base * (1 + maxRegressPct / 100);
    const regressPct = base > 0 ? (current / base - 1) * 100 : current > 0 ? Number.POSITIVE_INFINITY : 0;
    const ok = !gated || current <= limit;
    checks.push({ metric, class: metricClass, baseline: base, current, limit, regressPct, gated, ok });
    if (!ok) problems.push(`${metric}: ${current} > ${Math.round(limit * 1000) / 1000} (baseline ${base}, +${maxRegressPct}%)`);
  };
  for (const metric of LOAD_INSENSITIVE_METRICS) check(metric, "loadInsensitive", true);
  for (const metric of TIMING_METRICS) check(metric, "timing", gateTiming);
  return { ok: problems.length === 0, problems, checks };
};
