// Pure characterization/validation shared by the hosted driver and portable tests; no child or filesystem work.
import path from "node:path";

export const PHASE_PAIRS = 100;
export const phaseOrder = index => index % 2 === 0 ? ["a", "b"] : ["b", "a"];
export const releaseChildEnv = inherited => {
  const env = {};
  for (const [key, value] of Object.entries(inherited)) if (!key.toUpperCase().startsWith("FABRIC_WIN_MESH_TEST_")) env[key] = value;
  return env;
};
const requireValid = (ok, reason) => { if (!ok) throw new Error(`phase attribution: ${reason}`); };
const elapsed = value => requireValid(Number.isFinite(value) && value >= 0, "invalid elapsed sample");

export const validateCapability = (capability, packagedDigest) => {
  requireValid(capability?.meshAbi === 1 && typeof capability.absolutePath === "string" &&
    path.win32.isAbsolute(capability.absolutePath) && !capability.absolutePath.includes("\0") &&
    /^[a-f0-9]{64}$/.test(capability.digest) && capability.digest === packagedDigest,
  "capability must retain the adapter's absolute path and pinned packaged digest");
};
const sameHelper = (sample, capability) => requireValid(sample?.absolutePath === capability.absolutePath &&
  sample?.digest === capability.digest, "sample helper path/digest mismatch");

export const validateNoopResult = result => {
  requireValid(result.status === 0 && result.signal === null && result.error === null &&
    (result.stdout === "FMRCAP1 mesh-retire-v1 abi=1\n" || result.stdout === "FMRCAP1 mesh-retire-v1 abi=1\r\n") &&
    result.stderr === "", "capability child must join with exact output, exit 0, no signal/error/stderr");
};
export const validateNoopSample = (sample, capability) => {
  sameHelper(sample, capability);
  elapsed(sample.elapsedMs);
  validateNoopResult(sample);
};
export const validateReleaseSamples = (samples, capability) => {
  requireValid(Array.isArray(samples) && samples.length === 1, "operation requires exactly one release child (no retry/replay)");
  const sample = samples[0];
  sameHelper(sample, capability);
  elapsed(sample.elapsedMs);
  requireValid(sample.attempt === 1 && sample.outcome === "released", "release must be attempt 1 and released");
  return sample;
};

const summary = numbers => {
  requireValid(numbers.length > 0 && numbers.every(Number.isFinite), "invalid summary samples");
  const sorted = [...numbers].sort((a, b) => a - b);
  const totalMs = numbers.reduce((a, b) => a + b, 0);
  requireValid(Number.isFinite(totalMs), "nonfinite total");
  return { count: numbers.length, p50Ms: sorted[Math.ceil(numbers.length * 0.5) - 1],
    p95Ms: sorted[Math.ceil(numbers.length * 0.95) - 1], totalMs };
};

export const phaseReports = (head, capability, status = "invalid", pairs = []) => {
  const common = { head, helperDigest: capability?.digest ?? null, absolutePath: capability?.absolutePath ?? null,
    status, count: 0, p50Ms: null, p95Ms: null, totalMs: null };
  return {
    a: { ...common, phase: "a", count: pairs.filter(pair => pair.a).length,
      provenance: "same retained verified helper --capability-v1; rehash outside timer; env construction + spawnSync/join + strict result processing; no lock filesystem mutations, not zero machine work" },
    b: { ...common, phase: "b", count: pairs.reduce((n, pair) => n + (pair.b?.length ?? 0), 0),
      provenance: "real MeshStore.exclusive; exactly one afterChild attempt-1 released envelope per operation; excludes preparation/rehash and outer exclusive time" },
    c: { ...common, phase: "c", status: "not-separable",
      provenance: "exact native handle-open + identity-check is not exposed by Node standard APIs; no fs.open/fstat or dry-run substitute, no new ABI/hook" },
  };
};

export const aggregatePhases = ({ head, capability, packagedDigest, pairs, warmups }) => {
  requireValid(/^[a-f0-9]{40}$/.test(head), "exact head required");
  validateCapability(capability, packagedDigest);
  requireValid(Array.isArray(pairs) && pairs.length === PHASE_PAIRS, `requires exactly ${PHASE_PAIRS} measured pairs`);
  requireValid(Array.isArray(warmups) && warmups.length === 1, "requires one reported warmup pair");
  const validatePair = (pair, index) => {
    requireValid(pair?.index === index && JSON.stringify(pair.order) === JSON.stringify(phaseOrder(index)), "pair index/order mismatch");
    validateNoopSample(pair.a, capability);
    validateReleaseSamples(pair.b, capability);
  };
  for (let index = 0; index < warmups.length; index++) validatePair(warmups[index], index);
  for (let index = 0; index < pairs.length; index++) validatePair(pairs[index], index);
  const phases = phaseReports(head, capability, "measured", pairs);
  Object.assign(phases.a, summary(pairs.map(pair => pair.a.elapsedMs)));
  Object.assign(phases.b, summary(pairs.map(pair => pair.b[0].elapsedMs)));
  const residualSamples = pairs.map(pair => pair.b[0].elapsedMs - pair.a.elapsedMs);
  const quotient = phases.b.p50Ms > 0 ? phases.a.p50Ms / phases.b.p50Ms : null;
  const ratio = Number.isFinite(quotient) ? quotient : null;
  return { status: "measured", head, capability, measuredPairCount: PHASE_PAIRS,
    warmups: { capabilityPreparation: "prepareWindowsMeshCapability before store construction and all timers", pairs: warmups },
    phases, pairs,
    heuristic: { aOverB: ratio, basis: "a.p50Ms / b.p50Ms", threshold: 0.70,
      decision: ratio !== null && ratio >= 0.70 ? "suggests-process-transport-class" : "inconclusive",
      provenance: "measurement-only heuristic, never a performance assertion or proof that release time is all spawn" },
    residual: { ...summary(residualSamples), samples: residualSamples,
      provenance: "paired b-a coarse residual, negatives retained; NOT pure verification: capability mode loads token SIDs; full release additionally starts watchdog, parses request, verifies, mutates, cleans up and processes results" },
  };
};
