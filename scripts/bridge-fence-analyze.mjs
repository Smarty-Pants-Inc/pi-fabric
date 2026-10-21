#!/usr/bin/env node
// Read-only compact view of the raw per-process evidence, or an identical-config comparison.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

const read = file => JSON.parse(fs.readFileSync(file, "utf8"));
const finite = value => typeof value === "number" && Number.isFinite(value);
const count = value => Number.isSafeInteger(value) && value >= 0;
const sha = value => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
const equal = (left, right) => JSON.stringify(left) === JSON.stringify(right);
const requiredScripts = ["benchmark-bridge-fence.ts", "bridge-fence-backup.ts", "bridge-fence-profile.ts", "bridge-fence-inventory.ts",
  "bridge-fence-metrics.ts", "bridge-fence-runner.mjs", "bridge-fence-analyze.mjs"].map(name => `scripts/${name}`);
const phases = ["startup", "load", "drain", "idlebusy", "stop"];
const safePhase = phase => phases.includes(phase) ? phase : "other";
function dist(values) {
  const ordered = values.slice().sort((a, b) => a - b);
  const at = q => ordered.length ? ordered[Math.max(0, Math.ceil(ordered.length * q) - 1)] : null;
  return { n: ordered.length, p50Ms: at(.5), p99Ms: at(.99), maxMs: ordered.at(-1) ?? null };
}
function txGroups(holds) {
  const group = values => {
    const ordered = values.slice().sort((a, b) => a.ms - b.ms || a.began - b.began);
    const sample = tx => {
      if (!tx) return null;
      const b = tx.breakdown;
      const valid = b && [b.aMs, b.bMs, b.cMs].every(value => finite(value) && value >= 0) &&
        Math.abs(b.aMs + b.bMs + b.cMs - tx.ms) <= .05;
      return { holdMs: tx.ms, began: tx.began, reportIndex: tx.reportIndex, transactionIndex: tx.transactionIndex,
        phase: safePhase(tx.phase), path: ["recovery", "recovery-afterCommit", "fallback-or-recovery", "full-plan-fallback", "optimized-prepare",
        "afterCommit-reacquire", "effects-or-unattributed", "not-mirror", "unattributed-prepare"].includes(tx.path) ? tx.path : "unattributed",
        aMs: valid ? b.aMs : null, bMs: valid ? b.bMs : null, cMs: valid ? b.cMs : null,
        complete: valid && b.complete === true, overlapInconsistent: !valid || b.overlapInconsistent !== false,
        aFraction: valid && tx.ms > 0 ? b.aMs / tx.ms : null };
    };
    const p50Tx = sample(ordered[Math.max(0, Math.ceil(ordered.length * .5) - 1)]);
    const p99Tx = sample(ordered[Math.max(0, Math.ceil(ordered.length * .99) - 1)]), maxTx = sample(ordered.at(-1));
    return { ...dist(values.map(tx => tx.ms)), p50Tx, p99Tx, maxTx,
      incompleteTransactions: values.filter(tx => !tx.breakdown?.complete).length,
      inconsistentTransactions: values.filter(tx => tx.breakdown?.overlapInconsistent).length,
      go: [p99Tx, maxTx].every(tx => tx && tx.complete && !tx.overlapInconsistent && tx.aFraction >= .5) };
  };
  const actual = holds.filter(tx => tx.lockClass === "bridge" && tx.lockClassAttested === true && finite(tx.ms) && tx.ms >= 0);
  const recovery = tx => tx.phase === "startup";
  // Nonstartup authoritative work is NOT silently removed as 'recovery': old flags
  // cannot distinguish it from a full-plan fallback. Stop/final has its own all-TX tail.
  const steady = actual.filter(tx => ["load", "drain", "idlebusy"].includes(tx.phase));
  const groups = (key, values) => Object.fromEntries([...new Set(values.map(key))].sort().map(value => [value, group(values.filter(tx => key(tx) === value))]));
  return { all: group(actual), startup: group(actual.filter(recovery)), steady: group(steady),
    byPhase: groups(tx => safePhase(tx.phase), actual),
    byPath: groups(tx => ["recovery", "recovery-afterCommit", "fallback-or-recovery", "full-plan-fallback", "optimized-prepare", "afterCommit-reacquire", "unattributed-prepare"].includes(tx.path) ? tx.path : "other", actual) };
}
const pickNumbers = (value, keys) => Object.fromEntries(keys.filter(key => finite(value?.[key])).map(key => [key, value[key]]));
const snapshotView = snapshot => snapshot ? { counts: pickNumbers(snapshot.counts, ["kv", "meta", "changes", "tombstones"]),
  counters: pickNumbers(snapshot.counters, ["high_water", "commit_no", "state_bytes", "tombstone_ord", "epoch", "schema"]),
  hashes: Object.fromEntries(["kv", "meta", "changes", "tombstones"].filter(key => sha(snapshot.hashes?.[key])).map(key => [key, snapshot.hashes[key]])),
  metadataConsistent: snapshot.metadataConsistent === true, quickCheck: snapshot.quickCheck === "ok" || snapshot.quickCheck === true,
  ...pickNumbers(snapshot, ["pageSize", "pageCount", "freePages", "logicalDbBytes"]) } : null;
function analyzeLive(out, summary, terminal, config, seed, reports) {
  const installFile = path.join(out, "backup-install.json");
  const install = fs.existsSync(installFile) ? read(installFile) : null;
  const backup = summary.backup, drivers = seed.drivers;
  const initial = backup?.initial, end = backup?.end, delta = backup?.delta;
  const holds = reports.flatMap((report, reportIndex) => (report.metrics?.holds ?? []).map((tx, transactionIndex) => ({ ...tx, reportIndex, transactionIndex })));
  const bridgeTransactions = txGroups(holds);
  const samples = summary.ageSamples ?? reports.flatMap(report => report.ageSamples ?? []);
  const validAge = sample => Number.isInteger(sample.link) && sample.link >= 0 && sample.link < 6 && phases.includes(sample.phase) &&
    finite(sample.deliveryMs) && sample.deliveryMs >= 0 && finite(sample.sourceAppendToReceiptObservationMs) && sample.sourceAppendToReceiptObservationMs >= 0;
  const age = values => ({ samples: values.length, deliveryMs: dist(values.map(value => value.deliveryMs)),
    sourceAppendToReceiptObservationMs: dist(values.map(value => value.sourceAppendToReceiptObservationMs)) });
  const safeSamples = samples.filter(validAge);
  const ageByPhase = Object.fromEntries(phases.map(phase => [phase, age(safeSamples.filter(sample => sample.phase === phase))]).filter(([, value]) => value.samples));
  const ageByLink = Array.from({ length: 6 }, (_, link) => {
    const values = safeSamples.filter(sample => sample.link === link);
    return { link, ...age(values), byPhase: Object.fromEntries(phases.map(phase => [phase, age(values.filter(sample => sample.phase === phase))]).filter(([, value]) => value.samples)) };
  });
  const exitZero = children => Array.isArray(children) && children.every(child => child.exitCode === 0 && child.signalCode === null);
  const sourcePins = summary.sourcePins ?? {}, endPins = summary.endSourcePins ?? {};
  const sourceNames = [...new Set([...Object.keys(sourcePins), ...Object.keys(endPins)])].sort();
  const driverRows = drivers?.initialAdditiveKvRows;
  const expectedKvRows = initial?.counts?.kv + driverRows;
  const afterAdd = seed.nativeAfterAdd;
  const afterAddOk = afterAdd ? count(afterAdd.removedImportedRows) && count(afterAdd.changedImportedRows) &&
    afterAdd.expectedKvRows === expectedKvRows - afterAdd.removedImportedRows && afterAdd.actualKvRows === afterAdd.expectedKvRows &&
    afterAdd.addedRows === driverRows && afterAdd.snapshot?.counts?.kv === afterAdd.actualKvRows && afterAdd.snapshot?.metadataConsistent === true &&
    (afterAdd.snapshot?.quickCheck === "ok" || afterAdd.snapshot?.quickCheck === true) :
    seed.stateCalibration?.kvRows === expectedKvRows;
  const progress = summary.writerProgress ?? [];
  const cpuChildren = summary.cpuChildren;
  const cpuReports = summary.cpuReports;
  const pressure = summary.phasePressure;
  const response = summary.harnessResponse;
  const scheduling = summary.harnessScheduling;
  const expectedGroups = { controller: 1, writers: 6, readers: 100, cpuWorkers: config.cpuWorkers };
  const affinityOk = Number.isInteger(config.cpuCount) && config.cpuCount >= 4 && config.cpuCount <= 6 &&
    typeof config.cpuSet === "string" && /^\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*$/.test(config.cpuSet) &&
    ["load", "idlebusy"].every(phase => scheduling?.[phase]?.valid === true && scheduling[phase].cpuCount === config.cpuCount &&
      [scheduling[phase].start, scheduling[phase].end].every(snapshot => snapshot?.cpuSet === config.cpuSet &&
        Object.entries(expectedGroups).every(([group, expected]) => snapshot.groups?.[group]?.processes === expected &&
          snapshot.groups[group].sampled === expected && snapshot.groups[group].missing === 0 && snapshot.groups[group].affinityMismatch === 0 &&
          snapshot.groups[group].cgroupMismatch === 0)));
  const ownPressureOk = affinityOk && ["load", "idlebusy"].every(phase => {
    const window = pressure?.[phase];
    if (!window || window.ownScopeAttested !== true || !(window.elapsedMs > 0) || window.elapsedMs !== window.end?.at - window.start?.at ||
      window.start?.ownScopeAttested !== true || window.end?.ownScopeAttested !== true || !sha(window.start?.cgroupHash) || window.start.cgroupHash !== window.end.cgroupHash ||
      scheduling[phase].start?.cgroupHash !== window.start.cgroupHash || scheduling[phase].end?.cgroupHash !== window.end.cgroupHash) return false;
    return [["cpu", "cpuSomePct"], ["io", "ioSomePct"], ["memory", "memorySomePct"]].every(([resource, field]) => {
      const first = window.start.cgroupPressure?.[resource]?.some, last = window.end.cgroupPressure?.[resource]?.some;
      return finite(first) && finite(last) && last >= first && finite(window[field]) && window[field] >= 0 && window[field] <= 100 &&
        Math.abs(window[field] - (last - first) / (window.elapsedMs * 1000) * 100) <= .000001;
    });
  });
  const pressureOk = ["load", "idlebusy"].every(phase => pressure?.[phase]?.elapsedMs > 0 &&
    pressure[phase].elapsedMs === pressure[phase].end?.at - pressure[phase].start?.at &&
    ["cpuBusyPct", "cpuPsiSomePct", "ioPsiSomePct", "memoryPsiSomePct"].every(key => finite(pressure[phase][key]) && pressure[phase][key] >= 0 && pressure[phase][key] <= 100));
  const responseOk = count(response?.samples) && response.samples > 0 && finite(response?.maxLagMs) && response.maxLagMs >= 0 &&
    finite(response?.p99LagMs) && response.p99LagMs >= 0 && response.p99LagMs <= response.maxLagMs;
  const assertions = {
    terminalOk: terminal.ok === true && summary.ok === true, childrenClosed: terminal.childrenClosed === true,
    sixBridges100Sessions: summary.writers === 6 && summary.readerProcesses === 100 && config.sessions === 100,
    allExpectedChildrenExitedZero: summary.ownedChildren?.length === 106 && exitZero(summary.ownedChildren),
    allExpectedCpuChildrenExitedZero: count(config.cpuWorkers) && config.cpuWorkers <= 12 && summary.cpuWorkers === config.cpuWorkers &&
      cpuChildren?.length === config.cpuWorkers && exitZero(cpuChildren) && cpuReports?.length === config.cpuWorkers && cpuReports.every(report => report.ok === true &&
        ["load", "idlebusy"].every(phase => report.windows?.filter(window => window.phase === phase).length === 1)), 
    allReportsOk: reports.length === 106 && reports.every(report => report.ok === true),
    unchangedSource: summary.sourceUnchanged === true && sourceNames.length > 0 && sourceNames.every(name => sha(sourcePins[name]) && sourcePins[name] === endPins[name]),
    allHarnessScriptsPinned: requiredScripts.every(name => sha(sourcePins[name]) && sourcePins[name] === endPins[name]),
    executingAnalyzerMatchesPin: sourcePins["scripts/bridge-fence-analyze.mjs"] === createHash("sha256").update(fs.readFileSync(new URL(import.meta.url))).digest("hex"),
    pinnedLiveConfig: sha(config.backupSha256) && config.backupSha256.startsWith("912705ad") &&
      typeof config.fixtureSeed === "string" && /^[A-Za-z0-9-]{8,80}$/.test(config.fixtureSeed) && config.deliveriesPerLink === 32 &&
      config.batch === 32 && config.loadMs === 60000 && config.idleMs === 20000 && config.feedRetained === 4096,
    actualBackupCopyProof: install?.sha256 === config.backupSha256 && install?.copyHash === config.backupSha256 && install?.bootstrapPreservedSeparately === true &&
      backup?.copySha256 === config.backupSha256 && seed.backup?.sha256 === config.backupSha256 && seed.backup?.copySha256 === config.backupSha256 &&
      equal(seed.backup?.initial, install?.initial) && install?.namespaceCheck?.collisions === 0 && install?.namespaceCheck?.malformedJson === 0 &&
      count(install?.bytes) && install.bytes === install?.initial?.logicalDbBytes && install.initial.logicalDbBytes === install.initial.pageSize * install.initial.pageCount &&
      install?.initial?.metadataConsistent === true && (install?.initial?.quickCheck === "ok" || install?.initial?.quickCheck === true),
    actualInitialSnapshot: initial?.counts?.kv === 4763 && initial?.counts?.changes === 4041 && initial?.counts?.tombstones === 1000 && initial?.counts?.meta === 17 &&
      equal(initial, install?.initial) && initial?.metadataConsistent === true && (initial?.quickCheck === "ok" || initial?.quickCheck === true) &&
      ["kv", "meta", "changes", "tombstones"].every(table => sha(initial?.hashes?.[table])),
    stagedSourceStartEndUnchanged: backup?.sourceUnchanged === true && backup?.sha256 === config.backupSha256 &&
      backup?.sourceStartSha256 === config.backupSha256 && backup?.sourceEndSha256 === config.backupSha256 && backup?.fixtureSeed === config.fixtureSeed,
    nativeDriverAddReceipt: count(driverRows) && driverRows > 0 && count(drivers?.hubNativeRootsAdded) && count(drivers?.workRowsAdded) && afterAddOk,
    nativeFinalAccounting: end?.metadataConsistent === true && (end?.quickCheck === "ok" || end?.quickCheck === true) &&
      ["unchanged", "changed", "removed", "added"].every(key => count(delta?.[key])) && delta.unchanged + delta.changed + delta.removed === initial?.counts?.kv &&
      initial?.counts?.kv - delta.removed + delta.added === end?.counts?.kv && delta.initialKvHash === initial?.hashes?.kv && delta.endKvHash === end?.hashes?.kv &&
      ["kv", "meta", "changes", "tombstones"].every(table => delta.countDeltas?.[table] === end?.counts?.[table] - initial?.counts?.[table]) &&
      ["high_water", "commit_no", "state_bytes", "tombstone_ord", "epoch", "schema"].every(key =>
        finite(initial?.counters?.[key]) && finite(end?.counters?.[key]) && delta.counterDeltas?.[key] === end.counters[key] - initial.counters[key]),
    realNativeArchivedHistory: seed.historicalEvents === 46995 && summary.correctness?.historicalEvents === 46995 && summary.archiveEnabled === true &&
      seed.archiveVerification?.head?.sequence === 46995 && seed.archiveVerification?.samples?.length === 3 && seed.eventLogBytes > 40000000,
    exact192CorrectDeliveries: summary.correctness?.ok === true && summary.correctness?.sent === 192 && summary.correctness?.delivered === 192 &&
      ["drops", "duplicates", "outOfOrder", "invalid"].every(key => summary.correctness[key] === 0) &&
      [summary.correctness.sentByWriter, summary.correctness.deliveredByWriter].every(values => values?.length === 6 && values.every(value => value === 32)) &&
      reports.slice(0, 6).every(report => report.sent === 32 && report.delivered === 32),
    everyLinkProgress: progress.length === 6 && new Set(progress.map(writer => writer.index)).size === 6 && progress.every(writer =>
      Number.isInteger(writer.index) && writer.index >= 0 && writer.index < 6 && writer.sent === 32 && writer.delivered === 32 && Array.isArray(writer.progress) && writer.progress.length > 0),
    all192AgesJoined: samples.length === 192 && safeSamples.length === 192 && ageByLink.every(link => link.samples === 32) && summary.deliveryAge?.missingJoins === 0,
    nativeBridgeClassAttested: bridgeTransactions.all.n > 0 && holds.every(tx => ["bridge", "writeBatch", "other"].includes(tx.lockClass) && typeof tx.lockClassAttested === "boolean"),
    phaseHooksObserved: reports.slice(0, 6).every(report => report.metrics?.phaseHookSeen === true),
    actualTransactionIntervalsValid: holds.every(tx => finite(tx.ms) && tx.ms >= 0 && finite(tx.began) && finite(tx.ended) &&
      tx.ended >= tx.began && Math.abs(tx.ended - tx.began - tx.ms) <= .05),
    noInjection: typeof summary.trafficModel === "string" && summary.trafficModel.includes("no forced stale/fallback injection"),
    actualPhasePressure: pressureOk, measuredHarnessResponse: responseOk, entireHarnessCpuAffinity: affinityOk,
    ownedCpuSetPressure: ownPressureOk,
  };
  const evidenceGaps = Object.entries(assertions).filter(([, passed]) => !passed).map(([name]) => name);
  const maxHoldMs = bridgeTransactions.all.maxMs;
  return { out, ok: evidenceGaps.length === 0, assertions, config, seed, sourcePins, backup,
    byCallsite: {}, bridgeTransactions,
    liveView: { profile: "live-backup-b", ok: evidenceGaps.length === 0, assertions, evidenceGaps,
      configuration: { ...pickNumbers(config, ["sessions", "batch", "deliveriesPerLink", "cpuWorkers", "cpuCount", "loadMs", "idleMs", "readEveryMs", "controlEveryMs", "heartbeatMs", "feedRetained"]),
        cpuSet: typeof config.cpuSet === "string" && /^\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*$/.test(config.cpuSet) ? config.cpuSet : null,
        backupSha256: sha(config.backupSha256) ? config.backupSha256 : null },
      backup: { initial: snapshotView(initial), end: snapshotView(end),
        copyHashMatchesPin: assertions.actualBackupCopyProof, sourceStartEndUnchanged: assertions.stagedSourceStartEndUnchanged,
        nativeKvDelta: pickNumbers(delta, ["unchanged", "changed", "removed", "added"]),
        countDeltas: pickNumbers(delta?.countDeltas, ["kv", "meta", "changes", "tombstones"]),
        nativeCleanupAllowed: true },
      seed: { ...pickNumbers(seed, ["historicalEvents", "eventLogBytes"]), drivers: pickNumbers(drivers,
        ["hubNativeRootsAdded", "workRowsAdded", "initialAdditiveKvRows", "initialHostLeaseFiles", "initialParticipantFiles"]),
        expectedPostAddKvRows: finite(expectedKvRows) ? expectedKvRows : null },
      bridgeTransactions, step0: { go: evidenceGaps.length === 0 && bridgeTransactions.steady.go,
        fractionsPass: bridgeTransactions.steady.go,
        basis: "Same actual bridge-class TX ranked by hold; load/drain/idlebusy p99 AND max a/hold >= .5, complete and consistent. Startup and stop remain in all-phase tail. Nonstartup authoritative work is fallback-or-recovery, not silently excluded. No marginal-percentile addition." },
      age: { ...age(safeSamples), invalidSamples: samples.length - safeSamples.length, missingJoins: summary.deliveryAge?.missingJoins ?? null,
        byPhase: ageByPhase, byLink: ageByLink, origins: "deliveryMs: destination append minus source append; observationMs: receipt observation minus source append. Same-host clocks; not recipient runtime age." },
      correctness: { ok: summary.correctness?.ok === true,
        ...pickNumbers(summary.correctness, ["historicalEvents", "sent", "delivered", "drops", "duplicates", "outOfOrder", "invalid"]) },
      progress: progress.map(writer => ({ ...pickNumbers(writer, ["index", "sent", "delivered", "deliveredDuringLoad"]),
        samples: Array.isArray(writer.progress) ? writer.progress.length : 0 })),
      ambientHostPressure: Object.fromEntries(["load", "drain", "idlebusy"].map(phase => [phase, pickNumbers(pressure?.[phase], ["elapsedMs", "ambientCpuBusyPct", "cpuPsiSomePct", "ioPsiSomePct", "memoryPsiSomePct"])])),
      harnessSchedulingProxyNotPsi: Object.fromEntries(["load", "drain", "idlebusy"].map(phase => [phase, {
        ...pickNumbers(scheduling?.[phase], ["durationMs", "cpuCount", "runtimeNs", "runDelayNs", "runDelaySharePct"]),
        valid: scheduling?.[phase]?.valid === true,
        groups: Object.fromEntries(Object.keys(expectedGroups).map(group => [group, pickNumbers(scheduling?.[phase]?.groups?.[group],
          ["runtimeNs", "runDelayNs", "timeslices", "runDelaySharePct", "setCpuRuntimePct"])])) }])), 
      cpuWindows: (cpuReports ?? []).flatMap((report, worker) => (report.windows ?? []).map(window => ({ worker, phase: safePhase(window.phase),
        ...pickNumbers(window, ["requestedUntil", "from", "stoppedAt", "chunks"]),
        stopLatenessMs: finite(window.stoppedAt) && finite(window.requestedUntil) ? Math.max(0, window.stoppedAt - window.requestedUntil) : null,
        cpuUsage: pickNumbers(window.cpuUsage, ["user", "system"]) }))),
      harnessResponse: pickNumbers(response, ["maxLagMs", "p99LagMs", "samples"]),
      ownCpuSetPressure: { status: ownPressureOk ? "MEASURED" : "INCOMPLETE",
        method: "Dedicated transient scope cgroup-v2 kernel PSI total deltas; whole harness inherited 4–6 CPU affinity; owned child group/affinity readbacks. Host PSI and scheduler proxy are separate.",
        loadNear45To50PctTarget: finite(pressure?.load?.cpuSomePct) && pressure.load.cpuSomePct >= 45 && pressure.load.cpuSomePct <= 50,
        phases: Object.fromEntries(["load", "drain", "idlebusy"].map(phase => [phase, {
          ...pickNumbers(pressure?.[phase], ["elapsedMs", "cpuBusyPct", "cpuSomePct", "ioSomePct", "memorySomePct"]),
          ownScopeAttested: pressure?.[phase]?.ownScopeAttested === true }])) },
      reproductionCalibration: { maxHoldMs, secondsScaleReproduced: maxHoldMs !== null && maxHoldMs >= 1000,
        reportedNineSecondFloorReproduced: maxHoldMs !== null && maxHoldMs >= 9000 },
      limitations: "Native backup rows/meta/feed at capture; synthetic lease/participant/files, 46,995 native archived history events, finite 192-event burst and driver-only writes. Requested workers do not prove equal achieved pressure or production equivalence. CPU/SQL/FS categories overlap and are not exclusive phase components.",
      sourcePins: Object.fromEntries(requiredScripts.filter(name => sha(sourcePins[name])).map(name => [name, sourcePins[name]])) },
    deliveriesPerSecond: summary.deliveriesPerSecond };
}
function analyze(out) {
  const summary = read(path.join(out, "summary.json"));
  const terminal = read(path.join(out, "terminal.json"));
  const config = read(path.join(out, "config.json"));
  const seed = read(path.join(out, "seed.json"));
  const reports = [];
  for (let i = 0; i < summary.writers; i++) reports.push(read(path.join(out, `writer-${i}.json`)));
  for (let i = 0; i < summary.readerProcesses; i++) reports.push(read(path.join(out, `reader-${i}.json`)));
  if (config.profile === "live-backup-b") return analyzeLive(out, summary, terminal, config, seed, reports);
  const sql = {}, io = {};
  for (const report of reports) {
    for (const [key, value] of Object.entries(report.metrics.queries)) {
      const item = sql[key] ??= { calls: 0, totalMs: 0, maxMs: 0, sql: value.sql };
      item.calls += value.calls; item.totalMs += value.totalMs; item.maxMs = Math.max(item.maxMs, value.maxMs);
    }
    for (const [key, value] of Object.entries(report.metrics.io)) {
      const item = io[key] ??= { calls: 0, totalMs: 0, maxMs: 0 };
      item.calls += value.calls; item.totalMs += value.totalMs; item.maxMs = Math.max(item.maxMs, value.maxMs);
    }
  }
  const byTotal = entries => Object.fromEntries(Object.entries(entries).sort((a, b) => b[1].totalMs - a[1].totalMs));
  const assertions = { terminalOk: terminal.ok === true, childrenClosed: terminal.childrenClosed === true,
    allExpectedChildrenExitedZero: summary.ownedChildren.length === summary.writers + summary.readerProcesses &&
      summary.ownedChildren.every(child => child.exitCode === 0 && child.signalCode === null),
    unchangedSource: summary.sourceUnchanged === true, correctness: summary.correctness.ok === true,
    realSizedEventLog: seed.historicalEvents === (config.profile === "production-b" ? 46995 : 100000) &&
      summary.correctness.historicalEvents === seed.historicalEvents && seed.eventLogBytes > (config.profile === "production-b" ? 40000000 : 85000000),
    fleetStateNotEventsInKv: config.profile === "production-b" ? seed.hostRows === 501 && seed.participantRows === 1001 &&
      seed.controlSeenRows === 77 && seed.stateRows === 4750 : seed.hostRows === 801 && seed.participantRows === 1501 && seed.controlSeenRows >= 6000 &&
      seed.stateRows === seed.hostRows + seed.participantRows + seed.controlSeenRows,
    nativeArchiveForPrimaryProfile: config.profile !== "production-b" || (summary.archiveEnabled === true &&
      seed.archiveVerification?.head?.sequence === seed.historicalEvents && seed.archiveVerification?.samples?.length === 3),
    noInjection: summary.trafficModel.includes("no forced stale/fallback injection") };
  const aggregateRates = {};
  for (const phase of ["load", "idlebusy"]) {
    const rates = summary.readerRates.map(reader => reader.phases[phase]);
    const sum = name => rates.reduce((total, rate) => total + Number(rate[name] ?? 0), 0);
    const durationMs = phase === "load" ? config.loadMs : config.idleMs;
    aggregateRates[phase] = { offeredWindowMs: durationMs, deliveryWindowWithDrainMs: summary.windows[phase].to - summary.windows[phase].from,
      offeredControlHz: sum("requestedControlWriteHz"), offeredReadHz: sum("requestedReadHz"),
      offeredFileRenewalHz: sum("requestedNativeLeaseAndParticipantRenewalHz"),
      controlWrites: sum("controlWrites"), reads: sum("reads"), fileRenewals: sum("leaseAndParticipantRenewals"),
      achievedControlHz: sum("controlWrites") / durationMs * 1000, achievedReadHz: sum("reads") / durationMs * 1000,
      achievedFileRenewalHz: sum("leaseAndParticipantRenewals") / durationMs * 1000,
      missedControls: sum("missedControls"), missedReads: sum("missedReads"), missedRenewals: sum("missedRenewals"),
      maxObservedWalBytes: Math.max(...rates.map(rate => rate.maxWalBytes ?? 0)),
      maxObservedDbBytes: Math.max(...rates.map(rate => rate.maxDbBytes ?? 0)) };
  }
  const maxHoldMs = Math.max(0, ...Object.values(summary.byCallsite).map(group => group.maxMs));
  return { out, ok: Object.values(assertions).every(Boolean), assertions, config, seed,
    inventories: summary.inventories, feedRetention: summary.feedRetention, aggregateRates,
    reproductionCalibration: { maxHoldMs, secondsScaleReproduced: maxHoldMs >= 1000,
      reportedNineSecondFloorReproduced: maxHoldMs >= 9000,
      caveat: config.profile === "production-b" ? "Aggregate-calibrated native archive profile; exact topology/app sub-schemas, filesystem leaf layout and production operation timing remain synthetic assumptions. Absence of9s is not proof the production issue is absent."
        : "Secondary topology-heavy/archive-off shape. Absence of9s is not proof the production issue is absent." },
    deliveriesPerSecond: summary.deliveriesPerSecond, deliveredDuringLoad: summary.deliveredDuringLoad,
    deliveredDuringLoadByWriter: summary.deliveredDuringLoadByWriter,
    deliveriesPerSecondIncludingDrain: summary.deliveriesPerSecondIncludingDrain, correctness: summary.correctness,
    held: summary.held, byCallsite: byTotal(summary.byCallsite), byTransactionPath: summary.byTransactionPath,
    sourceByCallsite: byTotal(summary.sourceByCallsite ?? {}),
    fullScanFallbackCallbacks: summary.fullScanFallbackCallbacks,
    topologySqlScansInsideWriteLock: summary.topologySqlScansInsideWriteLock,
    kvFullSqlScansInsideWriteLock: summary.kvFullSqlScansInsideWriteLock,
    busyBeginAttempts: summary.busyBeginAttempts, readerRates: summary.readerRates,
    sql: byTotal(sql), synchronousFsInsideFence: byTotal(io),
    fsTimingCaveat: "Default fs-method wrappers; nested calls may overlap, so do not sum method totals as disjoint work. Native built-in references captured before wrapping may be missed. Primary B covers all actual hub write transactions; older A observer revisions wrapped fence callbacks only. See pinned observer source.",
    sourcePins: summary.sourcePins };
}
const brief = process.argv.includes("--brief");
const dirs = process.argv.slice(2).filter(arg => arg !== "--brief");
function view(result) {
  // Live backup output is allowlist-only even without --brief; never echo a model dump.
  if (result.liveView) return result.liveView;
  if (!brief) return result;
  const inventories = Object.fromEntries(Object.entries(result.inventories ?? {}).map(([phase, inventory]) => {
    const { prefixMix: _prefixMix, ...rest } = inventory; return [phase, rest];
  }));
  const { stateCalibration: _stateCalibration, ...seed } = result.seed;
  const { readerRates: _readerRates, sql, synchronousFsInsideFence, ...rest } = result;
  return { ...rest, seed, inventories,
    sqlTopByTotalMs: Object.fromEntries(Object.entries(sql).slice(0, 15)),
    synchronousFsInsideWriteTxTopByTotalMs: Object.fromEntries(Object.entries(synchronousFsInsideFence).slice(0, 30)) };
}
if (dirs.length < 1 || dirs.length > 2) throw new Error("usage: node scripts/bridge-fence-analyze.mjs EVIDENCE_DIR [AFTER_DIR]");
const results = dirs.map(analyze);
let comparisonOk = true;
if (results.length === 1) console.log(JSON.stringify(view(results[0]), null, 2));
else {
  const [before, after] = results;
  const live = before.config.profile === "live-backup-b" || after.config.profile === "live-backup-b";
  const sameConfig = ["loadMs", "idleMs", "batch", "sessions", "readEveryMs", "controlEveryMs", "heartbeatMs", "feedRetained", "targetDbBytes", "profile", "seedMs",
    ...(live ? ["backupSha256", "fixtureSeed", "deliveriesPerLink", "cpuWorkers", "cpuSet", "cpuCount"] : [])]
    .every(key => before.config[key] === after.config[key]);
  const sameSeed = live ? equal(before.backup?.initial, after.backup?.initial) && equal(before.seed.drivers, after.seed.drivers) &&
    before.seed.historicalEvents === after.seed.historicalEvents : ["historicalEvents", "stateRows", "hostRows", "participantRows", "controlSeenRows", "hostLeaseFiles", "participantFiles"]
    .every(key => before.seed[key] === after.seed[key]);
  const callsites = {};
  for (const site of new Set([...Object.keys(before.byCallsite), ...Object.keys(after.byCallsite)])) {
    callsites[site] = { before: before.byCallsite[site] ?? null, after: after.byCallsite[site] ?? null };
  }
  const harnessNames = [...new Set([...Object.keys(before.sourcePins), ...Object.keys(after.sourcePins), ...(live ? requiredScripts : [])])].filter(name => name.startsWith("scripts/"));
  const sameHarness = harnessNames.every(name => sha(before.sourcePins[name]) && before.sourcePins[name] === after.sourcePins[name]);
  comparisonOk = before.ok && after.ok && sameConfig && sameSeed && sameHarness;
  console.log(JSON.stringify({ ok: comparisonOk, sameConfig, sameSeed, sameHarness,
    note: "Identical requested load/rates and bounded backlog, not identical achieved counts: throughput is measured. Inspect actual readerRates/missed ticks before a causal claim.",
    throughput: { before: before.deliveriesPerSecond, after: after.deliveriesPerSecond,
      ratio: after.deliveriesPerSecond / before.deliveriesPerSecond }, callsites, before: view(before), after: view(after) }, null, 2));
}
if (!results.every(result => result.ok) || !comparisonOk) process.exitCode = 1;
