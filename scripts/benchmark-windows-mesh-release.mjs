#!/usr/bin/env node
// Hosted-only public-API T12 probe. Baseline and candidate use THIS same driver; no application replay.
// expected-skip: non-Windows/non-hosted cannot measure native release overhead.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

if (process.platform !== "win32" || process.env.GITHUB_ACTIONS !== "true") {
  console.log("SKIP-EXPECTED: #9569 performance requires hosted Windows CI; no local deletion or Linux estimate");
  process.exitCode = 4;
} else {
  const { values } = parseArgs({ options: {
    project: { type: "string" }, label: { type: "string" }, head: { type: "string" }, out: { type: "string" },
  } });
  if (!values.project || !["baseline", "candidate"].includes(values.label) || !/^[a-f0-9]{40}$/.test(values.head ?? "")) {
    throw new Error("required: --project PATH --label baseline|candidate --head EXACT_SHA [--out PATH]");
  }
  const project = fs.realpathSync(values.project);
  const digest = file => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
  const meshEntry = path.join(project, "dist/mesh.js");
  const helperSha256 = digest(path.join(project, "dist/native/fabric-win-remove.exe"));
  const hookEntry = path.join(project, "dist/storage/windows-mesh-release.js");
  const { MeshStore } = await import(pathToFileURL(meshEntry).href);
  // The pinned baseline predates this stable entry/hook. Absence is not a measured zero children.
  const releaseAdapter = fs.existsSync(hookEntry) ? await import(pathToFileURL(hookEntry).href) : undefined;
  const hooks = releaseAdapter?.windowsMeshReleaseTestHooks;
  if (values.label === "candidate" && !hooks) throw new Error("candidate release instrumentation missing");
  const summary = numbers => {
    if (!numbers.length) return null;
    const sorted = [...numbers].sort((a, b) => a - b);
    const at = q => sorted[Math.ceil(sorted.length * q) - 1];
    return { count: numbers.length, p50Ms: at(0.5), p95Ms: at(0.95), totalMs: numbers.reduce((a, b) => a + b, 0) };
  };
  // Every spawned observer/worker resolves ONLY on close, including spawn errors and timeout.
  const joined = (command, args, env, timeout, onChild) => new Promise(resolve => {
    const child = spawn(command, args, { env, timeout, windowsHide: true,
      stdio: onChild ? ["ignore", "pipe", "pipe", "ipc"] : ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "", error, overflow = false;
    const append = (old, chunk) => {
      const text = old + String(chunk);
      if (text.length > 1024 * 1024) { overflow = true; return text.slice(-1024 * 1024); }
      return text;
    };
    child.stdout.on("data", chunk => { stdout = append(stdout, chunk); });
    child.stderr.on("data", chunk => { stderr = append(stderr, chunk); });
    child.once("error", caught => { error = caught.message; });
    child.once("close", (code, signal) => resolve({ pid: child.pid ?? null, code, signal, error: error ?? null,
      overflow, stdout, stderr, joinedAtUtc: new Date().toISOString() }));
    onChild?.(child);
  });
  const awaitStart = () => new Promise((resolve, reject) => {
    const finish = error => {
      clearTimeout(timer);
      process.off("message", message); process.off("disconnect", disconnected);
      if (process.connected) process.disconnect();
      if (error) reject(error); else resolve();
    };
    const message = value => finish(value?.type === "start" ? undefined : new Error("contention cohort aborted before start"));
    const disconnected = () => finish(new Error("contention controller disconnected before start"));
    const timer = setTimeout(() => finish(new Error("contention start deadline")), 120_000);
    process.once("message", message); process.once("disconnect", disconnected);
    process.send({ type: "ready" }, error => { if (error) finish(error); });
  });
  const census = async () => {
    // The one-shot PowerShell observer is itself a child; explicitly exclude its actual PID.
    const command = `$ErrorActionPreference='Stop'; $p=Get-Process -Id ${process.pid}; ` +
      `$c=@(Get-CimInstance Win32_Process -Filter 'ParentProcessId=${process.pid}' | Where-Object { $_.ProcessId -ne $PID } | ` +
      `Select-Object ProcessId,ParentProcessId,Name,CreationDate); ` +
      `@{sampledAtUtc=[DateTime]::UtcNow.ToString('o'); nodePid=$p.Id; handleCount=$p.HandleCount; ` +
      `observerPid=$PID; children=$c} | ConvertTo-Json -Compress -Depth 4`;
    const result = await joined("pwsh.exe", ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command], process.env, 30_000);
    if (result.code !== 0 || result.signal || result.error || result.overflow) throw new Error(`native census failed: ${JSON.stringify(result)}`);
    const sample = JSON.parse(result.stdout);
    if (sample.nodePid !== process.pid || !Number.isInteger(sample.handleCount) || !Array.isArray(sample.children)) {
      throw new Error(`invalid native census: ${result.stdout}`);
    }
    return { ...sample, childCount: sample.children.length, observerJoinedAtUtc: result.joinedAtUtc };
  };
  const run = async (root, count) => {
    const children = [], sections = [], failures = [];
    const before = await census();
    if (process.send) await awaitStart();
    if (hooks) hooks.afterChild = sample => { children.push({ ...sample }); };
    const started = performance.now();
    let store;
    try {
      store = new MeshStore(root, 64 * 1024, 500, { lockProtocol: 1, lockTimeoutMs: 30_000 });
      for (let i = 0; i < count; i++) {
        const sectionStart = performance.now();
        await store.exclusive(() => undefined);
        sections.push(performance.now() - sectionStart);
      }
    } catch (error) { failures.push(String(error.stack ?? error)); }
    finally {
      if (hooks) delete hooks.afterChild;
      try { store?.closeState(); } catch (error) { failures.push(`store close failed: ${error}`); }
    }
    const totalMs = performance.now() - started;
    const after = await census();
    if (sections.length !== count) failures.push(`${sections.length}/${count} completed sections (no replay)`);
    if (values.label === "candidate" && children.length !== count) failures.push(`${children.length}/${count} joined release-child samples`);
    if (children.some(sample => !Number.isFinite(sample.elapsedMs) || sample.elapsedMs < 0 ||
      sample.digest !== helperSha256 || sample.attempt !== 1 || sample.outcome !== "released")) {
      failures.push("release-child sample failed timing/digest/first-attempt/released validation");
    }
    return { pid: process.pid, sections, children: hooks ? children : null, totalMs, failures,
      nativeHandleCensus: { before, after, delta: after.handleCount - before.handleCount } };
  };
  // Candidate controller only: a separate fixture after the unchanged serial/contention cohorts.
  const diagnosePhases = async fixtureRoot => {
    // Never load diagnostic support in the baseline or workers, or inside any existing measurement envelope.
    const { PHASE_PAIRS, aggregatePhases, phaseOrder, phaseReports, releaseChildEnv,
      validateCapability, validateNoopResult, validateReleaseSamples } = await import("./windows-mesh-release-phases.mjs");
    const pairs = [], warmups = [], failures = [];
    let capability, store, attribution;
    const noop = pair => {
      // Match attempt(): exact captured capability, lstat + rehash immediately BEFORE the timer, never adopt a hash.
      const stat = fs.lstatSync(capability.absolutePath);
      if (!stat.isFile() || stat.isSymbolicLink() || digest(capability.absolutePath) !== capability.digest) {
        throw new Error("diagnostic retained helper no longer matches captured capability");
      }
      const started = performance.now();
      const child = spawnSync(capability.absolutePath, ["--capability-v1"], {
        timeout: 5000, windowsHide: true, maxBuffer: 4096, env: releaseChildEnv(process.env),
      });
      const result = { status: child.status, signal: child.signal, error: child.error?.message ?? null,
        stdout: Buffer.from(child.stdout ?? []).toString("utf8"), stderr: Buffer.from(child.stderr ?? []).toString("utf8") };
      try { validateNoopResult(result); }
      finally {
        pair.a = { elapsedMs: performance.now() - started, absolutePath: capability.absolutePath, digest: capability.digest, ...result };
      }
    };
    const measurePair = async (target, index) => {
      const pair = { index, order: phaseOrder(index), a: null, b: [] };
      target.push(pair); // Keep partial evidence even if correctness fails; never replay an operation.
      for (const phase of pair.order) {
        if (phase === "a") noop(pair);
        else {
          hooks.afterChild = sample => { pair.b.push({ ...sample }); };
          try { await store.exclusive(() => undefined); }
          finally { delete hooks.afterChild; }
          validateReleaseSamples(pair.b, capability);
        }
      }
    };
    try {
      capability = Object.freeze({ ...releaseAdapter.prepareWindowsMeshCapability() });
      validateCapability(capability, helperSha256);
      if (hooks.checkpoint || hooks.deadlineMs !== undefined) throw new Error("diagnostic requires default production child envelope");
      store = new MeshStore(fixtureRoot, 64 * 1024, 500, { lockProtocol: 1, lockTimeoutMs: 30_000 });
      await measurePair(warmups, 0); // Capability, store and one pair primed outside the measured 100 pairs.
      for (let i = 0; i < PHASE_PAIRS; i++) await measurePair(pairs, i);
      attribution = aggregatePhases({ head: values.head, capability, packagedDigest: helperSha256, pairs, warmups });
    } catch (error) { failures.push(String(error.stack ?? error)); }
    finally {
      delete hooks.afterChild;
      try { store?.closeState(); } catch (error) { failures.push(`diagnostic store close failed: ${error}`); }
    }
    let leftovers = [];
    try {
      leftovers = fs.existsSync(fixtureRoot) ? fs.readdirSync(fixtureRoot).filter(entry =>
        entry === ".lock" || entry === "custody.lock" || entry.startsWith("r.") || entry.includes(".released.")) : [];
      if (leftovers.some(entry => entry === ".lock" || entry === "custody.lock")) failures.push("diagnostic canonical lock residue");
    } catch (error) { failures.push(`diagnostic residue inspection failed: ${error}`); }
    attribution ??= { status: "invalid", head: values.head, capability: capability ?? null, pairs,
      warmups: { capabilityPreparation: "attempted before all measurement timers", pairs: warmups },
      phases: phaseReports(values.head, capability, "invalid", pairs), heuristic: null, residual: null };
    if (failures.length) attribution.status = "invalid";
    for (const phase of Object.values(attribution.phases)) {
      if (failures.length && phase.phase !== "c") phase.status = "invalid";
      console.log(`WINDOWS_MESH_RELEASE_PHASE: ${JSON.stringify(phase)}`);
    }
    return { ...attribution, fixtureRoot, leftovers, failures };
  };
  const workerRoot = process.env.FABRIC_RELEASE_BENCH_WORKER_ROOT;
  if (workerRoot) {
    const result = await run(workerRoot, 25);
    console.log(`FABRIC_RELEASE_BENCH_WORKER: ${JSON.stringify(result)}`);
    if (result.failures.length) process.exitCode = 1;
  } else {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-release-bench-"));
    try {
      const serial = await run(path.join(root, "serial"), 100);
      const shared = path.join(root, "contended");
      fs.mkdirSync(shared);
      const before = await census();
      const started = performance.now();
      const cohort = [], ready = new Set();
      let cohortStartedAtUtc = null, aborted = false;
      const send = (child, type) => { if (child.connected) child.send({ type }, () => {}); };
      const onWorker = child => {
        cohort.push(child);
        child.on("message", value => {
          if (value?.type !== "ready") return;
          ready.add(child.pid);
          if (aborted) { send(child, "abort"); return; }
          if (ready.size === 4) {
            cohortStartedAtUtc = new Date().toISOString();
            for (const peer of cohort) send(peer, "start");
          }
        });
        child.once("close", () => {
          if (!cohortStartedAtUtc) { aborted = true; for (const peer of cohort) send(peer, "abort"); }
        });
        if (aborted) send(child, "abort");
      };
      const workers = Array.from({ length: 4 }, () => joined(process.execPath,
        [fileURLToPath(import.meta.url), ...process.argv.slice(2)],
        { ...process.env, FABRIC_RELEASE_BENCH_WORKER_ROOT: shared }, 180_000, onWorker));
      // No fail-fast Promise.all: even a failed contender must not escape a sibling rejection.
      const settled = await Promise.allSettled(workers);
      const wallMs = performance.now() - started;
      const after = await census();
      const failures = [...serial.failures], contended = [], workerReceipts = [];
      for (const result of settled) {
        if (result.status === "rejected") { failures.push(String(result.reason)); continue; }
        const { stdout, ...receipt } = result.value;
        workerReceipts.push(receipt);
        const lines = stdout.split(/\r?\n/).filter(line => line.startsWith("FABRIC_RELEASE_BENCH_WORKER: "));
        if (lines.length === 1) {
          try {
            const sample = JSON.parse(lines[0].slice("FABRIC_RELEASE_BENCH_WORKER: ".length));
            if (sample.pid !== receipt.pid) throw new Error("worker PID mismatch");
            contended.push(sample);
            failures.push(...sample.failures);
          } catch (error) { failures.push(`invalid worker receipt: ${error}`); }
        } else failures.push(`missing worker receipt: ${stdout}`);
        if (receipt.code !== 0 || receipt.signal || receipt.error || receipt.overflow) failures.push(`worker failed: ${JSON.stringify(receipt)}`);
      }
      const childSamples = contended.flatMap(sample => sample.children ?? []);
      if (contended.length !== 4 || contended.reduce((n, sample) => n + sample.sections.length, 0) !== 100) failures.push("contention did not complete 4 x 25 sections");
      if (values.label === "candidate" && childSamples.length !== 100) failures.push(`${childSamples.length}/100 contended joined release-child samples`);
      const leftovers = ["serial", "contended"].flatMap(name => fs.existsSync(path.join(root, name))
        ? fs.readdirSync(path.join(root, name)).filter(entry => entry === ".lock" || entry === "custody.lock" || entry.startsWith("r.") || entry.includes(".released."))
          .map(entry => `${name}/${entry}`) : []);
      const canonicalLeftovers = leftovers.filter(entry => [".lock", "custody.lock"].includes(path.basename(entry)));
      if (canonicalLeftovers.length) failures.push(`canonical residue: ${canonicalLeftovers.join(", ")}`);
      // Cleanup-pending T is a valid release outcome: count/report it, not a timing/handle-style flaky threshold.
      const retirementResidue = leftovers.filter(entry => !canonicalLeftovers.includes(entry));
      const childSummary = samples => hooks ? { status: "instrumented", provenance: "afterChild hook: joined release attempts only; excludes capability-preparation children",
        timing: summary(samples.map(sample => sample.elapsedMs)), samples } : { status: "not-instrumented", timing: null, samples: null };
      const phaseAttribution = values.label === "candidate" ? await diagnosePhases(path.join(root, "diagnostic")) : null;
      if (phaseAttribution) failures.push(...phaseAttribution.failures);
      const receipt = {
        ...(phaseAttribution ? { phaseAttribution } : {}),
        schema: "fabric-windows-mesh-release-benchmark/2", label: values.label, head: values.head, project,
        sampledAtUtc: new Date().toISOString(), runner: { name: process.env.RUNNER_NAME, os: os.release(), arch: process.arch,
          runId: process.env.GITHUB_RUN_ID, attempt: process.env.GITHUB_RUN_ATTEMPT }, runtime: process.version,
        artifacts: { meshEntry, meshSha256: digest(meshEntry), lockSha256: digest(path.join(project, "bun.lock")),
          helperSha256,
          nativeManifest: JSON.parse(fs.readFileSync(path.join(project, "dist/native/manifest.json"), "utf8")) },
        serial: { sections: summary(serial.sections), totalMs: serial.totalMs,
          totalProvenance: "store construction + 100 sections + closeState; excludes import and boundary census",
          releaseChildren: childSummary(serial.children ?? []), nativeHandleCensus: serial.nativeHandleCensus },
        fourProcessContention: { sections: summary(contended.flatMap(sample => sample.sections)), wallMs,
          wallProvenance: "worker spawn through all close events; includes worker import and boundary census, unlike section latencies",
          startBarrier: { readyPids: [...ready], cohortStartedAtUtc, provenance: "one-shot IPC start after all four imports and before-census; no polling" },
          releaseChildren: childSummary(childSamples), workers: contended, workerReceipts,
          nativeHandleCensus: { before, after, delta: after.handleCount - before.handleCount } },
        nativeCensusProvenance: "Get-Process(node PID).HandleCount includes census-observer transport handles. Direct Win32_Process child lists exclude that observer's actual PID. Worker before/after deltas also include IPC startup/disconnect. All are boundary samples, not peak/total descendant counts; report-only",
        fixtureRoot: root, leftovers, retirementResidue: { count: retirementResidue.length, paths: retirementResidue,
          provenance: "directory entries sampled after all operations/workers joined, before owned fixture cleanup" }, failures,
      };
      if (values.out) fs.writeFileSync(values.out, `${JSON.stringify(receipt, null, 2)}\n`);
      console.log(`WINDOWS_MESH_RELEASE_PERF: ${JSON.stringify(receipt)}`);
      if (failures.length) process.exitCode = 1;
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  }
}
