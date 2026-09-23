// Local orchestration benchmark, NOT a model-quality/cost benchmark. No credentials or network.
// bun run benchmark:prewalk --out /tmp/prewalk.json
// --quick reduces sampling; exit 0 = complete, 2 = complete with product findings, 1 = harness error.
// Companion evidence tooling: bench/prewalk/probe-prewalk-drift.mjs (stage/count diagnostic) and
// bench/prewalk/compare-prewalk-runs.mjs (gzip archive + run comparison).
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { performance } from "node:perf_hooks";
import { groupRows, stats } from "./lib/prewalk-bench-lib.mjs";
import { createPassiveHostSession } from "../../scripts/lib/passive-host-session.mjs";
import { build } from "esbuild";
import { Agent } from "@earendil-works/pi-agent-core";
import { SessionManager, convertToLlm } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";

const self = fileURLToPath(import.meta.url);
const root = path.resolve(path.dirname(self), "../..");
const hash = value => createHash("sha256").update(value).digest("hex");
const bytes = value => Buffer.byteLength(typeof value === "string" ? value : JSON.stringify(value));
const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
const frontier = { provider: "openai-codex", id: "synthetic-frontier", api: "openai-responses", reasoning: true };
const executor = { provider: "zro", id: "synthetic-executor", api: "openai-completions", reasoning: true };
const task = "Continue BENCH-TASK, preserve scope and verify the change.";
const outer = { role: "toolResult", toolName: "fabric_exec", toolCallId: "outer", isError: false,
  content: [{ type: "text", text: "BENCH-OUTER-RESULT" }], timestamp: 3 };
const assistant = (model, content, stopReason = "stop") => ({ role: "assistant", content,
  provider: model.provider, model: model.id, api: model.api, usage, stopReason, timestamp: 2 });
const textOf = messages => messages.map(m => typeof m.content === "string" ? m.content :
  (m.content ?? []).filter(p => p.type === "text").map(p => p.text).join("\n")).join("\n");
async function timed(fn) {
  const cpu = process.cpuUsage(), start = performance.now();
  const value = await fn();
  const wallMs = performance.now() - start, used = process.cpuUsage(cpu);
  return { value, wallMs, cpuMs: (used.user + used.system) / 1000 };
}
function planFor(profile, s) {
  const compact = { outcome: "BENCH-PLAN: finish a scoped change", steps: ["Inspect the owning code", "Make the smallest change", "Verify the result"],
    verification: ["Run the focused regression and build"], risks: "Preserve unrelated user work" };
  const long = "L".repeat(3900);
  return s.checkedPrewalkPlan(profile === "compact" ? compact : {
    outcome: "BENCH-PLAN " + long, steps: [long, long], verification: [long], risks: long,
  });
}

async function queueTrial(s, c, dropPlan = false) {
  const plan = planFor(c.profile, s), renderedPlan = s.prewalkPlanText(plan);
  const source = SessionManager.inMemory(root), sid = source.getSessionId();
  const history = [];
  for (let i = 0; i < c.history; i++) history.push(
    { role: "user", content: `Earlier independent task ${i}`, timestamp: 0 },
    assistant(frontier, [{ type: "text", text: `Earlier result ${i}: ` + "context ".repeat(40) }]),
  );
  const user = { role: "user", content: task, timestamp: 1 };
  const first = assistant(frontier, [
    { type: "thinking", thinking: "BENCH-DIGEST: verify the scoped boundary before completion.",
      thinkingSignature: '{"id":"rs_synthetic","type":"reasoning","encrypted_content":"synthetic"}' },
    { type: "toolCall", id: "outer", name: "fabric_exec", arguments: { code: "synthetic first edit" } },
  ], "toolUse");
  for (const m of [...history, user, first, outer]) source.appendMessage(m);
  const controller = new s.PrewalkController();
  if (c.prewalk) {
    controller.arm({ model: "zro/synthetic-executor", sessionId: sid, task, requirePlan: true, alwaysRearm: true });
    assert.equal(controller.submitPlan(sid, plan), true);
  }
  const nudges = c.prewalk ? [
    { role: "custom", customType: "pi-fabric-prewalk-armed", content: s.prewalkArmedPrompt("in-place", "zro/synthetic-executor", true), display: false, timestamp: 1 },
    { role: "custom", customType: "pi-fabric-prewalk-plan", content: s.prewalkPlanPrompt("zro/synthetic-executor"), display: false, timestamp: 1 },
    { role: "custom", customType: "pi-fabric-prewalk-continue", content: "STALE-BENCH-CONTINUATION", display: false,
      details: { mode: "in-place", continuationId: "stale" }, timestamp: 1 },
  ] : [];
  const captures = [];
  const agent = new Agent({ followUpMode: c.followUpMode, steeringMode: "one-at-a-time",
    // The transcript ends on the completed outer tool result, exactly like the
    // production boundary turn: the executor's first request is the loop's next
    // step, not a message after an artificial finished turn.
    initialState: { model: frontier, messages: [...history, user, ...nudges, first, outer] },
    convertToLlm,
    transformContext: async messages => {
      // LQ1: hand the controller's canonical pending continuation to the filter
      // so the executor's first request carries the task/plan/digest while
      // competing steers still drain. dropPlan simulates a lost payload (no
      // live message, no pending injection) so the canary must still fail.
      const pending = dropPlan ? undefined : controller.pendingContinuationMessage(sid);
      const filtered = s.filterPrewalkContinuationMessages(messages, id => controller.acceptContinuation(sid, id), pending).messages;
      return dropPlan ? filtered.filter(m => m.customType !== "pi-fabric-prewalk-continue") : filtered;
    },
    streamFn: (model, ctx) => {
      // Capture outside-provider contexts; no serialization or marker searches inside the timer.
      captures.push({ model: model.id, messages: ctx.messages.slice() });
      const stream = createAssistantMessageEventStream();
      const completed = assistant(executor, [{ type: "text", text: "Synthetic completion." }]);
      stream.push({ type: "start", partial: completed });
      stream.push({ type: "done", reason: "stop", message: completed });
      return stream;
    },
  });
  const ctx = { cwd: root, model: frontier, sessionManager: source,
    modelRegistry: { find: (provider, id) => [frontier, executor].find(m => m.provider === provider && m.id === id) },
    ui: { notify() {}, setStatus() {} } };
  const selections = [], deliveries = [];
  // Real host delivery against a real agent: passive sends queue while the run
  // is active and land after the boundary turn's tool results.
  const hostSession = createPassiveHostSession(agent, source);
  const extension = {
    setModel: async model => { selections.push(model.id); agent.state.model = model; ctx.model = model; return true; },
    sendMessage: (message, options) => { deliveries.push(hostSession.sendCustomMessage(message, options)); },
  };
  const boundary = await timed(async () => {
    if (c.prewalk) {
      const pending = s.claimFabricHandoff(controller, { audits: [{ ref: "pi.edit", success: true, nestedToolCallId: "edit-1", startedAt: 1, endedAt: 2, args: { path: "synthetic.ts" } }] }, sid, "auto");
      assert.equal(pending?.kind, "prewalk-in-place");
      await s.runFabricHandoffAtBoundary(controller, { executeHandoff() { throw new Error("Unexpected child spawn"); } }, extension, pending, outer, ctx);
    } else {
      await extension.setModel(executor);
      // Same passive delivery as the prewalk column: the control continuation
      // rides the boundary context instead of queueing a wake-up turn.
      extension.sendMessage({ customType: "bench-control", content: "Continue the existing task.", display: false }, { triggerTurn: false });
    }
    await Promise.all(deliveries);
  });
  assert.equal(agent.hasQueuedMessages(), false, "passive delivery must not queue a wake-up turn");
  for (let i = 0; i < c.steers; i++) agent.steer({ role: "user", content: `BENCH-STEER-${i}: external graph update.`, timestamp: 4 });
  const continuation = await timed(() => agent.continue());
  const requests = captures.map(r => {
    const text = textOf(r.messages);
    // Exactly one canonical continuation per provider-bound request: injected
    // into the first request, carried by the transcript after the turn flush.
    const continuationCount = text.split(renderedPlan).length - 1;
    return { model: r.model, bytes: bytes(r.messages), continuationCount, plan: continuationCount > 0, digest: text.includes("BENCH-DIGEST"),
      task: text.includes("BENCH-TASK"), outer: text.includes("BENCH-OUTER-RESULT"), stale: text.includes("STALE-BENCH-CONTINUATION"),
      arm: text.includes("this session owes a recorded plan before handoff"), checkpoint: text.includes("Record the plan now with prewalk.plan"),
      steers: Array.from({ length: c.steers }, (_, i) => text.includes(`BENCH-STEER-${i}`)).filter(Boolean).length };
  });
  // Passive delivery rides the boundary context, so the executor's work is the
  // same request the frontier would send, plus one per legitimate steering turn
  // — no queued wake-up turn of its own (witnessed by the in-place queue test in
  // tests/prewalk-handoff.test.ts: three steers, three requests).
  assert.equal(requests.length, Math.max(1, c.steers), "passive continuation request count");
  assert.ok(requests.every(r => r.model === executor.id && r.task && r.outer && !r.stale), "trajectory/model/stale invariant");
  assert.equal(requests.at(-1).steers, c.steers, "steering content preserved");
  if (c.prewalk) {
    assert.ok(requests.some(r => r.plan), "eventual plan missing");
    assert.ok(requests.every(r => r.continuationCount === 1), "canonical continuation exactly once per request");
    assert.ok(requests.some(r => r.digest), "eventual digest missing");
    if (c.steers === 0) assert.ok(requests[0].plan && requests[0].digest, "uncontended first-request contract");
  }
  const returning = await timed(async () => {
    if (c.prewalk) assert.equal(await s.settleInPlacePrewalk(controller, extension, ctx, { compactOnReturn: false }), true);
    else await extension.setModel(frontier);
  });
  assert.deepEqual(selections, [executor.id, frontier.id]);
  assert.equal(agent.state.model.id, frontier.id);
  if (c.prewalk) {
    assert.equal(controller.status().state, "armed");
    assert.equal(await s.settleInPlacePrewalk(controller, extension, ctx), false);
    assert.deepEqual(selections, [executor.id, frontier.id], "return happens once");
    const filtered = s.filterPrewalkContinuationMessages(agent.state.messages, id => controller.acceptContinuation(sid, id));
    assert.equal(filtered.messages.some(m => m.customType === "pi-fabric-prewalk-continue"), false, "settled continuation removed");
  }
  return { ...c, boundaryMs: boundary.wallMs, boundaryCpuMs: boundary.cpuMs,
    continueMs: continuation.wallMs, continueCpuMs: continuation.cpuMs, returnMs: returning.wallMs, returnCpuMs: returning.cpuMs,
    totalMs: boundary.wallMs + continuation.wallMs + returning.wallMs,
    planJsonChars: JSON.stringify(plan).length, planTextBytes: bytes(renderedPlan), requests,
    totalContextBytes: requests.reduce((n, r) => n + r.bytes, 0) };
}

function bump(file) {
  const stat = fs.statSync(file);
  fs.utimesSync(file, stat.atime, new Date(stat.mtimeMs + 1000));
}
async function driftTrials(s, settings) {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prewalk-drift-"));
  const samples = [], checks = [];
  try {
    for (const git of [true, false]) for (const count of settings.sizes) {
      const cwd = path.join(scratch, `${git ? "git" : "walk"}-${count}`);
      fs.mkdirSync(cwd);
      if (git) execFileSync("git", ["-C", cwd, "-c", "init.templateDir=", "init", "-q"], { stdio: "pipe" });
      for (let i = 0; i < count; i++) fs.writeFileSync(path.join(cwd, `f${String(i).padStart(5, "0")}.txt`), "initial ".repeat(8));
      const file = path.join(cwd, "f00000.txt");
      for (let iteration = -settings.warmup; iteration < settings.samples; iteration++) {
        const tracker = new s.PrewalkDriftTracker();
        const measure = async (name, fn) => {
          const result = await timed(fn);
          if (iteration >= 0) samples.push({ git, count, iteration, name, workload: git ? "retained-git-untracked" : "retained-walk",
            wallMs: result.wallMs, cpuMs: result.cpuMs, reported: result.value ? { ...result.value } : null });
          return result.value;
        };
        await measure("baseline", () => tracker.captureBaseline("bench", cwd));
        assert.equal(await measure("clean", () => tracker.evaluate("bench", cwd)), undefined);
        fs.writeFileSync(file, `changed ${iteration}: ` + "value ".repeat(10));
        const changed = await measure("changed-one", () => tracker.evaluate("bench", cwd));
        assert.equal(changed?.modified, 1); assert.deepEqual(changed.files, ["f00000.txt"]);
        bump(file);
        assert.equal(await measure("consecutive-touch", () => tracker.evaluate("bench", cwd)), undefined, "learned hash suppresses consecutive touch");
        assert.equal(await measure("idle-gap", () => tracker.evaluate("bench", cwd)), undefined);
        bump(file);
        const afterIdle = await measure("touch-after-idle", () => tracker.evaluate("bench", cwd));
        if (afterIdle) { assert.equal(afterIdle.modified, 1); assert.deepEqual(afterIdle.files, ["f00000.txt"]); }
        if (iteration >= 0) checks.push({ git, count, iteration, contentUnchanged: true, falseClaimAfterIdle: Boolean(afterIdle) });
        tracker.clear();
      }
    }
  } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
  return { samples, checks };
}

async function worker(subjects, settings, workerId) {
  const s = await import(pathToFileURL(subjects).href);
  // The subjects bundle lists each helper's owning module explicitly; a stale
  // module path yields `undefined` and the failure would surface as a swallowed
  // transformContext error far from its cause, so fail loudly here instead.
  for (const name of ["PrewalkController", "PrewalkDriftTracker", "checkedPrewalkPlan", "prewalkPlanText", "claimFabricHandoff", "runFabricHandoffAtBoundary", "filterPrewalkContinuationMessages", "prewalkArmedPrompt", "prewalkPlanPrompt", "settleInPlacePrewalk"])
    assert.equal(typeof s[name], "function", `subjects bundle is missing ${name}: update the bundle export list to that helper's current module`);
  const cells = [];
  for (const followUpMode of ["one-at-a-time", "all"]) for (const steers of [0, 1, 3])
    for (const history of [0, 100]) for (const profile of ["compact", "near-cap"])
      for (const prewalk of [false, true]) cells.push({ followUpMode, steers, history, profile, prewalk });
  const queue = [];
  for (let iteration = -settings.warmup; iteration < settings.samples; iteration++) {
    const offset = ((workerId + iteration) % cells.length + cells.length) % cells.length;
    const ordered = [...cells.slice(offset), ...cells.slice(0, offset)];
    if (iteration % 2 !== 0) ordered.reverse();
    for (const c of ordered) {
      const row = await queueTrial(s, c);
      if (iteration >= 0) queue.push({ workerId, iteration, ...row });
    }
  }
  // Canary: deliberately drop the current continuation; the eventual-plan invariant MUST reject it.
  await assert.rejects(() => queueTrial(s, { followUpMode: "one-at-a-time", steers: 0, history: 0, profile: "compact", prewalk: true }, true), /eventual plan missing/);
  const drift = await driftTrials(s, settings);
  return { workerId, pid: process.pid, queueCells: cells.length, missingPlanCanaryCaught: true, queue, drift };
}

function summarize(workers) {
  const queue = workers.flatMap(w => w.queue), drift = workers.flatMap(w => w.drift.samples);
  const queued = queue.filter(r => r.prewalk && r.steers > 0);
  const unqueued = queue.filter(r => r.prewalk && r.steers === 0);
  const churn = workers.flatMap(w => w.drift.checks);
  const queueRows = groupRows(queue, r => [r.prewalk ? "prewalk" : "control", r.followUpMode, r.steers, r.history, r.profile].join("/"), rows => ({
    config: Object.fromEntries(["prewalk", "followUpMode", "steers", "history", "profile"].map(k => [k, rows[0][k]])),
    samples: rows.length,
    ...Object.fromEntries(["boundaryMs", "boundaryCpuMs", "continueMs", "continueCpuMs", "returnMs", "returnCpuMs", "totalMs", "totalContextBytes"].map(k => [k, stats(rows.map(r => r[k]))])),
    requestCount: stats(rows.map(r => r.requests.length)), firstRequestPlanMisses: rows.filter(r => r.prewalk && !r.requests[0].plan).length,
    planJsonChars: rows[0].planJsonChars, planTextBytes: rows[0].planTextBytes,
    perWorkerTotalMs: groupRows(rows, r => r.workerId, values => stats(values.map(r => r.totalMs))),
  }));
  const findings = [];
  // Post-LQ1-fix contract: the canonical pending continuation is injected into
  // the first executor request, so contended first-request misses must stay 0.
  // A regression re-fires the LQ1 finding rather than failing the harness.
  const lq1 = {
    contendedTrials: queued.length,
    contendedFirstRequestMisses: queued.filter(r => !r.requests[0].plan || !r.requests[0].digest).length,
    noSteerTrials: unqueued.length,
    noSteerFirstRequestMisses: unqueued.filter(r => !r.requests[0].plan || !r.requests[0].digest).length,
  };
  if (lq1.contendedFirstRequestMisses > 0) findings.push({ id: "LQ1", severity: "high", scope: "first-request pending-context contract (LQ1) regression",
    description: "Contended first executor requests lack the plan/digest despite pending-context injection.", affected: lq1.contendedFirstRequestMisses,
    trials: lq1.contendedTrials, noSteerMisses: lq1.noSteerFirstRequestMisses, noSteerTrials: lq1.noSteerTrials });
  // Post-B2-fix contract: an intervening unchanged scan carries the learned
  // hash, so a later content-identical touch must not claim again.
  const falseClaims = churn.filter(c => c.falseClaimAfterIdle).length;
  if (falseClaims) findings.push({ id: "B2", severity: "medium", scope: "drift-tracker hash-carry regression",
    description: "An intervening unchanged scan loses the learned hash; a later content-identical touch is reported as another mutation.",
    affected: falseClaims, trials: churn.length, consecutiveTouchFalseClaims: 0 });
  return { queueTrials: queue.length, driftMeasurements: drift.length, queue: queueRows,
    drift: groupRows(drift, r => [r.git ? "git" : "walk", r.count, r.name].join("/"), rows => ({ samples: rows.length,
      wallMs: stats(rows.map(r => r.wallMs)), cpuMs: stats(rows.map(r => r.cpuMs)), claims: rows.filter(r => r.reported !== null).length })),
    lq1, b2Verified: churn.length > 0 && falseClaims === 0, findings };
}

const argv = process.argv.slice(2);
if (argv[0] === "--worker") {
  const result = await worker(argv[1], JSON.parse(argv[2]), Number(argv[3]));
  process.stdout.write(JSON.stringify(result));
} else {
  let out, quick = false;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--quick") quick = true;
    else if (argv[i] === "--out" && argv[i + 1]) out = path.resolve(argv[++i]);
    else throw new Error(`Unknown or incomplete argument: ${argv[i]}`);
  }
  assert.ok(out, "Specify --out /path/to/results.json (raw results must be retained)");
  assert.equal(fs.existsSync(out), false, "Refusing to overwrite prior benchmark evidence");
  const settings = { processes: quick ? 1 : 5, warmup: quick ? 1 : 3, samples: quick ? 2 : 20, sizes: quick ? [100, 1000] : [100, 1000, 10000] };
  const startedAt = new Date().toISOString(), start = performance.now();
  const temp = fs.mkdtempSync(path.join(root, ".benchmark-prewalk-"));
  try {
    const subjects = path.join(temp, "subjects.mjs");
    const bundle = await build({ stdin: { resolveDir: root, loader: "ts", contents: [
      "export { PrewalkController } from './src/prewalk/controller.ts';",
      "export { PrewalkDriftTracker } from './src/prewalk/fs-drift.ts';",
      "export { checkedPrewalkPlan, prewalkPlanText } from './src/prewalk/plan.ts';",
      "export { claimFabricHandoff, runFabricHandoffAtBoundary } from './src/prewalk/handoff.ts';",
      "export { filterPrewalkContinuationMessages, prewalkArmedPrompt, prewalkPlanPrompt } from './src/prewalk/messages.ts';",
      "export { settleInPlacePrewalk } from './src/prewalk/return.ts';",
    ].join("\n") }, outfile: subjects, bundle: true, packages: "external", platform: "node", format: "esm", metafile: true, logLevel: "silent" });
    const sourceHashes = Object.fromEntries(Object.keys(bundle.metafile.inputs).filter(p => p !== "<stdin>").sort().map(p => [p, hash(fs.readFileSync(path.resolve(root, p)))]));
    const workers = [];
    for (let i = 0; i < settings.processes; i++) {
      const child = spawnSync(process.execPath, [self, "--worker", subjects, JSON.stringify(settings), String(i)], {
        cwd: root, encoding: "utf8", timeout: 120_000, maxBuffer: 64 << 20, env: { ...process.env, PI_OFFLINE: "1" },
      });
      assert.equal(child.status, 0, `worker ${i} failed: ${child.error ?? child.stderr}`);
      workers.push(JSON.parse(child.stdout));
      process.stderr.write(`worker ${i + 1}/${settings.processes}: ${workers.at(-1).queue.length} queue trials complete\n`);
    }
    for (const [p, expected] of Object.entries(sourceHashes)) assert.equal(hash(fs.readFileSync(path.resolve(root, p))), expected, `Source changed during measurement: ${p}`);
    const summary = summarize(workers);
    const result = { schemaVersion: 1, startedAt, finishedAt: new Date().toISOString(), elapsedMs: performance.now() - start,
      status: summary.findings.length ? "completed_with_findings" : "completed", scope: "local orchestration; synthetic zero-token streams; NO quality/cost/real-model latency claims",
      settings, environment: { node: process.version, platform: process.platform, arch: process.arch, cpus: os.cpus().length,
        cpu: os.cpus()[0]?.model, memoryBytes: os.totalmem(), loadavg: os.loadavg(), git: execFileSync("git", ["--version"], { encoding: "utf8" }).trim(),
        hostPackages: Object.fromEntries(["pi-agent-core", "pi-coding-agent", "pi-ai"].map(name => [name, JSON.parse(fs.readFileSync(path.join(root, "node_modules/@earendil-works", name, "package.json"), "utf8")).version])) },
      provenance: { head: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
        sourceHashes, sourceFingerprint: hash(JSON.stringify(sourceHashes)), runnerSha256: hash(fs.readFileSync(self)), bundleSha256: hash(fs.readFileSync(subjects)) },
      method: { percentile: "nearest rank; pooled and per-worker medians", importsTimed: false, fixtureConstructionTimed: false,
        syntheticCaptureTimed: true, contextSerializationTimed: false, compactionEnabled: false, steeringMode: "one-at-a-time",
        limitations: ["Local model callbacks bypass authentication and provider I/O", "Streaming branch of real AgentSession delivery method on a minimal host, not full interactive-session lifecycle",
          "Initial synthetic transcript includes a completed boundary and no prior plan tool-result copy", "Byte counts serialize Pi model-input message objects, not provider wire bytes or tokens",
          "Baseline is a plain executor continuation, NOT frontier-only task execution", "Filesystem and runtime caches are warm; workers are fresh and measurements sequential"] },
      checks: { missingPlanCanaryCaught: workers.every(w => w.missingPlanCanaryCaught), sourceUnchanged: true,
        lq1FirstRequestContract: summary.lq1.contendedFirstRequestMisses === 0 && summary.lq1.noSteerFirstRequestMisses === 0,
        b2CarryHashVerified: summary.b2Verified }, summary, workers };
    fs.writeFileSync(out, JSON.stringify(result, null, 2) + "\n", { flag: "wx" });
    console.log(JSON.stringify({ out, status: result.status, queueTrials: summary.queueTrials, driftMeasurements: summary.driftMeasurements,
      lq1: summary.lq1, b2Verified: summary.b2Verified, findings: summary.findings,
      sourceFingerprint: result.provenance.sourceFingerprint, elapsedMs: result.elapsedMs }, null, 2));
    process.exitCode = summary.findings.length ? 2 : 0;
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
}
