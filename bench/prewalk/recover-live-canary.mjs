#!/usr/bin/env node
// Recover the frozen live-canary archive with the maintained evidence parser
// (bench/prewalk/lib/prewalk-live-evidence.mjs). Verifies every archived byte against
// checksums.sha256, validates the frozen manifest, result routes and
// recomputed usage, rejects incomplete evidence, and never re-runs a
// completed paid cell. Only derived outputs are written; the frozen archive
// stays byte-identical.
//
// usage: node bench/prewalk/recover-live-canary.mjs
//          [--archive <dir>] [--out <dir>]
//          [--sessions <dir>]       # optional: <dir>/<cell>/sessions/*.jsonl persisted entries
//          [--arrival-root <dir>]   # optional: <dir>/<cell>/arrival.jsonl stdout arrival log

import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { sha256, writeExclusive } from "./lib/prewalk-bench-lib.mjs";
import {
  analyzeCell,
  arrivalCompactionBounds,
  assertCompleteRecording,
  parseJsonLines,
  usageMatches,
} from "./lib/prewalk-live-evidence.mjs";

const argv = process.argv.slice(2);
const value = (flag, fallback) => {
  const index = argv.indexOf(flag);
  return index >= 0 ? argv[index + 1] : fallback;
};
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const archive = path.resolve(repoRoot, value("--archive", "docs/benchmarks/prewalk/2026-09-19/live-canary-KOtB4l"));
const out = path.resolve(repoRoot, value("--out", `${archive}-recovery`));
const sessionsRoot = value("--sessions");
const arrivalRoot = value("--arrival-root");
const cells = ["off1", "on1", "on2", "off2"];

// 1. The frozen archive must verify byte-for-byte before anything is read.
const checksumLines = fs
  .readFileSync(path.join(archive, "checksums.sha256"), "utf8")
  .trim()
  .split("\n")
  .filter(Boolean);
for (const line of checksumLines) {
  const hash = line.slice(0, 64);
  const rel = line.slice(66).trim();
  const actual = createHash("sha256").update(fs.readFileSync(path.join(archive, rel))).digest("hex");
  if (actual !== hash) throw new Error(`Archive checksum mismatch: ${rel}`);
}
const manifest = JSON.parse(fs.readFileSync(path.join(archive, "manifest.json"), "utf8"));
const mainModel = manifest?.main?.model;
const executorModel = manifest?.prewalk?.model;
if (typeof mainModel !== "string" || typeof executorModel !== "string" || typeof manifest?.head !== "string") {
  throw new Error("Archive manifest.json is missing head/main/prewalk model identity");
}

// 2. Parse, validate completeness, and re-derive every cell from the archive.
const consumedHashes = {};
const recordConsumed = (cell, file) => {
  consumedHashes[cell] ??= {};
  consumedHashes[cell][file] = sha256(fs.readFileSync(file));
};
const readGzJsonl = (cell, file) => {
  recordConsumed(cell, file);
  return parseJsonLines(zlib.gunzipSync(fs.readFileSync(file)).toString("utf8"), file);
};
const sessionEntries = (cell) => {
  if (!sessionsRoot) return [];
  const dir = path.join(sessionsRoot, cell, "sessions");
  if (!fs.existsSync(dir)) return [];
  const entries = [];
  for (const name of fs.readdirSync(dir).filter((candidate) => candidate.endsWith(".jsonl"))) {
    const file = path.join(dir, name);
    recordConsumed(cell, file);
    entries.push(...parseJsonLines(fs.readFileSync(file, "utf8"), file));
  }
  return entries;
};
const arrivalFor = (cell, liveEvents) => {
  if (!arrivalRoot) return null;
  const file = path.join(arrivalRoot, cell, "arrival.jsonl");
  if (!fs.existsSync(file)) return null;
  recordConsumed(cell, file);
  return arrivalCompactionBounds(liveEvents, parseJsonLines(fs.readFileSync(file, "utf8"), file));
};

const analyses = [];
for (const cell of cells) {
  const resultFile = path.join(archive, cell, "result.json");
  const result = JSON.parse(fs.readFileSync(resultFile, "utf8"));
  recordConsumed(cell, resultFile);
  const liveEvents = readGzJsonl(cell, path.join(archive, cell, "events.jsonl.gz"));
  const telemetryEvents = readGzJsonl(cell, path.join(archive, cell, "telemetry.jsonl.gz"));
  assertCompleteRecording(liveEvents, telemetryEvents);
  const isOn = cell.startsWith("on");

  if (result.quality?.success !== true) throw new Error(`${cell}: frozen quality is not success`);
  if ((result.changedProtected ?? []).length > 0) {
    throw new Error(`${cell}: protected files changed in the frozen run`);
  }
  if (result.finalModel !== mainModel) throw new Error(`${cell}: final model is not the Main model`);
  const selects = result.modelChanges ?? [];
  if (isOn) {
    const [toExecutor, toMain] = selects;
    if (
      selects.length !== 2 || !toExecutor || !toMain ||
      toExecutor.model !== executorModel || toExecutor.previous !== mainModel ||
      toMain.model !== mainModel || toMain.previous !== executorModel
    ) {
      throw new Error(`${cell}: frozen model route is not one Main/executor roundtrip`);
    }
  } else if (selects.length !== 0) {
    throw new Error(`${cell}: Prewalk OFF cell recorded model changes`);
  }

  const analysis = analyzeCell({
    name: cell,
    liveEvents,
    persistedEntries: sessionEntries(cell),
    telemetryEvents,
    compactionArrival: arrivalFor(cell, liveEvents),
  });
  const usage = usageMatches(analysis.perModel, result.perModel);
  if (!usage.ok) {
    throw new Error(`Usage mismatch for ${cell}: ${JSON.stringify(usage.mismatches.slice(0, 3))}`);
  }
  if (analysis.assistantCount !== result.assistantTurns) {
    throw new Error(`${cell}: recomputed assistant count ${analysis.assistantCount} != frozen ${result.assistantTurns}`);
  }
  if (analysis.modelSelects.length !== selects.length) {
    throw new Error(`${cell}: telemetry model_select count does not match the frozen route`);
  }
  analyses.push({ cell, isOn, analysis, wallMs: result.wallMs, quality: result.quality });
}

// 3. Derived outputs only. The frozen archive is never rewritten.
const seconds = (ms) => (ms === null || ms === undefined ? "null" : `${(ms / 1000).toFixed(1)} s`);
const onPre = analyses.filter((a) => a.isOn).map((a) => a.analysis.phases.preHandoffMainMs);
const offSpan = analyses.filter((a) => !a.isOn).map((a) => a.analysis.phases.assistantSpanMs);
const execIntervals = analyses.filter((a) => a.isOn).map((a) => a.analysis.phases.executorIntervalMs);
const returns = analyses.filter((a) => a.isOn).map((a) => a.analysis.phases.returnMs);

function reportText() {
  const messageCell = (analysis) =>
    analysis.prewalkMessages
      .map((message) => {
        const details = message.details ?? {};
        const continuationId = typeof details.continuationId === "string" ? ` (${details.continuationId})` : "";
        return `${message.customType}${continuationId} [${message.sources.join("+")}]`;
      })
      .join("; ") || "none";
  const rows = analyses
    .map(({ cell, isOn, analysis, wallMs, quality }) =>
      `| ${cell} | ${isOn ? "ON" : "OFF"} | ${(wallMs / 1000).toFixed(1)} s | ${seconds(analysis.phases.assistantSpanMs)} | ${seconds(analysis.phases.preHandoffMainMs)} | ${seconds(analysis.phases.executorIntervalMs)} | ${seconds(analysis.phases.executorAssistantSpanMs)} | ${seconds(analysis.phases.returnMs)} | ${
        analysis.phases.compaction
          ? `${analysis.phases.compaction.outcome} (${analysis.phases.compaction.attemptMs === null ? "attemptMs null" : seconds(analysis.phases.compaction.attemptMs)})`
          : "none"
      } | ${messageCell(analysis)} | ${quality.passed}/${quality.total} |`)
    .join("\n");
  return `# Live Prewalk canary recovery (2026-09-19)

Recovered analysis of the frozen four-cell canary at
${path.relative(repoRoot, archive)} using the maintained evidence parser
(bench/prewalk/lib/prewalk-live-evidence.mjs). Nothing was re-run: all
${checksumLines.length} archived files verified byte-for-byte against
checksums.sha256, every cell's recording passed completeness validation
(session identity, terminal agent_settled, telemetry shutdown, no turn-limit,
no failed assistant stop reasons), and usage recomputed from the live event
streams matched the frozen result.json perModel exactly. The frozen archive
stays byte-identical; only these derived outputs are written. Consumed input
hashes are recorded in recovery.json.

## Corrections to the original report

1. **Recorder gap, not missing messages.** The original finding that the
   telemetry extension "did not observe pi-fabric-prewalk-* custom entries"
   was a recorder shape bug: live custom messages appear in events.jsonl as
   message_end records with role "custom" and a message-level customType,
   while persisted session entries are type "custom_message" with a top-level
   customType. The canary recorder read session entries as message-shaped and
   always found none. Both ON cells carry the armed and continue messages,
   including the continuation IDs in the table below. The maintained recorder
   (bench/prewalk/prewalk-canary-telemetry.ts) projects both shapes through the
   shared normalizer, and the durable entry shape is covered by a
   deterministic SessionManager roundtrip regression.
2. **End-to-end wall difference, not measured handoff overhead.** The
   original "16-33 s of handoff overhead" framing overstated attribution, and
   the recovered spans must not be compared like-for-like: ON pre-handoff Main
   spans (first assistant message start to handoff model_select) were
   ${onPre.map(seconds).join(" / ")} while OFF assistant spans (first to last
   assistant message start) were ${offSpan.map(seconds).join(" / ")} on the
   identical task — ON's Main phase alone exceeded the OFF totals before the
   executor was involved, because ON turns include plan-checkpoint and
   handoff work that OFF lacks. Executor intervals (handoff model_select to
   return model_select) added ${execIntervals.map(seconds).join(" / ")} and
   returns ${returns.map(seconds).join(" / ")}, but provider latency and
   differing turn counts dominate. This evidence supports no causal
   decomposition of the arm gap, and none is claimed.
3. **Too-small compaction.** The compactOnReturn attempt stands: Pi exposes
   no eligibility check and the host still emits a raw session_compact_failed
   event (retained in the table and recovery.json) even though Fabric now
   classifies the exact "Nothing to compact (session too small)" callback as a
   benign cancelled outcome (src/core/compact-controller.ts,
   docs/programmatic-compaction.md). The attempt's duration is unknown:
   compaction_start precedes session_before_compact and the frozen archive
   recorded no stdout arrival timestamps, so attemptMs stays null. Future
   runs capture paired arrival timestamps through
   bench/prewalk/prewalk-canary-run.mjs.

## Recovered cells

| Cell | Arm | Wall | Assistant span | Pre-handoff Main | Executor interval | Executor span | Return | Compaction | Prewalk messages | Quality |
|---|---|---|---|---|---|---|---|---|---|---|
${rows}

Span definitions, timestamp provenance, and the reasons every null stays
null (phases.missing) live in recovery.json. Assistant-span timings are
message start-to-start, not response durations.

## Provenance and limits

- Persisted-session projections from the retained scratch were confirmed in
   the initial recovery pass before that scratch was cleaned; this
   regeneration analyzes the archived live projections${sessionsRoot ? " plus the supplied session directory" : ""}.
- All limits of the frozen canary apply unchanged: one extracted task, two
   attempts per arm, cold sessions, uncontrolled provider cache, recorded
   cost estimates rather than billed invoices.
`;
}

fs.mkdirSync(out, { recursive: true });
writeExclusive(
  path.join(out, "recovery.json"),
  JSON.stringify(
    {
      schemaVersion: 1,
      generatedAt: new Date().toISOString(),
      archive,
      archiveHead: manifest.head,
      mainModel,
      executorModel,
      sessions: sessionsRoot ?? null,
      arrivalRoot: arrivalRoot ?? null,
      verifiedArchiveFiles: checksumLines.length,
      consumedHashes,
      cells: analyses,
    },
    null,
    2,
  ) + "\n",
);
writeExclusive(path.join(out, "report.md"), reportText());
const sums = [];
for (const name of ["recovery.json", "report.md"]) {
  sums.push(`${sha256(fs.readFileSync(path.join(out, name)))}  ${name}`);
}
writeExclusive(path.join(out, "checksums.sha256"), sums.sort().join("\n") + "\n");
console.log(
  JSON.stringify({
    out,
    verifiedArchiveFiles: checksumLines.length,
    cells: analyses.map(({ cell, analysis }) => ({
      cell,
      prewalkMessages: analysis.prewalkMessages.map((message) => message.customType),
      phases: {
        assistantSpanMs: analysis.phases.assistantSpanMs,
        preHandoffMainMs: analysis.phases.preHandoffMainMs,
        executorIntervalMs: analysis.phases.executorIntervalMs,
        executorAssistantSpanMs: analysis.phases.executorAssistantSpanMs,
        returnMs: analysis.phases.returnMs,
        compaction: analysis.phases.compaction,
      },
    })),
  }),
);
