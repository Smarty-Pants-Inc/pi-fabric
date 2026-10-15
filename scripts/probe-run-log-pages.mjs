#!/usr/bin/env node
// Copied real run logs only: no product finalization, source retirement or deletion.
// Build first. Fresh child per page measures decoded-cache cold/warm, not OS page-cache eviction.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { constants, zstdCompressSync } from 'node:zlib';

const hash = value => createHash('sha256').update(value).digest('hex');
const pageValue = ({ generation: _generation, ...page }) => page;
const transcriptValue = ({ updatedAt: _updatedAt, ...transcript }) => transcript;
const nativeValue = ({ revision: _revision, updatedAt: _updatedAt, eventsFile: _eventsFile, sessionFile: _sessionFile, ...transcript }) => transcript;
const args = process.argv.slice(2);
if (args[0] === '--help' || args.length === 0) {
  console.log('Usage: node --expose-gc scripts/probe-run-log-pages.mjs SAMPLE_DIR NEW_OUTPUT_DIR [TRIALS=3]');
  process.exit(0);
}
const { readRunLogPage } = await import('../dist/log-tail.js');
const { readRunLogBuffer } = await import('../dist/run-log-source.js');
const { AgentTranscriptReader } = await import('../dist/ui/transcript-reader.js');
const { initializeConversationHost } = await import('../dist/ui/conversation-host.js');
initializeConversationHost(await import('@earendil-works/pi-coding-agent'));
const { NativeConversationReader } = await import('../dist/ui/conversation-native-reader.js');

if (args[0] === '--page') {
  const [, file, beforeText, oldGeneration] = args;
  const before = beforeText === 'latest' ? undefined : Number(beforeText);
  global.gc?.();
  const beforeMemory = process.memoryUsage();
  const start = performance.now();
  const cold = readRunLogPage(file, 200, before);
  const coldMs = performance.now() - start;
  global.gc?.();
  const retainedAfterCold = process.memoryUsage();
  const warmStart = performance.now();
  const warm = readRunLogPage(file, 200, before);
  const warmMs = performance.now() - warmStart;
  assert.deepEqual(warm, cold);
  assert.throws(() => readRunLogPage(file, 200, before ?? cold.before ?? 0, undefined, oldGeneration),
    error => error.name === 'cursor-stale');
  const source = { id: 'copied-sample', status: 'completed', logFile: file };
  const transcript = new AgentTranscriptReader().read(source);
  const native = new NativeConversationReader().read(source);
  assert(!transcript.error && !transcript.unavailable, 'dashboard transcript unavailable');
  assert(!native.error && !native.unavailable, 'native transcript unavailable');
  const decodedSha256 = hash(readRunLogBuffer(file));
  console.log(JSON.stringify({ coldMs, warmMs, pageSha256: hash(JSON.stringify(pageValue(cold))),
    transcriptSha256: hash(JSON.stringify(transcriptValue(transcript))),
    nativeSha256: hash(JSON.stringify(nativeValue(native))), decodedSha256,
    beforeMemory, retainedAfterCold, peakRssKiB: process.resourceUsage().maxRSS,
    decodedCacheRetainedArrayBufferDelta: retainedAfterCold.arrayBuffers - beforeMemory.arrayBuffers }));
  process.exit(0);
}

const [sampleInput, outputInput, trialsInput = '3'] = args;
assert(sampleInput && outputInput, 'SAMPLE_DIR and NEW_OUTPUT_DIR required');
const sample = fs.realpathSync(sampleInput);
const output = path.resolve(outputInput);
const trials = Number(trialsInput);
assert(Number.isInteger(trials) && trials >= 1 && trials <= 5, 'TRIALS must be 1..5');
assert(!fs.existsSync(output), 'Output must be a new caller-owned directory');
fs.mkdirSync(output, { mode: 0o700 });
const manifest = JSON.parse(fs.readFileSync(path.join(sample, 'manifest.json'), 'utf8'));
const picks = [...new Map(manifest.picks.map(pick => [pick.dest, pick])).values()];
const rows = [];
for (const pick of picks) {
  assert(/^runs__[0-9a-f]{32}$/.test(pick.dest), 'Unsafe manifest sample name');
  const plain = path.join(sample, pick.dest, 'events.jsonl');
  const original = fs.readFileSync(plain);
  assert.equal(original.length, pick.eventsBytes);
  const sourceSha256 = hash(original);
  const encoded = zstdCompressSync(original, { params: {
    [constants.ZSTD_c_compressionLevel]: 3, [constants.ZSTD_c_checksumFlag]: 1,
  } });
  const directory = path.join(output, pick.dest);
  fs.mkdirSync(directory, { mode: 0o700 });
  const logical = path.join(directory, 'events.jsonl');
  fs.writeFileSync(logical + '.zst', encoded, { flag: 'wx', mode: 0o600 });
  // Preserve canonical siblings as read-only inputs for result/recovery byte comparisons.
  const siblings = {};
  for (const name of ['status.json', 'reply.json', 'final-answer.json', 'settlement.json']) {
    const file = path.join(sample, pick.dest, name);
    if (fs.existsSync(file)) siblings[name] = hash(fs.readFileSync(file));
  }
  const source = { id: 'copied-sample', status: 'completed', logFile: plain };
  const expectedTranscript = hash(JSON.stringify(transcriptValue(new AgentTranscriptReader().read(source))));
  const expectedNative = hash(JSON.stringify(nativeValue(new NativeConversationReader().read(source))));
  const latest = readRunLogPage(plain, 200);
  const pages = [undefined, ...(latest.before === undefined ? [] : [latest.before])];
  const measurements = [];
  for (const before of pages) {
    const expectedPage = hash(JSON.stringify(pageValue(readRunLogPage(plain, 200, before))));
    for (let trial = 0; trial < trials; trial++) {
      const measured = JSON.parse(execFileSync(process.execPath, ['--expose-gc', fileURLToPath(import.meta.url),
        '--page', logical, before === undefined ? 'latest' : String(before), latest.generation],
      { encoding: 'utf8', timeout: 60_000, maxBuffer: 1024 * 1024 }));
      assert.equal(measured.pageSha256, expectedPage, 'raw page/cursor/order changed');
      assert.equal(measured.transcriptSha256, expectedTranscript, 'dashboard transcript/IDs/order changed');
      assert.equal(measured.nativeSha256, expectedNative, 'native replay/tools/messages changed');
      assert.equal(measured.decodedSha256, sourceSha256, 'decoded bytes changed');
      measurements.push({ page: before === undefined ? 'latest' : before, trial, ...measured });
    }
  }
  rows.push({ sample: pick.dest, originalBytes: original.length, compressedBytes: encoded.length,
    ratio: original.length / encoded.length, sourceSha256, siblings,
    cacheEligible: original.length <= 16 * 1024 * 1024, measurements });
}
const report = { passed: true, node: process.version, platform: process.platform, trials,
  coldDefinition: 'Fresh process and decoded LRU; OS page cache not evicted on shared host',
  warmDefinition: 'Same page, same child after explicit GC; files above 16 MiB are intentionally not cached',
  sourceRetirementExecuted: false, files: rows.length,
  originalBytes: rows.reduce((sum, row) => sum + row.originalBytes, 0),
  compressedBytes: rows.reduce((sum, row) => sum + row.compressedBytes, 0), rows };
fs.writeFileSync(path.join(output, 'report.json'), JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
console.log(JSON.stringify({ passed: true, files: report.files, originalBytes: report.originalBytes,
  compressedBytes: report.compressedBytes, report: path.join(output, 'report.json') }));
