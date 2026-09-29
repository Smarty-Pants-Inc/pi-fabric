import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { pathToFileURL } from "node:url";
import { build } from "esbuild";

// External packages resolve from this temporary directory under the repository.
const root = fs.mkdtempSync(path.resolve(".benchmark-turn-cpu-"));
try {
  const outfile = path.join(root, "subjects.mjs");
  await build({ stdin: { resolveDir: process.cwd(), loader: "ts", contents: `
export { BackgroundEntropyCompiler, compileEntropySurfaceAsync } from './src/entropy/compiler.ts';
export { SessionObservationCache, mergeObservationWindowAsync, poolToValueObservations } from './src/entropy/pool.ts';
export { sessionWindowEvidenceAsync } from './src/entropy/sessions.ts';
export { LiteralCallScanner } from './src/speculation/scanner.ts';
export { FabricSpeculationStreamTap } from './src/speculation/stream-tap.ts';
` }, outfile, bundle: true, platform: "node", format: "esm", packages: "external", logLevel: "silent" });
  const s = await import(pathToFileURL(outfile));
  const rows = [];
  async function measure(name, fn) {
    for (let i = 0; i < 2; i++) await fn();
    const samples = [];
    for (let i = 0; i < 5; i++) {
      const start = performance.now(); const cpu = process.cpuUsage();
      await fn();
      const used = process.cpuUsage(cpu);
      samples.push({ wallMs: performance.now() - start, cpuMs: (used.user + used.system) / 1000 });
    }
    const median = key => Math.round([...samples].sort((a,b) => a[key] - b[key])[2][key] * 100) / 100;
    const row = { name, wallMs: median("wallMs"), cpuMs: median("cpuMs") };
    rows.push(row); console.log(JSON.stringify(row));
  }
  const surface = { version: 1, actions: Array.from({ length: 150 }, (_,i) => ({ ref: `extensions.tool${i}`, inputSchema: {
    type: "object", properties: { path: { type: "string" }, mode: { type: "string", enum: ["fast", "safe"] }, limit: { type: "integer" } },
    required: ["path"], additionalProperties: false,
  } })) };
  for (const count of [1000, 10000]) {
    const makeTrace = i => ({ model: `provider/model${i % 2}`, taskKey: `task${Math.floor(i / 20)}`, operations:
      Array.from({ length: 8 }, (_,j) => ({ ref: `extensions.tool${j}`, args: { path: `file${i % 40}.ts`, mode: "safe", limit: 20 }, outcome: "succeeded" })),
    });
    let traces = Array.from({ length: count }, (_,i) => makeTrace(i));
    let observations = traces.flatMap(t => t.operations.flatMap(o => Object.entries(o.args).map(([key,value]) => ({ ref: o.ref, key, value }))));
    const compiler = new s.BackgroundEntropyCompiler();
    const cache = new s.SessionObservationCache();
    let pool = (await cache.merge(undefined, [{ file: "session", observations }])).file;
    const artifact = (await compiler.compile({ surface, windows: [{ file: "session", traces }] })).artifact;
    const uncached = () => s.compileEntropySurfaceAsync({ surface, traces, artifact, valueObservations: s.poolToValueObservations(pool) });
    const cached = () => compiler.compile({ surface, windows: [{ file: "session", traces }], artifact });
    assert.deepEqual((await cached()).report, (await uncached()).report);
    await measure(`entropy ${count * 8} ops: uncached`, uncached);
    await measure(`entropy ${count * 8} ops: cached`, cached);
    await measure(`observations ${observations.length}: uncached`, () => s.mergeObservationWindowAsync(pool, [{ file: "session", observations }]));
    await measure(`observations ${observations.length}: cached`, () => cache.merge(pool, [{ file: "session", observations }]));
    let next = count;
    await measure(`entropy + pool ${count * 8} ops: append one trace`, async () => {
      const added = makeTrace(next++);
      traces = [...traces, added];
      observations = [...observations, ...added.operations.flatMap(o => Object.entries(o.args).map(([key,value]) => ({ ref: o.ref, key, value })))];
      pool = (await cache.merge(pool, [{ file: "session", observations }])).file;
      await cached();
    });
  }
  const codeLines = Array.from({ length: 400 }, (_,i) => `await pi.read({path: 'file-${i}'});\n`);
  await measure("speculation 400 completed prefixes: ungated scanner", () => {
    const scanner = new s.LiteralCallScanner(); let prefix = "";
    for (const line of codeLines) { prefix += line; scanner.push(prefix); }
  });
  await measure("speculation 400 completed prefixes: bounded tap", () => {
    const original = Date.now; let now = 1000; let launches = 0;
    Date.now = () => now;
    try {
      const tap = new s.FabricSpeculationStreamTap({ enabled: () => true, maxBufferBytes: () => 100_000, isEligible: () => true, launch() { launches++; } });
      tap.setScannerFactory(() => new s.LiteralCallScanner());
      const event = (type, delta = "") => ({ assistantMessageEvent: { type, delta, contentIndex: 0,
        partial: { content: [{ type: "toolCall", name: "fabric_exec", id: "call" }] } } });
      tap.handleMessageUpdate(event("toolcall_start"), {});
      tap.handleMessageUpdate(event("toolcall_delta", '{"code":"'), {});
      for (const line of codeLines) { now += 51; tap.handleMessageUpdate(event("toolcall_delta", JSON.stringify(line).slice(1,-1)), {}); }
      tap.handleMessageUpdate({ assistantMessageEvent: { type: "toolcall_end", contentIndex: 0, toolCall: {} } }, {});
      assert.equal(launches, 400);
    } finally { Date.now = original; }
  });
  // The background compile reads only the active session, from a complete-line cursor.
  const sessionLine = JSON.stringify({ type: "message", message: { role: "toolResult", toolName: "fabric_exec",
    content: [{ type: "text", text: "ok" }], details: { success: true, trace: { kind: "pi-fabric.execution", version: 1,
      outcome: "succeeded", phases: [], operations: [{ type: "call", sequence: 0, ref: "pi.read", args: { path: "a.ts" }, outcome: "succeeded" }],
      counts: { droppedValues: 0, truncatedValues: 0, redactedValues: 0, droppedOperations: 0 } } } } }) + "\n";
  const sessionBody = sessionLine.repeat(5000);
  const copies = Array.from({ length: 7 }, (_, i) => {
    const file = path.join(root, `session-${i}.jsonl`); fs.writeFileSync(file, sessionBody); return file;
  });
  let copy = 0;
  await measure("own session 5000 lines: first read", () => s.sessionWindowEvidenceAsync([copies[copy++ % copies.length]], { windowsOnly: true }));
  const live = copies[0];
  await s.sessionWindowEvidenceAsync([live], { windowsOnly: true });
  await measure("own session 5000 lines: append one line, cursor read", async () => {
    fs.appendFileSync(live, sessionLine);
    const evidence = await s.sessionWindowEvidenceAsync([live], { windowsOnly: true });
    assert.ok(evidence.traceWindows[0].traces.length > 5000);
  });
  console.log(JSON.stringify({ runtime: process.version, arch: process.arch, synthetic: true, repeats: 5, rows }, null, 2));
} finally { fs.rmSync(root, { recursive: true, force: true }); }
