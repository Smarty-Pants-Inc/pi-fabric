#!/usr/bin/env node
// node scripts/benchmark-host-lease-discovery.ts --reader sync|async|both --rounds 12
// Builds a private source probe, NEVER dist. All fixtures, replaced directories and raw
// samples are retained under --out (default: a fresh directory in the OS temp root).
// Setup/churn is outside timing. This is discovery cost, not a whole-session benchmark.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { pathToFileURL, fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { setTimeout as delay, setImmediate as turn } from "node:timers/promises";
import { build } from "esbuild";
import { spawnSync } from "node:child_process";

const args = process.argv.slice(2);
const option = (name, fallback) => {
  const index = args.indexOf(name);
  return index < 0 ? fallback : args[index + 1];
};
const entries = Number(option("--entries", "1229"));
const rounds = Number(option("--rounds", "12"));
const mode = option("--reader", "sync");
if (!Number.isSafeInteger(entries) || entries < 2 || !Number.isSafeInteger(rounds) || rounds < 2 ||
  !["sync", "async", "both"].includes(mode)) throw new Error("Invalid --entries, --rounds or --reader");
const out = option("--out", fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lease-discovery-bench-")));
fs.mkdirSync(out, { recursive: true });
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const source = path.join(repo, "src/topology/host-leases.ts");
const revision = option("--revision", "");
const comms = args.includes("--comms");
const periodMs = Number(option("--period-ms", "100"));
if (!Number.isFinite(periodMs) || periodMs < 1) throw new Error("Invalid --period-ms");
const sourceBytes = filename => {
  if (!revision) return fs.readFileSync(filename, "utf8");
  const result = spawnSync("git", ["show", `${revision}:${path.relative(repo, filename)}`], { cwd: repo, encoding: "utf8" });
  if (result.status !== 0) throw new Error(result.stderr);
  return result.stdout;
};
const bundle = path.join(out, "host-leases-probe.mjs");
// Resolve external packages from the existing repo install without installing/copying/deleting it.
if (!fs.existsSync(path.join(out, "node_modules"))) fs.symlinkSync(path.join(repo, "node_modules"), path.join(out, "node_modules"), "dir");
await build({ stdin: { contents: `export * from ${JSON.stringify(source)}; export { StoreBridgeSide, bridgeHandoffScope } from ${JSON.stringify(path.join(repo, "src/mesh/bridge.ts"))}; export { ParticipantDirectory } from ${JSON.stringify(path.join(repo, "src/topology/participant-directory.ts"))};`, resolveDir: repo, loader: "ts" },
  outfile: bundle, bundle: true, packages: "external", platform: "node", format: "esm", logLevel: "warning",
  plugins: [{ name: "pinned-source-and-scan-counter", setup(builder) {
    builder.onLoad({ filter: /[/\\]src[/\\].*\.ts$/ }, ({ path: filename }) => {
      if (!filename.startsWith(repo + path.sep)) return undefined;
      let contents = sourceBytes(filename);
      if (filename === source) {
        const anchor = "export const readHostLeases = (meshRoot: string, problems?: string[]): Map<string, FabricHostLease> => {";
        if (!contents.includes(anchor)) throw new Error("Full-scan counter anchor moved; update benchmark explicitly");
        contents = contents.replace(anchor, anchor + "\n globalThis.__leaseBenchFullScans?.();");
      }
      return { contents, loader: "ts" };
    });
  } }],
});
const api = await import(pathToFileURL(bundle).href);
const readers = mode === "both" ? ["sync", "async"] : [mode];
if (readers.includes("async") && typeof api.readHostLeasesAsync !== "function") throw new Error("readHostLeasesAsync is not implemented");
const sourceHash = createHash("sha256").update(sourceBytes(source)).digest("hex");
const hash = value => createHash("sha256").update(value).digest("hex").slice(0, 32) + ".json";
const at = Date.now();
const idAt = (index, generation) => index === entries - 1 ? `host:tail:${generation}` : `host:${index}`;
const lease = (index, generation) => {
  const id = idAt(index, generation);
  return { format: 1, id, rootId: `session:${id}`, identityId: `session:${id}`, updatedAt: at + generation,
    expiresAt: index % 7 === 0 ? at - 60_000 : at + 600_000 + generation,
    startedAt: at - 3_600_000, incarnationToken: `incarnation:${generation}:${index}`,
    writer: { pid: process.pid, host: os.hostname(), releaseSha: sourceHash, lockProtocol: 3, stateBackend: "file", startedAt: at - 3_600_000 } };
};
const populate = (directory, generation) => {
  fs.mkdirSync(directory, { recursive: true });
  for (let i = 0; i < entries; i++) fs.writeFileSync(path.join(directory, hash(idAt(i, generation))), JSON.stringify(lease(i, generation)));
};
const percentile = (values, fraction) => [...values].sort((a, b) => a - b)[Math.min(values.length - 1, Math.ceil(values.length * fraction) - 1)];
const summary = values => ({ p50: percentile(values, .5), p95: percentile(values, .95), max: Math.max(...values) });
const samples = [];
for (const reader of comms ? [] : readers) {
  const read = reader === "sync" ? api.readHostLeases : api.readHostLeasesAsync;
  for (const scenario of ["cold", "warm", "atomic-replacement", "directory-replacement"]) {
    const root = path.join(out, reader, scenario);
    let directory = path.join(root, "host-leases");
    populate(directory, 0);
    if (scenario !== "cold") await read(root);
    for (let round = 0; round < rounds; round++) {
      let generation = 0;
      if (scenario === "cold") {
        directory = path.join(root, `cold-${round}`, "host-leases");
        populate(directory, 0);
      } else if (scenario === "atomic-replacement") {
        const temporary = path.join(root, `renewal-${round}.json`);
        fs.writeFileSync(temporary, JSON.stringify(lease(0, round + 1)));
        fs.renameSync(temporary, path.join(directory, hash("host:0")));
      } else if (scenario === "directory-replacement") {
        generation = round + 1;
        const next = path.join(root, `next-${generation}`);
        populate(next, generation);
        fs.renameSync(directory, path.join(root, `retained-${generation - 1}`));
        fs.renameSync(next, directory);
      }
      // A timer armed before the real entry point measures whether communication timers
      // can run DURING discovery, not just the scan's elapsed time. Include a final turn
      // so even a fully synchronous stall appears in timerGapMs and sentinelMs.
      await delay(5);
      let last = performance.now(), ticks = 0;
      const gaps = [];
      let scanning = true, ticksDuringScan = 0;
      const timer = setInterval(() => {
        const now = performance.now(); gaps.push(now - last); last = now; ticks++;
        if (scanning) ticksDuringScan++;
      }, 1);
      const start = performance.now(), cpu = process.cpuUsage();
      const sentinel = delay(0).then(() => performance.now() - start);
      const found = await read(scenario === "cold" ? path.dirname(directory) : root);
      const wallMs = performance.now() - start, used = process.cpuUsage(cpu);
      scanning = false;
      const sentinelMs = await sentinel;
      await delay(2);
      clearInterval(timer);
      if (found.size !== entries || found.get("host:1")?.incarnationToken !== `incarnation:${generation}:1` ||
        !found.has(idAt(entries - 1, generation)) || (generation > 0 && found.has(idAt(entries - 1, generation - 1))) ||
        found.get("host:0")?.updatedAt !== at + (scenario === "atomic-replacement" ? round + 1 : generation) ||
        !(found.get("host:0")?.expiresAt < at)) throw new Error(`Completeness/freshness/expiry failure: ${reader}/${scenario}/${round}`);
      const sample = { reader, scenario, round, entries: found.size, wallMs, cpuMs: (used.user + used.system) / 1000,
        sentinelMs, timerGapMs: Math.max(0, ...gaps), ticks, ticksDuringScan };
      samples.push(sample);
      fs.appendFileSync(path.join(out, "samples.jsonl"), JSON.stringify(sample) + "\n");
      await turn();
    }
  }
}
const commsResults = [];
if (comms) {
  const key = (kind, id) => `topology/${kind}/${createHash("sha256").update(id).digest("hex")}`;
  const ids = ["session:native-00000000", "session:held-00000000", "session:relay-00000000"];
  for (const handoffPresent of [false, true]) {
    const root = path.join(out, handoffPresent ? "comms-handoff" : "comms-common");
    const directory = path.join(root, "host-leases");
    fs.mkdirSync(directory, { recursive: true });
    const frame = index => {
      const value = lease(index, 0), id = ids[index] ?? value.id;
      const { writer, ...withoutWriter } = value;
      return { ...withoutWriter, ...(index === 1 || index === 2 ? {} : { writer }), id,
        rootId: index === entries - 1 ? ids[1] : id, identityId: id, expiresAt: at + 600_000 };
    };
    const populateComms = directory => {
      fs.mkdirSync(directory, { recursive: true });
      for (let i = 0; i < entries; i++) fs.writeFileSync(path.join(directory, hash(frame(i).id)), JSON.stringify(frame(i)));
    };
    populateComms(directory);
    const rows = ids.map((id, index) => ({ key: key("hosts", id), version: 1, updatedAt: at,
      updatedBy: { id, name: id, kind: "main" }, value: { format: 1, id, rootId: id,
        identity: { id, name: id, kind: "main" }, startedAt: at - 3_600_000,
        incarnationToken: frame(index).incarnationToken, updatedAt: at, expiresAt: at + 500_000,
        ...(index === 0 ? {} : { remoteHost: index === 1 ? "far" : "other" }) } }));
    rows.push({ key: key("participants", ids[0]), version: 1, updatedAt: at, updatedBy: rows[0].updatedBy,
      value: { format: 1, id: ids[0], rootId: ids[0], ownerHostId: ids[0], ownerIdentityId: ids[0],
        ownerIncarnation: frame(0).incarnationToken, kind: "root", name: "native", status: "idle",
        runner: "pi", transport: "host", capabilities: [], controlProtocol: "v1", startedAt: at - 3_600_000, updatedAt: at } });
    if (handoffPresent) rows.push({ key: key("handoffs", ids[1]), version: 1, updatedAt: at,
      updatedBy: rows[0].updatedBy, value: { format: 1, id: ids[1], sessionId: ids[1].slice(8), link: "far", operator: "bench", hub: "bench", handedOffAt: at } });
    const view = { get: key => rows.find(row => row.key === key), listAll: prefix => rows.filter(row => row.key.startsWith(prefix)), version: key => rows.find(row => row.key === key)?.version ?? 0 };
    const replaceOne = suffix => {
      const temporary = path.join(root, `renew-${suffix}.tmp`);
      fs.writeFileSync(temporary, JSON.stringify({ ...frame(100), updatedAt: Date.now() }));
      fs.renameSync(temporary, path.join(directory, hash(frame(100).id)));
    };
    const store = { root, stateBackend: "file", listAll: view.listAll, listAllShared: view.listAll,
      get: view.get, list: (prefix, count) => view.listAll(prefix).slice(0, count), stateToken: () => view,
      stateRevision: () => "benchmark-revision", stateView: () => ({ revision: "benchmark-revision", view }),
      stateUnchangedSince: () => true, withStateWriteFence: operation => operation(), prepareLock: async () => {},
      writeBatch: async input => {
        // A recorded-call state adapter, NOT a durable transaction benchmark. Force an
        // unrelated replacement before prepare to exercise the real strict authority recheck.
        replaceOne(`prepare-${Date.now()}`);
        const ops = input.prepare?.(view) ?? input.ops;
        if (ops.length) throw new Error("No-op mirror unexpectedly requested mutations");
        input.beforeCommit?.(); input.commitOutbox?.({ view, results: [] }); return [];
      } };
    const side = new api.StoreBridgeSide(store, "far", () => at);
    const participants = new api.ParticipantDirectory(store, { enabled: true, hostId: "benchmark-self",
      rootId: "session:benchmark-self-00000000", identity: { id: "session:benchmark-self-00000000", name: "bench", kind: "main" } });
    const counters = {}, perCall = {};
    let current = "idle", syncScans = 0;
    globalThis.__leaseBenchFullScans = () => {
      syncScans++; counters[current] = (counters[current] ?? 0) + 1;
    };
    const operations = [
      ["participantList", () => {
        if (!participants.list({ scope: "project" }, at).some(peer => peer.id === ids[0])) throw new Error("Known-host liveness lost");
      }],
      ["presence", () => side.presence()],
      ["relayOrigin", () => side.relayOrigin(ids[2])],
      ["relayStaleOrigin", () => side.relayStaleOrigin(ids[2])],
      ["relayPresence", () => side.relayPresence()],
      ["owned", () => side.owned()],
      ["held", () => { if (side.holds(ids[1]) === handoffPresent) throw new Error("Arbitrary-id native alias lost handoff authority"); }],
      ["subscriptionSnapshot", () => { const stop = side.subscribeChanges(() => {}, error => { throw error; }); stop(); }],
      // No-op, revision-capable adapter isolates real mirror authority logic without
      // executing state/file deletion. Handoff case is exercised by held/snapshot above.
      ...(!handoffPresent ? [["mirrorChurnAuthority", () => new api.StoreBridgeSide(store, "idle", () => at).mirror({ hosts: [], participants: [], reserved: [] })]] : []),
    ];
    const gaps = [], wall = [], cpu = [], sentinel = [];
    let previous = performance.now();
    const timer = setInterval(() => { const now = performance.now(); gaps.push(now - previous); previous = now; }, 1);
    const start = performance.now();
    for (let round = 0; round < rounds; round++) {
      await delay(Math.max(0, start + round * periodMs - performance.now()));
      // Alternate per-file atomic churn and whole-directory replacement. Every retired
      // directory remains on disk. The same schedule/source instrumentation runs at each pin.
      if (round % 2 === 0) replaceOne(`round-${round}`);
      else {
        const next = path.join(root, `next-${round}`); populateComms(next);
        fs.renameSync(directory, path.join(root, `retained-${round}`)); fs.renameSync(next, directory);
      }
      const cycle = performance.now(), used = process.cpuUsage();
      const cycleSentinel = delay(0).then(() => performance.now() - cycle);
      for (const [name, operation] of operations) {
        current = name; const begin = performance.now(); await operation();
        (perCall[name] ??= []).push(performance.now() - begin);
      }
      current = "idle";
      wall.push(performance.now() - cycle); const cost = process.cpuUsage(used); cpu.push((cost.user + cost.system) / 1000);
      sentinel.push(await cycleSentinel);
    }
    await delay(2); clearInterval(timer);
    const elapsedMs = performance.now() - start;
    const result = { handoffPresent, rounds, periodMs, nominalCyclesPerMinute: 60_000 / periodMs, elapsedMs,
      syncScans, syncScansPerObservedMinute: syncScans * 60_000 / elapsedMs,
      syncScansPerScheduledMinute: syncScans / rounds * 60_000 / periodMs,
      byEntrypoint: Object.fromEntries(Object.entries(perCall).map(([name, values]) => [name, {
        calls: values.length, syncScans: counters[name] ?? 0,
        syncScansPerScheduledMinute: (counters[name] ?? 0) / rounds * 60_000 / periodMs, wallMs: summary(values),
      }])), cycleWallMs: summary(wall), cycleCpuMs: summary(cpu), eventLoopSentinelMs: summary(sentinel), timerGapMs: summary(gaps),
      boundary: "Real StoreBridgeSide/ParticipantDirectory entrypoints and 1229 real lease files; in-memory revision/state adapter and recorded no-op writeBatch. Synthetic frequency, NOT live fleet telemetry. Timer gaps include fixture churn; per-call/cycle timing excludes setup churn." };
    commsResults.push(result);
    fs.writeFileSync(path.join(root, "schedule.json"), JSON.stringify({ result, gaps, wall, cpu, sentinel, perCall }, null, 2) + "\n");
    delete globalThis.__leaseBenchFullScans;
  }
}
const results = [];
for (const reader of comms ? [] : readers) for (const scenario of ["cold", "warm", "atomic-replacement", "directory-replacement"]) {
  const group = samples.filter(s => s.reader === reader && s.scenario === scenario);
  results.push({ reader, scenario, count: group.length, wallMs: summary(group.map(s => s.wallMs)),
    cpuMs: summary(group.map(s => s.cpuMs)), sentinelMs: summary(group.map(s => s.sentinelMs)),
    timerGapMs: summary(group.map(s => s.timerGapMs)), ticksDuringScan: group.reduce((sum, s) => sum + s.ticksDuringScan, 0) });
}
const report = { version: 1, at: new Date().toISOString(), runtime: process.version, platform: process.platform,
  source, sourceHash, bundleSha256: createHash("sha256").update(fs.readFileSync(bundle)).digest("hex"),
  scriptSha256: createHash("sha256").update(fs.readFileSync(fileURLToPath(import.meta.url))).digest("hex"),
  revision: revision || "working-tree", entries, rounds, out, commsResults, note: "Churn between scans; full namespace replacement retained. Setup/import excluded; no cleanup performed. Expired leases remain present.", results };
fs.writeFileSync(path.join(out, "report.json"), JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report, null, 2));
