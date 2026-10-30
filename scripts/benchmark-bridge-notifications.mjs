#!/usr/bin/env node
// smarty-dev#16879: real subscribeChanges + SQLite/fs.watch, synthetic data only, no timing gates.
// node scripts/benchmark-bridge-notifications.mjs --before-ref=<sha> --participants=512 --seconds=4 --out=<json>
// Run under smarty-reap + smarty-test-env + taskset. Historical bridge.ts and state-sqlite.ts are substituted; other dependencies are shared.
import assert from "node:assert/strict";
import { execFileSync, fork } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build } from "esbuild";

const file = fileURLToPath(import.meta.url), repo = path.resolve(path.dirname(file), "..");
const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const at = arg.indexOf("=");
  assert(arg.startsWith("--") && at > 2, `Expected --key=value: ${arg}`);
  return [arg.slice(2, at), arg.slice(at + 1)];
}));
for (const key of Object.keys(args)) assert(["before-ref", "participants", "seconds", "out", "writer", "bundle"].includes(key), `Unknown option ${key}`);
const N = Number(args.participants ?? 512), seconds = Number(args.seconds ?? 4);
assert(Number.isSafeInteger(N) && N > 0 && N <= 2_000);
assert(Number.isSafeInteger(seconds) && seconds >= 2 && seconds <= 30);
const NOW = 1_900_000_000_000, pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const hash = value => createHash("sha256").update(value).digest("hex");
const keyFor = (prefix, id) => `topology/${prefix}/${hash(id)}`;
const identityFor = index => ({ id: `session:synthetic-${index}-00000000`, name: `synthetic-${index}`, kind: "main", sessionId: `synthetic-${index}-00000000` });
const participantFor = index => {
  const identity = identityFor(index), id = identity.id;
  return { format: 1, id, rootId: id, ownerHostId: id, ownerIdentityId: id, kind: "root", name: identity.name,
    status: "idle", runner: "pi", transport: "process", capabilities: ["steer", "followUp", "fabric"], sessionId: identity.sessionId,
    startedAt: NOW - 10_000, updatedAt: NOW, controlProtocol: "v1", description: "Synthetic activity. ".repeat(16) };
};
const leaseFor = (index, generation) => {
  const id = identityFor(index).id;
  return { id, rootId: id, identityId: id, startedAt: NOW - 10_000, updatedAt: NOW + generation * 1_000, expiresAt: NOW + 60_000 + generation * 1_000 };
};

if (args.writer) {
  const { MeshStore, writeHostLease } = await import(pathToFileURL(args.bundle).href);
  const store = new MeshStore(args.writer, 64 * 1024, 1_000, { stateBackend: "sqlite" });
  assert.equal(store.stateBackend, "sqlite");
  let generation = 0, status = "idle";
  process.on("message", async phase => {
    try {
      const period = phase === "busy" ? 10 : 50;
      let commits = 0, leaseFiles = 0, participantCommits = 0;
      for (let tick = 0; tick < seconds * 1_000 / period; tick++) {
        if (phase !== "idle") {
          await store.put({ key: "unrelated/benchmark", value: { phase, tick }, identity: identityFor(0) });
          commits++;
          if (tick % (1_000 / period) === 0) {
            generation++;
            for (let index = 0; index < N; index++) writeHostLease(args.writer, leaseFor(index, generation));
            leaseFiles += N;
            if (phase === "busy") {
              status = `working-${generation}`;
              await store.put({ key: keyFor("participants", identityFor(0).id),
                value: { ...participantFor(0), status }, identity: identityFor(0) });
              participantCommits++;
            }
          }
        }
        await pause(period);
      }
      process.send({ commits, leaseFiles, participantCommits, generation, status });
    } catch (error) { console.error(error); process.exitCode = 1; process.disconnect(); }
  });
  process.on("disconnect", () => { store.closeState(); });
  process.send("ready");
} else {
  assert.equal(process.platform, "linux", "/proc observer rchar measurement requires Linux");
  assert(args["before-ref"], "--before-ref=<commit> is required");
  const git = (...argv) => execFileSync("git", argv, { cwd: repo, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }).trim();
  const head = git("rev-parse", "HEAD"), before = git("rev-parse", "--verify", `${args["before-ref"]}^{commit}`);
  const sourceFiles = ["src/mesh/bridge.ts", "src/mesh/state-sqlite.ts"];
  const sources = Object.fromEntries(["before", "candidate"].map(label => [label,
    Object.fromEntries(sourceFiles.map(source => [source, label === "before"
      ? execFileSync("git", ["show", `${before}:${source}`], { cwd: repo, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 })
      : fs.readFileSync(path.join(repo, source), "utf8")]))]));
  const allowedSourceChanges = new Set(sourceFiles);
  const otherChanges = git("diff", "--name-only", before, "--", "src").split("\n").filter(name => name && !allowedSourceChanges.has(name));
  assert.deepEqual(otherChanges, [], "This comparison substitutes ONLY bridge.ts and state-sqlite.ts");
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-notifications-"));
  fs.chmodSync(scratch, 0o700);
  const bundles = {}, bundleHashes = {}, implementations = {};
  const historicalSources = new Map(sourceFiles.map(source => [path.join(repo, source), sources.before[source]]));
  for (const label of Object.keys(sources)) {
    const bundle = path.join(scratch, `${label}.mjs`);
    await build({ stdin: { contents: 'export { MeshStore } from "./src/mesh/store.ts";\n' +
      'export { SqliteStateStore } from "./src/mesh/state-sqlite.ts";\n' +
      'export { writeHostLease } from "./src/topology/host-leases.ts";\n' +
      'export { StoreBridgeSide } from "./src/mesh/bridge.ts";\n', resolveDir: repo, loader: "ts" },
      bundle: true, platform: "node", target: "node24", format: "esm", outfile: bundle, logLevel: "error",
      plugins: label === "before" ? [{ name: "historical-source-files", setup(build) {
        build.onLoad({ filter: /.*/ }, args => {
          const contents = historicalSources.get(args.path);
          return contents === undefined ? null : { contents, loader: "ts", resolveDir: path.dirname(args.path) };
        });
      } }] : [],
    });
    bundles[label] = bundle;
    bundleHashes[label] = hash(fs.readFileSync(bundle));
    implementations[label] = await import(pathToFileURL(bundle).href);
  }
  const rchar = () => Number(/^rchar: (\d+)$/m.exec(fs.readFileSync(`/proc/${process.pid}/io`, "utf8"))[1]);
  const rows = [], projections = {}, workloads = {};
  for (const label of Object.keys(sources)) {
    const root = path.join(scratch, label);
    fs.mkdirSync(root, { mode: 0o700 });
    const implementation = implementations[label];
    const { MeshStore, SqliteStateStore, writeHostLease, StoreBridgeSide } = implementation;
    SqliteStateStore.openSync(root, 64 * 1024, 1_000, { initialize: "create" }).close();
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
    assert.equal(store.stateBackend, "sqlite");
    const ops = [];
    for (let index = 0; index < N; index++) {
      const identity = identityFor(index), id = identity.id;
      ops.push({ kind: "put", key: keyFor("hosts", id), value: { format: 1, id, rootId: id, identity,
        startedAt: NOW - 10_000, updatedAt: NOW, expiresAt: NOW + 60_000 }, identity });
      ops.push({ kind: "put", key: keyFor("participants", id), value: participantFor(index), identity });
      writeHostLease(root, leaseFor(index, 0));
    }
    await store.writeBatch({ identity: identityFor(0), ops });
    const writer = fork(file, [`--writer=${root}`, `--bundle=${bundles[label]}`, `--participants=${N}`, `--seconds=${seconds}`], { stdio: ["ignore", "inherit", "inherit", "ipc"] });
    const exited = once(writer, "exit");
    const message = () => Promise.race([once(writer, "message").then(([value]) => value), exited.then(([code]) => { throw new Error(`Writer exited early: ${code}`); })]);
    let unsubscribe;
    try {
      assert.equal(await message(), "ready");
      const side = new StoreBridgeSide(store, "far", () => NOW);
      const notices = { presence: 0, lease: 0, events: 0 }, failures = [];
      unsubscribe = side.subscribeChanges(kind => { notices[kind]++; }, error => failures.push(String(error)));
      await pause(400); // let fixture-creation hints settle, not idle polling
      projections[label] = {};
      workloads[label] = {};
      for (const phase of ["idle", "steady", "busy"]) {
        for (const kind of Object.keys(notices)) notices[kind] = 0;
        const startRead = rchar(), startCpu = process.cpuUsage(), startTime = performance.now();
        const done = message();
        writer.send(phase);
        const workload = await done;
        await pause(400); // includes the trailing coalescing scan in the observer window
        const elapsed = (performance.now() - startTime) / 1_000, cpu = process.cpuUsage(startCpu), bytes = rchar() - startRead;
        rows.push({ label, phase, elapsedSeconds: elapsed, observerPid: process.pid, writerPid: writer.pid,
          rcharBytes: bytes, rcharBytesPerSecond: bytes / elapsed, cpuMs: (cpu.user + cpu.system) / 1_000,
          cpuPercent: (cpu.user + cpu.system) / (elapsed * 10_000), notices: { ...notices }, workload });
        assert.deepEqual(failures, []);
        if (phase !== "idle") assert(notices.lease + notices.presence > 0, `${label}/${phase}: no real topology notifications`);
        // Public projection checks are outside the measurement window, not charged to subscribeChanges.
        const projection = await side.presence();
        assert.equal(projection.hosts.length, N);
        assert.equal(projection.participants.length, N);
        assert.equal(projection.participants.find(row => row.id === identityFor(0).id).status, workload.status);
        assert(projection.hosts.every(host => host.expiresAt === NOW + 60_000 + workload.generation * 1_000));
        projections[label][phase] = hash(JSON.stringify(projection));
        workloads[label][phase] = workload;
      }
    } finally {
      unsubscribe?.();
      writer.disconnect();
      const [code] = await exited;
      store.closeState();
      assert.equal(code, 0, "Writer must terminate cleanly");
    }
  }
  assert.deepEqual(workloads.before, workloads.candidate, "Variants must execute the same workload");
  assert.deepEqual(projections.before, projections.candidate, "Final public projections differ");
  const result = { head, before, participants: N, secondsPerWorkload: seconds,
    sourceHashes: Object.fromEntries(Object.entries(sources).map(([label, files]) => [label,
      Object.fromEntries(Object.entries(files).map(([source, contents]) => [source, hash(contents)]))])),
    scriptHash: hash(fs.readFileSync(file)), bundleHashes, rows, projections,
    measurement: "Observer /proc/PID/io rchar and process.cpuUsage; separate writer PID, fixture/build/public projections excluded; 400ms trailing drain included",
    limitations: "Synthetic warm fixture; one window per variant; rchar is logical reads, not disk bytes. Notification counts are real; final public projections checked, not every notification preimage. No CI timing threshold.",
    scratch };
  const text = JSON.stringify(result, null, 2) + "\n";
  if (args.out) fs.writeFileSync(path.resolve(args.out), text);
  console.log(text);
}
