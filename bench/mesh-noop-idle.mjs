#!/usr/bin/env node
// #18131: simulated idle Main publication through the REAL ParticipantDirectory caller.
// Reuses the participant-refresh-idle.mjs private-scratch/JSON-result style and existing esbuild.
// No generic identical-put shortcut is assumed: revisions/guards/outbox keep their normal contract.
//
// node bench/mesh-noop-idle.mjs --extension-root <complete-package-root> --label baseline --out <dir>
// node bench/mesh-noop-idle.mjs --extension-root <complete-package-root> --label candidate --out <dir>
//   [--backend file|sqlite] [--modeled-seconds 300] [--participants 30] [--scratch <private-local-temp-base>]
// Backend defaults to sqlite; every case asserts the actual backend (no fallback coverage).
//
// Each invocation bundles that root's real source into scratch (ParticipantDirectory is not a
// public dist export). Both full source and installed dependencies are required; nothing in the
// selected root is changed. Input-closure and harness hashes identify the measured bytes. This is
// NOT the production dist build or a live Pi CPU/latency profile. Date.now advances in 1s/5s steps;
// performance.now measures actual elapsed wall time. No sleeps or automatic directory timers.
// Isolated idle workload: no independent commit witnesses are synthesized. Filesystem mtimes stay
// real, so confirmation acquisition rates must not be extrapolated to a busy fleet with witnesses.
// The runtime's old 25s override is NOT modeled here; runtime-presence-cadence.test.ts covers it.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build, version as esbuildVersion } from "esbuild";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
const options = new Map();
const accepted = new Set(["--extension-root", "--label", "--out", "--scratch", "--modeled-seconds", "--participants", "--revision", "--backend"]);
for (let i = 0; i < args.length; i += 2) {
  if (!accepted.has(args[i]) || !args[i + 1] || args[i + 1].startsWith("--") || options.has(args[i])) {
    throw new Error(`Invalid or duplicate option ${args[i] ?? ""}; see the script header for usage`);
  }
  options.set(args[i], args[i + 1]);
}
const extensionRoot = fs.realpathSync(path.resolve(options.get("--extension-root") ?? repo));
const label = options.get("--label") ?? "candidate";
if (!/^[A-Za-z0-9._-]+$/.test(label)) throw new Error("--label must be a simple filename label");
const backend = options.get("--backend") ?? "sqlite";
if (backend !== "file" && backend !== "sqlite") throw new Error("--backend must be file or sqlite");
const modeledSeconds = Number(options.get("--modeled-seconds") ?? 300);
const participants = Number(options.get("--participants") ?? 30);
if (!Number.isSafeInteger(modeledSeconds) || modeledSeconds < 20 || modeledSeconds > 1_800 || modeledSeconds % 5 !== 0) {
  throw new Error("--modeled-seconds must be a multiple of 5 between 20 and 1800 (lock stats retain 60 minutes)");
}
if (!Number.isSafeInteger(participants) || participants < 1 || participants > 1_000) throw new Error("--participants must be 1..1000");
for (const name of ["src/topology/participant-directory.ts", "src/mesh/store.ts", "node_modules", "package.json", "bun.lock"]) {
  if (!fs.existsSync(path.join(extensionRoot, name))) throw new Error(`Missing benchmark input: ${path.join(extensionRoot, name)}`);
}
const sha = value => createHash("sha256").update(value).digest("hex");
const keyFor = (prefix, id) => prefix + sha(id);
const round = n => Math.round(n * 1_000) / 1_000;
process.umask(0o077);
const scratch = fs.realpathSync(fs.mkdtempSync(path.join(path.resolve(options.get("--scratch") ?? os.tmpdir()), "mesh-noop-idle-")));
const out = path.resolve(options.get("--out") ?? path.join(scratch, "results"));
fs.mkdirSync(out, { recursive: true, mode: 0o700 });
const agentDir = path.join(scratch, "agent");
fs.mkdirSync(agentDir, { mode: 0o700 });
// Do not import a caller's live mesh, role, profile, stats sink or process incarnation.
for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) delete process.env[key];
process.env.PI_CODING_AGENT_DIR = agentDir;
process.env.PI_FABRIC_PROJECT_ROOT = scratch;
process.env.PI_FABRIC_MESH_STATE_BACKEND = backend;
process.env.PI_FABRIC_MESH_CHECKPOINT_LOG = "off";
process.env.PI_FABRIC_LOCK_STATS = "1";
// Existing fixture seam, as in participant-refresh-idle.mjs: only NEW scratch databases.
if (backend === "sqlite") globalThis[Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures")] = "create";
fs.symlinkSync(path.join(extensionRoot, "node_modules"), path.join(scratch, "node_modules"), process.platform === "win32" ? "junction" : "dir");
const harness = path.join(scratch, "directory-harness.mjs");
const bundled = await build({
  absWorkingDir: extensionRoot,
  stdin: { contents: [
    'export { ParticipantDirectory } from "./src/topology/participant-directory.ts";',
    'export { MeshStore } from "./src/mesh/store.ts";',
    'export { readHostLeaseCurrent } from "./src/topology/host-leases.ts";',
    'export { lockStatsHost } from "./src/mesh/commit-stats.ts";',
  ].join("\n"), resolveDir: extensionRoot, sourcefile: "mesh-noop-idle-entry.ts", loader: "ts" },
  outfile: harness, bundle: true, packages: "external", platform: "node", format: "esm", target: "node24",
  metafile: true, logLevel: "warning",
});
const inputs = Object.keys(bundled.metafile.inputs).filter(name => name !== "mesh-noop-idle-entry.ts")
  .map(name => ({ path: name, sha256: sha(fs.readFileSync(path.resolve(extensionRoot, name))) }))
  .sort((a, b) => a.path.localeCompare(b.path));
const { ParticipantDirectory, MeshStore, readHostLeaseCurrent, lockStatsHost } = await import(pathToFileURL(harness).href);
const recorder = globalThis[Symbol.for("pi-fabric.mesh.lock-stats")];
if (!recorder?.stats) throw new Error("The existing native lock-stat recorder is not active");

// Read L8's own flushed per-process file, rather than counting API calls or attempted operations.
const sample = root => {
  recorder.flush();
  const file = path.join(root, "lock-stats", `${lockStatsHost()}-${process.pid}.json`);
  const raw = JSON.parse(fs.readFileSync(file, "utf8"));
  const classes = {}, operations = {};
  for (const minute of raw.minutes) {
    for (const [name, bucket] of Object.entries(minute.classes)) {
      const value = classes[name] ??= { acquisitions: 0, holdMs: 0, waitMs: 0, timeouts: 0, tries: 0 };
      value.acquisitions += bucket.n; value.holdMs += bucket.holdMs; value.waitMs += bucket.waitMs;
      value.timeouts += bucket.timeouts; value.tries += bucket.tries;
    }
    for (const [name, bucket] of Object.entries(minute.operations ?? {})) operations[name] = (operations[name] ?? 0) + bucket.n;
  }
  return { file, classes, operations };
};
const difference = (after, before) => {
  const classes = {}, operations = {};
  for (const name of new Set([...Object.keys(after.classes), ...Object.keys(before.classes)])) {
    classes[name] = Object.fromEntries(["acquisitions", "holdMs", "waitMs", "timeouts", "tries"].map(field =>
      [field, round((after.classes[name]?.[field] ?? 0) - (before.classes[name]?.[field] ?? 0))]));
  }
  for (const name of new Set([...Object.keys(after.operations), ...Object.keys(before.operations)])) {
    operations[name] = (after.operations[name] ?? 0) - (before.operations[name] ?? 0);
  }
  return { classes, operations };
};

const realNow = Date.now;
const cases = [];
try {
  for (const kind of ["idle-change-refresh", "idle-heartbeat", "changed-key", "legacy-lease-renewal"]) {
    // Separate database / directory per case; seed and first publication are outside the delta.
    const root = path.join(scratch, kind);
    let now = realNow();
    Date.now = () => now;
    const identity = { id: "session:bench-main", sessionId: "bench-main", name: "main", kind: "main" };
    const mesh = new MeshStore(root, 256 * 1024, 1_000, { stateBackend: backend, backgroundReadCacheMs: 5_000, readActive: () => false });
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 5_000, reapDeadHosts: false });
    let model = "bench/model-0";
    directory.registerSource(() => Array.from({ length: participants }, (_, index) => ({
      format: 1, id: index === 0 ? identity.id : `actor:bench-${index}`, rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, kind: index === 0 ? "root" : "actor",
      name: index === 0 ? "bench-main" : `worker-${index}`, status: "idle", runner: "pi",
      transport: index === 0 ? "host" : "process", capabilities: index === 0 ? ["steer", "followUp", "fabric"] : ["steer", "stop"],
      ...(index === 0 ? { sessionId: identity.sessionId, label: "project-1", cwd: scratch, model, pendingMessages: false } : {}),
      startedAt: 1, updatedAt: now, controlProtocol: "v1",
    })));
    try {
      if (mesh.stateBackend !== backend) throw new Error(`${kind}: requested ${backend}, actual ${mesh.stateBackend}; backend fallback is not coverage`);
      if (kind === "legacy-lease-renewal") {
        // A live pre-capability host keeps real compatibility renewals due, without injecting
        // direct writes into the measured loop. Its long fixed lease lasts the modeled run.
        const peer = { id: "session:legacy-reader", sessionId: "legacy-reader", name: "old-main", kind: "main" };
        await mesh.put({ key: keyFor("topology/hosts/", peer.id), identity: peer, value: {
          format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: now,
          updatedAt: now, expiresAt: now + (modeledSeconds + 60) * 1_000,
        } });
      }
      await directory.refresh(); // No start(): manual refreshes are the only source of work.
      const rootKey = keyFor("topology/participants/", identity.id);
      const hostKey = keyFor("topology/hosts/", identity.id);
      const initialRoot = mesh.get(rootKey, { fresh: true });
      const initialHost = mesh.get(hostKey, { fresh: true });
      if (!initialRoot || !initialHost) throw new Error(`${kind}: real initial publication is missing`);
      const before = sample(root);
      // File has no SQLite store/stats; do not invent transaction/commit counts for it.
      const beforeDb = backend === "sqlite" ? mesh.stateBackendHandle.store.stats() : null;
      const stepMs = kind === "idle-change-refresh" || kind === "changed-key" ? 1_000 : 5_000;
      const iterations = modeledSeconds * 1_000 / stepMs;
      const started = performance.now();
      for (let index = 1; index <= iterations; index++) {
        now += stepMs;
        if (kind === "changed-key") model = `bench/model-${index}`;
        if (kind === "idle-heartbeat" || kind === "legacy-lease-renewal") await directory.refresh();
        else await directory.refreshPresence();
      }
      const wallElapsedMs = performance.now() - started;
      const afterDb = backend === "sqlite" ? mesh.stateBackendHandle.store.stats() : null;
      const after = sample(root);
      const delta = difference(after, before);
      const finalRoot = mesh.get(rootKey, { fresh: true });
      const finalHost = mesh.get(hostKey, { fresh: true });
      const lease = readHostLeaseCurrent(root, identity.id);
      if (!finalRoot || !finalHost) throw new Error(`${kind}: real publication disappeared`);
      if (kind === "changed-key" && (finalRoot.value.model !== model || finalRoot.version <= initialRoot.version)) {
        throw new Error("changed-key: the real directory did not publish the changed Main model");
      }
      // Unchanged-root rewrites are measured below, not forbidden on a historical baseline.
      if (kind === "legacy-lease-renewal" && (finalHost.version <= initialHost.version ||
        finalHost.value.updatedAt <= initialHost.value.updatedAt || finalHost.value.expiresAt <= initialHost.value.expiresAt)) {
        throw new Error("legacy-lease-renewal: real stored host lease did not renew");
      }
      if (!lease || lease.updatedAt <= initialHost.value.updatedAt) throw new Error(`${kind}: file lease did not renew`);
      const acquisitions = Object.values(delta.classes).reduce((sum, bucket) => sum + bucket.acquisitions, 0);
      const writeBatch = delta.classes.writeBatch?.acquisitions ?? 0;
      const confirm = delta.classes["heartbeat/confirm"]?.acquisitions ?? 0;
      if (Object.values(delta.classes).some(bucket => bucket.timeouts || bucket.tries)) throw new Error(`${kind}: unexpected lock refusal`);
      if ((kind === "changed-key" || kind === "legacy-lease-renewal") && writeBatch === 0) throw new Error(`${kind}: positive control acquired no writer`);
      const statsArtifact = path.join(out, `lock-stats-${label}-${kind}.json`);
      fs.copyFileSync(after.file, statsArtifact);
      cases.push({ kind, backend: mesh.stateBackend, iterations, participants, modeledSeconds, stepMs, wallElapsedMs: round(wallElapsedMs),
        throughputRefreshesPerWallSecond: round(iterations * 1_000 / wallElapsedMs),
        writerAcquisitions: acquisitions, writeBatchAcquisitions: writeBatch, heartbeatConfirmAcquisitions: confirm,
        acquisitionsPerModeledMinute: round(acquisitions * 60 / modeledSeconds),
        writeBatchPerModeledMinute: round(writeBatch * 60 / modeledSeconds),
        heartbeatConfirmPerModeledMinute: round(confirm * 60 / modeledSeconds),
        asyncSqliteTransactions: beforeDb === null ? null : afterDb.transactions - beforeDb.transactions,
        changingCommits: beforeDb === null ? null : afterDb.commits - beforeDb.commits,
        byClass: delta.classes, byOperation: delta.operations,
        rootVersionBefore: initialRoot.version, rootVersionAfter: finalRoot.version,
        rootRewritten: finalRoot.version !== initialRoot.version,
        hostVersionBefore: initialHost.version, hostVersionAfter: finalHost.version,
        hostExpiresAtBefore: initialHost.value.expiresAt, hostExpiresAtAfter: finalHost.value.expiresAt,
        fileLeaseRenewed: lease.updatedAt > initialHost.value.updatedAt, statsArtifact,
      });
    } finally {
      directory.suspend(); // No start()/exit hook and no close()/withdrawal traffic in the window.
      mesh.closeState();
      Date.now = realNow;
    }
  }
} finally {
  Date.now = realNow;
  recorder.dispose();
}
const result = {
  label, backend, extensionRoot, revisionLabel: options.get("--revision") ?? null, node: process.version, esbuildVersion,
  packageSha256: sha(fs.readFileSync(path.join(extensionRoot, "package.json"))),
  lockfileSha256: sha(fs.readFileSync(path.join(extensionRoot, "bun.lock"))),
  harness, harnessSha256: sha(fs.readFileSync(harness)), sourceClosureSha256: sha(JSON.stringify(inputs)),
  inputs, benchmarkSha256: sha(fs.readFileSync(fileURLToPath(import.meta.url))),
  measurement: `real source ParticipantDirectory; ${backend} writer lock-stat deltas; startup excluded; no live Pi process`,
  limitations: ["Source harness, not production dist; no runtime presence-pass override or actor registry model",
    "Modeled Date clock; actual filesystem mtimes; isolated mesh without independent commit witnesses",
    "Sequential throughput only: no fleet contention, CPU profile, wall-minute rate or proven savings implied"],
  scratch, cases,
};
const resultFile = path.join(out, `result-${label}.json`);
fs.writeFileSync(resultFile, JSON.stringify(result, null, 2) + "\n", { mode: 0o600 });
// Leave this script's private scratch / harness for reproducibility; caller owns final cleanup.
console.log(JSON.stringify({ resultFile, sourceClosureSha256: result.sourceClosureSha256, cases }, null, 2));
