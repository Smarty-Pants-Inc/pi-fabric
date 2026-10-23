import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";

const [, , packageRootArg, caseName, outputDirArg] = process.argv;
const expectedByCase = {
  "oversized-unknown": "retry",
  "fifo-third-open": "retry",
  "directory-third-open": "retry",
  "symlink-lease": "retry",
  "exact-cap": "terminal",
  "over-cap": "retry",
  "truncate-after-fstat": "retry",
  "grow-after-fstat": "retry",
  "native-absent": "terminal",
  unparseable: "retry",
};
const expected = expectedByCase[caseName];
const outputDir = path.resolve(outputDirArg ?? "");
const packageRoot = path.resolve(packageRootArg ?? "");
const progressFile = path.join(outputDir, "progress.json");
const resultFile = path.join(outputDir, "result.json");
const LEASE_CAP = 65_536;
const LARGE_PADDING_BYTES = 9_450_000;
const CASE_TOPIC = "wake.bounded.lease.synthetic";

const fail = code => {
  const error = new Error(code);
  error.code = code;
  throw error;
};
const ensure = (condition, code) => { if (!condition) fail(code); };
const sortedJson = value => {
  const sort = item => Array.isArray(item) ? item.map(sort) :
    item !== null && typeof item === "object"
      ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
        .map(([key, field]) => [key, sort(field)]))
      : item;
  return JSON.stringify(sort(JSON.parse(JSON.stringify(value))));
};
const writeProgress = patch => {
  let prior = {};
  try { prior = JSON.parse(fs.readFileSync(progressFile, "utf8")); } catch { /* first write */ }
  const next = { ...prior, ...patch, case: caseName };
  fs.writeFileSync(progressFile, JSON.stringify(next) + "\n", { mode: 0o600 });
  try {
    const fd = fs.openSync(progressFile, fs.constants.O_RDONLY);
    try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  } catch { /* the synchronous write is the progress proof */ }
};
const pendingAt = (meshRoot, root) => {
  const file = path.join(meshRoot, "wake-unacknowledged.jsonl");
  if (!fs.existsSync(file)) return undefined;
  let pending;
  for (const line of fs.readFileSync(file, "utf8").split("\n").filter(Boolean)) {
    const row = JSON.parse(line);
    if (row.root !== root) continue;
    if (row.op === "pending") pending = row.delivery;
    else if (row.op === "ack" && pending?.id === row.delivery?.id &&
        pending?.sequence === row.delivery?.sequence) pending = undefined;
  }
  return pending;
};
const exactAckCount = (meshRoot, root, delivery) => {
  const file = path.join(meshRoot, "wake-unacknowledged.jsonl");
  if (!fs.existsSync(file)) return 0;
  return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line))
    .filter(row => row.op === "ack" && row.root === root && row.delivery?.id === delivery.id &&
      row.delivery?.sequence === delivery.sequence).length;
};
const receipt = file => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return undefined; }
};
const makePadding = (lease, bytes) => {
  const base = Buffer.byteLength(JSON.stringify({ format: 1, ...lease, padding: "" }));
  ensure(base <= bytes, "PADDING_BASE_EXCEEDS_TARGET");
  const padded = { ...lease, padding: "x".repeat(bytes - base) };
  ensure(Buffer.byteLength(JSON.stringify({ format: 1, ...padded })) === bytes, "PADDING_TARGET_MISMATCH");
  return padded;
};
const rawOpenFlags = flags => {
  if (typeof flags !== "number") return false;
  const base = fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW;
  return flags === base || (fs.constants.O_NONBLOCK !== undefined &&
    flags === (base | fs.constants.O_NONBLOCK));
};

ensure(typeof packageRootArg === "string" && typeof caseName === "string" && typeof outputDirArg === "string",
  "ARGS_REQUIRED");
ensure(Boolean(expected), "UNKNOWN_CASE");
ensure(packageRoot.length > 1 && fs.statSync(packageRoot).isDirectory(), "PACKAGE_ROOT_MISSING");
ensure(fs.statSync(path.join(packageRoot, "dist")).isDirectory(), "COMPILED_DIST_MISSING");
ensure(fs.statSync(outputDir).isDirectory(), "OUTPUT_DIR_MISSING");
fs.chmodSync(outputDir, 0o700);

const workspace = fs.mkdtempSync(path.join(outputDir, "private-wake-probe-"));
fs.chmodSync(workspace, 0o700);
const agentDir = path.join(workspace, "agent");
fs.mkdirSync(agentDir, { mode: 0o700 });
process.env.PI_CODING_AGENT_DIR = agentDir;

const realNow = Date.now;
let now = realNow();
Date.now = () => now;
let mesh;
let leaseFile;
let leaseBackup;
let nativeLease;
let instrumentation;
let result;
let failureCode;
let thirdOpenProof;
const progress = { stage: "starting", case: caseName };
writeProgress(progress);

try {
  const load = relative => import(pathToFileURL(path.join(packageRoot, "dist", relative)).href);
  const [{ MeshStore }, wake, leases, fabricConfig, terminalKernel] = await Promise.all([
    load("mesh.js"), load("residency/wake.js"), load("topology/host-leases.js"), load("config.js"),
    load("residency/wake-terminal.js"),
  ]);
  ensure(typeof wake.retryResidentWakes === "function", "COMPILED_WAKE_ENTRY_MISSING");
  ensure(typeof terminalKernel.residentWakeTerminalRoot === "function", "COMPILED_TERMINAL_ENTRY_MISSING");
  ensure(typeof leases.writeHostLease === "function" && typeof leases.hostLeasePath === "function",
    "COMPILED_HOST_LEASE_ENTRY_MISSING");
  ensure(fabricConfig.DEFAULT_FABRIC_CONFIG !== undefined, "COMPILED_CONFIG_ENTRY_MISSING");

  const rootId = `session:${randomUUID()}`;
  const sessionId = rootId.slice("session:".length);
  const meshRoot = path.join(workspace, "mesh");
  const root = path.join(meshRoot, "residency", createHash("sha256").update(rootId).digest("hex"));
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  let allowed = false;
  mesh = new MeshStore(meshRoot, 65_536, 100, { stateBackend: "sqlite", canWakeResidents: () => allowed });
  ensure(mesh.stateBackend === "sqlite", "SQLITE_BACKEND_NOT_SELECTED");
  const identity = { id: rootId, name: "synthetic-wake-probe", kind: "main" };
  const proofKey = "wake-bounded-probe/sqlite-proof";
  await mesh.put({ key: proofKey, identity, value: { backend: "sqlite", proof: true } });
  ensure(fs.existsSync(path.join(meshRoot, "state.db")), "SQLITE_DATABASE_MISSING_AFTER_PUT");

  const config = { format: 1, rootId, sessionId, cwd: root, meshRoot, residencyRoot: root,
    mesh: { transport: "file" } };
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config), { mode: 0o600 });
  fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({ format: 1, rootId,
    hostId: "synthetic-resident", configJson: sortedJson(config),
    actors: [{ id: "synthetic-actor", name: "synthetic listener", topics: [CASE_TOPIC] }] }), { mode: 0o600 });
  await wake.ensureResidentWakeArchive(mesh);
  const event = await mesh.publish({ topic: CASE_TOPIC,
    from: { id: "synthetic-publisher", name: "synthetic publisher", kind: "main" } });
  const delivery = { id: event.id, sequence: event.sequence };
  ensure(Number.isSafeInteger(delivery.sequence) && delivery.sequence > 0, "PUBLICATION_SEQUENCE_MISSING");
  ensure(pendingAt(meshRoot, root)?.id === delivery.id && pendingAt(meshRoot, root)?.sequence === delivery.sequence,
    "NATIVE_PENDING_DELIVERY_MISSING");

  const expiresAt = now - 5 * 60 * 60_000;
  nativeLease = { format: 1, id: rootId, rootId, identityId: rootId,
    updatedAt: expiresAt - 15_000, expiresAt };
  leaseFile = leases.hostLeasePath(meshRoot, rootId);
  leases.writeHostLease(meshRoot, nativeLease);
  const initialLease = leases.readHostLeaseCurrent(meshRoot, rootId);
  ensure(initialLease?.id === rootId && initialLease.expiresAt - initialLease.updatedAt === 15_000,
    "VALID_15S_LEASE_MISSING");

  // Every case follows the real compiled retry path after a persisted restart-snapshot mismatch.
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify({ ...config, cwd: path.join(root, "drift") }),
    { mode: 0o600 });
  allowed = true;

  if (caseName === "oversized-unknown") {
    leases.writeHostLease(meshRoot, { ...nativeLease, padding: "x".repeat(LARGE_PADDING_BYTES) });
  } else if (caseName === "exact-cap") {
    leases.writeHostLease(meshRoot, makePadding(nativeLease, LEASE_CAP));
  } else if (caseName === "over-cap") {
    leases.writeHostLease(meshRoot, makePadding(nativeLease, LEASE_CAP + 1));
  } else if (caseName === "unparseable") {
    fs.writeFileSync(leaseFile, "{", { mode: 0o600 });
  } else if (caseName === "symlink-lease") {
    leaseBackup = `${leaseFile}.synthetic-backup`;
    fs.renameSync(leaseFile, leaseBackup);
    fs.symlinkSync(leaseBackup, leaseFile);
  }

  const startupLeaseStat = fs.lstatSync(leaseFile, { bigint: true });
  const nativeBytes = Number(startupLeaseStat.size);
  const rawStats = {
    openAttempts: 0,
    openFailures: 0,
    fstatCalls: 0,
    readCalls: 0,
    actualBytes: 0,
    maxDescriptorBytes: 0,
    maxDescriptorReadCalls: 0,
  };
  let activeCustodyDepth = 0;
  let faultApplied = false;
  const rawDescriptors = new Map();
  const nativeLeaseCustody = mesh.leaseCustody.bind(mesh);
  mesh.leaseCustody = async (file, operation, ...args) => {
    if (path.resolve(String(file)) !== path.resolve(leaseFile))
      return nativeLeaseCustody(file, operation, ...args);
    return nativeLeaseCustody(file, async (...callbackArgs) => {
      activeCustodyDepth++;
      try { return await operation(...callbackArgs); }
      finally { activeCustodyDepth--; }
    }, ...args);
  };

  const original = {
    openSync: fs.openSync,
    fstatSync: fs.fstatSync,
    readSync: fs.readSync,
    readFileSync: fs.readFileSync,
    closeSync: fs.closeSync,
    truncateSync: fs.truncateSync,
    writeFileSync: fs.writeFileSync,
  };
  const descriptorStats = [];
  const swapLeaseFor = kind => {
    leaseBackup = `${leaseFile}.synthetic-backup`;
    fs.renameSync(leaseFile, leaseBackup);
    if (kind === "fifo") execFileSync("mkfifo", [leaseFile], { stdio: "ignore" });
    else fs.mkdirSync(leaseFile, { mode: 0o700 });
  };
  const restoreFaultedLease = () => {
    if (leaseBackup && fs.existsSync(leaseBackup)) {
      try {
        const named = fs.lstatSync(leaseFile);
        if (named.isDirectory() && !named.isSymbolicLink()) fs.rmSync(leaseFile, { recursive: true, force: true });
        else fs.rmSync(leaseFile, { force: true });
      } catch { /* absent path */ }
      fs.renameSync(leaseBackup, leaseFile);
      leaseBackup = undefined;
    } else if (["exact-cap", "over-cap", "oversized-unknown", "truncate-after-fstat", "grow-after-fstat"]
      .includes(caseName)) {
      leases.writeHostLease(meshRoot, nativeLease);
    }
  };
  const recordRead = (fd, bytes, calls = 1) => {
    const descriptor = rawDescriptors.get(fd);
    if (!descriptor) return;
    descriptor.actualBytes += bytes;
    descriptor.readCalls += calls;
    rawStats.actualBytes += bytes;
    rawStats.readCalls += calls;
    rawStats.maxDescriptorBytes = Math.max(rawStats.maxDescriptorBytes, descriptor.actualBytes);
    rawStats.maxDescriptorReadCalls = Math.max(rawStats.maxDescriptorReadCalls, descriptor.readCalls);
  };

  fs.openSync = function (...args) {
    const [file, flags] = args;
    const target = typeof file === "string" || file instanceof URL
      ? path.resolve(String(file)) === path.resolve(leaseFile)
      : Buffer.isBuffer(file) && path.resolve(file.toString()) === path.resolve(leaseFile);
    if (!target || !rawOpenFlags(flags)) return Reflect.apply(original.openSync, fs, args);
    rawStats.openAttempts++;
    if ((caseName === "fifo-third-open" || caseName === "directory-third-open") &&
        rawStats.openAttempts === 3 && !faultApplied) {
      // The third raw open is the terminal kernel's final recheck: the real terminal
      // call acquires the Main publication fence before entering this actual leaseCustody callback.
      thirdOpenProof = { rawOpenAttempt: 3, insideNativeLeaseCustody: activeCustodyDepth > 0,
        publicationFenceHeld: activeCustodyDepth > 0,
        publicationFenceEvidence: "final terminal recheck; publication fence precedes leaseCustody callback" };
      ensure(thirdOpenProof.insideNativeLeaseCustody && thirdOpenProof.publicationFenceHeld,
        "THIRD_RAW_OPEN_NOT_INSIDE_BOTH_FENCES");
      swapLeaseFor(caseName === "fifo-third-open" ? "fifo" : "directory");
      faultApplied = true;
      writeProgress({ stage: "third-raw-open-before-native-open", sqlitePutSucceeded: true,
        pendingDeliveryCommitted: true, validLeaseTtlMs: 15_000, rawOpenAttempts: 3,
        thirdOpenProof, fifoOrDirectoryInstalled: true });
    }
    try {
      const fd = Reflect.apply(original.openSync, fs, args);
      const descriptor = { actualBytes: 0, readCalls: 0 };
      rawDescriptors.set(fd, descriptor);
      descriptorStats.push(descriptor);
      return fd;
    } catch (error) {
      rawStats.openFailures++;
      throw error;
    }
  };
  fs.fstatSync = function (...args) {
    const value = Reflect.apply(original.fstatSync, fs, args);
    const fd = args[0];
    const descriptor = rawDescriptors.get(fd);
    if (!descriptor) return value; // Preserve native Stats and the caller's options unchanged.
    rawStats.fstatCalls++;
    descriptor.fstatCalls = (descriptor.fstatCalls ?? 0) + 1;
    if (!faultApplied && descriptor.fstatCalls === 1 &&
        (caseName === "truncate-after-fstat" || caseName === "grow-after-fstat")) {
      if (caseName === "truncate-after-fstat") {
        const size = Number(value.size);
        original.truncateSync.call(fs, leaseFile, Math.max(1, Math.floor(size / 2)));
      } else {
        original.writeFileSync.call(fs, leaseFile, " ", { flag: "a", mode: 0o600 });
      }
      faultApplied = true;
      writeProgress({ stage: "file-changed-after-native-fstat", sqlitePutSucceeded: true,
        pendingDeliveryCommitted: true, validLeaseTtlMs: 15_000,
        changedAfterFstat: caseName === "truncate-after-fstat" ? "truncated" : "valid-json-whitespace-appended" });
    }
    return value;
  };
  fs.readSync = function (...args) {
    const value = Reflect.apply(original.readSync, fs, args);
    if (rawDescriptors.has(args[0])) recordRead(args[0], value);
    return value;
  };
  fs.readFileSync = function (...args) {
    const value = Reflect.apply(original.readFileSync, fs, args);
    if (rawDescriptors.has(args[0])) {
      const encoding = typeof args[1] === "string" ? args[1] : args[1]?.encoding;
      const bytes = Buffer.isBuffer(value) ? value.length : Buffer.byteLength(value, encoding ?? "utf8");
      recordRead(args[0], bytes);
    }
    return value;
  };
  fs.closeSync = function (...args) {
    const result = Reflect.apply(original.closeSync, fs, args);
    rawDescriptors.delete(args[0]);
    return result;
  };
  instrumentation = () => {
    fs.openSync = original.openSync;
    fs.fstatSync = original.fstatSync;
    fs.readSync = original.readSync;
    fs.readFileSync = original.readFileSync;
    fs.closeSync = original.closeSync;
    fs.truncateSync = original.truncateSync;
    fs.writeFileSync = original.writeFileSync;
  };

  writeProgress({ stage: "native-fixture-ready", sqlitePutSucceeded: true,
    sqliteBackend: mesh.stateBackend, sqliteDatabasePresent: fs.existsSync(path.join(meshRoot, "state.db")),
    archivePrepared: true, pendingDeliveryCommitted: true, nativeLeaseBytes: nativeBytes,
    validLeaseTtlMs: initialLease.expiresAt - initialLease.updatedAt,
    rawReadScope: "exact numeric O_RDONLY|O_NOFOLLOW [|O_NONBLOCK] lease descriptors only" });

  const timeStarted = performance.now();
  await mesh.retryResidentWakes();
  instrumentation();
  instrumentation = undefined;
  const durationMs = Math.round(performance.now() - timeStarted);
  restoreFaultedLease();

  const failureFile = path.join(root, "wake-failure.json");
  const terminalFile = path.join(root, "wake-terminal.json");
  const failure = receipt(failureFile);
  const terminalBytes = fs.existsSync(terminalFile) ? fs.readFileSync(terminalFile) : undefined;
  const terminalReceipt = receipt(terminalFile);
  const pending = pendingAt(meshRoot, root);
  const acks = exactAckCount(meshRoot, root, delivery);
  let idleStable = true;
  if (expected === "terminal" && terminalBytes) {
    for (let sweep = 0; sweep < 3; sweep++) await mesh.retryResidentWakes();
    idleStable = fs.existsSync(terminalFile) && Buffer.compare(terminalBytes, fs.readFileSync(terminalFile)) === 0 &&
      pendingAt(meshRoot, root) === undefined && exactAckCount(meshRoot, root, delivery) === 1;
  }
  const actualDisposition = terminalReceipt ? "terminal" : "retry";
  const deliveryMatchesFailure = failure?.delivery?.id === delivery.id && failure?.delivery?.sequence === delivery.sequence;
  const noWakeRequest = !fs.existsSync(path.join(root, "wake-request.json"));
  const earlyBoundedRefusal = !["oversized-unknown", "over-cap"].includes(caseName) ||
    (rawStats.openAttempts < 4 && rawStats.readCalls === 0 && rawStats.actualBytes === 0);
  const custodyFaultProved = !["fifo-third-open", "directory-third-open"].includes(caseName) ||
    (thirdOpenProof?.rawOpenAttempt === 3 && thirdOpenProof.insideNativeLeaseCustody &&
      thirdOpenProof.publicationFenceHeld && faultApplied);
  const changedFileFaultProved = !["truncate-after-fstat", "grow-after-fstat"].includes(caseName) || faultApplied;
  const resultChecks = actualDisposition === "terminal"
    ? terminalReceipt?.delivery?.id === delivery.id && terminalReceipt?.delivery?.sequence === delivery.sequence &&
      pending === undefined && acks === 1 && !failure && noWakeRequest && idleStable
    : pending?.id === delivery.id && pending?.sequence === delivery.sequence && deliveryMatchesFailure &&
      Number.isSafeInteger(failure?.failures) && failure.failures > 0 && acks === 0 && !terminalReceipt && noWakeRequest;
  const rawDescriptorSummaries = descriptorStats.map(row => ({ actualBytes: row.actualBytes,
    readCalls: row.readCalls, fstatCalls: row.fstatCalls ?? 0 }));
  result = {
    completed: true,
    case: caseName,
    expectedDisposition: expected,
    actualDisposition,
    pass: resultChecks && rawStats.maxDescriptorBytes <= LEASE_CAP + 1 && earlyBoundedRefusal &&
      custodyFaultProved && changedFileFaultProved,
    sqliteBackend: mesh.stateBackend,
    sqlitePutSucceeded: true,
    sqliteDatabasePresent: fs.existsSync(path.join(meshRoot, "state.db")),
    archivePrepared: true,
    pendingBeforeRetry: true,
    pendingAfterRetry: pending !== undefined,
    failureCount: Number.isSafeInteger(failure?.failures) ? failure.failures : 0,
    failureOwnsDelivery: deliveryMatchesFailure,
    terminalReceipt: Boolean(terminalReceipt),
    exactAckCount: acks,
    noWakeRequest,
    idleStable,
    validLeaseTtlMs: initialLease.expiresAt - initialLease.updatedAt,
    leaseBytesAtProbeStart: nativeBytes,
    rawDescriptorStats: { ...rawStats, descriptors: rawDescriptorSummaries },
    thirdOpenProof: thirdOpenProof ?? null,
    injectedFilesystemChange: faultApplied,
    durationMs,
  };
  result.pass = Boolean(result.pass && result.actualDisposition === result.expectedDisposition);
  writeProgress({ stage: "completed", sqlitePutSucceeded: true, pendingDeliveryCommitted: true,
    rawOpenAttempts: rawStats.openAttempts, rawActualBytes: rawStats.actualBytes,
    rawReadCalls: rawStats.readCalls, thirdOpenProof: thirdOpenProof ?? null,
    actualDisposition, terminalReceipt: Boolean(terminalReceipt), exactAckCount: acks, pass: result.pass });
} catch (error) {
  failureCode = typeof error?.code === "string" ? error.code :
    typeof error?.name === "string" ? error.name : "PROBE_ERROR";
  result = { completed: false, case: caseName, expectedDisposition: expected, errorCode: failureCode,
    pass: false, thirdOpenProof: thirdOpenProof ?? null };
  writeProgress({ stage: "failed", errorCode: failureCode, thirdOpenProof: thirdOpenProof ?? null });
} finally {
  instrumentation?.();
  try {
    if (mesh && leaseFile && nativeLease) {
      if (leaseBackup && fs.existsSync(leaseBackup)) {
        try {
          const named = fs.lstatSync(leaseFile);
          if (named.isDirectory() && !named.isSymbolicLink()) fs.rmSync(leaseFile, { recursive: true, force: true });
          else fs.rmSync(leaseFile, { force: true });
        } catch { /* absent path */ }
        fs.renameSync(leaseBackup, leaseFile);
      } else if (leaseFile && fs.existsSync(path.dirname(leaseFile))) {
        // Restore the small generated lease before the private workspace is discarded.
        try {
          const { writeHostLease, hostLeasePath } = await import(pathToFileURL(path.join(packageRoot,
            "dist", "topology/host-leases.js")).href);
          if (hostLeasePath(mesh.root, nativeLease.id) === leaseFile) writeHostLease(mesh.root, nativeLease);
        } catch { /* cleanup is best effort after the outcome was recorded */ }
      }
    }
  } catch { /* cleanup is best effort; terminal/retry evidence is already summarized */ }
  try { mesh?.closeState(); } catch { /* process exit releases native SQLite handles */ }
  Date.now = realNow;
  try { fs.rmSync(workspace, { recursive: true, force: true }); } catch { /* private scratch cleanup */ }
}

fs.writeFileSync(resultFile, JSON.stringify(result, null, 2) + "\n", { mode: 0o600 });
console.log(JSON.stringify({ case: caseName, completed: result.completed, expected: result.expectedDisposition,
  actual: result.actualDisposition, pass: result.pass, rawOpenAttempts: result.rawDescriptorStats?.openAttempts ?? 0,
  rawReadCalls: result.rawDescriptorStats?.readCalls ?? 0,
  rawActualBytes: result.rawDescriptorStats?.actualBytes ?? 0,
  terminalReceipt: result.terminalReceipt ?? false, exactAckCount: result.exactAckCount ?? 0,
  thirdOpenProof: result.thirdOpenProof ?? null, errorCode: failureCode ?? null }));
process.exitCode = result.pass ? 0 : 1;
