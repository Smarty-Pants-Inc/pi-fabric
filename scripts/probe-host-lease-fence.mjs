#!/usr/bin/env node
// Linux, foreground, compiled-bundle SIGSTOP proofs for #7313 / PR #742 Round 3.
// Run after `bun run build`: node scripts/probe-host-lease-fence.mjs
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { fork, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = fileURLToPath(import.meta.url), project = path.resolve(path.dirname(here), "..");
const dist = path.join(project, "dist"), chunks = path.join(dist, "chunks");
assert.equal(process.platform, "linux", "SIGSTOP and kernel identity probes require Linux");
async function compiledSymbol(name) {
  for (const file of fs.readdirSync(chunks).filter(file => file.endsWith(".js"))) {
    const source = fs.readFileSync(path.join(chunks, file), "utf8");
    const exports = source.match(/export\s*\{([^}]*)\}/)?.[1];
    if (!exports?.split(/[,\s]+/).includes(name)) continue;
    const module = await import(pathToFileURL(path.join(chunks, file)).href);
    if (module[name]) return module[name];
  }
  throw new Error(`Compiled export not found: ${name}`);
}
const { MeshStore } = await import(pathToFileURL(path.join(dist, "mesh.js")).href);
const [ParticipantDirectory, renewHostLease, writeHostLease, hostLeasePath, readHostLeaseCurrent, readParticipantFiles] =
  await Promise.all(["ParticipantDirectory", "renewHostLease", "writeHostLease", "hostLeasePath", "readHostLeaseCurrent", "readParticipantFiles"].map(compiledSymbol));
const hostId = "compiled-r3-host", identity = { id: "compiled-r3-owner", name: "probe", kind: "agent" };
const options = { enabled: true, hostId, rootId: "compiled-r3-root", identity,
  reapDeadHosts: false, heartbeatMs: 60_000, leaseMs: 120_000 };
const candidate = { format: 1, id: "compiled-r3-agent", kind: "agent", rootId: options.rootId,
  ownerHostId: hostId, ownerIdentityId: identity.id, name: "candidate", status: "running", runner: "pi",
  transport: "process", capabilities: [], startedAt: 1, updatedAt: 1, controlProtocol: "v1" };
const message = value => process.send?.(value);
const pause = (point) => { message({ type: "paused", point }); process.kill(process.pid, "SIGSTOP"); };
const [mode, root, backend, filesPolicy] = process.argv.slice(2);

if (mode?.startsWith("child-")) {
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend: backend });
  assert.equal(mesh.stateBackend, backend);
  let directory;
  try {
    if (mode === "child-after-renew") {
      if (filesPolicy === "files-only") await mesh.put({ key: "topology/liveness",
        value: { version: 1, hostLeases: "files", participants: "files" }, identity });
      directory = new ParticipantDirectory(mesh, options); await directory.start();
      const custody = mesh.leaseCustody.bind(mesh); let armed = true;
      mesh.leaseCustody = async (file, operation, timeout, prepared) => {
        if (armed && operation.constructor.name === "AsyncFunction") { armed = false; pause("after-renew-before-owned-batch"); }
        return custody(file, operation, timeout, prepared);
      };
      directory.registerSource(() => [candidate]);
      const error = await directory.refresh().then(() => undefined, error => error);
      message({ type: "result", code: error?.code, canConsume: directory.canConsumeMesh() });
    } else if (mode === "child-held-commit") {
      const lease = JSON.parse(fs.readFileSync(path.join(root, "probe-input.json"), "utf8"));
      await renewHostLease(mesh, lease, { claim: true });
      await mesh.leaseCustody(hostLeasePath(root, hostId), () => pause("holding-physical-commit-gate"));
      message({ type: "result", code: "gate-released" });
    } else if (mode === "child-legacy") {
      const legacy = JSON.parse(fs.readFileSync(path.join(root, "probe-input.json"), "utf8"));
      pause("legacy-writer-before-unfenced-overwrite");
      writeHostLease(root, legacy); // Deliberately models an old release that ignores new custody.
      message({ type: "result", code: "legacy-overwritten" });
    } else throw new Error(`Unknown child mode: ${mode}`);
  } finally { await directory?.close(); mesh.closeState(); process.disconnect?.(); }
} else {
  const base = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "compiled-host-lease-r3-"));
  const children = new Set();
  const prepareRoot = (root, backend) => {
    fs.mkdirSync(root, { mode: 0o700 });
    if (backend !== "sqlite") return;
    // Exercise production admission; never set the test-only sqlite initializer symbol.
    for (const command of ["import", "cutover"]) {
      const result = spawnSync(process.execPath, [path.join(project, "bin/fabric-mesh-backend"), command, "--root", root],
        { encoding: "utf8", timeout: 10_000 });
      assert.equal(result.status, 0, `${command}: ${result.stdout} ${result.stderr}`);
    }
  };
  const child = (mode, root, backend, filesOnly = false) => {
    const process = fork(here, [mode, root, backend, filesOnly ? "files-only" : "compat"], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
    children.add(process);
    let stdout = "", stderr = "";
    process.stdout.on("data", chunk => { stdout += chunk; }); process.stderr.on("data", chunk => { stderr += chunk; });
    const queue = [], waiters = [];
    const deadline = setTimeout(() => { process.kill("SIGCONT"); process.kill("SIGKILL"); }, 15_000);
    process.on("message", value => { queue.push(value); for (const wake of [...waiters]) wake(); });
    const closed = new Promise(resolve => {
      process.once("error", error => resolve({ error, stdout, stderr }));
      process.once("close", (code, signal) => { clearTimeout(deadline); children.delete(process); resolve({ code, signal, stdout, stderr }); });
    });
    const next = type => new Promise((resolve, reject) => {
      let timer;
      const wake = () => {
        const index = queue.findIndex(value => value.type === type);
        if (index < 0) return;
        clearTimeout(timer); waiters.splice(waiters.indexOf(wake), 1); resolve(queue.splice(index, 1)[0]);
      };
      timer = setTimeout(() => { waiters.splice(waiters.indexOf(wake), 1); reject(new Error(`Missing child ${type}: ${stderr}`)); }, 10_000);
      waiters.push(wake); wake();
    });
    process.joined = closed;
    return { process, next, closed, stop: async () => { process.kill("SIGCONT"); process.kill("SIGKILL"); await closed; } };
  };
  const joined = async run => {
    const result = await run.closed; assert.equal(result.code, 0, JSON.stringify(result));
  };
  const stopped = run => {
    // Force/confirm the native pause once, never infer pause from a message alone.
    process.kill(run.process.pid, "SIGSTOP");
    const stat = fs.readFileSync(`/proc/${run.process.pid}/stat`, "utf8");
    assert.equal(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0], "T");
  };
  const report = value => console.log(JSON.stringify({ compiled: true, ...value }));
  try {
    for (const backend of ["file", "sqlite"]) {
      for (const filesOnly of [false, true]) {
        const root = path.join(base, `${backend}-after-renew-${filesOnly}`); prepareRoot(root, backend);
        const run = child("child-after-renew", root, backend, filesOnly), mesh = new MeshStore(root, 65_536, 100, { stateBackend: backend });
        try {
          const paused = await run.next("paused"); stopped(run);
          const before = mesh.listAll("", { fresh: true }), predecessor = readHostLeaseCurrent(root, hostId);
          const successor = { ...predecessor, incarnationToken: randomUUID() };
          await renewHostLease(mesh, successor, { claim: true });
          process.kill(run.process.pid, "SIGCONT"); const result = await run.next("result"); await joined(run);
          assert.equal(result.code, "FABRIC_HOST_LEASE_SUPERSEDED"); assert.equal(result.canConsume, false);
          assert.deepEqual(mesh.listAll("", { fresh: true }), before); assert.deepEqual(readParticipantFiles(root), []);
          assert.deepEqual(readHostLeaseCurrent(root, hostId), successor);
          report({ backend, filesOnly, case: paused.point, verdict: "PASS", code: result.code, stateUnchanged: true, successorPreserved: true });
        } finally { await run.stop(); mesh.closeState(); }
      }
      {
        const root = path.join(base, `${backend}-held-commit`); prepareRoot(root, backend);
        const lease = { id: hostId, rootId: options.rootId, identityId: identity.id, incarnationToken: randomUUID(),
          startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000 };
        fs.writeFileSync(path.join(root, "probe-input.json"), JSON.stringify(lease));
        const run = child("child-held-commit", root, backend), mesh = new MeshStore(root, 65_536, 100, { stateBackend: backend });
        try {
          const paused = await run.next("paused"); stopped(run);
          const successor = { ...lease, incarnationToken: randomUUID() };
          const error = await renewHostLease(mesh, successor, { claim: true, timeoutMs: 1_000 }).then(() => undefined, error => error);
          assert.equal(error?.code, "FABRIC_MESH_LOCK_TIMEOUT"); assert.deepEqual(readHostLeaseCurrent(root, hostId), lease);
          process.kill(run.process.pid, "SIGCONT"); await run.next("result"); await joined(run);
          await renewHostLease(mesh, successor, { claim: true }); assert.deepEqual(readHostLeaseCurrent(root, hostId), successor);
          report({ backend, case: paused.point, verdict: "PASS", stoppedHolderNotStolen: true, resumedGateJoined: true });
        } finally { await run.stop(); mesh.closeState(); }
      }
      {
        const root = path.join(base, `${backend}-legacy`); prepareRoot(root, backend);
        const legacy = { id: hostId, rootId: options.rootId, identityId: identity.id, startedAt: 1,
          updatedAt: Date.now(), expiresAt: Date.now() + 120_000 };
        fs.writeFileSync(path.join(root, "probe-input.json"), JSON.stringify(legacy));
        const run = child("child-legacy", root, backend), mesh = new MeshStore(root, 65_536, 100, { stateBackend: backend });
        const directory = new ParticipantDirectory(mesh, options);
        try {
          const paused = await run.next("paused"); stopped(run); await directory.start();
          const before = mesh.listAll("", { fresh: true });
          process.kill(run.process.pid, "SIGCONT"); await run.next("result"); await joined(run);
          const error = await directory.refresh().then(() => undefined, error => error);
          assert.equal(error?.code, "FABRIC_HOST_LEASE_CONTESTED"); assert.equal(directory.canConsumeMesh(), false);
          assert.deepEqual(mesh.listAll("", { fresh: true }), before); assert.deepEqual(readHostLeaseCurrent(root, hostId), legacy);
          report({ backend, case: paused.point, verdict: "PASS", code: error.code, stateUnchanged: true });
        } finally { await run.stop(); await directory.close(); mesh.closeState(); }
      }
      {
        const root = path.join(base, `${backend}-fresh-legacy`); prepareRoot(root, backend);
        const mesh = new MeshStore(root, 65_536, 100, { stateBackend: backend });
        try {
          const legacy = { id: hostId, rootId: options.rootId, identityId: identity.id, startedAt: 1,
            updatedAt: Date.now(), expiresAt: Date.now() + 30 };
          writeHostLease(root, legacy); const successor = { ...legacy, incarnationToken: randomUUID(), expiresAt: Date.now() + 120_000 };
          const error = await renewHostLease(mesh, successor, { claim: true, timeoutMs: 0 }).then(() => undefined, error => error);
          assert.equal(error?.code, "FABRIC_HOST_LEASE_LEGACY_BUSY"); assert.deepEqual(readHostLeaseCurrent(root, hostId), legacy);
          await renewHostLease(mesh, successor, { claim: true }); assert.deepEqual(readHostLeaseCurrent(root, hostId), successor);
          report({ backend, case: "fresh-legacy-one-expiry-wake", verdict: "PASS", code: error.code });
        } finally { mesh.closeState(); }
      }
    }
  } finally {
    // Every native child is resumed/stopped and joined before this foreground script returns.
    for (const child of children) { child.kill("SIGCONT"); child.kill("SIGKILL"); await child.joined; }
    fs.rmSync(base, { recursive: true, force: true });
  }
}
