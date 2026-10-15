import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { RESIDENT_DRAIN_ABI, residentDrainReceiptPath, type ResidentDrainIntent,
  type ResidentDrainReceipt as EndpointDrainReceipt } from "../src/residency/drain.js";
import { mainGenerationPath, residentLaunchSpec, type ResidentMainGeneration } from "../src/residency/handover.js";
import { hostLeasePath } from "../src/topology/host-leases.js";
import type { ResidentCommandResponse, ResidentHostConfig } from "../src/residency/protocol.js";
import { main, type ResidentDrainReceipt, type ResidentDrainDependencies } from "../src/resident-drain-cli.js";
import { residentHostId, residentRoot } from "../src/residency/protocol.js";
import { processStartTime } from "../src/residency/process-identity.js";
import type { ResidentChannelOwnerProbe } from "../src/actors-cli.js";
import type { AttemptLaunchReceipt } from "../src/residency/attempt-containment-contract.js";

// Keep reason: data safety + OS-user/release/process trust boundaries. These
// fixtures exercise the operator entrypoint but never signal/stop a native child.
// Deliberately retained on disk: no cleanup/deletion runs in this suite.
function fixture() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-drain-preflight-")));
  const meshRoot = path.join(root, "mesh");
  const rootId = "test-root";
  const directory = residentRoot(meshRoot, rootId);
  const expected = path.join(root, "old");
  const target = path.join(root, "new");
  fs.mkdirSync(path.join(expected, "dist/residency"), { recursive: true });
  fs.mkdirSync(target);
  fs.writeFileSync(path.join(expected, "dist/index.js"), "// retained fixture\n");
  fs.writeFileSync(path.join(expected, "dist/residency/pi-entry.js"), "// retained fixture\n");
  fs.mkdirSync(path.join(directory, "requests"), { recursive: true });
  fs.mkdirSync(path.join(directory, "responses"));
  fs.mkdirSync(path.join(directory, "actors"));
  fs.writeFileSync(path.join(directory, "requests/pending.json"), '{"accepted":"keep"}\n');
  fs.writeFileSync(path.join(directory, "actors/queue.json"), '["keep queued work"]\n');
  fs.writeFileSync(path.join(directory, "watchdog-custody.json"), '{"custody":"keep"}\n');
  const config: Record<string, unknown> = { format: 1, rootId, meshRoot, residencyRoot: directory,
    // Desired release can differ: it is NOT evidence of the loaded release.
    fabricExtensionPath: path.join(target, "dist/index.js") };
  const birth = processStartTime(process.pid);
  expect(birth).toMatch(/^\d+$/);
  const owner: Record<string, unknown> = { format: 1, hostId: residentHostId(rootId), token: "fixture-owner",
    pid: process.pid, processStartTime: birth, releaseRoot: expected,
    fabricExtensionPath: path.join(expected, "dist/index.js"), configDigest: "a".repeat(64),
    handover: { abi: "fabric-resident-1", launcher: { pid: process.pid, processStartTime: birth,
      token: "fixture-launcher", entry: path.join(expected, "dist/residency/pi-entry.js"),
      runtime: fs.realpathSync(process.execPath) } } };
  const save = () => {
    fs.writeFileSync(path.join(directory, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(directory, "owner.json"), JSON.stringify(owner));
  };
  save();
  const argv = ["--resident", directory, "--mesh-root", meshRoot, "--expected-release", expected, "--target-release", target];
  return { root, rootId, directory, meshRoot, expected, target, config, owner, save, argv };
}

// Capture names, contents, inode, birth/mtime/ctime and modes without modifying the
// fixture. Reads may update atime, which is deliberately not an asserted mutation.
function snapshot(root: string): unknown[] {
  const entries: unknown[] = [];
  const walk = (directory: string) => {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name);
      const stat = fs.lstatSync(file);
      entries.push([path.relative(root, file), stat.ino, stat.mode, stat.mtimeMs, stat.ctimeMs,
        stat.isSymbolicLink() ? fs.readlinkSync(file) : stat.isFile() ? fs.readFileSync(file).toString("base64") : null]);
      if (stat.isDirectory()) walk(file);
    }
  };
  walk(root);
  return entries;
}

async function invoke(argv: string[], probe?: ResidentChannelOwnerProbe) {
  const stdout: string[] = [];
  const exit = await main(argv, { out: text => stdout.push(text) }, probe);
  expect(stdout).toHaveLength(1);
  const receipt = JSON.parse(stdout[0]!) as ResidentDrainReceipt;
  expect(receipt).toMatchObject({ phase: "preflight", unchanged: true, transitionAttempted: false,
    rollback: { status: "not-needed", reason: "no-transition-attempted" } });
  expect(receipt.elapsedMs).toBeGreaterThanOrEqual(0);
  expect(Number.isFinite(receipt.elapsedMs)).toBe(true);
  return { exit, receipt };
}

async function refusal(f: ReturnType<typeof fixture>, code: string, argv = f.argv, probe?: ResidentChannelOwnerProbe) {
  const before = snapshot(f.root);
  const result = await invoke(argv, probe);
  expect(result.exit).not.toBe(0);
  const explicitMesh = argv[3]!;
  expect(result.receipt).toMatchObject({ ok: false, outcome: "refused", code, resident: path.relative(explicitMesh, argv[1]!) || ".",
    expected: path.relative(explicitMesh, f.expected), target: path.relative(explicitMesh, f.target) });
  expect(JSON.stringify(result.receipt)).not.toContain(String(f.owner.token));
  expect(JSON.stringify(result.receipt)).not.toContain("fixture-launcher");
  expect(JSON.stringify(result.receipt)).not.toContain(f.root);
  expect(result.receipt.reason.length).toBeGreaterThan(0);
  expect(snapshot(f.root)).toEqual(before);
  return result.receipt;
}

function negotiatedFixture() {
  const f = fixture();
  fs.mkdirSync(path.join(f.target, "dist/residency"), { recursive: true });
  fs.writeFileSync(path.join(f.expected, "dist/worker.js"), "// retained old worker\n");
  fs.writeFileSync(path.join(f.target, "dist/index.js"), "// retained new extension\n");
  fs.writeFileSync(path.join(f.target, "dist/worker.js"), "// retained new worker\n");
  fs.writeFileSync(path.join(f.target, "dist/residency/pi-entry.js"), "// retained new entry\n");
  const loaded: ResidentHostConfig = { format: 1, rootId: f.rootId, sessionId: "existing-main-session",
    cwd: f.root, projectRoot: f.root, meshRoot: f.meshRoot, actorRoot: path.join(f.root, "actors"),
    sessionActorRoot: path.join(f.root, "session-actors"), residencyRoot: f.directory, fullCodeMode: true,
    agents: structuredClone(DEFAULT_FABRIC_CONFIG.agents), mesh: structuredClone(DEFAULT_FABRIC_CONFIG.mesh),
    retention: structuredClone(DEFAULT_FABRIC_CONFIG.retention), piBinary: fs.realpathSync(process.execPath),
    claudeBinary: "drain-fixture-missing-claude", vedaBinary: "drain-fixture-missing-veda",
    fabricExtensionPath: path.join(f.expected, "dist/index.js"), workerPath: path.join(f.expected, "dist/worker.js") };
  const previous = residentLaunchSpec(loaded, path.join(f.expected, "dist/residency/pi-entry.js"));
  const snapshotPath = path.join(f.directory, `launch-${previous.digest}.json`);
  fs.writeFileSync(snapshotPath, JSON.stringify(previous.config));
  Object.assign(f.owner, { configDigest: previous.digest, requestFence: 1, commands: ["releaseDrain"], drain: { abi: RESIDENT_DRAIN_ABI } });
  // Mutable desired config deliberately diverges. Only immutable old config is
  // authority for session identity, pins and topology when constructing target.
  Object.assign(f.config, { ...loaded, sessionId: "untrusted-desired-session", actorRoot: path.join(f.root, "desired-actors"),
    fabricExtensionPath: path.join(f.target, "dist/index.js"), workerPath: path.join(f.target, "dist/worker.js") });
  f.save();
  const mainGeneration: ResidentMainGeneration = { nonce: "existing-main-nonce-not-minted-by-cli", pid: process.pid,
    processStartTime: processStartTime(process.pid)!, rootId: f.rootId, sessionId: loaded.sessionId, releaseRoot: f.target };
  fs.writeFileSync(mainGenerationPath(f.directory), JSON.stringify(mainGeneration));
  const hash = createHash("sha256").update(f.rootId).digest("hex");
  const participantPath = path.join(f.meshRoot, "participants", `${hash}.json`);
  fs.mkdirSync(path.dirname(participantPath), { recursive: true });
  const identity = { id: f.rootId, name: "Existing Main", kind: "main" as const, sessionId: loaded.sessionId };
  const now = Date.now();
  const participant = { format: 1, key: `topology/participants/${hash}`, version: 1, updatedAt: now, updatedBy: identity,
    value: { format: 1, id: f.rootId, rootId: f.rootId, sessionId: loaded.sessionId, ownerHostId: f.rootId, ownerIdentityId: f.rootId,
      kind: "root", name: identity.name, status: "idle", runner: "pi", transport: "host", capabilities: ["fabric"],
      mainProcess: { pid: process.pid, host: os.hostname(), startTime: mainGeneration.processStartTime },
      startedAt: now, updatedAt: now, controlProtocol: "v1" } };
  fs.writeFileSync(participantPath, JSON.stringify(participant));
  const leasePath = hostLeasePath(f.meshRoot, f.rootId);
  fs.mkdirSync(path.dirname(leasePath), { recursive: true });
  const lease = { format: 1, id: f.rootId, rootId: f.rootId, identityId: f.rootId, updatedAt: now,
    expiresAt: now + 60_000, session: { id: loaded.sessionId, startedAt: now, updatedAt: now, expiresAt: now + 60_000 } };
  fs.writeFileSync(leasePath, JSON.stringify(lease));
  return { ...f, previous, snapshotPath, mainGeneration, participant, participantPath, lease, leasePath };
}

type NegotiatedFixture = ReturnType<typeof negotiatedFixture>;
function advertiseLaunch(f: ReturnType<typeof fixture>) {
  const launch: AttemptLaunchReceipt = { attemptId: randomUUID(), scope: "fixture-launch.scope",
    cgroupPath: path.join(f.root, "private-cgroup"), launchedInScope: true, at: Date.now() - 10 };
  const record: Record<string, unknown> = { ...launch };
  const file = path.join(f.directory, `attempt-launch-${launch.attemptId}.json`);
  const saveRecord = () => fs.writeFileSync(file, JSON.stringify(record), { mode: 0o600 });
  f.owner.launchAttestation = launch; f.save(); saveRecord();
  return { launch, record, file, saveRecord };
}
function initialDrain(f: NegotiatedFixture, intent: ResidentDrainIntent): EndpointDrainReceipt {
  const now = Date.now();
  return { abi: RESIDENT_DRAIN_ABI, id: randomUUID(), rootId: f.rootId, owner: intent.expected.owner,
    launcher: intent.expected.launcher, previous: f.previous, target: intent.target, main: intent.main,
    phase: "held", outcome: "held", code: "DRAIN_HELD", reason: "Admission held for cooperative checkpoint",
    at: now, deadlineAt: now + intent.timeoutMs!, admissionHeld: true };
}
function resumedDrain(initial: EndpointDrainReceipt): EndpointDrainReceipt {
  return { ...initial, phase: "checkpointed", outcome: "containment-unavailable", code: "CONTAINMENT_UNAVAILABLE",
    reason: "Checkpoint read back; A resumed. Complete attempt containment unavailable; no A exit or B launch.",
    terminal: true, admissionHeld: false, checkpointReadback: true, at: Date.now(), resumedAt: Date.now() };
}
async function negotiatedInvoke(f: NegotiatedFixture, releaseDrain: (intent: ResidentDrainIntent, signal?: AbortSignal) => Promise<ResidentCommandResponse>, timeoutMs = 2_000) {
  const stdout: string[] = [];
  const client = vi.fn(() => ({ releaseDrain }));
  const dependencies: ResidentDrainDependencies = { client, timeoutMs };
  const exit = await main(f.argv, { out: text => stdout.push(text) }, undefined, dependencies);
  expect(stdout).toHaveLength(1);
  expect(stdout[0]).not.toContain("fixture-owner");
  expect(stdout[0]).not.toContain("fixture-launcher");
  expect(stdout[0]).not.toContain(f.mainGeneration.nonce);
  expect(stdout[0]).not.toContain(f.root);
  return { exit, receipt: JSON.parse(stdout[0]!) as ResidentDrainReceipt, client };
}
function response(drain: EndpointDrainReceipt): ResidentCommandResponse {
  return { format: 1, requestId: "recorded-call", ok: true, completedAt: Date.now(), drain };
}

// No host startup, process launch, signals or cleanup. The endpoint seam records
// requests and publishes retained durable receipts on the real fs.watch path.
describe.skipIf(process.platform !== "linux")("fabric-resident-drain negotiated cooperative endpoint", () => {
  it("holds then watches the exact durable terminal receipt, returns nonzero partial delivery, and preserves existing Main identity", async () => {
    const f = negotiatedFixture();
    const mainBefore = fs.readFileSync(mainGenerationPath(f.directory), "utf8");
    const participantBefore = fs.readFileSync(f.participantPath, "utf8");
    const send = vi.fn(async (intent: ResidentDrainIntent) => {
      expect(intent.main).toEqual(f.mainGeneration);
      expect(intent.caller.identity).toEqual(f.participant.updatedBy);
      expect(intent.expected.configDigest).toBe(f.previous.digest);
      expect(intent.target.config.sessionId).toBe(f.previous.config.sessionId);
      expect(intent.target.config.actorRoot).toBe(f.previous.config.actorRoot);
      expect(intent.target.config.sessionActorRoot).toBe(f.previous.config.sessionActorRoot);
      expect(intent.target.config.piBinary).toBe(f.previous.config.piBinary);
      expect(intent.target.releaseRoot).toBe(f.target);
      const initial = initialDrain(f, intent);
      fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(initial));
      setImmediate(() => {
        // Unrelated receipt cannot replace the response-correlated operation.
        fs.writeFileSync(residentDrainReceiptPath(f.directory, randomUUID()), JSON.stringify({ ...resumedDrain(initial), id: randomUUID() }));
        fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(resumedDrain(initial)));
      });
      return response(initial);
    });
    const { exit, receipt } = await negotiatedInvoke(f, send);
    expect(send).toHaveBeenCalledTimes(1);
    expect(exit).toBe(1);
    expect(receipt).toMatchObject({ ok: false, phase: "checkpointed", outcome: "containment-unavailable", code: "CONTAINMENT_UNAVAILABLE",
      unchanged: false, transitionAttempted: true, drain: { terminal: true, admissionHeld: false, checkpointReadback: true },
      rollback: { status: "not-attempted", reason: "no-target-launch-authorized" } });
    expect(fs.readFileSync(mainGenerationPath(f.directory), "utf8")).toBe(mainBefore);
    expect(fs.readFileSync(f.participantPath, "utf8")).toBe(participantBefore);
    expect(fs.readdirSync(path.join(f.directory, "requests"))).toEqual(["pending.json"]);
  });

  it("compares an advertised owner receipt with the authentic launcher record without inventing remote custody", async () => {
    const f = negotiatedFixture(); const attestation = advertiseLaunch(f);
    const send = vi.fn(async (intent: ResidentDrainIntent) => {
      expect(intent).not.toHaveProperty("launchAttestation");
      expect(intent.expected).not.toHaveProperty("launchAttestation");
      const initial = initialDrain(f, intent);
      fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(resumedDrain(initial)));
      return response(initial);
    });
    const result = await negotiatedInvoke(f, send);
    expect(send).toHaveBeenCalledTimes(1);
    expect(result.receipt).toMatchObject({ code: "CONTAINMENT_UNAVAILABLE", outcome: "containment-unavailable",
      transitionAttempted: true, drain: { checkpointReadback: true, admissionHeld: false } });
    expect(JSON.stringify(result.receipt)).not.toContain(attestation.launch.cgroupPath);
    expect(result.receipt).not.toHaveProperty("launchAttestation");
    expect(result.receipt.drain).not.toHaveProperty("launchAttestation");
    expect(fs.readFileSync(attestation.file, "utf8")).toBe(JSON.stringify(attestation.record));
  });

  it("refuses every advertised launcher-record mismatch or absence without dispatch or effects", async () => {
    for (const field of ["missing", "attemptId", "scope", "cgroupPath", "launchedInScope", "at", "extra", "symlink", "mode"] as const) {
      const f = negotiatedFixture(); const a = advertiseLaunch(f);
      if (field === "missing") fs.renameSync(a.file, `${a.file}.retained`);
      else if (field === "symlink") { fs.renameSync(a.file, `${a.file}.retained`); fs.symlinkSync(`${a.file}.retained`, a.file); }
      else if (field === "mode") fs.chmodSync(a.file, 0o644);
      else {
        if (field === "extra") a.record.token = "private-record-token";
        else a.record[field] = field === "at" ? 1 : field === "launchedInScope" ? false : "mismatched-field";
        a.saveRecord();
      }
      const before = snapshot(f.root); const send = vi.fn();
      const result = await negotiatedInvoke(f, send);
      expect(result.receipt).toMatchObject({ phase: "preflight", outcome: "refused", code: "CONTAINMENT_UNAVAILABLE",
        unchanged: true, transitionAttempted: false });
      expect(result.client).not.toHaveBeenCalled(); expect(send).not.toHaveBeenCalled();
      expect(JSON.stringify(result.receipt)).not.toContain("private-record-token");
      expect(snapshot(f.root)).toEqual(before);
    }
  });

  it("does not infer custody from an orphan record when an uncontained cooperative owner advertises none", async () => {
    const f = negotiatedFixture(); advertiseLaunch(f); delete f.owner.launchAttestation; f.save();
    const result = await negotiatedInvoke(f, async intent => {
      const initial = initialDrain(f, intent);
      fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(resumedDrain(initial)));
      return response(initial);
    });
    expect(result.receipt).toMatchObject({ outcome: "containment-unavailable", unchanged: false, transitionAttempted: true,
      drain: { checkpointReadback: true, admissionHeld: false } });
  });

  it("classifies a lost or replaced advertised attestation after negotiation as needs-operator", async () => {
    for (const fault of ["missing", "replaced", "owner-removed", "both-rebound"] as const) {
      const f = negotiatedFixture(); const a = advertiseLaunch(f);
      const result = await negotiatedInvoke(f, async intent => {
        const initial = initialDrain(f, intent);
        fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(initial));
        setImmediate(() => {
          if (fault === "missing") fs.renameSync(a.file, `${a.file}.retained`);
          if (fault === "replaced") { a.record.scope = "replacement.scope"; a.saveRecord(); }
          if (fault === "owner-removed") { delete f.owner.launchAttestation; f.save(); }
          if (fault === "both-rebound") { a.launch.scope = "replacement.scope"; a.record.scope = a.launch.scope; a.saveRecord(); f.save(); }
          fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(resumedDrain(initial)));
        });
        return response(initial);
      });
      expect(result.receipt).toMatchObject({ phase: "needs-operator", outcome: "needs-operator", code: "DRAIN_OUTCOME_UNKNOWN",
        unchanged: false, transitionAttempted: true });
    }
  });

  it("refuses wrong expected immutable release before invoking any client", async () => {
    const f = negotiatedFixture(); f.owner.releaseRoot = f.target; f.save();
    const before = snapshot(f.root);
    const send = vi.fn();
    const result = await negotiatedInvoke(f, send);
    expect(result.receipt).toMatchObject({ code: "EXPECTED_RELEASE_MISMATCH", unchanged: true, transitionAttempted: false });
    expect(result.client).not.toHaveBeenCalled(); expect(send).not.toHaveBeenCalled();
    expect(snapshot(f.root)).toEqual(before);
  });

  it("keeps every unsupported/partial capability on exact read-only legacy refusal", async () => {
    for (const patch of [{ drain: undefined }, { drain: { abi: "future-drain" } }, { commands: [] },
      { commands: undefined }, { requestFence: undefined }]) {
      const f = negotiatedFixture(); Object.assign(f.owner, patch); f.save();
      const before = snapshot(f.root); const send = vi.fn();
      const result = await negotiatedInvoke(f, send);
      expect(result.receipt).toMatchObject({ phase: "preflight", outcome: "refused", code: "DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE",
        unchanged: true, transitionAttempted: false, rollback: { status: "not-needed", reason: "no-transition-attempted" } });
      expect(result.receipt.reason).toContain("digest not recomputed/validated");
      expect(result.client).not.toHaveBeenCalled(); expect(send).not.toHaveBeenCalled();
      expect(snapshot(f.root)).toEqual(before);
    }
  });

  it("refuses missing, stale, remote or non-owning Main without minting identity or dispatching", async () => {
    for (const fault of ["missing", "stale", "remote", "wrong-writer", "wrong-birth", "expired-session", "reloading"] as const) {
      const f = negotiatedFixture();
      if (fault === "missing") fs.renameSync(mainGenerationPath(f.directory), path.join(f.directory, "retained-main-generation.json"));
      if (fault === "stale") fs.writeFileSync(mainGenerationPath(f.directory), JSON.stringify({ ...f.mainGeneration, processStartTime: "0" }));
      if (fault === "remote") Object.assign(f.participant.value, { remoteHost: "other-host" });
      if (fault === "wrong-writer") f.participant.updatedBy.id = "other-main";
      if (fault === "wrong-birth") f.participant.value.mainProcess.startTime = "0";
      if (fault === "reloading") f.participant.value.status = "reloading";
      if (fault === "expired-session") { f.lease.session.expiresAt = Date.now() - 1; fs.writeFileSync(f.leasePath, JSON.stringify(f.lease)); }
      fs.writeFileSync(f.participantPath, JSON.stringify(f.participant));
      const before = snapshot(f.root); const send = vi.fn();
      const result = await negotiatedInvoke(f, send);
      expect(result.receipt).toMatchObject({ code: "MAIN_AUTHORITY_UNAVAILABLE", unchanged: true, transitionAttempted: false });
      expect(result.client).not.toHaveBeenCalled(); expect(send).not.toHaveBeenCalled();
      expect(snapshot(f.root)).toEqual(before);
    }
  });

  it("accepts the actual owning Main on A as well as B, but refuses any third release without effects", async () => {
    const f = negotiatedFixture();
    f.mainGeneration.releaseRoot = f.expected;
    fs.writeFileSync(mainGenerationPath(f.directory), JSON.stringify(f.mainGeneration));
    const result = await negotiatedInvoke(f, async intent => {
      expect(intent.main.releaseRoot).toBe(f.expected);
      const initial = initialDrain(f, intent);
      fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(resumedDrain(initial)));
      return response(initial);
    });
    expect(result.receipt).toMatchObject({ outcome: "containment-unavailable", unchanged: false, transitionAttempted: true });
    const other = negotiatedFixture();
    fs.writeFileSync(mainGenerationPath(other.directory), JSON.stringify({ ...other.mainGeneration, releaseRoot: other.root }));
    const before = snapshot(other.root); const send = vi.fn();
    const refused = await negotiatedInvoke(other, send);
    expect(refused.receipt).toMatchObject({ code: "MAIN_AUTHORITY_UNAVAILABLE", unchanged: true, transitionAttempted: false });
    expect(send).not.toHaveBeenCalled(); expect(snapshot(other.root)).toEqual(before);
  });

  it("does not disclose private durable owner/launcher tokens, config or absolute paths", async () => {
    const f = negotiatedFixture();
    const secret = "retained-private-config-not-public";
    // Endpoint private fields need not be traversed or serialized for public JSON.
    const result = await negotiatedInvoke(f, async intent => {
      const initial = initialDrain(f, intent);
      initial.reason = `private owner=${intent.expected.owner.token} launcher=${intent.expected.launcher.token} path=${f.root}`;
      const terminal = resumedDrain(initial);
      terminal.reason = initial.reason;
      terminal.checkpoint = { private: secret, token: intent.expected.owner.token, root: f.root };
      fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(terminal));
      return response(initial);
    });
    expect(result.receipt.outcome).toBe("containment-unavailable");
    const output = JSON.stringify(result.receipt);
    expect(output).not.toContain(secret);
    expect(output).not.toContain("fixture-owner"); expect(output).not.toContain("fixture-launcher");
    expect(output).not.toContain(f.root);
    expect(result.receipt.drain?.owner.tokenFingerprint).toMatch(/^sha256:[0-9a-f]{16}$/);
    expect(result.receipt.drain?.launcher.tokenFingerprint).toMatch(/^sha256:[0-9a-f]{16}$/);
    expect(result.receipt.drain?.previous.releaseRoot).toBe(path.relative(f.meshRoot, f.expected));
    expect(result.receipt.drain?.target.releaseRoot).toBe(path.relative(f.meshRoot, f.target));
    expect(result.receipt.drain).not.toHaveProperty("checkpoint");
    expect(result.receipt.drain).not.toHaveProperty("main");
  });

  it("validates old configDigest against immutable snapshot rather than desired config", async () => {
    const f = negotiatedFixture();
    fs.writeFileSync(f.snapshotPath, JSON.stringify({ ...f.previous.config, sessionId: "tampered-old-snapshot" }));
    const before = snapshot(f.root); const send = vi.fn();
    const result = await negotiatedInvoke(f, send);
    expect(result.receipt).toMatchObject({ code: "LOADED_SNAPSHOT_INVALID", unchanged: true, transitionAttempted: false });
    expect(result.client).not.toHaveBeenCalled(); expect(send).not.toHaveBeenCalled();
    expect(snapshot(f.root)).toEqual(before);
  });

  it("refuses an unpinned target artifact without dispatching", async () => {
    const f = negotiatedFixture();
    fs.renameSync(path.join(f.target, "dist/worker.js"), path.join(f.target, "retained-worker.js"));
    const before = snapshot(f.root); const send = vi.fn();
    const result = await negotiatedInvoke(f, send);
    expect(result.receipt).toMatchObject({ code: "TARGET_SNAPSHOT_INVALID", unchanged: true, transitionAttempted: false });
    expect(send).not.toHaveBeenCalled(); expect(snapshot(f.root)).toEqual(before);
  });

  it("reports post-dispatch uncertainty honestly instead of advertising unchanged or success", async () => {
    for (const fault of ["missing-response", "wrong-owner", "wrong-target", "held-terminal", "owner-replaced", "main-replaced", "invalid-json", "deadline-changed"] as const) {
      const f = negotiatedFixture();
      const send = vi.fn(async (intent: ResidentDrainIntent) => {
        const initial = initialDrain(f, intent);
        if (fault === "missing-response") return { format: 1 as const, requestId: "recorded", ok: true, completedAt: Date.now() };
        fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(initial));
        setImmediate(() => {
          const terminal = resumedDrain(initial);
          if (fault === "wrong-owner") terminal.owner = { ...terminal.owner, processStartTime: "0" };
          if (fault === "wrong-target") terminal.target = { ...terminal.target, digest: "b".repeat(64) };
          if (fault === "held-terminal") terminal.admissionHeld = true;
          if (fault === "owner-replaced") { f.owner.token = "another-owner"; f.save(); }
          if (fault === "main-replaced") fs.writeFileSync(mainGenerationPath(f.directory), JSON.stringify({ ...f.mainGeneration, nonce: "new-generation" }));
          if (fault === "deadline-changed") terminal.deadlineAt++;
          fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), fault === "invalid-json" ? "{" : JSON.stringify(terminal));
        });
        return response(initial);
      });
      const { exit, receipt } = await negotiatedInvoke(f, send);
      expect(send).toHaveBeenCalledTimes(1); expect(exit).toBe(1);
      expect(receipt).toMatchObject({ ok: false, phase: "needs-operator", outcome: "needs-operator", code: "DRAIN_OUTCOME_UNKNOWN",
        unchanged: false, transitionAttempted: true, rollback: { status: "not-attempted", reason: "terminal-state-unconfirmed" } });
    }
  });

  it("returns the endpoint's terminal needs-operator receipt without claiming A resumed", async () => {
    const f = negotiatedFixture();
    const { exit, receipt } = await negotiatedInvoke(f, async intent => {
      const initial = initialDrain(f, intent);
      const terminal: EndpointDrainReceipt = { ...initial, phase: "needs-operator", outcome: "needs-operator", terminal: true,
        code: "CHECKPOINT_UNCERTAIN", reason: "Checkpoint callback still in flight; A resume unconfirmed", at: Date.now() };
      fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(terminal));
      return response(initial);
    });
    expect(exit).toBe(1);
    expect(receipt).toMatchObject({ phase: "needs-operator", outcome: "needs-operator", code: "CHECKPOINT_UNCERTAIN",
      unchanged: false, transitionAttempted: true, drain: { admissionHeld: true } });
  });

  it("uses the resident deadline after handshake latency and observes natural timeout resume", async () => {
    const f = negotiatedFixture();
    const { receipt } = await negotiatedInvoke(f, async intent => {
      // Deterministic one-shot latency injection, not polling or worker abort.
      await new Promise<void>(resolve => setTimeout(resolve, 40));
      const initial = initialDrain(f, intent);
      fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(initial));
      setTimeout(() => {
        const resumed: EndpointDrainReceipt = { ...initial, phase: "held", outcome: "resumed", terminal: true,
          code: "DRAIN_TIMEOUT", reason: "A naturally resumed after resident drain deadline", admissionHeld: false,
          at: Date.now(), resumedAt: Date.now() };
        fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(resumed));
      }, intent.timeoutMs!);
      return response(initial);
    }, 100);
    expect(receipt).toMatchObject({ phase: "held", outcome: "resumed", code: "DRAIN_TIMEOUT", unchanged: false,
      transitionAttempted: true, drain: { admissionHeld: false, terminal: true } });
  });

  it("bounds a missing terminal receipt with a finite deadline and needs-operator", async () => {
    const f = negotiatedFixture(); const started = Date.now();
    const { exit, receipt } = await negotiatedInvoke(f, async intent => {
      const initial = initialDrain(f, intent);
      fs.writeFileSync(residentDrainReceiptPath(f.directory, initial.id), JSON.stringify(initial));
      return response(initial);
    }, 80);
    expect(exit).toBe(1); expect(Date.now() - started).toBeLessThan(1_500);
    expect(receipt).toMatchObject({ phase: "needs-operator", outcome: "needs-operator", code: "DRAIN_OUTCOME_UNKNOWN",
      unchanged: false, transitionAttempted: true });
    expect(receipt.reason).toContain("deadline expired");
  });

  it("ResidentActorClient sends negotiated releaseDrain over the existing request/response channel", async () => {
    const f = negotiatedFixture();
    const target = residentLaunchSpec({ ...f.previous.config, fabricExtensionPath: path.join(f.target, "dist/index.js"),
      workerPath: path.join(f.target, "dist/worker.js") }, path.join(f.target, "dist/residency/pi-entry.js"));
    const intent: ResidentDrainIntent = { main: f.mainGeneration, caller: { identity: f.participant.updatedBy, hostId: f.rootId }, target,
      expected: { owner: { pid: process.pid, processStartTime: processStartTime(process.pid)!, token: String(f.owner.token), hostId: String(f.owner.hostId) },
        launcher: (f.owner.handover as { launcher: ResidentDrainIntent["expected"]["launcher"] }).launcher,
        releaseRoot: f.expected, configDigest: f.previous.digest } };
    const commands: unknown[] = [];
    const requests = path.join(f.directory, "requests");
    const handled = new Set<string>();
    let watcher: fs.FSWatcher | undefined;
    const keepAlive = setTimeout(() => {}, 1_500);
    try {
      watcher = fs.watch(requests, (_event, filename) => {
        const name = filename?.toString();
        if (!name?.endsWith(".json") || name === "pending.json" || handled.has(name) || !fs.existsSync(path.join(requests, name))) return;
        const command = JSON.parse(fs.readFileSync(path.join(requests, name), "utf8")) as ResidentDrainIntent & {
          requestId: string; operation: string; format: number; rootId: string };
        handled.add(name); commands.push(command);
        const drain = initialDrain(f, command);
        fs.writeFileSync(path.join(f.directory, "responses", name), JSON.stringify({ ...response(drain), requestId: command.requestId }));
      });
      const client = new ResidentActorClient(f.meshRoot, f.rootId, 1_000);
      const result = await client.releaseDrain(intent);
      expect(result).toMatchObject({ ok: true, drain: { abi: RESIDENT_DRAIN_ABI, outcome: "held" } });
      expect(commands).toHaveLength(1);
      expect(commands[0]).toMatchObject({ ...intent, format: 2, operation: "releaseDrain", rootId: f.rootId });
      expect(fs.readFileSync(mainGenerationPath(f.directory), "utf8")).toBe(JSON.stringify(f.mainGeneration));
      expect(processStartTime(process.pid)).toBe(f.mainGeneration.processStartTime);
    } finally { watcher?.close(); clearTimeout(keepAlive); }
  });

  it("ResidentActorClient negotiates/fences releaseDrain before writing requests", async () => {
    const f = negotiatedFixture();
    const target = residentLaunchSpec({ ...f.previous.config, fabricExtensionPath: path.join(f.target, "dist/index.js"),
      workerPath: path.join(f.target, "dist/worker.js") }, path.join(f.target, "dist/residency/pi-entry.js"));
    const intent: ResidentDrainIntent = { main: f.mainGeneration, caller: { identity: f.participant.updatedBy, hostId: f.rootId }, target,
      expected: { owner: { pid: process.pid, processStartTime: processStartTime(process.pid)!, token: String(f.owner.token), hostId: String(f.owner.hostId) },
        launcher: (f.owner.handover as { launcher: ResidentDrainIntent["expected"]["launcher"] }).launcher,
        releaseRoot: f.expected, configDigest: f.previous.digest } };
    const client = new ResidentActorClient(f.meshRoot, f.rootId, 100);
    for (const patch of [{ commands: [] }, { drain: undefined }, { configDigest: "b".repeat(64) }]) {
      const saved = { ...f.owner }; Object.assign(f.owner, patch); f.save();
      const before = snapshot(f.root);
      await expect(client.releaseDrain(intent)).rejects.toThrow();
      expect(snapshot(f.root)).toEqual(before);
      Object.assign(f.owner, saved); f.save();
    }
  });
});

describe.skipIf(process.platform !== "linux")("fabric-resident-drain read-only preflight", () => {
  it("allows only JSON help to succeed, without a mesh default", async () => {
    const { exit, receipt } = await invoke(["--help"]);
    expect(exit).toBe(0);
    expect(receipt).toMatchObject({ ok: true, outcome: "help", code: "HELP", resident: null, expected: null, target: null });
    expect(receipt.help).toContain("setActivationFilter skips/consumes");
    expect(receipt.help).toContain("no actor stop/control");
    expect(receipt.help).toContain("not that the concurrent\nservice is frozen");
    expect(receipt.observed).toBeUndefined();
    expect((await invoke(["-h"])).exit).toBe(0);
  });

  it("requires every flag and refuses unknown, duplicate and bypass flags", async () => {
    expect((await invoke([])).receipt).toMatchObject({ ok: false, code: "INVALID_ARGUMENTS", resident: null });
    const f = fixture();
    for (let index = 0; index < f.argv.length; index += 2) {
      const argv = f.argv.filter((_, position) => position !== index && position !== index + 1);
      expect((await invoke(argv)).receipt.code).toBe("INVALID_ARGUMENTS");
    }
    for (const extra of [["--force"], ["--dry-run"], ["--help"], ["--resident", f.directory]]) {
      await refusal(f, "INVALID_ARGUMENTS", [...f.argv, ...extra]);
    }
  });

  it("refuses an expected release mismatch from either immutable owner field", async () => {
    for (const field of ["releaseRoot", "fabricExtensionPath"]) {
      const f = fixture();
      f.owner[field] = field === "releaseRoot" ? f.target : path.join(f.target, "dist/index.js");
      f.save();
      await refusal(f, "EXPECTED_RELEASE_MISMATCH");
    }
  });

  it("identifies legacy loaded-path mismatch before refusing missing attestation", async () => {
    const f = fixture();
    delete f.owner.releaseRoot;
    delete f.owner.configDigest;
    delete f.owner.handover;
    f.owner.fabricExtensionPath = path.join(f.target, "dist/index.js");
    f.save();
    await refusal(f, "EXPECTED_RELEASE_MISMATCH");
    f.owner.fabricExtensionPath = path.join(f.expected, "dist/index.js");
    f.save();
    const receipt = await refusal(f, "MISSING_LAUNCH_ATTESTATION");
    expect(receipt.observed?.loadedRelease).toEqual({ releaseRoot: null,
      fabricExtensionPath: path.relative(f.meshRoot, String(f.owner.fabricExtensionPath)) });
  });

  it("refuses each absent attestation field even when mutable config claims the old release", async () => {
    for (const field of ["releaseRoot", "fabricExtensionPath", "configDigest", "handover"]) {
      const f = fixture();
      delete f.owner[field];
      f.config.fabricExtensionPath = path.join(f.expected, "dist/index.js");
      f.save();
      await refusal(f, "MISSING_LAUNCH_ATTESTATION");
    }
  });

  it("refuses a valid birth-bound attested root precisely at the absent admission protocol", async () => {
    const f = fixture();
    const receipt = await refusal(f, "DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE");
    expect(receipt.observed).toEqual({ rootId: f.rootId, hostId: f.owner.hostId, pid: process.pid,
      processStartTime: f.owner.processStartTime,
      tokenFingerprint: `sha256:${createHash("sha256").update(String(f.owner.token)).digest("hex").slice(0, 16)}`,
      loadedRelease: { releaseRoot: path.relative(f.meshRoot, f.expected),
        fabricExtensionPath: path.relative(f.meshRoot, path.join(f.expected, "dist/index.js")) } });
    expect(receipt.reason).toContain("Structural launch identity is available");
    expect(receipt.reason).toContain("digest not recomputed/validated");
    expect(receipt.reason).toContain("closes admission and checkpoints accepted work");
    expect(receipt.reason).toContain("setActivationFilter skips/consumes rather than holds");
    expect(receipt.reason).toContain("pauseForRelease is internal");
    expect(receipt.reason).toContain("releaseChange is unconditionally deferred");
    expect(processStartTime(process.pid)).toBe(f.owner.processStartTime);
  });

  it("preserves exact legacy no-effects refusal with or without an authentic containment advertisement", async () => {
    for (const advertised of [false, true]) {
      const f = fixture(); if (advertised) advertiseLaunch(f);
      await refusal(f, "DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE");
    }
  });

  it("accepts an unambiguous resident id/prefix but still refuses admission", async () => {
    const f = fixture();
    for (const selector of [path.basename(f.directory), path.basename(f.directory).slice(0, 12)]) {
      const argv = [...f.argv]; argv[1] = selector;
      const before = snapshot(f.root);
      const { exit, receipt } = await invoke(argv);
      expect(exit).toBe(1);
      expect(receipt).toMatchObject({ code: "DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE", resident: path.relative(f.meshRoot, f.directory) });
      expect(snapshot(f.root)).toEqual(before);
    }
  });

  it("rejects missing, malformed, stale and invalid resident births/PIDs", async () => {
    for (const birth of [undefined, "", "invalid"]) {
      const f = fixture(); f.owner.processStartTime = birth; f.save();
      await refusal(f, "RESIDENT_BIRTH_MISSING");
    }
    for (const patch of [{ processStartTime: "0" }, { pid: 0 }, { pid: -1 }, { pid: "123" }, { pid: 1.5 }]) {
      const f = fixture(); Object.assign(f.owner, patch); f.save();
      await refusal(f, "RESIDENT_BIRTH_MISMATCH");
    }
  });

  it("rejects absent, malformed and stale handover launcher identity", async () => {
    for (const patch of [{ launcher: undefined }, { launcher: {} }, { abi: "future-abi" }]) {
      const f = fixture(); Object.assign(f.owner.handover as object, patch); f.save();
      await refusal(f, "MISSING_LAUNCH_ATTESTATION");
    }
    const f = fixture();
    const handover = f.owner.handover as { launcher: Record<string, unknown> };
    handover.launcher.processStartTime = "0"; f.save();
    await refusal(f, "LAUNCHER_BIRTH_MISMATCH");
  });

  it("refuses malformed digest and unverifiable immutable launch files", async () => {
    const f = fixture(); f.owner.configDigest = "not-a-digest"; f.save();
    await refusal(f, "MISSING_LAUNCH_ATTESTATION");
    const other = fixture();
    (other.owner.handover as { launcher: Record<string, unknown> }).launcher.runtime = path.join(other.root, "absent-runtime");
    other.save(); await refusal(other, "MISSING_LAUNCH_ATTESTATION");
  });

  it("rejects selectors outside the mesh, symlink selectors, ambiguity and unknown IDs", async () => {
    const f = fixture();
    fs.mkdirSync(path.join(f.meshRoot, "residency/abcdef01"));
    fs.mkdirSync(path.join(f.meshRoot, "residency/abcdef02"));
    fs.symlinkSync(f.directory, path.join(f.meshRoot, "residency/linked"));
    for (const selector of [f.expected, path.join(f.meshRoot, "residency/linked"), "abc", "not-hex", "ffff"]) {
      const argv = [...f.argv]; argv[1] = selector;
      await refusal(f, "RESIDENT_SELECTOR_INVALID", argv);
    }
  });

  it("rejects config root, owner host, residency and explicit mesh mismatches", async () => {
    for (const patch of [{ rootId: "different-root" }, { meshRoot: path.join(os.tmpdir(), "missing-drain-mesh") },
      { residencyRoot: os.tmpdir() }]) {
      const f = fixture(); Object.assign(f.config, patch); f.save();
      await refusal(f, "RESIDENT_IDENTITY_MISMATCH");
    }
    const f = fixture(); f.owner.hostId = residentHostId("other-root"); f.save();
    await refusal(f, "RESIDENT_IDENTITY_MISMATCH");
    const other = fixture();
    const aliasMesh = path.join(other.root, "other-mesh"); fs.mkdirSync(aliasMesh);
    // Same residency parent through a symlink must not hide the config mesh mismatch.
    fs.symlinkSync(path.join(other.meshRoot, "residency"), path.join(aliasMesh, "residency"));
    const argv = [...other.argv]; argv[3] = aliasMesh;
    await refusal(other, "RESIDENT_IDENTITY_MISMATCH", argv);
  });

  it("enforces channel OS-user ownership and refuses a symlink owner file", async () => {
    const f = fixture();
    await refusal(f, "RESIDENT_CHANNEL_UNOWNED", f.argv, { platform: "linux", getuid: () => fs.statSync(f.directory).uid + 1 });
    // Move only our fixture owner aside; no deletion, native kill or live-root access.
    const ownerPath = path.join(f.directory, "owner.json");
    fs.renameSync(ownerPath, path.join(f.directory, "retained-owner.json"));
    fs.symlinkSync(path.join(f.directory, "retained-owner.json"), ownerPath);
    await refusal(f, "RESIDENT_CHANNEL_UNOWNED");
  });

  it("refuses malformed resident JSON and missing explicit mesh", async () => {
    const f = fixture(); fs.writeFileSync(path.join(f.directory, "owner.json"), "{");
    const receipt = await refusal(f, "RESIDENT_STATE_INVALID");
    expect(receipt.observed).toBeUndefined();
    const other = fixture(); const argv = [...other.argv]; argv[3] = path.join(other.root, "missing-mesh");
    await refusal(other, "INVALID_MESH_ROOT", argv);
  });

  it("requires real absolute release roots, not mutable symlink selectors", async () => {
    const f = fixture(); const alias = path.join(f.root, "old-alias"); fs.symlinkSync(f.expected, alias);
    for (const [index, value] of [[5, alias], [5, "relative-old"], [7, path.join(f.root, "missing-target")]] as const) {
      const argv = [...f.argv]; argv[index] = value;
      const before = snapshot(f.root);
      const { exit, receipt } = await invoke(argv);
      expect(exit).toBe(1); expect(receipt.code).toBe("INVALID_RELEASE_ROOT");
      expect(snapshot(f.root)).toEqual(before);
    }
  });
});
