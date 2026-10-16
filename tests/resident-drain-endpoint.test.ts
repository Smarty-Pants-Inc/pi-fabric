import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ResidentHost, runResidentHostFromConfigPath } from "../src/residency/host.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { newResidentRequestId } from "../src/residency/request-expiry.js";
import { RESIDENT_COMMANDS, type ResidentCommand, type ResidentCommandResponse, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import { residentLaunchSpec, mainGenerationPath, assertAutomaticReleaseRecovery, assertResidentStagedReady,
  handoverStagedReadyPath, writeResidentStagedReadyReceipt, type ResidentStagedReadyReceipt, type ResidentMainGeneration } from "../src/residency/handover.js";
import { RESIDENT_DRAIN_ABI, readResidentDrainReceipt, residentDrainReceiptPath, type ResidentDrainIntent, type ResidentDrainReceipt } from "../src/residency/drain.js";
import { failClosedContainment, ContainmentUnavailableError, AttemptExitUnconfirmedError, type AttemptContainment, type AttemptLaunchReceipt } from "../src/residency/attempt-containment-contract.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

// Keep reason: trust boundaries and data safety. These use the real host/file command
// path with live birth-bound Main publication and retained artifacts. No B/worker is
// launched, no live root is touched, and no native removal implementation is called.
// Main's setup installs the deletion guard before imports; fixtures below require
// its pinned private temp, not a bespoke recording preload or external root.
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

function recordLaunchAttestation(root: string, receipt: AttemptLaunchReceipt): void {
  // Fixture acts as the launcher writer, never the product host/client. No inference/native removal.
  const file = path.join(root, `attempt-launch-${receipt.attemptId}.json`);
  const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
  try { fs.writeFileSync(fd, JSON.stringify(receipt)); fs.fsyncSync(fd); }
  finally { fs.closeSync(fd); }
}

function watchJson<T>(file: string, accept: (value: T) => boolean = () => true, timeoutMs = 5_000): Promise<T> {
  return new Promise((resolve, reject) => {
    let done = false;
    const watcher = fs.watch(path.dirname(file), inspect);
    const timer = setTimeout(() => finish(new Error(`Timed out observing ${file}`)), timeoutMs);
    function finish(error?: Error, value?: T) {
      if (done) return;
      done = true; clearTimeout(timer); watcher.close();
      if (error) reject(error); else resolve(value!);
    }
    function inspect() {
      try {
        const value = JSON.parse(fs.readFileSync(file, "utf8")) as T;
        if (accept(value)) finish(undefined, value);
      } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") finish(error as Error); }
    }
    watcher.on("error", error => finish(error)); inspect();
  });
}
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
async function fixture(containment: AttemptContainment = failClosedContainment, launchAttestation?: AttemptLaunchReceipt,
  recordedAttestation: AttemptLaunchReceipt | null | undefined = launchAttestation) {
  const privateTemp = residentDrainTemp("fabric-drain-endpoint-");
  const { root } = privateTemp;
  const a = path.join(root, "A"), b = path.join(root, "B");
  for (const release of [a, b]) {
    fs.mkdirSync(path.join(release, "dist/residency"), { recursive: true });
    for (const file of ["index.js", "worker.js", "residency/pi-entry.js"]) fs.writeFileSync(path.join(release, "dist", file), "// retained endpoint artifact; never executed\n");
  }
  const identity = { id: "session:drain-endpoint", name: "Main", kind: "main" as const, sessionId: "drain-endpoint" };
  const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true, notifyOnComplete: false },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.join(a, "dist/worker.js"), fabricExtensionPath: path.join(a, "dist/index.js"),
    piBinary: process.execPath, claudeBinary: "endpoint-missing-claude", vedaBinary: "endpoint-missing-veda" };
  fs.mkdirSync(config.residencyRoot, { mode: 0o700 });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config), { mode: 0o600 });
  const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
    ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
    runner: "pi", transport: "host", capabilities: ["fabric"], cwd: root, sessionId: identity.sessionId,
    startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
    mainProcess: { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid)! } }]);
  await participants.start();
  const previous = residentLaunchSpec(config, path.join(a, "dist/residency/pi-entry.js"));
  const target = residentLaunchSpec({ ...config, workerPath: path.join(b, "dist/worker.js"), fabricExtensionPath: path.join(b, "dist/index.js") }, path.join(b, "dist/residency/pi-entry.js"));
  const launcher = { pid: process.pid, processStartTime: processStartTime(process.pid)!, token: "endpoint-launcher", entry: previous.entry, runtime: previous.runtime };
  if (recordedAttestation) recordLaunchAttestation(config.residencyRoot, recordedAttestation);
  const idle = vi.fn();
  const host = new ResidentHost(previous.config, idle, undefined, { launcher, spec: previous, ...(launchAttestation ? { launchAttestation } : {}) }, config.sessionId, containment);
  await host.start();
  const ownerPath = path.join(config.residencyRoot, "owner.json");
  const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8")) as ResidentHostOwner;
  const main: ResidentMainGeneration = { pid: process.pid, processStartTime: processStartTime(process.pid)!, nonce: randomUUID(), rootId: identity.id,
    sessionId: identity.sessionId, releaseRoot: a };
  fs.writeFileSync(mainGenerationPath(config.residencyRoot), JSON.stringify(main), { mode: 0o600 });
  const intent: ResidentDrainIntent = { caller: { identity, hostId: identity.id }, main, target,
    expected: { releaseRoot: a, configDigest: owner.configDigest!, launcher,
      owner: { pid: owner.pid, processStartTime: owner.processStartTime!, token: owner.token, hostId: owner.hostId } }, timeoutMs: 2_000 };
  // Byte sentinel represents accepted, behind-boundary durable input; tests retain it forever.
  fs.mkdirSync(config.actorRoot, { recursive: true });
  const sentinel = path.join(config.actorRoot, "accepted-input-retained.json");
  fs.writeFileSync(sentinel, '{"id":"retained-input","attempts":3,"preparationAttempts":2}\n');
  function send(command: ResidentCommand) {
    const responsePath = path.join(config.residencyRoot, "responses", `${command.requestId}.json`);
    const response = watchJson<ResidentCommandResponse>(responsePath);
    fs.writeFileSync(path.join(config.residencyRoot, "requests", `${command.requestId}.json`), JSON.stringify(command), { mode: 0o600 });
    return response;
  }
  function drain(patch: Partial<ResidentDrainIntent> = {}, format: 2 | 3 = 2) {
    // Format 3 requires an expiry-compatible request generation, so v2 is the normal probe.
    const requestId = format === 3 ? newResidentRequestId() : randomUUID();
    const command = { ...structuredClone(intent), ...patch, format, operation: "releaseDrain", requestId,
      rootId: config.rootId, createdAt: Date.now() } as ResidentCommand;
    return send(command);
  }
  function terminal(receipt: ResidentDrainReceipt) {
    return watchJson<ResidentDrainReceipt>(residentDrainReceiptPath(config.residencyRoot, receipt.id), value => value.terminal === true);
  }
  return { root, a, b, config, host, participants, mesh, previous, target, owner, ownerPath, intent, main, idle, sentinel,
    send, drain, terminal, close: async () => {
      try { await host.close(); await participants.close(); mesh.closeState(); }
      finally { privateTemp.assertNoOutsideDeletion(); }
    } };
}

describe.skipIf(process.platform !== "linux")("owner-authenticated cooperative resident endpoint", () => {
  it("negotiates releaseDrain and default containment rejects every method", async () => {
    expect(RESIDENT_COMMANDS).toContain("releaseDrain");
    await expect(failClosedContainment.launchInScope({} as never, "attempt")).rejects.toBeInstanceOf(ContainmentUnavailableError);
    await expect(failClosedContainment.attemptMembership("attempt")).rejects.toMatchObject({ code: "CONTAINMENT_UNAVAILABLE", reason: expect.any(String) });
    await expect(failClosedContainment.joinAttemptExit("attempt", Date.now() + 10)).rejects.toBeInstanceOf(ContainmentUnavailableError);
    const error = new AttemptExitUnconfirmedError({ attemptId: "attempt", remainingPids: [123] });
    expect(error).toMatchObject({ code: "ATTEMPT_EXIT_UNCONFIRMED", attemptId: "attempt", remainingPids: [123] });
    expect(() => assertAutomaticReleaseRecovery()).toThrow(/complete exit receipts/);
  });

  it("holds/checkpoints/readbacks/resumes A with Main still on A; missing launch attestation blocks injected membership", async () => {
    const containment: AttemptContainment = { launchInScope: vi.fn(),
      attemptMembership: vi.fn(async attemptId => ({ attemptId, scope: "fixture-owned", pids: [], populated: false })), joinAttemptExit: vi.fn() };
    const f = await fixture(containment);
    try {
      expect(f.owner.drain).toEqual({ abi: RESIDENT_DRAIN_ABI });
      const response = await f.drain();
      expect(response).toMatchObject({ ok: true, drain: { phase: "held", outcome: "held", admissionHeld: true, terminal: false } });
      expect(response.drain!.id).not.toBe(response.requestId);
      const receipt = await f.terminal(response.drain!);
      expect(receipt).toMatchObject({ phase: "checkpointed", outcome: "containment-unavailable", code: "CONTAINMENT_UNAVAILABLE",
        admissionHeld: false, terminal: true, checkpointReadback: true, owner: f.intent.expected.owner, launcher: f.intent.expected.launcher,
        main: f.main, previous: { digest: f.previous.digest }, target: { digest: f.target.digest } });
      expect(receipt.checkpoint).toMatchObject({ actors: { project: expect.anything(), session: expect.anything() },
        agents: expect.anything(), control: expect.anything(), lifecycle: expect.anything() });
      expect(readResidentDrainReceipt(f.config.residencyRoot, receipt.id, response.drain)).toEqual(receipt);
      expect(JSON.parse(fs.readFileSync(f.ownerPath, "utf8"))).toEqual(f.owner);
      expect(fs.readFileSync(f.sentinel, "utf8")).toContain('"attempts":3');
      expect(f.idle).not.toHaveBeenCalled();
      expect(containment.launchInScope).not.toHaveBeenCalled(); expect(containment.joinAttemptExit).not.toHaveBeenCalled();
      expect(containment.attemptMembership).not.toHaveBeenCalled();
      expect(receipt.resumedAt).toBe(receipt.at);
      expect(fs.existsSync(path.join(f.config.residencyRoot, "handover.json"))).toBe(false);
      expect(f.mesh.read({ topic: "host.reloaded" })).toHaveLength(0);
    } finally { await f.close(); }
  });

  it("trusted injected channel selects launch attemptId, never drain ID; positive membership still cannot exit/start", async () => {
    const launchAttestation: AttemptLaunchReceipt = { attemptId: randomUUID(), scope: "retained-fixture.scope",
      cgroupPath: "/unopened/retained-fixture", launchedInScope: true, at: Date.now() };
    const containment: AttemptContainment = { launchInScope: vi.fn(), joinAttemptExit: vi.fn(),
      attemptMembership: vi.fn(async attemptId => ({ attemptId, scope: launchAttestation.scope, pids: [process.pid], populated: true })) };
    const f = await fixture(containment, launchAttestation);
    try {
      const response = await f.drain();
      const receipt = await f.terminal(response.drain!);
      expect(receipt).toMatchObject({ outcome: "containment-unavailable", admissionHeld: false, checkpointReadback: true });
      expect(receipt.id).not.toBe(launchAttestation.attemptId);
      expect(containment.attemptMembership).toHaveBeenCalledExactlyOnceWith(launchAttestation.attemptId);
      expect(containment.launchInScope).not.toHaveBeenCalled(); expect(containment.joinAttemptExit).not.toHaveBeenCalled();
      expect(f.idle).not.toHaveBeenCalled(); expect(fs.existsSync(launchAttestation.cgroupPath)).toBe(false);
    } finally { await f.close(); }
  });

  it.each(["missing", "mismatched"] as const)("%s launcher attestation record cannot be advertised or consulted; cooperative A still resumes", async kind => {
    const launchAttestation: AttemptLaunchReceipt = { attemptId: randomUUID(), scope: "retained-untrusted.scope",
      cgroupPath: "/unopened/untrusted-fixture", launchedInScope: true, at: Date.now() };
    const containment: AttemptContainment = { launchInScope: vi.fn(), joinAttemptExit: vi.fn(),
      attemptMembership: vi.fn(async attemptId => ({ attemptId, scope: launchAttestation.scope, pids: [process.pid], populated: true })) };
    const record = kind === "missing" ? null : { ...launchAttestation, at: launchAttestation.at - 1 };
    const f = await fixture(containment, launchAttestation, record);
    try {
      expect(f.owner.launchAttestation).toBeUndefined();
      expect(f.owner.drain).toEqual({ abi: RESIDENT_DRAIN_ABI });
      const response = await f.drain();
      expect(response).toMatchObject({ ok: true, drain: { admissionHeld: true, terminal: false } });
      const receipt = await f.terminal(response.drain!);
      expect(receipt).toMatchObject({ outcome: "containment-unavailable", checkpointReadback: true, admissionHeld: false, terminal: true });
      expect(containment.attemptMembership).not.toHaveBeenCalled();
      expect(containment.launchInScope).not.toHaveBeenCalled(); expect(containment.joinAttemptExit).not.toHaveBeenCalled();
      expect(JSON.parse(fs.readFileSync(f.ownerPath, "utf8"))).toEqual(f.owner);
      expect(f.idle).not.toHaveBeenCalled(); expect(f.mesh.read({ topic: "host.reloaded" })).toHaveLength(0);
    } finally { await f.close(); }
  });

  it("a launch record changed after startup cannot select even the previously trusted injected channel", async () => {
    const launchAttestation: AttemptLaunchReceipt = { attemptId: randomUUID(), scope: "retained-revalidated.scope",
      cgroupPath: "/unopened/revalidated-fixture", launchedInScope: true, at: Date.now() };
    const containment: AttemptContainment = { launchInScope: vi.fn(), joinAttemptExit: vi.fn(), attemptMembership: vi.fn() };
    const f = await fixture(containment, launchAttestation);
    try {
      expect(f.owner.launchAttestation).toEqual(launchAttestation);
      fs.writeFileSync(path.join(f.config.residencyRoot, `attempt-launch-${launchAttestation.attemptId}.json`),
        JSON.stringify({ ...launchAttestation, at: launchAttestation.at - 1 }));
      const response = await f.drain();
      expect(await f.terminal(response.drain!)).toMatchObject({ outcome: "containment-unavailable", checkpointReadback: true, admissionHeld: false });
      expect(containment.attemptMembership).not.toHaveBeenCalled(); expect(f.idle).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  it.each(["missing", "mismatched", "matching"] as const)("ENV bootstrap consumes receipt but requires the %s launcher record before trusting it", async kind => {
    const f = await fixture();
    try {
      // Stop at construction: test the real ENV/parent/spec bootstrap, not host quiet or an IPC stand-in.
      const releaseRoot = path.resolve(".");
      const config = { ...f.config, workerPath: path.join(releaseRoot, "dist/worker.js"), fabricExtensionPath: path.join(releaseRoot, "dist/index.js") };
      const runtime = fs.realpathSync(`/proc/${process.ppid}/exe`);
      const spec = residentLaunchSpec(config, path.join(releaseRoot, "dist/residency/pi-entry.js"), runtime);
      const receipt: AttemptLaunchReceipt = { attemptId: randomUUID(), scope: "retained-bootstrap.scope",
        cgroupPath: "/unopened/bootstrap-fixture", launchedInScope: true, at: Date.now() };
      if (kind !== "missing") recordLaunchAttestation(f.config.residencyRoot, { ...receipt, at: kind === "matching" ? receipt.at : receipt.at - 1 });
      vi.stubEnv("PI_FABRIC_RESIDENT_LAUNCHER", JSON.stringify({ pid: process.ppid, processStartTime: processStartTime(process.ppid)!,
        token: "bootstrap-launcher", runtime, entry: spec.entry }));
      vi.stubEnv("PI_FABRIC_RESIDENT_SPEC_DIGEST", spec.digest);
      vi.stubEnv("PI_FABRIC_RESIDENT_GENERATION", randomUUID());
      vi.stubEnv("PI_FABRIC_RESIDENT_LAUNCH_ATTESTATION", JSON.stringify(receipt));
      vi.stubEnv("PI_FABRIC_RESIDENT_ATTEMPT", undefined);
      const configPath = path.join(f.config.residencyRoot, "config.json");
      fs.writeFileSync(configPath, JSON.stringify(config));
      let constructed: ResidentHost | undefined;
      const stop = new Error("fixture bootstrap construction boundary");
      vi.spyOn(ResidentHost.prototype, "start").mockImplementation(async function (this: ResidentHost) { constructed = this; throw stop; });
      await expect(runResidentHostFromConfigPath(configPath)).rejects.toBe(stop);
      expect(constructed?.launch?.launchAttestation).toEqual(kind === "matching" ? receipt : undefined);
      expect(process.env.PI_FABRIC_RESIDENT_LAUNCH_ATTESTATION).toBeUndefined();
      expect(process.env.PI_FABRIC_RESIDENT_GENERATION).toBeUndefined();
      expect(f.idle).not.toHaveBeenCalled();
    } finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); await f.close(); }
  });

  it("format-3 drain uses the same held and terminal receipt path", async () => {
    const f = await fixture();
    try {
      const response = await f.drain({}, 3);
      expect(response.ok).toBe(true);
      expect(await f.terminal(response.drain!)).toMatchObject({ outcome: "containment-unavailable", admissionHeld: false });
      expect(f.idle).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  it("wrong expected release wins before even invalid target validation or any hold", async () => {
    const f = await fixture();
    try {
      fs.writeFileSync(f.target.entry, "// changed after pin\n");
      const pause = vi.spyOn(f.host.actors, "pauseForRelease");
      const before = fs.readFileSync(f.sentinel, "utf8");
      const response = await f.drain({ expected: { ...f.intent.expected, releaseRoot: f.b } });
      expect(response).toMatchObject({ ok: false, drain: { phase: "refused", outcome: "refused", code: "EXPECTED_RELEASE_MISMATCH", admissionHeld: false } });
      expect(pause).not.toHaveBeenCalled(); expect(fs.readFileSync(f.sentinel, "utf8")).toBe(before);
      expect(fs.readdirSync(f.config.residencyRoot).some(file => file.startsWith("drain-"))).toBe(false);
      expect(fs.existsSync(path.join(f.config.residencyRoot, "decisions", `${response.requestId}.json`))).toBe(false);
    } finally { await f.close(); }
  });

  it.each(["owner birth", "owner token", "launcher birth", "launcher token", "config digest"])("refuses exact %s mismatch without effects", async kind => {
    const f = await fixture();
    try {
      const expected = structuredClone(f.intent.expected);
      if (kind === "owner birth") expected.owner.processStartTime = "0";
      if (kind === "owner token") expected.owner.token = "foreign";
      if (kind === "launcher birth") expected.launcher.processStartTime = "0";
      if (kind === "launcher token") expected.launcher.token = "foreign";
      if (kind === "config digest") expected.configDigest = "0".repeat(64);
      const pause = vi.spyOn(f.host.actors, "pauseForRelease");
      const response = await f.drain({ expected });
      expect(response.ok).toBe(false); expect(response.drain).toMatchObject({ outcome: "refused", admissionHeld: false });
      expect(pause).not.toHaveBeenCalled(); expect(f.idle).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  it.each(["caller host", "caller session", "caller kind", "main nonce", "main birth", "main release"])("refuses forged/obsolete %s", async kind => {
    const f = await fixture();
    try {
      const caller = structuredClone(f.intent.caller), main = structuredClone(f.main);
      if (kind === "caller host") caller.hostId = "foreign";
      if (kind === "caller session") caller.identity.sessionId = "foreign";
      if (kind === "caller kind") caller.identity.kind = "actor";
      if (kind === "main nonce") main.nonce = "foreign";
      if (kind === "main birth") main.processStartTime = "0";
      if (kind === "main release") { main.releaseRoot = path.join(f.root, "unbound"); fs.writeFileSync(mainGenerationPath(f.config.residencyRoot), JSON.stringify(main)); }
      const pause = vi.spyOn(f.host.actors, "pauseForRelease");
      expect(await f.drain({ caller, main })).toMatchObject({ ok: false, errorCode: "RESIDENT_ACTOR_FORBIDDEN", drain: { outcome: "refused" } });
      expect(pause).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  it("a live recorded PID is not authority when verified participant Main birth differs", async () => {
    const f = await fixture();
    try {
      const get = f.host.participants.get.bind(f.host.participants);
      vi.spyOn(f.host.participants, "get").mockImplementation((...args) => {
        const root = get(...args);
        return root?.id === f.config.rootId ? { ...root, mainProcess: { pid: f.main.pid, host: os.hostname(), startTime: "0" } } : root;
      });
      const pause = vi.spyOn(f.host.actors, "pauseForRelease");
      expect(await f.drain()).toMatchObject({ ok: false, errorCode: "RESIDENT_ACTOR_FORBIDDEN", drain: { admissionHeld: false, outcome: "refused" } });
      expect(pause).not.toHaveBeenCalled(); expect(f.idle).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  it.each(["requests", "processing", "root", "identity"])("refuses group/other mutable %s channel before hold", async kind => {
    const f = await fixture();
    let channel = f.config.residencyRoot;
    try {
      if (kind === "identity") channel = mainGenerationPath(f.config.residencyRoot);
      else if (kind !== "root") channel = path.join(f.config.residencyRoot, kind);
      const stat = fs.statSync(channel);
      fs.chmodSync(channel, stat.mode | 0o020);
      const pause = vi.spyOn(f.host.actors, "pauseForRelease");
      const response = await f.drain();
      expect(response).toMatchObject({ ok: false, drain: { code: "RESIDENT_CHANNEL_UNOWNED", outcome: "refused" } });
      expect(pause).not.toHaveBeenCalled();
    } finally { fs.chmodSync(channel, channel === mainGenerationPath(f.config.residencyRoot) ? 0o600 : 0o700); await f.close(); }
  });

  it("refuses wrong-uid actual incoming request without changing ownership on disk", async () => {
    const f = await fixture();
    try {
      const requestId = randomUUID();
      const incoming = path.join(f.config.residencyRoot, "processing", `${requestId}.json`);
      const lstat = fs.lstatSync.bind(fs);
      const probe = vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
        const stat = lstat(file, options as never);
        if (String(file) === incoming) Object.defineProperty(stat, "uid", { value: process.getuid!() + 1 });
        return stat;
      }) as typeof fs.lstatSync);
      const pause = vi.spyOn(f.host.actors, "pauseForRelease");
      const command: ResidentCommand = { ...f.intent, format: 2, operation: "releaseDrain", requestId,
        rootId: f.config.rootId, createdAt: Date.now() };
      expect(await f.send(command)).toMatchObject({ ok: false, drain: { code: "RESIDENT_CHANNEL_UNOWNED" } });
      expect(pause).not.toHaveBeenCalled(); probe.mockRestore();
    } finally { await f.close(); }
  });

  it("refuses symlink incoming request even with authentic caller fields", async () => {
    const f = await fixture();
    try {
      const requestId = randomUUID();
      const command = { ...f.intent, operation: "releaseDrain", format: 2, requestId, rootId: f.config.rootId, createdAt: Date.now() };
      const retained = path.join(f.root, "retained-request.json"); fs.writeFileSync(retained, JSON.stringify(command));
      const response = watchJson<ResidentCommandResponse>(path.join(f.config.residencyRoot, "responses", `${requestId}.json`));
      fs.symlinkSync(retained, path.join(f.config.residencyRoot, "requests", `${requestId}.json`));
      expect(await response).toMatchObject({ ok: false, drain: { code: "RESIDENT_CHANNEL_UNOWNED" } });
    } finally { await f.close(); }
  });

  it("rejects changed artifact/topology and missing loaded attestation without pausing", async () => {
    const f = await fixture();
    try {
      const pause = vi.spyOn(f.host.actors, "pauseForRelease");
      const target = residentLaunchSpec({ ...f.target.config, meshRoot: path.join(f.root, "other-mesh") }, f.target.entry, f.target.runtime);
      expect(await f.drain({ target })).toMatchObject({ ok: false, error: expect.stringMatching(/cannot migrate meshRoot/) });
      fs.writeFileSync(f.target.entry, "// invalid changed artifact\n");
      expect(await f.drain()).toMatchObject({ ok: false, error: expect.stringMatching(/snapshot changed/) });
      fs.writeFileSync(f.ownerPath, JSON.stringify({ ...f.owner, drain: undefined }));
      expect(await f.drain()).toMatchObject({ ok: false, drain: { code: "DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE" } });
      expect(pause).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  it("absolute timeout resumes A, retains new file input, and invalidates late checkpoint before writes", async () => {
    const f = await fixture();
    const entered = deferred(), release = deferred();
    const original = f.host.actors.checkpointForRelease.bind(f.host.actors) as (assertHeld?: () => void) => Promise<void>;
    let lateValid = true;
    const checkpoint = vi.spyOn(f.host.actors, "checkpointForRelease").mockImplementation(async (...args: unknown[]) => {
      const assertHeld = args[0] as (() => void) | undefined;
      await original(assertHeld); entered.resolve(); await release.promise;
      try { assertHeld?.(); } catch { lateValid = false; return; }
      throw new Error("A stale checkpoint was allowed to continue");
    });
    try {
      const response = await f.drain({ timeoutMs: 500 }); await entered.promise;
      const requestId = randomUUID();
      const command: ResidentCommand = { format: 1, operation: "createActor", requestId, rootId: f.config.rootId,
        createdAt: Date.now(), caller: f.intent.caller, request: { name: "held-file-input", instructions: "Retained; never activated", residency: "durable" } };
      const queuedPath = path.join(f.config.residencyRoot, "requests", `${requestId}.json`);
      const created = f.send(command);
      expect(fs.existsSync(queuedPath)).toBe(true);
      const receipt = await f.terminal(response.drain!);
      expect(receipt).toMatchObject({ outcome: "resumed", code: "DRAIN_TIMEOUT", admissionHeld: false, terminal: true });
      const accepted = await created;
      expect(accepted.ok).toBe(true); expect(accepted.actor?.name).toBe("held-file-input");
      expect(f.host.actors.listOwned().filter(actor => actor.name === "held-file-input")).toHaveLength(1);
      release.resolve(); await checkpoint.mock.results[0]!.value;
      expect(lateValid).toBe(false);
      expect(readResidentDrainReceipt(f.config.residencyRoot, receipt.id)).toEqual(receipt);
      expect(f.idle).not.toHaveBeenCalled();
    } finally { release.resolve(); await f.close(); }
  });

  it("wall deadline classifies a checkpoint edge before its timeout callback", async () => {
    const f = await fixture(); const entered = deferred(), release = deferred();
    const original = f.host.actors.checkpointForRelease.bind(f.host.actors) as (assertHeld?: () => void) => Promise<void>;
    vi.spyOn(f.host.actors, "checkpointForRelease").mockImplementation(async (...args: unknown[]) => {
      const assertHeld = args[0] as (() => void) | undefined;
      await original(assertHeld); entered.resolve(); await release.promise; assertHeld?.();
    });
    let clock: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const response = await f.drain({ timeoutMs: 10_000 }); await entered.promise;
      clock = vi.spyOn(Date, "now").mockReturnValue(response.drain!.deadlineAt);
      release.resolve();
      const receipt = await f.terminal(response.drain!);
      expect(receipt).toMatchObject({ outcome: "resumed", code: "DRAIN_TIMEOUT", admissionHeld: false, terminal: true,
        reason: expect.stringMatching(/deadline|timed out|timeout/i) });
      expect(fs.readFileSync(f.sentinel, "utf8")).toContain('"preparationAttempts":2'); expect(f.idle).not.toHaveBeenCalled();
    } finally { clock?.mockRestore(); release.resolve(); await f.close(); }
  });

  it("lost current Main while held resumes A without custody or a late complete receipt", async () => {
    const f = await fixture(); const entered = deferred(), release = deferred();
    const original = f.host.actors.checkpointForRelease.bind(f.host.actors) as (assertHeld?: () => void) => Promise<void>;
    vi.spyOn(f.host.actors, "checkpointForRelease").mockImplementation(async (...args: unknown[]) => {
      const assertHeld = args[0] as (() => void) | undefined;
      await original(assertHeld); entered.resolve(); await release.promise; assertHeld?.();
    });
    try {
      const response = await f.drain(); await entered.promise;
      fs.writeFileSync(mainGenerationPath(f.config.residencyRoot), JSON.stringify({ ...f.main, nonce: "replaced" })); release.resolve();
      expect(await f.terminal(response.drain!)).toMatchObject({ outcome: "resumed", admissionHeld: false, code: "DRAIN_UNCERTAIN" });
      expect(f.idle).not.toHaveBeenCalled(); expect(fs.existsSync(path.join(f.config.residencyRoot, "handover.json"))).toBe(false);
    } finally { release.resolve(); await f.close(); }
  });

  it("checkpoint disk readback failure resumes A and never claims checked quiet", async () => {
    const f = await fixture();
    try {
      const actors = f.host.actors as unknown as { releaseCheckpointReceipt(): unknown };
      vi.spyOn(actors, "releaseCheckpointReceipt").mockImplementation(() => { throw new Error("fixture disk item/attempt mismatch"); });
      const response = await f.drain();
      const receipt = await f.terminal(response.drain!);
      expect(receipt).toMatchObject({ outcome: "resumed", admissionHeld: false, code: "DRAIN_UNCERTAIN", reason: expect.stringMatching(/item\/attempt mismatch/) });
      expect(receipt.quiet).toBeUndefined(); expect(receipt.checkpointReadback).not.toBe(true);
      expect(fs.readFileSync(f.sentinel, "utf8")).toContain('"preparationAttempts":2'); expect(f.idle).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  it("missing or nonzero staged readiness cannot authorize a business gate", async () => {
    const f = await fixture();
    try {
      const receipt: ResidentStagedReadyReceipt = { abi: "fabric-resident-1", id: randomUUID(), rootId: f.config.rootId,
        owner: f.intent.expected.owner, launcher: f.intent.expected.launcher, kind: "target", digest: f.target.digest,
        businessAdmissions: 0, admissionHeld: true, checkpointReadback: true, checkpoint: { retained: true }, at: Date.now() };
      expect(() => assertResidentStagedReady(f.config.residencyRoot, receipt)).toThrow(/uncertain/);
      writeResidentStagedReadyReceipt(f.config.residencyRoot, receipt);
      expect(() => assertResidentStagedReady(f.config.residencyRoot, receipt)).not.toThrow();
      fs.writeFileSync(handoverStagedReadyPath(f.config.residencyRoot, receipt.id), JSON.stringify({ ...receipt, businessAdmissions: 1 }));
      expect(() => assertResidentStagedReady(f.config.residencyRoot, receipt)).toThrow(/uncertain/);
      expect(() => assertAutomaticReleaseRecovery()).toThrow(/unavailable/);
      expect(f.idle).not.toHaveBeenCalled();
    } finally { await f.close(); }
  });

  // Decision C: incident smarty-dev#11172 removed the timer that hid a lost final-settlement wake.
  // E2E: idle-wakes gate, owner fv2-11172wake/event-lead; both modes <=1 wake/s, no site >1/min.
  it("11172 drain hold releases queued request, config event, and due schedule after final control settlement", async () => {
    const f = await fixture();
    const entered = deferred(), release = deferred();
    // Main is the real sender; the resident owner must not count its own outgoing ACK wait as drain custody.
    const sender = new FabricControlPlane(f.mesh, f.intent.caller.identity,
      { enabled: true, hostId: f.config.rootId, canConsumeMesh: () => f.participants.canConsumeMesh() });
    sender.start(async () => ({ accepted: false, error: "fixture Main does not accept commands" }));
    let control: Promise<unknown> | undefined, queued: Promise<ResidentCommandResponse> | undefined;
    const configPath = path.join(f.config.residencyRoot, "config.json");
    const replacementPath = `${configPath}.${randomUUID()}.tmp`;
    try {
      const target = await f.send({ format: 1, operation: "createActor", requestId: randomUUID(), rootId: f.config.rootId,
        createdAt: Date.now(), caller: f.intent.caller,
        request: { name: "drain-control-target", instructions: "Retained mailbox target; never activated", residency: "durable",
          delivery: "mailbox", triggerTurn: false } });
      expect(target.ok).toBe(true); expect(target.actor?.id).toEqual(expect.any(String));
      const resolveBinding = f.host.actors.resolveActivationBinding.bind(f.host.actors);
      vi.spyOn(f.host.actors, "resolveActivationBinding").mockImplementation(async (...args) => {
        entered.resolve(); await release.promise; return resolveBinding(...args);
      });
      control = sender.request(f.host.hostId, target.actor!.id, "followUp",
        { message: "held precut mailbox obligation", triggerTurn: false }, f.host.hostId);
      await Promise.race([entered.promise, new Promise<never>((_, reject) => setTimeout(() => reject(new Error("control admission did not enter")), 2_000))]);
      const held = await f.drain({ timeoutMs: 4_000 });
      expect(held).toMatchObject({ ok: true, drain: { phase: "held", outcome: "held", admissionHeld: true, terminal: false } });
      const maintenanceSample = vi.spyOn(f.host.actors, "nextStuckPreparingAt");
      const queuedRequestId = randomUUID();
      queued = f.send({ format: 1, operation: "createActor", requestId: queuedRequestId, rootId: f.config.rootId,
        createdAt: Date.now(), caller: f.intent.caller,
        request: { name: "drain-queued-create", instructions: "Queued after the real drain hold", residency: "durable",
          delivery: "mailbox", triggerTurn: false } });
      const queuedPath = path.join(f.config.residencyRoot, "requests", `${queuedRequestId}.json`);
      expect(fs.existsSync(queuedPath)).toBe(true);
      let admitted = false;
      void queued.then(() => { admitted = true; });
      const replacement = { ...f.config, mesh: { ...f.config.mesh, residentIdleExitMs: 1_234 },
        retention: { ...f.config.retention, oneShotRunMs: f.config.retention.oneShotRunMs + 1 } };
      fs.writeFileSync(replacementPath, JSON.stringify(replacement), { mode: 0o600 }); fs.renameSync(replacementPath, configPath);
      const topic = `drain.merge.${randomUUID().replaceAll("-", "")}`;
      const scheduleKey = `drain-merge-${randomUUID()}`;
      await f.mesh.schedule({ topic, kind: "held-maintenance", from: f.intent.caller.identity, text: "must wait for resume",
        dueAt: Date.now() + 200, key: scheduleKey });
      await new Promise(resolve => setTimeout(resolve, 450));
      expect(admitted).toBe(false);
      expect(fs.existsSync(queuedPath)).toBe(true);
      expect(f.host.actors.listOwned().some(actor => actor.name === "drain-queued-create")).toBe(false);
      expect(f.mesh.scheduled({ topic })).toHaveLength(1);
      expect(f.mesh.read({ topic })).toHaveLength(0);
      expect(maintenanceSample).not.toHaveBeenCalled();
      // Resume the original binding and the real precut withReleaseAdmission/ACK settlement.
      // No watcher callback, timer advance, fresh request, or synthetic wake is supplied.
      release.resolve();
      await expect(control).resolves.toMatchObject({ acknowledged: true });
      expect(await f.terminal(held.drain!)).toMatchObject({ outcome: "containment-unavailable", code: "CONTAINMENT_UNAVAILABLE",
        admissionHeld: false, terminal: true });
      expect(await queued).toMatchObject({ ok: true, actor: { name: "drain-queued-create" } });
      await new Promise(resolve => setTimeout(resolve, 250));
      expect(f.mesh.scheduled({ topic })).toHaveLength(0);
      expect(f.mesh.read({ topic })).toHaveLength(1);
      expect(maintenanceSample).toHaveBeenCalled();
      expect(f.idle).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      void control?.catch(() => undefined);
      void queued?.catch(() => undefined);
      if (fs.existsSync(replacementPath)) fs.rmSync(replacementPath, { force: true });
      await sender.close();
      await f.close();
    }
  });
});
