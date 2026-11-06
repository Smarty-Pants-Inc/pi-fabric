import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { mainGenerationPath, residentLaunchSpec, type ResidentMainGeneration } from "../src/residency/handover.js";
import { readResidentDrainReceipt, residentDrainReceiptPath, type ResidentDrainIntent, type ResidentDrainReceipt } from "../src/residency/drain.js";
import { readResidentGenerationExit, residentGenerationExitPath } from "../src/residency/generation-exit.js";
import { residentRoot, type ResidentCommand, type ResidentCommandResponse, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { AttemptContainment } from "../src/residency/attempt-containment-contract.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

// Keep: data safety and the host/file-command trust boundary. No worker/target is
// launched or signalled. All product deletion is guarded inside the private temp.
// Real process-birth disappearance and the CLI/launcher path are separate integration coverage.
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
function watchJson<T>(file: string, accept: (value: T) => boolean = () => true): Promise<T> {
  return new Promise((resolve, reject) => {
    let done = false;
    const watcher = fs.watch(path.dirname(file), inspect);
    const timer = setTimeout(() => finish(new Error(`Timed out observing ${file}`)), 5_000);
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
async function fixture() {
  const privateTemp = residentDrainTemp("fabric-drain-exit-");
  const { root } = privateTemp;
  const a = path.join(root, "A"), b = path.join(root, "B");
  for (const release of [a, b]) {
    fs.mkdirSync(path.join(release, "dist/residency"), { recursive: true });
    for (const file of ["index.js", "worker.js", "residency/pi-entry.js"]) {
      fs.writeFileSync(path.join(release, "dist", file), "// retained endpoint artifact; never executed\n");
    }
  }
  const identity = { id: "session:drain-exit", name: "Main", kind: "main" as const, sessionId: "drain-exit" };
  const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: residentRoot(path.join(root, "mesh"), identity.id), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true, notifyOnComplete: false },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.join(a, "dist/worker.js"), fabricExtensionPath: path.join(a, "dist/index.js"),
    piBinary: process.execPath, claudeBinary: "exit-missing-claude", vedaBinary: "exit-missing-veda" };
  fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });
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
  const launcher = { pid: process.pid, processStartTime: processStartTime(process.pid)!, token: "exit-launcher", entry: previous.entry, runtime: previous.runtime };
  const containment: AttemptContainment = { launchInScope: vi.fn(), attemptMembership: vi.fn(), joinAttemptExit: vi.fn() };
  const idle = vi.fn();
  const host = new ResidentHost(config, idle, undefined, { launcher, spec: previous }, config.sessionId, containment);
  await host.start();
  const ownerPath = path.join(config.residencyRoot, "owner.json");
  const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8")) as ResidentHostOwner;
  const main: ResidentMainGeneration = { pid: process.pid, processStartTime: processStartTime(process.pid)!, nonce: randomUUID(), rootId: identity.id,
    sessionId: identity.sessionId, releaseRoot: a };
  fs.writeFileSync(mainGenerationPath(config.residencyRoot), JSON.stringify(main), { mode: 0o600 });
  const intent: ResidentDrainIntent = { caller: { identity, hostId: identity.id }, main, target,
    expected: { releaseRoot: a, configDigest: owner.configDigest!, launcher,
      owner: { pid: owner.pid, processStartTime: owner.processStartTime!, token: owner.token, hostId: owner.hostId } }, timeoutMs: 2_000 };
  function drain(patch: Partial<ResidentDrainIntent> = { exit: true }) {
    const requestId = randomUUID();
    const command = { ...structuredClone(intent), ...patch, format: 2, operation: "releaseDrain", requestId,
      rootId: config.rootId, createdAt: Date.now() } as ResidentCommand;
    const response = watchJson<ResidentCommandResponse>(path.join(config.residencyRoot, "responses", `${requestId}.json`));
    fs.writeFileSync(path.join(config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify(command), { mode: 0o600 });
    return response;
  }
  function terminal(receipt: ResidentDrainReceipt) {
    return watchJson<ResidentDrainReceipt>(residentDrainReceiptPath(config.residencyRoot, receipt.id), value => value.terminal === true);
  }
  return { root, config, host, owner, ownerPath, previous, launcher, participants, mesh, idle, containment, intent, drain, terminal,
    close: async () => {
      try { await host.close(); await participants.close(); mesh.closeState(); }
      finally { privateTemp.assertNoOutsideDeletion(); }
    } };
}

describe.skipIf(process.platform !== "linux")("explicit graceful resident drain exit endpoint", () => {
  it("publishes the command response before close, retains admission/fence through teardown, and records stopped before exited", async () => {
    const f = await fixture();
    const entered = deferred(), release = deferred();
    const close = f.host.actors.close.bind(f.host.actors);
    vi.spyOn(f.host.actors, "close").mockImplementation(async () => { entered.resolve(); await release.promise; await close(); });
    const resumed = vi.spyOn(f.host.actors, "resumeAfterRelease");
    let heldId: string | undefined;
    // Product diagnostic hooks deliberately swallow errors. Capture facts here
    // and assert outside the hook so a failed expectation cannot disappear.
    const beforeRelease = vi.fn(() => ({ idleCalls: [...f.idle.mock.calls],
      exit: readResidentGenerationExit(f.config.residencyRoot, f.owner.token), ownerPresent: fs.existsSync(f.ownerPath),
      drain: heldId ? readResidentDrainReceipt(f.config.residencyRoot, heldId) : undefined }));
    f.host.beforeRelease = beforeRelease;
    try {
      const response = await f.drain();
      expect(response).toMatchObject({ ok: true, drain: { phase: "held", admissionHeld: true, terminal: false } });
      heldId = response.drain!.id;
      await entered.promise;
      expect(fs.existsSync(path.join(f.config.residencyRoot, "responses", `${response.requestId}.json`))).toBe(true);
      expect(fs.readdirSync(path.join(f.config.residencyRoot, "processing"))).toEqual([]);
      expect(readResidentDrainReceipt(f.config.residencyRoot, response.drain!.id)).toMatchObject({ phase: "checkpointed", terminal: false });
      expect(readResidentGenerationExit(f.config.residencyRoot, f.owner.token)).toBeUndefined();
      expect(fs.existsSync(f.ownerPath)).toBe(true);
      await expect(f.host.actors.create({ name: "not-admitted", instructions: "Never run", residency: "durable" })).rejects.toThrow(/held|closing/);
      release.resolve();
      const receipt = await f.terminal(response.drain!);
      expect(receipt).toMatchObject({ phase: "exited", outcome: "exited", code: "DRAIN_EXITED", admissionHeld: true,
        terminal: true, checkpointReadback: true, owner: f.intent.expected.owner });
      expect(receipt.resumedAt).toBeUndefined();
      expect(readResidentGenerationExit(f.config.residencyRoot, f.owner.token)).toMatchObject({ reason: "stopped", pid: process.pid, writer: "host", at: expect.any(Number) });
      expect(fs.existsSync(f.ownerPath)).toBe(false);
      expect(beforeRelease).toHaveBeenCalledOnce();
      expect(beforeRelease.mock.results[0]!.value).toMatchObject({ idleCalls: [["stopped"]], ownerPresent: true,
        exit: { writer: "host", reason: "stopped" }, drain: { phase: "exited", outcome: "exited", terminal: true } });
      expect(resumed).not.toHaveBeenCalled();
      expect(f.containment.launchInScope).not.toHaveBeenCalled();
      expect(f.containment.attemptMembership).not.toHaveBeenCalled();
      expect(f.containment.joinAttemptExit).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(f.config.residencyRoot, "handover.json"))).toBe(false);
    } finally { release.resolve(); await f.close(); }
  });

  it.each([undefined, false, "true"])("keeps the reversible default for exit=%s", async exit => {
    const f = await fixture();
    const close = vi.spyOn(f.host, "close");
    try {
      const response = await f.drain({ ...(exit === undefined ? {} : { exit: exit as boolean }) });
      expect(response.ok).toBe(true);
      expect(await f.terminal(response.drain!)).toMatchObject({ outcome: "containment-unavailable", admissionHeld: false });
      expect(close).not.toHaveBeenCalled();
      expect(f.idle).not.toHaveBeenCalled();
      expect(readResidentGenerationExit(f.config.residencyRoot, f.owner.token)).toBeUndefined();
      expect(JSON.parse(fs.readFileSync(f.ownerPath, "utf8"))).toEqual(f.owner);
    } finally { await f.close(); }
  });

  it.each(["requests", "processing"])("immediately refuses another %s file without confusing its own command with work", async channel => {
    const f = await fixture();
    const rename = fs.renameSync.bind(fs);
    const debt = path.join(f.config.residencyRoot, channel, "another-command.json");
    const injection = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      rename(source, target);
      if (path.dirname(String(source)) === path.join(f.config.residencyRoot, "requests")) fs.writeFileSync(debt, "{}", { mode: 0o600 });
    });
    const pause = vi.spyOn(f.host.actors, "pauseForRelease");
    try {
      expect(await f.drain()).toMatchObject({ ok: false, drain: { phase: "refused", outcome: "refused",
        code: "DRAIN_EXIT_REQUESTS_BUSY", reason: expect.stringMatching(/in-flight requests|processing/), admissionHeld: false } });
      expect(pause).not.toHaveBeenCalled();
      expect(f.idle).not.toHaveBeenCalled();
    } finally { injection.mockRestore(); fs.rmSync(debt, { force: true }); await f.close(); }
  });

  it("refuses producer outbox debt without holding admission or consuming the debt", async () => {
    const f = await fixture();
    const directory = path.join(f.config.residencyRoot, "delivery-outbox");
    fs.mkdirSync(directory, { recursive: true });
    const debt = path.join(directory, "retained-debt.json");
    fs.writeFileSync(debt, '{"retained":"unconfirmed handoff"}');
    const pause = vi.spyOn(f.host.actors, "pauseForRelease");
    try {
      expect(await f.drain()).toMatchObject({ ok: false, drain: { code: "DRAIN_EXIT_OUTBOX_DEBT", admissionHeld: false } });
      expect(fs.readFileSync(debt, "utf8")).toContain("unconfirmed handoff");
      expect(pause).not.toHaveBeenCalled(); expect(f.idle).not.toHaveBeenCalled();
    } finally { fs.rmSync(debt); await f.close(); }
  });

  it.each(["actor run", "actor setup", "worker run", "worker custody"])("refuses %s immediately instead of waiting or stopping it", async kind => {
    const f = await fixture();
    if (kind === "actor run") vi.spyOn(f.host.actors, "inFlightActorIds").mockReturnValue(["live-actor"]);
    if (kind === "actor setup") vi.spyOn(f.host.actors, "releaseCustodyCount").mockReturnValue(1);
    if (kind === "worker run") vi.spyOn(f.host.agents, "listForUi").mockReturnValue([{ id: "live-worker", status: "running" }] as never);
    if (kind === "worker custody") vi.spyOn(f.host.agents, "hasRetainedCustody").mockReturnValue(true);
    const pause = vi.spyOn(f.host.actors, "pauseForRelease"), stop = vi.spyOn(f.host.agents, "stop");
    try {
      expect(await f.drain()).toMatchObject({ ok: false, drain: { code: kind.startsWith("actor") ? "DRAIN_EXIT_ACTORS_BUSY" : "DRAIN_EXIT_WORKERS_BUSY",
        outcome: "refused", admissionHeld: false, terminal: true } });
      expect(pause).not.toHaveBeenCalled(); expect(stop).not.toHaveBeenCalled(); expect(f.idle).not.toHaveBeenCalled();
    } finally { vi.restoreAllMocks(); await f.close(); }
  });

  it("returns a correlated precommit refusal through the real actor proxy without requiring a durable drain file", async () => {
    const f = await fixture();
    vi.spyOn(f.host.agents, "hasRetainedCustody").mockReturnValue(true);
    const client = new ResidentActorClient(f.config.meshRoot, f.config.rootId, 2_000);
    try {
      const response = await client.releaseDrain({ ...f.intent, exit: true });
      expect(response).toMatchObject({ ok: false, drain: { phase: "refused", outcome: "refused", terminal: true,
        code: "DRAIN_EXIT_WORKERS_BUSY", admissionHeld: false } });
      expect(readResidentDrainReceipt(f.config.residencyRoot, response.drain!.id)).toBeUndefined();
      expect(f.idle).not.toHaveBeenCalled();
      // The exception behavior of ordinary actor operations must not change.
      await expect(client.actorStatus("missing-actor")).rejects.toThrow("Unknown Fabric actor: missing-actor");
    } finally { vi.restoreAllMocks(); await f.close(); }
  });

  it("does not return an uncorrelated drain refusal from the actor proxy", async () => {
    const f = await fixture();
    vi.spyOn(f.host.agents, "hasRetainedCustody").mockReturnValue(true);
    const read = fs.readFileSync.bind(fs);
    const tamper = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      const value = read(file, options as never);
      if (typeof file === "string" && path.dirname(file) === path.join(f.config.residencyRoot, "responses")) {
        const response = JSON.parse(String(value)) as ResidentCommandResponse;
        if (response.drain) return JSON.stringify({ ...response, drain: { ...response.drain, rootId: "foreign-root" } });
      }
      return value;
    }) as typeof fs.readFileSync);
    try {
      const client = new ResidentActorClient(f.config.meshRoot, f.config.rootId, 2_000);
      await expect(client.releaseDrain({ ...f.intent, exit: true })).rejects.toThrow(/worker|custody/i);
      expect(f.idle).not.toHaveBeenCalled();
    } finally { tamper.mockRestore(); vi.restoreAllMocks(); await f.close(); }
  });

  it("retains queued actor events and their FIFO ids across explicit exit and an ordinary relaunch", async () => {
    const f = await fixture();
    let replacement: ResidentHost | undefined;
    f.host.actors.pauseForRelease(); // activation-only: real accepted queue, no worker starts
    const actor = await f.host.actors.create({ name: "retained-events", instructions: "Never execute in this fixture", residency: "durable", delivery: "mailbox", tools: [] });
    const ids = [f.host.actors.tell(actor.id, "first").messageId, f.host.actors.tell(actor.id, "second").messageId];
    try {
      const response = await f.drain();
      const receipt = await f.terminal(response.drain!);
      expect(receipt.outcome).toBe("exited");
      const checkpoint = receipt.checkpoint as { actors: { project: { queues: Array<{ actorId: string; file: string; items: Array<{ id: string }> }> } } };
      const queue = checkpoint.actors.project.queues.find(item => item.actorId === actor.id)!;
      expect(queue.items.map(item => item.id)).toEqual(ids);
      expect(JSON.parse(fs.readFileSync(queue.file, "utf8")).items.map((item: { id: string }) => item.id)).toEqual(ids);
      // Exercise the existing host launch path; keep activation paused only to
      // inspect restoration without launching a worker in this host-only suite.
      vi.spyOn(ActorDirectory.prototype, "resumeAfterRelease").mockImplementation(() => {});
      replacement = new ResidentHost(f.config, () => {}, undefined, { launcher: f.launcher, spec: f.previous });
      await replacement.start();
      expect(replacement.actors.status(actor.id)).toMatchObject({ queued: 2 });
      expect(replacement.actors.messages(actor.id).filter(message => message.direction === "in").map(message => message.id)).toEqual(ids);
      expect(JSON.parse(fs.readFileSync(f.ownerPath, "utf8")).token).not.toBe(f.owner.token);
    } finally { await replacement?.close(); await f.close(); }
  });

  it("keeps admission held as needs-operator on a failed checkpoint, without closing or resuming", async () => {
    const f = await fixture();
    vi.spyOn(f.host.actors, "releaseCheckpointReceipt").mockImplementation(() => { throw new Error("fixture checkpoint disk mismatch"); });
    const resume = vi.spyOn(f.host.actors, "resumeAfterRelease"), close = vi.spyOn(f.host, "close");
    try {
      const response = await f.drain();
      expect(await f.terminal(response.drain!)).toMatchObject({ phase: "needs-operator", outcome: "needs-operator", admissionHeld: true,
        reason: expect.stringContaining("disk mismatch") });
      expect(f.host.actors.releaseAdmissionHeld).toBe(true);
      expect(resume).not.toHaveBeenCalled(); expect(close).not.toHaveBeenCalled(); expect(f.idle).not.toHaveBeenCalled();
      await expect(f.host.actors.create({ name: "still-held", instructions: "Never run", residency: "durable" })).rejects.toThrow(/held/);
    } finally { await f.close(); }
  });

  it("bounds checkpoint grace without reopening admission or allowing a late exit", async () => {
    const f = await fixture();
    const entered = deferred(), release = deferred();
    const checkpoint = f.host.actors.checkpointForRelease.bind(f.host.actors);
    const pending = vi.spyOn(f.host.actors, "checkpointForRelease").mockImplementation(async assertHeld => {
      await checkpoint(assertHeld); entered.resolve(); await release.promise; assertHeld?.();
    });
    try {
      const response = await f.drain({ exit: true, timeoutMs: 400 }); await entered.promise;
      expect(await f.terminal(response.drain!)).toMatchObject({ phase: "needs-operator", outcome: "needs-operator", code: "DRAIN_TIMEOUT", admissionHeld: true });
      release.resolve(); await expect(pending.mock.results[0]!.value).rejects.toThrow(/expired|stale/);
      expect(f.idle).not.toHaveBeenCalled();
      expect(readResidentDrainReceipt(f.config.residencyRoot, response.drain!.id)?.outcome).toBe("needs-operator");
      expect(f.host.actors.releaseAdmissionHeld).toBe(true);
    } finally { release.resolve(); await f.close(); }
  });

  it("reports close failure as needs-operator without a stopped or exited success record", async () => {
    const f = await fixture();
    const close = f.host.agents.close.bind(f.host.agents);
    vi.spyOn(f.host.agents, "close").mockImplementationOnce(async () => { await close(); throw new Error("fixture graceful close failed"); });
    try {
      const response = await f.drain();
      expect(await f.terminal(response.drain!)).toMatchObject({ phase: "needs-operator", outcome: "needs-operator", admissionHeld: true,
        code: "DRAIN_EXIT_UNCERTAIN", reason: expect.stringContaining("graceful close failed") });
      expect(readResidentGenerationExit(f.config.residencyRoot, f.owner.token)).toBeUndefined();
      expect(fs.existsSync(f.ownerPath)).toBe(true); // retain the exact fenced failure for operator inspection
      expect(f.idle).toHaveBeenCalledExactlyOnceWith("stopped");
    } finally { await f.close(); }
  });

  it("does not claim exited on close timeout and keeps that terminal outcome after a late graceful close", async () => {
    const f = await fixture();
    const entered = deferred(), release = deferred();
    const close = f.host.actors.close.bind(f.host.actors);
    vi.spyOn(f.host.actors, "close").mockImplementation(async () => { entered.resolve(); await release.promise; await close(); });
    try {
      const response = await f.drain({ exit: true, timeoutMs: 500 }); await entered.promise;
      const receipt = await f.terminal(response.drain!);
      expect(receipt).toMatchObject({ phase: "needs-operator", outcome: "needs-operator", code: "DRAIN_TIMEOUT", admissionHeld: true });
      expect(readResidentGenerationExit(f.config.residencyRoot, f.owner.token)).toBeUndefined();
      const stopped = watchJson(residentGenerationExitPath(f.config.residencyRoot, f.owner.token));
      release.resolve(); await stopped;
      expect(readResidentDrainReceipt(f.config.residencyRoot, receipt.id)).toEqual(receipt);
      expect(fs.existsSync(f.ownerPath)).toBe(false);
    } finally { release.resolve(); await f.close(); }
  });
});
