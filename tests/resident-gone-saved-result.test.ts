import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentTransportHandle } from "../src/agents/types.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, type ResidentHostConfig } from "../src/residency/protocol.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// Recorded incident: five saved COMPLETED actor outcomes, absent native PIDs,
// no manager-result.json, and FIFO events stuck behind the in-flight activation.
// Linux supplies exact kernel births for the gone/live counterexample.
beforeEach(() => installInProcessResidentFence());
const ERROR = "fixture: transient native stop observation failed; exit unconfirmed";
const SAVED_WARNINGS = ["fixture: terminal result saved before native exit; OS exit unobserved"];
const readJson = <T = Record<string, unknown>>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const writeJson = (file: string, value: unknown) => fs.writeFileSync(file, JSON.stringify(value, null, 2));
const release = (file: string) => { if (!fs.existsSync(file)) fs.writeFileSync(file, "release\n"); };

// Native filesystem commits and close callbacks are the event sources. One
// absolute deadline is a failure bound, not a status/liveness polling timer.
const observe = (root: string) => {
  const listeners = new Set<() => void>();
  const wake = () => { for (const listener of [...listeners]) listener(); };
  let watcherError: Error | undefined;
  const watcher = fs.watch(root, { recursive: true, persistent: false }, wake);
  watcher.on("error", error => { watcherError = error; wake(); });
  const until = (predicate: () => boolean, what: string, ms = 8_000): Promise<void> => new Promise((resolve, reject) => {
    let finished = false;
    const finish = (error?: unknown) => {
      if (finished) return;
      finished = true; clearTimeout(timer); listeners.delete(check);
      if (error) reject(error); else resolve();
    };
    const check = () => {
      try { if (watcherError) throw watcherError; if (predicate()) finish(); }
      catch (error) { finish(error); }
    };
    const timer = setTimeout(() => finish(new Error(`Resident gone/saved deadline: ${what}; fixture ${root}`)), ms);
    listeners.add(check); check();
  });
  return { wake, until, close: () => watcher.close() };
};

type Launch = { id: string; directory: string; native: AgentTransportHandle; closed: boolean; injected: number };
type QueueItem = { id: string; resumed?: boolean; executionStarted?: boolean };
const queue = (root: string, actorId: string): QueueItem[] => {
  const directory = path.join(root, "actors", actorId);
  return fs.readdirSync(directory).filter(name => /^queue-.*\.json$/.test(name)).sort()
    .flatMap(name => readJson<{ items: QueueItem[] }>(path.join(directory, name)).items);
};

// Vitest's global teardown removes its own TMPDIR even if this suite does not.
// SETTLE_REPRO_EVIDENCE_ROOT opts into caller-owned scratch outside that root;
// otherwise evidence stays in the test temp dir. No source trace is explicitly removed.
// writeFile (rather than rename/copy into a wider deletion ceiling) also leaves
// the standard test deletion guard intact. A SHA-256 inventory binds the copy.
const preserve = (source: string, destination: string) => {
  const inventory: Array<{ path: string; bytes: number; sha256: string }> = [];
  const visit = (directory: string, relative = "") => {
    fs.mkdirSync(path.join(destination, relative), { recursive: true, mode: 0o700 });
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const name = path.join(relative, entry.name);
      if (entry.isDirectory()) visit(path.join(directory, entry.name), name);
      else if (entry.isFile()) {
        const bytes = fs.readFileSync(path.join(directory, entry.name));
        fs.writeFileSync(path.join(destination, name), bytes, { mode: 0o600 });
        inventory.push({ path: name, bytes: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex") });
      }
    }
  };
  visit(source);
  writeJson(path.join(destination, "retained-inventory.json"), inventory);
};

const fixture = async (name: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `resident-gone-saved-${name}-`));
  const evidenceBase = process.env.SETTLE_REPRO_EVIDENCE_ROOT;
  // An explicit caller-owned sink survives both Vitest and smarty-test-env's
  // temp removers. The default must stay writable, not climb to filesystem root.
  const evidence = fs.mkdtempSync(path.join(evidenceBase ?? os.tmpdir(), `settle-retained-${name}-`));
  const events = observe(root);
  const gates = path.join(root, "gates"); fs.mkdirSync(gates);
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: `session:gone-saved-${name}`, sessionId: `gone-saved-${name}`,
    cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    residencyRoot: path.join(root, "resident"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, budgetUsd: 0, retainRuns: true,
      notifyOnComplete: false, sessionExport: false, maxConcurrent: 2 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/gone-saved-result-worker.mjs"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const launches: Launch[] = [];
  const original = ProcessTransport.prototype.launch;
  const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
    const native = await original.call(this, request);
    const argumentsMap = new Map<string, string>();
    for (let index = 0; index < request.workerArguments.length; index += 2) {
      argumentsMap.set(request.workerArguments[index]!, request.workerArguments[index + 1]!);
    }
    const directory = path.dirname(argumentsMap.get("--status-file")!);
    const launch: Launch = { id: request.id, directory, native, closed: false, injected: 0 };
    launches.push(launch);
    void native.closed!.then(() => { launch.closed = true; events.wake(); });
    const first = fs.readFileSync(argumentsMap.get("--task-file")!, "utf8").includes("SETTLE_FIRST");
    if (!first) return native;
    // Bounded injection at the ACTUAL transport boundary. The real child,
    // session PID, kernel birth, liveness, native close, and subsequent stop
    // calls are unchanged. One stop fails while the saved-result worker lives;
    // historical observation uncertainty ends on its actual native close.
    return { ...native,
      stop: async options => {
        if (launch.injected === 0) {
          launch.injected++;
          writeJson(path.join(directory, "fixture-injected-stop.json"), { at: Date.now(), error: ERROR });
          events.wake(); throw new Error(ERROR);
        }
        return native.stop(options);
      },
      lostContact: () => launch.injected > 0 && !launch.closed ? ERROR : native.lostContact?.(),
    };
  });
  const host = new ResidentHost(config);
  let actorId: string | undefined;
  const payload = (label: string, ready = false) => {
    const saveGate = path.join(gates, `${label}.save`);
    const exitGate = path.join(gates, `${label}.exit`);
    if (ready) { release(saveGate); release(exitGate); }
    return { fixtureReply: `SAVED_${label}_ANSWER`, fixtureSaveGate: saveGate, fixtureExitGate: exitGate };
  };
  const outputs = () => actorId ? host.actors.messages(actorId, 100).filter(message => message.direction === "out") : [];
  const deliveries = () => host.mesh.listAll(residentDeliveryPrefix(config.rootId)).map(entry => entry.value);
  const snapshot = (label: string) => {
    writeJson(path.join(root, `${label}.json`), { actor: actorId ? host.actors.status(actorId) : undefined,
      queue: actorId ? queue(root, actorId) : [], outputs: outputs(), deliveries: deliveries(),
      launches: launches.map(({ id, native, directory, closed, injected }) => ({ id, pid: native.sessionId,
        directory, closed, injected, identity: host.agents.workerIdentity(id),
        managerResult: fs.existsSync(path.join(directory, "manager-result.json")),
        ...(fs.existsSync(path.join(directory, "unresolved-worker.json"))
          ? { unresolved: readJson(path.join(directory, "unresolved-worker.json")) } : {}) })) });
  };
  const close = async () => {
    // Retain the failing phase BEFORE host shutdown changes any actor state.
    snapshot("before-close"); preserve(root, path.join(evidence, "before-close"));
    spy.mockRestore();
    for (const file of fs.readdirSync(gates)) release(path.join(gates, file));
    for (const launch of launches) {
      const task = fs.readFileSync(path.join(launch.directory, "task.txt"), "utf8");
      for (const key of ["fixtureSaveGate", "fixtureExitGate"]) {
        const match = task.match(new RegExp(`"${key}":\\s*("(?:\\\\.|[^"\\\\])*")`));
        if (match) release(JSON.parse(match[1]!) as string);
      }
    }
    // First join voluntary exits. Then close actor admission BEFORE closing
    // AgentManager, so pending FIFO items are checkpointed, not launched during
    // shutdown. Concurrent normal close avoids ActorManager's 30-second grace
    // waiting on the deliberately orphaned result promise. This is teardown
    // only, after the regression snapshot and every outcome assertion.
    await Promise.all(launches.map(launch => launch.native.closed));
    const actorsClosed = host.actors.close();
    void actorsClosed.catch(() => undefined);
    const closeOutcomes = await Promise.allSettled([actorsClosed, host.agents.close()]);
    let closeError = closeOutcomes.filter(outcome => outcome.status === "rejected")
      .map(outcome => String(outcome.reason)).join("; ") || undefined;
    try { await host.close(); } catch (error) { closeError = `${closeError ?? ""}; ${String(error)}`; }
    const live = await Promise.all(launches.map(async launch => ({ id: launch.id, alive: await launch.native.isAlive() })));
    writeJson(path.join(root, "quiescent.json"), { closeError, live, rawTracesRetained: true });
    events.close(); preserve(root, path.join(evidence, "after-close"));
    console.info(`gone/saved-result retained fixture: ${evidence}`);
    expect(live.every(item => !item.alive), "all exact fixture workers exited").toBe(true);
  };
  try {
    await host.start();
    const actor = await host.actors.create({ name: `saved-result-${name}`, instructions: "Reply to each event in order.",
      residency: "durable", transport: "process", responseMode: "directive", delivery: "followUp", triggerTurn: false,
      coalesce: false, model: "fixture/visible", tools: [], extensions: false });
    actorId = actor.id;
    return { root, evidence, host, actor, events, launches, payload, outputs, deliveries, snapshot, close };
  } catch (error) { await close(); throw error; }
};

// This suite intentionally fails on the unmodified source: normal result
// publication is required, never an assertion that the actor remains wedged.
// Native proof here requires Linux /proc births and its pre-publication drain;
// Windows instead has early terminal publication with a separate native join,
// exercised without platform substitution by actor-permit-watch's recovery cases.
describe.skipIf(process.platform !== "linux")("resident gone native worker with a saved terminal result", () => {
  it("publishes the saved result once and continues the durable actor FIFO after its terminal monitor failed", async () => {
    const f = await fixture("gone");
    try {
      const firstPayload = f.payload("FIRST");
      const first = f.host.actors.tell(f.actor.id, "SETTLE_FIRST", firstPayload);
      await f.events.until(() => f.launches.length === 1 && fs.existsSync(path.join(f.launches[0]!.directory, "fixture-ready.json")), "first native worker running");
      const launch = f.launches[0]!;
      const identity = f.host.agents.workerIdentity(launch.id)!;
      expect(identity).toMatchObject({ pid: Number(launch.native.sessionId), startTime: expect.stringMatching(/^\d+$/), open: true });
      expect(await launch.native.isAlive()).toBe(true);
      const second = f.host.actors.tell(f.actor.id, "SETTLE_SECOND", f.payload("SECOND", true));
      const third = f.host.actors.tell(f.actor.id, "SETTLE_THIRD", f.payload("THIRD", true));
      expect(queue(f.root, f.actor.id).map(item => item.id)).toEqual([first.messageId, second.messageId, third.messageId]);
      expect(f.host.actors.status(f.actor.id)).toMatchObject({ status: "running", queued: 2, inFlightRun: { id: launch.id } });
      f.snapshot("accepted-fifo");
      release(firstPayload.fixtureSaveGate);
      await f.events.until(() => fs.existsSync(path.join(launch.directory, "fixture-saved.json")), "saved terminal worker result");
      const saved = readJson(path.join(launch.directory, "status.json"));
      expect(saved).toMatchObject({ id: launch.id, status: "completed", exitCode: null, text: expect.any(String), value: {
        action: "message", message: "SAVED_FIRST_ANSWER", data: { savedByNativeWorker: true } } });
      expect(saved.warnings).toEqual(SAVED_WARNINGS);
      expect(saved.error).toBeUndefined();
      expect(readJson(path.join(launch.directory, "settlement.json"))).toMatchObject({ runId: launch.id, outcome: "completed", text: saved.text });
      // Terminal is saved while the real process still lives. The first bounded
      // drain rejects and the outer monitor catch installs unresolved custody.
      await f.events.until(() => fs.existsSync(path.join(launch.directory, "unresolved-worker.json")), "historical unconfirmed-exit marker");
      expect(launch.injected).toBe(1);
      expect(await launch.native.isAlive()).toBe(true);
      f.snapshot("terminal-monitor-failed");
      expect(fs.existsSync(path.join(launch.directory, "manager-result.json"))).toBe(false);
      release(firstPayload.fixtureExitGate);
      await launch.native.closed;
      await f.events.until(() => launch.closed, "actual native close");
      expect(await launch.native.isAlive()).toBe(false);
      expect(() => process.kill(identity.pid, 0)).toThrowError(expect.objectContaining({ code: "ESRCH" }));
      // No public stop, fresh spawn, manual recovery, or test-side settlement:
      // the resident must repair this on the original worker's close/maintenance.
      let continued = true;
      try {
        await f.events.until(() => f.outputs().filter(message => message.text === "SAVED_THIRD_ANSWER").length === 1 &&
          !f.host.actors.status(f.actor.id).inFlightRun && f.host.actors.status(f.actor.id).queued === 0,
        "saved-result publication and FIFO continuation", 5_000);
      } catch { continued = false; }
      f.snapshot("after-native-close");
      const resultFile = path.join(launch.directory, "manager-result.json");
      expect.soft(continued, `native PID ${identity.pid} is gone; saved completed result must unblock FIFO; evidence ${f.evidence}`).toBe(true);
      expect.soft(fs.existsSync(resultFile), "manager terminal result must be durably published").toBe(true);
      if (fs.existsSync(resultFile)) {
        const recovered = readJson(resultFile);
        expect.soft(recovered).toMatchObject({ id: launch.id, status: "completed", exitCode: null, text: saved.text, value: saved.value });
        const savedWarnings = saved.warnings as string[];
        const recoveredWarnings = recovered.warnings as string[];
        // The host may append its independently recorded launch diagnostics.
        expect.soft(recoveredWarnings.filter(warning => savedWarnings.includes(warning)),
          "saved warnings must survive exactly once, alongside host diagnostics").toEqual(savedWarnings);
        expect.soft(recovered.error, "saved completion must not acquire an error overlay").toBeUndefined();
      }
      expect.soft(f.outputs().map(message => message.text), "one saved answer per event, in accepted order").toEqual([
        "SAVED_FIRST_ANSWER", "SAVED_SECOND_ANSWER", "SAVED_THIRD_ANSWER",
      ]);
      expect.soft(f.outputs().filter(message => message.runId === launch.id && message.text === "SAVED_FIRST_ANSWER")).toHaveLength(1);
      expect.soft(f.launches.map(item => item.id)).toHaveLength(3);
      expect.soft(queue(f.root, f.actor.id), "accepted inputs acknowledged only after their saved results").toEqual([]);
      expect.soft(f.host.actors.status(f.actor.id)).toMatchObject({ queued: 0 });
      expect.soft(f.host.actors.status(f.actor.id).inFlightRun).toBeUndefined();
      // The root delivery outbox is a second observable delivery boundary.
      expect.soft(JSON.stringify(f.deliveries()).split("SAVED_FIRST_ANSWER").length - 1).toBe(1);
      expect.soft(f.outputs().some(message => message.error)).toBe(false);
    } finally { await f.close(); }
  }, 30_000);

  it("does not settle or advance the FIFO merely because a still-live native worker saved a terminal result", async () => {
    const f = await fixture("live");
    try {
      const firstPayload = f.payload("FIRST");
      const first = f.host.actors.tell(f.actor.id, "SETTLE_FIRST", firstPayload);
      await f.events.until(() => f.launches.length === 1 && fs.existsSync(path.join(f.launches[0]!.directory, "fixture-ready.json")), "live counterexample ready");
      const launch = f.launches[0]!;
      const identity = f.host.agents.workerIdentity(launch.id)!;
      const second = f.host.actors.tell(f.actor.id, "SETTLE_SECOND", f.payload("SECOND", true));
      release(firstPayload.fixtureSaveGate);
      await f.events.until(() => fs.existsSync(path.join(launch.directory, "unresolved-worker.json")), "live worker terminal drain failure");
      // Normal close would drive repair. Keep it withheld for two existing
      // resident maintenance periods; this finite observation catches a repair
      // that treats saved terminal status alone as execution-exit proof.
      await new Promise(resolve => setTimeout(resolve, 1_200));
      expect(await launch.native.isAlive()).toBe(true);
      expect(f.host.agents.workerIdentity(launch.id)).toMatchObject({ pid: identity.pid, startTime: identity.startTime });
      expect(launch.closed).toBe(false);
      expect(readJson(path.join(launch.directory, "status.json"))).toMatchObject({ status: "completed" });
      expect(fs.existsSync(path.join(launch.directory, "manager-result.json"))).toBe(false);
      expect(f.host.actors.status(f.actor.id)).toMatchObject({ status: "running", queued: 1, inFlightRun: { id: launch.id } });
      expect(queue(f.root, f.actor.id).map(item => item.id)).toEqual([first.messageId, second.messageId]);
      expect(f.outputs()).toEqual([]);
      expect(f.deliveries()).toEqual([]);
      expect(f.launches).toHaveLength(1);
      f.snapshot("saved-terminal-but-worker-live");
    } finally { await f.close(); }
  }, 25_000);
});
