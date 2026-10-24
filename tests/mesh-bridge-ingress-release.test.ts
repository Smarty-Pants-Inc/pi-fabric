import { createHash, randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance as monotonicClock } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Held-ingress canonical release refusal. Linux only: there the canonical release is fd-pinned through
// /proc/self/fd. Other platforms release through rename/verifiedRemove, which this file does not model.
// Real MeshBridge -> StoreBridgeSide -> EventLog -> SQLite write fence. The pre-effect refusal is a genuine
// zero-wait BEGIN IMMEDIATE refused by a raw transaction, rolled back right after the counted refusal, so
// SQLite is free again before any retry could reach it. One fixed fault is injected only into the release
// of that exact refused acquisition (its pinned descriptor, canonical inode and owner record), inside this
// test's private temporary root; the owner record, or the ownerless canonical directory, then remains.
// Contract: retained off-lock time and the retained retry/backoff follow only a CONFIRMED canonical
// release. A refused or unproven release keeps custody truthful: nothing deletes what remains, its time is
// not off-lock time, no retained sleep is even scheduled, the publication is not replayed against it, and
// the bridge ends on that failure instead of retrying it. The refusal is still counted. Timers are never
// faked: native setTimeout calls pass through unchanged and are attributed by their direct call site.
// The rejection keeps the original certified fence refusal and the native release error by identity.
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); }
});

const TOPIC = "fleet.work.busy-streak.1";
const INGRESS_TEXT = "held-ingress-release-refusal";
const PINNED_OWNER = /^\/proc\/self\/fd\/(\d+)\/owner$/;
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
const native = async (store: MeshStore, name: string) => {
  const now = Date.now(), id = `session:ingress-release-${name}`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  await store.put({ key: `topology/hosts/${hash(id)}`, identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(store.root, { key: `topology/participants/${hash(id)}`, version: 1, updatedAt: now, updatedBy: identity,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name, label: name,
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return identity;
};
const deferred = <T = void>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const within = async <T>(work: Promise<T>, label: string, ms = 5_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<T>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms} ms`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
const queueTickets = (root: string) => {
  try { return fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort(); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
const sqliteBusy = (error: unknown): boolean => {
  const code = (error as { errcode?: unknown } | null)?.errcode;
  return typeof code === "number" && (code & 0xff) === 5;
};
const codeOf = (error: unknown): string | undefined => (error as NodeJS.ErrnoException | undefined)?.code;
const shape = (error: unknown) => error === undefined ? undefined : {
  name: error instanceof Error ? error.name : typeof error,
  code: typeof codeOf(error) === "string" ? codeOf(error) : undefined,
};
const causeChainIncludes = (error: unknown, target: unknown): boolean => {
  let current = error;
  for (let depth = 0; current !== undefined && current !== null && depth < 8; depth += 1) {
    if (current === target) return true;
    current = (current as { cause?: unknown }).cause;
  }
  return false;
};
// Every toLocal line carrying the retained off-lock metric: progress, terminal, stopped and recovered.
const toLocalOffLockMs = (logs: readonly string[]): number[] => logs.filter(message => message.includes("(toLocal)"))
  .flatMap(message => { const match = /retainedOffLockMs=(\d+)/.exec(message); return match ? [Number(match[1])] : []; });
const counts = (kinds: readonly string[]) =>
  kinds.reduce<Record<string, number>>((all, kind) => ({ ...all, [kind]: (all[kind] ?? 0) + 1 }), {});
// Completed toLocal lines that report a failed held-ingress lifetime's own fence refusal count.
const ingressRefusalLines = (logs: readonly string[]) => logs.flatMap(message => {
  const match = /^bridge ingress busy streak (?:terminal|stopped) \(toLocal\); (\d+) fence refusals; retainedOffLockMs=(\d+)$/
    .exec(message);
  return match ? [{ fenceRefusals: Number(match[1]), retainedOffLockMs: Number(match[2]) }] : [];
});
// The direct scheduler of a native timer: the first stack frame outside this file, the spy and the runtime.
// Only the lock domain's own sleep sites count: the retained helper's delay and the canonical lock waits.
const RUNTIME_FRAME = /node_modules|node:internal|\((?:native|<anonymous>)\)$|mesh-bridge-ingress-release\.test\.ts/;
const LOCK_SLEEP_SITE = /[\\/]src[\\/]mesh[\\/](?:mesh-lock|lock-queue)\.ts:\d/;
const scheduledByLockDomain = (): boolean => {
  const limit = Error.stackTraceLimit;
  Error.stackTraceLimit = 64;
  const stack = new Error().stack ?? "";
  Error.stackTraceLimit = limit;
  const direct = stack.split("\n").slice(1).map(frame => frame.trim()).find(frame => !RUNTIME_FRAME.test(frame));
  return direct !== undefined && LOCK_SLEEP_SITE.test(direct);
};

const ingressFixture = async (name: string) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), `mesh-ingress-release-${name}-`));
  const hub = new MeshStore(path.join(base, "hub"), 65_536, 100, { stateBackend: "sqlite", lockProtocol: 1, lockTimeoutMs: 500 });
  const far = new MeshStore(path.join(base, "far"), 65_536, 100, { stateBackend: "sqlite", lockProtocol: 1, lockTimeoutMs: 500 });
  const sender = await native(far, `${name}-far`), recipient = await native(hub, `${name}-hub`);
  const local = new StoreBridgeSide(hub, "far"), remote = new StoreBridgeSide(far, "hub");
  const logs: string[] = [], cursorPath = path.join(base, "cursor.json"), toLocalRetry = deferred<void>();
  const bridge = new MeshBridge({ local, remote, localName: "hub", remoteName: "far", cursorPath, stopMs: 50,
    log: message => {
      logs.push(message);
      // The existing outer toLocal retry branch logs this line right before it sleeps.
      if (message.includes("(toLocal); retrying in")) toLocalRetry.resolve();
    } });
  let running: Promise<void> | undefined, failure: unknown;
  await bridge.start();
  cleanups.push(async () => {
    await bridge.stop();
    await running;
    hub.closeState(); far.closeState();
    fs.rmSync(base, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  const idle = deferred<void>(), tail = remote.tail.bind(remote);
  vi.spyOn(remote, "tail").mockImplementation(async (...args) => {
    const page = await tail(...args);
    if (page.events.length === 0) idle.resolve();
    return page;
  });
  return {
    hub, far, sender, recipient, local, remote, bridge, logs, idle: idle.promise, toLocalRetry: toLocalRetry.promise,
    launch: () => { running = bridge.run().catch(error => { failure = error; }); },
    running: () => running, failure: () => failure,
    cursorAfter: (): number => {
      try { return (JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { toLocal?: { after?: number } }).toLocal?.after ?? 0; }
      catch { return 0; }
    },
  };
};
type Fixture = Awaited<ReturnType<typeof ingressFixture>>;

type StepKind = "publish-start" | "acquire" | "acquire-refused" | "refusal" | "release" | "owner-unlink" | "inject" |
  "append" | "settled";
type Fault = "unlink-eperm" | "proof-read-eio" | "rmdir-eperm" | "reclaim-same-process" | "reclaim-foreign-live";

// Observers delegate every native call. Steps are recorded only during the FIRST held-ingress publication
// (StoreBridgeSide.publishWithReceipt for the ingress text), from its call to its settlement.
const instrument = (f: Fixture, plan: { fault?: Fault; rollbackAfterRefusals: number }) => {
  const root = f.hub.root, lockPath = path.join(root, ".lock"), ownerPath = path.join(lockPath, "owner");
  const eventsPath = path.join(root, "events.jsonl");
  // Bound before any spy below: the test's own native operations are never recorded as product steps.
  const read = fs.readFileSync.bind(fs), mkdir = fs.mkdirSync.bind(fs), rmdir = fs.rmdirSync.bind(fs);
  const unlink = fs.unlinkSync.bind(fs), rename = fs.renameSync.bind(fs), write = fs.writeFileSync.bind(fs);
  const append = fs.appendFileSync.bind(fs), exec = DatabaseSync.prototype.exec, schedule = globalThis.setTimeout;
  const publish = f.hub.publish.bind(f.hub), publishWithReceipt = f.local.publishWithReceipt.bind(f.local);
  const stateWriteFence = f.hub.withStateWriteFence; // unbound: called below with the original receiver
  const eperm = plan.fault === "rmdir-eperm" ? "rmdir" : "unlink";
  const injectedError = plan.fault === "proof-read-eio"
    ? Object.assign(new Error("EIO: i/o error, read (injected release refusal)"), { code: "EIO", errno: -5, syscall: "read" })
    : Object.assign(new Error(`EPERM: operation not permitted, ${eperm} (injected release refusal)`),
      { code: "EPERM", errno: -1, syscall: eperm });
  const steps: Array<{ kind: StepKind; at: number }> = [];
  const settled = deferred<{ fulfilled: boolean; error?: unknown }>();
  const s = {
    steps, injectedError, lockPath, ownerPath, settled: settled.promise,
    raw: undefined as DatabaseSync | undefined, sqliteHeld: false,
    started: false, inFlight: false, refusals: 0, appended: 0, injected: 0, ownerUnlinks: 0,
    faultActive: plan.fault !== undefined, armed: false,
    refused: undefined as { record: string; dev: number; ino: number } | undefined,
    successor: undefined as { record: string; ino: number } | undefined,
    reclaimedReceipt: `${lockPath}.dead.test-reclaim`,
    probes: { refusedRecordInPlace: 0, other: 0 },
    fenceRefusals: [] as Array<{ error: unknown; own: unknown }>,
    // Native timers the lock domain itself scheduled during the first publication (helper backoff, lock waits).
    lockTimers: [] as Array<{ ms: number; step: number; afterInject: boolean }>,
  };
  const mark = (kind: StepKind): void => { if (s.inFlight) steps.push({ kind, at: monotonicClock.now() }); };
  const settle = (fulfilled: boolean, error?: unknown): void => {
    mark("settled");
    s.inFlight = false;
    settled.resolve(fulfilled ? { fulfilled } : { fulfilled, error });
  };
  const rollbackSqlite = (): void => {
    if (s.raw && s.sqliteHeld) { exec.call(s.raw, "ROLLBACK"); s.sqliteHeld = false; }
  };
  const restoreFault = (): void => { s.faultActive = false; s.armed = false; };
  // Only the release of the refused acquisition: its pinned descriptor still names the canonical inode,
  // and that inode still holds the exact refused owner record.
  const refusedPin = (file: unknown): boolean => {
    if (!s.faultActive || !s.armed || !s.refused || typeof file !== "string") return false;
    const match = PINNED_OWNER.exec(file);
    if (!match) return false;
    try {
      const pinned = fs.fstatSync(Number(match[1])), canonical = fs.lstatSync(lockPath);
      return pinned.isDirectory() && pinned.dev === s.refused.dev && pinned.ino === s.refused.ino &&
        canonical.dev === s.refused.dev && canonical.ino === s.refused.ino && read(file, "utf8") === s.refused.record;
    } catch { return false; }
  };
  const injected = (): void => { s.injected += 1; mark("inject"); };
  // Only the canonical rmdir that follows the release's real owner unlink of the refused acquisition: the name
  // still holds the refused inode, now empty.
  const ownerlessRefusedCanonical = (target: unknown): boolean => {
    if (plan.fault !== "rmdir-eperm" || !s.faultActive || !s.armed || !s.refused || s.ownerUnlinks === 0 ||
      typeof target !== "string" || path.resolve(target) !== lockPath) return false;
    try {
      const canonical = fs.lstatSync(lockPath);
      return canonical.isDirectory() && canonical.dev === s.refused.dev && canonical.ino === s.refused.ino &&
        fs.readdirSync(lockPath).length === 0;
    } catch { return false; }
  };
  // Pass-through: the original callback, arguments and handle. Only recorded during the first publication, and
  // only when the lock domain itself is the direct scheduler.
  vi.spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
    try {
      if (s.inFlight && scheduledByLockDomain()) {
        s.lockTimers.push({ ms: Number(args[1] ?? 0), step: steps.length, afterInject: s.injected > 0 });
      }
    } catch { /* observation never changes the call */ }
    return schedule(...args);
  }) as typeof setTimeout);

  vi.spyOn(f.local, "publishWithReceipt").mockImplementation((event, held, fromHub) => {
    const first = !s.started && event.text === INGRESS_TEXT;
    if (first) { s.started = true; s.inFlight = true; }
    const pending = publishWithReceipt(event, held, fromHub);
    // Observe only: the bridge receives this very promise.
    if (first) void pending.then(() => settle(true), (error: unknown) => settle(false, error));
    return pending;
  });
  // Pass-through: the hub's own state write fence, with the original receiver, arguments and thrown error. Records
  // each refusal it throws during the first publication, with its own property descriptors as thrown.
  vi.spyOn(f.hub, "withStateWriteFence").mockImplementation(function <T>(this: MeshStore, ...args: [() => T]): T {
    try { return Reflect.apply(stateWriteFence, this, args) as T; }
    catch (error) {
      try { if (s.inFlight) s.fenceRefusals.push({ error, own: Object.getOwnPropertyDescriptors(error) }); }
      catch { /* observation never changes the call */ }
      throw error;
    }
  });
  vi.spyOn(f.hub, "publish").mockImplementation(input => {
    if (input.text === INGRESS_TEXT) mark("publish-start");
    return publish(input);
  });
  vi.spyOn(fs, "mkdirSync").mockImplementation((target, options) => {
    if (!s.inFlight || typeof target !== "string" || path.resolve(target) !== lockPath) return mkdir(target, options);
    try {
      const result = mkdir(target, options);
      mark("acquire");
      return result;
    } catch (error) {
      if (codeOf(error) === "EEXIST") {
        mark("acquire-refused");
        let current: string | undefined;
        try { current = read(ownerPath, "utf8"); } catch { /* ownerless or gone */ }
        if (s.refused && current === s.refused.record) s.probes.refusedRecordInPlace += 1;
        else s.probes.other += 1;
      }
      throw error;
    }
  });
  vi.spyOn(fs, "rmdirSync").mockImplementation((...args: Parameters<typeof fs.rmdirSync>) => {
    const [target] = args;
    // Fixed refusal BEFORE the exact canonical rmdir: the ownerless canonical directory stays in place.
    if (ownerlessRefusedCanonical(target)) { injected(); throw injectedError; }
    rmdir(...args);
    if (typeof target === "string" && path.resolve(target) === lockPath) mark("release");
  });
  vi.spyOn(DatabaseSync.prototype, "exec").mockImplementation(function (this: DatabaseSync, sql: string) {
    try { return exec.call(this, sql); }
    catch (error) {
      if (s.inFlight && this !== s.raw && sql.trim().toUpperCase() === "BEGIN IMMEDIATE" && sqliteBusy(error)) {
        s.refusals += 1;
        mark("refusal");
        if (!s.refused) {
          try {
            // The fence runs under the publication's own canonical lock: capture that exact custody.
            const record = read(ownerPath, "utf8"), stat = fs.lstatSync(lockPath);
            if (stat.isDirectory() && record.split("\n")[1] === String(process.pid)) {
              s.refused = { record, dev: stat.dev, ino: stat.ino };
              s.armed = s.faultActive;
            }
          } catch { /* not captured: the assertions fail explicitly */ }
        }
        if (s.refusals >= plan.rollbackAfterRefusals) rollbackSqlite();
      }
      throw error;
    }
  });
  vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
    const result = append(file, data, options);
    if (typeof file === "string" && path.resolve(file) === eventsPath && String(data).includes(INGRESS_TEXT)) {
      s.appended += 1;
      mark("append");
    }
    return result;
  });
  if (plan.fault === "unlink-eperm" || plan.fault === "rmdir-eperm") {
    const fault = plan.fault;
    vi.spyOn(fs, "unlinkSync").mockImplementation(target => {
      const bound = refusedPin(target);
      // Fixed refusal BEFORE the pinned owner unlink: the owner record stays in place.
      if (bound && fault === "unlink-eperm") { injected(); throw injectedError; }
      unlink(target);
      // The release's own unlink of exactly the refused record, through the descriptor pinned on its inode.
      if (bound) { s.ownerUnlinks += 1; mark("owner-unlink"); }
    });
  } else if (plan.fault !== undefined) {
    const fault = plan.fault;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (refusedPin(file)) {
        // The release cannot prove its pinned owner: custody is unknown to it and stays in place.
        if (fault === "proof-read-eio") { injected(); throw injectedError; }
        if (!s.successor) {
          // After the release's real proof read, an external recoverer detaches our whole directory (as stale
          // recovery does) and a LIVE successor publishes a complete record at the name: the interleaving the
          // ordinary pinned-release tests use, here inside a retained held-ingress lifetime.
          const result = (read as (...args: unknown[]) => unknown)(file, options);
          injected();
          rename(lockPath, s.reclaimedReceipt);
          mkdir(lockPath, { mode: 0o700 });
          const pid = fault === "reclaim-same-process" ? process.pid : process.ppid;
          const record = `successor-${randomUUID()}\n${pid}\n${Date.now()}\n`;
          write(path.join(lockPath, "owner"), record, { encoding: "utf8", flag: "wx", mode: 0o600 });
          s.successor = { record, ino: fs.lstatSync(lockPath).ino };
          return result;
        }
      }
      return (read as (...args: unknown[]) => unknown)(file, options);
    }) as typeof fs.readFileSync);
  }
  cleanups.push(async () => {
    // Restore the fault first: no later release, stop or teardown step can meet it.
    restoreFault();
    await f.bridge.stop();
    await f.running();
    rollbackSqlite();
    s.raw?.close();
  });
  const holdSqlite = (): void => {
    s.raw = new DatabaseSync(path.join(root, "state.db"));
    s.raw.exec("PRAGMA busy_timeout = 0");
    s.raw.exec("BEGIN IMMEDIATE");
    s.raw.prepare("UPDATE meta SET value = value WHERE name = 'commit_no'").run();
    s.sqliteHeld = true;
  };
  return Object.assign(s, { holdSqlite, restoreFault, kinds: () => steps.map(step => step.kind) });
};
type Instrumented = ReturnType<typeof instrument>;

// One real held-ingress publication through the running bridge; stop and join the bridge after it settles.
// With `untilDecision`, first let the toLocal direction decide on the settled rejection (bounded): either run()
// ends, or the existing outer retry branch logs its retry.
const runFirstIngress = async (f: Fixture, s: Instrumented, untilDecision = false) => {
  f.launch();
  await within(f.idle, "real empty toLocal page before SQLite contention");
  await new Promise<void>(resolve => setImmediate(resolve));
  s.holdSqlite();
  const sent = await f.far.publish({ topic: TOPIC, kind: "ask", from: f.sender, to: f.recipient.id,
    text: INGRESS_TEXT, durable: true });
  const outcome = await within(s.settled, "first held-ingress publication settlement");
  const decision = untilDecision ? await within(Promise.race([f.running()!.then(() => "run-ended" as const),
    f.toLocalRetry.then(() => "outer-retry" as const)]), "toLocal direction decision after the settled publication") : undefined;
  await f.bridge.stop();
  await f.running();
  s.restoreFault();
  return { sent, outcome, decision };
};
const observation = (label: string, f: Fixture, s: Instrumented, outcome: { fulfilled: boolean; error?: unknown },
  decision?: string) =>
  console.log(`${label} ` + JSON.stringify({ steps: counts(s.kinds()), firstSteps: s.kinds().slice(0, 8),
    refusals: s.refusals, injected: s.injected, ownerUnlinks: s.ownerUnlinks, appended: s.appended, probes: s.probes,
    lockTimers: s.lockTimers.length, lockTimersAfterInject: s.lockTimers.filter(timer => timer.afterInject).length,
    firstLockTimerMs: s.lockTimers.slice(0, 6).map(timer => timer.ms), decision,
    offLockMs: toLocalOffLockMs(f.logs), ingressLines: ingressRefusalLines(f.logs), fulfilled: outcome.fulfilled,
    rejection: shape(outcome.error), rejectionCarriesInjectedFault: causeChainIncludes(outcome.error, s.injectedError),
    bridgeFailure: shape(f.failure()) }));

describe.skipIf(process.platform !== "linux")("held ingress canonical release refusal", () => {
  it.each([
    { fault: "unlink-eperm" as const, label: "EPERM before the pinned owner unlink" },
    { fault: "proof-read-eio" as const, label: "EIO on the pinned owner proof (unknown custody)" },
    { fault: "rmdir-eperm" as const, label: "EPERM before the canonical rmdir after the real owner unlink" },
  ])("$label: no off-lock time, sleep or retained retry while canonical custody may remain", async ({ fault }) => {
    const f = await ingressFixture(fault);
    const s = instrument(f, { fault, rollbackAfterRefusals: 1 });
    const { sent, outcome, decision } = await runFirstIngress(f, s, true);
    observation("INGRESS_RELEASE_REFUSAL", f, s, outcome, decision);
    const kinds = s.kinds(), ownerless = fault === "rmdir-eperm";
    expect(s.refused, "a genuine pre-effect SQLite fence refusal under the publication's own canonical lock").toBeDefined();
    expect(s.refusals).toBe(1);
    expect(s.ownerUnlinks, "only the partial case's release really unlinks the refused record (pinned)").toBe(ownerless ? 1 : 0);
    expect(s.injected, "the fixed fault met the release of that exact refused acquisition").toBeGreaterThanOrEqual(1);
    const inject = kinds.indexOf("inject"), settledAt = kinds.indexOf("settled");
    expect(kinds.slice(0, inject + 1))
      .toEqual(["publish-start", "acquire", "refusal", ...(ownerless ? ["owner-unlink"] : []), "inject"]);
    expect(settledAt).toBeGreaterThan(inject);
    expect(outcome.fulfilled).toBe(false);
    const afterRefusedRelease = kinds.slice(inject + 1, settledAt);
    expect.soft(afterRefusedRelease.filter(kind => kind === "publish-start"),
      "the retained helper must not replay the publication after an unconfirmed release").toEqual([]);
    expect.soft(afterRefusedRelease.filter(kind => kind === "acquire" || kind === "acquire-refused"),
      "no retained canonical probe may follow an unconfirmed release").toEqual([]);
    // Even a sleep that never replays: no helper backoff or lock wait is scheduled after the unconfirmed release.
    expect.soft(s.lockTimers.filter(timer => timer.afterInject),
      "no retained lock-domain sleep while canonical custody may remain").toEqual([]);
    // The outer bridge sleep: its retry line stays absent and the bridge run ends on this failure.
    expect.soft(f.logs.filter(message => message.includes("(toLocal); retrying in")),
      "no outer bridge retry sleep after an unconfirmed release").toEqual([]);
    expect.soft(decision, "the unconfirmed release is not retryable: the bridge run ends").toBe("run-ended");
    expect.soft(f.failure(), "the bridge fails with the publication's own rejection").toBe(outcome.error);
    // Non-vacuous diagnostics: the one genuine refusal is still counted, and the completed line reports zero.
    expect.soft(ingressRefusalLines(f.logs), "one completed ingress line: 1 fence refusal, 0 off-lock ms")
      .toEqual([{ fenceRefusals: 1, retainedOffLockMs: 0 }]);
    const offLockMs = toLocalOffLockMs(f.logs);
    expect.soft(offLockMs, "a toLocal line carrying retainedOffLockMs exists").not.toEqual([]);
    expect.soft(offLockMs.filter(ms => ms !== 0),
      "no retained off-lock time while canonical custody may remain").toEqual([]);
    // Truthful custody: the refused inode remains exactly, with its record (or ownerless after the real unlink).
    const canonical = fs.lstatSync(s.lockPath);
    expect(canonical.isDirectory()).toBe(true);
    expect(canonical.dev).toBe(s.refused!.dev);
    expect(canonical.ino).toBe(s.refused!.ino);
    if (ownerless) expect(fs.readdirSync(s.lockPath), "the ownerless canonical directory remains").toEqual([]);
    else {
      expect(fs.readdirSync(s.lockPath)).toEqual(["owner"]);
      expect(fs.readFileSync(s.ownerPath, "utf8")).toBe(s.refused!.record);
    }
    expect(s.appended).toBe(0);
    expect(f.hub.read({ topic: TOPIC, after: 0 })).toEqual([]);
    expect(f.cursorAfter()).toBeLessThan(sent.sequence);
    expect(queueTickets(f.hub.root)).toEqual([]);
    // The rejection keeps the ORIGINAL certified fence refusal and the ORIGINAL injected native error, by identity.
    expect(s.fenceRefusals, "exactly one actual refusal thrown by the hub's state write fence").toHaveLength(1);
    const { error: fenceRefusal, own } = s.fenceRefusals[0]!;
    expect((fenceRefusal as { busyCode?: unknown }).busyCode, "the certified zero-wait SQLite refusal").toBe("FABRIC_MESH_STATE_BUSY");
    const rejection = outcome.error as { name?: unknown; code?: unknown; retryable?: unknown; fenceRefusal?: unknown; cause?: unknown };
    expect.soft([rejection.name, rejection.code, rejection.retryable], "a nonretryable release-unconfirmed rejection")
      .toEqual(["MeshLockReleaseUnconfirmedError", "FABRIC_MESH_LOCK_RELEASE_UNCONFIRMED", false]);
    expect.soft(rejection.fenceRefusal, "the same original fence refusal object").toBe(fenceRefusal);
    expect.soft(rejection.cause, "the same injected native release error object, not a copy").toBe(s.injectedError);
    expect.soft(Object.getOwnPropertyDescriptors(fenceRefusal), "the original fence refusal is not mutated").toEqual(own);
  });

  it("positive control: confirmed releases still retry in one retained lifetime and account only off-lock time", async () => {
    const f = await ingressFixture("confirmed");
    const s = instrument(f, { rollbackAfterRefusals: 2 });
    const { sent, outcome } = await runFirstIngress(f, s);
    observation("INGRESS_RELEASE_CONFIRMED", f, s, outcome);
    const kinds = s.kinds();
    expect(outcome.fulfilled).toBe(true);
    expect(s.refusals).toBe(2);
    expect(s.injected).toBe(0);
    const appendAt = kinds.indexOf("append");
    // Each replay follows a native rmdir of the canonical lock (a confirmed release), then reacquires it.
    expect(kinds.slice(0, appendAt + 1)).toEqual(["publish-start", "acquire", "refusal", "release",
      "publish-start", "acquire", "refusal", "release", "publish-start", "acquire", "append"]);
    // Each confirmed release is followed by exactly one native backoff scheduled by the lock domain's own sleep
    // site, within the helper's legitimate 2-20 ms, and only then by the replay: the attribution is live.
    expect(s.lockTimers.map(timer => [kinds[timer.step - 1], kinds[timer.step]]),
      "one lock-domain backoff between each confirmed release and its replay")
      .toEqual([["release", "publish-start"], ["release", "publish-start"]]);
    for (const timer of s.lockTimers) {
      expect(timer.ms).toBeGreaterThanOrEqual(2);
      expect(timer.ms).toBeLessThanOrEqual(20);
    }
    const physicalOffLockMs = [3, 7].reduce((sum, release) => sum + s.steps[release + 2]!.at - s.steps[release]!.at, 0);
    const recovered = f.logs.filter(message => message.startsWith("bridge ingress busy streak recovered (toLocal)"));
    expect(recovered).toHaveLength(1);
    const logged = /; (\d+) fence refusals; retainedOffLockMs=(\d+)$/.exec(recovered[0]!);
    expect(logged).not.toBeNull();
    expect(Number(logged![1])).toBe(2);
    const offLockMs = Number(logged![2]);
    expect(offLockMs, "confirmed releases are accounted as retained off-lock time").toBeGreaterThan(0);
    // Sampling skew per physical edge only; fence-held time is never counted as off-lock time.
    expect(offLockMs).toBeLessThanOrEqual(Math.round(physicalOffLockMs) + 2 * 2 + 2);
    expect(f.logs.some(message => message.includes("(toLocal); retrying in"))).toBe(false);
    expect(s.appended).toBe(1);
    expect(f.hub.read({ topic: TOPIC, after: 0 })).toMatchObject([{ text: INGRESS_TEXT, verification: "bridge" }]);
    expect(f.cursorAfter()).toBe(sent.sequence);
    expect(fs.existsSync(s.lockPath)).toBe(false);
    expect(queueTickets(f.hub.root)).toEqual([]);
  });

  it.each([
    { fault: "reclaim-same-process" as const, label: "a live same-process successor" },
    { fault: "reclaim-foreign-live" as const, label: "a live foreign-process successor" },
  ])("never deletes $label that took the name after our directory was detached", async ({ fault }) => {
    expect(process.ppid).not.toBe(process.pid);
    const f = await ingressFixture(fault);
    const s = instrument(f, { fault, rollbackAfterRefusals: 1 });
    const { sent, outcome, decision } = await runFirstIngress(f, s, true);
    observation("INGRESS_RELEASE_SUCCESSOR", f, s, outcome, decision);
    expect(s.refused).toBeDefined();
    expect(s.injected).toBe(1);
    expect(s.successor).toBeDefined();
    expect(outcome.fulfilled).toBe(false);
    // The successor's exact record and inode survive our release and every later attempt.
    const canonical = fs.lstatSync(s.lockPath);
    expect(canonical.ino).toBe(s.successor!.ino);
    expect(fs.readdirSync(s.lockPath)).toEqual(["owner"]);
    expect(fs.readFileSync(s.ownerPath, "utf8")).toBe(s.successor!.record);
    // Our detached directory keeps our exact record: the release never acted on a broken precondition.
    const receipt = fs.lstatSync(s.reclaimedReceipt);
    expect(receipt.ino).toBe(s.refused!.ino);
    expect(fs.readdirSync(s.reclaimedReceipt)).toEqual(["owner"]);
    expect(fs.readFileSync(path.join(s.reclaimedReceipt, "owner"), "utf8")).toBe(s.refused!.record);
    expect(s.appended).toBe(0);
    expect(f.hub.read({ topic: TOPIC, after: 0 })).toEqual([]);
    expect(f.cursorAfter()).toBeLessThan(sent.sequence);
    expect(queueTickets(f.hub.root)).toEqual([]);
    // Custody moved: no retained replay, canonical probe or lock-domain sleep follows, the bridge run ends on this
    // rejection, and the one genuine refusal is still counted with zero off-lock time.
    const kinds = s.kinds(), afterMoved = kinds.slice(kinds.indexOf("inject") + 1, kinds.indexOf("settled"));
    expect.soft(afterMoved.filter(kind => kind === "publish-start" || kind === "acquire" || kind === "acquire-refused"),
      "no retained replay or canonical probe after custody moved").toEqual([]);
    expect.soft(s.lockTimers.filter(timer => timer.afterInject), "no retained lock-domain sleep after custody moved").toEqual([]);
    expect.soft(decision, "the moved-custody refusal is not retryable: the bridge run ends").toBe("run-ended");
    expect.soft(f.failure(), "the bridge fails with the publication's own rejection").toBe(outcome.error);
    expect.soft(ingressRefusalLines(f.logs), "one completed ingress line: 1 fence refusal, 0 off-lock ms")
      .toEqual([{ fenceRefusals: 1, retainedOffLockMs: 0 }]);
    expect.soft([...new Set(toLocalOffLockMs(f.logs))], "toLocal off-lock lines exist and all report 0 ms").toEqual([0]);
    // No native failure exists for moved custody: no cause is invented; the original fence refusal is still kept.
    expect(s.fenceRefusals, "exactly one actual refusal thrown by the hub's state write fence").toHaveLength(1);
    expect.soft((outcome.error as { fenceRefusal?: unknown }).fenceRefusal).toBe(s.fenceRefusals[0]!.error);
    expect.soft((outcome.error as { cause?: unknown }).cause, "no invented native cause").toBeUndefined();
  });
});
