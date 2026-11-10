import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { buildSync } from "esbuild";
import { afterEach, expect, it } from "vitest";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshStore } from "../src/mesh/store.js";
import { readHostLeases } from "../src/topology/host-leases.js";

type Message = Record<string, unknown> & { pid: number };
type ChildState = {
  child: ChildProcess;
  messages: Message[];
  stdout: () => string;
  stderr: () => string;
  closed: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  next: (key: string) => Promise<Message>;
  send: (command: string) => void;
};

const roots: string[] = [];
const children: ChildState[] = [];
const stores: MeshStore[] = [];
const SEED_RECEIPTS = 24_000;
const MAX_RECEIPTS = 100_000;
const PUBLISHERS = 4;
const PUBLISHES_PER_PROCESS = 40;
const LOCK_TIMEOUT_MS = 120_000;
const TOPIC = "mesh.keyed-publication-fifo";
const PARTICIPANT_ID = "session:mesh-keyed-fifo-participant";

const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
const withTimeout = async <T>(work: Promise<T>, ms: number, label: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<T>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms} ms`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
const waitFor = async (predicate: () => boolean, ms: number, label: string): Promise<void> => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`${label} was not observed within ${ms} ms`);
    await delay(10);
  }
};

const observeChild = (child: ChildProcess): ChildState => {
  let stdout = "", stderr = "", buffer = "";
  const messages: Message[] = [];
  const waiters: Array<{ key: string; resolve: (message: Message) => void; reject: (error: Error) => void }> = [];
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  child.stdout?.on("data", chunk => {
    const text = String(chunk);
    stdout += text;
    buffer += text;
    for (let at = buffer.indexOf("\n"); at >= 0; at = buffer.indexOf("\n")) {
      const line = buffer.slice(0, at);
      buffer = buffer.slice(at + 1);
      if (!line) continue;
      const message = JSON.parse(line) as Message;
      messages.push(message);
      for (const waiter of waiters.filter(item => item.key in message)) {
        waiters.splice(waiters.indexOf(waiter), 1);
        waiter.resolve(message);
      }
    }
  });
  child.stderr?.on("data", chunk => { stderr += String(chunk); });
  child.once("close", (code, signal) => {
    for (const waiter of waiters.splice(0)) {
      waiter.reject(new Error(`native child exited ${code}/${signal} before ${waiter.key}: ${stderr}${stdout}`));
    }
  });
  return {
    child, messages, stdout: () => stdout, stderr: () => stderr, closed,
    next: key => {
      const existing = messages.find(message => key in message);
      if (existing) return Promise.resolve(existing);
      if (child.exitCode !== null || child.signalCode !== null) {
        return Promise.reject(new Error(`native child exited before ${key}: ${stderr}${stdout}`));
      }
      return new Promise((resolve, reject) => waiters.push({ key, resolve, reject }));
    },
    send: command => {
      if (!child.stdin?.writable) throw new Error(`native child stdin closed before ${command}`);
      child.stdin.write(`${command}\n`);
    },
  };
};

const startWorker = (bundle: string, base: string, role: string, identity: string, count = 0): ChildState => {
  const child = spawn(process.execPath, [bundle, role, path.join(base, "mesh"), identity,
    String(count), String(MAX_RECEIPTS)], {
    cwd: process.cwd(), stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, PI_CODING_AGENT_DIR: base, PI_FABRIC_LOCK_STATS: "0", PI_FABRIC_MESH_CHECKPOINT_LOG: "off" },
  });
  const state = observeChild(child);
  children.push(state);
  return state;
};

const seedRepresentativeReceipts = (root: string): void => {
  const directory = path.join(root, "event-receipts");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const from = { id: "session:fifo-seed", name: "fifo-seed", kind: "main" };
  const createdAt = Date.now();
  for (let index = 0; index < SEED_RECEIPTS; index += 1) {
    const dedupeKey = `fifo-seed:${index}`;
    const file = path.join(directory, `${createHash("sha256").update(dedupeKey).digest("hex")}.json`);
    fs.writeFileSync(file, JSON.stringify({ id: `seed-event-${index}`, sequence: index + 1, dedupeKey,
      topic: TOPIC, kind: "message", from, createdAt }), { mode: 0o600 });
  }
  // These are a test-owned receipt population, not published mesh events. Keep subsequent real event
  // sequences beyond their representative receipt range while avoiding a synthetic historical log.
  fs.writeFileSync(path.join(root, "sequence"), String(SEED_RECEIPTS), { mode: 0o600 });
};

const queueTickets = (root: string): string[] => {
  try { return fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort(); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
};
const ticketPid = (name: string): number => Number(name.split("-")[1]);
const canonicalOwnerPid = (root: string): number | undefined => {
  try {
    const pid = Number(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split("\n")[1]);
    return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
};
const processIsLive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
};

const participantKey = (id: string): string =>
  `topology/participants/${createHash("sha256").update(id).digest("hex")}`;
const participantLease = (root: string) => readHostLeases(root).get(PARTICIPANT_ID);

afterEach(async () => {
  for (const child of children) {
    if (child.child.exitCode === null && child.child.signalCode === null) child.child.kill("SIGTERM");
  }
  await Promise.all(children.map(child => child.closed));
  children.splice(0);
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

it("publishes keyed events exactly once across native FIFO waiters while a participant registers and renews", async () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-keyed-publication-fifo-"));
  roots.push(base);
  const root = path.join(base, "mesh");
  fs.mkdirSync(root, { mode: 0o700 });
  seedRepresentativeReceipts(root);
  expect(SEED_RECEIPTS + 1 + 10_000).toBeLessThan(MAX_RECEIPTS);

  const bundle = path.join(base, "mesh-keyed-publication-fifo-worker.mjs");
  buildSync({ entryPoints: [path.resolve("tests/fixtures/mesh-keyed-publication-fifo-worker.ts")],
    bundle: true, platform: "node", format: "esm", outfile: bundle });
  const observer = new MeshStore(root, 65_536, 1_000, { lockTimeoutMs: LOCK_TIMEOUT_MS, maxDedupeReceipts: MAX_RECEIPTS });
  stores.push(observer);

  // A real ParticipantDirectory activation establishes the native registration before contention.
  const participant = startWorker(bundle, base, "participant", PARTICIPANT_ID);
  const participantReady = await withTimeout(participant.next("ready"), 10_000, "participant process readiness");
  expect(participantReady.pid).toBe(participant.child.pid);
  participant.send("start");
  const registration = await withTimeout(participant.next("registered"), 15_000, "native participant registration");
  expect(registration.sourceCalls).toBeGreaterThanOrEqual(1);
  const registeredLease = participantLease(root);
  expect(registeredLease).toBeDefined();
  expect(registeredLease!.expiresAt).toBeGreaterThan(registeredLease!.updatedAt);
  expect(observer.get(participantKey(PARTICIPANT_ID))?.value).toMatchObject({
    id: PARTICIPANT_ID, kind: "root", rootId: PARTICIPANT_ID, ownerHostId: PARTICIPANT_ID,
  });

  // The separate native holder owns the canonical .lock while every keyed publisher starts its
  // real MeshStore.publish path. Admission evidence comes from the live ticket files, not a timer.
  const holder = startWorker(bundle, base, "holder", "holder");
  await withTimeout(holder.next("ready"), 10_000, "lock-holder process readiness");
  holder.send("hold");
  const entered = await withTimeout(holder.next("entered"), 10_000, "native canonical lock acquisition");
  expect(entered.pid).toBe(holder.child.pid);
  expect(canonicalOwnerPid(root)).toBe(holder.child.pid);
  expect(processIsLive(holder.child.pid!)).toBe(true);

  const publishers = Array.from({ length: PUBLISHERS }, (_, index) =>
    startWorker(bundle, base, "publisher", `writer-${index}`, PUBLISHES_PER_PROCESS));
  const ready = await Promise.all(publishers.map(child => withTimeout(child.next("ready"), 10_000, "publisher process readiness")));
  ready.forEach((message, index) => expect(message.pid).toBe(publishers[index]!.child.pid));
  publishers.forEach(child => child.send("go"));

  // Wait for physical receipts from the real holder, all four native publisher processes, and the
  // real participant heartbeat. The canonical owner stays live throughout this observation.
  const expectedWaiterPids = [...publishers.map(state => state.child.pid!), participant.child.pid!];
  await waitFor(() => {
    const names = queueTickets(root);
    return canonicalOwnerPid(root) === holder.child.pid &&
      expectedWaiterPids.every(pid => names.some(name => ticketPid(name) === pid));
  }, 15_000, "live native FIFO waiter receipts");
  const liveTickets = queueTickets(root);
  const waiterReceipts = liveTickets.filter(name => expectedWaiterPids.includes(ticketPid(name))).map(name => {
    const stat = fs.lstatSync(path.join(meshLockQueueDirectory(root), name));
    expect(stat.isFile()).toBe(true);
    expect(processIsLive(ticketPid(name))).toBe(true);
    return { name, pid: ticketPid(name), inode: stat.ino, size: stat.size };
  });
  expect(new Set(waiterReceipts.map(receipt => receipt.pid))).toEqual(new Set(expectedWaiterPids));
  expect(canonicalOwnerPid(root)).toBe(holder.child.pid);

  // A real participant heartbeat renews its host lease while its shared-state confirmation and
  // the keyed publishers are waiting behind that same live canonical owner.
  await waitFor(() => {
    const lease = participantLease(root);
    const names = queueTickets(root);
    return !!lease && lease.updatedAt > registeredLease!.updatedAt &&
      names.some(name => ticketPid(name) === participant.child.pid);
  }, 15_000, "participant lease renewal with a live FIFO ticket");
  const renewedWhileContended = participantLease(root);
  expect(renewedWhileContended!.updatedAt).toBeGreaterThan(registeredLease!.updatedAt);
  expect(canonicalOwnerPid(root)).toBe(holder.child.pid);
  console.info("KEYED_FIFO_LIVE_RECEIPTS " + JSON.stringify({ holderPid: holder.child.pid,
    ownerPid: canonicalOwnerPid(root), waiterReceipts, participantLeaseUpdatedAt: renewedWhileContended!.updatedAt,
    registeredLeaseUpdatedAt: registeredLease!.updatedAt, seedReceiptCount: SEED_RECEIPTS, maxReceipts: MAX_RECEIPTS }));

  holder.send("release");
  const released = await withTimeout(holder.next("released"), 10_000, "canonical lock release");
  expect(released.pid).toBe(holder.child.pid);
  const holderExit = await withTimeout(holder.closed, 10_000, "lock-holder terminal state");
  expect(holderExit).toEqual({ code: 0, signal: null });

  const publisherResults = await Promise.all(publishers.map(async (state, index) => {
    const result = await withTimeout(state.next("done"), LOCK_TIMEOUT_MS, `publisher ${index} completion`);
    const exit = await withTimeout(state.closed, 10_000, `publisher ${index} native exit`);
    expect(exit, state.stderr()).toEqual({ code: 0, signal: null });
    expect(result.receiptReadsUnderOwnLock).toBe(0);
    expect(result.receiptReads).toEqual(expect.any(Array));
    const counts = result.receiptReads as number[];
    expect(counts.length).toBeGreaterThanOrEqual(PUBLISHES_PER_PROCESS);
    expect(Math.min(...counts)).toBeGreaterThanOrEqual(SEED_RECEIPTS);
    return result;
  }));

  participant.send("stop");
  const stopped = await withTimeout(participant.next("stopped"), 30_000, "participant directory shutdown");
  expect(stopped.sourceCalls).toBeGreaterThanOrEqual(2);
  const participantExit = await withTimeout(participant.closed, 10_000, "participant native exit");
  expect(participantExit, participant.stderr()).toEqual({ code: 0, signal: null });

  const expectedEvents = publisherResults.flatMap(result => result.first as Array<Record<string, unknown>>);
  const replayedEvents = publisherResults.flatMap(result => result.replays as Array<Record<string, unknown>>);
  expect(expectedEvents).toHaveLength(PUBLISHERS * PUBLISHES_PER_PROCESS);
  expect(replayedEvents).toEqual(expectedEvents);
  const actualEvents = observer.read({ topic: TOPIC, after: 0, limit: 1_000 });
  expect(actualEvents).toHaveLength(expectedEvents.length);
  expect(actualEvents.map(event => event.dedupeKey).sort()).toEqual(expectedEvents.map(event => event.dedupeKey as string).sort());
  expect(new Set(actualEvents.map(event => event.id)).size).toBe(expectedEvents.length);
  expect(new Set(actualEvents.map(event => event.sequence)).size).toBe(expectedEvents.length);
  expect(actualEvents.every(event => event.sequence > SEED_RECEIPTS)).toBe(true);
  expect(queueTickets(root)).toEqual([]);
  expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  const receiptNames = fs.readdirSync(path.join(root, "event-receipts"));
  expect(receiptNames.filter(name => name.endsWith(".pending.json"))).toEqual([]);
  expect(receiptNames.filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toHaveLength(SEED_RECEIPTS + expectedEvents.length);

  console.info("KEYED_FIFO_RESULT " + JSON.stringify({ holder: { pid: holder.child.pid, exit: holderExit },
    publishers: publishers.map((state, index) => ({ pid: state.child.pid, exit: { code: 0, signal: null },
      keyedCalls: (publisherResults[index]!.first as unknown[]).length,
      replays: (publisherResults[index]!.replays as unknown[]).length,
      receiptReads: (publisherResults[index]!.receiptReads as number[]).length })),
    participant: { pid: participant.child.pid, exit: participantExit, sourceCalls: stopped.sourceCalls,
      registeredLeaseUpdatedAt: registeredLease!.updatedAt, renewedLeaseUpdatedAt: renewedWhileContended!.updatedAt },
    seedReceipts: SEED_RECEIPTS, publishedEvents: actualEvents.length, stableReplay: true, pendingIntents: 0 }));
}, 300_000); // Allow native workers and the 24k-receipt seed to finish on Windows.
