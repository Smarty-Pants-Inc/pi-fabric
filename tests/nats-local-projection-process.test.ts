import { createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { MeshStore } from "../src/mesh/store.js";
import { hostLeasePath, readHostLease, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { ensureTransportStream } from "../src/transport/nats/provision.js";
import { openNatsKv, LEASES_BUCKET, PRESENCE_BUCKET } from "../src/transport/nats/kv.js";
import { transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const MACHINE = "machine-a";
const WORKER = path.resolve("tests/fixtures/nats-projection-process-worker.mjs");
const token = (value: string): string => createHash("sha256").update(value, "utf8").digest("hex").slice(0, 32);
const projectionLeaseKey = (id: string): string => `${MACHINE}.h.${token(id)}`;
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
const bounded = async <T>(promise: Promise<T>, label: string, timeoutMs = 30_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs} ms`)), timeoutMs);
    })]);
  } finally { if (timer) clearTimeout(timer); }
};

type WorkerEvent = { pid: number; event: string; [key: string]: unknown };
interface WorkerHandle {
  readonly child: ChildProcess;
  readonly stderr: () => string;
  next(event: string, timeoutMs?: number): Promise<WorkerEvent>;
  close(): Promise<void>;
}
const startWorker = (options: Record<string, unknown>, onEvent?: (event: WorkerEvent) => void): WorkerHandle => {
  const child = spawn(process.execPath, [WORKER, JSON.stringify(options)], {
    cwd: process.cwd(), stdio: ["pipe", "pipe", "pipe"],
  });
  const events: WorkerEvent[] = [];
  const waiters: Array<() => void> = [];
  let buffered = "", stderr = "";
  child.stdout!.on("data", chunk => {
    buffered += String(chunk);
    for (let end = buffered.indexOf("\n"); end >= 0; end = buffered.indexOf("\n")) {
      const line = buffered.slice(0, end);
      buffered = buffered.slice(end + 1);
      try {
        const event = JSON.parse(line) as WorkerEvent;
        events.push(event);
        onEvent?.(event);
      } catch { stderr += `[stdout] ${line}\n`; }
    }
    for (const wake of waiters.splice(0)) wake();
  });
  child.stderr!.on("data", chunk => { stderr += String(chunk); });
  const exited = new Promise<[number | null, NodeJS.Signals | null]>(resolve => {
    child.once("close", (code, signal) => {
      resolve([code, signal]);
      for (const wake of waiters.splice(0)) wake();
    });
    child.once("error", error => { stderr += `[spawn] ${String(error)}\n`; for (const wake of waiters.splice(0)) wake(); });
  });
  return {
    child,
    stderr: () => stderr,
    next: async (event, timeoutMs = 30_000) => {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const failure = events.findIndex(item => item.event === "failure");
        if (failure >= 0 && event !== "failure") {
          const [item] = events.splice(failure, 1);
          throw new Error(`NATS projection worker ${child.pid} failed: ${String(item?.message)}\n${stderr}`);
        }
        const index = events.findIndex(item => item.event === event);
        if (index >= 0) return events.splice(index, 1)[0]!;
        if (child.exitCode !== null || child.signalCode !== null) {
          throw new Error(`NATS projection worker ${child.pid} exited (${child.exitCode ?? child.signalCode}) before "${event}": ${stderr}`);
        }
        const left = deadline - Date.now();
        if (left <= 0) throw new Error(`NATS projection worker ${child.pid}: no "${event}" within ${timeoutMs} ms: ${stderr}`);
        await new Promise<void>(resolve => {
          const timer = setTimeout(resolve, left);
          waiters.push(() => { clearTimeout(timer); resolve(); });
        });
      }
    },
    close: async () => {
      if (child.exitCode === null && child.signalCode === null) {
        child.stdin!.write(`${JSON.stringify({ command: "close" })}\n`);
        try { await bounded(exited.then(() => undefined), `worker ${child.pid} close`, 15_000); }
        catch {
          if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
          await bounded(exited.then(() => undefined), `worker ${child.pid} signal cleanup`, 5_000);
        }
      }
      await exited;
    },
  };
};

let harness: PathBHarness | undefined;
let root: string | undefined;
let workers: WorkerHandle[] = [];

describe.skipIf(!binary)("NATS projection multi-process startup", () => {
  beforeAll(async () => {
    harness = await createPathBHarness({ binary: binary!, tls: {} });
    const admin = await harness.connect(harness.users.bootstrap);
    const js = jetstream(admin);
    const manager = await jetstreamManager(admin);
    await openNatsKv(js, { bucket: LEASES_BUCKET }, true);
    await openNatsKv(js, { bucket: PRESENCE_BUCKET }, true);
    await ensureTransportStream(manager, {
      name: transportInboxStream(MACHINE),
      subjects: [transportInboxStreamSubject(MACHINE)],
      maxBytes: 1_073_741_824,
      maxAgeMs: 0,
      maxMessageBytes: 1_048_576,
      duplicateWindowMs: 600_000,
      replicas: 1,
    });
  }, 30_000);

  afterEach(async () => {
    for (const worker of workers.splice(0)) await worker.close();
    if (root) fs.rmSync(root, { recursive: true, force: true });
    root = undefined;
  });
  afterAll(async () => { await harness?.stop(); });

  it("starts three Fabric directories concurrently through held-lock contention without weakening owner or revision fences", async () => {
    if (!harness) throw new Error("Path B fixture was not initialized");
    root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-projection-process-"));
    const meshRoot = path.join(root, "mesh");
    fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });

    const leases = (await openNatsKv(jetstream(await harness.connect(harness.users.bootstrap)), { bucket: LEASES_BUCKET }, false))
      .withMachine(MACHINE);
    const now = Date.now();
    const revisionId = "session:projection-process-revision";
    const staleLease = {
      id: revisionId, rootId: "projection-root", identityId: "projection-identity", incarnation: "projection-old-incarnation",
      pid: process.pid, pidStartTime: "1", bootId: "12345678-1234-4234-8234-123456789abc",
      updatedAt: now, expiresAt: now + 120_000,
    };
    const revisionOne = await leases.put(projectionLeaseKey(revisionId), `${revisionId}#${staleLease.incarnation}`, staleLease, 0);

    const conflictId = "session:projection-process-conflict";
    const projectedConflict = {
      id: conflictId, rootId: "projection-root", identityId: "nats-owner", incarnation: "nats-stale-incarnation",
      pid: process.pid, pidStartTime: "1", bootId: "12345678-1234-4234-8234-123456789abc",
      updatedAt: now, expiresAt: now + 120_000,
    };
    await leases.put(projectionLeaseKey(conflictId), `${conflictId}#${projectedConflict.incarnation}`, projectedConflict, 0);

    const conflictingOwner: FabricHostLease = {
      id: conflictId, rootId: "real-local-root", identityId: "real-local-owner",
      incarnationToken: "real-conflicting-owner", startedAt: now - 1_000,
      updatedAt: now + 10_000, expiresAt: now + 120_000,
    };
    const mesh = new MeshStore(meshRoot, 65_536, 1_000);
    writeHostLease(mesh.root, conflictingOwner);
    mesh.closeState();

    const lockPath = path.join(meshRoot, "host-lease-locks", path.basename(hostLeasePath(meshRoot, revisionId), ".json"));
    const lockHeld = deferred<WorkerEvent>();
    const nats = harness.runtimeOptions(harness.users.machineA);
    const launchedAt = Date.now();
    for (let index = 0; index < 3; index++) {
      const hostId = `session:projection-process-${index}`;
      workers.push(startWorker({
        index,
        hostId,
        rootId: hostId,
        meshRoot,
        transportStateDir: path.join(root, `transport-${index}`),
        nats,
        holdLockPath: lockPath,
        holdLockMs: 800,
      }, event => { if (event.event === "lock-held") lockHeld.resolve(event); }));
    }

    await bounded(lockHeld.promise, "a real process holding the shared host-lease lock", 15_000);
    const currentLease = { ...staleLease, incarnation: "projection-new-incarnation", updatedAt: now + 1_000, expiresAt: now + 121_000 };
    const revisionTwo = await leases.put(projectionLeaseKey(revisionId), `${revisionId}#${currentLease.incarnation}`, currentLease, revisionOne);
    await Promise.all(workers.map(worker => worker.next("ready", 30_000)));
    expect(Date.now() - launchedAt).toBeLessThan(15_000);

    const projectedFile = JSON.parse(fs.readFileSync(hostLeasePath(meshRoot, revisionId), "utf8")) as {
      incarnationToken?: string; natsProjection?: { revision?: number; incarnation?: string };
    };
    expect(projectedFile).toMatchObject({
      incarnationToken: currentLease.incarnation,
      natsProjection: { revision: revisionTwo, incarnation: currentLease.incarnation },
    });
    expect(projectedFile.natsProjection?.revision).toBeGreaterThan(revisionOne);
    expect(readHostLease(meshRoot, conflictId)).toMatchObject({
      id: conflictId,
      rootId: conflictingOwner.rootId,
      identityId: conflictingOwner.identityId,
      incarnationToken: conflictingOwner.incarnationToken,
      updatedAt: conflictingOwner.updatedAt,
    });
  }, 60_000);
});
