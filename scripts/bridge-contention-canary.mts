import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { FabricControlPlane, type FabricControlAcceptance } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

/**
 * Cross-process bridge contention canary.  The parent owns the scratch directory after this
 * exits: this file deliberately never removes a store, a receipt, or a child-owned artifact.
 *
 * The hub runs the real mesh-bridge CLI. Its stdio command is a second real agent process. The
 * control planes below are real FabricControlPlane instances over the two SQLite stores; their
 * handlers are the receiver/ACK path, not a plain event listener.
 */

const SCRIPT = fileURLToPath(import.meta.url);
// The installed bridge is deliberately resolved from the package checkout, not from the
// temporary evidence root.  This remains correct when this file is esbuild-bundled elsewhere.
const BIN = path.resolve(process.cwd(), "bin/mesh-bridge");
const N = 50;
const LOCK_HOLD_MS = 3_000;
const DEADLINE_MS = 110_000;
const MESH_BYTES = 256 * 1024;
const MESH_READ = 500;
const HUB = "hub";
const AGENT = "agent";
const HUB_ID = "session:canary-hub-00000000";
const AGENT_ID = "session:canary-agent-00000000";

type Child = ChildProcess & { stderr: NonNullable<ChildProcess["stderr"]> };

interface Native {
  identity: MeshIdentity;
  ownerIncarnation: string;
}

interface TallyEntry {
  calls: number;
  accepted: number;
}

interface Tally {
  readonly expected: Set<string>;
  readonly entries: Map<string, TallyEntry>;
  invalid: number;
  duplicateCalls: number;
}

interface LockReceipt {
  readyAt: number;
  doneAt: number;
  holdMs: number;
}

const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const hostKey = (id: string): string => `topology/hosts/${hash(id)}`;
const participantKey = (id: string): string => `topology/participants/${hash(id)}`;
const sleep = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
let currentEvidenceRoot: string | undefined;

const parseFlags = (argv: string[]): Map<string, string> => {
  const flags = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i];
    const value = argv[i + 1];
    if (!key?.startsWith("--") || value === undefined) throw new Error(`bad canary argument: ${key ?? "<missing>"}`);
    flags.set(key.slice(2), value);
  }
  return flags;
};

const required = (flags: Map<string, string>, key: string): string => {
  const value = flags.get(key);
  if (!value) throw new Error(`--${key} is required`);
  return value;
};

const openStore = (root: string): MeshStore => {
  // Only this canary bootstraps its private empty stores; ordinary bridge opens still require import.
  SqliteStateStore.openSync(root, MESH_BYTES, MESH_READ, { initialize: "create" }).close();
  return new MeshStore(root, MESH_BYTES, MESH_READ, {
    stateBackend: "sqlite", lockProtocol: 3, lockTimeoutMs: 45_000, staleLockMs: 60_000,
  });
};

const installNative = async (store: MeshStore, id: string, name: string): Promise<Native> => {
  const now = Date.now();
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  const ownerIncarnation = `canary:${id}`;
  await store.put({ key: hostKey(id), identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 600_000,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 600_000 });
  writeParticipantFile(store.root, { key: participantKey(id), version: 1, updatedAt: now, updatedBy: identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp"], sessionId: name, startedAt: now, updatedAt: now,
    controlProtocol: "v1",
  } });
  return { identity, ownerIncarnation };
};

/** Watch-driven observation: the timer is only a finite failure bound, never a progress carrier. */
const observe = (root: string, condition: () => boolean, timeoutMs: number, label: string): Promise<void> =>
  new Promise((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined;
    let timer: NodeJS.Timeout | undefined;
    let lastError: unknown;
    const finish = (error?: Error): void => {
      watcher?.close();
      if (timer) clearTimeout(timer);
      error ? reject(error) : resolve();
    };
    const check = (): void => {
      try {
        if (condition()) finish();
      } catch (error) {
        lastError = error;
      }
    };
    watcher = fs.watch(root, check);
    watcher.on("error", error => finish(error));
    timer = setTimeout(() => finish(new Error(`${label} timed out: ${String(lastError ?? "not observed")}`)), timeoutMs);
    check();
  });

const waitForChildText = (child: Child, text: string, timeoutMs: number, label: string): Promise<void> =>
  new Promise((resolve, reject) => {
    let output = "";
    let timer: NodeJS.Timeout | undefined;
    const finish = (error?: Error): void => {
      if (timer) clearTimeout(timer);
      error ? reject(error) : resolve();
    };
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      output += chunk;
      if (output.includes(text)) finish();
    });
    child.once("error", error => finish(error));
    child.once("close", (code, signal) => {
      if (!output.includes(text)) finish(new Error(`${label} closed (${code}/${signal}): ${output}`));
    });
    timer = setTimeout(() => finish(new Error(`${label} did not emit ${JSON.stringify(text)}: ${output}`)), timeoutMs);
  });

interface LockProcess {
  child: Child;
  ready: Promise<number>;
  done: Promise<LockReceipt>;
}

const spawnLockHolder = (db: string, holdMs: number): LockProcess => {
  const child = spawn(process.execPath, [SCRIPT, "lock-holder", "--db", db, "--hold-ms", String(holdMs)], {
    stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "sqlite" },
  }) as Child;
  let output = "";
  let buffer = "";
  let readyDone = false;
  let doneDone = false;
  let resolveReady!: (value: number) => void;
  let rejectReady!: (error: Error) => void;
  let resolveDone!: (value: LockReceipt) => void;
  let rejectDone!: (error: Error) => void;
  const ready = new Promise<number>((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
  const done = new Promise<LockReceipt>((resolve, reject) => { resolveDone = resolve; rejectDone = reject; });
  void ready.catch(() => undefined);
  void done.catch(() => undefined);
  const readyTimer = setTimeout(() => {
    if (!readyDone) { readyDone = true; rejectReady(new Error(`lock holder did not acquire ${db}: ${output}`)); }
  }, 15_000);
  const doneTimer = setTimeout(() => {
    if (!doneDone) { doneDone = true; rejectDone(new Error(`lock holder did not finish ${db}: ${output}`)); }
  }, 20_000);
  const fail = (error: Error): void => {
    if (!readyDone) { readyDone = true; clearTimeout(readyTimer); rejectReady(error); }
    if (!doneDone) { doneDone = true; clearTimeout(doneTimer); rejectDone(error); }
  };
  child.stdout?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    output += chunk;
    buffer += chunk;
    for (;;) {
      const newline = buffer.indexOf("\n");
      if (newline < 0) break;
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      try {
        const value = JSON.parse(line) as Partial<LockReceipt> & { phase?: string };
        if (value.phase === "ready" && !readyDone && typeof value.readyAt === "number") {
          readyDone = true; clearTimeout(readyTimer); resolveReady(value.readyAt);
        }
        if (value.phase === "done" && !doneDone && typeof value.readyAt === "number" &&
            typeof value.doneAt === "number" && typeof value.holdMs === "number") {
          doneDone = true; clearTimeout(doneTimer);
          resolveDone({ readyAt: value.readyAt, doneAt: value.doneAt, holdMs: value.holdMs });
        }
      } catch { /* wait for the next complete JSON line */ }
    }
  });
  child.once("error", fail);
  child.once("close", (code, signal) => {
    if (code !== 0 || signal !== null) fail(new Error(`lock holder closed (${code}/${signal}): ${output}`));
  });
  return { child, ready, done };
};

const tally = (markers: readonly string[]): Tally => ({
  expected: new Set(markers), entries: new Map(), invalid: 0, duplicateCalls: 0,
});

const handlerFor = (state: Tally) => async (command: { data?: unknown; commandId: string }): Promise<FabricControlAcceptance> => {
  const marker = typeof command.data === "object" && command.data !== null &&
    typeof (command.data as Record<string, unknown>).marker === "string"
    ? (command.data as { marker: string }).marker : undefined;
  if (!marker || !state.expected.has(marker)) {
    state.invalid += 1;
    return { accepted: false, error: "invalid canary marker", notRun: true };
  }
  const entry = state.entries.get(marker) ?? { calls: 0, accepted: 0 };
  entry.calls += 1;
  if (entry.calls > 1) state.duplicateCalls += 1;
  entry.accepted += 1;
  state.entries.set(marker, entry);
  return { accepted: true, messageId: `ack:${marker}`, triggered: true, result: { marker } };
};

const terminate = async (child: Child | undefined): Promise<void> => {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  await new Promise<void>(resolve => {
    const timer = setTimeout(() => { child.kill("SIGKILL"); }, 4_000);
    child.once("close", () => { clearTimeout(timer); resolve(); });
  });
};

const lockHolderMain = async (flags: Map<string, string>): Promise<void> => {
  const dbPath = required(flags, "db");
  const holdMs = Number(flags.get("hold-ms") ?? LOCK_HOLD_MS);
  if (!Number.isSafeInteger(holdMs) || holdMs < 2_000 || holdMs > 10_000) throw new Error("invalid lock hold");
  const sqlite = process.getBuiltinModule("node:sqlite") as { DatabaseSync: new (file: string) => {
    exec(sql: string): void; close(): void;
  } };
  const db = new sqlite.DatabaseSync(dbPath);
  const readyAt = Date.now();
  try {
    db.exec("PRAGMA busy_timeout = 1000");
    db.exec("BEGIN IMMEDIATE");
    process.stdout.write(`${JSON.stringify({ phase: "ready", readyAt, holdMs })}\n`);
    await sleep(holdMs);
    db.exec("COMMIT");
    const doneAt = Date.now();
    process.stdout.write(`${JSON.stringify({ phase: "done", readyAt, doneAt, holdMs })}\n`);
  } finally {
    try { db.close(); } catch { /* already closed */ }
  }
};

const runCanary = async (): Promise<void> => {
  process.env.PI_FABRIC_MESH_STATE_BACKEND = "sqlite";
  const evidenceRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bridge-contention-canary-"));
  currentEvidenceRoot = evidenceRoot;
  fs.chmodSync(evidenceRoot, 0o700);
  const hubRoot = path.join(evidenceRoot, HUB);
  const agentRoot = path.join(evidenceRoot, AGENT);
  const cursor = path.join(evidenceRoot, "hub-agent.cursor.json");
  const hubStore = openStore(hubRoot);
  const agentStore = openStore(agentRoot);
  let hubProcess: Child | undefined;
  let hubSender: FabricControlPlane | undefined;
  let agentSender: FabricControlPlane | undefined;
  let bridgeLog = "";
  const lockProcesses: Child[] = [];
  const started = performance.now();
  const hubNative = await installNative(hubStore, HUB_ID, "canary-hub");
  const agentNative = await installNative(agentStore, AGENT_ID, "canary-agent");
  const hubDirectory = new ParticipantDirectory(hubStore, {
    enabled: true, hostId: hubNative.identity.id, rootId: hubNative.identity.id, identity: hubNative.identity,
    heartbeatMs: 60_000, leaseMs: 600_000, reapDeadHosts: false,
  });
  const agentDirectory = new ParticipantDirectory(agentStore, {
    enabled: true, hostId: agentNative.identity.id, rootId: agentNative.identity.id, identity: agentNative.identity,
    heartbeatMs: 60_000, leaseMs: 600_000, reapDeadHosts: false,
  });
  const hubMarkers = Array.from({ length: N }, (_, i) => `${HUB}-to-${AGENT}-${i}`);
  const agentMarkers = Array.from({ length: N }, (_, i) => `${AGENT}-to-${HUB}-${i}`);
  const hubTally = tally(agentMarkers);
  const agentTally = tally(hubMarkers);
  let authenticatedAcks = 0;
  try {
    // Two real bridge processes: the installed run command and its installed agent command over
    // local stdio.  The canary wrapper itself is not a bridge process.
    hubProcess = spawn(process.execPath, [BIN, "run", "--mesh", hubRoot, "--name", HUB, "--remote", AGENT,
      "--cursor", cursor, "--lock-protocol", "3", "--call-timeout-ms", "10000", "--",
      process.execPath, BIN, "agent", "--mesh", agentRoot, "--peer", HUB], {
      stdio: ["ignore", "ignore", "pipe"], env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "sqlite" },
    }) as Child;
    hubProcess.stderr.on("data", chunk => { bridgeLog += String(chunk); });
    await waitForChildText(hubProcess, "linked hub <-> agent", 20_000, "hub bridge");
    await Promise.all([
      observe(hubRoot, () => hubDirectory.mirroredControlOwner(AGENT_ID, AGENT_ID, AGENT_ID)?.remoteHost === AGENT, 20_000, "hub mirror"),
      observe(agentRoot, () => agentDirectory.mirroredControlOwner(HUB_ID, HUB_ID, HUB_ID)?.remoteHost === HUB, 20_000, "agent mirror"),
    ]);

    // One control plane per native owner both sends and receives, as a real Main does.
    hubSender = new FabricControlPlane(hubStore, hubNative.identity, {
      enabled: true, hostId: hubNative.identity.id, ownerIncarnation: hubNative.ownerIncarnation,
      acknowledgementTimeoutMs: 45_000, bridgeTimeoutMs: 45_000,
      readMirroredOwner: (...args) => hubDirectory.mirroredControlOwner(...args),
    });
    agentSender = new FabricControlPlane(agentStore, agentNative.identity, {
      enabled: true, hostId: agentNative.identity.id, ownerIncarnation: agentNative.ownerIncarnation,
      acknowledgementTimeoutMs: 45_000, bridgeTimeoutMs: 45_000,
      readMirroredOwner: (...args) => agentDirectory.mirroredControlOwner(...args),
    });
    hubSender.start(handlerFor(hubTally));
    agentSender.start(handlerFor(agentTally));

    const hubLock = spawnLockHolder(path.join(hubRoot, "state.db"), LOCK_HOLD_MS);
    const agentLock = spawnLockHolder(path.join(agentRoot, "state.db"), LOCK_HOLD_MS);
    lockProcesses.push(hubLock.child, agentLock.child);
    await Promise.all([hubLock.ready, agentLock.ready]);

    const send = async (sender: FabricControlPlane, target: Native, markers: readonly string[], route: string): Promise<void> => {
      await Promise.all(markers.map(async (marker, index) => {
        const receipt = await sender.request(
          target.identity.id, target.identity.id, "followUp",
          { message: marker, data: { marker, index, route }, ownerIncarnation: target.ownerIncarnation },
          target.identity.id, { routedRemoteHost: route, idempotencyKey: `bridge-contention:${marker}`, timeoutMs: 60_000 },
        );
        if (!receipt.acknowledged || receipt.messageId !== `ack:${marker}`) throw new Error(`invalid ACK for ${marker}`);
        authenticatedAcks++;
      }));
    };
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        Promise.all([
          send(hubSender, agentNative, hubMarkers, AGENT),
          send(agentSender, hubNative, agentMarkers, HUB),
        ]),
        new Promise<never>((_, reject) => {
          deadline = setTimeout(() => reject(new Error("control-plane receipts exceeded canary deadline")), DEADLINE_MS);
        }),
      ]);
    } finally { if (deadline) clearTimeout(deadline); }

    const allEntries = [...hubTally.entries.values(), ...agentTally.entries.values()];
    const delivered = allEntries.reduce((sum, entry) => sum + entry.accepted, 0);
    const duplicates = hubTally.duplicateCalls + agentTally.duplicateCalls;
    const drops = 2 * N - delivered;
    const bridgeDrops = bridgeLog.split("\n").filter(line => /mesh-bridge: (toLocal|toRemote): (dropped|refused) /.test(line)).length;
    const lockReceipts = await Promise.all([hubLock.done, agentLock.done]);
    const lockDurations = lockReceipts.map(receipt => Math.max(0, receipt.doneAt - receipt.readyAt));
    const lockHoldsSatisfied = lockDurations.every(duration => duration >= 2_000);
    const result = {
      ok: drops === 0 && bridgeDrops === 0 && duplicates === 0 && hubTally.invalid === 0 && agentTally.invalid === 0 &&
        hubTally.entries.size === N && agentTally.entries.size === N && lockHoldsSatisfied,
      evidenceRoot, sent: 2 * N, delivered, duplicates, drops, bridgeDrops,
      deliveredEachWay: { hubToAgent: agentTally.entries.size, agentToHub: hubTally.entries.size },
      authenticatedAcks,
      receiverInvalid: hubTally.invalid + agentTally.invalid,
      receiverExactlyOnce: hubTally.entries.size === N && agentTally.entries.size === N && duplicates === 0,
      lockDurationsMs: lockDurations, elapsedMs: Math.round(performance.now() - started),
    };
    fs.writeFileSync(path.join(evidenceRoot, "bridge-contention-canary.receipt.json"), `${JSON.stringify(result)}\n`, { mode: 0o600 });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    if (!result.ok) throw new Error(`canary invariant failed: ${JSON.stringify(result)}`);
  } finally {
    for (const child of lockProcesses) await terminate(child);
    await Promise.allSettled([hubSender?.close(), agentSender?.close()].filter((value): value is Promise<void> => value !== undefined));
    fs.writeFileSync(path.join(evidenceRoot, "bridge.log"), bridgeLog, { mode: 0o600 });
    await terminate(hubProcess);
    hubStore.closeState();
    agentStore.closeState();
  }
};

const main = async (): Promise<void> => {
  const mode = process.argv[2];
  if (mode === "lock-holder") return lockHolderMain(parseFlags(process.argv.slice(3)));
  return runCanary();
};

try {
  await main();
} catch (error) {
  const message = error instanceof Error ? error.stack ?? error.message : String(error);
  process.stderr.write(`${JSON.stringify({ ok: false, error: message, evidenceRoot: currentEvidenceRoot })}\n`);
  process.exitCode = 1;
}
