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
 * Phase 1 retains the real hub/agent pair. Phase 2 runs three real hub-link CLIs sharing one
 * SQLite hub, each with its real stdio agent, and sends A <-> B through production relay UDS.
 * FabricControlPlane and ParticipantDirectory own delivery, routing and authenticated ACKs.
 * This is contention evidence, not a deterministic reproduction of a stale/inflight race.
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
const childClosures = new WeakMap<Child, Promise<void>>();

/** Every owned process has an absolute lifetime and a joined close, including failed spawns. */
const ownChild = (child: Child): Child => {
  const closed = new Promise<void>(resolve => {
    let force: ReturnType<typeof setTimeout> | undefined;
    const lifetime = setTimeout(() => {
      child.kill("SIGTERM");
      force = setTimeout(() => { child.kill("SIGKILL"); }, 8_000);
    }, 240_000);
    child.once("close", () => { clearTimeout(lifetime); if (force) clearTimeout(force); resolve(); });
    child.on("error", () => undefined);
  });
  childClosures.set(child, closed);
  return child;
};

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
  const child = ownChild(spawn(process.execPath, [SCRIPT, "lock-holder", "--db", db, "--hold-ms", String(holdMs)], {
    stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "sqlite" },
  }) as Child);
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
  if (!child) return;
  const closed = childClosures.get(child);
  if (!closed) throw new Error("cannot terminate an unowned canary child");
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  const timer = setTimeout(() => { child.kill("SIGKILL"); }, 8_000);
  try { await closed; } finally { clearTimeout(timer); }
};

const lockHolderMain = async (flags: Map<string, string>): Promise<void> => {
  const dbPath = required(flags, "db");
  const holdMs = Number(flags.get("hold-ms") ?? LOCK_HOLD_MS);
  if (!Number.isSafeInteger(holdMs) || holdMs < 2_000 || holdMs > 10_000) throw new Error("invalid lock hold");
  const sqlite = process.getBuiltinModule("node:sqlite") as { DatabaseSync: new (file: string) => {
    exec(sql: string): void; close(): void;
  } };
  const db = new sqlite.DatabaseSync(dbPath);
  try {
    db.exec("PRAGMA busy_timeout = 1000");
    db.exec("BEGIN IMMEDIATE");
    const readyAt = Date.now();
    process.stdout.write(`${JSON.stringify({ phase: "ready", readyAt, holdMs })}\n`);
    await sleep(holdMs);
    db.exec("COMMIT");
    const doneAt = Date.now();
    process.stdout.write(`${JSON.stringify({ phase: "done", readyAt, doneAt, holdMs })}\n`);
  } finally {
    try { db.close(); } catch { /* already closed */ }
  }
};

const runPairCanary = async (evidenceRoot: string) => {
  const runtime = path.join(evidenceRoot, "pair-runtime");
  fs.mkdirSync(runtime, { mode: 0o700 });
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
    hubProcess = ownChild(spawn(process.execPath, [BIN, "run", "--mesh", hubRoot, "--name", HUB, "--remote", AGENT,
      "--cursor", cursor, "--lock-protocol", "3", "--call-timeout-ms", "10000", "--",
      process.execPath, BIN, "agent", "--mesh", agentRoot, "--peer", HUB], {
      stdio: ["ignore", "ignore", "pipe"],
      env: { ...process.env, XDG_RUNTIME_DIR: runtime, PI_FABRIC_MESH_STATE_BACKEND: "sqlite" },
    }) as Child);
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
      phase: "pair", evidenceRoot, sent: 2 * N, delivered, duplicates, drops, bridgeDrops,
      deliveredEachWay: { hubToAgent: agentTally.entries.size, agentToHub: hubTally.entries.size },
      authenticatedAcks,
      receiverInvalid: hubTally.invalid + agentTally.invalid,
      receiverExactlyOnce: hubTally.entries.size === N && agentTally.entries.size === N && duplicates === 0,
      lockDurationsMs: lockDurations, elapsedMs: Math.round(performance.now() - started),
    };
    fs.writeFileSync(path.join(evidenceRoot, "pair.receipt.json"), `${JSON.stringify(result)}\n`, { mode: 0o600 });
    if (!result.ok) throw new Error(`pair invariant failed: ${JSON.stringify(result)}`);
    return result;
  } finally {
    for (const child of lockProcesses) await terminate(child);
    await Promise.allSettled([hubSender?.close(), agentSender?.close()].filter((value): value is Promise<void> => value !== undefined));
    await terminate(hubProcess);
    fs.writeFileSync(path.join(evidenceRoot, "bridge.log"), bridgeLog, { mode: 0o600 });
    hubStore.closeState();
    agentStore.closeState();
  }
};

/** Three independent production hub-link CLIs share ONE physical SQLite hub. */
const runCrossSpokeCanary = async (evidenceRoot: string) => {
  if (process.platform !== "linux" || process.release.name !== "node") {
    throw new Error("cross-spoke production relay requires Linux Node; no substituted relay is permitted");
  }
  const base = path.join(evidenceRoot, "cross-spoke");
  fs.mkdirSync(base, { mode: 0o700 });
  const runtime = path.join(base, "runtime");
  fs.mkdirSync(runtime, { mode: 0o700 });
  const names = [HUB, "spoke-a", "spoke-b", "spoke-c"] as const;
  const stores = new Map<string, MeshStore>();
  const natives = new Map<string, Native>();
  const directories = new Map<string, ParticipantDirectory>();
  const links: Array<{ remote: string; child: Child; log: string }> = [];
  const controls: FabricControlPlane[] = [];
  const locks: LockProcess[] = [];
  const started = performance.now();
  const aMarkers = Array.from({ length: N }, (_, i) => `cross-a-to-b-${i}`);
  const bMarkers = Array.from({ length: N }, (_, i) => `cross-b-to-a-${i}`);
  const aTally = tally(bMarkers), bTally = tally(aMarkers);
  let authenticatedAcks = 0;
  let authenticatedCommands = 0;
  const env = { ...process.env, XDG_RUNTIME_DIR: runtime, PI_FABRIC_MESH_STATE_BACKEND: "sqlite" };
  const events = (store: MeshStore, topic: string) => store.read({ topic, after: 0, limit: MESH_READ });
  const object = (value: unknown): value is Record<string, unknown> =>
    typeof value === "object" && value !== null && !Array.isArray(value);
  try {
    for (const name of names) {
      const store = openStore(path.join(base, name));
      stores.set(name, store);
      const native = await installNative(store, `session:canary-cross-${name}-00000000`, `cross-${name}`);
      natives.set(name, native);
      directories.set(name, new ParticipantDirectory(store, {
        enabled: true, hostId: native.identity.id, rootId: native.identity.id, identity: native.identity,
        heartbeatMs: 60_000, leaseMs: 600_000, reapDeadHosts: false,
      }));
    }
    const hub = stores.get(HUB)!;
    for (const remote of names.slice(1)) {
      const child = ownChild(spawn(process.execPath, [BIN, "run", "--mesh", hub.root, "--name", HUB,
        "--remote", remote, "--cursor", path.join(base, `${remote}.cursor.json`),
        "--lock-protocol", "3", "--call-timeout-ms", "10000", "--",
        process.execPath, BIN, "agent", "--mesh", stores.get(remote)!.root, "--peer", HUB,
        "--lock-protocol", "3"], { env, stdio: ["ignore", "ignore", "pipe"] }) as Child);
      const link = { remote, child, log: "" };
      links.push(link);
      child.stderr.on("data", chunk => { link.log += String(chunk); });
      await waitForChildText(child, `linked ${HUB} <-> ${remote}`, 20_000, `${remote} bridge`);
    }
    const reexported = (source: string, target: string): boolean => {
      const native = natives.get(target)!;
      const store = stores.get(source)!;
      const owner = directories.get(source)!.mirroredControlOwner(native.identity.id, native.identity.id, native.identity.id);
      const row = store.get(hostKey(native.identity.id), { fresh: true })?.value;
      return owner?.remoteHost === target && owner.ownerIncarnation === native.ownerIncarnation &&
        object(row) && row.bridgeVia === HUB && row.remoteHost === target;
    };
    await Promise.all([
      observe(stores.get("spoke-a")!.root, () => reexported("spoke-a", "spoke-b"), 20_000, "A observes B via hub"),
      observe(stores.get("spoke-b")!.root, () => reexported("spoke-b", "spoke-a"), 20_000, "B observes A via hub"),
      observe(hub.root, () => names.slice(1).every(name => {
        const native = natives.get(name)!;
        return directories.get(HUB)!.mirroredControlOwner(native.identity.id, native.identity.id, native.identity.id)?.remoteHost === name;
      }), 20_000, "hub holds all three spokes"),
    ]);
    const mirrorsObservedAt = Date.now();
    const relayRegistrations = links.map(({ remote, child }) => {
      const dir = path.join(runtime, "mesh-bridge", "relay");
      const registration = JSON.parse(fs.readFileSync(path.join(dir, `relay-${remote}.json`), "utf8")) as Record<string, unknown>;
      const socket = fs.lstatSync(path.join(dir, `relay-${remote}.sock`));
      if (registration.pid !== child.pid || registration.meshRoot !== fs.realpathSync(hub.root) ||
        registration.remoteName !== remote || registration.localName !== HUB ||
        registration.bridgeScriptPath !== fs.realpathSync(BIN) || !socket.isSocket() ||
        (socket.mode & 0o777) !== 0o600 || socket.uid !== process.getuid!()) {
        throw new Error(`missing production relay registration for ${remote}`);
      }
      return { remote, pid: child.pid };
    });
    const makeControl = (name: string, expectedSender: string, state: Tally): FabricControlPlane => {
      const native = natives.get(name)!;
      const control = new FabricControlPlane(stores.get(name)!, native.identity, {
        enabled: true, hostId: native.identity.id, ownerIncarnation: native.ownerIncarnation,
        acknowledgementTimeoutMs: 45_000, bridgeTimeoutMs: 45_000,
        readMirroredOwner: (...args) => directories.get(name)!.mirroredControlOwner(...args),
      });
      controls.push(control);
      const accept = handlerFor(state);
      control.start((command, from, _signal, verification) => {
        if (verification !== "bridge" || from.id !== natives.get(expectedSender)!.identity.id || command.operation !== "followUp") {
          state.invalid++;
          return { accepted: false, error: "unauthenticated cross-spoke command", notRun: true };
        }
        authenticatedCommands++;
        return accept(command);
      });
      return control;
    };
    const aControl = makeControl("spoke-a", "spoke-b", aTally);
    const bControl = makeControl("spoke-b", "spoke-a", bTally);
    const hubLock = spawnLockHolder(path.join(hub.root, "state.db"), LOCK_HOLD_MS);
    locks.push(hubLock);
    await hubLock.ready;
    const requestsStartedAt = Date.now();
    const sourceQueuedAt: Record<string, number> = {};
    const sourceQueued = ["spoke-a", "spoke-b"].map(name => observe(stores.get(name)!.root, () => {
      const source = natives.get(name)!;
      const count = events(stores.get(name)!, "fabric.control.command").filter(event => event.from.id === source.identity.id).length;
      if (count !== N) return false;
      sourceQueuedAt[name] = Date.now();
      return true;
    }, 15_000, `${name} source batch committed`));
    const send = async (sender: FabricControlPlane, targetName: string, markers: string[]): Promise<void> => {
      const target = natives.get(targetName)!;
      await Promise.all(markers.map(async (marker, index) => {
        const receipt = await sender.request(target.identity.id, target.identity.id, "followUp",
          { message: marker, data: { marker, index }, ownerIncarnation: target.ownerIncarnation }, target.identity.id,
          { routedRemoteHost: targetName, idempotencyKey: `bridge-contention:${marker}`, timeoutMs: 60_000 });
        if (!receipt.acknowledged || receipt.routed !== "mesh" || receipt.messageId !== `ack:${marker}`) {
          throw new Error(`invalid cross-spoke ACK for ${marker}`);
        }
        authenticatedAcks++;
      }));
    };
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        Promise.all([...sourceQueued, send(aControl, "spoke-b", aMarkers), send(bControl, "spoke-a", bMarkers)]),
        new Promise<never>((_, reject) => {
          deadline = setTimeout(() => reject(new Error("cross-spoke receipts exceeded canary deadline")), DEADLINE_MS);
        }),
      ]);
    } finally { if (deadline) clearTimeout(deadline); }
    await observe(hub.root, () => events(hub, "fabric.control.command").length === 2 * N, 15_000, "all hub relay receipts");
    const lockReceipt = await hubLock.done;
    const lockDuration = lockReceipt.doneAt - lockReceipt.readyAt;
    const queuedUnderLock = ["spoke-a", "spoke-b"].every(name =>
      sourceQueuedAt[name]! >= lockReceipt.readyAt && sourceQueuedAt[name]! < lockReceipt.doneAt);
    const received = (name: string, source: string) => events(stores.get(name)!, "fabric.control.command")
      .filter(event => event.to === natives.get(name)!.identity.id && event.from.id === natives.get(source)!.identity.id);
    const aReceived = received("spoke-a", "spoke-b"), bReceived = received("spoke-b", "spoke-a");
    const deliveryStampValid = (event: (typeof aReceived)[number], source: string): boolean =>
      event.verification === "bridge" && object(event.data) && object(event.data.bridge) &&
      event.data.bridge.from === source && event.data.bridge.via === HUB;
    const hubReceipts = events(hub, "fabric.control.command");
    const hubBodyAbsent = hubReceipts.every(event =>
      !Object.hasOwn(event, "data") && !Object.hasOwn(event, "text") && !Object.hasOwn(event, "message"));
    const idleSpokeCommands = events(stores.get("spoke-c")!, "fabric.control.command").length;
    const entries = [...aTally.entries.values(), ...bTally.entries.values()];
    const delivered = entries.reduce((sum, entry) => sum + entry.accepted, 0);
    const duplicates = aTally.duplicateCalls + bTally.duplicateCalls;
    const receiverInvalid = aTally.invalid + bTally.invalid;
    const drops = 2 * N - delivered;
    const bridgeDrops = links.reduce((sum, link) => sum + link.log.split("\n")
      .filter(line => /mesh-bridge: (toLocal|toRemote): (dropped|refused|skipped) /.test(line)).length, 0);
    const linksLive = links.every(({ child }) => child.exitCode === null && child.signalCode === null);
    const receiverExactlyOnce = aTally.entries.size === N && bTally.entries.size === N && duplicates === 0;
    const productionRelayDelivered = aReceived.length === N && bReceived.length === N &&
      aReceived.every(event => deliveryStampValid(event, "spoke-b")) &&
      bReceived.every(event => deliveryStampValid(event, "spoke-a")) && hubBodyAbsent && hubReceipts.length === 2 * N;
    const result = {
      phase: "crossSpoke", evidenceRoot: base,
      ok: drops === 0 && duplicates === 0 && bridgeDrops === 0 && receiverInvalid === 0 && receiverExactlyOnce &&
        authenticatedAcks === 2 * N && authenticatedCommands === 2 * N && productionRelayDelivered && linksLive &&
        idleSpokeCommands === 0 && queuedUnderLock && lockDuration >= 2_000 &&
        requestsStartedAt >= lockReceipt.readyAt && requestsStartedAt < lockReceipt.doneAt,
      sent: 2 * N, delivered, duplicates, drops, bridgeDrops, authenticatedAcks, authenticatedCommands,
      deliveredEachWay: { aToB: bTally.entries.size, bToA: aTally.entries.size },
      receiverInvalid, receiverExactlyOnce, productionRelayDelivered, hubBodyAbsent,
      hubRelayReceipts: hubReceipts.length, idleSpokeCommands, relayRegistrations,
      mirrorsObservedAt, requestsStartedAt, sourceQueuedAt, queuedUnderLock,
      lockStores: [HUB], lockDurationsMs: [lockDuration], lockReceipt,
      elapsedMs: Math.round(performance.now() - started),
    };
    fs.writeFileSync(path.join(base, "cross-spoke.receipt.json"), `${JSON.stringify(result)}\n`, { mode: 0o600 });
    if (!result.ok) throw new Error(`cross-spoke invariant failed: ${JSON.stringify(result)}`);
    return result;
  } finally {
    await Promise.all(locks.map(lock => terminate(lock.child)));
    await Promise.allSettled(controls.map(control => control.close()));
    await Promise.all(links.map(link => terminate(link.child)));
    for (const link of links) fs.writeFileSync(path.join(base, `${link.remote}.bridge.log`), link.log, { mode: 0o600 });
    for (const store of stores.values()) store.closeState();
  }
};

const runCanary = async (): Promise<void> => {
  process.env.PI_FABRIC_MESH_STATE_BACKEND = "sqlite";
  const evidenceRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-bridge-contention-canary-"));
  currentEvidenceRoot = evidenceRoot;
  fs.chmodSync(evidenceRoot, 0o700);
  const phases: Record<string, unknown> = {};
  let phase = "pair";
  try {
    phases.pair = await runPairCanary(evidenceRoot);
    phase = "crossSpoke";
    phases.crossSpoke = await runCrossSpokeCanary(evidenceRoot);
    const result = { ok: true, evidenceRoot, ...phases };
    fs.writeFileSync(path.join(evidenceRoot, "bridge-contention-canary.receipt.json"), `${JSON.stringify(result)}\n`, { mode: 0o600 });
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch (error) {
    const failure = { ok: false, phase, error: error instanceof Error ? error.message : String(error) };
    const phaseReceipt = phase === "pair" ? path.join(evidenceRoot, "pair.receipt.json")
      : path.join(evidenceRoot, "cross-spoke", "cross-spoke.receipt.json");
    if (fs.existsSync(path.dirname(phaseReceipt)) && !fs.existsSync(phaseReceipt)) {
      fs.writeFileSync(phaseReceipt, `${JSON.stringify(failure)}\n`, { mode: 0o600 });
    }
    const result = { ok: false, evidenceRoot, ...phases, failedPhase: phase, error: failure.error };
    fs.writeFileSync(path.join(evidenceRoot, "bridge-contention-canary.receipt.json"), `${JSON.stringify(result)}\n`, { mode: 0o600 });
    throw error;
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
