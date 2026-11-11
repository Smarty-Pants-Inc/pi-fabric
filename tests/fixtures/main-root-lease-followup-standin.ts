/** Native, offline Main stand-in for #19998 B. Only run by the owned parent test.
 * No retry/pending implementation lives here: FILE routing, claims, fencing, Main's
 * journal and native SessionManager receipts all come from production APIs.
 */
import fs from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import path from "node:path";
import readline from "node:readline";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { MeshBackgroundRetry, ownProcessIncarnation, readPhysicalHostIdentity } from "../../src/core/atomic-write.js";
import { MainAgentController } from "../../src/main-agent.js";
import { MeshLock } from "../../src/mesh/mesh-lock.js";
import { MeshStore, meshProcessStartedAt, type MeshIdentity } from "../../src/mesh/store.js";
import { AgentMessageRouter } from "../../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../../src/topology/control-plane.js";
import { hostLeasePath, LIVENESS_POLICY_KEY, readHostLeases, writeHostLease } from "../../src/topology/host-leases.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../../src/topology/types.js";

const [mode, scratch, meshRoot, sessionFile, rawProofOptions] = process.argv.slice(2);
if (!scratch || !meshRoot || !["main", "holder", "reader"].includes(mode!)) throw new Error("stand-in: invalid private fixture arguments");
// Additional cases opt in explicitly. The original four cases keep every default.
const proofOptions = JSON.parse(rawProofOptions || "{}") as { lockTimeoutMs?: number; instrumentLeaseProof?: boolean };
if (proofOptions.lockTimeoutMs !== undefined && ![5_000, 35_000].includes(proofOptions.lockTimeoutMs)) {
  throw new Error("stand-in: only the explicit 5s retry variant or original 35s timeout is supported");
}
const incarnation = await ownProcessIncarnation();
if (!incarnation) throw new Error("Native process incarnation unavailable; this is not host proof");
const report = (event: string, fields: Record<string, unknown> = {}) => {
  process.stdout.write(JSON.stringify({ event, pid: process.pid, incarnation, at: Date.now(), ...fields }) + "\n");
};
const sha256 = (file: string) => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const sdk = ["@earendil-works/pi-coding-agent", "@earendil-works/pi-ai"].map(name => {
  const file = fs.realpathSync(fileURLToPath(import.meta.resolve(name)));
  let directory = path.dirname(file);
  for (;;) {
    const metadata = path.join(directory, "package.json");
    if (fs.existsSync(metadata)) {
      const pkg = JSON.parse(fs.readFileSync(metadata, "utf8"));
      if (pkg.name === name) return { name, file, sha256: sha256(file), metadata,
        metadataSha256: sha256(metadata), version: pkg.version as string,
        runtimeFiles: [path.join(path.dirname(file), name.endsWith("pi-coding-agent")
          ? "core/session-manager.js" : "providers/faux.js")].map(file => ({ file, sha256: sha256(file) })) };
    }
    const parent = path.dirname(directory);
    if (parent === directory) throw new Error(`SDK metadata missing: ${name}`);
    directory = parent;
  }
});
const runtimeReceipt = { mode, sourceManifestSha256: process.env.HOST_LEASE_PROOF_SOURCE_MANIFEST,
  compiledFixtureSha256: sha256(fileURLToPath(import.meta.url)), sdk, options: proofOptions,
  heartbeatMs: 1_000, leaseMs: 15_000, holderTimeoutMs: 35_000, deadmanMs: 90_000 };
report("runtime-receipt", runtimeReceipt);
process.once("exit", code => report("native-terminal", { ...runtimeReceipt, code }));
// Observation ONLY: this monitor never handles/suppresses an uncaught failure.
process.on("uncaughtExceptionMonitor", error => report("uncaught-observed", { error: String(error), stack: error.stack }));
const lines = readline.createInterface({ input: process.stdin });
// The parent owns the scope; these deadlines only bound failures within it.
const deadman = setTimeout(() => { report("deadline"); process.exit(72); }, 90_000);

if (mode === "holder") {
  let release!: () => void;
  const released = new Promise<void>(resolve => { release = resolve; });
  lines.on("line", line => { if (line === "release") release(); });
  lines.on("close", release);
  const lock = new MeshLock(meshRoot, { lockProtocol: 2, lockTimeoutMs: 35_000, staleLockMs: 90_000 }, () => {});
  // Dedicated custody process, not an async critical section in a Pi session.
  // This is the same .lock used by FILE state and control event publication.
  await lock.withLockAcrossAwait(async () => {
    report("held", { owner: fs.readFileSync(path.join(meshRoot, ".lock", "owner"), "utf8") });
    await released;
  }, 35_000, "custody");
  report("released");
  clearTimeout(deadman);
  lines.close();
  process.exit(0);
} else if (mode === "reader") {
  if (!sessionFile) throw new Error("reader requires an exact target root id");
  const identity: MeshIdentity = { id: `proof-reader:${process.pid}`, name: "read-only routing observer", kind: "main" };
  // Not an acting Main: never start/register a source/control plane/heartbeat.
  // MeshStore's existing census registration/close is metadata-only; it is the
  // sole reader-side write. No lease, participant, shared-state or event write.
  const mesh = new MeshStore(meshRoot, 65_536, 1_000, {
    stateBackend: "file", lockProtocol: 2, lockTimeoutMs: 35_000, staleLockMs: 90_000, readCacheMs: 0,
  });
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, identity, rootId: identity.id, hostId: identity.id, reapDeadHosts: false, listReadCacheMs: 0,
  });
  let reads = 0;
  const read = async (event: string) => {
    await directory.refreshRoutingView();
    const target = directory.get(sessionFile, Date.now(), { fresh: true });
    const censusDirectory = path.join(meshRoot, ".writer-census");
    const census = fs.existsSync(censusDirectory) ? fs.readdirSync(censusDirectory).sort().map(name => ({
      name, bytes: fs.readFileSync(path.join(censusDirectory, name), "utf8"),
    })) : [];
    report(event, { targetId: sessionFile, target, reads: ++reads, heartbeatArmed: directory.heartbeatArmed,
      routingUnavailable: directory.routingUnavailable(), policy: mesh.get(LIVENESS_POLICY_KEY, { fresh: true }),
      census, leaseWriters: [...readHostLeases(meshRoot).values()].map(lease => ({ id: lease.id, writer: lease.writer })),
      observerId: identity.id, ownLeasePresent: readHostLeases(meshRoot).has(identity.id),
      metadataOnlyWrites: "MeshStore census registration/close; no publication or control consumer" });
  };
  await read("reader-ready");
  lines.on("line", line => {
    void (async () => {
      if (line === "read") await read("reader-view");
      else if (line === "close") {
        await read("reader-final");
        mesh.closeState();
        clearTimeout(deadman);
        lines.close();
        process.exit(0);
      }
    })().catch(error => { report("fatal", { error: String(error) }); process.exit(73); });
  });
  lines.on("close", () => { clearTimeout(deadman); process.exit(0); });
} else {
  const manager = sessionFile
    ? SessionManager.open(sessionFile, path.dirname(sessionFile))
    : SessionManager.create(scratch, path.join(scratch, "sessions"));
  if (!sessionFile) {
    manager.appendSessionInfo("Main"); // Deliberately identical on the unrelated-session counterexample.
    manager.appendMessage({ role: "user", content: "offline host proof", timestamp: Date.now() });
    manager.appendMessage(fauxAssistantMessage("native session seed; no inference"));
  }
  const sessionId = manager.getSessionId();
  const identity: MeshIdentity = { id: `session:${sessionId}`, sessionId, name: "Main", kind: "main" };
  const mesh = new MeshStore(meshRoot, 65_536, 1_000, {
    stateBackend: "file", lockProtocol: 2, lockTimeoutMs: proofOptions.lockTimeoutMs ?? 35_000, staleLockMs: 90_000, readCacheMs: 0,
  });
  let status: FabricParticipantRecord["status"] = "idle";
  // Opt-in private controls observe actual operations; no new renewal/retry/timer.
  const leaseFile = hostLeasePath(meshRoot, identity.id);
  const originalRead = fs.readFileSync.bind(fs);
  const originalRename = fs.renameSync.bind(fs);
  const physical = proofOptions.instrumentLeaseProof ? readPhysicalHostIdentity() : undefined;
  const maybeRead = (file: string): string | undefined => {
    try { return originalRead(file, "utf8"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  };
  const custody = () => {
    const hash = createHash("sha256").update(path.basename(leaseFile)).digest("hex");
    const base = path.join(meshRoot, "host-lease-commits");
    const domain = fs.existsSync(path.join(base, hash)) ? hash : hash.slice(0, 16);
    const sharedOwner = maybeRead(path.join(meshRoot, ".lock", "owner"));
    let sharedIncarnation: string | undefined;
    if (sharedOwner) {
      try {
        const stat = originalRead(`/proc/${Number(sharedOwner.split("\n")[1])}/stat`, "utf8");
        sharedIncarnation = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
      } catch { /* Absence/unknown is retained, never fabricated process proof. */ }
    }
    return { gatePath: path.join(base, domain, "custody.lock", "owner"),
      gateOwner: maybeRead(path.join(base, domain, "custody.lock", "owner")),
      sharedOwner, sharedIncarnation, physical };
  };
  const ownHeldCustody = () => {
    const held = custody();
    const fields = held.gateOwner?.split("\n");
    if (fields?.[1] !== String(process.pid) || fields[3] !== incarnation || !held.sharedOwner ||
      fields[4] !== physical?.machineId || fields[5] !== physical?.bootId) {
      throw new Error("fault command requires actual own physical-host gate and held shared custody");
    }
    return held;
  };
  let gateSequence = 0, activeGate: number | undefined, attemptSequence = 0, pendingAttempt: number | undefined;
  let armedEio = false;
  let publicationBarrier: { reached: boolean; released: Promise<void>; release: () => void } | undefined;
  let injectedError: NodeJS.ErrnoException | undefined;
  if (proofOptions.instrumentLeaseProof) {
    const leaseCustody = mesh.leaseCustody.bind(mesh);
    mesh.leaseCustody = async <T>(file: string, operation: () => T | Promise<T>, timeoutMs = 0,
      options: { ownIncarnation?: string | undefined } = {}): Promise<T> => {
      if (file !== leaseFile) return leaseCustody(file, operation, timeoutMs, options);
      const gate = ++gateSequence;
      try {
        return await leaseCustody(file, async () => {
          activeGate = gate;
          report("host-gate-entered", { gate, custody: custody() });
          try { return await operation(); }
          finally { activeGate = undefined; }
        }, timeoutMs, options);
      } finally { report("host-gate-released", { gate, custody: custody() }); }
    };
    const writeBatch = mesh.writeBatch.bind(mesh);
    mesh.writeBatch = async (...args: Parameters<MeshStore["writeBatch"]>) => {
      if (publicationBarrier && !publicationBarrier.reached && args[0].lockTiming?.operation === "directory-publish") {
        const barrier = publicationBarrier;
        const held = custody();
        const fields = held.gateOwner?.split("\n");
        if (activeGate === undefined || fields?.[1] !== String(process.pid) || fields[3] !== incarnation ||
          fields[4] !== physical?.machineId || fields[5] !== physical?.bootId) {
          throw new Error("publication barrier requires the actual own physical-host gate");
        }
        barrier.reached = true;
        report("publication-barrier-ready", { gate: activeGate, custody: held });
        await barrier.released; // Stdin release only: normal heartbeat remains armed throughout.
        report("publication-barrier-released", { gate: activeGate, custody: custody() });
      }
      const attempt = ++attemptSequence;
      pendingAttempt = attempt;
      report("publication-attempt-start", { attempt, gate: activeGate, custody: custody(), lockTimeoutMs: mesh.lockTimeoutMs });
      try {
        const result = await writeBatch(...args);
        report("publication-attempt-committed", { attempt, gate: activeGate });
        return result;
      } catch (error) {
        report("publication-attempt-error", { attempt, gate: activeGate, error: String(error),
          code: (error as NodeJS.ErrnoException).code, custody: custody() });
        throw error;
      } finally { pendingAttempt = undefined; }
    };
    fs.readFileSync = ((...args: Parameters<typeof fs.readFileSync>) => {
      const result = originalRead(...args);
      if (String(args[0]) === leaseFile) {
        const stack = new Error("lease CAS read observation").stack;
        if (stack?.includes("commitHostLeaseUnderCustody")) report("held-cas-read", {
          gate: activeGate, attempt: pendingAttempt, custody: custody(), stack,
          token: JSON.parse(String(result)).incarnationToken,
        });
      }
      return result;
    }) as typeof fs.readFileSync;
    fs.renameSync = (source, target) => {
      if (String(target) !== leaseFile) return originalRename(source, target);
      const bytes = originalRead(source, "utf8");
      const held = custody();
      if (armedEio && pendingAttempt !== undefined) {
        ownHeldCustody(); // Do not count an unlocked or queued write as fault coverage.
        armedEio = false;
        injectedError = Object.assign(new Error("lease-proof injected held-path EIO"), { code: "EIO" });
        report("held-write-fault-fired", { gate: activeGate, attempt: pendingAttempt, custody: held,
          bytes, stack: injectedError.stack, code: injectedError.code });
        throw injectedError;
      }
      originalRename(source, target);
      report("lease-write", { gate: activeGate, attempt: pendingAttempt, custody: held, lease: JSON.parse(bytes) });
    };
    const failure = MeshBackgroundRetry.prototype.failure;
    MeshBackgroundRetry.prototype.failure = function(error) {
      if ((error as NodeJS.ErrnoException)?.code === "FABRIC_HOST_LEASE_SUPERSEDED" && pendingAttempt !== undefined) {
        report("heartbeat-supersession-observed", { code: (error as NodeJS.ErrnoException).code,
          custody: custody(), attempt: pendingAttempt, stack: new Error("supersession catch receipt").stack });
      }
      if (error === injectedError) report("heartbeat-catch-observed", {
        sameInjectedError: true, code: injectedError?.code, custody: custody(),
        attempt: pendingAttempt, stack: new Error("background failure receipt").stack,
      });
      return failure.call(this, error);
    };
  }
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, identity, rootId: identity.id, hostId: identity.id,
    heartbeatMs: 1_000, leaseMs: 15_000, reapDeadHosts: false,
  });
  directory.registerSource(() => [{
    format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    kind: "root", name: "Main", sessionId, cwd: scratch, status,
    capabilities: ["steer", "followUp", "fabric"], runner: "pi", transport: "host", controlProtocol: "v1",
    startedAt: meshProcessStartedAt, updatedAt: Date.now(),
  }]);
  const handlers = new Map<string, Array<(event: unknown, ctx: ExtensionContext) => unknown>>();
  const context = { cwd: scratch, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: manager } as unknown as ExtensionContext;
  const pi = {
    on(name: string, handler: (event: unknown, ctx: ExtensionContext) => unknown) {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(fn => fn !== handler));
    },
    getThinkingLevel: () => "off",
    sendMessage(message: { customType: string; content: string; details: unknown }, options: unknown) {
      // Native SDK serializer and native persistent session, not a receipt-shaped array.
      manager.appendCustomMessageEntry(message.customType, message.content, true, message.details);
      report("native-append", { sessionId, sessionFile: manager.getSessionFile(), details: message.details, options });
    },
  } as unknown as ExtensionAPI;
  const main = new MainAgentController(pi, identity.id, true, scratch, sessionId);
  main.attachFollowUpDrain(context, 0, path.join(meshRoot, "main-followups", `${encodeURIComponent(sessionId)}.json`));
  await directory.start();
  const plane = new FabricControlPlane(mesh, identity, {
    enabled: true, hostId: identity.id, ownerIncarnation: directory.ownerIncarnation,
    pollMs: 20, canConsumeMesh: () => directory.canConsumeMesh(),
  });
  const router = new AgentMessageRouter(
    { status: () => { throw new Error("Unknown Fabric agent"); } } as never,
    { identity, mesh, validateDirectMessage: () => {}, status: () => { throw new Error("Unknown Fabric actor"); } } as never,
    main, directory, plane, binding => binding,
  );
  let holdAfterNative = false;
  let releaseHandler: (() => void) | undefined;
  plane.start(async (command, from, signal, verification) => {
    report("handler", { commandId: command.commandId, targetId: command.targetId,
      ownerIncarnation: command.ownerIncarnation, message: command.message });
    const acceptance = await router.acceptControl(command, from, signal, verification);
    main.confirmInbox(); // Real session-file fsync + outcome receipt path, never an ACK substitute.
    if (holdAfterNative && command.operation === "followUp") {
      report("handler-held", { commandId: command.commandId, acceptance });
      await new Promise<void>(resolve => { releaseHandler = resolve; });
    }
    return acceptance;
  });
  const snapshot = (event: string) => report(event, {
    id: identity.id, sessionId, sessionFile: manager.getSessionFile(), processStartedAt: meshProcessStartedAt,
    ownerIncarnation: directory.ownerIncarnation, confirmedAt: directory.confirmedAt(),
    lease: readHostLeases(meshRoot).get(identity.id),
    // Metadata only; canConsumeMesh can request work, so it has a separate command.
    ...(proofOptions.instrumentLeaseProof ? { admission: directory.controlAdmission(), heartbeatArmed: directory.heartbeatArmed } : {}),
  });
  snapshot("ready");
  lines.on("line", line => {
    void (async () => {
      if (line === "snapshot") snapshot("snapshot");
      else if (line === "admission") {
        const canConsumeMesh = directory.canConsumeMesh();
        report("admission", { canConsumeMesh, confirmedAt: directory.confirmedAt(), admission: directory.controlAdmission(),
          heartbeatArmed: directory.heartbeatArmed, custody: custody() });
      } else if (line === "arm-publication-barrier") {
        if (!proofOptions.instrumentLeaseProof || publicationBarrier) throw new Error("publication barrier unavailable");
        let release!: () => void;
        const released = new Promise<void>(resolve => { release = resolve; });
        publicationBarrier = { reached: false, released, release };
        report("publication-barrier-armed");
      } else if (line === "release-publication-barrier") {
        if (!proofOptions.instrumentLeaseProof || !publicationBarrier?.reached) throw new Error("publication barrier not reached");
        publicationBarrier.release();
        publicationBarrier = undefined;
      } else if (line === "arm-held-eio") {
        if (!proofOptions.instrumentLeaseProof) throw new Error("fault instrumentation not enabled");
        const held = ownHeldCustody();
        armedEio = true;
        report("held-write-fault-armed", { custody: held, attempt: pendingAttempt });
      } else if (line === "replace-held-token") {
        if (!proofOptions.instrumentLeaseProof || pendingAttempt === undefined) throw new Error("no pending instrumented publication");
        const held = ownHeldCustody();
        const predecessor = readHostLeases(meshRoot).get(identity.id);
        if (!predecessor) throw new Error("cannot replace an absent lease");
        const successor = { ...predecessor, incarnationToken: randomUUID() };
        writeHostLease(meshRoot, successor); // Deliberate private-fixture hostile replacement, never a product path.
        const bytes = originalRead(leaseFile, "utf8");
        report("held-token-replaced", { custody: held, attempt: pendingAttempt, predecessor, successor, bytes });
      } else if (line === "probe-superseded") {
        try { await directory.refresh(); report("superseded-probe-unexpected-success"); }
        catch (error) { report("superseded-probe", { error: String(error), code: (error as NodeJS.ErrnoException).code,
          bytes: originalRead(leaseFile, "utf8"), heartbeatArmed: directory.heartbeatArmed }); }
      } else if (line === "change") {
        status = "running"; // Real changed publication must wait for shared-state custody.
        void directory.refresh().then(() => snapshot("refresh-committed"), error => report("refresh-error", { error: String(error) }));
        report("refresh-started");
      } else if (line === "pause") {
        main.prepareReload();
        plane.pause();
        await directory.quiesce("reload");
        snapshot("paused");
      } else if (line === "hold-after-native") {
        holdAfterNative = true;
        report("armed");
      } else if (line === "release-handler") {
        holdAfterNative = false;
        releaseHandler?.();
      } else if (line === "confirm") {
        main.confirmInbox();
        snapshot("confirmed");
      } else if (line === "close") {
        releaseHandler?.();
        await plane.close();
        main.closeFollowUpDrain();
        await directory.close();
        mesh.closeState();
        clearTimeout(deadman);
        lines.close();
        report("closed");
        process.exit(0);
      }
    })().catch(error => { report("fatal", { error: String(error) }); process.exit(73); });
  });
  lines.on("close", () => { clearTimeout(deadman); process.exit(0); });
}
