import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { readPhysicalHostIdentity, writeJsonAtomic } from "../core/atomic-write.js";
import { nativeRecoveryCarrierCustody, nativeRecoveryCarrierIdentities, readNativeRecoveryCustody } from "../core/native-recovery.js";
import { withConfirmedSessionFile } from "../core/session-receipts.js";
import type { NativeRecoveryBinding, NativeRecoveryReceivedBinding, NativeRecoveryCustodyReader, NativeRecoverySessionRef,
  NativeRecoveryTransfer, SessionReceiptManager } from "../core/session-receipts.js";
import { confirmedMainInboxIds } from "./root-inbox-shared.js";
import { processStartTime, residentProcessAlive } from "../residency/process-identity.js";
import { bridgeLeaseAlarms } from "./bridge-alarms.js";
import type { HeldAgentMessage, MainAgentController } from "../main-agent.js";
import type { MeshIdentity, MeshStore } from "../mesh/store.js";
import type { FabricIsolatedSourceFailure, FabricParticipantInfo, FabricParticipantSource } from "./types.js";

export interface StallAlarmOptions {
  rootPresenceAlarmMs: number;
  undeliveredAlarmMs: number;
  rootGoneTtlMs: number;
}
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const absentKey = (rootId: string): string => `topology/root-absence/${hash(rootId)}`;
const mailbox = (meshRoot: string): string => path.join(meshRoot, "main-followups");
const ownerFile = (meshRoot: string, sessionId: string): string => path.join(mailbox(meshRoot), `${encodeURIComponent(sessionId)}.owner.json`);
const routeFile = (meshRoot: string, id: string): string => path.join(mailbox(meshRoot), "routes", `${hash(id)}.json`);
const successorFile = (meshRoot: string, rootId: string): string => path.join(mailbox(meshRoot), "successors", `${hash(rootId)}.json`);
/** Unique identity minted by the ORIGINAL native receiver registration, never a launch label. */
export interface NativeLauncherIdentity {
  rootId: string; activationId: string; pid: number; host: string; processStartedAt?: string | undefined;
  processBootId?: string | undefined;
}
type NativeProcessIdentity = NativeLauncherIdentity & { processStartedAt: string; processBootId: string };
const canonicalUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const sameNativeProcess = (a: NativeProcessIdentity, b: NativeProcessIdentity): boolean =>
  a.pid === b.pid && a.host === b.host && a.processStartedAt === b.processStartedAt && a.processBootId === b.processBootId;
// Current-process registration and authenticated predecessor grants. Never reconstruct from disk.
const registeredMainActivations = new Map<string, { activationId: string; launcher: NativeLauncherIdentity;
  predecessors: NativeLauncherIdentity[] }>();
const sameLauncherIdentity = (a: NativeLauncherIdentity | undefined, b: NativeLauncherIdentity | undefined): boolean =>
  !!a && !!b && a.rootId === b.rootId && a.activationId === b.activationId && a.pid === b.pid &&
  a.host === b.host && a.processStartedAt === b.processStartedAt && a.processBootId === b.processBootId;
const registrationKey = (meshRoot: string, rootId: string): string => `${meshRoot}\u0000${rootId}`;
const read = <T>(file: string): T | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
};
const names = (directory: string): string[] => {
  try { return fs.readdirSync(directory); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
interface InboxOwner {
  rootId: string; sessionId: string; sessionFile?: string | undefined;
  ownerIdentityId: string; pid: number; host?: string; name?: string; processStartedAt?: string | undefined;
  processBootId?: string | undefined; retired?: true; activationId?: string;
  /** Legacy launcher label is display-only. Project and identity are receiver-owned, never mesh grants. */
  nativeLauncher?: string | undefined; nativeProject?: string | undefined;
  nativeLauncherIdentity?: NativeLauncherIdentity | undefined;
  /** Source-global native seal; ordinary lifecycle re-entry may not reopen it. */
  nativeRecovery?: NativeRecoveryBinding;
}
interface NativeRouteCustody {
  version: 1; operationId: string; preparedDigest: string;
  source: NativeRecoverySessionRef; target: NativeRecoverySessionRef; ordinals: number[];
}
interface InboxRoute { oldRoot: string; newRoot?: string; messageId: string; item?: HeldAgentMessage; done?: true;
  nativeRecovery?: NativeRouteCustody }
interface Successor { oldRoot: string; newRoot?: string; activationId?: string;
  nativeRecovery?: NativeRecoveryReceivedBinding & { receiver: NativeProcessIdentity } }

/** Explicit native activation, not a name/cwd inference. Each runtime captures its
 * own token: resuming a root reopens fresh delivery, never the predecessor runtime.
 * Per-message move tombstones live separately and are never reset here. */
export const registerMainInbox = (meshRoot: string, identity: MeshIdentity, sessionId: string, sessionFile?: string,
  nativeLauncher?: string, nativeProject?: string): string => {
  const previousOwner = read<InboxOwner>(ownerFile(meshRoot, sessionId));
  const previousSuccessor = read<Successor>(successorFile(meshRoot, identity.id));
  if ((previousOwner && "nativeRecovery" in previousOwner) || (previousSuccessor && "nativeRecovery" in previousSuccessor)) {
    throw new Error("Cannot reactivate a native recovery source");
  }
  const activationId = randomUUID();
  const launcher: NativeLauncherIdentity = { rootId: identity.id, activationId, pid: process.pid,
    host: os.hostname(), processStartedAt: processStartTime(process.pid), processBootId: readPhysicalHostIdentity()?.bootId };
  writeJsonAtomic(ownerFile(meshRoot, sessionId), { rootId: identity.id, sessionId, sessionFile, activationId, nativeLauncher, nativeProject,
    nativeLauncherIdentity: launcher, ownerIdentityId: identity.id, pid: launcher.pid, host: launcher.host,
    name: identity.name, processStartedAt: launcher.processStartedAt, processBootId: launcher.processBootId } satisfies InboxOwner, { durable: true });
  writeJsonAtomic(successorFile(meshRoot, identity.id), { oldRoot: identity.id, activationId } satisfies Successor, { durable: true });
  registeredMainActivations.set(registrationKey(meshRoot, identity.id), { activationId, launcher, predecessors: [] });
  return activationId;
};
export const registeredMainActivation = (meshRoot: string, rootId: string): string | undefined =>
  registeredMainActivations.get(registrationKey(meshRoot, rootId))?.activationId;
export const registeredMainLauncherIdentity = (meshRoot: string, rootId: string): NativeLauncherIdentity | undefined => {
  const launcher = registeredMainActivations.get(registrationKey(meshRoot, rootId))?.launcher;
  return launcher && { ...launcher };
};
export const registeredMainPredecessors = (meshRoot: string, rootId: string): NativeLauncherIdentity[] =>
  registeredMainActivations.get(registrationKey(meshRoot, rootId))?.predecessors.map(identity => ({ ...identity })) ?? [];
/** Only a live process-local registration can receive or originate predecessor grants. */
const authenticatedLaunchers = (meshRoot: string, owner: InboxOwner): NativeLauncherIdentity[] | undefined => {
  const registered = registeredMainActivations.get(registrationKey(meshRoot, owner.rootId));
  if (!registered || registered.activationId !== owner.activationId || owner.ownerIdentityId !== owner.rootId ||
      owner.sessionId !== owner.rootId.slice(8) || owner.pid !== process.pid || owner.host !== os.hostname() ||
      (process.platform === "linux" && (!owner.processStartedAt || owner.processStartedAt !== processStartTime(process.pid) ||
        !owner.processBootId || owner.processBootId !== readPhysicalHostIdentity()?.bootId)) ||
      !sameLauncherIdentity(owner.nativeLauncherIdentity, registered.launcher)) return undefined;
  return [registered.launcher, ...registered.predecessors].map(identity => ({ ...identity }));
};
const inheritLaunchers = (meshRoot: string, newRoot: string, predecessors: NativeLauncherIdentity[] | undefined): void => {
  if (!predecessors || !newRoot.startsWith("session:")) return;
  const owner = read<InboxOwner>(ownerFile(meshRoot, newRoot.slice(8)));
  if (!owner || owner.rootId !== newRoot || owner.retired || !authenticatedLaunchers(meshRoot, owner)) return;
  const registered = registeredMainActivations.get(registrationKey(meshRoot, newRoot))!;
  registered.predecessors = [...registered.predecessors, ...predecessors]
    .filter((identity, index, all) => all.findIndex(other => sameLauncherIdentity(identity, other)) === index)
    .map(identity => ({ ...identity }));
};
/** smarty-dev#8881 F-01: optional activation liveness, checked inside the custody critical section
 * before any write. Custody acquisition awaits; a session disposed in that wait must commit nothing. */
const fenceActivation = (live?: () => boolean): void => {
  if (live && !live()) throw new Error("Pi Fabric mesh activation lifecycle retired");
};
/** Called only after the old Main/control drainer has closed on an actual native session change. */
export const recordMainSuccessor = async (mesh: MeshStore, oldRoot: string, oldSessionId: string, newRoot: string,
  live?: () => boolean): Promise<void> => {
  if (oldRoot === newRoot) return;
  await mesh.custody(() => {
    fenceActivation(live);
    const owner = read<InboxOwner>(ownerFile(mesh.root, oldSessionId));
    if (!owner || owner.rootId !== oldRoot || owner.pid !== process.pid) throw new Error("Cannot rotate an unowned Main inbox");
    const prior = read<Successor>(successorFile(mesh.root, oldRoot));
    if (owner.nativeRecovery && nativeRoot(owner.nativeRecovery.target) !== newRoot) throw new Error("Conflicting native Main successor");
    if (prior?.newRoot && prior.newRoot !== newRoot) throw new Error("Main inbox already has a different successor");
    const launchers = authenticatedLaunchers(mesh.root, owner);
    if (prior?.nativeRecovery && prior.nativeRecovery.target.sessionId !== newRoot.slice(8)) throw new Error("Conflicting native Main successor");
    writeJsonAtomic(successorFile(mesh.root, oldRoot), { ...prior, oldRoot, newRoot, activationId: owner.activationId }, { durable: true });
    writeJsonAtomic(ownerFile(mesh.root, oldSessionId), { ...owner, retired: true }, { durable: true });
    inheritLaunchers(mesh.root, newRoot, launchers);
  });
};

/** Pi's native replacement shuts down the old extension runtime before starting the
 * new one. Bind its explicit targetSessionFile, never infer succession from a label. */
export const stageMainSuccessor = async (mesh: MeshStore, oldRoot: string, sessionId: string, targetSessionFile: string): Promise<void> => {
  await mesh.custody(() => {
    const file = ownerFile(mesh.root, sessionId);
    const owner = read<InboxOwner>(file);
    if (!owner || owner.rootId !== oldRoot || owner.pid !== process.pid) throw new Error("Cannot retire an unowned Main inbox");
    if (owner.nativeRecovery && owner.nativeRecovery.target.sessionFile !== path.resolve(targetSessionFile)) throw new Error("Conflicting native Main rotation target");
    const prior = read<Successor>(successorFile(mesh.root, oldRoot));
    if (prior?.nativeRecovery && prior.nativeRecovery.target.sessionFile !== path.resolve(targetSessionFile)) throw new Error("Conflicting native Main rotation target");
    // Resuming the current native session keeps its root; it is not a rotation.
    if (owner.sessionFile && path.resolve(owner.sessionFile) === path.resolve(targetSessionFile)) return;
    writeJsonAtomic(path.join(mailbox(mesh.root), "rotations", `${hash(path.resolve(targetSessionFile))}.json`),
      { oldRoot, sessionId, activationId: owner.activationId, targetSessionFile: path.resolve(targetSessionFile),
        nativeLauncherIdentity: owner.nativeLauncherIdentity, nativeLauncherGrants: authenticatedLaunchers(mesh.root, owner) }, { durable: true });
    writeJsonAtomic(file, { ...owner, retired: true }, { durable: true });
  });
};
export const confirmMainSuccessor = async (mesh: MeshStore, newRoot: string, sessionFile?: string,
  live?: () => boolean): Promise<boolean> => {
  if (!sessionFile) return false;
  return mesh.custody(() => {
    fenceActivation(live);
    const intentFile = path.join(mailbox(mesh.root), "rotations", `${hash(path.resolve(sessionFile))}.json`);
    const intent = read<{ oldRoot: string; sessionId: string; activationId?: string; targetSessionFile: string;
      nativeLauncherIdentity?: NativeLauncherIdentity; nativeLauncherGrants?: NativeLauncherIdentity[] }>(intentFile);
    if (!intent) {
      // Native reconciliation is already committed independently of this one-use
      // ordinary intent. Re-observing it must not erase metadata or demand a new switch.
      for (const name of names(path.join(mailbox(mesh.root), "successors")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))) {
        const prior = read<Successor>(path.join(mailbox(mesh.root), "successors", name));
        if (prior?.newRoot !== newRoot || prior.nativeRecovery?.target.sessionFile !== path.resolve(sessionFile)) continue;
        if (!sameLauncherIdentity(prior.nativeRecovery.receiver, nativeReceiver(mesh.root, prior.nativeRecovery.target, true))) {
          throw new Error("Native Main receiver activation changed");
        }
        return true;
      }
      return false;
    }
    const owner = read<InboxOwner>(ownerFile(mesh.root, intent.sessionId));
    // An interrupted/canceled switch cannot authorize a different activation.
    if (!owner?.retired || owner.activationId !== intent.activationId) return false;
    if (intent.targetSessionFile !== path.resolve(sessionFile) || owner.rootId !== intent.oldRoot || newRoot === intent.oldRoot) throw new Error("Invalid Main rotation intent");
    const file = successorFile(mesh.root, intent.oldRoot);
    const prior = read<Successor>(file);
    if (prior?.newRoot && prior.newRoot !== newRoot) throw new Error("Main inbox already has a different successor");
    // The staged native intent authenticates the old registration across runtime/process replacement.
    // A legacy intent/owner without identity still routes messages, but grants no retirement authority.
    if (prior?.nativeRecovery && (prior.nativeRecovery.target.sessionId !== newRoot.slice(8) ||
        prior.nativeRecovery.target.sessionFile !== path.resolve(sessionFile))) throw new Error("Conflicting native Main successor");
    writeJsonAtomic(file, { ...prior, oldRoot: intent.oldRoot, newRoot, activationId: owner.activationId }, { durable: true });
    if (sameLauncherIdentity(intent.nativeLauncherIdentity, owner.nativeLauncherIdentity)) {
      inheritLaunchers(mesh.root, newRoot, intent.nativeLauncherGrants);
    }
    // An intent authorizes this switch only, not a later return to the same file.
    fs.unlinkSync(intentFile);
    return true;
  });
};

const nativeRoot = (scope: NativeRecoverySessionRef): string => `session:${scope.sessionId}`;
const sameNativeSession = (a: NativeRecoverySessionRef, b: NativeRecoverySessionRef): boolean =>
  a.sessionId === b.sessionId && a.sessionFile === b.sessionFile;
const sameNativeOperation = (a: NativeRouteCustody | NativeRecoveryBinding, b: NativeRouteCustody | NativeRecoveryBinding): boolean =>
  a.version === b.version && a.operationId === b.operationId && a.preparedDigest === b.preparedDigest &&
  sameNativeSession(a.source, b.source) && sameNativeSession(a.target, b.target);
const nativeBinding = (transfer: NativeRecoveryTransfer): NativeRecoveryBinding => {
  const common = { version: 1 as const, operationId: transfer.operationId, preparedDigest: transfer.preparedDigest,
    preparedEntryId: transfer.preparedEntryId, source: { ...transfer.source }, target: { ...transfer.target } };
  return transfer.phase === "received" ? { ...common, phase: "received", receivedEntryId: transfer.receivedEntryId }
    : { ...common, phase: "prepared" };
};
const receivedNativeBinding = (transfer: NativeRecoveryTransfer): NativeRecoveryReceivedBinding => {
  const binding = nativeBinding(transfer);
  if (binding.phase !== "received") throw new Error("Native Main successor is not RECEIVED");
  return binding;
};
const nativeTransfers = (scope: NativeRecoverySessionRef, reader: NativeRecoveryCustodyReader): readonly NativeRecoveryTransfer[] => {
  const result = readNativeRecoveryCustody(scope, reader);
  if (result.status === "hold") throw new Error(`Native Main custody HOLD: ${result.reason}`);
  return result.status === "validated" ? result.transfers : [];
};
const nativeOwner = (meshRoot: string, scope: NativeRecoverySessionRef): InboxOwner | undefined => {
  if (!canonicalUuid.test(scope.sessionId) || !path.isAbsolute(scope.sessionFile) ||
      fs.realpathSync(scope.sessionFile) !== scope.sessionFile) throw new Error("Invalid canonical native Main scope");
  const owner = read<InboxOwner>(ownerFile(meshRoot, scope.sessionId));
  if (owner && (owner.rootId !== nativeRoot(scope) || owner.ownerIdentityId !== owner.rootId ||
      owner.sessionId !== scope.sessionId || owner.sessionFile !== scope.sessionFile)) throw new Error("Native Main owner scope mismatch");
  return owner;
};
/** A disk label is not a current-process native registration. Even sealed ancestors
 * need positive same-host full birth evidence; unknown-is-live is not positive proof. */
const nativeReceiver = (meshRoot: string, scope: NativeRecoverySessionRef, requireActive: boolean): NativeProcessIdentity => {
  const owner = nativeOwner(meshRoot, scope);
  const receiver = owner?.nativeLauncherIdentity;
  if (!owner || !receiver || typeof owner.activationId !== "string" || !owner.activationId ||
      receiver.rootId !== owner.rootId || receiver.activationId !== owner.activationId || !Number.isSafeInteger(receiver.pid) || receiver.pid <= 0 ||
      receiver.pid !== owner.pid || typeof receiver.host !== "string" || !receiver.host || receiver.host !== owner.host ||
      typeof receiver.processStartedAt !== "string" || !/^\d+$/.test(receiver.processStartedAt) ||
      receiver.processStartedAt !== owner.processStartedAt || typeof receiver.processBootId !== "string" ||
      !canonicalUuid.test(receiver.processBootId) || receiver.processBootId !== owner.processBootId) throw new Error("Missing exact native Main registration");
  if (process.platform !== "linux" || receiver.host !== os.hostname() ||
      readPhysicalHostIdentity()?.bootId !== receiver.processBootId || processStartTime(receiver.pid) !== receiver.processStartedAt) {
    throw new Error("No positive native Main receiver full birth");
  }
  if (receiver.pid === process.pid) {
    if (!authenticatedLaunchers(meshRoot, owner)) throw new Error("Unregistered local native Main receiver");
  } else if (!residentProcessAlive(receiver.pid, receiver.processStartedAt)) throw new Error("No positive native Main receiver liveness");
  if (requireActive) {
    if (owner.retired || owner.nativeRecovery) throw new Error("Native Main receiver is sealed");
    const activation = read<Successor>(successorFile(meshRoot, owner.rootId));
    if (!activation || activation.oldRoot !== owner.rootId || activation.activationId !== owner.activationId ||
        activation.newRoot || activation.nativeRecovery) throw new Error("Missing active native Main registration");
  }
  return { ...receiver, processStartedAt: receiver.processStartedAt, processBootId: receiver.processBootId };
};
const journalFile = (meshRoot: string, sessionId: string): string => path.join(mailbox(meshRoot), `${encodeURIComponent(sessionId)}.json`);
/** Idempotent passes still owe barriers, not rename/watcher churn for identical rows. */
const writeNativeRecord = (file: string, previous: unknown, next: unknown): void => {
  if (JSON.stringify(previous) === JSON.stringify(next) && withConfirmedSessionFile(file, () => {})) return;
  writeJsonAtomic(file, next, { durable: true });
};

/** This is bookkeeping in existing routes/owners/successors, not another transfer store. */
const reconcileNativeCustody = (meshRoot: string, scope: NativeRecoverySessionRef, reader: NativeRecoveryCustodyReader): void => {
  const transfers = nativeTransfers(scope, reader);
  for (const transfer of transfers) {
    const owner = nativeOwner(meshRoot, transfer.source);
    const priorSuccessor = read<Successor>(successorFile(meshRoot, nativeRoot(transfer.source)));
    if (priorSuccessor && (priorSuccessor.oldRoot !== nativeRoot(transfer.source) ||
        (owner && priorSuccessor.activationId !== owner.activationId))) throw new Error("Native Main source activation mismatch");
    if (owner?.nativeRecovery && !sameNativeOperation(owner.nativeRecovery, transfer)) throw new Error("Conflicting native Main source seal");
    if (priorSuccessor?.newRoot && priorSuccessor.newRoot !== nativeRoot(transfer.target)) throw new Error("Conflicting native Main successor");
    if (owner?.nativeRecovery?.phase === "received" && transfer.phase !== "received") throw new Error("Native Main custody phase regressed");
    if (priorSuccessor?.nativeRecovery && (transfer.phase !== "received" || !sameNativeOperation(priorSuccessor.nativeRecovery, transfer) ||
        priorSuccessor.nativeRecovery.preparedEntryId !== transfer.preparedEntryId ||
        (transfer.phase === "received" && priorSuccessor.nativeRecovery.receivedEntryId !== transfer.receivedEntryId))) throw new Error("Conflicting native Main successor binding");
    if (owner) writeNativeRecord(ownerFile(meshRoot, transfer.source.sessionId), owner,
      { ...owner, retired: true, nativeRecovery: nativeBinding(transfer) });
    const journal = read<{ items: HeldAgentMessage[] }>(journalFile(meshRoot, transfer.source.sessionId));
    const claims = new Map<string, number[]>();
    for (const occurrence of transfer.occurrences) {
      if (!occurrence.customMessage || occurrence.customMessage.customType !== "pi-fabric-agent-message") continue;
      for (const id of nativeRecoveryCarrierIdentities(occurrence.customMessage).carrierIds) {
        const ordinals = claims.get(id) ?? [];
        if (!ordinals.includes(occurrence.ordinal)) ordinals.push(occurrence.ordinal);
        claims.set(id, ordinals);
      }
    }
    for (const [id, ordinals] of claims) {
      const membership = nativeRecoveryCarrierCustody({ status: "validated", transfers }, { carrierId: id });
      if (membership.status === "hold" && !membership.transfer) throw new Error(membership.reason ?? "Conflicting native Main carrier");
      const file = routeFile(meshRoot, id);
      const prior = read<InboxRoute>(file);
      if (prior && prior.messageId !== id) throw new Error("Native Main route identity mismatch");
      const custody: NativeRouteCustody = { version: 1, operationId: transfer.operationId, preparedDigest: transfer.preparedDigest,
        source: { ...transfer.source }, target: { ...transfer.target }, ordinals };
      if (prior?.nativeRecovery && !sameNativeOperation(prior.nativeRecovery, custody)) {
        // Repeated reconciliation starts at the oldest edge. An already advanced
        // claim stays advanced; only a validated contiguous native edge may move it.
        const chain = transfers.some(edge => sameNativeOperation(edge, prior.nativeRecovery!))
          ? transfers : nativeTransfers(prior.nativeRecovery.source, reader);
        const previous = chain.find(edge => sameNativeOperation(edge, prior.nativeRecovery!));
        const incoming = chain.find(edge => sameNativeOperation(edge, transfer));
        if (!previous || !incoming) throw new Error("Conflicting native Main message claim");
        if (chain.indexOf(previous) > chain.indexOf(incoming)) continue;
        if (previous.phase !== "received" || !sameNativeSession(previous.target, transfer.source)) throw new Error("Conflicting native Main message target");
      } else if (prior && !prior.nativeRecovery && prior.newRoot !== nativeRoot(transfer.source) &&
          prior.newRoot !== nativeRoot(transfer.target)) throw new Error("Main message already claimed elsewhere");
      if (prior?.nativeRecovery && prior.newRoot !== nativeRoot(prior.nativeRecovery.target)) throw new Error("Native Main route target mismatch");
      const item = journal?.items.find(item => item.id === id) ?? prior?.item;
      if (item && item.id !== id) throw new Error("Native Main message identity mismatch");
      writeNativeRecord(file, prior, { ...prior, oldRoot: prior?.oldRoot ?? nativeRoot(transfer.source), newRoot: nativeRoot(transfer.target),
        messageId: id, ...(item ? { item } : {}), nativeRecovery: custody } satisfies InboxRoute);
    }
  }
  // Reserve every member before requiring a receiver. A failed bootstrap cannot
  // leave later children unclaimed for a competing ordinary maintenance pass.
  for (const transfer of transfers) {
    if (transfer.phase !== "received") continue;
    const oldRoot = nativeRoot(transfer.source), newRoot = nativeRoot(transfer.target);
    const owner = nativeOwner(meshRoot, transfer.source), targetOwner = nativeOwner(meshRoot, transfer.target);
    const source = nativeReceiver(meshRoot, transfer.source, false), target = nativeReceiver(meshRoot, transfer.target, false);
    if (!sameNativeProcess(source, target)) throw new Error("Native Main receiver must match original live source PID/full birth/host");
    const file = successorFile(meshRoot, oldRoot), prior = read<Successor>(file);
    const targetTransfers = nativeTransfers(transfer.target, reader);
    if (!targetTransfers.some(edge => sameNativeOperation(edge, transfer) && edge.phase === "received" &&
        edge.preparedEntryId === transfer.preparedEntryId && edge.receivedEntryId === transfer.receivedEntryId)) throw new Error("Native Main target pair unavailable");
    const receiver = nativeReceiver(meshRoot, transfer.target, !targetTransfers.some(edge => sameNativeSession(edge.source, transfer.target)));
    if (!sameLauncherIdentity(source, nativeReceiver(meshRoot, transfer.source, false)) || !sameLauncherIdentity(target, receiver) ||
        JSON.stringify(owner) !== JSON.stringify(nativeOwner(meshRoot, transfer.source)) ||
        JSON.stringify(targetOwner) !== JSON.stringify(nativeOwner(meshRoot, transfer.target)) ||
        JSON.stringify(prior) !== JSON.stringify(read<Successor>(file))) throw new Error("Native Main registration changed during read");
    // Outer activation belongs to S; receiver.activationId is the separately registered T.
    const activationId = owner?.activationId ?? prior?.activationId;
    if (!activationId) throw new Error("Missing native Main source activation");
    const recovery = { ...receivedNativeBinding(transfer), receiver };
    if (prior?.nativeRecovery && !sameLauncherIdentity(prior.nativeRecovery.receiver, receiver)) {
      const previous = prior.nativeRecovery.receiver;
      if (previous.rootId !== newRoot || typeof previous.activationId !== "string" || !previous.activationId ||
          !sameNativeProcess(previous, receiver)) throw new Error("Conflicting native Main receiver receipt PID/full birth/host");
      // Only the SAME live native process may refresh T's activation. A restarted
      // process cannot rewrite the original launch birth, even after its death.
    }
    writeNativeRecord(file, prior, { ...prior, oldRoot, newRoot, activationId, nativeRecovery: recovery } satisfies Successor);
  }
};

const resolveNativeSuccessor = (meshRoot: string, oldRoot: string, reader: NativeRecoveryCustodyReader):
  { rootId: string; bindings: readonly NativeRecoveryReceivedBinding[] } | undefined => {
  if (!oldRoot.startsWith("session:")) return undefined;
  const visited = new Set<string>(), bindings: NativeRecoveryReceivedBinding[] = [];
  const snapshots = new Map<string, string | undefined>();
  const registrations = new Map<string, { scope: NativeRecoverySessionRef; launcher: NativeProcessIdentity }>();
  const stableRead = <T>(file: string): T | undefined => {
    const value = read<T>(file), snapshot = JSON.stringify(value);
    if (snapshots.has(file) && snapshots.get(file) !== snapshot) throw new Error("Native Main records changed during resolution");
    snapshots.set(file, snapshot);
    return value;
  };
  const registration = (scope: NativeRecoverySessionRef, active = false): NativeProcessIdentity => {
    stableRead(ownerFile(meshRoot, scope.sessionId));
    const launcher = nativeReceiver(meshRoot, scope, active), previous = registrations.get(scope.sessionId);
    if (previous && (!sameNativeSession(previous.scope, scope) || !sameLauncherIdentity(previous.launcher, launcher))) {
      throw new Error("Native Main registration changed during resolution");
    }
    registrations.set(scope.sessionId, { scope, launcher });
    return launcher;
  };
  let rootId = oldRoot, anchor: NativeProcessIdentity | undefined, lastReceiver: NativeProcessIdentity | undefined;
  for (;;) {
    if (visited.has(rootId)) throw new Error("Cyclic native Main successor chain");
    visited.add(rootId);
    const row = stableRead<Successor>(successorFile(meshRoot, rootId));
    if (row !== undefined && (!row || typeof row !== "object" || Array.isArray(row) || row.oldRoot !== rootId ||
        (row.newRoot !== undefined && (typeof row.newRoot !== "string" || !row.newRoot)) ||
        (row.activationId !== undefined && (typeof row.activationId !== "string" || !row.activationId)))) {
      throw new Error("Invalid native Main successor locator");
    }
    if (!row || !("nativeRecovery" in row)) {
      const binding = bindings[bindings.length - 1];
      const owner = stableRead<InboxOwner>(ownerFile(meshRoot, rootId.slice(8)));
      if (owner !== undefined && (!owner || typeof owner !== "object" || Array.isArray(owner) ||
          owner.rootId !== rootId || owner.sessionId !== rootId.slice(8))) throw new Error("Invalid native Main owner locator");
      const scope = binding?.target ?? (owner?.sessionFile ? { sessionId: owner.sessionId, sessionFile: owner.sessionFile } : undefined);
      // Read the canonical terminal session BEFORE trusting absence of its locator.
      // T may already have PREPARED T->U while T's successor row is missing.
      const targetTransfers = scope ? nativeTransfers(scope, reader) : [];
      if (!binding) {
        if ((owner && "nativeRecovery" in owner) || targetTransfers.some(edge => edge.phase !== "received" ||
            (scope && sameNativeSession(edge.source, scope)))) throw new Error("Native Main recovery has no committed successor locator");
        if (targetTransfers.length) {
          // Canonical incoming-only RECEIVED proves this scope has NO outgoing
          // recovery. It is current T, not an old address needing a successor.
          // Still require the same live native birth across every predecessor;
          // normal current-T ownership/admission gates remain the caller's job.
          if (!scope || !sameNativeSession(targetTransfers.at(-1)!.target, scope)) throw new Error("Native Main incoming scope mismatch");
          let incomingAnchor: NativeProcessIdentity | undefined;
          for (const edge of targetTransfers) {
            const source = registration(edge.source), target = registration(edge.target);
            incomingAnchor ??= source;
            if (!sameNativeProcess(incomingAnchor, source) || !sameNativeProcess(source, target)) {
              throw new Error("Native Main receiver must match original live source PID/full birth/host");
            }
          }
          registration(scope, true);
          for (const { scope, launcher } of registrations.values()) {
            if (!sameLauncherIdentity(launcher, nativeReceiver(meshRoot, scope, false))) throw new Error("Native Main full birth changed during resolution");
          }
        }
        for (const file of snapshots.keys()) stableRead(file);
        return undefined; // Positive no-outgoing evidence (or ordinary absence), never uncertainty.
      }
      if (row?.newRoot || !targetTransfers.some(edge => sameNativeOperation(edge, binding) && edge.phase === "received" &&
          edge.preparedEntryId === binding.preparedEntryId && edge.receivedEntryId === binding.receivedEntryId) ||
          targetTransfers.some(edge => sameNativeSession(edge.source, binding.target))) throw new Error("Native Main terminal is sealed or its pair/locator is missing");
      if (!sameLauncherIdentity(lastReceiver, registration(binding.target, true))) throw new Error("Native Main receiver receipt changed");
      for (const { scope, launcher } of registrations.values()) {
        if (!sameLauncherIdentity(launcher, nativeReceiver(meshRoot, scope, false))) throw new Error("Native Main full birth changed during resolution");
      }
      for (const file of snapshots.keys()) stableRead(file);
      return { rootId, bindings };
    }
    const recorded = row.nativeRecovery;
    if (!recorded || typeof recorded !== "object" || recorded.version !== 1 || recorded.phase !== "received" ||
        typeof recorded.operationId !== "string" || !recorded.operationId || !/^[a-f0-9]{64}$/.test(recorded.preparedDigest) ||
        typeof recorded.preparedEntryId !== "string" || !recorded.preparedEntryId ||
        typeof recorded.receivedEntryId !== "string" || !recorded.receivedEntryId || !recorded.source || !recorded.target ||
        nativeRoot(recorded.source) !== rootId || row.newRoot !== nativeRoot(recorded.target) || !row.activationId) {
      throw new Error("Invalid native Main received successor locator");
    }
    const source = registration(recorded.source), receiver = registration(recorded.target);
    anchor ??= source;
    if (!sameNativeProcess(anchor, source) || !sameNativeProcess(source, receiver)) throw new Error("Native Main receiver must match original live source PID/full birth/host");
    const sourceOwner = nativeOwner(meshRoot, recorded.source);
    if (!sourceOwner || sourceOwner.activationId !== row.activationId || !sourceOwner.nativeRecovery ||
        !sameNativeOperation(sourceOwner.nativeRecovery, recorded) || sourceOwner.nativeRecovery.phase !== "received" ||
        sourceOwner.nativeRecovery.preparedEntryId !== recorded.preparedEntryId ||
        sourceOwner.nativeRecovery.receivedEntryId !== recorded.receivedEntryId) throw new Error("Native Main source activation/seal mismatch");
    const transfer = nativeTransfers(recorded.source, reader).find(edge => sameNativeOperation(edge, recorded));
    if (!transfer || transfer.phase !== "received" || transfer.preparedEntryId !== recorded.preparedEntryId ||
        transfer.receivedEntryId !== recorded.receivedEntryId) throw new Error("Native Main received pair unavailable");
    if (!sameLauncherIdentity(receiver, recorded.receiver)) throw new Error("Native Main receiver receipt mismatch");
    registration(recorded.source); registration(recorded.target);
    lastReceiver = receiver;
    bindings.push(receivedNativeBinding(transfer));
    rootId = row.newRoot!;
  }
};

/** Revalidate native ownership inside the same lock as every permanent route claim. */
export const reconcileNativeMainInboxCustody = async (mesh: MeshStore, scope: NativeRecoverySessionRef,
  readNativeCustody: NativeRecoveryCustodyReader, live?: () => boolean): Promise<void> => {
  await mesh.custody(() => {
    fenceActivation(live);
    reconcileNativeCustody(mesh.root, scope, readNativeCustody);
  });
};

/** Ordinary rotation rows are never authority to use an old native address. */
export const resolveNativeMainSuccessor = (meshRoot: string, oldRoot: string,
  readNativeCustody: NativeRecoveryCustodyReader): { rootId: string; bindings: readonly NativeRecoveryReceivedBinding[] } | undefined => {
  return resolveNativeSuccessor(meshRoot, oldRoot, readNativeCustody); // Unknown/torn facts throw; undefined proves no outgoing recovery.
};

export const mainInboxActive = (meshRoot: string, rootId: string, activationId?: string): boolean => {
  const current = read<Successor>(successorFile(meshRoot, rootId));
  const owner = rootId.startsWith("session:") ? read<InboxOwner>(ownerFile(meshRoot, rootId.slice(8))) : undefined;
  return !owner?.nativeRecovery && !current?.newRoot && (activationId === undefined || current?.activationId === activationId);
};

/** Permanent per-message claim: a resumed predecessor must never replay a moved message. */
export const mainInboxOwns = (meshRoot: string, rootId: string, id: string): boolean => {
  const route = read<InboxRoute>(routeFile(meshRoot, id));
  return !route || route.newRoot === rootId;
};

const absence = async (mesh: MeshStore, identity: MeshIdentity, rootId: string, live: boolean, now: number): Promise<number | undefined> => {
  const key = absentKey(rootId);
  const current = mesh.get(key, { fresh: true });
  if (live) {
    if (current) await mesh.writeBatch({ identity, lockTiming: { operation: "root-absence-clear" },
      ops: [{ kind: "delete", key, ifVersion: current.version, onConflict: "skip" }] });
    return undefined;
  }
  if (current) return (current.value as { firstAbsentAt: number }).firstAbsentAt;
  await mesh.writeBatch({ identity, lockTiming: { operation: "root-absence-mark" }, ops: [], prepare: view => view.get(key) ? [] :
    [{ kind: "put", key, ifVersion: view.version(key), value: { rootId, firstAbsentAt: now } }] });
  return (mesh.get(key, { fresh: true })!.value as { firstAbsentAt: number }).firstAbsentAt;
};

/** Owning host's committed presence pass, never a new timer. Absence is an alert,
 * NOT adoption authority: lineageAlive's positive-closure policy is unchanged. */
export const rootPresenceAlarms = async (mesh: MeshStore, identity: MeshIdentity, hostId: string,
  participants: readonly FabricParticipantInfo[], thresholdMs = 15 * 60_000, now = Date.now()): Promise<void> => {
  await bridgeLeaseAlarms(mesh, identity, now);
  const live = new Set(participants.filter(p => p.kind === "root" && !p.stale).map(p => p.id));
  // Re-arm even if the old members have since disappeared or changed owners.
  for (const entry of mesh.listAll("topology/root-absence/", { fresh: true })) {
    const rootId = (entry.value as { rootId: string }).rootId;
    if (live.has(rootId)) await absence(mesh, identity, rootId, true, now);
  }
  // The local reaper also owns stale native rows left by a dead publishing runtime.
  const roots = new Set(participants.filter(p => p.kind !== "root" && p.remoteHost === undefined &&
    (p.ownerHostId === hostId || p.stale)).map(p => p.rootId));
  for (const rootId of roots) {
    const since = await absence(mesh, identity, rootId, live.has(rootId), now);
    if (since === undefined || now - since <= thresholdMs) continue;
    const members = participants.filter(p => p.kind !== "root" && p.rootId === rootId);
    const byKind: Record<string, number> = Object.create(null), byStatus: Record<string, number> = Object.create(null);
    for (const member of members) { byKind[member.kind] = (byKind[member.kind] ?? 0) + 1; byStatus[member.status] = (byStatus[member.status] ?? 0) + 1; }
    await mesh.publish({ topic: "ops.owner", kind: "root.presence.alarm", from: identity, to: rootId,
      dedupeKey: `root-presence:${rootId}:${since}`, text: `Root ${rootId} absent since ${new Date(since).toISOString()}; ${members.length} members remain`,
      data: { rootId, firstAbsentAt: since, byKind, byStatus } });
  }
};

/** Held messages one alarm lists by id; its heldCount still counts them all. */
const PROVIDER_DEAD_LISTED_MESSAGES = 50;
/** Alarms per mesh publishBatch call; the loop publishes batches until every address is covered. */
const PROVIDER_DEAD_ALARM_BATCH = 50;

/**
 * smarty-dev#8742: a Main whose provider rejects every request as non-retryable reaches ops at
 * once, on the inbox-age alarm's path: ops.owner, addressed to the Main's owner and to the sender
 * of every held message (Main holds up to 200), one inbox.age.alarm per address however many of
 * its messages are held. PR #841 review: no held message's sender is left for the age pass.
 * An address's alarm reuses the age pass's dedupe key for its first held message, so it crosses
 * the existing mesh bridge to remote senders and the age pass never repeats it; the age pass
 * still reports that address's other messages if they stay unread past mesh.undeliveredAlarmMs.
 */
/**
 * pi-fabric#939 P2-b: the one recipient of the agent-admission alarm. An owner alarm is never published without one,
 * and never to a name ("org", "org-kate"): only an exact `session:<uuid>` address. The configured owner identity
 * first, else this Main's own session id (the manager's owner); undefined means log locally and skip the publish.
 */
const SESSION_ADDRESS = /^session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const admissionAlarmRecipient = (ownerIdentityId: string | undefined, identity: MeshIdentity | undefined): string | undefined =>
  [ownerIdentityId?.trim(), identity?.kind === "main" ? identity.id : undefined]
    .find((candidate): candidate is string => typeof candidate === "string" && SESSION_ADDRESS.test(candidate));
/** Local admission diagnostics retain raw failure detail; callers supply only a fixed,
 * payload-free gate classification here. The directory owns episode timing and retries. */
export const controlAdmissionAlarm = async (mesh: MeshStore, identity: MeshIdentity, rootId: string,
  hostId: string, incident: { since: number; dedupeKey: string; reason: string }): Promise<void> => {
  const to = admissionAlarmRecipient(rootId, undefined);
  if (!to) return; // Never substitute a name, broadcast, or an unrelated owner.
  await mesh.publish({ topic: "ops.owner", kind: "fabric.control-admission.alarm", from: identity, to,
    dedupeKey: incident.dedupeKey,
    text: `Control admission for ${rootId} gated since ${new Date(incident.since).toISOString()}: ${incident.reason}`,
    data: { rootId, hostId, controlAdmission: "gated", since: incident.since, reason: incident.reason } });
};

/** An operational source outage is host health, not a control-admission gate. */
export const isolatedParticipantSourceAlarm = async (mesh: MeshStore, identity: MeshIdentity, rootId: string,
  hostId: string, incident: FabricIsolatedSourceFailure & { dedupeKey: string }): Promise<void> => {
  const to = admissionAlarmRecipient(rootId, undefined);
  if (!to) return;
  const { source, since, reason, participantId } = incident;
  await mesh.publish({ topic: "ops.owner", kind: "fabric.participant-source.alarm", from: identity, to,
    dedupeKey: incident.dedupeKey,
    text: `Participant source ${source} on ${hostId} failed since ${new Date(since).toISOString()}: ${reason}`,
    data: { rootId, hostId, source, since, reason, ...(participantId ? { participantId } : {}) } });
};

/** The mesh envelope of an AgentManager alarm. A named run's failed recovery save is an ops.owner `actor.alarm`
 * (pi-fabric#939 round 4). The held-permit count and a child launched without a short TMPDIR that fits the fleet bash
 * guard (pi-fabric#1554, a host condition) are fabric.alarm.agent-admission events of their own kinds. */
export const admissionAlarmEnvelope = (alarm: { kind?: string }): { topic: string; kind: string } =>
  alarm.kind === "recovered-answer-save-failed" ? { topic: "ops.owner", kind: "actor.alarm" }
    : { topic: "fabric.alarm.agent-admission", kind: alarm.kind === "short-tmpdir-unavailable" ? alarm.kind : "agent-admission-held" };

export const providerDeadAlarms = async (mesh: MeshStore, identity: MeshIdentity, rootId: string,
  event: ProviderDeadAlarmEvent, ownerIdentityId?: string): Promise<void> => {
  const owners = [...new Set([identity.id, ...(ownerIdentityId ? [ownerIdentityId] : [])])];
  const fields = providerDeadAlarmFields(event);
  const reason = fields.kind === "unclassified" ? "provider-dead (unclassified)" : `provider-dead (non-retryable: ${fields.error})`;
  for (const to of owners) await mesh.publish({
    topic: "ops.owner", kind: "fabric.main.provider-dead", from: identity, to,
    dedupeKey: `provider-dead:${rootId}:${event.since}:${to}`,
    text: `Main ${rootId} ${reason}: ${fields.cause}; ${event.held.length} held message(s) unread`,
    data: { rootId, ...fields, held: event.held.slice(0, PROVIDER_DEAD_LISTED_MESSAGES), heldCount: event.held.length } });
  // Deduplicated per address: each sender once, listing its own held messages; owners list all.
  const byAddress = new Map<string, Array<ProviderDeadAlarmEvent["held"][number]>>();
  for (const item of event.held) {
    for (const to of new Set([item.from, ...owners])) {
      const items = byAddress.get(to);
      if (items) items.push(item); else byAddress.set(to, [item]);
    }
  }
  const alarms = [...byAddress].map(([to, items]) => {
    const first = items[0]!;
    return { topic: "ops.owner", kind: "inbox.age.alarm", from: identity, to, dedupeKey: `inbox-age:${first.id}:${to}`,
      text: items.length === 1 ? `Undelivered Main message ${first.id} to ${rootId}: ${reason}`
        : `${items.length} undelivered Main messages to ${rootId}, first ${first.id}: ${reason}`,
      data: { messageId: first.id, rootId, sentAt: first.sentAt, reason: "provider-dead", error: fields.error,
        messageIds: items.slice(0, PROVIDER_DEAD_LISTED_MESSAGES).map(item => item.id), heldCount: items.length } };
  });
  // A batch may commit only a prefix: continue from the first uncommitted alarm.
  for (let next = 0; next < alarms.length;) {
    const committed = await mesh.publishBatch(alarms.slice(next, next + PROVIDER_DEAD_ALARM_BATCH));
    if (!committed.length) throw new Error("Provider-dead alarm batch committed nothing");
    next += committed.length;
  }
};

/** What a provider-dead alarm may carry: the shape of FabricMainProviderDeadEvent, nothing else. */
export interface ProviderDeadAlarmEvent {
  kind?: "non-retryable" | "unclassified";
  error: string; status?: number; type?: string; code?: string; failures?: number;
  since: string; cause: string;
  held: ReadonlyArray<{ id: string; from: string; sentAt: number }>;
}

/**
 * smarty-dev#8742 security review: alarm data is an explicit allowlist (status, known type, fixed
 * code, Fabric's own cause), so a field an event happens to carry can never ride along to ops.owner,
 * held messages' senders or the mesh bridge. Provider message text is never one of them.
 */
export const providerDeadAlarmFields = (event: Omit<ProviderDeadAlarmEvent, "held">) => ({
  kind: event.kind ?? "non-retryable", error: event.error, since: event.since, cause: event.cause,
  ...(event.status === undefined ? {} : { status: event.status }),
  ...(event.type === undefined ? {} : { type: event.type }),
  ...(event.code === undefined ? {} : { code: event.code }),
  ...(event.failures === undefined ? {} : { failures: event.failures }),
});

export class MainInboxMaintenance {
  readonly #receipts = new Map<string, SessionReceiptManager>();
  constructor(readonly mesh: MeshStore, readonly identity: MeshIdentity, readonly participants: FabricParticipantSource,
    readonly main: MainAgentController, readonly options: StallAlarmOptions,
    readonly readNativeCustody?: NativeRecoveryCustodyReader) {}

  async run(now = Date.now()): Promise<void> {
    const ownerFor = (rootId: string): InboxOwner | undefined => {
      for (const name of names(mailbox(this.mesh.root)).filter(name => name.endsWith(".owner.json"))) {
        const owner = read<InboxOwner>(path.join(mailbox(this.mesh.root), name));
        if (owner?.rootId === rootId) return owner;
      }
      return undefined;
    };
    const confirmed = (owner: InboxOwner): Set<string> => {
      const key = JSON.stringify([owner.sessionId, owner.sessionFile]);
      let manager = this.#receipts.get(key);
      if (!manager) {
        manager = { getEntries: () => [], getSessionFile: () => owner.sessionFile, isPersisted: () => true };
        this.#receipts.set(key, manager);
      }
      return confirmedMainInboxIds(manager, owner.sessionId);
    };
    const dropJournalItem = (owner: Pick<InboxOwner, "sessionId">, id: string): void => {
      const file = path.join(mailbox(this.mesh.root), `${encodeURIComponent(owner.sessionId)}.json`);
      const journal = read<{ version: 1; items: HeldAgentMessage[] }>(file);
      if (journal?.items.some(item => item.id === id)) writeJsonAtomic(file,
        { ...journal, items: journal.items.filter(item => item.id !== id) }, { durable: true });
    };
    const alarm = async (item: HeldAgentMessage, rootId: string, ownerIdentityId: string): Promise<void> => {
      if (now - item.sentAt <= this.options.undeliveredAlarmMs) return;
      for (const to of new Set([item.from.id, ownerIdentityId])) await this.mesh.publish({
        topic: "ops.owner", kind: "inbox.age.alarm", from: this.identity, to,
        dedupeKey: `inbox-age:${item.id}:${to}`, text: `Undelivered Main message ${item.id} to ${rootId}`,
        data: { messageId: item.id, rootId, sentAt: item.sentAt } });
    };
    // Do this before receipt cleanup or live-root filtering: PREPARED seals S even
    // while its original process and presence still look alive.
    if (this.readNativeCustody) {
      const scopes = new Map<string, NativeRecoverySessionRef>();
      for (const name of names(mailbox(this.mesh.root)).filter(name => name.endsWith(".owner.json"))) {
        const owner = read<InboxOwner>(path.join(mailbox(this.mesh.root), name));
        if (owner?.sessionFile) scopes.set(owner.sessionId, { sessionId: owner.sessionId, sessionFile: owner.sessionFile });
      }
      for (const name of names(path.join(mailbox(this.mesh.root), "routes")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))) {
        const route = read<InboxRoute>(path.join(mailbox(this.mesh.root), "routes", name));
        if (route?.nativeRecovery) scopes.set(route.nativeRecovery.source.sessionId, route.nativeRecovery.source);
      }
      for (const scope of scopes.values()) {
        try { await reconcileNativeMainInboxCustody(this.mesh, scope, this.readNativeCustody); }
        catch { /* Keep originals; every destructive/admission decision rechecks below. */ }
      }
    }
    this.main.confirmInbox();
    const roots = this.participants.list({ kinds: ["root"], scope: "project", fresh: true });
    const live = new Set(roots.filter(p => !p.stale).map(p => p.id));
    for (const name of names(mailbox(this.mesh.root)).filter(name => name.endsWith(".owner.json"))) {
      const owner = read<InboxOwner>(path.join(mailbox(this.mesh.root), name));
      if (!owner || typeof owner.rootId !== "string" || typeof owner.sessionId !== "string" || !Number.isSafeInteger(owner.pid)) throw new Error("Invalid Main inbox owner");
      const journal = read<{ items: HeldAgentMessage[] }>(path.join(mailbox(this.mesh.root), `${encodeURIComponent(owner.sessionId)}.json`));
      if (!journal?.items?.length) continue;
      // Only the owning Main monitors a live inbox; any surviving Main may recover a dead one.
      if (live.has(owner.rootId) && owner.rootId !== this.identity.id) continue;
      const since = await absence(this.mesh, this.identity, owner.rootId, live.has(owner.rootId), now);
      const ids = confirmed(owner);
      for (const item of journal.items) {
        // nextTurn is passive session context, not a Main steer/followUp to inherit.
        if (item.deliverAs === "nextTurn") continue;
        if (ids.has(item.id) || (item.chain && ids.has(item.chain))) continue;
        const previousRoute = read<InboxRoute>(routeFile(this.mesh.root, item.id));
        if (previousRoute && previousRoute.newRoot !== owner.rootId) continue;
        await alarm(item, owner.rootId, owner.ownerIdentityId);
        // A lapsed lease is not permission to move a still-running writer's volatile queue.
        if (live.has(owner.rootId) || (!owner.retired && residentProcessAlive(owner.pid, owner.processStartedAt))) continue;
        await this.mesh.custody(() => {
          const currentOwner = read<InboxOwner>(ownerFile(this.mesh.root, owner.sessionId));
          if (this.readNativeCustody && owner.sessionFile) {
            const custody = readNativeRecoveryCustody({ sessionId: owner.sessionId, sessionFile: owner.sessionFile }, this.readNativeCustody);
            if (custody.status === "hold" || (currentOwner?.nativeRecovery && custody.status === "none") ||
                nativeRecoveryCarrierCustody(custody, { carrierId: item.id,
                  ...(item.chain ? { chain: item.chain } : {}), ...(item.deliveryId ? { deliveryId: item.deliveryId } : {}) }).status !== "none") return;
          } else if (currentOwner?.nativeRecovery) return;
          if (!currentOwner || JSON.stringify(currentOwner) !== JSON.stringify(owner) ||
            (!currentOwner.retired && residentProcessAlive(currentOwner.pid, currentOwner.processStartedAt))) return;
          // Native confirmation can publish succession without changing a retired
          // owner. Decide reroute OR TTL disposal under custody, holding the lock
          // through the durable claim and source removal: no pre-lock absence of
          // a successor can authorize destructive no-destination custody.
          const successor = read<Successor>(successorFile(this.mesh.root, owner.rootId));
          const newRoot = successor?.oldRoot === owner.rootId ? successor.newRoot : undefined;
          if (!newRoot && (since === undefined || now - since <= this.options.rootGoneTtlMs)) return;
          const file = routeFile(this.mesh.root, item.id);
          const prior = read<InboxRoute>(file);
          if (prior && prior.newRoot !== owner.rootId) return;
          writeJsonAtomic(file, { ...(newRoot ? { newRoot } : {}),
            messageId: item.id, item, oldRoot: prior && !prior.done ? prior.oldRoot : owner.rootId } satisfies InboxRoute, { durable: true });
          // Move, not copy: the durable claim owns the payload before the old journal
          // loses it. A crash here is recovered from that claim, never from both inboxes.
          dropJournalItem(owner, item.id);
        });
      }
    }
    for (const name of names(path.join(mailbox(this.mesh.root), "routes")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))) {
      const file = path.join(mailbox(this.mesh.root), "routes", name);
      const pending = read<InboxRoute>(file)!;
      if (pending.nativeRecovery) {
        // A route disposition is not native consumption. Even a done route keeps
        // payload/outcome obligations while native input is pending or ambiguous.
        if (!this.readNativeCustody) continue;
        await this.mesh.custody(() => {
          const current = read<InboxRoute>(file);
          if (!current?.nativeRecovery || JSON.stringify(current) !== JSON.stringify(pending)) return;
          const custody = readNativeRecoveryCustody(current.nativeRecovery.source, this.readNativeCustody!);
          if (custody.status !== "validated") return;
          const transfer = custody.transfers.find(edge => sameNativeOperation(edge, current.nativeRecovery!));
          if (!transfer || transfer.phase !== "received" || current.newRoot !== nativeRoot(transfer.target)) return;
          const status = nativeRecoveryCarrierCustody(custody, { carrierId: current.messageId });
          if (status.status !== "received" && status.status !== "consumed") return;
          if (!current.item || current.newRoot !== this.identity.id || !mainInboxActive(this.mesh.root, current.newRoot)) return;
          const receiver = nativeReceiver(this.mesh.root, transfer.target, true);
          const successor = read<Successor>(successorFile(this.mesh.root, nativeRoot(transfer.source)));
          if (!sameNativeOperation(successor?.nativeRecovery ?? current.nativeRecovery, transfer) ||
              !sameLauncherIdentity(successor?.nativeRecovery?.receiver, receiver)) return;
          // Main adopts original bookkeeping without sending native-owned input.
          // Its own reader and durable outcome/index barriers decide retirement.
          this.main.receiveInboxItem(current.item);
          if (status.status === "consumed") {
            dropJournalItem({ sessionId: transfer.source.sessionId }, current.item.id);
            const { item: _payload, ...claim } = current;
            writeJsonAtomic(file, { ...claim, done: true }, { durable: true });
          }
        });
        continue;
      }
      if (pending.done) continue;
      const item = pending.item;
      if (!item || pending.messageId !== item.id) throw new Error("Invalid pending Main inbox claim");
      const target = pending.newRoot;
      const since = target ? await absence(this.mesh, this.identity, target, live.has(target), now) : undefined;
      const observedOwner = target ? ownerFor(target) : undefined;
      const observedIds = observedOwner ? confirmed(observedOwner) : undefined;
      if (target && !observedIds?.has(item.id) && !(item.chain && observedIds?.has(item.chain))) {
        await alarm(item, target, observedOwner?.ownerIdentityId ?? target);
      }
      // Claims are durable inbox custody, even before the target has a journal.
      // Serialize recovery AND journal admission against root activation and other
      // drainers under the file custody lock (smarty-dev#6477 L5); publishing happens
      // afterwards, outside custody.
      const route = await this.mesh.custody((): InboxRoute | undefined => {
        const current = read<InboxRoute>(file);
        if (!current || JSON.stringify(current) !== JSON.stringify(pending)) return;
        if (current.nativeRecovery) return;
        if (this.readNativeCustody) {
          const owners = [ownerFor(current.oldRoot), ...(target ? [ownerFor(target)] : [])];
          for (const source of owners) {
            if (!source?.sessionFile) continue;
            const custody = readNativeRecoveryCustody({ sessionId: source.sessionId, sessionFile: source.sessionFile }, this.readNativeCustody);
            if (custody.status === "hold" || (source.nativeRecovery && custody.status === "none") ||
                nativeRecoveryCarrierCustody(custody, { carrierId: item.id,
                  ...(item.chain ? { chain: item.chain } : {}), ...(item.deliveryId ? { deliveryId: item.deliveryId } : {}) }).status !== "none") return;
          }
        } else if (ownerFor(current.oldRoot)?.nativeRecovery || (target && ownerFor(target)?.nativeRecovery)) return;
        if (!target) return current;
        const owner = ownerFor(target);
        const ids = owner ? confirmed(owner) : undefined;
        if (ids?.has(item.id) || (item.chain && ids?.has(item.chain))) return current;
        if (target === this.identity.id && mainInboxActive(this.mesh.root, target) && !owner?.retired) {
          this.main.receiveInboxItem(item);
          return current;
        }
        // A lapsed lease is still not a death proof, including for claim payloads.
        if (live.has(target) || (owner && !owner.retired && residentProcessAlive(owner.pid, owner.processStartedAt))) return;
        const successor = read<Successor>(successorFile(this.mesh.root, target));
        const next = successor?.oldRoot === target ? successor.newRoot : undefined;
        if (!next && (since === undefined || now - since <= this.options.rootGoneTtlMs)) return;
        const { newRoot: _target, ...custody } = current;
        const recovered: InboxRoute = { ...custody, ...(next ? { newRoot: next } : {}) };
        writeJsonAtomic(file, recovered, { durable: true });
        if (owner) dropJournalItem(owner, item.id);
        if (!next) return recovered;
        if (next === this.identity.id && mainInboxActive(this.mesh.root, next)) {
          this.main.receiveInboxItem(item);
          return recovered;
        }
        return undefined; // The named live successor (or a later recovery pass) drains.
      });
      if (!route) continue;
      const text = route.newRoot ? `rerouted: ${route.oldRoot} -> ${route.newRoot}` : "undeliverable: root gone";
      await this.mesh.publish({ topic: "fleet.work.inbox-receipts", kind: route.newRoot ? "rerouted" : "undeliverable",
        from: this.identity, to: item.from.id, text, dedupeKey: `inbox-disposition:${item.id}:${route.oldRoot}:${route.newRoot ?? "gone"}`,
        data: { messageId: item.id, oldRoot: route.oldRoot, newRoot: route.newRoot } });
      await this.mesh.custody(() => {
        const current = read<InboxRoute>(file);
        if (!current || JSON.stringify(current) !== JSON.stringify(route)) return;
        const { item: _payload, ...claim } = route;
        writeJsonAtomic(file, { ...claim, done: true }, { durable: true });
      });
    }
  }
}
