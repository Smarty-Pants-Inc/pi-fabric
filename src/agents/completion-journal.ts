import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { syncPathNamespace, writeJsonAtomic } from "../core/atomic-write.js";
import { residentProcessAlive } from "../residency/process-identity.js";
import type { MeshStore } from "../mesh/store.js";
import type { AgentHandleInfo, AgentRunRecord, AgentRunResult } from "./types.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../topology/types.js";

/** Captured while the addressed Main is alive; directory reaping cannot erase its lane. */
export interface CompletionRecipient {
  rootId: string;
  sessionId: string;
  projectRoot: string;
  cwd: string;
  name: string;
  role?: string | undefined;
  startedAt: number;
}
export interface CompletionEnvelope {
  format: 1;
  recipient: CompletionRecipient;
  result: AgentRunResult;
}
/** An attempt is not logical settlement: its supervisor may resume/retry the same run id. */
interface CompletionCandidate extends CompletionEnvelope {
  supervisor: { pid: number; processStartedAt?: string };
}
/** Remote observers get an allowlisted summary, never private result/log/session bodies. */
export interface CompletionSummary extends AgentHandleInfo {
  startedAt: number;
  updatedAt: number;
  finishedAt?: number;
  completionDelivery?: AgentRunRecord["completionDelivery"];
}
const canonical = (value: string): string => {
  try { return fs.realpathSync.native(value); } catch { return path.resolve(value); }
};
const sameRecipientLane = (a: CompletionRecipient, b: CompletionRecipient): boolean =>
  canonical(a.cwd) === canonical(b.cwd) && canonical(a.projectRoot) === canonical(b.projectRoot) &&
  a.name === b.name && a.role === b.role;
const sameLane = (recipient: CompletionRecipient, root: FabricParticipantInfo): boolean =>
  root.remoteHost === undefined && root.kind === "root" && root.cwd !== undefined &&
  canonical(root.cwd) === canonical(recipient.cwd) &&
  canonical(root.projectRoot ?? root.cwd) === canonical(recipient.projectRoot) &&
  root.name === recipient.name && root.role === recipient.role;

/** Fresh, live roots only. A reload lease is live, not permission to steal its results. */
export const completionSuccessor = (
  recipient: CompletionRecipient, roots: readonly FabricParticipantInfo[],
): FabricParticipantInfo | undefined => {
  if (!(recipient.startedAt > 0) || roots.some(root => root.id === recipient.rootId && !root.stale)) return undefined;
  return roots.filter(root => !root.stale && sameLane(recipient, root) && root.sessionId !== recipient.sessionId &&
    root.startedAt > recipient.startedAt && root.interactive !== false &&
    root.capabilities.includes("steer") && root.capabilities.includes("followUp"))
    .sort((a, b) => b.startedAt - a.startedAt || a.id.localeCompare(b.id))[0];
};
const key = (id: string): string => createHash("sha256").update(id).digest("hex");
const directory = (meshRoot: string): string => path.join(meshRoot, "agent-completions");
const envelopePath = (meshRoot: string, id: string): string => path.join(directory(meshRoot), `${key(id)}.json`);
const candidatePath = (meshRoot: string, id: string): string => path.join(directory(meshRoot), "attempts", `${key(id)}.json`);
const receiptPath = (meshRoot: string, id: string): string => path.join(directory(meshRoot), "receipts", `${key(id)}.json`);
const claimPrefix = "residency/completion-claims/";
const claimKey = (id: string): string => `${claimPrefix}${key(id)}`;
const read = <T>(file: string): T | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; } catch { return undefined; }
};
const files = (dir: string): string[] => {
  try { return fs.readdirSync(dir).filter(file => /^[a-f0-9]{64}\.json$/.test(file)); } catch { return []; }
};
interface CompletionReceipt { id: string; sessionId: string; consumedAt: number }
/** Only proven absence authorizes delivery. An unknown replay fence is a storage fault. */
const readReplayFence = <T>(file: string, label = "Completion"): T | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      // A dangling receipt symlink exists but cannot be read: ENOENT alone is not absence.
      try { fs.lstatSync(file); } catch (absence) {
        if ((absence as NodeJS.ErrnoException).code === "ENOENT") return undefined;
      }
    }
    throw new Error(`${label} replay fence is unreadable at ${file}: ${String(error)}`);
  }
};
/** A readable rename may have failed its post-rename barrier. Confirm this attempt,
 * binding both the validated bytes and reopenable namespace to the synced inode. */
const syncCompletionFile = (file: string, value: unknown): void => {
  // Match the inbox: Windows FlushFileBuffers requires a write-capable file handle.
  const fd = fs.openSync(file, process.platform === "win32" ? "r+" : "r");
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || JSON.stringify(JSON.parse(fs.readFileSync(fd, "utf8"))) !== JSON.stringify(value)) {
      throw new Error(`Completion file changed before durability confirmation at ${file}`);
    }
    fs.fsyncSync(fd);
    syncPathNamespace(file, stat);
  } finally { fs.closeSync(fd); }
};
const readReceiptRecord = (file: string, id?: string): CompletionReceipt | undefined => {
  const value = readReplayFence<CompletionReceipt>(file);
  if (value === undefined) return undefined;
  if (!value || typeof value.id !== "string" || (id !== undefined && value.id !== id) ||
    path.basename(file) !== `${key(value.id)}.json` || typeof value.sessionId !== "string" || !value.sessionId ||
    typeof value.consumedAt !== "number" || !Number.isFinite(value.consumedAt) || value.consumedAt <= 0) {
    throw new Error(`Completion replay fence is invalid at ${file}; retain the outcome for repair`);
  }
  return value;
};
const readReceipt = (file: string, id?: string): CompletionReceipt | undefined => {
  const value = readReceiptRecord(file, id);
  if (value !== undefined) syncCompletionFile(file, value);
  return value;
};
export const completionConsumed = (meshRoot: string, id: string): boolean =>
  readReceipt(receiptPath(meshRoot, id), id) !== undefined;
export const consumeCompletion = (meshRoot: string, id: string, sessionId: string): void => {
  if (!completionConsumed(meshRoot, id)) {
    writeJsonAtomic(receiptPath(meshRoot, id), { id, sessionId, consumedAt: Date.now() }, { durable: true });
  }
};
/** Stable run id fences committed outcomes. Settlement supersedes an uncommitted attempt. */
export const saveCompletion = (meshRoot: string, recipient: CompletionRecipient, result: AgentRunResult): void => {
  if (result.actorId) return;
  if (!completionConsumed(meshRoot, result.id)) {
    legacyCompletionConsumed(meshRoot, recipient.rootId, result.id);
    const file = envelopePath(meshRoot, result.id);
    const existing = read<CompletionEnvelope>(file);
    if (!(existing?.format === 1 && existing.result?.id === result.id)) {
      writeJsonAtomic(file, { format: 1, recipient, result } satisfies CompletionEnvelope, { durable: true });
    } else {
      // A preceding save may have renamed successfully but thrown before durability.
      syncCompletionFile(file, existing);
    }
  }
  fs.rmSync(candidatePath(meshRoot, result.id), { force: true });
};
/** Logical settlement must use the immutable host-owned launch address, not today's /name. */
export const completionRecipientFromRun = (meshRoot: string, runDirectory: string): CompletionRecipient | undefined => {
  const file = path.join(runDirectory, "completion-recipient.json");
  let manifest: { meshRoot?: string; recipient?: CompletionRecipient };
  try { manifest = JSON.parse(fs.readFileSync(file, "utf8")); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; // legacy run
    throw error;
  }
  const recipient = manifest?.recipient;
  if (typeof manifest?.meshRoot !== "string" || canonical(manifest.meshRoot) !== canonical(meshRoot) ||
    !recipient || typeof recipient.rootId !== "string" || typeof recipient.sessionId !== "string" ||
    typeof recipient.cwd !== "string" || typeof recipient.projectRoot !== "string" ||
    typeof recipient.name !== "string" || typeof recipient.startedAt !== "number" ||
    !Number.isFinite(recipient.startedAt) || (recipient.role !== undefined && typeof recipient.role !== "string")) {
    throw new Error(`Invalid admitted completion recipient at ${file}`);
  }
  return recipient;
};
/** Called only after the preceding worker's exit is confirmed, before its retry launches. */
export const discardWorkerCompletion = (meshRoot: string, id: string): void => {
  fs.rmSync(candidatePath(meshRoot, id), { force: true });
};
/** The host-owned launch manifest pins the retry supervisor, not the recipient Main's lease. */
export const saveWorkerCompletion = (statusFile: string, result: AgentRunRecord): void => {
  const manifest = read<{ meshRoot: string; recipient: CompletionRecipient; supervisor?: CompletionCandidate["supervisor"] }>(
    path.join(path.dirname(statusFile), "completion-recipient.json"));
  if (!manifest || typeof manifest.meshRoot !== "string" || !manifest.recipient ||
    typeof manifest.recipient.rootId !== "string" || typeof manifest.recipient.sessionId !== "string" ||
    typeof manifest.recipient.cwd !== "string" || typeof manifest.recipient.projectRoot !== "string" ||
    typeof manifest.recipient.name !== "string" || typeof manifest.recipient.startedAt !== "number" ||
    !["completed", "failed", "stopped", "timed_out"].includes(result.status) || result.actorId) return;
  // Pre-supervisor manifests cannot prove orphan settlement. Leave their status.json intact;
  // the live manager still commits its logical outcome. Never guess that an attempt is final.
  if (!manifest.supervisor || !Number.isSafeInteger(manifest.supervisor.pid) || manifest.supervisor.pid <= 0) return;
  try {
    if (completionConsumed(manifest.meshRoot, result.id) || fs.existsSync(envelopePath(manifest.meshRoot, result.id))) return;
    writeJsonAtomic(candidatePath(manifest.meshRoot, result.id), {
      format: 1, recipient: manifest.recipient, result: result as AgentRunResult, supervisor: manifest.supervisor,
    } satisfies CompletionCandidate, { durable: true });
  } catch (error) {
    result.warnings = [...(result.warnings ?? []), `Completion remains in the worker status: journal save failed: ${String(error).slice(0, 500)}`];
  }
};
const promoteOrphans = (meshRoot: string, projectRoot: string): void => {
  for (const file of files(path.join(directory(meshRoot), "attempts"))) {
    const candidate = read<CompletionCandidate>(path.join(directory(meshRoot), "attempts", file));
    if (candidate?.format !== 1 || !candidate.result || !candidate.recipient || !candidate.supervisor ||
      !Number.isSafeInteger(candidate.supervisor.pid) || candidate.supervisor.pid <= 0 ||
      typeof candidate.result.id !== "string" || file !== `${key(candidate.result.id)}.json` ||
      typeof candidate.recipient.projectRoot !== "string" || canonical(candidate.recipient.projectRoot) !== canonical(projectRoot)) continue;
    if (completionConsumed(meshRoot, candidate.result.id)) {
      fs.rmSync(candidatePath(meshRoot, candidate.result.id), { force: true });
    } else if (!residentProcessAlive(candidate.supervisor.pid, candidate.supervisor.processStartedAt)) {
      saveCompletion(meshRoot, candidate.recipient, candidate.result);
    }
  }
};
const savedCompletion = (meshRoot: string, projectRoot: string, id: string): CompletionEnvelope | undefined => {
  promoteOrphans(meshRoot, projectRoot);
  const value = read<CompletionEnvelope>(envelopePath(meshRoot, id));
  if (value?.format !== 1 || value.result?.id !== id || typeof value.recipient?.projectRoot !== "string" ||
    canonical(value.recipient.projectRoot) !== canonical(projectRoot)) return undefined;
  return value;
};
export const legacyCompletionConsumed = (meshRoot: string, rootId: string, id: string): boolean => {
  // A B72 Main may consume a newer host's result using only its existing metadata receipt.
  const file = path.join(meshRoot, "residency", key(rootId), "agents", `${id}.json`);
  const metadata = readReplayFence<{ rootId?: string; id?: string; completionConsumedAt?: number }>(file, "Legacy completion");
  if (metadata === undefined) return false;
  if (!metadata || metadata.rootId !== rootId || metadata.id !== id ||
    (metadata.completionConsumedAt !== undefined && (typeof metadata.completionConsumedAt !== "number" ||
      !Number.isFinite(metadata.completionConsumedAt) || metadata.completionConsumedAt <= 0))) {
    throw new Error(`Legacy completion replay fence is invalid at ${file}; retain the outcome for repair`);
  }
  return metadata.completionConsumedAt !== undefined;
};
export const pendingCompletions = (meshRoot: string, projectRoot: string): CompletionEnvelope[] =>
  pendingCompletionsExcept(meshRoot, projectRoot);
const pendingCompletionsExcept = (meshRoot: string, projectRoot: string, consumed?: ReadonlySet<string>): CompletionEnvelope[] => {
  promoteOrphans(meshRoot, projectRoot);
  return files(directory(meshRoot)).flatMap(file => {
    // Already consumed by this journal: suppress replay without using a
    // cached receipt as durability authority for claim retirement or storage.
    if (consumed?.has(file)) return [];
    // Candidate scans only suppress delivery; a visible valid receipt is enough
    // to leave its source alone. They do not consume a body or retire a claim.
    // Those authority paths independently reconfirm file AND namespace barriers.
    // Avoid O(n²) fsyncs of old receipts on every idle journal drain.
    const receipt = path.join(directory(meshRoot), "receipts", file);
    if (consumed ? readReceiptRecord(receipt) : readReceipt(receipt)) return [];
    const value = read<CompletionEnvelope>(path.join(directory(meshRoot), file));
    if (value?.format !== 1 || !value.recipient || !value.result ||
      typeof value.recipient.rootId !== "string" || typeof value.recipient.sessionId !== "string" ||
      typeof value.recipient.projectRoot !== "string" || typeof value.recipient.cwd !== "string" ||
      typeof value.recipient.name !== "string" || typeof value.recipient.startedAt !== "number" ||
      (value.recipient.role !== undefined && typeof value.recipient.role !== "string") ||
      typeof value.result.id !== "string" || !/^[a-f0-9]{32}$/.test(value.result.id) ||
      typeof value.result.name !== "string" || typeof value.result.text !== "string" || typeof value.result.startedAt !== "number" ||
      file !== `${key(value.result.id)}.json` ||
      !["completed", "failed", "stopped", "timed_out"].includes(value.result.status) ||
      canonical(value.recipient.projectRoot) !== canonical(projectRoot) || completionConsumed(meshRoot, value.result.id)) return [];
    return legacyCompletionConsumed(meshRoot, value.recipient.rootId, value.result.id) ? [] : [value];
  });
};
export const pendingCompletionResult = (envelope: CompletionEnvelope): AgentRunResult => ({
  ...envelope.result,
  completionDelivery: { status: "undelivered", addressedTo: envelope.recipient.sessionId },
});

export class CompletionJournal {
  readonly #enqueued = new Set<string>();
  // A monotonic local suppression fence, not a cached durability confirmation.
  // Destructive claim retirement continues to reopen/sync its exact receipt.
  readonly #consumed = new Set<string>();
  #rememberConsumed(id: string): void {
    this.#consumed.add(`${key(id)}.json`);
    if (this.#consumed.size > 1024) this.#consumed.delete(this.#consumed.values().next().value!);
  }
  constructor(readonly meshRoot: string, readonly recipientSource: CompletionRecipient | (() => CompletionRecipient),
    readonly participants: FabricParticipantSource, readonly mesh: MeshStore,
    readonly enqueue: (result: AgentRunResult, delivered: () => void) => void) {}

  get recipient(): CompletionRecipient {
    return typeof this.recipientSource === "function" ? this.recipientSource() : this.recipientSource;
  }
  /** admittedRecipient is host-only queue metadata, never a field supplied by a worker/guest. */
  save(result: AgentRunResult, admittedRecipient?: CompletionRecipient): void {
    if (result.actorId) return;
    const admitted = result.logFile
      ? completionRecipientFromRun(this.meshRoot, path.dirname(result.logFile)) : admittedRecipient;
    // A live-name source cannot safely reconstruct a missing admission address,
    // even when a queued task terminated before its launch manifest existed.
    // Legacy fixed-address journals retain their original immutable fallback.
    if (!admitted && typeof this.recipientSource === "function") {
      throw new Error(`Missing admitted completion recipient for ${result.id}`);
    }
    saveCompletion(this.meshRoot, admitted ?? this.recipient, result);
  }
  forget(id: string): void {
    const envelope = savedCompletion(this.meshRoot, this.recipient.projectRoot, id);
    if (envelope && !this.#canRead(envelope)) return;
    consumeCompletion(this.meshRoot, id, this.recipient.sessionId);
    this.#rememberConsumed(id);
    fs.rmSync(envelopePath(this.meshRoot, id), { force: true });
    fs.rmSync(candidatePath(this.meshRoot, id), { force: true });
    this.#enqueued.delete(id);
    void this.#retireClaim(id).catch(() => undefined); // A crash/failure is reconciled by drain.
  }
  pending(): CompletionEnvelope[] { return pendingCompletionsExcept(this.meshRoot, this.recipient.projectRoot, this.#consumed); }
  result(id: string): AgentRunResult | CompletionSummary | undefined {
    const envelope = savedCompletion(this.meshRoot, this.recipient.projectRoot, id);
    if (!envelope) return undefined;
    const consumed = completionConsumed(this.meshRoot, id) || legacyCompletionConsumed(this.meshRoot, envelope.recipient.rootId, id);
    if (!this.#canRead(envelope)) {
      const r = envelope.result;
      return { id: r.id, name: r.name.slice(0, 80), status: r.status, runner: r.runner, transport: r.transport,
        cwd: envelope.recipient.cwd, startedAt: r.startedAt, updatedAt: r.updatedAt,
        ...(r.finishedAt !== undefined ? { finishedAt: r.finishedAt } : {}),
        ...(!consumed ? { completionDelivery: { status: "undelivered" as const, addressedTo: envelope.recipient.sessionId } } : {}) };
    }
    const { completionDelivery: _delivery, ...result } = envelope.result;
    return consumed ? result : pendingCompletionResult(envelope);
  }
  acknowledge(id: string, localRunSettled = false): boolean {
    const envelope = savedCompletion(this.meshRoot, this.recipient.projectRoot, id);
    // Without an envelope, only the settled owning manager's result-consumption callback can fence a
    // failed/delayed journal publication. Observer status/wait cannot forge an early receipt.
    if (envelope ? !this.#canRead(envelope) : !localRunSettled) return false;
    consumeCompletion(this.meshRoot, id, this.recipient.sessionId);
    this.#rememberConsumed(id);
    this.#enqueued.delete(id);
    void this.#retireClaim(id).catch(() => undefined);
    return true;
  }
  async drain(deliver = true): Promise<void> {
    // Receipts are the durable replay fence. Retire crash-left claims even with no pending body.
    for (const claim of this.mesh.listAll(claimPrefix)) {
      const receipt = readReceipt(path.join(directory(this.meshRoot), "receipts", `${claim.key.slice(claimPrefix.length)}.json`));
      if (typeof receipt?.id === "string" && claim.key === claimKey(receipt.id)) await this.#retireClaim(receipt.id, claim);
    }
    const pending = this.pending();
    if (!pending.length) return;
    const roots = this.participants.list({ scope: "project", kinds: ["root"], fresh: true });
    for (const envelope of pending) {
      if (this.#enqueued.has(envelope.result.id) || !this.#canDeliver(envelope, roots)) continue;
      const ck = claimKey(envelope.result.id);
      const claim = this.mesh.get(ck, { fresh: true });
      const owner = (claim?.value as { rootId?: string } | undefined)?.rootId;
      if (owner && owner !== this.recipient.rootId && roots.some(root => root.id === owner)) continue;
      if (owner !== this.recipient.rootId) {
        try {
          await this.mesh.put({ key: ck, ifVersion: claim?.version ?? 0,
            identity: { id: this.recipient.rootId, name: "main", kind: "main" },
            value: { rootId: this.recipient.rootId, sessionId: this.recipient.sessionId } });
        } catch { continue; } // Another live successor owns admission; leave the source pending.
      }
      if (completionConsumed(this.meshRoot, envelope.result.id)) { await this.#retireClaim(envelope.result.id); continue; }
      // Notification policy suppresses only inbox enqueue, not exact-lane recovery ownership.
      // A quiet successor can still list and explicitly consume its settled result.
      if (!deliver) continue;
      const redelivered = envelope.recipient.rootId !== this.recipient.rootId;
      this.#enqueued.add(envelope.result.id);
      try {
        this.enqueue({ ...envelope.result, ...(redelivered ? {
          completionDelivery: { status: "undelivered", addressedTo: envelope.recipient.sessionId,
            redeliveredFrom: envelope.recipient.sessionId },
        } : {}) }, () => {
          try {
            consumeCompletion(this.meshRoot, envelope.result.id, this.recipient.sessionId);
            this.#rememberConsumed(envelope.result.id);
            void this.#retireClaim(envelope.result.id).catch(() => undefined);
          } finally { this.#enqueued.delete(envelope.result.id); }
        });
      } catch { this.#enqueued.delete(envelope.result.id); } // Source stays pending if admission failed.
    }
  }
  async #retireClaim(id: string, snapshot = this.mesh.get(claimKey(id), { fresh: true })): Promise<void> {
    if (!snapshot || !completionConsumed(this.meshRoot, id)) return;
    const owner = snapshot.value as { rootId?: string; sessionId?: string };
    // Only journal-owned claims; CAS cannot erase a replacement owner/version.
    if (typeof owner?.rootId !== "string" || typeof owner.sessionId !== "string" || snapshot.updatedBy.id !== owner.rootId) return;
    try { await this.mesh.delete({ key: snapshot.key, ifVersion: snapshot.version }); } catch { /* next drain reconciles */ }
  }
  #canRead(envelope: CompletionEnvelope): boolean {
    // Unknown legacy fences block body access and acknowledgment as well as idle delivery.
    legacyCompletionConsumed(this.meshRoot, envelope.recipient.rootId, envelope.result.id);
    if (envelope.recipient.rootId === this.recipient.rootId && envelope.recipient.sessionId === this.recipient.sessionId) return true;
    if (!sameRecipientLane(envelope.recipient, this.recipient) || this.recipient.startedAt <= envelope.recipient.startedAt) return false;
    const receipt = readReceipt(receiptPath(this.meshRoot, envelope.result.id), envelope.result.id);
    if (receipt?.sessionId === this.recipient.sessionId) return true;
    const claim = this.mesh.get(claimKey(envelope.result.id), { fresh: true });
    const owner = claim?.value as { rootId?: string; sessionId?: string } | undefined;
    return owner?.rootId === this.recipient.rootId && owner.sessionId === this.recipient.sessionId &&
      claim?.updatedBy.id === owner.rootId && this.participants.list({ scope: "project", kinds: ["root"], fresh: true })
        .some(root => !root.stale && root.id === owner.rootId && root.sessionId === owner.sessionId &&
          root.interactive !== false && sameLane(envelope.recipient, root)) &&
      !this.participants.list({ scope: "project", kinds: ["root"], fresh: true })
        .some(root => !root.stale && root.id === envelope.recipient.rootId);
  }
  #canDeliver(envelope: CompletionEnvelope, roots?: FabricParticipantInfo[]): boolean {
    if (envelope.recipient.rootId === this.recipient.rootId && envelope.recipient.sessionId === this.recipient.sessionId) return true;
    if (!sameRecipientLane(envelope.recipient, this.recipient)) return false;
    return completionSuccessor(envelope.recipient, roots ??
      this.participants.list({ scope: "project", kinds: ["root"], fresh: true }))?.id === this.recipient.rootId;
  }
}
