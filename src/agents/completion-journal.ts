import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { writeJsonAtomic } from "../core/atomic-write.js";
import type { MeshStore } from "../mesh/store.js";
import type { AgentRunRecord, AgentRunResult } from "./types.js";
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
const canonical = (value: string): string => {
  try { return fs.realpathSync.native(value); } catch { return path.resolve(value); }
};
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
const receiptPath = (meshRoot: string, id: string): string => path.join(directory(meshRoot), "receipts", `${key(id)}.json`);
const read = <T>(file: string): T | undefined => {
  try { return JSON.parse(fs.readFileSync(file, "utf8")) as T; } catch { return undefined; }
};
export const completionConsumed = (meshRoot: string, id: string): boolean => {
  const receipt = read<{ id?: string }>(receiptPath(meshRoot, id));
  return receipt?.id === id;
};
export const consumeCompletion = (meshRoot: string, id: string, sessionId: string): void => {
  if (!completionConsumed(meshRoot, id)) {
    writeJsonAtomic(receiptPath(meshRoot, id), { id, sessionId, consumedAt: Date.now() }, { durable: true });
  }
};
/** Stable run id is the idempotency key; receipts survive every successor and release. */
export const saveCompletion = (meshRoot: string, recipient: CompletionRecipient, result: AgentRunResult): void => {
  if (result.actorId || completionConsumed(meshRoot, result.id)) return;
  const file = envelopePath(meshRoot, result.id);
  const existing = read<CompletionEnvelope>(file);
  if (existing?.format === 1 && existing.result?.id === result.id) return;
  writeJsonAtomic(file, { format: 1, recipient, result } satisfies CompletionEnvelope, { durable: true });
};
/** Ordinary detached workers finish without their Main/manager. The launch manifest is host-owned. */
export const saveWorkerCompletion = (statusFile: string, result: AgentRunRecord): void => {
  const manifest = read<{ meshRoot: string; recipient: CompletionRecipient }>(path.join(path.dirname(statusFile), "completion-recipient.json"));
  if (!manifest || typeof manifest.meshRoot !== "string" || !manifest.recipient ||
    typeof manifest.recipient.rootId !== "string" || typeof manifest.recipient.sessionId !== "string" ||
    typeof manifest.recipient.cwd !== "string" || typeof manifest.recipient.projectRoot !== "string" ||
    typeof manifest.recipient.name !== "string" || typeof manifest.recipient.startedAt !== "number" ||
    !["completed", "failed", "stopped", "timed_out"].includes(result.status)) return;
  try { saveCompletion(manifest.meshRoot, manifest.recipient, result as AgentRunResult); }
  catch (error) {
    result.warnings = [...(result.warnings ?? []), `Completion remains in the worker status: journal save failed: ${String(error).slice(0, 500)}`];
  }
};
const savedCompletion = (meshRoot: string, projectRoot: string, id: string): CompletionEnvelope | undefined => {
  const value = read<CompletionEnvelope>(envelopePath(meshRoot, id));
  if (value?.format !== 1 || value.result?.id !== id || typeof value.recipient?.projectRoot !== "string" ||
    canonical(value.recipient.projectRoot) !== canonical(projectRoot)) return undefined;
  return value;
};
const legacyCompletionConsumed = (meshRoot: string, envelope: CompletionEnvelope): boolean => {
  // A B72 Main may consume a newer host's result using only its existing metadata receipt.
  const metadata = read<{ rootId?: string; id?: string; completionConsumedAt?: number }>(
    path.join(meshRoot, "residency", key(envelope.recipient.rootId), "agents", `${envelope.result.id}.json`));
  return metadata?.rootId === envelope.recipient.rootId && metadata.id === envelope.result.id &&
    typeof metadata.completionConsumedAt === "number" && metadata.completionConsumedAt > 0;
};
export const pendingCompletions = (meshRoot: string, projectRoot: string): CompletionEnvelope[] => {
  let files: string[];
  try { files = fs.readdirSync(directory(meshRoot)); } catch { return []; }
  return files.filter(file => /^[a-f0-9]{64}\.json$/.test(file)).flatMap(file => {
    // Completed payloads can be large. Read only their small receipt during idle scans.
    const receipt = read<{ id?: string }>(path.join(directory(meshRoot), "receipts", file));
    if (typeof receipt?.id === "string" && `${key(receipt.id)}.json` === file) return [];
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
    return legacyCompletionConsumed(meshRoot, value) ? [] : [value];
  });
};
export const pendingCompletionResult = (envelope: CompletionEnvelope): AgentRunResult => ({
  ...envelope.result,
  completionDelivery: { status: "undelivered", addressedTo: envelope.recipient.sessionId },
});

export class CompletionJournal {
  readonly #enqueued = new Set<string>();
  constructor(readonly meshRoot: string, readonly recipient: CompletionRecipient,
    readonly participants: FabricParticipantSource, readonly mesh: MeshStore,
    readonly enqueue: (result: AgentRunResult, delivered: () => void) => void) {}

  save(result: AgentRunResult): void { saveCompletion(this.meshRoot, this.recipient, result); }
  forget(id: string): void {
    consumeCompletion(this.meshRoot, id, this.recipient.sessionId);
    fs.rmSync(envelopePath(this.meshRoot, id), { force: true });
    this.#enqueued.delete(id);
  }
  pending(): CompletionEnvelope[] { return pendingCompletions(this.meshRoot, this.recipient.projectRoot); }
  result(id: string): AgentRunResult | undefined {
    const envelope = savedCompletion(this.meshRoot, this.recipient.projectRoot, id);
    if (!envelope) return undefined;
    const { completionDelivery: _delivery, ...result } = envelope.result;
    return completionConsumed(this.meshRoot, id) || legacyCompletionConsumed(this.meshRoot, envelope)
      ? result : pendingCompletionResult(envelope);
  }
  acknowledge(id: string): void {
    const envelope = this.pending().find(value => value.result.id === id);
    // A wait can consume before the background callback publishes its envelope.
    if (!envelope || this.#canDeliver(envelope)) {
      consumeCompletion(this.meshRoot, id, this.recipient.sessionId);
      this.#enqueued.delete(id);
    }
  }
  async drain(): Promise<void> {
    const pending = this.pending();
    if (!pending.length) return;
    const roots = this.participants.list({ scope: "project", kinds: ["root"], fresh: true });
    for (const envelope of pending) {
      if (this.#enqueued.has(envelope.result.id) || !this.#canDeliver(envelope, roots)) continue;
      const claimKey = `residency/completion-claims/${key(envelope.result.id)}`;
      const claim = this.mesh.get(claimKey);
      const owner = (claim?.value as { rootId?: string } | undefined)?.rootId;
      if (owner && owner !== this.recipient.rootId && roots.some(root => root.id === owner)) continue;
      if (owner !== this.recipient.rootId) {
        try {
          await this.mesh.put({ key: claimKey, ifVersion: claim?.version ?? 0,
            identity: { id: this.recipient.rootId, name: "main", kind: "main" },
            value: { rootId: this.recipient.rootId, sessionId: this.recipient.sessionId } });
        } catch { continue; } // Another live successor owns admission; leave the source pending.
      }
      if (completionConsumed(this.meshRoot, envelope.result.id)) continue;
      const redelivered = envelope.recipient.rootId !== this.recipient.rootId;
      this.#enqueued.add(envelope.result.id);
      try {
        this.enqueue({ ...envelope.result, ...(redelivered ? {
          completionDelivery: { status: "undelivered", addressedTo: envelope.recipient.sessionId,
            redeliveredFrom: envelope.recipient.sessionId },
        } : {}) }, () => {
          try { consumeCompletion(this.meshRoot, envelope.result.id, this.recipient.sessionId); }
          finally { this.#enqueued.delete(envelope.result.id); } // A failed receipt is retried, not re-sent.
        });
      } catch { this.#enqueued.delete(envelope.result.id); } // Source stays pending if admission failed.
    }
  }
  #canDeliver(envelope: CompletionEnvelope, roots?: FabricParticipantInfo[]): boolean {
    if (envelope.recipient.rootId === this.recipient.rootId) return true;
    if (canonical(envelope.recipient.projectRoot) !== canonical(this.recipient.projectRoot)) return false;
    return completionSuccessor(envelope.recipient, roots ??
      this.participants.list({ scope: "project", kinds: ["root"], fresh: true }))?.id === this.recipient.rootId;
  }
}
