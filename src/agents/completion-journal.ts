import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { createHash } from "node:crypto";
import { syncPathNamespace, syncPathNamespaceAsync, writeJsonAtomic } from "../core/atomic-write.js";
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
const canonicalAsync = async (value: string): Promise<string> => {
  try { return await fs.promises.realpath(value); } catch { return path.resolve(value); }
};
const sameRecipientLane = (a: CompletionRecipient, b: CompletionRecipient): boolean =>
  canonical(a.cwd) === canonical(b.cwd) && canonical(a.projectRoot) === canonical(b.projectRoot) &&
  a.name === b.name && a.role === b.role;
const sameRecipientLaneAsync = async (a: CompletionRecipient, b: CompletionRecipient): Promise<boolean> =>
  await canonicalAsync(a.cwd) === await canonicalAsync(b.cwd) &&
  await canonicalAsync(a.projectRoot) === await canonicalAsync(b.projectRoot) &&
  a.name === b.name && a.role === b.role;
const sameLane = (recipient: CompletionRecipient, root: FabricParticipantInfo): boolean =>
  root.remoteHost === undefined && root.kind === "root" && root.cwd !== undefined &&
  canonical(root.cwd) === canonical(recipient.cwd) &&
  canonical(root.projectRoot ?? root.cwd) === canonical(recipient.projectRoot) &&
  root.name === recipient.name && root.role === recipient.role;
const sameLaneAsync = async (recipient: CompletionRecipient, root: FabricParticipantInfo): Promise<boolean> =>
  root.remoteHost === undefined && root.kind === "root" && root.cwd !== undefined &&
  await canonicalAsync(root.cwd) === await canonicalAsync(recipient.cwd) &&
  await canonicalAsync(root.projectRoot ?? root.cwd) === await canonicalAsync(recipient.projectRoot) &&
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
const completionSuccessorAsync = async (
  recipient: CompletionRecipient, roots: readonly FabricParticipantInfo[],
): Promise<FabricParticipantInfo | undefined> => {
  if (!(recipient.startedAt > 0) || roots.some(root => root.id === recipient.rootId && !root.stale)) return undefined;
  const eligible: FabricParticipantInfo[] = [];
  for (const root of roots) {
    if (!root.stale && root.sessionId !== recipient.sessionId && root.startedAt > recipient.startedAt &&
      root.interactive !== false && root.capabilities.includes("steer") && root.capabilities.includes("followUp") &&
      await sameLaneAsync(recipient, root)) eligible.push(root);
  }
  return eligible.sort((a, b) => b.startedAt - a.startedAt || a.id.localeCompare(b.id))[0];
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
const readAsync = async <T>(file: string): Promise<T | undefined> => {
  try { return JSON.parse(await fs.promises.readFile(file, "utf8")) as T; } catch { return undefined; }
};
const isJournalFile = (file: string): boolean => /^[a-f0-9]{64}\.json$/.test(file);
const files = (dir: string): string[] => {
  try { return fs.readdirSync(dir).filter(isJournalFile); } catch { return []; }
};
// Idle scans must yield on inspected entries, not just successful cleanup/delivery.
// A setImmediate turn lets timers/IO run between slices without chaining microtasks.
// Keep both a time budget and an entry cap: one slow read must not make a
// slice unbounded, while the cap protects against cheap work starving the event loop.
const SCAN_SLICE_BUDGET_MS = 4;
const SCAN_SLICE_ENTRY_LIMIT = 2;
export type CompletionJournalSliceObserver = (durationMs: number) => void;
let sliceObserver: CompletionJournalSliceObserver | undefined;
export const setCompletionJournalSliceObserver = (observer: CompletionJournalSliceObserver | undefined): void => {
  sliceObserver = observer;
};
async function* scanSlices<T>(entries: Iterable<T>): AsyncGenerator<T> {
  let scanned = 0;
  let sliceStartedAt = performance.now();
  const finishSlice = (): void => {
    sliceObserver?.(performance.now() - sliceStartedAt);
  };
  for (const entry of entries) {
    // The previous consumer's synchronous reads/parsing count toward this budget.
    // Generator resumes and resolved promises are not event-loop turns: reset the
    // clock only after our setImmediate runs. Actual consumer IO waits may
    // conservatively cause an extra turn, but must not hide synchronous work.
    if (scanned > 0 && (scanned >= SCAN_SLICE_ENTRY_LIMIT || performance.now() - sliceStartedAt >= SCAN_SLICE_BUDGET_MS)) {
      finishSlice();
      await new Promise<void>(resolve => setImmediate(resolve));
      sliceStartedAt = performance.now();
      scanned = 0;
    }
    scanned++;
    yield entry;
  }
  if (scanned > 0) finishSlice();
}
async function* scanTargets(dirs: readonly string[]): AsyncGenerator<string> {
  for (const dir of dirs) {
    let entries: string[];
    try { entries = await fs.promises.readdir(dir); } catch { continue; }
    for await (const file of scanSlices(entries.filter(isJournalFile))) {
      yield path.join(dir, file);
    }
  }
}
interface CompletionReceipt { id: string; sessionId: string; consumedAt: number }
interface CompletionClaim { rootId: string; sessionId: string; recipient?: CompletionRecipient }
const fingerprint = (stat: fs.Stats): string => JSON.stringify([stat.dev, stat.ino, stat.size, stat.mtimeMs, stat.ctimeMs]);
// Plain scans only read receipts. Every destructive cleanup confirms the full namespace
// afresh: an unchanged endpoint inode is not evidence that its parent entries are durable.
const confirmReceipt = async (file: string, value: CompletionReceipt): Promise<void> => {
  const handle = await fs.promises.open(file, process.platform === "win32" ? "r+" : "r");
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || JSON.stringify(JSON.parse(await handle.readFile("utf8"))) !== JSON.stringify(value)) {
      throw new Error(`Completion file changed before durability confirmation at ${file}`);
    }
    await handle.sync();
    await syncPathNamespaceAsync(file, stat);
    if (fingerprint(await handle.stat()) !== fingerprint(stat)) throw new Error(`Completion file changed during confirmation at ${file}`);
  } finally { await handle.close(); }
};
// Journal writers put the address before the result. Read only that bounded prefix,
// including old pretty-printed envelopes; never parse a foreign result body.
const addressBuffer = Buffer.alloc(16 * 1024);
const readRecipient = (file: string): CompletionRecipient | undefined => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, "r");
    const prefix = addressBuffer.toString("utf8", 0, fs.readSync(fd, addressBuffer, 0, addressBuffer.length, 0));
    const start = /^\s*\{\s*"format"\s*:\s*1\s*,\s*"recipient"\s*:\s*/.exec(prefix)?.[0].length;
    if (start === undefined || prefix[start] !== "{") return undefined;
    let depth = 0, quoted = false, escaped = false;
    for (let i = start; i < prefix.length; i++) {
      const character = prefix[i];
      if (quoted) {
        if (escaped) escaped = false;
        else if (character === "\\") escaped = true;
        else if (character === '"') quoted = false;
      } else if (character === '"') quoted = true;
      else if (character === "{") depth++;
      else if (character === "}" && --depth === 0) {
        const address = JSON.parse(prefix.slice(start, i + 1)) as CompletionRecipient;
        return typeof address.projectRoot === "string" && typeof address.cwd === "string" &&
          typeof address.rootId === "string" && typeof address.sessionId === "string" &&
          typeof address.name === "string" ? address : undefined;
      }
    }
  } catch { /* Missing, torn or oversized addresses cannot authorize body/fence access. */ }
  finally { if (fd !== undefined) fs.closeSync(fd); }
  return undefined;
};
const readRecipientAsync = async (file: string): Promise<CompletionRecipient | undefined> => {
  let handle: fs.promises.FileHandle | undefined;
  try {
    handle = await fs.promises.open(file, "r");
    const buffer = Buffer.alloc(addressBuffer.length);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const prefix = buffer.toString("utf8", 0, bytesRead);
    const start = /^\s*\{\s*"format"\s*:\s*1\s*,\s*"recipient"\s*:\s*/.exec(prefix)?.[0].length;
    if (start === undefined || prefix[start] !== "{") return undefined;
    let depth = 0, quoted = false, escaped = false;
    for (let i = start; i < prefix.length; i++) {
      const character = prefix[i];
      if (quoted) {
        if (escaped) escaped = false;
        else if (character === "\\") escaped = true;
        else if (character === '"') quoted = false;
      } else if (character === '"') quoted = true;
      else if (character === "{") depth++;
      else if (character === "}" && --depth === 0) {
        const address = JSON.parse(prefix.slice(start, i + 1)) as CompletionRecipient;
        return typeof address.projectRoot === "string" && typeof address.cwd === "string" &&
          typeof address.rootId === "string" && typeof address.sessionId === "string" &&
          typeof address.name === "string" ? address : undefined;
      }
    }
  } catch { /* Missing, torn or oversized addresses cannot authorize body/fence access. */ }
  finally { await handle?.close(); }
  return undefined;
};
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
const readReplayFenceAsync = async <T>(file: string, label = "Completion"): Promise<T | undefined> => {
  try { return JSON.parse(await fs.promises.readFile(file, "utf8")) as T; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      try { await fs.promises.lstat(file); } catch (absence) {
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
const readReceiptAsync = async (file: string, id?: string): Promise<CompletionReceipt | undefined> => {
  const value = await readReplayFenceAsync<CompletionReceipt>(file);
  if (value === undefined) return undefined;
  if (!value || typeof value.id !== "string" || (id !== undefined && value.id !== id) ||
    path.basename(file) !== `${key(value.id)}.json` || typeof value.sessionId !== "string" || !value.sessionId ||
    typeof value.consumedAt !== "number" || !Number.isFinite(value.consumedAt) || value.consumedAt <= 0) {
    throw new Error(`Completion replay fence is invalid at ${file}; retain the outcome for repair`);
  }
  return value;
};
const readReceipt = (file: string, id?: string): CompletionReceipt | undefined => {
  const value = readReplayFence<CompletionReceipt>(file);
  if (value === undefined) return undefined;
  if (!value || typeof value.id !== "string" || (id !== undefined && value.id !== id) ||
    path.basename(file) !== `${key(value.id)}.json` || typeof value.sessionId !== "string" || !value.sessionId ||
    typeof value.consumedAt !== "number" || !Number.isFinite(value.consumedAt) || value.consumedAt <= 0) {
    throw new Error(`Completion replay fence is invalid at ${file}; retain the outcome for repair`);
  }
  return value;
};
export const completionConsumed = (meshRoot: string, id: string): boolean =>
  readReceipt(receiptPath(meshRoot, id), id) !== undefined;
const completionConsumedAsync = async (meshRoot: string, id: string): Promise<boolean> =>
  await readReceiptAsync(receiptPath(meshRoot, id), id) !== undefined;
export const consumeCompletion = (meshRoot: string, id: string, sessionId: string): void => {
  const file = receiptPath(meshRoot, id);
  const receipt = readReceipt(file, id);
  if (receipt) syncCompletionFile(file, receipt);
  else writeJsonAtomic(file, { id, sessionId, consumedAt: Date.now() }, { durable: true });
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
  } else {
    // Candidate removal must not rely on a visible but failed receipt write.
    consumeCompletion(meshRoot, result.id, recipient.sessionId);
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
const promoteOrphanAsync = async (meshRoot: string, projectRoot: string, project: string, target: string,
  accepts: (recipient: CompletionRecipient) => Promise<boolean>): Promise<void> => {
  const file = path.basename(target);
  const address = await readRecipientAsync(target);
  if (!address || await canonicalAsync(address.projectRoot) !== project || !await accepts(address)) return;
  const fence = path.join(directory(meshRoot), "receipts", file);
  if (await readReceiptAsync(fence)) return;
  const candidate = await readAsync<CompletionCandidate>(target);
  if (candidate?.format !== 1 || !candidate.result || !candidate.recipient || !candidate.supervisor ||
    !Number.isSafeInteger(candidate.supervisor.pid) || candidate.supervisor.pid <= 0 ||
    typeof candidate.result.id !== "string" || file !== `${key(candidate.result.id)}.json` ||
    typeof candidate.recipient.projectRoot !== "string" || await canonicalAsync(candidate.recipient.projectRoot) !== await canonicalAsync(projectRoot)) return;
  if (!await completionConsumedAsync(meshRoot, candidate.result.id) &&
    !residentProcessAlive(candidate.supervisor.pid, candidate.supervisor.processStartedAt)) {
    saveCompletion(meshRoot, candidate.recipient, candidate.result);
  }
};
const promoteOrphans = (meshRoot: string, projectRoot: string,
  accepts: (recipient: CompletionRecipient) => boolean = () => true): void => {
  const project = canonical(projectRoot);
  for (const file of files(path.join(directory(meshRoot), "attempts"))) {
    const address = readRecipient(path.join(directory(meshRoot), "attempts", file));
    if (address && canonical(address.projectRoot) === project && accepts(address)) {
      // Synchronous public queries retain their established behavior.
      promoteOrphanSync(meshRoot, projectRoot, project, path.join(directory(meshRoot), "attempts", file), accepts);
    }
  }
};
const promoteOrphanSync = (meshRoot: string, projectRoot: string, project: string, target: string,
  accepts: (recipient: CompletionRecipient) => boolean): void => {
  const file = path.basename(target);
  const address = readRecipient(target);
  if (!address || canonical(address.projectRoot) !== project || !accepts(address)) return;
  const fence = path.join(directory(meshRoot), "receipts", file);
  if (readReceipt(fence)) return;
  const candidate = read<CompletionCandidate>(target);
  if (candidate?.format !== 1 || !candidate.result || !candidate.recipient || !candidate.supervisor ||
    !Number.isSafeInteger(candidate.supervisor.pid) || candidate.supervisor.pid <= 0 ||
    typeof candidate.result.id !== "string" || file !== `${key(candidate.result.id)}.json` ||
    typeof candidate.recipient.projectRoot !== "string" || canonical(candidate.recipient.projectRoot) !== canonical(projectRoot)) return;
  if (!completionConsumed(meshRoot, candidate.result.id) && !residentProcessAlive(candidate.supervisor.pid, candidate.supervisor.processStartedAt)) {
    saveCompletion(meshRoot, candidate.recipient, candidate.result);
  }
};
const savedCompletion = (meshRoot: string, projectRoot: string, id: string): CompletionEnvelope | undefined => {
  promoteOrphans(meshRoot, projectRoot);
  const file = envelopePath(meshRoot, id);
  const recipient = readRecipient(file);
  if (!recipient || canonical(recipient.projectRoot) !== canonical(projectRoot)) return undefined;
  const value = read<CompletionEnvelope>(file);
  if (value?.format !== 1 || value.result?.id !== id || typeof value.recipient?.projectRoot !== "string" ||
    canonical(value.recipient.projectRoot) !== canonical(projectRoot)) return undefined;
  return value;
};
const legacyCompletionConsumedAsync = async (meshRoot: string, rootId: string, id: string): Promise<boolean> => {
  const file = path.join(meshRoot, "residency", key(rootId), "agents", `${id}.json`);
  const metadata = await readReplayFenceAsync<{ rootId?: string; id?: string; completionConsumedAt?: number }>(file, "Legacy completion");
  if (metadata === undefined) return false;
  if (!metadata || metadata.rootId !== rootId || metadata.id !== id ||
    (metadata.completionConsumedAt !== undefined && (typeof metadata.completionConsumedAt !== "number" ||
      !Number.isFinite(metadata.completionConsumedAt) || metadata.completionConsumedAt <= 0))) {
    throw new Error(`Legacy completion replay fence is invalid at ${file}; retain the outcome for repair`);
  }
  return metadata.completionConsumedAt !== undefined;
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
const pendingCompletionAsync = async (meshRoot: string, projectRoot: string, project: string, target: string,
  accepts: (recipient: CompletionRecipient) => Promise<boolean>, consumed?: ReadonlySet<string>): Promise<CompletionEnvelope[]> => {
  const file = path.basename(target);
  if (consumed?.has(file)) return [];
  const recipient = await readRecipientAsync(target);
  if (!recipient || await canonicalAsync(recipient.projectRoot) !== project || !await accepts(recipient)) return [];
  if (await readReceiptAsync(path.join(directory(meshRoot), "receipts", file))) return [];
  const value = await readAsync<CompletionEnvelope>(target);
  if (value?.format !== 1 || !value.recipient || !value.result ||
    typeof value.recipient.rootId !== "string" || typeof value.recipient.sessionId !== "string" ||
    typeof value.recipient.projectRoot !== "string" || typeof value.recipient.cwd !== "string" ||
    typeof value.recipient.name !== "string" || typeof value.recipient.startedAt !== "number" ||
    (value.recipient.role !== undefined && typeof value.recipient.role !== "string") ||
    typeof value.result.id !== "string" || !/^[a-f0-9]{32}$/.test(value.result.id) ||
    typeof value.result.name !== "string" || typeof value.result.text !== "string" || typeof value.result.startedAt !== "number" ||
    file !== `${key(value.result.id)}.json` ||
    !["completed", "failed", "stopped", "timed_out"].includes(value.result.status) ||
    await canonicalAsync(value.recipient.projectRoot) !== await canonicalAsync(projectRoot) ||
    await completionConsumedAsync(meshRoot, value.result.id)) return [];
  return await legacyCompletionConsumedAsync(meshRoot, value.recipient.rootId, value.result.id) ? [] : [value];
};
const pendingCompletion = (meshRoot: string, projectRoot: string, project: string, target: string,
  accepts: (recipient: CompletionRecipient) => boolean, consumed?: ReadonlySet<string>): CompletionEnvelope[] => {
  const file = path.basename(target);
  if (consumed?.has(file)) return [];
  const recipient = readRecipient(target);
  if (!recipient || canonical(recipient.projectRoot) !== project || !accepts(recipient)) return [];
  if (readReceipt(path.join(directory(meshRoot), "receipts", file))) return [];
  const value = read<CompletionEnvelope>(target);
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
};
export const pendingCompletions = (meshRoot: string, projectRoot: string,
  accepts: (recipient: CompletionRecipient) => boolean = () => true): CompletionEnvelope[] =>
  pendingCompletionsExcept(meshRoot, projectRoot, accepts);
const pendingCompletionsExcept = (meshRoot: string, projectRoot: string,
  accepts: (recipient: CompletionRecipient) => boolean = () => true, consumed?: ReadonlySet<string>): CompletionEnvelope[] => {
  promoteOrphans(meshRoot, projectRoot, accepts);
  const project = canonical(projectRoot);
  return files(directory(meshRoot)).flatMap(file => pendingCompletion(meshRoot, projectRoot, project, path.join(directory(meshRoot), file), accepts, consumed));
};
const scanPendingCompletions = async (meshRoot: string, projectRoot: string,
  accepts: (recipient: CompletionRecipient) => Promise<boolean>, consumed?: ReadonlySet<string>): Promise<CompletionEnvelope[]> => {
  const promotionProject = await canonicalAsync(projectRoot);
  for await (const target of scanTargets([path.join(directory(meshRoot), "attempts")])) {
    await promoteOrphanAsync(meshRoot, projectRoot, promotionProject, target, accepts);
  }
  const project = await canonicalAsync(projectRoot);
  const pending: CompletionEnvelope[] = [];
  for await (const target of scanTargets([directory(meshRoot)])) {
    pending.push(...await pendingCompletionAsync(meshRoot, projectRoot, project, target, accepts, consumed));
  }
  return pending;
};
export const pendingCompletionResult = (envelope: CompletionEnvelope): AgentRunResult => ({
  ...envelope.result,
  completionDelivery: { status: "undelivered", addressedTo: envelope.recipient.sessionId },
});

export class CompletionJournal {
  readonly #enqueued = new Set<string>();
  // A monotonic local suppression fence, not a cached durability confirmation.
  // Destructive claim retirement continues to reopen/sync its exact receipt.
  readonly #suppressed = new Set<string>();
  #rememberConsumed(id: string): void {
    this.#suppressed.add(`${key(id)}.json`);
    if (this.#suppressed.size > 1024) this.#suppressed.delete(this.#suppressed.values().next().value!);
  }

  // Explicit cleanup is logically final for this client even while durable retirement
  // is delayed/refused. Keep the receipt and envelope for crash-safe drain retries.
  readonly #forgotten = new Set<string>();
  // Preserve same-session wait/status after unlink without retaining an unbounded disk journal.
  readonly #consumed = new Map<string, CompletionEnvelope>();
  #remember(envelope: CompletionEnvelope): void {
    if (this.#forgotten.has(envelope.result.id)) return;
    this.#consumed.set(envelope.result.id, envelope);
    if (this.#consumed.size > 64) this.#consumed.delete(this.#consumed.keys().next().value!);
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
    this.#forgotten.add(id);
    this.#rememberConsumed(id);
    this.#enqueued.delete(id);
    this.#consumed.delete(id);
    void this.#retireClaim(id).catch(() => undefined); // A crash/failure is reconciled by drain.
  }
  pending(): CompletionEnvelope[] { return pendingCompletionsExcept(this.meshRoot, this.recipient.projectRoot, undefined, this.#suppressed); }
  result(id: string): AgentRunResult | CompletionSummary | undefined {
    if (this.#forgotten.has(id)) return undefined;
    const envelope = savedCompletion(this.meshRoot, this.recipient.projectRoot, id) ?? this.#consumed.get(id);
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
    if (envelope) this.#remember(envelope);
    this.#rememberConsumed(id);
    this.#enqueued.delete(id);
    void this.#retireClaim(id).catch(() => undefined);
    return true;
  }
  async drain(deliver = true): Promise<void> {
    const recipient = this.recipient; // Keep live Main renames visible even during an empty poll.
    const claims = this.mesh.listAll(claimPrefix);
    // An absent journal has no bodies/attempts to inspect. Do not issue directory
    // scans or async canonicalization on the ordinary empty-root idle path.
    // Existing journals and bodyless claims still use bounded asynchronous scans.
    if (!claims.length && !fs.existsSync(directory(this.meshRoot))) return;
    // Gate before reading a fence, even without a body. The authenticated claim retains
    // its owner's lane so a dead Main's same-lane successor can reclaim bounded state.
    let retired = 0;
    for await (const claim of scanSlices(claims)) {
      if (!await this.#canRetireClaimAsync(claim)) continue;
      const receipt = await readReceiptAsync(path.join(directory(this.meshRoot), "receipts", `${claim.key.slice(claimPrefix.length)}.json`));
      if (typeof receipt?.id === "string" && claim.key === claimKey(receipt.id)) {
        // A failed CAS/delete must leave its evidence for the next pass, not retry
        // a replacement version through the envelope-pruning loop below.
        if (!await this.#retireClaim(receipt.id, claim)) return;
        if (++retired === 128) break;
      }
    }
    const accepts = async (address: CompletionRecipient): Promise<boolean> =>
      (address.rootId === recipient.rootId && address.sessionId === recipient.sessionId) || await sameRecipientLaneAsync(address, recipient);
    // Bound crash-left cleanup; receipt barriers use the async filesystem, never the UI thread.
    let pruned = 0;
    const project = await canonicalAsync(recipient.projectRoot);
    for await (const target of scanTargets([directory(this.meshRoot), path.join(directory(this.meshRoot), "attempts")])) {
      const file = path.basename(target);
      const address = await readRecipientAsync(target);
      if (!address || await canonicalAsync(address.projectRoot) !== project || !await accepts(address)) continue;
      const fence = path.join(directory(this.meshRoot), "receipts", file);
      const receipt = await readReceiptAsync(fence);
      if (!receipt) continue;
      // A legacy claim has no address of its own: the envelope is its last lane
      // evidence. Do not unlink that evidence while an ineligible predecessor
      // still owns the claim; otherwise no later Main can authorize retirement.
      const claim = this.mesh.get(claimKey(receipt.id), { fresh: true });
      if (claim) {
        if (!await this.#canRetireClaimAsync(claim)) continue;
        await this.#retireClaim(receipt.id, claim);
        if (this.mesh.get(claimKey(receipt.id), { fresh: true })) continue;
      } else {
        await confirmReceipt(fence, receipt);
        fs.rmSync(target, { force: true });
      }
      if (++pruned === 128) break;
    }
    // The inbox already owns enqueued notices until its durable carrier confirms
    // them. Avoid reopening their legacy metadata during every idle pass: on
    // Windows that reader can contend with a synchronous acknowledgment rename.
    // Receipt confirmation/claim cleanup above is deliberately NOT suppressed.
    const suppressed = new Set(this.#suppressed);
    for (const id of this.#enqueued) suppressed.add(`${key(id)}.json`);
    const pending = await scanPendingCompletions(this.meshRoot, recipient.projectRoot, accepts, suppressed);
    if (!pending.length) return;
    const roots = this.participants.list({ scope: "project", kinds: ["root"], fresh: true });
    for await (const envelope of scanSlices(pending)) {
      if (this.#enqueued.has(envelope.result.id) || !await this.#canDeliverAsync(envelope, roots)) continue;
      const ck = claimKey(envelope.result.id);
      const claim = this.mesh.get(ck, { fresh: true });
      const owner = (claim?.value as { rootId?: string } | undefined)?.rootId;
      if (owner && owner !== this.recipient.rootId && roots.some(root => root.id === owner)) continue;
      if (owner !== this.recipient.rootId) {
        try {
          await this.mesh.put({ key: ck, ifVersion: claim?.version ?? 0,
            identity: { id: this.recipient.rootId, name: "main", kind: "main" },
            value: { rootId: this.recipient.rootId, sessionId: this.recipient.sessionId, recipient: this.recipient } satisfies CompletionClaim });
        } catch { continue; } // Another live successor owns admission; leave the source pending.
      }
      if (await completionConsumedAsync(this.meshRoot, envelope.result.id)) { await this.#retireClaim(envelope.result.id); continue; }
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
            this.#remember(envelope);
            this.#rememberConsumed(envelope.result.id);
            void this.#retireClaim(envelope.result.id).catch(() => undefined);
          } finally { this.#enqueued.delete(envelope.result.id); }
        });
      } catch { this.#enqueued.delete(envelope.result.id); } // Source stays pending if admission failed.
    }
  }
  async #canRetireClaimAsync(snapshot: NonNullable<ReturnType<MeshStore["get"]>>): Promise<boolean> {
    const owner = snapshot.value as Partial<CompletionClaim> | undefined;
    if (typeof owner?.rootId !== "string" || typeof owner.sessionId !== "string" || snapshot.updatedBy.id !== owner.rootId) return false;
    const recipient = this.recipient;
    if (owner.rootId === recipient.rootId && owner.sessionId === recipient.sessionId) return true;
    // Older claims can use the bounded envelope address; new claims retain this
    // address themselves, including after unlink. Never infer a lane from a session id.
    const address = owner.recipient ?? await readRecipientAsync(path.join(directory(this.meshRoot), `${snapshot.key.slice(claimPrefix.length)}.json`));
    if (!address ||
      (owner.recipient && (address.rootId !== owner.rootId || address.sessionId !== owner.sessionId)) ||
      typeof address.cwd !== "string" || typeof address.projectRoot !== "string" || typeof address.name !== "string" ||
      typeof address.startedAt !== "number" || !Number.isFinite(address.startedAt) ||
      (address.role !== undefined && typeof address.role !== "string") || !await sameRecipientLaneAsync(address, recipient)) return false;
    const successor = await completionSuccessorAsync(address, this.participants.list({ scope: "project", kinds: ["root"], fresh: true }));
    return successor?.id === recipient.rootId && successor.sessionId === recipient.sessionId && await sameLaneAsync(recipient, successor);
  }
  async #retireClaim(id: string, snapshot = this.mesh.get(claimKey(id), { fresh: true })): Promise<boolean> {
    if (snapshot && !await this.#canRetireClaimAsync(snapshot)) return false;
    // The versioned delete cannot erase a replacement owner/version.
    const file = receiptPath(this.meshRoot, id);
    const receipt = await readReceiptAsync(file, id);
    if (!receipt) return false;
    // One fresh confirmation authorizes this consumed outcome's cleanup batch.
    // Do not retire its claim and then owe a second barrier before body unlink:
    // that barrier could fail after the claim was already lost.
    const recipient = this.recipient;
    await confirmReceipt(file, receipt);
    const targets: string[] = [];
    for (const target of [envelopePath(this.meshRoot, id), candidatePath(this.meshRoot, id)]) {
      const address = await readRecipientAsync(target);
      if (address && await canonicalAsync(address.projectRoot) === await canonicalAsync(recipient.projectRoot) &&
        ((address.rootId === recipient.rootId && address.sessionId === recipient.sessionId) || await sameRecipientLaneAsync(address, recipient))) targets.push(target);
    }
    // The envelope is the last lane evidence for legacy claims. Keep it until the
    // versioned deletion commits; a failed/interrupted delete is retried by drain.
    if (snapshot) {
      try { await this.mesh.delete({ key: snapshot.key, ifVersion: snapshot.version }); }
      catch { return false; }
    }
    for (const target of targets) fs.rmSync(target, { force: true });
    return true;
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
  async #canDeliverAsync(envelope: CompletionEnvelope, roots?: FabricParticipantInfo[]): Promise<boolean> {
    if (envelope.recipient.rootId === this.recipient.rootId && envelope.recipient.sessionId === this.recipient.sessionId) return true;
    if (!await sameRecipientLaneAsync(envelope.recipient, this.recipient)) return false;
    return (await completionSuccessorAsync(envelope.recipient, roots ??
      this.participants.list({ scope: "project", kinds: ["root"], fresh: true })))?.id === this.recipient.rootId;
  }
}
