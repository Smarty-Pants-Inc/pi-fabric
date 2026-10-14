import fs from "node:fs";
import path from "node:path";
import { readFinalAnswerReceipt, readTaskControlJson, readTaskControlJsonAt, readTaskRunStatus, writeTaskControlJson, removeTaskControlFile, openTaskControlReadFile, openLegacyTaskControlFiles, taskControlFileStat, MAX_TASK_CONTROL_ROWS, taskControlDirectory, withTaskControlDirectory, withTaskControlReadDirectory, visitLegacyTaskControlDirectories, withTaskControlSubdirectory, initializeTaskControlDirectory } from "../worker/terminal-answer.js";
import { FabricSteerAdmissionLimitError, type AgentTerminalNotice, type AgentUndeliveredMessage } from "./terminal-target.js";
import { followUpFile, releaseFollowUpPayload, settleFollowUp } from "./follow-up-delivery.js";
import { copyFabricProvenance, type FabricTurnProvenance } from "../fabric-provenance.js";
import type { AgentRunRecord } from "./types.js";

export interface TerminalAdmission {
  id: string;
  delivery: "steer" | "followUp";
  sender?: FabricTurnProvenance["sender"];
}

/** Malformed provenance is not an anonymous admission. */
export const terminalAdmissionFromEntry = (value: unknown): TerminalAdmission | undefined => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return;
  const entry = value as { id?: unknown; type?: unknown; provenance?: unknown };
  if (typeof entry.id !== "string" || !/^[a-zA-Z0-9_-]{1,200}$/.test(entry.id) ||
      (entry.type !== "steer" && entry.type !== "follow_up")) return;
  const provenance = copyFabricProvenance(entry.provenance);
  if (entry.provenance !== undefined && !provenance) return;
  return { id: entry.id, delivery: entry.type === "steer" ? "steer" : "followUp",
    ...(provenance ? { sender: provenance.sender } : {}) };
};

/** Compare copied identities, including normalized remote kind and optional name. */
export const terminalSenderMatches = (value: unknown, sender: TerminalAdmission["sender"]): boolean => {
  if (value === undefined) return sender === undefined;
  const copied = copyFabricProvenance({ v: 1, channel: "fabric", via: "steer", sender: value })?.sender;
  return !!copied && !!sender && copied.id === sender.id && copied.kind === sender.kind &&
    copied.verified === sender.verified && copied.name === sender.name;
};

/** A worker row proves consumption only for this exact original admission. */
export const terminalControlMatchesAdmission = (value: unknown, admission: TerminalAdmission | undefined): boolean => {
  if (!admission || !value || typeof value !== "object" || Array.isArray(value)) return false;
  const control = value as { id?: unknown; delivery?: unknown; provenance?: unknown };
  if (control.id !== admission.id || control.delivery !== admission.delivery) return false;
  const provenance = copyFabricProvenance(control.provenance);
  if (control.provenance !== undefined && !provenance) return false;
  return terminalSenderMatches(provenance?.sender, admission.sender);
};

export const MAX_STEER_CONTROLS = MAX_TASK_CONTROL_ROWS;
export const MAX_STEER_BYTES = 1024 * 1024;
export const MAX_STEER_ENTRY_BYTES = 64 * 1024;
export const MAX_PUBLIC_REFUSALS = 64;
export interface TerminalAdmissionIndex {
  byId: Map<string, TerminalAdmission>;
  controls: number;
  bytes: number;
  truncated: boolean;
}
// Bounded cache, invalidated by the journal's exact native identity/version.
// One streaming pass is reused by scan, recovery, routing and receipt views.
const admissionCache = new Map<string, { signature: string; index: TerminalAdmissionIndex }>();
const signature = (s: fs.BigIntStats): string => `${s.dev}:${s.ino}:${s.size}:${s.mtimeNs}:${s.ctimeNs}`;
export class FabricSteerJournalError extends Error {
  override readonly name = "FabricSteerJournalError";
  readonly code = "FABRIC_STEER_JOURNAL_INVALID";
}
const requireJournal = (fd: number): fs.BigIntStats => {
  try { return taskControlFileStat(fd); }
  catch { throw new FabricSteerJournalError("Steering journal must be an owned regular file with one link"); }
};
export const appendTerminalAdmission = (directory: string, line: string): void => withTaskControlDirectory(directory, (control, directoryFd) => {
  // Recheck at the actual write too: a host downgrade hook may re-enter admission.
  const index = readTerminalAdmissions(directory), bytes = Buffer.byteLength(line);
  if (index.truncated || index.controls >= MAX_STEER_CONTROLS || bytes > MAX_STEER_ENTRY_BYTES || index.bytes + bytes > MAX_STEER_BYTES) {
    throw new FabricSteerAdmissionLimitError(path.basename(directory), MAX_STEER_CONTROLS, MAX_STEER_BYTES);
  }
  const file = path.join(control, "steer.jsonl"), created = !fs.existsSync(file);
  const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT |
    (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0), 0o600);
  try {
    requireJournal(fd); fs.appendFileSync(fd, line, "utf8"); fs.fsyncSync(fd);
    if (created && process.platform !== "win32") fs.fsyncSync(directoryFd);
  }
  finally { fs.closeSync(fd); }
});
const cacheIndex = (directory: string, version: string, index: TerminalAdmissionIndex): void => {
  admissionCache.delete(directory);
  if (admissionCache.size >= 64) admissionCache.delete(admissionCache.keys().next().value!);
  admissionCache.set(directory, { signature: version, index });
};
const emptyAdmissions = (): TerminalAdmissionIndex => ({ byId: new Map(), controls: 0, bytes: 0, truncated: false });
const readAdmissionJournal = (fd: number, directory: string): TerminalAdmissionIndex => {
  try {
    const stat = requireJournal(fd), version = signature(stat);
    const cacheKey = `${directory}:${stat.dev}:${stat.ino}`;
    const cached = admissionCache.get(cacheKey);
    if (cached?.signature === version) return cached.index;
    const index = emptyAdmissions();
    index.bytes = Number(stat.size);
    index.truncated = index.bytes > MAX_STEER_BYTES;
    const buffer = Buffer.alloc(16 * 1024);
    let offset = 0, remainder = Buffer.alloc(0), skipping = false;
    while (offset < Math.min(index.bytes, MAX_STEER_BYTES)) {
      const length = fs.readSync(fd, buffer, 0, Math.min(buffer.length, MAX_STEER_BYTES - offset), offset);
      if (!length) break;
      offset += length;
      let chunk = Buffer.concat([remainder, buffer.subarray(0, length)]);
      remainder = Buffer.alloc(0);
      if (skipping) {
        const end = chunk.indexOf(10);
        if (end < 0) continue;
        chunk = chunk.subarray(end + 1); skipping = false;
      }
      let start = 0, end: number;
      while ((end = chunk.indexOf(10, start)) >= 0) {
        const line = chunk.subarray(start, end); start = end + 1;
        if (!line.length) continue;
        index.controls++;
        if (line.length > MAX_STEER_ENTRY_BYTES || index.controls > MAX_STEER_CONTROLS) { index.truncated = true; continue; }
        try {
          const admission = terminalAdmissionFromEntry(JSON.parse(line.toString("utf8")));
          if (admission && !index.byId.has(admission.id)) index.byId.set(admission.id, admission);
        } catch { /* A malformed row never grants sender authority. */ }
      }
      remainder = Buffer.from(chunk.subarray(start));
      if (remainder.length > MAX_STEER_ENTRY_BYTES) { remainder = Buffer.alloc(0); skipping = true; index.truncated = true; }
    }
    if (remainder.length || skipping) index.truncated = true; // incomplete/oversized ingress fails closed
    // Never cache a pass across a concurrent append/replacement.
    if (signature(requireJournal(fd)) === version) cacheIndex(cacheKey, version, index);
    return index;
  } finally { fs.closeSync(fd); }
};
export const readTerminalAdmissions = (directory: string): TerminalAdmissionIndex => {
  try {
    const journals: TerminalAdmissionIndex[] = [], seen = new Set<string>();
    const read = (fd: number | undefined): void => {
      if (fd === undefined) return;
      let stat: fs.BigIntStats;
      try { stat = requireJournal(fd); } catch (error) { fs.closeSync(fd); throw error; }
      const identity = `${stat.dev}:${stat.ino}`;
      if (seen.has(identity)) { fs.closeSync(fd); return; }
      seen.add(identity); journals.push(readAdmissionJournal(fd, directory));
    };
    // Original admissions precede newer sibling ingress after a legacy restart.
    // Oldest layer first (in-run, then old sibling): every legacy journal is kept.
    let legacyFds: number[] = [];
    try { legacyFds = openLegacyTaskControlFiles(directory, "steer.jsonl").reverse(); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    for (let index = 0; index < legacyFds.length; index++) {
      try { read(legacyFds[index]); }
      catch (error) { for (const fd of legacyFds.slice(index + 1)) fs.closeSync(fd); throw error; }
    }
    try { read(openTaskControlReadFile(directory, "steer.jsonl")); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    const index = emptyAdmissions();
    for (const journal of journals) {
      index.bytes += journal.bytes; index.controls += journal.controls; index.truncated ||= journal.truncated;
      for (const [id, admission] of journal.byId) if (!index.byId.has(id)) index.byId.set(id, admission);
    }
    index.truncated ||= index.bytes > MAX_STEER_BYTES || index.controls > MAX_STEER_CONTROLS;
    return index;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return emptyAdmissions();
    const refused = error instanceof FabricSteerJournalError ? error : new FabricSteerJournalError(`Cannot safely open steering journal: ${String(error)}`);
    console.warn(`[pi-fabric] ${refused.code}: ${refused.message}`);
    throw refused;
  }
};
export const readTerminalAdmission = (directory: string, id: string): TerminalAdmission | undefined => readTerminalAdmissions(directory).byId.get(id);

const retainedNotice = (directory: string, admission: TerminalAdmission, receiptId: string): AgentTerminalNotice | undefined => {
  for (const suffix of [".json.delivered", ".json"]) {
    try {
      const notice = readTaskControlJson<AgentTerminalNotice>(directory, path.join("terminal-notices", admission.id + suffix));
      if (notice?.code === "FABRIC_TARGET_TERMINAL" && notice.targetId === path.basename(directory) && notice.finalAnswerReceiptId === receiptId &&
          notice.messageId === admission.id && notice.delivery === admission.delivery && terminalSenderMatches(notice.sender, admission.sender)) return notice;
    } catch { /* An uncertain notice never establishes a refusal. */ }
  }
  return undefined;
};
export const readUndeliveredMessage = (directory: string, id: string): AgentUndeliveredMessage | undefined => {
  const receipt = readFinalAnswerReceipt(directory, path.basename(directory));
  const admission = readTerminalAdmission(directory, id);
  if (!receipt || !admission || !retainedNotice(directory, admission, receipt.id)) return;
  return { code: "FABRIC_TARGET_TERMINAL", reason: "run-ended", messageId: id, delivery: admission.delivery,
    finalAnswerReceiptId: receipt.id, ...(admission.sender ? { sender: admission.sender } : {}) };
};
export const readUndeliveredMessageView = (directory: string): { messages: AgentUndeliveredMessage[]; total: number; truncated: boolean } => {
  const receipt = readFinalAnswerReceipt(directory, path.basename(directory));
  const index = readTerminalAdmissions(directory);
  const result = { messages: [] as AgentUndeliveredMessage[], total: 0, truncated: index.truncated };
  if (!receipt) return result;
  for (const admission of index.byId.values()) {
    if (!retainedNotice(directory, admission, receipt.id)) continue;
    result.total++;
    if (result.messages.length < MAX_PUBLIC_REFUSALS) result.messages.push({ code: "FABRIC_TARGET_TERMINAL", reason: "run-ended", messageId: admission.id,
      delivery: admission.delivery, finalAnswerReceiptId: receipt.id, ...(admission.sender ? { sender: admission.sender } : {}) });
  }
  result.truncated ||= result.total > result.messages.length;
  return result;
};
export const readUndeliveredMessages = (directory: string): AgentUndeliveredMessage[] => readUndeliveredMessageView(directory).messages;

/** Durable sender outbox. Failed routes retry on ordinary status/recovery/sweep wakes. */
export class TerminalNoticeOutbox {
  readonly #routing = new Set<string>();
  constructor(readonly deliver?: (notice: AgentTerminalNotice) => void | Promise<void>) {}

  retain(directory: string, notice: AgentTerminalNotice, route = true): void {
    if (!/^[a-zA-Z0-9_-]{1,200}$/.test(notice.messageId)) throw new Error("Invalid terminal notice identity");
    const relative = path.join("terminal-notices", `${notice.messageId}.json`);
    if (readTaskControlJson(directory, relative + ".delivered")) return;
    initializeTaskControlDirectory(directory);
    writeTaskControlJson(directory, relative, notice);
    if (notice.delivery === "followUp" && /^[0-9a-f-]{36}$/.test(notice.messageId)) {
      const admission = followUpFile(directory, notice.messageId);
      if (fs.existsSync(admission)) {
        settleFollowUp(admission, "cancelled");
        releaseFollowUpPayload(admission);
      }
    }
    if (route) this.route(directory);
  }

  /** Reconstruct a missing outbox after a host crash between refusal and scan. */
  recover(directory: string): void {
    try {
      const receipt = readFinalAnswerReceipt(directory, path.basename(directory));
      if (!receipt) return;
      const status = readTaskRunStatus<AgentRunRecord>(directory);
      if (status.id !== receipt.runId || status.actorId || status.runner !== "pi" || status.finalAnswerReceipt?.id !== receipt.id ||
          !["completed", "failed", "stopped", "timed_out"].includes(status.status)) return;
      const index = readTerminalAdmissions(directory);
      for (const admission of index.byId.values()) {
        let state: { state?: string } | undefined;
        try { state = readTaskControlJson(directory, path.join("terminal-controls", `${admission.id}.json`)); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
        if (state?.state === "delivered" && terminalControlMatchesAdmission(state, admission)) continue;
        this.retain(directory, { code: "FABRIC_TARGET_TERMINAL", targetId: receipt.runId,
          finalAnswerReceiptId: receipt.id, messageId: admission.id, delivery: admission.delivery,
          ...(admission.sender ? { sender: admission.sender } : {}) }, false);
      }
      this.route(directory);
    } catch { /* Unsafe metadata retains custody; never guess attribution or crash maintenance. */ }
  }

  hasPending(directory: string): boolean {
    let count = 0, legacySelected = false;
    const scan = (pinned: string): boolean => {
      try { return withTaskControlSubdirectory(pinned, "terminal-notices", parent => {
        const entries = fs.opendirSync(parent);
        try {
          for (;;) {
            const entry = entries.readSync();
            if (!entry) return false;
            if (++count > MAX_STEER_CONTROLS * 2) return true;
            if (!entry.isFile()) return true;
            if (entry.name.endsWith(".json.delivered")) continue;
            if (!/^[a-zA-Z0-9_-]{1,200}\.json$/.test(entry.name)) return true;
            const relative = path.join("terminal-notices", entry.name);
            const notice = readTaskControlJsonAt(pinned, relative);
            const ack = readTaskControlJson(directory, relative + ".delivered");
            if (!notice || JSON.stringify(ack) !== JSON.stringify(notice)) return true;
          }
        } finally { entries.closeSync(); }
      }); } catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
    };
    try {
      const pending = withTaskControlReadDirectory(directory, pinned => { legacySelected = fs.realpathSync(pinned) === path.resolve(directory); return scan(pinned); });
      return pending || (!legacySelected && visitLegacyTaskControlDirectories(directory, scan).some(Boolean));
    } catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  }

  route(directory: string): void {
    if (!this.deliver) return;
    try {
      const outbox = path.join(taskControlDirectory(directory), "terminal-notices");
      const receipt = readFinalAnswerReceipt(directory, path.basename(directory));
      if (!receipt) return;
      const index = readTerminalAdmissions(directory);
      for (const admission of index.byId.values()) {
        const entry = admission.id + ".json";
        const file = path.join(outbox, entry);
        if (this.#routing.has(file)) continue;
        let notice: AgentTerminalNotice | undefined;
        try { notice = readTaskControlJson(directory, path.join("terminal-notices", entry)); } catch { continue; }
        if (!notice || notice.code !== "FABRIC_TARGET_TERMINAL" || notice.targetId !== receipt.runId || notice.finalAnswerReceiptId !== receipt.id ||
            entry !== `${notice.messageId}.json` || admission?.delivery !== notice.delivery || !admission.sender ||
            !terminalSenderMatches(notice.sender, admission.sender)) continue;
        try {
          const acknowledged = readTaskControlJson(directory, path.join("terminal-notices", entry + ".delivered"));
          if (acknowledged !== undefined) {
            if (JSON.stringify(acknowledged) === JSON.stringify(notice)) removeTaskControlFile(directory, path.join("terminal-notices", entry));
            continue; // an uncertain ACK retains custody, never repeats delivery
          }
        } catch { continue; }
        initializeTaskControlDirectory(directory);
        const identity = withTaskControlDirectory(directory, (_pinned, fd) => {
          const stat = fs.fstatSync(fd, { bigint: true }); return `${stat.dev}:${stat.ino}`;
        });
        this.#routing.add(file); // stable key only; never a retained /proc/self/fd path
        void Promise.resolve().then(() => this.deliver!(notice)).then(() => {
          // Reopen after the async route, and require the original host namespace.
          withTaskControlDirectory(directory, (_pinned, fd) => {
            const stat = fs.fstatSync(fd, { bigint: true });
            if (`${stat.dev}:${stat.ino}` !== identity || readFinalAnswerReceipt(directory, receipt.runId)?.id !== receipt.id) throw new Error("Terminal outbox namespace changed during route");
            const current = readTaskControlJson(directory, path.join("terminal-notices", entry));
            if (JSON.stringify(current) !== JSON.stringify(notice)) throw new Error("Terminal notice changed during route");
            writeTaskControlJson(directory, path.join("terminal-notices", entry + ".delivered"), notice);
            removeTaskControlFile(directory, path.join("terminal-notices", entry));
            // Legacy sources stay read-only; matching sibling ACKs suppress replay.
          });
        }).catch(() => { /* Receipt remains durable; the next ordinary wake retries. */ })
          .finally(() => this.#routing.delete(file));
      }
    } catch { /* Unsafe metadata remains pending/vetoed, never an unhandled host exception. */ }
  }
}
