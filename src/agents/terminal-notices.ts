import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { readFinalAnswerReceipt, readTaskControlJson, openTaskControlFile, taskControlFileStat, MAX_TASK_CONTROL_ROWS, taskControlDirectory, withTaskControlDirectory } from "../worker/terminal-answer.js";
import { FabricSteerAdmissionLimitError, type AgentTerminalNotice, type AgentUndeliveredMessage } from "./terminal-target.js";
import { followUpFile, releaseFollowUpPayload, settleFollowUp } from "./follow-up-delivery.js";
import { copyFabricProvenance, type FabricTurnProvenance } from "../fabric-provenance.js";

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
export const readTerminalAdmissions = (directory: string): TerminalAdmissionIndex => {
  const empty = (): TerminalAdmissionIndex => ({ byId: new Map(), controls: 0, bytes: 0, truncated: false });
  try { return withTaskControlDirectory(directory, control => {
  const fd = openTaskControlFile(path.join(control, "steer.jsonl"));
  try {
    const stat = requireJournal(fd), version = signature(stat);
    const cached = admissionCache.get(directory);
    if (cached?.signature === version) return cached.index;
    const index = empty();
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
    if (signature(requireJournal(fd)) === version) cacheIndex(directory, version, index);
    return index;
  } finally { fs.closeSync(fd); }
  }); } catch (error) {
    admissionCache.delete(directory);
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return empty();
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
    const file = path.join(taskControlDirectory(directory), "terminal-notices", `${notice.messageId}.json`);
    if (fs.existsSync(`${file}.delivered`)) return;
    writeJsonAtomic(file, notice, { durable: true });
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
    const receipt = readFinalAnswerReceipt(directory, path.basename(directory));
    if (!receipt) return;
    try {
      const status = JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));
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
    } catch { /* Native sources remain retained; never guess incomplete attribution. */ }
    this.route(directory);
  }

  hasPending(directory: string): boolean {
    let entries: fs.Dir | undefined;
    try {
      entries = fs.opendirSync(path.join(taskControlDirectory(directory), "terminal-notices"));
      for (let count = 0; count <= MAX_STEER_CONTROLS * 2; count++) {
        const entry = entries.readSync();
        if (!entry) return false;
        if (!entry.name.endsWith(".json.delivered")) return true;
      }
      return true; // An oversized/uncertain outbox retains custody.
    } catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
    finally { entries?.closeSync(); }
  }

  route(directory: string): void {
    if (!this.deliver) return;
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
      if (fs.existsSync(`${file}.delivered`)) {
        try {
          const acknowledged = readTaskControlJson(directory, path.join("terminal-notices", entry + ".delivered"));
          if (JSON.stringify(acknowledged) === JSON.stringify(notice)) fs.rmSync(file);
        } catch { /* Uncertain/corrupt route receipt retains custody, never repeats delivery. */ }
        continue;
      }
      this.#routing.add(file);
      void Promise.resolve().then(() => this.deliver!(notice)).then(() => {
        // A routed ACK is not sender consumption. Keep the native refusal and
        // its route receipt through retention, and suppress restart duplicates.
        writeJsonAtomic(`${file}.delivered`, notice, { durable: true });
        fs.rmSync(file);
      }).catch(() => { /* Receipt remains durable; the next ordinary wake retries. */ })
        .finally(() => this.#routing.delete(file));
    }
  }
}
