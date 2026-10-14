import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { readFinalAnswerReceipt, withTaskFinalAnswerLock } from "../worker/terminal-answer.js";
import type { AgentTerminalNotice } from "./terminal-target.js";
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

/** Refusal requires a native discard ACK or locked proof that no stdin bytes
 * were attempted. Native close, a queued row, or an event alone proves neither.
 * Legacy queued rows without transport evidence remain uncertain. */
export const terminalControlRefusalProven = (directory: string, admission: TerminalAdmission, runId: string): boolean =>
  withTaskFinalAnswerLock(directory, () => {
    if (!readFinalAnswerReceipt(directory, runId)) return false;
    const file = path.join(directory, "terminal-controls", `${admission.id}.json`);
    let control: { state?: string; forwarded?: unknown };
    try { control = JSON.parse(fs.readFileSync(file, "utf8")); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      // A dangling link is uncertainty, not an absent transport row.
      try { fs.lstatSync(file); return false; }
      catch (missing) { if ((missing as NodeJS.ErrnoException).code !== "ENOENT") throw missing; }
      try { if (!fs.lstatSync(path.dirname(file)).isDirectory()) return false; }
      catch (missing) { if ((missing as NodeJS.ErrnoException).code !== "ENOENT") throw missing; }
      return true;
    }
    if (!terminalControlMatchesAdmission(control, admission) ||
        Object.keys(control).some(key => !["id", "delivery", "state", "provenance", "forwarded"].includes(key)) ||
        control.forwarded !== undefined && typeof control.forwarded !== "boolean") return false;
    if (control.state === "refused") return true;
    if (control.state !== "queued" || control.forwarded !== false) return false;
    // The receipt blocks every later injection under this same lock.
    writeJsonAtomic(file, { ...control, state: "refused" }, { durable: true });
    return true;
  });

/** Manager-owned native ingress journal, not worker event data or final prose. */
export const readTerminalAdmission = (directory: string, id: string): TerminalAdmission | undefined => {
  try {
    for (const line of fs.readFileSync(path.join(directory, "steer.jsonl"), "utf8").split("\n")) {
      if (!line.trim()) continue;
      const entry = JSON.parse(line);
      if (entry?.id !== id || (entry.type !== "steer" && entry.type !== "follow_up")) continue;
      return terminalAdmissionFromEntry(entry);
    }
  } catch { /* Missing/malformed admission never grants a notification recipient. */ }
  return undefined;
};

/** Durable sender outbox. Failed routes retry on ordinary status/recovery/sweep wakes. */
export class TerminalNoticeOutbox {
  readonly #routing = new Set<string>();
  constructor(readonly deliver?: (notice: AgentTerminalNotice) => void | Promise<void>) {}

  retain(directory: string, notice: AgentTerminalNotice): void {
    const admission = readTerminalAdmission(directory, notice.messageId);
    const receipt = readFinalAnswerReceipt(directory, notice.targetId);
    if (!admission || admission.delivery !== notice.delivery || !receipt || receipt.id !== notice.finalAnswerReceiptId ||
        !terminalSenderMatches(notice.sender, admission.sender) ||
        !terminalControlRefusalProven(directory, admission, receipt.runId)) return;
    const file = path.join(directory, "terminal-notices", `${notice.messageId}.json`);
    if (fs.existsSync(`${file}.delivered`)) return;
    writeJsonAtomic(file, notice, { durable: true });
    if (notice.delivery === "followUp" && /^[0-9a-f-]{36}$/.test(notice.messageId)) {
      const admission = followUpFile(directory, notice.messageId);
      if (fs.existsSync(admission)) {
        settleFollowUp(admission, "cancelled");
        releaseFollowUpPayload(admission);
      }
    }
    this.route(directory);
  }

  /** Reconstruct a missing outbox after a host crash between refusal and scan. */
  recover(directory: string): void {
    const receipt = readFinalAnswerReceipt(directory, path.basename(directory));
    if (!receipt) return;
    try {
      const status = JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));
      if (status.id !== receipt.runId || status.actorId || status.runner !== "pi" || status.finalAnswerReceipt?.id !== receipt.id ||
          !["completed", "failed", "stopped", "timed_out"].includes(status.status)) return;
      for (const line of fs.readFileSync(path.join(directory, "steer.jsonl"), "utf8").split("\n")) {
        if (!line.trim()) continue;
        const entry = JSON.parse(line);
        if ((entry.type !== "steer" && entry.type !== "follow_up") || typeof entry.id !== "string" || !/^[a-zA-Z0-9_-]{1,200}$/.test(entry.id)) continue;
        const admission = readTerminalAdmission(directory, entry.id);
        if (!admission) continue;
        if (!terminalControlRefusalProven(directory, admission, receipt.runId)) continue;
        this.retain(directory, { code: "FABRIC_TARGET_TERMINAL", targetId: receipt.runId,
          finalAnswerReceiptId: receipt.id, messageId: entry.id, delivery: admission.delivery,
          ...(admission.sender ? { sender: admission.sender } : {}) });
      }
    } catch { /* Native sources remain retained; never guess incomplete attribution. */ }
    this.route(directory);
  }

  hasPending(directory: string): boolean {
    try { return fs.readdirSync(path.join(directory, "terminal-notices")).some(name => !name.endsWith(".json.delivered")); }
    catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  }

  route(directory: string): void {
    if (!this.deliver) return;
    const outbox = path.join(directory, "terminal-notices");
    let entries: string[];
    try { entries = fs.readdirSync(outbox); } catch { return; }
    const receipt = readFinalAnswerReceipt(directory, path.basename(directory));
    if (!receipt) return;
    for (const entry of entries) {
      if (!/^[a-zA-Z0-9_-]{1,200}\.json$/.test(entry)) continue;
      const file = path.join(outbox, entry);
      if (this.#routing.has(file)) continue;
      let notice: AgentTerminalNotice;
      try { notice = JSON.parse(fs.readFileSync(file, "utf8")); } catch { continue; }
      const admission = readTerminalAdmission(directory, notice.messageId);
      if (notice.code !== "FABRIC_TARGET_TERMINAL" || notice.targetId !== receipt.runId || notice.finalAnswerReceiptId !== receipt.id ||
          entry !== `${notice.messageId}.json` || admission?.delivery !== notice.delivery || !admission.sender ||
          !terminalSenderMatches(notice.sender, admission.sender) ||
          !terminalControlRefusalProven(directory, admission, receipt.runId)) continue;
      if (fs.existsSync(`${file}.delivered`)) {
        try {
          const acknowledged = JSON.parse(fs.readFileSync(`${file}.delivered`, "utf8"));
          if (JSON.stringify(acknowledged) === JSON.stringify(notice)) fs.rmSync(file);
        } catch { /* Uncertain/corrupt route receipt retains custody, never repeats delivery. */ }
        continue;
      }
      this.#routing.add(file);
      void Promise.resolve().then(() => {
        // Recheck at actual dispatch, not only while enumerating the outbox.
        if (!terminalControlRefusalProven(directory, admission!, receipt.runId)) throw new Error("Terminal refusal is unproven");
        return this.deliver!(notice);
      }).then(() => {
        // A routed ACK is not sender consumption. Keep the native refusal and
        // its route receipt through retention, and suppress restart duplicates.
        writeJsonAtomic(`${file}.delivered`, notice, { durable: true });
        fs.rmSync(file);
      }).catch(() => { /* Receipt remains durable; the next ordinary wake retries. */ })
        .finally(() => this.#routing.delete(file));
    }
  }
}
