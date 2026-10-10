import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { readFinalAnswerReceipt } from "../worker/terminal-answer.js";
import type { AgentTerminalNotice } from "./terminal-target.js";
import { followUpFile, releaseFollowUpPayload, settleFollowUp } from "./follow-up-delivery.js";
import { copyFabricProvenance, type FabricTurnProvenance } from "../fabric-provenance.js";

/** Manager-owned native ingress journal, not worker event data or final prose. */
export const readTerminalAdmission = (directory: string, id: string): { delivery: "steer" | "followUp"; sender?: FabricTurnProvenance["sender"] } | undefined => {
  try {
    for (const line of fs.readFileSync(path.join(directory, "steer.jsonl"), "utf8").split("\n")) {
      if (!line.trim()) continue;
      const entry = JSON.parse(line);
      if (entry.id !== id || (entry.type !== "steer" && entry.type !== "follow_up")) continue;
      const provenance = copyFabricProvenance(entry.provenance);
      return { delivery: entry.type === "steer" ? "steer" : "followUp", ...(provenance ? { sender: provenance.sender } : {}) };
    }
  } catch { /* Missing/malformed admission never grants a notification recipient. */ }
  return undefined;
};

/** Durable sender outbox. Failed routes retry on ordinary status/recovery/sweep wakes. */
export class TerminalNoticeOutbox {
  readonly #routing = new Set<string>();
  constructor(readonly deliver?: (notice: AgentTerminalNotice) => void | Promise<void>) {}

  retain(directory: string, notice: AgentTerminalNotice): void {
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
        let state: { state?: string } | undefined;
        try { state = JSON.parse(fs.readFileSync(path.join(directory, "terminal-controls", `${entry.id}.json`), "utf8")); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
        if (state?.state === "delivered") continue;
        const admission = readTerminalAdmission(directory, entry.id);
        if (!admission) continue;
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
          admission.sender.id !== notice.sender?.id || admission.sender.kind !== notice.sender.kind ||
          admission.sender.verified !== notice.sender.verified || admission.sender.name !== notice.sender.name) continue;
      if (fs.existsSync(`${file}.delivered`)) {
        try {
          const acknowledged = JSON.parse(fs.readFileSync(`${file}.delivered`, "utf8"));
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
