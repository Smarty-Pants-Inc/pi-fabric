import fs from "node:fs";
import path from "node:path";
import { writeFileAtomic } from "../core/atomic-write.js";
import { copyFabricProvenance, type FabricTurnProvenance } from "../fabric-provenance.js";
import { withTaskFinalAnswerLock } from "./terminal-answer.js";

export interface TerminalControl {
  id: string;
  delivery: "steer" | "followUp";
  provenance?: FabricTurnProvenance | undefined;
  state: "queued" | "delivered" | "refused";
  /** Durable write-ahead transport evidence. Missing legacy metadata is uncertain. */
  forwarded?: boolean;
}
const validId = (id: string): boolean => /^[0-9a-f-]{36}$/.test(id);
const controlFile = (directory: string, id: string): string => {
  if (!validId(id)) throw new Error("Invalid terminal control identity");
  return path.join(directory, "terminal-controls", id + ".json");
};
export const trackTerminalControl = (directory: string, control: Omit<TerminalControl, "state">): void => {
  const file = controlFile(directory, control.id);
  if (fs.existsSync(file)) return; // A replay never resurrects consumed/refused input.
  writeFileAtomic(file, JSON.stringify({ ...control, forwarded: false, state: "queued" }), { durable: true });
};
export const readTerminalControl = (directory: string, id: string): TerminalControl | undefined => {
  try {
    const item = JSON.parse(fs.readFileSync(controlFile(directory, id), "utf8"));
    if (item.id === id && ["steer", "followUp"].includes(item.delivery) && ["queued", "delivered", "refused"].includes(item.state) &&
        (item.forwarded === undefined || typeof item.forwarded === "boolean")) {
      const provenance = copyFabricProvenance(item.provenance);
      if (item.provenance !== undefined && !provenance) return undefined;
      return { id, delivery: item.delivery, state: item.state, provenance,
        ...(item.forwarded === undefined ? {} : { forwarded: item.forwarded }) };
    }
  } catch { /* No invented consumption or identity from uncertain files. */ }
  return undefined;
};
/** Called inside the injection/publication lock, BEFORE attempting any stdin write. */
export const markTerminalControlForwarded = (directory: string, id: string): boolean => {
  const item = readTerminalControl(directory, id);
  if (item?.state !== "queued") return false;
  writeFileAtomic(controlFile(directory, id), JSON.stringify({ ...item, forwarded: true }), { durable: true });
  return true;
};
/** Native refusal is a discard ACK. Host callers may settle only proven unforwarded rows.
 * Serialize with native consumption so durable delivered can never become refused.
 * beforeTransition publishes synchronous evidence under the lock; it must not reacquire it. */
export const settleTerminalControl = (directory: string, id: string, state: "delivered" | "refused", unforwardedOnly = false,
  beforeTransition?: (control: TerminalControl) => void): TerminalControl | undefined =>
  withTaskFinalAnswerLock(directory, () => {
    const item = readTerminalControl(directory, id);
    if (item?.state === "queued" && (!unforwardedOnly || item.forwarded === false)) {
      const settled = { ...item, state };
      beforeTransition?.(settled);
      writeFileAtomic(controlFile(directory, id), JSON.stringify(settled), { durable: true });
      return settled;
    }
    return item;
  });
export const terminalControls = (directory: string): TerminalControl[] => {
  const root = path.join(directory, "terminal-controls");
  try {
    return fs.readdirSync(root).flatMap(name => {
      if (!name.endsWith(".json") || !validId(name.slice(0, -5))) return [];
      const item = readTerminalControl(directory, name.slice(0, -5));
      return item ? [item] : [];
    });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
};
export const queuedTerminalControls = (directory: string): TerminalControl[] => terminalControls(directory).filter(item => item.state === "queued");
// Private envelopes attach an unambiguous transport marker. The native context
// hook strips it, so identity/provenance never depend on the model's final text.
export const terminalControlMessage = (id: string, text: string): string => `<!-- fabric-control:${id} -->\n${text}`;
export const terminalControlMessageId = (text: string): string | undefined => /^<!-- fabric-control:([0-9a-f-]{36}) -->\n/.exec(text)?.[1];
/** The RPC prompt ID is not passed to AgentSession; its private command carries the same ID. */
export const terminalControlInputId = (input: unknown): string | undefined => {
  if (typeof input === "string") return terminalControlMessageId(input) ?? /^\/fabric-delivery ([0-9a-f-]{36})$/.exec(input)?.[1];
  if (Array.isArray(input)) return input.map(terminalControlInputId).find(id => id !== undefined);
  if (input && typeof input === "object") {
    const value = input as { text?: unknown; content?: unknown };
    return terminalControlInputId(value.text ?? value.content);
  }
  return undefined;
};
