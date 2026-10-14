import fs from "node:fs";
import path from "node:path";
import { readTaskControlJson, readTaskControlJsonAt, writeTaskControlJson, MAX_TASK_CONTROL_ROWS, FabricTaskControlDirectoryError, withTaskControlReadDirectory, visitLegacyTaskControlDirectories, withTaskControlSubdirectory } from "./terminal-answer.js";
import { copyFabricProvenance, type FabricTurnProvenance } from "../fabric-provenance.js";
import { withTaskFinalAnswerLock } from "./terminal-answer.js";

export interface TerminalControl {
  id: string;
  delivery: "steer" | "followUp";
  provenance?: FabricTurnProvenance | undefined;
  state: "queued" | "delivered" | "refused";
  /** Durable write-ahead transport evidence. Missing legacy metadata is uncertain. */
  forwarded?: boolean;
  /** Refused rows only: the line was never read/injected because the run had ended. */
  reason?: "run-ended" | undefined;
}
const validId = (id: string): boolean => /^[0-9a-f-]{36}$/.test(id);
const controlFile = (directory: string, id: string): string => {
  if (!validId(id)) throw new Error("Invalid terminal control identity");
  return path.join(directory, "terminal-controls", id + ".json");
};
export const trackTerminalControl = (directory: string, control: Omit<TerminalControl, "state">): void => {
  controlFile(directory, control.id); // validate before publication
  if (readTerminalControl(directory, control.id)) return; // never resurrect consumed/refused input
  writeTaskControlJson(directory, path.join("terminal-controls", control.id + ".json"), { ...control, forwarded: false, state: "queued" });
};
const parseTerminalControl = (id: string, value: unknown): TerminalControl | undefined => {
  const item = value as any;
  if (item?.id === id && ["steer", "followUp"].includes(item.delivery) && ["queued", "delivered", "refused"].includes(item.state) &&
      (item.forwarded === undefined || typeof item.forwarded === "boolean")) {
    const provenance = copyFabricProvenance(item.provenance);
    if (item.provenance !== undefined && !provenance) return undefined;
    return { id, delivery: item.delivery, state: item.state, provenance,
      ...(item.forwarded === undefined ? {} : { forwarded: item.forwarded }),
      ...(item.state === "refused" && item.reason === "run-ended" ? { reason: "run-ended" as const } : {}) };
  }
  return undefined;
};
export const readTerminalControl = (directory: string, id: string): TerminalControl | undefined => {
  if (!validId(id)) return;
  try {
    const item = parseTerminalControl(id, readTaskControlJson<unknown>(directory, path.join("terminal-controls", id + ".json")));
    if (item) {
      return item;
    }
  } catch { /* No invented consumption or identity from uncertain files. */ }
  return undefined;
};
/** Called inside the injection/publication lock, BEFORE attempting any stdin write. */
export const markTerminalControlForwarded = (directory: string, id: string): boolean => {
  const item = readTerminalControl(directory, id);
  if (item?.state !== "queued") return false;
  writeTaskControlJson(directory, path.join("terminal-controls", id + ".json"), { ...item, forwarded: true });
  return true;
};
/** Native refusal is a discard ACK. Host callers may settle only proven unforwarded rows.
 * Serialize with native consumption so durable delivered can never become refused.
 * beforeTransition publishes synchronous evidence under the lock; it must not reacquire it.
 * Compat: a string fourth argument is the legacy (directory, id, state, reason) form and does not gate. */
export const settleTerminalControl = (directory: string, id: string, state: "delivered" | "refused",
  unforwardedOnlyOrReason: boolean | "run-ended" = false,
  beforeTransition?: (control: TerminalControl) => void, explicitReason?: "run-ended"): TerminalControl | undefined =>
  withTaskFinalAnswerLock(directory, () => {
    const unforwardedOnly = unforwardedOnlyOrReason === true;
    const reason = explicitReason ?? (typeof unforwardedOnlyOrReason === "string" ? unforwardedOnlyOrReason : undefined);
    const item = readTerminalControl(directory, id);
    if (item?.state === "queued" && (!unforwardedOnly || item.forwarded === false)) {
      const settled: TerminalControl = { ...item, state, ...(state === "refused" && reason ? { reason } : {}) };
      beforeTransition?.(settled);
      writeTaskControlJson(directory, path.join("terminal-controls", id + ".json"), settled);
      return settled;
    }
    return item;
  });
export const terminalControls = (directory: string): TerminalControl[] => {
  try {
    const controls = new Map<string, TerminalControl>();
    let count = 0, legacySelected = false;
    const scan = (pinned: string): void => {
      try { withTaskControlSubdirectory(pinned, "terminal-controls", parent => {
        // Keep the namespace fd alive through enumeration AND row reads.
        const entries = fs.opendirSync(parent, { bufferSize: 32 });
        try {
          for (;;) {
            const entry = entries.readSync();
            if (!entry) return;
            if (++count > MAX_TASK_CONTROL_ROWS) throw new FabricTaskControlDirectoryError("Terminal control registry exceeds its entry bound");
            const id = entry.name.slice(0, -5);
            if (!entry.isFile() || !entry.name.endsWith(".json") || !validId(id) || controls.has(id)) continue;
            let item: TerminalControl | undefined;
            try { item = parseTerminalControl(id, readTaskControlJsonAt<unknown>(parent, entry.name)); }
            catch (error) { if (error instanceof SyntaxError) throw error; continue; /* unsafe rows prove no queued input */ }
            if (item) controls.set(id, item);
          }
        } finally { entries.closeSync(); }
      }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    };
    withTaskControlReadDirectory(directory, pinned => { legacySelected = fs.realpathSync(pinned) === path.resolve(directory); scan(pinned); });
    if (!legacySelected) visitLegacyTaskControlDirectories(directory, scan);
    return [...controls.values()];
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
