import fs from "node:fs";
import path from "node:path";
import { readTaskControlJson, readTaskControlJsonAt, writeTaskControlJson, MAX_TASK_CONTROL_ROWS, FabricTaskControlDirectoryError, withTaskControlReadDirectory, visitLegacyTaskControlDirectories, withTaskControlSubdirectory } from "./terminal-answer.js";
import { copyFabricProvenance, type FabricTurnProvenance } from "../fabric-provenance.js";

export interface TerminalControl {
  id: string;
  delivery: "steer" | "followUp";
  provenance?: FabricTurnProvenance | undefined;
  state: "queued" | "delivered" | "refused";
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
  writeTaskControlJson(directory, path.join("terminal-controls", control.id + ".json"), { ...control, state: "queued" });
};
export const readTerminalControl = (directory: string, id: string): TerminalControl | undefined => {
  if (!validId(id)) return;
  try {
    const item = readTaskControlJson<any>(directory, path.join("terminal-controls", id + ".json"));
    if (item?.id === id && ["steer", "followUp"].includes(item.delivery) && ["queued", "delivered", "refused"].includes(item.state)) {
      return { id, delivery: item.delivery, state: item.state, provenance: copyFabricProvenance(item.provenance),
        ...(item.state === "refused" && item.reason === "run-ended" ? { reason: "run-ended" as const } : {}) };
    }
  } catch { /* No invented consumption or identity from uncertain files. */ }
  return undefined;
};
export const settleTerminalControl = (directory: string, id: string, state: "delivered" | "refused", reason?: "run-ended"): void => {
  const item = readTerminalControl(directory, id);
  if (item?.state === "queued") writeTaskControlJson(directory, path.join("terminal-controls", id + ".json"),
    { ...item, state, ...(state === "refused" && reason ? { reason } : {}) });
};
export const queuedTerminalControls = (directory: string): TerminalControl[] => {
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
            try { item = readTaskControlJsonAt<TerminalControl>(parent, entry.name); }
            catch (error) { if (error instanceof SyntaxError) throw error; continue; /* unsafe rows prove no queued input */ }
            if (item?.id === id && ["steer", "followUp"].includes(item.delivery) && ["queued", "delivered", "refused"].includes(item.state)) controls.set(id, item);
          }
        } finally { entries.closeSync(); }
      }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    };
    withTaskControlReadDirectory(directory, pinned => { legacySelected = fs.realpathSync(pinned) === path.resolve(directory); scan(pinned); });
    if (!legacySelected) visitLegacyTaskControlDirectories(directory, scan);
    return [...controls.values()].filter(item => item.state === "queued");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
};
// Private envelopes attach an unambiguous transport marker. The native context
// hook strips it, so identity/provenance never depend on the model's final text.
export const terminalControlMessage = (id: string, text: string): string => `<!-- fabric-control:${id} -->\n${text}`;
export const terminalControlMessageId = (text: string): string | undefined => /^<!-- fabric-control:([0-9a-f-]{36}) -->\n/.exec(text)?.[1];
