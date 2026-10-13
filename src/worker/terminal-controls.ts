import fs from "node:fs";
import path from "node:path";
import { writeFileAtomic } from "../core/atomic-write.js";
import { readTaskControlJson, MAX_TASK_CONTROL_ROWS, FabricTaskControlDirectoryError, withTaskControlDirectory } from "./terminal-answer.js";
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
export const trackTerminalControl = (directory: string, control: Omit<TerminalControl, "state">): void => withTaskControlDirectory(directory, pinned => {
  // The durable atomic writer fsyncs ancestors; /proc is not fsync-able.
  // Resolve the validated host-owned fd to its canonical namespace for writes.
  const file = controlFile(fs.realpathSync(pinned), control.id);
  if (fs.existsSync(file)) return; // A replay never resurrects consumed/refused input.
  writeFileAtomic(file, JSON.stringify({ ...control, state: "queued" }), { durable: true });
});
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
  if (item?.state === "queued") withTaskControlDirectory(directory, pinned => writeFileAtomic(controlFile(fs.realpathSync(pinned), id),
    JSON.stringify({ ...item, state, ...(state === "refused" && reason ? { reason } : {}) }), { durable: true }));
};
export const queuedTerminalControls = (directory: string): TerminalControl[] => {
  try {
    return withTaskControlDirectory(directory, pinned => {
      const entries = fs.opendirSync(path.join(pinned, "terminal-controls"), { bufferSize: 32 }), queued: TerminalControl[] = [];
      let count = 0;
      try {
        for (;;) {
          const entry = entries.readSync();
          if (!entry) return queued;
          if (++count > MAX_TASK_CONTROL_ROWS) throw new FabricTaskControlDirectoryError("Terminal control registry exceeds its entry bound");
          if (!entry.isFile() || !entry.name.endsWith(".json") || !validId(entry.name.slice(0, -5))) continue;
          const item = readTerminalControl(directory, entry.name.slice(0, -5));
          if (item?.state === "queued") queued.push(item);
        }
      } finally { entries.closeSync(); }
    });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
};
// Private envelopes attach an unambiguous transport marker. The native context
// hook strips it, so identity/provenance never depend on the model's final text.
export const terminalControlMessage = (id: string, text: string): string => `<!-- fabric-control:${id} -->\n${text}`;
export const terminalControlMessageId = (text: string): string | undefined => /^<!-- fabric-control:([0-9a-f-]{36}) -->\n/.exec(text)?.[1];
