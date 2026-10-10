import fs from "node:fs";
import path from "node:path";
import { writeFileAtomic } from "../core/atomic-write.js";
import { copyFabricProvenance, type FabricTurnProvenance } from "../fabric-provenance.js";

export interface TerminalControl {
  id: string;
  delivery: "steer" | "followUp";
  provenance?: FabricTurnProvenance | undefined;
  state: "queued" | "delivered" | "refused";
}
const validId = (id: string): boolean => /^[0-9a-f-]{36}$/.test(id);
const controlFile = (directory: string, id: string): string => {
  if (!validId(id)) throw new Error("Invalid terminal control identity");
  return path.join(directory, "terminal-controls", id + ".json");
};
export const trackTerminalControl = (directory: string, control: Omit<TerminalControl, "state">): void => {
  const file = controlFile(directory, control.id);
  if (fs.existsSync(file)) return; // A replay never resurrects consumed/refused input.
  writeFileAtomic(file, JSON.stringify({ ...control, state: "queued" }), { durable: true });
};
export const readTerminalControl = (directory: string, id: string): TerminalControl | undefined => {
  try {
    const item = JSON.parse(fs.readFileSync(controlFile(directory, id), "utf8"));
    if (item.id === id && ["steer", "followUp"].includes(item.delivery) && ["queued", "delivered", "refused"].includes(item.state)) {
      return { id, delivery: item.delivery, state: item.state, provenance: copyFabricProvenance(item.provenance) };
    }
  } catch { /* No invented consumption or identity from uncertain files. */ }
  return undefined;
};
export const settleTerminalControl = (directory: string, id: string, state: "delivered" | "refused"): void => {
  const item = readTerminalControl(directory, id);
  if (item?.state === "queued") writeFileAtomic(controlFile(directory, id), JSON.stringify({ ...item, state }), { durable: true });
};
export const queuedTerminalControls = (directory: string): TerminalControl[] => {
  const root = path.join(directory, "terminal-controls");
  try {
    return fs.readdirSync(root).flatMap(name => {
      if (!name.endsWith(".json") || !validId(name.slice(0, -5))) return [];
      const item = readTerminalControl(directory, name.slice(0, -5));
      return item?.state === "queued" ? [item] : [];
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
