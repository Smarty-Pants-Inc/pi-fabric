import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";

/** Advisory per-key events, independent of generic state/lease commits. */
export const RESIDENCY_NOTIFICATION_DIR = "residency-notifications";
export const isResidencyNotificationKey = (key: string): boolean =>
  key.startsWith("residency/deliveries/") || key.startsWith("residency/completion-claims/");
export const residencyNotificationName = (key: string): string =>
  `${createHash("sha256").update(key).digest("hex")}.json`;

/** A lost hint is replayed at attachment/explicit recovery, never by an idle retry tick.
 * The authoritative commit stands even if publication fails. Deletion removes the hint,
 * keeping its lifetime bounded by the live key rather than retaining another journal. */
export const publishResidencyNotification = (root: string, key: string, present: boolean): void => {
  if (!isResidencyNotificationKey(key)) return;
  const directory = path.join(root, RESIDENCY_NOTIFICATION_DIR);
  const file = path.join(directory, residencyNotificationName(key));
  try {
    if (!present) { fs.unlinkSync(file); return; }
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    writeJsonAtomic(file, { key, nonce: randomBytes(16).toString("hex") });
  } catch { /* Advisory only: startup/explicit recovery rereads authoritative state. */ }
};

/** Filenames and bytes are hints only. The caller must re-read the exact state key
 * and validate its signature/owner/receipt/CAS before attempting any delivery. */
export const readResidencyNotification = (root: string, filename: string): string | undefined => {
  if (!/^[a-f0-9]{64}\.json$/.test(filename)) return undefined;
  try {
    const file = path.join(root, RESIDENCY_NOTIFICATION_DIR, filename);
    if (fs.statSync(file).size > 2_048) return undefined;
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as { key?: unknown };
    return typeof value.key === "string" && isResidencyNotificationKey(value.key) &&
      residencyNotificationName(value.key) === filename ? value.key : undefined;
  } catch { return undefined; }
};
