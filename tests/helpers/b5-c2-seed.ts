import fs from "node:fs";
import path from "node:path";
import { recoveryCanonical, recoveryHash, type RecoverySessionLedger } from "../../src/recovery/input-admission.js";

/** Deliberate test-only corrupt/incomplete retained history; NOT authoritative NEW evidence.
 * Production initialization must never certify historyComplete=false as a complete seal. */
export function seedIncompleteRecoveryHistory(root: string, sessionId: string): void {
  const file = path.join(root, "input-admission", "v1", "session", `${recoveryHash(sessionId)}.json`);
  const payload: RecoverySessionLedger = { version: 1, sessionId, revision: 0, complete: false,
    inputs: [], outcomes: [], maintenance: [], retrySlots: [], repairs: [], recoveryGenerationPins: [], taskAborts: [], driveReceipts: [] };
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify({ version: 1, payload, sha256: recoveryHash(recoveryCanonical(payload)) }), { flag: "wx" });
}
