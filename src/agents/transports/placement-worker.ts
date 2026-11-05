import fs from "node:fs";
import { createHash } from "node:crypto";
import type { AgentRunRecord, AgentTransportLaunch } from "../types.js";
import { writeFileAtomic, writeJsonAtomic } from "../../core/atomic-write.js";

const sha256 = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const inputFlags = new Set(["--task-file", "--schema-file", "--images-file", "--session-file"]);

/** This is a byte/argv envelope, not another worker options model. The receiver
 * owns declared path mapping and installed release/runtime compatibility checks.
 */
export const writePlacementWorkerPacket = (request: AgentTransportLaunch, file: string, taskBytes: Buffer): void => {
  if (request.workerArguments.length % 2 || request.workerArguments.some(value => typeof value !== "string")) {
    throw new Error("Native placement requires ordered worker flag/value strings");
  }
  const args = new Map<string, string>();
  for (let index = 0; index < request.workerArguments.length; index += 2) {
    args.set(request.workerArguments[index]!, request.workerArguments[index + 1]!);
  }
  if (args.get("--id") !== request.id) throw new Error("Native placement worker id does not match launch id");
  const workerPath = fs.realpathSync(request.workerPath);
  // Empty optional file flags mean absent to the native parser; keep the argv
  // value verbatim but do not invent a file payload for that absence.
  const inputs = [...args].filter(([flag, input]) => inputFlags.has(flag) && input !== "").map(([flag, input]) => {
    const bytes = flag === "--task-file" ? taskBytes : fs.readFileSync(input);
    return { flag, base64: bytes.toString("base64"), sha256: sha256(bytes) };
  });
  writeJsonAtomic(file, {
    version: 1, workerPath, workerSha256: sha256(fs.readFileSync(workerPath)),
    workerArguments: [...request.workerArguments], inputs,
  });
};

interface NativeReceipt {
  worker: AgentRunRecord;
  sessionExport?: { base64: string; sha256: string };
}
const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);
const finite = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const count = (value: unknown): boolean => finite(value) && Number.isSafeInteger(value) && value >= 0;
const terminal = new Set(["completed", "failed", "stopped", "timed_out"]);

/** rc:null is handled by the outer receipt parser: even a terminal worker
 * checkpoint is not native-exit proof while the receiver still reports pending.
 * Unknown native fields are retained, never reconstructed from final prose.
 */
export const nativePlacementReceipt = (receipt: Record<string, unknown>, id: string, host: string): NativeReceipt => {
  const worker = receipt.worker;
  if (!object(worker) || worker.id !== id || !terminal.has(String(worker.status))
    || worker.placementHost !== undefined && worker.placementHost !== host) {
    throw new Error("Native placement requires a matching terminal worker record");
  }
  const usage = worker.usage;
  if (["name", "task", "runner", "transport", "cwd", "text"].some(key => typeof worker[key] !== "string")
    || worker.transport !== "process" || !worker.runner
    || !finite(worker.startedAt) || !finite(worker.updatedAt) || !count(worker.turns) || !count(worker.toolCalls)
    || !object(usage) || ["input", "output", "cacheRead", "cacheWrite", "cost"].some(key => !finite(usage[key]) || (usage[key] as number) < 0)
    || worker.finishedAt !== undefined && !finite(worker.finishedAt)
    || worker.exitCode !== undefined && worker.exitCode !== null && !(finite(worker.exitCode) && Number.isSafeInteger(worker.exitCode))
    || worker.replyVia !== undefined && worker.replyVia !== "tool"
    || ["stderr", "model", "requestedModel", "admittedModel", "thinking", "admittedThinking", "sessionId", "runnerSessionId", "fabricRelease"].some(key => worker[key] !== undefined && typeof worker[key] !== "string")
    || ["warnings", "runnerSessionIds"].some(key => worker[key] !== undefined && (!Array.isArray(worker[key]) || (worker[key] as unknown[]).some(value => typeof value !== "string")))) {
    throw new Error("Invalid native placement worker record");
  }
  const sessionExport = receipt.sessionExport;
  if (sessionExport !== undefined && (!object(sessionExport) || typeof sessionExport.base64 !== "string" || typeof sessionExport.sha256 !== "string")) {
    throw new Error("Invalid native placement session export");
  }
  return { worker: worker as unknown as AgentRunRecord, ...(sessionExport !== undefined ? { sessionExport: sessionExport as NonNullable<NativeReceipt["sessionExport"]> } : {}) };
};

/** Write only to the caller's declared output, never a path from the receipt.
 * Decode/hash before touching it so corrupt data cannot replace a prior export.
 */
export const writePlacementSessionExport = (receipt: Pick<NativeReceipt, "sessionExport">, destination: string | undefined, status: AgentRunRecord["status"]): void => {
  if (!receipt.sessionExport) {
    if (destination !== undefined && status === "completed") throw new Error("Native placement completed without requested session export");
    // A failed/interrupted worker may never have created its native session.
    return;
  }
  const { base64, sha256: expected } = receipt.sessionExport;
  const bytes = Buffer.from(base64, "base64");
  if (bytes.toString("base64") !== base64 || !/^[a-f0-9]{64}$/u.test(expected) || sha256(bytes) !== expected) {
    throw new Error("Invalid native placement session export hash/encoding");
  }
  if (destination !== undefined) writeFileAtomic(destination, bytes);
};
