import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, vi } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import { verifiedRemove } from "../src/storage/verified-remove.js";
import { appendTerminalAdmission } from "../src/agents/terminal-notices.js";
import type { AgentTerminalNotice } from "../src/agents/terminal-target.js";
import type { FabricTurnProvenance } from "../src/host-compatibility.js";
import { createHostTaskRun, legacyTaskControlSibling, saveFinalAnswerReceipt, TASK_CONTROL_LAYOUT, taskControlDirectory, writeTaskControlJson } from "../src/worker/terminal-answer.js";
import { settleTerminalControl, trackTerminalControl, type TerminalControl } from "../src/worker/terminal-controls.js";

// Shared fixtures for the task-control archive suites. Each suite keeps its own
// hoisted vi.mock of verified-remove; this module sees the same mocked binding.
export const roots: string[] = [];
export const registerArchiveCleanup = () => afterEach(() => {
  vi.restoreAllMocks();
  vi.mocked(verifiedRemove).mockReset();
  vi.mocked(verifiedRemove).mockReturnValue({ removed: true, bytes: 0 });
  // The parent recorded-removal runner suppresses fixture cleanup locally.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
export const day = 24 * 60 * 60 * 1000;
export const json = (file: string, value: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
};
// RAW old-history builder only (sibling-v1 `.ctl`, in-run legacy, or explicitly UNBOUND
// central). Current v2 ALWAYS refuses these layers and preserves their bytes (#12214);
// this helper never mints a v2 binding/snapshot nor calls validation as a bootstrap.
export const setup = (layout: "sibling-v1" | "legacy" | "migrated" = "sibling-v1", alias = "archive") => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "task-control-archive-")); roots.push(root);
  const source = path.join(root, "source-run"); fs.mkdirSync(source, { mode: 0o700 });
  const receipt = { version: 1, id: "original-answer", runId: "source-run", recordedAt: day, text: "preserved final answer" };
  const record = { id: "source-run", runner: "pi", status: "completed", transport: "process", sessionId: "2147483647", finishedAt: day,
    ...(layout === "sibling-v1" ? { taskControlLayout: layout } : {}), finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } };
  json(path.join(source, "status.json"), record);
  fs.writeFileSync(path.join(source, "task.txt"), "original task", { mode: 0o600 });
  // sibling-v1 fixtures are OLD read-only siblings; "migrated" is raw central history
  // with NO binding (unbound), written by mkdir/json only.
  const controls = layout === "sibling-v1" ? legacyTaskControlSibling(source) : layout === "migrated" ? taskControlDirectory(source) : source;
  fs.mkdirSync(controls, { recursive: true, mode: 0o700 });
  json(path.join(controls, "final-answer.json"), receipt);
  const sender = { id: "session:original", kind: "main", verified: "mesh" };
  const provenance = { v: 1, channel: "fabric", via: "steer", sender };
  const admission = { id: "original-steer", type: "steer", message: "original admission", provenance };
  fs.writeFileSync(path.join(controls, "steer.jsonl"), JSON.stringify(admission) + "\n", { mode: 0o600 });
  const control = { id: admission.id, delivery: "steer", state: "refused", reason: "run-ended", provenance };
  json(path.join(controls, "terminal-controls", admission.id + ".json"), control);
  const notice = { code: "FABRIC_TARGET_TERMINAL", targetId: receipt.runId, messageId: admission.id, delivery: "steer", finalAnswerReceiptId: receipt.id, sender };
  json(path.join(controls, "terminal-notices", admission.id + ".json.delivered"), notice);
  const actor = { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: alias };
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: day });
  const archive = path.join(root, "actor", "runs", alias);
  return { root, source, controls, receipt, record, admission, control, notice, alias, actor, store, archive };
};
export const setupRaw = setup;

// Current-v2 seed through the production writers only: exclusive host allocation at the
// FINAL lexical path, then real receipt/admission/track/settle/notice writers.
export const CURRENT_ADMISSION_ID = "11111111-1111-4111-8111-111111111111";
export const setupCurrent = (alias = "archive", sourceDirectory?: string) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "task-control-archive-")); roots.push(root);
  const source = path.resolve(sourceDirectory ?? path.join(root, "source-run"));
  if (fs.existsSync(source)) throw new Error("setupCurrent requires an absent final source path");
  fs.mkdirSync(path.dirname(source), { recursive: true, mode: 0o700 });
  createHostTaskRun(source);
  // Archives retain the logical record id even under child/nested aliases.
  // Retention ages by receipt recordedAt (retention.ts recordAgeReference); pin the genuine writer's clock to `day`.
  const clock = vi.spyOn(Date, "now").mockReturnValue(day);
  let receipt: ReturnType<typeof saveFinalAnswerReceipt>;
  try { receipt = saveFinalAnswerReceipt(source, "source-run", "preserved final answer"); } finally { clock.mockRestore(); }
  const sender = { id: "session:original", kind: "main", verified: "mesh" } as const satisfies FabricTurnProvenance["sender"];
  const provenance = { v: 1, channel: "fabric", via: "steer", sender } as const satisfies FabricTurnProvenance;
  const admission = { id: CURRENT_ADMISSION_ID, type: "steer", message: "original admission", provenance } as const;
  appendTerminalAdmission(source, JSON.stringify(admission) + "\n");
  trackTerminalControl(source, { id: admission.id, delivery: "steer", provenance });
  settleTerminalControl(source, admission.id, "refused", "run-ended");
  // Current host tracking persists write-ahead transport evidence; only RAW
  // legacy fixture rows above intentionally omit `forwarded`.
  const control: TerminalControl = { id: admission.id, delivery: "steer", state: "refused", reason: "run-ended", provenance, forwarded: false };
  const notice: AgentTerminalNotice = { code: "FABRIC_TARGET_TERMINAL", targetId: receipt.runId, messageId: admission.id, delivery: "steer", finalAnswerReceiptId: receipt.id, sender };
  writeTaskControlJson(source, path.join("terminal-notices", admission.id + ".json.delivered"), notice);
  const record = { id: "source-run", runner: "pi", status: "completed", transport: "process", sessionId: "2147483647", finishedAt: day,
    taskControlLayout: TASK_CONTROL_LAYOUT, finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } };
  json(path.join(source, "status.json"), record);
  fs.writeFileSync(path.join(source, "task.txt"), "original task", { mode: 0o600 });
  const controls = taskControlDirectory(source);
  const actor = { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: alias };
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: day });
  const archive = path.join(root, "actor", "runs", alias);
  return { root, source, controls, receipt, record, admission, control, notice, alias, actor, store, archive };
};

/** Stable map of every entry under the given roots: path -> base64 bytes / dir / link / absent. */
export const bytesOf = (...dirs: string[]): Record<string, string> => {
  const out: Record<string, string> = {};
  const walk = (entry: string): void => {
    let stat: fs.Stats;
    try { stat = fs.lstatSync(entry); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") { out[entry] = "absent"; return; }
      throw error;
    }
    if (stat.isSymbolicLink()) out[entry] = "link:" + fs.readlinkSync(entry);
    else if (stat.isDirectory()) { out[entry] = "dir"; for (const name of fs.readdirSync(entry).sort()) walk(path.join(entry, name)); }
    else if (stat.isFile()) out[entry] = fs.readFileSync(entry).toString("base64");
    else out[entry] = "other";
  };
  for (const dir of dirs) walk(path.resolve(dir));
  return Object.fromEntries(Object.entries(out).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
};

/** Original artifact paths, derived from the fixture's actual admission id. */
export const originalFiles = (f: { source: string; controls: string; admission: { id: string } }): string[] => [
  path.join(f.source, "status.json"),
  path.join(f.source, "task.txt"),
  path.join(f.controls, "final-answer.json"),
  path.join(f.controls, "steer.jsonl"),
  path.join(f.controls, "terminal-controls", f.admission.id + ".json"),
  path.join(f.controls, "terminal-notices", f.admission.id + ".json.delivered"),
];

export const removalPaths = () => vi.mocked(verifiedRemove).mock.calls.map(([target]) => target);
export const addMigrationOverlay = (f: ReturnType<typeof setup>, source = f.source) => {
  // Raw adversarial overlay; consumers must expect refusal + byte preservation.
  const sibling = taskControlDirectory(source);
  fs.mkdirSync(sibling, { recursive: true, mode: 0o700 });
  json(path.join(source, "terminal-controls", f.admission.id + ".json"), { ...f.control, state: "queued", reason: undefined });
  const newer = { ...f.admission, id: "new-steer", message: "new sibling admission" };
  const journal = Buffer.from(JSON.stringify(newer) + "\n");
  fs.writeFileSync(path.join(sibling, "steer.jsonl"), journal, { mode: 0o600 });
  const delivered = { ...f.control, state: "delivered", reason: undefined };
  json(path.join(sibling, "terminal-controls", f.admission.id + ".json"), delivered);
  json(path.join(sibling, "terminal-controls", newer.id + ".json"), { ...delivered, id: newer.id });
  return { newer, journal, delivered };
};
