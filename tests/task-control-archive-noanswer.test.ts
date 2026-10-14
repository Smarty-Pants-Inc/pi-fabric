import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import { appendTerminalAdmission } from "../src/agents/terminal-notices.js";
import { canRemoveTerminalRun, pruneActorRunArchives, runTreeExitVeto } from "../src/storage/retention.js";
import { verifiedRemove } from "../src/storage/verified-remove.js";
import { createHostTaskRun, initializeTaskControlDirectory, legacyTaskControlSibling, MAX_TASK_CONTROL_BYTES, readFinalAnswerReceipt, TASK_CONTROL_LAYOUT, taskControlDirectory, withLegacyTaskControlDirectory } from "../src/worker/terminal-answer.js";
import { queuedTerminalControls, settleTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";
import { bytesOf, day, json, registerArchiveCleanup, removalPaths, roots, setupCurrent } from "./task-control-archive-fixtures.js";

// Keep reason: trust boundary / data safety. Exercise the actual archive and
// collection judgment, recording only the last native removal boundary.
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: vi.fn(() => ({ removed: true, bytes: 0 })) };
});
registerArchiveCleanup();
const provenance = { v: 1, channel: "fabric", via: "steer", sender: { id: "session:original", kind: "main", verified: "mesh" } } as const;
const admissionId = "11111111-1111-4111-8111-111111111111";
/** The run's OWN v2 `snapshot:null` binding, as published by the host allocator / archive writer. */
const nullBinding = (run: string) => {
  const stat = fs.lstatSync(run, { bigint: true });
  return { version: 2, runId: path.basename(run), dev: String(stat.dev), ino: String(stat.ino), snapshot: null };
};
const bindingBytes = (run: string) => fs.readFileSync(path.join(taskControlDirectory(run), ".legacy-read.json"));
const actorFor = (root: string) => {
  const alias = "archive-alias", actor = { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: alias };
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: day });
  return { alias, actor, store, archive: path.join(root, "actor", "runs", alias) };
};
// Current-v2 seed at the final lexical path: exclusive host allocation (own v2
// snapshot:null binding), real host admission writer, worker track -> queued ->
// settle delivered. No raw copy, no old grant, no bootstrap-by-validation.
const setupNoAnswer = (status: "stopped" | "failed", sourceDirectory?: string) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "task-control-archive-noanswer-")); roots.push(root);
  const sourceRuns = sourceDirectory ? path.dirname(sourceDirectory) : path.join(root, "source-runs"), source = sourceDirectory ?? path.join(sourceRuns, "source-run");
  fs.mkdirSync(sourceRuns, { recursive: true, mode: 0o700 });
  createHostTaskRun(source);
  const sibling = taskControlDirectory(source), binding = bindingBytes(source);
  expect(JSON.parse(binding.toString("utf8"))).toEqual(nullBinding(source));
  // Nested aliases keep the original record id, as in the historical fixture.
  const record = { id: "source-run", runner: "pi", status: "running", transport: "process", sessionId: "2147483647", updatedAt: day, taskControlLayout: TASK_CONTROL_LAYOUT };
  json(path.join(source, "status.json"), record);
  const admission = { id: admissionId, type: "steer", message: "resumed admission", provenance };
  const journal = Buffer.from(JSON.stringify(admission) + "\n");
  appendTerminalAdmission(source, journal.toString("utf8"));
  trackTerminalControl(source, { id: admission.id, delivery: "steer", provenance });
  const queued = { id: admission.id, delivery: "steer", provenance, state: "queued" };
  expect(queuedTerminalControls(source)).toEqual([queued]);
  settleTerminalControl(source, admission.id, "delivered");
  const delivered = { ...queued, state: "delivered" };
  expect(JSON.parse(fs.readFileSync(path.join(sibling, "terminal-controls", admission.id + ".json"), "utf8"))).toEqual(delivered);
  expect(fs.readFileSync(path.join(sibling, "steer.jsonl"))).toEqual(journal);
  expect(queuedTerminalControls(source)).toEqual([]);
  const terminal = { ...record, status, finishedAt: day };
  json(path.join(source, "status.json"), terminal);
  expect(() => initializeTaskControlDirectory(source)).not.toThrow(); // validation only: proves the v2 binding
  expect(bindingBytes(source)).toEqual(binding);
  return { root, sourceRuns, source, sibling, record: terminal, admission, queued, delivered, journal, binding, ...actorFor(root) };
};
// Historical (pre-cutoff, #12214) raw no-answer history: in-run queued debt plus an
// old `.ctl` delivered overlay. Never bound, never bootstrapped: always refused.
const setupLegacyNoAnswer = () => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "task-control-archive-noanswer-")); roots.push(root);
  const source = path.join(root, "source-runs", "source-run"), sibling = legacyTaskControlSibling(source);
  const record = { id: "source-run", runner: "pi", status: "stopped", transport: "process", sessionId: "2147483647", updatedAt: day, finishedAt: day };
  json(path.join(source, "status.json"), record);
  const admission = { id: admissionId, type: "steer", message: "resumed admission", provenance };
  fs.writeFileSync(path.join(source, "steer.jsonl"), JSON.stringify(admission) + "\n", { mode: 0o600 });
  const queued = { id: admission.id, state: "queued", delivery: "steer", provenance }, delivered = { ...queued, state: "delivered" };
  json(path.join(source, "terminal-controls", admission.id + ".json"), queued);
  json(path.join(sibling, "terminal-controls", admission.id + ".json"), delivered);
  return { root, source, sibling, record, admission, queued, delivered, ...actorFor(root) };
};

describe("actor archive paired task controls", () => {
  // Historical title kept byte-identical. Current-v2 replaces the unsupported legacy
  // overlay (raw queued debt + promoted delivered sibling, refused since #12214): the
  // consumed no-answer pair is now one host-written central v2 namespace per run.
  it.each(["stopped", "failed"] as const)("archives real consumed legacy %s no-answer overlays, including nested aliases, and collects both pairs", async status => {
    const f = setupNoAnswer(status), nested = path.join(f.source, "nested", "child-alias");
    const child = setupNoAnswer(status, nested);
    expect(bindingBytes(f.source)).toEqual(f.binding);
    expect(runTreeExitVeto(f.source, 0, undefined, true)).toBeUndefined();
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, f.source);
    for (const source of [f.source, nested]) {
      const archive = path.join(f.archive, path.relative(f.source, source)), ctl = taskControlDirectory(archive);
      expect(fs.readFileSync(path.join(archive, "status.json"))).toEqual(fs.readFileSync(path.join(source, "status.json")));
      expect(JSON.parse(fs.readFileSync(path.join(archive, "status.json"), "utf8")).id).toBe(f.record.id);
      expect(fs.readFileSync(path.join(ctl, "steer.jsonl"))).toEqual(f.journal);
      expect(JSON.parse(fs.readFileSync(path.join(ctl, "terminal-controls", f.admission.id + ".json"), "utf8"))).toEqual(f.delivered);
      // Source current control stays delivered (no raw queued row); its binding is byte-preserved.
      expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(source), "terminal-controls", f.admission.id + ".json"), "utf8"))).toEqual(f.delivered);
      expect(fs.existsSync(path.join(source, "terminal-controls"))).toBe(false);
      expect(queuedTerminalControls(source)).toEqual([]);
      expect(bindingBytes(source)).toEqual(source === f.source ? f.binding : child.binding);
      // The archive carries its OWN fresh v2 null binding, never the source's.
      expect(JSON.parse(fs.readFileSync(path.join(ctl, ".legacy-read.json"), "utf8"))).toEqual(nullBinding(archive));
      expect(bindingBytes(archive)).not.toEqual(bindingBytes(source));
      expect(withLegacyTaskControlDirectory(archive, () => true)).toBeUndefined();
      expect(fs.existsSync(path.join(archive, "final-answer.json"))).toBe(false);
      expect(fs.existsSync(path.join(ctl, "final-answer.json"))).toBe(false);
      expect(readFinalAnswerReceipt(archive, f.record.id)).toBeUndefined();
      expect(canRemoveTerminalRun(source)).toBe(true);
      expect(canRemoveTerminalRun(archive)).toBe(true);
    }
    expect(pruneActorRunArchives({ runsDirectory: f.sourceRuns, retentionMs: day, now: 4 * day, dryRun: true })).toEqual([f.source]);
    expect(pruneActorRunArchives({ runsDirectory: path.dirname(f.archive), retentionMs: day, now: 4 * day, dryRun: true })).toEqual([f.archive]);
    // f.sibling is the source's host-allocated CENTRAL v2 namespace; nested centrals sit outside each tree.
    expect(removalPaths()).toEqual([f.source, f.sibling, taskControlDirectory(nested),
      f.archive, taskControlDirectory(f.archive), taskControlDirectory(path.join(f.archive, "nested", "child-alias"))]);
    expect(vi.mocked(verifiedRemove).mock.calls.every(([, , options]) => options.dryRun === true)).toBe(true);
    expect(fs.existsSync(f.source)).toBe(true);
    expect(fs.existsSync(f.archive)).toBe(true);
  });

  it.each(["claimed-missing-receipt", "source-sender", "source-id", "source-delivery", "overlay-sender", "overlay-id", "overlay-delivery",
    "source-refused", "source-unknown", "overlay-queued", "source-extra-field", "notice", "duplicate-notice", "unknown-artifact"] as const)("refuses no-answer %s without publishing archive authority", async bad => {
    // Cutoff (#12214): these are OLD no-answer overlay faults. Raw in-run + `.ctl`
    // history is never bound or bootstrapped; the fault is applied FIRST, then the
    // archive and the validator refuse it with every byte preserved, never final.
    const f = setupLegacyNoAnswer();
    const old = path.join(f.source, "terminal-controls", f.admission.id + ".json"), overlay = path.join(f.sibling, "terminal-controls", f.admission.id + ".json");
    if (bad === "claimed-missing-receipt") json(path.join(f.source, "status.json"), { ...f.record, finalAnswerReceipt: { id: "claimed-answer", recordedAt: day } });
    if (bad.endsWith("sender") || bad.endsWith("id") || bad.endsWith("delivery")) {
      const source = bad.startsWith("source");
      json(source ? old : overlay, { ...(source ? f.queued : f.delivered),
        ...(bad.endsWith("sender") ? { provenance: { ...f.queued.provenance, sender: { ...f.queued.provenance.sender, id: "session:forged" } } } : {}),
        ...(bad.endsWith("id") ? { id: "other-control" } : {}),
        ...(bad.endsWith("delivery") ? { delivery: "followUp" } : {}) });
    }
    if (bad === "source-refused" || bad === "source-unknown") json(old, { ...f.queued, state: bad.slice(7) });
    if (bad === "overlay-queued") json(overlay, f.queued);
    if (bad === "source-extra-field") json(old, { ...f.queued, authority: "invented" });
    if (bad === "notice" || bad === "duplicate-notice") {
      const notice = { code: "FABRIC_TARGET_TERMINAL", targetId: f.record.id, messageId: f.admission.id, delivery: "steer", finalAnswerReceiptId: "missing-answer", sender: f.queued.provenance.sender };
      json(path.join(f.source, "terminal-notices", f.admission.id + ".json.delivered"), notice);
      if (bad === "duplicate-notice") json(path.join(f.sibling, "terminal-notices", f.admission.id + ".json.delivered"), notice);
    }
    if (bad === "unknown-artifact") json(path.join(f.sibling, "unknown.json"), {});
    const before = bytesOf(path.dirname(f.source), f.sibling, taskControlDirectory(f.source));
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow("Missing protected task control directory");
    expect(() => initializeTaskControlDirectory(f.source)).toThrow("FABRIC_LEGACY_SNAPSHOT_REQUIRED");
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(fs.existsSync(taskControlDirectory(f.source))).toBe(false); // no minted authority
    expect(() => readFinalAnswerReceipt(f.source, f.record.id)).toThrow("FABRIC_LEGACY_SNAPSHOT_REQUIRED");
    expect(bytesOf(path.dirname(f.source), f.sibling, taskControlDirectory(f.source))).toEqual(before);
    expect(removalPaths()).toEqual([]);
  });

  it("counts original artifact bytes before collapsing matching overlays", async () => {
    // Current-v2: no overlay layer exists to collapse. Real admissions are delivered
    // through the host writers, then the actual central delivered rows are padded so
    // the aggregate archive artifact bound (not queued debt) is what refuses.
    const f = setupCurrent();
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    for (let index = 0; index < 70; index++) {
      const id = `00000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`;
      appendTerminalAdmission(f.source, JSON.stringify({ ...f.admission, id }) + "\n");
      trackTerminalControl(f.source, { id, delivery: "steer", provenance: f.admission.provenance });
      settleTerminalControl(f.source, id, "delivered");
      const row = path.join(f.controls, "terminal-controls", id + ".json"), delivered = fs.readFileSync(row, "utf8");
      expect(JSON.parse(delivered)).toEqual({ id, delivery: "steer", provenance: f.admission.provenance, state: "delivered" });
      fs.writeFileSync(row, delivered.padEnd(MAX_TASK_CONTROL_BYTES - 1, " "));
    }
    expect(queuedTerminalControls(f.source)).toEqual([]);
    const before = bytesOf(f.source, f.controls);
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow("byte bound exceeded");
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(bytesOf(f.source, f.controls)).toEqual(before);
    expect(removalPaths()).toEqual([]);
  });
});
