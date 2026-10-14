import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import { canRemoveTerminalRun, pruneActorRunArchives, runTreeExitVeto } from "../src/storage/retention.js";
import { verifiedRemove } from "../src/storage/verified-remove.js";
import { createHostTaskRun, initializeTaskControlDirectory, legacyTaskControlSibling, saveFinalAnswerReceipt, MAX_FINAL_ANSWER_BYTES, MAX_TASK_CONTROL_BYTES, readFinalAnswerReceipt, taskControlDirectory, withLegacyTaskControlDirectory } from "../src/worker/terminal-answer.js";
import { queuedTerminalControls, settleTerminalControl } from "../src/worker/terminal-controls.js";

// Keep reason: trust boundary / data safety. Exercise the actual archive and
// collection judgment, recording only the last native removal boundary.
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: vi.fn(() => ({ removed: true, bytes: 0 })) };
});
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.mocked(verifiedRemove).mockReset();
  vi.mocked(verifiedRemove).mockReturnValue({ removed: true, bytes: 0 });
  // The parent recorded-removal runner suppresses fixture cleanup locally.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const day = 24 * 60 * 60 * 1000;
const json = (file: string, value: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
};
const setup = (layout: "sibling-v1" | "legacy" | "migrated" = "sibling-v1", alias = "archive") => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "task-control-archive-")); roots.push(root);
  const source = path.join(root, "source-run"); fs.mkdirSync(source, { mode: 0o700 });
  const receipt = { version: 1, id: "original-answer", runId: "source-run", recordedAt: day, text: "preserved final answer" };
  const record = { id: "source-run", runner: "pi", status: "completed", transport: "process", sessionId: "2147483647", finishedAt: day,
    ...(layout === "sibling-v1" ? { taskControlLayout: layout } : {}), finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } };
  json(path.join(source, "status.json"), record);
  fs.writeFileSync(path.join(source, "task.txt"), "original task", { mode: 0o600 });
  // sibling-v1 fixtures are OLD read-only siblings; new controls are central.
  const controls = layout === "sibling-v1" ? legacyTaskControlSibling(source) : source;
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
  if (layout === "migrated") initializeTaskControlDirectory(source);
  const actor = { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: alias };
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: day });
  const archive = path.join(root, "actor", "runs", alias);
  return { root, source, controls, receipt, record, admission, control, notice, alias, actor, store, archive };
};
const setupNoAnswer = (status: "stopped" | "failed", sourceDirectory?: string) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "task-control-archive-noanswer-")); roots.push(root);
  const sourceRuns = sourceDirectory ? path.dirname(sourceDirectory) : path.join(root, "source-runs"), source = sourceDirectory ?? path.join(sourceRuns, "source-run");
  const record = { id: "source-run", runner: "pi", status: "running", transport: "process", sessionId: "2147483647", updatedAt: day };
  json(path.join(source, "status.json"), record);
  const provenance = { v: 1, channel: "fabric", via: "steer", sender: { id: "session:original", kind: "main", verified: "mesh" } };
  const admission = { id: "11111111-1111-4111-8111-111111111111", type: "steer", message: "resumed admission", provenance };
  const journal = Buffer.from(JSON.stringify(admission) + "\n");
  fs.writeFileSync(path.join(source, "steer.jsonl"), journal, { mode: 0o600 });
  const queued = { id: admission.id, state: "queued", delivery: "steer", provenance };
  json(path.join(source, "terminal-controls", admission.id + ".json"), queued);
  expect(queuedTerminalControls(source)).toEqual([queued]);
  initializeTaskControlDirectory(source);
  const sibling = taskControlDirectory(source), stat = fs.lstatSync(source, { bigint: true });
  const grant = fs.readFileSync(path.join(sibling, ".legacy-read.json"));
  expect(JSON.parse(grant.toString("utf8"))).toEqual({ version: 1, runId: path.basename(source), dev: String(stat.dev), ino: String(stat.ino) });
  settleTerminalControl(source, admission.id, "delivered");
  const delivered = { ...queued, state: "delivered" };
  expect(JSON.parse(fs.readFileSync(path.join(sibling, "terminal-controls", admission.id + ".json"), "utf8"))).toEqual(delivered);
  expect(queuedTerminalControls(source)).toEqual([]);
  const terminal = { ...record, status, finishedAt: day };
  json(path.join(source, "status.json"), terminal);
  const alias = "archive-alias", actor = { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: alias };
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: day });
  return { root, sourceRuns, source, sibling, record: terminal, admission, queued, delivered, journal, grant, alias, actor, store,
    archive: path.join(root, "actor", "runs", alias) };
};
const removalPaths = () => vi.mocked(verifiedRemove).mock.calls.map(([target]) => target);
const originalFiles = ["final-answer.json", "steer.jsonl", "terminal-controls/original-steer.json", "terminal-notices/original-steer.json.delivered"];
const addMigrationOverlay = (f: ReturnType<typeof setup>, source = f.source) => {
  const sibling = taskControlDirectory(source);
  json(path.join(source, "terminal-controls", f.admission.id + ".json"), { ...f.control, state: "queued", reason: undefined });
  const newer = { ...f.admission, id: "new-steer", message: "new sibling admission" };
  const journal = Buffer.from(JSON.stringify(newer) + "\n");
  fs.writeFileSync(path.join(sibling, "steer.jsonl"), journal, { mode: 0o600 });
  const delivered = { ...f.control, state: "delivered", reason: undefined };
  json(path.join(sibling, "terminal-controls", f.admission.id + ".json"), delivered);
  json(path.join(sibling, "terminal-controls", newer.id + ".json"), { ...delivered, id: newer.id });
  return { newer, journal, delivered };
};

describe("actor archive paired task controls", () => {
  it.each(["sibling-v1", "legacy", "migrated"] as const)("retainRun preserves original %s evidence under an alias, then selects paired collection", async layout => {
    const f = setup(layout);
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, f.source);
    expect(fs.readFileSync(path.join(f.archive, "status.json"))).toEqual(fs.readFileSync(path.join(f.source, "status.json")));
    for (const relative of originalFiles) {
      expect(fs.readFileSync(path.join(taskControlDirectory(f.archive), relative))).toEqual(fs.readFileSync(path.join(f.controls, relative)));
    }
    expect(readFinalAnswerReceipt(f.archive, f.record.id)).toEqual(f.receipt);
    expect(readFinalAnswerReceipt(f.archive, f.alias)).toBeUndefined();
    expect(fs.existsSync(path.join(taskControlDirectory(f.archive), ".legacy-read.json"))).toBe(false);
    expect(withLegacyTaskControlDirectory(f.archive, () => true)).toBeUndefined();
    if (process.platform !== "win32") expect(fs.statSync(taskControlDirectory(f.archive)).mode & 0o777).toBe(0o700);
    // Published centrally: no raw `<archive>.ctl` sibling in the task-visible runs tree.
    expect(fs.existsSync(legacyTaskControlSibling(f.archive))).toBe(false);
    expect(f.store.retainedRunIds(f.actor)).toEqual([f.alias]);
    expect(canRemoveTerminalRun(f.archive)).toBe(true);
    f.store.pruneRuns({ ...f.actor, lastRunId: "newest" }, 4 * day);
    expect(removalPaths()).toEqual([f.archive, taskControlDirectory(f.archive)]);
    // Recorded collection never changes either original or archive bytes.
    expect(fs.existsSync(f.source)).toBe(true);
    expect(fs.existsSync(f.archive)).toBe(true);
  });

  it("retains nested protected pairs and collects them with their enclosing archive", async () => {
    const f = setup(), child = setup();
    const nested = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.dirname(nested), { mode: 0o700 });
    // Move the real protected pair: cpSync creates its root with 0755 here,
    // which is correctly NOT a valid protected source sibling.
    fs.renameSync(child.source, nested);
    fs.renameSync(legacyTaskControlSibling(child.source), legacyTaskControlSibling(nested));
    if (process.platform !== "win32") expect(fs.statSync(legacyTaskControlSibling(nested)).mode & 0o777).toBe(0o700);
    expect(runTreeExitVeto(f.source, 0, undefined, true)).toBeUndefined();
    expect(readFinalAnswerReceipt(nested, child.record.id)).toEqual(child.receipt);
    expect(canRemoveTerminalRun(nested)).toBe(true);
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, f.source);
    const archivedChild = path.join(f.archive, "nested", "child");
    expect(readFinalAnswerReceipt(archivedChild, child.record.id)).toEqual(child.receipt);
    // The source's old nested sibling is not copied; the archived child is central.
    expect(fs.existsSync(legacyTaskControlSibling(archivedChild))).toBe(false);
    expect(canRemoveTerminalRun(f.archive)).toBe(true);
    f.store.pruneRuns({ ...f.actor, lastRunId: "newest" }, 4 * day);
    // Nested central controls live outside the tree, so they join the plan.
    expect(removalPaths()).toEqual([f.archive, taskControlDirectory(f.archive), taskControlDirectory(archivedChild)]);
  });

  it("preserves opaque nested bytes alongside a genuine protected run pair", async () => {
    const f = setup("legacy"), child = setup();
    const nested = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.dirname(nested), { mode: 0o700 });
    fs.renameSync(child.source, nested);
    fs.renameSync(legacyTaskControlSibling(child.source), legacyTaskControlSibling(nested));
    const opaque = Buffer.from([0, 255, 10, 128, 65]);
    fs.writeFileSync(path.join(f.source, "nested", "plain.txt"), opaque, { mode: 0o600 });
    await f.store.retainRun(f.actor, f.alias, f.source);
    expect(fs.readFileSync(path.join(f.archive, "nested", "plain.txt"))).toEqual(opaque);
    expect(fs.readFileSync(path.join(f.source, "nested", "plain.txt"))).toEqual(opaque);
    const archivedChild = path.join(f.archive, "nested", "child");
    for (const relative of originalFiles) {
      expect(fs.readFileSync(path.join(taskControlDirectory(archivedChild), relative))).toEqual(fs.readFileSync(path.join(legacyTaskControlSibling(nested), relative)));
    }
    expect(fs.readFileSync(path.join(archivedChild, "status.json"))).toEqual(fs.readFileSync(path.join(nested, "status.json")));
    expect(readFinalAnswerReceipt(archivedChild, child.record.id)).toEqual(child.receipt);
    expect(fs.existsSync(path.join(taskControlDirectory(archivedChild), ".legacy-read.json"))).toBe(false);
    expect(canRemoveTerminalRun(archivedChild)).toBe(true);
    // Opaque content is archive data, not new authority to collect its parent.
    expect(canRemoveTerminalRun(f.archive)).toBe(false);
    expect(removalPaths()).toEqual([]);
  });

  it.each(["root", "nested"] as const)("rejects copy failure when %s protected evidence must transfer", async location => {
    const f = setup(location === "nested" ? "legacy" : "sibling-v1"), child = setup();
    const nested = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.dirname(nested), { mode: 0o700 });
    if (location === "nested") {
      // Isolate the nested transfer veto: this legacy root has no controls.
      for (const name of ["final-answer.json", "steer.jsonl", "terminal-controls", "terminal-notices"]) {
        fs.renameSync(path.join(f.source, name), path.join(f.root, "saved-" + name));
      }
      fs.renameSync(child.source, nested);
      fs.renameSync(legacyTaskControlSibling(child.source), legacyTaskControlSibling(nested));
    } else fs.writeFileSync(path.join(f.source, "nested", "plain.txt"), "opaque");
    const prune = vi.spyOn(f.store, "pruneRuns");
    vi.spyOn(fs, "cpSync").mockImplementation(() => { throw new Error("nested unavailable"); });
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow("nested unavailable");
    expect(prune).not.toHaveBeenCalled();
    expect(fs.existsSync(f.source)).toBe(true);
    expect(removalPaths()).toEqual([]);
  });

  it("normalizes mixed journals legacy-first and only the matching delivered overlay", async () => {
    const f = setup("migrated"), mixed = addMigrationOverlay(f);
    const oldJournal = fs.readFileSync(path.join(f.source, "steer.jsonl"));
    const oldControl = fs.readFileSync(path.join(f.source, "terminal-controls", f.admission.id + ".json"));
    // A matching notice copied in both namespaces is the same original sender ACK.
    json(path.join(taskControlDirectory(f.source), "terminal-notices", f.admission.id + ".json.delivered"), f.notice);
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, f.source);
    expect(fs.readFileSync(path.join(taskControlDirectory(f.archive), "steer.jsonl"))).toEqual(Buffer.concat([oldJournal, mixed.journal]));
    expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(f.archive), "terminal-controls", f.admission.id + ".json"), "utf8"))).toEqual(mixed.delivered);
    expect(fs.readFileSync(path.join(f.source, "terminal-controls", f.admission.id + ".json"))).toEqual(oldControl);
    expect(fs.readFileSync(path.join(f.archive, "status.json"))).toEqual(fs.readFileSync(path.join(f.source, "status.json")));
    expect(readFinalAnswerReceipt(f.archive, f.record.id)).toEqual(f.receipt);
    expect(canRemoveTerminalRun(f.archive)).toBe(true);
    f.store.pruneRuns({ ...f.actor, lastRunId: "newest" }, 4 * day);
    expect(removalPaths()).toEqual([f.archive, taskControlDirectory(f.archive)]);
  });

  it("recursively promotes nested migrated evidence under aliases without transporting grants", async () => {
    const f = setup(), child = setup("legacy"), grandchild = setup("legacy");
    const nested = path.join(f.source, "nested", "child-alias"), deep = path.join(nested, "nested", "deep-alias");
    fs.mkdirSync(path.dirname(nested), { recursive: true, mode: 0o700 });
    fs.cpSync(child.source, nested, { recursive: true });
    // Actual legacy bootstrap must accept an old record under an archive alias,
    // without runner metadata. Do not manufacture or rebind a marker in fixtures.
    const { runner: _runner, ...oldRecord } = child.record;
    json(path.join(nested, "status.json"), oldRecord);
    initializeTaskControlDirectory(nested);
    const mixed = addMigrationOverlay(child, nested);
    fs.mkdirSync(path.dirname(deep), { mode: 0o700 });
    fs.cpSync(grandchild.source, deep, { recursive: true });
    initializeTaskControlDirectory(deep);
    const grant = fs.readFileSync(path.join(taskControlDirectory(nested), ".legacy-read.json"));
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, f.source);
    for (const source of [nested, deep]) {
      const archived = path.join(f.archive, path.relative(f.source, source));
      expect(fs.readFileSync(path.join(archived, "status.json"))).toEqual(fs.readFileSync(path.join(source, "status.json")));
      expect(readFinalAnswerReceipt(archived, child.record.id)).toEqual(child.receipt);
      expect(fs.existsSync(path.join(taskControlDirectory(archived), ".legacy-read.json"))).toBe(false);
      expect(withLegacyTaskControlDirectory(archived, () => true)).toBeUndefined();
      expect(fs.existsSync(path.join(archived, "final-answer.json"))).toBe(false);
      expect(canRemoveTerminalRun(archived)).toBe(true);
    }
    expect(fs.readFileSync(path.join(taskControlDirectory(nested), ".legacy-read.json"))).toEqual(grant);
    expect(fs.readFileSync(path.join(taskControlDirectory(f.archive), "final-answer.json"))).toEqual(fs.readFileSync(path.join(f.controls, "final-answer.json")));
    expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(path.join(f.archive, "nested", "child-alias")), "terminal-controls", child.admission.id + ".json"), "utf8"))).toEqual(mixed.delivered);
    expect(canRemoveTerminalRun(f.archive)).toBe(true);
    f.store.pruneRuns({ ...f.actor, lastRunId: "newest" }, 4 * day);
    const archivedNested = path.join(f.archive, "nested", "child-alias");
    expect(removalPaths()).toEqual([f.archive, taskControlDirectory(f.archive), taskControlDirectory(archivedNested),
      taskControlDirectory(path.join(archivedNested, "nested", "deep-alias"))]);
  });

  it.each(["stopped", "failed"] as const)("archives real consumed legacy %s no-answer overlays, including nested aliases, and collects both pairs", async status => {
    const f = setupNoAnswer(status), nested = path.join(f.source, "nested", "child-alias");
    const child = setupNoAnswer(status, nested);
    expect(runTreeExitVeto(f.source, 0, undefined, true)).toBeUndefined();
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, f.source);
    for (const source of [f.source, nested]) {
      const archive = path.join(f.archive, path.relative(f.source, source)), ctl = taskControlDirectory(archive);
      expect(fs.readFileSync(path.join(archive, "status.json"))).toEqual(fs.readFileSync(path.join(source, "status.json")));
      expect(JSON.parse(fs.readFileSync(path.join(archive, "status.json"), "utf8")).id).toBe(f.record.id);
      expect(fs.readFileSync(path.join(ctl, "steer.jsonl"))).toEqual(f.journal);
      expect(JSON.parse(fs.readFileSync(path.join(ctl, "terminal-controls", f.admission.id + ".json"), "utf8"))).toEqual(f.delivered);
      expect(JSON.parse(fs.readFileSync(path.join(source, "terminal-controls", f.admission.id + ".json"), "utf8"))).toEqual(f.queued);
      expect(fs.readFileSync(path.join(taskControlDirectory(source), ".legacy-read.json"))).toEqual(source === f.source ? f.grant : child.grant);
      expect(fs.existsSync(path.join(ctl, ".legacy-read.json"))).toBe(false);
      expect(withLegacyTaskControlDirectory(archive, () => true)).toBeUndefined();
      expect(fs.existsSync(path.join(archive, "final-answer.json"))).toBe(false);
      expect(fs.existsSync(path.join(ctl, "final-answer.json"))).toBe(false);
      expect(readFinalAnswerReceipt(archive, f.record.id)).toBeUndefined();
      expect(canRemoveTerminalRun(source)).toBe(true);
      expect(canRemoveTerminalRun(archive)).toBe(true);
    }
    expect(pruneActorRunArchives({ runsDirectory: f.sourceRuns, retentionMs: day, now: 4 * day, dryRun: true })).toEqual([f.source]);
    expect(pruneActorRunArchives({ runsDirectory: path.dirname(f.archive), retentionMs: day, now: 4 * day, dryRun: true })).toEqual([f.archive]);
    // f.sibling is the source's CENTRAL bootstrap; nested centrals sit outside each tree.
    expect(removalPaths()).toEqual([f.source, f.sibling, taskControlDirectory(nested),
      f.archive, taskControlDirectory(f.archive), taskControlDirectory(path.join(f.archive, "nested", "child-alias"))]);
    expect(vi.mocked(verifiedRemove).mock.calls.every(([, , options]) => options.dryRun === true)).toBe(true);
    expect(fs.existsSync(f.source)).toBe(true);
    expect(fs.existsSync(f.archive)).toBe(true);
  });

  it.each(["claimed-missing-receipt", "source-sender", "source-id", "source-delivery", "overlay-sender", "overlay-id", "overlay-delivery",
    "source-refused", "source-unknown", "overlay-queued", "source-extra-field", "notice", "duplicate-notice", "unknown-artifact"] as const)("refuses no-answer %s without publishing archive authority", async bad => {
    const f = setupNoAnswer("stopped");
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
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(fs.readFileSync(path.join(f.sibling, ".legacy-read.json"))).toEqual(f.grant);
    expect(removalPaths()).toEqual([]);
  });

  it("counts original artifact bytes before collapsing matching overlays", async () => {
    const f = setup("migrated"); addMigrationOverlay(f);
    for (let index = 0; index < 70; index++) {
      const admission = { ...f.admission, id: `large-${index}` };
      fs.appendFileSync(path.join(f.source, "steer.jsonl"), JSON.stringify(admission) + "\n");
      const queued = JSON.stringify({ ...f.control, id: admission.id, state: "queued", reason: undefined });
      fs.writeFileSync(path.join(f.source, "terminal-controls", admission.id + ".json"), queued.padEnd(MAX_TASK_CONTROL_BYTES - 1, " "), { mode: 0o600 });
      json(path.join(taskControlDirectory(f.source), "terminal-controls", admission.id + ".json"), { ...f.control, id: admission.id, state: "delivered", reason: undefined });
    }
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow("byte bound exceeded");
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(removalPaths()).toEqual([]);
  });

  it("keeps the durable route receipt and untouched source when archival completes", async () => {
    const f = setup("sibling-v1", "source-run");
    const dispatch = { runId: f.alias, ledger: path.join(f.root, "ledger.jsonl"), decision: { decisionId: "dispatch", routeClass: "bounded-lookup", mode: "live" } };
    json(path.join(f.source, "route-dispatch-receipt.json"), dispatch);
    const sync = vi.spyOn(fs, "fsyncSync");
    await f.store.retainRun(f.actor, f.alias, f.source);
    expect(JSON.parse(fs.readFileSync(path.join(f.archive, "route-dispatch-receipt.json"), "utf8"))).toEqual(dispatch);
    expect(JSON.parse(fs.readFileSync(path.join(f.source, "route-dispatch-receipt.json"), "utf8"))).toEqual(dispatch);
    expect(sync).toHaveBeenCalled();
    expect(canRemoveTerminalRun(f.archive)).toBe(true);
  });

  it("counts runs once: skips only validated old siblings and keeps unknown .ctl entries visible", async () => {
    const f = setup(); await f.store.retainRun(f.actor, f.alias, f.source);
    const runs = path.dirname(f.archive);
    fs.mkdirSync(path.join(runs, "old-run"), { mode: 0o700 });
    fs.mkdirSync(legacyTaskControlSibling(path.join(runs, "old-run")), { mode: 0o700 });
    fs.mkdirSync(path.join(runs, "stray.ctl"), { mode: 0o700 });
    expect(f.store.retainedRunIds(f.actor)).toEqual([f.alias, "old-run", "stray.ctl"]);
  });

  it("preserves the late-reference fence and leaves the paired controls untouched", async () => {
    const f = setup(); await f.store.retainRun(f.actor, f.alias, f.source);
    expect(pruneActorRunArchives({ runsDirectory: path.dirname(f.archive), retentionMs: day, now: 4 * day,
      isRetained: (_id, final) => final === true })).toEqual([]);
    expect(removalPaths()).toEqual([]);
    expect(fs.readFileSync(path.join(taskControlDirectory(f.archive), "final-answer.json"), "utf8")).toContain(f.receipt.text);
  });

  it.each(["unknown", "hardlink", "subdirectory-link", "file-link", "oversized", "entry-bound", "missing", "foreign"] as const)("refuses unsafe %s protected controls without copying them or removing source", async bad => {
    const f = setup();
    if (bad === "unknown") fs.writeFileSync(path.join(f.controls, "arbitrary-private-data"), "not an archive artifact");
    if (bad === "hardlink") fs.linkSync(path.join(f.controls, "final-answer.json"), path.join(f.root, "extra-link"));
    if (bad === "subdirectory-link") {
      const source = path.join(f.controls, "terminal-controls"); fs.renameSync(source, source + "-original");
      fs.symlinkSync(source + "-original", source, "dir");
    }
    if (bad === "file-link") {
      const file = path.join(f.controls, "final-answer.json"); fs.renameSync(file, path.join(f.root, "linked-answer"));
      fs.symlinkSync(path.join(f.root, "linked-answer"), file);
    }
    if (bad === "oversized") fs.writeFileSync(path.join(f.controls, "final-answer.json"), Buffer.alloc(MAX_FINAL_ANSWER_BYTES + 1));
    if (bad === "entry-bound") {
      for (let index = 0; index < 770; index++) json(path.join(f.controls, "terminal-controls", `extra-${index}.json`), { id: `extra-${index}` });
    }
    if (bad === "missing") fs.renameSync(f.controls, f.controls + "-missing");
    if (bad === "foreign") {
      const identity = fs.statSync(f.controls), native = fs.fstatSync;
      vi.spyOn(fs, "fstatSync").mockImplementation(((...args: Parameters<typeof native>) => {
        const stat = native(...args);
        if (String(stat.dev) !== String(identity.dev) || String(stat.ino) !== String(identity.ino)) return stat;
        return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: typeof stat.uid === "bigint" ? BigInt((process.getuid?.() ?? 0) + 1) : (process.getuid?.() ?? 0) + 1 });
      }) as typeof native);
    }
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(fs.existsSync(path.join(f.source, "status.json"))).toBe(true);
    expect(removalPaths()).toEqual([]);
  });

  it("rejects a renamed/replaced archive run during control creation even without a route receipt", async () => {
    const f = setup(), mkdir = fs.mkdirSync, central = path.basename(taskControlDirectory(f.archive));
    let swapped = false;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((...args: Parameters<typeof mkdir>) => {
      const result = mkdir(...args);
      if (!swapped && path.basename(String(args[0])) === central) {
        swapped = true;
        fs.renameSync(f.archive, f.archive + "-held");
        mkdir(f.archive, { mode: 0o700 });
      }
      return result;
    }) as typeof mkdir);
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow("changed while it was held");
    expect(swapped).toBe(true);
    expect(fs.readdirSync(f.archive)).toEqual([]);
    expect(fs.existsSync(path.join(f.archive + "-held", "status.json"))).toBe(false);
    expect(fs.existsSync(path.join(taskControlDirectory(f.archive), "final-answer.json"))).toBe(false);
    expect(fs.existsSync(f.source)).toBe(true);
    expect(removalPaths()).toEqual([]);
  });

  it("does not heal or follow an unsafe destination control sibling", async () => {
    const f = setup(), target = path.join(f.root, "unrelated"); fs.mkdirSync(target, { mode: 0o700 });
    fs.mkdirSync(path.dirname(f.archive), { recursive: true, mode: 0o700 });
    fs.mkdirSync(path.dirname(taskControlDirectory(f.archive)), { recursive: true, mode: 0o700 });
    fs.symlinkSync(target, taskControlDirectory(f.archive), "dir");
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.readdirSync(target)).toEqual([]);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(removalPaths()).toEqual([]);
  });

  it.each(["forged-marker", "mismatched-sender", "mismatched-delivery", "mismatched-id", "old-mismatch", "duplicate-answer", "duplicate-admission", "mismatched-notice", "missing-answer", "incomplete-journal"] as const)("does not turn %s legacy evidence into archive authority", async bad => {
    const f = setup("migrated"), mixed = addMigrationOverlay(f);
    const sibling = taskControlDirectory(f.source), overlay = path.join(sibling, "terminal-controls", f.admission.id + ".json");
    if (bad === "forged-marker") json(path.join(sibling, ".legacy-read.json"), { version: 1, runId: f.record.id, dev: "0", ino: "0" });
    if (bad === "mismatched-sender") json(overlay, { ...mixed.delivered, provenance: { ...f.admission.provenance, sender: { ...f.admission.provenance.sender, id: "session:forged" } } });
    if (bad === "mismatched-delivery") json(overlay, { ...mixed.delivered, delivery: "followUp" });
    if (bad === "mismatched-id") json(overlay, { ...mixed.delivered, id: "other-steer" });
    if (bad === "old-mismatch") json(path.join(f.source, "terminal-controls", f.admission.id + ".json"), { ...f.control, state: "queued", reason: undefined, id: "other-steer" });
    if (bad === "duplicate-answer") json(path.join(sibling, "final-answer.json"), f.receipt);
    if (bad === "duplicate-admission") fs.writeFileSync(path.join(sibling, "steer.jsonl"), JSON.stringify(f.admission) + "\n");
    if (bad === "mismatched-notice") json(path.join(sibling, "terminal-notices", f.admission.id + ".json.delivered"), { ...f.notice, finalAnswerReceiptId: "other-answer" });
    if (bad === "missing-answer") fs.renameSync(path.join(f.source, "final-answer.json"), path.join(f.root, "original-answer.json"));
    if (bad === "incomplete-journal") fs.writeFileSync(path.join(f.source, "steer.jsonl"), JSON.stringify(f.admission));
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.readFileSync(bad === "missing-answer" ? path.join(f.root, "original-answer.json") : path.join(f.source, "final-answer.json"), "utf8")).toContain(f.receipt.text);
    expect(removalPaths()).toEqual([]);
  });

  it.each(["forged-marker", "duplicate-answer", "mismatched-overlay", "unknown", "missing"] as const)("refuses nested migrated %s before publishing root controls or status", async bad => {
    const f = setup(), child = setup("legacy"), nested = path.join(f.source, "nested", "child-alias");
    fs.mkdirSync(path.dirname(nested), { mode: 0o700 });
    fs.cpSync(child.source, nested, { recursive: true });
    initializeTaskControlDirectory(nested);
    const sibling = taskControlDirectory(nested), mixed = addMigrationOverlay(child, nested);
    if (bad === "forged-marker") json(path.join(sibling, ".legacy-read.json"), { version: 1, runId: "child-alias", dev: "0", ino: "0" });
    if (bad === "duplicate-answer") json(path.join(sibling, "final-answer.json"), child.receipt);
    if (bad === "mismatched-overlay") json(path.join(sibling, "terminal-controls", child.admission.id + ".json"), { ...mixed.delivered, id: "other" });
    if (bad === "unknown") fs.writeFileSync(path.join(sibling, "unknown"), "not terminal evidence");
    if (bad === "missing") {
      json(path.join(nested, "status.json"), { ...child.record, taskControlLayout: "sibling-v1" });
      fs.renameSync(sibling, sibling + "-missing");
    }
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(fs.existsSync(nested)).toBe(true);
    expect(removalPaths()).toEqual([]);
  });

  // Real filesystem tri-layout: in-run legacy -> old bound sibling-v1 -> central.
  const triLayout = (options: { bound?: boolean; siblingAnswer?: boolean } = {}) => {
    const f = setup("legacy"), sibling = legacyTaskControlSibling(f.source);
    json(path.join(f.source, "terminal-controls", f.admission.id + ".json"), { ...f.control, state: "queued", reason: undefined });
    fs.mkdirSync(sibling, { mode: 0o700 });
    const stat = fs.lstatSync(f.source, { bigint: true });
    if (options.bound !== false) json(path.join(sibling, ".legacy-read.json"), { version: 1, runId: path.basename(f.source), dev: String(stat.dev), ino: String(stat.ino) });
    const row = (id: string, message: string) => Buffer.from(JSON.stringify({ ...f.admission, id, message }) + "\n");
    const siblingJournal = row("sibling-steer", "old sibling admission"), centralJournal = row("central-steer", "central admission");
    fs.writeFileSync(path.join(sibling, "steer.jsonl"), siblingJournal, { mode: 0o600 });
    const delivered = { ...f.control, state: "delivered", reason: undefined };
    json(path.join(sibling, "terminal-controls", f.admission.id + ".json"), delivered);
    if (options.siblingAnswer) json(path.join(sibling, "final-answer.json"), f.receipt);
    json(path.join(f.source, "status.json"), { ...f.record, taskControlLayout: "sibling-v1" });
    initializeTaskControlDirectory(f.source);
    fs.writeFileSync(path.join(taskControlDirectory(f.source), "steer.jsonl"), centralJournal, { mode: 0o600 });
    const snapshot = (directory: string) => fs.readdirSync(directory, { recursive: true, withFileTypes: true }).filter(entry => entry.isFile())
      .map(entry => [path.relative(directory, path.join(entry.parentPath, entry.name)), fs.readFileSync(path.join(entry.parentPath, entry.name)).toString("base64")]).sort();
    return { ...f, sibling, siblingJournal, centralJournal, delivered, snapshot };
  };

  it("tri-layout archive retains in-run, old sibling, then central rows chronologically with the legitimate overlay", async () => {
    const t = triLayout(), inRunJournal = fs.readFileSync(path.join(t.source, "steer.jsonl"));
    const before = [t.snapshot(t.source), t.snapshot(t.sibling)];
    await t.store.retainRun(t.actor, t.alias, t.source);
    const archived = taskControlDirectory(t.archive);
    expect(fs.readFileSync(path.join(archived, "steer.jsonl"))).toEqual(Buffer.concat([inRunJournal, t.siblingJournal, t.centralJournal]));
    expect(JSON.parse(fs.readFileSync(path.join(archived, "terminal-controls", t.admission.id + ".json"), "utf8"))).toEqual(t.delivered);
    expect(JSON.parse(fs.readFileSync(path.join(archived, "terminal-notices", t.admission.id + ".json.delivered"), "utf8"))).toEqual(t.notice);
    expect(readFinalAnswerReceipt(t.archive, t.record.id)).toEqual(t.receipt);
    // Archival never rewrites the in-run or old sibling layers.
    expect([t.snapshot(t.source), t.snapshot(t.sibling)]).toEqual(before);
  });

  it.each(["duplicate-result", "unbound"] as const)("tri-layout archive refuses a %s chain without dropping in-run evidence", async bad => {
    const t = triLayout(bad === "unbound" ? { bound: false } : { siblingAnswer: true });
    const before = t.snapshot(t.source);
    await expect(t.store.retainRun(t.actor, t.alias, t.source)).rejects.toThrow(bad === "unbound" ? "Unproven legacy archive controls" : "Conflicting legacy archive control");
    expect(fs.existsSync(path.join(t.archive, "status.json"))).toBe(false);
    expect(t.snapshot(t.source)).toEqual(before);
    expect(removalPaths()).toEqual([]);
  });
});

// DELTA-SEC-01 cutoff (#12214): existing raw old/v1 history is ALWAYS unknown.
// No generic seed producer is assumed here; only current-only v2 and refusal.
const bytesOf = (...directories: string[]) => directories.filter(directory => fs.existsSync(directory)).flatMap(directory =>
  fs.readdirSync(directory, { recursive: true, withFileTypes: true }).filter(entry => entry.isFile())
    .map(entry => [path.join(entry.parentPath, entry.name), fs.readFileSync(path.join(entry.parentPath, entry.name)).toString("base64")]))
  .sort(([a], [b]) => a!.localeCompare(b!));
describe("raw legacy cutoff: unknown history refuses, every byte preserved", () => {
  it.each(["sibling-v1", "legacy"] as const)("refuses %s raw history at bootstrap, read, archive and collection", async layout => {
    const f = setup(layout);
    const before = bytesOf(f.source, legacyTaskControlSibling(f.source), taskControlDirectory(f.source));
    expect(() => initializeTaskControlDirectory(f.source)).toThrow("FABRIC_LEGACY_SNAPSHOT_REQUIRED");
    expect(() => readFinalAnswerReceipt(f.source, f.record.id)).toThrow();
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    expect(removalPaths()).toEqual([]);
    expect(bytesOf(f.source, legacyTaskControlSibling(f.source), taskControlDirectory(f.source))).toEqual(before);
  });
  it("refuses a pre-existing central with a v1 binding instead of publishing its single answer", async () => {
    const f = setup("legacy");
    const central = taskControlDirectory(f.source);
    fs.mkdirSync(central, { recursive: true, mode: 0o700 });
    const stat = fs.lstatSync(f.source, { bigint: true });
    json(path.join(central, ".legacy-read.json"), { version: 1, runId: "source-run", dev: String(stat.dev), ino: String(stat.ino) });
    const before = bytesOf(f.source, central);
    expect(() => readFinalAnswerReceipt(f.source, f.record.id)).toThrow();
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    expect(removalPaths()).toEqual([]);
    expect(bytesOf(f.source, central)).toEqual(before);
  });
  it("archive publishes its own v2 current-only binding, never the source binding or a snapshot", async () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "task-control-archive-v2-")); roots.push(root);
    const source = path.join(root, "source-run");
    createHostTaskRun(source); // exclusive fresh run + null central at the final lexical path
    const receipt = saveFinalAnswerReceipt(source, "source-run", "current answer");
    json(path.join(source, "status.json"), { id: "source-run", runner: "pi", status: "completed", transport: "process", sessionId: "2147483647",
      finishedAt: day, taskControlLayout: "central-v1", finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: day });
    const actor = { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: "archive" }, archive = path.join(root, "actor", "runs", "archive");
    await store.retainRun(actor, "archive", source);
    const held = fs.lstatSync(archive, { bigint: true });
    expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(archive), ".legacy-read.json"), "utf8"))).toEqual({
      version: 2, runId: "archive", dev: String(held.dev), ino: String(held.ino), snapshot: null });
    expect(fs.existsSync(path.join(taskControlDirectory(archive), ".legacy-snapshot"))).toBe(false);
    expect(readFinalAnswerReceipt(archive, "source-run")).toEqual(receipt);
  });
});
