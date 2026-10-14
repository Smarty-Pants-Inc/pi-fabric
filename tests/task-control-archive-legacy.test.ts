import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import { canRemoveTerminalRun } from "../src/storage/retention.js";
import { appendTerminalAdmission } from "../src/agents/terminal-notices.js";
import { settleTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";
import { createHostTaskRun, initializeTaskControlDirectory, legacyTaskControlSibling, saveFinalAnswerReceipt, readFinalAnswerReceipt, taskControlDirectory, withLegacyTaskControlDirectory } from "../src/worker/terminal-answer.js";
import { addMigrationOverlay, day, json, registerArchiveCleanup, removalPaths, roots, setup } from "./task-control-archive-fixtures.js";

// Keep reason: trust boundary / data safety. Exercise the actual archive and
// collection judgment, recording only the last native removal boundary.
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: vi.fn(() => ({ removed: true, bytes: 0 })) };
});
registerArchiveCleanup();

// DELTA-SEC-01 cutoff (#12214): raw old/v1/unbound history is ALWAYS refused and
// byte-preserved. Faults apply to RAW history first; snapshots are taken after.
const LEGACY = "FABRIC_LEGACY_SNAPSHOT_REQUIRED";
const filesOf = (...directories: string[]) => directories.filter(directory => fs.existsSync(directory)).flatMap(directory => fs.statSync(directory).isFile()
  ? [[directory, fs.readFileSync(directory).toString("base64")]] : fs.readdirSync(directory, { recursive: true, withFileTypes: true }).filter(entry => entry.isFile())
    .map(entry => [path.join(entry.parentPath, entry.name), fs.readFileSync(path.join(entry.parentPath, entry.name)).toString("base64")]))
  .sort(([a], [b]) => a!.localeCompare(b!));
// Current-v2 seed ONLY through host writers at the final lexical path (never a fabricated binding).
const seedCurrent = (run: string, runner = true) => {
  const id = path.basename(run), messageId = "11111111-1111-4111-8111-111111111111";
  createHostTaskRun(run);
  const sender = { id: "session:original", kind: "main", verified: "mesh" } as const;
  const provenance = { v: 1, channel: "fabric", via: "steer", sender } as const;
  appendTerminalAdmission(run, JSON.stringify({ id: messageId, type: "steer", message: "retained input", ts: day, provenance }) + "\n");
  trackTerminalControl(run, { id: messageId, delivery: "steer", provenance });
  settleTerminalControl(run, messageId, "delivered");
  // Retention ages by receipt recordedAt (retention.ts recordAgeReference); pin the genuine writer's clock to `day`.
  const clock = vi.spyOn(Date, "now").mockReturnValue(day);
  let receipt: ReturnType<typeof saveFinalAnswerReceipt>;
  try { receipt = saveFinalAnswerReceipt(run, id, "current answer " + id); } finally { clock.mockRestore(); }
  json(path.join(run, "status.json"), { id, ...(runner ? { runner: "pi" } : {}), status: "completed", transport: "process", sessionId: "2147483647",
    finishedAt: day, finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
  const control = JSON.parse(fs.readFileSync(path.join(taskControlDirectory(run), "terminal-controls", messageId + ".json"), "utf8"));
  return { id, receipt, control, messageId };
};
const ownBinding = (archived: string) => {
  const held = fs.lstatSync(archived, { bigint: true });
  return { version: 2, runId: path.basename(archived), dev: String(held.dev), ino: String(held.ino), snapshot: null };
};

describe("actor archive paired task controls", () => {
  // Cutoff #12214: mixed raw journals + unbound central overlay are unknown history, never merged.
  it("normalizes mixed journals legacy-first and only the matching delivered overlay", async () => {
    const f = setup("migrated");
    addMigrationOverlay(f);
    // A matching notice copied in both namespaces is the same original sender ACK.
    json(path.join(taskControlDirectory(f.source), "terminal-notices", f.admission.id + ".json.delivered"), f.notice);
    const layers = [f.source, legacyTaskControlSibling(f.source), taskControlDirectory(f.source)], before = filesOf(...layers);
    expect(() => initializeTaskControlDirectory(f.source)).toThrow(LEGACY);
    expect(() => readFinalAnswerReceipt(f.source, f.record.id)).toThrow(LEGACY);
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow(LEGACY);
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(removalPaths()).toEqual([]);
    expect(filesOf(...layers)).toEqual(before);
  });

  // Cutoff #12214: recursion is retained on genuine current-v2 runs seeded at their final
  // nested lexical paths (no legacy promotion); the archive mints its own bindings.
  it("recursively promotes nested migrated evidence under aliases without transporting grants", async () => {
    const f = setup();
    const root = path.join(f.root, "current"), source = path.join(root, "source-run");
    const nested = path.join(source, "nested", "child-alias"), deep = path.join(nested, "nested", "deep-alias");
    fs.mkdirSync(root, { mode: 0o700 });
    const top = seedCurrent(source);
    // A current record under an archive alias without runner metadata stays opaque status.
    const seeded = new Map([[nested, seedCurrent(nested, false)], [deep, seedCurrent(deep)]]);
    const grants = [source, nested, deep].map(run => fs.readFileSync(path.join(taskControlDirectory(run), ".legacy-read.json")));
    expect(canRemoveTerminalRun(source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, source);
    for (const [run, child] of seeded) {
      const archived = path.join(f.archive, path.relative(source, run));
      expect(fs.readFileSync(path.join(archived, "status.json"))).toEqual(fs.readFileSync(path.join(run, "status.json")));
      expect(readFinalAnswerReceipt(archived, child.id)).toEqual(child.receipt);
      expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(archived), ".legacy-read.json"), "utf8"))).toEqual(ownBinding(archived));
      expect(fs.existsSync(path.join(taskControlDirectory(archived), ".legacy-snapshot"))).toBe(false);
      expect(fs.existsSync(path.join(archived, ".legacy-read.json"))).toBe(false);
      expect(withLegacyTaskControlDirectory(archived, () => true)).toBeUndefined();
      expect(fs.existsSync(path.join(archived, "final-answer.json"))).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(archived), "terminal-controls", child.messageId + ".json"), "utf8"))).toEqual(child.control);
      expect(canRemoveTerminalRun(archived)).toBe(true);
    }
    expect([source, nested, deep].map(run => fs.readFileSync(path.join(taskControlDirectory(run), ".legacy-read.json")))).toEqual(grants);
    expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(f.archive), ".legacy-read.json"), "utf8"))).toEqual(ownBinding(f.archive));
    expect(fs.readFileSync(path.join(taskControlDirectory(f.archive), "final-answer.json"))).toEqual(fs.readFileSync(path.join(taskControlDirectory(source), "final-answer.json")));
    expect(readFinalAnswerReceipt(f.archive, top.id)).toEqual(top.receipt);
    expect(canRemoveTerminalRun(f.archive)).toBe(true);
    f.store.pruneRuns({ ...f.actor, lastRunId: "newest" }, 4 * day);
    const archivedNested = path.join(f.archive, "nested", "child-alias");
    expect(removalPaths()).toEqual([f.archive, taskControlDirectory(f.archive), taskControlDirectory(archivedNested),
      taskControlDirectory(path.join(archivedNested, "nested", "deep-alias"))]);
  });

  it.each(["forged-marker", "mismatched-sender", "mismatched-delivery", "mismatched-id", "old-mismatch", "duplicate-answer", "duplicate-admission", "mismatched-notice", "missing-answer", "incomplete-journal"] as const)("does not turn %s legacy evidence into archive authority", async bad => {
    // Cutoff #12214: faults apply to RAW migrated history (no bootstrap); refusal is the legacy cutoff.
    const f = setup("migrated"), mixed = addMigrationOverlay(f);
    const sibling = taskControlDirectory(f.source), overlay = path.join(sibling, "terminal-controls", f.admission.id + ".json");
    if (bad === "forged-marker") json(path.join(sibling, ".legacy-read.json"), { version: 1, runId: f.record.id, dev: "0", ino: "0" });
    if (bad === "mismatched-sender") json(overlay, { ...mixed.delivered, provenance: { ...f.admission.provenance, sender: { ...f.admission.provenance.sender, id: "session:forged" } } });
    if (bad === "mismatched-delivery") json(overlay, { ...mixed.delivered, delivery: "followUp" });
    if (bad === "mismatched-id") json(overlay, { ...mixed.delivered, id: "other-steer" });
    if (bad === "old-mismatch") json(path.join(f.source, "terminal-controls", f.admission.id + ".json"), { ...f.control, state: "queued", reason: undefined, id: "other-steer" });
    // Raw migrated history now lives in the unbound central (f.controls); the second layer is in-run.
    if (bad === "duplicate-answer") json(path.join(f.source, "final-answer.json"), f.receipt);
    if (bad === "duplicate-admission") fs.writeFileSync(path.join(f.source, "steer.jsonl"), JSON.stringify(f.admission) + "\n");
    if (bad === "mismatched-notice") json(path.join(sibling, "terminal-notices", f.admission.id + ".json.delivered"), { ...f.notice, finalAnswerReceiptId: "other-answer" });
    if (bad === "missing-answer") fs.renameSync(path.join(f.controls, "final-answer.json"), path.join(f.root, "original-answer.json"));
    if (bad === "incomplete-journal") fs.writeFileSync(path.join(f.controls, "steer.jsonl"), JSON.stringify(f.admission));
    const layers = [f.source, legacyTaskControlSibling(f.source), sibling, path.join(f.root, "original-answer.json")], before = filesOf(...layers);
    expect(() => initializeTaskControlDirectory(f.source)).toThrow(LEGACY);
    expect(() => readFinalAnswerReceipt(f.source, f.record.id)).toThrow(LEGACY);
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow(LEGACY);
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(fs.readFileSync(bad === "missing-answer" ? path.join(f.root, "original-answer.json") : path.join(f.controls, "final-answer.json"), "utf8")).toContain(f.receipt.text);
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    expect(removalPaths()).toEqual([]);
    expect(filesOf(...layers)).toEqual(before);
  });

  it.each(["forged-marker", "duplicate-answer", "mismatched-overlay", "unknown", "missing"] as const)("refuses nested migrated %s before publishing root controls or status", async bad => {
    // Cutoff #12214: a CURRENT root holds a copied RAW nested child (no bootstrap); faults apply first.
    const f = setup(), child = setup("legacy"), root = path.join(f.root, "current"), source = path.join(root, "source-run");
    const nested = path.join(source, "nested", "child-alias");
    fs.mkdirSync(root, { mode: 0o700 });
    const top = seedCurrent(source);
    fs.mkdirSync(path.dirname(nested), { mode: 0o700 });
    fs.cpSync(child.source, nested, { recursive: true });
    const sibling = taskControlDirectory(nested), mixed = addMigrationOverlay(child, nested);
    if (bad === "forged-marker") json(path.join(sibling, ".legacy-read.json"), { version: 1, runId: "child-alias", dev: "0", ino: "0" });
    if (bad === "duplicate-answer") json(path.join(sibling, "final-answer.json"), child.receipt);
    if (bad === "mismatched-overlay") json(path.join(sibling, "terminal-controls", child.admission.id + ".json"), { ...mixed.delivered, id: "other" });
    if (bad === "unknown") fs.writeFileSync(path.join(sibling, "unknown"), "not terminal evidence");
    if (bad === "missing") {
      json(path.join(nested, "status.json"), { ...child.record, taskControlLayout: "sibling-v1" });
      fs.renameSync(sibling, sibling + "-missing");
    }
    const layers = [source, taskControlDirectory(source), sibling, sibling + "-missing", child.source], before = filesOf(...layers);
    expect(() => initializeTaskControlDirectory(nested)).toThrow(LEGACY);
    expect(() => readFinalAnswerReceipt(nested, child.record.id)).toThrow(LEGACY);
    await expect(f.store.retainRun(f.actor, f.alias, source)).rejects.toThrow(LEGACY);
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "nested"))).toBe(false);
    expect(fs.existsSync(nested)).toBe(true);
    expect(readFinalAnswerReceipt(source, top.id)).toEqual(top.receipt);
    expect(removalPaths()).toEqual([]);
    expect(filesOf(...layers)).toEqual(before);
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
    // Raw central layer (no bootstrap): cutoff #12214 never mints authority for it.
    fs.mkdirSync(taskControlDirectory(f.source), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(taskControlDirectory(f.source), "steer.jsonl"), centralJournal, { mode: 0o600 });
    const snapshot = (directory: string) => fs.readdirSync(directory, { recursive: true, withFileTypes: true }).filter(entry => entry.isFile())
      .map(entry => [path.relative(directory, path.join(entry.parentPath, entry.name)), fs.readFileSync(path.join(entry.parentPath, entry.name)).toString("base64")]).sort();
    return { ...f, sibling, siblingJournal, centralJournal, delivered, snapshot };
  };

  // Cutoff #12214: the tri-layout chain is unknown history; no chronological merge is published.
  it("tri-layout archive retains in-run, old sibling, then central rows chronologically with the legitimate overlay", async () => {
    const t = triLayout(), central = taskControlDirectory(t.source);
    const before = [t.snapshot(t.source), t.snapshot(t.sibling), t.snapshot(central)];
    expect(() => initializeTaskControlDirectory(t.source)).toThrow(LEGACY);
    expect(() => readFinalAnswerReceipt(t.source, t.record.id)).toThrow(LEGACY);
    await expect(t.store.retainRun(t.actor, t.alias, t.source)).rejects.toThrow(LEGACY);
    expect(fs.existsSync(taskControlDirectory(t.archive))).toBe(false);
    expect(fs.existsSync(path.join(t.archive, "status.json"))).toBe(false);
    expect(canRemoveTerminalRun(t.source)).toBe(false);
    expect(removalPaths()).toEqual([]);
    // Refusal never rewrites the in-run, old sibling or raw central layers.
    expect([t.snapshot(t.source), t.snapshot(t.sibling), t.snapshot(central)]).toEqual(before);
  });

  it.each(["duplicate-result", "unbound"] as const)("tri-layout archive refuses a %s chain without dropping in-run evidence", async bad => {
    // Cutoff #12214: uniform legacy refusal replaces the obsolete merge-specific errors.
    const t = triLayout(bad === "unbound" ? { bound: false } : { siblingAnswer: true }), central = taskControlDirectory(t.source);
    const before = [t.snapshot(t.source), t.snapshot(t.sibling), t.snapshot(central)];
    expect(() => readFinalAnswerReceipt(t.source, t.record.id)).toThrow(LEGACY);
    await expect(t.store.retainRun(t.actor, t.alias, t.source)).rejects.toThrow(LEGACY);
    expect(fs.existsSync(path.join(t.archive, "status.json"))).toBe(false);
    expect(fs.existsSync(taskControlDirectory(t.archive))).toBe(false);
    expect([t.snapshot(t.source), t.snapshot(t.sibling), t.snapshot(central)]).toEqual(before);
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
