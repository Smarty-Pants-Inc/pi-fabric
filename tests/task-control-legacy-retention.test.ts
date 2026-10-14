import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ResidentRequestRetention, sweepResidentRuns } from "../src/residency/retention.js";
import { canRemoveManagedRunRoot, canRemoveTerminalRun, isPairedTaskControlEntry, markRunRootClosed, pruneActorRunArchives, removeUnreferencedRun, runTreeExitVeto, sweepTempRunRoots } from "../src/storage/retention.js";
import { verifiedRemove } from "../src/storage/verified-remove.js";
import { retentionV2Enabled } from "../src/storage/retention-platform.js";
import { createHostTaskRun, initializeTaskControlDirectory, legacyTaskControlSibling, saveFinalAnswerReceipt, taskControlDirectory, taskSteerFile, TASK_CONTROL_LAYOUT } from "../src/worker/terminal-answer.js";
import { settleTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";

// Keep reason: trust-boundary and data-safety coverage of the real selection /
// retention path. Only the native remover is recorded; no retention delete runs.
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: vi.fn(() => ({ removed: true, bytes: 0 })) };
});
vi.mock("../src/storage/retention-platform.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/retention-platform.js")>();
  return { ...actual, retentionV2Enabled: vi.fn(actual.retentionV2Enabled) };
});
const roots: string[] = [];
const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
afterEach(() => {
  Object.defineProperty(process, "platform", nativePlatform);
  vi.restoreAllMocks(); vi.mocked(verifiedRemove).mockReset();
  vi.mocked(retentionV2Enabled).mockReturnValue(nativePlatform.value !== "win32");
  vi.mocked(verifiedRemove).mockReturnValue({ removed: true, bytes: 0 });
  // Fixture cleanup is covered by the parent's recorded-removal runner locally;
  // real removal coverage belongs to hosted CI, never a local apply.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
// Host receipts use the real clock; collection is still four days after those host events.
const day = 24 * 60 * 60 * 1000, now = Date.now() + 4 * day;
const json = (file: string, value: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
};
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "legacy-control-retention-")); roots.push(root); return root;
};
const seedRun = (root: string, id = "run", layout: "legacy" | "sibling-v1" | "legacy-read" = "legacy") => {
  // sibling-v1 is the OLD read-only sibling; host bootstrap (legacy-read) is central.
  const run = path.join(root, id), sibling = layout === "sibling-v1" ? legacyTaskControlSibling(run) : taskControlDirectory(run);
  fs.mkdirSync(run, { recursive: true, mode: 0o700 });
  if (layout === "sibling-v1") fs.mkdirSync(sibling, { mode: 0o700 });
  const ctl = layout === "sibling-v1" ? sibling : run;
  const receipt = { version: 1, id: "final-receipt", runId: id, recordedAt: day, text: "final answer" };
  const record = { id, runner: "pi", status: "completed", transport: "process", sessionId: "2147483647", finishedAt: day, updatedAt: day,
    ...(layout === "sibling-v1" ? { taskControlLayout: layout } : {}), finalAnswerReceipt: { id: receipt.id, recordedAt: day } };
  json(path.join(run, "status.json"), record);
  json(path.join(ctl, "final-answer.json"), receipt);
  const sender = { id: "session:original", kind: "main", verified: "mesh" };
  const provenance = { v: 1, channel: "fabric", via: "steer", sender };
  const messageId = "original-steer";
  fs.writeFileSync(path.join(ctl, "steer.jsonl"), JSON.stringify({ id: messageId, type: "steer", provenance, message: "retained input" }) + "\n", { mode: 0o600 });
  const control = { id: messageId, delivery: "steer", state: "delivered", provenance };
  json(path.join(ctl, "terminal-controls", `${messageId}.json`), control);
  const notice = { code: "FABRIC_TARGET_TERMINAL", targetId: id, messageId, delivery: "steer", finalAnswerReceiptId: receipt.id, sender };
  if (layout === "legacy-read") initializeTaskControlDirectory(run); // Host-owned inode-bound eligibility, never a status grant.
  fs.utimesSync(run, day / 1000, day / 1000);
  return { root, run, ctl, sibling, id, messageId, record, receipt, control, notice };
};
// Current protected seed: host exclusive allocation (v2 null central), host final-answer
// receipt, host admission/track/settle. Never a raw auto-import.
const seedCurrentRun = (root: string, id = "run") => {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const run = path.join(root, id), sibling = taskControlDirectory(run), ctl = sibling;
  createHostTaskRun(run);
  const receipt = saveFinalAnswerReceipt(run, id, "final answer");
  const record = { id, runner: "pi", status: "completed", transport: "process", sessionId: "2147483647", finishedAt: day, updatedAt: day,
    taskControlLayout: TASK_CONTROL_LAYOUT, finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } };
  json(path.join(run, "status.json"), record);
  const sender = { id: "session:original", kind: "main", verified: "mesh" };
  const provenance = { v: 1, channel: "fabric", via: "steer", sender };
  const messageId = "11111111-1111-4111-8111-111111111111";
  fs.writeFileSync(taskSteerFile(run), JSON.stringify({ id: messageId, type: "steer", provenance, message: "retained input" }) + "\n", { mode: 0o600 });
  trackTerminalControl(run, { id: messageId, delivery: "steer", provenance } as never);
  settleTerminalControl(run, messageId, "delivered");
  const control = { id: messageId, delivery: "steer", state: "delivered", provenance };
  const notice = { code: "FABRIC_TARGET_TERMINAL", targetId: id, messageId, delivery: "steer", finalAnswerReceiptId: receipt.id, sender };
  fs.utimesSync(run, day / 1000, day / 1000);
  return { root, run, ctl, sibling, id, messageId, record, receipt, control, notice };
};
const seedQueuedNoAnswerRun = () => {
  const root = scratch(), id = "run", run = path.join(root, id), sibling = taskControlDirectory(run);
  const record = { id, runner: "pi", status: "running", transport: "process", sessionId: "2147483647", updatedAt: day };
  json(path.join(run, "status.json"), record);
  const messageId = "11111111-1111-4111-8111-111111111111";
  const provenance = { v: 1, channel: "fabric", via: "steer", sender: { id: "session:original", kind: "main", verified: "mesh" } };
  const journal = JSON.stringify({ id: messageId, type: "steer", provenance, message: "resumed input" }) + "\n";
  fs.writeFileSync(path.join(run, "steer.jsonl"), journal, { mode: 0o600 });
  const control = { id: messageId, delivery: "steer", state: "queued", provenance };
  json(path.join(run, "terminal-controls", `${messageId}.json`), control);
  return { root, run, sibling, record, messageId, journal, control };
};
const seedTrackedFollowUpRun = () => {
  const root = scratch(), run = path.join(root, "run"), sibling = taskControlDirectory(run);
  createHostTaskRun(run);
  json(path.join(run, "status.json"), { id: "run", status: "stopped", transport: "process", sessionId: "2147483647",
    taskControlLayout: TASK_CONTROL_LAYOUT, finishedAt: day, updatedAt: day });
  const messageId = "11111111-1111-4111-8111-111111111111";
  const provenance = { v: 1, channel: "fabric", via: "followUp", sender: { id: "session:original", kind: "main", verified: "mesh" } };
  const entry = { id: messageId, type: "follow_up", followUpId: messageId, deadlineAt: day, provenance, message: "tracked input" };
  const journal = path.join(sibling, "steer.jsonl");
  fs.writeFileSync(journal, JSON.stringify(entry) + "\n", { mode: 0o600 });
  const admission = path.join(run, "follow-ups", `${messageId}.json`), settled = admission + ".settled";
  json(admission, { messageId, deadlineAt: day });
  fs.mkdirSync(settled, { mode: 0o700 });
  fs.writeFileSync(path.join(settled, "state"), "delivered", { mode: 0o600 });
  return { root, run, sibling, messageId, entry, journal, admission, settled };
};
const removePaths = () => vi.mocked(verifiedRemove).mock.calls.map(([target]) => target);

describe("existing-record legacy READ retention", () => {
  it("judges and records collection of settled current controls without widening the run-file allowlist", () => {
    const f = seedCurrentRun(scratch());
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, f.sibling]);
    expect(fs.existsSync(f.run)).toBe(true);
    expect(fs.readFileSync(path.join(f.ctl, "steer.jsonl"), "utf8")).toContain("retained input");
  });
  // CUT (Option 2, #12214): tri-layer overlay enumeration, resumed raw queued upgrade and
  // the no-answer overlay matrix are unsupported; "raw legacy retention cutoff" replaces them.
  it.each(["queued", "unknown", "wrong sender", "pending notice", "duplicate admission", "malformed journal", "unknown local file"])("retains legacy %s sources without invoking removal", bad => {
    const f = seedRun(scratch());
    if (bad === "queued" || bad === "unknown") json(path.join(f.ctl, "terminal-controls", `${f.messageId}.json`), { ...f.control, state: bad });
    if (bad === "wrong sender") json(path.join(f.ctl, "terminal-controls", `${f.messageId}.json`), { ...f.control, provenance: { ...f.control.provenance, sender: { ...f.control.provenance.sender, id: "session:forged" } } });
    if (bad === "pending notice") json(path.join(f.ctl, "terminal-notices", `${f.messageId}.json`), f.notice);
    if (bad === "duplicate admission") fs.appendFileSync(path.join(f.ctl, "steer.jsonl"), fs.readFileSync(path.join(f.ctl, "steer.jsonl")));
    if (bad === "malformed journal") fs.appendFileSync(path.join(f.ctl, "steer.jsonl"), "{broken}\n");
    if (bad === "unknown local file") fs.writeFileSync(path.join(f.run, "worker-forgery.json"), "{}");
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
  it("requires the original sender ACK for an unconsumed current admission", () => {
    const f = seedCurrentRun(scratch());
    // Current unforwarded refusal must retain the host's transport evidence.
    // Omitting `forwarded` fabricates an uncertain pre-v2 row even after an ACK.
    json(path.join(f.ctl, "terminal-controls", `${f.messageId}.json`), { ...f.control, forwarded: false, state: "refused", reason: "run-ended" });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    json(path.join(f.ctl, "terminal-notices", `${f.messageId}.json.delivered`), { ...f.notice, sender: { ...f.notice.sender, id: "session:forged" } });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    json(path.join(f.ctl, "terminal-notices", `${f.messageId}.json.delivered`), f.notice);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, f.sibling]);
  });
  it.each(["missing sibling", "local controls", "unknown sibling file", "live worker", "unresolved worker", "unknown marker", "forged legacy marker"])("never collects protected %s", bad => {
    const f = seedRun(scratch(), "run", "sibling-v1");
    if (bad === "missing sibling" || bad === "unknown marker") fs.renameSync(f.sibling, path.join(f.root, "held-control"));
    if (bad === "missing sibling" || bad === "local controls" || bad === "unknown marker" || bad === "forged legacy marker") {
      json(path.join(f.run, "final-answer.json"), f.receipt);
      fs.writeFileSync(path.join(f.run, "steer.jsonl"), JSON.stringify({ id: f.messageId, type: "steer", provenance: f.control.provenance }) + "\n");
      json(path.join(f.run, "terminal-controls", `${f.messageId}.json`), f.control);
    }
    if (bad === "unknown sibling file") fs.writeFileSync(path.join(f.sibling, "unknown.json"), "{}");
    if (bad === "live worker") json(path.join(f.run, "status.json"), { ...f.record, sessionId: String(process.pid) });
    if (bad === "unresolved worker") json(path.join(f.run, "unresolved-worker.json"), {});
    if (bad === "unknown marker") json(path.join(f.run, "status.json"), { ...f.record, taskControlLayout: "unknown" });
    if (bad === "forged legacy marker") json(path.join(f.run, "status.json"), { ...f.record, taskControlLayout: "legacy-read" });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
  it("does not accept a forged host legacy-eligibility marker", () => {
    const f = seedRun(scratch(), "run", "sibling-v1");
    json(path.join(f.sibling, ".legacy-read.json"), { version: 1, runId: f.id, dev: "0", ino: "0" });
    json(path.join(f.run, "status.json"), { ...f.record, taskControlLayout: "legacy-read" });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
  it.skipIf(process.platform === "win32").each(["linked legacy control", "hard-linked legacy control", "linked sibling"])("retains %s", bad => {
    const f = seedRun(scratch(), "run", bad === "linked sibling" ? "sibling-v1" : "legacy");
    if (bad === "linked sibling") {
      const held = path.join(f.root, "held-control"); fs.renameSync(f.sibling, held); fs.symlinkSync(held, f.sibling);
    } else {
      const control = path.join(f.ctl, "terminal-controls", `${f.messageId}.json`), held = path.join(f.root, "held.json");
      fs.renameSync(control, held);
      if (bad === "linked legacy control") fs.symlinkSync(held, control); else fs.linkSync(held, control);
    }
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
  it("retains admission debt without a final answer, and respects an expired walk", () => {
    const f = seedRun(scratch());
    fs.renameSync(path.join(f.ctl, "final-answer.json"), path.join(f.root, "held-answer.json"));
    fs.renameSync(path.join(f.ctl, "terminal-controls"), path.join(f.root, "held-controls"));
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(canRemoveTerminalRun(f.run, () => true)).toBe(false);
    expect(removePaths()).toEqual([]);
  });
});

describe("tracked follow-up admission discharge", () => {
  it.each(["delivered", "cancelled"])("collects the exact %s tracked admission without inventing a terminal row", state => {
    const f = seedTrackedFollowUpRun(), original = fs.readFileSync(f.journal, "utf8");
    fs.writeFileSync(path.join(f.settled, "state"), state);
    expect(fs.existsSync(path.join(f.sibling, "terminal-controls"))).toBe(false);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(canRemoveTerminalRun(f.run, () => true)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, f.sibling]);
    expect(fs.readFileSync(f.journal, "utf8")).toBe(original);
    expect(fs.existsSync(f.admission)).toBe(true);
  });
  it.each([
    "steer", "untracked follow-up", "wrong tracked id", "other settled message", "wrong receipt id", "missing receipt",
    "queued receipt", "settling receipt", "malformed receipt", "invalid provenance", "unknown filename", "queued control", "legacy queued control",
    ...(process.platform === "win32" ? [] : ["linked admission", "hard-linked state", "linked directory"]),
  ])("vetoes %s through the public collector despite an otherwise settled tracked artifact", bad => {
    const f = seedTrackedFollowUpRun(), other = "22222222-2222-4222-8222-222222222222";
    const entry = { ...f.entry };
    if (bad === "steer") entry.type = "steer";
    if (bad === "untracked follow-up") delete (entry as Partial<typeof entry>).followUpId;
    if (bad === "wrong tracked id") entry.followUpId = other;
    if (bad === "other settled message") { entry.id = other; entry.followUpId = other; }
    if (bad === "invalid provenance") entry.provenance.sender.verified = "invented";
    fs.writeFileSync(f.journal, JSON.stringify(entry) + "\n");
    if (bad === "wrong receipt id") json(f.admission, { messageId: other, deadlineAt: day });
    if (bad === "missing receipt") fs.renameSync(f.settled, path.join(f.root, "held-receipt"));
    if (bad === "queued receipt") fs.writeFileSync(path.join(f.settled, "state"), "queued");
    if (bad === "settling receipt") fs.renameSync(path.join(f.settled, "state"), path.join(f.root, "held-state"));
    if (bad === "malformed receipt") fs.writeFileSync(f.admission, "{broken}");
    if (bad === "unknown filename") json(path.join(path.dirname(f.admission), "unknown.json"), {});
    if (bad === "queued control") json(path.join(f.sibling, "terminal-controls", `${f.messageId}.json`), {
      id: f.messageId, delivery: "followUp", state: "queued", provenance: f.entry.provenance,
    });
    if (bad === "legacy queued control") {
      fs.copyFileSync(f.journal, path.join(f.run, "steer.jsonl"));
      fs.renameSync(f.sibling, f.sibling + ".held-controls");
      json(path.join(f.run, "status.json"), { id: "run", status: "stopped", transport: "process", sessionId: "2147483647", finishedAt: day });
      json(path.join(f.run, "terminal-controls", `${f.messageId}.json`), { id: f.messageId, delivery: "followUp", state: "queued", provenance: f.entry.provenance });
    }
    if (bad === "linked admission") {
      const held = path.join(f.root, "held-admission.json"); fs.renameSync(f.admission, held); fs.symlinkSync(held, f.admission);
    }
    if (bad === "hard-linked state") fs.linkSync(path.join(f.settled, "state"), path.join(f.root, "held-state"));
    if (bad === "linked directory") {
      const directory = path.dirname(f.admission), held = path.join(f.root, "held-follow-ups");
      fs.renameSync(directory, held); fs.symlinkSync(held, directory);
    }
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
});

describe("paired control tree collection", () => {
  it("checks nested native exit without mistaking sibling metadata for a worker and records its enclosing removal", () => {
    const f = seedCurrentRun(scratch());
    const child = seedCurrentRun(path.join(f.run, "nested"), "child");
    expect(runTreeExitVeto(f.run, 0, undefined, true)).toBeUndefined();
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, f.sibling, child.sibling]); // Nested central controls are never stranded.
    json(path.join(child.run, "status.json"), { ...child.record, sessionId: String(process.pid) });
    vi.mocked(verifiedRemove).mockClear();
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
  it.each(["unknown contents", "unpaired", ...(process.platform === "win32" ? [] : ["non-private"])])("does not blindly skip nested %s .ctl", bad => {
    const f = seedRun(scratch(), "run", "sibling-v1"), nested = path.join(f.run, "nested");
    const child = seedRun(nested, "child", "sibling-v1");
    if (bad === "unknown contents") fs.writeFileSync(path.join(child.sibling, "unknown.json"), "{}");
    if (bad === "unpaired") fs.mkdirSync(path.join(nested, "orphan.ctl"), { mode: 0o700 });
    if (bad === "non-private") fs.chmodSync(child.sibling, 0o755);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
  it("validates a managed root pair and refuses an unmatched control sibling", () => {
    const root = scratch(), f = seedCurrentRun(root);
    markRunRootClosed(root, day, true);
    expect(canRemoveManagedRunRoot(root)).toBe(true);
    fs.mkdirSync(path.join(root, "orphan.ctl"), { mode: 0o700 });
    expect(isPairedTaskControlEntry(root, "orphan.ctl")).toBe(false);
    expect(canRemoveManagedRunRoot(root)).toBe(false);
    expect(fs.existsSync(f.run)).toBe(true);
  });
  it("records the closed temp-root pair, never treats an unknown .ctl as a run", () => {
    const tempRoot = scratch(), root = path.join(tempRoot, "pi-fabric-runs-pair");
    const f = seedCurrentRun(root); markRunRootClosed(root, day, true);
    fs.mkdirSync(path.join(root, "unknown.ctl"), { mode: 0o700 });
    expect(sweepTempRunRoots({ tempRoot, now, orphanedTempRunRetentionMs: day, oneShotRunRetentionMs: day })).toEqual({ removedRoots: [], removedRuns: [f.run] });
    expect(removePaths()).toEqual([f.run, f.sibling]);
    expect(fs.existsSync(path.join(root, "unknown.ctl"))).toBe(true);
  });
  // CUT: central-over-old-sibling tri-layer planning (unsupported under Option 2).
  it.each(["late reference", "run identity refusal"])("retains control evidence when the main run fence rejects %s", refusal => {
    const f = seedCurrentRun(scratch());
    let referenced = false;
    expect(removeUnreferencedRun(f.run, fs.statSync(f.run), () => true, true)).toBe(false);
    expect(removePaths()).toEqual([]);
    vi.mocked(verifiedRemove).mockImplementationOnce((_target, _stat, options) => {
      if (refusal === "late reference") {
        referenced = true;
        expect(options.proceed?.()).toBe(false);
      }
      return { removed: false, reason: refusal };
    });
    expect(removeUnreferencedRun(f.run, fs.statSync(f.run), () => referenced, true)).toBe(false);
    expect(removePaths()).toEqual([f.run]);
    expect(fs.existsSync(path.join(f.sibling, "final-answer.json"))).toBe(true);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  it("reports the run removed while retaining controls after partial pair cleanup", () => {
    const f = seedCurrentRun(scratch());
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.mocked(verifiedRemove).mockReturnValueOnce({ removed: true, bytes: 0 })
      .mockReturnValueOnce({ removed: false, reason: "recorded control refusal" });
    expect(removeUnreferencedRun(f.run, fs.statSync(f.run), undefined, true)).toBe(true);
    expect(removePaths()).toEqual([f.run, f.sibling]);
    expect(warning).toHaveBeenCalledWith(expect.stringContaining("partial cleanup"));
    expect(fs.existsSync(f.sibling)).toBe(true);
  });
});

describe("resident pair callers", () => {
  it.each(["linux", "win32"] as const)("streaming %s keeps the fingerprint fence on both recorded pair removals", platform => {
    // Select the collector algorithm, not a foreign filesystem contract.
    vi.mocked(retentionV2Enabled).mockReturnValue(platform !== "win32");
    const root = scratch(), runs = path.join(root, "runs"), f = seedCurrentRun(runs);
    const actorRoot = path.join(root, "actors"); json(path.join(actorRoot, "actors.json"), { actors: [] });
    json(path.join(root, "results", `${f.id}.json`), { ...f.record, text: "preserved full result", events: [], exitCode: 0 });
    const collector = new ResidentRequestRetention(root, [actorRoot], { retainRuns: false });
    try { collector.sweep(now, new Set(), 10_000); } finally { collector.close(); }
    expect(removePaths()).toEqual([f.run, f.sibling]);
    for (const [, , options] of vi.mocked(verifiedRemove).mock.calls) {
      expect(options.deleter).toBe("residentRunRetention");
      expect(options.proceed?.()).toBe(true);
    }
    json(path.join(actorRoot, "actors.json"), { actors: [{ id: "actor", lastRunId: f.id }] });
    for (const [, , options] of vi.mocked(verifiedRemove).mock.calls) expect(options.proceed?.()).toBe(false);
    expect(fs.existsSync(f.run)).toBe(true);
    expect(fs.existsSync(f.sibling)).toBe(true);
  });
  it("fenced startup records the pair and preserves new registry references", () => {
    const root = scratch(), runs = path.join(root, "runs"), f = seedCurrentRun(runs);
    const actorRoot = path.join(root, "actors"); json(path.join(actorRoot, "actors.json"), { actors: [] });
    json(path.join(root, "results", `${f.id}.json`), { ...f.record, text: "preserved full result", events: [], exitCode: 0 });
    expect(sweepResidentRuns(runs, now, 10_000, { actorRoots: [actorRoot], dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, f.sibling]);
    json(path.join(actorRoot, "actors.json"), { actors: [{ id: "actor", lastRunId: f.id }] });
    vi.mocked(verifiedRemove).mockClear();
    expect(sweepResidentRuns(runs, now, 10_000, { actorRoots: [actorRoot], dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
});

// central-v1 preserves unmarked pre-v2 in-run READ/retention semantics without
// minting write authority. Old siblings still veto; no layout is auto-upgraded.
const filesOf = (...directories: string[]) => directories.filter(directory => fs.existsSync(directory)).flatMap(directory =>
  fs.readdirSync(directory, { recursive: true, withFileTypes: true }).filter(entry => entry.isFile())
    .map(entry => [path.join(entry.parentPath, entry.name), fs.readFileSync(path.join(entry.parentPath, entry.name)).toString("base64")]))
  .sort(([a], [b]) => a!.localeCompare(b!));
describe("raw legacy retention cutoff", () => {
  it.each(["legacy", "sibling-v1"] as const)("judges settled %s history read-only without minting authority or changing bytes", layout => {
    const f = seedRun(scratch(), "run", layout);
    const before = filesOf(f.run, legacyTaskControlSibling(f.run), taskControlDirectory(f.run));
    expect(() => initializeTaskControlDirectory(f.run)).toThrow("FABRIC_LEGACY_SNAPSHOT_REQUIRED");
    const collectible = layout === "legacy";
    expect(canRemoveTerminalRun(f.run)).toBe(collectible);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual(collectible ? [f.run] : []);
    // A pre-v2 run has no central pair to remove. The native removal is recorded,
    // not executed; this checks selection without laundering legacy authority.
    expect(removePaths()).toEqual(collectible ? [f.run] : []);
    expect(fs.existsSync(taskControlDirectory(f.run))).toBe(false);
    expect(filesOf(f.run, legacyTaskControlSibling(f.run), taskControlDirectory(f.run))).toEqual(before);
  });
  it("unknown raw queued debt keeps custody: no upgrade, no settle, no collection", () => {
    const f = seedQueuedNoAnswerRun();
    json(path.join(f.run, "status.json"), { ...f.record, status: "stopped", finishedAt: day });
    const before = filesOf(f.run, taskControlDirectory(f.run));
    expect(() => initializeTaskControlDirectory(f.run)).toThrow("FABRIC_LEGACY_SNAPSHOT_REQUIRED");
    // Read-only fallback never authorizes the lock/write path. Assert the typed
    // refusal rather than treating validation-only setup as a silent bootstrap.
    expect(() => settleTerminalControl(f.run, f.messageId, "delivered")).toThrow("FABRIC_LEGACY_SNAPSHOT_REQUIRED");
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(removePaths()).toEqual([]);
    expect(filesOf(f.run, taskControlDirectory(f.run))).toEqual(before);
  });
});
