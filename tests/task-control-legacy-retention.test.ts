import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ResidentRequestRetention, sweepResidentRuns } from "../src/residency/retention.js";
import { canRemoveManagedRunRoot, canRemoveTerminalRun, isPairedTaskControlEntry, markRunRootClosed, pruneActorRunArchives, removeUnreferencedRun, runTreeExitVeto, sweepTempRunRoots } from "../src/storage/retention.js";
import { verifiedRemove } from "../src/storage/verified-remove.js";
import { initializeTaskControlDirectory, legacyTaskControlSibling, taskControlDirectory, TASK_CONTROL_LAYOUT } from "../src/worker/terminal-answer.js";
import { queuedTerminalControls, settleTerminalControl } from "../src/worker/terminal-controls.js";

// Keep reason: trust-boundary and data-safety coverage of the real selection /
// retention path. Only the native remover is recorded; no retention delete runs.
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: vi.fn(() => ({ removed: true, bytes: 0 })) };
});
const roots: string[] = [];
const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
afterEach(() => {
  Object.defineProperty(process, "platform", nativePlatform);
  vi.restoreAllMocks(); vi.mocked(verifiedRemove).mockReset();
  vi.mocked(verifiedRemove).mockReturnValue({ removed: true, bytes: 0 });
  // Fixture cleanup is covered by the parent's recorded-removal runner locally;
  // real removal coverage belongs to hosted CI, never a local apply.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const day = 24 * 60 * 60 * 1000, now = 4 * day;
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
  fs.mkdirSync(run, { mode: 0o700 });
  initializeTaskControlDirectory(run);
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
  it.each(["legacy", "legacy-read", "sibling-v1"] as const)("judges and records collection of settled %s controls without widening the run-file allowlist", layout => {
    const f = seedRun(scratch(), "run", layout);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual(layout === "legacy" ? [f.run] : [f.run, f.sibling]);
    expect(fs.existsSync(f.run)).toBe(true);
    expect(fs.readFileSync(path.join(f.ctl, "steer.jsonl"), "utf8")).toContain("retained input");
  });
  it("enumerates eligible old controls and the delivered sibling overlay without hiding old queued debt", () => {
    const f = seedRun(scratch(), "run", "legacy-read");
    json(path.join(f.ctl, "terminal-controls", `${f.messageId}.json`), { ...f.control, state: "queued" });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    json(path.join(f.sibling, "terminal-controls", `${f.messageId}.json`), f.control);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    const other = "new-admission";
    fs.writeFileSync(path.join(f.sibling, "steer.jsonl"), JSON.stringify({ id: other, type: "steer", provenance: f.control.provenance }) + "\n");
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    json(path.join(f.sibling, "terminal-controls", `${other}.json`), { ...f.control, id: other });
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, f.sibling]);
  });
  it.each(["stopped", "failed"])("collects a resumed legacy queued admission with its real delivered overlay after %s without a final answer", status => {
    const f = seedQueuedNoAnswerRun();
    expect(fs.existsSync(f.sibling)).toBe(false);
    expect(queuedTerminalControls(f.run)).toEqual([f.control]);
    initializeTaskControlDirectory(f.run);
    const stat = fs.lstatSync(f.run, { bigint: true });
    expect(JSON.parse(fs.readFileSync(path.join(f.sibling, ".legacy-read.json"), "utf8"))).toEqual({
      version: 1, runId: f.record.id, dev: String(stat.dev), ino: String(stat.ino),
    });
    settleTerminalControl(f.run, f.messageId, "delivered");
    expect(JSON.parse(fs.readFileSync(path.join(f.sibling, "terminal-controls", `${f.messageId}.json`), "utf8"))).toEqual({ ...f.control, state: "delivered" });
    expect(queuedTerminalControls(f.run)).toEqual([]);
    json(path.join(f.run, "status.json"), { ...f.record, status, finishedAt: day, updatedAt: day });
    expect(fs.existsSync(path.join(f.run, "final-answer.json"))).toBe(false);
    expect(fs.existsSync(path.join(f.sibling, "final-answer.json"))).toBe(false);
    expect(runTreeExitVeto(f.run, 0, undefined, true)).toBeUndefined();
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(canRemoveTerminalRun(f.run, () => true)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, f.sibling]);
    expect(vi.mocked(verifiedRemove).mock.calls.every(([, , options]) => options.dryRun === true)).toBe(true);
    expect(fs.readFileSync(path.join(f.run, "steer.jsonl"), "utf8")).toBe(f.journal);
    expect(JSON.parse(fs.readFileSync(path.join(f.run, "terminal-controls", `${f.messageId}.json`), "utf8"))).toEqual(f.control);
    expect(fs.existsSync(f.run)).toBe(true);
    expect(fs.existsSync(f.sibling)).toBe(true);
  });
  it.each([
    "no overlay", "queued overlay", "overlay wrong sender", "overlay wrong id", "overlay wrong delivery",
    "source wrong sender", "source wrong id", "source wrong delivery", "source unknown", "source refused",
    "source malformed", "source extra field", "duplicate journal", "malformed journal", "pending notice",
    "missing ctl", "live process", "unresolved worker", "unknown sibling file", "missing grant", "forged grant",
  ])("vetoes no-answer legacy queued collection for %s even with an otherwise delivered overlay", bad => {
    const f = seedQueuedNoAnswerRun();
    initializeTaskControlDirectory(f.run);
    settleTerminalControl(f.run, f.messageId, "delivered");
    json(path.join(f.run, "status.json"), { ...f.record, status: "stopped", finishedAt: day, updatedAt: day });
    const source = path.join(f.run, "terminal-controls", `${f.messageId}.json`);
    const overlay = path.join(f.sibling, "terminal-controls", `${f.messageId}.json`);
    if (bad === "no overlay") fs.renameSync(overlay, path.join(f.root, "held-overlay.json"));
    if (bad === "queued overlay") json(overlay, f.control);
    if (bad.includes("wrong")) json(bad.startsWith("source") ? source : overlay, {
      ...f.control, state: bad.startsWith("source") ? "queued" : "delivered",
      ...(bad.endsWith("id") ? { id: "other-control" } : {}),
      ...(bad.endsWith("delivery") ? { delivery: "followUp" } : {}),
      ...(bad.endsWith("sender") ? { provenance: { ...f.control.provenance, sender: { ...f.control.provenance.sender, id: "session:forged" } } } : {}),
    });
    if (bad === "source unknown" || bad === "source refused") json(source, { ...f.control, state: bad.slice(7) });
    if (bad === "source malformed") fs.writeFileSync(source, "{broken}");
    if (bad === "source extra field") json(source, { ...f.control, authority: "invented" });
    if (bad === "duplicate journal") fs.writeFileSync(path.join(f.sibling, "steer.jsonl"), f.journal);
    if (bad === "malformed journal") fs.appendFileSync(path.join(f.run, "steer.jsonl"), "{broken}\n");
    if (bad === "pending notice") json(path.join(f.sibling, "terminal-notices", `${f.messageId}.json`), {});
    if (bad === "missing ctl") fs.renameSync(f.sibling, path.join(f.root, "held-ctl"));
    if (bad === "live process") json(path.join(f.run, "status.json"), { ...f.record, status: "stopped", sessionId: String(process.pid), finishedAt: day });
    if (bad === "unresolved worker") json(path.join(f.run, "unresolved-worker.json"), {});
    if (bad === "unknown sibling file") json(path.join(f.sibling, "unknown.json"), {});
    if (bad === "missing grant") fs.renameSync(path.join(f.sibling, ".legacy-read.json"), path.join(f.root, "held-grant.json"));
    if (bad === "forged grant") json(path.join(f.sibling, ".legacy-read.json"), { version: 1, runId: f.record.id, dev: "0", ino: "0" });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([]);
    expect(removePaths()).toEqual([]);
  });
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
  it("requires the original sender ACK for an unconsumed legacy admission", () => {
    const f = seedRun(scratch());
    json(path.join(f.ctl, "terminal-controls", `${f.messageId}.json`), { ...f.control, state: "refused", reason: "run-ended" });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    json(path.join(f.ctl, "terminal-notices", `${f.messageId}.json.delivered`), { ...f.notice, sender: { ...f.notice.sender, id: "session:forged" } });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    json(path.join(f.ctl, "terminal-notices", `${f.messageId}.json.delivered`), f.notice);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run]);
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
      fs.renameSync(f.journal, path.join(f.run, "steer.jsonl"));
      fs.renameSync(f.sibling, path.join(f.root, "held-controls"));
      json(path.join(f.run, "status.json"), { id: "run", status: "stopped", transport: "process", sessionId: "2147483647", finishedAt: day });
      json(path.join(f.run, "terminal-controls", `${f.messageId}.json`), { id: f.messageId, delivery: "followUp", state: "queued", provenance: f.entry.provenance });
      initializeTaskControlDirectory(f.run);
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
    const f = seedRun(scratch(), "run", "sibling-v1");
    const child = seedRun(path.join(f.run, "nested"), "child", "sibling-v1");
    expect(isPairedTaskControlEntry(path.dirname(child.run), "child.ctl")).toBe(true);
    expect(runTreeExitVeto(f.run, 0, undefined, true)).toBeUndefined();
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, f.sibling]); // Nested pair is inside the judged parent tree.
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
    const root = scratch(), f = seedRun(root, "run", "sibling-v1");
    markRunRootClosed(root, day, true);
    expect(canRemoveManagedRunRoot(root)).toBe(true);
    fs.mkdirSync(path.join(root, "orphan.ctl"), { mode: 0o700 });
    expect(isPairedTaskControlEntry(root, "orphan.ctl")).toBe(false);
    expect(canRemoveManagedRunRoot(root)).toBe(false);
    expect(fs.existsSync(f.run)).toBe(true);
  });
  it("records the closed temp-root pair, never treats an unknown .ctl as a run", () => {
    const tempRoot = scratch(), root = path.join(tempRoot, "pi-fabric-runs-pair");
    const f = seedRun(root, "run", "sibling-v1"); markRunRootClosed(root, day, true);
    fs.mkdirSync(path.join(root, "unknown.ctl"), { mode: 0o700 });
    expect(sweepTempRunRoots({ tempRoot, now, orphanedTempRunRetentionMs: day, oneShotRunRetentionMs: day })).toEqual({ removedRoots: [], removedRuns: [f.run] });
    expect(removePaths()).toEqual([f.run, f.sibling]);
    expect(fs.existsSync(path.join(root, "unknown.ctl"))).toBe(true);
  });
  it("plans central controls with their run, keeps a validated old sibling, and never strands nested central controls", () => {
    const f = seedRun(scratch(), "run", "sibling-v1");
    initializeTaskControlDirectory(f.run); // host bootstrap: central over the intact old sibling
    const child = seedRun(path.join(f.run, "nested"), "child", "legacy-read");
    expect(fs.existsSync(child.sibling)).toBe(true);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: day, now, dryRun: true })).toEqual([f.run]);
    expect(removePaths()).toEqual([f.run, taskControlDirectory(f.run), f.sibling, child.sibling]);
    // Recorded dry run: every original byte stays.
    expect(fs.existsSync(path.join(f.sibling, "final-answer.json"))).toBe(true);
  });
  it.each(["late reference", "run identity refusal"])("retains control evidence when the main run fence rejects %s", refusal => {
    const f = seedRun(scratch(), "run", "sibling-v1");
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
    const f = seedRun(scratch(), "run", "sibling-v1");
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
    Object.defineProperty(process, "platform", { ...nativePlatform, value: platform });
    const root = scratch(), runs = path.join(root, "runs"), f = seedRun(runs, "run", "sibling-v1");
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
    const root = scratch(), runs = path.join(root, "runs"), f = seedRun(runs, "run", "sibling-v1");
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
