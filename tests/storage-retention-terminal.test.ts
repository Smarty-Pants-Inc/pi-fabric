import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TerminalNoticeOutbox, readTerminalAdmission, terminalControlMatchesAdmission } from "../src/agents/terminal-notices.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { canRemoveTerminalRun, pruneActorRunArchives } from "../src/storage/retention.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-retention-")); roots.push(root);
  const run = path.join(root, "run"); fs.mkdirSync(run);
  const receipt = saveFinalAnswerReceipt(run, "run", "final answer");
  const record = { id: "run", runner: "pi", status: "completed", transport: "process", sessionId: "99999999", updatedAt: Date.now(),
    finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } };
  writeJsonAtomic(path.join(run, "status.json"), record);
  const sender = { id: "session:sender", kind: "main", verified: "mesh" };
  const provenance = { v: 1, channel: "fabric", via: "steer", sender };
  const id = "11111111-1111-4111-8111-111111111111";
  const notice = { code: "FABRIC_TARGET_TERMINAL", targetId: "run", finalAnswerReceiptId: receipt.id, delivery: "steer", messageId: id, sender };
  const control = { id, delivery: "steer", state: "refused", provenance };
  fs.writeFileSync(path.join(run, "steer.jsonl"), JSON.stringify({ id, type: "steer", message: "stale", provenance }) + "\n");
  return { root, run, receipt, record, id, notice, control };
};

describe("terminal-answer retention custody", () => {
  const acknowledgeFinalNotice = (f: ReturnType<typeof fixture>): void => {
    writeJsonAtomic(path.join(f.run, "terminal-notices", `${f.id}.json.delivered`), f.notice);
  };
  const finalAnswerLock = (run: string, owner: string): string => {
    const lock = path.join(run, ".task-final-answer.lock");
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), owner);
    return lock;
  };
  it("collects a stale final-answer lock left by an exited holder, through the collector dry-run", () => {
    const f = fixture();
    acknowledgeFinalNotice(f);
    const helper = path.join(f.root, "terminal-answer.mjs");
    buildSync({ entryPoints: [path.resolve("src/worker/terminal-answer.ts")], outfile: helper, bundle: true, platform: "node", format: "esm" });
    // process.exit bypasses finally: the real lock holder leaves its native owner record behind.
    const child = spawnSync(process.execPath, ["--input-type=module", "--eval",
      `import { withTaskFinalAnswerLock } from ${JSON.stringify(pathToFileURL(helper).href)}; withTaskFinalAnswerLock(${JSON.stringify(f.run)}, () => process.exit(0));`], { encoding: "utf8", timeout: 10_000 });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
    const owner = path.join(f.run, ".task-final-answer.lock", "owner");
    const [token, pid] = fs.readFileSync(owner, "utf8").trim().split("\n");
    expect(Number(pid)).toBe(child.pid);
    // Advance only the saved timestamp, not a sleep or the live-process fence.
    fs.writeFileSync(owner, `${token}\n${pid}\n${Date.now() - 60_000}\n`);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    const collected = pruneActorRunArchives({ runsDirectory: f.root, retentionMs: 0, now: Date.now() + 1, dryRun: true });
    expect(collected).toEqual([f.run]);
    expect(fs.existsSync(f.run)).toBe(true);
  });
  it("keeps a live old lock and fails closed for ownerless, malformed, extra, and linked lock contents", () => {
    const live = fixture();
    acknowledgeFinalNotice(live);
    finalAnswerLock(live.run, `11111111-1111-4111-8111-111111111111\n${process.pid}\n${Date.now() - 60_000}\n`);
    expect(canRemoveTerminalRun(live.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: live.root, retentionMs: 0, now: Date.now() + 1, dryRun: true })).toEqual([]);

    for (const kind of ["ownerless", "malformed", "extra", "linked"] as const) {
      const f = fixture();
      acknowledgeFinalNotice(f);
      const lock = path.join(f.run, ".task-final-answer.lock");
      fs.mkdirSync(lock);
      if (kind === "malformed") fs.writeFileSync(path.join(lock, "owner"), `not-an-owner\n99999999\n${Date.now() - 60_000}\n`);
      if (kind === "extra") {
        fs.writeFileSync(path.join(lock, "owner"), `11111111-1111-4111-8111-111111111111\n99999999\n${Date.now() - 60_000}\n`);
        fs.writeFileSync(path.join(lock, "unexpected"), "no");
      }
      if (kind === "linked") {
        const outside = path.join(f.root, "outside-owner");
        fs.writeFileSync(outside, `11111111-1111-4111-8111-111111111111\n99999999\n${Date.now() - 60_000}\n`);
        try { fs.symlinkSync(outside, path.join(lock, "owner")); } catch { continue; }
      }
      expect(canRemoveTerminalRun(f.run), kind).toBe(false);
    }
  });
  it("keeps young dead-holder locks and treats uncertain owner liveness as live", () => {
    const f = fixture();
    acknowledgeFinalNotice(f);
    const lock = finalAnswerLock(f.run, `11111111-1111-4111-8111-111111111111\n99999999\n${Date.now()}\n`);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    fs.writeFileSync(path.join(lock, "owner"), `11111111-1111-4111-8111-111111111111\n99999999\n${Date.now() - 60_000}\n`);
    const workerPid = 99999998, lockPid = 99999999;
    writeJsonAtomic(path.join(f.run, "status.json"), { ...f.record, sessionId: String(workerPid) });
    const nativeKill = process.kill.bind(process);
    const probe = vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (pid === lockPid) throw Object.assign(new Error("denied"), { code: "EPERM" });
      if (pid === workerPid) throw Object.assign(new Error("exited worker"), { code: "ESRCH" });
      return nativeKill(pid, signal);
    });
    try {
      expect(canRemoveTerminalRun(f.run)).toBe(false);
      expect(probe).toHaveBeenCalledWith(workerPid, 0);
      expect(probe).toHaveBeenCalledWith(lockPid, 0);
    } finally { probe.mockRestore(); }
  });
  it("keeps emitted but unrouted refusals and unread steering sources", () => {
    const f = fixture();
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), f.control);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    writeJsonAtomic(path.join(f.run, "terminal-notices", `${f.id}.json`), f.notice);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it("collects only after retained sender-route ACK, including an unread tail and unlink crash", () => {
    const f = fixture();
    writeJsonAtomic(path.join(f.run, "terminal-notices", `${f.id}.json.delivered`), f.notice);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), f.control);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    writeJsonAtomic(path.join(f.run, "terminal-notices", `${f.id}.json`), f.notice);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  it("reconstructs a missing sender outbox after receipt-before-scan host crash", async () => {
    const f = fixture();
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), f.control);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    const delivered: unknown[] = [];
    const successor = new TerminalNoticeOutbox(notice => { delivered.push(notice); });
    successor.recover(f.run);
    await vi.waitFor(() => expect(fs.existsSync(path.join(f.run, "terminal-notices", `${f.id}.json.delivered`))).toBe(true));
    expect(delivered).toEqual([f.notice]);
    successor.recover(f.run);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(delivered).toHaveLength(1);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  it.each(["delivery", "sender", "id", "kind", "name", "verified", "missing-provenance"])("retains mismatched delivered %s uncertainty without fabricating a refusal during recovery", async mismatch => {
    const f = fixture();
    const sender = f.control.provenance.sender;
    const control = { ...f.control, state: "delivered",
      ...(mismatch === "delivery" ? { delivery: "followUp" } : {}),
      ...(mismatch === "id" ? { id: "other-control" } : {}),
      provenance: mismatch === "missing-provenance" ? undefined : { ...f.control.provenance, sender: {
        ...sender,
        ...(mismatch === "sender" ? { id: "session:senderB" } : {}),
        ...(mismatch === "kind" ? { kind: "agent" } : {}),
        ...(mismatch === "name" ? { name: "different name" } : {}),
        ...(mismatch === "verified" ? { verified: "bridge" } : {}),
      } },
    };
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), control);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    const pending = path.join(f.run, "terminal-notices", `${f.id}.json`);
    new TerminalNoticeOutbox().recover(f.run); // No route cannot turn uncertainty into refusal debt.
    expect(fs.existsSync(pending)).toBe(false);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    const delivered: unknown[] = [];
    const successor = new TerminalNoticeOutbox(notice => { delivered.push(notice); });
    successor.recover(f.run);
    successor.recover(f.run);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(delivered).toEqual([]);
    expect(fs.existsSync(pending)).toBe(false);
    expect(fs.existsSync(`${pending}.delivered`)).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(f.run, "terminal-controls", `${f.id}.json`), "utf8"))).toEqual(JSON.parse(JSON.stringify(control)));
    expect(readTerminalAdmission(f.run, f.id)).toEqual({ id: f.id, delivery: "steer", sender });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: 0, now: Date.now() + 1, dryRun: true })).toEqual([]);
    expect(fs.existsSync(f.run)).toBe(true);
  });
  it.each(["delivery", "sender", "kind", "name", "verified"])("does not discharge a mismatched row with an incorrectly attributed refusal %s", mismatch => {
    const f = fixture();
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered", delivery: "followUp" });
    writeJsonAtomic(path.join(f.run, "terminal-notices", `${f.id}.json.delivered`), {
      ...f.notice, ...(mismatch === "delivery" ? { delivery: "followUp" } : {}), sender: {
        ...f.notice.sender,
        ...(mismatch === "sender" ? { id: "session:senderB" } : {}),
        ...(mismatch === "kind" ? { kind: "agent" } : {}),
        ...(mismatch === "name" ? { name: "different name" } : {}),
        ...(mismatch === "verified" ? { verified: "bridge" } : {}),
      },
    });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it("requires the original admission even when no final answer was recorded", () => {
    const f = fixture();
    fs.rmSync(path.join(f.run, "final-answer.json"));
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered" });
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered", delivery: "followUp" });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    fs.rmSync(path.join(f.run, "steer.jsonl"));
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it("fails closed for unknown rows and does not normalize malformed provenance into anonymous consumption", () => {
    const f = fixture();
    const admission = readTerminalAdmission(f.run, f.id)!;
    for (const value of [undefined, null, 1, "delivered", [], {}, { ...f.control, provenance: null },
      { ...f.control, provenance: { ...f.control.provenance, sender: { id: "broken" } } }]) {
      expect(terminalControlMatchesAdmission(value, admission)).toBe(false);
    }
    fs.writeFileSync(path.join(f.run, "steer.jsonl"), JSON.stringify({ id: f.id, type: "steer" }) + "\n");
    const anonymous = readTerminalAdmission(f.run, f.id)!;
    expect(terminalControlMatchesAdmission({ id: f.id, delivery: "steer" }, anonymous)).toBe(true);
    expect(terminalControlMatchesAdmission({ id: f.id, delivery: "steer", provenance: null }, anonymous)).toBe(false);
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), { id: f.id, delivery: "steer", state: "delivered", provenance: null });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    fs.writeFileSync(path.join(f.run, "steer.jsonl"), JSON.stringify({ id: f.id, type: "steer", provenance: null }) + "\n");
    expect(readTerminalAdmission(f.run, f.id)).toBeUndefined();
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it("compares copied sender identity rather than foreign envelope fields", () => {
    const f = fixture();
    const provenance = { ...f.control.provenance, sender: { ...f.control.provenance.sender, kind: "main", verified: "bridge", name: 42 } };
    fs.writeFileSync(path.join(f.run, "steer.jsonl"), JSON.stringify({ id: f.id, type: "steer", provenance }) + "\n");
    const control = { ...f.control, state: "delivered", provenance: { ...provenance, sender: {
      ...provenance.sender, kind: "remote", name: undefined, extra: "not identity",
    } } };
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), control);
    expect(terminalControlMatchesAdmission(control, readTerminalAdmission(f.run, f.id))).toBe(true);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  it("does not require refusal for controls already consumed by non-final context", async () => {
    const f = fixture();
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered" });
    fs.mkdirSync(path.join(f.run, "terminal-notices"));
    const delivered: unknown[] = [];
    new TerminalNoticeOutbox(notice => { delivered.push(notice); }).recover(f.run);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(delivered).toEqual([]);
    expect(fs.readdirSync(path.join(f.run, "terminal-notices"))).toEqual([]);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  it.each(["mismatched-answer", "mismatched-notice", "unknown-control", "pending-notice", "invalid-answer", "live-worker"])("fails closed for %s", bad => {
    const f = fixture();
    writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered" });
    if (bad === "mismatched-answer") writeJsonAtomic(path.join(f.run, "final-answer.json"), { ...f.receipt, runId: "other" });
    if (bad === "mismatched-notice") writeJsonAtomic(path.join(f.run, "terminal-notices", `${f.id}.json.delivered`), { ...f.notice, finalAnswerReceiptId: "other" });
    if (bad === "unknown-control") writeJsonAtomic(path.join(f.run, "terminal-controls", `${f.id}.json`), { ...f.control, state: "unknown" });
    if (bad === "pending-notice") writeJsonAtomic(path.join(f.run, "terminal-notices", `${f.id}.json`), f.notice);
    if (bad === "invalid-answer") fs.writeFileSync(path.join(f.run, "final-answer.json"), "{}");
    if (bad === "live-worker") writeJsonAtomic(path.join(f.run, "status.json"), { ...f.record, sessionId: String(process.pid) });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it.skipIf(process.platform === "win32")("refuses symlinked terminal control artifacts", () => {
    const f = fixture();
    const linked = path.join(f.root, "control.json"); writeJsonAtomic(linked, { ...f.control, state: "delivered" });
    fs.mkdirSync(path.join(f.run, "terminal-controls"));
    fs.symlinkSync(linked, path.join(f.run, "terminal-controls", `${f.id}.json`));
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
});
