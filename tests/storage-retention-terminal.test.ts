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
import { saveFinalAnswerReceipt, taskControlDirectory, taskSteerFile } from "../src/worker/terminal-answer.js";
import { markTerminalControlForwarded, settleTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-retention-")); roots.push(root);
  const run = path.join(root, "run"); fs.mkdirSync(run); fs.mkdirSync(taskControlDirectory(run), { mode: 0o700 });
  const receipt = saveFinalAnswerReceipt(run, "run", "final answer");
  const record = { id: "run", runner: "pi", status: "completed", transport: "process", sessionId: "99999999", updatedAt: Date.now(),
    finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } };
  writeJsonAtomic(path.join(run, "status.json"), record);
  const sender = { id: "session:sender", kind: "main" as const, verified: "mesh" as const };
  const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const, sender };
  const id = "11111111-1111-4111-8111-111111111111";
  const notice = { code: "FABRIC_TARGET_TERMINAL", targetId: "run", finalAnswerReceiptId: receipt.id, delivery: "steer", messageId: id, sender };
  const control = { id, delivery: "steer", state: "refused", forwarded: false, provenance };
  fs.writeFileSync(taskSteerFile(run), JSON.stringify({ id, type: "steer", message: "stale", provenance }) + "\n");
  return { root, run, ctl: taskControlDirectory(run), receipt, record, id, notice, control };
};

describe("terminal-answer retention custody", () => {
  const acknowledgeFinalNotice = (f: ReturnType<typeof fixture>): void => {
    writeJsonAtomic(path.join(f.ctl, "terminal-notices", `${f.id}.json.delivered`), f.notice);
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
    // PR protected layout: the real holder locks inside the private control directory.
    const protectedLock = path.join(f.ctl, ".task-final-answer.lock");
    const owner = path.join(protectedLock, "owner");
    const [token, pid] = fs.readFileSync(owner, "utf8").trim().split("\n");
    expect(Number(pid)).toBe(child.pid);
    // Advance only the saved timestamp, not a sleep or the live-process fence.
    fs.writeFileSync(owner, `${token}\n${pid}\n${Date.now() - 60_000}\n`);
    // Interrupted staging/locks inside the protected namespace retain custody.
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    // Main behavior for the run-root lock name: a proven-stale exited holder is collected.
    fs.renameSync(protectedLock, path.join(f.run, ".task-final-answer.lock"));
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
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), f.control);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    writeJsonAtomic(path.join(f.ctl, "terminal-notices", `${f.id}.json`), f.notice);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it("collects only after retained sender-route ACK, including an unread tail and unlink crash", () => {
    const f = fixture();
    writeJsonAtomic(path.join(f.ctl, "terminal-notices", `${f.id}.json.delivered`), f.notice);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), f.control);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    writeJsonAtomic(path.join(f.ctl, "terminal-notices", `${f.id}.json`), f.notice);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  it("reconstructs a missing sender outbox after receipt-before-scan host crash", async () => {
    const f = fixture();
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), f.control);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    const delivered: unknown[] = [];
    const successor = new TerminalNoticeOutbox(notice => { delivered.push(notice); });
    successor.recover(f.run);
    await vi.waitFor(() => expect(fs.existsSync(path.join(f.ctl, "terminal-notices", `${f.id}.json.delivered`))).toBe(true));
    expect(delivered).toEqual([f.notice]);
    successor.recover(f.run);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(delivered).toHaveLength(1);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  // Recovery has no live worker: use the real admission, receipt, native registry,
  // outbox and route ACK files. Microtasks drain the outbox's Promise chain only.
  const drainNoticeRoute = async (): Promise<void> => {
    for (let i = 0; i < 8; i++) await Promise.resolve();
  };
  const queuedRecoveryFixture = () => {
    const f = fixture();
    trackTerminalControl(f.run, { id: f.id, delivery: "steer", provenance: f.control.provenance });
    expect(fs.existsSync(path.join(f.run, "lifecycle.jsonl"))).toBe(false);
    return f;
  };
  const refusalEvents = (file: string): { event: string; data: unknown }[] =>
    fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
  const sameInode = (a: fs.Stats, b: fs.Stats): boolean => a.dev === b.dev && a.ino === b.ino;
  const matchesRefusal = (event: { event: string; data: unknown }, notice: ReturnType<typeof fixture>["notice"]): boolean => {
    const data = event.data as typeof notice | undefined;
    return event.event === "message.refused" && data?.code === notice.code && data.targetId === notice.targetId &&
      data.messageId === notice.messageId && data.delivery === notice.delivery && data.finalAnswerReceiptId === notice.finalAnswerReceiptId &&
      data.sender?.id === notice.sender.id && data.sender.kind === notice.sender.kind && data.sender.verified === notice.sender.verified;
  };

  it("rehydrates an unforwarded refusal with lifecycle file and namespace barriers before registry publication, then routes once", async () => {
    const f = queuedRecoveryFixture();
    const registry = path.join(f.ctl, "terminal-controls", `${f.id}.json`);
    const lifecycle = path.join(f.run, "lifecycle.jsonl");
    const pending = path.join(f.ctl, "terminal-notices", `${f.id}.json`);
    // Protected writes may rename through a pinned descriptor path; match the registry entry, not the prefix.
    const isRegistry = (to: fs.PathLike): boolean =>
      path.basename(String(to)) === `${f.id}.json` && path.basename(path.dirname(String(to))) === "terminal-controls";
    const namespaces: fs.Stats[] = [];
    for (let directory = f.run; ; directory = path.dirname(directory)) {
      namespaces.push(fs.statSync(directory));
      if (path.dirname(directory) === directory) break;
    }
    let lifecycleSynced = false;
    const syncedNamespaces: fs.Stats[] = [];
    const publications: { queuedBefore: boolean; matchingEvent: boolean; fileSynced: boolean; namespaceSynced: boolean }[] = [];
    const nativeSync = fs.fsyncSync;
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const inode = fs.fstatSync(fd);
      nativeSync(fd);
      if (fs.existsSync(lifecycle) && sameInode(inode, fs.statSync(lifecycle)) &&
          refusalEvents(lifecycle).some(event => matchesRefusal(event, f.notice))) {
        lifecycleSynced = true;
        syncedNamespaces.length = 0; // Only namespace barriers AFTER the matching file fsync count.
      } else if (lifecycleSynced && inode.isDirectory()) syncedNamespaces.push(inode);
    });
    const nativeRename = fs.renameSync;
    const rename = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (isRegistry(to) && JSON.parse(fs.readFileSync(from, "utf8")).state === "refused") {
        // Observe synchronously BEFORE the actual registry rename, not after recover
        // returns. Assertions stay outside recover's intentionally fail-closed catch.
        publications.push({
          queuedBefore: JSON.parse(fs.readFileSync(registry, "utf8")).state === "queued",
          matchingEvent: refusalEvents(lifecycle).some(event => matchesRefusal(event, f.notice)),
          fileSynced: lifecycleSynced,
          namespaceSynced: process.platform === "win32" || namespaces.every(expected => syncedNamespaces.some(actual => sameInode(expected, actual))),
        });
      }
      nativeRename(from, to);
    });
    const delivered: unknown[] = [];
    const successor = new TerminalNoticeOutbox(notice => { delivered.push(notice); });
    try {
      successor.recover(f.run);
      await drainNoticeRoute();
    } finally { rename.mockRestore(); sync.mockRestore(); }
    expect(publications).toEqual([{ queuedBefore: true, matchingEvent: true, fileSynced: true, namespaceSynced: true }]);
    expect(JSON.parse(fs.readFileSync(registry, "utf8"))).toEqual({ ...f.control, forwarded: false });
    expect(delivered).toEqual([f.notice]);
    expect(JSON.parse(fs.readFileSync(`${pending}.delivered`, "utf8"))).toEqual(f.notice);
    expect(fs.existsSync(pending)).toBe(false);
    successor.recover(f.run);
    new TerminalNoticeOutbox(notice => { delivered.push(notice); }).recover(f.run);
    await drainNoticeRoute();
    expect(delivered).toEqual([f.notice]);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });

  it.each(["append", "fsync"] as const)("keeps queued custody and no sender ACK on lifecycle %s failure, then retries recovery", async fault => {
    const f = queuedRecoveryFixture();
    const registry = path.join(f.ctl, "terminal-controls", `${f.id}.json`);
    const before = fs.readFileSync(registry, "utf8");
    const lifecycle = path.join(f.run, "lifecycle.jsonl");
    const pending = path.join(f.ctl, "terminal-notices", `${f.id}.json`);
    const isLifecycle = (file: fs.PathLike | number): boolean => typeof file === "number"
      ? fs.existsSync(lifecycle) && sameInode(fs.fstatSync(file), fs.statSync(lifecycle))
      : String(file) === lifecycle;
    let faults = 0;
    const nativeAppend = fs.appendFileSync;
    const append = vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
      if (fault === "append" && isLifecycle(file)) {
        faults++;
        throw Object.assign(new Error("lifecycle append unavailable"), { code: "EIO" });
      }
      nativeAppend(file, data, options);
    });
    const nativeSync = fs.fsyncSync;
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fault === "fsync" && isLifecycle(fd)) {
        faults++;
        throw Object.assign(new Error("lifecycle fsync unavailable"), { code: "EIO" });
      }
      nativeSync(fd);
    });
    const delivered: unknown[] = [];
    const successor = new TerminalNoticeOutbox(notice => { delivered.push(notice); });
    try {
      successor.recover(f.run);
      await drainNoticeRoute();
      expect(faults).toBeGreaterThan(0);
      expect(fs.readFileSync(registry, "utf8")).toBe(before);
      expect(fs.existsSync(path.dirname(pending))).toBe(false);
      expect(fs.existsSync(`${pending}.delivered`)).toBe(false);
      expect(delivered).toEqual([]);
      expect(canRemoveTerminalRun(f.run)).toBe(false);
    } finally { append.mockRestore(); sync.mockRestore(); }
    successor.recover(f.run);
    await drainNoticeRoute();
    expect(JSON.parse(fs.readFileSync(registry, "utf8"))).toEqual({ ...f.control, forwarded: false });
    expect(refusalEvents(lifecycle)).toContainEqual(expect.objectContaining({ event: "message.refused", data: f.notice }));
    expect(delivered).toEqual([f.notice]);
    expect(JSON.parse(fs.readFileSync(`${pending}.delivered`, "utf8"))).toEqual(f.notice);
    successor.recover(f.run);
    await drainNoticeRoute();
    expect(delivered).toEqual([f.notice]);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });

  it.each([
    { state: "queued", forwarded: true }, { state: "queued", forwarded: undefined },
    { state: "delivered", forwarded: true }, { state: "delivered", forwarded: false },
  ])("never turns uncertain or consumed $state/$forwarded into undelivered during recovery", async transport => {
    const f = fixture();
    const registry = path.join(f.ctl, "terminal-controls", `${f.id}.json`);
    writeJsonAtomic(registry, { ...f.control, ...transport }, { durable: true });
    const before = fs.readFileSync(registry, "utf8");
    const delivered: unknown[] = [];
    const successor = new TerminalNoticeOutbox(notice => { delivered.push(notice); });
    successor.recover(f.run);
    successor.recover(f.run);
    await drainNoticeRoute();
    expect(fs.readFileSync(registry, "utf8")).toBe(before);
    expect(fs.existsSync(path.join(f.run, "lifecycle.jsonl"))).toBe(false);
    expect(fs.existsSync(path.join(f.ctl, "terminal-notices"))).toBe(false);
    expect(delivered).toEqual([]);
    expect(canRemoveTerminalRun(f.run)).toBe(transport.state === "delivered");
  });

  it("routes an actual forwarded native discard ACK without fabricating a worker protocol event", async () => {
    const f = queuedRecoveryFixture();
    // These are the native child's real durable registry APIs, not a synthetic
    // lifecycle claim. A forwarded queued row is still uncertain until this ACK.
    expect(markTerminalControlForwarded(f.run, f.id)).toBe(true);
    const delivered: unknown[] = [];
    const successor = new TerminalNoticeOutbox(notice => { delivered.push(notice); });
    successor.recover(f.run);
    await drainNoticeRoute();
    expect(delivered).toEqual([]);
    expect(settleTerminalControl(f.run, f.id, "refused")).toMatchObject({ state: "refused", forwarded: true });
    const registry = path.join(f.ctl, "terminal-controls", `${f.id}.json`);
    const acknowledged = fs.readFileSync(registry, "utf8");
    successor.recover(f.run);
    await drainNoticeRoute();
    expect(delivered).toEqual([f.notice]);
    expect(JSON.parse(fs.readFileSync(path.join(f.ctl, "terminal-notices", `${f.id}.json.delivered`), "utf8"))).toEqual(f.notice);
    successor.recover(f.run);
    await drainNoticeRoute();
    expect(delivered).toEqual([f.notice]);
    expect(fs.readFileSync(registry, "utf8")).toBe(acknowledged);
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
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), control);
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    const pending = path.join(f.ctl, "terminal-notices", `${f.id}.json`);
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
    expect(JSON.parse(fs.readFileSync(path.join(f.ctl, "terminal-controls", `${f.id}.json`), "utf8"))).toEqual(JSON.parse(JSON.stringify(control)));
    expect(readTerminalAdmission(f.run, f.id)).toEqual({ id: f.id, delivery: "steer", sender });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: f.root, retentionMs: 0, now: Date.now() + 1, dryRun: true })).toEqual([]);
    expect(fs.existsSync(f.run)).toBe(true);
  });
  it.each(["delivery", "sender", "kind", "name", "verified"])("does not discharge a mismatched row with an incorrectly attributed refusal %s", mismatch => {
    const f = fixture();
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered", delivery: "followUp" });
    writeJsonAtomic(path.join(f.ctl, "terminal-notices", `${f.id}.json.delivered`), {
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
    fs.rmSync(path.join(f.ctl, "final-answer.json"));
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered" });
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered", delivery: "followUp" });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    fs.rmSync(taskSteerFile(f.run));
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it("retains unread admission debt even without a final answer or any consumption registry", () => {
    const f = fixture();
    fs.renameSync(path.join(f.ctl, "final-answer.json"), path.join(f.root, "held-final-answer.json"));
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered" });
    expect(canRemoveTerminalRun(f.run)).toBe(true);
    const other = "unread-admission";
    fs.appendFileSync(taskSteerFile(f.run), JSON.stringify({ id: other, type: "steer", provenance: f.control.provenance }) + "\n");
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it("fails closed for unknown rows and does not normalize malformed provenance into anonymous consumption", () => {
    const f = fixture();
    const admission = readTerminalAdmission(f.run, f.id)!;
    for (const value of [undefined, null, 1, "delivered", [], {}, { ...f.control, provenance: null },
      { ...f.control, provenance: { ...f.control.provenance, sender: { id: "broken" } } }]) {
      expect(terminalControlMatchesAdmission(value, admission)).toBe(false);
    }
    fs.writeFileSync(taskSteerFile(f.run), JSON.stringify({ id: f.id, type: "steer" }) + "\n");
    const anonymous = readTerminalAdmission(f.run, f.id)!;
    expect(terminalControlMatchesAdmission({ id: f.id, delivery: "steer" }, anonymous)).toBe(true);
    expect(terminalControlMatchesAdmission({ id: f.id, delivery: "steer", provenance: null }, anonymous)).toBe(false);
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), { id: f.id, delivery: "steer", state: "delivered", provenance: null });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
    fs.writeFileSync(taskSteerFile(f.run), JSON.stringify({ id: f.id, type: "steer", provenance: null }) + "\n");
    expect(readTerminalAdmission(f.run, f.id)).toBeUndefined();
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it("compares copied sender identity rather than foreign envelope fields", () => {
    const f = fixture();
    const provenance = { ...f.control.provenance, sender: { ...f.control.provenance.sender, kind: "main", verified: "bridge", name: 42 } };
    fs.writeFileSync(taskSteerFile(f.run), JSON.stringify({ id: f.id, type: "steer", provenance }) + "\n");
    const control = { ...f.control, state: "delivered", provenance: { ...provenance, sender: {
      ...provenance.sender, kind: "remote", name: undefined, extra: "not identity",
    } } };
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), control);
    expect(terminalControlMatchesAdmission(control, readTerminalAdmission(f.run, f.id))).toBe(true);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  it("does not require refusal for controls already consumed by non-final context", async () => {
    const f = fixture();
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered" });
    fs.mkdirSync(path.join(f.ctl, "terminal-notices"));
    const delivered: unknown[] = [];
    new TerminalNoticeOutbox(notice => { delivered.push(notice); }).recover(f.run);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(delivered).toEqual([]);
    expect(fs.readdirSync(path.join(f.ctl, "terminal-notices"))).toEqual([]);
    expect(canRemoveTerminalRun(f.run)).toBe(true);
  });
  it.each(["mismatched-answer", "mismatched-notice", "unknown-control", "pending-notice", "invalid-answer", "live-worker"])("fails closed for %s", bad => {
    const f = fixture();
    writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), { ...f.control, state: "delivered" });
    if (bad === "mismatched-answer") writeJsonAtomic(path.join(f.ctl, "final-answer.json"), { ...f.receipt, runId: "other" });
    if (bad === "mismatched-notice") writeJsonAtomic(path.join(f.ctl, "terminal-notices", `${f.id}.json.delivered`), { ...f.notice, finalAnswerReceiptId: "other" });
    if (bad === "unknown-control") writeJsonAtomic(path.join(f.ctl, "terminal-controls", `${f.id}.json`), { ...f.control, state: "unknown" });
    if (bad === "pending-notice") writeJsonAtomic(path.join(f.ctl, "terminal-notices", `${f.id}.json`), f.notice);
    if (bad === "invalid-answer") fs.writeFileSync(path.join(f.ctl, "final-answer.json"), "{}");
    if (bad === "live-worker") writeJsonAtomic(path.join(f.run, "status.json"), { ...f.record, sessionId: String(process.pid) });
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
  it.skipIf(process.platform === "win32")("refuses symlinked terminal control artifacts", () => {
    const f = fixture();
    const linked = path.join(f.root, "control.json"); writeJsonAtomic(linked, { ...f.control, state: "delivered" });
    fs.mkdirSync(path.join(f.ctl, "terminal-controls"));
    fs.symlinkSync(linked, path.join(f.ctl, "terminal-controls", `${f.id}.json`));
    expect(canRemoveTerminalRun(f.run)).toBe(false);
  });
});
