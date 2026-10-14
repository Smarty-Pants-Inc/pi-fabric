import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { canRemoveTerminalRun, pruneActorRunArchives, runTreeExitVeto } from "../src/storage/retention.js";
import { initializeTaskControlDirectory, legacyTaskControlSibling, MAX_FINAL_ANSWER_BYTES, readFinalAnswerReceipt, taskControlDirectory, withLegacyTaskControlDirectory } from "../src/worker/terminal-answer.js";
import { bytesOf, day, json, originalFiles, registerArchiveCleanup, removalPaths, roots, setup, setupCurrent } from "./task-control-archive-fixtures.js";

// Keep reason: trust boundary / data safety. Exercise the actual archive and
// collection judgment, recording only the last native removal boundary.
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: vi.fn(() => ({ removed: true, bytes: 0 })) };
});
registerArchiveCleanup();
const nativeGetuid = Object.getOwnPropertyDescriptor(process, "getuid");
afterEach(() => {
  if (nativeGetuid) Object.defineProperty(process, "getuid", nativeGetuid);
  else Reflect.deleteProperty(process, "getuid");
});
// The archive's OWN fresh v2 current-only binding: bound to the held archive inode,
// named for the archive leaf, never the source grant, never a snapshot.
const expectOwnNullBinding = (run: string) => {
  const held = fs.lstatSync(run, { bigint: true });
  expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(run), ".legacy-read.json"), "utf8"))).toEqual({
    version: 2, runId: path.basename(run), dev: String(held.dev), ino: String(held.ino), snapshot: null });
  expect(fs.existsSync(path.join(taskControlDirectory(run), ".legacy-snapshot"))).toBe(false);
};
// Map an absolute original artifact (originalFiles) to its archived counterpart.
const archivedPath = (f: { source: string; controls: string }, file: string, archive: string) =>
  file.startsWith(f.controls + path.sep) ? path.join(taskControlDirectory(archive), path.relative(f.controls, file)) : path.join(archive, path.relative(f.source, file));

describe("actor archive paired task controls", () => {
  // central-v1 permits read-only unmarked pre-v2 in-run evidence, but never
  // promotes it into archive authority. Old siblings/unbound central still refuse.
  it.each(["sibling-v1", "legacy", "migrated"] as const)("retainRun preserves original %s evidence under an alias, then selects paired collection", async layout => {
    const f = setup(layout);
    const dirs = [f.source, legacyTaskControlSibling(f.source), taskControlDirectory(f.source)], before = bytesOf(...dirs);
    for (const file of originalFiles(f)) expect(before[file]).toBe(fs.readFileSync(file).toString("base64"));
    expect(() => initializeTaskControlDirectory(f.source)).toThrow("FABRIC_LEGACY_SNAPSHOT_REQUIRED");
    if (layout === "legacy") expect(readFinalAnswerReceipt(f.source, f.record.id)).toEqual(f.receipt);
    else expect(() => readFinalAnswerReceipt(f.source, f.record.id)).toThrow();
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(legacyTaskControlSibling(f.archive))).toBe(false);
    expect(canRemoveTerminalRun(f.source)).toBe(false);
    expect(removalPaths()).toEqual([]);
    expect(bytesOf(...dirs)).toEqual(before);
  });

  it("retainRun preserves current-v2 evidence under an alias with its own v2 binding, then selects paired collection", async () => {
    const f = setupCurrent();
    const sourceControls = taskControlDirectory(f.source);
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, f.source);
    expect(fs.readFileSync(path.join(f.archive, "status.json"))).toEqual(fs.readFileSync(path.join(f.source, "status.json")));
    expect(originalFiles(f)).toContain(path.join(sourceControls, "terminal-notices", f.admission.id + ".json.delivered"));
    for (const file of originalFiles(f)) {
      expect(fs.readFileSync(archivedPath(f, file, f.archive))).toEqual(fs.readFileSync(file));
    }
    expect(readFinalAnswerReceipt(f.archive, f.record.id)).toEqual(f.receipt);
    expect(readFinalAnswerReceipt(f.archive, f.alias)).toBeUndefined();
    expectOwnNullBinding(f.archive);
    expect(fs.readFileSync(path.join(taskControlDirectory(f.archive), ".legacy-read.json"))).not.toEqual(fs.readFileSync(path.join(sourceControls, ".legacy-read.json")));
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
    const f = setupCurrent();
    const nested = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.dirname(nested), { mode: 0o700 });
    // Seed the child by exclusive host allocation AT its final nested path; never move/copy a bound run.
    const child = setupCurrent("archive", nested);
    if (process.platform !== "win32") expect(fs.statSync(taskControlDirectory(nested)).mode & 0o777).toBe(0o700);
    expect(runTreeExitVeto(f.source, 0, undefined, true)).toBeUndefined();
    expect(readFinalAnswerReceipt(nested, child.record.id)).toEqual(child.receipt);
    expect(canRemoveTerminalRun(nested)).toBe(true);
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    await f.store.retainRun(f.actor, f.alias, f.source);
    const archivedChild = path.join(f.archive, "nested", "child");
    expect(readFinalAnswerReceipt(archivedChild, child.record.id)).toEqual(child.receipt);
    expectOwnNullBinding(archivedChild);
    // The source's old nested sibling is not copied; the archived child is central.
    expect(fs.existsSync(legacyTaskControlSibling(archivedChild))).toBe(false);
    expect(canRemoveTerminalRun(f.archive)).toBe(true);
    f.store.pruneRuns({ ...f.actor, lastRunId: "newest" }, 4 * day);
    // Nested central controls live outside the tree, so they join the plan.
    expect(removalPaths()).toEqual([f.archive, taskControlDirectory(f.archive), taskControlDirectory(archivedChild)]);
  });

  it("preserves opaque nested bytes alongside a genuine protected run pair", async () => {
    const f = setupCurrent();
    const nested = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.dirname(nested), { mode: 0o700 });
    const child = setupCurrent("archive", nested);
    const opaque = Buffer.from([0, 255, 10, 128, 65]);
    fs.writeFileSync(path.join(f.source, "nested", "plain.txt"), opaque, { mode: 0o600 });
    await f.store.retainRun(f.actor, f.alias, f.source);
    expect(fs.readFileSync(path.join(f.archive, "nested", "plain.txt"))).toEqual(opaque);
    expect(fs.readFileSync(path.join(f.source, "nested", "plain.txt"))).toEqual(opaque);
    const archivedChild = path.join(f.archive, "nested", "child");
    expect(child.controls).toBe(taskControlDirectory(nested));
    for (const file of originalFiles(child)) {
      expect(fs.readFileSync(archivedPath(child, file, archivedChild))).toEqual(fs.readFileSync(file));
    }
    expect(fs.readFileSync(path.join(archivedChild, "status.json"))).toEqual(fs.readFileSync(path.join(nested, "status.json")));
    expect(readFinalAnswerReceipt(archivedChild, child.record.id)).toEqual(child.receipt);
    expectOwnNullBinding(archivedChild);
    expect(canRemoveTerminalRun(archivedChild)).toBe(true);
    // Opaque content is archive data, not new authority to collect its parent.
    expect(canRemoveTerminalRun(f.archive)).toBe(false);
    expect(removalPaths()).toEqual([]);
  });

  it.each(["root", "nested"] as const)("rejects copy failure when %s protected evidence must transfer", async location => {
    const f = setupCurrent();
    // Precondition before any nested mutation: an opaque nested file legitimately vetoes collection.
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    const nested = path.join(f.source, "nested", "child");
    fs.mkdirSync(path.dirname(nested), { mode: 0o700 });
    // Current-v2 roots always carry protected controls; the nested child is a
    // genuine current run allocated at its final path (never moved).
    if (location === "nested") {
      const child = setupCurrent("archive", nested);
      expect(readFinalAnswerReceipt(nested, child.record.id)).toEqual(child.receipt);
      expect(canRemoveTerminalRun(f.source)).toBe(true);
    } else fs.writeFileSync(path.join(f.source, "nested", "plain.txt"), "opaque");
    const prune = vi.spyOn(f.store, "pruneRuns");
    const cp = vi.spyOn(fs, "cpSync").mockImplementation(() => { throw new Error("nested unavailable"); });
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow("nested unavailable");
    expect(cp).toHaveBeenCalled();
    expect(String(cp.mock.calls[0]![0])).toBe(path.join(f.source, "nested"));
    expect(prune).not.toHaveBeenCalled();
    expect(fs.existsSync(f.source)).toBe(true);
    expect(removalPaths()).toEqual([]);
  });

  it("keeps the durable route receipt and untouched source when archival completes", async () => {
    const f = setupCurrent("source-run");
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
    const f = setupCurrent(); await f.store.retainRun(f.actor, f.alias, f.source);
    const runs = path.dirname(f.archive);
    fs.mkdirSync(path.join(runs, "old-run"), { mode: 0o700 });
    fs.mkdirSync(legacyTaskControlSibling(path.join(runs, "old-run")), { mode: 0o700 });
    fs.mkdirSync(path.join(runs, "stray.ctl"), { mode: 0o700 });
    expect(f.store.retainedRunIds(f.actor)).toEqual([f.alias, "old-run", "stray.ctl"]);
  });

  it("preserves the late-reference fence and leaves the paired controls untouched", async () => {
    const f = setupCurrent(); await f.store.retainRun(f.actor, f.alias, f.source);
    expect(pruneActorRunArchives({ runsDirectory: path.dirname(f.archive), retentionMs: day, now: 4 * day,
      isRetained: (_id, final) => final === true })).toEqual([]);
    expect(removalPaths()).toEqual([]);
    expect(fs.readFileSync(path.join(taskControlDirectory(f.archive), "final-answer.json"), "utf8")).toContain(f.receipt.text);
  });

  it.each(["unknown", "hardlink", "subdirectory-link", "file-link", "oversized", "entry-bound", "missing", "foreign"] as const)("refuses unsafe %s protected controls without copying them or removing source", async bad => {
    const f = setupCurrent(); // f.controls: the protected central controls of a genuine v2 run
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    if (bad === "unknown") fs.writeFileSync(path.join(f.controls, "arbitrary-private-data"), "not an archive artifact");
    // Central state and the temporary run can be on different Windows volumes.
    // Keep link/rename fault artifacts beside the source namespace, not in temp.
    const extraLink = f.controls + ".extra-link", linkedAnswer = f.controls + ".linked-answer";
    if (bad === "hardlink") { fs.linkSync(path.join(f.controls, "final-answer.json"), extraLink); roots.push(extraLink); }
    if (bad === "subdirectory-link") {
      const source = path.join(f.controls, "terminal-controls"); fs.renameSync(source, source + "-original");
      fs.symlinkSync(source + "-original", source, "dir");
    }
    if (bad === "file-link") {
      const file = path.join(f.controls, "final-answer.json"); fs.renameSync(file, linkedAnswer); roots.push(linkedAnswer);
      fs.symlinkSync(linkedAnswer, file);
    }
    if (bad === "oversized") fs.writeFileSync(path.join(f.controls, "final-answer.json"), Buffer.alloc(MAX_FINAL_ANSWER_BYTES + 1));
    if (bad === "entry-bound") {
      for (let index = 0; index < 770; index++) json(path.join(f.controls, "terminal-controls", `extra-${index}.json`), { id: `extra-${index}` });
    }
    if (bad === "missing") fs.renameSync(f.controls, f.controls + "-missing");
    if (bad === "foreign") {
      // Supply both ownership capability and the platform's actual stat path:
      // Windows checks a directory with lstat, POSIX through its held fd.
      const identity = fs.statSync(f.controls), uid = process.getuid?.() ?? identity.uid;
      if (!process.getuid) Object.defineProperty(process, "getuid", { configurable: true, value: () => uid });
      const foreign = <T extends fs.Stats | fs.BigIntStats>(stat: T): T => {
        if (String(stat.dev) !== String(identity.dev) || String(stat.ino) !== String(identity.ino)) return stat;
        return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: typeof stat.uid === "bigint" ? BigInt(uid + 1) : uid + 1 });
      };
      const fstat = fs.fstatSync, lstat = fs.lstatSync;
      vi.spyOn(fs, "fstatSync").mockImplementation(((...args: Parameters<typeof fstat>) => foreign(fstat(...args))) as typeof fstat);
      vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof lstat>) => {
        const stat = lstat(...args);
        return stat ? foreign(stat) : stat;
      }) as typeof lstat);
    }
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(fs.existsSync(path.join(f.source, "status.json"))).toBe(true);
    expect(removalPaths()).toEqual([]);
  });

  it("rejects a renamed/replaced archive run during control creation even without a route receipt", async () => {
    // Central publication mkdirs `<central>.stage` (terminal-answer.ts publishCentral) then renames it;
    // swap the archive at that stage boundary so the post-publication identity check must refuse.
    const f = setupCurrent(), mkdir = fs.mkdirSync, stage = path.basename(taskControlDirectory(f.archive)) + ".stage";
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    let swapped = false;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((...args: Parameters<typeof mkdir>) => {
      const result = mkdir(...args);
      if (!swapped && path.basename(String(args[0])) === stage) {
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
    const f = setupCurrent(), target = path.join(f.root, "unrelated"); fs.mkdirSync(target, { mode: 0o700 });
    expect(canRemoveTerminalRun(f.source)).toBe(true);
    fs.mkdirSync(path.dirname(f.archive), { recursive: true, mode: 0o700 });
    fs.mkdirSync(path.dirname(taskControlDirectory(f.archive)), { recursive: true, mode: 0o700 });
    fs.symlinkSync(target, taskControlDirectory(f.archive), "dir");
    await expect(f.store.retainRun(f.actor, f.alias, f.source)).rejects.toThrow();
    expect(fs.readdirSync(target)).toEqual([]);
    expect(fs.existsSync(path.join(f.archive, "status.json"))).toBe(false);
    expect(removalPaths()).toEqual([]);
  });
});
