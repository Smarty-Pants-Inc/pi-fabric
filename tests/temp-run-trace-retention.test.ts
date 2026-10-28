import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { shortTmpdirName } from "../src/agents/short-tmpdir.js";
import { FABRIC_RUN_ROOT_PREFIX, markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

const roots: string[] = [];
const HOUR = 60 * 60 * 1_000;
const DAY = 24 * HOUR;
const temporaryDirectory = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-run-trace-retention-"));
  roots.push(root);
  return root;
};
const writeStatus = (directory: string, record: Record<string, unknown>): void => {
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify(record));
};
const filesWithBytes = (directory: string, name: string): Buffer[] => {
  const found: Buffer[] = [];
  const visit = (current: string): void => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const file = path.join(current, entry.name);
      if (entry.isDirectory()) visit(file);
      else if (entry.isFile() && entry.name.includes(name)) found.push(fs.readFileSync(file));
    }
  };
  visit(directory);
  return found;
};

/** Snapshot original paths as well as bytes: a move into a graveyard is NOT preservation in place. */
const fileBytes = (directory: string): Record<string, Buffer> => {
  const files: Record<string, Buffer> = {};
  const visit = (current: string): void => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const file = path.join(current, entry.name);
      if (entry.isDirectory()) visit(file);
      else if (entry.isFile()) files[path.relative(directory, file)] = fs.readFileSync(file);
    }
  };
  visit(directory);
  return files;
};
/** After the injected refresh, record deletion attempts without executing them; moves still expose the real race. */
const recordNamespaceAfter = (active: () => boolean): string[] => {
  const calls: string[] = [];
  const { unlinkSync, rmdirSync, mkdirSync, renameSync } = fs;
  vi.spyOn(fs, "unlinkSync").mockImplementation(((file: fs.PathLike) => {
    if (active()) calls.push(`unlink:${String(file)}`);
    else unlinkSync(file);
  }) as typeof fs.unlinkSync);
  vi.spyOn(fs, "rmdirSync").mockImplementation(((file: fs.PathLike) => {
    if (active()) calls.push(`rmdir:${String(file)}`);
    else rmdirSync(file);
  }) as typeof fs.rmdirSync);
  vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: fs.MakeDirectoryOptions | fs.Mode | null) => {
    if (active()) calls.push(`mkdir:${String(file)}`);
    return mkdirSync(file, options);
  }) as typeof fs.mkdirSync);
  vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
    if (active()) calls.push(`rename:${String(from)}->${String(to)}`);
    renameSync(from, to);
  }) as typeof fs.renameSync);
  return calls;
};

afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("temporary run trace retention", () => {
  it("keeps every raw JSONL trace and metadata anchor byte-identical while pruning eligible bloat", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}closed`);
    markRunRootActive(root, 1);
    const run = path.join(root, "run");
    writeStatus(run, { status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    fs.writeFileSync(path.join(run, "task.txt"), "completed task payload");
    fs.writeFileSync(path.join(run, "reply.json"), '{"text":"completed result"}');
    fs.writeFileSync(path.join(run, "run-identity.json"), '{"run":"stable"}');
    fs.mkdirSync(path.join(run, "tmp", "cache"), { recursive: true });
    fs.writeFileSync(path.join(run, "tmp", "output.txt"), "temporary output");
    fs.writeFileSync(path.join(run, "tmp", "cache", "compiler.cache"), "temporary cache");

    const traces: Array<{ relative: string; bytes: Buffer }> = [
      { relative: "events.jsonl", bytes: Buffer.from('{"event":"original"}\n') },
      { relative: "lifecycle.jsonl", bytes: Buffer.from('{"phase":"finished"}\n') },
      { relative: "steer.jsonl", bytes: Buffer.from('{"type":"worker_observation"}\n') },
      { relative: "route-session.jsonl", bytes: Buffer.from('{"session":"route"}\n') },
      { relative: "session.jsonl", bytes: Buffer.from('{"message":"session"}\n') },
      { relative: "session.jsonl.20260101T000000Z.bak", bytes: Buffer.from('{"message":"rotated backup"}\n') },
      { relative: "session.jsonl.orphaned.bak", bytes: Buffer.from('{"message":"recovery backup"}\n') },
      { relative: "route-session.jsonl.reclaim", bytes: Buffer.from('{"session":"reclaim"}\n') },
      { relative: "tmp/worker-session.jsonl", bytes: Buffer.from('{"worker":"scratch"}\n') },
      { relative: "tmp/cache/session.jsonl.reclaim", bytes: Buffer.from('{"worker":"scratch reclaim"}\n') },
      { relative: "durable/initial/store/main.jsonl", bytes: Buffer.from('{"durable":"main"}\n') },
      { relative: "durable/initial/store/doc-0.jsonl", bytes: Buffer.from('{"durable":"doc"}\n') },
      { relative: "durable/initial/store/doc-1.jsonl.reclaim", bytes: Buffer.from('{"durable":"reclaim"}\n') },
      { relative: "handoff-session/fork.jsonl", bytes: Buffer.from('{"handoff":"fork"}\n') },
      { relative: "nested/worker/events.jsonl", bytes: Buffer.from('{"nested":"events"}\n') },
      { relative: "nested/worker/session.jsonl.backup", bytes: Buffer.from('{"nested":"session backup"}\n') },
    ];
    for (const trace of traces) {
      const file = path.join(run, trace.relative);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, trace.bytes);
    }
    writeStatus(path.join(run, "nested", "worker"), {
      status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647",
    });

    markRunRootClosed(root, 1, true);
    const ownerBefore = fs.readFileSync(path.join(root, ".fabric-owner.json"));
    const statusBefore = fs.readFileSync(path.join(run, "status.json"));
    const identityBefore = fs.readFileSync(path.join(run, "run-identity.json"));
    const nestedStatusBefore = fs.readFileSync(path.join(run, "nested", "worker", "status.json"));
    const kill = vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2_147_483_647) throw Object.assign(new Error("process is absent"), { code: "ESRCH" });
      return true;
    });
    try {
      sweepTempRunRoots({
        tempRoot,
        now: 100 * DAY,
        orphanedTempRunRetentionMs: 6 * HOUR,
        oneShotRunRetentionMs: DAY,
      });

      expect(fs.readFileSync(path.join(root, ".fabric-owner.json"))).toEqual(ownerBefore);
      expect(fs.readFileSync(path.join(run, "status.json"))).toEqual(statusBefore);
      expect(fs.readFileSync(path.join(run, "run-identity.json"))).toEqual(identityBefore);
      expect(fs.readFileSync(path.join(run, "nested", "worker", "status.json"))).toEqual(nestedStatusBefore);
      for (const trace of traces) {
        expect(fs.readFileSync(path.join(run, trace.relative)), trace.relative).toEqual(trace.bytes);
      }
      for (const file of ["task.txt", "reply.json", "tmp/output.txt", "tmp/cache/compiler.cache"]) {
        expect(fs.existsSync(path.join(run, file)), file).toBe(false);
      }
      expect(filesWithBytes(run, ".jsonl")).toHaveLength(traces.length);
    } finally {
      kill.mockRestore();
    }
  });

  it.each(["jsonl", "ndjson"])("keeps a %s trace in its recorded external short TMPDIR while pruning scratch bloat", extension => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}external-scratch`);
    markRunRootActive(root, 1);
    const runId = "external-run";
    const run = path.join(root, runId);
    const scratch = path.join(tempRoot, shortTmpdirName(runId));
    fs.mkdirSync(scratch, { mode: 0o700 });
    writeStatus(run, {
      status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647", temporaryDirectory: scratch,
    });
    fs.writeFileSync(path.join(run, "task.txt"), "eligible task bloat");
    fs.writeFileSync(path.join(run, "reply.json"), '{"text":"eligible reply bloat"}');
    const scratchTrace = Buffer.from('{"worker":"external scratch trace"}\n');
    const traceFile = path.join(scratch, `worker-session.${extension}`);
    fs.writeFileSync(traceFile, scratchTrace);
    fs.mkdirSync(path.join(scratch, "cache"), { recursive: true });
    fs.writeFileSync(path.join(scratch, "cache", "compiler.cache"), "eligible scratch cache");
    fs.writeFileSync(path.join(scratch, "output.txt"), "eligible scratch output");
    markRunRootClosed(root, 1, true);
    const statusBefore = fs.readFileSync(path.join(run, "status.json"));
    const kill = vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2_147_483_647) throw Object.assign(new Error("process is absent"), { code: "ESRCH" });
      return true;
    });
    try {
      const result = sweepTempRunRoots({
        tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
      });
      expect(result).toEqual({ removedRuns: [], removedRoots: [] });
      expect(fs.existsSync(run)).toBe(true);
      expect(fs.readFileSync(path.join(run, "status.json"))).toEqual(statusBefore);
      expect(fs.readFileSync(traceFile)).toEqual(scratchTrace);
      for (const file of ["task.txt", "reply.json"]) expect(fs.existsSync(path.join(run, file))).toBe(false);
      for (const file of ["cache/compiler.cache", "output.txt"]) expect(fs.existsSync(path.join(scratch, file))).toBe(false);
    } finally {
      kill.mockRestore();
    }
  });

  it.skipIf(process.platform !== "linux").each(["jsonl", "ndjson"])("keeps a %s trace created after artifact judgment during verified removal", extension => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}trace-race`);
    markRunRootActive(root, 1);
    const run = path.join(root, "run");
    fs.mkdirSync(run);
    const artifacts = ["task.txt", "reply.json"].map(name => path.join(run, name));
    fs.writeFileSync(artifacts[0]!, "eligible task bloat");
    fs.writeFileSync(artifacts[1]!, '{"text":"eligible reply bloat"}');
    writeStatus(run, { status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    const originalTrace = Buffer.from('{"event":"judged trace"}\n');
    const lateTrace = Buffer.from('{"event":"appeared during removal"}\n');
    fs.writeFileSync(path.join(run, "events.jsonl"), originalTrace);
    markRunRootClosed(root, 1, true);
    const statusBefore = fs.readFileSync(path.join(run, "status.json"));
    const old = new Date(Date.now() - 30 * DAY);
    fs.utimesSync(run, old, old);
    const before = fileBytes(run), lateFile = path.join(run, `late-worker.${extension}`);
    let injected = false, beforeInjection: unknown, refreshedMtime: number | undefined;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (artifacts.includes(target) && !injected) {
        beforeInjection = fileBytes(run);
        injected = true;
        fs.writeFileSync(lateFile, lateTrace);
        refreshedMtime = fs.lstatSync(run).mtimeMs;
      }
    };
    const namespaceCalls = recordNamespaceAfter(() => injected);
    const kill = vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2_147_483_647) throw Object.assign(new Error("process is absent"), { code: "ESRCH" });
      return true;
    });
    try {
      const result = sweepTempRunRoots({
        tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
      });
      expect(injected).toBe(true);
      expect(beforeInjection, "inject at the first eligible artifact, before any prior removal").toEqual(before);
      expect(result).toEqual({ removedRuns: [], removedRoots: [] });
      expect(fs.existsSync(run)).toBe(true);
      expect(fs.readFileSync(path.join(run, "status.json"))).toEqual(statusBefore);
      expect(fs.readFileSync(path.join(run, "events.jsonl"))).toEqual(originalTrace);
      expect(fs.readFileSync(lateFile)).toEqual(lateTrace);
      // The late trace changes the judged root namespace/mtime: preserve eligible bloat too rather than partially prune.
      for (const artifact of artifacts) expect(fs.existsSync(artifact), artifact).toBe(true);
      expect(namespaceCalls, "refuse before any graveyard mkdir, move, unlink or rmdir").toEqual([]);
      expect(fileBytes(run)).toEqual({ ...before, [path.basename(lateFile)]: lateTrace });
      expect(fs.lstatSync(run).mtimeMs, "do not backdate the late trace's root refresh").toBe(refreshedMtime);
    } finally {
      delete verifiedRemoveTestHooks.beforeOpen;
      kill.mockRestore();
    }
  });

  it("ordinary multi-nontrace cleanup advances only its own mutations across nested and external directories", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}ordinary-cleanup`);
    markRunRootActive(root, 1);
    const runId = "ordinary-run", run = path.join(root, runId);
    const scratch = path.join(tempRoot, shortTmpdirName(runId));
    fs.mkdirSync(path.join(scratch, "cache", "nested"), { recursive: true, mode: 0o700 });
    writeStatus(run, {
      status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647", temporaryDirectory: scratch,
    });
    fs.writeFileSync(path.join(run, "task.txt"), "ordinary task");
    fs.writeFileSync(path.join(run, "reply.json"), '{"text":"ordinary reply"}');
    fs.mkdirSync(path.join(run, "tmp", "cache"), { recursive: true });
    fs.writeFileSync(path.join(run, "tmp", "cache", "output.txt"), "ordinary nested scratch");
    const child = path.join(run, "nested", "child");
    writeStatus(child, { status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    fs.writeFileSync(path.join(child, "reply.json"), '{"text":"ordinary child reply"}');
    fs.writeFileSync(path.join(scratch, "output.txt"), "ordinary external output");
    fs.writeFileSync(path.join(scratch, "cache", "nested", "compiler.cache"), "ordinary external cache");
    markRunRootClosed(root, 1, true);
    const seen: string[] = [];
    verifiedRemoveTestHooks.beforeOpen = target => { seen.push(target); };
    const expectedArtifacts = [...Object.keys(fileBytes(run)).map(name => path.join(run, name)),
      ...Object.keys(fileBytes(scratch)).map(name => path.join(scratch, name))];

    expect(sweepTempRunRoots({
      tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
    })).toEqual({ removedRuns: [run], removedRoots: [root] });
    // Includes status+reply even if status is encountered first: our own status unlink must not invent a custody veto.
    for (const artifact of expectedArtifacts) expect(seen, artifact).toContain(artifact);
    expect(fs.existsSync(run)).toBe(false);
    expect(fs.existsSync(scratch)).toBe(false);
    expect(fs.existsSync(root)).toBe(false);
  });

  it.skipIf(process.platform !== "linux")("does not adopt an external refresh between its first and second nontrace removals", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}intervening-refresh`);
    markRunRootActive(root, 1);
    const run = path.join(root, "run");
    writeStatus(run, { status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    fs.writeFileSync(path.join(run, "events.jsonl"), '{"event":"trace anchor"}\n');
    const artifacts = ["task.txt", "reply.json"].map(name => path.join(run, name));
    for (const artifact of artifacts) fs.writeFileSync(artifact, `original ${path.basename(artifact)}`);
    markRunRootClosed(root, 1, true);
    const before = fileBytes(run), seams: string[] = [];
    let injected = false, firstWasRemoved = false, refreshedMtime: number | undefined;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (!artifacts.includes(target) || seams.includes(target)) return;
      seams.push(target);
      if (seams.length !== 2) return;
      firstWasRemoved = !fs.existsSync(seams[0]!);
      const refreshed = new Date(Date.now() + 5_000);
      fs.utimesSync(run, refreshed, refreshed);
      refreshedMtime = fs.lstatSync(run).mtimeMs;
      injected = true;
    };
    const namespaceCalls = recordNamespaceAfter(() => injected);

    expect(sweepTempRunRoots({
      tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
    })).toEqual({ removedRuns: [], removedRoots: [] });
    expect(seams).toHaveLength(2);
    expect(firstWasRemoved, "the helper completed its first removal before the external refresh").toBe(true);
    expect(injected).toBe(true);
    const expected = { ...before };
    delete expected[path.basename(seams[0]!)];
    expect(fileBytes(run), "only the completed first removal is authorized").toEqual(expected);
    expect(namespaceCalls, "no second graveyard mkdir/move/delete after the refresh").toEqual([]);
    expect(fs.lstatSync(run).mtimeMs, "do not adopt or backdate the external refresh").toBe(refreshedMtime);
  });

  it.skipIf(process.platform !== "linux").each([
    ["nested ancestor", "run", "tmp"],
    ["nested parent", "run", "tmp/cache"],
    ["external scratch root", "scratch", ""],
    ["external scratch ancestor", "scratch", "cache"],
    ["external scratch parent", "scratch", "cache/nested"],
  ])("keeps judged artifacts and the refreshed mtime when the %s changes before its first removal", (_name, tree, relative) => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}directory-refresh`);
    markRunRootActive(root, 1);
    const runId = "refresh-run", run = path.join(root, runId);
    const scratch = tree === "scratch" ? path.join(tempRoot, shortTmpdirName(runId)) : undefined;
    if (scratch) fs.mkdirSync(scratch, { mode: 0o700 });
    writeStatus(run, {
      status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647",
      ...(scratch ? { temporaryDirectory: scratch } : {}),
    });
    const judgedTree = scratch ?? run;
    fs.writeFileSync(path.join(judgedTree, "events.jsonl"), '{"event":"preserved trace"}\n');
    const parent = path.join(judgedTree, scratch ? "cache/nested" : "tmp/cache");
    fs.mkdirSync(parent, { recursive: true });
    const artifacts = ["output.txt", "compiler.cache"].map(name => path.join(parent, name));
    for (const artifact of artifacts) fs.writeFileSync(artifact, `keep ${path.basename(artifact)}`);
    const refreshedDirectory = path.join(judgedTree, relative!);
    const old = new Date(Date.now() - 30 * DAY);
    fs.utimesSync(refreshedDirectory, old, old);
    markRunRootClosed(root, 1, true);
    const snapshot = () => ({ run: fileBytes(run), scratch: scratch ? fileBytes(scratch) : undefined });
    const before = snapshot();
    let firstArtifact: string | undefined, beforeRefresh: unknown, refreshedMtime: number | undefined;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (!artifacts.includes(target) || firstArtifact !== undefined) return;
      firstArtifact = target;
      beforeRefresh = snapshot();
      const refreshed = new Date(Date.now() + 5_000);
      fs.utimesSync(refreshedDirectory, refreshed, refreshed);
      refreshedMtime = fs.lstatSync(refreshedDirectory).mtimeMs;
    };
    const namespaceCalls = recordNamespaceAfter(() => firstArtifact !== undefined);

    expect(sweepTempRunRoots({
      tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
    })).toEqual({ removedRuns: [], removedRoots: [] });
    expect(artifacts).toContain(firstArtifact);
    expect(beforeRefresh, "the first eligible seam precedes every artifact removal").toEqual(before);
    expect(namespaceCalls, "the changed captured directory vetoes before namespace mutation").toEqual([]);
    expect(snapshot(), "all original artifact and trace paths stay byte-identical").toEqual(before);
    expect(fs.lstatSync(refreshedDirectory).mtimeMs, "never backdate an external refresh").toBe(refreshedMtime);
  });

  it("keeps unresolved nested-worker custody and all of its bytes intact", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}nested-custody`);
    markRunRootActive(root, 1);
    const parent = path.join(root, "parent");
    writeStatus(parent, { status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    fs.writeFileSync(path.join(parent, "task.txt"), "parent payload");
    const child = path.join(parent, "nested", "worker");
    writeStatus(child, { status: "failed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    fs.writeFileSync(path.join(child, "events.jsonl"), '{"child":"trace"}\n');
    fs.writeFileSync(path.join(child, "unresolved-worker.json"), '{"reason":"worker custody is unresolved"}');
    markRunRootClosed(root, 1, true);
    const before = new Map<string, Buffer>();
    const visit = (directory: string): void => {
      for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        const file = path.join(directory, entry.name);
        if (entry.isDirectory()) visit(file);
        else if (entry.isFile()) before.set(path.relative(root, file), fs.readFileSync(file));
      }
    };
    visit(root);
    const kill = vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2_147_483_647) throw Object.assign(new Error("process is absent"), { code: "ESRCH" });
      return true;
    });
    try {
      sweepTempRunRoots({
        tempRoot,
        now: 100 * DAY,
        orphanedTempRunRetentionMs: 6 * HOUR,
        oneShotRunRetentionMs: DAY,
      });
      for (const [relative, bytes] of before) {
        expect(fs.readFileSync(path.join(root, relative)), relative).toEqual(bytes);
      }
      expect(fs.existsSync(parent)).toBe(true);
      expect(fs.existsSync(child)).toBe(true);
    } finally {
      kill.mockRestore();
    }
  });
});
