import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as cleanupCustody from "../src/agents/worktree-manager.js";
import { RESIDENT_HOST_FORMAT, residentResultPath } from "../src/residency/protocol.js";

// Exercise Windows' main sweep by toggling its gate, not process.platform: the real
// custody helper then uses this host's native behavior (no foreign DACL tooling).
const platform = vi.hoisted(() => ({ v2: true }));
const verifiedRemovals = vi.hoisted(() => [] as string[]);
vi.mock("../src/storage/retention-platform.js", () => ({ retentionV2Enabled: () => platform.v2 }));
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return { ...actual, verifiedRemove: (target: string, _judged: unknown, _options: { deleter: string; dryRun?: boolean }) => {
    verifiedRemovals.push(target);
    return { removed: false, reason: "recorded only (test)" };
  } };
});
const { ResidentRequestRetention, sweepResidentRuns } = await import("../src/residency/retention.js");

const DAY = 86_400_000, EXITED_PID = 2_147_483_647, ID = "expired-resident-run";
const REASON = "resident run retention whole-root custody";
const payloads = {
  "session.jsonl": Buffer.from('{"type":"session","version":3}\n{"type":"message","id":"partial"'),
  "durable/initial/store/main.jsonl": Buffer.from([0x7b, 0x22, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x22, 0x3a, 0xff, 0x00]),
  "handoff-session/fork.jsonl": Buffer.from('{"type":"session","id":"fork"}\n{"type":"message","content":"fork bytes"}\n'),
};
const roots: string[] = [];
afterEach(() => {
  platform.v2 = true; vi.restoreAllMocks(); verifiedRemovals.length = 0;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const json = (file: string, value: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
};
const fixture = () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "resident-run-archive-"))); roots.push(root);
  const kill = process.kill.bind(process);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    if (pid === EXITED_PID && signal === 0) throw Object.assign(new Error("kill ESRCH"), { code: "ESRCH" });
    return kill(pid, signal);
  });
  const runs = path.join(root, "runs"), run = path.join(runs, ID), now = Date.now(), aged = now - 3 * DAY;
  const result = { id: ID, name: "retention fixture", task: "preserve resident traces", cwd: root,
    runner: "pi", transport: "process", status: "completed", startedAt: aged - DAY, finishedAt: aged,
    updatedAt: aged, turns: 1, toolCalls: 0, text: "saved outcome", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  json(path.join(run, "status.json"), { ...result, sessionId: String(EXITED_PID) });
  for (const [relative, bytes] of Object.entries(payloads)) {
    const file = path.join(run, relative); fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 }); fs.writeFileSync(file, bytes);
  }
  json(path.join(root, "agents", `${ID}.json`), { format: RESIDENT_HOST_FORMAT, id: ID, rootId: "session:fixture",
    runDirectory: run, handle: { id: ID, name: result.name, cwd: root, runner: "pi", transport: "process", status: "completed", residency: "durable" },
    createdAt: result.startedAt, updatedAt: result.updatedAt });
  const resultFile = residentResultPath(root, ID); json(resultFile, result);
  fs.utimesSync(run, aged / 1000, aged / 1000);
  return { root, runs, run, now, resultFile, resultBytes: fs.readFileSync(resultFile), status: fs.readFileSync(path.join(run, "status.json")) };
};
const expectPayloads = (run: string) => {
  for (const [relative, bytes] of Object.entries(payloads)) expect(fs.readFileSync(path.join(run, relative)).equals(bytes), relative).toBe(true);
};
const expectPreserved = (f: ReturnType<typeof fixture>, run: string) => {
  expectPayloads(run);
  expect(fs.readFileSync(path.join(run, "status.json")).equals(f.status)).toBe(true);
  expect(fs.readFileSync(f.resultFile).equals(f.resultBytes)).toBe(true);
};
const archiveRaces = (["symlink", "replaced inode", "refreshed mtime", "foreign owner"] as const)
  .filter(race => (race !== "symlink" || process.platform !== "win32") && (race !== "foreign owner" || !!process.getuid));
type Mode = "offline" | "v2" | "main";
const sweep = (mode: Mode, f: ReturnType<typeof fixture>, live = new Set<string>(), dryRun = false): string[] | undefined => {
  if (mode === "offline") return sweepResidentRuns(f.runs, f.now, 10_000,
    { retainRuns: false, retentionMs: DAY, dryRun });
  platform.v2 = mode === "v2";
  const retention = new ResidentRequestRetention(f.root, [], { retainRuns: false, residentRunMs: DAY });
  try { retention.sweep(f.now, live, 10_000); } finally { retention.close(); }
  return undefined;
};
const expectArchived = (f: ReturnType<typeof fixture>) => {
  const archive = `${f.runs}.trace-archive`;
  expect(fs.existsSync(f.run)).toBe(false);
  const containers = fs.readdirSync(archive).filter(name => name.startsWith("cleanup-"));
  expect(containers).toHaveLength(1);
  const directory = path.join(archive, containers[0]!), receipt = JSON.parse(fs.readFileSync(path.join(directory, "custody.json"), "utf8"));
  expect(receipt).toMatchObject({ id: ID, reason: REASON, source: f.run, destination: path.join(directory, "tree"), state: "moved" });
  expectPayloads(receipt.destination);
  expect(fs.readFileSync(path.join(receipt.destination, "status.json")).equals(f.status)).toBe(true);
  expect(fs.readFileSync(f.resultFile).equals(f.resultBytes)).toBe(true);
  expect(verifiedRemovals.some(target => target === f.run || target.startsWith(`${f.run}${path.sep}`))).toBe(false);
};

describe("expired resident run trace custody", () => {
  it.each([["offline", "offline"], ["streaming V2", "v2"], ["streaming Windows/main gate", "main"]] as const)(
    "%s archives session, durable-store and handoff bytes whole", (_name, mode) => {
      const f = fixture();
      const result = sweep(mode, f);
      if (mode === "offline") expect(result).toEqual([f.run]);
      else expect(result).toBeUndefined();
      expectArchived(f);
    });

  it("dry-run selects the expired run without verifiedRemove, archive creation or tree mutation", () => {
    const f = fixture();
    expect(sweep("offline", f, new Set(), true)).toEqual([f.run]);
    expect(verifiedRemovals).toEqual([]);
    expect(fs.existsSync(`${f.runs}.trace-archive`)).toBe(false);
    expect(fs.existsSync(f.run)).toBe(true); expectPayloads(f.run);
    expect(fs.readFileSync(path.join(f.run, "status.json")).equals(f.status)).toBe(true);
  });

  it.each([["offline reference", "offline"], ["streaming live id", "v2"]] as const)(
    "%s keeps the run in place", (_name, mode) => {
      const f = fixture();
      if (mode === "offline") {
        const actors = path.join(f.root, "actors"); json(path.join(actors, "actors.json"), { actors: [{ id: "actor", lastRunId: ID }] });
        expect(sweepResidentRuns(f.runs, f.now, 10_000, { actorRoots: [actors], retainRuns: false, retentionMs: DAY })).toEqual([]);
      } else expect(sweep(mode, f, new Set([ID]))).toBeUndefined();
      expect(fs.existsSync(f.run)).toBe(true); expectPayloads(f.run);
      expect(fs.existsSync(`${f.runs}.trace-archive`)).toBe(false);
    });

  it.each(archiveRaces)("does not attempt archive custody when %s races after measurement", race => {
    const f = fixture();
    const archive = `${f.runs}.trace-archive`;
    const retain = vi.spyOn(cleanupCustody, "retainCleanupRoot");
    let measured = 0, foreignOwnerStats = 0;
    let movedOriginal: string | undefined;
    const removed = sweepResidentRuns(f.runs, f.now, 10_000, {
      retainRuns: false, retentionMs: DAY,
      // This real seam runs only after the candidate's judged stat, age and safe-tree checks.
      measure: run => {
        expect(run).toBe(f.run);
        measured++;
        if (race === "symlink") {
          movedOriginal = path.join(f.root, "original");
          fs.renameSync(f.run, movedOriginal);
          fs.symlinkSync(movedOriginal, f.run, "dir");
        } else if (race === "replaced inode") {
          movedOriginal = path.join(f.root, "original");
          const replacement = path.join(f.root, "replacement");
          fs.cpSync(f.run, replacement, { recursive: true });
          fs.renameSync(f.run, movedOriginal);
          fs.renameSync(replacement, f.run);
        } else if (race === "refreshed mtime") {
          const future = (Date.now() + 60_000) / 1000;
          fs.utimesSync(f.run, future, future);
        } else {
          // ownedStat checks process.getuid(), not geteuid(); report a foreign uid through the real lstat seam.
          const lstat = fs.lstatSync;
          vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
            const stat = Reflect.apply(lstat, fs, [file, ...args]) as fs.Stats;
            if (String(file) !== f.run) return stat;
            foreignOwnerStats++;
            return new Proxy(stat, {
              get(target, property) {
                if (property === "uid") return target.uid + 1;
                const value = Reflect.get(target, property, target) as unknown;
                return typeof value === "function" ? value.bind(target) : value;
              },
            });
          }) as typeof fs.lstatSync);
        }
        return 1;
      },
    });

    expect(measured).toBe(1);
    expect(removed).toEqual([]);
    expect(retain).not.toHaveBeenCalled();
    expect(fs.existsSync(archive)).toBe(false);
    expect(fs.existsSync(f.run)).toBe(true);
    if (race === "symlink") {
      expect(fs.lstatSync(f.run).isSymbolicLink()).toBe(true);
      expect(fs.readlinkSync(f.run)).toBe(movedOriginal);
    } else {
      expect(fs.lstatSync(f.run).isDirectory()).toBe(true);
    }
    expectPreserved(f, movedOriginal ?? f.run);
    if (race === "replaced inode") expectPreserved(f, f.run);
    if (race === "refreshed mtime") expect(fs.statSync(f.run).mtimeMs).toBeGreaterThan(f.now);
    if (race === "foreign owner") expect(foreignOwnerStats).toBeGreaterThan(0);
  });

  it("keeps the active tree intact when archive custody fails", () => {
    const f = fixture();
    const retain = vi.spyOn(cleanupCustody, "retainCleanupRoot").mockImplementation(() => { throw new Error("archive failure"); });
    expect(sweep("main", f)).toBeUndefined();
    expect(retain).toHaveBeenCalledTimes(1);
    expect(fs.existsSync(f.run)).toBe(true); expectPayloads(f.run);
    expect(fs.existsSync(`${f.runs}.trace-archive`)).toBe(false);
    expect(verifiedRemovals.some(target => target === f.run || target.startsWith(`${f.run}${path.sep}`))).toBe(false);
  });
});
