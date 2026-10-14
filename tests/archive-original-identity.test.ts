import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import { privateDirTestHooks } from "../src/storage/private-dir.js";
import { createHostTaskRun, taskControlDirectory, TASK_CONTROL_LAYOUT } from "../src/worker/terminal-answer.js";

// Keep reason: G83 data safety / trust boundary. Real allocation, source validation
// and archive publication; only destructive boundaries and central renames are
// recorded. Native Windows source directory opens remain #12444's responsibility.
const probe = vi.hoisted(() => ({
  opened: [] as { directory: string; fd: number | undefined; close: () => void }[],
  removals: [] as { operation: string; target: string }[],
  renames: [] as { from: string; to: string }[],
}));
vi.mock("../src/storage/private-dir.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/storage/private-dir.js")>();
  return { ...actual, openPrivateDir: (directory: string, anchor?: string) => {
    const held = actual.openPrivateDir(directory, anchor);
    const close = vi.fn(() => held.close());
    probe.opened.push({ directory: path.resolve(directory), fd: held.fd, close });
    return { ...held, close };
  } };
});

// Deliberately no fixture cleanup: every rm/unlink/rmdir form records only.
// Restore mocks only after all assertions; no native control remover is invoked.
beforeEach(() => {
  probe.opened.length = probe.removals.length = probe.renames.length = 0;
  const record = (operation: string, target: unknown) => {
    probe.removals.push({ operation, target: String(target) });
  };
  for (const name of ["rmSync", "unlinkSync", "rmdirSync"] as const) {
    vi.spyOn(fs, name).mockImplementation(((target: unknown) => record(name, target)) as never);
  }
  for (const name of ["rm", "unlink", "rmdir"] as const) {
    vi.spyOn(fs, name).mockImplementation(((...args: unknown[]) => {
      record(name, args[0]);
      const callback = args.at(-1);
      if (typeof callback === "function") callback(null);
    }) as never);
    vi.spyOn(fs.promises, name).mockImplementation((async (target: unknown) => record(`promises.${name}`, target)) as never);
  }
  const nativeRename = fs.renameSync;
  const state = fs.realpathSync(process.env.XDG_STATE_HOME!);
  const temp = fs.realpathSync(os.tmpdir());
  expect(state === temp || state.startsWith(temp + path.sep)).toBe(false);
  const copy = (from: string, to: string): void => {
    const stat = fs.lstatSync(from);
    expect(stat.isSymbolicLink()).toBe(false);
    if (stat.isDirectory()) {
      fs.mkdirSync(to, { mode: 0o700 });
      fs.chmodSync(to, stat.mode & 0o777);
      for (const name of fs.readdirSync(from)) copy(path.join(from, name), path.join(to, name));
    } else {
      expect(stat.isFile()).toBe(true);
      fs.writeFileSync(to, fs.readFileSync(from), { flag: "wx", mode: 0o600 });
    }
  };
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    const destination = path.join(fs.realpathSync(path.dirname(String(to))), path.basename(String(to)));
    if (destination === state || destination.startsWith(state + path.sep)) {
      // Held /proc/self/fd paths expire when the product closes its handle.
      // Record physical source names now so retained-byte assertions stay real.
      probe.renames.push({ from: path.join(fs.realpathSync(path.dirname(String(from))), path.basename(String(from))), to: destination });
      copy(String(from), String(to)); // Retain the source stage; never central rename.
      return;
    }
    // Only the explicit adversarial archive swap may use a native rename.
    expect(destination.startsWith(temp + path.sep)).toBe(true);
    expect(String(to)).toBe(`${String(from)}-held`);
    nativeRename(from, to);
  });
  vi.spyOn(fs.promises, "rename").mockImplementation(async (from, to) => { fs.renameSync(from, to); });
  vi.spyOn(fs, "rename").mockImplementation((from, to, callback) => {
    try { fs.renameSync(from, to); callback(null); }
    catch (error) { callback(error as NodeJS.ErrnoException); }
  });
});
afterEach(() => {
  delete privateDirTestHooks.platform;
  vi.restoreAllMocks();
});

const seed = (run: string) => {
  createHostTaskRun(run);
  // Seed a valid host receipt directly: a copy-only saveFinalAnswerReceipt
  // would intentionally leave its renamed .tmp beside the source answer. The
  // archive correctly refuses that unknown artifact; never hide it in scans.
  // The actual receipt writer is covered by task-allocation-commit.test.ts.
  const answer = { version: 1 as const, id: `${path.basename(run)}-answer`, runId: path.basename(run),
    recordedAt: Date.now(), text: "original protected answer" };
  fs.writeFileSync(path.join(taskControlDirectory(run), "final-answer.json"), JSON.stringify(answer), { mode: 0o600, flag: "wx" });
  const status = JSON.stringify({ id: path.basename(run), status: "completed", taskControlLayout: TASK_CONTROL_LAYOUT,
    finalAnswerReceipt: { id: answer.id, recordedAt: answer.recordedAt } });
  fs.writeFileSync(path.join(run, "status.json"), status, { mode: 0o600 });
  return { answer, status };
};
const fixture = (nested = false) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "archive-identity-"));
  // Distinct runtime workspace, never the tracked checkout or its state root.
  const workspace = path.join(root, "workspace");
  fs.mkdirSync(workspace, { mode: 0o700 });
  const source = path.join(workspace, "source"), original = seed(source);
  const child = path.join(source, "nested", "child");
  const childOriginal = nested ? seed(child) : undefined;
  const actor = { sessionFile: path.join(workspace, "actor", "session.jsonl") };
  const archive = path.join(workspace, "actor", "runs", "archive");
  const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 86400000 });
  const prune = vi.spyOn(store, "pruneRuns").mockImplementation(() => {});
  return { source, original, child, childOriginal, actor, archive, store, prune };
};
const expectStagesRetained = () => {
  expect(probe.renames.length).toBeGreaterThan(0);
  for (const rename of probe.renames) expect(fs.existsSync(rename.from)).toBe(true);
};

// The source reader still POSIX-opens baseA. Simulate only private-dir's Windows
// fdless branch, not process.platform; native Windows coverage is intentionally pending.
describe.skipIf(process.platform === "win32")("archive original identity (G83)", () => {
  it.each([false, true])("allows unchanged fdless archives, nested=%s", async nested => {
    const f = fixture(nested);
    privateDirTestHooks.platform = "win32";
    await expect(f.store.retainRun(f.actor, "archive", f.source)).resolves.toBeUndefined();
    const held = probe.opened.find(open => open.directory === f.archive)!;
    expect(held.fd).toBeUndefined();
    expect(held.close).toHaveBeenCalledOnce();
    expect(fs.readFileSync(path.join(f.archive, "status.json"), "utf8")).toBe(f.original.status);
    expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(f.archive), "final-answer.json"), "utf8"))).toEqual(f.original.answer);
    if (nested) {
      const childArchive = path.join(f.archive, "nested", "child");
      expect(probe.opened.find(open => open.directory === childArchive)!.fd).toBeUndefined();
      expect(JSON.parse(fs.readFileSync(path.join(taskControlDirectory(childArchive), "final-answer.json"), "utf8"))).toEqual(f.childOriginal!.answer);
    }
    expect(f.prune).toHaveBeenCalledOnce();
    expectStagesRetained();
  });

  it.each([
    ["win32", false], ["win32", true], ["linux", false], ["linux", true],
  ] as const)("refuses %s archive replacement at central .stage mkdir, nested=%s", async (platform, nested) => {
    const f = fixture(nested);
    privateDirTestHooks.platform = platform;
    const target = nested ? path.join(f.archive, "nested", "child") : f.archive;
    const stage = path.basename(taskControlDirectory(target)) + ".stage";
    const mkdir = fs.mkdirSync;
    let swapped = false;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((...args: Parameters<typeof mkdir>) => {
      const result = mkdir(...args);
      if (!swapped && path.basename(String(args[0])) === stage) {
        swapped = true;
        fs.renameSync(target, target + "-held");
        mkdir(target, { mode: 0o700 });
      }
      return result;
    }) as typeof mkdir);
    await expect(f.store.retainRun(f.actor, "archive", f.source)).rejects.toThrow("changed while it was held");
    expect(swapped).toBe(true);
    const held = probe.opened.find(open => open.directory === target)!;
    expect(held.fd === undefined).toBe(platform === "win32");
    expect(held.close).toHaveBeenCalledOnce();
    expect(fs.readdirSync(target)).toEqual([]);
    // Nested status was copied before the child was opened; never rewrite it.
    if (!nested) expect(fs.existsSync(path.join(target + "-held", "status.json"))).toBe(false);
    else expect(fs.readFileSync(path.join(target + "-held", "status.json"), "utf8")).toBe(f.childOriginal!.status);
    expect(fs.existsSync(path.join(taskControlDirectory(target), "final-answer.json"))).toBe(false);
    expect(fs.readFileSync(path.join(f.source, "status.json"), "utf8")).toBe(f.original.status);
    expect(f.prune).not.toHaveBeenCalled();
    expectStagesRetained();
  });

  it.each(["win32", "linux"] as const)("closes the %s private directory when original identity capture fails", async platform => {
    const f = fixture();
    privateDirTestHooks.platform = platform;
    const lstat = fs.lstatSync, fstat = fs.fstatSync;
    let injected = false;
    vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof lstat>) => {
      if (!injected && platform === "win32" && String(args[0]) === f.archive &&
          probe.opened.some(open => open.directory === f.archive)) {
        injected = true;
        throw new Error("capture unavailable");
      }
      return lstat(...args);
    }) as typeof lstat);
    vi.spyOn(fs, "fstatSync").mockImplementation(((...args: Parameters<typeof fstat>) => {
      if (!injected && platform === "linux" &&
          probe.opened.some(open => open.directory === f.archive && open.fd === args[0])) {
        injected = true;
        throw new Error("capture unavailable");
      }
      return fstat(...args);
    }) as typeof fstat);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await f.store.retainRun(f.actor, "archive", f.source);
    expect(injected).toBe(true);
    const held = probe.opened.find(open => open.directory === f.archive)!;
    expect(held.close).toHaveBeenCalledOnce();
    if (held.fd !== undefined) expect(() => fstat(held.fd!)).toThrow();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("capture unavailable"));
    expect(f.prune).not.toHaveBeenCalled();
    expect(fs.existsSync(taskControlDirectory(f.archive))).toBe(false);
    expect(fs.readFileSync(path.join(f.source, "status.json"), "utf8")).toBe(f.original.status);
  });
});
