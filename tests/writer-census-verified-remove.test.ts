// Data-safety boundary (smarty-dev#11570). No MeshStore, children, or product deletion:
// the real Linux verifier is forced to dryRun; off-Linux fallback runs through recorded no-op primitives.
// rm/unlink/rmdir are no-ops, including on the unfixed baseline. Cleanup owns only mkdtemp roots.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as store from "../src/mesh/store.js";
import { censusSync } from "../src/mesh/writer-census.js";

type Result = import("../src/storage/verified-remove.js").VerifiedRemoveResult;
type Options = Parameters<typeof import("../src/storage/verified-remove.js").verifiedRemove>[2];
type Call = { target: string; judged: fs.Stats | undefined; options: Options; result: Result };
const probe = vi.hoisted(() => ({ calls: [] as Call[], result: undefined as Result | undefined,
  before: undefined as ((target: string) => void) | undefined }));
vi.mock("../src/storage/verified-remove.js", async importOriginal => {
  const real = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return { ...real, verifiedRemove: (target: string, judged: fs.Stats | undefined, options: Options): Result => {
    probe.before?.(target);
    // Linux runs the REAL identity/type/mtime checks in dryRun; off Linux uses no-op primitives with the helper default-disabled.
    const result = probe.result ?? real.verifiedRemove(target, judged,
      process.platform === "linux" ? { ...options, dryRun: true } : options);
    probe.calls.push({ target, judged, options, result });
    return result;
  } };
});

const DEAD = 4_000_000;
const STARTED = 1_700_000_000_000;
const host = os.hostname();
const originalPlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
const cleanup = fs.rmSync;
const roots: string[] = [];
const primitives: Array<{ operation: string; file: string; options?: fs.RmOptions | undefined }> = [];
let afterDeadLiveness: (() => void) | undefined;
const failure = (code: string): Error => Object.assign(new Error(code), { code });
const body = (fields: Record<string, unknown> = {}): string => JSON.stringify({ format: 1, pid: DEAD, host,
  startedAt: STARTED, releaseSha: "abc123", lockProtocol: 2, stateBackend: "sqlite", ...fields });
const fixture = (temporary = false, text = temporary ? "torn" : body()): { root: string; file: string; name: string } => {
  const tmp = process.env.TMPDIR;
  if (!tmp) throw new Error("writer-census safety fixtures require a private TMPDIR");
  const root = fs.mkdtempSync(path.join(tmp, "writer-census-verified-"));
  roots.push(root);
  const directory = path.join(root, ".writer-census");
  fs.mkdirSync(directory, { mode: 0o700 });
  const name = `${store.censusHostSlug(host)}-${DEAD}-${STARTED}.json${temporary ? ".ab12.tmp" : ""}`;
  const file = path.join(directory, name);
  fs.writeFileSync(file, text, { mode: 0o600 });
  return { root, file, name };
};
const swap = (file: string, text: string): void => {
  fs.writeFileSync(`${file}.swap`, text, { mode: 0o600 });
  fs.renameSync(`${file}.swap`, file);
};
const unknown = (report: ReturnType<typeof censusSync>, name: string): void => {
  expect(report.unknown).toContainEqual(expect.objectContaining({ source: "process-record", name, reason: expect.any(String) }));
};
const refused = (report: ReturnType<typeof censusSync>, file: string, reason: RegExp): void => {
  expect(probe.calls.filter(call => call.target === file)).toEqual([expect.objectContaining({
    result: expect.objectContaining({ removed: false, reason: expect.stringMatching(reason) }) })]);
  expect(report.unknown).toContainEqual(expect.objectContaining({ name: path.basename(file),
    reason: expect.stringMatching(/removal refused:/) }));
  expect(primitives).toEqual([]);
};
/** Observe only the census's held fd, not descriptors subsequently reused by the verifier. */
const trace = (file: string, options: { beforeOpen?: () => void; afterOpen?: () => void; openError?: string; readError?: string; statError?: string } = {}) => {
  const observed = { fd: undefined as number | undefined, stats: [] as fs.Stats[], reads: [] as fs.PathOrFileDescriptor[], closes: 0 };
  const open = fs.openSync, stat = fs.fstatSync, read = fs.readFileSync, close = fs.closeSync;
  let held = false;
  vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
    if (String(target) === file && options.openError) throw failure(options.openError);
    if (String(target) === file) options.beforeOpen?.();
    const fd = (open as (...args: unknown[]) => number)(target, ...rest);
    if (String(target) === file) { observed.fd = fd; held = true; options.afterOpen?.(); }
    return fd;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, ...rest: unknown[]) => {
    if (held && fd === observed.fd && options.statError) throw failure(options.statError);
    const result = (stat as (...args: unknown[]) => fs.Stats)(fd, ...rest);
    if (held && fd === observed.fd) observed.stats.push(result);
    return result;
  }) as typeof fs.fstatSync);
  vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    if ((held && target === observed.fd) || target === file) {
      observed.reads.push(target);
      if (options.readError) throw failure(options.readError);
    }
    return (read as (...args: unknown[]) => unknown)(target, ...rest);
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "closeSync").mockImplementation(fd => {
    if (held && fd === observed.fd) { observed.closes++; held = false; }
    return close(fd);
  });
  return observed;
};
const afterPrunability = (inject: () => void) => {
  const real = store.censusRecordPrunable;
  let done = false;
  return vi.spyOn(store, "censusRecordPrunable").mockImplementation((value, localHost) => {
    const prunable = real(value, localHost);
    if (prunable && !done) { done = true; inject(); }
    return prunable;
  });
};

beforeEach(() => {
  probe.calls.length = 0;
  probe.result = undefined;
  probe.before = undefined;
  primitives.length = 0;
  afterDeadLiveness = undefined;
  // Deterministic ESRCH, with no real signal or pid allocation. Other pids are alive.
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    expect(signal).toBe(0);
    if (pid === DEAD) { const inject = afterDeadLiveness; afterDeadLiveness = undefined; inject?.(); throw failure("ESRCH"); }
    return true;
  });
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { primitives.push({ operation: "rm", file: String(file), options }); });
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { primitives.push({ operation: "unlink", file: String(file) }); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => { primitives.push({ operation: "rmdir", file: String(file) }); });
});
afterEach(() => {
  vi.restoreAllMocks();
  Object.defineProperty(process, "platform", originalPlatform);
  // Only the roots this suite allocated; never a product-owned path or a caller-supplied root.
  for (const root of roots.splice(0)) cleanup(root, { recursive: true, force: true });
});

describe.runIf(process.platform === "linux")("writer census exact judged inode (real verification, dry run)", () => {
  it("refuses a temporary swapped after the dead-pid liveness judgment", () => {
    const { root, file } = fixture(true);
    const original = fs.lstatSync(file);
    afterDeadLiveness = () => swap(file, "live writer's in-flight write");
    const report = censusSync(root);
    refused(report, file, /replaced since judgment/);
    expect(probe.calls[0]!.judged!.ino).toBe(original.ino);
    expect(fs.readFileSync(file, "utf8")).toBe("live writer's in-flight write");
  });

  it("refuses a live record swapped after the complete prunability judgment", () => {
    const { root, file } = fixture();
    const live = body({ pid: process.pid });
    afterPrunability(() => swap(file, live));
    const report = censusSync(root);
    refused(report, file, /replaced since judgment/);
    expect(fs.readFileSync(file, "utf8")).toBe(live);
  });

  it.each([false, true])("refuses a same-inode refresh after judgment (temporary=%s)", temporary => {
    const { root, file } = fixture(temporary);
    const original = fs.lstatSync(file);
    const refresh = (): void => { fs.utimesSync(file, new Date(), new Date(original.mtimeMs + 60_000)); };
    if (temporary) afterDeadLiveness = refresh;
    else afterPrunability(refresh);
    const report = censusSync(root);
    refused(report, file, /modified since judgment/);
    expect(fs.lstatSync(file).ino).toBe(original.ino);
    expect(fs.existsSync(file)).toBe(true);
  });

  it("binds stat AND content to the old held fd when the name is swapped immediately after open", () => {
    const { root, file } = fixture();
    const live = body({ pid: process.pid });
    const observed = trace(file, { afterOpen: () => swap(file, live) });
    const judged = vi.spyOn(store, "censusRecordPrunable");
    const report = censusSync(root);
    refused(report, file, /replaced since judgment/);
    expect(judged).toHaveBeenCalledWith(JSON.parse(body()), host);
    expect(observed.reads).toEqual([observed.fd]); // No path-based content read, before OR after open.
    expect(probe.calls[0]!.judged).toBe(observed.stats[0]);
    expect(probe.calls[0]!.judged!.ino).not.toBe(fs.lstatSync(file).ino);
    expect(observed.closes).toBe(1);
    expect(fs.readFileSync(file, "utf8")).toBe(live);
  });

  it.each([false, true])("hands the exact pre-judgment stat to verification for an unchanged dead file (temporary=%s)", temporary => {
    const { root, file, name } = fixture(temporary);
    const observed = trace(file);
    const lstat = fs.lstatSync;
    const named: fs.Stats[] = [];
    vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const stat = (lstat as (...args: unknown[]) => fs.Stats)(target, ...rest);
      if (String(target) === file) named.push(stat);
      return stat;
    }) as typeof fs.lstatSync);
    const report = censusSync(root);
    expect(probe.calls).toHaveLength(1);
    const call = probe.calls[0]!;
    expect(call.target).toBe(file);
    expect(call.judged).toBe(temporary ? named[0] : observed.stats[0]);
    expect(call.options).toEqual({ deleter: temporary ? "censusTornWrite" : "censusRecordPrune", fallback: expect.any(Function) });
    expect(call.result).toEqual({ removed: true, bytes: Buffer.byteLength(temporary ? "torn" : body()) });
    expect(report.unknown.map(item => item.name)).not.toContain(name);
    expect(fs.existsSync(file)).toBe(true); // A successful DRY RUN is not an actual archive/delete.
    expect(primitives).toEqual([]);
    if (!temporary) { expect(observed.closes).toBe(1); expect(observed.reads).toEqual([observed.fd]); }
  });

  it.each([false, true])("never follows a symlink at a removable name (temporary=%s)", temporary => {
    const { root, file, name } = fixture(temporary);
    const target = path.join(root, "outside-census");
    fs.renameSync(file, target);
    fs.symlinkSync(target, file);
    const observed = trace(file);
    const report = censusSync(root);
    unknown(report, name);
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
    expect(observed.fd).toBeUndefined();
    expect(observed.reads).toEqual([]);
    expect(fs.readFileSync(target, "utf8")).toBe(temporary ? "torn" : body());
  });

  it("refuses a link substituted between the pre-open type check and open", () => {
    const { root, file, name } = fixture();
    const open = fs.openSync;
    let changed = false;
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (!changed && String(target) === file) {
        changed = true;
        fs.renameSync(file, path.join(root, "held"));
        fs.symlinkSync(path.join(root, "held"), file);
      }
      return (open as (...args: unknown[]) => number)(target, ...rest);
    }) as typeof fs.openSync);
    unknown(censusSync(root), name);
    expect(changed).toBe(true);
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
  });
});

describe("writer census conservative evidence and fd lifetime", () => {
  it.each([
    { label: "live local", host, pid: process.pid },
    { label: "foreign", host: "another-host", pid: DEAD },
  ])("retains a $label temporary as unknown without removal", ({ host: writerHost, pid }) => {
    const { root, file } = fixture(true);
    const name = `${store.censusHostSlug(writerHost)}-${pid}-${STARTED}.json.ab12.tmp`;
    const retained = path.join(path.dirname(file), name);
    fs.renameSync(file, retained);
    unknown(censusSync(root), name);
    expect(fs.readFileSync(retained, "utf8")).toBe("torn");
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
  });

  it.each([
    { label: "live local", text: body({ pid: process.pid }) },
    { label: "foreign", text: body({ host: "another-host" }) },
    { label: "unparsable", text: "{" },
    { label: "unsupported format", text: body({ format: 2 }) },
    { label: "invalid pid", text: body({ pid: 0 }) },
    { label: "invalid start", text: body({ startedAt: 0 }) },
    { label: "hostless", text: body({ host: "" }) },
  ])("never submits a $label record for removal, and closes its fd", ({ label, text }) => {
    const { root, file, name } = fixture(false, text);
    const observed = trace(file);
    const report = censusSync(root);
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
    expect(observed.closes).toBe(1);
    expect(fs.readFileSync(file, "utf8")).toBe(text);
    if (label === "live local") expect(report.unknown).toEqual([]);
    else unknown(report, name);
  });

  // Lead decision A: inode conversion must not add a metadata-prunability filter.
  // The documented stricter metadata policy remains a named #11570 follow-up.
  it.each([
    { label: "unknown release", fields: { releaseSha: "unknown" } },
    { label: "unsupported backend", fields: { stateBackend: "redis" } },
    { label: "unsupported protocol", fields: { lockProtocol: 4 } },
    { label: "incoherent backend list", fields: { stateBackend: "file", stateBackends: ["file", "sqlite"] } },
  ])("preserves existing prunability for a dead record with $label", ({ fields }) => {
    const { root, file, name } = fixture(false, body(fields));
    const observed = trace(file);
    probe.result = { removed: false, reason: "recorded control refusal" };
    const report = censusSync(root);
    expect(probe.calls).toHaveLength(1);
    expect(probe.calls[0]).toMatchObject({ target: file, options: { deleter: "censusRecordPrune" } });
    expect(probe.calls[0]!.judged).toBe(observed.stats[0]);
    expect(observed.closes).toBe(1);
    unknown(report, name);
    expect(primitives).toEqual([]);
  });

  it.each(["open", "read", "stat"] as const)("reports EACCES at %s as unknown and closes every acquired fd", stage => {
    const { root, file, name } = fixture();
    const observed = trace(file, stage === "open" ? { openError: "EACCES" }
      : stage === "read" ? { readError: "EACCES" } : { statError: "EACCES" });
    const report = censusSync(root);
    expect(report.unknown).toContainEqual(expect.objectContaining({ name, reason: `${file}: EACCES` }));
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
    expect(fs.existsSync(file)).toBe(true);
    expect(observed.closes).toBe(stage === "open" ? 0 : 1);
    if (stage === "open") expect(observed.fd).toBeUndefined();
    else expect(observed.fd).toEqual(expect.any(Number));
  });

  it.runIf(process.platform === "linux")("closes the fd and refuses a directory substituted just before open, without reading it", () => {
    const { root, file, name } = fixture();
    const observed = trace(file, { beforeOpen: () => {
      fs.renameSync(file, path.join(root, "held"));
      fs.mkdirSync(file, { mode: 0o700 });
      fs.writeFileSync(path.join(file, "held.json"), body(), { mode: 0o600 });
    } });
    unknown(censusSync(root), name);
    expect(observed.closes).toBe(1);
    expect(observed.reads).toEqual([]);
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
    expect(fs.readFileSync(path.join(file, "held.json"), "utf8")).toBe(body());
  });

  it("does not turn an ENOENT from a held-fd read into omitted evidence", () => {
    const { root, file, name } = fixture();
    const observed = trace(file, { readError: "ENOENT" });
    expect(censusSync(root).unknown).toContainEqual(expect.objectContaining({ name, reason: `${file}: ENOENT` }));
    expect(observed.closes).toBe(1);
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
  });

  it.each([false, true])("does not judge or recurse into directories (temporary=%s)", temporary => {
    const { root, file, name } = fixture(temporary);
    fs.renameSync(file, path.join(root, "held"));
    fs.mkdirSync(file, { mode: 0o700 });
    const child = path.join(file, "held.json");
    fs.writeFileSync(child, body(), { mode: 0o600 });
    const graveyard = path.join(path.dirname(file), ".retention-graveyard-123");
    fs.mkdirSync(graveyard, { mode: 0o700 });
    fs.writeFileSync(path.join(graveyard, "writer.json"), body(), { mode: 0o600 });
    const observed = trace(file);
    const report = censusSync(root);
    unknown(report, name);
    unknown(report, path.basename(graveyard));
    expect(observed.fd).toBeUndefined();
    expect(observed.reads).toEqual([]);
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
    expect(fs.readFileSync(child, "utf8")).toBe(body());
    expect(fs.readFileSync(path.join(graveyard, "writer.json"), "utf8")).toBe(body());
  });

  it.each([false, true])("reports a pre-open lstat EACCES without removal (temporary=%s)", temporary => {
    const { root, file, name } = fixture(temporary);
    const lstat = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === file) throw failure("EACCES");
      return (lstat as (...args: unknown[]) => unknown)(target, ...rest);
    }) as typeof fs.lstatSync);
    expect(censusSync(root).unknown).toContainEqual(expect.objectContaining({ name, reason: `${file}: EACCES` }));
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
  });

  it.each([false, true])("omits a target truly absent since listing (temporary=%s)", temporary => {
    const { root, file, name } = fixture(temporary);
    const readdir = fs.readdirSync;
    let moved = false;
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      const names = (readdir as (...args: unknown[]) => unknown)(target, ...rest);
      if (!moved && String(target) === path.dirname(file)) { moved = true; fs.renameSync(file, path.join(root, "held")); }
      return names;
    }) as typeof fs.readdirSync);
    expect(censusSync(root).unknown.map(item => item.name)).not.toContain(name);
    expect(moved).toBe(true);
    expect(probe.calls).toEqual([]);
    expect(primitives).toEqual([]);
  });

  for (const temporary of [false, true]) {
    it.each([
      { label: "pre-move absence", reason: "open failed (ENOENT)", moved: true, grave: false, visible: false },
      { label: "false absence with occupied name", reason: "open failed (ENOENT)", moved: false, grave: false, visible: true },
      { label: "graveyard-held ENOENT", reason: "verify failed (ENOENT)", moved: true, grave: true, visible: true },
      { label: "unresolved held graveyard", reason: "verify failed (ENOENT); graveyard location unresolved", moved: true, grave: false, visible: true },
      { label: "ordinary refusal", reason: "target was modified since judgment", moved: false, grave: false, visible: true },
    ])(`distinguishes $label from omission (temporary=${temporary})`, ({ reason, moved, grave, visible }) => {
      const { root, file, name } = fixture(temporary);
      const held = path.join(root, "held");
      probe.before = target => { if (moved) fs.renameSync(target, held); };
      probe.result = { removed: false, reason, ...(grave ? { graveyard: held } : {}) };
      const report = censusSync(root);
      expect(probe.calls).toHaveLength(1);
      if (visible) unknown(report, name);
      else expect(report.unknown.map(item => item.name)).not.toContain(name);
      if (grave) expect(report.unknown[0]!.reason).toContain(`(at ${held})`);
      expect(fs.existsSync(moved ? held : file)).toBe(true);
      expect(primitives).toEqual([]);
    });
  }

  it.each([
    { platform: "darwin", temporary: false },
    { platform: "darwin", temporary: true },
    { platform: "win32", temporary: false },
    { platform: "win32", temporary: true },
  ])("removes a dead file via the real off-Linux fallback (platform=$platform, temporary=$temporary)", ({ platform, temporary }) => {
    const { root, file, name } = fixture(temporary);
    Object.defineProperty(process, "platform", { ...originalPlatform, value: platform });
    const report = censusSync(root);
    expect(probe.calls).toHaveLength(1);
    const call = probe.calls[0]!;
    expect(call.target).toBe(file);
    expect(call.result).toEqual({ removed: true, bytes: 0 });
    expect(call.options).toEqual({ deleter: temporary ? "censusTornWrite" : "censusRecordPrune", fallback: expect.any(Function) });
    expect(report.unknown.map(item => item.name)).not.toContain(name);
    expect(primitives).toEqual([{ operation: "rm", file, options: { force: true } }]);
    expect(fs.existsSync(file)).toBe(true); // The recorded rmSync probe does not actually delete the file.
  });
});
