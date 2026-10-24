// Decision C: MOCKED unit evidence, NOT Windows/NTFS ACL proof or a timing benchmark.
// Keep reason: trust boundary/data safety. Exercise the existing public policy with its DEFAULT native runner;
// execFileSync's output and Windows-path lstats are fake; gate regressions also bound path/realpath/create seams.
// No native helpers, actual Windows filesystem fixtures or new production APIs.
// Contract: prove once per process per absolute root + root/DB BigInt dev/ino identity, never ctime/ChangeTime.
// Same-identity ACL edits are deliberately NOT re-proved: first proof denies foreign WRITE_DAC/WRITE_OWNER;
// trusted user/Admin/accepted service-owner re-ACL remains a residual. A new process or replaced root/DB re-proves.
// This mocked contract is not a Windows safety claim. Unknown/unreadable identity requires full proof without cache;
// positively observed missing/wrong-type/link DURING a proof after its known before-identity must refuse.
// Prior cache knowledge alone must not turn a DB already absent before this fresh proof into a permanent refusal.
import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WindowsStateAclRequest } from "../src/mesh/windows-state-acl.js";
import type { SqliteOpener } from "../src/mesh/state-sqlite.js";

const USER = "S-1-5-21-1-2-3-1001";
const OTHER = "S-1-5-21-1-2-3-1002";
const SYSTEM = "S-1-5-18";
const ADMINS = "S-1-5-32-544";
const FULL = 0x1f01ff;
const METADATA = 0x00120080;
const ROOT = "C:\\Users\\me\\.pi\\mesh";
const DB = `${ROOT}\\state.db`;
const FLAGS = `${ROOT}\\state-checkpoint.flags`;
const LEXICAL = ["C:\\Users\\me\\.pi", "C:\\Users\\me", "C:\\Users", "C:\\"];
const REAL = ["C:\\state\\private", "C:\\state"];
const FILES = [DB, `${DB}-wal`, `${DB}-shm`, `${DB}-journal`];
// Explicit expected wire plan, not derived from whatever paths the implementation happens to request.
const PLAN = [ROOT, ...LEXICAL, ...REAL, FLAGS, ...FILES];
const key = (file: string) => file.toLowerCase();
const request = (): WindowsStateAclRequest => ({
  root: ROOT, real: "C:\\state\\private\\mesh", files: [...FILES], directories: [FLAGS],
});
type Ace = { type: number; flags: number; sid: string; mask: number };
type Entry = { path: string } & Record<string, unknown>;
type Snapshot = { userSid: string; ownerSid: string; protect: string | null; entries: Entry[] };
const allow = (sid: string, mask: number, flags = 0): Ace => ({ type: 0, flags, sid, mask });
const trustedDacl = (): Ace[] => [allow(USER, FULL, 3), allow(SYSTEM, FULL, 3), allow(ADMINS, FULL, 3)];

// Timestamps are decoys: neither nanosecond nor millisecond ctime may participate in cache admission.
const fakeStat = (ino: bigint, directory: boolean): fs.BigIntStats => ({
  dev: 7n, ino, ctimeNs: 1_700_000_000_000_000_000n, ctimeMs: 1_700_000_000_000n,
  isSymbolicLink: () => false, isDirectory: () => directory, isFile: () => !directory,
} as fs.BigIntStats);
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
// Default Windows ACL findings are advisory. Old gate refusal/race rows opt in explicitly per test (never globally);
// afterEach unstubs it. The value is read by the gate at each call, so the same module instance can switch.
const ENFORCE = "PI_FABRIC_MESH_STATE_ACL_ENFORCE";
const ADVISORY = "FABRIC_MESH_STATE_ACL_ADVISORY";
const enforceAcl = () => vi.stubEnv(ENFORCE, "1");

beforeEach(() => vi.resetModules());
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  Object.defineProperty(process, "platform", platform);
  vi.resetModules();
});

const fixture = async (owner = USER, withGate = false) => {
  // Load before installing the filesystem seam, so the module loader never consumes fake Windows stats.
  const { windowsStateAclRefusal } = await import("../src/mesh/windows-state-acl.js");
  const gate = withGate ? await import("../src/mesh/state-gate.js") : undefined;
  if (withGate) {
    // Decision C: process.platform does not change Linux Node's default path dialect. Only mocked drive paths
    // use win32 operations; loader/host POSIX paths retain their real implementation. No production API seam.
    const host = { resolve: path.resolve, dirname: path.dirname, basename: path.basename, join: path.join };
    const windows = { resolve: path.win32.resolve, dirname: path.win32.dirname,
      basename: path.win32.basename, join: path.win32.join };
    const drivePath = (file: string | undefined) => /^[a-z]:[\\/]/i.test(file ?? "");
    vi.spyOn(path, "resolve").mockImplementation((...parts) =>
      drivePath(parts[0]) ? windows.resolve(...parts) : host.resolve(...parts));
    vi.spyOn(path, "dirname").mockImplementation(file =>
      drivePath(file) ? windows.dirname(file) : host.dirname(file));
    vi.spyOn(path, "basename").mockImplementation((file, suffix) =>
      drivePath(file) ? windows.basename(file, suffix) : host.basename(file, suffix));
    vi.spyOn(path, "join").mockImplementation((...parts) =>
      drivePath(parts[0]) ? windows.join(...parts) : host.join(...parts));
    const realpath = fs.realpathSync.bind(fs);
    const nativeRealpath = fs.realpathSync.native.bind(fs.realpathSync);
    vi.spyOn(fs, "realpathSync").mockImplementation(((file: fs.PathLike, options?: unknown) =>
      /^[a-z]:[\\/]/i.test(String(file)) ? String(file) : realpath(file, options as never)) as typeof fs.realpathSync);
    // The public gate resolves the root's parent; use the same explicit real-ancestor plan as request().
    Object.assign(fs.realpathSync, { native: (file: fs.PathLike, options?: unknown) =>
      /^[a-z]:[\\/]/i.test(String(file))
        ? String(file) === LEXICAL[0] ? "C:\\state\\private" : String(file)
        : nativeRealpath(file, options as never) });
  }
  Object.defineProperty(process, "platform", { ...platform, value: "win32" });
  vi.stubEnv("SystemRoot", "C:\\Windows");
  // The reader's own UNVERIFIED notice still uses emitWarning; the gate's advisory uses console/stderr (Pi's setupCli
  // silences emitWarning). Advisory lines are captured and swallowed; everything else calls through.
  const warning = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
  const advisoryLines: string[] = [];
  let viaConsole = false;
  const write = process.stderr.write.bind(process.stderr) as (...args: unknown[]) => boolean;
  vi.spyOn(process.stderr, "write").mockImplementation(((chunk: unknown, ...rest: unknown[]) => {
    if (String(chunk).includes(ADVISORY)) { if (!viaConsole) advisoryLines.push(String(chunk)); return true; }
    return write(chunk, ...rest);
  }) as typeof process.stderr.write);
  for (const level of ["warn", "error"] as const) {
    const original = console[level].bind(console);
    vi.spyOn(console, level).mockImplementation((...args: unknown[]) => {
      const text = args.map(String).join(" ");
      if (text.includes(ADVISORY)) { advisoryLines.push(text); return; }
      viaConsole = true;
      try { original(...args); } finally { viaConsole = false; }
    });
  }
  const stats = new Map<string, fs.BigIntStats>();
  const entries = new Map<string, Entry>();
  const unreadable = new Set<string>();
  const rawUnreadable = new Set<string>(); // opt-in: ordinary gate lstats still work
  let nextIno = 9_007_199_254_740_992n; // +1 must not disappear through Number coercion of a 64-bit file ID
  const present = (file: string) => {
    const directory = !FILES.includes(file);
    stats.set(key(file), fakeStat(nextIno++, directory));
    entries.set(key(file), { path: file, attributes: directory ? 0x10 : 0x20,
      owner, control: 0x9004, dacl: trustedDacl() });
  };
  PLAN.forEach(present);
  const absent = (file: string) => { stats.delete(key(file)); entries.delete(key(file)); };
  const stat = (file: string) => stats.get(key(file))!;
  const entry = (file: string) => entries.get(key(file))!;
  const unsafe = (file: string, ace = allow(OTHER, FULL)) => {
    entry(file).dacl = [...trustedDacl(), allow("S-1-3-0", FULL, 0xb), ace];
  };
  const state = {
    plan: [...PLAN],
    failure: undefined as Error | undefined,
    output: (snapshot: Snapshot): string => JSON.stringify(snapshot),
    observed: (_file: string, _options: fs.StatSyncOptions | undefined, _stat: fs.BigIntStats | undefined): void => undefined,
    beforeLstat: (_file: string, _options: fs.StatSyncOptions | undefined): void => undefined,
  };
  const actualLstat = fs.lstatSync.bind(fs);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: fs.StatSyncOptions) => {
    const name = String(file);
    if (!/^[a-z]:[\\/]/i.test(name)) return actualLstat(file, options);
    if (!PLAN.some(target => key(target) === key(name))) throw new Error(`unexpected mocked Windows path: ${name}`);
    if (unreadable.has(key(name)) || (options?.bigint === true && rawUnreadable.has(key(name)))) {
      throw Object.assign(new Error("mocked lstat denied"), { code: "EACCES" });
    }
    state.beforeLstat(name, options);
    const found = stats.get(key(name));
    const snapshot = found ? { ...found } : undefined; // independent, as with a native lstat
    state.observed(name, options, snapshot);
    if (snapshot) return snapshot;
    if (options?.throwIfNoEntry === false) return undefined;
    throw Object.assign(new Error("mocked missing path"), { code: "ENOENT" });
  }) as typeof fs.lstatSync);
  const native = vi.spyOn(childProcess, "execFileSync").mockImplementation((...args: unknown[]) => {
    const options = args[2] as { env?: NodeJS.ProcessEnv };
    const wire = JSON.parse(options.env?.PI_FABRIC_STATE_ACL ?? "null") as { paths: string[]; protect: string | null };
    expect(wire.paths).toEqual(state.plan);
    if (state.failure) throw state.failure;
    return state.output({ userSid: USER, ownerSid: owner, protect: wire.protect ? "ok" : null,
      entries: wire.paths.map(file => entries.get(key(file)) ?? { path: file, missing: true }) });
  });
  // Never pass the optional runner argument: that would miss a default-runner-only cache.
  const judge = (input = request()) => windowsStateAclRefusal(input);
  const warm = (input = request()) => {
    expect(judge(input)).toBeUndefined();
    expect(judge(input)).toBeUndefined(); // synchronous: no await/microtask/timer between requests
    expect(native).toHaveBeenCalledTimes(1);
  };
  const advisories = () => [...advisoryLines];
  return { judge, warm, gate, native, warning, advisories, stat, entry, unsafe, present, absent, unreadable, rawUnreadable, state };
};

describe("Windows mesh ACL identity cache — Decision C MOCKED unit contract", () => {
  it.each([USER, ADMINS])("CACHE-01 unchanged successful gate uses one child across 3 calls (owner %s)", async owner => {
    const f = await fixture(owner);
    f.warm();
    expect(f.judge()).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(1); // base RED: currently 2 at warm(), 3 without that assertion
    expect(f.warning).not.toHaveBeenCalled();
  });

  it("CACHE-02 approved foreign metadata and safe CREATOR OWNER IO remain cacheable", async () => {
    const f = await fixture();
    for (const file of [ROOT, FLAGS, ...FILES]) {
      f.entry(file).dacl = [...trustedDacl(), allow(OTHER, METADATA), allow("S-1-3-0", FULL, 0xb)];
    }
    f.warm();
    expect(f.judge()).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(1);
  });

  it.each([ROOT, ...FILES, FLAGS, ...LEXICAL, ...REAL])(
    "CACHE-03 ctime changes on %s are ignored, including a simulated trusted-user ACE edit", async target => {
      const f = await fixture();
      f.warm();
      f.stat(target).ctimeNs += 1n;
      f.unsafe(target); // modeled trusted-user re-ACL, NOT evidence that a foreign SID could perform this edit
      expect(f.judge()).toBeUndefined();
      f.stat(target).ctimeMs += 1n;
      expect(f.judge()).toBeUndefined();
      Reflect.deleteProperty(f.stat(target), "ctimeNs");
      Reflect.deleteProperty(f.stat(target), "ctimeMs");
      expect(f.judge()).toBeUndefined();
      expect(f.native).toHaveBeenCalledTimes(1);
    });

  it.each([
    [ROOT, "dev"], [ROOT, "ino"], [DB, "dev"], [DB, "ino"],
  ] as const)("CACHE-04 %s %s replacement invalidates with unchanged timestamps", async (target, field) => {
    const f = await fixture();
    f.warm();
    f.stat(target)[field] += 1n;
    f.unsafe(target);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(2);
  });

  it("CACHE-05 an absent DB cannot supply identity; its appearance requires full proof", async () => {
    const f = await fixture();
    f.absent(DB);
    expect(f.judge()).toBeUndefined();
    expect(f.judge()).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(2);
    f.present(DB); // root identity and every timestamp stay unchanged
    f.unsafe(DB);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(3);
  });

  it("CACHE-06 an expanded target plan cannot reuse a narrower successful proof", async () => {
    const f = await fixture();
    const narrow = { ...request(), files: [DB] };
    f.state.plan = PLAN.filter(file => !FILES.includes(file) || file === DB);
    f.warm(narrow);
    f.unsafe(`${DB}-wal`); // root/DB identity is unchanged; this descriptor was not in the narrower proof
    f.state.plan = [...PLAN];
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(2);
  });

  it.each(["creation-parent", "root"] as const)(
    "CACHE-07 %s cannot reuse an ordinary-ancestor proof for identical paths", async role => {
      const f = await fixture();
      const ordinary: WindowsStateAclRequest = { ...request(), role: "ancestor", files: [], directories: [] };
      f.state.plan = [ROOT, ...LEXICAL, ...REAL];
      f.unsafe(ROOT, allow(OTHER, FULL, 0xb)); // ordinary ancestors skip IO; root/creation-parent must refuse it
      // Test role separation, without demanding either reuse or always-fresh reads for ordinary ancestors.
      expect(f.judge(ordinary)).toBeUndefined();
      expect(f.native).toHaveBeenCalledTimes(1);
      expect(f.judge({ ...ordinary, role })).toContain(`grants ${OTHER}`);
      expect(f.native).toHaveBeenCalledTimes(2);
    });

  it("CACHE-07 successful creation-parent checks are always fresh", async () => {
    const f = await fixture();
    const input: WindowsStateAclRequest = { ...request(), role: "creation-parent", files: [], directories: [] };
    f.state.plan = [ROOT, ...LEXICAL, ...REAL];
    expect(f.judge(input)).toBeUndefined();
    expect(f.judge(input)).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(2);
    f.unsafe(ROOT, allow(OTHER, FULL, 0xb));
    expect(f.judge(input)).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(3);
  });

  it("CACHE-08 successful protect/readback is always fresh", async () => {
    const f = await fixture();
    const input = { ...request(), protect: true };
    expect(f.judge(input)).toBeUndefined();
    expect(f.judge(input)).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(2);
    f.unsafe(ROOT);
    expect(f.judge(input)).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(3);
  });

  it("CACHE-08 protect cannot reuse an existing-root proof, including elevated Administrators ownership", async () => {
    const f = await fixture(ADMINS);
    f.warm();
    expect(f.judge({ ...request(), protect: true })).toMatch(/owned by S-1-5-32-544, not the current user/);
    expect(f.native).toHaveBeenCalledTimes(2);
    const options = f.native.mock.calls[1]![2] as { env: NodeJS.ProcessEnv };
    expect(JSON.parse(options.env.PI_FABRIC_STATE_ACL!).protect).toBe(ROOT);
  });

  it.each(["foreign ACE", "unknown ACE", "malformed", "incomplete", "unreadable descriptor"] as const)(
    "CACHE-09 %s evidence is never cached, so a repaired snapshot is read next time", async kind => {
      const f = await fixture();
      f.state.output = snapshot => {
        if (kind === "malformed") return "not JSON";
        if (kind === "incomplete") return JSON.stringify({ ...snapshot, entries: snapshot.entries.slice(1) });
        const root = snapshot.entries[0]!;
        if (kind === "unreadable descriptor") root.error = "mocked Access is denied";
        else root.dacl = [...trustedDacl(), allow("S-1-3-0", FULL, 0xb),
          { ...allow(OTHER, FULL), type: kind === "unknown ACE" ? 9 : 0 }];
        return JSON.stringify(snapshot);
      };
      const refusal = f.judge();
      if (kind === "foreign ACE") expect(refusal).toContain(`grants ${OTHER}`);
      else if (kind === "unknown ACE") expect(refusal).toMatch(/unsupported or unproven ACE/);
      else expect(refusal).toMatch(/(?:unreadable|invalid) Windows/);
      // The output builder edited the fixture entry for descriptor cases; restore it without changing identity.
      delete f.entry(ROOT).error;
      f.entry(ROOT).dacl = trustedDacl();
      f.state.output = snapshot => JSON.stringify(snapshot);
      expect(f.judge()).toBeUndefined();
      expect(f.native).toHaveBeenCalledTimes(2);
      expect(f.judge()).toBeUndefined();
      expect(f.native).toHaveBeenCalledTimes(2); // only this second, fully verified success may be retained
    });

  it.each(["ENOENT", "ETIMEDOUT", "nonzero"])("CACHE-10 warned fail-open %s never becomes a cached proof", async kind => {
    const f = await fixture();
    f.state.failure = Object.assign(new Error(`mocked native ${kind}`), kind === "nonzero" ? { status: 1 } : { code: kind });
    expect(f.judge()).toBeUndefined();
    expect(f.judge()).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(2);
    expect(f.warning).toHaveBeenCalledTimes(1);
    f.state.failure = undefined;
    f.unsafe(ROOT);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(3);
  });

  it.each(["ENOBUFS", "ERR_CHILD_PROCESS_STDIO_MAXBUFFER"])("CACHE-11 overflow %s never caches incomplete proof", async code => {
    const f = await fixture();
    f.state.failure = Object.assign(new Error("mocked overflow"), { code });
    expect(f.judge()).toMatch(/incomplete Windows ACL snapshot/);
    expect(f.judge()).toMatch(/incomplete Windows ACL snapshot/);
    expect(f.native).toHaveBeenCalledTimes(2);
    expect(f.warning).not.toHaveBeenCalled();
    f.state.failure = undefined;
    f.unsafe(ROOT);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(3);
  });

  it.each([ROOT, DB])("CACHE-12 missing ctime on %s remains cacheable from the first proof", async target => {
    const f = await fixture();
    Reflect.deleteProperty(f.stat(target), "ctimeNs");
    Reflect.deleteProperty(f.stat(target), "ctimeMs");
    f.warm();
  });

  it.each([
    [ROOT, "dev", "missing"], [ROOT, "ino", "missing"], [DB, "dev", "missing"], [DB, "ino", "missing"],
    [ROOT, "dev", "number"], [ROOT, "ino", "number"], [DB, "dev", "number"], [DB, "ino", "number"],
  ] as const)("CACHE-12 %s %s identity is %s: full native read on every call", async (target, field, kind) => {
    const f = await fixture();
    f.warm();
    if (kind === "missing") Reflect.deleteProperty(f.stat(target), field);
    else Reflect.set(f.stat(target), field, Number(f.stat(target)[field])); // not a proven BigInt file identity
    expect(f.judge()).toBeUndefined();
    expect(f.judge()).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(3);
    f.unsafe(target);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(4);
  });

  it.each([ROOT, DB])("CACHE-13 unreadable %s identity disables hits, not the full native read", async target => {
    const f = await fixture();
    f.warm();
    f.unreadable.add(key(target));
    expect(f.judge()).toBeUndefined();
    expect(f.judge()).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(3);
    f.unsafe(target);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(4);
  });

  it.each([ROOT, FLAGS, ...FILES, ...LEXICAL, ...REAL])(
    "CACHE-14 module reset models a new process: first proof refuses a foreign ACE on %s", async target => {
      const f = await fixture();
      f.warm();
      f.unsafe(target);
      expect(f.judge()).toBeUndefined(); // deliberate same-process, same-identity residual
      expect(f.native).toHaveBeenCalledTimes(1);
      vi.resetModules(); // module-lifetime model only; no actual process or Windows security claim
      const { windowsStateAclRefusal } = await import("../src/mesh/windows-state-acl.js");
      expect(windowsStateAclRefusal(request())).toContain(`grants ${OTHER}`);
      expect(f.native).toHaveBeenCalledTimes(2);
    });

  it.each([
    [ROOT, "dev"], [ROOT, "ino"], [DB, "dev"], [DB, "ino"],
  ] as const)("CACHE-15 %s %s swapped during the child cannot inherit its safe snapshot", async (target, field) => {
    const f = await fixture();
    f.state.output = snapshot => {
      const oldIdentityProof = JSON.stringify(snapshot);
      f.stat(target)[field] += 1n; // swap after the safe descriptor read, before the native child returns
      f.unsafe(target); // the replacement has never had a successful proof; timestamps are unchanged
      f.state.output = next => JSON.stringify(next);
      return oldIdentityProof;
    };
    expect(f.judge()).toBeDefined(); // do not use a safe descriptor snapshot paired with a different identity
    const reads = f.native.mock.calls.length;
    expect(reads).toBeGreaterThanOrEqual(1);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(reads + 1);
  });

  it.each([ROOT, DB].flatMap(target => ["missing", "wrong type", "link"].map(kind => [target, kind] as const)))(
    "CACHE-16 known %s becomes %s after a complete safe snapshot: refuse, not uncached allow", async (target, kind) => {
      const f = await fixture();
      f.state.output = snapshot => {
        const safe = JSON.stringify(snapshot);
        if (kind === "missing") f.absent(target);
        else if (kind === "link") f.stat(target).isSymbolicLink = () => true;
        else {
          f.stat(target).isDirectory = () => target === DB;
          f.stat(target).isFile = () => target === ROOT;
        }
        return safe;
      };
      expect(f.judge()).toBeDefined();
      expect(f.native).toHaveBeenCalledTimes(1);
      expect(f.warning).not.toHaveBeenCalled();
    });

  it.each([ROOT, DB])("CACHE-17 known %s missing in the native snapshot must refuse even if lstat still sees its ID", async target => {
    const f = await fixture();
    f.state.output = snapshot => JSON.stringify({ ...snapshot,
      entries: snapshot.entries.map(entry => entry.path === target ? { path: target, missing: true } : entry) });
    expect(f.judge()).toBeDefined();
    expect(f.native).toHaveBeenCalledTimes(1);
  });

  it.each([ROOT, DB].flatMap(target => ["unknown ID", "unreadable"].map(kind => [target, kind] as const)))(
    "CACHE-18 known %s becomes %s after proof: allow full proof, never cache or permanently refuse", async (target, kind) => {
      const f = await fixture();
      const originalIno = f.stat(target).ino;
      f.state.output = snapshot => {
        const safe = JSON.stringify(snapshot);
        if (kind === "unknown ID") Reflect.deleteProperty(f.stat(target), "ino");
        else f.unreadable.add(key(target));
        return safe;
      };
      expect(f.judge()).toBeUndefined();
      expect(f.judge()).toBeUndefined();
      expect(f.native).toHaveBeenCalledTimes(2);
      f.unsafe(target);
      expect(f.judge()).toContain(`grants ${OTHER}`);
      expect(f.native).toHaveBeenCalledTimes(3);
      expect(f.warning).not.toHaveBeenCalled();
      f.stat(target).ino = originalIno;
      f.unreadable.delete(key(target));
      f.entry(target).dacl = trustedDacl();
      f.state.output = snapshot => JSON.stringify(snapshot);
      expect(f.judge()).toBeUndefined();
      expect(f.judge()).toBeUndefined();
      expect(f.native).toHaveBeenCalledTimes(4);
    });

  it("CACHE-20 DB already absent BEFORE a fresh gate proof retains pre/post-create checks despite prior cache knowledge", async () => {
    const f = await fixture(USER, true);
    enforceAcl(); // explicit opt-in: this test asserts the enforced refusal
    f.warm();
    f.absent(DB);
    const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
    const create = vi.fn(() => { f.present(DB); f.unsafe(DB); });
    expect(() => f.gate!.openPrivateStateDb(DB, sqliteOpen, create))
      .toThrow(new RegExp(`state\\.db grants ${OTHER}`));
    expect(create).toHaveBeenCalledTimes(1);
    expect(sqliteOpen).not.toHaveBeenCalled();
    expect(f.native).toHaveBeenCalledTimes(3);
    expect(f.warning).not.toHaveBeenCalled();
  });

  it("CACHE-21 real gate positive control reaches the SQLite opener with the default reader and stable IDs", async () => {
    const f = await fixture(USER, true);
    const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
    const create = vi.fn();
    f.gate!.openPrivateStateDb(DB, sqliteOpen, create).close();
    expect(sqliteOpen).toHaveBeenCalledTimes(1);
    expect(sqliteOpen).toHaveBeenCalledWith(DB);
    expect(create).toHaveBeenCalledTimes(1);
    f.gate!.openPrivateStateDb(DB, sqliteOpen, create).close();
    expect(sqliteOpen).toHaveBeenCalledTimes(2);
    expect(f.native).toHaveBeenCalledTimes(1);
    expect(f.warning).not.toHaveBeenCalled();
    expect(f.advisories()).toEqual([]); // a safe root never advises
  });

  // Decision C: REAL gate -> REAL reader -> DEFAULT runner; recorded observations, not NTFS/deletion proof.
  it.each([DB, ROOT])("CACHE-19 real gate never calls the SQLite opener after known %s is renamed away during proof", async target => {
    const f = await fixture(USER, true);
    enforceAcl(); // explicit opt-in: this test asserts the enforced refusal
    const events: string[] = [];
    f.state.output = snapshot => {
      const completeSafeSnapshot = JSON.stringify(snapshot);
      events.push("safe snapshot complete", `rename away ${target}`);
      for (const file of target === ROOT ? [ROOT, FLAGS, ...FILES] : [DB]) f.absent(file);
      return completeSafeSnapshot;
    };
    f.state.observed = (file, options, stat) => {
      if (file === target && options?.bigint === true && stat === undefined) events.push(`observed missing ${target}`);
    };
    const exclusiveCreate = vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: string | number) => {
      expect(String(file)).toBe(DB);
      expect(flags).toBe("wx");
      events.push("exclusive create EEXIST");
      throw Object.assign(new Error("recorded planted winner"), { code: "EEXIST" });
    }) as typeof fs.openSync);
    const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
    const create = vi.fn(() => {
      events.push("plant replacement");
      for (const file of target === ROOT ? [ROOT, FLAGS, ...FILES] : [DB]) f.present(file);
      f.entry(DB).owner = OTHER;
      f.unsafe(DB);
      if (target === ROOT) { f.entry(ROOT).owner = OTHER; f.unsafe(ROOT); }
      try { fs.closeSync(fs.openSync(DB, "wx")); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    });
    let refusal: unknown;
    try { f.gate!.openPrivateStateDb(DB, sqliteOpen, create).close(); }
    catch (error) { refusal = error; }
    expect(events.slice(0, 3)).toEqual([
      "safe snapshot complete", `rename away ${target}`, `observed missing ${target}`,
    ]);
    expect(sqliteOpen).not.toHaveBeenCalled();
    expect(refusal).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
    expect(create).not.toHaveBeenCalled();
    expect(exclusiveCreate).not.toHaveBeenCalled();
    expect(f.native).toHaveBeenCalledTimes(1);
  });

  // Lead's presence table. Every row invokes the real gate and default reader, not a conditional fake opener.
  type Observation = "id" | "ino0" | "missing-ID" | "numeric-ID" | "unreadable";
  const observations: Observation[] = ["id", "ino0", "missing-ID", "numeric-ID", "unreadable"];
  const unknownObservations: Observation[] = ["ino0", "missing-ID", "numeric-ID"];
  type GateFixture = Awaited<ReturnType<typeof fixture>>;
  const observeAs = (f: GateFixture, target: string, observation: Observation, identity: bigint) => {
    f.rawUnreadable.delete(key(target));
    f.stat(target).dev = 7n;
    f.stat(target).ino = identity;
    if (observation === "ino0") f.stat(target).ino = 0n;
    if (observation === "missing-ID") Reflect.deleteProperty(f.stat(target), "ino");
    if (observation === "numeric-ID") Reflect.set(f.stat(target), "ino", Number(identity));
    if (observation === "unreadable") f.rawUnreadable.add(key(target));
  };
  const openOutcome = (f: GateFixture, open: SqliteOpener, create?: () => void) => {
    let refusal: unknown;
    try { f.gate!.openPrivateStateDb(DB, open, create).close(); }
    catch (error) { refusal = error; }
    return refusal;
  };

  it.each([ROOT, DB].flatMap(target => observations.flatMap(before => observations.map(after =>
    ({ target, before, after, cacheable: before === "id" && after === "id" })))))(
    "CACHE-22 TABLE $target $before -> $after: covered presence opens; only stable known ID caches", async row => {
      const f = await fixture(USER, true);
      const identity = f.stat(row.target).ino;
      observeAs(f, row.target, row.before, identity);
      f.state.output = snapshot => {
        const safe = JSON.stringify(snapshot);
        observeAs(f, row.target, row.after, identity);
        return safe;
      };
      const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
      expect(openOutcome(f, sqliteOpen)).toBeUndefined();
      expect(sqliteOpen).toHaveBeenCalledTimes(1);
      expect(f.native).toHaveBeenCalledTimes(1);
      observeAs(f, row.target, row.before, identity);
      expect(openOutcome(f, sqliteOpen)).toBeUndefined();
      expect(sqliteOpen).toHaveBeenCalledTimes(2);
      expect(sqliteOpen).toHaveBeenNthCalledWith(1, DB);
      expect(sqliteOpen).toHaveBeenNthCalledWith(2, DB);
      expect(f.native).toHaveBeenCalledTimes(row.cacheable ? 1 : 2);
      expect(f.warning).not.toHaveBeenCalled();
      expect(f.advisories()).toEqual([]); // safe evidence: no advisory, so no advisory read-skip either
    });

  it.each([ROOT, DB].flatMap(target => observations.flatMap(before =>
    ["native-missing/post-present", "native-missing/replacement", "post-missing", "post-wrong-type", "post-link"]
      .map(change => ({ target, before, change })))))(
    "CACHE-23 TABLE $target $before -> $change: refuse before create and SQLite", async row => {
      const f = await fixture(USER, true);
      enforceAcl(); // explicit opt-in: this row asserts the enforced refusal
      observeAs(f, row.target, row.before, f.stat(row.target).ino);
      f.state.output = snapshot => {
        const safe = JSON.stringify({ ...snapshot, entries: snapshot.entries.map(entry =>
          entry.path === row.target && row.change.startsWith("native-missing")
            ? { path: row.target, missing: true } : entry) });
        f.rawUnreadable.delete(key(row.target));
        if (row.change === "post-missing") f.absent(row.target);
        if (row.change === "post-wrong-type") {
          f.stat(row.target).isDirectory = () => row.target === DB;
          f.stat(row.target).isFile = () => row.target === ROOT;
        }
        if (row.change === "post-link") f.stat(row.target).isSymbolicLink = () => true;
        if (row.change === "native-missing/replacement") {
          f.present(row.target);
          f.entry(row.target).owner = OTHER;
          f.unsafe(row.target);
        }
        return safe;
      };
      const exclusiveCreate = vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: string | number) => {
        expect(String(file)).toBe(DB);
        expect(flags).toBe("wx");
        throw Object.assign(new Error("recorded planted winner"), { code: "EEXIST" });
      }) as typeof fs.openSync);
      const create = vi.fn(() => {
        for (const file of row.target === ROOT ? [ROOT, FLAGS, ...FILES] : [DB]) f.present(file);
        f.entry(row.target).owner = OTHER;
        f.unsafe(row.target);
        try { fs.closeSync(fs.openSync(DB, "wx")); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      });
      const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
      const refusal = openOutcome(f, sqliteOpen, create);
      expect(sqliteOpen).not.toHaveBeenCalled();
      expect(refusal).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
      expect(create).not.toHaveBeenCalled();
      expect(exclusiveCreate).not.toHaveBeenCalled();
      expect(f.native).toHaveBeenCalledTimes(1);
      expect(f.warning).not.toHaveBeenCalled();
    });

  it.each([ROOT, DB])("CACHE-24 TABLE known %s -> different known ID: refuse before create/opener", async target => {
    const f = await fixture(USER, true);
    enforceAcl(); // explicit opt-in: this test asserts the enforced refusal
    f.state.output = snapshot => {
      const safe = JSON.stringify(snapshot);
      f.stat(target).ino += 1n;
      return safe;
    };
    const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
    const create = vi.fn();
    expect(openOutcome(f, sqliteOpen, create)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
    expect(sqliteOpen).not.toHaveBeenCalled();
    expect(create).not.toHaveBeenCalled();
    expect(f.native).toHaveBeenCalledTimes(1);
  });

  it.each([ROOT, DB].flatMap(target => unknownObservations.flatMap(identity =>
    ["wrong-type", "link"].map(kind => ({ target, identity, kind })))))(
    "CACHE-25 TABLE initially $kind $target with $identity: invalid type refuses independently of ID", async row => {
      const f = await fixture(USER, true);
      enforceAcl(); // explicit opt-in: exact create/native counts are the enforced sequence
      observeAs(f, row.target, row.identity, f.stat(row.target).ino);
      if (row.kind === "link") f.stat(row.target).isSymbolicLink = () => true;
      else {
        f.stat(row.target).isDirectory = () => row.target === DB;
        f.stat(row.target).isFile = () => row.target === ROOT;
      }
      const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
      const create = vi.fn();
      expect(openOutcome(f, sqliteOpen, create)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
      expect(sqliteOpen).not.toHaveBeenCalled();
      expect(create).not.toHaveBeenCalled();
      expect(f.native).toHaveBeenCalledTimes(row.target === ROOT ? 0 : 1);
    });

  it.each(["safe", "unsafe"] as const)(
    "CACHE-26 TABLE absent DB before/during/after preproof -> explicit create (%s): unconditional postproof", async safety => {
      const f = await fixture(USER, true);
      if (safety === "unsafe") enforceAcl(); // the refusal row is enforcement-only
      f.absent(DB);
      const nativeDbPresence: boolean[] = [];
      f.state.output = snapshot => {
        nativeDbPresence.push(snapshot.entries.find(entry => entry.path === DB)!.missing !== true);
        return JSON.stringify(snapshot);
      };
      const create = vi.fn(() => { f.present(DB); if (safety === "unsafe") f.unsafe(DB); });
      const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
      const refusal = openOutcome(f, sqliteOpen, create);
      expect(create).toHaveBeenCalledTimes(1);
      expect(nativeDbPresence).toEqual([false, true]);
      expect(f.native).toHaveBeenCalledTimes(2);
      if (safety === "safe") {
        expect(refusal).toBeUndefined();
        expect(sqliteOpen).toHaveBeenCalledTimes(1);
        expect(sqliteOpen).toHaveBeenCalledWith(DB);
      } else {
        expect(refusal).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
        expect(sqliteOpen).not.toHaveBeenCalled();
      }
      expect(f.warning).not.toHaveBeenCalled();
    });

  it.each([ROOT, DB].flatMap(target => ["native-present", "native-missing"].flatMap(nativeState =>
    observations.map(after => ({ target, nativeState, after })))))(
    "CACHE-27 TABLE absent $target -> $after DURING proof ($nativeState), without create path: refuse", async row => {
      const f = await fixture(USER, true);
      enforceAcl(); // explicit opt-in: this row asserts the enforced refusal
      f.absent(row.target);
      f.state.output = snapshot => {
        f.present(row.target);
        observeAs(f, row.target, row.after, f.stat(row.target).ino);
        return JSON.stringify({ ...snapshot, entries: snapshot.entries.map(entry =>
          entry.path === row.target && row.nativeState === "native-present" ? f.entry(row.target) : entry) });
      };
      const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
      const create = vi.fn();
      expect(openOutcome(f, sqliteOpen, create)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
      expect(create).not.toHaveBeenCalled();
      expect(sqliteOpen).not.toHaveBeenCalled();
      expect(f.native).toHaveBeenCalledTimes(1);
    });

  it.each(["safe", "unsafe"] as const)(
    "CACHE-28 TABLE unreadable raw DB before -> native missing -> confirmed absent -> planted create (%s)", async safety => {
      const f = await fixture(USER, true);
      if (safety === "unsafe") enforceAcl(); // the refusal row is enforcement-only
      f.rawUnreadable.add(key(DB));
      const nativeDbPresence: boolean[] = [];
      f.state.output = snapshot => {
        const first = nativeDbPresence.length === 0;
        const wire = { ...snapshot, entries: snapshot.entries.map(entry =>
          first && entry.path === DB ? { path: DB, missing: true } : entry) };
        nativeDbPresence.push(wire.entries.find(entry => entry.path === DB)!.missing !== true);
        if (first) { f.rawUnreadable.delete(key(DB)); f.absent(DB); }
        return JSON.stringify(wire);
      };
      const create = vi.fn(() => { f.present(DB); if (safety === "unsafe") f.unsafe(DB); });
      const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
      const refusal = openOutcome(f, sqliteOpen, create);
      expect(create).toHaveBeenCalledTimes(1);
      expect(nativeDbPresence).toEqual([false, true]);
      expect(f.native).toHaveBeenCalledTimes(2);
      if (safety === "safe") {
        expect(refusal).toBeUndefined();
        expect(sqliteOpen).toHaveBeenCalledTimes(1);
        expect(sqliteOpen).toHaveBeenCalledWith(DB);
      } else {
        expect(refusal).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
        expect(sqliteOpen).not.toHaveBeenCalled();
      }
      expect(f.warning).not.toHaveBeenCalled();
    });

  it.each([ROOT, DB].flatMap(target => observations.flatMap(before => observations.map(after =>
    ({ target, before, after })))))(
    "CACHE-29 TABLE $target $before -> native MISSING -> $after: missing descriptor never covers presence", async row => {
      const f = await fixture(USER, true);
      enforceAcl(); // explicit opt-in: this row asserts the enforced refusal
      const identity = f.stat(row.target).ino;
      observeAs(f, row.target, row.before, identity);
      f.state.output = snapshot => {
        const missing = JSON.stringify({ ...snapshot, entries: snapshot.entries.map(entry =>
          entry.path === row.target ? { path: row.target, missing: true } : entry) });
        observeAs(f, row.target, row.after, identity);
        return missing;
      };
      const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
      const create = vi.fn();
      expect(openOutcome(f, sqliteOpen, create)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
      expect(sqliteOpen).not.toHaveBeenCalled();
      expect(create).not.toHaveBeenCalled();
      expect(f.native).toHaveBeenCalledTimes(1);
      expect(f.warning).not.toHaveBeenCalled();
    });

  it("CACHE-30 TABLE root absent -> native missing -> absent never bypasses createStateRoot/protect", async () => {
    const f = await fixture(USER, true);
    enforceAcl(); // explicit opt-in: this test asserts the enforced refusal
    f.absent(ROOT);
    const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
    const create = vi.fn();
    expect(openOutcome(f, sqliteOpen, create)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
    expect(create).not.toHaveBeenCalled();
    expect(sqliteOpen).not.toHaveBeenCalled();
    expect(f.native).toHaveBeenCalledTimes(1);
    expect(f.warning).not.toHaveBeenCalled();
  });

  it.each(["unreadable", "absent"] as const)(
    "CACHE-31 TABLE no-create gate: %s before -> native missing -> after absent cannot authorize a connection", async before => {
      const f = await fixture(USER, true);
      enforceAcl(); // explicit opt-in: this row asserts the enforced refusal
      if (before === "absent") f.absent(DB);
      else f.rawUnreadable.add(key(DB));
      let afterAbsent = false;
      let planted = false;
      f.state.output = snapshot => {
        const missing = JSON.stringify({ ...snapshot, entries: snapshot.entries.map(entry =>
          entry.path === DB ? { path: DB, missing: true } : entry) });
        f.rawUnreadable.delete(key(DB));
        f.absent(DB);
        return missing;
      };
      f.state.observed = (file, options, stat) => {
        if (file === DB && options?.bigint === true && stat === undefined) afterAbsent = true;
      };
      f.state.beforeLstat = (file, options) => {
        if (file === DB && options?.bigint !== true && afterAbsent) {
          f.present(DB);
          f.unsafe(DB);
          planted = true;
        }
      };
      const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
      const refusal = openOutcome(f, sqliteOpen);
      expect(sqliteOpen).not.toHaveBeenCalled();
      expect(refusal).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
      expect(planted).toBe(false);
      expect(f.native.mock.calls.length).toBeGreaterThanOrEqual(1);
      expect(f.native.mock.calls.length).toBeLessThanOrEqual(2);
      expect(f.warning).not.toHaveBeenCalled();
    });

  // Warn-by-default with an explicit, reversible opt-in. REAL gate -> REAL default reader (recorded snapshot); one
  // module instance across every switch, so the setting must be read at each call. The unsafe snapshot never changes:
  // only the setting does. The reader never caches an unsafe proof; the gate remembers a root's advisory, so default
  // opens after the first finding take no native read, while every enforced open takes a fresh one and refuses.
  const advisories = (f: GateFixture) => f.advisories();
  const unsetEnforce = () => vi.stubEnv(ENFORCE, undefined);

  it("ADVISORY-01 unsafe state.db ACE: default warns and reaches the opener; ENFORCE=1 refuses before it; unset/0 reach it again", async () => {
    const f = await fixture(USER, true);
    f.unsafe(DB);
    const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
    unsetEnforce();
    expect(process.env[ENFORCE]).toBeUndefined();
    expect(openOutcome(f, sqliteOpen)).toBeUndefined();
    expect(sqliteOpen).toHaveBeenCalledTimes(1);
    expect(sqliteOpen).toHaveBeenLastCalledWith(DB);
    expect(advisories(f)).toEqual([expect.stringContaining(`state.db grants ${OTHER}`)]);
    expect(advisories(f)[0]).toContain(ROOT);
    expect(advisories(f)[0]).toContain(ENFORCE);
    expect(f.native).toHaveBeenCalledTimes(1);

    enforceAcl();
    expect(openOutcome(f, sqliteOpen)).toMatchObject({
      code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringContaining(`state.db grants ${OTHER}`) });
    expect(sqliteOpen).toHaveBeenCalledTimes(1);
    expect(f.native).toHaveBeenCalledTimes(2); // fresh evidence: the earlier advisory open cached nothing

    unsetEnforce();
    expect(openOutcome(f, sqliteOpen)).toBeUndefined();
    expect(sqliteOpen).toHaveBeenCalledTimes(2);
    vi.stubEnv(ENFORCE, "0");
    expect(openOutcome(f, sqliteOpen)).toBeUndefined();
    expect(sqliteOpen).toHaveBeenCalledTimes(3);
    for (const call of [1, 2, 3]) expect(sqliteOpen).toHaveBeenNthCalledWith(call, DB);
    expect(f.native).toHaveBeenCalledTimes(2); // off again: the remembered advisory, no native read

    // Advisory opens repaired nothing: switching back on still refuses the same unchanged finding, from a fresh read.
    enforceAcl();
    expect(openOutcome(f, sqliteOpen)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
    expect(sqliteOpen).toHaveBeenCalledTimes(3);
    expect(f.native).toHaveBeenCalledTimes(3);
    expect(f.entry(DB).dacl).toContainEqual(allow(OTHER, FULL));
    expect(advisories(f)).toHaveLength(1); // one warning per checked root; refusals add none
  });

  it("ADVISORY-02 only the exact string 1 enforces: 0/false/true/yes/padded/empty values keep starting", async () => {
    const f = await fixture(USER, true);
    f.unsafe(ROOT);
    const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
    const advisoryValues = ["0", "false", "FALSE", "true", "yes", "on", "enforce", " 1", "1 ", "01", "1.0", "+1", ""];
    for (const [index, value] of advisoryValues.entries()) {
      vi.stubEnv(ENFORCE, value);
      expect(openOutcome(f, sqliteOpen), JSON.stringify(value)).toBeUndefined();
      expect(sqliteOpen, JSON.stringify(value)).toHaveBeenCalledTimes(index + 1);
    }
    enforceAcl();
    expect(openOutcome(f, sqliteOpen)).toMatchObject({
      code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringContaining(`grants ${OTHER}`) });
    expect(sqliteOpen).toHaveBeenCalledTimes(advisoryValues.length);
    expect(f.native).toHaveBeenCalledTimes(2); // the first advisory read, then the fresh enforced read
    expect(advisories(f)).toEqual([expect.stringContaining(`grants ${OTHER}`)]);
  });

  it("ADVISORY-03 warning dedupe never authorizes an enforced open and never hides the first actual warning", async () => {
    const f = await fixture(USER, true);
    f.unsafe(ROOT);
    const sqliteOpen = vi.fn(() => ({ close: vi.fn() }) as never);
    enforceAcl(); // on first, in a fresh module: refuses without spending the root's first warning
    expect(openOutcome(f, sqliteOpen)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
    expect(sqliteOpen).not.toHaveBeenCalled();
    expect(advisories(f)).toEqual([]);
    unsetEnforce();
    expect(openOutcome(f, sqliteOpen)).toBeUndefined();
    expect(sqliteOpen).toHaveBeenCalledTimes(1);
    expect(advisories(f)).toEqual([expect.stringContaining(`grants ${OTHER}`)]); // the first actual warning
    expect(openOutcome(f, sqliteOpen)).toBeUndefined();
    expect(sqliteOpen).toHaveBeenCalledTimes(2);
    expect(advisories(f)).toHaveLength(1); // deduped per root
    enforceAcl(); // an already-warned root is still refused
    expect(openOutcome(f, sqliteOpen)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
    expect(sqliteOpen).toHaveBeenCalledTimes(2);
    unsetEnforce();
    expect(openOutcome(f, sqliteOpen)).toBeUndefined();
    expect(sqliteOpen).toHaveBeenCalledTimes(3);
    // refuse, warn, (remembered), refuse, (remembered): each enforced open read afresh, advisory repeats did not.
    expect(f.native).toHaveBeenCalledTimes(3);
  });

  // The advisory default covers ACL evidence only: lstat type/link and the post-open inode check still refuse.
  it.each(["link", "wrong type", "replaced while opening"] as const)(
    "ADVISORY-04 default mode keeps the state.db %s integrity refusal", async kind => {
      const f = await fixture(USER, true);
      unsetEnforce();
      if (kind === "link") f.stat(DB).isSymbolicLink = () => true;
      if (kind === "wrong type") { f.stat(DB).isDirectory = () => true; f.stat(DB).isFile = () => false; }
      const close = vi.fn();
      const sqliteOpen = vi.fn(() => {
        if (kind === "replaced while opening") f.stat(DB).ino += 1n;
        return { close } as never;
      });
      const refusal = openOutcome(f, sqliteOpen);
      expect(refusal).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringContaining(DB) });
      if (kind === "replaced while opening") {
        expect((refusal as Error).message).toMatch(/replaced while opening/);
        expect(sqliteOpen).toHaveBeenCalledTimes(1);
        expect(close).toHaveBeenCalledTimes(1);
      } else expect(sqliteOpen).not.toHaveBeenCalled();
    });

  // P2: the gate's remembered advisory skips only the repeat ACL read, never the mandatory lstat/type/inode checks.
  it.each(["link", "wrong type", "replaced while opening"] as const)(
    "ADVISORY-05 after a remembered advisory (no native read) the state.db %s integrity refusal still runs", async kind => {
      const f = await fixture(USER, true);
      unsetEnforce();
      f.unsafe(DB);
      let armed = false;
      const close = vi.fn();
      const sqliteOpen = vi.fn(() => {
        if (armed && kind === "replaced while opening") f.stat(DB).ino += 1n;
        return { close } as never;
      });
      expect(openOutcome(f, sqliteOpen)).toBeUndefined();
      expect(f.native).toHaveBeenCalledTimes(1);
      expect(f.advisories()).toEqual([expect.stringContaining(`state.db grants ${OTHER}`)]);
      armed = true;
      if (kind === "link") f.stat(DB).isSymbolicLink = () => true;
      if (kind === "wrong type") { f.stat(DB).isDirectory = () => true; f.stat(DB).isFile = () => false; }
      const refusal = openOutcome(f, sqliteOpen);
      expect(refusal).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringContaining(DB) });
      expect(f.native).toHaveBeenCalledTimes(1); // the advisory hit skipped only the ACL read
      expect(f.advisories()).toHaveLength(1);
      if (kind === "replaced while opening") {
        expect((refusal as Error).message).toMatch(/replaced while opening/);
        expect(sqliteOpen).toHaveBeenCalledTimes(2);
        expect(close).toHaveBeenCalledTimes(2);
      } else expect(sqliteOpen).toHaveBeenCalledTimes(1);
      enforceAcl(); // and the remembered advisory never authorizes enforcement: a fresh read refuses
      expect(openOutcome(f, sqliteOpen)).toMatchObject({ code: "FABRIC_MESH_STATE_UNSUPPORTED" });
      expect(f.native).toHaveBeenCalledTimes(2);
    });
});
