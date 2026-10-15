// Decision C: MOCKED unit evidence, NOT Windows/NTFS ACL proof or a timing benchmark.
// Keep reason: trust boundary/data safety. Exercise the existing public policy with its DEFAULT native runner;
// only execFileSync's output and Windows-path lstats are fake. No native helpers, filesystem fixtures or new APIs.
import childProcess from "node:child_process";
import fs from "node:fs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WindowsStateAclRequest } from "../src/mesh/windows-state-acl.js";

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

// Nanoseconds are deliberate: +1ns below leaves ctimeMs unchanged. No wall clock or real ACL claim.
const fakeStat = (ino: bigint, directory: boolean): fs.BigIntStats => ({
  dev: 7n, ino, ctimeNs: 1_700_000_000_000_000_000n, ctimeMs: 1_700_000_000_000n,
  isSymbolicLink: () => false, isDirectory: () => directory, isFile: () => !directory,
} as fs.BigIntStats);
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;

beforeEach(() => vi.resetModules());
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  Object.defineProperty(process, "platform", platform);
  vi.resetModules();
});

const fixture = async (owner = USER) => {
  // Load before installing the filesystem seam, so the module loader never consumes fake Windows stats.
  const { windowsStateAclRefusal } = await import("../src/mesh/windows-state-acl.js");
  Object.defineProperty(process, "platform", { ...platform, value: "win32" });
  vi.stubEnv("SystemRoot", "C:\\Windows");
  const warning = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
  const stats = new Map<string, fs.BigIntStats>();
  const entries = new Map<string, Entry>();
  const unreadable = new Set<string>();
  let nextIno = 100n;
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
  const actualLstat = fs.lstatSync.bind(fs);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: fs.StatSyncOptions) => {
    const name = String(file);
    if (!/^[a-z]:[\\/]/i.test(name)) return actualLstat(file, options);
    if (!PLAN.some(target => key(target) === key(name))) throw new Error(`unexpected mocked Windows path: ${name}`);
    if (unreadable.has(key(name))) throw Object.assign(new Error("mocked lstat denied"), { code: "EACCES" });
    const found = stats.get(key(name));
    if (found) return found;
    if (options?.throwIfNoEntry === false) return undefined;
    throw Object.assign(new Error("mocked missing path"), { code: "ENOENT" });
  }) as typeof fs.lstatSync);
  const state = {
    plan: [...PLAN],
    failure: undefined as Error | undefined,
    output: (snapshot: Snapshot): string => JSON.stringify(snapshot),
  };
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
  return { judge, warm, native, warning, stat, entry, unsafe, present, absent, unreadable, state };
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
    "CACHE-03 same-turn ctimeNs change on %s forces a fresh snapshot and foreign-ACE refusal", async target => {
      const f = await fixture();
      f.warm();
      f.stat(target).ctimeNs += 1n; // ctimeMs/dev/ino stay identical: millisecond stamps cannot establish this hit
      f.unsafe(target);
      const refusal = f.judge();
      expect(refusal).toContain(target);
      expect(refusal).toContain(`grants ${OTHER}`);
      expect(f.native).toHaveBeenCalledTimes(2);
    });

  it.each(["dev", "ino"] as const)("CACHE-04 %s replacement invalidates even with identical ctimeNs", async field => {
    const f = await fixture();
    f.warm();
    f.stat(DB)[field] += 1n;
    f.unsafe(DB);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(2);
  });

  it.each([DB, `${DB}-wal`, `${DB}-shm`, `${DB}-journal`, FLAGS])(
    "CACHE-05 previously absent %s appearing invalidates without relying on parent ctime", async target => {
      const f = await fixture();
      f.absent(target);
      f.warm();
      f.present(target); // all other identity stamps intentionally unchanged
      f.unsafe(target);
      expect(f.judge()).toContain(`grants ${OTHER}`);
      expect(f.native).toHaveBeenCalledTimes(2);
    });

  it("CACHE-06 an expanded target plan cannot reuse a narrower successful proof", async () => {
    const f = await fixture();
    const narrow = { ...request(), files: [] };
    f.state.plan = PLAN.filter(file => !FILES.includes(file));
    f.warm(narrow);
    f.unsafe(DB); // same identity, but this descriptor has never been verified in the narrower proof
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
      f.warm(ordinary);
      expect(f.judge({ ...ordinary, role })).toContain(`grants ${OTHER}`);
      expect(f.native).toHaveBeenCalledTimes(2);
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

  it.each([ROOT, DB, LEXICAL[0]!, REAL[0]!])(
    "CACHE-12 missing ctimeNs on %s means full native read on every call", async target => {
      const f = await fixture();
      f.warm();
      Reflect.deleteProperty(f.stat(target), "ctimeNs");
      expect(f.judge()).toBeUndefined();
      expect(f.judge()).toBeUndefined();
      expect(f.native).toHaveBeenCalledTimes(3);
      f.unsafe(target); // no observable identity signal: insufficient stamps must never justify a hit
      expect(f.judge()).toContain(`grants ${OTHER}`);
      expect(f.native).toHaveBeenCalledTimes(4);
    });

  it("CACHE-13 an unreadable identity disables hits, not the full native ACL read", async () => {
    const f = await fixture();
    f.warm();
    f.unreadable.add(key(REAL[0]!));
    expect(f.judge()).toBeUndefined();
    expect(f.judge()).toBeUndefined();
    expect(f.native).toHaveBeenCalledTimes(3);
    f.unsafe(REAL[0]!);
    expect(f.judge()).toContain(`grants ${OTHER}`);
    expect(f.native).toHaveBeenCalledTimes(4);
  });
});
