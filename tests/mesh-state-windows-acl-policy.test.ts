// smarty-dev#8920: the Windows ACL policy of the mesh state gate. The native read is a pinned PowerShell child that
// Linux cannot run, so these feed recorded snapshots through the runner seam; windows-latest CI covers the real read.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { windowsStateAclRefusal, type WindowsStateAclRequest } from "../src/mesh/windows-state-acl.js";

const USER = "S-1-5-21-1-2-3-1001";
const OTHER = "S-1-5-21-1-2-3-1002";
const SYSTEM = "S-1-5-18";
const ADMINS = "S-1-5-32-544";
const TRUSTED_INSTALLER = "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464";
const AUTH_USERS = "S-1-5-11";
const USERS = "S-1-5-32-545";
const EVERYONE = "S-1-1-0";
const CREATOR_OWNER = "S-1-3-0";
const FULL = 0x1f01ff;
const MODIFY = 0x1301bf;
const READ_EXECUTE = 0x1200a9;
const ADD_SUBDIR = 0x4;
const DIR = 0x10;

type Ace = { type: number; flags: number; sid: string | null; mask: number | null };
type Entry = Record<string, unknown>;
const allow = (sid: string, mask: number, flags = 0x13): Ace => ({ type: 0, flags, sid, mask });
const profileDacl = (): Ace[] => [allow(SYSTEM, FULL), allow(ADMINS, FULL), allow(USER, FULL)];
const dir = (p: string, owner: string, dacl: Ace[] | null, extra: Entry = {}): Entry =>
  ({ path: p, attributes: DIR, owner, control: 0x8004, dacl, ...extra });

const request: WindowsStateAclRequest = {
  root: "C:\\Users\\me\\.pi\\mesh",
  real: "C:\\Users\\me\\.pi\\mesh",
  files: ["C:\\Users\\me\\.pi\\mesh\\state.db"],
  directories: ["C:\\Users\\me\\.pi\\mesh\\state-checkpoint.flags"],
};
// The order the module sends: root, ancestors (lexical then real), directories, files.
const defaultEntries = (): Entry[] => [
  dir("C:\\Users\\me\\.pi\\mesh", USER, profileDacl()),
  dir("C:\\Users\\me\\.pi", USER, profileDacl()),
  dir("C:\\Users\\me", USER, [allow(SYSTEM, FULL, 0x3), allow(ADMINS, FULL, 0x3), allow(USER, FULL, 0x3)]),
  // C:\Users: CREATOR OWNER inherit-only full control (the sticky analogue); read for Users and Everyone.
  dir("C:\\Users", SYSTEM, [allow(SYSTEM, FULL, 0x3), allow(ADMINS, FULL, 0x3), allow(USERS, READ_EXECUTE, 0),
    allow(EVERYONE, READ_EXECUTE, 0), allow(CREATOR_OWNER, 0x10000000, 0xb)]),
  // C:\: Authenticated Users may add subdirectories, and hold an inherit-only Modify for children.
  dir("C:\\", TRUSTED_INSTALLER, [allow(SYSTEM, FULL, 0x3), allow(ADMINS, FULL, 0x3), allow(USERS, READ_EXECUTE, 0x3),
    allow(AUTH_USERS, MODIFY, 0xb), allow(AUTH_USERS, ADD_SUBDIR, 0)]),
  { path: "C:\\Users\\me\\.pi\\mesh\\state-checkpoint.flags", missing: true },
  { path: "C:\\Users\\me\\.pi\\mesh\\state.db", attributes: 0x20, owner: USER, control: 0x8404, dacl: profileDacl() },
];

const runner = (entries: Entry[], extra: Entry = {}) => {
  const calls: unknown[] = [];
  const run = (input: string) => {
    calls.push(JSON.parse(input));
    return JSON.stringify({ userSid: USER, ownerSid: USER, protect: null, entries, ...extra });
  };
  return { run, calls };
};
const judge = (entries: Entry[], input: Partial<WindowsStateAclRequest> = {}, extra: Entry = {}) =>
  windowsStateAclRefusal({ ...request, ...input }, runner(entries, extra).run);
const withEntry = (index: number, change: Entry): Entry[] =>
  defaultEntries().map((entry, at) => (at === index ? { ...entry, ...change } : entry));

describe("Windows mesh state ACL policy (smarty-dev#8920)", () => {
  afterEach(() => vi.restoreAllMocks());

  it("accepts a profile root under the default C:\\ and C:\\Users ACLs, with one batched read", () => {
    const { run, calls } = runner(defaultEntries());
    expect(windowsStateAclRefusal(request, run)).toBeUndefined();
    expect(calls).toEqual([{ protect: null, paths: defaultEntries().map(entry => entry.path) }]);
  });

  it("reads lexical and real ancestors once each", () => {
    const { run, calls } = runner([]);
    windowsStateAclRefusal({ ...request, real: "D:\\data\\mesh", files: [], directories: [] }, run);
    expect((calls[0] as { paths: string[] }).paths).toEqual([
      "C:\\Users\\me\\.pi\\mesh", "C:\\Users\\me\\.pi", "C:\\Users\\me", "C:\\Users", "C:\\", "D:\\data", "D:\\",
    ]);
  });

  it("refuses a root with an Everyone:(M) grant, also beside a deny ACE", () => {
    const dacl = [...profileDacl(), { type: 1, flags: 0, sid: EVERYONE, mask: FULL }, allow(EVERYONE, MODIFY)];
    expect(judge(withEntry(0, { dacl }))).toMatch(/grants S-1-1-0 write, delete or permission rights/);
  });

  it("refuses a state file a foreign user may write, delete or re-ACL", () => {
    for (const mask of [0x2, 0x10000, 0x40000, 0x80000, 0x40000000]) {
      expect(judge(withEntry(6, { dacl: [...profileDacl(), allow(OTHER, mask, 0)] }))).toMatch(/state\.db grants/);
    }
  });

  it("refuses a root owned by another account, even a trusted one", () => {
    expect(judge(withEntry(0, { owner: OTHER }))).toMatch(/owned by S-1-5-21-1-2-3-1002, not the current user/);
    expect(judge(withEntry(0, { owner: ADMINS }))).toMatch(/not the current user/);
  });

  // Lead ruling: an elevated run creates the root, state.db, -wal, -shm and the flag directory owned by
  // BUILTIN\Administrators (its TokenOwner); that, and only that, default owner is accepted on every strict target.
  it("accepts a root and state files owned by Administrators only on an elevated run", () => {
    const elevated = { ownerSid: ADMINS };
    expect(judge(withEntry(6, { owner: ADMINS }), {}, elevated)).toBeUndefined();
    expect(judge(withEntry(0, { owner: ADMINS }), {}, elevated)).toBeUndefined(); // an existing elevated root
    expect(judge(withEntry(6, { owner: ADMINS }))).toMatch(/state\.db is owned by S-1-5-32-544, not the current user/);
    expect(judge(withEntry(0, { owner: ADMINS }))).toMatch(/mesh is owned by S-1-5-32-544, not the current user/); // not elevated
    expect(judge(withEntry(6, { owner: OTHER }), {}, elevated)).toMatch(/state\.db is owned by S-1-5-21-1-2-3-1002/);
    expect(judge(defaultEntries(), {}, { ownerSid: undefined })).toMatch(/invalid Windows ACL snapshot/);
  });

  // A token may name another group it belongs to as its default owner (a shared "Developers" group, Users...). Files
  // owned by that group are writable by its other members' processes: no default owner but elevated Administrators is
  // trusted, even when the snapshot says the token itself would create files so.
  it("refuses state files and a flag directory owned by any other token default owner", () => {
    for (const group of [OTHER, USERS, AUTH_USERS, EVERYONE]) {
      const tokenOwner = { ownerSid: group };
      expect(judge(withEntry(6, { owner: group }), {}, tokenOwner)).toMatch(new RegExp(`state\\.db is owned by ${group}`));
      expect(judge(withEntry(0, { owner: group }), {}, tokenOwner)).toMatch(new RegExp(`mesh is owned by ${group}`));
      const flags = dir("C:\\Users\\me\\.pi\\mesh\\state-checkpoint.flags", group, profileDacl());
      expect(judge(defaultEntries().map((entry, at) => (at === 5 ? flags : entry)), {}, tokenOwner))
        .toMatch(/state-checkpoint\.flags is owned by/);
    }
    // Administrators owning a file while the token's default owner is the user: not an elevated creation, refused.
    expect(judge(withEntry(6, { owner: ADMINS }), {}, { ownerSid: USER })).toMatch(/state\.db is owned by S-1-5-32-544/);
  });

  // CREATOR OWNER inherit-only full control gives whoever creates a child inside full control of it. On the root or
  // the flag directory a foreign creator could add a state-adjacent entry it controls, so strict targets refuse it;
  // an ancestor's is not its own access (the next component is judged itself), so C:\Users's default passes.
  it("refuses CREATOR OWNER inherit-only grants on strict targets, allows them on ancestors only", () => {
    const creator = allow(CREATOR_OWNER, 0x10000000, 0xb);
    expect(judge(withEntry(0, { dacl: [...profileDacl(), creator] }))).toMatch(/mesh grants S-1-3-0/);
    const flags = dir("C:\\Users\\me\\.pi\\mesh\\state-checkpoint.flags", USER, [...profileDacl(), creator]);
    expect(judge(defaultEntries().map((entry, at) => (at === 5 ? flags : entry)))).toMatch(/state-checkpoint\.flags grants S-1-3-0/);
    expect(judge(withEntry(1, { dacl: [...profileDacl(), creator] }))).toBeUndefined();
    expect(judge(defaultEntries())).toBeUndefined(); // C:\Users carries CREATOR OWNER (IO) full control
  });

  it("refuses an ancestor another account may delete, rename into or take over", () => {
    for (const mask of [0x10000, 0x40, 0x40000, 0x80000, MODIFY, FULL, 0x40000000, 0x100]) {
      expect(judge(withEntry(1, { dacl: [...profileDacl(), allow(AUTH_USERS, mask, 0)] }))).toMatch(/an ancestor of .* grants S-1-5-11/);
    }
  });

  it("allows only add-file/add-subdirectory and inherit-only grants on an ancestor", () => {
    expect(judge(withEntry(1, { dacl: [...profileDacl(), allow(OTHER, 0x6 | READ_EXECUTE, 0)] }))).toBeUndefined();
    expect(judge(withEntry(1, { dacl: [...profileDacl(), allow(OTHER, FULL, 0xb)] }))).toBeUndefined();
    // On the root itself an inherit-only foreign grant still refuses: the state files would inherit it.
    expect(judge(withEntry(0, { dacl: [...profileDacl(), allow(OTHER, FULL, 0xb)] }))).toMatch(/grants S-1-5-21-1-2-3-1002/);
  });

  it("refuses an ancestor owned by an untrusted account", () => {
    expect(judge(withEntry(3, { owner: OTHER }))).toMatch(/C:\\Users is owned by S-1-5-21-1-2-3-1002/);
  });

  it("fails closed on a null DACL, unknown ACE types or flags, and malformed output", () => {
    expect(judge(withEntry(0, { dacl: null }))).toMatch(/null or unproven DACL/);
    expect(judge(withEntry(0, { dacl: [{ type: 9, flags: 0, sid: USER, mask: FULL }] }))).toMatch(/unsupported or unproven ACE/);
    expect(judge(withEntry(0, { dacl: [allow(USER, FULL, 0x40)] }))).toMatch(/unsupported or unproven ACE/);
    expect(judge(withEntry(0, { dacl: [{ type: 0, flags: 0, sid: null, mask: null }] }))).toMatch(/unsupported or unproven ACE/);
    expect(judge(defaultEntries().slice(1))).toMatch(/invalid Windows ACL snapshot/);
    expect(windowsStateAclRefusal(request, () => "not json")).toMatch(/unreadable Windows ACL snapshot/);
    expect(judge(defaultEntries(), {}, { userSid: "me" })).toMatch(/invalid Windows ACL snapshot/);
  });

  it("refuses an unreadable or vanished component, but a missing state file or flag directory passes", () => {
    expect(judge(withEntry(2, { error: "Access is denied" }))).toMatch(/C:\\Users\\me has an unreadable Windows security descriptor/);
    expect(judge([...defaultEntries().slice(0, 2), { path: "C:\\Users\\me", missing: true }, ...defaultEntries().slice(3)]))
      .toMatch(/vanished/);
    expect(judge([...defaultEntries().slice(0, 6), { path: "C:\\Users\\me\\.pi\\mesh\\state.db", missing: true }])).toBeUndefined();
  });

  // A foreign-owned sidecar whose descriptor this user may not read (an attacker's -wal with a DACL denying us
  // READ_CONTROL) is reported per entry as an error, not as missing: it refuses, while a genuinely absent one passes.
  it("refuses a state file whose descriptor is unreadable; an absent one passes", () => {
    const files = ["C:\\Users\\me\\.pi\\mesh\\state.db", "C:\\Users\\me\\.pi\\mesh\\state.db-wal"];
    const head = defaultEntries().slice(0, 7);
    expect(judge([...head, { path: files[1], error: "Attempted to perform an unauthorized operation." }], { files }))
      .toMatch(/state\.db-wal has an unreadable Windows security descriptor/);
    expect(judge([...head, { path: files[1], missing: true }], { files })).toBeUndefined();
  });

  it("refuses an existing flag directory a foreign user may write", () => {
    const flags = dir("C:\\Users\\me\\.pi\\mesh\\state-checkpoint.flags", USER, [...profileDacl(), allow(USERS, 0x2, 0)]);
    expect(judge(defaultEntries().map((entry, at) => (at === 5 ? flags : entry)))).toMatch(/state-checkpoint\.flags grants S-1-5-32-545/);
  });

  // A junction on the chain (C:\Users\me\.pi -> D:\elsewhere) reads ambiguously: its own descriptor or its
  // target's. Lead ruling: refuse it, telling the user how to fix the configuration.
  it("refuses a reparse-point ancestor with an actionable message; the default profile chain passes", () => {
    expect(judge(withEntry(1, { attributes: DIR | 0x400 }))).toBe("C:\\Users\\me\\.pi, an ancestor of C:\\Users\\me\\.pi\\mesh, "
      + "is a reparse point (junction or symbolic link); set the mesh state root to a path with no junctions or symbolic links in it");
    expect(judge(withEntry(4, { attributes: DIR | 0x400 }))).toMatch(/^C:\\, an ancestor .* is a reparse point/);
    expect(judge(defaultEntries())).toBeUndefined();
  });

  it("refuses a reparse point or the wrong type among the strict targets", () => {
    expect(judge(withEntry(0, { attributes: DIR | 0x400 }))).toBe("C:\\Users\\me\\.pi\\mesh is a reparse point");
    expect(judge(withEntry(6, { attributes: 0x20 | 0x400 }))).toBe("C:\\Users\\me\\.pi\\mesh\\state.db is a reparse point");
    expect(judge(withEntry(6, { attributes: DIR }))).toMatch(/state\.db is not a regular file/);
  });

  it("a created root must come back protected and owned by the current user", () => {
    const { run, calls } = runner(withEntry(0, { control: 0x9004 }), { protect: "ok" });
    expect(windowsStateAclRefusal({ ...request, protect: true }, run)).toBeUndefined();
    expect((calls[0] as { protect: string }).protect).toBe("C:\\Users\\me\\.pi\\mesh");
    expect(judge(defaultEntries(), { protect: true }, { protect: "ok" })).toMatch(/still inherits its ACL/);
    expect(judge(withEntry(0, { control: 0x9004 }), { protect: true }, { protect: "failed: owner S-1-5-21-1-2-3-1002" }))
      .toMatch(/private ACL could not be set \(failed: owner S-1-5-21-1-2-3-1002\)/);
    expect(judge(withEntry(0, { control: 0x9004, owner: ADMINS }), { protect: true }, { protect: "ok" })).toMatch(/not the current user/);
    // Elevated: Administrators may own an EXISTING root, but a root we just made private must read back as ours.
    expect(judge(withEntry(0, { control: 0x9004, owner: ADMINS }), { protect: true }, { protect: "ok", ownerSid: ADMINS }))
      .toMatch(/owned by S-1-5-32-544, not the current user .*, which it was just given/);
  });

  // assertPrivatePath's direct callers (raiseCheckpointFlag's freshly made flag directory): the path is a strict
  // directory target with the shared owner rule, its parents ancestors; it is never judged as a state root.
  it("judges a direct private path as a directory target, with the elevated Administrators exception", () => {
    const flag = "C:\\Users\\me\\.pi\\mesh\\state-checkpoint.flags";
    const single = { root: flag, real: flag, role: "directory" as const, files: [], directories: [] };
    const entries = (owner: string, dacl = profileDacl()) => [dir(flag, owner, dacl), ...defaultEntries().slice(0, 5)];
    const { run, calls } = runner(entries(USER));
    expect(windowsStateAclRefusal(single, run)).toBeUndefined();
    expect((calls[0] as { paths: string[] }).paths).toEqual([flag, ...defaultEntries().slice(0, 5).map(entry => entry.path)]);
    expect(windowsStateAclRefusal(single, runner(entries(ADMINS), { ownerSid: ADMINS }).run)).toBeUndefined();
    expect(windowsStateAclRefusal(single, runner(entries(ADMINS)).run)).toMatch(/flags is owned by S-1-5-32-544/);
    expect(windowsStateAclRefusal(single, runner(entries(OTHER), { ownerSid: OTHER }).run)).toMatch(/flags is owned by S-1-5-21-1-2-3-1002/);
    expect(windowsStateAclRefusal(single, runner(entries(USER, [...profileDacl(), allow(USERS, 0x2, 0)])).run))
      .toMatch(/flags grants S-1-5-32-545/);
    // The kind is enforced: a directory where a file was expected refuses.
    expect(windowsStateAclRefusal({ ...single, role: "file" }, runner(entries(USER)).run)).toMatch(/is not a regular file/);
  });

  it("warns once and opens as before when the ACL reader cannot run", () => {
    const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
    const fail = () => { throw Object.assign(new Error("spawn powershell.exe ENOENT"), { code: "ENOENT" }); };
    expect(windowsStateAclRefusal(request, fail)).toBeUndefined();
    expect(windowsStateAclRefusal(request, fail)).toBeUndefined();
    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0]![0])).toMatch(/could not read Windows ACLs \(spawn powershell\.exe ENOENT\)/);
  });
});

describe("state gate wiring on win32 (smarty-dev#8920)", () => {
  afterEach(() => { vi.doUnmock("../src/mesh/windows-state-acl.js"); vi.resetModules(); });

  const asWindows = async <T>(run: () => Promise<T>): Promise<T> => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try { return await run(); } finally { Object.defineProperty(process, "platform", platform); }
  };
  // The native reader stands in for the live descriptors: `verdicts` is what it reports on each successive read.
  const loadGate = async (refusal: string | undefined, verdicts: (string | undefined)[] = []) => {
    const calls: WindowsStateAclRequest[] = [];
    vi.resetModules();
    vi.doMock("../src/mesh/windows-state-acl.js", () => ({
      windowsStateAclRefusal: (input: WindowsStateAclRequest) => {
        calls.push(input);
        return verdicts.length > 0 ? verdicts.shift() : refusal;
      },
    }));
    return { gate: await import("../src/mesh/state-gate.js"), calls };
  };
  const tmp = () => fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "winacl-"));

  it("refuses a proven foreign writer with FABRIC_MESH_STATE_UNSUPPORTED", async () => {
    const { gate } = await loadGate("C:\\x grants S-1-1-0 write");
    const root = tmp();
    await asWindows(async () => {
      expect(() => gate.assertPrivateStateRoot(root)).toThrow(expect.objectContaining({ code: "FABRIC_MESH_STATE_UNSUPPORTED" }));
    });
  });

  it("protects only a root it created, with the state files and flag directory in its read", async () => {
    const { gate, calls } = await loadGate(undefined);
    const root = path.join(tmp(), "mesh");
    await asWindows(async () => {
      gate.createStateRoot(root);
      gate.createStateRoot(root); // exists now: never re-protected
      expect(calls.map(call => call.protect)).toEqual([true]);
      expect(calls[0]!.files.map(file => path.basename(file))).toEqual(["state.db", "state.db-wal", "state.db-shm"]);
      expect(calls[0]!.directories.map(dir => path.basename(dir))).toEqual(["state-checkpoint.flags"]);
    });
  });

  // F1: another process may add an Everyone:(M) ACE between two checks without this process ever yielding (a native
  // ACL write is not a JS event). Two separate gate calls in one synchronous turn must each read the live ACL.
  it("re-reads the ACL for a second synchronous gate call in the same turn and sees an ACE added in between", async () => {
    const { gate, calls } = await loadGate(undefined, [undefined, "C:\\x grants S-1-1-0 write"]);
    const root = tmp();
    await asWindows(async () => {
      gate.assertPrivateStateRoot(root); // proven: no foreign writer yet
      expect(() => gate.assertPrivateStateRoot(root)).toThrow(/grants S-1-1-0 write/); // no await between
      expect(calls).toHaveLength(2);
    });
  });

  // F1: raiseCheckpointFlag calls the exported assertPrivatePath on its own, outside any open; a flag directory
  // another user may write must refuse there, not only inside the open's batched read.
  it("exported assertPrivatePath reads the native ACL: a foreign-writable flag directory refuses, nothing is written", async () => {
    const { gate, calls } = await loadGate("state-checkpoint.flags grants S-1-5-32-545 write");
    const sqlite = await import("../src/mesh/state-sqlite.js");
    const root = tmp();
    const flags = path.join(root, "state-checkpoint.flags");
    await asWindows(async () => {
      expect(gate.assertPrivatePath(path.join(root, "absent"), "directory")).toBeUndefined(); // missing passes, no read
      expect(calls).toHaveLength(0);
      fs.mkdirSync(flags);
      expect(() => gate.assertPrivatePath(flags, "directory")).toThrow(expect.objectContaining({ code: "FABRIC_MESH_STATE_UNSUPPORTED" }));
      expect(calls[0]).toMatchObject({ root: flags, role: "directory", directories: [], files: [], protect: false });
      expect(() => sqlite.raiseCheckpointFlag(root, "token")).toThrow(/grants S-1-5-32-545/);
      expect(fs.readdirSync(flags)).toEqual([]);
      expect(calls).toHaveLength(2); // assertPrivatePath, then raiseCheckpointFlag's own native read
    });
  });

  // Performance without a policy change: the read-only scan runs on every writer-lock attempt, so it must never spawn
  // the native reader; the open's gate read covers the flag directory. Its lstat checks stay on every call.
  it("checkpointFlagRaised polls a real flag directory with no ACL read, keeping the lstat checks", async () => {
    const { calls } = await loadGate(undefined);
    const sqlite = await import("../src/mesh/state-sqlite.js");
    const root = tmp();
    const flags = path.join(root, "state-checkpoint.flags");
    await asWindows(async () => {
      expect(sqlite.checkpointFlagRaised(root)).toBe(false); // missing
      fs.mkdirSync(flags);
      expect(sqlite.checkpointFlagRaised(root)).toBe(false); // empty
      fs.writeFileSync(path.join(flags, "1.2.flag"), "");
      for (let poll = 0; poll < 5; poll += 1) expect(sqlite.checkpointFlagRaised(root)).toBe(true);
      expect(calls).toHaveLength(0);
      // A symbolic link in its place, or a file, still raises nothing (lstat, every call).
      const other = path.join(root, "elsewhere");
      fs.renameSync(flags, other);
      fs.symlinkSync(other, flags);
      expect(sqlite.checkpointFlagRaised(root)).toBe(false);
      fs.unlinkSync(flags);
      fs.writeFileSync(flags, "");
      expect(sqlite.checkpointFlagRaised(root)).toBe(false);
      expect(calls).toHaveLength(0);
    });
  });

  // A stand-in connection: openPinned off Linux only re-lstats the checked state.db after open().
  const opener = (opened: string[]) => (file: string) => { opened.push(file); return { close: () => undefined } as never; };

  it("validates the root, state files and flag directory in one native read on every open", async () => {
    const { gate, calls } = await loadGate(undefined);
    const root = tmp();
    const file = path.join(root, "state.db");
    fs.writeFileSync(file, "");
    const opened: string[] = [];
    await asWindows(async () => {
      gate.openPrivateStateDb(file, opener(opened)).close();
      gate.openPrivateStateDb(file, opener(opened)).close();
    });
    expect(opened).toEqual([file, file]);
    expect(calls).toHaveLength(2); // one per open, never cached
    for (const call of calls) {
      expect(call).toMatchObject({ root, protect: false });
      expect(call.files).toEqual([file, `${file}-wal`, `${file}-shm`]);
      expect(call.directories).toEqual([path.join(root, "state-checkpoint.flags")]);
    }
  });

  it("runs a Windows create inside the gate after its one read; a refused root gets no state.db", async () => {
    const order: string[] = [];
    const { gate, calls } = await loadGate(undefined);
    const root = tmp();
    const file = path.join(root, "state.db");
    await asWindows(async () => {
      gate.openPrivateStateDb(file, (opened) => { order.push(`open ${calls.length}`); return opener([])(opened); },
        () => { order.push(`create ${calls.length}`); fs.writeFileSync(file, "", { flag: "wx" }); }).close();
    });
    expect(order).toEqual(["create 1", "open 1"]); // one read, before the create
    const refused = await loadGate("C:\\x grants S-1-1-0 write");
    const other = tmp();
    let created = false;
    await asWindows(async () => {
      expect(() => refused.gate.openPrivateStateDb(path.join(other, "state.db"), opener([]), () => { created = true; }))
        .toThrow(/grants S-1-1-0 write/);
    });
    expect(created).toBe(false);
    expect(fs.readdirSync(other)).toEqual([]);
  });

  it("opens a store on an existing root with one native read, sync and async", async () => {
    const { calls } = await loadGate(undefined);
    const { SqliteStateStore } = await import("../src/mesh/state-sqlite.js");
    const root = tmp();
    await asWindows(async () => {
      SqliteStateStore.openSync(root, 1024, 16, { initialize: "detached" }).close();
      expect(calls).toHaveLength(1);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
      (await SqliteStateStore.open(root, 1024, 16, { initialize: "detached" })).close();
      expect(calls).toHaveLength(2);
    });
    expect(calls.every(call => call.root === root && call.protect === false && call.directories.length === 1)).toBe(true);
  });

  // F3: a recursive mkdir returns the FIRST directory it created. If another account creates the root right after
  // its missing parent was made, the root is theirs: it must never be re-owned or re-ACLed as ours.
  it("never protects a root another account created at the recursive-parent boundary", async () => {
    const { gate, calls } = await loadGate(undefined);
    const root = path.join(tmp(), "missing-parent", "mesh");
    const mkdir = fs.mkdirSync.bind(fs);
    const made: unknown[][] = [];
    vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
      made.push([target, options?.recursive ?? false]);
      if (target === root && !options?.recursive) mkdir(root); // the other account wins the race
      return mkdir(target, options);
    }) as typeof fs.mkdirSync);
    try {
      await asWindows(async () => { gate.createStateRoot(root); });
    } finally { vi.mocked(fs.mkdirSync).mockRestore(); }
    expect(made).toEqual([[path.dirname(root), true], [root, false]]);
    expect(calls).toEqual([]); // no protect, no mutation: the gate judges it on open
  });
});
