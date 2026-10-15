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
const LOCAL_SERVICE = "S-1-5-19";
const NETWORK_SERVICE = "S-1-5-20";
const ADMINS = "S-1-5-32-544";
const TRUSTED_INSTALLER = "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464";
const AUTH_USERS = "S-1-5-11";
const USERS = "S-1-5-32-545";
const EVERYONE = "S-1-1-0";
const CREATOR_OWNER = "S-1-3-0";
const CREATOR_GROUP = "S-1-3-1";
const FULL = 0x1f01ff;
const MODIFY = 0x1301bf;
const READ_EXECUTE = 0x1200a9;
const FOREIGN_READ = 0x120089;
const METADATA_ONLY = 0x00120080;
const GENERIC_READ = 0x80000000;
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
const strictTargets = ["mesh root", "state-checkpoint.flags", "state.db", "state.db-wal", "state.db-shm", "state.db-journal"] as const;
const strictRefusal = (
  target: typeof strictTargets[number], ace: Ace | Ace[], owner = USER, tokenOwner = USER,
): string | undefined => {
  const dacl = [...profileDacl(), ...(Array.isArray(ace) ? ace : [ace])];
  const identity = { ownerSid: tokenOwner };
  if (target === "mesh root") return judge(withEntry(0, { owner, dacl }), {}, identity);
  if (target === "state-checkpoint.flags") {
    const flags = dir("C:\\Users\\me\\.pi\\mesh\\state-checkpoint.flags", owner, dacl);
    return judge(defaultEntries().map((entry, at) => (at === 5 ? flags : entry)), {}, identity);
  }
  if (target === "state.db") return judge(withEntry(6, { owner, dacl }), {}, identity);
  const file = `${request.root}\\${target}`;
  return judge([...defaultEntries(), { path: file, attributes: 0x20, owner, control: 0x8404, dacl }],
    { files: [...request.files, file] }, identity);
};
const roleRefusal = (
  target: string, role: "ancestor" | "creation-parent", owner: string, dacl: Ace[],
): string | undefined => {
  const entries = [dir(target, owner, dacl)];
  for (let at = path.win32.dirname(target); ; at = path.win32.dirname(at)) {
    if (at.toLowerCase() !== target.toLowerCase()) entries.push(dir(at, USER, profileDacl()));
    if (at === path.win32.dirname(at)) break;
  }
  return windowsStateAclRefusal({ root: target, real: target, role, files: [], directories: [] }, runner(entries).run);
};

describe("Windows mesh state ACL policy (smarty-dev#8920)", () => {
  afterEach(() => vi.restoreAllMocks());

  it("accepts a profile root under the default C:\\ and C:\\Users ACLs, with one batched read", () => {
    const { run, calls } = runner(defaultEntries());
    expect(windowsStateAclRefusal(request, run)).toBeUndefined();
    expect(calls).toEqual([{ protect: null, paths: defaultEntries().map(entry => entry.path) }]);
  });

  // F1 trust boundary: the nearest existing parent of a missing root uses the approved ANCESTOR policy,
  // not strict-root policy. Default volume/profile add-child and inherit-only grants must still pass.
  it("preflights an existing creation parent as an ancestor, refusing reparse points and foreign writers", () => {
    const parent = "C:\\Users\\me";
    const input: WindowsStateAclRequest = { root: parent, real: parent, role: "ancestor", files: [], directories: [] };
    const entries = defaultEntries().slice(2, 5);
    entries[0] = dir(parent, USER, [...profileDacl(), allow(OTHER, 0x6, 0), allow(CREATOR_OWNER, FULL, 0xb)]);
    const { run, calls } = runner(entries);
    expect(windowsStateAclRefusal(input, run)).toBeUndefined();
    expect(calls).toEqual([{ protect: null, paths: entries.map(entry => entry.path) }]);
    expect(windowsStateAclRefusal(input, runner([{ ...entries[0], attributes: DIR | 0x400 }, ...entries.slice(1)]).run))
      .toMatch(/ancestor .* reparse point/);
    expect(windowsStateAclRefusal(input, runner([{ ...entries[0], dacl: [allow(OTHER, MODIFY, 0)] }, ...entries.slice(1)]).run))
      .toMatch(/ancestor .* grants .* write, delete or permission rights/);
  });

  it("accepts LocalService and NetworkService owners only on ancestor roles with a safe DACL", () => {
    const safeDacl = [...profileDacl(), allow(CREATOR_OWNER, FULL, 0xb)];
    for (const owner of [LOCAL_SERVICE, NETWORK_SERVICE]) {
      for (const role of ["ancestor", "creation-parent"] as const) {
        expect(roleRefusal("C:\\Users\\me\\.pi", role, owner, safeDacl), `${owner} ${role}`).toBeUndefined();
        expect(roleRefusal("C:\\Users\\me\\.pi", role, OTHER, safeDacl), `${OTHER} ${role}`).toMatch(/owned by/);
      }
    }
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
    expect(judge(withEntry(0, { dacl }))).toMatch(/grants S-1-1-0 (?:read, )?write, delete or permission rights/);
  });

  it("refuses untrusted data-reading ACEs on every strict target and names read access in the refusal", () => {
    const readAces = [
      allow(EVERYONE, FOREIGN_READ, 0),
      allow(USERS, GENERIC_READ, 0),
      allow(EVERYONE, FOREIGN_READ, 0xb), // inherit-only still reaches state files and the flag directory
      ...[0x1, 0x8, 0x20, 0x20000000].flatMap(mask => [allow(OTHER, mask, 0), allow(OTHER, mask, 0xb)]),
    ];
    const refusals = strictTargets.flatMap(target => readAces.map(ace => strictRefusal(target, ace)));
    expect(refusals).toEqual(strictTargets.flatMap(() => readAces.map(() => expect.stringMatching(/read/i))));
  });

  it("allows metadata-only reads on strict targets and leaves trusted full-control grants unchanged", () => {
    expect(strictTargets.map(target => strictRefusal(target, allow(EVERYONE, METADATA_ONLY, 0))))
      .toEqual(strictTargets.map(() => undefined));
    const trustedAces = [USER, SYSTEM, ADMINS, TRUSTED_INSTALLER].map(sid => allow(sid, FULL, 0xb));
    for (const target of strictTargets) {
      expect(trustedAces.map(ace => strictRefusal(target, ace)), target).toEqual(trustedAces.map(() => undefined));
    }
  });

  it("keeps foreign read grants permitted on ordinary ancestors and the immediate creation parent", () => {
    expect(judge(withEntry(1, { dacl: [...profileDacl(), allow(EVERYONE, FOREIGN_READ, 0)] }))).toBeUndefined();
    const parent = "C:\\Users\\me\\.pi";
    const creationParent: WindowsStateAclRequest = { root: parent, real: parent, role: "creation-parent", files: [], directories: [] };
    expect(windowsStateAclRefusal(creationParent, runner([
      dir(parent, USER, [...profileDacl(), allow(EVERYONE, FOREIGN_READ, 0)]), ...defaultEntries().slice(2, 5),
    ]).run)).toBeUndefined();
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

  it("keeps LocalService and NetworkService untrusted on every strict target", () => {
    for (const target of strictTargets) {
      for (const serviceSid of [LOCAL_SERVICE, NETWORK_SERVICE]) {
        expect(strictRefusal(target, allow(OTHER, FULL, 0), serviceSid, serviceSid), `${target} ${serviceSid}`)
          .toMatch(new RegExp(`is owned by ${serviceSid}, not the current user`));
      }
      expect(strictRefusal(target, [], USER, USER), `${target} user owner`).toBeUndefined();
      expect(strictRefusal(target, [], ADMINS, ADMINS), `${target} elevated owner`).toBeUndefined();
    }
    // Strict targets still stop at the existing ownership error before inspecting a bad DACL.
    const firstError = strictRefusal("mesh root", allow(OTHER, FULL, 0), LOCAL_SERVICE, LOCAL_SERVICE);
    expect(firstError).toMatch(/mesh is owned by S-1-5-19, not the current user/);
    expect(firstError).not.toContain("mask 0x");
  });

  it("does not treat service SIDs as trusted DACL trustees", () => {
    for (const serviceSid of [LOCAL_SERVICE, NETWORK_SERVICE]) {
      for (const target of strictTargets) {
        expect(strictRefusal(target, allow(serviceSid, FULL, 0))).toMatch(new RegExp(`grants ${serviceSid} .*mask 0x1f01ff`));
      }
      for (const role of ["ancestor", "creation-parent"] as const) {
        const flags = role === "ancestor" ? 0 : 0xb;
        expect(roleRefusal("C:\\Users\\me\\.pi", role, USER,
          [...profileDacl(), allow(serviceSid, FULL, flags)]))
          .toMatch(new RegExp(`grants ${serviceSid} .*mask 0x1f01ff`));
      }
    }
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

  // CO IO expands to the child's creator, not an arbitrary foreign account. The exception is safe only if the
  // entire strict DACL still denies foreign add-child/write rights; it must never short-circuit that DACL scan.
  it.each(strictTargets)("accepts safe CREATOR OWNER inherit-only full control on %s beside foreign metadata only", target => {
    for (const flags of [0x8, 0xb, 0x1b]) {
      for (const mask of [FULL, 0x10000000]) {
        const creator = allow(CREATOR_OWNER, mask, flags);
        expect(strictRefusal(target, creator)).toBeUndefined();
        for (const metadataFlags of [0, 0xb]) {
          const metadata = allow(OTHER, METADATA_ONLY, metadataFlags);
          expect(strictRefusal(target, [metadata, creator])).toBeUndefined();
          expect(strictRefusal(target, [creator, metadata])).toBeUndefined();
        }
      }
    }
    expect(judge(withEntry(1, { dacl: [...profileDacl(), allow(CREATOR_OWNER, FULL, 0xb)] }))).toBeUndefined();
    expect(judge(defaultEntries())).toBeUndefined(); // C:\Users carries CREATOR OWNER (IO) full control
  });

  it.each(strictTargets)("refuses every other unsafe foreign allow on %s before or after CREATOR OWNER IO", target => {
    const creator = allow(CREATOR_OWNER, FULL, 0xb);
    for (const mask of [0x2, ADD_SUBDIR, 0x40000000, 0x10000000, FULL, FOREIGN_READ, GENERIC_READ]) {
      for (const flags of [0, 0xb]) {
        const foreign = allow(OTHER, mask, flags);
        for (const aces of [[foreign, creator], [creator, foreign]]) {
          expect(strictRefusal(target, aces), `mask 0x${mask.toString(16)}, flags ${flags}`)
            .toMatch(/grants S-1-5-21-1-2-3-1002 .*rights/);
        }
      }
    }
  });

  it.each(strictTargets)("does not let deny ACEs cancel an unsafe allow beside CREATOR OWNER IO on %s", target => {
    const creator = allow(CREATOR_OWNER, FULL, 0xb);
    const foreign = allow(OTHER, ADD_SUBDIR, 0);
    const deny = { ...foreign, type: 1, mask: FULL };
    for (const aces of [
      [deny, creator, foreign], [deny, foreign, creator], [creator, deny, foreign],
      [creator, foreign, deny], [foreign, deny, creator], [foreign, creator, deny],
    ]) {
      expect(strictRefusal(target, aces)).toMatch(/grants S-1-5-21-1-2-3-1002 .*rights/);
    }
  });

  it.each(strictTargets)("refuses Creator Group IO and non-inherit-only CREATOR OWNER full control on %s", target => {
    for (const mask of [FULL, 0x10000000]) {
      expect(strictRefusal(target, allow(CREATOR_GROUP, mask, 0xb))).toMatch(/grants S-1-3-1/);
      for (const flags of [0, 0x3, 0x13]) {
        expect(strictRefusal(target, allow(CREATOR_OWNER, mask, flags))).toMatch(/grants S-1-3-0/);
      }
    }
  });

  it("validates ACEs after a safe CREATOR OWNER IO ACE instead of returning early", () => {
    expect(strictRefusal("mesh root", [allow(CREATOR_OWNER, FULL, 0xb), allow(USER, FULL, 0x40)]))
      .toMatch(/unsupported or unproven ACE/);
  });

  it("advises a private project folder or mesh root override without accepting unsafe Users grants", () => {
    const advice = "use a private project folder, or set mesh.root/PI_FABRIC_MESH_ROOT to a private path";
    const rootRefusal = strictRefusal("mesh root", allow(USERS, ADD_SUBDIR, 0));
    expect(rootRefusal).toMatch(/mesh grants S-1-5-32-545 .*mask 0x4/);
    expect(rootRefusal).toContain(advice);
    const parent = "C:\\Users\\me\\.pi";
    const creationParent: WindowsStateAclRequest = { root: parent, real: parent, role: "creation-parent", files: [], directories: [] };
    const creator = allow(CREATOR_OWNER, FULL, 0xb);
    for (const flags of [0x3, 0xb]) {
      const foreign = allow(USERS, ADD_SUBDIR, flags);
      for (const aces of [[creator, foreign], [foreign, creator]]) {
        const parentRefusal = windowsStateAclRefusal(creationParent, runner([
          dir(parent, USER, [...profileDacl(), ...aces]), ...defaultEntries().slice(2, 5),
        ]).run);
        expect(parentRefusal).toMatch(/immediate creation parent .* grants S-1-5-32-545 inheritable .*mask 0x4/);
        expect(parentRefusal).toContain(advice);
      }
    }
  });

  it("checks root inheritance even while SQLite sidecars are absent, permitting only trusted creator expansion", () => {
    const sidecars = ["-wal", "-shm", "-journal"].map(suffix => `${request.files[0]}${suffix}`);
    const files = [...request.files, ...sidecars];
    const creator = allow(CREATOR_OWNER, FULL, 0xb);
    const entries = [...defaultEntries(), ...sidecars.map(file => ({ path: file, missing: true }))];
    entries[0] = dir(request.root, USER, [...profileDacl(), creator]);
    expect(judge(entries, { files })).toBeUndefined();
    // Absence cannot hide a foreign read grant that would reach a later SQLite-created sidecar.
    entries[0] = dir(request.root, USER, [...profileDacl(), creator, allow(OTHER, FOREIGN_READ, 0xb)]);
    expect(judge(entries, { files })).toMatch(/mesh grants S-1-5-21-1-2-3-1002 read/);
    // Once created by SQLite, CO expands to the user (or elevated token owner Administrators), both trusted.
    for (const owner of [USER, ADMINS]) {
      entries[0] = dir(request.root, owner, [...profileDacl(), creator]);
      const present = entries.map(entry => sidecars.includes(entry.path as string)
        ? { path: entry.path, attributes: 0x20, owner, control: 0x8404, dacl: [...profileDacl(), allow(owner, FULL, 0)] }
        : entry);
      expect(judge(present, { files }, { ownerSid: owner })).toBeUndefined();
    }
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

  it("refuses unsafe foreign inheritance on the immediate creation parent, but keeps CREATOR OWNER and distant IO safe", () => {
    const parent = "C:\\Users\\me\\.pi";
    const creationParent: WindowsStateAclRequest = {
      root: parent, real: parent, role: "creation-parent", files: [], directories: [],
    };
    const entries = defaultEntries().slice(1, 5);
    expect(windowsStateAclRefusal(creationParent, runner([
      dir(parent, USER, [...profileDacl(), allow(OTHER, FULL, 0xb)]), ...entries.slice(1),
    ]).run)).toMatch(/immediate creation parent .* grants S-1-5-21-1-2-3-1002 inheritable/);
    expect(windowsStateAclRefusal(creationParent, runner([
      dir(parent, USER, [...profileDacl(), allow(OTHER, MODIFY, 0x3)]), ...entries.slice(1),
    ]).run)).toMatch(/immediate creation parent .* grants S-1-5-21-1-2-3-1002 inheritable/);
    expect(windowsStateAclRefusal(creationParent, runner([
      dir(parent, USER, [...profileDacl(), allow(OTHER, 0x40, 0x8)]), ...entries.slice(1),
    ]).run)).toMatch(/immediate creation parent .* grants S-1-5-21-1-2-3-1002 inheritable/);
    expect(windowsStateAclRefusal(creationParent, runner([
      dir(parent, USER, [...profileDacl(), allow(CREATOR_OWNER, FULL, 0xb)]), ...entries.slice(1),
    ]).run)).toBeUndefined();
    // C:\\ retains its approved inherit-only Modify grant because it is a distant ancestor, not the creation parent.
    expect(windowsStateAclRefusal(creationParent, runner(entries).run)).toBeUndefined();
    // An ordinary, non-inheritable add-child grant remains the approved ancestor control.
    expect(windowsStateAclRefusal(creationParent, runner([
      dir(parent, USER, [...profileDacl(), allow(OTHER, 0x6, 0)]), ...entries.slice(1),
    ]).run)).toBeUndefined();
  });

  it("preserves Users CI, AD and IO classification equally on C: and D:", () => {
    for (const drive of ["C", "D"]) {
      const volume = `${drive}:\\`;
      expect(roleRefusal(volume, "ancestor", USER, [
        ...profileDacl(), allow(USERS, ADD_SUBDIR, 0x2), allow(USERS, ADD_SUBDIR, 0), allow(USERS, FULL, 0xb),
      ]), `${drive}: ordinary ancestor`).toBeUndefined();

      const parent = `${drive}:\\Users`;
      expect(roleRefusal(parent, "creation-parent", USER, [...profileDacl(), allow(USERS, ADD_SUBDIR, 0)]),
        `${drive}: non-inheritable AD`).toBeUndefined();
      expect(roleRefusal(parent, "creation-parent", USER, [...profileDacl(), allow(USERS, ADD_SUBDIR, 0x2)]),
        `${drive}: CI plus AD`).toMatch(new RegExp(`grants ${USERS} inheritable .*mask 0x4`));
      expect(roleRefusal(parent, "creation-parent", USER, [...profileDacl(), allow(USERS, FULL, 0xb)]),
        `${drive}: IO write`).toMatch(new RegExp(`grants ${USERS} inheritable .*mask 0x1f01ff`));
    }
  });

  it("still refuses Users write, modify, full, delete-child, DACL and owner rights on both ancestor roles", () => {
    const unsafeMasks = [0x40000000, MODIFY, FULL, 0x40, 0x40000, 0x80000];
    for (const role of ["ancestor", "creation-parent"] as const) {
      const flags = role === "ancestor" ? 0 : 0x2;
      for (const mask of unsafeMasks) {
        expect(roleRefusal("C:\\Users\\me\\.pi", role, USER,
          [...profileDacl(), allow(USERS, mask, flags)]))
          .toMatch(new RegExp(`grants ${USERS} .*mask 0x${mask.toString(16)}`));
      }
    }
  });

  it("refuses Users and service-trustee writes even when that same service owns the ancestor", () => {
    for (const owner of [LOCAL_SERVICE, NETWORK_SERVICE]) {
      for (const role of ["ancestor", "creation-parent"] as const) {
        const flags = role === "ancestor" ? 0 : 0x2;
        for (const trustee of [USERS, owner]) {
          for (const mask of [0x40000000, MODIFY, FULL, 0x40, 0x40000, 0x80000]) {
            const refusal = roleRefusal("D:\\", role, owner, [...profileDacl(), allow(trustee, mask, flags)]);
            expect(refusal, `${owner} ${role} ${trustee} 0x${mask.toString(16)}`)
              .toMatch(new RegExp(`grants ${trustee} .*mask 0x${mask.toString(16)}`));
            expect(refusal).not.toContain(`is owned by ${owner}`);
          }
        }
      }
    }
    for (const drive of ["C", "D"]) {
      for (const owner of [LOCAL_SERVICE, NETWORK_SERVICE]) {
        expect(roleRefusal(`${drive}:\\`, "ancestor", owner,
          [...profileDacl(), allow(USERS, ADD_SUBDIR, 0x2)])).toBeUndefined();
        expect(roleRefusal(`${drive}:\\`, "creation-parent", owner,
          [...profileDacl(), allow(USERS, ADD_SUBDIR, 0x2)])).toMatch(/inheritable .*mask 0x4/);
      }
    }
  });

  it("reports an unsafe DACL with accepted service-owner context, not a false owner refusal", () => {
    const refusal = roleRefusal("C:\\Users\\me\\.pi", "ancestor", LOCAL_SERVICE, [
      ...profileDacl(), allow(USERS, 0x40000, 0), allow(OTHER, 0x80000, 0),
    ]);
    expect(refusal).toMatch(/owner(?: SID)?\s*[:=]?\s*S-1-5-19/i);
    expect(refusal).not.toMatch(/is owned by S-1-5-19|not the current user/i);
    expect(refusal).toContain(USERS);
    expect(refusal).toContain("mask 0x40000");
    expect(refusal?.match(new RegExp(`${USERS}[^\\n]*?mask 0x40000`, "g")) ?? []).toHaveLength(1);
    expect(refusal).toContain(OTHER);
    expect(refusal).toContain("mask 0x80000");
    expect(refusal?.match(new RegExp(`${OTHER}[^\\n]*?mask 0x80000`, "g")) ?? []).toHaveLength(1);
  });

  it("reports the first failed ancestor owner and every unsafe ACE occurrence independent of order", () => {
    const deny = { ...allow(USERS, FULL, 0), type: 1 };
    const creator = allow(CREATOR_OWNER, FULL, 0xb);
    const repeated = allow(USERS, 0x40000, 0);
    const unsafe = [repeated, allow(OTHER, 0x80000, 0), allow(USERS, 0x10000, 0), repeated];
    for (const aces of [[deny, creator, ...unsafe], [...unsafe].reverse().concat([creator, deny])]) {
      const entries = defaultEntries();
      entries[1] = dir("C:\\Users\\me\\.pi", AUTH_USERS, [...profileDacl(), ...aces]);
      entries[2] = dir("C:\\Users\\me", USER, [...profileDacl(), allow(EVERYONE, 0x40000000, 0)]);
      const refusal = windowsStateAclRefusal(request, runner(entries).run);
      expect(refusal).toMatch(new RegExp(`owner(?: SID)?\\s*[:=]?\\s*${AUTH_USERS}`, "i"));
      expect(refusal).toContain(USERS);
      expect(refusal).toContain(OTHER);
      expect(refusal?.match(new RegExp(`${USERS}[^\\n]*?mask 0x40000`, "g")) ?? []).toHaveLength(2);
      expect(refusal?.match(new RegExp(`${OTHER}[^\\n]*?mask 0x80000`, "g")) ?? []).toHaveLength(1);
      expect(refusal?.match(new RegExp(`${USERS}[^\\n]*?mask 0x10000`, "g")) ?? []).toHaveLength(1);
      expect(refusal?.match(/mask 0x40000/g) ?? []).toHaveLength(2);
      expect(refusal?.match(/mask 0x80000/g) ?? []).toHaveLength(1);
      expect(refusal).not.toContain(EVERYONE);
      expect(refusal).not.toContain("mask 0x40000000");
    }
  });

  it("continues past a malformed ACE to report a later unsafe ACE", () => {
    const malformed: Ace = { type: 9, flags: 0, sid: OTHER, mask: FULL };
    const refusal = roleRefusal("C:\\Users\\me\\.pi", "ancestor", NETWORK_SERVICE,
      [...profileDacl(), malformed, allow(USERS, 0x40000, 0)]);
    expect(refusal).toMatch(/unsupported or unproven ACE/i);
    expect(refusal).toContain(NETWORK_SERVICE);
    expect(refusal).not.toMatch(/is owned by S-1-5-20|not the current user/i);
    expect(refusal).toContain(USERS);
    expect(refusal).toContain("mask 0x40000");
    expect(refusal?.match(new RegExp(`${USERS}[^\\n]*?mask 0x40000`, "g")) ?? []).toHaveLength(1);
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

  // Decision C: recorded native snapshot policy, not Windows/NTFS proof. Absence is not unreadable evidence.
  it.each(["missing", "unreadable"] as const)("distinguishes a %s DB descriptor from an initially absent DB allowance", kind => {
    const entries = withEntry(6, kind === "missing" ? { missing: true } : { error: "recorded Access is denied" });
    const result = judge(entries);
    if (kind === "missing") expect(result).toBeUndefined();
    else expect(result).toMatch(/state\.db has an unreadable Windows security descriptor/);
    entries[0] = dir(request.root, USER, [...profileDacl(), allow(OTHER, FULL, 0)]);
    expect(judge(entries)).toMatch(/mesh grants S-1-5-21-1-2-3-1002/);
  });

  it("a missing root descriptor is not an optional missing DB or an unreadable-identity fallback", () => {
    expect(judge(withEntry(0, { missing: true }))).toMatch(/mesh vanished while its Windows ACL was read/);
  });

  it("allows an absent state.db-journal sidecar", () => {
    const journal = `${request.root}\\state.db-journal`;
    expect(judge([...defaultEntries(), { path: journal, missing: true }], { files: [...request.files, journal] })).toBeUndefined();
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

  // F2 trust boundary: either pipe can exceed execFileSync's maxBuffer. Node 24's observed spawnSync shape is
  // ENOBUFS/errno -105/status null/SIGTERM; do not mistake that incomplete proof for a missing executable.
  it.each(["ENOBUFS", "ERR_CHILD_PROCESS_STDIO_MAXBUFFER"])("refuses %s from stdout or stderr overflow without warning", code => {
    const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
    for (const stream of ["stdout", "stderr"]) {
      const fail = () => { throw Object.assign(new Error(`${stream} maxBuffer exceeded`),
        { code, errno: -105, status: null, signal: "SIGTERM", [stream]: "truncated output" }); };
      expect(windowsStateAclRefusal(request, fail)).toMatch(/incomplete Windows ACL snapshot .*buffer limit/);
    }
    expect(warn).not.toHaveBeenCalled();
  });

  it.each([
    ["ENOENT", { code: "ENOENT" }],
    ["nonzero exit", { status: 1 }],
    ["timeout", { code: "ETIMEDOUT", status: null, signal: "SIGTERM" }],
    ["ENOBUFS", { code: "ENOBUFS", errno: -105, status: null, signal: "SIGTERM" }],
    ["ERR_CHILD_PROCESS_STDIO_MAXBUFFER", { code: "ERR_CHILD_PROCESS_STDIO_MAXBUFFER" }],
  ] as const)("refuses a new-root setter runner failure (%s), never warning or accepting an unprotected root", (kind, fields) => {
    const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
    const fail = () => { throw Object.assign(new Error(`native setter ${kind}`), fields); };
    expect(windowsStateAclRefusal({ ...request, protect: true }, fail)).toMatch(/was created but its private ACL could not be set/);
    expect(warn).not.toHaveBeenCalled();
  });

  it("warns once and opens as before for existing-root read ENOENT, nonzero exit and timeout", () => {
    const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => undefined);
    const fail = () => { throw Object.assign(new Error("spawn powershell.exe ENOENT"), { code: "ENOENT" }); };
    expect(windowsStateAclRefusal(request, fail)).toBeUndefined();
    expect(windowsStateAclRefusal(request, fail)).toBeUndefined();
    for (const fields of [{ status: 1 }, { code: "ETIMEDOUT", status: null, signal: "SIGTERM" }]) {
      expect(windowsStateAclRefusal(request, () => { throw Object.assign(new Error("native read failed"), fields); })).toBeUndefined();
    }
    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0]![0])).toMatch(/could not read Windows ACLs \(spawn powershell\.exe ENOENT\)/);
  });
});

describe("state gate wiring on win32 (smarty-dev#8920)", () => {
  afterEach(() => { vi.restoreAllMocks(); vi.doUnmock("../src/mesh/windows-state-acl.js"); vi.resetModules(); });

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
      expect(calls.map(call => call.protect)).toEqual([false, true]);
      expect(calls[0]).toMatchObject({ root: path.dirname(root), role: "creation-parent", files: [], directories: [] });
      expect(calls[1]!.files.map(file => path.basename(file))).toEqual(["state.db", "state.db-wal", "state.db-shm", "state.db-journal"]);
      expect(calls[1]!.directories.map(dir => path.basename(dir))).toEqual(["state-checkpoint.flags"]);
    });
  });

  // Decision C: this wiring seam deliberately does not cache. The real reader's identity-only process cache is
  // covered by mesh-state-windows-acl-cache.test.ts; here each gate call must still delegate its complete request.
  it("delegates successive root checks to the proof layer and propagates its refusal", async () => {
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

  // The read-only scan runs on every writer-lock attempt, so it must never spawn the native reader; the root's
  // admission proof covers the flag directory. Its lstat checks stay on every call.
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

  it.each(["file", "sqlite"] as const)("admits an existing state.db before the %s backend's first operation", async stateBackend => {
    const { calls } = await loadGate(undefined);
    const { createStateBackend } = await import("../src/mesh/state-backend.js");
    const root = tmp();
    fs.writeFileSync(path.join(root, "state.db"), "");
    await asWindows(async () => {
      const backend = createStateBackend({ root, maxEventBytes: 1024, maxReadEvents: 16, lock: {} as never }, { stateBackend });
      try {
        expect(backend.kind).toBe(stateBackend);
        expect(calls).toHaveLength(1); // before a read/write can start its busy/try budget
        expect(calls[0]).toMatchObject({ root, protect: false });
        expect(calls[0]!.files.map(file => path.basename(file))).toEqual(["state.db", "state.db-wal", "state.db-shm", "state.db-journal"]);
      } finally { backend.close(); }
    });
  });

  // Decision C wiring: only positively observed absence may skip the full proof, not an indeterminate lstat.
  it("admitExistingStateRoot uses full native proof for an unreadable DB lstat, preserving safe/refused verdicts", async () => {
    const why = "state.db has an unreadable Windows security descriptor (recorded descriptor denial)";
    const { gate, calls } = await loadGate(undefined, [undefined, why]);
    const root = tmp();
    const file = path.join(root, "state.db");
    const actualLstat = fs.lstatSync.bind(fs);
    vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
      if (String(target) === file) throw Object.assign(new Error("recorded DB identity lstat denied"), { code: "EACCES" });
      return actualLstat(target, options);
    }) as typeof fs.lstatSync);
    await asWindows(async () => {
      expect(() => gate.admitExistingStateRoot(root)).not.toThrow();
      expect(calls).toHaveLength(1);
      expect(calls[0]).toMatchObject({ root, protect: false, files: [file, `${file}-wal`, `${file}-shm`, `${file}-journal`] });
      expect(() => gate.admitExistingStateRoot(root)).toThrow(why);
      expect(calls).toHaveLength(2);
    });
  });

  it("keeps backend admission child-free when state.db is absent", async () => {
    const { calls } = await loadGate(undefined);
    const { createStateBackend } = await import("../src/mesh/state-backend.js");
    const root = tmp();
    await asWindows(async () => {
      const backend = createStateBackend({ root, maxEventBytes: 1024, maxReadEvents: 16, lock: {} as never }, { stateBackend: "file" });
      backend.close();
    });
    expect(calls).toEqual([]);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  });

  // A stand-in connection: openPinned off Linux only re-lstats the checked state.db after open().
  const opener = (opened: string[]) => (file: string) => { opened.push(file); return { close: () => undefined } as never; };

  it("delegates the complete root, state files and flag directory plan on every open", async () => {
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
    expect(calls).toHaveLength(2); // one delegation per open; this mock has no identity cache
    for (const call of calls) {
      expect(call).toMatchObject({ root, protect: false });
      expect(call.files).toEqual([file, `${file}-wal`, `${file}-shm`, `${file}-journal`]);
      expect(call.directories).toEqual([path.join(root, "state-checkpoint.flags")]);
    }
  });

  it("runs a Windows create inside the gate with a post-create native recheck; a refused root gets no state.db", async () => {
    const order: string[] = [];
    const { gate, calls } = await loadGate(undefined);
    const root = tmp();
    const file = path.join(root, "state.db");
    await asWindows(async () => {
      gate.openPrivateStateDb(file, (opened) => { order.push(`open ${calls.length}`); return opener([])(opened); },
        () => { order.push(`create ${calls.length}`); fs.writeFileSync(file, "", { flag: "wx" }); }).close();
    });
    expect(order).toEqual(["create 1", "open 2"]); // the missing-file create is followed by a fresh native read
    expect(calls).toHaveLength(2);
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

  it("refuses a database planted after the first snapshot, including an EEXIST create winner", async () => {
    const planted = "state.db was created after the ACL snapshot and has an untrusted owner";
    const { gate, calls } = await loadGate(undefined, [undefined, planted]);
    const root = tmp();
    const file = path.join(root, "state.db");
    const opened: string[] = [];
    await asWindows(async () => {
      expect(() => gate.openPrivateStateDb(file, opener(opened), () => {
        // The attacker wins the name before our wx attempt; the production callback ignores this EEXIST.
        fs.writeFileSync(file, "planted", { flag: "wx" });
        try { fs.closeSync(fs.openSync(file, "wx")); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      })).toThrow(planted);
    });
    expect(calls).toHaveLength(2);
    expect(opened).toEqual([]);
  });

  it("delegates admission and open through the complete root plan, sync and async (fake reader has no cache)", async () => {
    const { calls } = await loadGate(undefined);
    const { SqliteStateStore } = await import("../src/mesh/state-sqlite.js");
    const root = tmp();
    await asWindows(async () => {
      SqliteStateStore.openSync(root, 1024, 16, { initialize: "detached" }).close();
      expect(calls).toHaveLength(2);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
      (await SqliteStateStore.open(root, 1024, 16, { initialize: "detached" })).close();
      // This seam replaces the caching reader: existing-DB admission and the open each delegate once.
      expect(calls).toHaveLength(4);
    });
    expect(calls.every(call => call.root === root && call.protect === false && call.directories.length === 1)).toBe(true);
  });

  // F1 recorded creation counterexample: a missing root through a junction used to mutate the target before
  // refusing. Exercise both public entrypoints, including their BEFORE-fence create path, on a real link fixture.
  it.each(["create", "detached"] as const)("public sync/async opens refuse a missing root below a junction before any mutation (%s)", async initialize => {
    const { calls } = await loadGate(undefined);
    const { SqliteStateStore } = await import("../src/mesh/state-sqlite.js");
    const base = tmp();
    const target = path.join(base, "target");
    fs.mkdirSync(target);
    fs.writeFileSync(path.join(target, "sentinel"), "unchanged");
    const junction = path.join(base, "junction");
    fs.symlinkSync(target, junction, "junction");
    const root = path.join(junction, "new", "mesh");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    await asWindows(async () => {
      expect(() => SqliteStateStore.openSync(root, 1024, 16, { initialize }))
        .toThrow(expect.objectContaining({ code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringContaining(junction) }));
      await expect(SqliteStateStore.open(root, 1024, 16, { initialize })).rejects.toThrow(/path with no junctions or symbolic links/);
    });
    expect(mkdir).not.toHaveBeenCalled();
    expect(calls).toEqual([]); // no protect, hence no target ACL mutation
    expect(fs.readdirSync(target)).toEqual(["sentinel"]);
    expect(fs.readFileSync(path.join(target, "sentinel"), "utf8")).toBe("unchanged");
  });

  it("public new-root opens refuse a failed setter before the fence or state.db creation", async () => {
    const why = "new root was created but its private ACL could not be set (native ACL reader failed: ENOENT)";
    const { calls } = await loadGate(undefined, [undefined, why, undefined, why]);
    const { SqliteStateStore } = await import("../src/mesh/state-sqlite.js");
    const base = tmp();
    const sync = path.join(base, "sync");
    const asyncRoot = path.join(base, "async");
    const open = vi.fn();
    await asWindows(async () => {
      expect(() => SqliteStateStore.openSync(sync, 1024, 16, { initialize: "create", open }))
        .toThrow(expect.objectContaining({ code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringContaining("private ACL could not be set") }));
      await expect(SqliteStateStore.open(asyncRoot, 1024, 16, { initialize: "create", open })).rejects.toThrow(/private ACL could not be set/);
    });
    expect(calls.map(call => [call.root, call.protect])).toEqual([[base, false], [sync, true], [base, false], [asyncRoot, true]]);
    expect(open).not.toHaveBeenCalled();
    expect(fs.readdirSync(sync)).toEqual([]); // no custody.lock/.lock or state.db
    expect(fs.readdirSync(asyncRoot)).toEqual([]);
  });

  it.each(["is a reparse point", "grants S-1-1-0 write"])("refuses a native ancestor preflight that %s before any mkdir or protect", async why => {
    const { gate, calls } = await loadGate(why);
    const base = tmp();
    const root = path.join(base, "new", "mesh");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    await asWindows(async () => { expect(() => gate.createStateRoot(root)).toThrow(why); });
    expect(mkdir).not.toHaveBeenCalled();
    expect(calls).toEqual([expect.objectContaining({ root: base, role: "creation-parent", protect: false, files: [], directories: [] })]);
    expect(fs.readdirSync(base)).toEqual([]);
  });

  it("creates a missing ordinary parent chain non-recursively, checking each new parent before using it", async () => {
    const { gate, calls } = await loadGate(undefined);
    const base = tmp();
    const parent = path.join(base, "new");
    const root = path.join(parent, "mesh");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    await asWindows(async () => { gate.createStateRoot(root); });
    expect(mkdir.mock.calls).toEqual([[parent, { mode: 0o700 }], [root, { mode: 0o700 }]]);
    expect(calls.map(call => [call.root, call.role, call.protect])).toEqual([
      [base, "creation-parent", false], [parent, "creation-parent", false], [root, undefined, true],
    ]);
    expect(fs.lstatSync(root).isDirectory()).toBe(true);
  });

  it("refuses a newly created parent whose inherited ACL is unsafe before creating its child", async () => {
    const { gate, calls } = await loadGate(undefined, [undefined, "new parent grants S-1-1-0 write"]);
    const base = tmp();
    const root = path.join(base, "new", "mesh");
    await asWindows(async () => { expect(() => gate.createStateRoot(root)).toThrow(/new parent grants/); });
    expect(fs.readdirSync(path.dirname(root))).toEqual([]);
    expect(calls.map(call => call.protect)).toEqual([false, false]);
  });

  it("rechecks a created parent before continuing if it is replaced by a junction", async () => {
    const { gate, calls } = await loadGate(undefined);
    const base = tmp();
    const target = path.join(base, "target");
    fs.mkdirSync(target);
    const parent = path.join(base, "new");
    const root = path.join(parent, "mesh");
    const mkdir = fs.mkdirSync.bind(fs);
    const made: fs.PathLike[] = [];
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
      made.push(file);
      const result = mkdir(file, options);
      if (file === parent) {
        fs.renameSync(parent, path.join(base, "moved-parent"));
        fs.symlinkSync(target, parent, "junction");
      }
      return result;
    }) as typeof fs.mkdirSync);
    await asWindows(async () => { expect(() => gate.createStateRoot(root)).toThrow(/path with no junctions/); });
    expect(made).toEqual([parent]);
    expect(calls.map(call => call.protect)).toEqual([false]);
    expect(fs.readdirSync(target)).toEqual([]);
  });

  // EEXIST at either boundary is someone else's directory, never our permission to protect, repair or adopt it.
  it.each(["parent", "root"])("never adopts or protects an EEXIST winner at the %s boundary", async winner => {
    const { gate, calls } = await loadGate(undefined);
    const root = path.join(tmp(), "missing-parent", "mesh");
    const parent = path.dirname(root);
    const mkdir = fs.mkdirSync.bind(fs);
    const made: unknown[][] = [];
    vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
      made.push([target, options?.recursive ?? false]);
      if (target === (winner === "root" ? root : parent)) mkdir(target); // another account wins
      return mkdir(target, options);
    }) as typeof fs.mkdirSync);
    await asWindows(async () => { expect(() => gate.createStateRoot(root)).toThrow(/appeared during creation; it was not adopted/); });
    expect(made).toEqual(winner === "root" ? [[parent, false], [root, false]] : [[parent, false]]);
    expect(calls.map(call => call.protect)).toEqual(winner === "root" ? [false, false] : [false]);
    expect(fs.readdirSync(winner === "root" ? root : parent)).toEqual([]);
  });
});
