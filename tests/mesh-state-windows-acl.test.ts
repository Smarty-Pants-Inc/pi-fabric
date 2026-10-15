import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { openPrivateStateDb } from "../src/mesh/state-gate.js";
import { openNodeSqlite, raiseCheckpointFlag, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { aclOwner, icaclsGrant, icaclsShow, readAcl, setOwner, tokenSids } from "./helpers/windows-acl.js";

// smarty-dev#8920: Windows ACL privacy of the SQLite mesh state root, its state files, its flag directory and its
// ancestors, proved on real NTFS ACLs (icacls grants, Get-Acl/icacls read-back) with the real node:sqlite opener.
// No mocks. Hosted-only: fixtures live in a private directory below the runner's DEFAULT USERPROFILE (not TMPDIR,
// whose RUNNER_TEMP ancestors are not a profile), so local runs never create or delete them. The profile and its
// system ancestors are never modified; every grant is on a fixture directory created by this suite.
//
// Policy (fabric-store ruling): root, state.db/-wal/-shm and the flag directory refuse any foreign
// write/modify/full/delete/WRITE_DAC/WRITE_OWNER ACE; owner must be the current user, or Administrators only when
// the token's default owner is Administrators (elevated). Trusted: the current user, SYSTEM, Administrators, TrustedInstaller. Ancestors refuse foreign DELETE/WRITE_DAC/WRITE_OWNER/DELETE_CHILD/
// write/modify/full; add-file/add-subdirectory-only and CREATOR OWNER-only inheritance are allowed (sticky analogue).
const EVERYONE = "S-1-1-0";
const SYSTEM = "S-1-5-18";
const ADMINISTRATORS = "S-1-5-32-544";
const TRUSTED_INSTALLER = "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464";
const FULL = 0x1f01ff;
const MODIFY = 0x301bf;
const EVERYONE_READ = 0x120089;
const DATA_READ_BITS = 0x90000001;
const UNSUPPORTED = { code: "FABRIC_MESH_STATE_UNSUPPORTED" };

const created: string[] = [];
let fixtureUser: string | undefined;
// An elevated runner may default to Administrators as owner. Make each existing fixture explicitly ours so a
// writer refusal cannot accidentally pass for the wrong reason (foreign ownership). The product must own its own
// newly created root/files: the new-root positive control below does not repair those.
const ownFixture = (file: string): void => {
  fixtureUser ??= tokenSids().user;
  if (aclOwner(file).sid === fixtureUser) return;
  const changed = setOwner(file, fixtureUser);
  expect(changed.changed, changed.trace.join(" | ")).toBe(true);
};
const ownedDirectory = (directory: string): void => { fs.mkdirSync(directory); ownFixture(directory); };
afterEach(() => { for (const dir of created.splice(0).reverse()) fs.rmSync(dir, { recursive: true, force: true }); });
/** A fresh private fixture directory directly below the default profile (inherits only the profile ACL). */
const profileDir = (): string => {
  const profile = process.env.USERPROFILE;
  if (!profile || path.resolve(os.homedir()) !== path.resolve(profile)) throw new Error(`USERPROFILE ${String(profile)} is not the home directory ${os.homedir()}`);
  const dir = fs.mkdtempSync(path.join(profile, "fabric-mesh-acl-"));
  created.push(dir);
  ownFixture(dir);
  return dir;
};
const openAsync = (root: string) => SqliteStateStore.open(root, 65_536, 1_000, { initialize: "detached" });
const openSync = (root: string) => SqliteStateStore.openSync(root, 65_536, 1_000, { initialize: "detached" });
const opens = { open: openAsync, openSync } as const;
/** The refusal an open throws, or undefined when it opened (that store is closed at once, so cleanup can delete). */
const refusal = async (open: () => SqliteStateStore | Promise<SqliteStateStore>): Promise<unknown> => {
  try { (await open()).close(); return undefined; } catch (error) { return error; }
};
/** One real write and read through node:sqlite, then close. */
const roundTrip = async (store: SqliteStateStore): Promise<void> => {
  try {
    await store.put({ key: "acl/probe", value: "ok", identity: { id: "test", name: "test", kind: "agent" } });
    expect(store.get("acl/probe")?.value).toBe("ok");
  } finally { store.close(); }
};
/** A genuine grant, confirmed by Get-Acl (explicit rule for `sid` with every `bits`) before any assertion relies on it. */
const grant = (file: string, sid: string, rights: string, bits: number): void => {
  icaclsGrant(file, `*${sid}:${rights}`);
  const rules = readAcl(file).rules.filter(rule => rule.sid === sid && !rule.inherited && rule.type === "Allow");
  expect(rules.some(rule => (rule.rights & bits) === bits), `Get-Acl ${file}: ${JSON.stringify(rules)}`).toBe(true);
};

describe.runIf(process.env.GITHUB_ACTIONS === "true" && process.platform === "win32")("Windows mesh state ACL privacy (smarty-dev#8920)", () => {
  it.each(Object.keys(opens) as (keyof typeof opens)[])("%s refuses a root with a genuine Everyone:(M) grant before creating state.db", async name => {
    const root = profileDir();
    grant(root, EVERYONE, "(OI)(CI)(M)", MODIFY);
    expect(icaclsShow(root)).toMatch(/Everyone:\(OI\)\(CI\)\(M\)/);
    expect(await refusal(() => opens[name](root))).toMatchObject(UNSUPPORTED);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  }, 120_000);

  it.each([["write", "(W)", 0x2], ["modify", "(M)", MODIFY], ["full control", "(F)", FULL], ["delete", "(DE)", 0x10000],
    ["change permissions", "(WDAC)", 0x40000], ["take ownership", "(WO)", 0x80000]] as const)(
    "refuses a root where a foreign SID has %s", async (_name, rights, bits) => {
      const root = profileDir();
      grant(root, "S-1-5-32-545", rights, bits);
      expect(await refusal(() => openAsync(root))).toMatchObject(UNSUPPORTED);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    }, 120_000);

  it("refuses a real state.db with a verified Everyone (R) data-read grant for async and sync SQLite opens", async () => {
    const root = profileDir();
    await roundTrip(await openAsync(root));
    const file = path.join(root, "state.db");
    grant(file, EVERYONE, "(R)", EVERYONE_READ);
    const everyone = readAcl(file).rules.filter(rule => rule.sid === EVERYONE && !rule.inherited && rule.type === "Allow");
    expect(everyone.some(rule => (rule.rights & EVERYONE_READ) === EVERYONE_READ), readAcl(file).sddl).toBe(true);
    expect(icaclsShow(file)).toMatch(/Everyone:\(R\)/);
    expect(await refusal(() => openAsync(root))).toMatchObject(UNSUPPORTED);
    expect(await refusal(() => openSync(root))).toMatchObject(UNSUPPORTED);
  }, 120_000);

  it.each(["state.db-wal", "state.db-shm", "state.db-journal"])("refuses an existing %s with a verified Everyone (R) data-read grant", async file => {
    const root = profileDir();
    await roundTrip(await openAsync(root));
    const target = path.join(root, file);
    if (!fs.existsSync(target)) fs.writeFileSync(target, "");
    grant(target, EVERYONE, "(R)", EVERYONE_READ);
    expect(icaclsShow(target)).toMatch(/Everyone:\(R\)/);
    expect(await refusal(() => openAsync(root))).toMatchObject(UNSUPPORTED);
    expect(await refusal(() => openSync(root))).toMatchObject(UNSUPPORTED);
  }, 120_000);

  it("opens real node:sqlite in a clean root below the default USERPROFILE without touching its ancestors", async () => {
    const profile = os.homedir();
    const before = readAcl(profile).sddl;
    const root = profileDir();
    await roundTrip(await openAsync(root));
    await roundTrip(openSync(root));
    // A new root below the profile opens too.
    await roundTrip(await openAsync(path.join(profileDir(), "new-root")));
    expect(readAcl(profile).sddl).toBe(before);
  }, 120_000);

  it("uses the production default mesh root for SQLite below a hosted USERPROFILE project", async () => {
    const projectRoot = profileDir();
    const expectedRoot = path.join(projectRoot, ".pi", "fabric", "mesh");
    const envKeys = ["PI_FABRIC_MESH_ROOT", "PI_FABRIC_PROJECT_ROOT", "PI_FABRIC_MESH_STATE_BACKEND", "PI_CODING_AGENT_DIR"] as const;
    const previousEnv = new Map(envKeys.map(key => [key, process.env[key]] as const));
    const restoreEnv = (): void => {
      for (const [key, value] of previousEnv) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    };
    let runtime: FabricRuntimeState | undefined;
    let resolvedPath: string | undefined;
    let outcome = "not started";
    let failure: string | undefined;
    const aclChain = (root: string) => {
      const entries: { path: string; exists: boolean; acl?: ReturnType<typeof readAcl>; error?: string }[] = [];
      let current = path.resolve(root);
      for (;;) {
        const exists = fs.existsSync(current);
        if (!exists) entries.push({ path: current, exists });
        else {
          try { entries.push({ path: current, exists, acl: readAcl(current) }); }
          catch (error) { entries.push({ path: current, exists, error: error instanceof Error ? error.message : String(error) }); }
        }
        const parent = path.dirname(current);
        if (parent === current) break;
        current = parent;
      }
      return entries;
    };

    try {
      delete process.env.PI_FABRIC_MESH_ROOT;
      delete process.env.PI_FABRIC_PROJECT_ROOT;
      process.env.PI_FABRIC_MESH_STATE_BACKEND = "sqlite";
      process.env.PI_CODING_AGENT_DIR = path.join(projectRoot, ".pi", "coding-agent");
      const config = normalizeFabricConfig({
        mesh: { enabled: true, stateBackend: "sqlite" },
        mcp: { enabled: false, cache: { enabled: false } },
        agents: { enabled: false }, residency: { enabled: false },
        prewalk: { enabled: false, alwaysRearm: false },
      });
      expect(config.mesh.root).toBeUndefined();
      const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn() } as unknown as ExtensionAPI;
      const context = {
        cwd: projectRoot, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
        modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn() },
        sessionManager: { getSessionId: () => "mesh-default-root-acl", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
        ui: { setStatus: vi.fn(), notify: vi.fn() },
      } as unknown as ExtensionContext;
      const unused = path.join(projectRoot, "unused.mjs");
      fs.writeFileSync(unused, "export default {}; ");
      runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: unused, worker: unused, residentHost: unused, skills: projectRoot } });
      await runtime.initialize(context, config);
      const mesh = runtime.mesh;
      resolvedPath = mesh.root;
      expect(path.resolve(mesh.root)).toBe(path.resolve(expectedRoot));
      expect(mesh.stateBackend).toBe("sqlite");
      await mesh.put({ key: "acl/default-root-probe", value: "sqlite-default-root", identity: { id: "mesh-default-root-acl", name: "probe", kind: "agent" } });
      expect(mesh.get("acl/default-root-probe")?.value).toBe("sqlite-default-root");
      outcome = "passed";
    } catch (error) {
      outcome = "failed";
      failure = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      let actualPath = resolvedPath;
      if (!actualPath && runtime) {
        try { actualPath = runtime.mesh.root; } catch { /* initialization may have failed before mesh construction */ }
      }
      const rootsToReport = [...new Set([actualPath ?? expectedRoot, expectedRoot])];
      try {
        console.log(JSON.stringify({
          event: "mesh-default-root-acl",
          outcome,
          ...(failure ? { failure } : {}),
          projectRoot,
          expectedPath: expectedRoot,
          resolvedPath: actualPath ?? null,
          roots: rootsToReport.map(root => ({ path: root, ancestors: aclChain(root) })),
        }));
      } finally {
        try { await runtime?.shutdown(); }
        finally { restoreEnv(); }
      }
    }
  }, 120_000);

  it.each(Object.keys(opens) as (keyof typeof opens)[])("%s gives a new root an explicit protected ACL: current user and SYSTEM full control, inheritable", async name => {
    const { user } = tokenSids();
    const root = path.join(profileDir(), "nested", "new-root");
    expect(fs.existsSync(path.dirname(root))).toBe(false);
    expect(fs.existsSync(root)).toBe(false);
    await roundTrip(await opens[name](root));
    const acl = readAcl(root);
    expect(acl.owner).toBe(user);
    expect(acl.protected, acl.sddl).toBe(true);
    expect(acl.rules.filter(rule => rule.inherited), acl.sddl).toEqual([]);
    for (const sid of [user, SYSTEM]) {
      expect(acl.rules.some(rule => rule.sid === sid && rule.type === "Allow" && (rule.rights & FULL) === FULL
        && /ContainerInherit/.test(rule.inheritance) && /ObjectInherit/.test(rule.inheritance) && rule.propagation === "None"),
      `${sid} in ${acl.sddl}`).toBe(true);
    }
    const trusted = new Set([user, SYSTEM, ADMINISTRATORS, TRUSTED_INSTALLER]);
    expect(acl.rules.filter(rule => !trusted.has(rule.sid)), acl.sddl).toEqual([]);
    const shown = icaclsShow(root);
    expect(shown).not.toMatch(/\(I\)/);
    expect(shown).toMatch(/NT AUTHORITY\\SYSTEM:\(OI\)\(CI\)\(F\)/);
    expect(shown.toLowerCase()).toContain(`\\${os.userInfo().username.toLowerCase()}:(oi)(ci)(f)`);
  }, 120_000);

  it.each(["async", "sync"] as const)("refuses a missing root below an immediate parent with foreign inherit-only full control (%s) before creation", async mode => {
    const parent = profileDir();
    const root = path.join(parent, "new-root");
    grant(parent, EVERYONE, "(OI)(CI)(IO)(F)", FULL);
    const rule = readAcl(parent).rules.find(entry => entry.sid === EVERYONE && !entry.inherited && entry.type === "Allow"
      && (entry.rights & FULL) === FULL);
    expect(rule, readAcl(parent).sddl).toBeDefined();
    expect(rule?.inheritance).toContain("ContainerInherit");
    expect(rule?.inheritance).toContain("ObjectInherit");
    expect(rule?.propagation).toContain("InheritOnly");
    expect(icaclsShow(parent)).toMatch(/Everyone:\(OI\)\(CI\)\(IO\)\(F\)/);
    const children = fs.readdirSync(parent);
    expect(fs.existsSync(root)).toBe(false);

    const open = () => mode === "async"
      ? SqliteStateStore.open(root, 65_536, 1_000, { initialize: "create" })
      : SqliteStateStore.openSync(root, 65_536, 1_000, { initialize: "create" });
    const error = await refusal(open);
    expect(error).toMatchObject({ ...UNSUPPORTED, message: expect.stringContaining(`refuses ${parent}:`) });
    expect(fs.existsSync(root)).toBe(false);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    expect(fs.readdirSync(parent)).toEqual(children);
  }, 120_000);

  it("refuses a state.db planted with a genuine Everyone (M) ACL after the native snapshot, before SQLite opens it", () => {
    const root = profileDir();
    const file = path.join(root, "state.db");
    const seed = path.join(root, "controlled-seed.db");
    let openerCalls = 0;
    let connection: ReturnType<typeof openNodeSqlite> | undefined;
    let error: unknown;
    const create = () => {
      // Create a valid SQLite seed only after openPrivateStateDb has taken its real native ACL snapshot.
      openNodeSqlite(seed).close();
      grant(seed, EVERYONE, "(M)", MODIFY);
      fs.renameSync(seed, file);
      // Match the public O_EXCL create callback: an attacker winning with EEXIST is not itself fatal.
      try { fs.closeSync(fs.openSync(file, "wx", 0o600)); }
      catch (caught) { if ((caught as NodeJS.ErrnoException).code !== "EEXIST") throw caught; }
    };

    try {
      connection = openPrivateStateDb(file, target => {
        openerCalls += 1;
        return openNodeSqlite(target);
      }, create);
    } catch (caught) { error = caught; }
    finally { connection?.close(); }

    expect(error).toMatchObject(UNSUPPORTED);
    expect(openerCalls).toBe(0);
    expect(fs.existsSync(seed)).toBe(false);
    expect(fs.existsSync(file)).toBe(true);
    const everyone = readAcl(file).rules.filter(rule => rule.sid === EVERYONE && !rule.inherited && rule.type === "Allow");
    expect(everyone.some(rule => (rule.rights & MODIFY) === MODIFY), readAcl(file).sddl).toBe(true);
  }, 120_000);

  it("allows SQLite-created WAL sidecars inheriting the protected root ACL without foreign data-read", async () => {
    const { user } = tokenSids();
    const trusted = new Set([user, SYSTEM, ADMINISTRATORS, TRUSTED_INSTALLER]);
    const root = path.join(profileDir(), "new-root");
    const store = await openAsync(root);
    try {
      await store.put({ key: "acl/probe", value: "ok", identity: { id: "test", name: "test", kind: "agent" } });
      for (const suffix of ["-wal", "-shm"]) {
        const file = path.join(root, `state.db${suffix}`);
        expect(fs.existsSync(file), file).toBe(true);
        const acl = readAcl(file);
        expect(acl.rules.some(rule => rule.inherited), acl.sddl).toBe(true);
        const foreignDataReaders = acl.rules.filter(rule => rule.type === "Allow" && !trusted.has(rule.sid)
          && (rule.rights & DATA_READ_BITS) !== 0);
        expect(foreignDataReaders, acl.sddl).toEqual([]);
      }
    } finally { store.close(); }
  }, 120_000);

  it("allows a clean state.db created by the gate callback with real node:sqlite", () => {
    const root = profileDir();
    const file = path.join(root, "state.db");
    let creates = 0;
    const db = openPrivateStateDb(file, openNodeSqlite, () => {
      fs.closeSync(fs.openSync(file, "wx", 0o600));
      creates += 1;
    });
    try {
      expect(creates).toBe(1);
      expect(fs.existsSync(file)).toBe(true);
      expect(db.prepare("SELECT 1 AS value").get()?.value).toBe(1);
      expect(readAcl(file).rules.filter(rule => rule.sid === EVERYONE && !rule.inherited && rule.type === "Allow"
        && (rule.rights & MODIFY) === MODIFY)).toEqual([]);
    } finally { db.close(); }
  }, 120_000);

  it.each([["delete child", "(DC)", 0x40], ["delete", "(DE)", 0x10000], ["change permissions", "(WDAC)", 0x40000],
    ["take ownership", "(WO)", 0x80000], ["write", "(W)", 0x2], ["modify", "(M)", MODIFY], ["full control", "(F)", FULL]] as const)(
    "refuses a private root whose private ancestor grants a foreign SID %s", async (_name, rights, bits) => {
      const ancestor = profileDir();
      const root = path.join(ancestor, "root");
      ownedDirectory(root);
      // No inheritance flags: the right applies to the ancestor alone; the root itself stays private.
      grant(ancestor, EVERYONE, rights, bits);
      expect(readAcl(root).rules.filter(rule => rule.sid === EVERYONE)).toEqual([]);
      expect(await refusal(() => openAsync(root))).toMatchObject(UNSUPPORTED);
      expect(await refusal(() => openSync(root))).toMatchObject(UNSUPPORTED);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    }, 120_000);

  it("allows an ancestor with add-file/add-subdirectory-only and CREATOR OWNER-only inheritance (like C:\\Users)", async () => {
    const ancestor = profileDir();
    const root = path.join(ancestor, "root");
    ownedDirectory(root);
    grant(ancestor, EVERYONE, "(WD,AD)", 0x6);
    icaclsGrant(ancestor, "*S-1-3-0:(OI)(CI)(IO)(F)");
    expect(readAcl(ancestor).rules.some(rule => rule.sid === "S-1-3-0" && rule.propagation.includes("InheritOnly"))).toBe(true);
    await roundTrip(await openAsync(root));
  }, 120_000);

  it.each(["state.db", "state.db-wal", "state.db-shm"])("refuses an existing %s that a foreign SID may modify", async file => {
    const root = profileDir();
    await roundTrip(await openAsync(root));
    const target = path.join(root, file);
    if (!fs.existsSync(target)) fs.writeFileSync(target, "");
    grant(target, EVERYONE, "(M)", MODIFY);
    expect(await refusal(() => openAsync(root))).toMatchObject(UNSUPPORTED);
    expect(await refusal(() => openSync(root))).toMatchObject(UNSUPPORTED);
  }, 120_000);

  it("refuses to raise a checkpoint flag in a flag directory that a foreign SID may modify", () => {
    const root = profileDir();
    const flags = path.join(root, "state-checkpoint.flags");
    ownedDirectory(flags);
    grant(flags, EVERYONE, "(OI)(CI)(M)", MODIFY);
    let raised: unknown;
    try { raised = raiseCheckpointFlag(root, "acl-probe"); } catch (error) { raised = error; }
    expect(raised).toMatchObject(UNSUPPORTED);
    expect(fs.readdirSync(flags)).toEqual([]);
  }, 120_000);

  it("opens an existing Administrators-owned root only with the elevated Administrators default owner", async () => {
    const { owner } = tokenSids();
    const root = profileDir();
    const changed = setOwner(root, ADMINISTRATORS);
    expect(changed.changed, changed.trace.join(" | ")).toBe(true);
    expect(readAcl(root).owner).toBe(ADMINISTRATORS);
    if (owner === ADMINISTRATORS) {
      await roundTrip(await openAsync(root));
      await roundTrip(openSync(root));
    } else {
      expect(await refusal(() => openAsync(root))).toMatchObject(UNSUPPORTED);
      expect(await refusal(() => openSync(root))).toMatchObject(UNSUPPORTED);
    }
  }, 120_000);

  it("refuses a pre-squatted root owned by a foreign SID (DACL unchanged, only the owner differs)", async () => {
    const { user, owner } = tokenSids();
    const foreign = ["S-1-5-32-545", SYSTEM, ADMINISTRATORS].find(sid => sid !== user && sid !== owner);
    expect(foreign, `token user ${user}, owner ${owner}`).toBeDefined();
    const root = path.join(profileDir(), "squat");
    ownedDirectory(root);
    const original = aclOwner(root);
    expect(original.sid, original.trace).toBeDefined();
    const set = setOwner(root, foreign!);
    try {
      expect(set.changed, set.trace.join(" | ")).toBe(true);
      expect(await refusal(() => openAsync(root))).toMatchObject(UNSUPPORTED);
      expect(await refusal(() => openSync(root))).toMatchObject(UNSUPPORTED);
      expect(fs.readdirSync(root)).toEqual([]);
    } finally {
      if (aclOwner(root).sid !== original.sid) {
        const back = setOwner(root, original.sid!);
        if (!back.changed) console.warn(`[foreign-owner fixture] owner not reverted: ${back.trace.join(" | ")}`);
      }
    }
  }, 240_000);

  it("refuses a junction ancestor with an actionable error, while the direct profile path opens", async () => {
    const parent = profileDir();
    const target = path.join(parent, "target");
    ownedDirectory(target);
    const root = path.join(target, "root");
    await roundTrip(await openAsync(root));
    const junction = path.join(parent, "junction");
    fs.symlinkSync(target, junction, "junction");
    const error = await refusal(() => openAsync(path.join(junction, "root")));
    expect(error).toMatchObject({ ...UNSUPPORTED, message: expect.stringContaining(junction) });
    expect((error as Error).message).toMatch(/state root.*path with no junctions/i);
    await roundTrip(await openAsync(root));
  }, 120_000);

  it("refuses a nested missing root below a junction without changing the target", async () => {
    const parent = profileDir();
    const target = path.join(parent, "target");
    ownedDirectory(target);
    const junction = path.join(parent, "junction");
    fs.symlinkSync(target, junction, "junction");
    const targetAcl = readAcl(target).sddl;
    const targetChildren = fs.readdirSync(target).sort();
    const root = path.join(junction, "new", "mesh");
    expect(fs.existsSync(path.dirname(root))).toBe(false);
    expect(fs.existsSync(root)).toBe(false);

    for (const name of Object.keys(opens) as (keyof typeof opens)[]) {
      const error = await refusal(() => opens[name](root));
      expect(error).toMatchObject({ ...UNSUPPORTED, message: expect.stringContaining(junction) });
      expect((error as Error).message).toMatch(/state root.*path with no junctions/i);
      expect(readAcl(target).sddl).toBe(targetAcl);
      expect(fs.readdirSync(target).sort()).toEqual(targetChildren);
    }
  }, 120_000);

  it("rechecks between two synchronous opens in the same JavaScript turn", async () => {
    const root = path.join(profileDir(), "root");
    const first = openSync(root);
    first.close();
    // icacls is synchronous too: no await/microtask may clear a cross-open cache before the second open.
    grant(root, EVERYONE, "(OI)(CI)(M)", MODIFY);
    expect(await refusal(() => openSync(root))).toMatchObject(UNSUPPORTED);
  }, 120_000);

  it("rechecks at every open: an ACE added after an earlier successful open refuses the next one", async () => {
    const ancestor = profileDir();
    const root = path.join(ancestor, "root");
    await roundTrip(await openAsync(root));
    await roundTrip(openSync(root));
    grant(root, EVERYONE, "(OI)(CI)(M)", MODIFY);
    expect(await refusal(() => openAsync(root))).toMatchObject(UNSUPPORTED);
    expect(await refusal(() => openSync(root))).toMatchObject(UNSUPPORTED);
    // And on the ancestor: a second fresh root, opened once, then its private ancestor gains a foreign DELETE_CHILD.
    const other = path.join(profileDir(), "root");
    await roundTrip(await openAsync(other));
    grant(path.dirname(other), EVERYONE, "(DC)", 0x40);
    expect(await refusal(() => openAsync(other))).toMatchObject(UNSUPPORTED);
  }, 120_000);
});
