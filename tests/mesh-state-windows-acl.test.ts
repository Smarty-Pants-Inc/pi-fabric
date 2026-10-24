import childProcess from "node:child_process";
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
// No ACL mocks: the native-cache tests use only a call-through process-count spy. Hosted-only: fixtures live in a
// private directory below the runner's DEFAULT profile as the OS reports it (os.userInfo().homedir), not TMPDIR, whose
// RUNNER_TEMP ancestors are not a profile, so local runs never create or delete them. The profile and its
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
// ACL findings warn by default. Refusal rows opt in explicitly per test (in-process: stubbed env; fresh process:
// the child's env only); afterEach unstubs. The gate reads the setting at each call.
const ENFORCE = "PI_FABRIC_MESH_STATE_ACL_ENFORCE";
const ADVISORY = "FABRIC_MESH_STATE_ACL_ADVISORY";
const UNVERIFIED = "FABRIC_MESH_STATE_ACL_UNVERIFIED";
const enforceAcl = () => vi.stubEnv(ENFORCE, "1");

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
afterEach(() => {
  vi.unstubAllEnvs();
  for (const dir of created.splice(0).reverse()) fs.rmSync(dir, { recursive: true, force: true });
});
const within = (parent: string, child: string): boolean => {
  const relative = path.relative(path.resolve(parent), path.resolve(child));
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
};
/** The OS-provided profile of this token's user, NOT os.homedir()/USERPROFILE. On Windows os.homedir() reads USERPROFILE
 * first, and fleet isolation (scripts/test-temp.ts isolateTestFleetEnvironment, asserted by tests/test-temp.test.ts)
 * points USERPROFILE at <TMPDIR>/pi-fabric-test-fleet-<id>/home. That puts fixtures below RUNNER_TEMP, whose volume
 * ancestors are not a profile: in the hosted run of 2026-10-24, D:\, D:\a, D:\a\_temp and the job root all granted
 * Authenticated Users 0x1301bf on themselves, so every enforced open refused at an ancestor before any state file.
 * tests/test-deletion-guard.mjs isShared uses the same OS-provided rule. A profile under test temp fails loudly: a
 * refusal row must never pass because of an unsafe fixture ancestor. */
const defaultProfile = (): string => {
  const profile = os.userInfo().homedir;
  if (!profile || !path.isAbsolute(profile)) throw new Error(`OS profile ${String(profile)} is not an absolute path`);
  for (const temp of [os.tmpdir(), process.env.RUNNER_TEMP]) {
    if (temp && within(temp, profile)) throw new Error(`OS profile ${profile} is inside test temp ${temp}; ACL fixtures need the private profile chain`);
  }
  return path.resolve(profile);
};
/** A fresh private fixture directory directly below the default profile (inherits only the profile ACL). */
const profileDir = (): string => {
  const profile = defaultProfile();
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

/** Decision C: count ONLY the production native reader, never replace its execution or descriptor evidence.
 * vi.spyOn without mockImplementation calls the original execFileSync. Independent Get-Acl readback and icacls
 * helpers use spawnSync, so fixture preparation/verification cannot inflate this count. Restore in every finally. */
const countNativeAclProofs = () => {
  const spy = vi.spyOn(childProcess, "execFileSync");
  const indexes = () => spy.mock.calls.flatMap(([, , options], index) => options && typeof options === "object"
    && typeof options.env?.PI_FABRIC_STATE_ACL === "string" ? [index] : []);
  return {
    count: () => indexes().length,
    assertRealReads: () => {
      for (const index of indexes()) {
        expect(spy.mock.calls[index]![0]).toMatch(/[\\/]WindowsPowerShell[\\/]v1\.0[\\/]powershell\.exe$/i);
        const result = spy.mock.results[index]!;
        // A thrown child execution can be accepted by today's tooling fallback, but is NOT a verified proof.
        expect(result.type).toBe("return");
        const snapshot = JSON.parse(String(result.value)) as { entries?: unknown };
        expect(Array.isArray(snapshot.entries)).toBe(true);
      }
    },
    restore: () => spy.mockRestore(),
  };
};
/** A fresh process has no admitted proof and checks the real ACL through the normal SQLite entry point. */
const FRESH_PROCESS_ACL = String.raw`
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
const [root, mode] = process.argv.slice(1);
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { SqliteStateStore } = await jiti.import("./src/mesh/state-sqlite.ts");
try {
  const store = mode === "sync"
    ? SqliteStateStore.openSync(root, 65_536, 1_000, { initialize: "detached" })
    : await SqliteStateStore.open(root, 65_536, 1_000, { initialize: "detached" });
  store.close();
  process.stdout.write(JSON.stringify({ ok: true, code: null, message: null }) + "\n");
} catch (error) {
  process.stdout.write(JSON.stringify({ ok: false, code: error?.code ?? null, message: String(error?.message ?? error) }) + "\n");
}
`;
interface FreshProcessResult { ok: boolean; code: string | null; message: string | null }
/** A fresh process opening `root` with the setting exactly `enforce` in its own environment (absent when undefined). */
const freshProcessOpen = (root: string, mode: "async" | "sync", enforce: "1" | "0" | undefined) => {
  const env = { ...process.env };
  delete env[ENFORCE];
  if (enforce !== undefined) env[ENFORCE] = enforce;
  const result = childProcess.spawnSync(process.execPath,
    ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", FRESH_PROCESS_ACL, root, mode],
    { cwd: process.cwd(), encoding: "utf8", windowsHide: true, timeout: 60_000, maxBuffer: 1024 * 1024, env });
  expect(result.error?.message ?? "").toBe("");
  expect(result.status, String(result.stderr)).toBe(0);
  return { ...(JSON.parse(String(result.stdout).trim()) as FreshProcessResult), stderr: String(result.stderr) };
};
/** The enforced fresh-process refusal of the existing refusal rows (explicit opt-in in the child only). */
const freshProcessRefusal = (root: string, mode: "async" | "sync" = "sync"): FreshProcessResult => {
  const { stderr, ...parsed } = freshProcessOpen(root, mode, "1");
  expect(stderr).toBe("");
  expect(parsed.ok).toBe(false);
  return parsed;
};
/** Log bigint ctimeNs as metadata only; admission validity uses identity, never timestamps. */
const nativeCacheStamp = (file: string) => {
  const stat = fs.lstatSync(file, { bigint: true, throwIfNoEntry: false });
  return { path: file, stamp: stat ? {
    dev: String(stat.dev), ino: String(stat.ino), size: String(stat.size),
    kind: stat.isDirectory() ? "directory" : stat.isFile() ? "file" : "other",
    ctimeNs: typeof stat.ctimeNs === "bigint" ? String(stat.ctimeNs) : null,
    mtimeNs: String(stat.mtimeNs),
  } : null };
};
/** The complete gate target set, including missing sidecars/flag directory and both checked ancestor chains. */
const nativeCacheStamps = (root: string) => {
  const paths = new Set([root, ...["state.db", "state.db-wal", "state.db-shm", "state.db-journal", "state-checkpoint.flags"]
    .map(file => path.join(root, file))]);
  for (const start of [path.resolve(root), fs.realpathSync.native(root)]) {
    for (let at = path.dirname(start); ; at = path.dirname(at)) {
      paths.add(at);
      if (at === path.dirname(at)) break;
    }
  }
  return [...paths].map(nativeCacheStamp);
};
/** Compare existence, type, volume and file ID; ctime and mtime are deliberately not cache keys. */
const nativeCacheIdentities = (entries: ReturnType<typeof nativeCacheStamps>) => entries.map(entry => ({
  path: entry.path, exists: entry.stamp !== null, kind: entry.stamp?.kind ?? null,
  dev: entry.stamp?.dev ?? null, ino: entry.stamp?.ino ?? null,
}));
/** Seed and retain a live SQLite connection so ordinary WAL/SHM close cleanup cannot affect fixture observations. */
const seedCacheFixture = async (root: string): Promise<SqliteStateStore> => {
  const keeper = openSync(root);
  try {
    await keeper.put({ key: "acl/cache-probe", value: "seeded", identity: { id: "test", name: "test", kind: "agent" } });
    expect(keeper.get("acl/cache-probe")?.value).toBe("seeded");
    for (const file of ["state.db", "state.db-wal", "state.db-shm"]) expect(fs.existsSync(path.join(root, file)), file).toBe(true);
    return keeper;
  } catch (error) { keeper.close(); throw error; }
};

describe.runIf(process.env.GITHUB_ACTIONS === "true" && process.platform === "win32")("Windows mesh state ACL privacy (smarty-dev#8920)", () => {
  // Fixture precondition, enforced: advisory positive controls still pass on an unsafe ancestor (they only warn), and
  // refusal rows that check just the code or the root path would then pass for the wrong reason. Names any offender.
  it("fixture precondition: a clean root below the OS profile is admitted under enforcement, async and sync", async () => {
    enforceAcl();
    const root = profileDir();
    for (const name of Object.keys(opens) as (keyof typeof opens)[]) {
      const error = await refusal(() => opens[name](root));
      expect(error === undefined ? undefined : String((error as Error).message ?? error), name).toBeUndefined();
    }
  }, 120_000);

  // Native integration: C1 proves admitted identity reuse; C2 documents same-process trusted ACL mutation and
  // fresh-process refusal; C3 preserves the first-proof ancestor boundary. No ACL evidence is mocked.
  it("native-cache-C1: unchanged existing root and state.db reuse the admitted native proof across sync and async opens", async () => {
    const root = path.join(profileDir(), "root");
    const proofs = countNativeAclProofs();
    const keeper = await seedCacheFixture(root);
    const admittedProofs = proofs.count();
    let before: ReturnType<typeof nativeCacheStamps> | undefined;
    let after: ReturnType<typeof nativeCacheStamps> | undefined;
    try {
      before = nativeCacheStamps(root);
      expect(before.find(entry => entry.path === path.resolve(root))?.stamp).toMatchObject({ kind: "directory" });
      expect(before.find(entry => entry.path === path.join(path.resolve(root), "state.db"))?.stamp).toMatchObject({ kind: "file" });
      const first = openSync(root);
      try { expect(first.get("acl/cache-probe")?.value).toBe("seeded"); } finally { first.close(); }
      const second = openSync(root);
      try { expect(second.get("acl/cache-probe")?.value).toBe("seeded"); } finally { second.close(); }
      const third = await openAsync(root);
      try { expect(third.get("acl/cache-probe")?.value).toBe("seeded"); } finally { third.close(); }
      const fourth = openSync(root);
      try { expect(fourth.get("acl/cache-probe")?.value).toBe("seeded"); } finally { fourth.close(); }
      after = nativeCacheStamps(root);
      expect(nativeCacheIdentities(after)).toEqual(nativeCacheIdentities(before));
      proofs.assertRealReads();
      expect(proofs.count()).toBe(admittedProofs);
    } finally {
      try {
        console.log(JSON.stringify({ event: "native-cache-C1", admittedProofs, nativeProofs: proofs.count(), before,
          after: after ?? nativeCacheStamps(root) }));
      } finally { proofs.restore(); keeper.close(); }
    }
  }, 120_000);

  it("native-cache-C2: Everyone R added to state.db is accepted by the same-identity cache, then refused by a fresh process", async () => {
    const root = path.join(profileDir(), "root");
    const keeper = await seedCacheFixture(root);
    const file = path.join(root, "state.db");
    const proofs = countNativeAclProofs();
    let before: ReturnType<typeof nativeCacheStamp> | undefined;
    let after: ReturnType<typeof nativeCacheStamp> | undefined;
    let acceptedProofs = 0;
    try {
      const accepted = openSync(root);
      try { expect(accepted.get("acl/cache-probe")?.value).toBe("seeded"); } finally { accepted.close(); }
      acceptedProofs = proofs.count();
      before = nativeCacheStamp(file);
      grant(file, EVERYONE, "(R)", EVERYONE_READ);
      const cached = openSync(root);
      try { expect(cached.get("acl/cache-probe")?.value).toBe("seeded"); } finally { cached.close(); }
      after = nativeCacheStamp(file);
      expect(proofs.count()).toBe(acceptedProofs);
      expect(after.stamp?.dev).toBe(before.stamp?.dev);
      expect(after.stamp?.ino).toBe(before.stamp?.ino);
      expect(after.stamp?.kind).toBe("file");
      expect(icaclsShow(file)).toMatch(/Everyone:\(R\)/);
      for (const mode of ["async", "sync"] as const) {
        expect(freshProcessRefusal(root, mode)).toMatchObject({ code: UNSUPPORTED.code, message: expect.stringContaining(file) });
      }
      proofs.assertRealReads();
    } finally {
      try {
        console.log(JSON.stringify({ event: "native-cache-C2", acceptedProofs, nativeProofs: proofs.count(), before, after,
          ctimeNsChanged: before && after ? before.stamp?.ctimeNs !== after.stamp?.ctimeNs : null }));
      } finally { proofs.restore(); keeper.close(); }
    }
  }, 120_000);

  it("native-cache-C3: first proof refuses a fixture root whose ancestor has foreign DELETE_CHILD", () => {
    enforceAcl();
    const ancestor = profileDir();
    const root = path.join(ancestor, "root");
    ownedDirectory(root);
    grant(ancestor, EVERYONE, "(DC)", 0x40);
    const proofs = countNativeAclProofs();
    try {
      let error: unknown;
      try { openSync(root).close(); } catch (caught) { error = caught; }
      expect(error).toMatchObject(UNSUPPORTED);
      expect((error as Error).message).toContain(ancestor);
      expect(proofs.count()).toBe(1);
      proofs.assertRealReads();
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
      expect(readAcl(root).rules.filter(rule => rule.sid === EVERYONE)).toEqual([]);
    } finally { proofs.restore(); }
  }, 120_000);

  it.each(Object.keys(opens) as (keyof typeof opens)[])("%s refuses a root with a genuine Everyone:(M) grant before creating state.db", async name => {
    enforceAcl();
    const root = profileDir();
    grant(root, EVERYONE, "(OI)(CI)(M)", MODIFY);
    expect(icaclsShow(root)).toMatch(/Everyone:\(OI\)\(CI\)\(M\)/);
    expect(await refusal(() => opens[name](root))).toMatchObject(UNSUPPORTED);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  }, 120_000);

  it.each([["write", "(W)", 0x2], ["modify", "(M)", MODIFY], ["full control", "(F)", FULL], ["delete", "(DE)", 0x10000],
    ["change permissions", "(WDAC)", 0x40000], ["take ownership", "(WO)", 0x80000]] as const)(
    "refuses a root where a foreign SID has %s", async (_name, rights, bits) => {
      enforceAcl();
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
    for (const mode of ["async", "sync"] as const) {
      expect(freshProcessRefusal(root, mode)).toMatchObject({ code: UNSUPPORTED.code, message: expect.stringContaining(file) });
    }
  }, 120_000);

  it.each(["state.db-wal", "state.db-shm", "state.db-journal"])("refuses an existing %s with a verified Everyone (R) data-read grant", async file => {
    const root = profileDir();
    await roundTrip(await openAsync(root));
    const target = path.join(root, file);
    if (!fs.existsSync(target)) fs.writeFileSync(target, "");
    grant(target, EVERYONE, "(R)", EVERYONE_READ);
    expect(icaclsShow(target)).toMatch(/Everyone:\(R\)/);
    for (const mode of ["async", "sync"] as const) {
      expect(freshProcessRefusal(root, mode)).toMatchObject({ code: UNSUPPORTED.code, message: expect.stringContaining(target) });
    }
  }, 120_000);

  it("opens real node:sqlite in a clean root below the default USERPROFILE without touching its ancestors", async () => {
    const profile = defaultProfile();
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
    enforceAcl(); // owner/protected-DACL setup is enforcement-only; the default never mutates ACLs
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
    enforceAcl();
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
    enforceAcl();
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
    enforceAcl(); // the protected root exists only under enforcement
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
      enforceAcl();
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
    for (const mode of ["async", "sync"] as const) {
      expect(freshProcessRefusal(root, mode)).toMatchObject({ code: UNSUPPORTED.code, message: expect.stringContaining(target) });
    }
  }, 120_000);

  it("refuses to raise a checkpoint flag in a flag directory that a foreign SID may modify", () => {
    enforceAcl();
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
    enforceAcl();
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
    enforceAcl();
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
    enforceAcl(); // the reparse-ancestor finding and lexical creation walk refuse only under enforcement
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
    enforceAcl();
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

  it("a fresh process refuses a root ACL added after an earlier successful open", () => {
    const root = path.join(profileDir(), "root");
    const first = openSync(root);
    first.close();
    grant(root, EVERYONE, "(OI)(CI)(M)", MODIFY);
    expect(freshProcessRefusal(root, "sync")).toMatchObject({ code: UNSUPPORTED.code, message: expect.stringContaining(root) });
  }, 120_000);

  it("a fresh process refuses ACL changes on the root and its ancestor", async () => {
    const ancestor = profileDir();
    const root = path.join(ancestor, "root");
    await roundTrip(await openAsync(root));
    await roundTrip(openSync(root));
    grant(root, EVERYONE, "(OI)(CI)(M)", MODIFY);
    for (const mode of ["async", "sync"] as const) {
      expect(freshProcessRefusal(root, mode)).toMatchObject({ code: UNSUPPORTED.code, message: expect.stringContaining(root) });
    }
    // And on the ancestor: a second fresh root, opened once, then its private ancestor gains a foreign DELETE_CHILD.
    const other = path.join(profileDir(), "root");
    await roundTrip(await openAsync(other));
    grant(path.dirname(other), EVERYONE, "(DC)", 0x40);
    expect(freshProcessRefusal(other, "async")).toMatchObject({ code: UNSUPPORTED.code, message: expect.stringContaining(path.dirname(other)) });
  }, 120_000);

  // Warn-by-default with a tested switch back, on real NTFS: one genuine Everyone:(M) root ACE, real native reads and
  // real node:sqlite, in this process and in fresh processes. Only the setting changes; the ACL is never repaired.
  it("native-rollback: a genuine unsafe root ACE warns and starts by default, refuses only with ENFORCE=1, then starts again when unset", async () => {
    vi.stubEnv(ENFORCE, undefined);
    const root = path.join(profileDir(), "root");
    // Clean seed in a child process only: a parent open would cache this root+DB identity, and the same-identity
    // grant below is not re-read in-process (native-cache-C2), hiding the finding this test needs.
    expect(freshProcessOpen(root, "sync", undefined)).toMatchObject({ ok: true, code: null, stderr: "" });
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
    grant(root, EVERYONE, "(OI)(CI)(M)", MODIFY);
    expect(icaclsShow(root)).toMatch(/Everyone:\(OI\)\(CI\)\(M\)/);
    const sddl = readAcl(root).sddl;
    // Call-through: the finding is still printed. The advisory travels on the gate's dedicated stderr route, which
    // Pi's setupCli() leaves on (it silences process.emitWarning); its code is in the text. The global console is
    // never that route (P2-B: an app's console.warn/error spies must not see it), so console calls are counted apart.
    const advisoryLines: string[] = [];
    const consoleLines: string[] = [];
    const consoleSpies = (["warn", "error"] as const).map(level => {
      const original = console[level].bind(console);
      return vi.spyOn(console, level).mockImplementation((...args: unknown[]) => {
        const text = args.map(String).join(" ");
        if (text.includes(ADVISORY) || text.includes(UNVERIFIED)) consoleLines.push(text);
        original(...args);
      });
    });
    const stderrWrite = process.stderr.write.bind(process.stderr) as (...args: unknown[]) => boolean;
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation(((chunk: unknown, ...rest: unknown[]) => {
      if (String(chunk).includes(ADVISORY)) advisoryLines.push(String(chunk));
      return stderrWrite(chunk, ...rest);
    }) as typeof process.stderr.write);
    const advisories = () => [...advisoryLines];
    const proofs = countNativeAclProofs();
    const steps: { step: string; outcome: unknown; nativeProofs: number; advisories: number }[] = [];
    const record = (step: string, outcome: unknown) =>
      steps.push({ step, outcome, nativeProofs: proofs.count(), advisories: advisories().length });
    try {
      // 1. Default (unset): the privacy finding is logged and startup proceeds, sync and async.
      await roundTrip(openSync(root));
      await roundTrip(await openAsync(root));
      expect(advisories()).toEqual([expect.stringContaining(EVERYONE)]);
      expect(advisories()[0]).toContain(ENFORCE);
      record("default-unset", "opened");
      vi.stubEnv(ENFORCE, "0");
      await roundTrip(openSync(root));
      record("explicit-0", "opened");

      // 2. On: the same process now refuses before SQLite opens, as does a fresh process.
      enforceAcl();
      for (const name of Object.keys(opens) as (keyof typeof opens)[]) {
        const error = await refusal(() => opens[name](root));
        expect(error).toMatchObject({ ...UNSUPPORTED, message: expect.stringContaining(EVERYONE) });
        record(`enforced-${name}`, (error as Error).message);
      }
      const freshEnforced = freshProcessOpen(root, "sync", "1");
      expect(freshEnforced).toMatchObject({ ok: false, code: UNSUPPORTED.code, message: expect.stringContaining(root) });
      record("enforced-fresh-process", freshEnforced);

      // 3. Off again (unset): the same process and a fresh process start; the earlier data is intact.
      vi.stubEnv(ENFORCE, undefined);
      const reopened = openSync(root);
      try { expect(reopened.get("acl/probe")?.value).toBe("ok"); } finally { reopened.close(); }
      await roundTrip(await openAsync(root));
      record("rollback-unset", "opened");
      const freshDefault = freshProcessOpen(root, "async", undefined);
      expect(freshDefault).toMatchObject({ ok: true, code: null });
      expect(freshDefault.stderr).toContain(ADVISORY);
      expect(freshDefault.stderr).toContain(EVERYONE);
      record("rollback-fresh-process", freshDefault);

      // Neither mode repaired or changed the ACL; the finding was real native evidence, re-read while enforced.
      expect(readAcl(root).sddl).toBe(sddl);
      expect(icaclsShow(root)).toMatch(/Everyone:\(OI\)\(CI\)\(M\)/);
      expect(advisories()).toHaveLength(1);
      expect(consoleLines).toEqual([]); // the dedicated route, never the global console
      proofs.assertRealReads();
      // Advisory caching (P2): the first parent read warns and is remembered; default/0 repeats take no native read,
      // every enforced open reads afresh and refuses, and off again returns to the remembered advisory. The fresh
      // child seed above is what makes that first parent read see the real Everyone:(M) grant.
      // Counts are cumulative in recorded order: `opens` iterates open (async) BEFORE openSync, so each enforced step
      // adds exactly one fresh read to the step recorded just before it (CI 0710 trace: 1, 1, 2, 3, then 3).
      const proofsAt = (step: string) => steps.find(entry => entry.step === step)!.nativeProofs;
      expect(steps.map(entry => entry.step)).toEqual(["default-unset", "explicit-0", "enforced-open", "enforced-openSync",
        "enforced-fresh-process", "rollback-unset", "rollback-fresh-process"]);
      expect(proofsAt("default-unset")).toBe(1);
      expect(proofsAt("explicit-0")).toBe(proofsAt("default-unset"));
      expect(proofsAt("enforced-open")).toBe(proofsAt("explicit-0") + 1);
      expect(proofsAt("enforced-openSync")).toBe(proofsAt("enforced-open") + 1);
      expect(proofsAt("rollback-unset")).toBe(proofsAt("enforced-openSync"));
    } finally {
      try { console.log(JSON.stringify({ event: "native-rollback", root, steps, nativeProofs: proofs.count() })); }
      finally { proofs.restore(); for (const spy of consoleSpies) spy.mockRestore(); stderr.mockRestore(); }
    }
  }, 240_000);
});
