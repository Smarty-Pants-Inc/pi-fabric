import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";

// #1912's isolated home/file probes: never read or change the user's alpha disable file.
const alpha = vi.hoisted(() => ({ home: vi.fn(() => "/fixture-home"), exists: vi.fn(() => false) }));
vi.mock("node:os", async original => ({
  ...await original<typeof import("node:os")>(), homedir: alpha.home,
}));
vi.mock("node:fs", async original => ({
  ...await original<typeof import("node:fs")>(), existsSync: alpha.exists,
}));

const roots: string[] = [];
beforeEach(() => {
  vi.clearAllMocks();
  vi.stubEnv("SMARTY_ALPHA", "0");
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity = { id: "admission-test", name: "admission-test", kind: "agent" as const };
async function fixture(options: Parameters<typeof SqliteStateStore.open>[3] = {}) {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "sqlite-admission-")); roots.push(parent);
  const root = path.join(parent, "mesh"); fs.mkdirSync(root);
  const store = await SqliteStateStore.open(root, 65536, 1000, { initialize: "create", ...options });
  return { store, root, file: path.join(root, "state.db") };
}

describe("SQLite native admission", () => {
  it.each(["0", "1"])("a native SQLite holder bounds acquisition and leaves no write or waiter (SMARTY_ALPHA=%s)", async enabled => {
    vi.stubEnv("SMARTY_ALPHA", enabled);
    const { store, file } = await fixture({ lockTimeoutMs: 100 });
    const holder = new DatabaseSync(file, { timeout: 0 });
    holder.exec("BEGIN IMMEDIATE");
    const began = performance.now();
    try {
      await expect(store.put({ key: "deadline", value: 3, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(performance.now() - began).toBeLessThan(1000);
      expect(store.get("deadline")).toBeUndefined();
      expect(store.stats()).toMatchObject({ writesWaiting: 0, fifoJoins: 0 });
    } finally { if (holder.isTransaction) holder.exec("ROLLBACK"); holder.close(); store.close(); }
  });

  it.each(["0", "1"])("abort interrupts native SQLite contention and leaves no write or waiter (SMARTY_ALPHA=%s)", async enabled => {
    vi.stubEnv("SMARTY_ALPHA", enabled);
    const controller = new AbortController();
    const { store, file } = await fixture({ writeSignal: controller.signal });
    const holder = new DatabaseSync(file, { timeout: 0 });
    holder.exec("BEGIN IMMEDIATE");
    const abort = setTimeout(() => controller.abort(new Error("admission cancelled")), 40);
    const began = performance.now();
    try {
      await expect(store.put({ key: "abort", value: 4, identity })).rejects.toThrow("admission cancelled");
      expect(performance.now() - began).toBeLessThan(1000);
      expect(store.get("abort")).toBeUndefined();
      expect(store.stats()).toMatchObject({ writesWaiting: 0, fifoJoins: 0 });
    } finally { clearTimeout(abort); if (holder.isTransaction) holder.exec("ROLLBACK"); holder.close(); store.close(); }
  });
});

// Trust-boundary regression: the same real SQLite admission must relax only the POSIX ancestor check.
describe.skipIf(process.platform === "win32" || typeof process.getuid !== "function")("SQLite alpha ancestor admission", () => {
  const unsafeRoot = () => {
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), "sqlite-alpha-ancestor-")); roots.push(parent);
    const root = path.join(parent, "mesh"); fs.mkdirSync(root, { mode: 0o700 });
    fs.chmodSync(parent, 0o777);
    return { parent, root, file: path.join(root, "state.db") };
  };
  const freshGate = async () => {
    vi.resetModules();
    return import("../src/mesh/state-gate.js");
  };

  it.each(["0", "1"])("the real store creates and writes beneath an unsafe ancestor only in alpha (SMARTY_ALPHA=%s)", async enabled => {
    vi.stubEnv("SMARTY_ALPHA", enabled);
    const { root, file } = unsafeRoot();
    const opening = SqliteStateStore.open(root, 65536, 1000, { initialize: "create" });
    if (enabled === "0") {
      await expect(opening).rejects.toThrow(/its ancestor .* without the sticky bit/);
      expect(fs.existsSync(file)).toBe(false);
    } else {
      const store = await opening;
      try {
        await store.put({ key: "alpha", value: "admitted", identity });
        expect(store.get("alpha")?.value).toBe("admitted");
      } finally { store.close(); }
    }
  });

  it("SMARTY_ALPHA=1 opens beneath an unsafe ancestor and warns once with the named reason", async () => {
    vi.stubEnv("SMARTY_ALPHA", "1");
    const { openPrivateStateDb } = await freshGate();
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    for (let index = 0; index < 2; index++) {
      const { root, file } = unsafeRoot();
      const seed = new DatabaseSync(file); seed.close(); fs.chmodSync(file, 0o600);
      for (let attempt = 0; attempt < 2; attempt++) {
        const open = () => openPrivateStateDb(file, target => new DatabaseSync(target));
        expect(() => open().close(), "alpha must admit the real SQLite connection, not fail at its ancestor").not.toThrow();
        const db = open();
        try {
          db.exec("CREATE TABLE IF NOT EXISTS alpha_admission (value INTEGER)");
          db.exec("INSERT INTO alpha_admission VALUES (1)");
          expect(db.prepare("SELECT count(*) AS n FROM alpha_admission").get()).toMatchObject({ n: attempt + 1 });
        } finally { db.close(); }
      }
      expect(fs.statSync(root).mode & 0o777).toBe(0o700);
    }
    expect(warning).toHaveBeenCalledTimes(1);
    expect(warning).toHaveBeenCalledWith(expect.stringMatching(/FABRIC_MESH_STATE_ANCESTOR_ALPHA.*ancestor.*SMARTY_ALPHA/));
  });

  it("SMARTY_ALPHA=0 keeps the original unsafe-ancestor refusal before SQLite opens", async () => {
    const { openPrivateStateDb } = await freshGate();
    const { file } = unsafeRoot();
    const seed = new DatabaseSync(file); seed.close(); fs.chmodSync(file, 0o600);
    const opener = vi.fn(target => new DatabaseSync(target));
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    expect(() => openPrivateStateDb(file, opener)).toThrow(/its ancestor .* is group or other writable without the sticky bit/);
    expect(opener).not.toHaveBeenCalled();
    expect(warning).not.toHaveBeenCalled();
  });

  describe.each(["0", "1"])("mandatory state guards (SMARTY_ALPHA=%s)", enabled => {
    beforeEach(() => {
      vi.stubEnv("SMARTY_ALPHA", enabled);
      vi.spyOn(console, "warn").mockImplementation(() => undefined);
    });

    it.each(["root writable", "database writable", "database directory", "database symlink", "wal symlink", "shm writable"] as const)(
      "refuses %s before opening SQLite", async kind => {
        const { openPrivateStateDb } = await freshGate();
        const { parent, root, file } = unsafeRoot();
        // Alpha ON must reach these guards even beneath an unsafe ancestor. OFF uses a safe ancestor so the
        // expected file/path refusal cannot be masked by the original ancestor refusal.
        if (enabled === "0") fs.chmodSync(parent, 0o700);
        const seed = new DatabaseSync(file); seed.close(); fs.chmodSync(file, 0o600);
        const target = kind === "root writable" ? root : kind === "wal symlink" ? `${file}-wal`
          : kind === "shm writable" ? `${file}-shm` : file;
        const reason = kind === "root writable" || kind === "database writable" || kind === "shm writable"
          ? "is group or other writable" : kind === "database directory" ? "is not a regular file" : "is a symbolic link";
        if (kind === "root writable") fs.chmodSync(root, 0o770);
        if (kind === "database writable") fs.chmodSync(file, 0o660);
        if (kind === "database directory" || kind === "database symlink") {
          fs.renameSync(file, `${file}.original`);
          if (kind === "database directory") fs.mkdirSync(file);
          else fs.symlinkSync(`${file}.original`, file);
        }
        if (kind === "wal symlink") fs.symlinkSync(file, `${file}-wal`);
        if (kind === "shm writable") { fs.writeFileSync(`${file}-shm`, ""); fs.chmodSync(`${file}-shm`, 0o660); }
        const opener = vi.fn(target => new DatabaseSync(target));
        expect(() => openPrivateStateDb(file, opener)).toThrow(expect.objectContaining({
          code: "FABRIC_MESH_STATE_UNSUPPORTED", message: expect.stringContaining(`refuses ${target}: it ${reason}`),
        }));
        expect(opener).not.toHaveBeenCalled();
      });

    it.skipIf(process.platform !== "linux")("refuses an ABA inode swap during the real SQLite open and closes it before SQL", async () => {
      const { openPrivateStateDb } = await freshGate();
      const { parent, root, file } = unsafeRoot();
      if (enabled === "0") fs.chmodSync(parent, 0o700);
      const foreign = path.join(root, "foreign.db");
      for (const target of [file, foreign]) {
        const seed = new DatabaseSync(target);
        try { seed.exec("CREATE TABLE original (value INTEGER)"); } finally { seed.close(); }
        fs.chmodSync(target, 0o600);
      }
      const checked = fs.statSync(file);
      for (const swap of ["rename", "symlink"] as const) {
        let opened: DatabaseSync | undefined;
        let closed = 0;
        let sql = 0;
        const swapping = (target: string) => {
          fs.renameSync(file, `${file}.aside`);
          if (swap === "symlink") fs.symlinkSync(foreign, file);
          else fs.copyFileSync(foreign, file);
          opened = new DatabaseSync(target);
          fs.renameSync(`${file}.aside`, file); // ABA: the path has the checked inode again before the gate resumes.
          const close = opened.close.bind(opened);
          opened.close = () => { closed++; close(); };
          const prepare = opened.prepare.bind(opened);
          opened.prepare = (text: string) => { sql++; return prepare(text); };
          return opened;
        };
        try {
          expect(() => openPrivateStateDb(file, swapping), swap).toThrow(/the connection('s own descriptor on it is not proven| opened a different file)/);
          expect(closed, swap).toBe(1);
          expect(sql, swap).toBe(0);
          expect(fs.statSync(file).ino, swap).toBe(checked.ino);
        } finally { if (opened && closed === 0) opened.close(); }
      }
      const db = openPrivateStateDb(file, target => new DatabaseSync(target));
      try { expect(db.prepare("SELECT count(*) AS n FROM original").get()).toMatchObject({ n: 0 }); }
      finally { db.close(); }
    });
  });

  it("rechecks the live alpha flag after a warning instead of caching permission", async () => {
    const { openPrivateStateDb } = await freshGate();
    const { file } = unsafeRoot();
    const seed = new DatabaseSync(file); seed.close(); fs.chmodSync(file, 0o600);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const open = () => openPrivateStateDb(file, target => new DatabaseSync(target));
    vi.stubEnv("SMARTY_ALPHA", "1");
    expect(() => open().close()).not.toThrow();
    vi.stubEnv("SMARTY_ALPHA", "0");
    expect(open).toThrow(/its ancestor .* without the sticky bit/);
    vi.stubEnv("SMARTY_ALPHA", "1");
    expect(() => open().close()).not.toThrow();
    expect(warning).toHaveBeenCalledTimes(1);
  });
});
