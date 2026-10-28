import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { sweepMeshRetention } from "../src/storage/retention-cli.js";

// smarty-dev#10635: an actors/ entry removed between the listing's readdir and its lstat re-lists (at most 3
// listings per check) instead of stopping the sweep; a replacement still stops it. Dry runs only.
const DAY = 24 * 60 * 60 * 1_000;
const NOW = Date.now();
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const tmp = (prefix: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(root); return root; };
const run = (directory: string, finishedAt: number) => {
  write(path.join(directory, "status.json"), JSON.stringify({ transport: "process", sessionId: "2147483647", status: "completed", finishedAt }));
  write(path.join(directory, "task.txt"), "generic retention fixture\n");
  write(path.join(directory, "reply.json"), '{"text":"done"}');
  return directory;
};
/** A dead session registry with two expired candidates, a plain file actors/s and an empty registry actors/other. */
const mesh = () => {
  const root = tmp("fabric-10635-");
  const actors = path.join(root, "actors");
  const registry = path.join(actors, "01a0cd9c-dead-session");
  const actor = path.join(registry, "actor");
  write(path.join(registry, "actors.json"), JSON.stringify({ actors: [{
    id: "actor", rootId: "session:01a0cd9c-dead-session", status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest",
  }] }));
  write(path.join(actor, "session.jsonl"), "live");
  run(path.join(actor, "runs", "latest"), NOW - 9 * DAY);
  const old = run(path.join(actor, "runs", "old"), NOW - 8 * DAY);
  const orphan = run(path.join(registry, "removed-actor", "runs", "orphan"), NOW - 10 * DAY);
  write(path.join(actors, "other", "actors.json"), JSON.stringify({ actors: [] }));
  const entry = path.join(actors, "s");
  fs.writeFileSync(entry, "not a registry");
  return { root, actors, old, orphan, entry, away: tmp("fabric-10635-away-") };
};
const inEntries = () => /at entries \(/.test(new Error().stack ?? "");
/** Hooks around the n-th listing of actors/ made by the entry listing (1 is the sweep's initial listing). */
const onListing = (actors: string, hooks: Record<number, { before?: () => void; after?: () => void }>) => {
  const list = fs.readdirSync;
  let count = 0;
  vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
    const n = String(target) === actors && inEntries() ? ++count : 0;
    hooks[n]?.before?.();
    const names = (list as (...args: unknown[]) => unknown)(target, ...rest);
    hooks[n]?.after?.();
    return names;
  }) as typeof fs.readdirSync);
  return () => count;
};
/** Fails every entry-listing lstat of `file` after the initial listing with `code`; returns the failure count. */
const failLstat = (file: string, code: string, times = Infinity) => {
  const stat = fs.lstatSync;
  let initial = true, failures = 0;
  vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
    if (String(target) === file && inEntries()) {
      if (initial) initial = false;
      else if (failures < times) { failures++; throw Object.assign(new Error(`${code}: ${file}`), { code }); }
    }
    return (stat as (...args: unknown[]) => unknown)(target, ...rest);
  }) as typeof fs.lstatSync);
  return () => failures;
};
/** Recorded fs, no real removal: entry-listing readdirs of actors/ gain `extra(n)` names at listing n, and each lstat of an
 * actors/ entry inside entries() fails with `fail(name, n)` or is rewritten by `stat`. A call to entries() begins at the
 * observation's own lstat of actors/ (outside entries); `calls()` gives each call's listings and failures. */
const recorded = (actors: string, options: {
  extra?: (n: number) => string[];
  fail?: (name: string, n: number) => string | undefined;
  stat?: (name: string, n: number) => fs.Stats | undefined;
} = {}) => {
  const deep = () => { const limit = Error.stackTraceLimit; Error.stackTraceLimit = 100; try { return inEntries(); } finally { Error.stackTraceLimit = limit; } };
  const list = fs.readdirSync as (...args: unknown[]) => string[], stat = fs.lstatSync as (...args: unknown[]) => fs.Stats;
  const calls: Array<{ listings: number; failures: number }> = [];
  let n = 0;
  vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
    const names = list(target, ...rest);
    if (String(target) !== actors || !deep()) return names;
    n++;
    if (calls.length === 0) calls.push({ listings: 0, failures: 0 });
    calls.at(-1)!.listings++;
    return [...names, ...(options.extra?.(n) ?? []).filter(name => !names.includes(name))];
  }) as typeof fs.readdirSync);
  vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
    const file = String(target);
    if (file === actors && !deep()) calls.push({ listings: 0, failures: 0 });
    else if (path.dirname(file) === actors && deep()) {
      const code = options.fail?.(path.basename(file), n);
      if (code) { calls.at(-1)!.failures++; throw Object.assign(new Error(`${code}: ${file}`), { code }); }
      const fake = options.stat?.(path.basename(file), n);
      if (fake) return fake;
    }
    return stat(target, ...rest);
  }) as typeof fs.lstatSync);
  return { calls: () => calls.filter(call => call.listings > 0) };
};
/** A copy of a real file's lstat with another inode (read past the spies). */
const realLstat = fs.lstatSync;
const withIno = (file: string, ino: number): fs.Stats => {
  const real = realLstat(file);
  return Object.assign(Object.create(Object.getPrototypeOf(real)), real, { ino });
};
const uncertain = (root: string) => ({ path: path.join(root, "actors"), reason: expect.stringMatching(/registry roots are uncertain/) });
const sweep = (root: string) => sweepMeshRetention(root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
const stopped = (root: string) => ({ path: path.join(root, "actors"), reason: expect.stringMatching(/registry roots changed during the sweep/) });

describe("ENOENT-aware actors/ entry listing (#10635)", () => {
  it.each([1, 2])("plans eligible runs when actors/s is removed between readdir and lstat (listing %i)", async n => {
    const m = mesh();
    const listings = onListing(m.actors, { [n]: { after: () => fs.rmSync(m.entry) } });
    const plan = await sweep(m.root);
    expect(listings()).toBeGreaterThan(n);
    expect(fs.existsSync(m.entry)).toBe(false);
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
  });

  it("plans the stable candidates when a registry directory disappears between readdir and lstat (a benign removal)", async () => {
    const m = mesh();
    onListing(m.actors, { 2: { after: () => fs.renameSync(path.join(m.actors, "other"), path.join(m.away, "other")) } });
    const plan = await sweep(m.root);
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
  });

  it("stops for good when a new name changes inode across one call's ENOENT retry (recorded fs)", async () => {
    const m = mesh();
    // Listing 2 (the first observation): s vanishes and the new name n has inode 900001; its retry sees n at 900002.
    // Every later call sees n stable at 900002, which must not be adopted.
    const fs_ = recorded(m.actors, {
      extra: n => n >= 2 ? ["n"] : [],
      fail: (name, n) => name === "s" && n === 2 ? "ENOENT" : undefined,
      stat: (name, n) => name === "n" ? withIno(m.entry, n === 2 ? 900001 : 900002) : undefined,
    });
    const plan = await sweep(m.root);
    expect(fs_.calls()[1]).toEqual({ listings: 2, failures: 1 });
    expect(plan.skipped).toContainEqual(stopped(m.root));
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.orphan);
  });

  it("plans eligible runs when a genuinely new name stays stable across one call's ENOENT retry (recorded fs)", async () => {
    const m = mesh();
    const fs_ = recorded(m.actors, {
      extra: n => n >= 2 ? ["n"] : [],
      fail: (name, n) => name === "s" && n === 2 ? "ENOENT" : undefined,
      stat: name => name === "n" ? withIno(m.entry, 900001) : undefined,
    });
    const plan = await sweep(m.root);
    expect(fs_.calls()[1]).toEqual({ listings: 2, failures: 1 });
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
  });

  it.each([
    ["a directory", (e: string) => fs.mkdirSync(e)],
    ["a new file", (e: string) => fs.writeFileSync(e, "not a registry")],
  ] as const)("stops when the vanished actors/s comes back as %s at the re-list", async (_name, recreate) => {
    const m = mesh();
    onListing(m.actors, { 2: { after: () => fs.renameSync(m.entry, path.join(m.away, "s")) }, 3: { before: () => recreate(m.entry) } });
    const plan = await sweep(m.root);
    expect(plan.skipped).toContainEqual(stopped(m.root));
    expect(plan.removedRuns).not.toEqual(expect.arrayContaining([m.old, m.orphan]));
  });

  it("stops when a name seen in one attempt is replaced before the re-list (no absorption across attempts)", async () => {
    const m = mesh();
    const added = path.join(m.actors, "n");
    onListing(m.actors, {
      2: { before: () => fs.writeFileSync(added, "first"), after: () => fs.rmSync(m.entry) },
      3: { before: () => { fs.renameSync(added, path.join(m.away, "n")); fs.writeFileSync(added, "second"); } },
    });
    const plan = await sweep(m.root);
    expect(plan.skipped).toContainEqual(stopped(m.root));
    expect(plan.removedRuns).not.toEqual(expect.arrayContaining([m.old, m.orphan]));
  });

  it("keeps every candidate, not sticky, when the entry keeps vanishing: 3 listings per entries() call", async () => {
    const m = mesh();
    const fs_ = recorded(m.actors, { fail: (name, n) => name === "s" && n > 1 ? "ENOENT" : undefined });
    const plan = await sweep(m.root);
    const [initial, ...later] = fs_.calls();
    expect(initial).toEqual({ listings: 1, failures: 0 });
    expect(later.length).toBeGreaterThan(1);
    for (const call of later) expect(call).toEqual({ listings: 3, failures: 3 });
    expect(plan.skipped).toContainEqual(uncertain(m.root));
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.orphan);
  });

  it("re-lists through a transient ENOENT and plans eligible runs", async () => {
    const m = mesh();
    const failures = failLstat(m.entry, "ENOENT", 2);
    const plan = await sweep(m.root);
    expect(failures()).toBe(2);
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
  });

  it("a permission error ends only its own entries() call after one listing, not sticky", async () => {
    const m = mesh();
    const fs_ = recorded(m.actors, { fail: (name, n) => name === "s" && n === 2 ? "EACCES" : undefined });
    const plan = await sweep(m.root);
    const calls = fs_.calls();
    expect(calls[1]).toEqual({ listings: 1, failures: 1 });
    for (const call of [calls[0], ...calls.slice(2)]) expect(call).toEqual({ listings: 1, failures: 0 });
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
  });

  it("plans eligible runs when the listing is unchanged or only gains names", async () => {
    const m = mesh();
    onListing(m.actors, { 2: { before: () => fs.writeFileSync(path.join(m.actors, "note.txt"), "benign") } });
    const plan = await sweep(m.root);
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
  });
});
