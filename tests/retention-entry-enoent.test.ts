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
  write(path.join(directory, "events.jsonl"), '{"sequence":1}\n');
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

  it("stops when a registry directory disappears between readdir and lstat (the root set changed)", async () => {
    const m = mesh();
    onListing(m.actors, { 2: { after: () => fs.renameSync(path.join(m.actors, "other"), path.join(m.away, "other")) } });
    const plan = await sweep(m.root);
    expect(plan.skipped).toContainEqual(stopped(m.root));
    expect(plan.removedRuns).not.toEqual(expect.arrayContaining([m.old, m.orphan]));
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

  it("stops after 3 listings when the entry keeps vanishing", async () => {
    const m = mesh();
    const listings = onListing(m.actors, {});
    const failures = failLstat(m.entry, "ENOENT");
    const plan = await sweep(m.root);
    expect(failures()).toBe(3);
    expect(listings()).toBe(4);
    expect(plan.skipped).toContainEqual(stopped(m.root));
    expect(plan.removedRuns).not.toEqual(expect.arrayContaining([m.old, m.orphan]));
  });

  it("re-lists through a transient ENOENT and plans eligible runs", async () => {
    const m = mesh();
    const failures = failLstat(m.entry, "ENOENT", 2);
    const plan = await sweep(m.root);
    expect(failures()).toBe(2);
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
  });

  it("stops at once on a permission error", async () => {
    const m = mesh();
    const listings = onListing(m.actors, {});
    const failures = failLstat(m.entry, "EACCES");
    const plan = await sweep(m.root);
    expect(failures()).toBe(1);
    expect(listings()).toBe(2);
    expect(plan.skipped).toContainEqual(stopped(m.root));
    expect(plan.removedRuns).not.toEqual(expect.arrayContaining([m.old, m.orphan]));
  });

  it("plans eligible runs when the listing is unchanged or only gains names", async () => {
    const m = mesh();
    onListing(m.actors, { 2: { before: () => fs.writeFileSync(path.join(m.actors, "note.txt"), "benign") } });
    const plan = await sweep(m.root);
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
  });
});
