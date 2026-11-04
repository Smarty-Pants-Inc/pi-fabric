import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { sweepMeshRetention } from "../src/storage/retention-cli.js";

// smarty-dev#9160: every direct actors/ entry's lstat type, dev and ino is fenced for the whole sweep. Dry runs only:
// the plan (removedRuns) is what --apply would delete, and the final pre-delete check runs in a dry run too.
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
/** A dead session registry with two expired resident candidates and an entry actors/s of the given kind. */
const mesh = (kind: "file" | "directory", runOrder: "normal" | "reversed" = process.env.PI_FABRIC_RETENTION_RUN_ORDER === "reverse" ? "reversed" : "normal") => {
  const root = tmp("fabric-9160-");
  const registry = path.join(root, "actors", "01a0cd9c-dead-session");
  const actor = path.join(registry, "actor");
  write(path.join(registry, "actors.json"), JSON.stringify({ actors: [{
    id: "actor", rootId: "session:01a0cd9c-dead-session", status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest",
  }] }));
  write(path.join(actor, "session.jsonl"), "live");
  const actorRun = run(path.join(actor, "runs", "latest"), NOW - 9 * DAY);
  const host = path.join(root, "residency", "host");
  write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
  write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:01a0cd9c-dead-session", residencyRoot: host, meshRoot: root }));
  const createResidentRun = (name: "old" | "orphan") => {
    const age = name === "old" ? 8 : 10;
    const candidate = run(path.join(host, "runs", name), NOW - age * DAY);
    fs.utimesSync(candidate, (NOW - age * DAY) / 1_000, (NOW - age * DAY) / 1_000);
    return candidate;
  };
  const runs: Record<"old" | "orphan", string> = { old: "", orphan: "" };
  const creationOrder = runOrder === "normal" ? ["old", "orphan"] as const : ["orphan", "old"] as const;
  for (const name of creationOrder) runs[name] = createResidentRun(name);
  const { old, orphan } = runs;
  const entry = path.join(root, "actors", "s");
  if (kind === "file") fs.writeFileSync(entry, "not a registry"); else fs.mkdirSync(entry);
  return { root, old, orphan, actorRun, entry, away: path.join(tmp("fabric-9160-away-"), "s") };
};
const onStatusRead = (changes: Record<string, () => unknown>) => {
  const original = fs.readFileSync;
  const done = new Set<string>();
  let injected = 0;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    for (const [candidate, change] of Object.entries(changes)) {
      if (!done.has(candidate) && file === path.join(candidate, "status.json")) {
        done.add(candidate);
        if (change() !== false) injected++;
      }
    }
    return (original as (...args: unknown[]) => unknown)(file, ...rest);
  }) as typeof fs.readFileSync);
  return () => injected;
};
const toRegistry = (entry: string, away: string, runId: string) => {
  fs.renameSync(entry, away);
  write(path.join(entry, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle", lastRunId: runId }] }));
};
let shuffleSeedLogged = false;
const shuffleWithSeed = (entries: fs.Dirent[], seed: string) => {
  const parsedSeed = Number(seed);
  let state = (Number.isFinite(parsedSeed) ? Math.trunc(parsedSeed) : [...seed].reduce((hash, char) => Math.imul(hash ^ char.charCodeAt(0), 16777619), 2166136261)) >>> 0;
  state = (state + 0x9e3779b9) >>> 0;
  const random = () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 0x1_0000_0000;
  };
  for (let index = entries.length - 1; index > 0; index--) {
    const swapIndex = Math.floor(random() * (index + 1));
    const current = entries[index]!;
    entries[index] = entries[swapIndex]!;
    entries[swapIndex] = current;
  }
};
const sweep = (root: string) => {
  const opendir = fs.opendirSync as unknown as (...args: unknown[]) => fs.Dir;
  vi.spyOn(fs, "opendirSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const directory = opendir(file, ...rest) as fs.Dir;
    if (String(file) !== path.join(root, "residency", "host", "runs")) return directory;
    const entries: fs.Dirent[] = [];
    let entry: fs.Dirent | null;
    while ((entry = directory.readSync()) !== null) entries.push(entry);
    directory.closeSync();
    entries.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
    const seed = process.env.PI_FABRIC_RETENTION_OPENDIR_SHUFFLE_SEED;
    if (seed !== undefined) {
      if (!shuffleSeedLogged) {
        process.stderr.write(`[retention-entry-identity] opendirSync shuffle seed=${seed}\n`);
        shuffleSeedLogged = true;
      }
      shuffleWithSeed(entries, seed);
    }
    let index = 0;
    return { readSync: () => entries[index++] ?? null, closeSync: () => undefined } as fs.Dir;
  }) as typeof fs.opendirSync);
  return sweepMeshRetention(root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY, residentRunRetentionMs: DAY });
};
const stopped = (root: string) => ({ path: path.join(root, "actors"), reason: expect.stringMatching(/registry roots changed during the sweep/) });
const assertActorRunRetained = (m: ReturnType<typeof mesh>, plan: { removedRuns: string[] }) => {
  expect(plan.removedRuns).not.toContain(m.actorRun);
  expect(fs.existsSync(m.actorRun)).toBe(true);
};

describe.skipIf(process.platform !== "linux")("sweep fences each direct actors/ entry's type and identity (#9160)", () => {
  it.each(["normal", "reversed"] as const)("file becomes a same-named registry referencing the candidate: kept, and the stop is sticky after a restore (%s run creation order)", async runOrder => {
    const m = mesh("file", runOrder);
    const moved = path.join(path.dirname(m.away), "s-dir");
    let replaced = false;
    const replaceThenRestore = (runId: string) => () => {
      if (!replaced) {
        replaced = true;
        toRegistry(m.entry, m.away, runId);
      } else {
        fs.renameSync(m.entry, moved);
        fs.renameSync(m.away, m.entry);
      }
    };
    const injected = onStatusRead({
      [m.old]: replaceThenRestore("old"),
      [m.orphan]: replaceThenRestore("orphan"),
    });
    const plan = await sweep(m.root);
    expect(injected()).toBe(2);
    assertActorRunRetained(m, plan);
    expect(fs.lstatSync(m.entry).isFile()).toBe(true);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.orphan);
    expect(plan.skipped).toContainEqual(stopped(m.root));
  });

  it.each([
    ["a directory replaced by a new directory without actors.json", "directory", (e: string, a: string) => { fs.renameSync(e, a); fs.mkdirSync(e); }],
    ["a file replaced by a new file", "file", (e: string, a: string) => { fs.renameSync(e, a); fs.writeFileSync(e, "not a registry"); }],
    ["a file replaced by a directory without a registry", "file", (e: string, a: string) => { fs.renameSync(e, a); fs.mkdirSync(e); }],
  ] as const)("stops on %s", async (_name, kind, change) => {
    const m = mesh(kind);
    let changed = false;
    const changeFirstCandidate = () => {
      if (changed) return false;
      changed = true;
      change(m.entry, m.away);
    };
    const injected = onStatusRead({ [m.old]: changeFirstCandidate, [m.orphan]: changeFirstCandidate });
    const plan = await sweep(m.root);
    expect(injected()).toBe(1);
    assertActorRunRetained(m, plan);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.orphan);
    expect(plan.skipped).toContainEqual(stopped(m.root));
  });

  it.each(["file", "directory"] as const)("plans eligible runs when an unchanged %s entry stays", async kind => {
    const m = mesh(kind);
    const plan = await sweep(m.root);
    assertActorRunRetained(m, plan);
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
  });

  it("still plans eligible runs when unrelated entries are added during the sweep", async () => {
    const m = mesh("file");
    const injected = onStatusRead({
      [m.old]: () => fs.writeFileSync(path.join(m.root, "actors", "note.txt"), "benign"),
      [m.orphan]: () => fs.mkdirSync(path.join(m.root, "actors", "new-empty")),
    });
    const plan = await sweep(m.root);
    expect(injected()).toBe(2);
    assertActorRunRetained(m, plan);
    expect(plan.removedRuns).toEqual(expect.arrayContaining([m.old, m.orphan]));
    expect(plan.skipped).not.toContainEqual(stopped(m.root));
  });

  // A directory with actors.json swapped in here is also caught by the watched-directory probe; a link to one is not.
  it("stops when the entry is swapped for a link to a registry after the initial discovery walk", async () => {
    const m = mesh("file");
    const actorsRoot = path.join(m.root, "actors");
    const list = fs.readdirSync;
    let walked = false, swapped = false;
    vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === actorsRoot && !swapped) {
        if (new Error().stack?.includes("discover")) walked = true;
        else if (walked) {
          swapped = true;
          write(path.join(m.away, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle", lastRunId: "orphan" }] }));
          fs.renameSync(m.entry, path.join(path.dirname(m.away), "s-file"));
          fs.symlinkSync(m.away, m.entry);
        }
      }
      return (list as (...args: unknown[]) => unknown)(target, ...rest);
    }) as typeof fs.readdirSync);
    const plan = await sweep(m.root);
    expect(swapped).toBe(true);
    assertActorRunRetained(m, plan);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.orphan);
    expect(plan.skipped).toContainEqual(stopped(m.root));
  });
});

it.skipIf(process.platform === "linux")("retains actor and resident reply files outside Linux", async () => {
  const m = mesh("file");
  const plan = await sweep(m.root);
  expect(plan.removedRuns).toEqual([]);
  assertActorRunRetained(m, plan);
  for (const directory of [m.actorRun, m.old, m.orphan]) {
    expect(fs.existsSync(path.join(directory, "reply.json"))).toBe(true);
  }
});
