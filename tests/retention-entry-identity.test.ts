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
/** A dead session registry with two expired candidates (old, then orphan) and an entry actors/s of the given kind. */
const mesh = (kind: "file" | "directory") => {
  const root = tmp("fabric-9160-");
  const registry = path.join(root, "actors", "01a0cd9c-dead-session");
  const actor = path.join(registry, "actor");
  write(path.join(registry, "actors.json"), JSON.stringify({ actors: [{
    id: "actor", rootId: "session:01a0cd9c-dead-session", status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest",
  }] }));
  write(path.join(actor, "session.jsonl"), "live");
  run(path.join(actor, "runs", "latest"), NOW - 9 * DAY);
  const old = run(path.join(actor, "runs", "old"), NOW - 8 * DAY);
  const orphan = run(path.join(registry, "removed-actor", "runs", "orphan"), NOW - 10 * DAY);
  const entry = path.join(root, "actors", "s");
  if (kind === "file") fs.writeFileSync(entry, "not a registry"); else fs.mkdirSync(entry);
  return { root, old, orphan, entry, away: path.join(tmp("fabric-9160-away-"), "s") };
};
const onStatusRead = (changes: Record<string, () => void>) => {
  const original = fs.readFileSync;
  const done = new Set<string>();
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    for (const [candidate, change] of Object.entries(changes)) {
      if (!done.has(candidate) && file === path.join(candidate, "status.json")) { done.add(candidate); change(); }
    }
    return (original as (...args: unknown[]) => unknown)(file, ...rest);
  }) as typeof fs.readFileSync);
  return () => done.size;
};
const toRegistry = (entry: string, away: string, runId: string) => {
  fs.renameSync(entry, away);
  write(path.join(entry, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle", lastRunId: runId }] }));
};
const sweep = (root: string) => sweepMeshRetention(root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });
const stopped = (root: string) => ({ path: path.join(root, "actors"), reason: expect.stringMatching(/registry roots changed during the sweep/) });

describe("sweep fences each direct actors/ entry's type and identity (#9160)", () => {
  it("file becomes a same-named registry referencing the candidate: kept, and the stop is sticky after a restore", async () => {
    const m = mesh("file");
    const moved = path.join(path.dirname(m.away), "s-dir");
    const injected = onStatusRead({
      [m.old]: () => toRegistry(m.entry, m.away, "old"),
      [m.orphan]: () => { fs.renameSync(m.entry, moved); fs.renameSync(m.away, m.entry); },
    });
    const plan = await sweep(m.root);
    expect(injected()).toBe(2);
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
    const injected = onStatusRead({ [m.old]: () => change(m.entry, m.away) });
    const plan = await sweep(m.root);
    expect(injected()).toBe(1);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.orphan);
    expect(plan.skipped).toContainEqual(stopped(m.root));
  });

  it.each(["file", "directory"] as const)("plans eligible runs when an unchanged %s entry stays", async kind => {
    const m = mesh(kind);
    const plan = await sweep(m.root);
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
          write(path.join(m.away, "actors.json"), JSON.stringify({ actors: [{ id: "adopter", status: "idle", lastRunId: "old" }] }));
          fs.renameSync(m.entry, path.join(path.dirname(m.away), "s-file"));
          fs.symlinkSync(m.away, m.entry);
        }
      }
      return (list as (...args: unknown[]) => unknown)(target, ...rest);
    }) as typeof fs.readdirSync);
    const plan = await sweep(m.root);
    expect(swapped).toBe(true);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.skipped).toContainEqual(stopped(m.root));
  });
});
