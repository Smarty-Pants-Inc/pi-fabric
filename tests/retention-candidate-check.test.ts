import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { sweepMeshRetention } from "../src/storage/retention-cli.js";

// smarty-dev#9058: each candidate is decided at its own final check. Every sweep here is a dry run (dryRun: true);
// races are placed exactly with fake lstat/readdir/readFile wrappers over owned temp fixtures. The only real writes
// are fixture writes (a registry gaining a reference) inside this test's own temp root; nothing here renames or
// deletes a product path. Sequences: .local/9058-race-tests.md.
const DAY = 24 * 60 * 60 * 1_000;
const NOW = Date.now();
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const tmp = (prefix: string) => { const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(root); return root; };
const run = (directory: string, status: Record<string, unknown>) => {
  write(path.join(directory, "status.json"), JSON.stringify({ transport: "process", sessionId: "2147483647", ...status }));
  write(path.join(directory, "events.jsonl"), '{"sequence":1}\n');
  write(path.join(directory, "reply.json"), '{"text":"done"}');
  return directory;
};
const registryFile = (rows: unknown[]) => JSON.stringify({ actors: rows });

/** A dead Main's registry: `old` (expired, candidate), `latest` (kept), and a removed actor's expired `orphan`. */
const mesh = () => {
  const root = tmp("fabric-9058-candidate-");
  const actors = path.join(root, "actors");
  const registry = path.join(actors, "01a0cd9c-dead-session");
  const actor = path.join(registry, "actor");
  write(path.join(registry, "actors.json"), registryFile([{
    id: "actor", rootId: "session:01a0cd9c-dead-session", status: "idle", sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest",
  }]));
  write(path.join(actor, "session.jsonl"), "live");
  const old = run(path.join(actor, "runs", "old"), { status: "completed", finishedAt: NOW - 8 * DAY });
  const latest = run(path.join(actor, "runs", "latest"), { status: "completed", finishedAt: NOW - 9 * DAY });
  // Processed after `old` (listings are sorted below): the next, independent candidate.
  const orphan = run(path.join(registry, "removed-actor", "runs", "orphan"), { status: "failed", finishedAt: NOW - 10 * DAY });
  return { root, actors, registry, actor, old, latest, orphan };
};

/** Every file under root with its bytes: the dry run must leave this unchanged except for the test's own writes. */
const snapshot = (root: string): Record<string, string> => {
  const out: Record<string, string> = {};
  const walk = (dir: string) => {
    for (const name of fs.readdirSync(dir).sort()) {
      const file = path.join(dir, name);
      const stat = fs.lstatSync(file);
      if (stat.isDirectory()) walk(file); else out[path.relative(root, file)] = fs.readFileSync(file, "utf8");
    }
  };
  walk(root);
  return out;
};

type Call = (...args: unknown[]) => unknown;
const enoent = (file: string) => Object.assign(new Error(`ENOENT: no such file or directory, lstat '${file}'`), { code: "ENOENT", errno: -2, syscall: "lstat", path: file });

/**
 * Fake fs layer. `onLstat` sees every lstat (path, real call) and decides; readdir results are sorted (a permutation
 * of the real listing, so candidate order is fixed); `onStatusRead` fires once, when the sweep reads the candidate's
 * status.json, which happens before its final check.
 */
const fakeFs = (hooks: {
  candidate: string;
  onStatusRead?: () => void;
  onLstat?: (file: string, real: () => unknown, quiet: boolean) => unknown;
  onReaddir?: (file: string, real: () => unknown) => unknown;
  onReadFile?: (file: string, real: () => unknown) => unknown;
}) => {
  const lstat = fs.lstatSync as unknown as Call, readdir = fs.readdirSync as unknown as Call, readFile = fs.readFileSync as unknown as Call;
  let statusRead = false;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    if (!statusRead && file === path.join(hooks.candidate, "status.json")) { statusRead = true; hooks.onStatusRead?.(); }
    const real = () => readFile(file, ...rest);
    return typeof file === "string" && hooks.onReadFile ? hooks.onReadFile(file, real) : real();
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const quiet = (rest[0] as { throwIfNoEntry?: boolean } | undefined)?.throwIfNoEntry === false;
    const real = () => lstat(file, ...rest);
    return hooks.onLstat ? hooks.onLstat(String(file), real, quiet) : real();
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const real = () => readdir(file, ...rest);
    const names = hooks.onReaddir ? hooks.onReaddir(String(file), real) : real();
    if (!Array.isArray(names)) return names;
    return [...names].sort((a, b) => {
      const x = typeof a === "string" ? a : (a as fs.Dirent).name, y = typeof b === "string" ? b : (b as fs.Dirent).name;
      return x < y ? -1 : x > y ? 1 : 0;
    });
  }) as typeof fs.readdirSync);
  return { statusRead: () => statusRead };
};
/** An lstat that reports the path gone (ENOENT, or undefined for throwIfNoEntry: false). */
const gone = (file: string, quiet: boolean) => { if (quiet) return undefined; throw enoent(file); };
const isAbsent = (real: () => unknown): boolean => {
  try { return real() === undefined; } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return true; throw error; }
};

const sweep = (root: string) => sweepMeshRetention(root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY });

describe("smarty-dev#9058 final candidate check (dry run)", () => {
  // P1 (a): the final check's re-walk meets a session directory that is gone at the walker's lstat; it reappears right
  // AFTER that lstat (before the walker's next filesystem call elsewhere) with an actors.json that references the run.
  // The walk skipped a vanished entry: this candidate cannot be judged unreferenced at this check.
  it("keeps the candidate when a vanished re-walk entry reappears with a referencing actors.json after the walker's lstat", async () => {
    const m = mesh();
    const flicker = path.join(m.actors, "01a118ab-flicker-session");
    write(path.join(flicker, "actors.json"), registryFile([{ id: "other", status: "idle", lastRunId: "its-own-run" }]));
    const before = snapshot(m.root);
    let phase: "idle" | "armed" | "vanished" | "back" = "idle";
    const trace: string[] = [];
    const inFlicker = (file: string) => file === flicker || file.startsWith(flicker + path.sep);
    const reappear = () => {
      phase = "back"; trace.push("reappear+reference");
      write(path.join(flicker, "actors.json"), registryFile([{ id: "adopter", status: "idle", lastRunId: "old" }]));
    };
    const step = (file: string) => {
      // The walker left the vanished entry: it reappears now, with a reference to the candidate.
      if (phase === "vanished" && !inFlicker(file)) reappear();
      if (phase === "armed" && inFlicker(file)) { phase = "vanished"; trace.push(`vanish@${path.relative(m.root, file)}`); }
      return phase === "vanished" && inFlicker(file);
    };
    const fake = fakeFs({
      candidate: m.old,
      // A new, unrelated session forces a re-walk at old's final check.
      onStatusRead: () => { write(path.join(m.actors, "01a118ab-new-session", "actors.json"), registryFile([])); phase = "armed"; trace.push("trigger"); },
      onLstat: (file, real, quiet) => step(file) ? gone(file, quiet) : real(),
      onReaddir: (file, real) => { if (step(file)) throw enoent(file); return real(); },
      onReadFile: (file, real) => { if (step(file)) throw enoent(file); return real(); },
    });
    const plan = await sweep(m.root);
    expect(fake.statusRead()).toBe(true);
    expect(trace).toEqual(["trigger", `vanish@${path.join("actors", "01a118ab-flicker-session")}`, "reappear+reference"]);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.latest);
    // No sticky stop: the next, independent candidate (unreferenced, stable) is still planned.
    expect(plan.removedRuns).toContain(m.orphan);
    expect(snapshot(m.root)).toEqual({
      ...before,
      [path.relative(m.root, path.join(flicker, "actors.json"))]: registryFile([{ id: "adopter", status: "idle", lastRunId: "old" }]),
      [path.join("actors", "01a118ab-new-session", "actors.json")]: registryFile([]),
    });
  });

  // P1 (b): a listed directory without a registry (watched) gains an actors.json that references the run immediately
  // after the re-walk's hasRegistry lstat of <dir>/actors.json returned absent.
  it("keeps the candidate when a listed non-registry directory gains a referencing actors.json right after the walker's probe", async () => {
    const m = mesh();
    const session = path.join(m.actors, "01a118ab-listed-session");
    fs.mkdirSync(session, { recursive: true }); // listed, watched, no registry yet
    const registry = path.join(session, "actors.json");
    const before = snapshot(m.root);
    let phase: "idle" | "armed" | "visiting" | "done" = "idle";
    const trace: string[] = [];
    const fake = fakeFs({
      candidate: m.old,
      onStatusRead: () => { write(path.join(m.actors, "01a118ab-new-session", "actors.json"), registryFile([])); phase = "armed"; trace.push("trigger"); },
      onLstat: (file, real) => {
        // The walker visits the directory (lstat of the directory itself), then probes its actors.json.
        if (phase === "armed" && file === session) { phase = "visiting"; trace.push("walker-lstat-dir"); }
        if (phase === "visiting" && file === registry) {
          let result: unknown, thrown: unknown, absentNow = false;
          try { result = real(); absentNow = result === undefined; } catch (error) { thrown = error; absentNow = (error as NodeJS.ErrnoException).code === "ENOENT"; }
          if (absentNow) {
            phase = "done"; trace.push("probe-absent", "write-reference");
            write(registry, registryFile([{ id: "adopter", status: "idle", lastRunId: "old" }]));
          }
          if (thrown !== undefined) throw thrown;
          return result;
        }
        return real();
      },
    });
    const plan = await sweep(m.root);
    expect(fake.statusRead()).toBe(true);
    expect(trace).toEqual(["trigger", "walker-lstat-dir", "probe-absent", "write-reference"]);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).toContain(m.orphan);
    expect(snapshot(m.root)).toEqual({
      ...before,
      [path.relative(m.root, registry)]: registryFile([{ id: "adopter", status: "idle", lastRunId: "old" }]),
      [path.join("actors", "01a118ab-new-session", "actors.json")]: registryFile([]),
    });
  });

  it("plans the expired run and the later safe candidate when an unrelated session registry appears before the final check", async () => {
    const m = mesh();
    const fake = fakeFs({ candidate: m.old,
      onStatusRead: () => write(path.join(m.actors, "01a118ab-new-session", "actors.json"), registryFile([{ id: "newcomer", status: "idle", lastRunId: "its-own-run" }])) });
    const plan = await sweep(m.root);
    expect(fake.statusRead()).toBe(true);
    expect(plan.removedRuns.sort()).toEqual([m.old, m.orphan].sort());
    expect(fs.existsSync(path.join(m.old, "reply.json"))).toBe(true);
    expect(fs.existsSync(path.join(m.orphan, "reply.json"))).toBe(true);
  });

  it("keeps the run a cross-root registry created before the final check references", async () => {
    const m = mesh();
    const fake = fakeFs({ candidate: m.old,
      onStatusRead: () => write(path.join(m.actors, "01a118ab-new-session", "actors.json"), registryFile([{ id: "adopter", status: "idle", lastRunId: "old" }])) });
    const plan = await sweep(m.root);
    expect(fake.statusRead()).toBe(true);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).toContain(m.orphan);
    expect(fs.existsSync(path.join(m.old, "reply.json"))).toBe(true);
  });

  it("vetoes the candidate when its own actors.json vanishes before the final check", async () => {
    const m = mesh();
    const own = path.join(m.registry, "actors.json");
    const before = snapshot(m.root); // before the fake layer: the snapshot itself reads status.json
    let vanished = false;
    const fake = fakeFs({ candidate: m.old, onStatusRead: () => { vanished = true; },
      onLstat: (file, real, quiet) => vanished && file === own ? gone(file, quiet) : real(),
      onReadFile: (file, real) => { if (vanished && file === own) throw enoent(file); return real(); } });
    // Fail closed either way: the candidate is not planned, or the sweep refuses as a whole (no plan at all).
    const outcome = await sweep(m.root).catch((error: unknown) => ({ removedRuns: [] as string[], error }));
    expect(fake.statusRead()).toBe(true);
    expect(outcome.removedRuns).not.toContain(m.old);
    vi.restoreAllMocks();
    expect(snapshot(m.root)).toEqual(before);
  });

  it("vetoes the candidate when its own actors.json becomes unsafe (hard-linked) before the final check", async () => {
    const m = mesh();
    const own = path.join(m.registry, "actors.json");
    const elsewhere = path.join(tmp("fabric-9058-link-"), "actors.json");
    const fake = fakeFs({ candidate: m.old, onStatusRead: () => fs.linkSync(own, elsewhere) });
    const plan = await sweep(m.root);
    expect(fake.statusRead()).toBe(true);
    expect(fs.lstatSync(own).nlink).toBe(2);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(fs.existsSync(path.join(m.old, "reply.json"))).toBe(true);
  });

  it("guards: the race fixtures order old before orphan and plan both when nothing races", async () => {
    const m = mesh();
    const order: string[] = [];
    fakeFs({ candidate: m.old, onReadFile: (file, real) => {
      if (file.endsWith(`${path.sep}status.json`) && (file.startsWith(m.old) || file.startsWith(m.orphan))) order.push(path.basename(path.dirname(file)));
      return real();
    } });
    const plan = await sweep(m.root);
    expect(order.indexOf("old")).toBeGreaterThanOrEqual(0);
    expect(order.indexOf("old")).toBeLessThan(order.indexOf("orphan"));
    expect(plan.removedRuns.sort()).toEqual([m.old, m.orphan].sort());
  });
});

// Design corrections (.local/9058-design-{opus,astra}.md; counterexamples: .local/9058-candidate-counterexamples.md).
// Still dry runs only; the only real writes are fixture writes inside this test's own temp roots.
const eio = (file: string, syscall: string) => Object.assign(new Error(`EIO: i/o error, ${syscall} '${file}'`), { code: "EIO", errno: -5, syscall, path: file });

describe("smarty-dev#9058 design corrections (dry run)", () => {
  // Correction (a), anti-sticky: one transient final stat/read failure keeps only that candidate. The names never change
  // (no re-walk), so a raw refs.snapshot() poisoned with "*" by the failed fresh() would wrongly keep the next candidate.
  it.each(["lstat", "readFile"] as const)("keeps old but plans the next orphan after one transient final registry %s failure", async kind => {
    const m = mesh();
    const own = path.join(m.registry, "actors.json");
    const rows = [{ id: "actor", rootId: "session:01a0cd9c-dead-session", status: "idle", sessionFile: path.join(m.actor, "session.jsonl"), lastRunId: "latest" }];
    const before = snapshot(m.root);
    let armed = false, failures = 0;
    const fake = fakeFs({
      candidate: m.old,
      onStatusRead: () => {
        armed = true;
        // readFile: a same-rows rewrite with a new size forces a re-parse, so the final check really reads it.
        if (kind === "readFile") write(own, registryFile(rows) + " ");
      },
      onLstat: (file, real) => {
        if (kind === "lstat" && armed && failures === 0 && file === own) { failures++; throw eio(file, "lstat"); }
        return real();
      },
      onReadFile: (file, real) => {
        if (kind === "readFile" && armed && failures === 0 && file === own) { failures++; throw eio(file, "read"); }
        return real();
      },
    });
    const plan = await sweep(m.root);
    expect(fake.statusRead()).toBe(true);
    expect(failures).toBe(1);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.latest);
    expect(plan.removedRuns).toEqual([m.orphan]);
    expect(snapshot(m.root)).toEqual(kind === "readFile" ? { ...before, [path.relative(m.root, own)]: registryFile(rows) + " " } : before);
  });

  // Astra "read-generation race": an earlier registry, already stat'ed by this check, is really rewritten (unrelated
  // content, new size) while a later registry is being read. Its stamp changed inside the check: keep this candidate;
  // the next check adopts the rewrite and the next safe candidate is planned.
  // smarty-dev#976 A: one overlapping rewrite of a valid registry is retried (bounded); the stable recheck plans old.
  it("plans the current candidate after an earlier registry is rewritten once during a later registry's read (stable recheck), and plans the next", async () => {
    const m = mesh();
    const a = path.join(m.registry, "actors.json");
    const b = path.join(m.actors, "01b0-busy-session", "actors.json");
    const aRows = [{ id: "actor", rootId: "session:01a0cd9c-dead-session", status: "idle", sessionFile: path.join(m.actor, "session.jsonl"), lastRunId: "latest" }];
    const bRows = [{ id: "busy", status: "idle", lastRunId: "busy-run" }];
    write(b, registryFile(bRows));
    const before = snapshot(m.root);
    let phase: "idle" | "armed" | "done" = "idle", aSeen = false;
    const trace: string[] = [];
    const fake = fakeFs({
      candidate: m.old,
      // B gets a new size so this check must re-parse it (readFileSync of B happens inside the check).
      onStatusRead: () => { write(b, registryFile(bRows) + " "); phase = "armed"; trace.push("trigger"); },
      onLstat: (file, real) => { if (phase === "armed" && file === a && !aSeen) { aSeen = true; trace.push("a-stat"); } return real(); },
      onReadFile: (file, real) => {
        if (phase === "armed" && file === b) {
          trace.push("b-read");
          if (aSeen) { phase = "done"; trace.push("rewrite-a"); write(a, registryFile(aRows) + "  "); }
        }
        return real();
      },
    });
    const plan = await sweep(m.root);
    expect(fake.statusRead()).toBe(true);
    expect(trace.slice(0, 4)).toEqual(["trigger", "a-stat", "b-read", "rewrite-a"]);
    expect(plan.removedRuns).toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.latest);
    expect(plan.removedRuns).toContain(m.orphan);
    expect(snapshot(m.root)).toEqual({
      ...before,
      [path.relative(m.root, a)]: registryFile(aRows) + "  ",
      [path.relative(m.root, b)]: registryFile(bRows) + " ",
    });
  });

  // P1 (b) on the DEAD RESIDENT path: the resident candidate's final check (shared wrapper) meets a listed non-registry
  // directory that gains an actors.json referencing that resident run right after the walker's probe. The current
  // resident run is kept; the other eligible resident run is still planned; the dead host's flock fence is claimed and
  // released normally (no skip for the root, and the lock is free afterwards).
  it.skipIf(process.platform !== "linux")("keeps the resident candidate on a gain-after-probe race and plans the other resident run", async () => {
    const m = mesh();
    const host = path.join(m.root, "residency", "host");
    write(path.join(host, "host.lock"), JSON.stringify({ pid: 2147483647 }));
    write(path.join(host, "config.json"), JSON.stringify({ format: 1, rootId: "session:01a0cd9c-dead-session", residencyRoot: host, meshRoot: m.root }));
    const residentRuns = ["r-one", "r-two"].map(name => run(path.join(host, "runs", name), { status: "completed", finishedAt: NOW - 3 * DAY }));
    for (const directory of residentRuns) fs.utimesSync(directory, (NOW - 3 * DAY) / 1_000, (NOW - 3 * DAY) / 1_000);
    const session = path.join(m.actors, "01a118ab-listed-session");
    fs.mkdirSync(session, { recursive: true }); // listed, no registry yet
    const registry = path.join(session, "actors.json");
    const before = snapshot(m.root);
    let phase: "idle" | "armed" | "visiting" | "done" = "idle", chosen: string | undefined;
    const trace: string[] = [];
    fakeFs({
      candidate: path.join(m.root, "no-such-candidate"),
      onReadFile: (file, real) => {
        // The first resident candidate whose status.json is read is the raced one (opendir order is not fixed).
        if (phase === "idle" && path.basename(file) === "status.json" && residentRuns.includes(path.dirname(file))) {
          chosen = path.dirname(file); phase = "armed"; trace.push("trigger");
          write(path.join(m.actors, "01a118ab-new-session", "actors.json"), registryFile([]));
        }
        return real();
      },
      onLstat: (file, real) => {
        if (phase === "armed" && file === session) { phase = "visiting"; trace.push("walker-lstat-dir"); }
        if (phase === "visiting" && file === registry && isAbsent(real)) {
          phase = "done"; trace.push("probe-absent", "write-reference");
          write(registry, registryFile([{ id: "adopter", status: "idle", lastRunId: path.basename(chosen!) }]));
        }
        return real();
      },
    });
    const plan = await sweepMeshRetention(m.root, { now: NOW, dryRun: true, runRetentionMs: 7 * DAY, residentRunRetentionMs: DAY });
    expect(trace).toEqual(["trigger", "walker-lstat-dir", "probe-absent", "write-reference"]);
    const other = residentRuns.find(directory => directory !== chosen)!;
    expect(plan.removedRuns).not.toContain(chosen);
    expect(plan.removedRuns).toContain(other);
    expect(plan.removedRuns).toContain(m.orphan);
    expect(plan.skipped.filter(entry => entry.path === host)).toEqual([]);
    vi.restoreAllMocks();
    expect(snapshot(m.root)).toEqual({
      ...before,
      [path.relative(m.root, registry)]: registryFile([{ id: "adopter", status: "idle", lastRunId: path.basename(chosen!) }]),
      [path.join("actors", "01a118ab-new-session", "actors.json")]: registryFile([]),
    });
    // The fence was released: a waking host takes the lock at once.
    const { lockFile } = await import("../src/residency/file-lock.js");
    fs.closeSync(await lockFile(path.join(host, "host.lock"), 0));
  });
});

// CODE-P1 (.local/9058-final-audit.md): the default project registry actors/actors.json is saved by ActorRegistryStore:
// prepare() stages `actors.json.<pid>.<uuid>.prepared` in actors/, and the synchronous commit() publishes it with
// renameAtomic, so every normal save is a new inode generation. Such a save must not sticky-stop later candidates.
// The REAL store prepare/commit runs on this test's private temp mesh only (prepared before any fs spy); no product
// path, no retention delete, dry runs only.
describe("smarty-dev#9058 project registry atomic save (dry run)", () => {
  const projectStore = async (m: ReturnType<typeof mesh>) => {
    const { ActorRegistryStore } = await import("../src/actors/registry-store.js");
    const store = new ActorRegistryStore(m.actors);
    store.prepare([{ id: "project-actor", status: "idle", lastRunId: "its-own-run" }]).commit();
    return store;
  };
  const ino = (file: string) => fs.lstatSync(file).ino;
  // Windows has no uid/mode, so a store save is a sticky replacement there: planning stops (fail closed; omitting the
  // orphan is safe). POSIX accepts the safe generation and still plans the orphan. smarty-dev#7858
  const atomicSavePlan = (m: ReturnType<typeof mesh>) => process.platform === "win32" ? [] : [m.orphan];

  // smarty-dev#976 A: an unrelated POSIX save overlapping old's check is retried; the stable recheck plans old too.
  it("plans old after a normal store save overlaps its final check and a stable recheck, then plans the orphan", async () => {
    const m = mesh();
    const project = path.join(m.actors, "actors.json"), own = path.join(m.registry, "actors.json");
    const store = await projectStore(m);
    const prepared = store.prepare([{ id: "project-actor", status: "idle", lastRunId: "another-run" }]);
    const inoBefore = ino(project);
    let armed = false, committed = false;
    // Overlap: commit at old's ownerBefore lstat (the pre-observation already stamped the project file; the read follows).
    const fake = fakeFs({ candidate: m.old, onStatusRead: () => { armed = true; },
      onLstat: (file, real) => {
        if (armed && !committed && file === own && !(new Error().stack ?? "").includes("observe")) { committed = true; prepared.commit(); }
        return real();
      } });
    const plan = await sweep(m.root);
    vi.restoreAllMocks();
    expect(fake.statusRead()).toBe(true);
    expect(committed).toBe(true);
    expect(ino(project)).not.toBe(inoBefore);
    expect(plan.removedRuns.slice().sort()).toEqual(process.platform === "win32" ? [] : [m.old, m.orphan].sort());
    expect(fs.existsSync(path.join(m.old, "reply.json"))).toBe(true);
  });

  it("keeps old referenced by a cross-root store save and plans the orphan", async () => {
    const m = mesh();
    const project = path.join(m.actors, "actors.json");
    const store = await projectStore(m);
    const prepared = store.prepare([{ id: "adopter", status: "idle", lastRunId: "old" }]);
    const inoBefore = ino(project);
    const fake = fakeFs({ candidate: m.old, onStatusRead: () => prepared.commit() });
    const plan = await sweep(m.root);
    vi.restoreAllMocks();
    expect(fake.statusRead()).toBe(true);
    expect(ino(project)).not.toBe(inoBefore);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).toEqual(atomicSavePlan(m));
    expect(fs.existsSync(path.join(m.old, "reply.json"))).toBe(true);
  });

  for (const kind of ["directory", "symlink", "hardlink", "foreign-uid", "writable-by-others"] as const) {
  // foreign-uid / writable-by-others need POSIX uid and mode bits (no process.getuid on Windows; smarty-dev#7858).
  it.skipIf((kind === "foreign-uid" || kind === "writable-by-others") && typeof process.getuid !== "function")(`project actors.json becoming ${kind} permanently keeps both`, async () => {
    const m = mesh();
    const project = path.join(m.actors, "actors.json");
    await projectStore(m);
    let flipped = false;
    const fake = fakeFs({ candidate: m.old,
      onStatusRead: () => {
        flipped = true;
        if (kind === "foreign-uid") return;
        if (kind === "writable-by-others") { fs.chmodSync(project, 0o666); return; }
        const saved = path.join(tmp("fabric-9058-project-"), "actors.json");
        if (kind === "hardlink") { fs.linkSync(project, saved); return; }
        fs.renameSync(project, saved);
        if (kind === "directory") fs.mkdirSync(project); else fs.symlinkSync(saved, project);
      },
      onLstat: (file, real) => {
        const stat = real();
        if (kind === "foreign-uid" && flipped && file === project && stat) return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: (stat as fs.Stats).uid + 1 });
        return stat;
      } });
    const outcome = await sweep(m.root).catch((error: unknown) => ({ removedRuns: [] as string[], error }));
    expect(fake.statusRead()).toBe(true);
    expect(outcome.removedRuns).toEqual([]);
    expect(fs.existsSync(path.join(m.old, "reply.json"))).toBe(true);
    expect(fs.existsSync(path.join(m.orphan, "reply.json"))).toBe(true);
  });
  }
});

// smarty-dev#976 E1 (.local/976-e1-red-tests.md): the live Ryzen 1 dry run planned none of 8906 expired unreferenced
// runs: each was vetoed at its final check, and the sweep was sticky-stopped by "an actors entry changed type or
// identity". RED-first tests on the current source, dry runs only. The only real writes are this test's own fixture
// writes: REAL ActorRegistryStore prepare()/commit() saves and withLock() lock generations on its private temp mesh.
// Every fs.rmSync/rmdirSync/unlinkSync is intercepted and recorded (never executed, #4664); the one removal the native
// writer performs here, withLock's release `fs.rmSync(<actors>/actors.json.lock, { recursive: true })`, is modelled
// by RENAMING that exact private fixture lock directory to another owned private temp path (namespace vacancy only).
// No child process may start. prepared.dispose() is never called.
const rcFrames = (): string[] => {
  const limit = Error.stackTraceLimit; Error.stackTraceLimit = 80;
  const stack = new Error().stack ?? ""; Error.stackTraceLimit = limit;
  return stack.split("\n").filter(line => line.includes("retention-cli")).map(line => /^\s*at (?:async )?([^\s(]+)/.exec(line)?.[1] ?? "?");
};
/** checkedFresh's own-registry stamp (ownerBefore, or the stable post-read compare): one per bracket start/stable end. */
const isOwnerStamp = (): boolean => { const frames = rcFrames(); return frames[0] === "stampOf" && frames[1] === "checkedFresh"; };

// Linux-only: these native Ryzen 1 private-POSIX-lock, no-spawn fixtures do not model the Windows helper's legitimate
// spawn (smarty-dev#7858). The original cross-platform contract tests above are unchanged.
describe.skipIf(process.platform !== "linux")("E1 live mesh: transient registry rewrite and lock generation (dry run)", () => {
  type Removal = { call: string; path: string; action: "moved" | "blocked" | "absent"; to?: string };
  /** Records every removal call and never executes one; `lockPaths` (private fixture lock dirs) are moved aside.
   * "absent": the target did not exist (writeFileAtomic's post-rename `rmSync(temp, { force: true })` no-op). */
  const guardRemovals = (lockPaths: string[] = []) => {
    const realRename = fs.renameSync.bind(fs);
    const removals: Removal[] = [], spawned: string[] = [];
    for (const call of ["rmSync", "rmdirSync", "unlinkSync"] as const) {
      vi.spyOn(fs, call).mockImplementation(((target: fs.PathLike) => {
        const file = String(target);
        if (call === "rmSync" && lockPaths.includes(file) && fs.existsSync(file)) {
          const to = path.join(tmp("fabric-976-e1-lockgen-"), "released.lock");
          realRename(file, to); removals.push({ call, path: file, action: "moved", to }); return;
        }
        removals.push({ call, path: file, action: fs.existsSync(file) ? "blocked" : "absent" });
      }) as never);
    }
    for (const call of ["spawn", "spawnSync", "execFile", "execFileSync", "exec", "execSync", "fork"] as const) {
      vi.spyOn(childProcess, call).mockImplementation(((command: unknown) => { spawned.push(String(command)); throw new Error("E1 test: no child processes"); }) as never);
    }
    // What a test asserts empty: every removal that would have touched an existing path, and every spawn.
    const effective = () => ({ removals: removals.filter(removal => removal.action !== "absent"), spawned });
    return { removals, spawned, effective };
  };
  const liveRows = (lastRunId: string, extra: Record<string, unknown>[] = []) => [{ id: "busy", status: "idle", lastRunId }, ...extra];
  /** A live session's registry actors/01b0-live-session/actors.json, written by the REAL store (fixture only). */
  const liveStore = async (m: ReturnType<typeof mesh>) => {
    const { ActorRegistryStore } = await import("../src/actors/registry-store.js");
    const directory = path.join(m.actors, "01b0-live-session");
    fs.mkdirSync(directory, { recursive: true });
    const store = new ActorRegistryStore(directory);
    store.prepare(liveRows("busy-run")).commit();
    return { store, file: path.join(directory, "actors.json") };
  };
  /** Trace: first status.json read of old/orphan, and every checkedFresh owner stamp of the dead Main's registry. */
  const tracer = (m: ReturnType<typeof mesh>, onStamp: () => void) => {
    const own = path.join(m.registry, "actors.json"), trace: string[] = [];
    let writing = false;
    const fixtureWrite = (label: string, write: () => void) => { writing = true; try { write(); trace.push(label); } finally { writing = false; } };
    fakeFs({ candidate: m.old,
      onReadFile: (file, real) => {
        for (const [name, directory] of [["old", m.old], ["orphan", m.orphan]] as const)
          if (!writing && file === path.join(directory, "status.json") && !trace.includes(`status:${name}`)) trace.push(`status:${name}`);
        return real();
      },
      onLstat: (file, real) => {
        if (!writing && file === own && trace.includes("status:old") && isOwnerStamp()) { trace.push("stamp"); onStamp(); }
        return real();
      } });
    return { trace, fixtureWrite };
  };
  const between = (trace: string[], from: string, to: string) =>
    trace.slice(trace.indexOf(from) + 1, trace.includes(to) ? trace.indexOf(to) : undefined).filter(event => event === "stamp").length;
  const runsPresent = (m: ReturnType<typeof mesh>) => [m.old, m.latest, m.orphan].every(run => fs.existsSync(path.join(run, "reply.json")));

  // RED (E1 progress): an unrelated live actor's lastRunId moves on in ONE real store save that overlaps old's first
  // bracket; the registry is stable afterwards. The current candidate must be rechecked (bounded) and planned, not only
  // the later orphan. Current source: old is vetoed once and never rechecked.
  it("RED: plans the current candidate after one overlapping unrelated save and a stable recheck", async () => {
    const m = mesh();
    const live = await liveStore(m);
    const guard = guardRemovals();
    let saves = 0;
    const { trace, fixtureWrite } = tracer(m, () => {
      if (saves++ === 0) fixtureWrite("save:unrelated", () => live.store.prepare(liveRows("busy-run-2")).commit());
    });
    const plan = await sweep(m.root);
    expect(trace.slice(0, 3)).toEqual(["status:old", "stamp", "save:unrelated"]); // fixture: the save is inside old's first bracket
    expect(fs.readFileSync(live.file, "utf8")).toContain("busy-run-2");
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
    expect(plan.removedRuns).not.toContain(m.latest);
    expect(plan.removedRuns.slice().sort()).toEqual([m.old, m.orphan].sort());
    // A second, stable bracket for old (before + after stamps) before the orphan is considered.
    expect(between(trace, "save:unrelated", "status:orphan")).toBeGreaterThanOrEqual(2);
  });

  // Positive control: the overlapping real save ADDS a cross-root reference to old. Any recheck must see it: old kept.
  it("control: keeps old when the overlapping save adds a cross-root reference, and plans the orphan", async () => {
    const m = mesh();
    const live = await liveStore(m);
    const guard = guardRemovals();
    let saves = 0;
    const { trace, fixtureWrite } = tracer(m, () => {
      if (saves++ === 0) fixtureWrite("save:cross-root-ref", () => live.store.prepare(liveRows("busy-run-2", [{ id: "adopter", status: "idle", lastRunId: "old" }])).commit());
    });
    const plan = await sweep(m.root);
    expect(trace.slice(0, 3)).toEqual(["status:old", "stamp", "save:cross-root-ref"]);
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
    expect(plan.removedRuns).toEqual([m.orphan]);
  });

  // Counterexample: the registry changes inside EVERY bracket. Rechecks must be bounded, then the candidate is kept.
  it("counterexample: a continually rewritten registry gets bounded rechecks, then old (and orphan) are kept", async () => {
    const m = mesh();
    const live = await liveStore(m);
    const guard = guardRemovals();
    let saves = 0;
    const { trace, fixtureWrite } = tracer(m, () => {
      saves++; fixtureWrite(`save:${saves}`, () => live.store.prepare(liveRows(`busy-run-${saves + 1}`)).commit());
    });
    const plan = await sweep(m.root);
    expect(trace.slice(0, 3)).toEqual(["status:old", "stamp", "save:1"]);
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
    expect(plan.removedRuns).toEqual([]);
    // Exactly 3 complete attempts for old: before + after owner stamp each (6), then old is kept.
    expect(between(trace, "status:old", "status:orphan")).toBe(6);
    // At most 3 attempts each for old and orphan: one save per stamp.
    expect(saves).toBeLessThanOrEqual(12);
  });

  // Counterexample: the first bracket is disturbed by an unrelated save; the NEXT bracket (a recheck of old, or the
  // orphan's on the current source) publishes a cross-root reference to old. old must be kept either way.
  it("counterexample: a cross-root reference added on the later check keeps old", async () => {
    const m = mesh();
    const live = await liveStore(m);
    const guard = guardRemovals();
    let saves = 0;
    const { trace, fixtureWrite } = tracer(m, () => {
      saves++;
      if (saves === 1) fixtureWrite("save:unrelated", () => live.store.prepare(liveRows("busy-run-2")).commit());
      if (saves === 2) fixtureWrite("save:cross-root-ref", () => live.store.prepare(liveRows("busy-run-2", [{ id: "adopter", status: "idle", lastRunId: "old" }])).commit());
    });
    const plan = await sweep(m.root);
    expect(trace.slice(0, 3)).toEqual(["status:old", "stamp", "save:unrelated"]);
    expect(trace).toContain("save:cross-root-ref");
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.latest);
  });

  // Counterexample: with the unrelated save, the candidate's OWN registry goes missing (moved to a private path) and
  // stays missing: unsafe, so every recheck keeps old; the orphan (same owner) is kept too.
  it("counterexample: its own registry missing after an overlapping save still keeps old", async () => {
    const m = mesh();
    const live = await liveStore(m);
    const own = path.join(m.registry, "actors.json"), aside = path.join(tmp("fabric-976-e1-own-"), "actors.json");
    const guard = guardRemovals();
    let saves = 0;
    const { trace, fixtureWrite } = tracer(m, () => {
      if (saves++ === 0) fixtureWrite("save:unrelated+own-missing", () => { live.store.prepare(liveRows("busy-run-2")).commit(); fs.renameSync(own, aside); });
    });
    const outcome = await sweep(m.root).catch((error: unknown) => ({ removedRuns: [] as string[], error }));
    expect(trace.slice(0, 3)).toEqual(["status:old", "stamp", "save:unrelated+own-missing"]);
    expect(fs.existsSync(own)).toBe(false);
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
    expect(outcome.removedRuns).toEqual([]);
  });

  // RED (sticky stop): the normal project store's withLock() (the native writer) mkdirs actors/actors.json.lock (0700)
  // and writes its `owner`, runs the operation, then releases with fs.rmSync of that directory; the next writer's
  // withLock mkdirs the same name as a NEW directory inode. The sweep lists generation 1 (held at start); it is
  // released while the sweep awaits (a residency entry without host.lock: claimFence refuses before any flock/spawn);
  // generation 2 is taken at old's status read. A lock generation is not a registry replacement: no sticky stop.
  it.skipIf(process.platform !== "linux")("RED: a project-store withLock lock-dir generation during the sweep does not sticky-stop planning", async () => {
    const m = mesh();
    const { ActorRegistryStore } = await import("../src/actors/registry-store.js");
    const project = new ActorRegistryStore(m.actors);
    project.prepare([{ id: "project-actor", status: "idle", lastRunId: "its-own-run" }]).commit();
    const lock = path.join(m.actors, "actors.json.lock");
    const noHost = path.join(m.root, "residency", "no-host");
    fs.mkdirSync(noHost, { recursive: true });
    const guard = guardRemovals([lock]);
    const trace: string[] = [];
    let releaseA!: () => void, releaseB!: () => void;
    const heldA = new Promise<void>(resolve => { releaseA = resolve; });
    const heldB = new Promise<void>(resolve => { releaseB = resolve; });
    const lockA = project.withLock(() => heldA); // generation 1, held when the sweep starts
    const ino1 = fs.lstatSync(lock).ino;
    trace.push("gen1:held");
    let lockB: Promise<void> | undefined, ino2: number | undefined;
    fakeFs({ candidate: m.old,
      onStatusRead: () => {
        lockB = project.withLock(() => heldB); // generation 2: same name, new inode
        ino2 = fs.lstatSync(lock).ino; trace.push("status:old", "gen2:held");
      },
      onLstat: (file, real) => {
        if (file === lock && trace.at(-1) === "gen1:held") trace.push("sweep-listed-gen1");
        if (file === path.join(noHost, "host.lock") && !trace.includes("gen1:release")) { trace.push("gen1:release"); releaseA(); }
        return real();
      } });
    const plan = await sweep(m.root);
    releaseB(); await lockA; await lockB;
    expect(trace).toEqual(["gen1:held", "sweep-listed-gen1", "gen1:release", "status:old", "gen2:held"]);
    expect(ino2).toBeDefined();
    expect(ino2).not.toBe(ino1);
    // Both releases were the native rmSync of the private fixture lock, moved aside; nothing else was removed.
    expect(guard.effective().removals.map(({ call, path: file, action }) => ({ call, file, action }))).toEqual([
      { call: "rmSync", file: lock, action: "moved" }, { call: "rmSync", file: lock, action: "moved" }]);
    expect(guard.spawned).toEqual([]);
    expect(runsPresent(m)).toBe(true);
    expect(plan.skipped.map(entry => entry.reason).filter(reason => reason.includes("changed type or identity"))).toEqual([]);
    expect(plan.removedRuns.slice().sort()).toEqual([m.old, m.orphan].sort());
  });

  // A concrete reference observed on a REJECTED (overlapping) attempt is kept for the invocation even when it is
  // removed again before the stable third attempt.
  it("keeps old when a cross-root reference seen on a rejected overlap is removed before the stable third attempt", async () => {
    const m = mesh();
    const live = await liveStore(m);
    const guard = guardRemovals();
    let saves = 0;
    const { trace, fixtureWrite } = tracer(m, () => {
      saves++;
      if (saves === 1) fixtureWrite("save:add-ref", () => live.store.prepare(liveRows("busy-run-2", [{ id: "adopter", status: "idle", lastRunId: "old" }])).commit());
      if (saves === 3) fixtureWrite("save:drop-ref", () => live.store.prepare(liveRows("busy-run-3")).commit());
    });
    const plan = await sweep(m.root);
    expect(trace.slice(0, 3)).toEqual(["status:old", "stamp", "save:add-ref"]);
    expect(trace).toContain("save:drop-ref");
    expect(fs.readFileSync(live.file, "utf8")).not.toContain("\"old\"");
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
    expect(plan.removedRuns).not.toContain(m.old);
    expect(plan.removedRuns).not.toContain(m.latest);
  });

  // B1: an empty/torn lock owner (private 0700 dir, private empty `owner`) appearing after the first listing is
  // transient UNKNOWN: old is kept, nothing is seeded or poisoned, and once the name is vacant the orphan is planned.
  it.each(["", "torn", "x".repeat(257)].flatMap(content => [false, true].map(recognised => ({ content, recognised }))))(
    "keeps old on an incomplete-owner lock generation (recognised=$recognised), records no identity, and plans the later orphan", async ({ content, recognised }) => {
    const m = mesh();
    const lock = path.join(m.actors, "actors.json.lock");
    if (recognised) {
      fs.mkdirSync(lock, { mode: 0o700 });
      fs.writeFileSync(path.join(lock, "owner"), `00000000-0000-4000-8000-000000000000\n${process.pid}\n${NOW}\n`, { mode: 0o600 });
    }
    const guard = guardRemovals();
    const trace: string[] = [];
    fakeFs({ candidate: m.old,
      onStatusRead: () => { if (!recognised) fs.mkdirSync(lock, { mode: 0o700 }); fs.writeFileSync(path.join(lock, "owner"), content, { mode: 0o600 }); trace.push("torn:held"); },
      onReadFile: (file, real) => {
        if (file === path.join(m.orphan, "status.json") && !trace.includes("torn:valid")) {
          fs.writeFileSync(path.join(lock, "owner"), `00000000-0000-4000-8000-000000000000\n${process.pid}\n${NOW}\n`, { mode: 0o600 }); trace.push("torn:valid");
        }
        return real();
      } });
    const plan = await sweep(m.root);
    expect(trace).toEqual(["torn:held", "torn:valid"]);
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
    expect(plan.skipped.map(entry => entry.reason).filter(reason => reason.includes("changed type or identity"))).toEqual([]);
    expect(plan.removedRuns).toEqual([m.orphan]);
  });

  // Physically unsafe shapes of a RECOGNISED lock generation are a permanent stop, even when restored afterwards.
  // Mutated after the sweep listed the valid (withLock-held) generation, restored when the orphan is considered.
  type Unsafe = { name: string; mutate: (lock: string) => () => void; fakeUid?: boolean };
  const unsafe: Unsafe[] = [
    { name: "group/other permission", mutate: lock => { fs.chmodSync(lock, 0o755); return () => fs.chmodSync(lock, 0o700); } },
    { name: "extra actors.json child", mutate: lock => { const extra = path.join(lock, "actors.json"), aside = path.join(tmp("fabric-976-e1-extra-"), "a");
      fs.writeFileSync(extra, "{}", { mode: 0o600 }); return () => fs.renameSync(extra, aside); } },
    { name: "hard-linked owner", mutate: lock => { const owner = path.join(lock, "owner"), link = path.join(tmp("fabric-976-e1-link-"), "owner");
      const bytes = fs.readFileSync(owner); fs.linkSync(owner, link);
      return () => { const fresh = path.join(tmp("fabric-976-e1-link-"), "fresh"); fs.writeFileSync(fresh, bytes, { mode: 0o600 }); fs.renameSync(fresh, owner); }; } },
    { name: "foreign uid", fakeUid: true, mutate: () => () => undefined },
    { name: "type (directory -> file)", mutate: lock => { const aside = path.join(tmp("fabric-976-e1-type-"), "lock");
      fs.renameSync(lock, aside); fs.writeFileSync(lock, "", { mode: 0o600 });
      return () => { fs.renameSync(lock, path.join(tmp("fabric-976-e1-type-"), "file")); fs.renameSync(aside, lock); }; } },
  ];
  it.each(unsafe)("permanently stops on a recognised lock turned physically unsafe ($name), naming offender and identities", async ({ mutate, fakeUid }) => {
    const m = mesh();
    const { ActorRegistryStore } = await import("../src/actors/registry-store.js");
    const project = new ActorRegistryStore(m.actors);
    project.prepare([{ id: "project-actor", status: "idle", lastRunId: "its-own-run" }]).commit();
    const lock = path.join(m.actors, "actors.json.lock");
    let release!: () => void;
    const held = project.withLock(() => new Promise<void>(resolve => { release = resolve; }));
    expect(fs.lstatSync(lock).isDirectory()).toBe(true);
    const guard = guardRemovals([lock]);
    let restore: (() => void) | undefined, mutated = false, restored = false;
    fakeFs({ candidate: m.old,
      onStatusRead: () => { restore = mutate(lock); mutated = true; },
      onReadFile: (file, real) => {
        if (file === path.join(m.orphan, "status.json") && restore && !restored) { restore(); restored = true; }
        return real();
      },
      onLstat: (file, real) => {
        const stat = real();
        if (fakeUid && mutated && !restored && file === lock && stat instanceof fs.Stats)
          return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: stat.uid + 1 });
        return stat;
      } });
    const plan = await sweep(m.root);
    if (restore && !restored) { restore(); restored = true; }
    release(); await held;
    expect(mutated).toBe(true);
    expect(guard.spawned).toEqual([]);
    expect(guard.effective().removals.filter(removal => removal.action !== "moved")).toEqual([]);
    expect(runsPresent(m)).toBe(true);
    expect(plan.removedRuns).toEqual([]);
    const stops = plan.skipped.map(entry => entry.reason).filter(reason => reason.includes("changed type or identity"));
    expect(stops.length).toBeGreaterThan(0);
    expect(stops[0]).toMatch(/actors\.json\.lock .+ -> .+/);
  });

  it("refuses a first listing with an incomplete private lock owner without planning", async () => {
    const m = mesh(), lock = path.join(m.actors, "actors.json.lock");
    fs.mkdirSync(lock, { mode: 0o700 }); fs.writeFileSync(path.join(lock, "owner"), "", { mode: 0o600 });
    const guard = guardRemovals();
    await expect(sweep(m.root)).rejects.toThrow("Actor references are uncertain");
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
  });

  // SEC-01 (.local/976-e1-delta-audit.md), RECORDED probes: a private 0700 lock with a 0600 owner present BEFORE the first
  // listing whose record is not exactly the store's grammar is UNKNOWN, so the sweep refuses its initial uncertainty.
  // (a) short read: the file holds a valid prefix plus junk; lockIdentity's one readSync returns ONLY the prefix bytes.
  const PREFIX = `00000000-0000-4000-8000-000000000000\n${process.pid}\n${NOW}\n`;
  it.each([
    { name: "(a) short read of valid prefix + trailing junk", owner: `${PREFIX}junk`, short: true },
    { name: "(b) full read with trailing junk (control)", owner: `${PREFIX}junk`, short: false },
    { name: "(c) 36-hyphen token", owner: `${"-".repeat(36)}\n${process.pid}\n${NOW}\n`, short: false },
    { name: "(d) overflow pid", owner: `00000000-0000-4000-8000-000000000000\n${"9".repeat(40)}\n${NOW}\n`, short: false },
    { name: "(e) overflow time", owner: `00000000-0000-4000-8000-000000000000\n${process.pid}\n${"9".repeat(40)}\n`, short: false },
    { name: "(f) bad start grammar", owner: `${PREFIX}7x\n`, short: false },
  ])("SEC-01 probe: refuses an initial lock owner that is not the exact store record: $name", async ({ owner, short }) => {
    const m = mesh(), lock = path.join(m.actors, "actors.json.lock"), ownerFile = path.join(lock, "owner");
    fs.mkdirSync(lock, { mode: 0o700 }); fs.writeFileSync(ownerFile, owner, { mode: 0o600 });
    expect(Buffer.byteLength(owner)).toBeLessThanOrEqual(256);
    const guard = guardRemovals(), realRead = fs.readSync as unknown as Call, shortReads: { got: unknown; size: number }[] = [];
    vi.spyOn(fs, "readSync").mockImplementation(((fd: number, buffer: Buffer, ...rest: unknown[]) => {
      if (!short || shortReads.length || !rcFrames().includes("lockIdentity")) return realRead(fd, buffer, ...rest);
      const got = realRead(fd, buffer, 0, Buffer.byteLength(PREFIX), 0);
      shortReads.push({ got, size: fs.fstatSync(fd).size }); return got;
    }) as never);
    await expect(sweep(m.root)).rejects.toThrow("Actor references are uncertain");
    expect(shortReads).toEqual(short ? [{ got: Buffer.byteLength(PREFIX), size: Buffer.byteLength(owner) }] : []);
    expect(fs.readFileSync(ownerFile, "utf8")).toBe(owner);
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(runsPresent(m)).toBe(true);
  });

  it("keeps a candidate when the lock's raw directory generation changes during classification", async () => {
    const m = mesh(), lock = path.join(m.actors, "actors.json.lock");
    fs.mkdirSync(lock, { mode: 0o700 });
    fs.writeFileSync(path.join(lock, "owner"), `00000000-0000-4000-8000-000000000000\n${process.pid}\n${NOW}\n`, { mode: 0o600 });
    const guard = guardRemovals(); let armed = false, raced = false;
    fakeFs({ candidate: m.old, onStatusRead: () => { armed = true; }, onLstat: (file, real) => {
      const stat = real();
      if (armed && !raced && file === lock && (new Error().stack ?? "").includes("lockIdentity")) {
        raced = true;
        return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { ino: (stat as fs.Stats).ino + 1 });
      }
      return stat;
    } });
    const plan = await sweep(m.root);
    expect(raced).toBe(true); expect(plan.removedRuns).toEqual([m.orphan]);
    expect(guard.effective()).toEqual({ removals: [], spawned: [] });
    expect(plan.skipped.some(item => item.reason.includes("changed type or identity"))).toBe(false);
    expect(runsPresent(m)).toBe(true);
  });

  // CODE-02 (.local/976-e1-delta-audit.md), RECORDED MODEL only (no native Windows claim): inside ownerSafe alone, the own
  // actors.json reports mode 0666 and, when noUid, process.getuid is absent. Elsewhere getuid and lstat are real.
  it.each([false, true])("recorded mode probe: an own 0666 actors.json inside ownerSafe (noUid=%s)", async noUid => {
    const m = mesh(), own = path.join(m.registry, "actors.json");
    const original = Object.getOwnPropertyDescriptor(process, "getuid")!, realGetuid = process.getuid;
    const inOwnerSafe = () => rcFrames().includes("ownerSafe");
    let hidden = 0, modes = 0;
    try {
      if (noUid) Object.defineProperty(process, "getuid", { configurable: true, enumerable: original.enumerable,
        get: () => { if (!inOwnerSafe()) return realGetuid; hidden++; return undefined; } });
      const guard = guardRemovals();
      fakeFs({ candidate: m.old, onLstat: (file, real) => {
        const stat = real();
        if (file !== own || !(stat instanceof fs.Stats) || !inOwnerSafe()) return stat;
        modes++; return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { mode: (stat.mode & ~0o777) | 0o666 });
      } });
      const plan = await sweep(m.root);
      expect(modes).toBeGreaterThan(0);
      expect(hidden > 0).toBe(noUid);
      expect(guard.effective()).toEqual({ removals: [], spawned: [] });
      expect(runsPresent(m)).toBe(true);
      expect(plan.removedRuns).not.toContain(m.latest);
      expect(plan.removedRuns.slice().sort()).toEqual(noUid ? [m.old, m.orphan].sort() : []);
    } finally { Object.defineProperty(process, "getuid", original); }
    expect(Object.getOwnPropertyDescriptor(process, "getuid")).toEqual(original);
  });
});
