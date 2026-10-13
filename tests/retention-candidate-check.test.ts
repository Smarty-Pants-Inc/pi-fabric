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
    expect(trace).toEqual(["trigger", "vanish@actors/01a118ab-flicker-session", "reappear+reference"]);
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
  it("keeps the current candidate when an earlier registry is rewritten during a later registry's read, and plans the next", async () => {
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
    expect(plan.removedRuns).not.toContain(m.old);
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
