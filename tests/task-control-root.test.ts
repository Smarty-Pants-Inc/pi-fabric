import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  TASK_CONTROL_LAYOUT, assertTaskControlWriteRoots, createArchiveTaskControlDirectory, createHostTaskRun, ensureTaskControlRoot, initializeTaskControlDirectory,
  isTaskControlDirectoryEntry, legacyTaskControlSibling, openLegacyTaskControlFiles, readFinalAnswerReceipt, saveFinalAnswerReceipt, taskControlDirectory,
  taskControlReadDirectory, taskControlRoot, writeTaskControlJson,
} from "../src/worker/terminal-answer.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { resolveChildWritePolicy } from "../src/agents/child-env.js";
import { queuedTerminalControls } from "../src/worker/terminal-controls.js";
import { appendTerminalAdmission, readTerminalAdmissions } from "../src/agents/terminal-notices.js";

// The parent supplies an isolated XDG_STATE_HOME outside TMPDIR. Fixtures are
// retained; nothing is deleted. Without that isolation, never touch real state.
const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usable = process.platform !== "win32" && !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const original = process.env.XDG_STATE_HOME;
afterEach(() => { process.env.XDG_STATE_HOME = original; });

const stateCase = () => fs.mkdtempSync(path.join(isolatedState!, "control-root-case-"));
const runFixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "task-control-root-"));
  const run = path.join(root, "run"), workspace = path.join(root, "workspace");
  // Option 2: the run path stays ABSENT so createHostTaskRun owns its exclusive mkdir.
  // Raw/unknown cases mkdir the run explicitly themselves (rawRun) before writing history.
  fs.mkdirSync(workspace, { mode: 0o700 });
  return { root, run, workspace };
};
const rawRun = (fixture: ReturnType<typeof runFixture>) => { fs.mkdirSync(fixture.run, { mode: 0o700 }); return fixture; };
// Recursive byte/inode/mode snapshot used to prove refusals preserve every original byte.
const tree = (target: string): unknown[] => {
  if (!fs.existsSync(target)) return [[target, "absent"]];
  const out: unknown[] = [];
  const walk = (file: string) => {
    const stat = fs.lstatSync(file);
    out.push([path.relative(target, file), stat.mode, stat.ino, stat.nlink, stat.isFile() ? fs.readFileSync(file).toString("base64") : stat.isSymbolicLink() ? fs.readlinkSync(file) : "dir"]);
    if (stat.isDirectory()) for (const name of fs.readdirSync(file).sort()) walk(path.join(file, name));
  };
  walk(target);
  return out;
};
// Typed unsafe refusal (owner OPTION 1): unknown raw/v1 history is refused, never imported.
const expectUnsafe = (operation: () => unknown) => {
  let error: unknown;
  try { operation(); } catch (caught) { error = caught; }
  expect(error, "expected unsafe legacy refusal").toBeInstanceOf(Error);
  const { code, message } = error as Error & { code?: string };
  expect(code === "FABRIC_LEGACY_SNAPSHOT_REQUIRED" || /FABRIC_LEGACY_SNAPSHOT_REQUIRED|unsafe/i.test(message), message).toBe(true);
};

describe.skipIf(!usable)("central task control root", () => {
  it("lives under the validated state home, outside temp, and refuses temp placement", () => {
    process.env.XDG_STATE_HOME = stateCase();
    expect(taskControlRoot()).toBe(path.join(process.env.XDG_STATE_HOME, "pi-fabric", "task-control"));
    expect(insideTemp(ensureTaskControlRoot())).toBe(false);
    expect(fs.statSync(taskControlRoot()).mode & 0o777).toBe(0o700);
    process.env.XDG_STATE_HOME = fs.mkdtempSync(path.join(os.tmpdir(), "state-in-temp-"));
    expect(() => taskControlRoot()).toThrow(/temporary directory/);
  });

  it("refuses a symlink alias and a group-writable state home", () => {
    const base = stateCase(), real = path.join(base, "real"), alias = path.join(base, "alias");
    fs.mkdirSync(real, { mode: 0o700 }); fs.symlinkSync(real, alias, "dir");
    process.env.XDG_STATE_HOME = alias;
    expect(() => taskControlRoot()).toThrow(/link alias/);
    const open = path.join(base, "open"); fs.mkdirSync(open, { mode: 0o700 }); fs.chmodSync(open, 0o770);
    process.env.XDG_STATE_HOME = open;
    expect(() => taskControlRoot()).toThrow(/writable by other users/);
    const loose = path.join(base, "loose"); fs.mkdirSync(path.join(loose, "pi-fabric"), { recursive: true, mode: 0o700 }); fs.chmodSync(path.join(loose, "pi-fabric"), 0o755);
    process.env.XDG_STATE_HOME = loose;
    expect(() => taskControlRoot()).toThrow(/owned private directory/);
  });

  it("derives a deterministic run-id plus path identity, so reused archive ids never collide", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const a = runFixture(), b = runFixture();
    expect(taskControlDirectory(a.run)).toBe(taskControlDirectory(a.run + path.sep));
    expect(path.dirname(taskControlDirectory(a.run))).toBe(taskControlRoot());
    expect(path.basename(taskControlDirectory(a.run))).toMatch(/^run--[0-9a-f]{32}$/);
    expect(taskControlDirectory(a.run)).not.toBe(taskControlDirectory(b.run));
    expect(taskControlDirectory(a.run).startsWith(a.root)).toBe(false);
  });

  it("refuses grants on the root, its rename-capable ancestors and parent controls (SEC01); admits normal run/workspace grants", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = runFixture(), parentRun = runFixture();
    createHostTaskRun(s.run); createHostTaskRun(parentRun.run);
    const grant = (...roots: string[]) => resolveChildWritePolicy({ writableRoots: roots }, undefined, s.workspace)!;
    expect(() => assertTaskControlWriteRoots(s.run, s.workspace, grant(s.run, s.workspace))).not.toThrow();
    for (const root of [process.env.XDG_STATE_HOME, taskControlRoot(), path.dirname(taskControlRoot()), taskControlDirectory(s.run), taskControlDirectory(parentRun.run)]) {
      expect(() => assertTaskControlWriteRoots(s.run, s.workspace, grant(s.run, s.workspace, root))).toThrow(/overlaps writable root/);
    }
    // The child grant validator requires an existing root outside the cwd: create a
    // safe private x so the overlap refusal itself is what the assertion exercises.
    const nested = path.join(taskControlDirectory(parentRun.run), "x"); fs.mkdirSync(nested, { mode: 0o700 });
    expect(() => assertTaskControlWriteRoots(s.run, s.workspace, grant(s.run, nested))).toThrow(/overlaps writable root/);
  });

  it("keys the lexical path, refusing a child-planted nested alias onto a victim run without touching its controls", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const victim = runFixture(), child = runFixture();
    createHostTaskRun(victim.run);
    expect(saveFinalAnswerReceipt(victim.run, "run", "VICTIM").text).toBe("VICTIM");
    writeTaskControlJson(victim.run, "marker.json", { owner: "victim" });
    const victimControl = taskControlDirectory(victim.run);
    const snapshot = () => fs.readdirSync(victimControl).sort().map(name => {
      const stat = fs.lstatSync(path.join(victimControl, name));
      return [name, stat.mode, stat.ino, stat.isFile() ? fs.readFileSync(path.join(victimControl, name)).toString("base64") : "dir"];
    });
    const before = snapshot(), beforeDir = fs.lstatSync(victimControl);
    // Nested ancestor alias: child/nest/deep/link -> victim root, so link/run physically is victim.run.
    fs.mkdirSync(path.join(child.workspace, "nest", "deep"), { recursive: true, mode: 0o700 });
    const link = path.join(child.workspace, "nest", "deep", "link"); fs.symlinkSync(victim.root, link, "dir");
    // Leaf alias: child/nest/run -> victim.run (same basename).
    const leaf = path.join(child.workspace, "nest", "run"); fs.symlinkSync(victim.run, leaf, "dir");
    for (const alias of [path.join(link, "run"), leaf, path.join(link, "missing", "run")]) {
      expect(() => taskControlDirectory(alias)).toThrow(/link alias/);
      expect(() => createHostTaskRun(alias)).toThrow(/link alias/);
      expect(() => initializeTaskControlDirectory(alias)).toThrow(/link alias/);
      expect(() => saveFinalAnswerReceipt(alias, "run", "FORGED")).toThrow(/link alias/);
      expect(() => writeTaskControlJson(alias, "marker.json", { owner: "child" })).toThrow(/link alias/);
    }
    expect(snapshot()).toEqual(before);
    const afterDir = fs.lstatSync(victimControl);
    expect([afterDir.ino, afterDir.mode, afterDir.mtimeMs]).toEqual([beforeDir.ino, beforeDir.mode, beforeDir.mtimeMs]);
    expect(readFinalAnswerReceipt(victim.run, "run")?.text).toBe("VICTIM");
    // Counterexample: a real nested child run with the same basename works in its own namespace.
    const real = path.join(child.workspace, "nest", "deep", "real", "run"); fs.mkdirSync(path.dirname(real), { recursive: true, mode: 0o700 }); // parent only
    createHostTaskRun(real);
    expect(saveFinalAnswerReceipt(real, "run", "CHILD").text).toBe("CHILD");
    expect(path.basename(taskControlDirectory(real))).toMatch(/^run--[0-9a-f]{32}$/);
    expect(taskControlDirectory(real)).not.toBe(victimControl);
    expect(readFinalAnswerReceipt(victim.run, "run")?.text).toBe("VICTIM");
    expect(snapshot()).toEqual(before);
    // A missing future run path without a link is accepted deterministically.
    const future = path.join(child.workspace, "future", "run");
    expect(taskControlDirectory(future)).toBe(taskControlDirectory(future + path.sep));
    expect(taskControlDirectory(future)).not.toBe(victimControl);
  });

  it("never recreates missing controls of a central-v1 run, while archive creation admits a fresh destination", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = rawRun(runFixture());
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "completed", taskControlLayout: TASK_CONTROL_LAYOUT });
    writeJsonAtomic(path.join(s.run, "final-answer.json"), { version: 1, id: "forged", runId: "run", recordedAt: 1, text: "untrusted" });
    expect(() => initializeTaskControlDirectory(s.run)).toThrow(/refusing recreation/);
    expect(() => createHostTaskRun(s.run)).toThrow(); // host ctor never mints over an existing marked run
    expect(() => taskControlReadDirectory(s.run)).toThrow(/legacy fallback refused/);
    expect(fs.existsSync(taskControlDirectory(s.run))).toBe(false);
    const archive = path.join(s.root, "archive"); fs.mkdirSync(path.join(archive, "run"), { recursive: true, mode: 0o700 });
    const dest = path.join(archive, "run");
    writeJsonAtomic(path.join(dest, "status.json"), { id: "run", status: "completed", taskControlLayout: TASK_CONTROL_LAYOUT });
    createArchiveTaskControlDirectory(dest); createArchiveTaskControlDirectory(dest); // create, then validate existing
    expect(fs.statSync(taskControlDirectory(dest)).mode & 0o777).toBe(0o700);
    expect(fs.existsSync(taskControlDirectory(s.run))).toBe(false);
  });

  // CUTOFF (owner OPTION 1, #12214): every existing raw old/v1 layout is unknown history.
  // Initialize/read/save/queue refuse with a typed unsafe error; every original byte stays.
  // Owner OPTION 2: the generic seed producer and protected legacy positives are an explicit
  // cut (#12214), not unresolved todos. The only positive constructor is createHostTaskRun.
  const rawLayout = (options: { siblingAnswer?: string; inRunAnswer?: string; bound?: boolean; sibling?: boolean; layout?: string; queued?: boolean }) => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = rawRun(runFixture()), sibling = legacyTaskControlSibling(s.run), id = "0b6f4c1e-1f0a-4c55-9a51-2f6a0d1e7c11";
    if (options.queued !== false) {
      fs.mkdirSync(path.join(s.run, "terminal-controls"), { mode: 0o700 });
      writeJsonAtomic(path.join(s.run, "terminal-controls", `${id}.json`), { id, delivery: "steer", state: "queued" });
    }
    if (options.inRunAnswer) writeJsonAtomic(path.join(s.run, "final-answer.json"), { version: 1, id: "in", runId: "run", recordedAt: 1, text: options.inRunAnswer });
    if (options.sibling !== false) {
      fs.mkdirSync(sibling, { mode: 0o700 });
      const stat = fs.lstatSync(s.run, { bigint: true });
      if (options.bound !== false) writeJsonAtomic(path.join(sibling, ".legacy-read.json"), { version: 1, runId: "run", dev: String(stat.dev), ino: String(stat.ino) });
      if (options.siblingAnswer) writeJsonAtomic(path.join(sibling, "final-answer.json"), { version: 1, id: "old", runId: "run", recordedAt: 1, text: options.siblingAnswer });
    }
    writeJsonAtomic(path.join(s.run, "status.json"), options.layout === undefined ? { id: "run", status: "completed" } : { id: "run", status: "completed", taskControlLayout: options.layout });
    const before = { run: tree(s.run), sibling: tree(sibling), central: tree(taskControlDirectory(s.run)) };
    const preserved = () => {
      expect(tree(s.run)).toEqual(before.run);
      expect(tree(sibling)).toEqual(before.sibling);
      expect(tree(taskControlDirectory(s.run))).toEqual(before.central); // never auto-upgraded
    };
    return { ...s, sibling, id, preserved };
  };
  const expectRefusedEverywhere = (t: ReturnType<typeof rawLayout>) => {
    expectUnsafe(() => readFinalAnswerReceipt(t.run, "run"));          // before bootstrap: old public read refuses
    expectUnsafe(() => queuedTerminalControls(t.run));
    expectUnsafe(() => initializeTaskControlDirectory(t.run));         // validation-only: no import/auto-upgrade
    expect(() => createHostTaskRun(t.run)).toThrow();                  // host ctor never mints over pre-existing history
    expectUnsafe(() => readFinalAnswerReceipt(t.run, "run"));          // still refused afterwards
    expectUnsafe(() => saveFinalAnswerReceipt(t.run, "run", "NEW"));   // no NEW save, no public finality
    expectUnsafe(() => queuedTerminalControls(t.run));
    t.preserved();
  };

  it("CUTOFF: raw sibling-v1 (single genuine-looking answer) refuses initialize/read/save and preserves bytes", () => {
    const t = rawLayout({ siblingAnswer: "OLD_SIBLING", bound: false, queued: false, layout: "sibling-v1" });
    expectUnsafe(() => taskControlReadDirectory(t.run));
    expect(isTaskControlDirectoryEntry(t.root, "run.ctl")).toBe(true); // still a control entry, never collected as artifact
    expectRefusedEverywhere(t);
    expect(fs.readdirSync(t.root).sort()).toEqual(["run", "run.ctl", "workspace"]);
  });

  it("CUTOFF: inode-bound v1 tri-layout (in-run, sibling, both, empty-looking) is unknown history, never final", () => {
    for (const options of [{ inRunAnswer: "IN_RUN" }, { siblingAnswer: "OLD_SIBLING" }, { siblingAnswer: "OLD_SIBLING", inRunAnswer: "IN_RUN" }, {}]) {
      expectRefusedEverywhere(rawLayout({ ...options, layout: "sibling-v1" }));
    }
  });

  it("CUTOFF: un-bootstrapped bound sibling plus in-run answer refuses before any bootstrap", () => {
    const t = rawLayout({ siblingAnswer: "OLD_SIBLING", inRunAnswer: "IN_RUN", queued: false, layout: "sibling-v1" });
    expectUnsafe(() => readFinalAnswerReceipt(t.run, "run"));
    t.preserved();
  });

  it("CUTOFF: unbound old sibling no longer ignores in-run rows and saves NEW; it refuses, bytes preserved (#12214)", () => {
    expectRefusedEverywhere(rawLayout({ inRunAnswer: "SPOOF", bound: false, layout: "sibling-v1" }));
  });

  it("CUTOFF: in-run-only legacy record with no layout marker refuses (was IN_RUN)", () => {
    expectRefusedEverywhere(rawLayout({ inRunAnswer: "IN_RUN", sibling: false, queued: false }));
  });

  it("CUTOFF: raw history with v1 binding cannot be bootstrapped, and a central write cannot launder it", () => {
    const t = rawLayout({ siblingAnswer: "OLD_SIBLING", layout: "sibling-v1" });
    expectUnsafe(() => initializeTaskControlDirectory(t.run));
    expect(() => writeTaskControlJson(t.run, "final-answer.json", { version: 1, id: "dup", runId: "run", recordedAt: 2, text: "CENTRAL" })).toThrow();
    expectUnsafe(() => readFinalAnswerReceipt(t.run, "run"));
    t.preserved();
  });

  it("single-source current-only answer stays final (host ctor writes the null-snapshot binding; initialize only validates)", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const blank = rawRun(runFixture()); // existing blank-looking run is unknown, not fresh
    expect(() => initializeTaskControlDirectory(blank.run)).toThrow(); // validation-only: never mints for a blank existing run
    expect(fs.existsSync(taskControlDirectory(blank.run))).toBe(false);
    const c = runFixture(); // NEW distinct run path, still absent
    expect(fs.existsSync(c.run)).toBe(false);
    createHostTaskRun(c.run); // exclusive mkdir + null central
    expect(fs.statSync(c.run).mode & 0o777).toBe(0o700);
    expect(() => createHostTaskRun(c.run)).toThrow(/already exists/); // EEXIST is never idempotent success
    initializeTaskControlDirectory(c.run); // validates the published authority
    expect(fs.existsSync(path.join(taskControlDirectory(c.run), ".legacy-read.json"))).toBe(true);
    expect(saveFinalAnswerReceipt(c.run, "run", "CURRENT").text).toBe("CURRENT");
    expect(readFinalAnswerReceipt(c.run, "run")?.text).toBe("CURRENT");
    expect(saveFinalAnswerReceipt(c.run, "run", "AGAIN").text).toBe("CURRENT"); // never overwrite
    expect(queuedTerminalControls(c.run)).toEqual([]);
    expect(fs.existsSync(legacyTaskControlSibling(c.run))).toBe(false);
  });

  it.skipIf(process.platform !== "linux")("openLegacyTaskControlFiles: current-only yields no fds; raw layers refuse without leaking any fd", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const c = runFixture(); createHostTaskRun(c.run);
    expect(openLegacyTaskControlFiles(c.run, "terminal-controls/probe.json")).toEqual([]);
    const u = rawLayout({ layout: "sibling-v1", queued: false });
    const probeU = path.join(u.sibling, "probe.json"); fs.writeFileSync(probeU, "{}", { mode: 0o600 });
    const other = path.join(u.root, "elsewhere.json"); fs.writeFileSync(other, "{}", { mode: 0o600 });
    fs.linkSync(other, path.join(u.run, "probe.json"));
    const targets = [fs.realpathSync(probeU), fs.realpathSync(other)];
    const opened = () => fs.readdirSync("/proc/self/fd").filter(fd => {
      try { return targets.includes(fs.readlinkSync(path.join("/proc/self/fd", fd))); } catch { return false; }
    });
    const before = [tree(u.run), tree(u.sibling)];
    expect(opened()).toEqual([]);
    expectUnsafe(() => openLegacyTaskControlFiles(u.run, "probe.json"));
    expect(opened()).toEqual([]); // no raw fd opened or leaked
    expect([tree(u.run), tree(u.sibling)]).toEqual(before);
  });

  it("after host bootstrap, raw old-layout forgery is ignored; host receipt and admission events stay authoritative", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = runFixture(), sibling = legacyTaskControlSibling(s.run), id = "0b6f4c1e-1f0a-4c55-9a51-2f6a0d1e7c11";
    createHostTaskRun(s.run); // manager order: host ctor (exclusive mkdir + null central) -> custody/status/task files
    // Task-side forgery written AFTER bootstrap into raw old-layout mirrors.
    fs.mkdirSync(path.join(s.run, "terminal-controls"), { mode: 0o700 });
    writeJsonAtomic(path.join(s.run, "terminal-controls", `${id}.json`), { id, delivery: "steer", state: "queued" });
    writeJsonAtomic(path.join(s.run, "final-answer.json"), { version: 1, id: "in", runId: "run", recordedAt: 1, text: "FORGED_IN_RUN" });
    fs.writeFileSync(path.join(s.run, "steer.jsonl"), JSON.stringify({ type: "steer", id: "forged" }) + "\n", { mode: 0o600 });
    fs.mkdirSync(sibling, { mode: 0o700 });
    const stat = fs.lstatSync(s.run, { bigint: true });
    writeJsonAtomic(path.join(sibling, ".legacy-read.json"), { version: 1, runId: "run", dev: String(stat.dev), ino: String(stat.ino) });
    writeJsonAtomic(path.join(sibling, "final-answer.json"), { version: 1, id: "old", runId: "run", recordedAt: 1, text: "FORGED_SIBLING" });
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "running", taskControlLayout: "sibling-v1" });
    const raw = () => [tree(s.run), tree(sibling)], before = raw();
    initializeTaskControlDirectory(s.run); // validation still succeeds; nothing re-captured
    expect(readFinalAnswerReceipt(s.run, "run")).toBeUndefined();
    expect(queuedTerminalControls(s.run)).toEqual([]);
    expect(openLegacyTaskControlFiles(s.run, "final-answer.json")).toEqual([]);
    expect([...readTerminalAdmissions(s.run).byId.keys()]).toEqual([]);
    appendTerminalAdmission(s.run, JSON.stringify({ type: "steer", id: "host" }) + "\n"); // real host admission event
    expect([...readTerminalAdmissions(s.run).byId.keys()]).toEqual(["host"]);
    expect(saveFinalAnswerReceipt(s.run, "run", "CURRENT").text).toBe("CURRENT"); // real host receipt event
    expect(readFinalAnswerReceipt(s.run, "run")?.text).toBe("CURRENT");
    expect(saveFinalAnswerReceipt(s.run, "run", "AGAIN").text).toBe("CURRENT");
    expect(raw()).toEqual(before); // raw mirrors never written, read back or repaired
  });

  it("creates a deeply nonexistent absolute XDG state home privately", () => {
    process.env.XDG_STATE_HOME = path.join(stateCase(), "a", "b", "c");
    const root = ensureTaskControlRoot();
    expect(root).toBe(path.join(process.env.XDG_STATE_HOME, "pi-fabric", "task-control"));
    expect(fs.statSync(path.join(process.env.XDG_STATE_HOME, "..", "..")).mode & 0o777).toBe(0o700);
  });

  it("missing old sibling for a sibling-v1 run refuses fallback and recreation", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = rawRun(runFixture());
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "completed", taskControlLayout: "sibling-v1" });
    expect(() => taskControlReadDirectory(s.run)).toThrow();
    expect(() => initializeTaskControlDirectory(s.run)).toThrow();
    expect(fs.existsSync(taskControlDirectory(s.run))).toBe(false);
  });
});
