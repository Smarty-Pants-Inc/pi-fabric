import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { writeFileAtomic } from "../src/core/atomic-write.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

// pi-fabric#590 round 6 scope cut (smarty-dev#6477 L7): "no worse than main".
// The SAME fault injections run against this store; the observable outcomes must be
// exactly the set main produces. The golden file was recorded by running THIS file on
// a gh/main checkout with ACTOR_REGISTRY_EQUIVALENCE_RECORD=<file>; this file passes
// unchanged on main too. L7 moves file steps (appends) before the lock, so steps are
// not aligned by index: each fault/crash is swept over EVERY file-system step and the
// SET of reachable outcomes is compared.
//
// Observable outcomes per run: the save's result, then the messages every actor shows
//  - restart: a new process (the lock of a dead holder reaped),
//  - osCrash: an OS crash right after the save (a rename whose directory barrier never
//    succeeded is lost: a file reverts to its content at its directory's last barrier),
//  - older: an older-release writer saves its owned rows without `messageHistory`
//    (durably, under the registry lock), then an OS crash,
// each followed by the manager's next save (it re-sends m1 when the save was not
// acknowledged, else sends m2) to expose duplicate appends.

const MUTATORS = ["openSync", "writeSync", "writeFileSync", "fsyncSync", "renameSync", "rmSync", "mkdirSync"] as const;
const ids = ["a", "b", "c"].map(letter => letter.repeat(32));
const message = (id: string) => ({ id, direction: "in", text: id });
const withoutRefs = ({ messageHistory: _history, messages: _messages, ...row }: Record<string, unknown>) => row;
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const appendAll = (store: ActorRegistryStore, id: string) => store.update(current => ({
  actors: current.map(row => ({ ...withoutRefs(row), registryMessageAppend: [message(id)] })), value: true }));

const view = (actorRoot: string): string => {
  try {
    const store = new ActorRegistryStore(actorRoot);
    const rows = store.records().sort((left, right) => left.id.localeCompare(right.id));
    if (rows.map(row => row.id).join() !== ids.join()) return `rows ${rows.map(row => row.id.slice(0, 1)).join()}`;
    const views = rows.map(row => store.messages(row).map(entry => (entry as { id: string }).id).join(","));
    return new Set(views).size === 1 ? `all ${views[0]}` : views.map((entry, index) => `${"abc"[index]}:${entry}`).join(" ");
  } catch (error) { return `throws ${String(error)}`; }
};

const fixture = async () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "registry-main-equivalence-"))); roots.push(root);
  const actorRoot = path.join(root, "actors");
  const store = new ActorRegistryStore(actorRoot);
  await store.update(() => ({ actors: ids.map(id => ({ id, name: id, rootId: "session:equivalence", residency: "session",
    instructions: "persona", messages: [message("m0")], status: "idle" })), durable: true, value: true }));
  return { root, actorRoot, store };
};

type Target = "registry" | "a" | "b" | "c";
type Arm = { crashAt?: number; fault?: { target: Target; times: number; from: number | "rename" } };

/** Count every file-system mutation, fail directory barriers, crash, and track which
 * renames are durable (a rename is durable once its directory's fsync succeeded). */
const instrument = (actorRoot: string, arm: Arm) => {
  const directory = (target: Target) => target === "registry" ? actorRoot : path.join(actorRoot, `${target.repeat(32)}`, "registry");
  const tracked = [path.join(actorRoot, "actors.json"), ...ids.map(id => path.join(actorRoot, id, "registry", "messages-head.json"))];
  const read = (file: string) => { try { return fs.readFileSync(file, "utf8"); } catch { return undefined; } };
  const durable = new Map(tracked.map(file => [file, read(file)]));
  const real = Object.fromEntries(MUTATORS.map(name => [name, (fs[name] as (...args: unknown[]) => unknown).bind(fs)]));
  const paths = new Map<number, string>();
  const failing = arm.fault ? directory(arm.fault.target) : undefined;
  let calls = 0, dead = false, active = arm.fault?.from === 0, remaining = arm.fault?.times ?? 0, enabled = true;
  for (const name of MUTATORS) {
    vi.spyOn(fs, name).mockImplementation(((...args: unknown[]) => {
      if (enabled) {
        if (dead || calls === arm.crashAt) { dead = true; throw Object.assign(new Error("simulated crash"), { code: "ECRASH" }); }
        if (typeof arm.fault?.from === "number" && calls >= arm.fault.from) active = true;
        calls++;
        if (active && remaining > 0 && name === "fsyncSync" && paths.get(args[0] as number) === failing) {
          remaining--;
          throw Object.assign(new Error("EIO: directory barrier"), { code: "EIO" });
        }
      }
      // Barriers are counted and modelled, not paid for.
      const result = name === "fsyncSync" ? undefined : real[name]!(...args);
      if (name === "openSync") paths.set(result as number, String(args[0]));
      if (name === "fsyncSync") {
        const synced = paths.get(args[0] as number);
        for (const file of tracked) if (path.dirname(file) === synced) durable.set(file, read(file));
      }
      if (enabled && name === "renameSync" && arm.fault?.from === "rename" && String(args[1]) === tracked[0]) active = true;
      return result;
    }) as never);
  }
  return {
    calls: () => calls,
    crashed: () => dead,
    /** The device recovers and the process is restarted: the rest of the run is healthy. */
    stop: () => { enabled = false; const snapshot = new Map(durable); vi.restoreAllMocks(); return snapshot; },
  };
};

const osCrash = (actorRoot: string, durable: ReadonlyMap<string, string | undefined>, from: string, keep: string[] = []) => {
  for (const [file, contents] of durable) {
    if (keep.includes(path.basename(file))) continue;
    const target = path.join(actorRoot, path.relative(from, file));
    if (contents === undefined) fs.rmSync(target, { force: true });
    else fs.writeFileSync(target, contents);
  }
};

const olderWriter = (actorRoot: string) => new ActorRegistryStore(actorRoot).withLock(() => {
  const registry = path.join(actorRoot, "actors.json");
  const raw = JSON.parse(fs.readFileSync(registry, "utf8")) as { actors: Record<string, unknown>[] };
  writeFileAtomic(registry, JSON.stringify({ format: 1, actors: raw.actors.map(row => ({ ...withoutRefs(row), messages: [] })) }), { durable: true });
});

const observe = async (arm: Arm) => {
  const { root, actorRoot, store } = await fixture();
  const probe = instrument(actorRoot, arm);
  const result = await appendAll(store, "m1").then(() => "ok", () => probe.crashed() ? "crashed" : "error");
  const steps = probe.calls();
  const durable = probe.stop();
  fs.rmSync(path.join(actorRoot, "actors.json.lock"), { recursive: true, force: true });
  const next = result === "ok" ? "m2" : "m1";
  const branch = async (name: string, prepare: (copy: string) => void | Promise<void>) => {
    const copy = path.join(root, name);
    fs.cpSync(actorRoot, copy, { recursive: true });
    await prepare(copy);
    const seen = view(copy);
    await appendAll(new ActorRegistryStore(copy), next).catch(() => undefined);
    return [seen, view(copy)];
  };
  const outcome = {
    result,
    restart: await branch("restart", () => undefined),
    osCrash: await branch("os-crash", copy => osCrash(copy, durable, actorRoot)),
    older: await branch("older", async copy => { await olderWriter(copy); osCrash(copy, durable, actorRoot, ["actors.json"]); }),
  };
  fs.rmSync(root, { recursive: true, force: true });
  return { steps, outcome: JSON.stringify(outcome) };
};

/** Every distinct outcome over all fault start points (or crash points). */
const sweep = async (arm: (step: number) => Arm, probeArm: Arm) => {
  const { steps } = await observe(probeArm);
  expect(steps).toBeGreaterThan(20);
  const outcomes = new Set<string>();
  for (let step = 0; step <= steps; step++) outcomes.add((await observe(arm(step))).outcome);
  return [...outcomes].sort();
};

const SCENARIOS: Array<{ name: string; run: () => Promise<string[]> }> = [
  { name: "healthy", run: async () => [(await observe({})).outcome] },
  ...(["registry", "a", "b", "c"] as const).flatMap(target => [
    ...[1, Number.POSITIVE_INFINITY].map(times => ({
      name: `barrier ${target} fails ${times === 1 ? "once" : "persistently"} from every step`,
      run: () => sweep(from => ({ fault: { target, times, from } }), {}),
    })),
    {
      name: `crash at every step; barrier ${target} fails persistently after the registry rename`,
      run: () => sweep(crashAt => ({ crashAt, fault: { target, times: Number.POSITIVE_INFINITY, from: "rename" } }),
        { fault: { target, times: Number.POSITIVE_INFINITY, from: "rename" } }),
    },
  ]),
  { name: "crash at every step; healthy barriers", run: () => sweep(crashAt => ({ crashAt }), {}) },
];

const record = process.env.ACTOR_REGISTRY_EQUIVALENCE_RECORD;
const recorded: Record<string, string[]> = {};

// Windows has no directory fsync (the barrier faults cannot fire there).
describe.skipIf(process.platform === "win32")("actor registry failure semantics are identical to main (pi-fabric#590, smarty-dev#6477 L7)", () => {
  it.each(SCENARIOS)("$name", async ({ name, run }) => {
    const outcomes = await run();
    if (record) {
      recorded[name] = outcomes;
      fs.writeFileSync(record, `${JSON.stringify(recorded, null, 2)}\n`);
      return;
    }
    const golden = JSON.parse(fs.readFileSync(path.join(import.meta.dirname, "fixtures", "actor-registry-main-outcomes.json"), "utf8")) as Record<string, string[]>;
    expect(outcomes).toEqual(golden[name]);
  }, 240_000);
});
