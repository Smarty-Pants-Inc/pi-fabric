import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { sweepMeshRetention } from "../src/storage/retention-cli.js";

const DAY = 86_400_000;
const NOW = 1_800_000_000_000;
const TOKEN = "00000000-0000-4000-8000-000000000000";
const PREFIX = `${TOKEN}\n${process.pid}\n${NOW}\n`;
const roots: string[] = [];
const removals: { method: string; target: string }[] = [];

// #4664: install BEFORE any fixture/product setup. No sync, callback or promise removal calls through,
// including the real writer's lock release. Owned fixture teardown runs only AFTER restoring these spies.
beforeEach(() => {
  removals.length = 0;
  const record = (method: string, target: unknown) => { removals.push({ method, target: String(target) }); };
  for (const method of ["rmSync", "unlinkSync", "rmdirSync"] as const) {
    vi.spyOn(fs, method).mockImplementation(((target: unknown) => record(method, target)) as never);
  }
  for (const method of ["rm", "unlink", "rmdir"] as const) {
    vi.spyOn(fs, method).mockImplementation(((...args: unknown[]) => {
      record(method, args[0]);
      (args.at(-1) as (error: null) => void)(null);
    }) as never);
    vi.spyOn(fs.promises, method).mockImplementation((async (target: unknown) => {
      record(`promises.${method}`, target);
    }) as never);
  }
});
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const write = (file: string, data: string) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, data, { mode: 0o600 });
};
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-1072-owner-label-"));
  roots.push(root);
  const actors = path.join(root, "actors");
  const actor = path.join(actors, "actor");
  write(path.join(actors, "actors.json"), JSON.stringify({ actors: [{
    id: "actor", status: "idle", lastRunId: "latest", sessionFile: path.join(actor, "session.jsonl"),
  }] }));
  write(path.join(actor, "session.jsonl"), "live");
  const runs = ["old", "latest"].map(id => {
    const run = path.join(actor, "runs", id);
    write(path.join(run, "status.json"), JSON.stringify({
      transport: "process", sessionId: "2147483647", status: "completed", finishedAt: NOW - 9 * DAY,
    }));
    write(path.join(run, "events.jsonl"), '{"sequence":1}\n');
    write(path.join(run, "reply.json"), '{"text":"done"}');
    return run;
  });
  const lock = path.join(actors, "actors.json.lock");
  return { root, actors, lock, owner: path.join(lock, "owner"), old: runs[0]!, latest: runs[1]! };
};
type Fixture = ReturnType<typeof fixture>;
const owner = (m: Fixture, record: string) => write(m.owner, record);
const sweep = (m: Fixture) => sweepMeshRetention(m.root, { dryRun: true, now: NOW, runRetentionMs: 7 * DAY });
const unchanged = (m: Fixture) => {
  for (const run of [m.old, m.latest]) expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe('{"text":"done"}');
};
const planned = async (m: Fixture) => {
  const result = await sweep(m);
  expect(result.dryRun).toBe(true);
  expect(result.removedRuns).toEqual([m.old]);
  expect(result.skipped).toEqual([]);
  unchanged(m);
};

// Native POSIX private owner-file contract: no Windows helper or child processes needed.
describe.skipIf(process.platform !== "linux")("retention project-registry owner label (#1072, #8526)", () => {
  it.each(["external", "retention: diagnostic label", " ", "~".repeat(64)])(
    "plans while the REAL registry writer holds its five-field lock (%s)", async label => {
      const m = fixture();
      const store = new ActorRegistryStore(m.actors);
      let release!: () => void;
      const held = store.withLock(() => new Promise<void>(resolve => { release = resolve; }), undefined, label);
      try {
        const fields = fs.readFileSync(m.owner, "utf8").split("\n");
        expect(fields).toHaveLength(6);
        expect(fields[4]).toBe(label);
        expect(fields[5]).toBe("");
        await planned(m);
        expect(removals).toEqual([]); // still held across the complete sweep
      } finally {
        release();
        await held; // native release is recorded, NOT executed
      }
      expect(removals).toEqual([{ method: "rmSync", target: m.lock }]);
      expect(fs.existsSync(m.owner)).toBe(true);
      unchanged(m);
    },
  );

  it.each([
    ["legacy three fields", PREFIX],
    ["legacy numeric fourth", `${PREFIX}123\n`],
    ["legacy zero fourth", `${PREFIX}0\n`],
    ["legacy leading-zero fourth", `${PREFIX}00123\n`],
    ["new numeric fourth", `${PREFIX}123\nexternal\n`],
    ["new empty fourth", `${PREFIX}\nexternal\n`],
    ["new empty fourth and numeric label", `${PREFIX}\n123\n`],
    ["new numeric fourth and numeric label", `${PREFIX}123\n456\n`],
    ["safe integer limits and zero time", `${TOKEN}\n9007199254740991\n0\n\nexternal\n`],
  ])("accepts the exact complete format: %s", async (_name, record) => {
    const m = fixture();
    owner(m, record);
    await planned(m);
    expect(fs.readFileSync(m.owner, "utf8")).toBe(record);
    expect(removals).toEqual([]);
  });

  it.each([
    ["empty record", ""],
    ["torn identity", TOKEN],
    ["bad UUID", `${"-".repeat(36)}\n${process.pid}\n${NOW}\n\nexternal\n`],
    ["non-v4 UUID", `${TOKEN.replace("4000", "3000")}\n${process.pid}\n${NOW}\n\nexternal\n`],
    ["bad UUID variant", `${TOKEN.replace("8000", "7000")}\n${process.pid}\n${NOW}\n\nexternal\n`],
    ["zero pid", `${TOKEN}\n0\n${NOW}\n\nexternal\n`],
    ["overflow pid", `${TOKEN}\n9007199254740992\n${NOW}\n\nexternal\n`],
    ["overflow time", `${TOKEN}\n${process.pid}\n9007199254740992\n\nexternal\n`],
    ["negative time", `${TOKEN}\n${process.pid}\n-1\n\nexternal\n`],
    ["leading-zero pid", `${TOKEN}\n01\n${NOW}\n\nexternal\n`],
    ["leading-zero time", `${TOKEN}\n${process.pid}\n01\n\nexternal\n`],
    ["empty fourth without fifth", `${PREFIX}\n`],
    ["label in fourth slot", `${PREFIX}external\n`],
    ["nonnumeric start", `${PREFIX}7x\nexternal\n`],
    ["empty label", `${PREFIX}123\n\n`],
    ["torn label", `${PREFIX}123\nexternal`],
    ["unknown sixth field", `${PREFIX}123\nexternal\nextra\n`],
    ["extra terminal newline", `${PREFIX}123\nexternal\n\n`],
    ["label newline injection", `${PREFIX}\none\ntwo\n`],
    ["label tab", `${PREFIX}\none\ttwo\n`],
    ["label CR", `${PREFIX}\nexternal\r\n`],
    ["label NUL", `${PREFIX}\nexternal\0\n`],
    ["label DEL", `${PREFIX}\nexternal\x7f\n`],
    ["non-ASCII label", `${PREFIX}\ncaf\u00e9\n`],
    ["overlong label", `${PREFIX}\n${"a".repeat(65)}\n`],
    ["legacy trailing junk", `${PREFIX}junk`],
    ["oversized owner", `${PREFIX}\n${"a".repeat(257)}\n`],
  ])("keeps an initially malformed owner unknown: %s", async (_name, record) => {
    const m = fixture();
    owner(m, record);
    await expect(sweep(m)).rejects.toThrow(`Actor references are uncertain: ${m.actors}`);
    expect(removals).toEqual([]);
    expect(fs.readFileSync(m.owner, "utf8")).toBe(record);
    unchanged(m);
  });

  it.each([PREFIX, `${PREFIX}\n123\n`])("does not trust a short read of an otherwise valid prefix (%j)", async prefix => {
    const m = fixture();
    const record = `${prefix}extra\n`;
    owner(m, record);
    const ino = fs.lstatSync(m.owner).ino;
    const read = fs.readSync;
    let shortReads = 0;
    vi.spyOn(fs, "readSync").mockImplementation(((...args: Parameters<typeof fs.readSync>) => {
      const bytes = read(...args);
      if (fs.fstatSync(args[0]).ino !== ino) return bytes;
      shortReads++;
      expect(bytes).toBe(Buffer.byteLength(record));
      return Buffer.byteLength(prefix);
    }) as typeof fs.readSync);
    await expect(sweep(m)).rejects.toThrow(`Actor references are uncertain: ${m.actors}`);
    expect(shortReads).toBe(1);
    expect(removals).toEqual([]);
    unchanged(m);
  });
});
