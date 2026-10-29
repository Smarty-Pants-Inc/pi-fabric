import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FABRIC_RUN_ROOT_PREFIX, sweepTempRunRoots } from "../src/storage/retention.js";
import { ageRunRoot } from "./helpers/age-run-root.js";

// Data-safety keep reason: the temp sweep now retires trees into custody, not
// a deletion graveyard. Inject swaps AFTER metadata judgment, during its worker
// exit check; every refusal must preserve original/replacement/outside bytes.
const HOUR = 3_600_000;
const DAY = 24 * HOUR;
const OWNER = 2_147_483_646;
const WORKER = 2_147_483_647;
const NOW = Date.now();
const roots: string[] = [];
const payload = Buffer.from('{"sequence":0}\n{"torn":');
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const shape = (file: string): unknown => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return null;
  if (stat.isSymbolicLink()) return ["link", fs.readlinkSync(file)];
  if (stat.isDirectory()) return Object.fromEntries(fs.readdirSync(file).sort().map(name => [name, shape(path.join(file, name))]));
  return fs.readFileSync(file).toString("hex");
};
type Kind = "closed" | "orphan";
const fixture = (kind: Kind) => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-runroot-swaps-")); roots.push(tempRoot);
  const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}${kind}`), run = path.join(root, "run");
  fs.mkdirSync(run, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id: "run", status: "completed", finishedAt: 1, transport: "process", sessionId: String(WORKER) }), { mode: 0o600 });
  fs.writeFileSync(path.join(run, "events.jsonl"), payload, { mode: 0o600 });
  fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: OWNER, startedAt: 1, heartbeatAt: 1,
    ...(kind === "closed" ? { closedAt: 1, childrenStopped: true } : { orphanedAt: 1 }) }), { mode: 0o600 });
  ageRunRoot(root, 1);
  const sweep = () => sweepTempRunRoots({ tempRoot, now: NOW, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });
  const deletes: string[] = [];
  const unlink = fs.unlinkSync, rm = fs.rmSync;
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { deletes.push(String(file)); return unlink(file); });
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { deletes.push(String(file)); return rm(file, options); });
  return { tempRoot, root, run, sweep, deletes, judged: shape(run) };
};
const SWAPS = ["replaced inode", "symlink", "foreign owner", "refreshed mtime"] as const;

describe.skipIf(process.platform !== "linux")("run-root custody keeps the judged tree", () => {
  describe.each(["closed", "orphan"] as Kind[])("%s root", kind => {
    it("positive control: archives the exact eligible bytes without unlinking raw traces", () => {
      const f = fixture(kind);
      const result = f.sweep();
      expect(result.removedRuns).toEqual(kind === "closed" ? [f.run] : []);
      expect(result.removedRoots).toEqual([f.root]);
      const archive = `${f.root}.trace-archive`;
      const receipts = fs.readdirSync(archive).map(name => JSON.parse(fs.readFileSync(path.join(archive, name, "custody.json"), "utf8")));
      const receipt = receipts.find(row => row.source === f.run && row.state === "moved");
      expect(receipt).toBeDefined(); expect(shape(receipt.destination)).toEqual(f.judged);
      expect(fs.readFileSync(path.join(receipt.destination, "events.jsonl"))).toEqual(payload);
      expect(f.deletes.some(file => file.endsWith("events.jsonl"))).toBe(false);
    });
    it.each(SWAPS)("a %s swap during the safety check archives/deletes no tree entry", swap => {
      const f = fixture(kind), aside = path.join(f.tempRoot, "judged-aside"), outside = path.join(f.tempRoot, "outside");
      fs.mkdirSync(outside, { mode: 0o700 }); fs.writeFileSync(path.join(outside, "keep"), "outside");
      const outsideBefore = shape(outside), stat = fs.lstatSync;
      let swapped = 0, workerChecks = 0;
      vi.spyOn(process, "kill").mockImplementation(pid => {
        // Recovery's first absence probe precedes the per-run inventory. Swap
        // on the next probe, inside the safety check AFTER that judgment.
        if (pid === WORKER && ++workerChecks === 2) {
          swapped++;
          if (swap === "refreshed mtime") fs.utimesSync(f.run, new Date(), new Date());
          else if (swap === "foreign owner") {
            vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
              const value = (stat as (...args: unknown[]) => fs.Stats)(file, ...args);
              return String(file) === f.run ? Object.assign(Object.create(Object.getPrototypeOf(value)), value, { uid: value.uid + 1 }) : value;
            }) as typeof fs.lstatSync);
          } else {
            fs.renameSync(f.run, aside);
            if (swap === "symlink") fs.symlinkSync(outside, f.run);
            else { fs.mkdirSync(f.run, { mode: 0o700 }); fs.writeFileSync(path.join(f.run, "status.json"), "fresh"); }
          }
        }
        throw Object.assign(new Error("gone"), { code: "ESRCH" });
      });
      const result = f.sweep();
      vi.restoreAllMocks();
      expect(swapped).toBeGreaterThan(0);
      expect(result).toEqual({ removedRoots: [], removedRuns: [] });
      expect(f.deletes).toEqual([]);
      expect(fs.existsSync(`${f.root}.trace-archive`)).toBe(false);
      expect(shape(outside)).toEqual(outsideBefore);
      if (swap === "symlink") expect(shape(f.run)).toEqual(["link", outside]);
      else if (swap === "replaced inode") expect(shape(f.run)).toEqual({ "status.json": Buffer.from("fresh").toString("hex") });
      else expect(shape(f.run)).toEqual(f.judged);
      if (swap === "symlink" || swap === "replaced inode") expect(shape(aside)).toEqual(f.judged);
    });
  });
});
