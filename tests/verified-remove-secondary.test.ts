import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { pruneResidentChildLogs, pruneWedgeEvidence } from "../src/residency/launcher.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import * as expiry from "../src/residency/request-expiry.js";
import { acknowledgeResidentResponse, commitResidentRequest, type ResidentCommand } from "../src/residency/protocol.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// smarty-dev#7916: the secondary path-based deleters remove only the inode they judged. A swap between judgment and
// delete (symlink, replaced inode, refreshed mtime) deletes nothing.
const roots: string[] = [];
afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const tmp = (prefix: string): string => { const value = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(value); return value; };
const DAY = 24 * 60 * 60 * 1000;
const old = new Date(Date.now() - 30 * DAY);

type Swap = "symlink" | "inode" | "mtime";
const SWAPS: Swap[] = ["symlink", "inode", "mtime"];
/** Swap `target` once, on its own beforeOpen; returns the outside object and the path the swapped-in object lives at. */
const arm = (target: string, swap: Swap, directory = false) => {
  const outside = path.join(tmp("pi-fabric-vr-outside-"), "outside");
  if (directory) { fs.mkdirSync(outside); fs.writeFileSync(path.join(outside, "keep"), "outside"); }
  else fs.writeFileSync(outside, "outside");
  let swapped = false;
  verifiedRemoveTestHooks.beforeOpen = (file) => {
    if (swapped || file !== target) return;
    swapped = true;
    if (swap === "mtime") { fs.utimesSync(target, new Date(), new Date()); return; }
    if (swap === "symlink") { fs.rmSync(target, { recursive: true, force: true }); fs.symlinkSync(outside, target); return; }
    // The fresh object exists before the judged one goes, so its inode number cannot be reused.
    const fresh = `${target}.fresh`;
    if (directory) { fs.mkdirSync(fresh); fs.writeFileSync(path.join(fresh, "keep"), "fresh"); }
    else fs.writeFileSync(fresh, "fresh");
    if (directory) fs.rmSync(target, { recursive: true, force: true });
    fs.renameSync(fresh, target);
  };
  return {
    expectNothingDeleted: () => {
      expect(swapped).toBe(true);
      expect(fs.existsSync(outside)).toBe(true);
      if (directory) expect(fs.readFileSync(path.join(outside, "keep"), "utf8")).toBe("outside");
      expect(() => fs.lstatSync(target)).not.toThrow();
      if (swap === "symlink") expect(fs.lstatSync(target).isSymbolicLink()).toBe(true);
    },
  };
};

const completions = () => {
  const root = tmp("pi-fabric-vr-completions-");
  const sessionFile = path.join(root, "session.jsonl");
  fs.writeFileSync(sessionFile, "");
  const store = new ActorChildCompletionStore(sessionFile);
  fs.mkdirSync(store.directory, { recursive: true });
  const file = path.join(store.directory, `${"a".repeat(32)}.receipt`);
  fs.writeFileSync(file, "receipt");
  fs.utimesSync(file, old, old);
  return { store, file };
};
const wedges = () => {
  const dir = tmp("pi-fabric-vr-wedges-");
  for (let i = 1; i <= 6; i++) {
    fs.mkdirSync(path.join(dir, `2025-01-0${i}T00:00:00.000Z`));
    fs.writeFileSync(path.join(dir, `2025-01-0${i}T00:00:00.000Z`, "status"), "evidence");
  }
  return { dir, oldest: path.join(dir, "2025-01-01T00:00:00.000Z") };
};
const childLogs = () => {
  const root = tmp("pi-fabric-vr-logs-");
  const file = path.join(root, "child-old.log");
  fs.writeFileSync(file, "log");
  fs.utimesSync(file, old, old);
  return { root, file, keep: path.join(root, "child-current.log") };
};
const requests = () => {
  const root = tmp("pi-fabric-vr-requests-");
  const time = Date.now() - expiry.RESIDENT_REQUEST_RETENTION_MS - 1_000;
  const requestId = expiry.newResidentRequestId(time);
  const command = { format: 3, operation: "removeActor", requestId, rootId: "session:retention", id: "entity", createdAt: time } as ResidentCommand;
  commitResidentRequest(root, command, "entity", "host");
  const response = { format: 1 as const, requestId, ok: true, completedAt: time };
  fs.mkdirSync(path.join(root, "responses"), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, "responses", `${requestId}.json`), JSON.stringify(response), { mode: 0o600 });
  acknowledgeResidentResponse(root, response, time, 3);
  const file = (kind: string) => path.join(root, kind, `${requestId}.json`);
  return { root, file, sweep: () => new ResidentRequestRetention(root).sweep(Date.now(), new Set(), 10_000) };
};

describe.skipIf(process.platform !== "linux")("secondary deleters remove only the judged inode (smarty-dev#7916)", () => {
  describe("ActorChildCompletionStore.prune", () => {
    it("removes an unswapped expired completion file", () => {
      const { store, file } = completions();
      store.prune(1000);
      expect(fs.existsSync(file)).toBe(false);
    });
    it.each(SWAPS)("deletes nothing after a %s swap", (swap) => {
      const { store, file } = completions();
      const probe = arm(file, swap);
      store.prune(1000);
      probe.expectNothingDeleted();
    });
    // pi-fabric#850 round 1 SEC P1 (2): the age comes from the same stat as the removal. A second stat that meets a
    // swapped-in old inode (put back before the removal) must not make a fresh group look expired.
    it("keeps a fresh group when a second stat would meet a swapped-in old inode", () => {
      const { store, file } = completions();
      fs.utimesSync(file, new Date(), new Date());
      const stat = fs.statSync;
      const oldStat = Object.assign(Object.create(Object.getPrototypeOf(fs.statSync(file))), fs.statSync(file), { mtimeMs: old.getTime() });
      const spy = vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) =>
        target === file ? oldStat : (stat as (...args: unknown[]) => unknown)(target, ...rest)) as typeof fs.statSync);
      try { store.prune(1000); } finally { spy.mockRestore(); }
      expect(fs.readFileSync(file, "utf8")).toBe("receipt");
    });
  });

  describe("pruneWedgeEvidence", () => {
    it("removes the unswapped oldest directory beyond five", () => {
      const { dir, oldest } = wedges();
      pruneWedgeEvidence(dir);
      expect(fs.existsSync(oldest)).toBe(false);
      expect(fs.readdirSync(dir)).toHaveLength(5);
    });
    it.each(SWAPS)("deletes nothing after a %s swap", (swap) => {
      const { dir, oldest } = wedges();
      const probe = arm(oldest, swap, true);
      pruneWedgeEvidence(dir);
      probe.expectNothingDeleted();
      expect(fs.readdirSync(dir).filter(name => !name.startsWith(".retention-graveyard-"))).toHaveLength(6);
    });
  });

  describe("pruneResidentChildLogs", () => {
    it("removes an unswapped log older than seven days", () => {
      const { root, file, keep } = childLogs();
      pruneResidentChildLogs(root, keep);
      expect(fs.existsSync(file)).toBe(false);
    });
    it.each(SWAPS)("deletes nothing after a %s swap", (swap) => {
      const { root, file, keep } = childLogs();
      const probe = arm(file, swap);
      pruneResidentChildLogs(root, keep);
      probe.expectNothingDeleted();
    });
  });

  describe("ResidentRequestRetention #collect", () => {
    it("removes the unswapped expired response, decision and acknowledgement", () => {
      const { file, sweep } = requests();
      sweep();
      for (const kind of ["responses", "decisions", "acknowledgements"]) expect(fs.existsSync(file(kind))).toBe(false);
    });
    // pi-fabric#850 round 1 SEC P1 (1): an expired acknowledgement is read and judged; a same-uid writer then replaces it
    // with a newer valid one before the removal. The newer acknowledgement, and everything else, stays.
    it("keeps a newer acknowledgement swapped in after the expired one was judged", () => {
      const { file, sweep } = requests();
      const ackPath = file("acknowledgements");
      const newer = { ...JSON.parse(fs.readFileSync(ackPath, "utf8")), acknowledgedAt: Date.now() };
      const read = fs.readFileSync;
      let swapped = false;
      const spy = vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
        const result = (read as (...args: unknown[]) => unknown)(target, ...rest);
        if (!swapped && target === ackPath) {
          swapped = true;
          fs.writeFileSync(`${ackPath}.new`, JSON.stringify(newer), { mode: 0o600 });
          fs.renameSync(`${ackPath}.new`, ackPath);
        }
        return result;
      }) as typeof fs.readFileSync);
      try { sweep(); } finally { spy.mockRestore(); }
      expect(swapped).toBe(true);
      expect(JSON.parse(fs.readFileSync(ackPath, "utf8"))).toEqual(newer);
      for (const kind of ["responses", "decisions"]) expect(fs.existsSync(file(kind))).toBe(true);
    });

    it.each(SWAPS)("deletes nothing after a %s swap of the response, and stops there", (swap) => {
      const { file, sweep } = requests();
      const probe = arm(file("responses"), swap);
      sweep();
      probe.expectNothingDeleted();
      expect(fs.existsSync(file("decisions"))).toBe(true);
      expect(fs.existsSync(file("acknowledgements"))).toBe(true);
    });
  });
});
