import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { DurableDirectory } from "../src/core/atomic-write.js";

const roots: string[] = [];
const identity = { id: "session:round4", name: "main", kind: "main" as const };
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-atomic-")); roots.push(directory); return directory; };
const tick = () => new Promise<void>(resolve => setTimeout(resolve, 5));
const until = async (condition: () => boolean) => { for (let n = 0; !condition(); n++) { if (n > 1000) throw new Error("mesh probe timed out"); await tick(); } };
afterEach(() => { vi.restoreAllMocks(); for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });
const descriptors = () => {
  const files = new Map<number, string>(), open = fs.openSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; });
  return files;
};

describe("#2479 M round-four regressions", () => {
  // Exact output of merge-base d0ff0eb3's MeshStore writer, seeded at 41 and
  // advanced to 42. No new-format put precedes the unchanged upgrade operation.
  const legacy = fs.readFileSync(path.resolve("tests/fixtures/mesh-state-pre-atomic-audit.json"), "utf8");
  for (const operation of ["delete", "batch"] as const) {
    it.each([false, true])(`F8 confirms legacy ${operation} no-op without rewriting (older checkpoint: %s)`, async checkpoint => {
      const directory = root(), mesh = new MeshStore(directory, 65536, 100);
      if (checkpoint) await mesh.put({ key: "test/old", value: "checkpoint", identity });
      const serialized = legacy;
      const successor = path.join(directory, "legacy-successor.tmp");
      fs.writeFileSync(successor, serialized); fs.renameSync(successor, path.join(directory, "state.json"));
      const fileSync = vi.spyOn(fs, "fsync");
      const receipt = operation === "delete" ? await mesh.delete({ key: "test/absent" })
        : await mesh.writeBatch({ identity, ops: [{ kind: "put", key: "test/old", value: "skip", ifVersion: 0, onConflict: "skip" }] });
      expect(receipt).toEqual(operation === "delete" ? { deleted: false } : [{ key: "test/old", applied: false, version: 42 }]);
      expect(fileSync).toHaveBeenCalledTimes(1);
      expect(fs.readFileSync(path.join(directory, "state.json"), "utf8")).toBe(serialized);
      expect(fs.readFileSync(path.join(directory, "state.durable.json"), "utf8")).toBe(serialized);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8")).generation).toBe(42);
    });
  }

  it.skipIf(process.platform === "win32")("CI namespace churn reconfirms more than four benign ancestor changes without acknowledging an ABA early", () => {
    const parent = root(), directory = path.join(parent, "mesh"); fs.mkdirSync(directory);
    const receipt = new DurableDirectory(directory); receipt.prepare();
    const files = descriptors(), sync = fs.fsyncSync.bind(fs);
    let changes = 0, parentBarriers = 0;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const file = files.get(fd)!;
      sync(fd);
      if (file === parent) parentBarriers++;
      if (file === directory && changes < 5) {
        const away = path.join(parent, "away"); fs.renameSync(directory, away); fs.renameSync(away, directory); changes++;
      }
    });
    expect(() => receipt.sync(directory)).not.toThrow();
    expect(changes).toBe(5); expect(parentBarriers).toBeGreaterThanOrEqual(5);
  });
});
