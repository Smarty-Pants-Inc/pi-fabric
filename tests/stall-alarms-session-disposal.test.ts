import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { registerMainInbox, recordMainSuccessor, stageMainSuccessor, confirmMainSuccessor } from "../src/topology/stall-alarms.js";

// smarty-dev#8881 F-01: successor attribution awaits custody acquisition. A native dispose while
// that acquisition is pending must not commit successor/owner/rotation metadata once custody frees.
const dirs: string[] = [];
const releases: Array<() => void> = [];
afterEach(() => {
  for (const release of releases.splice(0)) { try { release(); } catch { /* already released */ } }
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
  vi.restoreAllMocks();
});
const identity = (id: string): MeshIdentity => ({ id: `session:${id}`, sessionId: id, name: id, kind: "main" });
const A = identity("A"), B = identity("B");
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const setup = () => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-f01-")));
  dirs.push(dir);
  const mesh = new MeshStore(dir, 64 * 1024, 500);
  const box = path.join(dir, "main-followups");
  return {
    mesh,
    owner: path.join(box, "A.owner.json"),
    successor: path.join(box, "successors", `${hash(A.id)}.json`),
    intent: (file: string) => path.join(box, "rotations", `${hash(path.resolve(file))}.json`),
  };
};
const snapshot = (...files: string[]) => files.map(file => fs.existsSync(file) ? fs.readFileSync(file, "utf8") : undefined);
const hold = async (root: string): Promise<() => void> => {
  const release = await acquireMeshCustodyLock(root);
  let released = false;
  const once = () => { if (!released) { released = true; release(); } };
  releases.push(once);
  return once;
};
/** Observed pending gate: the call has not settled across an event-loop turn while custody is held. */
const pending = async (promise: Promise<unknown>): Promise<boolean> => {
  let settled = false;
  promise.then(() => { settled = true; }, () => { settled = true; });
  await new Promise(resolve => setImmediate(resolve));
  return !settled;
};

it("recordMainSuccessor rechecks liveness inside custody: disposal during the wait writes nothing", async () => {
  const { mesh, owner, successor } = setup();
  registerMainInbox(mesh.root, A, "A");
  const before = snapshot(owner, successor);
  const release = await hold(mesh.root);
  let alive = true;
  const live = vi.fn(() => alive);
  const call = (recordMainSuccessor as (...args: unknown[]) => Promise<void>)(mesh, A.id, "A", B.id, live);
  expect(await pending(call)).toBe(true);
  alive = false;                                    // native ctx disposed while custody is pending
  release();
  await expect(call).rejects.toThrow(/lifecycle retired/);
  expect(live).toHaveBeenCalled();
  expect(snapshot(owner, successor)).toEqual(before);
});

it("confirmMainSuccessor rechecks liveness inside custody: disposal during the wait keeps the intent", async () => {
  const { mesh, owner, successor, intent } = setup();
  const target = path.join(mesh.root, "B.jsonl");
  registerMainInbox(mesh.root, A, "A", path.join(mesh.root, "A.jsonl"));
  await stageMainSuccessor(mesh, A.id, "A", target);
  const before = snapshot(owner, successor, intent(target));
  expect(before[2]).toBeDefined();
  const unlink = vi.spyOn(fs, "unlinkSync");
  const release = await hold(mesh.root);
  let alive = true;
  const call = (confirmMainSuccessor as (...args: unknown[]) => Promise<boolean>)(mesh, B.id, target, () => alive);
  expect(await pending(call)).toBe(true);
  alive = false;
  release();
  await expect(call).rejects.toThrow(/lifecycle retired/);
  expect(unlink.mock.calls.filter(([file]) => String(file) === intent(target))).toEqual([]);
  expect(snapshot(owner, successor, intent(target))).toEqual(before);
});

it("a live session waiting on the same held custody still writes normal successor metadata", async () => {
  const { mesh, owner, successor, intent } = setup();
  registerMainInbox(mesh.root, A, "A");
  let release = await hold(mesh.root);
  const record = (recordMainSuccessor as (...args: unknown[]) => Promise<void>)(mesh, A.id, "A", B.id, () => true);
  expect(await pending(record)).toBe(true);
  release();
  await record;
  expect(JSON.parse(fs.readFileSync(successor, "utf8"))).toMatchObject({ oldRoot: A.id, newRoot: B.id });
  expect(JSON.parse(fs.readFileSync(owner, "utf8"))).toMatchObject({ rootId: A.id, retired: true });

  // Confirm positive control: the intent is consumed via a recorded unlink probe, never a real delete.
  const { mesh: mesh2, owner: owner2, successor: successor2, intent: intent2 } = setup();
  const target = path.join(mesh2.root, "B.jsonl");
  registerMainInbox(mesh2.root, A, "A", path.join(mesh2.root, "A.jsonl"));
  await stageMainSuccessor(mesh2, A.id, "A", target);
  const unlinked: string[] = [];
  const realUnlink = fs.unlinkSync.bind(fs);
  // Record only the rotation-intent unlink; lock-owner cleanup still uses the real call.
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
    if (String(file) === intent2(target)) { unlinked.push(String(file)); return; }
    realUnlink(file);
  });
  release = await hold(mesh2.root);
  const confirm = (confirmMainSuccessor as (...args: unknown[]) => Promise<boolean>)(mesh2, B.id, target, () => true);
  expect(await pending(confirm)).toBe(true);
  release();
  expect(await confirm).toBe(true);
  expect(unlinked).toContain(intent2(target));
  expect(JSON.parse(fs.readFileSync(successor2, "utf8"))).toMatchObject({ oldRoot: A.id, newRoot: B.id });
  expect(JSON.parse(fs.readFileSync(owner2, "utf8"))).toMatchObject({ retired: true });
  void intent; void owner2;
});

it("no-predicate helper callers keep their existing behavior", async () => {
  const { mesh, successor } = setup();
  registerMainInbox(mesh.root, A, "A");
  await recordMainSuccessor(mesh, A.id, "A", B.id);
  expect(JSON.parse(fs.readFileSync(successor, "utf8"))).toMatchObject({ newRoot: B.id });
});
