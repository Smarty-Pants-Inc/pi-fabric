import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const identity = { id: "actor:probe", name: "probe", kind: "actor" as const };

describe("disposable actor mesh-write lifetime (#5256)", () => {
  it.each([1, 2] as const)("aborts every waiting admission without touching protocol %s's holder or committed state", async lockProtocol => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-cancel-")); roots.push(root);
    const controller = new AbortController();
    const store = new MeshStore(root, 64 * 1024, 100, { lockProtocol, writeSignal: controller.signal });
    await store.put({ key: "keep/value", value: "committed", identity });
    const lock = path.join(root, ".lock"); fs.mkdirSync(lock);
    const owner = `owned-fixture\n${process.pid}\n${Date.now()}\n`;
    fs.writeFileSync(path.join(lock, "owner"), owner);
    let attempts = 0;
    const stat = fs.lstatSync.bind(fs);
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: fs.StatOptions) => {
      if (String(file) === lock) attempts++;
      return stat(file, options);
    }) as typeof fs.lstatSync);
    const callback = vi.fn();
    const pending = [
      store.put({ key: "cancel/value", value: "not committed", identity }),
      store.delete({ key: "keep/value" }),
      store.publish({ topic: "cancel.notice", from: identity }),
      store.writeBatch({ ops: [{ kind: "delete", key: "keep/value" }], identity }),
      store.confirmWritable(callback),
      store.exclusive(callback),
    ].map(promise => promise.then(() => "unexpected commit", error => error));
    // A real acquisition attempt, not a timing sleep, establishes the blocked path.
    try { await vi.waitFor(() => expect(attempts).toBeGreaterThan(0), { timeout: 5_000 }); }
    finally { controller.abort(); await Promise.all(pending); }
    expect((await Promise.all(pending)).every(error => error?.name === "AbortError")).toBe(true);
    expect(callback).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
    expect(store.get("keep/value")?.value).toBe("committed");
    expect(store.get("cancel/value")).toBeUndefined();
    expect(store.read({ topic: "cancel.notice" })).toEqual([]);
    fs.rmSync(lock, { recursive: true }); // only the fixture's own holder
    await expect(store.put({ key: "later/value", value: 1, identity })).rejects.toMatchObject({ name: "AbortError" });
    // Cancellation belongs to this store lifetime, not the shared mesh or another owner.
    await new MeshStore(root, 64 * 1024, 100).put({ key: "other/value", value: 2, identity });
  });

  it("does not roll back a synchronous operation already admitted under the lock", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mesh-cancel-")); roots.push(root);
    const controller = new AbortController();
    const store = new MeshStore(root, 64 * 1024, 100, { writeSignal: controller.signal });
    expect(await store.exclusive(() => { controller.abort(); fs.writeFileSync(path.join(root, "admitted"), "kept"); return "committed"; })).toBe("committed");
    expect(fs.readFileSync(path.join(root, "admitted"), "utf8")).toBe("kept");
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  });
});
