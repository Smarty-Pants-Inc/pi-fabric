import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it } from "vitest";
import { MeshStore, type MeshStoreOptions } from "../src/mesh/store.js";

it.each(["publish", "put", "batch", "confirm"] as const)("retires an in-flight %s lock waiter without committing or affecting another store", async operation => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-write-stop-"));
  const controller = new AbortController();
  const mesh = new MeshStore(root, 65536, 1000, { writeSignal: controller.signal } as MeshStoreOptions);
  const peer = new MeshStore(root, 65536, 1000);
  const identity = { id: "owner", name: "owner", kind: "main" as const };
  const lock = path.join(root, ".lock");
  fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), `test\n${process.pid}\n${Date.now()}\n`);
  const pending = (operation === "publish" ? mesh.publish({ topic: "work", from: identity })
    : operation === "put" ? mesh.put({ key: "work", value: "no", identity })
    : operation === "batch" ? mesh.writeBatch({ identity, ops: [{ kind: "put", key: "work", value: "no" }] })
    : mesh.confirmWritable()).then(() => ({ committed: true }), error => ({ error }));
  try {
    await new Promise(resolve => setTimeout(resolve, 30));
    controller.abort(new Error("host closing: retire publication"));
    fs.rmSync(lock, { recursive: true });
    expect(await pending).toMatchObject({ error: expect.objectContaining({ message: "host closing: retire publication" }) });
    expect(mesh.tail(0, 10).events).toHaveLength(0); expect(mesh.get("work")).toBeUndefined();
    await expect(mesh.put({ key: "later", value: "no", identity })).rejects.toThrow("retire publication");
    await peer.put({ key: "peer", value: "yes", identity }); expect(mesh.get("peer")?.value).toBe("yes");
  } finally { fs.rmSync(lock, { recursive: true, force: true }); await pending; fs.rmSync(root, { recursive: true, force: true }); }
});
