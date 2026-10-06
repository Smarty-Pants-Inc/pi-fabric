import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { createMeshGrant, listMeshGrants, postWithMeshGrant } from "../src/mesh/grants.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "grant-recovery-")); roots.push(root);
  const store = new MeshStore(root, 65536, 100);
  const { token } = await createMeshGrant(store, { topic: "hooks", ttlMs: 60000, uses: 1, createdBy: { id: "main", name: "main", kind: "main" } });
  return { store, token };
};

it("does not publish when the durable grant debit fails", async () => {
  const { store, token } = await fixture();
  const original = atomic.writeFileAtomic;
  const write = vi.spyOn(atomic, "writeFileAtomic").mockImplementation((file, text, options) => {
    if (path.basename(file) === "grants.json") throw new Error("injected grant failure");
    return original(file, text, options);
  });
  await expect(postWithMeshGrant(store, { token })).rejects.toThrow("injected grant failure");
  expect(store.read({ topic: "hooks" })).toHaveLength(0);
  write.mockRestore();
  await postWithMeshGrant(new MeshStore(store.root, 65536, 100), { token });
  expect(store.read({ topic: "hooks" })).toHaveLength(1);
  expect(listMeshGrants(store)[0]!.uses).toBe(0);
});

it("recovers an indeterminate append as spent instead of authorizing a second event", async () => {
  const { store, token } = await fixture();
  const transact = store.transact.bind(store);
  vi.spyOn(store, "transact").mockImplementation(operation => transact(append => operation(input => {
    append(input);
    throw new Error("process lost after event append");
  })));
  await expect(postWithMeshGrant(store, { token })).rejects.toThrow("process lost");
  const events = store.read({ topic: "hooks" });
  expect(events).toHaveLength(1);
  expect(listMeshGrants(store)[0]!.uses).toBe(0);
  await expect(postWithMeshGrant(new MeshStore(store.root, 65536, 100), { token })).rejects.toThrow("no uses left");
  expect(store.read({ topic: "hooks" })).toEqual(events);
  const stored = JSON.parse(fs.readFileSync(path.join(store.root, "grants.json"), "utf8"));
  expect(stored.grants[0].lastEventId).toBe(events[0]!.id);
});

it("syncs the reservation before crossing the append effect boundary", async () => {
  const { store, token } = await fixture();
  const write = vi.spyOn(atomic, "writeFileAtomic");
  await postWithMeshGrant(store, { token });
  const debit = write.mock.calls.find(([file]) => path.basename(file) === "grants.json");
  expect(debit?.[2]).toMatchObject({ durable: true });
});
