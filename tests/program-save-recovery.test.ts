import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { ProgramStore } from "../src/programs/store.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

it("repairs discovery after record publication succeeds but index publication fails", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "program-save-recovery-"));
  roots.push(root);
  const store = new ProgramStore(root);
  const original = atomic.writeJsonAtomicAsync;
  const publish = vi.spyOn(atomic, "writeJsonAtomicAsync").mockImplementation(async (file, value, options) => {
    if (path.basename(file) === "index.json") throw new Error("injected index failure");
    await original(file, value, options);
  });
  const input = { name: "recover", code: "return 42;" };
  await expect(store.save(input, "typescript")).rejects.toThrow("injected index failure");
  expect(await store.list()).toEqual([]);
  expect(fs.readdirSync(root).filter(file => /^[a-f0-9]{64}\.json$/.test(file))).toHaveLength(1);
  publish.mockRestore();
  const retry = await new ProgramStore(root).save(input, "typescript");
  expect(retry.created).toBe(false);
  expect((await store.resolve("recover")).digest).toBe(retry.record.digest);
  expect(await store.list()).toHaveLength(1);
  await store.save(input, "typescript");
  expect(await store.list()).toHaveLength(1);
});
