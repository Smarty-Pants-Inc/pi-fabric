import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { createFileSystemMemorySource } from "../src/memory/fs-source.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fs-coverage-")); roots.push(root); return root; };
const list = (root: string) => createFileSystemMemorySource({ id: "archive", root }).listSessions({ limit: 10 });
const unavailable = { sessions: [], coverage: { complete: false, reason: "fs_source_unavailable" } };

it("distinguishes an empty archive from a missing configured archive", async () => {
  const root = temp();
  expect(await list(root)).toEqual([]);
  expect(await list(path.join(root, "missing"))).toEqual(unavailable);
});

it("sanitizes root scan failures as unavailable instead of complete", async () => {
  const root = temp();
  vi.spyOn(fs, "readdirSync").mockImplementation(() => { throw new Error("EACCES private archive " + root); });
  const result = await list(root);
  expect(result).toEqual(unavailable);
  expect(JSON.stringify(result)).not.toContain(root);
});

it("keeps accessible sessions but marks a failed subtree scan incomplete", async () => {
  const root = temp(); const privateDir = path.join(root, "private"); fs.mkdirSync(privateDir);
  fs.writeFileSync(path.join(root, "ok.jsonl"), '{"type":"session","id":"ok","cwd":"/work"}\n');
  const original = fs.readdirSync;
  vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, options: unknown) => {
    if (String(file) === privateDir) throw new Error("EACCES " + privateDir);
    return original(file, options as { withFileTypes: true });
  }) as typeof fs.readdirSync);
  const result = await list(root);
  expect(result).toMatchObject({ sessions: [{ sessionKey: "ok.jsonl" }], coverage: { complete: false, reason: "fs_source_incomplete" } });
  expect(JSON.stringify(result)).not.toContain(privateDir);
});

it("marks descriptor read and stat failures incomplete", async () => {
  const root = temp(); const file = path.join(root, "bad.jsonl"); fs.writeFileSync(file, "{}\n");
  const original = fs.readFileSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, options: unknown) => {
    // Secure loading reads the already-validated descriptor, not the path.
    if (typeof target === "number" || String(target) === file) throw new Error("EIO " + file);
    return original(target, options as BufferEncoding);
  }) as typeof fs.readFileSync);
  expect(await list(root)).toEqual({ sessions: [], coverage: { complete: false, reason: "fs_source_incomplete" } });
  vi.restoreAllMocks();
  const stat = fs.statSync;
  vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, options: unknown) => {
    if (String(target) === file) throw new Error("EIO " + file);
    return stat(target, options as { bigint: false });
  }) as typeof fs.statSync);
  expect(await list(root)).toMatchObject({ coverage: { complete: false, reason: "fs_source_incomplete" } });
});
