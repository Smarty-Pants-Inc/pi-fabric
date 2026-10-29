import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { retainPriorAttemptFile } from "../src/storage/retain-prior-attempt.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("prior-attempt artifact retention", () => {
  it.each(["lifecycle.jsonl", "reply.json", "status.json"])("moves %s with its exact bytes and custody receipt", name => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prior-attempt-"));
    roots.push(root);
    const file = path.join(root, name);
    const bytes = Buffer.from('{"attempt":"superseded π"}\n');
    fs.writeFileSync(file, bytes);
    const original = fs.lstatSync(file), deletions: string[] = [];
    const rmSync = fs.rmSync, unlinkSync = fs.unlinkSync, rm = fs.promises.rm, unlink = fs.promises.unlink;
    vi.spyOn(fs, "rmSync").mockImplementation((target, options) => { deletions.push(String(target)); return rmSync(target, options); });
    vi.spyOn(fs, "unlinkSync").mockImplementation(target => { deletions.push(String(target)); return unlinkSync(target); });
    vi.spyOn(fs.promises, "rm").mockImplementation((target, options) => { deletions.push(String(target)); return rm(target, options); });
    vi.spyOn(fs.promises, "unlink").mockImplementation(target => { deletions.push(String(target)); return unlink(target); });

    const retained = retainPriorAttemptFile(file);

    expect(retained).toBeDefined();
    expect(fs.existsSync(file)).toBe(false);
    expect(fs.readFileSync(retained!)).toEqual(bytes);
    const receiptPath = path.join(path.dirname(retained!), "custody.json");
    const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8"));
    expect(receipt).toMatchObject({ format: 1, source: file, destination: retained, state: "moved",
      sourceIdentity: { dev: original.dev, ino: original.ino, size: original.size } });
    expect(fs.lstatSync(retained!)).toMatchObject({ dev: original.dev, ino: original.ino, size: original.size });
    expect(deletions.filter(target => target === file || target.startsWith(`${root}${path.sep}`))).toEqual([]);
  });

  it("refuses a link without following or removing it", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prior-attempt-link-"));
    roots.push(root);
    const outside = path.join(root, "outside.json");
    const file = path.join(root, "reply.json");
    fs.writeFileSync(outside, "outside bytes");
    fs.symlinkSync(outside, file);

    expect(() => retainPriorAttemptFile(file)).toThrow(/unsafe prior-attempt artifact/);
    expect(fs.readFileSync(outside, "utf8")).toBe("outside bytes");
    expect(fs.lstatSync(file).isSymbolicLink()).toBe(true);
  });
});
