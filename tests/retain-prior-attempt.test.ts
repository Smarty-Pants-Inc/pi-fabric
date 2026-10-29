import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { retainPriorAttemptFile } from "../src/storage/retain-prior-attempt.js";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("prior-attempt artifact retention", () => {
  it.each(["lifecycle.jsonl", "reply.json"])("renames %s without losing its bytes", name => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prior-attempt-"));
    roots.push(root);
    const file = path.join(root, name);
    const bytes = Buffer.from('{"attempt":"superseded π"}\n');
    fs.writeFileSync(file, bytes);

    const retained = retainPriorAttemptFile(file);

    expect(retained).toBeDefined();
    expect(retained!.startsWith(`${file}.prior-`)).toBe(true);
    expect(retained!.slice(`${file}.prior-`.length)).toMatch(/^[0-9a-f-]{36}$/);
    expect(fs.existsSync(file)).toBe(false);
    expect(fs.readFileSync(retained!)).toEqual(bytes);
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
