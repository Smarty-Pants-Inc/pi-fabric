import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { removeTree } from "../src/agents/rm.js";

describe("removeTree", () => {
  it("removes a directory tree", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-rm-"));
    fs.mkdirSync(path.join(dir, "nested", "deep"), { recursive: true });
    fs.writeFileSync(path.join(dir, "nested", "deep", "file.txt"), "x");
    await removeTree(dir);
    expect(fs.existsSync(dir)).toBe(false);
  });

  it.each(["ENOTEMPTY", "EBUSY"])("retries on transient %s and then succeeds", async (code) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-rm-"));
    fs.mkdirSync(path.join(dir, "nested"), { recursive: true });
    fs.writeFileSync(path.join(dir, "nested", "file.txt"), "x");
    let calls = 0;
    await removeTree(dir, async (target, options) => {
      calls++;
      if (calls < 3) {
        const err = new Error(`transient removal failure: ${code}`) as NodeJS.ErrnoException;
        err.code = code;
        throw err;
      }
      return fs.promises.rm(target, options);
    });
    expect(calls).toBe(3);
    expect(fs.existsSync(dir)).toBe(false);
  });

  it("rethrows non-retryable errors without retrying", async () => {
    let calls = 0;
    await expect(
      removeTree("ignored", async () => {
        calls++;
        const err = new Error("permission denied") as NodeJS.ErrnoException;
        err.code = "EACCES";
        throw err;
      }),
    ).rejects.toThrow("permission denied");
    expect(calls).toBe(1);
  });
});
