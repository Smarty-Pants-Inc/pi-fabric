import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { savePiSettlementReceipt } from "../src/worker/settlement-receipt.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const file = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-settlement-")); roots.push(root); return path.join(root, "settlement.json"); };

describe("native successful settlement receipt (#5256)", () => {
  it("persists intentional empty output with durability barriers", () => {
    const target = file();
    const sync = vi.spyOn(fs, "fsyncSync");
    savePiSettlementReceipt(target, "run", "");
    expect(JSON.parse(fs.readFileSync(target, "utf8"))).toEqual({ version: 1, runId: "run", outcome: "completed", text: "" });
    expect(sync).toHaveBeenCalled();
  });
  it.skipIf(process.platform === "win32")("rejects a visible receipt when its post-rename namespace barrier fails", () => {
    const target = file();
    const fsync = fs.fsyncSync.bind(fs);
    let barriers = 0;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (++barriers === 2) throw new Error("receipt namespace barrier failed");
      fsync(fd);
    });
    expect(() => savePiSettlementReceipt(target, "run", "result")).toThrow("receipt namespace barrier failed");
    // Visibility is not certification: the worker sets its durable flag ONLY
    // after this function returns, including the post-rename barrier.
    expect(fs.existsSync(target)).toBe(true);
  });

  it("never certifies success if a durability barrier fails", () => {
    const target = file();
    vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("receipt barrier failed"); });
    expect(() => savePiSettlementReceipt(target, "run", "result")).toThrow("receipt barrier failed");
    expect(fs.existsSync(target)).toBe(false);
  });
});
