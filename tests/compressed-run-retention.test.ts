import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { canRemoveTerminalRun } from "../src/storage/retention.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const run = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "compressed-run-retention-"));
  roots.push(root);
  fs.writeFileSync(path.join(root, "status.json"), JSON.stringify({ id: "a".repeat(32), status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" }));
  fs.writeFileSync(path.join(root, "events.jsonl.zst"), "compressed");
  return root;
};

describe("compressed run retention names", () => {
  it("accepts compressed logs and exact codec temp evidence behind existing fences", () => {
    const root = run();
    for (const name of [
      ".events.jsonl.compress-123-0123456789abcdef0123456789abcdef.tmp",
      ".events.jsonl.restore-123-abcdef0123456789abcdef0123456789.tmp",
    ]) fs.writeFileSync(path.join(root, name), "evidence");
    expect(canRemoveTerminalRun(root)).toBe(true);
  });

  it("keeps malformed or unknown temporary names fail-closed", () => {
    const root = run();
    fs.writeFileSync(path.join(root, ".events.jsonl.compress-0-0123456789abcdef0123456789abcdef.tmp"), "bad");
    expect(canRemoveTerminalRun(root)).toBe(false);
  });
});
