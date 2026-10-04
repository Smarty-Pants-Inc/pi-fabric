import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ARCHIVE_PENDING_FILE, commitRunArchive, readPendingRunArchives, stageRunArchive } from "../src/agents/archive-custody.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { runTreeExitVeto } from "../src/storage/retention.js";
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
describe("independent archive custody sinks", () => {
  it.each(["settlement", "shutdown"] as const)("a committed %s sink cannot release the other exact full outcome", kind => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "archive-custody-")); roots.push(directory);
    const result = { id: path.basename(directory), name: "full source", status: "stopped", text: "full result".repeat(10000), startedAt: 1 } as AgentRunResult;
    stageRunArchive(directory, { format: 1, kind: "settlement", result });
    stageRunArchive(directory, { format: 1, kind: "shutdown", result: { ...result, error: "Host stopped" } });
    commitRunArchive(directory, kind);
    const remaining = kind === "shutdown" ? "settlement" : "shutdown";
    expect(readPendingRunArchives(directory)).toEqual([{ format: 1, kind: remaining, result: { ...result, ...(remaining === "shutdown" ? { error: "Host stopped" } : {}) } }]);
    expect(runTreeExitVeto(directory)).toMatch(/archive is pending/);
    commitRunArchive(directory, remaining);
    expect(fs.existsSync(path.join(directory, ARCHIVE_PENDING_FILE))).toBe(false);
  });
});
