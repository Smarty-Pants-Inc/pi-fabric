import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readJsonlPage } from "../src/log-tail.js";
import {
  FABRIC_RUN_ROOT_PREFIX, markRunRootActive, markRunRootClosed,
  pruneActorRunArchives, sweepTempRunRoots,
} from "../src/storage/retention.js";

const roots: string[] = [];
const HOUR = 3_600_000;
const now = 10 * HOUR;
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Data safety: exercise real retention entry points and the same paged file reader
// used by agents.logs, not an internal compaction mock or a source-text assertion.
describe("raw terminal run trace preservation", () => {
  it.each(["actor", "closed root"] as const)("keeps every head byte and the paging generation through %s maintenance", kind => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-raw-trace-"));
    roots.push(root);
    const runs = path.join(root, kind === "actor" ? "runs" : FABRIC_RUN_ROOT_PREFIX + "closed");
    const directory = path.join(runs, "old");
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ status: "completed",
      finishedAt: 4 * HOUR, transport: "process", sessionId: "2147483647" }));
    const file = path.join(directory, "events.jsonl");
    const original = Buffer.from(Array.from({ length: 401 }, (_, sequence) =>
      JSON.stringify({ sequence, text: sequence === 0 ? "RAW-TRACE-HEAD-🙂" : "event" }) + "\n").join(""));
    fs.writeFileSync(file, original);
    const lastPage = readJsonlPage(file, 100);
    if (kind === "closed root") {
      markRunRootActive(runs, 4 * HOUR);
      markRunRootClosed(runs, 4 * HOUR, true);
    }
    const maintain = () => kind === "actor"
      ? pruneActorRunArchives({ runsDirectory: runs, retentionMs: 24 * HOUR, now })
      : sweepTempRunRoots({ tempRoot: root, orphanedTempRunRetentionMs: 24 * HOUR,
        oneShotRunRetentionMs: 24 * HOUR, now });
    maintain();
    const after = fs.readFileSync(file);
    expect(after.includes(Buffer.from("RAW-TRACE-HEAD-🙂")), "retention dropped raw trace head bytes").toBe(true);
    expect(after.equals(original), "retention changed raw trace bytes").toBe(true);
    let page = readJsonlPage(file, 100, lastPage.before, undefined, lastPage.generation);
    const lines = [...lastPage.lines];
    for (let remaining = 4; remaining > 0; remaining--) {
      lines.unshift(...page.lines);
      if (!page.hasMore) break;
      page = readJsonlPage(file, 100, page.before, undefined, page.generation);
    }
    expect(lines.map(line => (line.parsed as { sequence: number }).sequence))
      .toEqual(Array.from({ length: 401 }, (_, sequence) => sequence));
    maintain();
    expect(fs.readFileSync(file)).toEqual(original);
  });
});
