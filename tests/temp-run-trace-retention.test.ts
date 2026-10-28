import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { shortTmpdirName } from "../src/agents/short-tmpdir.js";
import { FABRIC_RUN_ROOT_PREFIX, markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

const roots: string[] = [];
const HOUR = 60 * 60 * 1_000;
const DAY = 24 * HOUR;
const temporaryDirectory = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-run-trace-retention-"));
  roots.push(root);
  return root;
};
const writeStatus = (directory: string, record: Record<string, unknown>): void => {
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify(record));
};
const filesWithBytes = (directory: string, name: string): Buffer[] => {
  const found: Buffer[] = [];
  const visit = (current: string): void => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const file = path.join(current, entry.name);
      if (entry.isDirectory()) visit(file);
      else if (entry.isFile() && entry.name.includes(name)) found.push(fs.readFileSync(file));
    }
  };
  visit(directory);
  return found;
};

afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("temporary run trace retention", () => {
  it("keeps every raw JSONL trace and metadata anchor byte-identical while pruning eligible bloat", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}closed`);
    markRunRootActive(root, 1);
    const run = path.join(root, "run");
    writeStatus(run, { status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    fs.writeFileSync(path.join(run, "task.txt"), "completed task payload");
    fs.writeFileSync(path.join(run, "reply.json"), '{"text":"completed result"}');
    fs.writeFileSync(path.join(run, "run-identity.json"), '{"run":"stable"}');
    fs.mkdirSync(path.join(run, "tmp", "cache"), { recursive: true });
    fs.writeFileSync(path.join(run, "tmp", "output.txt"), "temporary output");
    fs.writeFileSync(path.join(run, "tmp", "cache", "compiler.cache"), "temporary cache");

    const traces: Array<{ relative: string; bytes: Buffer }> = [
      { relative: "events.jsonl", bytes: Buffer.from('{"event":"original"}\n') },
      { relative: "lifecycle.jsonl", bytes: Buffer.from('{"phase":"finished"}\n') },
      { relative: "steer.jsonl", bytes: Buffer.from('{"type":"worker_observation"}\n') },
      { relative: "route-session.jsonl", bytes: Buffer.from('{"session":"route"}\n') },
      { relative: "session.jsonl", bytes: Buffer.from('{"message":"session"}\n') },
      { relative: "session.jsonl.20260101T000000Z.bak", bytes: Buffer.from('{"message":"rotated backup"}\n') },
      { relative: "session.jsonl.orphaned.bak", bytes: Buffer.from('{"message":"recovery backup"}\n') },
      { relative: "route-session.jsonl.reclaim", bytes: Buffer.from('{"session":"reclaim"}\n') },
      { relative: "tmp/worker-session.jsonl", bytes: Buffer.from('{"worker":"scratch"}\n') },
      { relative: "tmp/cache/session.jsonl.reclaim", bytes: Buffer.from('{"worker":"scratch reclaim"}\n') },
      { relative: "durable/initial/store/main.jsonl", bytes: Buffer.from('{"durable":"main"}\n') },
      { relative: "durable/initial/store/doc-0.jsonl", bytes: Buffer.from('{"durable":"doc"}\n') },
      { relative: "durable/initial/store/doc-1.jsonl.reclaim", bytes: Buffer.from('{"durable":"reclaim"}\n') },
      { relative: "handoff-session/fork.jsonl", bytes: Buffer.from('{"handoff":"fork"}\n') },
      { relative: "nested/worker/events.jsonl", bytes: Buffer.from('{"nested":"events"}\n') },
      { relative: "nested/worker/session.jsonl.backup", bytes: Buffer.from('{"nested":"session backup"}\n') },
    ];
    for (const trace of traces) {
      const file = path.join(run, trace.relative);
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, trace.bytes);
    }
    writeStatus(path.join(run, "nested", "worker"), {
      status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647",
    });

    markRunRootClosed(root, 1, true);
    const ownerBefore = fs.readFileSync(path.join(root, ".fabric-owner.json"));
    const statusBefore = fs.readFileSync(path.join(run, "status.json"));
    const identityBefore = fs.readFileSync(path.join(run, "run-identity.json"));
    const nestedStatusBefore = fs.readFileSync(path.join(run, "nested", "worker", "status.json"));
    const kill = vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2_147_483_647) throw Object.assign(new Error("process is absent"), { code: "ESRCH" });
      return true;
    });
    try {
      sweepTempRunRoots({
        tempRoot,
        now: 100 * DAY,
        orphanedTempRunRetentionMs: 6 * HOUR,
        oneShotRunRetentionMs: DAY,
      });

      expect(fs.readFileSync(path.join(root, ".fabric-owner.json"))).toEqual(ownerBefore);
      expect(fs.readFileSync(path.join(run, "status.json"))).toEqual(statusBefore);
      expect(fs.readFileSync(path.join(run, "run-identity.json"))).toEqual(identityBefore);
      expect(fs.readFileSync(path.join(run, "nested", "worker", "status.json"))).toEqual(nestedStatusBefore);
      for (const trace of traces) {
        expect(fs.readFileSync(path.join(run, trace.relative)), trace.relative).toEqual(trace.bytes);
      }
      for (const file of ["task.txt", "reply.json", "tmp/output.txt", "tmp/cache/compiler.cache"]) {
        expect(fs.existsSync(path.join(run, file)), file).toBe(false);
      }
      expect(filesWithBytes(run, ".jsonl")).toHaveLength(traces.length);
    } finally {
      kill.mockRestore();
    }
  });

  it("keeps a trace in its recorded external short TMPDIR while pruning scratch bloat", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}external-scratch`);
    markRunRootActive(root, 1);
    const runId = "external-run";
    const run = path.join(root, runId);
    const scratch = path.join(tempRoot, shortTmpdirName(runId));
    fs.mkdirSync(scratch, { mode: 0o700 });
    writeStatus(run, {
      status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647", temporaryDirectory: scratch,
    });
    fs.writeFileSync(path.join(run, "task.txt"), "eligible task bloat");
    fs.writeFileSync(path.join(run, "reply.json"), '{"text":"eligible reply bloat"}');
    const scratchTrace = Buffer.from('{"worker":"external scratch trace"}\n');
    fs.writeFileSync(path.join(scratch, "worker-session.jsonl"), scratchTrace);
    fs.mkdirSync(path.join(scratch, "cache"), { recursive: true });
    fs.writeFileSync(path.join(scratch, "cache", "compiler.cache"), "eligible scratch cache");
    fs.writeFileSync(path.join(scratch, "output.txt"), "eligible scratch output");
    markRunRootClosed(root, 1, true);
    const statusBefore = fs.readFileSync(path.join(run, "status.json"));
    const kill = vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2_147_483_647) throw Object.assign(new Error("process is absent"), { code: "ESRCH" });
      return true;
    });
    try {
      const result = sweepTempRunRoots({
        tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
      });
      expect(result).toEqual({ removedRuns: [], removedRoots: [] });
      expect(fs.existsSync(run)).toBe(true);
      expect(fs.readFileSync(path.join(run, "status.json"))).toEqual(statusBefore);
      expect(fs.readFileSync(path.join(scratch, "worker-session.jsonl"))).toEqual(scratchTrace);
      for (const file of ["task.txt", "reply.json"]) expect(fs.existsSync(path.join(run, file))).toBe(false);
      for (const file of ["cache/compiler.cache", "output.txt"]) expect(fs.existsSync(path.join(scratch, file))).toBe(false);
    } finally {
      kill.mockRestore();
    }
  });

  it.skipIf(process.platform !== "linux")("keeps a trace created after artifact judgment during verified removal", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}trace-race`);
    markRunRootActive(root, 1);
    const run = path.join(root, "run");
    fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "task.txt"), "eligible task bloat");
    writeStatus(run, { status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    const originalTrace = Buffer.from('{"event":"judged trace"}\n');
    const lateTrace = Buffer.from('{"event":"appeared during removal"}\n');
    fs.writeFileSync(path.join(run, "events.jsonl"), originalTrace);
    markRunRootClosed(root, 1, true);
    const statusBefore = fs.readFileSync(path.join(run, "status.json"));
    let injected = false;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (target === path.join(run, "task.txt") && !injected) {
        injected = true;
        fs.writeFileSync(path.join(run, "late-worker.jsonl"), lateTrace);
      }
    };
    const kill = vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2_147_483_647) throw Object.assign(new Error("process is absent"), { code: "ESRCH" });
      return true;
    });
    try {
      const result = sweepTempRunRoots({
        tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
      });
      expect(injected).toBe(true);
      expect(result).toEqual({ removedRuns: [], removedRoots: [] });
      expect(fs.existsSync(run)).toBe(true);
      expect(fs.readFileSync(path.join(run, "status.json"))).toEqual(statusBefore);
      expect(fs.readFileSync(path.join(run, "events.jsonl"))).toEqual(originalTrace);
      expect(fs.readFileSync(path.join(run, "late-worker.jsonl"))).toEqual(lateTrace);
      // The late trace changes the judged root namespace/mtime: preserve eligible bloat too rather than partially prune.
      expect(fs.existsSync(path.join(run, "task.txt"))).toBe(true);
    } finally {
      delete verifiedRemoveTestHooks.beforeOpen;
      kill.mockRestore();
    }
  });

  it("keeps unresolved nested-worker custody and all of its bytes intact", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, `${FABRIC_RUN_ROOT_PREFIX}nested-custody`);
    markRunRootActive(root, 1);
    const parent = path.join(root, "parent");
    writeStatus(parent, { status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    fs.writeFileSync(path.join(parent, "task.txt"), "parent payload");
    const child = path.join(parent, "nested", "worker");
    writeStatus(child, { status: "failed", finishedAt: 1, transport: "process", sessionId: "2147483647" });
    fs.writeFileSync(path.join(child, "events.jsonl"), '{"child":"trace"}\n');
    fs.writeFileSync(path.join(child, "unresolved-worker.json"), '{"reason":"worker custody is unresolved"}');
    markRunRootClosed(root, 1, true);
    const before = new Map<string, Buffer>();
    const visit = (directory: string): void => {
      for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        const file = path.join(directory, entry.name);
        if (entry.isDirectory()) visit(file);
        else if (entry.isFile()) before.set(path.relative(root, file), fs.readFileSync(file));
      }
    };
    visit(root);
    const kill = vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2_147_483_647) throw Object.assign(new Error("process is absent"), { code: "ESRCH" });
      return true;
    });
    try {
      sweepTempRunRoots({
        tempRoot,
        now: 100 * DAY,
        orphanedTempRunRetentionMs: 6 * HOUR,
        oneShotRunRetentionMs: DAY,
      });
      for (const [relative, bytes] of before) {
        expect(fs.readFileSync(path.join(root, relative)), relative).toEqual(bytes);
      }
      expect(fs.existsSync(parent)).toBe(true);
      expect(fs.existsSync(child)).toBe(true);
    } finally {
      kill.mockRestore();
    }
  });
});
