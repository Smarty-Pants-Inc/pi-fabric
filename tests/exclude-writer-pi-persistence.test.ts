import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

// Keep reason: data safety at the real installed SDK -> filesystem boundary.
// Controlled public pre-open currentness seam; NOT B1/OFD/native-addon,
// cross-host, process-loss, <=120s continuity, or Main qualification.
// The reported ordering is investigated, not assumed to imply a byte violation.
const roots: string[] = [];
const hash = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const user = (content: string) => ({ role: "user" as const, content, timestamp: 1 });

beforeAll(() => {
  // Public API availability is deliberately outside every expected rejection.
  expect(typeof SessionManager.openForRecoveryAfterExclusion).toBe("function");
  expect(typeof SessionManager.create).toBe("function");
  expect(typeof SessionManager.prototype.enableRecoveryPersistence).toBe("function");
  expect(typeof SessionManager.prototype.recoverRecoveryAppends).toBe("function");
});

afterEach(() => {
  vi.restoreAllMocks();
  syncBuiltinESMExports();
  // Explicitly authorized: only this test's freshly-created mkdtemp trees.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function retained(kind: "conversation" | "header-only" | "no-final-lf") {
  const root = fs.mkdtempSync(join(tmpdir(), "pi-preopen-persistence-"));
  roots.push(root);
  const sessions = join(root, "sessions");
  const original = SessionManager.create(root, sessions);
  if (kind === "header-only") original.enableRecoveryPersistence();
  else original.appendMessage(user("retained-before-receiving"));
  const file = original.getSessionFile()!;
  if (kind === "no-final-lf") {
    // A complete final JSON record lacking its LF is retained crash evidence,
    // already exercised by Pi's native recovery tests. Never alter SDK bytes.
    const bytes = fs.readFileSync(file);
    fs.writeFileSync(file, bytes.subarray(0, -1));
  }
  return { root, sessions, file, before: fs.readFileSync(file) };
}

function observe(file: string, atPersistenceOpen?: () => void) {
  const realOpen = fs.openSync.bind(fs);
  const trace: string[] = [];
  const open = vi.spyOn(fs, "openSync").mockImplementation((...args: Parameters<typeof fs.openSync>) => {
    // Delegate first: actual SDK opens the actual retained file, never a fake fd.
    const fd = realOpen(...args);
    if (String(args[0]) === file && typeof args[1] === "number" && (args[1] & fs.constants.O_RDWR) !== 0) {
      trace.push("real-retained-O_RDWR-open");
      atPersistenceOpen?.();
    }
    return fd;
  });
  const writes = {
    write: vi.spyOn(fs, "writeSync"),
    writeFile: vi.spyOn(fs, "writeFileSync"),
    appendFile: vi.spyOn(fs, "appendFileSync"),
    rename: vi.spyOn(fs, "renameSync"),
    truncate: vi.spyOn(fs, "truncateSync"),
    ftruncate: vi.spyOn(fs, "ftruncateSync"),
    fsync: vi.spyOn(fs, "fsyncSync"),
  };
  // The installed package is native ESM, external to Vitest's module mock graph.
  // Update Node's real named fs exports; all spies call the original real FS.
  syncBuiltinESMExports();
  return { trace, open, writes };
}

function counts(observation: ReturnType<typeof observe>) {
  return Object.fromEntries(Object.entries(observation.writes).map(([name, spy]) => [name, spy.mock.calls.length]));
}

describe("installed Pi guarded recovery-open persistence boundary", () => {
  it.each(["conversation", "header-only", "no-final-lf"] as const)(
    "%s: currentness loss during persistence enable rejects without changing retained bytes",
    async (kind) => {
      const t = retained(kind);
      let current = true;
      let checks = 0;
      const stale = new Error("TEST_PREOPEN_CURRENTNESS_LOST");
      const observation = observe(t.file, () => { current = false; });
      await expect(SessionManager.openForRecoveryAfterExclusion(t.file, {
        excludeWriter: async ({ sessionFile }) => {
          expect(sessionFile).toBe(t.file);
          observation.trace.push("excludeWriter-return");
          return {
            assertCurrent() {
              observation.trace.push(`assertCurrent:${++checks}:${current}`);
              if (!current) throw stale;
            },
          };
        },
      }, t.sessions, t.root)).rejects.toBe(stale);
      // The positive trigger proves this is the actual enableRecoveryPersistence
      // O_RDWR boundary, not a rejection before import/open or a stub manager.
      expect(observation.trace).toContain("real-retained-O_RDWR-open");
      expect(current).toBe(false);
      expect(observation.trace.at(-1)).toMatch(/^assertCurrent:\d+:false$/);
      expect(fs.readFileSync(t.file)).toEqual(t.before);
      expect(counts(observation)).toEqual({
        write: 0, writeFile: 0, appendFile: 0, rename: 0, truncate: 0, ftruncate: 0, fsync: 0,
      });
      console.info("PI_PREOPEN_BOUNDARY", JSON.stringify({ kind, trace: observation.trace,
        writes: counts(observation), before: hash(t.before), after: hash(fs.readFileSync(t.file)) }));
    },
  );

  it("valid currentness still permits legitimate retained-tail recovery and one native persisted append", async () => {
    const t = retained("no-final-lf");
    const observation = observe(t.file);
    let checks = 0;
    const manager = await SessionManager.openForRecoveryAfterExclusion(t.file, {
      excludeWriter: async () => ({ assertCurrent() { checks++; } }),
    }, t.sessions, t.root);
    expect(manager).toBeInstanceOf(SessionManager);
    expect(manager.getRecoveryPersistenceState()).toEqual({
      enabled: true, preparedOpen: true, exclusionValidated: true, needsRecovery: true,
    });
    expect(checks).toBeGreaterThan(0);
    expect(fs.readFileSync(t.file)).toEqual(t.before);
    expect(counts(observation)).toEqual({
      write: 0, writeFile: 0, appendFile: 0, rename: 0, truncate: 0, ftruncate: 0, fsync: 0,
    });
    const beforeExplicitGuard = checks;
    manager.assertRecoveryExclusion();
    expect(checks).toBe(beforeExplicitGuard + 1); // Guard really is installed on the receiver.
    const scan = manager.recoverRecoveryAppends();
    expect(scan.issues.map((issue) => issue.reason)).toEqual(["missing-final-lf"]);
    expect(manager.inspectRecoveryEntries().issues).toEqual([]);
    expect(fs.readFileSync(t.file)).toEqual(Buffer.concat([t.before, Buffer.from("\n")]));
    const recovered = manager.drainRecoveryAppends();
    expect(recovered).toHaveLength(1);
    const [recoveredAppend] = recovered;
    assert(recoveredAppend !== undefined, "retained-tail recovery must emit its append");
    const id = manager.appendMessage(user("legitimate-after-recovery"));
    const appends = manager.drainRecoveryAppends();
    expect(appends).toHaveLength(1);
    const [appended] = appends;
    assert(appended !== undefined, "legitimate native append must be persisted");
    expect(appended.entryId).toBe(id);
    expect(appended.flushReceipt).toMatchObject({ kind: "file-fsync", namespaceDurable: true });
    const final = fs.readFileSync(t.file);
    expect(final).toEqual(Buffer.concat([t.before, Buffer.from("\n"), Buffer.from(appended.bytes)]));
    expect(manager.getEntries().map((entry) => entry.id)).toEqual([recoveredAppend.entryId, id]);
    expect(manager.drainRecoveryAppends()).toEqual([]);
    expect(observation.writes.write).toHaveBeenCalled();
    expect(observation.writes.fsync).toHaveBeenCalled();
    expect(observation.writes.appendFile).not.toHaveBeenCalled();
    expect(observation.writes.rename).not.toHaveBeenCalled();
    console.info("PI_PREOPEN_POSITIVE", JSON.stringify({ checks, writes: counts(observation),
      before: hash(t.before), after: hash(final), recovered: recovered.length, appended: 1 }));
  });
});
