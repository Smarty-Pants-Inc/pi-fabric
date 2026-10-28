import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runSessionDrainCli } from "../src/cli/session-drain.js";
import { journalBranch } from "../src/seat-drain/journal-branch.js";
import { createSeat, SID, type Seat } from "./seat-drain-fixture.js";

// The bridge seam's resume read (rule T-FIX3), synthetic journals only.
const linux = process.platform === "linux" && typeof process.getuid === "function";
const sha = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");
const dirs: string[] = [];
const seats: Seat[] = [];
afterEach(async () => {
  for (const seat of seats.splice(0)) await seat.close();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const header = JSON.stringify({ type: "session", version: 3, id: SID, timestamp: "2026-10-25T00:00:00.000Z", cwd: "/synthetic" });
const line = (id: string, parentId: string | null, extra: Record<string, unknown> = {}) =>
  `${JSON.stringify({ type: "message", id, parentId, timestamp: "2026-10-25T00:00:01.000Z", ...extra })}\n`;
const SEALED = `${header}\n${line("e1", null)}${line("e2", "e1")}`;
const journal = (text: string): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "journal-branch-"));
  dirs.push(dir);
  const file = path.join(dir, "s.jsonl");
  fs.writeFileSync(file, text);
  return file;
};

describe("journalBranch: the current leaf, the sealed leaf's place on its branch, and the bytes after the seal", () => {
  it("no append: the seal IS the leaf, [] appended, an empty suffix, the prefix hash is the seal's", () => {
    expect(journalBranch(journal(SEALED), "e2", Buffer.byteLength(SEALED))).toEqual({ leafId: "e2", entries: 2, sealOnBranch: true,
      appendedSinceSeal: [], truncated: false, journalSha256: sha(SEALED), journalBytes: Buffer.byteLength(SEALED), sealedSha256: sha(SEALED),
      suffixBytes: 0, suffixBase64: "" });
  });

  it("one thinking_level_change: exactly {id, parentId, type, thinkingLevel}, and the suffix bytes", () => {
    const change = `${JSON.stringify({ type: "thinking_level_change", id: "e3", parentId: "e2", timestamp: "t", thinkingLevel: "high" })}\n`;
    const read = journalBranch(journal(SEALED + change), "e2", Buffer.byteLength(SEALED));
    expect(read).toMatchObject({ leafId: "e3", sealOnBranch: true, sealedSha256: sha(SEALED), suffixBytes: Buffer.byteLength(change),
      suffixBase64: Buffer.from(change).toString("base64"),
      appendedSinceSeal: [{ id: "e3", parentId: "e2", type: "thinking_level_change", thinkingLevel: "high" }] });
    expect(Object.keys(read.appendedSinceSeal![0]!).sort()).toEqual(["id", "parentId", "thinkingLevel", "type"]);
  });

  it("a diverged branch: the sealed leaf is not an ancestor of the current leaf", () => {
    expect(journalBranch(journal(`${SEALED}${line("e3", "e1")}`), "e2", Buffer.byteLength(SEALED)))
      .toMatchObject({ leafId: "e3", sealOnBranch: false, appendedSinceSeal: null });
  });

  it("a broken journal (missing parent, bad line) is an error, never a branch", () => {
    expect(() => journalBranch(journal(`${header}\n${line("e2", "missing")}`), "e2")).toThrow(expect.objectContaining({ code: "JOURNAL_BRANCH_BROKEN" }));
    expect(() => journalBranch(journal(`${header}\nnot json\n`), "e2")).toThrow(expect.objectContaining({ code: "JOURNAL_LINE_INVALID" }));
  });

  it("a suffix over 64 KiB is reported by size only; a file shorter than the seal has no prefix hash", () => {
    const big = line("e3", "e2", { text: "x".repeat(70 * 1024) });
    expect(journalBranch(journal(SEALED + big), "e2", Buffer.byteLength(SEALED))).toMatchObject({ suffixBase64: null,
      suffixBytes: Buffer.byteLength(big) });
    expect(journalBranch(journal(SEALED), "e2", Buffer.byteLength(SEALED) + 10)).toMatchObject({ sealedSha256: null });
  });
});

describe.skipIf(!linux)("pi-fabric session drain --identity: the server-verified, read-only identity read", () => {
  const cli = async (args: string[]) => {
    let stdout = ""; let stderr = "";
    const code = await runSessionDrainCli(args, { stdout: { write: (text: string) => { stdout += text; return true; } },
      stderr: { write: (text: string) => { stderr += text; return true; } } });
    return { code, stderr, line: stdout ? JSON.parse(stdout) as Record<string, any> : undefined };
  };

  it("reports the Main's session file, leaf and owner incarnation, and with --since reads the file itself", async () => {
    const seat = await createSeat(); seats.push(seat);
    const file = journal(SEALED);
    seat.native.state.sessionFile = file;
    seat.native.state.leafId = "e2";
    const plain = await cli([SID, "--mesh", seat.meshRoot, "--identity"]);
    expect(plain).toMatchObject({ code: 0, line: { op: "identity", sessionId: SID, identity: { rootId: `session:${SID}`, pid: process.pid,
      sessionFile: file, leafId: "e2", ownerIncarnation: "00000000-0000-4000-8000-0000000001c1" } } });
    const since = await cli([SID, "--mesh", seat.meshRoot, "--identity", "--since", "e2", "--sealed-bytes", String(Buffer.byteLength(SEALED))]);
    expect(since).toMatchObject({ code: 0, line: { journal: { leafId: "e2", sealOnBranch: true, appendedSinceSeal: [], sealedSha256: sha(SEALED) } } });
  });

  it("refuses a server that is not the session's owner before reading anything", async () => {
    const seat = await createSeat(); seats.push(seat);
    const owner = path.join(seat.meshRoot, "main-followups", `${SID}.owner.json`);
    fs.writeFileSync(owner, JSON.stringify({ ...JSON.parse(fs.readFileSync(owner, "utf8")), pid: process.pid + 100_000 }));
    expect(await cli([SID, "--mesh", seat.meshRoot, "--identity"])).toMatchObject({ code: 1, line: { error: "MAIN_DRAIN_SERVER_NOT_OWNER" } });
  });

  it("--since and --sealed-bytes belong to --identity", async () => {
    const seat = await createSeat(); seats.push(seat);
    expect((await cli([SID, "--mesh", seat.meshRoot, "--status", "--since", "e2"])).code).toBe(2);
    expect((await cli([SID, "--mesh", seat.meshRoot, "--identity", "--sealed-bytes", "10"])).code).toBe(2);
    expect((await cli([SID, "--mesh", seat.meshRoot, "--identity", "--since", "bad id"])).code).toBe(2);
  });
});
