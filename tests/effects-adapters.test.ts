import { afterEach, describe, expect, it, vi } from "vitest";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { chmod, link, lstat, mkdir, mkdtemp, open, readFile, readdir, symlink, writeFile } from "node:fs/promises";
import type { FileHandle } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import {
  commitFileEffect, effectBytesHash, MAX_EFFECT_FILE_BYTES, prepareFileEffect, reconcileFileEffect,
  type PreparedFileEffect,
} from "../src/effects/file.js";
import {
  executeShellEffect, prepareShellEffect, shellEffectCommand, type CertifiedShellProfile,
} from "../src/effects/shell.js";
import { createEffectFailpoints, type EffectAdapterControls, type EffectFailpointName } from "../src/effects/failpoints.js";

// Keep reason: data safety + trust boundary. These checks use actual files,
// fsync/rename operations and a fixed-program child, including process kills in
// windows a/b and the staging/rename gaps that are expensive in a native flow.
// They do NOT qualify power loss, B6's post-append c, B5's receiver d, record
// orchestration, or direct/nested tool dispatch. Scratch/evidence is retained.
const live: EffectAdapterControls = { assertLive() {} };
async function workspace(): Promise<string> {
  const base = await mkdtemp(join(tmpdir(), "fabric-effects-adapters-"));
  const root = join(base, "workspace");
  await mkdir(root);
  return root;
}
function portable<T>(value: T): T { return JSON.parse(JSON.stringify(value)) as T; }
function sortedJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(sortedJson).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${sortedJson((value as Record<string, unknown>)[key])}`).join(",")}}`;
  return JSON.stringify(value);
}
async function bytes(root: string, path = "file.txt"): Promise<string> { return readFile(join(root, path), "utf8"); }
async function inode(root: string): Promise<bigint> { return (await lstat(join(root, "file.txt"), { bigint: true })).ino; }

afterEach(() => vi.restoreAllMocks());

describe("host-owned durable boundary instrumentation", () => {
  const identity = { logicalSessionId: "logical", podUID: "pod-1", bootId: "boot-1", ownerIncarnation: "owner-1" };
  it("is inert by default and refuses malformed selected boundary identities", async () => {
    const root = await workspace();
    const attemptFile = join(root, "attempts.jsonl");
    const reached = vi.fn();
    await createEffectFailpoints({ attemptFile, identity, onReached: reached })("intent-durable");
    expect(reached).not.toHaveBeenCalled();
    expect(await readdir(root)).toEqual([]);
    const selected = createEffectFailpoints({ attemptFile, identity, points: ["intent-durable"], pause: async () => {} });
    await expect(selected("intent-durable", { key: "physical-call-id", toolCallId: "call-1" })).rejects.toThrow("original effect key");
    await expect(selected("intent-durable", { key: "a".repeat(64), toolCallId: "" })).rejects.toThrow("physical-call alias");
    expect(await readdir(root)).toEqual([]);
    expect(() => createEffectFailpoints({ attemptFile: "relative", identity })).toThrow("absolute provisioned path");
  });

  it("durably appends all raw attempts before callback/pause without serializing paused effects", async () => {
    const root = await workspace();
    const attemptFile = join(root, "attempts.jsonl");
    let release!: () => void;
    let allReached!: () => void;
    const paused = new Promise<void>(resolve => { release = resolve; });
    const reached = new Promise<void>(resolve => { allReached = resolve; });
    let count = 0;
    const fire = createEffectFailpoints({ attemptFile, identity, points: ["intent-durable"],
      async onReached(receipt) {
        const rows = (await readFile(attemptFile, "utf8")).trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
        expect(rows.some(row => row.toolCallId === receipt.toolCallId && row.key === receipt.key)).toBe(true);
        if (++count === 2) allReached();
      }, pause: () => paused,
    });
    const details = { key: "b".repeat(64), toolCallId: "physical-one", originalBytes: "first" };
    const first = fire("intent-durable", details);
    details.originalBytes = "caller changed its object";
    const second = fire("intent-durable", { key: "b".repeat(64), toolCallId: "physical-two", originalBytes: "second" });
    try {
      await Promise.race([reached, Promise.all([first, second])]);
      const rows = (await readFile(attemptFile, "utf8")).trim().split("\n").map(line => JSON.parse(line) as { details: { originalBytes: string }; toolCallId: string });
      expect(rows.map(row => row.toolCallId)).toEqual(["physical-one", "physical-two"]);
      expect(rows.map(row => row.details.originalBytes)).toEqual(["first", "second"]);
      expect((await lstat(attemptFile)).mode & 0o777).toBe(0o600);
    } finally { release(); await Promise.all([first, second]); }
  });
});

describe("durable literal file sink", () => {
  it("prepares immutable JSON-portable bytes, distinguishes absent/empty, preserves result isolation", async () => {
    const root = await workspace();
    const intent = await prepareFileEffect(root, "write", { path: "file.txt", content: "" });
    expect(intent.pre).toEqual({ exists: false, sha256: null, mode: null, size: 0 });
    expect(intent.post).toEqual({ exists: true, sha256: effectBytesHash(""), mode: 0o644, size: 0 });
    expect(Object.isFrozen(intent)).toBe(true);
    expect(Object.isFrozen(intent.preparedResult.content[0])).toBe(true);
    const stored = JSON.parse(sortedJson(intent)) as PreparedFileEffect;
    const result = await commitFileEffect(stored, live);
    expect(await bytes(root)).toBe("");
    (result.content[0] as { text: string }).text = "caller changed result";
    const originalInode = await inode(root);
    const replay = await reconcileFileEffect(stored, live);
    expect(replay.outcome).toBe("committed");
    expect(replay.result).toEqual(intent.preparedResult);
    expect(await inode(root)).toBe(originalInode);
    expect(intent.preparedResult.content[0]!.text).not.toBe("caller changed result");
    const emptyPre = await prepareFileEffect(root, "write", { path: "file.txt", content: "new" });
    expect(emptyPre.pre.exists).toBe(true);
    expect(emptyPre.pre.sha256).not.toBeNull();
  });

  it("uses unique disjoint original-byte matches, preserves BOM/CRLF and exact mode", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "\ufeffalpha\r\nbeta\r\ngamma\r\n");
    await chmod(join(root, "file.txt"), 0o751);
    const intent = await prepareFileEffect(root, "edit", {
      path: "file.txt", edits: [{ oldText: "gamma", newText: "alpha" }, { oldText: "alpha", newText: "beta" }],
    });
    expect(intent.pre.mode).toBe(0o751);
    expect(Buffer.from(intent.desiredBase64, "base64").toString()).toBe("\ufeffbeta\r\nbeta\r\nalpha\r\n");
    const seen: string[] = [];
    const result = await commitFileEffect(portable(intent), {
      assertLive() {},
      async failpoint(point, details) {
        seen.push(point);
        if (point === "file-temp-synced") {
          const staged = await readFile(details!.tempPath as string);
          expect(staged.toString("base64")).toBe(intent.desiredBase64);
          expect((await lstat(details!.tempPath as string)).mode & 0o7777).toBe(0o751);
          expect(await bytes(root)).toBe("\ufeffalpha\r\nbeta\r\ngamma\r\n");
        }
        if (point === "destination-committed") expect(await bytes(root)).toBe("\ufeffbeta\r\nbeta\r\nalpha\r\n");
      },
    });
    expect(seen).toEqual(["file-temp-synced", "file-renamed", "destination-committed"]);
    expect(result.content).toEqual([{ type: "text", text: "Successfully replaced 2 block(s) in file.txt." }]);
    expect((await lstat(join(root, "file.txt"))).mode & 0o7777).toBe(0o751);
  });

  it.each([
    { original: "aaaa", edits: [{ oldText: "aa", newText: "x" }] },
    { original: "abcde", edits: [{ oldText: "abc", newText: "x" }, { oldText: "bcd", newText: "y" }] },
    { original: "abc", edits: [{ oldText: "", newText: "x" }] },
    { original: "abc", edits: [{ oldText: "not present", newText: "x" }] },
  ])("refuses ambiguous/overlapping/empty/missing literal matches without mutation: %j", async ({ original, edits }) => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), original);
    await expect(prepareFileEffect(root, "edit", { path: "file.txt", edits })).rejects.toThrow();
    expect(await bytes(root)).toBe(original);
    expect(await readdir(root)).toEqual(["file.txt"]);
  });

  it("does not normalize literal text, accept legacy shapes, edit invalid UTF-8, or create parents", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "a\r\nb");
    await expect(prepareFileEffect(root, "edit", { path: "file.txt", edits: [{ oldText: "a\nb", newText: "x" }] })).rejects.toThrow();
    await expect(prepareFileEffect(root, "edit", { path: "file.txt", oldText: "a", newText: "x" })).rejects.toThrow();
    await expect(prepareFileEffect(root, "edit", { path: "file.txt", edits: "[]" })).rejects.toThrow();
    await writeFile(join(root, "invalid.txt"), Buffer.from([0xff]));
    await expect(prepareFileEffect(root, "edit", { path: "invalid.txt", edits: [{ oldText: "x", newText: "y" }] })).rejects.toThrow(/UTF-8/);
    await expect(prepareFileEffect(root, "write", { path: "missing/child", content: "x" })).rejects.toThrow();
    expect(await readdir(root)).toEqual(["file.txt", "invalid.txt"]);
  });

  it("refuses traversal, symlink ancestors/roots/targets, hardlinks and nonregular targets", async () => {
    const root = await workspace();
    const outside = join(dirname(root), "outside");
    await mkdir(outside);
    await writeFile(join(outside, "valuable"), "outside");
    await symlink(outside, join(root, "alias"));
    await symlink(join(outside, "valuable"), join(root, "target-link"));
    await link(join(outside, "valuable"), join(root, "target-hardlink"));
    await mkdir(join(root, "directory"));
    for (const path of ["../outside/valuable", "alias/valuable", "target-link", "target-hardlink", "directory", "/etc/passwd", "sub/../file.txt", "./file.txt", "x\\y"]) {
      await expect(prepareFileEffect(root, "write", { path, content: "bad" })).rejects.toThrow();
    }
    const rootLink = join(dirname(root), "workspace-link");
    await symlink(root, rootLink);
    await expect(prepareFileEffect(rootLink, "write", { path: "file.txt", content: "bad" })).rejects.toThrow();
    expect(await readFile(join(outside, "valuable"), "utf8")).toBe("outside");
    expect(await readdir(root)).toEqual(["alias", "directory", "target-hardlink", "target-link"]);
  });

  it("rejects non-Unicode/bounded inputs and corrupt or unsupported stored payloads without redo", async () => {
    const root = await workspace();
    await expect(prepareFileEffect(root, "write", { path: "file.txt", content: "\ud800" })).rejects.toThrow();
    await expect(prepareFileEffect(root, "write", { path: "file.txt", content: "x".repeat(MAX_EFFECT_FILE_BYTES + 1) })).rejects.toThrow();
    const intent = await prepareFileEffect(root, "write", { path: "file.txt", content: "desired" });
    const corruptions = [
      { format: "future-version" }, { desiredBase64: Buffer.from("wrong").toString("base64") },
      { integrity: "0".repeat(64) }, { pre: { exists: true, sha256: null, mode: 0o644, size: 0 } },
      { preparedResult: { content: [{ type: "text", text: "forged" }], details: {} } },
    ];
    for (const change of corruptions) {
      const outcome = await reconcileFileEffect({ ...portable(intent), ...change } as PreparedFileEffect, live);
      expect(outcome.outcome).toBe("uncertain");
    }
    expect(await readdir(root)).toEqual([]);
  });

  it("neither rematches an interrupted edit nor overwrites later bytes/mode/existence", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "old");
    const intent = await prepareFileEffect(root, "edit", { path: "file.txt", edits: [{ oldText: "old", newText: "new" }] });
    await writeFile(join(root, "file.txt"), "old plus external change");
    expect((await reconcileFileEffect(intent, live)).outcome).toBe("uncertain");
    expect(await bytes(root)).toBe("old plus external change");
    await writeFile(join(root, "file.txt"), "old");
    await chmod(join(root, "file.txt"), 0o600);
    expect((await reconcileFileEffect(intent, live)).outcome).toBe("uncertain");
    expect((await lstat(join(root, "file.txt"))).mode & 0o777).toBe(0o600);
    const absentIntent = await prepareFileEffect(root, "write", { path: "absent", content: "new" });
    await writeFile(join(root, "absent"), "");
    expect((await reconcileFileEffect(absentIntent, live)).outcome).toBe("uncertain");
    expect(await readFile(join(root, "absent"), "utf8")).toBe("");
  });

  it("serializes parallel same-target commits and refuses an obsolete pre-state", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "pre");
    const first = await prepareFileEffect(root, "write", { path: "file.txt", content: "first" });
    const second = await prepareFileEffect(root, "write", { path: "file.txt", content: "second" });
    let reached!: () => void, resume!: () => void;
    const atTemp = new Promise<void>((resolve) => { reached = resolve; });
    const gate = new Promise<void>((resolve) => { resume = resolve; });
    const active = commitFileEffect(first, {
      assertLive() {}, async failpoint(point) { if (point === "file-temp-synced") { reached(); await gate; } },
    });
    await atTemp;
    const queued = reconcileFileEffect(second, live);
    resume();
    await active;
    expect((await queued).outcome).toBe("uncertain");
    expect(await bytes(root)).toBe("first");
    expect((await readdir(root)).filter((name) => name.endsWith(".tmp"))).toEqual([]);
  });

  it("joins duplicate commits without replacing the inode twice, and no-ops an identical file", async () => {
    const root = await workspace();
    const intent = await prepareFileEffect(root, "write", { path: "file.txt", content: "same" });
    let renames = 0;
    const controls: EffectAdapterControls = { assertLive() {}, failpoint(point) { if (point === "file-renamed") renames++; } };
    const results = await Promise.all([commitFileEffect(intent, controls), commitFileEffect(intent, controls)]);
    expect(results[0]).toEqual(results[1]);
    expect(renames).toBe(1);
    const before = await inode(root);
    const noop = await prepareFileEffect(root, "write", { path: "file.txt", content: "same" });
    await commitFileEffect(noop, controls);
    expect(await inode(root)).toBe(before);
    expect(renames).toBe(1);
  });

  it("rechecks authority and target identity at the rename boundary; retains staged evidence", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "pre");
    const intent = await prepareFileEffect(root, "write", { path: "file.txt", content: "post" });
    let isLive = true;
    const outcome = await reconcileFileEffect(intent, {
      assertLive() { if (!isLive) throw new Error("stale owner"); },
      failpoint(point) { if (point === "file-temp-synced") isLive = false; },
    });
    expect(outcome.outcome).toBe("uncertain");
    expect(await bytes(root)).toBe("pre");
    expect((await readdir(root)).filter((name) => name.endsWith(".tmp"))).toHaveLength(1);
    const changed = await reconcileFileEffect(intent, {
      assertLive() {}, async failpoint(point) { if (point === "file-temp-synced") await writeFile(join(root, "file.txt"), "other"); },
    });
    expect(changed.outcome).toBe("uncertain");
    expect(await bytes(root)).toBe("other");
    expect((await readdir(root)).filter((name) => name.endsWith(".tmp"))).toHaveLength(2);
  });

  it.each(["file", "directory"] as const)("failed %s fsync stays uncertain and preserves recovery evidence", async (failureKind) => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "pre");
    const intent = await prepareFileEffect(root, "write", { path: "file.txt", content: "post" });
    const probe = await open(join(root, "file.txt"), "r");
    const prototype = Object.getPrototypeOf(probe) as FileHandle;
    const originalSync = prototype.sync;
    await probe.close();
    const sync = vi.spyOn(prototype, "sync").mockImplementation(async function (this: FileHandle) {
      const stat = await this.stat();
      if (failureKind === "directory" ? stat.isDirectory() : stat.isFile()) {
        throw Object.assign(new Error("injected storage fsync error"), { code: "EIO" });
      }
      await originalSync.call(this);
    });
    const outcome = await reconcileFileEffect(intent, live);
    expect(outcome.outcome).toBe("uncertain");
    expect(outcome.reason).toContain("fsync error");
    expect(await bytes(root)).toBe(failureKind === "file" ? "pre" : "post");
    const priorInode = await inode(root);
    expect((await readdir(root)).filter((name) => name.endsWith(".tmp"))).toHaveLength(failureKind === "file" ? 1 : 0);
    sync.mockRestore();
    expect((await reconcileFileEffect(intent, live)).outcome).toBe("committed");
    expect(await bytes(root)).toBe("post");
    if (failureKind === "directory") expect(await inode(root)).toBe(priorInode);
  });

  it("syncs every workspace ancestor bottom-up and keeps an ancestor-barrier error uncertain", async () => {
    const root = await workspace();
    const one = join(root, "one"), two = join(one, "two");
    await mkdir(two, { recursive: true });
    const directories = await Promise.all([root, one, two].map((path) => lstat(path, { bigint: true })));
    const intent = await prepareFileEffect(root, "write", { path: "one/two/file.txt", content: "post" });
    const probe = await open(root, "r");
    const prototype = Object.getPrototypeOf(probe) as FileHandle;
    const originalSync = prototype.sync;
    await probe.close();
    const seen: bigint[] = [];
    const sync = vi.spyOn(prototype, "sync").mockImplementation(async function (this: FileHandle) {
      const stat = await this.stat({ bigint: true });
      if (stat.isDirectory()) {
        seen.push(stat.ino);
        if (stat.ino === directories[0]!.ino) throw new Error("workspace ancestor fsync failed");
      }
      await originalSync.call(this);
    });
    const outcome = await reconcileFileEffect(intent, live);
    expect(outcome.outcome).toBe("uncertain");
    expect(outcome.reason).toContain("ancestor fsync failed");
    expect(seen).toEqual(directories.map((stat) => stat.ino).reverse());
    expect(await readFile(join(two, "file.txt"), "utf8")).toBe("post");
    const priorInode = (await lstat(join(two, "file.txt"), { bigint: true })).ino;
    sync.mockRestore();
    expect((await reconcileFileEffect(intent, live)).outcome).toBe("committed");
    expect((await lstat(join(two, "file.txt"), { bigint: true })).ino).toBe(priorInode);
  });

  it("partial staging followed by ENOSPC retains evidence and never changes the destination", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "pre");
    const intent = await prepareFileEffect(root, "write", { path: "file.txt", content: "post" });
    const probe = await open(join(root, "file.txt"), "r");
    const prototype = Object.getPrototypeOf(probe) as FileHandle;
    const originalWrite = prototype.writeFile;
    await probe.close();
    const write = vi.spyOn(prototype, "writeFile").mockImplementation(async function (this: FileHandle) {
      await originalWrite.call(this, Buffer.from("partial"));
      throw Object.assign(new Error("injected ENOSPC after partial staging"), { code: "ENOSPC" });
    });
    const outcome = await reconcileFileEffect(intent, live);
    expect(outcome.outcome).toBe("uncertain");
    expect(outcome.reason).toContain("ENOSPC");
    expect(await bytes(root)).toBe("pre");
    const evidence = (await readdir(root)).filter((name) => name.endsWith(".tmp"));
    expect(evidence).toHaveLength(1);
    expect(await readFile(join(root, evidence[0]!), "utf8")).toBe("partial");
    write.mockRestore();
    expect((await reconcileFileEffect(intent, live)).outcome).toBe("committed");
    expect(await bytes(root)).toBe("post");
    expect(await readFile(join(root, evidence[0]!), "utf8")).toBe("partial");
  });

  it("an actual rename permission error does not fall back to in-place writing", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "pre");
    const intent = await prepareFileEffect(root, "write", { path: "file.txt", content: "post" });
    const before = await inode(root);
    let outcome;
    try {
      outcome = await reconcileFileEffect(intent, { assertLive() {}, async failpoint(point) { if (point === "file-temp-synced") await chmod(root, 0o500); } });
    } finally { await chmod(root, 0o700); }
    expect(outcome!.outcome).toBe("uncertain");
    expect(await bytes(root)).toBe("pre");
    expect(await inode(root)).toBe(before);
    expect((await readdir(root)).filter((name) => name.endsWith(".tmp"))).toHaveLength(1);
  });
});

// Durable outer intent is deliberately a small test harness, not a fake B4
// records store or B6 native receipt. The adapter itself supplies the three file
// callbacks; the harness supplies the actual intent-file fsync barrier for a.
const CHILD = `
import { prepareFileEffect, commitFileEffect } from ${JSON.stringify(resolve("src/effects/file.ts"))};
import { open } from 'node:fs/promises';
import { dirname } from 'node:path';
const [root, intentFile, selected] = process.argv.slice(1);
const intent = await prepareFileEffect(root, 'edit', {path:'file.txt', edits:[{oldText:'old',newText:'stored successor bytes'}]});
const record = await open(intentFile, 'wx', 0o600);
try { await record.writeFile(JSON.stringify(intent)); await record.sync(); } finally { await record.close(); }
const directory = await open(dirname(intentFile), 'r');
try { await directory.sync(); } finally { await directory.close(); }
async function reached(point, details={}) {
  if (point !== selected) return;
  process.stdout.write(JSON.stringify({point, ...details}) + '\\n');
  process.stdin.resume();
  await new Promise(resolve => process.stdin.once('data', resolve));
}
await reached('intent-durable');
await commitFileEffect(intent, {assertLive(){}, failpoint:reached});
throw new Error('selected failpoint was not paused');
`;

async function killAt(root: string, point: EffectFailpointName): Promise<{ intent: PreparedFileEffect; receipt: Record<string, unknown> }> {
  const intentFile = join(dirname(root), "durable-intent.json");
  const child = spawn("bun", ["--eval", CHILD, root, intentFile, point], { stdio: ["pipe", "pipe", "pipe"] });
  let stderr = "";
  child.stderr.setEncoding("utf8").on("data", (text) => { stderr += text; });
  const closed = once(child, "close");
  // Event-driven boundary receipt, with a single bounded missing-event safety
  // deadline. Always kill/wait the exact owned child, including assertion errors.
  const lines = createInterface({ input: child.stdout });
  let deadline: ReturnType<typeof setTimeout> | undefined;
  try {
    const receipt = await new Promise<Record<string, unknown>>((resolve, reject) => {
      deadline = setTimeout(() => reject(new Error(`child did not reach ${point}: ${stderr}`)), 15_000);
      lines.once("line", (line) => { try { resolve(JSON.parse(line)); } catch (error) { reject(error); } });
      child.once("error", reject);
      child.once("exit", (code, signal) => reject(new Error(`early child exit ${code}/${signal}: ${stderr}`)));
    });
    expect(receipt.point).toBe(point);
    child.kill("SIGKILL");
    const [code, signal] = await closed;
    expect(code).toBeNull();
    expect(signal).toBe("SIGKILL");
    return { intent: JSON.parse(await readFile(intentFile, "utf8")) as PreparedFileEffect, receipt };
  } finally {
    if (deadline) clearTimeout(deadline);
    lines.close();
    child.stdin.destroy();
    if (child.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await closed; }
  }
}

describe("actual process-kill recovery at adapter boundaries (not B5/B6)", () => {
  it.each(["intent-durable", "file-temp-synced", "file-renamed", "destination-committed"] as const)("recovers stored edit bytes after SIGKILL at %s", async (point) => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "old");
    await chmod(join(root, "file.txt"), 0o640);
    const { intent, receipt } = await killAt(root, point);
    const committedBeforeKill = point === "file-renamed" || point === "destination-committed";
    expect(await bytes(root)).toBe(committedBeforeKill ? "stored successor bytes" : "old");
    const killedInode = await inode(root);
    const stagedBefore = (await readdir(root)).filter((name) => name.endsWith(".tmp"));
    if (point === "file-temp-synced") {
      expect(stagedBefore).toHaveLength(1);
      expect(await readFile(receipt.tempPath as string, "utf8")).toBe("stored successor bytes");
    }
    // A regenerated model request is deliberately different and is never used
    // for reconciliation. Only the durably stored edit payload reaches the sink.
    const regenerated = { path: "file.txt", edits: [{ oldText: "old", newText: "WRONG regenerated bytes" }] };
    expect(regenerated.edits[0]!.newText).not.toBe(Buffer.from(intent.desiredBase64, "base64").toString());
    const replay = await reconcileFileEffect(intent, live);
    expect(replay.outcome).toBe("committed");
    expect(replay.result).toEqual(intent.preparedResult);
    expect(await bytes(root)).toBe("stored successor bytes");
    expect((await lstat(join(root, "file.txt"))).mode & 0o7777).toBe(0o640);
    if (committedBeforeKill) expect(await inode(root)).toBe(killedInode);
    if (point === "file-temp-synced") expect((await readdir(root)).filter((name) => name.endsWith(".tmp"))).toEqual(stagedBefore);
    const completedInode = await inode(root);
    expect((await reconcileFileEffect(intent, live)).outcome).toBe("committed");
    expect(await inode(root)).toBe(completedInode);
  }, 20_000);
});

describe("fixed reviewed read/hash subprocess", () => {
  it("executes fixed code under a controlled environment, checks bytes and never changes the file", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "edited bytes\n");
    const profile: CertifiedShellProfile = { kind: "read-sha256-v1", path: "file.txt", sha256: effectBytesHash("edited bytes\n") };
    const args = { command: shellEffectCommand(profile.path, profile.sha256) };
    const prepared = await prepareShellEffect(root, args, profile);
    expect(Object.isFrozen(prepared)).toBe(true);
    const before = await lstat(join(root, "file.txt"), { bigint: true });
    const poisonedEnv = join(dirname(root), "python-startup.py");
    await writeFile(poisonedEnv, "raise RuntimeError('caller startup must not execute')\n");
    const saved = process.env.PYTHONSTARTUP;
    process.env.PYTHONSTARTUP = poisonedEnv;
    try {
      const result = await executeShellEffect(portable(prepared), live);
      expect(result.content).toEqual([{ type: "text", text: `${profile.sha256}  file.txt\n` }]);
      expect(result.details).toEqual({ exitCode: 0, readOnly: true, profile: "read-sha256-v1" });
      expect(await executeShellEffect(prepared, live)).toEqual(result);
    } finally {
      if (saved === undefined) delete process.env.PYTHONSTARTUP;
      else process.env.PYTHONSTARTUP = saved;
    }
    const after = await lstat(join(root, "file.txt"), { bigint: true });
    expect([after.ino, after.mtimeNs, after.ctimeNs, after.size]).toEqual([before.ino, before.mtimeNs, before.ctimeNs, before.size]);
    expect(await readdir(root)).toEqual(["file.txt"]);
  });

  it("refuses arbitrary code/shell, redirects, child jobs, cwd/env/options and changed prepared commands", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "safe");
    const profile: CertifiedShellProfile = { kind: "read-sha256-v1", path: "file.txt", sha256: effectBytesHash("safe") };
    const command = shellEffectCommand(profile.path, profile.sha256);
    for (const unsupported of ["echo bad > file.txt", `${command}; echo bad`, `${command} &`, `${command}\ntrue`, "/usr/bin/python3 -c 'print(1)'", ` ${command}`]) {
      await expect(prepareShellEffect(root, { command: unsupported }, profile)).rejects.toThrow(/exactly/);
    }
    for (const option of [{ cwd: root }, { env: {} }, { timeout: 1 }, { shell: "/bin/sh" }, { code: "bad" }]) {
      await expect(prepareShellEffect(root, { command, ...option }, profile)).rejects.toThrow(/host-controlled/);
    }
    const prepared = await prepareShellEffect(root, { command }, profile);
    await expect(executeShellEffect({ ...portable(prepared), command: "echo bad > file.txt" }, live)).rejects.toThrow(/corrupt/);
    await expect(executeShellEffect({ ...portable(prepared), format: "unknown" } as unknown as typeof prepared, live)).rejects.toThrow(/corrupt/);
    expect(await bytes(root)).toBe("safe");
  });

  it("rejects changed bytes, symlink/hardlink inputs and stale authority before spawning", async () => {
    const root = await workspace();
    await writeFile(join(root, "file.txt"), "safe");
    const profile: CertifiedShellProfile = { kind: "read-sha256-v1", path: "file.txt", sha256: effectBytesHash("safe") };
    const prepared = await prepareShellEffect(root, { command: shellEffectCommand(profile.path, profile.sha256) }, profile);
    await expect(executeShellEffect(prepared, { assertLive() { throw new Error("stale"); } })).rejects.toThrow("stale");
    await writeFile(join(root, "file.txt"), "changed");
    await expect(executeShellEffect(prepared, live)).rejects.toThrow(/changed/);
    await symlink(join(root, "file.txt"), join(root, "alias"));
    const alias = { ...profile, path: "alias", sha256: effectBytesHash("changed") };
    await expect(prepareShellEffect(root, { command: shellEffectCommand(alias.path, alias.sha256) }, alias)).rejects.toThrow();
    await link(join(root, "file.txt"), join(root, "hardlink"));
    const hard = { ...profile, path: "hardlink", sha256: effectBytesHash("changed") };
    await expect(prepareShellEffect(root, { command: shellEffectCommand(hard.path, hard.sha256) }, hard)).rejects.toThrow();
    expect(await bytes(root)).toBe("changed");
  });
});
