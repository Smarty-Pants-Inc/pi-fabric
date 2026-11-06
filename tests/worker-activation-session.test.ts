import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SessionManager, createBashToolDefinition, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AssistantMessage } from "@earendil-works/pi-ai";
import { ActivationSession } from "../src/worker/activation-session.js";

const roots: string[] = [];
const root = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-activation-retain-"));
  roots.push(dir);
  return dir;
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const dir of roots.splice(0)) fs.rmSync(dir, {recursive: true, force: true});
});
const archivedBytes = (dir: string): Buffer[] => {
  const archive = path.join(dir, "session-archive");
  if (!fs.existsSync(archive)) return [];
  return fs.readdirSync(archive).filter(name => name !== "MANIFEST.jsonl")
    .map(name => fs.readFileSync(path.join(archive, name)));
};
const user = (content: string) => ({role: "user" as const, content, timestamp: 1});
const assistant = (text: string): AssistantMessage => ({
  role: "assistant", content: [{type: "text", text}], api: "openai-completions", provider: "test", model: "test",
  stopReason: "stop", timestamp: 2,
  usage: {input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0}},
});

const compactedActivation = (journal: string, dir: string) => {
  const activation = new ActivationSession(journal, path.join(dir, "run"), dir);
  const session = SessionManager.open(activation.file);
  const first = session.appendMessage(user("CURRENT_ACTIVATION_DIRECTIONS"));
  session.appendMessage(assistant("current decision"));
  const target = session.appendMessage({role: "toolResult", toolCallId: "read", toolName: "read",
    content: [{type: "text", text: "FULL_ORIGINAL_RESULT " + "x".repeat(20_000)}], isError: false, timestamp: 3});
  session.appendCompaction("ACTIVATION_ONLY_SUMMARY", first, 9000, {compactor: "fabric-activation-tools"}, true);
  session.appendContextEdit(target, {content: [{type: "text", text: "LOCAL_EXCERPT"}]});
  const original = session.getBranch();
  const activationId = session.getSessionId();
  expect(JSON.stringify(session.buildSessionContext().messages)).toContain("LOCAL_EXCERPT");
  expect(JSON.stringify(session.buildSessionContext().messages)).not.toContain("FULL_ORIGINAL_RESULT");
  return {activation, original, activationId};
};

describe("activation journal retention", () => {
  it("binds the native session and Bash PI_SESSION_ID to the registered header across compaction and rotation (#4313)", async () => {
    const dir = root();
    const journal = path.join(dir, "actor.jsonl");
    const prior = SessionManager.open(journal);
    prior.appendMessage(user("OLD_PRIVATE_HISTORY"));
    prior.appendMessage(assistant("old reply"));
    const probe = async (run: string) => {
      const activation = new ActivationSession(journal, path.join(dir, run), dir);
      const session = SessionManager.open(activation.file);
      const header = JSON.parse(fs.readFileSync(journal, "utf8").split("\n", 1)[0]!);
      const observed: string[] = [];
      const bash = createBashToolDefinition(dir, { operations: { exec: async (_command, _cwd, options) => {
        observed.push(options.env!.PI_SESSION_ID!);
        return { exitCode: 0 };
      } } });
      const check = async () => {
        await bash.execute("probe", { command: "true" }, undefined, undefined,
          { sessionManager: session } as never);
        expect(session.getSessionId()).toBe(header.id);
        expect(observed.at(-1)).toBe(header.id);
      };
      expect(session.getBranch()).toEqual([]); // identity does not leak history
      await check();
      const kept = session.appendMessage(user("CURRENT_ACTIVATION"));
      session.appendMessage(assistant("current reply"));
      session.appendCompaction("LOCAL_SUMMARY", kept, 1000);
      await check();
      activation.retain();
      expect(SessionManager.open(journal).getSessionId()).toBe(header.id);
      return header.id;
    };
    const original = await probe("first");
    expect(await probe("restarted-worker")).toBe(original);
    fs.renameSync(journal, journal + ".bak");
    const rotated = SessionManager.open(journal);
    rotated.appendMessage(user("NEW_ROTATED_HISTORY"));
    rotated.appendMessage(assistant("new reply"));
    expect(await probe("after-rotation")).not.toBe(original);
  });
  it.each([false, true])("retains every local inference record as non-message audit data (existing journal: %s)", existing => {
    const dir = root();
    const journal = path.join(dir, "actor.jsonl");
    if (existing) {
      const prior = SessionManager.open(journal);
      prior.appendMessage(user("EARLIER_ACTIVATION_SENTINEL"));
      prior.appendMessage(assistant("earlier decision"));
    }
    const before = fs.existsSync(journal) ? fs.readFileSync(journal) : Buffer.alloc(0);
    const priorLeaf = existing ? SessionManager.open(journal).getLeafId() : null;
    const {activation, original, activationId} = compactedActivation(journal, dir);
    const raw = fs.readFileSync(activation.file);
    activation.retain();
    expect(archivedBytes(dir)).toContainEqual(raw);
    const after = fs.readFileSync(journal);
    expect(after.subarray(0, before.length)).toEqual(before);
    const restored = SessionManager.open(journal);
    const retained = restored.getBranch().slice(existing ? 2 : 0);
    expect(retained).toHaveLength(original.length);
    for (const [index, entry] of original.entries()) {
      const linked = {...entry, parentId: entry.parentId === null ? priorLeaf : entry.parentId};
      if (entry.type === "compaction" || entry.type === "context_edit") {
        expect(retained[index]).toEqual({type: "custom", customType: "fabric-activation-context",
          id: linked.id, parentId: linked.parentId, timestamp: linked.timestamp,
          data: {scope: "activation", activationId, entry}});
      } else {
        expect(retained[index]).toEqual(linked);
      }
    }
    const context = JSON.stringify(restored.buildSessionContext().messages);
    if (existing) expect(context).toContain("EARLIER_ACTIVATION_SENTINEL");
    expect(context).toContain("CURRENT_ACTIVATION_DIRECTIONS");
    expect(context).toContain("FULL_ORIGINAL_RESULT");
    expect(context).not.toContain("LOCAL_EXCERPT");
    expect(context).not.toContain("ACTIVATION_ONLY_SUMMARY");
    expect(fs.existsSync(activation.file)).toBe(false);
  });

  it("honors genuine full-history compactions both before and after activation retention", () => {
    const dir = root();
    const journal = path.join(dir, "actor.jsonl");
    const prior = SessionManager.open(journal);
    prior.appendMessage(user("INTENTIONALLY_DISCARDED_BY_GLOBAL_COMPACTION"));
    prior.appendMessage(assistant("discarded decision"));
    const kept = prior.appendMessage(user("GLOBAL_RETAINED_SENTINEL"));
    prior.appendMessage(assistant("retained decision"));
    prior.appendCompaction("GENUINE_GLOBAL_SUMMARY", kept, 1000);
    const before = fs.readFileSync(journal);
    const {activation, original} = compactedActivation(journal, dir);
    activation.retain();
    expect(fs.readFileSync(journal).subarray(0, before.length)).toEqual(before);
    const restored = SessionManager.open(journal);
    let context = JSON.stringify(restored.buildSessionContext().messages);
    expect(context).toContain("GENUINE_GLOBAL_SUMMARY");
    expect(context).toContain("GLOBAL_RETAINED_SENTINEL");
    expect(context).toContain("CURRENT_ACTIVATION_DIRECTIONS");
    expect(context).not.toContain("INTENTIONALLY_DISCARDED_BY_GLOBAL_COMPACTION");
    expect(context).not.toContain("ACTIVATION_ONLY_SUMMARY");
    const beforeNext = fs.readFileSync(journal);
    restored.appendCompaction("GENUINE_LATER_GLOBAL_SUMMARY", original[0]!.id, 2000);
    expect(fs.readFileSync(journal).subarray(0, beforeNext.length)).toEqual(beforeNext);
    context = JSON.stringify(SessionManager.open(journal).buildSessionContext().messages);
    expect(context).toContain("GENUINE_LATER_GLOBAL_SUMMARY");
    expect(context).toContain("CURRENT_ACTIVATION_DIRECTIONS");
    expect(context).not.toContain("GLOBAL_RETAINED_SENTINEL");
    expect(context).not.toContain("GENUINE_GLOBAL_SUMMARY");
  });

  it("preserves raw whitespace, CRLF and a missing final newline rather than reserializing evidence", () => {
    const dir = root();
    const journal = path.join(dir, "actor.jsonl");
    const activation = new ActivationSession(journal, path.join(dir, "raw"), dir);
    const session = SessionManager.open(activation.file);
    session.appendMessage(user("BYTE_EXACT_λ"));
    session.appendMessage(assistant("raw reply"));
    const raw = Buffer.from(" \t" + fs.readFileSync(activation.file, "utf8").trimEnd().replaceAll("\n", " \r\n"));
    fs.writeFileSync(activation.file, raw);
    activation.retain();
    expect(archivedBytes(dir)).toContainEqual(raw);
    expect(JSON.stringify(SessionManager.open(journal).buildSessionContext().messages)).toContain("BYTE_EXACT_λ");
    expect(fs.existsSync(activation.file)).toBe(false);
  });

  it("archives a header-only child byte-for-byte before unlinking it", () => {
    const dir = root();
    const journal = path.join(dir, "actor.jsonl");
    const activation = new ActivationSession(journal, path.join(dir, "empty"), dir);
    const raw = fs.readFileSync(activation.file);
    const unlink = fs.unlinkSync.bind(fs);
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
      if (file === activation.file) expect(archivedBytes(dir)).toContainEqual(raw);
      return unlink(file);
    });
    activation.retain();
    expect(archivedBytes(dir)).toContainEqual(raw);
    expect(fs.existsSync(activation.file)).toBe(false);
    expect(fs.existsSync(journal)).toBe(false);
  });

  it.each(["", "{broken header\n", '{"type":"not-session","id":"bad"}\n',
    '{"type":"session","id":"bad","timestamp":"invalid","cwd":"x"}\n'])
    ("archives invalid raw evidence before refusing its semantic merge: %s", raw => {
      const dir = root();
      const journal = path.join(dir, "actor.jsonl");
      const activation = new ActivationSession(journal, path.join(dir, "invalid"), dir);
      fs.writeFileSync(activation.file, raw);
      expect(() => activation.retain()).toThrow();
      expect(fs.readFileSync(activation.file, "utf8")).toBe(raw);
      expect(archivedBytes(dir)).toContainEqual(Buffer.from(raw));
      expect(fs.existsSync(journal)).toBe(false);
    });

  it.each(["writeSync", "readSync", "fsyncSync"] as const)("keeps original and journal unchanged when archive %s fails", operation => {
    const dir = root();
    const journal = path.join(dir, "actor.jsonl");
    const prior = SessionManager.open(journal);
    prior.appendMessage(user("EARLIER_ACTIVATION"));
    prior.appendMessage(assistant("earlier reply"));
    const {activation} = compactedActivation(journal, dir);
    const raw = fs.readFileSync(activation.file);
    const before = fs.readFileSync(journal);
    vi.spyOn(fs, operation).mockImplementation(() => { throw new Error("archive I/O failed"); });
    expect(() => activation.retain()).toThrow(/archive I\/O failed/);
    vi.restoreAllMocks();
    expect(fs.readFileSync(activation.file)).toEqual(raw);
    expect(fs.readFileSync(journal)).toEqual(before);
  });

  it.each([false, true])("keeps the original if archive readback fails (empty child: %s)", empty => {
    const dir = root();
    const journal = path.join(dir, "actor.jsonl");
    const activation = empty ? new ActivationSession(journal, path.join(dir, "empty-failure"), dir)
      : compactedActivation(journal, dir).activation;
    const raw = fs.readFileSync(activation.file);
    const open = fs.openSync.bind(fs), read = fs.readSync.bind(fs);
    const readers = new Set<number>();
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, flags: number | string, mode?: number) => {
      const fd = open(target, flags, mode);
      if (typeof target === "string" && target.startsWith(path.join(dir, "session-archive") + path.sep) &&
          typeof flags === "number" && !(flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR))) readers.add(fd);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "readSync").mockImplementation(((fd: number, ...args: unknown[]) => {
      if (readers.has(fd)) throw new Error("archive readback failed");
      return Reflect.apply(read, fs, [fd, ...args]);
    }) as typeof fs.readSync);
    expect(() => activation.retain()).toThrow(/archive readback failed/);
    vi.restoreAllMocks();
    expect(fs.readFileSync(activation.file)).toEqual(raw);
    expect(fs.existsSync(journal)).toBe(false);
  });

  it("refuses concurrent durable journal changes and leaves isolated evidence recoverable", () => {
    const dir = root();
    const journal = path.join(dir, "actor.jsonl");
    const {activation} = compactedActivation(journal, dir);
    const concurrent = SessionManager.open(journal);
    concurrent.appendMessage(user("CONCURRENT_WRITER"));
    concurrent.appendMessage(assistant("concurrent reply"));
    const before = fs.readFileSync(journal);
    expect(() => activation.retain()).toThrow(/journal changed/);
    expect(fs.readFileSync(journal)).toEqual(before);
    expect(fs.existsSync(activation.file)).toBe(true);
  });
});
