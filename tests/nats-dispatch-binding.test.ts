import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { bindDispatchRunContent, DISPATCH_RUN_BINDING_TTL_MS } from "../src/mesh/nats-auth/dispatch-binding.js";

const bindingTests = describe.skipIf(process.platform === "win32");
let stateHome: string;

beforeEach(() => {
  stateHome = fs.mkdtempSync(path.join(os.tmpdir(), "nats-dispatch-binding-"));
  vi.stubEnv("XDG_STATE_HOME", stateHome);
});

afterEach(() => {
  vi.unstubAllEnvs();
  if (stateHome && fs.existsSync(stateHome)) fs.rmSync(stateHome, { recursive: true, force: true });
  vi.restoreAllMocks();
});

const bindingDirectory = () => path.join(stateHome, "pi-fabric", "nats-dispatch");
const bindingPath = (runId: string) => path.join(bindingDirectory(), `${createHash("sha256").update(runId).digest("hex")}.json`);

bindingTests("NATS dispatch run bindings", () => {
  it("binds task content to the actual sender and state-directory identity", () => {
    const runId = "dispatch-sender-binding";
    bindDispatchRunContent(runId, "private task text", "target:one", "provider:cli:sender-one");
    const recordPath = bindingPath(runId);
    const recordBytes = fs.readFileSync(recordPath, "utf8");
    const record = JSON.parse(recordBytes) as { format: number; createdAt: number; contentSha256: string };
    expect(record.format).toBe(2);
    expect(record.createdAt).toEqual(expect.any(Number));
    expect(record.contentSha256).toBe(createHash("sha256").update(JSON.stringify([
      "private task text", "target:one", "provider:cli:sender-one",
      (() => {
        const stat = fs.statSync(bindingDirectory());
        return { dev: stat.dev, ino: stat.ino, uid: stat.uid };
      })(),
    ])).digest("hex"));
    expect(recordBytes).not.toContain("private task text");
    expect(() => bindDispatchRunContent(runId, "private task text", "target:one", "provider:cli:sender-two"))
      .toThrow("run id reused with different content");
    expect(() => bindDispatchRunContent(runId, "private task text", "target:two", "provider:cli:sender-one"))
      .toThrow("run id reused with different content");
    expect(fs.readFileSync(recordPath, "utf8")).toBe(recordBytes);
  });

  it("rejects a copied binding in a different state-directory inode", () => {
    const runId = "dispatch-directory-binding";
    bindDispatchRunContent(runId, "same task", "target:one", "provider:cli:sender-one");
    const oldDirectory = bindingDirectory();
    const savedDirectory = path.join(stateHome, "saved-dispatch-state");
    const fileName = path.basename(bindingPath(runId));
    fs.renameSync(oldDirectory, savedDirectory);
    fs.mkdirSync(oldDirectory, { mode: 0o700 });
    fs.chmodSync(oldDirectory, 0o700);
    fs.copyFileSync(path.join(savedDirectory, fileName), path.join(oldDirectory, fileName));
    expect(() => bindDispatchRunContent(runId, "same task", "target:one", "provider:cli:sender-one"))
      .toThrow("run id reused with different content");
  });

  it("publishes bindings atomically and leaves no final or temporary file after a failed rename", () => {
    const runId = "dispatch-atomic-binding";
    const rename = vi.spyOn(fs, "renameSync").mockImplementationOnce(() => { throw new Error("injected rename failure"); });
    expect(() => bindDispatchRunContent(runId, "task", "target", "sender"))
      .toThrow("could not safely persist dispatch run binding");
    expect(rename).toHaveBeenCalledTimes(1);
    const [temporary, target] = rename.mock.calls[0]!;
    expect(path.basename(String(target))).toBe(path.basename(bindingPath(runId)));
    expect(path.basename(String(temporary))).toContain(`${path.basename(bindingPath(runId))}.`);
    expect(fs.existsSync(bindingPath(runId))).toBe(false);
    expect(fs.readdirSync(bindingDirectory())).toEqual([]);
    rename.mockRestore();

    bindDispatchRunContent(runId, "task", "target", "sender");
    expect(JSON.parse(fs.readFileSync(bindingPath(runId), "utf8"))).toMatchObject({ format: 2 });
    expect(fs.readdirSync(bindingDirectory())).toEqual([path.basename(bindingPath(runId))]);
  });

  it("expires old run bindings after the bounded retention period", () => {
    expect(DISPATCH_RUN_BINDING_TTL_MS).toBe(7 * 24 * 60 * 60 * 1_000);
    vi.useFakeTimers();
    const now = 1_800_000_000_000;
    vi.setSystemTime(now);
    const expiredRunId = "dispatch-expired-binding";
    bindDispatchRunContent(expiredRunId, "old task", "target", "sender");
    const expiredPath = bindingPath(expiredRunId);
    expect(fs.existsSync(expiredPath)).toBe(true);

    vi.setSystemTime(now + DISPATCH_RUN_BINDING_TTL_MS + 1);
    bindDispatchRunContent("another-run", "other task", "target", "sender");
    expect(fs.existsSync(expiredPath)).toBe(false);
    expect(() => bindDispatchRunContent(expiredRunId, "new task", "target", "sender")).not.toThrow();
    expect(JSON.parse(fs.readFileSync(expiredPath, "utf8")).createdAt).toBe(now + DISPATCH_RUN_BINDING_TTL_MS + 1);
  });
});
