import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import type { AgentRunResult } from "../src/agents/types.js";

// Storage-only proof. Actor wake delivery after the empty preflight needs an integration
// proof before shipping; an explicit call on a later turn is not that wake proof.
it("does not claim an empty spool and re-reads a completion on a subsequent call", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-child-idle-preflight-"));
  const sessionFile = path.join(root, "session.jsonl");
  fs.writeFileSync(sessionFile, "");
  const store = new ActorChildCompletionStore(sessionFile);
  const mkdir = vi.spyOn(fs, "mkdirSync");
  try {
    expect(store.pending()).toEqual([]);
    expect(mkdir.mock.calls.some(([file]) => String(file).endsWith(`${path.sep}.claim.lock`))).toBe(false);
    const result = { id: "b".repeat(32), name: "child", status: "completed", text: "saved result",
      startedAt: Date.now(), finishedAt: Date.now() } as AgentRunResult;
    store.enqueue(result, { id: "actor:owner", kind: "actor", runId: "a".repeat(32) });
    mkdir.mockClear();
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(store.pending()).toEqual([expect.objectContaining({ result: expect.objectContaining({ id: result.id }) })]);
    expect(mkdir.mock.calls.some(([file]) => String(file).endsWith(`${path.sep}.claim.lock`))).toBe(true);
  } finally {
    mkdir.mockRestore();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
