import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentCompletionInbox } from "../src/agents/completion-inbox.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const inboxes: AgentCompletionInbox[] = [];
const setup = (notifyOnComplete = true) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-completion-flow-"));
  roots.push(root);
  const handlers = new Map<string, (event: any, context: ExtensionContext) => unknown>();
  const sendMessage = vi.fn();
  const context = { isIdle: () => false, hasPendingMessages: () => false, hasUI: false } as ExtensionContext;
  const pi = {
    on: (name: string, handler: (event: any, context: ExtensionContext) => unknown) => {
      handlers.set(name, handler);
      return () => handlers.delete(name);
    }, sendMessage,
  } as unknown as ExtensionAPI;
  const inbox = new AgentCompletionInbox(pi, context);
  inboxes.push(inbox);
  const completed = vi.fn((result) => inbox.enqueue(result));
  const consumed = vi.fn((id) => inbox.acknowledge(id));
  const settled = vi.fn();
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, notifyOnComplete }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root,
    onBackgroundComplete: completed, onResultConsumed: consumed,
    onLifecycle: (event) => { if (event.event === "run.completed") settled(); },
  });
  managers.push(manager);
  return {
    manager, completed, consumed, settled, sendMessage,
    boundary: () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, context),
  };
};

afterEach(async () => {
  for (const inbox of inboxes.splice(0)) inbox.close();
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("real worker completion flow", () => {
  it("retracts a detached completion when a later program waits for the already-settled result", async () => {
    const h = setup();
    const handle = await h.manager.spawn({ task: "work", transport: "process" });
    h.manager.detachSignal(handle.id);
    await vi.waitFor(() => expect(h.completed).toHaveBeenCalledOnce(), { timeout: 5_000 });
    expect(h.sendMessage).not.toHaveBeenCalled();
    const result = await h.manager.wait(handle.id);
    expect(result.text).toBe("fake worker complete");
    expect(h.consumed).toHaveBeenCalledWith(handle.id);
    h.boundary();
    expect(h.sendMessage).not.toHaveBeenCalled();
  });

  it("notifies exactly once when a worker settles before spawn detaches", async () => {
    const h = setup();
    const handle = await h.manager.spawn({ task: "fast work", transport: "process" });
    await vi.waitFor(() => expect(h.settled).toHaveBeenCalledOnce(), { timeout: 5_000 });
    expect(h.completed).not.toHaveBeenCalled();
    h.manager.detachSignal(handle.id);
    h.manager.detachSignal(handle.id);
    expect(h.completed).toHaveBeenCalledOnce();
    h.boundary();
    expect(h.sendMessage).toHaveBeenCalledOnce();
    expect(h.sendMessage.mock.calls[0]![0].content).toContain("fake worker complete");
  });

  it("keeps blocking run quiet and acknowledges its returned result", async () => {
    const h = setup();
    const result = await h.manager.run({ task: "foreground", transport: "process" });
    expect(result.status).toBe("completed");
    h.boundary();
    expect(h.completed).not.toHaveBeenCalled();
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.consumed).toHaveBeenCalledExactlyOnceWith(result.id);
  });

  it("respects notifyOnComplete=false even for a fast detached worker", async () => {
    const h = setup(false);
    const handle = await h.manager.spawn({ task: "quiet", transport: "process" });
    await vi.waitFor(() => expect(h.manager.status(handle.id).status).toBe("completed"), { timeout: 5_000 });
    h.manager.detachSignal(handle.id);
    h.boundary();
    expect(h.completed).not.toHaveBeenCalled();
    expect(h.sendMessage).not.toHaveBeenCalled();
  });

  it.each(["stop", "cleanup"] as const)("retracts pending completion on explicit %s", async (operation) => {
    const h = setup();
    const handle = await h.manager.spawn({ task: "superseded", transport: "process" });
    h.manager.detachSignal(handle.id);
    await vi.waitFor(() => expect(h.completed).toHaveBeenCalledOnce(), { timeout: 5_000 });
    await h.manager[operation](handle.id);
    h.boundary();
    expect(h.sendMessage).not.toHaveBeenCalled();
  });
});
