import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager, HOST_STOP_REASON } from "../src/agents/manager.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { AgentCompletionInbox } from "../src/agents/completion-inbox.js";
import {
  readStoppedRuns, rememberStoppedAtClose, restoreStoppedRuns, STOPPED_AGENTS_ENTRY, takeReloadStoppedNotice,
} from "../src/agents/stopped-runs.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const managers: AgentManager[] = [];
const roots: string[] = [];
const manager = (extra: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), "stopped-runs-test-"));
  roots.push(runRoot);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  const created = new AgentManager(
    process.cwd(),
    { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, transport: "process", sessionExport: false },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot, ...extra },
  );
  managers.push(created);
  return created;
};
afterEach(async () => {
  await Promise.allSettled(managers.splice(0).map((created) => created.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const entry = (data: unknown) => ({ type: "custom", customType: STOPPED_AGENTS_ENTRY, data });

describe("task agents stopped by a reload (smarty-dev#1602)", () => {
  it("defers consumption of a restored terminal result until delivery", async () => {
    const consumed = vi.fn();
    const next = manager({ onResultConsumed: consumed });
    next.restorePreviousRuns([{
      id: "old-run", name: "worker", task: "", status: "stopped", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, text: "restored result", error: HOST_STOP_REASON,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    } as AgentRunResult]);
    let commit: (() => void) | undefined;
    await expect(next.wait("old-run", { deferConsumption(consume) { commit = consume; } })).resolves.toMatchObject({ text: "restored result" });
    expect(consumed).not.toHaveBeenCalled();
    expect(commit).toBeTypeOf("function");
    commit!();
    expect(consumed).toHaveBeenCalledExactlyOnceWith("old-run");
  });
  it("reports each run the close stopped, with the reason, last error and last event time", async () => {
    const onStoppedAtClose = vi.fn();
    const first = manager({ onStoppedAtClose });
    const handle = await first.spawn({ task: "HANG", runner: "pi", extensions: false });
    await first.close();
    expect(onStoppedAtClose).toHaveBeenCalledTimes(1);
    const [results] = onStoppedAtClose.mock.calls[0] as [AgentRunResult[]];
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({ id: handle.id, status: "stopped" });
    expect(results[0]!.error).toMatch(
      new RegExp(`^${HOST_STOP_REASON}; last error: Agent stopped; last event: (none|\\d{4}-\\d\\d-\\d\\dT)`),
    );
  });

  it("answers wait, status and steer for a previous runtime's run from its record", async () => {
    const stopped = {
      id: "old-run", name: "worker", task: "", status: "stopped", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, text: "",
      error: `${HOST_STOP_REASON}; last error: Agent stopped; last event: 1970-01-01T00:00:00.002Z`,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    } as AgentRunResult;
    const { runs } = readStoppedRuns([entry({ stopped: [stopped] })]);
    const onResultConsumed = vi.fn();
    const next = manager({ onResultConsumed });
    await expect(next.wait("old-run")).rejects.toThrow("Unknown Fabric agent");
    next.restorePreviousRuns(runs);
    await expect(next.wait("old-run")).resolves.toMatchObject({ status: "stopped", error: stopped.error });
    expect(onResultConsumed).toHaveBeenCalledWith("old-run");
    expect(next.status("old-run")).toMatchObject({ status: "stopped", error: stopped.error });
    expect(() => next.steer("old-run", "hi")).toThrow(/is stopped: stopped by host reload\/shutdown/);
    expect(() => next.steer("old-run", "hi")).not.toThrow(/Unknown Fabric agent/);
    await expect(next.stop("old-run")).resolves.toMatchObject({ status: "stopped" });
    await expect(next.wait("never-seen")).rejects.toThrow("Unknown Fabric agent");
  });

  it("reads back stopped runs from the session and skips delivered ones", () => {
    const run = (id: string) => ({ id, status: "stopped" }) as AgentRunResult;
    const read = readStoppedRuns([
      { type: "custom", customType: "other", data: { stopped: [run("x")] } },
      entry({ stopped: [run("a"), run("b")] }),
      entry({ delivered: ["a"] }),
      entry({ stopped: [run("c")] }),
    ]);
    expect(read.runs.map((r) => r.id)).toEqual(["a", "b", "c"]);
    expect(read.undelivered.map((r) => r.id)).toEqual(["b", "c"]);
  });

  describe("replay at the next session start", () => {
    const stoppedRun = {
      id: "old-run", name: "worker", task: "", status: "stopped", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, text: "",
      error: `${HOST_STOP_REASON}; last error: Agent stopped; last event: none`,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    } as AgentRunResult;
    // One runtime start: a real inbox on an idle UI session, and the session entries Pi keeps.
    const start = (entries: unknown[], notifyOnComplete: boolean) => {
      const sendMessage = vi.fn();
      const notify = vi.fn();
      const context = { isIdle: () => true, hasPendingMessages: () => false, hasUI: true, ui: { notify } } as unknown as ExtensionContext;
      const inbox = new AgentCompletionInbox({ on: () => () => {}, sendMessage } as unknown as ExtensionAPI, context);
      const next = manager();
      const markDelivered = restoreStoppedRuns({
        entries, notifyOnComplete,
        restore: (runs) => next.restorePreviousRuns(runs),
        enqueue: (run, delivered) => inbox.enqueue(run, delivered),
        appendEntry: (data) => entries.push(entry(data)),
      });
      return { next, inbox, sendMessage, notify, markDelivered };
    };

    it("with notices off keeps the result queryable, with no notice and no Main turn", async () => {
      const entries: unknown[] = [entry({ stopped: [stoppedRun] })];
      const run = start(entries, false);
      await new Promise((resolve) => setTimeout(resolve, 120));
      expect(run.notify).not.toHaveBeenCalled();
      expect(run.sendMessage).not.toHaveBeenCalled();
      await expect(run.next.wait("old-run")).resolves.toMatchObject({ status: "stopped", error: stoppedRun.error });
      run.inbox.close();
    });

    it("with notices on delivers once, and not again at the start after", async () => {
      const entries: unknown[] = [entry({ stopped: [stoppedRun] })];
      const first = start(entries, true);
      await vi.waitFor(() => expect(first.sendMessage).toHaveBeenCalledTimes(1));
      expect(first.sendMessage.mock.calls[0]![1]).toMatchObject({ triggerTurn: true });
      expect(first.notify).toHaveBeenCalledTimes(1);
      first.inbox.close();
      const second = start(entries, true);
      await new Promise((resolve) => setTimeout(resolve, 120));
      expect(second.sendMessage).not.toHaveBeenCalled();
      await expect(second.next.wait("old-run")).resolves.toMatchObject({ status: "stopped" });
      second.inbox.close();
    });
  });
});

describe("notice after a reload stopped task agents (smarty-dev#1882)", () => {
  const run = (id: string, name: string) => ({ id, name, status: "stopped" }) as AgentRunResult;

  it("a real close records the stopped names; the reloaded session start shows them once", async () => {
    const sessionId = `reload-notice-${Date.now()}`;
    const first = manager({ onStoppedAtClose: (results) => rememberStoppedAtClose(sessionId, results) });
    await first.spawn({ task: "HANG", runner: "pi", extensions: false, name: "reload-victim" });
    await first.close();
    expect(takeReloadStoppedNotice(sessionId, "reload")).toBe(
      'The last /reload stopped 1 task agent: reload-victim; spawn with residency: "durable" to keep agents across reloads.',
    );
    expect(takeReloadStoppedNotice(sessionId, "reload")).toBeUndefined();
  });

  it("counts and names several, and shows nothing for another session or a non-reload start", () => {
    rememberStoppedAtClose("s1", [run("a", "alpha"), run("b", "beta")]);
    expect(takeReloadStoppedNotice("s2", "reload")).toBeUndefined();
    expect(takeReloadStoppedNotice("s1", "reload")).toMatch(/^The last \/reload stopped 2 task agents: alpha, beta; /);
    rememberStoppedAtClose("s1", [run("a", "alpha")]);
    expect(takeReloadStoppedNotice("s1", "new")).toBeUndefined();
    expect(takeReloadStoppedNotice("s1", "reload")).toBeUndefined();
  });
});
