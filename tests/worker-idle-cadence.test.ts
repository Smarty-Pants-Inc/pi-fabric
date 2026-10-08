import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { GROUP_OBSERVE_MAX_MS, GROUP_OBSERVE_MIN_MS, observeGroupAdaptively } from "../src/worker/execution-group.js";

// smarty-dev#4250: an idle process-task worker must stay quiet. A fixed 100 ms
// /proc scan and a fixed 200 ms steer poll kept every idle worker at ~17% of a
// core and ~25 wakeups/s. These pin the replacement cadences.

describe("adaptive execution-group observer", () => {
  afterEach(() => { vi.useRealTimers(); });

  const run = (snapshots: () => string) => {
    vi.useFakeTimers();
    const calls: number[] = [];
    const observer = observeGroupAdaptively(() => { calls.push(Date.now()); return snapshots(); });
    return { observer, calls };
  };

  it("observes at 100 ms first, then doubles to the cap while membership is unchanged", () => {
    const { observer, calls } = run(() => "100:birth");
    const start = Date.now();
    vi.advanceTimersByTime(60_000);
    const gaps = calls.map((at, index) => at - (index ? calls[index - 1]! : start));
    expect(gaps.slice(0, 8)).toEqual([100, 100, 200, 400, 800, 1600, 3200, 5000]);
    expect(Math.max(...gaps)).toBe(GROUP_OBSERVE_MAX_MS);
    // ~0.2 scans/s once idle, not 10.
    expect(calls.length).toBeLessThan(20);
    observer.stop();
  });

  it("returns to 100 ms after a membership change", () => {
    let members = "100:birth";
    const { observer, calls } = run(() => members);
    vi.advanceTimersByTime(20_000);
    expect(observer.delayMs).toBe(GROUP_OBSERVE_MAX_MS);
    members = "100:birth,101:tool";
    const before = calls.length;
    vi.advanceTimersToNextTimer(); // The next scan sees the spawn.
    expect(observer.delayMs).toBe(GROUP_OBSERVE_MIN_MS);
    vi.advanceTimersByTime(GROUP_OBSERVE_MIN_MS);
    expect(calls.length).toBe(before + 2);
    observer.stop();
  });

  it("a poke on child activity restores the 100 ms cadence at once", () => {
    const { observer, calls } = run(() => "100:birth");
    vi.advanceTimersByTime(20_000);
    const before = calls.length;
    observer.poke();
    vi.advanceTimersByTime(GROUP_OBSERVE_MIN_MS);
    expect(calls.length).toBe(before + 1);
    // Repeated pokes while fast do not reschedule or add scans.
    for (let index = 0; index < 50; index++) observer.poke();
    vi.advanceTimersByTime(GROUP_OBSERVE_MIN_MS - 1);
    expect(calls.length).toBe(before + 1);
    observer.stop();
  });

  it("backs off a repeated unconfirmed-group error and stops cleanly", () => {
    const { observer, calls } = run(() => { throw new Error("exit unconfirmed"); });
    vi.advanceTimersByTime(20_000);
    expect(observer.delayMs).toBe(GROUP_OBSERVE_MAX_MS);
    observer.stop();
    const stopped = calls.length;
    observer.poke();
    vi.advanceTimersByTime(60_000);
    expect(calls.length).toBe(stopped);
  });
});

describe.skipIf(process.platform === "win32")("idle worker steer delivery", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  afterEach(async () => {
    await Promise.all(managers.splice(0).map(manager => manager.close()));
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it("delivers a steer to an idle worker from the file watch, not the 2 s safety poll", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-idle-steer-"));
    roots.push(root);
    const inputs = path.join(root, "claude-inputs.jsonl");
    vi.stubEnv("FAKE_CLAUDE_INPUT_LOG", inputs);
    vi.stubEnv("FAKE_CLAUDE_HOLD_UNTIL", "RELEASE");
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000 }, {
      workerPath: path.resolve("src/worker.ts"),
      claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"),
      runRoot: root,
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "hold this turn", runner: "claude", transport: "process", model: "claude/haiku" });
    const lines = () => fs.existsSync(inputs)
      ? fs.readFileSync(inputs, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as { at: number; text: string })
      : [];
    const until = async (predicate: () => boolean, ms: number) => {
      const deadline = Date.now() + ms;
      while (!predicate()) {
        if (Date.now() > deadline) throw new Error("timed out: " + JSON.stringify(lines()));
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    };
    await until(() => lines().some(line => line.text.includes("hold this turn")), 15_000);
    // Let the worker go idle: its steer timer is now on the 2 s safety cadence.
    await new Promise(resolve => setTimeout(resolve, 2_500));
    const steeredAt = Date.now();
    expect(manager.steer(handle.id, "RELEASE now").queued).toBe(true);
    await until(() => lines().some(line => line.text.includes("RELEASE now")), 5_000);
    const latency = lines().find(line => line.text.includes("RELEASE now"))!.at - steeredAt;
    expect(latency).toBeLessThan(1_000);
    const result = await manager.wait(handle.id);
    expect(result.status, result.error).toBe("completed");
  }, 30_000);
});
