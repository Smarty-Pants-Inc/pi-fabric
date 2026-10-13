import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#6337: nextStuckPreparingAt is the exact one-shot deadline for reportStuckPreparing:
// the episode's remaining active time, never a permit wait, never an already-noticed episode.
const MIN = 60_000;
const THRESHOLD = 10 * MIN;
const cleanups: Array<() => Promise<void>> = [];
const waitFor = async (predicate: () => boolean, ms = 5_000) => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for the actor state");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
};
// Only the probes run on a frozen clock: activation persistence needs a moving one.
const at = (ms: number) => { vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(ms); };
const jump = (ms: number) => { vi.useFakeTimers({ toFake: ["Date"], shouldAdvanceTime: true }); vi.setSystemTime(ms); };
type Options = NonNullable<ConstructorParameters<typeof ActorManager>[6]>;
const setup = (options: Options, maxConcurrent: number, agentOptions: NonNullable<ConstructorParameters<typeof AgentManager>[2]> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stuck-deadline-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), ...agentOptions,
  });
  const notices: string[] = [];
  const actors = new ActorManager("stuck-deadline", { id: "session:stuck-deadline", name: "main", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, (delivery) => {
      if (delivery.message.source === "fabric-host") notices.push(delivery.message.text ?? "");
    },
    { actorRoot: path.join(root, "actors"), persistent: true, preparationRetryMs: 60, preparationTimeoutMs: 24 * 60 * MIN, ...options });
  cleanups.push(async () => { vi.useRealTimers(); await actors.close(); await agents.close(); fs.rmSync(root, { recursive: true, force: true }); });
  const stuck = () => notices.filter((text) => text.includes("stuck in preparing"));
  return { actors, agents, stuck };
};
afterEach(async () => { vi.useRealTimers(); for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); vi.restoreAllMocks(); });

describe("ActorManager.nextStuckPreparingAt (smarty-dev#6337)", () => {
  it("is the exact active deadline, due when overdue, and clears once noticed or settled", async () => {
    const gate = deferred<void>();
    let stalled = false;
    const s = setup({ resolvePiModel: (model) => stalled ? gate.promise.then(() => model) : model }, 20);
    cleanups.push(async () => { gate.resolve(); });
    const actor = await s.actors.create({ name: "deadline-prep", model: "provider/test", instructions: "Reply", responseMode: "text", coalesce: false });
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBeUndefined(); // Idle: nothing to arm.
    stalled = true;
    s.actors.tell(actor.id, "work");
    await waitFor(() => s.actors.status(actor.id).preparing?.phase === "binding");
    const since = s.actors.status(actor.id).preparing!.startedAt;
    at(since + MIN);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBe(since + THRESHOLD);
    expect(s.actors.nextStuckPreparingAt(0)).toBeUndefined(); // Disabled threshold, as reportStuckPreparing.
    // The deadline is the boundary: one ms before reports nothing, at it reports once.
    at(since + THRESHOLD - 1);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBe(since + THRESHOLD);
    expect(s.actors.reportStuckPreparing(THRESHOLD)).toBe(0);
    // Overdue but not yet reported: due now (a missed one-shot fires at once).
    at(since + 3 * THRESHOLD);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBe(since + 3 * THRESHOLD);
    at(since + THRESHOLD);
    expect(s.actors.reportStuckPreparing(THRESHOLD)).toBe(1);
    // Already noticed: no deadline, so no hot re-arm on the same episode; cadence is unchanged.
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
    at(since + 5 * THRESHOLD);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
    expect(s.actors.reportStuckPreparing(THRESHOLD)).toBe(0);
    expect(s.stuck()).toHaveLength(1);
    // Exited: settlement emits a change and leaves nothing to arm.
    vi.useRealTimers();
    let changes = 0;
    const unsubscribe = s.actors.subscribe(() => { changes++; });
    stalled = false;
    gate.resolve();
    await waitFor(() => s.actors.inFlightCount() === 0 && s.actors.status(actor.id).queued === 0 && !s.actors.status(actor.id).preparing);
    unsubscribe();
    expect(changes).toBeGreaterThan(0);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
  });

  it("keeps pre-wait active time, excludes the permit wait, and emits a change at each transition", async () => {
    const binding = deferred<void>();
    const launch = deferred<void>();
    let stallBinding = false;
    const s = setup({ resolvePiModel: (model) => stallBinding ? binding.promise.then(() => model) : model }, 1, {
      preparePiModel: async (model) => { if (model === "provider/stalled") await launch.promise; return model; } });
    cleanups.push(async () => { binding.resolve(); launch.resolve(); });
    const blocker = await s.agents.spawn({ task: "HANG", model: "provider/healthy" });
    cleanups.push(async () => { await s.agents.stop(blocker.id).catch(() => undefined); });
    const actor = await s.actors.create({ name: "permit-deadline", model: "provider/stalled", instructions: "Reply", responseMode: "text", coalesce: false });
    // What a host listener sees when it re-arms on each actor event.
    const seen: Array<{ status: string; phase?: string; next?: number }> = [];
    s.actors.subscribe(() => {
      const info = s.actors.status(actor.id);
      seen.push({ status: info.status, ...(info.preparing ? { phase: info.preparing.phase } : {}),
        ...(() => { const next = s.actors.nextStuckPreparingAt(THRESHOLD); return next === undefined ? {} : { next }; })() });
    });
    stallBinding = true;
    s.actors.tell(actor.id, "work");
    await waitFor(() => s.actors.status(actor.id).preparing?.phase === "binding");
    const since = s.actors.status(actor.id).preparing!.startedAt;
    expect(seen.some((entry) => entry.status === "preparing")).toBe(true); // Activation start emitted.
    // Eight minutes in binding, then the permit wait.
    jump(since + 8 * MIN);
    binding.resolve();
    await waitFor(() => s.actors.status(actor.id).status === "waiting");
    const waitingAt = s.actors.status(actor.id).preparing!.startedAt;
    const preWaitActive = waitingAt - since;
    expect(preWaitActive).toBeGreaterThanOrEqual(8 * MIN);
    // The waiting transition emitted, and a permit wait alone arms nothing.
    expect(seen).toContainEqual({ status: "waiting", phase: "waiting" });
    at(waitingAt + 30 * MIN);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
    expect(s.actors.reportStuckPreparing(THRESHOLD)).toBe(0);
    jump(waitingAt + 30 * MIN);
    seen.length = 0;
    await s.agents.stop(blocker.id);
    await waitFor(() => s.actors.status(actor.id).status === "preparing" && s.actors.status(actor.id).preparing?.phase === "launch");
    const admittedAt = s.actors.status(actor.id).preparing!.startedAt;
    const deadline = admittedAt + THRESHOLD - preWaitActive;
    // Admission emitted, and the listener already sees the exact resumed deadline.
    expect(seen).toContainEqual({ status: "preparing", phase: "launch", next: deadline });
    at(admittedAt);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBe(deadline);
    // The public phase start (admittedAt + threshold) would be preWaitActive late.
    expect(admittedAt + THRESHOLD - deadline).toBe(preWaitActive);
    at(deadline - 1);
    expect(s.actors.reportStuckPreparing(THRESHOLD)).toBe(0);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBe(deadline);
    at(deadline);
    expect(s.actors.reportStuckPreparing(THRESHOLD)).toBe(1);
    expect(s.actors.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
    expect(s.stuck()).toEqual([expect.stringContaining("Phase: launch. Not counted: 30 min waiting for an admission permit.")]);
  });
});
