import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#6337: ActorDirectory.nextStuckPreparingAt is the earliest deadline across the
// primary (project) and secondary (session) scope managers, as reportStuckPreparing sums both.
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
// Probes run on a frozen clock; activation itself needs a moving one.
const at = (ms: number) => { vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(ms); };
const jump = (ms: number) => { vi.useFakeTimers({ toFake: ["Date"], shouldAdvanceTime: true }); vi.setSystemTime(ms); };

const setup = (resolvePiModel: (model: string) => string | Promise<string>) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-directory-stuck-deadline-"));
  const sessionId = "directory-deadline";
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 20 }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const notices: string[] = [];
  const directory = new ActorDirectory([
    sessionId, { id: `session:${sessionId}`, name: "main", kind: "main", sessionId }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents,
    (delivery) => { if (delivery.message.source === "fabric-host") notices.push(delivery.message.text ?? ""); },
    { persistent: true, preparationRetryMs: 60, preparationTimeoutMs: 24 * 60 * MIN, resolvePiModel },
  ], { project: path.join(root, "mesh", "actors"), session: path.join(root, "mesh", "actors", sessionId) }, "project");
  cleanups.push(async () => { vi.useRealTimers(); await directory.close(); await agents.close(); fs.rmSync(root, { recursive: true, force: true }); });
  return { directory, stuck: () => notices.filter((text) => text.includes("stuck in preparing")) };
};
afterEach(async () => { vi.useRealTimers(); for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); vi.restoreAllMocks(); });

describe("ActorDirectory.nextStuckPreparingAt (smarty-dev#6337)", () => {
  it("reports a secondary-scope deadline while the primary is idle, takes the earlier of both, and clears once noticed", async () => {
    const secondaryGate = deferred<void>(), primaryGate = deferred<void>();
    // Model resolution also runs at create, so the gates arm only once both actors exist.
    let armed = false;
    const s = setup((model) => !armed ? model : model === "provider/secondary" ? secondaryGate.promise.then(() => model)
      : model === "provider/primary" ? primaryGate.promise.then(() => model) : model);
    cleanups.push(async () => { secondaryGate.resolve(); primaryGate.resolve(); });
    const secondary = await s.directory.create({ name: "secondary-prep", model: "provider/secondary", instructions: "Reply", responseMode: "text", coalesce: false, scope: "session" });
    const primary = await s.directory.create({ name: "primary-prep", model: "provider/primary", instructions: "Reply", responseMode: "text", coalesce: false, scope: "project" });
    expect(s.directory.status(secondary.id).scope).toBe("session");
    expect(s.directory.status(primary.id).scope).toBe("project");
    // Both scopes idle: nothing to arm.
    armed = true;
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();

    // Only the secondary-scope actor is preparing; the primary scope stays idle.
    s.directory.tell(secondary.id, "work");
    await waitFor(() => s.directory.status(secondary.id).preparing?.phase === "binding");
    const secondarySince = s.directory.status(secondary.id).preparing!.startedAt;
    expect(s.directory.status(primary.id).preparing).toBeUndefined();
    at(secondarySince + MIN);
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBe(secondarySince + THRESHOLD);
    expect(s.directory.nextStuckPreparingAt(0)).toBeUndefined();
    at(secondarySince + THRESHOLD - 1);
    expect(s.directory.reportStuckPreparing(THRESHOLD)).toBe(0);

    // The primary-scope actor starts later, so the secondary deadline is the earlier one.
    jump(secondarySince + 2 * MIN);
    s.directory.tell(primary.id, "work");
    await waitFor(() => s.directory.status(primary.id).preparing?.phase === "binding");
    const primarySince = s.directory.status(primary.id).preparing!.startedAt;
    expect(primarySince).toBeGreaterThanOrEqual(secondarySince + 2 * MIN);
    at(primarySince + 1);
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBe(secondarySince + THRESHOLD);
    // Passing `now` through to both scopes: overdue secondary is due at once.
    expect(s.directory.nextStuckPreparingAt(THRESHOLD, secondarySince + 3 * THRESHOLD)).toBe(secondarySince + 3 * THRESHOLD);

    // At the secondary boundary exactly one notice; afterwards the primary deadline is next.
    at(secondarySince + THRESHOLD);
    expect(s.directory.reportStuckPreparing(THRESHOLD)).toBe(1);
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBe(primarySince + THRESHOLD);
    at(primarySince + THRESHOLD - 1);
    expect(s.directory.reportStuckPreparing(THRESHOLD)).toBe(0);
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBe(primarySince + THRESHOLD);

    // Primary noticed too: both scopes undefined, so the directory arms nothing and stays quiet.
    at(primarySince + THRESHOLD);
    expect(s.directory.reportStuckPreparing(THRESHOLD)).toBe(1);
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
    at(primarySince + 5 * THRESHOLD);
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
    expect(s.directory.reportStuckPreparing(THRESHOLD)).toBe(0);
    expect(s.stuck()).toHaveLength(2);
  });

  it("keeps a primary-only deadline and its noticed counterexample with the secondary idle", async () => {
    const gate = deferred<void>();
    let armed = false;
    const s = setup((model) => armed && model === "provider/primary" ? gate.promise.then(() => model) : model);
    cleanups.push(async () => { gate.resolve(); });
    const primary = await s.directory.create({ name: "primary-only", model: "provider/primary", instructions: "Reply", responseMode: "text", coalesce: false });
    expect(s.directory.status(primary.id).scope).toBe("project");
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
    armed = true;
    s.directory.tell(primary.id, "work");
    await waitFor(() => s.directory.status(primary.id).preparing?.phase === "binding");
    const since = s.directory.status(primary.id).preparing!.startedAt;
    at(since + MIN);
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBe(since + THRESHOLD);
    at(since + THRESHOLD);
    expect(s.directory.reportStuckPreparing(THRESHOLD)).toBe(1);
    expect(s.directory.nextStuckPreparingAt(THRESHOLD)).toBeUndefined();
    expect(s.stuck()).toHaveLength(1);
  });
});
