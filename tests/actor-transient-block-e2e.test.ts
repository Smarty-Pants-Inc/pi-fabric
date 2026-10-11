import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ACTOR_TRANSIENT_BLOCK_BACKOFF_MS, ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#8487 end to end: the real Fabric worker process (src/worker.ts) runs a fake Pi
// RPC child (tests/fixtures/fake-pi-rpc.mjs). Its first activation fails with a real provider
// transport error, the block expires after its backoff, and the next event, routed over the
// mesh topic only once the published actor record is routable again, runs and clears it.
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

const waitFor = async (predicate: () => boolean, timeoutMs = 60_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for actor state");
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
};

describe("transient activation block, real worker end to end (smarty-dev#8487)", () => {
  it("a real provider failure blocks routing, the block expires, and the next routed event runs and clears it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-8487-e2e-"));
    roots.push(root);
    const piBinary = path.resolve("tests/fixtures/fake-pi-rpc.mjs");
    fs.chmodSync(piBinary, 0o755);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"), piBinary, runRoot: path.join(root, "runs"),
    });
    closers.push(() => agents.close());
    const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true,
    });
    closers.push(() => actors.close());
    const actor = await actors.create({ name: "review-route", instructions: "Review each request.", responseMode: "text",
      topics: ["review.requests"], coalesce: false });

    // A fake wall clock over real timers, and the block's expiry wake captured so it can fire at
    // the faked expiry instead of five real minutes later.
    const realNow = Date.now.bind(Date);
    let skew = 0;
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
    const wakes: Array<() => void> = [];
    const realSetTimeout = globalThis.setTimeout;
    vi.spyOn(globalThis, "setTimeout").mockImplementation(((fn: () => void, ms?: number, ...args: unknown[]) => {
      if (ms === ACTOR_TRANSIENT_BLOCK_BACKOFF_MS) wakes.push(fn);
      return realSetTimeout(fn, ms, ...args);
    }) as unknown as typeof setTimeout);

    // 1. The real worker reports the fake provider's transport failure; it is classified transient.
    await expect(actors.ask(actor.id, "FAIL_PROVIDER")).rejects.toThrow(/fetch failed/);
    await waitFor(() => !actors.status(actor.id).inFlightRun && !actors.status(actor.id).preparing);
    const failedRun = actors.status(actor.id).lastRunId;
    expect(failedRun).toBeDefined();
    expect(agents.status(failedRun!)).toMatchObject({ status: "failed" });
    const blocked = actors.status(actor.id).activationBlocked!;
    expect(blocked).toMatchObject({ code: "provider-transient", count: 1, reason: expect.stringContaining("openai-codex/gpt-test: fetch failed · WebSocket error"),
      retryAt: blocked.since + ACTOR_TRANSIENT_BLOCK_BACKOFF_MS });
    expect(blocked.classification).toMatch(/^transient: /);

    // 2. The router's view: the published actor record. A router that skips blocked actors
    // (factory review_routing) does not route to it now.
    const presence = () => mesh.get(`actors/test/${actor.id}`)?.value as Record<string, unknown> | undefined;
    const routable = () => { const value = presence();
      return value !== undefined && value.activationBlocked === undefined && ["idle", "running", "queued"].includes(String(value.status)); };
    await waitFor(() => presence()?.activationBlocked !== undefined);
    expect(routable()).toBe(false);
    expect(wakes).toHaveLength(1);

    // 3. The backoff ends: the one wake republishes the record without the block.
    skew = blocked.retryAt! + 1_000 - realNow();
    wakes[0]!();
    await waitFor(routable);
    expect(presence()).toMatchObject({ activationBlockExpired: { code: "provider-transient", count: 1, classification: blocked.classification } });

    // 4. The router routes the next event over the mesh; the real worker runs it and the block clears.
    const from: MeshIdentity = { id: "session:router", name: "router", kind: "main", sessionId: "router" };
    await mesh.publish({ topic: "review.requests", from, text: "next review request" });
    await waitFor(() => actors.status(actor.id).lastRunId !== failedRun && actors.status(actor.id).status === "idle" &&
      !actors.status(actor.id).inFlightRun && actors.status(actor.id).queued === 0);
    // A completed run's handle is released (only failed runs stay in the agent registry).
    const recovered = actors.status(actor.id);
    expect(() => agents.status(recovered.lastRunId!)).toThrow(/Unknown Fabric agent/);
    expect(recovered).not.toHaveProperty("activationBlocked");
    expect(recovered).not.toHaveProperty("activationBlockExpired");
    const out = actors.messages(actor.id).filter((message) => message.direction === "out");
    expect(out.at(-1)).not.toHaveProperty("error");
    expect(out.at(-1)).toMatchObject({ runId: recovered.lastRunId, text: expect.stringContaining("validated actor response") });
    await waitFor(() => presence() !== undefined && presence()!.activationBlockExpired === undefined);
    expect(presence()).not.toHaveProperty("activationBlocked");
    expect(routable()).toBe(true);
  }, 180_000);
});
