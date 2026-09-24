import { describe, expect, it, vi } from "vitest";
import {
  awaitPeerSettle,
  buildPeerCards,
  peerLabelPrefix,
  type FabricPeerCard,
  type PeerSettleResult,
} from "../src/topology/peer-settle.js";
import type { FabricPeerInfo } from "../src/topology/types.js";

const peer = (
  id: string,
  options: Partial<FabricPeerInfo> = {},
): FabricPeerInfo => ({
  id,
  name: `Peer ${id.slice(0, 8)}`,
  kind: "peer",
  status: "idle",
  runner: "pi",
  transport: "host",
  cwd: "/repo/project",
  sessionId: id,
  startedAt: 1,
  updatedAt: 1,
  pendingMessages: false,
  local: false,
  ...options,
});

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("peerLabelPrefix", () => {
  it("derives initials from the basename words", () => {
    expect(peerLabelPrefix("/repo/pi-queue-steer")).toBe("PQS");
    expect(peerLabelPrefix("/repo/fabric")).toBe("FAB");
    expect(peerLabelPrefix("/repo/my.repo")).toBe("MR");
    expect(peerLabelPrefix(undefined)).toBe("P");
    expect(peerLabelPrefix("/repo/---")).toBe("P");
  });
});

describe("buildPeerCards", () => {
  it("sorts by creation time and falls back to the mesh name", () => {
    const cards = buildPeerCards([
      peer("session:bbb", { startedAt: 2, label: "FAB-2", model: "gpt-5.4" }),
      peer("session:aaa", { startedAt: 1, name: "Peer session" }),
    ]);
    expect(cards.map((card) => card.label)).toEqual(["Peer session", "FAB-2"]);
    expect(cards[0]).toMatchObject({ id: "session:aaa", status: "idle", pendingMessages: false });
    expect(cards[1]).toMatchObject({ label: "FAB-2", model: "gpt-5.4" });
  });

  it("keeps label-provided fields optional", () => {
    const cards: FabricPeerCard[] = buildPeerCards([peer("session:aaa", { label: "F-1" })]);
    expect(cards[0]?.model).toBeUndefined();
    expect(cards[0]?.label).toBe("F-1");
  });
});

describe("awaitPeerSettle", () => {
  it("resolves immediately when nothing matches an empty mesh", async () => {
    await expect(awaitPeerSettle({ poll: () => [], settledForMs: 10, pollMs: 5 })).resolves.toEqual({ ok: true });
  });

  it("rejects an unmatched selector", async () => {
    const result = await awaitPeerSettle({
      poll: () => [peer("session:aaa", { label: "PQS-1" })],
      selector: "PQS-9",
      settledForMs: 10,
      pollMs: 5,
    });
    expect(result).toEqual({ ok: false, error: 'No Fabric peer matches "PQS-9" on this project mesh' });
  });

  it("matches selectors by label case-insensitively and by id", async () => {
    const idle = [peer("session:aaa", { label: "PqS-1" })];
    await expect(
      awaitPeerSettle({ poll: () => idle, selector: "pqs-1", settledForMs: 10, pollMs: 5 }),
    ).resolves.toEqual({ ok: true });
    await expect(
      awaitPeerSettle({ poll: () => idle, selector: "session:aaa", settledForMs: 10, pollMs: 5 }),
    ).resolves.toEqual({ ok: true });
  });

  it("waits for a running peer to settle plus the quiet window", async () => {
    const target = peer("session:aaa", { label: "PQS-1", status: "running" });
    const updates: string[][] = [];
    const started = Date.now();
    const promise = awaitPeerSettle({
      poll: () => [target],
      settledForMs: 40,
      pollMs: 5,
      onUpdate: (progress) => updates.push(progress.waiting.map((peer) => peer.label)),
    });
    await sleep(25);
    target.status = "idle";
    await expect(promise).resolves.toEqual({ ok: true });
    const elapsed = Date.now() - started;
    expect(elapsed).toBeGreaterThanOrEqual(55);
    expect(updates.some((waiting) => waiting.includes("PQS-1"))).toBe(true);
    expect(updates.at(-1)).toEqual([]);
  });

  it("holds an idle-at-arm peer for one quiet window", async () => {
    const started = Date.now();
    await expect(
      awaitPeerSettle({
        poll: () => [peer("session:aaa")],
        settledForMs: 40,
        pollMs: 5,
      }),
    ).resolves.toEqual({ ok: true });
    expect(Date.now() - started).toBeGreaterThanOrEqual(35);
  });

  it("restarts the watch when a quiet peer starts after arming", async () => {
    const target = peer("session:aaa", { label: "PQS-1" });
    const started = Date.now();
    const promise = awaitPeerSettle({
      poll: () => [target],
      settledForMs: 40,
      pollMs: 5,
    });
    // Start before the initial quiet window closes: the settle must not fire early.
    await sleep(20);
    target.status = "running";
    await sleep(30);
    target.status = "idle";
    await expect(promise).resolves.toEqual({ ok: true });
    // 20ms quiet-idle, then ~30ms running, then 40ms settle watch.
    expect(Date.now() - started).toBeGreaterThanOrEqual(80);
  });

  it("treats a vanished peer as settled", async () => {
    const target = peer("session:aaa", { status: "running" });
    let live: FabricPeerInfo[] = [target];
    const started = Date.now();
    const promise = awaitPeerSettle({ poll: () => live, settledForMs: 2_000, pollMs: 5 });
    await sleep(20);
    live = [];
    await expect(promise).resolves.toEqual({ ok: true });
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  it("resolves cancelled on abort and stops polling", async () => {
    const target = peer("session:aaa", { status: "running" });
    const controller = new AbortController();
    let polls = 0;
    const promise = awaitPeerSettle({
      poll: () => {
        polls += 1;
        return [target];
      },
      settledForMs: 5_000,
      pollMs: 5,
      signal: controller.signal,
    });
    await sleep(20);
    controller.abort();
    await expect(promise).resolves.toEqual({ ok: false, error: "cancelled" });
    const settledPolls = polls;
    await sleep(25);
    expect(polls).toBe(settledPolls);
  });

  it("honors an already-aborted signal", async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(
      awaitPeerSettle({
        poll: () => [peer("session:aaa", { status: "running" })],
        settledForMs: 10,
        pollMs: 5,
        signal: controller.signal,
      }),
    ).resolves.toEqual({ ok: false, error: "cancelled" });
  });
});

// smarty-dev#266: during a mesh write stall every peer looks departed ("settled").
describe("awaitPeerSettle during a mesh write stall", () => {
  const stalled = new Error("Fabric mesh is write-stalled: Timed out waiting for the Fabric mesh lock held by pid 7");

  it("refuses to arm while the mesh is stalled", async () => {
    await expect(awaitPeerSettle({ poll: () => [], stalled: () => stalled }))
      .resolves.toEqual({ ok: false, error: stalled.message });
  });

  // Review F1 on #24: a vanished peer is a departure only after a later heartbeat commit.
  it("counts a vanished peer as settled only after a heartbeat commit that follows its disappearance", async () => {
    let live: FabricPeerInfo[] = [peer("session:aaa", { status: "running" })];
    let confirmed = Date.now();
    let result: PeerSettleResult | undefined;
    void awaitPeerSettle({ poll: () => live, confirmedAt: () => confirmed, settledForMs: 60_000, pollMs: 5 })
      .then((settled) => { result = settled; });
    await sleep(20);
    live = [];
    await sleep(60);
    expect(result).toBeUndefined();                            // no commit since it vanished
    live = [peer("session:aaa", { status: "idle" })];          // back: the absence is forgotten
    await sleep(20);
    live = [];
    await sleep(20);
    confirmed = Date.now();
    await sleep(40);
    expect(result).toEqual({ ok: true });
  });

  it("waits for a commit after arming before an empty or partial snapshot counts as settled", async () => {
    let live: FabricPeerInfo[] = [];
    let confirmed = Date.now() - 1_000;
    let result: PeerSettleResult | undefined;
    void awaitPeerSettle({ poll: () => live, confirmedAt: () => confirmed, settledForMs: 30, pollMs: 5 })
      .then((settled) => { result = settled; });
    await sleep(40);
    expect(result).toBeUndefined();                            // empty, but not confirmed
    // The arming snapshot missed it; it shows up in the same poll as the confirming commit.
    live = [peer("session:late", { status: "running" })];
    confirmed = Date.now();
    await sleep(60);
    expect(result).toBeUndefined();                            // now watched, and still running
    live = [peer("session:late", { status: "idle" })];
    await vi.waitFor(() => expect(result).toEqual({ ok: true }), { timeout: 2_000, interval: 10 });
  });

  it("checks a peer that settled before confirmation again on the confirming snapshot", async () => {
    let live: FabricPeerInfo[] = [peer("session:quiet", { status: "idle" })];
    let confirmed = Date.now() - 1_000;
    let result: PeerSettleResult | undefined;
    void awaitPeerSettle({ poll: () => live, confirmedAt: () => confirmed, settledForMs: 30, pollMs: 5 })
      .then((settled) => { result = settled; });
    await sleep(60);                                           // quiet for 30 ms: provisionally settled
    expect(result).toBeUndefined();
    live = [peer("session:quiet", { status: "running" })];     // it resumes before the confirming poll
    confirmed = Date.now();
    await sleep(60);
    expect(result).toBeUndefined();
    live = [peer("session:quiet", { status: "idle" })];
    await vi.waitFor(() => expect(result).toEqual({ ok: true }), { timeout: 2_000, interval: 10 });
  });

  it("reports a stall that starts while waiting instead of settling", async () => {
    let stall: Error | undefined;
    const waiting = awaitPeerSettle({
      poll: () => (stall ? [] : [peer("session:busy", { status: "running" })]),
      stalled: () => stall,
      pollMs: 20,
      settledForMs: 10_000,
    });
    await sleep(50);
    stall = stalled;
    await expect(waiting).resolves.toEqual({ ok: false, error: stalled.message });
  });

  it("finishes with ok:false when a later poll throws, instead of escaping its timer", async () => {
    let calls = 0;
    const waiting = awaitPeerSettle({
      poll: () => {
        calls += 1;
        if (calls > 2) throw new Error("directory read failed");
        return [peer("session:busy", { status: "running" })];
      },
      pollMs: 20,
      settledForMs: 10_000,
    });
    await expect(waiting).resolves.toEqual({ ok: false, error: "directory read failed" });
  });
});
