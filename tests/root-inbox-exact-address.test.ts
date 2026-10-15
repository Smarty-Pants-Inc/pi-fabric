import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, rootInboxSession, type RootInboxSession } from "../src/topology/root-inbox.js";

// smarty-dev#4313 idle-wake priority, contract level (NOT E2E, Decision C). Boundary cases of
// RootInbox.wake against a real MeshStore in a temp dir; the real second-process publication,
// pinned Main and <5 s wake are covered by tests/root-inbox-idle-wake-cli.test.ts only.
// Policy: an event with to === identity.id bypasses the 60 s steer grace and the wake cooldown;
// name/alias shadows keep both; unaddressed/wrong-recipient events never wake nor block.
const roots: string[] = [];
const me: MeshIdentity = { id: "session:me", name: "main", kind: "main", sessionId: "me" };
const peer: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };
const ALIAS = "fabric-v2";
const held: RootInboxSession = { holdsBatch: () => true, holdsSteer: () => false };
const notHeld: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
const idle = () => true;
const steered = (key: string, topic = "fleet.work.pi-fabric.1"): RootInboxSession => ({
  ...rootInboxSession([{
    type: "custom_message", timestamp: new Date().toISOString(), customType: "pi-fabric-agent-message",
    details: { from: { id: peer.id }, data: { topic, key } },
  }]),
  holdsBatch: () => true,
});

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-root-inbox-exact-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  let offset = 0;
  const clock = { now: () => Date.now() + offset, advance: (ms: number) => { offset += ms; } };
  // Default steer grace (60 s) and default-sized cooldown: no steerGraceMs override.
  const inbox = () => new RootInbox(mesh, me, () => [me.id, ALIAS], { now: clock.now, wakeCooldownMs: 5 * 60_000 });
  const send = (text: string, to: string | undefined, kind = "ask", topic = "fleet.work.pi-fabric.1") =>
    mesh.publish({ topic, ...(to === undefined ? {} : { to }), kind, from: peer, text, data: { ref: "x#1", key: text } });
  const texts = (events: MeshEvent[] | undefined) => (events ?? []).map((event) => event.text);
  const wakeReasons = () => mesh.read({ after: 0, limit: 500 })
    .filter((event) => event.topic === "fabric.inbox.wake").map((event) => (event.data as { reason: string }).reason);
  return { mesh, clock, inbox, send, texts, wakeReasons };
};

afterEach(() => {
  // PI_TEST_RETAIN_SCRATCH=1 keeps temp roots (local no-delete runs); CI cleans up.
  if (process.env.PI_TEST_RETAIN_SCRATCH === "1") { roots.splice(0); return; }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("RootInbox.wake exact-address priority (#4313 contract)", () => {
  it("delivers an exact-session ask now behind young broadcast and wrong-recipient events under the default grace", async () => {
    const { inbox, send, texts } = setup();
    const box = inbox();
    await box.next(held);
    await send("broadcast", undefined, "progress");
    await send("wrong recipient", "session:other");
    await send("exact ask", me.id);
    const batch = await box.wake(held, idle);
    expect(texts(batch?.events)).toEqual(["exact ask"]);
    expect(box.wakeDueAt).toBeUndefined();
  });

  it("does not wake for young unaddressed, wrong-recipient or alias events alone", async () => {
    const { inbox, send } = setup();
    const box = inbox();
    await box.next(held);
    await send("broadcast", undefined);
    await send("wrong recipient", "session:other");
    expect(await box.wake(held, idle)).toBeUndefined();
    expect(box.wakeDueAt).toBeUndefined();
    const alias = await send("alias", ALIAS);
    expect(await box.wake(held, idle)).toBeUndefined();
    expect(box.wakeDueAt).toBe(alias.createdAt + 60_000);
  });

  it("wakes again for consecutive exact-session work inside the cooldown", async () => {
    const { inbox, send, texts, wakeReasons } = setup();
    const box = inbox();
    await box.next(held);
    await send("first exact", me.id);
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["first exact"]);
    await send("second exact", me.id);
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["second exact"]);
    await box.close();
    expect(wakeReasons()).toEqual(["addressed", "addressed"]);
  });

  it("keeps a later alias shadow behind grace and cooldown, then delivers it at a turn boundary", async () => {
    const { clock, inbox, send, texts } = setup();
    const box = inbox();
    await box.next(held);
    await send("exact", me.id);
    const alias = await send("alias after", ALIAS);
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["exact"]);
    expect(box.wakeDueAt).toBe(alias.createdAt + 60_000);
    clock.advance(61_000);
    // Past the grace but inside the cooldown: an alias shadow does not buy another idle turn.
    expect(await box.wake(held, idle)).toBeUndefined();
    expect(box.wakeDueAt).toBeGreaterThan(clock.now());
    // A turn-boundary reconcile has no cooldown: the shadow arrives in order.
    expect(texts((await box.next(held)).events)).toEqual(["alias after"]);
  });

  it("drops an alias shadow its native steer already delivered, while the exact event still wakes", async () => {
    const { clock, inbox, send, texts } = setup();
    const box = inbox();
    await box.next(held);
    await send("exact", me.id);
    await send("alias-steered", ALIAS);
    const session = steered("alias-steered");
    expect(texts((await box.wake(session, idle))?.events)).toEqual(["exact"]);
    clock.advance(6 * 60_000);
    expect(await box.wake(session, idle)).toBeUndefined();
    expect((await box.next(session)).events).toEqual([]);
  });

  it("admits the ordered prefix: young alias shadows before the exact event come with it, in order", async () => {
    const { inbox, send, texts } = setup();
    const box = inbox();
    await box.next(held);
    await send("alias one", ALIAS);
    await send("broadcast", undefined);
    await send("alias two", ALIAS);
    await send("exact", me.id);
    expect(texts((await box.wake(held, idle))?.events)).toEqual(["alias one", "alias two", "exact"]);
  });

  it("admits exact work at a new-run/settle boundary, but not on a cancelled commit-only boundary", async () => {
    const { inbox, send, texts } = setup();
    const box = inbox();
    box.start();
    await send("held during cancel", me.id);
    expect((await box.next(notHeld, { priority: true, commitOnly: true })).events).toEqual([]);
    expect((await box.next(notHeld, { turnEnd: true })).events).toEqual([]);
    expect(texts((await box.next(notHeld, { priority: true })).events)).toEqual(["held during cancel"]);
    await box.close();
  });

  it("never re-injects an exact event after confirmed consumption, also across a reload", async () => {
    const { inbox, send, texts } = setup();
    const box = inbox();
    await box.next(held);
    await send("exact", me.id);
    const batch = await box.wake(notHeld, idle);
    expect(texts(batch?.events)).toEqual(["exact"]);
    await box.consume(batch!.events.map((event) => event.id));
    expect(await box.wake(notHeld, idle)).toBeUndefined();
    expect((await box.next(notHeld)).events).toEqual([]);
    await send("next exact", me.id);
    expect(texts((await box.wake(notHeld, idle))?.events)).toEqual(["next exact"]);
    await box.close();
    const reloaded = inbox();
    // The reload holds only the unconsumed pending batch, never the consumed one.
    expect(texts((await reloaded.next(notHeld)).events)).toEqual(["next exact"]);
    await reloaded.close();
  });
});
