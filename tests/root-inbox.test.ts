import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, rootInboxMessage } from "../src/topology/root-inbox.js";

// smarty-dev#754 §3.2 step 3: a Main reconciles the work events addressed to it that no steer
// delivered, from a processing cursor that moves only past what it has judged.
const roots: string[] = [];
const me: MeshIdentity = { id: "session:me", name: "main", kind: "main", sessionId: "me" };
const peer: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-root-inbox-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  let offset = 0;
  const clock = { now: () => Date.now() + offset, advance: (ms: number) => { offset += ms; } };
  const inbox = (steerGraceMs = 60_000) =>
    new RootInbox(mesh, me, () => [me.id, "fabric-v2"], { now: clock.now, steerGraceMs });
  const work = (text: string, to = me.id, topic = "fleet.work.pi-fabric.1") =>
    mesh.publish({ topic, to, kind: "ack", from: peer, text, data: { ref: "Smarty-Pants-Inc/pi-fabric#1", key: text } });
  return { mesh, clock, inbox, work };
};

afterEach(() => {
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("RootInbox", () => {
  it("starts at the present, then brings work events addressed to this root by id or name", async () => {
    const { clock, inbox, work } = setup();
    await work("before the inbox existed");
    const box = inbox(0);
    expect(box.unseen().events).toEqual([]);
    await box.advance(box.unseen());
    await work("by id");
    await work("by name", "fabric-v2");
    await work("to someone else", "session:other");
    await work("not a work topic", me.id, "ops.owner");
    clock.advance(1);
    const batch = box.unseen();
    expect(batch.events.map((event) => event.text)).toEqual(["by id", "by name"]);
    await box.advance(batch);
    expect(box.unseen().events).toEqual([]);
  });

  it("leaves a young event for a later reconcile, and never moves past it", async () => {
    const { clock, inbox, work } = setup();
    const box = inbox(60_000);
    await box.advance(box.unseen());
    await work("young");
    let batch = box.unseen();
    expect(batch.events).toEqual([]);
    await box.advance(batch);
    clock.advance(61_000);
    batch = box.unseen();
    expect(batch.events.map((event) => event.text)).toEqual(["young"]);
  });

  it("skips a work event whose steer already reached this root", async () => {
    const { clock, inbox, work } = setup();
    const box = inbox(0);
    await box.advance(box.unseen());
    await work("steered too");
    await work("only published");
    box.noteDelivered(peer.id, "  steered too ");
    clock.advance(1);
    expect(box.unseen().events.map((event) => event.text)).toEqual(["only published"]);
  });

  it("keeps its cursor across a restart, so a delivered event does not come again", async () => {
    const { clock, inbox, work } = setup();
    const first = inbox(0);
    await first.advance(first.unseen());
    await work("once");
    clock.advance(1);
    const batch = first.unseen();
    expect(batch.events).toHaveLength(1);
    await first.advance(batch);
    await work("after the restart");
    clock.advance(1);
    const second = inbox(0);
    expect(second.unseen().events.map((event) => event.text)).toEqual(["after the restart"]);
  });

  it("formats one message with each event's sender, ref, key and text", async () => {
    const { clock, inbox, work } = setup();
    const box = inbox(0);
    await box.advance(box.unseen());
    await work("ETA ~09:30Z <PR>");
    clock.advance(1);
    const message = rootInboxMessage(box.unseen().events);
    expect(message.customType).toBe("pi-fabric-inbox");
    expect(message.content).toContain('ref="Smarty-Pants-Inc/pi-fabric#1"');
    expect(message.content).toContain("ETA ~09:30Z &lt;PR&gt;");
  });
});

describe("MainAgentController delivery observer", () => {
  it("reports each agent message it delivers, for the inbox to skip its shadow record", () => {
    const pi = { sendMessage: vi.fn(), getThinkingLevel: vi.fn() } as unknown as ExtensionAPI;
    const main = new MainAgentController(pi, "session:me", true, process.cwd(), "me");
    const seen: Array<[string, string]> = [];
    main.deliveryObserver = (fromId, text) => seen.push([fromId, text]);
    main.deliverAgent({ from: { id: "session:peer", name: "main", kind: "main" }, message: "hello", delivery: "followUp" } as never);
    expect(seen).toEqual([["session:peer", "hello"]]);
  });
});
