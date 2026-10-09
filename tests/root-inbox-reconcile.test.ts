import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, ROOT_INBOX_ALARM_TOPIC, rootInboxMessage, rootInboxSession } from "../src/topology/root-inbox.js";

const me: MeshIdentity = { id: "session:main", name: "Main", kind: "main" };
const peer: MeshIdentity = { id: "session:peer", name: "Peer", kind: "main" };
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.restoreAllMocks();
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-root-inbox-reconcile-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  cleanups.push(() => mesh.closeState());
  let now = Date.now();
  const box = () => {
    const inbox = new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs: 0 });
    cleanups.push(() => inbox.close()); return inbox;
  };
  const entries: unknown[] = [];
  const session = () => rootInboxSession(entries);
  const work = async (text: string) => {
    const event = await mesh.publish({ from: peer, to: me.id, topic: "fleet.work.test", text });
    now = Math.max(now, event.createdAt + 1); return event;
  };
  const alarms = () => mesh.read({ after: 0, limit: 100, topic: ROOT_INBOX_ALARM_TOPIC });
  return { mesh, box, entries, session, work, alarms, advance: (ms: number) => { now += ms; } };
};

describe("root inbox commit-only and wedge alarm (#4313)", () => {
  it("never admits new work on commit-only reconcile, with or without a held batch", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const first = await h.work("pending");
    const batch = await inbox.next(h.session());
    const second = await h.work("newer");
    expect((await inbox.next(h.session(), { commitOnly: true })).events).toEqual([]);
    expect((h.mesh.get(inbox.key)!.value as any).pending.ids).toEqual([first.id]);
    h.entries.push({ type: "custom_message", ...rootInboxMessage(batch.events) });
    expect(await inbox.next(h.session(), { commitOnly: true })).toEqual({ events: [], through: first.sequence });
    expect((h.mesh.get(inbox.key)!.value as any).pending).toBeUndefined();
    expect((await inbox.next(h.session(), { commitOnly: true })).through).toBe(first.sequence);
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([second.id]);
  });

  it("is silent at 15 minutes and without turn evidence, then alarms once across reloads", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const event = await h.work("stuck"); await inbox.next(h.session());
    h.advance(15 * 60_000);
    await inbox.next(h.session(), { turnEnd: true }); expect(h.alarms()).toEqual([]);
    h.advance(1);
    await inbox.next(h.session());
    await inbox.next(h.session(), { commitOnly: true }); expect(h.alarms()).toEqual([]);
    await inbox.next(h.session(), { turnEnd: true });
    await h.box().next(h.session(), { turnEnd: true });
    expect(h.alarms()).toHaveLength(1);
    expect(h.alarms()[0]?.data).toMatchObject({ rootId: me.id,
      pending: { after: 0, through: event.sequence, ids: [event.id] }, heldMs: 15 * 60_000 + 1 });
    expect((h.mesh.get(inbox.key)!.value as any).pending.alarmed).toBe(true);
  });

  it("alarms an old held batch before committing it, then allows an alarm for a different range", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    await h.work("first");
    h.entries.push({ type: "custom_message", ...rootInboxMessage((await inbox.next(h.session())).events) });
    h.advance(15 * 60_000 + 1);
    await inbox.next(h.session(), { turnEnd: true, commitOnly: true });
    expect((h.mesh.get(inbox.key)!.value as any).pending).toBeUndefined();
    await h.work("second"); await inbox.next(h.session());
    h.advance(15 * 60_000 + 1);
    await inbox.next(h.session(), { turnEnd: true });
    expect(h.alarms()).toHaveLength(2);
    expect(new Set(h.alarms().map(event => event.dedupeKey)).size).toBe(2);
  });

  it("deduplicates publication if a crash/failed cursor save occurs immediately after the alarm", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    await h.work("stuck"); await inbox.next(h.session()); h.advance(15 * 60_000 + 1);
    vi.spyOn(h.mesh, "put").mockRejectedValueOnce(new Error("cursor save failed"));
    await expect(inbox.next(h.session(), { turnEnd: true })).rejects.toThrow("cursor save failed");
    expect(h.alarms()).toHaveLength(1);
    await h.box().next(h.session(), { turnEnd: true });
    expect(h.alarms()).toHaveLength(1);
    expect((h.mesh.get(inbox.key)!.value as any).pending.alarmed).toBe(true);
  });

  it("still commits confirmed work if observational alarm publication fails", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    await h.work("held");
    h.entries.push({ type: "custom_message", ...rootInboxMessage((await inbox.next(h.session())).events) });
    h.advance(15 * 60_000 + 1);
    vi.spyOn(h.mesh, "publish").mockRejectedValueOnce(new Error("alarm publication failed"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await inbox.next(h.session(), { turnEnd: true, commitOnly: true });
    expect((h.mesh.get(inbox.key)!.value as any).pending).toBeUndefined();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("alarm publication failed"));
  });

  it("uses the saved state's timestamp for a pre-upgrade pending batch", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const event = await h.work("legacy pending");
    await h.mesh.put({ key: inbox.key, identity: me, value: { after: 0, pending: { through: event.sequence, ids: [event.id] } } });
    h.advance(15 * 60_000 + 10);
    await h.box().next(h.session(), { turnEnd: true, commitOnly: true });
    expect(h.alarms()).toHaveLength(1);
    expect((h.mesh.get(inbox.key)!.value as any).pending.since).toBeTypeOf("number");
  });
});
