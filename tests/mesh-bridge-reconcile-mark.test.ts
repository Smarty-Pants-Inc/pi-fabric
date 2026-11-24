import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshBridge, type BridgePresence, type BridgeSide } from "../src/mesh/bridge.js";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanups.splice(0).reverse()) await close(); });

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-reconcile-mark-"));
  const identity = (name: string): MeshIdentity => ({ id: `session:${name}-00000000`, name: "main", kind: "main" });
  const hub = identity("hub"), far = identity("far");
  const presence = (who: MeshIdentity): BridgePresence => {
    const now = Date.now();
    return { hosts: [{ record: { format: 1, id: who.id, rootId: who.id, identity: who,
      startedAt: now, updatedAt: now, expiresAt: now + 120_000 }, observedAt: now, expiresAt: now + 120_000 }],
      participants: [], reserved: [who.id] };
  };
  const side = (who: MeshIdentity) => {
    const events: MeshEvent[] = [], scans: number[] = [];
    const append = (id: string, topic = "fleet.work.test"): MeshEvent => {
      const event: MeshEvent = { id, sequence: events.length + 1, topic, kind: "event", from: who,
        to: who === far ? hub.id : far.id, createdAt: Date.now(), verification: "mesh", data: {} };
      events.push(event);
      return event;
    };
    const port: BridgeSide = {
      latestSequence: async () => events.length,
      read: async after => ({ events: events.filter(event => event.sequence > after &&
        event.verification !== "bridge" && event.topic === "fleet.work.test"), through: events.length }),
      presence: async () => presence(who), mirror: async () => {},
      publish: async event => {
        const copy: MeshEvent = { ...event, id: `copy-${event.data.bridge.id}`, sequence: events.length + 1,
          createdAt: Date.now(), verification: "bridge" };
        events.push(copy);
        return { sequence: copy.sequence };
      },
      bridgedIds: async after => {
        scans.push(after);
        return { ids: events.filter(event => event.sequence > after && event.verification === "bridge")
          .map(event => (event.data as { bridge: { id: string } }).bridge.id), through: events.length, done: true };
      },
    };
    return { port, events, scans, append };
  };
  const local = side(hub), remote = side(far);
  local.port.owned = async () => presence(far);
  local.port.holds = () => true;
  const cursorPath = path.join(root, "cursor.json");
  const create = () => {
    const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: local.port, remote: remote.port, cursorPath });
    cleanups.push(() => bridge.stop());
    return bridge;
  };
  cleanups.push(async () => { fs.rmSync(root, { recursive: true, force: true }); });
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8"));
  return { create, local, remote, cursor };
};

describe("bridge restart reconcile frontier", () => {
  // Keep reason: quiet directions otherwise reread their entire destination history on every restart.
  it("restarts quiet directions scanning from their drained destination marks, not the old marks", async () => {
    const f = fixture(), first = f.create();
    await first.start();
    f.local.append("hub-telemetry", "fleet.telemetry");
    f.remote.append("far-telemetry", "fleet.telemetry");
    expect(await first.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
    await first.stop();
    f.local.scans.length = f.remote.scans.length = 0;
    // Main skips head-only recovery. New unrelated appends require a scan from the saved floors.
    f.local.append("hub-after-stop", "fleet.telemetry");
    f.remote.append("far-after-stop", "fleet.telemetry");
    await f.create().start();
    expect(f.local.scans).toEqual([1]);
    expect(f.remote.scans).toEqual([1]);
    expect(f.cursor()).toMatchObject({ toRemote: { after: 1, mark: 1 }, toLocal: { after: 1, mark: 1 } });
  });

  // Keep reason: a destination append with an in-flight/lost receipt must stay in restart recovery.
  it("recovers a lost receipt after mark advancement and an event published during restart exactly once", async () => {
    const f = fixture(), first = f.create();
    await first.start();
    f.remote.append("delivered");
    expect(await first.step()).toMatchObject({ toLocal: 1 });
    f.local.append("hub-telemetry", "fleet.telemetry");
    f.remote.append("far-telemetry", "fleet.telemetry");
    await first.step();
    expect(f.cursor().toLocal).toMatchObject({ after: 2, mark: 2 });

    f.remote.append("lost-receipt");
    const publish = f.local.port.publish, lost = new Error("receipt lost after append");
    let changed!: (kind: "events") => void, appended!: () => void, failReceipt!: () => void;
    f.local.port.subscribeChanges = callback => { changed = callback; return () => {}; };
    f.remote.port.subscribeChanges = () => () => {};
    const atAppend = new Promise<void>(resolve => { appended = resolve; });
    const receipt = new Promise<never>((_resolve, reject) => { failReceipt = () => reject(lost); });
    f.local.port.publish = async (...args) => {
      await publish(...args);
      changed("events"); appended();
      return receipt;
    };
    const running = first.run().then(() => undefined, error => error);
    try {
      await atAppend;
      for (let turn = 0; turn < 100 && f.cursor().toRemote.after < 3; turn++)
        await new Promise<void>(resolve => setImmediate(resolve));
      expect(f.cursor().toRemote.after).toBe(3); // The opposite drain saved while the receipt was in flight.
      expect(f.cursor().toLocal).toMatchObject({ after: 2, mark: 2 }); // It must not exclude that unknown delivery.
    } finally { failReceipt(); await running; }
    expect(await running).toBe(lost);
    f.local.port.publish = publish;
    expect(f.cursor().toLocal).toMatchObject({ after: 2, mark: 2 });
    await first.stop();

    const scan = f.local.port.bridgedIds;
    f.local.port.bridgedIds = async after => {
      const page = await scan(after);
      f.remote.append("during-restart");
      return page;
    };
    const restarted = f.create();
    await restarted.start();
    expect(await restarted.step()).toMatchObject({ toLocal: 1 });
    expect(f.local.events.filter(event => event.verification === "bridge").map(event => event.id))
      .toEqual(["copy-delivered", "copy-lost-receipt", "copy-during-restart"]);
    expect(f.cursor().toLocal.after).toBe(4);
    expect(await restarted.step()).toMatchObject({ toLocal: 0 });
  });
});
