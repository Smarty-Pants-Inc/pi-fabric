import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import { RootInbox, rootInboxSession, type RootInboxSession } from "../src/topology/root-inbox.js";

// smarty-dev#8962: code-voice got one <fabric-inbox> event (e2d5cfb1, mesh seq 1132029) at least
// six times after it had consumed it. An event injected into a turn is acknowledged by id and never
// injected again: not by a read that raced the ack, not after a restart, not by a stale writer's
// re-scan from an older cursor, and never past the stale horizon on the pending path.
const peer: MeshIdentity = { id: "session:dev-lead", name: "dev-lead", kind: "main" };
// The canonical session file does not (yet) hold the injected message: the case consume() exists for.
const notHeld: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
const TOPIC = "fleet.work.voice";
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

/** A real Fabric Main runtime (mesh, root inbox, ack path) on a temp mesh root; Pi itself is faked. */
const mainRuntime = (root: string, sessionId: string) => {
  const sent: Array<{ details: { ids: string[]; receipts: string[] } }> = [];
  const pi = { on: vi.fn(() => () => {}), events: { emit: vi.fn() }, getThinkingLevel: () => "off",
    getSessionName: () => "code-voice", sendMessage: vi.fn((message: never) => sent.push(message)), appendEntry: vi.fn(),
  } as unknown as ExtensionAPI;
  const context = { cwd: root, mode: "rpc", hasUI: false, isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(root, "unused-extension.mjs"), worker: path.join(root, "unused-worker.mjs"),
    residentHost: path.join(root, "unused-resident.mjs"), skills: root,
  } });
  return { runtime, context, pi, sent };
};

describe("root inbox acknowledges injected events by id (smarty-dev#8962)", () => {
  it("never injects an acknowledged event again: not to a read that raced the ack, not after a restart", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-ack-runtime-"));
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    cleanups.push(() => fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
      agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    });
    const sessionId = "c0dec0de-0000-4000-8000-000000008962";
    const mainId = `session:${sessionId}`;
    const first = mainRuntime(root, sessionId);
    let firstUp = true;
    cleanups.push(async () => { if (firstUp) await first.runtime.shutdown(); });
    await first.runtime.initialize(first.context, config);
    const event = await first.runtime.mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: mainId, text: "voice lane ready" });
    // Only bypass the inbox's existing 60-second steer grace, not publication or routing.
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      const batch = await first.runtime.nextRootInbox(notHeld);
      expect(batch?.events.map(value => value.id)).toEqual([event.id]);
      deliverRootInbox(first.pi, batch!.events);
      const { details } = first.sent[0]!;
      expect(details.ids).toEqual([event.id]);
      // The message enters a turn (message_end acks it) while another inbox read is in flight
      // (a turn-boundary reconcile or the idle wake). The ack must not be dropped by that read.
      const raced = first.runtime.nextRootInbox(notHeld);
      await first.runtime.consumeRootInbox(details.ids, details.receipts);
      expect((await raced)?.events ?? []).toEqual([]);
      for (let turn = 0; turn < 3; turn++) expect((await first.runtime.nextRootInbox(notHeld))?.events).toEqual([]);
    } finally { clock.mockRestore(); }
    // Restart: a new runtime for the same session on the same mesh re-reads the durable cursor.
    await first.runtime.shutdown();
    firstUp = false;
    const second = mainRuntime(root, sessionId);
    cleanups.push(() => second.runtime.shutdown());
    await second.runtime.initialize(second.context, config);
    const later = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      expect((await second.runtime.nextRootInbox(notHeld))?.events).toEqual([]);
      const fresh = await second.runtime.mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: mainId, text: "new work" });
      later.mockReturnValue(Date.now() + 2 * 61_000);
      expect((await second.runtime.nextRootInbox(notHeld))?.events.map(value => value.id)).toEqual([fresh.id]);
    } finally { later.mockRestore(); }
  }, 30_000);
});

const me: MeshIdentity = { id: "session:code-voice", name: "code-voice", kind: "main" };
const setup = (options: { horizonMs?: number } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-ack-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  cleanups.push(() => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const box = () => {
    const inbox = new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs: 0, ...options });
    cleanups.push(() => inbox.close());
    return inbox;
  };
  const publish = (text = "shadow", data?: unknown) => mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: me.id, text, data });
  return { mesh, box, publish, advance: (ms: number) => { now += ms; } };
};
const ids = (events: readonly MeshEvent[] | undefined) => (events ?? []).map(event => event.id);

describe("root inbox high-water dedup and stale re-delivery (smarty-dev#8962)", () => {
  it("does not re-inject an acknowledged event when a stale writer saves an older cursor and a restart re-scans", async () => {
    const h = setup();
    const before = h.box(); before.start();
    const event = await h.publish("voice lane ready"); h.advance(1);
    const batch = await before.next(notHeld);
    expect(ids(batch.events)).toEqual([event.id]);
    // A reload: the successor loads the pending batch and the injected message acks it there.
    const after = h.box();
    await after.consume([event.id]);
    expect((await after.next(notHeld)).events).toEqual([]);
    // The predecessor's in-flight turn-boundary reconcile lands late and saves its cached view
    // (pending batch, no receipt), because a follow-up receipt changed its state.
    const followUp = rootInboxSession([{ type: "custom_message", customType: "pi-fabric-agent-message",
      timestamp: new Date(Date.now()).toISOString(), details: { id: "fu-1", from: { id: peer.id } } }]);
    await before.next(followUp, { commitOnly: true });
    // A restart re-scans from whatever cursor is durable now: nothing at or below the mark again.
    for (let restart = 0; restart < 3; restart++) expect((await h.box().next(notHeld)).events).toEqual([]);
    const fresh = await h.publish("new work"); h.advance(1);
    expect(ids((await h.box().next(notHeld)).events)).toEqual([fresh.id]);
  });

  it("expires a pending event past the stale horizon on re-delivery, including one the log no longer returns", async () => {
    const h = setup({ horizonMs: 60_000 });
    const inbox = h.box(); inbox.start();
    const kept = await h.publish("kept"); const gone = await h.publish("gone"); h.advance(1);
    expect(ids((await inbox.next(notHeld)).events)).toEqual([kept.id, gone.id]);
    // The live log stops returning one pending event (retention/rotation): it is neither delivered nor expired yet.
    const read = h.mesh.read.bind(h.mesh);
    vi.spyOn(h.mesh, "read").mockImplementation(input => read(input).filter(event => event.id !== gone.id));
    h.advance(61_000);
    // Never acked, not held, past the horizon: re-delivery injects nothing and reports it stale once.
    const redelivery = await h.box().next(notHeld);
    expect(redelivery.events).toEqual([]);
    expect(redelivery.skippedStale).toBe(2);
    // The pending batch no longer wedges the cursor: new work arrives.
    const fresh = await h.publish("new work"); h.advance(1);
    expect(ids((await h.box().next(notHeld)).events)).toEqual([fresh.id]);
    expect(h.mesh.get(inbox.key, { fresh: true })?.value).not.toHaveProperty("pending.ids", [gone.id]);
  });
});
