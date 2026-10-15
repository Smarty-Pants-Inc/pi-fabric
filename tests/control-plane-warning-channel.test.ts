import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

const roots: string[] = [];
const planes: FabricControlPlane[] = [];
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const acks = (mesh: MeshStore) => mesh.read({ topic: "fabric.control.ack", limit: 1_000 });

const setup = (onUnfencedSenderWarning?: (message: string) => void) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-warning-channel-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 1_000);
  const owner = new FabricControlPlane(mesh, identity("session:owner"), { enabled: true, hostId: "session:owner", pollMs: 20,
    acknowledgementTimeoutMs: 1_000, ...(onUnfencedSenderWarning ? { onUnfencedSenderWarning } : {}) });
  planes.push(owner);
  let published = 0;
  const send = async (id: string, commandId: string) => {
    await mesh.publish({ topic: "fabric.control.command", kind: "steer", from: identity(id), to: "session:owner",
      data: { version: 1, commandId, targetId: "actor:target", operation: "steer", replyTo: id,
        requestedAt: Date.now(), deadlineAt: Date.now() + 5_000 } });
    published++;
    await vi.waitFor(() => expect(acks(mesh)).toHaveLength(published));
    return acks(mesh).at(-1)!.data;
  };
  return { owner, send };
};

afterEach(async () => {
  await Promise.all(planes.splice(0).map(value => value.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("unfenced legacy sender warning channel", () => {
  it("routes the counted, hourly rate-limited warning to the supplied sink and never to stderr", async () => {
    const sink = vi.fn(); const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const at = Date.now(); const clock = vi.spyOn(Date, "now").mockReturnValue(at);
    const { owner, send } = setup(sink);
    const handler = vi.fn(() => ({ accepted: true })); owner.start(handler);
    await send("session:a", "a:1"); await send("session:a", "a:2"); await send("session:b", "b:1");
    clock.mockReturnValue(at + 60 * 60 * 1_000 - 1); await send("session:a", "a:3");
    expect(sink).toHaveBeenCalledTimes(2);
    clock.mockReturnValue(at + 60 * 60 * 1_000); await send("session:a", "a:4");
    expect(handler).toHaveBeenCalledTimes(5);
    expect(sink.mock.calls.map(call => call[0])).toEqual([
      'Unfenced legacy sender delivery count=1 sender="session:a" operation=steer target="actor:target" mesh.controlIncarnationFence=warn',
      'Unfenced legacy sender delivery count=1 sender="session:b" operation=steer target="actor:target" mesh.controlIncarnationFence=warn',
      'Unfenced legacy sender delivery count=4 sender="session:a" operation=steer target="actor:target" mesh.controlIncarnationFence=warn',
    ]);
    expect(warn).not.toHaveBeenCalled();
  });

  it("falls back to the exact stderr line without a sink", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { owner, send } = setup(); owner.start(() => ({ accepted: true }));
    await send("session:a", "a:1"); await send("session:a", "a:2");
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]![0]).toBe('[pi-fabric] Unfenced legacy sender delivery count=1 sender="session:a" operation=steer target="actor:target" mesh.controlIncarnationFence=warn');
  });

  it("keeps acceptance when the sink throws", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const sink = vi.fn(() => { throw new Error("stale ctx"); });
    const { owner, send } = setup(sink);
    const handler = vi.fn(() => ({ accepted: true, messageId: "legacy" })); owner.start(handler);
    expect(await send("session:a", "a:1")).toMatchObject({ accepted: true, messageId: "legacy" });
    expect(sink).toHaveBeenCalledOnce(); expect(handler).toHaveBeenCalledOnce(); expect(warn).not.toHaveBeenCalled();
  });
});

// The runtime wires the sink to the live ctx: ui.notify with a UI, the exact stderr line without one.
describe("FabricRuntimeState unfenced sender warning wiring", () => {
  const runtimeMain = async (hasUI: boolean) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-warning-runtime-")); roots.push(root);
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("HERDR_PANE_ID", undefined);
    const sessionId = hasUI ? "aaaaaaaa-0000-4000-8000-0000000000a1" : "bbbbbbbb-0000-4000-8000-0000000000b2";
    const sendMessage = vi.fn();
    const notify = vi.fn();
    const pi = { on: vi.fn(() => () => {}), events: { emit: vi.fn() },
      getThinkingLevel: () => "off", getSessionName: () => undefined, sendMessage,
    } as unknown as ExtensionAPI;
    const context = { cwd: root, mode: "rpc", hasUI, isProjectTrusted: () => true,
      isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => ({ ok: true }) },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
        getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
      ui: { setStatus: vi.fn(), notify },
    } as unknown as ExtensionContext;
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
      extension: path.join(root, "unused-extension.mjs"), worker: path.join(root, "unused-worker.mjs"),
      residentHost: path.join(root, "unused-resident.mjs"), skills: root,
    } });
    await runtime.initialize(context, normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
      agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false },
    }));
    shutdowns.push(() => runtime.shutdown());
    const mainId = `session:${sessionId}`;
    // A legacy sender omits ownerIncarnation; the default warn fence admits and counts it.
    const legacy = (commandId: string) => runtime.mesh.publish({ topic: "fabric.control.command", kind: "followUp",
      from: identity("session:legacy"), to: mainId,
      data: { version: 1, commandId, targetId: mainId, operation: "followUp", replyTo: "session:legacy",
        requestedAt: Date.now(), deadlineAt: Date.now() + 10_000, message: `legacy ${commandId}` } });
    const ack = async (commandId: string) => {
      const find = () => acks(runtime.mesh).find(e => (e.data as { commandId?: string } | undefined)?.commandId === commandId);
      await vi.waitFor(() => expect(find()).toBeDefined(), { timeout: 8000, interval: 50 });
      return find()!.data;
    };
    return { runtime, notify, sendMessage, legacy, ack };
  };
  const shutdowns: Array<() => Promise<void>> = [];
  afterEach(async () => {
    for (const close of shutdowns.splice(0).reverse()) await close();
    vi.unstubAllEnvs();
  });
  const unfenced = (calls: unknown[][]) => calls.filter(call => String(call[0]).includes("Unfenced legacy sender"));
  const line = (count: number) => `Unfenced legacy sender delivery count=${count} sender="session:legacy" operation=followUp target=`;

  it("with a UI, notifies the counted warning and never writes it to stderr; delivery still happens", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const m = await runtimeMain(true);
    const event = await m.legacy("ui:1");
    expect(await m.ack("ui:1")).toMatchObject({ accepted: true });
    // Actual runtime bootstrap opts in only its journalled local Main; verify the durable
    // shared claim's original-envelope marker, not a manually constructed plane option.
    const targetId = (event.data as { targetId: string }).targetId;
    const claimKey = "topology/control-seen/" + createHash("sha256").update(`${targetId}\0ui:1`).digest("hex");
    expect(m.runtime.mesh.get(claimKey, { fresh: true })?.value).toMatchObject({
      commandId: "ui:1", targetId, followUpEventId: event.id,
    });
    await m.legacy("ui:2");
    expect(await m.ack("ui:2")).toMatchObject({ accepted: true });
    expect(m.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ content: expect.stringContaining("legacy ui:1") }), expect.anything());
    expect(m.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ content: expect.stringContaining("legacy ui:2") }), expect.anything());
    const notified = unfenced(m.notify.mock.calls);
    expect(notified).toHaveLength(1); // hourly rate limit: the second delivery only counts
    expect(String(notified[0]![0])).toContain(line(1));
    expect(notified[0]![1]).toBe("warning");
    expect(unfenced(warn.mock.calls)).toEqual([]);
  }, 30_000);

  it("without a UI, keeps the exact stderr line and still delivers the accepted command", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const m = await runtimeMain(false);
    await m.legacy("cli:1");
    expect(await m.ack("cli:1")).toMatchObject({ accepted: true });
    expect(m.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ content: expect.stringContaining("legacy cli:1") }), expect.anything());
    expect(unfenced(m.notify.mock.calls)).toEqual([]);
    const warned = unfenced(warn.mock.calls);
    expect(warned).toHaveLength(1);
    expect(String(warned[0]![0])).toContain(`[pi-fabric] ${line(1)}`);
  }, 30_000);
});
